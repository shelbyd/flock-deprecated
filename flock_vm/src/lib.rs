#![feature(thread_id_value)]

use flock_bytecode::ByteCode;

mod cluster;
use cluster::*;

mod task;
use task::*;

mod task_queue;
use task_queue::{ControlFlow, TaskQueue};

mod thread_runner;

use std::sync::Arc;

use dashmap::DashMap;

pub fn run(bytecode: ByteCode) -> Result<(), ExecutionError> {
    let mut vm = Vm::create();
    let bytecode = Arc::new(bytecode);
    let bytecode_id = vm.register(&bytecode);

    vm.block_on_task(TaskOrder {
        id: 0,
        task: Task::new(),
        bytecode_id,
    })?;

    Ok(())
}

pub struct Vm {
    task_queue: Arc<TaskQueue<TaskOrder>>,
    finished: Arc<DashMap<usize, TaskOrder>>,
    bytecode_registry: Option<Arc<ByteCode>>,
    cluster: Arc<Cluster>,
}

impl Vm {
    pub fn create() -> Vm {
        Vm {
            task_queue: Arc::new(TaskQueue::<TaskOrder>::new()),
            finished: Arc::new(DashMap::new()),
            bytecode_registry: None,
            cluster: Arc::new(Cluster::connect()),
        }
    }

    fn register(&mut self, bytecode: &Arc<ByteCode>) -> u64 {
        self.bytecode_registry = Some(bytecode.clone());
        0
    }

    fn block_on_task(&mut self, mut task_order: TaskOrder) -> Result<(), ExecutionError> {
        let mut threads = (0..num_cpus::get())
            .map(|_| {
                let mut executor = self.executor();
                std::thread::spawn(move || executor.run())
            })
            .collect::<Vec<_>>();

        threads.extend((0..1).map(|_| {
            let mut executor = self.remote_executor();
            std::thread::spawn(move || executor.run())
        }));

        self.executor().run_to_completion(&mut task_order)?;

        self.task_queue.finish(move || {
            for thread in threads {
                thread.join().unwrap()?;
            }
            Ok(())
        })?;

        assert_eq!(self.finished.len(), 0);

        Ok(())
    }

    fn executor(&self) -> Executor {
        Executor {
            handle: self.task_queue.handle(),
            bytecode: self.bytecode_registry.as_ref().unwrap().clone(),
            finished: self.finished.clone(),
        }
    }

    fn remote_executor(&self) -> RemoteExecutor {
        RemoteExecutor {
            handle: self.task_queue.handle(),
            finished: self.finished.clone(),
            cluster: self.cluster.clone(),
        }
    }
}

struct Executor {
    handle: task_queue::Handle<TaskOrder>,
    bytecode: Arc<ByteCode>,
    finished: Arc<DashMap<usize, TaskOrder>>,
}

impl Executor {
    fn run(&mut self) -> Result<(), ExecutionError> {
        while self.busy_tick()? {}
        Ok(())
    }

    fn busy_tick(&mut self) -> Result<bool, ExecutionError> {
        let mut next = match self.handle.next() {
            ControlFlow::Continue(n) => n,
            ControlFlow::Finish => return Ok(false),
            ControlFlow::Retry => return Ok(true),
        };

        self.run_to_completion(&mut next)?;
        let already_there = self.finished.insert(next.id, next);
        assert!(already_there.is_none());
        Ok(true)
    }

    fn run_to_completion(&mut self, task_order: &mut TaskOrder) -> Result<(), ExecutionError> {
        // TODO(shelbyd): Never overflow stack.
        loop {
            match task_order.task.run(&self.bytecode)? {
                Execution::Terminated => {
                    return Ok(());
                }
                Execution::Fork => {
                    use rand::Rng;

                    let mut forked = task_order.clone();
                    forked.id = rand::thread_rng().gen();
                    // TODO(shelbyd): Never generate duplicate ids.

                    forked.task.forked = true;
                    task_order.task.forked = false;

                    forked.task.stack.push(task_order.id as i64);
                    task_order.task.stack.push(forked.id as i64);

                    self.handle.push(forked);
                }
                Execution::Join { task_id, count } => {
                    let joined = self.busy_until_task_done(task_id)?;
                    let other_stack = &joined.task.stack;
                    let to_push = other_stack.split_at(other_stack.len() - count).1;
                    task_order.task.stack.extend(to_push.iter().cloned());
                }
            }
        }
    }

    fn busy_until_task_done(&mut self, task_id: usize) -> Result<TaskOrder, ExecutionError> {
        loop {
            // TODO(shelbyd): Error with unrecognized task id.
            if let Some(done) = self.finished.remove(&task_id) {
                return Ok(done.1);
            }
            self.busy_tick()?;
        }
    }
}

struct RemoteExecutor {
    handle: task_queue::Handle<TaskOrder>,
    finished: Arc<DashMap<usize, TaskOrder>>,
    cluster: Arc<Cluster>,
}

impl RemoteExecutor {
    fn run(&mut self) -> Result<(), ExecutionError> {
        loop {
            match self.handle.next() {
                ControlFlow::Retry => continue,
                ControlFlow::Finish => return Ok(()),
                ControlFlow::Continue(task_order) => match self.cluster.run(task_order) {
                    Ok(finished) => {
                        let already_there = self.finished.insert(finished.id, finished);
                        assert!(already_there.is_none());
                    }
                    Err(RunError::CouldNotRun(order)) => {
                        self.handle.push(order);
                        std::thread::sleep(std::time::Duration::from_millis(10));
                    }
                    Err(RunError::Execution(e)) => return Err(e),
                },
            }
        }
    }
}

#[derive(Debug, Clone)]
struct TaskOrder {
    id: usize,
    task: Task,
    bytecode_id: u64,
}
