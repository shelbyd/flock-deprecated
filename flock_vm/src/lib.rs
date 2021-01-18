#![feature(thread_id_value)]

use flock_bytecode::ByteCode;

pub mod cluster;
use cluster::*;

mod task;
use task::*;

mod task_queue;
use task_queue::{ControlFlow, TaskQueue};

mod thread_runner;

use std::sync::Arc;

use dashmap::DashMap;

gflags::define! {
    pub --remote-workers: u16 = 1
}

gflags::define! {
    pub --max-local-workers: usize = usize::MAX
}

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

type FinishedMap = DashMap<usize, Result<TaskOrder, ExecutionError>>;
type ByteCodeMap = DashMap<u64, Arc<ByteCode>>;

pub struct Vm {
    task_queue: TaskQueue<TaskOrder>,
    finished: Arc<FinishedMap>,
    bytecode_registry: Arc<ByteCodeMap>,
    cluster: Option<Arc<Cluster>>,
    workers: Vec<std::thread::JoinHandle<()>>,
}

impl Vm {
    pub fn create() -> Vm {
        let mut result = Vm {
            task_queue: TaskQueue::<TaskOrder>::new(),
            finished: Arc::new(DashMap::new()),
            bytecode_registry: Arc::new(DashMap::new()),
            cluster: Some(Arc::new(Cluster::connect())),
            workers: Vec::new(),
        };
        result.workers = result.spawn_workers();
        result
    }

    pub fn create_leaf() -> Vm {
        let mut result = Vm {
            task_queue: TaskQueue::<TaskOrder>::new(),
            finished: Arc::new(DashMap::new()),
            bytecode_registry: Arc::new(DashMap::new()),
            cluster: None,
            workers: Vec::new(),
        };
        result.workers = result.spawn_workers();
        result
    }

    fn register(&mut self, bytecode: &Arc<ByteCode>) -> u64 {
        self.bytecode_registry.insert(0, bytecode.clone());
        0
    }

    fn block_on_task(&mut self, task_order: TaskOrder) -> Result<(), ExecutionError> {
        self.spawn_workers();

        self.executor().run_to_completion(task_order)?;
        assert_eq!(self.finished.len(), 0);

        Ok(())
    }

    fn spawn_workers(&self) -> Vec<std::thread::JoinHandle<()>> {
        let mut workers = Vec::new();

        let local_workers = std::cmp::min(num_cpus::get(), MAX_LOCAL_WORKERS.flag);
        workers.extend(
            (0..local_workers)
                .map(|_| self.executor())
                .map(|mut executor| std::thread::spawn(move || executor.run())),
        );

        workers.extend(
            (0..REMOTE_WORKERS.flag)
                .filter_map(|_| self.remote_executor())
                .map(|mut executor| std::thread::spawn(move || executor.run())),
        );

        workers
    }

    fn executor(&self) -> Executor {
        Executor {
            handle: self.task_queue.handle(),
            bytecode: self.bytecode_registry.clone(),
            finished: self.finished.clone(),
        }
    }

    fn remote_executor(&self) -> Option<RemoteExecutor> {
        self.cluster.as_ref().map(|cluster| RemoteExecutor {
            handle: self.task_queue.handle(),
            finished: self.finished.clone(),
            cluster: cluster.clone(),
        })
    }

    fn enqueue(&self, task_order: TaskOrder) {
        self.task_queue.push(task_order);
    }

    fn take_finished(&self, id: usize) -> Option<Result<TaskOrder, ExecutionError>> {
        self.finished.remove(&id).map(|pair| pair.1)
    }
}

impl Drop for Vm {
    fn drop(&mut self) {
        let workers = &mut self.workers;
        self.task_queue.finish(move || {
            for thread in workers.drain(..) {
                thread.join().unwrap();
            }
        });
    }
}

struct Executor {
    handle: task_queue::Handle<TaskOrder>,
    bytecode: Arc<ByteCodeMap>,
    finished: Arc<FinishedMap>,
}

impl Executor {
    fn run(&mut self) {
        while self.busy_tick() {}
    }

    fn busy_tick(&mut self) -> bool {
        let next = match self.handle.next() {
            ControlFlow::Continue(n) => n,
            ControlFlow::Finish => return false,
            ControlFlow::Retry => return true,
        };
        let id = next.id;

        let result = self.run_to_completion(next);
        let already_there = self.finished.insert(id, result);
        assert!(already_there.is_none());
        true
    }

    fn run_to_completion(
        &mut self,
        mut task_order: TaskOrder,
    ) -> Result<TaskOrder, ExecutionError> {
        // TODO(shelbyd): Never overflow stack.
        loop {
            let bytecode = self.bytecode.get(&task_order.bytecode_id).unwrap().clone();
            match task_order.task.run(&bytecode)? {
                Execution::Terminated => {
                    return Ok(task_order);
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
        let mut last_failed = false;
        loop {
            // TODO(shelbyd): Error with unrecognized task id.
            if let Some(done) = self.finished.remove(&task_id) {
                return done.1;
            }
            if !self.busy_tick() {
                if last_failed {
                    return Err(ExecutionError::UnableToProgress);
                }
                last_failed = true;
            }
        }
    }
}

struct RemoteExecutor {
    handle: task_queue::Handle<TaskOrder>,
    finished: Arc<FinishedMap>,
    cluster: Arc<Cluster>,
}

impl RemoteExecutor {
    fn run(&mut self) {
        loop {
            match self.handle.next() {
                ControlFlow::Retry => continue,
                ControlFlow::Finish => return,
                ControlFlow::Continue(task_order) => {
                    let id = task_order.id;
                    match self.cluster.run(task_order) {
                        Ok(finished) => {
                            let already_there = self.finished.insert(id, Ok(finished));
                            assert!(already_there.is_none());
                        }
                        Err(RunError::CouldNotRun(order)) => {
                            self.handle.push(order);
                            std::thread::sleep(std::time::Duration::from_millis(10));
                        }
                        Err(RunError::Execution(e)) => {
                            let already_there = self.finished.insert(id, Err(e));
                            assert!(already_there.is_none());
                        }
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
struct TaskOrder {
    id: usize,
    task: Task,
    bytecode_id: u64,
}
