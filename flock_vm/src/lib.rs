#![feature(thread_id_value)]

use flock_bytecode::ByteCode;

mod task;
use task::*;

mod task_queue;
use task_queue::TaskQueue;

mod thread_runner;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub fn run(bytecode: ByteCode) -> Result<(), ExecutionError> {
    let mut vm = Vm::connect().unwrap_or_else(|| Vm::create());
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
    finished: Arc<Mutex<HashMap<usize, TaskOrder>>>,
    bytecode_registry: Option<Arc<ByteCode>>,
}

impl Vm {
    pub fn create() -> Vm {
        Vm {
            task_queue: Arc::new(TaskQueue::<TaskOrder>::new()),
            finished: Arc::new(Mutex::new(HashMap::new())),
            bytecode_registry: None,
        }
    }

    pub fn connect() -> Option<Vm> {
        None
    }

    fn register(&mut self, bytecode: &Arc<ByteCode>) -> u64 {
        self.bytecode_registry = Some(bytecode.clone());
        0
    }

    fn block_on_task(&mut self, mut task_order: TaskOrder) -> Result<(), ExecutionError> {
        let threads = (0..num_cpus::get())
            .map(|_| {
                let mut executor = self.executor();
                std::thread::spawn(move || executor.run())
            })
            .collect::<Vec<_>>();

        self.executor().run_to_completion(&mut task_order)?;

        for thread in threads {
            thread.join().unwrap()?;
        }

        assert_eq!(self.finished.lock().unwrap().len(), 0);

        Ok(())
    }

    fn executor(&self) -> Executor {
        Executor {
            handle: self.task_queue.handle(),
            bytecode: self.bytecode_registry.as_ref().unwrap().clone(),
            finished: self.finished.clone(),
        }
    }
}

struct Executor {
    handle: task_queue::Handle<TaskOrder>,
    bytecode: Arc<ByteCode>,
    finished: Arc<Mutex<HashMap<usize, TaskOrder>>>,
}

impl Executor {
    fn run(&mut self) -> Result<(), ExecutionError> {
        while self.busy_tick()? {}
        Ok(())
    }

    fn busy_tick(&mut self) -> Result<bool, ExecutionError> {
        if let Some(mut next) = self.handle.next() {
            self.run_to_completion(&mut next)?;
            let already_there = self.finished
                .lock()
                .unwrap()
                .insert(next.id, next);
            assert!(already_there.is_none());
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn run_to_completion(&mut self, task_order: &mut TaskOrder) -> Result<(), ExecutionError> {
        loop {
            match task_order.task.run(&self.bytecode)? {
                Execution::Terminated => return Ok(()),
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
            if let Some(done) = self.finished.lock().unwrap().remove(&task_id) {
                return Ok(done);
            }
            self.busy_tick()?;
        }
    }
}

#[derive(Debug, Clone)]
struct TaskOrder {
    id: usize,
    task: Task,
    bytecode_id: u64,
}

use serde::{Deserialize, Serialize};
#[derive(Deserialize, Serialize, Debug)]
pub enum Message {
    Test,
}
