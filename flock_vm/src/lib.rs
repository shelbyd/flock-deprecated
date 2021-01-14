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

    vm.push_task(TaskOrder {
        id: 0,
        task: Task::new(),
        bytecode_id,
    });

    vm.block_on_task(0)?;

    Ok(())
}

pub struct Vm {
    task_queue: Arc<TaskQueue<TaskOrder>>,
    sub_tasks: Arc<Mutex<HashMap<usize, SubTask>>>,
    bytecode_registry: Option<Arc<ByteCode>>,
}

impl Vm {
    pub fn create() -> Vm {
        Vm {
            task_queue: Arc::new(TaskQueue::<TaskOrder>::new()),
            sub_tasks: Arc::new(Mutex::new(HashMap::new())),
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

    fn push_task(&mut self, task_order: TaskOrder) {
        self.task_queue.push(task_order);
    }

    fn block_on_task(&mut self, task_id: usize) -> Result<(), ExecutionError> {
        let mut threads = (0..num_cpus::get())
            .map(|_| {
                let mut executor = self.executor();
                std::thread::spawn(move || executor.run())
            })
            .collect::<Vec<_>>();

        let message_sender = std::thread::spawn(|| {
            use std::time::*;

            let mut node = flock_rpc::Node::<Message>::new(18455).unwrap();
            node.connect("127.0.0.1:18454").unwrap();
            for _ in 0..3 {
                node.broadcast(Message::Test).unwrap();
                std::thread::sleep(Duration::from_millis(1000));
            }
            std::thread::sleep(Duration::from_millis(3000));
            Ok(())
        });

        threads.push(message_sender);

        for thread in threads {
            thread.join().unwrap()?;
        }

        assert_eq!(self.sub_tasks.lock().unwrap().len(), 1);
        assert!(self.sub_tasks.lock().unwrap().contains_key(&task_id));

        Ok(())
    }

    fn executor(&self) -> Executor {
        Executor {
            handle: self.task_queue.handle(),
            bytecode: self.bytecode_registry.as_ref().unwrap().clone(),
            sub_tasks: self.sub_tasks.clone(),
        }
    }
}

struct Executor {
    handle: task_queue::Handle<TaskOrder>,
    bytecode: Arc<ByteCode>,
    sub_tasks: Arc<Mutex<HashMap<usize, SubTask>>>,
}

impl Executor {
    fn run(&mut self) -> Result<(), ExecutionError> {
        while self.tick()? {}
        Ok(())
    }

    fn tick(&mut self) -> Result<bool, ExecutionError> {
        let mut task_order = match self.handle.next() {
            None => return Ok(false),
            Some(t) => t,
        };

        match task_order.task.run(&self.bytecode)? {
            Execution::Terminated => {
                let mut lock = self.sub_tasks.lock().unwrap();

                let existing = lock.insert(task_order.id, SubTask::Finished(task_order));
                match existing {
                    Some(SubTask::Blocking(tasks)) => {
                        for task in tasks {
                            self.handle.push(task);
                        }
                    }
                    None => {}
                    Some(SubTask::Finished(_)) => unreachable!(),
                }
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
                self.handle.push(task_order);
            }
            Execution::Join { task_id, count } => {
                let mut sub_tasks = self.sub_tasks.lock().unwrap();

                // TODO(shelbyd): Error with unrecognized task id.
                let sub_task = sub_tasks
                    .remove(&task_id)
                    .unwrap_or(SubTask::Blocking(Vec::new()));
                match sub_task {
                    SubTask::Blocking(mut blocked) => {
                        task_order.task.program_counter -= 1;
                        task_order.task.stack.push(task_id as i64);
                        blocked.push(task_order);
                        sub_tasks.insert(task_id, SubTask::Blocking(blocked));
                    }
                    SubTask::Finished(joined) => {
                        let other_stack = &joined.task.stack;
                        let to_push = other_stack.split_at(other_stack.len() - count).1;
                        task_order.task.stack.extend(to_push.iter().cloned());
                        self.handle.push(task_order);
                    }
                }
            }
        }

        Ok(true)
    }
}

#[derive(Debug, Clone)]
struct TaskOrder {
    id: usize,
    task: Task,
    bytecode_id: u64,
}

#[derive(Debug)]
enum SubTask {
    Finished(TaskOrder),
    Blocking(Vec<TaskOrder>),
}

use serde::{Deserialize, Serialize};
#[derive(Deserialize, Serialize, Debug)]
pub enum Message {
    Test,
}
