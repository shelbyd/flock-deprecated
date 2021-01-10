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
    let vm = Vm::new();
    let bytecode = Arc::new(bytecode);

    vm.task_queue.push(TaskState {
        id: 0,
        task: Task::new(),
    });

    let mut threads = Vec::new();

    let max_threads = usize::MAX;
    for _ in 0..usize::min(max_threads, num_cpus::get()) {
        let mut executor = vm.executor(&bytecode);
        let thread = std::thread::spawn(move || executor.run());
        threads.push(thread);
    }

    for thread in threads {
        thread.join().unwrap()?;
    }

    assert_eq!(vm.sub_tasks.lock().unwrap().len(), 1);

    Ok(())
}

pub struct Vm {
    task_queue: Arc<TaskQueue<TaskState>>,
    sub_tasks: Arc<Mutex<HashMap<usize, SubTask>>>,
}

impl Vm {
    fn new() -> Vm {
        Vm {
            task_queue: Arc::new(TaskQueue::<TaskState>::new()),
            sub_tasks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn executor(&self, bytecode: &Arc<ByteCode>) -> Executor {
        Executor {
            handle: self.task_queue.handle(),
            bytecode: bytecode.clone(),
            sub_tasks: self.sub_tasks.clone(),
        }
    }
}

struct Executor {
    handle: task_queue::Handle<TaskState>,
    bytecode: Arc<ByteCode>,
    sub_tasks: Arc<Mutex<HashMap<usize, SubTask>>>,
}

impl Executor {
    fn run(&mut self) -> Result<(), ExecutionError> {
        while self.tick()? {}
        Ok(())
    }

    fn tick(&mut self) -> Result<bool, ExecutionError> {
        let mut task_state = match self.handle.next() {
            None => return Ok(false),
            Some(t) => t,
        };

        match task_state.task.run(&self.bytecode)? {
            Execution::Terminated => {
                let mut lock = self.sub_tasks.lock().unwrap();

                let existing = lock.insert(task_state.id, SubTask::Finished(task_state));
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

                let mut forked = task_state.clone();
                forked.id = rand::thread_rng().gen();
                // TODO(shelbyd): Never generate duplicate ids.

                forked.task.forked = true;
                task_state.task.forked = false;

                forked.task.stack.push(task_state.id as i64);
                task_state.task.stack.push(forked.id as i64);

                self.handle.push(forked);
                self.handle.push(task_state);
            }
            Execution::Join { task_id, count } => {
                let mut sub_tasks = self.sub_tasks.lock().unwrap();

                // TODO(shelbyd): Error with unrecognized task id.
                let sub_task = sub_tasks
                    .remove(&task_id)
                    .unwrap_or(SubTask::Blocking(Vec::new()));
                match sub_task {
                    SubTask::Blocking(mut blocked) => {
                        task_state.task.program_counter -= 1;
                        task_state.task.stack.push(task_id as i64);
                        blocked.push(task_state);
                        sub_tasks.insert(task_id, SubTask::Blocking(blocked));
                    }
                    SubTask::Finished(joined) => {
                        let other_stack = &joined.task.stack;
                        let to_push = other_stack.split_at(other_stack.len() - count).1;
                        task_state.task.stack.extend(to_push.iter().cloned());
                        self.handle.push(task_state);
                    }
                }
            }
        }

        Ok(true)
    }
}

#[derive(Debug, Clone)]
struct TaskState {
    id: usize,
    task: Task,
}

#[derive(Debug)]
enum SubTask {
    Finished(TaskState),
    Blocking(Vec<TaskState>),
}
