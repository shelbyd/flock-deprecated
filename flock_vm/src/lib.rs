#![feature(thread_id_value)]

use flock_bytecode::{ByteCode, ConditionFlags, OpCode};

mod task_queue;
use task_queue::TaskQueue;

mod thread_runner;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub fn run(bytecode: ByteCode) -> Result<(), ExecutionError> {
    Vm::new().run(bytecode)
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

    fn run(&self, bytecode: ByteCode) -> Result<(), ExecutionError> {
        let bytecode = Arc::new(bytecode);

        self.task_queue.push(TaskState {
            id: 0,
            task: Task::new(),
        });

        let mut threads = Vec::new();

        let max_threads = usize::MAX;
        for _ in 0..usize::min(max_threads, num_cpus::get()) {
            let mut executor = self.executor(&bytecode);
            let thread = std::thread::spawn(move || executor.run());
            threads.push(thread);
        }

        for thread in threads {
            thread.join().unwrap()?;
        }
        Ok(())
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
enum Execution {
    Terminated,
    Fork,
    Join { task_id: usize, count: usize },
}

#[derive(Debug)]
enum SubTask {
    Finished(TaskState),
    Blocking(Vec<TaskState>),
}

#[derive(Debug, Clone)]
pub struct Task {
    program_counter: usize,
    stack: Vec<i64>,
    forked: bool,
}

impl Task {
    fn new() -> Task {
        Task {
            program_counter: 0,
            stack: Vec::new(),
            forked: false,
        }
    }

    fn run(&mut self, bytecode: &ByteCode) -> Result<Execution, ExecutionError> {
        loop {
            if let ControlFlow::Return(execution) = self.tick(bytecode)? {
                return Ok(execution);
            }
        }
    }

    fn tick(&mut self, bytecode: &ByteCode) -> Result<ControlFlow, ExecutionError> {
        let op = match bytecode.get(self.program_counter) {
            Some(op) => op,
            None => return Ok(ControlFlow::Return(Execution::Terminated)),
        };
        self.program_counter += 1;

        match op {
            OpCode::Push(value) => {
                self.stack.push(*value);
            }
            OpCode::Add => {
                let a = self.pop()?;
                let b = self.pop()?;
                self.stack.push(a.overflowing_add(b).0);
            }
            OpCode::DumpDebug => {
                self.print_debug(bytecode);
            }
            OpCode::Jump(flags, target) => {
                let target = match target {
                    None => self.pop()?,
                    Some(t) => *t,
                };

                let should_jump = {
                    let zero = flags
                        .contains(ConditionFlags::ZERO)
                        .implies(*self.peek()? == 0);
                    let forked = flags.contains(ConditionFlags::FORK).implies(self.forked);
                    zero && forked
                };
                if should_jump {
                    self.program_counter = target as usize;
                }
            }
            OpCode::JumpToSubroutine(target) => {
                let target = match target {
                    None => self.pop()?,
                    Some(t) => *t,
                };

                self.stack.push(self.program_counter as i64);
                self.program_counter = target as usize;
            }
            OpCode::Bury(index) => {
                let value = self.pop()?;

                let insert_index = self
                    .stack
                    .len()
                    .checked_sub(*index as usize)
                    .ok_or(ExecutionError::BuryOutOfRange(*index))?;

                self.stack.insert(insert_index, value);
            }
            OpCode::Dredge(index) => {
                let remove_index = (self.stack.len() - 1)
                    .checked_sub(*index as usize)
                    .ok_or(ExecutionError::DredgeOutOfRange(*index))?;
                let value = self.stack.remove(remove_index);
                self.stack.push(value);
            }
            OpCode::Duplicate => {
                let value = self.pop()?;
                self.stack.push(value);
                self.stack.push(value);
            }
            OpCode::Pop => {
                self.pop()?;
            }
            OpCode::Return => {
                let target = self.pop()?;
                self.program_counter = target as usize;
            }
            OpCode::Fork => {
                return Ok(ControlFlow::Return(Execution::Fork));
            }
            OpCode::Join(count) => {
                let task_id = self.pop()? as usize;
                return Ok(ControlFlow::Return(Execution::Join {
                    task_id,
                    count: *count as usize,
                }));
            }
            OpCode::Halt => {
                return Ok(ControlFlow::Return(Execution::Terminated));
            }
            op => {
                unimplemented!("Unhandled opcode {:?}", op);
            }
        }

        Ok(ControlFlow::Continue)
    }

    fn pop(&mut self) -> Result<i64, ExecutionError> {
        self.stack.pop().ok_or(ExecutionError::PopFromEmptyStack)
    }

    fn peek(&mut self) -> Result<&i64, ExecutionError> {
        self.stack
            .get(self.stack.len() - 1)
            .ok_or(ExecutionError::PeekFromEmptyStack)
    }

    fn print_debug(&self, bytecode: &ByteCode) {
        eprintln!("Flock VM Debug");
        eprintln!("PC: {}", self.program_counter);

        eprintln!("");

        eprintln!("OpCodes:");
        let bounds: usize = 5;
        for (i, op) in bytecode.surrounding(self.program_counter, bounds) {
            let delta = (i as isize) - (self.program_counter as isize);
            eprintln!("  {:#2}: {:?}", delta, op);
        }

        eprintln!("");

        eprintln!("Stack:");
        for (i, value) in self.stack.iter().rev().enumerate() {
            eprintln!("  {:#03} {:#018x} ({})", i, value, value)
        }
    }
}

enum ControlFlow {
    Continue,
    Return(Execution),
}

#[derive(Debug)]
pub enum ExecutionError {
    PopFromEmptyStack,
    PeekFromEmptyStack,
    DredgeOutOfRange(i64),
    BuryOutOfRange(i64),
    UnknownTaskId(usize),
}

impl std::error::Error for ExecutionError {}

impl std::fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

trait BoolImplies {
    fn implies(self, other: Self) -> Self;
}

impl BoolImplies for bool {
    fn implies(self, other: bool) -> bool {
        if self {
            other
        } else {
            true
        }
    }
}
