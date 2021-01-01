#![feature(thread_id_value)]

use flock_bytecode::{ByteCode, ConditionFlags, OpCode};

mod task_queue;
use task_queue::TaskQueue;

mod thread_runner;

use lockfree::map::Map;
use std::sync::Arc;

pub struct Vm {
    bytecode: ByteCode,
    task_queue: Arc<TaskQueue<TaskState>>,
    finished: Arc<Map<usize, TaskState>>,
}

impl Vm {
    pub fn new(bytecode: ByteCode) -> Vm {
        Vm {
            bytecode,
            task_queue: Arc::new(TaskQueue::<TaskState>::new()),
            finished: Arc::new(Map::new()),
        }
    }

    pub fn run(self) -> Result<(), ExecutionError> {
        let self_ = Arc::new(self);

        self_.task_queue.push(TaskState {
            id: 0,
            task: Task::new(),
        });

        let mut threads = Vec::new();

        let max_threads = usize::MAX;
        for _ in 0..usize::min(max_threads, num_cpus::get()) {
            let self_ = self_.clone();

            let thread = std::thread::spawn(move || -> Result<_, ExecutionError> {
                let mut queue_handle = self_.task_queue.clone().handle();
                loop {
                    let should_continue =
                        self_.tick(&self_.bytecode, &mut queue_handle, &self_.finished)?;
                    if !should_continue {
                        return Ok(());
                    }
                }
            });
            threads.push(thread);
        }

        for thread in threads {
            thread.join().unwrap()?;
        }
        Ok(())
    }

    fn tick(
        &self,
        bytecode: &ByteCode,
        queue_handle: &mut task_queue::Handle<TaskState>,
        finished: &Map<usize, TaskState>,
    ) -> Result<bool, ExecutionError> {
        let mut task_state = match queue_handle.next() {
            None => return Ok(false),
            Some(t) => t,
        };

        match task_state.task.run(bytecode)? {
            Execution::Terminated => {
                finished.insert(task_state.id, task_state);
                queue_handle.task_finished();
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

                queue_handle.push(forked);
                queue_handle.push(task_state);
            }
            Execution::Join { task_id, count } => {
                let should_drop_task = if let Some(entry) = finished.get(&task_id) {
                    let other_stack = &entry.val().task.stack;
                    let to_push = other_stack.split_at(other_stack.len() - count).1;
                    task_state.task.stack.extend(to_push.iter().cloned());
                    queue_handle.push(task_state);

                    true
                } else {
                    // TODO(shelbyd): Error with unrecognized task id.
                    task_state.task.program_counter -= 1;
                    task_state.task.stack.push(task_id as i64);

                    queue_handle.push_blocked(task_state);
                    false
                };
                // TODO(shelbyd): Workaround for lifetime of read guard.
                if should_drop_task {
                    finished.remove(&task_id);
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
                    if flags.is_empty() {
                        true
                    } else {
                        let zero = flags
                            .contains(ConditionFlags::ZERO)
                            .implies(*self.peek()? == 0);
                        let forked = flags.contains(ConditionFlags::FORK).implies(self.forked);
                        zero && forked
                    }
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
                // self.print_debug(bytecode);
                // {
                //     use std::io::BufRead;
                //     let stdin = std::io::stdin();
                //     stdin.lock().read_line(&mut String::new()).unwrap();
                // }

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
