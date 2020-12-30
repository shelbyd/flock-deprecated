use flock_bytecode::{ByteCode, ConditionFlags, OpCode};

use std::collections::VecDeque;

pub struct Vm {
    tasks: VecDeque<TaskState>,
}

impl Vm {
    pub fn new() -> Vm {
        Vm {
            tasks: {
                let mut que = VecDeque::new();
                que.push_back(TaskState {
                    id: 0,
                    task: Task::new(),
                    state: State::Ready,
                });
                que
            },
        }
    }

    pub fn run(&mut self, bytecode: &ByteCode) -> Result<(), ExecutionError> {
        loop {
            let all_terminated = self.tasks.iter().all(|t| t.state == State::Terminated);
            if all_terminated {
                return Ok(());
            }

            let mut task_state = match self.tasks.pop_front() {
                None => return Ok(()),
                Some(t) => t,
            };

            match task_state.state {
                State::Blocked | State::Terminated => {
                    self.tasks.push_back(task_state);
                    continue;
                }
                State::Ready => {}
            }

            match task_state.task.run(bytecode)? {
                Execution::Terminated => {
                    task_state.state = State::Terminated;
                    self.tasks.push_back(task_state);
                }
                Execution::Blocked => {
                    task_state.state = State::Blocked;
                    self.tasks.push_back(task_state);
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

                    self.tasks.push_front(forked);
                    self.tasks.push_front(task_state);
                }
                Execution::Join { task_id, count } => {
                    let other_task = self
                        .tasks
                        .iter()
                        .find(|t| t.id == task_id)
                        .ok_or(ExecutionError::UnknownTaskId(task_id))?;
                    match other_task.state {
                        State::Terminated => {
                            let other_stack = &other_task.task.stack;
                            let to_push = other_stack.split_at(other_stack.len() - count).1;
                            task_state.task.stack.extend(to_push.iter().cloned());
                            self.tasks.push_front(task_state);
                        }
                        State::Ready | State::Blocked => {
                            // TODO(shelbyd): Put into a proper blocked state.
                            task_state.task.program_counter -= 1;
                            task_state.task.stack.push(task_id as i64);
                            self.tasks.push_back(task_state);
                        }
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
struct TaskState {
    id: usize,
    task: Task,
    state: State,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum State {
    Ready,
    Blocked,
    Terminated,
}

enum Execution {
    Terminated,
    Blocked,
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
                        if flags.contains(ConditionFlags::FORK) && self.forked {
                            // dbg!("jump fork");
                            // dbg!(&self.stack);
                        }
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
