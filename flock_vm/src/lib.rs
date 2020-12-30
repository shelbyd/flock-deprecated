use flock_bytecode::{ByteCode, OpCode};

pub struct Vm {
    program_counter: usize,
    stack: Vec<i64>,
}

impl Vm {
    pub fn new() -> Vm {
        Vm {
            program_counter: 0,
            stack: Vec::new(),
        }
    }

    pub fn run(&mut self, bytecode: &ByteCode) -> Result<(), ExecutionError> {
        loop {
            if let ControlFlow::Exit = self.tick(bytecode)? {
                break;
            }
        }
        Ok(())
    }

    fn tick(&mut self, bytecode: &ByteCode) -> Result<ControlFlow, ExecutionError> {
        let op = match bytecode.get(self.program_counter) {
            Some(op) => op,
            None => return Ok(ControlFlow::Exit),
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
                eprintln!("Flock VM Debug");
                eprintln!("PC: {}", self.program_counter);

                eprintln!("");

                eprintln!("OpCodes:");
                let bounds: usize = 5;
                for (i, op) in bytecode.surrounding(self.program_counter, bounds) {
                    let delta = (i as isize) - ((self.program_counter - 1) as isize);
                    eprintln!("  {:#2}: {:?}", delta, op);
                }

                eprintln!("");

                eprintln!("Stack:");
                for (i, value) in self.stack.iter().rev().enumerate() {
                    eprintln!("  {:#03} {:#018x} ({})", i, value, value)
                }
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
}

enum ControlFlow {
    Exit,
    Continue,
}

#[derive(Debug)]
pub enum ExecutionError {
    PopFromEmptyStack,
}

impl std::error::Error for ExecutionError {}

impl std::fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
