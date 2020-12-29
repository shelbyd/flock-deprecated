use flock_bytecode::{ByteCode, OpCode};

use crate::statement::{Argument, Statement};

pub fn to_bytecode(statements: &[Statement]) -> Result<ByteCode, Box<dyn std::error::Error>> {
    let opcodes = statements
        .iter()
        .map(to_opcode)
        .filter_map(|op| op.transpose())
        .collect::<Result<Vec<OpCode>, _>>()?;
    Ok(ByteCode::from(opcodes))
}

fn to_opcode(statement: &Statement) -> Result<Option<OpCode>, Box<dyn std::error::Error>> {
    match statement {
        Statement::Comment(_) => Ok(None),
        Statement::EmptyLine => Ok(None),
        Statement::LabelDefinition(_) => Ok(None),
        Statement::Command1("PUSH", arg) => Ok(Some(OpCode::Push(resolve(arg)))),
        Statement::Command0("ADD") => Ok(Some(OpCode::Add)),
        Statement::Command0("DUMP_DEBUG") => Ok(Some(OpCode::DumpDebug)),
        s => Err(Box::new(UnrecognizedStatementError::new(s))),
    }
}

#[derive(Debug)]
pub struct UnrecognizedStatementError {
    message: String,
}

impl UnrecognizedStatementError {
    fn new(statement: &Statement) -> Self {
        UnrecognizedStatementError {
            message: format!("Unrecognized statement {:?}", statement),
        }
    }
}

impl std::error::Error for UnrecognizedStatementError {}

impl std::fmt::Display for UnrecognizedStatementError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

fn resolve(argument: &Argument) -> i64 {
    match argument {
        Argument::Literal(n) => *n,
    }
}
