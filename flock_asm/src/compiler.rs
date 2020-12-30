use flock_bytecode::{ByteCode, ConditionFlags, OpCode};
use std::collections::HashMap;

use crate::statement::{Argument, Statement};

type LabelTable<'s> = HashMap<&'s str, usize>;
type OpCodeThunk<'s> = dyn FnOnce(&LabelTable<'s>) -> Result<OpCode, CompilationError> + 's;

pub fn to_bytecode(statements: &[Statement]) -> Result<ByteCode, Box<dyn std::error::Error>> {
    let mut thunks = Vec::new();

    let mut label_table = HashMap::new();
    for statement in statements {
        let action = compile_action(statement)?;
        match action {
            Some(CompileAction::OpCodeThunk(thunk)) => {
                thunks.push(thunk);
            }
            Some(CompileAction::PushOpcode(code)) => {
                thunks.push(Box::new(|_: &_| Ok(code)) as Box<OpCodeThunk>);
            }
            Some(CompileAction::RegisterLabel(label)) => {
                label_table.insert(label, thunks.len());
            }
            None => {}
        }
    }
    let opcodes: Vec<OpCode> = thunks
        .into_iter()
        .map(move |th| th(&label_table))
        .collect::<Result<_, _>>()?;
    Ok(ByteCode::from(opcodes))
}

enum CompileAction<'s> {
    PushOpcode(OpCode),
    OpCodeThunk(Box<OpCodeThunk<'s>>),
    RegisterLabel(&'s str),
}

impl<'s> From<OpCode> for CompileAction<'s> {
    fn from(opcode: OpCode) -> Self {
        CompileAction::PushOpcode(opcode)
    }
}

fn compile_action<'s>(
    statement: &'s Statement,
) -> Result<Option<CompileAction<'s>>, Box<dyn std::error::Error>> {
    let action = match statement {
        Statement::Comment(_) => return Ok(None),
        Statement::EmptyLine => return Ok(None),
        Statement::LabelDefinition(label) => CompileAction::RegisterLabel(label),
        Statement::Command1("PUSH", arg) => CompileAction::OpCodeThunk(Box::new(move |table: &_| {
            Ok(OpCode::Push(resolve(arg, table)?))
        }) as Box<_>)
        .into(),
        Statement::Command0("ADD") => OpCode::Add.into(),
        Statement::Command0("DUMP_DEBUG") => OpCode::DumpDebug.into(),
        Statement::Command1("JMP", Argument::LiteralStr(arg)) => {
            OpCode::Jump(parse_jump_arg(arg)?).into()
        }
        Statement::Command0("JMP") => OpCode::Jump(ConditionFlags::EMPTY).into(),
        s => Err(CompilationError::UnrecognizedStatement(format!("{:?}", s)))?,
    };
    Ok(Some(action))
}

fn resolve(argument: &Argument, label_table: &LabelTable) -> Result<i64, CompilationError> {
    match argument {
        Argument::LiteralNumber(n) => Ok(*n),
        Argument::Reference(r) => label_table
            .get(r)
            .map(|index| *index as i64)
            .ok_or(CompilationError::UnresolvedReference(r.to_string())),
        Argument::LiteralStr(_) => unreachable!(),
    }
}

#[derive(Debug)]
pub enum CompilationError {
    UnresolvedReference(String),
    UnrecognizedStatement(String),
    UnrecognizedConditionFlags(String),
}

impl std::error::Error for CompilationError {}

impl std::fmt::Display for CompilationError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

// TODO(shelbyd): Don't parse in the compiler.
fn parse_jump_arg(arg: &str) -> Result<ConditionFlags, CompilationError> {
    if arg == "z" {
        Ok(ConditionFlags::ZERO)
    } else {
        Err(CompilationError::UnrecognizedConditionFlags(
            arg.to_string(),
        ))
    }
}
