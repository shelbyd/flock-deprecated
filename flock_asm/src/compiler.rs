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
            Some(CompileAction::RegisterValue(label, value)) => {
                label_table.insert(label, value as usize);
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
    RegisterValue(&'s str, i64),
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
        Statement::ValueDeclaration(label, value) => CompileAction::RegisterValue(label, *value),
        Statement::Command1("PUSH", arg) => {
            thunk(move |table| Ok(OpCode::Push(resolve(arg, table)?)))
        }
        Statement::Command0("ADD") => OpCode::Add.into(),
        Statement::Command0("DUMP_DEBUG") => OpCode::DumpDebug.into(),
        Statement::Command0("JMP") => OpCode::Jump(ConditionFlags::EMPTY, None).into(),
        Statement::Command1("JMP", Argument::LiteralStr(arg)) => {
            OpCode::Jump(parse_jump_arg(arg)?, None).into()
        }
        Statement::Command1("JMP", ref_ @ Argument::Reference(_)) => thunk(move |table| {
            Ok(OpCode::Jump(
                ConditionFlags::EMPTY,
                Some(resolve(ref_, table)?),
            ))
        }),
        Statement::Command2("JMP", Argument::LiteralStr(arg), ref_ @ Argument::Reference(_)) => {
            thunk(move |table| {
                let target = Some(resolve(ref_, table)?);
                let flags = parse_jump_arg(arg)?;
                Ok(OpCode::Jump(flags, target))
            })
        }
        Statement::Command0("JSR") => OpCode::JumpToSubroutine(None).into(),
        Statement::Command1("JSR", ref_ @ Argument::Reference(_)) => {
            thunk(move |table| Ok(OpCode::JumpToSubroutine(Some(resolve(ref_, table)?))))
        }
        Statement::Command1("BURY", arg) => {
            thunk(move |table| Ok(OpCode::Bury(resolve(arg, table)?)))
        }
        Statement::Command1("DREDGE", arg) => {
            thunk(move |table| Ok(OpCode::Dredge(resolve(arg, table)?)))
        }
        Statement::Command0("DUP") => OpCode::Duplicate.into(),
        Statement::Command0("RET") => OpCode::Return.into(),
        Statement::Command0("POP") => OpCode::Pop.into(),
        Statement::Command0("FORK") => OpCode::Fork.into(),
        Statement::Command1("JOIN", Argument::LiteralNumber(n)) => OpCode::Join(*n).into(),
        Statement::Command0("HALT") => OpCode::Halt.into(),
        Statement::Command1("STORE", arg) => {
            thunk(move |table| Ok(OpCode::Store(resolve(arg, table)? as u64)))
        }
        Statement::Command1("STORE_REL", arg) => {
            thunk(move |table| Ok(OpCode::StoreRelative(resolve(arg, table)? as u64)))
        }
        Statement::Command1("LOAD", arg) => {
            thunk(move |table| Ok(OpCode::Load(resolve(arg, table)? as u64)))
        }
        Statement::Command1("LOAD_REL", arg) => {
            thunk(move |table| Ok(OpCode::LoadRelative(resolve(arg, table)? as u64)))
        }
        Statement::Command0("PANIC") => OpCode::Panic.into(),
        s => Err(CompilationError::UnrecognizedStatement(format!("{:?}", s)))?,
    };
    Ok(Some(action))
}

fn thunk<'s>(
    func: impl FnOnce(&LabelTable<'s>) -> Result<OpCode, CompilationError> + 's,
) -> CompileAction<'s> {
    CompileAction::OpCodeThunk(Box::new(func))
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
    arg.chars()
        .map(|c| match c {
            'z' => Ok(ConditionFlags::ZERO),
            'f' => Ok(ConditionFlags::FORK),
            c => Err(CompilationError::UnrecognizedConditionFlags(c.to_string())),
        })
        .fold(Ok(ConditionFlags::EMPTY), |flags, c| Ok(flags? | c?))
}
