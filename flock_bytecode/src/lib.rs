#[derive(Debug)]
pub struct ByteCode {
    opcodes: Vec<OpCode>,
}

impl From<Vec<OpCode>> for ByteCode {
    fn from(opcodes: Vec<OpCode>) -> ByteCode {
        ByteCode { opcodes }
    }
}

#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum OpCode {
    Push(i64),
    Add,
    DumpDebug,
}
