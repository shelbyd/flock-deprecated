#[derive(Debug)]
pub struct ByteCode {
    opcodes: Vec<OpCode>,
}

impl ByteCode {
    pub fn get(&self, index: usize) -> Option<&OpCode> {
        self.opcodes.get(index)
    }

    pub fn surrounding(
        &self,
        index: usize,
        bounds: usize,
    ) -> impl Iterator<Item = (usize, &OpCode)> {
        let start = index.saturating_sub(bounds);
        let end = usize::min(index.saturating_add(bounds), self.opcodes.len() - 1);
        (start..=end).map(move |i| (i, &self.opcodes[i]))
    }
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
