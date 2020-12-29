#[derive(Debug, PartialEq, Eq)]
pub enum Statement {
    Comment(String),
    EmptyLine,
    LabelDefinition(String),
    Command0(String),
    Command1(String, Argument),
}

#[derive(Debug, PartialEq, Eq)]
pub enum Argument {
    Value(i64),
}
