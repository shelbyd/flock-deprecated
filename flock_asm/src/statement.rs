#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Statement<'s> {
    Comment(&'s str),
    EmptyLine,
    LabelDefinition(&'s str),
    Command0(&'s str),
    Command1(&'s str, Argument),
}

#[derive(Debug, PartialEq, Eq)]
pub enum Argument {
    Literal(i64),
}
