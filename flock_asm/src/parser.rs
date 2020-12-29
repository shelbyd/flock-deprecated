use nom::{
    branch::alt,
    bytes::complete::{tag, take_while},
    character::complete::{digit1, line_ending, multispace0, multispace1, not_line_ending},
    character::is_alphanumeric,
    combinator::{all_consuming, complete, map, peek, rest},
    multi::separated_list0,
    sequence::{preceded, terminated, tuple},
    IResult,
};

use crate::statement::{Argument, Statement};

pub fn parse_asm(input: &str) -> IResult<&str, Vec<Statement>> {
    all_consuming(separated_list0(line_ending, single_statement))(input)
}

fn single_statement(input: &str) -> IResult<&str, Statement> {
    alt((
        comment,
        empty_line,
        label_definition,
        command_1_arg,
        command_0_arg,
    ))(input)
}

fn comment(input: &str) -> IResult<&str, Statement> {
    map(
        preceded(tag("#"), take_while(|c| c != '\n' && c != '\r')),
        |s: &str| Statement::Comment(s.to_string()),
    )(input)
}

fn empty_line(input: &str) -> IResult<&str, Statement> {
    map(peek(line_ending), |_| Statement::EmptyLine)(input)
}

fn label_definition(input: &str) -> IResult<&str, Statement> {
    map(terminated(label, tag(":")), |s| {
        Statement::LabelDefinition(s.to_string())
    })(input)
}

fn label(input: &str) -> IResult<&str, &str> {
    take_while(|c: char| is_alphanumeric(c as u8) || c == '_')(input)
}

fn command_0_arg(input: &str) -> IResult<&str, Statement> {
    map(tuple((multispace0, command)), |(_, command)| {
        Statement::Command0(command.to_string())
    })(input)
}

fn command_1_arg(input: &str) -> IResult<&str, Statement> {
    map(
        tuple((multispace0, command, multispace1, argument)),
        |(_, command, _, arg)| Statement::Command1(command.to_string(), arg),
    )(input)
}

fn command(input: &str) -> IResult<&str, &str> {
    take_while(|c| is_alphanumeric(c as u8) || c == '_')(input)
}

fn argument(input: &str) -> IResult<&str, Argument> {
    map(digit1, |n: &str| Argument::Value(n.parse::<i64>().unwrap()))(input)
}
