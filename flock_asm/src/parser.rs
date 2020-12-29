use nom::{
    branch::alt,
    bytes::complete::{tag, take_while},
    character::complete::{alphanumeric1, digit1, line_ending, multispace0, multispace1},
    combinator::{all_consuming, map, opt, peek, recognize},
    multi::{separated_list0, separated_list1},
    sequence::{preceded, terminated, tuple},
    IResult,
};

use crate::statement::{Argument, Statement};

pub fn parse_asm(input: &str) -> IResult<&str, Vec<Statement>> {
    all_consuming(terminated(
        separated_list0(line_ending, single_statement),
        opt(line_ending),
    ))(input)
}

fn single_statement(input: &str) -> IResult<&str, Statement> {
    alt((
        empty_line,
        comment,
        label_definition,
        command_1_arg,
        command_0_arg,
    ))(input)
}

fn comment(input: &str) -> IResult<&str, Statement> {
    map(
        preceded(tag("#"), take_while(|c| c != '\n' && c != '\r')),
        |s: &str| Statement::Comment(s),
    )(input)
}

fn empty_line(input: &str) -> IResult<&str, Statement> {
    map(peek(line_ending), |_| Statement::EmptyLine)(input)
}

fn label_definition(input: &str) -> IResult<&str, Statement> {
    map(terminated(label, tag(":")), |s| {
        Statement::LabelDefinition(s)
    })(input)
}

fn label(input: &str) -> IResult<&str, &str> {
    ident(input)
}

fn command_0_arg(input: &str) -> IResult<&str, Statement> {
    map(tuple((multispace0, command)), |(_, command)| {
        Statement::Command0(command)
    })(input)
}

fn command_1_arg(input: &str) -> IResult<&str, Statement> {
    map(
        tuple((multispace0, command, multispace1, argument)),
        |(_, command, _, arg)| Statement::Command1(command, arg),
    )(input)
}

fn command(input: &str) -> IResult<&str, &str> {
    ident(input)
}

fn ident(input: &str) -> IResult<&str, &str> {
    recognize(separated_list1(tag("_"), alphanumeric1))(input)
}

fn argument(input: &str) -> IResult<&str, Argument> {
    map(digit1, |n: &str| Argument::Literal(n.parse::<i64>().unwrap()))(input)
}
