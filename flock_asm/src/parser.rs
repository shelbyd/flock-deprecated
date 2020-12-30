use nom::{
    branch::alt,
    bytes::complete::{tag, take_while},
    character::complete::{
        alpha1, alphanumeric1, digit1, line_ending, multispace0, one_of, space0, space1,
    },
    combinator::{all_consuming, eof, map, opt, peek, recognize},
    multi::{separated_list0, separated_list1},
    sequence::{delimited, preceded, terminated, tuple},
    IResult,
};

use crate::statement::{Argument, Statement};

pub fn parse_asm(input: &str) -> IResult<&str, Vec<Statement>> {
    all_consuming(separated_list0(line_ending, single_statement))(input)
}

fn single_statement(input: &str) -> IResult<&str, Statement> {
    delimited(
        space0,
        alt((
            empty_line,
            comment,
            label_definition,
            command_2_arg,
            command_1_arg,
            command_0_arg,
        )),
        space0,
    )(input)
}

fn comment(input: &str) -> IResult<&str, Statement> {
    map(
        preceded(one_of("#;"), take_while(|c| c != '\n' && c != '\r')),
        |s: &str| Statement::Comment(s),
    )(input)
}

fn empty_line(input: &str) -> IResult<&str, Statement> {
    map(peek(alt((line_ending, eof))), |_| Statement::EmptyLine)(input)
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
        tuple((multispace0, command, space1, argument)),
        |(_, command, _, arg)| Statement::Command1(command, arg),
    )(input)
}

fn command(input: &str) -> IResult<&str, &str> {
    ident(input)
}

fn ident(input: &str) -> IResult<&str, &str> {
    recognize(separated_list1(tag("_"), alphanumeric1))(input)
}

fn command_2_arg(input: &str) -> IResult<&str, Statement> {
    map(
        tuple((
            multispace0,
            command,
            space1,
            argument,
            space0,
            tag(","),
            space0,
            argument,
        )),
        |(_, command, _, arg0, _, _, _, arg1)| Statement::Command2(command, arg0, arg1),
    )(input)
}

fn argument(input: &str) -> IResult<&str, Argument> {
    let literal_number = map(recognize(tuple((opt(tag("-")), digit1))), |n: &str| {
        Argument::LiteralNumber(n.parse::<i64>().unwrap())
    });
    let literal_str = map(alpha1, Argument::LiteralStr);
    let reference = map(preceded(tag("$"), ident), Argument::Reference);
    alt((literal_number, reference, literal_str))(input)
}
