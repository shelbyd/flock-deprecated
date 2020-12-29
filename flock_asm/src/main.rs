use structopt::StructOpt;

mod parser;
use parser::parse_asm;

mod statement;

#[derive(StructOpt)]
struct Options {
    file: std::path::PathBuf,
}

type DynResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn main() -> DynResult<()> {
    let options = Options::from_args();
    let contents = {
        use std::io::Read;
        let mut file = std::fs::File::open(options.file)?;
        let mut string = String::new();
        file.read_to_string(&mut string)?;
        string
    };

    let asm_statements = match parse_asm(&contents) {
        Ok(s) => s,
        Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => {
            eprintln!("Parse Error:\n{:#?}", e);
            std::process::exit(1);
        }
        Err(nom::Err::Incomplete(_)) => {
            eprintln!("Incomplete Input");
            std::process::exit(1);
        }
    };

    dbg!(&asm_statements);

    Ok(())
}
