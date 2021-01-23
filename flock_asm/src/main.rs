mod compiler;
use compiler::to_bytecode;

mod parser;
use parser::parse_asm;

mod statement;

type DynResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn main() -> DynResult<()> {
    pretty_env_logger::init_timed();
    let args = gflags::parse_os();

    let contents = {
        use std::io::Read;
        let file_path = args
            .get(0)
            .expect("Must provide 1 positional argument as file to compile");
        let mut file = std::fs::File::open(file_path)?;
        let mut string = String::new();
        file.read_to_string(&mut string)?;
        string
    };

    let asm_statements = match parse_asm(&contents) {
        Ok(s) => s.1,
        Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => {
            log::error!("Parse Error:\n{:#?}", e);
            std::process::exit(1);
        }
        Err(nom::Err::Incomplete(_)) => {
            log::error!("Incomplete input");
            std::process::exit(1);
        }
    };

    let bytecode = to_bytecode(&asm_statements)?;

    flock_vm::run(bytecode)?;

    Ok(())
}
