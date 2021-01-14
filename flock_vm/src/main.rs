use flock_vm::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut node = flock_rpc::Node::<Message>::new(18454)?;
    for message in node.messages() {
        dbg!(message);
    }

    Ok(())
}
