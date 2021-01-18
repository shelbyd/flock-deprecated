use flock_vm::{cluster::ClusterServer, Vm};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let vm = Vm::create_leaf();
    ClusterServer::new(vm).listen().await?;

    Ok(())
}
