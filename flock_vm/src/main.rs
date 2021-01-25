use flock_vm::{cluster::ClusterServer, Vm};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    pretty_env_logger::init_timed();

    let vm = Vm::create_leaf();
    ClusterServer::new(&vm.handle()).listen().await?;

    Ok(())
}
