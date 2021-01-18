use flock_vm::cluster::ClusterServer;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    ClusterServer.listen().await?;

    Ok(())
}
