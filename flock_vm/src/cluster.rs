use serde::{Deserialize, Serialize};

use crate::{ExecutionError, TaskOrder, VmHandle};
use std::sync::Arc;
use tokio_serde::formats::Json;

gflags::define! {
    pub --listen-port: u16 = 18454
}

gflags::define! {
    --remote-connection: &str
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    Test,
}

pub struct Cluster {
    runtime: tokio::runtime::Runtime,
    peers: Vec<ClusterServiceClient>,
    vm: Arc<VmHandle>,
}

impl Cluster {
    pub fn connect(handle: &Arc<VmHandle>) -> Cluster {
        let runtime = tokio::runtime::Runtime::new().unwrap();

        runtime.spawn(ClusterServer::new(handle).listen());

        let peers = runtime.block_on(async {
            if REMOTE_CONNECTION.is_present() {
                let to_cluster_server =
                    tarpc::serde_transport::tcp::connect(REMOTE_CONNECTION.flag, Json::default)
                        .await
                        .unwrap();
                let client =
                    ClusterServiceClient::new(tarpc::client::Config::default(), to_cluster_server)
                        .spawn()
                        .unwrap();
                vec![client]
            } else {
                Vec::new()
            }
        });

        Cluster {
            runtime,
            peers,
            vm: handle.clone(),
        }
    }

    pub(crate) fn run(&self, task_order: TaskOrder) -> Result<TaskOrder, RunError> {
        let from_remote = self.runtime.block_on(async {
            for peer in self.peers.iter() {
                eprintln!("Requesting remote execution of task {}", task_order.id);
                match self.run_on_peer(&task_order, peer.clone()).await {
                    Err(e) => {
                        dbg!(e);
                    }
                    Ok(Ok(to)) => return Some(Ok(to)),
                    Ok(Err(e)) => return Some(Err(RunError::Execution(e))),
                }
            }
            None
        });
        from_remote.unwrap_or_else(|| Err(RunError::CouldNotRun(task_order)))
    }

    async fn run_on_peer(
        &self,
        task_order: &TaskOrder,
        mut client: ClusterServiceClient,
    ) -> std::io::Result<Result<TaskOrder, ExecutionError>> {
        loop {
            use std::time::*;
            let mut context = tarpc::context::current();
            context.deadline = SystemTime::now() + Duration::from_secs(300);
            match client
                .run_to_completion(context, task_order.clone())
                .await?
            {
                Ok(result) => return Ok(result),
                Err(UnknownByteCode(id)) => {
                    let bytecode = self.vm.bytecode_registry.get(&id).unwrap().as_ref().clone();
                    client
                        .define_bytecode(tarpc::context::current(), id, bytecode)
                        .await?;
                }
            }
        }
    }
}

pub(crate) enum RunError {
    Execution(ExecutionError),
    CouldNotRun(TaskOrder),
}

#[tarpc::service]
trait ClusterService {
    async fn run_to_completion(
        task_order: TaskOrder,
    ) -> Result<Result<TaskOrder, ExecutionError>, UnknownByteCode>;

    async fn define_bytecode(id: u64, bytecode: flock_bytecode::ByteCode);
}

#[derive(Clone)]
pub struct ClusterServer {
    vm: Arc<VmHandle>,
}

impl ClusterServer {
    pub fn new(vm: &Arc<VmHandle>) -> Self {
        ClusterServer { vm: vm.clone() }
    }

    pub async fn listen(self) -> std::io::Result<()> {
        use futures::*;
        use tarpc::{
            server::{Channel, Handler},
            *,
        };
        let mut listener =
            tarpc::serde_transport::tcp::listen(("0.0.0.0", LISTEN_PORT.flag), Json::default)
                .await?;
        listener.config_mut().max_frame_length(4294967296);

        listener
            .filter_map(|r| future::ready(r.ok()))
            .map(server::BaseChannel::with_defaults)
            .max_channels_per_key(1, |t| t.as_ref().peer_addr().unwrap().ip())
            .map(|channel| channel.respond_with(self.clone().serve()).execute())
            .buffer_unordered(10)
            .for_each(|_| async {})
            .await;
        Ok(())
    }
}

#[tarpc::server]
impl ClusterService for ClusterServer {
    async fn run_to_completion(
        self,
        _: tarpc::context::Context,
        task_order: TaskOrder,
    ) -> Result<Result<TaskOrder, ExecutionError>, UnknownByteCode> {
        eprintln!("Requested to execute task {}", task_order.id);
        if !self
            .vm
            .bytecode_registry
            .contains_key(&task_order.bytecode_id)
        {
            // TODO(shelbyd): Request ByteCode from client.
            return Err(UnknownByteCode(task_order.bytecode_id));
        }
        let id = task_order.id;
        self.vm.queue_handle.push_nonworker(task_order);
        let mut interval = tokio::time::interval(core::time::Duration::from_millis(1));

        loop {
            interval.tick().await;
            if let Some(task_order) = self.vm.finished.remove(&id) {
                return Ok(task_order.1);
            }
        }
    }

    async fn define_bytecode(
        self,
        _: tarpc::context::Context,
        id: u64,
        bytecode: flock_bytecode::ByteCode,
    ) {
        self.vm.bytecode_registry.insert(id, Arc::new(bytecode));
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct UnknownByteCode(u64);

trait AwaitBlock {
    type Output;

    fn await_block(self) -> Self::Output;
}

impl<F> AwaitBlock for F
where
    F: core::future::Future,
{
    type Output = <Self as core::future::Future>::Output;

    fn await_block(self) -> Self::Output {
        lazy_static::lazy_static! {
            static ref RUNTIME: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
        }
        RUNTIME.block_on(self)
    }
}
