use serde::{Deserialize, Serialize};

use crate::{ExecutionError, TaskOrder, Vm};
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
}

impl Cluster {
    pub fn connect() -> Cluster {
        let runtime = tokio::runtime::Runtime::new().unwrap();

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

        Cluster { runtime, peers }
    }

    pub(crate) fn run(&self, task_order: TaskOrder) -> Result<TaskOrder, RunError> {
        if self.peers.len() == 0 {
            return Err(RunError::CouldNotRun(task_order));
        }
        let from_remote = self.runtime.block_on(async {
            for peer in self.peers.iter() {
                let mut client = peer.clone();
                match client
                    .run_to_completion(tarpc::context::current(), task_order.clone())
                    .await
                {
                    Err(e) => {
                        dbg!(e);
                    }
                    Ok(Ok(to)) => return Some(Ok(to)),
                    Ok(Err(execution)) => return Some(Err(RunError::Execution(execution))),
                }
            }
            None
        });
        from_remote.unwrap_or_else(|| Err(RunError::CouldNotRun(task_order)))
    }
}

pub(crate) enum RunError {
    Execution(ExecutionError),
    CouldNotRun(TaskOrder),
}

#[tarpc::service]
trait ClusterService {
    async fn run_to_completion(task_order: TaskOrder) -> Result<TaskOrder, ExecutionError>;
}

#[derive(Clone)]
pub struct ClusterServer {
    vm: Arc<Vm>,
}

impl ClusterServer {
    pub fn new(vm: Vm) -> Self {
        ClusterServer { vm: Arc::new(vm) }
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
    ) -> Result<TaskOrder, ExecutionError> {
        let id = task_order.id;
        self.vm.enqueue(task_order);
        let mut interval = tokio::time::interval(core::time::Duration::from_millis(1));

        loop {
            interval.tick().await;
            if let Some(task_order) = self.vm.take_finished(id) {
                return task_order;
            }
        }
    }
}

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
