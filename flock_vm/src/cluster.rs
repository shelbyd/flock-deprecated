use serde::{Deserialize, Serialize};

use crate::{ExecutionError, TaskOrder, VmHandle};
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio_serde::formats::Json;

gflags::define! {
    pub --listen-port: u16 = 18454
}

gflags::define! {
    --remote-connections: &str
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    Test,
}

pub struct Cluster {
    runtime: Arc<Runtime>,
    peers: Vec<ClusterServiceClient>,
    vm: Arc<VmHandle>,
}

impl Cluster {
    pub fn connect(handle: &Arc<VmHandle>) -> Cluster {
        let runtime = Arc::new(tokio::runtime::Runtime::new().unwrap());

        runtime.spawn(ClusterServer::new(handle).listen());

        // TODO(shelbyd): Include client in Cluster upon new connection.
        let peers = runtime.block_on(async {
            if REMOTE_CONNECTIONS.is_present() {
                let mut clients = Vec::new();
                for addr in REMOTE_CONNECTIONS.flag.split(',') {
                    let transport = tarpc::serde_transport::tcp::connect(addr, Json::default)
                        .await
                        .unwrap();
                    let client =
                        ClusterServiceClient::new(tarpc::client::Config::default(), transport)
                            .spawn()
                            .unwrap();
                    clients.push(client);
                }
                clients
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

    pub(crate) fn peers(&self) -> Vec<Peer> {
        self.peers
            .iter()
            .map(|client| Peer {
                client: client.clone(),
                runtime: self.runtime.clone(),
                vm: self.vm.clone(),
            })
            .collect()
    }

    pub(crate) fn store(&self, addr: u64, value: i64) {
        log::debug!("Storing remotely {} @ {:x}", value, addr);
        for mut peer in self.peers() {
            loop {
                match peer.store(addr, value) {
                    Ok(()) => break,
                    Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => break,
                    Err(e) => {
                        log::error!("Store error: {}", e);
                    }
                }
            }
        }
    }
}

pub(crate) enum RunError {
    Execution(ExecutionError),
    ConnectionReset,
    Unknown,
}

pub struct Peer {
    client: ClusterServiceClient,
    runtime: Arc<Runtime>,
    vm: Arc<VmHandle>,
}

impl Peer {
    pub(crate) fn try_run(&mut self, task_order: &TaskOrder) -> Result<TaskOrder, RunError> {
        log::info!("Requesting remote execution of task {}", task_order.id);
        self.runtime.clone().block_on(async {
            match self.run_loop(&task_order).await {
                Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => {
                    Err(RunError::ConnectionReset)
                }
                Err(e) => {
                    log::error!("{}", e);
                    Err(RunError::Unknown)
                }
                Ok(Ok(to)) => Ok(to),
                Ok(Err(e)) => Err(RunError::Execution(e)),
            }
        })
    }

    async fn run_loop(
        &mut self,
        task_order: &TaskOrder,
    ) -> std::io::Result<Result<TaskOrder, ExecutionError>> {
        loop {
            use std::time::*;
            let mut context = tarpc::context::current();
            context.deadline = SystemTime::now() + Duration::from_secs(300);
            match self
                .client
                .run_to_completion(context, task_order.clone())
                .await?
            {
                Ok(result) => return Ok(result),
                Err(UnknownByteCode(id)) => {
                    let bytecode = self.vm.bytecode_registry.get(&id).unwrap().as_ref().clone();
                    self.client
                        .define_bytecode(tarpc::context::current(), id, bytecode)
                        .await?;
                }
            }
        }
    }

    fn store(&mut self, addr: u64, value: i64) -> std::io::Result<()> {
        self.runtime.clone().block_on(async {
            self.client
                .store(tarpc::context::current(), addr, value)
                .await?;
            Ok(())
        })
    }
}

impl std::fmt::Debug for Peer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.client.fmt(formatter)
    }
}

#[tarpc::service]
trait ClusterService {
    async fn run_to_completion(
        task_order: TaskOrder,
    ) -> Result<Result<TaskOrder, ExecutionError>, UnknownByteCode>;

    async fn define_bytecode(id: u64, bytecode: flock_bytecode::ByteCode);

    async fn store(addr: u64, value: i64);
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
        log::info!("Requested to execute task {}", task_order.id);
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

    async fn store(self, _: tarpc::context::Context, addr: u64, value: i64) {
        log::debug!("Storing from remote {} @ 0x{:x}", value, addr);
        self.vm.memory.insert(addr, value);
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
