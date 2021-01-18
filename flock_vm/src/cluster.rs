use flock_rpc::Node;
use serde::{Deserialize, Serialize};

use crate::{ExecutionError, TaskOrder};

gflags::define! {
    --listen-port: u16 = 18454
}

gflags::define! {
    --remote-connection: &str
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    Test,
}

pub struct Cluster {
    node: Node<Message>,
}

impl Cluster {
    pub fn connect() -> Cluster {
        let mut node = Node::new(LISTEN_PORT.flag).unwrap();

        if REMOTE_CONNECTION.is_present() {
            node.connect(REMOTE_CONNECTION.flag).unwrap();
        }

        Cluster { node }
    }

    pub(crate) fn run(&self, task_order: TaskOrder) -> Result<TaskOrder, RunError> {
        Err(RunError::CouldNotRun(task_order))
    }
}

pub(crate) enum RunError {
    Execution(ExecutionError),
    CouldNotRun(TaskOrder),
}
