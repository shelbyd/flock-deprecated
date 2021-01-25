#![feature(thread_id_value)]

use flock_bytecode::ByteCode;

pub mod cluster;
use cluster::*;

mod task;
use task::*;

mod task_queue;
use task_queue::{ControlFlow, TaskQueue};

mod thread_runner;

use std::sync::Arc;

use dashmap::DashMap;

gflags::define! {
    pub --max-local-workers: usize = usize::MAX
}

pub fn run(bytecode: ByteCode) -> Result<(), ExecutionError> {
    let mut vm = Vm::create();
    let bytecode = Arc::new(bytecode);
    let bytecode_id = vm.register(&bytecode);

    vm.block_on_task(TaskOrder {
        id: 0,
        task: Task::new(),
        bytecode_id,
    })?;

    Ok(())
}

type FinishedMap = DashMap<usize, Result<TaskOrder, ExecutionError>>;
type ByteCodeMap = DashMap<u64, Arc<ByteCode>>;

pub struct VmHandle {
    queue_handle: task_queue::Handle<TaskOrder>,
    finished: FinishedMap,
    bytecode_registry: ByteCodeMap,
    memory: DashMap<u64, i64>,
}

impl VmHandle {
    fn new(queue: &TaskQueue<TaskOrder>) -> VmHandle {
        VmHandle {
            queue_handle: queue.handle(),
            finished: DashMap::new(),
            bytecode_registry: DashMap::new(),
            memory: DashMap::new(),
        }
    }
}

pub struct Vm {
    task_queue: TaskQueue<TaskOrder>,
    shared: Arc<VmHandle>,
    cluster: Option<Arc<Cluster>>,
    workers: Vec<std::thread::JoinHandle<()>>,
}

impl Vm {
    pub fn create() -> Vm {
        let task_queue = TaskQueue::new();
        let shared = Arc::new(VmHandle::new(&task_queue));
        Vm {
            cluster: Some(Arc::new(Cluster::connect(&shared))),
            shared,
            task_queue,
            workers: Vec::new(),
        }
        .spawn_workers()
    }

    pub fn create_leaf() -> Vm {
        let task_queue = TaskQueue::new();
        Vm {
            cluster: None,
            shared: Arc::new(VmHandle::new(&task_queue)),
            task_queue,
            workers: Vec::new(),
        }
        .spawn_workers()
    }

    pub fn handle(&self) -> Arc<VmHandle> {
        self.shared.clone()
    }

    fn register(&mut self, bytecode: &Arc<ByteCode>) -> u64 {
        self.shared.bytecode_registry.insert(0, bytecode.clone());
        0
    }

    fn block_on_task(&mut self, task_order: TaskOrder) -> Result<(), ExecutionError> {
        self.executor().run_to_completion(task_order)?;
        assert_eq!(self.shared.finished.len(), 0);

        Ok(())
    }

    fn spawn_workers(mut self) -> Self {
        let mut workers = Vec::new();

        let local_workers = std::cmp::min(num_cpus::get(), MAX_LOCAL_WORKERS.flag);
        workers.extend(
            (0..local_workers)
                .map(|_| self.executor())
                .map(|mut executor| std::thread::spawn(move || executor.run())),
        );

        workers.extend(
            self.cluster
                .iter()
                .flat_map(|cluster| cluster.peers())
                .map(|peer| self.remote_executor(peer))
                .map(|mut executor| std::thread::spawn(move || executor.run())),
        );

        self.workers = workers;
        self
    }

    fn executor(&self) -> Executor {
        Executor {
            handle: self.task_queue.handle(),
            shared: self.shared.clone(),
        }
    }

    fn remote_executor(&self, peer: Peer) -> RemoteExecutor {
        RemoteExecutor {
            handle: self.task_queue.handle(),
            shared: self.shared.clone(),
            peer,
        }
    }
}

impl Drop for Vm {
    fn drop(&mut self) {
        let workers = &mut self.workers;
        self.task_queue.finish(move || {
            for thread in workers.drain(..) {
                thread.join().unwrap();
            }
        });
    }
}

struct Executor {
    handle: task_queue::Handle<TaskOrder>,
    shared: Arc<VmHandle>,
}

impl Executor {
    fn run(&mut self) {
        while self.busy_tick() {}
    }

    fn busy_tick(&mut self) -> bool {
        let next = match self.handle.next() {
            ControlFlow::Continue(n) => n,
            ControlFlow::Finish => return false,
            ControlFlow::Retry => return true,
        };
        let id = next.id;

        let result = self.run_to_completion(next);
        let already_there = self.shared.finished.insert(id, result);
        assert!(already_there.is_none());
        true
    }

    fn run_to_completion(
        &mut self,
        mut task_order: TaskOrder,
    ) -> Result<TaskOrder, ExecutionError> {
        // TODO(shelbyd): Never overflow stack.
        loop {
            let bytecode = self
                .shared
                .bytecode_registry
                .get(&task_order.bytecode_id)
                .unwrap()
                .clone();
            match task_order.task.run(&bytecode)? {
                Execution::Terminated => {
                    return Ok(task_order);
                }
                Execution::Fork => {
                    use rand::Rng;

                    let mut forked = task_order.clone();
                    forked.id = rand::thread_rng().gen();
                    // TODO(shelbyd): Never generate duplicate ids.

                    forked.task.forked = true;
                    task_order.task.forked = false;

                    forked.task.stack.push(task_order.id as i64);
                    task_order.task.stack.push(forked.id as i64);

                    self.handle.push(forked);
                }
                Execution::Join { task_id, count } => {
                    let joined = self.busy_until_task_done(task_id)?;
                    let other_stack = &joined.task.stack;
                    let to_push = other_stack.split_at(other_stack.len() - count).1;
                    task_order.task.stack.extend(to_push.iter().cloned());
                }
                Execution::Store { addr, value } => {
                    self.shared.memory.insert(addr, value);
                }
                Execution::Load { addr } => {
                    task_order.task.stack.push(self.shared.memory.get(&addr).map(|ref_| *ref_.value()).unwrap_or(0));
                }
            }
        }
    }

    fn busy_until_task_done(&mut self, task_id: usize) -> Result<TaskOrder, ExecutionError> {
        let mut last_failed = false;
        loop {
            // TODO(shelbyd): Error with unrecognized task id.
            if let Some(done) = self.shared.finished.remove(&task_id) {
                return done.1;
            }
            if !self.busy_tick() {
                if last_failed {
                    return Err(ExecutionError::UnableToProgress);
                }
                last_failed = true;
            }
        }
    }
}

struct RemoteExecutor {
    handle: task_queue::Handle<TaskOrder>,
    shared: Arc<VmHandle>,
    peer: Peer,
}

impl RemoteExecutor {
    fn run(&mut self) {
        while let Some(task_order) = self.handle.wait_next() {
            let to_insert = match self.peer.try_run(&task_order) {
                Ok(finished) => Ok(finished),
                Err(RunError::Execution(e)) => Err(e),
                Err(RunError::ConnectionReset) => {
                    self.handle.push_nonworker(task_order);
                    log::warn!("Connection to peer {:?} lost", self.peer);
                    return;
                }
                Err(RunError::Unknown) => {
                    self.handle.push_nonworker(task_order);
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    continue;
                }
            };
            let already_there = self.shared.finished.insert(task_order.id, to_insert);
            assert!(already_there.is_none());
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
struct TaskOrder {
    id: usize,
    task: Task,
    bytecode_id: u64,
}
