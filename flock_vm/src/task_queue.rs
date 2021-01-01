use std::sync::{Arc, RwLock};

use crossbeam_deque::{Injector, Steal, Stealer, Worker};

pub struct TaskQueue<T> {
    injector: Injector<T>,
    ready_stealers: RwLock<Vec<Stealer<T>>>,
    blocked_stealers: RwLock<Vec<Stealer<T>>>,
}

impl<T> TaskQueue<T> {
    pub fn new() -> Self {
        TaskQueue {
            injector: Injector::new(),
            ready_stealers: RwLock::new(Vec::new()),
            blocked_stealers: RwLock::new(Vec::new()),
        }
    }

    pub fn handle(self: Arc<Self>) -> Handle<T> {
        let ready_worker = Worker::new_lifo();
        self.ready_stealers
            .write()
            .unwrap()
            .push(ready_worker.stealer());

        let blocked_worker = Worker::new_lifo();
        self.blocked_stealers
            .write()
            .unwrap()
            .push(blocked_worker.stealer());

        Handle {
            queue: self.clone(),
            ready: ready_worker,
            blocked: blocked_worker,
        }
    }

    pub fn push(&self, item: T) {
        self.injector.push(item);
    }

    fn ready_into(&self, worker: &Worker<T>) -> Option<T> {
        let from_injector = || steal_into(worker, |_| self.injector.steal());
        let from_shared_ready = || {
            self.ready_stealers
                .read()
                .unwrap()
                .iter()
                .filter_map(|s| steal_into(worker, |_| s.steal()))
                .next()
        };
        None.or_else(from_injector).or_else(from_shared_ready)
    }

    fn blocked_into(&self, worker: &Worker<T>) -> Option<T> {
        self.blocked_stealers
            .read()
            .unwrap()
            .iter()
            .filter_map(|s| steal_into(worker, |_| s.steal()))
            .next()
    }
}

fn steal_into<T>(worker: &Worker<T>, mut stealer: impl FnMut(&Worker<T>) -> Steal<T>) -> Option<T> {
    loop {
        match stealer(worker) {
            Steal::Success(v) => return Some(v),
            Steal::Empty => return None,
            Steal::Retry => {}
        }
    }
}

pub struct Handle<T> {
    queue: Arc<TaskQueue<T>>,
    ready: Worker<T>,
    blocked: Worker<T>,
}

impl<T> Handle<T> {
    pub fn push(&self, item: T) {
        self.ready.push(item);
    }

    pub fn push_blocked(&self, item: T) {
        self.blocked.push(item);
    }

    pub fn task_finished(&mut self) {}

    pub fn next(&mut self) -> Option<T> {
        self.ready
            .pop()
            .or_else(|| self.queue.ready_into(&self.ready))
            .or_else(|| self.blocked.pop())
            .or_else(|| self.queue.blocked_into(&self.blocked))
    }
}
