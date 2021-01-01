use std::sync::Arc;

use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use lockfree::map::{Insertion, Map, Removed};

pub struct TaskQueue<T> {
    injector: Injector<T>,
    ready_stealers: Map<usize, Stealer<T>>,
    blocked_stealers: Map<usize, Stealer<T>>,
}

impl<T> TaskQueue<T> {
    pub fn new() -> Self {
        TaskQueue {
            injector: Injector::new(),
            ready_stealers: Map::new(),
            blocked_stealers: Map::new(),
        }
    }

    pub fn handle(self: Arc<Self>) -> Handle<T> {
        let ready_worker = Worker::new_lifo();
        insert_into(&self.ready_stealers, ready_worker.stealer());

        let blocked_worker = Worker::new_fifo();
        insert_into(&self.blocked_stealers, ready_worker.stealer());

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
                .iter()
                .filter_map(|entry| steal_into(worker, |_| entry.val().steal()))
                .next()
        };
        None.or_else(from_injector).or_else(from_shared_ready)
    }

    fn blocked_into(&self, worker: &Worker<T>) -> Option<T> {
        self.blocked_stealers
            .iter()
            .filter_map(|entry| steal_into(worker, |_| entry.val().steal()))
            .next()
    }
}

fn insert_into<T>(map: &Map<usize, T>, value: T) {
    use rand::Rng;

    let id = || rand::thread_rng().gen();

    let mut removed = match map.insert(id(), value) {
        None => return,
        Some(removed) => removed,
    };
    loop {
        match Removed::try_as_mut(&mut removed) {
            Some((k, _)) => {
                *k = id();
            }
            None => continue,
        }
        removed = match map.reinsert(removed) {
            Insertion::Created => return,
            Insertion::Failed(removed) => removed,
            Insertion::Updated(removed) => removed,
        };
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
        None.or_else(|| self.ready.pop())
            .or_else(|| self.queue.ready_into(&self.ready))
            .or_else(|| self.blocked.pop())
            .or_else(|| self.queue.blocked_into(&self.blocked))
    }
}
