use std::collections::VecDeque;
use std::sync::RwLock;

pub struct TaskQueue<T> {
    ready: RwLock<VecDeque<T>>,
    blocked: RwLock<VecDeque<T>>,
}

impl<T> TaskQueue<T> {
    pub fn new() -> Self {
        TaskQueue {
            ready: RwLock::new(VecDeque::new()),
            blocked: RwLock::new(VecDeque::new()),
        }
    }

    pub fn push(&self, item: T) {
        self.ready.write().unwrap().push_back(item);
    }

    pub fn push_blocked(&self, item: T) {
        self.blocked.write().unwrap().push_back(item);
    }

    pub fn task_finished(&self) {}

    pub fn next(&self) -> Option<T> {
        [&self.ready, &self.blocked]
            .iter()
            .filter_map(|q| q.write().unwrap().pop_front())
            .next()
    }
}
