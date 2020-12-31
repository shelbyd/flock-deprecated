use std::collections::VecDeque;
use std::sync::Mutex;

pub struct TaskQueue<T> {
    ready: Mutex<VecDeque<T>>,
    blocked: Mutex<VecDeque<T>>,
}

impl<T> TaskQueue<T> {
    pub fn new() -> Self {
        TaskQueue {
            ready: Mutex::new(VecDeque::new()),
            blocked: Mutex::new(VecDeque::new()),
        }
    }

    pub fn push(&self, item: T) {
        self.ready.lock().unwrap().push_back(item);
    }

    pub fn push_blocked(&self, item: T) {
        self.blocked.lock().unwrap().push_back(item);
    }

    pub fn next(&self) -> Option<T> {
        [&self.ready, &self.blocked]
            .iter()
            .filter_map(|q| q.lock().unwrap().pop_front())
            .next()
    }
}
