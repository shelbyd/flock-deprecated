use std::collections::VecDeque;
use std::sync::Mutex;

pub struct TaskQueue<T> {
    deque: Mutex<VecDeque<T>>,
}

impl<T> TaskQueue<T> {
    pub fn new() -> Self {
        TaskQueue {
            deque: Mutex::new(VecDeque::new()),
        }
    }

    pub fn push(&self, item: T) {
        self.deque.lock().unwrap().push_back(item);
    }

    pub fn next(&self) -> Option<T> {
        self.deque.lock().unwrap().pop_front()
    }
}
