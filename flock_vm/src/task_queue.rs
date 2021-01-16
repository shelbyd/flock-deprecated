use std::collections::VecDeque;

use flume::*;

pub struct TaskQueue<T> {
    sender: Sender<T>,
    receiver: Receiver<T>,
}

impl<T> TaskQueue<T> {
    pub fn new() -> Self {
        let (sender, receiver) = flume::unbounded();
        TaskQueue { sender, receiver }
    }

    pub fn handle(&self) -> Handle<T> {
        Handle {
            local_work: VecDeque::new(),
            sender: self.sender.clone(),
            receiver: self.receiver.clone(),
        }
    }
}

pub struct Handle<T> {
    local_work: VecDeque<T>,
    sender: Sender<T>,
    receiver: Receiver<T>,
}

impl<T> Handle<T> {
    pub fn push(&mut self, item: T) {
        self.local_work.push_back(item);
        if let Some(amount) = self.push_to_shared() {
            for work in self.local_work.drain(..amount) {
                self.sender.send(work).unwrap();
            }
        }
    }

    fn push_to_shared(&mut self) -> Option<usize> {
        if self.local_work.len() > self.sender.len() * 2 {
            Some(self.local_work.len() / 2)
        } else {
            None
        }
    }

    pub fn next(&mut self) -> Option<T> {
        if let Some(local) = self.local_work.pop_back() {
            return Some(local);
        }
        // TODO(shelbyd): Do something intelligent when all workers are done.
        self.receiver.recv_timeout(std::time::Duration::from_millis(10)).ok()
    }
}
