use std::collections::VecDeque;

use flume::*;

pub struct TaskQueue<T> {
    sender: Sender<ControlFlow<T>>,
    receiver: Receiver<ControlFlow<T>>,
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

    pub fn finish<F: FnOnce() -> R, R>(&self, task_closer: F) -> R {
        self.sender.send(ControlFlow::Finish).unwrap();
        let result = task_closer();
        loop {
            match self
                .receiver
                .recv_timeout(std::time::Duration::from_secs(1))
            {
                Ok(ControlFlow::Finish) => break,
                Ok(ControlFlow::Continue(_)) => {}
                _ => unreachable!(),
            }
        }
        result
    }
}

#[derive(Debug)]
pub enum ControlFlow<T> {
    Continue(T),
    Retry,
    Finish,
}

pub struct Handle<T> {
    local_work: VecDeque<T>,
    sender: Sender<ControlFlow<T>>,
    receiver: Receiver<ControlFlow<T>>,
}

impl<T> Handle<T> {
    pub fn push(&mut self, item: T) {
        self.local_work.push_back(item);
        if let Some(amount) = self.push_to_shared() {
            log::info!("Sending {} items to machine shared work pool", amount);
            for work in self.local_work.drain(..amount) {
                self.sender.send(ControlFlow::Continue(work)).unwrap();
            }
        }
    }

    pub fn push_nonworker(&self, item: T) {
        self.sender.send(ControlFlow::Continue(item)).unwrap();
    }

    fn push_to_shared(&mut self) -> Option<usize> {
        if self.local_work.len() > self.sender.len() * 2 {
            let amount = std::cmp::max(1, self.local_work.len() / 2);
            Some(amount)
        } else {
            None
        }
    }

    pub fn next(&mut self) -> ControlFlow<T> {
        if let Some(local) = self.local_work.pop_back() {
            return ControlFlow::Continue(local);
        }

        match self
            .receiver
            .recv_timeout(std::time::Duration::from_millis(1))
        {
            Ok(ControlFlow::Continue(t)) => ControlFlow::Continue(t),
            Ok(ControlFlow::Finish) => {
                self.sender.send(ControlFlow::Finish).unwrap();
                ControlFlow::Finish
            }
            Ok(ControlFlow::Retry) => unreachable!(),
            Err(RecvTimeoutError::Timeout) => ControlFlow::Retry,
            Err(RecvTimeoutError::Disconnected) => ControlFlow::Finish,
        }
    }

    pub fn wait_next(&mut self) -> Option<T> {
        loop {
            match self.next() {
                ControlFlow::Retry => {}
                ControlFlow::Finish => return None,
                ControlFlow::Continue(t) => return Some(t),
            }
        }
    }
}
