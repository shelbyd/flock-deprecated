use std::collections::VecDeque;
use std::num::NonZeroU64;
use std::sync::RwLock;

use lockfree::map::Map;

pub struct TaskQueue<T> {
    thread_queues: Map<NonZeroU64, SingleQueue<T>>,
}

impl<T> TaskQueue<T> {
    pub fn new() -> Self {
        TaskQueue {
            thread_queues: Map::new(),
        }
    }

    pub fn push(&self, item: T) {
        self.with_current(|current| {
            current.ready.write().unwrap().push_back(item);
        });
    }

    fn with_current<U>(&self, cb: impl FnOnce(&SingleQueue<T>) -> U) -> U {
        let current_id = std::thread::current().id().as_u64();

        if let Some(q) = self.thread_queues.get(&current_id) {
            return cb(q.val());
        }

        let new = SingleQueue::new();
        let result = cb(&new);
        self.thread_queues.insert(current_id, new);
        result
    }

    pub fn push_blocked(&self, item: T) {
        self.with_current(|current| {
            current.blocked.write().unwrap().push_back(item);
        });
    }

    pub fn task_finished(&self) {}

    pub fn next(&self) -> Option<T> {
        let from_current = self.with_current(|current| current.next_ready_back());
        let with_other_ready = from_current.or_else(|| {
            let result = self
                .thread_queues
                .iter()
                .filter(|entry| *entry.key() != std::thread::current().id().as_u64())
                .filter_map(|entry| entry.val().next_ready_front())
                .next();
            result
        });
        let with_current_blocked =
            with_other_ready.or_else(|| self.with_current(|c| c.next_blocked()));

        let current_id = std::thread::current().id().as_u64();
        let with_other_blocked = with_current_blocked.or_else(|| {
            let result = self
                .thread_queues
                .iter()
                .filter(|entry| *entry.key() != current_id)
                .filter_map(|entry| entry.val().next_blocked())
                .next();
            result
        });
        with_other_blocked
    }
}

struct SingleQueue<T> {
    ready: RwLock<VecDeque<T>>,
    blocked: RwLock<VecDeque<T>>,
}

impl<T> SingleQueue<T> {
    fn new() -> Self {
        SingleQueue {
            ready: RwLock::new(VecDeque::new()),
            blocked: RwLock::new(VecDeque::new()),
        }
    }

    fn next_ready_back(&self) -> Option<T> {
        if self.ready.read().unwrap().len() == 0 {
            return None;
        }
        self.ready.write().unwrap().pop_back()
    }

    fn next_ready_front(&self) -> Option<T> {
        self.ready.write().unwrap().pop_front()
    }

    fn next_blocked(&self) -> Option<T> {
        self.blocked.write().unwrap().pop_front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::thread_runner::*;

    #[test]
    fn push_one_task_gives_it() {
        let q = TaskQueue::<usize>::new();
        q.push(42);
        assert_eq!(q.next(), Some(42));
    }

    #[test]
    fn push_blocked_task_gives_it() {
        let q = TaskQueue::<usize>::new();
        q.push_blocked(42);
        assert_eq!(q.next(), Some(42));
    }

    #[test]
    fn push_gives_unblocked_before_blocked() {
        let q = TaskQueue::<usize>::new();
        q.push_blocked(0);
        q.push(42);
        q.push_blocked(1);
        assert_eq!(q.next(), Some(42));
    }

    #[test]
    fn tasks_available_for_other_thread() {
        let thread0 = ThreadRunner::new();
        let thread1 = ThreadRunner::new();
        let q = TaskQueue::<usize>::new();

        thread0.run(|| q.push(42)).unwrap();

        assert_eq!(thread1.run(|| q.next()).unwrap(), Some(42));
    }

    #[test]
    fn thread_gets_own_task() {
        let thread0 = ThreadRunner::new();
        let thread1 = ThreadRunner::new();
        let q = TaskQueue::<usize>::new();

        thread0.run(|| q.push(0)).unwrap();
        thread1.run(|| q.push(42)).unwrap();
        thread0.run(|| q.push(1)).unwrap();

        assert_eq!(thread1.run(|| q.next()).unwrap(), Some(42));
    }

    #[test]
    fn prefer_ready_on_other_thread_before_blocked_on_ours() {
        let thread0 = ThreadRunner::new();
        let thread1 = ThreadRunner::new();
        let q = TaskQueue::<usize>::new();

        thread0.run(|| q.push(42)).unwrap();
        thread1.run(|| q.push_blocked(0)).unwrap();

        assert_eq!(thread1.run(|| q.next()).unwrap(), Some(42));
    }

    #[test]
    fn our_threads_pulls_from_back() {
        let q = TaskQueue::<usize>::new();

        q.push(0);
        q.push(1);
        q.push(2);

        assert_eq!(q.next(), Some(2));
    }

    #[test]
    fn other_threads_pull_from_front() {
        let other_thread = ThreadRunner::new();
        let q = TaskQueue::<usize>::new();

        q.push(0);
        q.push(1);
        q.push(2);

        assert_eq!(other_thread.run(|| q.next()).unwrap(), Some(0));
    }
}
