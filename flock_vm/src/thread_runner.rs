#![allow(unused)]

use std::any::Any;
use std::sync::mpsc;
use std::thread::{spawn, JoinHandle};

type Passed = Box<dyn Any + Send>;
type Thunk = Box<dyn FnOnce() -> Passed + Send>;

pub struct ThreadRunner {
    requests: mpsc::SyncSender<Thunk>,
    responses: mpsc::Receiver<Passed>,
    handle: JoinHandle<()>,
}

impl ThreadRunner {
    pub fn new() -> Self {
        let (requests_send, requests_receive) = mpsc::sync_channel::<Thunk>(0);
        let (responses_send, responses_receive) = mpsc::sync_channel(0);

        let thread = spawn(move || loop {
            let request = requests_receive.recv().unwrap();
            let result = request();
            responses_send.send(result).unwrap();
        });

        ThreadRunner {
            handle: thread,
            requests: requests_send,
            responses: responses_receive,
        }
    }

    pub fn run<'s, T: Clone + Send + 'static>(
        &'s self,
        req: impl FnOnce() -> T + Send + 's,
    ) -> Result<T, Box<dyn std::error::Error>> {
        self.requests.send(unsafe {
            let boxed: Box<dyn FnOnce() -> Passed + Send> = Box::new(|| Box::new(req()));
            std::mem::transmute::<_, Box<dyn FnOnce() -> Passed + Send + 'static>>(boxed)
        })?;
        let result = self.responses.recv()?;
        Ok(result.downcast_ref::<T>().unwrap().clone())
    }
}
