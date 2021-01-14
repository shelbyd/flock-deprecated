use serde::*;
use std::collections::*;
use std::io::*;
use std::net::*;
use std::sync::{mpsc::*, *};

pub type Result<T> = std::result::Result<T, RpcError>;

pub struct Node<M> {
    messages: Receiver<Message<M>>,
    messages_tx: Sender<Message<M>>,
    peers: Arc<Mutex<HashMap<PeerId, Peer>>>,
}

fn spawn_stream_worker<'de, M: Deserialize<'de> + Send + 'static>(
    stream: TcpStream,
    message_sender: &Sender<Message<M>>,
    peers: &Mutex<HashMap<PeerId, Peer>>,
) -> Result<()> {
    let peer = Peer::new(stream.try_clone()?)?;
    let peer_id = peer.id();
    peers.lock().unwrap().insert(peer_id, peer);

    let message_tx = message_sender.clone();
    std::thread::spawn(move || {
        let de = serde_json::Deserializer::new(serde_json::de::IoRead::new(BufReader::new(stream)))
            .into_iter();
        for message in de {
            message_tx
                .send(Message {
                    peer: peer_id,
                    contents: message.unwrap(),
                })
                .unwrap();
        }
    });
    Ok(())
}

impl<'de, M: Deserialize<'de> + Serialize + Send + 'static> Node<M> {
    pub fn new(port: u16) -> Result<Node<M>> {
        let (message_tx, message_rx) = channel();

        let peers = Arc::new(Mutex::new(HashMap::new()));

        let thread_messages = message_tx.clone();
        let thread_peers = peers.clone();
        std::thread::spawn(move || {
            let listener = TcpListener::bind(("0.0.0.0", port)).unwrap();
            for stream in listener.incoming() {
                spawn_stream_worker(stream.unwrap(), &thread_messages, &thread_peers).unwrap();
            }
        });

        Ok(Node {
            messages: message_rx,
            messages_tx: message_tx,
            peers,
        })
    }

    pub fn connect(&mut self, s: &str) -> Result<()> {
        let stream = TcpStream::connect(s)?;
        spawn_stream_worker(stream, &self.messages_tx, &self.peers)?;

        Ok(())
    }

    pub fn broadcast(&mut self, message: M) -> Result<()> {
        for peer in self.peers.lock().unwrap().values_mut() {
            peer.send(&message)?;
        }
        Ok(())
    }

    pub fn messages(&mut self) -> impl Iterator<Item = Message<M>> + '_ {
        self.messages.iter()
    }
}

struct Peer {
    peer_addr: SocketAddr,
    writer: BufWriter<TcpStream>,
}

impl Peer {
    fn new(stream: TcpStream) -> Result<Peer> {
        Ok(Peer {
            peer_addr: stream.peer_addr()?,
            writer: BufWriter::new(stream),
        })
    }

    fn id(&self) -> PeerId {
        PeerId(self.peer_addr)
    }

    fn send<M: Serialize>(&mut self, message: &M) -> Result<()> {
        let mut ser = serde_json::Serializer::new(&mut self.writer);
        message.serialize(&mut ser)?;
        self.writer.flush()?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Message<M> {
    pub contents: M,
    pub peer: PeerId,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct PeerId(SocketAddr);

#[derive(Debug)]
pub enum RpcError {
    Io(std::io::Error),
    Parse(serde_json::Error),
}

impl From<std::io::Error> for RpcError {
    fn from(error: std::io::Error) -> RpcError {
        RpcError::Io(error)
    }
}

impl From<serde_json::Error> for RpcError {
    fn from(error: serde_json::Error) -> RpcError {
        RpcError::Parse(error)
    }
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        todo!()
    }
}

impl std::error::Error for RpcError {}
