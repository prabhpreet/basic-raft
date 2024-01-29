use serde::{Serialize, Deserialize};
use tokio::sync::mpsc;
use crate::types::{Term, LogEntry, LogIndex, ServerID, uindex};

pub type NodeSender = mpsc::Sender<(MsgAddr, Message)>;
pub type NodeSenderReciever = mpsc::Receiver<(MsgAddr, Message)>;

pub type NodeReceiver = mpsc::Receiver<(ServerID,Message)>;
pub type NodeReceiverSender = mpsc::Sender<(ServerID,Message)>;

use tokio::sync::Mutex;
use std::sync::Arc;

pub trait NodeBus {
    fn register(&mut self) -> (NodeSender, NodeBusRxSender, Vec<tokio::task::JoinHandle<()>>);
}

pub type NodeBusRxSender = Arc<Mutex<Option<NodeReceiverSender>>>;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum MsgAddr {
    Node(ServerID),
    AllNodes
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct MessageEnvelope {
    pub mdest: MsgAddr,
    pub msource: ServerID,
    pub mtype: Message
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Message {
    //Candidate's term
    //Current term for candidate to update itself
    pub term: Term,
    pub payload: MessagePayload 
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum MessagePayload {
    Test(Test),
    //Invoked by candidates to gather votes
    RequestVoteRequest(RequestVoteRequest),
    RequestVoteResponse(RequestVoteResponse),
    AppendEntriesRequest(AppendEntriesRequest),
    AppendEntriesResponse(AppendEntriesResponse),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Test { }


#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct RequestVoteRequest {
    //Index of candidate's last log entry
    pub last_log_index: LogIndex,
    //Term of candidate's last log entry
    pub last_log_term: Term,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct RequestVoteResponse {
    //True means candidate received vote
    pub vote_granted: bool,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct AppendEntriesRequest {
    //So follower can redirect clients
    pub leader_id: ServerID,
    //Index of log entry immediately preceding new ones
    pub prev_log_index: LogIndex,
    //Term of prevLogIndex entry
    pub prev_log_term: Term,
    //Log entries to store (empty for heartbeat; may send more than one for efficiency)
    pub entries: Vec<LogEntry>,
    //Leader's commitIndex
    pub leader_commit: LogIndex,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct AppendEntriesResponse {
    //True if follower contained entry matching prevLogIndex and prevLogTerm
    pub success: bool,
    //Index of highest log entry known to be committed
    pub match_index: LogIndex,
}