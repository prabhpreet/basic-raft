mod timeout;
mod client_request;
mod state_handle;
mod receive;


use std::collections::HashMap;

use crate::protocol::{Message, NodeBus, NodeReceiver, NodeSender};
use crate::types::{LogCommand, LogEntry, LogIndex, ServerID, Term};
use log::debug;
use tokio::sync::mpsc::{self, Receiver, Sender};

use self::client_request::client_request_loop;
use self::state_handle::state_handle;
use self::timeout::timeout_loop;

pub struct ServerInit {
    pub node: ServerID,
    pub all_servers: Vec<ServerID>,
    pub bus: Box<dyn NodeBus>,
    //Entries retrieved from permanent storage
    pub current_term: Term,
    pub voted_for: Option<ServerID>,
    pub log: Vec<LogEntry>,
}

pub struct ServerVars {
    //The server's term number
    pub current_term: Term,
    //The server's ID
    pub state: State,
    //The candidate the server voted for in the current term or None if it hasn't voted
    pub voted_for: Option<ServerID>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct LogVars {
    //Sequence of log entries
    log: Vec<LogEntry>,
    //Index of latest entry in the log the state machine can apply
    commit_index: LogIndex,
}

#[derive(Debug, PartialEq, Clone)]
pub enum State {
    Follower,
    Candidate(CandidateVars),
    Leader(LeaderVars),
}

impl State {
    pub fn is_follower(&self) -> bool {
        match self {
            State::Follower => true,
            _ => false,
        }
    }
    pub fn is_candidate(&self) -> bool {
        match self {
            State::Candidate(_) => true,
            _ => false,
        }
    }
    pub fn is_leader(&self) -> bool {
        match self {
            State::Leader(_) => true,
            _ => false,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct LeaderVars {
    //The next entry to send to each follower
    next_index: HashMap<ServerID,LogIndex>,
    //The latest entry that each follower has acknowledged the same as the leader
    match_index: HashMap<ServerID,LogIndex>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct CandidateVars {
    //The set of servers from which the candidate has received a RequestVote response in its currentTerm.
    votes_responded: Vec<ServerID>,
    //The set of servers from which the candidate has received a RequestVote response granting its vote.
    votes_granted: Vec<ServerID>,
}

pub struct ServerState {
    pub server_vars: ServerVars,
    pub log_vars: LogVars,
    pub all_servers: Vec<ServerID>,
    pub node: ServerID,
}

pub struct Server {
    node: ServerID,
    all_servers: Vec<ServerID>,
    bus: Box<dyn NodeBus>,
    bus_receiver_handle: tokio::task::JoinHandle<()>,
    state_handle: tokio::task::JoinHandle<()>,
    client_handle: tokio::task::JoinHandle<()>,
    timeout_handle: tokio::task::JoinHandle<()>,
    client_channel: mpsc::Sender<String>,
}

#[derive(Debug, PartialEq, Clone)]
enum StateChange {
    Timeout,
    ClientRequest(LogCommand),
    Receive((ServerID,Message))
}

impl Server {
    pub fn new(config: ServerInit) -> Server {
        /*
           \* Server i restarts from stable storage.
           \* It loses everything but its currentTerm, votedFor, and log.
           Restart(i) ==
               /\ state'          = [state EXCEPT ![i] = Follower]
               /\ votesResponded' = [votesResponded EXCEPT ![i] = {}]
               /\ votesGranted'   = [votesGranted EXCEPT ![i] = {}]
               /\ voterLog'       = [voterLog EXCEPT ![i] = [j \in {} |-> <<>>]]
               /\ nextIndex'      = [nextIndex EXCEPT ![i] = [j \in Server |-> 1]]
               /\ matchIndex'     = [matchIndex EXCEPT ![i] = [j \in Server |-> 0]]
               /\ commitIndex'    = [commitIndex EXCEPT ![i] = 0]
               /\ UNCHANGED <<messages, currentTerm, votedFor, log, elections>>
        */
                let server_state = ServerState {
            server_vars: ServerVars {
                current_term: config.current_term,
                state: State::Follower,
                voted_for: config.voted_for,
            },
            log_vars: LogVars {
                log: config.log,
                commit_index: LogIndex(0),
            },
            all_servers: config.all_servers.clone(),
            node: config.node,
        };

        let (change_sender, change_receiver) = mpsc::channel::<StateChange>(1000);
        let (client_channel, client_receiver) = mpsc::channel::<String>(1);
        let client_handle = tokio::spawn(client_request_loop(change_sender.clone(), client_receiver));

        let mut bus = config.bus;
        let (bus_sender, bus_receiver) = bus.register();
        let change_handle = tokio::spawn(state_handle(server_state, change_sender.clone(), change_receiver, bus_sender));

        let bus_receiver_handle = tokio::spawn(receive::receive_handle( bus_receiver, change_sender.clone()));
        let timeout_handle = tokio::spawn(timeout_loop(change_sender.clone()));

        Server {
            node: config.node,
            all_servers: config.all_servers,
            bus,
            bus_receiver_handle,
            state_handle: change_handle,
            client_handle,
            timeout_handle,
            client_channel,
        }
    }

    pub fn get_client_channel(&self) -> Sender<String> {
        self.client_channel.clone()
    }

}

impl Drop for Server {
    fn drop(&mut self) {
        debug!("Dropping server {:?}", self.node);
        self.state_handle.abort();
        self.client_handle.abort();
        self.timeout_handle.abort();
        self.bus_receiver_handle.abort();
    }
}

#[cfg(test)]
mod tests {
    use std::{env, thread::sleep, time::Duration};

    use log::debug;

    use super::*;
    use crate::{bus::TokioMulticastUdpBus, types::uindex};

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }
    #[tokio::test]
    async fn server() {
        init();
    
        debug!("Starting server");
        let server_ids = vec![ServerID(1), ServerID(2), ServerID(3), ServerID(4)];
        let mut servers = vec![];
        for (index,server) in server_ids.iter().enumerate() {
            let bus = Box::new(TokioMulticastUdpBus::new(
                "224.0.0.2:3000".parse().unwrap(),
                ServerID(index as uindex),
            ));
            let server = Server::new(ServerInit {
                node: ServerID(index as uindex),
                all_servers: server_ids.clone(),
                bus: bus,
                current_term: Term(0),
                voted_for: None,
                log: vec![],
            });
            servers.push(server);
        }

        tokio::time::sleep(Duration::from_secs(100)).await;
        drop(servers);

    }
}
