mod client_request;
mod receive;
mod state_handle;
mod timeout;

use crate::protocol::{Message, NodeBus};
use crate::types::{LogCommand, LogEntry, LogIndex, ServerConfig, ServerID, Term};
use log::debug;
use std::collections::HashMap;
use tokio::sync::mpsc::{self, Sender};

use self::client_request::client_request_loop;
use self::state_handle::state_handle;
use self::timeout::ElectionTimer;

pub struct ServerInit {
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
    next_index: HashMap<ServerID, LogIndex>,
    //The latest entry that each follower has acknowledged the same as the leader
    match_index: HashMap<ServerID, LogIndex>,
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
    client_handle: (mpsc::Sender<String>, Option<mpsc::Receiver<String>>),
}

#[derive(Debug, PartialEq, Clone)]
enum StateChange {
    ElectionTimeout,
    ClientRequest(LogCommand),
    Receive((ServerID, Message)),
}

impl Server {
    pub fn new(config: ServerConfig) -> Server {
        let (client_channel, client_receiver) = mpsc::channel::<String>(1);
        Server {
            node: config.node,
            all_servers: config.all_servers,
            client_handle: (client_channel, Some(client_receiver)),
        }
    }

    pub async fn run(&mut self, mut bus: Box<dyn NodeBus + Send>, init_config: ServerInit) {
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
                current_term: init_config.current_term,
                state: State::Follower,
                voted_for: init_config.voted_for,
            },
            log_vars: LogVars {
                log: init_config.log,
                commit_index: LogIndex(0),
            },
            all_servers: self.all_servers.clone(),
            node: self.node,
        };
        let (change_sender, change_receiver) = mpsc::channel::<StateChange>(1000);

        let client_receiver = self.client_handle.1.take().unwrap();
        let client_handle =
            tokio::spawn(client_request_loop(change_sender.clone(), client_receiver));

        let (bus_sender, bus_rx_sender, bus_tasks) = bus.register();
        let bus_receiver_handle = tokio::spawn(receive::receive_handle(
            bus_rx_sender,
            change_sender.clone(),
            self.node,
        ));

        let timer = ElectionTimer::new(change_sender.clone());

        let change_handle = tokio::spawn(state_handle(
            server_state,
            change_receiver,
            bus_sender,
            timer,
        ));

        debug!("Waiting on threads joining");
        while change_handle.is_finished() {
            tokio::time::sleep(std::time::Duration::from_millis(10000)).await;
            debug!("Server run alive");
        }
        //Wait on all threads
        tokio::join!(change_handle, bus_receiver_handle, client_handle);

        debug!("Waiting on bus tasks joining");
        for task in bus_tasks {
            task.await.unwrap();
        }

        debug!("End of run");
    }

    pub fn get_client_channel(&self) -> Sender<String> {
        self.client_handle.0.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::{net::Ipv4Addr, thread::sleep, time::Duration};

    use log::debug;

    use super::*;
    use crate::bus::TokioMulticastUdpBus;

    fn init(server_id: ServerID) {
        let port = 4000 + (server_id.0 as u16);
        println!("Starting debug server {} on port {}", server_id.0, port);
        console_subscriber::Builder::default().server_addr((Ipv4Addr::LOCALHOST, port)).build();
        let _ = env_logger::builder()
            //Format timestamp to include milliseconds
            .format_timestamp(Some(env_logger::TimestampPrecision::Millis))
            .is_test(true)
            .try_init();
    }
    #[test]
    fn server() {
        procspawn::init();
        println!("Starting server test");
        sleep(Duration::from_secs(10));

        let server_ids = vec![ServerID(1), ServerID(2), ServerID(3), ServerID(4)];
        let mut server_handles = vec![];
        for server_id in server_ids.iter() {
            let handle = procspawn::spawn(
                (server_id.clone(), server_ids.clone()),
                |(server_id, server_ids): (ServerID, Vec<ServerID>)| {
                    init(server_id);
                    debug!("Starting server {:?}", server_id);
                    //Tokio runtime create
                    let rt_out = tokio::runtime::Builder::new_multi_thread()
                        .enable_all()
                        .build()
                        .unwrap()
                        .block_on(async move {
                            let bus = Box::new(TokioMulticastUdpBus::new(
                                "224.0.0.2:3000".parse().unwrap(),
                                server_id,
                            ));
                            let mut server = Server::new(ServerConfig {
                                node: server_id,
                                all_servers: server_ids.clone(),
                            });
                            //Add a random sleep
                            //sleep(Duration::from_millis(rand::random::<u64>() % 5000));

                            server
                                .run(
                                    bus,
                                    ServerInit {
                                        current_term: Term(0),
                                        voted_for: None,
                                        log: vec![],
                                    },
                                )
                                .await;
                            debug!("Server {:?} exited", server_id);
                        });
                    debug!("Server {:?} exited with {:?}", server_id, rt_out );
                },
            );
            server_handles.push(handle);
        }

        let mut exit = false;

        loop {
            sleep(Duration::from_secs(10));
            debug!("Processes heartbeat");
            for (index, handle) in server_handles.iter().enumerate() {
                let pid = handle.pid();
                if pid.is_none() {
                    debug!("Server {} Process {:?} exited", index, pid);
                    exit = true;
                    break;
                }
                debug!("Server {} Process {:?} heartbeat", index, handle.pid());
            }
            if exit {
                break;
            }
        }

        for handle in server_handles {
            handle.join().unwrap();
        }
    }
}
