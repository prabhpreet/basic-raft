use std::net::SocketAddr;

#[allow(non_camel_case_types)]
pub type uindex = u128;

use log::Log;
use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ServerID(pub uindex);

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct ServerConfig {
    pub node: ServerID,
    pub all_servers: Vec<ServerID>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
//Time period of arbitrary length, logical clock 
//Increases monotonically over time
pub struct Term(pub uindex);

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum LogCommand {
    Heartbeat,
    Append(String),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct LogEntry {
    pub term: Term,
    pub command: LogCommand,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy, PartialOrd, Eq, Ord)]
pub struct LogIndex(pub uindex);
