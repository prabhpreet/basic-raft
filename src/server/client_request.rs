use std::time::Duration;

use log::debug;
use tokio::sync::mpsc;

use crate::{
    server::{CandidateVars, ServerState, State, Term},
    types::{LogCommand, LogEntry},
};

use super::StateChange;

/* Leader i receives a client request to add v to the log.
ClientRequest(i, v) ==
    /\ state[i] = Leader
    /\ LET entry == [term  |-> currentTerm[i],
                     value |-> v]
           newLog == Append(log[i], entry)
       IN  log' = [log EXCEPT ![i] = newLog]
    /\ UNCHANGED <<messages, serverVars, candidateVars,
                   leaderVars, commitIndex>>
*/

pub(super) fn client_request(server_state: &mut ServerState, value: LogCommand) -> bool {
    match server_state.server_vars.state {
        State::Leader(_) => {
            let entry: LogEntry = LogEntry {
                term: server_state.server_vars.current_term,
                command: value.clone(),
            };
            server_state.log_vars.log.push(entry);

            debug!("{:?}: Client request, appending to log- {:?}", server_state.node, value);
            true
        }
        _ => false,
    }
}

pub(super) async fn client_request_loop(
    cs: mpsc::Sender<StateChange>,
    mut client_receiver: mpsc::Receiver<String>,
) {
    loop {
        if let Ok(value) =
            tokio::time::timeout(Duration::from_millis(100), client_receiver.recv()).await
        {
            match value {
                Some(value) => {
                    cs.send(StateChange::ClientRequest(LogCommand::Append(value)))
                        .await
                        .unwrap();
                }
                None => {
                    break;
                }
            }
        } else {
            cs.send(StateChange::Timeout).await.unwrap();
        }
    }
}
