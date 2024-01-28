use std::time::Duration;

use log::{debug,error};
use tokio::sync::{mpsc, oneshot::error};

use crate::server::{ServerState, State, CandidateVars, Term};

use super::StateChange;
pub(super) fn timeout(server_state: &mut ServerState) -> bool {
    /*
    Timeout(i) == /\ state[i] \in {Follower, Candidate}
          /\ state' = [state EXCEPT ![i] = Candidate]
          /\ currentTerm' = [currentTerm EXCEPT ![i] = currentTerm[i] + 1]
          \* Most implementations would probably just set the local vote
          \* atomically, but messaging localhost for it is weaker.
          /\ votedFor' = [votedFor EXCEPT ![i] = Nil]
          /\ votesResponded' = [votesResponded EXCEPT ![i] = {}]
          /\ votesGranted'   = [votesGranted EXCEPT ![i] = {}]
          /\ voterLog'       = [voterLog EXCEPT ![i] = [j \in {} |-> <<>>]]
          /\ UNCHANGED <<messages, leaderVars, logVars>>
    */

    match server_state.server_vars.state {
        State::Follower | State::Candidate(_) => {
            let candidate_vars = CandidateVars {
                votes_responded: vec![],
                votes_granted: vec![],
            };
            debug!("{:?}: Timeout, becoming candidate- votes {:?}/{:?}", server_state.node, candidate_vars.votes_granted.len(), server_state.all_servers.len());
            server_state.server_vars.current_term = Term(server_state.server_vars.current_term.0 + 1);
            server_state.server_vars.voted_for = None;
            server_state.server_vars.state = State::Candidate(candidate_vars);
            true
        }
        State::Leader(_) => {false}
    }
}

pub(super) enum Timer {
    StateChangeFollower,
    StateChange
}

pub(super) async fn timeout_loop(cs: mpsc::Sender<StateChange>) {
    debug!("Timeout loop started");
    loop {
        //Random delay between 150 to 300 ms
        let delay = Duration::from_millis(rand::random::<u64>() % 300 + 150);
        tokio::time::sleep(delay).await;
        cs.send(StateChange::Timeout).await.unwrap_or_else(|err|{
            error!("Timeout loop error sending timeout: {:?}", err); });
        debug!("Timeout loop sent timeout");
    }
}