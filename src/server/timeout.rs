use std::time::Duration;

use log::{debug, error};
use tokio::sync::mpsc;

use crate::{server::{CandidateVars, ServerState, State, Term}, types::ServerID};

use super::StateChange;
pub(super) fn election_timeout(server_state: &mut ServerState) -> bool {
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
            debug!(
                "{:?},{:?}: Timeout, becoming candidate- votes {:?}/{:?}",
                server_state.server_vars.current_term,
                server_state.node,
                candidate_vars.votes_granted.len(),
                server_state.all_servers.len()
            );
            server_state.server_vars.current_term =
                Term(server_state.server_vars.current_term.0 + 1);
            server_state.server_vars.voted_for = None;
            server_state.server_vars.state = State::Candidate(candidate_vars);
            true
        }
        State::Leader(_) => false,
    }
}

pub(super) struct ElectionTimer {
    handle: Option<tokio::task::JoinHandle<()>>,
    cs: mpsc::Sender<StateChange>,
}

impl ElectionTimer {
    const TIMEOUT_MIN: u64 = 1500;
    const VARIANCE: u64 = 1500;
    pub fn new(cs: mpsc::Sender<StateChange>) -> ElectionTimer {
        ElectionTimer { handle: None, cs }
    }

    async fn timeout_loop(node: ServerID, mut term: Term, cs: mpsc::Sender<StateChange>) {
        debug!("{:?},{:?}- Timeout loop started", term, node);
        loop {
            //Random delay between 150 to 300 ms
            let delay = Duration::from_millis(
                (rand::random::<u64>() % ElectionTimer::VARIANCE) + ElectionTimer::TIMEOUT_MIN,
            );
            tokio::time::sleep(delay).await;
            debug!("{:?},{:?}- Timing out now", term, node);
            if let Ok(_) = cs.send(StateChange::ElectionTimeout).await {
                debug!("{:?},{:?}- Timeout loop sent timeout", term, node);
            } else {
                error!("{:?},{:?}- Timeout loop error sending timeout", term, node);
                break;
            }
            //Increment term
            term = Term(term.0 + 1);
        }
        debug!("{:?},{:?}- Timeout loop ended", term, node);
    }

    pub fn update_state(&mut self, server_state: &ServerState) {
        match server_state.server_vars.state {
            State::Follower |  State::Candidate(_)  => {
                if self.handle.is_none() {
                    debug!("{:?},{:?}: Updating timer state to start", server_state.server_vars.current_term, server_state.node);
                    let cs = self.cs.clone();
                    self.handle = Some(tokio::spawn(ElectionTimer::timeout_loop(server_state.node, server_state.server_vars.current_term,cs)));
                }
            }
            State::Leader(_)=> {
                if let Some(handle) = self.handle.take() {
                    debug!("{:?},{:?}: Updating timer state to stop",  server_state.server_vars.current_term,server_state.node);
                    handle.abort();
                }
            }
        }
    }

    pub fn restart_timer(&mut self, server_state: &ServerState) {
        match server_state.server_vars.state {
            State::Follower => {
                debug!("{:?},{:?}: Restarting timer", server_state.server_vars.current_term, server_state.node);
                if let Some(handle) = self.handle.take() {
                    handle.abort();
                }
                let cs = self.cs.clone();
                self.handle = Some(tokio::spawn(ElectionTimer::timeout_loop(server_state.node, server_state.server_vars.current_term,cs)));
            }
            State::Leader(_) | State::Candidate(_) => {}
        }
    }
}

impl Drop for ElectionTimer {
    fn drop(&mut self) {
        debug!("Dropping timer");
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}
