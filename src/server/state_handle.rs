use log::{debug, error};
use tokio::sync::mpsc;

use crate::{
    protocol::{AppendEntriesRequest, Message, MsgAddr, NodeSender, RequestVoteRequest},
    types::{uindex, LogIndex, ServerID, Term},
};

use super::{ServerState, State, StateChange};
use async_recursion::async_recursion;

pub(super) async fn state_handle(
    mut server_state: ServerState,
    change_sender: mpsc::Sender<StateChange>,
    mut change_receiver: mpsc::Receiver<StateChange>,
    bus_sender: NodeSender,
) {
    #[async_recursion]
    async fn evaluate_change(server_state: &mut ServerState, bus_sender: &NodeSender) {
        if request_vote(server_state, &bus_sender).await {
            evaluate_change(server_state, bus_sender).await
        } else if become_leader(server_state) {
            evaluate_change(server_state, bus_sender).await
        } else if advance_commit_index(server_state) {
            evaluate_change(server_state, bus_sender).await
        } else if append_entries(server_state, &bus_sender).await {
            evaluate_change(server_state, bus_sender).await
        }
    }

    debug!("{:?}: Starting state handle", server_state.node);
    loop {
        let change = change_receiver.recv().await;
        if change.is_none() {
            break;
        }

        match change.unwrap() {
            StateChange::Timeout => {
                if super::timeout::timeout(&mut server_state) {
                    evaluate_change(&mut server_state, &bus_sender).await;
                }
            }
            StateChange::ClientRequest(value) => {
                if super::client_request::client_request(&mut server_state, value) {
                    evaluate_change(&mut server_state, &bus_sender).await;
                }
            }
            StateChange::Receive((message_src, message)) => {
                if super::receive::receive(&mut server_state, message_src, message, &bus_sender)
                    .await
                {
                    evaluate_change(&mut server_state, &bus_sender).await;
                }
            }
        }
    }
    error!("{:?}: Exited state change thread", server_state.node);
}

/*
 Candidate i sends j a RequestVote request.
RequestVote(i, j) ==
    /\ state[i] = Candidate
    /\ j \notin votesResponded[i]
    /\ Send([mtype         |-> RequestVoteRequest,
             mterm         |-> currentTerm[i],
             mlastLogTerm  |-> LastTerm(log[i]),
             mlastLogIndex |-> Len(log[i]),
             msource       |-> i,
             mdest         |-> j])
    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars>>
 */

async fn request_vote(server_state: &mut ServerState, bus_sender: &NodeSender) -> bool {
    debug!("{:?}: Requesting vote", server_state.node);
    match &mut server_state.server_vars.state {
        State::Candidate(candidate_vars) => {
            for server in &server_state.all_servers {
                if !candidate_vars.votes_responded.contains(server) {
                    let message = Message {
                        term: server_state.server_vars.current_term,
                        payload: crate::protocol::MessagePayload::RequestVoteRequest(
                            RequestVoteRequest {
                                last_log_term: server_state
                                    .log_vars
                                    .log
                                    .last()
                                    .map(|x| x.term)
                                    .unwrap_or(Term(0)),
                                last_log_index: LogIndex(server_state.log_vars.log.len() as uindex),
                            },
                        ),
                    };

                    let _ = bus_sender
                        .send((MsgAddr::Node(server.clone()), message))
                        .await;
                }
            }
        }
        _ => {}
    }
    false
}

/* Candidate i transitions to leader.
BecomeLeader(i) ==
    /\ state[i] = Candidate
    /\ votesGranted[i] \in Quorum
    /\ state'      = [state EXCEPT ![i] = Leader]
    /\ nextIndex'  = [nextIndex EXCEPT ![i] =
                         [j \in Server |-> Len(log[i]) + 1]]
    /\ matchIndex' = [matchIndex EXCEPT ![i] =
                         [j \in Server |-> 0]]
    /\ elections'  = elections \cup
                         {[eterm     |-> currentTerm[i],
                           eleader   |-> i,
                           elog      |-> log[i],
                           evotes    |-> votesGranted[i],
                           evoterLog |-> voterLog[i]]}
    /\ UNCHANGED <<messages, currentTerm, votedFor, candidateVars, logVars>>

 */
fn become_leader(server_state: &mut ServerState) -> bool {
    match &mut server_state.server_vars.state {
        State::Candidate(candidate_vars) => {
            if candidate_vars.votes_granted.len() > server_state.all_servers.len() / 2 {
                debug!(
                    "{:?}: Becoming leader- votes {:?}/{:?}",
                    server_state.node,
                    candidate_vars.votes_granted.len(),
                    server_state.all_servers.len()
                );
                let leader_vars = super::LeaderVars {
                    next_index: server_state
                        .all_servers
                        .iter()
                        .map(|x| {
                            (
                                x.clone(),
                                LogIndex(server_state.log_vars.log.len() as uindex + 1),
                            )
                        })
                        .collect(),
                    match_index: server_state
                        .all_servers
                        .iter()
                        .map(|x| (x.clone(), LogIndex(0)))
                        .collect(),
                };
                server_state.server_vars.state = State::Leader(leader_vars);
                true
            } else {
                debug!(
                    "{:?}: Not becoming leader- votes {:?}/{:?}",
                    server_state.node,
                    candidate_vars.votes_granted.len(),
                    server_state.all_servers.len()
                );
                false
            }
        }
        _ => {
            debug!(
                "{:?}: Not becoming leader- not candidate",
                server_state.node
            );
            false
        }
    }
}

/* Leader i advances its commitIndex.
\* This is done as a separate step from handling AppendEntries responses,
\* in part to minimize atomic regions, and in part so that leaders of
\* single-server clusters are able to mark entries committed.
AdvanceCommitIndex(i) ==
    /\ state[i] = Leader
    /\ LET \* The set of servers that agree up through index.
           Agree(index) == {i} \cup {k \in Server :
                                         matchIndex[i][k] >= index}
           \* The maximum indexes for which a quorum agrees
           agreeIndexes == {index \in 1..Len(log[i]) :
                                Agree(index) \in Quorum}
           \* New value for commitIndex'[i]
           newCommitIndex ==
              IF /\ agreeIndexes /= {}
                 /\ log[i][Max(agreeIndexes)].term = currentTerm[i]
              THEN
                  Max(agreeIndexes)
              ELSE
                  commitIndex[i]
       IN commitIndex' = [commitIndex EXCEPT ![i] = newCommitIndex]
    /\ UNCHANGED <<messages, serverVars, candidateVars, leaderVars, log>>

 */
fn advance_commit_index(server_state: &mut ServerState) -> bool {
    match &mut server_state.server_vars.state {
        State::Leader(leader_vars) => {
            //Get the maximum index for which a quorum agrees
            let max_index: Option<uindex> = (1..=server_state.log_vars.log.len())
                .filter(|index| {
                    let agree: Vec<&ServerID> = server_state
                        .all_servers
                        .iter()
                        .filter(|server| {
                            *leader_vars.match_index.get(server).unwrap_or(&LogIndex(0))
                                >= LogIndex(*index as uindex)
                        })
                        .collect();
                    agree.len() > server_state.all_servers.len() / 2
                })
                .fold(None, |max, x| match max {
                    None => Some(x as uindex),
                    Some(y) => Some(std::cmp::max(x as uindex, y)),
                });

            if let Some(max_index) = max_index {
                debug!("{:?}: Advancing commit index", server_state.node);
                let max_entry = server_state.log_vars.log.get(max_index as usize).unwrap();
                if max_entry.term == server_state.server_vars.current_term {
                    server_state.log_vars.commit_index = LogIndex(max_index);
                    true
                } else {
                    false
                }
            } else {
                debug!("{:?}: Not advancing commit index", server_state.node);
                false
            }
        }
        _ => {
            debug!(
                "{:?}: Not advancing commit index- not leader",
                server_state.node
            );
            false
        }
    }
}

/* Leader i sends j an AppendEntries request containing up to 1 entry.
\* While implementations may want to send more than 1 at a time, this spec uses
\* just 1 because it minimizes atomic regions without loss of generality.
AppendEntries(i, j) ==
    /\ i /= j
    /\ state[i] = Leader
    /\ LET prevLogIndex == nextIndex[i][j] - 1
           prevLogTerm == IF prevLogIndex > 0 THEN
                              log[i][prevLogIndex].term
                          ELSE
                              0
           \* Send up to 1 entry, constrained by the end of the log.
           lastEntry == Min({Len(log[i]), nextIndex[i][j]})
           entries == SubSeq(log[i], nextIndex[i][j], lastEntry)
       IN Send([mtype          |-> AppendEntriesRequest,
                mterm          |-> currentTerm[i],
                mprevLogIndex  |-> prevLogIndex,
                mprevLogTerm   |-> prevLogTerm,
                mentries       |-> entries,
                \* mlog is used as a history variable for the proof.
                \* It would not exist in a real implementation.
                mlog           |-> log[i],
                mcommitIndex   |-> Min({commitIndex[i], lastEntry}),
                msource        |-> i,
                mdest          |-> j])
    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars>>

 */
async fn append_entries(server_state: &mut ServerState, bus_sender: &NodeSender) -> bool {
    match &mut server_state.server_vars.state {
        State::Leader(leader_vars) => {
            for server in &server_state.all_servers {
                let prev_log_index =
                    leader_vars.next_index.get(server).map(|v| v.0).unwrap_or(0) - 1;
                let prev_log_term = if prev_log_index > 0 {
                    server_state
                        .log_vars
                        .log
                        .get(prev_log_index as usize)
                        .unwrap()
                        .term
                } else {
                    Term(0)
                };

                let next_index = leader_vars.next_index.get(server).map(|v| v.0);
                let last_entry = match next_index {
                    Some(next_index) => {
                        std::cmp::min(next_index as usize, server_state.log_vars.log.len())
                    }
                    None => server_state.log_vars.log.len(),
                };
                let entries = server_state.log_vars.log
                    [next_index.unwrap_or(0) as usize..last_entry]
                    .to_vec();
                let commit_index =
                    std::cmp::min(server_state.log_vars.commit_index.0, last_entry as uindex);

                debug!(
                    "{:?}: Appending entries to {:?}, len {:?}",
                    server_state.node,
                    server,
                    entries.len()
                );
                let message = Message {
                    term: server_state.server_vars.current_term,
                    payload: crate::protocol::MessagePayload::AppendEntriesRequest(
                        AppendEntriesRequest {
                            prev_log_index: LogIndex(prev_log_index),
                            prev_log_term,
                            entries,
                            leader_commit: LogIndex(commit_index),
                            leader_id: server_state.node,
                        },
                    ),
                };

                let _ = bus_sender
                    .send((MsgAddr::Node(server.clone()), message))
                    .await;
            }
            false
        }
        _ => {
            debug!("{:?}: Not appending entries- not leader", server_state.node);
            false
        }
    }
}
