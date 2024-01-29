use std::time::Duration;

use log::debug;
use tokio::sync::mpsc;

use crate::{
    bus, protocol::{Message, MsgAddr, NodeBus, NodeBusRxSender, NodeReceiver, NodeSender, RequestVoteResponse}, types::{uindex, LogIndex, ServerID, Term}
};

pub(super) async fn receive_handle(
    rx_sender_mutex: NodeBusRxSender,
    change_sender: tokio::sync::mpsc::Sender<super::StateChange>,
    node: ServerID,
) {
    debug!("{:?}- Starting receive handle", node);
    let (bus_sender, mut bus_receiver) = mpsc::channel(5000);
    rx_sender_mutex.lock().await.replace(bus_sender);
    debug!("{:?}- Sent sender, starting recv listen", node);
    while let Some((id, message)) = bus_receiver.recv().await {
        //Send a state using change sender
        debug!("{:?}- Received message from node: {:?}", node, id);
        if let Ok(()) = change_sender
            .send(super::StateChange::Receive((id, message)))
            .await
        {
            debug!("{:?}- Sent message from {:?} to state handle", node, id);
        } else {
            debug!("{:?}- Failed to send message to state handle {:?}", node, id);
            break;
        }
    }
    debug!("Stopping receive handle");
}

/* Responses with stale terms are ignored.
DropStaleResponse(i, j, m) ==
    /\ m.mterm < currentTerm[i]
    /\ Discard(m)
    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars>>
*/
fn drop_stale_response(server_state: &mut super::ServerState, message_term: Term) -> bool {
    if message_term < server_state.server_vars.current_term {
        debug!(
            "{:?}: Drop stale response from term {:?}",
            server_state.node, message_term);
        return true;
    }
    false
}

/* Any RPC with a newer term causes the recipient to advance its term first.
UpdateTerm(i, j, m) ==
    /\ m.mterm > currentTerm[i]
    /\ currentTerm'    = [currentTerm EXCEPT ![i] = m.mterm]
    /\ state'          = [state       EXCEPT ![i] = Follower]
    /\ votedFor'       = [votedFor    EXCEPT ![i] = Nil]
       \* messages is unchanged so m can be processed further.
    /\ UNCHANGED <<messages, candidateVars, leaderVars, logVars>>

 */

fn update_term(server_state: &mut super::ServerState, message_term: Term) -> bool {
    if message_term > server_state.server_vars.current_term {
        server_state.server_vars.current_term = message_term;
        server_state.server_vars.state = super::State::Follower;
        server_state.server_vars.voted_for = None;
        debug!(
            "{:?}: Update term to {:?}",
            server_state.node, message_term);
        true
    } else {
        false
    }
}

/* Receive a message.
Receive(m) ==
    LET i == m.mdest
        j == m.msource
    IN \* Any RPC with a newer term causes the recipient to advance
       \* its term first. Responses with stale terms are ignored.
       \/ UpdateTerm(i, j, m)
       \/ /\ m.mtype = RequestVoteRequest
          /\ HandleRequestVoteRequest(i, j, m)
       \/ /\ m.mtype = RequestVoteResponse
          /\ \/ DropStaleResponse(i, j, m)
             \/ HandleRequestVoteResponse(i, j, m)
       \/ /\ m.mtype = AppendEntriesRequest
          /\ HandleAppendEntriesRequest(i, j, m)
       \/ /\ m.mtype = AppendEntriesResponse
          /\ \/ DropStaleResponse(i, j, m)
             \/ HandleAppendEntriesResponse(i, j, m)
 */

pub(super) async fn receive(
    server_state: &mut super::ServerState,
    message_src: ServerID,
    message: crate::protocol::Message,
    bus_sender: &NodeSender,
    election_timer: &mut super::ElectionTimer,
) -> bool {
    let message_term = message.term;
    //Update term
    if update_term(server_state, message_term) {
        debug!("{:?}: Update term to {:?}", server_state.node, message_term);
        return true;
    }

    debug!("{:?}: Received message {:?}", server_state.node, message);

    match message.payload {
        crate::protocol::MessagePayload::RequestVoteRequest(request) => {
            handle_request_vote_request(
                server_state,
                message_src,
                request,
                message_term,
                bus_sender,
            )
            .await
        }
        crate::protocol::MessagePayload::RequestVoteResponse(response) => {
            if drop_stale_response(server_state, message_term) {
                return false;
            }
            handle_request_vote_response(server_state, message_src, response, message_term).await
        }
        crate::protocol::MessagePayload::AppendEntriesRequest(request) => {
            handle_append_entries_request(
                server_state,
                message_src,
                request,
                message_term,
                bus_sender,
                election_timer
            )
            .await
        }
        crate::protocol::MessagePayload::AppendEntriesResponse(response) => {
            if drop_stale_response(server_state, message_term) {
                return false;
            }
            handle_append_entries_response(server_state, message_src, response, message_term).await
        }
        _ => {
            panic!("Invalid message type")
        }
    }
}

/* Server i receives a RequestVote request from server j with m.mterm <= currentTerm[i].
HandleRequestVoteRequest(i, j, m) ==
    LET logOk == \/ m.mlastLogTerm > LastTerm(log[i])
                 \/ /\ m.mlastLogTerm = LastTerm(log[i])
                    /\ m.mlastLogIndex >= Len(log[i])
        grant == /\ m.mterm = currentTerm[i]
                 /\ logOk
                 /\ votedFor[i] \in {Nil, j}
    IN /\ m.mterm <= currentTerm[i]
       /\ \/ grant  /\ votedFor' = [votedFor EXCEPT ![i] = j]
          \/ ~grant /\ UNCHANGED votedFor
       /\ Reply([mtype        |-> RequestVoteResponse,
                 mterm        |-> currentTerm[i],
                 mvoteGranted |-> grant,
                 \* mlog is used just for the `elections' history variable for
                 \* the proof. It would not exist in a real implementation.
                 mlog         |-> log[i],
                 msource      |-> i,
                 mdest        |-> j],
                 m)
       /\ UNCHANGED <<state, currentTerm, candidateVars, leaderVars, logVars>>

 */

async fn handle_request_vote_request(
    server_state: &mut super::ServerState,
    message_src: ServerID,
    message: crate::protocol::RequestVoteRequest,
    message_term: Term,
    bus_sender: &NodeSender,
) -> bool {
    let mut state_change = false;

    fn last_term(log: &Vec<crate::types::LogEntry>) -> Term {
        if log.len() == 0 {
            return Term(0);
        }
        log.last().unwrap().term
    }

    if message_term > server_state.server_vars.current_term {
        return false;
    }
    let log_ok = message.last_log_term > last_term(&server_state.log_vars.log)
        || (message.last_log_term == last_term(&server_state.log_vars.log)
            && message.last_log_index >= LogIndex(server_state.log_vars.log.len() as uindex));

    let grant = (message_term == server_state.server_vars.current_term)
        && log_ok
        && (server_state.server_vars.voted_for == None
            || server_state.server_vars.voted_for == Some(message_src));

    if grant {
        server_state.server_vars.voted_for = Some(message_src);
        state_change = true;
    }

    let message = Message {
        term: server_state.server_vars.current_term,
        payload: crate::protocol::MessagePayload::RequestVoteResponse(RequestVoteResponse {
            vote_granted: grant,
        }),
    };

    let _ = bus_sender.send((MsgAddr::Node(message_src), message)).await;

    state_change
}

/* Server i receives a RequestVote response from server j with  m.mterm = currentTerm[i].
HandleRequestVoteResponse(i, j, m) ==
    \* This tallies votes even when the current state is not Candidate, but
    \* they won't be looked at, so it doesn't matter.
    /\ m.mterm = currentTerm[i]
    /\ votesResponded' = [votesResponded EXCEPT ![i] =
                              votesResponded[i] \cup {j}]
    /\ \/ /\ m.mvoteGranted
          /\ votesGranted' = [votesGranted EXCEPT ![i] =
                                  votesGranted[i] \cup {j}]
          /\ voterLog' = [voterLog EXCEPT ![i] =
                              voterLog[i] @@ (j :> m.mlog)]
       \/ /\ ~m.mvoteGranted
          /\ UNCHANGED <<votesGranted, voterLog>>
    /\ Discard(m)
    /\ UNCHANGED <<serverVars, votedFor, leaderVars, logVars>>

*/
async fn handle_request_vote_response(
    server_state: &mut super::ServerState,
    message_src: ServerID,
    message: crate::protocol::RequestVoteResponse,
    message_term: Term,
) -> bool {
    if message_term != server_state.server_vars.current_term {
        return false;
    }

    match &mut server_state.server_vars.state {
        super::State::Candidate(vars) => {
            vars.votes_responded.push(message_src);
            if message.vote_granted {
                vars.votes_granted.push(message_src);
            }
            true
        }
        _ => false,
    }
}

/*
\* Server i receives an AppendEntries request from server j with
\* m.mterm <= currentTerm[i]. This just handles m.entries of length 0 or 1, but
\* implementations could safely accept more by treating them the same as
\* multiple independent requests of 1 entry.
HandleAppendEntriesRequest(i, j, m) ==
    LET logOk == \/ m.mprevLogIndex = 0
                 \/ /\ m.mprevLogIndex > 0
                    /\ m.mprevLogIndex <= Len(log[i])
                    /\ m.mprevLogTerm = log[i][m.mprevLogIndex].term
    IN /\ m.mterm <= currentTerm[i]
       /\ \/ /\ \* reject request
                \/ m.mterm < currentTerm[i]
                \/ /\ m.mterm = currentTerm[i]
                   /\ state[i] = Follower
                   /\ \lnot logOk
             /\ Reply([mtype           |-> AppendEntriesResponse,
                       mterm           |-> currentTerm[i],
                       msuccess        |-> FALSE,
                       mmatchIndex     |-> 0,
                       msource         |-> i,
                       mdest           |-> j],
                       m)
             /\ UNCHANGED <<serverVars, logVars>>
          \/ \* return to follower state
             /\ m.mterm = currentTerm[i]
             /\ state[i] = Candidate
             /\ state' = [state EXCEPT ![i] = Follower]
             /\ UNCHANGED <<currentTerm, votedFor, logVars, messages>>
          \/ \* accept request
             /\ m.mterm = currentTerm[i]
             /\ state[i] = Follower
             /\ logOk
             /\ LET index == m.mprevLogIndex + 1
                IN \/ \* already done with request
                       /\ \/ m.mentries = << >>
                          \/ /\ m.mentries /= << >>
                             /\ Len(log[i]) >= index
                             /\ log[i][index].term = m.mentries[1].term
                          \* This could make our commitIndex decrease (for
                          \* example if we process an old, duplicated request),
                          \* but that doesn't really affect anything.
                       /\ commitIndex' = [commitIndex EXCEPT ![i] =
                                              m.mcommitIndex]
                       /\ Reply([mtype           |-> AppendEntriesResponse,
                                 mterm           |-> currentTerm[i],
                                 msuccess        |-> TRUE,
                                 mmatchIndex     |-> m.mprevLogIndex +
                                                     Len(m.mentries),
                                 msource         |-> i,
                                 mdest           |-> j],
                                 m)
                       /\ UNCHANGED <<serverVars, log>>
                   \/ \* conflict: remove 1 entry
                       /\ m.mentries /= << >>
                       /\ Len(log[i]) >= index
                       /\ log[i][index].term /= m.mentries[1].term
                       /\ LET new == [index2 \in 1..(Len(log[i]) - 1) |->
                                          log[i][index2]]
                          IN log' = [log EXCEPT ![i] = new]
                       /\ UNCHANGED <<serverVars, commitIndex, messages>>
                   \/ \* no conflict: append entry
                       /\ m.mentries /= << >>
                       /\ Len(log[i]) = m.mprevLogIndex
                       /\ log' = [log EXCEPT ![i] =
                                      Append(log[i], m.mentries[1])]
                       /\ UNCHANGED <<serverVars, commitIndex, messages>>
       /\ UNCHANGED <<candidateVars, leaderVars>>
*/
async fn handle_append_entries_request(
    server_state: &mut super::ServerState,
    message_src: ServerID,
    message: crate::protocol::AppendEntriesRequest,
    message_term: Term,
    bus_sender: &NodeSender,
    election_timer: &mut super::ElectionTimer
) -> bool {
    election_timer.restart_timer(server_state);
    if message_term > server_state.server_vars.current_term {
        return false;
    }

    let prev_log_term = server_state
        .log_vars
        .log
        .get(message.prev_log_index.0 as usize);
    let log_ok = if message.prev_log_index == LogIndex(0) {
        true
    } else {
        message.prev_log_index <= LogIndex(server_state.log_vars.log.len() as uindex)
            && prev_log_term.is_some()
            && message.prev_log_term == prev_log_term.unwrap().term
    };

    if (message_term < server_state.server_vars.current_term)
        || (!log_ok
            && message_term == server_state.server_vars.current_term
            && server_state.server_vars.state == super::State::Follower)
    {
        let message = Message {
            term: server_state.server_vars.current_term,
            payload: crate::protocol::MessagePayload::AppendEntriesResponse(
                crate::protocol::AppendEntriesResponse {
                    success: false,
                    match_index: LogIndex(0),
                },
            ),
        };

        let _ = bus_sender.send((MsgAddr::Node(message_src), message)).await;
        return false;
    } else if message_term == server_state.server_vars.current_term
        && server_state.server_vars.state.is_candidate()
    {
        //Return to follower state
        server_state.server_vars.state = super::State::Follower;
        return true;
    }

    if message_term == server_state.server_vars.current_term
        && server_state.server_vars.state == super::State::Follower
        && log_ok
    {
        let index = message.prev_log_index.0 + 1;
        let log_index_term = server_state.log_vars.log.get(index as usize);
        if message.entries.len() == 0
            || (message.entries.len() != 0
                && server_state.log_vars.log.len() as uindex >= index
                && log_index_term.is_some()
                && log_index_term.unwrap().term == message.entries[0].term)
        {
            server_state.log_vars.commit_index = message.leader_commit;
            let message = Message {
                term: server_state.server_vars.current_term,
                payload: crate::protocol::MessagePayload::AppendEntriesResponse(
                    crate::protocol::AppendEntriesResponse {
                        success: true,
                        match_index: LogIndex(
                            message.prev_log_index.0 + message.entries.len() as uindex,
                        ),
                    },
                ),
            };

            let _ = bus_sender.send((MsgAddr::Node(message_src), message)).await;
            return true;
        } else if message.entries.len() != 0
            && server_state.log_vars.log.len() as uindex >= index
            && log_index_term.is_some()
            && log_index_term.unwrap().term != message.entries[0].term
        {
            server_state.log_vars.log.pop();
            return true;
        } else if message.entries.len() != 0
            && server_state.log_vars.log.len() as uindex == message.prev_log_index.0
        {
            server_state.log_vars.log.push(message.entries[0].clone());
            return true;
        } else {
            unreachable!("Invalid state");
        }
    }
    false
}

/* Server i receives an AppendEntries response from server j with m.mterm = currentTerm[i].
HandleAppendEntriesResponse(i, j, m) ==
    /\ m.mterm = currentTerm[i]
    /\ \/ /\ m.msuccess \* successful
          /\ nextIndex'  = [nextIndex  EXCEPT ![i][j] = m.mmatchIndex + 1]
          /\ matchIndex' = [matchIndex EXCEPT ![i][j] = m.mmatchIndex]
       \/ /\ \lnot m.msuccess \* not successful
          /\ nextIndex' = [nextIndex EXCEPT ![i][j] =
                               Max({nextIndex[i][j] - 1, 1})]
          /\ UNCHANGED <<matchIndex>>
    /\ Discard(m)
    /\ UNCHANGED <<serverVars, candidateVars, logVars, elections>>

*/
async fn handle_append_entries_response(
    server_state: &mut super::ServerState,
    message_src: ServerID,
    message: crate::protocol::AppendEntriesResponse,
    message_term: Term,
) -> bool {
    if message_term != server_state.server_vars.current_term {
        return false;
    }
    match &mut server_state.server_vars.state {
        super::State::Leader(vars) => {
            if message.success {
                vars.next_index
                    .insert(message_src, LogIndex(message.match_index.0 + 1));
                vars.match_index
                    .insert(message_src, LogIndex(message.match_index.0));
            } else {
                vars.next_index.insert(
                    message_src,
                    LogIndex(std::cmp::max(message.match_index.0 - 1, 1)),
                );
            }
            true
        }
        _ => false,
    }
}
