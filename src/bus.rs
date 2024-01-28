use crate::protocol::{Message, MessageEnvelope, MsgAddr, NodeBus};
use crate::types::ServerID;
use log::{debug,trace};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub struct TokioMulticastUdpBus {
    sock_addr: std::net::SocketAddr,
    socket: Arc<tokio::net::UdpSocket>,
    node: ServerID,
    tasks: Option<(JoinHandle<()>, JoinHandle<()>)>,
}

impl TokioMulticastUdpBus {
    pub fn new(sock_addr: std::net::SocketAddr, node: ServerID) -> TokioMulticastUdpBus {
        //Check if IPAddr is multicast
        if !sock_addr.ip().is_multicast() {
            panic!("IPAddr is not multicast");
        }
        let socket = socket2::Socket::new(
            socket2::Domain::IPV4,
            socket2::Type::DGRAM,
            Some(socket2::Protocol::UDP),
        )
        .unwrap();
        let s2_sock_addr = socket2::SockAddr::from(sock_addr);
        socket.set_reuse_address(true).unwrap();
        socket.bind(&s2_sock_addr).unwrap();

        let std_socket: std::net::UdpSocket = socket.into();

        let tokio_socket = tokio::net::UdpSocket::from_std(std_socket).unwrap();
        match sock_addr.ip() {
            std::net::IpAddr::V4(ip) => {
                tokio_socket
                    .join_multicast_v4(ip, std::net::Ipv4Addr::new(0, 0, 0, 0))
                    .unwrap();
            }
            std::net::IpAddr::V6(ip) => {
                tokio_socket.join_multicast_v6(&ip, 0).unwrap();
            }
        }

        //Tokio bind to multicast UDP port
        let socket = Arc::new(tokio_socket);

        TokioMulticastUdpBus {
            sock_addr,
            socket,
            node,
            tasks: None,
        }
    }
}

impl NodeBus for TokioMulticastUdpBus {
    fn register(&mut self) -> (mpsc::Sender<(MsgAddr, Message)>, mpsc::Receiver<(ServerID,Message)>) {
        //Create channels for sending and receiving messages
        let (sender_tx, mut sender_rx) = mpsc::channel(32);
        let (receiver_tx, receiver_rx) = mpsc::channel(32);

        let rx_socket = self.socket.clone();
        let tx_socket = self.socket.clone();

        let node = self.node.clone();
        //Reciever task
        let rx_task = tokio::spawn(async move {
            let mut buf = [0; 1024];
            trace!("Starting receiver task");
            while let Ok((size, _)) = rx_socket.recv_from(&mut buf).await {
                trace!("Received message");
                //Parse message envelope
                if let Ok(message_envelope) = postcard::from_bytes::<MessageEnvelope>(&buf[0..size])
                {
                    //Check if message is for this node
                    match message_envelope.mdest {
                        MsgAddr::Node(n) if n == node => {
                            debug!(
                                "Receiving message to node: {:?} from {:?}",
                                node, message_envelope.msource
                            );
                            receiver_tx.send((message_envelope.msource , message_envelope.mtype)).await.unwrap();
                            debug!(
                                "Received message to node: {:?} from {:?}",
                                node, message_envelope.msource
                            );
                        }
                        MsgAddr::AllNodes => {
                            trace!(
                                "Receiving message from node: {:?}",
                                message_envelope.msource
                            );
                            receiver_tx.send((message_envelope.msource, message_envelope.mtype)).await.unwrap();
                            trace!("Received message from node: {:?}", node);
                        }
                        _ => {
                            trace!(
                                "Node: {:?}, Message not for this node: {:?}",
                                node, message_envelope
                            );
                        }
                    }
                }
            }
        });

        let node = self.node.clone();
        let sock_addr = self.sock_addr.clone();

        //Sender task
        let tx_task = tokio::spawn(async move {
            trace!("Starting sender task");
            while let Some((addr, message)) = sender_rx.recv().await {
                let message_envelope = MessageEnvelope {
                    mdest: addr,
                    msource: node.clone(),
                    mtype: message,
                };

                //Serialize and send message on UDP port
                let message_serialized = postcard::to_allocvec(&message_envelope).unwrap();
                trace!("Sending message to node");

                tx_socket
                    .send_to(&message_serialized, sock_addr)
                    .await
                    .unwrap();
            }
        });

        self.tasks = Some((rx_task, tx_task));

        (sender_tx, receiver_rx)
    }
}

impl Drop for TokioMulticastUdpBus {
    fn drop(&mut self) {
        if let Some((rx_task, tx_task)) = self.tasks.take() {
            rx_task.abort();
            tx_task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{env, time::Duration};

    use super::*;
    use crate::{protocol::{Message, MsgAddr, Test}, types::Term};
    #[test]
    fn test_udp_bus() {
        //Procspawn init
        procspawn::init();
        let test_message = Message{
        term: Term(0),
        payload: crate::protocol::MessagePayload::Test(Test{}),
        };

        //Create node processes

        let mut reciever = procspawn::spawn(test_message.clone(), |test_message: Message| {
            //Tokio runtime create
            let rt = tokio::runtime::Runtime::new().unwrap();

            let handle = rt.block_on(async {
                let node2 = ServerID(2);
                let mut bus2 = TokioMulticastUdpBus::new("224.0.0.1:3000".parse().unwrap(), node2);
                let (_tx2, mut rx2) = bus2.register();

                trace!("Running listen");
                while let Some((id,message)) = rx2.recv().await {
                    trace!("Something received");
                    if message == test_message {
                        trace!("Message received");
                        return Ok(());
                    } else {
                        return Err(());
                    }
                }
                trace!("Error");
                drop(bus2);
                Err(())
            });

            handle.unwrap();

            rt.shutdown_background();
        });

        let mut sender = procspawn::spawn(test_message, |test_message: Message| {
            //Tokio runtime create
            let rt = tokio::runtime::Runtime::new().unwrap();

            rt.block_on(async {
                let node1 = ServerID(1);
                let node2 = ServerID(2);

                let mut bus1 = TokioMulticastUdpBus::new("224.0.0.1:3000".parse().unwrap(), node1);
                let (tx1, _rx1) = bus1.register();

                tx1.send((MsgAddr::Node(node2), test_message))
                    .await
                    .unwrap();

                tokio::time::sleep(Duration::from_secs(5)).await;

                drop(bus1);
            });
            rt.shutdown_background();
        });

        sender.join_timeout(Duration::from_secs(10)).unwrap();
        reciever.join_timeout(Duration::from_secs(1)).unwrap();

        assert!(true);
    }
}
