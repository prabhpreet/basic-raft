use crate::protocol::{MessageEnvelope, MsgAddr, NodeBus, NodeBusRxSender, NodeSender};
use crate::types::ServerID;
use log::{debug, trace,error};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;

pub struct TokioMulticastUdpBus {
    sock_addr: std::net::SocketAddr,
    socket: Arc<tokio::net::UdpSocket>,
    node: ServerID,
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
        }
    }
}

impl NodeBus for TokioMulticastUdpBus {
    fn register(
        &mut self,
    ) -> (NodeSender, NodeBusRxSender, Vec<JoinHandle<()>>)
     {
        //Create channels for sending and receiving messages
        let (sender_tx, mut sender_rx) = mpsc::channel(32);

        let tx_socket = self.socket.clone();

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
                debug!("Sending message to node {:?}", message_envelope);

                tx_socket
                    .send_to(&message_serialized, sock_addr)
                    .await
                    .unwrap();
            }
        });

        let rx_socket = self.socket.clone();
        let node = self.node.clone();

        let node_bus_rx_sender: NodeBusRxSender = Arc::new(Mutex::new(None));
        let node_bus_rx_sender_return = node_bus_rx_sender.clone();

        //Reciever task
        let rx_task = tokio::spawn(async move {
            debug!("Starting receiver task");

            //Wait for node_bus_rx_sender
            let receiver_tx = loop {
                let value = node_bus_rx_sender.lock().await;
                if value.is_none() {
                    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
                }
                else {
                    break value.clone().unwrap();
                }
            };

            debug!("Acquired sender");
            let mut buf = [0; 1024];
            while let Ok((size, _)) = rx_socket.recv_from(&mut buf).await {
                trace!("Received message");
                //Parse message envelope
                if let Ok(message_envelope) = postcard::from_bytes::<MessageEnvelope>(&buf[0..size])
                {
                    //Check if message is for this node
                    match message_envelope.mdest {
                        MsgAddr::Node(n) if n == node => {
                            trace!(
                                "Receiving message to node: {:?} from {:?}: {:?}",
                                node,
                                message_envelope.msource,
                                message_envelope.mtype
                            );
                            if let Err(_) = receiver_tx
                                .send((message_envelope.msource, message_envelope.mtype))
                                .await
                            {
                                error!("Error recieving message on node {:?} from {:?}", node, message_envelope.msource);
                            } else {
                                debug!(
                                    "Received message to node: {:?} from {:?}",
                                    node, message_envelope.msource
                                );
                            }
                        }
                        MsgAddr::AllNodes => {
                            trace!(
                                "Receiving message from node: {:?}",
                                message_envelope.msource
                            );
                            if let Err(_) = receiver_tx
                                .send((message_envelope.msource, message_envelope.mtype))
                                .await
                            {
                                error!("Error recieving message on node {:?} from {:?}", node, message_envelope.msource);
                            }
                            trace!("Received message from node: {:?}", node);
                        }
                        _ => {
                            trace!(
                                "Node: {:?}, Message not for this node: {:?}",
                                node,
                                message_envelope
                            );
                        }
                    }
                }
            }
        });


        let tasks = vec![tx_task, rx_task];

        (sender_tx,node_bus_rx_sender_return , tasks)
    }
}


#[cfg(test)]
mod tests {
    use std::{env, time::Duration};

    use super::*;
    use crate::{
        protocol::{Message, MsgAddr, Test},
        types::Term,
    };
    #[test]
    fn test_udp_bus() {
        //Procspawn init
        procspawn::init();
        let test_message = Message {
            term: Term(0),
            payload: crate::protocol::MessagePayload::Test(Test {}),
        };

        //Create node processes

        let mut reciever = procspawn::spawn(test_message.clone(), |test_message: Message| {
            //Tokio runtime create
            let rt = tokio::runtime::Runtime::new().unwrap();

            let handle = rt.block_on(async {
                let node2 = ServerID(2);
                let mut bus2 = TokioMulticastUdpBus::new("224.0.0.1:3000".parse().unwrap(), node2);
                let (rxtx2,mut rx2) = mpsc::channel(32);
                let (_,sender_mutex,tasks) = bus2.register();
                sender_mutex.lock().await.replace(rxtx2);

                trace!("Running listen");
                while let Some((id, message)) = rx2.recv().await {
                    trace!("Something received");
                    if message == test_message {
                        trace!("Message received");
                        return Ok(());
                    } else {
                        return Err(());
                    }
                }
                for task in tasks {
                    task.abort();
                }
                drop(bus2);
                Err(())
            });

            

            rt.shutdown_background();
        });

        let mut sender = procspawn::spawn(test_message, |test_message: Message| {
            //Tokio runtime create
            let rt = tokio::runtime::Runtime::new().unwrap();

            rt.block_on(async {
                let node1 = ServerID(1);
                let node2 = ServerID(2);

                let mut bus1 = TokioMulticastUdpBus::new("224.0.0.1:3000".parse().unwrap(), node1);
                let (tx1,_,tasks) = bus1.register();

                tx1.send((MsgAddr::Node(node2), test_message))
                    .await
                    .unwrap();

                tokio::time::sleep(Duration::from_secs(5)).await;

                for task in tasks {
                    task.abort();
                }

                drop(bus1);
            });
            rt.shutdown_background();
        });

        sender.join_timeout(Duration::from_secs(10)).unwrap();
        reciever.join_timeout(Duration::from_secs(1)).unwrap();

        assert!(true);
    }
}
