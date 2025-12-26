use futures_util::{future::join_all, StreamExt};
use rumqttc::{AsyncMode, ClientBuilder, Message, MqttOptions, NetworkOptions, SyncMode, V4};
use std::{option, thread, time::Duration};
use tokio::{runtime::Handle, time::timeout};

#[test]
fn test_workflow() {
    let mut options = MqttOptions::new("client1", "localhost", 1883);
    options.set_keep_alive(Duration::from_secs(3));
    options.set_manual_acks(false);
    options.set_clean_session(true);

    let mut clients = ClientBuilder::new(SyncMode, options, NetworkOptions::new())
        .with_clients(1)
        .build();

    let mut client = clients.get_mut(0).unwrap();

    let tokenid1 = client.publish("hello", rumqttc::QoS::AtLeastOnce, false, b"hello world");
    let tokenid2 = client.publish("hello", rumqttc::QoS::AtLeastOnce, false, b"hello world");
    // client.event_tx.send(rumqttc::IOEvent::ConnectionData);

    let msg = client.wait().unwrap();
    match msg {
        Message::PublishAck(puback) => {
            assert_eq!(puback.token_id, tokenid1);
        }
        _ => panic!("Unexpected message {:?}", msg),
    }

    let msg = client.wait().unwrap();
    match msg {
        Message::PublishAck(puback) => {
            assert_eq!(puback.token_id, tokenid2);
        }
        _ => panic!("Unexpected message {:?}", msg),
    }

    let toekn3 = client
        .subscribe("home/temp", rumqttc::QoS::AtLeastOnce)
        .unwrap();

    let msg = client.wait().unwrap();
    match msg {
        Message::SubscribeAck(suback) => {
            assert_eq!(suback.token_id, toekn3);
        }
        _ => panic!("Unexpected message {:?}", msg),
    }

    let token4 = client.unsubscribe("home/temp").unwrap();
    let msg = client.wait().unwrap();
    match msg {
        Message::UnSubAck(unsuback) => {
            assert_eq!(unsuback.token_id, token4);
        }
        _ => panic!("Unexpected message {:?}", msg),
    }
}

#[test]
fn test_broadcast() {
    let mut options = MqttOptions::new("client2", "localhost", 1883);
    options.set_keep_alive(Duration::from_secs(3));
    options.set_clean_session(true);

    let mut clients = ClientBuilder::new(SyncMode, options, NetworkOptions::new())
        .with_clients(2)
        .build();

    let mut client1 = clients.pop().unwrap();
    let mut client2 = clients.pop().unwrap();

    let token1 = client1
        .subscribe("home/temp", rumqttc::QoS::AtLeastOnce)
        .unwrap();

    let msg = client1.wait().unwrap();
    match msg {
        Message::SubscribeAck(suback) => {
            assert_eq!(suback.token_id, token1);
        }
        _ => panic!("Unexpected message {:?}", msg),
    }

    let token2 = client2
        .subscribe("home/temp", rumqttc::QoS::AtLeastOnce)
        .unwrap();

    let msg = client2.wait().unwrap();
    match msg {
        Message::SubscribeAck(suback) => {
            assert_eq!(suback.token_id, token2);
        }
        _ => panic!("Unexpected message {:?}", msg),
    }

    client1.publish("home/temp", rumqttc::QoS::AtMostOnce, false, b"23C");

    let publish = client1.next().unwrap();
    println!("Received publish :{:?}", publish);

    let publish = client2.next().unwrap();
    println!("Received publish :{:?}", publish);
}

#[test]
fn test_sharedsub() {
    let mut options = MqttOptions::new("client3", "localhost", 1883);
    options.set_clean_session(true);
    options.set_keep_alive(Duration::from_secs(3));
    options.set_manual_acks(true);

    let mut clients = ClientBuilder::new(SyncMode, options, NetworkOptions::new())
        .with_clients(2)
        .build();

    let mut client1 = clients.pop().unwrap();
    let mut client2 = clients.pop().unwrap();

    let token1 = client1
        .subscribe("home/temp", rumqttc::QoS::AtLeastOnce)
        .unwrap();

    let msg = client1.wait().unwrap();
    match msg {
        Message::SubscribeAck(suback) => {
            assert_eq!(suback.token_id, token1);
        }
        _ => panic!("Unexpected message {:?}", msg),
    }

    let token2 = client2
        .subscribe("home/temp", rumqttc::QoS::AtLeastOnce)
        .unwrap();

    let msg = client2.wait().unwrap();
    match msg {
        Message::SubscribeAck(suback) => {
            assert_eq!(suback.token_id, token2);
        }
        _ => panic!("Unexpected message {:?}", msg),
    }
    client1.publish("home/temp", rumqttc::QoS::AtMostOnce, false, b"23C");

    let publish = client1.next().unwrap();
    println!("Received publish :{:?}", publish);
    client1.ack(&publish).unwrap();

    // match timeout(Duration::from_secs(3), client2.next()) {
    //     Ok(_) => {
    //         assert!(false)
    //     }
    //     Err(_) => {
    //         assert!(true)
    //     }
    // }
}

#[test]
fn test_publish_to_internal_clients() {
    let mut options = MqttOptions::new("client4", "localhost", 1883);
    options.set_keep_alive(Duration::from_secs(3));

    let mut clients = ClientBuilder::new(SyncMode, options, NetworkOptions::new())
        .with_clients(2)
        .build();

    let mut client1 = clients.pop().unwrap();
    let mut client2 = clients.pop().unwrap();

    let token1 = client1
        .subscribe("home/temp", rumqttc::QoS::AtLeastOnce)
        .unwrap();

    let msg = client1.wait().unwrap();
    match msg {
        Message::SubscribeAck(suback) => {
            assert_eq!(suback.token_id, token1);
        }
        _ => panic!("Unexpected message {:?}", msg),
    }

    let publish_token = client2.publish("home/temp", rumqttc::QoS::AtLeastOnce, false, b"23C");

    let msg = client1.next().unwrap();
    println!("Received publish :{:?}", msg);

    let msg = client2.wait().unwrap();
    match msg {
        Message::PublishAck(puback) => {
            assert_eq!(puback.token_id, publish_token);
        }
        _ => panic!("Unexpected message {:?}", msg),
    }
}

#[test]
fn test_client_disconnect() {
    let mut options = MqttOptions::new("client5", "localhost", 1883);
    options.set_keep_alive(Duration::from_secs(3));
    options.set_clean_session(true);
    let mut clients = ClientBuilder::new(SyncMode, options, NetworkOptions::new())
        .with_clients(1)
        .build();
    let mut client = clients.pop().unwrap();
    let tokenid1 = client.publish("hello", rumqttc::QoS::AtLeastOnce, false, b"hello world");
    let msg = client.wait().unwrap();
    match msg {
        Message::PublishAck(puback) => {
            assert_eq!(puback.token_id, tokenid1);
        }
        _ => panic!("Unexpected message {:?}", msg),
    }
    client.disconnect().unwrap();
    let msg = client.wait().unwrap();
    match msg {
        Message::Shutdown => {
            println!("Client disconnected successfully");
            assert!(true);
        }
        _ => {
            panic!("Unexpected message {:?}", msg);
        }
    }
}

#[test]
fn test_clean() {
    let mut options = MqttOptions::new("client6", "localhost", 1883);
    options.set_keep_alive(Duration::from_secs(3));
    options.set_clean_session(true);
    let mut clients = ClientBuilder::new(SyncMode, options, NetworkOptions::new())
        .with_clients(1)
        .build();
    let mut client = clients.pop().unwrap();
    let tokenid1 = client.publish("hello", rumqttc::QoS::AtLeastOnce, false, b"hello world");

    client.event_tx.send(rumqttc::IOEvent::MockError).unwrap();
    let msg = client.wait().unwrap();
    match msg {
        Message::PublishAck(puback) => {
            assert!(true);
        }
        Message::Reconnecting => {
            assert!(true);
        }
        _ => panic!("Unexpected message {:?}", msg),
    }
}
