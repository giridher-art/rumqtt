use futures_util::{StreamExt, future::join_all};
use rumqttc::{AsyncMode, ClientBuilder, Message, MqttOptions, NetworkOptions, V4};
use std::{thread, time::Duration};
use tokio::runtime::Handle;

#[tokio::test]
async fn test_rumqttc_next() {
    let options = MqttOptions::new("client1", "localhost", 1883);
    let mut clients = ClientBuilder::new(V4, AsyncMode, options, NetworkOptions::new())
        .with_clients(1)
        .with_mode(AsyncMode)
        .build();
    let mut client = clients.get_mut(0).unwrap();

    let tokenid1 = client
        .publish("hello", rumqttc::QoS::AtLeastOnce, false, b"hello world")
        .await
        .unwrap();
    let tokenid2 = client
        .publish("hello", rumqttc::QoS::AtLeastOnce, false, b"hello world")
        .await
        .unwrap();
    // client.event_tx.send(rumqttc::IOEvent::ConnectionData);

    let msg = client.wait().await.unwrap();
    match msg {
        Message::PublishAck(puback) => {
            assert_eq!(puback.token_id, tokenid1);
        }
        _ => panic!("Unexpected message {:?}", msg),
    }

    let msg = client.wait().await.unwrap();
    match msg {
        Message::PublishAck(puback) => {
            assert_eq!(puback.token_id, tokenid2);
        }
        _ => panic!("Unexpected message {:?}", msg),
    }

    let toekn3 = client
        .subscribe("home/temp", rumqttc::QoS::AtLeastOnce)
        .await
        .unwrap();

    let msg = client.wait().await.unwrap();
    match msg {
        Message::SubscribeAck(suback) => {
            assert_eq!(suback.token_id, toekn3);
        }
        _ => panic!("Unexpected message {:?}", msg),
    }

    let publish_msg = client.next().await.unwrap();
    println!("Received publish: {:?}", publish_msg);

    assert_eq!(publish_msg.topic, "home/temp");

    let token4 = client.unsubscribe("home/temp").await.unwrap();
    let msg = client.wait().await.unwrap();
    match msg {
        Message::UnSubAck(unsuback) => {
            assert_eq!(unsuback.token_id, token4);
        }
        _ => panic!("Unexpected message {:?}", msg),
    }
}
