use std::collections::HashMap;
use std::hash::Hash;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::Extension;
use axum::extract::Path;
use axum::{
    extract::ws::{WebSocket, WebSocketUpgrade},
    response::Response,
    routing::get,
    Router,
};
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::producer::FutureProducer;
use rdkafka::Message;
use tokio::runtime::Handle;
struct State{
    online_user_notifications: HashMap<String, String>,
}
#[tokio::main]
async fn main() {
    let state = Arc::new(Mutex::new(State{
        online_user_notifications: HashMap::new(),
    }));
    let app = Router::new()
        .route("/ws/:user_id", get(handler))
        .route_layer(Extension(state));
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn handler(
    ws: WebSocketUpgrade,
    Extension(state): Extension<Arc<Mutex<State>>>,
    Path(user_id): Path<String>
) -> Response {
    state.lock().unwrap().online_user_notifications.insert(user_id.clone(), String::from(""));
    let user_id = user_id.clone();
    ws.on_upgrade( move |socket| handle_socket(socket, state, user_id))
}

async fn handle_socket(mut socket: WebSocket,state: Arc<Mutex<State>>, user_id: String) {
    loop {
        
    }
}
fn kafka_consumer(topic: &str, rt_handle: Arc<Handle>) {
    let consumer: Arc<StreamConsumer> = Arc::new(ClientConfig::new()
    .set("group.id", "1")
    .set("bootstrap.servers", "localhost:9092")
    .set("enable.partition.eof", "false")
    .set("session.timeout.ms", "6000")
    .set("enable.auto.commit", "true")
    .set_log_level(RDKafkaLogLevel::Debug)
    .create()
    .unwrap());

consumer
    .subscribe(&[topic]).unwrap();

loop {
    let consumer = consumer.clone();
    rt_handle.spawn(async move {        
        match consumer.recv().await {
            Err(e) => println!("Kafka error: {}", e),
            Ok(m) => {
                let _payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        println!("Error while deserializing message payload: {:?}", e);
                        ""
                    }
                };
                // Deserialize the message into the appropriate struct and send it to the main thread for further processing
    
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        };
    }); 
}
}

