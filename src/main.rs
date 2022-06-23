use serde::Deserialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;
use tokio::sync::Mutex;

use axum::extract::Path;
use axum::Extension;
use axum::{
    extract::ws::{WebSocket, WebSocketUpgrade},
    response::Response,
    routing::get,
    Router,
};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::Message;
use tokio::runtime::Handle;
struct State {
    online_user_notifications: HashMap<String, String>, // user_id video_id
    new_videos: HashMap<String, String>, // ChannelId, video_id
    users_channel_subscriptions: HashMap<String, Vec<String>>
}
#[derive(Debug, Deserialize)]
struct NewVideo {
    channel_id: String,
    video_id: String,
}
#[tokio::main]
async fn main() {
    let handle = Arc::new(Handle::current());
    let consumer: Arc<StreamConsumer> = Arc::new(
        ClientConfig::new()
            .set("group.id", "1")
            .set("bootstrap.servers", "localhost:9092")
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .create()
            .unwrap(),
    );
    let state = Arc::new(Mutex::new(State {
        online_user_notifications: HashMap::new(),
        new_videos: HashMap::new(),
        users_channel_subscriptions: HashMap::new()
    }));
    fetch_users(state.clone()).await;
    let handle_fill = {
        let state = state.clone();
        thread::spawn(move || {
            kafka_consumer("test", handle, consumer, state);
        })
    };
    let app = Router::new()
        .route("/ws/:user_id", get(handler))
        .route_layer(Extension(state));
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
    handle_fill.join().unwrap();
}
/// Load users from the database and there subscriptions
async fn fetch_users(state: Arc<Mutex<State>>) {
    todo!()
}
/// Write new notification to the database
async fn write_user_notification(state: Arc<Mutex<State>>, user_id: String) {
    todo!()
}

async fn handler(
    ws: WebSocketUpgrade,
    Extension(state): Extension<Arc<Mutex<State>>>,
    Path(user_id): Path<String>,
) -> Response {
    state
        .lock()
        .await
        .online_user_notifications
        .insert(user_id.clone(), String::from(""));
    let user_id = user_id.clone();
    ws.on_upgrade(move |socket| handle_socket(socket, state, user_id))
}

async fn handle_socket(mut socket: WebSocket, state: Arc<Mutex<State>>, user_id: String) {
    loop {
        let res = socket.recv().await;
        match res {
            Some(msg) => match msg {
                Ok(_) => (), // We dont care about the message, essentially we only want to see if the socket was closed
                Err(_) => break,
            },
            None => (), // Still open
        };

        match state
            .lock()
            .await
            .online_user_notifications
            .get(&user_id)
            .unwrap()
            .as_str()
        {
            "" => continue,
            _ => socket
                .send(axum::extract::ws::Message::Text(
                    state
                        .lock()
                        .await
                        .online_user_notifications
                        .get(&user_id)
                        .unwrap()
                        .to_string(),
                ))
                .await
                .unwrap(), // get video data push to database and send video data to socket
        }
    }
}
fn kafka_consumer(
    topic: &str,
    rt_handle: Arc<Handle>,
    consumer: Arc<StreamConsumer>,
    state: Arc<Mutex<State>>,
) {
    consumer.subscribe(&[topic]).unwrap();

    loop {
        let state = state.clone();
        let consumer = consumer.clone();
        rt_handle.spawn(async move {
            match consumer.recv().await {
                Err(e) => println!("Kafka error: {}", e),
                Ok(m) => {
                    let payload = match m.payload_view::<str>() {
                        None => return,
                        Some(Ok(s)) => s,
                        Some(Err(_)) => return,
                    };
                    // Deserialize the message into the appropriate struct and send it to the processing thread for further processing
                    let new_video: NewVideo = serde_json::from_str(payload).unwrap();
                    state.lock().await.new_videos.insert(new_video.channel_id,new_video.video_id);
                    drop(state);
                    consumer.commit_message(&m, CommitMode::Async).unwrap();
                }
            };
        });
    }
}
