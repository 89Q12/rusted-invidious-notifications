use rusted_invidious_types::database::db_manager::DbManager;
use serde::Deserialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::Level;

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
use tokio::sync::RwLock;
struct State {
    online_user_notifications: HashMap<String, String>, // user_id video_id
    new_videos: HashMap<String, String>,                // ChannelId, video_id
    users_channel_subscriptions: HashMap<String, Vec<String>>, // ChannelId, user_ids
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
    let db = DbManager::new("192.168.100.100:19042", None).await.unwrap();
    let state = Arc::new(RwLock::new(State {
        online_user_notifications: HashMap::new(),
        new_videos: HashMap::new(),
        users_channel_subscriptions: fetch_users(&db).await,
    }));
    state.write().await.users_channel_subscriptions.insert(
        "UCz7AdIU5tFoaqs2UG3ZHQHw".to_string(),
        ["1".to_string()].to_vec(),
    );
    {
        let state = state.clone();
        handle.spawn(async {
            kafka_consumer("video_feed", consumer, state).await;
        });
    };
    let app = Router::new()
        .route("/ws/:user_id", get(handler))
        .route_layer(Extension(state));
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}
/// Load users from the database and there subscriptions
async fn fetch_users(db: &DbManager) -> HashMap<String, Vec<String>> {
    let mut users: Vec<String> = Vec::new();
    // If an error occurs, here we cant recover it anyway so panicing is fine
    db.get_users(&mut users).await;
    db.get_subs().await.unwrap()
}

async fn handler(
    ws: WebSocketUpgrade,
    Extension(state): Extension<Arc<RwLock<State>>>,
    Path(user_id): Path<String>,
) -> Response {
    state
        .write()
        .await
        .online_user_notifications
        .insert(user_id.clone(), String::from(""));
    let user_id = user_id.clone();
    ws.on_upgrade(move |socket| handle_socket(socket, state, user_id))
}

async fn handle_socket(mut socket: WebSocket, state: Arc<RwLock<State>>, user_id: String) {
    loop {
        let res = socket.recv().await;
        match res {
            Some(msg) => match msg {
                Ok(_) => {
                    match state
                        .read()
                        .await
                        .online_user_notifications
                        .get(&user_id)
                        .unwrap()
                        .as_str()
                        .is_empty()
                    {
                        true => {
                            socket
                                .send(axum::extract::ws::Message::Text(format!(
                                    "No notification received for user {}",
                                    user_id
                                )))
                                .await
                                .unwrap();
                            continue;
                        }
                        false => socket
                            .send(axum::extract::ws::Message::Text(
                                state
                                    .read()
                                    .await
                                    .online_user_notifications
                                    .get(&user_id)
                                    .unwrap()
                                    .to_string(),
                            ))
                            .await
                            .unwrap(), // get video data push to database and send video data to socket
                    }
                } // We dont care about the message, essentially we only want to see if the socket was closed
                Err(_) => {
                    tracing::event!(target:"ws", Level::DEBUG, "user: {} went offline", user_id);
                    println!("user: {} went offline", user_id);
                    state
                        .write()
                        .await
                        .online_user_notifications
                        .remove(&user_id);
                    break;
                }
            },
            None => break, // Still open
        };
    }
}
async fn kafka_consumer(topic: &str, consumer: Arc<StreamConsumer>, state: Arc<RwLock<State>>) {
    consumer.subscribe(&[topic]).unwrap();

    loop {
        let state = state.clone();
        let consumer = consumer.clone();
        match consumer.recv().await {
            Err(e) => println!("Kafka error: {}", e),
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    None => {
                        drop(state);
                        drop(consumer);
                        return;
                    }
                    Some(Ok(s)) => s,
                    Some(Err(_)) => {
                        drop(state);
                        drop(consumer);
                        return;
                    }
                };
                println!("Received {}", payload);
                // Deserialize the message into the appropriate struct and send it to the processing thread for further processing
                let new_video: NewVideo = serde_json::from_str(payload).unwrap();
                state
                    .write()
                    .await
                    .new_videos
                    .insert(new_video.channel_id.clone(), new_video.video_id.clone());
                let users: &Vec<String> = &state
                    .read()
                    .await
                    .users_channel_subscriptions
                    .get(&new_video.channel_id.clone())
                    .unwrap()
                    .to_owned();
                println!("{:?}", users);
                for user in users.iter() {
                    print!("User {}", user);
                    if state
                        .read()
                        .await
                        .online_user_notifications
                        .contains_key(user)
                    {
                        state
                            .write()
                            .await
                            .online_user_notifications
                            .insert(user.to_string(), new_video.video_id.clone());
                    }
                }
                consumer.commit_message(&m, CommitMode::Async).unwrap();
                drop(state);
                drop(consumer);
            }
        };
    }
}
