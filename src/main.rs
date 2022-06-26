use scylla::cql_to_rust::FromRowError;
use scylla::transport::errors::QueryError;
use scylla::transport::query_result::FirstRowError;
use scylla::{FromRow, FromUserType, IntoUserType};
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
use scylla::cql_to_rust::FromCqlVal;
use scylla::{
    prepared_statement::PreparedStatement, transport::errors::NewSessionError, Session,
    SessionBuilder,
};
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
    let db = Database::new("192.168.100.100:19042", None).await.unwrap();
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
async fn fetch_users(db: &Database) -> HashMap<String, Vec<String>> {
    let mut users: Vec<String> = Vec::new();
    // If an error occurs, here we cant recover it anyway so panicing is fine
    db.get_users(&mut users).await;
    db.get_subs(&mut users).await.unwrap()
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
            .as_str().is_empty()
        {
            true => {
                socket
                .send(axum::extract::ws::Message::Text(format!("No notification received for user {}", user_id))).await
                .unwrap();
                continue
            },
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
                }, // We dont care about the message, essentially we only want to see if the socket was closed
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
                .unwrap().to_owned();
                println!("{:?}", users);
                for user in users.iter() 
                {
                    print!("User {}",user);
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

#[derive(Debug)]
pub enum DbError {
    QueryError(QueryError),
    FromRowError(FromRowError),
    FirstRowError(FirstRowError),
}
struct Database {
    session: Session,
    prepared_statements: Vec<PreparedStatement>,
}
impl Database {
    /// Initializes a new DbManager struct and creates the database session
    async fn new(uri: &str, known_hosts: Option<Vec<String>>) -> Result<Self, NewSessionError> {
        let session_builder;
        if known_hosts.is_some() {
            session_builder = SessionBuilder::new()
                .known_node(uri)
                .known_nodes(&known_hosts.unwrap());
        } else {
            session_builder = SessionBuilder::new().known_node(uri);
        }

        match session_builder.build().await {
            Ok(session) => {
                match session.use_keyspace("rusted_invidious", false).await {
                    Ok(_) => {
                        tracing::event!(target:"db", Level::DEBUG, "Successfully set keyspace")
                    }
                    Err(_) => panic!("KESPACE NOT FOUND EXISTING...."),
                }
                let get_user = session.prepare("SELECT * FROM users");
                let get_user_uid = session.prepare("SELECT uid FROM username_uuid WHERE name = ?");
                let get_subs = session.prepare("SELECT channel_id FROM user_subscriptions");
                let results = tokio::join!(get_user, get_user_uid, get_subs);
                let mut prepared_statements = Vec::new();
                prepared_statements.push(results.0.unwrap());
                prepared_statements.push(results.1.unwrap());
                prepared_statements.push(results.2.unwrap());
                Ok(Self {
                    session,
                    prepared_statements,
                })
            }
            Err(err) => Err(err),
        }
    }
    /// gets all users from the database
    pub async fn get_users(&self, users: &mut Vec<String>) -> Option<DbError> {
        let res = match self
            .session
            .execute(&self.prepared_statements.get(0).unwrap(), &[])
            .await
        {
            Ok(res) => res,
            Err(err) => return Some(DbError::QueryError(err)),
        };
        for row in res.rows().unwrap().into_iter() {
            users.push(match row.into_typed::<User>() {
                Ok(user) => user.uuid,
                Err(err) => return Some(DbError::FromRowError(err)),
            });
        }
        None
    }
    /// Gets the subscriptions for all users in the database
    pub async fn get_subs(
        &self,
        users: &mut Vec<String>,
    ) -> Result<HashMap<String, Vec<String>>, DbError> {
        let mut map: HashMap<String, Vec<String>> = HashMap::new();
        let mut subvec: Vec<UserSubscribed> = Vec::new();
        let res = match self
            .session
            .execute(&self.prepared_statements.get(2).unwrap(), &[])
            .await
        {
            Ok(res) => res,
            Err(err) => return Err(DbError::QueryError(err)),
        };
        for row in res.rows().unwrap().into_iter() {
            let sub = match row.into_typed::<UserSubscribed>() {
                Ok(sub) => sub,
                Err(err) => return Err(DbError::FromRowError(err)),
            };
            subvec.push(sub);
        }
        subvec.iter().for_each(|sub| {
            if !map.contains_key(&sub.channel_id) {
                map.insert(
                    sub.channel_id.to_owned(),
                    subvec
                        .iter()
                        .filter(|inner_sub| *inner_sub.channel_id == sub.channel_id)
                        .map(|inner_sub| inner_sub.uid.to_owned())
                        .collect(),
                );
            }
        });
        Ok(map)
    }
}

/// Represents a user queried from the database
#[derive(Debug, IntoUserType, FromUserType, FromRow)]
pub struct User {
    uuid: String, // partition key
    name: String, // clustering key
    password: String,
    token: String,
    feed_needs_update: bool,
}

impl User {
    pub fn is_authenticated(&self) -> bool {
        self.name.is_empty()
    }
}

/// Used to query the uuid of a user by name.
#[derive(Debug, IntoUserType, FromUserType, FromRow)]
pub struct UsernameUuid {
    name: String, // Primary key
    uuid: String,
}
/// Represents a channel that a user has subcribed to
#[derive(Debug, IntoUserType, FromUserType, FromRow)]
pub struct UserSubscribed {
    uid: String,     // partition key
    subuuid: String, // clustering key
    channel_id: String,
}
