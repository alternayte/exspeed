use std::sync::Arc;
use exspeed_protocol::messages::{ClientMessage, ServerMessage};
use exspeed_streams::StorageEngine;
use crate::handlers;

pub struct Broker {
    storage: Arc<dyn StorageEngine>,
}

impl Broker {
    pub fn new(storage: Arc<dyn StorageEngine>) -> Self {
        Self { storage }
    }

    pub fn handle_message(&self, msg: ClientMessage) -> ServerMessage {
        match msg {
            ClientMessage::CreateStream(req) => handlers::handle_create_stream(&self.storage, req),
            ClientMessage::Publish(req) => handlers::handle_publish(&self.storage, req),
            ClientMessage::Fetch(req) => handlers::handle_fetch(&self.storage, req),
            ClientMessage::Connect(_) | ClientMessage::Ping => {
                ServerMessage::Error {
                    code: 500,
                    message: "message should be handled by connection layer".into(),
                }
            }
        }
    }
}
