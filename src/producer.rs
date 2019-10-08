use redis::{Commands, RedisResult};
use std::cell::RefCell;
use crate::message;

pub struct Producer {
    queue_name: String,
    client: RefCell<redis::Connection>,
}

impl Producer {
    pub fn new(queue_name: String, client: redis::Connection) -> Producer {
        Producer {
            queue_name,
            client: RefCell::new(client),
        }
    }

    /// Push a new job to the source queue.
    pub fn push<T: message::MessageEncodable>(&self, job: T) -> RedisResult<()> {
        self.client
            .borrow_mut()
            .lpush(self.queue_name.as_str(), job.encode_job())
    }
}
