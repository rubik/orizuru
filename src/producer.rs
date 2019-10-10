use crate::message;
use redis::Commands;
use std::cell::RefCell;

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
    pub fn push<T: message::MessageEncodable>(&self, job: T) -> Result<(), &'static str> {
        let encoded = job.encode_job()?;
        self.client
            .borrow_mut()
            .lpush(self.queue_name.as_str(), encoded).or(Err("failed to push"))
    }

    /// Get the number of remaining jobs in the queue.
    pub fn size(&self) -> u64 {
        self.client
            .borrow_mut()
            .llen(self.queue_name.as_str())
            .unwrap_or(0)
    }
}
