use redis::{Commands, ErrorKind, RedisResult, Value};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::ops::{Deref, Drop};
use std::cell::{RefCell};

/// Message objects that can be reconstructed from the data stored in Redis.
///
/// Implemented for all `Deserialize` objects by default by relying on Msgpack
/// decoding.
pub trait MessageDecodable
where
    Self: Sized,
{
    /// Decode the given Redis value into a message
    ///
    /// In the default implementation, the string value is decoded by assuming
    /// it was encoded through the Msgpack encoding.
    fn decode_job(value: &Value) -> RedisResult<Self>;
}

/// Message objects that can be encoded to a string to be stored in Redis.
///
/// Implemented for all `Serialize` objects by default by encoding with Msgpack.
pub trait MessageEncodable {
    /// Encode the value into a bytes array to be inserted into Redis.
    ///
    /// In the default implementation, the object is encoded with Msgpack.
    fn encode_job(&self) -> Vec<u8>;
}

impl<T: DeserializeOwned> MessageDecodable for T {
    fn decode_job(value: &Value) -> RedisResult<T> {
        match *value {
            Value::Data(ref v) => rmp_serde::decode::from_slice(v)
                .map_err(|_| From::from((ErrorKind::TypeError, "Msgpack decode failed"))),
            _ => Err((ErrorKind::TypeError, "Can only decode from a string"))?,
        }
    }
}

impl<T: Serialize> MessageEncodable for T {
    fn encode_job(&self) -> Vec<u8> {
        rmp_serde::encode::to_vec(self).unwrap()
    }
}

pub struct MessageGuard<'a, T: 'a> {
    message: T,
    payload: Vec<u8>,
    client: &'a RefCell<redis::Connection>,
    processing_queue_name: String,
    unacked_queue_name: String,
    acked: bool,
    rejected: bool,
}

impl<'a, T> MessageGuard<'a, T> {
    pub fn new(message: T, payload: Vec<u8>, client: &'a RefCell<redis::Connection>, processing_queue_name: String, unacked_queue_name: String, acked: bool, rejected: bool) -> MessageGuard<'a, T> {
        MessageGuard {
            message,
            payload,
            client,
            processing_queue_name,
            unacked_queue_name,
            acked,
            rejected,
        }
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn message(&self) -> &T {
        &self.message
    }

    /// Acknowledge the message and remove it from the *processing* queue.
    pub fn ack(&mut self) -> RedisResult<Value> {
        self.acked = true;
        self.client.borrow_mut().lrem(
            self.processing_queue_name.as_str(),
            1,
            self.payload.clone(),
        )
    }

    /// Reject the message and push it from the *processing* queue to the
    /// *unack* queue.
    pub fn reject(&mut self) -> RedisResult<Value> {
        self.rejected = true;
        redis::pipe()
            .atomic()
            .cmd("LPUSH")
            .arg(self.unacked_queue_name.as_str())
            .arg(self.payload.clone())
            .ignore()
            .cmd("LREM")
            .arg(self.processing_queue_name.as_str())
            .arg(1)
            .arg(self.payload.clone())
            .ignore()
            .query(&mut *self.client.borrow_mut())
    }

    pub fn client(&self) -> &RefCell<redis::Connection> {
        self.client
    }
}

impl<'a, T> Deref for MessageGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.message
    }
}

impl<'a, T> Drop for MessageGuard<'a, T> {
    fn drop(&mut self) {
        if !self.rejected && !self.acked {
            let _ = self.reject();
        }
    }
}