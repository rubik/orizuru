use orizuru::{Consumer, Producer};
use redis::Commands;
use rmp_serde::Serializer;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[macro_use]
mod test_utils;

#[derive(Deserialize, Serialize)]
struct Message {
    id: u64,
}

#[test]
fn producer_can_enqueue() {
    redis_fixture!(client, con, consumer, "p", producer, {
        assert_eq!(0, producer.size());
        assert_eq!(0, consumer.size());

        producer.push(Message { id: 53 }).unwrap();

        assert_eq!(1, producer.size());
        assert_eq!(1, consumer.size());

        let j = consumer.next::<Message>().unwrap().unwrap();
        assert_eq!(53, j.id);
    });
}
