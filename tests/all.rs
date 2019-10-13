use orizuru::{Consumer, Producer, GC, CONSUMERS_KEY};
use redis::Commands;
use serde::{Deserialize, Serialize};
use std::time;
use uuid::Uuid;

#[macro_use]
mod test_utils;

#[derive(Deserialize, Serialize)]
struct Message {
    id: u64,
}

#[test]
fn mixed_one_consumer() {
    redis_fixture!(client, con, consumer, producer, gc, {
        assert!(consumer.register().is_ok());
        assert_eq!(0, producer.size());
        assert_eq!(0, consumer.size());

        for i in 0..15 {
            assert!(producer.push(Message { id: i }).is_ok());
        }

        while let Some(m) = consumer.next::<Message>() {
            consumer.heartbeat(time::Duration::from_millis(100));

            if m.is_err() {
                continue;
            }

            let mut m = m.unwrap();
            if m.id <= 7 {
                assert!(m.ack().is_ok());
            } else if m.id > 7 && m.id < 14 {
                assert!(m.reject().is_ok());
            }

            if m.id == 14 {
                consumer.stop();
            }
        }

        assert!(consumer.is_stopped());
        assert_eq!(gc.collect(), Ok(7));
        assert_eq!(gc.collect_one(consumer.name()), Ok(0));

        assert!(consumer.deregister().is_ok());
        let cons: Vec<String> = con.smembers(CONSUMERS_KEY).unwrap();
        assert!(!cons.contains(&String::from(consumer.name())));
    });
}
