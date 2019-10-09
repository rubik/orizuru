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

fn sample_job_payload(id: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    let job = Message { id };
    job.serialize(&mut Serializer::new(&mut buf)).unwrap();
    buf
}

#[test]
fn decodes_job() {
    redis_fixture!(client, con, consumer, {
        let _: () = con
            .rpush(consumer.source_queue(), sample_job_payload(42))
            .unwrap();

        let j = consumer.next::<Message>().unwrap().unwrap();
        assert_eq!(42, j.id);
    });
}

#[test]
fn unacked_to_unack_queue() {
    redis_fixture!(client, con, consumer, {
        let pqueue = consumer.processing_queue();
        let uqueue = consumer.unacked_queue();

        let _: () = con
            .lpush(consumer.source_queue(), sample_job_payload(42))
            .unwrap();

        {
            let j = consumer.next::<Message>().unwrap().unwrap();
            assert_eq!(42, j.id);
            let in_proc: Vec<Vec<u8>> = con.lrange(pqueue, 0, -1).unwrap();
            assert_eq!(1, in_proc.len());
            assert_eq!(sample_job_payload(42), in_proc[0]);
        }

        assert_eq!(0, con.llen(pqueue).unwrap());
        assert_eq!(1, con.llen(uqueue).unwrap());
    });
}

#[test]
fn rejected_to_unack_queue() {
    redis_fixture!(client, con, consumer, {
        let pqueue = consumer.processing_queue();
        let uqueue = consumer.unacked_queue();

        let _: () = con
            .lpush(consumer.source_queue(), sample_job_payload(42))
            .unwrap();

        {
            let mut j = consumer.next::<Message>().unwrap().unwrap();
            assert_eq!(42, j.id);
            let in_proc: Vec<Vec<u8>> = con.lrange(pqueue, 0, -1).unwrap();
            assert_eq!(1, in_proc.len());
            assert_eq!(sample_job_payload(42), in_proc[0]);
            let _ = j.reject();
        }

        let in_proc_size: u32 = con.llen(pqueue).unwrap();
        assert_eq!(0, in_proc_size);
        assert_eq!(1, con.llen(uqueue).unwrap());
    });
}

#[test]
fn acked_are_released() {
    redis_fixture!(client, con, consumer, {
        let pqueue = consumer.processing_queue();

        let _: () = con
            .lpush(consumer.source_queue(), sample_job_payload(42))
            .unwrap();

        let mut m = consumer.next::<Message>().unwrap().unwrap();
        let _ = m.ack();
        let in_proc: u32 = con.llen(pqueue).unwrap();
        assert_eq!(0, in_proc);
    });
}

#[test]
fn can_be_stopped() {
    redis_fixture!(client, con, consumer, {
        let _: () = con
            .lpush(consumer.source_queue(), sample_job_payload(1))
            .unwrap();
        let _: () = con
            .lpush(consumer.source_queue(), sample_job_payload(2))
            .unwrap();
        let _: () = con
            .lpush(consumer.source_queue(), sample_job_payload(3))
            .unwrap();

        assert_eq!(3, consumer.size());

        while let Some(m) = consumer.next::<Message>() {
            let _m = m.unwrap();
            let _ = consumer.stop();
        }

        assert_eq!(2, consumer.size());
    });
}

#[test]
fn producer_can_enqueue() {
    redis_fixture!(client, con, consumer, producer, {
        assert_eq!(0, producer.size());
        assert_eq!(0, consumer.size());

        producer.push(Message { id: 53 }).unwrap();

        assert_eq!(1, producer.size());
        assert_eq!(1, consumer.size());

        let j = consumer.next::<Message>().unwrap().unwrap();
        assert_eq!(53, j.id);
    });
}
