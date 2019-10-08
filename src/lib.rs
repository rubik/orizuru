mod message;
mod consumer;
mod producer;

pub use consumer::Consumer;
pub use producer::Producer;
pub use message::{MessageEncodable, MessageDecodable, MessageGuard};


#[cfg(test)]
mod test {
    use super::{Consumer, Producer, MessageGuard};
    use redis::Commands;
    use rmp_serde::Serializer;
    use serde::{Deserialize, Serialize};

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
        let client = redis::Client::open("redis://127.0.0.1:6379/").unwrap();
        let mut con = client.get_connection().unwrap();
        let con2 = client.get_connection().unwrap();
        let worker = Consumer::new("consumer-1".into(), "default".into(), con2);

        let _: () = con
            .rpush(worker.source_queue(), sample_job_payload(42))
            .unwrap();

        let j = worker.next::<Message>().unwrap().unwrap();
        assert_eq!(42, j.id);
    }

    #[test]
    fn releases_job_unacked() {
        let client = redis::Client::open("redis://127.0.0.1:6379/").unwrap();
        let mut con = client.get_connection().unwrap();
        let con2 = client.get_connection().unwrap();
        let worker = Consumer::new("consumer-2".into(), "default".into(), con2);
        let bqueue = worker.processing_queue();

        let _: () = con.del(bqueue).unwrap();
        let _: () = con
            .lpush(worker.source_queue(), sample_job_payload(42))
            .unwrap();

        {
            let j = worker.next::<Message>().unwrap().unwrap();
            assert_eq!(42, j.id);
            let in_backup: Vec<Vec<u8>> = con.lrange(bqueue, 0, -1).unwrap();
            assert_eq!(1, in_backup.len());
            assert_eq!(sample_job_payload(42), in_backup[0]);
        }

        let in_backup: u32 = con.llen(bqueue).unwrap();
        assert_eq!(0, in_backup);
    }

    #[test]
    fn releases_job_acked() {
        let client = redis::Client::open("redis://127.0.0.1:6379/").unwrap();
        let mut con = client.get_connection().unwrap();
        let con2 = client.get_connection().unwrap();
        let worker = Consumer::new("consumer-3".into(), "default".into(), con2);
        let bqueue = worker.processing_queue();

        let _: () = con.del(bqueue).unwrap();
        let _: () = con
            .lpush(worker.source_queue(), sample_job_payload(42))
            .unwrap();

        let mut m = worker.next::<Message>().unwrap().unwrap();
        m.ack();
        let in_backup: u32 = con.llen(bqueue).unwrap();
        assert_eq!(0, in_backup);
    }

    #[test]
    fn can_be_stopped() {
        let client = redis::Client::open("redis://127.0.0.1:6379/").unwrap();
        let mut con = client.get_connection().unwrap();
        let con2 = client.get_connection().unwrap();
        let worker = Consumer::new("consumer-4".into(), "stopper".into(), con2);

        let _: () = con.del(worker.source_queue()).unwrap();
        let _: () = con
            .lpush(worker.source_queue(), sample_job_payload(1))
            .unwrap();
        let _: () = con
            .lpush(worker.source_queue(), sample_job_payload(2))
            .unwrap();
        let _: () = con
            .lpush(worker.source_queue(), sample_job_payload(3))
            .unwrap();

        assert_eq!(3, worker.size());

        while let Some(task) = worker.next::<Message>() {
            let _task = task.unwrap();
            worker.stop();
        }

        assert_eq!(2, worker.size());
    }

    #[test]
    fn producer_can_enqueue() {
        let client = redis::Client::open("redis://127.0.0.1:6379/").unwrap();
        let mut con = client.get_connection().unwrap();
        let con2 = client.get_connection().unwrap();
        let con3 = client.get_connection().unwrap();

        let prod = Producer::new("queue-0".into(), con2);
        let worker = Consumer::new("consumer-5".into(), "queue-0".into(), con3);
        let _: () = con.del(worker.source_queue()).unwrap();

        assert_eq!(0, prod.size());
        assert_eq!(0, worker.size());

        prod.push(Message { id: 53 }).unwrap();

        assert_eq!(1, prod.size());
        assert_eq!(1, worker.size());

        let j = worker.next::<Message>().unwrap().unwrap();
        assert_eq!(53, j.id);
    }

    #[test]
    fn does_not_drop_unacked() {
        let client = redis::Client::open("redis://127.0.0.1:6379/").unwrap();
        let mut con = client.get_connection().unwrap();
        let con2 = client.get_connection().unwrap();
        let worker = Consumer::new("consumer-6".into(), "failure".into(), con2);

        let _: () = con.del(worker.source_queue()).unwrap();
        let _: () = con.del(worker.processing_queue()).unwrap();
        let _: () = con.del(worker.unacked_queue()).unwrap();
        let _: () = con
            .lpush(worker.source_queue(), sample_job_payload(1))
            .unwrap();

        {
            let mut m: MessageGuard<Message> = worker.next().unwrap().unwrap();
            m.reject();
        }

        assert_eq!(0, con.llen(worker.processing_queue()).unwrap());
        assert_eq!(1, con.llen(worker.unacked_queue()).unwrap());
    }
}
