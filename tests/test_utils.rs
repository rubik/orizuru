macro_rules! redis_fixture {
    ($client:ident, $con:ident, $consumer:ident, $code:block) => {
        let u = Uuid::new_v4();
        let $client = redis::Client::open("redis://127.0.0.1:6379/").unwrap();
        let mut $con = $client.get_connection().unwrap();
        let con2 = $client.get_connection().unwrap();
        let $consumer = Consumer::new(
            format!("consumer-{}", u).into(),
            format!("q-{}", u).into(),
            con2,
        );
        let _: () = $con.del($consumer.source_queue()).unwrap();
        let _: () = $con.del($consumer.processing_queue()).unwrap();
        let _: () = $con.del($consumer.unacked_queue()).unwrap();

        $code

        let _: () = $con.del($consumer.source_queue()).unwrap();
        let _: () = $con.del($consumer.processing_queue()).unwrap();
        let _: () = $con.del($consumer.unacked_queue()).unwrap();
    };

    ($client:ident, $con:ident, $consumer:ident, "p", $producer:ident, $code:block) => {
        redis_fixture!($client, $con, $consumer, {
            let con3 = $client.get_connection().unwrap();
            let $producer = Producer::new(
                $consumer.source_queue().into(),
                con3,
            );

            $code
        });
    };

    ($client:ident, $con:ident, $consumer:ident, "g", $gc:ident, $code:block) => {
        redis_fixture!($client, $con, $consumer, {
            let con3 = $client.get_connection().unwrap();
            let $gc = GC::new(con3);

            $code
        });
    };
}
