<div align="center">
  <img alt="Orizuru logo" src="https://github.com/rubik/orizuru/raw/master/logo.png" height="130" />
</div>

<div align="center">
  <h1>Orizuru (折鶴)</h1>
  <p>A reliable, scalable and flexible Redis message queue for Rust.</p>
  <a href="https://travis-ci.org/rubik/orizuru">
    <img src="https://img.shields.io/travis/rubik/orizuru?style=for-the-badge" alt="Build">
  </a>
  <a href="https://crates.io/crates/orizuru">
   <img src="https://img.shields.io/crates/d/orizuru?style=for-the-badge" alt="Downloads (all time)">
  <a>
  <a href="https://github.com/rubik/orizuru/blob/master/LICENSE">
    <img src="https://img.shields.io/crates/l/orizuru?style=for-the-badge" alt="ISC License">
  </a>
  <br>
  <br>
</div>


# Design
Queues are backed by Redis lists and as such they support multiple producers
and consumers:

    * To publish a message to a *source* queue, a producer runs an
      [`LPUSH`](https://redis.io/commands/lpush) command;
    * Periodically, a consumer fetches messages from the *source* queue and
      pushes them to its own *processing* queue. Then, it may choose to
      acknowledge them or reject them. Acknowledged messages are removed from
      the *processing* queue, while rejected messages are moved to the *unack*
      queue.

Each queue can have an unlimited number of concurrent consumers.
Consumers fetch messages with the
[`BRPOPLPUSH`](https://redis.io/commands/brpoplpush) command, that blocks until
at least one message is available on the source queue, and then pushes it to
its processing queue. This operation is atomic, which means that only one
consumer can receive the message even if there are multiple consumers fetching
messages from the *source* queue.

Optionally, unacknowledged messages can be collected by the garbage collector
that periodically returns them to the source queue. If the garbage collector
does not run, *unack* queues are essentially dead-letter queues and could grow
without bound.
