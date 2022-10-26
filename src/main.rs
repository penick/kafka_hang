use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use futures::StreamExt;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{
    consumer::{Consumer, ConsumerContext, StreamConsumer},
    ClientConfig, ClientContext, Statistics,
};
use snafu::ResultExt;
use snafu::Snafu;
use stream_cancel::Tripwire;
use tokio::spawn;
use tokio::sync::Notify;

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type Result<T> = std::result::Result<T, Error>;

const NUM_TOPICS: usize = 20;
const NUM_MSGS: usize = 100;

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    let admin = Arc::new(create_admin());

    let topics: Vec<String> = { 1..NUM_TOPICS + 1 }
        .into_iter()
        .map(|i| format!("topic{}", i))
        .collect();

    for topic in &topics {
        //println!("adding topic {}", topic);
        let topic = [&NewTopic::new(
            topic.as_str(),
            1,
            TopicReplication::Fixed(-1),
        )];
        admin
            .create_topics(topic, &AdminOptions::new())
            .await
            .expect("admin creation failure");
    }

    for _ in 0..999999999 {
        let mut futures = vec![];
        let (trigger, tripwire) = Tripwire::new();

        let notify = Arc::new(Notify::new());
        let count = Arc::new(AtomicUsize::new(0));

        for topic in &topics {
            let topic = topic.clone();
            let notify = notify.clone();
            let count = count.clone();
            let task = spawn(async move {
                let topic = &topic;
                let producer: &FutureProducer = &ClientConfig::new()
                    .set("bootstrap.servers", "localhost")
                    .set("message.timeout.ms", "5000")
                    .create()
                    .expect("producer creation error");

                let futures = (0..NUM_MSGS)
                    .map(|i| async move {
                        // The send operation on the topic returns a future, which will be
                        // completed once the result or failure from Kafka is received.
                        let delivery_status = producer
                            .send(
                                FutureRecord::to(topic)
                                    .payload(&format!("Message {}", i))
                                    .key(&format!("Key {}", i))
                                    .headers(OwnedHeaders::new().add("k1", "v1")),
                                Duration::from_secs(0),
                            )
                            .await;
                        delivery_status
                    })
                    .collect::<Vec<_>>();

                loop {
                    tokio::select! {
                      _ = join_all(futures) => {
                        //let topic = &[topic.as_str()];
                        count.fetch_add(1, Ordering::SeqCst);
                        notify.notify_one();
                        //println!("submitted messages");
                        break;
                      },
                    }
                }
            });
            futures.push(task);
        }

        for topic in &topics {
            let tripwire = tripwire.clone();
            let topic = topic.clone();
            let task = spawn(async move {
                let client_id = &topic;
                let consumer = create_consumer(client_id, std::slice::from_ref(&topic.as_str()))
                    .expect("consumer creation failure");
                let mut stream = consumer.stream();
                let mut msg_count = 0;
                let mut err_count = 0;

                loop {
                    tokio::select! {
                      _ = tripwire.clone() => {
                        println!("all done {} messages = {}, errors = {}", topic, msg_count, err_count);
                        break;
                      },
                      message = stream.next() => match message {
                          None => {panic!("shouldn't happen!"); },  // WHY?
                          //Some(Err(error)) => println!("got error: {:?}", error),
                          Some(Err(_error)) => { err_count += 1; },
                          Some(Ok(_msg)) => {
                              msg_count += 1;
                              tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                              //println!("got message: {:?}", _msg);
                          }
                      },
                    }
                }
            });
            futures.push(task);
        }

        while count.load(Ordering::SeqCst) != NUM_TOPICS {
            notify.notified().await;
            //println!("notified {}", count.load(Ordering::SeqCst));
        }

        // Give time for consumers to get messages
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

        for topic in &topics {
            admin
                .delete_topics(&[topic], &AdminOptions::new())
                .await
                .expect("couldn't delete topic");
        }

        drop(trigger);

        //println!("joining");
        let len = futures.len();
        let _ = join_all(futures).await;
        println!("complete {} {}", chrono::offset::Local::now(), len);
    }
}

#[derive(Debug, Snafu)]
enum BuildError {
    #[snafu(display("Could not create Kafka consumer: {}", source))]
    KafkaCreateError { source: rdkafka::error::KafkaError },
    #[snafu(display("Could not subscribe to Kafka topics: {}", source))]
    KafkaSubscribeError { source: rdkafka::error::KafkaError },
}

pub(crate) struct KafkaStatisticsContext;

impl ClientContext for KafkaStatisticsContext {
    fn stats(&self, _statistics: Statistics) {
        //println!("Stats {:?}", statistics);
    }
}

impl ConsumerContext for KafkaStatisticsContext {}

fn create_consumer(
    client_id: &str,
    topics: &[&str],
) -> crate::Result<StreamConsumer<KafkaStatisticsContext>> {
    let mut client_config = ClientConfig::new();
    client_config
        .set("group.id", "pipeline-vector-group-v1")
        .set("bootstrap.servers", "localhost")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .set("socket.timeout.ms", "60000")
        .set("fetch.wait.max.ms", "100")
        .set("enable.partition.eof", "false")
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "5000")
        .set("enable.auto.offset.store", "false")
        .set("statistics.interval.ms", "1000")
        .set("client.id", client_id);

    let consumer = client_config
        .create_with_context::<_, StreamConsumer<_>>(KafkaStatisticsContext)
        .context(KafkaCreateSnafu)?;
    consumer.subscribe(topics).context(KafkaSubscribeSnafu)?;

    Ok(consumer)
}

fn create_admin() -> AdminClient<DefaultClientContext> {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", "localhost");

    config.create().expect("admin client creation failed")
}
