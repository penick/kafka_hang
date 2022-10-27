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
    message::Message,
};
use snafu::ResultExt;
use snafu::Snafu;
use stream_cancel::Tripwire;
use tokio::spawn;

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type Result<T> = std::result::Result<T, Error>;

const NUM_TOPICS: usize = 100;
const NUM_MSGS: usize = 200;

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    let bootstrap_servers = std::env::args().nth(1).expect("no bootstraps servers given");

    let admin = Arc::new(create_admin(&bootstrap_servers));

    let topics: Vec<String> = { 1..NUM_TOPICS + 1 }
        .into_iter()
        .map(|i| format!("test.rdkafka.{}", i))
        .collect();

    for _ in 0..999999999 {
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

        println!("created topics");

        let mut futures = vec![];
        let (trigger, tripwire) = Tripwire::new();

        for topic in &topics {
            let producer: &FutureProducer = &ClientConfig::new()
                .set("bootstrap.servers", &bootstrap_servers)
                .set("message.timeout.ms", "5000")
                .create()
                .expect("producer creation error");

            let producer_futures = (0..NUM_MSGS)
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

            let _result = join_all(producer_futures).await;
            //println!("wrote messages to topic {}: {:?}", topic, result);
        }


        for topic in &topics {
            let tripwire = tripwire.clone();
            let topic = topic.clone();
            let bootstrap_servers = bootstrap_servers.clone();
            let task = spawn(async move {
                let client_id = &topic;
                let consumer = create_consumer(
                    &bootstrap_servers, 
                    client_id, 
                    std::slice::from_ref(&topic.as_str())
                )
                    .expect("consumer creation failure");
                let mut stream = consumer.stream();
                let mut msg_count = 0;
                let mut err_count = 0;
                let mut err_store_offset_count = 0;

                loop {
                    tokio::select! {
                      _ = tripwire.clone() => {
                        println!("all done {} messages = {}, errors = {}, errors store offset = {}", topic, msg_count, err_count, err_store_offset_count);
                        break;
                      },
                      message = stream.next() => match message {
                          None => {panic!("shouldn't happen!"); },  // WHY?
                          Some(Err(_error)) => { err_count += 1; println!("got error: {:?}", _error) },
                          Some(Ok(msg)) => {
                              msg_count += 1;
                              tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                              if let Err(_err)  = consumer.store_offset(msg.topic(), msg.partition(), msg.offset()) {
                                  err_store_offset_count += 1;
                              }
                              //println!("got message: {:?}", _msg);
                          }
                      },
                    }
                }
            });
            futures.push(task);
        }


        // Give time for consumers to get messages
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        println!("drop topics");
        for topic in &topics {
            admin
                .delete_topics(&[topic], &AdminOptions::new())
                .await
                .expect("couldn't delete topic");
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

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
    bootstrap_servers: &str,
    client_id: &str,
    topics: &[&str],
) -> crate::Result<StreamConsumer<KafkaStatisticsContext>> {
    let mut client_config = ClientConfig::new();
    client_config
        .set("group.id", "pipeline-vector-group-v1")
        .set("bootstrap.servers", bootstrap_servers)
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

fn create_admin(
    bootstrap_servers: &str,
) -> AdminClient<DefaultClientContext> {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", bootstrap_servers);
    config.create().expect("admin client creation failed")
}
