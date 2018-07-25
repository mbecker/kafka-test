// before running, either globally install kafka-node  (npm install kafka-node)
// or add kafka-node to the dependencies of the local application
var InfiniteLoop = require("infinite-loop");
var il = new InfiniteLoop();

var kafka = require("kafka-node");
var Producer = kafka.Producer;
KeyedMessage = kafka.KeyedMessage;

var client;
KeyedMessage = kafka.KeyedMessage;

var APP_VERSION = "0.8.5";
var APP_NAME = "KafkaProducer";

var topicName = "a516817-kentekens";
var KAFKA_BROKER_IP = "212.47.241.204:32400";

// from the Oracle Event Hub - Platform Cluster Connect Descriptor
var kafkaConnectDescriptor = KAFKA_BROKER_IP;

console.log("Running Module " + APP_NAME + " version " + APP_VERSION);

function initializeKafkaProducer(attempt) {
  try {
    console.log(
      `Try to initialize Kafka Client at ${kafkaConnectDescriptor} and Producer, attempt ${attempt}`
    );
    const client = new kafka.KafkaClient({ kafkaHost: kafkaConnectDescriptor });
    console.log("created client");
    producer = new Producer(client);
    console.log("submitted async producer creation request");
    producer.on("ready", function() {
      console.log("Producer is ready in " + APP_NAME);
    });
    producer.on("error", function(err) {
      console.log(
        "failed to create the client or the producer " + JSON.stringify(err)
      );
    });
  } catch (e) {
    console.log("Exception in initializeKafkaProducer" + JSON.stringify(e));
    console.log("Try again in 5 seconds");
    setTimeout(initializeKafkaProducer, 5000, ++attempt);
  }
} //initializeKafkaProducer
initializeKafkaProducer(1);

var eventPublisher = module.exports;

eventPublisher.publishEvent = function(eventKey, event) {
  km = new KeyedMessage(eventKey, JSON.stringify(event));
  payloads = [{ topic: topicName, messages: [km], partition: 0 }];
  producer.send(payloads, function(err, data) {
    if (err) {
      console.error(
        "Failed to publish event with key " +
          eventKey +
          " to topic " +
          topicName +
          " :" +
          JSON.stringify(err)
      );
    }
    console.log(
      "Published event with key " +
        eventKey +
        " to topic " +
        topicName +
        " :" +
        JSON.stringify(data)
    );
  });
};

var counter = 0;
function addOne() {
  counter++;
  eventPublisher.publishEvent("mykey", { msg: counter, kafka: "aareon" });
}

//add it by calling .add
il.add(addOne, []);

//example calls: (after waiting for three seconds to give the producer time to initialize)
setTimeout(function() {
  il.run();
}, 3000);


/* Kafka Performance Test
https://community.hortonworks.com/content/supportkb/151880/errorexception-thrown-by-the-agent-javarmiserverex.html
./bin/kafka-run-class.sh org.apache.kafka.tools.ProducerPerformance --throughput=2000000 --topic=MY_TOPIC --num-records=50000000 --record-size=2000 --producer-props bootstrap.servers=212.47.241.204:32400 buffer.memory=67108864 batch.size=64000
*/

https://prasanthkothuri.wordpress.com/2016/10/04/benchmarking-apache-kafka-on-openstack-vms/

Single producer publishing 1000 byte messages with no replication
# create kafka topic
./bin/kafka-topics.sh --zookeeper zookeeper.kafka:2181 --create --topic test --partitions 48 --replication-factor 1
# run the producer to publish events to Kafka topic
./bin/kafka-run-class.sh org.apache.kafka.tools.ProducerPerformance --topic test --num-records 50000000 --record-size 100 --throughput -1 --producer-props acks=1 bootstrap.servers=212.47.241.204:32400 buffer.memory=104857600 batch.size=9000

Single producer publishing 500 byte messages with (3x) and with out replication
The objective of this test is to understand the cost of the replication

# create kafka topic (with replication)
bin/kafka-topics.sh --zookeeper habench001:2181 --create --topic testr3 --partitions 48 --replication-factor 3
# publish messages to kafka topic with required settings
bin/kafka-run-class.sh org.apache.kafka.tools.ProducerPerformance --topic testr3 --num-records 30000000 --record-size 500 --throughput -1 --producer-props acks=1 bootstrap.servers=habench101:9092 buffer.memory=104857600 batch.size=6000

Three producers, 3x async replication with different message sizes
The object of the test is to understand the effect of the message size on the producer throughput

# publish 200 byte messages to kafka topic
bin/kafka-run-class.sh org.apache.kafka.tools.ProducerPerformance --topic testr3 --num-records 30000000 --record-size 200 --throughput -1 --producer-props acks=1 bootstrap.servers=habench101:9092 buffer.memory=104857600 batch.size=6000
# publish 500 byte messages to kafka topic
bin/kafka-run-class.sh org.apache.kafka.tools.ProducerPerformance --topic testr3 --num-records 15000000 --record-size 500 --throughput -1 --producer-props acks=1 bootstrap.servers=habench101:9092 buffer.memory=104857600 batch.size=6000

# publish 1000 byte messages to kafka topic
bin/kafka-run-class.sh org.apache.kafka.tools.ProducerPerformance --topic testr3 --num-records 10000000 --record-size 1000 --throughput -1 --producer-props acks=1 bootstrap.servers=habench101:9092 buffer.memory=104857600 batch.size=6000
