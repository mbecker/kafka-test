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
./bin/kafka-run-class.sh org.apache.kafka.tools.ProducerPerformance --throughput=1000 --topic=MY_TOPIC --num-records=50000000 --record-size=200 --producer-props bootstrap.servers=212.47.241.204:32400 buffer.memory=67108864 batch.size=64000
*/