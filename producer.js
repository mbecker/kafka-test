var kafka = require("kafka-node");
var Producer = kafka.Producer;
var KeyedMessage = kafka.KeyedMessage;
var Client = kafka.Client;
var client = new Client("212.47.241.204:32400");
var argv = require("optimist").argv;
var topic = argv.topic || "a516817-kentekens";
var p = argv.p || 0;
var a = argv.a || 0;
var producer = new Producer(client, { requireAcks: 1 });

producer.on("ready", function() {
  

  let i, x = 0;
  while (i < 10) {
    let message = "Message: " + x;
    let keyedMessage = new KeyedMessage("keyed", "a keyed message: " + x);
    x = x + 1;
    
    producer.send(
        [
          {
            topic: topic,
            partition: p,
            messages: [message, keyedMessage],
            attributes: a
          }
        ],
        function(err, result) {
          console.log(err || result);
          process.exit();
          console.log("-- " + x + " ---");
          if(err) {
              console.log(err);
          } else {
              console.log("Result: \n" + result);
          }

        }
      );
}
  
});

producer.on("error", function(err) {
  console.log("error", err);
});
