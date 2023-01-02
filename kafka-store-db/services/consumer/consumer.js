const kafka = require('kafka-node');
const { Client: PgClient } = require('pg');
const weather = require('./weather');
var MongoClient = require('mongodb').MongoClient;
var url = "mongodb://localhost:27017/";

(async () => {
  //const pgClient = new PgClient();
  //await pgClient.connect();

  const kafkaClientOptions = { sessionTimeout: 100, spinDelay: 100, retries: 2 };
  const kafkaClient = new kafka.KafkaClient('localhost:9092', 'consumer-client', kafkaClientOptions);
  
  const topics = [
    { topic: 'weather-topic' }
  ];
  
  const options = {
    autoCommit: true,
    fetchMaxWaitMs: 1000,
    fetchMaxBytes: 1024 * 1024,
    encoding: 'buffer'
  };
  
  const kafkaConsumer = new kafka.Consumer(kafkaClient, topics, options);
  
  kafkaConsumer.on('message', async function(message) {
    console.log('Message received:', message);
    const messageBuffer = new Buffer(message.value, 'binary');

    const decodedMessage = weather.fromBuffer(messageBuffer.slice(0));
    console.log('Decoded Message:', typeof decodedMessage, decodedMessage);


    MongoClient.connect(url, function(err, db) {
      if (err) throw err;
      var dbo = db.db("weatherDb");
      var myobj = { name: "Company Inc", address: "Highway 37" };
      dbo.collection("weathers").insertOne({'degree': decodedMessage.degree, 'date': Date(decodedMessage.date)}, function(err, res) {
        if (err) throw err;
        console.log("1 document inserted");
        db.close();
      });
    });

    //console.log('Insert Response:', insertResponse);
  });
  
  kafkaClient.on('error', (error) => console.error('Kafka client error:', error));
  kafkaConsumer.on('error', (error) => console.error('Kafka consumer error:', error));
})();