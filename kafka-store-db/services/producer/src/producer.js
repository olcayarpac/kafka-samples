const _ = require('underscore');
const bodyParser = require('body-parser');
const express = require('express');
const Router = require('express-promise-router');
const kafka = require('kafka-node');
const { Client: PgClient } = require('pg');
const weather = require('./weatherSchema');

//const pgClient = new PgClient();
//pgClient.connect();

const kafkaClientOptions = { sessionTimeout: 100, spinDelay: 100, retries: 2 };
const kafkaClient = new kafka.KafkaClient({kafkaHost: 'localhost:9092'});
const kafkaProducer = new kafka.HighLevelProducer(kafkaClient);

kafkaClient.on('error', (error) => console.error('Kafka client error: ', error));
kafkaProducer.on('error', (error) => console.error('Kafka producer error: ', error));


const app = express();
const router = new Router();

app.use('/', router);
router.use(bodyParser.json());
router.use(bodyParser.urlencoded({ extended: true }));

router.post('/weather', (req, res) => {
  const { degree } = req.body;
  const parsedTotal = Number(degree);

  if (_.isNaN(parsedTotal)) {
    res.status(400);
    res.json({ error: 'Ensure degree is a valid number.' });
    return;
  }

  const messageBuffer = weather.toBuffer({
    degree: parsedTotal,
    date: Date.now()
  });

  const payload = [{
    topic: 'weather-topic',
    messages: messageBuffer,
    attributes: 1
  }];

  kafkaProducer.send(payload, function(error, result) {
    console.info('Sent payload to Kafka:', payload);

    if (error) {
      console.error('Sending payload failed:');
      res.status(500).json(error);
    } else {
      console.log('Sending payload result:', result);
      res.status(202).json(result);
    }
  });
});

app.listen('5555');
