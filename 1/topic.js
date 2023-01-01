const { Kafka } = require("kafkajs");

createTopic();

async function createTopic() {

  try {

    const kafka = new Kafka({
      clientId: "kafkaClient",
      brokers: ["localhost:9092"]
    });

    const admin = kafka.admin();
    console.log("Connecting to kafka broker...");
    await admin.connect();
    await admin.createTopics({
      topics: [
        { topic: 'Logs', numPartitions: 1},
        { topic: 'Messages', numPartitions: 1}

      ]
    })

    console.log("Topic created");
    await admin.disconnect();
  }
  catch (error) {
    console.log(error)
  }
}