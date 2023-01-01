const { Kafka } = require("kafkajs");

const topic_name = process.argv[2] || "Messages";
const message = process.argv[3] || "Empty message";
const partition = process.argv[4] || 0;

createProducer();

async function createProducer() {

  try {

    const kafka = new Kafka({
      clientId: "kafkaClient",
      brokers: ["localhost:9092"]
    });

    const producer = kafka.producer();
    await producer.connect();
    const messageResult = await producer.send({
      topic: topic_name,
      messages : [
        {
          value: message,
          partition: partition
        }
      ]
    });
    console.log("Message sent: ", JSON.stringify(messageResult));
    await producer.disconnect();
  }
  catch (error) {
    console.log(error)
  }
}