const { Kafka } = require("kafkajs");

const topic_name = process.argv[2] || "Messages"

createConsumer();

async function createConsumer() {

  try {

    const kafka = new Kafka({
      clientId: "kafkaClient",
      brokers: ["localhost:9092"]
    });

    const consumer = kafka.consumer({
      groupId: "consumer1"
    });

    await consumer.connect();
    await consumer.subscribe({
      topic: topic_name,
      fromBeginning: true
    })

    await consumer.run({
      eachMessage: async result => {
        console.log(`Partititon ${result.partition} ${result.message.value}`)
      }
    })

  }
  catch (error) {
    console.log(error)
  }
}