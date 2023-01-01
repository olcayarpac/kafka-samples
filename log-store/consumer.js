const { Kafka } = require("kafkajs");

createConsumer();

async function createConsumer() {
  try {
    const kafka = new Kafka({
      clientId: "kafka_log_store_client",
      brokers: ["localhost:9092"]
    });

    const consumer = kafka.consumer({
      groupId: "log_store_consumer_group"
    });

    console.log("Connecting to consumer..");
    await consumer.connect();
    console.log("Connected");

    // Consumer Subscribe..
    await consumer.subscribe({
      topic: "LogStoreTopic",
      fromBeginning: true
    });

    await consumer.run({
      eachMessage: async result => {
        console.log(
          `Incoming message: ${result.message.value}, Par => ${result.partition}`
        );
      }
    });
  } catch (error) {
    console.log("Error: ", error);
  }
}