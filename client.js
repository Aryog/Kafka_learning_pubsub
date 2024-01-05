const { Kafka } = require('kafkajs');
exports.kafka = new Kafka({
    clientId: "my-kafka-app",
    brokers: ["192.168.1.70:9092"]
});

