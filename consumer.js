const { Kafka } = require('kafkajs');
const mongoose = require('mongoose');

// Connexion à MongoDB
mongoose.connect('mongodb://localhost:27017/kafkaMessages', {
  useNewUrlParser: true,
  useUnifiedTopology: true
});

// Modèle de message
const Message = mongoose.model('Message', new mongoose.Schema({
  value: String
}));

// Configuration Kafka
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'test-group' });

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'test-topic', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const msgValue = message.value.toString();

      console.log({ value: msgValue });

      // Enregistrement dans MongoDB
      try {
        await Message.create({ value: msgValue });
        console.log('Message enregistré dans MongoDB');
      } catch (err) {
        console.error('Erreur lors de l\'enregistrement dans MongoDB', err);
      }
    },
  });
};

run().catch(console.error);


