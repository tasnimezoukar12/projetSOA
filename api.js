const express = require('express');
const mongoose = require('mongoose');
const app = express();

mongoose.connect('mongodb://localhost:27017/kafkaMessages', {
  useNewUrlParser: true,
  useUnifiedTopology: true
});

const Message = mongoose.model('Message', new mongoose.Schema({
  value: String
}));

app.get('/messages', async (req, res) => {
  const messages = await Message.find();
  res.json(messages);
});

app.listen(3000, () => console.log('API démarrée sur http://localhost:3000'));
