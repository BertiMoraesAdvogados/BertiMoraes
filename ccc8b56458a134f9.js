// Webhook WhatsApp Cloud API -> Maestro
// Responde ao handshake GET do Meta e repassa POSTs ao Maestro com o secret.
// Deploy no Render: Build: npm install express | Start: node app.js
// Env var: VERIFY_TOKEN (use o valor abaixo no painel do Meta também)

const express = require('express');
const app = express();
app.use(express.json({ limit: '1mb' }));

const PORT = process.env.PORT || 3000;
const VERIFY_TOKEN = process.env.VERIFY_TOKEN || 'TROQUE_ESTE_TOKEN';
const MAESTRO_URL = 'https://maestro.adapta.one/webhooks/generic/58626084cade76f9f18abe3516618ba4822ab8ed65971e30a7d0fef3169b7484';
const MAESTRO_SECRET = 'd8fc78d01792f78bfa70b4eb228ab0b12cf7fb253b29807912ac16c58e729b90';

// Handshake de verificação do Meta Cloud API
app.get('/', (req, res) => {
  const mode = req.query['hub.mode'];
  const token = req.query['hub.verify_token'];
  const challenge = req.query['hub.challenge'];
  if (mode === 'subscribe' && token === VERIFY_TOKEN) {
    console.log('WEBHOOK VERIFICADO');
    res.status(200).send(challenge);
  } else {
    res.status(403).end();
  }
});

// Eventos do Meta -> repassa ao Maestro
app.post('/', (req, res) => {
  const payload = JSON.stringify(req.body);
  const url = new URL(MAESTRO_URL);
  const options = {
    hostname: url.hostname,
    path: url.pathname,
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Webhook-Secret': MAESTRO_SECRET,
      'Content-Length': Buffer.byteLength(payload),
    },
  };
  const preq = require('https').request(options, pres => {
    console.log('Maestro respondeu:', pres.statusCode);
    res.status(200).end();
  });
  preq.on('error', e => {
    console.error('Erro ao repassar ao Maestro:', e.message);
    res.status(200).end(); // sempre 200 ao Meta para evitar reenvios em loop
  });
  preq.write(payload);
  preq.end();
});

app.listen(PORT, () => console.log(`Webhook WhatsApp ativo na porta ${PORT}`));
