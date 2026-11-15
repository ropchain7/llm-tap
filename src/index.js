const express = require('express');
const proxyRouter = require('./proxy');

const app = express();
const PORT = process.env.PORT || 8000;
const TARGET_BASE_URL = process.env.TARGET_BASE_URL || 'https://openrouter.ai/api/v1';

app.use(express.json({ limit: '10mb' }));
app.use(express.text({ type: 'text/*', limit: '10mb' }));

app.use(proxyRouter);

app.use((err, req, res, next) => {
  console.error('Unexpected error:', err.message);
  console.error(err.stack);
  
  if (res.headersSent) {
    return next(err);
  }
  
  res.status(500).json({
    error: 'Internal Server Error',
    message: err.message
  });
});

app.listen(PORT, () => {
  console.log(`OpenRouter Logging Gateway started`);
  console.log(`Listening on port: ${PORT}`);
  console.log(`Target URL: ${TARGET_BASE_URL}`);
});
