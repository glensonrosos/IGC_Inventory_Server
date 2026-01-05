import express from 'express';
import cors from 'cors';
import morgan from 'morgan';
import routes from './routes/index.js';

const app = express();

app.set('etag', false);

app.use(cors());
app.use(express.json({ limit: '2mb' }));
app.use(morgan('dev'));

app.use((req, res, next) => {
  if (String(req.originalUrl || '').startsWith('/api/')) {
    res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.set('Pragma', 'no-cache');
    res.set('Expires', '0');
    res.set('Surrogate-Control', 'no-store');
  }
  next();
});

app.get('/api/health', (req, res) => {
  res.json({ status: 'ok' });
});

app.use('/api', routes);

export default app;
