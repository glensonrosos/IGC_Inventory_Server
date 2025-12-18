import dotenv from 'dotenv';
import { connectDB, ensureAdmin, syncModelIndexes } from './setup.js';
import app from './app.js';

dotenv.config();

const PORT = process.env.PORT || 5000;

(async () => {
  await connectDB();
  await ensureAdmin();
  await syncModelIndexes();
  app.listen(PORT, () => console.log(`Server listening on port ${PORT}`));
})();
