import { Router } from 'express';
import { listTransactions } from '../controllers/transactionsController.js';

const router = Router();

router.get('/', listTransactions);

export default router;
