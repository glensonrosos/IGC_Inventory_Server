import { Router } from 'express';
import { listEarlyBuy, createEarlyBuy, updateEarlyBuy } from '../controllers/earlyBuyController.js';

const router = Router();

router.get('/', listEarlyBuy);
router.post('/', createEarlyBuy);
router.put('/:id', updateEarlyBuy);

export default router;
