import { Router } from 'express';
import { stockIn, stockOut } from '../controllers/stockController.js';

const router = Router();

router.post('/in', stockIn);
router.post('/out', stockOut);

export default router;
