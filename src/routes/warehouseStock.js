import { Router } from 'express';
import { listWarehouseStock } from '../controllers/warehouseStockController.js';

const router = Router();

router.get('/', listWarehouseStock);

export default router;
