import { Router } from 'express';
import { listWarehouses, createWarehouse, updateWarehouse, deleteWarehouse } from '../controllers/warehousesController.js';

const router = Router();

router.get('/', listWarehouses);
router.post('/', createWarehouse);
router.put('/:id', updateWarehouse);
router.delete('/:id', deleteWarehouse);

export default router;
