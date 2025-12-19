import { Router } from 'express';
import { listShipments, createTransfer, createPalletTransfer, deliverShipment, updateEDD, dueToday, backfillReferences, backfillOwNumbers } from '../controllers/shipmentsController.js';

const router = Router();

router.get('/', listShipments);
router.get('/due-today', dueToday);
router.post('/transfer', createTransfer);
router.post('/transfer-pallet', createPalletTransfer);
router.post('/:id/deliver', deliverShipment);
router.put('/:id/edd', updateEDD);
router.post('/backfill-references', backfillReferences);
router.post('/backfill-ow-numbers', backfillOwNumbers);

export default router;
