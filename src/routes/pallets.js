import { Router } from 'express';
import { listPallets, createPallet, updatePalletStatus, updateComposition } from '../controllers/palletsController.js';

const router = Router();

router.get('/', listPallets);
router.post('/', createPallet);
router.put('/:palletId/status', updatePalletStatus);
router.put('/:palletId/composition', updateComposition);

export default router;
