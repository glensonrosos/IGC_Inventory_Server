import { Router } from 'express';
import multer from 'multer';
import { listOnProcess, importOnProcess, listBatches, dueToday, updateBatch, updateBatchItems, exportBatch, getBatchItems, importOnProcessPallets, getBatchPallets, updateBatchPallets, transferBatchPallets, addBatchPallet, listPalletsByPo } from '../controllers/onProcessController.js';

const router = Router();
const upload = multer();

router.get('/', listOnProcess);
router.post('/import', upload.single('file'), importOnProcess);
router.get('/batches', listBatches);
router.get('/batches/due-today', dueToday);
router.get('/batches/:id/items', getBatchItems);
router.patch('/batches/:id', updateBatch);
router.patch('/batches/:id/items', updateBatchItems);
router.get('/batches/:id/export', exportBatch);

// Pallet-group endpoints
router.post('/pallets/import', upload.single('file'), importOnProcessPallets);
router.get('/batches/:id/pallets', getBatchPallets);
router.get('/pallets/by-po', listPalletsByPo);
router.post('/batches/:id/pallets', addBatchPallet);
router.patch('/batches/:id/pallets', updateBatchPallets);
router.post('/batches/:id/pallets/transfer', transferBatchPallets);

export default router;
