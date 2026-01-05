import { Router } from 'express';
import multer from 'multer';
import { createOrder, assignPallets, autoAllocate, previewFulfilledCsv, commitFulfilledCsv, createFulfilledManual, createUnfulfilledOrder, listUnfulfilledOrders, listFulfilledImports, getUnfulfilledOrderById, updateUnfulfilledOrderStatus, updateUnfulfilledOrderDetails, getImportedOrderById, updateImportedOrderStatus, updateImportedOrderDetails, checkImportedOrderStock, checkUnfulfilledOrderStock, palletPicker, onWaterDetails, onProcessDetails, rebalanceProcessingOrders } from '../controllers/ordersController.js';

const router = Router();
const upload = multer({ storage: multer.memoryStorage() });

router.post('/', createOrder);
router.post('/fulfilled/preview', upload.single('file'), previewFulfilledCsv);
router.post('/fulfilled', upload.single('file'), commitFulfilledCsv);
router.post('/fulfilled/manual', createFulfilledManual);
router.get('/fulfilled/imports', listFulfilledImports);
router.get('/fulfilled/imports/:id', getImportedOrderById);
router.get('/fulfilled/imports/:id/stock-check', checkImportedOrderStock);
router.put('/fulfilled/imports/:id/status', updateImportedOrderStatus);
router.put('/fulfilled/imports/:id', updateImportedOrderDetails);

router.get('/pallet-picker', palletPicker);
router.get('/pallet-picker/on-water', onWaterDetails);
router.get('/pallet-picker/on-process', onProcessDetails);

router.post('/unfulfilled', createUnfulfilledOrder);
router.get('/unfulfilled', listUnfulfilledOrders);
router.post('/unfulfilled/rebalance-processing', rebalanceProcessingOrders);
router.get('/unfulfilled/:id', getUnfulfilledOrderById);
router.get('/unfulfilled/:id/stock-check', checkUnfulfilledOrderStock);
router.put('/unfulfilled/:id/status', updateUnfulfilledOrderStatus);
router.put('/unfulfilled/:id', updateUnfulfilledOrderDetails);
router.put('/:id/assign-pallets', assignPallets);
router.put('/:id/auto-allocate', autoAllocate);

export default router;
