import { Router } from 'express';
import { inventoryReport, exportReport, palletSummaryByGroup, palletSalesReport } from '../controllers/reportsController.js';

const router = Router();

router.post('/inventory', inventoryReport);
router.get('/export', exportReport);
router.get('/pallet-summary-by-group', palletSummaryByGroup);
router.get('/pallet-sales', palletSalesReport);

export default router;
