import { Router } from 'express';
import { inventoryReport, exportReport, palletSummaryByGroup } from '../controllers/reportsController.js';

const router = Router();

router.post('/inventory', inventoryReport);
router.get('/export', exportReport);
router.get('/pallet-summary-by-group', palletSummaryByGroup);

export default router;
