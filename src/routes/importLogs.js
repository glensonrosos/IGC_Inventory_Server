import { Router } from 'express';
import { listImportLogs } from '../controllers/importLogsController.js';

const router = Router();

router.get('/', listImportLogs);

export default router;
