import { Router } from 'express';
import multer from 'multer';
import { previewImport, commitImport } from '../controllers/importController.js';

const upload = multer({ storage: multer.memoryStorage() });
const router = Router();

router.post('/preview', upload.single('file'), previewImport);
router.post('/', upload.single('file'), commitImport);

export default router;
