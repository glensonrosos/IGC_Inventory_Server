import { Router } from 'express';
import multer from 'multer';
import { importPreview, importCommit, listGroupOverview, groupDetails, createAdjustment } from '../controllers/palletInventoryController.js';

const router = Router();
const upload = multer();

router.post('/import/preview', upload.single('file'), importPreview);
router.post('/import', upload.single('file'), importCommit);
router.post('/adjustments', createAdjustment);
router.get('/groups', listGroupOverview);
router.get('/groups/:groupName', groupDetails);

export default router;
