import { Router } from 'express';
import { getSettings, updateSettings } from '../controllers/settingsController.js';

const router = Router();

router.get('/', getSettings);
router.put('/', updateSettings);

export default router;
