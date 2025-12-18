import { Router } from 'express';
import { login, changePassword } from '../controllers/authController.js';
import { authRequired } from '../middleware/auth.js';

const router = Router();
router.post('/login', login);
router.post('/change-password', authRequired, changePassword);

export default router;
