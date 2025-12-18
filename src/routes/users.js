import { Router } from 'express';
import { listUsers, createUser, setUserEnabled, resetUserPassword, setUserRole } from '../controllers/usersController.js';

const router = Router();

router.get('/', listUsers);
router.post('/', createUser);
router.patch('/:id/status', setUserEnabled);
router.patch('/:id/role', setUserRole);
router.post('/:id/reset-password', resetUserPassword);

export default router;
