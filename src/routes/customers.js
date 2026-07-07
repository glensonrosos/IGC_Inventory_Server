import { Router } from 'express';
import { listCustomers, createCustomer, updateCustomer, setCustomerEnabled } from '../controllers/customersController.js';

const router = Router();

router.get('/', listCustomers);
router.post('/', createCustomer);
router.patch('/:id', updateCustomer);
router.patch('/:id/status', setCustomerEnabled);

export default router;
