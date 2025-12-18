import { Router } from 'express';
import multer from 'multer';
import { listItems, createItem, getItem, updateItem, importItemsExcel, deleteItem } from '../controllers/itemsController.js';

const router = Router();
const upload = multer({ storage: multer.memoryStorage() });
router.get('/', listItems);
router.post('/', createItem);
router.post('/registry-import', upload.single('file'), importItemsExcel);
router.get('/:itemCode', getItem);
router.put('/:itemCode', updateItem);
router.delete('/:itemCode', deleteItem);

export default router;
