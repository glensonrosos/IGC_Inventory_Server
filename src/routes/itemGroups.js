import { Router } from 'express';
import multer from 'multer';
import { createItemGroup, listItemGroups, importItemGroups, deleteItemGroup, updateItemGroup, renameItemGroup } from '../controllers/itemGroupsController.js';

const router = Router();
const upload = multer();

router.get('/', listItemGroups);
router.post('/', createItemGroup);
router.post('/import', upload.single('file'), importItemGroups);
router.put('/:id', updateItemGroup);
router.put('/:id/rename', renameItemGroup);
router.delete('/:id', deleteItemGroup);

export default router;
