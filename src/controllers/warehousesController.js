import Warehouse from '../models/Warehouse.js';
import WarehouseStock from '../models/WarehouseStock.js';
import PalletGroupStock from '../models/PalletGroupStock.js';

export const listWarehouses = async (req, res) => {
  const q = (req.query.q || '').toString().trim().toLowerCase();
  const filter = q ? { $or: [ { name: new RegExp(q, 'i') }, { address: new RegExp(q, 'i') } ] } : {};
  const docs = await Warehouse.find(filter).sort({ name: 1 }).lean();
  res.json(docs);
};

export const createWarehouse = async (req, res) => {
  const { name, address } = req.body || {};
  if (!name) return res.status(400).json({ message: 'name required' });
  const exists = await Warehouse.findOne({ name });
  if (exists) return res.status(409).json({ message: 'Warehouse already exists' });
  const doc = await Warehouse.create({ name, address: address || '' });
  res.status(201).json(doc);
};

export const updateWarehouse = async (req, res) => {
  const { id } = req.params;
  const allowed = ['name','address'];
  const updates = {};
  for (const k of allowed) if (k in req.body) updates[k] = req.body[k];
  if (!Object.keys(updates).length) return res.status(400).json({ message: 'no updates' });
  const doc = await Warehouse.findByIdAndUpdate(id, updates, { new: true });
  if (!doc) return res.status(404).json({ message: 'Not found' });
  res.json(doc);
};

export const deleteWarehouse = async (req, res) => {
  const { id } = req.params;
  // Block deletion if any stock exists in this warehouse
  const hasStock = await WarehouseStock.exists({ warehouseId: id, qtyPieces: { $gt: 0 } });
  if (hasStock) return res.status(400).json({ message: 'Cannot delete warehouse: it has item stock quantity. Move/clear stock first.' });
  // Also block deletion if any pallet inventory exists in this warehouse
  const hasPallets = await PalletGroupStock.exists({ warehouseId: id, pallets: { $gt: 0 } });
  if (hasPallets) return res.status(400).json({ message: 'Cannot delete warehouse: it has pallet inventory. Move/clear pallet stocks first.' });
  const doc = await Warehouse.findByIdAndDelete(id);
  if (!doc) return res.status(404).json({ message: 'Not found' });
  res.json({ ok: true });
};
