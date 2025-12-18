import Item from '../models/Item.js';
import StockMovement from '../models/StockMovement.js';

export const stockIn = async (req, res) => {
  const { reference = '', items = [], notes = '' } = req.body || {};
  if (!Array.isArray(items) || items.length === 0) return res.status(400).json({ message: 'items required' });
  const movementItems = [];
  for (const it of items) {
    const { itemCode, qtyPieces, packSize } = it || {};
    if (!itemCode || !Number.isFinite(qtyPieces)) continue;
    let doc = await Item.findOne({ itemCode });
    if (!doc) {
      if (!Number.isFinite(packSize) || packSize <= 0) return res.status(400).json({ message: `packSize required for new item ${itemCode}` });
      doc = await Item.create({ itemCode, packSize, totalQty: 0 });
    }
    doc.totalQty = (doc.totalQty || 0) + Number(qtyPieces);
    await doc.save();
    movementItems.push({ itemCode, qtyPieces: Number(qtyPieces), packSize: doc.packSize });
  }
  const movement = await StockMovement.create({ type: 'IN', reference, items: movementItems, createdBy: req.user?.id, notes });
  res.status(201).json({ message: 'stock in recorded', movement });
};

export const stockOut = async (req, res) => {
  const { reference = '', items = [], notes = '' } = req.body || {};
  if (!Array.isArray(items) || items.length === 0) return res.status(400).json({ message: 'items required' });
  const movementItems = [];
  for (const it of items) {
    const { itemCode, qtyPieces } = it || {};
    if (!itemCode || !Number.isFinite(qtyPieces)) continue;
    const doc = await Item.findOne({ itemCode });
    if (!doc) return res.status(404).json({ message: `Item ${itemCode} not found` });
    doc.totalQty = Math.max(0, (doc.totalQty || 0) - Number(qtyPieces));
    await doc.save();
    movementItems.push({ itemCode, qtyPieces: Number(qtyPieces), packSize: doc.packSize });
  }
  const movement = await StockMovement.create({ type: 'OUT', reference, items: movementItems, createdBy: req.user?.id, notes });
  res.status(201).json({ message: 'stock out recorded', movement });
};
