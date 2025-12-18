import WarehouseStock from '../models/WarehouseStock.js';
import Item from '../models/Item.js';

export const listWarehouseStock = async (req, res) => {
  const { warehouseId, q } = req.query;
  if (!warehouseId) return res.status(400).json({ message: 'warehouseId required' });
  const filter = { warehouseId };
  if (q) {
    // itemCode prefix contains
    filter.itemCode = { $regex: q.toString(), $options: 'i' };
  }
  const docs = await WarehouseStock.find(filter).sort({ itemCode: 1 }).lean();
  if (!docs.length) return res.json(docs);
  const codes = Array.from(new Set(docs.map(d => d.itemCode).filter(Boolean)));
  const items = await Item.find({ itemCode: { $in: codes } })
    .select('itemCode itemGroup description color')
    .lean();
  const map = new Map(items.map(i => [i.itemCode, i]));
  const enriched = docs.map(d => {
    const it = map.get(d.itemCode) || {};
    return {
      ...d,
      itemGroup: it.itemGroup || '',
      description: it.description || '',
      color: it.color || '',
    };
  });
  res.json(enriched);
};
