import StockMovement from '../models/StockMovement.js';

export const listTransactions = async (req, res) => {
  const { page = 1, limit = 20, type, itemCode, startDate, endDate } = req.query;
  const p = Math.max(1, Number(page));
  const l = Math.min(100, Math.max(1, Number(limit)));

  const filter = {};
  if (type) filter.type = type;
  if (itemCode) filter['items.itemCode'] = itemCode;
  if (startDate || endDate) {
    filter.createdAt = {};
    if (startDate) filter.createdAt.$gte = new Date(startDate);
    if (endDate) filter.createdAt.$lte = new Date(endDate);
  }

  const [items, total] = await Promise.all([
    StockMovement.find(filter)
      .sort({ createdAt: -1 })
      .skip((p - 1) * l)
      .limit(l)
      .populate('warehouseId', 'name')
      .lean(),
    StockMovement.countDocuments(filter)
  ]);

  res.json({ items, page: p, limit: l, total, pages: Math.ceil(total / l) });
};
