import ImportLog from '../models/ImportLog.js';

export const listImportLogs = async (req, res) => {
  const { page = 1, limit = 20, poNumber, itemCode, type, startDate, endDate } = req.query;
  const p = Math.max(1, Number(page));
  const l = Math.min(100, Math.max(1, Number(limit)));
  const filter = {};
  if (poNumber) filter.poNumber = new RegExp(poNumber, 'i');
  if (itemCode) filter.itemCode = new RegExp(itemCode, 'i');
  if (type) filter.type = type;
  if (startDate || endDate) {
    filter.createdAt = {};
    if (startDate) filter.createdAt.$gte = new Date(startDate);
    if (endDate) filter.createdAt.$lte = new Date(endDate);
  }
  const [items, total] = await Promise.all([
    ImportLog.find(filter).sort({ createdAt: -1 }).skip((p-1)*l).limit(l).lean(),
    ImportLog.countDocuments(filter)
  ]);
  res.json({ items, page: p, limit: l, total, pages: Math.ceil(total/l) });
};
