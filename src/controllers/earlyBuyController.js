import EarlyBuyOrder from '../models/EarlyBuyOrder.js';

function pad(n, w = 4) { return String(n).padStart(w, '0'); }

async function nextEarlyBuyId() {
  // Find highest EORD-#### and increment
  const last = await EarlyBuyOrder.findOne({ id: { $regex: /^EORD-\d{4}$/ } })
    .sort({ id: -1 })
    .select('id')
    .lean();
  if (!last || !last.id) return `EORD-${pad(1)}`;
  const m = String(last.id).match(/EORD-(\d{4})/);
  const n = m ? parseInt(m[1], 10) : 0;
  return `EORD-${pad((Number.isFinite(n) ? n : 0) + 1)}`;
}

export const listEarlyBuy = async (req, res) => {
  const docs = await EarlyBuyOrder.find({}).sort({ createdAt: -1 }).lean();
  return res.json(docs);
};

export const updateEarlyBuy = async (req, res) => {
  const idParam = String(req.params.id || '').trim();
  if (!idParam) return res.status(400).json({ message: 'Missing order id' });
  const b = req.body || {};
  const updatedBy = String(req.user?.username || req.user?.name || req.user?.email || '').trim();
  const required = ['customerEmail','customerName','customerPhone','shippingAddress','createdAt'];
  for (const k of required) {
    if (!String(b[k] || '').trim()) {
      return res.status(400).json({ message: `${k} is required` });
    }
  }
  const createdAt = String(b.createdAt).slice(0,10);
  const estFulfillment = String(b.estFulfillment || '').slice(0,10);
  const estDelivered = String(b.estDelivered || '').slice(0,10);
  const today = new Date().toISOString().slice(0,10);
  if (createdAt > today) return res.status(400).json({ message: 'Created Order Date cannot be in the future' });
  if (!estFulfillment) return res.status(400).json({ message: 'Estimated ShipDate for Customer is required' });
  if (estFulfillment && estFulfillment < createdAt) return res.status(400).json({ message: 'Estimated ShipDate must be \u2265 Created Order Date' });
  if (String(b.status || '').trim().toLowerCase() === 'shipped' && !estDelivered) {
    return res.status(400).json({ message: 'Estimated Arrival Date is required when status is SHIPPED' });
  }
  if (estDelivered && estDelivered < estFulfillment) return res.status(400).json({ message: 'Estimated Arrival Date must be \u2265 Estimated ShipDate' });

  const lines = Array.isArray(b.lines) ? b.lines : [];
  if (!lines.some((l)=> Number(l?.qty||0) > 0)) {
    return res.status(400).json({ message: 'Please add at least one pallet with quantity > 0' });
  }

  const update = {
    status: String(b.status || 'processing').trim().toLowerCase(),
    ...(updatedBy ? { updatedBy } : {}),
    warehouseId: 'MPG',
    createdAtYmd: createdAt,
    estFulfillment,
    estDelivered,
    customerEmail: String(b.customerEmail||'').trim(),
    customerName: String(b.customerName||'').trim(),
    customerPhone: String(b.customerPhone||'').trim(),
    shippingAddress: String(b.shippingAddress||'').trim(),
    originalPrice: String(b.originalPrice||'').trim(),
    shippingPercent: String(b.shippingPercent||'').trim(),
    discountPercent: String(b.discountPercent||'').trim(),
    notes: String(b.notes||'').trim(),
    lines: lines.map((l)=> ({
      groupName: String(l?.groupName||'').trim(),
      lineItem: String(l?.lineItem||'').trim(),
      palletName: String(l?.palletName||'').trim(),
      qty: Math.max(0, Math.floor(Number(l?.qty||0)))
    })),
  };

  const doc = await EarlyBuyOrder.findOneAndUpdate({ id: idParam }, update, { new: true }).lean();
  if (!doc) return res.status(404).json({ message: 'Order not found' });
  return res.json(doc);
};

export const createEarlyBuy = async (req, res) => {
  const b = req.body || {};
  const updatedBy = String(req.user?.username || req.user?.name || req.user?.email || '').trim();
  const required = ['customerEmail','customerName','customerPhone','shippingAddress','createdAt'];
  for (const k of required) {
    if (!String(b[k] || '').trim()) {
      return res.status(400).json({ message: `${k} is required` });
    }
  }
  const createdAt = String(b.createdAt).slice(0,10);
  const estFulfillment = String(b.estFulfillment || '').slice(0,10);
  const estDelivered = String(b.estDelivered || '').slice(0,10);
  const today = new Date().toISOString().slice(0,10);
  if (createdAt > today) return res.status(400).json({ message: 'Created Order Date cannot be in the future' });
  if (estFulfillment && estFulfillment < createdAt) return res.status(400).json({ message: 'Estimated ShipDate must be \\u2265 Created Order Date' });
  if (estDelivered && estDelivered < estFulfillment) return res.status(400).json({ message: 'Estimated Arrival Date must be \\u2265 Estimated ShipDate' });

  const lines = Array.isArray(b.lines) ? b.lines : [];
  if (!lines.some((l)=> Number(l?.qty||0) > 0)) {
    return res.status(400).json({ message: 'Please add at least one pallet with quantity > 0' });
  }

  const id = await nextEarlyBuyId();
  const doc = await EarlyBuyOrder.create({
    id,
    status: String(b.status || 'processing').trim().toLowerCase(),
    ...(updatedBy ? { updatedBy } : {}),
    warehouseId: 'MPG',
    createdAtYmd: createdAt,
    estFulfillment,
    estDelivered,
    customerEmail: String(b.customerEmail||'').trim(),
    customerName: String(b.customerName||'').trim(),
    customerPhone: String(b.customerPhone||'').trim(),
    shippingAddress: String(b.shippingAddress||'').trim(),
    originalPrice: String(b.originalPrice||'').trim(),
    shippingPercent: String(b.shippingPercent||'').trim(),
    discountPercent: String(b.discountPercent||'').trim(),
    notes: String(b.notes||'').trim(),
    lines: lines.map((l)=> ({
      groupName: String(l?.groupName||'').trim(),
      lineItem: String(l?.lineItem||'').trim(),
      palletName: String(l?.palletName||'').trim(),
      qty: Math.max(0, Math.floor(Number(l?.qty||0)))
    })),
  });
  return res.status(201).json(doc);
};
