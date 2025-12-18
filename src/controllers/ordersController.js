import Order from '../models/Order.js';
import Pallet from '../models/Pallet.js';
import { autoAllocateOrder } from '../services/allocation.js';
import * as XLSX from 'xlsx';
import Warehouse from '../models/Warehouse.js';
import ItemGroup from '../models/ItemGroup.js';
import PalletGroupStock from '../models/PalletGroupStock.js';
import PalletGroupTxn from '../models/PalletGroupTxn.js';
import FulfilledOrderImport from '../models/FulfilledOrderImport.js';
import UnfulfilledOrder from '../models/UnfulfilledOrder.js';

const normalizeStr = (v) => (v == null ? '' : String(v)).trim();

const normalizeHeaderKey = (v) =>
  normalizeStr(v)
    .replace(/\u00a0/g, ' ')
    .replace(/\s+/g, ' ')
    .toLowerCase();

const getCol = (row, header) => {
  if (!row || typeof row !== 'object') return '';
  if (Object.prototype.hasOwnProperty.call(row, header)) return row[header];
  const want = normalizeHeaderKey(header);
  for (const k of Object.keys(row)) {
    if (normalizeHeaderKey(k) === want) return row[k];
  }
  return '';
};

const applyInventoryDeltaForOrder = async ({ warehouseId, orderNumber, lines, committedBy, deltaSign, allowNegative, reason }) => {
  // deltaSign: -1 => deduct, +1 => restore
  const applied = [];
  try {
    for (const ln of lines) {
      const qty = Math.floor(Number(ln.qty || 0));
      if (!Number.isFinite(qty) || qty <= 0) continue;

      let stock;
      if (deltaSign < 0) {
        if (allowNegative) {
          stock = await PalletGroupStock.findOneAndUpdate(
            { warehouseId, groupName: ln.groupName },
            { $inc: { pallets: -qty } },
            { upsert: true, new: true, setDefaultsOnInsert: true }
          );
        } else {
          stock = await PalletGroupStock.findOneAndUpdate(
            { warehouseId, groupName: ln.groupName, pallets: { $gte: qty } },
            { $inc: { pallets: -qty } },
            { new: true }
          );
          if (!stock) throw new Error(`Insufficient pallets for ${ln.groupName}`);
        }
      } else {
        stock = await PalletGroupStock.findOneAndUpdate(
          { warehouseId, groupName: ln.groupName },
          { $inc: { pallets: qty } },
          { upsert: true, new: true, setDefaultsOnInsert: true }
        );
      }

      const txn = await PalletGroupTxn.create({
        poNumber: normalizeStr(orderNumber),
        groupName: ln.groupName,
        warehouseId,
        palletsDelta: deltaSign < 0 ? -qty : qty,
        status: 'Adjustment',
        reason: reason || '',
        committedBy: committedBy || '',
      });
      applied.push({ groupName: ln.groupName, qty, txnId: txn?._id });
    }
    return { ok: true, lines: applied.length };
  } catch (e) {
    // rollback best effort
    for (const a of applied) {
      try {
        await PalletGroupStock.updateOne(
          { warehouseId, groupName: a.groupName },
          { $inc: { pallets: deltaSign < 0 ? a.qty : -a.qty } }
        );
      } catch {}
      try {
        if (a.txnId) await PalletGroupTxn.deleteOne({ _id: a.txnId });
      } catch {}
    }
    throw e;
  }
};

const getColByPrefix = (row, headerPrefix) => {
  const direct = getCol(row, headerPrefix);
  if (direct !== '') return direct;
  if (!row || typeof row !== 'object') return '';
  const want = normalizeHeaderKey(headerPrefix);
  for (const k of Object.keys(row)) {
    const got = normalizeHeaderKey(k);
    if (got.startsWith(want)) return row[k];
  }
  return '';
};

const getColByPrefixInfo = (row, headerPrefix) => {
  if (!row || typeof row !== 'object') return { key: '', value: '' };
  if (Object.prototype.hasOwnProperty.call(row, headerPrefix)) {
    return { key: headerPrefix, value: row[headerPrefix] };
  }
  const want = normalizeHeaderKey(headerPrefix);
  for (const k of Object.keys(row)) {
    const got = normalizeHeaderKey(k);
    if (got === want) return { key: k, value: row[k] };
  }
  for (const k of Object.keys(row)) {
    const got = normalizeHeaderKey(k);
    if (got.startsWith(want)) return { key: k, value: row[k] };
  }
  return { key: '', value: '' };
};

const toDateOrNull = (v) => {
  if (v == null || v === '') return null;

  const excelSerialToDate = (num) => {
    if (!Number.isFinite(num)) return null;
    // Excel 1900 date system includes a fake leap day (1900-02-29).
    // For serials >= 60, subtract 1 day to align with real calendar dates.
    const num1900 = num >= 60 ? num - 1 : num;
    // Prefer xlsx's conversion first
    try {
      const dc = XLSX.SSF.parse_date_code(num1900);
      if (dc && dc.y && dc.m && dc.d) {
        const dt = new Date(dc.y, dc.m - 1, dc.d, dc.H || 0, dc.M || 0, dc.S || 0);
        if (!Number.isNaN(dt.getTime())) return dt;
      }
    } catch {}

    // Fallback: convert excel serial days to Unix epoch.
    // 1900-date-system: Excel day 25569 == 1970-01-01
    // 1904-date-system: Excel day 24107 == 1970-01-01
    const ms1900 = (num1900 - 25569) * 86400 * 1000;
    const ms1904 = (num - 24107) * 86400 * 1000;
    const d1900 = new Date(ms1900);
    const d1904 = new Date(ms1904);

    const ok = (d) => {
      const t = d.getTime();
      if (Number.isNaN(t)) return false;
      const y = d.getUTCFullYear();
      return y >= 1990 && y <= 2100;
    };

    if (ok(d1900)) return d1900;
    if (ok(d1904)) return d1904;
    if (!Number.isNaN(d1900.getTime())) return d1900;
    if (!Number.isNaN(d1904.getTime())) return d1904;
    return null;
  };

  // Some CSV exports provide Excel date serials as strings (e.g. "45485.92015")
  if (typeof v === 'string') {
    const vs = normalizeStr(v);
    if (/^\d+(?:\.\d+)?$/.test(vs)) {
      const num = Number(vs);
      if (Number.isFinite(num)) {
        const dt = excelSerialToDate(num);
        if (dt) return dt;
      }
    }
  }

  // XLSX may return Excel date serials as numbers
  if (typeof v === 'number' && Number.isFinite(v)) {
    const dt = excelSerialToDate(v);
    if (dt) return dt;
  }

  const s = normalizeStr(v);
  if (!s) return null;

  // Prefer ISO-like formats that Date can reliably parse
  const native = new Date(s);
  if (!Number.isNaN(native.getTime())) return native;

  // Handle "YYYY-MM-DD" and "YYYY-MM-DD HH:mm[:ss][ TZ]" (common CSV exports)
  // Examples:
  // - 2024-07-12
  // - 2024-07-12 10:04
  // - 2024-07-12 10:04:00
  // - 2024-07-12 10:04:00 +0800
  // - 2024-07-12 10:04:00 +08:00
  // - 2024-07-12 10:04:00Z
  const isoDateOnly = s.match(/^\s*(\d{4})-(\d{2})-(\d{2})\s*$/);
  if (isoDateOnly) {
    const y = Number(isoDateOnly[1]);
    const mo = Number(isoDateOnly[2]);
    const d = Number(isoDateOnly[3]);
    const dt = new Date(y, mo - 1, d);
    return Number.isNaN(dt.getTime()) ? null : dt;
  }

  const isoDateTime = s.match(
    /^\s*(\d{4})-(\d{2})-(\d{2})[ T](\d{1,2}):(\d{2})(?::(\d{2}))?\s*(Z|[+-]\d{2}:?\d{2})?\s*$/i
  );
  if (isoDateTime) {
    const y = Number(isoDateTime[1]);
    const mo = Number(isoDateTime[2]);
    const d = Number(isoDateTime[3]);
    const hh = Number(isoDateTime[4]);
    const mm = Number(isoDateTime[5]);
    const ss = Number(isoDateTime[6] || 0);
    const tzRaw = normalizeStr(isoDateTime[7] || '');

    if (!tzRaw) {
      const dt = new Date(y, mo - 1, d, hh, mm, ss);
      return Number.isNaN(dt.getTime()) ? null : dt;
    }

    let tz = tzRaw.toUpperCase();
    // convert +0800 -> +08:00
    if (/^[+-]\d{4}$/.test(tz)) tz = `${tz.slice(0, 3)}:${tz.slice(3)}`;
    const iso = `${String(y).padStart(4, '0')}-${String(mo).padStart(2, '0')}-${String(d).padStart(2, '0')}T${String(hh).padStart(2, '0')}:${String(mm).padStart(2, '0')}:${String(ss).padStart(2, '0')}${tz === 'Z' ? 'Z' : tz}`;
    const dt = new Date(iso);
    return Number.isNaN(dt.getTime()) ? null : dt;
  }

  // Handle common CSV exports like "7/12/2024 10:04" (ambiguous M/D vs D/M)
  // Heuristic:
  // - If first part > 12 => D/M
  // - Else if second part > 12 => M/D
  // - Else default to D/M (matches many non-US locales)
  const m = s.match(/^\s*(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(AM|PM)?)?\s*$/i);
  if (m) {
    let a = Number(m[1]);
    let b = Number(m[2]);
    const y = Number(m[3]);
    let hh = Number(m[4] || 0);
    const mm = Number(m[5] || 0);
    const ss = Number(m[6] || 0);
    const ap = normalizeStr(m[7]).toUpperCase();
    if (ap === 'PM' && hh < 12) hh += 12;
    if (ap === 'AM' && hh === 12) hh = 0;

    let day;
    let month;
    if (a > 12) {
      day = a;
      month = b;
    } else if (b > 12) {
      month = a;
      day = b;
    } else {
      day = a;
      month = b;
    }

    const dt = new Date(y, month - 1, day, hh, mm, ss);
    return Number.isNaN(dt.getTime()) ? null : dt;
  }

  return null;
};

const buildLineItemMap = async () => {
  const groups = await ItemGroup.find({ active: true }).select('name lineItem').lean();
  const byLineItemLower = new Map();
  for (const g of groups) {
    const li = normalizeStr(g.lineItem || '');
    if (!li) continue;
    byLineItemLower.set(li.toLowerCase(), g.name);
  }
  return byLineItemLower;
};

const buildGroupNameMap = async () => {
  const groups = await ItemGroup.find({ active: true }).select('name lineItem').lean();
  const byGroupLower = new Map();
  for (const g of groups) {
    const name = normalizeStr(g.name || '');
    if (!name) continue;
    byGroupLower.set(name.toLowerCase(), g.name);
  }
  return byGroupLower;
};

const resolveGroupName = ({ input, byGroupLower, byLineItemLower }) => {
  const s = normalizeStr(input);
  if (!s) return '';
  const key = s.toLowerCase();
  return byGroupLower.get(key) || byLineItemLower.get(key) || '';
};

const normalizeOrderStatus = (v) => {
  const s = normalizeStr(v).toLowerCase();
  if (!s) return '';
  if (s === 'created') return 'create';
  if (s === 'cancelled') return 'cancel';
  return s;
};

const ORDER_STATUSES = ['create', 'backorder', 'fulfilled', 'cancel'];

const getNextManualOrderNumber = async () => {
  // Format: ORD-0001 (monotonic increasing). Best-effort (not a transaction-safe counter).
  const last = await UnfulfilledOrder.findOne({ orderNumber: /^ORD-\d+$/ })
    .sort({ createdAt: -1 })
    .select('orderNumber createdAt')
    .lean();
  const prev = normalizeStr(last?.orderNumber);
  const m = prev.match(/^ORD-(\d+)$/);
  const n = m ? Number(m[1]) : 0;
  const next = Number.isFinite(n) ? n + 1 : 1;
  return `ORD-${String(next).padStart(4, '0')}`;
};

const validateStockAllOrNothing = async ({ warehouseId, lines }) => {
  const needs = new Map();
  for (const ln of lines) {
    const key = ln.groupName;
    needs.set(key, (needs.get(key) || 0) + Number(ln.qty || 0));
  }
  for (const [groupName, qty] of needs.entries()) {
    const stock = await PalletGroupStock.findOne({ warehouseId, groupName }).lean();
    const current = Number(stock?.pallets || 0);
    if (!stock) return { ok: false, message: `No stock record for ${groupName} in this warehouse` };
    if (current < qty) return { ok: false, message: `Insufficient pallets for ${groupName}: available ${current}, required ${qty}` };
  }
  return { ok: true };
};

const applyInventoryDeductionForOrder = async ({ warehouseId, orderNumber, lines, committedBy }) => {
  // Pre-validate stock across all lines
  const stockOk = await validateStockAllOrNothing({ warehouseId, lines });
  if (!stockOk.ok) throw new Error(stockOk.message);

  const applied = [];
  try {
    for (const ln of lines) {
      const stock = await PalletGroupStock.findOneAndUpdate(
        { warehouseId, groupName: ln.groupName, pallets: { $gte: ln.qty } },
        { $inc: { pallets: -ln.qty } },
        { new: true }
      );
      if (!stock) throw new Error(`Insufficient pallets for ${ln.groupName}`);

      const txn = await PalletGroupTxn.create({
        poNumber: normalizeStr(orderNumber),
        groupName: ln.groupName,
        warehouseId,
        palletsDelta: -Number(ln.qty || 0),
        status: 'Adjustment',
        reason: 'order_fulfilled',
        committedBy: committedBy || '',
      });
      applied.push({ groupName: ln.groupName, qty: ln.qty, txnId: txn?._id });
    }
    return { ok: true, deductedLines: applied.length };
  } catch (e) {
    // rollback best effort
    for (const a of applied) {
      try {
        await PalletGroupStock.updateOne({ warehouseId, groupName: a.groupName }, { $inc: { pallets: a.qty } });
      } catch {}
      try {
        if (a.txnId) await PalletGroupTxn.deleteOne({ _id: a.txnId });
      } catch {}
    }
    throw e;
  }
};

const applyFulfilledOrder = async ({ warehouseId, orderNumber, meta, lines, committedBy, source }) => {
  // idempotency
  const exists = await FulfilledOrderImport.findOne({ orderNumber }).lean();
  if (exists) throw new Error(`Duplicate order import: ${orderNumber}`);

  const deducted = await applyInventoryDeductionForOrder({ warehouseId, orderNumber, lines, committedBy });

  await FulfilledOrderImport.create({
    orderNumber,
    email: meta.email,
    fulfilledAt: meta.fulfilledAt || undefined,
    createdAtOrder: meta.createdAtOrder || undefined,
    billingName: meta.billingName,
    billingPhone: meta.billingPhone,
    shippingName: meta.shippingName,
    shippingStreet: meta.shippingStreet,
    shippingAddress1: meta.shippingAddress1,
    shippingPhone: meta.shippingPhone,
    warehouseId,
    lines: lines.map((l) => ({ lineItem: l.lineItem, groupName: l.groupName, qty: Number(l.qty || 0) })),
    source,
    status: 'fulfilled',
    committedBy: committedBy || '',
  });

  return deducted;
};

export const createUnfulfilledOrder = async (req, res) => {
  const { warehouseId, customerEmail, customerName, customerPhone, createdAtOrder, estFulfillmentDate, shippingAddress, lines = [], status } = req.body || {};
  if (!warehouseId) return res.status(400).json({ message: 'warehouseId required' });
  if (!Array.isArray(lines) || lines.length === 0) return res.status(400).json({ message: 'lines required' });
  if (!normalizeStr(customerPhone)) return res.status(400).json({ message: 'customerPhone required' });

  const wh = await Warehouse.findById(warehouseId).lean();
  if (!wh) return res.status(400).json({ message: 'warehouse not found' });

  const byLineItemLower = await buildLineItemMap();
  const byGroupLower = await buildGroupNameMap();

  const parsedLines = [];
  for (const ln of lines) {
    const search = normalizeStr(ln?.search || ln?.lineItem || ln?.groupName || '');
    const qty = Number(ln?.qty);
    if (!search) return res.status(400).json({ message: 'pallet id required' });
    if (!Number.isFinite(qty) || qty <= 0) return res.status(400).json({ message: 'qty must be > 0' });
    const groupName = resolveGroupName({ input: search, byGroupLower, byLineItemLower });
    if (!groupName) return res.status(400).json({ message: `Unknown Pallet Description / Pallet ID: ${search}` });
    // Find canonical lineItem for saving (best effort)
    const g = await ItemGroup.findOne({ name: groupName }).select('lineItem name').lean();
    parsedLines.push({
      groupName,
      lineItem: (g?.lineItem || search).trim(),
      qty: Math.floor(qty),
    });
  }

  const nextStatus = normalizeOrderStatus(status) || 'create';
  if (!ORDER_STATUSES.includes(nextStatus) || nextStatus === 'fulfilled' || nextStatus === 'cancel') {
    return res.status(400).json({ message: 'Invalid status' });
  }

  // create: must have enough stock, then deduct now.
  // backorder: deduct now but allow negative.
  if (nextStatus === 'create') {
    const ok = await validateStockAllOrNothing({ warehouseId, lines: parsedLines });
    if (!ok.ok) return res.status(400).json({ message: ok.message });
  }

  const orderNumber = await getNextManualOrderNumber();

  const committedBy = String(req.user?.username || req.user?.id || '');
  await applyInventoryDeltaForOrder({
    warehouseId,
    orderNumber,
    lines: parsedLines,
    committedBy,
    deltaSign: -1,
    allowNegative: nextStatus === 'backorder',
    reason: nextStatus === 'backorder' ? 'order_backorder' : 'order_create',
  });
  const doc = await UnfulfilledOrder.create({
    orderNumber,
    warehouseId,
    customerEmail: normalizeStr(customerEmail),
    customerName: normalizeStr(customerName),
    customerPhone: normalizeStr(customerPhone),
    createdAtOrder: createdAtOrder ? new Date(createdAtOrder) : new Date(),
    estFulfillmentDate: estFulfillmentDate ? new Date(estFulfillmentDate) : undefined,
    shippingAddress: normalizeStr(shippingAddress),
    lines: parsedLines,
    status: nextStatus,
    committedBy,
  });
  res.status(201).json(doc);
};

export const listUnfulfilledOrders = async (req, res) => {
  const limit = Math.min(Math.max(Number(req.query?.limit) || 200, 1), 1000);
  const docs = await UnfulfilledOrder.find({})
    .sort({ createdAt: -1 })
    .limit(limit)
    .lean();
  res.json(docs);
};

export const listFulfilledImports = async (req, res) => {
  const limit = Math.min(Math.max(Number(req.query?.limit) || 200, 1), 1000);
  const docs = await FulfilledOrderImport.find({})
    .sort({ createdAt: -1 })
    .limit(limit)
    .lean();
  res.json(docs);
};

export const getImportedOrderById = async (req, res) => {
  const { id } = req.params;
  const doc = await FulfilledOrderImport.findById(id).lean();
  if (!doc) return res.status(404).json({ message: 'Order not found' });
  res.json(doc);
};

export const updateImportedOrderDetails = async (req, res) => {
  const { id } = req.params;
  const existing = await FulfilledOrderImport.findById(id).lean();
  if (!existing) return res.status(404).json({ message: 'Order not found' });
  if (String(existing.status || '') === 'fulfilled') return res.status(400).json({ message: 'Fulfilled orders are locked' });

  const { email, billingName, billingPhone, shippingName, shippingStreet, fulfilledAt, createdAtOrder } = req.body || {};
  const set = {};
  if (email !== undefined) set.email = normalizeStr(email);
  if (billingName !== undefined) set.billingName = normalizeStr(billingName);
  if (billingPhone !== undefined) set.billingPhone = normalizeStr(billingPhone);
  if (shippingName !== undefined) set.shippingName = normalizeStr(shippingName);
  if (shippingStreet !== undefined) set.shippingStreet = normalizeStr(shippingStreet);
  if (fulfilledAt !== undefined) set.fulfilledAt = normalizeStr(fulfilledAt) ? new Date(fulfilledAt) : null;
  if (createdAtOrder !== undefined) set.createdAtOrder = normalizeStr(createdAtOrder) ? new Date(createdAtOrder) : null;

  const doc = await FulfilledOrderImport.findByIdAndUpdate(id, { $set: set }, { new: true }).lean();
  res.json(doc);
};

export const checkImportedOrderStock = async (req, res) => {
  const { id } = req.params;
  const doc = await FulfilledOrderImport.findById(id).select('warehouseId lines').lean();
  if (!doc) return res.status(404).json({ message: 'Order not found' });
  const warehouseId = String(doc.warehouseId || '');
  const lines = Array.isArray(doc.lines) ? doc.lines : [];
  const ok = await validateStockAllOrNothing({
    warehouseId,
    lines: lines.map((l) => ({ groupName: l.groupName, qty: l.qty })),
  });
  if (!ok.ok) return res.status(400).json({ message: ok.message });
  res.json({ ok: true });
};

export const updateImportedOrderStatus = async (req, res) => {
  const { id } = req.params;
  const { status } = req.body || {};
  const next = normalizeOrderStatus(status);
  if (!ORDER_STATUSES.includes(next)) {
    return res.status(400).json({ message: 'Invalid status' });
  }

  const doc = await FulfilledOrderImport.findById(id);
  if (!doc) return res.status(404).json({ message: 'Order not found' });

  const prev = normalizeOrderStatus(doc.status || 'create') || 'create';
  if (prev === 'fulfilled') return res.status(400).json({ message: 'Fulfilled orders are locked' });

  const committedBy = String(req.user?.username || req.user?.id || '');
  const orderLines = (doc.lines || []).map((l) => ({ groupName: l.groupName, qty: l.qty }));

  if (next === 'cancel' && prev !== 'cancel') {
    if (prev === 'create' || prev === 'backorder') {
      await applyInventoryDeltaForOrder({ warehouseId: doc.warehouseId, orderNumber: doc.orderNumber, lines: orderLines, committedBy, deltaSign: +1, allowNegative: true, reason: 'order_cancel' });
    }
    doc.status = 'cancel';
    await doc.save();
    return res.json(doc.toObject());
  }

  if ((next === 'create' || next === 'backorder') && prev === 'cancel') {
    if (next === 'create') {
      const ok = await validateStockAllOrNothing({ warehouseId: doc.warehouseId, lines: orderLines });
      if (!ok.ok) return res.status(400).json({ message: ok.message });
    }
    await applyInventoryDeltaForOrder({ warehouseId: doc.warehouseId, orderNumber: doc.orderNumber, lines: orderLines, committedBy, deltaSign: -1, allowNegative: next === 'backorder', reason: next === 'backorder' ? 'order_backorder' : 'order_create' });
  }

  // fulfilled does not change inventory (inventory already affected on create/backorder)
  doc.status = next;
  await doc.save();
  res.json(doc.toObject());
};

export const getUnfulfilledOrderById = async (req, res) => {
  const { id } = req.params;
  const doc = await UnfulfilledOrder.findById(id).lean();
  if (!doc) return res.status(404).json({ message: 'Order not found' });
  res.json(doc);
};

export const checkUnfulfilledOrderStock = async (req, res) => {
  const { id } = req.params;
  const doc = await UnfulfilledOrder.findById(id).select('warehouseId lines').lean();
  if (!doc) return res.status(404).json({ message: 'Order not found' });
  const warehouseId = String(doc.warehouseId || '');
  const lines = Array.isArray(doc.lines) ? doc.lines : [];
  const ok = await validateStockAllOrNothing({
    warehouseId,
    lines: lines.map((l) => ({ groupName: l.groupName, qty: l.qty })),
  });
  if (!ok.ok) return res.status(400).json({ message: ok.message });
  res.json({ ok: true });
};

export const updateUnfulfilledOrderStatus = async (req, res) => {
  const { id } = req.params;
  const { status } = req.body || {};
  const next = normalizeOrderStatus(status);
  if (!ORDER_STATUSES.includes(next)) {
    return res.status(400).json({ message: 'Invalid status' });
  }

  const doc = await UnfulfilledOrder.findById(id);
  if (!doc) return res.status(404).json({ message: 'Order not found' });

  const prev = normalizeOrderStatus(doc.status || 'create') || 'create';
  if (prev === 'fulfilled') return res.status(400).json({ message: 'Fulfilled orders are locked' });

  const committedBy = String(req.user?.username || req.user?.id || '');
  const orderLines = (doc.lines || []).map((l) => ({ groupName: l.groupName, qty: l.qty }));

  if (next === 'cancel' && prev !== 'cancel') {
    if (prev === 'create' || prev === 'backorder') {
      await applyInventoryDeltaForOrder({ warehouseId: doc.warehouseId, orderNumber: doc.orderNumber, lines: orderLines, committedBy, deltaSign: +1, allowNegative: true, reason: 'order_cancel' });
    }
    doc.status = 'cancel';
    await doc.save();
    return res.json(doc.toObject());
  }

  if ((next === 'create' || next === 'backorder') && prev === 'cancel') {
    if (next === 'create') {
      const ok = await validateStockAllOrNothing({ warehouseId: doc.warehouseId, lines: orderLines });
      if (!ok.ok) return res.status(400).json({ message: ok.message });
    }
    await applyInventoryDeltaForOrder({ warehouseId: doc.warehouseId, orderNumber: doc.orderNumber, lines: orderLines, committedBy, deltaSign: -1, allowNegative: next === 'backorder', reason: next === 'backorder' ? 'order_backorder' : 'order_create' });
  }

  // fulfilled does not change inventory (inventory already affected on create/backorder)
  doc.status = next;
  await doc.save();
  res.json(doc.toObject());
};

export const updateUnfulfilledOrderDetails = async (req, res) => {
  const { id } = req.params;
  const { customerName, customerEmail, customerPhone, estFulfillmentDate, shippingAddress } = req.body || {};

  const existing = await UnfulfilledOrder.findById(id).select('status').lean();
  if (!existing) return res.status(404).json({ message: 'Order not found' });
  if (String(existing.status || '') === 'fulfilled') return res.status(400).json({ message: 'Fulfilled orders are locked' });

  const set = {};
  if (customerName !== undefined) set.customerName = normalizeStr(customerName);
  if (customerEmail !== undefined) set.customerEmail = normalizeStr(customerEmail);
  if (customerPhone !== undefined) set.customerPhone = normalizeStr(customerPhone);
  if (shippingAddress !== undefined) set.shippingAddress = normalizeStr(shippingAddress);
  if (estFulfillmentDate !== undefined) {
    const s = normalizeStr(estFulfillmentDate);
    set.estFulfillmentDate = s ? new Date(s) : null;
  }

  const doc = await UnfulfilledOrder.findByIdAndUpdate(
    id,
    { $set: set },
    { new: true }
  ).lean();
  if (!doc) return res.status(404).json({ message: 'Order not found' });
  res.json(doc);
};

export const createOrder = async (req, res) => {
  const { customerName, customerAddress, orderLines = [], estDeliveryDate, totalPalletUsed } = req.body || {};
  if (!customerName) return res.status(400).json({ message: 'customerName required' });
  if (!Array.isArray(orderLines) || orderLines.length === 0) return res.status(400).json({ message: 'orderLines required' });

  // Auto-generate order number: ORD-YYYYMMDD-XXXX (incremental suffix by count that day)
  const now = new Date();
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, '0');
  const d = String(now.getDate()).padStart(2, '0');
  const dateKey = `${y}${m}${d}`;
  const dailyCount = await Order.countDocuments({ createdAt: { $gte: new Date(`${y}-${m}-${d}T00:00:00.000Z`), $lte: new Date(`${y}-${m}-${d}T23:59:59.999Z`) } });
  const orderNumber = `ORD-${dateKey}-${String(dailyCount + 1).padStart(4, '0')}`;

  const doc = await Order.create({ orderNumber, customerName, customerAddress, orderLines, estDeliveryDate, totalPalletUsed });
  res.status(201).json(doc);
};

export const assignPallets = async (req, res) => {
  const { id } = req.params;
  const { assignments = [] } = req.body || {};
  // assignments: [{ lineIndex, palletIds: [] }]
  const order = await Order.findById(id);
  if (!order) return res.status(404).json({ message: 'Order not found' });

  for (const a of assignments) {
    const line = order.orderLines[a.lineIndex];
    if (!line) continue;
    const palletIds = Array.isArray(a.palletIds) ? a.palletIds : [];
    line.assignedPallets = Array.from(new Set([...(line.assignedPallets||[]), ...palletIds]));
    // mark pallets reserved
    await Pallet.updateMany({ palletId: { $in: palletIds } }, { $set: { status: 'reserved' } });
  }
  order.status = 'confirmed';
  await order.save();
  res.json(order);
};

export const autoAllocate = async (req, res) => {
  const { id } = req.params;
  const order = await Order.findById(id);
  if (!order) return res.status(404).json({ message: 'Order not found' });
  const { assignments, shortages } = await autoAllocateOrder(order);
  // apply assignments to order record
  for (const a of assignments) {
    const line = order.orderLines[a.lineIndex];
    if (!line) continue;
    line.assignedPallets = Array.from(new Set([...(line.assignedPallets||[]), ...a.palletIds]));
  }
  order.status = shortages.length ? 'confirmed' : 'picked';
  await order.save();
  res.json({ order, assignments, shortages });
};

export const previewFulfilledCsv = async (req, res) => {
  const { warehouseId } = req.query;
  if (!req.file) return res.status(400).json({ message: 'file required' });
  if (!warehouseId) return res.status(400).json({ message: 'warehouseId required' });
  const wh = await Warehouse.findById(warehouseId).lean();
  if (!wh) return res.status(400).json({ message: 'warehouse not found' });

  const wb = XLSX.read(req.file.buffer, { type: 'buffer' });
  const ws = wb.Sheets[wb.SheetNames[0]];
  const rows = XLSX.utils.sheet_to_json(ws, { defval: '' });
  const totalRows = rows.length;
  const errors = [];

  const byLineItemLower = await buildLineItemMap();

  // Group by order number
  const orders = new Map();
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    const rowNum = i + 2;
    const orderNumber = normalizeStr(r['Name']);
    const email = normalizeStr(r['Email']);
    const lineItem = normalizeStr(r['Lineitem name']);
    const qty = Number(r['Lineitem quantity']);
    if (!orderNumber) { errors.push({ rowNum, errors: ['Order # (Name) required'] }); continue; }
    if (!lineItem) { errors.push({ rowNum, errors: ['Lineitem name required'] }); continue; }
    if (!Number.isFinite(qty) || qty <= 0) { errors.push({ rowNum, errors: ['Lineitem quantity must be > 0'] }); continue; }
    const groupName = byLineItemLower.get(lineItem.toLowerCase());
    if (!groupName) { errors.push({ rowNum, errors: [`Unknown Pallet Description / Pallet ID: ${lineItem}`] }); continue; }

    if (!orders.has(orderNumber)) {
      const fulfilledAtInfo = getColByPrefixInfo(r, 'Fulfilled at');
      const createdAtInfo = getColByPrefixInfo(r, 'Created at');
      orders.set(orderNumber, {
        orderNumber,
        meta: {
          email,
          fulfilledAt: toDateOrNull(fulfilledAtInfo.value),
          createdAtOrder: toDateOrNull(createdAtInfo.value),
          _dateDebug: {
            fulfilledAtKey: fulfilledAtInfo.key,
            fulfilledAtRaw: fulfilledAtInfo.value,
            createdAtKey: createdAtInfo.key,
            createdAtRaw: createdAtInfo.value,
          },
          billingName: normalizeStr(getCol(r, 'Billing Name')),
          billingPhone: normalizeStr(getCol(r, 'Billing Phone')),
          shippingName: normalizeStr(getCol(r, 'Shipping Name')),
          shippingStreet: normalizeStr(getCol(r, 'Shipping Street')),
          shippingAddress1: normalizeStr(getCol(r, 'Shipping Address1')),
          shippingPhone: normalizeStr(getCol(r, 'Shipping Phone')),
        },
        lines: [],
      });
    }
    orders.get(orderNumber).lines.push({ lineItem, groupName, qty: Math.floor(qty) });
  }

  // Validate stock availability for this import preview (across all orders/lines in the file)
  if (orders.size > 0 && errors.length === 0) {
    const allLines = [];
    for (const o of orders.values()) {
      for (const ln of (o.lines || [])) {
        allLines.push({ groupName: ln.groupName, qty: Number(ln.qty || 0) });
      }
    }
    const ok = await validateStockAllOrNothing({ warehouseId, lines: allLines });
    if (!ok.ok) errors.push({ rowNum: '-', errors: [ok.message] });
  }

  // Idempotency check
  for (const [orderNumber] of orders.entries()) {
    const exists = await FulfilledOrderImport.findOne({ orderNumber }).lean();
    if (exists) errors.push({ rowNum: '-', errors: [`Duplicate order import: ${orderNumber}`] });
  }

  const parsedOrders = Array.from(orders.values()).map((o) => ({
    orderNumber: o.orderNumber,
    lineCount: o.lines.length,
    totalQty: o.lines.reduce((s, l) => s + (Number(l.qty) || 0), 0),
    createdAtOrder: o.meta?.createdAtOrder ? o.meta.createdAtOrder.toISOString() : null,
    fulfilledAt: o.meta?.fulfilledAt ? o.meta.fulfilledAt.toISOString() : null,
    dateDebug: {
      createdAtKey: o.meta?._dateDebug?.createdAtKey || '',
      createdAtRaw: o.meta?._dateDebug?.createdAtRaw ?? '',
      fulfilledAtKey: o.meta?._dateDebug?.fulfilledAtKey || '',
      fulfilledAtRaw: o.meta?._dateDebug?.fulfilledAtRaw ?? '',
    },
  }));

  res.json({ totalRows, orderCount: orders.size, errorCount: errors.length, errors, orders: parsedOrders });
};

export const commitFulfilledCsv = async (req, res) => {
  const { warehouseId } = req.query;
  if (!req.file) return res.status(400).json({ message: 'file required' });
  if (!warehouseId) return res.status(400).json({ message: 'warehouseId required' });
  const wh = await Warehouse.findById(warehouseId).lean();
  if (!wh) return res.status(400).json({ message: 'warehouse not found' });

  const wb = XLSX.read(req.file.buffer, { type: 'buffer' });
  const ws = wb.Sheets[wb.SheetNames[0]];
  const rows = XLSX.utils.sheet_to_json(ws, { defval: '' });
  const errors = [];

  const byLineItemLower = await buildLineItemMap();
  const orders = new Map();

  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    const rowNum = i + 2;
    const orderNumber = normalizeStr(r['Name']);
    const email = normalizeStr(r['Email']);
    const lineItem = normalizeStr(r['Lineitem name']);
    const qty = Number(r['Lineitem quantity']);
    if (!orderNumber) { errors.push({ rowNum, errors: ['Order # (Name) required'] }); continue; }
    if (!lineItem) { errors.push({ rowNum, errors: ['Lineitem name required'] }); continue; }
    if (!Number.isFinite(qty) || qty <= 0) { errors.push({ rowNum, errors: ['Lineitem quantity must be > 0'] }); continue; }
    const groupName = byLineItemLower.get(lineItem.toLowerCase());
    if (!groupName) { errors.push({ rowNum, errors: [`Unknown Pallet Description / Pallet ID: ${lineItem}`] }); continue; }

    if (!orders.has(orderNumber)) {
      const fulfilledAtInfo = getColByPrefixInfo(r, 'Fulfilled at');
      const createdAtInfo = getColByPrefixInfo(r, 'Created at');
      orders.set(orderNumber, {
        orderNumber,
        meta: {
          email,
          fulfilledAt: toDateOrNull(fulfilledAtInfo.value),
          createdAtOrder: toDateOrNull(createdAtInfo.value),
          billingName: normalizeStr(getCol(r, 'Billing Name')),
          billingPhone: normalizeStr(getCol(r, 'Billing Phone')),
          shippingName: normalizeStr(getCol(r, 'Shipping Name')),
          shippingStreet: normalizeStr(getCol(r, 'Shipping Street')),
          shippingAddress1: normalizeStr(getCol(r, 'Shipping Address1')),
          shippingPhone: normalizeStr(getCol(r, 'Shipping Phone')),
        },
        lines: [],
      });
    }
    orders.get(orderNumber).lines.push({ lineItem, groupName, qty: Math.floor(qty) });
  }

  // reject if parse errors
  if (errors.length) return res.status(400).json({ errorCount: errors.length, errors });

  // idempotency upfront (allow backfill of missing dates)
  for (const o of orders.values()) {
    const exists = await FulfilledOrderImport.findOne({ orderNumber: o.orderNumber }).lean();
    if (exists) {
      const canBackfillCreated = !exists.createdAtOrder && !!o?.meta?.createdAtOrder;
      const canBackfillFulfilled = !exists.fulfilledAt && !!o?.meta?.fulfilledAt;
      const canBackfill = canBackfillCreated || canBackfillFulfilled;
      if (!canBackfill) errors.push({ rowNum: '-', errors: [`Duplicate order import: ${o.orderNumber}`] });
    }
  }
  if (errors.length) return res.status(400).json({ errorCount: errors.length, errors });

  const committedBy = String(req.user?.username || req.user?.id || '');
  let committed = 0;
  try {
    for (const o of orders.values()) {
      const existing = await FulfilledOrderImport.findOne({ orderNumber: o.orderNumber }).select('_id createdAtOrder fulfilledAt').lean();
      const canBackfillCreated = existing && !existing.createdAtOrder && !!o?.meta?.createdAtOrder;
      const canBackfillFulfilled = existing && !existing.fulfilledAt && !!o?.meta?.fulfilledAt;
      if (existing && (canBackfillCreated || canBackfillFulfilled)) {
        await FulfilledOrderImport.updateOne(
          { _id: existing._id },
          {
            $set: {
              ...(canBackfillFulfilled ? { fulfilledAt: o.meta.fulfilledAt || undefined } : {}),
              ...(canBackfillCreated ? { createdAtOrder: o.meta.createdAtOrder || undefined } : {}),
            },
          }
        );
      } else {
        await FulfilledOrderImport.create({
          orderNumber: o.orderNumber,
          email: o.meta.email,
          fulfilledAt: o.meta.fulfilledAt || undefined,
          createdAtOrder: o.meta.createdAtOrder || undefined,
          billingName: o.meta.billingName,
          billingPhone: o.meta.billingPhone,
          shippingName: o.meta.shippingName,
          shippingStreet: o.meta.shippingStreet,
          shippingAddress1: o.meta.shippingAddress1,
          shippingPhone: o.meta.shippingPhone,
          warehouseId,
          lines: (o.lines || []).map((l) => ({ lineItem: l.lineItem, groupName: l.groupName, qty: Number(l.qty || 0) })),
          source: 'csv',
          status: 'created',
          committedBy: committedBy || '',
        });
      }
      committed += 1;
    }
    res.json({ ok: true, committedOrders: committed });
  } catch (e) {
    return res.status(400).json({ message: e?.message || 'Import failed' });
  }
};

export const createFulfilledManual = async (req, res) => {
  const { warehouseId, orderNumber, email, fulfilledAt, createdAt, lines = [], billingName, billingPhone, shippingName, shippingStreet, shippingAddress1, shippingPhone } = req.body || {};
  if (!warehouseId) return res.status(400).json({ message: 'warehouseId required' });
  if (!orderNumber) return res.status(400).json({ message: 'orderNumber required' });
  if (!Array.isArray(lines) || lines.length === 0) return res.status(400).json({ message: 'lines required' });

  const wh = await Warehouse.findById(warehouseId).lean();
  if (!wh) return res.status(400).json({ message: 'warehouse not found' });

  const byLineItemLower = await buildLineItemMap();
  const parsedLines = [];
  for (const ln of lines) {
    const lineItem = normalizeStr(ln?.lineItem);
    const qty = Number(ln?.qty);
    if (!lineItem) return res.status(400).json({ message: 'lineItem required' });
    if (!Number.isFinite(qty) || qty <= 0) return res.status(400).json({ message: 'qty must be > 0' });
    const groupName = byLineItemLower.get(lineItem.toLowerCase());
    if (!groupName) return res.status(400).json({ message: `Unknown Pallet ID: ${lineItem}` });
    parsedLines.push({ lineItem, groupName, qty: Math.floor(qty) });
  }

  const committedBy = String(req.user?.username || req.user?.id || '');
  try {
    const meta = {
      email: normalizeStr(email),
      fulfilledAt: fulfilledAt ? new Date(fulfilledAt) : null,
      createdAtOrder: createdAt ? new Date(createdAt) : null,
      billingName: normalizeStr(billingName),
      billingPhone: normalizeStr(billingPhone),
      shippingName: normalizeStr(shippingName),
      shippingStreet: normalizeStr(shippingStreet),
      shippingAddress1: normalizeStr(shippingAddress1),
      shippingPhone: normalizeStr(shippingPhone),
    };
    const result = await applyFulfilledOrder({ warehouseId, orderNumber: normalizeStr(orderNumber), meta, lines: parsedLines, committedBy, source: 'manual' });
    res.json(result);
  } catch (e) {
    res.status(400).json({ message: e?.message || 'Create failed' });
  }
};
