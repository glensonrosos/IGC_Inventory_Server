import Order from '../models/Order.js';
import Pallet from '../models/Pallet.js';
import { autoAllocateOrder } from '../services/allocation.js';
import * as XLSX from 'xlsx';
import Warehouse from '../models/Warehouse.js';
import ItemGroup from '../models/ItemGroup.js';
import PalletGroupStock from '../models/PalletGroupStock.js';
import PalletGroupTxn from '../models/PalletGroupTxn.js';
import PalletGroupReservation from '../models/PalletGroupReservation.js';
import Shipment from '../models/Shipment.js';
import OnProcessPallet from '../models/OnProcessPallet.js';
import OnProcessBatch from '../models/OnProcessBatch.js';
import FulfilledOrderImport from '../models/FulfilledOrderImport.js';
import UnfulfilledOrder from '../models/UnfulfilledOrder.js';

const normalizeStr = (v) => (v == null ? '' : String(v)).trim();

const fmtDateYmd = (d) => {
  if (!d) return '';
  const dt = d instanceof Date ? d : new Date(d);
  if (Number.isNaN(dt.getTime())) return '';
  // Use local date parts to avoid off-by-one day issues from UTC conversion
  const y = dt.getFullYear();
  const m = String(dt.getMonth() + 1).padStart(2, '0');
  const day = String(dt.getDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
};

const NO_STOCKS_MESSAGE = 'NO Stocks available, please Produce Product from the On-Process Page';

const noStocksPayload = ({ items }) => {
  const list = Array.isArray(items) ? items : [];
  return {
    code: 'NO_STOCKS',
    message: NO_STOCKS_MESSAGE,
    noStocks: list.map((it) => ({
      lineItem: String(it?.lineItem || '').trim(),
      groupName: String(it?.groupName || '').trim(),
      required: Math.max(0, Math.floor(Number(it?.required || 0))),
      available: Math.max(0, Math.floor(Number(it?.available || 0))),
    })),
  };
};

const getSecondWarehouseFor = async (warehouseId) => {
  const wid = String(warehouseId || '').trim();
  if (!wid) return null;

  const current = await Warehouse.findById(wid).select('name isPrimary').lean();
  if (!current) return null;

  // Preferred pairing:
  // - If current is primary => choose a non-primary warehouse (e.g., PEBA)
  // - If current is non-primary => choose the primary warehouse
  let found = null;
  if (current.isPrimary) {
    found = await Warehouse.findOne({ isPrimary: { $ne: true } }).select('name').sort({ name: 1 }).lean();
  } else {
    found = await Warehouse.findOne({ isPrimary: true }).select('name').lean();
  }

  // Fallback (legacy): pick any other warehouse
  if (!found) {
    const list = await Warehouse.find({}).select('name').sort({ name: 1 }).lean();
    found = (list || []).find((w) => String(w?._id || '').trim() && String(w?._id || '').trim() !== wid) || null;
  }

  return found ? { _id: String(found._id), name: String(found.name || '').trim() } : null;
};

const getReservationMaps = async ({ warehouseId }) => {
  const rows = await PalletGroupReservation.aggregate([
    { $addFields: { warehouseIdStr: { $toString: '$warehouseId' }, sourceWarehouseIdStr: { $toString: '$sourceWarehouseId' } } },
    { $match: { warehouseIdStr: String(warehouseId || '') } },
    {
      $group: {
        _id: { groupName: '$groupName', source: '$source', sourceWarehouseIdStr: '$sourceWarehouseIdStr' },
        qty: { $sum: '$qty' },
      },
    },
  ]);

  const onWater = new Map();
  const onProcess = new Map();
  const physicalByWarehouse = new Map();

  for (const r of rows || []) {
    const groupName = String(r?._id?.groupName || '').trim();
    const source = String(r?._id?.source || '').trim();
    const wid = String(r?._id?.sourceWarehouseIdStr || '').trim();
    const qty = Number(r?.qty || 0);
    if (!groupName || !Number.isFinite(qty) || qty <= 0) continue;

    if (source === 'on_water') {
      onWater.set(groupName, (onWater.get(groupName) || 0) + qty);
      continue;
    }
    if (source === 'on_process') {
      onProcess.set(groupName, (onProcess.get(groupName) || 0) + qty);
      continue;
    }

    if ((source === 'primary' || source === 'second') && wid) {
      if (!physicalByWarehouse.has(wid)) physicalByWarehouse.set(wid, new Map());
      const m = physicalByWarehouse.get(wid);
      m.set(groupName, (m.get(groupName) || 0) + qty);
    }
  }

  return { onWater, onProcess, physicalByWarehouse };
};

const buildOnWaterMapForWarehouse = async ({ warehouseId, resolveToGroupName }) => {
  const secondWarehouse = await getSecondWarehouseFor(warehouseId);
  const whIds = [String(warehouseId || '').trim(), String(secondWarehouse?._id || '').trim()].filter((v) => v);

  const ships = await Shipment.find({
    status: 'on_water',
    warehouseId: { $in: whIds },
    notes: { $regex: 'pallet-group:', $options: 'i' },
  })
    .select('notes')
    .lean();

  const map = new Map();
  const re = /pallet-group:([^;|]+);\s*pallets:(\d+)/gi;
  for (const s of ships || []) {
    const text = String(s?.notes || '');
    re.lastIndex = 0;
    let m;
    while ((m = re.exec(text)) !== null) {
      const groupName = resolveToGroupName(String(m[1] || '').trim());
      const pallets = Number(m[2] || 0);
      if (!groupName || !Number.isFinite(pallets) || pallets <= 0) continue;
      map.set(groupName, (map.get(groupName) || 0) + pallets);
    }
  }
  return map;
};

const buildOnProcessMap = async ({ resolveToGroupName }) => {
  const opAgg = await OnProcessPallet.aggregate([
    { $match: { status: { $ne: 'cancelled' } } },
    { $addFields: { remaining: { $subtract: ['$totalPallet', { $ifNull: ['$transferredPallet', 0] }] } } },
    { $match: { remaining: { $gt: 0 } } },
    { $group: { _id: '$groupName', pallets: { $sum: '$remaining' } } },
  ]);
  const map = new Map();
  for (const r of opAgg || []) {
    const groupName = resolveToGroupName(String(r?._id || '').trim());
    const pallets = Number(r?.pallets || 0);
    if (!groupName || !Number.isFinite(pallets) || pallets <= 0) continue;
    map.set(groupName, pallets);
  }
  return map;
};

const rebalanceProcessingOrderAllocations = async ({ order, resolveToGroupName }) => {
  const status = normalizeOrderStatus(order?.status || 'processing') || 'processing';
  if ((status !== 'processing' && status !== 'ready_to_ship') || !order?._id || !order?.warehouseId || !order?.orderNumber) return false;

  const primaryWarehouseId = String(order.warehouseId);
  const secondWarehouse = await getSecondWarehouseFor(primaryWarehouseId);
  if (!secondWarehouse?._id) return false;

  const wh2 = String(secondWarehouse._id);
  const orderNumber = String(order.orderNumber || '').trim();

  const reservation = await getReservationMaps({ warehouseId: primaryWarehouseId });
  const onWaterMap = await buildOnWaterMapForWarehouse({ warehouseId: primaryWarehouseId, resolveToGroupName });

  const reservedOnWaterTotals = new Map(reservation.onWater);
  const reservedPhysicalPrimary = new Map(reservation.physicalByWarehouse?.get(primaryWarehouseId) || []);
  const reservedPhysicalSecond = new Map(reservation.physicalByWarehouse?.get(wh2) || []);

  const resDocs = await PalletGroupReservation.find({
    orderNumber,
    warehouseId: primaryWarehouseId,
    qty: { $gt: 0 },
  })
    .select('groupName source qty sourceWarehouseId')
    .lean();

  const orderOnWater = new Map();
  const orderOnProcess = new Map();
  const orderSecond = new Map();
  for (const d of resDocs || []) {
    const groupName = String(d?.groupName || '').trim();
    const source = String(d?.source || '').trim();
    const qty = Math.floor(Number(d?.qty || 0));
    const srcWid = String(d?.sourceWarehouseId || '').trim();
    if (!groupName || !Number.isFinite(qty) || qty <= 0) continue;
    if (source === 'on_water') orderOnWater.set(groupName, (orderOnWater.get(groupName) || 0) + qty);
    else if (source === 'on_process') orderOnProcess.set(groupName, (orderOnProcess.get(groupName) || 0) + qty);
    else if (source === 'second' && srcWid === wh2) orderSecond.set(groupName, (orderSecond.get(groupName) || 0) + qty);
  }

  const nextAllocs = Array.isArray(order.allocations) ? order.allocations.map((a) => ({ ...a })) : [];
  const addAlloc = (groupName, qty, source, warehouseId) => {
    const g = String(groupName || '').trim();
    const q = Math.floor(Number(qty || 0));
    if (!g || !Number.isFinite(q) || q <= 0) return;
    const found = nextAllocs.find((a) => String(a?.groupName || '').trim() === g && String(a?.source || '').trim() === source && (warehouseId ? String(a?.warehouseId || '').trim() === String(warehouseId) : !a?.warehouseId));
    if (found) found.qty = Math.floor(Number(found.qty || 0) + q);
    else {
      const row = { groupName: g, qty: q, source };
      if (warehouseId) row.warehouseId = String(warehouseId);
      nextAllocs.push(row);
    }
  };
  const decAlloc = (groupName, qty, source, warehouseId) => {
    const g = String(groupName || '').trim();
    let remaining = Math.floor(Number(qty || 0));
    if (!g || !Number.isFinite(remaining) || remaining <= 0) return;
    for (const a of nextAllocs) {
      if (remaining <= 0) break;
      if (String(a?.groupName || '').trim() !== g) continue;
      if (String(a?.source || '').trim() !== source) continue;
      if (warehouseId) {
        if (String(a?.warehouseId || '').trim() !== String(warehouseId)) continue;
      } else if (a?.warehouseId) {
        continue;
      }
      const have = Math.floor(Number(a?.qty || 0));
      if (!Number.isFinite(have) || have <= 0) continue;
      const take = Math.min(have, remaining);
      a.qty = have - take;
      remaining -= take;
    }
  };

  const decReservation = async ({ groupName, source, qty, sourceWarehouseId }) => {
    const q = Math.floor(Number(qty || 0));
    if (!q || q <= 0) return;
    const query = { orderNumber, warehouseId: primaryWarehouseId, groupName, source };
    if (sourceWarehouseId) query.sourceWarehouseId = String(sourceWarehouseId);
    await PalletGroupReservation.findOneAndUpdate(query, { $inc: { qty: -q } });
    await PalletGroupReservation.deleteMany({ ...query, qty: { $lte: 0 } });
  };
  const incReservation = async ({ groupName, source, qty, sourceWarehouseId }) => {
    const q = Math.floor(Number(qty || 0));
    if (!q || q <= 0) return;
    const query = { orderNumber, warehouseId: primaryWarehouseId, groupName, source };
    if (sourceWarehouseId) query.sourceWarehouseId = String(sourceWarehouseId);
    await PalletGroupReservation.findOneAndUpdate(
      query,
      { $inc: { qty: q }, $setOnInsert: { committedBy: '' } },
      { upsert: true, new: true, setDefaultsOnInsert: true }
    );
  };

  let moved = false;
  const groups = new Set([...Array.from(orderOnWater.keys()), ...Array.from(orderSecond.keys()), ...Array.from(orderOnProcess.keys())]);
  for (const groupName of groups) {
    const g = String(groupName || '').trim();
    if (!g) continue;

    // 1) on-water -> primary
    const ow = Math.max(0, Number(orderOnWater.get(g) || 0));
    if (ow > 0) {
      const stockDoc = await PalletGroupStock.findOne({ warehouseId: primaryWarehouseId, groupName: g }).select('pallets').lean();
      const total = Math.max(0, Number(stockDoc?.pallets || 0));
      const reserved = Math.max(0, Number(reservedPhysicalPrimary.get(g) || 0));
      const avail = Math.max(0, total - reserved);
      const take = Math.min(ow, avail);
      if (take > 0) {
        await decReservation({ groupName: g, source: 'on_water', qty: take });
        await incReservation({ groupName: g, source: 'primary', qty: take, sourceWarehouseId: primaryWarehouseId });
        decAlloc(g, take, 'on_water');
        addAlloc(g, take, 'primary', primaryWarehouseId);

        orderOnWater.set(g, ow - take);
        reservedOnWaterTotals.set(g, Math.max(0, Number(reservedOnWaterTotals.get(g) || 0) - take));
        reservedPhysicalPrimary.set(g, reserved + take);
        moved = true;
      }
    }

    // 2) second -> on-water
    const sec = Math.max(0, Number(orderSecond.get(g) || 0));
    if (sec > 0) {
      const onWaterTotal = Math.max(0, Number(onWaterMap.get(g) || 0));
      const onWaterReserved = Math.max(0, Number(reservedOnWaterTotals.get(g) || 0));
      const onWaterAvail = Math.max(0, onWaterTotal - onWaterReserved);
      const take = Math.min(sec, onWaterAvail);
      if (take > 0) {
        await decReservation({ groupName: g, source: 'second', qty: take, sourceWarehouseId: wh2 });
        await incReservation({ groupName: g, source: 'on_water', qty: take });
        decAlloc(g, take, 'second', wh2);
        addAlloc(g, take, 'on_water');

        orderSecond.set(g, sec - take);
        reservedPhysicalSecond.set(g, Math.max(0, Number(reservedPhysicalSecond.get(g) || 0) - take));
        reservedOnWaterTotals.set(g, onWaterReserved + take);
        moved = true;
      }
    }

    // 3) on-process -> second
    const op = Math.max(0, Number(orderOnProcess.get(g) || 0));
    if (op > 0) {
      const stockDoc = await PalletGroupStock.findOne({ warehouseId: wh2, groupName: g }).select('pallets').lean();
      const total = Math.max(0, Number(stockDoc?.pallets || 0));
      const reserved = Math.max(0, Number(reservedPhysicalSecond.get(g) || 0));
      const avail = Math.max(0, total - reserved);
      const take = Math.min(op, avail);
      if (take > 0) {
        await decReservation({ groupName: g, source: 'on_process', qty: take });
        await incReservation({ groupName: g, source: 'second', qty: take, sourceWarehouseId: wh2 });
        decAlloc(g, take, 'on_process');
        addAlloc(g, take, 'second', wh2);

        orderOnProcess.set(g, op - take);
        reservedPhysicalSecond.set(g, reserved + take);
        moved = true;
      }
    }
  }

  const isFullyPrimaryNow = () => {
    for (const v of Array.from(orderOnWater.values())) if (Number(v || 0) > 0) return false;
    for (const v of Array.from(orderOnProcess.values())) if (Number(v || 0) > 0) return false;
    for (const v of Array.from(orderSecond.values())) if (Number(v || 0) > 0) return false;
    return true;
  };

  // If the order is already fully primary, update status to READY TO SHIP (even if no movement happened)
  if (!moved && isFullyPrimaryNow()) {
    let primaryDoneAt = null;
    try {
      const latest = await PalletGroupReservation.findOne({ orderNumber, warehouseId: primaryWarehouseId, source: 'primary', qty: { $gt: 0 } })
        .sort({ updatedAt: -1 })
        .select('updatedAt')
        .lean();
      if (latest?.updatedAt) {
        const dt = new Date(latest.updatedAt);
        if (!Number.isNaN(dt.getTime())) primaryDoneAt = dt;
      }
    } catch {
      primaryDoneAt = null;
    }

    await UnfulfilledOrder.updateOne(
      { _id: order._id },
      {
        $set: {
          status: 'ready_to_ship',
          ...(primaryDoneAt ? { estFulfillmentDate: primaryDoneAt } : {}),
        },
      }
    );
    return true;
  }

  if (!moved) return false;

  const cleanedAllocs = nextAllocs
    .map((a) => ({ ...a, qty: Math.floor(Number(a?.qty || 0)) }))
    .filter((a) => String(a?.groupName || '').trim() && Number.isFinite(a?.qty) && a.qty > 0);

  const nextUpdate = { allocations: cleanedAllocs };
  if (isFullyPrimaryNow()) {
    let primaryDoneAt = null;
    try {
      const latest = await PalletGroupReservation.findOne({ orderNumber, warehouseId: primaryWarehouseId, source: 'primary', qty: { $gt: 0 } })
        .sort({ updatedAt: -1 })
        .select('updatedAt')
        .lean();
      if (latest?.updatedAt) {
        const dt = new Date(latest.updatedAt);
        if (!Number.isNaN(dt.getTime())) primaryDoneAt = dt;
      }
    } catch {
      primaryDoneAt = null;
    }
    nextUpdate.status = 'ready_to_ship';
    if (primaryDoneAt) nextUpdate.estFulfillmentDate = primaryDoneAt;
  }

  await UnfulfilledOrder.updateOne({ _id: order._id }, { $set: nextUpdate });
  return true;
};

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

const getPostActionSumsByGroup = (postActions) => {
  const rows = Array.isArray(postActions) ? postActions : [];
  const byGroup = new Map();
  for (const a of rows) {
    const g = normalizeStr(a?.groupName || '');
    const kind = normalizeStr(a?.kind || '').toLowerCase();
    const qty = Math.floor(Number(a?.qty || 0));
    if (!g || !Number.isFinite(qty) || qty <= 0) continue;
    if (!byGroup.has(g)) byGroup.set(g, { returned: 0, damaged: 0, total: 0 });
    const rec = byGroup.get(g);
    if (kind === 'returned') rec.returned += qty;
    else if (kind === 'damaged') rec.damaged += qty;
    rec.total += qty;
  }
  return byGroup;
};

const getOrderedQtyByGroup = (lines) => {
  const out = new Map();
  const list = Array.isArray(lines) ? lines : [];
  for (const ln of list) {
    const g = normalizeStr(ln?.groupName || '');
    const qty = Math.floor(Number(ln?.qty || 0));
    if (!g || !Number.isFinite(qty) || qty <= 0) continue;
    out.set(g, (out.get(g) || 0) + qty);
  }
  return out;
};

export const returnCompletedOrderPallets = async (req, res) => {
  const { id } = req.params;
  const { groupName, qty, notes, actions, committedAt } = req.body || {};

  const doc = await UnfulfilledOrder.findById(id);
  if (!doc) return res.status(404).json({ message: 'Order not found' });

  const status = normalizeStr(doc.status || '').toLowerCase();
  if (status !== 'completed' && status !== 'shipped') return res.status(400).json({ message: 'Only shipped/completed orders can be adjusted' });

  const shipDt = (() => {
    const d = doc?.estFulfillmentDate ? new Date(doc.estFulfillmentDate) : null;
    return d && !Number.isNaN(d.getTime()) ? d : null;
  })();

  const orderedByGroup = getOrderedQtyByGroup(doc.lines);

  const actionList = Array.isArray(actions) && actions.length
    ? actions
    : [{ groupName, qty, notes, committedAt }];

  const parsed = [];
  for (const a of actionList) {
    const g = normalizeStr(a?.groupName || '');
    const q = Math.floor(Number(a?.qty || 0));
    const n = normalizeStr(a?.notes || '');
    if (!g) continue;
    if (!Number.isFinite(q) || q <= 0) continue;
    parsed.push({ groupName: g, qty: q, notes: n, committedAt: a?.committedAt });
  }
  if (!parsed.length) return res.status(400).json({ message: 'No valid actions provided' });

  for (const p of parsed) {
    const ordered = Number(orderedByGroup.get(p.groupName) || 0);
    if (!ordered) return res.status(400).json({ message: `Invalid pallet group: ${p.groupName}` });
  }

  const usedByGroup = getPostActionSumsByGroup(doc.postActions);
  const additionalByGroup = new Map();
  for (const p of parsed) {
    additionalByGroup.set(p.groupName, (additionalByGroup.get(p.groupName) || 0) + p.qty);
  }
  for (const [g, add] of additionalByGroup.entries()) {
    const ordered = Number(orderedByGroup.get(g) || 0);
    const used = Number(usedByGroup.get(g)?.total || 0);
    const remaining = ordered - used;
    if (add > remaining) {
      return res.status(400).json({ message: `Qty exceeds remaining for ${g}. Ordered ${ordered}, already adjusted ${used}, remaining ${Math.max(0, remaining)}.` });
    }
  }

  const committedBy = String(req.user?.username || req.user?.id || '');
  const dtFromBody = (() => {
    const raw = normalizeStr(committedAt);
    if (!raw) return null;
    const d = new Date(`${raw}T00:00:00`);
    return Number.isNaN(d.getTime()) ? null : d;
  })();

  if (shipDt && dtFromBody && dtFromBody < shipDt) {
    return res.status(400).json({ message: 'Return/Damage date cannot be earlier than Estimated Shipdate for Customer' });
  }

  await applyInventoryDeltaForOrder({
    warehouseId: doc.warehouseId,
    orderNumber: doc.orderNumber,
    lines: parsed.map((p) => ({ groupName: p.groupName, qty: p.qty })),
    committedBy,
    deltaSign: +1,
    allowNegative: true,
    reason: 'returned',
  });

  doc.postActions = Array.isArray(doc.postActions) ? doc.postActions : [];
  for (const p of parsed) {
    const dtFromLine = (() => {
      const raw = normalizeStr(p?.committedAt);
      if (!raw) return null;
      const d = new Date(`${raw}T00:00:00`);
      return Number.isNaN(d.getTime()) ? null : d;
    })();

    if (shipDt && dtFromLine && dtFromLine < shipDt) {
      return res.status(400).json({ message: 'Return/Damage date cannot be earlier than Estimated Shipdate for Customer' });
    }

    doc.postActions.push({
      kind: 'returned',
      groupName: p.groupName,
      qty: p.qty,
      notes: p.notes,
      committedAt: dtFromLine || dtFromBody || new Date(),
      committedBy,
    });
  }
  doc.lastUpdatedBy = committedBy;
  await doc.save();

  return res.json({ ok: true, lines: parsed.length });
};

export const damageCompletedOrderPallets = async (req, res) => {
  const { id } = req.params;
  const { groupName, qty, notes, actions, committedAt } = req.body || {};

  const doc = await UnfulfilledOrder.findById(id);
  if (!doc) return res.status(404).json({ message: 'Order not found' });

  const status = normalizeStr(doc.status || '').toLowerCase();
  if (status !== 'completed' && status !== 'shipped') return res.status(400).json({ message: 'Only shipped/completed orders can be adjusted' });

  const shipDt = (() => {
    const d = doc?.estFulfillmentDate ? new Date(doc.estFulfillmentDate) : null;
    return d && !Number.isNaN(d.getTime()) ? d : null;
  })();

  const orderedByGroup = getOrderedQtyByGroup(doc.lines);

  const actionList = Array.isArray(actions) && actions.length
    ? actions
    : [{ groupName, qty, notes, committedAt }];

  const parsed = [];
  for (const a of actionList) {
    const g = normalizeStr(a?.groupName || '');
    const q = Math.floor(Number(a?.qty || 0));
    const n = normalizeStr(a?.notes || '');
    if (!g) continue;
    if (!Number.isFinite(q) || q <= 0) continue;
    parsed.push({ groupName: g, qty: q, notes: n, committedAt: a?.committedAt });
  }
  if (!parsed.length) return res.status(400).json({ message: 'No valid actions provided' });

  for (const p of parsed) {
    const ordered = Number(orderedByGroup.get(p.groupName) || 0);
    if (!ordered) return res.status(400).json({ message: `Invalid pallet group: ${p.groupName}` });
  }

  const usedByGroup = getPostActionSumsByGroup(doc.postActions);
  const additionalByGroup = new Map();
  for (const p of parsed) {
    additionalByGroup.set(p.groupName, (additionalByGroup.get(p.groupName) || 0) + p.qty);
  }
  for (const [g, add] of additionalByGroup.entries()) {
    const ordered = Number(orderedByGroup.get(g) || 0);
    const used = Number(usedByGroup.get(g)?.total || 0);
    const remaining = ordered - used;
    if (add > remaining) {
      return res.status(400).json({ message: `Qty exceeds remaining for ${g}. Ordered ${ordered}, already adjusted ${used}, remaining ${Math.max(0, remaining)}.` });
    }
  }

  const committedBy = String(req.user?.username || req.user?.id || '');
  const dtFromBody = (() => {
    const raw = normalizeStr(committedAt);
    if (!raw) return null;
    const d = new Date(`${raw}T00:00:00`);
    return Number.isNaN(d.getTime()) ? null : d;
  })();

  // Log-only transactions (no stock change). Keep palletsDelta=0.
  for (const p of parsed) {
    await PalletGroupTxn.create({
      poNumber: normalizeStr(doc.orderNumber),
      groupName: p.groupName,
      warehouseId: doc.warehouseId,
      palletsDelta: 0,
      status: 'Adjustment',
      reason: 'damage',
      committedBy,
    });
  }

  doc.postActions = Array.isArray(doc.postActions) ? doc.postActions : [];
  for (const p of parsed) {
    const dtFromLine = (() => {
      const raw = normalizeStr(p?.committedAt);
      if (!raw) return null;
      const d = new Date(`${raw}T00:00:00`);
      return Number.isNaN(d.getTime()) ? null : d;
    })();

    if (shipDt && dtFromLine && dtFromLine < shipDt) {
      return res.status(400).json({ message: 'Return/Damage date cannot be earlier than Estimated Shipdate for Customer' });
    }

    doc.postActions.push({
      kind: 'damaged',
      groupName: p.groupName,
      qty: p.qty,
      notes: p.notes,
      committedAt: dtFromLine || dtFromBody || new Date(),
      committedBy,
    });
  }
  await doc.save();

  return res.json({ ok: true, lines: parsed.length });
};

export const onWaterDetails = async (req, res) => {
  try {
    const warehouseId = String(req.query.warehouseId || '').trim();
    const groupNameRaw = normalizeStr(req.query.groupName || '');
    if (!warehouseId) return res.status(400).json({ message: 'warehouseId required' });
    if (!groupNameRaw) return res.status(400).json({ message: 'groupName required' });

    const secondWarehouse = await getSecondWarehouseFor(warehouseId);
    const whIds = [String(warehouseId || '').trim(), String(secondWarehouse?._id || '').trim()].filter((v) => v);

    const keyOf = (v) =>
      normalizeStr(v)
        .replace(/\u00a0/g, ' ')
        .replace(/\s+/g, ' ')
        .toLowerCase();
    const targetKey = keyOf(groupNameRaw);

    const ships = await Shipment.find({
      status: 'on_water',
      warehouseId: { $in: whIds },
      notes: { $regex: 'pallet-group:', $options: 'i' },
    })
      .select('owNumber reference estDeliveryDate notes createdAt')
      .sort({ estDeliveryDate: 1, createdAt: 1 })
      .lean();

    const re = /pallet-group:([^;|]+);\s*pallets:(\d+)/gi;
    const rows = [];
    for (const s of ships || []) {
      const text = String(s?.notes || '');
      re.lastIndex = 0;
      let m;
      let qty = 0;
      while ((m = re.exec(text)) !== null) {
        const groupKey = keyOf(String(m[1] || '').trim());
        if (!groupKey || groupKey !== targetKey) continue;
        const pallets = Number(m[2] || 0);
        if (!Number.isFinite(pallets) || pallets <= 0) continue;
        qty += pallets;
      }
      if (qty <= 0) continue;
      rows.push({
        id: String(s?._id || ''),
        reference: String(s?.owNumber || s?.reference || s?._id || ''),
        edd: fmtDateYmd(s?.estDeliveryDate || null) || '',
        qty,
      });
    }

    res.json({ warehouseId, groupName: groupNameRaw, rows });
  } catch (e) {
    console.error('orders.onWaterDetails failed', e);
    res.status(500).json({ message: 'Failed to load on-water details' });
  }
};

export const onProcessDetails = async (req, res) => {
  try {
    const groupNameRaw = normalizeStr(req.query.groupName || '');
    if (!groupNameRaw) return res.status(400).json({ message: 'groupName required' });

    const esc = (s) => String(s || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const rx = new RegExp(`^${esc(groupNameRaw).replace(/\s+/g, '\\s+').replace(/\u00a0/g, '\\s+')}\s*$`, 'i');

    const rows = await OnProcessPallet.aggregate([
      { $match: { status: { $ne: 'cancelled' }, groupName: { $regex: rx } } },
      {
        $addFields: {
          remaining: { $subtract: ['$totalPallet', { $ifNull: ['$transferredPallet', 0] }] },
        },
      },
      { $match: { remaining: { $gt: 0 } } },
      {
        $lookup: {
          from: OnProcessBatch.collection.name,
          localField: 'batchId',
          foreignField: '_id',
          as: 'batch',
        },
      },
      { $unwind: { path: '$batch', preserveNullAndEmptyArrays: true } },
      {
        $project: {
          _id: 0,
          reference: { $ifNull: ['$batch.reference', '$poNumber'] },
          edd: '$batch.estFinishDate',
          qty: '$remaining',
        },
      },
      { $sort: { edd: 1, reference: 1 } },
    ]);

    const mapped = (rows || []).map((r) => ({
      reference: normalizeStr(r?.reference || ''),
      edd: fmtDateYmd(r?.edd || null) || '',
      qty: Number(r?.qty || 0),
    }));

    return res.json({ groupName: groupNameRaw, rows: mapped });
  } catch (e) {
    console.error('orders.onProcessDetails failed', e);
    return res.status(500).json({ message: 'Failed to load on-process details' });
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
  // legacy aliases
  if (s === 'created') return 'processing';
  if (s === 'create') return 'processing';
  if (s === 'ready-to-ship') return 'ready_to_ship';
  if (s === 'ready to ship') return 'ready_to_ship';
  if (s === 'fulfilled') return 'completed';
  if (s === 'cancelled') return 'canceled';
  if (s === 'cancel') return 'canceled';
  // canonical
  if (s === 'backorder') return 'processing';
  return s;
};

const ORDER_STATUSES = ['processing', 'ready_to_ship', 'shipped', 'delivered', 'completed', 'canceled'];

const isInventoryConsumingStatus = (s) => {
  const v = normalizeOrderStatus(s);
  // Physical stock is deducted at SHIPPED and remains deducted after.
  return v === 'shipped' || v === 'delivered' || v === 'completed';
};

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
    status: 'completed',
    committedBy: committedBy || '',
  });

  return deducted;
};

export const createUnfulfilledOrder = async (req, res) => {
  const { warehouseId, customerEmail, customerName, customerPhone, createdAtOrder, originalPrice, discountPercent, estFulfillmentDate, estDeliveredDate, shippingAddress, notes, lines = [], status } = req.body || {};
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

  const nextStatus = normalizeOrderStatus(status) || 'processing';
  if (!ORDER_STATUSES.includes(nextStatus) || nextStatus === 'completed' || nextStatus === 'canceled') {
    return res.status(400).json({ message: 'Invalid status' });
  }

  const orderNumber = await getNextManualOrderNumber();

  const committedBy = String(req.user?.username || req.user?.id || '');

  // Allocation hierarchy: Primary Warehouse -> On-Water -> On-Process -> 2nd Warehouse
  const secondWarehouse = await getSecondWarehouseFor(warehouseId);

  const keyOf = (v) =>
    normalizeStr(v)
      .replace(/\u00a0/g, ' ')
      .replace(/\s+/g, ' ')
      .toLowerCase();
  const groups = await ItemGroup.find({ active: true }).select('name lineItem').lean();
  const byGroupKey = new Map((groups || []).map((g) => [keyOf(g?.name || ''), String(g?.name || '').trim()]));
  const byLineItemKey = new Map((groups || []).map((g) => [keyOf(g?.lineItem || ''), String(g?.name || '').trim()]));
  const resolveToGroupName = (raw) => {
    const s = String(raw || '').trim();
    if (!s) return '';
    const k = keyOf(s);
    return byGroupKey.get(k) || byLineItemKey.get(k) || s;
  };

  const reservation = await getReservationMaps({ warehouseId });
  const onWaterMap = await buildOnWaterMapForWarehouse({ warehouseId, resolveToGroupName });
  const onProcessMap = await buildOnProcessMap({ resolveToGroupName });

  const allocations = [];
  const deductPrimary = new Map();
  const deductSecond = new Map();
  const reserveOnWater = new Map();
  const reserveOnProcess = new Map();

  const shortages = [];

  for (const ln of parsedLines) {
    const groupName = String(ln.groupName || '').trim();
    const need = Math.floor(Number(ln.qty || 0));
    if (!groupName || !Number.isFinite(need) || need <= 0) continue;

    // Primary (reserve only; do NOT deduct yet)
    const primaryStockDoc = await PalletGroupStock.findOne({ warehouseId, groupName }).lean();
    const primaryReserved = Math.max(0, Number(reservation.physicalByWarehouse?.get(String(warehouseId))?.get(groupName) || 0));
    const primaryAvail = Math.max(0, Number(primaryStockDoc?.pallets || 0) - primaryReserved);
    let remaining = need;
    const takePrimary = Math.min(primaryAvail, remaining);
    if (takePrimary > 0) {
      deductPrimary.set(groupName, (deductPrimary.get(groupName) || 0) + takePrimary);
      allocations.push({ groupName, qty: takePrimary, source: 'primary' });
      remaining -= takePrimary;
    }

    // On-Water (subtract existing reservations)
    const onWaterTotal = Math.max(0, Number(onWaterMap.get(groupName) || 0));
    const onWaterReserved = Math.max(0, Number(reservation.onWater.get(groupName) || 0));
    const onWaterAvail = Math.max(0, onWaterTotal - onWaterReserved);
    const takeOnWater = Math.min(onWaterAvail, remaining);
    if (takeOnWater > 0) {
      reserveOnWater.set(groupName, (reserveOnWater.get(groupName) || 0) + takeOnWater);
      allocations.push({ groupName, qty: takeOnWater, source: 'on_water' });
      remaining -= takeOnWater;
    }

    // 2nd Warehouse (reserve only; do NOT deduct yet)
    if (remaining > 0 && secondWarehouse?._id) {
      const wh2 = secondWarehouse._id;
      const secondStockDoc = await PalletGroupStock.findOne({ warehouseId: wh2, groupName }).lean();
      const secondReserved = Math.max(0, Number(reservation.physicalByWarehouse?.get(String(wh2))?.get(groupName) || 0));
      const secondAvail = Math.max(0, Number(secondStockDoc?.pallets || 0) - secondReserved);
      const takeSecond = Math.min(secondAvail, remaining);
      if (takeSecond > 0) {
        deductSecond.set(groupName, (deductSecond.get(groupName) || 0) + takeSecond);
        allocations.push({ groupName, qty: takeSecond, source: 'second', warehouseId: wh2 });
        remaining -= takeSecond;
      }
    }

    // On-Process (subtract existing reservations)
    const onProcessTotal = Math.max(0, Number(onProcessMap.get(groupName) || 0));
    const onProcessReserved = Math.max(0, Number(reservation.onProcess.get(groupName) || 0));
    const onProcessAvail = Math.max(0, onProcessTotal - onProcessReserved);
    const takeOnProcess = Math.min(onProcessAvail, remaining);
    if (takeOnProcess > 0) {
      reserveOnProcess.set(groupName, (reserveOnProcess.get(groupName) || 0) + takeOnProcess);
      allocations.push({ groupName, qty: takeOnProcess, source: 'on_process' });
      remaining -= takeOnProcess;
    }

    if (remaining > 0) {
      shortages.push({
        lineItem: String(ln?.lineItem || '').trim(),
        groupName,
        required: need,
        available: Math.max(0, need - remaining),
      });
    }
  }

  if (shortages.length) {
    return res.status(400).json(noStocksPayload({ items: shortages }));
  }

  // Create reservations (all tiers). Physical stock is only deducted when order becomes SHIPPED.
  const reserveDocs = [];
  for (const [groupName, qty] of deductPrimary.entries()) {
    reserveDocs.push({ orderNumber, warehouseId, sourceWarehouseId: warehouseId, groupName, source: 'primary', qty, committedBy });
  }
  for (const [groupName, qty] of reserveOnWater.entries()) {
    reserveDocs.push({ orderNumber, warehouseId, groupName, source: 'on_water', qty, committedBy });
  }
  for (const [groupName, qty] of reserveOnProcess.entries()) {
    reserveDocs.push({ orderNumber, warehouseId, groupName, source: 'on_process', qty, committedBy });
  }
  if (secondWarehouse?._id) {
    for (const [groupName, qty] of deductSecond.entries()) {
      reserveDocs.push({ orderNumber, warehouseId, sourceWarehouseId: secondWarehouse._id, groupName, source: 'second', qty, committedBy });
    }
  }
  if (reserveDocs.length) {
    await PalletGroupReservation.insertMany(reserveDocs);
  }

  const nOriginal = Number(originalPrice);
  const hasOriginal = Number.isFinite(nOriginal);
  const nDiscount = Number(discountPercent);
  const hasDiscount = Number.isFinite(nDiscount);
  const safeDiscount = hasDiscount ? Math.min(100, Math.max(0, nDiscount)) : 0;
  const computedFinal = hasOriginal ? nOriginal * (1 - safeDiscount / 100) : null;
  const doc = await UnfulfilledOrder.create({
    orderNumber,
    warehouseId,
    customerEmail: normalizeStr(customerEmail),
    customerName: normalizeStr(customerName),
    customerPhone: normalizeStr(customerPhone),
    createdAtOrder: createdAtOrder ? new Date(createdAtOrder) : new Date(),
    originalPrice: hasOriginal ? nOriginal : undefined,
    discountPercent: hasDiscount ? safeDiscount : undefined,
    finalPrice: hasOriginal ? computedFinal : undefined,
    estFulfillmentDate: estFulfillmentDate ? new Date(estFulfillmentDate) : undefined,
    estDeliveredDate: nextStatus === 'shipped' && normalizeStr(estDeliveredDate) ? new Date(normalizeStr(estDeliveredDate)) : undefined,
    shippingAddress: normalizeStr(shippingAddress),
    notes: normalizeStr(notes),
    lines: parsedLines,
    allocations,
    status: nextStatus,
    committedBy,
  });
  res.status(201).json(doc);
};

export const palletPicker = async (req, res) => {
  try {
    const warehouseId = String(req.query.warehouseId || '').trim();
    const q = normalizeStr(req.query.q || '').toLowerCase();
    const debugGroupNameRaw = normalizeStr(req.query.debugGroupName || '');
    if (!warehouseId) return res.status(400).json({ message: 'warehouseId required' });

    const keyOf = (v) =>
      normalizeStr(v)
        .replace(/\u00a0/g, ' ')
        .replace(/\s+/g, ' ')
        .toLowerCase();

    const warehouses = await Warehouse.find({}).select('name').sort({ name: 1 }).lean();
    const whIds = (warehouses || []).map((w) => String(w._id));

    const groups = await ItemGroup.find({ active: true }).select('name lineItem').lean();
    const byGroupKey = new Map((groups || []).map((g) => [keyOf(g?.name || ''), String(g?.name || '').trim()]));
    const byLineItemKey = new Map((groups || []).map((g) => [keyOf(g?.lineItem || ''), String(g?.name || '').trim()]));
    const resolveToGroupName = (raw) => {
      const s = String(raw || '').trim();
      if (!s) return '';
      const k = keyOf(s);
      return byGroupKey.get(k) || byLineItemKey.get(k) || s;
    };

    // Warehouse availability per group
    // Note: do NOT filter by Warehouse list here; inventory page relies on all PalletGroupStock entries,
    // and filtering can hide valid stock if Warehouse IDs drift or are missing in the list.
    const stocks = await PalletGroupStock.aggregate([
      { $group: { _id: { groupName: '$groupName', warehouseId: '$warehouseId' }, pallets: { $sum: '$pallets' } } },
    ]);
    const debugStocks = debugGroupNameRaw
      ? (stocks || []).filter((s) => keyOf(String(s?._id?.groupName || '').trim()) === keyOf(debugGroupNameRaw))
      : [];
    const stockByGroup = new Map();
    for (const s of stocks) {
      const rawGroup = String(s?._id?.groupName || '').trim();
      const groupName = resolveToGroupName(rawGroup);
      const groupKey = keyOf(groupName);
      const wid = String(s?._id?.warehouseId || '').trim();
      if (!groupKey || !wid) continue;
      if (!stockByGroup.has(groupKey)) stockByGroup.set(groupKey, {});
      stockByGroup.get(groupKey)[wid] = Number(s.pallets || 0);
    }

    const reserved = await getReservationMaps({ warehouseId });

    const secondWarehouse = await getSecondWarehouseFor(warehouseId);
    const onWaterWhIds = [String(warehouseId || '').trim(), String(secondWarehouse?._id || '').trim()].filter((v) => v);

    // On-water per group for the selected warehouse: sum pallets + latest EDD
    const ships = await Shipment.find({
      status: 'on_water',
      warehouseId: { $in: onWaterWhIds },
      notes: { $regex: 'pallet-group:', $options: 'i' },
    })
      .select('notes estDeliveryDate')
      .lean();
    const onWater = new Map();
    const onWaterEdd = new Map();
    const onWaterShipments = new Map();
    const re = /pallet-group:([^;|]+);\s*pallets:(\d+)/gi;
    for (const s of ships) {
      const text = String(s?.notes || '');
      re.lastIndex = 0;
      let m;
      while ((m = re.exec(text)) !== null) {
        const groupName = resolveToGroupName(String(m[1] || '').trim());
        const groupKey = keyOf(groupName);
        const pallets = Number(m[2] || 0);
        if (!groupKey || !Number.isFinite(pallets) || pallets <= 0) continue;
        onWater.set(groupKey, (onWater.get(groupKey) || 0) + pallets);
        const d = s?.estDeliveryDate ? new Date(s.estDeliveryDate) : null;
        if (d && !Number.isNaN(d.getTime())) {
          const prev = onWaterEdd.get(groupKey);
          if (!prev || d.getTime() > prev.getTime()) onWaterEdd.set(groupKey, d);

          if (!onWaterShipments.has(groupKey)) onWaterShipments.set(groupKey, []);
          onWaterShipments.get(groupKey).push({ d, qty: pallets });
        }
      }
    }

    // On-process per group: remaining pallets (total - transferred) and earliest estFinishDate across batches
    const opAgg = await OnProcessPallet.aggregate([
      { $match: { status: { $ne: 'cancelled' } } },
      {
        $addFields: {
          remaining: { $subtract: ['$totalPallet', { $ifNull: ['$transferredPallet', 0] }] },
        },
      },
      { $match: { remaining: { $gt: 0 } } },
      {
        $lookup: {
          from: OnProcessBatch.collection.name,
          localField: 'batchId',
          foreignField: '_id',
          as: 'batch',
        },
      },
      { $unwind: { path: '$batch', preserveNullAndEmptyArrays: true } },
      {
        $group: {
          _id: '$groupName',
          pallets: { $sum: '$remaining' },
          minEdd: { $min: '$batch.estFinishDate' },
        },
      },
    ]);
    const onProcess = new Map();
    const onProcessEdd = new Map();
    for (const r of opAgg) {
      const groupName = resolveToGroupName(String(r?._id || '').trim());
      const groupKey = keyOf(groupName);
      if (!groupKey) continue;
      onProcess.set(groupKey, Number(r.pallets || 0));
      const d = r?.minEdd ? new Date(r.minEdd) : null;
      if (d && !Number.isNaN(d.getTime())) onProcessEdd.set(groupKey, d);
    }

    // Queued demand: existing open orders in this warehouse that will consume supply tiers before new orders
    // Include legacy statuses for backward compatibility
    const openStatuses = ['processing', 'create', 'backorder', 'created'];
    const unfulfilledQueuedAgg = await UnfulfilledOrder.aggregate([
      { $addFields: { warehouseIdStr: { $toString: '$warehouseId' } } },
      { $match: { warehouseIdStr: String(warehouseId), status: { $in: openStatuses } } },
      { $unwind: '$lines' },
      { $group: { _id: '$lines.groupName', pallets: { $sum: '$lines.qty' } } },
    ]);
    const importQueuedAgg = await FulfilledOrderImport.aggregate([
      { $addFields: { warehouseIdStr: { $toString: '$warehouseId' } } },
      { $match: { warehouseIdStr: String(warehouseId), status: { $in: openStatuses } } },
      { $unwind: '$lines' },
      { $group: { _id: '$lines.groupName', pallets: { $sum: '$lines.qty' } } },
    ]);
    const queued = new Map();
    for (const r of [...(unfulfilledQueuedAgg || []), ...(importQueuedAgg || [])]) {
      const groupName = resolveToGroupName(String(r?._id || '').trim());
      const groupKey = keyOf(groupName);
      if (!groupKey) continue;
      queued.set(groupKey, (queued.get(groupKey) || 0) + Number(r.pallets || 0));
    }

    const rows = (groups || [])
      .map((g) => {
        const groupName = String(g?.name || '').trim();
        const groupKey = keyOf(groupName);
        const lineItem = String(g?.lineItem || '').trim();
        const basePerWarehouse = stockByGroup.get(groupKey) || {};

        // Subtract reserved physical stock from each warehouse bucket so UI reflects queued reservations.
        const perWarehouse = {};
        for (const [wid, qtyRaw] of Object.entries(basePerWarehouse || {})) {
          const reservedQty = Number(reserved.physicalByWarehouse?.get(String(wid))?.get(groupName) || 0);
          perWarehouse[wid] = Math.max(0, Number(qtyRaw || 0) - reservedQty);
        }

        const selectedWarehouseAvailable = Number(perWarehouse[String(warehouseId)] || 0);

        const reservedOnWater = Number(reserved.onWater.get(groupName) || 0);
        const reservedOnProcess = Number(reserved.onProcess.get(groupName) || 0);
        const onWaterAvail = Math.max(0, Number(onWater.get(groupKey) || 0) - reservedOnWater);
        const onProcessAvail = Math.max(0, Number(onProcess.get(groupKey) || 0) - reservedOnProcess);

        const shipList = (onWaterShipments.get(groupKey) || [])
          .filter((x) => x?.d && !Number.isNaN(x.d.getTime()) && Number(x?.qty || 0) > 0)
          .sort((a, b) => a.d.getTime() - b.d.getTime())
          .map((x) => ({ edd: fmtDateYmd(x.d) || '', qty: Number(x.qty || 0) }));
        return {
          groupName,
          lineItem,
          perWarehouse,
          selectedWarehouseAvailable,
          queuedPallets: Number(queued.get(groupKey) || 0),
          onWaterPallets: onWaterAvail,
          onWaterEdd: fmtDateYmd(onWaterEdd.get(groupKey) || null) || '',
          onWaterShipments: shipList,
          onProcessPallets: onProcessAvail,
          onProcessEdd: fmtDateYmd(onProcessEdd.get(groupKey) || null) || '',
        };
      })
      .filter((r) => {
        if (!q) return true;
        return (
          String(r.groupName || '').toLowerCase().includes(q) ||
          String(r.lineItem || '').toLowerCase().includes(q)
        );
      })
      .sort((a, b) => String(a.groupName).localeCompare(String(b.groupName)));

    const debug = debugGroupNameRaw
      ? (() => {
          const k = keyOf(debugGroupNameRaw);
          const resolved = resolveToGroupName(debugGroupNameRaw);
          const rk = keyOf(resolved);
          const row = rows.find((r) => keyOf(r.groupName) === rk) || null;

          let debugOnWaterTotal = 0;
          const debugOnWaterShips = [];
          try {
            for (const s of ships || []) {
              const text = String(s?.notes || '');
              re.lastIndex = 0;
              let m;
              let qty = 0;
              while ((m = re.exec(text)) !== null) {
                const groupName = resolveToGroupName(String(m[1] || '').trim());
                const groupKey = keyOf(groupName);
                if (!groupKey || groupKey !== rk) continue;
                const pallets = Number(m[2] || 0);
                if (!Number.isFinite(pallets) || pallets <= 0) continue;
                qty += pallets;
              }
              if (qty <= 0) continue;
              debugOnWaterTotal += qty;
              debugOnWaterShips.push({
                id: String(s?._id || ''),
                warehouseId: String(s?.warehouseId || ''),
                reference: String(s?.reference || ''),
                estDeliveryDate: fmtDateYmd(s?.estDeliveryDate || null) || '',
                qty,
                notes: String(s?.notes || ''),
              });
            }
          } catch {
            // ignore
          }

          return {
            debugGroupName: debugGroupNameRaw,
            debugKey: k,
            resolvedGroupName: resolved,
            resolvedKey: rk,
            perWarehouseFromMap: stockByGroup.get(rk) || null,
            sampleRowPerWarehouse: row ? row.perWarehouse : null,
            rawAggregatedStocks: (debugStocks || []).map((s) => ({
              groupName: String(s?._id?.groupName || ''),
              warehouseId: String(s?._id?.warehouseId || ''),
              pallets: Number(s?.pallets || 0),
            })),
            onWaterWarehouseIdsScanned: onWaterWhIds,
            onWaterParsedTotal: debugOnWaterTotal,
            onWaterMatchedShipments: debugOnWaterShips,
          };
        })()
      : undefined;

    res.json({
      warehouseId,
      warehouses: (warehouses || []).map((w) => ({ _id: String(w._id), name: w.name })),
      rows,
      ...(debug ? { debug } : {}),
    });
  } catch (e) {
    console.error('orders.palletPicker failed', e);
    res.status(500).json({ message: 'Failed to load pallet picker data' });
  }
};

export const listUnfulfilledOrders = async (req, res) => {
  const limit = Math.min(Math.max(Number(req.query?.limit) || 200, 1), 1000);
  const docs = await UnfulfilledOrder.find({})
    .sort({ createdAt: -1 })
    .limit(limit)
    .lean();

  const keyOf = (v) =>
    normalizeStr(v)
      .replace(/\u00a0/g, ' ')
      .replace(/\s+/g, ' ')
      .toLowerCase();

  const toYmd = (d) => fmtDateYmd(d);
  const addMonthsYmd = (ymd, months) => {
    const s = String(ymd || '').trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return '';
    const dt = new Date(`${s}T00:00:00`);
    if (Number.isNaN(dt.getTime())) return '';
    const next = new Date(dt);
    next.setMonth(next.getMonth() + Math.floor(Number(months || 0)));
    return fmtDateYmd(next);
  };

  const today = fmtDateYmd(new Date());
  const cutoffSecond = addMonthsYmd(today, 3);

  const whIds = Array.from(new Set((docs || []).map((d) => String(d?.warehouseId || '').trim()).filter((v) => v)));

  const onWaterEddByWarehouse = new Map();
  const onWaterShipmentsByWarehouse = new Map();
  if (whIds.length) {
    const ships = await Shipment.find({
      status: 'on_water',
      warehouseId: { $in: whIds },
      notes: { $regex: 'pallet-group:', $options: 'i' },
    })
      .select('notes estDeliveryDate warehouseId sourceWarehouseId')
      .lean();

    const re = /pallet-group:([^;|]+);\s*pallets:(\d+)/gi;
    for (const s of ships || []) {
      const d = s?.estDeliveryDate ? new Date(s.estDeliveryDate) : null;
      if (!d || Number.isNaN(d.getTime())) continue;

      const wid = String(s?.warehouseId || '').trim();
      if (!wid || !whIds.includes(wid)) continue;

      const text = String(s?.notes || '');
      re.lastIndex = 0;
      let m;
      while ((m = re.exec(text)) !== null) {
        const groupKey = keyOf(String(m[1] || '').trim());
        const pallets = Number(m[2] || 0);
        if (!groupKey || !Number.isFinite(pallets) || pallets <= 0) continue;

        if (!onWaterEddByWarehouse.has(wid)) onWaterEddByWarehouse.set(wid, new Map());
        const mm = onWaterEddByWarehouse.get(wid);
        const prev = mm.get(groupKey);
        if (!prev || d.getTime() > prev.getTime()) mm.set(groupKey, d);

        if (!onWaterShipmentsByWarehouse.has(wid)) onWaterShipmentsByWarehouse.set(wid, new Map());
        const sm = onWaterShipmentsByWarehouse.get(wid);
        if (!sm.has(groupKey)) sm.set(groupKey, []);
        sm.get(groupKey).push({ d, qty: Math.floor(pallets) });
      }
    }
  }

  const onProcessEddByGroup = new Map();
  try {
    const opAgg = await OnProcessPallet.aggregate([
      { $match: { status: { $ne: 'cancelled' } } },
      { $addFields: { remaining: { $subtract: ['$totalPallet', { $ifNull: ['$transferredPallet', 0] }] } } },
      { $match: { remaining: { $gt: 0 } } },
      {
        $lookup: {
          from: OnProcessBatch.collection.name,
          localField: 'batchId',
          foreignField: '_id',
          as: 'batch',
        },
      },
      { $unwind: { path: '$batch', preserveNullAndEmptyArrays: true } },
      { $group: { _id: '$groupName', minEdd: { $min: '$batch.estFinishDate' } } },
    ]);
    for (const r of opAgg || []) {
      const k = keyOf(String(r?._id || '').trim());
      const d = r?.minEdd ? new Date(r.minEdd) : null;
      if (!k || !d || Number.isNaN(d.getTime())) continue;
      onProcessEddByGroup.set(k, d);
    }
  } catch {
    // ignore
  }

  // Build reservation-based view per order so shipdate is derived from actual reserved tiers (not allocations).
  const reservationByOrder = new Map();
  try {
    const processingOrders = (docs || []).filter((d) => {
      const s = normalizeOrderStatus(d?.status || 'processing') || 'processing';
      return s === 'processing' || s === 'ready_to_ship';
    });
    const orderNumbers = Array.from(
      new Set(
        processingOrders
          .map((d) => String(d?.orderNumber || '').trim())
          .filter((v) => v)
      )
    );

    if (orderNumbers.length && whIds.length) {
      const resAgg = await PalletGroupReservation.aggregate([
        { $addFields: { orderNumberStr: { $toString: '$orderNumber' }, warehouseIdStr: { $toString: '$warehouseId' } } },
        { $match: { orderNumberStr: { $in: orderNumbers }, warehouseIdStr: { $in: whIds }, qty: { $gt: 0 } } },
        {
          $group: {
            _id: { orderNumberStr: '$orderNumberStr', source: '$source', groupName: '$groupName' },
            qty: { $sum: '$qty' },
            maxUpdatedAt: { $max: '$updatedAt' },
          },
        },
      ]);

      for (const r of resAgg || []) {
        const orderNumber = String(r?._id?.orderNumberStr || '').trim();
        const source = String(r?._id?.source || '').trim().toLowerCase();
        const groupKey = keyOf(String(r?._id?.groupName || '').trim());
        const qty = Math.floor(Number(r?.qty || 0));
        if (!orderNumber || !source || !Number.isFinite(qty) || qty <= 0) continue;

        if (!reservationByOrder.has(orderNumber)) {
          reservationByOrder.set(orderNumber, { hasSecond: false, onWater: new Map(), onProcessKeys: new Set(), primaryUpdatedAt: null });
        }
        const rec = reservationByOrder.get(orderNumber);
        if (source === 'second') rec.hasSecond = true;
        else if (source === 'on_water' && groupKey) rec.onWater.set(groupKey, (rec.onWater.get(groupKey) || 0) + qty);
        else if (source === 'on_process' && groupKey) rec.onProcessKeys.add(groupKey);
        else if (source === 'primary') {
          const dt = (r?.maxUpdatedAt ? new Date(r.maxUpdatedAt) : null);
          if (dt && !Number.isNaN(dt.getTime())) {
            const prev = rec.primaryUpdatedAt ? new Date(rec.primaryUpdatedAt) : null;
            if (!prev || dt.getTime() > prev.getTime()) rec.primaryUpdatedAt = dt;
          }
        }
      }
    }
  } catch {
    // ignore; fall back to existing stored estFulfillmentDate
  }

  const computeOnWaterCompletionYmd = ({ warehouseId, groupKey, neededQty }) => {
    const wid = String(warehouseId || '').trim();
    const gk = String(groupKey || '').trim();
    let remaining = Math.max(0, Math.floor(Number(neededQty || 0)));
    if (!wid || !gk || !Number.isFinite(remaining) || remaining <= 0) return '';

    const sm = onWaterShipmentsByWarehouse.get(wid);
    const list = sm && sm.get(gk) ? sm.get(gk) : [];
    const ships = (Array.isArray(list) ? list : [])
      .filter((x) => x?.d && !Number.isNaN(new Date(x.d).getTime()) && Number(x?.qty || 0) > 0)
      .map((x) => ({ d: new Date(x.d), qty: Math.floor(Number(x.qty || 0)) }))
      .filter((x) => !Number.isNaN(x.d.getTime()) && x.qty > 0)
      .sort((a, b) => a.d.getTime() - b.d.getTime());

    let last = null;
    for (const s of ships) {
      if (remaining <= 0) break;
      const take = Math.min(remaining, Math.max(0, s.qty));
      if (take <= 0) continue;
      remaining -= take;
      last = s.d;
    }

    if (remaining <= 0 && last) return toYmd(last);
    // fallback to previous aggregated behavior (latest EDD) if we can't cover
    const map = onWaterEddByWarehouse.get(wid);
    const d2 = map ? map.get(gk) : null;
    return d2 ? toYmd(d2) : '';
  };

  const out = (docs || []).map((d) => {
    const status = normalizeOrderStatus(d?.status || 'processing') || 'processing';
    if (status !== 'processing' && status !== 'ready_to_ship') return d;

    const orderNumber = String(d?.orderNumber || '').trim();
    const r = orderNumber && reservationByOrder.has(orderNumber)
      ? reservationByOrder.get(orderNumber)
      : { hasSecond: false, onWater: new Map(), onProcessKeys: new Set(), primaryUpdatedAt: null };

    const wid = String(d?.warehouseId || '').trim();

    let best = '';

    for (const [gKey, qty] of Array.from((r?.onWater || new Map()).entries())) {
      const ymd = computeOnWaterCompletionYmd({ warehouseId: wid, groupKey: gKey, neededQty: qty });
      if (ymd && (!best || ymd > best)) best = ymd;
    }
    for (const gKey of Array.from(r?.onProcessKeys || [])) {
      const d2 = onProcessEddByGroup.get(gKey) || null;
      const base = d2 ? toYmd(d2) : '';
      const ready = base ? addMonthsYmd(base, 3) : '';
      if (ready && (!best || ready > best)) best = ready;
    }

    // If fully primary (no second/on-water/on-process), use the completion date (primary reservation last updated)
    // and mark order READY TO SHIP; otherwise keep it PROCESSING.
    const hasOnWater = r?.onWater && typeof r.onWater?.size === 'number' ? r.onWater.size > 0 : false;
    const fullyPrimary = !r?.hasSecond && !hasOnWater && !(r?.onProcessKeys && r.onProcessKeys.size);
    const primaryDone = r?.primaryUpdatedAt ? toYmd(r.primaryUpdatedAt) : '';
    // If this order pulls from the 2nd warehouse, shipdate is at least today + 3 months.
    // Final shipdate should be the maximum across all applicable sources.
    let ymdOut = best || (fullyPrimary && primaryDone ? primaryDone : today);
    if (r?.hasSecond && cutoffSecond) {
      if (!ymdOut || cutoffSecond > ymdOut) ymdOut = cutoffSecond;
    }
    const derivedStatus = fullyPrimary ? 'ready_to_ship' : 'processing';
    return {
      ...d,
      status: derivedStatus,
      estFulfillmentDate: ymdOut ? new Date(`${ymdOut}T00:00:00`) : d.estFulfillmentDate,
    };
  });

  res.json(out);
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
  if (normalizeOrderStatus(existing.status || '') === 'completed') return res.status(400).json({ message: 'Completed orders are locked' });

  const { email, billingName, billingPhone, shippingName, shippingStreet, fulfilledAt, createdAtOrder } = req.body || {};
  const set = {};
  const committedBy = String(req.user?.username || req.user?.id || '');
  set.lastUpdatedBy = committedBy;
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

  const prev = normalizeOrderStatus(doc.status || 'processing') || 'processing';
  if (prev === 'completed') return res.status(400).json({ message: 'Completed orders are locked' });

  const committedBy = String(req.user?.username || req.user?.id || '');
  const orderLines = (doc.lines || []).map((l) => ({ groupName: l.groupName, qty: l.qty }));

  if (next === 'canceled' && prev !== 'canceled') {
    if (isInventoryConsumingStatus(prev)) {
      const allocs = Array.isArray(doc.allocations) ? doc.allocations : [];
      const primaryMap = new Map();
      const secondMap = new Map();
      for (const a of allocs) {
        const g = String(a?.groupName || '').trim();
        const qty = Math.floor(Number(a?.qty || 0));
        const src = String(a?.source || '').trim();
        if (!g || !Number.isFinite(qty) || qty <= 0) continue;
        if (src === 'second') {
          const wid = String(a?.warehouseId || '').trim();
          if (!wid) continue;
          if (!secondMap.has(wid)) secondMap.set(wid, new Map());
          secondMap.get(wid).set(g, (secondMap.get(wid).get(g) || 0) + qty);
        } else if (src === 'primary') {
          primaryMap.set(g, (primaryMap.get(g) || 0) + qty);
        }
      }

      // Backward compatibility: if no allocations saved, restore from primary using order lines.
      const primaryLines = primaryMap.size
        ? Array.from(primaryMap.entries()).map(([groupName, qty]) => ({ groupName, qty }))
        : orderLines;
      if (primaryLines.length) {
        await applyInventoryDeltaForOrder({ warehouseId: doc.warehouseId, orderNumber: doc.orderNumber, lines: primaryLines, committedBy, deltaSign: +1, allowNegative: true, reason: 'order_canceled' });
      }
      for (const [wid, gmap] of secondMap.entries()) {
        const lines = Array.from(gmap.entries()).map(([groupName, qty]) => ({ groupName, qty }));
        if (!lines.length) continue;
        await applyInventoryDeltaForOrder({ warehouseId: wid, orderNumber: doc.orderNumber, lines, committedBy, deltaSign: +1, allowNegative: true, reason: 'order_canceled_second_warehouse' });
      }

      // clear logical reservations
      await PalletGroupReservation.deleteMany({ orderNumber: String(doc.orderNumber || '').trim() });
    }
    doc.status = 'canceled';
    await doc.save();
    return res.json(doc.toObject());
  }

  if (next === 'processing' && prev === 'canceled') {
    // Re-allocate using current availability
    const secondWarehouse = await getSecondWarehouseFor(doc.warehouseId);
    const keyOf = (v) =>
      normalizeStr(v)
        .replace(/\u00a0/g, ' ')
        .replace(/\s+/g, ' ')
        .toLowerCase();
    const groups = await ItemGroup.find({ active: true }).select('name lineItem').lean();
    const byGroupKey = new Map((groups || []).map((g) => [keyOf(g?.name || ''), String(g?.name || '').trim()]));
    const byLineItemKey = new Map((groups || []).map((g) => [keyOf(g?.lineItem || ''), String(g?.name || '').trim()]));
    const resolveToGroupName = (raw) => {
      const s = String(raw || '').trim();
      if (!s) return '';
      const k = keyOf(s);
      return byGroupKey.get(k) || byLineItemKey.get(k) || s;
    };

    const reservation = await getReservationMaps({ warehouseId: doc.warehouseId });
    const onWaterMap = await buildOnWaterMapForWarehouse({ warehouseId: doc.warehouseId, resolveToGroupName });
    const onProcessMap = await buildOnProcessMap({ resolveToGroupName });

    const allocs = [];
    const deductPrimary = new Map();
    const deductSecond = new Map();
    const reserveOnWater = new Map();
    const reserveOnProcess = new Map();

    const shortages = [];

    for (const ln of orderLines) {
      const groupName = String(ln.groupName || '').trim();
      const need = Math.floor(Number(ln.qty || 0));
      if (!groupName || !Number.isFinite(need) || need <= 0) continue;

      const primaryStockDoc = await PalletGroupStock.findOne({ warehouseId: doc.warehouseId, groupName }).lean();
      const primaryAvail = Math.max(0, Number(primaryStockDoc?.pallets || 0));
      let remaining = need;
      const takePrimary = Math.min(primaryAvail, remaining);
      if (takePrimary > 0) {
        deductPrimary.set(groupName, (deductPrimary.get(groupName) || 0) + takePrimary);
        allocs.push({ groupName, qty: takePrimary, source: 'primary' });
        remaining -= takePrimary;
      }

      const onWaterTotal = Math.max(0, Number(onWaterMap.get(groupName) || 0));
      const onWaterReserved = Math.max(0, Number(reservation.onWater.get(groupName) || 0));
      const onWaterAvail = Math.max(0, onWaterTotal - onWaterReserved);
      const takeOnWater = Math.min(onWaterAvail, remaining);
      if (takeOnWater > 0) {
        reserveOnWater.set(groupName, (reserveOnWater.get(groupName) || 0) + takeOnWater);
        allocs.push({ groupName, qty: takeOnWater, source: 'on_water' });
        remaining -= takeOnWater;
      }

      const onProcessTotal = Math.max(0, Number(onProcessMap.get(groupName) || 0));
      const onProcessReserved = Math.max(0, Number(reservation.onProcess.get(groupName) || 0));
      const onProcessAvail = Math.max(0, onProcessTotal - onProcessReserved);
      const takeOnProcess = Math.min(onProcessAvail, remaining);
      if (takeOnProcess > 0) {
        reserveOnProcess.set(groupName, (reserveOnProcess.get(groupName) || 0) + takeOnProcess);
        allocs.push({ groupName, qty: takeOnProcess, source: 'on_process' });
        remaining -= takeOnProcess;
      }

      if (remaining > 0 && secondWarehouse?._id) {
        const wh2 = secondWarehouse._id;
        const secondStockDoc = await PalletGroupStock.findOne({ warehouseId: wh2, groupName }).lean();
        const secondAvail = Math.max(0, Number(secondStockDoc?.pallets || 0));
        const takeSecond = Math.min(secondAvail, remaining);
        if (takeSecond > 0) {
          deductSecond.set(groupName, (deductSecond.get(groupName) || 0) + takeSecond);
          allocs.push({ groupName, qty: takeSecond, source: 'second', warehouseId: wh2 });
          remaining -= takeSecond;
        }
      }

      if (remaining > 0) {
        shortages.push({
          lineItem: String(ln?.lineItem || '').trim(),
          groupName,
          required: need,
          available: Math.max(0, need - remaining),
        });
      }
    }

    if (shortages.length) {
      return res.status(400).json(noStocksPayload({ items: shortages }));
    }

    const primaryLines = Array.from(deductPrimary.entries()).map(([groupName, qty]) => ({ groupName, qty }));
    const secondLines = Array.from(deductSecond.entries()).map(([groupName, qty]) => ({ groupName, qty }));
    if (primaryLines.length) {
      await applyInventoryDeltaForOrder({ warehouseId: doc.warehouseId, orderNumber: doc.orderNumber, lines: primaryLines, committedBy, deltaSign: -1, allowNegative: false, reason: 'order_processing' });
    }
    if (secondLines.length && secondWarehouse?._id) {
      await applyInventoryDeltaForOrder({ warehouseId: secondWarehouse._id, orderNumber: doc.orderNumber, lines: secondLines, committedBy, deltaSign: -1, allowNegative: false, reason: 'order_processing_second_warehouse' });
    }

    await PalletGroupReservation.deleteMany({ orderNumber: String(doc.orderNumber || '').trim() });
    const reserveDocs = [];
    for (const [groupName, qty] of reserveOnWater.entries()) {
      reserveDocs.push({ orderNumber: doc.orderNumber, warehouseId: doc.warehouseId, groupName, source: 'on_water', qty, committedBy });
    }
    for (const [groupName, qty] of reserveOnProcess.entries()) {
      reserveDocs.push({ orderNumber: doc.orderNumber, warehouseId: doc.warehouseId, groupName, source: 'on_process', qty, committedBy });
    }
    if (reserveDocs.length) await PalletGroupReservation.insertMany(reserveDocs);

    doc.allocations = allocs;
  }

  // shipped/delivered/completed do not change inventory (inventory already affected on processing)
  doc.status = next;
  if (next !== 'shipped') {
    doc.estDeliveredDate = null;
  }
  await doc.save();
  res.json(doc.toObject());
};

export const getUnfulfilledOrderById = async (req, res) => {
  const { id } = req.params;

  const raw = await UnfulfilledOrder.findById(id).select('orderNumber warehouseId status allocations lastUpdatedBy').lean();
  if (!raw) return res.status(404).json({ message: 'Order not found' });

  try {
    const resolveToGroupName = (v) => String(v || '').trim();
    await rebalanceProcessingOrderAllocations({ order: { ...raw, _id: id }, resolveToGroupName });
  } catch {
    // best-effort; do not block UI
  }

  const doc = await UnfulfilledOrder.findById(id)
    .populate('warehouseId', 'name')
    .populate('allocations.warehouseId', 'name')
    .select('orderNumber warehouseId status allocations lines customerEmail customerName customerPhone createdAtOrder originalPrice discountPercent finalPrice estFulfillmentDate estDeliveredDate shippingAddress notes postActions committedBy lastUpdatedBy createdAt updatedAt')
    .lean();
  if (!doc) return res.status(404).json({ message: 'Order not found' });

  let reservedBreakdown = [];
  try {
    const orderNumber = String(doc?.orderNumber || '').trim();
    const warehouseId = String(doc?.warehouseId?._id || doc?.warehouseId || '').trim();
    if (orderNumber && warehouseId) {
      const rows = await PalletGroupReservation.aggregate([
        { $addFields: { orderNumberStr: { $toString: '$orderNumber' }, warehouseIdStr: { $toString: '$warehouseId' } } },
        { $match: { orderNumberStr: orderNumber, warehouseIdStr: warehouseId } },
        { $group: { _id: { groupName: '$groupName', source: '$source' }, qty: { $sum: '$qty' } } },
      ]);

      const byGroup = new Map();
      for (const r of rows || []) {
        const groupName = String(r?._id?.groupName || '').trim();
        const source = String(r?._id?.source || '').trim().toLowerCase();
        const qty = Math.floor(Number(r?.qty || 0));
        if (!groupName || !Number.isFinite(qty) || qty <= 0) continue;
        if (!byGroup.has(groupName)) byGroup.set(groupName, { groupName, primary: 0, onWater: 0, second: 0, onProcess: 0 });
        const rec = byGroup.get(groupName);
        if (source === 'primary') rec.primary += qty;
        else if (source === 'on_water') rec.onWater += qty;
        else if (source === 'second') rec.second += qty;
        else if (source === 'on_process') rec.onProcess += qty;
      }

      reservedBreakdown = Array.from(byGroup.values())
        .map((r) => ({ id: r.groupName, ...r }))
        .sort((a, b) => String(a.groupName).localeCompare(String(b.groupName)));
    }
  } catch {
    reservedBreakdown = [];
  }

  res.json({ ...doc, reservedBreakdown });
};

export const rebalanceProcessingOrders = async (req, res) => {
  try {
    const { warehouseId, groupNames } = req.body || {};
    const keyOf = (v) =>
      normalizeStr(v)
        .replace(/\u00a0/g, ' ')
        .replace(/\s+/g, ' ')
        .toLowerCase();
    const groups = await ItemGroup.find({ active: true }).select('name lineItem').lean();
    const byGroupKey = new Map((groups || []).map((g) => [keyOf(g?.name || ''), String(g?.name || '').trim()]));
    const byLineItemKey = new Map((groups || []).map((g) => [keyOf(g?.lineItem || ''), String(g?.name || '').trim()]));
    const resolveToGroupName = (raw) => {
      const s = String(raw || '').trim();
      if (!s) return '';
      const k = keyOf(s);
      return byGroupKey.get(k) || byLineItemKey.get(k) || s;
    };

    const filterWarehouseId = String(warehouseId || '').trim();
    const wantedGroupKeys = new Set(
      (Array.isArray(groupNames) ? groupNames : [])
        .map((g) => keyOf(resolveToGroupName(g)))
        .filter((v) => v)
    );

    const query = {};
    if (filterWarehouseId) query.warehouseId = filterWarehouseId;

    const rows = await UnfulfilledOrder.find(query).select('_id orderNumber warehouseId status allocations').lean();
    let updated = 0;
    for (const r of rows || []) {
      try {
        const status = normalizeOrderStatus(r?.status || 'processing') || 'processing';
        if (status !== 'processing' && status !== 'ready_to_ship') continue;

        if (wantedGroupKeys.size) {
          const allocs = Array.isArray(r?.allocations) ? r.allocations : [];
          let hit = false;
          for (const a of allocs) {
            const g = resolveToGroupName(a?.groupName || '');
            const k = keyOf(g);
            if (k && wantedGroupKeys.has(k)) {
              hit = true;
              break;
            }
          }
          if (!hit) continue;
        }

        const changed = await rebalanceProcessingOrderAllocations({ order: r, resolveToGroupName });
        if (changed) updated += 1;
      } catch {
        // best-effort
      }
    }
    return res.json({ ok: true, updated });
  } catch (e) {
    return res.status(500).json({ message: 'Failed to rebalance processing orders' });
  }
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
  const { status, estDeliveredDate } = req.body || {};
  const next = normalizeOrderStatus(status);
  if (!ORDER_STATUSES.includes(next)) {
    return res.status(400).json({ message: 'Invalid status' });
  }

  const doc = await UnfulfilledOrder.findById(id);
  if (!doc) return res.status(404).json({ message: 'Order not found' });

  const prev = normalizeOrderStatus(doc.status || 'processing') || 'processing';
  if (prev === 'completed') return res.status(400).json({ message: 'Completed orders are locked' });

  const committedBy = String(req.user?.username || req.user?.id || '');
  const orderLines = (doc.lines || []).map((l) => ({ groupName: l.groupName, qty: l.qty }));

  if (next === 'canceled' && prev !== 'canceled') {
    if (isInventoryConsumingStatus(prev)) {
      const allocs = Array.isArray(doc.allocations) ? doc.allocations : [];
      const primaryMap = new Map();
      const secondMap = new Map();
      for (const a of allocs) {
        const g = String(a?.groupName || '').trim();
        const qty = Math.floor(Number(a?.qty || 0));
        const src = String(a?.source || '').trim();
        if (!g || !Number.isFinite(qty) || qty <= 0) continue;
        if (src === 'second') {
          const wid = String(a?.warehouseId || '').trim();
          if (!wid) continue;
          if (!secondMap.has(wid)) secondMap.set(wid, new Map());
          secondMap.get(wid).set(g, (secondMap.get(wid).get(g) || 0) + qty);
        } else if (src === 'primary') {
          primaryMap.set(g, (primaryMap.get(g) || 0) + qty);
        }
      }

      const primaryLines = primaryMap.size
        ? Array.from(primaryMap.entries()).map(([groupName, qty]) => ({ groupName, qty }))
        : orderLines;
      if (primaryLines.length) {
        await applyInventoryDeltaForOrder({ warehouseId: doc.warehouseId, orderNumber: doc.orderNumber, lines: primaryLines, committedBy, deltaSign: +1, allowNegative: true, reason: 'order_canceled' });
      }
      for (const [wid, gmap] of secondMap.entries()) {
        const lines = Array.from(gmap.entries()).map(([groupName, qty]) => ({ groupName, qty }));
        if (!lines.length) continue;
        await applyInventoryDeltaForOrder({ warehouseId: wid, orderNumber: doc.orderNumber, lines, committedBy, deltaSign: +1, allowNegative: true, reason: 'order_canceled_second_warehouse' });
      }
    }

    // Always clear reservations on cancel (processing reservations or any leftovers)
    await PalletGroupReservation.deleteMany({ orderNumber: String(doc.orderNumber || '').trim() });
    doc.status = 'canceled';
    doc.lastUpdatedBy = committedBy;
    await doc.save();
    return res.json(doc.toObject());
  }

  if (next === 'processing' && prev === 'canceled') {
    // Re-create reservations using current availability (no physical deductions)
    const secondWarehouse = await getSecondWarehouseFor(doc.warehouseId);
    const keyOf = (v) =>
      normalizeStr(v)
        .replace(/\u00a0/g, ' ')
        .replace(/\s+/g, ' ')
        .toLowerCase();
    const groups = await ItemGroup.find({ active: true }).select('name lineItem').lean();
    const byGroupKey = new Map((groups || []).map((g) => [keyOf(g?.name || ''), String(g?.name || '').trim()]));
    const byLineItemKey = new Map((groups || []).map((g) => [keyOf(g?.lineItem || ''), String(g?.name || '').trim()]));
    const resolveToGroupName = (raw) => {
      const s = String(raw || '').trim();
      if (!s) return '';
      const k = keyOf(s);
      return byGroupKey.get(k) || byLineItemKey.get(k) || s;
    };

    await PalletGroupReservation.deleteMany({ orderNumber: String(doc.orderNumber || '').trim() });

    const reservation = await getReservationMaps({ warehouseId: doc.warehouseId });
    const onWaterMap = await buildOnWaterMapForWarehouse({ warehouseId: doc.warehouseId, resolveToGroupName });
    const onProcessMap = await buildOnProcessMap({ resolveToGroupName });

    const allocs = [];
    const reservePrimary = new Map();
    const reserveSecond = new Map();
    const reserveOnWater = new Map();
    const reserveOnProcess = new Map();

    const shortages = [];

    for (const ln of orderLines) {
      const groupName = String(ln.groupName || '').trim();
      const need = Math.floor(Number(ln.qty || 0));
      if (!groupName || !Number.isFinite(need) || need <= 0) continue;

      const primaryStockDoc = await PalletGroupStock.findOne({ warehouseId: doc.warehouseId, groupName }).lean();
      const primaryReserved = Math.max(0, Number(reservation.physicalByWarehouse?.get(String(doc.warehouseId))?.get(groupName) || 0));
      const primaryAvail = Math.max(0, Number(primaryStockDoc?.pallets || 0) - primaryReserved);
      let remaining = need;
      const takePrimary = Math.min(primaryAvail, remaining);
      if (takePrimary > 0) {
        reservePrimary.set(groupName, (reservePrimary.get(groupName) || 0) + takePrimary);
        allocs.push({ groupName, qty: takePrimary, source: 'primary' });
        remaining -= takePrimary;
      }

      const onWaterTotal = Math.max(0, Number(onWaterMap.get(groupName) || 0));
      const onWaterReserved = Math.max(0, Number(reservation.onWater.get(groupName) || 0));
      const onWaterAvail = Math.max(0, onWaterTotal - onWaterReserved);
      const takeOnWater = Math.min(onWaterAvail, remaining);
      if (takeOnWater > 0) {
        reserveOnWater.set(groupName, (reserveOnWater.get(groupName) || 0) + takeOnWater);
        allocs.push({ groupName, qty: takeOnWater, source: 'on_water' });
        remaining -= takeOnWater;
      }

      const onProcessTotal = Math.max(0, Number(onProcessMap.get(groupName) || 0));
      const onProcessReserved = Math.max(0, Number(reservation.onProcess.get(groupName) || 0));
      const onProcessAvail = Math.max(0, onProcessTotal - onProcessReserved);
      const takeOnProcess = Math.min(onProcessAvail, remaining);
      if (takeOnProcess > 0) {
        reserveOnProcess.set(groupName, (reserveOnProcess.get(groupName) || 0) + takeOnProcess);
        allocs.push({ groupName, qty: takeOnProcess, source: 'on_process' });
        remaining -= takeOnProcess;
      }

      if (remaining > 0 && secondWarehouse?._id) {
        const wh2 = secondWarehouse._id;
        const secondStockDoc = await PalletGroupStock.findOne({ warehouseId: wh2, groupName }).lean();
        const secondReserved = Math.max(0, Number(reservation.physicalByWarehouse?.get(String(wh2))?.get(groupName) || 0));
        const secondAvail = Math.max(0, Number(secondStockDoc?.pallets || 0) - secondReserved);
        const takeSecond = Math.min(secondAvail, remaining);
        if (takeSecond > 0) {
          reserveSecond.set(groupName, (reserveSecond.get(groupName) || 0) + takeSecond);
          allocs.push({ groupName, qty: takeSecond, source: 'second', warehouseId: wh2 });
          remaining -= takeSecond;
        }
      }

      if (remaining > 0) {
        shortages.push({
          lineItem: String(ln?.lineItem || '').trim(),
          groupName,
          required: need,
          available: Math.max(0, need - remaining),
        });
      }
    }

    if (shortages.length) {
      return res.status(400).json(noStocksPayload({ items: shortages }));
    }

    const reserveDocs = [];
    for (const [groupName, qty] of reservePrimary.entries()) {
      reserveDocs.push({ orderNumber: doc.orderNumber, warehouseId: doc.warehouseId, sourceWarehouseId: doc.warehouseId, groupName, source: 'primary', qty, committedBy });
    }
    for (const [groupName, qty] of reserveOnWater.entries()) {
      reserveDocs.push({ orderNumber: doc.orderNumber, warehouseId: doc.warehouseId, groupName, source: 'on_water', qty, committedBy });
    }
    for (const [groupName, qty] of reserveOnProcess.entries()) {
      reserveDocs.push({ orderNumber: doc.orderNumber, warehouseId: doc.warehouseId, groupName, source: 'on_process', qty, committedBy });
    }
    if (secondWarehouse?._id) {
      for (const [groupName, qty] of reserveSecond.entries()) {
        reserveDocs.push({ orderNumber: doc.orderNumber, warehouseId: doc.warehouseId, sourceWarehouseId: secondWarehouse._id, groupName, source: 'second', qty, committedBy });
      }
    }
    if (reserveDocs.length) await PalletGroupReservation.insertMany(reserveDocs);

    doc.allocations = allocs;
  }

  if (next === 'shipped' && (prev === 'processing' || prev === 'ready_to_ship')) {
    const d = normalizeStr(estDeliveredDate);
    if (d) {
      doc.estDeliveredDate = new Date(d);
    }
    // Deduct physical stock at the moment of shipping (based on planned allocations)
    const allocs = Array.isArray(doc.allocations) ? doc.allocations : [];
    const primaryMap = new Map();
    const secondMap = new Map();
    for (const a of allocs) {
      const g = String(a?.groupName || '').trim();
      const qty = Math.floor(Number(a?.qty || 0));
      const src = String(a?.source || '').trim();
      if (!g || !Number.isFinite(qty) || qty <= 0) continue;
      if (src === 'second') {
        const wid = String(a?.warehouseId || '').trim();
        if (!wid) continue;
        if (!secondMap.has(wid)) secondMap.set(wid, new Map());
        secondMap.get(wid).set(g, (secondMap.get(wid).get(g) || 0) + qty);
      } else if (src === 'primary') {
        primaryMap.set(g, (primaryMap.get(g) || 0) + qty);
      }
    }

    const primaryLines = primaryMap.size
      ? Array.from(primaryMap.entries()).map(([groupName, qty]) => ({ groupName, qty }))
      : [];
    if (primaryLines.length) {
      await applyInventoryDeltaForOrder({ warehouseId: doc.warehouseId, orderNumber: doc.orderNumber, lines: primaryLines, committedBy, deltaSign: -1, allowNegative: false, reason: 'order_shipped' });
    }
    for (const [wid, gmap] of secondMap.entries()) {
      const lines = Array.from(gmap.entries()).map(([groupName, qty]) => ({ groupName, qty }));
      if (!lines.length) continue;
      await applyInventoryDeltaForOrder({ warehouseId: wid, orderNumber: doc.orderNumber, lines, committedBy, deltaSign: -1, allowNegative: false, reason: 'order_shipped_second_warehouse' });
    }

    // Shipping consumes the reservation; keep monitoring clean.
    await PalletGroupReservation.deleteMany({ orderNumber: String(doc.orderNumber || '').trim() });
  }

  // shipped/delivered/completed do not change inventory (inventory already affected on processing)
  doc.status = next;
  doc.lastUpdatedBy = committedBy;
  await doc.save();
  res.json(doc.toObject());
};

export const updateUnfulfilledOrderDetails = async (req, res) => {
  const { id } = req.params;
  const { customerName, customerEmail, customerPhone, originalPrice, discountPercent, estFulfillmentDate, estDeliveredDate, shippingAddress, notes, lines } = req.body || {};

  const existing = await UnfulfilledOrder.findById(id).select('status warehouseId orderNumber lines originalPrice discountPercent').lean();
  if (!existing) return res.status(404).json({ message: 'Order not found' });
  if (normalizeOrderStatus(existing.status || '') === 'completed') return res.status(400).json({ message: 'Completed orders are locked' });

  const set = {};
  const committedBy = String(req.user?.username || req.user?.id || '');
  set.lastUpdatedBy = committedBy;
  if (customerName !== undefined) set.customerName = normalizeStr(customerName);
  if (customerEmail !== undefined) set.customerEmail = normalizeStr(customerEmail);
  if (customerPhone !== undefined) set.customerPhone = normalizeStr(customerPhone);
  if (originalPrice !== undefined) {
    const n = Number(originalPrice);
    set.originalPrice = Number.isFinite(n) ? n : null;
  }
  if (discountPercent !== undefined) {
    const n = Number(discountPercent);
    set.discountPercent = Number.isFinite(n) ? Math.min(100, Math.max(0, n)) : null;
  }
  if (shippingAddress !== undefined) set.shippingAddress = normalizeStr(shippingAddress);
  if (notes !== undefined) set.notes = normalizeStr(notes);
  if (estFulfillmentDate !== undefined) {
    const s = normalizeStr(estFulfillmentDate);
    set.estFulfillmentDate = s ? new Date(s) : null;
  }
  if (estDeliveredDate !== undefined) {
    const s = normalizeStr(estDeliveredDate);
    set.estDeliveredDate = s ? new Date(s) : null;
  }

  if (originalPrice !== undefined || discountPercent !== undefined) {
    const nextOriginal = originalPrice !== undefined ? Number(originalPrice) : Number(existing?.originalPrice);
    const nextDiscount = discountPercent !== undefined ? Number(discountPercent) : Number(existing?.discountPercent);
    const hasOriginal = Number.isFinite(nextOriginal);
    const hasDiscount = Number.isFinite(nextDiscount);
    const safeDiscount = hasDiscount ? Math.min(100, Math.max(0, nextDiscount)) : 0;
    set.finalPrice = hasOriginal ? nextOriginal * (1 - safeDiscount / 100) : null;
  }

  const prevStatus = normalizeOrderStatus(existing.status || 'processing') || 'processing';

  if (lines !== undefined) {
    if (prevStatus !== 'processing' && prevStatus !== 'ready_to_ship') return res.status(400).json({ message: 'Only processing orders can be edited' });
    if (!Array.isArray(lines)) return res.status(400).json({ message: 'lines must be an array' });

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
      const g = await ItemGroup.findOne({ name: groupName }).select('lineItem name').lean();
      parsedLines.push({
        groupName,
        lineItem: (normalizeStr(ln?.lineItem) || g?.lineItem || search).trim(),
        qty: Math.floor(qty),
      });
    }

    // De-duplicate by groupName (sum qty)
    const merged = new Map();
    for (const ln of parsedLines) {
      merged.set(ln.groupName, (merged.get(ln.groupName) || 0) + Number(ln.qty || 0));
    }
    const nextLines = Array.from(merged.entries()).map(([groupName, qty]) => {
      const sample = parsedLines.find((p) => p.groupName === groupName);
      return {
        groupName,
        lineItem: (sample?.lineItem || groupName).trim(),
        qty: Math.floor(Number(qty || 0)),
      };
    });

    // processing: reserve-only. Recompute reservations + allocations based on new lines.
    const orderNumber = String(existing.orderNumber || '').trim();
    // committedBy declared above; keep using it for reservation committedBy
    await PalletGroupReservation.deleteMany({ orderNumber });

    const secondWarehouse = await getSecondWarehouseFor(existing.warehouseId);
    const keyOf = (v) =>
      normalizeStr(v)
        .replace(/\u00a0/g, ' ')
        .replace(/\s+/g, ' ')
        .toLowerCase();
    const groups = await ItemGroup.find({ active: true }).select('name lineItem').lean();
    const byGroupKey = new Map((groups || []).map((g) => [keyOf(g?.name || ''), String(g?.name || '').trim()]));
    const byLineItemKey = new Map((groups || []).map((g) => [keyOf(g?.lineItem || ''), String(g?.name || '').trim()]));
    const resolveToGroupName = (raw) => {
      const s = String(raw || '').trim();
      if (!s) return '';
      const k = keyOf(s);
      return byGroupKey.get(k) || byLineItemKey.get(k) || s;
    };

    const reservation = await getReservationMaps({ warehouseId: existing.warehouseId });
    const onWaterMap = await buildOnWaterMapForWarehouse({ warehouseId: existing.warehouseId, resolveToGroupName });
    const onProcessMap = await buildOnProcessMap({ resolveToGroupName });

    const allocations = [];
    const reservePrimary = new Map();
    const reserveSecond = new Map();
    const reserveOnWater = new Map();
    const reserveOnProcess = new Map();

    const shortages = [];

    for (const ln of nextLines) {
      const groupName = String(ln.groupName || '').trim();
      const need = Math.floor(Number(ln.qty || 0));
      if (!groupName || !Number.isFinite(need) || need <= 0) continue;

      const primaryStockDoc = await PalletGroupStock.findOne({ warehouseId: existing.warehouseId, groupName }).lean();
      const primaryReserved = Math.max(0, Number(reservation.physicalByWarehouse?.get(String(existing.warehouseId))?.get(groupName) || 0));
      const primaryAvail = Math.max(0, Number(primaryStockDoc?.pallets || 0) - primaryReserved);
      let remaining = need;
      const takePrimary = Math.min(primaryAvail, remaining);
      if (takePrimary > 0) {
        reservePrimary.set(groupName, (reservePrimary.get(groupName) || 0) + takePrimary);
        allocations.push({ groupName, qty: takePrimary, source: 'primary' });
        remaining -= takePrimary;
      }

      const onWaterTotal = Math.max(0, Number(onWaterMap.get(groupName) || 0));
      const onWaterReserved = Math.max(0, Number(reservation.onWater.get(groupName) || 0));
      const onWaterAvail = Math.max(0, onWaterTotal - onWaterReserved);
      const takeOnWater = Math.min(onWaterAvail, remaining);
      if (takeOnWater > 0) {
        reserveOnWater.set(groupName, (reserveOnWater.get(groupName) || 0) + takeOnWater);
        allocations.push({ groupName, qty: takeOnWater, source: 'on_water' });
        remaining -= takeOnWater;
      }

      const onProcessTotal = Math.max(0, Number(onProcessMap.get(groupName) || 0));
      const onProcessReserved = Math.max(0, Number(reservation.onProcess.get(groupName) || 0));
      const onProcessAvail = Math.max(0, onProcessTotal - onProcessReserved);
      const takeOnProcess = Math.min(onProcessAvail, remaining);
      if (takeOnProcess > 0) {
        reserveOnProcess.set(groupName, (reserveOnProcess.get(groupName) || 0) + takeOnProcess);
        allocations.push({ groupName, qty: takeOnProcess, source: 'on_process' });
        remaining -= takeOnProcess;
      }

      if (remaining > 0 && secondWarehouse?._id) {
        const wh2 = secondWarehouse._id;
        const secondStockDoc = await PalletGroupStock.findOne({ warehouseId: wh2, groupName }).lean();
        const secondReserved = Math.max(0, Number(reservation.physicalByWarehouse?.get(String(wh2))?.get(groupName) || 0));
        const secondAvail = Math.max(0, Number(secondStockDoc?.pallets || 0) - secondReserved);
        const takeSecond = Math.min(secondAvail, remaining);
        if (takeSecond > 0) {
          reserveSecond.set(groupName, (reserveSecond.get(groupName) || 0) + takeSecond);
          allocations.push({ groupName, qty: takeSecond, source: 'second', warehouseId: wh2 });
          remaining -= takeSecond;
        }
      }

      if (remaining > 0) {
        shortages.push({
          lineItem: String(ln?.lineItem || '').trim(),
          groupName,
          required: need,
          available: Math.max(0, need - remaining),
        });
      }
    }

    if (shortages.length) {
      return res.status(400).json(noStocksPayload({ items: shortages }));
    }

    const reserveDocs = [];
    for (const [groupName, qty] of reservePrimary.entries()) {
      reserveDocs.push({ orderNumber, warehouseId: existing.warehouseId, sourceWarehouseId: existing.warehouseId, groupName, source: 'primary', qty, committedBy });
    }
    for (const [groupName, qty] of reserveOnWater.entries()) {
      reserveDocs.push({ orderNumber, warehouseId: existing.warehouseId, groupName, source: 'on_water', qty, committedBy });
    }
    for (const [groupName, qty] of reserveOnProcess.entries()) {
      reserveDocs.push({ orderNumber, warehouseId: existing.warehouseId, groupName, source: 'on_process', qty, committedBy });
    }
    if (secondWarehouse?._id) {
      for (const [groupName, qty] of reserveSecond.entries()) {
        reserveDocs.push({ orderNumber, warehouseId: existing.warehouseId, sourceWarehouseId: secondWarehouse._id, groupName, source: 'second', qty, committedBy });
      }
    }
    if (reserveDocs.length) await PalletGroupReservation.insertMany(reserveDocs);
    set.allocations = allocations;

    set.lines = nextLines;

    const fullyPrimary = reserveSecond.size === 0 && reserveOnWater.size === 0 && reserveOnProcess.size === 0;
    set.status = fullyPrimary ? 'ready_to_ship' : 'processing';

    // Only allow setting Estimated Order Delivered when SHIPPED; otherwise clear it
    if (set.status !== 'shipped') {
      set.estDeliveredDate = null;
    }
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
