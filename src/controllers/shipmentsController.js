import Shipment from '../models/Shipment.js';
import OnProcessPallet from '../models/OnProcessPallet.js';
import ItemGroup from '../models/ItemGroup.js';
import PalletGroupStock from '../models/PalletGroupStock.js';
import PalletGroupTxn from '../models/PalletGroupTxn.js';
import PalletGroupReservation from '../models/PalletGroupReservation.js';
import UnfulfilledOrder from '../models/UnfulfilledOrder.js';
import WarehouseStock from '../models/WarehouseStock.js';
import Item from '../models/Item.js';
import StockMovement from '../models/StockMovement.js';
import ImportLog from '../models/ImportLog.js';
import Counter from '../models/Counter.js';

export const listShipments = async (req, res) => {
  const { status } = req.query;
  const q = {};
  if (status) q.status = status;

  const escapeRegExp = (s) => String(s || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const toWsRegex = (s) => escapeRegExp(String(s || '').trim().replace(/\s+/g, ' ')).replace(/ /g, '\\s+');

  const legacyQ = (req.query.q || '').toString().trim();
  const qRef = (req.query.qRef || '').toString().trim();
  const qPallet = (req.query.qPallet || '').toString().trim();
  const eddFromRaw = (req.query.eddFrom || '').toString().trim();
  const eddToRaw = (req.query.eddTo || '').toString().trim();

  const and = [];

  const refText = qRef || legacyQ;
  if (refText) {
    const regex = { $regex: toWsRegex(refText), $options: 'i' };
    and.push({
      $or: [
        { owNumber: regex },
        { reference: regex }
      ]
    });
  }

  if (qPallet) {
    const regex = { $regex: toWsRegex(qPallet), $options: 'i' };
    and.push({ $or: [{ notes: regex }, { 'items.itemCode': regex }] });
  } else if (legacyQ && !qRef) {
    // legacy behavior: q also searches pallet/notes + item codes when qRef isn't used
    const regex = { $regex: toWsRegex(legacyQ), $options: 'i' };
    and.push({ $or: [{ notes: regex }, { 'items.itemCode': regex }] });
  }

  if (eddFromRaw || eddToRaw) {
    const range = {};
    if (eddFromRaw) {
      const d = new Date(`${eddFromRaw}T00:00:00`);
      if (!isNaN(d.getTime())) range.$gte = d;
    }
    if (eddToRaw) {
      const d = new Date(`${eddToRaw}T23:59:59`);
      if (!isNaN(d.getTime())) range.$lte = d;
    }
    if (Object.keys(range).length) {
      and.push({ estDeliveryDate: range });
    }
  }

  if (and.length) q.$and = and;
  const page = Math.max(0, parseInt(req.query.page?.toString() || '0', 10));
  const pageSize = Math.max(1, Math.min(200, parseInt(req.query.pageSize?.toString() || '20', 10)));
  const total = await Shipment.countDocuments(q);
  const items = await Shipment.find(q)
    .sort({ createdAt: -1 })
    .skip(page * pageSize)
    .limit(pageSize)
    .populate('warehouseId', 'name')
    .populate('sourceWarehouseId', 'name')
    .lean();
  res.json({ items, total, page, pageSize });
};

export const dueToday = async (req, res) => {
  try {
    const now = new Date();
    const y = now.getFullYear();
    const m = now.getMonth();
    const d = now.getDate();
    const start = new Date(y, m, d, 0, 0, 0, 0);
    const end = new Date(y, m, d, 23, 59, 59, 999);
    const docs = await Shipment.find({ status: 'on_water', estDeliveryDate: { $gte: start, $lte: end } }).sort({ estDeliveryDate: 1 }).lean();
    res.json({ count: docs.length, items: docs });
  } catch (e) {
    console.error('shipments.dueToday failed', e);
    res.status(500).json({ message: 'Failed to load shipments due today' });
  }
};

export const getShipmentById = async (req, res) => {
  const { id } = req.params;
  const doc = await Shipment.findById(id)
    .populate('warehouseId', 'name')
    .populate('sourceWarehouseId', 'name')
    .lean();
  if (!doc) return res.status(404).json({ message: 'shipment not found' });
  res.json(doc);
};

export const createTransfer = async (req, res) => {
  const { sourceWarehouseId, warehouseId, items = [], reference, estDeliveryDate, notes } = req.body || {};
  if (!sourceWarehouseId) return res.status(400).json({ message: 'sourceWarehouseId required' });
  if (!warehouseId) return res.status(400).json({ message: 'warehouseId required' });
  if (!Array.isArray(items) || items.length === 0) return res.status(400).json({ message: 'items required' });

  // decrement stock from source warehouse immediately
  for (const it of items) {
    const { itemCode, qtyPieces } = it || {};
    if (!itemCode || !Number.isFinite(qtyPieces) || qtyPieces <= 0) continue;
    const src = await WarehouseStock.findOne({ itemCode, warehouseId: sourceWarehouseId });
    const available = src?.qtyPieces || 0;
    if (available < qtyPieces) {
      return res.status(400).json({ message: `insufficient stock for ${itemCode} in source warehouse` });
    }
  }
  for (const it of items) {
    const { itemCode, qtyPieces } = it || {};
    await WarehouseStock.findOneAndUpdate(
      { itemCode, warehouseId: sourceWarehouseId },
      { $inc: { qtyPieces: -qtyPieces } },
      { new: true }
    );
    // decrement global available qty while on-water
    const itemDoc = await Item.findOne({ itemCode });
    if (itemDoc) {
      itemDoc.totalQty = Math.max(0, (itemDoc.totalQty || 0) - (qtyPieces || 0));
      await itemDoc.save();
    }
  }

  // generate transfer reference if not provided e.g., TR-000001
  let ref = (reference || '').trim();
  if (!ref) {
    const ctr = await Counter.findOneAndUpdate(
      { name: 'transfer' },
      { $inc: { seq: 1 } },
      { upsert: true, new: true, setDefaultsOnInsert: true }
    );
    const num = String(ctr.seq).padStart(6, '0');
    ref = `TR-${num}`;
  }

  const doc = await Shipment.create({
    kind: 'transfer',
    status: 'on_water',
    sourceWarehouseId,
    warehouseId,
    estDeliveryDate: estDeliveryDate ? new Date(estDeliveryDate) : undefined,
    items: items.map((i)=>({ itemCode: i.itemCode, qtyPieces: i.qtyPieces, packSize: i.packSize })),
    reference: ref,
    notes: notes || '',
    createdBy: req.user?.id,
  });

  // create stock movement OUT at source warehouse
  const outItems = items.map(i=>({ itemCode: i.itemCode, qtyPieces: i.qtyPieces, packSize: i.packSize }));
  if (outItems.length) {
    await StockMovement.create({
      type: 'OUT',
      reference: `TRANS - ${ref}`,
      items: outItems,
      createdBy: req.user?.id,
      warehouseId: sourceWarehouseId,
      notes: 'transfer|on-water'
    });
  }

  res.status(201).json(doc);
};

export const createPalletTransfer = async (req, res) => {
  const { sourceWarehouseId, warehouseId, pallets = [], reference, estDeliveryDate } = req.body || {};
  if (!sourceWarehouseId) return res.status(400).json({ message: 'sourceWarehouseId required' });
  if (!warehouseId) return res.status(400).json({ message: 'warehouseId required' });
  if (sourceWarehouseId === warehouseId) return res.status(400).json({ message: 'Source and destination must be different' });
  const poNumber = String(reference || '').trim();
  if (!poNumber) return res.status(400).json({ message: 'PO# (reference) is required' });
  if (!Array.isArray(pallets) || pallets.length === 0) return res.status(400).json({ message: 'pallets required' });

  const committedBy = String(req.user?.username || req.user?.id || '').trim();
  const migrateSecondToOnWater = async ({ destWarehouseId, srcWarehouseId, groupName, pallets }) => {
    const dest = String(destWarehouseId || '').trim();
    const src = String(srcWarehouseId || '').trim();
    const g = String(groupName || '').trim();
    let remaining = Math.max(0, Math.floor(Number(pallets || 0)));
    if (!dest || !src || !g || !remaining) return;

    const toMove = await PalletGroupReservation.find({
      warehouseId: dest,
      sourceWarehouseId: src,
      groupName: g,
      source: 'second',
      qty: { $gt: 0 },
    })
      .sort({ createdAt: 1 })
      .lean();

    for (const r of toMove) {
      if (remaining <= 0) break;
      const have = Math.max(0, Math.floor(Number(r?.qty || 0)));
      if (!have) continue;
      const take = Math.min(have, remaining);
      if (take <= 0) continue;

      const orderNumber = String(r?.orderNumber || '').trim();

      const left = have - take;
      if (left <= 0) {
        await PalletGroupReservation.deleteOne({ _id: r._id });
      } else {
        await PalletGroupReservation.updateOne({ _id: r._id }, { $set: { qty: left } });
      }

      await PalletGroupReservation.findOneAndUpdate(
        {
          orderNumber,
          warehouseId: dest,
          groupName: g,
          source: 'on_water',
        },
        {
          $inc: { qty: take },
          $setOnInsert: { committedBy: committedBy || String(r?.committedBy || '') },
        },
        { upsert: true, new: true, setDefaultsOnInsert: true }
      );

      // Best-effort: keep allocations in sync so shipdate suggestion and UI sources match.
      try {
        const ord = await UnfulfilledOrder.findOne({ orderNumber }).select('status allocations warehouseId').lean();
        const st = String(ord?.status || '').trim().toLowerCase();
        const ordWid = String(ord?.warehouseId || '').trim();
        if (st === 'processing' && ordWid === dest) {
          const allocs = Array.isArray(ord?.allocations) ? ord.allocations.map((a) => ({ ...a })) : [];

          let dec = take;
          for (const a of allocs) {
            if (dec <= 0) break;
            if (String(a?.groupName || '').trim() !== g) continue;
            if (String(a?.source || '').trim() !== 'second') continue;
            if (String(a?.warehouseId || '').trim() !== src) continue;
            const q = Math.floor(Number(a?.qty || 0));
            if (!Number.isFinite(q) || q <= 0) continue;
            const t2 = Math.min(q, dec);
            a.qty = q - t2;
            dec -= t2;
          }

          const ow = allocs.find((a) => String(a?.groupName || '').trim() === g && String(a?.source || '').trim() === 'on_water');
          if (ow) ow.qty = Math.floor(Number(ow.qty || 0) + take);
          else allocs.push({ groupName: g, qty: take, source: 'on_water' });

          const cleaned = allocs
            .map((a) => ({ ...a, qty: Math.floor(Number(a?.qty || 0)) }))
            .filter((a) => String(a?.groupName || '').trim() && Number.isFinite(a?.qty) && a.qty > 0);
          await UnfulfilledOrder.updateOne({ orderNumber }, { $set: { allocations: cleaned } });
        }
      } catch {
        // ignore
      }

      remaining -= take;
    }
  };

  // validate availability
  const cleaned = pallets
    .map((p) => ({ groupName: String(p.groupName || '').trim(), pallets: Number(p.pallets) }))
    .filter((p) => p.groupName && Number.isFinite(p.pallets) && p.pallets > 0);
  if (!cleaned.length) return res.status(400).json({ message: 'No valid pallet rows (groupName, pallets)' });

  for (const p of cleaned) {
    const src = await PalletGroupStock.findOne({ groupName: p.groupName, warehouseId: sourceWarehouseId });
    const available = Number(src?.pallets || 0);
    if (available < p.pallets) {
      return res.status(400).json({ message: `insufficient pallet stock for ${p.groupName} in source warehouse` });
    }
  }

  // decrement source pallet stock and log txn OUT
  const now = new Date();
  for (const p of cleaned) {
    await PalletGroupStock.findOneAndUpdate(
      { groupName: p.groupName, warehouseId: sourceWarehouseId },
      { $inc: { pallets: -p.pallets } },
      { new: true }
    );
    await PalletGroupTxn.create({
      poNumber,
      groupName: p.groupName,
      warehouseId: sourceWarehouseId,
      palletsDelta: -Math.abs(p.pallets),
      status: 'On-Water',
      wasOnWater: true,
      estDeliveryDate: estDeliveryDate ? new Date(estDeliveryDate) : undefined,
      committedAt: now,
      committedBy: 'transfer',
    });

    // If these pallets were reserved from a 2nd warehouse for PROCESSING orders, move reservations second -> on_water FIFO.
    await migrateSecondToOnWater({ destWarehouseId: warehouseId, srcWarehouseId: sourceWarehouseId, groupName: p.groupName, pallets: p.pallets });
  }

  // store pallet list in notes for export + delivery processing
  const noteSegments = cleaned.map(p => `pallet-group:${p.groupName}; pallets:${p.pallets}`);
  const notes = noteSegments.join(' | ');

  const doc = await Shipment.create({
    kind: 'transfer',
    status: 'on_water',
    sourceWarehouseId,
    warehouseId,
    estDeliveryDate: estDeliveryDate ? new Date(estDeliveryDate) : undefined,
    items: [],
    reference: poNumber,
    notes,
    createdBy: req.user?.id,
  });

  res.status(201).json(doc);
};

export const deliverShipment = async (req, res) => {
  const { id } = req.params;
  const ship = await Shipment.findById(id);
  if (!ship) return res.status(404).json({ message: 'shipment not found' });
  if (ship.status === 'delivered' || ship.status === 'transferred') return res.status(400).json({ message: 'already delivered' });
  // Server-side guard: EDD must exist and cannot be in the future
  if (!ship.estDeliveryDate) {
    return res.status(400).json({ message: 'EDD is required before delivery' });
  }

  const today = new Date();
  const startOfToday = new Date(today.getFullYear(), today.getMonth(), today.getDate());
  const edd = new Date(ship.estDeliveryDate);
  const eddDateOnly = new Date(edd.getFullYear(), edd.getMonth(), edd.getDate());
  if (eddDateOnly.getTime() > startOfToday.getTime()) {
    return res.status(400).json({ message: 'EDD cannot be in the future' });
  }

  const committedBy = String(req.user?.username || req.user?.id || '').trim();

  const keyOf = (v) =>
    String(v || '')
      .replace(/\u00a0/g, ' ')
      .replace(/\s+/g, ' ')
      .trim()
      .toLowerCase();

  let resolveToGroupName = (raw) => String(raw || '').trim();
  try {
    const groups = await ItemGroup.find({ active: true }).select('name lineItem').lean();
    const byGroupKey = new Map((groups || []).map((g) => [keyOf(g?.name || ''), String(g?.name || '').trim()]));
    const byLineItemKey = new Map((groups || []).map((g) => [keyOf(g?.lineItem || ''), String(g?.name || '').trim()]));
    resolveToGroupName = (raw) => {
      const s = String(raw || '').trim();
      if (!s) return '';
      const k = keyOf(s);
      return byGroupKey.get(k) || byLineItemKey.get(k) || s;
    };
  } catch {
    // best-effort
  }

  const migrateOnWaterToPrimary = async ({ warehouseId, groupName, pallets }) => {
    const wid = String(warehouseId || '').trim();
    const g = String(groupName || '').trim();
    let remaining = Math.max(0, Math.floor(Number(pallets || 0)));
    if (!wid || !g || !remaining) return;

    const toMove = await PalletGroupReservation.find({
      warehouseId: wid,
      groupName: g,
      source: 'on_water',
      qty: { $gt: 0 },
    })
      .sort({ createdAt: 1 })
      .lean();

    for (const r of toMove) {
      if (remaining <= 0) break;
      const have = Math.max(0, Math.floor(Number(r?.qty || 0)));
      if (!have) continue;
      const take = Math.min(have, remaining);
      if (take <= 0) continue;

      const left = have - take;
      if (left <= 0) {
        await PalletGroupReservation.deleteOne({ _id: r._id });
      } else {
        await PalletGroupReservation.updateOne({ _id: r._id }, { $set: { qty: left } });
      }

      await PalletGroupReservation.findOneAndUpdate(
        {
          orderNumber: String(r?.orderNumber || '').trim(),
          warehouseId: wid,
          sourceWarehouseId: wid,
          groupName: g,
          source: 'primary',
        },
        {
          $inc: { qty: take },
          $setOnInsert: { committedBy: committedBy || String(r?.committedBy || '') },
        },
        { upsert: true, new: true, setDefaultsOnInsert: true }
      );

      remaining -= take;
    }
  };

  // apply to destination warehouse (legacy item transfers + imports)
  for (const it of ship.items) {
    const { itemCode, qtyPieces } = it;
    await WarehouseStock.findOneAndUpdate(
      { itemCode, warehouseId: ship.warehouseId },
      { $inc: { qtyPieces } },
      { upsert: true, new: true, setDefaultsOnInsert: true }
    );
  }

  // if it's an import, also increase global Item.totalQty and create IN movement
  if (ship.kind === 'import') {
    const movements = [];
    for (const it of ship.items) {
      const { itemCode, qtyPieces, packSize } = it;
      const item = await Item.findOne({ itemCode });
      if (item) {
        item.totalQty = (item.totalQty || 0) + (qtyPieces || 0);
        await item.save();
      }
      movements.push({ itemCode, qtyPieces, packSize });
    }
    if (movements.length) {
      await StockMovement.create({
        type: 'IN',
        reference: ship.reference || id,
        items: movements,
        createdBy: req.user?.id,
        notes: 'on-water|Delivered'
      });
    }

    // If this import shipment originated from pallet inventory imports, the notes contain segments like:
    // "pallet-group:<groupName>; pallets:<count>" possibly separated by " | ". Update pallet stocks and txns.
    const noteText = ship.notes || '';
    const re = /pallet-group:([^;|]+);\s*pallets:(\d+)/g;
    let match;
    const now = new Date();
    while ((match = re.exec(noteText)) !== null) {
      const groupName = resolveToGroupName(String(match[1] || '').trim());
      const pallets = Number(match[2] || 0);
      if (!groupName || !Number.isFinite(pallets) || pallets <= 0) continue;
      // increment pallet stock for this warehouse
      await PalletGroupStock.findOneAndUpdate(
        { groupName, warehouseId: ship.warehouseId },
        { $inc: { pallets } },
        { upsert: true, new: true, setDefaultsOnInsert: true }
      );
      // record a Delivered transaction for this delivered segment (one entry per delivery)
      await PalletGroupTxn.create({
        poNumber: ship.reference,
        groupName,
        warehouseId: ship.warehouseId,
        palletsDelta: pallets,
        status: 'Delivered',
        wasOnWater: true,
        committedAt: now,
      });
      await migrateOnWaterToPrimary({ warehouseId: ship.warehouseId, groupName, pallets });
      // lock matching on-process pallets for this PO/group that are fully completed
      await OnProcessPallet.updateMany(
        { poNumber: ship.reference, groupName, status: 'completed', locked: { $ne: true } },
        { $set: { locked: true } }
      );
    }
  } else if (ship.kind === 'transfer') {
    const noteText = ship.notes || '';
    const hasPalletSegments = /pallet-group:/i.test(noteText);
    if (hasPalletSegments) {
      // pallet transfer delivery: increment pallet stock at destination + log txn IN
      const re = /pallet-group:([^;|]+);\s*pallets:(\d+)/g;
      let match;
      const now = new Date();
      while ((match = re.exec(noteText)) !== null) {
        const groupName = resolveToGroupName(String(match[1] || '').trim());
        const pallets = Number(match[2] || 0);
        if (!groupName || !Number.isFinite(pallets) || pallets <= 0) continue;
        await PalletGroupStock.findOneAndUpdate(
          { groupName, warehouseId: ship.warehouseId },
          { $inc: { pallets } },
          { upsert: true, new: true, setDefaultsOnInsert: true }
        );
        await PalletGroupTxn.create({
          poNumber: ship.reference || '',
          groupName,
          warehouseId: ship.warehouseId,
          palletsDelta: Math.abs(pallets),
          status: 'Delivered',
          wasOnWater: true,
          committedAt: now,
          committedBy: 'transfer',
        });
        await migrateOnWaterToPrimary({ warehouseId: ship.warehouseId, groupName, pallets });
      }
    } else {
      // legacy item-stock transfer behavior
      const movements = ship.items.map(({ itemCode, qtyPieces, packSize }) => ({ itemCode, qtyPieces, packSize }));
      if (movements.length) {
        await StockMovement.create({
          type: 'IN',
          reference: `TRANS - ${ship.reference || id}`,
          items: movements,
          createdBy: req.user?.id,
          warehouseId: ship.warehouseId,
          notes: 'on-water|Transfered'
        });
      }
      // increment global available qty back on arrival
      for (const it of ship.items) {
        const { itemCode, qtyPieces } = it;
        const itemDoc = await Item.findOne({ itemCode });
        if (itemDoc) {
          itemDoc.totalQty = (itemDoc.totalQty || 0) + (qtyPieces || 0);
          await itemDoc.save();
        }
      }
    }
  }

  // For pallet transfers, treat completion as delivered; keep legacy item transfers as transferred.
  const isPalletTransfer = ship.kind === 'transfer' && /pallet-group:/i.test(String(ship.notes || ''));
  ship.status = ship.kind === 'transfer' ? (isPalletTransfer ? 'delivered' : 'transferred') : 'delivered';
  await ship.save();
  res.json({ message: 'delivered', shipment: ship });
};

export const updateEDD = async (req, res) => {
  const { id } = req.params;
  const { estDeliveryDate } = req.body || {};
  const ship = await Shipment.findById(id);
  if (!ship) return res.status(404).json({ message: 'shipment not found' });
  if (!estDeliveryDate) return res.status(400).json({ message: 'estDeliveryDate is required' });
  const d = new Date(estDeliveryDate);
  if (isNaN(d.getTime())) return res.status(400).json({ message: 'estDeliveryDate is invalid' });
  ship.estDeliveryDate = d;
  await ship.save();
  res.json(ship);
};

// Backfill missing shipment.reference using PO# from ImportLog
export const backfillReferences = async (req, res) => {
  const q = { status: 'on_water', $or: [{ reference: { $exists: false } }, { reference: '' }] };
  const ships = await Shipment.find(q).lean();
  let updated = 0;
  for (const s of ships) {
    try {
      const codes = Array.from(new Set((s.items || []).map(i => i.itemCode).filter(Boolean)));
      if (!codes.length) continue;
      const logs = await ImportLog.aggregate([
        { $match: { itemCode: { $in: codes } } },
        { $group: { _id: '$poNumber', count: { $sum: 1 } } },
        { $sort: { count: -1 } },
        { $limit: 1 }
      ]);
      const bestPo = (logs && logs[0] && logs[0]._id) || '';
      if (!bestPo) continue;
      await Shipment.updateOne({ _id: s._id }, { $set: { reference: bestPo } });
      updated += 1;
    } catch (e) {
      // skip this shipment on error
    }
  }
  res.json({ message: 'backfill complete', updated, totalCandidates: ships.length });
};

export const backfillOwNumbers = async (req, res) => {
  const q = { $or: [{ owNumber: { $exists: false } }, { owNumber: '' }] };
  const ships = await Shipment.find(q).sort({ createdAt: 1 }).lean();
  let updated = 0;
  for (const s of ships) {
    try {
      const ctr = await Counter.findOneAndUpdate(
        { name: 'shipment_ow' },
        { $inc: { seq: 1 } },
        { upsert: true, new: true, setDefaultsOnInsert: true }
      );
      const num = String(ctr.seq).padStart(4, '0');
      const owNumber = `OW-${num}`;
      await Shipment.updateOne({ _id: s._id }, { $set: { owNumber } });
      updated += 1;
    } catch (e) {
      // skip this shipment on error
    }
  }
  res.json({ message: 'backfill owNumber complete', updated, totalCandidates: ships.length });
};
