import * as XLSX from 'xlsx';
import mongoose from 'mongoose';
import PalletGroupStock from '../models/PalletGroupStock.js';
import PalletGroupTxn from '../models/PalletGroupTxn.js';
import Warehouse from '../models/Warehouse.js';
import ItemGroup from '../models/ItemGroup.js';
import Item from '../models/Item.js';
import Shipment from '../models/Shipment.js';

const normalizeStr = (v) => (v == null ? '' : String(v)).trim();

export const importPreview = async (req, res) => {
  const { warehouseId } = req.query;
  if (!req.file) return res.status(400).json({ message: 'file required' });
  if (!warehouseId) return res.status(400).json({ message: 'warehouseId required' });
  const wb = XLSX.read(req.file.buffer, { type: 'buffer' });
  const ws = wb.Sheets[wb.SheetNames[0]];
  const rawRows = XLSX.utils.sheet_to_json(ws, { header: 1, defval: '' });
  if (!rawRows.length) return res.status(400).json({ message: 'Empty worksheet' });

  const expectedHeader = ['PO #', 'Pallet Name', 'Total Pallet'];
  const normHeader = (h) => String(h || '').trim().toLowerCase();
  const receivedHeader = Array.isArray(rawRows[0]) ? rawRows[0].map(normHeader) : [];
  const expectedHeaderNorm = expectedHeader.map(normHeader);
  const headerMatches = receivedHeader.length === expectedHeaderNorm.length
    && expectedHeaderNorm.every((h, i) => receivedHeader[i] === h);
  if (!headerMatches) {
    return res.status(400).json({
      message: 'Invalid template. Column headers must match the template exactly.',
      expectedHeader,
      receivedHeader: Array.isArray(rawRows[0]) ? rawRows[0].map((h) => String(h ?? '')) : []
    });
  }

  const rows = XLSX.utils.sheet_to_json(ws, { defval: '' });
  let totalRows = rows.length;
  const errors = [];
  const seen = new Set();
  const parsed = [];

  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    const rowNum = i + 2; // header row is 1
    const poNumber = normalizeStr(r['PO #']);
    const palletName = normalizeStr(r['Pallet Name'] ?? r['Pallet name'] ?? r['Pallet']);
    const pallets = Number(r['Total Pallet']);
    const rowErrors = [];
    if (!poNumber) rowErrors.push('PO # required');
    if (!palletName) rowErrors.push('Pallet Name required');
    if (!Number.isFinite(pallets) || pallets <= 0) rowErrors.push('Total Pallet must be > 0');
    const key = `${poNumber}||${palletName}`;
    if (seen.has(key)) rowErrors.push('Duplicate PO # + Pallet Name');
    seen.add(key);
    // Validate pallet group registered and active (Pallet Name is globally unique)
    const escapeRegex = (s) => String(s || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const grp = palletName
      ? await ItemGroup.findOne({ palletName: new RegExp(`^${escapeRegex(palletName)}$`, 'i'), active: true }).select('name').lean()
      : null;
    const groupName = grp?.name ? String(grp.name) : '';
    if (palletName && !grp) rowErrors.push('Pallet Name not registered or inactive');
    // Check duplicate existing transaction for same PO# + Group + Warehouse
    if (poNumber && groupName && warehouseId) {
      const exists = await PalletGroupTxn.findOne({ poNumber, groupName, warehouseId }).lean();
      if (exists) rowErrors.push('Duplicate: PO # + Pallet Name already imported for this Warehouse');
    }
    // Also block duplicates across ANY warehouse as requested
    if (poNumber && groupName) {
      const existsAny = await PalletGroupTxn.findOne({ poNumber, groupName }).lean();
      if (existsAny) rowErrors.push('Duplicate: PO # + Pallet Name already imported (any warehouse)');
    }
    if (rowErrors.length) {
      errors.push({ rowNum, errors: rowErrors });
      continue;
    }
    parsed.push({ poNumber, palletName, groupName, pallets });
  }

  res.json({ totalRows, errorCount: errors.length, errors, duplicateCount: totalRows - parsed.length, rows: parsed });
};

export const importCommit = async (req, res) => {
  const { status, warehouseId, estDeliveryDate } = req.query;
  if (!req.file) return res.status(400).json({ message: 'file required' });
  if (!['Delivered', 'On-Water'].includes(String(status))) return res.status(400).json({ message: 'invalid status' });
  if (!warehouseId) return res.status(400).json({ message: 'warehouseId required' });
  const wh = await Warehouse.findById(warehouseId);
  if (!wh) return res.status(400).json({ message: 'warehouse not found' });

  const wb = XLSX.read(req.file.buffer, { type: 'buffer' });
  const ws = wb.Sheets[wb.SheetNames[0]];
  const rawRows = XLSX.utils.sheet_to_json(ws, { header: 1, defval: '' });
  if (!rawRows.length) return res.status(400).json({ message: 'Empty worksheet' });

  const expectedHeader = ['PO #', 'Pallet Name', 'Total Pallet'];
  const normHeader = (h) => String(h || '').trim().toLowerCase();
  const receivedHeader = Array.isArray(rawRows[0]) ? rawRows[0].map(normHeader) : [];
  const expectedHeaderNorm = expectedHeader.map(normHeader);
  const headerMatches = receivedHeader.length === expectedHeaderNorm.length
    && expectedHeaderNorm.every((h, i) => receivedHeader[i] === h);
  if (!headerMatches) {
    return res.status(400).json({
      message: 'Invalid template. Column headers must match the template exactly.',
      expectedHeader,
      receivedHeader: Array.isArray(rawRows[0]) ? rawRows[0].map((h) => String(h ?? '')) : []
    });
  }

  const rows = XLSX.utils.sheet_to_json(ws, { defval: '' });
  const errors = [];
  let created = 0;

  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    const rowNum = i + 2;
    const poNumber = normalizeStr(r['PO #']);
    const palletName = normalizeStr(r['Pallet Name'] ?? r['Pallet name'] ?? r['Pallet']);
    const pallets = Number(r['Total Pallet']);
    const rowErrors = [];
    if (!poNumber) rowErrors.push('PO # required');
    if (!palletName) rowErrors.push('Pallet Name required');
    if (!Number.isFinite(pallets) || pallets <= 0) rowErrors.push('Total Pallet must be > 0');
    if (rowErrors.length) { errors.push({ rowNum, errors: rowErrors }); continue; }

    const escapeRegex = (s) => String(s || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const grp = palletName
      ? await ItemGroup.findOne({ palletName: new RegExp(`^${escapeRegex(palletName)}$`, 'i'), active: true }).select('name').lean()
      : null;
    const groupName = grp?.name ? String(grp.name) : '';
    if (palletName && !grp) { errors.push({ rowNum, errors: ['Pallet Name not registered or inactive'] }); continue; }

    try {
      const exists = await PalletGroupTxn.findOne({ poNumber, groupName, warehouseId });
      if (exists) { errors.push({ rowNum, errors: ['Duplicate entry for same PO # + Pallet Name + Warehouse'] }); continue; }
      const existsAny = await PalletGroupTxn.findOne({ poNumber, groupName });
      if (existsAny) { errors.push({ rowNum, errors: ['Duplicate entry for same PO # + Pallet Name (any warehouse)'] }); continue; }

      await PalletGroupTxn.create({
        poNumber,
        groupName,
        warehouseId,
        palletsDelta: pallets,
        status,
        wasOnWater: status === 'On-Water',
        committedBy: 'existing_inventory',
        estDeliveryDate: estDeliveryDate ? new Date(estDeliveryDate) : undefined,
      });
      created += 1;

      if (status === 'Delivered') {
        await PalletGroupStock.findOneAndUpdate(
          { groupName, warehouseId },
          { $inc: { pallets } },
          { new: true, upsert: true }
        );
      } else if (status === 'On-Water') {
        // Upsert a single on-water import shipment per PO# + warehouse
        const query = { kind: 'import', status: 'on_water', warehouseId, reference: poNumber };
        const existing = await Shipment.findOne(query);
        const newNote = `pallet-group:${groupName}; pallets:${pallets}`;
        if (existing) {
          const notes = (existing.notes || '');
          const hasNote = notes.includes(`pallet-group:${groupName};`);
          if (estDeliveryDate) existing.estDeliveryDate = new Date(estDeliveryDate);
          if (!hasNote) existing.notes = notes ? `${notes} | ${newNote}` : newNote;
          await existing.save();
        } else {
          await Shipment.create({
            kind: 'import',
            status: 'on_water',
            warehouseId,
            estDeliveryDate: estDeliveryDate ? new Date(estDeliveryDate) : undefined,
            reference: poNumber,
            items: [],
            notes: newNote,
            createdBy: req.user?.id,
          });
        }
      }
    } catch (e) {
      const msg = (e && e.code === 11000)
        ? 'Duplicate entry for same PO # + Pallet Name + Warehouse'
        : (e?.message || 'Unexpected error');
      errors.push({ rowNum, errors: [msg] });
    }
  }

  res.json({ ok: true, created, errorCount: errors.length, errors });
};

export const listGroupOverview = async (req, res) => {
  const q = normalizeStr(req.query.search || '');
  const stocks = await PalletGroupStock.aggregate([
    { $group: { _id: { groupName: '$groupName', warehouseId: '$warehouseId' }, pallets: { $sum: '$pallets' } } },
  ]);
  const allGroupNames = Array.from(new Set(stocks.map(s => s._id.groupName)));
  let filteredGroupNames = allGroupNames;
  if (q) {
    const qlc = q.toLowerCase();
    const byName = new Set(allGroupNames.filter(n => n.toLowerCase().includes(qlc)));
    const items = await Item.find({
      $or: [
        { itemCode: new RegExp(q, 'i') },
        { description: new RegExp(q, 'i') },
        { color: new RegExp(q, 'i') },
      ]
    }).select('itemGroup').lean();
    const byItem = new Set(items.map(i => i.itemGroup).filter(Boolean));
    const union = new Set([...byName, ...byItem]);
    filteredGroupNames = Array.from(union);
  }
  const byGroup = new Map(filteredGroupNames.map(n => [n, []]));
  for (const s of stocks) {
    if (!byGroup.has(s._id.groupName)) continue;
    byGroup.get(s._id.groupName).push({ warehouseId: s._id.warehouseId.toString(), pallets: s.pallets });
  }
  const result = Array.from(byGroup.entries()).map(([groupName, perWarehouse]) => ({
    groupName,
    perWarehouse,
    totalPallets: perWarehouse.reduce((a, b) => a + (b.pallets || 0), 0),
  }));
  res.json(result.sort((a,b)=> a.groupName.localeCompare(b.groupName)));
};

export const groupDetails = async (req, res) => {
  const groupName = normalizeStr(req.params.groupName || '');
  if (!groupName) return res.status(400).json({ message: 'groupName required' });
  const perWarehouse = await PalletGroupStock.find({ groupName }).lean();
  const items = await Item.find({ itemGroup: groupName }).select('itemCode description color itemGroup packSize').lean();
  const whMap = new Map(perWarehouse.map(s => [String(s.warehouseId), s.pallets]));
  const itemsWithDerived = items.map((it) => {
    const packSize = Number(it.packSize) || 0;
    const derived = {};
    for (const [wid, pallets] of whMap.entries()) derived[wid] = { pallets, qty: pallets * packSize };
    const totalPallets = Array.from(whMap.values()).reduce((a,b)=>a+b,0);
    const totalQty = totalPallets * packSize;
    return { ...it, packSize, totalPallets, perWarehouse: derived, totalQty };
  });
  const txns = await PalletGroupTxn.find({ groupName }).sort({ createdAt: -1 }).limit(20).lean();
  res.json({ groupName, perWarehouse, items: itemsWithDerived, recentTransactions: txns });
};

export const createAdjustment = async (req, res) => {
  const { warehouseId, items, reference } = req.body || {};
  if (!warehouseId) return res.status(400).json({ message: 'warehouseId required' });
  if (!Array.isArray(items) || items.length === 0) return res.status(400).json({ message: 'items required' });

  const wh = await Warehouse.findById(warehouseId).lean();
  if (!wh) return res.status(400).json({ message: 'warehouse not found' });

  const parsed = [];
  const seen = new Set();
  for (const it of items) {
    const groupName = normalizeStr(it?.groupName || '');
    const qty = Number(it?.qty);
    if (!groupName) return res.status(400).json({ message: 'groupName required' });
    if (!Number.isFinite(qty) || qty <= 0) return res.status(400).json({ message: 'qty must be > 0' });
    const key = groupName.toLowerCase();
    if (seen.has(key)) return res.status(400).json({ message: `duplicate groupName: ${groupName}` });
    seen.add(key);
    parsed.push({ groupName, qty: Math.floor(qty) });
  }

  const committedBy = String(req.user?.username || req.user?.id || '');

  const applyWithoutTransaction = async () => {
    const applied = [];
    try {
      for (const it of parsed) {
        const stock = await PalletGroupStock.findOneAndUpdate(
          {
            groupName: it.groupName,
            warehouseId,
            pallets: { $gte: it.qty },
          },
          { $inc: { pallets: -it.qty } },
          { new: true }
        );
        if (!stock) {
          const exists = await PalletGroupStock.findOne({ groupName: it.groupName, warehouseId }).lean();
          if (!exists) throw new Error(`No stock record for ${it.groupName} in this warehouse`);
          throw new Error(`Insufficient pallets for ${it.groupName}`);
        }

        const txn = await PalletGroupTxn.create({
          poNumber: normalizeStr(reference || 'ADJ'),
          groupName: it.groupName,
          warehouseId,
          palletsDelta: -it.qty,
          status: 'Adjustment',
          reason: 'loss',
          committedBy,
        });

        applied.push({ groupName: it.groupName, qty: it.qty, txnId: txn?._id });
      }
      return { created: applied.length };
    } catch (e) {
      // best-effort rollback
      for (const a of applied) {
        try {
          await PalletGroupStock.updateOne(
            { groupName: a.groupName, warehouseId },
            { $inc: { pallets: a.qty } }
          );
        } catch {}
        try {
          if (a.txnId) await PalletGroupTxn.deleteOne({ _id: a.txnId });
        } catch {}
      }
      throw e;
    }
  };

  // Prefer transaction when available; fallback automatically for standalone MongoDB.
  const session = await mongoose.startSession();
  try {
    const result = await session.withTransaction(async () => {
      const createdTxns = [];

      for (const it of parsed) {
        const stock = await PalletGroupStock.findOne({ groupName: it.groupName, warehouseId }).session(session);
        const current = Number(stock?.pallets || 0);
        if (!stock) throw new Error(`No stock record for ${it.groupName} in this warehouse`);
        if (current < it.qty) throw new Error(`Insufficient pallets for ${it.groupName}: available ${current}, requested ${it.qty}`);

        await PalletGroupStock.updateOne(
          { _id: stock._id },
          { $inc: { pallets: -it.qty } },
          { session }
        );

        const txn = await PalletGroupTxn.create([
          {
            poNumber: normalizeStr(reference || 'ADJ'),
            groupName: it.groupName,
            warehouseId,
            palletsDelta: -it.qty,
            status: 'Adjustment',
            reason: 'loss',
            committedBy,
          }
        ], { session });

        createdTxns.push(txn?.[0]);
      }

      return { created: createdTxns.length };
    });

    res.json({ ok: true, ...result });
  } catch (e) {
    const msg = String(e?.message || '');
    const isTxnUnsupported = msg.includes('Transaction numbers are only allowed') || msg.includes('replica set member') || msg.includes('mongos');
    if (isTxnUnsupported) {
      try {
        const result = await applyWithoutTransaction();
        return res.json({ ok: true, ...result });
      } catch (e2) {
        return res.status(400).json({ message: e2?.message || 'Adjustment failed' });
      }
    }
    res.status(400).json({ message: e?.message || 'Adjustment failed' });
  } finally {
    session.endSession();
  }
};
