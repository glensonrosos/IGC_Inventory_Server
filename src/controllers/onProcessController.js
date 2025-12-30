import XLSX from 'xlsx';
import OnProcessItem from '../models/OnProcessItem.js';
import ImportLog from '../models/ImportLog.js';
import Item from '../models/Item.js';
import OnProcessBatch from '../models/OnProcessBatch.js';
import Counter from '../models/Counter.js';
import OnProcessPallet from '../models/OnProcessPallet.js';
import ItemGroup from '../models/ItemGroup.js';
import Shipment from '../models/Shipment.js';
import PalletGroupStock from '../models/PalletGroupStock.js';
import PalletGroupTxn from '../models/PalletGroupTxn.js';
import PalletGroupReservation from '../models/PalletGroupReservation.js';

export const listOnProcess = async (req, res) => {
  const pallets = await OnProcessPallet.find({}).sort({ createdAt: -1 }).lean();
  res.json(pallets);
};

export const getBatchItems = async (req, res) => {
  const { id } = req.params;
  const items = await OnProcessItem.find({ batchId: id }).sort({ createdAt: 1 }).lean();
  const codes = Array.from(new Set(items.map(i => i.itemCode)));
  const meta = await Item.find({ itemCode: { $in: codes } }).select('itemCode itemGroup description color').lean();
  const map = new Map(meta.map(m => [m.itemCode, m]));
  const enriched = items.map(it => {
    const m = map.get(it.itemCode) || {};
    return { ...it, itemGroup: m.itemGroup || '', description: m.description || '', color: m.color || '' };
  });
  res.json(enriched);
};

export const importOnProcess = async (req, res) => {
  try {
    if (!req.file || !req.file.buffer) return res.status(400).json({ message: 'No file uploaded' });
    const wb = XLSX.read(req.file.buffer, { type: 'buffer' });
    const sheet = wb.Sheets[wb.SheetNames[0]];
    if (!sheet) return res.status(400).json({ message: 'No sheet found in workbook' });

    const rows = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: '' });
    if (!rows.length) return res.status(400).json({ message: 'Empty worksheet' });

    const header = rows[0].map((h) => String(h).trim().toLowerCase());
    const poIdx = header.findIndex((h) => ['po #','po','po#','po number','ponumber'].includes(h));
    const codeIdx = header.findIndex((h) => ['item code','itemcode','code'].includes(h));
    const qtyIdx = header.findIndex((h) => ['total qty','qty','totalqty'].includes(h));
    const packIdx = header.findIndex((h) => ['pack size','pack','packsize'].includes(h));

    const errors = [];
    const parsed = [];
    const seen = new Set();

    for (let i = 1; i < rows.length; i++) {
      const row = rows[i];
      const rowNum = i + 1;
      const poNumber = (row[poIdx] ?? '').toString().trim();
      const itemCode = (row[codeIdx] ?? '').toString().trim();
      const totalQty = Number(row[qtyIdx]);
      const packSize = Number(row[packIdx]);
      const rowErrs = [];
      if (!poNumber) rowErrs.push('PO# is required');
      if (!itemCode) rowErrs.push('Item Code is required');
      if (!Number.isFinite(totalQty)) rowErrs.push('Total Qty is required and must be a number');
      if (!Number.isFinite(packSize) || packSize <= 0) rowErrs.push('Pack Size is required and must be > 0');
      if (rowErrs.length) { errors.push({ rowNum, itemCode, errors: rowErrs }); continue; }
      const key = `${poNumber}::${itemCode}`.toLowerCase();
      if (seen.has(key)) { errors.push({ rowNum, itemCode, errors: ['Duplicate PO# + Item Code in file'] }); continue; }
      seen.add(key);
      parsed.push({ rowNum, poNumber, itemCode, totalQty, packSize });
    }

    // Check item registration + enabled
    const codes = Array.from(new Set(parsed.map(r => r.itemCode)));
    if (codes.length) {
      const dbItems = await Item.find({ itemCode: { $in: codes } }).select('itemCode enabled').lean();
      const map = new Map(dbItems.map(d => [d.itemCode, d]));
      for (const r of parsed) {
        const rec = map.get(r.itemCode);
        if (!rec) errors.push({ rowNum: r.rowNum, itemCode: r.itemCode, errors: ['item not registered'] });
        else if (rec.enabled === false) errors.push({ rowNum: r.rowNum, itemCode: r.itemCode, errors: ['This item is currently disabled. Remove this row from the file.'] });
      }
    }

    // Check duplicates against inventory imports (ImportLog)
    const keys = parsed.map(r => ({ poNumber: r.poNumber, itemCode: r.itemCode }));
    if (keys.length) {
      const found = await ImportLog.find({ $or: keys }).select('poNumber itemCode').lean();
      const set = new Set(found.map(f => `${f.poNumber}::${f.itemCode}`.toLowerCase()));
      for (const r of parsed) {
        if (set.has(`${r.poNumber}::${r.itemCode}`.toLowerCase())) {
          errors.push({ rowNum: r.rowNum, itemCode: r.itemCode, errors: ['Already imported into Inventory (PO# + Item Code)'] });
        }
      }
    }

    // Check duplicates against existing OnProcess entries
    const existOnProc = await OnProcessItem.find({ $or: parsed.map(r => ({ poNumber: r.poNumber, itemCode: r.itemCode })) }).select('poNumber itemCode').lean();
    const existSet = new Set(existOnProc.map(e => `${e.poNumber}::${e.itemCode}`.toLowerCase()));
    for (const r of parsed) {
      if (existSet.has(`${r.poNumber}::${r.itemCode}`.toLowerCase())) {
        errors.push({ rowNum: r.rowNum, itemCode: r.itemCode, errors: ['Already exists in On-Process list'] });
      }
    }

    // Create those without errors for their row
    const valid = parsed.filter(r => !errors.some(e => e.rowNum === r.rowNum));
    let created = 0;
    if (valid.length) {
      // Group by PO# and ensure a batch exists per PO# with auto reference
      const byPo = valid.reduce((acc, r) => { (acc[r.poNumber] ||= []).push(r); return acc; }, {});
      const docs = [];
      for (const po of Object.keys(byPo)) {
        let batch = await OnProcessBatch.findOne({ poNumber: po });
        if (!batch) {
          const d = new Date();
          d.setMonth(d.getMonth() + 1);
          const c = await Counter.findOneAndUpdate(
            { name: 'on_process' },
            { $inc: { seq: 1 } },
            { upsert: true, new: true }
          );
          const ref = `PROC-${String(c.seq).padStart(6, '0')}`;
          batch = await OnProcessBatch.create({ poNumber: po, reference: ref, estFinishDate: d, createdBy: req.user?.id });
        } else if (!batch.estFinishDate) {
          const d = new Date();
          d.setMonth(d.getMonth() + 1);
          batch.estFinishDate = d;
          await batch.save();
        }
        for (const r of byPo[po]) {
          docs.push({ ...r, batchId: batch._id, createdBy: req.user?.id });
        }
      }
      try {
        const inserted = await OnProcessItem.insertMany(docs, { ordered: false });
        created = Array.isArray(inserted) ? inserted.length : 0;
      } catch (e) {
        // unique errors are already flagged above; ignore here
      }
    }

    return res.json({ created, skipped: parsed.length - created, errorCount: errors.length, errors });
  } catch (err) {
    console.error('importOnProcess failed', err);
    return res.status(500).json({ message: 'Failed to import on-process items' });
  }
};

export const listBatches = async (req, res) => {
  const batches = await OnProcessBatch.find({}).sort({ createdAt: -1 }).lean();
  const ids = batches.map(b => b._id);
  const counts = await OnProcessItem.aggregate([
    { $match: { batchId: { $in: ids } } },
    { $group: { _id: '$batchId', count: { $sum: 1 } } }
  ]);
  const countMap = new Map(counts.map(c => [String(c._id), c.count]));
  const data = batches.map(b => ({ ...b, itemCount: countMap.get(String(b._id)) || 0 }));
  res.json(data);
};

export const dueToday = async (req, res) => {
  try {
    const now = new Date();
    const y = now.getFullYear();
    const m = now.getMonth();
    const d = now.getDate();
    const start = new Date(y, m, d, 0, 0, 0, 0);
    const end = new Date(y, m, d, 23, 59, 59, 999);
    const q = {
      status: { $ne: 'completed' },
      $or: [
        { estFinishDate: { $exists: false } },
        { estFinishDate: null },
        { estFinishDate: { $gte: start, $lte: end } },
      ],
    };
    const count = await OnProcessBatch.countDocuments(q);
    res.json({ count });
  } catch (e) {
    console.error('onProcess.dueToday failed', e);
    res.status(500).json({ message: 'Failed to load on-process due today' });
  }
};

export const updateBatch = async (req, res) => {
  const { id } = req.params;
  const { status, estFinishDate, notes } = req.body || {};
  const allowed = ['in-progress','partial-done','completed'];
  const update = {};
  if (status && allowed.includes(status)) update.status = status;
  if (estFinishDate !== undefined) update.estFinishDate = estFinishDate ? new Date(estFinishDate) : null;
  if (typeof notes === 'string') update.notes = notes;
  const doc = await OnProcessBatch.findByIdAndUpdate(id, update, { new: true });
  if (!doc) return res.status(404).json({ message: 'batch not found' });
  res.json(doc);
};

export const updateBatchItems = async (req, res) => {
  const { id } = req.params;
  const { items } = req.body || {};
  if (!Array.isArray(items)) return res.status(400).json({ message: 'items required' });
  const allowed = ['on_process','completed','cancelled'];
  let updated = 0;
  for (const it of items) {
    const { itemCode, status, notes } = it || {};
    if (!itemCode) continue;
    const update = {};
    if (status && allowed.includes(status)) update.status = status;
    if (typeof notes === 'string') update.notes = notes;
    const resu = await OnProcessItem.updateOne({ batchId: id, itemCode }, { $set: update });
    updated += resu.modifiedCount || 0;
  }
  res.json({ updated });
};

export const exportBatch = async (req, res) => {
  const { id } = req.params;
  const batch = await OnProcessBatch.findById(id).lean();
  if (!batch) return res.status(404).json({ message: 'batch not found' });
  // Export Pallet list with batch metadata
  const pallets = await OnProcessPallet.find({ batchId: id }).sort({ createdAt: 1 }).lean();
  const header = ['Pallet Description','Total Pallet','Finished','Transferred','Remaining','Status','Notes'];
  const data = [];
  // metadata rows
  data.push(['Reference', batch.reference]);
  data.push(['PO #', batch.poNumber]);
  data.push(['Status', batch.status]);
  data.push(['Estimated Date Finish', batch.estFinishDate ? new Date(batch.estFinishDate).toISOString().slice(0,10) : '']);
  data.push(['Notes/Remarks', batch.notes || '']);
  data.push([]);
  data.push(header);
  for (const p of pallets) {
    const remaining = Math.max(0, (p.totalPallet || 0) - ((p.transferredPallet || 0) + (p.finishedPallet || 0)));
    data.push([
      p.groupName,
      p.totalPallet || 0,
      p.finishedPallet || 0,
      p.transferredPallet || 0,
      remaining,
      p.status || 'in_progress',
      p.notes || ''
    ]);
  }
  const ws = XLSX.utils.aoa_to_sheet(data);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'On-Process Pallets');
  const buf = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });
  res.setHeader('Content-Disposition', `attachment; filename="${batch.reference}.xlsx"`);
  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.send(buf);
};

// ===================== PALLET-GROUP (ON-PROCESS) =====================

export const importOnProcessPallets = async (req, res) => {
  try {
    if (!req.file || !req.file.buffer) return res.status(400).json({ message: 'No file uploaded' });
    const wb = XLSX.read(req.file.buffer, { type: 'buffer' });
    const sheet = wb.Sheets[wb.SheetNames[0]];
    if (!sheet) return res.status(400).json({ message: 'No sheet found in workbook' });

    const rows = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: '' });
    if (!rows.length) return res.status(400).json({ message: 'Empty worksheet' });

    const header = rows[0].map(h => String(h).trim().toLowerCase());
    const poIdx = header.findIndex(h => ['po #','po','po#','po number','ponumber'].includes(h));
    const groupIdx = header.findIndex(h => ['pallet description','palletdescription','description','pallet group','palletgroup','group','group name','pallet'].includes(h));
    const totalIdx = header.findIndex(h => ['total pallet','total pallets','pallets','total'].includes(h));

    const errors = [];
    const parsed = [];
    const seen = new Set();
    for (let i = 1; i < rows.length; i++) {
      const row = rows[i];
      const rowNum = i + 1;
      const poNumber = (row[poIdx] ?? '').toString().trim();
      const groupName = (row[groupIdx] ?? '').toString().trim();
      const totalPallet = Number(row[totalIdx]);
      const rowErrs = [];
      if (!poNumber) rowErrs.push('PO# is required');
      if (!groupName) rowErrs.push('Pallet Description is required');
      if (!Number.isFinite(totalPallet) || totalPallet <= 0) rowErrs.push('Total Pallet must be > 0');
      if (rowErrs.length) { errors.push({ rowNum, poNumber, groupName, errors: rowErrs }); continue; }
      const key = `${poNumber}::${groupName}`.toLowerCase();
      if (seen.has(key)) { errors.push({ rowNum, poNumber, groupName, errors: ['Duplicate PO# + Pallet Description in file'] }); continue; }
      seen.add(key);
      parsed.push({ rowNum, poNumber, groupName, totalPallet });
    }

    // validate groups are registered and active
    const groups = Array.from(new Set(parsed.map(r => r.groupName)));
    const found = await ItemGroup.find({ name: { $in: groups } }).select('name active').lean();
    const map = new Map(found.map(f => [f.name, f]));
    for (const r of parsed) {
      const g = map.get(r.groupName);
      if (!g) errors.push({ rowNum: r.rowNum, poNumber: r.poNumber, groupName: r.groupName, errors: ['pallet description not registered'] });
      else if (g.active === false) errors.push({ rowNum: r.rowNum, poNumber: r.poNumber, groupName: r.groupName, errors: ['pallet description is inactive'] });
    }

    // check duplicates existing
    const exist = await OnProcessPallet.find({ $or: parsed.map(p => ({ poNumber: p.poNumber, groupName: p.groupName })) }).select('poNumber groupName').lean();
    const existSet = new Set(exist.map(e => `${e.poNumber}::${e.groupName}`.toLowerCase()));
    for (const r of parsed) {
      if (existSet.has(`${r.poNumber}::${r.groupName}`.toLowerCase())) {
        errors.push({ rowNum: r.rowNum, poNumber: r.poNumber, groupName: r.groupName, errors: ['Already exists in On-Process pallets'] });
      }
    }

    const valid = parsed.filter(r => !errors.some(e => e.rowNum === r.rowNum));
    let created = 0;
    if (valid.length) {
      const byPo = valid.reduce((acc, r) => { (acc[r.poNumber] ||= []).push(r); return acc; }, {});
      const docs = [];
      for (const po of Object.keys(byPo)) {
        let batch = await OnProcessBatch.findOne({ poNumber: po });
        if (!batch) {
          const d = new Date();
          d.setMonth(d.getMonth() + 1);
          const c = await Counter.findOneAndUpdate(
            { name: 'on_process' },
            { $inc: { seq: 1 } },
            { upsert: true, new: true }
          );
          const ref = `PROC-${String(c.seq).padStart(6, '0')}`;
          batch = await OnProcessBatch.create({ poNumber: po, reference: ref, estFinishDate: d, createdBy: req.user?.id });
        } else if (!batch.estFinishDate) {
          const d = new Date();
          d.setMonth(d.getMonth() + 1);
          batch.estFinishDate = d;
          await batch.save();
        }
        for (const r of byPo[po]) docs.push({ ...r, batchId: batch._id, createdBy: req.user?.id });
      }
      try {
        const inserted = await OnProcessPallet.insertMany(docs, { ordered: false });
        created = Array.isArray(inserted) ? inserted.length : 0;
      } catch {}
    }

    return res.json({ created, skipped: parsed.length - created, errorCount: errors.length, errors });
  } catch (err) {
    console.error('importOnProcessPallets failed', err);
    return res.status(500).json({ message: 'Failed to import on-process pallets' });
  }
};

export const getBatchPallets = async (req, res) => {
  const { id } = req.params;
  const rows = await OnProcessPallet.find({ batchId: id }).sort({ createdAt: 1 }).lean();
  const toUnlockIds = [];
  const mapped = rows.map(r => {
    const remainingPallet = Math.max(0, (r.totalPallet || 0) - ((r.transferredPallet || 0) + (r.finishedPallet || 0)));
    // If there is remaining work, the row should not stay locked.
    if (r.locked && remainingPallet > 0) {
      toUnlockIds.push(r._id);
    }
    return {
      ...r,
      remainingPallet,
      locked: Boolean(r.locked) && remainingPallet === 0,
    };
  });
  if (toUnlockIds.length) {
    try {
      await OnProcessPallet.updateMany({ _id: { $in: toUnlockIds } }, { $set: { locked: false } });
    } catch {}
  }
  res.json(mapped);
};

// Fetch on-process pallets by PO number directly
export const listPalletsByPo = async (req, res) => {
  const po = String(req.query.po || '').trim();
  if (!po) return res.status(400).json({ message: 'po is required' });
  const rows = await OnProcessPallet.find({ poNumber: po }).sort({ createdAt: 1 }).lean();
  const mapped = rows.map(r => ({
    groupName: r.groupName,
    totalPallet: r.totalPallet,
    finishedPallet: r.finishedPallet || 0,
    transferredPallet: r.transferredPallet || 0,
    status: r.status || 'in_progress',
  }));
  res.json(mapped);
};

export const updateBatchPallets = async (req, res) => {
  const { id } = req.params;
  const { pallets } = req.body || {};
  if (!Array.isArray(pallets)) return res.status(400).json({ message: 'pallets required' });
  let updated = 0;
  let skippedLocked = 0;
  let skippedCancelWithTransfer = 0;
  for (const p of pallets) {
    const { groupName, finishedPallet, totalPallet, status, notes } = p || {};
    if (!groupName) continue;
    // Load doc to respect locking
    const doc = await OnProcessPallet.findOne({ batchId: id, groupName });
    if (!doc) continue;
    if (doc.locked) {
      // Locked rows: allow ONLY increasing totalPallet (never decreasing), since transferred is already committed.
      // Ignore all other updates.
      if (Number.isFinite(totalPallet) && totalPallet > 0) {
        const transferred = Math.max(0, doc.transferredPallet || 0);
        const prevTotal = Math.max(0, doc.totalPallet || 0);
        const nextTotal = Math.max(prevTotal, transferred, Number(totalPallet));
        if (nextTotal > prevTotal) {
          await OnProcessPallet.updateOne(
            { _id: doc._id },
            {
              $set: {
                totalPallet: nextTotal,
                status: 'in_progress',
                locked: false,
              },
            }
          );
          updated += 1;
        } else {
          skippedLocked += 1;
        }
      } else {
        skippedLocked += 1;
      }
      continue;
    }
    const update = {};
    if (Number.isFinite(finishedPallet) && finishedPallet >= 0) {
      const maxFinish = Math.max(0, (doc.totalPallet || 0) - (doc.transferredPallet || 0));
      update.finishedPallet = Math.min(finishedPallet, maxFinish);
    }
    if (Number.isFinite(totalPallet) && totalPallet > 0) {
      // Ensure total cannot be set below already transferred
      const minTotal = Math.max(0, doc.transferredPallet || 0);
      update.totalPallet = Math.max(totalPallet, minTotal);
      // Also cap current finished against new total
      const newMaxFinish = Math.max(0, (update.totalPallet || doc.totalPallet || 0) - (doc.transferredPallet || 0));
      if (update.finishedPallet == null) update.finishedPallet = Math.min(doc.finishedPallet || 0, newMaxFinish);
      else update.finishedPallet = Math.min(update.finishedPallet, newMaxFinish);
    }
    if (typeof status === 'string') {
      if (status === 'cancelled' && (doc.transferredPallet || 0) > 0) {
        skippedCancelWithTransfer += 1;
      } else {
        update.status = status;
      }
    }
    if (typeof notes === 'string') update.notes = notes;
    const resu = await OnProcessPallet.updateOne({ _id: doc._id }, { $set: update });
    updated += resu.modifiedCount || 0;
  }
  res.json({ updated, skippedLocked, skippedCancelWithTransfer });
};

export const addBatchPallet = async (req, res) => {
  const { id } = req.params;
  const { groupName, totalPallet } = req.body || {};
  if (!groupName || !Number.isFinite(totalPallet) || totalPallet <= 0) return res.status(400).json({ message: 'groupName and totalPallet (>0) required' });
  const batch = await OnProcessBatch.findById(id);
  if (!batch) return res.status(404).json({ message: 'batch not found' });
  // validate not duplicate and group exists/active
  const exists = await OnProcessPallet.findOne({ poNumber: batch.poNumber, groupName });
  if (exists) return res.status(400).json({ message: 'pallet description already exists for this PO#' });
  const grp = await ItemGroup.findOne({ name: groupName, active: true }).lean();
  if (!grp) return res.status(400).json({ message: 'pallet description not registered or inactive' });
  const doc = await OnProcessPallet.create({
    batchId: batch._id,
    poNumber: batch.poNumber,
    groupName,
    totalPallet,
    finishedPallet: 0,
    transferredPallet: 0,
    status: 'in_progress',
    createdBy: req.user?.id,
  });
  res.status(201).json(doc);
};

export const transferBatchPallets = async (req, res) => {
  const { id } = req.params;
  const { mode, warehouseId, estDeliveryDate, items } = req.body || {};
  if (!['delivered','on_water'].includes(mode)) return res.status(400).json({ message: 'mode must be delivered or on_water' });
  if (!warehouseId) return res.status(400).json({ message: 'warehouseId required' });
  if (!Array.isArray(items) || !items.length) return res.status(400).json({ message: 'items required' });

  const batch = await OnProcessBatch.findById(id).lean();
  if (!batch) return res.status(404).json({ message: 'batch not found' });
  const poNumber = batch.poNumber;
  const now = new Date();
  const committedBy = String(req.user?.username || req.user?.id || '').trim();

  // aggregate pallets to transfer per group
  const toTransfer = items
    .filter(it => it && it.groupName && Number.isFinite(it.pallets) && it.pallets > 0)
    .map(it => ({ groupName: String(it.groupName), pallets: Number(it.pallets) }));
  if (!toTransfer.length) return res.status(400).json({ message: 'no valid items to transfer' });

  if (mode === 'delivered') {
    const migrateOnProcessToPrimary = async ({ groupName, pallets }) => {
      const wid = String(warehouseId || '').trim();
      const g = String(groupName || '').trim();
      let remaining = Math.max(0, Math.floor(Number(pallets || 0)));
      if (!wid || !g || !remaining) return;

      const toMove = await PalletGroupReservation.find({
        warehouseId: wid,
        groupName: g,
        source: 'on_process',
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

    for (const it of toTransfer) {
      await PalletGroupStock.findOneAndUpdate(
        { groupName: it.groupName, warehouseId },
        { $inc: { pallets: it.pallets } },
        { upsert: true, new: true, setDefaultsOnInsert: true }
      );
      await PalletGroupTxn.create({
        poNumber,
        groupName: it.groupName,
        warehouseId,
        palletsDelta: it.pallets,
        status: 'Delivered',
        wasOnWater: false,
        committedAt: now,
        notes: 'on_process | Delivered'
      });

      // Move any existing order reservations from on_process -> primary (so Orders reserved stock updates).
      await migrateOnProcessToPrimary({ groupName: it.groupName, pallets: it.pallets });
    }
  } else {
    const noteChunks = toTransfer.map(t => `pallet-group:${t.groupName}; pallets:${t.pallets}`);
    const appended = `on-process import | ${noteChunks.join(' | ')}`;
    // Always create a new Shipment entry even if PO# (reference) is the same
    await Shipment.create({
      kind: 'import',
      status: 'on_water',
      reference: poNumber,
      warehouseId,
      estDeliveryDate: estDeliveryDate ? new Date(estDeliveryDate) : undefined,
      items: [],
      notes: appended,
      createdBy: req.user?.id,
    });

    // Move existing order reservations from on_process -> on_water for this warehouse/group.
    // Without this, Orders "Reserved Stock" will still show on_process reserved even after transfer.
    for (const it of toTransfer) {
      let remaining = Math.max(0, Math.floor(Number(it.pallets || 0)));
      if (!it.groupName || !remaining) continue;

      const toMove = await PalletGroupReservation.find({
        warehouseId,
        groupName: it.groupName,
        source: 'on_process',
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

        // decrement (or delete) the on_process reservation
        const left = have - take;
        if (left <= 0) {
          await PalletGroupReservation.deleteOne({ _id: r._id });
        } else {
          await PalletGroupReservation.updateOne({ _id: r._id }, { $set: { qty: left } });
        }

        // increment (upsert) a matching on_water reservation for the same order
        await PalletGroupReservation.findOneAndUpdate(
          {
            orderNumber: String(r?.orderNumber || '').trim(),
            warehouseId,
            groupName: it.groupName,
            source: 'on_water',
          },
          {
            $inc: { qty: take },
            $setOnInsert: { committedBy: committedBy || String(r?.committedBy || '') },
          },
          { upsert: true, new: true, setDefaultsOnInsert: true }
        );

        remaining -= take;
      }
    }
  }

  // apply transfer to on-process pallets: decrease finished, increase transferred, recompute status, and lock if fully delivered
  for (const it of toTransfer) {
    const doc = await OnProcessPallet.findOne({ batchId: id, groupName: it.groupName });
    if (!doc) continue;
    const finished = Math.max(0, doc.finishedPallet || 0);
    const transferred = Math.max(0, doc.transferredPallet || 0);
    const total = Math.max(0, doc.totalPallet || 0);
    const maxTransfer = Math.max(0, Math.min(finished, total - transferred));
    const qty = Math.min(it.pallets, maxTransfer);
    if (qty <= 0) continue;
    doc.finishedPallet = finished - qty;
    doc.transferredPallet = transferred + qty;
    const remainingAfter = Math.max(0, total - (doc.transferredPallet + doc.finishedPallet));
    doc.status = remainingAfter === 0 ? 'completed' : (doc.finishedPallet > 0 ? 'partial' : 'in_progress');
    if (mode === 'delivered' && doc.transferredPallet >= total) {
      doc.locked = true;
    }
    await doc.save();
  }

  res.json({ message: 'transfer created', mode, warehouseId, poNumber, items: toTransfer });
};
