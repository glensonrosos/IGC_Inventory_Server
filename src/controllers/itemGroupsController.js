import ItemGroup from '../models/ItemGroup.js';
import XLSX from 'xlsx';
import Item from '../models/Item.js';
import PalletGroupStock from '../models/PalletGroupStock.js';
import PalletGroupReservation from '../models/PalletGroupReservation.js';
import PalletGroupTxn from '../models/PalletGroupTxn.js';
import OnProcessPallet from '../models/OnProcessPallet.js';
import UnfulfilledOrder from '../models/UnfulfilledOrder.js';
import FulfilledOrderImport from '../models/FulfilledOrderImport.js';

export const listItemGroups = async (req, res) => {
  const groups = await ItemGroup.find({}).sort({ palletName: 1, lineItem: 1, name: 1 }).lean();
  res.json(groups);
};

export const deleteItemGroup = async (req, res) => {
  const { id } = req.params || {};
  const group = await ItemGroup.findById(id);
  if (!group) return res.status(404).json({ message: 'Item group not found' });
  const itemCount = await Item.countDocuments({ itemGroup: group.name });
  if (itemCount > 0) {
    return res.status(400).json({ message: 'Cannot delete a group that has items', itemCount });
  }
  await ItemGroup.deleteOne({ _id: id });
  return res.json({ message: 'deleted' });
};

export const createItemGroup = async (req, res) => {
  const { palletName, lineItem, palletDescription, price } = req.body || {};
  const pn = String(palletName || '').trim();
  const li = String(lineItem || '').trim();
  const desc = String(palletDescription || '').trim();
  if (!pn) return res.status(400).json({ message: 'Pallet Name required' });
  if (!li) return res.status(400).json({ message: 'Pallet ID required' });
  if (!desc) return res.status(400).json({ message: 'Pallet Description required' });

  const escapeRegex = (s) => String(s || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const existsPN = await ItemGroup.findOne({ palletName: new RegExp(`^${escapeRegex(pn)}$`, 'i') }).lean();
  if (existsPN) return res.status(409).json({ message: 'Pallet Name already exists' });
  const existsLI = await ItemGroup.findOne({ lineItem: new RegExp(`^${escapeRegex(li)}$`, 'i') }).lean();
  if (existsLI) return res.status(409).json({ message: 'Pallet ID already exists' });

  const name = `${pn} - ${li}`;
  const exists = await ItemGroup.findOne({ name });
  if (exists) return res.status(409).json({ message: 'Pallet Group already exists' });
  const p = Number(price);
  const priceValue = Number.isFinite(p) ? p : undefined;
  const doc = await ItemGroup.create({ name, active: true, palletName: pn, lineItem: li, palletDescription: desc, price: priceValue });
  res.status(201).json(doc);
};

export const updateItemGroup = async (req, res) => {
  const { id } = req.params || {};
  const allowed = ['active', 'lineItem', 'price', 'palletName', 'palletDescription'];
  const updates = {};
  for (const k of allowed) if (k in req.body) updates[k] = req.body[k];
  if ('price' in updates) {
    const p = Number(updates.price);
    updates.price = Number.isFinite(p) ? p : undefined;
  }

  const group = await ItemGroup.findById(id);
  if (!group) return res.status(404).json({ message: 'Item group not found' });

  const escapeRegex = (s) => String(s || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  if (typeof updates.palletName === 'string') {
    const nextPNOnly = String(updates.palletName || '').trim();
    if (!nextPNOnly) return res.status(400).json({ message: 'Pallet Name required' });
    const existsPN = await ItemGroup.findOne({ _id: { $ne: id }, palletName: new RegExp(`^${escapeRegex(nextPNOnly)}$`, 'i') }).lean();
    if (existsPN) return res.status(409).json({ message: 'Pallet Name already exists' });
  }
  if (typeof updates.lineItem === 'string') {
    const nextLIOnly = String(updates.lineItem || '').trim();
    if (!nextLIOnly) return res.status(400).json({ message: 'Pallet ID required' });
    const existsLI = await ItemGroup.findOne({ _id: { $ne: id }, lineItem: new RegExp(`^${escapeRegex(nextLIOnly)}$`, 'i') }).lean();
    if (existsLI) return res.status(409).json({ message: 'Pallet ID already exists' });
  }

  // If Pallet Name or Pallet ID changes, update the group key (ItemGroup.name) and cascade to dependents.
  const nextPN = (typeof updates.palletName === 'string') ? String(updates.palletName || '').trim() : String(group.palletName || '').trim();
  const nextLI = (typeof updates.lineItem === 'string') ? String(updates.lineItem || '').trim() : String(group.lineItem || '').trim();
  const nextName = `${nextPN} - ${nextLI}`;
  const prevName = String(group.name || '').trim();

  if (nextPN && nextLI && nextName !== prevName) {
    const exists = await ItemGroup.findOne({ _id: { $ne: id }, name: nextName });
    if (exists) return res.status(409).json({ message: 'Pallet Group already exists' });

    // Update group document
    await ItemGroup.updateOne({ _id: id }, { $set: { ...updates, name: nextName, palletName: nextPN, lineItem: nextLI } });

    // Cascade update for references that use the group name string
    await Item.updateMany({ itemGroup: prevName }, { $set: { itemGroup: nextName } });
    await PalletGroupStock.updateMany({ groupName: prevName }, { $set: { groupName: nextName } });
    await PalletGroupReservation.updateMany({ groupName: prevName }, { $set: { groupName: nextName } });
    await PalletGroupTxn.updateMany({ groupName: prevName }, { $set: { groupName: nextName } });
    await OnProcessPallet.updateMany({ groupName: prevName }, { $set: { groupName: nextName } });

    await UnfulfilledOrder.updateMany(
      { 'lines.groupName': prevName },
      { $set: { 'lines.$[e].groupName': nextName } },
      { arrayFilters: [{ 'e.groupName': prevName }] }
    );
    await UnfulfilledOrder.updateMany(
      { 'allocations.groupName': prevName },
      { $set: { 'allocations.$[e].groupName': nextName } },
      { arrayFilters: [{ 'e.groupName': prevName }] }
    );
    await FulfilledOrderImport.updateMany(
      { 'lines.groupName': prevName },
      { $set: { 'lines.$[e].groupName': nextName } },
      { arrayFilters: [{ 'e.groupName': prevName }] }
    );

    const doc = await ItemGroup.findById(id).lean();
    return res.json(doc);
  }

  // No rename of group key required; just update allowed fields
  const doc = await ItemGroup.findByIdAndUpdate(id, { $set: updates }, { new: true }).lean();
  return res.json(doc);
};

export const renameItemGroup = async (req, res) => {
  try {
    const { id } = req.params || {};
    const nextDesc = String(req.body?.name || '').trim();
    if (!id) return res.status(400).json({ message: 'id required' });
    if (!nextDesc) return res.status(400).json({ message: 'name required' });

    const group = await ItemGroup.findById(id);
    if (!group) return res.status(404).json({ message: 'Item group not found' });

    const prevDesc = String(group.palletDescription || '').trim();
    if (prevDesc === nextDesc) return res.json({ ok: true, renamed: false });

    await ItemGroup.updateOne({ _id: id }, { $set: { palletDescription: nextDesc } });
    return res.json({ ok: true, renamed: true });
  } catch (e) {
    return res.status(500).json({ message: 'Failed to rename Pallet Description' });
  }
};

export const importItemGroups = async (req, res) => {
  try {
    if (!req.file || !req.file.buffer) {
      return res.status(400).json({ message: 'No file uploaded' });
    }

    const wb = XLSX.read(req.file.buffer, { type: 'buffer' });
    const sheet = wb.Sheets[wb.SheetNames[0]];
    if (!sheet) return res.status(400).json({ message: 'No sheet found in workbook' });

    const rows = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: '' });
    if (!rows.length) return res.status(400).json({ message: 'Empty worksheet' });

    const expectedHeaderNoPrice = ['Pallet Name', 'Pallet Description', 'Pallet ID', 'Item Code', 'Item Description', 'Color', 'Pack Size'];
    const expectedHeaderWithPrice = ['Pallet Name', 'Pallet Description', 'Pallet ID', 'Price', 'Item Code', 'Item Description', 'Color', 'Pack Size'];
    const normHeader = (h) => String(h || '').trim().toLowerCase();
    const receivedHeader = Array.isArray(rows[0]) ? rows[0].map(normHeader) : [];
    const expectedHeaderNoPriceNorm = expectedHeaderNoPrice.map(normHeader);
    const expectedHeaderWithPriceNorm = expectedHeaderWithPrice.map(normHeader);
    // Relaxed: accept if all required headers are present anywhere in the header row
    const hasAll = (need) => need.every((h) => receivedHeader.includes(h));
    const headerMatchesNoPrice = hasAll(expectedHeaderNoPriceNorm);
    const headerMatchesWithPrice = hasAll(expectedHeaderWithPriceNorm);
    const headerMatches = headerMatchesNoPrice || headerMatchesWithPrice;
    if (!headerMatches) {
      return res.status(400).json({
        message: 'Invalid template. Column headers must match the template exactly.',
        expectedHeader: expectedHeaderNoPrice,
        receivedHeader: Array.isArray(rows[0]) ? rows[0].map((h) => String(h ?? '')) : []
      });
    }

    // Determine the index of the pallet name, group name column, optional line item column, and optional item count column.
    const header = rows[0].map((h) => String(h).trim().toLowerCase());
    const palletNameIdx = header.findIndex((h) => ['pallet name','palletname'].includes(h));
    let nameIdx = header.findIndex((h) => ['group name', 'name', 'item group', 'itemgroup', 'pallet group', 'palletgroup', 'pallet description', 'palletdescription', 'description'].includes(h));
    if (nameIdx < 0) nameIdx = 0; // fallback: first column
    const lineItemIdx = header.findIndex((h) => ['line item','lineitem','line','pallet id','palletid','pallet'].includes(h));
    const priceIdx = header.findIndex((h) => ['price', 'pallet price', 'palletprice'].includes(h));
    const countIdx = header.findIndex((h) => ['item count', 'count', 'items', 'itemcount'].includes(h));

    // Optional item columns if user uploads a combined sheet (Pallet Group + Items)
    const itemCodeIdx = header.findIndex(h => ['item code','itemcode'].includes(h));
    const itemDescIdx = header.findIndex(h => ['item description','itemdescription','description'].includes(h));
    const itemColorIdx = header.findIndex(h => ['color'].includes(h));
    const itemPackIdx = header.findIndex(h => ['pack size','packsize'].includes(h));

    const seen = new Set();
    const rowsParsed = [];
    const descByGroup = new Map(); // lower(groupKey) -> palletDescription
    const priceByGroup = new Map(); // lower(groupName) -> number
    const errors = [];

    const escapeRegex = (s) => String(s || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

    for (let i = 1; i < rows.length; i++) {
      const row = rows[i];
      const rowNum = i + 1;
      const li = lineItemIdx >= 0 ? String(row[lineItemIdx] ?? '').trim() : '';
      const pn = palletNameIdx >= 0 ? String(row[palletNameIdx] ?? '').trim() : '';
      const rawPrice = priceIdx >= 0 ? String(row[priceIdx] ?? '').trim() : '';
      const rawDesc = (row[nameIdx] ?? '').toString().trim();

      // If user provided any values but left Pallet Description blank, surface a validation error.
      const itemCodeCell = itemCodeIdx >= 0 ? String(row[itemCodeIdx] ?? '').trim() : '';
      const itemDescCell = itemDescIdx >= 0 ? String(row[itemDescIdx] ?? '').trim() : '';
      const itemColorCell = itemColorIdx >= 0 ? String(row[itemColorIdx] ?? '').trim() : '';
      const packCell = itemPackIdx >= 0 ? String(row[itemPackIdx] ?? '').trim() : '';
      const anyOther = Boolean(li || pn || rawPrice || itemCodeCell || itemDescCell || itemColorCell || packCell);
      if (!rawDesc) {
        if (anyOther) {
          errors.push({ rowNum, name: '-', errors: ['Pallet Description is required'] });
        }
        // Skip blank lines silently
        continue;
      }

      if (!pn) {
        errors.push({ rowNum, name: rawDesc, errors: ['Pallet Name is required'] });
      }
      if (!li) {
        errors.push({ rowNum, name: rawDesc, errors: ['Pallet ID is required'] });
      }

      const groupName = `${String(pn || '').trim()} - ${String(li || '').trim()}`.trim();
      const groupLower = groupName.toLowerCase();

      const keyLower = groupLower;

      if (priceIdx >= 0) {
        if (!rawPrice) {
          errors.push({ rowNum, name: groupName || rawDesc, errors: ['Price is required'] });
        } else {
          const p = Number(rawPrice);
          if (!Number.isFinite(p)) {
            errors.push({ rowNum, name: groupName || rawDesc, errors: ['Price must be a valid number'] });
          }
          if (Number.isFinite(p)) {
            // Enforce price consistency for the same Pallet Group within the file
            if (priceByGroup.has(keyLower) && Number(priceByGroup.get(keyLower)) !== Number(p)) {
              errors.push({ rowNum, name: groupName || rawDesc, errors: ['Price must be the same for all rows of this Pallet Group in the file'] });
            }
            priceByGroup.set(keyLower, p);
          }
        }
      }

      // Track description per group key (Pallet Name + Pallet ID), enforce consistency within the file
      if (descByGroup.has(keyLower)) {
        const prevDesc = descByGroup.get(keyLower) || '';
        if (rawDesc && prevDesc && prevDesc.toLowerCase() !== rawDesc.toLowerCase()) {
          errors.push({ rowNum, name: groupName, errors: ['Pallet Description must be the same for all rows of this Pallet Group in the file'] });
        }
        if (rawDesc && !prevDesc) descByGroup.set(keyLower, rawDesc);
      } else {
        descByGroup.set(keyLower, rawDesc);
      }
      const key = groupName.toLowerCase();
      if (seen.has(key)) {
        // only keep first occurrence for group metadata rows
        continue;
      }
      seen.add(key);
      const uploadedCount = countIdx >= 0 ? Number(row[countIdx]) : 0;
      const itemCount = Number.isFinite(uploadedCount) ? uploadedCount : 0;
      rowsParsed.push({ name: groupName, rowNum, itemCount, lineItem: li, palletName: pn, palletDescription: rawDesc, price: priceByGroup.get(keyLower) });
    }

    // Check existing group keys in DB
    const names = rowsParsed.map(r => r.name);
    const existing = await ItemGroup.find({ name: { $in: names } }).lean();
    const existingSet = new Set(existing.map((g) => g.name.toLowerCase()));
    const toCreate = rowsParsed.filter((r) => !existingSet.has(r.name.toLowerCase()));

    // Prepare item import diff but do not write yet
    let itemsCreated = 0;
    let itemsUpdated = 0;
    let itemsSkipped = 0;
    let pendingItemOps = [];
    const hasItemColumns = itemCodeIdx >= 0;
    const missingRequired = (label, rowNum, name) => errors.push({ rowNum, name, errors: [`${label} is required`] });
    if (hasItemColumns) {
      const nameByLower = new Map();
      const allGroups = await ItemGroup.find({ name: { $in: names } }).select('name active').lean();
      for (const g of allGroups) nameByLower.set(g.name.toLowerCase(), g);
      // Include groups that will be created in this import so items can be processed in one go
      for (const g of toCreate) {
        nameByLower.set(g.name.toLowerCase(), { name: g.name, active: true });
      }
      // Build parsed item rows with row numbers
      const itemParsed = [];
      for (let i = 1; i < rows.length; i++) {
        const row = rows[i];
        const rawDesc = (row[nameIdx] ?? '').toString().trim();
        const li = lineItemIdx >= 0 ? String(row[lineItemIdx] ?? '').trim() : '';
        const pn = palletNameIdx >= 0 ? String(row[palletNameIdx] ?? '').trim() : '';
        const rawPrice = priceIdx >= 0 ? String(row[priceIdx] ?? '').trim() : '';
        const itemCode = (row[itemCodeIdx] ?? '').toString().trim();
        if (!rawDesc) { continue; }
        const rowNum = i + 1;

        const groupName = `${String(pn || '').trim()} - ${String(li || '').trim()}`.trim();

        // Enforce required columns for pallet template
        if (!pn) missingRequired('Pallet Name', rowNum, rawDesc);
        if (!li) missingRequired('Pallet ID', rowNum, rawDesc);
        if (!itemCode) missingRequired('Item Code', rowNum, groupName || rawDesc);
        const desc = itemDescIdx >= 0 ? (row[itemDescIdx] ?? '').toString().trim() : '';
        const color = itemColorIdx >= 0 ? (row[itemColorIdx] ?? '').toString().trim() : '';
        let packVal = NaN;
        if (itemPackIdx >= 0) {
          const rawPack = (row[itemPackIdx] ?? '').toString().trim();
          if (rawPack === '') {
            packVal = 0; // treat blank pack size as 0
          } else {
            const parsed = Number(rawPack);
            packVal = parsed;
          }
        }

        if (!desc) missingRequired('Item Description', rowNum, groupName || rawDesc);
        if (!color) missingRequired('Color', rowNum, groupName || rawDesc);

        let priceNum;
        if (priceIdx >= 0) {
          if (!rawPrice) missingRequired('Price', rowNum, groupName || rawDesc);
          const parsedPrice = rawPrice === '' ? NaN : Number(rawPrice);
          priceNum = parsedPrice;
          if (!Number.isFinite(parsedPrice)) {
            errors.push({ rowNum, name: groupName || rawDesc, errors: ['Price must be a valid number'] });
          }
        }

        itemParsed.push({ groupName, lineItem: li, itemCode, description: desc, color, packSize: packVal, rowNum, price: priceNum });
      }
      // Dedupe within file by (itemCode, groupName)
      const byKey = new Map();
      for (const r of itemParsed) {
        const gdoc = nameByLower.get(r.groupName.toLowerCase());
        if (!gdoc || gdoc.active === false) { continue; }
        if (!Number.isFinite(r.packSize)) { errors.push({ rowNum: r.rowNum, itemCode: r.itemCode, errors: ['Pack Size must be a valid number (or leave blank for 0)'] }); continue; }
        if (r.packSize < 0) { errors.push({ rowNum: r.rowNum, itemCode: r.itemCode, errors: ['Pack Size must be >= 0'] }); continue; }
        const key = `${gdoc.name.toLowerCase()}|${r.itemCode.toLowerCase()}`;
        if (!byKey.has(key)) {
          byKey.set(key, { ...r, groupName: gdoc.name });
        } else {
          const prev = byKey.get(key);
          const same = prev.description === r.description && prev.color === r.color && Number(prev.packSize) === Number(r.packSize);
          if (same) {
            itemsSkipped++; // identical duplicate row in file
          } else {
            errors.push({ rowNum: r.rowNum, itemCode: r.itemCode, itemGroup: gdoc.name, errors: ['Duplicate Item Code in the same Pallet Group in file with different details'] });
            // last occurrence wins
            byKey.set(key, { ...r, groupName: gdoc.name });
          }
        }
      }
      // Defer DB writes until after groups are inserted; counts will be computed in pending ops below
      // Update group price even if Pallet ID is blank (price-only updates)
      for (const [lowerName, p] of priceByGroup.entries()) {
        if (typeof p !== 'number' || !Number.isFinite(p)) continue;
        const gdoc = await ItemGroup.findOne({ name: new RegExp(`^${escapeRegex(lowerName)}$`, 'i') }).lean();
        if (!gdoc) continue;
        const prev = Number(gdoc.price);
        const prevOk = typeof gdoc.price === 'number' && Number.isFinite(prev);
        if (!prevOk || prev !== p) {
          pendingItemOps.push({ type: 'updatePrice', id: gdoc._id, price: p });
        }
      }
      // Build item upsert ops (defer execution)
      for (const r of byKey.values()) {
        const existing = await Item.findOne({ itemCode: r.itemCode, itemGroup: r.groupName });
        if (existing) {
          const changes = {};
          if (existing.description !== r.description) changes.description = r.description;
          if (existing.color !== r.color) changes.color = r.color;
          if (Number(existing.packSize) !== Number(r.packSize)) changes.packSize = Number(r.packSize);
          if (Object.keys(changes).length) { pendingItemOps.push({ type: 'updateItem', id: existing._id, changes }); itemsUpdated++; }
          else { itemsSkipped++; }
        } else {
          pendingItemOps.push({ type: 'createItem', payload: { itemCode: r.itemCode, itemGroup: r.groupName, description: r.description, color: r.color, totalQty: 0, packSize: Number(r.packSize), enabled: true } });
          itemsCreated++;
        }
      }
    }

    // If any errors, reject the entire import (no writes)
    if (errors.length) {
      const result = {
        created: 0,
        skipped: 0,
        errorCount: errors.length,
        errors,
        itemsCreated: 0,
        itemsUpdated: 0,
        itemsSkipped: 0,
      };
      return res.status(400).json(result);
    }

    // Perform writes only when there are no validation errors
    const createdDocs = await ItemGroup.insertMany(
      toCreate.map((r) => ({ name: r.name, palletName: (r.palletName || '').trim(), lineItem: (r.lineItem || '').trim(), palletDescription: (r.palletDescription || '').trim(), price: typeof r.price === 'number' ? r.price : undefined })),
      { ordered: false }
    ).catch(() => []);

    for (const op of pendingItemOps) {
      if (op.type === 'updatePrice') {
        await ItemGroup.updateOne({ _id: op.id }, { $set: { price: op.price } });
      } else if (op.type === 'updatePalletName') {
        await ItemGroup.updateOne({ _id: op.id }, { $set: { palletName: op.palletName } });
      } else if (op.type === 'updateItem') {
        await Item.updateOne({ _id: op.id }, { $set: op.changes });
      } else if (op.type === 'createItem') {
        await Item.create(op.payload);
      }
    }

    const result = {
      created: Array.isArray(createdDocs) ? createdDocs.length : 0,
      skipped: rowsParsed.filter(r => existingSet.has(r.name.toLowerCase())).length,
      errorCount: 0,
      errors: [],
      itemsCreated,
      itemsUpdated,
      itemsSkipped,
    };

    return res.json(result);
  } catch (err) {
    console.error('importItemGroups failed', err);
    return res.status(500).json({ message: 'Failed to import item groups' });
  }
};
