import ItemGroup from '../models/ItemGroup.js';
import XLSX from 'xlsx';
import Item from '../models/Item.js';

export const listItemGroups = async (req, res) => {
  const groups = await ItemGroup.find({}).sort({ name: 1 }).lean();
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
  const { name, lineItem } = req.body || {};
  if (!name) return res.status(400).json({ message: 'name required' });
  const exists = await ItemGroup.findOne({ name });
  if (exists) return res.status(409).json({ message: 'Item group already exists' });
  const doc = await ItemGroup.create({ name, active: true, lineItem: (lineItem || '').trim() });
  res.status(201).json(doc);
};

export const updateItemGroup = async (req, res) => {
  const { id } = req.params || {};
  const allowed = ['name', 'active', 'lineItem'];
  const updates = {};
  for (const k of allowed) if (k in req.body) updates[k] = req.body[k];
  if (updates.name) {
    const exists = await ItemGroup.findOne({ _id: { $ne: id }, name: updates.name });
    if (exists) return res.status(409).json({ message: 'Item group already exists' });
  }
  const doc = await ItemGroup.findByIdAndUpdate(id, { $set: updates }, { new: true });
  if (!doc) return res.status(404).json({ message: 'Item group not found' });
  return res.json(doc);
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

    const expectedHeader = ['Pallet Description', 'Pallet ID', 'Item Code', 'Item Description', 'Color', 'Pack Size'];
    const normHeader = (h) => String(h || '').trim().toLowerCase();
    const receivedHeader = Array.isArray(rows[0]) ? rows[0].map(normHeader) : [];
    const expectedHeaderNorm = expectedHeader.map(normHeader);
    const headerMatches = receivedHeader.length === expectedHeaderNorm.length
      && expectedHeaderNorm.every((h, i) => receivedHeader[i] === h);
    if (!headerMatches) {
      return res.status(400).json({
        message: 'Invalid template. Column headers must match the template exactly.',
        expectedHeader,
        receivedHeader: Array.isArray(rows[0]) ? rows[0].map((h) => String(h ?? '')) : []
      });
    }

    // Determine the index of the group name column, optional line item column, and optional item count column.
    const header = rows[0].map((h) => String(h).trim().toLowerCase());
    let nameIdx = header.findIndex((h) => ['group name', 'name', 'item group', 'itemgroup', 'pallet group', 'palletgroup', 'pallet description', 'palletdescription', 'description'].includes(h));
    if (nameIdx < 0) nameIdx = 0; // fallback: first column
    const lineItemIdx = header.findIndex((h) => ['line item','lineitem','line','pallet id','palletid','pallet'].includes(h));
    const countIdx = header.findIndex((h) => ['item count', 'count', 'items', 'itemcount'].includes(h));

    // Optional item columns if user uploads a combined sheet (Pallet Group + Items)
    const itemCodeIdx = header.findIndex(h => ['item code','itemcode'].includes(h));
    const itemDescIdx = header.findIndex(h => ['item description','itemdescription','description'].includes(h));
    const itemColorIdx = header.findIndex(h => ['color'].includes(h));
    const itemPackIdx = header.findIndex(h => ['pack size','packsize'].includes(h));

    const seen = new Set();
    const rowsParsed = [];
    const lineItemByGroup = new Map(); // lower(groupName) -> lineItem string
    const errors = [];

    for (let i = 1; i < rows.length; i++) {
      const row = rows[i];
      const raw = (row[nameIdx] ?? '').toString().trim();
      const rowNum = i + 1;
      if (!raw) {
        // Skip blank lines silently
        continue;
      }
      const li = lineItemIdx >= 0 ? String(row[lineItemIdx] ?? '').trim() : '';
      // Track line item per group, and enforce consistency within the file
      const keyLower = raw.toLowerCase();
      if (lineItemByGroup.has(keyLower)) {
        const prevLI = lineItemByGroup.get(keyLower) || '';
        if (li && prevLI && prevLI !== li) {
          errors.push({ rowNum, name: raw, errors: ['Pallet ID must be the same for all rows of this Pallet Description in the file'] });
        }
        if (li && !prevLI) lineItemByGroup.set(keyLower, li);
      } else {
        lineItemByGroup.set(keyLower, li);
      }
      const key = raw.toLowerCase();
      if (seen.has(key)) {
        // Skip duplicates within the same file silently
        continue;
      }
      seen.add(key);
      const uploadedCount = countIdx >= 0 ? Number(row[countIdx]) : 0;
      const itemCount = Number.isFinite(uploadedCount) ? uploadedCount : 0;
      rowsParsed.push({ name: raw, rowNum, itemCount, lineItem: li });
    }

    // Check existing names in DB
    const names = rowsParsed.map(r => r.name);
    const existing = await ItemGroup.find({ name: { $in: names } }).lean();
    const existingSet = new Set(existing.map((g) => g.name.toLowerCase()));
    const toCreate = rowsParsed.filter((r) => !existingSet.has(r.name.toLowerCase()));

    // Validate consistency against existing groups with lineItem
    for (const g of existing) {
      const lower = g.name.toLowerCase();
      const liSeen = (lineItemByGroup.get(lower) || '').trim();
      if (liSeen && (g.lineItem || '').trim() && (g.lineItem || '').trim() !== liSeen) {
        errors.push({ rowNum: '-', name: g.name, errors: ['Pallet ID mismatch with existing Pallet Description'] });
      }
    }

    // Existing names are simply skipped; no errors recorded

    // Prepare item import diff but do not write yet
    let itemsCreated = 0;
    let itemsUpdated = 0;
    let itemsSkipped = 0;
    let pendingItemOps = [];
    const hasItemColumns = itemCodeIdx >= 0;
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
        const rawName = (row[nameIdx] ?? '').toString().trim();
        const li = lineItemIdx >= 0 ? String(row[lineItemIdx] ?? '').trim() : '';
        const itemCode = (row[itemCodeIdx] ?? '').toString().trim();
        if (!rawName || !itemCode) { continue; }
        const desc = itemDescIdx >= 0 ? (row[itemDescIdx] ?? '').toString().trim() : '';
        const color = itemColorIdx >= 0 ? (row[itemColorIdx] ?? '').toString().trim() : '';
        const packVal = itemPackIdx >= 0 ? Number(String(row[itemPackIdx]).toString().trim()) : NaN;
        const rowNum = i + 1;
        itemParsed.push({ groupName: rawName, lineItem: li, itemCode, description: desc, color, packSize: packVal, rowNum });
      }
      // Dedupe within file by (itemCode, groupName)
      const byKey = new Map();
      for (const r of itemParsed) {
        const gdoc = nameByLower.get(r.groupName.toLowerCase());
        if (!gdoc || gdoc.active === false) { continue; }
        // Validate line item consistency per group
        const liSeen = (lineItemByGroup.get(r.groupName.toLowerCase()) || '').trim();
        if (liSeen && r.lineItem && r.lineItem.trim() && liSeen !== r.lineItem.trim()) {
          errors.push({ rowNum: r.rowNum, itemCode: r.itemCode, itemGroup: gdoc.name, errors: ['Pallet ID must be the same for all rows of this Pallet Description in the file'] });
          continue;
        }
        if (!Number.isFinite(r.packSize)) { errors.push({ rowNum: r.rowNum, itemCode: r.itemCode, errors: ['Pack Size required'] }); continue; }
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
            errors.push({ rowNum: r.rowNum, itemCode: r.itemCode, itemGroup: gdoc.name, errors: ['Duplicate Item Code in the same Pallet Description in file with different details'] });
            // last occurrence wins
            byKey.set(key, { ...r, groupName: gdoc.name });
          }
        }
      }
      // Defer DB writes until after groups are inserted; counts will be computed in pending ops below
      // After reading items, update lineItem for groups if provided and not conflicting (prepare ops)
      for (const [lowerName, li] of lineItemByGroup.entries()) {
        const gdoc = await ItemGroup.findOne({ name: new RegExp(`^${lowerName}$`, 'i') }).lean();
        if (!gdoc) continue;
        if (li && (!gdoc.lineItem || !gdoc.lineItem.trim())) {
          pendingItemOps.push({ type: 'updateLineItem', id: gdoc._id, lineItem: li });
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
      toCreate.map((r) => ({ name: r.name, lineItem: (r.lineItem || '').trim() })),
      { ordered: false }
    ).catch(() => []);

    for (const op of pendingItemOps) {
      if (op.type === 'updateLineItem') {
        await ItemGroup.updateOne({ _id: op.id }, { $set: { lineItem: op.lineItem } });
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
