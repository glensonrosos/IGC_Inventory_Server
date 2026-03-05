import Item from '../models/Item.js';
import ItemGroup from '../models/ItemGroup.js';
import XLSX from 'xlsx';
import { computePacksOnHand, computePalletsOnHand, getPacksPerPallet } from '../utils/calc.js';

export const listItems = async (req, res) => {
  const { group, color, q, includeDisabled } = req.query;
  const filter = {};
  if (String(includeDisabled) !== '1') filter.enabled = { $ne: false };
  if (group) filter.itemGroup = group;
  if (color) filter.color = color;
  if (q) filter.$or = [
    { itemCode: new RegExp(q, 'i') },
    { description: new RegExp(q, 'i') }
  ];

  const items = await Item.find(filter).sort({ itemCode: 1 }).lean();
  const packsPerPallet = getPacksPerPallet();
  const mapped = items.map(it => {
    const packs = computePacksOnHand(it.totalQty, it.packSize);
    const pallets = computePalletsOnHand(packs, packsPerPallet);
    return { ...it, packsOnHand: packs, palletsOnHand: pallets };
  });
  res.json(mapped);
};

export const createItem = async (req, res) => {
  const { itemCode, itemGroup, description, color, price, totalQty, packSize, enabled, upc } = req.body || {};
  if (!itemCode) return res.status(400).json({ message: 'itemCode required' });
  if (packSize != null && Number(packSize) < 0) return res.status(400).json({ message: 'packSize must be >= 0' });
  if (itemGroup) {
    const groupDoc = await ItemGroup.findOne({ name: itemGroup }).lean();
    if (!groupDoc) return res.status(400).json({ message: 'Pallet Group not found' });
    if (groupDoc.active === false) return res.status(400).json({ message: 'Pallet Group is inactive' });
  }
  const existingWithCode = await Item.find({ itemCode }).select('itemCode itemGroup upc').lean();
  const existingSameGroup = existingWithCode.find((it) => String(it.itemGroup || '') === String(itemGroup || ''));
  if (existingSameGroup) return res.status(409).json({ message: 'Item already exists in this pallet group' });
  const normalizedUpc = typeof upc === 'string' ? upc.trim() : '';
  const canonicalUpc = existingWithCode.reduce((acc, it) => {
    const val = String(it.upc || '').trim();
    return acc || val;
  }, '');
  if (canonicalUpc && canonicalUpc !== normalizedUpc) {
    const conflictGroup = existingWithCode.find((it) => String(it.upc || '').trim() === canonicalUpc)?.itemGroup || '';
    const scope = conflictGroup ? ` in pallet group "${conflictGroup}"` : '';
    return res.status(400).json({ message: `Item Code already uses UPC "${canonicalUpc}"${scope}. Please use the same UPC.` });
  }
  const p = Number(price);
  const priceValue = Number.isFinite(p) ? p : 0;
  const doc = await Item.create({
    itemCode,
    itemGroup: itemGroup || '',
    description: description || '',
    color: color || '',
    upc: normalizedUpc,
    price: priceValue,
    totalQty: Number(totalQty) || 0,
    packSize: (packSize == null ? 0 : Number(packSize)),
    enabled: typeof enabled === 'boolean' ? enabled : true,
  });
  res.status(201).json(doc);
};

export const getItem = async (req, res) => {
  const { itemCode } = req.params;
  const doc = await Item.findOne({ itemCode }).lean();
  if (!doc) return res.status(404).json({ message: 'Not found' });
  const packs = computePacksOnHand(doc.totalQty, doc.packSize);
  const pallets = computePalletsOnHand(packs, getPacksPerPallet());
  res.json({ ...doc, packsOnHand: packs, palletsOnHand: pallets });
};

export const updateItem = async (req, res) => {
  const { itemCode } = req.params;
  const allowed = ['itemGroup','description','color','price','totalQty','packSize','enabled','upc'];
  const updates = {};
  for (const k of allowed) if (k in req.body) updates[k] = req.body[k];
  const target = await Item.findOne({ itemCode });
  if (!target) return res.status(404).json({ message: 'Not found' });
  if ('packSize' in updates && Number(updates.packSize) <= 0) return res.status(400).json({ message: 'packSize must be > 0' });
  if ('price' in updates) {
    const p = Number(updates.price);
    updates.price = Number.isFinite(p) ? p : 0;
  }
  if ('upc' in updates) {
    updates.upc = typeof updates.upc === 'string' ? updates.upc.trim() : '';
  }
  const incomingUpc = 'upc' in updates ? updates.upc : String(target.upc || '').trim();
  const siblings = await Item.find({ itemCode }).select('_id upc itemGroup').lean();
  const canonicalUpc = siblings.reduce((acc, it) => {
    if (String(it._id) === String(target._id)) return acc;
    const val = String(it.upc || '').trim();
    return acc || val;
  }, '');
  if (canonicalUpc && canonicalUpc !== incomingUpc) {
    const conflictGroup = siblings.find((it) => String(it._id) !== String(target._id) && String(it.upc || '').trim() === canonicalUpc)?.itemGroup || '';
    const scope = conflictGroup ? ` in pallet group "${conflictGroup}"` : '';
    return res.status(400).json({ message: `Item Code already uses UPC "${canonicalUpc}"${scope}. Please use the same UPC.` });
  }
  const doc = await Item.findOneAndUpdate({ _id: target._id }, updates, { new: true });
  res.json(doc);
};

export const deleteItem = async (req, res) => {
  const { itemCode } = req.params;
  const { group } = req.query;
  const filter = { itemCode };
  if (typeof group === 'string') filter.itemGroup = group;
  const doc = await Item.findOneAndDelete(filter);
  if (!doc) return res.status(404).json({ message: 'Not found' });
  res.json({ ok: true });
};

export const importItemsExcel = async (req, res) => {
  if (!req.file) return res.status(400).json({ message: 'file is required' });
  try {
    const wb = XLSX.read(req.file.buffer, { type: 'buffer' });
    const sheet = wb.Sheets[wb.SheetNames[0]];
    const rows = XLSX.utils.sheet_to_json(sheet, { defval: '' });
    const norm = (s) => String(s || '').trim();
    const toBool = (v) => {
      const s = String(v ?? '').trim().toLowerCase();
      if (s === '1' || s === 'true' || s === 'yes' || s === 'y') return true;
      if (s === '0' || s === 'false' || s === 'no' || s === 'n') return false;
      return undefined;
    };
    const parseRow = (r) => ({
      itemCode: norm(r['Item Code'] ?? r['item code'] ?? r['ItemCode'] ?? r['itemcode']),
      itemGroup: norm(r['Pallet Description'] ?? r['pallet description'] ?? r['Pallet Group'] ?? r['pallet group'] ?? r['Item Group'] ?? r['item group'] ?? r['ItemGroup'] ?? r['itemgroup']),
      description: norm(r['Item Description'] ?? r['item description'] ?? r['Description'] ?? r['description']),
      upc: norm(r['UPC'] ?? r['upc']),
      color: norm(r['Color'] ?? r['color']),
      packSize: Number(String(r['Pack Size'] ?? r['pack size'] ?? r['PackSize'] ?? '').toString().trim()),
      enabled: toBool(r['Enable'] ?? r['enabled'] ?? r['Enabled']),
      rowNum: Number(r.__rowNum || 0)
    });
    const parsed = rows.map(parseRow).map((r, i) => ({ ...r, rowNum: i + 2 }));

    const errors = [];
    const groups = Array.from(new Set(parsed.map(r => r.itemGroup).filter(Boolean)));
    const existingGroups = await ItemGroup.find({ name: { $in: groups } }).select('name active').lean();
    const groupSet = new Set(existingGroups.filter(g => g.active !== false).map(g => g.name));

    // Dedupe within the file by (itemCode,itemGroup)
    const byKey = new Map(); // key -> first row object
    let created = 0, updated = 0, skipped = 0;
    for (const r of parsed) {
      if (!r.itemCode) { errors.push({ rowNum: r.rowNum, itemCode: r.itemCode, errors: ['Item Code required'] }); skipped++; continue; }
      if (!r.itemGroup) { errors.push({ rowNum: r.rowNum, itemCode: r.itemCode, errors: ['Item Group required'] }); skipped++; continue; }
      if (!Number.isFinite(r.packSize)) { errors.push({ rowNum: r.rowNum, itemCode: r.itemCode, errors: ['Pack Size required'] }); skipped++; continue; }
      if (Number(r.packSize) < 0) { errors.push({ rowNum: r.rowNum, itemCode: r.itemCode, errors: ['Pack Size must be >= 0'] }); skipped++; continue; }
      if (!groupSet.has(r.itemGroup)) {
        const found = existingGroups.find(g=>g.name===r.itemGroup);
        const reason = found && found.active === false ? 'Pallet Group is inactive' : 'Pallet Group not registered';
        errors.push({ rowNum: r.rowNum, itemCode: r.itemCode, errors: [reason] }); skipped++; continue; }
      const key = `${r.itemGroup.toLowerCase()}|${r.itemCode.toLowerCase()}`;
      if (!byKey.has(key)) {
        byKey.set(key, r);
      } else {
        const prev = byKey.get(key);
        const same = (prev.description === r.description) && (prev.color === r.color) && (Number(prev.packSize) === Number(r.packSize)) && (prev.enabled === r.enabled);
        if (same) {
          skipped++; // identical duplicate row within file
        } else {
          errors.push({ rowNum: r.rowNum, itemCode: r.itemCode, itemGroup: r.itemGroup, errors: ['Duplicate Item Code in the same Pallet Description in file with different details'] });
          // Prefer last occurrence rule: replace previous with latest values
          byKey.set(key, r);
        }
      }
    }

    // Upsert to DB using deduped set (last occurrence wins)
    for (const r of byKey.values()) {
      const existing = await Item.findOne({ itemCode: r.itemCode, itemGroup: r.itemGroup });
      if (existing) {
        const changes = {};
        if (existing.description !== r.description) changes.description = r.description;
        if (existing.color !== r.color) changes.color = r.color;
        if (String(existing.upc || '') !== String(r.upc || '')) changes.upc = r.upc;
        if (Number(existing.packSize) !== Number(r.packSize)) changes.packSize = Number(r.packSize);
        if (typeof r.enabled === 'boolean' && existing.enabled !== r.enabled) changes.enabled = r.enabled;
        if (Object.keys(changes).length) { await Item.updateOne({ _id: existing._id }, { $set: changes }); updated++; }
        else { skipped++; }
      } else {
        await Item.create({ itemCode: r.itemCode, itemGroup: r.itemGroup, description: r.description, color: r.color, upc: r.upc || '', totalQty: 0, packSize: Number(r.packSize), enabled: (r.enabled === undefined ? true : r.enabled) });
        created++;
      }
    }

    res.json({ created, updated, skipped, errorCount: errors.length, errors });
  } catch (e) {
    res.status(500).json({ message: 'failed to parse file', error: String(e?.message || e) });
  }
};
