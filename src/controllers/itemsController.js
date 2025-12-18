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
  const { itemCode, itemGroup, description, color, totalQty, packSize, enabled } = req.body || {};
  if (!itemCode) return res.status(400).json({ message: 'itemCode required' });
  if (packSize != null && Number(packSize) < 0) return res.status(400).json({ message: 'packSize must be >= 0' });
  if (itemGroup) {
    const groupDoc = await ItemGroup.findOne({ name: itemGroup }).lean();
    if (!groupDoc) return res.status(400).json({ message: 'Pallet Description not found' });
    if (groupDoc.active === false) return res.status(400).json({ message: 'Pallet Description is inactive' });
  }
  const exists = await Item.findOne({ itemCode, itemGroup: itemGroup || '' });
  if (exists) return res.status(409).json({ message: 'Item already exists in this pallet description' });
  const doc = await Item.create({
    itemCode,
    itemGroup: itemGroup || '',
    description: description || '',
    color: color || '',
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
  const allowed = ['itemGroup','description','color','totalQty','packSize','enabled'];
  const updates = {};
  for (const k of allowed) if (k in req.body) updates[k] = req.body[k];
  if ('packSize' in updates && Number(updates.packSize) <= 0) return res.status(400).json({ message: 'packSize must be > 0' });
  const doc = await Item.findOneAndUpdate({ itemCode }, updates, { new: true });
  if (!doc) return res.status(404).json({ message: 'Not found' });
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
        const reason = found && found.active === false ? 'Item Group is inactive' : 'Item Group not registered';
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
        if (Number(existing.packSize) !== Number(r.packSize)) changes.packSize = Number(r.packSize);
        if (typeof r.enabled === 'boolean' && existing.enabled !== r.enabled) changes.enabled = r.enabled;
        if (Object.keys(changes).length) { await Item.updateOne({ _id: existing._id }, { $set: changes }); updated++; }
        else { skipped++; }
      } else {
        await Item.create({ itemCode: r.itemCode, itemGroup: r.itemGroup, description: r.description, color: r.color, totalQty: 0, packSize: Number(r.packSize), enabled: (r.enabled === undefined ? true : r.enabled) });
        created++;
      }
    }

    res.json({ created, updated, skipped, errorCount: errors.length, errors });
  } catch (e) {
    res.status(500).json({ message: 'failed to parse file', error: String(e?.message || e) });
  }
};
