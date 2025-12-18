import Item from '../models/Item.js';
import StockMovement from '../models/StockMovement.js';
import { parseWorkbookBuffer } from '../utils/excel.js';
import ImportLog from '../models/ImportLog.js';
import WarehouseStock from '../models/WarehouseStock.js';
import Shipment from '../models/Shipment.js';

export const previewImport = async (req, res) => {
  if (!req.file) return res.status(400).json({ message: 'file is required' });
  const { rows, errors } = parseWorkbookBuffer(req.file.buffer);
  // Required fields per-row: poNumber, itemCode, totalQty, packSize
  for (const r of rows) {
    const rowErrs = [];
    if (!r.poNumber) rowErrs.push('PO# is required');
    if (!r.itemCode) rowErrs.push('Item Code is required');
    if (!(Number.isFinite(r.totalQty))) rowErrs.push('Total Qty is required and must be a number');
    if (!(Number.isFinite(r.packSize)) || r.packSize <= 0) rowErrs.push('Pack Size is required and must be > 0');
    if (rowErrs.length) errors.push({ rowNum: r.rowNum, itemCode: r.itemCode, errors: rowErrs });
  }
  // Enforce: item must be pre-registered and enabled
  const codes = Array.from(new Set(rows.map(r => r.itemCode).filter(Boolean)));
  if (codes.length) {
    const existing = await Item.find({ itemCode: { $in: codes } }).select('itemCode enabled').lean();
    const existMap = new Map(existing.map(e => [e.itemCode, e]));
    for (const r of rows) {
      const rec = r.itemCode ? existMap.get(r.itemCode) : null;
      if (r.itemCode && !rec) {
        errors.push({ rowNum: r.rowNum, itemCode: r.itemCode, errors: ['item not registered'] });
      } else if (rec && rec.enabled === false) {
        errors.push({ rowNum: r.rowNum, itemCode: r.itemCode, errors: ['This item is currently disabled. Remove this row from the file.'] });
      }
    }
  }
  // Duplicate detection by (poNumber + itemCode)
  const keys = rows
    .filter(r => r.poNumber && r.itemCode)
    .map(r => ({ poNumber: r.poNumber, itemCode: r.itemCode }));
  // In-file duplicate detection by itemCode
  const codeCounts = rows.reduce((acc, r) => {
    const c = (r.itemCode || '').toString();
    if (!c) return acc;
    acc[c] = (acc[c] || 0) + 1;
    return acc;
  }, {});
  for (const r of rows) {
    const c = (r.itemCode || '').toString();
    if (c && codeCounts[c] > 1) {
      errors.push({ rowNum: r.rowNum, itemCode: r.itemCode, errors: ['Duplicate Item Code in file'] });
    }
  }
  let duplicates = [];
  if (keys.length) {
    const found = await ImportLog.find({ $or: keys }).select('poNumber itemCode').lean();
    const set = new Set(found.map(f => `${f.poNumber}::${f.itemCode}`));
    duplicates = rows
      .filter(r => r.poNumber && r.itemCode && set.has(`${r.poNumber}::${r.itemCode}`))
      .map(r => ({ poNumber: r.poNumber, itemCode: r.itemCode }));
  }
  return res.json({ rows, errors, totalRows: rows.length, errorCount: errors.length, duplicates, duplicateCount: duplicates.length });
};

export const commitImport = async (req, res) => {
  if (!req.file) return res.status(400).json({ message: 'file is required' });
  const type = (req.query.type || '').toString();
  if (!['stock_in','initial','orders'].includes(type)) return res.status(400).json({ message: 'invalid type' });
  const allowDuplicates = false; // duplicates are not allowed
  const status = (req.query.status || 'Delivered').toString();
  const warehouseId = (req.query.warehouseId || '').toString();
  const estDeliveryDate = req.query.estDeliveryDate ? new Date(req.query.estDeliveryDate.toString()) : null;

  if (['stock_in','initial'].includes(type)) {
    if (!['Delivered','On-Water'].includes(status)) return res.status(400).json({ message: 'invalid status' });
    if (!warehouseId) return res.status(400).json({ message: 'warehouseId required' });
    if (status === 'On-Water' && !estDeliveryDate) return res.status(400).json({ message: 'estDeliveryDate required for On-Water' });
  }

  const { rows, errors } = parseWorkbookBuffer(req.file.buffer);
  // Required fields per-row: poNumber, itemCode, totalQty, packSize
  for (const r of rows) {
    const rowErrs = [];
    if (!r.poNumber) rowErrs.push('PO# is required');
    if (!r.itemCode) rowErrs.push('Item Code is required');
    if (!(Number.isFinite(r.totalQty))) rowErrs.push('Total Qty is required and must be a number');
    if (!(Number.isFinite(r.packSize)) || r.packSize <= 0) rowErrs.push('Pack Size is required and must be > 0');
    if (rowErrs.length) errors.push({ rowNum: r.rowNum, itemCode: r.itemCode, errors: rowErrs });
  }
  if (errors.length) return res.status(400).json({ message: 'validation errors', errors });

  // Enforce: item must be pre-registered and enabled
  const codes = Array.from(new Set(rows.map(r => r.itemCode).filter(Boolean)));
  if (codes.length) {
    const existing = await Item.find({ itemCode: { $in: codes } }).select('itemCode enabled').lean();
    const existMap = new Map(existing.map(e => [e.itemCode, e]));
    const notRegistered = rows.filter(r => r.itemCode && !existMap.has(r.itemCode)).map(r => ({ rowNum: r.rowNum, itemCode: r.itemCode }));
    if (notRegistered.length) {
      return res.status(400).json({ message: 'items not registered', notRegistered, count: notRegistered.length });
    }
    const disabled = rows.filter(r => {
      const rec = existMap.get(r.itemCode);
      return rec && rec.enabled === false;
    }).map(r => ({ rowNum: r.rowNum, itemCode: r.itemCode }));
    if (disabled.length) {
      return res.status(400).json({ message: 'some items are disabled; remove these rows and re-import', disabled, count: disabled.length });
    }
  }

  // Duplicate detection
  const keys = rows
    .filter(r => r.poNumber && r.itemCode)
    .map(r => ({ poNumber: r.poNumber, itemCode: r.itemCode }));
  // In-file duplicate detection by itemCode
  const codeCounts = rows.reduce((acc, r) => {
    const c = (r.itemCode || '').toString();
    if (!c) return acc;
    acc[c] = (acc[c] || 0) + 1;
    return acc;
  }, {});
  const fileDupCodes = Object.keys(codeCounts).filter(c => codeCounts[c] > 1);
  if (fileDupCodes.length) {
    const fileDupRows = rows.filter(r => fileDupCodes.includes((r.itemCode || '').toString())).map(r => ({ rowNum: r.rowNum, itemCode: r.itemCode }));
    return res.status(400).json({ message: 'duplicate item codes in file', duplicates: fileDupRows, duplicateCount: fileDupRows.length });
  }
  let duplicates = [];
  if (keys.length) {
    const found = await ImportLog.find({ $or: keys }).select('poNumber itemCode').lean();
    const set = new Set(found.map(f => `${f.poNumber}::${f.itemCode}`));
    duplicates = rows
      .filter(r => r.poNumber && r.itemCode && set.has(`${r.poNumber}::${r.itemCode}`))
      .map(r => ({ poNumber: r.poNumber, itemCode: r.itemCode }));
  }
  if (duplicates.length) {
    return res.status(409).json({ message: 'duplicate rows detected by PO# + Item Code', duplicates, duplicateCount: duplicates.length });
  }

  let upserted = 0;
  const movements = [];
  const shipmentItems = [];

  for (const r of rows) {
    const { itemCode, totalQty, packSize } = r;
    if (!itemCode || !Number.isFinite(totalQty) || !Number.isFinite(packSize) || packSize <= 0) continue;

    const existing = await Item.findOne({ itemCode });
    if (!existing) continue; // should be blocked earlier; safety
    // For stock_in, increment totalQty; for initial, set absolute; for orders, skip here
    if (type === 'stock_in') {
      if (status === 'Delivered') {
        existing.totalQty = (existing.totalQty || 0) + totalQty;
      }
    } else if (type === 'initial') {
      if (status === 'Delivered') {
        existing.totalQty = totalQty;
      }
    }
    if (existing.packSize !== packSize && Number.isFinite(packSize) && packSize > 0) {
      existing.packSize = packSize;
    }
    await existing.save();
    upserted++;

    if (type === 'stock_in' || type === 'initial') {
      movements.push({ itemCode, qtyPieces: totalQty, packSize });
      if (status === 'Delivered') {
        // Upsert per-warehouse stock
        await WarehouseStock.findOneAndUpdate(
          { itemCode, warehouseId },
          { $inc: { qtyPieces: totalQty } },
          { upsert: true, new: true, setDefaultsOnInsert: true }
        );
      } else if (status === 'On-Water') {
        shipmentItems.push({ itemCode, qtyPieces: totalQty, packSize });
      }
    }
  }

  // Determine most-common PO# across rows for consistent reference
  const poCounts = rows.reduce((acc, r) => {
    const po = (r?.poNumber || '').toString();
    if (!po) return acc;
    acc[po] = (acc[po] || 0) + 1;
    return acc;
  }, {});
  const firstPo = Object.keys(poCounts).sort((a,b)=> poCounts[b]-poCounts[a])[0] || (rows.find(r=>r?.poNumber)?.poNumber) || '';
  if (movements.length && status === 'Delivered') {
    await StockMovement.create({
      type: 'IN',
      reference: firstPo,
      warehouseId,
      items: movements,
      createdBy: req.user?.id,
      notes: `${type}|${status}`
    });
  }

  // If On-Water, create a shipment record (no stock movement yet)
  let createdShipment = null;
  if (shipmentItems.length && status === 'On-Water') {
    createdShipment = await Shipment.create({
      kind: 'import',
      status: 'on_water',
      reference: firstPo,
      warehouseId,
      estDeliveryDate: estDeliveryDate || undefined,
      items: shipmentItems,
      notes: type,
      createdBy: req.user?.id,
    });
  }

  // Save ImportLog entries for monitoring
  const fileName = req.file?.originalname || '';
  const userId = req.user?.id;
  const docs = rows
    .filter(r => r.itemCode)
    .map(r => ({ type, fileName, poNumber: r.poNumber || '', itemCode: r.itemCode, totalQty: r.totalQty ?? null, packSize: r.packSize ?? null, userId }));
  if (docs.length) await ImportLog.insertMany(docs);

  return res.json({ message: 'imported', upserted, movementItems: status === 'Delivered' ? movements.length : 0, onWaterItems: status === 'On-Water' ? shipmentItems.length : 0, shipmentId: createdShipment?._id || null });
};
