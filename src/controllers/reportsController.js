import XLSX from 'xlsx';
import Item from '../models/Item.js';
import Warehouse from '../models/Warehouse.js';
import Shipment from '../models/Shipment.js';
import PalletGroupStock from '../models/PalletGroupStock.js';
import OnProcessPallet from '../models/OnProcessPallet.js';
import PalletGroupTxn from '../models/PalletGroupTxn.js';
import ItemGroup from '../models/ItemGroup.js';
import { computePacksOnHand, computePalletsOnHand, getPacksPerPallet } from '../utils/calc.js';

export const inventoryReport = async (req, res) => {
  const { group, color, lowStock, q } = req.body || {};
  const filter = {};
  if (group) filter.itemGroup = group;
  if (color) filter.color = color;
  if (q) filter.$or = [{ itemCode: new RegExp(q, 'i') }, { description: new RegExp(q, 'i') }];
  const rows = await Item.find(filter).sort({ itemCode: 1 }).lean();
  const ppp = getPacksPerPallet();
  const data = rows.map(r => {
    const packs = computePacksOnHand(r.totalQty, r.packSize);
    const pallets = computePalletsOnHand(packs, ppp);
    const isLow = r.lowStockThreshold > 0 && r.totalQty <= r.lowStockThreshold;
    return {
      itemCode: r.itemCode,
      itemGroup: r.itemGroup,
      description: r.description,
      color: r.color,
      totalQty: r.totalQty,
      packSize: r.packSize,
      packsOnHand: packs,
      palletsOnHand: pallets,
      lowStock: isLow
    };
  });
  const result = lowStock ? data.filter(d => d.lowStock) : data;
  res.json({ items: result, count: result.length });
};

export const exportReport = async (req, res) => {
  const { format = 'xlsx', group, color, lowStock, q } = req.query;
  const body = { group, color, lowStock: lowStock === 'true', q };
  // reuse logic
  const tmpReq = { body };
  const tmpRes = {
    _payload: null,
    json(v) { this._payload = v; }
  };
  await inventoryReport(tmpReq, tmpRes);
  const items = tmpRes._payload.items || [];

  const ws = XLSX.utils.json_to_sheet(items);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'Inventory');

  if (format === 'csv') {
    const csv = XLSX.utils.sheet_to_csv(ws);
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename="inventory.csv"');
    return res.send(csv);
  }
  const buf = XLSX.write(wb, { bookType: 'xlsx', type: 'buffer' });
  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition', 'attachment; filename="inventory.xlsx"');
  return res.send(buf);
};

export const palletSummaryByGroup = async (req, res) => {
  // Warehouses
  const warehousesRaw = await Warehouse.find({}).select('name isPrimary').sort({ name: 1 }).lean();
  const warehouses = (warehousesRaw || []).map((w) => ({ _id: String(w._id), name: w.name, isPrimary: Boolean(w.isPrimary) }));
  const whIdStrs = warehouses.map(w => String(w._id));

  // Warehouse totals (pallet inventory): group by Pallet Group + warehouseId
  const whAgg = await PalletGroupStock.aggregate([
    { $addFields: { warehouseIdStr: { $toString: '$warehouseId' } } },
    { $match: { warehouseIdStr: { $in: whIdStrs } } },
    { $group: { _id: { itemGroup: '$groupName', warehouseId: '$warehouseIdStr' }, pallets: { $sum: '$pallets' } } },
  ]);
  const byGroup = new Map();
  for (const r of whAgg) {
    const g = String(r?._id?.itemGroup || '');
    if (!g) continue;
    if (!byGroup.has(g)) byGroup.set(g, { itemGroup: g, warehouses: {}, onProcessQty: 0, onWaterQty: 0 });
    byGroup.get(g).warehouses[String(r._id.warehouseId)] = Number(r.pallets || 0);
  }

  // On-Process totals (Option B): pallets not yet transferred still count as on-process.
  // Definition: finished + remaining = totalPallet - transferredPallet.
  const opAgg = await OnProcessPallet.aggregate([
    { $match: { status: { $ne: 'cancelled' } } },
    { $group: { _id: { itemGroup: '$groupName' }, pallets: { $sum: { $subtract: ['$totalPallet', { $ifNull: ['$transferredPallet', 0] }] } } } },
  ]);
  for (const r of opAgg) {
    const g = String(r?._id?.itemGroup || '');
    if (!g) continue;
    if (!byGroup.has(g)) byGroup.set(g, { itemGroup: g, warehouses: {}, onProcessQty: 0, onWaterQty: 0 });
    byGroup.get(g).onProcessQty = Number(r.pallets || 0);
  }

  // On-Water totals (pallet inventory): parse pallet-group segments in Shipment.notes for on-water shipments.
  // Notes contain chunks like: "pallet-group:<groupName>; pallets:<count>" (possibly separated by " | ").
  const ships = await Shipment.find({ status: 'on_water', notes: { $regex: 'pallet-group:', $options: 'i' } })
    .select('notes')
    .lean();
  const owMap = new Map();
  const re = /pallet-group:([^;|]+);\s*pallets:(\d+)/gi;
  for (const s of ships) {
    const text = String(s?.notes || '');
    let m;
    while ((m = re.exec(text)) !== null) {
      const groupName = String(m[1] || '').trim();
      const pallets = Number(m[2] || 0);
      if (!groupName || !Number.isFinite(pallets) || pallets <= 0) continue;
      owMap.set(groupName, (owMap.get(groupName) || 0) + pallets);
    }
  }
  for (const [g, pallets] of owMap.entries()) {
    if (!byGroup.has(g)) byGroup.set(g, { itemGroup: g, warehouses: {}, onProcessQty: 0, onWaterQty: 0 });
    byGroup.get(g).onWaterQty = Number(pallets || 0);
  }

  const rows = Array.from(byGroup.values()).sort((a, b) => String(a.itemGroup).localeCompare(String(b.itemGroup)));
  res.setHeader('Cache-Control', 'no-store');
  res.json({ warehouses, rows });
};

export const palletSalesReport = async (req, res) => {
  const { from, to, top = '20' } = req.query || {};

  const fromStr = String(from || '').trim();
  const toStr = String(to || '').trim();
  const isYmd = (s) => /^\d{4}-\d{2}-\d{2}$/.test(String(s || '').trim());
  if (!isYmd(fromStr) || !isYmd(toStr)) {
    return res.status(400).json({ message: 'from and to are required (YYYY-MM-DD)' });
  }

  const fromDt = new Date(`${fromStr}T00:00:00.000Z`);
  const toDt = new Date(`${toStr}T23:59:59.999Z`);
  if (Number.isNaN(fromDt.getTime()) || Number.isNaN(toDt.getTime())) {
    return res.status(400).json({ message: 'Invalid date range' });
  }

  const topN = Math.max(1, Math.min(200, Math.floor(Number(top || 20) || 20)));

  const groups = await ItemGroup.find({ active: true }).select('name lineItem').lean();
  const palletIdByGroupLower = new Map((groups || []).map((g) => [String(g?.name || '').trim().toLowerCase(), String(g?.lineItem || '').trim()]));

  const agg = await PalletGroupTxn.aggregate([
    {
      $match: {
        status: 'Delivered',
        committedAt: { $gte: fromDt, $lte: toDt },
        palletsDelta: { $gt: 0 },
      },
    },
    {
      $group: {
        _id: { groupName: '$groupName' },
        soldPallets: { $sum: '$palletsDelta' },
      },
    },
  ]);

  const soldByGroupLower = new Map();
  for (const r of agg || []) {
    const g = String(r?._id?.groupName || '').trim();
    const sold = Number(r?.soldPallets || 0);
    if (!g || !Number.isFinite(sold) || sold <= 0) continue;
    soldByGroupLower.set(g.toLowerCase(), sold);
  }

  const topSelling = Array.from(soldByGroupLower.entries())
    .map(([groupLower, soldPallets]) => {
      const groupName = (groups || []).find((g) => String(g?.name || '').trim().toLowerCase() === groupLower)?.name || '';
      const palletId = palletIdByGroupLower.get(groupLower) || '';
      return { palletId, groupName, soldPallets };
    })
    .sort((a, b) => Number(b.soldPallets || 0) - Number(a.soldPallets || 0))
    .slice(0, topN);

  const nonPerforming = (groups || [])
    .map((g) => {
      const groupName = String(g?.name || '').trim();
      const groupLower = groupName.toLowerCase();
      const palletId = String(g?.lineItem || '').trim();
      const soldPallets = Number(soldByGroupLower.get(groupLower) || 0);
      return { palletId, groupName, soldPallets, reason: soldPallets <= 0 ? '0 sold in selected date range' : '' };
    })
    .filter((r) => Number(r.soldPallets || 0) <= 0)
    .sort((a, b) => String(a.groupName || '').localeCompare(String(b.groupName || '')));

  res.setHeader('Cache-Control', 'no-store');
  return res.json({ from: fromStr, to: toStr, topSelling, nonPerforming });
};
