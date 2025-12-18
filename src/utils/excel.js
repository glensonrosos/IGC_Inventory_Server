import XLSX from 'xlsx';
import { computePacksOnHand, computePalletsOnHand, getPacksPerPallet } from './calc.js';

const headerMap = {
  'po #': 'poNumber',
  'po#': 'poNumber',
  'ponumber': 'poNumber',
  'item code': 'itemCode',
  'itemcode': 'itemCode',
  'item group': 'itemGroup',
  'itemgroup': 'itemGroup',
  'item description': 'description',
  'description': 'description',
  'color': 'color',
  'total qty': 'totalQty',
  'totalqty': 'totalQty',
  'pack size': 'packSize',
  'packsize': 'packSize',
  '# of pallets': 'palletCount',
  'pallets': 'palletCount',
  'palletcount': 'palletCount'
};

const normalizeRow = (row) => {
  const out = {};
  for (const [k, v] of Object.entries(row)) {
    const key = String(k).trim().toLowerCase();
    const mapped = headerMap[key];
    if (mapped) out[mapped] = v;
  }
  return out;
};

const toNumber = (v) => {
  if (v === '' || v == null) return NaN;
  const n = Number(v);
  return Number.isFinite(n) ? n : NaN;
};

export const parseWorkbookBuffer = (buffer) => {
  const wb = XLSX.read(buffer, { type: 'buffer' });
  const sheet = wb.Sheets[wb.SheetNames[0]];
  const rows = XLSX.utils.sheet_to_json(sheet, { defval: '' });
  const packsPerPallet = getPacksPerPallet();

  const parsed = [];
  const errors = [];

  rows.forEach((raw, idx) => {
    const r = normalizeRow(raw);
    const rowNum = idx + 2; // header assumed row 1

    const itemCode = (r.itemCode || '').toString().trim();
    const poNumber = (r.poNumber || '').toString().trim();
    const itemGroup = (r.itemGroup || '').toString().trim();
    const description = (r.description || '').toString().trim();
    const color = (r.color || '').toString().trim();

    const totalQty = toNumber(r.totalQty);
    const packSize = toNumber(r.packSize);
    const palletCount = toNumber(r.palletCount);

    const rowErrors = [];
    if (!itemCode) rowErrors.push('itemCode required');
    if (!Number.isFinite(totalQty)) rowErrors.push('totalQty must be numeric');
    if (!Number.isFinite(packSize)) rowErrors.push('packSize must be numeric');
    if (Number.isFinite(packSize) && packSize === 0) rowErrors.push('packSize must be > 0');

    const packsOnHand = Number.isFinite(totalQty) && Number.isFinite(packSize) && packSize > 0
      ? Math.floor(totalQty / packSize)
      : 0;
    const palletsEstimate = computePalletsOnHand(packsOnHand, packsPerPallet);

    const parsedRow = {
      rowNum,
      poNumber,
      itemCode,
      itemGroup,
      description,
      color,
      totalQty: Number.isFinite(totalQty) ? totalQty : null,
      packSize: Number.isFinite(packSize) ? packSize : null,
      palletCount: Number.isFinite(palletCount) ? palletCount : null,
      packsOnHand,
      palletsEstimate
    };

    if (rowErrors.length) {
      errors.push({ rowNum, itemCode, errors: rowErrors });
    }
    parsed.push(parsedRow);
  });

  return { rows: parsed, errors };
};
