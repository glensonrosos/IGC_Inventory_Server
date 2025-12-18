import XLSX from 'xlsx';
import { parseWorkbookBuffer } from '../src/utils/excel.js';

const makeSheet = (rows) => {
  const ws = XLSX.utils.json_to_sheet(rows);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'Sheet1');
  const buf = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });
  return buf;
};

describe('excel parser', () => {
  test('maps headers and computes preview fields', () => {
    const buf = makeSheet([
      { 'PO #': 'PO-1', 'Item Code': 'ABC', 'Item Group': 'G1', 'Item Description': 'Desc', 'Color': 'Red', 'Total Qty': 100, 'Pack Size': 20, '# of Pallets': 2 }
    ]);
    const { rows, errors } = parseWorkbookBuffer(buf);
    expect(errors).toHaveLength(0);
    expect(rows).toHaveLength(1);
    const r = rows[0];
    expect(r.poNumber).toBe('PO-1');
    expect(r.itemCode).toBe('ABC');
    expect(r.totalQty).toBe(100);
    expect(r.packSize).toBe(20);
    expect(r.packsOnHand).toBe(5);
    expect(r.palletsEstimate).toBeGreaterThan(0);
  });

  test('validates required itemCode and numeric fields', () => {
    const buf = makeSheet([
      { 'PO #': 'PO-2', 'Item Code': '', 'Total Qty': 'xx', 'Pack Size': 0 }
    ]);
    const { rows, errors } = parseWorkbookBuffer(buf);
    expect(rows).toHaveLength(1);
    expect(errors.length).toBeGreaterThan(0);
    const errs = errors[0].errors.join(' ');
    expect(errs).toMatch(/itemCode required/);
    expect(errs).toMatch(/totalQty must be numeric/);
    expect(errs).toMatch(/packSize must be > 0/);
  });
});
