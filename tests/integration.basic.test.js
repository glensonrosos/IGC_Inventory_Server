import request from 'supertest';
import mongoose from 'mongoose';
import { MongoMemoryServer } from 'mongodb-memory-server';
import app from '../src/app.js';
import { connectDB, ensureAdmin } from '../src/setup.js';
import XLSX from 'xlsx';

let mongod;
let token;

beforeAll(async () => {
  mongod = await MongoMemoryServer.create();
  const uri = mongod.getUri();
  process.env.MONGO_URI = uri;
  process.env.JWT_SECRET = 'test_jwt';
  process.env.ADMIN_EMAIL = 'admin@example.com';
  process.env.ADMIN_PASSWORD = 'admin123';
  await connectDB();
  await ensureAdmin();
});

afterAll(async () => {
  await mongoose.disconnect();
  if (mongod) await mongod.stop();
});

describe('API integration', () => {
  test('health', async () => {
    const res = await request(app).get('/api/health');
    expect(res.status).toBe(200);
    expect(res.body.status).toBe('ok');
  });

  test('login', async () => {
    const res = await request(app).post('/api/auth/login').send({ email: 'admin@example.com', password: 'admin123' });
    expect(res.status).toBe(200);
    expect(res.body.token).toBeTruthy();
    token = res.body.token;
  });

  test('create and list item', async () => {
    const create = await request(app)
      .post('/api/items')
      .set('Authorization', `Bearer ${token}`)
      .send({ itemCode: 'T1', totalQty: 100, packSize: 10, description: 'Test', itemGroup: 'G', color: 'Red' });
    expect(create.status).toBe(201);

    const list = await request(app)
      .get('/api/items')
      .set('Authorization', `Bearer ${token}`);
    expect(list.status).toBe(200);
    expect(Array.isArray(list.body)).toBe(true);
    expect(list.body.find((x) => x.itemCode === 'T1')).toBeTruthy();
  });

  test('import preview accepts xlsx', async () => {
    // build small workbook in-memory
    const ws = XLSX.utils.json_to_sheet([
      { 'PO #': 'PO-100', 'Item Code': 'IMP1', 'Item Description': 'Import Item', 'Total Qty': 50, 'Pack Size': 10 }
    ]);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, 'Sheet1');
    const buf = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });

    const res = await request(app)
      .post('/api/items/import/preview')
      .set('Authorization', `Bearer ${token}`)
      .attach('file', buf, 'test.xlsx');

    expect(res.status).toBe(200);
    expect(res.body.totalRows).toBe(1);
    expect(res.body.errorCount).toBe(0);
  });
});
