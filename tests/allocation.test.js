import mongoose from 'mongoose';
import { MongoMemoryServer } from 'mongodb-memory-server';
import Item from '../src/models/Item.js';
import Pallet from '../src/models/Pallet.js';
import { autoAllocateOrder } from '../src/services/allocation.js';

let mongod;

beforeAll(async () => {
  mongod = await MongoMemoryServer.create();
  const uri = mongod.getUri();
  await mongoose.connect(uri);
});

afterAll(async () => {
  await mongoose.disconnect();
  if (mongod) await mongod.stop();
});

afterEach(async () => {
  await Item.deleteMany({});
  await Pallet.deleteMany({});
});

describe('allocation service', () => {
  test('assigns full pallets FIFO and decrements stock for packs/pieces', async () => {
    // Seed item and pallets
    await Item.create({ itemCode: 'A', itemGroup: 'G', description: 'Item A', color: 'red', totalQty: 200, packSize: 10 });
    await Pallet.create({ palletId: 'P1', status: 'available', composition: [{ itemCode: 'A', packSize: 10, packs: 5, pieces: 50 }], totalPacks: 5, totalPieces: 50 });
    await Pallet.create({ palletId: 'P2', status: 'available', composition: [{ itemCode: 'A', packSize: 10, packs: 6, pieces: 60 }], totalPacks: 6, totalPieces: 60 });

    const order = { orderLines: [ { itemCode: 'A', packSize: 10, requestedPallets: 1, packsRequested: 5, piecesRequested: 0 } ] };

    const { assignments, shortages } = await autoAllocateOrder(order);

    expect(shortages).toHaveLength(0);
    expect(assignments).toHaveLength(1);
    expect(assignments[0].palletIds.length).toBe(1);

    // First created pallet should be reserved (FIFO)
    const p1 = await Pallet.findOne({ palletId: 'P1' });
    expect(p1.status).toBe('reserved');

    // Item stock decremented by packsRequested*packSize = 50
    const item = await Item.findOne({ itemCode: 'A' });
    expect(item.totalQty).toBe(150);
  });

  test('shortage is reported when stock insufficient', async () => {
    await Item.create({ itemCode: 'B', totalQty: 10, packSize: 5 });
    const order = { orderLines: [ { itemCode: 'B', packSize: 5, packsRequested: 5, piecesRequested: 0 } ] }; // need 25 pieces
    const { shortages } = await autoAllocateOrder(order);
    expect(shortages).toHaveLength(1);
    expect(shortages[0].missingPieces).toBe(15);
    const item = await Item.findOne({ itemCode: 'B' });
    expect(item.totalQty).toBe(0);
  });
});
