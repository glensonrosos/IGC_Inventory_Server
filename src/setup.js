import mongoose from 'mongoose';
import dotenv from 'dotenv';
import User from './models/User.js';
import bcrypt from 'bcryptjs';
import Item from './models/Item.js';
import PalletGroupTxn from './models/PalletGroupTxn.js';
import FulfilledOrderImport from './models/FulfilledOrderImport.js';
import UnfulfilledOrder from './models/UnfulfilledOrder.js';

dotenv.config();

export const connectDB = async () => {
  const uri = process.env.MONGO_URI || 'mongodb://localhost:27017/igc_inventory';
  mongoose.set('strictQuery', true);
  await mongoose.connect(uri);
  console.log('MongoDB connected');
};

export const ensureAdmin = async () => {
  const envUsername = process.env.ADMIN_USERNAME;
  const envPassword = process.env.ADMIN_PASSWORD;
  const legacyEmail = process.env.ADMIN_EMAIL; // backward compat
  const username = envUsername || (legacyEmail ? String(legacyEmail).split('@')[0] : '');
  const password = envPassword;
  if (!username || !password) return;
  let user = await User.findOne({ username });
  if (!user) {
    const hash = await bcrypt.hash(password, 10);
    user = await User.create({ username, password: hash, role: 'admin' });
    console.log('Created default admin (username):', username);
  }
};

export const syncModelIndexes = async () => {
  // Ensure DB indexes match current schemas (drops obsolete ones like unique itemCode)
  await Item.syncIndexes();
  // Drop legacy unique user.email index (causes duplicate key when email is null)
  try {
    await User.collection.dropIndex('email_1');
    console.log('Dropped legacy index on User.email');
  } catch (e) {
    if (e && e.codeName !== 'IndexNotFound' && e.message && !String(e.message).includes('index not found')) {
      console.warn('dropIndex warning (User):', e.message || e);
    }
  }
  await User.syncIndexes();
  // Drop legacy unique txn index if it exists, then sync to non-unique compound index
  try {
    // old index name from error logs: poNumber_1_groupName_1_warehouseId_1
    await PalletGroupTxn.collection.dropIndex('poNumber_1_groupName_1_warehouseId_1');
    console.log('Dropped legacy unique index on PalletGroupTxn');
  } catch (e) {
    if (e && e.codeName !== 'IndexNotFound' && e.message && !String(e.message).includes('index not found')) {
      console.warn('dropIndex warning (PalletGroupTxn):', e.message || e);
    }
  }
  await PalletGroupTxn.syncIndexes();

  await FulfilledOrderImport.syncIndexes();
  await UnfulfilledOrder.syncIndexes();
};
