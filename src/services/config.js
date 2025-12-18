import Setting from '../models/Setting.js';

export const getPacksPerPalletAsync = async () => {
  const rec = await Setting.findOne({ key: 'packsPerPallet' }).lean();
  const fromDb = Number(rec?.value);
  if (Number.isFinite(fromDb) && fromDb > 0) return fromDb;
  const envVal = Number(process.env.DEFAULT_PACKS_PER_PALLET || 50);
  return Number.isFinite(envVal) && envVal > 0 ? envVal : 50;
};

export const getAllSettings = async () => {
  const packsPerPallet = await getPacksPerPalletAsync();
  return { packsPerPallet };
};

export const setPacksPerPallet = async (val) => {
  const num = Number(val);
  if (!Number.isFinite(num) || num <= 0) throw new Error('packsPerPallet must be > 0');
  await Setting.findOneAndUpdate(
    { key: 'packsPerPallet' },
    { key: 'packsPerPallet', value: num },
    { upsert: true }
  );
  return { packsPerPallet: num };
};
