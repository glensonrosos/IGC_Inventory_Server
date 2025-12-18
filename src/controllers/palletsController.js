import Pallet from '../models/Pallet.js';

export const listPallets = async (req, res) => {
  const { status, q } = req.query;
  const filter = {};
  if (status) filter.status = status;
  if (q) filter.$or = [ { palletId: new RegExp(q, 'i') }, { warehouseLocation: new RegExp(q, 'i') } ];
  const pallets = await Pallet.find(filter).sort({ createdAt: -1 }).lean();
  res.json(pallets);
};

export const createPallet = async (req, res) => {
  const { palletId, warehouseLocation = '', composition = [] } = req.body || {};
  if (!palletId) return res.status(400).json({ message: 'palletId required' });
  const exists = await Pallet.findOne({ palletId });
  if (exists) return res.status(409).json({ message: 'palletId exists' });
  const totalPacks = composition.reduce((s, c) => s + (Number(c.packs) || 0), 0);
  const totalPieces = composition.reduce((s, c) => s + (Number(c.pieces) || 0), 0);
  const doc = await Pallet.create({ palletId, warehouseLocation, composition, totalPacks, totalPieces });
  res.status(201).json(doc);
};

export const updatePalletStatus = async (req, res) => {
  const { palletId } = req.params;
  const { status } = req.body || {};
  const allowed = ['available','allocated','shipped','reserved'];
  if (!allowed.includes(status)) return res.status(400).json({ message: 'invalid status' });
  const doc = await Pallet.findOneAndUpdate({ palletId }, { status }, { new: true });
  if (!doc) return res.status(404).json({ message: 'Not found' });
  res.json(doc);
};

export const updateComposition = async (req, res) => {
  const { palletId } = req.params;
  const { composition = [], warehouseLocation } = req.body || {};
  if (!Array.isArray(composition)) return res.status(400).json({ message: 'composition must be an array' });
  const normalized = composition.map((c) => {
    const packSize = Number(c.packSize) || 0;
    const packs = Number(c.packs) || 0;
    const pieces = Number(c.pieces);
    return {
      itemCode: (c.itemCode || '').toString(),
      packSize,
      packs,
      pieces: Number.isFinite(pieces) ? pieces : packs * packSize
    };
  });
  const totalPacks = normalized.reduce((s, c) => s + (Number(c.packs) || 0), 0);
  const totalPieces = normalized.reduce((s, c) => s + (Number(c.pieces) || 0), 0);
  const patch = { composition: normalized, totalPacks, totalPieces };
  if (warehouseLocation != null) patch.warehouseLocation = warehouseLocation;
  const doc = await Pallet.findOneAndUpdate({ palletId }, patch, { new: true });
  if (!doc) return res.status(404).json({ message: 'Not found' });
  res.json(doc);
};
