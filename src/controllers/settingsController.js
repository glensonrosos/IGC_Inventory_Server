import { getAllSettings, setPacksPerPallet } from '../services/config.js';

export const getSettings = async (req, res) => {
  const cfg = await getAllSettings();
  res.json(cfg);
};

export const updateSettings = async (req, res) => {
  const { packsPerPallet } = req.body || {};
  const updated = await setPacksPerPallet(packsPerPallet);
  res.json(updated);
};
