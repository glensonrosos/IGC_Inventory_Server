export const computePacksOnHand = (totalQty, packSize) => {
  if (!packSize || packSize <= 0) return 0;
  return Math.floor((Number(totalQty) || 0) / (Number(packSize) || 1));
};

export const computePalletsOnHand = (packsOnHand, packsPerPallet) => {
  if (!packsPerPallet || packsPerPallet <= 0) return 0;
  return Math.ceil(packsOnHand / packsPerPallet);
};

export const getPacksPerPallet = () => {
  const d = Number(process.env.DEFAULT_PACKS_PER_PALLET || 50);
  return Number.isFinite(d) && d > 0 ? d : 50;
};
