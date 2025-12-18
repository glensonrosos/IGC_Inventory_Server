import Item from '../models/Item.js';
import Pallet from '../models/Pallet.js';

// Very simple FIFO allocation:
// 1) If orderLines request requestedPallets, try assign full pallets first from available pallets containing that item.
// 2) If insufficient pallets, fallback to packs/pieces by reducing Item.totalQty (reservation not persisted here).
// Returns { assignments, shortages }
export const autoAllocateOrder = async (order) => {
  const assignments = []; // [{ lineIndex, palletIds: [] }]
  const shortages = [];   // [{ lineIndex, missingPieces }]

  for (let i = 0; i < order.orderLines.length; i++) {
    const line = order.orderLines[i];
    const { itemCode, packSize, packsRequested = 0, piecesRequested = 0, requestedPallets = 0 } = line;

    const palletIds = [];
    if (requestedPallets && requestedPallets > 0) {
      // find available pallets that fully contain this item (any composition ok)
      const pallets = await Pallet.find({ status: 'available', 'composition.itemCode': itemCode }).sort({ createdAt: 1 }).lean();
      for (const p of pallets) {
        if (palletIds.length >= requestedPallets) break;
        palletIds.push(p.palletId);
      }
    }

    // Assign pallets found
    if (palletIds.length) {
      assignments.push({ lineIndex: i, palletIds });
      await Pallet.updateMany({ palletId: { $in: palletIds } }, { $set: { status: 'reserved' } });
    }

    // Packs/pieces allocation against stock (simple decrement)
    const doc = await Item.findOne({ itemCode });
    if (!doc) {
      const needPieces = (packsRequested * (packSize || doc?.packSize || 1)) + (piecesRequested || 0);
      shortages.push({ lineIndex: i, missingPieces: needPieces });
      continue;
    }
    const effPack = packSize || doc.packSize || 1;
    const needPieces = (packsRequested * effPack) + (piecesRequested || 0);
    if (!needPieces) continue;

    if (doc.totalQty >= needPieces) {
      doc.totalQty -= needPieces; // consume
      await doc.save();
    } else {
      const missing = needPieces - doc.totalQty;
      doc.totalQty = 0;
      await doc.save();
      shortages.push({ lineIndex: i, missingPieces: missing });
    }
  }

  return { assignments, shortages };
};
