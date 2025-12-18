import mongoose from 'mongoose';

const palletGroupStockSchema = new mongoose.Schema(
  {
    groupName: { type: String, required: true, index: true, trim: true },
    warehouseId: { type: mongoose.Schema.Types.ObjectId, ref: 'Warehouse', required: true, index: true },
    pallets: { type: Number, required: true, default: 0 },
  },
  { timestamps: true }
);

palletGroupStockSchema.index({ groupName: 1, warehouseId: 1 }, { unique: true });

export default mongoose.model('PalletGroupStock', palletGroupStockSchema);
