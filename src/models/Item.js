import mongoose from 'mongoose';

const itemSchema = new mongoose.Schema(
  {
    itemCode: { type: String, required: true, trim: true },
    itemGroup: { type: String, default: '' },
    description: { type: String, default: '' },
    color: { type: String, default: '' },
    totalQty: { type: Number, required: true, default: 0 },
    packSize: { type: Number, required: true, default: 1 },
    packsOnHand: { type: Number, default: 0 },
    palletsOnHand: { type: Number, default: 0 },
    lowStockThreshold: { type: Number, default: 0 },
    enabled: { type: Boolean, default: true }
  },
  { timestamps: true }
);

// Ensure uniqueness per pallet group
itemSchema.index({ itemCode: 1, itemGroup: 1 }, { unique: true });

export default mongoose.model('Item', itemSchema);
