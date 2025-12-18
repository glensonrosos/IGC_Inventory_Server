import mongoose from 'mongoose';

const importLogSchema = new mongoose.Schema(
  {
    batchId: { type: mongoose.Schema.Types.ObjectId, ref: 'ImportBatch' },
    type: { type: String, enum: ['stock_in','initial','orders'], required: true },
    fileName: { type: String, default: '' },
    poNumber: { type: String, index: true },
    itemCode: { type: String, index: true },
    totalQty: { type: Number },
    packSize: { type: Number },
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
  },
  { timestamps: true }
);

// For quick duplicate checks by PO + Item
importLogSchema.index({ poNumber: 1, itemCode: 1 });

export default mongoose.model('ImportLog', importLogSchema);
