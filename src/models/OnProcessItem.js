import mongoose from 'mongoose';

const onProcessItemSchema = new mongoose.Schema(
  {
    batchId: { type: mongoose.Schema.Types.ObjectId, ref: 'OnProcessBatch', index: true },
    poNumber: { type: String, required: true, index: true },
    itemCode: { type: String, required: true, index: true },
    totalQty: { type: Number, required: true },
    packSize: { type: Number, required: true },
    status: { type: String, enum: ['on_process','completed','cancelled'], default: 'on_process' },
    notes: { type: String, default: '' },
    createdBy: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
  },
  { timestamps: true }
);

onProcessItemSchema.index({ poNumber: 1, itemCode: 1 }, { unique: true });

export default mongoose.model('OnProcessItem', onProcessItemSchema);
