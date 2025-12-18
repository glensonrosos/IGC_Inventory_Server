import mongoose from 'mongoose';

const onProcessPalletSchema = new mongoose.Schema(
  {
    batchId: { type: mongoose.Schema.Types.ObjectId, ref: 'OnProcessBatch', index: true },
    poNumber: { type: String, required: true, index: true },
    groupName: { type: String, required: true, index: true },
    totalPallet: { type: Number, required: true },
    finishedPallet: { type: Number, default: 0 },
    transferredPallet: { type: Number, default: 0 },
    status: { type: String, enum: ['in_progress','partial','completed','cancelled'], default: 'in_progress' },
    locked: { type: Boolean, default: false },
    notes: { type: String, default: '' },
    createdBy: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
  },
  { timestamps: true }
);

onProcessPalletSchema.index({ poNumber: 1, groupName: 1 }, { unique: true });

export default mongoose.model('OnProcessPallet', onProcessPalletSchema);
