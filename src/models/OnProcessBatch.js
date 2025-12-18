import mongoose from 'mongoose';

const onProcessBatchSchema = new mongoose.Schema(
  {
    reference: { type: String, required: true, unique: true, index: true },
    poNumber: { type: String, required: true, index: true },
    status: { type: String, enum: ['in-progress','partial-done','completed'], default: 'in-progress' },
    estFinishDate: { type: Date },
    notes: { type: String, default: '' },
    createdBy: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
  },
  { timestamps: true }
);

export default mongoose.model('OnProcessBatch', onProcessBatchSchema);
