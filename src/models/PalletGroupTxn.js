import mongoose from 'mongoose';

const palletGroupTxnSchema = new mongoose.Schema(
  {
    poNumber: { type: String, trim: true },
    groupName: { type: String, required: true, index: true, trim: true },
    warehouseId: { type: mongoose.Schema.Types.ObjectId, ref: 'Warehouse', required: true, index: true },
    palletsDelta: { type: Number, required: true },
    status: { type: String, enum: ['Delivered','On-Water','Adjustment'], required: true },
    reason: { type: String, trim: true },
    wasOnWater: { type: Boolean, default: false },
    estDeliveryDate: { type: Date },
    committedAt: { type: Date, default: Date.now },
    committedBy: { type: String, default: '' },
  },
  { timestamps: true }
);

palletGroupTxnSchema.index({ poNumber: 1, groupName: 1, warehouseId: 1 });

export default mongoose.model('PalletGroupTxn', palletGroupTxnSchema);
