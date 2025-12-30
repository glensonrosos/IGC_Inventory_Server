import mongoose from 'mongoose';

const fulfilledOrderLineSchema = new mongoose.Schema(
  {
    lineItem: { type: String, required: true, trim: true },
    groupName: { type: String, required: true, trim: true },
    qty: { type: Number, required: true },
  },
  { _id: false }
);

const fulfilledOrderImportSchema = new mongoose.Schema(
  {
    orderNumber: { type: String, required: true, unique: true, trim: true },
    email: { type: String, trim: true },
    fulfilledAt: { type: Date },
    createdAtOrder: { type: Date },
    billingName: { type: String, trim: true },
    billingPhone: { type: String, trim: true },
    shippingName: { type: String, trim: true },
    shippingStreet: { type: String, trim: true },
    shippingAddress1: { type: String, trim: true },
    shippingPhone: { type: String, trim: true },
    warehouseId: { type: mongoose.Schema.Types.ObjectId, ref: 'Warehouse', required: true, index: true },
    lines: { type: [fulfilledOrderLineSchema], default: [] },
    source: { type: String, enum: ['csv', 'manual'], required: true },
    status: { type: String, enum: ['processing','shipped','delivered','completed','canceled','create','backorder','fulfilled','cancel','created','cancelled'], default: 'completed' },
    committedBy: { type: String, default: '' },
  },
  { timestamps: true }
);

export default mongoose.model('FulfilledOrderImport', fulfilledOrderImportSchema);
