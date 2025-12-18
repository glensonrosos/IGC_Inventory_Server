import mongoose from 'mongoose';

const unfulfilledOrderLineSchema = new mongoose.Schema(
  {
    lineItem: { type: String, required: true, trim: true },
    groupName: { type: String, required: true, trim: true },
    qty: { type: Number, required: true },
  },
  { _id: false }
);

const unfulfilledOrderSchema = new mongoose.Schema(
  {
    orderNumber: { type: String, required: true, unique: true, trim: true },
    warehouseId: { type: mongoose.Schema.Types.ObjectId, ref: 'Warehouse', required: true, index: true },
    customerEmail: { type: String, trim: true },
    customerName: { type: String, trim: true },
    customerPhone: { type: String, trim: true },
    createdAtOrder: { type: Date },
    estFulfillmentDate: { type: Date },
    shippingAddress: { type: String, trim: true },
    lines: { type: [unfulfilledOrderLineSchema], default: [] },
    status: { type: String, enum: ['create','backorder','fulfilled','cancel','created','cancelled'], default: 'create' },
    committedBy: { type: String, default: '' },
  },
  { timestamps: true }
);

export default mongoose.model('UnfulfilledOrder', unfulfilledOrderSchema);
