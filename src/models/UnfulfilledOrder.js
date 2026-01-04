import mongoose from 'mongoose';

const unfulfilledOrderLineSchema = new mongoose.Schema(
  {
    lineItem: { type: String, required: true, trim: true },
    groupName: { type: String, required: true, trim: true },
    qty: { type: Number, required: true },
  },
  { _id: false }
);

const unfulfilledOrderAllocationSchema = new mongoose.Schema(
  {
    groupName: { type: String, required: true, trim: true },
    qty: { type: Number, required: true },
    source: { type: String, enum: ['primary', 'on_water', 'on_process', 'second'], required: true },
    // for 'second' source; 'primary' uses order.warehouseId
    warehouseId: { type: mongoose.Schema.Types.ObjectId, ref: 'Warehouse' },
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
    originalPrice: { type: Number },
    discountPercent: { type: Number },
    finalPrice: { type: Number },
    estFulfillmentDate: { type: Date },
    estDeliveredDate: { type: Date },
    shippingAddress: { type: String, trim: true },
    notes: { type: String, trim: true },
    lines: { type: [unfulfilledOrderLineSchema], default: [] },
    allocations: { type: [unfulfilledOrderAllocationSchema], default: [] },
    status: { type: String, enum: ['processing','ready_to_ship','shipped','delivered','completed','canceled','create','backorder','fulfilled','cancel','created','cancelled'], default: 'processing' },
    committedBy: { type: String, default: '' },
  },
  { timestamps: true }
);

export default mongoose.model('UnfulfilledOrder', unfulfilledOrderSchema);
