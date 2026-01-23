import mongoose from 'mongoose';

const lineSchema = new mongoose.Schema(
  {
    groupName: { type: String, required: true, trim: true },
    lineItem: { type: String, required: true, trim: true },
    palletName: { type: String, default: '', trim: true },
    qty: { type: Number, required: true, min: 0 },
  },
  { _id: false }
);

const earlyBuyOrderSchema = new mongoose.Schema(
  {
    id: { type: String, required: true, unique: true, index: true }, // EORD-0001
    status: {
      type: String,
      enum: ['processing', 'ready_to_ship', 'shipped', 'completed', 'canceled'],
      default: 'processing',
      index: true,
    },
    warehouseId: { type: String, default: '', trim: true },
    createdAtYmd: { type: String, required: true }, // YYYY-MM-DD (local date as string)
    estFulfillment: { type: String, default: '' },
    estDelivered: { type: String, default: '' },
    customerEmail: { type: String, required: true },
    customerName: { type: String, required: true },
    customerPhone: { type: String, required: true },
    shippingAddress: { type: String, required: true },
    originalPrice: { type: String, default: '' },
    shippingPercent: { type: String, default: '' },
    discountPercent: { type: String, default: '' },
    notes: { type: String, default: '' },
    lines: { type: [lineSchema], default: [] },
  },
  { timestamps: true }
);

export default mongoose.model('EarlyBuyOrder', earlyBuyOrderSchema);
