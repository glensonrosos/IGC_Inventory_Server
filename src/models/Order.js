import mongoose from 'mongoose';

const orderLineSchema = new mongoose.Schema(
  {
    itemCode: String,
    packSize: Number,
    packsRequested: Number,
    piecesRequested: Number,
    requestedPallets: Number,
    assignedPallets: [String]
  },
  { _id: false }
);

const orderSchema = new mongoose.Schema(
  {
    orderNumber: { type: String, required: true, unique: true },
    customerName: { type: String, required: true },
    customerAddress: { type: String },
    orderLines: [orderLineSchema],
    status: { type: String, enum: ['draft','confirmed','picked','shipped','cancelled'], default: 'draft' },
    estDeliveryDate: Date,
    totalPalletUsed: { type: Number }
  },
  { timestamps: true }
);

export default mongoose.model('Order', orderSchema);
