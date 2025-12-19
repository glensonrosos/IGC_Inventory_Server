import mongoose from 'mongoose';
import Counter from './Counter.js';

const shipmentItemSchema = new mongoose.Schema(
  {
    itemCode: { type: String, required: true },
    qtyPieces: { type: Number, required: true },
    packSize: { type: Number },
  },
  { _id: false }
);

const shipmentSchema = new mongoose.Schema(
  {
    kind: { type: String, enum: ['import', 'transfer'], required: true },
    status: { type: String, enum: ['on_water', 'delivered', 'transferred'], default: 'on_water', index: true },
    owNumber: { type: String, default: '', index: true },
    reference: { type: String, default: '' },
    sourceWarehouseId: { type: mongoose.Schema.Types.ObjectId, ref: 'Warehouse' },
    warehouseId: { type: mongoose.Schema.Types.ObjectId, ref: 'Warehouse', required: true },
    estDeliveryDate: { type: Date },
    items: { type: [shipmentItemSchema], default: [] },
    notes: { type: String, default: '' },
    createdBy: { type: String },
  },
  { timestamps: true }
);

shipmentSchema.pre('validate', async function (next) {
  try {
    if (!this.isNew) return next();
    if (String(this.owNumber || '').trim()) return next();
    const ctr = await Counter.findOneAndUpdate(
      { name: 'shipment_ow' },
      { $inc: { seq: 1 } },
      { upsert: true, new: true, setDefaultsOnInsert: true }
    );
    const num = String(ctr.seq).padStart(4, '0');
    this.owNumber = `OW-${num}`;
    return next();
  } catch (e) {
    return next(e);
  }
});

export default mongoose.model('Shipment', shipmentSchema);
