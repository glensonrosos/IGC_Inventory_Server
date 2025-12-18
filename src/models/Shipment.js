import mongoose from 'mongoose';

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

export default mongoose.model('Shipment', shipmentSchema);
