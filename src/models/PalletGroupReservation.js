import mongoose from 'mongoose';

const palletGroupReservationSchema = new mongoose.Schema(
  {
    orderNumber: { type: String, required: true, trim: true, index: true },
    warehouseId: { type: mongoose.Schema.Types.ObjectId, ref: 'Warehouse', required: true, index: true },
    // where the reserved stock will come from (for physical warehouses)
    sourceWarehouseId: { type: mongoose.Schema.Types.ObjectId, ref: 'Warehouse', index: true },
    groupName: { type: String, required: true, trim: true, index: true },
    source: { type: String, enum: ['primary', 'on_water', 'on_process', 'second'], required: true, index: true },
    qty: { type: Number, required: true },
    committedBy: { type: String, default: '' },
  },
  { timestamps: true }
);

palletGroupReservationSchema.index({ warehouseId: 1, groupName: 1, source: 1 });
palletGroupReservationSchema.index({ sourceWarehouseId: 1, groupName: 1, source: 1 });

export default mongoose.model('PalletGroupReservation', palletGroupReservationSchema);
