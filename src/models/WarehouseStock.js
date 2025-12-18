import mongoose from 'mongoose';

const warehouseStockSchema = new mongoose.Schema(
  {
    itemCode: { type: String, required: true, index: true },
    warehouseId: { type: mongoose.Schema.Types.ObjectId, ref: 'Warehouse', required: true, index: true },
    qtyPieces: { type: Number, required: true, default: 0 },
  },
  { timestamps: true }
);

warehouseStockSchema.index({ itemCode: 1, warehouseId: 1 }, { unique: true });

export default mongoose.model('WarehouseStock', warehouseStockSchema);
