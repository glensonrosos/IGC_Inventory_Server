import mongoose from 'mongoose';

const movementSchema = new mongoose.Schema(
  {
    type: { type: String, enum: ['IN','OUT','ADJUSTMENT','ALLOCATE','RECEIPT'], required: true },
    reference: { type: String, default: '' },
    warehouseId: { type: mongoose.Schema.Types.ObjectId, ref: 'Warehouse' },
    items: [
      {
        itemCode: String,
        qtyPieces: Number,
        packSize: Number,
        palletId: String
      }
    ],
    createdBy: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
    notes: { type: String, default: '' }
  },
  { timestamps: true }
);

export default mongoose.model('StockMovement', movementSchema);
