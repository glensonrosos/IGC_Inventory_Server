import mongoose from 'mongoose';

const palletSchema = new mongoose.Schema(
  {
    palletId: { type: String, required: true, unique: true },
    warehouseLocation: { type: String, default: '' },
    status: { type: String, enum: ['available','allocated','shipped','reserved'], default: 'available' },
    composition: [
      {
        itemCode: String,
        packSize: Number,
        packs: Number,
        pieces: Number
      }
    ],
    totalPacks: { type: Number, default: 0 },
    totalPieces: { type: Number, default: 0 }
  },
  { timestamps: true }
);

export default mongoose.model('Pallet', palletSchema);
