import mongoose from 'mongoose';

const warehouseSchema = new mongoose.Schema(
  {
    name: { type: String, required: true, unique: true, trim: true },
    address: { type: String, default: '' }
  },
  { timestamps: true }
);

export default mongoose.model('Warehouse', warehouseSchema);
