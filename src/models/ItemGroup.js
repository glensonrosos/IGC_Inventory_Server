import mongoose from 'mongoose';

const itemGroupSchema = new mongoose.Schema(
  {
    name: { type: String, required: true, unique: true, trim: true },
    active: { type: Boolean, default: true },
    lineItem: { type: String, default: '' },
    palletName: { type: String, trim: true, default: '' },
    palletDescription: { type: String, trim: true, default: '' },
    price: { type: Number }
  },
  { timestamps: true }
);

export default mongoose.model('ItemGroup', itemGroupSchema);
