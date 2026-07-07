import mongoose from 'mongoose';

const customerSchema = new mongoose.Schema(
  {
    email: { type: String, required: true, lowercase: true, trim: true, index: true },
    name: { type: String, required: true, trim: true },
    phone: { type: String, default: '', trim: true },
    accountNumber: { type: String, default: '', trim: true },
    companyName: { type: String, default: '', trim: true },
    shippingAddress: { type: String, required: true, trim: true },
    enabled: { type: Boolean, default: true },
  },
  { timestamps: true }
);

export default mongoose.model('Customer', customerSchema);
