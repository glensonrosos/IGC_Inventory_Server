import Customer from '../models/Customer.js';

const toPublicError = (e) => {
  const msg = e?.message || 'Unknown error';
  if (e?.code === 11000) {
    const fields = Object.keys(e?.keyValue || {});
    const field = fields[0] || 'field';
    return { status: 400, body: { message: `${field} already exists` } };
  }
  if (e?.name === 'ValidationError') {
    return { status: 400, body: { message: msg } };
  }
  return { status: 500, body: { message: msg } };
};

const assertAdmin = (req, res) => {
  if (req.user?.role !== 'admin') {
    res.status(403).json({ message: 'admin only' });
    return false;
  }
  return true;
};

export const listCustomers = async (req, res) => {
  if (!assertAdmin(req, res)) return;
  try {
    const rows = await Customer.find({})
      .select('_id email name phone accountNumber companyName shippingAddress enabled createdAt updatedAt')
      .sort({ createdAt: -1 })
      .lean();
    res.json(rows);
  } catch (e) {
    const pub = toPublicError(e);
    res.status(pub.status).json(pub.body);
  }
};

export const createCustomer = async (req, res) => {
  if (!assertAdmin(req, res)) return;
  try {
    const { email, name, phone, accountNumber, companyName, shippingAddress, enabled } = req.body || {};
    const em = String(email || '').trim().toLowerCase();
    const nm = String(name || '').trim();
    const addr = String(shippingAddress || '').trim();
    if (!em) return res.status(400).json({ message: 'email required' });
    if (!nm) return res.status(400).json({ message: 'name required' });
    if (!addr) return res.status(400).json({ message: 'shippingAddress required' });

    const exists = await Customer.findOne({ email: em }).select('_id').lean();
    if (exists) return res.status(400).json({ message: 'email already exists' });

    const doc = await Customer.create({
      email: em,
      name: nm,
      phone: String(phone || '').trim(),
      accountNumber: String(accountNumber || '').trim(),
      companyName: String(companyName || '').trim(),
      shippingAddress: addr,
      enabled: enabled === false ? false : true,
    });
    res.status(201).json(doc);
  } catch (e) {
    const pub = toPublicError(e);
    res.status(pub.status).json(pub.body);
  }
};

export const updateCustomer = async (req, res) => {
  if (!assertAdmin(req, res)) return;
  try {
    const { id } = req.params;
    const { email, name, phone, accountNumber, companyName, shippingAddress } = req.body || {};
    const set = {};
    if (email !== undefined) set.email = String(email || '').trim().toLowerCase();
    if (name !== undefined) set.name = String(name || '').trim();
    if (phone !== undefined) set.phone = String(phone || '').trim();
    if (accountNumber !== undefined) set.accountNumber = String(accountNumber || '').trim();
    if (companyName !== undefined) set.companyName = String(companyName || '').trim();
    if (shippingAddress !== undefined) set.shippingAddress = String(shippingAddress || '').trim();

    if (set.email && !set.email.includes('@')) return res.status(400).json({ message: 'invalid email' });
    if (set.name === '') return res.status(400).json({ message: 'name required' });
    if (set.shippingAddress === '') return res.status(400).json({ message: 'shippingAddress required' });

    const doc = await Customer.findByIdAndUpdate(id, { $set: set }, { new: true }).lean();
    if (!doc) return res.status(404).json({ message: 'customer not found' });
    res.json(doc);
  } catch (e) {
    const pub = toPublicError(e);
    res.status(pub.status).json(pub.body);
  }
};

export const setCustomerEnabled = async (req, res) => {
  if (!assertAdmin(req, res)) return;
  try {
    const { id } = req.params;
    const { enabled } = req.body || {};
    if (typeof enabled !== 'boolean') return res.status(400).json({ message: 'enabled must be boolean' });
    const doc = await Customer.findByIdAndUpdate(id, { $set: { enabled } }, { new: true })
      .select('_id email name enabled');
    if (!doc) return res.status(404).json({ message: 'customer not found' });
    res.json(doc);
  } catch (e) {
    const pub = toPublicError(e);
    res.status(pub.status).json(pub.body);
  }
};
