import bcrypt from 'bcryptjs';
import User from '../models/User.js';

const toPublicError = (e) => {
  const msg = e?.message || 'Unknown error';
  // Mongo duplicate key
  if (e?.code === 11000) {
    const fields = Object.keys(e?.keyValue || {});
    const field = fields[0] || 'field';
    return { status: 400, body: { message: `${field} already exists` } };
  }
  // Mongoose validation
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

export const listUsers = async (req, res) => {
  if (!assertAdmin(req, res)) return;
  try {
    const users = await User.find({})
      .select('_id username role enabled createdAt updatedAt')
      .sort({ createdAt: -1 })
      .lean();
    res.json(users);
  } catch (e) {
    console.error('users.listUsers failed', e);
    const pub = toPublicError(e);
    res.status(pub.status).json(pub.body);
  }
};

export const createUser = async (req, res) => {
  if (!assertAdmin(req, res)) return;
  try {
    const { username, password, role } = req.body || {};
    const uname = String(username || '').trim().toLowerCase();
    const pwd = String(password || '').trim();
    const r = String(role || '').trim();

    if (!uname) return res.status(400).json({ message: 'username required' });
    if (!pwd) return res.status(400).json({ message: 'password required' });
    if (!['admin', 'user'].includes(r)) return res.status(400).json({ message: 'role must be admin or user' });

    const exists = await User.findOne({ username: uname }).select('_id').lean();
    if (exists) return res.status(400).json({ message: 'username already exists' });

    const hash = await bcrypt.hash(pwd, 10);
    const doc = await User.create({ username: uname, password: hash, role: r, enabled: true });
    res.status(201).json({ id: doc._id, username: doc.username, role: doc.role, enabled: doc.enabled });
  } catch (e) {
    console.error('users.createUser failed', e);
    const pub = toPublicError(e);
    res.status(pub.status).json(pub.body);
  }
};

export const setUserEnabled = async (req, res) => {
  if (!assertAdmin(req, res)) return;
  try {
    const { id } = req.params;
    const { enabled } = req.body || {};
    if (typeof enabled !== 'boolean') return res.status(400).json({ message: 'enabled must be boolean' });

    // prevent disabling yourself (safety)
    if (String(req.user?.id) === String(id) && enabled === false) {
      return res.status(400).json({ message: 'cannot disable your own account' });
    }

    const user = await User.findByIdAndUpdate(id, { $set: { enabled } }, { new: true }).select('_id username role enabled');
    if (!user) return res.status(404).json({ message: 'user not found' });
    res.json({ id: user._id, username: user.username, role: user.role, enabled: user.enabled });
  } catch (e) {
    console.error('users.setUserEnabled failed', e);
    const pub = toPublicError(e);
    res.status(pub.status).json(pub.body);
  }
};

export const resetUserPassword = async (req, res) => {
  if (!assertAdmin(req, res)) return;
  try {
    const { id } = req.params;
    const { newPassword } = req.body || {};
    const pwd = String(newPassword || '').trim();
    if (!pwd) return res.status(400).json({ message: 'newPassword required' });

    const user = await User.findById(id);
    if (!user) return res.status(404).json({ message: 'user not found' });

    const hash = await bcrypt.hash(pwd, 10);
    user.password = hash;
    await user.save();
    res.json({ message: 'password reset' });
  } catch (e) {
    console.error('users.resetUserPassword failed', e);
    const pub = toPublicError(e);
    res.status(pub.status).json(pub.body);
  }
};

export const setUserRole = async (req, res) => {
  if (!assertAdmin(req, res)) return;
  try {
    const { id } = req.params;
    const { role } = req.body || {};
    const r = String(role || '').trim();

    if (!['admin', 'user'].includes(r)) return res.status(400).json({ message: 'role must be admin or user' });

    // prevent changing your own role (safety)
    if (String(req.user?.id) === String(id)) {
      return res.status(400).json({ message: 'cannot change your own role' });
    }

    const user = await User.findByIdAndUpdate(id, { $set: { role: r } }, { new: true })
      .select('_id username role enabled');
    if (!user) return res.status(404).json({ message: 'user not found' });
    res.json({ id: user._id, username: user.username, role: user.role, enabled: user.enabled });
  } catch (e) {
    console.error('users.setUserRole failed', e);
    const pub = toPublicError(e);
    res.status(pub.status).json(pub.body);
  }
};
