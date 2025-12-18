import User from '../models/User.js';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';

export const login = async (req, res) => {
  const { username, password } = req.body;
  // Seed default admin/admin if there are no users
  const count = await User.estimatedDocumentCount();
  if (count === 0) {
    const hash = await bcrypt.hash('admin', 10);
    await User.create({ username: 'admin', password: hash, role: 'admin', enabled: true });
  }
  const user = await User.findOne({ username });
  if (!user) return res.status(401).json({ message: 'Invalid credentials' });
  if (user.enabled === false) return res.status(401).json({ message: 'Account is disabled' });
  const ok = await bcrypt.compare(password, user.password);
  if (!ok) return res.status(401).json({ message: 'Invalid credentials' });
  const token = jwt.sign({ id: user._id, username: user.username, role: user.role }, process.env.JWT_SECRET || 'secret', { expiresIn: '7d' });
  res.json({ token, user: { id: user._id, username: user.username, role: user.role } });
};

export const changePassword = async (req, res) => {
  const { oldPassword, newPassword } = req.body || {};
  if (!oldPassword || !newPassword) return res.status(400).json({ message: 'oldPassword and newPassword required' });
  const userId = req.user?.id;
  const user = await User.findById(userId);
  if (!user) return res.status(404).json({ message: 'User not found' });
  const ok = await bcrypt.compare(oldPassword, user.password);
  if (!ok) return res.status(400).json({ message: 'Old password is incorrect' });
  const hash = await bcrypt.hash(newPassword, 10);
  user.password = hash;
  await user.save();
  return res.json({ message: 'password updated' });
};
