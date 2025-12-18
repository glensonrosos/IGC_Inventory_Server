import { computePacksOnHand, computePalletsOnHand } from '../src/utils/calc.js';

describe('calc utils', () => {
  test('packsOnHand floors totalQty/packSize', () => {
    expect(computePacksOnHand(100, 20)).toBe(5);
    expect(computePacksOnHand(99, 20)).toBe(4);
    expect(computePacksOnHand(0, 20)).toBe(0);
    expect(computePacksOnHand(10, 0)).toBe(0);
  });

  test('palletsOnHand uses ceil(packs/ppp)', () => {
    expect(computePalletsOnHand(100, 50)).toBe(2);
    expect(computePalletsOnHand(51, 50)).toBe(2);
    expect(computePalletsOnHand(50, 50)).toBe(1);
    expect(computePalletsOnHand(0, 50)).toBe(0);
  });
});
