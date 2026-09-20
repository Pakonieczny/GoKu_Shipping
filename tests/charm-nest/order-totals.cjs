const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync('charm-nest-bridge.js', 'utf8');
const context = vm.createContext({});
vm.runInContext(source.slice(source.indexOf('  function orderTotals('), source.indexOf('  function buildHead(')), context);
const totals = rows => JSON.parse(JSON.stringify(context.orderTotals(rows)));
const line = (receipt, transaction, quantity, extra = {}) => ({
  order: { receiptId: receipt }, line: { transactionId: transaction, quantity }, state: 'pulled', ...extra
});
assert.deepEqual(totals([]), { orders: 0, charms: 0 });
const rows = [
  line(123, 'a', 2), line('123', 'b', 3),
  line(456, 'c', 1, { state: 'held' }),
  line(789, 'd', 12, { spec: { noDesign: true, quantity: 12 } }),
  line(789, 'e', 4, { state: 'noDesign' }),
  line(999, 'f', 20, { state: 'gone' }),
  line(456, 'g', 9, { state: 'committed', spec: { quantity: 2 } })
];
assert.deepEqual(totals(rows), { orders: 3, charms: 8 }, 'distinct orders, physical quantity, held charms, and open completed work');
assert.deepEqual(totals([...rows, { ...rows[0], key: 'restored-copy' }]), { orders: 3, charms: 8 }, 'restored duplicate transaction is counted once');
rows[0].line.quantity = 5;
assert.deepEqual(totals(rows), { orders: 3, charms: 11 }, 'quantity changes update the total');
assert.deepEqual(totals([line('only-chain', 'a', 3, { spec: { noDesign: true } })]), { orders: 1, charms: 0 }, 'chain-only order counts as an order, not a charm');
assert.deepEqual(totals([line('invalid', 'a', NaN), line('invalid', 'b', -2), line('invalid', 'c', 0)]), { orders: 1, charms: 0 }, 'invalid quantities never poison totals');
console.log('Order totals OK: distinct receipts, quantities, restored duplicates, no-design exclusions, closed orders and updates');
