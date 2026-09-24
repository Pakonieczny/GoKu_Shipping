const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const root = path.resolve(__dirname, '../..');
for (const page of ['design.html', 'design-1.html']) {
  const html = fs.readFileSync(path.join(root, page), 'utf8');
  const constants = html.slice(html.indexOf('const METALS = ['), html.indexOf('const PAGE_SIZE ='));
  const logic = html.slice(html.indexOf('function normMetalText(s)'), html.indexOf('/* ═══ 5 · ETSY REQUEST PIPELINE'));
  assert(constants && logic, `${page}: material classifier missing`);
  const api = vm.runInNewContext(`${constants}\n${logic}\n({ metalKeyFromText, txMetalResolved, metalCountsFor })`);
  const classify = text => api.metalKeyFromText(text, { solidStrict: true });

  for (const karat of ['10', '14']) {
    assert.equal(classify(`${karat}k Gold + Engrave`), 'gold', `${page}: bare ${karat}k Gold`);
    assert.equal(classify(`${karat}k Gold Filled`), 'gold', `${page}: ${karat}k Gold Filled`);
    assert.equal(classify(`${karat}k Solid Gold`), `${karat}k`, `${page}: genuine ${karat}k solid`);
  }
  assert.equal(classify('Solid Gold 10k'), '10k');
  assert.equal(classify('14/20 Rose Gold Filled'), 'rose');
  assert.equal(classify('14k Gold Filled + solid gold chain'), 'gold', `${page}: explicit GF wins`);
  assert.equal(classify('14k Gold + Engrave; solid chain'), 'gold', `${page}: unrelated solid text`);

  const tx = { quantity: 2, title: '14k Solid Gold listing', _metalKey: '14k solid gold',
    variations: [{ formatted_name: 'Metal', formatted_value: '14k Gold + Engrave' }] };
  assert.equal(api.txMetalResolved(tx), 'gold', `${page}: ordered metal variation wins`);
  assert.equal(api.metalCountsFor([tx]).gold, 2, `${page}: counts and bridge receive GF`);
  assert.equal(api.metalCountsFor([tx])['14k'], 0, `${page}: no solid count`);
  assert.equal(api.txMetalResolved({ _metalKey: '14k solid gold' }), '', `${page}: stale cache alone is not proof`);
}

const sorter = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
const routing = sorter.slice(sorter.indexOf('function routeByName(name, folder)'), sorter.indexOf('/* ═══ 6 · sources & intake'));
assert(routing, 'sorter: filename routing missing');
const route = vm.runInNewContext(`${routing}\nrouteByName`);
for (const karat of ['10', '14']) {
  assert.equal(route(`${karat}k Gold + Engrave.ai`), 'gold', `sorter: bare ${karat}k Gold`);
  assert.equal(route(`${karat}k Gold Filled.ai`), 'gold', `sorter: ${karat}k GF`);
  assert.equal(route(`${karat}k Solid Gold.ai`), `gold${karat}k`, `sorter: solid ${karat}k`);
}
assert.equal(route('14/20 Rose Gold.ai'), 'rose');
assert.equal(route('14k Gold Filled - solid chain.ai'), 'gold');
assert.equal(route('10k.ai'), null, 'an ambiguous filename requires the operator to select a sheet');
console.log('Design, Design-1 and sorter material classifications passed');
