// A Laser tab that nobody is looking at reads nothing (FC2): the previous-sheet watch of charm-nest-laser-time.js skips its tick while the tab is hidden, and the
// first look at the tab reads at once when the figure has gone stale. A shown tab keeps its read every 5 minutes; a page with no document keeps working.
//   node tests/cost/fc2-laser-hidden.cjs
'use strict';
const assert = require('assert'), path = require('path'), fs = require('fs'), vm = require('vm');
const SRC = fs.readFileSync(path.join(__dirname, '..', '..', 'charm-nest-laser-time.js'), 'utf8');
const MIN = 60000;
const ok = s => process.stdout.write('ok   ' + s + '\n');
const sleep = ms => new Promise(r => setTimeout(r, ms));

function load(withDocument) {
  const timers = [], listeners = [], doc = withDocument ? { hidden: false, addEventListener: (t, f) => listeners.push([t, f]) } : undefined;
  const store = new Map();
  const ctx = { console, setTimeout, clearTimeout, setInterval: (f, ms) => { timers.push({ f, ms }); return { unref() {} }; }, clearInterval() {}, localStorage: { getItem: k => (store.has(k) ? store.get(k) : null), setItem: (k, v) => store.set(k, String(v)), removeItem: k => store.delete(k) } };
  if (doc) ctx.document = doc;
  vm.createContext(ctx); vm.runInContext(SRC, ctx);
  return { L: ctx.LaserSheetTime, timers, listeners, doc };
}

(async () => {
  let T = 1.8e12, calls = 0;
  const who = { person: 'Ana M.', startAt: T, session: 'sess-1' };
  const wire = L => L.configure({ who: () => who, role: () => 'laser', now: () => T, call: async b => { calls++; assert.strictEqual(b.op, 'laserSheetLast'); return { ok: true, now: T, last: null }; } });
  const fire = h => { for (const [t, f] of h.listeners) if (t === 'visibilitychange') f(); };

  // 1. a shown tab: one timer of 60 s, a read when nothing is known, none while the answer is fresh, one more when it is 5 minutes old
  let h = load(true); wire(h.L);
  assert.strictEqual(h.timers.length, 1); assert.strictEqual(h.timers[0].ms, 60000); assert.ok(h.listeners.some(([t]) => t === 'visibilitychange'), 'it listens for the tab being shown');
  h.timers[0].f(); await sleep(5); assert.strictEqual(calls, 1, 'a shown tab with nothing known reads');
  for (let i = 0; i < 4; i++) { T += MIN; h.timers[0].f(); await sleep(2); } assert.strictEqual(calls, 1, 'fresh for 5 minutes');
  T += MIN + 1; h.timers[0].f(); await sleep(5); assert.strictEqual(calls, 2, 'a read when 5 minutes old'); ok('a shown tab reads when nothing is known and again every 5 minutes, nothing between');

  // 2. hidden for half an hour: 30 ticks, no read; hidden -> visible fires the check which reads at once because the figure is stale
  calls = 0; h.doc.hidden = true; fire(h);
  for (let i = 0; i < 30; i++) { T += MIN; h.timers[0].f(); await sleep(1); }
  assert.strictEqual(calls, 0, 'a hidden tab reads nothing in 30 minutes (was 6)');
  fire(h); await sleep(3); assert.strictEqual(calls, 0, 'becoming hidden reads nothing');
  h.doc.hidden = false; fire(h); await sleep(5); assert.strictEqual(calls, 1, 'shown again after 30 minutes: read at once');
  fire(h); await sleep(3); assert.strictEqual(calls, 1, 'shown again with a fresh figure: no second read');
  ok('hidden for 30 minutes: 0 reads (was 6); the first look reads at once, only if the figure is stale');

  // 3. shown again after less than 5 minutes: no read; signed out or another role: no read
  h.doc.hidden = true; T += 2 * MIN; h.doc.hidden = false; fire(h); await sleep(3); assert.strictEqual(calls, 1, 'a short absence needs no read');
  let role = 'laser'; const h2 = load(true); h2.L.configure({ who: () => who, role: () => role, now: () => T, call: async () => { calls++; return { ok: true, now: T, last: null }; } });
  calls = 0; role = 'admin'; fire(h2); h2.timers[0].f(); await sleep(3); assert.strictEqual(calls, 0, 'only a Laser sign-in reads'); ok('a short absence and a non-Laser sign-in read nothing');

  // 4. a page with no document keeps its timer (the old behaviour)
  calls = 0; const h3 = load(false); wire(h3.L); assert.strictEqual(h3.listeners.length, 0);
  h3.timers[0].f(); await sleep(5); assert.strictEqual(calls, 1); ok('without a document the watch works as before');
  process.stdout.write('laser hidden OK\n'); process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
