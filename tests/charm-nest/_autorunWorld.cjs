// Shared by the adv-autorun-*.cjs tests: the orders, a plain look at the Auto run, and the checks every one of them ends with
// (nothing lost, nothing twice, the sorter and the Design Station agree). Runs against the day-sim harness (the real sorter and
// the framed station in Chromium, the repo's fake site); nothing here touches anything live.
const assert = require('assert');

const day = Math.floor(Date.now() / 1000);
const GF = '14k Gold Filled', SS = 'Sterling Silver';
/** One line, one piece, no engraving. */
const receipt = (rid, sku, metal) => ({ receipt_id: rid, order_number: rid, name: 'Buyer ' + rid, country_iso: 'US', city: 'Austin', message_from_buyer: '', update_timestamp: day, create_timestamp: day - 3600, status: 'Paid', is_shipped: false,
  transactions: [{ transaction_id: Number(`${rid}1`), listing_id: 1718000 + +sku.slice(-2), receipt_id: rid, sku, title: `${sku} charm`, quantity: 1, expected_ship_date: day + 3 * 86400, is_personalized: false, variations: [{ formatted_name: 'Metal', formatted_value: metal }] }] });
const SKUS = [1, 2, 3, 6, 7, 8].map(n => 'BR-TST-' + String(n).padStart(2, '0'));
/** n Gold Filled and n Sterling Silver orders, numbered from `first`. */
const wave = (first, n) => { const out = []; for (let i = 0; i < n; i++) { out.push(receipt(first + 2 * i, SKUS[i % SKUS.length], GF)); out.push(receipt(first + 2 * i + 1, SKUS[(i + 3) % SKUS.length], SS)); } return out; };
/** The fixture master for these orders: charms (30 to 42 mm) big enough that a sheet holds about five and is full with the sixth order of a metal. */
const MASTERS = [{ count: 12, scale: 3 }];

/** The run as the page sees it (null while the page is loading). */
const view = (page) => page.evaluate(() => {
  const r = B.run, sheets = allSheets().filter(p => r && p.runId === r.runId);
  return { run: r && { id: r.runId, status: r.status, step: r.step, stoppedBy: r.stoppedBy || null, kind: RunCtl.stopKind(r), busy: !!r.arrivalBusy, committed: (r.committed || []).length },
    working: sheets.filter(p => ['nesting', 'queued', 'finishing'].includes(p.status) || p._operationStarting || p.persisted && !p.persistedDone).length,
    sheets: sheets.map(p => `${p.metal}${p.page}:${p.status}:${p.placements.length}/${p.charms.length}${p.releaseFull ? 'F' : ''}${p.runHold ? 'H' : ''}${p.setId ? '@' + p.setId.split('-').pop() : ''}`),
    rows: Orders.rows().reduce((m, x) => (m[x.state] = (m[x.state] || 0) + 1, m), {}), sets: Sets.ofRun(r && r.runId).map(s => `${s.name}:${s.status}:${s.committedAt ? 'C' : '-'}:${s.sheetIds.length}`),
    check: { error: Arrivals.state().error, last: Arrivals.state().lastCheck, pending: !!Arrivals.state().pending }, rowCount: Orders.rows().length };
}).catch(() => null);

/** Makes the next order check due now, as the clock would: in the page and in the browser's copy of the checks' times, which the tick reads under the lock open sorters share. */
const due = page => page.evaluate(() => { const k = 'cn.arrivals.' + (typeof WORKSPACE_SANDBOX !== 'undefined' && WORKSPACE_SANDBOX ? 'sandbox' : 'production'); let o = {}; try { o = JSON.parse(localStorage.getItem(k) || '{}'); } catch (_) {} o.nextCheck = Date.now(); localStorage.setItem(k, JSON.stringify(o)); Arrivals.state().nextCheck = Date.now(); });

/** Waits until the run rests (processed or complete, nothing nesting or being added) for `calm` looks in a row. Presses nothing. Returns the last view, with `timeout` set if it never rested. */
async function rest(page, { timeoutMs = 300000, calm = 5, log = () => {}, also = () => true } = {}) {
  const t0 = Date.now(); let n = 0, last = '', v = null;
  while (Date.now() - t0 < timeoutMs) {
    await new Promise(r => setTimeout(r, 1500));
    v = await view(page); if (!v) { n = 0; continue; }
    const line = JSON.stringify([v.run && v.run.status, v.run && v.run.step, v.working, v.sheets, v.rows, v.sets]);
    if (line !== last) { log(`run ${v.run && v.run.status}/${v.run && v.run.step}${v.run && v.run.stoppedBy ? ' STOP:' + v.run.stoppedBy : ''} | working ${v.working} | ${v.sheets.join(' ')} | ${JSON.stringify(v.rows)} | ${v.sets.join(' ')}`); last = line; }
    const ok = v.run && ['processed', 'complete'].includes(v.run.status) && !v.run.busy && !v.working && !v.check.pending && also(v);
    n = ok ? n + 1 : 0; if (n >= calm) return v;
  }
  return Object.assign({ timeout: true }, v);
}

/** What the fake site holds once the run rests, and the checks that must hold whatever happened on the way. `all` are the receipt ids of every order Etsy has shown. */
async function outcome(H, all) {
  const { st, page } = H;
  const sets = st.list('Charm_Nest_Sets'), sheets = st.list('Charm_Nest_Sheets').filter(s => !s.archived), pools = st.list('Charm_Pool'), ledger = st.list('Design_Completed Orders').map(x => String(x._id)).sort();
  const rows = await page.evaluate(() => Orders.rows().map(r => ({ rid: String(r.order.receiptId), state: r.state, ids: r.poolIds })));
  return { sets, sheets, pools, ledger, rows };
}
function check(o, all, { committedOnly = false } = {}) {
  const { sets, sheets, pools, ledger, rows } = o;
  // none lost, none twice: every order has exactly its one live pool row and sits on exactly one saved sheet
  const live = pools.filter(p => !['abandoned', 'superseded'].includes(p.state));
  const byOrder = new Map(); for (const p of live) { const k = String(p.poolId).split('_')[0]; byOrder.set(k, (byOrder.get(k) || 0) + 1); }
  for (const rid of all) assert.equal(byOrder.get(rid) || 0, 1, `order ${rid} has one live piece (has ${byOrder.get(rid) || 0})`);
  const onSheets = new Map(); for (const s of sheets) for (const id of s.poolIds || []) onSheets.set(id, (onSheets.get(id) || []).concat(s.id));
  for (const p of live) if (!committedOnly || true) assert.equal((onSheets.get(p.poolId) || []).length, 1, `piece ${p.poolId} is on exactly one sheet (${JSON.stringify(onSheets.get(p.poolId) || [])})`);
  // the sorter and the station agree on what is done
  const committedHere = new Set(sets.filter(s => s.committedAt).flatMap(s => (s.committed || []).map(String)));
  assert.deepStrictEqual([...committedHere].sort(), ledger, 'the orders the sorter calls committed are the orders the station marked design-complete');
  const lines = [...new Set(rows.filter(r => r.state === 'committed').map(r => r.rid))].sort();
  assert.deepStrictEqual(lines, ledger, 'and the order lines on screen say the same');
  // a set committed twice, or two sets with one number
  const seqs = sets.filter(s => s.status !== 'superseded').map(s => `${s.day}#${s.seq}`); assert.equal(new Set(seqs).size, seqs.length, 'set numbers are not repeated: ' + seqs.join(' '));
  const inSets = new Map(); for (const s of sets.filter(x => x.committedAt)) for (const rid of (s.committed || []).map(String)) inSets.set(rid, (inSets.get(rid) || 0) + 1);
  for (const [rid, n] of inSets) assert.equal(n, 1, `order ${rid} is committed in one set (in ${n})`);
  return { committed: committedHere.size, sets: sets.filter(s => s.committedAt).length };
}
module.exports = { receipt, wave, MASTERS, view, rest, due, outcome, check, GF, SS };
