// A fake `personOrders` (plans/employee-hr/api.md, E4 section) for the order list's tests and screenshots (charm-nest-efficiency-orders.js).
// FAKE DATA ONLY: invented customers, invented order numbers, drawn pictures as data addresses, a fake passcode. No network, no Firestore.
//   const F = require('./efficiency-orders-fixture.cjs'); const fx = F.make(); fx.answer(body) → { status, json }
//   fx.add(n) makes n new orders at the top (as if the person just worked them) · fx.touch(i) takes order i to the top (worked again)
//   fx.state.fail = 2 fails the next two calls with 503 · fx.state.notes = ['...'] is what the answer's `notes` say · `sort` is ignored, like E4's
//   fx.state.calls logs { op, q, cursor, limit, from, to, station, name } of every call (never the key) · fx.setDelay(fn(body) → ms)
const KEY = 'fixture-pass-123';
const NY_DAY = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit' });
const ymd = t => NY_DAY.format(new Date(t));
const MONTHS = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'];
const CUSTOMERS = ['Maya Lindgren', 'Theo Okafor', 'Priya Raman', 'Lena Hoffmann', 'Marcus Bell', 'Sofia Alvarez', 'Noah Fitzgerald', 'Hana Sato', 'Isabel Moreau', 'Dmitri Volkov', 'Amara Nwosu', 'Callum Reid'];
const CHARMS = [['Heart charm', 'HEART-14'], ['Star charm', 'STAR-10'], ['Initial A', 'INIT-A'], ['Moon charm', 'MOON-22'], ['Paw print', 'PAW-05'], ['Anchor charm', 'ANCH-08'], ['Birth flower', 'FLWR-12'], ['Compass', 'COMP-03']];
const STATIONS = ['sorting', 'welding', 'assembly', 'shipping'];
const COLORS = ['#c8a24e', '#8d95a0', '#c08578', '#b08d2a', '#d9b545'];
/** A small drawn picture (a data address): no file, no network. */
const pic = (i, label) => 'data:image/svg+xml,' + encodeURIComponent(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64"><rect width="64" height="64" fill="#faf7f1"/><circle cx="32" cy="30" r="${14 + (i % 4) * 2}" fill="${COLORS[i % COLORS.length]}"/><text x="32" y="58" font-size="8" text-anchor="middle" fill="#5b554c" font-family="sans-serif">${String(label || '').slice(0, 10)}</text></svg>`);

function make(opts = {}) {
  const t0 = Date.now(), base = opts.now || Date.now();
  const st = { calls: [], fail: 0, key: opts.key || KEY, delay: null, added: 0, mode: 'real', person: opts.person || 'Giovanna' };
  const now = () => base + (Date.now() - t0);
  const count = opts.count == null ? 60 : opts.count;
  const orders = [];
  /** One invented order: i = 0 is the newest. A few have many pieces, a few no stored details (the Sandbox or an unstored receipt). */
  function build(i, at) {
    const np = i % 11 === 3 ? 7 : 1 + (i % 4), bare = i % 13 === 5, rid = String(3521000100 + i * 37);
    const pieces = bare ? [] : Array.from({ length: np }, (_, k) => { const c = CHARMS[(i + k) % CHARMS.length]; return { id: rid + '-' + (k + 1), label: c[0], sku: c[1], thumbUrl: i % 7 === 4 && k === 1 ? '' : pic(i + k, c[0]) }; });
    const station = STATIONS[i % STATIONS.length], dur = [252000, 41000, 3720000, 95000, 618000, 12000, 1500][i % 7];
    const undone = i % 9 === 2 ? 1 : 0, rejected = i % 17 === 6 ? 1 : 0;
    const issues = [];
    if (undone) issues.push({ kind: 'undone', label: 'Completion undone', at: at + 60000, note: '' });
    if (rejected) issues.push({ kind: 'refused', label: 'Piece refused', at: at + 90000, note: 'scratched' });
    if (i % 15 === 4) issues.push({ kind: 'reprint', label: 'Label reprinted', at: at + 30000, note: '' });
    return { rid, number: rid, at, day: ymd(at), station, stations: i % 5 === 1 ? [station, STATIONS[(i + 1) % 4]] : [station], durationMs: dur, spanMs: dur + 90000,
      scans: 2 + (i % 5), completes: i % 6 === 5 ? 0 : 1, prints: i % 3, parts: np, undone, rejected, errors: 0,
      steps: [{ station, firstAt: at, lastAt: at + dur, durationMs: dur, scans: 2 + (i % 5), completes: 1, prints: i % 3, parts: np }], issues,
      customer: bare ? '' : CUSTOMERS[i % CUSTOMERS.length], info: !bare, thumbUrl: bare ? '' : pic(i, 'order'), qr: { text: rid }, pieces, piecesCount: pieces.length };
  }
  for (let i = 0; i < count; i++) orders.push(build(i, base - 4 * 60000 - i * 11 * 60000));
  /** Does an order match every word of the query (order number, customer, piece label or SKU, station, date, issue word)? */
  function hit(o, q) {
    const ws = String(q || '').toLowerCase().split(/\s+/).filter(Boolean); if (!ws.length) return true;
    const [y, m, d] = o.day.split('-').map(Number);
    const hay = [o.number, o.customer, ...o.pieces.flatMap(p => [p.label, p.sku]), ...o.stations, o.day, `${MONTHS[m - 1]} ${d}`, `${m}/${d}`, ...o.issues.flatMap(i => [i.kind, i.label])].join(' | ').toLowerCase();
    return ws.every(w => hay.includes(w));
  }
  function personOrders(b) {
    if (b.name && String(b.name).toLowerCase() !== st.person.toLowerCase()) return { ok: true, now: now(), mode: st.mode, name: b.name, found: false, q: b.q || '', total: 0, scanned: 0, searched: { orders: 0, withDetails: 0 }, orders: [], next: null, notes: [] };
    const limit = Math.max(1, Math.min(100, +b.limit || 25)), off = /^o(\d+)$/.test(b.cursor || '') ? +b.cursor.slice(1) : 0;
    let list = orders.filter(o => (!b.from || o.day >= b.from) && (!b.to || o.day <= b.to) && (!b.station || o.stations.includes(b.station)));
    const scanned = list.length; list = list.filter(o => hit(o, b.q));
    // (E4 lists newest first by the last action and ignores `sort` today: so does this)
    const page = list.slice(off, off + limit), next = off + limit < list.length ? 'o' + (off + limit) : null;
    return { ok: true, now: now(), mode: st.mode, name: b.name, found: true, total: list.length, scanned, searched: { orders: scanned, withDetails: Math.min(scanned, st.withDetails == null ? scanned : st.withDetails) },
      orders: page, next, notes: st.notes || [] };
  }
  /** What the harness answers to a POST body (the passcode is the only gate). */
  function answer(body) {
    st.calls.push({ op: body.op, q: body.q, cursor: body.cursor, limit: body.limit, from: body.from, to: body.to, station: body.station, name: body.name, sort: body.sort, sandbox: body.sandbox });
    if (body.key !== st.key) return { status: 401, json: { ok: false, error: 'unauthorized' } };
    if (st.fail > 0) { st.fail--; return { status: 503, json: { ok: false, error: 'both reads failed' } }; }
    if (body.op !== 'personOrders') return { status: 400, json: { ok: false, error: 'bad op' } };
    return { status: 200, json: personOrders(body) };
  }
  /** n new orders at the top, worked just now (the newest first). */
  function add(n = 1) {
    const fresh = [];
    for (let k = 0; k < n; k++) { st.added++; const o = build(1000 + st.added, now() - k * 1000); o.rid = String(3529000000 + st.added); o.number = o.rid; o.qr = { text: o.rid }; o.customer = 'Fresh Customer ' + st.added; fresh.push(o); }
    orders.unshift(...fresh);   // (the first of them is the newest)
  }
  /** An order worked again: it takes the top of the list (the server lists newest first by the LAST action, which is not the first action's time). */
  function touch(i) {
    const o = orders.splice(i, 1)[0], t = now(); o.steps[0].lastAt = t; o.issues = o.issues.concat([{ kind: 'reprint', label: 'Label reprinted', at: t, note: '' }]);
    orders.unshift(o); return o;
  }
  return { answer, add, touch, state: st, orders, hit, setDelay(f) { st.delay = f; }, delayFor(body) { return st.delay ? +st.delay(body) || 0 : 0 } };
}
module.exports = { make, KEY, ymd, pic };
