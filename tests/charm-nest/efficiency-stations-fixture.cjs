// A fake `live` feed (employeeEfficiency op "live", plans/employee-hr/api.md) for the stations board's tests and screenshots.
// FAKE DATA ONLY: invented people, invented order numbers and customers, pictures drawn here as small SVG data addresses. No network, no Firestore.
//   const F = require('./efficiency-stations-fixture.cjs'); const fx = F.make(); fx.answer(body) → { status, json }
//   fx.start(station, order) · fx.finish(station, person) · fx.signOut(name) · fx.clear() · fx.state.fail = n (the next n answers fail)
const KEY = 'fixture-pass-123';

const hue = n => (n * 47) % 360;
/** A small invented picture: a rounded tile with a shape on it (kind: photo, vector). */
const pic = (n, kind) => {
  const bg = `hsl(${hue(n)} 38% ${kind === 'vector' ? 97 : 82}%)`, fg = `hsl(${hue(n)} 42% 38%)`;
  const shape = kind === 'vector'
    ? `<path d="M60 22c14 0 24 10 24 24 0 20-24 42-24 42S36 66 36 46c0-14 10-24 24-24z" fill="none" stroke="${fg}" stroke-width="3"/><circle cx="60" cy="46" r="7" fill="none" stroke="${fg}" stroke-width="3"/>`
    : `<circle cx="60" cy="52" r="${24 + (n % 3) * 4}" fill="${fg}" opacity=".85"/><rect x="26" y="78" width="68" height="10" rx="5" fill="${fg}" opacity=".45"/>`;
  return 'data:image/svg+xml;utf8,' + encodeURIComponent(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 120 108" width="120" height="108"><rect width="120" height="108" fill="${bg}"/>${shape}</svg>`);
};
const BROKEN = '/__fixture/missing-picture.jpg';

function make(opts = {}) {
  const st = { calls: [], bad: 0, fail: 0, key: opts.key || KEY, http: null };
  const t0 = Date.now();
  const at = () => Date.now();
  const mk = (rid, o) => Object.assign({ rid, orderNumber: rid, customer: 'Fixture Customer', thumbUrl: pic(+rid.slice(-3), 'photo'), qr: { text: rid }, pieces: [], scannedAt: at() - 95000 }, o);
  const S = [
    { key: 'shipping', label: 'Shipping', people: [{ name: 'Michael V.', since: at() - 5 * 3600000, partsToday: 41, ordersToday: 12, medianOrderMs: 252000, longestIdleMs: 1320000 }], current: [mk('3521000101', { person: 'Michael V.', customer: 'Nora Fixture', pieces: [{ id: '3521000101_1', label: 'Piece 1', thumbUrl: pic(101, 'vector') }] })], lastEventAt: at() - 20000, counts: { partsToday: 41, ordersToday: 12 }, spark: [0, 2, 1, 4, 3, 5, 2, 6, 4, 3, 7, 5] },
    { key: 'assembly', label: 'Assembly', people: [{ name: 'Anna M.', since: at() - 4 * 3600000, partsToday: 33, ordersToday: 9, medianOrderMs: 312000, longestIdleMs: 900000 }, { name: 'Ivy R.', since: at() - 3 * 3600000 }],
      current: [mk('3521000202', { person: 'Anna M.', customer: 'Owen Fixture', scannedAt: at() - 410000, pieces: [{ id: '3521000202_1', label: 'Piece 1', thumbUrl: pic(202, 'vector') }, { id: '3521000202_2', label: 'Piece 2', thumbUrl: pic(203, 'vector') }] }),
        mk('3521000203', { person: 'Ivy R.', customer: 'Mara Fixture', scannedAt: at() - 38000 })], lastEventAt: at() - 8000, counts: { partsToday: 52, ordersToday: 17 }, spark: [1, 1, 3, 2, 5, 4, 4, 6, 3, 5, 6, 4] },
    { key: 'welding', label: 'Welding', people: [{ name: 'Giovanna C.', since: at() - 6 * 3600000 }],
      current: [mk('3521000303', { person: 'Giovanna C.', customer: 'Theo Fixture', scannedAt: at() - 252000, note: 'Waiting on the second piece', pieces: [{ id: '3521000303_1', label: 'Piece 1', thumbUrl: pic(301, 'vector') }, { id: '3521000303_2', label: 'Piece 2', thumbUrl: BROKEN }, { id: '3521000303_3', label: 'Piece 3', thumbUrl: pic(303, 'vector') }] })],
      lastEventAt: at() - 31000, counts: { partsToday: 64, ordersToday: 21 }, spark: [2, 3, 2, 5, 6, 4, 7, 5, 8, 6, 5, 7] },
    { key: 'sorting', label: 'Sorting', people: [{ name: 'Dana S.', since: at() - 2 * 3600000 }], current: [], lastEventAt: at() - 12 * 60000, counts: { partsToday: 18, ordersToday: 6 } },
    { key: 'design', label: 'Design', people: [], current: [], lastEventAt: null, counts: { partsToday: 0, ordersToday: 0 }, state: 'offline' },
    { key: 'sorter', label: 'Sorter / Laser', people: [{ name: 'Paul K.', since: at() - 12 * 3600000 }], current: [mk('3521000404', { person: 'Paul K.', customer: 'Lena Fixture', thumbUrl: '', scannedAt: at() - 3720000, pieces: [] })], lastEventAt: at() - 5000, counts: { partsToday: 29, ordersToday: 25 } },
    { key: 'inbox', label: 'Inbox', people: [{ name: 'Rae T.', since: at() - 1 * 3600000 }], current: [], lastEventAt: at() - 3 * 60000, counts: { partsToday: 0, ordersToday: 0 } }
  ];
  const find = k => S.find(s => s.key === k);
  const stateOf = s => (s.state === 'offline' && !s.people.length ? 'offline' : s.current.length ? 'working' : s.people.length ? 'idle' : 'offline');
  const signed = () => S.flatMap(s => s.people.map(p => ({ name: p.name, stationKey: s.key, since: p.since || at() - 3600000, lastSeenAt: at() - 4000 })));
  function live(sandbox) {
    return { ok: true, at: at(), mode: sandbox ? 'sandbox' : 'real', stations: S.map(s => Object.assign({}, s, { state: stateOf(s), people: s.people.map(p => Object.assign({}, p)), current: s.current.map(c => Object.assign({}, c)) })), signedIn: signed() };
  }
  function answer(body) {
    st.calls.push({ op: body.op, key: body.key, sandbox: body.sandbox === true });
    if (st.http) return { status: st.http.status, json: st.http.json };
    if (body.key !== st.key) { st.bad++; return { status: 401, json: { ok: false, error: 'unauthorized' } }; }
    if (st.fail > 0) { st.fail--; return { status: 503, json: { ok: false, error: 'both reads failed' } }; }
    if (body.op === 'live') return { status: 200, json: st.hook ? st.hook(live(body.sandbox === true), body) : live(body.sandbox === true) };
    return { status: 400, json: { ok: false, error: 'bad op' } };
  }
  return {
    answer, state: st, KEY, now: at,
    /** An order is scanned at a station by a person. */
    start(station, o) { const s = find(station), rid = String(o.rid); const p = o.person; if (!s.people.some(x => x.name === p)) s.people.push({ name: p, since: at() - 1800000 }); s.current.push({ rid, orderNumber: rid, customer: o.customer || 'Fixture Customer', thumbUrl: o.thumbUrl === undefined ? pic(+rid.slice(-3), 'photo') : o.thumbUrl, qr: { text: rid }, pieces: o.pieces || [], scannedAt: o.scannedAt || at(), person: p, note: o.note }); s.lastEventAt = at(); },
    /** The person's order is done. */
    finish(station, person) { const s = find(station); const i = s.current.findIndex(c => !person || c.person === person); if (i >= 0) s.current.splice(i, 1); s.lastEventAt = at(); if (s.counts) { s.counts.ordersToday += 1; s.counts.partsToday += 2; } },
    signOut(name) { for (const s of S) s.people = s.people.filter(p => p.name !== name); },
    clear() { for (const s of S) { s.current = []; } },
    wipe() { S.length = 0; },
    setHook(f) { st.hook = f || null; }, find, pic, BROKEN
  };
}
module.exports = { make, KEY, pic, BROKEN };
