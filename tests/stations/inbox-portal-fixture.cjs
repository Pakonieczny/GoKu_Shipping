// A fake inbox backend (plans/stations-round2/api.md: IN1's `personInbox`, `personOrders` with station "inbox", and the inbox block of `live`) for the
// portal's tests and screenshots (charm-nest-efficiency-person.js Inbox section, charm-nest-efficiency-stations.js Inbox card).
// FAKE DATA ONLY: two invented people, 400 invented customers, invented order numbers, drawn pictures as data addresses, a fake passcode.
// No network, no Firestore. It sits on top of the person fixture of the employee page (everything else, `person` and so on, is that fixture's).
//   const F = require('./inbox-portal-fixture.cjs'); const fx = F.make({ now }); fx.answer(body) -> { status, json }
//   People: 'Ana M.' (about nine replies on a working day, a few on some weekends) and 'Ben R.' (a known operator who has sent nothing).
//   Any other name is "found:false". Sent replies are recorded from `knownFrom` (150 days ago) on: the days before are UNKNOWN (null), never zero. Customers come in 3-day clusters of about 14, so one customer may get several messages within days.
//   fx.state.calls logs { op, name, range, day, from, to, q, cursor, top, compare } of every call the fixture answers itself (never the key)
//   fx.state.failInbox = 2 fails the next two personInbox calls with 503 · fx.state.unsupported = true answers personInbox with 400 "unknown op"
//   fx.state.notes = ['...'] is what the answer's `notes` say · fx.setDelay(fn(body) -> ms) · fx.setInbox('two' | 'quiet' | 'none' | 'old') is the board's inbox station
//   fx.truth(name, body) is the answer itself (no call is logged) · fx.rowsOf(name, from, to) the orders covered · fx.agg(name, from, to) the plain sums
const PF = require('../charm-nest/efficiency-person-fixture.cjs');
const KEY = PF.KEY, ymd = PF.ymd, addDays = PF.addDays;
const HR = 3600000, DAY = 86400000;
const NYF = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit' });
const dayMs = d => Date.parse(d + 'T12:00:00Z');
const diff = (a, b) => Math.round((dayMs(b) - dayMs(a)) / DAY);
const dow = d => new Date(dayMs(d)).getUTCDay();
const mondayOf = d => addDays(d, -((dow(d) + 6) % 7));
const rnd = s => { let h = 2166136261; for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 16777619); } return ((h >>> 0) % 100000) / 100000; };
const r1 = v => Math.round(v * 10) / 10, r2 = v => Math.round(v * 100) / 100;
function midnight(day) { let t = dayMs(day) - 12 * HR + 4 * HR; for (let i = 0; i < 6 && NYF.format(new Date(t - 1)) === day; i++) t -= HR; for (let i = 0; i < 6 && NYF.format(new Date(t)) !== day; i++) t += HR; return t; }
const RANGES = { day: 1, week: 7, month: 30, quarter: 90, year: 365 };
const MONTHS = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'];
const FIRST = ['Maya', 'Theo', 'Priya', 'Lena', 'Marcus', 'Sofia', 'Noah', 'Hana', 'Isabel', 'Dmitri', 'Amara', 'Callum', 'Ines', 'Jonas', 'Keiko', 'Luca', 'Mira', 'Omar', 'Petra', 'Quinn'];
const LAST = ['Lindgren', 'Okafor', 'Raman', 'Hoffmann', 'Bell', 'Alvarez', 'Fitzgerald', 'Sato', 'Moreau', 'Volkov', 'Nwosu', 'Reid', 'Costa', 'Dahl', 'Egan', 'Frost', 'Greco', 'Hale', 'Iyer', 'Joshi'];
const CUST = Array.from({ length: 400 }, (_, i) => `${FIRST[i % 20]} ${LAST[(Math.floor(i / 20) + (i % 20) * 7) % 20]}`);   // (400 invented customers, every name different)
const ridOf = (c, k) => String(3521000000 + c * 100 + k * 7 + 5);
const median = a => { if (!a.length) return null; const s = a.slice().sort((x, y) => x - y), m = s.length >> 1; return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; };

function make(opts = {}) {
  const t0 = Date.now(), base = opts.now || Date.now(), now = () => base + (Date.now() - t0);
  const pf = opts.pf || PF.make({ now: base, key: opts.key, orders: 60 }), today = pf.today, knownFrom = addDays(today, -150);
  const st = { calls: [], failInbox: 0, unsupported: false, notes: [], delay: null, key: pf.state.key, inbox: 'two', inboxAt: now(), mode: 'real' };
  const PEOPLE = { 'ana m.': { name: 'Ana M.', key: 'ana m', n: 9 }, 'ben r.': { name: 'Ben R.', key: 'ben r', n: 0 } };
  const personOf = name => PEOPLE[String(name || '').toLowerCase()] || null;

  /* ── one day of replies: the same every time it is asked ── */
  const cache = new Map();
  function repliesOf(P, day) {
    if (!P || !P.n || day < knownFrom || day > today) return [];
    const k = P.key + '|' + day; if (cache.has(k)) return cache.get(k);
    const w = dow(day), mid = midnight(day), r = s => rnd(P.key + day + s), out = [];
    const n = w === 0 || w === 6 ? (r('we') < .25 ? 1 + Math.floor(r('wn') * 3) : 0) : r('off') < .08 ? 0 : Math.max(2, Math.round(P.n * (.55 + r('n') * .9)));
    for (let i = 0; i < n; i++) {
      const c = (Math.floor(diff(knownFrom, day) / 3) * 11 + Math.floor(Math.pow(r('c' + i), 1.6) * 14)) % CUST.length, o = Math.floor(r('o' + i) * 3), msgs = r('m' + i) < .24 ? 2 : 1, h = 8 + Math.floor(r('h' + i) * 9), t = mid + h * HR + Math.floor(r('i' + i) * 60) * 60000;
      if (day === today && t >= base) continue;   // (today only has what was sent before the run's clock)
      out.push({ t, c, o, rid: ridOf(c, o), msgs, hour: h, day });
    }
    out.sort((a, b) => a.t - b.t); cache.set(k, out); return out;
  }
  const unknownOf = day => (day < knownFrom || day > today || dow(day) === 0 || dow(day) === 6 || rnd('unk' + day) > .14 ? 0 : 1);   // (replies sent before any name was recorded, everybody's)
  const knownDaysIn = (d0, d1) => { const a = d0 < knownFrom ? knownFrom : d0, b = d1 > today ? today : d1; return b < a ? 0 : diff(a, b) + 1; };

  /** The plain sums of one person over [from, to] (days before knownFrom contribute nothing and are not counted as known). */
  function agg(P, from, to) {
    const rs = [], per = new Map(), rids = new Set(), act = new Set(), hours = Array.from({ length: 24 }, (_, h) => ({ hour: h, replies: 0, messages: 0 }));
    for (let d = from; d <= to; d = addDays(d, 1)) for (const x of repliesOf(P, d)) {
      rs.push(x); rids.add(x.rid); act.add(d); hours[x.hour].replies++; hours[x.hour].messages += x.msgs;
      const p = per.get(x.c) || { c: x.c, messages: 0, replies: 0, orders: new Set(), lastAt: 0, rid: '' }; p.messages += x.msgs; p.replies++; p.orders.add(x.rid); if (x.t >= p.lastAt) { p.lastAt = x.t; p.rid = x.rid; } per.set(x.c, p);
    }
    const messages = rs.reduce((n, x) => n + x.msgs, 0), counts = [...per.values()].map(p => p.messages);
    return { replies: rs.length, orders: rids.size, customers: per.size, messages, activeDays: act.size, hours, per, knownDays: knownDaysIn(from, to), counts,
      average: per.size ? r2(messages / per.size) : null, median: median(counts), max: counts.length ? Math.max(...counts) : null };
  }

  /* ── a window: rolling days ending on `day` (default today), or {from,to} ── */
  function windowOf(b) {
    let to = /^\d{4}-\d{2}-\d{2}$/.test(b.day || '') ? b.day : today; if (to > today) to = today;
    if (b.range && typeof b.range === 'object') { const f = b.range.from, t = b.range.to > today ? today : b.range.to; return { from: f, to: t, days: diff(f, t) + 1 }; }
    const n = RANGES[b.range] || 7; return { from: addDays(to, -(n - 1)), to, days: n };
  }
  function bucket(P, d0, d1) {
    const days = diff(d0, d1) + 1;
    if (!knownDaysIn(d0, d1)) return { day: d0, to: d1, days, replies: null, orders: null, customers: null, messages: null, activeDays: null };
    const A = agg(P, d0, d1), o = { day: d0, to: d1, days, replies: A.replies, orders: A.orders, customers: A.customers, messages: A.messages, activeDays: A.activeDays };
    if (days === 1) { const rs = repliesOf(P, d0); if (rs.length) { o.firstAt = rs[0].t; o.lastAt = rs[rs.length - 1].t; } }
    return o;
  }
  function seriesOf(P, w) {
    if (w.days <= 92) { const out = []; for (let d = w.from; d <= w.to; d = addDays(d, 1)) out.push(bucket(P, d, d)); return { granularity: 'day', series: out }; }
    const mons = new Map(); for (let d = w.from; d <= w.to; d = addDays(d, 1)) { const m = mondayOf(d), k = m < w.from ? w.from : m; if (!mons.has(k)) mons.set(k, []); mons.get(k).push(d); }
    return { granularity: 'week', series: [...mons.entries()].map(([k, l]) => bucket(P, k, l[l.length - 1])) };
  }
  const MT = (label, unit, v, pv, def) => { const delta = v != null && pv != null ? r2(v - pv) : null; return { label, unit, value: v, prev: pv, delta, deltaPct: delta != null && pv ? r1(delta / Math.abs(pv) * 100) : null, better: null, def, estimated: false }; };

  function personInbox(b) {
    const P = personOf(b.name), w = windowOf(b), compare = b.compare !== false, pw = compare ? { from: addDays(w.from, -w.days), to: addDays(w.from, -1), days: w.days } : null;
    const head = { ok: true, now: now(), mode: st.mode, name: P ? P.name : String(b.name || ''), range: b.range, from: w.from, to: w.to, days: w.days, today, live: w.to === today, prev: pw, knownFrom };
    if (!P) return Object.assign(head, { found: false, spellings: [], granularity: 'day', totals: {}, series: [], hours: [], perCustomer: null, unknown: { replies: 0, messages: 0 }, notes: ['Nothing has been recorded under this name yet.'], partial: false, errors: [] });
    const known = knownDaysIn(w.from, w.to) > 0, A = agg(P, w.from, w.to), Q = pw && knownDaysIn(pw.from, pw.to) > 0 ? agg(P, pw.from, pw.to) : null, se = seriesOf(P, w);
    const v = f => (known ? f(A) : null), pv = f => (Q ? f(Q) : null);
    const per = (a, f) => (a.customers ? f(a) : null), rpd = a => (a.activeDays ? r1(a.replies / a.activeDays) : null);
    const totals = { replies: MT('Replies sent', 'count', v(a => a.replies), pv(a => a.replies), 'Replies sent from the inbox.'), orders: MT('Orders covered', 'orders', v(a => a.orders), pv(a => a.orders), 'Different orders those replies were about.'),
      customers: MT('Customers', 'count', v(a => a.customers), pv(a => a.customers), 'Different customers who got a reply.'), messages: MT('Messages sent', 'count', v(a => a.messages), pv(a => a.messages), 'Messages sent to customers.'),
      messagesPerCustomer: MT('Messages per customer', 'count', known ? per(A, a => a.average) : null, Q ? per(Q, a => a.average) : null, 'Messages divided by customers.'), maxPerCustomer: MT('Most to one customer', 'count', known ? A.max : null, Q ? Q.max : null, 'The most messages one customer got.'),
      repliesPerDay: MT('Replies per day with replies', 'count', known ? rpd(A) : null, Q ? rpd(Q) : null, 'Replies divided by days with replies.'), daysActive: MT('Days with replies', 'days', v(a => a.activeDays), pv(a => a.activeDays), 'Days with at least one reply.') };
    const top = Math.max(1, Math.min(25, +b.top || 5)), rows = [...A.per.values()].sort((x, y) => y.messages - x.messages || y.replies - x.replies || y.lastAt - x.lastAt).slice(0, top).map(p => ({ customer: CUST[p.c], messages: p.messages, replies: p.replies, orders: p.orders.size, lastAt: p.lastAt, rid: p.rid }));
    const dist = [1, 2, 3, 4, 5].map(m => ({ messages: m, customers: A.counts.filter(n => (m === 5 ? n >= 5 : n === m)).length }));
    dist[4].plus = true;
    let un = 0, unm = 0; for (let d = w.from; d <= w.to; d = addDays(d, 1)) { const n = unknownOf(d); un += n; unm += n; }
    const partial = known && w.from < knownFrom, notes = st.notes.slice();
    return Object.assign(head, { found: true, spellings: [P.name], granularity: se.granularity, totals, series: se.series, hours: known ? A.hours : A.hours.map(h => ({ hour: h.hour, replies: null, messages: null })),
      perCustomer: known ? { average: per(A, a => a.average), median: A.median, max: A.max, total: A.customers, distribution: dist, top: rows } : { average: null, median: null, max: null, total: null, distribution: [], top: [] },
      unknown: { replies: un, messages: unm }, notes, partial: false, errors: [], knownDays: knownDaysIn(w.from, w.to), partialWindow: partial });
  }

  /* ── the orders covered (personOrders, station "inbox"): one row per order the person sent a reply on, newest first ── */
  function rowsOf(name, from, to) {
    const P = personOf(name); if (!P) return [];
    const by = new Map();
    for (let d = from; d <= to; d = addDays(d, 1)) for (const x of repliesOf(P, d)) { const o = by.get(x.rid) || { rid: x.rid, c: x.c, k: x.o, replies: 0, messages: 0, at: 0 }; o.replies++; o.messages += x.msgs; if (x.t > o.at) o.at = x.t; by.set(x.rid, o); }
    return [...by.values()].sort((a, b) => b.at - a.at).map(o => {
      const T = pf.orders.orders[(o.c * 3 + o.k) % pf.orders.orders.length], pieces = T.pieces.map((p, i) => Object.assign({}, p, { id: o.rid + '-' + (i + 1) }));
      return Object.assign({}, T, { rid: o.rid, number: o.rid, customer: CUST[o.c], at: o.at, day: NYF.format(new Date(o.at)), station: 'inbox', stations: ['inbox'], durationMs: 0, spanMs: 0, scans: 0, completes: 0, prints: 0, parts: 0, undone: 0, rejected: 0, errors: 0,
        steps: [], issues: [], qr: { text: o.rid }, pieces, piecesCount: pieces.length, info: true, replies: o.replies, messages: o.messages });
    });
  }
  const hit = (o, q) => { const ws = String(q || '').toLowerCase().split(/\s+/).filter(Boolean); if (!ws.length) return true; const [, m, d] = o.day.split('-').map(Number), hay = [o.number, o.customer, o.day, `${MONTHS[m - 1]} ${d}`, `${m}/${d}`].join(' | ').toLowerCase(); return ws.every(x => hay.includes(x)); };
  function personOrders(b) {
    const P = personOf(b.name), from = b.from || addDays(today, -6), to = b.to || today;
    if (!P) return { ok: true, now: now(), mode: st.mode, name: b.name, found: false, q: b.q || '', total: 0, scanned: 0, searched: { orders: 0, withDetails: 0 }, orders: [], next: null, notes: [] };
    const all = rowsOf(P.name, from, to), list = all.filter(o => hit(o, b.q)), limit = Math.max(1, Math.min(100, +b.limit || 25)), off = /^o(\d+)$/.test(b.cursor || '') ? +b.cursor.slice(1) : 0;
    return { ok: true, now: now(), mode: st.mode, name: P.name, found: true, total: list.length, scanned: all.length, searched: { orders: all.length, withDetails: all.length }, orders: list.slice(off, off + limit), next: off + limit < list.length ? 'o' + (off + limit) : null, notes: [] };
  }

  /* ── the board: the console's `live` read (the person fixture's), with the Inbox station of the chosen kind ── */
  function live() {
    const j = pf.live(), at = now(), t1 = st.inboxAt, mode = st.inbox;   // (the people's times are fixed when the mode is set, so "time since last input" grows by itself, as it does for a real page)
    j.stations = j.stations.filter(s => s.key !== 'sorter' && s.key !== 'qr');
    const ib = j.stations.find(s => s.key === 'inbox'); if (!ib) return j;
    const P = (name, since, input, device) => ({ name, since: t1 - since, lastSeenAt: at - 4000, lastInputAt: input == null ? null : t1 - input, device: device || 'etsy-mail-1', deviceLabel: 'Inbox 1' });
    const people = mode === 'two' ? [P('Rae T.', 3.1 * HR, 38000), P('Dev K.', 52 * 60000, null, 'etsy-mail-2')] : mode === 'quiet' ? [P('Rae T.', 95 * 60000, 14 * 60000)] : [];
    ib.people = people; ib.state = people.length ? 'working' : 'offline'; ib.lastEventAt = t1 - (mode === 'quiet' ? 14 * 60000 : 30000);
    ib.counts = { partsToday: 0, ordersToday: 0, scansToday: 0 };
    if (mode === 'two') ib.inbox = { day: today, replies: 16, orders: 11, customers: 9, messages: 19, unknown: 2, byPerson: [{ name: 'Rae T.', replies: 9, orders: 7, customers: 6, messages: 11 }, { name: 'Dev K.', replies: 5, orders: 4, customers: 3, messages: 6 }] };
    else if (mode === 'quiet') ib.inbox = { day: today, replies: 0, orders: 0, customers: 0, messages: 0, unknown: 0, byPerson: [] };
    else if (mode === 'none') ib.inbox = { day: today, replies: 3, orders: 3, customers: 3, messages: 3, unknown: 0, byPerson: [{ name: 'Rae T.', replies: 3, orders: 3, customers: 3, messages: 3 }] };
    else delete ib.inbox;
    j.signedIn = (j.signedIn || []).filter(x => x.stationKey !== 'inbox').concat(people.map(p => ({ name: p.name, stationKey: 'inbox', device: p.device, since: p.since, lastSeenAt: p.lastSeenAt })));
    return j;
  }

  const log = b => st.calls.push({ op: b.op, name: b.name, range: b.range, day: b.day, from: b.from, to: b.to, q: b.q, cursor: b.cursor, top: b.top, compare: b.compare, station: b.station, sandbox: b.sandbox });
  function answer(b) {
    const mine = b.op === 'personInbox' || b.op === 'live' || (b.op === 'personOrders' && b.station === 'inbox');
    if (!mine) return pf.answer(b);
    log(b);
    if (b.key !== st.key) return { status: 401, json: { ok: false, error: 'unauthorized' } };
    if (b.op === 'live') return { status: 200, json: live() };
    if (b.op === 'personInbox') {
      if (st.unsupported) return { status: 400, json: { ok: false, error: 'unknown op' } };
      if (st.failInbox > 0) { st.failInbox--; return { status: 503, json: { ok: false, error: 'both reads failed' } }; }
      return { status: 200, json: personInbox(b) };
    }
    return { status: 200, json: personOrders(b) };
  }
  return { answer, state: st, pf, today, knownFrom, CUST, ymd, addDays, personInbox, live, rowsOf: (n, f, t) => rowsOf(n, f, t), agg: (n, f, t) => agg(personOf(n), f, t),
    truth: (name, body) => personInbox(Object.assign({ op: 'personInbox', name, range: 'week', compare: true }, body || {})), setInbox(m) { st.inbox = m; st.inboxAt = now(); }, setDelay(f) { st.delay = f; pf.setDelay(f); }, delayFor(b) { return st.delay ? +st.delay(b) || 0 : pf.delayFor(b); } };
}
module.exports = { make, KEY, ymd, addDays, CUST };
