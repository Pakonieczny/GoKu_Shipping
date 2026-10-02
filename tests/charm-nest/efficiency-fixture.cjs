// A fake `employeeEfficiency` (contract.md, "Console API") for the Employee efficiency console's tests and screenshots.
// FAKE DATA ONLY: four invented people, invented order numbers, a fake passcode. No network, no Firestore.
//   const F = require('./efficiency-fixture.cjs'); const fx = F.make(); fx.answer(body) → { status, json }   fx.bump() adds work.
const KEY = 'fixture-pass-123';
const NY_HOUR = new Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York', hourCycle: 'h23', hour: 'numeric' });
const NY_DAY = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit' });
const ymd = t => NY_DAY.format(new Date(t));
const addDays = (day, n) => { const [y, m, d] = day.split('-').map(Number); return new Date(Date.UTC(y, m - 1, d + n)).toISOString().slice(0, 10); };
const nyAt = (day, h, m) => { // a New York clock time of that day, in ms (EDT/EST aware by search)
  const [y, mo, d] = day.split('-').map(Number); let t = Date.UTC(y, mo - 1, d, h + 4, m);
  for (let i = 0; i < 2; i++) { const got = +NY_HOUR.format(new Date(t)); t += (h - got) * 3600000; }
  return t;
};
const arr = map => { const a = new Array(24).fill(0); for (const k of Object.keys(map)) a[+k] = map[k]; return a; };
const sum = (...as) => { const o = new Array(24).fill(0); for (const a of as) a.forEach((v, i) => { o[i] += v; }); return o; };
const total = a => a.reduce((n, v) => n + v, 0);

/** The people of the shop day: station hours (parts per New York hour), and what each did. */
function crew() {
  return [
    { name: 'Giovanna', in: [7, 52], out: null, stations: { welding: { h: arr({ 8: 14, 9: 28, 10: 31, 11: 22, 12: 9, 13: 30, 14: 34, 15: 19 }), min: 330, scans: 121, orders: 24 }, sorting: { h: arr({ 9: 6, 10: 8, 13: 11, 14: 7, 15: 4 }), min: 95, scans: 65, orders: 18 } }, active: 392, idle: 33, scans: 186, orders: 37, now: ['welding'] },
    { name: 'Anna', in: [8, 3], out: null, stations: { assembly: { h: arr({ 8: 9, 9: 17, 10: 21, 11: 19, 12: 6, 13: 20, 14: 23, 15: 12 }), min: 458, scans: 143, orders: 29 } }, active: 361, idle: 71, scans: 143, orders: 29, now: ['assembly'] },
    { name: 'Michael', in: [8, 0], back: [13, 10], out: null, stations: { shipping: { h: arr({ 8: 11, 9: 15, 10: 18, 11: 14, 13: 9, 14: 16, 15: 8 }), min: 372, scans: 131, orders: 33 } }, active: 301, idle: 52, scans: 131, orders: 33, now: ['shipping'] },
    { name: 'Ivy', in: [8, 15], out: [14, 15], stations: { design: { h: arr({ 8: 5, 9: 7, 10: 9, 11: 6, 12: 3, 13: 8, 14: 4 }), min: 360, scans: 64, orders: 12 } }, active: 288, idle: 44, scans: 64, orders: 12, now: [] }
  ];
}

function make(opts = {}) {
  const t0 = Date.now(), base = opts.now || Date.UTC(2026, 9, 2, 19, 42);   // 2 Oct 2026, 3:42 PM New York
  const st = { bumps: 0, calls: [], wrong: 0, fail: 0, key: opts.key || KEY };
  const now = () => base + (Date.now() - t0);
  const today = () => ymd(now());
  const people = crew();
  const fakeOrder = (i, k) => String(3521000100 + i * 37 + k * 5);

  function dayPeople(day, bumped) {
    const isToday = day === today();
    return people.map((p, pi) => {
      const hrs = Object.values(p.stations).map(s => s.h.slice());
      if (isToday && bumped && p.now.length) { const st0 = Object.values(p.stations)[0]; hrs[0][+NY_HOUR.format(new Date(now()))] += bumped * 3; }
      const perHour = sum(...hrs), parts = total(perHour) * (isToday ? 1 : 1 + ((pi + day.charCodeAt(9)) % 5) / 10) | 0;
      const stations = Object.entries(p.stations).map(([name, s], si) => ({ station: name, minutes: s.min, parts: total(hrs[si]), scanParts: s.scans + 3 * si, scans: s.scans, completes: Math.round(total(hrs[si]) / 4), prints: 8 + pi + si, orders: s.orders }));
      const orders = [];
      for (let k = 0; k < 30; k++) orders.push({ orderId: fakeOrder(pi, k), stations: Object.keys(p.stations).slice(0, 1 + (k % Object.keys(p.stations).length)), parts: 2 + ((k * 7 + pi) % 9), lastAt: now() - (k * 7 + 3 + pi) * 60000 });
      const firstIn = nyAt(day, p.in[0], p.in[1]), on = isToday && !p.out;
      return { name: p.name, status: on ? 'on' : 'out', firstIn, lastOut: on ? null : nyAt(day, (p.out || [16, 30])[0], (p.out || [16, 30])[1]), onSince: on ? (p.back ? nyAt(day, p.back[0], p.back[1]) : firstIn) : null, inDay: day,
        nowAt: on ? p.now : [], source: 'events', stations,
        totals: { parts: total(perHour), scanParts: p.scans + 9, scans: p.scans, orders: p.orders, rejects: pi, errors: pi % 2, activeMin: p.active, idleMin: p.idle, signedInMin: p.active + p.idle + 17 + pi, rate: Math.round(total(perHour) / (p.active / 60) * 10) / 10, secPerScan: Math.round(p.active * 60 / p.scans * 10) / 10 },
        perHour, orders };
    });
  }
  function overview(b) {
    const days = [1, 7, 30].includes(+b.days) ? +b.days : 1, end = b.day && b.day <= today() ? b.day : today();
    let ppl = dayPeople(end, st.bumps);
    if (days > 1) ppl = ppl.map((p, i) => Object.assign({}, p, { status: i === 0 || i === 1 ? 'on' : 'out', totals: Object.assign({}, p.totals, { parts: p.totals.parts * days * 0.8 | 0, scans: p.totals.scans * days * .8 | 0, orders: p.totals.orders * days * .7 | 0, activeMin: p.totals.activeMin * days * .8 | 0, idleMin: p.totals.idleMin * days }), orders: p.orders }));
    const perStation = {};
    for (const p of crew()) for (const [name, s] of Object.entries(p.stations)) perStation[name] = sum(perStation[name] || new Array(24).fill(0), s.h);
    if (end === today() && st.bumps) { const h = +NY_HOUR.format(new Date(now())); perStation.welding[h] += st.bumps * 3; }
    const scale = days > 1 ? days * 0.8 : 1;
    const stations = Object.keys(perStation).map(name => ({ station: name, parts: Math.round(total(perStation[name]) * scale), scans: Math.round(60 * scale), orders: Math.round((10 + name.length * 3) * scale), peopleNow: end === today() ? people.filter(p => !p.out && p.now.includes(name)).map(p => p.name) : [] }));
    const partsAll = ppl.reduce((n, p) => n + p.totals.parts, 0);
    const trend = []; for (let i = 13; i >= 0; i--) { const d = addDays(end, -i); trend.push({ day: d, parts: 520 + ((i * 97) % 260) + (i === 0 ? partsAll - 520 - 0 : 0) * (days === 1 ? 1 : 0), orders: 70 + ((i * 31) % 40), people: 3 + (i % 2), source: 'events' }); }
    if (days > 1) for (let i = days - 1; i >= 14; i--) trend.unshift({ day: addDays(end, -i), parts: 480 + ((i * 53) % 300), orders: 66 + ((i * 17) % 40), people: 3 + (i % 3), source: 'events' });
    const feed = [];
    for (let k = 0; k < 40 + st.bumps; k++) { const p = people[k % 4], s = Object.keys(p.stations)[k % Object.keys(p.stations).length], a = ['complete', 'scan', 'scan', 'print', 'complete', 'reject'][k % 6]; feed.push({ id: 'ev' + (1000 - k + st.bumps), at: now() - (k * 95 + 20) * 1000, person: p.name, station: s, action: a, orderId: fakeOrder(k % 4, k % 30), parts: a === 'complete' ? 3 + (k % 8) : 0 }); }
    const f2 = b.after && /^c\d+$/.test(b.after) ? feed.slice(0, Math.max(0, st.bumps - +b.after.slice(1))) : feed;
    return { ok: true, now: now(), day: end, days, cursor: 'c' + st.bumps, delta: !!(b.after && /^c\d+$/.test(b.after)), people: ppl,
      business: { totals: { parts: partsAll, scans: ppl.reduce((n, p) => n + p.totals.scans, 0), orders: Math.round(ppl.reduce((n, p) => n + p.totals.orders, 0) * 0.8), people: 4 }, perHour: Object.fromEntries(Object.entries(perStation).map(([k, v]) => [k, days > 1 ? v.map(x => x * days) : v])), stations, trend },
      feed: f2.slice(0, 40), sources: { events: true, seals: false, sessions: true }, notes: [] };
  }
  function person(b) {
    const p = people.find(x => x.name.toLowerCase() === String(b.name || '').toLowerCase()), n = Math.min(62, Math.max(1, +b.days || 30)), end = b.day || today(), days = [];
    for (let i = n - 1; i >= 0; i--) {
      const d = addDays(end, -i), dow = new Date(d + 'T12:00:00Z').getUTCDay(), off = dow === 0 || dow === 6 || !p;
      const parts = off ? 0 : 200 + ((i * 41 + p.name.length * 17) % 160);
      days.push({ day: d, source: off ? 'none' : 'events', parts, scanParts: off ? 0 : parts + 12, scans: off ? 0 : Math.round(parts * .6), orders: off ? 0 : Math.round(parts / 9), rejects: 0, errors: 0, activeMin: off ? 0 : 300 + (i % 7) * 12, idleMin: off ? 0 : 50 + (i % 5) * 7, signedInMin: off ? 0 : 400 + (i % 6) * 9, firstIn: off ? null : nyAt(d, 7 + (i % 2), 50 + (i % 9)), lastOut: off ? null : nyAt(d, 16, 10 + (i % 40)), stations: [], perHour: new Array(24).fill(0) });
    }
    const t = days.reduce((a, d) => ({ parts: a.parts + d.parts, orders: a.orders + d.orders, signedInMin: a.signedInMin + d.signedInMin }), { parts: 0, orders: 0, signedInMin: 0 });
    return { ok: true, now: now(), name: p ? p.name : b.name, from: days[0].day, to: end, days, totals: t, sources: { events: true }, notes: [] };
  }
  /** What the harness answers to a POST body (the passcode is the only gate). */
  function answer(body, headers = {}) {
    st.calls.push({ op: body.op, key: body.key, days: body.days, day: body.day, after: body.after, name: body.name });
    if (body.key !== st.key) { st.wrong++; return { status: 401, json: { ok: false, error: 'unauthorized' } }; }
    if (st.fail > 0) { st.fail--; return { status: 503, json: { ok: false, error: 'both reads failed' } }; }
    if (body.op === 'overview') return { status: 200, json: overview(body) };
    if (body.op === 'person') return { status: 200, json: person(body) };
    return { status: 400, json: { ok: false, error: 'bad op' } };
  }
  return { answer, state: st, setKey(k) { st.key = k; }, now, today, bump() { st.bumps++; }, KEY };
}
module.exports = { make, KEY, ymd, addDays, nyAt };
