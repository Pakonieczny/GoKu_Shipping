// A fake `employeeEfficiency` (contract.md, "Console API") for the Employee efficiency console's tests and screenshots.
// FAKE DATA ONLY: four invented people, invented order numbers, a fake passcode. No network, no Firestore.
//   const F = require('./efficiency-fixture.cjs'); const fx = F.make(); fx.answer(body) → { status, json }   fx.bump() adds work.
// TWO STORES, like the real function: a request with `sandbox:true` reads the sorter's Sandbox copies (one person, Paul, at the
// Sorter: 29 parts, 25 orders, as on Paul's screenshot of 5 Oct); every other request reads the real stations (the crew below).
// op `live` (plans/employee-hr/api.md): who is signed in and the order each station has now; fx.setLiveMode('old') answers it
// "unknown op" like a server that does not have it yet.
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
const svg = (fill, label) => 'data:image/svg+xml;base64,' + Buffer.from(`<svg xmlns="http://www.w3.org/2000/svg" width="64" height="64"><rect width="64" height="64" fill="${fill}"/><circle cx="32" cy="32" r="14" fill="#fffefb" opacity=".8"/><text x="32" y="37" font-size="13" text-anchor="middle" fill="#5b554c" font-family="sans-serif">${label || ''}</text></svg>`).toString('base64');

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
  /** The early states: `empty` (nobody yet, events not started) and `sessions` (sign-ins only, no activity logged). */
  function early(b) {
    const full = overview(b), note = 'Activity events have not been recorded for these days yet: showing sign-in time and order seals only.';
    const zero = { parts: 0, scanParts: 0, scans: 0, orders: 0, rejects: 0, errors: 0, activeMin: 0, idleMin: 0, rate: 0, secPerScan: 0 };
    const ppl = st.mode === 'empty' ? [] : full.people.map(p => Object.assign({}, p, { source: 'sessions', stations: p.stations.map(x => ({ station: x.station, minutes: x.minutes, parts: 0, scanParts: 0, scans: 0, completes: 0, prints: 0, orders: 0 })), totals: Object.assign({}, zero, { signedInMin: p.totals.signedInMin }), perHour: new Array(24).fill(0), orders: [] }));
    return Object.assign({}, full, { people: ppl, business: { totals: { parts: 0, scans: 0, orders: 0, people: ppl.length }, perHour: {}, stations: ['sorting', 'welding', 'assembly', 'shipping'].map(n => ({ station: n, parts: 0, scans: 0, orders: 0, peopleNow: ppl.filter(p => p.status === 'on' && p.nowAt.includes(n)).map(p => p.name) })), trend: full.business.trend.map(d => Object.assign({}, d, { parts: 0, orders: 0 })) }, feed: [], sources: { events: false, seals: false, sessions: true }, notes: [note], partial: true });
  }
  /** History from before the events began (source "seals"): orders and scans, no parts; hourly lines count order steps. */
  function sealed(b) {
    const full = overview(b), note = 'Activity events have not been recorded for these days yet: showing sign-in time and order seals only.';
    const ppl = full.people.map((p, i) => Object.assign({}, p, { source: 'seals', stations: p.stations.map(x => ({ station: x.station, minutes: x.minutes, parts: 0, scanParts: 0, scans: x.scans, completes: 0, prints: 0, orders: x.orders })), totals: Object.assign({}, p.totals, { parts: 0, scanParts: 0, rejects: 0, errors: 0, activeMin: 0, idleMin: 0, rate: 0, secPerScan: 0 }), orders: [] }));
    const perHour = {}; for (const p of ppl) for (const s of p.stations) perHour[s.station] = sum(perHour[s.station] || new Array(24).fill(0), arr({ 9: 6, 10: 9, 11: 7, 13: 8, 14: 10 }));
    return Object.assign({}, full, { people: ppl, business: { totals: { parts: 0, scans: ppl.reduce((n, p) => n + p.totals.scans, 0), orders: 90, people: ppl.length }, perHour, stations: Object.keys(perHour).map(n => ({ station: n, parts: 0, scans: 40, orders: 20, peopleNow: [] })), trend: full.business.trend.map(d => Object.assign({}, d, { parts: 0, source: 'seals' })) }, feed: [], sources: { events: false, seals: true, sessions: true }, notes: [note], partial: true });
  }
  /** One order across the stations: who, where, when, the work and the waiting (op orders). */
  function order(b) {
    const id = String(b.orderId || '').replace(/\D/g, ''); if (!id) return { status: 400, json: { ok: false, error: 'orderId required' } };
    const base = now() - 6 * 3600000;
    if (st.orderMode === 'none' || id === '999999999') return { status: 200, json: { ok: true, now: now(), orderId: id, steps: [], events: [], totals: { firstAt: null, lastAt: null, spanMs: 0, workMs: 0, people: 0, stations: 0 }, sources: { events: false, seals: false }, notes: [] } };
    const mins = x => x * 60000, S = [
      { station: 'sorting', person: 'Giovanna', a: 0, b: 6, work: 4, scans: 3, completes: 1, prints: 0, parts: 4, source: 'events' },
      { station: 'welding', person: 'Giovanna', a: 48, b: 71, work: 17, scans: 2, completes: 1, prints: 0, parts: 4, source: 'events' },
      { station: 'assembly', person: 'Anna', a: 130, b: 152, work: 15, scans: 2, completes: 1, prints: 1, parts: 4, source: 'events' },
      { station: 'shipping', person: 'Michael', a: 301, b: 306, work: 4, scans: 2, completes: 1, prints: 2, parts: 4, source: st.orderMode === 'seals' ? 'seals' : 'events' }];
    let far = 0; const steps = S.map((x, i) => { const first = base + mins(x.a), last = base + mins(x.b), wait = i ? Math.max(0, first - far) : 0; far = Math.max(far, last); return { station: x.station, person: x.person, firstAt: first, lastAt: last, workMs: x.source === 'seals' ? 0 : mins(x.work), waitMs: wait, scans: x.scans, completes: x.completes, prints: x.prints, parts: x.source === 'seals' ? 0 : x.parts, source: x.source }; });
    const notes = st.orderMode === 'seals' ? ["Steps marked seals come from the order's timeline seals (before activity events), with no part counts or work time."] : [];
    return { status: 200, json: { ok: true, now: now(), orderId: id, steps, events: steps.map(x => ({ at: x.firstAt, person: x.person, station: x.station, device: x.station + '-1', action: 'complete', parts: x.parts, detail: '', source: x.source })), totals: { firstAt: steps[0].firstAt, lastAt: steps[3].lastAt, spanMs: steps[3].lastAt - steps[0].firstAt, workMs: steps.reduce((n, x) => n + x.workMs, 0), people: 3, stations: 4 }, sources: { events: true, seals: st.orderMode === 'seals' }, notes } };
  }
  /** The sorter's Sandbox store, as on Paul's screenshot: Paul alone, at the Sorter, 29 parts and 25 orders. Nothing at the real stations. */
  function sandboxOverview(b) {
    const days = [1, 7, 30].includes(+b.days) ? +b.days : 1, end = b.day && b.day <= today() ? b.day : today();
    const hours = arr({ 0: 1, 8: 19, 9: 3, 10: 2, 11: 2, 12: 2 }), trend = [];
    for (let i = 13; i >= 0; i--) trend.push({ day: addDays(end, -i), parts: i === 0 ? 29 : 0, orders: i === 0 ? 25 : 0, people: i === 0 ? 1 : 0, source: i === 0 ? 'events' : 'none' });
    const paul = { name: 'Paul', status: end === today() ? 'on' : 'out', firstIn: nyAt(end, 0, 40), lastOut: null, onSince: end === today() ? nyAt(end, 0, 40) : null, inDay: end, nowAt: end === today() ? ['sorter'] : [], source: 'events',
      stations: [{ station: 'sorter', minutes: 726, parts: 29, scanParts: 0, scans: 0, completes: 25, prints: 0, orders: 25 }],
      totals: { parts: 29, scanParts: 0, scans: 0, orders: 25, rejects: 0, errors: 0, activeMin: 5, idleMin: 700, signedInMin: 726, rate: 328, secPerScan: 0 }, perHour: hours, orders: [] };
    const zero = n => ({ station: n, parts: 0, scans: 0, orders: 0, peopleNow: [] });
    return { ok: true, now: now(), day: end, days, cursor: 'c0', delta: false, people: [paul],
      business: { totals: { parts: 29, scans: 0, orders: 25, people: 1 }, perHour: { sorter: hours }, stations: ['sorting', 'welding', 'assembly', 'shipping'].map(zero).concat([{ station: 'sorter', parts: 29, scans: 0, orders: 25, peopleNow: end === today() ? ['Paul'] : [] }]), trend: b.trend === false ? trend.slice(-1) : trend },
      feed: [{ id: 'sb1', at: now() - 60000, person: 'Paul', station: 'sorter', action: 'complete', orderId: '3521009001', parts: 1 }], sources: { events: true, seals: false, sessions: true }, notes: [] };
  }
  /** op live: who is signed in right now and the order each station has now. */
  function liveAnswer(sb) {
    const t = now(), d = today(), thumb = (c, l) => svg(c, l);
    if (sb) return { ok: true, at: t, mode: 'sandbox', stations: [
      { key: 'sorter', label: 'Sorter', state: 'working', people: ['Paul'], current: [{ person: 'Paul', rid: '3521009001', orderNumber: '3521009001', customer: 'Sandbox Buyer', scannedAt: t - 95000, thumbUrl: thumb('#d9cfb8', 'S'), qr: { text: '3521009001' }, pieces: [{ id: 's1', label: 'Piece 1', thumbUrl: thumb('#c8bb9c', '1') }] }], lastEventAt: t - 95000, counts: { partsToday: 29, ordersToday: 25 } }],
      signedIn: [{ name: 'Paul', stationKey: 'sorter', since: nyAt(d, 0, 40), lastSeenAt: t - 40000 }] };
    const cur = (person, rid, ago, n, c) => ({ person, rid, orderNumber: rid, customer: 'Buyer ' + rid.slice(-3), scannedAt: t - ago, thumbUrl: thumb(c, 'O'), qr: { text: rid }, pieces: Array.from({ length: n }, (_, i) => ({ id: rid + '-' + i, label: 'Piece ' + (i + 1), thumbUrl: thumb(c, String(i + 1)) })) });
    const bump = st.bumps * 3;
    return { ok: true, at: t, mode: 'real', stations: [
      { key: 'welding', label: 'Welding', state: 'working', people: ['Giovanna'], current: [cur('Giovanna', '3521000101', 252000, 3, '#d8c7a0')], lastEventAt: t - 30000 - st.bumps, counts: { partsToday: 189 + bump, ordersToday: 24 } },
      { key: 'assembly', label: 'Assembly', state: 'working', people: ['Anna'], current: [cur('Anna', '3521000138', 61000, 2, '#cdd6c0')], lastEventAt: t - 50000, counts: { partsToday: 147, ordersToday: 29 } },
      { key: 'shipping', label: 'Shipping', state: 'working', people: ['Michael'], current: [cur('Michael', '3521000175', 13000, 1, '#d6c3bd')], lastEventAt: t - 20000, counts: { partsToday: 111, ordersToday: 33 } },
      { key: 'sorting', label: 'Sorting', state: 'idle', people: [], current: [], lastEventAt: t - 5400000, counts: { partsToday: 36, ordersToday: 18 } },
      { key: 'design', label: 'Design', state: 'offline', people: [], current: [], lastEventAt: t - 5 * 3600000, counts: { partsToday: 42, ordersToday: 12 } }],
      signedIn: [{ name: 'Giovanna', stationKey: 'welding', since: nyAt(d, 7, 52), lastSeenAt: t - 20000 }, { name: 'Anna', stationKey: 'assembly', since: nyAt(d, 8, 3), lastSeenAt: t - 90000 }, { name: 'Michael', stationKey: 'shipping', since: nyAt(d, 13, 10), lastSeenAt: t - 4000 }] };
  }
  /** What the harness answers to a POST body (the passcode is the only gate). */
  function answer(body, headers = {}) {
    st.calls.push({ op: body.op, key: body.key, days: body.days, day: body.day, after: body.after, name: body.name, orderId: body.orderId, sandbox: body.sandbox === true, trend: body.trend });
    if (st.http && (!st.httpOps || st.httpOps.includes(body.op))) { const h = st.http; return { status: h.status, json: h.json }; }
    if (body.key !== st.key) { st.wrong++; return { status: 401, json: { ok: false, error: 'unauthorized' } }; }
    if (st.fail > 0 && (!st.failOps || st.failOps.includes(body.op))) { st.fail--; return { status: 503, json: { ok: false, error: 'both reads failed' } }; }
    if (body.op === 'live') { if (st.liveMode === 'old') return { status: 400, json: { ok: false, error: 'unknown op' } }; const j = liveAnswer(body.sandbox === true); return { status: 200, json: st.liveHook ? st.liveHook(j, body) : j }; }
    if (body.sandbox === true) {
      if (body.op === 'overview') return { status: 200, json: sandboxOverview(body) };
      if (body.op === 'person') return { status: 200, json: { ok: true, now: now(), name: body.name, from: addDays(today(), -6), to: today(), days: [], totals: { parts: 0, orders: 0, signedInMin: 0 }, sources: { events: false }, notes: [] } };
      if (body.op === 'orders') return { status: 200, json: { ok: true, now: now(), orderId: String(body.orderId || ''), steps: [], events: [], totals: { firstAt: null, lastAt: null, spanMs: 0, workMs: 0, people: 0, stations: 0 }, sources: { events: false, seals: false }, notes: [] } };
    }
    if (body.op === 'orders') return order(body);
    if (body.op === 'overview') { const j = st.mode === 'seals' ? sealed(body) : st.mode ? early(body) : overview(body); return { status: 200, json: st.hook ? st.hook(j, body) : j }; }
    if (body.op === 'person') return { status: 200, json: person(body) };
    return { status: 400, json: { ok: false, error: 'bad op' } };
  }
  return { answer, state: st, setKey(k) { st.key = k; }, setMode(m) { st.mode = m || ''; }, setHook(f) { st.hook = f || null; }, setHttp(status, json, ops) { st.http = status ? { status, json: json || { ok: false, error: 'x' } } : null; st.httpOps = status && ops ? ops : null; }, setLiveMode(m) { st.liveMode = m || ''; }, setLiveHook(f) { st.liveHook = f || null; }, failOnly(ops) { st.failOps = ops || null; }, setOrderMode(m) { st.orderMode = m || ''; }, now, today, bump() { st.bumps++; }, KEY };
}
module.exports = { make, KEY, ymd, addDays, nyAt };
