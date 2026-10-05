// An INVENTED shop for the employee page's tests (ops person / personOrders and the helpers behind them): 14 weeks of sign-ins,
// activity events, daily rollups, inbox events, stored receipts and listing pictures, for six invented people, with days off, a
// holiday, a Saturday, a late start, a short day, a new hire, a reopened completion, refusals, errors, reprints, rescans and failed
// inbox replies. Plus a small Sandbox_ set that must never leak into the real numbers. NO real names, PINs or customers.
//   const S = require('./employee-profile-seed.cjs');  const truth = S.seed((collection, id, doc) => store.put(collection, id, doc));
//   S.NOW is the fake "now" (Mon 5 Oct 2026, 15:00 New York); everything is deterministic (seeded generator), `truth` says what was generated.
'use strict';
const NOW = Date.UTC(2026, 9, 5, 19, 0);                          // Mon 5 Oct 2026, 15:00 in New York (EDT)
const TODAY = '2026-10-05', FIRST_DAY = '2026-06-29';
const DAY_MS = 86400000;
const addDays = (day, n) => { const [y, m, d] = day.split('-').map(Number); return new Date(Date.UTC(y, m - 1, d + n)).toISOString().slice(0, 10); };
const dow = day => new Date(day + 'T12:00:00Z').getUTCDay();
const nyAt = (day, h, m) => { const [y, mo, d] = day.split('-').map(Number); return Date.UTC(y, mo - 1, d, h + 4, m || 0); };   // (every day here is EDT)
const hourOf = at => String(new Date(at - 4 * 3600000).getUTCHours()).padStart(2, '0');
function rng(seed) { let a = seed >>> 0; return () => { a |= 0; a = a + 0x6D2B79F5 | 0; let t = Math.imul(a ^ a >>> 15, 1 | a); t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t; return ((t ^ t >>> 14) >>> 0) / 4294967296; }; }

// who works when. `off` = days off, `extra` = weekend days worked, `late`/`short` = {day: [hour, minute]} start / end overrides.
const CREW = [
  { name: 'Giovanna C.', main: ['welding', 'assembly'], dev: ['weld-1', 'assembly-1'], days: [1, 2, 3, 4, 5], off: ['2026-07-20', '2026-07-21', '2026-09-14'], late: { '2026-08-10': [10, 30] }, short: { '2026-08-26': [11, 0] }, inbox: 'Giovanna' },
  { name: 'Michael_V', main: ['shipping'], dev: ['shipping-1'], days: [1, 2, 3, 4, 5], off: ['2026-08-14'], extra: ['2026-09-12'] },
  { name: 'Ana_M', main: ['assembly'], dev: ['assembly-2'], days: [1, 2, 3, 4, 5], off: ['2026-09-02'], extra: ['2026-09-12'] },
  { name: 'Ivy_Y', main: ['design'], dev: ['design-message'], days: [1, 2, 3, 4] },                      // part time: never a Friday
  { name: 'Empress D.', main: ['sorting'], dev: ['sorting-1'], days: [1, 2, 3, 4, 5], since: '2026-09-14' },   // a new hire
  { name: 'Paul K', main: ['inbox'], dev: ['etsy-mail-1'], days: [1, 2, 3, 4, 5], sparse: 4, inboxOnly: true }  // the owner: the inbox now and then
];
const HOLIDAYS = ['2026-09-07'];
const CUSTOMERS = ['Jane Smith', 'Marcus Bell', 'Priya Nair', 'Tom Alvarez', 'Hana Ito', 'Lucia Romero', 'Ben Carter', 'Nora Quinn'];
const SKUS = ['CH-MOON-GF', 'CH-STAR-SS', 'ST-PEARL-GF', 'CH-HEART-RG', 'ST-BEE-SS', 'CH-LEAF-GF'];

function works(p, day, r) {
  if (day < FIRST_DAY || day > TODAY || HOLIDAYS.includes(day)) return false;
  if (p.since && day < p.since) return false;
  if ((p.off || []).includes(day)) return false;
  if ((p.extra || []).includes(day)) return true;
  if (!p.days.includes(dow(day))) return false;
  if (p.sparse) return r() < 1 / p.sparse;
  return true;
}

function seed(put) {
  const truth = { people: {}, days: {}, orders: {}, receipts: {}, issues: [], inbox: [], sandbox: {}, firstDay: FIRST_DAY, today: TODAY, now: NOW };
  let orderN = 0;
  const nextRid = () => String(3521000000 + (++orderN) * 7);
  const allDays = []; for (let d = FIRST_DAY; d <= TODAY; d = addDays(d, 1)) allDays.push(d);
  const rolls = new Map();                                                // 'day__person' → rollup under construction

  function tally(roll, ev) {
    const st = roll.stations[ev.station] || (roll.stations[ev.station] = { scans: 0, scanParts: 0, completes: 0, parts: 0, orders: 0, prints: 0, rejects: 0, errors: 0, notes: 0, undos: 0, undoParts: 0, undoOrders: 0, activeMs: 0, idleMs: 0, firstAt: ev.at, lastAt: ev.at });
    const a = ev.action;
    if (a === 'scan') { st.scans++; st.scanParts += ev.parts; } else if (a === 'complete') { st.completes++; st.parts += ev.parts; st.orders += ev.orders; } else if (a === 'print') st.prints++;
    else if (a === 'reject') st.rejects++; else if (a === 'undo') { st.undos++; st.undoParts += ev.parts; st.undoOrders += ev.orders; } else if (a === 'error') st.errors++; else st.notes++;
    if (ev.sincePrevMs <= 300000) st.activeMs += ev.sincePrevMs; else st.idleMs += ev.sincePrevMs;
    st.firstAt = Math.min(st.firstAt, ev.at); st.lastAt = Math.max(st.lastAt, ev.at);
    roll.events++; roll.firstAt = Math.min(roll.firstAt, ev.at); roll.lastAt = Math.max(roll.lastAt, ev.at);
    if (a === 'scan' || a === 'complete' || a === 'undo') {
      const h = roll.hours[ev.hour] || (roll.hours[ev.hour] = { parts: 0, scans: 0, undoParts: 0, by: {} }), b = h.by[ev.station] || (h.by[ev.station] = { parts: 0, scans: 0, undoParts: 0 });
      if (a === 'scan') { h.scans++; b.scans++; } else if (a === 'complete') { h.parts += ev.parts; b.parts += ev.parts; } else { h.undoParts += ev.parts; b.undoParts += ev.parts; }
    }
    if (ev.orderId) (roll.touched[ev.orderId] || (roll.touched[ev.orderId] = {}))[ev.station] = true;
  }
  function emit(ctx, ev0) {
    const seq = ++ctx.seq, at = ev0.at, since = Math.max(0, Math.min(3600000, at - ctx.prev));
    const ev = Object.assign({ id: `${ctx.dev}_${ctx.pc}_${seq}_${at}`, station: ctx.station, device: ctx.dev, computer: ctx.pc, session: '', person: ctx.person, action: 'note', orderId: '', line: '', sku: '', parts: 0, orders: 0, detail: '', at, seq,
      sincePrevMs: since, ts: at + 1000, serverAt: at + 1000, day: ctx.day, hour: hourOf(at), v: 1 }, ev0, { station: ev0.station || ctx.station });
    ctx.prev = at;
    ctx.put('Station_Activity', ev.id, ev);
    const key = ctx.day + '__' + ctx.person; let roll = rolls.get(key);
    if (!roll) rolls.set(key, roll = { day: ctx.day, person: ctx.person, v: 1, events: 0, firstAt: at, lastAt: at, stations: {}, hours: {}, touched: {} });
    tally(roll, ev);
    return ev;
  }

  for (const p of CREW) {
    const r = rng(p.name.length * 977 + 13);
    truth.people[p.name] = { days: [], parts: 0 };
    for (const day of allDays) {
      if (!works(p, day, r)) continue;
      const isToday = day === TODAY, late = (p.late || {})[day], short = (p.short || {})[day];
      const startAt = late ? nyAt(day, late[0], late[1]) : nyAt(day, 8, Math.floor(r() * 20)), endAt = isToday ? NOW - 60000 : nyAt(day, short ? short[0] : 16, short ? short[1] : 20 + Math.floor(r() * 25));
      const lunchA = nyAt(day, 12, 0), lunchB = nyAt(day, 12, 35);
      const sessions = [];
      const mk = (id, station, dev, s, e, live, reason) => ({ id, person: p.name, station, device: dev, computerId: 'pc-' + p.name.replace(/\W/g, '').slice(0, 4).padEnd(4, 'X') + '01', computerLabel: '', startAt: s, lastSeenAt: live ? NOW - 60000 : e, endAt: live ? null : e, endReason: live ? null : reason, minutes: 0 });
      const st1 = p.main[0], st2 = p.main[1] || p.main[0], d1 = p.dev[0], d2 = p.dev[1] || p.dev[0];
      if (isToday) {
        if (p.name === 'Ivy_Y') sessions.push(mk(`s-${day}-${p.name}-1`, st1, d1, startAt, nyAt(day, 12, 10), false, 'signOut'));      // signed out at 12:10
        else { sessions.push(mk(`s-${day}-${p.name}-1`, st1, d1, startAt, lunchA, false, 'signOut')); sessions.push(mk(`s-${day}-${p.name}-2`, st2, d2, lunchB, 0, true)); }   // back from lunch, still on
      } else if (late || short || p.inboxOnly) sessions.push(mk(`s-${day}-${p.name}-1`, st1, d1, startAt, endAt, false, 'signOut'));
      else { sessions.push(mk(`s-${day}-${p.name}-1`, st1, d1, startAt, lunchA, false, 'signOut')); sessions.push(mk(`s-${day}-${p.name}-2`, st2, d2, lunchB, endAt, false, 'closed')); }
      for (const s of sessions) put('Station_Sessions', s.id, s);
      const dayTruth = { day, person: p.name, parts: 0, touched: 0, orders: 0, ordersFin: 0, scans: 0, undos: 0, rejects: 0, errors: 0, prints: 0, signedMs: sessions.reduce((n, s) => n + ((s.endAt || NOW) - s.startAt), 0), firstIn: startAt, issues: {} };
      truth.people[p.name].days.push(day); truth.days[day + '|' + p.name] = dayTruth;
      // the day's work: orders in sequence at the person's station(s)
      const n = p.inboxOnly ? 0 : (isToday ? 9 : 14 + Math.floor(r() * 13));
      let t = startAt;
      const ctxOf = (station, dev) => ({ put, person: p.name, station, dev, pc: 'pc-' + p.name.replace(/\W/g, '').slice(0, 4).padEnd(4, 'X'), seq: 0, prev: startAt, day });
      let ctx = ctxOf(st1, d1); const ctx2 = p.main[1] ? Object.assign(ctxOf(st2, d2), { prev: lunchB }) : ctx;
      for (let i = 0; i < n; i++) {
        const rid = nextRid(), pieces = 1 + Math.floor(r() * 4), station = t >= lunchB && p.main[1] ? st2 : st1, c = station === st2 ? ctx2 : ctx;
        if (i % 5 === 0) {                                                               // a batch of five orders, then a pause: batches are spread over the shift
          const nb = Math.ceil(n / 5), span = Math.max(nb * 8 * 60000, (isToday ? NOW : endAt) - startAt - 40 * 60000);
          t = Math.max(t, startAt + 8 * 60000 + Math.floor(i / 5) * Math.floor(span / nb) + Math.floor(r() * 5 * 60000));
          if (t >= lunchA && t < lunchB) t = lunchB;
        }
        if (isToday && t > NOW - 5 * 60000) break;
        t += (40 + Math.floor(r() * 160)) * 1000;
        const how = r() < 0.5 ? 'phone scan' : 'typed';
        const flags = { rescan: false, reprint: false, undo: false, reject: false, error: false, lookup: false };
        // fixed injections, so the tests can name them: the reopened completion, a refusal, errors, a reprint, a rescan
        if (p.name === 'Giovanna C.' && day === '2026-09-30' && i === 2) flags.undo = true;
        if (p.name === 'Giovanna C.' && day === '2026-10-01' && i === 1) flags.rescan = true;
        if (p.name === 'Giovanna C.' && day === '2026-10-02' && i === 3) flags.reject = true;
        if (p.name === 'Ana_M' && day === '2026-10-01' && i === 4) flags.lookup = true;
        if (p.name === 'Ana_M' && day === '2026-10-02' && i === 2) flags.error = true;
        if (p.name === 'Michael_V' && day === '2026-10-01' && i === 3) flags.reprint = true;
        if (p.name === 'Ivy_Y' && day === '2026-10-01' && i === 1) flags.held = true;
        if (p.name === 'Michael_V' && day === '2026-09-25' && i === 0) flags.cancel = true;
        const ev = (o) => emit(c, o);
        ev({ at: t, action: 'scan', orderId: rid, parts: pieces, detail: how });
        dayTruth.scans++; dayTruth.touched++;
        if (flags.cancel) { t += 20000; ev({ at: t, action: 'reject', orderId: rid, detail: 'cancelled order' }); dayTruth.rejects++; truth.issues.push({ person: p.name, day, rid, kind: 'cancelAlert' }); continue; }
        if (flags.reject) { t += 20000; ev({ at: t, action: 'reject', orderId: rid, detail: 'no stud earrings on this order' }); dayTruth.rejects++; truth.issues.push({ person: p.name, day, rid, kind: 'refused' }); continue; }
        if (flags.held) { t += 20000; ev({ at: t, action: 'reject', orderId: rid, detail: 'held: customOrder' }); dayTruth.rejects++; truth.issues.push({ person: p.name, day, rid, kind: 'heldOrSkipped' }); continue; }
        if (flags.rescan) { t += 30000; ev({ at: t, action: 'scan', orderId: rid, parts: pieces, detail: how + ' · again' }); dayTruth.scans++; truth.issues.push({ person: p.name, day, rid, kind: 'rescan' }); }
        if (flags.lookup) { t += 5000; ev({ at: t, action: 'error', orderId: rid, detail: 'Etsy order lookup failed' }); dayTruth.errors++; truth.issues.push({ person: p.name, day, rid, kind: 'lookupFailed' }); }
        if (flags.error) { t += 5000; ev({ at: t, action: 'error', orderId: rid, detail: 'Complete Order failed' }); dayTruth.errors++; truth.issues.push({ person: p.name, day, rid, kind: 'failed' }); }
        if (p.main[0] === 'shipping') { t += 30000; ev({ at: t, action: 'print', orderId: rid, parts: pieces, detail: 'QR label' }); dayTruth.prints++; }
        if (flags.reprint) { t += 40000; ev({ at: t, action: 'print', orderId: rid, parts: pieces, detail: 'QR label, again' }); dayTruth.prints++; truth.issues.push({ person: p.name, day, rid, kind: 'reprint' }); }
        t += (30 + Math.floor(r() * 120)) * 1000;
        ev({ at: t, action: 'complete', orderId: rid, parts: pieces, orders: 1, detail: '' });
        dayTruth.parts += pieces; dayTruth.orders++; dayTruth.ordersFin++;
        truth.orders[rid] = { rid, person: p.name, day, station, pieces, at: t };
        if (flags.undo) {
          t += 90000; ev({ at: t, action: 'undo', orderId: rid, parts: pieces, orders: 1, detail: 'completion undone' }); dayTruth.undos++; dayTruth.parts -= pieces; dayTruth.ordersFin--; truth.issues.push({ person: p.name, day, rid, kind: 'undone' });
          t += 120000; ev({ at: t, action: 'complete', orderId: rid, parts: pieces, orders: 1, detail: '' }); dayTruth.parts += pieces; dayTruth.ordersFin++;
        }
      }
      truth.people[p.name].parts += dayTruth.parts;
      // the inbox: the account "Giovanna" and the owner "Paul K" (E3's exact detail phrases), the last 20 days
      const inboxName = p.inbox || (p.inboxOnly ? p.name : '');
      if (inboxName && day >= addDays(TODAY, -19) && !isToday) {
        const ic = { put, person: inboxName, station: 'inbox', dev: 'etsy-mail-1', pc: 'pc-IBOX', seq: 0, prev: nyAt(day, 9, 0), day };
        if (!p.inboxOnly) { const s = mk(`si-${day}-${inboxName}`, 'inbox', 'etsy-mail-1', nyAt(day, 9, 0), nyAt(day, 11, 30), false, 'signOut'); s.person = inboxName; put('Station_Sessions', s.id, s); }
        let it = nyAt(day, 9, 5);
        const convs = p.inboxOnly ? 2 : 4;
        for (let k = 0; k < convs; k++) {
          const rid = nextRid(), e = o => emit(ic, o), tok = o => { truth.inbox.push(Object.assign({ person: inboxName, day, rid }, o)); };
          it += 4 * 60000; e({ at: it, action: 'note', orderId: rid, detail: 'reply drafted' }); tok({ kind: 'drafted' });
          it += 2 * 60000; const edited = k % 2 === 0;
          e({ at: it, action: 'note', orderId: rid, detail: 'reply sent' + (edited ? ' · ai draft edited' : ' · ai draft') + (k === 0 ? ` · first reply ${20 + k * 15}m` : '') }); tok({ kind: 'sent', edited, firstReplyMin: k === 0 ? 20 + k * 15 : null });
          it += 60000;
          if (k === 3 && day === '2026-10-02') { e({ at: it, action: 'error', orderId: rid, detail: 'reply failed · QUEUED_EXPIRED' }); tok({ kind: 'failed' }); }
          else { e({ at: it, action: 'note', orderId: rid, detail: 'reply delivered' }); tok({ kind: 'delivered' }); }
          if (k === 1) { it += 30000; e({ at: it, action: 'complete', orderId: rid, detail: 'conversation done' }); tok({ kind: 'done' }); }
        }
      }
    }
  }
  // the rollups, written from the events (so a test can compare the two)
  for (const [key, roll] of rolls) { roll.firstAt = Math.min(roll.firstAt, roll.lastAt); put('Efficiency_Daily', key, roll); }

  // stored receipts + listing pictures for the orders of the last 21 days (a few orders have none: "info:false")
  let k = 0;
  for (const o of Object.values(truth.orders)) {
    if (o.day < addDays(TODAY, -21)) continue;
    k++; if (k % 9 === 0) continue;
    const lines = o.pieces > 2 ? 2 : 1, tx = [];
    for (let i = 0; i < lines; i++) { const lid = String(901000 + ((k + i) % 6)); tx.push({ transaction_id: Number(o.rid) * 10 + i, listing_id: Number(lid), sku: SKUS[(k + i) % SKUS.length], title: `Handmade ${SKUS[(k + i) % SKUS.length]} charm`, quantity: i === 0 ? o.pieces - (lines - 1) : 1 }); }
    const customer = CUSTOMERS[k % CUSTOMERS.length];
    put('EtsyMail_Receipts', o.rid, { receipt_id: o.rid, buyer_name: customer, created_timestamp: Math.floor(o.at / 1000) - 86400, raw: { receipt_id: Number(o.rid), name: customer, transactions: tx } });
    truth.receipts[o.rid] = { customer, skus: tx.map(x => x.sku), pieces: tx.reduce((n, x) => n + x.quantity, 0) };
  }
  for (let i = 0; i < 6; i++) if (i !== 5) put('Etsy_Listing_Image_Cache', String(901000 + i), { images: [{ rank: 1, url_570xN: `https://i.etsystatic.com/fake/il_570xN.${901000 + i}_abc.jpg` }] });

  // the sandbox: its own small shop, loud numbers (a leak into the real answers shows at once)
  for (const day of ['2026-10-01', '2026-10-02', '2026-10-05']) {
    for (const [person, station] of [['Giovanna C.', 'welding'], ['Sandy Tester', 'sorting']]) {
      const s = { id: `sb-${day}-${person}`, person, station, device: station + '-1', computerId: 'pc-SAND01', computerLabel: '', startAt: nyAt(day, 9, 0), lastSeenAt: nyAt(day, 15, 0), endAt: nyAt(day, 15, 0), endReason: 'signOut', minutes: 360, sandbox: true };
      put('Sandbox_Station_Sessions', s.id, s);
      put('Sandbox_Efficiency_Daily', `${day}__${person}`, { day, person, v: 1, sandbox: true, events: 40, firstAt: nyAt(day, 9, 10), lastAt: nyAt(day, 14, 50),
        stations: { [station]: { scans: 40, scanParts: 400, completes: 20, parts: 1000, orders: 20, prints: 0, rejects: 0, errors: 0, notes: 0, undos: 0, undoParts: 0, undoOrders: 0, activeMs: 5 * 3600000, idleMs: 600000, firstAt: nyAt(day, 9, 10), lastAt: nyAt(day, 14, 50) } },
        hours: { '10': { parts: 1000, scans: 40, undoParts: 0, by: { [station]: { parts: 1000, scans: 40, undoParts: 0 } } } }, touched: { [`999000${day.slice(8)}1${person.length}`]: { [station]: true } } });
      truth.sandbox[day + '|' + person] = { parts: 1000 };
    }
  }
  return truth;
}

module.exports = { seed, NOW, TODAY, FIRST_DAY, CREW, HOLIDAYS, nyAt, addDays, hourOf, dow };
