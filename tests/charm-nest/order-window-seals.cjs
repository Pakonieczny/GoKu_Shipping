// NOWINSEAL (Paul, 19 Oct 2026, of the strip this test was written for: "Remove this UI from the Detailed Order view. I did not ask for this."): the "ENGRAVING APPROVED · All 2 pieces approved" panel of seals is GONE from the order window.
// This test now proves it stays gone in every scenario below (back, front + back, monogram, half approved, a record without a job, discs, mixed), and that the approval seals are still where they always were in the window (the Engraving
// section's own cards). #owSeals is the order's Release Hold seal alone now (release-seal-window.cjs proves that part).
// (the original header, kept for what the scenarios are:)
// WINSEAL (Paul, 19 Oct 2026): "All orders that are approved for engraving, or monograms by the user must have a clear seal that is easily visible to the user whether they are inside the Detail Order view or looking at a list
// view in the Orders tab." This is the DETAIL ORDER VIEW half (the list cards are another change). The order window now has an approval strip, `#owSeals`, across the top of the Overview (the first cell of its grid, above the pictures and the head card): one REAL seal for every
// approved piece / face (CNEngravingSeals for an engraving, CNMonogramSeals for a monogram), labelled BACK ENGRAVING / FRONT ENGRAVING / MONOGRAM with who and when, and "N of M pieces approved" when only some pieces are. Before this the
// seals were only (a) on the corner of each Engraving card, for the piece(s) the section shows, and (b) in the hover card of the "Engraving" / "Monogram" chip and in the collapsed Details list.
//   what is proved (the REAL page over the offline fake site, bridge-server.cjs; Engrave jobs are made by Engrave.ensureJobs and decided here, as the other order-window tests do; no Firestore, no timer, no read of its own)
//     1  node: the strip's source reads no network, sets no timer, adds no listener (the HARD CONSTRAINT: this project is about cutting Firebase usage); it is painted from the window's own paint/paintNow
//     2  an approved BACK engraving: one BACK ENGRAVING seal in the strip, labelled, with who and when, visible without hovering, not clipped; the order's other chrome is untouched
//     3  FRONT + BACK approved: two seals (FRONT ENGRAVING, BACK ENGRAVING), one piece, "Engraving approved" with no count (all approved)
//     4  an approved MONOGRAM: the MONOGRAM seal and NEVER a BACK ENGRAVING seal (CNEngravingSeals invents a legacy one for any approved object with no seals)
//     5  HALF approved: "1 of 3 pieces approved" (a plain piece is no piece to approve); the approved piece's seal only; the strip says what is still to approve
//     6  nothing approved / nothing to approve / cut plain ("No engraving" is not an approval): no strip at all and nothing else moves
//     7  LIVE: an approval made while the window is open (in memory, and one that arrives from the order's timeline) shows its seal at the window's next paint; taking it away takes the seal away
//     8  an order with no Engrave job on this computer (committed to the laser; approved elsewhere): the seal its record, or its timeline, holds; counted discs, one piece each, "2 of 3"; an earring pair (never engraved) has no strip
//     9  mixed order (an approved monogram, an engraved piece to approve): "1 of 2 pieces approved" with the monogram's seal only
//    10  layout: the strip is inside the window, above the head card, at 1440 / 1024 / 390 px, no sideways scroll; the seals are real `.seal` elements of the right family
//   AUDIT=1 prints what each scenario shows today at rest and in hover cards (the audit of the request); SHOTS=<dir> keeps screenshots.
//   node tests/charm-nest/order-window-seals.cjs [playwright-core dir]      (PW_DIR=<playwright node_modules> CHROMIUM=<chrome>)
// What a pass does NOT prove: a live order on the live page (live data, live fonts, live drawings): only that the page's own code puts the strip in the window and keeps it right on the offline fake.
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const read = f => fs.readFileSync(path.join(root, f), 'utf8');
const SHOTS = process.env.SHOTS || '', AUDIT = !!process.env.AUDIT;
const fails = []; let nCheck = 0;
const check = (ok, msg) => { nCheck++; if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
const BAD_LEGACY = /legacy:/;

// ═══ 1 · the wiring, in the page's own files (no browser) ═══
{
  const bridge = read('charm-nest-bridge.js'), html = read('charm-nest-1.html'), oe = read('charm-nest-order-engraving.js');
  const fn = (src, start, end) => { const i = src.indexOf(start), j = src.indexOf(end, i + start.length); return i >= 0 && j > i ? src.slice(i, j) : ''; };
  const code = s => s.replace(/\/\*[\s\S]*?\*\//g, '').replace(/(^|[^:])\/\/[^\n]*/g, '$1');
  const paint = fn(bridge, 'function paintSeals(', '/** Paint the window from the row it is showing. */'), approved = fn(oe, 'function approvals() {', '    const id0 = ++nextId;');
  check(paint.length > 100 && !/approvals\(|owApItem|owApHd|data-ow-seal\b|ENGRAVING APPROVED|pieces approved/i.test(code(paint)), 'source: paintSeals draws no approvals panel any more (no approvals(), no seal items, no "N pieces approved")');
  check(approved.length > 100, 'source: OrderEngraving has approvals() (which pieces of the order are approved, a pure reading of what the page holds)');
  const both = code(paint) + code(approved);
  check(!/setInterval|setTimeout|requestAnimationFrame|addEventListener|\bfetch\(|XMLHttpRequest|\bapi\(|ensureSeals|loadSeals|\.load\(|\.refresh\(\)|Firestore|firebase/i.test(both), 'source: no timer, no listener, no network read, no ensureSeals / loadSeals in the strip\'s code (nothing new is read or polled)');
  check(/paintSeals\(r\)/.test(fn(bridge, 'function paintNow(r) {', 'function splitLine')) && /paintNow\(r\);/.test(fn(bridge, 'function paint() {', '/** WRONGCHARM')), 'source: the strip (now the Release Hold seal alone) is still painted from paintNow() (the paths an approval already takes)');
  check(/id="owSeals"/.test(html) && /\.owApproved\b/.test(html) && /byId\("owSeals"\)/.test(paint) && html.indexOf('.owApproved{') > html.indexOf('.relSealWords'), 'source: the page has #owSeals and its CSS (.owApproved), right after the other seal CSS (.relSeal)');
  check(/charm-nest-bridge\.js\?v=[^"]*-ws1/.test(html) && /charm-nest-order-engraving\.js\?v=[^"]*-ws1/.test(html), 'source: the cache tokens of the changed scripts are bumped (-ws1)');
}

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser part is skipped'); return finish(); }
  if (SHOTS) fs.mkdirSync(SHOTS, { recursive: true });
  const { start } = require('./bridge-server.cjs');
  const sleep = ms => new Promise(r => setTimeout(r, ms));

  // ═══ the world: one fake site, the real page, a dozen orders (every buyer made up) ═══
  const DAY = 86400, NOW_S = Math.floor(Date.now() / 1000), SHIP = NOW_S + 3 * DAY, OLD = NOW_S - 2 * DAY, T = Date.now();
  const MONO_T = 'Monogram Necklace Personalized Jewelry Initials Pendant';
  const V = (name, value) => ({ name, value }), GOLD = [V('Metal', '14k Gold Filled')];
  const eng = (sku, o) => Object.assign({ sku, title: sku.replace(/_/g, ' ') + ' charm', variations: GOLD }, o || {});
  // eng:  { mode: 'back' | 'both', front, back } and what its jobs are: [face, state, by, minutes ago]; `pair`: the line is a pair (jobs per ear)
  const ORD = {
    back:    { rid: '4210000011', buyer: 'Ada Back', lines: [eng('FIREBIRD_2', { eng: { mode: 'back', back: 'GOOD LUCK' }, jobs: [['back', 'approved', 'Paul', 60]] })] },
    both:    { rid: '4210000022', buyer: 'Bea Both', lines: [eng('MINI_0445', { title: 'Mini tag necklace, front and back engraved', variations: [V('Metal Choice', 'Gold Filled'), V('Charm Type', 'Tag1 (front+back)')], eng: { mode: 'both', front: 'W', back: 'ONE TWO' }, jobs: [['front', 'approved', 'Ana', 90], ['back', 'approved', 'Ben', 30]] })] },
    mono:    { rid: '4210000033', buyer: 'Cy Mono', lines: [{ sku: 'MONOGRAM_5351', listing: '997905351', title: MONO_T, variations: GOLD, pers: ['APV'], mono: { letters: 'APV', approved: [['', 'Ana', 45]] } }] },
    half:    { rid: '4210000044', buyer: 'Di Half', lines: [eng('FIREBIRD_2', { eng: { mode: 'back', back: 'ALPHA' }, jobs: [['back', 'approved', 'Paul', 50]] }), eng('OWL_CHARM', { eng: { mode: 'back', back: 'BETA' }, jobs: [['back', 'review']] }), eng('LEAF_CHARM', { title: 'Pressed leaf charm necklace' }), eng('MOON_DISC', { eng: { mode: 'back', back: 'GAMMA' }, jobs: [['back', 'review']] })] },
    mixedAll:{ rid: '4210000166', buyer: 'Ira All', lines: [{ sku: 'MONOGRAM_9317', listing: '606439317', title: 'Monogram Necklace Dainty Monogram Necklace', variations: GOLD, pers: ['OSR'], mono: { letters: 'OSR', approved: [['', 'Cat', 15]] } }, eng('OWL_CHARM', { eng: { mode: 'back', back: 'ALL DONE' }, jobs: [['back', 'approved', 'Dan', 10]] })] },
    none:    { rid: '4210000055', buyer: 'Ed None', lines: [eng('OWL_CHARM', { eng: { mode: 'back', back: 'LOVE MOM' }, jobs: [['back', 'review']] })] },
    plain:   { rid: '4210000066', buyer: 'Flo Plain', lines: [eng('LEAF_CHARM', { title: 'Pressed leaf charm necklace' })] },
    cut:     { rid: '4210000077', buyer: 'Gus Cut', lines: [eng('FIREBIRD_2', { eng: { mode: 'back', back: 'SKIP ME' }, jobs: [['back', 'skipped', 'Paul', 20]] })] },
    mixed:   { rid: '4210000088', buyer: 'Hal Mixed', lines: [{ sku: 'MONOGRAM_9317', listing: '606439317', title: 'Monogram Necklace Dainty Monogram Necklace', variations: GOLD, pers: ['OSR'], mono: { letters: 'OSR', approved: [['', 'Cat', 15]] } }, eng('OWL_CHARM', { eng: { mode: 'back', back: 'MIXED' }, jobs: [['back', 'review']] })] },
    discs:   { rid: '4210000099', buyer: 'Ivy Discs', lines: [{ sku: 'INITIAL_DISC_4571', title: 'Initial Disc Necklace Personalized', variations: [V('Metal', '14k Gold Filled'), V('Necklace options', 'ROSEGOLD - 3 Disc'), V('Fonts', '16"/ Typewriter')], pers: ['Tag 1: J, Tag 2: Q, Tag 3: R'], discs: 3, eng: { mode: 'back', back: 'J' }, jobs: [['back', 'approved', 'Dee', 25, 'D1'], ['back', 'approved', 'Dee', 24, 'D2'], ['back', 'review', '', 0, 'D3']] }] },
    ears:    { rid: '4210000100', buyer: 'Jo Ears', lines: [{ sku: 'MASKS HAPPY/SAD', listing: '1737560072', title: 'Theatre Happy Sad Drama Mask Earrings available in 14k Gold Fill', variations: [V('Metal choice', 'Gold'), V('Style', 'Stud earrings')] }] },
    rec:     { rid: '4210000133', buyer: 'Mae Record', lines: [eng('FIREBIRD_2', { eng: { mode: 'back', back: 'KEPT' }, rec: ['Rae', 5] })] },        // committed to the laser, no job on this computer, the approval is in the line's record
    recEv:   { rid: '4210000144', buyer: 'Ned Events', lines: [eng('OWL_CHARM', { eng: { mode: 'back', back: 'HEARD' }, recEv: ['Tim', 7] })] },     // committed, no job, the record says approved but kept no seal: the order's timeline holds it
    live:    { rid: '4210000111', buyer: 'Kit Live', lines: [eng('FIREBIRD_2', { eng: { mode: 'back', back: 'LIVE ONE' }, jobs: [['back', 'review']] }), eng('OWL_CHARM', { eng: { mode: 'back', back: 'LIVE TWO' }, jobs: [['back', 'review']] })] },
    liveMono:{ rid: '4210000122', buyer: 'Lou Late', lines: [{ sku: 'MONOGRAM_9317', listing: '606439317', title: 'Monogram Necklace Dainty Monogram Necklace', variations: GOLD, pers: ['JQX'], mono: { letters: 'JQX', approved: [] } }] }
  };
  const orderOf = o => ({ receiptId: o.rid, orderNumber: o.rid, createTs: OLD, updateTs: OLD + 60, shipBy: SHIP, buyer: { name: o.buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
    lines: o.lines.map((l, i) => ({ transactionId: o.rid + (i + 1), listingId: l.listing || '18' + o.rid.slice(-6) + i, sku: l.sku, title: l.title || l.sku.replace(/_/g, ' ') + ' charm', quantity: 1, expectedShipDate: SHIP, variations: l.variations || GOLD, metalKey: 'gold', metalLabel: 'GF 14/20', personalization: l.pers || [], buyerMessage: '' })) });

  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  let page = null;
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.route(/\/\.netlify\/functions\/etsyMailOrderLink/, r => r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ engagements: [], active: null, conversation: null, ok: true, n: 0 }) }));
    await context.addInitScript(() => {
      try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); localStorage.setItem('cn.tour.seen', '1'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {}
      window.prompt = () => 'Test Operator';
    });
    page = await context.newPage(); const errors = [], writes = [];
    page.setDefaultTimeout(25000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    page.on('request', rq => { if (rq.method() !== 'GET' && rq.method() !== 'OPTIONS' && /claude|openai|anthropic|etsy/i.test(rq.url())) writes.push(rq.url()); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.Engrave && window.CNEngravingSeals && window.CNMonogramSeals && CN.S.cloud.ok === true, null, { timeout: 60000 });

    // the orders, through the real intake; the engraved lines get the engraving the buyer asked for (spec.engraving) and their jobs (Engrave.ensureJobs), decided as the scenario says
    const made = await page.evaluate(async ({ orders, T }) => {
      await Orders.loadMaps(true);
      const out = {}, mono = {};
      const rowsOf = [];
      for (const [name, o, ord] of orders) {
        out[name] = { rid: o.rid, keys: [] };
        ord.lines.forEach((line, i) => { const key = CharmNestOrders.lineKey(ord, line); const row = { key, order: ord, line, arrivedAt: T - 3 * 3600e3, spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: [key + '_1'], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); out[name].keys.push(key); rowsOf.push([name, i, row, o.lines[i]]); });
        for (const l of ord.lines) B.master.entries.set(l.sku, { sku: l.sku, updatedAt: 1 });
      }
      Orders.interpretAll(); await Orders.loadMaps(true); Orders.interpretAll();
      const art = text => { const c = document.createElement('canvas'); c.width = 300; c.height = 300; const x = c.getContext('2d'); x.fillStyle = '#fff'; x.fillRect(0, 0, 300, 300); x.fillStyle = '#111'; x.font = '36px sans-serif'; x.textAlign = 'center'; String(text || '').split('\n').forEach((t, i) => x.fillText(t, 150, 140 + i * 44)); return c; };
      Engrave.renderBack = job => art(job.text || ''); Engrave.renderFront = job => art(job.text || ''); Engrave.render = () => {};   // (the Engrave tab itself is not under test)
      const info = {};
      for (const [name, i, row, src] of rowsOf) {
        if (src.mono) { mono[row.key] = src.mono.letters; continue; }
        if (!src.eng) continue;
        const face = t => (t ? { text: t, lines: [t] } : undefined);
        if (src.discs) { row.poolIds = Array.from({ length: src.discs }, (_, n) => row.key + '_' + (n + 1)); row.poolIds.forEach((id, n) => B.pool.rows.set(id, { poolId: id, orderId: row.order.receiptId, transactionId: row.line.transactionId, lineKey: row.key, sku: row.line.sku, material: 'gold', copy: n + 1, quantity: src.discs, state: 'written' })); }   // (a counted line: one pool piece for each disc)
        row.spec.engraveCandidate = true;
        if (src.pair) delete row.spec.engraveNever;   // (a synthetic pair that IS engraved: the rule that no earring is engraved is a different test's)
        row.spec.engraving = { v: 1, mode: src.eng.mode, front: face(src.eng.front), back: face(src.eng.back), source: 'note', textSource: 'labelled', confidence: 1, needsPerson: false };
        if (src.rec || src.recEv) {   // (a finished line the way a record keeps it: no Engrave job here)
          const [by, mins] = src.rec || src.recEv, at = T - mins * 60e3; row.state = 'committed';
          row.engrave = Object.assign({ needed: true, state: 'approved', approved: true, text: src.eng.back, approvedBy: by, approvedAt: at }, src.rec ? { seals: [{ id: 'engraveApproved:' + at + ':' + by, how: 'engraveApproved', at, by }] } : {});
          if (src.recEv) row.engrave.approvedBy = row.engrave.approvedAt = undefined;
          info[row.key] = 'no job'; continue;
        }
        const jobs = Engrave.ensureJobs(row) || [];
        info[row.key] = jobs.map(j => j.key.replace(/^\d+_\d+/, '') + ':' + j.state);
        for (const j of jobs) {
          const f = Engrave.faceOf(j), slot = j.slot || '';
          const want = (src.jobs || []).find(x => x[0] === f && (x[4] || '') === slot) || (src.jobs || []).find(x => x[0] === f && !x[4]);
          const words = (f === 'front' ? src.eng.front : src.eng.back) || '';
          Object.assign(j, { state: 'review', text: words, lines: [words], fit: { size: 10, capMm: 2, centre: [0, 0], angle: 0, weight: 'Regular' }, view: {}, verify: { geometry: { ok: true } }, source: 'note', confidence: 1, backs: [], activityAt: T });
          if (!(j.copies || []).length) j.copies = [row.key + '_' + (1 + jobs.indexOf(j))];
          if (!want) continue;
          const [, st, by, mins] = want, at = T - mins * 60e3;
          // (an approval the page's own way: the verified intent, then Engrave.recoverApprovals() settles it: the seal, the state, the line's record, the timeline event)
          if (st === 'approved') j.approvalIntent = { phase: 'verified', at, by, text: words };
          else if (st === 'skipped') { Object.assign(j, { state: 'skipped', decidedAt: at, decidedBy: by, approvedBy: null }); CNEngravingSeals.add(j, 'engravePlain', by, at); j.row.engrave = Object.assign(j.row.engrave || {}, { needed: false, state: 'skipped', approved: true, text: words }); }
        }
        row.engrave = Object.assign({ needed: true }, row.engrave || {});
      }
      Engrave.recoverApprovals();
      return { out, mono, info };
    }, { orders: Object.entries(ORD).map(([n, o]) => [n, o, orderOf(o)]), T });
    console.log('  jobs: ' + JSON.stringify(made.info));
    check(Object.keys(made.mono).length === 4, 'the intake reads the four monogram lines: ' + Object.keys(made.mono).join(' '));
    const K = (n, i = 0) => made.out[n].keys[i];
    {   // the order's timeline holds the approval of the events-only line (the record kept no seal): what another computer wrote when it approved
      const k = K('recEv'), rid = ORD.recEv.rid, at = T - 7 * 60e3;
      srv.st.put('Order_Timeline', rid + '~engraveApproved~' + k + '_1-' + at, { id: k + '_1-' + at, type: 'engraveApproved', at, by: 'Tim', orderId: rid, lineKey: k, transactionId: rid + '1', source: 'sorter', station: 'sorter', device: 'other-computer', sheetId: '', sheet: '', setId: '', milestone: false, text: 'Placement approved', data: { text: 'HEARD', poolId: k + '_1', copy: 1, sku: 'OWL_CHARM' } });
    }

    // ── the monogram widget's service: a status of a piece (approved or not) and the CORE's real seal readers, fed this order's monogramApproved events; the fake site's timeline holds the same events, so the window's own feed agrees ──
    const MA = {};   // line key -> [[slot, by, minutes ago]] (the approvals standing)
    for (const [n, o] of Object.entries(ORD)) o.lines.forEach((l, i) => { if (l.mono) MA[K(n, i)] = l.mono.approved.map(a => a.slice()); });
    await page.exposeFunction('__tlSet', (rid, evs) => {
      for (const k of [...srv.st.docs.keys()]) if (k.startsWith('Order_Timeline/' + rid + '~monogramApproved~')) srv.st.docs.delete(k);
      for (const e of evs) srv.st.put('Order_Timeline', rid + '~monogramApproved~' + e.id, Object.assign({ source: 'sorter', station: 'sorter', device: 'charm-nest-1', transactionId: '', sheetId: '', sheet: '', setId: '', text: 'Monogram ' + e.data.letters, milestone: false }, e));
    });
    await page.evaluate(({ ma, letters, T }) => {
      window.__ma = ma; window.__T = T;
      const svc = Engrave.monogramUI(), rids = new Set(), at = m => T - m * 60e3;
      svc.status = (row, ask) => {
        const r = row.parentRow || row; if (!(r.spec && r.spec.special && r.spec.special.monogram)) return null;
        const of = Math.max(1, +(r.spec.quantity || 1)), slot = typeof ask === 'string' && /^C\d+$/.test(ask) ? ask : of > 1 ? 'C1' : null, ap = ((window.__ma[r.key] || []).some(a => (a[0] || '') === (slot || '')));
        return { state: ap ? 'approved' : 'ready', label: ap ? 'Approved' : 'Ready to approve', letters: letters[r.key] || '', approved: ap, dirty: false, sent: false, rid: String(r.order.receiptId), lineKey: r.key, pieceKey: slot ? r.key + '#' + slot : r.key, slot, n: slot ? +slot.slice(1) : 1, of };
      };
      window.__syncMono = () => {
        const byRid = {}; for (const r of rids) byRid[r] = [];
        for (const [k, list] of Object.entries(window.__ma)) { const rid = k.split('_')[0]; rids.add(rid); (byRid[rid] = byRid[rid] || []); list.forEach(([slot, by, mins]) => byRid[rid].push({ id: k + (slot ? '#' + slot : '') + '-' + at(mins), type: 'monogramApproved', at: at(mins), by, lineKey: k, orderId: rid, data: { letters: letters[k], slot: slot || '', n: slot ? +slot.slice(1) : 1, of: slot ? 2 : 1 } })); }
        for (const [rid, evs] of Object.entries(byRid)) { svc.feedEvents(rid, evs); window.__tlSet(rid, evs); }
      };
      window.__syncMono();
      window.__reads = []; for (const n of ['ensureSeals', 'loadSeals']) { const f = svc[n]; if (typeof f === 'function') svc[n] = function () { window.__reads.push(n); return f.apply(this, arguments); }; }   // (the service's two ways to ask the timeline for seals: the strip must never use them)
    }, { ma: MA, letters: made.mono, T });
    check(await page.evaluate(k => CNMonogramSeals.list(Orders.rows().find(r => r.key === k), null).length, K('mono')) === 1, 'the real monogram reader gives the approved monogram line its one seal');

    // ── helpers over the window ──
    const closeWin = async () => { await page.evaluate(() => { const o = document.getElementById('orderWin'); if (o && o.open) document.getElementById('owClose').click(); }); await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 8000 }).catch(() => {}); await page.waitForTimeout(250); };
    const openOv = async (name, opts) => {
      await closeWin();
      await page.evaluate(({ k, opts }) => OrderWin.open(k, opts), { k: K(name), opts });
      await page.waitForFunction(k => OrderWin.isOpen() && OrderWin.key() === k, K(name));
      await page.waitForSelector('#owMeta [data-owf-chip="order"]', { timeout: 20000 });
      await sleep(1100);
    };
    // what the strip says (null when it is not there / hidden)
    const strip = () => page.evaluate(() => {
      const s = document.getElementById('owSeals'); if (!s) return { exists: false, shown: false, items: [], text: '', head: '', count: '', n: '', m: '', html: '', rect: { t: 0, b: 0, l: 0, w: 0, h: 0 } };
      const shown = !s.hidden && !!(s.offsetParent || s.getClientRects().length), r = s.getBoundingClientRect(), dlg = document.getElementById('orderWin').getBoundingClientRect();
      const items = [...s.querySelectorAll('[data-ow-seal]')].map(it => { const sl = it.querySelector('.seal'), q = sl ? sl.getBoundingClientRect() : null, m = sl && sl.querySelector('svg') ? (() => { try { return JSON.parse(sl.querySelector('svg').getAttribute('data-seal-model')); } catch (_) { return null; } })() : null;
        return { kind: it.dataset.kind, face: it.dataset.face || '', piece: it.dataset.piece || '', text: it.innerText.replace(/\s+/g, ' ').trim(), seals: it.querySelectorAll('.seal').length, action: m && m.action, family: m && m.family, by: m && m.by, at: m && m.at, w: q ? Math.round(q.width) : 0, inside: !!q && q.left >= dlg.left - 1 && q.right <= dlg.right + 1 && q.top >= dlg.top - 1 && q.bottom <= dlg.bottom + 1 }; });
      return { exists: true, shown, text: s.innerText.replace(/\s+/g, ' ').trim(), head: (s.querySelector('[data-ow-seals-head]') || {}).textContent || '', count: (s.querySelector('[data-ow-seals-count]') || {}).textContent || '', n: s.dataset.n, m: s.dataset.m, rect: { t: Math.round(r.top), l: Math.round(r.left), w: Math.round(r.width), h: Math.round(r.height), b: Math.round(r.bottom) }, items, html: s.innerHTML, sw: document.getElementById('orderWin').scrollWidth, cw: document.getElementById('orderWin').clientWidth };
    });
    // the audit: every seal in the window, where it is and whether a person sees it without hovering
    const inventory = () => page.evaluate(() => {
      const dlg = document.getElementById('orderWin'), dr = dlg.getBoundingClientRect();
      const where = el => el.closest('#owSeals') ? 'strip' : el.closest('#owNowCard') ? 'head card' : el.closest('#owEng .engMono') ? 'monogram card' : el.closest('#owEng') ? 'engraving card' : el.closest('.owfPop') ? 'chip card (hover)' : el.closest('#owMeta .owfList') ? 'details list' : el.closest('#owMeta') ? 'facts' : el.closest('#owRail, .owRail') ? 'rail' : el.closest('#owPcSum') ? 'pieces list' : el.closest('.owChat') ? 'chat' : 'other';
      const vis = el => { for (let n = el; n && n.nodeType === 1; n = n.parentElement) { const cs = getComputedStyle(n); if (cs.display === 'none' || cs.visibility === 'hidden' || n.hidden || +cs.opacity === 0) return false; } const r = el.getBoundingClientRect(); return r.width > 4 && r.height > 4; };
      const list = [...dlg.querySelectorAll('.seal')].map(s => { let m = null; try { m = JSON.parse(s.querySelector('svg').getAttribute('data-seal-model')); } catch (_) {} const r = s.getBoundingClientRect(); const sc = s.closest('.owMain,.owVInfo'), cr = sc ? sc.getBoundingClientRect() : dr;
        return { where: where(s), action: m && m.action, by: m && m.by, w: Math.round(r.width), visible: vis(s), inView: r.top >= Math.max(dr.top, cr.top) - 1 && r.bottom <= Math.min(dr.bottom, cr.bottom) + 1 && r.left >= dr.left - 1 && r.right <= dr.right + 1 }; });
      return list;
    });
    const hoverCards = async () => {   // what only appears in a card: the facts chips' cards
      const out = {};
      for (const id of ['engraving']) {
        const chip = await page.$(`#owMeta [data-owf-chip="${id}"]`); if (!chip) { out[id] = null; continue; }
        await chip.click(); await sleep(500);
        out[id] = await page.evaluate(() => { const p = document.querySelector('.owfPop:not([hidden])'); return p ? { title: (p.querySelector('.owfPopHd') || {}).textContent, seals: [...p.querySelectorAll('.seal')].map(s => { try { return JSON.parse(s.querySelector('svg').getAttribute('data-seal-model')).action; } catch (_) { return '?'; } }) } : null; });
        await page.keyboard.press('Escape').catch(() => {}); await sleep(200);
      }
      return out;
    };
    const audit = async (name, label) => {
      await openOv(name);
      const inv = await inventory(), st = await strip(), hov = await hoverCards();
      const seen = inv.filter(x => x.visible && x.inView && x.where !== 'chip card (hover)' && x.where !== 'details list');
      const chip = await page.evaluate(() => { const c = document.querySelector('#owMeta [data-owf-chip="engraving"]'); return c ? c.textContent.trim() : null; });
      const eng = await page.evaluate(() => { const h = document.getElementById('owEng'); return { host: h ? !h.hidden : false, cards: [...document.querySelectorAll('#owEng .engCard, #owEng .engMono')].map(c => (c.dataset.piece || c.dataset.key || '').replace(/^\d+_/, '') + ':' + (c.dataset.state || '')) }; });
      console.log(`  AUDIT ${label}: chip=${JSON.stringify(chip)} · engraving section=${JSON.stringify(eng)}`);
      console.log(`        seals visible at rest (${seen.length}): ${seen.map(x => `${x.action || '?'}@${x.where}(${x.w}px)`).join(', ') || 'none'}`);
      console.log(`        seals in hover cards only: ${JSON.stringify(hov.engraving)} · seals in the window but not in view: ${JSON.stringify(inv.filter(x => !(x.visible && x.inView)).map(x => `${x.action}@${x.where}`))}`);
      console.log(`        strip: ${st.exists ? (st.shown ? JSON.stringify(st.text) : 'present, hidden') : 'none (no #owSeals)'}`);
      if (SHOTS) await page.screenshot({ path: path.join(SHOTS, `audit-${name}.png`) });
      return { inv, st };
    };
    if (AUDIT) {
      for (const [n, label] of [['back', '(a) approved back engraving'], ['both', '(b) approved front + back'], ['mono', '(c) approved monogram'], ['half', '(d) 4 lines, 1 engraving approved, 2 to approve, 1 plain'], ['mixed', '(d2) approved monogram + engraved piece to approve'], ['discs', '(e) three discs, two approved'], ['rec', '(h) committed, no job, approval in the record'], ['recEv', '(i) committed, no job, approval only in the timeline'], ['ears', '(e2) earring pair, never engraved'], ['none', '(f) engraving to approve'], ['cut', '(g) cut plain']]) await audit(n, label);
      await closeWin(); return finish();
    }
    const around = () => page.evaluate(() => {   // what else is in the window: the head card, the pieces list, the facts chip, the Engraving cards (the strip must change none of them)
      const q = s => document.querySelector(s), vis = e => !!e && !e.hidden && !!(e.offsetParent || e.getClientRects().length);
      const sc = q('#orderWin .owMain'), sr = sc.getBoundingClientRect(), ss = q('#owSeals'), nc = q('#owNowCard'), pc = q('#owPcSum');
      return { chip: (q('#owMeta [data-owf-chip="engraving"]') || {}).textContent, cards: [...document.querySelectorAll('#owEng .engCard, #owEng .engMono')].length, now: vis(nc), nowTop: nc ? Math.round(nc.getBoundingClientRect().top) : null, pcs: vis(pc),
        stripDisplay: ss ? getComputedStyle(ss).display : 'none', stripH: ss ? Math.round(ss.getBoundingClientRect().height) : 0, firstShown: (() => { const c = [...q('#orderWin .owMain').children].find(e => vis(e)); return c ? c.id || c.className : ''; })(), scrollTop: sc.scrollTop, sTop: Math.round(sr.top), sBottom: Math.round(sr.bottom) };
    });
    const sealsIn = html => [...String(html).matchAll(/data-seal-model="([^"]*)"/g)].map(m => { try { return JSON.parse(m[1].replace(/&quot;/g, '"').replace(/&amp;/g, '&')).action; } catch (_) { return '?'; } });
    const by = (st, f) => st.items.filter(i => !f || i.face === f);

    // ═══ 2 · NOWINSEAL: no approvals panel in the window, whatever the order holds; the Engraving section's own seals are still there ═══
    for (const name of ['back', 'both', 'mono', 'half', 'mixed', 'mixedAll', 'rec', 'discs', 'cut', 'none']) {
      await openOv(name);
      const st = await strip(), inv = await inventory();
      check(!st.shown && st.items.length === 0 && !/engraving approved|monogram approved|pieces? approved/i.test(st.text), `${name}: no "Engraving approved" panel in the Detailed Order view (${JSON.stringify([st.shown, st.items.length, st.text.slice(0, 80)])})`);
      if (name === 'back' || name === 'both') check(inv.some(x => x.where === 'engraving card' && /ENGRAVING/.test(x.action || '')), `${name}: the approval seal is still on the window's Engraving card: ${JSON.stringify(inv.map(x => [x.where, x.action]))}`);
    }
    await closeWin();
    finish();
  } catch (e) { console.error(e); fails.push('crashed: ' + (e && e.message)); finish(); }
  finally { await browser.close(); await srv.close(); }
})();

function finish() {
  console.log(fails.length ? `\n${fails.length} of ${nCheck} checks FAILED:\n  - ` + fails.join('\n  - ') : `\nall ${nCheck} checks passed`);
  process.exitCode = fails.length ? 1 : 0;
}
