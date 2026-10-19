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
  check(paint.length > 100, 'source: the window has paintSeals (the approval strip\'s one painter)');
  check(approved.length > 100, 'source: OrderEngraving has approvals() (which pieces of the order are approved, a pure reading of what the page holds)');
  const both = code(paint) + code(approved);
  check(!/setInterval|setTimeout|requestAnimationFrame|addEventListener|\bfetch\(|XMLHttpRequest|\bapi\(|ensureSeals|loadSeals|\.load\(|\.refresh\(\)|Firestore|firebase/i.test(both), 'source: no timer, no listener, no network read, no ensureSeals / loadSeals in the strip\'s code (nothing new is read or polled)');
  check(/paintSeals\(r\)/.test(fn(bridge, 'function paintNow(r) {', 'function splitLine')) && /paintNow\(r\);/.test(fn(bridge, 'function paint() {', '/** WRONGCHARM')), 'source: the strip is painted from paintNow(), which paint() ends in and the timeline feed calls (the paths an approval already takes)');
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

    // ═══ 2 · an approved BACK engraving ═══
    await openOv('back'); let st = await strip(), ar = await around();
    check(st.exists && st.shown && st.items.length === 1 && st.n === '1' && st.m === '1', '2 an approved back engraving: the strip shows (n=1, m=1) with ONE item: ' + JSON.stringify([st.shown, st.items.length, st.n, st.m]));
    check(st.items[0] && st.items[0].kind === 'engrave' && st.items[0].face === 'back' && st.items[0].action === 'BACK ENGRAVING' && st.items[0].seals === 1 && st.items[0].by === 'Paul' && Math.abs(st.items[0].at - (T - 60 * 60e3)) < 2000, '2 its seal is the real BACK ENGRAVING seal, by Paul, at the approval time: ' + JSON.stringify(st.items[0] && [st.items[0].action, st.items[0].by, st.items[0].family]));
    check(/Engraving approved/i.test(st.head) && /BACK ENGRAVING/.test(st.text) && /Approved by Paul/.test(st.text) && /(Oct|Nov|Dec|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep) \d+ · \d+:\d\d (AM|PM)/.test(st.text) && st.count === '', '2 it says BACK ENGRAVING, who and when, and no count (every piece is approved): ' + JSON.stringify(st.text));
    check((st.items[0] || {}).w >= 50 && st.items[0].inside && st.rect.t >= ar.sTop - 1 && st.rect.b <= ar.sBottom + 1 && ar.firstShown === 'owSeals', '2 it is visible with no hovering and no scrolling: the first thing in the Overview (width ' + st.items[0].w + 'px), inside the window and the scroll area: ' + JSON.stringify([st.rect.t, st.rect.b, ar.sTop, ar.sBottom, ar.firstShown]));
    check(ar.chip === 'Engraving' && ar.cards === 1 && ar.now, '2 the rest of the Overview is what it was: the Engraving chip, the one Engraving card, the head card: ' + JSON.stringify(ar));
    if (SHOTS) await page.screenshot({ path: path.join(SHOTS, 'ows-back.png') });

    // ═══ 3 · FRONT + BACK ═══
    await openOv('both'); st = await strip();
    check(st.shown && st.items.length === 2 && by(st, 'front').length === 1 && by(st, 'back').length === 1 && st.items[0].face === 'front' && st.items[1].face === 'back', '3 front + back approved: two items, the front first: ' + JSON.stringify(st.items.map(i => i.face + ':' + i.action)));
    check((by(st, 'front')[0] || {}).action === 'FRONT ENGRAVING' && by(st, 'front')[0].by === 'Ana' && (by(st, 'back')[0] || {}).action === 'BACK ENGRAVING' && by(st, 'back')[0].by === 'Ben', '3 each its own seal, its own person: ' + JSON.stringify(st.items.map(i => [i.action, i.by])));
    check(st.n === '1' && st.m === '1' && st.count === '' && /Front.*Approved by Ana/i.test(st.text) && /Back.*Approved by Ben/i.test(st.text), '3 one piece, both faces approved: no count; the words say FRONT ENGRAVING by Ana and BACK ENGRAVING by Ben: ' + JSON.stringify(st.text));
    if (SHOTS) await page.screenshot({ path: path.join(SHOTS, 'ows-both.png') });

    // ═══ 4 · a MONOGRAM ═══
    await openOv('mono'); st = await strip();
    check(st.shown && st.items.length === 1 && st.items[0].kind === 'monogram' && st.items[0].action === 'MONOGRAM' && st.items[0].family === 'monogram' && st.items[0].by === 'Ana' && st.items[0].seals === 1, '4 an approved monogram: ONE item, the MONOGRAM seal (monogram family), by Ana: ' + JSON.stringify(st.items.map(i => [i.kind, i.action, i.family, i.by])));
    check(!/BACK ENGRAVING|FRONT ENGRAVING|CUT PLAIN/.test(st.text) && !sealsIn(st.html).some(a => /ENGRAVING|CUT PLAIN/.test(a)) && !BAD_LEGACY.test(st.html) && /Monogram approved/i.test(st.head) && /Approved by Ana/.test(st.text), '4 and NEVER a BACK ENGRAVING seal (no seal model, no word of the engraving, no legacy seal): ' + JSON.stringify(sealsIn(st.html)));
    const monoLeak = await page.evaluate(k => { const r = Orders.rows().find(x => x.key === k); return { engrave: r.engrave == null, job: !!Engrave.jobOf(r) }; }, K('mono'));
    check(monoLeak.engrave && !monoLeak.job, '4 opening the window made no engraving of the monogram line (no r.engrave, no Engrave job)');
    if (SHOTS) await page.screenshot({ path: path.join(SHOTS, 'ows-mono.png') });

    // ═══ 5 · HALF approved ═══
    await openOv('half'); st = await strip();
    check(st.shown && st.items.length === 1 && st.n === '1' && st.m === '3' && st.count === '1 of 3 pieces approved', '5 four lines (three engraved, one plain): "1 of 3 pieces approved" (the plain piece is no piece to approve): ' + JSON.stringify([st.count, st.n, st.m]));
    const tip = await page.evaluate(() => (document.querySelector('#owSeals [data-ow-seals-count]') || {}).title || '');
    check(/OWL CHARM/.test(tip) && /MOON DISC/.test(tip) && !/LEAF CHARM/.test(tip) && !/FIREBIRD/.test(tip), '5 and what is still to approve is its hover title: ' + JSON.stringify(tip));
    check(/FIREBIRD 2/.test((st.items[0] || {}).text) && st.items[0].action === 'BACK ENGRAVING' && await page.evaluate(() => document.querySelector('#owSeals [data-ow-seals-count]').classList.contains('part')), '5 the one seal is the approved piece\'s, named, and the count is the amber "part" one: ' + JSON.stringify(st.items[0].text));
    // one piece picked in the window (the Engraving section narrows to it): the strip is the whole order's, and still says 1 of 3
    await page.evaluate(k => OrderWin.selectPiece(k), K('half', 1)); await sleep(900);
    st = await strip(); const sec = await page.evaluate(() => [...document.querySelectorAll('#owEng .engCard')].map(c => c.dataset.piece.replace(/^\d+_/, '')));
    check(sec.length === 1 && /42100000442/.test(sec[0]) && st.shown && st.items.length === 1 && st.count === '1 of 3 pieces approved', '5 with the UNAPPROVED piece picked (its card alone in the section, no seal anywhere in it) the strip still shows the order\'s one approval and "1 of 3": ' + JSON.stringify([sec, st.count]));
    if (SHOTS) await page.screenshot({ path: path.join(SHOTS, 'ows-half-picked.png') });
    await page.evaluate(() => OrderWin.selectPiece(null)); await sleep(300);
    // counted discs: a piece each
    await openOv('discs'); st = await strip();
    check(st.shown && st.items.length === 2 && st.count === '2 of 3 pieces approved' && /Disc 1/.test(st.items[0].text) && /Disc 2/.test(st.items[1].text), '5 three discs, two approved: "2 of 3 pieces approved", each seal names its disc: ' + JSON.stringify(st.items.map(i => i.text.slice(0, 60))));
    // the mixed order: an approved monogram, an engraved piece to approve
    await openOv('mixed'); st = await strip();
    check(st.shown && st.items.length === 1 && st.items[0].kind === 'monogram' && st.count === '1 of 2 pieces approved' && /Monogram approved/i.test(st.head) && !/BACK ENGRAVING/.test(st.text), '9 a monogram approved + an engraved piece to approve: the monogram\'s seal only, "1 of 2 pieces approved": ' + JSON.stringify([st.head, st.count, st.items.map(i => i.action)]));

    // a finished order with no Engrave job on this computer: the approval is in the line's record, or only in the order's timeline (the facts chip reads both; so does the strip)
    await openOv('rec'); st = await strip();
    check(st.shown && st.items.length === 1 && st.items[0].action === 'BACK ENGRAVING' && st.items[0].by === 'Rae' && /Approved by Rae/.test(st.text) && st.count === '', '8 a committed order with NO Engrave job and its approval in the line\'s record: the strip shows the BACK ENGRAVING seal by Rae: ' + JSON.stringify(st.items.map(i => [i.action, i.by])));
    await openOv('recEv'); await page.evaluate(() => OrderWin.nudge()); await page.waitForFunction(() => { const s = document.getElementById('owSeals'); return s && !s.hidden; }, null, { timeout: 12000 }).catch(() => {}); st = await strip();
    check(st.shown && st.items.length === 1 && st.items[0].action === 'BACK ENGRAVING' && st.items[0].by === 'Tim', '8 a committed order with no job whose record kept no seal: the approval the order\'s timeline holds (by Tim) is the strip\'s seal: ' + JSON.stringify(st.items.map(i => [i.action, i.by])));
    if (SHOTS) await page.screenshot({ path: path.join(SHOTS, 'ows-record-only.png') });

    await openOv('mixedAll'); st = await strip();
    check(st.shown && st.items.length === 2 && st.items.map(i => i.kind).join() === 'monogram,engrave' && /Engraving and monogram approved/i.test(st.head) && st.count === 'All 2 pieces approved' && st.items[0].action === 'MONOGRAM' && st.items[1].action === 'BACK ENGRAVING', '9 a monogram and an engraved piece, both approved: both seals in the lines\' order, "Engraving and monogram approved", "All 2 pieces approved": ' + JSON.stringify([st.head, st.count, st.items.map(i => i.action)]));
    // ═══ 6 · nothing approved, nothing to approve, cut plain, an earring pair: no strip, and nothing else moves ═══
    for (const [n, why] of [['none', 'an engraving to approve'], ['plain', 'a plain piece'], ['cut', 'a piece cut plain ("No engraving" is no approval)'], ['ears', 'an earring pair (never engraved)']]) {
      await openOv(n); st = await strip(); ar = await around();
      check(st.exists && !st.shown && ar.stripDisplay === 'none' && ar.stripH === 0 && st.html === '' && ar.firstShown !== 'owSeals', '6 ' + why + ': no strip (hidden, no height, nothing in it): ' + JSON.stringify([st.shown, ar.stripDisplay, ar.stripH]));
    }
    const cutChip = await page.evaluate(() => 0); await openOv('cut'); ar = await around();
    check(ar.chip === 'Engraving', '6 the cut-plain order keeps its Engraving chip and its hover card with the CUT PLAIN seal (the window\'s other seals are untouched)');
    await openOv('none'); ar = await around();
    check(ar.cards === 1 && ar.chip === 'Engraving', '6 the unapproved order\'s Overview is what it was (the card to approve, the chip)');

    // ═══ 7 · LIVE: an approval made while the window is open shows its seal through the window's own repaint path ═══
    await openOv('live'); st = await strip();
    check(st.exists && !st.shown, '7 the window opens on two pieces to approve: no strip');
    // this computer approves the first piece (the page's own settle: the verified intent, Engrave.recoverApprovals): no repaint is asked for here, the section's own tick and the window's paint do it
    await page.evaluate(({ k, T }) => { const j = Engrave.jobOf(Orders.rows().find(r => r.key === k)); j.approvalIntent = { phase: 'verified', at: T, by: 'Live Lee', text: j.text }; Engrave.recoverApprovals(); }, { k: K('live', 0), T: Date.now() });
    const t0 = Date.now(); await page.waitForFunction(() => { const s = document.getElementById('owSeals'); return s && !s.hidden && s.querySelectorAll('.seal').length === 1; }, null, { timeout: 12000 }).catch(() => {});
    st = await strip();
    check(st.shown && st.items.length === 1 && st.count === '1 of 2 pieces approved' && st.items[0].by === 'Live Lee' && st.items[0].action === 'BACK ENGRAVING', '7 live (this computer): the seal and "1 of 2 pieces approved" appear by themselves ' + (Date.now() - t0) + ' ms after the approval: ' + JSON.stringify([st.count, st.items.map(i => i.by)]));
    // another computer approves the second piece: its event reaches the window through the order's timeline feed (the window's existing read, nudged once here instead of waited for)
    const ev = await page.evaluate(k => { const r = Orders.rows().find(x => x.key === k), j = Engrave.jobOf(r); return { rid: r.order.receiptId, tx: r.line.transactionId, lineKey: r.key, poolId: (j.copies || [])[0], text: j.text }; }, K('live', 1));
    const evAt = Date.now() + 500;
    srv.st.put('Order_Timeline', ev.rid + '~engraveApproved~' + ev.poolId + '-' + evAt, { id: ev.poolId + '-' + evAt, type: 'engraveApproved', at: evAt, by: 'Far Away', orderId: ev.rid, lineKey: ev.lineKey, transactionId: ev.tx, source: 'sorter', station: 'sorter', device: 'other-computer', sheetId: '', sheet: '', setId: '', milestone: false, text: 'Placement approved', data: { text: ev.text, poolId: ev.poolId, copy: 1, sku: 'OWL_CHARM' } });
    await sleep(600); await page.evaluate(() => OrderWin.nudge());
    const t1 = Date.now(); await page.waitForFunction(() => { const s = document.getElementById('owSeals'); return s && !s.hidden && s.querySelectorAll('.seal').length === 2; }, null, { timeout: 15000 }).catch(() => {});
    st = await strip();
    check(st.shown && st.items.length === 2 && st.items.some(i => i.by === 'Far Away') && /All 2 pieces approved/.test(st.count), '7 live (another computer, through the timeline feed): its seal joins and the count reads "All 2 pieces approved" (' + (Date.now() - t1) + ' ms): ' + JSON.stringify([st.count, st.items.map(i => i.by)]));
    if (SHOTS) await page.screenshot({ path: path.join(SHOTS, 'ows-live.png') });
    // a monogram approved while the window is open, then taken away again
    await openOv('liveMono'); st = await strip();
    check(st.exists && !st.shown, '7 a monogram window opens with nothing approved: no strip');
    await page.evaluate(({ k }) => { window.__ma[k] = [['', 'Cy Late', 0.1]]; window.__syncMono(); }, { k: K('liveMono') });
    await page.evaluate(() => OrderWin.nudge());
    await page.waitForFunction(() => { const s = document.getElementById('owSeals'); return s && !s.hidden && s.querySelectorAll('.seal').length === 1; }, null, { timeout: 15000 }).catch(() => {});
    st = await strip();
    check(st.shown && st.items.length === 1 && st.items[0].kind === 'monogram' && st.items[0].by === 'Cy Late' && !sealsIn(st.html).some(a => /ENGRAVING/.test(a)), '7 live monogram: its seal appears at the window\'s next paint, never an engraving seal: ' + JSON.stringify(st.items.map(i => [i.action, i.by])));
    await page.evaluate(({ k }) => { window.__ma[k] = []; window.__syncMono(); }, { k: K('liveMono') });
    await page.evaluate(() => OrderWin.nudge());
    check(await page.waitForFunction(() => { const s = document.getElementById('owSeals'); return s && s.hidden && !s.querySelector('.seal'); }, null, { timeout: 15000 }).then(() => true, () => false), '7 and the seal goes when the approval does');

    // ═══ 10 · layout at three widths ═══
    for (const [w, h] of [[1440, 900], [1024, 768], [390, 800]]) {
      await page.setViewportSize({ width: w, height: h }); await sleep(500);
      await openOv('half'); st = await strip(); ar = await around();
      const lay = await page.evaluate(() => { const d = document.getElementById('orderWin'), s = document.getElementById('owSeals'), nc = document.getElementById('owNowCard'), r = s.getBoundingClientRect(), dr = d.getBoundingClientRect(); return { within: r.left >= dr.left - 1 && r.right <= dr.right + 1, aboveNow: nc.hidden || r.bottom <= nc.getBoundingClientRect().top + 1, sw: d.scrollWidth, cw: d.clientWidth, items: [...s.querySelectorAll('.owApItem')].every(i => { const q = i.getBoundingClientRect(); return q.right <= r.right + 1 && q.width > 40; }) }; });
      check(st.shown && lay.within && lay.aboveNow && lay.sw <= lay.cw + 1 && lay.items, '10 at ' + w + ' px: the strip is inside the window, above the head card, nothing sideways: ' + JSON.stringify(lay));
      if (SHOTS) await page.screenshot({ path: path.join(SHOTS, `ows-half-${w}.png`) });
    }
    await page.setViewportSize({ width: 1440, height: 900 });
    await closeWin();
    const reads = await page.evaluate(() => window.__reads);
    check(Array.isArray(reads) && reads.length === 0, 'the monogram service was never asked to read seals (ensureSeals / loadSeals: none, over every window above): ' + JSON.stringify(reads));
    check(errors.length === 0, 'no page errors: ' + errors.join(' | ').slice(0, 300));
    check(writes.length === 0, 'no call to Etsy or an AI service');
    finish();
  } catch (e) { console.error(e); fails.push('crashed: ' + (e && e.message)); finish(); }
  finally { await browser.close(); await srv.close(); }
})();

function finish() {
  console.log(fails.length ? `\n${fails.length} of ${nCheck} checks FAILED:\n  - ` + fails.join('\n  - ') : `\nall ${nCheck} checks passed`);
  process.exitCode = fails.length ? 1 : 0;
}
