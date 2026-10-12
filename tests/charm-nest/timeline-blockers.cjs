// Paul, 12 Oct 2026: "The only thing that should be shown in the UI of this timeline hover popup is the information about any issues or missing requirements that
// are holding back that particular order from moving forward ... the order doesn't care about what is happening with the sheet."
// The REAL header rail (OrderTimelineUI.mount, compact) is mounted in jsdom over seven fixture orders and every step of every one is hovered; what the card
// says is read back row by row. The page's context (tlContext: the order's lines and the readiness of the sheets they sit on) is the page's own shape, and each
// fixture's sheets are loaded with every sheet-level problem there is (19 engravings to approve, QR labels not printed, layout, front and back files, Cut Sheet):
// the card must come out the same with those sheets as with none, because a card speaks of the order's own pieces only.
//   NODE_PATH=<dir with jsdom> node tests/charm-nest/timeline-blockers.cjs
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const { JSDOM } = require('jsdom');
const R = path.join(__dirname, '../..') + path.sep;
const dom = new JSDOM('<main id="host"></main><p id="outside">Work area</p>', { url: 'http://127.0.0.1', runScripts: 'outside-only', pretendToBeVisual: true });
const w = dom.window, d = w.document;
let now = 0, id = 0, hit = null; const timers = new Map();
w.setTimeout = (fn, ms = 0) => { timers.set(++id, { fn, at: now + ms }); return id; }; w.clearTimeout = i => timers.delete(i); w.setInterval = () => ++id; w.clearInterval = () => {};
w.matchMedia = () => ({ matches: false }); w.Element.prototype.getAnimations = () => []; w.Element.prototype.animate = () => ({ finished: Promise.resolve(), cancel() {}, playState: 'finished' });
d.elementFromPoint = () => hit;
Object.defineProperty(w.HTMLElement.prototype, 'offsetWidth', { get() { return this.classList.contains('tlExp') ? 272 : 84; } });
Object.defineProperty(w.HTMLElement.prototype, 'offsetHeight', { get() { return this.classList.contains('tlExp') ? 160 : 84; } });
for (const f of ['charm-nest-motion.js', 'charm-nest-piece-placement.js', 'charm-nest-timeline-ui.js']) w.eval(fs.readFileSync(R + f, 'utf8'));
const T = w.OrderTimelineUI, PP = w.PiecePlacement, DELAY = w.Seal.zoom.DELAY;
async function advance(ms) { const end = now + ms; for (;;) { const nx = [...timers.entries()].filter(([, t]) => t.at <= end).sort((a, b) => a[1].at - b[1].at)[0]; if (!nx) break; now = nx[1].at; timers.delete(nx[0]); nx[1].fn(); for (let i = 0; i < 10; i++) await Promise.resolve(); } now = end; for (let i = 0; i < 10; i++) await Promise.resolve(); }
const point = (type, node, to = null) => { if (['pointerover', 'pointermove'].includes(type)) hit = node; if (type === 'pointerout') hit = to; node.dispatchEvent(new w.MouseEvent(type, { bubbles: true, relatedTarget: to, clientX: 400, clientY: 300 })); };
const rectFor = el => { const r = { left: 400, right: 484, top: 300, bottom: 384, width: 84, height: 84 }; el.getBoundingClientRect = () => r; el.getClientRects = () => [r]; };

/* ── the fixtures ───────────────────────────────────────────────────────────── */
const BASE = Date.UTC(2026, 9, 12, 11, 0);
const SHEET = 'GF Draft 3', SID = 'sh-d3';
// a sheet loaded with every problem a SHEET can have: none of it is the order's
const NOISE = [{ name: SHEET, sheetId: SID, placed: 21, stages: { layout: false, front: false, approval: false, backs: false, qr: false }, ready: false, required: 19, saved: 0, waiting: 19, cut: false, uncut: 3 }];
let n = 0;
function order(rid, specs, extra) {
  extra = extra || {};
  const pieces = [], lines = [], events = [];
  const ev = (type, dt, o) => events.push(Object.assign({ id: `${rid}~${type}~e${++n}`, orderId: rid, type, at: BASE + dt * 60000, by: 'Ana P.', source: 'sorter' }, o || {}));
  ev('arrived', 0, { by: 'Etsy', source: 'etsy' });
  const pl = {};   // each piece's placement, as the page's PiecePlacement says it
  specs.forEach((s, j) => {
    const key = `${rid}_${100 + j}`, qty = s.qty || 1, pools = Array.from({ length: qty }, (_, c) => `${key}_${c + 1}`);
    const line = { title: s.title, engrave: s.engrave || null, engraveFaces: s.faces, form: s.form || '', ...(s.engrave ? {} : { engraveCandidate: false }) };
    pieces.push({ key, tid: String(100 + j), qty, line, pools, sheets: s.on ? [SID] : [] });
    lines.push({ sku: s.sku, form: s.form || '', title: s.title, state: s.state || (s.on ? 'nested' : 'pooled'), reason: s.reason || '', wait: s.wait || null, hold: !!s.hold, hand: !!s.hand, problem: s.problem || '',
      engrave: s.engrave || null, engraveCandidate: s.engrave ? undefined : false, special: '', onSheet: !!s.on, ...(s.faces ? { engraveFaces: s.faces, engraveDone: s.done } : {}) });
    const sheets = s.on ? [{ id: SID, label: SHEET, metal: 'gold', cut: !!extra.cut, pools }] : [];
    pl[key] = PP.resolve({ key, qty, copies: qty, sheets, hold: s.hold ? (s.reason || true) : false, hand: s.hand ? { state: 'completed', how: 'print', completedBy: 'Ana P.' } : null, cancelled: extra.cancelled ? { at: BASE, by: 'Paul' } : false });
    if (s.on) ev('placed', 5 + j, { sheet: SHEET, sheetId: SID, lineKey: key, transactionId: String(100 + j), text: 'Placed on ' + SHEET, data: { poolId: pools[0] } });
    if (s.hand) ev('sealPrinted', 6 + j, { lineKey: key, transactionId: String(100 + j), text: 'QR label printed', data: { n: 1 } });
    for (const f of s.approvedFaces || []) ev('engraveApproved', 8 + j, { lineKey: key, transactionId: String(100 + j), text: f + ' engraving approved', data: { face: f } });
    if (s.engrave && s.engrave.approved) ev('engraveApproved', 9 + j, { lineKey: key, transactionId: String(100 + j), text: 'Back engraving approved' });
  });
  for (const e of extra.events || []) ev(e[0], e[1], e[2]);
  const roll = PP.roll(pieces.map(p => pl[p.key]));
  return { rid, pieces, lines, events, cancelled: extra.cancelled ? { at: BASE + 30 * 60000, by: extra.cancelled === 'etsy' ? 'Etsy' : 'Paul', source: extra.cancelled === 'etsy' ? 'etsy' : 'sorter', why: 'Buyer asked' } : null, place: k => (k ? pl[k] : roll) };
}
const eng = (text, o) => Object.assign({ needed: true, approved: false, state: 'review', text }, o || {});
const FIX = {
  // the picture (Paul, 12 Oct): a bracelet completed by hand, two GF necklaces on GF Draft 3, and the sheet loaded with 19 other engravings, QR labels not printed
  picture: order('4201047416', [{ sku: '6190', form: 'bracelet', title: 'GOLD BRACELET CHAIN 6190', hand: true }, { sku: '40171', form: 'necklace', title: 'LEAPING 40171', on: true }, { sku: '10814', form: 'necklace', title: 'POLICE 10814', on: true }]),
  // the same order after the laser cut: Sorted is where it is, and its own QR label is not printed yet
  pictureCut: order('4201047417', [{ sku: '6190', form: 'bracelet', title: 'GOLD BRACELET CHAIN 6190', hand: true }, { sku: '40171', form: 'necklace', title: 'LEAPING 40171', on: true }, { sku: '10814', form: 'necklace', title: 'POLICE 10814', on: true }], { cut: true, events: [['laserDone', 20, { sheet: SHEET, sheetId: SID, station: 'laser', by: 'Marco R.' }]] }),
  // every piece on a sheet (a necklace of quantity 2 and a plain stud pair), engravings approved
  onSheet: order('4201047420', [{ sku: '31001', form: 'necklace', title: 'HEART NECKLACE', qty: 2, on: true, engrave: eng('Mom', { approved: true, state: 'written' }) }, { sku: '22002', form: 'stud earrings', title: 'STAR stud earrings', on: true }]),
  // one piece held, the other two on the sheet
  held: order('4201047421', [{ sku: '31001', form: 'necklace', title: 'HEART NECKLACE', on: true }, { sku: '31002', form: 'necklace', title: 'MOON NECKLACE', hold: true, reason: 'wrong size' }, { sku: '31003', form: 'necklace', title: 'SUN NECKLACE', on: true }], { events: [['held', 7, { text: 'Held: wrong size', data: { reason: 'wrong size' } }]] }),
  // one piece has no charm
  unmatched: order('4201047422', [{ sku: '31001', form: 'necklace', title: 'HEART NECKLACE', on: true }, { sku: 'ZZ-9', form: 'necklace', title: 'ODD NECKLACE', state: 'unmatched', reason: 'unknown SKU' }]),
  // cancelled after it was nested: nothing is owed by any piece, whatever the sheet says; its steps stop at the laser
  cancelled: order('4201047423', [{ sku: '31001', form: 'necklace', title: 'HEART NECKLACE', state: 'gone' }, { sku: '31002', form: 'necklace', title: 'MOON NECKLACE', state: 'gone' }], { cancelled: 'etsy', events: [['placed', 8, { sheet: SHEET, sheetId: SID, lineKey: '4201047423_100', transactionId: '100', data: { poolId: '4201047423_100_1' } }], ['placed', 9, { sheet: SHEET, sheetId: SID, lineKey: '4201047423_101', transactionId: '101', data: { poolId: '4201047423_101_1' } }], ['etsyCancelled', 30, { by: 'Etsy', source: 'etsy' }]] }),
  // a pair of huggies engraved on one ear only (the other is cut plain), a necklace on the sheet, and a bracelet completed by hand that still carries an engraving nobody approved
  pair: order('4201047424', [{ sku: '50001', form: 'huggie hoop earrings', title: 'PEARL huggie hoop earrings', on: true, engrave: eng('Anna') }, { sku: '31001', form: 'necklace', title: 'HEART NECKLACE', on: true }, { sku: '6190', form: 'bracelet', title: 'GOLD BRACELET CHAIN 6190', hand: true, engrave: eng('Old words') }]),
  // front and back engraving, the front approved and the back not; a second necklace fully approved
  faces: order('4201047425', [{ sku: '31001', form: 'necklace', title: 'HEART NECKLACE', on: true, engrave: eng('W / 1. 27. 24'), faces: ['front', 'back'], done: { front: true, back: false }, approvedFaces: ['front'] }, { sku: '31002', form: 'necklace', title: 'MOON NECKLACE', on: true, engrave: eng('Sam', { approved: true, state: 'written' }) }]),
  // six pieces on no sheet: one waits for a fuller sheet, one is simply not on a sheet yet, four need a person (a person comes first, whatever order the pieces are in), and only four rows show, with how many more
  crowd: order('4201047426', [{ sku: '1', form: 'necklace', title: 'A', wait: { kind: 'fill', material: 'gold', pct: 41 } }, { sku: '2', form: 'necklace', title: 'B', state: 'unmatched' }, { sku: '3', form: 'necklace', title: 'C', state: 'noDesign' }, { sku: '4', form: 'necklace', title: 'D', state: 'oversize' }, { sku: '5', form: 'necklace', title: 'E', problem: 'two SKUs' }, { sku: '6', form: 'necklace', title: 'F' }])
};
const STEPS = ['arrived', 'sheet', 'engraved', 'laser', 'sorted', 'welded', 'assembled', 'shipped'];
// what each card says, per step: [state, rows as "kind: text"]. "clear" is the calm line of the step the order is at (nothing of its own holds it).
const WANT = {
  picture: { arrived: ['done', []], sheet: ['done', []], laser: ['now', [], true], sorted: ['later', []], assembled: ['later', []], shipped: ['later', []], engraved: ['none'], welded: ['none'] },
  pictureCut: { arrived: ['done', []], sheet: ['done', []], laser: ['done', []], sorted: ['now', ['label: Label not printed yet']], assembled: ['later', []], shipped: ['later', []], engraved: ['none'], welded: ['none'] },
  onSheet: { arrived: ['done', []], sheet: ['done', []], engraved: ['done', []], laser: ['now', [], true], sorted: ['later', []], welded: ['later', []], assembled: ['later', []], shipped: ['later', []] },
  held: {
    arrived: ['done', []],
    sheet: ['now', ['person: SKU 31002 (necklace) is held: wrong size. Release it in Review']], laser: ['later', ['person: SKU 31002 (necklace) is held: wrong size. Release it in Review']],
    sorted: ['later', ['person: SKU 31002 (necklace) is held: wrong size. Release it in Review']], assembled: ['later', ['person: SKU 31002 (necklace) is held: wrong size. Release it in Review']],
    shipped: ['later', ['person: SKU 31002 (necklace) is held: wrong size. Release it in Review']], engraved: ['none'], welded: ['none']
  },
  unmatched: { arrived: ['done', []], sheet: ['now', ['person: SKU ZZ-9 (necklace) has no charm (unknown SKU); pick it in Review']], laser: ['later', []], sorted: ['later', []], assembled: ['later', []], shipped: ['later', []], engraved: ['none'], welded: ['none'] },
  cancelled: {
    arrived: ['done', []], sheet: ['done', []], laser: ['stopped', ['stop: Cancelled on Etsy: this step will not happen']], sorted: ['gone', ['stop: Cancelled on Etsy: this step will not happen']],
    assembled: ['gone', ['stop: Cancelled on Etsy: this step will not happen']], shipped: ['gone', ['stop: Cancelled on Etsy: this step will not happen']], engraved: ['none'], welded: ['none']
  },
  pair: {
    arrived: ['done', []], sheet: ['done', []], engraved: ['now', ['person: Approve the back engraving “Anna” of SKU 50001 (huggie hoop earrings) in Review']], laser: ['later', []], sorted: ['later', []],
    assembled: ['later', []], shipped: ['later', []], welded: ['none']
  },
  crowd: {
    arrived: ['done', []], sheet: ['now', ['person: SKU 2 (necklace) has no charm; pick it in Review', 'person: SKU 3 (necklace) has no design; finish it under Custom Orders', 'person: SKU 4 (necklace) is too big for the plate; resize it in Review', 'person: SKU 5 (necklace): two SKUs; decide it in Review'], false, '2 more'],
    laser: ['later', []], sorted: ['later', []], assembled: ['later', []], shipped: ['later', []], engraved: ['none'], welded: ['none']
  },
  faces: {
    arrived: ['done', []], sheet: ['done', []], engraved: ['now', ['person: Approve the back engraving “W / 1. 27. 24” of SKU 31001 (necklace) in Review']], laser: ['later', []], sorted: ['later', []],
    assembled: ['later', []], shipped: ['later', []], welded: ['none']
  }
};
// the same orders with the sheets' problems taken away must read the same (a card never reads the sheet's readiness)
const seen = (name, sheets) => async () => {
  const F = FIX[name], host = d.createElement('div'); host.className = 'tlHost'; d.getElementById('host').appendChild(host);
  w.OrderTimeline = { get: async () => ({ events: F.events, cancelled: F.cancelled, where: null }) };
  const ctx = { lines: F.lines, sheets };
  const tl = T.mount(host, { orderId: F.rid, live: false, compact: true, pieces: F.pieces, stages: () => T.stagesFor(F.pieces.map(p => p.line)), placement: k => F.place(k), context: () => ctx });
  await advance(0); await advance(0);
  const out = {};
  for (const k of STEPS) {
    const stop = host.querySelector(`.tlStop[data-stage="${k}"]`); if (!stop) { out[k] = ['absent']; continue; }
    const seal = stop.querySelector('.tlSeal'); rectFor(seal);
    point('pointerover', seal); await advance(DELAY + 50);
    const exp = host.querySelector('.tlExp.on') || d.querySelector('.tlExp.on'), shown = exp && w.getComputedStyle(exp).display !== 'none';
    out[k] = shown ? { title: (exp.querySelector('.xh b') || {}).textContent, chip: (exp.querySelector('.xh .xs') || {}).textContent || '', rows: [...exp.querySelectorAll('.tlReq .rq:not(.ok):not(.clear):not(.more)')].map(li => li.className.replace(/\brq\b/, '').trim() + ': ' + li.textContent.replace(/^(Waiting|Needs a person|Stopped)/, '')),
      clear: !!exp.querySelector('.tlReq .rq.clear'), more: (exp.querySelector('.tlReq .rq.more') || {}).textContent || '', text: exp.textContent } : null;
    point('pointerout', seal, d.getElementById('outside')); await advance(400);
  }
  tl.destroy(); host.remove();
  return out;
};
(async () => {
  let checks = 0;
  if (process.env.DUMP) { const o = await seen(process.env.DUMP, NOISE)(); for (const k of STEPS) console.log(k, JSON.stringify(o[k] && o[k].rows ? { chip: o[k].chip, rows: o[k].rows, clear: o[k].clear, text: o[k].text } : o[k])); process.exit(0); }
  for (const name of Object.keys(FIX)) {
    const loud = await seen(name, NOISE)(), quiet = await seen(name, [])();
    for (const k of STEPS) {
      const want = WANT[name][k], a = loud[k], b = quiet[k];
      if (!want) { assert(!a || a[0] === 'absent', `${name}/${k}: the rail has a step the fixture does not expect`); continue; }
      if (want[0] === 'none') { assert(!a || a[0] === 'absent' || /^No |Not/.test(a.text || ''), `${name}/${k}: a step this order does not take says "${a && a.text}"`); checks++; continue; }
      assert(a && a.rows, `${name}/${k}: no card opened`);
      assert.deepEqual(a.rows, want[1], `${name}/${k}: the card's rows`);
      assert.equal(a.more.replace(/\s+/g, ' ').trim(), want[3] || '', `${name}/${k}: the "N more" row`);
      assert.equal(!!a.clear, !!want[2], `${name}/${k}: the calm line "No issues or missing requirements"`);
      assert.deepEqual([a.rows, a.clear], [b.rows, b.clear], `${name}/${k}: the sheets' problems change the card`);
      // (nothing about a sheet, whatever else is said)
      assert(!/QR labels|engravings? still to approve|layout|front cut file|back files|Cut Sheet|is ready|commit/i.test(a.text), `${name}/${k}: the card speaks of the sheet: ${a.text}`);
      assert(a.rows.length <= 4, `${name}/${k}: more than four rows`);
      checks++;
    }
  }
  // the picture, in words: the Laser cut card for order 4201047416 says its title and that nothing holds it back, and none of the old sheet lines
  const pic = (await seen('picture', NOISE)()).laser;
  assert.equal(pic.title, 'Laser cut'); assert.match(pic.text, /No issues or missing requirements/); assert(!/GF Draft 3|19 engravings|Print the QR/.test(pic.text), pic.text);
  console.log(`PASS: ${checks} card checks over ${Object.keys(FIX).length} orders, each with and without a sheet full of problems: only the order's own pieces speak`);
})().catch(e => { console.error(e); process.exit(1); });
