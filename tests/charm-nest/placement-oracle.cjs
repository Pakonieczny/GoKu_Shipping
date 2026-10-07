// PLACEMENT ORACLE (worker C2, Paul 5 Oct 2026): "there's no mismatch in orders being on sheets, not being on sheets ... everything needs to be
// coordinated as information is added changed removed altered. Everything needs to be seamlessly updated in the cloud and on all portions of the
// application." (plans/consistency/plan.md, point 3; the table of surfaces is plans/consistency/audit.md)
//
// ONE question asked of EVERY surface that says or implies "on a sheet / not on a sheet / on hold / which sheet / completed":
// where is this piece now?  The answer is worked out here from the cloud documents alone (truthOf: sheet records, pool rows, cancel records, custom
// records; never from the page, never from OrderPieces / PiecePlacement), and every surface must say the same.
//
//   node tests/charm-nest/placement-oracle.cjs              all scenarios, all surfaces, exit 1 on any surface that disagrees
//   node tests/charm-nest/placement-oracle.cjs --list       print the disagreements and exit 0 (to record a baseline)
//   --only hold,release   only those scenarios      --surfaces ow,list   only those surfaces      --dump   print every claim
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules  (CHROMIUM=<chrome> to override)
//
// How it runs. Real sorter pages share ONE fake backend (bridge-server.cjs: the real charmNestLibrary handler over an in-memory Firestore):
//   OWNER   the page that holds the run (live sheets, pool rows, B.run): the transition is made here with the app's own flows
//           (OrderHold.run / OrderHold.release / SheetWin.takeOffOrder), as a person does it; the other transitions are made through the library's
//           own ops, as another computer would (a sheet deleted, a piece completed by hand, an order put on a sheet).
//   VIEWERS one page per surface, each a second device: the Etsy pull and the cloud, nothing else (no live sheets, no pool rows, no run). A viewer
//           never saw the change happen; it must learn it from the cloud, with no reload, within 3 s of the cloud's last write (Paul's timeline
//           rule: "within two or three seconds").
// The owner's own surfaces are read after the change too (they must be right at once: the person who pressed the button sees them first).
// Offline only: no live endpoint, no Etsy, no paid model call (counted: the test fails if one is made), nothing written outside the in-memory fake.
const fs = require('fs'), path = require('path');
process.env.CHARM_NEST_DELETE_CODE = 'oracle-' + Math.random().toString(36).slice(2, 10);   // (the fake's sheet delete asks for a code: a made-up one, never the shop's)
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || [path.join(root, 'node_modules'), '/opt/node22/lib/node_modules/playwright/node_modules'].find(d => fs.existsSync(path.join(d, 'playwright-core')));
const { start } = require('./bridge-server.cjs');

const argv = process.argv.slice(2), flag = n => argv.includes('--' + n), opt = n => { const i = argv.indexOf('--' + n); return i >= 0 ? argv[i + 1] : ''; };
const ONLY = opt('only') ? new Set(opt('only').split(',')) : null, SURF = opt('surfaces') ? new Set(opt('surfaces').split(',')) : null, DUMP = flag('dump'), LIST = flag('list');
const sleep = ms => new Promise(r => setTimeout(r, ms));
const until = async (fn, ms = 20000, what = '') => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await sleep(100); } };

const SHEETS = 'Charm_Nest_Sheets', POOL = 'Charm_Pool', TL = 'Order_Timeline', CANC = 'Charm_Nest_Cancelled', CUSTOM = 'Charm_Custom_Orders', SETS = 'Charm_Nest_Sets', RUNS = 'Charm_Nest_Runs';
const RUN = 'run-oracle', BASE_TS = 1790000000, DAY = '2026-10-05';
const CODE = { gold: 'GF', silver: 'SS', rose: 'RG' };
const tx = n => 5000000000 + n;
const pid = (rid, n, copy) => `${rid}_${tx(n)}_${copy}`;
const lineKey = (rid, n) => `${rid}_${tx(n)}`;

/* ═══════════════════════════ the shop: the sheets and the orders ═══════════════════════════
   sheets: GF Sheet 1 and SS Sheet 1 in Set 1 (an order can be spread over both), GF Sheet 2 in Set 2. Each sheet carries filler orders (an order alone on a sheet cannot be held).
   Every scenario names the ONE order it is about (its pieces are the ones asked of every surface). Pieces are lines (n = the transaction number).
   `on`: a sheet id (one copy on it), an array of sheet ids (one copy each; null = that copy waits), or nothing (waiting). `custom`: a piece made by hand, never on a sheet. */
const SHEET_DEF = [
  { id: 'sh-gf1', metal: 'gold', n: 1, set: 'set-1' },
  { id: 'sh-ss1', metal: 'silver', n: 1, set: 'set-1' },
  { id: 'sh-gf2', metal: 'gold', n: 2, set: 'set-2' },
];
const SET_DEF = [{ id: 'set-1', seq: 1 }, { id: 'set-2', seq: 2 }];
const sheetLabel = s => `${CODE[s.metal]} Sheet ${s.n}`;
const SHEET = Object.fromEntries(SHEET_DEF.map(s => [s.id, s]));
const FILLER = [   // orders that stay put on their sheets, in every scenario (the engine may move one into freed room: the cloud says where it is)
  { rid: '4181000001', lines: [{ n: 1, metal: 'gold', on: 'sh-gf1' }] },
  { rid: '4181000002', lines: [{ n: 2, metal: 'gold', on: 'sh-gf1' }] },
  { rid: '4181000003', lines: [{ n: 3, metal: 'silver', on: 'sh-ss1' }] },
  { rid: '4181000004', lines: [{ n: 4, metal: 'silver', on: 'sh-ss1' }] },
  { rid: '4181000005', lines: [{ n: 5, metal: 'gold', on: 'sh-gf2' }] },
  { rid: '4181000006', lines: [{ n: 6, metal: 'gold', on: 'sh-gf2' }] },
];
const copiesOf = l => l.custom ? [] : Array.isArray(l.on) ? l.on : [l.on || null];
/** The order a scenario is about: two pieces (lines). */
const subject = (rid, lines) => ({ rid, lines: lines.map((l, i) => Object.assign({ n: 10 + i, metal: 'gold' }, l)) });

/* ═══════════════════════════ spec -> cloud documents and page rows ═══════════════════════════ */
const specOf = orders => {
  const sheets = SHEET_DEF.map(s => ({ id: s.id, metal: s.metal, page: s.n, set: s.set, items: [] }));
  for (const o of orders) for (const l of o.lines) copiesOf(l).forEach((on, i) => { if (on) sheets.find(s => s.id === on).items.push([o.rid, l.n, i + 1]); });
  return { sheets: sheets.filter(s => s.items.length), orders };
};
const fileBase = s => `${CODE[s.metal]}_${DAY}_Set-${SET_DEF.find(x => x.id === s.set).seq}_Sheet-${s.n}`;

/** The sheet documents, pool rows, run record and set documents of a spec, as the app saves them. */
function cloudDocs(spec) {
  const NOW = Date.now(), docs = { sheets: [], pool: [], run: null, sets: [] }, lines = {};
  for (const o of spec.orders) for (const l of o.lines) {
    const key = lineKey(o.rid, l.n), cs = copiesOf(l);
    lines[key] = { orderId: o.rid, transactionId: String(tx(l.n)), sku: l.custom ? '' : 'TEST-' + l.n, state: cs.length && cs.every(Boolean) ? 'written' : l.custom ? 'noDesign' : 'pooled', quantity: Math.max(1, cs.length), material: l.metal, poolIds: cs.map((_, i) => pid(o.rid, l.n, i + 1)), noDesign: !!l.custom };
  }
  docs.run = { runId: RUN, status: 'running', step: 'nest', day: DAY, lines, orders: spec.orders.map(o => o.rid), sheets: {}, holds: {}, errors: [], resumable: true };
  for (const sh of spec.sheets) {
    const s = SHEET[sh.id], charms = [], placements = [], poolIds = [], orders = [];
    sh.items.forEach(([rid, n, copy], i) => {
      const poolId = pid(rid, n, copy), id = `${sh.id}-c${i}`;
      charms.push({ id, poolId, order: rid, name: `${rid} · TEST-${n}` }); poolIds.push(poolId); if (!orders.includes(rid)) orders.push(rid);
      placements.push({ id, cxPt: 30 + (i % 6) * 40, cyPt: 30 + Math.floor(i / 6) * 40, angle: 0, wPt: 28, hPt: 28 });
    });
    docs.sheets.push({ id: sh.id, setId: s.set, setSeq: SET_DEF.find(x => x.id === s.set).seq, sheetIndex: s.n, runId: RUN, metal: s.metal, day: DAY, fileBase: fileBase(s), folder: fileBase(s), status: 'complete', placedCount: placements.length, charmCount: placements.length,
      density: .5, stock: { wPt: 300, hPt: 150, wIn: 6, hIn: 4.5 }, placements, charms, poolIds, orders, verification: { ok: true }, outputs: {}, label: { files: [], orders }, createdAt: NOW - 3600e3, updatedAt: NOW - 600e3 });
  }
  for (const o of spec.orders) for (const l of o.lines) copiesOf(l).forEach((on, i) => docs.pool.push({ poolId: pid(o.rid, l.n, i + 1), orderId: o.rid, transactionId: String(tx(l.n)), lineKey: lineKey(o.rid, l.n), sku: 'TEST-' + l.n, material: l.metal, copy: i + 1, quantity: copiesOf(l).length, runId: RUN,
    state: on ? 'written' : 'ready', sheetId: on || null, setId: on ? SHEET[on].set : null, sheetName: on ? fileBase(SHEET[on]) : null, createdAt: NOW - 3600e3, updatedAt: NOW - 600e3 }));
  for (const set of SET_DEF) { const sh = spec.sheets.filter(x => SHEET[x.id].set === set.id); if (sh.length) docs.sets.push({ setId: set.id, seq: set.seq, day: DAY, runId: RUN, sheetIds: sh.map(x => x.id), materials: [...new Set(sh.map(x => x.metal))], orders: {}, labelFiles: [], status: 'labelled' }); }
  return docs;
}
function clearCloud(srv) { for (const k of [SHEETS, POOL, TL, CANC, CUSTOM, SETS, RUNS]) for (const [key] of [...srv.st.docs]) if (key.startsWith(k + '/')) srv.raw.del(key); }
function seedCloud(srv, spec) {
  clearCloud(srv);
  const put = srv.raw.set, d = cloudDocs(spec);
  for (const s of d.sheets) put(SHEETS + '/' + s.id, s);
  for (const p of d.pool) put(POOL + '/' + p.poolId, p);
  for (const s of d.sets) put(SETS + '/' + s.setId, s);
  put(RUNS + '/' + d.run.runId, d.run);
}

/* ═══════════════════════════ the cloud's own truth: where is each piece now ═══════════════════════════
   From the documents alone (never from the page). cancelled: the order's cancel record is there; done: the piece's custom record is completed (by
   hand or by a printed QR label) and is not open again; sheet: a saved, not archived sheet record lists one of its copies; held: the piece's pool
   rows were taken off for a hold (abandoned, heldAt, no sheet claims them); otherwise it waits for a sheet (a piece made by hand waits as `open`). */
function truthOf(st, orders) {
  const out = {}, sheets = st.list(SHEETS).filter(s => !s.archived);
  for (const o of orders) {
    const cancelled = !!st.doc(CANC, o.rid);
    for (const l of o.lines) {
      const rec = st.doc(CUSTOM, lineKey(o.rid, l.n)), ids = copiesOf(l).map((_, i) => pid(o.rid, l.n, i + 1)), rows = ids.map(id => st.doc(POOL, id)).filter(Boolean);
      const on = sheets.filter(s => (s.poolIds || []).some(p => ids.includes(p))), copiesOn = ids.filter(id => sheets.some(s => (s.poolIds || []).includes(id))).length;
      let state, at = [];
      if (cancelled) state = 'cancelled';
      else if (l.custom) state = rec && rec.state === 'completed' ? 'done' : 'open';
      // a take-off (a hold) is written in the same commit that edits the sheet record, and wins over a record that still lists the piece (C3: _charmNestPlacement.js)
      else if (rows.length && rows.every(r => r.state === 'abandoned' && r.heldAt && !r.removedAt && !r.repooledAt && !r.sheetId)) state = 'held';
      else if (on.length) { state = 'sheet'; at = [...new Set(on.map(s => sheetLabel(SHEET[s._id])))].sort(); }
      else state = 'waiting';
      out[lineKey(o.rid, l.n)] = { rid: o.rid, n: l.n, state, sheets: at, how: rec && rec.how || null, partial: state === 'sheet' && copiesOn < ids.length };
    }
  }
  return out;
}
/** The sheet records as the cloud holds them now: id -> { label, set, orders:[rid], charms } */
const sheetTruth = st => Object.fromEntries(st.list(SHEETS).filter(s => !s.archived).map(s => [s._id, { label: sheetLabel(SHEET[s._id]), set: SHEET[s._id].set, orders: [...new Set((s.poolIds || []).map(p => String(p).split('_')[0]))].sort(), charms: (s.poolIds || []).length }]));

/* ═══════════════════════════ what a surface's words say ═══════════════════════════ */
/** A surface's words, read as a claim: which placements the words say, and which sheets they name. */
function say(text) {
  let t = ' ' + String(text == null ? '' : text).replace(/\s+/g, ' ') + ' ';
  t = t.replace(/Taken off [^.]*? by \S+/gi, ' ').replace(/\bwas on [^.;]*/gi, ' ');   // (a hold's own history: where it WAS)
  const off = /\bnot on a sheet\b|\bnot placed\b|\bunplaced\b|\boff (its|the) sheet\b/i.test(t);   // says only that it is on no sheet: true of a piece waiting, held, cancelled or made by hand
  t = t.replace(/\bnot on a sheet( yet)?\b/gi, ' ');
  const labels = [...new Set([...t.matchAll(/\b(GF|SS|RG|10K|14K) Sheet (\d+)\b/g)].map(m => `${m[1]} Sheet ${m[2]}`).concat([...t.matchAll(/\b(GF|SS|RG)_\d{4}-\d\d-\d\d_Set-\d+_Sheet-(\d+)\b/g)].map(m => `${m[1]} Sheet ${m[2]}`)))].sort();
  const states = new Set();
  if (/cancel/i.test(t)) states.add('cancelled');
  if (/\bon hold\b|\bheld\b/i.test(t)) states.add('held');
  if (/\bcompleted by hand\b|\bcompleted\b|\bmade by hand\b|custom · done|\bqr label printed\b|\bdone\b/i.test(t)) states.add('done');
  if (labels.length || /\b(nested|written|on sheet|on a sheet|laser cut|engraved|labelled|committed|next: (engraved|laser cut|sorted|welded|assembled|shipped))\b/i.test(t)) states.add('sheet');
  if (/\bwaiting\b|\bpooled\b/i.test(t)) states.add('waiting');
  if (off && !states.size) states.add('off');
  const more = labels.length ? +((/\+(\d+)\b/.exec(t) || [])[1]) || 0 : 0;   // ("GF Sheet 1 +1": the first sheet named, and how many others it is on)
  return { states, labels, more };
}
/** One claim judged against the cloud's truth (T: truthOf, S: sheetTruth). Returns a list of what is wrong (empty: it agrees). */
function judge(c, T, S) {
  const bad = [], quote = t => JSON.stringify(String(t)).slice(0, 90), orderT = Object.values(T).filter(t => t.rid === c.rid), tl = c.n != null ? T[lineKey(c.rid, c.n)] : null;
  switch (c.kind) {
    case 'count': {                // a sheet's counts (Library card, sheet window)
      const s = S[c.sheetId];
      if (c.deleted) return s ? [`says the sheet is no longer there, the cloud has ${s.label}`] : [];   // (a window that says so of a sheet the cloud no longer has agrees)
      if (!s) return [`shows ${c.sheetId}, which the cloud does not have`];
      if (c.orders != null && c.orders !== s.orders.length) bad.push(`says ${c.orders} order(s), the cloud's ${s.label} has ${s.orders.length}`);
      if (c.charms != null && c.charms !== s.charms) bad.push(`says ${c.charms} charm(s), the cloud's ${s.label} has ${s.charms}`);
      if (c.backs != null && c.backs !== s.charms) bad.push(`"what is left" counts ${c.backs} back engraving(s), the cloud's ${s.label} has ${s.charms} piece(s)`);
      return bad;
    }
    case 'ids': { const want = Object.keys(S).sort(); return c.ids.join() === want.join() ? [] : [`shows sheets ${c.ids.join(', ') || 'none'}, the cloud has ${want.join(', ')}`]; }
    case 'shared': {               // the orders that tie a sheet to its set: on that sheet and on a sheet outside the set it is going to
      const s = S[c.sheetId], want = s ? s.orders.filter(rid => Object.entries(S).some(([id, o]) => id !== c.sheetId && o.set !== c.targetSetId && o.orders.includes(rid))) : [];
      return c.rids.join() === want.join() ? [] : [`lists orders ${c.rids.join(', ') || 'none'}, the cloud's are ${want.join(', ') || 'none'}`];
    }
    case 'holdPile': { const want = new Set(Object.values(T).filter(t => t.state === 'held').map(t => t.rid)).size; return c.count === want ? [] : [`On hold pile counts ${c.count} order(s), the cloud has ${want} held`]; }
    case 'openSheet': { const on = tl && tl.state === 'sheet'; return c.shown === !!on ? [] : [`${c.shown ? 'offers' : 'does not offer'} Open sheet, the piece is ${tl ? tl.state : '?'}${on ? ' on ' + tl.sheets.join(' + ') : ''}`]; }
    case 'cancelFlag': { const want = orderT.length > 0 && orderT.every(t => t.state === 'cancelled'); return c.cancelled === want ? [] : [`says the order is ${c.cancelled ? '' : 'not '}cancelled, the cloud says ${want ? '' : 'not '}cancelled`]; }
  }
  if (c.dots != null) {            // the progress dots: Nested done means it is on a sheet (or later)
    const states = tl ? [tl.state] : orderT.map(t => t.state), off = states.every(s => ['waiting', 'held', 'open'].includes(s));
    if (c.dots >= 2 && off) bad.push(`${c.dots - 1} step(s) beyond Order in drawn done, but ${tl ? 'the piece is ' + tl.state : 'no piece is on a sheet'}`);
    if (c.dots < 2 && tl && tl.state === 'sheet' && !tl.partial) bad.push(`Nested drawn hollow, but the piece is on ${tl.sheets.join(' + ')}`);
    return bad;
  }
  const w = say(c.text), states = tl ? [tl.state] : orderT.map(t => t.state), allowed = new Set(states.flatMap(s => s === 'sheet' ? (tl && tl.partial ? ['sheet', 'waiting', 'off'] : ['sheet']) : [s === 'open' ? 'waiting' : s, 'off']));   // (a piece with some copies on a sheet and some not may say either)
  const sheetsOK = tl ? tl.sheets : [...new Set(orderT.flatMap(t => t.sheets))], truth = [...new Set(states)].join('/');
  const wrong = [...w.states].filter(s => !allowed.has(s));
  if (wrong.length) bad.push(`says ${[...w.states].join(' + ')} (${quote(c.text)}), the cloud says ${truth}`);
  else if (tl && w.states.size > 1 && !tl.partial) bad.push(`contradicts itself (${quote(c.text)}): ${[...w.states].join(' and ')}`);
  else if (!w.states.size && (c.required === true || (typeof c.required === 'string' && states.some(s => c.required.split(',').includes(s))))) bad.push(`says nothing about where it is (${quote(c.text)}), the cloud says ${truth}`);
  const stray = w.labels.filter(l => !sheetsOK.includes(l));
  if (stray.length) bad.push(`names ${stray.join(', ')} (${quote(c.text)}), the cloud's sheet${sheetsOK.length === 1 ? ' is' : 's are'} ${sheetsOK.join(', ') || 'none'}`);
  else if (w.labels.length && tl && tl.state === 'sheet' && !tl.partial && !c.somePlaces && !(c.copy && sheetsOK.length > 1) && (w.more ? w.labels.length + w.more !== sheetsOK.length : w.labels.join() !== sheetsOK.join())) bad.push(`names ${w.labels.join(', ')}${w.more ? ' +' + w.more : ''}, the cloud's are ${sheetsOK.join(', ')}`);
  return bad;
}

/* ═══════════════════════════ the backend and the pages ═══════════════════════════ */
const ETSY_FN = new Set(['listOpenOrders', 'etsyOrderProxy', 'etsyImages', 'refreshEtsyToken', 'etsySandbox']);
const PAID_FN = /charmNestAgent|charmEngrave|charmMaster|callClaude/;
async function backend() {
  const srv = await start({ receipts: [] }), st = srv.st, writes = [];
  const set = st.docs.set.bind(st.docs), del = st.docs.delete.bind(st.docs);
  srv.lastWrite = Date.now();
  st.docs.set = (k, v) => { writes.push(k); srv.lastWrite = Date.now(); return set(k, v); };
  st.docs.delete = k => { writes.push('-' + k); srv.lastWrite = Date.now(); return del(k); };
  const anthro = require(path.join(root, 'netlify/functions/_etsyMailAnthropic.js')), real = anthro.callClaudeRaw;
  // (the harness model is already a stub; this makes any call to it a counted failure, and one that answers nothing)
  srv.paid = 0; anthro.callClaudeRaw = async () => { srv.paid++; throw new Error('the placement oracle makes no AI call'); };
  srv.writes = writes; srv.raw = { set: (k, v) => set(k, v), del: k => del(k) }; srv.outside = [];
  /** the library's op, called as another computer would: the real handler over the same in-memory store */
  srv.call = async body => { const out = await st.handlers.charmNestLibrary.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body), queryStringParameters: {} }); return JSON.parse(out.body || '{}'); };
  return srv;
}
// opts.ctx: a browser context already made here (a second tab of the same computer: shared storage and BroadcastChannel; its routes and init script are set once)
// opts.motion: true keeps the app's real motion (stamps and flights delay the page's own redraws, as they do for a person); the default is reduced motion
// opts.employee: the name the context signs in with (the owner's is Paul, a viewer's is Viewer)   opts.latency: ms added to every call to a Netlify function (a real call is never instant)
async function openPage(browser, srv, { owner, name, ctx: shared, motion, employee, latency }) {
  const fresh = !shared, ctx = shared || await browser.newContext({ viewport: { width: 1500, height: 950 }, reducedMotion: motion ? 'no-preference' : 'reduce' });
  if (fresh) await ctx.route(url => !/^https?:\/\/(127\.0\.0\.1|localhost)[:/]/.test(url.href), r => {
    const u = r.request().url();
    if (/gstatic\.com\/firebasejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(u) ? '' : "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, uploadBytes = nope;" });
    if (/qrcodejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) });
    if (/fonts\.g/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
    srv.outside.push(u); return r.abort();
  });
  // a page that asks Claude to read a custom order (Review does, for a piece made by hand) is answered "nothing" here, before the fake sees it: the oracle never reaches an AI, paid or not (counted: srv.agentAsked)
  if (fresh) await ctx.route(/\/\.netlify\/functions\/(charmNestAgent|charmEngrave|charmMaster|callClaude)/, route => { srv.agentAsked = (srv.agentAsked || 0) + 1; return route.fulfill({ status: 200, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': '*' }, body: JSON.stringify({ ok: true, skipped: 'the placement oracle makes no AI call' }) }); });
  // the order timeline as the cloud answers it: the recorded events plus the ones DERIVED from the pool, the sheets and the sets (the fake's own timelineGet answers recorded events alone)
  if (fresh) await ctx.route(/\/\.netlify\/functions\/charmNestLibrary/, async route => {
    const req = route.request(); let body = {}; try { body = JSON.parse(req.postData() || '{}'); } catch (_) {}
    // a page's own ask for a reading by the model (startAgent: Review's customRead of a piece made by hand) is turned back here: no job is parked, no model is reached, not even the fake one (counted: srv.agentAsked)
    if (req.method() === 'POST' && body.op === 'startAgent') { srv.agentAsked = (srv.agentAsked || 0) + 1; return route.fulfill({ status: 200, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': '*' }, body: JSON.stringify({ error: 'the placement oracle makes no AI call' }) }); }
    if (req.method() !== 'POST' || body.op !== 'timelineGet') return route.fallback();
    try { const out = await srv.st.handlers.charmNestLibrary.handler({ httpMethod: 'POST', headers: {}, body: req.postData(), queryStringParameters: {} }); await route.fulfill({ status: out.statusCode || 200, headers: Object.assign({ 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': '*' }, out.headers || {}), body: out.body || '{}' }); }
    catch (e) { await route.fulfill({ status: 500, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ error: String(e.message) }) }); }
  });
  if (fresh && latency) await ctx.route(/\/\.netlify\/functions\//, async r => { await sleep(latency); return r.fallback(); });   // (registered last, so it answers first: every call takes `latency` ms more)
  if (fresh) await ctx.addInitScript(({ owner, employee }) => {
    try {
      if (!localStorage.getItem('cn.employee')) localStorage.setItem('cn.employee', employee || (owner ? 'Paul' : 'Viewer'));
      const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); s.pollOrders = 'off'; s.runMode = 'manual'; s.sound = 'off'; s.notify = 'off'; s.review = 'on'; s.dsOrigin = 'http://127.0.0.1:9'; localStorage.setItem('cn.settings', JSON.stringify(s));
    } catch (_) { /* about:blank */ }
    window.confirm = () => false; window.prompt = () => 'Paul'; window.alert = () => {};   // (false: a viewer never takes up the run offered on the banner)
    if (!owner) return;
    window.confirm = () => true;
    // the owner's nest is a stub (the hold and the release place pieces and save the sheets through it, as order-hold-engine.cjs does)
    const iv = setInterval(() => {
      if (typeof window.startNest !== 'function' || window.startNest.__stub) return;
      const MM = 72 / 25.4, P = window.CharmNestPDF, realPath = P.pathToCanvas;
      P.drawCharm = (c2, c, t, k) => { const [x, y] = t(c.centerPt[0], c.centerPt[1]); c2.beginPath(); c2.arc(x, y, c.rMm * MM * k, 0, 7); c2.lineWidth = Math.max(1, .35 * k); c2.strokeStyle = '#d0312d'; c2.stroke(); };
      P.pathToCanvas = (c2, p, t) => { if (p && p.circle) { const [x, y] = t(p.cx, p.cy), [x1] = t(p.cx + p.r, p.cy), r = Math.abs(x1 - x); c2.moveTo(x + r, y); c2.arc(x, y, r, 0, Math.PI * 2); return; } return realPath(c2, p, t); };
      P.cutLinesOf = () => [];
      const stubNest = sh => {
        sh.status = 'nesting'; sh.persistedDone = false; sh.persisted = Promise.resolve(); sh.jobId = 'job-' + Math.random().toString(36).slice(2, 8); sh.stage = 'test nest';
        setTimeout(async () => {
          try {
            for (const c of sh.charms) if (c.pinned && !sh.placements.some(p => p.id === c.id)) sh.placements.push({ id: c.id, cxPt: c.pinned.cxPt, cyPt: c.pinned.cyPt, angle: c.pinned.angle || 0, wPt: c.widthPt, hPt: c.heightPt });
            sh.placements = sh.placements.filter(p => sh.charms.some(c => c.id === p.id));
            await api('charmNestLibrary', { op: 'putSheet', sheet: { id: sh.sheetId, metal: sh.metal, charms: sh.charms.map(c => ({ id: c.id, poolId: c.poolId, order: c.order })), placements: sh.placements.map(p => Object.assign({}, p)), poolIds: sh.charms.map(c => c.poolId), orders: [...new Set(sh.charms.map(c => c.order))], placedCount: sh.placements.length, charmCount: sh.charms.length } }, { quiet: true });
          } catch (e) { sh.problem = e.message; }
          sh.status = sh.problem ? 'ready' : 'complete'; sh.dirty = !!sh.problem; sh.intakeAppend = false; sh.appendOnly = false; sh.stage = ''; sh.persistedDone = !sh.problem; sh.density = Math.min(.9, sh.placements.length * .08);
          try { CN.renderCard(sh); } catch (_) {}
        }, 120);
      };
      stubNest.__stub = true; window.startNest = stubNest; clearInterval(iv);
    }, 0);
  }, { owner, employee });
  const page = await ctx.newPage(), errors = [];
  page.setDefaultTimeout(20000);
  page.on('pageerror', e => errors.push('page: ' + e.message));
  page.on('console', m => { if (m.type() === 'error' && !/firebase stub|Failed to load resource|ERR_FAILED|net::|forced failure/.test(m.text())) errors.push('console: ' + m.text().slice(0, 240)); });
  await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { timeout: 90000 });
  await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.Orders && window.OrderWin && window.SheetWin && SheetWin.takeOffOrder && window.OrderHold && window.HoldUI && window.startNest, null, { timeout: 90000 });
  if (owner) await page.waitForFunction(() => window.startNest.__stub, null, { timeout: 20000 });
  return { ctx, page, errors, name, owner };
}
/** The pages' rows: the pulled lines of the orders. A VIEWER holds nothing else ('pooled', what a freshly pulled line is). The OWNER also holds the sheets live,
 *  the pool rows and the open run (the run's own state: what a person at the machine that cut the sheets sees). */
async function seedPage(page, spec, { owner }) {
  await page.evaluate(({ spec, owner, RUN, BASE_TS }) => {
    localStorage.removeItem('cn.orderhold.run'); localStorage.removeItem('cn.sheetwin.freed');
    const MM = 72 / 25.4, R = 5, D = Math.ceil(2 * R * MM) + 2, tid = n => 5000000000 + n;
    const mkBits = () => { const b = new Uint8Array(D * D); for (let y = 0; y < D; y++) for (let x = 0; x < D; x++) if (Math.hypot(x + .5 - D / 2, y + .5 - D / 2) <= D / 2 - 1) b[y * D + x] = 1; return b; };
    for (const m of Object.keys(CN.S.sheets)) { const pr = CN.S.sheets[m]; pr.pages.length = 1; const p0 = pr.pages[0]; p0.charms = []; p0.placements = []; p0.sheetId = null; p0.fileBase = null; for (const k of ['laserDoneAt', 'roseCutAt', 'recalled', 'setId', 'rosePlan', 'roseProtected']) delete p0[k]; p0.status = 'idle'; p0.page = 1; pr.active = 0; }
    window.B.pool.rows.clear();
    if (owner) for (const sh of spec.sheets) {
      const pr = CN.S.sheets[sh.metal]; let pg = null;
      if (sh.page === 1) pg = pr.pages[0]; else { while (pr.pages.length < sh.page) addPage(sh.metal); pg = pr.pages[sh.page - 1]; }
      pg.charms = []; pg.placements = [];
      sh.items.forEach(([rid, n, copy], i) => {
        const poolId = `${rid}_${tid(n)}_${copy}`, id = `${sh.id}-c${i}`;
        pg.charms.push({ id, name: `${rid} · TEST-${n}`, poolId, order: rid, lineKey: `${rid}_${tid(n)}`, sku: 'TEST-' + n, sourceId: 's', ringGeometryVersion: 3, centerPt: [D / 2, D / 2], bbox: [0, 0, D, D], outline: { circle: 1, cx: 0, cy: 0, r: R * MM }, members: [], rMm: R, w: D, h: D, scale: 1, bits: mkBits(), widthPt: 2 * R * MM, heightPt: 2 * R * MM, areaPt: Math.PI * (R * MM) ** 2 });
        pg.placements.push({ id, cxPt: 30 + (i % 6) * 40, cyPt: 30 + Math.floor(i / 6) * 40, angle: 0, wPt: 2 * R * MM, hPt: 2 * R * MM });
        window.B.pool.rows.set(poolId, { poolId, orderId: rid, sheetId: sh.id, state: 'placed', material: sh.metal });
      });
      Object.assign(pg, { status: 'complete', sheetId: sh.id, fileBase: sh.fileBase, sheetIndex: sh.page, page: sh.page, setId: sh.set, runId: RUN, dirty: false, persistedDone: true, persisted: Promise.resolve(), problem: null, density: .3, verification: { ok: true }, intakeAppend: false, appendOnly: false });
      for (const k of ['laserDoneAt', 'roseCutAt', 'recalled']) delete pg[k];
      pr.active = Math.max(0, sh.page - 1); CN.renderCard(pg);
    }
    const rows = [];
    for (const o of spec.orders) for (const l of o.lines) {
      const cs = l.custom ? [] : Array.isArray(l.on) ? l.on : [l.on || null];
      const order = { receiptId: o.rid, orderNumber: o.rid, createTs: BASE_TS - 9000, updateTs: BASE_TS, shipBy: BASE_TS + 500000, buyer: { name: 'Buyer ' + o.rid.slice(-3) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [] };
      const ln = { transactionId: String(tid(l.n)), listingId: '1800' + l.n, sku: l.custom ? '' : 'TEST-' + l.n, title: l.custom ? 'Custom piece ' + l.n : 'Test charm ' + l.n, quantity: Math.max(1, cs.length), variations: [{ name: 'Metal', value: l.metal === 'silver' ? 'Sterling Silver' : '14k Gold Filled' }], metalKey: l.metal, metalLabel: l.metal === 'silver' ? 'Sterling Silver' : '14k Gold Filled', personalization: [] };
      order.lines = [ln];
      const key = `${o.rid}_${ln.transactionId}`;   // (the app's own line key: receipt_transaction)
      if (!l.custom) window.B.master.entries.set('TEST-' + l.n, { sku: 'TEST-' + l.n, updatedAt: 1 });   // (a design in a master file: no "Unknown SKU" question on the line)
      rows.push({ key, order, line: ln, spec: { designSku: ln.sku, quantity: Math.max(1, cs.length), material: l.metal, problems: [], ...(l.custom ? { noDesign: true, special: { label: 'Custom', notCut: true } } : {}) }, problems: [], state: l.custom ? 'noDesign' : (owner && cs.length && cs.every(Boolean) ? 'written' : 'pooled'), reason: null,
        poolIds: cs.map((_, i) => `${o.rid}_${tid(l.n)}_${i + 1}`), engrave: null, material: l.metal, arrivedAt: Date.now() - 7200000 });
    }
    window.B.orders.rows = rows; window.B.orders.byKey = new Map(rows.map(r => [r.key, r]));
    window.B.sets.clear();
    try { Cancelled.reset && Cancelled.reset(); } catch (_) {}
    try { OrderPieces._learned && OrderPieces._learned.clear(); } catch (_) {}
    if (owner) {   // the open run: the owner's hold saves its lines (hold, state) into the run's record, as the real machine does
      window.B.run = { runId: RUN, status: 'running', step: 'nest', day: spec.day || '2026-10-05', setId: null, releasePolicy: 2, solidIncluded: {}, mode: 'manual', startedAt: Date.now() - 3600e3, updatedAt: Date.now(), lines: Object.fromEntries(rows.map(r => Orders.lineRecord(r))), sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: spec.orders.map(o => o.rid) };
    } else window.B.run = null;
  }, { spec: Object.assign({}, spec, { sheets: spec.sheets.map(s => Object.assign({}, s, { fileBase: fileBase(SHEET[s.id]) })) }), owner, RUN, BASE_TS });
  await page.waitForTimeout(200);
}

module.exports = { SHEETS, POOL, TL, CANC, CUSTOM, SETS, RUNS, RUN, BASE_TS, DAY, CODE, SHEET_DEF, SET_DEF, SHEET, FILLER, subject, specOf, cloudDocs, seedCloud, truthOf, sheetTruth, say, judge, pid, lineKey, tx, fileBase, sheetLabel, sleep, until, backend, openPage, seedPage };
if (require.main === module) require('./placement-oracle-run.cjs').main(module.exports).catch(e => { console.error(e); process.exit(2); });
