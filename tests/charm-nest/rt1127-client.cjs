/* RT1127 client (Paul, 27 Nov 2026: "The Library sheets are NOT updating to the latest versions from the cloud. I have also seen sheets update and reverse back to their previous incorrect state ...
   it can not take more than 30s at worst case scenario to visually update the system everywhere ... I also did not see any feedback to the user when a sheet is being altered"), Node only, offline:
   node tests/charm-nest/rt1127-client.cjs
   The page's own code is run (vm), each part from its file; what only a browser can show is checked in the code's text.
     A · NEWERWINS, the store (charm-nest-store.js): an older answer never replaces a newer copy (by revision, by the record's counters); a list's rows and order stay the newer; a read asked after
         news is never handed the read that left before it (FRESHREAD)
     B · NEWERWINS, the Library (charm-nest-bridge.js LaserReview record / applyStatus): an older read changes nothing; a newer one shows its picture at once (picNow)
     C · SHEETFRESH, the Library's own drawing (charm-nest-1.html LIVEPV): a page behind the cloud (layout or content revision) shows the cloud's picture under its version; picUrl versions by previewRev too
     D · SHEETFRESH, the sheet window (charm-nest-sheetwin.js follow): a layout / picture change reads it again; a change named while busy is kept and read once free; Manual Control alone does not hold it
     E · FEEDRACE (charm-nest-sync-worker.js): a write named by an answer is read by a surface whose own read began while the question was out; quietSince and newsAt follow the answer's time
     F · SHEETFRESH, the Nest cards (charm-nest-cloud-sheets.js): a hand move elsewhere (contentRev alone) rebuilds; a copy that never matched learns or is rebuilt when it follows; Manual Control defers
     G · run-follow (charm-nest-run-follow.js): a name heard while the run is being read is read once more right after
     H · SHEETBUSY (charm-nest-1.html): "Updating…" on the Library card, the sheet window's header and the order window's chips while the cloud changes the sheet; gone with the finished record
     I · the wiring only a browser runs (text): api() FRESHREAD, the order window's Sheet tab on the feed, the Library's unknown sheets, the window's versioned picture, the spinner's sources */
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm');
const ROOT = path.join(__dirname, '..', '..');
const read = f => fs.readFileSync(path.join(ROOT, f), 'utf8');
let passed = 0, failed = 0;
const ok = (name, cond, detail) => { console.log((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + JSON.stringify(detail).slice(0, 600))); if (cond) passed++; else failed++; };
const between = (src, from, to) => { const a = src.indexOf(from); if (a < 0) return null; const b = src.indexOf(to, a + from.length); return b < 0 ? null : src.slice(a, b + to.length); };
const upto = (src, from, to) => { const a = src.indexOf(from); if (a < 0) return null; const b = src.indexOf(to, a + from.length); return b < 0 ? null : src.slice(a, b); };   // (the end marker left out)
const sleep = ms => new Promise(r => setTimeout(r, ms));
const freshStore = () => { const f = require.resolve(path.join(ROOT, 'charm-nest-store.js')); delete require.cache[f]; return require(f); };
const BASE = { console, setTimeout, clearTimeout, Date, Math, JSON, Promise, Map, Set, WeakMap, Object, Array, String, Number, Boolean, Error, TypeError, RegExp, Symbol, Infinity, NaN, isFinite, parseInt, parseFloat, URL };

(async () => {
  /* ── A · the store ── */
  {
    const St = freshStore(); await St.init({ world: 'production' });
    St.put('sheets', 's1', { id: 's1', v: 'new', updatedAt: 2000 }, '200.5');
    const took = St.put('sheets', 's1', { id: 's1', v: 'old', updatedAt: 1000 }, '100.9');
    ok('A1 an answer read at an older revision does not replace the newer copy', !took && St.get('sheets', 's1').v === 'new' && St.rev('sheets', 's1') === '200.5', { took, v: St.get('sheets', 's1').v });
    St.put('sheets', 's2', { id: 's2', v: 'new', updatedAt: 2000, layoutRev: 5, previewAt: 1900 }, '');
    St.put('sheets', 's2', { id: 's2', v: 'old', updatedAt: 1500, layoutRev: 4, previewAt: 1900 }, '');
    ok('A2 no revision either side: the record\'s counters decide (updatedAt, layoutRev lower, none higher: older)', St.get('sheets', 's2').v === 'new');
    St.put('sheets', 's2', { id: 's2', v: 'same', updatedAt: 2000, layoutRev: 5, previewAt: 1900 }, '');
    St.put('sheets', 's3', { id: 's3', v: 'a', updatedAt: 1000 }, '100.1'); St.put('sheets', 's3', { id: 's3', v: 'b', updatedAt: 1000 }, '100.2');
    ok('A3 the same counters, or a newer revision: taken as before', St.get('sheets', 's2').v === 'same' && St.get('sheets', 's3').v === 'b');
    St.put('sheets', 'c', { id: 'c', v: 'held new', updatedAt: 5000 }, '500.1');
    const idOf = x => x.id;
    const rows = St.take('L', 'sheets', { sheets: [{ id: 'c', v: 'list old', updatedAt: 4000 }, { id: 'd', v: 'd1', updatedAt: 4000 }], ids: ['c', 'd'], upTo: '9.1', rev: '450.0-7', revs: { c: '400.1', d: '400.2' } }, 'sheets', idOf);
    ok('A4 a list\'s answer: a row it sends older than the one held is drawn as held (the newer)', rows && rows.map(r => r.v).join(',') === 'held new,d1', rows && rows.map(r => r.v));
    const rows2 = St.take('L2', 'sheets', { sheets: [{ id: 'c', v: 'older server list', updatedAt: 4000 }] }, 'sheets', idOf);
    ok('A5 a list from an older server (no ids): its rows are the newest held too', rows2 && rows2[0].v === 'held new', rows2);
    St.take('L3', 'sheets', { sheets: [{ id: 'e', updatedAt: 1 }, { id: 'f', updatedAt: 1 }], ids: ['e', 'f'], upTo: '9.2', rev: '600.0-9', revs: { e: '1.1', f: '1.2' } }, 'sheets', idOf);
    const back = St.take('L3', 'sheets', { sheets: [], ids: ['e'], upTo: '9.0', rev: '590.0-8', revs: {} }, 'sheets', idOf);
    ok('A6 a list answer older than the list held (two reads out, the earlier landing last) does not take the list back', back && back.length === 2 && St.list('L3').rev === '600.0-9', { back: back && back.map(r => r.id), rev: St.list('L3').rev });
    let made = 0; const mk = () => new Promise(r => setTimeout(() => r({ n: ++made }), 30));
    const p1 = St.shared('k', mk); await sleep(5); const news = Date.now(); await sleep(5);
    const p2 = St.shared('k', mk, { after: news }), p3 = St.shared('k', mk, { after: news });
    const [x1, x2, x3] = await Promise.all([p1, p2, p3]);
    ok('A7 FRESHREAD: a read asked after news is not handed the read that left before it; reads asked after it share one', made === 2 && x1.n === 1 && x2.n === 2 && x3.n === 2, { made, x1, x2, x3 });
    const p4 = St.shared('j', mk), p5 = St.shared('j', mk, { after: 0 });
    ok('A8 FRESHREAD: with no news since it left, the read in flight is shared as before', (await p4).n === (await p5).n);
  }

  /* ── B · the Library's records ── */
  {
    const bsrc = read('charm-nest-bridge.js');
    const St = freshStore(); await St.init({ world: 'production' });
    const ctx = vm.createContext(Object.assign({}, BASE));
    ctx.window = ctx; ctx.CharmNestStore = St; ctx.records = new Map(); ctx.sets = new Map(); ctx.live = { sigs: new Map(), shape: new Map() };
    const keeps = [], pics = []; ctx.keep = (s, r) => keeps.push(s.id); ctx.picNow = (s, id) => pics.push(id); ctx.patchRow = () => {}; ctx.wantReload = () => {};
    ctx.document = { querySelectorAll: () => [] };
    const rec = between(bsrc, '  const olderRec=(a,b)=>', "  const recOf=id=>records.get(String(id || '')) || null;") || (bsrc.match(/\n  function record\(s,rev\)\{[^\n]*\n/) || [''])[0];
    const app = upto(bsrc, '  function applyStatus(response,ids,always){', '\n  const arm=ms=>');
    try { vm.runInContext(rec + '\n' + app + '\nthis.LR = { record, applyStatus };', ctx); } catch (e) { ok('B0 LaserReview record / applyStatus load', false, e.message); }
    const LR = ctx.LR || { record: () => null, applyStatus: () => false };
    const newer = { id: 'S1', updatedAt: 2000, layoutRev: 4, previewAt: 1990, preview: 'P' }, older = { id: 'S1', updatedAt: 1000, layoutRev: 3, previewAt: 900, preview: 'P' };
    ctx.records.set('S1', newer);
    const got = LR.record(older, '1.1');
    ok('B1 record(): an older read changes nothing (record kept, no picture, no store write)', ctx.records.get('S1') === newer && !pics.includes('S1') && !keeps.includes('S1'), { pics, keeps });
    ok('B2 record() hands back the newer record the Library holds', got === newer);
    ctx.records.set('S2', { id: 'S2', updatedAt: 3000, layoutRev: 7 });
    const d1 = LR.applyStatus({ sheets: [{ id: 'S2', updatedAt: 2500, layoutRev: 6 }] }, ['S2'], false);
    ok('B3 applyStatus(): a live answer older than the record held is not taken (no redraw, the newer stays)', ctx.records.get('S2').layoutRev === 7 && d1 === false, { d1, rec: ctx.records.get('S2') });
    pics.length = 0;
    const d2 = LR.applyStatus({ sheets: [{ id: 'S2', updatedAt: 4000, layoutRev: 8, previewAt: 3990, preview: 'P2' }] }, ['S2'], false);
    ok('B4 applyStatus(): a newer answer is taken, and its picture is shown at once (picNow, not only through record())', d2 === true && ctx.records.get('S2').layoutRev === 8 && pics.includes('S2'), { d2, pics });
  }

  /* ── C · the Library's own drawing (LIVEPV) and picUrl ── */
  {
    const html = read('charm-nest-1.html');
    const pu = (html.match(/\nwindow\.picUrl = [^\n]*/) || [''])[0];
    const pctx = vm.createContext(Object.assign({}, BASE, { cors: u => u })); pctx.window = pctx;
    try { vm.runInContext(pu, pctx); } catch (e) { ok('C0 picUrl load', false, e.message); }
    const u1 = pctx.picUrl ? pctx.picUrl({ preview: 'https://x/p.png', previewRev: 3 }) : '', u2 = pctx.picUrl ? pctx.picUrl({ preview: 'https://x/p.png', previewAt: 1234, previewRev: 3 }) : '';
    ok('C1 picUrl: a picture with only its layout revision is versioned too (?v=r3); its time when it has one', /[?&]v=r3$/.test(u1) && /[?&]v=1234$/.test(u2), { u1, u2 });
    const live = between(html, '(function livePreviews() {', 'window.LivePreview = { tick, forget: id => id ? memo.delete(id) : memo.clear() };\n})();');
    const St = freshStore(); await St.init({ world: 'production' });
    const imgs = [], pages = [], lr = new Map();
    const img = id => { const o = { dataset: { sheetPreview: id }, isConnected: true, offsetParent: {}, _src: 'https://cloud/' + id + '.png?v=1', getAttribute(n) { return n === 'src' ? this._src : null; }, removeAttribute() {} }; Object.defineProperty(o, 'src', { get() { return this._src; }, set(v) { this._src = v; } }); imgs.push(o); return o; };
    const ctx = vm.createContext(Object.assign({}, BASE, {
      document: { querySelectorAll: s => (s === 'img[data-sheet-preview]' ? imgs : []), createElement: () => ({ width: 0, height: 0, toDataURL: () => 'data:image/png;local' }), body: {}, addEventListener() {}, hidden: false },
      S: { library: { rows: [] } }, allSheets: () => pages, stockFor: () => ({ wPt: 100, hPt: 100 }), paintPreview: () => {}, shippedOf: () => ({}),
      picUrl: r => r.preview + '?v=' + (r.previewAt || 'r' + r.previewRev), setInterval: () => 0, requestAnimationFrame: () => 0, MutationObserver: class { observe() {} } }));
    ctx.window = ctx; ctx.CharmNestStore = St; ctx.LaserReview = { recOf: id => lr.get(id) || null };
    try { vm.runInContext(live, ctx); } catch (e) { ok('C0 LIVEPV load', false, e.message); }
    const pl = [{ id: 'c1', xPt: 1, yPt: 1, angle: 0 }];
    pages.push({ sheetId: 'A', layoutRev: 3, contentRev: 0, placements: pl, charms: [], status: 'complete' }); lr.set('A', { id: 'A', layoutRev: 4, previewRev: 4, previewAt: 900, preview: 'https://cloud/A.png', updatedAt: 1000 });
    pages.push({ sheetId: 'B', layoutRev: 4, contentRev: 0, placements: pl, charms: [], status: 'complete' }); lr.set('B', { id: 'B', layoutRev: 4, previewRev: 4, previewAt: 900, preview: 'https://cloud/B.png', updatedAt: 1000 });
    pages.push({ sheetId: 'C', layoutRev: 4, contentRev: 1, cloudSeen: { layoutRev: 4, contentRev: 2 }, placements: pl, charms: [], status: 'complete' }); lr.set('C', { id: 'C', layoutRev: 4, previewRev: 4, previewAt: 950, preview: 'https://cloud/C.png', updatedAt: 1000 });
    pages.push({ sheetId: 'D', layoutRev: null, placements: pl, charms: [], status: 'complete' }); lr.set('D', { id: 'D', layoutRev: 2, previewAt: 700, preview: 'https://cloud/D.png', updatedAt: 1000 });
    pages.push({ sheetId: 'E', layoutRev: null, placements: pl, charms: [], status: 'nesting' }); lr.set('E', { id: 'E', layoutRev: 2, previewAt: 700, preview: 'https://cloud/E.png', updatedAt: 1000 });
    pages.push({ sheetId: 'F', layoutRev: 1, placements: pl, charms: [], status: 'complete' });
    const iA = img('A'), iB = img('B'), iC = img('C'), iD = img('D'), iE = img('E'), iF = img('F');
    iA._src = 'data:image/png;stale'; iA.dataset.livePreview = '1';
    try { ctx.LivePreview.tick(); } catch (e) { ok('C0 LIVEPV tick', false, e.message); }
    ok('C2 a page whose layout is behind the cloud\'s: the cloud\'s picture under its version, never the stale local drawing', iA.src === 'https://cloud/A.png?v=900' && !iA.dataset.livePreview, { src: iA.src, live: iA.dataset.livePreview });
    ok('C3 a page as new as the cloud: its own live drawing, as before', iB.src === 'data:image/png;local' && iB.dataset.livePreview === '1', iB.src);
    ok('C4 a hand move elsewhere (content revision ahead, layout the same): the cloud\'s picture', iC.src === 'https://cloud/C.png?v=950', iC.src);
    ok('C5 a copy that never matched the cloud and is not this page\'s own work: the cloud\'s picture; its own nest in progress: drawn by itself', iD.src === 'https://cloud/D.png?v=700' && iE.src === 'data:image/png;local', { d: iD.src, e: iE.src });
    ok('C6 no cloud record known: drawn by itself as before', iF.src === 'data:image/png;local', iF.src);
  }

  /* ── D · the sheet window's follow ── */
  {
    const src = read('charm-nest-sheetwin.js');
    const block = between(src, '  const backSig = r =>', '  setInterval(follow, 2000);');
    const calls = [], opened = [];
    const ctx = vm.createContext(Object.assign({}, BASE, { setInterval: () => 0 }));
    ctx.window = ctx; ctx.document = { hidden: false };
    ctx.W = { dlg: { open: true }, id: 'S1', token: 1, rec: { id: 'S1', poolIds: ['p1'], orders: ['1'], setId: 'T1', layoutRev: 3, previewAt: 100, previewRev: 3 }, going: new Set(), landing: [] };
    ctx.MC = { on: false, edits: new Map(), from: new Map() }; ctx.HOP = { pending: 0 };
    ctx.FOLLOW = { id: 'S1', revs: { 's:S1': '1.1' }, busy: false, fails: 0, at: Date.now(), moved: Date.now(), touch: Date.now() };
    ctx.followGap = () => 600000; ctx.tryDo = f => { try { return f ? f() : null; } catch (_) { return null; } };
    ctx.followHook = function () {}; ctx.mcChanged = (a, b) => !a || !b || a.cxPt !== b.cxPt;
    let answer = null; ctx.api = async (fn, body) => { calls.push(body); return answer; };
    ctx.open = async id => { opened.push(id); ctx.W.rec = Object.assign({}, ctx.W.rec, answer.sheets[0]); };
    try { vm.runInContext(block + '\nthis.SW = { follow, memberSig };', ctx); } catch (e) { ok('D0 the follow loads', false, e.message); }
    const SW = ctx.SW || { follow: async () => {} };
    answer = { sheets: [{ id: 'S1', poolIds: ['p1'], orders: ['1'], setId: 'T1', layoutRev: 4, previewAt: 100, previewRev: 3 }], revs: { 's:S1': '2.1' } };
    await SW.follow(true);
    ok('D1 a new layout (Change SKU, a new drawing: the same pieces and orders) reads the window again', calls.length === 1 && opened.length === 1, { calls: calls.length, opened });
    answer = { sheets: [{ id: 'S1', poolIds: ['p1'], orders: ['1'], setId: 'T1', layoutRev: 4, previewAt: 200, previewRev: 4 }], revs: { 's:S1': '3.1' } };
    await SW.follow(true);
    ok('D2 a picture drawn again (previewAt, previewRev) reads it again', opened.length === 2, opened);
    calls.length = 0; opened.length = 0; ctx.FOLLOW.at = Date.now();
    ctx.W.hand = true; await SW.follow(true);
    ok('D3 named while a charm is in hand: nothing asked now', calls.length === 0);
    ctx.W.hand = false; answer = { sheets: [{ id: 'S1', poolIds: ['p1', 'p2'], orders: ['1'], setId: 'T1', layoutRev: 5, previewAt: 200, previewRev: 4 }], revs: { 's:S1': '4.1' } };
    await SW.follow();   // (the 2 s beat, the feed live: its gap is ten minutes)
    ok('D4 ... and read at the next beat once the hand is free, not at the ten-minute net', calls.length === 1 && opened.length === 1, { calls: calls.length, opened });
    calls.length = 0; opened.length = 0; ctx.FOLLOW.at = Date.now();
    ctx.MC.on = true; answer = { sheets: [{ id: 'S1', poolIds: ['p1', 'p2'], orders: ['1'], setId: 'T1', layoutRev: 6, previewAt: 200, previewRev: 4 }], revs: { 's:S1': '5.1' } };
    await SW.follow(true);
    ok('D5 Manual Control on, no gesture and no move made: the change is read now', calls.length === 1 && opened.length === 1, { calls: calls.length });
    calls.length = 0; opened.length = 0; ctx.FOLLOW.at = Date.now();
    ctx.MC.drag = { moved: true }; await SW.follow(true);
    ok('D6 mid Manual Control drag: nothing read under the hand', calls.length === 0);
    ctx.MC.drag = null; answer = { sheets: [{ id: 'S1', poolIds: ['p1', 'p2'], orders: ['1'], setId: 'T1', layoutRev: 7, previewAt: 200, previewRev: 4 }], revs: { 's:S1': '6.1' } };
    await SW.follow();
    ok('D7 ... and read as soon as the drag ends', calls.length === 1 && opened.length === 1, { calls: calls.length });
  }

  /* ── E · the changes feed's race ── */
  {
    const src = read('charm-nest-sync-worker.js');
    class BC { constructor() {} postMessage() {} close() {} }
    const ctx = vm.createContext(Object.assign({}, BASE, { BroadcastChannel: BC, navigator: { onLine: true }, setInterval: () => 0, clearInterval: () => {} }));
    ctx.self = ctx;
    vm.runInContext(src, ctx);
    const Y = ctx.CNSync; let n = 0, gate = null; const asked = [];
    const api = async (fn, body) => { if (body.op !== 'getChanges') return {}; asked.push(Date.now()); n++; if (n === 1) return { n: '1.0.0.0' }; return new Promise(res => { gate = res; }); };
    Y.start({ world: 'production', api, enabled: () => true });
    await sleep(20);
    let since = 0; const reads = [];
    Y.follow({ name: 't', kinds: 's', wild: '', active: () => true, since: () => since, wants: () => true, read: (why, keys) => reads.push([why, keys.slice()]) });
    Y.poke(); for (let i = 0; i < 100 && !gate; i++) await sleep(20);
    const q = asked[asked.length - 1]; await sleep(15);
    since = Date.now(); const t = Date.now();   // (the surface's own read began here: after the question left, before the write it names)
    await sleep(15); const g = gate; gate = null; g({ n: '2.0.0.0', keys: ['s:abc'] }); await sleep(10);
    ok('E1 a write named by an answer is read by a surface whose own read began while the question was out (it had not seen it)', reads.some(r => r[1].includes('s:abc')), { reads, q, since });
    ok('E2 quietSince: a list read while the question was out is not "quiet" once the answer names a sheet', Y.quietSince(t, 's') === false);
    ok('E3 newsAt: the time this tab heard of the change (FRESHREAD)', typeof Y.newsAt === 'function' && Y.newsAt() >= t, typeof Y.newsAt === 'function' ? Y.newsAt() - t : 'none');
    reads.length = 0; since = Date.now() + 60000;   // (a surface whose read began after the answer came back holds the change)
    Y.poke(); for (let i = 0; i < 100 && !gate; i++) await sleep(20);
    gate({ n: '3.0.0.0', keys: ['s:abc'] }); await sleep(10);
    ok('E4 a surface whose own read began after the answer came back is spared a read (no new reads for nothing)', reads.length === 0, reads);
    Y.stop();
  }

  /* ── F · the Nest cards' cloud sheets ── */
  {
    const src = read('charm-nest-cloud-sheets.js');
    const pages = [], rebuilt = [], gets = [];
    const recs = {};
    const ctx = vm.createContext(Object.assign({}, BASE, { allSheets: () => pages }));
    ctx.self = ctx;
    ctx.CN = { api: async (fn, body) => { if (body.op === 'getSheet') { gets.push(body.id); return { sheet: JSON.parse(JSON.stringify(recs[body.id])), rev: '9.' + gets.length, pools: [] }; } return {}; }, agent() {}, renderCard() {} };
    ctx.Recall = { rebuild: async (p, o) => { rebuilt.push(p.sheetId); p.placements = JSON.parse(JSON.stringify(o.answer.placements)); }, piecesOf: async () => [] };
    vm.runInContext(src, ctx);
    const CS = ctx.CharmNestCloudSheets;
    const at = (x, y) => [{ id: 'c1', cxPt: x, cyPt: y, angle: 0 }];
    pages.push({ sheetId: 'A', layoutRev: 3, contentRev: 1, placements: at(10, 10), charms: [], status: 'complete', followed: true }); recs.A = { id: 'A', layoutRev: 3, contentRev: 2, placements: at(40, 10) };
    pages.push({ sheetId: 'B', layoutRev: 3, contentRev: 1, placements: at(10, 10), charms: [], status: 'complete', followed: true }); recs.B = { id: 'B', layoutRev: 3, contentRev: 2, placements: at(10, 10) };
    pages.push({ sheetId: 'C', layoutRev: null, _parted: { layoutRev: 2, contentRev: 0 }, placements: at(10, 10), charms: [], status: 'complete', followed: true }); recs.C = { id: 'C', layoutRev: 3, contentRev: 0, placements: at(50, 50) };
    pages.push({ sheetId: 'D', layoutRev: null, placements: at(10, 10), charms: [], status: 'complete' }); recs.D = { id: 'D', layoutRev: 6, contentRev: 2, placements: at(10, 10) };
    pages.push({ sheetId: 'E', layoutRev: 3, contentRev: 0, _manualHold: true, placements: at(10, 10), charms: [], status: 'complete', followed: true }); recs.E = { id: 'E', layoutRev: 4, contentRev: 0, placements: at(20, 10) };
    await CS._changed(['s:A', 's:B', 's:C', 's:D', 's:E']);
    const P = id => pages.find(p => p.sheetId === id);
    ok('F1 a hand move on another computer (content revision up, layout the same, other places): the page is rebuilt from the cloud', rebuilt.includes('A') && P('A').contentRev === 2, { rebuilt, a: P('A').contentRev });
    ok('F2 the same places already (its own move): no rebuild, the content revision is learned', !rebuilt.includes('B') && P('B').contentRev === 2, { b: P('B').contentRev });
    ok('F3 a followed copy that parted from the cloud, which moved after: rebuilt from the cloud', rebuilt.includes('C') && P('C').layoutRev === 3, { rebuilt });
    ok('F4 a copy that never matched but has the cloud\'s places now: its revisions are learned (it follows from now on)', P('D').layoutRev === 6 && P('D').contentRev === 2 && !rebuilt.includes('D'), { d: [P('D').layoutRev, P('D').contentRev] });
    ok('F5 a sheet Manual Control holds: not read, not rebuilt under the hand (read once it is let go)', !gets.includes('E') && !rebuilt.includes('E'), { gets });
    ok('F6 the newest cloud revisions read are kept on the page (LIVEPV compares with them)', P('B').cloudSeen && P('B').cloudSeen.contentRev === 2, P('B').cloudSeen);
  }

  /* ── G · run-follow ── */
  {
    const src = read('charm-nest-run-follow.js');
    let gets = 0, gate = null;
    const ctx = vm.createContext(Object.assign({}, BASE));
    ctx.self = ctx;
    ctx.CN = { api: async (fn, body) => { if (body.op === 'runList') return { runs: [{ runId: 'R1', status: 'running' }] }; if (body.op === 'runGet') { gets++; if (gets === 1) return new Promise(r => { gate = r; }); return { run: { lines: {}, orders: [], sheets: {} } }; } return {}; } };
    vm.runInContext(src, ctx);
    const F = ctx.CharmNestRunFollow;
    const first = F.now('start'); for (let i = 0; i < 50 && !gate; i++) await sleep(5);
    await F.now('keys');   // (r:<run> named while the run is being read)
    gate({ run: { lines: {}, orders: [], sheets: {} } }); await first; await sleep(30);
    ok('G1 a run named while it was being read is read once more right after (not left to the five-minute net)', gets === 2, { gets });
  }

  /* ── H · SHEETBUSY ── */
  {
    const html = read('charm-nest-1.html');
    const sb = between(html, '(function sheetBusy() {', 'window.CNSheetBusy = { see, of, why: busyWhy, decorate, soon };\n})();');
    class El {
      constructor(tag, cls) { this.tagName = tag; this.className = cls || ''; this.children = []; this.parentNode = null; this.dataset = {}; this.attrs = {}; this.title = ''; this.innerHTML = ''; this.id = ''; }
      get classList() { const s = this; return { contains: c => s.className.split(/\s+/).includes(c), add: c => { if (!s.className.split(/\s+/).includes(c)) s.className += ' ' + c; } }; }
      get parentElement() { return this.parentNode; }
      appendChild(c) { c.remove(); c.parentNode = this; this.children.push(c); return c; }
      insertBefore(c, ref) { c.remove(); c.parentNode = this; const i = this.children.indexOf(ref); if (i < 0) this.children.push(c); else this.children.splice(i, 0, c); return c; }
      remove() { if (this.parentNode) { const a = this.parentNode.children, i = a.indexOf(this); if (i >= 0) a.splice(i, 1); this.parentNode = null; } }
      setAttribute(k, v) { this.attrs[k] = String(v); } getAttribute(k) { return this.attrs[k] == null ? null : this.attrs[k]; }
      closest(sel) { const c = sel.replace(/^\./, ''); let n = this; while (n) { if (n.classList.contains(c)) return n; n = n.parentNode; } return null; }
      querySelector(sel) { const m = /^:scope > \.(\S+)$/.exec(sel); return m ? this.children.find(c => c.classList.contains(m[1])) || null : null; }
    }
    const body = new El('body'), head = new El('head');
    const card = body.appendChild(new El('div', 'libCard')), pic = card.appendChild(new El('img', 'pv')), meta = card.appendChild(new El('div', 'm')); pic.dataset.sheetPreview = 'A';
    const swHead = body.appendChild(new El('div', 'swHeadR')), swState = swHead.appendChild(new El('span', 'swState'));
    const chip = body.appendChild(new El('button', 'owShChip')); chip.dataset.sheet = 'A';
    const document = { body, head, createElement: t => new El(t), getElementById: id => head.children.find(c => c.id === id) || null, addEventListener() {},
      querySelectorAll: s => (s === 'img[data-sheet-preview]' ? [pic] : s === '.owShChip[data-sheet]' ? [chip] : []), querySelector: s => (s === 'dialog.sheetWin .swHeadR' ? swHead : s === '.cnSheetBusy' ? null : null) };
    const St = freshStore(); await St.init({ world: 'production' });
    const ctx = vm.createContext(Object.assign({}, BASE, { document, MutationObserver: class { observe() {} } }));
    ctx.window = ctx; ctx.CharmNestStore = St; ctx.SheetWin = { isOpen: () => true, current: () => 'A' };
    try { vm.runInContext(sb, ctx); } catch (e) { ok('H0 SHEETBUSY loads', false, e && e.message); }
    const B = ctx.CNSheetBusy || { why: () => '', decorate() {} }, now = Date.now();
    ok('H1 no busy field on the record (a cloud older than these marks): not busy', B.why({ id: 'x', saving: false, layoutRev: 3, updatedAt: now }) === '');
    ok('H2 the reasons: a nest queued / running, charms to place, files owed, saving, the picture behind its layout',
      B.why({ nestJob: { status: 'running', at: now - 1000 } }) === 'nesting' && B.why({ nestJob: { status: 'queued', at: now } }) === 'nesting' && B.why({ nestOwed: now - 5000 }) === 'placing' && B.why({ filesOwed: now - 5000 }) === 'writing files'
      && B.why({ saving: true, updatedAt: now }) === 'writing files' && B.why({ previewRev: 3, layoutRev: 4, updatedAt: now }) === 'writing files', [B.why({ nestJob: { status: 'running', at: now - 1000 } }), B.why({ nestOwed: now - 5000 })]);
    ok('H3 not busy: a finished nest, a mark older than an hour, a sheet cut or archived', B.why({ nestJob: { status: 'done', at: now } }) === '' && B.why({ filesOwed: now - 2 * 3600000 }) === '' && B.why({ filesOwed: now, laserDoneAt: now }) === '' && B.why({ filesOwed: now, archived: true }) === '');
    const badge = h => h.children.find(c => c.classList.contains('cnSheetBusy'));
    St.put('sheets', 'A', { id: 'A', nestJob: { status: 'running', at: now }, layoutRev: 4, previewRev: 4, updatedAt: now }, '10.1');
    B.decorate();
    ok('H4 the Library card says "Updating…" with its spinner under the picture, the reason on hover', !!badge(meta) && /Updating…/.test(badge(meta).innerHTML) && /owSpin/.test(badge(meta).innerHTML) && /nesting/.test(badge(meta).title), badge(meta) && badge(meta).title);
    ok('H5 the sheet window\'s header says it too, before its state', !!badge(swHead) && swHead.children.indexOf(badge(swHead)) < swHead.children.indexOf(swState));
    ok('H6 and the order window\'s sheet chip', !!badge(chip));
    St.put('sheets', 'A', { id: 'A', nestJob: { status: 'done', at: now + 5 }, layoutRev: 4, previewRev: 4, updatedAt: now + 10 }, '11.1');
    B.decorate();
    ok('H7 the finished record arrives (the feed): every spinner goes by itself', !badge(meta) && !badge(swHead) && !badge(chip));
    St.put('sheets', 'A', { id: 'A', nestJob: { status: 'running', at: now }, layoutRev: 4, previewRev: 4, updatedAt: now }, '10.1');
    if (B.see) B.see({ id: 'A', nestJob: { status: 'running', at: now }, layoutRev: 4, previewRev: 4, updatedAt: now - 1 });
    B.decorate();
    ok('H8 an older read landing after it never brings the spinner back (NEWERWINS)', !badge(meta));
  }

  /* ── I · the wiring a browser runs (code text) ── */
  {
    const html = read('charm-nest-1.html'), sw = read('charm-nest-sheetwin.js'), br = read('charm-nest-bridge.js'), cs = read('charm-nest-cloud-sheets.js');
    ok('I1 api(): a read is shared only with one that left after the last news (FRESHREAD)', /CharmNestStore\.shared\([^\n]*\{ after: apiNewsAt\(\) \}\)/.test(html) && /CNSync\.newsAt\(\)/.test(html));
    ok('I2 the order window\'s Sheet tab follows the feed for the sheets it keeps (no 60 s stale record)', /name: "order-sheet", kinds: "s"/.test(sw) && /function recFor\(id, o\) \{\s*tryDo\(orderFollow\)/.test(sw) && /OrderWin\.remade\(ids\)/.test(sw));
    ok('I3 the sheet window draws the newer copy held, never an older answer (open, the Sheet tab)', /St\.older\("sheetFull", id, r\.sheet, r\.rev \|\| ""\)/.test(sw) && /St\.older\("sheetFull", id, r\.sheet, r\.rev \|\| ""\) && St\.get\("sheetFull", id\) \|\| r\.sheet/.test(sw));
    ok('I4 the sheet window\'s picture: versioned, and replaced when the record\'s is newer (not only when empty)', /const pvLink = r =>/.test(sw) && !/!E\.pv\.getAttribute\("src"\)\) E\.pv\.src = cors\(rec\.outputs\.preview\.url\)/.test(sw) && /abs\(cur\) !== abs\(pv\)\) E\.pv\.src = pv/.test(sw));
    ok('I5 a sheet the Library holds nowhere, named by the feed, has the list read again (a new draft shows within seconds)', /if\(k\[0\]==='s' && id && id!=='\*' && liveOn\(\) && !records\.has\(id\)\)\{[^\n]*if\(!held\)wantReload\(\);\}/.test(br));
    ok('I6 SHEETBUSY is fed by every read of a sheet: the store, the Library\'s live read and record, the sheet window, the order window, the Nest cards',
      /St\.on\(ev => \{ if \(ev && ev\.rec && \(ev\.kind === "sheets" \|\| ev\.kind === "sheetFull"\)\) see\(ev\.rec, ev\.id\)/.test(html) && /CNSheetBusy\?\.see\?\.\(s,s\.id\)/.test(br) && /CNSheetBusy\?\.see\?\.\(s,id\)/.test(br)
      && /CNSheetBusy\.see\(live, id\)/.test(sw) && /CNSheetBusy\.see\(got, id\)/.test(sw) && /root\.CNSheetBusy\.see\(rec, id\)/.test(cs));
    ok('I7 no new timer: the sheet window\'s kept change is read by its existing 2 s beat; the spinner by the records and the page\'s own changes', /setInterval\(follow, 2000\)/.test(sw) && !/setInterval\([^)]*decorate/.test(between(html, '(function sheetBusy() {', '})();') || ''));
  }

  console.log(`\nrt1127-client: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
