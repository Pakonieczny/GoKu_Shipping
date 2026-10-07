// PS2: the nesting side of partial sheets (charm-nest-partial-nest.js, with the nestClaim / claimLate hooks of charm-nest-rose-ui.js and the page's
// overflowToNextSheet). OFFLINE: a jsdom window with a fake page (CN) and a fake cloud (stock documents with an owner), the REAL solver for the trial packs
// (a fake worker runs CharmNestSolver.solve in this process), the real rose-ui functions, and the page's real overflowToNextSheet lifted out of
// charm-nest-1.html. Needs jsdom (not in the repo: NODE_PATH=<a folder with node_modules/jsdom>); it says so and passes nothing when it is missing.
//   node tests/charm-nest/partial-nest.cjs
const assert = require('node:assert/strict'), fs = require('node:fs');
let JSDOM; try { ({ JSDOM } = require('jsdom')); } catch (_) { console.log('partial-nest: SKIPPED (jsdom is not installed)'); process.exit(0); }
const R = require('../../charm-nest-rose'), Solver = require('../../charm-nest-solver');
const MM = 25.4 / 72, SC = 6, W = 150, H = 60;   // a 150 x 60 pt sheet; pieces are 24 x 16 pt blocks
const block = id => { const w = 24 * SC, h = 16 * SC, bits = new Uint8Array(w * h).fill(1); return { id, order: id, w, h, scale: SC, bits, areaPt2: bits.length / (SC * SC), outline: { subpaths: [[['m', [0, 0]], ['l', [24, 0]], ['l', [24, 16]], ['l', [0, 16]], ['h']]] }, centerPt: [12, 8], members: [] }; };
// stock left of x = cut is already gone, on every row: what is left is the strip from cut to the sheet's right edge
const stockOf = (id, cut, rev = 1) => ({ id, metal: 'gold14k', wPt: W, hPt: H, revision: rev, owner: null, available: true, profileJson: JSON.stringify({ version: 1, wPt: W, hPt: H, axis: 'x', step: .5, values: Array(H / .5).fill(cut) }) });
const card = s => ({ id: s.id + '-' + s.revision, stockId: s.id, revision: s.revision, metal: s.metal, status: 'available', sheetWMm: W * MM, sheetHMm: H * MM, bboxMm: { x: s.cut * MM, y: 0, w: (W - s.cut) * MM, h: H * MM }, areaMm2: (W - s.cut) * H * MM * MM, outline: null });
const wait = ms => new Promise(r => setTimeout(r, ms));

function world(metal, policy) {
  const dom = new JSDOM('<body></body>', { url: 'https://example.test', runScripts: 'outside-only' }), w = dom.window;
  w.IntersectionObserver = class { observe() { } unobserve() { } };
  const log = { api: [], started: [], dirty: [], toasts: [], stocks: [] }, docs = new Map(), cuts = new Map();
  const add = (id, cut, rev) => { const s = stockOf(id, cut, rev); docs.set(id, s); cuts.set(id, cut); return card({ ...s, cut }); };
  const page = n => ({ metal, page: n, charms: [], placements: [], rejects: [], status: 'idle', persisted: false, persistedDone: true });
  const sh = page(1), S = { sheets: { [metal]: { pages: [sh], active: 0 } }, settings: { maxFill: .8, clearancePt: -.5, insetPt: 1.5, stock: {} }, cloud: { ok: true } };
  let cards = [], pol = policy || { mode: 'auto', wMm: 100, hMm: 50 };
  const api = async (name, b) => {
    log.api.push(b); const d = docs.get(b.stockId);
    if (b.op === 'roseGet') { if (!d) throw new Error('Sheet not found'); return { stock: { ...d }, cuts: [], more: false }; }
    if (b.op === 'roseRelease') {
      const own = [...docs.values()].find(x => x.id === b.stockId); if (!own || own.owner !== b.sheetId) throw new Error('This stock reservation changed');
      if (!own.revision && !own.profileJson) docs.delete(own.id); else { own.owner = null; own.available = true; } return { ok: true };
    }
    if (b.op === 'roseClaim') {
      const held = [...docs.values()].find(x => x.owner === b.sheetId);
      if (b.exact && held && held.id !== b.stockId && !b.swap) throw new Error('This sheet already holds another physical sheet. Give it back before choosing a partial sheet');
      let st = b.exact ? docs.get(b.stockId) : held || (b.stockId ? docs.get(b.stockId) : null);
      if (b.exact && !st) throw new Error('Partial sheet not found');
      if (!st && !b.fresh && !b.stockId) st = [...docs.values()].find(x => x.available && x.metal === b.metal && Math.abs(x.wPt - b.wPt) < .01 && Math.abs(x.hPt - b.hPt) < .01 && !x.owner);
      if (!st && b.onlyRemnant) return { stock: null, protectedJson: null };
      if (!st) { st = { id: 'rgs-new-' + docs.size, metal: b.metal, wPt: b.wPt, hPt: b.hPt, revision: 0, profileJson: null, owner: null, available: true }; docs.set(st.id, st); }
      if (st.owner && st.owner !== b.sheetId) throw new Error('This ' + b.metal + ' sheet is reserved for another layout');
      if (b.revision != null && st.revision !== b.revision) throw new Error('This remnant changed. Reload its history before nesting');
      // swap: what the sheet holds goes back in the same call, after every check has passed (a refused claim loses nothing)
      if (b.swap && held && held.id !== st.id) { if (!held.revision && !held.profileJson) docs.delete(held.id); else { held.owner = null; held.available = true; } }
      st.owner = b.sheetId; st.available = false; return { stock: { ...st }, protectedJson: null };
    }
    return {};
  };
  const pieces = (n, tag = 'p') => Array.from({ length: n }, (_, i) => block(tag + i));
  w.CharmNestRose = R; w.CharmNestSolver = Solver; w.Sets = { ofRun: () => [] };
  w.PartialSheets = { list: async () => ({ items: cards.filter(c => c.status === 'available' && (x.stale || !docs.get(c.stockId).owner)) }), policy: () => pol, changed() { },
    stocks: async ids => { log.stocks.push(ids.slice()); return Object.fromEntries(ids.map(id => { const m = /^(.+)-(\d+)$/.exec(id), d = m && docs.get(m[1]); return [id, d ? { stockId: d.id, revision: d.revision, wPt: d.wPt, hPt: d.hPt, profileJson: d.profileJson, metal: d.metal, current: d.revision === +m[2] } : { missing: true }]; })); } };
  const C = w.CN = {
    S, esc: s => String(s), uid: () => 'u', agent() { }, toast: (m, k) => log.toasts.push(m), inflatedArea: c => c.areaPt2, activeCharms: s => s.charms.filter(c => !c.excluded),
    stockFor: (m, s) => { const k = s && (s.roseStock || s.recalled && s.recalled.stock || s.keptStock || s.newStock); return k && k.wPt ? { wPt: k.wPt, hPt: k.hPt } : { wPt: W, hPt: H }; },
    pagesOf: m => S.sheets[m].pages, allSheets: () => Object.values(S.sheets).flatMap(x => x.pages), renderCard() { }, drawPreview() { }, api,
    addPage: m => { const p = page(Math.max(...S.sheets[m].pages.map(q => q.page)) + 1); S.sheets[m].pages.push(p); return p; },
    sheetDirty: s => { log.dirty.push(s); s.dirty = true; s.status = 'ready'; s.placements = []; s.rejects = []; delete s.rosePlan; },
    startNest: s => { log.started.push({ sheet: s, byHand: !!s._byHand }); },
    buildJob: s => { const st = C.stockFor(s.metal, s), items = C.activeCharms(s); return { sheet: { wPt: st.wPt, hPt: st.hPt, insetPt: 1.5, ...(s.roseStock && s.roseStock.profileJson ? { remnant: JSON.parse(s.roseStock.profileJson) } : {}) }, clearancePt: -.5, angles: [0, 90], fineRes: 2, coarseRes: .5, timeBudgetMs: 3000, maxFill: .8, maxTrials: 60, seed: 1, careful: true, block: true, pieces: items.map(c => ({ id: c.id, w: c.w, h: c.h, scale: c.scale, bits: c.bits, areaPt2: c.areaPt2, order: c.order || c.id, orderDate: 0, pinned: c.pinned || null })) }; },
    solverWorker: () => { const k = { onmessage: null, dead: false, terminate() { k.dead = true; }, postMessage(m) { if (m.type !== 'solve') return; Solver.solve(m.job, {}).then(r => { if (!k.dead && k.onmessage) k.onmessage({ data: { type: 'done', jobId: m.jobId, result: Solver.publicLayout(r) } }); }).catch(e => k.onmessage && k.onmessage({ data: { type: 'error', jobId: m.jobId, message: e.message } })); } }; return k; }
  };
  w.eval(`window.Session={schedule(){}};`);
  w.eval(fs.readFileSync('charm-nest-rose-ui.js', 'utf8'));
  w.eval(fs.readFileSync('charm-nest-partial-nest.js', 'utf8'));
  const x = { stale: false, w, C, sh, log, docs, S, add, pieces, setCards: c => { cards = c; }, setPolicy: p => { pol = p; }, page };
  return x;
}
const ids = list => list.map(c => c.id).sort().join();

(async () => {
  // ── 1. one partial that holds everything: the preview is a real trial, nothing is written; the commit re-nests all pieces on that outline ──
  {
    const x = world('gold14k'), { w, sh, log, docs } = x, PN = w.PartialNest, RS = w.RoseStock;
    const big = x.add('rgs-big', 20), small = x.add('rgs-small', 110);
    x.setCards([big, small]);
    sh.charms = x.pieces(8); sh.placements = sh.charms.map((c, i) => ({ id: c.id, cxPt: 30 + i * 3, cyPt: 20, angle: 0 })); sh.status = 'complete'; sh.sheetId = 'g14-s1';
    // the sheet is in use on a fresh (never cut) physical sheet
    docs.set('rgs-fresh', { id: 'rgs-fresh', metal: 'gold14k', wPt: W, hPt: H, revision: 0, profileJson: null, owner: 'g14-s1', available: false }); sh.roseStock = { ...docs.get('rgs-fresh') };
    assert.equal(PN.canSeat(sh).ok, true);
    log.api.length = 0;
    const steps = []; const pv = await PN.preview(sh, [big.id], { maxMs: 3000, onStep: s => steps.push(s.key) });
    assert(pv.ok && pv.fitsAll && pv.pieces === 8 && pv.links.length === 1 && pv.links[0].placed === 8 && pv.continues.n === 0, 'all 8 pieces fit on the big partial: ' + JSON.stringify({ ...pv, links: pv.links.map(l => ({ ...l, placements: undefined })) }));
    assert.match(pv.words, /^All 8 pieces fit on this partial sheet\.$/); assert.deepEqual(steps, ['check', 'trial', 'done']);
    assert.deepEqual(new Set(pv.links[0].pieceIds), new Set(sh.charms.map(c => c.id)), 'the trial placed every piece exactly once');
    assert.equal(log.api.length, 0, 'the preview calls nothing of its own: the partial\'s stock comes from PartialSheets.stocks, once'); assert.equal(log.stocks.length, 1);
    assert.equal(log.started.length, 0, 'the preview nests nothing'); assert.equal(docs.get('rgs-big').owner, null, 'the preview claims nothing');
    // commit
    log.api.length = 0; const before = sh.charms.slice(), seen = [];
    const r = await PN.seat(sh, [big.id], { preview: pv, onStep: s => seen.push(s.key) });
    assert(r.ok && r.started && r.moved === 8, JSON.stringify(r));
    assert.equal(log.api.map(b => b.op).join(), 'roseClaim,roseGet', 'ONE claim that swaps (the old sheet goes back inside it), then the history: ' + JSON.stringify(log.api.map(b => b.op)));
    const claim = log.api.find(b => b.op === 'roseClaim'); assert.equal(JSON.stringify([claim.stockId, claim.revision, claim.exact, claim.partialId, claim.nesting, claim.swap, claim.wPt, claim.hPt]), JSON.stringify(['rgs-big', 1, true, big.id, true, true, W, H]), 'one exact claim of the chosen partial, at its own size');
    assert.equal(docs.has('rgs-fresh'), false, 'the old, never-cut gold sheet was given back (the server deletes it)'); assert.equal(docs.get('rgs-big').owner, 'g14-s1');
    assert.equal(sh.roseStock.id, 'rgs-big'); assert.equal(sh.roseRevision, 1);
    assert(sh.charms.length === before.length && sh.charms.every((c, i) => c === before[i]), 'the same pieces, same order, none lost, none added'); assert(sh.charms.every(c => c.pinned == null));
    assert.equal(log.dirty.length, 1); assert.deepEqual(log.started.map(s => [s.sheet, s.byHand]), [[sh, true]], 're-nested once, by hand, as the Nest button does');
    assert.equal(seen.join(), 'check,claim,nest,done');
    assert.equal(PN.partialOf(sh), big.id); assert.equal(PN.chain(sh).length, 1);
    // choosing the partial it already sits on does nothing
    const same = await PN.seat(sh, [big.id]); assert(!same.ok && same.code === 'same', JSON.stringify(same));
  }

  // ── 2. a refused claim (another sheet took the partial a moment ago) changes nothing: the old sheet is held again, nothing is nested ──
  {
    const x = world('gold14k'), { w, sh, log, docs } = x, PN = w.PartialNest;
    const p = x.add('rgs-p', 20); x.setCards([p]); sh.charms = x.pieces(4); sh.sheetId = 'g14-s1'; sh.status = 'complete'; sh.placements = [{ id: 'p0', cxPt: 20, cyPt: 20, angle: 0 }];
    docs.set('rgs-old', { id: 'rgs-old', metal: 'gold14k', wPt: W, hPt: H, revision: 2, profileJson: stockOf('x', 5).profileJson, owner: 'g14-s1', available: false }); sh.roseStock = { ...docs.get('rgs-old') };
    x.stale = true; docs.get('rgs-p').owner = 'another-sheet'; docs.get('rgs-p').available = false;   // taken meanwhile; the page's list was a moment old
    const r = await PN.seat(sh, [p.id]);
    assert(!r.ok && r.code === 'taken' && /could not be reserved/.test(r.why), JSON.stringify(r));
    assert.equal(sh.roseStock.id, 'rgs-old', 'the sheet holds what it held'); assert.equal(docs.get('rgs-old').owner, 'g14-s1'); assert.equal(log.started.length, 0, 'nothing was nested'); assert.equal(sh.placements.length, 1, 'its layout is untouched');
  }

  // ── 3. several partials: filled one after the other, no limit; the chain grows through the page's own overflow; incoming pieces keep going down it ──
  {
    const x = world('gold14k'), { w, sh, log, docs, C } = x, PN = w.PartialNest;
    const cards = ['a', 'b', 'c', 'd', 'e', 'f'].map(k => x.add('rgs-' + k, 100));   // each takes about the same few pieces
    x.setCards(cards); sh.sheetId = 'g14-s1'; sh.status = 'complete';
    // how many one small partial holds: a trial of plenty of pieces on one
    sh.charms = x.pieces(30); const one = await PN.preview(sh, [cards[0].id], { maxMs: 2500, maxLinks: 1 });
    assert(one.ok && !one.fitsAll && one.links.length === 1 && one.links[0].placed >= 2, 'a small partial holds some: ' + one.words); const cap = one.links[0].placed;
    assert(one.continues.n === 30 - cap && /continue/.test(one.words), one.words); assert.equal(one.continues.next, 'partial', 'automatic: the next partial that fits is offered when more are available');
    // a chain of three, listed by the person, for just over two partials' worth of pieces
    const n = cap * 2 + 1; sh.charms = x.pieces(n);
    const pv = await PN.preview(sh, [cards[0].id, cards[1].id, cards[2].id], { maxMs: 2500 });
    assert(pv.ok && pv.links.length >= 2 && pv.links.length <= 3, pv.words);
    const placed = pv.links.flatMap(l => l.pieceIds); assert.equal(new Set(placed).size, placed.length, 'no piece is on two partials');
    assert.equal(placed.length + pv.continues.n, n, 'no piece is lost: ' + JSON.stringify([placed.length, pv.continues.n, n]));
    assert.equal(pv.fitsAll, pv.continues.n === 0); assert.equal(pv.links.map(l => l.partialId).join(), [cards[0], cards[1], cards[2]].slice(0, pv.links.length).map(c => c.id).join(), 'filled in the order the person listed them');
    assert(pv.links[0].placed >= pv.links[1].placed - 1, 'the first partial is filled before the second takes the remainder');
    // commit the chain, then drive the page's real overflow with each sheet's rejects (the sheet now holds plenty of pieces so each can pass some on)
    sh.charms = x.pieces(60, 'q');
    const r = await PN.seat(sh, [cards[0].id, cards[1].id, cards[2].id], { preview: pv }); assert(r.ok, JSON.stringify(r));
    assert.equal(sh._partialChain.length, 2, 'two partials wait for the pieces that do not fit');
    const src = fs.readFileSync('charm-nest-1.html', 'utf8'), a = src.indexOf('function overflowToNextSheet'), b = src.indexOf('function inflatedArea', a);
    const overflow = new Function('S', 'addPage', 'manualSheetClosed', 'computeSaturation', 'renderCard', 'agent', 'toast', 'renderRail', 'updateTopSub', 'startNest', 'orderSummary', 'labelOf', 'RunCtl', 'PartialNest', 'window',
      src.slice(a, b) + '\nreturn overflowToNextSheet;')(C.S, C.addPage, () => false, () => { }, () => { }, () => { }, () => { }, () => { }, () => { }, C.startNest, () => ({ text: '' }), m => m, null, w.PartialNest, w);
    const rejectLast = (page, k) => { const move = page.charms.slice(-k); page.placements = page.charms.filter(c => !move.includes(c)).map(c => ({ id: c.id, cxPt: 1, cyPt: 1, angle: 0 })); page.rejects = move.map(c => c.id); page.verification = { ok: true }; return overflow(page, false); };
    log.started.length = 0; const pg2 = rejectLast(sh, 30);
    assert(pg2 && pg2 !== sh && pg2.roseStock.id === 'rgs-b' && pg2._partialPending && pg2.charms.length === 30, 'the pieces that do not fit go to a NEW page made for the next partial');
    assert.deepEqual(log.started.map(s => s.sheet), [pg2], 'that page nests at once'); assert.equal(sh.charms.length, 30, 'moved whole, never copied');
    await w.RoseStock.nestClaim(pg2); assert.equal(pg2._partialPending, false); assert.equal(docs.get('rgs-b').owner, pg2.sheetId, 'the page claims exactly its partial when it nests');
    const claimB = log.api.filter(q => q.op === 'roseClaim').pop(); assert.equal(JSON.stringify([claimB.stockId, claimB.exact, claimB.partialId]), JSON.stringify(['rgs-b', true, 'rgs-b-1']));
    const pg3 = rejectLast(pg2, 25); assert(pg3 && pg3.roseStock.id === 'rgs-c' && pg3 !== pg2, 'then the next partial of the chain');
    await w.RoseStock.nestClaim(pg3);
    // the listed partials are used up: automatic takes the best fit of the others, again and again (no limit), each page claimed when it nests
    let cur = pg3, made = [sh, pg2, pg3];
    for (let i = 0; i < 3; i++) { const nx = rejectLast(cur, 20 - 5 * i); assert(nx && !made.includes(nx) && nx._partialId && nx.roseStock, `automatic continuation ${i + 1}`); await w.RoseStock.nestClaim(nx); made.push(nx); cur = nx; }
    assert.equal(made.map(p => p.roseStock.id).join(), 'rgs-a,rgs-b,rgs-c,rgs-d,rgs-e,rgs-f', 'six partial sheets in one chain');
    assert.equal(new Set(made.map(p => p.roseStock.id)).size, 6, 'no partial is used twice');
    const none = rejectLast(cur, 5); assert(!none || !none.roseStock || none._partialId == null, 'partials run out: the page does what it does today (a new sheet), no chain page is invented');
    // incoming pieces: the open page first (the earlier ones stay filled), what does not fit goes to the next page of the chain, not to a new one
    const again = overflow(sh, false); assert.equal(again, null, 'nothing left over: nothing moves');
    sh.rejects = [sh.charms[0].id]; sh.placements = sh.placements.filter(p => p.id !== sh.charms[0].id); const ov = overflow(sh, false);
    assert.equal(ov, pg2, 'a later overflow from the first sheet goes to the chain\'s next sheet (it is still open), so the chain only grows at its end');
  }

  // ── 4. the policy: 'new' never claims a partial by itself; 'auto' is today's nester, unchanged ──
  for (const metal of ['rose', 'gold14k', 'gold10k']) {
    const auto = world(metal), neu = world(metal, { mode: 'new', wMm: 120, hMm: 60 });
    for (const x of [auto, neu]) { x.leftover = x.add('rgs-left', 100); x.docs.get('rgs-left').metal = metal; x.sh.charms = x.pieces(2); x.setCards([x.leftover]); }
    // auto
    await auto.w.RoseStock.nestClaim(auto.sh);
    const ca = auto.log.api.find(b => b.op === 'roseClaim');
    assert.equal(!!ca.fresh, false); assert.equal(!!ca.onlyRemnant, metal !== 'rose', metal + ': auto = today (Rose Gold claims the first available stock; 10K/14K a leftover that fits, nothing else)'); assert.equal(auto.sh.roseStock && auto.sh.roseStock.id, 'rgs-left');
    // new
    await neu.w.RoseStock.nestClaim(neu.sh);
    assert.equal(neu.docs.get('rgs-left').owner, null, metal + ': policy new never takes the partial by itself');
    if (metal === 'rose') { const cn = neu.log.api.find(b => b.op === 'roseClaim'); assert(cn.fresh === true && !cn.stockId && cn.wPt === 120 / MM && cn.hPt === 60 / MM, 'Rose Gold takes a brand new physical sheet at the size the person set: ' + JSON.stringify(cn)); assert(neu.sh.roseStock.id.startsWith('rgs-new'), 'a new sheet'); }
    else { assert.equal(neu.log.api.length, 0, metal + ': a gold sheet takes no physical sheet and asks nothing'); assert(!neu.sh.roseStock && Math.abs(neu.sh.newStock.wPt - 120 / MM) < 1e-9 && Math.abs(neu.sh.newStock.hPt - 60 / MM) < 1e-9, 'the sheet is made at the stipulated size'); assert.equal(neu.C.stockFor(metal, neu.sh).wPt, 120 / MM); }
    // a sheet already nested keeps the size it has; a partial is used only when the person picks it
    const was = neu.sh.sheetId; neu.sh.sheetId = 'already'; neu.sh.newStock = null; assert.equal(neu.w.PartialNest.newSheet(neu.sh), false, 'a sheet that was nested keeps its size');
    neu.sh.sheetId = was; neu.sh.roseStock = null; neu.sh.status = 'complete'; neu.sh.placements = []; neu.sh.charms = neu.pieces(2);
    const none = await neu.w.PartialNest.preview(neu.sh, []); assert(!none.ok && none.code === 'choose' && /Choose a partial sheet/.test(none.words), 'policy new: no partial unless the person lists one');
    const listed = await neu.w.PartialNest.preview(neu.sh, [neu.leftover.id], { maxMs: 2000 }); assert(listed.ok && listed.fitsAll, 'a partial the person picked is used');
  }

  // ── 5. a recorded cut is permanent: refused in words, nothing is called ──
  {
    const x = world('rose'), { w, sh, log } = x, PN = w.PartialNest; const p = x.add('rgs-p', 20); x.docs.get('rgs-p').metal = 'rose'; x.setCards([p]); sh.charms = x.pieces(3);
    for (const [patch, code] of [[{ roseCutAt: 1760000000000 }, 'cut'], [{ laserDoneAt: 1760000000000 }, 'laser'], [{ processReady: true }, 'laser'], [{ recalled: {} }, 'recalled'], [{ status: 'nesting' }, 'busy'], [{ rosePlan: { lines: [] } }, 'line']]) {
      Object.assign(sh, { roseCutAt: null, laserDoneAt: null, processReady: false, recalled: null, status: 'complete', rosePlan: null }, patch); log.api.length = 0;
      const can = PN.canSeat(sh); assert(!can.ok && can.code === code, code + ': ' + JSON.stringify(can));
      const pv = await PN.preview(sh, [p.id]); assert(!pv.ok && pv.code === code && pv.fitsAll === false, JSON.stringify(pv));
      const r = await PN.seat(sh, [p.id]); assert(!r.ok && r.code === code); assert.equal(log.api.length, 0, code + ': nothing is read, claimed or released'); assert.equal(log.started.length, 0);
      if (code === 'cut') assert(/recorded cut is permanent/.test(pv.words) && /keeps its metal/.test(pv.words), pv.words);
    }
    Object.assign(sh, { roseCutAt: null, laserDoneAt: null, recalled: null, status: 'complete', rosePlan: null });
    w.Sets.ofRun = () => [{ committedAt: 1, sheetIds: [sh.sheetId] }]; sh.sheetId = 'rose-x'; assert.equal(PN.canSeat(sh).code, 'set', 'a committed set'); w.Sets.ofRun = () => [];
    assert.equal(PN.canSeat({ ...sh, metal: 'gold' }).code, 'metal', 'GF has no partial sheets');
  }

  // ── 6. GF1's open problem: claimLate never reserves a physical sheet for an approved gold sheet; a sheet not yet approved still is claimed ──
  for (const metal of ['gold14k', 'gold10k']) {
    const x = world(metal), { w, sh, log } = x, RS = w.RoseStock;
    Object.assign(sh, { sheetId: metal + '-s1', setId: 'set-1', draft: false, runId: 'run-1', persistedDone: true, verification: { ok: true }, dirty: false, status: 'complete', charms: x.pieces(2), placements: [{ id: 'p0', cxPt: 20, cyPt: 20, angle: 0 }] });
    for (const [label, patch, sets] of [['a committed set', {}, [{ committedAt: 5, sheetIds: [sh.sheetId] }]], ['a ready seal on the sheet', { processReady: true }, []], ['a ready seal on its set', {}, [{ processReady: true, sheetIds: [sh.sheetId] }]], ['laser done', { laserDoneAt: 7 }, []], ['laser pending', { laserSetPending: true }, []]]) {
      Object.assign(sh, { processReady: false, laserDoneAt: null, laserSetPending: false }, patch); w.Sets.ofRun = () => sets; log.api.length = 0;
      await RS.claimLate(sh); assert(!sh.roseStock && log.api.length === 0, metal + ': ' + label + ' is never claimed late: ' + JSON.stringify(log.api.map(b => b.op)));
    }
    Object.assign(sh, { processReady: false, laserDoneAt: null, laserSetPending: false }); w.Sets.ofRun = () => [{ sheetIds: [sh.sheetId] }]; log.api.length = 0;   // in a set that is not committed or sealed: Paul's rule, it is claimed and needs Cut Sheet
    await RS.claimLate(sh); assert(sh.roseStock && log.api.some(b => b.op === 'roseClaim' && b.fresh), metal + ': a sheet not yet approved is still claimed');
  }
  // ── 7. the face the panel reads (window.PartialEngine): the shapes PS1 asked for, the commit uses the chain that was previewed ──
  {
    const x = world('gold14k'), { w, sh, log, docs } = x, E = w.PartialEngine; delete w.PartialSheets.stocks;   // (the fallback: one roseGet with noCuts per partial)
    const a = x.add('rgs-a', 100), b = x.add('rgs-b', 100), c = x.add('rgs-c', 100); [a, b, c].forEach(k => { k.sourceSheet = '14K Sheet 1'; k.sourceSet = 'Set ' + k.id.slice(-3, -2); }); x.setCards([a, b, c]);
    sh.charms = x.pieces(9); sh.sheetId = 'g14-s1'; sh.status = 'complete';
    const fired = []; const off = E.on(e => fired.push(e.metal));
    const pv = await E.preview(sh, a.id);
    assert(pv.ok && pv.pieces === 9 && typeof pv.fitsAll === 'boolean' && pv.fits > 0 && pv.rest === 9 - pv.fits && pv.chain.length >= 2 && pv.chain[0].partialId === a.id, JSON.stringify(pv));
    assert.equal(pv.chain.map(r => r.fits).reduce((n, v) => n + v, 0) + (pv.then ? 1 : 0) > 0, true); assert.match(pv.chain[0].name, /14K Sheet 1 · Set/); assert(['new', 'wait', null].includes(pv.then));
    assert(log.api.length >= 1 && log.api.every(q => q.op === 'roseGet' && q.noCuts === true), 'without the page cache the trial reads each stock document alone, without its cuts: ' + JSON.stringify(log.api.map(q => q.op)));
    const bad = await E.preview({ ...sh, roseCutAt: 5 }, a.id); assert(!bad.ok && /permanent/.test(bad.reason), JSON.stringify(bad));
    const steps = []; log.api.length = 0;
    const done = await E.useOn(sh, a.id, { onStep: s => steps.push(s.key) });
    assert(done.ok && done.used[0] === a.id && done.used.join() === pv.chain.map(r => r.partialId).join() && done.placed === pv.fits && done.rest === 9 - pv.fits, JSON.stringify(done));
    assert.equal(steps.join(), 'start,claimed,nesting,done'); assert.equal(log.api.filter(q => q.op === 'roseClaim').length, 1, 'only the first partial is claimed now; the others when their pages nest');
    assert.equal(sh._partialChain.length, pv.chain.length - 1, 'the previewed chain is what waits for the overflow');
    assert(fired.includes('gold14k')); const ch = E.chain('gold14k'); assert.equal(ch.length, 1); assert.equal(ch[0].n, 1); assert.equal(ch[0].partialId, a.id); assert.equal(ch[0].pieces, 0); assert.equal(ch[0].sheetPage, 1); assert.equal(ch[0].full, false); off();
  }
  console.log('partial-nest OK: preview is a real trial pack that writes nothing; seat gives back, claims once and re-nests every piece; a refused claim changes nothing; chains of partials fill in order with no limit and incoming pieces follow; policy new never auto-claims; a recorded cut refuses; approved gold sheets are never claimed late');
})().catch(e => { console.error(e); process.exitCode = 1; });
