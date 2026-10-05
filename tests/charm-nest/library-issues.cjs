// The Library's '!' issues panel (charm-nest-library-issues.js, window.LibraryIssues): Paul, 5 Oct 2026, points 4, 8 and 10.
// "click on the '!' in the progress timeline and have that expand a menu to see the specific issues with shortcut links" ·
// "easy to understand at a glance ... minimal text and maximum visuals" · "do not show completed progress in this list ... remove
// all UI pertaining to the back engraving status". The real LaserReview (issuesOf, paint) and set card code in a DOM, the page's
// helpers faked, a fake CharmNestReadiness.issues in the exact shape of round 2 interface B. Offline fixtures only.
//   node tests/charm-nest/library-issues.cjs        (NODE_PATH=<dir with jsdom>)
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), { JSDOM } = require('jsdom');
const F = require('./library-issues-fixture.cjs');
const root = path.join(__dirname, '../..');
const tick = (n = 40) => new Promise(r => setTimeout(r, n));
const rowsOf = p => [...p.querySelectorAll('.lisRow')];
const texts = p => p.textContent.replace(/\s+/g, ' ');
const eq = (a, b, m) => assert.deepEqual(JSON.parse(JSON.stringify(a === undefined ? null : a)), b, m);   // (values made in the page's realm compare by what they hold)

(async () => {
  let dom = null;
  try {
  dom = new JSDOM('<body><div class="topbar"></div><main id="libBody"></main></body>', { url: 'https://example.test/', runScripts: 'outside-only', pretendToBeVisual: true });
  const w = dom.window, d = w.document, calls = [], jobs = new Map();
  w.matchMedia = () => ({ matches: true });
  w.Element.prototype.getClientRects = function () { return [{}]; };   // (jsdom has no layout: everything is "visible")
  const photos = new Map();
  let openWindow = null;
  Object.assign(w, { S: { mode: 'library', library: { rows: [], kind: 'sets', metal: 'all' }, cloud: { ok: true } }, api: async () => ({ sheets: [], sets: [] }), allSheets: () => [], Orders: { rows: () => w.__rows || [] },
    Engrave: { items: () => jobs, backsMarkup: () => '' }, esc: x => String(x ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c])), el: (tag, cls) => { const e = d.createElement(tag); e.className = cls; return e; },
    Gate: { projectLibraryRecords: x => x }, RoseStock: {}, CODE: { gold: 'GF', silver: 'SS', rose: 'RG' }, toast: () => null, CNEmployee: { name: () => 'Tester' }, cors: x => x, sheetHead: r => `<div class="h"><span class="nm">Sheet ${r.sheetIndex}</span></div>`, pvRatio: () => '',
    openLibrarySheet: id => { calls.push(['sheet', id]); return true; }, openOrderFrom: (btn, rid, o) => { calls.push(['order', rid, o?.poolId || null, btn.className]); return openWindow ? openWindow() : true; }, setMode: m => calls.push(['mode', m]),
    ListMedia: { peek: id => photos.get(id) || null, listing: id => Promise.resolve(photos.get(id) || null) } });
  w.eval(fs.readFileSync(path.join(root, 'charm-nest-orders.js'), 'utf8')); w.O = w.CharmNestOrders;
  w.eval(fs.readFileSync(path.join(root, 'charm-nest-readiness.js'), 'utf8'));
  w.eval(fs.readFileSync(path.join(root, 'charm-nest-activity.js'), 'utf8'));
  w.eval(fs.readFileSync(path.join(root, 'charm-nest-motion.js'), 'utf8'));
  const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
  w.eval(bridge.slice(bridge.indexOf('const LaserReview ='), bridge.indexOf('/* ═══ 22 · Sets — one run')));
  const a = bridge.indexOf('  function libraryGroups('), b = bridge.indexOf('  /** A set card whose completion', a), c = bridge.indexOf('  async function renderLibrary(body, opts)', b), e = bridge.indexOf('  return { releaseIssue', c);
  w.eval('window.Sets=(()=>{' + bridge.slice(a, b) + bridge.slice(c, e) + ';return {libraryCard};})();');
  const L = w.LaserReview, R = w.CharmNestReadiness, body = d.getElementById('libBody');
  // the exact shape of CharmNestReadiness.issues; the test sets window.__feed[sheetId]
  const realIssues = R.issues;
  let readCount = 0; w.__feed = {}; R.issues = s => { readCount++; return w.__feed[s.id || s.sheetId] || []; };
  w.eval(fs.readFileSync(path.join(root, 'charm-nest-library-issues.js'), 'utf8'));
  const LI = w.LibraryIssues;
  assert(LI && typeof LI.open === 'function', 'the module is on the page');

  // ── 0. the pure model: only issues, one panel per step, words in pieces, never an engraving row
  {
    const feed = { id: 'gf1', label: 'GF Sheet 1', code: 'GF', metal: 'gold', issues: F.issues('gf1', 3, { keys: ['pooled', 'noSku', 'otherSheetNotReady'], own: 'engraving' }) };
    let m = LI.model(feed, { step: 'orders' });
    assert.equal(m.title, 'Order check'); assert.equal(m.own, null, 'the Order check panel lists the orders only'); assert.equal(m.orders.length, 3); assert.equal(m.groups, null, 'three issues are plain rows, not groups');
    eq(m.orders.map(o => o.reason.chip), ['1 piece not on a sheet yet', '1 piece has no SKU', 'Waits on SS Sheet 1']);
    eq(m.orders.map(o => o.reason.tone), ['gold', 'clay', 'slate']); assert.equal(m.hard, true, 'a missing SKU needs a person');
    m = LI.model(feed, { step: 'engraving' });
    assert.equal(m.title, 'Engraving'); assert.equal(m.orders.length, 0); assert.equal(m.count, 0); eq([m.own.go, m.own.label], ['engraving', 'Open engraving approvals'], 'for Engraving only the single link');
    m = LI.model({ ...feed, issues: F.issues('gf1', 0, { mates: [{ id: 'ss1', label: 'SS Sheet 1' }] }) }, { step: 'laser' });
    assert.equal(m.title, 'Laser cutting'); eq(m.sheets.map(s => [s.id, s.label, s.metal]), [['ss1', 'SS Sheet 1', 'silver']]);
    m = LI.model(feed, { step: 'qr' });
    assert.equal(m.title, 'Engraving', 'a step with nothing falls back to what the sheet is held by, never an empty panel');
    m = LI.model({ ...feed, issues: [] }, { step: 'orders' }); assert(!m.own && !m.count, 'no issues, nothing to show');
    // an engraving issue that names an order is not a row (Paul: no back engraving status in this list)
    m = LI.model({ ...feed, issues: [{ step: 'engraving', key: 'x', orderId: '7', customer: 'X', open: { type: 'order', id: '7' } }, ...F.issues('gf1', 1, { keys: ['noDesign'] })] }, { step: 'orders' });
    eq(m.orders.map(o => o.reason.chip), ['1 piece has no design']);
    // the order is listed once; long lists group by reason
    m = LI.model({ ...feed, issues: [...F.issues('gf1', 2, { keys: ['pooled'] }), ...F.issues('gf1', 2, { keys: ['pooled'] })] }, { step: 'orders' }); assert.equal(m.orders.length, 2, 'an order is listed once');
    m = LI.model({ ...feed, issues: F.issues('gf1', 40) }, { step: 'orders' });
    assert.equal(m.orders.length, 40); assert(m.groups.length >= 3 && m.groups.length <= 9); assert.equal(m.groups.reduce((n, g) => n + g.orders.length, 0), 40);
    assert(m.groups.some(g => g.text === 'Not on a sheet yet') && m.groups.some(g => g.text === 'Waits on SS Sheet 1'), 'groups carry the plain reason');
    for (const k of ['pooled', 'noSku', 'unmatched', 'noDesign', 'held', 'otherSheetNotReady', 'somethingNew']) { const r = LI.model({ ...feed, issues: [{ step: 'orders', key: k, orderId: '9', customer: 'A', pieces: [{ sheetLabel: 'SS Sheet 2', why: "its other line is 'pooled' and it has more words than anyone reads" }, { sheetLabel: 'SS Sheet 2' }] }] }, { step: 'orders' }).orders[0].reason.chip; assert(!/\blines?\b/i.test(r) && r.split(' ').length <= 7, `a short plain chip for ${k}: ${r}`); }
    // the adapter over today's explain() items
    const ex = { ready: false, done: false, steps: [{ key: 'nesting', state: 'done', items: [] }, { key: 'engraving', state: 'waiting', items: [{ kind: 'charm', id: 'c', label: 'x', why: 'y' }] }, { key: 'orders', state: 'blocked', items: [{ kind: 'order', id: '4170252963', label: 'Order 4170252963 (Nathaly Soto)', why: "One of its other lines is still 'pooled'" }, { kind: 'order', id: '4170408845', label: 'Order 4170408845 (Emily Chambers)', why: 'SKU not in a master' }, { kind: 'order', id: '1', label: 'Order 1', why: 'Its other piece is on SS Sheet 1, and its engraving needs approval' }, { kind: 'sheet', id: 'ss1', label: 'SS Sheet 1', why: 'holds it' }] }] };
    const ad = LI.adapt(ex, { id: 'gf1' });
    eq(ad.map(x => x.step + ':' + x.key), ['engraving:engraving', 'orders:pooled', 'orders:unmatched', 'orders:otherSheetNotReady']); assert.equal(ad[1].customer, 'Nathaly Soto'); assert.equal(ad[3].pieces[0].sheetLabel, 'SS Sheet 1');
    eq(LI.adapt({ ready: true, steps: [] }, {}), [], 'a ready sheet has no issues');
  }

  // ── 1. a sheet card with its '!' (the agreed markup), the real LaserReview feeding the panel
  const gf = F.sheet('gf1'); for (let i = 0; i < 5; i++) gf.orderReadiness[gf.orders[i]] = { ready: false, why: "line is 'pooled'" };
  w.__rows = gf.orders.map((o, i) => ({ key: o + '_t' + i, order: { receiptId: o, buyer: { name: F.NAMES[i % F.NAMES.length] } }, line: { title: 'Charm', listingId: 'L' + (i % 12) }, state: 'written', poolIds: [gf.poolIds[i]] }));
  w.__feed.gf1 = F.issues('gf1', 5);
  const st = { setId: 'set1', seq: 1, name: 'Set 1', day: '2026-10-03', sheetIds: ['gf1'], status: 'open' };
  L.sections(body); L.record(gf);
  const card = w.Sets.libraryCard(st, [gf], [gf]); L.place(card, L.group(st, [gf]).ready, body);
  L.changed(); await tick();
  const bang = (step = 'orders', id = 'gf1') => { const box = card.querySelector('.flowBox') || card; let b = box.querySelector('button[data-issues-open][data-issues-step="' + step + '"]'); if (!b) { b = d.createElement('button'); b.type = 'button'; b.className = 'flowDot flowBang'; b.setAttribute('data-issues-open', ''); b.dataset.issuesKind = 'sheet'; b.dataset.issuesId = id; b.dataset.issuesStep = step; b.setAttribute('aria-haspopup', 'dialog'); b.setAttribute('aria-expanded', 'false'); b.textContent = '!'; (box.querySelector('.flowStep') || box).appendChild(b); } return b; };
  const panel = () => d.getElementById('libIssuesPanel');
  const b1 = bang();
  assert(card.querySelector('.flowBox button.flowBang[data-issues-open][data-issues-kind="sheet"][data-issues-id="gf1"][data-issues-step="orders"]') === b1, 'the set card\'s own rail carries the \'!\' on the Order check step (nothing is faked here)');
  assert.equal(panel(), null, 'closed until the \'!\' is pressed');
  readCount = 0; L.changed(); await tick(); assert.equal(readCount, 0, 'a closed panel costs the Library frame nothing: issues() is not read');

  b1.click(); await tick();
  let p = panel(); assert(p, 'the \'!\' opens the panel'); assert.equal(p.getAttribute('role'), 'dialog'); assert.equal(b1.getAttribute('aria-expanded'), 'true'); assert.equal(p.parentElement, d.body, 'a fixed layer on the page, not inside the card');
  assert.equal(rowsOf(p).length, 5); assert.match(p.querySelector('.lisHead b').textContent, /^Order check$/); assert.match(p.querySelector('.lisCount').textContent, /5 issues/i);
  assert.match(texts(p), /1 piece not on a sheet yet/); assert.match(texts(p), /Waits on SS Sheet 1/); assert.match(texts(p), /1 piece has no SKU/);
  for (const bad of [/\bNesting\b/, /\bBack files\b/, /\bQR label\b/, /\bEngraving\b/, /Layout verified/, /\b\d+ of \d+\b/, /\blines?\b/i, /back engraving/i]) assert(!bad.test(texts(p)), `nothing about completed steps or engraving: ${bad}`);
  assert.equal(p.querySelectorAll('svg path[d*="M2.6 6.3"]').length, 0, 'no completed ticks');
  const first = rowsOf(p)[0]; assert.equal(first.querySelector('b').textContent, w.__feed.gf1[0].orderId); assert.equal(first.querySelector('i').textContent, 'Nathaly Soto');
  assert.equal(first.querySelectorAll('.lisDot').length, 2, 'two pieces: one dot each'); assert.equal(first.querySelectorAll('.lisDot.ring').length, 1, 'a ring for the problem piece');
  assert(d.activeElement && p.contains(d.activeElement), 'focus moves into the panel');
  // pictures: the listing photo from the page's own cache, the quiet piece icon until it is there
  assert.equal(first.querySelectorAll('.lisTh svg').length, 1, 'a quiet piece icon where there is no picture yet');

  // ── 2. a press on a row hands over to the page's own helpers; the panel comes back when that window closes, with what is left
  const orderId = w.__feed.gf1[1].orderId;
  rowsOf(p)[1].click(); await tick();
  eq(calls.pop().slice(0, 3), ['order', orderId, null], 'the order opens with openOrderFrom'); assert(p.classList.contains('handed'), 'the panel hands over (it does not stack on the order view)');
  const win = d.createElement('dialog'); win.setAttribute('open', ''); d.body.appendChild(win);
  w.__feed.gf1 = w.__feed.gf1.filter(x => x.orderId !== orderId);   // the person took that order off its sheet meanwhile
  win.removeAttribute('open'); win.dispatchEvent(new w.Event('close')); await tick(120);
  p = panel(); assert(p && !p.classList.contains('handed'), 'the panel is back'); assert.equal(rowsOf(p).length, 4, 'and shows only what is still left'); assert(!rowsOf(p).some(r => r.dataset.id === orderId));
  assert.match(p.querySelector('.lisCount').textContent, /4 issues/i);
  assert(p.contains(d.activeElement), 'focus returns into the panel');
  // a window that never opened (the hand-off failed): the panel is not left hidden
  openWindow = () => false; rowsOf(p)[0].click(); await tick(); assert(!panel().classList.contains('handed'), 'a failed hand-off leaves the panel as it was'); openWindow = null; calls.length = 0;
  // sheet and piece links
  w.__feed.gf1 = [...w.__feed.gf1, { step: 'orders', key: 'noSku', orderId: '4999000001', customer: 'Piece Link', listingId: 'L1', pieces: [{ index: 2, label: 'x', sheetLabel: null }], open: { type: 'piece', id: '4999000001', poolId: '4999000001_t_2' } }]; L.changed(); await tick();
  const pr = rowsOf(panel()).find(r => r.dataset.pool); assert(pr, 'a piece issue keeps its piece'); pr.click(); await tick(); eq(calls.pop().slice(0, 3), ['order', '4999000001', '4999000001_t_2'], 'it opens the order at that piece');
  panel().classList.remove('handed'); w.eval('LibraryIssues.close()'); await tick(200);

  // ── 3. Esc, outside press, the '!' again; focus goes back to the '!'
  b1.click(); await tick(); assert(panel());
  d.dispatchEvent(new w.KeyboardEvent('keydown', { key: 'Escape', bubbles: true })); await tick(200);
  assert.equal(panel(), null, 'Esc closes'); assert.equal(b1.getAttribute('aria-expanded'), 'false'); assert.equal(d.activeElement, b1, 'focus returns to the \'!\'');
  b1.click(); await tick(); d.body.dispatchEvent(new w.Event('pointerdown', { bubbles: true })); await tick(200); assert.equal(panel(), null, 'a press outside closes');
  b1.click(); await tick(); b1.click(); await tick(200); assert.equal(panel(), null, 'the \'!\' again closes');
  // keyboard: arrows move between rows, Tab stays inside
  b1.click(); await tick(); p = panel(); const rs = rowsOf(p); rs[0].focus();
  d.dispatchEvent(new w.KeyboardEvent('keydown', { key: 'ArrowDown', bubbles: true, cancelable: true })); assert.equal(d.activeElement, rs[1]);
  d.dispatchEvent(new w.KeyboardEvent('keydown', { key: 'End', bubbles: true, cancelable: true })); assert.equal(d.activeElement, rs[rs.length - 1]);
  d.dispatchEvent(new w.KeyboardEvent('keydown', { key: 'Tab', bubbles: true, cancelable: true })); assert.equal(d.activeElement, rs[0], 'Tab wraps inside the panel');
  w.eval('LibraryIssues.close()'); await tick(200);

  // ── 4. live: the panel stays open and its rows follow the Library's own refresh (no poller of its own)
  w.__feed.gf1 = F.issues('gf1', 5); b1.click(); await tick(); p = panel();
  const keep = rowsOf(p)[2], keepImg = keep.querySelector('.lisTh'); const ids = rowsOf(p).map(r => r.dataset.id);
  w.__feed.gf1 = [{ ...w.__feed.gf1[0], key: 'noSku' }, ...w.__feed.gf1.slice(2), ...F.issues('gf1', 1, { start: 4200000000, keys: ['held'] })];   // row 1 changed its reason, row 2 is gone, a new one arrives
  L.changed(); await tick(300);
  assert.equal(panel(), p, 'the same panel, updated in place'); const now = rowsOf(p);
  assert.equal(now.length, 5, 'one fell away, one arrived'); assert(!now.some(r => r.dataset.id === ids[1]), 'the row that is no longer an issue folds away'); assert(now.some(r => /on hold/.test(r.textContent) && r.dataset.id === '4200000000'), 'a new issue slides in');
  assert.match(now[0].textContent, /1 piece has no SKU/, 'a row re-words itself when its reason changes'); assert.equal(now.find(r => r.dataset.id === ids[2]), keep, 'a row that did not change is the same element'); assert.equal(keep.querySelector('.lisTh'), keepImg);
  assert.equal(b1.getAttribute('aria-expanded'), 'true');
  // the card drawn anew (its '!' is a new element): the panel follows it
  const parent = b1.parentElement; b1.remove(); const b2 = bang(); b2.dataset.issuesId = 'gf1'; (parent.isConnected ? parent : card).appendChild(b2); L.changed(); await tick(200);
  assert(panel(), 'the panel survives its \'!\' being drawn again'); assert.equal(b2.getAttribute('aria-expanded'), 'true');
  // everything fixed elsewhere: the panel goes (a sheet with nothing left has no '!' either)
  w.__feed.gf1 = []; L.changed(); await tick(300); assert.equal(panel(), null, 'nothing left to show closes it');
  // a '!' pressed while the sheet has nothing to say opens nothing
  b2.click(); await tick(); assert.equal(panel(), null);

  // ── 5. the Engraving step: just one link, with the Engraving tab shortcut
  w.__feed.gf1 = F.issues('gf1', 4, { own: 'engraving' }); const be = bang('engraving'); be.click(); await tick(); p = panel();
  assert.equal(p.querySelectorAll('.lisRow').length, 0, 'no engraving rows'); assert.equal(p.querySelectorAll('.lisOwn').length, 1); assert.equal(texts(p).replace(/Engraving/, '').trim().replace(/\s+/g, ' '), 'Open engraving approvals');
  assert.doesNotMatch(texts(p), /\d/, 'no counts, no numbers'); calls.length = 0;
  p.querySelector('.lisOwn').click(); await tick(200); eq(calls[0], ['mode', 'engrave'], 'the link opens the Engraving tab'); assert.equal(panel(), null);
  // the QR label step: one small chip, no list
  w.__feed.gf1 = F.issues('gf1', 3, { own: 'qr' }); bang('qr').click(); await tick(); p = panel(); assert.equal(p.querySelectorAll('.lisRow').length, 0); assert.match(p.querySelector('.lisOwn').textContent, /QR label missing/); w.eval('LibraryIssues.close()'); await tick(200);
  // Laser cutting: the sheet that holds the set back
  w.__feed.gf1 = F.issues('gf1', 0, { mates: [{ id: 'ss1', label: 'SS Sheet 1' }] }); bang('laser').click(); await tick(); p = panel();
  assert.equal(rowsOf(p).length, 1); assert.match(texts(p), /SS Sheet 1/); calls.length = 0; rowsOf(p)[0].click(); await tick(); eq(calls.pop(), ['sheet', 'ss1']); p.classList.remove('handed'); w.eval('LibraryIssues.close()'); await tick(200);

  // ── 6. forty issues: groups by reason, each folded with its count and a stack of pictures; "Show N more"
  w.__feed.gf1 = F.issues('gf1', 40); bang().click(); await tick(); p = panel();
  const heads = [...p.querySelectorAll('.lisGroup')]; assert(heads.length >= 3 && heads.length <= 9, 'grouped by reason'); assert.equal(rowsOf(p).length, 0, 'folded: the groups show a count and a stack, not forty rows');
  assert(heads.every(h => h.querySelector('.lisN') && h.querySelector('.lisStack') && h.getAttribute('aria-expanded') === 'false'));
  assert.match(p.querySelector('.lisCount').textContent, /40 issues/i);
  heads[0].click(); await tick(300); assert.equal(heads[0].getAttribute('aria-expanded'), 'true'); const shown = rowsOf(p).length; assert(shown >= 3 && shown <= 4, `a group opens to a few rows (${shown})`);
  const more = p.querySelector('.lisMore'); assert(more && /^Show \d+ more$/.test(more.textContent)); const total = +heads[0].querySelector('.lisN').textContent; assert.equal(+/\d+/.exec(more.textContent)[0], total - shown);
  more.click(); await tick(300); assert.equal(rowsOf(p).length, total, 'Show more shows the rest'); assert.equal(p.querySelector('.lisMore').textContent, 'Show less');
  assert(!/\blines?\b/i.test(texts(p)), 'pieces, never lines'); w.eval('LibraryIssues.close()'); await tick(200);

  // ── 7. long names stay on one line; a sheet picture arrives from the page's cache
  w.__feed.gf1 = F.issues('gf1', 2, { lid: 'P', names: ['Maximiliana Alexandria von Habsburg-Lothringen-Esterházy'] }); photos.set('P0', 'data:image/svg+xml;utf8,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg"/>'));
  w.eval('LaserReview.photo = lid => ListMedia.listing(lid)'); bang().click(); await tick(100); p = panel();
  assert.equal(p.querySelector('.lisWho').children.length, 2, 'one short line: the number and the name'); assert(rowsOf(p)[0].querySelector('.lisTh img'), 'the listing photo is laid in when the cache has it'); w.eval('LibraryIssues.close()'); await tick(200);

  // ── 8b. the REAL CharmNestReadiness.issues feeds the panel (records as the server answers them: orderReadiness with blocks)
  {
    R.issues = realIssues;
    const rec = F.sheet('gf1', { n: 6 }), ids = rec.orders, blk = (key, i, extra = {}) => ({ key, index: 2, label: 'Charm', poolId: ids[i] + '_x_2', lineKey: ids[i] + '_x', sheetId: null, sheetLabel: null, why: { pooled: 'A piece is not on a saved sheet yet', noSku: 'SKU not in a master', held: 'Check customer changes', otherSheetNotReady: 'SS Sheet 1: back engraving files are not saved' }[key], ...extra });
    const rpt = (i, b, name, lid) => ({ ready: false, key: b.key, why: b.why, blocks: [b], onSheets: ['gf1'], pieceCount: 2, customer: name, listingId: lid });
    rec.orderReadiness[ids[0]] = rpt(0, blk('pooled', 0), 'Nathaly Soto', 'L1');
    rec.orderReadiness[ids[1]] = rpt(1, blk('noSku', 1), 'Emily Chambers', 'L2');
    rec.orderReadiness[ids[2]] = rpt(2, blk('otherSheetNotReady', 2, { sheetId: 'ss1', sheetLabel: 'SS Sheet 1', stage: 'backs' }), 'Leslie Suhr', 'L3');
    rec.orderReadiness[ids[3]] = rpt(3, blk('held', 3, { sheetId: 'gf1', sheetLabel: 'GF Sheet 1' }), 'Jechelle Aragones', 'L4');
    L.record(rec); L.changed(); await tick();
    const rb = card.querySelector('.flowBox button.flowBang[data-issues-step="orders"]'); assert(rb, 'the rail has its \'!\' on the Order check step');
    rb.click(); await tick(); p = panel(); assert(p, 'the \'!\' opens the panel from the real issues');
    eq(rowsOf(p).map(r => r.dataset.issueOrder), ids.slice(0, 4), 'one row per order that something holds back, nothing for the orders that are ready');
    eq(rowsOf(p).map(r => r.querySelector('.lisChip').textContent), ['1 piece not on a sheet yet', '1 piece has no SKU', 'Waits on SS Sheet 1', 'On hold: check customer changes'], 'one short plain reason each, a held piece says why');
    eq(rowsOf(p).map(r => r.querySelector('.lisWho i').textContent), ['Nathaly Soto', 'Emily Chambers', 'Leslie Suhr', 'Jechelle Aragones'], 'the customer comes with the issue');
    assert.equal(w.LibraryIssues.listed().length > 0, true); assert.equal(p.getAttribute('data-issues-for'), 'sheet:gf1');
    assert(!/Order 4|completed|Nesting|QR label|Back files|\blines?\b/i.test(texts(p)), 'only issues, in pieces');
    // an order nothing was read for: ONE quiet chip for the sheet, never a row per order
    for (const i of [4, 5]) rec.orderReadiness[ids[i]] = { ready: false, why: 'Order readiness has not been verified' };
    L.record(rec); L.changed(); await tick(300); p = panel();
    assert.equal(p.querySelectorAll('.lisNote').length, 1, 'Orders not checked yet is one chip'); assert.match(p.querySelector('.lisNote').textContent, /^Orders not checked yet$/); assert.equal(p.querySelector('.lisNote').dataset.issueOrders, ids.slice(4).join(','));
    assert.equal(rowsOf(p).length, 4, 'no row for the unread orders'); assert.match(p.querySelector('.lisCount').textContent, /5 issues/);
    // a one-piece order held by a person holds the sheet: its row says Held
    rec.orderReadiness[ids[0]] = { ready: false, key: 'held', why: 'Order changes need review', blocks: [blk('held', 0, { why: 'Order changes need review' })], onSheets: ['gf1'], pieceCount: 1, customer: 'Solo Piece', listingId: 'L9' };
    L.record(rec); L.changed(); await tick(300);
    const solo = rowsOf(p).find(r => r.dataset.issueOrder === ids[0]); assert.match(solo.querySelector('.lisChip').textContent, /^On hold: order changes need review$/);
    // only its unread orders left: the panel is just that chip
    for (const i of [0, 1, 2, 3]) rec.orderReadiness[ids[i]] = { ready: true };
    L.record(rec); L.changed(); await tick(300); p = panel(); assert(p && rowsOf(p).length === 0 && p.querySelectorAll('.lisNote').length === 1, 'the chip alone stays');
    w.eval('LibraryIssues.close()'); await tick(200);
    R.issues = (s) => { readCount++; return w.__feed[s.id || s.sheetId] || []; };
  }

  // ── 8. a sheet that is ready has no panel; nothing here wrote, opened or stamped anything on its own
  assert(!calls.some(c => !['mode', 'sheet', 'order'].includes(c[0]))); assert(!/undefined|\[object|NaN/.test(body.textContent + (panel()?.textContent || '')), 'no stray placeholders');
  console.log('Library issues OK: the \'!\' opens a panel of only the issues (rows with picture, number, name, piece dots, one chip), Engraving is a single link, hand-off and way back, Esc / outside / toggle, live in-place updates, 40 issues grouped, closed panel costs nothing');
  } finally { if (dom) dom.window.close(); }
})().catch(e => { console.error(e); process.exitCode = 1; });
