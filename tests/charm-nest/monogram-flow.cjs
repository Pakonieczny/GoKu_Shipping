// MONOGRAM FLOW (MU4, 16 Oct 2026): one monogram order, from the Etsy order to the sheet, in the REAL sorter page (charm-nest-1.html) in headless Chromium over the repo's fake site (bridge-server.cjs through the engraving world
// _engraveRealWorld.cjs): the real intake, pool, classifier and the real Master library holding Paul's own 78 letters (fixtures/SKUs_MONOGRAM_2024-1015.ai, indexed by the local script). No Etsy, no Claude (the page's agent call is a stub
// that records and answers), nothing live. The widget is the one component CharmNestMonogramUI.mountLine (/tmp/mu/CONTRACT.md: hooks data-mw-*), shown in the Engrave tab (detail, full) and in the Order details window (compact).
//   1  the orders: three monogram lines + a sized one + odd ones are read (letters, size), a plain engraved order and the engraved bar of a mixed order are not monograms
//   2  the Engrave tab: no Monograms tab; a monogram row shows the finished charm and its letters, never Front / Back; its detail holds the widget (full); the plain engraved card is what it was
//   3  letters recognised and fetched: letter 1 from the Left alphabet, 2 from the Middle, 3 from the Right (only those files); the preview is the geometry the engine cuts (one piece, ~14.8 mm, counters), in the line's metal; ONE size line
//   4  editing: a letter typed -> spinner + the page's progress bar, Approve waits, ONE remake of the newest edit, a stale answer never overwrites, a quick second edit makes one build, swap / rotate / paste / Esc, a letter typed back is
//      instant (no fetch, no build), a digit / emoji is refused calmly, an empty box waits (no build)
//   5  trouble: a letter missing from an alphabet (plain message naming it), a letter file that fails (Try again), odd buyer words (2 letters, a name, 4 letters, digits, emoji, non-Latin): a plain message and the three boxes
//   6  the piece size: the order's size option (16MM) is the envelope: the piece never passes it, the line says so; an order without a size is the natural piece
//   7  Approve (MONOSEND, 18 Oct 2026: the green Approve, one press approves AND sends, on the buyer's metal, no designs window; final on the sheet, a second press is nothing; a metal the order does not give: approved, the designs window
//      opens): Monogram APV.ai on the order's custom designs (read back by the page's own cut-file preflight as ONE piece of the previewed size), the same state in the OTHER place; Change then a new
//      letter REPLACES the design (never two, the metal a person set kept), the same letters again change nothing; another order's monogram is its own design
//   8  the Order details window: the compact widget, the same state as the Engrave detail (typing in one shows in the other), no Front / Back cards for the monogram, a mixed order keeps its engraved bar's card
//   9  on the sheets: the Review tab does not list the approved monogram (MONOROUTE, 17 Oct 2026: it is the Engraving tab's, its custom card still holds the design); the design went to the sheet from the widget's own Approve (MONOSEND, 18 Oct 2026: no link, no designs window),
//      the widget says it is on the sheets and is read only; a reload / second tab keeps the approved design
//  10  the AI line (stubbed agent): "Assembled with AI: …", "Needs a person: …" with Approve anyway on the second press, Improve again, a failing call, the day's 3 calls
//  11  phone width 400: the list, the Engrave detail and the Order window fit (no sideways scroll, nothing outside its card), reduced motion; no console error and no page error anywhere
//   node tests/charm-nest/monogram-flow.cjs [playwright-core dir]      run it through /tmp/cn-run.sh 1500      Screenshots (1440 and 400 px) go to $SHOTS (default: monogram-flow-shots in the temp directory).
//   MONO_REAL=0 uses stand-in alphabets (vendored fonts) instead of Paul's file.      MONO_ONLY=<n,n> runs only those sections.      It needs the page with the monogram widget (CharmNestMonogramUI.mountLine / widgets()) and says so when it is not there.
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const SHOTS = process.env.SHOTS || path.join(require('os').tmpdir(), 'monogram-flow-shots');
const ONLY = process.env.MONO_ONLY ? new Set(process.env.MONO_ONLY.split(',').map(Number)) : null;
const fails = []; let nCheck = 0;
const check = (ok, msg) => { nCheck++; if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
const sleep = ms => new Promise(r => setTimeout(r, ms));
const V = (name, value) => ({ name, value });
const MONO_TITLE = 'Monogram Necklace Personalized Jewelry Initials Pendant';

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  try { require(path.join(pwDir, 'playwright-core')); } catch (_) { console.log('monogram-flow: no playwright-core, skipped'); return; }
  process.env.PW_DIR = pwDir; fs.mkdirSync(SHOTS, { recursive: true });
  const RW = require('./_engraveRealWorld.cjs'), Base = require('./_orderFactsWorld.cjs'), Alpha = require('./_monogramAlphabets.cjs');
  const M = require(path.join(root, 'charm-nest-monogram.js')), F = require(path.join(root, 'charm-nest-monogram-fit.js')), Real = require('./_monogramReal.cjs');
  const T0 = Date.now();

  // orders: a monogram line may sit beside an engraved bar (lines: [...]), the world's own orderOf takes one line only
  function orderOf(fx, now) {
    const at = (d, h, m) => { const t = new Date(now); t.setDate(t.getDate() + d); t.setHours(h, m, 0, 0); return Math.floor(t.getTime() / 1000); };
    const shipBy = at(fx.ship, 17, 0), line = (x, i) => ({ transactionId: fx.rid + (i + 1), listingId: x.listing, sku: x.sku, title: x.title, quantity: x.qty || 1, expectedShipDate: shipBy, variations: x.variations || [], metalKey: x.metalKey || 'gold', metalLabel: x.metalLabel || 'GF 14/20', personalization: x.personalization || [], buyerMessage: x.buyerMessage || '' });
    return { receiptId: fx.rid, orderNumber: fx.rid, createTs: at(fx.placed.day, fx.placed.h, fx.placed.m), updateTs: at(0, 12, 0), shipBy, buyer: { name: fx.buyer }, buyerMessage: fx.buyerMessage || '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: (fx.lines || [fx]).map(line) };
  }
  Base.orderOf = orderOf;
  const MONO = (words, metal, extra) => Object.assign({ listing: '997905351', sku: 'MONOGRAM_5351', title: MONO_TITLE, variations: [V('Metal choice', metal === 'silver' ? 'Silver' : metal === 'rose' ? 'Rose Gold' : 'Gold'), V('Chain length', '16 inches')], personalization: [words], metalKey: metal || 'gold' }, extra || {});
  const BAR = { listing: '1890000101', sku: 'COMET_9427', title: 'Comet Necklace, Celestial Charm Necklace in 14k Gold Fill', variations: [V('Metal choice', 'Gold'), V('Font Choice', 'Stylish')], personalization: ['Name One'] };
  const O = RW.ORDERS, J = RW.EXPECT_JOBS;
  const add = (name, rid, buyer, lines, n) => { O[name] = { rid, buyer, placed: { day: 0, h: 9, m: n }, ship: 5, lines }; J[name] = 0; };
  add('apv', '4200683017', 'Name One', [MONO('APV', 'gold')], 10);
  add('osr', '4202783520', 'Name Two', [MONO('O S R', 'silver', { listing: '606439317', sku: 'MONOGRAM_9317', title: 'Monogram Necklace Dainty Monogram Necklace' })], 11);
  add('jqx', '4200900001', 'Name Three', [MONO('J.Q.X', 'rose', { listing: '888000001', sku: 'NEWMONO_1', title: 'Custom Monogram Pendant Necklace, 3 Initials' })], 12);
  add('sized', '4200900021', 'Name Four', [MONO('A P V', 'gold', { variations: [V('Metal choice', 'Gold'), V('Size', '16MM')] })], 13);
  add('mixed', '4290000131', 'Name Five', [MONO('K.L.M', 'gold'), BAR], 14); J.mixed = 1;
  for (const [i, [name, words]] of Object.entries([['odd2', 'A.P'], ['odd3', 'Jane Doe'], ['odd4', 'ABCD'], ['odd5', '123'], ['odd6', '😀😀😀'], ['odd7', 'АПВ']])) add(name, '42009000' + (30 + +i), 'Name ' + (6 + +i), [MONO(words, 'gold')], 20 + +i);
  const ODD = { odd2: ['A.P', /Type the third letter|three letters/i, ['A', 'P', '']], odd3: ['Jane Doe', /7 letters/, ['', '', '']], odd4: ['ABCD', /4 letters/, ['', '', '']], odd5: ['123', /digits/i, ['', '', '']], odd6: ['😀😀😀', /no letters/i, ['', '', '']], odd7: ['АПВ', /no letters/i, ['', '', '']] };
  for (const [i, [name, words]] of Object.entries([['ai1', 'G.W.X'], ['ai2', 'N.F.Z'], ['ai3', 'H.R.Y']])) add(name, '42009000' + (50 + +i), 'Name ' + (12 + +i), [MONO(words, 'gold')], 30 + +i);
  const rid = n => O[n].rid;

  const w = await RW.world({ viewport: { width: 1440, height: 900 }, shots: SHOTS });
  if (!w) { console.log('monogram-flow: no playwright-core, skipped'); return; }
  const { page, srv } = w, st = srv.st;
  const consoleErrors = [];
  page.on('console', m => { if (m.type() === 'error' && !/Failed to load resource|net::ERR|favicon/i.test(m.text())) consoleErrors.push(m.text().slice(0, 200)); });
  let blobHold = null, blobFail = null; const blobSeen = [];
  await w.context.route(/Monogram_\w+_\w\.ai/i, async route => {
    const url = route.request().url(); blobSeen.push(url);
    if (blobFail && blobFail.test(url)) return route.abort('failed');
    if (blobHold && blobHold.test(url)) await new Promise(res => blobHold.waiting.push(res));
    return route.continue();
  });
  const holdOn = re => { re.waiting = []; blobHold = re; };
  const release = () => { const b = blobHold; blobHold = null; if (b) b.waiting.forEach(f => f()); };
  const shot = async name => { try { await page.screenshot({ path: path.join(SHOTS, 'flow-' + name + '.png') }); } catch (e) { console.log('   (no screenshot ' + name + ')'); } };
  const shotEl = async (sel, name) => { try { await page.locator(sel).first().screenshot({ path: path.join(SHOTS, 'flow-' + name + '.png') }); } catch (e) { console.log('   (no screenshot ' + name + ')'); } };
  const section = (n, title, fn) => ({ n, title, fn });

  // ── reading a widget as a person sees it ──
  const mwSel = (r, mode) => `.mw[data-rid="${r}"]` + (mode ? `[data-mode="${mode}"]` : '');
  const read = (r, mode) => page.evaluate(({ r, mode }) => {
    const h = document.querySelector(`.mw[data-rid="${r}"]` + (mode ? `[data-mode="${mode}"]` : '')); if (!h) return null;
    const q = s => h.querySelector(s), vis = e => !!e && !e.hidden && getComputedStyle(e).display !== 'none' && getComputedStyle(e).visibility !== 'hidden';
    const t = e => (e ? e.textContent.replace(/\s+/g, ' ').trim() : '');
    const path = q('[data-mw-pic] svg path'), hooks = ['[data-mw-spin]', '[data-mw-assembly]', '[data-mw-msg]', '[data-mw-retry]', '[data-mw-approve]', '[data-mw-approved]', '[data-mw-change]', '[data-mw-again]', '[data-mw-sheet]'];
    const leaks = hooks.filter(s => { const e = q(s); return e && e.hidden && getComputedStyle(e).display !== 'none'; });       // (marked hidden, but CSS shows it anyway)
    const ap = q('[data-mw-approve]');
    return { state: h.dataset.state, busy: h.dataset.busy || '', mode: h.dataset.mode, ins: [...h.querySelectorAll('[data-mw-letter]')].map(i => i.value), insDisabled: [...h.querySelectorAll('[data-mw-letter]')].map(i => i.disabled), invalid: [...h.querySelectorAll('[data-mw-letter]')].map(i => i.getAttribute('aria-invalid') === 'true'),
      says: t(q('[data-mw-says]')), size: vis(q('[data-mw-size]')) ? t(q('[data-mw-size]')) : '', asm: vis(q('[data-mw-assembly]')) ? { text: t(q('[data-mw-assembly] > span') || q('[data-mw-assembly]')), status: q('[data-mw-assembly]').dataset.status || '' } : null,
      msg: vis(q('[data-mw-msg]')) ? { text: t(q('[data-mw-msg]')), kind: q('[data-mw-msg]').dataset.kind || '' } : null, retry: vis(q('[data-mw-retry]')), spin: vis(q('[data-mw-spin]')), picBusy: (q('[data-mw-pic]') || {}).getAttribute ? q('[data-mw-pic]').getAttribute('aria-busy') : '',
      approve: ap ? { text: t(ap), shown: vis(ap), disabled: ap.disabled, title: ap.title || '' } : null, badge: vis(q('[data-mw-approved]')) ? t(q('[data-mw-approved]')) : '', change: vis(q('[data-mw-change]')), again: vis(q('[data-mw-again]')), sheet: vis(q('[data-mw-sheet]')), swaps: h.querySelectorAll('[data-mw-swap]').length, rotate: !!q('[data-mw-rotate]'),
      d: path ? path.getAttribute('d') : '', points: path ? (path.getAttribute('d').match(/[ML]/g) || []).length : 0, subpaths: path ? (path.getAttribute('d').match(/M/g) || []).length : 0, fill: path ? path.getAttribute('fill') : '', stroke: path ? path.getAttribute('stroke') : '', leaks, text: t(h).slice(0, 400) };
  }, { r, mode });
  const states = r => page.evaluate(r => (window.CharmNestMonogramUI.widgets() || []).filter(x => x.state().rid === r).map(x => { const s = x.state(); return { mode: x.mode, state: s.state, busy: s.busy, letters: s.letters.join(''), canApprove: s.canApprove, approved: s.approved && s.approved.letters, sent: s.approved && s.approved.sent, dirty: s.dirty, locked: s.locked, metal: s.metal, sizeMm: s.sizeMm, fit: s.fit, assembly: s.assembly, error: s.error && s.error.kind, notes: s.notes }; }), r);
  const until = async (r, mode, pred, what, ms = 90000) => { const t0 = Date.now(); let x; for (;;) { x = await read(r, mode); if (x && pred(x)) return x; if (Date.now() - t0 > ms) { console.log('   (waiting for ' + what + ', last: ' + JSON.stringify(x && Object.assign({}, x, { d: x.d.length })).slice(0, 500) + ')'); return x; } await sleep(80); } };
  const ready = (r, mode, ms) => until(r, mode, x => !x.spin && ['ready', 'check', 'approved', 'sent'].includes(x.state), 'a finished monogram', ms);
  const builds = () => page.evaluate(() => window.__builds.slice());
  const fetched = () => page.evaluate(() => window.__fetched.slice());
  const typeIn = async (r, mode, i, text) => { await page.click(`${mwSel(r, mode)} [data-mw-letter="${i}"]`); await page.keyboard.type(text); };
  const pasteWord = (r, mode, text) => page.evaluate(({ sel, text }) => { const t = document.querySelector(sel + ' [data-mw-letter="0"]'); t.focus(); const dt = new DataTransfer(); dt.setData('text', text); t.dispatchEvent(new ClipboardEvent('paste', { clipboardData: dt, bubbles: true, cancelable: true })); }, { sel: mwSel(r, mode), text });
  const entries = () => page.evaluate(() => Object.values(CustomSheet.entries()).map(e => ({ ck: e.ck, rid: e.rid, files: e.files.map(f => ({ id: f.id, name: f.name, state: f.state, error: f.error, metal: f.metal, qty: f.qty, pieces: f.pieces, wMm: f.wMm, hMm: f.hMm })), sent: !!e.sent })));
  const files = async r => (await entries()).filter(e => String(e.rid) === String(r)).flatMap(e => e.files);
  const allNames = async () => (await entries()).map(e => e.files.map(f => f.name).join()).filter(Boolean).sort();
  const readyFiles = () => page.waitForFunction(() => Object.values(CustomSheet.entries()).every(e => e.files.every(f => f.state === 'ready' || f.state === 'error')), null, { timeout: 40000 }).catch(() => {});
  const instrument = () => page.evaluate(() => {
    window.__builds = []; window.__fetched = []; window.__calls = []; window.__answers = {}; window.__stage = new Set();
    if (window.__wrapped) return; window.__wrapped = true;
    const E = CharmNestMonogram, eb = E.build, ef = E.autoFit;
    // every build is recorded (letters + the options it was given); a word in __stage is staged as breaking one rule until the left letter is moved 0.3 mm (the real engine does the building and clamping; only the verdict is staged: section 10)
    const stage = (r, l, o) => {
      const tag = l.left.letter + l.middle.letter + l.right.letter;
      if (!r || !r.ok || !window.__stage.has(tag)) return r;
      window.__staged = (window.__staged || 0) + 1;
      const a = o && o.adjust, fixed = !!(a && a.L && a.L.dx >= 0.3);      // (fixed only by an adjust the AI loop hands to Engine.build: the engine's own autoFit moves the letters too and must not count)
      r.report.metrics = Object.assign({}, r.report.metrics, fixed ? { violations: [], distance: 0.5, ok: true } : { violations: [{ rule: 'contacts', key: 'contacts', text: 'the left and middle letters are not held by one contact near the top and one near the bottom' }], distance: 3, ok: false });
      return r;
    };
    E.build = function (l, o) { window.__builds.push(l.left.letter + l.middle.letter + l.right.letter + (o && o.fit ? '@' + o.fit.maxWidthMm : '')); return stage(eb.apply(this, arguments), l, o); };
    if (ef) E.autoFit = function (l, o) { return stage(ef.apply(this, arguments), l, null); };
    const mc = Pool.masterCharm; Pool.masterCharm = function (e, s) { window.__fetched.push(e.sku); return mc.apply(this, arguments); };
  });
  const settle = ms => RW.settle(page, ms || 400);
  const reloadMaster = async () => {
    for (let i = 0; i < 8; i++) { const want = [...st.docs.keys()].filter(k => k.startsWith('Charm_Master_Index/')).length; const have = await page.evaluate(async () => { await Master.load(true); return B.master.entries.size; }); if (have === want) return; await sleep(400); }
  };
  const openList = () => page.evaluate(() => { CN.setMode('engrave'); Engrave.restoreView({ tab: 'place', list: true, chosen: true, focus: null, pin: null, openDone: null }); Engrave.render(); });
  const openDetail = async n => {      // the Engrave detail of a monogram line, by its row (what a person does)
    await openList(); await settle(900);
    const row = await page.$(`#egQueue .doneRow[data-kind=monogram][data-rid="${rid(n)}"]`);
    if (row) await row.click(); else await page.evaluate(k => Engrave.openMonogram(k), keyOf(n));      // (a monogram line is a row of the one list, approved or not, and opens the same card)
    await page.waitForSelector(mwSel(rid(n), 'detail'), { timeout: 30000 }); await settle(500);
  };
  const openWin = async n => {         // the Order details window of the order
    await page.evaluate(async k => { CN.setMode('orders'); await OrderWin.open(k); }, keyOf(n));
    await page.waitForSelector('#orderWin[open]', { timeout: 20000 }); await page.waitForSelector(`#orderWin ${mwSel(rid(n), 'compact')}`, { timeout: 30000 }); await settle(500);
  };
  const closeWin = () => page.evaluate(() => { try { OrderWin.close(); } catch (_) { /* closed */ } });
  const keyOf = n => `${O[n].rid}_${O[n].rid}1`;
  // does anything stick out of its box: every visible descendant of each matching box must lie inside it (1.5 px slack); page / body: the sideways scroll of the whole page
  const overflow = sel => page.evaluate(sel => {
    const roots = [...document.querySelectorAll(sel)]; if (!roots.length) return null; const bad = []; let own = 0, left = 1e9, right = -1e9;
    for (const root of roots) {
      const rr = root.getBoundingClientRect(); own = Math.max(own, root.scrollWidth - root.clientWidth); left = Math.min(left, Math.round(rr.left)); right = Math.max(right, Math.round(rr.right));
      for (const el of root.querySelectorAll('*')) { const cs = getComputedStyle(el); if (cs.display === 'none' || cs.visibility === 'hidden' || el.closest('[hidden]') || el.closest('.sealRow')) continue; const r = el.getBoundingClientRect(); if (!r.width && !r.height) continue; if (r.right > rr.right + 1.5 || r.left < rr.left - 1.5) bad.push((el.className && el.className.baseVal === undefined ? '.' + String(el.className).split(' ')[0] : el.tagName) + ':' + Math.round(r.left - rr.left) + '/' + Math.round(r.right - rr.right)); }
    }
    return { n: roots.length, page: document.documentElement.scrollWidth - document.documentElement.clientWidth, body: document.body.scrollWidth - innerWidth, own, bad: bad.slice(0, 6), left, right, vw: innerWidth };
  }, sel);

  // the library: Paul's file indexed by the local script, else stand-ins
  const REAL = process.env.MONO_FILE || path.join(__dirname, 'fixtures', 'SKUs_MONOGRAM_2024-1015.ai'), REAL_OK = process.env.MONO_REAL !== '0' && fs.existsSync(REAL);
  let truth = null;           // the node-side engine on the same letters: what the preview must be
  const seedLibrary = async () => {
    if (!REAL_OK) { Alpha.seed(st, await Alpha.make()); return; }
    const tmp = fs.mkdtempSync(path.join(require('os').tmpdir(), 'cn-mono-flow-')), file = path.join(tmp, path.basename(REAL)); fs.copyFileSync(REAL, file);
    const { main } = require('../../scripts/index-master.cjs'), { REPO_TEST } = require('./bridge-server.cjs');
    st.repoStrict = true; process.env.CHARM_REPO_PASSWORD = REPO_TEST.password;
    await main(['node', 'x', file, '--origin', srv.sorterOrigin], () => {});
  };
  const truthOf = async (word, o) => { if (!REAL_OK) return null; truth = truth || await Real.load(); const r = M.build(await truth.word(word), o); return r && r.ok ? r : null; };
  const lettersH = r => (r.report.hoops && r.report.hoops.lettersMm ? r.report.hoops.lettersMm[1] : r.hMm);      // (MONOHOOPS, Paul 19 Oct 2026: the piece carries two hoops; 14.8 mm is the letters' height)

  const SECTIONS = [];
  let made = null;
  try {
    if (!await page.evaluate(() => !!(window.CharmNestMonogramUI && typeof CharmNestMonogramUI.widgets === 'function' && typeof CharmNestMonogramUI.mountLine === 'function'))) throw new Error('this page has no monogram widget (CharmNestMonogramUI.mountLine / widgets): run monogram-flow.cjs on the tree that carries the new monogram UI');
    await seedLibrary(); await reloadMaster();
    check(await page.evaluate(() => [...B.master.entries.keys()].filter(k => /^MONOGRAM_(LEFT|MID|RIGHT)_[A-Z]$/.test(k)).length) >= 78, 'the library holds the three alphabets (78 letter SKUs)');
    await instrument();      // (before the orders come in: the page builds a monogram quietly for a row's picture as soon as it can, and the staged verdicts of section 10 must be there for that build too)
    await page.evaluate(() => { window.__stage = new Set(['GWX', 'NFZ', 'HRY']); });
    made = await RW.build(w, ['apv', 'osr', 'jqx', 'sized', 'mixed', 'back', 'ai1', 'ai2', 'ai3'].concat(Object.keys(ODD)));

    // ═══ 1 · the orders ═══
    if (!ONLY || ONLY.has(1)) {
      console.log('\n── 1 · the orders');
      const specs = await page.evaluate(() => Orders.rows().map(r => ({ rid: r.order.receiptId, key: r.key, mono: !!(r.spec && r.spec.special && r.spec.special.monogram), words: r.spec && r.spec.special && r.spec.special.words, own: !!(r.spec && r.spec.ownDesign), problems: ((r.spec || {}).problems || []).length, engrave: !!(r.spec && r.spec.engraveCandidate), size: r.spec && r.spec.size, material: r.spec && r.spec.material })));
      const by = n => specs.filter(s => s.rid === rid(n));
      check(['apv', 'osr', 'jqx', 'sized', 'mixed'].every(n => by(n).some(s => s.mono)), 'five monogram orders are read as custom monograms (APV, O S R, J.Q.X, a sized one, and the monogram line of a mixed order)');
      check(by('apv')[0].words === 'APV' && by('osr')[0].words === 'O S R' && by('jqx')[0].words === 'J.Q.X' && by('apv')[0].own && by('apv')[0].problems === 0 && !by('apv')[0].engrave, 'APV / O S R / J.Q.X: the buyer\'s words as given, its own design, asks nothing, no engraving');
      check(by('mixed').length === 2 && by('mixed').filter(s => s.mono).length === 1 && by('back').every(s => !s.mono), 'a mixed order has one monogram line and one engraved bar; a plain engraved order has no monogram');
      check(by('sized')[0].size === '16MM' && by('apv')[0].size == null && by('apv')[0].material === 'gold' && by('osr')[0].material === 'silver', 'the size option (16MM) and the metal of each line are read: ' + JSON.stringify([by('sized')[0].size, by('apv')[0].material, by('osr')[0].material]));
      check(Object.keys(ODD).every(n => by(n).some(s => s.mono)), 'the odd buyer words (A.P, Jane Doe, ABCD, 123, emoji, Cyrillic) are still monogram lines of the listing table: a person types the letters');
    }

    // ═══ 2 · the Engrave tab ═══
    if (!ONLY || ONLY.has(2)) {
      console.log('\n── 2 · the Engrave tab');
      await openList(); await settle(2500);
      const tabs = await page.evaluate(() => [...document.querySelectorAll('#engraveView .egTab[data-tab]')].map(b => b.dataset.tab));
      check(!tabs.includes('mono') && !(await page.evaluate(() => !!document.querySelector('#egMono, #engraveView .egTab[data-tab="mono"]'))), 'there is no Monograms tab any more: ' + tabs.join(', '));
      const rows = await page.evaluate(() => [...document.querySelectorAll('#egQueue .doneRow')].map(r => ({ kind: r.dataset.kind || 'engraved', rid: r.dataset.rid, thumb: !!r.querySelector('[data-mw-thumb]'), faces: !!r.querySelector('.engFace, [data-face], .placementThumb'), text: r.innerText.replace(/\s+/g, ' ').trim() })));
      const mrow = rows.find(r => r.kind === 'monogram' && r.rid === rid('apv')), erow = rows.find(r => r.kind === 'engraved' && r.rid === rid('back'));
      check(rows.filter(r => r.kind === 'monogram').length >= 5 && mrow && mrow.thumb && /A P V/.test(mrow.text) && !mrow.faces && !/Front|Back/.test(mrow.text.replace(/Words on the back/ig, '')), 'Placements lists the monogram lines: APV shows its finished charm and its letters, never Front / Back: "' + (mrow && mrow.text.slice(0, 100)) + '"');
      check(erow && erow.faces && !erow.thumb, 'a plain engraved row is what it was (its faces, no monogram thumbnail)');
      await page.waitForFunction(r => { const t = document.querySelector(`#egQueue .doneRow[data-rid="${r}"] [data-mw-thumb]`); return t && t.querySelector('svg path'); }, rid('apv'), { timeout: 90000 }).catch(() => {});
      const thumb = await page.evaluate(r => { const t = document.querySelector(`#egQueue .doneRow[data-rid="${r}"] [data-mw-thumb]`); const p = t && t.querySelector('svg path'); return p ? { points: (p.getAttribute('d').match(/[ML]/g) || []).length, fill: p.getAttribute('fill') } : null; }, rid('apv'));
      check(thumb && thumb.points > (REAL_OK ? 1500 : 200), 'the row\'s picture is the cut piece of the letters (' + (thumb && thumb.points) + ' points)');
      await shot('1440-list');
      await openDetail('apv'); await ready(rid('apv'), 'detail');
      const x = await read(rid('apv'), 'detail');
      check(x && x.mode === 'detail' && x.ins.join('') === 'APV' && /A P V/.test(x.says), 'a click on the row opens ITS detail: the widget in full mode, the letters A P V: ' + JSON.stringify(x && [x.mode, x.ins, x.says]));
      check(await page.evaluate(() => { const c = document.querySelector('#egQueue .rvItem[data-mono]'); return !!c && !c.querySelector('.engThumb, .engFace, [data-face], .backHost, .engp'); }), 'and no Front / Back faces, no engraving panel in it');
      await shot('1440-detail-ready');
    }

    // ═══ 3 · letters recognised and fetched, the preview ═══
    if (!ONLY || ONLY.has(3)) {
      console.log('\n── 3 · letters, fetch, preview');
      await openDetail('apv'); const x = await ready(rid('apv'), 'detail');
      const f1 = await fetched();
      check(['MONOGRAM_LEFT_A', 'MONOGRAM_MID_P', 'MONOGRAM_RIGHT_V'].every(s => f1.includes(s)) && !f1.includes('MONOGRAM_LEFT_P') && !f1.includes('MONOGRAM_MID_A') && !f1.includes('MONOGRAM_RIGHT_A'), 'letter 1 came from the Left alphabet, 2 from the Middle, 3 from the Right: ' + [...new Set(f1)].filter(s => /_(A|P|V)$/.test(s)).sort().join(' '));
      check(x.state === 'ready' && x.points > 100 && x.subpaths >= 2 && !x.spin && x.picBusy === 'false', 'the preview is drawn (' + x.points + ' points, ' + x.subpaths + ' paths: the outline and its counters), no spinner');
      check(/^\d+\.\d × \d+\.\d mm/.test(x.size) || /\d+\.\d\s*[×x]\s*\d+\.\d\s*mm/.test(x.size), 'ONE calm size line: "' + x.size + '"');
      const t = await truthOf('APV'), s = (await states(rid('apv')))[0];
      if (t) {
        check(s.sizeMm && Math.abs(s.sizeMm[0] - t.wMm) < 0.1 && Math.abs(s.sizeMm[1] - t.hMm) < 0.1 && Math.abs(lettersH(t) - 14.8) < 0.5, `the preview is the geometry the engine cuts: ${s.sizeMm && s.sizeMm.map(v => v.toFixed(2)).join(' × ')} mm = the engine on Paul's letters ${t.wMm.toFixed(2)} × ${t.hMm.toFixed(2)} mm (the letters ${lettersH(t).toFixed(2)} mm tall, with the hoops)`);
        check(x.subpaths === t.rings.length && Math.abs(x.points - t.rings.reduce((a, g) => a + g.pts.length, 0)) <= 4, `the same rings: ${x.subpaths} paths and ${x.points} points = ${t.rings.length} and ${t.rings.reduce((a, g) => a + g.pts.length, 0)} (outline + ${t.rings.length - 1} counters)`);
      }
      check(x.size.includes('Gold') && s.metal === 'gold', 'the metal of the line is in the size line and in the state: ' + x.size);
      // the metal tone of the picture follows the line's metal (gold, silver, rose gold are three different tones)
      const tones = {}; for (const n of ['osr', 'jqx']) { await openDetail(n); const r = await ready(rid(n), 'detail'); tones[n] = r.fill; }
      tones.apv = x.fill;
      check(new Set([tones.apv, tones.osr, tones.jqx]).size === 3 && Object.values(tones).every(c => /^#[0-9a-f]{6}$/i.test(c)), 'gold, silver and rose gold are three different tones in the preview: ' + JSON.stringify(tones));
      check(x.stroke && /^#[0-9a-f]{6}$/i.test(x.stroke), 'one thin cut line round the outline: ' + x.stroke);
      await shot('1440-detail-osr');
    }

    // ═══ 4 · editing ═══
    if (!ONLY || ONLY.has(4)) {
      console.log('\n── 4 · editing a letter');
      await openDetail('apv'); await ready(rid('apv'), 'detail');
      const R0 = rid('apv'), D = 'detail';
      let b0 = (await builds()).length;
      await typeIn(R0, D, 1, 'S');
      let x = await read(R0, D);
      check(x.state === 'building' && x.spin && x.picBusy === 'true' && x.approve.disabled, 'a letter typed: the widget is busy at once (spinner on the picture, Approve waits): ' + x.state + '/' + x.busy);
      x = await ready(R0, D);
      check(x.state === 'ready' && x.ins.join('') === 'ASV' && !x.spin, 'the monogram was remade for A S V: ' + x.ins.join(''));
      const only = async (b, word) => (await builds()).slice(b).map(s => s.split('@')[0]).filter(s => s === word).length;      // (other orders' thumbnails build quietly now and then: count the word of this edit)
      check(await only(b0, 'ASV') === 1, 'ONE build for the edit: ' + (await builds()).slice(b0).join());
      // two quick edits: one remake of the last
      { const bb = (await builds()).length; await typeIn(R0, D, 1, 'T'); await typeIn(R0, D, 1, 'X'); x = await until(R0, D, v => v.ins.join('') === 'AXV' && v.state === 'ready' && !v.spin, 'AXV', 90000);
        check(x.ins.join('') === 'AXV' && await only(bb, 'AXV') === 1 && await only(bb, 'ATV') === 0, 'two quick edits make ONE monogram (the last): ' + (await builds()).slice(bb).join()); }
      // the page's progress bar while the letters are fetched (one letter file held back)
      holdOn(/Monogram_Mid_B\.ai/i); await typeIn(R0, D, 1, 'B');
      await page.waitForFunction(() => /Building monogram/.test((document.querySelector('.cnp.on') || {}).textContent || ''), null, { timeout: 8000 }).catch(() => {});
      x = await read(R0, D);
      check(x.state === 'building' && x.spin && x.busy === 'building', 'while a letter file is on its way the spinner stays: ' + x.state + '/' + x.busy);
      check(await page.evaluate(() => { const n = document.querySelector('.cnp.on'); return !!n && /Building monogram [A-Z]{3}/.test(n.textContent); }), 'and the page\'s progress bar says "Building monogram …"');
      check(x.d.length > 200, 'the old picture stays under the spinner (the widget never jumps)');
      await shot('1440-detail-building');
      release(); await ready(R0, D);
      check(await page.evaluate(() => !document.querySelector('.cnp.on')), 'the progress bar is gone when it is made');
      // a second edit while the first is still being fetched: only the newest is ever shown
      holdOn(/Monogram_Mid_K\.ai/i); const bs = (await builds()).length;
      await typeIn(R0, D, 1, 'K'); await sleep(900);
      check((await read(R0, D)).state === 'building', 'the first edit (K) is still being fetched');
      await typeIn(R0, D, 1, 'U'); const u = await ready(R0, D);
      check(u.state === 'ready' && u.ins.join('') === 'AUV', 'the second edit (U) finished first and is shown: ' + u.ins.join(''));
      release(); await sleep(1500);
      { const after = await read(R0, D), nb = (await builds()).slice(bs).map(s => s.split('@')[0]);
        check(after.d === u.d && after.ins.join('') === 'AUV' && after.state === 'ready' && !nb.includes('AKV'), 'the older edit finishing later never overwrites: same picture, no build for it: ' + nb.join()); }
      // swap and rotate
      await page.click(`${mwSel(R0, D)} [data-mw-swap="0"]`); x = await ready(R0, D);
      check(x.ins.join('') === 'UAV', 'swap Left and Middle: U A V: ' + x.ins.join(''));
      await page.click(`${mwSel(R0, D)} [data-mw-swap="1"]`); x = await ready(R0, D);
      check(x.ins.join('') === 'UVA', 'swap Middle and Right: U V A: ' + x.ins.join(''));
      { const seen = [x.ins.join('')]; for (let i = 0; i < 3; i++) { await page.click(`${mwSel(R0, D)} [data-mw-rotate]`); x = await ready(R0, D); seen.push(x.ins.join('')); }
        const cyc = new Set(['UVA', 'VAU', 'AUV']); check(seen[0] === seen[3] && seen.slice(0, 3).every(s => cyc.has(s)) && new Set(seen.slice(0, 3)).size === 3, 'rotate turns the three letters round and three presses are back where they began: ' + seen.join(' → ')); }
      // paste fills all three; Esc takes an edit back
      await pasteWord(R0, D, 'J.Q.P'); x = await ready(R0, D); check(x.ins.join('') === 'JQP' && x.state === 'ready', 'three letters pasted into any box fill all three: ' + x.ins.join(''));
      await typeIn(R0, D, 0, 'H'); await page.keyboard.press('Escape'); x = await ready(R0, D);
      check(x.ins.join('') === 'JQP' && x.state === 'ready', 'Esc in a letter box takes the edit back: ' + x.ins.join(''));
      await pasteWord(R0, D, 'APV'); x = await ready(R0, D);
      // a letter typed back is instant: no fetch, no build
      { await typeIn(R0, D, 1, 'S'); await ready(R0, D); const nb = (await builds()).length, nf = (await fetched()).length; const t1 = Date.now();
        await typeIn(R0, D, 1, 'P'); x = await until(R0, D, v => v.state === 'ready' && v.ins.join('') === 'APV' && !v.spin, 'APV again', 5000);
        check(x.state === 'ready' && x.ins.join('') === 'APV' && (await builds()).length === nb && (await fetched()).length === nf && Date.now() - t1 < 3000, `typing a letter back is instant: no build, no fetch (${Date.now() - t1} ms)`); }
      // not a letter: refused calmly
      { const nb = (await builds()).length; await page.click(`${mwSel(R0, D)} [data-mw-letter="1"]`); await page.keyboard.type('7'); x = await read(R0, D);
        check(x.ins.join('') === 'APV' && x.state === 'ready', 'a digit is refused: the box keeps its letter');
        await page.keyboard.type('😀'); x = await read(R0, D); check(x.ins.join('') === 'APV', 'an emoji too');
        await page.keyboard.press('Backspace'); x = await read(R0, D);
        check(x.state === 'letters' && x.ins[1] === '' && x.approve.disabled && x.invalid[1] && (await builds()).length === nb, 'an emptied box: the widget waits for the three letters (no build, Approve waits, the empty box is marked): ' + x.state);
        await shot('1440-detail-emptybox');
      check(/three letters|type the|letter/i.test((x.msg && x.msg.text) || x.text), 'and says so in plain words: "' + ((x.msg && x.msg.text) || x.text).slice(0, 160) + '"');
        await typeIn(R0, D, 1, 'p'); x = await ready(R0, D); check(x.ins.join('') === 'APV' && x.state === 'ready', 'a small letter is made a capital'); }
      check(x.leaks.length === 0, 'nothing marked hidden is showing: ' + x.leaks.join(' '));
    }

    // ═══ 5 · trouble: a missing letter, a failed load, odd words ═══
    if (!ONLY || ONLY.has(5)) {
      console.log('\n── 5 · trouble');
      await openDetail('apv'); await ready(rid('apv'), 'detail'); const R0 = rid('apv'), D = 'detail';
      const midY = st.docs.get('Charm_Master_Index/MONOGRAM_MID_Y');
      st.docs.delete('Charm_Master_Index/MONOGRAM_MID_Y'); await reloadMaster();
      const nb = (await builds()).length;
      await typeIn(R0, D, 1, 'Y'); let x = await until(R0, D, v => v.state === 'error', 'the missing-letter message');
      check(x.state === 'error' && x.msg && x.msg.kind === 'missing' && /Middle alphabet has no “Y”/.test(x.msg.text) && /has no Y yet/.test(x.msg.text) && !/MONOGRAM_MID_Y/.test(x.msg.text), 'a letter missing from an alphabet is a plain message naming it: "' + (x.msg && x.msg.text) + '"');
      check(!x.spin && x.picBusy === 'false' && x.approve.disabled && !(await builds()).slice(nb).some(s => s[1] === 'Y'), 'no spinner, Approve waits, nothing built with the missing letter');
      await shot('1440-detail-missing');
      st.docs.set('Charm_Master_Index/MONOGRAM_MID_Y', midY); await reloadMaster();
      await typeIn(R0, D, 1, 'P'); x = await ready(R0, D); check(x.state === 'ready' && !x.msg, 'typing another letter takes it out of the error');
      // a load that fails: Try again
      blobFail = /Monogram_Mid_Z\.ai/i; await typeIn(R0, D, 1, 'Z');
      x = await until(R0, D, v => v.state === 'error', 'the load failure', 60000);
      check(x.msg && x.msg.kind === 'load' && x.retry && !x.spin, 'a letter file that does not load: a plain message and Try again, no stuck spinner: "' + (x.msg && x.msg.text) + '"');
      check(await page.evaluate(() => !document.querySelector('.cnp.on')), 'and the page\'s progress bar is gone');
      blobFail = null; await page.click(`${mwSel(R0, D)} [data-mw-retry]`); x = await ready(R0, D, 60000);
      check(x.state === 'ready' && x.ins.join('') === 'AZV' && !x.msg, 'Try again loads it and the monogram is made: ' + x.ins.join(''));
      await typeIn(R0, D, 1, 'P'); await ready(R0, D);
      // odd words
      for (const [n, [words, want, boxes]] of Object.entries(ODD)) {
        await openDetail(n); const r = await until(rid(n), 'detail', v => v.state === 'letters' || v.state === 'ready', 'the odd order ' + n, 30000);
        check(r && r.state === 'letters' && r.msg && r.msg.kind === 'letters' && want.test(r.msg.text) && r.ins.join('|') === boxes.join('|') && r.approve.disabled && !r.spin, `the buyer's "${words}": ${r && r.msg && r.msg.text} (boxes ${r && r.ins.join('|')})`);
      }
      check((await Promise.all(Object.keys(ODD).map(async n => (await states(rid(n))).every(v => v.state === 'letters' && !v.sizeMm)))).every(Boolean), 'and nothing was built for any of them (state "letters", no size)');
      await openDetail('odd2'); await shot('1440-detail-odd');
      await typeIn(rid('odd2'), 'detail', 2, 'v'); x = await ready(rid('odd2'), 'detail', 90000);
      check(x.state === 'ready' && x.ins.join('') === 'APV', 'for "A.P" a person adds the third letter and the monogram is made');
    }

    // ═══ 6 · the piece size ═══
    if (!ONLY || ONLY.has(6)) {
      console.log('\n── 6 · the piece size');
      await openDetail('sized'); const x = await ready(rid('sized'), 'detail', 90000), s = (await states(rid('sized')))[0];
      check(s.fit && s.fit.maxWidthMm === 16 && s.sizeMm && s.sizeMm[0] <= 16.01, `the order's size option 16MM is the envelope: the piece is ${s.sizeMm && s.sizeMm.map(v => v.toFixed(2)).join(' × ')} mm and never wider than 16 mm (${JSON.stringify(s.fit)})`);
      check(/16/.test(x.size) && /\d+\.\d × \d+\.\d mm/.test(x.size), 'the size line says so: "' + x.size + '"');
      const f = await truthOf('APV', { fit: { maxWidthMm: 16 } });      // (MONOHOOPS: the engine fits the letters, then puts the hoops on at their real size)
      if (f) { check(Math.abs(s.sizeMm[0] - f.wMm) < 0.1 && Math.abs(s.sizeMm[1] - f.hMm) < 0.1, `the same as the engine fitted to 16 mm: ${f.wMm.toFixed(2)} × ${f.hMm.toFixed(2)} mm`); }
      check((await builds()).some(b => b.endsWith('@16')), 'the build was given the envelope (opts.fit 16 mm)');
      const apv = (await states(rid('apv')))[0] || {}; await openDetail('apv'); const a2 = (await states(rid('apv')))[0];
      check(a2 && !a2.fit, 'an order without a size is the natural piece (no envelope): ' + JSON.stringify(a2 && a2.fit));
      await shot('1440-detail-sized'); void apv;
    }

    // ═══ 7 · Approve, the design on the order and on the sheets, replace ═══
    // MONOSEND (Paul, 18 Oct 2026: "remove the 'Send to sheet' button and replace it with the same green 'Approve' button we use for the engravings ... Once the user presses the 'Approve' button on a given Monogram order then that
    // particular Monogram should automatically be added to the Nesting queue"): ONE press approves (the design, the seal) AND sends it, on the buyer's metal, with no designs window; on the sheet it is final. A metal the order does not
    // give is never guessed: the designs window opens so a person picks it (the 'sized' order is staged so), and Change / Replace is shown there, before it is on a sheet.
    if (!ONLY || ONLY.has(7)) {
      console.log('\n── 7 · Approve');
      const decided = k => page.evaluate(k => { const row = B.orders.byKey.get(k), d = row && CustomSheet.decisionOf(row); return d ? { pieces: (d.lines[k] || []).filter(p => !p.removed).length, by: d.by } : null; }, k);
      await openDetail('apv'); let x = await ready(rid('apv'), 'detail'); const R0 = rid('apv');
      check(x.state === 'ready' && x.ins.join('') === 'APV' && x.approve && x.approve.shown && !x.approve.disabled && x.approve.text === 'Approve' && !x.sheet, 'ready to approve: the button says Approve, and there is no "Send to Sheet →" link');
      check(await page.evaluate(r => { const b = document.querySelector(`.mw[data-rid="${r}"][data-mode="detail"] [data-mw-approve]`); return !!b && b.classList.contains('sage'); }, R0), 'it is the engravings\' green Approve (btn sage)');
      check((await files(R0)).length === 0, 'no design on the order yet');
      await page.click(`${mwSel(R0, 'detail')} [data-mw-approve]`);
      await page.waitForFunction(k => !!CustomSheet.decisionOf(B.orders.byKey.get(k)), keyOf('apv'), { timeout: 60000 }).catch(() => {});
      await readyFiles(); let fl = await files(R0);
      check(fl.length === 1 && fl[0].name === 'Monogram APV.ai', 'Approve puts Monogram APV.ai on the order\'s custom designs: ' + JSON.stringify(fl.map(f => f.name)));
      const t = await truthOf('APV');
      check(fl[0].state === 'ready' && fl[0].pieces === 1 && !fl[0].error, 'the page\'s cut-file preflight read it as ONE closed piece: ' + JSON.stringify({ state: fl[0].state, pieces: fl[0].pieces, error: fl[0].error }));
      check(t && Math.abs(fl[0].wMm - t.wMm) < 0.3 && Math.abs(fl[0].hMm - t.hMm) < 0.3, `of the previewed size: ${fl[0].wMm.toFixed(2)} × ${fl[0].hMm.toFixed(2)} mm (the engine: ${t && t.wMm.toFixed(2)} × ${t && t.hMm.toFixed(2)})`);
      const dA = await decided(keyOf('apv'));
      check(dA && dA.pieces === 1 && fl[0].metal === 'gold' && fl[0].qty === 1, 'and the same press sent it to the nesting queue: ONE piece, on the buyer\'s metal (GF): ' + JSON.stringify({ d: dA, metal: fl[0].metal, qty: fl[0].qty }));
      check(await page.evaluate(() => !document.querySelector('#cuDlg[open]')), 'the designs window did not open over the page');
      // the Engrave detail moves on (a one-piece order: back to the list or on); look at the widget wherever it is, in both places
      await openDetail('apv'); const inDetail = await ready(R0, 'detail');
      check(inDetail && inDetail.state === 'sent' && inDetail.approve && !inDetail.approve.shown && /On the sheet · Monogram APV/.test(inDetail.badge) && !inDetail.change && inDetail.insDisabled.every(Boolean), 'the Engrave detail says On the sheet · Monogram APV, final: no Approve, no Change, the boxes locked: ' + JSON.stringify(inDetail && [inDetail.state, inDetail.badge, inDetail.change]));
      { const nb = (await allNames()).join('|'); await page.evaluate(r => { const w = CharmNestMonogramUI.widgets().find(x => x.state().rid === r); return w.approve(); }, R0); await sleep(500);
        check((await allNames()).join('|') === nb && JSON.stringify(await decided(keyOf('apv'))) === JSON.stringify(dA), 'a second press changes nothing (the same design, the same one-piece decision)'); }
      await openWin('apv'); let c = await read(R0, 'compact');
      check(c && c.state === 'sent' && c.ins.join('') === 'APV' && /Monogram APV/.test(c.badge) && !c.change && c.approve && !c.approve.shown, 'the Order details window shows the same design on the sheet (compact): ' + JSON.stringify(c && [c.state, c.badge, c.change]));
      await shot('1440-orderwin-approved'); await closeWin();
      // a metal the order does not give ('sized', staged): approved, not sent, the designs window opens to pick it; Change, then a new letter REPLACES the design (the metal a person set is kept) and that press sends it
      const RS = rid('sized'), kS = keyOf('sized');
      await page.evaluate(k => { const r = B.orders.byKey.get(k); r.line.metalKey = 'moonlight'; r.spec.material = null; }, kS);
      await openDetail('sized'); await ready(RS, 'detail', 90000);
      await page.click(`${mwSel(RS, 'detail')} [data-mw-approve]`);
      await page.waitForSelector('#cuDlg[open] .cuFile', { timeout: 30000 }).catch(() => {});
      fl = await files(RS);
      check(await page.evaluate(() => !!document.querySelector('#cuDlg[open]') && /no metal on the order/.test(document.querySelector('#cuDlgSub').textContent)) && fl.length === 1 && !fl[0].metal && !await decided(kS), 'with no metal on the order it is approved but not sent, never guessed: the designs window opens (no metal on the order): ' + JSON.stringify(fl.map(f => [f.name, f.metal])));
      await page.click('#cuDlg .cuFile .cuM[data-m="silver"]'); await page.click('#cuDlg [data-x]'); await sleep(600);
      await openWin('sized'); await page.click(`${mwSel(RS, 'compact')} [data-mw-change]`);
      await page.waitForFunction(r => { const h = document.querySelector(`.mw[data-rid="${r}"][data-mode="compact"] [data-mw-letter="1"]`); return h && !h.disabled; }, RS, { timeout: 10000 }).catch(() => {});
      await typeIn(RS, 'compact', 1, 'S'); c = await ready(RS, 'compact', 90000);
      check(c.ins.join('') === 'ASV' && c.approve && c.approve.shown && /Replace approved/.test(c.approve.text) && !c.approve.disabled, 'after Change and a new letter the button says Replace approved: ' + JSON.stringify([c.ins.join(''), c.approve && c.approve.text, c.state]));
      check((await files(RS)).map(f => f.name).join() === 'Monogram APV.ai', 'until it is pressed the approved design is untouched');
      await page.click(`${mwSel(RS, 'compact')} [data-mw-approve]`);
      await page.waitForFunction(k => !!CustomSheet.decisionOf(B.orders.byKey.get(k)), kS, { timeout: 60000 }).catch(() => {}); await sleep(600); await readyFiles();
      fl = await files(RS);
      check(fl.length === 1 && fl[0].name === 'Monogram ASV.ai', 'the approved design is REPLACED, never two: ' + JSON.stringify(fl.map(f => f.name)));
      check(fl[0].metal === 'silver' && fl[0].state === 'ready' && fl[0].pieces === 1 && (await decided(kS) || {}).pieces === 1, 'the metal a person set carries over to it, and that press sent it (one piece): ' + fl[0].metal);
      const nbef = (await allNames()).join('|'); await page.evaluate(r => { const w = CharmNestMonogramUI.widgets().find(x => x.state().rid === r); return w.approve(); }, RS); await sleep(500);
      check((await allNames()).join('|') === nbef, 'approving the same letters again changes nothing');
      // the other place follows: the Engrave detail of the same line shows the replaced design
      await closeWin(); await openDetail('sized'); const d2 = await ready(RS, 'detail');
      check(d2.state === 'sent' && d2.ins.join('') === 'ASV' && /Monogram ASV/.test(d2.badge), 'the Engrave detail shows the replaced design too (A S V, on the sheet): ' + d2.badge);
      // another order's monogram is its own design
      await openDetail('osr'); await ready(rid('osr'), 'detail'); await page.click(`${mwSel(rid('osr'), 'detail')} [data-mw-approve]`);
      await page.waitForFunction(k => !!CustomSheet.decisionOf(B.orders.byKey.get(k)), keyOf('osr'), { timeout: 60000 }).catch(() => {}); await sleep(600);
      check((await allNames()).join('|') === 'Monogram APV.ai|Monogram ASV.ai|Monogram OSR.ai', 'a second order\'s monogram is its own design: ' + (await allNames()).join(' | '));
    }

    // ═══ 8 · the Order details window ═══
    if (!ONLY || ONLY.has(8)) {
      console.log('\n── 8 · the Order details window');
      await openDetail('jqx'); await ready(rid('jqx'), 'detail');
      await openWin('jqx'); const c = await ready(rid('jqx'), 'compact', 60000);
      const sec = await page.evaluate(r => { const o = document.querySelector('#orderWin [data-testid="ow-mono"]'); return { mono: !!o, cards: document.querySelectorAll('#orderWin .engCard').length, faces: [...document.querySelectorAll('#orderWin [data-testid="ow-eng-sec"] .engFace, #orderWin [data-testid="ow-eng-sec"] [data-face], #orderWin #owEng .engFace, #orderWin #owEng [data-face]')].map(e => e.tagName + '.' + String(e.className).split(' ')[0] + (e.dataset.face ? '[' + e.dataset.face + ']' : '') + '<' + (e.parentElement && String(e.parentElement.className).split(' ')[0]) + '>').slice(0, 4), rid: o && o.dataset.rid, widgets: (CharmNestMonogramUI.widgets() || []).filter(x => x.state().rid === r).map(x => x.mode).sort().join() }; }, rid('jqx'));
      check(c && c.mode === 'compact' && sec.mono && sec.cards === 0 && sec.faces.length === 0, 'the window\'s engraving section holds the monogram (compact), no Front / Back cards: ' + JSON.stringify(sec));
      check(sec.widgets.includes('compact'), 'two mounts of one line can be open together (' + sec.widgets + ')');
      // typing in the window shows in the detail behind it: one shared state
      const b0 = (await builds()).length; await typeIn(rid('jqx'), 'compact', 2, 'N');
      const cn = await until(rid('jqx'), 'compact', v => v.ins.join('') === 'JQN' && v.state === 'ready' && !v.spin, 'JQN in the window', 90000);
      const dn = await read(rid('jqx'), 'detail');
      check(cn.ins.join('') === 'JQN' && (!dn || (dn.ins.join('') === 'JQN' && dn.d === cn.d)), 'typing in the window shows in the Engrave detail too (one state, one build): ' + (dn ? dn.ins.join('') : 'detail closed') + ', ' + (await builds()).slice(b0).join());
      check((await builds()).slice(b0).map(s => s.split('@')[0]).filter(s => s === 'JQN').length === 1, 'and the monogram was built once for the two places');
      await shot('1440-orderwin-jqn'); await shotEl('#orderWin [data-testid="ow-mono"]', 'orderwin-mono-1440');
      await typeIn(rid('jqx'), 'compact', 2, 'X'); await ready(rid('jqx'), 'compact', 90000);
      await closeWin();
      // a mixed order: the monogram line is the widget, the engraved bar keeps its card
      await openWin('mixed'); await ready(rid('mixed'), 'compact', 90000);
      const mx = await page.evaluate(() => ({ mono: document.querySelectorAll('#orderWin [data-testid="ow-mono"]').length, cards: document.querySelectorAll('#orderWin .engCard').length, text: (document.querySelector('#orderWin [data-testid="ow-eng"]') || {}).innerText }));
      check(mx.mono === 1 && mx.cards >= 1, 'a mixed order: one monogram widget and the engraved bar\'s own card: ' + JSON.stringify({ mono: mx.mono, cards: mx.cards }));
      await shot('1440-orderwin-mixed'); await closeWin();
      await RW.openOrderWindow(w, 'back'); const plain = await page.evaluate(() => ({ mw: document.querySelectorAll('#orderWin .mw').length, cards: document.querySelectorAll('#orderWin .engCard').length }));
      check(plain.mw === 0 && plain.cards >= 1, 'a plain engraved order has no monogram widget and its engraving cards as before: ' + JSON.stringify(plain)); await closeWin();
    }

    // ═══ 9 · on the sheets from its own Approve, reload ═══
    if (!ONLY || ONLY.has(9)) {
      console.log('\n── 9 · on the sheets from its own Approve');
      const R0 = rid('osr');
      let ap = (await files(R0)).map(f => f.name);
      await page.evaluate(() => { window.__dlgSeen = 0; if (!window.__dlgSpy) { window.__dlgSpy = true; const o = CustomSheet.open; CustomSheet.open = function () { window.__dlgSeen++; return o.apply(this, arguments); }; } });
      if (!ap.length) { await openDetail('osr'); await ready(R0, 'detail'); await page.click(`${mwSel(R0, 'detail')} [data-mw-approve]`); await sleep(1500); await readyFiles(); ap = (await files(R0)).map(f => f.name); }
      check(ap.join() === 'Monogram OSR.ai', 'OSR has its design on the order: ' + ap.join());
      // MONOROUTE (Paul, 17 Oct 2026: "Monogram orders should be treated exactly like back engraving and should be routed to the Engraving tab and NOT the Review tab unless something else is wrong"): an approved monogram is
      // no card of the Review tab; MONOSEND (Paul, 18 Oct 2026): it is sent from where it is made, by its own Approve (no "Send to Sheet →" link, no designs window)
      await page.evaluate(() => { CN.setMode('review'); Review.render(); }); await sleep(700);
      check(await page.evaluate(r => !document.querySelector(`#rvList [data-rid="${r}"]`) && !document.querySelector('#rvList [data-cu-mono]'), R0), 'the Review tab does not list the approved monogram (no card, no Build monogram)');
      check(await page.evaluate(k => { const row = B.orders.byKey.get(k), it = Review.customItemFor(row.key, row), c = it && CustomSheet.cardOf(it); return !!c && c.files.map(f => f.name).join() === 'Monogram OSR.ai'; }, keyOf('osr')), 'though its custom card exists and holds the design (Review.customItemFor)');
      await openDetail('osr'); const x = await read(R0, 'detail');
      check(x && !x.sheet && !await page.evaluate(r => !!document.querySelector(`.mw[data-rid="${r}"] [data-mw-sheet]`), R0), 'the widget has no "Send to Sheet →" link');
      await page.click('[data-role="laser"]', { timeout: 2500 }).catch(() => {});
      await page.waitForFunction(k => CustomSheet.decisionOf(B.orders.byKey.get(k)), keyOf('osr'), { timeout: 60000 }).catch(() => {});
      await page.waitForFunction(() => !window.SendTour || !SendTour.playing(), null, { timeout: 40000 }).catch(() => {}); await sleep(900);
      const sent = await page.evaluate(k => { const row = B.orders.byKey.get(k), d = CustomSheet.decisionOf(row); return { decided: !!d, pooled: (row.poolIds || []).length, state: row.state }; }, keyOf('osr'));
      check(sent.decided && await page.evaluate(() => !window.__dlgSeen && !document.querySelector('#cuDlg[open]')), 'its Approve put the monogram on the sheets, with no designs window: ' + JSON.stringify(sent));
      const pcs = await page.evaluate(r => { const e = Object.values(CustomSheet.entries()).find(e => String(e.rid) === r); return e && e.sent ? Object.values(e.sent.lines).reduce((a, l) => a + l.filter(p => !p.removed).length, 0) : -1; }, R0);
      check(pcs === 1, 'the record of the send holds exactly ONE piece for the monogram (one connected charm, one quantity): ' + pcs);
      await openDetail('osr').catch(() => {}); let s = await read(R0, 'detail');
      if (!s) { await openList(); s = null; }
      const st2 = (await states(R0))[0];
      check(st2 && st2.state === 'sent' || (s && s.state === 'sent'), 'the widget says the design is on the sheets: ' + JSON.stringify(st2 && [st2.state, st2.sent]));
      if (s) check(s.insDisabled.every(Boolean) && s.approve && !s.approve.shown && !s.change, 'and it is read only: boxes locked, no Approve, no Change');
      // a reload (a second tab) keeps the approved designs
      await page.evaluate(async () => { try { await Session.flushNow(); } catch (_) { /* nothing to save */ } });
      const apvNow = (await entries()).map(e => e.files.map(f => f.name).join()).sort();
      const page2 = await w.context.newPage(); page2.setDefaultTimeout(30000); page2.on('pageerror', e => w.errors.push('page2: ' + e.message));
      await page2.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
      await page2.waitForFunction(() => window.CN && window.Orders && window.CustomSheet && CN.S.cloud.ok === true, null, { timeout: 60000 });
      await page2.evaluate(async () => { await Master.load(true); });
      await page2.evaluate(async ({ fixtures, orderSrc }) => {
        const orderOf = eval('(' + orderSrc + ')'), now = Date.now(); await Orders.loadMaps(true);
        for (const fx of fixtures) { const order = orderOf(fx, now); for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); if (B.orders.byKey.get(key)) continue; const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); } }
        Orders.interpretAll(); try { await CustomSheet.load(); } catch (_) { /* nothing saved */ }
      }, { fixtures: ['apv', 'osr', 'sized'].map(n => O[n]), orderSrc: orderOf.toString() });
      await sleep(1500);
      const p2 = await page2.evaluate(() => Object.values(CustomSheet.entries()).map(e => e.files.map(f => f.name).join()).sort());
      check(p2.join('|') === apvNow.join('|') && p2.length >= 1, 'a second tab (a reload reads the same workspace) has the approved designs: ' + p2.join(' | ') + ' (first tab: ' + apvNow.join(' | ') + ')');
      await page2.close();
    }

    // ═══ 10 · the AI line ═══
    if (!ONLY || ONLY.has(10)) {
      console.log('\n── 10 · the AI line (stubbed agent)');
      const IDENT = { scale: 1, sx: 1, dx: 0, dy: 0 }, ans = (dx, why) => ({ adjust: { L: { scale: 1, sx: 1, dx, dy: 0 }, M: IDENT, R: IDENT }, why });
      const queue = (word, items) => page.evaluate(({ word, items }) => { (window.__answers[word] = window.__answers[word] || []).push(...items); }, { word, items });
      const calls = () => page.evaluate(() => window.__calls.filter(c => c.mode === 'monoAssist').length);
      // the page's agent call: every call recorded, a monoAssist answered from the queue, nothing leaves the page
      await page.evaluate(() => CN.setAgent(async (mode, payload, opts) => {
        window.__calls.push({ mode, payload, opts }); if (mode !== 'monoAssist') return { skipped: 'not part of this test' };
        const q = window.__answers[payload.letters.join('')], next = q && q.shift(); if (next === 'throw') throw new Error('network down'); return next || { skipped: 'nothing queued' };
      }));
      await queue('GWX', [{ adjust: { L: { scale: 9, sx: 1, dx: 50, dy: 0 }, M: IDENT, R: IDENT }, why: 'Moved the G toward the W so a lower contact joins them.', model: 'fake', usage: {} }]);
      await queue('NFZ', [ans(-1.0, 'tried left'), ans(-0.5, 'tried less'), ans(0.1, 'tried a nudge')]);
      await queue('HRY', ['throw']);
      await openDetail('ai1'); let x = await until(rid('ai1'), 'detail', v => v.asm && /Assembled with AI/.test(v.asm.text) && !v.spin, 'GWX assembled', 120000);
      if (!x.asm) console.log('   (diagnosis: staged builds ' + await page.evaluate(() => window.__staged) + ', agent calls ' + await calls() + ', settings.mono ' + await page.evaluate(() => JSON.stringify((S.settings || {}).mono || null)) + ', calls ' + JSON.stringify(await page.evaluate(() => window.__calls.map(c => [c.mode, c.payload.letters && c.payload.letters.join(''), c.payload.round]))) + ', service state ' + JSON.stringify(await states(rid('ai1'))) + ')');
      check(x.asm && x.asm.status === 'assembled' && /Assembled with AI: Moved the G toward the W/.test(x.asm.text) && x.state === 'ready', 'a word that breaks a rule is assembled with the AI and the widget says ONE calm line: "' + (x.asm && x.asm.text) + '"');
      const c1 = await page.evaluate(() => window.__calls.find(c => c.mode === 'monoAssist' && c.payload.letters.join('') === 'GWX'));
      check(c1 && c1.mode === 'monoAssist' && c1.opts.background === true && c1.payload.round === 1 && /^data:image\/png;base64,/.test(c1.payload.image), 'the call is mode monoAssist, in the background, round 1, with the candidate as a PNG');
      check(x.approve && x.approve.shown && !x.approve.disabled && x.approve.text === 'Approve' && !x.again, 'the widget is ready, Approve is one press, no "Improve again" on a word that passes now');
      await openDetail('ai2'); x = await until(rid('ai2'), 'detail', v => v.asm && /Needs a person/.test(v.asm.text) && !v.spin, 'NFZ needs a person', 180000);
      const nfz = await page.evaluate(() => window.__calls.filter(c => c.mode === 'monoAssist' && c.payload.letters.join('') === 'NFZ'));
      check(nfz.length === 3 && nfz.map(c => c.payload.round).join() === '1,2,3', 'never more than 3 rounds for a word: exactly 3 calls, rounds 1, 2, 3 (' + nfz.length + ')');
      check(x.asm && x.asm.status === 'needs' && /Needs a person: the left and middle letters are not joined at both the top and the bottom/.test(x.asm.text) && x.state === 'check' && x.again, 'the widget says "Needs a person:" naming the limit, offers Improve again: ' + (x.asm ? x.asm.text.slice(0, 140) : 'no line'));
      await shot('1440-detail-needs');
      await page.click(`${mwSel(rid('ai2'), 'detail')} [data-mw-approve]`); x = await until(rid('ai2'), 'detail', v => v.approve && v.approve.text === 'Approve anyway', 'confirm');
      check(x.approve.text === 'Approve anyway' && (await files(rid('ai2'))).length === 0, 'the first press only asks "Approve anyway" (nothing approved yet)');
      await page.click(`${mwSel(rid('ai2'), 'detail')} [data-mw-approve]`);
      await page.waitForFunction(r => { const h = document.querySelector(`.mw[data-rid="${r}"]`); return !h || h.dataset.state === 'approved'; }, rid('ai2'), { timeout: 60000 }).catch(() => {}); await sleep(600);
      check((await files(rid('ai2'))).map(f => f.name).join() === 'Monogram NFZ.ai', 'the second press approves it: Monogram NFZ.ai');
      // Improve again with the day's 3 calls used: no call
      await openDetail('ai2'); const c0 = await calls(); x = await read(rid('ai2'), 'detail');
      if (x && x.again) { await page.click(`${mwSel(rid('ai2'), 'detail')} [data-mw-again]`); await sleep(800); x = await read(rid('ai2'), 'detail'); }
      check(await calls() === c0 && (!x || !x.asm || /3 tries|Needs a person|limit/i.test(x.asm.text)), 'Improve again after the 3 calls of the day: no call is made' + (x && x.asm ? ': "' + x.asm.text.slice(0, 100) + '"' : ''));
      await openDetail('ai3'); x = await until(rid('ai3'), 'detail', v => v.asm && /could not help|Needs a person/.test(v.asm.text) && !v.spin, 'HRY failed', 120000);
      check(x.state === 'check' && x.points > 100 && x.asm && /Needs a person/.test(x.asm.text) && /could not help/.test(x.asm.text), 'a failing call leaves the automatic monogram in place, says so, and still lets a person decide: "' + (x.asm ? x.asm.text.slice(0, 160) : 'no line') + '"');
      await page.evaluate(() => { window.__stage = new Set(); });
      check((await page.evaluate(() => window.__calls.every(c => ['monoAssist', 'engraveIntent'].includes(c.mode)))), 'every agent call was the stubbed monoAssist: no real call anywhere');
    }

    // ═══ 11 · phone width, motion, console ═══
    if (!ONLY || ONLY.has(11)) {
      console.log('\n── 11 · phone width 400, motion, console');
      await page.setViewportSize({ width: 400, height: 900 }); await settle(600);
      if (await page.evaluate(() => !document.getElementById('app').classList.contains('railOff'))) { await page.click('#btnRail').catch(() => {}); await settle(500); }
      await openList(); await settle(1800);
      let o = await overflow('#egQueue .doneRow[data-kind=monogram]');
      check(o && o.n >= 5 && o.page <= 1 && o.body <= 1 && o.bad.length === 0 && o.left >= -1 && o.right <= o.vw + 1, 'phone width, the Engrave list: no sideways scroll, every monogram row inside the screen and its own box ' + JSON.stringify(o));
      await shot('400-list');
      await openDetail('jqx'); const x = await ready(rid('jqx'), 'detail', 90000);
      o = await overflow('#egQueue .rvItem[data-mono]');
      check(o && o.page <= 1 && o.body <= 1 && o.bad.length === 0 && o.left >= -1 && o.right <= o.vw + 1, 'phone width, the Engrave detail: the card, its three letter boxes and its buttons fit ' + JSON.stringify(o));
      const boxes = await page.evaluate(r => [...document.querySelectorAll(`.mw[data-rid="${r}"][data-mode="detail"] [data-mw-letter]`)].map(i => { const b = i.getBoundingClientRect(); return [Math.round(b.left), Math.round(b.right), Math.round(b.height)]; }), rid('jqx'));
      check(boxes.length === 3 && boxes.every(b => b[0] >= 0 && b[1] <= 400 && b[2] >= 30), 'the three letter boxes are inside the screen and big enough to tap: ' + JSON.stringify(boxes));
      const ap = await page.evaluate(r => { const b = document.querySelector(`.mw[data-rid="${r}"][data-mode="detail"] [data-mw-approve]`); const rc = b && b.getBoundingClientRect(); return rc ? [Math.round(rc.left), Math.round(rc.right), Math.round(rc.height)] : null; }, rid('jqx'));
      check(ap && ap[0] >= 0 && ap[1] <= 400 && ap[2] >= 30, 'and so is Approve: ' + JSON.stringify(ap));
      await shot('400-detail');
      holdOn(/Monogram_Mid_H\.ai/i); await typeIn(rid('jqx'), 'detail', 1, 'H'); await sleep(700); await shot('400-detail-building'); o = await overflow('#egQueue .rvItem[data-mono]');
      check(o && o.page <= 1 && o.bad.length === 0, 'and while it is being made (spinner, bar): still fits ' + JSON.stringify(o)); release(); await ready(rid('jqx'), 'detail', 90000);
      const widest = () => page.evaluate(() => { const w = document.querySelector('#orderWin'), wr = w.getBoundingClientRect(); return [...w.querySelectorAll('*')].map(e => ({ n: (e.dataset && e.dataset.testid) || (e.id ? '#' + e.id : '') || (e.className && e.className.baseVal === undefined ? '.' + String(e.className).split(' ')[0] : e.tagName), r: Math.round(e.getBoundingClientRect().right - wr.left) })).sort((a, b) => b.r - a.r).slice(0, 3).map(x => x.n + '@' + x.r); });
      const unscroll = () => page.evaluate(() => { for (let e = document.querySelector('#orderWin'); e; e = e.parentElement) e.scrollLeft = 0; for (const e of document.querySelectorAll('#orderWin *')) if (e.scrollLeft) e.scrollLeft = 0; });
      await RW.openOrderWindow(w, 'back'); await unscroll(); const plainWin = await overflow('#orderWin'), plainWide = await widest(); await closeWin();       // (what the window itself does at this width for an order with no monogram: the yardstick)
      // (MONOSEAL: an approved monogram's seals lie across the card's top-right corner, as the engraving cards' do, up to 8 px past its right edge: the card's own overflow may be that much, no more)
      await openWin('apv'); await unscroll(); await sleep(300); o = await overflow('#orderWin'); const oc = await overflow('#orderWin [data-testid="ow-mono"]'), apvWide = await widest();
      check(o && oc && o.page <= 1 && o.body <= 1 && oc.bad.length === 0 && oc.own <= 14 && oc.n === 1 && oc.left >= -1 && oc.right <= oc.vw + 1, 'phone width, the Order details window: the compact widget fits its box and the screen, the page does not scroll sideways ' + JSON.stringify({ mono: oc, page: o && o.page }));
      // (not checked, only said: the window's own HEADER (.owRight with the close button) is wider than a 400 px screen on origin/main too, for plain orders as well, and scrolls the window sideways: it is not the widget's)
      console.log(`   (the Order window's own sideways scroll at 400 px, not the widget's: ${o && o.own} px with the monogram order, ${plainWin && plainWin.own} px with a plain one; widest: ${apvWide.join(' ')} / ${plainWide.join(' ')})`);
      await shot('400-orderwin'); await shotEl('#orderWin [data-testid="ow-mono"]', 'orderwin-mono-400'); await closeWin();
      await openWin('mixed'); o = await overflow('#orderWin'); const om = await overflow('#orderWin [data-testid="ow-mono"], #orderWin .engCard');
      check(o && om && o.page <= 1 && o.body <= 1 && om.bad.length === 0, 'phone width, a mixed order\'s window: the monogram widget and the bar\'s card fit ' + JSON.stringify({ cards: om, page: o && o.page })); await shot('400-orderwin-mixed'); await closeWin();
      await page.setViewportSize({ width: 1440, height: 900 }); await settle(500);
      // reduced motion: the spinner still shows but does not spin
      await page.emulateMedia({ reducedMotion: 'reduce' }); await openDetail('jqx');
      holdOn(/Monogram_Mid_G\.ai/i); await typeIn(rid('jqx'), 'detail', 1, 'G'); await sleep(500);
      const rm = await page.evaluate(r => { const i = document.querySelector(`.mw[data-rid="${r}"][data-mode="detail"] [data-mw-spin] i`); return i ? getComputedStyle(i).animationName : 'no spinner'; }, rid('jqx'));
      check(rm === 'none', 'reduced motion: the spinner does not spin (animation: ' + rm + ')');
      release(); await ready(rid('jqx'), 'detail', 90000); await page.emulateMedia({ reducedMotion: 'no-preference' });
      void x;
    }
  } catch (e) { fails.push('crash: ' + (e && e.stack || e)); console.error(e); try { await shot('crash'); } catch (_) { /* no page */ } }
  finally {
    check(w.errors.length === 0, 'no page error anywhere: ' + JSON.stringify(w.errors.slice(0, 3)));
    check(consoleErrors.length === 0, 'no console error anywhere: ' + JSON.stringify(consoleErrors.slice(0, 3)));
    check(w.warns.length === 0, 'no "order view:" warning (the Order window swallows an error with it): ' + JSON.stringify(w.warns.slice(0, 2)));
    await w.close();
  }
  console.log(`   (${Math.round((Date.now() - T0) / 1000)} s)`);
  console.log(fails.length ? `\nmonogram-flow: ${fails.length} FAILED of ${nCheck}:\n - ${fails.join('\n - ')}` : `\nmonogram-flow OK: ${nCheck} checks`);
  process.exit(fails.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
