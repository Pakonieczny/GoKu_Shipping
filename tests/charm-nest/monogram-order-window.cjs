// MONOUI · the ORDER DETAILS WINDOW (Paul, 16 Oct 2026: the monogram "in place of the currently existing back/front engraving ... in the Engraving Tab and the Detailed Order view Modal").
// The Engraving section of the Overview (#owEng, drawn by charm-nest-order-engraving.js) shows, for a MONOGRAM order line, the page's monogram widget (charm-nest-monogram-ui.js, mode "compact") instead of the Front / Back
// cards: the buyer's three letters, the preview of the finished charm, the finished-size line, the assembly status and one Approve. Over the REAL sorter (charm-nest-1.html) in headless Chromium on the fake loopback site
// (bridge-server.cjs): the real intake, pool, classifier and fit for the engraved lines (_engraveRealWorld.cjs), Paul's real three alphabets (his file, through the local indexer) for the letters. No Etsy, no paid AI, nothing live.
//   1  a monogram order: the section keeps its place and title (after the chip strip, "Engraving"), has ONE monogram card with the widget in compact density, NO Front/Back card, the three letters as the buyer wrote them, a preview
//      of the cut piece, ONE size line, ONE Approve; the window does not jump while it is built (the card keeps one height through reading, building, ready, an edit, approved)
//   2  editing a letter in the window remakes the monogram (building, then ready, the newest edit wins, typing a letter back is instant); Approve puts "Monogram <letters>.ai" on the order's custom designs, once; Change and a new
//      letter REPLACE it (never two); reopening the window shows it approved
//   3  a MIXED order (a monogram line, an engraved bar, a plain piece): "Show all" has the monogram card and the bar's own Front/Back card side by side, in the lines' order; one piece picked shows only that piece (the bar: its card,
//      the monogram: its card with the letters a person typed still there, the plain piece: no section at all); the swap is in the SAME tick; the bar's Approve is its own, untouched
//   4  an ORDER WITH ONLY A PLAIN PIECE keeps today's cards: no monogram card, the same .engCard, the same Front/Back; an earring pair still has no section
//   5  letters that cannot be read ("A.P"): the card says so in plain words and the three boxes wait for typing; the card still has the one height
//   6  phone width (400 px): the card is one column, nothing runs past the window, no sideways scroll, the buttons are big enough to press; the mixed order's cards stand one under the other
//   7  the Review card's Build monogram: its words no longer name the old tab, and it opens the Engrave tab's monogram piece through Engrave.openMonogram(line key)
//   Screenshots (1440 and 400 px) go to $SHOTS (default /tmp/mu/MU3/shots).
//   node tests/charm-nest/monogram-order-window.cjs [playwright-core dir]      MONO_REAL=0 uses stand-in alphabets (no real letters)
// What a pass does NOT prove: a live order on the live page (live data, the live library, the live AI assembler); only that the page's own code puts the widget in the window and keeps the window still, on the offline fake.
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const R = require('./_engraveRealWorld.cjs'), Base = require('./_orderFactsWorld.cjs'), A = require('./_monogramAlphabets.cjs');
const SHOTS = process.env.SHOTS || '/tmp/mu/MU3/shots';
const V = (name, value) => ({ name, value });

// the order as Etsy sends it: any number of lines (the shared builder gives every extra line none of its own words)
function orderOf(fx, now) {
  const at = (d, h, m) => { const t = new Date(now); t.setDate(t.getDate() + d); t.setHours(h, m, 0, 0); return Math.floor(t.getTime() / 1000); };
  const shipBy = at(fx.ship, 17, 0), line = (x, i) => ({ transactionId: fx.rid + (i + 1), listingId: x.listing, sku: x.sku, title: x.title, quantity: x.qty || 1, expectedShipDate: shipBy, variations: x.variations || [], metalKey: x.metalKey || 'gold', metalLabel: 'GF 14/20', personalization: x.personalization || [] });
  return { receiptId: fx.rid, orderNumber: fx.rid, createTs: at(fx.placed.day, fx.placed.h, fx.placed.m), updateTs: at(0, 12, 0), shipBy, buyer: { name: fx.buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: (fx.lines || [fx]).map(line) };
}
Base.orderOf = orderOf;   // (R.build serialises Base.orderOf into the page)
const MONO_TITLE = 'Monogram Necklace Personalized Jewelry Initials Pendant';
const MONO_APV = { listing: '997905351', sku: 'MONOGRAM_5351', title: MONO_TITLE, variations: [V('Metal choice', 'Gold'), V('Chain length', '16 inches')], personalization: ['APV'], metalKey: 'gold' };
const MONO_OSR = { listing: '606439317', sku: 'MONOGRAM_9317', title: 'Monogram Necklace Dainty Monogram Necklace', variations: [V('Metal choice', 'Silver')], personalization: ['O S R'], metalKey: 'silver' };
const MONO_ODD = { listing: '997905351', sku: 'MONOGRAM_5351', title: MONO_TITLE, variations: [V('Metal choice', 'Gold')], personalization: ['A.P'], metalKey: 'gold' };
const BAR = { listing: '1890000101', sku: 'COMET_9427', title: 'Comet Necklace, Celestial Charm Necklace in 14k Gold Fill', variations: [V('Metal choice', 'Gold'), V('Font Choice', 'Stylish')], personalization: ['Name One'] };
const CAT = { listing: '1890000107', sku: 'SCHRODINGER CAT 1', title: 'Cat Necklace, Whimsical Cat Charm Gift', variations: [V('Metal choice', 'Gold')], personalization: [] };
R.ORDERS.mono = { rid: '4200683017', buyer: 'Name One', placed: { day: 0, h: 9, m: 10 }, ship: 5, lines: [MONO_APV] };
R.ORDERS.mixed = { rid: '4290000131', buyer: 'Name Two', placed: { day: 0, h: 9, m: 20 }, ship: 5, lines: [MONO_OSR, BAR, CAT] };
R.ORDERS.odd = { rid: '4290000132', buyer: 'Name Three', placed: { day: 0, h: 9, m: 30 }, ship: 5, lines: [MONO_ODD] };
R.ORDERS.review = { rid: '4290000133', buyer: 'Name Four', placed: { day: 0, h: 9, m: 40 }, ship: 5, lines: [Object.assign({}, MONO_APV, { personalization: ['J.Q.X'] })] };
Object.assign(R.EXPECT_JOBS, { mono: 0, mixed: 1, odd: 0, review: 0 });

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  try { require(path.join(pwDir, 'playwright-core')); } catch (_) { console.log('monogram-order-window: no playwright-core, skipped'); return; }
  process.env.PW_DIR = pwDir;
  fs.mkdirSync(SHOTS, { recursive: true });
  const fails = []; let nCheck = 0;
  const check = (ok, msg) => { nCheck++; if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const w = await R.world({ viewport: { width: 1440, height: 900 }, reducedMotion: 'reduce' });
  if (!w) { console.log('monogram-order-window: no playwright-core, skipped'); return; }
  const { page, srv } = w, st = srv.st;
  const shot = async name => { try { await page.screenshot({ path: path.join(SHOTS, name + '.png') }); } catch (e) { console.log('   (no screenshot ' + name + ')'); } };
  // the letter files can be held, so a build stays "building" long enough to be looked at
  let hold = null;
  await w.context.route(/Monogram_\w+_\w\.ai/i, async route => { if (hold) await new Promise(res => hold.push(res)); return route.continue(); });
  const holdOn = () => { hold = []; };
  const release = () => { const h = hold; hold = null; if (h) h.forEach(f => f()); };
  try {
    // ═══ the library: Paul's three alphabets (his file, through the local indexer), else stand-ins ═══
    const REAL = process.env.MONO_FILE || path.join(__dirname, 'fixtures', 'SKUs_MONOGRAM_2024-1015.ai'), REAL_OK = process.env.MONO_REAL !== '0' && fs.existsSync(REAL);
    if (REAL_OK) {
      const tmp = fs.mkdtempSync(path.join(require('os').tmpdir(), 'cn-mono-ow-')), file = path.join(tmp, path.basename(REAL)); fs.copyFileSync(REAL, file);
      const { main } = require('../../scripts/index-master.cjs'), { REPO_TEST } = require('./bridge-server.cjs');
      st.repoStrict = true; process.env.CHARM_REPO_PASSWORD = REPO_TEST.password;
      const rep = await main(['node', 'x', file, '--origin', srv.sorterOrigin], () => {});
      check(!!rep && rep.written === 78, `the library holds Paul's three alphabets: 78 letter SKUs (${rep && rep.written})`);
    } else { A.seed(st, await A.make()); }
    await page.evaluate(async () => { await Master.load(true); });
    if (process.env.MU3_STUB) await page.evaluate(fs.readFileSync(process.env.MU3_STUB, 'utf8'));   // (development only: a stand-in widget while the real one is not on the page)
    // this test is about the window, not the engine's rules: every word is staged as meeting them, so no card is flagged "Needs a person" and nothing asks the AI (that flow is tests/charm-nest/monogram-ai.cjs)
    await page.evaluate(() => {
      window.__builds = []; const eb = CharmNestMonogram.build;
      CharmNestMonogram.build = function (l) { window.__builds.push(l.left.letter + l.middle.letter + l.right.letter); const r = eb.apply(this, arguments); if (r && r.ok && r.report && r.report.metrics && Array.isArray(r.report.metrics.violations)) r.report.metrics = Object.assign({}, r.report.metrics, { violations: [], ok: true }); return r; };
    });
    const made = await R.build(w, ['mono', 'mixed', 'odd', 'review', 'back', 'earrings']);
    const keys = n => page.evaluate(rid => Orders.rows().filter(r => r.order.receiptId === rid).map(r => r.key), R.ORDERS[n].rid);
    const kMono = (await keys('mono'))[0], [kOsr, kBar, kCat] = await keys('mixed'), kOdd = (await keys('odd'))[0], kBack = (await keys('back'))[0], kEars = (await keys('earrings'))[0], kRev = (await keys('review'))[0];
    check(!!kMono && !!kOsr && !!kBar && !!kCat && !!kOdd && !!kBack && !!kEars, 'the orders are in the page: a monogram order, a mixed order of three lines, an odd one, a plain engraved piece, an earring pair');
    check((made.mixed.jobs || []).length === 1 && (made.mono.jobs || []).length === 0, 'the engraved bar has its one Engrave job, the monogram line none');

    // ── what the section is, read in the page ──
    const SECTION = () => {
      const host = document.getElementById('owEng'), sec = document.getElementById('owEngSec'), title = sec && sec.querySelector('.engSecHd');
      const box = n => { if (!n) return null; const r = n.getBoundingClientRect(), c = getComputedStyle(n); return { l: Math.round(r.left), t: Math.round(r.top), w: Math.round(r.width), h: Math.round(r.height * 10) / 10, b: Math.round(r.bottom), shown: c.display !== 'none' && c.visibility !== 'hidden' && r.height > 0 }; };
      const kids = [...(host ? host.children : [])].map(n => ({ kind: n.matches('[data-testid="ow-mono"]') ? 'mono' : n.classList.contains('engCard') ? 'card' : 'other', key: n.dataset.key || '', state: n.dataset.state || '', box: box(n), faces: [...n.querySelectorAll('.engFace')].map(f => f.dataset.face).join('+') }));
      const mw = host && host.querySelector('.mw');
      return { hostHidden: !host || host.hidden, titleShown: !!(title && box(title).shown), titleText: title ? title.textContent : '', secH: sec ? Math.round(sec.getBoundingClientRect().height) : -1, kids, mw: mw ? mw.dataset.mode : '', mws: host ? host.querySelectorAll('.mw').length : 0,
        selected: window.OrderWin.selectedPiece(), afterMeta: !!(sec && sec.previousElementSibling && sec.previousElementSibling.id === 'owMeta'), facesAnywhere: host ? host.querySelectorAll('.engFace, [data-face]').length : 0 };
    };
    const look = () => page.evaluate(`(${SECTION})()`);
    const settle = (ms = 500) => R.settle(page, ms);
    const openWin = async (key, ms = 900) => { await page.evaluate(async k => { CN.setMode('orders'); await OrderWin.open(k); }, key); await page.waitForSelector('#orderWin[open]', { timeout: 15000 }).catch(() => {}); await settle(ms); };
    const inView = () => page.evaluate(() => { const e = document.getElementById('owEngSec'); if (e) e.scrollIntoView({ block: 'center' }); });
    const mw = (sel = '') => `#owEng [data-testid="ow-mono"] .mw ${sel}`.trim();
    const wst = () => page.evaluate(() => { const e = document.querySelector('#owEng .mw'); return e ? { state: e.dataset.state, busy: e.dataset.busy || '' } : null; });
    const until = async (fn, what, ms = 25000) => { const t0 = Date.now(); let v; for (;;) { v = await page.evaluate(fn); if (v) return v; if (Date.now() - t0 > ms) { console.log('   (waiting for ' + what + ')'); return null; } await sleep(80); } };
    const isState = (...s) => `(() => { const e = document.querySelector('#owEng .mw'); return !!e && ${JSON.stringify(s)}.includes(e.dataset.state) && !e.dataset.busy; })()`;
    const entries = () => page.evaluate(() => Object.values(CustomSheet.entries()).map(e => ({ ck: e.ck, files: e.files.map(f => f.name), sent: !!e.sent })));
    // the card's height, watched on every animation frame from the moment it appears: the window must not jump while the monogram is made
    const watchStart = () => page.evaluate(() => { window.__hw = { min: 1e9, max: 0, first: 0, n: 0, secMin: 1e9, secMax: 0, trans: [], last: '', on: true }; const loop = () => { const h = window.__hw; if (!h || !h.on) return; const c = document.querySelector('#owEng [data-testid="ow-mono"]'); if (c) { const r = c.getBoundingClientRect(), s = document.getElementById('owEngSec').getBoundingClientRect(); if (!h.first) h.first = r.height; h.n++; const m = c.querySelector('.mw'), tag = (m ? m.dataset.state + '/' + (m.dataset.busy || '') : 'no widget') + ' ' + Math.round(r.height * 10) / 10; if (tag !== h.last && h.trans.length < 40) { h.last = tag; h.trans.push(tag + ' | ' + (m ? [...m.querySelectorAll('.mwLine,.mwNote,.mwMsg,.mwSays,.mwAct')].filter(n => n.offsetHeight).map(n => n.className.split(' ')[0] + '[' + n.offsetHeight + ']:' + n.textContent.replace(/\s+/g, ' ').trim().slice(0, 50)).join(' ; ') : '')); } h.min = Math.min(h.min, r.height); h.max = Math.max(h.max, r.height); h.secMin = Math.min(h.secMin, s.height); h.secMax = Math.max(h.secMax, s.height); } requestAnimationFrame(loop); }; requestAnimationFrame(loop); });
    const watchStop = () => page.evaluate(() => { const h = window.__hw; if (h) h.on = false; return h; });
    const heightNow = () => page.evaluate(() => { const c = document.querySelector('#owEng [data-testid="ow-mono"]'); return c ? Math.round(c.getBoundingClientRect().height * 10) / 10 : 0; });
    const typeIn = async (i, text) => { await page.click(mw(`[data-mw-letter="${i}"]`)); await page.keyboard.type(text); };

    // ═══ 1 · a monogram order ═══
    holdOn(); await watchStart();
    await openWin(kMono, 700);
    let s = await look();
    check(!s.hostHidden && s.titleShown && s.titleText === 'Engraving' && s.afterMeta, 'the Engraving section keeps its place (right after the chip strip) and its title');
    check(s.kids.length === 1 && s.kids[0].kind === 'mono' && s.kids[0].key === kMono, 'the monogram order has ONE card in the section: the monogram card of its line');
    check(s.facesAnywhere === 0 && !s.kids.some(k => k.kind === 'card'), 'no Front / Back card and no engraving face anywhere in it');
    check(s.mws === 1 && s.mw === 'compact', 'the page\'s monogram widget is in it, in its compact density (' + s.mws + ' widget, mode "' + s.mw + '")');
    const h0 = await heightNow(); await shot('1440-monogram-building');
    check(['reading', 'building'].includes((await wst() || {}).state), 'while the letters are being fetched the card is "' + ((await wst() || {}).state) + '" (the files are held back)');
    check(await page.evaluate(() => { const d = document.querySelector('#owEng [data-testid="ow-mono-dot"]'); return !!d && !!d.title; }), 'the card\'s dot says where the monogram is, in words');
    check(await page.evaluate(() => { const d = document.querySelector('#owEng [data-testid="ow-mono-dot"]'), n = document.querySelector('#owEng [data-testid="ow-mono-head"] .engName'), c = getComputedStyle(d), f = getComputedStyle(n); return c.width === '8px' && c.height === '8px' && c.borderRadius !== '0px' && +f.fontWeight >= 600 && parseFloat(f.fontSize) < 14; }), 'the header is the engraving cards\' own (a round 8 px dot, the name in their type) even in a window with no engraving card');
    release();
    check(!!await until(isState('ready', 'check'), 'the finished monogram'), 'the monogram is made: ready to approve');
    s = await look(); const hReady = await heightNow();
    const rd = await page.evaluate(sel => { const q = s2 => document.querySelector(sel + ' ' + s2); return { letters: [0, 1, 2].map(i => (q(`[data-mw-letter="${i}"]`) || {}).value), path: (q('[data-mw-pic] svg path') || { getAttribute: () => '' }).getAttribute('d'), size: (q('[data-mw-size]') || {}).textContent || '', approve: document.querySelectorAll(sel + ' [data-mw-approve]').length, approveVis: !!q('[data-mw-approve]') && !q('[data-mw-approve]').hidden, inputs: document.querySelectorAll(sel + ' input').length, dotS: (document.querySelector('#owEng [data-testid="ow-mono-dot"]') || { dataset: {} }).dataset.s || '' }; }, '#owEng .mw');
    check(rd.letters.join('') === 'APV', 'the three letters are the buyer\'s: ' + rd.letters.join(' '));
    check(rd.inputs === 3 && rd.approve === 1 && rd.approveVis, 'three letter boxes and ONE Approve');
    check(/^M/.test(rd.path || '') && (rd.path || '').length > 300, 'the preview is the cut piece (a drawn outline of ' + (rd.path || '').length + ' characters)');
    check(/\d+\.\d\s*[×x]\s*1[45]\.\d\s*mm/.test(rd.size), 'ONE size line says the finished size: "' + rd.size.trim() + '"');
    check(rd.dotS === 'approve', 'the dot is the amber "to approve" one');
    await shot('1440-monogram-ready');

    // ═══ 2 · editing, approving, replacing ═══
    const b0 = (await page.evaluate(() => window.__builds.slice())).length;
    await typeIn(0, 'B');
    check(['building', 'ready'].includes((await wst() || {}).state), 'a letter typed in the window remakes the monogram');
    check(!!await until(isState('ready', 'check'), 'the remade monogram'), 'the remade monogram is ready');
    const b1 = (await page.evaluate(() => window.__builds.slice())).length;
    check(b1 === b0 + 1 && (await page.evaluate(() => window.__builds.slice(-1)[0])) === 'BPV', 'exactly one build, of B P V (' + (b1 - b0) + ' new)');
    await typeIn(0, 'A');
    check(!!await until(isState('ready', 'check'), 'the first monogram again'), 'typing the letter back gives the first monogram again');
    check((await page.evaluate(() => window.__builds.slice())).length === b1, 'typing a letter back is instant: nothing is built again');
    await page.click(mw('[data-mw-approve]'));
    check(!!await until(isState('approved'), 'the approval'), 'Approve: the card says approved');
    let es = await entries();
    check(es.length === 1 && es[0].files.join() === 'Monogram APV.ai', 'the design is on the order\'s custom designs, once: ' + JSON.stringify(es.map(e => e.files)));
    check(await page.evaluate(() => { const d = document.querySelector('#owEng [data-testid="ow-mono-dot"]'); return d && d.dataset.s === 'approved'; }), 'the dot is the green "approved" one');
    const hAppr = await heightNow(); await shot('1440-monogram-approved');
    // Change and a new letter REPLACE the design
    await page.click(mw('[data-mw-change]'));
    await typeIn(1, 'Q');
    check(!!await until(isState('ready', 'check'), 'the changed monogram'), 'Change and another letter: the new monogram is made');
    await page.click(mw('[data-mw-approve]'));
    check(!!await until(isState('approved'), 'the second approval'), 'approved again');
    es = await entries();
    check(es.length === 1 && es[0].files.join() === 'Monogram AQV.ai', 'the new design REPLACED the first, never two designs: ' + JSON.stringify(es.map(e => e.files)));
    // the window does not jump: the one height, from the card's first frame to the approved state
    const hw = await watchStop();
    check(hw.n > 20 && hw.max - hw.min <= 1.5, 'the window did not jump: the monogram card kept one height through reading, building, ready, an edit and approved (' + hw.min + '–' + hw.max + ' px over ' + hw.n + ' frames)' + (hw.max - hw.min > 1.5 ? ' ' + JSON.stringify(hw.trans) : ''));
    check(hw.secMax - hw.secMin <= 1.5, 'and so did the whole section (' + hw.secMin + '–' + hw.secMax + ' px)');
    check(Math.abs(h0 - hReady) <= 1.5 && Math.abs(hReady - hAppr) <= 1.5, 'building ' + h0 + ', ready ' + hReady + ', approved ' + hAppr + ' px');
    // a reopened window shows it approved, with its letters
    await page.evaluate(() => OrderWin.close()); await settle(400);
    await openWin(kMono, 1200);
    check(!!await until(isState('approved'), 'approved after reopening'), 'the window reopened shows the monogram approved');
    check(await page.evaluate(() => [0, 1, 2].map(i => document.querySelector(`#owEng .mw [data-mw-letter="${i}"]`).value).join('') === 'AQV'), 'with the letters of the approved design (A Q V)');

    // ═══ 3 · a mixed order: a monogram line, an engraved bar, a plain piece ═══
    await page.evaluate(() => OrderWin.close()); await settle(300);
    await openWin(kOsr, 1500);
    check(!!await until(isState('ready', 'check', 'approved'), 'the mixed order\'s monogram'), 'the mixed order\'s monogram is made');
    s = await look();
    check(s.kids.length === 2 && s.kids[0].kind === 'mono' && s.kids[0].key === kOsr && s.kids[1].kind === 'card' && s.kids[1].key === kBar, 'Show all: the monogram card, then the bar\'s own card, in the lines\' order; none for the plain piece');
    check(s.kids[1].faces !== '' && s.kids[0].faces === '', 'the bar keeps its Front/Back face (' + s.kids[1].faces + '), the monogram has none');
    check(s.kids[0].box.t === s.kids[1].box.t && s.kids[0].box.l < s.kids[1].box.l, 'the two cards stand side by side (' + s.kids[0].box.w + ' + ' + s.kids[1].box.w + ' px wide)');
    check(await page.evaluate(() => { const b = document.querySelector('#owEng .engCard [data-e=approve]'); return !!b && !b.disabled; }), 'the bar\'s Approve is its own: there for the bar, whatever the monogram does');
    await shot('1440-mixed');
    const SAME = async (act, arg) => page.evaluate(({ act, arg, sec }) => { const f = eval('(' + sec + ')'); eval('(' + act + ')')(arg); return f(); }, { act: String(act), arg, sec: String(SECTION) });
    // a letter typed in the monogram's card before another piece is picked: the state lives in the page's one monogram service, so it is there when the card comes back
    await typeIn(1, 'X'); await until(isState('ready', 'check'), 'the typed letter\'s monogram');
    // one piece picked: only that piece's card, in the same tick
    let t = await SAME(k => OrderWin.selectPiece(k), kBar);
    check(t.kids.length === 1 && t.kids[0].kind === 'card' && t.kids[0].key === kBar && !t.hostHidden && t.titleShown, 'the bar picked: only the bar\'s card, in the same tick (no monogram card)');
    check(await page.evaluate(() => !document.querySelector('#owEng [data-testid="ow-mono"]')), 'the monogram card is not in the window while another piece is picked');
    t = await SAME(k => OrderWin.selectPiece(k), kOsr);
    check(t.kids.length === 1 && t.kids[0].kind === 'mono' && t.kids[0].key === kOsr && !t.hostHidden && t.mw === 'compact', 'the monogram picked: only its card, in the same tick');
    check(await page.evaluate(() => [0, 1, 2].map(i => document.querySelector(`#owEng .mw [data-mw-letter="${i}"]`).value).join('') === 'OXR'), 'it has the letters a person typed before the other piece was picked (O X R): the state is kept');
    t = await SAME(k => OrderWin.selectPiece(k), kCat);
    check(t.hostHidden && !t.titleShown && t.kids.length === 0 && t.secH <= 2, 'a plain piece picked (nothing to engrave): no section at all: no title, no rule, no room (' + t.secH + ' px)');
    t = await SAME(k => OrderWin.selectPiece(k), null);
    check(t.kids.length === 2 && t.kids[0].kind === 'mono' && t.kids[1].kind === 'card', 'Show all again: both cards, the monogram first');
    check(await page.evaluate(() => [0, 1, 2].map(i => document.querySelector(`#owEng .mw [data-mw-letter="${i}"]`).value).join('') === 'OXR'), 'the monogram card is back with its letters (O X R)');
    // the monogram approved in the mixed order: its own design, on its own order
    await page.click(mw('[data-mw-approve]')).catch(() => {});
    await until(isState('approved'), 'the mixed order\'s approval');
    es = await entries();
    check(es.some(e => e.files.join() === 'Monogram OXR.ai') && es.length === 2, 'approving the mixed order\'s monogram puts Monogram OXR.ai on that order, the first order\'s design stays: ' + JSON.stringify(es.map(e => e.files)));
    check(await page.evaluate(() => { const b = document.querySelector('#owEng .engCard [data-e=approve]'); return !!b && !b.disabled; }), 'and the bar\'s Approve is still its own, still waiting');

    // ═══ 4 · only a plain piece: today's cards, untouched ═══
    await page.evaluate(() => OrderWin.close()); await settle(300);
    await openWin(kBack, 1200);
    s = await look();
    check(s.kids.length === 1 && s.kids[0].kind === 'card' && s.kids[0].key === kBack && s.mws === 0 && !s.hostHidden && s.titleShown, 'a plain engraved piece: its one engraving card, no monogram card, no widget');
    check(s.kids[0].faces !== '' && await page.evaluate(() => !document.querySelector('[data-testid="ow-mono"], .engMono, .mw')), 'its card has its Front/Back face, and nothing of the monogram is on the page');
    await page.evaluate(() => OrderWin.close()); await settle(300);
    await openWin(kEars, 800);
    s = await look();
    check(s.hostHidden && !s.titleShown && s.kids.length === 0, 'an earring pair still has no Engraving section');

    // ═══ 5 · letters that cannot be read ═══
    await page.evaluate(() => OrderWin.close()); await settle(300);
    await openWin(kOdd, 1200);
    check(!!await until(isState('letters'), 'the unreadable letters'), 'A.P: two letters of three: the card is in its "letters" state');
    s = await look(); const oddRead = await page.evaluate(() => ({ msg: (document.querySelector('#owEng .mw [data-mw-msg]') || {}).textContent || '', hidden: (document.querySelector('#owEng .mw [data-mw-msg]') || { hidden: true }).hidden, inputs: [...document.querySelectorAll('#owEng .mw [data-mw-letter]')].map(i => i.value), dotS: document.querySelector('#owEng [data-testid="ow-mono-dot"]').dataset.s || '' }));
    check(s.kids.length === 1 && oddRead.inputs.join('|') === 'A|P|' && /\w/.test(oddRead.msg) && !oddRead.hidden, 'it says so in plain words ("' + oddRead.msg.trim().slice(0, 70) + '") and the three boxes wait: ' + oddRead.inputs.join(' '));
    check(oddRead.dotS === 'words', 'the dot is the red "words" one');
    const hLetters = await heightNow(); await shot('1440-monogram-letters');
    check(hLetters >= hReady - 1.5, 'the card never shrinks under its reserved height for a message (' + hLetters + ' px, the finished one ' + hReady + ')');
    await typeIn(2, 'V');
    check(!!await until(isState('ready', 'check'), 'the typed third letter'), 'typing the third letter makes the monogram');
    const hOdd = await heightNow();
    check(Math.abs(hOdd - hReady) <= 1.5, 'the card has the same one height with a message-free state as with the finished monogram (' + hOdd + ' / ' + hReady + ' px)');

    // ═══ 6 · phone width ═══
    await page.setViewportSize({ width: 400, height: 900 }); await settle(500);
    const phone = async (name, key, plain) => {
      await page.evaluate(() => OrderWin.close()); await settle(300); await openWin(key, 1400); if (!plain) await until(isState('ready', 'check', 'approved'), name); await inView(); await settle(400);
      return page.evaluate(() => {
        const win = document.getElementById('orderWin'), card = document.querySelector('#owEng [data-testid="ow-mono"]'), mwEl = card && card.querySelector('.mw'), cr = card && card.getBoundingClientRect(), vw = innerWidth;
        const wide = mwEl ? [...mwEl.querySelectorAll('*')].filter(n => { const r = n.getBoundingClientRect(); return r.width > 0 && (r.right > cr.right + 1 || r.left < cr.left - 1); }).length : 0;
        const btn = n => { const r = n && n.getBoundingClientRect(); return r ? { h: Math.round(r.height), w: Math.round(r.width), inView: r.left >= 0 && r.right <= vw } : null; };
        const boxes = mwEl ? [...mwEl.querySelectorAll('[data-mw-letter]')].map(i => btn(i)) : [];
        const sec = document.getElementById('owEngSec'), overSec = sec ? [...sec.querySelectorAll('*')].filter(n => { const r = n.getBoundingClientRect(); return r.width > 0 && (r.right > vw + 1 || r.left < -1); }).slice(0, 4).map(n => (n.className && String(n.className).split(' ')[0] ? '.' + String(n.className).split(' ')[0] : n.tagName) + ':' + Math.round(n.getBoundingClientRect().right)) : [];
        return { overSec, pageScrollW: document.scrollingElement.scrollWidth, vw, winScrollW: win ? win.scrollWidth : 0, winClientW: win ? win.clientWidth : 0, cardL: cr ? Math.round(cr.left) : -1, cardR: cr ? Math.round(cr.right) : -1, cardW: cr ? Math.round(cr.width) : 0, wide, approve: btn(mwEl && (mwEl.querySelector('[data-mw-approve]:not([hidden])') || mwEl.querySelector('[data-mw-change]'))), boxes,
          kids: [...document.querySelectorAll('#owEng > *')].map(n => { const r = n.getBoundingClientRect(); return { kind: n.matches('[data-testid="ow-mono"]') ? 'mono' : 'card', l: Math.round(r.left), r: Math.round(r.right), t: Math.round(r.top), b: Math.round(r.bottom) }; }) };
      });
    };
    // (the window's own title bar is wider than a phone with or without a monogram, and was before it: only the Engraving section is judged, and the page does not scroll sideways)
    let p = await phone('the phone monogram', kMono);
    check(p.pageScrollW <= p.vw && p.overSec.length === 0, `phone: no sideways scroll, and nothing of the Engraving section past the window's edge (page ${p.pageScrollW}/${p.vw}${p.overSec.length ? ', over: ' + JSON.stringify(p.overSec) : ''})`);
    check(p.cardL >= 0 && p.cardR <= p.vw && p.cardW >= 300, `phone: the monogram card is in the window, one column (${p.cardL}–${p.cardR} of ${p.vw} px)`);
    check(p.wide === 0, 'phone: nothing of the widget runs past the card (' + p.wide + ' elements do)');
    check(p.boxes.length === 3 && p.boxes.every(b => b.h >= 32 && b.w >= 32 && b.inView) && p.approve && p.approve.h >= 28 && p.approve.inView, 'phone: the three letter boxes and the button are big enough to press (boxes ' + p.boxes.map(b => b.w + '×' + b.h).join(' ') + ', button ' + (p.approve && p.approve.w + '×' + p.approve.h) + ')');
    await shot('400-monogram-approved');
    // phone, building (the files held back)
    holdOn(); await page.evaluate(() => OrderWin.close()); await settle(300);
    await openWin(kOdd, 1200); await inView();
    await page.fill(mw('[data-mw-letter="2"]'), 'Z').catch(() => {});
    await settle(500); await shot('400-monogram-building'); release();
    await until(isState('ready', 'check', 'approved'), 'the phone build');
    p = await phone('the phone mixed order', kOsr);
    // (the mixed order at phone width: the monogram card, the bar's card under it, each as wide as the window)
    check(p.pageScrollW <= p.vw && p.kids.length === 2 && p.kids[0].kind === 'mono' && p.kids[1].kind === 'card' && p.kids[1].t >= p.kids[0].b - 1 && p.kids.every(k => k.l >= 0 && k.r <= p.vw), 'phone: the mixed order\'s cards stand one under the other, inside the window: ' + JSON.stringify(p.kids));
    await shot('400-mixed');
    // the narrowest phone (320 px): the widget stacks (picture over the letters), the card keeps ITS reserved height for that layout too
    await page.setViewportSize({ width: 320, height: 800 }); await settle(500);
    const plain320 = await phone('the 320 px plain piece', kBack, true);   // (the window's own title bar is wider than 320 px with or without a monogram: judged against a plain piece's window)
    p = await phone('the 320 px monogram', kMono);
    const h320 = await heightNow(); await watchStart(); await page.click(mw('[data-mw-change]')); await typeIn(0, 'C'); await until(isState('ready', 'check'), 'the 320 px edit'); await page.click(mw('[data-mw-approve]')); await until(isState('approved'), 'the 320 px approval'); const hw320 = await watchStop();
    check(p.pageScrollW <= Math.max(p.vw, plain320.pageScrollW) && p.overSec.length === 0 && p.cardL >= 0 && p.cardR <= p.vw, `320 px: the monogram adds nothing past the window's edge (card ${p.cardL}–${p.cardR} of ${p.vw}, page ${p.pageScrollW} against a plain piece's ${plain320.pageScrollW}${p.overSec.length ? ', over: ' + JSON.stringify(p.overSec) : ''})`);
    check(hw320.max - hw320.min <= 1.5 && Math.abs(hw320.max - h320) <= 1.5, `320 px: the stacked card keeps one height through an edit and an approval (${hw320.min}–${hw320.max} px, ${h320} before)`);
    await shot('320-monogram-approved');
    await page.evaluate(() => OrderWin.close()); await settle(300);
    await page.setViewportSize({ width: 1440, height: 900 }); await settle(500);

    // ═══ 7 · the Review card's Build monogram ═══
    await page.evaluate(() => { CN.setMode('review'); Review.render(); });
    await page.waitForSelector(`#rvList .reviewListRow[data-rid="${R.ORDERS.review.rid}"] [data-cu-mono]`, { timeout: 15000 }).catch(() => {});
    const btn = await page.evaluate(rid => { const b = document.querySelector(`#rvList .reviewListRow[data-rid="${rid}"] [data-cu-mono]`); return b ? { title: b.title, text: b.textContent.trim() } : null; }, R.ORDERS.review.rid);
    check(!!btn && btn.text === 'Build monogram' && !/Monograms/.test(btn.title), 'the Review card\'s Build monogram no longer names the old Monograms tab ("' + (btn && btn.title) + '")');
    await page.evaluate(() => { window.__opened = []; const o = Engrave.openMonogram; Engrave.openMonogram = function (k) { window.__opened.push(k); return o.apply(this, arguments); }; });
    await page.click(`#rvList .reviewListRow[data-rid="${R.ORDERS.review.rid}"] [data-cu-mono]`); await sleep(600);
    const opened = await page.evaluate(() => window.__opened.slice());
    check(opened.length === 1 && opened[0] === kRev, 'it opens the monogram through Engrave.openMonogram with the line\'s key (' + JSON.stringify(opened) + ')');
    check(await page.evaluate(() => !document.getElementById('engraveView').classList.contains('hidden')), 'and the Engrave tab is what the person sees');

    check(w.errors.length === 0, 'no page error' + (w.errors.length ? ': ' + w.errors.slice(0, 2).join(' | ') : ''));
    check(w.warns.length === 0, 'no order-window warning' + (w.warns.length ? ': ' + w.warns.slice(0, 2).join(' | ') : ''));
  } finally { release(); await w.close(); }
  console.log(`\n${nCheck - fails.length}/${nCheck} checks passed` + (fails.length ? `, ${fails.length} FAILED:\n  - ` + fails.join('\n  - ') : ''));
  process.exit(fails.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
