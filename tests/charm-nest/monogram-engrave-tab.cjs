// MONOUI / MU2 (Paul, 16 Oct 2026): "...all of the required monogram options, previews, functionality ... as part of the Engraving Tab and the Detailed Order view Modal in place of the currently existing back/front engraving."
// The ENGRAVE TAB part: a MONOGRAM order line is a PIECE of its order, drawn as what it is.
//   1  the order model (node, no page): charm-nest-engrave-order.js lists a monogram line as a piece (no faces, no job) once the page says which lines are monograms; nothing changes without that
//   2  the tab: no Monograms tab; Placements holds the monogram lines beside the engraved ones; a monogram row shows the finished charm (the service's thumbnail) and its three letters, never Front / Back
//   3  the detail card of a monogram line holds the widget (mode "detail"), the engraved piece's own head (counter, Back / Next, strip of the order's pieces, Open order, close), the buyer's words and messages
//   4  a MIXED order (a monogram line and an engraved bar): the strip says "Piece 1 of 2", Back / Next walk both kinds, Approve on the monogram is that piece's approval and the detail goes on to the bar (never another order),
//      the bar approved first goes on to the monogram, leaving an order with a piece not approved is said by the guide's one line; once approved both stay as rows of the ONE list (NODECIDED: no Decided tab) and the
//      monogram row opens the same card with the widget, approved, with its own Change
//   5  an order WITHOUT a monogram draws and behaves exactly as before (panel, Front / Back faces, Approve)
//   6  the old doors: Engrave.openMonogram(lineKey) lands on the line's card (approved or not: no Decided row any more); a saved view with tab "mono" lands in Placements; counts (tab badge, Engrave.pendingRows)
//   7  phone width (400): the list and the detail do not overflow sideways; no console errors
// Offline only: the REAL page over the repo's fake site (the _engraveRealWorld.cjs world) with three 26-letter alphabets (Paul's own file, indexed, unless MONO_REAL=0); no Etsy, no paid AI, nothing live. A pass proves the page's own code
// does this on the offline fake, not a live order. Screenshots (1440 and 400 px) go to $SHOTS (default /tmp/mu/MU2).
//   node tests/charm-nest/monogram-engrave-tab.cjs [playwright-core dir]
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const SHOTS = process.env.SHOTS || '/tmp/mu/MU2';
const fails = []; let nCheck = 0;
const check = (ok, msg) => { nCheck++; if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };

// ═══ 1 · the order model, in node ═══
{
  const Ord = require(path.join(root, 'charm-nest-engrave-order.js')), Gd = require(path.join(root, 'charm-nest-engrave-guide.js'));
  const L = (n, material) => ({ key: 'K' + n, state: 'held', order: { receiptId: '77' }, spec: { special: { monogram: true }, material }, line: { sku: 'MONOGRAM_5351' } });
  const rows = [L(1, 'gold'), L(2, 'silver')], st = { K1: 'to-approve', K2: 'approved' };
  const fn = r => st[r.key] || null;
  check(Ord.piecesOf('77', { rows }).length === 0, 'node: without the page\'s answer a monogram line adds no piece (nothing changes)');
  const ps = Ord.piecesOf('77', { rows, monoState: fn });
  check(ps.length === 2 && ps.every(p => p.kind === 'monogram' && p.faces.length === 0 && p.jobKeys.length === 0 && p.piece.faces.front === null && p.piece.faces.back === null), 'node: with it each monogram line is one piece with no faces and no job');
  check(ps[0].pieceKey === 'K1' && ps[0].state === 'to-approve' && ps[1].state === 'approved' && ps[0].label === 'GF · Monogram' && ps[1].label === 'SS · Monogram', 'node: its state is the page\'s (to-approve / approved), its label names the metal when the order has several lines: ' + ps.map(p => p.label + ':' + p.state).join(' '));
  const c = Ord.countOf('77', { rows, monoState: fn });
  check(c.n === 2 && c.approved === 1 && c.left === 1 && c.toApprove === 1, 'node: countOf counts it: ' + JSON.stringify(c));
  check(Ord.nextUnapproved('77', 'K2', { rows, monoState: fn }).pieceKey === 'K1' && Ord.nextUnapproved('77', 'K1', { rows, monoState: fn }) === null, 'node: nextUnapproved goes to the monogram still to approve, and nowhere when it is the only one left');
  check(Ord.step('77', 'K1', 1, { rows, monoState: fn }).pieceKey === 'K2' && Ord.step('77', 'K2', 1, { rows, monoState: fn }) === null, 'node: Back / Next step through it without wrapping');
  check(Ord.jobOf(ps[0]) === null && /data-piece="K1"/.test(Ord.stripHtml(ps, 'K1', {})), 'node: it has no job, and the strip lists it');
  check(Ord.piecesOf('77', { rows, monoState: null }).length === 0, 'node: o.monoState null turns it off');
  Ord.useMonogram(fn); check(Ord.piecesOf('77', { rows }).length === 2, 'node: useMonogram(fn) is the module\'s default answer'); Ord.useMonogram(null);
  const g = Gd.afterApprove({ key: 'K1', row: rows[0] });   // (the guide reads the order's pieces without options: through useMonogram)
  check(g === null, 'node: the guide does not break on a line without the page\'s answer');
  Ord.useMonogram(fn);
  const g2 = Gd.afterApprove({ key: 'K2', row: rows[1] }); Ord.useMonogram(null);
  check(!g2 || g2.pieceKey !== 'K2', 'node: afterApprove of a piece never names itself: ' + JSON.stringify(g2));
}

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  try { require(path.join(pwDir, 'playwright-core')); } catch (_) { console.log('monogram-engrave-tab: no playwright-core, the browser part is skipped'); finish(); return; }
  process.env.PW_DIR = pwDir;
  fs.mkdirSync(SHOTS, { recursive: true });
  const R = require('./_engraveRealWorld.cjs'), Base = require('./_orderFactsWorld.cjs'), A = require('./_monogramAlphabets.cjs');
  const V = (name, value) => ({ name, value });
  function orderOf(fx, now) {
    const at = (d, h, m) => { const t = new Date(now); t.setDate(t.getDate() + d); t.setHours(h, m, 0, 0); return Math.floor(t.getTime() / 1000); };
    const shipBy = at(fx.ship, 17, 0), line = (x, i) => ({ transactionId: fx.rid + (i + 1), listingId: x.listing, sku: x.sku, title: x.title, quantity: x.qty || 1, expectedShipDate: shipBy, variations: x.variations || [], metalKey: x.metalKey || 'gold', metalLabel: x.metalLabel, personalization: x.personalization || [] });
    return { receiptId: fx.rid, orderNumber: fx.rid, createTs: at(fx.placed.day, fx.placed.h, fx.placed.m), updateTs: at(0, 12, 0), shipBy, buyer: { name: fx.buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: (fx.lines || [fx]).map(line) };
  }
  Base.orderOf = orderOf;
  const MONO = { listing: '997905351', sku: 'MONOGRAM_5351', title: 'Monogram Necklace Personalized Jewelry Initials Pendant', variations: [V('Metal choice', 'Gold'), V('Chain length', '16 inches')], personalization: ['APV'], metalKey: 'gold' };
  const MONO2 = { listing: '606439317', sku: 'MONOGRAM_9317', title: 'Monogram Necklace Dainty Monogram Necklace', variations: [V('Metal choice', 'Silver')], personalization: ['O S R'], metalKey: 'silver' };
  const MONO3 = { listing: '997905351', sku: 'MONOGRAM_5351', title: 'Monogram Necklace Personalized Jewelry Initials Pendant', variations: [V('Metal choice', 'Gold')], personalization: ['J.Q.X'], metalKey: 'gold' };
  const BAR = { listing: '1890000101', sku: 'COMET_9427', title: 'Comet Necklace, Celestial Charm Necklace in 14k Gold Fill', variations: [V('Metal choice', 'Gold'), V('Font Choice', 'Stylish')], personalization: ['Name One'] };
  R.ORDERS.mono = { rid: '4200683017', buyer: 'Name One', placed: { day: 0, h: 9, m: 10 }, ship: 5, lines: [MONO] }; R.EXPECT_JOBS.mono = 0;
  R.ORDERS.mono2 = { rid: '4202783520', buyer: 'Name Two', placed: { day: 0, h: 9, m: 15 }, ship: 5, lines: [MONO2] }; R.EXPECT_JOBS.mono2 = 0;
  R.ORDERS.mixed = { rid: '4290000131', buyer: 'Name Three', placed: { day: 0, h: 9, m: 20 }, ship: 5, lines: [MONO3, BAR] }; R.EXPECT_JOBS.mixed = 1;       // (the monogram line first, the engraved bar second)
  R.ORDERS.mixedB = { rid: '4290000132', buyer: 'Name Four', placed: { day: 0, h: 9, m: 25 }, ship: 5, lines: [BAR, MONO2] }; R.EXPECT_JOBS.mixedB = 1;       // (the bar first, the monogram second)
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const w = await R.world({ viewport: { width: 1440, height: 900 }, shots: SHOTS });
  if (!w) { console.log('monogram-engrave-tab: no playwright-core, the browser part is skipped'); finish(); return; }
  const page = w.page, T0 = Date.now();
  let writes = 0; page.on('request', rq => { if (rq.method() !== 'GET' && rq.method() !== 'OPTIONS' && /127\.0\.0\.1|localhost/.test(rq.url()) && /claude|openai|anthropic|etsy/i.test(rq.url())) writes++; });
  const shot = async name => { try { await page.evaluate(() => document.querySelectorAll('.cnNbX').forEach(b => { try { b.click(); } catch (_) {} })); await sleep(250); await page.screenshot({ path: path.join(SHOTS, name + '.png') }); } catch (e) { console.log('   (no screenshot ' + name + ')'); } };
  // the same screen at phone width (400 px, the rail folded as a person would), a picture and the overflow check, then back to the desktop
  const overflow = () => page.evaluate(() => ({ sw: document.documentElement.scrollWidth, cw: document.documentElement.clientWidth, pane: (() => { const p = document.querySelector('#engraveView .egPane:not([hidden])'); return p ? [p.scrollWidth, p.clientWidth] : null; })(), card: (() => { const c = document.querySelector('#egQueue .rvItem'); return c ? [c.scrollWidth, c.clientWidth] : null; })() }));
  const atPhone = async name => {
    const rail0 = await page.evaluate(() => document.getElementById('app').classList.contains('railOff'));
    await page.setViewportSize({ width: 400, height: 900 }); await sleep(500);
    if (await page.evaluate(() => !document.getElementById('app').classList.contains('railOff'))) { await page.click('#btnRail').catch(() => {}); await sleep(500); }
    await page.evaluate(() => document.querySelectorAll('.cnNbX, .toast .x').forEach(b => { try { b.click(); } catch (_) {} })); await sleep(700);
    const o = await overflow(); await shot(name);
    await page.setViewportSize({ width: 1440, height: 900 }); await sleep(400);
    if (!rail0 && await page.evaluate(() => document.getElementById('app').classList.contains('railOff'))) { await page.click('#btnRail').catch(() => {}); await sleep(400); }
    return o;
  };
  const noSideways = (o, what) => check(o.sw <= o.cw + 1 && (!o.pane || o.pane[0] <= o.pane[1] + 1) && (!o.card || o.card[0] <= o.card[1] + 1), 'phone width, ' + what + ': no sideways overflow ' + JSON.stringify(o));
  try {
    // ═══ the world: the alphabets, the orders ═══
    const REAL = path.join(__dirname, 'fixtures', 'SKUs_MONOGRAM_2024-1015.ai'), REAL_OK = process.env.MONO_REAL !== '0' && fs.existsSync(REAL);
    if (REAL_OK) {
      const tmp = fs.mkdtempSync(path.join(require('os').tmpdir(), 'cn-mono-eg-')), file = path.join(tmp, path.basename(REAL)); fs.copyFileSync(REAL, file);
      const { main } = require('../../scripts/index-master.cjs'), { REPO_TEST } = require('./bridge-server.cjs');
      w.srv.st.repoStrict = true; process.env.CHARM_REPO_PASSWORD = REPO_TEST.password;
      await main(['node', 'x', file, '--origin', w.srv.sorterOrigin], () => {});
    } else A.seed(w.srv.st, await A.make());
    for (let i = 0; i < 8; i++) { const want = [...w.srv.st.docs.keys()].filter(k => k.startsWith('Charm_Master_Index/')).length, have = await page.evaluate(async () => { await Master.load(true); return B.master.entries.size; }); if (have >= want) break; await sleep(400); }
    // (this test is about the Engrave tab, not the engine's rules: every word is staged as meeting them, so no card is "Needs a person" and nothing asks the AI; those flows are tests/charm-nest/monogram-ai.cjs)
    await page.evaluate(() => { if (window.__wrapped) return; window.__wrapped = true; const eb = CharmNestMonogram.build; CharmNestMonogram.build = function () { const r = eb.apply(this, arguments); if (r && r.ok && r.report && r.report.metrics && Array.isArray(r.report.metrics.violations)) r.report.metrics = Object.assign({}, r.report.metrics, { violations: [], ok: true }); return r; }; });
    const made = await R.build(w, ['mono', 'mono2', 'mixed', 'mixedB', 'back']);
    const key = n => made[n].key, rid = n => R.ORDERS[n].rid;
    const lines = await page.evaluate(() => Orders.rows().map(r => ({ key: r.key, rid: r.order.receiptId, mono: !!(r.spec && r.spec.special && r.spec.special.monogram) })));
    const monoKey = n => lines.find(l => l.rid === rid(n) && l.mono).key, barKey = n => lines.find(l => l.rid === rid(n) && !l.mono).key;
    check(lines.filter(l => l.mono).length === 4 && lines.filter(l => !l.mono).length === 3, 'the world: 4 monogram lines (mono, mono2, mixed, mixedB) and 3 engraved lines (mixed, mixedB, back)');
    const jobs = () => page.evaluate(() => [...Engrave.items().values()].map(j => j.key + ':' + j.state));
    check((await jobs()).length === 3 && !(await jobs()).some(j => /^(4200683017|4202783520)/.test(j)), 'a monogram line has no Engrave job: only the 3 engraved lines have one: ' + (await jobs()).join(' '));

    const openList = () => page.evaluate(() => { CN.setMode('engrave'); Engrave.restoreView({ tab: 'place', list: true, chosen: true, focus: null, pin: null, openDone: null }); Engrave.render(); });
    const settle = ms => R.settle(page, ms || 400);
    const wid = rid => page.evaluate(r => { const x = (window.CharmNestMonogramUI.widgets() || []).find(w => w.state().rid === r); return x ? { state: x.state().state, mode: x.mode, letters: x.state().letters.join(''), busy: x.state().busy } : null; }, rid);
    const untilWidget = async (r, ok, what, ms = 60000) => { const t0 = Date.now(); let s; for (;;) { s = await wid(r); if (s && ok(s)) return s; if (Date.now() - t0 > ms) { console.log('   (waiting for ' + what + ', last: ' + JSON.stringify(s) + ')'); return s; } await sleep(120); } };
    const rows = () => page.evaluate(() => [...document.querySelectorAll('#egQueue .doneRow')].map(r => ({ kind: r.dataset.kind || 'engraved', rid: r.dataset.rid, text: r.innerText.replace(/\s+/g, ' ').trim(), thumb: !!r.querySelector('[data-mw-thumb]'), faces: !!r.querySelector('.engFace, [data-face], .placementThumb'), words: (r.querySelector('.w') || {}).textContent })));
    const tabs = () => page.evaluate(() => [...document.querySelectorAll('#engraveView .egTab[data-tab]')].map(b => b.dataset.tab + ':' + ((b.querySelector('b') || {}).textContent || '')));
    const card = () => page.evaluate(() => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); if (!c) return null; const mw = c.querySelector('.mw');
      return { mono: !!c.dataset.mono, key: c.dataset.key, state: c.dataset.state, mw: !!mw, mode: mw && mw.dataset.mode, mwState: mw && mw.dataset.state, strip: c.querySelector('.egPcStrip') ? c.querySelector('.egPcStrip').innerText.replace(/\s+/g, ' ').trim() : '', counter: (c.querySelector('.rh .kind') || {}).textContent, ttl: (c.querySelector('.ttl') || {}).textContent,
        guide: c.querySelector('.egGuide') && !c.querySelector('.egGuide').hidden ? c.querySelector('.egGuide').innerText.replace(/\s+/g, ' ').trim() : '', faces: !!c.querySelector('.engThumb, .engFace, [data-face], .backHost, .engp'), engApprove: !!c.querySelector('[data-a="approve"]'), prev: !!c.querySelector('[data-a=prev]'), next: !!c.querySelector('[data-a=next]'), close: !!c.querySelector('[data-a=close]'), open: !!c.querySelector('[data-open-order]'), said: !!c.querySelector('.owSaid, .pvSaid'), mail: !!c.querySelector('.pvMail') }; });

    // ═══ 2 · the tab: no Monograms tab, the monogram lines beside the engraved ones ═══
    await openList(); await settle(2500);
    check(!(await page.evaluate(() => !!document.querySelector('#engraveView .egTab[data-tab="mono"], #egMono'))) && (await tabs()).map(t => t.split(':')[0]).join() === 'place', 'there is no Monograms tab and no Decided tab: the one chip is Placements: ' + (await tabs()).join(' '));
    let rs = await rows();
    check(rs.length === 7 && rs.filter(r => r.kind === 'monogram').length === 4 && rs.filter(r => r.kind === 'engraved').length === 3, 'Placements lists the 4 monogram lines beside the 3 engraved ones (7 rows): ' + rs.map(r => r.kind[0] + r.rid.slice(-3)).join(' '));
    check((await tabs())[0] === 'place:7', 'the Placements badge counts them: ' + (await tabs())[0]);
    const mrow = rs.find(r => r.kind === 'monogram' && r.rid === rid('mono'));
    check(mrow && mrow.thumb && /A P V/.test(mrow.words || '') && !mrow.faces && !/Front|Back/.test(mrow.text.replace(/Words on the back/ig, '')), 'a monogram row shows the finished charm and its three letters, no Front / Back faces: "' + (mrow && mrow.text.slice(0, 110)) + '"');
    const erow = rs.find(r => r.kind === 'engraved' && r.rid === rid('back'));
    check(erow && erow.faces && /Words on the back/i.test(erow.text) && !erow.thumb, 'an engraved row is what it was (its pictures, "Words on the back", no monogram thumbnail): "' + (erow && erow.text.slice(0, 90)) + '"');
    const adj = rs.map(r => r.rid); check(adj.indexOf(rid('mixed')) >= 0 && adj[adj.indexOf(rid('mixed')) + 1] === rid('mixed') && adj[adj.indexOf(rid('mixedB')) + 1] === rid('mixedB'), 'the two rows of a mixed order stand together: ' + adj.join(' '));
    check(await page.evaluate(() => Engrave.pendingRows()) === 7, 'Engrave.pendingRows() (the Engrave badge) counts the monogram lines: ' + (await page.evaluate(() => Engrave.pendingRows())));
    await page.evaluate(() => document.querySelectorAll('.cnNbX, .toast .x, [aria-label="Close"]').forEach(b => { try { b.click(); } catch (_) {} }));   // (the station's role question is not what this picture is of)
    // nothing redraws a settled list: no render loop between the page and the monogram service
    await page.waitForFunction(() => Engrave.monoItems().length === 4 && Engrave.monoItems().every(m => m.st.state === 'ready'), null, { timeout: 90000 }).catch(() => {}); await settle(1200);
    { const quiet = await page.evaluate(() => new Promise(res => { const seen = []; const mo = new MutationObserver(m => { for (const x of m) seen.push(x.type + ':' + (x.target.className && x.target.className.baseVal === undefined ? x.target.className : '') + ':' + (x.attributeName || '')); }); mo.observe(document.getElementById('egQueue'), { subtree: true, childList: true, characterData: true, attributes: true }); setTimeout(() => { mo.disconnect(); res(seen); }, 2500); }));
      check(quiet.length <= 6, 'a settled list stays still (no render loop with the monogram service): ' + quiet.length + ' DOM changes in 2.5 s' + (quiet.length > 6 ? ' ' + JSON.stringify(quiet.slice(0, 8)) : '')); }
    await shot('list-1440');
    noSideways(await atPhone('list-400'), 'the list');
    // the search box finds a monogram by its letters, and by the word
    await page.fill('#egQ', 'OSR'); await settle(700);
    rs = await rows(); check(rs.length === 2 && rs.every(r => r.kind === 'monogram' && r.rid === rid('mono2') || r.rid === rid('mixedB')), 'searching the letters "OSR" lists the monogram lines of that word (mono2 and mixedB): ' + rs.map(r => r.kind[0] + r.rid.slice(-3)).join(' '));
    await page.fill('#egQ', 'monogram'); await settle(700);
    rs = await rows(); check(rs.length === 4 && rs.every(r => r.kind === 'monogram'), 'searching "monogram" lists the 4 monogram rows: ' + rs.length);
    await page.fill('#egQ', ''); await settle(700);

    // ═══ 3 · the detail card of a monogram line (a monogram-only order) ═══
    await page.click(`#egQueue .doneRow[data-kind=monogram][data-rid="${rid('mono')}"]`);
    await page.waitForSelector('#egQueue .rvItem[data-mono] .mw', { timeout: 20000 });
    let c = await card();
    check(c && c.mono && c.mw && c.mode === 'detail' && c.key === monoKey('mono') && c.ttl === rid('mono'), 'a click on the row opens ITS card: the widget in mode "detail" (' + JSON.stringify({ key: c && c.key, mode: c && c.mode }) + ')');
    check(c && !c.faces && !c.engApprove && c.prev && c.next && c.close && c.open && c.said && c.mail && /^\d+ OF \d+ · \d+ DONE$/i.test(String(c.counter).toUpperCase()), 'the card has the engraved piece\'s head (counter "' + (c && c.counter) + '", Back / Next, Open order, close), the buyer\'s words and messages, and no Front / Back / panel / engraving Approve');
    check(c && c.strip === '', 'a one-piece order draws no strip');
    const s1 = await untilWidget(rid('mono'), s => ['ready', 'check', 'approved'].includes(s.state), 'the monogram APV to be made');
    check(s1 && s1.letters === 'APV' && s1.state === 'ready', 'the widget reads the buyer\'s letters and makes the monogram: ' + JSON.stringify(s1));
    await settle(900); await shot('detail-mono-1440');
    noSideways(await atPhone('detail-mono-400'), 'a monogram card');
    // typing a letter: the widget remakes the monogram, the card's head and (after close) the row follow, and a re-render of the tab never takes the caret away
    await page.click('#egQueue .rvItem[data-mono] [data-mw-letter="1"]'); await page.keyboard.type('Q');
    await untilWidget(rid('mono'), s => s.letters === 'AQV' && s.state === 'ready', 'AQV made');
    check(await page.evaluate(() => { const n = document.activeElement; const ok0 = n && n.matches('[data-mw-letter]'); Engrave.render(); Engrave.render(); return ok0 && n === document.activeElement && document.activeElement.isConnected; }), 'typing in a letter box: a re-render of the Engrave tab leaves the focused box (and its caret) alone');
    await settle(500);
    check(/A Q V/.test((await page.evaluate(() => document.querySelector('#egQueue .rvItem[data-mono] [data-mono-sub]').textContent))), 'the card\'s head follows the letters (A Q V)');
    await page.evaluate(() => document.activeElement.blur());
    await page.click('#egQueue .rvItem[data-mono] [data-mw-letter="1"]'); await page.keyboard.type('P');
    await untilWidget(rid('mono'), s => s.letters === 'APV' && s.state === 'ready', 'APV again');
    await page.keyboard.press('Escape'); await page.evaluate(() => document.activeElement.blur()); await settle(300);
    await page.keyboard.press('Escape'); await settle(500);   // (the card has focus after it opens: Escape closes it back to the list)
    c = await card();
    if (c) { await page.click('#egQueue .rvItem[data-mono] [data-a=close]'); await settle(600); }
    check(await page.evaluate(() => !!document.querySelector('#egQueue .egPlacementList')), 'close goes back to the list of placements');

    // ═══ cycling: Back / Next visit EVERY piece of EVERY order once, both kinds, the pieces of an order one after another (the guide's "Leave" is pressed where it asks) ═══
    await page.click(`#egQueue .doneRow[data-kind=monogram][data-rid="${rid('mono')}"]`);
    await page.waitForSelector('#egQueue .rvItem[data-mono] .mw', { timeout: 20000 });
    const curKey = () => page.evaluate(() => (document.querySelector('#egQueue .rvItem[data-kind=placement]') || {}).dataset.key);
    const seq = [await curKey()];
    for (let i = 0; i < 6; i++) {
      const before = seq[seq.length - 1];
      await page.click('#egQueue .rvItem [data-a=next]'); await sleep(500);
      if (await page.evaluate(() => { const g = document.querySelector('#egQueue .rvItem .egGuide'); return !!g && !g.hidden; })) await page.click('#egQueue .rvItem .egGuide [data-g=leave]');
      await page.waitForFunction(k => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); return c && c.dataset.key !== k; }, before, { timeout: 20000 });
      seq.push(await curKey()); await sleep(300);
    }
    const allKeys = [monoKey('mono'), monoKey('mono2'), monoKey('mixed'), monoKey('mixedB'), barKey('mixed'), barKey('mixedB'), key('back')];
    check(new Set(seq).size === 7 && allKeys.every(k => seq.includes(k)), 'Next, six times: all 7 pieces (4 monograms, 3 engraved) were visited once, none skipped: ' + seq.map(k => k.slice(-6)).join(' '));
    check(Math.abs(seq.indexOf(monoKey('mixed')) - seq.indexOf(barKey('mixed'))) === 1 && Math.abs(seq.indexOf(monoKey('mixedB')) - seq.indexOf(barKey('mixedB'))) === 1, 'the two pieces of a mixed order are visited one after the other');
    const back = [seq[6]]; for (let i = 0; i < 6; i++) { const before = back[back.length - 1]; await page.click('#egQueue .rvItem [data-a=prev]'); await sleep(500);
      if (await page.evaluate(() => { const g = document.querySelector('#egQueue .rvItem .egGuide'); return !!g && !g.hidden; })) await page.click('#egQueue .rvItem .egGuide [data-g=leave]');
      await page.waitForFunction(k => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); return c && c.dataset.key !== k; }, before, { timeout: 20000 }); back.push(await curKey()); await sleep(300); }
    check(back.join() === seq.slice().reverse().join(), 'Back, six times, walks the same pieces the other way');
    await page.click('#egQueue .rvItem [data-a=close]'); await settle(600);

    // ═══ 4 · a MIXED order: monogram line first, engraved bar second ═══
    await page.click(`#egQueue .doneRow[data-kind=monogram][data-rid="${rid('mixed')}"]`);
    await page.waitForSelector('#egQueue .rvItem[data-mono] .mw', { timeout: 20000 });
    c = await card();
    check(c && c.mono && c.key === monoKey('mixed') && /Piece 1 of 2/i.test(c.strip) && /Monogram/.test(c.strip), 'its card says "Piece 1 of 2" and names the monogram in the strip: "' + (c && c.strip) + '"');
    await untilWidget(rid('mixed'), s => ['ready', 'check'].includes(s.state), 'the mixed order\'s monogram');
    await settle(1000); await shot('detail-mixed-1440');
    noSideways(await atPhone('detail-mixed-400'), 'a monogram card of a mixed order (with the strip)');
    await page.click('#egQueue .rvItem [data-a=next]'); await page.waitForFunction(k => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); return c && c.dataset.key === k; }, barKey('mixed'), { timeout: 20000 });
    c = await card();
    check(c && !c.mono && c.key === barKey('mixed') && /Piece 2 of 2/i.test(c.strip) && c.engApprove, 'Next from the monogram goes to the order\'s engraved bar (Piece 2 of 2, with its own Approve): ' + (c && c.strip));
    await settle(1200); await shot('detail-mixed-bar-1440');
    await page.click('#egQueue .rvItem [data-a=prev]'); await page.waitForFunction(k => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); return c && c.dataset.key === k && c.dataset.mono; }, monoKey('mixed'), { timeout: 20000 });
    check((await card()).mono, 'Back from the bar returns to the monogram');
    // a strip button goes to the other piece, both ways
    await page.click('#egQueue .rvItem .egPcTab[data-piece="' + barKey('mixed') + '"]'); await page.waitForFunction(k => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); return c && c.dataset.key === k && !c.dataset.mono; }, barKey('mixed'), { timeout: 20000 });
    await page.click('#egQueue .rvItem .egPcTab[data-piece="' + monoKey('mixed') + '"]'); await page.waitForFunction(k => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); return c && c.dataset.key === k && c.dataset.mono; }, monoKey('mixed'), { timeout: 20000 });
    check(true, 'the strip buttons go from the monogram to the bar and back');
    // Approve on the monogram: that piece's approval; the detail goes on to the bar of the SAME order, never another order
    writes = 0;
    await untilWidget(rid('mixed'), s => s.state === 'ready', 'the monogram ready');
    await page.click('#egQueue .rvItem[data-mono] [data-mw-approve]');
    await page.waitForFunction(k => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); return c && c.dataset.key === k; }, barKey('mixed'), { timeout: 60000 });
    await settle(900);
    c = await card();
    check(c && !c.mono && c.key === barKey('mixed') && /Piece 2 of 2/i.test(c.strip), 'the monogram approved: the detail goes on to the bar of the same order (not to another order): ' + (c && c.key));
    const ord = await page.evaluate(r => CharmNestEngraveOrder.piecesOf(r).map(p => p.kind + ':' + p.state).join(' '), rid('mixed'));
    check(/monogram:approved/.test(ord) && /to-approve/.test(ord), 'the order model: the monogram is approved, the bar is still to approve: ' + ord);
    check(await page.evaluate(k => { const s = Engrave.monogramUI().status(Orders.rows().find(r => r.key === k)); return s.approved; }, monoKey('mixed')), 'and the design is on the order (the service says approved)');
    // the guide: leaving an order with a piece still to approve is said, once, in one line (the bar is the piece left)
    await page.click('#egQueue .rvItem [data-a=next]'); await settle(600);
    c = await card();
    check(c && c.key === barKey('mixed') && /not approved yet/.test(c.guide), 'Next past the end of the order while the bar is not approved: the guide says so in one line and the card stays: "' + (c && c.guide) + '"');
    await page.click('#egQueue .rvItem [data-a="approve"]');
    await page.waitForFunction(k => { const j = Engrave.items().get(k); return j && j.state === 'approved'; }, barKey('mixed'), { timeout: 60000 });
    await settle(1500);
    const od = await page.evaluate(r => CharmNestEngraveOrder.countOf(r), rid('mixed'));
    check(od.n === 2 && od.approved === 2 && od.left === 0, 'the bar approved too: the order is done, both pieces approved: ' + JSON.stringify(od));
    // NODECIDED: both pieces of the finished order stay in the ONE list, the monogram row with its picture and letters and "Approved"; the row opens the same card with the widget (its own Change)
    await openList(); await settle(1200);
    const dr = await page.evaluate(() => [...document.querySelectorAll('#egQueue .egPlacementList .doneRow')].map(r => ({ kind: r.dataset.kind || 'engraved', rid: r.dataset.rid, decided: r.classList.contains('isDecided'), thumb: !!r.querySelector('[data-mw-thumb]'), text: r.innerText.replace(/\s+/g, ' ').trim() })));
    check(dr.filter(r => r.rid === rid('mixed')).length === 2 && dr.filter(r => r.rid === rid('mixed')).every(r => r.decided) && dr.some(r => r.rid === rid('mixed') && r.kind === 'monogram' && r.thumb && /A P V|J Q X/.test(r.text) && /approved/i.test(r.text)), 'the one list holds both rows of the finished mixed order (calm, nothing left to decide), the monogram one with its charm, its letters and "Approved": ' + dr.map(r => r.kind[0] + r.rid.slice(-3) + (r.decided ? 'D' : 'P')).join(' '));
    check(await page.evaluate(() => !document.querySelector('#egDone, #egBacks, .decidedRow, .doneDetail, [data-a=reopen], [data-a=monoChange], .egTab[data-tab="done"]')), 'and there is no Decided tab, row, detail, Reopen or Change under a row anywhere');
    await shot('approved-in-list-1440');
    await page.click(`#egQueue .egPlacementList .doneRow[data-kind=monogram][data-rid="${rid('mixed')}"]`);
    await page.waitForSelector('#egQueue .rvItem[data-mono] .mw', { timeout: 15000 }); await settle(900);
    const dd = await page.evaluate(() => { const c = document.querySelector('#egQueue .rvItem[data-mono]'), m = c && c.querySelector('.mw'); return { card: c && c.dataset.state, mode: m && m.dataset.mode, state: m && m.dataset.state, change: !!(m && m.querySelector('[data-mw-change]')), approve: !!(c && c.querySelector('[data-mw-approve]:not([disabled])')) }; });
    check(dd.mode === 'detail' && dd.state === 'approved' && dd.change, 'the approved monogram\'s row opens the same card with the widget under it, approved, with its own Change: ' + JSON.stringify(dd));
    await shot('approved-card-1440');
    noSideways(await atPhone('approved-card-400'), 'an approved monogram\'s card');
    check(await page.evaluate(() => { Engrave.render(); Engrave.render(); const c = document.querySelector('#egQueue .rvItem[data-mono]'); return !!(c && c.querySelector('.mw')); }), 'a re-render of the tab keeps the open approved monogram card');

    // the other way round: the bar first, then the monogram (mixedB)
    await openList(); await settle(1500);
    await page.click(`#egQueue .doneRow[data-kind=engraved][data-rid="${rid('mixedB')}"], #egQueue .doneRow:not([data-kind])[data-rid="${rid('mixedB')}"]`);
    await page.waitForFunction(k => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); return c && c.dataset.key === k; }, barKey('mixedB'), { timeout: 20000 });
    check(/Piece 1 of 2/i.test((await card()).strip), 'mixed order B: the bar is piece 1 of 2');
    await settle(1200);
    // Next from the bar reaches the monogram (same order); Next again would leave the order with 2 pieces not approved: the guide says it, "Go to piece 1" goes back to the bar, Leave goes on
    await page.click('#egQueue .rvItem [data-a=next]'); await page.waitForFunction(k => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); return c && c.dataset.key === k && c.dataset.mono; }, monoKey('mixedB'), { timeout: 20000 });
    await untilWidget(rid('mixedB'), s => ['ready', 'check'].includes(s.state), 'the second monogram made'); await settle(600);
    await page.click('#egQueue .rvItem[data-mono] [data-a=next]'); await settle(700);
    c = await card();
    check(c && c.mono && /2 pieces of this order are not approved yet/.test(c.guide), 'Next past the last piece with 2 pieces not approved: ONE guide line on the monogram card, the card stays: "' + (c && c.guide) + '"');
    await page.click('#egQueue .rvItem[data-mono] .egGuide [data-g=go]');
    await page.waitForFunction(k => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); return c && c.dataset.key === k && !c.dataset.mono; }, barKey('mixedB'), { timeout: 20000 });
    check(true, '"Go to piece 1" opens the bar of the order');
    await settle(900);
    await page.click('#egQueue .rvItem [data-a="approve"]');
    await page.waitForFunction(k => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); return c && c.dataset.key === k && c.dataset.mono; }, monoKey('mixedB'), { timeout: 60000 });
    c = await card();
    check(c && c.mono && c.mw && /Piece 2 of 2/i.test(c.strip), 'the bar approved: the detail goes on to the monogram of the same order (a piece with no job): ' + (c && c.strip));
    await untilWidget(rid('mixedB'), s => s.state === 'ready', 'the second monogram');
    await page.click('#egQueue .rvItem[data-mono] [data-mw-approve]');
    await page.waitForFunction(r => CharmNestEngraveOrder.countOf(r).left === 0, rid('mixedB'), { timeout: 60000 });
    check(true, 'and its Approve finishes the order: no piece left (' + JSON.stringify(await page.evaluate(r => CharmNestEngraveOrder.countOf(r), rid('mixedB'))) + ')');

    // ═══ 5 · an order WITHOUT a monogram, exactly as before ═══
    await page.evaluate(k => { CN.setMode('engrave'); Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: k, pin: k }); Engrave.render(); }, key('back'));
    await page.waitForSelector('#egQueue .rvItem[data-kind=placement]', { timeout: 20000 }); await settle(1200);
    c = await card();
    check(c && !c.mono && !c.mw && c.faces && c.engApprove && c.key === key('back') && c.strip === '', 'an order with no monogram: the engraved card with its panel and its own Approve, no widget, no strip: ' + JSON.stringify({ mono: c && c.mono, faces: c && c.faces, approve: c && c.engApprove }));
    await shot('detail-plain-1440');

    // ═══ 6 · the old doors ═══
    const openRes = await page.evaluate(k => { Engrave.openMonogram(k); return { tab: Engrave.view().tab, focus: Engrave.view().focus, list: Engrave.view().list }; }, monoKey('mono2'));
    await page.waitForSelector('#egQueue .rvItem[data-mono] .mw', { timeout: 20000 });
    check(openRes.tab === 'place' && openRes.list === false && (await card()).key === monoKey('mono2'), 'Engrave.openMonogram(lineKey) lands on that line\'s card in Placements: ' + JSON.stringify(openRes));
    await page.evaluate(() => { Engrave.restoreView({ tab: 'mono', chosen: true }); Engrave.render(); }); await settle(800);
    check((await page.evaluate(() => Engrave.view().tab)) === 'place' && !(await page.evaluate(() => !!document.querySelector('#egMono, .egTab[data-tab="mono"]'))), 'a saved view with the old "mono" tab lands in Placements');
    // approve mono2 so there is an approved one, then open it: its own card
    await page.evaluate(k => { Engrave.openMonogram(k); }, monoKey('mono2')); await page.waitForSelector('#egQueue .rvItem[data-mono] .mw', { timeout: 20000 });
    await untilWidget(rid('mono2'), s => s.state === 'ready', 'OSR ready');
    await page.click('#egQueue .rvItem[data-mono] [data-mw-approve]'); await page.waitForFunction(k => Engrave.monogramUI().status(Orders.rows().find(r => r.key === k)).approved, monoKey('mono2'), { timeout: 60000 }); await settle(1500);
    const dec = await page.evaluate(k => { Engrave.openMonogram(k); return { tab: Engrave.view().tab, list: Engrave.view().list, focus: Engrave.view().focus }; }, monoKey('mono2')); await settle(1200);
    check(dec.tab === 'place' && dec.list === false && dec.focus === monoKey('mono2') && await page.evaluate(k => { const c = document.querySelector('#egQueue .rvItem[data-mono]'); return !!(c && c.dataset.key === k && c.querySelector('.mw') && c.dataset.state === 'approved'); }, monoKey('mono2')), 'openMonogram of an approved line opens its own card, the widget in it, approved (no Decided row): ' + JSON.stringify(dec));
    check(await page.evaluate(() => Engrave.reviewedRows()) >= 3, 'Engrave.reviewedRows() still counts the monograms with nothing left to decide: ' + (await page.evaluate(() => Engrave.reviewedRows())));

    // (7 · phone width: checked above, beside each picture)
    console.log('   page errors:', JSON.stringify(w.errors), '· AI / network writes:', writes);
    check(w.errors.length === 0, 'no page errors');
    check(writes === 0, 'no call to Claude, OpenAI or Etsy was made');
  } catch (e) { fails.push('crashed: ' + (e && e.stack || e)); console.log('  ✗ crashed: ' + (e && e.stack || e)); }
  finally { await w.close(); }
  finish();
})().catch(e => { console.error(e); process.exit(1); });

function finish() {
  console.log(`\nmonogram-engrave-tab: ${nCheck - fails.length}/${nCheck} checks passed` + (fails.length ? ' · FAILED: ' + fails.join(' | ') : ''));
  process.exit(fails.length ? 1 : 0);
}
