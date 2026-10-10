// DISCCYCLE (discs-1010): a counted-disc order ("2 Disc", "3 Disc") in the ENGRAVE tab, in real headless Chromium, offline.
//
// Paul, 10 Oct 2026: "adapt the entire application to be able to cycle through all the chosen discs one by one in the engraving tab". The real page (the real CSS, the real
// Engrave module, the real placement card and the page's own fit) runs on the repo's fake site (tests/charm-nest/bridge-server.cjs, in-memory, zero network; nothing is
// written anywhere, no paid call, no live call). The orders are fixtures: a 2-disc necklace ("Tag 1: J, Tag 2: Q", Typewriter / Stylish), a 3-disc necklace, an earring pair and a
// single pendant around them.
//   node tests/charm-nest/disc-cycle-browser.cjs        (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>; SHOTS=<dir> to keep the screenshots; without a browser nothing runs)
// What it proves: the list has ONE row per necklace that tells its discs ("Disc 1 of 2 · J", "Disc 2 of 2 · Q", each with its font); the editor header has the Disc 1 | Disc 2 (| Disc 3)
// switch, the work area (the canvas) keeps its room, nothing overflows the card; the switch, Back and Next move disc by disc through every order; each disc has its own words, font and
// Approve; "Approve all discs" appears only for identical words and wakes only once every disc was on screen; the counters count orders; the Decided tab lists the discs with a Reopen each.
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const root = path.join(__dirname, '../..');

(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { try { ({ chromium } = require('/opt/node22/lib/node_modules/playwright/node_modules/playwright-core')); } catch (__) { console.log('  – no playwright-core: the browser checks were not run'); return; } }
  const shots = process.env.SHOTS || ''; if (shots) fs.mkdirSync(shots, { recursive: true });
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] }), { sorterOrigin } = srv;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium', args: ['--no-sandbox'] });
  const fails = []; let checks = 0;
  const check = (ok, msg) => { checks++; if (!ok) fails.push(msg); };
  try {
    const ctx = await browser.newContext({ viewport: { width: 1500, height: 1000 } });
    const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
    await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
    await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
    await ctx.route(/fonts\.googleapis|fonts\.gstatic/, r => r.fulfill({ status: 200, contentType: 'text/css', body: '' }));
    await ctx.route(/^https:\/\/firebasestorage\.googleapis\.com\//, r => r.fulfill({ status: 404, body: 'none' }));
    await ctx.addInitScript(({ sorter }) => {
      if (location.origin === sorter) localStorage.setItem('cn.employee', 'Tester');
      window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {};
      if (window.Notification) { try { Object.defineProperty(window, 'Notification', { value: undefined }); } catch (_) {} }
    }, { sorter: sorterOrigin });
    const page = await ctx.newPage(), errors = [];
    page.on('pageerror', e => errors.push('pageerror: ' + e.message));
    await page.goto(`${sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.CN && window.CN.S && window.Engrave && window.CharmNestEngraveFit && window.CharmNestPieceSwitch && window.CharmNestEngraveRows);
    await page.evaluate(async () => { CN.S.settings.review = 'off'; await Engrave.loadFonts(); });

    // the fixtures: every job is the page's own (a fit computed by the page's own engine on a round disc), held the way the Engrave module holds it
    await page.evaluate(() => {
      const ring = (cx, cy, r) => [...Array(32).keys()].map(i => [cx + r * Math.cos(i / 32 * 2 * Math.PI), cy + r * Math.sin(i / 32 * 2 * Math.PI)]);
      const mem = pts => { const xs = pts.map(p => p[0]), ys = pts.map(p => p[1]); return { kind: 'path', layer: 'CUT', closed: true, stroke: true, strokeRGB: [1, 0, 0], subpaths: [pts.map((p, i) => [i ? 'l' : 'm', p]).concat([['h']])], bbox: [Math.min(...xs), Math.min(...ys), Math.max(...xs), Math.max(...ys)] }; };
      const make = (outline, holes) => { const o = mem(outline); return { outline: o, members: [o, ...holes.map(mem)], bbox: o.bbox }; };
      const fit = (c, words) => CharmNestEngraveFit.calculate({ charm: c, lines: [words], lineMode: 'auto', viewOptions: CharmNestGeom.viewOptionsFor({ entry: {} }), maskOptions: { marginMm: .8, keepOut: [] },
        opts: { minCapMm: 1.6, maxHeightFrac: .4, lineGap: .216, minStrokeMm: 0, minGapMm: 0, tryRotated: true } }, Engrave.fonts, CharmNestGeom);
      const sideRow = (parent, slot, job, ids) => { const row = Object.create(parent); Object.defineProperties(row, { key: { value: parent.key + '#' + slot, enumerable: true, writable: true, configurable: true }, slot: { value: slot, enumerable: true, configurable: true }, side: { value: null, enumerable: true, configurable: true }, parentRow: { value: parent, enumerable: false, configurable: true }, poolIds: { value: ids, enumerable: true, configurable: true }, engrave: { get() { return job.engraveRec; }, set(v) { job.engraveRec = v; }, enumerable: true, configurable: true } }); return row; };
      const disc = make(ring(30, 30, 24), [ring(30, 51, 2)]), state = { jobs: {}, lines: {} };
      const line = (n, rid, sku, title, opt, words, fonts, kind, lineFont) => {
        const key = `${rid}_${n}`, parent = { key, state: 'pooled', poolIds: [], order: { receiptId: rid }, line: { sku, title, variations: [{ name: 'Necklace options', value: opt }], transactionId: String(n), quantity: 1 },
          spec: { designSku: sku, personalization: ['Tag 1: ' + words[0]], form: kind === 'pair' ? 'earring' : 'necklace', size: '', quantity: 1, font: lineFont ? { asked: lineFont, id: '', name: '', source: 'rule:font-asked' } : undefined }, engrave: { state: 'review' } };
        const jobs = words.map((w, i) => {
          const slot = kind === 'pair' ? (i ? 'R' : 'L') : kind === 'single' ? null : 'D' + (i + 1), res = fit(disc, w), pid = 'p-' + key + '-' + (i + 1), jk = slot ? key + '#' + slot : key;
          const job = { key: jk, rowKey: slot ? key : undefined, slot: slot || undefined, groupKey: rid + ':' + n, state: 'review', lines: [w], lineInput: [w], text: w, view: res.view, mask: res.mask, fit: res.fit, editCharm: disc, copies: [pid], confidence: .9, source: 'personalization', questions: [], requests: {}, backs: [],
            verify: { geometry: { ok: true } }, orientVersion: CharmNestGeom.ORIENT, materialVersion: 2, wantSize: res.fit && res.fit.size, engraveRec: { needed: true, state: 'review', approved: false } };
          if (fonts[i] && kind !== 'single') job.font = { name: fonts[i], asked: fonts[i] };
          parent.poolIds.push(pid); job.row = slot ? sideRow(parent, slot, job, [pid]) : parent; if (!slot) parent.engrave = { needed: true, state: 'review' };
          return job;
        });
        state.lines[rid] = { parent, jobs }; return jobs;
      };
      window.__fx = { line, state, ring, fit, disc };
      Engrave.items().clear();
      for (const j of [].concat(
        line(1, '4175370240', 'INITIAL_DISC_4571', 'Initial Disc Necklace Gold Disc Personalized', 'ROSEGOLD - 2 Disc', ['J', 'Q'], ['', ''], 'discs', 'Typewriter'),
        line(2, '4172791262', 'INITIAL_8391', 'Initial Disc Necklace 3 discs', 'GOLD - 3 Disc', ['Mom', 'Dad', 'Sis'], ['Stylish', 'Stylish', 'Comic'], 'discs', 'Stylish'),
        line(3, '4171852053', 'DUCK_HUGGIE', 'Huggie Hoops Rubber Duck', 'GOLD', ['Mom', 'Mom'], ['', ''], 'pair', ''),
        line(4, '4174322410', 'ONE-PENDANT', 'Tiny Initial Pendant', 'GOLD', ['Ana'], [''], 'single', ''))) Engrave.items().set(j.key, j);
      for (const j of Engrave.items().values()) j.row.engrave = Object.assign({ needed: true, approved: false }, { state: j.state });
      CN.setMode('engrave');
    });
    const view = (o) => page.evaluate(o => { Engrave.restoreView(Object.assign({ tab: 'place', chosen: true, list: true, focus: null }, o)); Engrave.render(); }, o);
    const settle = () => page.evaluate(async () => { await new Promise(r => setTimeout(r, 200)); await new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))); await new Promise(r => setTimeout(r, 120)); });
    const shot = async (name) => { if (process.env.STEPS) console.log('shot', name, new Date().toISOString()); if (shots) await page.screenshot({ path: path.join(shots, name) }); };
    const keyOf = (rid, slot) => `${rid}_${{ '4175370240': 1, '4172791262': 2, '4171852053': 3, '4174322410': 4 }[rid]}${slot ? '#' + slot : ''}`;
    const cardInfo = () => page.evaluate(() => {
      const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); if (!c) return null;
      const R = el => { if (!el) return null; const b = el.getBoundingClientRect(); return { x: b.x, y: b.y, w: b.width, h: b.height, r: b.right, b: b.bottom }; };
      const rh = c.querySelector('.rh'), sw = c.querySelector('.egEarSwitch'), x = c.querySelector('.rh .x');
      return { key: c.dataset.key, kind: c.querySelector('.rh .kind').textContent, card: R(c), rh: R(rh), sw: R(sw), close: R(x), canvas: R(c.querySelector('.backHost canvas')), host: R(c.querySelector('.backHost')),
        tabs: [...c.querySelectorAll('.egEarSwitch .egEarTab')].map(b => ({ chip: b.querySelector('.egPiece').textContent, words: b.querySelector('.egEarWords').textContent, font: (b.querySelector('.egEarFont') || {}).textContent || '', stage: b.querySelector('.egEarStage').textContent, on: b.getAttribute('aria-pressed'), r: R(b) })),
        words: c.querySelector('[data-f="words"]') && c.querySelector('[data-f="words"]').value, both: c.querySelector('[data-a="approveBoth"]') ? { text: c.querySelector('[data-a="approveBoth"]').textContent, disabled: c.querySelector('[data-a="approveBoth"]').disabled, title: c.querySelector('[data-a="approveBoth"]').title } : null,
        approve: !!c.querySelector('[data-a="approve"]'), plainTag: (c.querySelector('.rh .reviewIdentity > .egPiece') || {}).textContent || null, scrollW: document.documentElement.scrollWidth, innerW: window.innerWidth };
    });
    const press = async (sel) => { if (process.env.STEPS) console.log('press', sel); await page.click(sel); await settle(); return cardInfo(); };
    const within = (inner, outer) => inner && outer && inner.x >= outer.x - 1 && inner.r <= outer.r + 1 && inner.y >= outer.y - 1 && inner.b <= outer.b + 1;

    // ── 1 · the list: one row per necklace, its discs told on it ──
    await view({ list: true }); await settle();
    const list = await page.evaluate(() => [...document.querySelectorAll('.egPlacementList .placementRow')].map(r => ({ rid: r.dataset.rid, pair: r.classList.contains('earPairRow'), ears: [...r.querySelectorAll('.egEar')].map(e => ({ chip: e.querySelector('.egPiece').textContent, w: e.querySelector('.w').textContent, font: (e.querySelector('[data-font]') || {}).textContent || '', stage: e.querySelector('[data-stage]').textContent })), open: r.dataset.open, w: r.getBoundingClientRect().width })));
    check(list.length === 4, `the list has 4 rows (two necklaces, a pair, a pendant), not 8: ${list.length}`);
    const two = list.find(r => r.rid === '4175370240'), three = list.find(r => r.rid === '4172791262');
    check(two && JSON.stringify(two.ears.map(e => [e.chip, e.w, e.font, e.stage])) === JSON.stringify([['DISC 1 of 2', 'J', 'Typewriter (asked)', 'Placement to check'], ['DISC 2 of 2', 'Q', 'Typewriter (asked)', 'Placement to check']]), `the 2-disc row tells its discs: ${JSON.stringify(two && two.ears)}`);
    check(three && JSON.stringify(three.ears.map(e => [e.chip, e.w, e.font])) === JSON.stringify([['DISC 1 of 3', 'Mom', 'Stylish'], ['DISC 2 of 3', 'Dad', 'Stylish'], ['DISC 3 of 3', 'Sis', 'Comic']]), `the 3-disc row tells its discs: ${JSON.stringify(three && three.ears)}`);
    check(two && two.open === keyOf('4175370240', 'D1'), 'a click on the row opens Disc 1');
    check(JSON.stringify(list.find(r => r.rid === '4171852053').ears.map(e => e.chip)) === '["LEFT EAR","RIGHT EAR"]', 'the pair row is as it was (Left, Right)');
    check(list.find(r => r.rid === '4174322410').ears.length === 0, 'the pendant row is as it was (no piece block)');
    check(await page.evaluate(() => document.querySelector('.egTab[data-tab="place"] b').textContent) === '4', 'the Placements badge counts orders: 4');
    await shot('DISCCYCLE-1-list.png');

    // ── 2 · the editor on the 3-disc order ──
    await page.click(`.placementRow[data-rid="4172791262"]`); await settle();
    let c = await cardInfo();
    check(c && c.key === keyOf('4172791262', 'D1'), 'the row opened on Disc 1');
    check(c.tabs.length === 3 && c.tabs.map(t => t.chip).join('|') === 'DISC 1 of 3|DISC 2 of 3|DISC 3 of 3', `Disc 1 | Disc 2 | Disc 3 in the header: ${JSON.stringify(c.tabs.map(t => t.chip))}`);
    check(c.tabs.map(t => t.on).join() === 'true,false,false', 'Disc 1 is the one pressed');
    check(c.tabs.map(t => t.words).join() === 'Mom,Dad,Sis' && c.tabs.map(t => t.font).join() === 'Stylish,Stylish,Comic', `each disc: its words and its font by name: ${JSON.stringify(c.tabs.map(t => [t.words, t.font]))}`);
    check(c.kind === '1 of 4 · 0 done', `the counter counts orders: ${c.kind}`);
    check(c.words === 'Mom' && c.approve && c.plainTag === null, 'Disc 1\'s own words box and Approve');
    check(within(c.sw, c.rh) && c.tabs.every(t => within(t.r, c.rh)) && c.scrollW <= c.innerW, `the switch stays inside the card header (switch ${JSON.stringify(c.sw)}, header ${JSON.stringify(c.rh)})`);
    check(c.close && c.sw && !(c.sw.x < c.close.r && c.sw.r > c.close.x && c.sw.y < c.close.b && c.sw.b > c.close.y), 'the switch does not cover the close button');
    check(c.canvas && c.canvas.w > 200 && c.canvas.h > 150 && within(c.canvas, c.host), `the work area keeps its room: canvas ${Math.round(c.canvas.w)} x ${Math.round(c.canvas.h)} inside its box`);
    check(c.both === null, 'different words on the discs: no "Approve all discs"');
    await shot('DISCCYCLE-2-three-discs-disc1.png');
    // the switch: Disc 2, Disc 3
    c = await press('.egEarTab:nth-child(2)'); check(c.key === keyOf('4172791262', 'D2') && c.words === 'Dad' && c.tabs.map(t => t.on).join() === 'false,true,false' && c.kind === '1 of 4 · 0 done', `the switch shows Disc 2's own card: ${c.key} ${c.words} ${c.kind}`);
    await shot('DISCCYCLE-3-three-discs-disc2.png');
    c = await press('.egEarTab:nth-child(3)'); check(c.key === keyOf('4172791262', 'D3') && c.words === 'Sis' && c.tabs[2].font === 'Comic', 'and Disc 3\'s');
    await shot('DISCCYCLE-4-three-discs-disc3.png');
    // Back / Next: every disc of every order in turn
    await view({ list: false, focus: keyOf('4175370240', 'D1') }); await settle();
    const walk = []; c = await cardInfo(); walk.push(c.key);
    for (let i = 0; i < 7; i++) { c = await press('[data-a="next"]'); walk.push(c.key); }
    const stops = [keyOf('4175370240', 'D1'), keyOf('4175370240', 'D2'), keyOf('4172791262', 'D1'), keyOf('4172791262', 'D2'), keyOf('4172791262', 'D3')];
    check(JSON.stringify(walk.slice(0, 5)) === JSON.stringify(stops), `Next goes disc by disc: ${JSON.stringify(walk.slice(0, 5))}`);
    check(walk[5] === keyOf('4171852053', 'L') && walk[6] === keyOf('4174322410', null) && walk[7] === keyOf('4175370240', 'D1'), `then the pair (one stop), the pendant, and wraps to Disc 1: ${JSON.stringify(walk.slice(5))}`);
    c = await press('[data-a="prev"]'); check(c.key === keyOf('4174322410', null), 'Back from Disc 1 of the first order wraps to the last stop');
    c = await press('[data-a="prev"]'); check(c.key === keyOf('4171852053', 'L'), 'Back again: the pair');
    c = await press('[data-a="prev"]'); check(c.key === keyOf('4172791262', 'D3'), 'Back into the 3-disc order lands on its LAST disc');
    c = await press('[data-a="prev"]'); check(c.key === keyOf('4172791262', 'D2') && c.kind === '1 of 4 · 0 done', `and goes on to Disc 2 of the same order: ${c.key}`);
    // ── 3 · the 2-disc order: J and Q, each with its own font ──
    await view({ list: false, focus: keyOf('4175370240', 'D1') }); await settle(); c = await cardInfo();
    check(c.tabs.length === 2 && c.tabs.map(t => [t.chip, t.words, t.font].join(' ')).join('|') === 'DISC 1 of 2 J Typewriter (asked)|DISC 2 of 2 Q Typewriter (asked)', `the 2-disc switch: ${JSON.stringify(c.tabs.map(t => [t.chip, t.words, t.font]))}`);
    check(c.words === 'J' && c.both === null, 'Disc 1: J, no "all discs" press (the words differ)');
    await shot('DISCCYCLE-5-two-discs-disc1.png');
    c = await press('.egEarTab:nth-child(2)'); check(c.words === 'Q' && c.key === keyOf('4175370240', 'D2'), 'Disc 2: Q');
    await shot('DISCCYCLE-6-two-discs-disc2.png');
    // ── 4 · "Approve all discs": identical words, and every disc has been shown ──
    await page.evaluate(() => { for (const j of [Engrave.items().get('4175370240_1#D1'), Engrave.items().get('4175370240_1#D2')]) { j.lines = ['Mia']; j.text = 'Mia'; j.lineInput = ['Mia']; const r = __fx.fit(__fx.disc, 'Mia'); j.fit = r.fit; j.view = r.view; j.mask = r.mask; j.verify = { geometry: { ok: true } }; } });
    await view({ list: false, focus: keyOf('4175370240', 'D1') }); await settle(); c = await cardInfo();
    check(c.both && c.both.text === 'Approve all discs' && c.both.disabled === true && /Look at Disc 2's placement first/.test(c.both.title), `identical words: the press is offered but asleep until Disc 2 was shown: ${JSON.stringify(c.both)}`);
    await shot('DISCCYCLE-7-approve-all-asleep.png');
    c = await press('.egEarTab:nth-child(2)');
    check(c.both && c.both.disabled === false && /each with its own seal and back file/.test(c.both.title), `both discs were on screen: the press is ready: ${JSON.stringify(c.both)}`);
    await shot('DISCCYCLE-8-approve-all-ready.png');
    // ── 5 · approve one disc on its own: the order stays in Placements with the other disc (a single piece's card), the decided disc is under Decided ──
    await page.evaluate(() => { const j = Engrave.items().get('4175370240_1#D1'); j.state = 'approved'; j.approvedBy = 'Tester'; j.approvedAt = Date.now(); j.backs = []; j.row.engrave = { needed: true, state: 'approved', approved: true }; Engrave.render(); });
    await settle(); c = await cardInfo();
    check(c.key === keyOf('4175370240', 'D2') && c.tabs.length === 0 && c.plainTag === 'DISC 2 of 2', `one disc approved: the editor stays on the order, on Disc 2, as a single piece's card: ${c.key} ${c.plainTag}`);
    check(c.kind === '2 of 5 · 1 done', `the counter counts orders: 1 decided row + the 4 still to settle: ${c.kind}`);
    await view({ tab: 'done', list: true }); await settle();
    const dec = await page.evaluate(() => [...document.querySelectorAll('#egDone .decidedRow')].map(r => ({ rid: r.dataset.rid, tag: (r.querySelector('.engravingOrder .egPiece') || {}).textContent, reopen: [...r.querySelectorAll('[data-a="reopen"]')].map(n => n.textContent), badge: document.querySelector('.egTab[data-tab="done"] b').textContent })));
    check(dec.length === 1 && dec[0].rid === '4175370240' && dec[0].tag === 'DISC 1 of 2' && JSON.stringify(dec[0].reopen) === '["Reopen"]' && dec[0].badge === '1', `Decided lists Disc 1 as a single piece's row, tagged: ${JSON.stringify(dec)}`);
    // both discs decided: ONE decided row with a Reopen each
    await page.evaluate(() => { const j = Engrave.items().get('4175370240_1#D2'); j.state = 'approved'; j.approvedBy = 'Tester'; j.approvedAt = Date.now(); j.backs = []; j.row.engrave = { needed: true, state: 'approved', approved: true }; Engrave.render(); });
    await settle();
    const dec2 = await page.evaluate(() => [...document.querySelectorAll('#egDone .decidedRow')].map(r => ({ rid: r.dataset.rid, chips: [...r.querySelectorAll('.egEar .egPiece')].map(n => n.textContent), reopen: [...r.querySelectorAll('[data-a="reopen"]')].map(n => n.textContent), words: [...r.querySelectorAll('.egEar .w')].map(n => n.textContent) })));
    check(dec2.length === 1 && JSON.stringify(dec2[0].chips) === '["DISC 1 of 2","DISC 2 of 2"]' && JSON.stringify(dec2[0].reopen) === '["Reopen Disc 1","Reopen Disc 2"]' && JSON.stringify(dec2[0].words) === '["Mia","Mia"]', `both decided: one Decided row, a Reopen each: ${JSON.stringify(dec2)}`);
    await shot('DISCCYCLE-9-decided.png');
    // ── 6 · a narrow window: the switch wraps instead of overflowing ──
    await page.setViewportSize({ width: 900, height: 700 });
    await view({ tab: 'place', list: false, focus: keyOf('4172791262', 'D2') }); await settle(); c = await cardInfo();
    check(c.scrollW <= c.innerW && c.tabs.every(t => within(t.r, c.card)), `900 px wide: the three discs stay inside the card (no sideways scroll): ${c.scrollW}/${c.innerW}`);
    await shot('DISCCYCLE-10-narrow.png');
    check(errors.length === 0, 'no page errors: ' + errors.join(' | '));
    console.log(`Disc cycle OK in Chromium: ${checks} checks (list rows, 2-disc and 3-disc editor, switch, Back/Next, Approve all discs, Decided, narrow window)`);
  } finally { await browser.close(); srv.close(); }
  if (fails.length) { console.error(`\n${fails.length} failed:\n - ${fails.slice(0, 40).join('\n - ')}`); process.exit(1); }
})().catch(e => { console.error(e); process.exit(1); });
