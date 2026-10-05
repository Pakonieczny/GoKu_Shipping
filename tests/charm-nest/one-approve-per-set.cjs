// ONE "Approve for laser cutting" button per set (Paul, 5 Oct 2026 13:01 UTC, round 12): "There should only be one 'approved for laser cutting'
// button per Set of Sheets. Only individual sheets that are not already part of a set are allowed to have their own 'approved for laser cutting'
// button." Round 2 had given every sheet of a set its own button, round 7 made them green and grey together; now a set has ONE, at the bottom of
// its card under the sheet columns, and a sheet that is part of a set has none (its small rail, its line and its '!' stay; no set-level progress bar).
//   Part 1 (browser, the app's own shell and CSS around the REAL LaserReview and set card code, fake LibraryFlow / Moving bar, offline):
//     a 3-sheet set shows exactly one button, in the set card, last, left aligned, and zero in its sheet columns; grey with the one plain reason that
//     names the blocking sheet and opens that sheet's '!'; green when every sheet is ready; one press = one approve call for the SET and one Moving
//     bar; the live read turns it green and grey without a flicker (same button, same place) and without moving a sheet column; a stale page's refusal
//     is said calmly; a set of one, two and three sheets; a sheet that is part of no set (a sheet card, a loose group) keeps its own; a sheet that
//     joins a set loses its own and one that leaves gets it back at once; 10K / 14K Include, held, committed and cut sets; 1440 and 390 px (never
//     clipped, no sideways scroll); no reserved gap where the sheets' buttons used to be; the '!' panel never leaves a strip of the set's button.
//   Part 2 (Node, the real LibraryFlow over the in-memory shop running the real charmNestLibrary): the page's one press (approve kind 'set') approves
//     the whole set (each sheet's own seal and the set's), is the same as "Move set to Laser cutting", a set of one sheet gets what its sheet always
//     got, Rose Gold keeps its own yes, a half set is refused on the page and on the server (409) and writes nothing.
//   Part 3: mutants that bring the old behaviour back (a button on every sheet of a set, none on a loose sheet, none on the set) are caught.
//   node tests/charm-nest/one-approve-per-set.cjs [playwright-core dir]   (PW_DIR=…, CHROMIUM=…; NODE_PATH=<jsdom is not needed>; SHOTS=<dir> saves screenshots)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const F = require('./library-issues-fixture.cjs');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
let chromium;
try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser check was not run'); process.exit(0); }
const SHOTS = process.env.SHOTS || '';
const BRIDGE = F.read('charm-nest-bridge.js');
// (a phone-width page is cut into one tall picture: the window is made as tall as the card for the moment of the shot, then put back)
const shot = async (target, name) => {
  if (!SHOTS) return; fs.mkdirSync(SHOTS, { recursive: true });
  const pg = typeof target.page === 'function' ? target.page() : target, vp = pg.viewportSize(), tall = vp && vp.width < 600;
  if (tall) { await pg.setViewportSize({ width: vp.width, height: 5200 }); await pg.waitForTimeout(300); }
  await target.screenshot({ path: path.join(SHOTS, name + '.png') });
  if (tall) { await pg.setViewportSize(vp); await pg.waitForTimeout(200); }
};

/* ── the records ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
// a sheet of `n` pieces; `backs` of its back engravings approved and saved (default all); `qr` false: its QR label is the press's own step
const sheet = (id, { metal = 'gold', n = 6, base = 4170250000, setId = 'set1', index = 1, seq = 1, backs = n, qr = true, ...extra } = {}) => {
  const s = F.sheet(id, { n, metal, base, setId, index, seq });
  s.backPool = s.backPool.slice(0, backs); s._backs = backs;
  if (!qr) s.label = { files: [] };
  return Object.assign(s, extra);
};
const rowsOf = sheets => sheets.flatMap(s => s.poolIds.map((p, i) => ({ key: p.replace(/_1$/, ''), order: { receiptId: p.split('_')[0], buyer: { name: F.NAMES[i % F.NAMES.length] } }, line: { title: 'Charm', listingId: 'L' + (i % 12) }, state: 'written', poolIds: [p],
  engrave: i < (s._backs == null ? s.poolIds.length : s._backs) ? { needed: true, state: 'approved', approved: true } : { needed: true, state: 'words', approved: false } })));
const SET = (ids, extra = {}) => ({ setId: 'set1', seq: 1, name: 'Set 1', day: '2026-10-03', sheetIds: ids, status: 'open', ...extra });
// Paul's picture (image10): GF Sheet 1, RG Sheet 1 and SS Sheet 1 in Set-1; SS has 7 of 25 back engravings approved ("blocked") or all of them ("ready": only the
// press's own QR label step is left, so the set is still In progress, and its one button is green)
const paul = ready => { const gf = sheet('gf1', { n: 20, qr: !ready }), rg = sheet('rg1', { metal: 'rose', n: 6, base: 4175250000 }), ss = sheet('ss1', { metal: 'silver', n: 25, base: 4180250000, backs: ready ? 25 : 7 }); return { sheets: [gf, rg, ss], set: SET(['gf1', 'rg1', 'ss1']) }; };

/* ── the page: cards = [{type:'set', set, sheets} | {type:'group', group, sheets} | {type:'flat', sheets:[one]}] ── */
async function scene(browser, { width = 1440, height = 900, cards, bridge = '' }) {
  const errors = [], { page, context } = await F.openPage(browser, { width, height, fake: false, errors, bridge });
  page.setDefaultTimeout(8000);
  if (width < 600) await page.evaluate(() => document.getElementById('app').classList.add('railOff'));   // (a person folds the rail on a phone)
  await page.evaluate(({ cards, rows, picture }) => {
    window.__rows = rows; window.__sheets = []; window.__sets = []; window.__approves = []; window.__bars = []; window.__pending = [];
    const L = window.LaserReview, body = document.getElementById('libBody'); L.sections(body);
    window.LibraryFlow = { approve: o => { window.__approves.push({ ...o }); return new Promise((res, rej) => window.__pending.push({ res, rej })); } };
    window.LibraryApprovalUI = { show: (host, plan, opts) => { window.__bars.push({ host, plan, opts }); }, hide() {} };
    for (const c of cards) { for (const s of c.sheets) { s.preview = picture; window.__sheets.push(s); L.record(s); } if (c.set) window.__sets.push(c.set); }
    for (const c of cards) {
      if (c.type === 'flat') {
        const r = c.sheets[0], item = document.createElement('article'); item.className = 'librarySheet'; item.dataset.laserCard = 'sheet'; item._laserSheets = [r.id];
        item.innerHTML = `<div class="libCard hoverItem" data-id="${r.id}"><div class="h"><span class="nm">Sheet ${r.sheetIndex}</span></div><img class="pv" alt="" src="${picture}"><div class="m"><span class="sheetBackStatus" data-sheet-status="${r.id}"></span></div></div>` + L.labels(r);
        L.place(item, L.canCut(r), body);
      } else { const st = c.set || c.group, card = window.Sets.libraryCard(st, c.sheets, c.sheets); L.place(card, L.group(st, c.sheets).ready, body); }
    }
    L.changed();
  }, { cards, rows: rowsOf(cards.flatMap(c => c.sheets)), picture: F.sheetPicture() });
  await page.waitForSelector('.flowBox'); await page.waitForTimeout(500);
  return { page, context, errors };
}
// what is on the page: every set card's buttons (its own and its sheets'), every sheet card's
const probe = page => page.evaluate(() => {
  const btn = b => ({ key: b.dataset.approveFor, mode: b.dataset.mode, grey: b.querySelector('[data-approve-btn]').getAttribute('aria-disabled') === 'true', why: b.querySelector('[data-approve-why]').textContent.trim(), link: !!b.querySelector('[data-approve-reason]'), busy: b.querySelector('[data-approve-btn]').getAttribute('aria-busy') === 'true' });
  const sets = [...document.querySelectorAll('.setCard')].map(c => ({ id: c._laserSet && c._laserSet.setId || '', real: !!(c._laserSet && c._laserSet.setId && !c._laserSet.standalone && !c._laserSet.working), area: (c.closest('[data-laser-area]') || { dataset: {} }).dataset.laserArea || '',
    boxes: [...c.querySelectorAll(':scope > .approveBox')].map(btn), lastIsBox: !!c.lastElementChild && c.lastElementChild.classList.contains('approveBox'),
    sheets: [...c.querySelectorAll('.librarySheet')].map(a => ({ id: a.querySelector('.libCard').dataset.id, boxes: [...a.querySelectorAll('.approveBox')].map(btn), rail: !!a.querySelector('.flowBox'), steps: a.querySelectorAll('.flowStep').length, now: (a.querySelector('.flowNow') || { textContent: '' }).textContent })),
    setRails: c.querySelectorAll(':scope > .flowBox, :scope > .sh .flowBox').length }));
  const flat = [...document.querySelectorAll('[data-laser-card="sheet"]')].map(a => ({ id: a._laserSheets[0], boxes: [...a.querySelectorAll('.approveBox')].map(btn), rail: !!a.querySelector('.flowBox') }));
  return { sets, flat, total: document.querySelectorAll('.approveBox').length };
});
const geo = page => page.evaluate(() => {
  const c = document.querySelector('.setCard'), box = c && c.querySelector(':scope > .approveBox'), btn = box && box.querySelector('[data-approve-btn]');
  if (!c || !box) return null;
  const r = e => { const b = e.getBoundingClientRect(); return { l: b.left, t: b.top, r: b.right, b: b.bottom, w: b.width, h: b.height }; };
  const arts = [...c.querySelectorAll('.librarySheet')], why = box.querySelector('[data-approve-why]');
  return { card: r(c), box: r(box), btn: r(btn), why: why.textContent ? r(why) : null, arts: arts.map(r), vw: innerWidth, scrollW: document.documentElement.scrollWidth, boxClip: box.scrollWidth > box.clientWidth + 1, pad: parseFloat(getComputedStyle(c).paddingLeft),
    gaps: arts.map(a => { const f = a.querySelector('.flowBox'); return f ? a.getBoundingClientRect().bottom - f.getBoundingClientRect().bottom : null; }), lastOfArticle: arts.map(a => a.lastElementChild.className) };
});

/* ── the checks every run (and every mutant) must pass ── */
function basics(p) {
  const bad = [], set = p.sets.find(s => s.id === 'set1'), loose = p.flat.find(f => f.id === 'loose'), joined = p.flat.find(f => f.id === 'joined');
  if (!set) return ['no set card on the page'];
  if (set.boxes.length !== 1 || set.boxes[0].key !== 'set:set1') bad.push(`the set card has ${set.boxes.length} button(s) of its own (${set.boxes.map(b => b.key)}), expected exactly set:set1`);
  if (!set.lastIsBox) bad.push('the set\'s button is not the last thing in its card');
  for (const sh of set.sheets) if (sh.boxes.length) bad.push(`${sh.id}, a sheet that is part of the set, has ${sh.boxes.length} Approve button(s) of its own`);
  for (const sh of set.sheets) if (!sh.rail || sh.steps !== 7) bad.push(`${sh.id} lost its small rail`);
  if (set.setRails) bad.push('a set-level progress bar is back');
  if (!loose || loose.boxes.length !== 1) bad.push(`the sheet that is part of no set has ${loose ? loose.boxes.length : 'no card'} button(s), expected its own`);
  if (!joined || joined.boxes.length !== 0) bad.push(`a sheet card of a sheet that is part of a set has ${joined ? joined.boxes.length : 'no card'} button(s), expected none`);
  return bad;
}
const basicCards = () => { const p = paul(false); return [{ type: 'set', set: p.set, sheets: p.sheets }, { type: 'flat', sheets: [sheet('loose', { setId: null, n: 4, base: 4190250000, qr: false })] }, { type: 'flat', sheets: [sheet('joined', { setId: 'set9', n: 4, base: 4195250000, qr: false })] }]; };

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    // 0. the audit: nothing but the Library's one piece of code ever draws an Approve box
    for (const f of fs.readdirSync(root).filter(x => /^charm-nest-.*\.js$/.test(x) && x !== 'charm-nest-bridge.js')) {
      const src = fs.readFileSync(path.join(root, f), 'utf8').replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/[^\n]*/g, '');
      assert(!/className\s*=\s*['"][^'"]*approveBox|classList\.add\([^)]*approveBox|<[a-z]+[^>]*(?:class=["']approveBox|data-approve-btn)|setAttribute\(\s*['"]data-approve-btn|dataset\.approve(?:For|Btn)\s*=|approveFor\s*=/.test(src), `${f} draws an Approve box of its own (it may only read the one the bridge draws)`);
    }
    assert.equal((BRIDGE.match(/makeApproveBox\(/g) || []).length, 2, 'one definition and one call: syncBox is the only place an Approve box is made');
    assert.equal((BRIDGE.match(/[^\w]syncBox\(/g) || []).length, 3, 'syncBox: its definition, and two calls in syncApprove (a sheet that is part of no set, a set)');

    // 1. Paul's picture, not ready (image10 asked for ONE button, not three): GF ready, RG ready, SS 7 of 25 back engravings
    for (const width of [1440, 390]) {
      const tag = `${width}px`, { page, context, errors } = await scene(browser, { width, height: width === 390 ? 844 : 900, cards: [{ type: 'set', ...paul(false) }] });
      const p = await probe(page), set = p.sets[0];
      assert.equal(p.total, 1, `${tag}: exactly one Approve button on the page`); assert.deepEqual(set.boxes.map(b => b.key), ['set:set1'], `${tag}: in the set card`); assert(set.lastIsBox, `${tag}: last in the card, under the columns`);
      assert.deepEqual(set.sheets.map(s => s.boxes.length), [0, 0, 0], `${tag}: none in GF Sheet 1, RG Sheet 1 or SS Sheet 1`);
      assert.deepEqual(set.sheets.map(s => [s.rail, s.steps]), [[true, 7], [true, 7], [true, 7]], `${tag}: every sheet keeps its own small rail`); assert.equal(set.setRails, 0, `${tag}: no set-level progress bar`);
      assert(set.sheets.every(s => /step \d of 7/.test(s.now)), `${tag}: and its "step N of 7" line`);
      const b = set.boxes[0]; assert.equal(b.grey, true, `${tag}: grey`); assert.equal(b.mode, 'blocked');
      assert.equal(b.why, 'SS Sheet 1 · back engravings 7 of 25', `${tag}: one plain reason naming the blocking sheet and what is missing`); assert.equal(b.link, true, `${tag}: and a link`);
      assert(!/\blines?\b/i.test(b.why), 'pieces, never lines');
      // where it sits: under every sheet column, left aligned with the first one, whole on the screen, no sideways scroll, no empty gap under the columns' rails
      const g = await geo(page);
      assert(g.btn.t >= Math.max(...g.arts.map(a => a.b)) - 1, `${tag}: the button is under every sheet column (${g.btn.t} vs ${Math.max(...g.arts.map(a => a.b))})`);
      assert(Math.abs(g.btn.l - g.arts[0].l) <= 2.5 && Math.abs(g.btn.l - (g.card.l + g.pad + 1)) <= 3, `${tag}: left aligned with the first sheet column (${g.btn.l} vs ${g.arts[0].l})`);
      assert(g.btn.r <= g.vw - 1 && g.btn.l >= 0 && !g.boxClip && g.scrollW <= g.vw, `${tag}: never clipped, no sideways scroll (${g.btn.r} of ${g.vw}, page ${g.scrollW})`);
      assert(g.btn.w <= 300, `${tag}: the button keeps its look and size (${g.btn.w})`);
      assert(g.lastOfArticle.every(c => /flowBox/.test(c)) && g.gaps.every(x => x !== null && x <= 2), `${tag}: nothing is reserved where the sheets' buttons used to be: each column ends at its rail (${JSON.stringify(g.gaps)})`);
      if (width === 1440) assert(g.why && Math.abs((g.why.t + g.why.b) / 2 - (g.btn.t + g.btn.b) / 2) <= 6 && g.why.l > g.btn.r, `${tag}: the one plain line stands beside the button, on its row`);
      // a grey button presses nothing; its line opens the blocking sheet's '!' panel
      await page.click('.setCard > .approveBox [data-approve-btn]', { force: true }); await page.waitForTimeout(100);
      assert.equal(await page.evaluate(() => window.__approves.length), 0, `${tag}: a grey button presses nothing`);
      await page.click('.setCard > .approveBox button[data-approve-reason]'); await page.waitForTimeout(500);
      assert.equal(await page.evaluate(() => document.querySelector('.lisPanel') && document.querySelector('.lisPanel').getAttribute('data-issues-for')), 'sheet:ss1', `${tag}: the line opens SS Sheet 1's '!' panel`);
      await page.keyboard.press('Escape'); await page.waitForTimeout(300);
      await shot(page.locator('.setCard'), `set3-not-ready-${width}`); if (width === 1440) await shot(page, `set3-not-ready-${width}-page`);
      assert.deepEqual(errors, [], tag); await context.close();
    }

    // 2. green: every sheet ready to be approved (the QR label is the press's own step). ONE press, one call for the SET, one Moving bar, all three sheets move together
    for (const width of [1440, 390]) {
      const tag = `${width}px`, { page, context, errors } = await scene(browser, { width, height: width === 390 ? 844 : 900, cards: [{ type: 'set', ...paul(true) }] });
      let p = await probe(page), b = p.sets[0].boxes[0];
      assert.equal(p.total, 1, tag); assert.equal(b.grey, false, `${tag}: green`); assert.equal(b.mode, 'ready'); assert.equal(b.why, '', `${tag}: no reason line when it can be pressed`);
      assert.deepEqual(p.sets[0].sheets.map(s => s.boxes.length), [0, 0, 0], `${tag}: none on a sheet, green or not`);
      assert.equal(await page.evaluate(() => getComputedStyle(document.querySelector('.approveBox [data-approve-btn]')).backgroundColor !== getComputedStyle(document.body).backgroundColor), true);
      await shot(page.locator('.setCard'), `set3-ready-${width}`); if (width === 1440) await shot(page, `set3-ready-${width}-page`);
      // one press, however many times it is pressed
      await page.click('.setCard > .approveBox [data-approve-btn]'); await page.click('.setCard > .approveBox [data-approve-btn]', { force: true });
      assert.deepEqual(await page.evaluate(() => window.__approves), [{ kind: 'set', id: 'set1', by: 'Tester' }], `${tag}: one approve call, for the whole SET`);
      p = await probe(page); b = p.sets[0].boxes[0]; assert.equal(b.busy, true, `${tag}: it shows the one running approval`);
      assert.equal(await page.evaluate(() => document.querySelector('.approveBox [data-approve-btn]').textContent), 'Approving…'); assert(await page.$('.approveBox [data-approve-btn] .spin'), `${tag}: with a small spinner`);
      // the press made the QR label and sealed the set: every sheet is ready, the plan is shown on the set's card (one bar), the set moves to Laser cutting as one
      await page.evaluate(() => { window.__sheets = window.__sheets.map(s => ({ ...s, label: { files: [{ path: 'qr.png', url: 'https://example.com/qr.png', payload: 'x', orders: s.orders }] }, updatedAt: 9 })); window.__pending[0].res({ ok: true, auto: [{ key: 'seal', label: 'Ready seal recorded by Tester' }], needs: [], confirm: [], notes: [] }); });
      await page.waitForTimeout(300);
      assert.deepEqual(await page.evaluate(() => [window.__bars.length, window.__bars[0].host.className, window.__bars[0].opts.title, window.__bars[0].opts.where, window.__bars[0].host === document.querySelector('.setCard')]), [1, 'setCard', 'Approve for laser cutting', 'end', true], `${tag}: ONE Moving bar, on the set's card, under its button`);
      await page.evaluate(() => window.LaserReview.nudge()); await page.waitForTimeout(1000);
      p = await probe(page); assert.equal(p.sets[0].area, 'ready', `${tag}: the whole set reached Laser cutting together`); assert.equal(p.total, 0, `${tag}: and there is no button there`);
      assert.deepEqual(errors, [], tag); await context.close();
    }

    // 3. the live read (about every 3 s): grey and green again without a flicker or a jump: the same button, the same place, no column moves; the keyboard stays on it
    // (GF Sheet 1 has no QR label yet, so the set stays In progress when SS's last back engraving is approved: all it waits for is the press)
    { const p0 = paul(false); p0.sheets[0] = sheet('gf1', { n: 20, qr: false });
      const { page, context, errors } = await scene(browser, { cards: [{ type: 'set', ...p0 }] });
      await page.focus('.setCard > .approveBox [data-approve-btn]');
      const state = async () => page.evaluate(() => { const c = document.querySelector('.setCard'), b = c.querySelector(':scope > .approveBox'), r = e => { const x = e.getBoundingClientRect(); return [x.left, x.top, x.width, x.height].map(v => Math.round(v * 10) / 10); }; return { arts: [...c.querySelectorAll('.librarySheet')].map(r), box: r(b), card: r(c), node: b === window.__node, focus: document.activeElement === b.querySelector('[data-approve-btn]') }; });
      await page.evaluate(() => { window.__node = document.querySelector('.setCard > .approveBox'); window.__mut = []; new MutationObserver(l => window.__mut.push(...l.map(m => (m.target.closest ? (m.target.closest('.libCard') ? 'card' : m.target.closest('.sheetsRow') ? 'row' : 'other') : 'other')))).observe(document.querySelector('.sheetsRow'), { childList: true, subtree: true, attributes: true }); });
      const before = await state();
      // SS's backs all approved elsewhere: the live read turns the one button green
      await page.evaluate(({ full, rows }) => { window.__rows = rows; window.__sheets = window.__sheets.map(s => s.id === 'ss1' ? { ...s, backPool: full, updatedAt: 5 } : s); window.LaserReview.nudge(); }, { full: sheet('ss1', { metal: 'silver', n: 25, base: 4180250000 }).backPool, rows: rowsOf([sheet('gf1', { n: 20, qr: false }), sheet('rg1', { metal: 'rose', n: 6, base: 4175250000 }), sheet('ss1', { metal: 'silver', n: 25, base: 4180250000 })]) });
      await page.waitForTimeout(1100);
      let p = await probe(page), after = await state();
      assert.equal(p.sets[0].boxes[0].grey, false, 'the live read turned the one button green'); assert.equal(p.sets[0].boxes[0].why, '');
      assert.equal(after.node, true, 'the same button, not drawn again'); assert.equal(after.focus, true, 'the keyboard stayed on it');
      assert.deepEqual(after.arts, before.arts, 'no sheet column moved or changed size'); assert.deepEqual(after.box.slice(0, 2), before.box.slice(0, 2), 'the button stayed where it was'); assert.equal(after.box[3], before.box[3], 'with the same height: nothing below it jumps');
      // and grey again
      await page.evaluate(({ part, rows }) => { window.__rows = rows; window.__sheets = window.__sheets.map(s => s.id === 'ss1' ? { ...s, backPool: part, updatedAt: 8 } : s); window.LaserReview.nudge(); }, { part: sheet('ss1', { metal: 'silver', n: 25, base: 4180250000, backs: 24 }).backPool, rows: rowsOf([sheet('gf1', { n: 20, qr: false }), sheet('rg1', { metal: 'rose', n: 6, base: 4175250000 }), sheet('ss1', { metal: 'silver', n: 25, base: 4180250000, backs: 24 })]) });
      await page.waitForTimeout(1100);
      p = await probe(page); const again = await state();
      assert.equal(p.sets[0].boxes[0].grey, true); assert.equal(p.sets[0].boxes[0].why, 'SS Sheet 1 · back engravings 24 of 25'); assert.equal(again.node, true); assert.equal(again.focus, true, 'aria-disabled, not disabled: the keyboard stays on the grey button');
      assert.deepEqual(again.arts, before.arts, 'no sheet column moved'); assert.deepEqual(again.box.slice(0, 2), before.box.slice(0, 2));
      // a read that says nothing new redraws nothing of the button
      await page.evaluate(() => { window.__mut.length = 0; window.__box = []; new MutationObserver(l => window.__box.push(l.length)).observe(document.querySelector('.setCard > .approveBox'), { childList: true, subtree: true, attributes: true, characterData: true }); window.LaserReview.nudge(); });
      await page.waitForTimeout(1000); assert.deepEqual(await page.evaluate(() => window.__box), [], 'a read that says nothing new touches nothing of the button');
      assert.deepEqual(errors, []); await context.close(); }

    // 4. a stale page: the server's refusal (409) of a half set is said calmly in place, the button works again, the next read turns it grey
    { const { page, context, errors } = await scene(browser, { cards: [{ type: 'set', ...paul(true) }] });
      await page.evaluate(() => { delete window.LibraryApprovalUI; });
      await page.click('.setCard > .approveBox [data-approve-btn]');
      await page.evaluate(() => window.__pending[0].rej(Object.assign(new Error('Set 1 is approved together, so every sheet of it must be ready first: SS Sheet 1 · back engravings 24 of 25'), { status: 409 }))); await page.waitForTimeout(300);
      const said = await page.evaluate(() => { const pl = document.querySelector('.setCard > .approveBox [data-approve-plan]'); return { hidden: pl.hidden, text: pl.textContent.replace(/\s+/g, ' '), role: pl.getAttribute('role'), live: pl.getAttribute('aria-live'), alerts: document.querySelectorAll('[role="alert"]').length, btnBusy: document.querySelector('.setCard > .approveBox [data-approve-btn]').getAttribute('aria-busy') }; });
      assert.equal(said.hidden, false, 'the refusal is shown on the card'); assert.match(said.text, /Could not approve for laser cutting/); assert.match(said.text, /SS Sheet 1 · back engravings 24 of 25/); assert.equal(said.role, 'status'); assert.equal(said.live, 'polite'); assert.equal(said.alerts, 0, 'no alarm'); assert.equal(said.btnBusy, 'false', 'the button is free again');
      await page.evaluate(({ part, rows }) => { window.__rows = rows; window.__sheets = window.__sheets.map(s => s.id === 'ss1' ? { ...s, backPool: part, updatedAt: 8 } : s); window.LaserReview.nudge(); }, { part: sheet('ss1', { metal: 'silver', n: 25, base: 4180250000, backs: 24 }).backPool, rows: rowsOf([sheet('gf1', { n: 20, qr: false }), sheet('rg1', { metal: 'rose', n: 6, base: 4175250000 }), sheet('ss1', { metal: 'silver', n: 25, base: 4180250000, backs: 24 })]) });
      await page.waitForTimeout(1100);
      const p = await probe(page); assert.equal(p.sets[0].boxes[0].grey, true, 'the next read turns it grey'); assert.equal(p.sets[0].boxes[0].why, 'SS Sheet 1 · back engravings 24 of 25');
      assert.deepEqual(errors, []); await context.close(); }

    // 5. a set of one, two and three sheets: one button each, none on a sheet
    { const mk = k => { const ids = ['a', 'b', 'c'].slice(0, k), sheets = ids.map((id, i) => sheet(id, { n: 4, base: 4170250000 + i * 5e6, index: i + 1, qr: false, metal: ['gold', 'silver', 'rose'][i] })); return { type: 'set', sheets, set: SET(ids) }; };
      for (const k of [1, 2, 3]) {
        const { page, context, errors } = await scene(browser, { cards: [mk(k)] }), p = await probe(page), set = p.sets[0];
        assert.equal(p.total, 1, `a set of ${k} sheet(s): one button in all`); assert.deepEqual(set.boxes.map(b => b.key), ['set:set1']); assert(set.lastIsBox); assert.deepEqual(set.sheets.map(s => s.boxes.length), Array(k).fill(0)); assert.deepEqual(set.sheets.map(s => s.rail), Array(k).fill(true));
        assert.equal(set.boxes[0].grey, false, `a set of ${k}: green (only the QR label is left)`);
        if (k === 1) { await shot(page.locator('.setCard'), 'set1-ready-1440'); await page.evaluate(() => { window.__approves.length = 0; }); await page.click('.setCard > .approveBox [data-approve-btn]'); assert.deepEqual(await page.evaluate(() => window.__approves), [{ kind: 'set', id: 'set1', by: 'Tester' }], 'a set of one sheet is pressed as a set'); }
        assert.deepEqual(errors, []); await context.close();
      }
      // a set of one sheet whose back engravings wait: the rail says it, the one button keeps a quiet reason for a screen reader
      const { page, context } = await scene(browser, { cards: [{ type: 'set', sheets: [sheet('a', { n: 4, backs: 1 })], set: SET(['a']) }] }), p = await probe(page);
      assert.deepEqual(p.sets[0].boxes.map(b => [b.key, b.grey, b.why]), [['set:set1', true, '']], 'a lone sheet\'s engravings are the rail\'s to say');
      assert.match(await page.evaluate(() => document.querySelector('.approveBox [data-approve-btn]').getAttribute('aria-label')), /^Approve for laser cutting: .*Waiting on 3 back engravings/); await context.close(); }

    // 6. a sheet that is part of no set keeps its own button, exactly as before: a sheet card, the sheets of a loose group, a 14K / 10K sheet left out of its set
    for (const width of [1440, 390]) {
      const loose = sheet('l1', { setId: null, n: 5, base: 4170250000, qr: false }), held = sheet('l2', { setId: null, n: 5, base: 4180250000, draft: true }), solid = sheet('k1', { metal: 'gold14k', n: 5, base: 4190250000, solidIncluded: false });
      const { page, context, errors } = await scene(browser, { width, height: width === 390 ? 844 : 900, cards: [{ type: 'group', group: { standalone: false, working: true, status: 'held for a later set' }, sheets: [loose, held] }, { type: 'group', group: { standalone: true, working: true, status: 'not included in a set' }, sheets: [solid] }, { type: 'flat', sheets: [sheet('f1', { setId: null, n: 4, base: 4195250000, qr: false })] }] });
      const p = await probe(page), by = Object.fromEntries([...p.sets.flatMap(s => s.sheets), ...p.flat].map(s => [s.id, s]));
      assert.equal(p.sets.every(s => !s.real && s.boxes.length === 0), true, `${width}px: a loose group is no set: it has no button of its own`);
      assert.deepEqual([by.l1.boxes.map(b => [b.key, b.grey]), by.l2.boxes.map(b => [b.key, b.grey, b.why]), by.k1.boxes.map(b => [b.key, b.grey, b.why]), by.f1.boxes.map(b => [b.key, b.grey])],
        [[['sheet:l1', false]], [['sheet:l2', true, 'Still a draft']], [['sheet:k1', true, 'Not included in a set yet']], [['sheet:f1', false]]], `${width}px: each sheet of no set has its own, green or grey with its reason, as before`);
      assert.deepEqual(await page.evaluate(() => [...document.querySelectorAll('.librarySheet')].map(a => [a.querySelector('.flowBox') && a.querySelector('.flowBox').previousElementSibling.className, a.querySelector('.approveBox').previousElementSibling.className])).then(x => x.map(y => y[1])), ['flowBox', 'flowBox', 'flowBox', 'flowBox'], 'each one directly under its sheet\'s rail');
      await shot(page.locator('#libBody'), `standalone-${width}`); if (width === 1440) await shot(page, `standalone-${width}-page`);
      // pressing a loose sheet's button approves that sheet, as before
      await page.click('[data-approve-for="sheet:l1"] [data-approve-btn]'); assert.deepEqual(await page.evaluate(() => window.__approves), [{ kind: 'sheet', id: 'l1', by: 'Tester' }]);
      assert.deepEqual(errors, [], `${width}px`); await context.close();
    }

    // 7. a sheet that joins a set loses its own button, and one that leaves a set gets it back, as soon as the cloud says so (the cards are drawn again a moment later)
    { const flat = sheet('s1', { setId: null, n: 4, qr: false }), p14 = { sheets: [sheet('gf', { n: 4, qr: false }), sheet('k14', { metal: 'gold14k', n: 4, base: 4180250000, index: 2 })], set: SET(['gf', 'k14']) };
      const { page, context, errors } = await scene(browser, { cards: [{ type: 'flat', sheets: [flat] }, { type: 'set', ...p14 }] });
      const send = (id, patch, at) => page.evaluate(({ id, patch, at }) => { window.__sheets = window.__sheets.map(s => s.id === id ? { ...s, ...patch, updatedAt: at } : s); window.LaserReview.nudge(); }, { id, patch, at });
      const own = id => page.evaluate(id => [...document.querySelectorAll(`.librarySheet:has([data-id="${id}"]) .approveBox`)].map(b => [b.dataset.approveFor, b.querySelector('[data-approve-btn]').getAttribute('aria-disabled') === 'true']), id);
      assert.deepEqual(await own('s1'), [['sheet:s1', false]], 'a sheet of no set has its own button');
      await send('s1', { setId: 'set9' }, 5); await page.waitForTimeout(1000); assert.deepEqual(await own('s1'), [], 'it joined a set: its own button is gone');
      await send('s1', { setId: null }, 6); await page.waitForTimeout(1000); assert.deepEqual(await own('s1'), [['sheet:s1', false]], 'it left the set: its own button is back');
      await send('s1', { setId: 'set9', draft: false, solidIncluded: true }, 7); await page.waitForTimeout(1000); assert.deepEqual(await own('s1'), []);
      // a 14K sheet of a set, left out by its own Include switch: it is part of no set now (its own button, with its reason); the set's one button waits for it; included again, it is gone
      assert.deepEqual(await own('k14'), [], 'a 14K sheet that is included is part of the set: none of its own');
      await send('k14', { solidIncluded: false }, 5); await page.waitForTimeout(1000);
      assert.deepEqual(await own('k14'), [['sheet:k14', true]], 'left out of its set: its own button (grey: not included in a set yet)');
      let p = await probe(page); assert.equal(p.sets[0].boxes.length, 1); assert.equal(p.sets[0].boxes[0].why, '14K Sheet 2 · not included in a set yet', 'the set\'s one button waits for it');
      await send('k14', { solidIncluded: true }, 6); await page.waitForTimeout(1000);
      assert.deepEqual(await own('k14'), [], 'included again: none of its own'); p = await probe(page); assert.equal(p.sets[0].boxes.length, 1); assert.equal(p.sets[0].boxes[0].grey, false, 'and the set\'s button is green again');
      assert.deepEqual(errors, []); await context.close(); }

    // 8. held, committed, cut and completed sets; Rose Gold; a set whose sheet is not nested yet
    { const held = { sheets: [sheet('h1', { n: 4, qr: false, laserHold: { at: 3, by: 'Paul' } }), sheet('h2', { metal: 'silver', n: 4, base: 4180250000, index: 2, qr: false })], set: SET(['h1', 'h2'], { setId: 'sH', seq: 1 }) };
      held.sheets.forEach(s => { s.setId = 'sH'; });
      const comm = { sheets: [sheet('c1', { n: 4, setId: 'sC', base: 4185250000 }), sheet('c2', { metal: 'silver', n: 4, setId: 'sC', base: 4186250000, index: 2, backs: 1 })], set: SET(['c1', 'c2'], { setId: 'sC', seq: 2, committedAt: 5, status: 'complete-with-holds' }) };
      const nest = { sheets: [sheet('n1', { n: 4, setId: 'sN', base: 4187250000, qr: false }), sheet('n2', { metal: 'silver', n: 4, setId: 'sN', base: 4188250000, index: 2, status: 'nesting', dirty: true })], set: SET(['n1', 'n2'], { setId: 'sN', seq: 3 }) };
      const rose = { sheets: [sheet('r1', { n: 4, setId: 'sR', base: 4189250000, qr: false }), sheet('r2', { metal: 'rose', n: 4, setId: 'sR', base: 4191250000, index: 2, roseStockId: 'stock-1', qr: false })], set: SET(['r1', 'r2'], { setId: 'sR', seq: 4 }) };
      const cut = { sheets: [sheet('x1', { n: 4, setId: 'sX', base: 4192250000, laserDoneAt: 5 }), sheet('x2', { metal: 'silver', n: 4, setId: 'sX', base: 4193250000, index: 2, laserDoneAt: 5 })], set: SET(['x1', 'x2'], { setId: 'sX', seq: 5, laserDoneAt: 5 }) };
      const { page, context, errors } = await scene(browser, { cards: [held, comm, nest, rose, cut].map(c => ({ type: 'set', ...c })) });
      const p = await probe(page), by = Object.fromEntries(p.sets.map(s => [s.id, s]));
      assert.deepEqual([by.sH.boxes.map(b => [b.grey, b.why]), by.sH.sheets.map(s => s.boxes.length)], [[[false, '']], [0, 0]], 'a held sheet: the press lifts the hold, so the one button is green, none on the sheets');
      assert.deepEqual([by.sC.boxes.map(b => [b.grey, b.why]), by.sC.sheets.map(s => s.boxes.length)], [[[true, 'SS Sheet 2 · back engravings 1 of 4']], [0, 0]], 'a committed set that is not finished: one grey button, none on the sheets');
      assert.deepEqual([by.sN.boxes.map(b => [b.grey, b.why]), by.sN.sheets.map(s => s.boxes.length)], [[[true, 'SS Sheet 2 · still being laid out']], [0, 0]], 'a sheet not nested yet holds the set: one grey button');
      assert.deepEqual([by.sR.boxes.map(b => [b.grey, b.why]), by.sR.sheets.map(s => s.boxes.length)], [[[false, '']], [0, 0]], 'Rose Gold without its green line: its own yes, never a grey button; none on the sheets');
      assert.equal(by.sX.area, 'ready', 'a cut set is in Laser cutting'); assert.deepEqual([by.sX.boxes.length, by.sX.sheets.map(s => s.boxes.length)], [0, [0, 0]], 'no button on a cut set or its sheets');
      // the Completed list's set cards are not in In progress at all: no button there either
      await page.evaluate(() => { const done = document.createElement('div'); done.id = 'libDone'; document.body.appendChild(done); const c = [...document.querySelectorAll('.setCard')].find(x => x._laserSet.setId === 'sH'); done.appendChild(c); window.LaserReview.changed(); }); await page.waitForTimeout(500);
      assert.equal(await page.evaluate(() => document.querySelectorAll('#libDone .approveBox').length), 0, 'a set card shown under Completed has no Approve button');
      assert.deepEqual(errors, []); await context.close(); }

    // 9. the '!' panel opened near the button never leaves a strip of it (covered whole or left whole), at 1440 and 390 px, from every sheet of the set
    for (const width of [1440, 390]) for (const id of ['gf1', 'ss1']) {
      const tag = `${width}px, '!' of ${id}`, p0 = paul(false); if (id === 'gf1') p0.sheets[0] = sheet('gf1', { n: 20, backs: 9 });   // (GF Sheet 1 gets a real '!' of its own: 9 of 20 back engravings)
      const { page, context, errors } = await scene(browser, { width, height: width === 390 ? 844 : 900, cards: [{ type: 'set', ...p0 }] });
      const b = await page.$(`[data-issues-open][data-issues-id="${id}"]`); await b.scrollIntoViewIfNeeded(); await b.click(); await page.waitForTimeout(600);
      const m = await page.evaluate(() => { const pn = document.querySelector('.lisPanel'), bt = document.querySelector('.setCard > .approveBox [data-approve-btn]'); if (!pn || !bt) return null; const a = pn.getBoundingClientRect(), r = bt.getBoundingClientRect(), ix = Math.min(a.right, r.right) - Math.max(a.left, r.left), iy = Math.min(a.bottom, r.bottom) - Math.max(a.top, r.top); return { none: ix <= 0.5 || iy <= 0.5, whole: a.left <= r.left + 1 && a.right >= r.right - 1 && a.top <= r.top + 1 && a.bottom >= r.bottom - 1, panel: [a.left, a.top, a.right, a.bottom], btn: [r.left, r.top, r.right, r.bottom] }; });
      assert(m, `${tag}: the panel is open`); assert(m.none || m.whole, `${tag}: the set's button is covered whole or left whole, never a strip of it: ${JSON.stringify(m)}`);
      await page.keyboard.press('Escape'); await page.waitForTimeout(300); assert.deepEqual(errors, [], tag); await context.close();
    }

    // 3b. the checks above, once more over the shipped code, are the ones the mutants below must fail
    const real = await (async () => { const { page, context, errors } = await scene(browser, { cards: basicCards() }); const bad = basics(await probe(page)); await context.close(); assert.deepEqual(errors, []); return bad; })();
    assert.deepEqual(real, [], 'the shipped code passes the basic checks');

    // Part 3: mutants that bring the old behaviour back are caught by the same checks
    const mutate = (from, to) => { assert.equal(BRIDGE.split(from).length, 2, 'the mutant finds its place: ' + from.slice(0, 60)); return BRIDGE.replace(from, to); };
    const mutants = {
      'a button on every sheet of a set again': mutate('on && !part(projected(rec))?approveCase([rec]):null', 'on?approveCase([rec]):null'),
      'a sheet of no set loses its own button': mutate('const part=p=>real?!(p.draft || p.solidIncluded===false):inSetNow(p);', 'const part=p=>true;'),
      'the set has no button of its own': mutate("if(real)syncBox(card,'set',st.setId,on?setCase(card):null,setTitle(st));", "if(real)syncBox(card,'set',st.setId,null,setTitle(st));"),
      'the set button is drawn above its sheets': mutate("host.appendChild(box);   // (last in its host", "if(kind==='set')host.insertBefore(box,host.querySelector('.sheetsRow'));else host.appendChild(box);   // (last in its host"),
      'a set-level progress bar comes back': mutate("for(const slot of slots){const e=explainOf(slot,card,lookup);", "if(card.dataset.laserCard==='set'&&card._laserSet?.setId&&!card.querySelector(':scope > .flowBox')){const bx=document.createElement('div');bx.className='flowBox';card.insertBefore(bx,card.querySelector('.sheetsRow'));}for(const slot of slots){const e=explainOf(slot,card,lookup);")
    };
    for (const [name, src] of Object.entries(mutants)) {
      const { page, context } = await scene(browser, { cards: basicCards(), bridge: src }), bad = basics(await probe(page)); await context.close();
      assert(bad.length > 0, `the checks catch the mutant "${name}"`);
      if (process.env.VERBOSE) console.log('  mutant caught:', name, '->', bad[0]);
    }
  } finally { await browser.close(); }

  // ── Part 2: the real LibraryFlow over the in-memory shop (the real charmNestLibrary handler) ──
  const { start } = require('./bridge-server.cjs'), LF = require('../../charm-nest-flow.js');
  const S = 'Charm_Nest_Sheets', SETC = 'Charm_Nest_Sets', RUN = 'Charm_Nest_Runs';
  const srv = await start({ receipts: [] }), { st } = srv, now = Date.now(), ts = { toMillis: () => now }, day = '2026-10-03';
  const post = async body => { const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }); return { status: r.status, ...await r.json() }; };
  const image = 'data:image/svg+xml,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="30" height="20"><rect width="30" height="20"/></svg>');
  const lines = {}; let n = 0;
  const mk = (id, o = {}) => {
    const order = String(3900000000 + ++n), pool = order + '_1_1', engrave = o.engrave || 'plain';
    lines[order + '_1'] = { orderId: order, state: 'written', quantity: 1, poolIds: [pool], ...(engrave === 'plain' ? { engraveCandidate: false } : engrave === 'blocked' ? { engrave: { needed: true, state: 'review', approved: false } } : { engrave: { needed: true, state: 'written', approved: true } }) };
    const { engrave: _e, noLabel, metal, ...rest } = o;
    st.put(S, id, { id, runId: 'run-s', metal: metal || 'gold', day, status: 'complete', placedCount: 1, charmCount: 1, density: .7, stock: { wIn: 6, hIn: 4.5 }, poolIds: [pool], orders: [order], verification: { ok: true },
      outputs: { ai: { path: id + '.ai', url: srv.sorterOrigin + '/' + id + '.ai' }, preview: { path: id + '.png', url: image } },
      ...(noLabel ? {} : { label: { files: [{ path: id + '-qr.png', url: image, payload: order, orders: [order] }], orders: [order] } }), sheetIndex: 1, updatedAt: ts, createdAt: ts, ...rest });
    return { order, pool };
  };
  const mkSet = (setId, sheetIds, extra = {}) => st.put(SETC, setId, { setId, seq: +setId.replace(/\D/g, '') || 1, day, runId: 'run-s', sheetIds, materials: ['gold'], orders: {}, status: 'labelled', updatedAt: ts, createdAt: ts, ...extra });
  const held = id => !!(st.doc(S, id).laserHold && st.doc(S, id).laserHold.at), seals = id => (st.doc(S, id).processSeals || []).map(x => x.how + ':' + x.by).sort(), setSeals = id => (st.doc(SETC, id).processSeals || []).map(x => x.how + ':' + x.by).sort();
  const labels = [], roseCalls = [];
  const snap = ids => JSON.stringify(ids.map(i => [st.doc(S, i), st.doc(SETC, i)]));
  try {
    // set-1: three sheets, every one ready to be approved: GF held and without its QR label, RG cut and lined, SS without its QR label
    mkSet('set-1', ['gf-1', 'rg-1', 'ss-1']); mk('gf-1', { setId: 'set-1', setSeq: 1, noLabel: true, laserHold: { at: now - 500, by: 'Paul' } }); mk('rg-1', { setId: 'set-1', setSeq: 1, metal: 'rose', roseStockId: 'stock-1', rosePlanHash: 'h', roseCutAt: now - 9000 }); mk('ss-1', { setId: 'set-1', setSeq: 1, metal: 'silver', noLabel: true });
    // set-2: the same shape, to see that "Move set to Laser cutting" and the button are the same plan
    mkSet('set-2', ['gf-2', 'ss-2']); mk('gf-2', { setId: 'set-2', setSeq: 2, noLabel: true }); mk('ss-2', { setId: 'set-2', setSeq: 2, metal: 'silver', noLabel: true });
    // set-3 and set-4: a set of ONE sheet, pressed as a sheet (the old way) and as a set (now)
    mkSet('set-3', ['one-3']); mk('one-3', { setId: 'set-3', setSeq: 3, noLabel: true, laserHold: { at: now - 500, by: 'Paul' } });
    mkSet('set-4', ['one-4']); mk('one-4', { setId: 'set-4', setSeq: 4, noLabel: true, laserHold: { at: now - 500, by: 'Paul' } });
    // set-5: Rose Gold without its green line; set-6: a half set (SS has a back engraving to approve)
    mkSet('set-5', ['c-gf', 'c-rg']); mk('c-gf', { setId: 'set-5', setSeq: 5 }); mk('c-rg', { setId: 'set-5', setSeq: 5, metal: 'rose', roseStockId: 'stock-1' });
    mkSet('set-6', ['g-gf', 'g-ss']); mk('g-gf', { setId: 'set-6', setSeq: 6 }); mk('g-ss', { setId: 'set-6', setSeq: 6, metal: 'silver', engrave: 'blocked', noLabel: true });
    st.put(RUN, 'run-s', { runId: 'run-s', status: 'complete', lines });
    LF.configure({ api: post, employee: () => 'Paul', rows: () => [], remakeLabel: async id => { labels.push(id); const rec = st.doc(S, id); st.put(S, id, { label: { files: [{ path: id + '-qr.png', url: image, payload: rec.orders[0], orders: rec.orders }], orders: rec.orders } }); },
      rose: () => ({ check: async () => ({ needsLine: true, sheets: [{ sheetId: 'c-rg', label: 'RG Sheet 1', needsLine: true, source: 'live', blocked: '' }], confirm: { key: 'roseLine', label: 'Add the green dash line to RG Sheet 1?', detail: 'This calculates the cut contour for these charms' } }), calculate: async item => { roseCalls.push('calculate:' + item.id); return { ok: true, lines: 1, sheets: [] }; } }) });

    // a. the page's ONE press: approve kind 'set' approves the whole set, each sheet with its own seal, the set with its own; one plan back (one Moving bar)
    const typesOf = p => p.steps.map(x => `${x.type}:${x.sheetId || x.id || (x.sheetIds || []).join('+')}`).sort();
    const asSheet = await LF.plan({ kind: 'sheet', id: 'gf-2', to: { area: 'laser' } }), asSet = await LF.plan({ kind: 'set', id: 'set-2', to: { area: 'laser' } });
    assert.equal(asSet.ok, true, JSON.stringify(asSet.needs)); assert.deepEqual(typesOf(asSet), typesOf(asSheet), '"Move set to Laser cutting" and the move of a sheet of it are the same plan: every sheet of the set, one seal');
    assert.deepEqual(asSet.needs.map(x => x.key), asSheet.needs.map(x => x.key)); assert.deepEqual(asSet.steps.filter(x => x.type === 'qrLabel').map(x => x.sheetId).sort(), ['gf-2', 'ss-2']); assert.equal(asSet.steps.filter(x => x.type === 'seal').length, 1, 'one seal for the set');
    const a = await LF.approve({ kind: 'set', id: 'set-1', by: 'Paul' });
    assert.equal(a.approved, true, JSON.stringify(a.needs) + a.error); assert.deepEqual(labels.sort(), ['gf-1', 'ss-1'], 'the sheets that lacked their QR label got it'); assert(!held('gf-1'), 'the hold is lifted');
    for (const id of ['gf-1', 'rg-1', 'ss-1']) assert(seals(id).includes('laserReady:Paul'), `${id} has its own ready seal: ${seals(id)}`);
    assert(setSeals('set-1').includes('laserReady:Paul'), 'and the set has its own');
    const again = await LF.approve({ kind: 'set', id: 'set-1', by: 'Paul' }); assert.equal(again.approved, true); assert.deepEqual(again.applied, [], 'a second press finds everything done'); assert.deepEqual(labels.sort(), ['gf-1', 'ss-1']);
    assert.equal((await LF.plan({ kind: 'set', id: 'set-1', to: { area: 'nowhere' } })).from.area, 'laser', 'the whole set reached Laser cutting together');

    // b. a set of one sheet: pressed as a set it gets exactly what pressing its sheet always gave it
    const before3 = labels.length, one3 = await LF.approve({ kind: 'sheet', id: 'one-3', by: 'Paul' }), one4 = await LF.approve({ kind: 'set', id: 'set-4', by: 'Paul' });
    assert.equal(one3.approved, true, JSON.stringify(one3.needs)); assert.equal(one4.approved, true, JSON.stringify(one4.needs) + one4.error);
    assert.deepEqual([held('one-4'), seals('one-4'), !!st.doc(SETC, 'set-4').processReady, (st.doc(S, 'one-4').label.files || []).length], [held('one-3'), seals('one-3').map(x => x), !!st.doc(SETC, 'set-3').processReady, (st.doc(S, 'one-3').label.files || []).length], 'the same hold lifted, QR label, seals and set seal');
    assert.deepEqual(setSeals('set-4'), setSeals('set-3')); assert.equal(labels.length, before3 + 2, 'one QR label each');

    // c. Rose Gold: the green line is its own yes, never taken by the one press
    const ap = await LF.approve({ kind: 'set', id: 'set-5', by: 'Paul' });
    assert.equal(ap.approved, false); assert.deepEqual(ap.confirm.map(c => c.key), ['roseLine'], 'the green line stays a yes the person gives'); assert(!roseCalls.length, 'nothing was calculated'); assert.equal(st.doc(S, 'c-rg').rosePlanHash, undefined); assert(!st.doc(S, 'c-rg').roseCutAt, 'no cut recorded by the press');

    // d. a half set: refused on the page and on the server, and nothing is written (the stale page of a person who pressed an old green button)
    const ids6 = ['g-gf', 'g-ss', 'set-6'], was = snap(ids6), calls0 = st.calls.length;
    const half = await LF.approve({ kind: 'set', id: 'set-6', by: 'Paul' });
    assert.equal(half.approved, false); assert.equal(half.needs[0].key, 'setGate'); assert.match(half.needs[0].label, /SS Sheet 1 · back engravings 0 of 1/); assert.deepEqual(half.applied, []); assert.equal(snap(ids6), was, 'nothing was written');
    assert(!st.calls.slice(calls0).some(c => c.op === 'flowApply' || c.op === 'laserDone'), 'the page did not even try');
    for (const steps of [[{ type: 'seal', kind: 'set', id: 'set-6' }], [{ type: 'release', sheetIds: ['g-gf'] }, { type: 'seal', kind: 'set', id: 'set-6' }], [{ type: 'seal', kind: 'sheet', id: 'g-gf' }]]) {
      const r = await post({ op: 'flowApply', by: 'Paul', steps }); assert.equal(r.status, 409, JSON.stringify(steps)); assert(/approved together/.test(r.error) && /SS Sheet 1 · back engravings 0 of 1/.test(r.error), r.error);
    }
    assert.equal(snap(ids6), was, 'the server refused every one of them and wrote nothing');
  } finally { srv.close(); LF.configure({ api: null, remakeLabel: null, rose: () => null }); }
  console.log('One approve per set OK: a 3-sheet set shows exactly one button (last in its card, left aligned, none on its sheets, rails kept, no set progress bar), grey with the plain reason and its link or green, one press approves the set once with one Moving bar; live flips keep the button, its place and the columns; a stale 409 is calm; sets of 1, 2 and 3 sheets; sheets of no set keep their own; join and leave re-evaluate at once; 14K Include, held, committed, cut and Rose Gold sets; 1440 and 390 px; the \'!\' panel leaves no strip; the real flow approves the whole set, the same as Move set to Laser cutting; the mutants are caught');
})().catch(e => { console.error(e); process.exitCode = 1; });
