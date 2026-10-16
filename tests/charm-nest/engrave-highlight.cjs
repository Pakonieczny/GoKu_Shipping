// ENGHILITE (engrave-ui-1012 round 2, Paul's point 7, second half): "Highlight the AI chosen engraving options, so they're clearly visible to the user for a particular order/listing, unless the user decides to override
// those selections." The engraving panel of the Engrave tab (charm-nest-engrave-panel.js) marks what the SYSTEM chose and keeps it marked when a person picks something else. REAL page, headless Chromium, offline fake site
// (no Etsy, no paid reading, no live write). The real preview and position components are on the page (nothing is stubbed but CharmNestEngraveSetup.autoChoice in step 6, to prove it is the source of truth).
//   1  untouched: the mark (a gold sparkle, title "Chosen by the system") is on the system's mode segment, on the system's font (its entry in the list and the select's tint) and on the shown face's words box (tinted)
//   2  the person picks another mode: the selection (ring) moves, the mark does not (still one mark, on the system's segment)
//   3  the person types other words / picks another font: the tint goes, the mark stays (ghost beside the closed font drop-down, on the words box), its title now says what the system chose; typing the system's words back brings the tint back
//   4  the other face (the Front thumbnail) carries its own system words; Cancel brings every tint back; nothing disappears anywhere
//   5  a saved override with a recorded reason (engravingOverride.reason): the selection is the person's, the marks are on the system's choice and their titles show the reason, read-only
//   6  the source of truth is CharmNestEngraveSetup.autoChoice when the page has it (a stub here); a face the system does not engrave has no words / font mark
//   7  no new words: the marks hold no text, the panel's text is what it was, option texts are unchanged; the mode control is still ONE row at 232px and the marks take no room (widths equal with and without them)
//   8  an approved face (open for a change since NODECIDED) keeps its marks; an earring draws no panel at all; no paid call, no page error
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/engrave-highlight.cjs      (SHOTS=<dir> also writes screenshots)
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const SHIP = Math.floor(Date.UTC(2026, 9, 14, 17) / 1000), RID = '4195966039', TX = '5009123456789', KEY = `${RID}_${TX}`;
const ORDER = { receiptId: RID, orderNumber: RID, createTs: SHIP - 5 * 86400, updateTs: SHIP - 5 * 86400 + 60, shipBy: SHIP, buyer: { name: 'Name One' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: TX, listingId: '4458930445', sku: 'MINI_0445', title: 'Mini Tag Initial Charm Add On Charm Personalized Letter Charm', quantity: 1, expectedShipDate: SHIP, metalKey: 'gold', metalLabel: 'GF 14/20',
    variations: [{ name: 'Charm Type', value: 'Tag1 (front+back)' }, { name: 'Font choice', value: 'Ace' }, { name: 'Metal Choice', value: 'Gold Filled' }], personalization: ['front - W\nback - 1. 27. 24'] }] };
const TITLE = 'Chosen by the system', REASON = 'Changed: Customer asked in a message \u00b7 Name One';   // (ENGWHY\'s reader, when the page has it, adds the time after it: the checks below take the line as a prefix)
const startsWith = (t, head) => typeof t === 'string' && (t === head || t.startsWith(head + ' \u00b7 '));

/** what the marks look like on the panel at `sel` (runs in the page) */
const MARKS_FN = sel => {
      const p = document.querySelector(sel + ' .engp'); if (!p) return null;
      const bg = e => e ? getComputedStyle(e).backgroundColor : '', ta = p.querySelector('.engpWords'), box = p.querySelector('.engpBox'), s = p.querySelector('.engpFont'), fb = s && s.parentElement;
      const mk = h => { const k = h && h.querySelector(':scope > .engpSys'); return k ? { st: k.dataset.st, title: k.title, text: k.textContent } : null; };
      return {
        face: p.dataset.face, mode: p.dataset.mode, text: p.textContent.replace(/\s+/g, ' ').trim(),
        seg: [...p.querySelectorAll('.engpMode:not(.engpFaceSw) .engpSeg')].map(b => ({ m: b.dataset.mode, on: b.getAttribute('aria-checked') === 'true', sys: b.dataset.sys || '', title: b.title, mark: mk(b), bg: bg(b) })),
        words: { sys: box ? box.dataset.sys || '' : null, mark: mk(box), bg: bg(ta), value: ta && ta.value },
        font: { sys: fb ? fb.dataset.sys || '' : null, ghost: mk(fb), selTitle: s ? s.title : '', value: s && s.value, bg: bg(s), opts: s ? [...s.options].map(o => ({ v: o.value, text: o.text, label: o.label, star: !!o.dataset.sys })) : [] }
      };
};
const onlySeg = (m, mode) => m.seg.filter(s => s.mark).map(s => s.m).join() === mode && m.seg.filter(s => s.sys).map(s => s.m).join() === mode;
const CARD_BG = 'rgb(255, 254, 251)';

/** 9 · the REAL-DRAWING world (ENGLIVE: real charm drawings, real intake / pool / classifier / fit, real fonts, offline): on each real piece the marks are exactly the system's own choice (autoChoice), untouched; an override moves the selection, not the marks */
async function realWorld(check, shots) {
  let R; try { R = require('./_engraveRealWorld.cjs'); } catch (_) { console.log('  (no real-drawing world in this build: the real-drawing checks were skipped)'); return; }
  const w = await R.world({ viewport: { width: 1440, height: 900 }, shots: shots || path.join(require('os').tmpdir(), 'hilite-real-shots'), scale: shots ? 2 : 1 });
  if (!w) { console.log('  (no playwright-core: the real-drawing checks were skipped)'); return; }
  const PV = '.pvSetup', CLIP = { x: 350, y: 92, width: 600, height: 560 };
  try {
    const made = await R.build(w, ['back', 'front', 'both', 'discs']);
    const truthOf = (name, face, slot) => w.page.evaluate(({ rid, face, slot }) => {
      const row = Orders.rows().find(r => r.order.receiptId === rid), a = CharmNestEngraveSetup.autoChoice(row, slot || null), P = CharmNestEngravePreview, p = document.querySelector('.pvSetup .engp'), star = p && [...p.querySelectorAll('.engpFont option')].find(o => o.dataset.sys);
      return { mode: a.mode, text: a[face] ? a[face].text : null, fontId: a[face] ? a[face].fontId : null, starId: star ? P.fontIdOf(row, face, star.value) : null, shown: p ? p.dataset.face : null };
    }, { rid: made[name].rid, face, slot });
    const cases = [['back', 'back', null], ['front', 'front', null], ['both', 'front', null], ['both', 'back', null], ['discs', 'back', 'D2']];
    for (const [name, face, slot] of cases) {
      const jk = await R.openEngraveDetail(w, name, { face, slot });
      const tag = `${name}${slot ? ' ' + slot : ''} ${face}`;
      if (!jk) { check(false, `9 ${tag}: the order has no engraving job to open`); continue; }
      await w.page.waitForSelector(`${PV} .engp .engpWords`, { timeout: 20000 });
      const m = await w.page.evaluate(MARKS_FN, PV), t = await truthOf(name, face, slot);
      check(m.face === face && t.shown === face, `9 ${tag}: the panel is on the ${face}`);
      check(onlySeg(m, t.mode) && m.seg.find(s => s.m === t.mode).on && m.seg.every(s => !s.mark || s.mark.title === TITLE), `9 ${tag}: the mark and the tint are on the system's mode (${t.mode}), which is the selected one`);
      check(t.text && m.words.sys === 'same' && m.words.mark && m.words.mark.title === TITLE && m.words.bg !== CARD_BG, `9 ${tag}: the words ("${(m.words.value || '').replace(/\n/g, ' / ')}" for the system's "${t.text}") carry the mark and the tint, untouched: ` + JSON.stringify({ sys: m.words.sys }));
      check(t.fontId && t.starId === t.fontId && m.font.opts.filter(o => o.star).length === 1 && m.font.sys === 'same' && !m.font.ghost, `9 ${tag}: the system's font (${t.fontId}) is the starred entry of the list and is selected: ` + JSON.stringify({ star: m.font.opts.filter(o => o.star).map(o => o.label), starId: t.starId }));
      if (name === 'discs') check(m.words.value === 'B', `9 ${tag}: disc 2's own words ("${m.words.value}") are marked, not the line's`);
      if (shots && (name === 'back' || name === 'both')) await R.shot(w, `HILITE-real-${name}-${face}-untouched`, { clip: CLIP });
    }
    // an override in a real order: Back instead of Front + back, other words, another font: the ring and the words move, every mark stays
    await R.openEngraveDetail(w, 'both', { face: 'back' });
    await w.page.waitForSelector(`${PV} .engp .engpWords`, { timeout: 20000 });
    await w.page.click(`${PV} [data-engp=mode][data-mode=back]`); await w.page.waitForSelector(`${PV} .engp[data-mode=back]`);
    await w.page.fill(`${PV} .engpWords`, 'Name Two'); await w.page.selectOption(`${PV} .engpFont`, 'Pristina');
    const o = await w.page.evaluate(MARKS_FN, PV);
    check(o.seg.find(s => s.m === 'back').on && onlySeg(o, 'both') && o.words.sys === 'changed' && o.words.mark && /^Chosen by the system: NAME \/ ONE|^Chosen by the system: Name One/.test(o.words.mark.title) && o.font.sys === 'changed' && o.font.ghost && o.font.opts.filter(x => x.star).map(x => x.v).join() === '',
      '9 both, overridden (Back, other words, Pristina): the ring moved, every mark stayed on the system\'s choice, with its titles: ' + JSON.stringify({ seg: o.seg.filter(s => s.mark).map(s => s.m), words: o.words.mark && o.words.mark.title, ghost: o.font.ghost && o.font.ghost.title }));
    if (shots) await R.shot(w, 'HILITE-real-both-back-overridden', { clip: CLIP });
    await w.page.click(`${PV} [data-engp=cancel]`);
    await w.page.waitForFunction(() => { const c = document.querySelector('.pvSetup [data-engp=cancel]'); return c && c.disabled; });
    const c = await w.page.evaluate(MARKS_FN, PV);
    check(c.words.sys === 'same' && c.font.sys === 'same' && c.seg.find(s => s.m === 'both').on, '9 after Cancel the real order is the system\'s choice again, every tint back');
    check(w.errors.length === 0, '9 no page errors in the real world: ' + w.errors.join(' | ').slice(0, 300));
  } finally { await w.close(); }
}

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { try { ({ chromium } = require('/opt/node22/lib/node_modules/playwright/node_modules/playwright-core')); } catch (__) { console.log('engrave-highlight: playwright-core not found, skipped'); process.exit(0); } }
  const shots = process.env.SHOTS || ''; if (shots) fs.mkdirSync(shots, { recursive: true });
  const srv = await start({ receipts: [] });
  const POOLID = KEY + '_1';
  srv.st.put('Charm_Nest_Sheets', 'gold-su-1', { id: 'gold-su-1', metal: 'gold', charms: [{ id: 'g0', poolId: POOLID, order: RID }], placements: [{ id: 'g0', cxPt: 40, cyPt: 40, angle: 0 }], poolIds: [POOLID], orders: [RID], sheetIndex: 1, fileBase: 'gold-su-1', runId: 'run-test-1' });
  srv.st.put('Charm_Pool', POOLID, { poolId: POOLID, orderId: RID, sheetId: 'gold-su-1', state: 'placed', material: 'gold' });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium', args: ['--no-sandbox'] });
  const fails = [];
  const check = (ok, msg) => { if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
  try {
    const context = await browser.newContext({ viewport: { width: 1280, height: 720 }, deviceScaleFactor: shots ? 2 : 1 });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {} });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Engrave && window.CharmNestEngraveSetup && window.CharmNestEngravePanel && CN.S.cloud.ok === true, null, { timeout: 60000 });
    const hasHi = await page.evaluate(() => !!(window.CharmNestEngravePanel && CharmNestEngravePanel.hilite));
    check(hasHi, 'the panel carries the highlight (CharmNestEngravePanel.hilite)');
    if (!hasHi) throw new Error('no highlight in this build');

    // ── the order (read by the real intake: Front + back, "front - W / back - 1. 27. 24", font Ace) and its back job ──
    await page.evaluate(async ({ order, KEY }) => {
      const line = order.lines[0];
      const row = { key: KEY, order, line, arrivedAt: Date.now() - 3600e3, spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: [KEY + '_1'], engrave: null, material: null };
      B.orders.rows.push(row); B.orders.byKey.set(KEY, row);
      Orders.interpretAll(); await Orders.loadMaps(true); Orders.interpretAll();
      row.spec.engraveCandidate = true;
      const j = Engrave.ensureJob(row); Object.assign(j, { state: 'review', text: '1. 27. 24', lines: ['1. 27. 24'], fit: { size: 10, capMm: 2, centre: [0, 0], angle: 0, weight: 'Regular' }, view: {}, verify: { geometry: { ok: true } }, source: 'personalization', confidence: 0.9 });
      row.engrave = { needed: true, state: 'review', approved: false, text: j.text };
      window.__calls = []; const call = DesignLink.call; DesignLink.call = function (name) { __calls.push(name); return call.apply(this, arguments); };
    }, { order: ORDER, KEY });

    // what the marks look like on the panel at `sel`
    const marks = sel => page.evaluate(MARKS_FN, sel);

    // ── 1-4 · the Engrave tab's detail: card + panel, at 1280x720 ──
    await page.evaluate(() => { CharmNestEngravePanel.DRAFTS.clear(); setMode('engrave'); Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: null }); Engrave.render(); });
    await page.waitForSelector('.pvSetup .engp .engpWords', { timeout: 20000 });
    const PV = '.pvSetup';
    let m = await marks(PV);
    check(m.mode === 'both' && onlySeg(m, 'both') && m.seg.every(s => !s.mark || (s.mark.title === TITLE && s.title === TITLE)) && m.seg.find(s => s.m === 'both').on, `1 untouched: the mark and the tint are on the system's mode (Front + back), selected, and nowhere else; title "${TITLE}"`);
    check(m.words.sys === 'same' && m.words.mark && m.words.mark.st === 'same' && m.words.mark.title === TITLE && m.words.bg !== CARD_BG, `1 untouched: the words box (the Back: "${m.words.value}") carries the mark and a quiet tint (${m.words.bg})`);
    const starred = m.font.opts.filter(o => o.star);
    check(starred.length === 1 && starred[0].v === '' && /^✦ /.test(starred[0].label) && starred[0].label.endsWith(starred[0].text) && m.font.sys === 'same' && m.font.selTitle === TITLE && m.font.bg !== CARD_BG && !m.font.ghost,
      `1 untouched: the font drop-down: the system's font (the line's own, "${starred[0] && starred[0].text}") is starred in the list, selected, tinted, with no ghost: ` + JSON.stringify({ star: starred, sys: m.font.sys }));
    check(m.seg.filter(s => !s.sys).every(s => s.bg !== m.seg.find(x => x.sys).bg), '1 the tint is only on the system\'s segment');
    const textBefore = m.text, optsBefore = m.font.opts.map(o => o.text).join('|');
    check(!/✦/.test(m.text) && m.seg.every(s => !s.mark || s.mark.text === '') && !m.words.mark.text, `7 the marks hold no text of their own and the panel says only what it said: "${m.text.slice(0, 100)}"`);
    if (shots) await page.screenshot({ path: path.join(shots, 'HILITE-1-untouched.png'), clip: { x: 360, y: 120, width: 285, height: 440 } });

    // 2 · another mode
    await page.click(`${PV} [data-engp=mode][data-mode=back]`);
    await page.waitForSelector(`${PV} .engp[data-mode=back]`);
    m = await marks(PV);
    check(m.seg.find(s => s.m === 'back').on && !m.seg.find(s => s.m === 'both').on && onlySeg(m, 'both') && m.seg.find(s => s.m === 'both').mark.title === TITLE,
      '2 Back selected: the ring moved to Back, the mark and the tint stayed on Front + back (one mark, same title, no reason: none recorded)');
    check(m.seg.find(s => s.m === 'back').bg !== m.seg.find(s => s.m === 'both').bg, '2 the selected change is not tinted (it is the person\'s), the system\'s is');
    check(m.seg.length === 4 && m.seg.map(s => s.m).join() === 'none,front,back,both', '2 nothing disappeared: still the four choices');
    await page.click(`${PV} [data-engp=mode][data-mode=both]`);
    await page.waitForSelector(`${PV} .engp[data-mode=both]`);

    // 3 · other words, another font
    await page.fill(`${PV} .engpWords`, 'Name Two');
    m = await marks(PV);
    check(m.words.value === 'Name Two' && m.words.sys === 'changed' && m.words.mark && m.words.mark.st === 'changed' && m.words.bg === CARD_BG && m.words.mark.title === `${TITLE}: 1. 27. 24`,
      '3 other words typed: the tint went, the mark stayed (ghost) and its title says what the system chose: ' + JSON.stringify({ sys: m.words.sys, bg: m.words.bg, mark: m.words.mark }));
    await page.fill(`${PV} .engpWords`, '1. 27. 24');
    m = await marks(PV);
    check(m.words.sys === 'same' && m.words.mark.st === 'same' && m.words.bg !== CARD_BG && m.words.mark.title === TITLE, '3 typing the system\'s own words back brings the tint back');
    await page.selectOption(`${PV} .engpFont`, { label: 'Pristina' });
    m = await marks(PV);
    const lineFontText = starred[0].text;
    check(m.font.value === 'Pristina' && m.font.sys === 'changed' && m.font.bg === CARD_BG && m.font.ghost && m.font.ghost.st === 'changed' && m.font.ghost.title === `${TITLE}: ${lineFontText}` && m.font.opts.filter(o => o.star).map(o => o.v).join() === '',
      '3 another font picked: the tint went, a ghost mark sits beside the closed drop-down (title names the system\'s font), the system\'s entry is still starred in the list: ' + JSON.stringify({ sys: m.font.sys, ghost: m.font.ghost, star: m.font.opts.filter(o => o.star).map(o => o.label) }));
    check(m.font.opts.map(o => o.text).join('|') === optsBefore, '7 the options\' own texts are unchanged (the star is only in their display label)');
    check(onlySeg(m, 'both') && m.seg.find(s => s.m === 'both').on, '3 the mode is untouched by a words or font change');
    if (shots) await page.screenshot({ path: path.join(shots, 'HILITE-2-font-changed.png'), clip: { x: 360, y: 120, width: 285, height: 440 } });

    // 4 · the Front thumbnail: its own system words ("W"); then Cancel brings every tint back
    const hasCard = await page.evaluate(() => !!document.querySelector('.pvSetup .engFace[data-face=front] .engThumb'));
    if (hasCard) {
      await page.click(`${PV} .engFace[data-face=front] .engThumb`);
      await page.waitForFunction(() => document.querySelector('.pvSetup .engp') && document.querySelector('.pvSetup .engp').dataset.face === 'front');
      m = await marks(PV);
      check(m.words.value === 'W' && m.words.sys === 'same' && m.words.mark && m.words.mark.title === TITLE && m.words.bg !== CARD_BG, '4 the Front\'s words box ("W") carries its own mark and tint');
      check(m.font.sys === 'same' && !m.font.ghost && m.font.value === '', '4 the Pristina staged on the Back is the Back\'s own: the Front\'s drop-down is still the system\'s font, tinted, no ghost: ' + JSON.stringify({ value: m.font.value, sys: m.font.sys }));
    } else console.log('  (no card thumbnails in this build: the Front step was skipped)');
    await page.click(`${PV} [data-engp=cancel]`);
    await page.waitForFunction(() => { const c = document.querySelector('.pvSetup [data-engp=cancel]'); return c && c.disabled; });
    m = await marks(PV);
    check(m.mode === 'both' && onlySeg(m, 'both') && m.words.sys === 'same' && m.font.sys === 'same' && !m.font.ghost && m.font.opts.filter(o => o.star).length === 1, '4 after Cancel everything is the system\'s again: every tint is back, the marks never left');

    // ── 5-8 · in a 232px slot (the narrowest the Engrave detail gives the panel), with a real preview ──
    await page.evaluate(() => { document.getElementById('__slot')?.remove(); const s = document.createElement('div'); s.id = '__slot'; s.style.cssText = 'position:fixed;left:20px;top:20px;width:232px;background:#fffefb;z-index:99999;max-height:96vh;overflow:auto'; document.body.appendChild(s); });
    const mountSlot = (extra = '') => page.evaluate(({ KEY, extra }) => {
      const row = B.orders.byKey.get(KEY), s = document.getElementById('__slot'); CharmNestEngravePanel.DRAFTS.clear(); s.textContent = '';
      window.__h && window.__h.destroy();
      window.__h = CharmNestEngravePanel.mount(s, row, Object.assign({ job: Engrave.jobOf(row, null, 'back'), face: 'back' }, extra ? JSON.parse(extra) : {}));
    }, { KEY, extra });
    const S = '#__slot';

    // 7 · one row at 232px, the marks take no room
    await mountSlot();
    const row232 = () => page.evaluate(() => {
      const p = document.querySelector('#__slot .engp'), segs = [...p.querySelectorAll('.engpMode:not(.engpFaceSw) .engpSeg')], box = p.querySelector('.engpMode:not(.engpFaceSw)').getBoundingClientRect();
      return { panelW: Math.round(p.getBoundingClientRect().width), tops: segs.map(s => Math.round(s.getBoundingClientRect().top)), w: segs.map(s => Math.round(s.getBoundingClientRect().width * 100) / 100), right: Math.max(...segs.map(s => s.getBoundingClientRect().right)), boxRight: box.right, marks: p.querySelectorAll('.engpSys').length };
    });
    const withMarks = await row232();
    await page.evaluate(() => { const p = document.querySelector('#__slot .engp'); p.querySelectorAll('.engpSys').forEach(k => k.remove()); p.querySelectorAll('[data-sys]').forEach(k => delete k.dataset.sys); });
    const without = await row232();
    check(withMarks.marks >= 2 && without.marks === 0, '7 the marks are really there to compare (mode + words; the font\'s star is in its list): ' + withMarks.marks + ' with, ' + without.marks + ' without');
    check(withMarks.panelW >= 231 && new Set(withMarks.tops).size === 1 && withMarks.right <= withMarks.boxRight + 0.5, `7 at ${withMarks.panelW}px the four choices are still ONE row inside the control: tops ${withMarks.tops}, last right ${Math.round(withMarks.right)} <= ${Math.round(withMarks.boxRight)}`);
    check(JSON.stringify(withMarks.w) === JSON.stringify(without.w), `7 the marks take no room: segment widths are the same with and without them: ${withMarks.w} / ${without.w}`);
    await mountSlot();
    if (shots) await page.screenshot({ path: path.join(shots, 'HILITE-3-232px-untouched.png'), clip: { x: 10, y: 10, width: 252, height: 330 } });

    // 5 · a saved override with a recorded reason (ENGWHY's field): the selection is the person's, the marks are on the system's choice and say why
    await page.evaluate(KEY => { const row = B.orders.byKey.get(KEY), j = Engrave.jobOf(row, null, 'back'); Object.assign(j, { text: 'Name Two', lines: ['Name Two'] }); row.engravingOverride = { mode: 'back', back: { text: 'Name Two', fontWord: 'Pristina' }, by: 'Name One', at: 1, reason: { code: 'asked', text: 'Customer asked in a message', by: 'Name One', at: 1 } }; }, KEY);
    await mountSlot();
    m = await marks(S);
    const segBoth = m.seg.find(s => s.m === 'both'), segBack = m.seg.find(s => s.m === 'back');
    check(segBack.on && !segBoth.on && onlySeg(m, 'both') && startsWith(segBoth.mark.title, `${TITLE}\n${REASON}`) && segBoth.title === segBoth.mark.title, '5 override Back saved with a reason: Back is selected, the mark stays on Front + back and its title shows the reason read-only: ' + JSON.stringify(segBoth.title));
    check(m.words.value === 'Name Two' && m.words.sys === 'changed' && startsWith(m.words.mark.title, `${TITLE}: 1. 27. 24\n${REASON}`), '5 the words box shows the person\'s words, its mark names the system\'s words and the reason: ' + JSON.stringify(m.words.mark));
    check(m.font.value === 'Pristina' && m.font.sys === 'changed' && m.font.ghost && m.font.ghost.title.startsWith(`${TITLE}: Ace\n${REASON}`) && m.font.opts.filter(o => o.star).map(o => o.v).join() === '', '5 the font: Pristina selected, the ghost names the system\'s font and the reason: ' + JSON.stringify(m.font.ghost));
    if (shots) await page.screenshot({ path: path.join(shots, 'HILITE-4-232px-overridden.png'), clip: { x: 10, y: 10, width: 252, height: 330 } });
    // a code with no text still reads (the code, plainly); no reason at all: the plain title only
    await page.evaluate(KEY => { const row = B.orders.byKey.get(KEY); row.engravingOverride.reason = { code: 'misread', by: 'Name One', at: 1 }; }, KEY);
    await mountSlot(); m = await marks(S);
    check(/^Chosen by the system\nChanged: Misread( the note)? \u00b7 Name One/.test(m.seg.find(s => s.m === 'both').mark.title), '5 a reason with only a code reads in the button\'s words (ENGWHY\'s labels): ' + JSON.stringify(m.seg.find(s => s.m === 'both').mark.title));
    await page.evaluate(KEY => { const row = B.orders.byKey.get(KEY); delete row.engravingOverride.reason; }, KEY);
    await mountSlot(); m = await marks(S);
    check(m.seg.find(s => s.m === 'both').mark.title === TITLE && m.words.mark.title === `${TITLE}: 1. 27. 24`, '5 no recorded reason: the titles carry no reason line');
    await page.evaluate(KEY => { const row = B.orders.byKey.get(KEY), j = Engrave.jobOf(row, null, 'back'); Object.assign(j, { text: '1. 27. 24', lines: ['1. 27. 24'] }); delete row.engravingOverride; }, KEY);

    // 6 · the source of truth is autoChoice (ENGAUTO) when the page has it: stubbed here: the system chose Front only, "Name Three", Pristina
    await page.evaluate(() => { const S = CharmNestEngraveSetup; window.__autoWas = S.autoChoice; S.autoChoice = () => ({ mode: 'front', faces: ['front'], front: { text: 'Name Three', lines: ['Name Three'], fontWord: 'Pristina', fontId: 'marck-script' }, back: null, position: 'auto' }); });
    await mountSlot(); m = await marks(S);
    check(m.mode === 'both' && onlySeg(m, 'front') && m.seg.find(s => s.m === 'both').on && !m.seg.find(s => s.m === 'both').mark, '6 autoChoice says Front only: the mark is on Front (not selected), the person\'s/auto Front + back reading is just selected');
    check(m.face === 'back' && m.words.sys === '' && !m.words.mark && !m.font.ghost && m.font.opts.every(o => !o.star), '6 the Back is not engraved by the system\'s choice: no words mark, no font mark on it');
    await page.evaluate(() => __h.setFace('front')); m = await marks(S);
    check(m.face === 'front' && m.words.sys === 'changed' && m.words.mark.title === `${TITLE}: Name Three` && m.font.opts.filter(o => o.star).map(o => o.v).join() === 'Pristina' && m.font.sys === 'changed' && m.font.ghost.title === `${TITLE}: Pristina`,
      '6 the Front: the system\'s words and font (Pristina, its entry starred) are marked: ' + JSON.stringify({ words: m.words.mark, star: m.font.opts.filter(o => o.star).map(o => o.label), ghost: m.font.ghost }));
    await page.fill(`${S} .engpWords`, 'Name Three'); await page.selectOption(`${S} .engpFont`, 'Pristina'); m = await marks(S);   // (by value: the system's entry shows the label "\u2726 Pristina", its text and value are unchanged)
    check(m.words.sys === 'same' && m.font.sys === 'same' && !m.font.ghost && m.font.selTitle === TITLE, '6 typing the system\'s words and picking its font: both tints, no ghost');
    await page.evaluate(() => { if (window.__autoWas) CharmNestEngraveSetup.autoChoice = window.__autoWas; else delete CharmNestEngraveSetup.autoChoice; });

    // 8 · an approved face keeps its marks (and is not locked: its words box is open)
    const locked = await page.evaluate(KEY => {
      const row = B.orders.byKey.get(KEY), s = document.getElementById('__slot'), j = Engrave.jobOf(row, null, 'back'), was = { state: j.state, by: j.approvedBy }; Object.assign(j, { state: 'approved', approvedBy: 'Name One' });
      CharmNestEngravePanel.DRAFTS.clear(); __h.destroy(); s.textContent = ''; window.__h = CharmNestEngravePanel.mount(s, row, { job: j, face: 'back' });
      const out = { disabled: s.querySelector('.engpWords').disabled, mode: !!s.querySelector('.engpMode .engpSeg[data-sys] > .engpSys'), words: !!s.querySelector('.engpBox > .engpSys'), font: !!s.querySelector('.engpOption, .engpFont option[data-sys]') };
      Object.assign(j, { state: was.state, approvedBy: was.by }); __h.destroy(); s.textContent = ''; CharmNestEngravePanel.DRAFTS.clear(); return out;
    }, KEY);
    check(!locked.disabled && locked.mode && locked.words && locked.font, '8 an approved face is open for a change and still shows what the system chose: ' + JSON.stringify(locked));
    const ear = await page.evaluate(KEY => {
      const row = B.orders.byKey.get(KEY), earRow = Object.assign({}, row, { key: 'ear_1', spec: Object.assign({}, row.spec, { engraveNever: { rule: 'earring', form: 'earring', why: 'never' } }) });
      const slot = document.createElement('div'); document.body.appendChild(slot); CharmNestEngravePanel.mount(slot, earRow, { jobs: () => [] });
      const out = { html: slot.innerHTML, marks: slot.querySelectorAll('.engpSys').length }; slot.remove(); return out;
    }, KEY);
    check(ear.html === '' && ear.marks === 0, '8 an earring (never engraved) has no panel, so no mark and no Back choice');

    // the page was never asked anything it must not be
    const net = await page.evaluate(() => __calls.slice());
    const reads = srv.st.calls.filter(c => c.body && (c.body.mode === 'engraveIntent' || c.body.op === 'agent' && /engrave/i.test(JSON.stringify(c.body).slice(0, 400))));
    check(!net.includes('notes.set') && reads.length === 0, '8 no staff note, no paid reading: calls ' + JSON.stringify(net));
    check(errors.length === 0, '8 no page errors: ' + errors.join(' | ').slice(0, 300));
  } finally { await browser.close(); srv.close && srv.close(); }
  await realWorld(check, shots);
  if (fails.length) { console.log('engrave-highlight: ' + fails.length + ' failed'); process.exit(1); }
  console.log('engrave-highlight: ok');
})().catch(e => { console.error(e); process.exit(1); });
