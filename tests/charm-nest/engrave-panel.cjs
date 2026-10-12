// ENGPANEL (engrave-ui-1012): the engraving panel of the Engrave tab's detail card (charm-nest-engrave-panel.js), in the REAL page (charm-nest-1.html, headless Chromium against the local fake site;
// offline: no Etsy, no paid reading, no live write). Paul's image 7 / 9: the menu was a one-character-wide text box, a font drop-down on top of it, four choices on two rows, an "Undo my edit" button.
//   1  the mode control is ONE row: None | Front | Back | Front + back (all four on the same line, at the real width of the Engrave detail's column)
//   2  the text area is a normal width (>= 200px: computed in the real stylesheet), the font drop-down is under it and does not touch it; Save and Cancel are the only action buttons
//   3  none of the old words: no "ENGRAVES", "The system read", "nothing in the listing", "Undo", no percentage
//   4  an earring (spec.engraveNever) gets no panel, so it has no Back choice anywhere
//   5  picking a face switches the editor; editing the Front never changes the Back (the draft, the saved override and the jobs)
//   6  the position row acts on THAT face's job; Save and Cancel are off until something is staged; Cancel puts the position back (snapshot / restore)
//   7  typing and picking a font repaint THAT face's thumbnail at once (before Save), never the other one
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/engrave-panel.cjs
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const SHIP = Math.floor(Date.UTC(2026, 9, 14, 17) / 1000), RID = '4195966039', TX = '5009123456789', KEY = `${RID}_${TX}`;
const ORDER = { receiptId: RID, orderNumber: RID, createTs: SHIP - 5 * 86400, updateTs: SHIP - 5 * 86400 + 60, shipBy: SHIP, buyer: { name: 'Name One' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: TX, listingId: '4458930445', sku: 'MINI_0445', title: 'Mini Tag Initial Charm Add On Charm Personalized Letter Charm', quantity: 1, expectedShipDate: SHIP, metalKey: 'gold', metalLabel: 'GF 14/20',
    variations: [{ name: 'Charm Type', value: 'Tag1 (front+back)' }, { name: 'Font choice', value: 'Ace' }, { name: 'Metal Choice', value: 'Gold Filled' }], personalization: ['front - W\nback - 1. 27. 24'] }] };

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { try { ({ chromium } = require('/opt/node22/lib/node_modules/playwright/node_modules/playwright-core')); } catch (__) { console.log('engrave-panel: playwright-core not found, skipped'); process.exit(0); } }
  const shots = process.env.SHOTS || ''; if (shots) fs.mkdirSync(shots, { recursive: true });
  const srv = await start({ receipts: [] });
  const POOLID = KEY + '_1';
  srv.st.put('Charm_Nest_Sheets', 'gold-su-1', { id: 'gold-su-1', metal: 'gold', charms: [{ id: 'g0', poolId: POOLID, order: RID }], placements: [{ id: 'g0', cxPt: 40, cyPt: 40, angle: 0 }], poolIds: [POOLID], orders: [RID], sheetIndex: 1, fileBase: 'gold-su-1', runId: 'run-test-1' });
  srv.st.put('Charm_Pool', POOLID, { poolId: POOLID, orderId: RID, sheetId: 'gold-su-1', state: 'placed', material: 'gold' });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = [];
  const check = (ok, msg) => { if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
  try {
    const context = await browser.newContext({ viewport: { width: 1280, height: 720 }, deviceScaleFactor: 1 });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {} });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Engrave && window.CharmNestEngraveSetup && window.CharmNestEngravePanel && CN.S.cloud.ok === true, null, { timeout: 60000 });

    // ── the order (read by the real intake: Front + back, "front - W / back - 1. 27. 24") and its two jobs ──
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

    // ── 1-3 · the panel in the Engrave tab's detail, at 1280x720, 1100x800 (two columns) and 1024x768 (stacked) ──
    for (const [w, h] of [[1280, 720], [1100, 800], [1024, 768]]) {
      await page.setViewportSize({ width: w, height: h });
      await page.evaluate(() => { setMode('engrave'); Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: null }); Engrave.render(); });
      await page.waitForSelector('.pvSetup .engp', { timeout: 20000 });
      const m = await page.evaluate(() => {
        const p = document.querySelector('.pvSetup .engp'), r = e => { const b = e.getBoundingClientRect(); return { l: b.left, t: b.top, r: b.right, b: b.bottom, w: b.width, h: b.height }; };
        const segs = [...p.querySelectorAll('.engpMode:not(.engpFaceSw) .engpSeg')], ta = p.querySelector('.engpWords'), sel = p.querySelector('.engpFont');
        return { panel: r(p), segs: segs.map(s => ({ t: s.textContent.trim(), top: Math.round(r(s).t), right: Math.round(r(s).r) })), segBox: r(p.querySelector('.engpMode:not(.engpFaceSw)')), ta: r(ta), sel: r(sel),
          taW: parseFloat(getComputedStyle(ta).width), buttons: [...p.querySelectorAll('button')].map(b => b.textContent.trim()).filter(Boolean), text: p.textContent.replace(/\s+/g, ' ') };
      });
      check(m.segs.map(s => s.t).join('|') === 'None|Front|Back|Front + back', `${w}px: the choices are None | Front | Back | Front + back: ${m.segs.map(s => s.t).join(' | ')}`);
      check(new Set(m.segs.map(s => s.top)).size === 1 && m.segs.every(s => s.right <= Math.ceil(m.segBox.r)), `${w}px: all four on ONE row inside the control (tops ${m.segs.map(s => s.top)})`);
      check(m.panel.w >= 232 - 1, `${w}px: the panel is at least 232px wide: ${Math.round(m.panel.w)}px`);
      check(m.taW >= 200 && m.ta.w >= 200, `${w}px: the text area is a normal width: ${Math.round(m.ta.w)}px (computed ${Math.round(m.taW)})`);
      check(m.sel.t >= m.ta.b - 0.5 && m.sel.w >= 200, `${w}px: the font drop-down is under the text area, full width, touching nothing: select top ${Math.round(m.sel.t)} >= text bottom ${Math.round(m.ta.b)}, width ${Math.round(m.sel.w)}`);
      const acts = m.buttons.filter(b => !/^(None|Front|Back|Front \+ back)$/.test(b));
      check(acts.join('|') === 'Save|Cancel', `${w}px: Save and Cancel are the only action buttons: ${acts.join(' | ')}`);
      check(!/ENGRAVES|system read|nothing in the listing|Undo|sure|%/i.test(m.text), `${w}px: none of the old words in the panel: "${m.text.slice(0, 120)}"`);
      if (shots) await page.screenshot({ path: path.join(shots, `panel-${w}.png`) });
    }
    await page.setViewportSize({ width: 1280, height: 720 });

    // ── 4 · an earring has no panel, so no Back choice ──
    const ear = await page.evaluate(KEY => {
      const row = B.orders.byKey.get(KEY), earRow = Object.assign({}, row, { key: 'ear_1', spec: Object.assign({}, row.spec, { engraveNever: { rule: 'earring', form: 'earring', why: 'never' } }) });
      const slot = document.createElement('div'); document.body.appendChild(slot);
      CharmNestEngravePanel.mount(slot, earRow, { jobs: () => [] });
      const out = { html: slot.innerHTML, hidden: slot.hidden, modes: CharmNestEngravePanel.modesFor(earRow).length }; slot.remove(); return out;
    }, KEY);
    check(ear.html === '' && ear.hidden && ear.modes === 0, 'an earring (spec.engraveNever) draws no panel at all, so it has no Back choice: ' + JSON.stringify(ear));

    // ── a face nobody could place (an unlabelled "both") asks in one line; an approved face is locked, with its position row off ──
    const odd = await page.evaluate(KEY => {
      const row = B.orders.byKey.get(KEY), out = {}, slot = document.createElement('div'); document.body.appendChild(slot);
      const ask = Object.assign({}, row, { key: 'ask_1', spec: Object.assign({}, row.spec, { engraving: { v: 1, mode: 'both', source: 'note', textSource: 'unlabelled', needsPerson: true, confidence: 0.5, raw: 'Name One Name Two', front: null, back: null } }) });
      CharmNestEngravePanel.DRAFTS.clear(); CharmNestEngravePanel.mount(slot, ask, { jobs: () => [], face: 'front' });
      out.ask = { prompt: !!slot.querySelector('.engpAsk'), raw: (slot.querySelector('.engpRaw') || {}).textContent || '' };
      slot.textContent = ''; CharmNestEngravePanel.DRAFTS.clear();
      const j = Engrave.jobOf(row, null, 'back'), was = { state: j.state, by: j.approvedBy }; Object.assign(j, { state: 'approved', approvedBy: 'Name One' });
      CharmNestEngravePanel.mount(slot, row, { job: j, face: 'back' });
      out.locked = { words: slot.querySelector('.engpWords').disabled, font: slot.querySelector('.engpFont').disabled, lock: (slot.querySelector('.engpLock') || {}).textContent || '', pos: [...slot.querySelectorAll('.engpPb')].every(b => b.disabled) || !slot.querySelector('.engpPb') };
      Object.assign(j, { state: was.state, approvedBy: was.by }); slot.remove(); CharmNestEngravePanel.DRAFTS.clear(); return out;
    }, KEY);
    check(odd.ask.prompt && odd.ask.raw === 'Name One Name Two', 'words nobody could place: the panel asks in one line and shows the buyer\'s own words: ' + JSON.stringify(odd.ask));
    check(odd.locked.words && odd.locked.font && odd.locked.lock === 'Approved by Name One' && odd.locked.pos, 'an approved face is locked (words, font, position off) and says by whom: ' + JSON.stringify(odd.locked));

    // ── 7b · the REAL preview component (charm-nest-engrave-preview.js): typing and picking a font change what the thumbnail draws, at once ──
    const real = await page.evaluate(async KEY => {
      const P = window.CharmNestEngravePreview; if (!P) return { skipped: true };
      const row = B.orders.byKey.get(KEY), th = {}, box = document.createElement('div'); box.style.cssText = 'position:fixed;left:300px;top:40px;width:260px;z-index:99998';
      for (const f of ['front', 'back']) { const t = document.createElement('div'); t.className = 'engThumb'; t.style.cssText = 'width:120px;height:120px;display:inline-block'; box.appendChild(t); th[f] = t; P.render(t, { row, text: f === 'front' ? 'W' : '1. 27. 24', face: f, fontId: P.fontIdOf(row, f, '') }); }
      document.body.appendChild(box);
      const s = document.createElement('div'); s.style.cssText = 'position:fixed;left:20px;top:40px;width:232px;z-index:99999'; document.body.appendChild(s);
      CharmNestEngravePanel.DRAFTS.clear();
      const h = CharmNestEngravePanel.mount(s, row, { job: Engrave.jobOf(row, null, 'back'), face: 'back', thumb: f => th[f] }); h.setFace('front');
      const ta = s.querySelector('.engpWords'), sel = s.querySelector('.engpFont'), txt = f => th[f].querySelector('.engpvText');
      ta.value = 'Name Two'; ta.dispatchEvent(new Event('input', { bubbles: true }));
      const typed = { front: txt('front') && txt('front').textContent, back: txt('back') && txt('back').textContent };
      sel.value = 'Pristina'; sel.dispatchEvent(new Event('change', { bubbles: true }));
      const fam = { front: txt('front') && getComputedStyle(txt('front')).fontFamily, back: txt('back') && getComputedStyle(txt('back')).fontFamily, select: sel.style.fontFamily };
      const out = { typed, fam, id: P.fontIdOf(row, 'front', 'Pristina') };
      h.destroy(); s.remove(); box.remove(); CharmNestEngravePanel.DRAFTS.clear(); return out;
    }, KEY);
    if (!real.skipped) {
      check(/name two/i.test(real.typed.front || '') && !/name two/i.test(real.typed.back || ''), 'the real preview: the typed words (capitals: the line font is Ace) are on the FRONT thumbnail at once, the back still shows its own: ' + JSON.stringify(real.typed));
      check(real.id && new RegExp('CNEng ' + real.id).test(real.fam.front || '') && !new RegExp('CNEng ' + real.id).test(real.fam.back || ''), 'and the picked font (Pristina -> ' + real.id + ') is the front thumbnail\'s font family, not the back\'s: ' + JSON.stringify(real.fam));
    } else console.log('  (no CharmNestEngravePreview in this build: live-preview check skipped)');

    // ── 5-7 · faces, position and the live thumbnails, in a slot 232px wide ──
    await page.evaluate(KEY => {
      document.getElementById('__slot')?.remove(); const s = document.createElement('div'); s.id = '__slot'; s.style.cssText = 'position:fixed;left:20px;top:40px;width:232px;background:#fffefb;padding:0;z-index:99999;max-height:90vh;overflow:auto'; document.body.appendChild(s);
      const row = B.orders.byKey.get(KEY); delete row.engravingOverride; CharmNestEngravePanel.DRAFTS.clear();
      window.__log = { nudge: [], restore: [], snap: [], size: [], centre: [], update: [] };
      window.__thumbs = {}; for (const f of ['front', 'back']) { const t = document.createElement('div'); t.className = 'engThumb'; t.dataset.face = f; document.body.appendChild(t); __thumbs[f] = t; }
      window.__was = { pos: window.CharmNestEngravePosition, prev: window.CharmNestEngravePreview };
      window.CharmNestEngravePosition = { snapshot: j => { __log.snap.push(j.key); return { at: j.key }; }, restore: (j, s) => { __log.restore.push([j.key, s && s.at]); return true; }, nudge: (j, dx, dy) => { __log.nudge.push([j.key, dx, dy]); return { ok: true }; },
        centre: j => { __log.centre.push(j.key); return { ok: true }; }, size: (j, f) => { __log.size.push([j.key, f]); return { ok: true }; }, of: () => null };
      window.CharmNestEngravePreview = { update: (el, part) => { __log.update.push([el.dataset.face, JSON.parse(JSON.stringify(part))]); return el; }, fontIdOf: (r, f, w) => 'font:' + (w || 'line'), fontFamilyOf: id => "'CNEng " + id + "', serif", warm() {} };
      window.__h = CharmNestEngravePanel.mount(s, row, { job: Engrave.jobOf(row, null, 'back'), face: 'back', thumb: f => __thumbs[f] });
    }, KEY);
    const S = '#__slot';
    const state = () => page.evaluate(KEY => { const s = document.getElementById('__slot'), p = s.querySelector('.engp'), row = B.orders.byKey.get(KEY); return { face: p && p.dataset.face, mode: p && p.dataset.mode, text: p && p.querySelector('.engpWords') ? p.querySelector('.engpWords').value : null,
      save: p && p.querySelector('[data-engp=save]').disabled, cancel: p && p.querySelector('[data-engp=cancel]').disabled, w: p && p.querySelector('.engpWords') ? p.querySelector('.engpWords').getBoundingClientRect().width : 0,
      seg: [...s.querySelectorAll('.engpMode:not(.engpFaceSw) .engpSeg')].map(b => b.getAttribute('aria-checked') === 'true' ? b.textContent : '').join(''), ov: row.engravingOverride || null }; }, KEY);
    let st = await state();
    check(st.face === 'back' && st.mode === 'both' && st.text === '1. 27. 24' && st.save && st.cancel, 'opens on the Back (the card\'s face) with its words; Save and Cancel are off until something is staged: ' + JSON.stringify({ face: st.face, text: st.text, save: st.save, cancel: st.cancel }));
    check(st.w >= 200, 'the text area is ' + Math.round(st.w) + 'px wide in a 232px slot');

    await page.evaluate(() => __h.setFace('front'));
    st = await state();
    check(st.face === 'front' && st.text === 'W', 'picking the Front switches the editor to the front words: ' + JSON.stringify({ face: st.face, text: st.text }));
    await page.fill(S + ' .engpWords', 'Name One'); await page.selectOption(S + ' .engpFont', { label: 'Pristina' });
    st = await state();
    check(!st.save && !st.cancel, 'typing stages the edit: Save and Cancel turn on');
    await page.evaluate(() => __h.setFace('back')); st = await state();
    check(st.face === 'back' && st.text === '1. 27. 24', 'the Back still has its own words: ' + JSON.stringify({ face: st.face, text: st.text }));
    await page.evaluate(() => __h.setFace('front')); st = await state();
    check(st.text === 'Name One', 'and the Front kept what was typed (one draft per face): ' + st.text);

    // 7 · the thumbnails followed the typing, only that face's
    const up = await page.evaluate(() => __log.update.slice());
    const typed = up.filter(([f, p]) => p.text === 'Name One'), fontPick = up.filter(([f, p]) => /Pristina/.test(p.fontId || ''));
    check(typed.length >= 1 && typed.every(([f]) => f === 'front'), 'typing repainted the FRONT thumbnail with the words at once, never the back: ' + JSON.stringify(typed.map(x => x[0])));
    check(fontPick.length >= 1 && fontPick.every(([f]) => f === 'front'), 'picking a font repainted the front thumbnail in that font: ' + JSON.stringify(fontPick.map(x => [x[0], x[1].fontId])));
    check(!up.some(([f, p]) => f === 'back' && p.text === 'Name One'), 'the back thumbnail never showed the front words');

    // 6 · position: acts on THAT face's job; Cancel puts it back
    const jobKeys = await page.evaluate(KEY => ({ front: Engrave.jobOf(B.orders.byKey.get(KEY), null, 'front')?.key || null, back: Engrave.jobOf(B.orders.byKey.get(KEY), null, 'back')?.key || null }), KEY);
    const posRow = await page.evaluate(() => [...document.querySelectorAll('#__slot .engpPb')].map(b => b.dataset.act + (b.disabled ? ':off' : '')).join(','));
    // (the back job has a fit; the front job's placement is not made yet in this fixture, so its position buttons are off)
    check(/left:off/.test(posRow) && /larger:off/.test(posRow), 'a face whose words are not fitted yet has its position buttons off: ' + posRow);
    await page.evaluate(() => __h.setFace('back'));
    await page.click(S + ' [data-act=right]'); await page.click(S + ' [data-act=up]', { modifiers: ['Shift'] }); await page.click(S + ' [data-act=smaller]'); await page.click(S + ' [data-act=centre]');
    const lg = await page.evaluate(() => JSON.parse(JSON.stringify(__log)));
    check(lg.nudge.length === 2 && lg.nudge.every(n => n[0] === jobKeys.back) && lg.nudge[0][1] === 0.5 && lg.nudge[0][2] === 0 && lg.nudge[1][1] === 0 && lg.nudge[1][2] === 2, 'right = 0.5 mm, Shift + up = 2 mm, both on the BACK job only: ' + JSON.stringify(lg.nudge));
    check(lg.size.length === 1 && lg.size[0][1] === 0.95 && lg.centre.length === 1 && lg.size[0][0] === jobKeys.back, 'smaller = x0.95, centre: both on the back job');
    check(lg.snap.length === 1 && lg.snap[0] === jobKeys.back, 'the back\'s placement was snapshotted once, right before the panel first moved it (the front was not touched): ' + JSON.stringify(lg.snap));
    // a refused move (no room) dims THAT button with a title, nothing else; a move that works gives the room back; a job that cannot move (approved) turns the whole row off
    await page.evaluate(() => { const P = CharmNestEngravePosition; P.nudge = (j, dx, dy) => { __log.nudge.push([j.key, dx, dy]); return { ok: false, why: 'no room' }; }; });
    await page.click(S + ' [data-act=left]');
    let nr = await page.evaluate(() => { const b = document.querySelector('#__slot [data-act=left]'); return [b.disabled, b.title, document.querySelector('#__slot [data-act=right]').disabled]; });
    check(nr[0] && /No room/.test(nr[1]) && !nr[2], 'no room: the left button is dimmed with its title, the others stay: ' + JSON.stringify(nr));
    await page.click(S + ' [data-act=larger]');
    nr = await page.evaluate(() => document.querySelector('#__slot [data-act=left]').disabled);
    check(nr === false, 'a move that works gives the room back');
    await page.evaluate(() => { CharmNestEngravePosition.canMove = () => false; __h.draw(); });
    nr = await page.evaluate(() => [...document.querySelectorAll('#__slot .engpPb')].every(b => b.disabled));
    check(nr, 'a job that cannot move (approved, locked) has the whole position row off');
    await page.evaluate(() => { delete CharmNestEngravePosition.canMove; __h.draw(); });
    await page.click(S + ' [data-engp=cancel]'); st = await state();
    const lr = await page.evaluate(() => __log.restore.slice());
    check(lr.length === 1 && lr[0][0] === jobKeys.back && lr[0][1] === jobKeys.back, 'Cancel put the moved face\'s position back with the snapshot of that job, and touched no other job: ' + JSON.stringify(lr));
    check(st.save && st.cancel && st.text === '1. 27. 24' && st.mode === 'both' && !st.ov, 'and the words, font and mode are back to what was saved (nothing stored): ' + JSON.stringify({ text: st.text, mode: st.mode, ov: st.ov }));

    // 5 · editing the Front and saving never changes the Back
    await page.evaluate(() => __h.setFace('front')); await page.fill(S + ' .engpWords', 'Name One');
    await page.click(S + ' [data-engp=save]');
    await page.waitForFunction(KEY => !!B.orders.byKey.get(KEY).engravingOverride, KEY, { timeout: 15000 });
    const after = await page.evaluate(KEY => { const row = B.orders.byKey.get(KEY), ov = row.engravingOverride, f = Engrave.jobOf(row, null, 'front'), b = Engrave.jobOf(row, null, 'back'); return { ov, front: f && f.lines, back: b && b.lines, by: ov && ov.by, setupBack: CharmNestEngraveSetup.setupFor(row).back && CharmNestEngraveSetup.setupFor(row).back.text, setupFront: CharmNestEngraveSetup.setupFor(row).front && CharmNestEngraveSetup.setupFor(row).front.text }; }, KEY);
    check(after.ov && after.ov.front && after.ov.front.text === 'Name One' && after.by === 'Test Operator' && !after.ov.back, 'saved as row.engravingOverride by the signed-in login: only the front is in it: ' + JSON.stringify(after.ov));
    check(after.back && after.back.join('/') === '1. 27. 24' && after.setupBack === '1. 27. 24', 'the Back job and the Back setup are as they were: ' + JSON.stringify({ back: after.back, setupBack: after.setupBack }));
    check(after.setupFront === 'Name One' && after.front && (after.front.length === 0 || after.front.join('/') === 'Name One'), 'the Front setup (and its job, once it has words) say Name One: ' + JSON.stringify({ setup: after.setupFront, job: after.front }));
    await page.waitForTimeout(300); st = await state();
    check(st.save && st.cancel, 'after Save nothing is staged any more');

    // the page was never asked anything it must not be
    await page.evaluate(() => { window.CharmNestEngravePosition = __was.pos; window.CharmNestEngravePreview = __was.prev; });
    const net = await page.evaluate(() => __calls.slice());
    const reads = srv.st.calls.filter(c => c.body && (c.body.mode === 'engraveIntent' || c.body.op === 'agent' && /engrave/i.test(JSON.stringify(c.body).slice(0, 400))));
    check(!net.includes('notes.set') && reads.length === 0, 'no staff note, no paid reading: calls ' + JSON.stringify(net));
    check(errors.length === 0, 'no page errors: ' + errors.join(' | ').slice(0, 300));
  } finally { await browser.close(); srv.close && srv.close(); }
  if (fails.length) { console.log('engrave-panel: ' + fails.length + ' failed'); process.exit(1); }
  console.log('engrave-panel: ok');
})().catch(e => { console.error(e); process.exit(1); });
