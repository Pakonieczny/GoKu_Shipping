// LOCKSAVE (Paul, 28 Oct 2026: "I tried to save my changes in a charm editor, but then got this strange error, which I should not be seeing because there was nothing wrong with this charm": a "Not
// saved yet" dialog over the bird's BACK listing "Back · Outline · kept: on a sheet: the back shows only the cut lines …" and "Back · Cut line · kept …"). As in Illustrator, a locked part is never
// selected and never moved, and on Save the back's shape is never a refusal. SHAREDSHAPE (Paul, 29 Oct 2026: "Now you made it so I can't unlock the charm layers ... the user must always be able to
// unlock all the layers of any charm"): the locks are the person's, every one can be taken off and put on again. On the real bird (fixtures/PERCHED_43564.ai, SILHOUETTE_34560) opened from its BACK
// card, the "Charm Maker" words on it, its cut lines locked as they open:
//   L1 ⌘A          L2 Select ▸ All          L3 a marquee over the whole artboard          L4 the Layers panel's selection square on each locked row          L5 Select Inverse, Select Same
//   L6 Next Object (first above, last below)   L7 a double-click on the cut line   L8 a script's ctx.select of every part: never a locked part selected, nothing locked moved by the nudge after it
//   L9 the locks are the person's: the Layers lock column takes a cut line's lock off and puts it on again; Unlock All takes them all off, and ⌘A then selects the cut lines too; locked again
//      in Layers, ⌘A, a nudge and Align Left leave them unselected and where they were
//   L10 the words moved (⌘A and a nudge) and Save: saved, no refusal, the editor closed
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/ce-locked-select.cjs      (SHOTS=<dir> for screenshots)
'use strict';
const path = require('node:path');
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : evidence || ''}`.trimEnd()); };
const Q = '#egQueue .rvItem[data-kind=placement]';
const settleP = page => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));

(async () => {
  const R = require('./_engraveRealWorld.cjs');
  R.DESIGNS.SILHOUETTE_34560 = { file: 'PERCHED_43564.ai', entry: { sku: 'SILHOUETTE_34560', widthPt: 36.515260000000126, heightPt: 31.177639999999883, areaPt2: 449.3611111111111, members: 4, holes: 2, engravable: true, engravableBy: 'index', upAngle: 267.50234443756403, upSource: 'hole', charmHash: '831b2d80', backKeepOut: [] } };
  R.ORDERS.sil = Object.assign({}, R.ORDERS.back, { rid: '4290000141', sku: 'SILHOUETTE_34560', listing: '1890000141', title: 'Perched Bird Silhouette Necklace in Sterling Silver', buyer: 'Name Twelve', personalization: ['Charm Maker'] });
  R.EXPECT_JOBS.sil = 1;
  const w = await R.world({ viewport: { width: 1440, height: 900 }, shots: process.env.SHOTS || '/tmp/ce-locked-select-shots' });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  await w.page.waitForFunction(() => !window.CNRole || ['ask', 'role', 'admin'].includes(CNRole.state()), null, { timeout: 20000 }).catch(() => {});
  await w.page.evaluate(() => { try { if (window.CNRole && CNRole.state() === 'ask') CNRole.choose('design'); } catch (_) { /* no role step */ } });
  const page = w.page, st = w.srv.st;
  try {
    await R.build(w, ['sil']);
    const sheets = await page.evaluate(() => { const out = []; for (const s of allSheets()) if (s.charms && s.charms.length) { if (!s.folderPath) s.folderPath = 'charmnest/sheets/' + s.sheetId; out.push({ sheetId: s.sheetId, poolIds: s.charms.map(x => x.poolId) }); } return out; });
    for (const s of sheets) st.put('Charm_Nest_Sheets', s.sheetId, { sheetId: s.sheetId, poolIds: s.poolIds, backPool: [] });
    await R.openEngraveDetail(w, 'sil', { face: 'back' }); await R.settle(page, 700);
    await page.$eval(Q + ' .egCharmEdit', b => b.click());
    await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }); await settleP(page); await settleP(page);

    /** Every part (deep): id, locked, fixed, its box; what is selected, and which of it is locked. */
    const look = () => page.evaluate(() => {
      const E = CharmEditor.current, all = []; const walk = (l, lk) => l.forEach(i => { all.push({ id: i.id, role: i.role, locked: !!i.locked || lk, fixed: i.role === 'cut', box: E.box(i.id) }); if (i.children) walk(i.children, lk || !!i.locked); }); walk(E.items(), false);
      const sel = E.selection(); return { all, sel, bad: sel.filter(id => { const x = all.find(a => a.id === id); return !x || x.locked; }) };
    });
    const s0 = await look(), fixed = s0.all.filter(a => a.fixed), boxes0 = JSON.stringify(fixed.map(a => a.box));
    check(`L0 the bird's back opens with its cut lines locked (${fixed.map(a => a.role).join(', ')}), the words free (${s0.all.filter(a => !a.locked).map(a => a.role || a.id).join(', ')})`,
      fixed.length >= 1 && fixed.every(a => a.locked) && s0.all.some(a => !a.locked), JSON.stringify(s0.all));
    const unmoved = async () => JSON.stringify((await look()).all.filter(a => a.fixed).map(a => a.box)) === boxes0;
    const none = () => page.evaluate(() => CharmEditor.current.ai.act('deselect'));
    const nudge = async () => { await page.keyboard.press('ArrowRight'); await page.keyboard.press('Shift+ArrowDown'); await settleP(page); };
    const path_ = async (id, label, fn, opts = {}) => {
      await none(); await settleP(page); await fn(); await settleP(page);
      const s = await look(); if (opts.nudge !== false) await nudge();
      const still = await unmoved();
      check(`${id} ${label}: ${s.sel.length} selected, no locked part among them; the locked parts not moved`, s.bad.length === 0 && still && (opts.some === false || s.sel.length > 0), JSON.stringify({ sel: s.sel, bad: s.bad, still }));
    };
    const stage = async () => { const f = await page.evaluate(() => CharmEditor.current.frame()); const p = await page.evaluate(([x, y]) => CharmEditor.current.clientOf(x, y), [f.x0 + f.wPt + 6, f.y0 + f.hPt + 6]); await page.mouse.click(p.x, p.y); };
    const cmd = (name) => page.evaluate(n => { const c = CharmEditor.commands[n]; if (!c) return 'no ' + n; c.run(CharmEditor.current.ctx); return 'ok'; }, name);

    await path_('L1', '⌘A', async () => { await page.keyboard.press('v'); await stage(); await page.keyboard.press('ControlOrMeta+a'); });
    await path_('L2', 'Select ▸ All', () => page.evaluate(() => CharmEditor.current.ai.act('selectAll')));
    await path_('L3', 'a marquee over the whole stage', async () => {   // (corner to corner of the stage: the artboard fills it)
      const r = await page.evaluate(() => { const b = document.querySelector('.ce-root .ce-stage').getBoundingClientRect(); return [b.left, b.top, b.right, b.bottom]; }), a = { x: r[0] + 6, y: r[1] + 6 }, b = { x: r[2] - 6, y: r[3] - 6 };
      await page.keyboard.press('v'); await page.mouse.move(a.x, a.y); await page.mouse.down(); await page.mouse.move(b.x, b.y, { steps: 8 }); await page.mouse.up();
    });
    let rows = 0;
    await path_('L4', 'the Layers selection square on each locked row', async () => {
      rows = await page.evaluate(() => { const r = [...document.querySelectorAll('.ce-root .ce-layer.is-locked')]; for (const x of r) { const b = x.querySelector('[data-lact="sel"]'); if (b) b.click(); } return r.length; });
    }, { some: false });
    check(`L4 ... (${rows} locked rows in Layers)`, rows >= 1, String(rows));
    const r5 = [];
    await path_('L5', 'Select Inverse with nothing selected', async () => { r5.push(await cmd('ops.inverse')); });
    await path_('L5', 'Select Same Stroke and Fill from every part', async () => { await page.evaluate(() => CharmEditor.current.ctx.select(CharmEditor.current.items().map(i => i.id))); r5.push(await cmd('ops.sameStroke')); r5.push(await cmd('ops.sameFill')); }, { some: false });
    check(`L5 ... the commands ran (${r5.join(', ')})`, r5.every(x => x === 'ok'), r5.join());
    const r6 = [];
    await path_('L6', 'First Object Above', async () => { r6.push(await cmd('ai.selFirstAbove')); });
    await path_('L6', 'Last Object Below, then Next Object Above', async () => { r6.push(await cmd('ai.selLastBelow')); r6.push(await cmd('ai.selNextAbove')); });
    check(`L6 ... the commands ran (${r6.join(', ')})`, r6.every(x => x === 'ok'), r6.join());
    await path_('L7', 'a double-click on the cut line', async () => {
      const b = fixed[0].box; const p = await page.evaluate(([x, y]) => CharmEditor.current.clientOf(x, y), [b[0], (b[1] + b[3]) / 2]); await page.keyboard.press('v'); await page.mouse.dblclick(p.x, p.y);
    }, { some: false });
    await path_('L8', "a script's ctx.select of every part", () => page.evaluate(() => CharmEditor.current.ctx.select(CharmEditor.current.items().map(i => i.id))));

    // L9 · Unlock All and a lock toggled: what the face cannot change stays locked, unselected and where it was
    // (SHAREDSHAPE: the locks are the person's) one cut line's lock taken off in the Layers lock column, then put on again
    const lockOf = id => page.evaluate(id => { const r = document.querySelector(`.ce-root .ce-layer[data-id="${CSS.escape(id)}"]`), b = r && r.querySelector('[data-lact="lock"]'); return { row: !!r, pressed: b ? b.getAttribute('aria-pressed') : null, locked: !!(CharmEditor.current.items().find(i => i.id === id) || {}).locked }; }, id);
    const clickLock = id => page.evaluate(id => { const b = document.querySelector(`.ce-root .ce-layer[data-id="${CSS.escape(id)}"] [data-lact="lock"]`); if (b) b.click(); return !!b; }, id);
    const c0 = fixed[0].id; await clickLock(c0); await settleP(page); const off1 = await lockOf(c0); await clickLock(c0); await settleP(page); const on1 = await lockOf(c0);
    check(`L9 a cut line's lock taken off in the Layers lock column (${JSON.stringify(off1)}) and put on again (${JSON.stringify(on1)})`, off1.row && !off1.locked && off1.pressed === 'false' && on1.locked && on1.pressed === 'true', JSON.stringify({ off1, on1 }));
    await none(); await page.evaluate(() => CharmEditor.current.ai.act('unlockAll')); await settleP(page);
    const s9 = await look(); await stage(); await page.keyboard.press('ControlOrMeta+a'); await settleP(page); const all9 = await page.evaluate(() => CharmEditor.current.selection());
    check(`L9 Unlock All takes every lock off (${s9.all.filter(a => a.locked).length} locked) and ⌘A then selects the cut lines too (${all9.length} selected)`, s9.all.every(a => !a.locked) && fixed.every(a => all9.includes(a.id)), JSON.stringify({ s9: s9.all, all9 }));
    await none(); for (const a of fixed) await clickLock(a.id); await settleP(page);
    const s9b = await look();
    check('L9 ... and each locked again in Layers', s9b.all.filter(a => a.fixed).every(a => a.locked), JSON.stringify(s9b.all));
    await path_('L9', 'then ⌘A, a nudge and Align Left', async () => { await stage(); await page.keyboard.press('ControlOrMeta+a'); await nudge(); const k = await page.evaluate(() => Object.keys(CharmEditor.commands).find(k => /align.*left/i.test(k))); if (k) await cmd(k); });

    // L10 · the words moved and Save: no refusal
    await none(); await stage(); await page.keyboard.press('ControlOrMeta+a'); await nudge(); await nudge();
    const dirty = await page.evaluate(() => CharmEditor.current.dirty());
    await page.click('.ce-root [data-act="save"]');
    const t0 = Date.now(); let out = null;
    while (Date.now() - t0 < 60000) { out = await page.evaluate(() => ({ open: !!document.querySelector('.ce-root'), refuse: (document.querySelector('.ce-root .ce-refuse') || {}).textContent || '' })); if (!out.open || out.refuse) break; await new Promise(r => setTimeout(r, 250)); }
    await R.shot(w, 'locked-L10');
    check(`L10 the words moved on the back (dirty ${dirty}) and Save: saved, no "Not saved yet", the editor closed`, dirty && out && !out.open && !out.refuse, JSON.stringify(out).slice(0, 400));
    check('errors on the page: none', !w.errors.length, JSON.stringify(w.errors).slice(0, 400));
  } finally { await w.close(); }
  const bad = RESULTS.filter(r => !r.ok);
  console.log(bad.length ? `FAILED ${bad.length} of ${RESULTS.length}: ${bad.map(r => r.id).join(', ')}` : `PASS ${RESULTS.length} checks: LOCKSAVE — locked parts never selected or moved by any selection path, every lock the person's to take off and put on (SHAREDSHAPE), the back saves with no refusal`);
  if (bad.length) process.exitCode = 1;
})().catch(e => { console.error(e); process.exit(1); });
