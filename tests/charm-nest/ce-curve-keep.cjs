// CURVEKEEP + STAGEGONE (order 4239655218, 22 Nov 2026, Paul: "none of the text got correctly saved ... the letter A ended up on the front and the back and the curved text only on the back").
// The real sorter page over the fake site (_engraveRealWorld.cjs; offline, made-up names), a back-only disc whose back reads "A / 08.15.23", in Design & Engrave, as he did it:
//   K1 Front pressed in the panel; the Charm Editor opens on the STAGED front words; "A" is typed as a text of its own and the staged words are curved to the disc; Save: the front keeps BOTH, the
//      curved words as drawn (they were the staged words, which a Save never kept: the curve was lost and only the A stayed)
//   K2 after that Save the card shows what was kept (the panel and the work area are the back again: its stage went with its draft), and the editor opened again is handed the back's own words,
//      never the staged front words a second time
//   K3 on the back the engraving's words are curved and an "A" typed; Save: the back keeps the curved words and the A as drawn, the engraving's words of the back are taken off (said on the card),
//      where they used to go back to the engraving straight (the curve lost); the work area draws the back with its grid and its art; the Back thumbnail shows it; no page error
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/ce-curve-keep.cjs
'use strict';
const R = require('./_engraveRealWorld.cjs');
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${evidence || ''}`.trimEnd()); };
const settleP = page => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));

(async () => {
  R.ORDERS.discA = Object.assign({}, R.ORDERS.discTwin, { rid: '4290000131', listing: '1890000131', buyer: 'Name Eleven', personalization: ['A\n08.15.23'] });
  R.EXPECT_JOBS.discA = 1;
  const w = await R.world({ shots: process.env.SHOTS || require('os').tmpdir() + '/ce-curve-keep-shots' });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  const page = w.page, st = w.srv.st, Q = '#egQueue .rvItem[data-kind=placement]', errs = [];
  page.on('pageerror', e => errs.push(e.message));
  const rec = (pid, face) => { const r = st.doc('Charm_Pool_Art', face === 'front' ? pid + '~front' : pid); return r ? r.added.map(a => ({ kind: a.kind, text: a.text, d: (a.d || '').length })) : []; };
  const card = () => page.evaluate(Q => {
    const c = document.querySelector(Q), h = c && c.querySelector('.backHost'), cv = h && h.querySelector(':scope > canvas'), j = c && Engrave.items().get(c.dataset.key), sn = c && Engrave.stageNow(c.dataset.key);
    const blue = x => { if (!x) return 0; const d = x.getContext('2d').getImageData(0, 0, x.width, x.height).data; let n = 0; for (let i = 0; i < d.length; i += 4) if (d[i + 2] > 150 && d[i + 2] - d[i] > 45 && d[i + 2] - d[i + 1] > 30) n++; return n; };
    return { stage: sn && { choice: sn.choice, active: sn.active }, panel: (c.querySelector('.engpSeg[aria-pressed=true]') || {}).textContent || '', shape: c._svShape, grid: !!(cv && cv._gridLabel), backArt: cv && cv._backArt ? cv._backArt.length : 0, blue: blue(cv),
      thumbBack: blue(c.querySelector('.engFace[data-face="back"] .engThumb canvas')), job: j && { text: j.text, state: j.state, artOnly: !!(j.fit && j.fit.artOnly) }, note: (c.querySelector('.egArtNote') || {}).textContent || '' };
  }, Q);
  const openEditor = async () => {
    await page.click(Q + ' .egCharmEdit');
    await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }); await settleP(page); await settleP(page);
    return page.evaluate(() => { const r = CharmEditor.current.result(), o = {}; for (const f of ['front', 'back']) o[f] = ((r.faces[f] || {}).items || []).filter(i => i.role !== 'cut').map(i => ({ id: i.id, source: i.source, text: i.text })); return o; });
  };
  const faceTo = async f => { if ((await page.evaluate(() => CharmEditor.current.face())) !== f) { await page.click(`.ce-root .ce-head [data-face="${f}"]`); await settleP(page); } };
  /** On the face shown: "A" typed above the engraving's words (as a text of its own) and the words curved to the disc (Fit to the disc). */
  const splitAndCurve = () => page.evaluate(() => {
    const E = CharmEditor.current, ctx = E.ctx, w = E.items().find(i => i.source === 'engrave'); if (!w) return null;
    const b = E.box(w.id);
    ctx.replaceItems({ [w.id]: Object.assign({}, w, { text: '08.15.23', ty: (+w.ty || 0) + 9 }) }); ctx.commit('words');   // (the date under the middle, as Paul had it)
    const ids = ctx.addItems([{ kind: 'text', role: 'hatch', text: 'A', fontKey: 'mynerve', sizePt: 10, align: 'center', tx: (b[0] + b[2]) / 2, ty: b[1] + (b[3] - b[1]) * 0.25 }]); ctx.commit('A');
    ctx.select([w.id]); CharmEditor.commands['cx.arcFit'].run(ctx);
    const now = E.items().find(i => i.text === '08.15.23'); return { words: w.id, now: now && { id: now.id, source: now.source, arcR: now.arcR } };
  });
  const save = async () => {
    await page.click('.ce-root [data-act="save"]');
    for (let t0 = Date.now(); Date.now() - t0 < 30000; ) { await new Promise(r => setTimeout(r, 300)); if (!(await page.evaluate(() => !!(window.CharmEditor && CharmEditor.current && !document.querySelector('.ce-root .ce-hold'))))) break; }
    await R.settle(page, 2500);
    await page.waitForFunction(() => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'), j = c && Engrave.items().get(c.dataset.key); return j && !['ready', 'fitting'].includes(j.state); }, null, { timeout: 120000 }).catch(() => {});
    await page.evaluate(() => { const b = [...document.querySelectorAll('button')].find(x => /^Done/.test((x.textContent || '').trim()) && x.offsetParent); if (b) b.click(); });
    await R.settle(page, 1500);
  };
  try {
    await page.waitForFunction(() => !window.CNRole || ['ask', 'role', 'admin'].includes(CNRole.state()), null, { timeout: 20000 }).catch(() => {});
    await page.evaluate(() => { try { if (window.CNRole && CNRole.state() === 'ask') CNRole.choose('design'); } catch (_) {} });
    const made = await R.build(w, ['discA']), PID = made.discA.poolIds[0];
    check('K0 the world: a back-only disc reading "A / 08.15.23", fitted', made.discA.jobs.length === 1 && made.discA.jobs[0].face === 'back' && made.discA.jobs[0].fitted, JSON.stringify(made.discA.jobs.map(j => [j.face, j.state, j.text])));
    await R.openEngraveDetail(w, 'discA', { face: 'back' }); await R.settle(page, 900);

    /* ── K1 · Front pressed, the editor on the staged front words ── */
    await page.click(Q + ' .engpSeg[data-choice="front"]'); await R.settle(page, 1200);
    const o1 = await openEditor(); await faceTo('front');
    const e1 = await splitAndCurve(); await settleP(page);
    await save();
    const f1 = rec(PID, 'front');
    check('K1 the staged front words curved in the editor are kept on the front as drawn, beside the "A" typed there (the staged words were never kept: the curve was lost)',
      o1.front.some(i => i.source === 'engrave') && e1 && e1.now && e1.now.source === 'added' && e1.now.arcR && f1.some(a => a.text === '08.15.23' && a.d > 0) && f1.some(a => a.text === 'A'), JSON.stringify({ handed: o1.front, e1, front: f1 }));
    const c1 = await card();
    check('K2a after the Save the card shows what was kept: the stage went with its draft (the back again, in the panel and the work area)', c1.stage && c1.stage.choice === 'back' && /Back/.test(c1.panel) && /^job\|back/.test(c1.shape || ''), JSON.stringify(c1));

    /* ── K2 · the editor again: the back's own words, never the staged front words a second time ── */
    const o2 = await openEditor();
    check('K2b the editor opened again is handed the back\'s words, not the staged front words again', o2.back.some(i => i.source === 'engrave' && /08\.15\.23/.test(i.text || '')) && !o2.front.some(i => i.source === 'engrave'), JSON.stringify(o2));

    /* ── K3 · the back's words curved and an "A" typed; Save ── */
    await faceTo('back');
    const e3 = await splitAndCurve(); await settleP(page);
    await save();
    await R.openEngraveDetail(w, 'discA', { face: 'back' }); await R.settle(page, 1500);
    const b3 = rec(PID, 'back'), c3 = await card();
    check('K3a the back\'s words curved are kept on the back as drawn, with the "A"; the engraving\'s words of the back are taken off, said on the card (they used to go back to the engraving straight)',
      e3 && e3.now && e3.now.source === 'added' && b3.some(a => a.text === '08.15.23' && a.d > 0) && b3.some(a => a.text === 'A') && c3.job && c3.job.text === '' && c3.job.artOnly && /words are taken off/.test(c3.note), JSON.stringify({ e3, back: b3, job: c3.job, note: c3.note }));
    check('K3b the work area draws the back with its grid and its art (the curved words and the A, in the back\'s blue); the Back thumbnail shows them', c3.grid && c3.backArt === 2 && c3.blue > 300 && c3.thumbBack > 100, JSON.stringify({ grid: c3.grid, backArt: c3.backArt, blue: c3.blue, thumbBack: c3.thumbBack }));
    check('K4 no page error', !errs.length, errs.join(' | '));
  } catch (e) { check('run', false, (e && e.stack) || String(e)); }
  finally {
    await w.close();
    const bad = RESULTS.filter(r => !r.ok);
    console.log(bad.length ? `FAIL ce-curve-keep (${bad.length} of ${RESULTS.length})` : `PASS ce-curve-keep (${RESULTS.length})`);
    process.exit(bad.length ? 1 : 0);
  }
})();
