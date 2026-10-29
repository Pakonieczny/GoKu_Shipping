// EDITTARGET (Paul, 29 Oct 2026: "When I click the "Edit Design" button on a given order the Charm Editor does not update to open the Charm/Order currently selected. This also happened when I
// was in the Sheet view and was clicking on different charms on the Sheet and then clicking "Edit Design" and had the same issue"; order 4211572095, a bird, opened "ZODIAC_CONSTE_GEM-7", a piece
// of order 4213879228). The real sorter page over the repo's fake site (_engraveRealWorld.cjs: the real intake, pool, classifier and fit; offline, made-up names, nothing real written), three
// engraved orders: A (a comet necklace), B (a cat necklace, front and back) and C (a necklace of three discs), all on one sheet:
//   E0  A's and C's pieces saved in the Charm Editor from their engraving cards: each piece's own design (pc-<poolId>) is in the design library under its own order
//   E1  B, with no design of its own, has no Edit Design; A's piece design kept here under B (a send whose order was typed in, a save with another order's design in the editor) does not give B
//       an "Edit Design" either, and is never what B opens
//   E2  B's own piece saved too: A's window → Edit Design opens A; the window moved to B → Edit Design opens B, never A
//   E3  the open still on its way (the master read and the fonts take seconds on the live site): Edit Design on A, the window moved to B at once, Edit Design on B: the editor comes up on B, and A
//       never comes up after it (it was handed A's open before)
//   E4  an editor already up on A with a change in it: B's Edit Design asks through the editor's own Cancel (Keep editing keeps A, nothing else opens); Discard changes, and B opens
//   E5  the Sheet tab: A's window on its sheet, a click on B's charm, Edit Design opens B; a click on A's charm, Edit Design opens A; a button left from an earlier paint (the window moved on
//       without painting it) opens the order the window shows at the press
//   E6  the charm picked on the sheet is the piece Edit Design opens: C's second disc picked → "piece 2 of 3" (its design is the first disc's), the first picked
//       again → "piece 1 of 3"
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/edit-design-target.cjs      (SHOTS=<dir> for the screenshots)
'use strict';
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : evidence || ''}`.trimEnd()); };
const R = require('./_engraveRealWorld.cjs');

(async () => {
  const shots = process.env.SHOTS || '/tmp/edit-design-target-shots';
  const w = await R.world({ viewport: { width: 1440, height: 900 }, shots });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  const page = w.page, st = w.srv.st;
  await page.waitForFunction(() => !window.CNRole || ['ask', 'role', 'admin'].includes(CNRole.state()), null, { timeout: 20000 }).catch(() => {});
  await page.evaluate(() => { try { if (window.CNRole && CNRole.state() === 'ask') CNRole.choose('design'); } catch (_) { /* no role step on this page */ } });   // (the "Laser or Design?" bar covers buttons)
  const settle = ms => R.settle(page, ms);
  const until = async (fn, ms) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) return null; await new Promise(r => setTimeout(r, 150)); } };
  /** The editor up now: its title, its subtitle (the order and the piece), or null. */
  const editor = () => page.evaluate(() => { const r = document.querySelector('.ce-root'); if (!r || !window.CharmEditor || !CharmEditor.current) return null; return { title: (r.querySelector('.ce-id h2') || {}).textContent || '', sub: (r.querySelector('.ce-eyebrow') || {}).textContent || '', n: CharmEditor.current.items().length }; });
  const editorUp = ms => until(async () => { const e = await editor(); return e && e.n ? e : null; }, ms || 60000);
  const closeEditor = async () => { if (!(await editor())) return; await page.click('.ce-root [data-act="cancel"]'); await until(() => page.evaluate(() => !document.querySelector('.ce-root') && !(window.CharmEditor && CharmEditor.current)), 15000); await settle(200); };
  const button = () => page.evaluate(() => { const b = document.getElementById('owCpDesign'); return { rid: OrderWin.isOpen() ? String(OrderWin.rid()) : null, shown: !!b && !b.hidden && !!b.offsetParent, text: b ? b.textContent.trim() : '' }; });
  const openWin = async (key, view) => {
    await page.evaluate(() => { CN.setMode('orders'); if (OrderWin.isOpen()) OrderWin.close(); });
    await until(() => page.evaluate(() => !document.querySelector('#orderWin[open]')), 5000);
    await page.evaluate(async ({ key, view }) => { await OrderWin.open(key, view ? { view } : undefined); }, { key, view });
    await until(async () => (await button()).shown, 5000); await settle(500);
  };
  const moveTo = async (rid, view) => { await page.evaluate(({ rid, view }) => OrderWin.openOrder(rid, { view: view || 'info', keepFrom: true }), { rid, view }); await until(async () => (await button()).rid === rid, 10000); await settle(500); };
  const press = () => page.click('#owCpDesign');
  try {
    const made = await R.build(w, ['back', 'both', 'discs']);
    const A = made.back, B = made.both, C = made.discs;
    // (the Sheet tab reads the sheets' records: the world's one sheet is put where the page reads it)
    const sheets = await page.evaluate(() => { const out = []; for (const s of allSheets()) if (s.charms && s.charms.length) { if (!s.folderPath) s.folderPath = 'charmnest/sheets/' + s.sheetId; out.push({ sheetId: s.sheetId, poolIds: s.charms.map(x => x.poolId) }); } return out; });
    for (const s of sheets) st.put('Charm_Nest_Sheets', s.sheetId, { sheetId: s.sheetId, poolIds: s.poolIds, backPool: [] });
    const onOne = sheets.some(s => [A, B, C].every(o => o.poolIds.every(p => s.poolIds.includes(p))));
    /** A piece saved in the Charm Editor from its engraving card (a dot added): the bridge keeps the piece's own design, pc-<poolId>, under its order. */
    const saveFromCard = async (name, slot) => {
      await R.openEngraveDetail(w, name, slot ? { slot } : {}); await settle(600);
      await page.$eval('#egQueue .rvItem[data-kind=placement] .egCharmEdit', b => b.click());
      await editorUp();
      await page.evaluate(() => { const E = CharmEditor.current, f = E.frame(), x = f.x0 + f.wPt * 0.5, y = f.y0 + f.hPt * 0.55; E.ctx.addItems([{ kind: 'path', role: 'hatch', name: 'Dot', fill: true, stroke: false, subpaths: [[['m', [x, y]], ['l', [x + 0.6, y]], ['l', [x + 0.3, y + 0.6]], ['h']]], tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 }], { select: false }); E.ctx.commit('dot'); });
      await page.click('.ce-root [data-act="save"]');
    };
    const designed = rid => until(() => page.evaluate(rid => !document.querySelector('.ce-root') && CharmNestDesigns.rows().some(r => /^pc-/.test(r.id) && r.orderId === rid), rid), 90000);

    /* ── E0 ── */
    await saveFromCard('back'); const dA = await designed(A.rid);
    await saveFromCard('discs', 'D1'); const dC = await designed(C.rid);
    const lib = await page.evaluate(() => CharmNestDesigns.rows().map(r => [r.id, r.orderId]));
    check(`E0 the world: three orders on one sheet (${onOne}); A's and C's pieces saved from their engraving cards, each piece's design under its own order (${lib.map(x => x.join('→')).join(', ')})`,
      onOne && !!dA && !!dC && lib.some(([id, o]) => id === 'pc-' + A.poolIds[0] && o === A.rid) && lib.some(([id, o]) => id === 'pc-' + C.poolIds[0] && o === C.rid) && C.poolIds.length === 3, JSON.stringify({ sheets, lib, C: C.poolIds }));

    /* ── E1 · the button's words are this order's designs only ── */
    await openWin(B.key); const b1 = await button();
    // (A's piece design kept here under B: what a send whose order was typed in, or a save with another order's design in the editor, left in the page)
    await page.evaluate(({ rid, pid }) => { const T = CharmNestCustomPiece._t, d = { id: 'pc-' + pid, version: 1, name: 'COMET_9427', widthMm: 10, heightMm: 10, source: { kind: 'piece', poolId: pid }, at: Date.now() }; T.attached.set(rid, new Map([[d.id, Object.assign({ sends: [] }, d)]])); OrderWin.paint(); }, { rid: B.rid, pid: A.poolIds[0] });
    await settle(400);
    const b1b = await button(), att1 = await page.evaluate(rid => CharmNestCustomPiece.attachedOf(rid).map(a => a.id), B.rid);
    check(`E1 B with no design of its own: no Edit Design (${b1.shown ? '"' + b1.text + '"' : 'none'}); A's piece design kept here under B: still none (${b1b.shown ? '"' + b1b.text + '"' : 'none'}), and B's designs are [${att1.join(', ')}]`,
      b1.rid === B.rid && !b1.shown && !b1b.shown && att1.length === 0, JSON.stringify({ b1, b1b, att1 }));
    await page.evaluate(rid => CharmNestCustomPiece._t.attached.delete(rid), B.rid);
    await page.evaluate(() => { try { OrderWin.close(); } catch (_) {} }); await settle(300);
    await saveFromCard('both'); const dB = await designed(B.rid);

    /* ── E2 · A, then the window moved to B ── */
    await openWin(A.key); const b2a = await button();
    await press(); const e2a = await editorUp(); await R.shot(w, 'ET-E2-A');
    await closeEditor();
    await moveTo(B.rid); const b2b = await button();
    await press(); const e2b = await editorUp(); await R.shot(w, 'ET-E2-B');
    await closeEditor();
    check(`E2 A's window: "${b2a.text}" opens "${e2a && e2a.title}" (${e2a && e2a.sub}); the window moved to B: "${b2b.text}" opens "${e2b && e2b.title}" (${e2b && e2b.sub})`,
      !!dB && b2a.shown && b2a.text === 'Edit Design' && !!e2a && e2a.title === 'COMET_9427' && e2a.sub.startsWith(`order ${A.rid} ·`) && b2b.rid === B.rid && b2b.shown && b2b.text === 'Edit Design' && !!e2b && e2b.title === 'SCHRODINGER CAT 2' && e2b.sub.startsWith(`order ${B.rid} ·`),
      JSON.stringify({ b2a, e2a, b2b, e2b }));

    /* ── E3 · the open still on its way when the window moves on ── */
    await page.evaluate(() => { const AE = CharmNestArtEdit; window.__sync = AE.sync; AE.sync = function () { return new Promise(r => setTimeout(r, 2500)).then(() => window.__sync.apply(AE, arguments)); }; });   // (the live site's read: seconds)
    await openWin(A.key);
    await press(); await settle(100);
    const early = await editor();
    await moveTo(B.rid);
    await press();
    const e3 = await editorUp(); await R.shot(w, 'ET-E3-B-not-A');
    await settle(3500);
    const e3late = await editor(), n3 = await page.evaluate(() => document.querySelectorAll('.ce-root').length);
    await page.evaluate(() => { CharmNestArtEdit.sync = window.__sync; });
    await closeEditor();
    check(`E3 Edit Design on A while its open was still on its way (${early ? 'already up' : 'not up yet'}), the window moved to B, Edit Design on B: the editor comes up on "${e3 && e3.title}" (${e3 && e3.sub}) and 3.5 s later is still B's ("${e3late && e3late.sub}", ${n3} editor), never A's`,
      !early && !!e3 && e3.title === 'SCHRODINGER CAT 2' && e3.sub.startsWith(`order ${B.rid} ·`) && !!e3late && e3late.sub === e3.sub && n3 === 1, JSON.stringify({ early, e3, e3late, n3 }));

    /* ── E4 · an editor already up on A, with a change in it ── */
    await openWin(A.key); await press(); const e4a = await editorUp();
    await page.evaluate(() => { const E = CharmEditor.current, f = E.frame(), x = f.x0 + f.wPt * 0.4, y = f.y0 + f.hPt * 0.4; E.ctx.addItems([{ kind: 'path', role: 'hatch', name: 'Unsaved', fill: true, stroke: false, subpaths: [[['m', [x, y]], ['l', [x + 0.6, y]], ['l', [x + 0.3, y + 0.6]], ['h']]], tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 }], { select: false }); E.ctx.commit('unsaved'); });
    // (the editor covers the window: B's Edit Design is asked as the window's button asks it, for B's line)
    const askB = () => page.evaluate(key => { window.__pB = CharmNestCustomPiece.openDesigner({ rows: [B.orders.byKey.get(key)], row: B.orders.byKey.get(key) }); }, B.key);
    await askB();
    const asked = await until(() => page.evaluate(() => { const a = document.querySelector('.ce-ask'); return a ? [...a.querySelectorAll('[data-ask]')].map(b => b.dataset.ask + ':' + b.textContent.trim()) : null; }), 10000);
    await R.shot(w, 'ET-E4-asks');
    await page.click('.ce-ask [data-ask="no"]'); await settle(600);
    const kept = await editor(), gaveWay = await page.evaluate(() => Promise.race([window.__pB.then(v => v === null), new Promise(r => setTimeout(() => r('pending'), 3000))]));
    await askB();
    await until(() => page.evaluate(() => !!document.querySelector('.ce-ask')), 10000);
    await page.click('.ce-ask [data-ask="yes"]');
    const e4b = await until(async () => { const e = await editor(); return e && e.n && e.sub.includes(B.rid) ? e : null; }, 60000);
    await R.shot(w, 'ET-E4-B');
    await closeEditor();
    check(`E4 the editor up on A ("${e4a && e4a.sub}") with a change: B's Edit Design asks through the editor's own Cancel (${(asked || []).join(' / ')}); Keep editing keeps A ("${kept && kept.sub}") and opens nothing (${gaveWay}); Discard changes, and B opens ("${e4b && e4b.sub}")`,
      !!e4a && e4a.sub.includes(A.rid) && !!asked && asked.some(a => /^yes:/.test(a)) && asked.some(a => /^no:/.test(a)) && !!kept && kept.sub.includes(A.rid) && gaveWay === true && !!e4b && e4b.title === 'SCHRODINGER CAT 2', JSON.stringify({ e4a, asked, kept, gaveWay, e4b }));

    /* ── E5 · the Sheet tab ── */
    await openWin(A.key, 'sheet');
    await until(() => page.evaluate(() => { const i = OrderWin._sheet(); return !!(i && i.sheet && i.pointOf); }), 30000); await settle(600);
    const clickCharm = async (pid, rid) => { await until(() => page.evaluate(() => { const i = OrderWin._sheet(); return !!(i && i.pointOf); }), 15000); const pt = await page.evaluate(p => OrderWin._sheet().pointOf(p), pid); await page.mouse.click(pt.x, pt.y); await until(async () => (await button()).rid === rid, 10000); await settle(800); };
    await clickCharm(B.poolIds[0], B.rid); const b5b = await button();
    await press(); const e5b = await editorUp(); await closeEditor();
    await clickCharm(A.poolIds[0], A.rid); const b5a = await button();
    await press(); const e5a = await editorUp(); await closeEditor();
    // a button left from an earlier paint: the window moves to B without painting it (as when its paint stopped short), and the press opens what the window shows
    await page.evaluate(() => { const C = CharmNestCustomPiece; window.__ow = C.orderWin; C.orderWin = () => {}; });
    await clickCharm(B.poolIds[0], B.rid);
    await page.evaluate(() => { CharmNestCustomPiece.orderWin = window.__ow; });
    const stale = await button();
    await press(); const e5s = await editorUp(); await R.shot(w, 'ET-E5-stale-button'); await closeEditor();
    check(`E5 the Sheet tab: B's charm clicked, "${b5b.text}" opens "${e5b && e5b.sub}"; A's charm clicked, "${b5a.text}" opens "${e5a && e5a.sub}"; a button left from A's paint on B's window ("${stale.text}") opens "${e5s && e5s.sub}"`,
      b5b.rid === B.rid && b5b.shown && !!e5b && e5b.sub.startsWith(`order ${B.rid} ·`) && b5a.rid === A.rid && b5a.shown && !!e5a && e5a.sub.startsWith(`order ${A.rid} ·`) && stale.rid === B.rid && !!e5s && e5s.sub.startsWith(`order ${B.rid} ·`),
      JSON.stringify({ b5b, e5b, b5a, e5a, stale, e5s }));

    /* ── E6 · the charm picked on the sheet is the piece opened ── */
    await clickCharm(C.poolIds[1], C.rid);
    const f6 = await page.evaluate(() => OrderWin.where().focus);
    await press(); const e6a = await editorUp(); await closeEditor();
    // (back to its first disc, the one whose design is kept: the press follows the pick back)
    await clickCharm(C.poolIds[0], C.rid); await settle(300);
    const f6b = await page.evaluate(() => OrderWin.where().focus);
    await press(); const e6b = await editorUp(); await R.shot(w, 'ET-E6-first-disc'); await closeEditor();
    check(`E6 C's second disc picked on the sheet (${f6}): Edit Design opens "${e6a && e6a.sub}" (its design is the first disc's); the first picked again (${f6b}): "${e6b && e6b.sub}"`,
      f6 === C.poolIds[1] && !!e6a && e6a.sub.startsWith(`order ${C.rid} · piece 2 of 3`) && f6b === C.poolIds[0] && !!e6b && e6b.sub.startsWith(`order ${C.rid} · piece 1 of 3`), JSON.stringify({ f6, e6a, f6b, e6b }));

    check('E7 no page error', w.errors.length === 0, JSON.stringify(w.errors).slice(0, 400));
  } catch (e) { check('the run', false, String(e && e.stack || e).slice(0, 600)); }
  finally { await w.close(); }
  const bad = RESULTS.filter(r => !r.ok);
  console.log(`\n${RESULTS.length - bad.length}/${RESULTS.length} passed${bad.length ? ' · FAILED: ' + bad.map(r => r.id.split(' ')[0]).join(', ') : ''}`);
  console.log('screenshots in ' + shots);
  process.exit(bad.length ? 1 : 0);
})();
