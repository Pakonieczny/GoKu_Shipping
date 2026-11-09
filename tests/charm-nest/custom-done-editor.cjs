// CPX1 (Paul, 9 Nov 2026, order 4225890898): "Once a user completes an order using the editor inside the pop-up modal there needs to be an option that also completes the Custom portion of a
// given order ... This needs to be clearly visible and not hidden somewhere in that modal ... it will release any custom order from being held for review."
//   A  (node, the page faked) charm-nest-custom-done.js: the open Custom portions of an order (an Etsy custom line held in Review, a custom piece on a sheet; the saved piece's own line never),
//      ticked at first only for exactly one; an Etsy line is completed by its own Release Hold (CustomPrint.complete) and reopened by Undo; a piece on a cut / approved / sent sheet is
//      refused with the reason and NOTHING changes; nothing ticked, nothing done
//   B  the real sorter page over the repo's fake site (_engraveRealWorld.cjs: the real intake, pool, fit, the real charmNestLibrary over an in-memory Firestore; offline, made-up names):
//      B1 an order with no Custom piece: the card after Save has no Custom block
//      B2 the same order with a custom piece on its GF sheet: the card after Save shows "Also complete the Custom piece of order #…" FIRST (above its title), ticked, with the piece's name,
//         size and picture; the Send to sheet pop-up does not cover it; Done says what it does
//      B3 unticked, Done: nothing changes on the Custom piece (still on its sheet, not covered, no take-off written)
//      B4 ticked, Done: the piece is off its sheet by the take-off (poolUpdate: abandoned, removedReason, coveredBy the saved piece), CustomOrders lists it "covered", the order window's row
//         says "Completed in the Charm Editor · on <piece>" with the CUSTOM DONE IN EDITOR seal and no dots, the order no longer waits on it; a toast with Undo
//      B5 Undo: the piece is live again (coveredBy undone) and back on an open sheet of its metal
//      B6 a second custom piece: two ticks, none ticked at first
//      B7 the piece's sheet cut: the tick is off and says why; complete() refuses with the reason and writes nothing
//      B8 the Words decision: a back asking "Enter the requested inscription", words typed on it in the Charm Editor and saved: the question is answered (the job waits for Approve as drawn,
//         no Words card), and the order window's head card no longer says "Needs a decision"
//      B9 no page error
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/custom-done-editor.cjs      (CD_ONLY=A or B; SHOTS=<dir> for the screenshots)
'use strict';
const path = require('node:path');
const ROOT = path.join(__dirname, '../..');
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}${ok ? '' : '  ' + (evidence || '')}`); };
const ONLY = process.env.CD_ONLY || '';

/* ═══ A · the module, the page faked ═══ */
async function partA() {
  const g = globalThis;
  const calls = { update: [], takeOff: [], complete: [], reopen: [], putBack: [], noted: [] };
  const RID = '4225890898';
  const sheet = { sheetId: 'gf-draft-11', name: 'GF Draft 11', metal: 'gold', status: 'complete', charms: [{ id: 'c1', poolId: `${RID}_202611090001_1` }, { id: 'c0', poolId: `${RID}_5282523907_1` }], placements: [{ id: 'c1' }, { id: 'c0' }] };
  const fresh = () => {
    sheet.laserDoneAt = 0; sheet.charms = [{ id: 'c1', poolId: `${RID}_202611090001_1` }, { id: 'c0', poolId: `${RID}_5282523907_1` }];
    for (const k of Object.keys(calls)) calls[k] = [];
    g.B = { pool: { rows: new Map([[`${RID}_202611090001_1`, { poolId: `${RID}_202611090001_1`, custom: true, state: 'nested', sheetId: 'gf-draft-11', updatedAt: 1 }]]) } };
  };
  const listing = { key: `${RID}_5282523907`, order: { receiptId: RID }, line: { title: 'Dog portrait' }, spec: { designSku: 'DOG 9667' }, poolIds: [`${RID}_5282523907_1`], state: 'nested' };
  const customLine = { key: `${RID}_5282523908`, order: { receiptId: RID }, line: { title: 'Custom' }, spec: { special: { group: 'custom', label: 'Custom portrait' } }, poolIds: [], state: 'pulled' };
  let customRows = [], orderRows = [listing];
  g.__cdSheets = () => [sheet];
  g.__cdToast = () => null;
  g.Orders = { rows: () => orderRows, takeOffGone: async (rows, opt) => { calls.takeOff.push({ rows, opt }); const ids = new Set(rows[0].poolIds), mine = sheet.charms.filter(c => ids.has(c.poolId)); if (opt.before) opt.before(sheet, mine); sheet.charms = sheet.charms.filter(c => !ids.has(c.poolId)); await g.Pool.update(mine.map(c => c.poolId), Object.assign({ state: 'abandoned', sheetId: null, setId: null }, opt.patch)); return [{ sh: sheet, poolIds: mine.map(c => c.poolId) }]; } };
  g.Pool = { update: async (ids, patch) => { calls.update.push({ ids, patch }); return { ok: true }; } };
  g.Review = { customItemFor: (key, r) => (r === customLine ? { key: 'cu:' + key, kind: 'customOrder', rows: [r], done: false } : null) };
  g.CharmNestCustomPiece = { needsDesign: r => !!(r.spec && r.spec.special) };
  g.CustomPrint = { complete: async it => { calls.complete.push(it.key); return true; }, reopen: it => { calls.reopen.push(it.key); } };
  g.CharmNestCustomPool = { putBack: (list, metal, rid) => { calls.putBack.push({ n: list.length, metal, rid }); return 'GF sheet 12'; } };
  g.CustomOrders = { rowsOf: () => customRows, rows: () => customRows, note: rows => { calls.noted.push(rows); for (const p of rows) { const r = customRows.find(x => x.poolIds.includes(p.poolId)); if (r) { r.spec.custom.coveredBy = p.coveredBy && !p.coveredBy.undoneAt ? p.coveredBy : null; r.state = r.spec.custom.coveredBy ? 'covered' : 'pooled'; } } } };
  const piece = () => ({ key: `${RID}_202611090001`, custom: true, poolIds: [`${RID}_202611090001_1`], material: 'gold', state: 'nested', spec: { custom: { name: '', designId: `pc-${RID}_5282523907_1`, version: 3, widthMm: 16, heightMm: 16, by: 'Paul_K' } } });
  const CD = require(path.join(ROOT, 'charm-nest-custom-done.js'));
  const cover = { poolId: `${RID}_5282523907_1`, lineKey: listing.key, name: 'DOG 9667', designId: `pc-${RID}_5282523907_1`, version: 4 };

  // A1 · the order's open Custom portions
  fresh(); customRows = [piece()]; orderRows = [listing];
  const one = CD.openOf(RID, { savedKey: listing.key, savedPoolIds: listing.poolIds });
  check('A1 an order with a listing piece and ONE custom piece on GF Draft 11: one Custom portion (16.0 × 16.0 mm, on GF Draft 11), ticked at first; the saved piece itself is never offered',
    one.length === 1 && one[0].kind === 'piece' && one[0].wMm === 16 && one[0].sheets[0].name === 'GF Draft 11' && !one[0].why && CD.ticked(one, { sameOrder: true }) && !CD.ticked(one, { sameOrder: false })
      && /Also complete the Custom piece of order #4225890898/.test(CD.blockHtml(one, { rid: RID, ticked: true })) && / checked/.test(CD.blockHtml(one, { rid: RID, ticked: true })), JSON.stringify(one.map(i => [i.kind, i.wMm, i.sheets, i.why])));
  // A2 · none / two
  customRows = []; const none = CD.openOf(RID, { savedKey: listing.key });
  customRows = [piece(), Object.assign(piece(), { key: `${RID}_202611090002`, poolIds: [`${RID}_202611090002_1`] })];
  const two = CD.openOf(RID, { savedKey: listing.key }), twoHtml = CD.blockHtml(two, { rid: RID, ticked: CD.ticked(two, { sameOrder: true }) });
  check('A2 an order with no Custom piece: nothing offered; with two: two ticks, none ticked at first', none.length === 0 && two.length === 2 && (twoHtml.match(/data-cd-tick=/g) || []).length === 2 && !/ checked/.test(twoHtml), `${none.length} / ${two.length}`);
  // A3 · nothing ticked: nothing done
  fresh(); customRows = [piece()];
  const nothing = await CD.complete({ rid: RID, items: [], cover, by: 'Paul_K' });
  check('A3 nothing ticked: nothing is done, nothing written', !nothing.ok && !calls.update.length && !calls.takeOff.length && sheet.charms.length === 2, JSON.stringify(nothing));
  // A4 · a cut sheet: refused, nothing changes
  fresh(); customRows = [piece()]; sheet.laserDoneAt = Date.now();
  const cutItems = CD.openOf(RID, { savedKey: listing.key }), refused = await CD.complete({ rid: RID, items: [Object.assign({}, cutItems[0], { why: '' })], cover, by: 'Paul_K' });
  check(`A4 the piece's sheet already cut: the tick is off ("${cutItems[0] && cutItems[0].why}"), and complete() refuses ("${refused.why}") with nothing written`,
    /already cut/.test(cutItems[0].why) && !refused.ok && /already cut/.test(refused.why) && /Nothing was changed/.test(refused.why) && !calls.update.length && !calls.takeOff.length && sheet.charms.length === 2, JSON.stringify(refused));
  sheet.laserDoneAt = 0; sheet.releasedAt = Date.now();
  const appr = CD.openOf(RID, { savedKey: listing.key });
  check(`A4b an approved sheet is refused too ("${appr[0] && appr[0].why}")`, /approved for laser cutting/.test(appr[0].why)); delete sheet.releasedAt;
  // A5 · completed: off its sheet by the take-off, covered; Undo
  fresh(); customRows = [piece()];
  const items = CD.openOf(RID, { savedKey: listing.key }), r = await CD.complete({ rid: RID, items, cover, by: 'Paul_K' });
  const up = calls.update[0] || {}, p = up.patch || {};
  check('A5 confirmed: the piece comes off GF Draft 11 by the take-off (Orders.takeOffGone), its record abandoned with removedBy / removedReason and coveredBy the saved piece (DOG 9667 v4, who, when); the views told',
    r.ok && calls.takeOff.length === 1 && sheet.charms.length === 1 && p.state === 'abandoned' && p.sheetId === null && p.removedBy === 'Paul_K' && p.removedReason === 'completed in the Charm Editor' && p.coveredBy && p.coveredBy.poolId === cover.poolId && p.coveredBy.name === 'DOG 9667' && p.coveredBy.version === 4 && p.coveredBy.by === 'Paul_K' && p.coveredBy.at > 0
      && customRows[0].state === 'covered', JSON.stringify({ r: r.ok && r.why, p, st: customRows[0].state }));
  check('A5b the covered piece is no longer offered, and its row says what covered it', CD.openOf(RID, { savedKey: listing.key }).length === 0 && /Completed in the Charm Editor · on <b>DOG 9667<\/b>/.test(CD.rowHtml(customRows[0].spec.custom.coveredBy)));
  const u = await CD.undo(r.token), last = calls.update[calls.update.length - 1];
  check('A6 Undo: the piece is live again (state ready, coveredBy undone), put back on an open sheet of its metal (CharmNestCustomPool.putBack, never a cut one)',
    u.ok && last.patch.state === 'ready' && last.patch.coveredBy.undoneAt > 0 && calls.putBack.length === 1 && calls.putBack[0].metal === 'gold' && customRows[0].state === 'pooled' && (await CD.undo(r.token)).ok === false, JSON.stringify({ u, last, putBack: calls.putBack }));
  // A7 · an Etsy custom line held in Review: its own Release Hold; Undo reopens it
  fresh(); customRows = []; orderRows = [listing, customLine];
  const li = CD.openOf(RID, { savedKey: listing.key }), lr = await CD.complete({ rid: RID, items: li, cover, by: 'Paul_K' });
  check('A7 an Etsy custom line held in Review: offered and ticked; confirmed, it is released by its own Release Hold (CustomPrint.complete: its record, its seal, Resolved) and nothing else is touched',
    li.length === 1 && li[0].kind === 'line' && CD.ticked(li, { sameOrder: true }) && lr.ok && calls.complete.length === 1 && calls.complete[0] === 'cu:' + customLine.key && !calls.update.length, JSON.stringify({ li: li.map(x => x.kind), lr: lr.ok, calls }));
  await CD.undo(lr.token);
  check('A7b Undo reopens that custom line (CustomPrint.reopen): its hold is back', calls.reopen.length === 1);
  for (const k of ['B', '__cdSheets', '__cdToast', 'Orders', 'Pool', 'Review', 'CharmNestCustomPiece', 'CustomPrint', 'CharmNestCustomPool', 'CustomOrders']) delete g[k];
}

/* ═══ B · the real page ═══ */
async function partB() {
  const R = require('./_engraveRealWorld.cjs');
  const shots = process.env.SHOTS || '/tmp/custom-done-editor-shots';
  const w = await R.world({ viewport: { width: 1440, height: 900 }, shots, keepCards: true });
  if (!w) { console.log('SKIP B: no playwright-core'); return; }
  const page = w.page, st = w.srv.st;
  await page.waitForFunction(() => !window.CNRole || ['ask', 'role', 'admin'].includes(CNRole.state()), null, { timeout: 20000 }).catch(() => {});
  await page.evaluate(() => { try { if (window.CNRole && CNRole.state() === 'ask') CNRole.choose('design'); } catch (_) { /* no role step */ } });
  const settle = ms => R.settle(page, ms);
  const until = async (fn, ms) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) return null; await new Promise(r => setTimeout(r, 150)); } };
  const editorUp = ms => until(() => page.evaluate(() => !!(window.CharmEditor && CharmEditor.current && document.querySelector('.ce-root'))), ms || 60000);
  const dot = () => page.evaluate(() => { const E = CharmEditor.current, f = E.frame(), x = f.x0 + f.wPt * (0.47 + Math.random() * 0.06), y = f.y0 + f.hPt * (0.53 + Math.random() * 0.04); E.ctx.addItems([{ kind: 'path', role: 'hatch', name: 'Dot', fill: true, stroke: false, subpaths: [[['m', [x, y]], ['l', [x + 0.6, y]], ['l', [x + 0.6, y + 0.6]], ['l', [x, y + 0.6]], ['h']]] }]); });
  const openWin = async key => {
    await page.evaluate(() => { CN.setMode('orders'); if (OrderWin.isOpen()) OrderWin.close(); });
    await until(() => page.evaluate(() => !document.querySelector('#orderWin[open]')), 5000);
    await page.evaluate(async key => { await OrderWin.open(key); }, key);
    await until(() => page.evaluate(() => { const b = document.getElementById('owCpDesign'); return !!b && !b.hidden && !!b.offsetParent; }), 8000); await settle(400);
  };
  /** Edit Design from the order window, a change, Save: the card after it, as drawn. */
  const saveFromWin = async key => {
    await openWin(key);
    await page.click('#owCpDesign'); await editorUp(); await dot();
    await page.click('.ce-root [data-act="save"]');
    return until(() => page.evaluate(() => {
      const c = document.querySelector('.cem-card'); if (!c || !c.querySelector('[data-c]')) return null;
      const blk = c.querySelector('[data-cd-block]'), first = c.firstElementChild;
      return { h: c.querySelector('h3').textContent, b: [...c.querySelectorAll('[data-c]')].map(b => b.dataset.c + ':' + b.textContent.trim()), block: blk ? { title: blk.querySelector('h4').textContent, text: blk.textContent, ticks: [...blk.querySelectorAll('[data-cd-tick]')].map(t => ({ on: t.checked, off: t.disabled })), pic: !!blk.querySelector('[data-cd-pic] svg, [data-cd-pic] img'), top: !!first && first.contains(blk) } : null };
    }), 60000);
  };
  const press = async c => { await page.evaluate(c => document.querySelector(`.cem-card [data-c="${c}"]`).click(), c); await settle(300); };
  const popupOver = () => page.evaluate(() => !!document.querySelector('dialog.cpDlg[data-cp-after][open]'));
  const closePopup = () => page.evaluate(() => { const b = document.querySelector('dialog.cpDlg[data-cp-after] [data-cp-close]'); if (b) b.click(); });
  const takeOffs = pid => st.calls.filter(c => c.name === 'charmNestLibrary' && c.op === 'poolUpdate' && c.body && (c.body.poolIds || []).includes(pid));
  try {
    const made = await R.build(w, ['back', 'both']);
    const A = made.back, Bo = made.both;

    /* ── B1 · no Custom piece: no block ── */
    const c1 = await saveFromWin(Bo.key);
    check(`B1 an order with no Custom piece: the card after Save ("${c1 && c1.h}") has no Custom block`, !!c1 && !c1.block, JSON.stringify(c1));
    await settle(500); if (await popupOver()) await closePopup(); await settle(200); await press('done');
    await until(() => page.evaluate(() => !document.querySelector('.ce-root')), 8000);

    /* ── a custom piece sent for order A, onto its GF sheet ── */
    const MMp = 72 / 25.4, W = 16 * MMp;
    const sq = [[['m', [0, 0]], ['l', [W, 0]], ['l', [W, W]], ['l', [0, W]], ['h']]], mark = [[['m', [10, 10]], ['l', [20, 10]], ['l', [20, 20]], ['l', [10, 20]], ['h']]];
    const design0 = { name: 'Cat portrait', source: { kind: 'new' }, widthMm: 16, heightMm: 16, faces: { front: { face: 'front', unitPt: true, frame: { x0: 0, y0: 0, wPt: W, hPt: W }, items: [{ id: 'out', kind: 'path', role: 'cut', subpaths: sq, stroke: [0, 0, 0] }, { id: 'm', kind: 'path', role: 'hatch', subpaths: mark, fill: [0, 0, 1] }] } } };
    const sendPiece = name => page.evaluate(async ([d, rid, name]) => { const s = await CharmNestDesigns.put(Object.assign({}, d, { name })); const got = await CharmNestDesigns.get(s.id, s.version); return Pool.addCustomPiece({ design: got, version: s.version, hash: s.hash, metal: 'gold', orderId: rid, customName: '', quantity: 1, by: 'Paul_K' }); }, [design0, A.rid, name]);
    const placedOk = pid => page.evaluate(pid => { const sh = allSheets().find(s => (s.charms || []).some(c => c.poolId === pid)); return !!sh && !['nesting', 'finishing', 'queued'].includes(sh.status) && (sh.placements || []).some(p => sh.charms.find(c => c.id === p.id && c.poolId === pid)); }, pid);
    const ids1 = await sendPiece('Cat portrait'), pid1 = ids1[0];
    await until(() => placedOk(pid1), 120000);

    /* ── B2 · the block, first, ticked; no pop-up over it ── */
    const c2 = await saveFromWin(A.key);
    await settle(900); const pop2 = await popupOver();
    await R.shot(w, 'CPX1-B2-card');
    check(`B2 order ${A.rid} with a custom piece on its GF sheet: the card after Save shows "${c2 && c2.block && c2.block.title}" FIRST (${c2 && c2.block && c2.block.top}), ticked (${c2 && c2.block && JSON.stringify(c2.block.ticks)}), its size and picture (${c2 && c2.block && c2.block.pic}); the Send to sheet pop-up does not cover it (${!pop2}); Done says "${c2 && (c2.b.find(x => /^done:/.test(x)) || '')}"`,
      !!c2 && !!c2.block && c2.block.title === `Also complete the Custom piece of order #${A.rid}` && c2.block.top && c2.block.ticks.length === 1 && c2.block.ticks[0].on && /16\.0 × 16\.0 mm/.test(c2.block.text) && /taken off its sheet, and the order is released from Review/.test(c2.block.text) && c2.block.pic && !pop2
        && c2.b.includes('done:Done · complete Custom'), JSON.stringify({ c2, pop2 }));
    /* ── B3 · unticked: nothing changes ── */
    const t3 = takeOffs(pid1).length;
    await page.evaluate(() => { const t = document.querySelector('[data-cd-tick]'); t.click(); });
    const doneWord = await page.evaluate(() => document.querySelector('.cem-card [data-c="done"]').textContent);
    await press('done'); await settle(1200);
    const s3 = await page.evaluate(pid => ({ on: allSheets().some(s => (s.charms || []).some(c => c.poolId === pid)), cov: !!CharmNestCustomDone.coverOf({ poolId: pid }), state: (CustomOrders.rows().find(r => r.poolIds.includes(pid)) || {}).state }), pid1);
    check(`B3 unticked (Done reads "${doneWord}"): nothing changes on the Custom piece (still on its sheet ${s3.on}, not covered, ${takeOffs(pid1).length - t3} writes, listed "${s3.state}")`, doneWord === 'Done' && s3.on && !s3.cov && takeOffs(pid1).length === t3 && s3.state !== 'covered', JSON.stringify(s3));
    await until(() => page.evaluate(() => !document.querySelector('.ce-root')), 8000);

    /* ── B4 · ticked, Done ── */
    const c4 = await saveFromWin(A.key); await settle(600);
    const t4 = takeOffs(pid1).length;
    await press('done');
    const off4 = await until(() => page.evaluate(pid => !allSheets().some(s => (s.charms || []).some(c => c.poolId === pid)) && (CustomOrders.rows().find(r => r.poolIds.includes(pid)) || {}).state === 'covered', pid1), 20000);
    await settle(800);
    const w4 = takeOffs(pid1).slice(t4).map(c => c.body.patch), p4 = w4.find(p => p && p.coveredBy) || {};
    await openWin(A.key); await settle(800);
    const row4 = await page.evaluate(pid => { const box = document.getElementById('owPcSum'), r = box && [...box.querySelectorAll('.owPcRow')].find(x => /Completed in the Charm Editor/.test(x.textContent)); const head = box && box.querySelector('.owPcHd'); return { text: r ? r.textContent.replace(/\s+/g, ' ').trim() : '', seal: !!(r && r.querySelector('.seal svg[data-seal-model]')), sealWords: r && r.querySelector('.seal') ? r.querySelector('.seal').getAttribute('aria-label') : '', dots: r ? r.querySelectorAll('.pdot,[data-pdot]').length : -1, slow: r ? r.classList.contains('slow') : null, rows: box ? box.querySelectorAll('.owPcRow').length : 0, head: head ? head.textContent : '' }; }, pid1);
    const toast4 = await page.evaluate(() => { const t = [...document.querySelectorAll('#toasts .toast')].find(x => /completed in the Charm Editor/.test(x.textContent)); return t ? { text: t.textContent, undo: !!t.querySelector('button.btn') } : null; });
    await R.shot(w, 'CPX1-B4-window');
    check(`B4 ticked, Done: the piece is off its sheet and listed covered (${!!off4}); the take-off written (${JSON.stringify({ state: p4.state, sheetId: p4.sheetId, why: p4.removedReason, by: p4.coveredBy && p4.coveredBy.name })}); the order window says "${row4.text.slice(0, 90)}" with the seal (${row4.sealWords}), no dots (${row4.dots}), not the slowest (${row4.slow === false}); toast "${toast4 && toast4.text.slice(0, 80)}" with Undo (${toast4 && toast4.undo})`,
      !!c4 && !!c4.block && !!off4 && p4.state === 'abandoned' && p4.sheetId === null && p4.removedReason === 'completed in the Charm Editor' && !!p4.coveredBy && p4.coveredBy.poolId === A.poolIds[0] && p4.coveredBy.by
        && /Completed in the Charm Editor · on/.test(row4.text) && row4.seal && /completed in the Charm Editor/i.test(row4.sealWords) && row4.dots === 0 && row4.slow === false && !!toast4 && toast4.undo, JSON.stringify({ p4, row4, toast4 }));

    /* ── B5 · Undo ── */
    await page.evaluate(() => { const t = [...document.querySelectorAll('#toasts .toast')].find(x => /completed in the Charm Editor/.test(x.textContent)); t.querySelector('button.btn').click(); });
    const back5 = await until(() => page.evaluate(pid => { const sh = allSheets().find(s => (s.charms || []).some(c => c.poolId === pid)); const r = CustomOrders.rows().find(x => x.poolIds.includes(pid)); return sh && r && r.state !== 'covered' ? { sheet: sh.sheetId || sh.page, cut: !!sh.laserDoneAt, state: r.state } : null; }, pid1), 20000);
    const u5 = takeOffs(pid1).map(c => c.body.patch).pop() || {};
    check(`B5 Undo: the piece is live again (${u5.state}, coveredBy undone ${!!(u5.coveredBy && u5.coveredBy.undoneAt)}) and back on an open sheet of its metal (${JSON.stringify(back5)})`, !!back5 && !back5.cut && u5.state === 'ready' && !!(u5.coveredBy && u5.coveredBy.undoneAt), JSON.stringify({ back5, u5 }));
    await until(() => placedOk(pid1), 120000);

    /* ── B6 · two custom pieces: two ticks, none ticked ── */
    const ids2 = await sendPiece('Cat portrait two'); await until(() => placedOk(ids2[0]), 120000);
    const c6 = await saveFromWin(A.key);
    check(`B6 two custom pieces: "${c6 && c6.block && c6.block.title}" with two ticks, none ticked at first (${c6 && c6.block && JSON.stringify(c6.block.ticks)})`, !!c6 && !!c6.block && /Custom pieces of order/.test(c6.block.title) && c6.block.ticks.length === 2 && c6.block.ticks.every(t => !t.on), JSON.stringify(c6));
    await press('done'); await until(() => page.evaluate(() => !document.querySelector('.ce-root')), 8000);

    /* ── B7 · the sheet cut: refused ── */
    await page.evaluate(pid => { const sh = allSheets().find(s => (s.charms || []).some(c => c.poolId === pid)); sh.__cdCut = true; sh.laserDoneAt = Date.now(); }, pid1);
    const t7 = takeOffs(pid1).length;
    const c7 = await saveFromWin(A.key);
    const r7 = await page.evaluate(async ([rid, pid]) => { const it = CharmNestCustomDone.openOf(rid, {}).find(i => i.poolIds.includes(pid)); const r = await CharmNestCustomDone.complete({ rid, items: [Object.assign({}, it, { why: '' })], cover: { name: 'X' }, by: 'Paul_K' }); return { why: it && it.why, r }; }, [A.rid, pid1]);
    check(`B7 the piece's sheet cut: its tick is off and says "${r7.why}"; complete() refuses ("${r7.r.why}") and writes nothing (${takeOffs(pid1).length - t7})`,
      !!c7 && !!c7.block && c7.block.ticks.some(t => t.off) && /Cannot be completed here: .*already cut/.test(c7.block.text) && /already cut/.test(r7.why) && !r7.r.ok && /Nothing was changed/.test(r7.r.why) && takeOffs(pid1).length === t7, JSON.stringify({ c7, r7 }));
    await press('done'); await until(() => page.evaluate(() => !document.querySelector('.ce-root')), 8000);
    await page.evaluate(() => { for (const sh of allSheets()) if (sh.__cdCut) { delete sh.__cdCut; sh.laserDoneAt = 0; } });

    /* ── B8 · the Words decision answered by the editor ── */
    await openWin(Bo.key);
    const asked = await page.evaluate(async key => {
      const j = [...Engrave.items().values()].find(x => x.row && x.row.key === key && Engrave.faceOf(x) === 'back'); if (!j) return null;
      Object.assign(j, { text: '', lines: [] }); Engrave.sendBack(j, 'Enter the requested inscription');
      return { key: j.key, state: j.state, card: !!Review.items().find(x => x.key === 'eng:' + j.key && x.kind === 'engraveWords') };
    }, Bo.key);
    await settle(1500);
    await page.evaluate(key => Engrave.openCharmEditor(Engrave.items().get(key)), asked && asked.key);
    await editorUp();
    const face8 = await page.evaluate(() => CharmEditor.current.face ? CharmEditor.current.face() : null);
    await page.evaluate(() => { const E = CharmEditor.current, fr = E.frame(); E.ctx.addItems([{ kind: 'text', role: 'hatch', text: 'Haruki', fontKey: 'sans', sizePt: 6, align: 'center', tx: fr.x0 + fr.wPt / 2, ty: fr.y0 + fr.hPt / 2 }]); });
    await page.click('.ce-root [data-act="save"]');
    await until(() => page.evaluate(() => !!document.querySelector('.cem-card [data-c]')), 60000); await settle(800);
    if (await popupOver()) await closePopup();
    await press('done'); await until(() => page.evaluate(() => !document.querySelector('.ce-root')), 8000); await settle(800);
    const after8 = await page.evaluate(key => { const j = Engrave.items().get(key); return { state: j && j.state, editorWords: j && j.editorWords, card: !!Review.items().find(x => x.key === 'eng:' + key && x.kind === 'engraveWords') }; }, asked && asked.key);
    await openWin(Bo.key); await settle(1500);
    const head8 = await page.evaluate(() => { const d = document.getElementById('orderWin'); return d ? d.textContent.replace(/\s+/g, ' ') : ''; });
    check(`B8 a back asking "Enter the requested inscription" (${JSON.stringify(asked)}), "Haruki" typed on it in the Charm Editor (face ${face8}) and saved: the question is answered (${JSON.stringify(after8)}) and the order window no longer says "Needs a decision · Words" (${!/Enter the requested inscription/.test(head8)})`,
      !!asked && asked.state === 'words' && asked.card && after8.state !== 'words' && !after8.card && !/Enter the requested inscription/.test(head8), JSON.stringify({ asked, after8, head: head8.match(/.{0,60}(Needs a decision|requested inscription).{0,60}/i) }));

    check('B9 no page error', w.errors.length === 0, JSON.stringify(w.errors).slice(0, 400));
  } catch (e) { check('B the run', false, String(e && e.stack || e).slice(0, 800)); }
  finally { await w.close(); }
}

(async () => {
  if (ONLY !== 'B') await partA().catch(e => check('A the run', false, String(e && e.stack || e)));
  if (ONLY !== 'A') await partB();
  const bad = RESULTS.filter(r => !r.ok);
  console.log(`\n${RESULTS.length - bad.length}/${RESULTS.length} passed${bad.length ? ' · FAILED: ' + bad.map(r => r.id.split(' ')[0]).join(', ') : ''}`);
  process.exit(bad.length ? 1 : 0);
})();
