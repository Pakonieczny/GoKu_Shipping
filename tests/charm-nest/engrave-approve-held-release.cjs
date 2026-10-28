// HELDREL (Paul, 28 Oct 2026, order 4212020155 TINY BAR 7427: "The 'Approve' button is not working after I made adjustments to this charm's engraving." then "I noticed that the order that was released
// before is the one that has the malfunctioning Approve engraving button issue.")
//
// Paul's order was held with Hold Order (its piece taken off GF Draft 10), released from its Review card with Release Hold (made up again, placed again, its engraving job made again), and its Engraving card,
// adjusted, would not approve. The pool row keeps the hold's marks for good (heldAt / heldBy / heldReason, as history) beside the release's repooledAt. This test holds what the page must do with that:
//   A  Hold Order, then Release Hold from the Review card, then the Engraving card adjusted and approved: approved at the place the words were moved to, its back file written, the timeline's seal;
//      the cloud's pool row still carries the hold's marks (history) and nothing reads it as held (OrderPieces, the row, the card)
//   B  an approval that THROWS (anything inside it, here the seal's record) is never a silent press: the card says what went wrong, Approve is live again, no half-made approval is left, the next press approves
//   C  an order STILL on hold: its Engraving card (ENGPRE, Paul 13 Oct 2026: "a charm does not have to be placed on that sheet for it to be approved/updated") approves without a sheet and says nothing wrong;
//      its back file waits for a sheet (none is written while it is held)
// Offline only: the REAL page over the repo's fake site with real drawings (_engraveRealWorld.cjs). Made-up names only; no Etsy, no paid call, no live write.
//   /tmp/.../int/slot.sh timeout 600 node tests/charm-nest/engrave-approve-held-release.cjs      Exit code 1 while any check is red.
'use strict';
const R = require('./_engraveRealWorld.cjs');
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${evidence}`); };
const Q = '#egQueue .rvItem[data-kind=placement]';

(async () => {
  const w = await R.world({ viewport: { width: 1440, height: 900 } });
  if (!w) { console.log('no playwright-core: nothing run'); process.exit(0); }
  const page = w.page, st = w.srv.st;
  const until = async (fn, ms) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) return null; await new Promise(r => setTimeout(r, 150)); } };
  const cloud = (coll, pred) => [...st.docs.entries()].filter(([k]) => k.split('/')[0].endsWith(coll)).map(([k, v]) => Object.assign({ _id: k.split('/').slice(1).join('/') }, v)).filter(pred);
  const jobOf = rid => page.evaluate(rid => { const j = [...Engrave.items().values()].find(j => j.row && (j.row.parentRow || j.row).order.receiptId === rid && j.face !== 'front' && !/~front$/.test(j.key)); return j ? { key: j.key, state: j.state, pre: !!j.pre, approvedAt: j.approvedAt || null, intent: !!j.approvalIntent, centre: j.fit ? j.fit.centre.map(v => Math.round(v * 1000) / 1000) : null, nudged: !!j.nudged, backs: (j.backs || []).length, sheet: (j.copies || []).map(id => (Pool.sheetOf(id) || {}).sheetId || null) } : null; }, rid);
  const rowOf = rid => page.evaluate(rid => { const r = Orders.rows().find(x => x.order.receiptId === rid); const pp = window.OrderPieces && OrderPieces.of ? OrderPieces.of(rid) : []; return { state: r.state, hold: r.hold || null, poolIds: (r.poolIds || []).slice(), cloudHold: pp.some(p => p && p.hold), marked: !!(window.CharmNestReviewMark && CharmNestReviewMark.isMarked(rid)) }; }, rid);
  const card = key => page.evaluate(({ Q, key }) => { const c = document.querySelector(Q), mine = !!c && c.dataset.key === key, ap = mine ? c.querySelector('[data-a="approve"]') : null, why = mine ? c.querySelector('.egApproveWhy') : null; return { mine, approve: ap ? { text: ap.textContent.trim(), live: !ap.disabled } : null, why: why ? why.textContent.trim() : '' }; }, { Q, key });
  const press = () => page.evaluate(Q => { const b = document.querySelector(Q + ' [data-a="approve"]'); if (!b) return 'none'; if (b.disabled) return 'dimmed'; b.click(); return 'pressed'; }, Q);
  const open = async rid => { const key = await page.evaluate(rid => { const row = Orders.rows().find(r => r.order.receiptId === rid), j = Engrave.jobsOf(row).find(x => x.face !== 'front') || Engrave.jobsOf(row)[0]; if (!j) return null; try { if (window.OrderWin && OrderWin.close) OrderWin.close(); } catch (_) {} CN.setMode('engrave'); Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: j.key }); Engrave.render(); return j.key; }, rid); await page.waitForSelector(Q, { timeout: 15000 }).catch(() => {}); await R.settle(page, 700); return key; };
  /** Paul's adjustment: the words dragged in the work area (a few tries: a drag that lands on no glyph moves nothing). */
  async function drag(rid) {
    const c0 = (await jobOf(rid)).centre;
    for (const [dx, dy] of [[5, 2], [-5, 2], [5, -2]]) {
      const b = await page.evaluate(Q => { const c = document.querySelector(Q + ' .backHost canvas'); if (!c || !c._box) return null; const r = c.getBoundingClientRect(), k = r.width / c.width; return [r.left + c._box.centrePx[0] * k, r.top + c._box.centrePx[1] * k]; }, Q);
      if (!b) break;
      await page.mouse.move(b[0], b[1]); await page.mouse.down(); await page.mouse.move(b[0] + dx, b[1] + dy, { steps: 4 }); await page.mouse.up(); await R.settle(page, 300);
      if (JSON.stringify((await jobOf(rid)).centre) !== JSON.stringify(c0)) break;
    }
    return { c0, j: await jobOf(rid) };
  }
  const holdOrder = rid => page.evaluate(async rid => { const row = Orders.rows().find(r => r.order.receiptId === rid); const r = await ReviewSend.press(rid, row, { offSheets: true }); return JSON.parse(JSON.stringify(r)); }, rid);
  const releaseHold = rid => page.evaluate(async rid => { try { Review.syncOrderItems(); } catch (_) {} const it = ReviewRelease.itemOf(rid); const how = ReviewRelease.of(it); const ok = await ReviewRelease.press(it); return { ok, kind: it.kind, how: how && how.how }; }, rid);
  try {
    await R.build(w, ['back', 'front', 'both']);
    // an open run (the release places the order at once, as on Paul's sorter), the sheet in it
    await page.evaluate(() => { B.run = { runId: 'run-real', day: new Date().toISOString().slice(0, 10), setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] }; for (const sh of allSheets()) if (sh.charms.length) sh.runId = 'run-real'; Orders.render(); });
    const A = R.ORDERS.back.rid, BOTH = R.ORDERS.both.rid, C = R.ORDERS.front.rid;

    // ── A · Hold Order, Release Hold, then the card adjusted and approved ──
    const h = await holdOrder(A); await R.settle(page, 600);
    const a1 = await rowOf(A), aj1 = await jobOf(A), held = cloud('Charm_Pool', p => p.poolId && p.poolId.startsWith(A + '_'));
    check('A1 Hold Order: the order is on hold, its piece off the sheet, its engraving job gone with it; the cloud\'s pool row carries the hold\'s marks', h.ok && a1.hold && !a1.poolIds.length && !aj1 && held.some(p => p.state === 'abandoned' && p.heldBy && +p.heldAt > 0), JSON.stringify({ h, a1, aj1, held: held.map(p => ({ state: p.state, heldBy: p.heldBy, heldAt: !!p.heldAt })) }));
    const t0 = Date.now(), rel = await releaseHold(A);
    const a2 = await until(async () => { const j = await jobOf(A); return j && j.state === 'review' ? j : null; }, 120000);
    const row2 = await rowOf(A), live = cloud('Charm_Pool', p => p.poolId && p.poolId.startsWith(A + '_'));
    check('A2 Release Hold: back in line and placed, a new engraving job fitted and waiting for approval', rel.ok && !row2.hold && row2.poolIds.length === 1 && !!a2 && a2.sheet.every(Boolean), JSON.stringify({ rel, ms: Date.now() - t0, row2, a2 }));
    check('A3 the cloud\'s pool row keeps the hold\'s marks as history beside the release\'s repooledAt, and nothing reads it as held (OrderPieces, the row, Review)', live.some(p => p.heldBy && +p.heldAt > 0 && +p.repooledAt > +p.heldAt && p.state !== 'abandoned') && !row2.cloudHold && !row2.marked && row2.state !== 'held', JSON.stringify({ live: live.map(p => ({ state: p.state, heldBy: p.heldBy, heldAt: p.heldAt, repooledAt: p.repooledAt })), row2 }));
    const key = await open(A);
    const d = await drag(A);
    const f0 = await card(key);
    check('A4 its Engraving card, the words moved in the work area (Paul\'s adjustment): a live Approve', f0.mine && d.j.nudged && JSON.stringify(d.j.centre) !== JSON.stringify(d.c0) && f0.approve && f0.approve.live, JSON.stringify({ d, f0 }));
    const tp = Date.now(), p1 = await press();
    const a5 = await until(async () => { const j = await jobOf(A); return j && /approved|written/.test(j.state) ? j : null; }, 30000), msA = Date.now() - tp;
    const said = await card(key);
    check('A5 the press approves it, at the place the words were moved to, with no refusal said', p1 === 'pressed' && !!a5 && JSON.stringify(a5.centre) === JSON.stringify(d.j.centre) && !/Not approved/.test(said.why), JSON.stringify({ p1, msA, a5, said }));
    const w5 = await until(async () => { const j = await jobOf(A); return j && j.state === 'written' ? j : null; }, 60000);
    const seal = cloud('Order_Timeline', e => String(e.orderId) === A && e.type === 'engraveApproved'), back = cloud('Charm_Pool_Back', b => b.poolId && b.poolId.startsWith(A + '_') && +b.approvedAt === (a5 && a5.approvedAt));
    check('A6 its back file is written and recorded for the sheet it is on now, the Engraved seal on the order\'s timeline', !!w5 && back.length === 1 && back[0].sheetId === w5.sheet[0] && seal.length >= 1, JSON.stringify({ state: w5 && w5.state, back: back.map(b => b.sheetId), seal: seal.length }));

    // ── B · an approval that throws says so in the card; the next press approves ──
    const kb = await open(BOTH);
    await page.evaluate(() => { const S = window.CNEngravingSeals, keep = S.keep; window.__keep = keep; let once = true; S.keep = job => { if (once) { once = false; throw new Error('test: the seal record could not be read'); } return keep(job); }; });
    const tb = Date.now(), p2 = await press();
    const b1 = await until(async () => { const c = await card(kb); return c.why ? c : null; }, 30000), msB = Date.now() - tb;
    const bj = await jobOf(BOTH);
    check('B1 an approval that throws is not a silent press: the card says what went wrong, in words', p2 === 'pressed' && !!b1 && /^Not approved: something went wrong while approving \(test: the seal record could not be read\)/.test(b1.why), JSON.stringify({ p2, msB, b1 }));
    const b2 = await until(async () => { const c = await card(kb), j = await jobOf(BOTH); return c.approve && c.approve.live && c.approve.text === 'Approve' && j.state === 'review' ? { c, j } : null; }, 8000);
    check('B2 nothing is left half made: Approve is live again, the placement still waits for approval, no approval intent is kept', !!b2 && !b2.j.intent && !b2.j.approvedAt, JSON.stringify(b2 || { c: await card(kb), j: await jobOf(BOTH) }));
    await page.evaluate(() => { window.CNEngravingSeals.keep = window.__keep; });
    const p3 = await press();
    const b3 = await until(async () => { const j = await jobOf(BOTH); return j && /approved|written/.test(j.state) ? j : null; }, 30000);
    check('B3 the next press approves it', p3 === 'pressed' && !!b3, JSON.stringify({ p3, b3, bj }));

    // ── C · an order still on hold: its card approves without a sheet (ENGPRE), its back file waits ──
    const hc = await holdOrder(C); await R.settle(page, 600);
    const made = await page.evaluate(async rid => { const row = Orders.rows().find(r => r.order.receiptId === rid); try { await Engrave.early(row); } catch (e) { return String(e && e.message || e); } return 'ok'; }, C);
    const c1 = await until(async () => { const j = await jobOf(C); return j && j.state === 'review' ? j : null; }, 60000);
    const rc = await rowOf(C);
    check('C1 an order still on hold: its engraving is made without a sheet (ENGPRE) and waits for approval', hc.ok && !!rc.hold && made === 'ok' && !!c1 && c1.pre && c1.sheet.every(x => !x), JSON.stringify({ hc, rc, made, c1 }));
    const kc = await open(C);
    const p4 = await press();
    const c2 = await until(async () => { const j = await jobOf(C); return j && /approved|written/.test(j.state) ? j : null; }, 30000);
    const cw = await card(kc), still = await rowOf(C), cb = cloud('Charm_Pool_Back', b => b.poolId && b.poolId.startsWith(C + '_') && !b.invalidated);
    check('C2 its Approve approves it with nothing wrong said; the order stays on hold and no back file is written while it has no sheet', p4 === 'pressed' && !!c2 && c2.state === 'approved' && !/Not approved/.test(cw.why) && !!still.hold && !still.poolIds.length && cb.length === 0, JSON.stringify({ p4, c2, cw, still, backs: cb.length }));
  } catch (e) { check('run', false, String(e && e.stack || e).split('\n').slice(0, 3).join(' | ')); }
  check('no page error', w.errors.length === 0, w.errors.slice(0, 2).join(' | '));
  await w.close();
  const bad = RESULTS.filter(r => !r.ok);
  console.log(`\n${RESULTS.length - bad.length}/${RESULTS.length} pass`);
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
