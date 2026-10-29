// NONEPLAIN (Paul, 29 Oct 2026, sandbox order 4213864833: "I chose "None" for engraving and it still kept the engraving even after pressing the "Approve" button ... It just becomes a regular order with no
// extra engraving required. Also all sheets that contain such an order ... must also be updated in real-time to remove the offending engraving.") and NONEPLAIN 2 (Paul, 29 Oct 2026, order 4213825625: "When I
// press none the actual order just abruptly disappears with no feedback ... if the user presses none maybe they want to change their mind and then press back or front again and then what? Once the user selects
// "None" and clicks the green "Approve" button, you will need to update ALL of the thumbnails and information including ALL required Sheets ... There is no stamp needed for this, it simply reverts back to a
// normal order that has no extra engravings. The thumbnails will ALL need to revert back to just showing the Charm vector file alone in the Thumbnail everywhere in the application.")
// On the real sorter page over the repo's fake site (_engraveRealWorld.cjs: offline, made-up names, nothing live), the orders "back" and "back2" (COMET, back words), each back APPROVED and WRITTEN to the same
// sheet (the back's file and record on the sheet, its back index), the line's state "written" (its set's files written: NOT cut):
//   N1  None is STAGED only: nothing saved, the card stays, None pressed, the thumbnail and the work area show the charm alone, a line by Approve says what Approve will do, a green Approve is there
//   N2  Back pressed again restores exactly what was there (words, approval), nothing saved
//   N3  None again, then Approve (the A key): the engraving is taken away (no job to engrave, the person's override), NO seal and no stamp, one timeline line "Engraving removed by ...", a toast, the tab moves on
//   N4  every picture is the charm alone (made-thumbs, list boxes), the rail has no Engraved step
//   N5  the sheet drops it (here and in the cloud, the back record invalidated, the back index without it), the piece stays where it stood, the other order's back is kept
//   N6  the order window (opened after, same page): no Engraving chip, the Vector design the charm alone, no Engraving block
//   N7  a re-sync and an intake run never bring it back
//   N8  the green Approve button with None staged does the same (the other order)
//   N9  no page errors
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/engrave-none.cjs      (SHOTS=<dir> for screenshots)
'use strict';
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : evidence || ''}`.trimEnd()); };
const until = async (f, ms) => { const t0 = Date.now(); for (;;) { const v = await f(); if (v || Date.now() - t0 > ms) return v; await new Promise(r => setTimeout(r, 100)); } };

(async () => {
  const R = require('./_engraveRealWorld.cjs');
  R.ORDERS.back2 = Object.assign({}, R.ORDERS.back, { rid: '4290000141', listing: '1890000141', buyer: 'Name Twelve', personalization: ['Name Two'] }); R.EXPECT_JOBS.back2 = 1;
  const w = await R.world({ shots: process.env.SHOTS || '/tmp/engrave-none-shots' });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  const page = w.page, st = w.srv.st, errors = [];
  page.on('pageerror', e => errors.push(e.message));
  await page.waitForFunction(() => !window.CNRole || ['ask', 'role', 'admin'].includes(CNRole.state()), null, { timeout: 20000 }).catch(() => {}).then(() => page.evaluate(() => { try { if (window.CNRole && CNRole.state() === 'ask') CNRole.choose('design'); } catch (_) { /* none */ } }));
  const Q = '#egQueue .rvItem[data-kind=placement]';
  try {
    const made = await R.build(w, ['back', 'back2']);
    console.log(`   built: ${JSON.stringify([made.back.jobs.map(j => [j.key, j.state, j.text]), made.back2.jobs.map(j => [j.key, j.state, j.text])])}`);
    // (the sheets as the files are written to: the approval writes the back's file into the sheet's folder and records it on the sheet)
    const sheets = await page.evaluate(() => { const out = []; for (const s of allSheets()) if (s.charms && s.charms.length) { if (!s.folderPath) s.folderPath = 'charmnest/sheets/' + s.sheetId; out.push({ sheetId: s.sheetId, poolIds: s.charms.map(x => x.poolId) }); } return out; });
    for (const s of sheets) st.put('Charm_Nest_Sheets', s.sheetId, { sheetId: s.sheetId, poolIds: s.poolIds, backPool: [] });
    const approveWritten = async name => {
      const key = made[name].jobs[0].key;
      await R.openEngraveDetail(w, name, { face: 'back' }); await R.settle(page, 500);
      await page.click(Q + ' [data-a="approve"]');
      return until(() => page.evaluate(k => { const j = Engrave.items().get(k), sh = j && Pool.sheetOf(j.copies[0]); return j && j.state === 'written' && sh && sh.backOutputs && (sh.backPool || []).some(b => b.poolId === j.copies[0]) ? { state: j.state, sheet: sh.sheetId } : null; }, key), 120000);
    };
    const w1 = await approveWritten('back'), w2 = await approveWritten('back2');
    const key = made.back.jobs[0].key, pid = made.back.poolIds[0], rowKey = made.back.key;
    // (the cloud keeps a picture of each piece as made: as on Paul's order, the pool row holds the engraved one)
    await page.evaluate(() => CharmNestMadeThumb.flush()); await until(() => page.evaluate(p => !!(B.pool.rows.get(p) || {}).made, pid), 20000);
    // (the line's set's files are written: the line reads "written", as Paul's did; its sheet is NOT cut)
    await page.evaluate(rk => { B.orders.byKey.get(rk).state = 'written'; }, rowKey);
    const sheetId = w1 && w1.sheet;
    const before = await page.evaluate(p => ({ made: !!CharmNestMadeThumb.forPiece(p), cloudMade: !!(B.pool.rows.get(p) || {}).made, onSheet: (Pool.sheetOf(p).backPool || []).some(b => b.poolId === p), index: !!Pool.sheetOf(p).backOutputs, place: JSON.stringify((Pool.sheetOf(p).placements || []).find(x => (Pool.sheetOf(p).charms.find(c => c.poolId === p) || {}).id === x.id)) }), pid);
    console.log(`   before: ${JSON.stringify({ w1, w2, before })}`);
    check('N0 the back is approved and written to its sheet (record and back index), the piece has its picture as made', !!w1 && !!w2 && before.made && before.onSheet && before.index && before.cloudMade && (st.doc('Charm_Nest_Sheets', sheetId) || { backPool: [] }).backPool.some(b => b.poolId === pid), JSON.stringify({ w1, w2, before }));
    const owNow = rk => page.evaluate(rk => { const d = document.getElementById('orderWin'); return { chip: !!(d && d.querySelector('[data-owf-chip="engraving"]')), made: [...document.querySelectorAll('#owVector,[data-vector]')].filter(v => v._mvRow && v._mvRow.key === rk).map(v => (v.id || 'list') + ':' + (v.dataset.made || '')) }; }, rk);
    await R.openOrderWindow(w, 'back'); await until(async () => (await owNow(rowKey)).made.some(m => /^owVector:\d/.test(m)), 8000);
    const ow0 = await owNow(rowKey); await page.evaluate(() => { try { OrderWin.close(); } catch (_) { /* none */ } });
    check('N0 the order window shows the Engraving chip and the piece as made (front and back) before', ow0.chip && ow0.made.some(m => /^owVector:\d/.test(m)), JSON.stringify(ow0));

    // ── N1 · None is only STAGED ──
    await R.openEngraveDetail(w, 'back', { face: 'back' }); await R.settle(page, 600);
    await page.evaluate(() => {
      window.__noReload = 1; window.__tl = []; window.__toasts = [];
      const L = CNTimeline.line; CNTimeline.line = function (row, type, o) { try { window.__tl.push({ type, text: o && o.text }); } catch (_) { /* none */ } return L.apply(this, arguments); };
      const T = window.toast; window.toast = function (msg) { try { window.__toasts.push(String(msg)); } catch (_) { /* none */ } return T.apply(this, arguments); };
    });
    const cardNow = k => page.evaluate(([q, k]) => {
      const c = document.querySelector(q), th = f => { const t = c && c.querySelector(`.pvSetup .engFace[data-face="${f}"] .engThumb`); return t ? { pic: !!t.firstElementChild, words: [...t.querySelectorAll('.engpvText')].map(x => x.textContent).join('').trim() } : null; };
      const d = Engrave.draftOf(k), calm = c && c.querySelector('[data-a="approved"]'), ap = c && c.querySelector('[data-a="approve"]');
      return { key: c ? c.dataset.key : '', segs: c ? [...c.querySelectorAll('.pvSetup .engpSeg')].map(b => b.dataset.mode + ':' + b.getAttribute('aria-pressed')).join(' ') : '', back: th('back'), draft: d ? { empty: d.empty, lines: d.lines } : null,
        hint: ((c && c.querySelector('.engpNoneHint')) || {}).textContent || '', approve: !!ap && !ap.disabled && !!ap.getClientRects().length, calm: !!calm && !!calm.getClientRects().length, confirm: !!(c && c.querySelector('.engpConfirm')), err: ((c && c.querySelector('.engpErr')) || {}).textContent || '' };
    }, [Q, k]);
    const job = k => page.evaluate(([k, rk]) => { const j = Engrave.items().get(k), row = B.orders.byKey.get(rk), sh = Pool.sheetOf(j.copies[0]); return { state: j.state, text: j.text, approvedAt: j.approvedAt || 0, ov: (j.row.parentRow || j.row).engravingOverride || null, seals: CNEngravingSeals.list(j).map(s => s.how), rowSeals: ((j.row.engrave || {}).seals || []).length, onSheet: (sh.backPool || []).some(b => b.poolId === j.copies[0]), pool: (B.pool.rows.get(j.copies[0]) || {}).engrave, made: !!CharmNestMadeThumb.forPiece(j.copies[0]) }; }, [k, rowKey]);
    const c0 = await cardNow(key);
    await page.click(Q + ' .pvSetup .engpSeg[data-mode="none"]');
    await R.settle(page, 400);
    const c1 = await cardNow(key), j1 = await job(key);
    check('N1 None is staged only: nothing is saved (the back stays approved and on its sheet, no override), no "Cut it plain" row, no error', j1.state === 'written' && !j1.ov && j1.onSheet && j1.made && !c1.confirm && !c1.err, JSON.stringify({ j1, c1 }));
    check('N1 the card stays where it is, the None toggle pressed', c1.key === key && /none:true/.test(c1.segs) && /back:false/.test(c1.segs), JSON.stringify({ c0: c0.segs, c1 }));
    check('N1 the Back thumbnail and the work area show the charm alone, no words', !!c1.back && c1.back.pic && !c1.back.words && !!c1.draft && c1.draft.empty, JSON.stringify({ back: c1.back, draft: c1.draft, was: c0.back }));
    check('N1 a line by Approve says what Approve will do, and the green Approve is there (the piece was approved: its calm "Approved" makes way)', /No engraving · Approve to make this a regular order/.test(c1.hint) && c1.approve && !c1.calm, JSON.stringify(c1));
    if (w.shots) await R.shot(w, 'none-staged').catch(() => {});

    // ── N2 · Back again: what was there comes back ──
    await page.click(Q + ' .pvSetup .engpSeg[data-mode="back"]');
    await R.settle(page, 500);
    const c2 = await cardNow(key), j2 = await job(key);
    check('N2 Back pressed again restores exactly what was there: Back pressed, its words on the thumbnail and in the work area, still approved, nothing saved', /back:true/.test(c2.segs) && /none:false/.test(c2.segs) && !!c2.back && /Name One/i.test(c2.back.words) && !(c2.draft && c2.draft.empty) && j2.state === 'written' && j2.approvedAt > 0 && !j2.ov && !c2.hint && c2.calm, JSON.stringify({ c2, j2 }));

    // ── N3 · None again, then Approve (the A key) commits ──
    await page.click(Q + ' .pvSetup .engpSeg[data-mode="none"]'); await R.settle(page, 300);
    await page.evaluate(q => { const c = document.querySelector(q); if (!c.hasAttribute('tabindex')) c.setAttribute('tabindex', '-1'); c.focus(); }, Q);
    await page.keyboard.press('a');
    const done = await until(async () => { const j = await job(key); return j.state === 'none' ? j : null; }, 15000);
    await R.settle(page, 400);
    const tl = await page.evaluate(() => window.__tl), toasts = await page.evaluate(() => window.__toasts);
    check('N3 Approve (the A key) with None staged takes the engraving away: no job to engrave, no approval, the person\'s override says none, with who', !!done && !done.approvedAt && done.ov && done.ov.mode === 'none' && done.ov.by === 'Test Operator', JSON.stringify(done));
    check('N3 no stamp and no seal: no "cut plain" seal, the BACK ENGRAVING seal gone from the job and the line', !!done && done.seals.length === 0 && done.rowSeals === 0 && !tl.some(e => /skipped/.test(e.text || '')), JSON.stringify({ seals: done && done.seals, tl }));
    check('N3 one plain timeline line with who did it: "Engraving removed by Test Operator"', tl.some(e => e.type === 'engraveChanged' && /ngraving removed by Test Operator/.test(e.text || '')), JSON.stringify(tl));
    check('N3 clear feedback: "Order 4290000101 · no engraving · now a regular order"', toasts.some(t => /Order 4290000101 · no engraving · now a regular order/.test(t)), JSON.stringify(toasts));
    const moved = await page.evaluate(([q, k]) => { const c = document.querySelector(q); return { card: c ? c.dataset.key : '', pending: Engrave.pendingCount(), listed: [...document.querySelectorAll('#egQueue [data-key]')].some(e => e.dataset.key === k) }; }, [Q, key]);
    check('N3 the tab moves on as after any Approve, and the piece is in no list and counts nothing', moved.card !== key && !moved.listed && moved.pending === 0, JSON.stringify(moved));

    // ── N4 · every picture: the charm alone ──
    const pics = await until(() => page.evaluate(([p, rk]) => { const row = B.orders.byKey.get(rk); return !CharmNestMadeThumb.forPiece(p) && !CharmNestMadeThumb.forLine(row) ? { cloud: !!(B.pool.rows.get(p) || {}).made, boxes: [...document.querySelectorAll('[data-vector]')].filter(v => v._mvRow && v._mvRow.key === rk).map(v => v.dataset.made || '') } : null; }, [pid, rowKey]), 3000);
    check('N4 the piece\'s picture as made is gone at once: every list box of the line shows the charm vector alone (no Front/Back split)', !!pics && pics.boxes.every(m => !m), JSON.stringify(pics));
    const rail = await page.evaluate(rk => OrderTimelineUI.stagesFor(B.orders.byKey.get(rk)).map(s => s.k), rowKey);
    check('N4 the order\'s rail has no Engraved step (no BACK ENGRAVING stamp)', !rail.includes('engraved'), JSON.stringify(rail));

    // ── N5 · the sheet ──
    const sheet = await until(() => page.evaluate(p => { const sh = Pool.sheetOf(p); return !(sh.backPool || []).some(b => b.poolId === p) && sh.backOutputs && sh.backOutputs.count === (sh.backPool || []).length ? { here: (sh.backPool || []).length, index: sh.backOutputs.count, place: JSON.stringify((sh.placements || []).find(x => (sh.charms.find(c => c.poolId === p) || {}).id === x.id)) } : null; }, pid), 20000);
    const cloudSheet = await until(() => { const d = st.doc('Charm_Nest_Sheets', sheetId) || {}; return !(d.backPool || []).some(b => b.poolId === pid) && d.backOutputs && d.backOutputs.count === 1 ? d : null; }, 20000);
    const report = cloudSheet && cloudSheet.backOutputs.report && st.blobs.get(cloudSheet.backOutputs.report.path), listed = report ? JSON.parse(Buffer.from(report.buf).toString('utf8')).map(b => b.poolId) : null;
    const rec = st.doc('Charm_Pool_Back', pid) || {}, prow = await job(key);
    check('N5 the sheet drops the back: off the sheet here and in the cloud, its record invalidated, the back index lists only the other order, the piece not engraved', !!sheet && !!cloudSheet && rec.invalidated === true && !!listed && !listed.includes(pid) && listed.length === 1 && prow.pool === false, JSON.stringify({ sheet, cloud: cloudSheet && { backPool: (cloudSheet.backPool || []).map(b => b.poolId), backOutputs: cloudSheet.backOutputs }, listed, rec: { invalidated: rec.invalidated }, pool: prow.pool }));
    const other = await page.evaluate(k => { const j = Engrave.items().get(k); return j.state === 'written' && (Pool.sheetOf(j.copies[0]).backPool || []).some(b => b.poolId === j.copies[0]); }, made.back2.jobs[0].key);
    check('N5 the piece stays where it stood on the sheet; the other order\'s back is kept', !!sheet && sheet.place === before.place && other, JSON.stringify({ was: before.place, now: sheet && sheet.place, other }));

    // ── N6 · the order window, same page ──
    await R.openOrderWindow(w, 'back'); await R.settle(page, 800);
    const ow = await page.evaluate(rk => {
      const d = document.getElementById('orderWin'), eng = document.getElementById('owEng');
      return { chip: !!(d && d.querySelector('[data-owf-chip="engraving"]')), made: [...document.querySelectorAll('#owVector,[data-vector]')].filter(v => v._mvRow && v._mvRow.key === rk).map(v => (v.id || 'list') + ':' + (v.dataset.made || '')), eng: eng ? { hidden: !!eng.hidden, cards: eng.querySelectorAll('.engCard').length } : null, noReload: window.__noReload === 1 };
    }, rowKey);
    check('N6 the order window: no Engraving chip, its Vector design the charm alone, no Engraving block; no reload', !ow.chip && ow.made.some(m => /^owVector/.test(m)) && ow.made.every(m => !/:\d/.test(m)) && (!ow.eng || ow.eng.hidden || !ow.eng.cards) && ow.noReload, JSON.stringify(ow));
    if (w.shots) await R.shot(w, 'none-order-window').catch(() => {});
    await page.evaluate(() => { try { OrderWin.close(); } catch (_) { /* none */ } });

    // ── N7 · a re-sync and an intake run ──
    const sync = await page.evaluate(async ([rk, k]) => {
      const row = B.orders.byKey.get(rk);
      Orders.interpretAll(); Engrave.ensureJobs(row); try { await Engrave.classify(row); } catch (_) { /* below */ } await Engrave.fitAll();
      const a = Engrave.jobsOf(row).map(j => j.key + ':' + j.state);
      // (the job lost, as a page that never held it: the line is read again from the order's own personalisation)
      Engrave.items().delete(k); row.spec = null; Orders.interpretAll(); Engrave.ensureJobs(row); try { await Engrave.classify(row); } catch (_) { /* below */ } await Engrave.fitAll();
      const rec = Orders.lineRecord ? Orders.lineRecord(row)[1] : null;
      return { a, b: Engrave.jobsOf(row).map(j => j.key + ':' + j.state + ':' + (j.fit ? 'fit' : '')), ov: (rec && rec.engravingOverride) || row.engravingOverride, seals: ((rec && rec.engrave) || {}).seals || [], pending: Engrave.pendingCount() };
    }, [rowKey, key]);
    check('N7 a re-sync and an intake run never bring the engraving back (the override is in the line\'s record, no seal in it)', sync.a.every(s => /:none$/.test(s)) && sync.b.every(s => /:none:$/.test(s)) && sync.ov && sync.ov.mode === 'none' && sync.pending === 0 && !sync.seals.length, JSON.stringify(sync));

    // ── N8 · the green Approve button does the same (the other order) ──
    const key2 = made.back2.jobs[0].key;
    await R.openEngraveDetail(w, 'back2', { face: 'back' }); await R.settle(page, 600);
    await page.click(Q + ' .pvSetup .engpSeg[data-mode="none"]'); await R.settle(page, 300);
    const c8 = await cardNow(key2);
    await page.click(Q + ' [data-a="approve"]');
    const d8 = await until(async () => { const j = await job(key2); return j.state === 'none' ? j : null; }, 15000);
    check('N8 the green Approve button with None staged does the same: no engraving, no seal, off the sheet, the charm alone', !!d8 && !d8.seals.length && !d8.onSheet && !d8.made && /none:true/.test(c8.segs) && c8.approve, JSON.stringify({ c8, d8 }));
    if (w.shots) await R.shot(w, 'none-after').catch(() => {});
    check('N9 no page errors', errors.length === 0, errors.join(' | ').slice(0, 400));
  } catch (e) { console.error(e); check('ran', false, String(e && e.stack || e)); }
  finally { await w.close(); }
  const bad = RESULTS.filter(r => !r.ok);
  console.log(bad.length ? `engrave-none: ${bad.length} failed` : 'engrave-none: ok');
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
