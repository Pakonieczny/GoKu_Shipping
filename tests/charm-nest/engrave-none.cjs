// NONEPLAIN (Paul, 29 Oct 2026, sandbox order 4213864833: "I chose "None" for engraving and it still kept the engraving even after pressing the "Approve" button. When the user chooses "None" for engraving than the
// engraving must be completely removed and all Thumbnails must update in real-time to stop showing the need to add engraving. It just becomes a regular order with no extra engraving required. Also all sheets
// that contain such an order where the user pressed "None" for engraving must also be updated in real-time to remove the offending engraving.")
// On the real sorter page over the repo's fake site (_engraveRealWorld.cjs: offline, made-up names, nothing live), the order "back" (COMET, back "Name One"), its back APPROVED and WRITTEN to its sheet (the
// back's file and record on the sheet, its back index), the line's state "written" (its set's files written: NOT cut):
//   N1  a press on None in the card's face row keeps it at once: no "Cut it plain" row, the back job is cut plain (no job left to engrave, no fit pending, no approval pending), the person's override says none
//   N2  the decision is kept with who made it: a "cut plain" seal on the job, the order timeline's "Engraving skipped by ... — cut plain" line
//   N3  in the same tick, no reload: the Design & Engrave list no longer counts it as needing engraving; the piece's picture as made (charm-nest-made-thumb.js) is gone (its Vector design shows),
//       though the cloud's picture of it engraved is still on its pool row
//   N4  the sheet drops it: the back record leaves the sheet (this page's and the cloud's), the back's record is invalidated, the sheet's back index is cleared; the piece stays where it stood on the sheet
//   N5  the order window (opened after, same page): no Engraving chip, the Vector design boxes show no made picture, the Engraving block shows no back to engrave
//   N6  Approve with None selected (the A key, the card) resurrects nothing: the job stays cut plain, no new approval seal
//   N7  a re-sync and an intake run (the job lost, the line read again from the order's personalisation) never bring it back: the job is "no engraving"
//   N8  the work area's "No back engraving" (the card's own button) on another order removes that face the same way: the toggles say None, the sheet drops it, the made picture goes
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
    const before = await page.evaluate(([p, sid]) => ({ made: !!CharmNestMadeThumb.forPiece(p), cloudMade: !!(B.pool.rows.get(p) || {}).made, onSheet: (Pool.sheetOf(p).backPool || []).some(b => b.poolId === p), index: !!Pool.sheetOf(p).backOutputs, place: JSON.stringify((Pool.sheetOf(p).placements || []).find(x => (Pool.sheetOf(p).charms.find(c => c.poolId === p) || {}).id === x.id)) }), [pid, sheetId]);
    console.log(`   before: ${JSON.stringify({ w1, w2, before })}`);
    check('N0 the back is approved and written to its sheet (record and back index), the piece has its picture as made', !!w1 && !!w2 && before.made && before.onSheet && before.index && before.cloudMade && (st.doc('Charm_Nest_Sheets', sheetId) || { backPool: [] }).backPool.some(b => b.poolId === pid), JSON.stringify({ w1, w2, before }));

    const owNow = rk => page.evaluate(rk => { const d = document.getElementById('orderWin'); return { chip: !!(d && d.querySelector('[data-owf-chip="engraving"]')), made: [...document.querySelectorAll('#owVector,[data-vector]')].filter(v => v._mvRow && v._mvRow.key === rk).map(v => (v.id || 'list') + ':' + (v.dataset.made || '')) }; }, rk);
    await R.openOrderWindow(w, 'back'); await until(async () => (await owNow(rowKey)).made.some(m => /^owVector:\d/.test(m)), 8000);
    const ow0 = await owNow(rowKey); await page.evaluate(() => { try { OrderWin.close(); } catch (_) { /* none */ } });
    check('N0 the order window shows the Engraving chip and the piece as made (front and back) before', ow0.chip && ow0.made.some(m => /^owVector:\d/.test(m)), JSON.stringify(ow0));
    // ── N1 · None, pressed in the card ──
    await R.openEngraveDetail(w, 'back', { face: 'back' }); await R.settle(page, 600);
    await page.evaluate(() => { window.__noReload = 1; window.__tl = []; const L = CNTimeline.line; CNTimeline.line = function (row, type, o) { try { window.__tl.push({ type, text: o && o.text }); } catch (_) { /* none */ } return L.apply(this, arguments); }; });
    const segs0 = await page.evaluate(q => [...document.querySelectorAll(q + ' .pvSetup .engpSeg')].map(b => b.dataset.mode + ':' + b.getAttribute('aria-pressed')).join(' '), Q);
    await page.click(Q + ' .pvSetup .engpSeg[data-mode="none"]');
    const tick = await page.evaluate(() => new Promise(res => requestAnimationFrame(() => requestAnimationFrame(() => res({ confirm: !!document.querySelector('.engpConfirm'), err: (document.querySelector('.engpErr') || {}).textContent || '' })))));
    const cut = await until(() => page.evaluate(([k, rk]) => { const j = Engrave.items().get(k), row = B.orders.byKey.get(rk); return j && j.state === 'skipped' ? { state: j.state, fit: !!j.fit, approvedAt: j.approvedAt, ov: row.engravingOverride, pending: Engrave.pendingCount(), seals: CNEngravingSeals.list(j).map(s => s.how), engrave: row.engrave } : null; }, [key, rowKey]), 15000);
    check('N1 None is kept at once: no "Cut it plain" row, no error', !tick.confirm && !tick.err, JSON.stringify({ segs0, tick }));
    check('N1 the back is cut plain: no job to engrave, no approval pending; the person\'s override says none, with who', !!cut && !cut.approvedAt && cut.ov && cut.ov.mode === 'none' && cut.ov.by === 'Test Operator' && cut.engrave && cut.engrave.needed === false, JSON.stringify(cut));
    const tl = await page.evaluate(() => window.__tl);
    check('N2 the decision is kept with who made it: a "cut plain" seal and the timeline line', !!cut && cut.seals.includes('engravePlain') && cut.seals.includes('engraveApproved') && tl.some(e => e.type === 'engraveChanged' && /skipped by Test Operator/.test(e.text || '')), JSON.stringify({ seals: cut && cut.seals, tl }));

    // ── N3 · the lists and the pictures, no reload ──
    const lists = await until(() => page.evaluate(([p, k]) => {
      const pm = CharmNestMadeThumb.forPiece(p), row = [...document.querySelectorAll('#egQueue [data-key]')].find(e => e.dataset.key === k);
      return !pm ? { made: false, cloud: !!(B.pool.rows.get(p) || {}).made, pending: Engrave.pendingCount(), rows: Engrave.pendingRows(), rowText: row ? row.textContent.replace(/\s+/g, ' ').slice(0, 160) : '' } : null;
    }, [pid, key]), 3000);
    check('N3 the piece\'s picture as made is gone at once (its line shows the Vector design), though the cloud\'s engraved picture is still on its row', !!lists, JSON.stringify(lists));
    check('N3 the Design & Engrave list counts nothing to engrave for it', !!lists && lists.pending === 0, JSON.stringify(lists));
    const segs1 = await page.evaluate(([q, k]) => { CN.setMode('engrave'); Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: k }); Engrave.render(); return new Promise(r => setTimeout(() => r([...document.querySelectorAll(q + ' .pvSetup .engpSeg')].map(b => b.dataset.mode + ':' + b.getAttribute('aria-pressed')).join(' ')), 600)); }, [Q, key]);
    check('N3 the card of the piece, opened again, shows None pressed (Back not)', /none:true/.test(segs1) && /back:false/.test(segs1), segs1);

    // ── N4 · the sheet ──
    // (the sheet holds the other order's back too: its back index is drawn again with that one alone)
    const sheet = await until(() => page.evaluate(p => { const sh = Pool.sheetOf(p); return !(sh.backPool || []).some(b => b.poolId === p) && sh.backOutputs && sh.backOutputs.count === (sh.backPool || []).length ? { here: (sh.backPool || []).length, index: sh.backOutputs.count, place: JSON.stringify((sh.placements || []).find(x => (sh.charms.find(c => c.poolId === p) || {}).id === x.id)) } : null; }, pid), 20000);
    const cloudSheet = await until(() => { const d = st.doc('Charm_Nest_Sheets', sheetId) || {}; return !(d.backPool || []).some(b => b.poolId === pid) && d.backOutputs && d.backOutputs.count === 1 ? d : null; }, 20000);
    const report = cloudSheet && cloudSheet.backOutputs.report && st.blobs.get(cloudSheet.backOutputs.report.path), listed = report ? JSON.parse(Buffer.from(report.buf).toString('utf8')).map(b => b.poolId) : null;
    const rec = st.doc('Charm_Pool_Back', pid) || {};
    check('N4 the sheet drops the back: its record leaves the sheet here and in the cloud, the back record is invalidated, the back index lists only the other order', !!sheet && !!cloudSheet && rec.invalidated === true && !!listed && !listed.includes(pid) && listed.length === 1, JSON.stringify({ sheet, cloud: cloudSheet && { backPool: (cloudSheet.backPool || []).map(b => b.poolId), backOutputs: cloudSheet.backOutputs }, listed, rec: { invalidated: rec.invalidated } }));
    check('N4 the piece stays where it stood on the sheet', !!sheet && sheet.place === before.place, JSON.stringify({ was: before.place, now: sheet && sheet.place }));
    check('N4 the other order\'s back on the same sheet is kept', await page.evaluate(k => { const j = Engrave.items().get(k); return j.state === 'written' && (Pool.sheetOf(j.copies[0]).backPool || []).some(b => b.poolId === j.copies[0]); }, made.back2.jobs[0].key));

    // ── N5 · the order window, same page ──
    await R.openOrderWindow(w, 'back'); await R.settle(page, 800);
    const ow = await page.evaluate(rk => {
      const d = document.getElementById('orderWin'), eng = document.getElementById('owEng');
      return { chip: !!(d && d.querySelector('[data-owf-chip="engraving"]')), made: [...document.querySelectorAll('#owVector,[data-vector]')].filter(v => v._mvRow && v._mvRow.key === rk).map(v => (v.id || 'list') + ':' + (v.dataset.made || '')), eng: eng ? { hidden: !!eng.hidden, cards: eng.querySelectorAll('.engCard').length, text: eng.textContent.replace(/\s+/g, ' ').slice(0, 300), back: [...eng.querySelectorAll('.engFace[data-face="back"]')].map(f => f.textContent.replace(/\s+/g, ' ').slice(0, 80)) } : null, noReload: window.__noReload === 1 };
    }, rowKey);
    console.log(`   order window: ${JSON.stringify(ow)}`);
    check('N5 the order window has no Engraving chip (a regular order), no made picture in its Vector design boxes', !ow.chip && ow.made.some(m => /^owVector/.test(m)) && ow.made.every(m => !/:\d/.test(m)) && ow.noReload, JSON.stringify(ow));
    check('N5 the order window\'s Engraving block asks for no back engraving', !ow.eng || ow.eng.hidden || !ow.eng.cards || !/Name One/.test(ow.eng.back.join(' ')), JSON.stringify(ow.eng));
    if (w.shots) await R.shot(w, 'none-order-window').catch(() => {});
    await page.evaluate(() => { try { OrderWin.close(); } catch (_) { /* none */ } });

    // ── N6 · Approve with None selected ──
    await page.evaluate(([k]) => { CN.setMode('engrave'); Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: k }); Engrave.render(); }, [key]); await R.settle(page, 700);
    const card6 = await page.evaluate(q => { const c = document.querySelector(q); return c ? { key: c.dataset.key, approve: !!c.querySelector('[data-a="approve"]') } : null; }, Q);
    if (card6 && card6.approve) await page.click(Q + ' [data-a="approve"]'); else await page.keyboard.press('a');
    await R.settle(page, 1500);
    const after6 = await page.evaluate(k => { const j = Engrave.items().get(k); return { state: j.state, approvedAt: j.approvedAt, seals: CNEngravingSeals.list(j).map(s => s.how) }; }, key);
    check('N6 Approve with None selected resurrects nothing: still cut plain, no new approval seal', after6.state === 'skipped' && !after6.approvedAt && after6.seals.filter(s => s === 'engraveApproved').length === 1, JSON.stringify({ card6, after6 }));

    // ── N7 · a re-sync and an intake run ──
    const sync = await page.evaluate(async ([rk, k]) => {
      const row = B.orders.byKey.get(rk);
      Orders.interpretAll(); Engrave.ensureJobs(row); try { await Engrave.classify(row); } catch (_) { /* below */ } await Engrave.fitAll();
      const a = Engrave.jobsOf(row).map(j => j.key + ':' + j.state);
      // (the job lost, as a page that never held it: the line is read again from the order's own personalisation)
      Engrave.items().delete(k); row.spec = null; Orders.interpretAll(); Engrave.ensureJobs(row); try { await Engrave.classify(row); } catch (_) { /* below */ } await Engrave.fitAll();
      const rec = Orders.lineRecord ? Orders.lineRecord(row)[1] : null;
      return { a, b: Engrave.jobsOf(row).map(j => j.key + ':' + j.state + ':' + (j.fit ? 'fit' : '')), ov: (rec && rec.engravingOverride) || row.engravingOverride, pending: Engrave.pendingCount() };
    }, [rowKey, key]);
    check('N7 a re-sync and an intake run never bring the engraving back (the override is in the line\'s record)', sync.a.every(s => /:(skipped|none)$/.test(s)) && sync.b.every(s => /:(skipped|none):$/.test(s)) && sync.ov && sync.ov.mode === 'none' && sync.pending === 0, JSON.stringify(sync));

    // ── N8 · "No back engraving" in the work area, on the other order ──
    const key2 = made.back2.jobs[0].key, pid2 = made.back2.poolIds[0];
    await R.openEngraveDetail(w, 'back2', { face: 'back' }); await R.settle(page, 600);
    await page.click(Q + ' [data-a="skip"]');
    const n8 = await until(() => page.evaluate(([k, p]) => { const j = Engrave.items().get(k), sh = Pool.sheetOf(p); return j.state === 'skipped' && !(sh.backPool || []).some(b => b.poolId === p) ? { made: !!CharmNestMadeThumb.forPiece(p), pending: Engrave.pendingCount() } : null; }, [key2, pid2]), 15000);
    await page.evaluate(k => { CN.setMode('engrave'); Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: k }); Engrave.render(); }, key2); await R.settle(page, 700);
    const segs8 = await page.evaluate(q => [...document.querySelectorAll(q + ' .pvSetup .engpSeg')].map(b => b.dataset.mode + ':' + b.getAttribute('aria-pressed')).join(' '), Q);
    check('N8 "No back engraving" removes the face the same way: cut plain, off the sheet, no made picture, the toggles say None', !!n8 && !n8.made && /none:true/.test(segs8) && /back:false/.test(segs8), JSON.stringify({ n8, segs8 }));
    if (w.shots) await R.shot(w, 'none-after').catch(() => {});
    check('N9 no page errors', errors.length === 0, errors.join(' | ').slice(0, 400));
  } catch (e) { console.error(e); check('ran', false, String(e && e.stack || e)); }
  finally { await w.close(); }
  const bad = RESULTS.filter(r => !r.ok);
  console.log(bad.length ? `engrave-none: ${bad.length} failed` : 'engrave-none: ok');
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
