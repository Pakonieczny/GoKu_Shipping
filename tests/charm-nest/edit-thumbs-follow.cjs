// EDITFOLLOW (Paul, 29 Oct 2026, order 4213955608, a CALENDAR 86188: "When I updated this charm in the Charm Editor and then returned to the Engraving modal, not all of the visible vector thumbnails
// got updated correctly." The work area showed the new state, the words "SEMPTEMBER 1966" moved to the top of the calendar in the Charm Editor and a big "21"; the card's Front box beside it still
// showed the old drawing: the words small in the middle over the grid, over a small "21").
// On the real sorter page over the repo's fake site (_engraveRealWorld.cjs: offline, made-up names), the calendar master (fixtures/CALENDAR_86188.ai) ordered "front - September 1966 / back - 12:11 p.m.",
// its front approved first, as Paul's was:
//   E1  the Charm Editor opened from the Front card, a "21" written in a day square, the card's words moved up to the top of the calendar, Save: once the save and the refit landed, the card's Front box
//       (its left column: CharmNestEngraveCard / CharmNestEngravePreview) is a fresh drawing of the saved state, the same words at the same place, the same drawing, pixel for pixel on screen
//   E2  the box shows the words at their new place within a frame of the new placement landing (the job's fit at its new place, the work area's real drawing); every frame before it says Updating
//   E3  the piece's Orders row box shows the drawing of its current state; E4 the order window's Vector design and ENGRAVING card Front box too; E6 with the order window open, a save of the
//       front's art: its pictures follow once the face is fitted again, without the window being opened again; E5 nothing erred
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/edit-thumbs-follow.cjs      (SHOTS=<dir> for the screenshots)
'use strict';
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : evidence || ''}`.trimEnd()); };
const settleP = page => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
const until = async (f, ms) => { const t0 = Date.now(); for (;;) { const v = await f(); if (v || Date.now() - t0 > ms) return v; await new Promise(r => setTimeout(r, 150)); } };

(async () => {
  const R = require('./_engraveRealWorld.cjs');
  R.DESIGNS.CALENDAR_86188 = { file: 'CALENDAR_86188.ai', entry: { sku: 'CALENDAR_86188', widthPt: 33.68, heightPt: 39.3081, areaPt2: 1036.17, members: 15, holes: 1, engravable: true, engravableBy: 'index', upAngle: 89.9977, upSource: 'hole', charmHash: '60dde504', backKeepOut: [] } };
  R.ORDERS.cal = Object.assign({}, R.ORDERS.both, { rid: '4290000131', sku: 'CALENDAR_86188', title: 'Calendar Necklace, Personalized Month Calendar Charm', buyer: 'Name Eleven', personalization: ['front - September 1966\nback - 12:11 p.m.'] });
  R.EXPECT_JOBS.cal = 2;
  const w = await R.world({ shots: process.env.SHOTS || '/tmp/edit-thumbs-follow-shots' });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  await w.page.waitForFunction(() => !window.CNRole || ['ask', 'role', 'admin'].includes(CNRole.state()), null, { timeout: 20000 }).catch(() => {}).then(() => w.page.evaluate(() => { try { if (window.CNRole && CNRole.state() === 'ask') CNRole.choose('design'); } catch (_) { /* none */ } }));
  const page = w.page, st = w.srv.st, Q = '#egQueue .rvItem[data-kind=placement]', TH = Q + ' .engFace[data-face="front"] .engThumb';
  try {
    const made = await R.build(w, ['cal']), PID = made.cal.poolIds[0];
    console.log(`   built: ${made.cal.key} ${JSON.stringify(made.cal.jobs.map(j => [j.face, j.state, j.text, j.fitted]))}`);
    // (the front approved first, as Paul's was: the save sends it back to be fitted again and approved again)
    const sheets = await page.evaluate(() => { const out = []; for (const s of allSheets()) if (s.charms && s.charms.length) { if (!s.folderPath) s.folderPath = 'charmnest/sheets/' + s.sheetId; out.push({ sheetId: s.sheetId, poolIds: s.charms.map(x => x.poolId) }); } return out; });
    for (const s of sheets) st.put('Charm_Nest_Sheets', s.sheetId, { sheetId: s.sheetId, poolIds: s.poolIds, backPool: [] });
    await R.openEngraveDetail(w, 'cal', { face: 'front' }); await R.settle(page, 700);
    await page.click(Q + ' [data-a="approve"]', { timeout: 5000 }).catch(() => {});
    const approved = await until(() => !!st.doc('Charm_Pool_Back', PID + '~front'), 120000);
    await R.openEngraveDetail(w, 'cal', { face: 'front' }); await R.settle(page, 900);
    // (what a Front box shows: its words, their place, its drawing; and the same preview drawn fresh from the job as saved now, on a copy of its view: nothing remembered of an older drawing in it)
    await page.evaluate(() => {
      const jobOf = pid => [...Engrave.items().values()].find(x => x.face === 'front' && (x.copies || []).includes(pid));
      window.__ef = {
        jobOf,
        thumb: el => { const s = el && el._engpv, t = el && el.querySelector('.engpvText'), cv = el && el.querySelector('.engpvDraw canvas'); return s ? { text: t && !t.hidden ? t.textContent : '', style: t ? t.getAttribute('style') : '', canvas: cv ? cv.toDataURL() : '', ring: !!el.querySelector(':scope>.cnPicSpin') } : null; },
        job: pid => { const j = jobOf(pid); return j ? { key: j.key, state: j.state, centre: j.fit && j.fit.centre, size: j.fit && j.fit.size, lines: j.fit && j.fit.lines, nudged: !!j.nudged } : null; },
        fresh: (el, pid) => {
          const s = el._engpv, j = jobOf(pid); if (!s || !j || !j.fit || !j.view) return null;
          const view = JSON.parse(JSON.stringify(j.view, (k, v) => (v instanceof Map || typeof v === 'function' ? undefined : v)));
          const job2 = Object.assign(Object.create(Object.getPrototypeOf(j)), j, { view, fit: Object.assign({}, j.fit) });
          const r = el.getBoundingClientRect(), box = document.createElement('button'); box.className = 'engThumb'; box.id = 'efFresh';
          box.style.cssText = `position:fixed;left:${r.left}px;top:${Math.min(innerHeight - r.height - 4, r.bottom + 12)}px;width:${el.offsetWidth}px;height:${el.offsetHeight}px;max-width:none;z-index:2147483000`; document.body.appendChild(box);
          const P = CharmNestEngravePreview; P.render(box, Object.assign({}, s.opts, { job: job2, position: P.positionOf(job2, { row: s.opts.row, slot: s.opts.slot }) }));
          return __ef.thumb(box);
        }
      };
    });
    const before = await page.evaluate(([sel, pid]) => ({ th: __ef.thumb(document.querySelector(sel)), job: __ef.job(pid) }), [TH, PID]);
    // the Charm Editor from the Front card: a "21" in a day square, the card's words moved up to the top of the calendar
    await page.$eval(Q + ' .egCharmEdit', b => b.click());
    await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }); await settleP(page); await settleP(page);
    const lines = await page.evaluate(() => CharmEditor.current.items().filter(i => i.role === 'line').map(i => CharmEditor.current.box(i.id)));
    const u = lines.reduce((a, b) => [Math.min(a[0], b[0]), Math.min(a[1], b[1]), Math.max(a[2], b[2]), Math.max(a[3], b[3])]);
    const at = await page.evaluate(([x, y]) => CharmEditor.current.clientOf(x, y), [u[0] + (u[2] - u[0]) * (1.3 / 7), u[1] + (u[3] - u[1]) * 0.75]);
    await page.keyboard.press('v'); await settleP(page); await page.keyboard.press('t');
    await page.mouse.click(at.x, at.y); await settleP(page);
    await page.keyboard.type('21'); await page.keyboard.press('Enter'); await settleP(page);
    await page.keyboard.press('Escape'); await settleP(page);
    const mv = await page.evaluate(() => {
      const E = CharmEditor.current; let it = null; const walk = l => l.forEach(i => { if (i.kind === 'text' && i.source === 'engrave') it = it || i; if (i.children) walk(i.children); }); walk(E.items('front'));
      if (!it) return null; const b0 = E.box(it.id), f = E.frame();
      E.ctx.replaceItems({ [it.id]: Object.assign({}, it, { ty: it.ty - (b0[1] - (f.y0 + f.hPt * 0.12)) }) }); E.ctx.commit('move');
      return { id: it.id, b0, b1: E.box(it.id) };
    });
    // every frame from the press of Save: the Front box's words' place and its ring, the job's fit, the work area
    await page.evaluate(([sel, q, pid]) => {
      const L = window.__fr = { frames: [], stop: false }, now = () => performance.now();
      const probe = document.createElement('div'); probe.style.cssText = 'position:fixed;left:0;top:0;width:1px;height:1px;opacity:0;pointer-events:none'; document.body.appendChild(probe);
      new ResizeObserver(() => {
        if (L.stop) return; const b = document.querySelector(sel), t = b && b.querySelector('.engpvText'), j = __ef.jobOf(pid), card = document.querySelector(q), cv = card && card.querySelector('.backHost canvas');
        L.frames.push({ t: now(), top: t && !t.hidden ? t.style.top : '', ring: !!(b && b.querySelector(':scope>.cnPicSpin')), fit: !!(j && j.fit), cy: j && j.fit ? j.fit.centre[1] : null, work: cv ? (cv.classList.contains('egHeld') ? 'held' : 'real') : 'none' });
      }).observe(probe);
      let flip = false; const tick = () => { flip = !flip; probe.style.width = flip ? '2px' : '1px'; if (!L.stop) requestAnimationFrame(tick); }; requestAnimationFrame(tick);
      document.querySelector('.ce-root [data-act="save"]').addEventListener('click', () => { L.t0 = now(); }, { capture: true, once: true });
    }, [TH, Q, PID]);
    await page.click('.ce-root [data-act="save"]');
    await until(() => page.evaluate(() => !document.querySelector('.ce-root')), 30000);
    const j1 = await until(() => page.evaluate(([pid, c0]) => { const j = __ef.job(pid); return j && j.centre && j.nudged && Math.abs(j.centre[1] - c0[1]) > 1 ? j : null; }, [PID, before.job.centre]), 60000);
    await page.waitForFunction(q => { const card = document.querySelector(q), cv = card && card.querySelector('.backHost canvas'); return cv && !cv.classList.contains('egHeld') && !card.querySelector('.egRefitCue') && !document.querySelector(q + ' .cnPicSpin'); }, Q, { timeout: 60000 }).catch(() => {});
    await R.settle(page, 600);
    const fr = await page.evaluate(() => { __fr.stop = true; return { t0: __fr.t0, frames: __fr.frames.filter(f => f.t >= __fr.t0) }; });
    const now1 = await page.evaluate(([sel, pid]) => ({ th: __ef.thumb(document.querySelector(sel)), fresh: __ef.fresh(document.querySelector(sel), pid) }), [TH, PID]);
    // pixel for pixel: the Front box and the fresh drawing beside it, each as the screen shows it
    const shotA = await page.locator(TH).first().screenshot(), shotB = await page.locator('#efFresh').screenshot();
    const px = await page.evaluate(async ([a, b]) => {
      const load = src => new Promise(res => { const i = new Image(); i.onload = () => res(i); i.onerror = () => res(null); i.src = 'data:image/png;base64,' + src; });
      const [A, Bm] = await Promise.all([load(a), load(b)]); if (!A || !Bm) return { err: 'no image' };
      const W = Math.min(A.width, Bm.width), H = Math.min(A.height, Bm.height), data = im => { const c = document.createElement('canvas'); c.width = W; c.height = H; const g = c.getContext('2d'); g.drawImage(im, 0, 0); return g.getImageData(0, 0, W, H).data; };
      const da = data(A), db = data(Bm); let diff = 0, ink = 0;
      for (let k = 0; k < da.length; k += 4) { const x = (k / 4) % W, y = Math.floor(k / 4 / W); if (x < 3 || y < 3 || x >= W - 3 || y >= H - 3) continue;   // (the box's own edge: its border and the gold ring of the face being edited)
        const d = Math.max(Math.abs(da[k] - db[k]), Math.abs(da[k + 1] - db[k + 1]), Math.abs(da[k + 2] - db[k + 2])); if (d > 40) diff++; if (db[k] + db[k + 1] + db[k + 2] < 600) ink++; }
      return { w: A.width, h: A.height, w2: Bm.width, h2: Bm.height, diff, ink, share: +(diff / ((W - 6) * (H - 6))).toFixed(5) };
    }, [shotA.toString('base64'), shotB.toString('base64')]);
    await R.shot(w, 'EF-front-box-and-fresh');
    await page.evaluate(() => { const f = document.getElementById('efFresh'); if (f) f.remove(); });
    // the frames: the new placement lands (the job's fit at its new place, the work area real) → the box's words at their new place within a frame; until then the box says Updating
    const land = fr.frames.findIndex(f => f.fit && f.cy != null && Math.abs(f.cy - before.job.centre[1]) > 1 && f.work === 'real');
    const topNow = now1.th && /top:\s*([\d.]+)%/.exec(now1.th.style), topOld = before.th && /top:\s*([\d.]+)%/.exec(before.th.style);
    const shown = topNow ? fr.frames.findIndex((f, i) => f.top === topNow[1] + '%' && fr.frames.slice(i).every(g => g.top === f.top)) : -1;
    const oldNoRing = fr.frames.slice(0, shown < 0 ? fr.frames.length : shown).filter(f => !f.ring).length;
    console.log(`   approved first: ${approved}; the words ${topOld && topOld[1]}% from the top before, ${topNow && topNow[1]}% now (the fit's centre ${before.job.centre[1].toFixed(2)} → ${j1 ? j1.centre[1].toFixed(2) : '-'} pt); landed at frame ${land}, in the box from frame ${shown}`);
    check(`E1 after the Charm Editor's save (a "21", the words moved up ${mv ? (mv.b0[1] - mv.b1[1]).toFixed(1) : '-'} pt) and the refit, the card's Front box is a fresh drawing of the saved state: the same words at the same place (${!!now1.fresh && now1.fresh.style === now1.th.style}), the same drawing (${!!now1.fresh && now1.fresh.canvas === now1.th.canvas}), ${px.diff} of ${px.w * px.h} pixels apart on screen inside its edge`,
      !!mv && !!j1 && !!now1.fresh && now1.th.text === now1.fresh.text && now1.th.style === now1.fresh.style && now1.th.canvas === now1.fresh.canvas && now1.th.canvas !== before.th.canvas && px.w === px.w2 && px.share < 0.003 && px.ink > 200,
      JSON.stringify({ mv, j1, th: now1.th && now1.th.style, fresh: now1.fresh && now1.fresh.style, px }));
    check(`E2 the words are where they were put in the box (${topOld && topOld[1]}% → ${topNow && topNow[1]}% from the top), within a frame of the new placement landing (landed at frame ${land}, in the box from frame ${shown}); every frame before shows the Updating ring (${oldNoRing} without)`,
      !!topNow && !!topOld && +topNow[1] < +topOld[1] - 10 && land >= 0 && shown >= 0 && shown <= land + 1 && oldNoRing === 0, JSON.stringify({ land, shown, oldNoRing, frames: fr.frames.slice(Math.max(0, Math.min(land, shown) - 3), Math.max(land, shown) + 3) }));

    // ── the other pictures of the piece: its Orders row's box and the order window ──
    await page.evaluate(() => { CN.setMode('orders'); Orders.render(); });
    const rowBox = `[data-key="${made.cal.key}"] [data-vector]`;
    await page.locator(rowBox).first().scrollIntoViewIfNeeded().catch(() => {});
    const e3 = await until(() => page.evaluate(([sel, pid]) => { const h = document.querySelector(sel), im = h && h.querySelector(':scope>img'), pic = h && CharmNestMadeThumb.pictureOf(pid, null, h); return im && im.complete && pic && im.src === pic.url && !h.querySelector(':scope>.cnPicSpin') ? { ok: true, sig: CharmNestMadeThumb.sigOf(pid) } : null; }, [rowBox, PID]), 20000);
    check(`E3 the Orders row's box shows the piece as it is saved now (the drawing of its current state, ${e3 ? e3.sig : '-'}), no ring left`, !!e3, JSON.stringify(e3));
    await R.openOrderWindow(w, 'cal'); await R.settle(page, 900);
    let e4last = null;
    const e4 = await until(() => page.evaluate(([pid]) => {
      const v = document.getElementById('owVector'), im = v && v.querySelector(':scope>img'), pic = v && CharmNestMadeThumb.pictureOf(pid, null, v), th = document.querySelector('#orderWin .engFace[data-face="front"] .engThumb');
      const a = th && __ef.thumb(th), f = th && __ef.fresh(th, pid); const out = { vec: !!(im && im.complete && pic && im.src === pic.url), card: !!(a && f && a.style === f.style && a.canvas === f.canvas), rings: document.querySelectorAll('#orderWin .cnPicSpin').length, a: a && a.style, f: f && f.style };
      const fx = document.getElementById('efFresh'); if (fx) fx.remove(); window.__e4 = out; return out.vec && out.card && !out.rings ? out : null;
    }, [PID]), 20000);
    if (!e4) e4last = await page.evaluate(() => window.__e4);
    await R.shot(w, 'EF-order-window');
    check('E4 the order window: its Vector design is the piece as saved now, its ENGRAVING card\'s Front box a fresh drawing of the saved state, no ring left', !!e4, JSON.stringify(e4 || e4last));
    // (E6: with the order window open, the piece's front art changed (a square, saved as the Charm Editor saves it): its pictures follow once the face is fitted again, without the window being opened again)
    const c6 = await page.evaluate(pid => { const th = document.querySelector('#orderWin .engFace[data-face="front"] .engThumb'); return th ? __ef.thumb(th).canvas : ''; }, PID);
    await page.evaluate(async pid => {
      const charm = Pool.charmOf(pid), bb = charm.outline.bbox, cx = bb[0] + (bb[2] - bb[0]) * 0.75, cy = bb[1] + (bb[3] - bb[1]) * 0.25, s = 1.2;
      const sq = [[['m', [cx - s, -(cy - s)]], ['l', [cx + s, -(cy - s)]], ['l', [cx + s, -(cy + s)]], ['l', [cx - s, -(cy + s)]], ['h']]];
      await CharmNestArtEdit.save(pid, 'front', { added: [{ id: 'efSq', kind: 'path', role: 'hatch', subpaths: sq }] }, { view: { M: [1, 0, 0, 1, 0, 0], R: [1, 0, 0, 1, 0, 0] }, by: 'Test Operator' });
    }, PID);
    let e6last = null;
    const e6 = await until(() => page.evaluate(([pid, was]) => {
      const j = __ef.jobOf(pid), th = document.querySelector('#orderWin .engFace[data-face="front"] .engThumb'), v = document.getElementById('owVector'), im = v && v.querySelector(':scope>img'), pic = v && CharmNestMadeThumb.pictureOf(pid, null, v);
      if (!j || !j.fit || !th) return null;
      const a = __ef.thumb(th), f = __ef.fresh(th, pid), fx = document.getElementById('efFresh'); if (fx) fx.remove();
      const out = { moved: a.canvas !== was, same: !!(f && a.style === f.style && a.canvas === f.canvas), vec: !!(im && im.complete && pic && im.src === pic.url), rings: document.querySelectorAll('#orderWin .cnPicSpin').length }; window.__e6 = out;
      return out.moved && out.same && out.vec && !out.rings ? out : null;
    }, [PID, c6]), 20000);
    if (!e6) e6last = await page.evaluate(() => window.__e6);
    check('E6 with the order window open, a save of the piece\'s front art: its ENGRAVING card\'s Front box and its Vector design follow once the face is fitted again (a fresh drawing of the saved state), no ring left', !!e6, JSON.stringify(e6 || e6last));
    check('E5 nothing on the page erred', !w.errors.length, JSON.stringify(w.errors.slice(0, 3)));
  } catch (e) { check('the test ran', false, e && e.stack); }
  finally { await w.close(); }
  const failed = RESULTS.filter(r => !r.ok);
  console.log(`\nedit-thumbs-follow: ${RESULTS.length - failed.length}/${RESULTS.length} passed`);
  process.exit(failed.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
