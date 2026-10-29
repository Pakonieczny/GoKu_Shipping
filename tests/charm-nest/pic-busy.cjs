// PICBUSY (Paul, 28 Oct 2026: "when these thumbnails are being updated with the charm editor updates, there is no loading spinners to tell the user and give the user feedback that something is
// about to change or do the user is waiting on the system to update the changes. There must be feedback spinners everywhere. The user has to wait for the system to update."). charm-nest-pic-busy.js
// (one quiet gold ring, "Updating…", role=status, aria-busy on the box, the picture kept underneath, dimmed) put on and taken off by the work itself: CharmNestMadeThumb.expect / busySweep, ListMedia's
// soft watch, paintInto, CNEngravingSeals.wirePanel, the Engrave card. On the real sorter page over the repo's fake site (_engraveRealWorld.cjs: real drawings, real fits, offline, made-up names).
//   A  the Orders row of the mini tag: the moment a save of the piece is known (CharmNestMadeThumb.expect, as the Charm Editor's Save and a save landing call it) the row's box shows the ring in the
//      same frame, keeps its picture (dimmed, never blank), and every frame until its new picture is painted has the ring; the ring goes within two frames of the new picture
//   B  the real Charm Editor over the Engrave card (a "7" written on the front, Save): from the first frame after the press until the new drawing is in, the work area says it waits (the ring, or
//      the "Fitting…" cue), never both rings in one corner; the card's work area and every box of the piece end with no ring
//   C  the order window: its Vector design, its sheet panel picture and its ENGRAVING card's picture say Updating at once when a save is known, and stop when the new drawing is in
//   D  a picture that fails shows its fallback ("Unavailable · Retry") and drops the ring
//   E  ten seconds after the last change no ring is left anywhere, no box is aria-busy for it
//   F  prefers-reduced-motion: the ring stands still (animation none), else it turns; the Charm Editor's Save reads "Saving…" with the same ring while its check runs, then "Save" again
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/pic-busy.cjs      (SHOTS=<dir> for the screenshots)
'use strict';
const path = require('node:path');
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : evidence || ''}`.trimEnd()); };
const wait = ms => new Promise(r => setTimeout(r, ms));
const role = page => page.waitForFunction(() => !window.CNRole || ['ask', 'role', 'admin'].includes(CNRole.state()), null, { timeout: 20000 }).catch(() => {}).then(() => page.evaluate(() => { try { if (window.CNRole && CNRole.state() === 'ask') CNRole.choose('admin'); } catch (_) { /* no roles */ } }));

// (in the page: every frame, what each picture box of the piece shows: its ring, its picture, the Fitting cue; read after the frame's own work, before it is painted)
function recorder(pid) {
  const L = window.__pb = { frames: [], marks: [], stop: false }, now = () => performance.now(), MT = window.CharmNestMadeThumb;
  let n = 0; const idOf = el => el.__pbId || (el.__pbId = (el.id || el.dataset.vector != null && 'vec' || el.dataset.madePids != null && 'pids' || 'box') + ':' + (++n));
  const hosts = () => {
    const out = new Set();
    for (const el of document.querySelectorAll('[data-vector],#owVector')) { const r = el._mvRow; if (!r || !el.isConnected) continue; const own = (MT.host().poolIdsOf ? MT.host().poolIdsOf(r) : r.poolIds) || []; if (own.map(String).includes(pid)) out.add(el); }
    for (const el of document.querySelectorAll('[data-made-pids]')) if (el.dataset.madePids.split(' ').includes(pid)) out.add(el);
    for (const cv of document.querySelectorAll('canvas[data-made-pid]')) if (cv.dataset.madePid === pid && cv.parentElement) out.add(cv.parentElement);
    return [...out];
  };
  const shown = el => { if (!el.offsetParent && getComputedStyle(el).position !== 'fixed') return false; const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
  const snap = () => hosts().filter(shown).map(el => { const im = el.querySelector(':scope>img'), cv = el.querySelector(':scope>canvas'); return { id: idOf(el), why: CNPicBusy.why(el).join(), ring: !!el.querySelector(':scope>.cnPicSpin'), busy: el.getAttribute('aria-busy') === 'true', cue: !!el.querySelector('.egRefitCue'), src: im ? im.src : '', ok: !!(im && im.complete && im.naturalWidth), blank: !im && !cv && !el.querySelector('.engpv,svg,.thumbLoading,.by'), dim: im ? getComputedStyle(im).opacity : '' }; });
  L.snap = snap;
  const probe = document.createElement('div'); probe.style.cssText = 'position:fixed;left:0;top:0;width:1px;height:1px;opacity:0;pointer-events:none'; document.body.appendChild(probe);
  new ResizeObserver(() => { if (!L.stop) L.frames.push({ t: now(), boxes: snap(), held: MT.isBusy([pid]) }); }).observe(probe);
  let flip = false; const tick = () => { flip = !flip; probe.style.width = flip ? '2px' : '1px'; if (!L.stop) requestAnimationFrame(tick); }; requestAnimationFrame(tick);
}

(async () => {
  const R = require('./_engraveRealWorld.cjs');
  const w = await R.world({ shots: process.env.SHOTS || '/tmp/pic-busy-shots' });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  const page = w.page, st = w.srv.st, SHOTS = process.env.SHOTS || '';
  const shot = async (name, sel) => { if (!SHOTS) return; require('node:fs').mkdirSync(SHOTS, { recursive: true }); const file = path.join(SHOTS, name + '.png'); if (sel) await page.locator(sel).first().screenshot({ path: file }).catch(e => console.log('   shot', name, e.message)); else await page.screenshot({ path: file }).catch(() => {}); };
  try {
    await role(page);
    const made = await R.build(w, ['tag']), PID = made.tag.poolIds[0], KEY = made.tag.key, RID = made.tag.rid;
    console.log(`   built: ${KEY} pieces ${JSON.stringify(made.tag.poolIds)} jobs ${JSON.stringify(made.tag.jobs.map(j => [j.face, j.state, j.text, j.fitted]))}`);
    if (!st.doc('Charm_Pool', PID)) st.put('Charm_Pool', PID, { poolId: PID, orderId: RID, sku: '5X8.5', state: 'pooled' });
    await page.evaluate(() => { CN.setMode('orders'); Orders.render(); });
    const box = `[data-key="${KEY}"] [data-vector]`;
    await page.waitForFunction(sel => { const h = document.querySelector(sel); const im = h && h.querySelector('img'); return !!(h && h.dataset.made && im && im.complete && im.naturalWidth > 0); }, box, { timeout: 30000 }).catch(() => {});
    await page.locator(box).first().scrollIntoViewIfNeeded().catch(() => {});
    await R.settle(page, 2500);
    await page.evaluate(recorder, PID);

    // ── A: the Orders row, from the moment a save is known until its new picture is painted ──
    const a = await page.evaluate(async ([pid, sel]) => {
      const h = document.querySelector(sel), was = h.querySelector('img').src;
      CharmNestMadeThumb.expect([pid]);   // (what the Charm Editor's Save does as it is pressed)
      const same = { ring: !!h.querySelector(':scope>.cnPicSpin'), busy: h.getAttribute('aria-busy'), img: !!h.querySelector(':scope>img'), dim: getComputedStyle(h.querySelector(':scope>img')).opacity, role: (h.querySelector(':scope>.cnPicSpin') || {}).getAttribute?.('role'), name: (h.querySelector(':scope>.cnPicSpin') || {}).title };
      window.__pbA = { t0: performance.now(), was };
      return same;
    }, [PID, box]);
    await shot('PICBUSY-orders-row-updating', `[data-key="${KEY}"]`);
    await page.evaluate(async pid => {
      const charm = Pool.charmOf(pid), bb = charm.outline.bbox, cx = (bb[0] + bb[2]) / 2, cy = bb[1] + (bb[3] - bb[1]) * 0.3, s = 1.2;
      const sq = [[['m', [cx - s, -(cy - s)]], ['l', [cx + s, -(cy - s)]], ['l', [cx + s, -(cy + s)]], ['l', [cx - s, -(cy + s)]], ['h']]];
      await CharmNestArtEdit.save(pid, 'front', { added: [{ id: 'pbSq', kind: 'path', role: 'hatch', subpaths: sq }] }, { view: { M: [1, 0, 0, 1, 0, 0], R: [1, 0, 0, 1, 0, 0] }, by: 'Test Operator' });
    }, PID);
    await page.waitForFunction(([sel, was]) => { const im = document.querySelector(sel + ' > img'); return im && im.src !== was && im.complete && !document.querySelector(sel + ' > .cnPicSpin'); }, [box, (await page.evaluate(() => __pbA.was))], { timeout: 25000, polling: 50 }).catch(() => {});
    await R.settle(page, 300);
    const aRun = await page.evaluate(sel => {
      const h = document.querySelector(sel), id = h.__pbId, t0 = __pbA.t0, fr = __pb.frames.filter(f => f.t >= t0).map(f => ({ t: f.t, held: f.held, b: f.boxes.find(x => x.id === id) })).filter(f => f.b);
      const fin = h.querySelector(':scope>img') ? h.querySelector(':scope>img').src : '', iNew = fr.findIndex(f => f.b.src && f.b.src === fin && f.b.ok);   // (the picture it ends with: the new art first, then the words fitted again around it)
      const before = iNew < 0 ? fr : fr.slice(0, iNew), lost = before.filter(f => !f.b.ring), blank = before.filter(f => f.b.blank || !f.b.src);
      // (the piece may still be at work when its picture comes (its words fitted again): the ring goes when both are done)
      const iDone = iNew < 0 ? -1 : iNew + fr.slice(iNew).findIndex(f => !f.held), iGone = iDone < iNew ? -1 : fr.slice(iDone).findIndex(f => !f.b.ring);
      return { trail: fr.slice(Math.max(0, iNew - 2), iNew + 8).map(f => [Math.round(f.t - t0), f.b.why, f.held, f.b.src.slice(-12)]), frames: fr.length, iNew, iDone, newMs: iNew >= 0 ? Math.round(fr[iNew].t - t0) : -1, lost: lost.length, blank: blank.length, goneAfter: iGone, ringNow: !!h.querySelector(':scope>.cnPicSpin'), busyNow: h.getAttribute('aria-busy'), dimFirst: before.length ? before[0].b.dim : '' };
    }, box);
    check(`A1 the save known (expect), the Orders row's box says Updating in the same frame: the gold ring (role ${a.role}, "${a.name}"), aria-busy ${a.busy}, its picture kept underneath, dimmed (opacity ${a.dim})`, a.ring && a.busy === 'true' && a.img && +a.dim < 1 && a.role === 'status' && a.name === 'Updating…', JSON.stringify(a));
    check(`A2 every one of the ${aRun.iNew} frames until the new picture (the words fitted again around the new art) is painted (${aRun.newMs} ms) has the ring and a picture (never blank)`, aRun.iNew > 0 && aRun.lost === 0 && aRun.blank === 0, JSON.stringify(aRun));
    check(`A3 the ring goes with the new picture, once the piece is done (${aRun.goneAfter} frame${aRun.goneAfter === 1 ? '' : 's'} after it; the piece was still at work ${aRun.iDone - aRun.iNew} frames after its picture came) and the box is no longer busy`, aRun.goneAfter >= 0 && aRun.goneAfter <= 2 && !aRun.ringNow && aRun.busyNow !== 'true', JSON.stringify(aRun));

    // ── B: the real Charm Editor over the Engrave card ──
    const Q = '#egQueue .rvItem[data-kind=placement]';
    await R.openEngraveDetail(w, 'tag', { face: 'front' }); await R.settle(page, 1200);
    await page.click(Q + ' .egCharmEdit');
    await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }); await R.settle(page, 400);
    const cut = await page.evaluate(() => { const its = CharmEditor.current.items().filter(i => i.role === 'cut'); const bs = its.map(i => CharmEditor.current.box(i.id)); return bs.reduce((a, b) => [Math.min(a[0], b[0]), Math.min(a[1], b[1]), Math.max(a[2], b[2]), Math.max(a[3], b[3])]); });
    const at = await page.evaluate(([x, y]) => CharmEditor.current.clientOf(x, y), [cut[0] + (cut[2] - cut[0]) * 0.2, cut[1] + (cut[3] - cut[1]) * 0.5]);
    await page.keyboard.press('v'); await R.settle(page, 100); await page.keyboard.press('t');
    await page.mouse.click(at.x, at.y); await R.settle(page, 200);
    await page.keyboard.type('7'); await page.keyboard.press('Enter'); await R.settle(page, 300);
    await page.evaluate(() => { const b = document.querySelector('.ce-root [data-act="save"]'); b.addEventListener('click', () => { window.__pbB = { t0: performance.now() }; }, { capture: true, once: true }); });
    await page.click('.ce-root [data-act="save"]');
    // (until the card's work area shows the new drawing and nothing of the piece says Updating any more, then a few frames more)
    await page.waitForFunction(q => { const card = document.querySelector(q), cv = card && card.querySelector('.backHost canvas'); return window.__pbB && cv && !cv.classList.contains('egHeld') && !card.querySelector('.egRefitCue') && !document.querySelector('.cnPicSpin') && cv._frontArt && cv._frontArt.some(s => s.artAdded); }, Q, { timeout: 60000, polling: 100 }).catch(() => {});
    await R.settle(page, 400);
    const b = await page.evaluate(q => {
      const t0 = __pbB.t0, fr = __pb.frames.filter(f => f.t >= t0), card = document.querySelector(q), host = card && card.querySelector('.backHost'), id = host && host.__pbId;
      const first = fr[0], wa = fr.map(f => f.boxes.find(x => x.id === id)).filter(Boolean);
      const done = wa.findIndex(x => !x.ring && !x.cue && x.id), says = wa.slice(0, Math.max(0, done)).every(x => x.ring || x.cue);
      const ringsAtFirst = first ? first.boxes.map(x => [x.id, x.ring || x.cue]) : [];
      const cueAndRingShown = [...document.querySelectorAll('.backHost')].some(h => h.querySelector('.egRefitCue') && h.querySelector(':scope>.cnPicSpin') && getComputedStyle(h.querySelector(':scope>.cnPicSpin')).display !== 'none');
      return { frames: fr.length, ringsAtFirst, first: first ? Math.round(first.t - t0) : -1, waFrames: wa.length, says, rings: document.querySelectorAll('.cnPicSpin').length, cueAndRingShown };
    }, Q).catch(e => ({ err: e.message }));
    check(`B1 the first frame after the Charm Editor's Save is pressed (${b.first} ms) every box of the piece on screen says it waits (${JSON.stringify(b.ringsAtFirst)})`, b.ringsAtFirst && b.ringsAtFirst.length > 0 && b.ringsAtFirst.every(x => x[1]), JSON.stringify(b));
    check(`B2 the work area says it waits (the ring, or "Fitting…") every frame until its new drawing is in (${b.waFrames} frames), never two marks in one corner; then no ring is left (${b.rings})`, b.says && !b.cueAndRingShown && b.rings === 0, JSON.stringify(b));
    await shot('PICBUSY-engrave-card-after', Q);

    // ── C: the order window ──
    await R.openOrderWindow(w, 'tag'); await R.settle(page, 1200);
    // (a piece's canvas painted as made, as the order window's sheet panel and the sheet window's piece paint theirs: CharmNestMadeThumb.paintInto)
    const cv0 = await page.evaluate(async pid => {
      const d = document.createElement('div'); d.id = 'pbCanvasBox'; d.style.cssText = 'position:fixed;left:8px;bottom:8px;width:184px;height:184px;z-index:2147483000;background:#fff'; const cv = document.createElement('canvas'); cv.width = cv.height = 184; d.appendChild(cv); document.body.appendChild(d);
      const p = CharmNestMadeThumb.paintInto(cv, pid), during = !!d.querySelector(':scope>.cnPicSpin'); const ok = await p; await new Promise(r => requestAnimationFrame(r));
      return { during, ok, after: !!d.querySelector(':scope>.cnPicSpin'), key: cv._madeKey };
    }, PID);
    const c0 = await page.evaluate(pid => { CharmNestMadeThumb.expect([pid]); return __pb.snap().map(x => [x.id, x.ring]); }, PID);
    await shot('PICBUSY-order-window-updating', '#orderWin');
    const ow = await page.evaluate(() => ({ vec: !!document.querySelector('#owVector > .cnPicSpin'), pic: !!document.querySelector('#orderWin .cnPicBusy > canvas[data-made-pid]'), eng: !!document.querySelector('#orderWin [data-engraving-preview].cnPicBusy, #orderWin .engThumb.cnPicBusy'),
      seen: [...document.querySelectorAll('#orderWin [data-made-pids], #orderWin canvas[data-made-pid], #orderWin [data-engraving-preview], #orderWin .cnPicBusy')].map(e => e.tagName + '.' + String(e.className).replace(/\s+/g, '.') + (e.dataset.madePids ? '[pids]' : '') + (e.dataset.madePid ? '[pid]' : '') + (e.querySelector(':scope>.cnPicSpin') ? '*' : '')) }));
    console.log('   order window boxes: ' + JSON.stringify(ow.seen));
    await page.evaluate(async pid => {
      const charm = Pool.charmOf(pid), bb = charm.outline.bbox, cx = (bb[0] + bb[2]) / 2, cy = bb[1] + (bb[3] - bb[1]) * 0.7, s = 1;
      const sq = [[['m', [cx - s, -(cy - s)]], ['l', [cx + s, -(cy - s)]], ['l', [cx + s, -(cy + s)]], ['l', [cx - s, -(cy + s)]], ['h']]];
      await CharmNestArtEdit.save(pid, 'front', { added: [{ id: 'pbSq2', kind: 'path', role: 'hatch', subpaths: sq }] }, { view: { M: [1, 0, 0, 1, 0, 0], R: [1, 0, 0, 1, 0, 0] }, by: 'Test Operator' });
    }, PID);
    await page.waitForFunction(() => !document.querySelector('#orderWin .cnPicSpin'), null, { timeout: 30000, polling: 100 }).catch(() => {});
    await page.waitForFunction(() => !document.querySelector('#pbCanvasBox > .cnPicSpin'), null, { timeout: 30000, polling: 100 }).catch(() => {});
    const cv1 = await page.evaluate(() => { const d = document.getElementById('pbCanvasBox'), cv = d.querySelector('canvas'), out = { ring: !!d.querySelector(':scope>.cnPicSpin'), key: cv._madeKey, made: cv.dataset.made }; d.remove(); return out; });
    check(`C3 a piece's canvas painted as made (the sheet panel's, the sheet window's): the ring while it is painted (${cv0.during}), none after (${!cv0.after}); a save known: the ring (${c0.some(x => /^pbCanvasBox/.test(x[0]) && x[1])}); then painted again with the new drawing (${cv1.key !== cv0.key}) and no ring (${!cv1.ring})`,
      cv0.during && cv0.ok && !cv0.after && c0.some(x => /^pbCanvasBox/.test(x[0]) && x[1]) && cv1.key !== cv0.key && cv1.made === '1' && !cv1.ring, JSON.stringify({ cv0, cv1, c0 }));
    const c1 = await page.evaluate(() => ({ rings: document.querySelectorAll('#orderWin .cnPicSpin').length, vecImg: !!document.querySelector('#owVector > img'), busy: [...document.querySelectorAll('#orderWin [aria-busy="true"]')].filter(e => !e.querySelector('.thumbLoading')).length }));
    check(`C1 the order window: a save known, its Vector design (${ow.vec}), and its ENGRAVING card's Front/Back pictures (${ow.eng}) say Updating at once (${JSON.stringify(c0)})`, ow.vec && ow.eng && c0.length >= 2 && c0.every(x => x[1]), JSON.stringify({ ow, c0 }));
    check(`C2 once the new drawing is in, no ring is left in the order window (${c1.rings}), its Vector design shows a picture, nothing in it busy (${c1.busy})`, c1.rings === 0 && c1.vecImg && c1.busy === 0, JSON.stringify(c1));
    await page.evaluate(() => { try { OrderWin.close(); } catch (_) { /* closed */ } CN.setMode('orders'); Orders.render(); });
    await R.settle(page, 1500);

    // ── D: a picture that fails ──
    // (the piece's picture cannot be had: the box falls back to the Vector design, as it always did, and the ring goes)
    const d = await page.evaluate(async ([sel, pid]) => {
      const h = document.querySelector(sel), MT = CharmNestMadeThumb, real = MT.forLine;
      MT.forLine = function (row) { const f = real.apply(this, arguments); return f ? Object.assign({}, f, { key: 'made:test-fail', load: () => new Promise((_, no) => setTimeout(() => no(new Error('gone')), 400)) }) : f; };
      MT.host().refresh([pid]);
      const during = !!h.querySelector(':scope>.cnPicSpin'), was = (h.querySelector(':scope>img') || {}).src;
      let im = null; for (let i = 0; i < 60; i++) { await new Promise(r => setTimeout(r, 100)); im = h.querySelector(':scope>img'); if (im && im.src !== was && im.complete && !h.querySelector(':scope>.cnPicSpin')) break; }
      const out = { during, after: !!h.querySelector(':scope>.cnPicSpin'), fallback: !!(im && im.src !== was && im.naturalWidth), busy: h.getAttribute('aria-busy') };
      MT.forLine = real; MT.host().refresh([pid]);
      return out;
    }, [box, PID]);
    check(`D1 a picture that fails: the ring while it is asked for (${d.during}), then the Vector design in its place (${d.fallback}) and no ring, not busy`, d.during && !d.after && d.fallback && d.busy !== 'true', JSON.stringify(d));

    // ── E: ten seconds idle ──
    await wait(10000);
    const e = await page.evaluate(() => ({ rings: document.querySelectorAll('.cnPicSpin').length, count: CNPicBusy.count(), busy: [...document.querySelectorAll('.cnPicBusy')].length, which: [...document.querySelectorAll('.cnPicBusy')].map(e => e.tagName + '#' + e.id + '.' + String(e.className).replace(/\s+/g, '.') + ' in ' + (e.closest('[id]') || {}).id + ' shown ' + !!e.getClientRects().length) }));
    check(`E1 ten seconds after the last change no ring is left anywhere (${e.rings}), none owed (${e.count}), no box marked busy by it (${e.busy})`, e.rings === 0 && e.count === 0 && e.busy === 0, JSON.stringify(e));

    // ── F: reduced motion; the Charm Editor's Save while it is checked ──
    const anim = async () => page.evaluate(sel => { const h = document.querySelector(sel); CNPicBusy.on(h, 'test'); const i = h.querySelector(':scope>.cnPicSpin>i'), n = getComputedStyle(i).animationName; CNPicBusy.off(h, 'test'); return n; }, box);
    await page.emulateMedia({ reducedMotion: 'reduce' }); const still = await anim();
    await page.emulateMedia({ reducedMotion: 'no-preference' }); const turns = await anim();
    check(`F1 prefers-reduced-motion: the ring stands still (animation ${still}); otherwise it turns (${turns})`, still === 'none' && turns === 'cnPicSpin', JSON.stringify({ still, turns }));
    const f2 = await page.evaluate(async () => {
      const PT = 72 / 25.4; let end; const slow = new Promise(r => { end = r; });
      const run = CharmEditor.open({ title: 'PICBUSY', faces: { front: { face: 'front', unitPt: true, frame: { x0: 0, y0: 0, wPt: 15 * PT, hPt: 15 * PT }, items: [{ id: 'h1', kind: 'path', role: 'cut', subpaths: CharmEditor.model.ellipseSP(7.5 * PT, 7.5 * PT, 6 * PT, 6 * PT) }] } }, lockRoles: [], check: () => slow.then(() => null) });
      for (let i = 0; i < 50 && !document.querySelector('.ce-root [data-act="save"]'); i++) await new Promise(r => setTimeout(r, 50));
      const sb = document.querySelector('.ce-root [data-act="save"]'), was = sb.textContent.trim(); sb.click();
      await new Promise(r => requestAnimationFrame(r));
      const during = { text: sb.textContent.trim(), ring: !!sb.querySelector('.cnPicSpinInline'), busy: sb.getAttribute('aria-busy'), disabled: sb.disabled };
      end(); await Promise.race([run, new Promise(r => setTimeout(r, 1500))]);
      return { was, during, after: sb.isConnected ? sb.textContent.trim() : '(closed)', ring: !!document.querySelector('.cnPicSpinInline') };
    });
    check(`F2 the Charm Editor's Save reads "${f2.during.text}" with the ring, busy and disabled, while its save is checked; then "${f2.after}"`, f2.during.text === 'Saving…' && f2.during.ring && f2.during.busy === 'true' && f2.during.disabled && !f2.ring && (f2.after === '(closed)' || f2.after === f2.was), JSON.stringify(f2));
    check('G1 nothing on the page erred', !w.errors.length, JSON.stringify(w.errors.slice(0, 3)));
  } catch (e) { check('the test ran', false, e && e.stack); }
  finally { await w.close(); }
  const failed = RESULTS.filter(r => !r.ok);
  console.log(`\npic-busy: ${RESULTS.length - failed.length}/${RESULTS.length} passed`);
  process.exit(failed.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
