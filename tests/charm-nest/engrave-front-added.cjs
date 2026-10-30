// ENGFRONT (13 Oct 2026): a face added to a charm AFTER intake (the front engraving added to a charm that already has its back engraving) is classified and fitted at once,
// so the Front thumbnail draws the front of the charm with the words, and every press on a thumbnail shows something true.
//
// Paul: "I added the front engraving on this order, but when I click the "Front" engraving thumbnail the preview work area goes blank and does not actually load the front of the charm."
// (the work area read "Processing stopped before these words were read.": the front job existed, in state "classify", and nothing ever read or fitted it: CharmNestEngraveSetup.apply
// leaves a job in that state for the run, and a line outside a run's pass is never picked up.)
//
// Offline only: the real page over the fake site with REAL drawings (tests/charm-nest/_engraveRealWorld.cjs). No paid reading (the fake site's stub, COUNTED: the test fails if the Save,
// the fit or a thumbnail press asks it again), no Etsy, nothing real. Made-up names only; "B" and "Name One" are plain test strings.
//
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/engrave-front-added.cjs      (CHROMIUM=/opt/pw-browsers/chromium if the default path is missing; SHOTS=<dir> for screenshots)
//   Exit code 1 while any check is red; every line is PASS|FAIL id evidence.
const R = require('./_engraveRealWorld.cjs');
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${evidence}`); };

// the fake site logs every model call it answers; the ones asked after the world is built are the ones this flow made
let modelCalls = 0, counting = false; const realLog = console.log;
console.log = (...a) => { if (counting && /\[charmEngrave\].*(engraveIntent|engraveReview|labelRead|packing|grouping|layout)/.test(String(a[0]))) modelCalls++; return realLog.apply(console, a); };

const Q = '#egQueue .rvItem[data-kind=placement]';
/** what the Engrave detail's work area shows right now */
const workFacts = page => page.evaluate(Q => {
  const vis = e => { if (!e) return false; const b = e.getBoundingClientRect(), st = getComputedStyle(e); return b.width > 0 && b.height > 0 && st.visibility !== 'hidden' && st.display !== 'none' && !e.closest('[hidden]'); };
  const c = document.querySelector(Q); if (!c) return { open: false, tab: (document.querySelector('#engraveView .egTab.on') || {}).textContent || '', list: !!document.querySelector('#egQueue .egPlacementList'), old: document.querySelectorAll('#egDone, .doneRow.open, .decidedRow').length };
  const host = c.querySelector('.backHost'), cv = host && host.querySelector('canvas'), faces = [...c.querySelectorAll('.engCard .engFace')].filter(vis).map(f => ({ face: f.dataset.face, current: f.hasAttribute('aria-current') }));
  return { open: true, job: c.dataset.key, state: c.dataset.state, canvas: !!(cv && vis(cv)), text: host ? (() => { const h = host.cloneNode(true); h.querySelectorAll('.egZoom, .egZoomMenu').forEach(n => n.remove()); return h.textContent.replace(/\s+/g, ' ').trim().slice(0, 160); })() : '',   // (ENGVIEW's zoom control is not a message) spin: !!(host && host.querySelector('.spin')),
    approve: !!(c.querySelector('[data-a="approve"]') && vis(c.querySelector('[data-a="approve"]'))), faces: faces.map(f => f.face).join('+'), current: (faces.find(f => f.current) || {}).face || null, words: (c.querySelector('.engpWords') || {}).value || '', mode: (c.querySelector('.engp') || { dataset: {} }).dataset.mode || null,
    why: [...c.querySelectorAll('.ctl .why')].map(x => x.textContent.trim()).join(' | '),
    staged: !!(cv && cv._staged), viewFace: cv ? cv._viewFace || null : null, ink: cv ? (() => { try { const g = cv.getContext('2d'), d = g.getImageData(0, 0, cv.width, cv.height).data; let n = 0; for (let i = 0; i < d.length; i += 4) if (d[i] < 60 && d[i + 1] < 60 && d[i + 2] < 60 && d[i + 3] > 200) n++; return n; } catch (_) { return -1; } })() : -1 };
}, Q);
const jobsOf = (page, key) => page.evaluate(key => Engrave.jobsOf(B.orders.byKey.get(key)).map(j => ({ key: j.key, face: j.face === 'front' ? 'front' : 'back', state: j.state, text: j.text, fitted: !!(j.fit && ((j.fit.glyphs && j.fit.glyphs.length) || j.fit.cmds)), hasView: !!j.view, hasMask: !!j.mask, reason: j.reason || '',
  centre: j.fit && j.fit.centre ? j.fit.centre.map(v => Math.round(v * 1000) / 1000) : null, geometry: !!(j.verify && j.verify.geometry && j.verify.geometry.ok), seals: (j.engravingSeals || []).filter(s => s.how === 'engraveApproved').length, by: j.approvedBy || '' })), key);
const press = (page, face) => page.evaluate(({ Q, f }) => { const b = document.querySelector(Q + ' .engCard .engFace[data-face="' + f + '"] .engThumb'); if (b) b.click(); return !!b; }, { Q, f: face });
const until = async (fn, ms) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) return null; await new Promise(r => setTimeout(r, 300)); } };
/** every text the work area shows from now on (any "Processing stopped" is caught however briefly it is on screen) */
const watch = page => page.evaluate(() => {
  window.__seen = []; if (window.__mo) window.__mo.disconnect();
  const note = () => { const h = document.querySelector('#egQueue .rvItem[data-kind=placement] .backHost'); if (h) window.__seen.push(h.textContent.replace(/\s+/g, ' ').trim()); };
  window.__mo = new MutationObserver(note); window.__mo.observe(document.getElementById('engraveView'), { subtree: true, childList: true, characterData: true }); note();
});
const seen = page => page.evaluate(() => (window.__seen || []).filter((t, i, a) => t && a.indexOf(t) === i));

/** the person adds the front in the Engrave detail: Front & Back pressed stages the front beside the back (ENGSTAGE, Paul 30 Oct 2026: four options; the Front option alone would mean "the front only"), the Front
 *  thumbnail makes it the face edited, the front words are typed, and the staged change is kept without an approval (the panel's commit: what Approve does first; no Save, no question) */
const stageFront = page => page.evaluate(Q => { document.querySelector(Q + ' .engp [data-engp="choice"][data-choice="both"]').click(); const t = document.querySelector(Q + ' [data-eng-thumb="front"]') || document.querySelector(Q + ' .engCard .engFace[data-face="front"] .engThumb'); if (t) t.click(); }, Q);
async function addFront(w, name, text) {
  const page = w.page;
  await R.openEngraveDetail(w, name, { face: 'back' });
  await stageFront(page); await R.settle(page, 400);
  await until(() => page.evaluate(Q => { const p = document.querySelector(Q + ' .engp'); return !!p && p.dataset.face === 'front' && !p.querySelector('.engpWords').disabled; }, Q), 15000);
  const staged = await workFacts(page);
  await page.evaluate(({ Q, text }) => { const t = document.querySelector(Q + ' .engpWords'), set = Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value').set; t.focus(); set.call(t, text); t.dispatchEvent(new InputEvent('input', { bubbles: true, inputType: 'insertText', data: text })); t.dispatchEvent(new Event('keyup', { bubbles: true })); }, { Q, text });
  await R.settle(page, 400);
  const mid = await workFacts(page);
  await watch(page);
  await page.evaluate(Q => { const c = document.querySelector(Q); return c && c._pieceMount && c._pieceMount.panel.commit(); }, Q); await R.settle(page, 500);
  return { staged, mid };
}

(async () => {
  const w = await R.world({ shots: process.env.SHOTS || '/tmp/engfront-shots' });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  const page = w.page;
  try {
    R.ORDERS.stageT = Object.assign({}, R.ORDERS.back, { rid: '4290000151', listing: '1890000151', buyer: 'Name Thirteen' }); R.EXPECT_JOBS.stageT = 1;
    const made = await R.build(w, ['back', 'refAnna', 'refMarck', 'both', 'stageT']);
    const key = made.back.key, anna = made.refAnna.key, marck = made.refMarck.key, bothKey = made.both.key;
    counting = true;
    const j00 = made.back.jobs;
    check('A0 the back-only charm starts with one fitted back job and no front job', j00.length === 1 && j00[0].fitted && j00[0].face === 'back', JSON.stringify(j00.map(j => [j.face, j.state, j.fitted])));

    // ═══ P · Paul's flow: back fitted, add the front, press the Front thumbnail ═══
    const { staged, mid } = await addFront(w, 'back', 'B');
    // (STAGEALL, Paul 29 Oct 2026: "it's not approved until the Approve button is pressed again": Front & Back STAGES the front; it is made when the staged change is kept: Approve, or here the panel's commit)
    check('P1 Front & Back and the Front thumbnail stage the front: the Front is marked and edited, nothing is made or saved yet (no front job); Approve is there to keep it',
      staged.current === 'front' && staged.mode === 'both' && !/~front$/.test(staged.job || '') && staged.approve, JSON.stringify({ staged }));
    // (FRONTSTAGE, Paul 29 Oct 2026, "without any explanations": the staged front is SHOWN, never "being prepared": the charm's front, and the words typed fitted on it as a draft)
    check('P1b the work area shows the staged front at once (the charm\'s front, no "being prepared" line)', staged.canvas && staged.staged && staged.viewFace === 'front' && !/being prepared/i.test(staged.text), JSON.stringify(staged));
    check('P2 the front words are typed in the front\'s own box and drawn on the staged front in the work area (more ink than the bare front)', mid.words === 'B' && mid.staged && mid.viewFace === 'front' && mid.ink > staged.ink + 20 && !/being prepared/i.test(mid.text), JSON.stringify({ mid, bare: staged.ink }));
    await R.shot(w, 'FRONT-1-staged-unsaved');
    const fj = await until(async () => { const js = await jobsOf(page, key); const f = js.find(j => j.face === 'front'); return f && f.state === 'review' && f.fitted ? f : null; }, 90000);
    const sawNow = await seen(page);
    check('P3 the front job was made, read from the setup and fitted by itself (words B, view, mask, fit, geometry checked)', !!(fj && fj.text === 'B' && fj.hasView && fj.hasMask && fj.fitted && fj.geometry), JSON.stringify(fj));
    check('P4 "Processing stopped" was never on screen between the Save and the fit', !sawNow.some(t => /Processing stopped/i.test(t)), JSON.stringify(sawNow));
    await press(page, 'front'); await R.settle(page, 500);
    const f1 = await workFacts(page);
    await R.shot(w, 'FRONT-2-pressed');
    check('P5 the Front thumbnail shows the front: the card is the FRONT job, the drawing is there (canvas), no message, the Approve button is there', f1.current === 'front' && (/~front$/.test(f1.job) || f1.viewFace === 'front') && f1.canvas && !f1.text && f1.approve, JSON.stringify(f1));
    const back0 = (await jobsOf(page, key)).find(j => j.face === 'back');
    await press(page, 'back'); await R.settle(page, 500);
    const b1 = await workFacts(page);
    check('P6 the Back thumbnail shows the back again: the BACK job, drawn, its own fit unchanged', b1.current === 'back' && !/~front$/.test(b1.job) && (b1.viewFace === 'back' || !b1.viewFace) && b1.canvas && b1.state === 'review' && JSON.stringify((await jobsOf(page, key)).find(j => j.face === 'back').centre) === JSON.stringify(back0.centre), JSON.stringify(b1));

    // switching back and forth keeps each face's edit: a moved front, a staged (unsaved) back text
    await press(page, 'front'); await R.settle(page, 500);
    const front0 = (await jobsOf(page, key)).find(j => j.face === 'front').centre;
    await page.evaluate(key => { const j = Engrave.jobsOf(B.orders.byKey.get(key)).find(x => x.face === 'front'); Engrave.nudge(j, 0.5, 0); Engrave.render(); }, key);   // (the FRONT job, whichever card is open) await R.settle(page, 700);   // (BIGPANEL: the work area moves the words; the panel has no position buttons)
    const front1 = (await jobsOf(page, key)).find(j => j.face === 'front').centre;
    check('P7 the front is moved on its own (its position changed, the back\'s did not)', JSON.stringify(front1) !== JSON.stringify(front0) && JSON.stringify((await jobsOf(page, key)).find(j => j.face === 'back').centre) === JSON.stringify(back0.centre), JSON.stringify([front0, front1]));
    await press(page, 'back'); await R.settle(page, 500);
    await page.evaluate(Q => { const t = document.querySelector(Q + ' .engpWords'), set = Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value').set; t.focus(); set.call(t, 'Name Two'); t.dispatchEvent(new InputEvent('input', { bubbles: true, inputType: 'insertText', data: 'x' })); t.dispatchEvent(new Event('keyup', { bubbles: true })); }, Q); await R.settle(page, 500);
    await press(page, 'front'); await R.settle(page, 600);
    const f2 = await workFacts(page);
    await press(page, 'back'); await R.settle(page, 600);
    const b2 = await workFacts(page);
    const front2 = (await jobsOf(page, key)).find(j => j.face === 'front').centre;
    check('P8 front, back, front, back: the front keeps its move, the back keeps its unsaved words, both draw (no blank at any press)', JSON.stringify(front2) === JSON.stringify(front1) && b2.words === 'Name Two' && f2.canvas && b2.canvas, JSON.stringify({ front2, f2: [f2.current, f2.canvas, f2.words], b2: [b2.current, b2.canvas, b2.words] }));
    await page.evaluate(() => { CharmNestEngravePanel.DRAFTS.clear(); { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); if (c && c._pieceMount) c._pieceMount.refresh(); else Engrave.render(); } }); await R.settle(page, 500);   // (LIVEWORDS: no Cancel; the unsaved back words let go)

    // ═══ approvals: each face its own, one seal each, permanent; the card follows the other face after each ═══
    await press(page, 'front'); await R.settle(page, 500);
    await page.evaluate(Q => document.querySelector(Q + ' [data-a="approve"]').click(), Q);
    const frontDone = await until(async () => { const f = (await jobsOf(page, key)).find(j => j.face === 'front'); return f && ['approved', 'written'].includes(f.state) ? f : null; }, 60000);
    await R.settle(page, 800);
    const afterF = await jobsOf(page, key);
    await until(async () => (await jobsOf(page, key)).every(j => ['approved', 'written'].includes(j.state)), 60000); await R.settle(page, 800);
    const afterF2 = await jobsOf(page, key); void afterF;
    // (ENGSTAGE, Paul 30 Oct 2026: Approve keeps the PIECE as staged: every engraved face is approved by the one press, Front before Back, each with its own seal)
    check('P9 Approve on the front approves the piece: the front, then the back, one seal each', !!frontDone && afterF2.every(j => ['approved', 'written'].includes(j.state) && j.seals === 1), JSON.stringify(afterF2.map(j => [j.face, j.state, j.seals, j.by])));
    const w1 = await workFacts(page);
    check('P10 after the front is approved the work area is on the other face (the back, drawn), not blank', w1.open && !/~front$/.test(w1.job || '') && w1.canvas, JSON.stringify(w1));
    // the approved front is pressed: it opens as its own card (there is no Decided list: an approved face is the same detailed card as any face, with a calm "Approved" where the Approve was), never a snap back with nothing shown
    await R.openEngraveDetail(w, 'back', { face: 'back' }); await R.settle(page, 500);   // (the approved piece opened again: the tab moved on after its Approve)
    await press(page, 'front'); await R.settle(page, 900);
    const d1 = await workFacts(page);
    await R.shot(w, 'FRONT-3-approved-pressed');
    check('P11 pressing the approved Front thumbnail shows that face (the Front, drawn, approved, no Approve to press, no Decided list)', d1.open && (/~front$/.test(d1.job || '') || d1.viewFace === 'front') && d1.current === 'front' && d1.canvas && !d1.approve && /Placements/.test(await page.evaluate(() => (document.querySelector('#engraveView .egTab.on') || {}).textContent || '')) && await page.evaluate(() => document.querySelectorAll('#egDone, .doneRow.open, .decidedRow, [data-a=reopen]').length === 0), JSON.stringify(d1));
    await press(page, 'back'); await R.settle(page, 700);
    const w2 = await workFacts(page);
    check('P12 the Back thumbnail shows the back again: approved with the same press, drawn, nothing to press', w2.open && w2.current === 'back' && w2.canvas && !w2.approve, JSON.stringify(w2));
    const afterB = await jobsOf(page, key);
    check('P13 both faces approved, each with exactly ONE seal of its own (permanent), by the signed-in name', afterB.every(j => ['approved', 'written'].includes(j.state) && j.seals === 1 && j.by), JSON.stringify(afterB.map(j => [j.face, j.state, j.seals, j.by])));

    // ═══ Q · the back was approved FIRST, the front is added later: the approval stays, the front is prepared ═══
    await page.evaluate(() => Engrave.restoreView({ q: '' }));   // (the Decided landing above put the first order's number in the search box, as EngraveLink does)
    await R.openEngraveDetail(w, 'refAnna', { face: 'back' }); await R.settle(page, 600);
    await page.evaluate(Q => document.querySelector(Q + ' [data-a="approve"]').click(), Q);
    await until(async () => { const b = (await jobsOf(page, anna)).find(j => j.face === 'back'); return b && ['approved', 'written'].includes(b.state) ? b : null; }, 60000);
    const sealsBefore = (await jobsOf(page, anna))[0].seals;
    await watch(page);
    const res = await page.evaluate(async k => { const row = B.orders.byKey.get(k); return await Engrave.saveSetup(row, { mode: 'both', front: { text: 'A' } }); }, anna);
    const qj = await until(async () => { const js = await jobsOf(page, anna); const f = js.find(j => j.face === 'front'); return f && f.state === 'review' && f.fitted ? js : null; }, 90000);
    check('Q1 front added to a charm whose back is already approved: the front is prepared (words A, fitted); the back keeps its approval and its one seal', !!qj && res.ok && qj.find(j => j.face === 'front').text === 'A' && ['approved', 'written'].includes(qj.find(j => j.face === 'back').state) && qj.find(j => j.face === 'back').seals === 1 && sealsBefore === 1, JSON.stringify(qj && qj.map(j => [j.face, j.state, j.fitted, j.seals])));
    check('Q2 "Processing stopped" never showed there either', !(await seen(page)).some(t => /Processing stopped/i.test(t)), JSON.stringify(await seen(page)));

    // ═══ R · Approve both faces on a charm whose front was added later ═══
    await addFront(w, 'refMarck', 'M');
    const rj = await until(async () => { const js = await jobsOf(page, marck); const f = js.find(j => j.face === 'front'); return f && f.state === 'review' && f.fitted ? js : null; }, 90000);
    await R.openEngraveDetail(w, 'refMarck', { face: 'front' }); await R.settle(page, 700);
    const rw = await workFacts(page);
    const both = await page.evaluate(Q => { const b = document.querySelector(Q + ' [data-a="approve"]'); if (b) b.click(); return !!b; }, Q);   // (ENGSTAGE: the one Approve approves both faces)
    await until(async () => (await jobsOf(page, marck)).every(j => ['approved', 'written'].includes(j.state)), 90000);
    await R.settle(page, 800);
    const rb = await jobsOf(page, marck);
    check('R1 a front added later, then the one Approve: the Front is approved, then the Back, one seal each', !!rj && both && rw.canvas && rb.length === 2 && rb.every(j => ['approved', 'written'].includes(j.state) && j.seals === 1), JSON.stringify({ rw: [rw.job, rw.canvas, rw.approve], both, rb: rb.map(j => [j.face, j.state, j.seals]) }));

    // ═══ S · a line read again (the order changed on Etsy: every job of it goes back to "classify", no run is open): opening it prepares it, nothing says "stopped" ═══
    await page.evaluate(() => Engrave.restoreView({ q: '' }));
    await page.evaluate(k => { Engrave.invalidate(B.orders.byKey.get(k), 'order changed'); }, bothKey);
    const s0 = await jobsOf(page, bothKey);
    await watch(page);
    await R.openEngraveDetail(w, 'both', { face: 'front' });
    const s1 = await until(async () => { const js = await jobsOf(page, bothKey); return js.length === 2 && js.every(j => j.state === 'review' && j.fitted) ? js : null; }, 90000);
    await R.settle(page, 500);
    const sw = await workFacts(page), sseen = await seen(page);
    await R.shot(w, 'FRONT-4-reread');
    check('S1 a faced line read again and opened: both faces are read from the setup and fitted by themselves (front W, back Name One), the front is drawn', s0.every(j => j.state === 'classify') && !!s1 && s1.find(j => j.face === 'front').text === 'W' && /NAME|Name/i.test(s1.find(j => j.face === 'back').text) && sw.canvas && /~front$/.test(sw.job || ''), JSON.stringify({ s0: s0.map(j => j.state), s1: s1 && s1.map(j => [j.face, j.state, j.text]), sw: [sw.job, sw.canvas, sw.text] }));
    check('S2 "Processing stopped" never showed there (the work area said Preparing, or drew)', !sseen.some(t => /Processing stopped/i.test(t)), JSON.stringify(sseen));

    // ═══ T · ONEAPPROVE (Paul, 29 Oct 2026: the green Approve is the commit, one press is enough): the staged front, its words dragged left in the preview, then ONE press of A: the front is made, fitted, moved
    //     to where the words were dragged (the work area's own move), and approved; the back keeps its state ═══
    const tKey = made.stageT.key;
    await R.openEngraveDetail(w, 'stageT', { face: 'back' });
    await stageFront(page); await R.settle(page, 400);
    await page.evaluate(Q => { const t = document.querySelector(Q + ' .engpWords'), set = Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value').set; t.focus(); set.call(t, 'T'); t.dispatchEvent(new InputEvent('input', { bubbles: true, inputType: 'insertText', data: 'T' })); }, Q); await R.settle(page, 500);
    const t0 = await workFacts(page);
    const box = await page.evaluate(Q => { const cv = document.querySelector(Q + ' .backHost canvas'), r = cv.getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height / 2, w: r.width }; }, Q);
    await page.mouse.move(box.x, box.y); await page.mouse.down(); for (let k = 1; k <= 6; k++) { await page.mouse.move(box.x - k * box.w * 0.02, box.y, { steps: 1 }); await page.waitForTimeout(30); } await page.mouse.up(); await R.settle(page, 300);
    const tBack0 = (await jobsOf(page, tKey)).find(j => j.face === 'back');
    await page.evaluate(Q => { const c = document.querySelector(Q); if (!c.hasAttribute('tabindex')) c.setAttribute('tabindex', '-1'); c.focus(); }, Q); await page.keyboard.press('a');
    const tf = await until(async () => { const js = await jobsOf(page, tKey), f = js.find(j => j.face === 'front'); return f && ['approved', 'written'].includes(f.state) ? { f, b: js.find(j => j.face === 'back') } : null; }, 120000);
    const placedAt = await page.evaluate(k => { const row = B.orders.byKey.get(k), f = Engrave.jobsOf(row).find(j => j.face === 'front'); return f && f.fit && f.view ? { dx: f.fit.centre[0] - f.view.cx, nudged: !!f.nudged } : null; }, tKey);
    const tBackNow = await until(async () => { const b = (await jobsOf(page, tKey)).find(j => j.face === 'back'); return b && ['approved', 'written'].includes(b.state) ? b : null; }, 60000);
    check('T1 the staged front is shown with its words before Approve; ONE press of A makes it, fits it and approves it, and approves the back with it (ENGSTAGE: Approve keeps the piece)',
      t0.staged && t0.viewFace === 'front' && !!tf && tf.f.text.toUpperCase() === 'T' && tf.f.seals === 1 && !!tBackNow && !!tBack0, JSON.stringify({ t0: { staged: t0.staged, viewFace: t0.viewFace }, tf, back0: tBack0.state }));
    check('T2 the words are where they were dragged in the preview (moved by the work area\'s own move before the approval: left of the middle)', !!placedAt && placedAt.nudged && placedAt.dx < -1, JSON.stringify(placedAt));
    check('Z1 no model call (reading, review or any other) was made by any of this: the front takes its words from the setup', modelCalls === 0, 'calls after the world was built: ' + modelCalls);
    check('Z2 no page error', w.errors.length === 0, JSON.stringify(w.errors).slice(0, 300));
  } finally { console.log = realLog; await w.close(); }
  const bad = RESULTS.filter(r => !r.ok);
  console.log(`\n${RESULTS.length - bad.length}/${RESULTS.length} checks pass`);
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.log = realLog; console.error(e); process.exit(1); });
