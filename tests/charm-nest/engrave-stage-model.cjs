// ENGSTAGE (Paul, 30 Oct 2026): the engraving card's staged model, on the real sorter page over the repo's fake site (_engraveRealWorld.cjs: offline, made-up names).
//   "When I selected "Front" and pressed "Approve" then I got this empty screen with no front charm preview and no "Approved" button" (his order: the Back "Ruby", approved; Front pressed; Approve).
//   "When Engraving setting (None or Front or Back) has already been chosen and "Approved" and then the user updates their original choice then the "Approved" button must become immediately clickable
//    again ... If the user does not click "Approve" again then the changes will be lost and system will revert to previous state of engraving and the "Engrave" button will return to "Engraved"".
//   "We also need a clean and elegant solution to choose both Back & Front".  "the white selection background on each button does not work on the "Back" button".  "the hover states ... are inadequate".
//   P1-P3  the four options, one white pill that sits under the selected option, Back included, and slides; it fits the column at 40 px
//   A1-A7  Paul's case: the Back typed "Ruby" and approved; Front pressed; Approve: "Approved ✓" in the same frame, the card never empty, never "Words to confirm" / "Engrave these words"; the Front is made WITH
//          "Ruby", fitted and approved; the Back is taken away with no seal
//   B1-B4  Front & Back: the face with no words turns Approve off with the gold pulse; its thumbnail makes it active; typing lights Approve; one press approves both faces
//   C1-C4  a change and the card left with no Approve is discarded: the jobs, the override, the row of the order and the panel are as kept
//   D1-D3  rapid presses of the options and the thumbnails, typing, Approve pressed three times: one commit of what was staged at the press, no double jobs
//   H1     hover, focus-visible and pressed states on the options, the words box, Aα, Emoji, the font picker and Approve
//   PW_DIR=<playwright node_modules> CHROMIUM=<chromium> node tests/charm-nest/engrave-stage-model.cjs      (SHOTS=<dir> for screenshots)
'use strict';
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : String(evidence || '').slice(0, 900)}`.trimEnd()); };
const until = async (f, ms) => { const t0 = Date.now(); for (;;) { const v = await f(); if (v || Date.now() - t0 > ms) return v; await new Promise(r => setTimeout(r, 100)); } };

(async () => {
  const R = require('./_engraveRealWorld.cjs');
  const w = await R.world({ shots: process.env.SHOTS || '/tmp/engrave-stage-model-shots' });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  const page = w.page, errors = [];
  page.on('pageerror', e => errors.push(e.message));
  await page.waitForFunction(() => !window.CNRole || ['ask', 'role', 'admin'].includes(CNRole.state()), null, { timeout: 20000 }).catch(() => {}).then(() => page.evaluate(() => { try { if (window.CNRole && CNRole.state() === 'ask') CNRole.choose('design'); } catch (_) { /* none */ } }));
  const Q = '#egQueue .rvItem[data-kind=placement]', P = Q + ' .pvSetup';
  // (the page's helpers: the state of the card in front, the jobs of an order, a thumbnail of the card: the SPEC's [data-eng-thumb], else the card's own face box)
  await page.addInitScript(() => {});
  await page.evaluate(() => {
    window.__st = () => {
      const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'), p = c && c.querySelector('.engp'), b = c && c.querySelector('[data-engp-approve]');
      const thumb = f => c && (c.querySelector(`[data-eng-thumb="${f}"]`) || c.querySelector(`.engFace[data-face="${f}"]`));
      const t = f => { const n = thumb(f); return n ? { cur: n.getAttribute('aria-current') === 'true' || !!n.closest('[aria-current="true"]'), eng: n.dataset.engraved || '', text: [...n.querySelectorAll('.engpvText')].map(x => x.textContent).join('|') } : null; };
      return { key: c ? c.dataset.key : '', state: c ? c.dataset.state : '', panel: !!p, choice: p ? p.dataset.choice : '', active: p ? p.dataset.active : '', dirty: !!(p && p.dataset.dirty), need: p ? p.dataset.need || '' : '',
        pressed: p ? [...p.querySelectorAll('[data-engp="choice"]')].filter(x => x.getAttribute('aria-pressed') === 'true').map(x => x.dataset.choice) : [],
        words: p && p.querySelector('.engpWords') ? { value: p.querySelector('.engpWords').value, ph: p.querySelector('.engpWords').placeholder, face: p.querySelector('.engpWords').dataset.face } : null,
        approve: b ? { a: b.dataset.a, dis: b.disabled, text: b.textContent.trim(), gate: b.dataset.gate } : null, others: c ? c.querySelectorAll('.pvApproval [data-a="approve"]:not([data-engp-approve]),.pvApproval [data-a="approved"]:not([data-engp-approve])').length : 0,
        front: t('front'), back: t('back'), text: c ? c.textContent : '' };
    };
    window.__jobs = key => { const row = B.orders.byKey.get(key); return Engrave.jobsOf(row).map(j => ({ key: j.key, face: j.face === 'front' ? 'front' : 'back', state: j.state, text: j.text, seals: (j.engravingSeals || []).length, removed: !!(j.decision && j.decision.removed), fit: !!j.fit })); };
    window.__thumb = f => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); return c && (c.querySelector(`[data-eng-thumb="${f}"]`) || c.querySelector(`.engFace[data-face="${f}"] .engThumb`) || c.querySelector(`.engFace[data-face="${f}"]`)); };
  });
  const st = () => page.evaluate(() => window.__st());
  const jobsOf = key => page.evaluate(k => window.__jobs(k), key);
  const open = async (name, face) => { await R.openEngraveDetail(w, name, { face }); await R.settle(page, 500); };
  // (a piece by its job key: the page's own way to one piece of an order (Engrave.openPiece), a decided one included; else the view focused on it)
  // (an Approve's tail, its file and the tab moving on, is let finish before a piece is opened: the tab moving on after it would take the card away again)
  const quiet = () => until(() => page.evaluate(() => !CharmNestEngravePanel.COMMITS.size && ![...Engrave.items().values()].some(j => j._approvalTask || j._backTask || j.approvalPreparing || j.stamping || j.backSaving)), 60000).then(() => R.settle(page, 600));
  const openKey = async jk => { await quiet(); for (let i = 0; i < 3; i++) { await openKey1(jk); if (await page.evaluate(k => (document.querySelector('#egQueue .rvItem[data-kind=placement]') || {}).dataset?.key === k, jk)) return true; } return false; };
  const openKey1 = async jk => { const ok = await page.evaluate(k => { CN.setMode('engrave'); const j = Engrave.items().get(k); if (!j) return false; const rid = String(j.row.order.receiptId), pk = window.CharmNestEngraveFaces ? CharmNestEngraveFaces.pieceKeyOf(k) : k.replace(/~front$/, ''); let moved = false; try { moved = Engrave.openPiece ? Engrave.openPiece(rid, pk, j.face === 'front' ? 'front' : 'back') : false; } catch (_) { moved = false; } if (!moved) { Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: k, pin: null }); Engrave.render(); } return true; }, jk); await R.settle(page, 700); return ok; };
  const clickThumb = f => page.evaluate(f => { const n = window.__thumb(f); if (n) n.click(); return !!n; }, f);
  const typeInto = async (text, replace) => { await page.click(P + ' .engpWords'); if (replace) { await page.keyboard.press('Control+A'); await page.keyboard.press('Backspace'); } else await page.keyboard.press('End'); if (text) await page.keyboard.type(text, { delay: 15 }); await R.settle(page, 150); };
  const approvedAll = (key, faces) => until(async () => { const js = await jobsOf(key); return faces.every(f => js.some(j => j.face === f && ['approved', 'written'].includes(j.state))) ? js : null; }, 120000);
  try {
    const made = await R.build(w, ['back', 'both']);
    console.log(`   built: back ${JSON.stringify(made.back.jobs.map(j => [j.face, j.state, j.text]))} · both ${JSON.stringify(made.both.jobs.map(j => [j.face, j.state, j.text]))}`);
    const KB = made.back.key, KF = made.both.key;

    /* ── P · the four options and the one pill ── */
    await open('both', 'front');
    const pill = await page.evaluate(async () => {
      const box = document.querySelector('#egQueue .rvItem[data-kind=placement] .engp .engpChoice'), out = [];
      const wait = ms => new Promise(r => setTimeout(r, ms));
      for (const c of ['back', 'none', 'front', 'both', 'back']) {
        const b = box.isConnected ? box : document.querySelector('#egQueue .rvItem[data-kind=placement] .engp .engpChoice');
        b.querySelector(`[data-choice="${c}"]`).click();
        const b2 = document.querySelector('#egQueue .rvItem[data-kind=placement] .engp .engpChoice'), mid = getComputedStyle(b2.querySelector('.engpPill')).transitionDuration;
        await wait(320);
        const b3 = document.querySelector('#egQueue .rvItem[data-kind=placement] .engp .engpChoice'), pl = b3.querySelector('.engpPill'), seg = b3.querySelector(`[data-choice="${c}"]`), pr = pl.getBoundingClientRect(), sr = seg.getBoundingClientRect();
        out.push({ c, pressed: [...b3.querySelectorAll('[aria-pressed=true]')].map(x => x.dataset.choice).join(), dx: Math.round(Math.abs(pr.left - sr.left)), dw: Math.round(Math.abs(pr.width - sr.width)), bg: getComputedStyle(pl).backgroundColor, segBg: getComputedStyle(seg).backgroundColor, dur: mid, h: Math.round(sr.height), over: seg.scrollWidth > seg.clientWidth + 1, label: seg.textContent });
      }
      return out;
    });
    check('P1 four options None | Front | Back | Front & Back (or Both), exactly one pressed after each press', pill.length === 5 && pill.every(p => p.pressed === p.c), JSON.stringify(pill.map(p => [p.c, p.pressed])));
    check('P2 the one white pill sits under the selected option for every option, Back included (no tint of its own on any option)', pill.every(p => p.dx <= 2 && p.dw <= 2 && /^rgb\(255, 25[0-9], 2[45][0-9]\)$/.test(p.bg) && /rgba\(0, 0, 0, 0\)|transparent/.test(p.segBg)), JSON.stringify(pill));
    check('P3 the pill slides (a transform transition), the options are 40 px tall and their words fit', pill.every(p => /0\.18s|180ms/.test(p.dur) && p.h >= 40 && !p.over) && /^(Front & Back|Both)$/.test(pill.find(p => p.c === 'both').label), JSON.stringify(pill.map(p => [p.c, p.dur, p.h, p.over, p.label])));
    if (w.shots) await R.shot(w, 'stage-pill-back').catch(() => {});

    /* ── H · hover, focus-visible, pressed ── */
    await page.mouse.move(2, 2);
    const hov = {};
    for (const [name, sel, prop] of [['option', P + ' .engp [data-engp="choice"][data-choice="none"]', 'backgroundColor'], ['words', P + ' .engpWords', 'borderColor'], ['alpha', P + ' .engpTools [data-alpha]', 'backgroundColor'], ['emoji', P + ' .engpTools [data-emoji]', 'backgroundColor'], ['font', P + ' .engpFont', 'borderColor']]) {
      const has = await page.$(sel); if (!has) { hov[name] = { missing: true }; continue; }
      const before = await page.evaluate(([s, pr]) => getComputedStyle(document.querySelector(s))[pr], [sel, prop]);
      await page.hover(sel); await R.settle(page, 220);
      const after = await page.evaluate(([s, pr]) => { const n = document.querySelector(s), cs = getComputedStyle(n); return { v: cs[pr], cursor: cs.cursor }; }, [sel, prop]);
      await page.mouse.move(2, 2); await R.settle(page, 160);
      await page.focus('#egQ').catch(() => {}); await page.keyboard.press('Shift');
      const foc = await page.evaluate(s => { const n = document.querySelector(s); n.focus(); const cs = getComputedStyle(n); const r = { fv: n.matches(':focus-visible'), outline: cs.outlineStyle + ' ' + cs.outlineWidth, off: cs.outlineOffset }; n.blur(); return r; }, sel);
      hov[name] = { before, after: after.v, cursor: after.cursor, foc };
    }
    const hovOk = n => hov[n] && !hov[n].missing && hov[n].before !== hov[n].after && /pointer|text|auto/.test(hov[n].cursor) && hov[n].foc.fv && /^solid 2px$/.test(hov[n].foc.outline) && hov[n].foc.off === '2px';
    check('H1 hover changes the fill or the border of the options, the words box, Aα, Emoji and the font picker; keyboard focus gives each a 2 px gold ring 2 px out', ['option', 'words', 'font'].every(hovOk) && ['alpha', 'emoji'].every(n => hov[n].missing || hovOk(n)), JSON.stringify(hov));
    const apHov = await page.evaluate(() => { const b = document.querySelector('#egQueue .rvItem[data-kind=placement] [data-engp-approve]'); return b ? { dis: b.disabled, bg: getComputedStyle(b).backgroundColor } : null; });
    if (apHov && !apHov.dis) { await page.hover(Q + ' [data-engp-approve]'); await R.settle(page, 220); const a2 = await page.evaluate(() => { const b = document.querySelector('#egQueue .rvItem[data-kind=placement] [data-engp-approve]'); return getComputedStyle(b).backgroundColor; }); await page.mouse.move(2, 2); check('H1 hover darkens the green Approve', a2 !== apHov.bg, JSON.stringify({ apHov, a2 })); }
    else check('H1 Approve is green and live after a change (for its hover)', false, JSON.stringify(apHov));
    const press = await page.evaluate(() => { const s = [...document.styleSheets].map(x => { try { return [...x.cssRules].map(r => r.cssText).join('\n'); } catch (_) { return ''; } }).join('\n'); return { opt: /\.engpSeg:not\(:disabled\):active\s*\{[^}]*scale\(0?\.98\)/.test(s), tools: /translateY\(1px\)/.test(s), reduced: /prefers-reduced-motion/.test(s) }; });
    check('H1 pressed: the options scale to .98, the tools and Approve press 1 px down; reduced motion drops the movement', press.opt && press.tools && press.reduced, JSON.stringify(press));
    // (back to what is kept: the card left, its draft discarded)
    await page.evaluate(() => { Engrave.restoreView({ tab: 'place', list: true, chosen: true, focus: null }); Engrave.render(); }); await R.settle(page, 900);

    /* ── A · Paul's case ── */
    const backKey = made.back.jobs.find(j => j.face === 'back').key;
    await open('back', 'back');
    await typeInto('Ruby', true);
    const a0 = await st();
    check('A0 the Back typed "Ruby": Approve lights up, the panel is dirty, the Back thumbnail shows the words', a0.approve && a0.approve.a === 'approve' && !a0.approve.dis && a0.dirty && a0.choice === 'back', JSON.stringify(a0).slice(0, 600));
    await page.click(Q + ' [data-engp-approve]');
    const backDone = await approvedAll(KB, ['back']);
    check('A0 Approve keeps "Ruby" on the Back and approves it', !!backDone && /Ruby/.test(backDone.find(j => j.face === 'back').text), JSON.stringify(backDone));
    await R.settle(page, 600);
    await openKey(backKey);
    const a1 = await st();
    check('A1 the approved Back opens calm: "Approved", nothing staged, Back selected, the words "Ruby"', a1.approve && a1.approve.a === 'approved' && a1.approve.dis && /^Approved/.test(a1.approve.text) && !a1.dirty && a1.choice === 'back' && a1.words && a1.words.value === 'Ruby' && a1.others === 0, JSON.stringify(a1).slice(0, 700));
    await page.click(P + ' .engp [data-engp="choice"][data-choice="front"]');
    const a2 = await st();
    check('A2 Front pressed: Front selected and shown, its words "Ruby" (the order\'s words), Approve green at once', a2.choice === 'front' && a2.active === 'front' && a2.pressed.join() === 'front' && a2.words && a2.words.value === 'Ruby' && a2.words.face === 'front' && a2.approve && a2.approve.a === 'approve' && !a2.approve.dis && a2.dirty, JSON.stringify(a2).slice(0, 700));
    if (w.shots) await R.shot(w, 'stage-A2-front-staged').catch(() => {});
    // the press, and what the card shows in the same frame
    const a3 = await page.evaluate(() => { const b = document.querySelector('#egQueue .rvItem[data-kind=placement] [data-engp-approve]'); b.click(); return { a: b.dataset.a, dis: b.disabled, text: b.textContent.trim() }; });
    check('A3 Approve pressed: "Approved ✓" in the same frame (calm, off)', a3.a === 'approved' && a3.dis && /Approved\s*✓/.test(a3.text), JSON.stringify(a3));
    // while it is kept: the card in front is never empty and never asks for words; the front job never waits for words
    const bad = [], seen = new Set(); let frontJob = null;
    const t0 = Date.now();
    while (Date.now() - t0 < 120000) {
      const s = await page.evaluate(k => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); return { key: c ? c.dataset.key : '', panel: !!(c && c.querySelector('.engp')), ap: !!(c && c.querySelector('[data-engp-approve]')), text: c ? c.textContent : '', jobs: window.__jobs(k) }; }, KB);
      const fj = s.jobs.find(j => j.face === 'front'); if (fj) { seen.add(fj.state); frontJob = fj; }
      if (s.key && s.key.startsWith(KB) && (!s.panel || !s.ap)) bad.push('empty:' + s.key);
      if (s.key && s.key.startsWith(KB) && /Words to confirm|Engrave these words|gives no words/i.test(s.text)) bad.push('words:' + s.key);
      if (fj && ['approved', 'written'].includes(fj.state)) break;
      await new Promise(r => setTimeout(r, 120));
    }
    check('A4 while Approve is kept the card of the piece always has its panel and its Approve, never "Words to confirm" / "Engrave these words"', bad.length === 0, bad.slice(0, 6).join(' '));
    check('A5 the Front job never waits for words (made WITH the staged words)', !seen.has('words') && !seen.has('blocked'), [...seen].join(','));
    const aj = await jobsOf(KB), fr = aj.find(j => j.face === 'front'), bk = aj.find(j => j.face === 'back');
    check('A6 the Front exists, approved, with "Ruby"', !!fr && ['approved', 'written'].includes(fr.state) && /Ruby/.test(fr.text) && fr.fit, JSON.stringify(aj));
    check('A7 the Back is taken away with no seal ("none", removed), never cut plain', !!bk && bk.state === 'none' && bk.removed && bk.seals === 0, JSON.stringify(bk));
    if (w.shots) await R.shot(w, 'stage-A7-after').catch(() => {});

    /* ── B · Front & Back ── */
    const frontKey = fr && fr.key;
    await openKey(frontKey);
    const b0 = await st();
    await page.click(P + ' .engp [data-engp="choice"][data-choice="both"]');
    const b1 = await st();
    check('B1 Front & Back with no words for the Back: Approve off with the gold pulse on the Back (no text), the Front still shown', b1.choice === 'both' && b1.approve && b1.approve.dis && b1.approve.gate === 'need' && /back/.test(b1.need) && b1.active === 'front' && !/words/i.test((b1.approve.text || '').replace(/Approve/, '')), JSON.stringify({ b0: b0.approve, b1 }).slice(0, 700));
    const thumbed = await clickThumb('back'); await R.settle(page, 250);
    const b2 = await st();
    check('B2 the Back thumbnail makes the Back active: an empty words box named "Back words"', thumbed && b2.active === 'back' && b2.words && b2.words.face === 'back' && b2.words.value === '' && /Back words/.test(b2.words.ph), JSON.stringify(b2).slice(0, 600));
    await typeInto('Name Two');
    const b3 = await st();
    check('B3 typing the Back\'s words lights Approve', b3.choice === 'both' && b3.approve && b3.approve.a === 'approve' && !b3.approve.dis && b3.words.value === 'Name Two', JSON.stringify(b3).slice(0, 600));
    await page.click(Q + ' [data-engp-approve]');
    const both = await approvedAll(KB, ['front', 'back']);
    const bf = both && both.filter(j => j.face === 'front'), bb = both && both.filter(j => j.face === 'back');
    check('B4 one press approves both faces: the Front "Ruby", the Back "Name Two" (the Back taken away earlier is opened again with the new words)', !!both && bf.length === 1 && bb.length === 1 && /Ruby/.test(bf[0].text) && /Name Two/.test(bb[0].text), JSON.stringify(both));

    /* ── C · a change left with no Approve is discarded ── */
    await open('both', 'front');
    const c0 = await st();
    if (!(c0.approve && c0.approve.a === 'approved')) { await page.click(Q + ' [data-engp-approve]'); await approvedAll(KF, ['front', 'back']); await R.settle(page, 600); }
    const kept = await page.evaluate(k => ({ jobs: JSON.stringify(window.__jobs(k).map(j => [j.face, j.state, j.text])), ov: JSON.stringify(B.orders.byKey.get(k).engravingOverride || null) }), KF);
    const rowNow = () => page.evaluate(rid => { Engrave.restoreView({ tab: 'place', list: true, chosen: true, focus: null }); Engrave.render(); const v = document.getElementById('engraveView'); const r = [...v.querySelectorAll('#egQueue .placementRow')].find(n => n.textContent.includes(rid)); return r ? (r.dataset.state || '') + ' ' + r.textContent.replace(/\s+/g, ' ').trim().slice(0, 200) : ''; }, made.both.rid);
    const row0 = await rowNow(); await R.settle(page, 600);
    await open('both', 'front');
    await page.click(P + ' .engp [data-engp="choice"][data-choice="none"]');
    const c1 = await st();
    check('C1 an approved piece changed (None): Approve green at once', c1.choice === 'none' && c1.approve && c1.approve.a === 'approve' && !c1.approve.dis && c1.dirty, JSON.stringify(c1).slice(0, 500));
    await page.click(P + ' .engp [data-engp="choice"][data-choice="back"]'); await typeInto('Zed');
    await page.evaluate(() => { Engrave.restoreView({ tab: 'place', list: true, chosen: true, focus: null }); Engrave.render(); }); await R.settle(page, 900);
    const after = await page.evaluate(k => ({ jobs: JSON.stringify(window.__jobs(k).map(j => [j.face, j.state, j.text])), ov: JSON.stringify(B.orders.byKey.get(k).engravingOverride || null) }), KF);
    check('C2 the card left with no Approve: the jobs and the override are as kept', after.jobs === kept.jobs && after.ov === kept.ov, JSON.stringify({ kept, after }));
    const row1 = await rowNow(); await R.settle(page, 400);
    check('C3 the row of the order in the list shows what is kept (its chip as before the change)', row1 === row0 && !!row0, JSON.stringify({ row0, row1 }));
    await open('both', 'front');
    const c4 = await st();
    check('C4 opened again: Front & Back, calm "Approved", nothing staged, the kept words', c4.choice === 'both' && c4.approve && c4.approve.a === 'approved' && !c4.dirty && c4.words && !/Zed/.test(c4.words.value), JSON.stringify(c4).slice(0, 500));

    /* ── D · rapid presses, then Approve three times ── */
    const rapid = await page.evaluate(async () => {
      const q = '#egQueue .rvItem[data-kind=placement]', opt = c => document.querySelector(`${q} .engp [data-engp="choice"][data-choice="${c}"]`), tick = () => new Promise(r => requestAnimationFrame(r));
      for (const c of ['none', 'front', 'back', 'both', 'none', 'back', 'front', 'both', 'back']) { opt(c).click(); if (c === 'both') { const t = window.__thumb('front'); if (t) t.click(); const t2 = window.__thumb('back'); if (t2) t2.click(); } }
      await tick();
      const ta = document.querySelector(`${q} .engp .engpWords`); ta.focus(); ta.value = 'Fast'; ta.dispatchEvent(new Event('input', { bubbles: true }));
      const b = document.querySelector(`${q} [data-engp-approve]`), s0 = { choice: document.querySelector(`${q} .engp`).dataset.choice, face: ta.dataset.face, a: b.dataset.a, dis: b.disabled };
      b.click(); const s1 = { a: b.dataset.a, text: b.textContent.trim() };
      b.click(); document.querySelector(`${q}`).dispatchEvent(new KeyboardEvent('keydown', { key: 'a', bubbles: true }));
      return { s0, s1 };
    });
    check('D1 after rapid presses the staged state is exactly the last one (Back, "Fast"); Approve pressed shows "Approved ✓" at once', rapid.s0.choice === 'back' && rapid.s0.face === 'back' && rapid.s0.a === 'approve' && !rapid.s0.dis && rapid.s1.a === 'approved' && /✓/.test(rapid.s1.text), JSON.stringify(rapid));
    const dd = await until(async () => { const js = await jobsOf(KF); const b = js.find(j => j.face === 'back'); return b && ['approved', 'written'].includes(b.state) && /Fast/i.test(b.text) ? js : null; }, 120000);
    await R.settle(page, 1500);
    const dj = await jobsOf(KF);
    check('D2 the commit keeps what was staged at the press: the Back approved with "Fast", the Front taken away', !!dd && dj.filter(j => j.face === 'back').length === 1 && /Fast/i.test(dj.find(j => j.face === 'back').text) && ['approved', 'written'].includes(dj.find(j => j.face === 'back').state) && dj.find(j => j.face === 'front').state === 'none', JSON.stringify(dj));
    check('D3 no double jobs: one job per face of the piece', ['front', 'back'].every(f => dj.filter(j => j.face === f).length <= 1), JSON.stringify(dj));
    check('Z no page errors', errors.length === 0, errors.join(' | ').slice(0, 600));
  } catch (e) { console.error(e); check('ran', false, String(e && e.stack || e)); }
  finally { await w.close(); }
  const bad = RESULTS.filter(r => !r.ok);
  console.log(bad.length ? `engrave-stage-model: ${bad.length} failed` : `engrave-stage-model: ${RESULTS.length} checks passed`);
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
