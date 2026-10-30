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
    // (the card in front: never the copy the tab's motion leaves flying for a moment, .mGhost)
    window.__card = () => [...document.querySelectorAll('#egQueue .rvItem[data-kind=placement]')].find(c => !c.closest('.mGhost') && !c.classList.contains('mCopy')) || null;
    window.__st = () => {
      const c = window.__card(), p = c && c.querySelector('.engp'), b = c && c.querySelector('[data-engp-approve]');
      const thumb = f => c && (c.querySelector(`[data-eng-thumb="${f}"]`) || c.querySelector(`.engFace[data-face="${f}"]`));
      const t = f => { const n = thumb(f); return n ? { cur: n.getAttribute('aria-current') === 'true' || !!n.closest('[aria-current="true"]'), eng: n.dataset.engraved || '', text: [...n.querySelectorAll('.engpvText')].map(x => x.textContent).join('|') } : null; };
      return { key: c ? c.dataset.key : '', state: c ? c.dataset.state : '', panel: !!p, choice: p ? p.dataset.choice : '', active: p ? p.dataset.active : '', dirty: !!(p && p.dataset.dirty), need: p ? p.dataset.need || '' : '',
        pressed: p ? [...p.querySelectorAll('[data-engp="choice"]')].filter(x => x.getAttribute('aria-pressed') === 'true').map(x => x.dataset.choice) : [],
        words: p && p.querySelector('.engpWords') ? { value: p.querySelector('.engpWords').value, ph: p.querySelector('.engpWords').placeholder, face: p.querySelector('.engpWords').dataset.face } : null,
        approve: b ? { a: b.dataset.a, dis: b.disabled, text: b.textContent.trim(), gate: b.dataset.gate } : null, others: c ? c.querySelectorAll('.pvApproval [data-a="approve"]:not([data-engp-approve]),.pvApproval [data-a="approved"]:not([data-engp-approve])').length : 0,
        front: t('front'), back: t('back'), text: c ? c.textContent : '' };
    };
    window.__jobs = key => { const row = B.orders.byKey.get(key); return Engrave.jobsOf(row).map(j => ({ key: j.key, face: j.face === 'front' ? 'front' : 'back', state: j.state, text: j.text, seals: (j.engravingSeals || []).length, removed: !!(j.decision && j.decision.removed), fit: !!j.fit })); };
    window.__thumb = f => { const c = window.__card(); return c && (c.querySelector(`[data-eng-thumb="${f}"]`) || c.querySelector(`.engFace[data-face="${f}"] .engThumb`) || c.querySelector(`.engFace[data-face="${f}"]`)); };
  });
  const st = () => page.evaluate(() => window.__st());
  const jobsOf = key => page.evaluate(k => window.__jobs(k), key);
  const open = async (name, face) => { await R.openEngraveDetail(w, name, { face }); await R.settle(page, 500); };
  // (a piece by its job key: the page's own way to one piece of an order (Engrave.openPiece), a decided one included; else the view focused on it)
  // (an Approve's tail, its file and the tab moving on, is let finish before a piece is opened: the tab moving on after it would take the card away again)
  const quiet = () => until(() => page.evaluate(() => !CharmNestEngravePanel.COMMITS.size && !(window.Seal && Seal.busy()) && ![...Engrave.items().values()].some(j => j._approvalTask || j._backTask || j.approvalPreparing || j.stamping || j.backSaving)), 60000).then(() => R.settle(page, 600));
  const openKey = async jk => { await quiet(); for (let i = 0; i < 3; i++) { await openKey1(jk); if (await page.evaluate(k => (window.__card() || {}).dataset?.key === k, jk)) return true; } return false; };
  const openKey1 = async jk => { const ok = await page.evaluate(k => { CN.setMode('engrave'); const j = Engrave.items().get(k); if (!j) return false; const rid = String(j.row.order.receiptId), pk = window.CharmNestEngraveFaces ? CharmNestEngraveFaces.pieceKeyOf(k) : k.replace(/~front$/, ''); let moved = false; try { moved = Engrave.openPiece ? Engrave.openPiece(rid, pk, j.face === 'front' ? 'front' : 'back') : false; } catch (_) { moved = false; } if (!moved) { Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: k, pin: null }); Engrave.render(); } return true; }, jk); await R.settle(page, 700); return ok; };
  const clickThumb = f => page.evaluate(f => { const n = window.__thumb(f); if (n) n.click(); return !!n; }, f);
  const typeInto = async (text, replace) => { await page.click(P + ' .engpWords'); if (replace) { await page.keyboard.press('Control+A'); await page.keyboard.press('Backspace'); } else await page.keyboard.press('End'); if (text) await page.keyboard.type(text, { delay: 15 }); await R.settle(page, 150); };
  const approvedAll = (key, faces) => until(async () => { const js = await jobsOf(key); return faces.every(f => js.some(j => j.face === f && ['approved', 'written'].includes(j.state))) ? js : null; }, 120000);
  try {
    const made = await R.build(w, ['back', 'both', 'tag', 'front']);
    console.log(`   built: back ${JSON.stringify(made.back.jobs.map(j => [j.face, j.state, j.text]))} · both ${JSON.stringify(made.both.jobs.map(j => [j.face, j.state, j.text]))}`);
    const KB = made.back.key, KF = made.both.key;

    /* ── P · the four options and the one pill ── */
    await open('both', 'front');
    const pill = await page.evaluate(async () => {
      const box = window.__card().querySelector('.engp .engpChoice'), out = [];
      const wait = ms => new Promise(r => setTimeout(r, ms));
      for (const c of ['back', 'none', 'front', 'both', 'back']) {
        const b = window.__card().querySelector('.engp .engpChoice'); void box;   // (the card in front now: a card drawn again replaces the panel)
        b.querySelector(`[data-choice="${c}"]`).click();
        const b2 = window.__card().querySelector('.engp .engpChoice'), mid = getComputedStyle(b2.querySelector('.engpPill')).transitionDuration;
        await wait(320);
        for (let i = 0; i < 10; i++) { const bx = window.__card().querySelector('.engp .engpChoice'), p0 = bx.querySelector('.engpPill').getBoundingClientRect(), s0 = bx.querySelector(`[data-choice="${c}"]`).getBoundingClientRect(); if (Math.abs(p0.left - s0.left) <= 2) break; await wait(100); }   // (a card drawn again meanwhile slides its own pill: it ends under the option)
        const b3 = window.__card().querySelector('.engp .engpChoice'), pl = b3.querySelector('.engpPill'), seg = b3.querySelector(`[data-choice="${c}"]`), pr = pl.getBoundingClientRect(), sr = seg.getBoundingClientRect();
        out.push({ c, pressed: [...b3.querySelectorAll('[aria-pressed=true]')].map(x => x.dataset.choice).join(), dx: Math.round(Math.abs(pr.left - sr.left)), dw: Math.round(Math.abs(pr.width - sr.width)), bg: getComputedStyle(pl).backgroundColor, segBg: getComputedStyle(seg).backgroundColor, dur: mid, h: Math.round(sr.height), over: (() => { const t = [...seg.childNodes].find(n => n.nodeType === 3), r = document.createRange(); if (!t) return true; r.selectNodeContents(t); return r.getBoundingClientRect().width > seg.clientWidth - 2; })(), label: [...seg.childNodes].filter(n => n.nodeType === 3).map(n => n.nodeValue).join('').trim() });
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
    const apHov = await page.evaluate(() => { const b = window.__card().querySelector('[data-engp-approve]'); return b ? { dis: b.disabled, bg: getComputedStyle(b).backgroundColor } : null; });
    if (apHov && !apHov.dis) { await page.hover(Q + ' [data-engp-approve]'); await R.settle(page, 220); const a2 = await page.evaluate(() => { const b = window.__card().querySelector('[data-engp-approve]'); return getComputedStyle(b).backgroundColor; }); await page.mouse.move(2, 2); check('H1 hover darkens the green Approve', a2 !== apHov.bg, JSON.stringify({ apHov, a2 })); }
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
    const a3 = await page.evaluate(() => { const b = window.__card().querySelector('[data-engp-approve]'); b.click(); return { a: b.dataset.a, dis: b.disabled, text: b.textContent.trim() }; });
    check('A3 Approve pressed: "Approved ✓" in the same frame (calm, off)', a3.a === 'approved' && a3.dis && /Approved\s*✓/.test(a3.text), JSON.stringify(a3));
    // while it is kept: the card in front is never empty and never asks for words; the front job never waits for words
    const bad = [], seen = new Set(); let frontJob = null;
    const t0 = Date.now();
    while (Date.now() - t0 < 120000) {
      const s = await page.evaluate(k => { const c = window.__card(); return { key: c ? c.dataset.key : '', panel: !!(c && c.querySelector('.engp')), ap: !!(c && c.querySelector('[data-engp-approve]')), text: c ? c.textContent : '', jobs: window.__jobs(k) }; }, KB);
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
    const rowNow = async () => (await quiet(), page.evaluate(rid => { Engrave.restoreView({ tab: 'place', list: true, chosen: true, focus: null }); Engrave.render(); const v = document.getElementById('engraveView'); const r = [...v.querySelectorAll('#egQueue .placementRow')].find(n => n.textContent.includes(rid)); return r ? (r.dataset.state || '') + ' ' + r.textContent.replace(/\s+/g, ' ').trim().slice(0, 200) : ''; }, made.both.rid));   // (the tab's render waits for a stamp still coming down: quiet first)
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
      const q = '#egQueue .rvItem[data-kind=placement]', opt = c => window.__card().querySelector(`.engp [data-engp="choice"][data-choice="${c}"]`), tick = () => new Promise(r => requestAnimationFrame(r));
      for (const c of ['none', 'front', 'back', 'both', 'none', 'back', 'front', 'both', 'back']) { opt(c).click(); if (c === 'both') { const t = window.__thumb('front'); if (t) t.click(); const t2 = window.__thumb('back'); if (t2) t2.click(); } }
      await tick();
      const ta = window.__card().querySelector('.engp .engpWords'); ta.focus(); ta.value = 'Fast'; ta.dispatchEvent(new Event('input', { bubbles: true }));
      const b = window.__card().querySelector('[data-engp-approve]'), s0 = { choice: window.__card().querySelector('.engp').dataset.choice, face: ta.dataset.face, a: b.dataset.a, dis: b.disabled };
      b.click(); const s1 = { a: b.dataset.a, text: b.textContent.trim() };
      b.click(); window.__card().dispatchEvent(new KeyboardEvent('keydown', { key: 'a', bubbles: true }));
      return { s0, s1 };
    });
    check('D1 after rapid presses the staged state is exactly the last one (Back, "Fast"); Approve pressed shows "Approved ✓" at once', rapid.s0.choice === 'back' && rapid.s0.face === 'back' && rapid.s0.a === 'approve' && !rapid.s0.dis && rapid.s1.a === 'approved' && /✓/.test(rapid.s1.text), JSON.stringify(rapid));
    const dd = await until(async () => { const js = await jobsOf(KF); const b = js.find(j => j.face === 'back'); return b && ['approved', 'written'].includes(b.state) && /Fast/i.test(b.text) ? js : null; }, 120000);
    await R.settle(page, 1500);
    const dj = await jobsOf(KF);
    check('D2 the commit keeps what was staged at the press: the Back approved with "Fast", the Front taken away', !!dd && dj.filter(j => j.face === 'back').length === 1 && /Fast/i.test(dj.find(j => j.face === 'back').text) && ['approved', 'written'].includes(dj.find(j => j.face === 'back').state) && dj.find(j => j.face === 'front').state === 'none', JSON.stringify(dj));
    check('D3 no double jobs: one job per face of the piece', ['front', 'back'].every(f => dj.filter(j => j.face === f).length <= 1), JSON.stringify(dj));
    /* ── E · edits staged while a commit runs, then Approve again: both land in turn, and the card ends calm (never a stuck "Approved", never aria-busy) ── */
    await quiet();
    await R.openEngraveDetail(w, 'both', { face: 'back' }); await R.settle(page, 600);   // (the two-face order, its Back approved by D)
    const e0 = await page.evaluate(async () => {
      if (!window.__card() || !window.__card().querySelector('.engp .engpWords')) return { none: true };
      const c = window.__card(), ta = () => window.__card().querySelector('.engp .engpWords'), b = () => window.__card().querySelector('[data-engp-approve]'), type = v => { const t = ta(); t.focus(); t.value = v; t.dispatchEvent(new Event('input', { bubbles: true })); };
      const face = ta().dataset.face, start = ta().value;
      type(start + 'X'); b().click();
      const during = { a: b().dataset.a, text: b().textContent.trim() };
      await new Promise(r => setTimeout(r, 30));
      type(start + 'XY'); const relit = { a: b().dataset.a, dis: b().disabled, dirty: !!window.__card().querySelector('.engp').dataset.dirty };
      b().click(); void c;
      return { face, start, during, relit };
    });
    check('E1 a press, edits staged on top while it is kept: Approve lights up again for them, and a second press is taken', !e0.none && /✓/.test(e0.during.text) && e0.relit.a === 'approve' && !e0.relit.dis && e0.relit.dirty, JSON.stringify(e0));
    if (!e0.none) {
    const e1f = await until(async () => { const f = (await jobsOf(KF)).find(j => j.face === e0.face); return f && ['approved', 'written'].includes(f.state) && /XY$/i.test(f.text) ? f : null; }, 150000);
    const busy = await page.evaluate(() => [...document.querySelectorAll('#egQueue [data-engp-approve][aria-busy="true"]')].length);
    await quiet(); await R.openEngraveDetail(w, 'both', { face: e0.face }); await R.settle(page, 600);
    const e1s = await st(), e1 = e1f && e1s.approve && e1s.approve.a === 'approved' && !e1s.dirty ? { f: e1f, s: e1s } : null;
    check('E2 both land in turn: the face approved with the last words staged, the card calm "Approved", nothing staged, no busy Approve', !!e1 && busy === 0, JSON.stringify({ f: e1f && { text: e1f.text, state: e1f.state }, s: { approve: e1s.approve, dirty: e1s.dirty, key: e1s.key, words: e1s.words }, busy }));
    } else check('E2 both land in turn', false, 'the card of the piece did not open');
    /* ── F · a burst with no waits (coordinator, 30 Oct 2026, S7 seed 2): stage, press, stage, press ... the LAST press is the one kept, and once nothing runs any more the card says the truth: staged = kept ── */
    await quiet(); await R.openEngraveDetail(w, 'both', { face: 'back' }); await R.settle(page, 600);
    const burst = await page.evaluate(async key => {
      const P = CharmNestEngravePanel, card = () => window.__card(), opt = c => card().querySelector(`.engp [data-engp="choice"][data-choice="${c}"]`), ta = () => card().querySelector('.engp .engpWords'), ap = () => card().querySelector('[data-engp-approve]');
      const type = v => { const t = ta(); t.focus(); t.value = v; t.dispatchEvent(new Event('input', { bubbles: true })); };
      const seq = ['both', 'front', 'back', 'both', 'back', 'front', 'back', 'both', 'back', 'back'], presses = [];
      for (let i = 0; i < seq.length; i++) {
        opt(seq[i]).click();
        if (seq[i] === 'both') { const t = window.__thumb(i % 2 ? 'front' : 'back'); if (t) t.click(); }
        type('B' + i);
        const b = ap(); if (b && !b.disabled && b.dataset.a === 'approve') { b.click(); presses.push(i); }
        if (i % 3 === 2) await new Promise(r => setTimeout(r, 0));   // (now and then a task between: the commits start under the burst)
      }
      opt('back').click(); type('Last'); const b = ap(); const lit = !!b && !b.disabled; if (lit) b.click();
      return { presses, lit, st: P.stateOf(key) };
    }, KF);
    check('F1 a burst of options, words and presses: the last Approve is lit and taken; commits run under it', burst.lit && burst.presses.length >= 3 && burst.st && (burst.st.running || burst.st.pending || burst.st.committing), JSON.stringify(burst));
    const drained = await until(() => page.evaluate(key => { const s = CharmNestEngravePanel.busyOf(key); return !s.running && !s.pending && !s.committing ? s : null; }, KF), 180000);
    await quiet();
    const fj = await jobsOf(KF), fb = fj.find(j => j.face === 'back'), ff = fj.find(j => j.face === 'front');
    check('F2 when nothing runs any more the LAST press is what is kept: the Back approved with "Last", the Front not engraved', !!drained && !!fb && ['approved', 'written'].includes(fb.state) && /^Last$/i.test(String(fb.text).trim()) && (!ff || ['none', 'skipped'].includes(ff.state)), JSON.stringify({ drained, jobs: fj }));
    if (!(await page.evaluate(key => !!CharmNestEngravePanel.stateOf(key), KF))) { await R.openEngraveDetail(w, 'both', { face: 'back' }); await R.settle(page, 700); }   // (the tab moved on after the Approve: the piece opened again)
    const truth = await page.evaluate(key => { const s = CharmNestEngravePanel.stateOf(key), c = window.__card(), p = c && c.querySelector('.engp'), b = c && c.querySelector('[data-engp-approve]'); return { s, dirtyAttr: !!(p && p.dataset.dirty), a: b && b.dataset.a, dis: b && b.disabled, busy: !!(b && b.getAttribute('aria-busy')) }; }, KF);
    check('F3 and the card says the truth: nothing staged against the jobs, the calm "Approved", nothing busy (or, when it differs from the jobs, a lit Approve)', truth.s && (truth.s.dirty ? truth.a === 'approve' && !truth.dis : truth.a === 'approved' && !truth.dirtyAttr) && !truth.busy, JSON.stringify(truth));
    /* ── G · release blocker (A), S7 seed 3: a two-face piece approved, its Back moved (back to review) and its words changed, ONE Approve: BOTH faces approved, the card calm ── */
    const KT = made.tag.key;
    await quiet(); await R.openEngraveDetail(w, 'tag', { face: 'back' }); await R.settle(page, 700);
    { const s0 = await st(); if (!(s0.approve && s0.approve.a === 'approved')) { if (s0.approve && s0.approve.a === 'approve' && !s0.approve.dis) await page.evaluate(() => window.__card().querySelector('[data-engp-approve]').click()); await approvedAll(KT, ['front', 'back']); } }
    await quiet(); await R.openEngraveDetail(w, 'tag', { face: 'back' }); await R.settle(page, 700);
    const g0 = await jobsOf(KT);
    // the approved Back moved in the work area (its own move: it goes back to review, staged) and its words changed in the box, then ONE press
    await page.evaluate(key => { const b = Engrave.jobsOf(B.orders.byKey.get(key)).find(j => j.face !== 'front'); Engrave.nudge(b, 0.3, 0); }, KT); await R.settle(page, 400);
    const g1j = await jobsOf(KT);
    await typeInto('1. 27. 25', true);
    const g1 = await st();
    await page.evaluate(() => window.__card().querySelector('[data-engp-approve]').click());
    const g2 = await until(async () => { const js = await jobsOf(KT), s = await page.evaluate(k => CharmNestEngravePanel.busyOf(k), KT); return js.every(j => ['approved', 'written'].includes(j.state)) && !s.running && !s.pending && !s.committing ? js : null; }, 120000);
    await quiet(); await R.openEngraveDetail(w, 'tag', { face: 'back' }); await R.settle(page, 700);
    const g3 = await st();
    check('G1 the approved two-face piece: its Back moved back to review, its words changed: Approve lit, the panel dirty', g0.every(j => ['approved', 'written'].includes(j.state)) && g1j.find(j => j.face === 'back').state === 'review' && g1.approve && g1.approve.a === 'approve' && !g1.approve.dis && g1.dirty, JSON.stringify({ g0, g1j, g1: { approve: g1.approve, dirty: g1.dirty } }));
    check('G2 ONE Approve approves BOTH faces (the Back that went back to review included), with the new words', !!g2 && /1\. 27\. 25/.test(g2.find(j => j.face === 'back').text.replace(/\n/g, ' ')), JSON.stringify(g2 || await jobsOf(KT)));
    check('G3 opened again: calm "Approved", nothing staged', g3.approve && g3.approve.a === 'approved' && !g3.dirty, JSON.stringify({ approve: g3.approve, dirty: g3.dirty }));
    // and a face left unapproved under an approved one is never calm: the back moved again, nothing else
    await page.evaluate(key => { const b = Engrave.jobsOf(B.orders.byKey.get(key)).find(j => j.face !== 'front'); Engrave.nudge(b, 0.3, 0); }, KT); await R.settle(page, 400);
    const g4 = await st();
    check('G4 an approved face beside one gone back to review: dirty, Approve lit (never the calm "Approved")', g4.dirty && g4.approve && g4.approve.a === 'approve' && !g4.approve.dis, JSON.stringify({ approve: g4.approve, dirty: g4.dirty, jobs: await jobsOf(KT) }));
    await page.evaluate(() => { Engrave.restoreView({ tab: 'place', list: true, chosen: true, focus: null }); Engrave.render(); }); await R.settle(page, 900);   // (left with no Approve: the move is put back)

    /* ── H · release blocker (B), S7 seed 2: a burst of edits and presses with no waits on a piece under review; once idle the card is either calm with what is kept equal to what is staged, or Approve is lit ── */
    const KR = made.front.key;
    await quiet(); await R.openEngraveDetail(w, 'front', { face: 'front' }); await R.settle(page, 700);
    const hb = await page.evaluate(async key => {
      const card = () => window.__card(), opt = c => card().querySelector(`.engp [data-engp="choice"][data-choice="${c}"]`), ta = () => card().querySelector('.engp .engpWords'), ap = () => card().querySelector('[data-engp-approve]');
      const type = v => { const t = ta(); if (!t) return; t.focus(); t.value = v; t.dispatchEvent(new Event('input', { bubbles: true })); };
      const acts = ['t:ab', 'p', 'c:back', 't:yea', 'p', 't:yeaL', 'c:front', 'p', 't:yy', 'p', 'c:back', 't:ML', 'p', 't:yea', 'p'], did = [];
      for (const a of acts) {
        if (a === 'p') { const b = ap(); if (b && !b.disabled && b.dataset.a === 'approve') { b.click(); did.push('P'); } else did.push('-'); }
        else if (a.startsWith('c:')) { const o = opt(a.slice(2)); if (o) o.click(); did.push(a); }
        else { type(a.slice(2)); did.push(a); }
      }
      return { did, st: CharmNestEngravePanel.stateOf(key) };
    }, KR);
    const idle = await until(() => page.evaluate(key => { const s = CharmNestEngravePanel.busyOf(key); return !s.running && !s.pending && !s.committing ? s : null; }, KR), 180000);
    await quiet();
    if (!(await page.evaluate(key => !!CharmNestEngravePanel.stateOf(key), KR))) { const fc = ((await jobsOf(KR)).find(j => !['none', 'skipped'].includes(j.state)) || { face: 'front' }).face; await R.openEngraveDetail(w, 'front', { face: fc }); await R.settle(page, 700); }   // (the piece opened again on a face it engraves)
    const h1 = await page.evaluate(key => { const s = CharmNestEngravePanel.stateOf(key), c = window.__card(), b = c && c.querySelector('[data-engp-approve]'); return { s, a: b && b.dataset.a, dis: b && b.disabled, text: b && b.textContent.trim(), jobs: Engrave.jobsOf(B.orders.byKey.get(key)).map(j => [j.face === 'front' ? 'front' : 'back', j.state, j.text]) }; }, KR);
    check('H1 a burst of edits and presses with no waits: once idle, the card is calm with nothing staged, or Approve is lit (never "Approved ✓" with nothing running)', !!idle && h1.s && (h1.s.dirty ? (h1.a === 'approve' && !h1.dis) || h1.s.gate === 'need' : h1.a === 'approved') && !/✓/.test(h1.text || ''), JSON.stringify({ hb: hb.did.join(' '), h1 }));
    /* ── I · release blocker (A), the trace's two causes (S7 seeds 1 and 3, real input) ──
     *   I1-I2  a render met by an approval under way is owed, never dropped: the card is drawn again when the approval is done
     *   I3-I5  typing fast: a rebuild gives the focus and the caret back to the words box; an "a" or "s" that lands on the card mid-word goes into the words, never approves or stages None */
    await quiet(); await R.openEngraveDetail(w, 'tag', { face: 'back' }); await R.settle(page, 700);
    const i1 = await page.evaluate(async key => {
      const row = B.orders.byKey.get(key), j = Engrave.jobsOf(row).find(x => x.face !== 'front'), c0 = window.__card();
      j.stamping = true;   // (an approval being stamped)
      Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: j.key }); Engrave.render();   // (what a refit does: the card let go, a render asked for)
      await new Promise(r => setTimeout(r, 150));
      const c1 = window.__card();
      j.stamping = false;   // (the approval done)
      await new Promise(r => setTimeout(r, 400));
      const c2 = window.__card();
      return { heldWhileStamping: c1 === c0, drawnAfter: !!c2 && c2 !== c0 && c2.isConnected, key: c2 && c2.dataset.key };
    }, KT);
    check('I1 a render asked for while an approval is stamped waits for it (the card is not torn down under the stamp)', i1.heldWhileStamping, JSON.stringify(i1));
    check('I2 and is never dropped: once the approval is done the card is drawn again', i1.drawnAfter, JSON.stringify(i1));
    await R.settle(page, 400);
    // typing on the Back, a rebuild of the card under the typing: the focus and the caret come back to the box
    await page.click(P + ' .engpWords'); await page.keyboard.press('End'); await page.keyboard.type('xy', { delay: 20 });
    const i3 = await page.evaluate(async key => {
      const j = Engrave.jobsOf(B.orders.byKey.get(key)).find(x => x.face !== 'front'), c0 = window.__card();
      Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: j.key }); Engrave.render();   // (the tab draws the card again under the typing: a new card, a new panel; it waits for the typing to pause first)
      setTimeout(() => { const c = window.__card(); if (c) c.focus(); }, 30);   // (and takes the focus to the card, as the tab does 30 ms after a drawing when it can)
      for (let t = 0; t < 40 && window.__card() === c0; t++) await new Promise(r => setTimeout(r, 50));
      await new Promise(r => setTimeout(r, 160));
      const a = document.activeElement, ta = window.__card() && window.__card().querySelector('.engp .engpWords');
      return { inBox: !!ta && a === ta, caretAtEnd: !!ta && ta.selectionStart === ta.value.length, value: ta && ta.value, rebuilt: window.__card() !== c0 };
    }, KT);
    check('I3 a rebuild under the typing gives the focus and the caret back to the words box', i3.inBox && i3.caretAtEnd && /xy$/.test(i3.value || ''), JSON.stringify(i3));
    // the focus taken to the card itself mid-word: "a" and "s" typed at once go into the words, nothing is approved, None is not staged
    const i4 = await page.evaluate(async key => {
      const c = window.__card(), was = CharmNestEngravePanel.stateOf(key), ta = c.querySelector('.engp .engpWords');
      ta.dispatchEvent(new KeyboardEvent('keydown', { key: 'Shift', bubbles: true }));   // (a key typed in the box this instant: the typing goes on)
      c.focus();
      return { focusOnCard: document.activeElement === c, choice: was && was.choice };
    }, KT);
    await page.keyboard.type('as', { delay: 15 });
    await R.settle(page, 150);
    const i5 = await page.evaluate(key => { const s = CharmNestEngravePanel.stateOf(key), b = CharmNestEngravePanel.busyOf(key), ta = window.__card() && window.__card().querySelector('.engp .engpWords'); return { choice: s && s.choice, committing: b.committing || b.running, value: ta && ta.value, inBox: document.activeElement === ta }; }, KT);
    check('I4 "a" and "s" typed mid-word while the card had the focus: nothing approved, None not staged', i4.focusOnCard && !i5.committing && i5.choice === i4.choice && i5.choice !== 'none', JSON.stringify({ i4, i5 }));
    check('I5 and the two letters are in the words, typed where the caret was', /xyas$/.test(i5.value || '') && i5.inBox, JSON.stringify(i5));
    await page.evaluate(() => { Engrave.restoreView({ tab: 'place', list: true, chosen: true, focus: null }); Engrave.render(); }); await R.settle(page, 900);   // (left with no Approve: discarded)

    /* ── J · CAPSWORDS (coordinator, 30 Oct 2026, seed 3: staged "Name Oneo", kept "NAME\nONEO"): a capitals font, mixed case typed, Approve: calm, Approve not lit again, the box keeps what was typed ── */
    await quiet(); await R.openEngraveDetail(w, 'back', { face: 'back' }); await R.settle(page, 700);
    const j0 = await page.evaluate(() => {
      const O = window.CharmNestOrders, sel = window.__card().querySelector('.engp .engpFont'); if (!sel || !O) return { font: '' };
      const o = [...sel.options].find(x => x.value && O.fontCaps(O.fontOfPhrase(x.value))); if (!o) return { font: '', options: [...sel.options].map(x => x.value) };
      sel.value = o.value; sel.dispatchEvent(new Event('change', { bubbles: true })); return { font: o.value };
    });
    await R.settle(page, 200);
    await typeInto('Name Oneo', true);
    const j1 = await st();
    if (j1.approve && j1.approve.a === 'approve' && !j1.approve.dis) await page.click(Q + ' [data-engp-approve]');
    const jd = await until(async () => { const js = await jobsOf(KB), b = js.find(j => j.face === 'back'), s = await page.evaluate(k => CharmNestEngravePanel.busyOf(k), KB); return b && ['approved', 'written'].includes(b.state) && /NAME\s+ONEO/.test(b.text) && !s.running && !s.pending && !s.committing ? js : null; }, 120000);
    await quiet();
    let j2 = await st();
    if (!j2.panel || !jd || j2.key !== jd.find(j => j.face === 'back').key) { await R.openEngraveDetail(w, 'back', { face: 'back' }); await R.settle(page, 700); j2 = await st(); }   // (the tab moved on after the approval: the piece opened again)
    check('J1 a capitals font, "Name Oneo" typed and approved: kept in the fit\'s form (NAME ONEO), and the card calm: "Approved", Approve not lit again', !!j0.font && !!jd && j2.approve && j2.approve.a === 'approved' && !j2.dirty, JSON.stringify({ j0, j1: { approve: j1.approve, dirty: j1.dirty }, jobs: jd || await jobsOf(KB), j2: { key: j2.key, approve: j2.approve, dirty: j2.dirty, words: j2.words } }));
    check('J2 the words box keeps the casing typed ("Name Oneo"), never the fit\'s capitals', j2.words && j2.words.value === 'Name Oneo', JSON.stringify(j2.words));
    await page.evaluate(() => { Engrave.restoreView({ tab: 'place', list: true, chosen: true, focus: null }); Engrave.render(); }); await R.settle(page, 900);

    /* ── K · OWNINPUT (coordinator, 30 Oct 2026, the input tracer: "41 input events in the 2 s after an Approve were killed" by the seal's page-wide guard): while a stamp is coming down, the panel's
     *        own input goes through (a thumbnail, the words box and typing, Approve), and what would dismiss the card (Next) is still held ── */
    await quiet(); await R.openEngraveDetail(w, 'back', { face: 'back' }); await R.settle(page, 700);
    const k0 = await page.evaluate(() => {   // (a stamp held down 6 s: its seal waits for a running animation of its surface before it comes down)
      const host = document.createElement('div'); host.style.cssText = 'position:fixed;right:0;bottom:0;width:40px;height:40px;pointer-events:none'; const seal = document.createElement('div'); seal.className = 'seal'; seal.style.cssText = 'width:40px;height:40px'; host.appendChild(seal); document.body.appendChild(host);   // (on the page and seen: a seal out of sight is not stamped)
      host.animate([{ opacity: 1 }, { opacity: 0.99 }], { duration: 6000 });
      window.__k = Seal.press(seal).catch(() => {}).then(() => host.remove());
      return { busy: Seal.busy(), key: window.__card().dataset.key, words: window.__card().querySelector('.engp .engpWords').value };
    });
    await page.click(Q + ' [data-eng-thumb="front"]'); const k1 = await st();
    await page.click(Q + ' [data-eng-thumb="back"]'); const k2 = await st();
    await page.click(P + ' .engpWords'); await page.keyboard.press('End'); await page.keyboard.type('Q', { delay: 15 }); const k3 = await st();
    await page.click(Q + ' [data-engp-approve]'); const k4 = await page.evaluate(k => ({ st: window.__st().approve, busy: CharmNestEngravePanel.busyOf(k) }), KB);
    await page.click(Q + ' [data-a="next"]').catch(() => {}); const k5 = await page.evaluate(() => ({ key: (window.__card() || {}).dataset?.key, stillStamping: Seal.busy() }));
    await page.evaluate(() => window.__k);
    const kd = await until(async () => { const js = await jobsOf(KB), b = js.find(j => j.face === 'back'), s = await page.evaluate(k => CharmNestEngravePanel.busyOf(k), KB); return b && ['approved', 'written'].includes(b.state) && /ONEOQ/.test(b.text.replace(/\s+/g, '')) && !s.running && !s.pending && !s.committing ? js : null; }, 120000);
    check('K1 under a stamp the panel\'s own input goes through: the thumbnails, the words box and a typed letter, Approve ("Approved ✓", its commit under way)', k0.busy && k5.stillStamping && k1.active === 'front' && k2.active === 'back' && /Q$/.test((k3.words || {}).value || '') && k4.st && /✓/.test(k4.st.text) && (k4.busy.committing || k4.busy.running), JSON.stringify({ k0, k1: k1.active, k2: k2.active, k3: k3.words, k4, k5 }));
    check('K2 and what would take the card away (Next) is still held for the stamp', k5.key === k0.key, JSON.stringify({ k0: k0.key, k5 }));
    check('K3 the press made under the stamp is kept and approved (the Back "…ONEOQ")', !!kd, JSON.stringify(kd || await jobsOf(KB)));
    await quiet(); await page.evaluate(() => { Engrave.restoreView({ tab: 'place', list: true, chosen: true, focus: null }); Engrave.render(); }); await R.settle(page, 900);

    /* ── L · coordinator, 30 Oct 2026 (suite c554e1a, STAGE_TRACE): the last lit Approve lost under the stamp, and the earlier commit's "move on", released when the stamp ended, leaving the card with new
     *        edits staged. L1-L2: Approve, then an edit and the lit Approve again while the stamp comes down: the second commit happens. L3-L4: Approve, then typing while the stamp comes down: the card stays,
     *        the edit is kept and Approve is lit ── */
    const holdStamp = ms => page.evaluate(ms => {   // (a stamp held down: its seal waits for a running animation of its surface; the approvals' own stamps queue behind it)
      const host = document.createElement('div'); host.style.cssText = 'position:fixed;right:0;bottom:0;width:40px;height:40px;pointer-events:none'; const seal = document.createElement('div'); seal.className = 'seal'; seal.style.cssText = 'width:40px;height:40px'; host.appendChild(seal); document.body.appendChild(host);
      host.animate([{ opacity: 1 }, { opacity: 0.99 }], { duration: ms }); window.__hold = Seal.press(seal).catch(() => {}).then(() => host.remove()); return Seal.busy();
    }, ms);
    const commitDone = () => until(() => page.evaluate(k => { const s = CharmNestEngravePanel.busyOf(k); return !s.running && !s.pending && !s.committing; }, KT), 60000);
    const backOf = async () => (await jobsOf(KT)).find(j => j.face === 'back');
    await quiet(); await R.openEngraveDetail(w, 'tag', { face: 'back' }); await R.settle(page, 700);
    const l0 = await st();
    const lHeld = await holdStamp(7000);
    await typeInto('X'); await page.click(Q + ' [data-engp-approve]');
    await commitDone();
    const l1 = { back: await backOf(), busy: await page.evaluate(() => Seal.busy()), st: await st() };
    await typeInto('Y'); const l1b = await st(); await page.click(Q + ' [data-engp-approve]');
    const l1c = { busy: await page.evaluate(() => Seal.busy()), st: (await st()).approve, run: await page.evaluate(k => CharmNestEngravePanel.busyOf(k), KT) };
    await commitDone(); await page.evaluate(() => window.__hold); await quiet();
    const l2 = await backOf();
    check('L1 Approve, then an edit and the lit Approve pressed again while the stamp comes down: the second press goes through ("Approved ✓", its commit under way)', lHeld && l1.busy && /X$/.test(l1.back.text.replace(/\s+/g, '')) && l1b.approve && l1b.approve.a === 'approve' && l1c.busy && /✓/.test((l1c.st || {}).text || '') && (l1c.run.committing || l1c.run.running), JSON.stringify({ l0: l0.key, l1, l1b: { approve: l1b.approve, dirty: l1b.dirty, words: l1b.words }, l1c }));
    check('L2 and the second commit happens: the Back approved with both edits ("…XY")', ['approved', 'written'].includes(l2.state) && /XY$/.test(l2.text.replace(/\s+/g, '')), JSON.stringify(l2));
    await quiet(); { const s = await st(); if (s.key !== l0.key) { await R.openEngraveDetail(w, 'tag', { face: 'back' }); await R.settle(page, 700); } }
    const l3h = await holdStamp(7000);
    await typeInto('Z'); await page.click(Q + ' [data-engp-approve]');
    await commitDone();
    const l3a = { back: await backOf(), busy: await page.evaluate(() => Seal.busy()) };
    await typeInto('W');
    const l3b = { busy: await page.evaluate(() => Seal.busy()), st: await st() };
    await page.evaluate(() => window.__hold); await until(() => page.evaluate(() => !Seal.busy()), 15000); await R.settle(page, 900);
    const l4 = await st();
    check('L3 Approve, then typing while the stamp comes down: the first press is kept, the letter typed under the stamp is in the box', l3h && l3a.busy && /Z$/.test(l3a.back.text.replace(/\s+/g, '')) && ['approved', 'written'].includes(l3a.back.state) && l3b.busy && /ZW$/.test(((l3b.st.words || {}).value || '').replace(/\s+/g, '')), JSON.stringify({ l3a, l3b: { busy: l3b.busy, words: l3b.st.words, approve: l3b.st.approve } }));
    check('L4 once the stamp is done the card stays on the piece with the edit kept (staged) and Approve lit: the earlier commit\'s move on never takes it away', l4.key === l0.key && /ZW$/.test(((l4.words || {}).value || '').replace(/\s+/g, '')) && l4.approve && l4.approve.a === 'approve' && !l4.approve.dis && l4.dirty, JSON.stringify({ l0: l0.key, key: l4.key, words: l4.words, approve: l4.approve, dirty: l4.dirty }));
    await quiet(); await page.evaluate(() => { Engrave.restoreView({ tab: 'place', list: true, chosen: true, focus: null }); Engrave.render(); }); await R.settle(page, 900);
    check('Z no page errors', errors.length === 0, errors.join(' | ').slice(0, 600));
  } catch (e) { console.error(e); check('ran', false, String(e && e.stack || e)); }
  finally { await w.close(); }
  const bad = RESULTS.filter(r => !r.ok);
  console.log(bad.length ? `engrave-stage-model: ${bad.length} failed` : `engrave-stage-model: ${RESULTS.length} checks passed`);
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
