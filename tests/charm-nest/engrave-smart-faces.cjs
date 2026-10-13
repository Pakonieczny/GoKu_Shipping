// ENGSMART (engrave-ui-1012 round 2, Paul point 4, first half): "it is not well communicated here what was actually chosen by the user and it kinda looks like the engraving is missing, so if there is no
// engraving it should not show it, if there is only back engraving show that, only front show only that, and if both then show both ... intelligent and dynamically adapted in both the Engraving tab detailed
// modal and the detailed Order Modal popup". Run on the REAL-DRAWING world (_engraveRealWorld.cjs: the real page, real charm drawings, real Engrave jobs; offline, no Etsy, no paid call, nothing live).
//   1  the order window card shows ONLY the faces the engraving needs: back-only -> the Back box; front-only -> the Front box; both -> two; none -> no card; earrings -> no card
//      (no dashed empty slot, no "Front" over nothing, no data-empty markup; a one-face card keeps its height and its picture size)
//   2  the Engrave detail shows the same faces, and the mode control changes them in the SAME tick: Front + back adds the second box, None removes the faces, Front / Back leave the one box,
//      a single face is selected by itself, clicking a box selects that face
//   3  no stray text: a face that is not shown has no box and no text anywhere; the Front's draft never shows in the Back box (and the other way round); the words under a picture are the words typed
//   4  the staged edit dies with its panel session: Cancel, Save, closing the detail, opening another piece; reopening the card shows what the system chose, never an old draft
//   5  the faces of an untouched card are exactly the system's choice (CharmNestEngraveSetup.autoChoice) for every order
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/engrave-smart-faces.cjs [screenshotDir]
'use strict';
const fs = require('fs'), path = require('path');
const R = require('./_engraveRealWorld.cjs');
const SHOTS = process.argv[2] || process.env.SHOTS || '';

(async () => {
  const w = await R.world({ shots: SHOTS || '/tmp/engsmart-shots', viewport: { width: 1440, height: 900 } });
  if (!w) { console.log('engrave-smart-faces: playwright-core not found, skipped'); return; }
  const fails = [], check = (ok, msg) => { if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
  const page = w.page, shot = async n => { if (SHOTS) await R.shot(w, n); };
  try {
    const made = await R.build(w, ['back', 'front', 'both', 'tag', 'discs', 'earrings']);
    const WORDS = { back: 'Name One', front: 'Name One' };

    /* ── in the page: what the cards of the order window draw ── */
    const cardsOf = () => page.evaluate(() => [...document.querySelectorAll('#owEng .engCard')].map(c => {
      const r = e => { const b = e.getBoundingClientRect(); return { l: Math.round(b.left), t: Math.round(b.top), w: Math.round(b.width), h: Math.round(b.height), r: Math.round(b.right) }; };
      const faces = [...c.querySelectorAll('.engFace')], th = c.querySelector('.engThumb'), tx = c.querySelector('.engTxt'), lb = c.querySelector('.engLbl');
      return { piece: c.dataset.piece, faces: faces.map(f => f.dataset.face), labels: faces.map(f => f.querySelector('.engLbl').textContent), texts: faces.map(f => f.querySelector('.engTxt').textContent), n: (c.querySelector('.engFaces') || { dataset: {} }).dataset.n || '0',
        emptyMarkup: c.querySelectorAll('[data-empty]').length, dashed: [...c.querySelectorAll('.engThumb')].filter(t => getComputedStyle(t).borderStyle.includes('dashed')).length,
        text: c.textContent.replace(/\s+/g, ' ').trim(), card: r(c), thumb: th ? r(th) : null, txt: tx ? r(tx) : null, lbl: lb ? r(lb) : null, thumbs: faces.length };
    }));
    const hostOf = () => page.evaluate(() => { const h = document.getElementById('owEng'); return { hidden: !h || h.hidden, kids: h ? h.querySelectorAll('.engCard').length : 0 }; });

    /* 1 ── the order window */
    const seen = {};
    for (const n of ['back', 'front', 'both', 'tag', 'discs', 'earrings']) {
      await R.openOrderWindow(w, n); seen[n] = { cards: await cardsOf(), host: await hostOf() };
    }
    const one = c => c.length === 1 ? c[0] : null;
    { const c = one(seen.back.cards); check(!!c && c.faces.join() === 'back' && c.labels.join() === 'Back' && c.texts[0] === WORDS.back, `back only: ONE card with ONE face, the Back, with its words: ${JSON.stringify(c && [c.faces, c.labels, c.texts])}`);
      check(!!c && !/Front/.test(c.text) && c.emptyMarkup === 0 && c.dashed === 0 && c.n === '1', `back only: no "Front" anywhere on the card, no dashed box, no empty-slot markup (n=${c && c.n})`); }
    { const c = one(seen.front.cards); check(!!c && c.faces.join() === 'front' && c.labels.join() === 'Front', `front only: ONE card with ONE face, the Front: ${JSON.stringify(c && [c.faces, c.labels, c.texts])}`);
      check(!!c && !/Back/.test(c.text) && c.emptyMarkup === 0 && c.dashed === 0, 'front only: no "Back" anywhere on the card, no dashed box'); }
    { const c = one(seen.both.cards); check(!!c && c.faces.join() === 'front,back' && c.labels.join() === 'Front,Back' && c.texts.every(t => t) && c.dashed === 0 && c.n === '2', `front + back: ONE card with both faces, Front first, each with its words: ${JSON.stringify(c && [c.faces, c.texts])}`); }
    { const c = one(seen.tag.cards); check(!!c && c.faces.join() === 'front,back', `a mini tag (front + back): both faces: ${JSON.stringify(c && c.faces)}`); }
    check(seen.discs.cards.length === 3 && seen.discs.cards.every(c => c.faces.length >= 1 && c.dashed === 0 && c.emptyMarkup === 0), `three discs: one card each, only the faces they engrave: ${JSON.stringify(seen.discs.cards.map(c => c.faces.join('+')))}`);
    check(seen.earrings.cards.length === 0 && seen.earrings.host.hidden, `earrings: no card at all (the section is hidden): ${JSON.stringify(seen.earrings.host)}`);
    { const b = one(seen.back.cards), f = one(seen.front.cards), x = one(seen.both.cards);
      check(b && x && b.thumb.w === x.thumb.w && b.thumb.h === x.thumb.h, `a one-face card keeps the picture size of a two-face card: ${b && b.thumb.w}x${b && b.thumb.h} vs ${x && x.thumb.w}x${x && x.thumb.h}`);
      check(b && x && b.card.h <= x.card.h && x.card.h <= 212, `a one-face card is never taller than a two-face card (${b && b.card.h} <= ${x && x.card.h} <= 212)`);
      check(b && b.thumb.l - b.card.l <= 12 && b.txt.l >= b.thumb.r && b.lbl.l >= b.thumb.r, `a one-face card: the picture on the left, its title and words beside it (thumb.right ${b && b.thumb.r} <= words.left ${b && b.txt.l})`);
      check(f && f.thumb.w === b.thumb.w, 'the front-only card has the same picture size'); }
    await R.openOrderWindow(w, 'back'); await shot('SMART-order-back');
    await R.openOrderWindow(w, 'front'); await shot('SMART-order-front');
    await R.openOrderWindow(w, 'both'); await shot('SMART-order-both');

    /* none: a person's mode "None" saved through the real save path: no card, no face */
    const noneKey = made.back.key;
    const saved = await page.evaluate(async key => { const row = B.orders.byKey.get(key); const res = await Engrave.saveSetup(row, { mode: 'none' }); return { ok: !!(res && res.ok), msg: res && res.message || '', mode: CharmNestEngraveSetup.setupFor(row).mode }; }, noneKey);
    check(saved.ok && saved.mode === 'none', `None saved through the page's own save path: ${JSON.stringify(saved)}`);
    await R.openOrderWindow(w, 'back'); { const c = await cardsOf(), h = await hostOf(); check(c.length === 0 && h.hidden, `none: no card and no face (the section is hidden): cards ${c.length}, hidden ${h.hidden}`); }
    await page.evaluate(async key => { const row = B.orders.byKey.get(key); await Engrave.saveSetup(row, { mode: 'back', back: { text: 'Name One', fontWord: null } }); }, noneKey);
    await R.openOrderWindow(w, 'back'); { const c = await cardsOf(); check(c.length === 1 && c[0].faces.join() === 'back', `the Back mode saved again: the Back box is back: ${JSON.stringify(c.map(x => x.faces))}`); }

    /* pure model, in the page: the staged mode decides the faces (the Engrave detail passes it) */
    const model = await page.evaluate(key => {
      const row = B.orders.byKey.get(key), C = CharmNestEngraveCard, g = (stage, o) => C.piecesOf([row], Object.assign({ stage }, o || {})).map(p => ['front', 'back'].filter(f => p.faces[f]).join('+'));
      const txt = f => { const p = C.piecesOf([row], { stage: { mode: 'both', faces: { front: { text: 'rr', font: '' }, back: { text: 'Name One', font: '' } } } })[0]; return p.faces[f] && p.faces[f].text; };
      return { both: g({ mode: 'both' }), front: g({ mode: 'front' }), back: g({ mode: 'back' }), none: g({ mode: 'none' }), noneKeep: g({ mode: 'none' }, { keepEmpty: true }), saved: g(null), rr: [txt('front'), txt('back')] };
    }, noneKey);
    check(model.both[0] === 'front+back' && model.front[0] === 'front' && model.back[0] === 'back' && model.none.length === 0 && model.noneKeep[0] === '' && model.saved[0] === 'back', `piecesOf follows a staged mode: ${JSON.stringify(model)}`);
    check(model.rr[0] === 'rr' && model.rr[1] === 'Name One', 'a staged face carries only its own typed words (Front rr, Back Name One)');

    /* 2 ── the Engrave detail */
    const detail = () => page.evaluate(() => {
      const c = document.querySelector('#egQueue .rvItem[data-kind=placement] .engCard'), p = document.querySelector('#egQueue .engp');
      if (!c) return null;
      const faces = [...c.querySelectorAll('.engFace')];
      return { faces: faces.map(f => f.dataset.face), cur: faces.filter(f => f.getAttribute('aria-current') === 'true').map(f => f.dataset.face), labels: faces.map(f => f.querySelector('.engLbl').textContent), texts: faces.map(f => f.querySelector('.engTxt').textContent),
        thumbText: Object.fromEntries(faces.map(f => [f.dataset.face, (f.querySelector('.engpvText') || {}).textContent || ''])), text: c.textContent.replace(/\s+/g, ' ').trim(), name: (c.querySelector('.engName') || {}).textContent || '',
        dashed: [...c.querySelectorAll('.engThumb')].filter(t => getComputedStyle(t).borderStyle.includes('dashed')).length, emptyMarkup: c.querySelectorAll('[data-empty]').length,
        mode: p ? (p.querySelector('.engpSeg[aria-checked=true]') || {}).dataset.mode : null, pface: p ? p.dataset.face : null, words: p && p.querySelector('.engpWords') ? p.querySelector('.engpWords').value : null,
        save: p ? !p.querySelector('[data-engp=save]').disabled : null, cancel: p ? !p.querySelector('[data-engp=cancel]').disabled : null };
    });
    const clickMode = m => page.evaluate(m => { const b = document.querySelector(`#egQueue .engp [data-engp=mode][data-mode=${m}]`); b.click();   // (ONE task: what the person would see the moment the press is handled)
      const c = document.querySelector('#egQueue .rvItem[data-kind=placement] .engCard'), faces = c ? [...c.querySelectorAll('.engFace')] : [];
      return { faces: faces.map(f => f.dataset.face), cur: faces.filter(f => f.getAttribute('aria-current') === 'true').map(f => f.dataset.face), hasCard: !!c, text: c ? c.textContent.replace(/\s+/g, ' ') : '' }; }, m);
    const keyOfPiece = k => page.evaluate(k => k, k);
    await R.openEngraveDetail(w, 'back', { face: 'back' });
    let d = await detail();
    check(!!d && d.faces.join() === 'back' && d.cur.join() === 'back' && d.mode === 'back' && d.pface === 'back', `the Engrave detail of a back-only piece: only the Back box, selected by itself, the mode says Back: ${JSON.stringify(d && [d.faces, d.cur, d.mode])}`);
    check(!!d && d.dashed === 0 && d.emptyMarkup === 0 && !/Front/.test(d.text), 'the detail card: no dashed box and no "Front" over nothing');
    await shot('SMART-detail-back');
    let r = await clickMode('both');
    check(r.faces.join() === 'front,back', `Front + back pressed: the second box appears in the same tick: ${JSON.stringify(r.faces)}`);
    d = await detail(); check(d.faces.join() === 'front,back' && d.cur.length === 1 && d.dashed === 0 && d.save, `both boxes, one selected (${d.cur}), none dashed, Save is on`);
    await shot('SMART-detail-both-staged');
    r = await clickMode('none'); check(r.faces.length === 0 && r.hasCard && !/Front|Back/.test(r.text), `None pressed: the faces go in the same tick (the piece's name stays): ${JSON.stringify(r.faces)}`);
    await shot('SMART-detail-none-staged');
    r = await clickMode('front'); check(r.faces.join() === 'front' && r.cur.join() === 'front', `Front pressed: only the Front box, selected by itself: ${JSON.stringify(r)}`);
    r = await clickMode('back'); check(r.faces.join() === 'back' && r.cur.join() === 'back', `Back pressed: only the Back box, selected by itself: ${JSON.stringify(r)}`);
    r = await clickMode('both');
    // a press on a box selects that face (and the panel edits it)
    await page.click('#egQueue .engCard .engFace[data-face=front] .engThumb');
    d = await detail(); check(d.cur.join() === 'front' && d.pface === 'front', `a press on the Front box selects the Front (card ${d.cur}, panel ${d.pface})`);
    await page.click('#egQueue .engCard .engFace[data-face=back] .engThumb');
    d = await detail(); check(d.cur.join() === 'back' && d.pface === 'back', `a press on the Back box selects the Back (card ${d.cur}, panel ${d.pface})`);

    /* 3 ── no stray text */
    await page.click('#egQueue .engCard .engFace[data-face=front] .engThumb');
    await page.fill('#egQueue .engp .engpWords', 'rr');
    d = await detail();
    check(d.thumbText.front === 'rr' && !/rr/.test(d.thumbText.back) && d.texts[0] === 'rr' && d.texts[1] !== 'rr', `typing "rr" on the Front shows in the Front only (picture ${JSON.stringify(d.thumbText.front)}, words under it ${JSON.stringify(d.texts[0])}) and never in the Back box (${JSON.stringify(d.thumbText.back)} / ${JSON.stringify(d.texts[1])})`);
    r = await clickMode('back');
    d = await detail(); check(d.faces.join() === 'back' && !/rr/.test(d.text) && !Object.values(d.thumbText).some(t => /rr/.test(t)), `Back pressed again: the Front box is gone and "rr" is nowhere on the card: ${JSON.stringify(d.faces)} "${d.text}"`);
    await page.click('#egQueue .engp [data-engp=cancel]'); d = await detail();
    check(d.faces.join() === 'back' && d.mode === 'back' && !d.save && !/rr/.test(d.text), `Cancel: the card and the panel are back on what is saved (Back only, Save off): ${JSON.stringify([d.faces, d.mode, d.save])}`);
    const drafts = () => page.evaluate(() => [...CharmNestEngravePanel.DRAFTS.entries()].filter(([k, d]) => d.dirty || d.posTouched).map(([k]) => k));   // (a staged edit: a clean draft is only the panel's copy of what is saved)
    const allDrafts = () => page.evaluate(() => [...CharmNestEngravePanel.DRAFTS.keys()]);
    check((await drafts()).length === 0, 'Cancel ends the staged edit (no draft kept)');

    /* 4 ── the staged edit dies with its panel session */
    const stage = async () => { await clickMode('both'); await page.click('#egQueue .engCard .engFace[data-face=front] .engThumb'); await page.fill('#egQueue .engp .engpWords', 'rr'); };
    await stage(); check((await drafts()).length === 1, 'a staged edit (Front + back, "rr" on the Front) is kept while the card is open');
    // a redraw of the same card (the tab draws the detail again on every change) keeps it
    await page.evaluate(() => Engrave.render()); await R.settle(page, 400);
    d = await detail(); check(d.mode === 'both' && d.faces.join() === 'front,back' && d.thumbText.front === 'rr', `the tab drawing the detail again does not lose the edit: ${JSON.stringify([d.mode, d.faces, d.thumbText.front])}`);
    // closing the detail (the list) ends it
    await page.evaluate(() => { Engrave.restoreView({ tab: 'place', list: true, chosen: true, focus: null }); Engrave.render(); }); await R.settle(page, 700);
    check((await allDrafts()).length === 0, `closing the detail ends the staged edit (and the panel's copy of the saved state): drafts ${JSON.stringify(await allDrafts())}`);
    await R.openEngraveDetail(w, 'back', { face: 'back' }); d = await detail();
    check(d.faces.join() === 'back' && d.mode === 'back' && !d.save && !/rr/.test(d.text) && d.words !== 'rr', `reopening the card shows what the system chose, not the old draft: ${JSON.stringify([d.faces, d.mode, d.save, d.words])}`);
    // another piece opened ends it
    await stage(); check((await drafts()).length === 1, 'staged again');
    await R.openEngraveDetail(w, 'front', { face: 'front' }); await R.settle(page, 500);
    check((await drafts()).length === 0, `opening another order's piece ends the staged edit: ${JSON.stringify(await drafts())}`);
    d = await detail(); check(d.faces.join() === 'front' && d.mode === 'front' && !d.save, `the other piece opens on its own faces (Front only): ${JSON.stringify([d.faces, d.mode])}`);
    await R.openEngraveDetail(w, 'back', { face: 'back' }); d = await detail();
    check(d.faces.join() === 'back' && d.mode === 'back' && !/rr/.test(d.text), `back on the first piece: no old draft: ${JSON.stringify([d.faces, d.mode])}`);
    // the other face of the SAME piece (the tab shows another job) is still the same session
    await R.openEngraveDetail(w, 'both', { face: 'back' }); await clickMode('both'); await page.click('#egQueue .engCard .engFace[data-face=front] .engThumb'); await page.fill('#egQueue .engp .engpWords', 'rr');
    await R.settle(page, 500); d = await detail(); check(d.cur.join() === 'front' && /^rr$/i.test(d.thumbText.front), `the two faces of one piece share one session while the other face's job is drawn: ${JSON.stringify([d.cur, d.thumbText])}`);
    // Save (the page's own save path stubbed to say ok: nothing is written) ends it too
    await page.evaluate(() => { window.__saveWas = Engrave.saveSetup; Engrave.saveSetup = async () => ({ ok: true }); });
    await page.click('#egQueue .engp [data-engp=save]'); await R.settle(page, 300);
    check((await drafts()).length === 0, 'Save ends the staged edit');
    await page.evaluate(() => { Engrave.saveSetup = window.__saveWas; });

    /* 5 ── an untouched card is exactly the system's choice */
    for (const n of ['back', 'front', 'both', 'tag', 'discs']) {
      const cmp = await page.evaluate(key => {
        const row = B.orders.byKey.get(key); row.engravingOverride = null;
        const auto = CharmNestEngraveSetup.autoChoice ? CharmNestEngraveSetup.autoChoice(row) : null, ps = CharmNestEngraveCard.piecesOf([row], {});
        return { auto: auto && auto.faces.join('+'), cards: [...new Set(ps.map(p => ['front', 'back'].filter(f => p.faces[f]).join('+')))].join('|'), have: !!auto };
      }, made[n].key);
      check(!cmp.have || cmp.auto === cmp.cards, `${n}: the card's faces are the system's choice (autoChoice ${cmp.auto}, card ${cmp.cards})`);
    }
    check(w.errors.length === 0, 'no page errors: ' + w.errors.join(' | '));
  } catch (e) { fails.push('test crashed: ' + (e && e.stack || e)); console.error(e); }
  finally { await w.close(); }
  if (fails.length) { console.log('\nengrave-smart-faces: FAILED\n - ' + fails.join('\n - ')); process.exit(1); }
  console.log('engrave-smart-faces: ok');
})().catch(e => { console.error(e); process.exit(1); });
