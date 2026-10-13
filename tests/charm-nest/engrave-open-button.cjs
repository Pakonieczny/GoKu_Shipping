// ENGBUTTON (engrave-ui-1012, round 2b). Paul: "bring back the engraving button that we had, not just a cheap clickable card which doesn't even have correct hover states".
// The order window's engraving card has ONE real Engraving button again (the very `btn ghost sm egOpen` of the old card: "Fix in Engraving →", "View in Engraving →" once decided, gold while the words
// are unclear) with proper states, and its two thumbnails are real buttons too (a second, quieter way in).
// This test runs the REAL page in headless Chromium over ENGLIVE's real-drawing world (tests/charm-nest/_engraveRealWorld.cjs: real .ai drawings, real intake, pool, classifier, fit; offline, nothing
// written anywhere real, no Etsy, no paid call, made-up names) and checks:
//   1  the button exists on EVERY card (back only, front only, front + back, three discs): a <button type=button>, the old words, one per card, inside the card, nothing spills out of it
//   2  a press opens the right piece and the right face through the real EngraveLink.open({ rid, key, poolId, piece, face }): the order window closes, the Engrave tab is on THAT job; a thumbnail press
//      opens its own face; a press on the card's name / words / empty places opens nothing
//   3  every state differs in the computed styles: default / hover / focus-visible / active (pressed) / disabled (aria-disabled, a title that says why, nothing happens on a press) / loading (the labelled
//      spinner, "Opening Engraving…", full strength, a progress cursor, focus kept, a second press ignored); the same for the thumbnails (hover, pressed, focus ring, cursor)
//   4  the keyboard: Tab reaches the button and the thumbnails (in that card's order), Enter and Space press them
//   5  the three older places (the card of the Sheet view / sheet window / Review: charm-nest-engraving-seals.js panel()) draw the SAME button: same classes, same words, same computed look (only the card's
//      compact height differs), the same hover and pressed fill
//   6  the card is still about 175px tall at 207px wide (the noscroll test keeps the first row on screen)
// Screenshots of the states (the card with real drawings) go to round2/images/BUTTON-*.png (SHOTS=<dir> to put them elsewhere).
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/engrave-open-button.cjs      (about 1-2 minutes; CHROMIUM=<chrome> when the default path is missing)
// What a pass does NOT prove: a live order, the animation (ENGANIM's test), the way back (ENGBACK's test), any browser but headless Chromium.
'use strict';
const fs = require('fs'), path = require('path');
const R = require('./_engraveRealWorld.cjs');
const SHOTS = process.env.SHOTS || '/mnt/project-files/plans/engrave-ui-1012/round2/images';
const pwDir = ['/opt/node22/lib/node_modules/playwright/node_modules', path.join(R.root, 'node_modules')].find(d => fs.existsSync(path.join(d, 'playwright-core')));
if (!process.env.PW_DIR && pwDir) process.env.PW_DIR = pwDir;
if (!process.env.CHROMIUM) { const c = ['/opt/pw-browsers/chromium-1194/chrome-linux/chrome', '/opt/pw-browsers/chromium'].find(x => fs.existsSync(x)); if (c) process.env.CHROMIUM = c; }

let failed = 0, passed = 0;
const check = (ok, msg, extra) => { if (ok) { passed++; console.log('  PASS ' + msg); } else { failed++; console.log('  FAIL ' + msg + (extra !== undefined ? ' :: ' + JSON.stringify(extra) : '')); } };
const sleep = ms => new Promise(r => setTimeout(r, ms));

/** what a button looks like right now: the computed properties that a state may change */
const LOOK = () => {
  const sel = window.__look; const b = document.querySelector(sel); if (!b) return null;
  const cs = getComputedStyle(b), arrow = b.querySelector('span[aria-hidden]'), r = b.getBoundingClientRect();
  return { bg: cs.backgroundColor, border: cs.borderTopColor, color: cs.color, opacity: cs.opacity, transform: cs.transform, outline: cs.outlineStyle + ' ' + cs.outlineWidth + ' ' + cs.outlineColor, shadow: cs.boxShadow, cursor: cs.cursor,
    arrow: arrow ? getComputedStyle(arrow).transform : '', font: cs.fontFamily + '|' + cs.fontWeight + '|' + cs.fontSize, radius: cs.borderTopLeftRadius, borderW: cs.borderTopWidth + ' ' + cs.borderTopStyle, h: Math.round(r.height * 10) / 10, w: Math.round(r.width * 10) / 10,
    text: b.textContent.replace(/\s+/g, ' ').trim() };
};
const look = async (page, sel) => { await page.evaluate(s => { window.__look = s; }, sel); return page.evaluate(LOOK); };
const differ = (a, b, keys) => keys.filter(k => a[k] !== b[k]);

async function main() {
  const w = await R.world({ viewport: { width: 1280, height: 720 }, shots: SHOTS, scale: 2 });
  if (!w) { console.log('  - no playwright-core: the browser checks were not run (set PW_DIR=<dir holding playwright-core>)'); return 0; }
  const page = w.page;
  try {
    const made = await R.build(w, ['both', 'back', 'front', 'discs']);
    for (const n of ['both', 'back', 'front', 'discs']) if (!made[n] || !made[n].jobs.length) { check(false, `the world made no engraving job for ${n}`); return finish(); }
    await page.evaluate(() => {
      window.__links = []; window.__hold = 0; window.__go = true;
      const real = EngraveLink.open.bind(EngraveLink);
      EngraveLink.open = async t => { window.__links.push(JSON.parse(JSON.stringify(t))); if (window.__hold) await new Promise(r => setTimeout(r, window.__hold)); return window.__go ? real(t) : true; };
    });
    const cardsOf = () => page.evaluate(() => [...document.querySelectorAll('#owEng > .engCard')].map(c => {
      const b = [...c.querySelectorAll('.egOpen')], r = c.getBoundingClientRect(), ths = [...c.querySelectorAll('.engThumb')], br = b[0] && b[0].getBoundingClientRect();
      return { piece: c.dataset.piece, key: c.dataset.key, slot: c.dataset.slot || '', state: c.dataset.state, n: b.length, tag: b[0] && b[0].tagName, type: b[0] && b[0].getAttribute('type'), text: b[0] && b[0].textContent.replace(/\s+/g, ' ').trim(), cls: b[0] && [...b[0].classList].join(' '), de: b[0] && b[0].dataset.e, face: b[0] && b[0].dataset.face,
        label: b[0] && b[0].getAttribute('aria-label'), inside: !!br && br.left >= r.left && br.right <= r.right + 0.5 && br.bottom <= r.bottom + 0.5, spill: c.scrollWidth - c.clientWidth, w: r.width, h: r.height,
        faces: [...c.querySelectorAll('.engFace')].map(f => f.dataset.face), thumbs: ths.map(t => ({ tag: t.tagName, type: t.getAttribute('type'), label: t.getAttribute('aria-label'), tab: t.tabIndex, face: t.closest('.engFace').dataset.face })) };
    }));
    const jobOf = (name, face, slot) => made[name].jobs.find(j => j.face === face && (!slot || j.slot === slot));

    /* ── 1 · the button on every card ── */
    console.log('\n  1 · the Engraving button on every card');
        for (const n of ['both', 'back', 'front', 'discs']) {
      await R.openOrderWindow(w, n);
      const cards = await cardsOf();
      const want = n === 'discs' ? 3 : 1;
      check(cards.length === want, `${n}: ${want} card(s) drawn (${cards.length})`);
      for (const c of cards) {
        const id = `${n}${c.slot ? ' ' + c.slot : ''}`;
        check(c.n === 1 && c.tag === 'BUTTON' && c.type === 'button' && c.de === 'engrave', `${id}: exactly one real <button type=button data-e=engrave> in the card`, c);
        check(/^(Fix|View) in Engraving →$/.test(c.text) && /\bbtn\b/.test(c.cls) && /\bsm\b/.test(c.cls) && /\begOpen\b/.test(c.cls) && /\b(ghost|gold)\b/.test(c.cls), `${id}: the old button's words and classes ("${c.text}", ${c.cls})`);
        check(c.inside && c.spill <= 0, `${id}: the button sits inside the card and nothing spills out of it`, { inside: c.inside, spill: c.spill });
        console.log(`  info ${id}: faces shown ${c.faces.join('+')} (which faces a card shows is ENGSMART's; the button is on every card whatever they are)`);
        check(c.thumbs.every(t => t.tag === 'BUTTON' && t.type === 'button' && t.tab === 0 && /engraving of .+: open in Engraving/.test(t.label || '')), `${id}: each thumbnail is a real focusable button with its own name (aria-label)`, c.thumbs);
        check(c.h <= 180 && c.h >= 120 && c.w > 190 && c.w < 215, `${id}: the card is ${Math.round(c.h)}px tall at ${Math.round(c.w)}px wide (175 with two pictures, less with one)`);
      }
      check(await page.evaluate(() => [...document.querySelectorAll('#owEng > .engCard')].every(c => c.querySelectorAll('.egOpen').length === 1) && !document.querySelector('#owEng .engCard a[href], #owEng .engCard [role=link]')), `${n}: one button per card, no link or role=link stand-in`);
    }

    /* ── 2 · a press opens the right piece and the right face (the real EngraveLink, the real landing) ── */
    console.log('\n  2 · a press opens the right piece and face');
    const landed = () => page.evaluate(() => { const v = Engrave.view(); const ow = document.getElementById('orderWin'); return { focus: v.focus, tab: v.tab, orderOpen: !!(ow && ow.open), engrave: !document.getElementById('engraveView').classList.contains('hidden'), shown: [...document.querySelectorAll('#engraveView #egQueue .rvItem')].some(n => n.dataset.key === v.focus) }; });
    const cardSel = (name, slot) => `#owEng > .engCard[data-key="${made[name].key}"]${slot ? `[data-slot="${slot}"]` : ''}`;
    const opened = async (pressSel, name, face, slot, how) => {
      await R.openOrderWindow(w, name); await page.evaluate(() => { window.__links.length = 0; window.__go = true; window.__hold = 0; });
      await page.click(pressSel); await page.waitForFunction(() => window.__links.length >= 1, null, { timeout: 8000 }).catch(() => {});
      const l = await page.evaluate(() => window.__links[0] || null);
      await page.waitForFunction(() => { const ow = document.getElementById('orderWin'); return !(ow && ow.open) && !document.getElementById('engraveView').classList.contains('hidden'); }, null, { timeout: 12000 }).catch(() => {});
      await sleep(400); const at = await landed(), job = jobOf(name, face, slot), m = made[name];
      const id = `${name}${slot ? ' ' + slot : ''} ${how}`;
      check(!!l && l.rid === m.rid && l.key === m.key && (m.poolIds || []).includes(l.poolId) && l.face === face && l.piece === (job && job.key), `${id}: EngraveLink.open({ rid, key, poolId, piece, face }) = this order, this piece's ${face} job`, { l, want: { rid: m.rid, key: m.key, piece: job && job.key, face } });
      check(!at.orderOpen && at.engrave && at.focus === (job && job.key) && at.shown, `${id}: the order window closed and the Engrave tab is on that job's placement card`, { at, want: job && job.key });
    };
    await opened(cardSel('back') + ' .egOpen', 'back', 'back', '', 'button');
    await opened(cardSel('front') + ' .egOpen', 'front', 'front', '', 'button');
    await opened(cardSel('both') + ' .egOpen', 'both', 'front', '', 'button');                    // (both faces wait for approval: it opens the first one shown)
    await opened(cardSel('both') + ' .engFace[data-face=back] .engThumb', 'both', 'back', '', 'back thumbnail');
    await opened(cardSel('discs', 'D2') + ' .egOpen', 'discs', 'back', 'D2', 'button');
    await opened(cardSel('discs', 'D3') + ' .engFace[data-face=back] .engThumb', 'discs', 'back', 'D3', 'back thumbnail');
    // a press on what is NOT a control (the name, the words under a picture, the empty places) opens nothing: the card is not a clickable card
    await R.openOrderWindow(w, 'both'); await page.evaluate(() => { window.__links.length = 0; });
    for (const s of ['.engName', '.engFace[data-face=front] .engTxt', '.engFace[data-face=back] .engLbl', '.engHd']) await page.click(cardSel('both') + ' ' + s, { position: { x: 3, y: 3 } }).catch(() => {});
    await sleep(300);
    check(await page.evaluate(() => window.__links.length === 0 && document.getElementById('orderWin').open), 'a press on the name, the words, a title or the header opens nothing (only the button and the pictures do)');

    /* ── 3 · the states ── */
    console.log('\n  3 · the states differ');
    await R.openOrderWindow(w, 'both'); await page.evaluate(() => { window.__go = false; window.__links.length = 0; });
    const BTN = cardSel('both') + ' .egOpen', TH = cardSel('both') + ' .engFace[data-face=back] .engThumb';
    const clip = async () => page.evaluate(() => { const r = document.querySelector('#owEng > .engCard').getBoundingClientRect(); const s = (document.querySelector('.engSec') || document.getElementById('owEng')).getBoundingClientRect(); return { x: Math.max(0, r.left - 14), y: Math.max(0, s.top - 8), width: r.width + 28, height: r.bottom - s.top + 22 }; });
    const away = async () => { await page.mouse.move(5, 5); await page.evaluate(() => getSelection().removeAllRanges()); await sleep(260); };
    await away(); const d0 = await look(page, BTN); await R.shot(w, 'BUTTON-default', { clip: await clip() });
    await page.hover(BTN); await sleep(300); const dh = await look(page, BTN); await R.shot(w, 'BUTTON-hover', { clip: await clip() });
    await page.mouse.down(); await sleep(250); const da = await look(page, BTN); await R.shot(w, 'BUTTON-active', { clip: await clip() });
    await page.mouse.move(5, 5); await page.mouse.up(); await away();
    check(differ(d0, dh, ['bg', 'border', 'arrow']).length >= 2, `button hover differs from default (${differ(d0, dh, ['bg', 'border', 'arrow', 'shadow', 'transform']).join(', ')}): fill ${d0.bg} -> ${dh.bg}, edge ${d0.border} -> ${dh.border}`, { d0, dh });
    check(differ(dh, da, ['bg', 'transform']).length >= 1 && da.transform !== d0.transform, `button pressed differs from hover and from default (${differ(dh, da, ['bg', 'border', 'transform']).join(', ')}): ${dh.transform} -> ${da.transform}`, { dh, da });
    check(d0.cursor === 'pointer' && dh.cursor === 'pointer', 'button cursor is the pointer', { d0: d0.cursor });
    await page.focus(BTN); await page.keyboard.press('Shift+Tab'); await page.keyboard.press('Tab'); await sleep(250);   // (arrived by the keyboard: :focus-visible)
    const isFocus = await page.evaluate(s => { const b = document.querySelector(s); return b === document.activeElement && b.matches(':focus-visible'); }, BTN);
    const df = await look(page, BTN); await R.shot(w, 'BUTTON-focus', { clip: await clip() });
    check(isFocus && /solid 2px/.test(df.outline) && d0.outline !== df.outline, `button keyboard focus draws the 2px ring: "${d0.outline}" -> "${df.outline}"`, { isFocus, df });
    await page.evaluate(() => document.activeElement && document.activeElement.blur());

    // the thumbnail (secondary target): hover, pressed, focus ring, pointer
    await away(); const t0 = await look(page, TH);
    await page.hover(TH); await sleep(300); const th = await look(page, TH);
    await page.mouse.down(); await sleep(250); const ta = await look(page, TH); await R.shot(w, 'BUTTON-thumb-pressed', { clip: await clip() });
    await page.mouse.move(5, 5); await page.mouse.up(); await away();
    check(differ(t0, th, ['border', 'shadow', 'transform']).length >= 2 && t0.cursor === 'pointer', `thumbnail hover differs from default (${differ(t0, th, ['border', 'shadow', 'transform']).join(', ')}), pointer cursor`, { t0, th });
    check(differ(th, ta, ['border', 'shadow', 'transform']).length >= 2, `thumbnail pressed differs from hover (${differ(th, ta, ['border', 'shadow', 'transform']).join(', ')})`, { th, ta });
    await page.focus(TH); await page.keyboard.press('Shift+Tab'); await page.keyboard.press('Tab'); await sleep(250);
    const tf = await look(page, TH);
    check(await page.evaluate(s => document.querySelector(s) === document.activeElement && document.querySelector(s).matches(':focus-visible'), TH) && /solid 2px/.test(tf.outline) && t0.outline !== tf.outline, `thumbnail keyboard focus draws its ring: "${t0.outline}" -> "${tf.outline}"`, { tf });
    await page.evaluate(() => document.activeElement && document.activeElement.blur());

    // disabled (cannot open: the reason is the title) and loading (the labelled spinner): the real card, built in the page with the real piece, in a 207px cell like the order window's
    const mk = opts => page.evaluate(async o => {
      document.querySelectorAll('#btStage').forEach(n => n.remove());
      const C = CharmNestEngraveCard, row = B.orders.byKey.get(o.key), ps = C.piecesOf([row], { nameOf: () => 'SCHRODINGER CAT 2', isWorking: j => Engrave.isWorking(j) }), p0 = ps[0];
      if (window.__btSaved) { window.EngraveLink = window.__btSaved.L; Engrave.restoreView = window.__btSaved.rv; window.__btSaved = null; }   // (what an earlier call took away is given back)
      if (o.notReady) { window.__btSaved = { L: window.EngraveLink, rv: Engrave.restoreView }; window.EngraveLink = undefined; Engrave.restoreView = undefined; }   // (the page has no way to open Engraving: until the next call)
      let piece = o.gone ? Object.assign({}, p0, { row: Object.create(row, { state: { value: 'gone' } }) }) : p0;
      if (o.away) { piece = Object.assign({}, p0, { jobs: [] }); window.__realResolve = window.__realResolve || EngraveLink.resolve; EngraveLink.resolve = () => ({ job: null, tab: null, why: 'This piece is on hold and not on a sheet yet, so there is no engraving to open.' }); }
      window.__btCalls = []; window.__btRelease = null;
      const stage = document.createElement('div'); stage.id = 'btStage'; stage.style.cssText = 'position:fixed;left:30px;top:200px;width:207px;z-index:2147483000;background:#fffefb';
      document.body.append(stage);
      const el = C.card(piece, { onOpen: (face, key, btn) => { window.__btCalls.push([face, key, btn.className.split(' ')[0]]); return o.pending ? new Promise(r => { window.__btRelease = r; }) : null; } });
      stage.append(el); C.repaint(stage);
      return { n: stage.querySelectorAll('.egOpen').length };
    }, opts);
    const SB = '#btStage .egOpen', ST = '#btStage .engFace[data-face=back] .engThumb';
    const stageClip = () => page.evaluate(() => { const r = document.getElementById('btStage').getBoundingClientRect(); return { x: Math.max(0, r.left - 12), y: Math.max(0, r.top - 12), width: r.width + 24, height: r.height + 24 }; });
    // (the order window is a modal dialog: everything outside it is inert, so it is closed first and the stage stands on the page itself; every press below is a real one)
    await page.evaluate(async () => { if (OrderWin.close) await OrderWin.close(); const d = document.getElementById('orderWin'); if (d.open) d.close(); }); await sleep(300);
    // disabled: not ready
    await mk({ key: made.both.key, notReady: true });
    const dd = await page.evaluate(s => { const b = document.querySelector(s); return { disabledAttr: b.disabled, aria: b.getAttribute('aria-disabled'), title: b.title, tab: b.tabIndex, text: b.textContent.replace(/\s+/g, ' ').trim() }; }, SB);
    await page.hover(SB); await sleep(250); const dis = await look(page, SB); await R.shot(w, 'BUTTON-disabled', { clip: await stageClip() });
    await page.click(SB, { force: true }); await page.click(ST, { force: true }); await sleep(150);   // (force: Playwright does not press an aria-disabled button by itself; this is still a real mouse press on a page with no modal over it)
    const noCalls = await page.evaluate(() => window.__btCalls.length === 0);
    await mk({ key: made.both.key }); await away(); const en = await look(page, SB);
    check(dd.aria === 'true' && /not ready/i.test(dd.title) && dd.tab === 0 && !dd.disabledAttr, `disabled: aria-disabled, still focusable, the reason is its title ("${dd.title}")`, dd);
    check(+dis.opacity < 0.6 && dis.cursor === 'not-allowed' && dis.bg === en.bg && dis.transform === en.transform && dis.arrow === en.arrow, `disabled looks disabled (opacity ${dis.opacity}, cursor ${dis.cursor}) and does not react to the pointer`, { dis, en });
    check(noCalls, 'a press on the disabled button and on its thumbnails opens nothing');
    await mk({ key: made.both.key, gone: true });
    check(await page.evaluate(s => { const b = document.querySelector(s); return b.getAttribute('aria-disabled') === 'true' && /left the pull/.test(b.title); }, SB), 'disabled for an order that has left the pull: the title says so');
    // nothing to open yet: a piece with no engraving job that none is coming for (on hold, or on no sheet yet): EngraveLink.resolve says why, the button is off with that line as its title,
    // and it comes on by itself at the next hover or press once the piece has its job (it is asked again each time, never trusted from when the card was drawn)
    await mk({ key: made.both.key, away: true }); await page.hover(SB); await sleep(200);
    const hold = await page.evaluate(s => { const b = document.querySelector(s); return { aria: b.getAttribute('aria-disabled'), title: b.title, thumb: document.querySelector('#btStage .engThumb').getAttribute('aria-disabled') }; }, SB);
    await page.click(SB, { force: true }); await sleep(100);
    const heldCalls = await page.evaluate(() => window.__btCalls.length);
    check(hold.aria === 'true' && /on hold and not on a sheet yet/.test(hold.title) && hold.thumb === 'true' && heldCalls === 0, `a piece with no job to come: off, the title says why ("${hold.title}"), a press opens nothing`, { hold, heldCalls });
    await page.evaluate(() => { EngraveLink.resolve = () => ({ job: {}, tab: 'place', why: '' }); });
    await page.mouse.move(5, 5); await page.hover(SB); await sleep(200);
    const now = await page.evaluate(s => { const b = document.querySelector(s); return { aria: b.getAttribute('aria-disabled'), title: b.title }; }, SB);
    await page.click(SB); await sleep(100);
    check(now.aria === null && now.title === '' && await page.evaluate(() => window.__btCalls.length === 1), 'and once the piece has its job the button is on again at the next hover, and a press opens it', now);
    await page.evaluate(() => { EngraveLink.resolve = window.__realResolve; });
    // loading
    await mk({ key: made.both.key, pending: true }); await away();
    await page.focus(SB); const before = await look(page, SB);
    await page.keyboard.press('Enter'); await sleep(200);
    const ld = await page.evaluate(s => { const b = document.querySelector(s); return { busy: b.getAttribute('aria-busy'), text: b.textContent.replace(/\s+/g, ' ').trim(), spin: !!b.querySelector('.spin'), spinAnim: b.querySelector('.spin') ? getComputedStyle(b.querySelector('.spin')).animationName : '', focus: document.activeElement === b, calls: window.__btCalls.length, card: document.querySelector('#btStage .engCard').dataset.opening }; }, SB);
    const lk = await look(page, SB); await R.shot(w, 'BUTTON-loading', { clip: await stageClip() });
    await page.click(SB); await page.click(ST); await sleep(150);
    const again = await page.evaluate(() => window.__btCalls.length);
    check(ld.busy === 'true' && ld.text === 'Opening Engraving…' && ld.spin && ld.spinAnim !== 'none' && ld.card === '1', `loading: aria-busy, a spinner and the words "${ld.text}"`, ld);
    check(ld.focus && ld.calls === 1 && again === 1, `loading: keyboard focus stays on the button and a second press does nothing (${again} call)`, { ld, again });
    check(+lk.opacity === 1 && lk.cursor === 'progress' && lk.text !== before.text, `loading: full strength, progress cursor (${lk.opacity}, ${lk.cursor})`, { before, lk });
    await page.evaluate(() => window.__btRelease && window.__btRelease(true)); await sleep(200);
    const rel = await page.evaluate(s => { const b = document.querySelector(s); return { text: b.textContent.replace(/\s+/g, ' ').trim(), busy: b.getAttribute('aria-busy'), card: document.querySelector('#btStage .engCard').dataset.opening || '' }; }, SB);
    check(/^(Fix|View) in Engraving →$/.test(rel.text) && rel.busy === null && rel.card === '', 'loading ends: the button has its words and its state back', rel);
    // a thumbnail press shows the spinner on the thumbnail and on the button
    await mk({ key: made.both.key, pending: true });
    await page.click(ST); await sleep(200);
    const tl = await page.evaluate(s => { const t = document.querySelector(s); return { busy: t.getAttribute('aria-busy'), over: !!t.querySelector('.engOpening[role=status]'), label: (t.querySelector('.engOpening') || {}).title, btn: document.querySelector('#btStage .egOpen').textContent.replace(/\s+/g, ' ').trim() }; }, ST);
    check(tl.busy === 'true' && tl.over && tl.label === 'Opening Engraving…' && tl.btn === 'Opening Engraving…', 'a thumbnail press: the spinner on the picture and on the button, both say "Opening Engraving…"', tl);
    await page.evaluate(() => window.__btRelease && window.__btRelease(true)); await sleep(150);
    await page.evaluate(() => document.querySelectorAll('#btStage').forEach(n => n.remove()));

    // the loading state in the REAL order window (a slow EngraveLink): the labelled spinner is on the real button while the window is still there
    await R.openOrderWindow(w, 'back'); await page.evaluate(() => { window.__links.length = 0; window.__go = false; window.__hold = 700; });
    await page.click(cardSel('back') + ' .egOpen'); await sleep(250);
    const real = await page.evaluate(s => { const b = document.querySelector(s); return { text: b.textContent.replace(/\s+/g, ' ').trim(), busy: b.getAttribute('aria-busy') }; }, cardSel('back') + ' .egOpen');
    await R.shot(w, 'BUTTON-loading-real', { clip: await clip() });
    check(real.busy === 'true' && real.text === 'Opening Engraving…', 'in the real order window the button says "Opening Engraving…" with its spinner while it opens', real);
    await sleep(700); await page.evaluate(() => { window.__hold = 0; });
    check(await page.evaluate(s => { const b = document.querySelector(s); return !!b && b.getAttribute('aria-busy') === null && /Engraving →$/.test(b.textContent.trim()); }, cardSel('back') + ' .egOpen'), 'and gives the button back when the opening ends (here the stub opens nothing)');

    /* ── 4 · the keyboard ── */
    console.log('\n  4 · the keyboard');
    await R.openOrderWindow(w, 'both'); await page.evaluate(() => { window.__go = false; window.__hold = 0; window.__links.length = 0; });
    await page.focus(cardSel('both') + ' .egApproveButton');
    const order = [];
    for (let i = 0; i < 3; i++) { await page.keyboard.press('Tab'); order.push(await page.evaluate(() => { const a = document.activeElement; return a ? (a.className.split(' ').filter(c => /^(engThumb|egOpen|egApproveButton)$/.test(c))[0] || a.tagName) + (a.closest && a.closest('.engFace') ? ':' + a.closest('.engFace').dataset.face : '') : ''; })); }
    check(order.join() === 'engThumb:front,engThumb:back,egOpen', `Tab goes Approve, Front picture, Back picture, the Engraving button (${order.join(' > ')})`);
    await page.keyboard.press('Enter'); await sleep(150);
    check(await page.evaluate(() => window.__links.length === 1 && window.__links[0].face === 'front' && /~front$/.test(window.__links[0].piece)), 'Enter on the Engraving button opens the piece (its front, the first face to settle)', await page.evaluate(() => window.__links));
    await page.evaluate(() => { window.__links.length = 0; });
    await page.focus(cardSel('both') + ' .engFace[data-face=back] .engThumb'); await page.keyboard.press('Space'); await sleep(150);
    check(await page.evaluate(() => window.__links.length === 1 && window.__links[0].face === 'back'), 'Space on the Back picture opens the back', await page.evaluate(() => window.__links));
    await page.evaluate(() => { window.__links.length = 0; });
    await page.focus(cardSel('both') + ' .engFace[data-face=front] .engThumb'); await page.keyboard.press('Enter'); await sleep(150);
    check(await page.evaluate(() => window.__links.length === 1 && window.__links[0].face === 'front'), 'Enter on the Front picture opens the front');

    /* ── 5 · the same button in the older places ── */
    console.log('\n  5 · the same button everywhere');
    await R.openOrderWindow(w, 'both');
    const same = await page.evaluate(async key => {
      const S = CNEngravingSeals, row = B.orders.byKey.get(key), job = Engrave.jobsOf(row)[0];
      const box = document.createElement('div'); box.id = 'btOld'; box.style.cssText = 'position:fixed;left:700px;top:330px;width:300px;z-index:2147483000;background:#fffefb'; box.className = 'owPanel';
      box.innerHTML = S.panel({ kind: 'approve', job, text: job.text, face: 'back', back: null });
      document.getElementById('orderWin').append(box);   // (inside the modal order window: everything outside it is inert)
      const old = box.querySelector('.egOpen'), now = document.querySelector('#owEng > .engCard .egOpen');
      const pick = b => { const cs = getComputedStyle(b); return { cls: [...b.classList].sort().join(' '), text: b.textContent.replace(/\s+/g, ' ').trim(), de: b.dataset.e, tag: b.tagName, type: b.type, font: cs.fontFamily + '|' + cs.fontWeight + '|' + cs.fontSize, color: cs.color, bg: cs.backgroundColor, border: cs.borderTopWidth + ' ' + cs.borderTopStyle + ' ' + cs.borderTopColor, radius: cs.borderTopLeftRadius, h: b.getBoundingClientRect().height, padY: cs.paddingTop }; };
      return { old: old && pick(old), now: now && pick(now) };
    }, made.both.key);
    const o = same.old, nw = same.now;
    check(!!o && !!nw, 'the older panel (Sheet view, sheet window, Review: CNEngravingSeals.panel) draws its button');
    if (o && nw) {
      check(o.cls === nw.cls && o.text === nw.text && o.de === nw.de && o.tag === nw.tag && o.type === nw.type, `same classes, same words, same marker ("${o.text}" / "${nw.text}": ${nw.cls})`, same);
      check(o.font === nw.font && o.color === nw.color && o.bg === nw.bg && o.border === nw.border && o.radius === nw.radius, 'same computed look: font, colour, fill, edge, corner', same);
      console.log(`  info the card's button is ${Math.round(nw.h)}px tall (padding ${nw.padY}), the older panel's ${Math.round(o.h)}px (padding ${o.padY}): the one difference, the card is compact like its Approve button`);
    }
    await page.hover('#btOld .egOpen'); await sleep(300); const oh = await look(page, '#btOld .egOpen'); await page.mouse.move(5, 5);
    await page.hover(cardSel('both') + ' .egOpen'); await sleep(300); const nh = await look(page, cardSel('both') + ' .egOpen'); await away();
    check(oh.bg === nh.bg && oh.border === nh.border && oh.arrow === nh.arrow, `same hover in both places (fill ${oh.bg}, edge ${oh.border}, arrow ${oh.arrow})`, { oh, nh });
    await page.mouse.move(5, 5); await page.hover('#btOld .egOpen'); await page.mouse.down(); await sleep(250); const oa = await look(page, '#btOld .egOpen'); await page.mouse.move(5, 5); await page.mouse.up();
    await page.hover(cardSel('both') + ' .egOpen'); await page.mouse.down(); await sleep(250); const na = await look(page, cardSel('both') + ' .egOpen'); await page.mouse.move(5, 5); await page.mouse.up(); await away();
    check(oa.bg === na.bg && oa.transform === na.transform, `same pressed look in both places (fill ${oa.bg}, ${oa.transform})`, { oa, na });
    await R.shot(w, 'BUTTON-same-everywhere', { clip: await page.evaluate(() => { const a = document.querySelector('#owEng').getBoundingClientRect(), b = document.getElementById('btOld').getBoundingClientRect(); const x = Math.max(0, Math.min(a.left, b.left) - 10), y = Math.max(0, Math.min(a.top, b.top) - 30); return { x, y, width: Math.min(1280 - x, Math.max(a.right, b.right) - x + 10), height: Math.max(a.bottom, b.bottom) - y + 10 }; }) });
    await page.evaluate(() => document.getElementById('btOld') && document.getElementById('btOld').remove());

    check(!w.errors.length, 'no script error on the page throughout', w.errors.slice(0, 3));
  } finally { await w.close(); }
  return finish();
}
function finish() { console.log(failed ? `\nengrave-open-button: ${failed} FAILED, ${passed} passed` : `\nengrave-open-button: ${passed} checks passed`); return failed ? 1 : 0; }
main().then(c => process.exit(c)).catch(e => { console.error(e); process.exit(1); });
