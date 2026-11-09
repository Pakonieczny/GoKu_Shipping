// CES1 (Paul, 9 Nov 2026): "When I open the Charm Editor and make changes ... and then press blue 'Save' button the system does not update the actual order info and thumbnails throughout the
// application ... Also there should be a popup modal with specific options presented to the user that are relevant to that particular piece/order. This modal already exists but does not always
// show up. Also ... when I click the 'Edit Design' button to open the Charm Editor, it opens somewhere in the background ... anywhere the charm editors opened from must open immediately, and if
// it is already [open] and not given order or piece, then that particular piece or order must open in the charm editor."
// On the real sorter page over the repo's fake site (_engraveRealWorld.cjs: the real intake, pool, fit; offline, made-up names, nothing real written), two engraved orders A and B:
//   C1  the order window open and another modal window shown over it (one earlier in the page, as an engraving or sheet window can be): Edit Design opens the editor IN FRONT of both, with the keys
//   C2  a window shown over the open editor: Edit Design of the same piece brings the editor in front again (nothing else opens)
//   C3  the editor on A with a change, B asked for: "Save and switch / Discard and switch / Cancel"; Cancel keeps A; Save and switch saves A and opens B, no card over B
//   C4  a save from the order window's Edit Design: the card after Save is in front (over the order window), with this piece's choices (Done, Keep editing, Replace on the order…); Keep
//       editing opens the same piece again; every view is told at once: the order window painted again, the "cn-charm-saved" event for the piece
//   C5  a save from the engraving card's "Open in Charm Editor": the card is in front too
//   C6  the Design & Engrave "Charm Editor" button with the order window open: the "Design a charm" picker and the editor are in front; Save → the card "Saved · version 1" in front
//   CES2 (Paul, 9 Nov 2026: "I want the pop-up to open on every Save"): after each Save above (C4, C5, C6) the Send to sheet pop-up opens by itself, in front, over the card; × closes it
//       and the card is still there (C4p: for a piece it is set as a fix of that piece)
//   C7  no page error
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/ce-save-everywhere.cjs      (SHOTS=<dir> for the screenshots)
'use strict';
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : evidence || ''}`.trimEnd()); };
const R = require('./_engraveRealWorld.cjs');

(async () => {
  const shots = process.env.SHOTS || '/tmp/ce-save-everywhere-shots';
  const w = await R.world({ viewport: { width: 1440, height: 900 }, shots, keepCards: true });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  const page = w.page, st = w.srv.st;
  await page.waitForFunction(() => !window.CNRole || ['ask', 'role', 'admin'].includes(CNRole.state()), null, { timeout: 20000 }).catch(() => {});
  await page.evaluate(() => { try { if (window.CNRole && CNRole.state() === 'ask') CNRole.choose('design'); } catch (_) { /* no role step */ } });
  const settle = ms => R.settle(page, ms);
  const until = async (fn, ms) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) return null; await new Promise(r => setTimeout(r, 150)); } };
  const editor = () => page.evaluate(() => { const r = document.querySelector('.ce-root'); if (!r || !window.CharmEditor || !CharmEditor.current) return null; return { title: (r.querySelector('.ce-id h2') || {}).textContent || '', sub: (r.querySelector('.ce-eyebrow') || {}).textContent || '', n: CharmEditor.current.items().length }; });
  const editorUp = ms => until(async () => { const e = await editor(); return e && e.n ? e : null; }, ms || 60000);
  /** What the person sees at the middle of `sel` (the topmost thing there) is inside `sel`'s own box, and where the keys go. */
  const inFront = sel => page.evaluate(sel => {
    const el = document.querySelector(sel); if (!el) return { ok: false, why: 'none' };
    const b = el.getBoundingClientRect(), hit = document.elementFromPoint(b.left + b.width / 2, b.top + Math.min(b.height / 2, 60));
    return { ok: !!hit && el.contains(hit), hit: hit ? (hit.id || hit.className || hit.tagName) + '' : null, keys: !!document.activeElement && el.contains(document.activeElement) };
  }, sel);
  const closeEditor = async () => { if (!(await editor())) return; await page.evaluate(() => document.querySelector('.ce-root [data-act="cancel"]').click()); await settle(200); await page.evaluate(() => { const y = document.querySelector('.ce-ask [data-ask="yes"]'); if (y) y.click(); }); await until(() => page.evaluate(() => !document.querySelector('.ce-root')), 15000); await settle(200); };
  /** CES2: the Send to sheet pop-up opened by itself after Save: in front, its design drawn; then closed with × (nothing sent), the card left under it. */
  const popup = async () => {
    const up = await until(() => page.evaluate(() => { const d = document.querySelector('dialog.cpDlg[data-cp-after][open]'); return d && d.querySelector('[data-cp-plate] svg') ? { fix: !!d.querySelector('[data-cp-reason="fix"].on'), sendBtn: (d.querySelector('[data-cp-send]') || {}).textContent || '' } : null; }), 60000);
    const f = up ? await inFront('dialog.cpDlg[data-cp-after] .dlg') : { ok: false };
    await page.evaluate(() => { const b = document.querySelector('dialog.cpDlg[data-cp-after] [data-cp-close]'); if (b) b.click(); }); await settle(300);
    const gone = await page.evaluate(() => !document.querySelector('dialog.cpDlg[open]'));
    return { up: !!up, fix: !!(up && up.fix), front: f.ok, gone };
  };
  const cardDone = async () => { await page.evaluate(() => { const b = document.querySelector('.cem-card [data-c="done"]'); if (b) b.click(); }); await settle(200); };
  const openWin = async key => {
    await page.evaluate(() => { CN.setMode('orders'); if (OrderWin.isOpen()) OrderWin.close(); });
    await until(() => page.evaluate(() => !document.querySelector('#orderWin[open]')), 5000);
    await page.evaluate(async key => { await OrderWin.open(key); }, key);
    await until(() => page.evaluate(() => { const b = document.getElementById('owCpDesign'); return !!b && !b.hidden && !!b.offsetParent; }), 8000); await settle(500);
  };
  const dot = () => page.evaluate(() => { const E = CharmEditor.current, f = E.frame(), x = f.x0 + f.wPt * (0.47 + Math.random() * 0.06), y = f.y0 + f.hPt * (0.53 + Math.random() * 0.04); E.ctx.addItems([{ kind: 'path', role: 'hatch', name: 'Dot', fill: true, stroke: false, subpaths: [[['m', [x, y]], ['l', [x + 0.6, y]], ['l', [x + 0.3, y + 0.6]], ['h']]], tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 }], { select: false }); E.ctx.commit('dot'); });
  const artOf = pid => st.calls.filter(c => c.name === 'charmNestLibrary' && c.op === 'artPut' && c.body && c.body.poolId === pid).length;
  /** A modal window shown over everything, put FIRST in the page (as a window made earlier is): the last open dialog in the page's order is then not the one on top. */
  const overlay = () => page.evaluate(() => { let d = document.getElementById('cesOver'); if (!d) { d = document.createElement('dialog'); d.id = 'cesOver'; d.innerHTML = '<p style="width:70vw;height:70vh;margin:0;background:#fffbe6">Another window</p>'; document.body.prepend(d); } if (!d.open) d.showModal(); });
  const overlayOff = () => page.evaluate(() => { const d = document.getElementById('cesOver'); if (d && d.open) d.close(); });
  try {
    const made = await R.build(w, ['back', 'both']);
    const A = made.back, B = made.both;

    /* ── C1 · the editor in front of the order window AND another window over it ── */
    await openWin(A.key);
    await overlay();
    await page.evaluate(() => document.getElementById('owCpDesign').click());   // (as the window's own button presses it)
    const c1e = await editorUp(); await settle(400);
    const c1 = await inFront('.ce-root .ce-win'), c1l = await page.evaluate(() => !!document.querySelector('dialog.ce-layer[open] > .ce-root'));
    await R.shot(w, 'CES-C1-front');
    check(`C1 the order window open and another window over it: Edit Design opens "${c1e && c1e.sub}" in front of both (what is seen at its middle: ${c1.hit}; its own top-layer dialog ${c1l}), the keys are its own (${c1.keys})`,
      !!c1e && c1e.sub.includes(A.rid) && c1.ok && c1.keys && c1l, JSON.stringify({ c1e, c1, c1l }));

    /* ── C2 · a window over the open editor; Edit Design of the same piece brings it in front ── */
    await overlayOff(); await overlay(); await settle(200);
    const hidden = await inFront('.ce-root .ce-win');
    await page.evaluate(() => document.getElementById('owCpDesign').click()); await settle(500);
    const c2 = await inFront('.ce-root .ce-win'), n2 = await page.evaluate(() => document.querySelectorAll('.ce-root').length), c2e = await editor();
    check(`C2 a window shown over the open editor hides it (${!hidden.ok}); Edit Design of the same piece brings it in front again (${c2.ok}, keys ${c2.keys}), ${n2} editor, still "${c2e && c2e.sub}"`,
      !hidden.ok && c2.ok && c2.keys && n2 === 1 && !!c2e && c2e.sub.includes(A.rid), JSON.stringify({ hidden, c2, n2, c2e }));
    await overlayOff();

    /* ── C3 · another order asked for with changes in the editor ── */
    await dot();
    const askB = () => page.evaluate(key => { window.__pB = CharmNestCustomPiece.openDesigner({ rows: [B.orders.byKey.get(key)], row: B.orders.byKey.get(key) }, { piece: true }); }, B.key);
    await askB();
    const asked = await until(() => page.evaluate(() => { const a = document.querySelector('.ce-ask'); return a ? { h: a.querySelector('h3').textContent, b: [...a.querySelectorAll('[data-ask]')].map(b => b.dataset.ask + ':' + b.textContent.trim()) } : null; }), 10000);
    await R.shot(w, 'CES-C3-asks');
    await page.click('.ce-ask [data-ask="no"]'); await settle(500);
    const kept = await editor(), gaveWay = await page.evaluate(() => Promise.race([window.__pB.then(v => v === null), new Promise(r => setTimeout(() => r('pending'), 3000))]));
    const art0 = artOf(A.poolIds[0]);
    await askB();
    await until(() => page.evaluate(() => !!document.querySelector('.ce-ask [data-ask="alt"]')), 10000);
    await page.click('.ce-ask [data-ask="alt"]');
    const c3b = await until(async () => { const e = await editor(); return e && e.n && e.sub.includes(B.rid) ? e : null; }, 60000);
    await until(() => artOf(A.poolIds[0]) > art0, 30000); await settle(1500);
    const c3card = await page.evaluate(() => document.querySelectorAll('.cem-card').length), c3f = await inFront('.ce-root .ce-win');
    check(`C3 the editor on A with a change, B asked for: "${asked && asked.h}" (${asked && asked.b.join(' / ')}); Cancel keeps A ("${kept && kept.sub}", B gave way: ${gaveWay}); Save and switch saved A (${artOf(A.poolIds[0]) - art0} artwork save) and opened B ("${c3b && c3b.sub}"), no card over it (${c3card}), in front (${c3f.ok})`,
      !!asked && asked.h === 'Switch to another charm?' && ['alt:Save and switch', 'yes:Discard and switch', 'no:Cancel'].every(x => asked.b.includes(x)) && !!kept && kept.sub.includes(A.rid) && gaveWay === true
        && artOf(A.poolIds[0]) > art0 && !!c3b && c3card === 0 && c3f.ok, JSON.stringify({ asked, kept, gaveWay, c3b, c3card, c3f }));

    /* ── C4 · the card after a save from the order window's Edit Design; every view told ── */
    await closeEditor();
    await openWin(B.key);
    await page.evaluate(() => { window.__paints = 0; const p = OrderWin.paint; OrderWin.paint = function () { window.__paints++; return p.apply(this, arguments); }; window.__saved = []; document.addEventListener('cn-charm-saved', e => window.__saved.push(e.detail)); });
    await page.click('#owCpDesign');
    await editorUp(); await dot();
    const art1 = artOf(B.poolIds[0]);
    await page.click('.ce-root [data-act="save"]');
    const card4 = await until(() => page.evaluate(() => { const c = document.querySelector('.cem-card'); return c && c.querySelector('[data-c]') ? { h: c.querySelector('h3').textContent, t: c.textContent, b: [...c.querySelectorAll('[data-c]')].map(b => b.dataset.c + ':' + b.textContent.trim()) } : null; }), 60000);
    const p4 = await popup();
    check(`C4p the Send to sheet pop-up opens by itself after Save (${p4.up}), in front (${p4.front}), set as a fix of this piece (${p4.fix}); × closes it, nothing sent, the card stays (${p4.gone})`, p4.up && p4.front && p4.fix && p4.gone, JSON.stringify(p4));
    await settle(400); await R.shot(w, 'CES-C4-card');
    const c4f = await inFront('.cem-card'), told = await page.evaluate(() => ({ paints: window.__paints, saved: window.__saved, win: OrderWin.isOpen() }));
    check(`C4 a save from the order window's Edit Design (${artOf(B.poolIds[0]) - art1} artwork save): the card "${card4 && card4.h}" is in front of the order window (${c4f.ok}; the window still open: ${told.win}) with this piece's choices (${card4 && card4.b.join(' / ')}); every view told at once (the order window painted ${told.paints}×, cn-charm-saved ${JSON.stringify(told.saved)})`,
      artOf(B.poolIds[0]) > art1 && !!card4 && /^Saved/.test(card4.h) && card4.t.includes(B.rid) && ['done:Done', 'again:Keep editing'].every(x => card4.b.includes(x)) && card4.b.some(x => /^replace:Replace on the order/.test(x))
        && c4f.ok && told.win && told.paints > 0 && told.saved.some(d => d.poolIds.includes(B.poolIds[0]) && d.orderId === B.rid), JSON.stringify({ card4, c4f, told }));
    await page.click('.cem-card [data-c="again"]');
    const c4k = await editorUp(); const c4kf = await inFront('.ce-root .ce-win');
    check(`C4b Keep editing opens the same piece again ("${c4k && c4k.sub}"), in front (${c4kf.ok})`, !!c4k && c4k.sub.includes(B.rid) && c4kf.ok, JSON.stringify({ c4k, c4kf }));
    await closeEditor();

    /* ── C5 · the card after a save from the engraving card's "Open in Charm Editor" ── */
    await R.openEngraveDetail(w, 'back'); await settle(600);
    await page.$eval('#egQueue .rvItem[data-kind=placement] .egCharmEdit', b => b.click());
    await editorUp(); await dot();
    await page.click('.ce-root [data-act="save"]');
    const card5 = await until(() => page.evaluate(() => { const c = document.querySelector('.cem-card'); return c && c.querySelector('[data-c]') ? { h: c.querySelector('h3').textContent, b: [...c.querySelectorAll('[data-c]')].map(b => b.dataset.c) } : null; }), 60000);
    const p5 = await popup();
    await settle(300); const c5f = await inFront('.cem-card');
    check(`C5 a save from the engraving card's Open in Charm Editor: the card "${card5 && card5.h}" is in front (${c5f.ok}) with ${card5 && card5.b.join(', ')}`, !!card5 && /^Saved/.test(card5.h) && c5f.ok && card5.b.includes('done') && p5.up && p5.front && p5.gone, JSON.stringify({ card5, c5f, p5 }));
    await cardDone();

    /* ── C6 · the Design & Engrave Charm Editor button's way (a new design) with the order window open ── */
    await openWin(A.key);
    await page.evaluate(() => { window.__nd = CharmNestCeMasters.newDesign({ from: 'charmEditorButton', back: { label: 'Back to Design & Engrave' } }); });
    await until(() => page.evaluate(() => !!document.querySelector('.cem-root [data-panel="new"]:not([hidden])')), 15000); await settle(300);
    const c6p = await inFront('.cem-win');
    await page.click('.cem-root [data-act="start"]');
    await editorUp(); await settle(300);
    const c6e = await inFront('.ce-root .ce-win');
    await dot();
    await page.click('.ce-root [data-act="save"]');
    const card6 = await until(() => page.evaluate(() => { const c = document.querySelector('.cem-card'); return c && /Saved · version/.test(c.querySelector('h3').textContent) ? c.querySelector('h3').textContent : null; }), 60000);
    const p6 = await popup();
    await settle(300); const c6c = await inFront('.cem-card');
    await R.shot(w, 'CES-C6-card');
    check(`C6 with the order window open, the Charm Editor button's way: the picker in front (${c6p.ok}), the editor in front (${c6e.ok}), Save → "${card6}" in front (${c6c.ok})`, c6p.ok && c6e.ok && !!card6 && c6c.ok && p6.up && p6.front && p6.gone, JSON.stringify({ c6p, c6e, card6, c6c, p6 }));
    await cardDone();

    check('C7 no page error', w.errors.length === 0, JSON.stringify(w.errors).slice(0, 400));
  } catch (e) { check('the run', false, String(e && e.stack || e).slice(0, 600)); }
  finally { await w.close(); }
  const bad = RESULTS.filter(r => !r.ok);
  console.log(`\n${RESULTS.length - bad.length}/${RESULTS.length} passed${bad.length ? ' · FAILED: ' + bad.map(r => r.id.split(' ')[0]).join(', ') : ''}`);
  console.log('screenshots in ' + shots);
  process.exit(bad.length ? 1 : 0);
})();
