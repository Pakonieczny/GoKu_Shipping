// ENGNOTE (engrave-ui-1012 round 2, point 6) on ENGLIVE's REAL-DRAWING world (_engraveRealWorld.cjs): real charm drawings (COMET_9427, SCHRODINGER CAT 2), real intake / pool / classifier / fit, a real installed font.
// The customer's words box ("What the customer wrote") is in the Engrave tab detail beside the real work area: it holds the real order's personalisation and engraving options, the buyer's message and the staff note
// when there are some; it sits ABOVE the card and the panel (never over them), the work area still draws the real charm, and a decided row's detail shows it too. One world per file (engrave-customer-note.cjs has the
// per-state and the empty-order checks on the lighter fake site).
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/engrave-customer-note-real.cjs [shotsDir]
const R = require('./_engraveRealWorld.cjs');
(async () => {
  const w = await R.world({ viewport: { width: 1280, height: 720 }, shots: process.argv[2] || process.env.SHOTS || '/tmp/engnote-real-shots' });
  if (!w) { console.log('engrave-customer-note-real: playwright-core not found, skipped'); return; }
  const fails = [];
  const check = (ok, msg) => { if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
  try {
    await R.build(w, ['both', 'back']);
    const read = () => w.page.evaluate(() => {
      const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'), b = c && c.querySelector('[data-said]'), r = e => e && e.getBoundingClientRect();
      const card = c && c.querySelector('.engCard'), panel = c && c.querySelector('.engp'), side = c && c.querySelector('.pvSide'), cv = c && c.querySelector('.backHost canvas');
      return { has: !!c, said: b ? b.innerText.replace(/\s+/g, ' ').trim() : null, first: !!(side && side.firstElementChild === b), labels: b ? [...b.querySelectorAll('.lbl')].map(l => l.textContent) : [],
        saidBottom: b ? Math.round(r(b).bottom) : null, cardTop: card ? Math.round(r(card).top) : null, panelTop: panel ? Math.round(r(panel).top) : null, saidW: b ? Math.round(r(b).width) : 0, sideW: side ? Math.round(r(side).width) : 0,
        canvas: cv ? { w: Math.round(r(cv).width), h: Math.round(r(cv).height) } : null, titles: c ? [...c.querySelectorAll('h4.pvH')].map(h => h.textContent.trim()) : [], order: c ? c.querySelectorAll('.pvOrder').length : -1, text: c ? c.innerText.replace(/\s+/g, ' ') : '' };
    });

    // the necklace engraved front + back (SCHRODINGER CAT 2): front "W", back "Name One", Engraving Side: Both, font Ace
    await R.openEngraveDetail(w, 'both', { face: 'back' });
    let d = await read();
    check(d.has && d.said && /^what the customer wrote/i.test(d.said), 'the front + back necklace: the box is in the Engrave detail: "' + d.said + '"');
    check(/front - W/.test(d.said) && /back - Name One/.test(d.said), 'it holds the personalisation exactly as the customer wrote it (which words for which face)');
    check(/Engraving Side: Both/.test(d.said) && /Font: Ace/.test(d.said) && !/Metal|Gold|Silver/i.test(d.said), 'and the engraving options the customer chose (both sides, the font), not the metal');
    check(d.first && d.cardTop != null && d.saidBottom <= d.cardTop && d.saidBottom <= d.panelTop, 'it is the first thing of the panel\'s column, above the card and the panel, overlapping nothing: ' + JSON.stringify({ bottom: d.saidBottom, card: d.cardTop, panel: d.panelTop }));
    check(d.saidW > 0 && d.saidW <= d.sideW, 'it fits the column (' + d.saidW + ' of ' + d.sideW + ' px)');
    check(d.canvas && d.canvas.w > 150 && d.canvas.h > 150, 'the work area still draws the real charm: ' + JSON.stringify(d.canvas));
    check(d.titles.join('|') === 'What the customer wrote' && d.order === 0, 'its title is the only block title; no "From the order" list');
    await R.shot(w, 'NOTE-detail-both');
    // the same order seen on the front's face job: the customer's words are the order's, the same box
    await R.openEngraveDetail(w, 'both', { face: 'front' });
    d = await read();
    check(d.said && /front - W/.test(d.said) && /Engraving Side: Both/.test(d.said), 'on the front\'s job the same box is there');

    // the necklace engraved on the back only (COMET_9427): personalisation "Name One", font Stylish; a buyer message and a staff note join the box
    await w.page.evaluate(key => { const row = B.orders.byKey.get(key); row.spec.buyerMessage = 'Please keep it simple, thank you'; row.spec.staffNote = 'Name Two checked the spelling'; }, w.made.back.key);
    await R.openEngraveDetail(w, 'back', { face: 'back' });
    d = await read();
    check(d.said && /Name One/.test(d.said) && /Font: Stylish/.test(d.said) && /Please keep it simple, thank you/.test(d.said) && /Name Two checked the spelling/.test(d.said), 'the back-only necklace: personalisation, font, buyer message and staff note: "' + d.said + '"');
    check(d.labels.join('|') === 'Personalisation|Buyer message|Staff note|Options', 'each under its own small label: ' + d.labels.join('|'));
    check(d.canvas && d.canvas.w > 150, 'the real charm is drawn in the work area');
    await R.shot(w, 'NOTE-detail-back');

    // a decided row's opened detail
    const dd = await w.page.evaluate(async key => {
      const row = B.orders.byKey.get(key), j = Engrave.jobOf(row, null, 'back'), was = { state: j.state, by: j.approvedBy, at: j.approvedAt, decision: j.decision };
      Object.assign(j, { state: 'approved', approvedBy: 'Name One', approvedAt: Date.now() - 60e3, decision: { by: 'Name One', at: Date.now() - 60e3, text: j.text } });
      setMode('engrave'); Engrave.restoreView({ tab: 'done', list: false, chosen: true, openDone: j.key }); Engrave.render();
      const r = document.querySelector('#egDone .doneRow.open'), det = r && r.querySelector('.doneDetail'), b = det && det.querySelector('[data-said]');
      const out = { open: !!det, said: det ? det.querySelectorAll('[data-said]').length : -1, text: b ? b.innerText.replace(/\s+/g, ' ') : '' };
      Object.assign(j, was); Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: j.key }); Engrave.render();
      return out;
    }, w.made.back.key);
    check(dd.open && dd.said === 1 && /Name Two checked the spelling/.test(dd.text), 'a decided row\'s opened detail shows the box too');

    check(w.errors.length === 0, 'no page errors: ' + w.errors.join(' | ').slice(0, 300));
  } finally { await w.close(); }
  if (fails.length) { console.log('engrave-customer-note-real: ' + fails.length + ' failed'); process.exit(1); }
  console.log('engrave-customer-note-real: ok');
})().catch(e => { console.error(e); process.exit(1); });
