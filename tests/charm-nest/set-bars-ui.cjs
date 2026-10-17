// SETBAR-B (setbarB-1013): the UI of a SET OF TWO BARS (Paul, 13 Oct 2026, order 4202291826, Etsy listing 532401548 "Bar Necklace SET of 2": the large bar BAR_1548 and the small bar SKINNY_0191, one of each for every set):
//   "adapt the UI in a beautiful and thoughtful way, so that the user in this case can see both bar charms side-by-side, and then be able to manipulate each one for the back and front engraving, if chosen by the
//    buyer, as part of the Engraving detail modal and the Order details modal"
//   "The thinner bar always sits on the shorter chain unless the user special requests a different layout": show each bar with the chain length it sits on, the buyer's wording that belongs to each in the order
//    window, and, when the line is flagged 'layout requested', a calm one-line note that it waits for a person.
//   (NOSAID, Paul 17 Oct 2026: "Remove the personalization info from beneath the Engraving card inside the Detailed Order modal": the per-bar wording block that stood under the cards, "Large bar · 18″ Ginger / Small bar · 16″ Catalina",
//    is gone from the order window. Section E below now holds that nothing is drawn under the cards, and that each bar's own words are on its own card; the reader, OrderEngraving.said.of, still returns the per-bar wording as data.)
//
// What the data model gives (it exists since SIBLINGS / SCHROLINK; SETBAR-A adds the rule row for this listing): one order LINE with two pieces, slots D1 and D2, each its own design, job, words, Approve and seal.
// This test builds that line the way the app does for a person's saved pair (aliasPut, the real call over the fake site), with two synthetic bar drawings of the sizes SETBAR-A reports (25.9 x 4.9 mm and
// 25.9 x 3.9 mm) and one 50 x 7.2 mm bar, each with a Front and a Back job. SETBAR-A's CharmNestOrders.setPiecesOf (the chain each bar hangs on, its role and label, the layout flag) is a STUB of the same shape
// here: the UI consumes that function and never works the chain out itself.
//
// Offline only: the REAL page over the repo's fake site (_engraveRealWorld.cjs). Made-up buyer names. No Etsy, no paid call, no live write (the one approval below is made on the fake site's in-memory Firestore).
//   PW_DIR=/opt/node-tools/node_modules node tests/charm-nest/set-bars-ui.cjs      (CHROMIUM=<chrome>  SHOTS=<dir>)
// What a pass does NOT prove: a live order on the live page; the live BAR_1548 / SKINNY_0191 drawings (not in the repo); SETBAR-A's real mapping.
'use strict';
const R = require('./_engraveRealWorld.cjs'), Base = require('./_orderFactsWorld.cjs');

const MMPT = 72 / 25.4;
/** A PDF-compatible .ai of a rounded bar with a hole at each end on the CUT layer (as discAi() makes a disc): wMm x hMm. Nothing in it is a customer's. */
function barAi(wMm, hMm, title) {
  const f = n => (Math.round(n * 1000) / 1000).toString();
  const W = wMm * MMPT, H = hMm * MMPT, X = 10, Y = 10, x0 = X, x1 = X + W, y0 = Y, y1 = Y + H, cy = Y + H / 2, rr = H * 0.32, c = 0.5522847498 * rr;
  const circle = (cx, cy2, r) => { const k = 0.5522847498 * r; return `${f(cx + r)} ${f(cy2)} m ${f(cx + r)} ${f(cy2 + k)} ${f(cx + k)} ${f(cy2 + r)} ${f(cx)} ${f(cy2 + r)} c ${f(cx - k)} ${f(cy2 + r)} ${f(cx - r)} ${f(cy2 + k)} ${f(cx - r)} ${f(cy2)} c ${f(cx - r)} ${f(cy2 - k)} ${f(cx - k)} ${f(cy2 - r)} ${f(cx)} ${f(cy2 - r)} c ${f(cx + k)} ${f(cy2 - r)} ${f(cx + r)} ${f(cy2 - k)} ${f(cx + r)} ${f(cy2)} c h`; };
  const bar = `${f(x0 + rr)} ${f(y0)} m ${f(x1 - rr)} ${f(y0)} l ${f(x1 - rr + c)} ${f(y0)} ${f(x1)} ${f(y0 + rr - c)} ${f(x1)} ${f(y0 + rr)} c ${f(x1)} ${f(y1 - rr)} l ${f(x1)} ${f(y1 - rr + c)} ${f(x1 - rr + c)} ${f(y1)} ${f(x1 - rr)} ${f(y1)} c ${f(x0 + rr)} ${f(y1)} l ${f(x0 + rr - c)} ${f(y1)} ${f(x0)} ${f(y1 - rr + c)} ${f(x0)} ${f(y1 - rr)} c ${f(x0)} ${f(y0 + rr)} l ${f(x0)} ${f(y0 + rr - c)} ${f(x0 + rr - c)} ${f(y0)} ${f(x0 + rr)} ${f(y0)} c h`;
  const hole = H * 0.13, hx = H * 0.42;
  const content = ['/OC /ocCut BDC', `q 0 0 0 RG 0.283 w ${bar} S Q`, `q 0 0 0 RG 0.283 w ${circle(x0 + hx, cy, hole)} S Q`, `q 0 0 0 RG 0.283 w ${circle(x1 - hx, cy, hole)} S Q`, 'EMC'].join('\n') + '\n';
  const objs = ['<< /Type /Pages /Kids [ 4 0 R ] /Count 1 >>',
    '<< /Type /Catalog /Pages 1 0 R /OCProperties << /OCGs [ 5 0 R 6 0 R 7 0 R ] /D << /Order [ 5 0 R 6 0 R 7 0 R ] /ON [ 5 0 R 6 0 R 7 0 R ] /BaseState /ON >> >> >>',
    `<< /Producer (SETBAR-B test drawing) /Title (${title}) >>`,
    `<< /Type /Page /Parent 1 0 R /Resources << /Properties << /ocEng 5 0 R /ocHatch 6 0 R /ocCut 7 0 R >> >> /MediaBox [ 0 0 ${Math.ceil(W + 20)} ${Math.ceil(H + 20)} ] /Contents 8 0 R >>`,
    '<< /Type /OCG /Name (ENGRAVE) >>', '<< /Type /OCG /Name (HATCH) >>', '<< /Type /OCG /Name (CUT) >>', `<< /Length ${Buffer.byteLength(content)} >>\nstream\n${content}endstream`];
  let out = '%PDF-1.7\n'; const at = [];
  objs.forEach((o, i) => { at.push(Buffer.byteLength(out)); out += `${i + 1} 0 obj\n${o}\nendobj\n`; });
  const xs = Buffer.byteLength(out);
  out += `xref\n0 ${objs.length + 1}\n0000000000 65535 f \n` + at.map(n => String(n).padStart(10, '0') + ' 00000 n \n').join('') + `trailer\n<< /Size ${objs.length + 1} /Root 2 0 R /Info 3 0 R >>\nstartxref\n${xs}\n%%EOF\n`;
  return Buffer.from(out, 'latin1');
}
const BARS = { BAR_1548: { w: 25.9, h: 4.9 }, SKINNY_0191: { w: 25.9, h: 3.9 }, BAR_WIDE_9999: { w: 50, h: 7.2 } };
function seedBars(st) {
  for (const [sku, d] of Object.entries(BARS)) {
    const wPt = d.w * MMPT, hPt = d.h * MMPT, aiPath = 'charmnest/master/' + sku + '.ai';
    st.put('Charm_Master_Index', sku, { sku, widthPt: wPt, heightPt: hPt, areaPt2: wPt * hPt * 0.95, members: 3, holes: 2, engravable: true, engravableBy: 'index', upAngle: 90, upSource: 'drawn', charmHash: sku.slice(0, 4).toLowerCase() + '1234', backKeepOut: [], aiPath, thumbPath: null, blocked: null, sizes: null, conflict: null });
    st.blobs.set(aiPath, { buf: barAi(d.w, d.h, sku), generation: 1, meta: { contentType: 'application/illustrator', metadata: { firebaseStorageDownloadTokens: 't' } } });
  }
}
function orderOf(fx, now) {
  const at = (d, h, m) => { const t = new Date(now); t.setDate(t.getDate() + d); t.setHours(h, m, 0, 0); return Math.floor(t.getTime() / 1000); };
  const shipBy = at(fx.ship, 17, 0), line = (x, i) => ({ transactionId: fx.rid + (i + 1), listingId: x.listing, sku: x.sku, title: x.title, quantity: x.qty || 1, expectedShipDate: shipBy, variations: x.variations || [], metalKey: x.metalKey || 'gold', metalLabel: x.metalLabel || 'GF 14/20', personalization: x.personalization || [], buyerMessage: '' });
  return { receiptId: fx.rid, orderNumber: fx.rid, createTs: at(fx.placed.day, fx.placed.h, fx.placed.m), updateTs: at(0, 12, 0), shipBy, buyer: { name: fx.buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: (fx.lines || [fx]).map(line) };
}
const V = (name, value) => ({ name, value });
Base.orderOf = orderOf;
const SET_TITLE = 'Bar Necklace SET of 2, Custom Gold Bar Set, Engraved Necklace';
// the set, front + back (the listing's option map says "Engraving Side: Both" is no shape), and the same set of a longer large bar, back only; and, to prove nothing else changed, three plain discs of one design
R.ORDERS.bars = { rid: '4290000151', buyer: 'Name Seventeen', placed: { day: 0, h: 12, m: 20 }, ship: 5, lines: [{ listing: '1890000151', sku: 'BAR_1548', title: SET_TITLE, variations: [V('Metal choice', 'Gold'), V('Engraving Side', 'Both'), V('Font Choice', 'Ace')], personalization: ['front - W\nback - Name One'] }] }; R.EXPECT_JOBS.bars = 4;
R.ORDERS.barsWide = { rid: '4290000152', buyer: 'Name Eighteen', placed: { day: 0, h: 12, m: 30 }, ship: 5, lines: [{ listing: '1890000152', sku: 'BAR_WIDE_9999', title: SET_TITLE, variations: [V('Metal choice', 'Gold'), V('Font Choice', 'Ace')], personalization: ['Name Two'] }] }; R.EXPECT_JOBS.barsWide = 2;
R.ORDERS.discsCtl = { rid: '4290000153', buyer: 'Name Nineteen', placed: { day: 0, h: 12, m: 40 }, ship: 5, lines: [{ listing: '1890000105', sku: 'COMET_9427', title: 'Initial Disc Necklace, Personalized Letter Discs', variations: [V('Metal choice', 'Gold'), V('Number of Discs', '3 Discs')], personalization: ['Tag 1: A, Tag 2: B, Tag 3: C'] }] }; R.EXPECT_JOBS.discsCtl = 3;

const fails = [], note = (ok, what, more) => { console.log((ok ? '  PASS ' : '  FAIL ') + what + (more ? '  [' + more + ']' : '')); if (!ok) fails.push(what); };
const near = (a, b, tol) => Math.abs(a - b) <= tol * Math.max(Math.abs(b), 1e-9);

/** The Engrave detail as the person sees it. */
const DETAIL = () => {
  const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); if (!c) return null;
  const txt = n => (n ? n.textContent.replace(/\s+/g, ' ').trim() : '');
  const tabs = [...c.querySelectorAll('.reviewIdentity .egEarSwitch:not(.egFaces) .egEarTab')].map(b => ({ key: b.dataset.ear, chip: txt(b.querySelector('.egPiece')), pressed: b.getAttribute('aria-pressed') === 'true' }));
  const frame = el => { const f = el && el.querySelector('.engpvFrame'); if (!f) return null; const r = f.getBoundingClientRect(); return { w: r.width, h: r.height, rel: f.style.getPropertyValue('--rel') || '' }; };
  const strip = [...c.querySelectorAll('.pvMain .engSet .engSetPc')].map(b => ({ piece: b.dataset.piece, ear: b.dataset.ear || '', label: txt(b.querySelector('.engSetLbl')), len: txt(b.querySelector('.engSetLen')), pressed: b.getAttribute('aria-pressed') === 'true', canvas: !!b.querySelector('.engSetPic canvas'), frame: frame(b.querySelector('.engSetPic')), box: (() => { const r = b.getBoundingClientRect(); return { l: r.left, r: r.right }; })() }));
  const card = c.querySelector('.pvSetup .engCard'), approve = c.querySelector('.pvApproval [data-a=approve]'), pa = c.querySelector('.pvApproval').getBoundingClientRect(), st = c.querySelector('.pvMain .engSet');
  return {
    key: c.dataset.key, sub: txt(c.querySelector('.reviewIdentity .sub')), tabs, strip, note: txt(c.querySelector('.pvMain .engSetNote')), stripBox: st ? (() => { const r = st.getBoundingClientRect(); return { l: r.left, r: r.right }; })() : null, approveBox: { l: pa.left, r: pa.right },
    cardTitle: card ? txt(card.querySelector('.engName')) : '', cardThumbs: card ? [...card.querySelectorAll('.engFace')].map(f => ({ face: f.dataset.face, frame: frame(f.querySelector('.engThumb')) })) : [],
    approve: !!approve && !approve.disabled, texts: txt(c).length, turn: (c.querySelector('input[data-a=turn]') || {}).value, bothBars: [...c.querySelectorAll('button')].map(b => txt(b)).filter(t => /both bars|all bars|approve all|both pieces/i.test(t)),
    counter: txt(c.querySelector('.reviewProgress .kind')), canvasW: (c.querySelector('.backHost canvas') || {}).width || 0
  };
};
const settle = (page, ms = 700) => R.settle(page, ms);

(async () => {
  const w = await R.world({ viewport: { width: 1440, height: 1000 } });
  if (!w) { console.log('no playwright-core: nothing run'); return; }
  const page = w.page, shots = process.env.SHOTS;
  try {
    seedBars(w.srv.st);
    for (const lid of ['1890000151']) w.srv.st.put('Charm_Option_Map', lid, { listingId: lid, map: { 'engraving side': { both: { by: 'Test', at: 1, field: 'ignore', value: null } } } });
    await page.evaluate(async () => {
      await Master.load(true);
      // SETBAR-A's CharmNestOrders.setPiecesOf, as a STUB of the same shape (one entry per piece: slotKey, sku, role, label, thickness, lengths, length, suggestedLength, layoutAsked, layoutOpen): the thinner (small) bar
      // on the shorter chain; a buyer's other layout asked for and not yet answered (layoutOpen) leaves `length` null (the default stays only the suggestion)
      window.__LEN = ['16"', '18"']; window.__FLAG = false;
      CharmNestOrders.setPiecesOf = function (x) {
        const sp = x.spec || x, set = sp.pair && sp.pair.set; if (!set) return null; const L = window.__LEN || [], open = !!window.__FLAG, sug = i => (L.length === 2 ? (i === 1 ? L[0] : L[1]) : null);
        return set.designs.map((sku, i) => ({ n: i + 1, of: set.designs.length, unit: 1, slot: i, slotKey: 'D' + (i + 1), sku, role: i === 0 ? 'large' : 'small', label: i === 0 ? 'Large bar' : 'Small bar', thickness: i === 0 ? 'thicker' : 'thinner', lengths: L, length: open ? null : sug(i), suggestedLength: sug(i), layoutAsked: open, layoutOpen: open, engraved: true, rule: 'BAR_1548-SET', source: 'rule' }));
      };
      // the pairs a person saved for the two listings (the app's own call, the fake site)
      for (const [lid, sku, big] of [['1890000151', 'BAR_1548', 'BAR_1548'], ['1890000152', 'BAR_WIDE_9999', 'BAR_WIDE_9999']]) { const r = await CN.api('charmNestLibrary', { op: 'aliasPut', listingId: lid, fromSku: sku, pair: { L: big, R: 'SKINNY_0191' }, by: 'SETBAR-B', title: 'Bar Necklace SET of 2' }); if (!r || r.ok !== true) throw new Error('aliasPut ' + JSON.stringify(r)); }
    });
    const made = await R.build(w, ['bars', 'barsWide', 'discsCtl']);
    const keyOf = n => made[n].key, jobKey = (n, slot, face) => R.ORDERS[n].rid + '_' + R.ORDERS[n].rid + '1' + '#' + slot + (face === 'front' ? '~front' : '');
    note(made.bars.jobs.length === 4 && made.bars.pieceCount === 2 && made.bars.jobs.map(j => j.slot + ':' + j.face).sort().join() === 'D1:back,D1:front,D2:back,D2:front', 'the set is ONE line of two pieces (D1, D2), each with a Front and a Back job', made.bars.jobs.map(j => j.slot + ':' + j.face + ':' + j.state).join(' '));
    const skus = await page.evaluate(() => { const r = Orders.rows().find(x => x.order.receiptId === '4290000151'); return { set: r.spec.pair.set && r.spec.pair.set.designs, pool: r.poolIds.map(id => (B.pool.rows.get(id) || {}).sku) }; });
    note(JSON.stringify(skus.set) === '["BAR_1548","SKINNY_0191"]' && JSON.stringify(skus.pool) === '["BAR_1548","SKINNY_0191"]', 'its two pieces are two different designs: the large bar and the small bar', JSON.stringify(skus));
    note(made.barsWide.jobs.length === 2 && made.discsCtl.jobs.length === 3, 'the wide set (back only) has two jobs, the plain discs three');

    // ═══ A · the Engrave detail, the large bar (D1, back) ═══
    await R.openEngraveDetail(w, 'bars', { face: 'back', slot: 'D1' }); await settle(page, 900);
    let d = await page.evaluate(`(${DETAIL})()`);
    note(d && d.tabs.length === 2 && d.tabs.map(t => t.chip).join('|') === 'LARGE BAR · 18″|SMALL BAR · 16″', 'the piece switch names the bars and the chain each hangs on, not "Disc 1 of 2"', d && d.tabs.map(t => t.chip).join('|'));
    note(d && d.sub.startsWith('BAR_1548') && d.cardTitle === 'BAR 1548 · 18″', 'the large bar\'s detail says its own design (header BAR_1548, card "BAR 1548 · 18″")', d && (d.sub + ' / ' + d.cardTitle));
    note(d && d.strip.length === 2 && d.strip.map(s => s.label + ' ' + s.len).join('|') === 'Large bar 18″|Small bar 16″', 'both bars are shown side by side above the work area, each with its chain length: the thinner (small) bar on the shorter chain', d && d.strip.map(s => s.label + ' ' + s.len).join('|'));
    note(d && d.strip.length === 2 && d.strip.every(s => s.canvas) && d.strip[0]?.pressed && !d.strip[1]?.pressed, 'both bars are drawn (their real geometry) and the one being edited is marked', d && d.strip.map(s => s.pressed + ':' + s.canvas).join(' '));
    const f0 = d && d.strip[0]?.frame, f1 = d && d.strip[1]?.frame, pad = 0.04 * 25.9, expectH = (3.9 + 2 * pad) / (4.9 + 2 * pad);
    note(!!f0 && !!f1 && near(f1.w, f0.w, 0.04) && near(f1.h / f0.h, expectH, 0.05), 'drawn at ONE scale: the same length, the thinner bar visibly thinner by 3.9 / 4.9', f0 && f1 && `w ${f0.w.toFixed(1)}/${f1.w.toFixed(1)} h ratio ${(f1.h / f0.h).toFixed(3)} want ${expectH.toFixed(3)}`);
    note(!!d && !!d.stripBox && (d.stripBox.r <= d.approveBox.l + 1 || d.stripBox.l >= d.approveBox.r - 1), 'the strip never sits under the Approve block', d && JSON.stringify([d.stripBox, d.approveBox]));
    note(!!d && d.note === '' && d.bothBars.length === 0, 'no layout note unless the line is flagged, and no press that approves both bars at once', d && JSON.stringify([d.note, d.bothBars]));
    note(!!d && d.cardThumbs.map(t => t.face).join() === 'front,back', 'the panel card has the shown bar\'s Front and Back thumbnails (the face picker)', d && d.cardThumbs.map(t => t.face).join());
    if (shots) await R.shot(w, 'setbar-detail-large');

    // ═══ B · the small bar: one press on its picture, the whole detail follows (header, card, degree box on THAT bar) ═══
    await page.evaluate(k => document.querySelector('#egQueue .engSet .engSetPc[data-piece$="#D2"]').click(), keyOf('bars'));
    await settle(page, 1100);
    d = await page.evaluate(`(${DETAIL})()`);
    note(!!d && /#D2$/.test(d.key || '') && d.tabs[1]?.pressed && d.strip[1]?.pressed && !d.strip[0]?.pressed, 'a press on the small bar\'s picture shows the small bar (switch and strip agree)', d && d.key);
    note(!!d && d.sub.startsWith('SKINNY_0191') && d.cardTitle === 'SKINNY 0191 · 16″', 'its header says SKINNY_0191 (not the line\'s first design) and its card "SKINNY 0191 · 16″"', d && (d.sub + ' / ' + d.cardTitle));
    note(!!d && d.cardThumbs.length === 2 && d.cardThumbs.every(t => t.frame), 'the small bar\'s Front and Back are drawn in the panel card', d && JSON.stringify(d.cardThumbs));
    const ANG = () => page.evaluate(() => Object.fromEntries([...Engrave.items().values()].filter(j => j.row.order.receiptId === '4290000151').map(j => [j.key.replace(/^\d+_\d+/, ''), j.fit ? Math.round((j.fit.angle || 0) * 10) / 10 : null])));
    const angles0 = await ANG();
    await page.evaluate(() => { const b = document.querySelector('#egQueue input[data-a=turn]'); b.focus(); b.value = '12'; b.dispatchEvent(new Event('input', { bubbles: true })); b.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true })); b.blur(); });
    await settle(page, 900);
    const angles1 = await ANG();
    const moved = Object.keys(angles1).filter(k => Math.abs((angles1[k] || 0) - (angles0[k] || 0)) > 1);
    note(moved.length === 1 && /#D2$/.test(moved[0]), 'the degree box turns the bar being edited and no other piece', JSON.stringify({ moved, a0: angles0, a1: angles1 }));
    d = await page.evaluate(`(${DETAIL})()`);
    note(!!d && d.turn === '12', 'the box then shows 12 for the small bar', d && d.turn);
    // the face picker: the small bar's Front
    await page.evaluate(() => document.querySelector('#egQueue .pvSetup .engFace[data-face="front"] .engThumb').click()); await settle(page, 1100);
    d = await page.evaluate(`(${DETAIL})()`);
    note(!!d && /#D2~front$/.test(d.key || ''), 'the Front thumbnail shows the small bar\'s FRONT engraving in the work area', d && d.key);
    // Back / Next walk the pieces, none skipped
    await page.evaluate(() => document.querySelector('#egQueue [data-a=prev]').click()); await settle(page, 900);
    d = await page.evaluate(`(${DETAIL})()`);
    note(!!d && /#D1/.test(d.key || ''), 'Back from the small bar goes to the large bar (the order\'s previous piece)', d && d.key);
    await page.evaluate(() => document.querySelector('#egQueue [data-a=next]').click()); await settle(page, 900);
    d = await page.evaluate(`(${DETAIL})()`);
    note(!!d && /#D2/.test(d.key || ''), 'Next goes to the small bar again', d && d.key);
    if (shots) await R.shot(w, 'setbar-detail-small');

    // ═══ C · approvals are per piece; nothing approves both ═══
    await page.evaluate(() => { window.CNEngravingSeals.who = () => 'Test Operator'; });
    await R.openEngraveDetail(w, 'bars', { face: 'back', slot: 'D1' }); await settle(page, 900);
    const jb = jobKey('bars', 'D1', 'back');
    await page.evaluate(() => document.querySelector('#egQueue .pvApproval [data-a=approve]').click());
    await page.waitForFunction(k => { const j = Engrave.items().get(k); return j && ['approved', 'written'].includes(j.state); }, jb, { timeout: 120000 }).catch(() => {});
    const st = await page.evaluate(() => Object.fromEntries([...Engrave.items().values()].filter(j => j.row.order.receiptId === '4290000151').map(j => [j.key.replace(/^\d+_\d+/, ''), j.state])));
    note(['approved', 'written'].includes(st['#D1']) && st['#D2'] === 'review' && st['#D2~front'] === 'review', 'Approve on the large bar approves that bar\'s back only: the small bar (and every other face) still waits', JSON.stringify(st));

    // never skipped: leaving the order with the small bar not approved is said plainly, with one tap to that bar by its name
    await page.waitForFunction(() => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); return c && /#D1~front$/.test(c.dataset.key || ''); }, null, { timeout: 20000 }).catch(() => {});   // (after an approval the detail goes on to the piece's other face)
    await settle(page, 700);
    await page.evaluate(() => document.querySelector('#egQueue [data-a=close]').click()); await settle(page, 600);
    const guide = await page.evaluate(() => { const g = document.querySelector('#egQueue .egGuide'); return g && !g.hidden ? { text: g.textContent.replace(/\s+/g, ' ').trim(), go: (g.querySelector('[data-g=go]') || {}).title || '' } : null; });
    note(!!guide && /2 pieces of this order are not approved yet/.test(guide.text) && /^Open piece \d of 2: (Large|Small) bar$/.test(guide.go), 'closing with two pieces still to approve is held back by one calm line, and its button names a bar ('+"'Large bar' / 'Small bar'"+'), not "Disc n"', JSON.stringify(guide));

    // ═══ D · the layout-requested note, in the Engrave detail and the order window ═══
    await page.evaluate(() => { window.__FLAG = true; });
    await R.openEngraveDetail(w, 'bars', { face: 'back', slot: 'D2' }); await settle(page, 900);
    d = await page.evaluate(`(${DETAIL})()`);
    note(!!d && /a person decides which bar hangs on which chain/.test(d.note) && d.strip.length === 2 && d.strip.every(s => s.len === ''), 'a line flagged "layout requested" says in ONE calm line that a person decides, and shows no chain on either bar until then', d && JSON.stringify([d.note, d.strip.map(s => s.len)]));
    await page.evaluate(() => { window.__FLAG = false; });

    // ═══ E · the order window ═══
    const OWIN = () => {
      const txt = n => (n ? n.textContent.replace(/\s+/g, ' ').trim() : ''), lens = document.getElementById('owSetLens'), cards = [...document.querySelectorAll('#owEng .engCard')];
      const frame = el => { const f = el && el.querySelector('.engpvFrame'); if (!f) return null; const r = f.getBoundingClientRect(); return { w: r.width, h: r.height, rel: f.style.getPropertyValue('--rel') || '' }; };
      const note = document.getElementById('owEngNote');
      return { lens: lens && !lens.hidden ? [...lens.querySelectorAll('.owSetLen')].map(n => txt(n)) : null, lensNote: lens ? txt(lens.querySelector('.owSetNote')) : '', sku: txt(document.getElementById('owSku')), note: note && !note.hidden ? txt(note) : '',
        cards: cards.map(c => ({ title: txt(c.querySelector('.engName')), approve: !!c.querySelector('[data-e=approve]:not([disabled])'), faces: [...c.querySelectorAll('.engFace')].map(f => f.dataset.face).join('+'), frames: [...c.querySelectorAll('.engThumb')].map(t => frame(t)), words: txt(c.querySelector('.engTxt')), allWords: [...c.querySelectorAll('.engFace .engTxt')].map(t => txt(t)).join(' | ') })),
        saidBlocks: document.querySelectorAll('#orderWin #owEngSaid, #orderWin .engSaid, #orderWin .esRow, #orderWin .esBar, #orderWin [data-said]').length, buttons: [...document.querySelectorAll('#owEng button')].map(b => txt(b)).filter(t => /both|all/i.test(t)),
        rows: [...document.querySelectorAll('#owPcSum .owPcRow')].map(n => txt(n.querySelector('.owPcName'))), subs: [...document.querySelectorAll('#owPcSum .owPcSub')].map(n => txt(n.querySelector('.owPcSubName'))) };
    };
    await R.openOrderWindow(w, 'bars'); await page.evaluate(() => { const b = document.querySelector('#owPcSum .owPcX'); if (b) b.click(); }); await settle(page, 900);
    let o = await page.evaluate(`(${OWIN})()`);
    note(!!o.lens && o.lens.join('|') === 'Large bar18″|Small bar16″', 'the Vector design shows both bars side by side with each chain length under it', o.lens && o.lens.join('|'));
    note(o.cards.length === 2 && o.cards.map(c => c.title).join('|') === 'BAR 1548 · 18″|SKINNY 0191 · 16″', 'the Engraving section has one card per bar, each named by its own design and chain (not "BAR 1548 + SKINNY 0191 · Disc n")', o.cards.map(c => c.title).join('|'));
    note(o.cards.every(c => c.approve && c.faces === 'front+back') && o.buttons.length === 0, 'each bar\'s card has its OWN Approve and both faces; no button approves both bars', JSON.stringify(o.cards.map(c => [c.approve, c.faces])) + JSON.stringify(o.buttons));
    const ca = o.cards[0].frames[0], cb = o.cards[1].frames[0];
    note(!!ca && !!cb && near(cb.w, ca.w, 0.05) && near(cb.h / ca.h, expectH, 0.06), 'the two cards draw their bars at one scale (the thinner bar thinner)', ca && cb && `h ratio ${(cb.h / ca.h).toFixed(3)} want ${expectH.toFixed(3)}`);
    note(o.subs.join('|') === 'Large bar · 18″|Small bar · 16″', 'Its pieces lists the bars by name and chain, not "Piece 1 of 2"', o.subs.join('|'));
    note(o.note === '', 'no layout note on an ordinary set', o.note);
    // a buyer who asked for another layout: one calm line above the cards and under the picture, and no chain under either bar until a person decides
    await page.evaluate(() => { window.__FLAG = true; OrderWin.close(); }); await R.openOrderWindow(w, 'bars'); await settle(page, 900);
    o = await page.evaluate(`(${OWIN})()`);
    note(o.note === 'The buyer asked for a different layout, so a person decides which bar hangs on which chain.' && /different layout/.test(o.lensNote), 'a flagged line says in one calm line (above the cards, and under the picture) that a person decides', o.note);
    note(!!o.lens && o.lens.join('|') === 'Large bar|Small bar' && o.cards.map(c => c.title).join('|') === 'BAR 1548|SKINNY 0191', 'while the layout waits for a person no chain is shown under either bar or in either card', JSON.stringify([o.lens, o.cards.map(c => c.title)]));
    await page.evaluate(() => { window.__FLAG = false; OrderWin.close(); }); await R.openOrderWindow(w, 'bars'); await settle(page, 700);
    // the buyer's words that belong to each bar (two different words, as a per-length reading would give)
    await page.evaluate(() => { const j = Engrave.items().get('4290000151_42900001511#D2'); j.lines = ['Catalina']; j.text = 'Catalina'; j.lineInput = ['Catalina']; const j1 = Engrave.items().get('4290000151_42900001511#D1'); j1.lines = ['Ginger']; j1.text = 'Ginger'; j1.lineInput = ['Ginger']; OrderWin.close(); });
    await R.openOrderWindow(w, 'bars'); await settle(page, 900);
    o = await page.evaluate(`(${OWIN})()`);
    note(o.note === '' && !o.lensNote, 'no layout note once the flag is gone', o.note);
    // a press on a bar's line in Its pieces narrows the section to that bar (as a press on a disc does)
    await page.evaluate(() => { const b = document.querySelector('#owPcSum .owPcX'); if (b && b.getAttribute('aria-expanded') !== 'true') b.click(); }); await settle(page, 500);
    await page.evaluate(() => document.querySelector('#owPcSum .owPcSub[data-sub$="#D2"]').click()); await settle(page, 700);
    const o2 = await page.evaluate(`(${OWIN})()`), picked = await page.evaluate(() => OrderWin.selectedDisc());
    note(o2.cards.length === 1 && o2.cards[0].title === 'SKINNY 0191 · 16″' && picked === 'D2', 'a press on "Small bar · 16″" in Its pieces shows that bar\'s card alone (the pick is its slot, D2)', JSON.stringify([o2.cards.map(c => c.title), picked]));
    await page.evaluate(() => document.querySelector('#owPcSum .owPcSub[data-sub$="#D2"]').click()); await settle(page, 500);
    o = await page.evaluate(`(${OWIN})()`);
    note(o.saidBlocks === 0, 'NOSAID: the buyer\'s wording that belongs to each bar is no longer said in a block under the cards ("Large bar · 18″ Ginger / Small bar · 16″ Catalina" is gone from the order window)', String(o.saidBlocks));
    note(o.cards.length === 2 && /Ginger/i.test(o.cards[0].allWords) && /Catalina/i.test(o.cards[1].allWords) && !/Catalina/i.test(o.cards[0].allWords) && !/Ginger/i.test(o.cards[1].allWords), 'each bar\'s own words are on its own card: Ginger on the large bar\'s, Catalina on the small bar\'s', JSON.stringify(o.cards.map(c => c.allWords)));
    // the reader keeps the per-bar wording as data (OrderEngraving.said.of: pieces of a set of different designs each say the words that belong to them)
    const bars = await page.evaluate(() => { const S = window.OrderEngraving && OrderEngraving.said; const r = Orders.rows().find(x => String(x.order.receiptId) === '4290000151'); if (!S || !r || !window.CharmNestEngraveSides) return null;
      const pcs = [['Large bar', '18"', 'Ginger'], ['Small bar', '16"', 'Catalina']].map(([label, length, t]) => ({ lineKey: r.key, lineName: 'BAR', design: { label, length }, faces: { back: { text: t } } }));
      return S.of([r], pcs).map(e => e.bars); });
    note(!!bars && !!bars[0] && bars[0].length === 2 && bars[0][0][0] === 'Large bar · 18″' && /Ginger/.test(bars[0][0][1]) && bars[0][1][0] === 'Small bar · 16″' && /Catalina/.test(bars[0][1][1]), 'the reader (OrderEngraving.said.of) still returns each bar\'s words, under its name and chain, as data', JSON.stringify(bars));
    if (shots) await R.shot(w, 'setbar-order');

    // ═══ F · the wide set: true relative size by LENGTH (50 mm bar and the 25.9 mm bar) ═══
    await R.openEngraveDetail(w, 'barsWide', { face: 'back', slot: 'D2' }); await settle(page, 1200);
    d = await page.evaluate(`(${DETAIL})()`);
    const ratio = 25.9 / 50;
    note(!!d && d.strip.length === 2 && !!d.strip[0]?.frame && !!d.strip[1]?.frame && near(d.strip[1].frame.w / d.strip[0].frame.w, ratio, 0.06), 'a 50 mm bar and a 25.9 mm bar are drawn in the strip in their true proportion (width ratio 0.518)', d && d.strip[0]?.frame && d.strip[1]?.frame && (d.strip[1].frame.w / d.strip[0].frame.w).toFixed(3));
    note(!!d && d.cardThumbs.length === 1 && !!d.cardThumbs[0].frame && near(+d.cardThumbs[0].frame.rel, ratio, 0.03), 'the small bar\'s thumbnail in the panel is that fraction of the box', d && JSON.stringify(d.cardThumbs));
    await R.openOrderWindow(w, 'barsWide'); await settle(page, 900);
    o = await page.evaluate(`(${OWIN})()`);
    note(o.cards.length === 2 && !!o.cards[0].frames[0] && !!o.cards[1].frames[0] && near(o.cards[1].frames[0].w / o.cards[0].frames[0].w, ratio, 0.06) && o.cards.every(c => c.approve), 'the order window draws the two bars in that proportion, each with its own Approve', o.cards.map(c => c.frames[0] && c.frames[0].w.toFixed(1) + ' rel ' + c.frames[0].rel).join(' | '));
    if (shots) await R.shot(w, 'setbar-order-wide');

    // ═══ G · nothing else changed: three plain discs of one design ═══
    await R.openEngraveDetail(w, 'discsCtl', { face: 'back', slot: 'D2' }); await settle(page, 900);
    d = await page.evaluate(`(${DETAIL})()`);
    note(!!d && d.tabs.map(t => t.chip).join('|') === 'DISC 1 of 3|DISC 2 of 3|DISC 3 of 3' && d.strip.length === 0 && d.sub.startsWith('COMET_9427') && /^COMET 9427 · Disc 2$/.test(d.cardTitle), 'three plain discs: "DISC n of 3" tabs, no strip, the line\'s own design and "COMET 9427 · Disc 2"', d && JSON.stringify([d.tabs.map(t => t.chip), d.strip.length, d.sub, d.cardTitle]));
    await R.openOrderWindow(w, 'discsCtl'); await page.evaluate(() => { const b = document.querySelector('#owPcSum .owPcX'); if (b) b.click(); }); await settle(page, 700);
    o = await page.evaluate(`(${OWIN})()`);
    note(o.lens === null && o.note === '' && o.cards.map(c => c.title).join('|') === 'COMET 9427 · Disc 1|COMET 9427 · Disc 2|COMET 9427 · Disc 3' && o.subs.join('|') === 'Disc 1 of 3|Disc 2 of 3|Disc 3 of 3' && o.saidBlocks === 0, 'three plain discs in the order window: no bar labels, the cards and the list as they always were', JSON.stringify([o.lens, o.cards.map(c => c.title), o.subs]));

    note(w.errors.length === 0, 'no page errors', w.errors.slice(0, 2).join(' | '));
  } catch (e) { console.log('  FAIL test crashed: ' + (e && e.stack || e)); fails.push('crash'); }
  finally { await w.close(); }
  console.log(fails.length ? `\n${fails.length} FAILED:\n - ${fails.join('\n - ')}` : '\nAll checks passed');
  process.exit(fails.length ? 1 : 0);
})();
