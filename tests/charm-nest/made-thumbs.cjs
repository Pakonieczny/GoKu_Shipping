// THUMBSYNC (Paul, 28 Oct 2026: "the same changes should be saved as thumbnails to represent those particular orders so that a user scrolling through a list of orders can see updated thumbnails that already
// include the given charm's engraving for both back and front when necessary, including any images or text ... These changes must be real time on the host computer and updated on the cloud fairly quickly
// for others to see on other computers"). charm-nest-made-thumb.js, ListMedia (charm-nest-bridge.js) and charmNestLibrary.js op madeThumbPut, on the real sorter page over the repo's fake site
// (_engraveRealWorld.cjs: real drawings, real fits, offline, made-up names) and a SECOND computer (another browser context on the same fake site) that never pooled the order.
//   A  the mini tag ordered "front - W / back - 1. 27. 24": its Orders row shows the piece as it will be made, front and back side by side, the fitted words drawn in them (more ink than the same faces
//      without words); the picture is kept on the piece's pool row in the cloud (Storage, `made`), sent once the fits had settled
//   B  the second computer shows that picture in its own Orders row (read with the order's pool rows its placement feed reads anyway: no read of its own)
//   C  on the first page a square is added to the front in the charm editor (CharmNestArtEdit.save) and the back's words are resized twice right after: the row's picture shows the new part within a
//      second of the save, with no reload; the cloud is written ONCE for the burst (debounced), with the signature of what the page shows
//   D  the second computer's row shows the new picture after its changes feed moves, without a reload (the same page, the new address)
//   F  FACES (Paul, 28 Oct 2026: "label the back engraving with the same style of text with the black background ... the text font must be larger ... side-by-side and top bottom alignment, depending on
//      the charm shape ... split the image with very thin lines ... enlarge every image thumbnail preview ... by remove the text underneath"): each face is named under it in the earrings' dark chip with
//      words a third larger than theirs; a wide piece (the tag) is drawn one over the other, a tall one (the cat) side by side, parted by one hairline; the words under the pictures are gone from sight
//      (still read by a screen reader) and the boxes are larger than the 88 px they were; Reset zoom is a button on the picture's corner, reachable by Tab, named by its tooltip; the box says what it shows.
//      Screenshots of the rows (wide, tall, an earring pair) go to SHOTS.
//   E  the sandbox keeps its pictures apart (a Sandbox_ pool row and charmnest/sandbox/made/), a PNG that is not one is refused, a clear takes the picture and its file away
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/made-thumbs.cjs
'use strict';
const path = require('node:path');
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : evidence || ''}`.trimEnd()); };
const wait = ms => new Promise(r => setTimeout(r, ms));
const role = page => page.waitForFunction(() => !window.CNRole || ['ask', 'role', 'admin'].includes(CNRole.state()), null, { timeout: 20000 }).catch(() => {}).then(() => page.evaluate(() => { try { if (window.CNRole && CNRole.state() === 'ask') CNRole.choose('design'); } catch (_) { /* no role step */ } }));

(async () => {
  const R = require('./_engraveRealWorld.cjs'), Base = require('./_orderFactsWorld.cjs');
  const w = await R.world({ shots: process.env.SHOTS || '/tmp/made-thumbs-shots' });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  const page = w.page, st = w.srv.st;
  const puts = pid => st.calls.filter(c => c.op === 'madeThumbPut' && c.body && c.body.poolId === pid);
  try {
    await role(page);
    const made = await R.build(w, ['tag', 'both', 'earrings']), PID = made.tag.poolIds[0], KEY = made.tag.key, RID = made.tag.rid;
    console.log(`   built: ${KEY} pieces ${JSON.stringify(made.tag.poolIds)} jobs ${JSON.stringify(made.tag.jobs.map(j => [j.face, j.state, j.text, j.fitted]))}`);
    if (!st.doc('Charm_Pool', PID)) st.put('Charm_Pool', PID, { poolId: PID, orderId: RID, sku: '5X8.5', state: 'pooled' });   // (the pool row the page wrote; a world that did not reach the cloud gets one)
    // ── A ──
    await page.evaluate(() => { CN.setMode('orders'); Orders.render(); });
    const box = `[data-key="${KEY}"] [data-vector]`;
    await page.waitForFunction(sel => { const h = document.querySelector(sel); const im = h && h.querySelector('img'); return !!(h && h.dataset.made && im && im.complete && im.naturalWidth > 0); }, box, { timeout: 30000 }).catch(() => {});
    const a = await page.evaluate(([sel, pid]) => {
      const MT = CharmNestMadeThumb, h = document.querySelector(sel), im = h && h.querySelector('img'), s = MT.stateOf(pid);
      const ink = url => new Promise(res => { const i = new Image(); i.onload = () => { const c = document.createElement('canvas'); c.width = i.naturalWidth; c.height = i.naturalHeight; const g = c.getContext('2d'); g.drawImage(i, 0, 0); const d = g.getImageData(0, 0, c.width, c.height).data; let n = 0; for (let k = 0; k < d.length; k += 4) if (d[k] < 70 && d[k + 1] < 70 && d[k + 2] < 70) n++; res({ n, w: c.width, h: c.height }); }; i.src = url; });
      const bare = s ? MT.pictureOf(pid, { tiles: s.tiles.map(t => Object.assign({}, t, { words: null })), sig: 'bare-' + s.sig }) : null;
      return Promise.all([im ? ink(im.src) : null, bare ? ink(bare.url) : null]).then(([shown, without]) => ({ made: h && h.dataset.made, faces: s && s.faces, layout: s && MT.pictureOf(pid).layout, words: s && s.tiles.map(t => !!t.words), same: !!(im && s && im.src === MT.pictureOf(pid).url), shown, without }));
    }, [box, PID]);
    check(`A1 the Orders row of the mini tag shows the piece as it will be made, front and back (${a.shown ? a.shown.w + 'x' + a.shown.h : '-'} px, faces ${a.faces}, the long tag laid flat: one over the other, near a square), this page's own drawing`,
      a.made && a.same && JSON.stringify(a.faces) === '["front","back"]' && a.layout === 'column' && a.shown && Math.max(a.shown.w, a.shown.h) / Math.min(a.shown.w, a.shown.h) < 2, JSON.stringify(a));
    check(`A2 the fitted words are in it: ${a.shown && a.shown.n} dark pixels against ${a.without && a.without.n} for the same faces without words`, a.words && a.words.every(Boolean) && a.shown && a.without && a.shown.n > a.without.n + 600, JSON.stringify(a));
    for (let t0 = Date.now(); Date.now() - t0 < 20000 && !(st.doc('Charm_Pool', PID) || {}).made; ) await wait(250);
    const m0 = (st.doc('Charm_Pool', PID) || {}).made, n0 = puts(PID).length, sig0 = await page.evaluate(pid => CharmNestMadeThumb.sigOf(pid), PID);
    check(`A3 the cloud keeps it on the piece's pool row (${n0} upload${n0 === 1 ? '' : 's'} during the build, the last with what the page shows) and in Storage`, !!m0 && m0.sig === sig0 && st.blobs.has(m0.path) && /^charmnest\/made\//.test(m0.path) && n0 >= 1 && n0 <= 3, JSON.stringify({ m0, n0, sig0 }));

    // (the same picture in the Design & Engrave list and the order window's Vector design: every list box is ListMedia.vectorInto)
    const madeIn = sel => page.waitForFunction(([sel, key]) => [...document.querySelectorAll(sel)].some(h => h._mvRow && h._mvRow.key === key && h.dataset.made && h.querySelector('img') && h.querySelector('img').naturalWidth > 0), [sel, KEY], { timeout: 15000 }).then(() => true, () => false);
    await page.evaluate(() => { CN.setMode('engrave'); Engrave.restoreView({ tab: 'place', list: true, chosen: true }); Engrave.render(); });
    const inEngrave = await madeIn('#engraveView [data-vector]');
    await R.openOrderWindow(w, 'tag');
    const inWindow = await madeIn('#owVector');
    await page.evaluate(() => { try { OrderWin.close(); } catch (_) {} CN.setMode('orders'); Orders.render(); });
    check('A4 the Design & Engrave list and the order window\'s Vector design show the same picture', inEngrave && inWindow, JSON.stringify({ inEngrave, inWindow }));

    // ── F: the faces named and parted, the captions gone, the boxes larger ──
    for (let i = 0; i < 5 && await page.evaluate(() => !!document.querySelector('dialog[open]')); i++) { await page.keyboard.press('Escape'); await wait(400); }   // (the order window closed as a person closes it: nothing modal over the list)
    await page.evaluate(() => { for (const d of document.querySelectorAll('dialog[open]')) d.close(); });
    const shape = await page.evaluate(([a, b]) => Promise.all([a, b].map(pid => new Promise(res => {
      const pic = CharmNestMadeThumb.pictureOf(pid); if (!pic) return res(null);
      const i = new Image(); i.onload = () => { const c = document.createElement('canvas'); c.width = i.naturalWidth; c.height = i.naturalHeight; const g = c.getContext('2d'); g.drawImage(i, 0, 0); const d = g.getImageData(0, 0, c.width, c.height).data;
        const rule = (x, y) => { const k = (y * c.width + x) * 4; return Math.abs(d[k] - 0xd6) < 8 && Math.abs(d[k + 1] - 0xcf) < 8 && Math.abs(d[k + 2] - 0xc2) < 8; };
        let rows = 0, cols = 0; for (let y = 0; y < c.height; y++) { let n = 0; for (let x = 0; x < c.width; x++) if (rule(x, y)) n++; if (n > c.width * 0.85) rows++; } for (let x = 0; x < c.width; x++) { let n = 0; for (let y = 0; y < c.height; y++) if (rule(x, y)) n++; if (n > c.height * 0.85) cols++; }
        let dark = 0; for (let k = 0; k < d.length; k += 4) if (Math.abs(d[k] - 0x2a) < 6 && Math.abs(d[k + 1] - 0x27) < 6 && Math.abs(d[k + 2] - 0x24) < 6) dark++;
        res({ layout: pic.layout, labels: pic.labels, frac: pic.fontPx / Math.max(pic.w, pic.h), hair: pic.hair, w: pic.w, h: pic.h, ruleRows: rows, ruleCols: cols, chipInk: dark }); }; i.src = pic.url; }))), [PID, made.both.poolIds[0]]);
    const [wide, tall] = shape;
    check(`F1 each face is named under it in the earrings' dark chip, its words ${wide && (wide.frac * 100).toFixed(1)} % of the picture's long side (the earrings' 7 %): ${wide && wide.chipInk} chip pixels`, wide && tall && wide.labels && tall.labels && wide.frac >= 0.09 && tall.frac >= 0.09 && wide.chipInk > 200 && tall.chipInk > 200, JSON.stringify(shape));
    check(`F2 the wide tag one over the other (${wide && wide.w}x${wide && wide.h}), the tall cat side by side (${tall && tall.w}x${tall && tall.h}), each parted by ONE hairline of ${wide && wide.hair} px (${wide && wide.ruleRows} row / ${tall && tall.ruleCols} column of it)`,
      wide && tall && wide.layout === 'column' && tall.layout === 'row' && wide.ruleRows >= 1 && wide.ruleRows <= wide.hair + 1 && wide.ruleCols === 0 && tall.ruleCols >= 1 && tall.ruleCols <= tall.hair + 1 && tall.ruleRows === 0, JSON.stringify(shape));
    const rowLook = await page.evaluate(([k1, k2]) => [k1, k2].map(key => {
      const n = document.querySelector(`[data-key="${key}"]`), v = n && n.querySelector('[data-vector]'), fig = v && v.closest('figure'), cap = fig && fig.querySelector('figcaption'), reset = fig && fig.querySelector('.thumbReset');
      const r = v ? v.getBoundingClientRect() : null, f = fig ? fig.getBoundingClientRect() : null, cs = cap ? getComputedStyle(cap) : null;
      return { w: r && Math.round(r.width), h: r && Math.round(r.height), figH: f && Math.round(f.height), capPos: cs && cs.position, capFont: cs && cs.fontSize, capText: cap && cap.textContent, label: v && v.getAttribute('aria-label'), title: v && v.title,
        listing: (n.querySelector('[data-listing]') || {}).getAttribute && n.querySelector('[data-listing]').getAttribute('aria-label'), reset: reset ? { hidden: reset.hidden, title: reset.title } : null };
    }), [KEY, made.earrings.key]);
    const [tagRow, earRow] = rowLook;
    check(`F3 the words under the pictures are gone from sight (read by a screen reader: "${tagRow.capText}", "${earRow.capText}") and the boxes are ${tagRow.w}x${tagRow.h} px (88 before), the figure no taller than its box`,
      tagRow.capPos === 'absolute' && tagRow.capFont === '0px' && tagRow.w >= 100 && tagRow.h >= 100 && tagRow.figH - tagRow.h <= 1 && /Vector design/.test(tagRow.capText) && /Left \+ Right/.test(earRow.capText) && earRow.w === tagRow.w, JSON.stringify(rowLook));
    check(`F4 each box says what it shows ("${tagRow.label}", "${earRow.label}", "${tagRow.listing}") and its tooltip begins with it`, /As it will be made · front and back/.test(tagRow.label) && /^Vector design · Left \+ Right$/.test(earRow.label) && /Etsy listing/.test(tagRow.listing) && (!tagRow.title || tagRow.title.startsWith(tagRow.label)), JSON.stringify(rowLook));
    const reach = await page.evaluate(async key => {
      const fig = document.querySelector(`[data-key="${key}"] [data-vector]`).closest('figure'), b = fig.querySelector('.thumbReset'), box = fig.querySelector('[data-vector]').getBoundingClientRect();
      if (!b || b.hidden) return { none: true, hidden: b && b.hidden };
      const before = getComputedStyle(b).opacity; b.focus(); const now = document.activeElement === b, tabbable = b.tabIndex >= 0 && !b.disabled;
      await new Promise(r => setTimeout(r, 300)); const r = b.getBoundingClientRect(), still = document.activeElement === b;
      return { before, after: getComputedStyle(b).opacity, focused: now, still, active: document.activeElement && (document.activeElement.className || document.activeElement.tagName), tabbable, title: b.title, connected: b.isConnected, inCorner: r.right <= box.right + 1 && r.top >= box.top - 1 && r.right > box.right - 40 && r.top < box.top + 40 };
    }, KEY);
    check(`F5 Reset zoom sits on the picture's corner (unseen ${reach.before}, focused ${reach.after}), reachable by keyboard, named "${reach.title}"`, reach.focused && reach.before === '0' && reach.after === '1' && reach.inCorner && reach.title === 'Vector design · reset zoom', JSON.stringify(reach));
    if (process.env.SHOTS) { require('node:fs').mkdirSync(process.env.SHOTS, { recursive: true }); for (const [n, key] of [['wide-tag', KEY], ['tall-cat', made.both.key], ['earrings', made.earrings.key]]) { await page.locator(`[data-key="${key}"]`).first().scrollIntoViewIfNeeded().catch(() => {}); await page.locator(`[data-key="${key}"]`).first().screenshot({ path: path.join(process.env.SHOTS, `FACES-row-${n}.png`) }).catch(e => console.log('   shot', n, e.message)); } }

    // ── B: the second computer ──
    const ctx2 = await w.browser.newContext({ viewport: { width: 1440, height: 900 } });
    await ctx2.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => r.abort());
    await ctx2.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Second Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Second Operator'; });
    const p2 = await ctx2.newPage(), errors2 = []; p2.on('pageerror', e => errors2.push(e.message)); p2.setDefaultTimeout(30000);
    await p2.goto(`${w.srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await p2.waitForFunction(() => window.CN && window.Orders && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await role(p2);
    await p2.evaluate(async ({ fx, orderSrc }) => {
      const orderOf = eval('(' + orderSrc + ')'), order = orderOf(fx, Date.now());
      await Master.load({ force: true, fresh: true }).catch(() => {}); await Orders.loadMaps(true);
      for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); CN.setMode('orders'); Orders.render(); window.__notReloaded = 1;
    }, { fx: R.ORDERS.tag, orderSrc: Base.orderOf.toString() });
    const shownUrl = (pg, sel) => pg.evaluate(sel => { const h = document.querySelector(sel), MT = window.CharmNestMadeThumb, row = h && h._mvRow, f = row && MT ? MT.forLine(row) : null, im = h && h.querySelector('img'); return { made: !!(h && h.dataset.made), key: f ? f.key : '', loaded: !!(im && im.complete && im.naturalWidth > 0), w: im ? im.naturalWidth : 0, h: im ? im.naturalHeight : 0, src: im ? im.src.slice(0, 40) : '', alive: !!window.__notReloaded }; }, sel);
    let b = null; for (let t0 = Date.now(); Date.now() - t0 < 30000; ) { b = await shownUrl(p2, box); if (b.made && b.loaded && b.key.includes(m0.url)) break; await wait(300); }
    check(`B1 the second computer, which never pooled the order, shows the cloud's picture in its Orders row (${b && b.w}x${b && b.h} px)`, b && b.made && b.loaded && b.key.includes(m0.url) && b.w > 0, JSON.stringify(b));

    // ── C: a charm editor change and a burst of word moves on the first page ──
    const before = await page.evaluate(sel => { const im = document.querySelector(sel + ' img'); return im ? im.src : ''; }, box);
    const c = await page.evaluate(async ([pid, sel, was]) => {
      const charm = Pool.charmOf(pid), bb = charm.outline.bbox, cx = (bb[0] + bb[2]) / 2, cy = bb[1] + (bb[3] - bb[1]) * 0.3, s = 1.2;
      const sq = [[['m', [cx - s, -(cy - s)]], ['l', [cx + s, -(cy - s)]], ['l', [cx + s, -(cy + s)]], ['l', [cx - s, -(cy + s)]], ['h']]];
      const view = { M: [1, 0, 0, 1, 0, 0], R: [1, 0, 0, 1, 0, 0] };
      const t0 = performance.now();
      await CharmNestArtEdit.save(pid, 'front', { added: [{ id: 'madeSq', kind: 'path', role: 'hatch', subpaths: sq }] }, { view, by: 'Test Operator' });
      const saved = performance.now();
      // (an IMAGE put on the BACK in the charm editor: a traced picture's part, its own grey (charm-nest-ce-image.js): Paul, 28 Oct 2026, "that image must be perfectly visible ... in all thumbnail previews")
      const ix = bb[0] + (bb[2] - bb[0]) * 0.8, iy = bb[1] + (bb[3] - bb[1]) * 0.5, r = 1.5, img = [[['m', [ix - r, -(iy - r)]], ['l', [ix + r, -(iy - r)]], ['l', [ix + r, -(iy + r)]], ['l', [ix - r, -(iy + r)]], ['h']]];
      await CharmNestArtEdit.save(pid, 'back', { added: [{ id: 'madeImg', kind: 'path', role: 'hatch', fill: [0.45, 0.45, 0.45], artGrey: true, subpaths: img }] }, { view, by: 'Test Operator' });
      const back = Engrave.jobsOfPiece(pid).back;
      for (const k of [0.95, 0.9]) { if (back && back.fit) Engrave.resize(back, back.fit.size * k); await new Promise(r => setTimeout(r, 150)); }
      // the row's picture: the first frame it shows the new part (its front has the edit)
      let shownAt = 0;
      for (let i = 0; i < 200 && !shownAt; i++) { await new Promise(r => requestAnimationFrame(r)); const im = document.querySelector(sel + ' img'), s = CharmNestMadeThumb.stateOf(pid); if (im && im.src !== was && im.complete && s && Pool.charmOf(pid).artEdit && Pool.charmOf(pid).artEdit.front && im.src === CharmNestMadeThumb.pictureOf(pid).url) shownAt = performance.now(); if (!shownAt) await new Promise(r => setTimeout(r, 10)); }
      const greys = await new Promise(res => { const im = document.querySelector(sel + ' img'); const i = new Image(); i.onload = () => { const c = document.createElement('canvas'); c.width = i.naturalWidth; c.height = i.naturalHeight; const g = c.getContext('2d'); g.drawImage(i, 0, 0); const d = g.getImageData(0, 0, c.width, c.height).data; let n = 0; for (let k = 0; k < d.length; k += 4) if (Math.abs(d[k] - 115) < 12 && Math.abs(d[k + 1] - 115) < 12 && Math.abs(d[k + 2] - 115) < 12) n++; res(n); }; i.onerror = () => res(-1); i.src = im ? im.src : ''; });
      return { saveMs: Math.round(saved - t0), shownMs: shownAt ? Math.round(shownAt - saved) : -1, added: !!(Pool.charmOf(pid).artEdit && Pool.charmOf(pid).artEdit.front && Pool.charmOf(pid).artEdit.front.added), backImage: !!(Pool.charmOf(pid).artBack || []).length, greys };
    }, [PID, box, before]);
    // (the picture shown first may be the one with the edit and the words before the resizes; what matters is that the edit is in the row within a second of the save)
    check(`C1 the editor's new part is in the row's picture ${c.shownMs} ms after the save (the save took ${c.saveMs} ms), no reload`, c.added && c.shownMs >= 0 && c.shownMs <= 1000, JSON.stringify(c));
    // (read again once the burst has settled: the picture shown now holds the back's image too)
    const greysNow = await page.evaluate(sel => new Promise(res => { const im = document.querySelector(sel + ' img'), i = new Image(); i.onload = () => { const c = document.createElement('canvas'); c.width = i.naturalWidth; c.height = i.naturalHeight; const g = c.getContext('2d'); g.drawImage(i, 0, 0); const d = g.getImageData(0, 0, c.width, c.height).data; let n = 0; for (let k = 0; k < d.length; k += 4) if (Math.abs(d[k] - 115) < 12 && Math.abs(d[k + 1] - 115) < 12 && Math.abs(d[k + 2] - 115) < 12) n++; res(n); }; i.onerror = () => res(-1); i.src = im ? im.src : ''; }), box);
    check(`C1b an image put on the back in the charm editor is in the row's picture, in its own grey (${greysNow} grey pixels)`, c.backImage && greysNow > 30, JSON.stringify({ c, greysNow }));
    for (let t0 = Date.now(); Date.now() - t0 < 25000; ) { const st2 = await page.evaluate(() => CharmNestMadeThumb.state()); if (!st2.pending && !st2.sending && puts(PID).length > n0) { await wait(2500); const again = await page.evaluate(() => CharmNestMadeThumb.state()); if (!again.pending && !again.sending) break; } await wait(300); }
    const m1 = (st.doc('Charm_Pool', PID) || {}).made, n1 = puts(PID).length, sig1 = await page.evaluate(pid => CharmNestMadeThumb.sigOf(pid), PID);
    check(`C2 the cloud is written once for the burst (a front edit, a back image and two resizes: ${n1 - n0} upload${n1 - n0 === 1 ? '' : 's'}), with what the page shows; the old picture's file is gone`, n1 - n0 === 1 && m1 && m1.sig === sig1 && m1.url !== m0.url && !st.blobs.has(m0.path) && st.blobs.has(m1.path), JSON.stringify({ n0, n1, m1, sig1 }));

    // (the order window's sheet panel and the sheet window's piece draw the charm on a canvas: paintInto lays the picture over it, at the size it is drawn)
    const pi = await page.evaluate(async pid => { const cv = document.createElement('canvas'); cv.width = cv.height = 368; const ok = await CharmNestMadeThumb.paintInto(cv, pid); const d = cv.getContext('2d').getImageData(0, 0, 368, 368).data; let n = 0; for (let k = 0; k < d.length; k += 4) if (Math.abs(d[k] - 115) < 12 && Math.abs(d[k + 1] - 115) < 12 && Math.abs(d[k + 2] - 115) < 12) n++; return { ok, n, made: cv.dataset.made }; }, PID);
    check(`C3 a piece's canvas picture (the order window's sheet panel, the sheet window's piece) is painted as made, the back's image in it (${pi.n} grey pixels)`, pi.ok && pi.made === '1' && pi.n > 30, JSON.stringify(pi));

    // ── D: the second computer follows, without a reload ──
    let d = null; for (let t0 = Date.now(); Date.now() - t0 < 30000; ) { d = await shownUrl(p2, box); if (d.loaded && d.key.includes(m1.url)) break; await wait(300); }
    check(`D1 the second computer's row shows the new picture after its changes feed moved (${d && d.w}x${d && d.h} px), the same page (no reload)`, d && d.loaded && d.key.includes(m1.url) && d.alive, JSON.stringify(d));
    check('D2 nothing on either page erred', !w.errors.length && !errors2.length, JSON.stringify({ one: w.errors.slice(0, 3), two: errors2.slice(0, 3) }));
    if (process.env.SHOTS) { const rowSel = `[data-key="${KEY}"]`; await page.locator(rowSel).first().screenshot({ path: path.join(process.env.SHOTS, 'MADE-row-1.png') }).catch(() => {}); await p2.locator(rowSel).first().screenshot({ path: path.join(process.env.SHOTS, 'MADE-row-2.png') }).catch(() => {}); }

    // ── E: the server's own rules ──
    const call = async body => { const r = await fetch(`${w.srv.sorterOrigin}/.netlify/functions/charmNestLibrary`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }); return { status: r.status, out: await r.json().catch(() => ({})), feed: r.headers.get('x-cn-feed') || '' }; };
    const png = await page.evaluate(pid => CharmNestMadeThumb.pictureOf(pid).url, PID);
    st.put('Sandbox_Charm_Pool', PID, { poolId: PID, orderId: RID, state: 'pooled', charmHash: 'h1' });
    const sb = await call({ op: 'madeThumbPut', sandbox: true, poolId: PID, png, sig: 'abc1', faces: ['front', 'back'], by: 'Test Operator', dev: 'dtest1' });
    const sbRow = st.doc('Sandbox_Charm_Pool', PID), prod = st.doc('Charm_Pool', PID);
    check('E1 the sandbox keeps its picture on its own pool row and under charmnest/sandbox/made/; the real row is untouched; the answer names the order for the feed',
      sb.status === 200 && sbRow.made && /^charmnest\/sandbox\/made\//.test(sbRow.made.path) && sbRow.made.base === 'h1' && prod.made && prod.made.url === m1.url && new RegExp('p:' + RID).test(sb.feed), JSON.stringify({ sb, sbRow: sbRow && sbRow.made }));
    const bad = await call({ op: 'madeThumbPut', sandbox: true, poolId: PID, png: 'data:image/png;base64,' + Buffer.from('not a png at all').toString('base64'), sig: 'abc2' });
    const none = await call({ op: 'madeThumbPut', sandbox: true, poolId: '4290000999_42900009991_1', png, sig: 'abc3' });
    const files = () => [...st.blobs.keys()].filter(k => k.startsWith('charmnest/sandbox/made/'));
    check('E2 a picture that is not a PNG is refused, and one for a piece that is not in the pool leaves no file behind', bad.status === 400 && none.status === 404 && files().length === 1, JSON.stringify({ bad, none, files: files() }));
    const cl = await call({ op: 'madeThumbPut', sandbox: true, poolId: PID, clear: true, by: 'Test Operator' });
    check('E3 a clear takes the picture off the row and its file away', cl.status === 200 && !st.doc('Sandbox_Charm_Pool', PID).made && files().length === 0, JSON.stringify({ cl, files: files() }));
    await ctx2.close();
  } catch (e) { check('the test ran', false, e && e.stack); }
  finally { await w.close(); }
  const failed = RESULTS.filter(r => !r.ok);
  console.log(`\nmade-thumbs: ${RESULTS.length - failed.length}/${RESULTS.length} passed`);
  process.exit(failed.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
