// ENGLIVE (engrave-prenest-1013): the order window card and the Engrave detail of a HELD, NEVER-NESTED, BACK-ONLY DISC, on REAL drawings (tests/charm-nest/_engraveRealWorld.cjs: ORDERS.heldDisc and its twin ORDERS.discTwin).
//
// Paul, 13 Oct 2026, on the live order window of a Birth Flower disc necklace that is on Hold and was never nested: "This back engraving is incorrectly being shown on the front of the charm where it is clearly marked as
// "Back". Also the engraving chosen is not placed within the allowable engravable charm area just like in the engraving detailed modal. ... it won't let me go into the engraving detail screen because it says it's not yet
// placed/nested on a sheet. ... a charm does not have to be placed on that sheet for it to be approved/updated by the user for the correct back engraving."
// The cause (plans/engrave-prenest-1013/CONTEXT.md): such a line has NO Engrave job (jobs come with the pool), so the card draws the unmirrored front art with the words in one wide line, and the press is refused.
//
// THE WORLD. heldDisc = a round disc necklace (a made-up drawing: a 12 mm disc with a hoop and a flower engraved on its FRONT: discAi() in the world helper), back only, three short back lines ("Name One / Name Two / Date One")
// and the font as a typed word in the note ("Typewriter font" = Crimson Text), ON HOLD (row state "pulled" with a person's hold on it), in no run, in no pool, on no sheet. discTwin = the same disc, words and note as a normal order
// that went through the real pool, a sheet and the real fit: the page's OWN drawing of it (its job's view / fit) is the yardstick the held card is measured against.
// Offline fake site only: nothing real is written, no Etsy, no paid call, made-up names ("Name One").
//
// WHAT IS MEASURED (on the screenshot's pixels, in the page, no knowledge of the code under test; every number is in the evidence):
//   disc      the closed dark outline of the picture (a flood fill from the picture's edge cannot get in): its box gives the centre and the inner radius; a picture with no closed outline is "no disc"
//   art       saturated pixels (red / blue: the front's engraved art, the way the Vector design and the front view draw it) and dark ink beyond what the twin's back has
//   words     dark neutral ink INSIDE the outline (not the ring, not the hoop)
//   outside   dark ink beyond the ring that is not the hoop above the disc: the words that run off the disc
//   CONTROLS  a red test is worth something only if the same measure goes green on a right picture and red on the wrong one: C1 = the TWIN's Back thumbnail (the page's own drawing) must pass the three picture checks; C2 = a drawn
//             copy of the live bug (the front view with one wide line of words across it) must be caught by them.
//
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/engrave-prenest.cjs      (CHROMIUM=<chrome> if the default path is missing; SHOTS=<dir> for the screenshots; PN_ONLY=card|open|edit|front)
//   Every line is  PASS|FAIL  id  evidence  [owner]. Exit code 1 while any check is red. Many checks are RED on a build that has not got the ENGPRE / ENGTHUMB fixes: that is the point.
// What a pass does NOT prove: a live order, the live font files, any browser but headless Chromium, a real sheet; the held order is a pulled line with a hold flag, as the live one is, but its Hold came from this world.
'use strict';
const fs = require('fs'), path = require('path');
const R = require('./_engraveRealWorld.cjs');
const pwDir = ['/opt/node22/lib/node_modules/playwright/node_modules', path.join(R.root, 'node_modules')].find(d => fs.existsSync(path.join(d, 'playwright-core')));
if (!process.env.PW_DIR && pwDir) process.env.PW_DIR = pwDir;
if (!process.env.CHROMIUM) { const c = ['/opt/pw-browsers/chromium-1194/chrome-linux/chrome', '/opt/pw-browsers/chromium'].find(x => fs.existsSync(x)); if (c) process.env.CHROMIUM = c; }
const SHOTS = process.env.SHOTS || '/mnt/project-files/plans/engrave-prenest-1013/images';
const ONLY = process.env.PN_ONLY || '';

const RESULTS = [];
const OWNERS = { thumb: 'ENGTHUMB', pre: 'ENGPRE', front: 'ENGFRONT', world: 'ENGLIVE' };
function check(id, ownerKey, ok, evidence) {
  const owner = OWNERS[ownerKey] || ownerKey;
  RESULTS.push({ id, owner, ok: !!ok, evidence });
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${evidence}  [${owner}]`);
}
const f2 = x => (typeof x === 'number' && isFinite(x) ? (Math.round(x * 100) / 100).toString() : String(x));
const sleep = ms => new Promise(r => setTimeout(r, ms));

/* ── the instrument that lives in the page ─────────────────────────────────────────────────────────────────────────────────────────────────────── */
// (no backticks in here: it is a template string)
const INSTRUMENT = String.raw`(() => {
  if (window.__pn) return;
  const blobOf = b64 => { const s = atob(b64), u = new Uint8Array(s.length); for (let i = 0; i < s.length; i++) u[i] = s.charCodeAt(i); return new Blob([u], { type: 'image/png' }); };
  async function decode(b64) {
    const bmp = await createImageBitmap(blobOf(b64)), c = document.createElement('canvas'); c.width = bmp.width; c.height = bmp.height;
    const x = c.getContext('2d', { willReadFrequently: true }); x.fillStyle = '#fff'; x.fillRect(0, 0, c.width, c.height); x.drawImage(bmp, 0, 0);
    return x.getImageData(0, 0, c.width, c.height);
  }
  const r2 = x => Math.round(x * 100) / 100;
  async function measure(b64) {
    const img = await decode(b64), W = img.width, H = img.height, px = img.data, N = W * H;
    const dark = new Uint8Array(N); let artPx = 0;
    for (let i = 0; i < N; i++) {
      const r = px[i * 4], g = px[i * 4 + 1], b = px[i * 4 + 2], lum = .299 * r + .587 * g + .114 * b, sat = Math.max(r, g, b) - Math.min(r, g, b);
      if (lum < 165 && sat < 60) dark[i] = 1; else if (sat >= 70 && lum < 245) artPx++;
    }
    // the barrier: the dark ink, one pixel fatter (a thin outline can have a pale pixel in it)
    const bar = new Uint8Array(N);
    for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) if (dark[y * W + x]) for (let dy = -1; dy <= 1; dy++) for (let dx = -1; dx <= 1; dx++) { const yy = y + dy, xx = x + dx; if (yy >= 0 && yy < H && xx >= 0 && xx < W) bar[yy * W + xx] = 1; }
    // what the picture's edge reaches without crossing ink is OUTSIDE; the biggest of the rest is the disc
    const lab = new Int32Array(N), q = new Int32Array(N);
    const flood = (seed, id) => { let h = 0, t = 0, area = 0, x0 = W, y0 = H, x1 = -1, y1 = -1; q[t++] = seed; lab[seed] = id;
      while (h < t) { const i = q[h++], x = i % W, y = (i - x) / W; area++; if (x < x0) x0 = x; if (x > x1) x1 = x; if (y < y0) y0 = y; if (y > y1) y1 = y;
        if (x > 0 && !bar[i - 1] && !lab[i - 1]) { lab[i - 1] = id; q[t++] = i - 1; } if (x < W - 1 && !bar[i + 1] && !lab[i + 1]) { lab[i + 1] = id; q[t++] = i + 1; }
        if (y > 0 && !bar[i - W] && !lab[i - W]) { lab[i - W] = id; q[t++] = i - W; } if (y < H - 1 && !bar[i + W] && !lab[i + W]) { lab[i + W] = id; q[t++] = i + W; } }
      return { area, box: [x0, y0, x1, y1] }; };
    for (let x = 0; x < W; x++) for (const y of [0, H - 1]) { const i = y * W + x; if (!bar[i] && !lab[i]) flood(i, -1); }
    for (let y = 0; y < H; y++) for (const x of [0, W - 1]) { const i = y * W + x; if (!bar[i] && !lab[i]) flood(i, -1); }
    let best = null, id = 0;
    for (let i = 0; i < N; i++) if (!bar[i] && !lab[i]) { const c = flood(i, ++id); if (!best || c.area > best.area) best = c; }
    const out = { W, H, artPx, found: false };
    if (!best) return out;
    const bw = best.box[2] - best.box[0] + 1, bh = best.box[3] - best.box[1] + 1;
    // the disc's width is its diameter (the hoop is narrower); the cut line runs over the hoop too, so the inside is a little taller than wide: the disc's centre is a radius up from its bottom (the hoop is up)
    const rIn = bw / 2, cx = (best.box[0] + best.box[2]) / 2, cy = best.box[3] - rIn;
    Object.assign(out, { discArea: r2(best.area / N), discH: r2(bh / bw), rIn: r2(rIn), cx: r2(cx), cy: r2(cy) });
    out.found = best.area >= 0.1 * N && bw >= 0.35 * W && bh / bw >= 0.9 && bh / bw <= 1.35;
    let inner = 0, ring = 0, hoop = 0, outside = 0, maxD = 0, tx0 = 1e9, ty0 = 1e9, tx1 = -1e9, ty1 = -1e9, ox0 = 1e9, oy0 = 1e9, ox1 = -1e9, oy1 = -1e9, outMaxD = 0;
    for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) if (dark[y * W + x]) {
      const dx = x - cx, dy = y - cy, d = Math.hypot(dx, dy);
      if (d < rIn - 2) { inner++; if (d > maxD) maxD = d; if (x < tx0) tx0 = x; if (x > tx1) tx1 = x; if (y < ty0) ty0 = y; if (y > ty1) ty1 = y; }
      else if (d <= rIn + Math.max(5, 0.06 * rIn)) ring++;                    // the outline's own stroke (it grows with the picture: ENGBIG drew the pictures twice as big)
      else if (Math.abs(dx) <= 0.32 * rIn + 3 && dy < -0.75 * rIn && dy > -1.6 * rIn) hoop++;      // the hoop above the disc
      else { outside++; if (d > outMaxD) outMaxD = d; if (x < ox0) ox0 = x; if (x > ox1) ox1 = x; if (y < oy0) oy0 = y; if (y > oy1) oy1 = y; }
    }
    Object.assign(out, { inner, ring, hoop, outside, inkIn: r2(inner / (rIn * rIn)), textR: inner ? r2(maxD / rIn) : 0, outsideMaxR: outside ? r2(outMaxD / rIn) : 0,
      textBox: inner ? [r2((tx0 - cx) / rIn), r2((ty0 - cy) / rIn), r2((tx1 - cx) / rIn), r2((ty1 - cy) / rIn)] : null, outBox: outside ? [r2((ox0 - cx) / rIn), r2((oy0 - cy) / rIn), r2((ox1 - cx) / rIn), r2((oy1 - cy) / rIn)] : null });
    return out;
  }
  // the live bug, drawn: the FRONT view of the disc (art and all) with ONE wide line of words across it, in a box of w x h px
  async function bug(w, h) {
    const e = Master.entryFor('BFLOWER_9901'), src = await Pool.masterCharm(e, null), ch = src.charms[0];
    const base = Engrave.renderFront(ch, Math.max(w, h), {});
    const c = document.createElement('canvas'); c.width = w; c.height = h; const x = c.getContext('2d'); x.fillStyle = '#fff'; x.fillRect(0, 0, w, h);
    const s = Math.min(w / base.width, h / base.height) * 0.9; x.drawImage(base, (w - base.width * s) / 2, (h - base.height * s) / 2, base.width * s, base.height * s);
    x.fillStyle = '#111'; x.textBaseline = 'middle'; x.textAlign = 'center'; let fs = 20; x.font = fs + 'px serif'; const line = 'Name One Name Two Date One', want = w * 0.95, got = x.measureText(line).width; fs = Math.max(8, Math.floor(fs * want / got)); x.font = fs + 'px serif'; x.fillText(line, w / 2, h * 0.58);
    return c.toDataURL('image/png').split(',')[1];
  }
  // the Engrave tab as a person sees it, every 100 ms, while a press is being answered
  const frames = []; let timer = 0;
  const vis = e => { const b = e.getBoundingClientRect(); return b.width > 0 && b.height > 0 && !e.closest('[hidden]'); };
  const frame = () => {
    const q = document.getElementById('egQueue'), list = q && q.querySelector('.egPlacementList');
    frames.push({ t: Math.round(performance.now()), mode: CN.S.mode, owOpen: !!(OrderWin.isOpen && OrderWin.isOpen()), card: !!(q && q.querySelector('.rvItem[data-kind=placement]')), panel: !!(q && q.querySelector('.rvItem[data-kind=placement] .engp')),
      rows: list ? list.querySelectorAll(':scope > .placementRow').length : 0, none: !!(q && q.querySelector('.egNone')), text: q ? q.innerText.replace(/\s+/g, ' ').trim().length : 0, jobs: Engrave.items().size,
      note: [...document.querySelectorAll('.elWait, .toast, [role=status], [role=alert]')].filter(vis).map(n => n.textContent.replace(/\s+/g, ' ').trim()).filter(Boolean).join(' | ').slice(0, 200) });
  };
  window.__pn = { measure, bug,
    watch() { frames.length = 0; clearInterval(timer); frame(); timer = setInterval(frame, 100); return true; },
    unwatch() { clearInterval(timer); frame(); return frames.slice(); } };
})()`;

const measure = (page, b64) => page.evaluate(b => window.__pn.measure(b), b64);
const shotOf = async (page, sel) => { const el = await page.$(sel); if (!el) return null; try { return (await el.screenshot()).toString('base64'); } catch (_) { return null; } };
const lineOf = m => m ? `disc ${m.found ? 'found' : 'NOT found'} (area ${f2(m.discArea)}, height/width ${f2(m.discH)}, r ${f2(m.rIn)}px), art ${m.artPx}px, words ${m.inner}px (ink ${f2(m.inkIn)}/r², reach ${f2(m.textR)} of r), ring ${m.ring}px, hoop ${m.hoop}px, outside the outline ${m.outside}px${m.outside ? ` (reach ${f2(m.outsideMaxR)} of r, box ${JSON.stringify(m.outBox)})` : ''}` : 'no picture';

/* the three judgements on a Back picture, against the twin's (the page's own drawing of the same disc and words) */
const J = {
  back: (m, t) => !!m && m.found && m.artPx <= Math.max(8, (t ? t.artPx : 0) + 8) && m.inkIn <= (t ? Math.max(t.inkIn * 1.7, t.inkIn + 0.4) : 2.5) && m.hoop >= 12,                  // the back view: the disc and its hoop, no front art (no saturated art, no extra ink)
  words: (m, t) => !!m && m.found && m.inner >= 20 && m.inkIn >= (t ? t.inkIn * 0.5 : 0.3),                                                                                    // the words are drawn (not a dashed place, not a blank)
  inside: (m, t) => !!m && m.found && m.inner >= 20 && m.outside <= 2 && m.textR <= 0.985                                                                                         // every word inside the outline (a hair of anti-aliasing allowed)
};

/* ── the world ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
async function main() {
  const w = await R.world({ viewport: { width: 1440, height: 900 }, shots: SHOTS, scale: 2 });
  if (!w) { console.log('engrave-prenest: playwright-core not found, skipped'); return 0; }
  const page = w.page; let code = 0;
  const T = { rid: R.ORDERS.discTwin.rid, key: null }, H = { rid: R.ORDERS.heldDisc.rid, key: null };
  const state = (rid) => page.evaluate(rid => {
    const rows = Orders.rows().filter(r => r.order.receiptId === rid), r = rows[0]; if (!r) return null;
    const pp = window.PiecePlacement && PiecePlacement.ofRow(r), pool = [...B.pool.rows.values()].filter(p => String(p.id || p.poolId || '').startsWith(rid) || (r.poolIds || []).includes(p.id));
    let onSheet = 0; try { for (const sh of allSheets()) for (const c of (sh.charms || [])) if (String(c.id || '').includes(rid) || String(c.poolId || '').includes(rid)) onSheet++; } catch (e) { onSheet = -1; }
    const jobs = Engrave.jobsOf(r);
    return { state: r.state, hold: r.hold || null, poolIds: (r.poolIds || []).slice(), poolRows: pool.length, onSheet, place: pp && pp.state, placeOn: pp && !!pp.onSheet, jobs: jobs.map(j => ({ key: j.key, state: j.state, face: j.face === 'front' ? 'front' : 'back', text: j.text, by: j.approvedBy || null, at: j.approvedAt || null, hasFit: !!j.fit, hasView: !!j.view, pend: j.backPending || null })), engrave: r.engrave ? { state: r.engrave.state, approved: !!r.engrave.approved, text: r.engrave.text, seals: (r.engrave.seals || []).length } : null, override: r.engravingOverride ? { reason: r.engravingOverride.reason && r.engravingOverride.reason.code, by: r.engravingOverride.by, text: r.engravingOverride.back && r.engravingOverride.back.text } : null, poolSize: B.pool.rows.size };
  }, rid);
  try {
    // ── 0 · the world ──
    const made = await R.build(w, ['discTwin', 'heldDisc']);
    const tw = made.discTwin, hd = made.heldDisc; T.key = tw.key; H.key = hd.key;
    await page.evaluate(INSTRUMENT);
    const info = await page.evaluate(async () => {
      const e = Master.entryFor('BFLOWER_9901'); if (!e) return { err: 'no master entry' };
      const src = await Pool.masterCharm(e, null), c = src.charms[0], b = c.bbox;
      const cut = c.members.filter(m => m.closed && CharmNestGeom.pathRole(m) === 'cut'), art = c.members.filter(m => CharmNestGeom.pathRole(m) === 'artwork');
      return { w: +(b[2] - b[0]).toFixed(1), h: +(b[3] - b[1]).toFixed(1), cut: cut.length, art: art.length, font: CharmNestOrders.fontOfPhrase ? CharmNestOrders.fontOfPhrase('Typewriter') : null };
    });
    const s0 = { t: await state(T.rid), h: await state(H.rid) };
    const tj = tw.jobs[0];
    check('W0 the world: a round disc with a hoop and front art; the twin has a real fitted job; the held order is on hold, in no pool, on no sheet, with no job',
      'world', !info.err && Math.abs(info.w - 34) < 1 && info.h > info.w && info.cut >= 2 && info.art >= 5 && info.font === 'crimson-text'
        && tw.jobs.length === 1 && tj.fitted && tj.hasMask && tj.hasView && tj.fontKey === 'crimson-text' && tj.text === 'Name One\nName Two\nDate One' && tw.mode === 'back'
        && s0.h && s0.h.state === 'pulled' && !!s0.h.hold && s0.h.poolIds.length === 0 && s0.h.poolRows === 0 && s0.h.onSheet === 0 && s0.h.place === 'hold' && hd.mode === 'back' && hd.jobs.length === 0,
      `disc ${info.w}x${info.h} pt, ${info.cut} cut paths, ${info.art} art paths, "Typewriter" -> ${info.font}; twin job ${tj ? tj.state + ' ' + tj.fontKey + (tj.fitted ? ' fitted' : ' NOT fitted') + (tj.reason ? ' (' + tj.reason + ')' : '') + (tj.questions && tj.questions.length ? ' asks ' + JSON.stringify(tj.questions) : '') : 'none'} ${JSON.stringify(tj && tj.text)}; held row ${JSON.stringify(s0.h && { state: s0.h.state, hold: !!s0.h.hold, pool: s0.h.poolRows, sheet: s0.h.onSheet, place: s0.h.place, jobs: s0.h.jobs.length })}, mode ${hd.mode}${info.err ? ' ' + info.err : ''}`);

    const thumbSel = (face) => `#owEng .engCard .engFace[data-face="${face}"] .engThumb`;
    // a Back picture, waited for (a font being fetched hides the words for a moment: the picture is taken again, at most 4 times, until it has words)
    const backPic = async (name) => {
      await R.openOrderWindow(w, name); let m = null, b64 = null;
      for (let k = 0; k < 4; k++) {
        await page.waitForFunction(() => !document.querySelector('#owEng .engThumb[aria-busy="true"]'), null, { timeout: 8000 }).catch(() => {});
        b64 = await shotOf(page, thumbSel('back')); m = b64 ? await measure(page, b64) : null;
        if (m && m.found && m.inner >= 20) break; await R.settle(page, 1800);
      }
      return { m, b64 };
    };

    if (!ONLY || ONLY === 'card') {
      // ── 1 · the pictures: the twin's (the control), a drawn copy of the live bug (the control), then the held order's ──
      const twinPic = await backPic('discTwin'); await R.shot(w, 'ENGLIVE-prenest-twin-card');
      const mt = twinPic.m;
      check('C1 control: the twin\'s Back thumbnail (the page\'s own drawing of this disc, with a job) is the back view, with its words, inside the outline', 'world', J.back(mt, null) && J.words(mt, null) && J.inside(mt, null), lineOf(mt));
      const bugB64 = await page.evaluate(({ w, h }) => window.__pn.bug(w, h), { w: 208, h: 208 }), mb = await measure(page, bugB64);
      check('C2 control: a drawn copy of the live bug (the front view with ONE wide line of words across it) is caught by the same judgements (front art seen, words outside seen)', 'world', !J.back(mb, mt) && !J.inside(mb, mt) && mb.found, lineOf(mb));

      const heldPic = await backPic('heldDisc'); await R.shot(w, 'ENGLIVE-prenest-held-card'); const mh = heldPic.m;
      const cardInfo = await page.evaluate(() => { const cs = [...document.querySelectorAll('#owEng > .engCard')]; return cs.map(c => ({ faces: [...c.querySelectorAll('.engFace')].map(f => ({ face: f.dataset.face, empty: f.dataset.empty || null, dashed: !!f.querySelector('.engEmpty, [data-empty="1"], .engSlot'), thumb: !!f.querySelector('.engThumb'), text: f.textContent.replace(/\s+/g, ' ').trim().slice(0, 30) })), noJob: c.dataset.noJob || null, state: c.dataset.state })); });
      const heldInfo = cardInfo[0] || { faces: [] };
      check('PN1 held disc, Back box: the back view of the disc (outline and hoop), NO front art', 'thumb', J.back(mh, mt), `${lineOf(mh)}; the twin's own: art ${mt && mt.artPx}px, ink ${mt && f2(mt.inkIn)}/r²`);
      check('PN2 held disc, Back box: the words are drawn on the disc (not a dashed place, not blank)', 'thumb', J.words(mh, mt), `${lineOf(mh)}; the twin has ${mt && mt.inner}px of words (ink ${mt && f2(mt.inkIn)}/r²)`);
      check('PN3 held disc, Back box: the words lie inside the outline (the engravable area), none outside the disc', 'thumb', J.inside(mh, mt), lineOf(mh));
      if (mh && mt && mh.found && mh.inner >= 20 && mt.textBox) console.log(`  info  where the words sit (box as fractions of the disc radius, centre 0,0): held ${JSON.stringify(mh.textBox)}, the twin's ${JSON.stringify(mt.textBox)}`);
      const hf = heldInfo.faces;
      check('PN4 held disc: a back-only order draws NO Front box (no front thumbnail, no empty or dashed slot)', 'thumb', hf.length === 1 && hf[0].face === 'back' && !hf.some(f => f.face === 'front'), `faces drawn: ${JSON.stringify(hf)}, ${cardInfo.length} card(s)`);
    }

    if (!ONLY || ONLY === 'open') {
      // ── 2 · opening the Engrave detail from the order window: the button, then the Back thumbnail ──
      const press = async (how) => {
        await page.evaluate(() => { try { OrderWin.close(); } catch (_) {} });
        await R.openOrderWindow(w, 'heldDisc');
        const sel = how === 'button' ? '#owEng .engCard .egOpen' : thumbSel('back');
        const pre = await page.evaluate(sel => { const b = document.querySelector(sel); return b ? { dis: b.getAttribute('aria-disabled'), title: b.title || '', text: b.textContent.replace(/\s+/g, ' ').trim(), nDis: b.disabled } : null; }, sel);
        await page.evaluate(() => window.__pn.watch());
        const t0 = Date.now();
        await page.click(sel, { force: true, timeout: 5000 }).catch(() => {});
        let opened = false;
        while (Date.now() - t0 < 25000) { opened = await page.evaluate(() => CN.S.mode === 'engrave' && !!document.querySelector('#egQueue .rvItem[data-kind=placement] .engp')); if (opened) break; await sleep(150); }
        await R.settle(page, 900);
        const frames = await page.evaluate(() => window.__pn.unwatch());
        const post = await page.evaluate(() => ({ mode: CN.S.mode, view: (() => { const v = Engrave.view(); return { tab: v.tab, focus: v.focus || null, list: v.list, q: v.q || '' }; })(), owOpen: !!OrderWin.isOpen(), words: ((document.querySelector('#egQueue .rvItem[data-kind=placement] .engpWords') || {}).value) || null }));
        return { how, pre, opened, took: Date.now() - t0, frames, post };
      };
      const blank = f => f.mode === 'engrave' && !f.card && f.rows === 0 && (f.none || f.text < 3);
      const said = fr => [...new Set(fr.map(f => f.note).filter(n => /not (yet )?(placed|nested|on a sheet)|on hold and not|no engraving to open/i.test(n)))];
      const results = {};
      for (const how of ['button', 'thumb']) {
        const r = results[how] = await press(how);
        const heldNow = await state(H.rid); r.jobs = heldNow.jobs;
        const key0 = (r.jobs[0] || {}).key;
        if (how === 'button') {
          check('PN5 held disc: the "Fix in Engraving" button is enabled (not faded, no "on hold / not on a sheet yet" reason)', 'pre', !!r.pre && r.pre.dis !== 'true' && !r.pre.nDis && !/on hold|not on a sheet|not yet/i.test(r.pre.title), `aria-disabled ${r.pre && r.pre.dis}, title "${r.pre && r.pre.title}", text "${r.pre && r.pre.text}"`);
        }
        check(`PN6${how === 'button' ? 'a' : 'b'} held disc: pressing the ${how === 'button' ? '"Fix in Engraving" button' : 'Back thumbnail'} OPENS the Engrave detail of this order's back (panel, words, no refusal)`, 'pre',
          r.opened && r.post.mode === 'engrave' && !!r.post.view.focus && String(r.post.view.focus).startsWith(H.rid) && !r.post.owOpen && !said(r.frames).length && !!r.post.words && r.post.words.includes('Name Two'),
          `${r.opened ? 'opened' : 'NOT opened'} after ${r.took} ms; mode ${r.post.mode}, focus ${r.post.view.focus}, order window ${r.post.owOpen ? 'still open' : 'closed'}, words ${JSON.stringify(r.post.words)}, said ${JSON.stringify(said(r.frames)) || 'nothing'}; the order's jobs now: ${JSON.stringify(r.jobs.map(j => j.key + ':' + j.state))}`);
        r.blank = r.frames.filter(blank).length; r.engFrames = r.frames.filter(f => f.mode === 'engrave').length;
        void key0;
      }
      const eng = results.button.engFrames + results.thumb.engFrames, bl = results.button.blank + results.thumb.blank;
      // the direct link: EngraveLink.open({ rid, key }) (the way a Timeline chip or the Sheet window gets there)
      await page.evaluate(() => { try { OrderWin.close(); } catch (_) {} CN.setMode('orders'); });
      await page.evaluate(() => window.__pn.watch());
      const lk = await page.evaluate(async ({ rid, key }) => { try { return await EngraveLink.open({ rid, key }); } catch (e) { return 'threw ' + e.message; } }, { rid: H.rid, key: H.key });
      await R.settle(page, 900); const fr3 = await page.evaluate(() => window.__pn.unwatch());
      const direct = await page.evaluate(() => ({ mode: CN.S.mode, card: !!document.querySelector('#egQueue .rvItem[data-kind=placement] .engp'), focus: Engrave.view().focus || null }));
      const eng3 = fr3.filter(f => f.mode === 'engrave').length, bl3 = fr3.filter(blank).length;
      check('PN7 held disc: EngraveLink.open({ rid, key }) (a link from anywhere) opens the same detail', 'pre', lk === true && direct.mode === 'engrave' && direct.card && String(direct.focus || '').startsWith(H.rid) && !said(fr3).length, `open() answered ${JSON.stringify(lk)}; mode ${direct.mode}, panel ${direct.card}, focus ${direct.focus}, said ${JSON.stringify(said(fr3)) || 'nothing'}`);
      // the Engrave tab on the held order's number: a filtered list must show the piece (never an empty or blank list)
      await page.evaluate(({ rid }) => { try { OrderWin.close(); } catch (_) {} Engrave.restoreView({ tab: 'place', focus: null, list: true, chosen: true, q: rid, openDone: null }); CN.setMode('engrave'); Engrave.render(); }, { rid: H.rid });
      await R.settle(page, 1200);
      const filt = await page.evaluate(() => { const q = document.getElementById('egQueue'), list = q && q.querySelector('.egPlacementList'); return { rows: list ? list.querySelectorAll(':scope > .placementRow').length : 0, none: !!(q && q.querySelector('.egNone')), text: q ? q.innerText.replace(/\s+/g, ' ').trim().slice(0, 120) : '', box: (document.getElementById('egQ') || {}).value || '' }; });
      check('PN8 the Engrave tab never goes blank: not while a press is answered (frames every 100 ms), and a list filtered to the held order\'s number shows its piece', 'pre',
        eng + eng3 >= 3 && bl + bl3 === 0 && filt.rows >= 1 && !filt.none,
        `frames in the Engrave tab: ${results.button.engFrames} (button), ${results.thumb.engFrames} (thumbnail), ${eng3} (link); blank frames ${bl + bl3}${eng + eng3 < 3 ? ' (the presses never took the person to the Engrave tab)' : ''}; the list filtered to ${H.rid}: ${filt.rows} row(s)${filt.none ? ', the empty message' : ''}, text "${filt.text}"`);
      await R.shot(w, 'ENGLIVE-prenest-filtered-list');
    }

    if (!ONLY || ONLY === 'edit') {
      // ── 3 · the detail of the held order: the same job, the panel, a staged edit and Approve, with no sheet (offline fake only) ──
      const before = await state(H.rid), docs0 = new Map([...w.srv.st.docs].map(([k, v]) => [k, JSON.stringify(v).length]));
      const rows = () => [...w.srv.st.docs.keys()].filter(k => !docs0.has(k) && k.includes(H.rid));
      const opened = await page.evaluate(async ({ rid, key }) => { try { const ok = await EngraveLink.open({ rid, key }); return ok === true && !!document.querySelector('#egQueue .rvItem[data-kind=placement] .engp'); } catch (e) { return false; } }, { rid: H.rid, key: H.key });
      await R.settle(page, 900);
      // the job's fit runs in the background (a small labelled wait is fine): the detail is judged once it has settled (the page's own Retry for a fit that outran its limit on a loaded machine, at most 4 times)
      let fitted = null;
      if (opened) for (let i = 0; i < 160; i++) { fitted = await page.evaluate(rid => { const r = Orders.rows().find(x => x.order.receiptId === rid), j = r && Engrave.jobsOf(r)[0]; if (!j) return null; if (j.state === 'blocked' && /took too long/.test(j.reason || '') && (j.__r = (j.__r || 0) + 1) <= 4) Engrave.fitJob(j); return { state: j.state, fit: !!j.fit, view: !!j.view }; }, H.rid); if (fitted && fitted.state === 'review' && fitted.fit && fitted.view) break; await R.settle(page, 1500); }
      await R.settle(page, 900);
      const det = opened ? await page.evaluate(() => {
        const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'), cv = c && c.querySelector('.backHost canvas'), fontEl = c && c.querySelector('.engpFont');
        let ink = null; try { const x = cv && cv.getContext && cv.getContext('2d'); if (x) { const d = x.getImageData(0, 0, cv.width, cv.height).data; let n = 0; for (let i = 0; i < d.length; i += 4) if (d[i + 3] > 40 && (d[i] < 200 || d[i + 1] < 200 || d[i + 2] < 200)) n++; ink = n; } } catch (_) { ink = null; }
        const r = cv && cv.getBoundingClientRect();
        return { words: c && c.querySelector('.engpWords') && c.querySelector('.engpWords').value, font: fontEl ? String(fontEl.value || '').trim() : null, canvas: !!cv && cv.width > 0 && r.width >= 150 && r.height >= 150 && (ink === null || ink > 200), ink, size: r && [Math.round(r.width), Math.round(r.height)], waitText: c ? (c.querySelector('.backHost') || c).innerText.replace(/\s+/g, ' ').slice(0, 80) : '', front: !!(c && c.querySelector('.engFace[data-face=front]')), approve: !!(c && c.querySelector('[data-a=approve]')), focus: Engrave.view().focus || null };
      }) : null;
      const stD = await state(H.rid), jb = stD.jobs[0] || {};
      check('PN9 the Engrave detail of the held order: its own job under the key the line will have when it is pooled, the three lines in the box, the work area drawn, no Front box', 'pre',
        !!det && det.focus === H.key && jb.key === H.key && jb.face === 'back' && jb.hasFit && jb.hasView && det.words === 'Name One\nName Two\nDate One' && det.canvas && !det.front,
        det ? `focus ${det.focus}, job ${jb.key} ${jb.state} (fit ${jb.hasFit}, view ${jb.hasView}), the line's key is ${H.key}, words ${JSON.stringify(det.words)}, font box "${det.font}", work area ${det.canvas ? 'drawn' : 'MISSING'} (${JSON.stringify(det.size)}, ${det.ink === null ? 'ink not readable' : det.ink + ' inked px'}${det.canvas ? '' : ', says "' + det.waitText + '"'}), job settled ${JSON.stringify(fitted)}, front face ${det.front ? 'DRAWN' : 'not drawn'}` : 'the detail did not open, so there is nothing to look at');
      // a staged edit: other words, Save, the reason prompt (ENGWHY) answered with its first quick button
      let ed = null;
      if (det && det.words) {
        await page.evaluate(() => { const q = s => document.querySelector('#egQueue .rvItem[data-kind=placement] ' + s), t = q('.engpWords'), set = Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value').set; t.focus(); set.call(t, 'Name One\nName Two\nDate Two'); t.dispatchEvent(new InputEvent('input', { bubbles: true, inputType: 'insertText', data: 'x' })); t.dispatchEvent(new Event('keyup', { bubbles: true })); });
        await R.settle(page, 700);
        const staged = await page.evaluate(() => { const b = document.querySelector('#egQueue .rvItem[data-kind=placement] [data-engp=save]'); return { save: !!b && !b.disabled, approveOff: !!document.querySelector('#egQueue .rvItem[data-kind=placement] [data-a=approve]:disabled') }; });
        await page.evaluate(() => { const b = document.querySelector('#egQueue .rvItem[data-kind=placement] [data-engp=save]'); if (b) b.click(); });
        await R.settle(page, 600);
        const asked = await page.evaluate(() => !!document.querySelector('#egQueue .rvItem[data-kind=placement] .engpWhy [data-engp=why]'));
        await page.evaluate(() => { const g = document.querySelector('#egQueue .rvItem[data-kind=placement] .engpWhy'), b = g && g.querySelector('[data-engp=why]'); if (b) b.click(); });
        await R.settle(page, 1200);
        const after = await state(H.rid);
        ed = { staged, asked, after, docs: rows() };
      }
      check('PN10 a staged edit on the held order is recorded without a sheet (Save, the reason asked and answered: the override and a timeline record are kept) and the order is still held', 'pre',
        !!ed && ed.staged.save && ed.asked && !!ed.after.override && ed.after.override.text === 'Name One\nName Two\nDate Two' && !!ed.after.override.reason && ed.docs.some(k => /engraveChanged/.test(k)) && ed.after.state === 'pulled' && !!ed.after.hold && ed.after.poolRows === 0 && ed.after.onSheet === 0,
        ed ? `Save enabled ${ed.staged.save}, reason asked ${ed.asked}, override ${JSON.stringify(ed.after.override)}, new records for this order in the fake site: ${JSON.stringify(ed.docs.map(k => k.split('/')[0] + '/…' + k.slice(-24)))}; row ${ed.after.state}, hold ${!!ed.after.hold}, pool rows ${ed.after.poolRows}, on a sheet ${ed.after.onSheet}` : 'no staged edit could be made: the detail did not open');
      // the changed words are fitted again (under a heavy machine the fit worker can outrun its own limit: the page's own Retry is pressed, at most 4 times)
      let apr = null;
      if (ed) {
        let st = null;
        for (let i = 0; i < 120; i++) { st = await page.evaluate(rid => { const r = Orders.rows().find(x => x.order.receiptId === rid), j = r && Engrave.jobsOf(r)[0]; if (!j) return null; if (j.state === 'blocked' && /took too long/.test(j.reason || '') && (j.__r = (j.__r || 0) + 1) <= 4) Engrave.fitJob(j); return { state: j.state, text: j.text }; }, H.rid); if (st && st.state === 'review') break; await R.settle(page, 1500); }
        const can = await page.evaluate(() => { const b = document.querySelector('#egQueue .rvItem[data-kind=placement] [data-a=approve]'); return !!b && !b.disabled; });
        if (can) { await page.evaluate(() => document.querySelector('#egQueue .rvItem[data-kind=placement] [data-a=approve]').click()); for (let i = 0; i < 90; i++) { await R.settle(page, 1000); const s = await state(H.rid); if (s.jobs[0] && /approved|written/.test(s.jobs[0].state)) break; } }
        const fin = await state(H.rid);
        apr = { refit: st, can, fin, docs: rows() };
        await R.shot(w, 'ENGLIVE-prenest-held-approved');
      }
      const fj = apr && apr.fin.jobs[0];
      check('PN11 Approve on the held order is recorded without a sheet (approved by someone, when, with its seal and a timeline record), with the words as edited', 'pre',
        !!apr && apr.can && !!fj && /approved|written/.test(fj.state) && !!fj.by && !!fj.at && apr.fin.engrave && apr.fin.engrave.approved && apr.fin.engrave.seals >= 1 && fj.text === 'Name One\nName Two\nDate Two' && apr.docs.some(k => /engraveApproved/.test(k)),
        apr ? `re-fit ${apr.refit && apr.refit.state}, Approve ${apr.can ? 'enabled' : 'NOT enabled'}; job ${fj ? fj.state + ' by ' + fj.by + ' ' + JSON.stringify(fj.text) + (fj.pend ? ', back file waits: ' + fj.pend : '') : 'none'}; row.engrave ${JSON.stringify(apr.fin.engrave)}; records for this order in the fake site: ${JSON.stringify(apr.docs.map(k => k.slice(k.indexOf('/') + 1).replace(/^\d+~/, '').slice(0, 40)))}` : 'there was no edit to approve: the detail did not open');
      // the held order is still held after all of it
      const end = await state(H.rid), tEnd = await state(T.rid);
      check('PN12 the held order is still held after the presses, the edit and the approval: on hold, in no pool, no run, no sheet, nothing nested, and nothing else was pooled for it', 'pre',
        !!end && end.state === 'pulled' && !!end.hold && end.poolIds.length === 0 && end.poolRows === 0 && end.onSheet === 0 && end.place === 'hold' && !end.placeOn && end.poolSize === s0.t.poolSize && tEnd.poolSize === s0.t.poolSize,
        end ? `state ${end.state}, hold ${JSON.stringify(end.hold)}, poolIds ${end.poolIds.length}, pool rows ${end.poolRows}, on a sheet ${end.onSheet}, placement ${end.place} (on a sheet: ${end.placeOn}); the pool holds ${end.poolSize} row(s) (${s0.t.poolSize} before: the twin's)` : 'the row is gone');
      void before;
    }
    if (!ONLY || ONLY === 'front') {
      // ── 4 · ENGFRONT's case (Paul, 13 Oct 02:20): a FRONT engraving added to a charm that has a fitted back, then the Front thumbnail pressed: the work area must show the front charm with the words (never the
      //        "Processing stopped before these words were read." text of a job that was simply never started, never a blank), and Back again must still show the back. On the twin (the same disc, a real job and sheet). ──
      const P = page, QS = '#egQueue .rvItem[data-kind=placement]';
      const look = () => P.evaluate(rid => {
        const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); if (!c) return { card: false };
        const row = Orders.rows().find(r => r.order.receiptId === rid), jobs = row ? Engrave.jobsOf(row).map(j => ({ face: j.face === 'front' ? 'front' : 'back', state: j.state, fit: !!j.fit, view: !!j.view })) : [];
        const cv = c.querySelector('.backHost canvas'); let ink = null; try { const x = cv && cv.getContext && cv.getContext('2d'); if (x) { const d = x.getImageData(0, 0, cv.width, cv.height).data; let n = 0; for (let i = 0; i < d.length; i += 4) if (d[i + 3] > 40 && (d[i] < 200 || d[i + 1] < 200 || d[i + 2] < 200)) n++; ink = n; } } catch (_) { ink = null; }
        const r = cv && cv.getBoundingClientRect();
        return { card: true, cur: [...c.querySelectorAll('.engFace')].filter(f => f.getAttribute('aria-current')).map(f => f.dataset.face)[0] || null, words: (c.querySelector('.engpWords') || {}).value || '', jobs, canvas: !!cv && r.width >= 150 && r.height >= 150 && (ink === null || ink > 200), ink,
          stopped: /Processing stopped/i.test((c.querySelector('.backHost') || c).innerText), work: (c.querySelector('.backHost') || c).innerText.replace(/\s+/g, ' ').slice(0, 90) };
      }, T.rid);
      const click = sel => P.evaluate(sel => { const b = document.querySelector(sel); if (b) b.click(); return !!b; }, sel);
      // the earlier checks left the Engrave list filtered to the HELD order's number (PN8) and its detail open: this case starts from a clean Engrave view (no filter, nothing focused) so that it judges the twin, not what was left behind
      await P.evaluate(() => { try { OrderWin.close(); } catch (_) {} Engrave.restoreView({ tab: 'place', focus: null, list: true, chosen: true, q: '', openDone: null }); CN.setMode('engrave'); Engrave.render(); }); await R.settle(P, 900);
      await R.openEngraveDetail(w, 'discTwin', { face: 'back' }); await R.settle(P, 900);
      const f0 = await look();
      await click(QS + ' .engpSeg[data-mode="both"]'); await R.settle(P, 900);
      await click(QS + ' .engFace[data-face=front] .engThumb'); await R.settle(P, 900);
      await P.evaluate(() => { const t = document.querySelector('#egQueue .rvItem[data-kind=placement] .engpWords'); if (!t) return; const set = Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value').set; t.focus(); set.call(t, 'B'); t.dispatchEvent(new InputEvent('input', { bubbles: true, inputType: 'insertText', data: 'B' })); t.dispatchEvent(new Event('keyup', { bubbles: true })); });
      await R.settle(P, 700); await click(QS + ' [data-engp=save]'); await R.settle(P, 900);
      await P.evaluate(() => { const g = document.querySelector('#egQueue .rvItem[data-kind=placement] .engpWhy'), b = g && g.querySelector('[data-engp=why]'); if (b) b.click(); }); await R.settle(P, 1500);
      // the front job is made now; it is given time to be read and fitted (the page's own Retry for a fit that outran its limit on a loaded machine)
      let jobs = [];
      for (let i = 0; i < 120; i++) {
        jobs = await P.evaluate(rid => { const row = Orders.rows().find(r => r.order.receiptId === rid); return row ? Engrave.jobsOf(row).map(j => { if (j.state === 'blocked' && /took too long/.test(j.reason || '') && (j.__r = (j.__r || 0) + 1) <= 4) Engrave.fitJob(j); return { face: j.face === 'front' ? 'front' : 'back', state: j.state, fit: !!j.fit, view: !!j.view }; }) : []; }, T.rid);
        const fr = jobs.find(j => j.face === 'front'); if (fr && fr.fit && fr.view && jobs.length >= 2) break; await R.settle(P, 1500);
      }
      await click(QS + ' .engFace[data-face=front] .engThumb'); await R.settle(P, 1500);
      const fF = await look(); await R.shot(w, 'ENGLIVE-prenest-front-pressed');
      await click(QS + ' .engFace[data-face=back] .engThumb'); await R.settle(P, 1500);
      const fB = await look();
      const fj = fF.jobs && fF.jobs.find(j => j.face === 'front');
      check('PN13 a front engraving added LATER to a charm with a fitted back: pressing the Front thumbnail shows the front charm with its words (no "Processing stopped" text, no blank work area), and Back again still shows the back', 'front',
        f0.card && !!fj && fj.fit && fj.view && fF.cur === 'front' && fF.words === 'B' && fF.canvas && !fF.stopped && fB.cur === 'back' && fB.canvas && !fB.stopped && /Name One/.test(fB.words),
        `before: back job fitted ${JSON.stringify(f0.jobs)}; after adding Front + back and the words "B": jobs ${JSON.stringify(fF.jobs)}; Front pressed: ${fF.card ? `current ${fF.cur}, words ${JSON.stringify(fF.words)}, work area ${fF.canvas ? 'drawn (' + fF.ink + ' inked px)' : 'NOT drawn'}${fF.stopped ? ', says "' + fF.work + '"' : ''}` : 'no card'}; Back pressed again: ${fB.card ? `current ${fB.cur}, words ${JSON.stringify((fB.words || '').slice(0, 30))}, work area ${fB.canvas ? 'drawn (' + fB.ink + ' inked px)' : 'NOT drawn'}${fB.stopped ? ', says "' + fB.work + '"' : ''}` : 'no card'}`);
    }
    check('Z0 no page errors', 'world', !w.errors.length, w.errors.slice(0, 3).join(' | ').slice(0, 300) || 'none');
  } catch (e) { console.error(e); code = 2; }
  finally { await w.close(); }
  return code;
}

main().then(code => {
  const red = RESULTS.filter(r => !r.ok), by = {};
  for (const r of red) by[r.owner] = (by[r.owner] || []).concat(r.id);
  console.log(`\nengrave-prenest: ${RESULTS.length - red.length} PASS, ${red.length} FAIL`);
  for (const [o, ids] of Object.entries(by)) console.log(`  red for ${o}: ${ids.join(' ')}`);
  if (process.env.PN_JSON) fs.writeFileSync(process.env.PN_JSON, JSON.stringify(RESULTS, null, 1));
  process.exit(code || (red.length ? 1 : 0));
}).catch(e => { console.error(e); process.exit(2); });
