// ENGLIVE (engrave-ui-1012 round 2): the REAL-TIME test, on REAL drawings (tests/charm-nest/_engraveRealWorld.cjs).
//
// Paul, 12 Oct 2026, on the live round-1 UI: "nothing seems to happen in real-time", "the text in the work area does not show the preview of the selected text font style",
// the two boxes "are not working" (no charm, only plain text), "the front is gonna have to show all of the engraving/cutouts for each given charm", the order window "does not show the
// charm + engraving preview", "if there is only back engraving then it should show that ... intelligent and dynamically adapted in both", "the customer note ... put that back",
// "if a user tries to override the auto selection ... a quick prompt asking for a reason".
// Round 1 passed all its offline tests on a fake site with no drawings, fonts or jobs. This test measures what is on the screen, with real charm drawings, a real font and a real fit.
//
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/engrave-realtime.cjs        (CHROMIUM=/opt/pw-browsers/chromium if the default path is missing; SHOTS=<dir> for screenshots)
//   RT_ONLY=detail|faces|order|said|reason  runs one part.   Exit code 1 while any check is red; every line is  PASS|FAIL  id  evidence  [owner].  Many checks are RED until the round-2 workers land: that is the point.
//
// HOW "THE SAME TICK" IS MEASURED. Before each change the page's setTimeout / setInterval are held back (their callbacks wait; requestAnimationFrame and microtasks still run), the change is made (the
// words typed, the font picked, the mode pressed, a position button pressed), ONE animation frame passes, and the screen is photographed (element screenshots: both face thumbnails, the work area canvas)
// while the held callbacks still wait. So a debounce, a "settle for 200 ms", a repaint on Save all show as "nothing happened". Then the timers are released, the page settles, and it is photographed again:
// "same tick" = the picture already is the settled picture. Worker answers are not timers and still arrive (a fit that needs the worker is not the same tick either, it simply never shows here).
// WHAT IS MEASURED ON A PICTURE (all in the page, on the screenshot's pixels, no knowledge of the code under test):
//   charm   the ink (dark and neutral: not the brown frame, not the blue handles, not the grey labels) spans >= 55% of the box in its long side, covers >= 4% of the box, and has the aspect of the charm drawn by the app itself (either way up)
//   changed the pixels are not the pixels from before the change
//   settled the pixels are the pixels the page settles on (same tick == final)
//   look    the picture is aligned with the app's OWN rendering of that charm (Engrave.renderBack of a REFERENCE order made by the real pipeline: other words, other font) after bbox-normalising,
//           and the cells where two references differ (the text strokes) agree with the right reference: score = (agree right - agree wrong) / cells, 1 = the right font/words, -1 = the old ones.
//   CONTROLS (a red test is only worth something if the same measure goes green on a right picture): S2 = the work area at rest against the app's own rendering of that order; I1 = a thumbnail-sized picture of the
//           charm + engraving drawn by the app's own renderer, put in a box and photographed like the thumbnails. Both must pass or the numbers below mean nothing.
const R = require('./_engraveRealWorld.cjs');
const path = require('path'), fs = require('fs');

const ONLY = process.env.RT_ONLY || '';
const RESULTS = [];
const OWNERS = { thumb: 'ENGTHUMB', work: 'ENGWORK', smart: 'ENGSMART', auto: 'ENGAUTO', note: 'ENGNOTE', why: 'ENGWHY', hilite: 'ENGHILITE', world: 'ENGLIVE' };
function check(id, ownerKey, ok, evidence) {
  const owner = OWNERS[ownerKey] || ownerKey;
  RESULTS.push({ id, owner, ok: !!ok, evidence });
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${evidence}  [${owner}]`);
}
const f2 = x => (typeof x === 'number' && isFinite(x) ? (Math.round(x * 100) / 100).toString() : String(x));

/* ── the instrument that lives in the page ─────────────────────────────────────────────────────────────────────────────────────────────────────────── */
const INSTRUMENT = String.raw`(() => {
  if (window.__rt) return;
  const real = { st: window.setTimeout, ct: window.clearTimeout, si: window.setInterval, ci: window.clearInterval };
  let q = null, n = 0;
  const N = 64; let THR = 175;
  const blobOf = b64 => { const s = atob(b64), u = new Uint8Array(s.length); for (let i = 0; i < s.length; i++) u[i] = s.charCodeAt(i); return new Blob([u], { type: 'image/png' }); };
  async function decode(b64) {
    const bmp = await createImageBitmap(blobOf(b64)), c = document.createElement('canvas'); c.width = bmp.width; c.height = bmp.height;
    const x = c.getContext('2d', { willReadFrequently: true }); x.fillStyle = '#fff'; x.fillRect(0, 0, c.width, c.height); x.drawImage(bmp, 0, 0);
    return x.getImageData(0, 0, c.width, c.height);
  }
  const inkOf = img => {
    const { data, width: w, height: h } = img, ink = new Uint8Array(w * h);
    for (let i = 0; i < w * h; i++) { const r = data[i * 4], g = data[i * 4 + 1], b = data[i * 4 + 2], lum = .299 * r + .587 * g + .114 * b; ink[i] = lum < THR && Math.max(r, g, b) - Math.min(r, g, b) < 50 ? 1 : 0; }
    return { w, h, ink };
  };
  const bboxOf = m => { let x0 = m.w, y0 = m.h, x1 = -1, y1 = -1; for (let y = 0; y < m.h; y++) for (let x = 0; x < m.w; x++) if (m.ink[y * m.w + x]) { if (x < x0) x0 = x; if (x > x1) x1 = x; if (y < y0) y0 = y; if (y > y1) y1 = y; } return x1 < 0 ? null : [x0, y0, x1 + 1, y1 + 1]; };
  const cellsOf = (m, box) => {   // N x N, the share of ink in each cell of the ink's bounding box
    const c = new Float32Array(N * N), cnt = new Float32Array(N * N); if (!box) return c;
    const bw = box[2] - box[0], bh = box[3] - box[1];
    for (let y = box[1]; y < box[3]; y++) for (let x = box[0]; x < box[2]; x++) { const cx = Math.min(N - 1, Math.floor((x - box[0]) / bw * N)), cy = Math.min(N - 1, Math.floor((y - box[1]) / bh * N)), i = cy * N + cx; cnt[i]++; if (m.ink[y * m.w + x]) c[i]++; }
    for (let i = 0; i < c.length; i++) c[i] = cnt[i] ? c[i] / cnt[i] : 0;
    return c;
  };
  const tr = (c, t) => { const o = new Float32Array(N * N), rot = t & 3, flip = t >> 2; for (let y = 0; y < N; y++) for (let x = 0; x < N; x++) { let sx = x, sy = y; if (flip) sx = N - 1 - sx; for (let k = 0; k < rot; k++) { const nx = N - 1 - sy, ny = sx; sx = nx; sy = ny; } o[y * N + x] = c[sy * N + sx]; } return o; };
  const ncc = (a, b) => { let ma = 0, mb = 0; for (let i = 0; i < a.length; i++) { ma += a[i]; mb += b[i]; } ma /= a.length; mb /= a.length; let s = 0, va = 0, vb = 0; for (let i = 0; i < a.length; i++) { const x = a[i] - ma, y = b[i] - mb; s += x * y; va += x * x; vb += y * y; } return va && vb ? s / Math.sqrt(va * vb) : 0; };
  const bin = (c, thr = .12) => Array.from(c, v => v >= thr ? 1 : 0);
  const load = async b64 => { const m = inkOf(await decode(b64)), box = bboxOf(m); let k = 0; for (const v of m.ink) k += v; return { m, box, c: cellsOf(m, box), frac: k / (m.w * m.h) }; };
  const best = (a, rs) => { let bt = 0, bs = -2; for (let t = 0; t < 8; t++) { const s = rs.reduce((p, r) => p + ncc(a.c, tr(r.c, t)), 0) / rs.length; if (s > bs) { bs = s; bt = t; } } return { t: bt, ncc: bs }; };
  const r2 = x => Math.round(x * 100) / 100;
  window.__rt = {
    setThr(v) { THR = v; return THR; },
    freeze() { if (q) return 0; q = new Map(); n = 0;
      window.setTimeout = (f, ms, ...a) => { if (typeof f !== 'function') return real.st.call(window, f, ms, ...a); const id = -(++n); q.set(id, { k: 't', f, ms: +ms || 0, a }); return id; };
      window.setInterval = (f, ms, ...a) => { if (typeof f !== 'function') return real.si.call(window, f, ms, ...a); const id = -(++n); q.set(id, { k: 'i', f, ms: +ms || 0, a }); return id; };
      window.clearTimeout = id => { if (q && typeof id === 'number' && id < 0) q.delete(id); else real.ct.call(window, id); };
      window.clearInterval = id => { if (q && typeof id === 'number' && id < 0) q.delete(id); else real.ci.call(window, id); };
      return 1; },
    deferred() { return q ? q.size : 0; },
    thaw() { if (!q) return 0; const items = [...q.values()]; q = null; window.setTimeout = real.st; window.setInterval = real.si; window.clearTimeout = real.ct; window.clearInterval = real.ci; for (const it of items) { if (it.k === 't') real.st.call(window, it.f, it.ms, ...it.a); else real.si.call(window, it.f, it.ms, ...it.a); } return items.length; },
    async stats(b64) { const m = inkOf(await decode(b64)), box = bboxOf(m); if (!box) return { w: m.w, h: m.h, ink: 0 }; const bw = box[2] - box[0], bh = box[3] - box[1]; let k = 0; for (const v of m.ink) k += v; return { w: m.w, h: m.h, ink: r2(k / (m.w * m.h)), longFrac: r2(Math.max(bw / m.w, bh / m.h)), aspect: r2(bw / bh), bw, bh }; },
    async raw(a64, b64) { const A = await decode(a64), B = await decode(b64); if (A.width !== B.width || A.height !== B.height) return { size: false, diff: 1, a: [A.width, A.height], b: [B.width, B.height] }; let d = 0; for (let i = 0; i < A.width * A.height; i++) { const dl = Math.abs(.299 * (A.data[i * 4] - B.data[i * 4]) + .587 * (A.data[i * 4 + 1] - B.data[i * 4 + 1]) + .114 * (A.data[i * 4 + 2] - B.data[i * 4 + 2])); if (dl > 40) d++; } return { size: true, diff: r2(d / (A.width * A.height) * 100) / 100, px: d }; },
    async move(a64, b64) {   // where the new ink is relative to the ink that went (a move right gives dx > 0), in px
      const A = inkOf(await decode(a64)), B = inkOf(await decode(b64)); if (A.w !== B.w || A.h !== B.h) return { size: false };
      let nx = 0, ny = 0, nn = 0, lx = 0, ly = 0, ln = 0; for (let y = 0; y < A.h; y++) for (let x = 0; x < A.w; x++) { const a = A.ink[y * A.w + x], b = B.ink[y * A.w + x]; if (b && !a) { nx += x; ny += y; nn++; } else if (a && !b) { lx += x; ly += y; ln++; } }
      return nn && ln ? { size: true, dx: r2(nx / nn - lx / ln), dy: r2(ny / nn - ly / ln), nNew: nn, nLost: ln } : { size: true, dx: 0, dy: 0, nNew: nn, nLost: ln };
    },
    async look(a64, f64, g64) {   // does A look like F rather than G (two renderings of the same charm that differ in the words or the font)?
      const A = await load(a64), F = await load(f64), G = await load(g64);
      if (!A.box) return { err: 'blank picture' };
      const al = best(A, [F, G]), Fc = tr(F.c, al.t), Gc = tr(G.c, al.t), fb = bin(Fc), gb = bin(Gc), ab = bin(A.c);
      let nD = 0, aF = 0, aG = 0; for (let i = 0; i < ab.length; i++) if (fb[i] !== gb[i]) { nD++; if (ab[i] === fb[i]) aF++; else aG++; }
      return { t: al.t, ncc: r2(al.ncc), nccF: r2(ncc(A.c, Fc)), nccG: r2(ncc(A.c, Gc)), nD, score: nD ? r2((aF - aG) / nD) : 0 };
    },
    async own(a64, refs) {   // does the picture draw THIS charm? the cells of the app's own drawing of it (any of the given references, either way up) that the picture also inks (one cell of tolerance); a solid block does not pass
      const A = await load(a64); if (!A.box) return { err: 'blank picture', cover: 0, fill: 0 };
      const ab = bin(A.c), dil = new Uint8Array(N * N); for (let y = 0; y < N; y++) for (let x = 0; x < N; x++) { let v = 0; for (let dy = -1; dy <= 1 && !v; dy++) for (let dx = -1; dx <= 1; dx++) { const yy = y + dy, xx = x + dx; if (yy >= 0 && yy < N && xx >= 0 && xx < N && ab[yy * N + xx]) { v = 1; break; } } dil[y * N + x] = v; }
      let nA = 0; for (const v of ab) nA += v;
      let out = { cover: 0, which: -1, ncc: 0, fill: r2(nA / (N * N)), aspRatio: 1, inkRatio: 0 };
      for (let k = 0; k < refs.length; k++) { if (!refs[k]) continue; const Rf = await load(refs[k]); if (!Rf.box) continue; const al = best(A, [Rf]), Rc = bin(tr(Rf.c, al.t)); let nR = 0, hit = 0; for (let i = 0; i < Rc.length; i++) if (Rc[i]) { nR++; if (dil[i]) hit++; } const cv = nR ? hit / nR : 0; if (cv > out.cover) { const rb = Rf.box, ra = (rb[2] - rb[0]) / (rb[3] - rb[1]), rAsp = (al.t & 3) % 2 === 1 ? 1 / ra : ra, ab2 = A.box, aAsp = (ab2[2] - ab2[0]) / (ab2[3] - ab2[1]); out = { cover: r2(cv), which: k, ncc: r2(al.ncc), fill: out.fill, nCells: nR, aspRatio: r2(aAsp / rAsp), inkRatio: r2(Rf.frac ? A.frac / Rf.frac : 0) }; } }
      return out;
    },
    async cover(a64, with64, no64) {   // is the engraving that the reference has on the charm (cells with text, not in the charm-only picture) drawn in A? (one cell of tolerance each way)
      const A = await load(a64), W = await load(with64), O = await load(no64);
      if (!A.box) return { err: 'blank picture' };
      const grow = b => { const o = new Uint8Array(N * N); for (let y = 0; y < N; y++) for (let x = 0; x < N; x++) { let v = 0; for (let dy = -1; dy <= 1 && !v; dy++) for (let dx = -1; dx <= 1; dx++) { const yy = y + dy, xx = x + dx; if (yy >= 0 && yy < N && xx >= 0 && xx < N && b[yy * N + xx]) { v = 1; break; } } o[y * N + x] = v; } return o; };
      const al = best(A, [W]), Wc = bin(tr(W.c, al.t)), Oc = bin(tr(O.c, al.t)), ab = bin(A.c), Ag = grow(ab), Rg = grow(Wc.map((v, i) => v | Oc[i]));
      let nT = 0, hit = 0, nA = 0, extra = 0; for (let i = 0; i < ab.length; i++) { if (Wc[i] && !Oc[i]) { nT++; if (Ag[i]) hit++; } if (ab[i]) { nA++; if (!Rg[i]) extra++; } }
      return { t: al.t, ncc: r2(al.ncc), nText: nT, cover: nT ? r2(hit / nT) : 0, stray: nA ? r2(extra / nA) : 0 };
    },
    async art(a64, full64, outline64) {   // are the design's inner lines (cut-outs, engraved art) drawn: cells that the full front view has and the outline alone does not
      const A = await load(a64), F = await load(full64), O = await load(outline64);
      if (!A.box) return { err: 'blank picture' };
      const al = best(A, [F]), Fc = bin(tr(F.c, al.t)), Oc = bin(tr(O.c, al.t)), ab = bin(A.c);
      let nI = 0, hit = 0; for (let i = 0; i < ab.length; i++) if (Fc[i] && !Oc[i]) { nI++; if (ab[i]) hit++; }
      return { t: al.t, ncc: r2(al.ncc), nInner: nI, cover: nI ? r2(hit / nI) : 0 };
    },
    /* references: what the page's own renderer draws for a job (by key), as a PNG: 'text' = with its text, 'plain' = the charm alone, 'front' = Engrave.renderFront of its charm, 'frontOutline' = the outline alone */
    ref(key, kind, px) {
      const job = Engrave.items().get(key); if (!job) return null;
      let cv;
      if (kind === 'text') cv = Engrave.renderBack(job, px, { grid: false, hatch: false });
      else if (kind === 'work') cv = Engrave.renderBack(job, px, { grid: true, editable: true });
      else if (kind === 'plain') cv = Engrave.renderBack(Object.assign({}, job, { fit: null }), px, { grid: false, hatch: false });
      else { const ch = job.editCharm || Pool.charmOf(job.copies[0]); cv = Engrave.renderFront(kind === 'front' ? ch : Object.assign({}, ch, { members: [ch.outline] }), px, {}); }
      return cv && cv.toDataURL ? cv.toDataURL('image/png').split(',')[1] : null;
    },
    /* a reference at the SIZE OF A THUMBNAIL: the page's own drawing of the job, put in a box of that size and photographed like the thumbnails are (so the same down-scaling blurs the thin strokes the same way) */
    mountRef(key, kind, w, h, hatch) {
      const job = Engrave.items().get(key); if (!job) return false;
      const px = Math.max(60, Math.round(w * 2));
      let cv;
      if (kind === 'text') cv = Engrave.renderBack(job, px, { grid: false, hatch: !!hatch });
      else if (kind === 'plain') cv = Engrave.renderBack(Object.assign({}, job, { fit: null }), px, { grid: false, hatch: !!hatch });
      else { const ch = job.editCharm || Pool.charmOf(job.copies[0]); cv = Engrave.renderFront(kind === 'front' ? ch : Object.assign({}, ch, { members: [ch.outline] }), px, {}); }
      if (!cv) return false;
      const old = document.getElementById('__rtBox'); if (old) old.remove();
      const box = document.createElement('div'); box.id = '__rtBox'; box.style.cssText = 'position:fixed;left:8px;top:8px;z-index:99999;width:' + w + 'px;height:' + h + 'px;box-sizing:border-box;border:1px solid #d9d0c0;border-radius:0;background:#fff;overflow:hidden';
      cv.style.cssText = 'width:100%;height:100%;object-fit:contain;display:block'; box.appendChild(cv); (document.querySelector('dialog[open]') || document.body).appendChild(box); return true;   // (the order window is a modal dialog, in the top layer: the box goes inside it)
    },
    unmountRef() { const e = document.getElementById('__rtBox'); if (e) e.remove(); return true; },
    charmAspect(key) {   // the aspect (w/h of the ink box) of the charm as the app draws it, upright (front view) and as the work area draws it
      return null;
    }
  };
})()`;

/* ── helpers on the node side ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
const SEL = {
  detail: '#egQueue .rvItem[data-kind=placement]',
  thumb: face => `#egQueue .rvItem[data-kind=placement] .engCard .engFace[data-face="${face}"] .engThumb`,
  work: '#egQueue .rvItem[data-kind=placement] .backHost canvas'
};
async function shotB64(page, selector) {
  const loc = page.locator(selector).first();
  if (!(await loc.count())) return null;
  try { return (await loc.screenshot({ animations: 'allow', timeout: 8000 })).toString('base64'); } catch (e) { return null; }
}
// the ink threshold: thumbnails (thin strokes, anti-aliased down to a grey at 100 px) 175; the work area (it also draws light-grey labels and a grid, which must not count as ink) 120
const rtAt = (thr, page, fn, args) => page.evaluate(({ thr, fn, args }) => { window.__rt.setThr(thr); return window.__rt[fn](...args); }, { thr, fn, args });
const rt = (page, fn, ...args) => rtAt(175, page, fn, args);
/** the page's own drawing of a job as it would look in a thumbnail box of w x h px (kind: text | plain | front | frontOutline) */
async function boxed(page, key, kind, w, h, hatch) {
  const ok = await page.evaluate(({ key, kind, w, h, hatch }) => window.__rt.mountRef(key, kind, w, h, hatch), { key, kind, w, h, hatch });
  const b = ok ? await shotB64(page, '#__rtBox') : null; await page.evaluate(() => window.__rt.unmountRef()); return b;
}
const sizeOf = (page, selector) => page.evaluate(sel => { const e = document.querySelector(sel); if (!e) return null; const b = e.getBoundingClientRect(); return { w: Math.round(b.width), h: Math.round(b.height) }; }, selector);
const rtw = (page, fn, ...args) => rtAt(120, page, fn, args);
// "this picture draws the charm": the cells of the app's own drawing of it are drawn (>= 60%, one cell of tolerance), it is not a solid block, it has at least half the ink of the app's drawing, and the ink has the charm's shape
// (the aspect of its box within 30% of the charm's, either way up): the words alone, stretched over the charm's box, do not pass
const ownOk = (st, o) => !!st && !!o && !o.err && st.ink >= 0.005 && o.cover >= 0.6 && o.fill <= 0.6 && o.inkRatio >= 0.5 && Math.abs(Math.log(o.aspRatio || 1)) <= 0.3;
const ownEv = (st, o) => `${f2(((o && o.cover) || 0) * 100)}% of the cells of the app's own drawing of this charm are drawn in the picture; ink ${st ? f2(st.ink * 100) : '?'}% of the box = ${o ? f2(o.inkRatio || 0) : '?'} x the app's drawing, shape ${o ? f2(o.aspRatio || 0) : '?'} x the charm's${o && o.err ? ' (' + o.err + ')' : ''}`;

/** the facts of the Engrave detail now (DOM) */
const detailFacts = page => page.evaluate(() => {
  const vis = e => { if (!e) return false; const b = e.getBoundingClientRect(), s = getComputedStyle(e); return b.width > 0 && b.height > 0 && s.visibility !== 'hidden' && s.display !== 'none' && !e.closest('[hidden]'); };
  const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); if (!c) return { open: false };
  const faces = [...c.querySelectorAll('.engCard .engFace')].filter(vis).map(f => {
    const pv = f.querySelector('.engpv'), draw = f.querySelector('.engpvDraw'), txt = f.querySelector('.engpvText');
    return { face: f.dataset.face, empty: f.dataset.empty, label: (f.querySelector('.engTxt') || {}).textContent || '', current: f.hasAttribute('aria-current'), drawKids: draw ? draw.children.length : -1, drawHtml: draw ? draw.innerHTML.length : -1,
      pvText: txt && !txt.hidden ? txt.textContent : null, pvFont: txt && !txt.hidden ? getComputedStyle(txt).fontFamily : null, imgs: f.querySelectorAll('.engThumb img, .engThumb canvas, .engThumb svg').length, pvState: pv && pv.dataset.state };
  });
  const cv = c.querySelector('.backHost canvas');
  return { open: true, faces, work: !!cv, workBox: cv && cv._box ? { centre: cv._box.centrePx.map(Math.round), w: Math.round(Math.hypot(cv._box.corners[1][0] - cv._box.corners[0][0], cv._box.corners[1][1] - cv._box.corners[0][1])) } : null,
    mode: (c.querySelector('.engp') || {}).dataset ? c.querySelector('.engp').dataset.mode : null, face: (c.querySelector('.engp') || {}).dataset ? c.querySelector('.engp').dataset.face : null,
    words: (c.querySelector('.engpWords') || {}).value, font: (c.querySelector('.engpFont') || {}).value, fontLabel: (() => { const f = c.querySelector('.engpFont'); return f && f.selectedOptions[0] ? f.selectedOptions[0].textContent.trim() : ''; })(),
    job: (() => { try { const j = Engrave.items().get(window.__rtJobKey); return j && j.fit ? { centre: (j.fit.centre || []).map(v => Math.round(v * 100) / 100), text: j.text, font: j.fontKey } : null; } catch (_) { return null; } })() };
});


const hiliteFacts = page => page.evaluate(() => {
  const vis = e => { if (!e) return false; const b = e.getBoundingClientRect(), st = getComputedStyle(e); return b.width > 0 && b.height > 0 && st.visibility !== 'hidden' && st.display !== 'none' && +st.opacity > 0 && !e.closest('[hidden]'); };
  const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); if (!c) return { open: false };
  const segs = [...c.querySelectorAll('.engpMode:not(.engpFaceSw) .engpSeg[data-engp=mode]')], marked = segs.filter(x => x.hasAttribute('data-sys')).map(x => x.dataset.mode);
  const sparks = [...c.querySelectorAll('.engpSys')].filter(vis), box = c.querySelector('.engpBox'), fb = c.querySelector('.engpFontBox');
  return { open: true, marked, sparks: sparks.length, boxMark: box ? box.getAttribute('data-sys') : null, fontMark: fb ? fb.getAttribute('data-sys') : null, fontSpark: !!(fb && [...fb.querySelectorAll('.engpSys')].some(vis)), fontLabels: [...c.querySelectorAll('.engpFont option')].filter(o => o.hasAttribute('data-sys')).map(o => o.label || o.textContent), selected: (c.querySelector('.engpFont') || {}).value };
});

/** one measured change: before / same tick / settled pictures of both thumbs and the work area */
async function trial(w, act, arg) {
  const page = w.page;
  const grab = async () => ({ front: await shotB64(page, SEL.thumb('front')), back: await shotB64(page, SEL.thumb('back')), work: await shotB64(page, SEL.work), facts: await detailFacts(page) });
  const before = await grab();
  await page.evaluate(() => window.__rt.freeze());
  const held = await page.evaluate(({ act, arg }) => new Promise(res => { window.__act[act](arg); requestAnimationFrame(() => res(window.__rt.deferred())); }), { act, arg });
  const tick = await grab();
  await page.evaluate(() => window.__rt.thaw());
  await R.settle(page, 900);
  const settled = await grab();
  return { before, tick, settled, held };
}

/** the changes a person makes (run in the page) */
const ACTS = String.raw`(() => {
  const q = (s) => document.querySelector('#egQueue .rvItem[data-kind=placement] ' + s) || document.querySelector(s);
  window.__act = {
    type(text) { const t = q('.engpWords'); const set = Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value').set; t.focus(); set.call(t, text); t.dispatchEvent(new InputEvent('input', { bubbles: true, inputType: 'insertText', data: text })); t.dispatchEvent(new Event('keyup', { bubbles: true })); },
    font(word) { const s = q('.engpFont'); s.value = word; s.dispatchEvent(new Event('input', { bubbles: true })); s.dispatchEvent(new Event('change', { bubbles: true })); },
    mode(m) { q('.engpSeg[data-mode="' + m + '"]').click(); },
    nudge(dir) { q('.engpPb[data-act="' + dir + '"]').click(); },
    face(f) { const b = q('.engCard .engFace[data-face="' + f + '"] .engThumb'); b.click(); }
  };
})()`;

async function refsFor(w, made) {
  const page = w.page, px = 320;
  const keyOfJob = (n, face) => ((made[n] || { jobs: [] }).jobs.find(j => j.face === face) || {}).key;
  const base = keyOfJob('back', 'back'), anna = keyOfJob('refAnna', 'back'), marck = keyOfJob('refMarck', 'back');
  const get = (key, kind, size) => page.evaluate(({ key, kind, size }) => window.__rt.ref(key, kind, size), { key, kind, size });
  return { base, anna, marck, get, px };
}

/* ── part 1: the Engrave tab's detail, change by change ─────────────────────────────────────────────────────────────────────────────────────────── */
async function partDetail(w, made) {
  const page = w.page;
  const refs = await refsFor(w, made);
  await R.openEngraveDetail(w, 'back', { face: 'back' }); await R.settle(page, 900);
  const tsz = (await sizeOf(page, SEL.thumb('back'))) || { w: 104, h: 104 };
  const refImg = {}; for (const [k, key] of [['base', refs.base], ['anna', refs.anna], ['marck', refs.marck]]) { refImg[k] = { thumb: await boxed(page, key, 'text', tsz.w, tsz.h, false), work: await refs.get(key, 'work', 400) }; }
  const SENSE = 0.3;   // calibrated on the controls: the work area at rest scores +0.58 against its own order (S2) and a thumbnail drawn by the app's renderer +0.95 (I1); a picture that did not change scores negative; 0.3 = clearly closer to the right words / font than to the old ones
  const chRefs = { plain: await boxed(page, refs.base, 'plain', tsz.w, tsz.h, false), front: await boxed(page, refs.base, 'front', tsz.w, tsz.h) };
  // does a picture draw this charm? (the app's own drawing of it, cell by cell, either way up) and is it not blank / not a block
  const charmPic = async (b64, set) => {
    if (!b64) return { ok: false, ev: 'no picture' };
    const st = await rt(page, 'stats', b64), o = await rt(page, 'own', b64, [set.plain, set.front]);
    return { ok: ownOk(st, o), st, o, ev: ownEv(st, o) };
  };

  const jobKey0 = await R.openEngraveDetail(w, 'back', { face: 'back' }); await page.evaluate(k => { window.__rtJobKey = k; }, jobKey0);
  await page.evaluate(() => { try { return CharmNestEngravePreview.warm(); } catch (_) {} });
  await R.settle(page, 1200);
  const start = await detailFacts(page);
  await R.shot(w, 'LIVE-rt-detail-start');

  // ── the starting state ──
  const sb = await shotB64(page, SEL.thumb('back')), sw = await shotB64(page, SEL.work);
  const cpB = await charmPic(sb, chRefs);
  check('S1 the detail opens on the system\'s choice: mode back, the customer\'s words, the chosen font', 'auto', start.open && start.mode === 'back' && start.words === 'Name One' && /stylish/i.test(String(start.fontLabel)), `mode ${start.mode}, words "${start.words}", font "${start.fontLabel}"`);
  check('S1b a back-only necklace shows only the Back face on the card', 'smart', start.open && start.faces.length === 1 && start.faces[0].face === 'back', `faces shown ${JSON.stringify((start.faces || []).map(f => f.face))}`);
  check('S2 work area starts as the real charm with the engraving in the system\'s font (the instrument\'s own control)', 'world', await (async () => { if (!sw) return false; const l = await rtw(page, 'look', sw, refImg.base.work, refImg.anna.work); return l.nccF >= 0.7 && l.score >= SENSE; })(), 'the work area against the app\'s own rendering of that order: ' + JSON.stringify(sw ? await rtw(page, 'look', sw, refImg.base.work, refImg.anna.work) : null));
  check('T0 the back thumbnail at rest has the charm drawing in it, not only text', 'thumb', cpB.ok, cpB.ev + `; engpvDraw children ${start.faces[0] ? start.faces[0].drawKids : '-'}`);

  const hl0 = await hiliteFacts(page);
  check('H1 the system\'s own choice is marked while nothing is changed (its mode, its words, its font; a visible sparkle each)', 'hilite', hl0.open && hl0.marked.join() === 'back' && hl0.boxMark === 'same' && hl0.sparks >= 2 && hl0.fontLabels.length >= 1, `mode marked: ${JSON.stringify(hl0.marked)}, words box: ${hl0.boxMark}, visible sparkles: ${hl0.sparks}, system font entries marked: ${JSON.stringify(hl0.fontLabels)}`);

  // ── the instrument's own control: a thumbnail drawn by the app's renderer (injected here, it is NOT the app's thumbnail) must pass the very measures that the app's thumbnails are held to ──
  const ctlB = await boxed(page, refs.base, 'text', tsz.w, tsz.h, true);   // (the references are drawn without the hatch of the engraved art, this control with it: a different but legitimate drawing)
  const ctlCp = await charmPic(ctlB, chRefs), ctlLk = ctlB && await rt(page, 'look', ctlB, refImg.base.thumb, refImg.anna.thumb), ctlCv = ctlB && await rt(page, 'cover', ctlB, refImg.base.thumb, chRefs.plain);
  check('I1 instrument control: a thumbnail drawn by the app\'s own renderer passes the charm and look measures', 'world', ctlCp.ok && !!ctlLk && !ctlLk.err && ctlLk.nccF >= 0.45 && ctlLk.score >= SENSE && !!ctlCv && !ctlCv.err && ctlCv.cover >= 0.5 && ctlCv.stray <= 0.5, `${ctlCp.ev}; look ${JSON.stringify(ctlLk)}; engraving cover ${JSON.stringify(ctlCv)}`);

  const evalChange = async (label, t, o) => {
    // o: { thumbs: ['back'] | ['front','back'], work: { right: b64, wrong: b64 } | 'moves', thumbLook: { right, wrong } }
    const cpMap = {};
    for (const face of o.thumbs) {
      const cp = await charmPic(t.tick[face], chRefs); cpMap[face] = cp;
      check(`${label} · ${face} thumbnail has the charm drawing in it (same tick)`, 'thumb', cp.ok, cp.ev);
    }
    if (o.edit) {
      const face = o.edit;
      if (!t.before[face] || !t.tick[face]) check(`${label} · ${face} thumbnail changes in the same tick`, 'thumb', false, 'no thumbnail to photograph');
      else {
        const ch1 = await rt(page, 'raw', t.before[face], t.tick[face]), same = await rt(page, 'raw', t.tick[face], t.settled[face]);
        const late = await rt(page, 'raw', t.before[face], t.settled[face]);
        check(`${label} · ${face} thumbnail changes in the same tick, and is already the settled picture`, 'thumb', ch1.diff >= 0.001 && same.diff <= 0.003, `changed ${f2(ch1.diff * 100)}% of its pixels at the tick, then ${f2(same.diff * 100)}% more after settling (${f2(late.diff * 100)}% changed in all); ${t.held} timer(s) were waiting`);
        if (o.thumbLook) {
          const lk = await rt(page, 'look', t.tick[face], o.thumbLook.right.thumb, o.thumbLook.wrong.thumb);
          check(`${label} · ${face} thumbnail shows the new ${o.thumbLook.what} on the charm`, 'thumb', !!(cpMap[face] && cpMap[face].ok) && !lk.err && lk.nccF >= 0.45 && lk.score >= SENSE, `${cpMap[face] && cpMap[face].ok ? '' : 'no charm drawing in the picture; '}looks like the right one at ${f2(lk.nccF)}, the old one at ${f2(lk.nccG)}, strokes that tell them apart: ${lk.nD} cells, score ${f2(lk.score)} (aligned ${lk.ncc})${lk.err ? ' ' + lk.err : ''}`);
        }
      }
    }
    if (o.work) {
      if (!t.before.work || !t.tick.work) check(`${label} · work area redraws in the same tick`, 'work', false, 'no work area canvas to photograph');
      else {
        const ch1 = await rtw(page, 'raw', t.before.work, t.tick.work), same = await rtw(page, 'raw', t.tick.work, t.settled.work), lateW = await rtw(page, 'raw', t.before.work, t.settled.work);
        const lateTxt = `; by the time the page had settled (0.9 s) ${f2(lateW.diff * 100)}% of the picture had changed in all`;
        if (o.work === 'moves') {
          const mv = await rtw(page, 'move', t.before.work, t.tick.work);
          check(`${label} · work area moves the engraving in the same tick`, 'work', mv.size && ch1.diff > 0 && (Math.hypot(mv.dx, mv.dy) >= 1 || ch1.diff >= 0.002) && same.diff <= 0.003, `pressing "${o.dir}": the engraving moved ${f2(mv.dx)} px across and ${f2(mv.dy)} px down at the tick (${mv.nNew} new ink px, ${mv.nLost} gone), ${f2(same.diff * 100)}% more after settling${lateTxt}; the job's own text position (mm) ${JSON.stringify((t.before.facts.job || {}).centre)} -> ${JSON.stringify((t.tick.facts.job || {}).centre)} -> ${JSON.stringify((t.settled.facts.job || {}).centre)}`);
        } else {
          const lk = await rtw(page, 'look', t.tick.work, o.work.right.work, o.work.wrong.work);
          check(`${label} · work area shows the new ${o.work.what} in the same tick, before Save`, 'work', !lk.err && ch1.diff > 0 && lk.nccF >= 0.7 && lk.score >= SENSE && same.diff <= 0.003, `${f2(ch1.diff * 100)}% of the picture changed at the tick; looks like the new ${o.work.what} at ${f2(lk.nccF)} vs the old ${f2(lk.nccG)}, ${lk.nD} telling cells, score ${f2(lk.score)}; ${f2(same.diff * 100)}% more after settling${lateTxt}`);
        }
      }
    }
  };

  // which position button has room to move the engraving on this charm? (found by pressing, then cancelled; the measured change below uses it)
  const pickDir = async () => {
    for (const dir of ['right', 'left', 'up', 'down', 'smaller']) {
      const before = (await detailFacts(page)).job;
      const clicked = await page.evaluate(d => { const b = document.querySelector('#egQueue .rvItem[data-kind=placement] .engpPb[data-act="' + d + '"]'); if (!b || b.disabled) return false; b.click(); return true; }, dir);
      await R.settle(page, 250);
      const after = (await detailFacts(page)).job;
      await page.evaluate(() => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement] [data-engp=cancel]'); if (c && !c.disabled) c.click(); });
      await R.settle(page, 600);
      if (clicked && before && after && JSON.stringify(before.centre) !== JSON.stringify(after.centre)) return dir;
    }
    return 'right';
  };

  // ── mode back (the system's): type, font, nudge ──
  let t = await trial(w, 'type', 'Anna');
  await evalChange('R1 typing "Anna"', t, { thumbs: ['back'], edit: 'back', thumbLook: { what: 'words', right: refImg.anna, wrong: refImg.base }, work: { what: 'words', right: refImg.anna, wrong: refImg.base } });
  await page.evaluate(() => window.__act.type('Name One')); await R.settle(page, 500);   // (back to the system's words)
  t = await trial(w, 'font', 'Pristina');
  await evalChange('R2 picking the font Pristina', t, { thumbs: ['back'], edit: 'back', thumbLook: { what: 'font', right: refImg.marck, wrong: refImg.base }, work: { what: 'font', right: refImg.marck, wrong: refImg.base } });
  const hl2 = await hiliteFacts(page);
  check('H2 after another font is picked the system\'s choice stays marked (mode, font entry) and the change shows beside the closed list', 'hilite', hl2.open && hl2.marked.join() === 'back' && hl2.fontMark === 'changed' && hl2.fontSpark && hl2.fontLabels.length >= 1, `mode marked: ${JSON.stringify(hl2.marked)}, font box: ${hl2.fontMark}, sparkle beside the list: ${hl2.fontSpark}, system font entries marked: ${JSON.stringify(hl2.fontLabels)}, picked: "${hl2.selected}"`);
  await page.evaluate(() => window.__act.font('')); await R.settle(page, 500);
  const dir4 = await pickDir();
  t = await trial(w, 'nudge', dir4);
  await evalChange('R4 nudging the position', t, { thumbs: ['back'], edit: 'back', work: 'moves', dir: dir4 });
  await page.evaluate(() => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement] [data-engp=cancel]'); if (c && !c.disabled) c.click(); }); await R.settle(page, 700);

  // ── mode: faces follow the staged mode, live ──
  const faceNames = f => (f.faces || []).map(x => x.face).join('+') || 'none';
  const modes = [['front', 'front'], ['both', 'front+back'], ['none', 'none'], ['back', 'back']];
  for (const [m, want] of modes) {
    t = await trial(w, 'mode', m);
    const got = faceNames(t.tick.facts);
    check(`R3 pressing "${m}": the card shows ${want} in the same tick`, 'smart', got === want, `faces on the card at the tick: ${got}; ${t.tick.facts.faces.filter(f => f.empty === '1').length} empty slot(s)`);
    for (const face of ['front', 'back']) if (t.tick.facts.faces.some(f => f.face === face)) {
      const cp = await charmPic(t.tick[face], chRefs);
      check(`R3 pressing "${m}": the ${face} thumbnail has the charm drawing in it (same tick)`, 'thumb', cp.ok, cp.ev);
    }
  }
  await page.evaluate(() => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement] [data-engp=cancel]'); if (c && !c.disabled) c.click(); }); await R.settle(page, 700);

  // ── mode "Front + back" staged: BOTH thumbnails, and the edits of the back leave the front alone ──
  await page.evaluate(() => window.__act.mode('both')); await R.settle(page, 900);
  const both0 = await detailFacts(page);
  check('R5 with Front + back staged the card shows two faces, each with the charm', 'smart', faceNames(both0) === 'front+back', `faces ${faceNames(both0)}`);
  await page.evaluate(() => window.__act.face('back')); await R.settle(page, 500);
  t = await trial(w, 'type', 'Anna');
  await evalChange('R6 (front + back) typing "Anna" on the back', t, { thumbs: ['front', 'back'], edit: 'back', thumbLook: { what: 'words', right: refImg.anna, wrong: refImg.base }, work: { what: 'words', right: refImg.anna, wrong: refImg.base } });
  if (t.before.front && t.tick.front) { const iso = await rt(page, 'raw', t.before.front, t.tick.front), fst = await rt(page, 'stats', t.tick.front); check('R6 (front + back) typing on the back leaves the front thumbnail alone (and it is a picture of the charm, not an empty box)', 'thumb', iso.diff <= 0.002 && fst.ink >= 0.005, `${f2(iso.diff * 100)}% of the front thumbnail changed; it holds ${f2(fst.ink * 100)}% ink`); }
  await page.evaluate(() => window.__act.type('Name One')); await R.settle(page, 500);
  t = await trial(w, 'font', 'Pristina');
  await evalChange('R7 (front + back) picking the font Pristina for the back', t, { thumbs: ['front', 'back'], edit: 'back', thumbLook: { what: 'font', right: refImg.marck, wrong: refImg.base }, work: { what: 'font', right: refImg.marck, wrong: refImg.base } });
  await page.evaluate(() => window.__act.font('')); await R.settle(page, 500);
  const dir8 = dir4;   // (the same text, the same room; probing again would press Cancel and drop the staged Front + back)
  t = await trial(w, 'nudge', dir8);
  await evalChange('R8 (front + back) nudging the back', t, { thumbs: ['front', 'back'], edit: 'back', work: 'moves', dir: dir8 });
  await R.shot(w, 'LIVE-rt-detail-both-staged');
  await page.evaluate(() => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement] [data-engp=cancel]'); if (c && !c.disabled) c.click(); }); await R.settle(page, 700);

  // ── the front thumbnail shows the design's cut-outs and engraved art (the front-only and the front + back orders) ──
  for (const n of ['front', 'both']) {
    const jf = (made[n].jobs.find(j => j.face === 'front') || {}).key; if (!jf) { check(`A1 ${n}: the front thumbnail shows the design's cut-outs and art`, 'world', false, 'the world has no front job for this order'); continue; }
    await R.openEngraveDetail(w, n, { face: 'front' }); await R.settle(page, 900);
    const th = await shotB64(page, SEL.thumb('front')), sz = (await sizeOf(page, SEL.thumb('front'))) || tsz;
    const full = await boxed(page, jf, 'front', sz.w, sz.h), outl = await boxed(page, jf, 'frontOutline', sz.w, sz.h), plainJ = await boxed(page, jf, 'plain', sz.w, sz.h);
    const ar = th && full && outl ? await rt(page, 'art', th, full, outl) : null;
    const cp = await (async () => { if (!th) return { ok: false, ev: 'no picture' }; const st = await rt(page, 'stats', th), o = await rt(page, 'own', th, [full, plainJ]); return { ok: ownOk(st, o), ev: ownEv(st, o) }; })();
    check(`A1 ${n}: the front thumbnail has the charm drawing in it`, 'thumb', cp.ok, cp.ev);
    check(`A1 ${n}: the front thumbnail shows the design's cut-outs and engraved art`, 'thumb', !!ar && !ar.err && ar.nInner >= 8 && ar.cover >= 0.5, ar ? `${ar.nInner} cells of inner lines in the app's own front view, ${f2(ar.cover * 100)}% of them drawn in the thumbnail (aligned ${ar.ncc})` : 'no picture');
    await R.shot(w, 'LIVE-rt-detail-' + n);
  }
}

/* ── part 2: the order window card, for every order of the world ────────────────────────────────────────────────────────────────────────────────── */
async function partOrder(w, made) {
  const page = w.page;
  const WANT = { back: [['back']], front: [['front']], both: [['front', 'back']], tag: [['front', 'back']], discs: [['back'], ['back'], ['back']], earrings: [] };
  for (const n of ['back', 'front', 'both', 'tag', 'discs', 'earrings']) {
    await R.openOrderWindow(w, n);
    const cards = await page.evaluate(() => { const vis = e => { const b = e.getBoundingClientRect(); return b.width > 0 && b.height > 0 && !e.closest('[hidden]'); }; return [...document.querySelectorAll('#owEng .engCard')].filter(vis).map((c, i) => ({ i, piece: c.dataset.piece, faces: [...c.querySelectorAll('.engFace')].filter(vis).map(f => ({ face: f.dataset.face, empty: f.dataset.empty, label: (f.querySelector('.engTxt') || {}).textContent || '' })) })); });
    const want = WANT[n], got = cards.map(c => c.faces.map(f => f.face));
    check(`O1 order window ${n}: the card(s) show only the faces that are engraved`, 'smart', JSON.stringify(got) === JSON.stringify(want), `want ${JSON.stringify(want)}, got ${JSON.stringify(got)}; empty slots ${cards.reduce((a, c) => a + c.faces.filter(f => f.empty === '1').length, 0)}`);
    await R.shot(w, 'LIVE-rt-order-' + n);
    let ci = 0;
    for (const c of cards) {
      for (const f of c.faces) {
        if (f.empty === '1') continue;   // (an empty slot is O1's business: it should not be shown)
        const sel = `#owEng .engCard:nth-of-type(${c.i + 1}) .engFace[data-face="${f.face}"] .engThumb`;
        const th = await shotB64(page, sel); if (!th) { check(`O2 order window ${n} ${(c.piece || '').split('#')[1] || ''} ${f.face}: the thumbnail has the charm drawing in it`.replace('  ', ' '), 'thumb', false, 'no thumbnail'); continue; }
        const st = await rt(page, 'stats', th);
        const jk = ((made[n].jobs.filter(j => j.face === f.face)).find(j => !j.slot || (c.piece || '').endsWith('#' + j.slot)) || made[n].jobs.find(j => j.face === f.face) || {}).key;
        let cover = null, own = null;
        if (jk) { const sz = (await sizeOf(page, sel)) || { w: 104, h: 104 }, get = kind => boxed(page, jk, kind, sz.w, sz.h, false); const wt = await get('text'), pl = await get('plain'), fr = await get('front'); cover = wt && pl ? await rt(page, 'cover', th, wt, pl) : null; own = await rt(page, 'own', th, [pl, fr]); }
        const charm = ownOk(st, own);
        check(`O2 order window ${n} ${(c.piece || '').split('#')[1] || ''} ${f.face}: the thumbnail has the charm drawing in it`.replace('  ', ' '), 'thumb', charm, own ? ownEv(st, own) : 'no reference');
        check(`O3 order window ${n} ${(c.piece || '').split('#')[1] || ''} ${f.face}: the thumbnail shows that face's engraving ON the charm`.replace('  ', ' '), 'thumb', charm && !!cover && !cover.err && cover.nText >= 3 && cover.cover >= 0.5 && cover.stray <= 0.5, cover ? `${charm ? '' : 'no charm drawing in the picture; '}${cover.nText} cells of engraving in the app's own drawing of it, ${f2(cover.cover * 100)}% drawn in the thumbnail, stray ink ${f2(cover.stray * 100)}% (aligned ${cover.ncc}); the card says "${f.label}"` : 'no reference');
      }
      ci++;
    }
  }
}

/* ── part 3: the customer's own words are visible in the Engrave detail, and in the order window ────────────────────────────────────────────────── */
async function partSaid(w, made) {
  const page = w.page;
  const rd = async (n, face) => { await R.openEngraveDetail(w, n, { face }); await R.settle(page, 700); return page.evaluate(() => { const vis = e => { const b = e.getBoundingClientRect(); return b.width > 0 && b.height > 0 && !e.closest('[hidden]'); }; const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); const el = c && [...c.querySelectorAll('[data-said], .pvSaid, .owSaid')].find(vis); return { box: !!el, text: el ? el.textContent.replace(/\s+/g, ' ').trim() : '', title: !!(c && /What the customer wrote/i.test(c.innerText)) }; }); };
  for (const [n, face, expect] of [['back', 'back', 'Name One'], ['front', 'front', 'Name One'], ['both', 'back', 'Name One'], ['tag', 'back', '1. 27. 24']]) {
    const s = await rd(n, face);
    check(`N1 Engrave detail ${n}: the customer's own words box is visible with what they wrote`, 'note', s.box && s.text.includes(expect), `box ${s.box ? 'visible' : 'absent'}, title ${s.title}, text "${s.text.slice(0, 90)}"`);
  }
  for (const n of ['back', 'both']) {
    await R.openOrderWindow(w, n);
    const o = await page.evaluate(() => { const vis = e => { const b = e.getBoundingClientRect(); return b.width > 0 && b.height > 0 && !e.closest('[hidden]'); }; const el = [...document.querySelectorAll('#orderWin .owSaid')].find(vis); return { box: !!el, text: el ? el.textContent.replace(/\s+/g, ' ').trim() : '' }; });
    check(`N2 order window ${n}: "what the customer wrote" is visible`, 'note', o.box && /Name One/.test(o.text), `text "${o.text.slice(0, 90)}"`);
  }
}

/* ── part 4: overriding the system's choice asks for a reason first ─────────────────────────────────────────────────────────────────────────── */
async function partReason(w, made) {
  const page = w.page;
  await R.openEngraveDetail(w, 'back', { face: 'back' }); await R.settle(page, 900);
  await page.evaluate(() => window.__act.font('Pristina')); await R.settle(page, 500);
  await page.evaluate(() => { const b = document.querySelector('#egQueue .rvItem[data-kind=placement] [data-engp=save]'); if (b) b.click(); });
  await R.settle(page, 400);
  const probe = () => page.evaluate(() => {
    const vis = e => { const b = e.getBoundingClientRect(); return b.width > 0 && b.height > 0 && !e.closest('[hidden]'); };
    const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); if (!c) return { found: false };
    const row = B.orders.rows.find(r => r.order.receiptId === '4290000101'), ov = row && row.engravingOverride;
    // the prompt: ENGWHY's published markup (.engpWhy with .engpChip buttons and a text input), else any visible group with 3-4 quick buttons and a small text box that is not the words box nor the mode / position rows
    const groups = [...c.querySelectorAll('.engpWhy')].filter(vis);
    const generic = groups.length ? [] : [...c.querySelectorAll('[data-engp=reason], [data-reason], .engpReason, [role=dialog], [role=group], form, div')].filter(vis).filter(e => !e.querySelector('.engpWords') && !e.matches('.engpMode, .engpPos, .engpRow, .engpActs'));
    let best = null;
    for (const e of groups.concat(generic)) {
      const btns = [...e.querySelectorAll('button')].filter(vis).filter(b => !/^(save|cancel)$/i.test(b.textContent.trim()) && !b.matches('[data-engp=save],[data-engp=cancel],[data-engp=whySave],[data-engp=whyCancel],.engpSeg,.engpPb,.engpEmoji'));
      const inputs = [...e.querySelectorAll('input[type=text], input:not([type]), textarea')].filter(vis).filter(i => !i.matches('.engpWords'));
      if (btns.length >= 3 && btns.length <= 4 && inputs.length >= 1 && (!best || e.querySelectorAll('*').length < best.n)) best = { n: e.querySelectorAll('*').length, chips: btns.map(b => b.textContent.trim()), codes: btns.map(b => b.dataset.code || ''), inputs: inputs.length, cls: e.className };
    }
    return { found: true, prompt: best, saved: !!ov, reason: ov && ov.reason ? { code: ov.reason.code, text: ov.reason.text, by: ov.reason.by, at: !!ov.reason.at } : null, was: !!(ov && ov.was), text: c.querySelector('.engp') ? c.querySelector('.engp').innerText.replace(/\s+/g, ' ').slice(0, 200) : '' };
  });
  const p = await probe();
  await R.shot(w, 'LIVE-rt-reason-prompt');
  check('W1 changing the font and pressing Save asks for a reason first (3-4 quick buttons and a small text box), nothing saved yet', 'why', !!(p.prompt) && !p.saved, p.prompt ? `prompt with ${p.prompt.chips.length} quick buttons ${JSON.stringify(p.prompt.chips)} and ${p.prompt.inputs} text box(es); saved before the reason: ${p.saved}` : `no prompt; the choice was ${p.saved ? 'saved at once, with no reason asked' : 'not saved either'}; panel says: "${(p.text || '').slice(0, 120)}"`);
  // answering with a quick button saves the choice with the reason recorded (the in-memory fake site only: nothing real is written)
  let q = null;
  if (p.prompt) {
    await page.evaluate(() => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'), g = c.querySelector('.engpWhy') || c; const b = [...g.querySelectorAll('button')].find(x => { const r = x.getBoundingClientRect(); return r.width > 0 && !x.matches('[data-engp=save],[data-engp=cancel],[data-engp=whySave],[data-engp=whyCancel],.engpSeg,.engpPb,.engpEmoji') && !/^(save|cancel)$/i.test(x.textContent.trim()); }); if (b) b.click(); });
    await R.settle(page, 900); q = await probe();
  }
  check('W2 a quick button saves the changed font with the reason recorded (who, when, and what the system had chosen)', 'why', !!(q && q.saved && q.reason && q.reason.code && q.reason.by && q.reason.at && q.was), q ? `override saved ${q.saved}, reason ${JSON.stringify(q.reason)}, system's choice kept (was) ${q.was}` : 'there was no prompt to answer');
}

/* ── main ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
(async () => {
  const w = await R.world({ viewport: { width: 1440, height: 900 } });
  if (!w) { console.log('engrave-realtime: playwright-core not found, skipped'); process.exit(0); }
  let code = 0;
  try {
    const names = ONLY === 'detail' || ONLY === 'faces' ? ['back', 'front', 'both', 'refAnna', 'refMarck'] : ONLY === 'said' ? ['back', 'front', 'both', 'tag'] : ONLY === 'reason' ? ['back'] : ONLY === 'order' ? ['back', 'front', 'both', 'tag', 'discs', 'earrings'] : ['back', 'front', 'both', 'tag', 'discs', 'earrings', 'refAnna', 'refMarck'];
    const made = await R.build(w, names);
    const bad = Object.entries(made).filter(([n, v]) => n !== 'earrings' && !v.jobs.length);
    check('W0 the world: every order has the real jobs it should have, with a real fit', 'world', !bad.length && Object.entries(made).every(([n, v]) => v.jobs.length === (R.EXPECT_JOBS[n] == null ? 1 : R.EXPECT_JOBS[n]) && v.jobs.every(j => j.fitted && j.hasMask && j.hasView)), Object.entries(made).map(([n, v]) => `${n}:${v.jobs.map(j => j.face[0] + (j.slot ? j.slot : '') + '=' + (j.fitted ? j.fontKey : j.state)).join(',') || (v.never ? 'never' : 'none')}`).join(' '));
    await w.page.evaluate(INSTRUMENT); await w.page.evaluate(ACTS);
    if (!ONLY || ONLY === 'detail' || ONLY === 'faces') await partDetail(w, made);
    if (!ONLY || ONLY === 'order') await partOrder(w, made);
    if (!ONLY || ONLY === 'said') await partSaid(w, made);
    if (!ONLY || ONLY === 'reason') await partReason(w, made);
    check('Z0 no page errors', 'world', !w.errors.length, w.errors.slice(0, 3).join(' | ').slice(0, 300) || 'none');
  } catch (e) { console.error(e); code = 2; }
  finally { await w.close(); }
  const red = RESULTS.filter(r => !r.ok), by = {};
  for (const r of red) by[r.owner] = (by[r.owner] || 0) + 1;
  console.log(`\nengrave-realtime: ${RESULTS.length - red.length} PASS, ${red.length} FAIL`);
  for (const [o, n] of Object.entries(by)) console.log(`  red for ${o}: ${n}`);
  if (process.env.RT_JSON) fs.writeFileSync(process.env.RT_JSON, JSON.stringify(RESULTS, null, 1));
  process.exit(code || (red.length ? 1 : 0));
})();
