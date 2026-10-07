/* Options Studio, the history of a sheet (window.OptionsHistory). Pure rendering and filtering: no network, no storage, no timers.
   The modal (charm-nest-options-modal.js) loads the history (PartialSheets.history) and the one list of all partial sheets (PartialSheets.searchAll),
   and calls these:

     svg(history, { selected, width, frame, now, reveal, id })   the ORIGINAL physical sheet at true scale (100 x 50 mm frame), every earlier cut-away piece
                                                                 greyed (a slightly different grey per cut), the live remaining sheet in sheet colour, a dashed
                                                                 GREEN line for each cut with a numbered badge and a date / time / person label. Returns an SVG string.
     timeline(history, { selected, now })                        the list: "Original sheet 100 × 50 mm", then every cut, then what is left now. HTML string.
     view(history, o)                                            both side by side (drawing + legend | timeline), one wrapper to drop in a card.
     bind(root, history, { onSelect, selected })                 hover / click / keyboard between drawing and list inside `root`; returns { select, selected, destroy }.
     filter(items, query, { metal, status })                     the search over the loaded list of partial sheet cards.

   Painter's order, no polygon maths: the full sheet rectangle first, then each leftover L1 .. Ln on top (Lk lies inside Lk-1). The part of Lk-1 that Lk does not
   cover is the piece cut away by cut k and keeps the grey of cut k; the last leftover is the live sheet. The green line of cut k is the cut edge of Lk
   (PartialSheetsUI.edgesOf: the outline without the sheet's own border) minus the stretches that were already a cut edge of Lk-1 (those belong to an earlier cut).
   Motion: the layers fade in oldest to newest (about 0.9 s, the .ohReveal class), none under prefers-reduced-motion. */
(function (root) {
  'use strict';
  const doc = root.document || null;
  const GREEN = '#008974', TRAY = '#efe9de', SHEET = '#fffefb', EDGE = 'rgba(176,86,63,.9)';          // as charm-nest-partial-ui.js (the Nest preview's colours)
  const GREY_OLD = [239, 236, 231], GREY_NEW = [217, 213, 206];                                          // #efece7 (oldest cut) .. #d9d5ce (newest cut)
  const REF = { w: 100, h: 50 };                                                                         // the one 100 x 50 mm frame every sheet picture is drawn in
  const METAL = { rose: { code: 'RG', word: 'Rose Gold' }, gold10k: { code: '10K', word: '10K Gold' }, gold14k: { code: '14K', word: '14K Gold' } };
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const num = n => Math.round(n * 10) / 10;
  const r3 = n => Math.round(n * 1000) / 1000;
  const fmtInt = n => Math.round(+n || 0).toLocaleString('en-US');
  const plural = (n, one, many) => `${n} ${n === 1 ? one : many || one + 's'}`;
  const clip = (s, m) => (s.length > m ? s.slice(0, m - 1) + '…' : s);
  const PSU = () => root.PartialSheetsUI || null;                                                        // the picture helpers live there when it is loaded
  const reduced = () => { try { return !!(root.Motion && root.Motion.reduced && root.Motion.reduced()); } catch (_) { return false; } };

  /* ── words ── */
  /** A time as milliseconds, from what a record may hold (ms, numeric string, ISO text, Firestore timestamp); NaN when it is not there. */
  function toMs(v) {
    if (v == null || v === '' || v === false) return NaN;
    if (typeof v === 'number') return Number.isFinite(v) && v > 0 ? v : NaN;
    if (v instanceof Date) { const t = v.getTime(); return t > 0 ? t : NaN; }
    if (typeof v === 'string') { const s = v.trim(); if (/^\d+(\.\d+)?$/.test(s)) return toMs(+s); const t = Date.parse(s); return Number.isFinite(t) && t > 0 ? t : NaN; }
    if (typeof v === 'object') {
      if (typeof v.toMillis === 'function') return toMs(v.toMillis());
      const s = v.seconds != null ? v.seconds : v._seconds;
      if (s != null) return toMs(+s * 1000 + Math.floor((+(v.nanoseconds != null ? v.nanoseconds : v._nanoseconds) || 0) / 1e6));
    }
    return NaN;
  }
  /** "Today, 2:41 PM" · "Yesterday, 9:05 AM" · "Oct 5, 4:30 PM" · "Oct 5, 2025, 4:30 PM" (the same words as PartialSheetsUI.friendly). */
  function localFriendly(at, now) {
    const d = new Date(at), n = new Date(Number.isFinite(now) ? now : Date.now());
    const t = d.toLocaleTimeString(undefined, { hour: 'numeric', minute: '2-digit' });
    const day = x => new Date(x.getFullYear(), x.getMonth(), x.getDate()).getTime(), gap = Math.round((day(n) - day(d)) / 864e5);
    if (gap === 0) return `Today, ${t}`;
    if (gap === 1) return `Yesterday, ${t}`;
    return `${d.toLocaleDateString(undefined, d.getFullYear() === n.getFullYear() ? { month: 'short', day: 'numeric' } : { year: 'numeric', month: 'short', day: 'numeric' })}, ${t}`;
  }
  function friendly(at, now) {
    if (!Number.isFinite(at)) return 'Date not recorded';
    let s; try { const p = PSU(); s = p && p.friendly ? p.friendly(at, now) : localFriendly(at, now); } catch (_) { s = localFriendly(at, now); }
    return String(s).replace(/[  ]/g, ' ');
  }
  const greyOf = (k, n) => { const t = n > 1 ? (k - 1) / (n - 1) : .5; return '#' + GREY_OLD.map((a, i) => Math.round(a + (GREY_NEW[i] - a) * t).toString(16).padStart(2, '0')).join(''); };
  const metalWord = st => (METAL[st.metal] && METAL[st.metal].word) || (root.CN && root.CN.labelOf && st.metal ? root.CN.labelOf(st.metal) : '');

  /* ── geometry (rings are closed rings in real millimetres, top left origin, even-odd) ── */
  const okRing = r => Array.isArray(r) && r.length >= 3 && r.every(p => Array.isArray(p) && Number.isFinite(+p[0]) && Number.isFinite(+p[1]));
  function ringsOf(c) {   // null when the cut carries no outline at all (then the previous leftover stands); [] = nothing is left
    let r = c && (c.rings != null ? c.rings : c.outline);
    if (typeof r === 'string') { try { r = JSON.parse(r); } catch (_) { r = null; } }
    return Array.isArray(r) ? r.filter(okRing).map(ring => ring.map(p => [+p[0], +p[1]])) : null;
  }
  const pathOf = rings => rings.map(r => 'M' + r.map(p => r3(p[0]) + ' ' + r3(p[1])).join('L') + 'Z').join('');
  function localEdgesOf(rings, W, H) {   // a copy of PartialSheetsUI.edgesOf, so this file also works alone
    const e = .002, border = (a, b) => (Math.abs(a[0]) < e && Math.abs(b[0]) < e) || (Math.abs(a[0] - W) < e && Math.abs(b[0] - W) < e) || (Math.abs(a[1]) < e && Math.abs(b[1]) < e) || (Math.abs(a[1] - H) < e && Math.abs(b[1] - H) < e);
    let d = '';
    for (const r of rings) {
      let on = false;
      for (let i = 0; i < r.length; i++) {
        const a = r[i], b = r[(i + 1) % r.length];
        if (border(a, b)) { on = false; continue; }
        d += (on ? '' : 'M' + a[0] + ' ' + a[1]) + 'L' + b[0] + ' ' + b[1]; on = true;
      }
    }
    return d;
  }
  const edgesOf = (rings, W, H) => { const p = PSU(); try { if (p && typeof p.edgesOf === 'function') return p.edgesOf(rings, W, H); } catch (_) { /* the local copy */ } return localEdgesOf(rings, W, H); };
  /** A path string of M/L moves as a list of segments [[x0,y0],[x1,y1]]. */
  function parseD(d) {
    const out = [], re = /([ML])\s*(-?[\d.]+(?:e[-+]?\d+)?)[\s,]+(-?[\d.]+(?:e[-+]?\d+)?)/gi;
    let cur = null, m;
    while ((m = re.exec(d || ''))) { const p = [+m[2], +m[3]]; if (m[1].toUpperCase() === 'M' || !cur) cur = p; else { out.push([cur, p]); cur = p; } }
    return out;
  }
  /** `segs` without the stretches that lie on a segment of `cover` (a collinear overlap, 1-D). */
  function subtract(segs, cover, tol = .05) {
    const out = [];
    for (const [a, b] of segs) {
      const dx = b[0] - a[0], dy = b[1] - a[1], len = Math.hypot(dx, dy);
      if (len < 1e-6) continue;
      let parts = [[0, 1]];
      for (const [c, d] of cover) {
        const off = p => Math.abs((p[0] - a[0]) * dy - (p[1] - a[1]) * dx) / len;
        if (off(c) > tol || off(d) > tol) continue;
        const t0 = ((c[0] - a[0]) * dx + (c[1] - a[1]) * dy) / (len * len), t1 = ((d[0] - a[0]) * dx + (d[1] - a[1]) * dy) / (len * len), lo = Math.min(t0, t1), hi = Math.max(t0, t1);
        parts = parts.flatMap(([p, q]) => (hi <= p || lo >= q ? [[p, q]] : [...(lo > p ? [[p, lo]] : []), ...(hi < q ? [[hi, q]] : [])]));
      }
      for (const [p, q] of parts) if ((q - p) * len > .1) out.push([[a[0] + dx * p, a[1] + dy * p], [a[0] + dx * q, a[1] + dy * q]]);
    }
    return out;
  }
  function segsToD(segs) {
    let d = '', end = null;
    for (const [a, b] of segs) {
      d += end && Math.abs(end[0] - a[0]) < 1e-6 && Math.abs(end[1] - a[1]) < 1e-6 ? `L${r3(b[0])} ${r3(b[1])}` : `M${r3(a[0])} ${r3(a[1])}L${r3(b[0])} ${r3(b[1])}`;
      end = b;
    }
    return d;
  }
  const info = ([a, b]) => { const len = Math.hypot(b[0] - a[0], b[1] - a[1]); return { a, b, len, u: [(b[0] - a[0]) / len, (b[1] - a[1]) / len] }; };
  const ringArea = r => { let a = 0; for (let i = 0; i < r.length; i++) { const p = r[i], q = r[(i + 1) % r.length]; a += p[0] * q[1] - q[0] * p[1]; } return Math.abs(a) / 2; };
  const inRing = (p, r) => { let c = false; for (let i = 0, j = r.length - 1; i < r.length; j = i++) if ((r[i][1] > p[1]) !== (r[j][1] > p[1]) && p[0] < (r[j][0] - r[i][0]) * (p[1] - r[i][1]) / (r[j][1] - r[i][1]) + r[i][0]) c = !c; return c; };
  function areaOf(rings) {   // even-odd: a ring inside an odd number of others is a hole
    let t = 0;
    rings.forEach((r, i) => { const depth = rings.reduce((n, o, j) => n + (i !== j && inRing(r[0], o) ? 1 : 0), 0); t += (depth % 2 ? -1 : 1) * ringArea(r); });
    return Math.max(0, t);
  }

  /* ── the model: one pass over the cuts, oldest first (the history may be anything the server returned: it never throws) ── */
  function model(history, o = {}) {
    const h = history || {}, st = h.stock || {}, W = +st.wMm > 0 ? +st.wMm : REF.w, H = +st.hMm > 0 ? +st.hMm : REF.h, total = W * H;
    const now = Number.isFinite(+o.now) && o.now != null ? +o.now : undefined;
    const rect = `M0 0L${r3(W)} 0L${r3(W)} ${r3(H)}L0 ${r3(H)}Z`;
    let prev = [[[0, 0], [W, 0], [W, H], [0, H]]], prevArea = total, prevSegs = [];
    const cuts = (Array.isArray(h.cuts) ? h.cuts : []).map((c0, i) => {
      const c = c0 || {}, k = i + 1, own = ringsOf(c), rings = own || prev;
      const area = own ? areaOf(own) : Number.isFinite(+c.areaMm2) && c.areaMm2 != null ? +c.areaMm2 : prevArea;
      const segs = parseD(rings.length ? edgesOf(rings, W, H) : ''), mine = subtract(segs, prevSegs).map(info);
      const at = toMs(c.at), by = String(c.by == null ? '' : c.by).trim(), sheetName = String(c.sheetName || '').trim(), setName = String(c.setName || '').trim();
      const away = Math.max(0, prevArea - area);
      const out = { k, at, when: friendly(at, now), by, sheetName, setName, area, away, pct: total ? away / total * 100 : 0, layerD: pathOf(rings), segs: mine, lineD: segsToD(mine.map(s => [s.a, s.b])),
        pieceD: (k === 1 ? rect : pathOf(prev)) + pathOf(rings) };
      prev = rings; prevArea = area; prevSegs = segs;
      return out;
    });
    return { W, H, total, n: cuts.length, cuts, left: prevArea, st, now };
  }
  const selOf = (v, n) => { const k = Math.round(+v); return v != null && v !== '' && Number.isFinite(k) && k >= 1 && k <= n ? k : null; };
  const whatOf = c => [c.sheetName, c.setName].filter(Boolean).join(', ');

  /* ── the labels: each cut line gets a badge on it and a card of date, time and person beside it, in a spot nothing else uses ── */
  function placeTags(m, K, fw, fh) {
    const R = 11 * K, placed = [], samples = [], FS1 = 13, FS2 = 12, PADX = 9, PH = 42 * K;
    const box = (cx, cy, hw, hh) => ({ x0: cx - hw, y0: cy - hh, x1: cx + hw, y1: cy + hh });
    const ov = (a, b) => Math.max(0, Math.min(a.x1, b.x1) - Math.max(a.x0, b.x0)) * Math.max(0, Math.min(a.y1, b.y1) - Math.max(a.y0, b.y0));
    const area = b => (b.x1 - b.x0) * (b.y1 - b.y0), fr = { x0: 0, y0: 0, x1: fw, y1: fh };
    for (const c of m.cuts) for (const s of c.segs) for (let t = 0; t <= s.len; t += 1.2) samples.push([s.a[0] + s.u[0] * t, s.a[1] + s.u[1] * t]);
    const tags = [];
    for (const c of m.cuts) {
      if (!c.segs.length) continue;
      const l1 = clip(c.when, 28), l2 = clip(c.by || 'Person not recorded', 28);
      const pw = (Math.max(l1.length * FS1 * .6, l2.length * FS2 * .56) + 2 * PADX) * K, hw = pw / 2, hh = PH / 2;
      let best = null;
      search: for (const s of c.segs.slice().sort((a, b) => b.len - a.len).slice(0, 6)) for (const t of [.5, .28, .72]) {
        const ax = s.a[0] + s.u[0] * s.len * t, ay = s.a[1] + s.u[1] * s.len * t, n0 = [-s.u[1], s.u[0]];
        for (const su of [1, -1]) for (const sn of [1, -1]) {   // the card sits beside the badge, along the line, on either side of it
          const along = R + 3 * K + Math.abs(s.u[0]) * hw + Math.abs(s.u[1]) * hh, across = 4 * K + Math.abs(n0[0]) * hw + Math.abs(n0[1]) * hh;
          const pill = box(ax + s.u[0] * su * along + n0[0] * sn * across, ay + s.u[1] * su * along + n0[1] * sn * across, hw, hh), badge = box(ax, ay, R, R);
          let score = (area(pill) - ov(pill, fr)) * 4 + (area(badge) - ov(badge, fr)) * 4;   // outside the frame, over another card or badge, over a line
          for (const p of placed) score += ov(pill, p) + ov(badge, p);
          for (const q of samples) if (q[0] > pill.x0 && q[0] < pill.x1 && q[1] > pill.y0 && q[1] < pill.y1) score += 8;
          if (!best || score < best.score) best = { score, pill, badge, ax, ay };
          if (score === 0) break search;
        }
      }
      placed.push(best.pill, best.badge);
      tags.push({ c, l1, l2, FS1, FS2, PADX, pill: best.pill, ax: best.ax, ay: best.ay, R });
    }
    return tags;
  }

  /* ── the drawing ── */
  /** The history as one SVG string. `width` = about how many pixels wide it will be shown (the labels are sized for it; the SVG itself scales to its box). */
  function svg(history, o = {}) {
    ensureCss();
    const m = model(history, o), { W, H, n } = m, frame = o.frame !== false, fw = frame ? Math.max(REF.w, W) : W, fh = frame ? Math.max(REF.h, H) : H;
    const px = Math.max(240, +o.width || 720), K = fw / px, sel = selOf(o.selected, n), pfx = esc(o.id || 'oh'), step = n > 1 ? .45 / (n - 1) : 0;
    const on = k => (k === sel ? ' ohOn' : ''), dly = i => `style="--d:${r3(i * step)}s"`;
    let layers = n ? `<rect class="ohL ohBase" id="${pfx}-layer-0" data-cut="1" x="0" y="0" width="${r3(W)}" height="${r3(H)}" fill="${greyOf(1, n)}"/>`
      : `<rect class="ohL ohBase ohLive" id="${pfx}-layer-0" data-cut="live" x="0" y="0" width="${r3(W)}" height="${r3(H)}" fill="${SHEET}"/>`;
    m.cuts.forEach((c, i) => { if (c.layerD) layers += `<path class="ohL${c.k === n ? ' ohLive' : ''}" id="${pfx}-layer-${c.k}" data-cut="${c.k === n ? 'live' : c.k + 1}" d="${c.layerD}" fill="${c.k === n ? SHEET : greyOf(c.k + 1, n)}" fill-rule="evenodd" ${dly(i)}/>`; });
    const pieces = m.cuts.map(c => `<path class="ohPiece${on(c.k)}" id="${pfx}-piece-${c.k}" data-cut="${c.k}" d="${c.pieceD}" fill="${GREEN}" fill-opacity="0" fill-rule="evenodd"/>`).join('');
    const lines = m.cuts.filter(c => c.lineD).map(c => `<g class="ohLine${on(c.k)}" id="${pfx}-line-${c.k}" data-cut="${c.k}" ${dly(c.k - 1)}>`
      + `<path class="ohHalo" d="${c.lineD}" fill="none" stroke="#fff" stroke-opacity=".9" stroke-width="5.6" stroke-linejoin="round" vector-effect="non-scaling-stroke"/>`
      + `<path class="ohDash" d="${c.lineD}" fill="none" stroke="${GREEN}" stroke-width="2.6" stroke-dasharray="8 5" stroke-linejoin="round" vector-effect="non-scaling-stroke"/>`
      + `<path class="ohHit" d="${c.lineD}" fill="none" stroke="transparent" stroke-width="20" vector-effect="non-scaling-stroke" pointer-events="stroke"/></g>`).join('');
    const tags = placeTags(m, K, fw, fh).map(t => {
      const c = t.c, label = `Cut ${c.k}, ${c.when}, ${c.by || 'person not recorded'}`, x = t.pill.x0, y = t.pill.y0, pw = t.pill.x1 - t.pill.x0, ph = t.pill.y1 - t.pill.y0;
      return `<g class="ohTag${on(c.k)}" id="${pfx}-tag-${c.k}" data-cut="${c.k}" role="button" tabindex="0" aria-pressed="${c.k === sel}" aria-label="${esc(label)}" ${dly(c.k - 1)}><title>${esc(label)}</title>`
        + `<rect class="ohPill" x="${r3(x)}" y="${r3(y)}" width="${r3(pw)}" height="${r3(ph)}" rx="${r3(8 * K)}" ry="${r3(8 * K)}"/>`
        + `<text class="ohT1${Number.isFinite(c.at) ? '' : ' ohMiss'}" x="${r3(x + t.PADX * K)}" y="${r3(y + 18 * K)}" font-size="${r3(t.FS1 * K)}">${esc(t.l1)}</text>`
        + `<text class="ohT2${c.by ? '' : ' ohMiss'}" x="${r3(x + t.PADX * K)}" y="${r3(y + 34 * K)}" font-size="${r3(t.FS2 * K)}">${esc(t.l2)}</text>`
        + `<circle class="ohBadge" cx="${r3(t.ax)}" cy="${r3(t.ay)}" r="${r3(t.R)}"/>`
        + `<text class="ohBadgeN" x="${r3(t.ax)}" y="${r3(t.ay + 12 * K * .36)}" font-size="${r3(12 * K)}" text-anchor="middle">${c.k}</text></g>`;
    }).join('');
    const label = `Sheet history at true scale: ${num(W)} by ${num(H)} millimetres, ${n ? plural(n, 'cut') : 'never cut'}`;
    return `<svg class="ohSvg${o.reveal === false ? '' : ' ohReveal'}${reduced() ? ' ohStill' : ''}${sel ? ' ohHasSel' : ''}" viewBox="0 0 ${r3(fw)} ${r3(fh)}" style="aspect-ratio:${r3(fw)}/${r3(fh)}" role="group" aria-label="${esc(label)}">`
      + (frame ? `<rect class="ohTray" x="0" y="0" width="${r3(fw)}" height="${r3(fh)}" fill="${TRAY}"/>` : '')
      + `<g class="ohLayers">${layers}</g><g class="ohPieces">${pieces}</g>`
      + `<rect class="ohBorder" x=".25" y=".25" width="${r3(Math.max(0, W - .5))}" height="${r3(Math.max(0, H - .5))}" fill="none" stroke="${EDGE}" stroke-width="1" vector-effect="non-scaling-stroke" pointer-events="none"/>`
      + `<g class="ohLines">${lines}</g><g class="ohTags">${tags}</g></svg>`;
  }

  /* ── the list ── */
  const areaWords = c => `${fmtInt(c.away)} mm² cut away<span class="ohDot" aria-hidden="true">·</span>${(Math.round(c.pct * 10) / 10).toLocaleString('en-US')}% of the sheet`;
  function timeline(history, o = {}) {
    ensureCss();
    const m = model(history, o), { W, H, n } = m, sel = selOf(o.selected, n), word = metalWord(m.st);
    const swatch = c => `<span class="ohSw" style="--sw:${c}" aria-hidden="true"></span>`;
    let html = `<li class="ohStep"><div class="ohItem ohStatic"><span class="ohNum ohNumO" aria-hidden="true"></span><span class="ohBody"><span class="ohRow1"><b class="ohWhen">Original sheet ${num(W)} × ${num(H)} mm</b></span>`
      + `<span class="ohRow3">${word ? esc(word) + '<span class="ohDot" aria-hidden="true">·</span>' : ''}${fmtInt(m.total)} mm² to start with</span></span>${swatch(SHEET)}</div></li>`;
    for (const c of m.cuts) {
      const what = whatOf(c), name = `Cut ${c.k}, ${c.when}, ${c.by || 'person not recorded'}${what ? ', ' + what : ''}, ${fmtInt(c.away)} square millimetres cut away`;
      html += `<li class="ohStep"><button type="button" class="ohItem${c.k === sel ? ' ohOn' : ''}" data-cut="${c.k}" aria-pressed="${c.k === sel}" aria-label="${esc(name)}">`
        + `<span class="ohNum" aria-hidden="true">${c.k}</span><span class="ohBody">`
        + `<span class="ohRow1"><span class="ohKick">Cut ${c.k}</span><b class="ohWhen${Number.isFinite(c.at) ? '' : ' ohMiss'}">${esc(c.when)}</b></span>`
        + `<span class="ohRow2"><span class="ohWho${c.by ? '' : ' ohMiss'}">${esc(c.by || 'Person not recorded')}</span><span class="ohDot" aria-hidden="true">·</span><span class="ohWhat${what ? '' : ' ohMiss'}">${esc(what || 'Sheet not recorded')}</span></span>`
        + `<span class="ohRow3">${areaWords(c)}</span></span>${swatch(greyOf(c.k, n))}</button></li>`;
    }
    html += n ? `<li class="ohStep"><div class="ohItem ohStatic ohNow"><span class="ohNum ohNumNow" aria-hidden="true"></span><span class="ohBody"><span class="ohRow1"><b class="ohWhen">Left on the sheet now</b></span>`
      + `<span class="ohRow3">${fmtInt(m.left)} mm²<span class="ohDot" aria-hidden="true">·</span>${(Math.round(m.left / (m.total || 1) * 1000) / 10).toLocaleString('en-US')}% of the sheet${m.st.ownerSheetName ? `<span class="ohDot" aria-hidden="true">·</span>on ${esc(m.st.ownerSheetName)}` : ''}</span></span>${swatch(SHEET)}</div></li>`
      : `<li class="ohStep"><div class="ohItem ohStatic ohNoCuts"><span class="ohNum ohNumNow" aria-hidden="true"></span><span class="ohBody"><span class="ohRow1"><b class="ohWhen">No cut has been made on this sheet yet</b></span></span></div></li>`;
    return `<ol class="ohTl" aria-label="History of this sheet, oldest first">${html}</ol>`;
  }
  /** The drawing with its legend, beside the timeline: one wrapper for the History card. */
  function view(history, o = {}) {
    const n = (history && Array.isArray(history.cuts) ? history.cuts : []).length;
    return `<div class="ohView"><div class="ohFigure"><div class="ohStage">${svg(history, o)}</div>`
      + `<div class="ohLegend" aria-hidden="true"><span class="ohKey"><i class="ohKeyLive"></i>Sheet left now</span><span class="ohKey"><i class="ohKeyCut" style="--g1:${greyOf(1, 2)};--g2:${greyOf(2, 2)}"></i>Cut away, older to newer</span><span class="ohKey"><i class="ohKeyLine"></i>Cut line with its date, time and person</span></div></div>`
      + `<div class="ohSide"><div class="ohSideHead"><b>Timeline</b><span>${n ? plural(n, 'cut') : 'never cut'}</span></div>${timeline(history, o)}</div></div>`;
  }

  /* ── wiring: hover, click and keys between the drawing and the list (classes only: nothing moves) ── */
  function bind(rootEl, history, o = {}) {
    const none = { select() {}, selected: () => null, destroy() {} };
    if (!rootEl || !rootEl.addEventListener) return none;
    if (typeof rootEl._ohOff === 'function') rootEl._ohOff();
    const cuts = history && Array.isArray(history.cuts) ? history.cuts : [], n = cuts.length;
    let sel = selOf(o.selected, n), hot = null;
    const cutOf = t => { const el = t && t.closest ? t.closest('[data-cut]') : null; if (!el || !rootEl.contains(el)) return null; const k = +el.getAttribute('data-cut'); return Number.isInteger(k) && k >= 1 && k <= n ? k : null; };
    const paint = () => {
      rootEl.querySelectorAll('[data-cut]').forEach(el => { const k = +el.getAttribute('data-cut'); el.classList.toggle('ohOn', k === sel); el.classList.toggle('ohHot', k === hot); if (el.hasAttribute('aria-pressed')) el.setAttribute('aria-pressed', k === sel ? 'true' : 'false'); });
      [rootEl, ...rootEl.querySelectorAll('.ohSvg')].forEach(el => el.classList && el.classList.toggle('ohHasSel', sel != null));
    };
    const reveal = k => {   // keep the picked cut in view in a list that scrolls
      const b = rootEl.querySelector(`.ohItem[data-cut="${k}"]`), box = b && b.closest('.ohSide');
      if (!b || !box || box.scrollHeight <= box.clientHeight + 2) return;
      const r = b.getBoundingClientRect(), c = box.getBoundingClientRect();
      if (r.top < c.top) box.scrollTop -= c.top - r.top + 8; else if (r.bottom > c.bottom) box.scrollTop += r.bottom - c.bottom + 8;
    };
    const set = (k, silent) => { sel = k; paint(); if (!silent && typeof o.onSelect === 'function') { try { o.onSelect(k, k ? cuts[k - 1] : null); } catch (_) { /* a handler's own trouble stays its own */ } } };
    const onClick = e => { const k = cutOf(e.target); if (k == null) return; set(sel === k ? null : k); if (!e.target.closest('.ohItem')) reveal(k); };
    const onKey = e => { if ((e.key === 'Enter' || e.key === ' ') && e.target && e.target.tagName && e.target.tagName.toLowerCase() === 'g') { e.preventDefault(); onClick(e); } };
    const setHot = k => { if (k !== hot) { hot = k; paint(); } };
    const onOver = e => setHot(cutOf(e.target)), onLeave = () => setHot(null), onIn = e => setHot(cutOf(e.target)), onOut = () => setHot(null);
    const ev = [['click', onClick], ['keydown', onKey], ['mouseover', onOver], ['mouseleave', onLeave], ['focusin', onIn], ['focusout', onOut]];
    ev.forEach(([t, f]) => rootEl.addEventListener(t, f));
    const destroy = () => { ev.forEach(([t, f]) => rootEl.removeEventListener(t, f)); if (rootEl._ohOff === destroy) rootEl._ohOff = null; };
    rootEl._ohOff = destroy;
    paint();
    return { select: (k, silent) => set(selOf(k, n), silent), selected: () => sel, destroy };
  }

  /* ── the search: one loaded list, filtered in the browser ── */
  const MONTHS = ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december'];
  const DAYS = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];
  const MON = 'jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|june?|july?|aug(?:ust)?|sept?(?:ember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?';
  const monthOf = s => MONTHS.findIndex(x => x.slice(0, 3) === String(s).toLowerCase().slice(0, 3)) + 1;
  const alnum = v => String(v == null ? '' : v).toLowerCase().replace(/[^a-z0-9]/g, '');
  function metalKey(v) { const s = alnum(v); if (!s || s === 'all') return ''; if (s === 'rg' || s === 'rose' || s === 'rosegold') return 'rose'; if (s === '10k' || s === 'gold10k' || s === '10kgold') return 'gold10k'; if (s === '14k' || s === 'gold14k' || s === '14kgold') return 'gold14k'; return s; }
  function statusKey(v) { const s = alnum(v); return s === 'all' ? '' : s; }
  const STATUS_WORDS = { available: 'available', inuse: 'in use inuse', used: 'used', discarded: 'discarded' };
  function dateWords(ms, now) {
    if (!Number.isFinite(ms)) return '';
    const d = new Date(ms), mo = d.getMonth(), day = d.getDate(), y = d.getFullYear(), p2 = x => String(x).padStart(2, '0'), full = MONTHS[mo], abbr = full.slice(0, 3);
    return [full, abbr, abbr === 'sep' ? 'sept' : '', `${abbr} ${day}`, `${full} ${day}`, `${day} ${abbr}`, `${y}-${p2(mo + 1)}-${p2(day)}`, `${mo + 1}/${day}/${y}`, String(y), DAYS[d.getDay()], friendly(ms, now).toLowerCase()].join(' ');
  }
  const dateOf = (c, now) => [c.cutAt, c.lastUsedAt, c.usedAt, c.inUseAt].map(toMs).filter(Number.isFinite);
  const HAY = new WeakMap();
  function hayOf(c, now) {
    const day = new Date(Number.isFinite(now) ? now : Date.now()).toDateString(), hit = HAY.get(c);
    if (hit && hit.day === day) return hit;
    const w = +c.wMm || (c.bboxMm && +c.bboxMm.w) || 0, h = +c.hMm || (c.bboxMm && +c.bboxMm.h) || 0, sw = +c.sheetWMm || 0, sh = +c.sheetHMm || 0, mk = metalKey(c.metal) || metalKey(c.code), mw = METAL[mk] || {}, st = statusKey(c.status || 'available');
    const sizes = [[w, h], [sw, sh]].filter(s => s[0] && s[1]).map(([a, b]) => `${num(a)} x ${num(b)} mm ${num(a)}x${num(b)} ${num(a)} ${num(b)} ${(+a).toFixed(2)} ${(+b).toFixed(2)}`);
    const text = [c.sheetName, c.name, c.sourceSheet, c.sourceSet, c.cutBy, c.lastUsedBy, c.lastUsedSheet, c.usedBySheetName, c.usedBy, c.inUseBySheetName, c.inUseBy, c.metal, c.code, mw.word, mw.code, STATUS_WORDS[st] || st, ...sizes].map(x => String(x == null ? '' : x).toLowerCase().replace('×', 'x')).join(' ');
    const dates = dateOf(c, now), out = { day, text, dtext: dates.map(ms => dateWords(ms, now)).join(' '), dates: dates.map(ms => { const d = new Date(ms); return { y: d.getFullYear(), m: d.getMonth() + 1, d: d.getDate() }; }), mk, st };
    HAY.set(c, out);
    return out;
  }
  /** The date phrases of a query ("oct 5", "5 oct 2026", "2026-10-05", "10/5") become exact day constraints; what is left are plain words. */
  function parseQuery(q) {
    const dates = [], phrases = [];
    q = q.replace(/"([^"]+)"/g, (m, t) => { phrases.push({ t: t.trim(), whole: false }); return ' '; });   // "exact words"
    q = q.replace(/\b(sheet|set)s?\s+(\d+)\b/g, (m, w, n) => { phrases.push({ t: `${w} ${n}`, whole: true }); return ' '; });   // "sheet 9" is that name, not "sheet" and any 9
    const take = (re, f) => { q = q.replace(re, (...a) => { const c = f(a); if (!c) return a[0]; dates.push(c); return ' '; }); };
    const ok = c => (!c.m || (c.m >= 1 && c.m <= 12)) && (!c.d || (c.d >= 1 && c.d <= 31)) ? c : null;
    take(/\b(\d{4})-(\d{1,2})(?:-(\d{1,2}))?\b/g, a => ok({ y: +a[1], m: +a[2], d: a[3] ? +a[3] : 0 }));
    take(/\b(\d{1,2})\/(\d{1,2})(?:\/(\d{2,4}))?\b/g, a => ok({ m: +a[1], d: +a[2], y: a[3] ? (a[3].length === 2 ? 2000 + +a[3] : +a[3]) : 0 }));
    take(new RegExp(`\\b(${MON})\\.?\\s+(\\d{1,2})(?:st|nd|rd|th)?(?:,?\\s*(\\d{4}))?\\b`, 'g'), a => ok({ m: monthOf(a[1]), d: +a[2], y: a[3] ? +a[3] : 0 }));
    take(new RegExp(`\\b(\\d{1,2})(?:st|nd|rd|th)?\\s+(${MON})\\b\\.?(?:,?\\s*(\\d{4}))?\\b`, 'g'), a => ok({ m: monthOf(a[2]), d: +a[1], y: a[3] ? +a[3] : 0 }));
    take(new RegExp(`\\b(${MON})\\.?,?\\s+(\\d{4})\\b`, 'g'), a => ({ m: monthOf(a[1]), y: +a[2], d: 0 }));
    return { dates, phrases: phrases.filter(p => p.t), words: q.split(/[\s,;]+/).filter(Boolean) };
  }
  /** items (the cards of PartialSheets.searchAll) -> the ones that match; same order. `metal`: all | rose | gold10k | gold14k (or RG, 10K, 14K); `status`: all | available | inUse | used | discarded. */
  function filter(items, query, opt = {}) {
    const list = Array.isArray(items) ? items : [], now = Number.isFinite(+opt.now) && opt.now != null ? +opt.now : undefined;
    const mk = metalKey(opt.metal), sk = statusKey(opt.status), q = parseQuery(String(query == null ? '' : query).toLowerCase().replace(/×/g, 'x').trim());
    return list.filter(c => {
      if (!c) return false;
      const h = hayOf(c, now);
      if (mk && h.mk !== mk) return false;
      if (sk && h.st !== sk) return false;
      if (!q.dates.every(f => h.dates.some(d => (!f.y || d.y === f.y) && (!f.m || d.m === f.m) && (!f.d || d.d === f.d)))) return false;
      if (!q.phrases.every(p => (p.whole ? new RegExp(`\\b${p.t}\\b`).test(h.text) : h.text.includes(p.t) || h.dtext.includes(p.t)))) return false;
      return q.words.every(wd => h.text.includes(wd) || (/[a-z]/.test(wd) && h.dtext.includes(wd)) || (/^(19|20)\d\d$/.test(wd) && h.dates.some(d => d.y === +wd)));   // (a bare number is a name or a size, not a piece of a date)
    });
  }

  /* ── styles (the app's own tokens, with fallbacks so the file also looks right alone) ── */
  const CSS = `
.ohView,.ohTl,.ohSvg,.ohLegend,.ohStage,.ohSide{--oh-ink:var(--ink,#1c1a17);--oh-ink70:var(--ink70,#5b554c);--oh-ink45:var(--ink45,#938c80);--oh-ink25:var(--ink25,#c4bdb0);--oh-line:var(--line,#e4ddd0);--oh-card:var(--card,#fffefb);--oh-card2:var(--card2,#faf7f1);--oh-gold:var(--gold,#a9823f);--oh-goldLine:var(--goldLine,#e3d3a6);--oh-green:${GREEN}}
.ohView{display:grid;grid-template-columns:minmax(0,1.8fr) minmax(290px,1fr);gap:24px;align-items:start;min-width:0;font-family:var(--sans,system-ui,sans-serif);color:var(--oh-ink)}
@media (max-width:900px){.ohView{grid-template-columns:minmax(0,1fr)}}
.ohFigure{display:grid;gap:14px;min-width:0}
.ohStage{border:1px solid var(--oh-line);border-radius:14px;background:var(--oh-card2);padding:16px;min-width:0}
.ohSvg{display:block;width:100%;height:auto;border-radius:6px;box-shadow:0 1px 2px rgba(30,24,16,.1),0 10px 26px rgba(30,24,16,.1);overflow:hidden;user-select:none;-webkit-user-select:none}
.ohPiece{cursor:pointer;transition:fill-opacity .16s}
.ohPiece:hover,.ohPiece.ohHot{fill-opacity:.15}
.ohPiece.ohOn{fill-opacity:.26}
.ohLine{transition:opacity .16s;cursor:pointer}
.ohLine .ohDash{transition:stroke-width .12s}
.ohLine.ohHot .ohDash,.ohLine.ohOn .ohDash{stroke-width:3.8px}
.ohHit{pointer-events:stroke}
.ohTag{cursor:pointer;outline:none;transition:opacity .16s}
.ohPill{fill:rgba(255,254,251,.97);stroke:var(--oh-green);stroke-width:1.5px;vector-effect:non-scaling-stroke;transition:stroke-width .12s,fill .12s}
.ohTag:hover .ohPill,.ohTag.ohHot .ohPill{stroke-width:2.6px}
.ohTag.ohOn .ohPill{fill:#e6f3f0;stroke-width:2.8px}
.ohTag:focus-visible .ohPill{stroke:var(--oh-gold);stroke-width:3px}
.ohT1,.ohT2{font-family:var(--sans,system-ui,sans-serif);pointer-events:none}
.ohT1{font-weight:650;fill:var(--oh-ink)}
.ohT2{font-weight:500;fill:var(--oh-ink70)}
.ohT1.ohMiss,.ohT2.ohMiss{font-style:italic;fill:var(--oh-ink45)}
.ohBadge{fill:var(--oh-ink);stroke:#fff;stroke-width:2px;vector-effect:non-scaling-stroke;transition:fill .12s}
.ohTag:hover .ohBadge,.ohTag.ohHot .ohBadge,.ohTag.ohOn .ohBadge{fill:var(--oh-green)}
.ohBadgeN{font-family:var(--mono,ui-monospace,Menlo,Consolas,monospace);font-weight:700;fill:#fff;pointer-events:none}
.ohHasSel .ohLine:not(.ohOn):not(.ohHot){opacity:.42}
.ohHasSel .ohTag:not(.ohOn):not(.ohHot){opacity:.62}
.ohReveal .ohL:not(.ohBase){animation:ohFade .45s ease backwards;animation-delay:var(--d,0s)}
.ohReveal .ohLine,.ohReveal .ohTag{animation:ohRise .45s cubic-bezier(.3,.1,.2,1) backwards;animation-delay:var(--d,0s)}
@keyframes ohFade{from{opacity:0}to{opacity:1}}
@keyframes ohRise{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:none}}
.ohStill .ohL,.ohStill .ohLine,.ohStill .ohTag{animation:none!important}
.ohLegend{display:flex;flex-wrap:wrap;gap:8px 20px;font-size:12px;color:var(--oh-ink70);padding:0 4px}
.ohKey{display:inline-flex;align-items:center;gap:8px}
.ohKey i{display:inline-block;flex:none;border-radius:4px}
.ohKeyLive{width:22px;height:14px;background:${SHEET};border:1px solid ${EDGE}}
.ohKeyCut{width:22px;height:14px;background:linear-gradient(90deg,var(--g1),var(--g2));border:1px solid rgba(30,24,16,.16)}
.ohKeyLine{width:26px;height:0;border-top:2.5px dashed var(--oh-green);border-radius:0}
.ohSide{min-width:0;max-height:min(680px,74vh);overflow:auto;overscroll-behavior:contain;padding:0 4px 4px 0;font-family:var(--sans,system-ui,sans-serif)}
.ohSideHead{display:flex;align-items:baseline;justify-content:space-between;gap:10px;margin:0 0 10px;font-size:10px;letter-spacing:.11em;text-transform:uppercase;font-weight:600;color:var(--oh-ink45)}
.ohSideHead b{font-weight:600;color:var(--oh-ink70)}
.ohTl{list-style:none;margin:0;padding:0;display:grid;gap:4px;position:relative;font-family:var(--sans,system-ui,sans-serif);color:var(--oh-ink)}
.ohTl::before{content:"";position:absolute;left:27px;top:22px;bottom:22px;width:0;border-left:2px dashed color-mix(in srgb,var(--oh-green) 45%,transparent)}
.ohStep{position:relative;margin:0;padding:0}
.ohItem{display:grid;grid-template-columns:30px minmax(0,1fr) auto;align-items:start;gap:13px;width:100%;box-sizing:border-box;margin:0;padding:11px 12px;text-align:left;font:inherit;color:inherit;background:transparent;border:1px solid transparent;border-radius:12px;transition:background-color .13s,border-color .13s,box-shadow .13s}
button.ohItem{cursor:pointer}
button.ohItem:hover,button.ohItem.ohHot{background:var(--oh-card2);border-color:var(--oh-line)}
button.ohItem.ohOn{background:var(--oh-card);border-color:var(--oh-ink);box-shadow:0 0 0 2px var(--oh-goldLine)}
button.ohItem:focus-visible{outline:2px solid var(--oh-gold);outline-offset:2px}
.ohNum{position:relative;z-index:1;display:inline-flex;align-items:center;justify-content:center;width:30px;height:30px;box-sizing:border-box;border-radius:50%;background:var(--oh-ink);color:#fff;font:700 12.5px/1 var(--mono,ui-monospace,Menlo,Consolas,monospace);box-shadow:0 0 0 3px var(--oh-card,#fffefb);transition:background-color .13s}
button.ohItem:hover .ohNum,button.ohItem.ohHot .ohNum,button.ohItem.ohOn .ohNum{background:var(--oh-green)}
.ohNumO,.ohNumNow{background:var(--oh-card);border:2px solid var(--oh-ink25);box-shadow:0 0 0 3px var(--oh-card,#fffefb)}
.ohNumNow{border-color:var(--oh-green)}
.ohBody{display:grid;gap:2px;min-width:0}
.ohRow1{display:flex;align-items:baseline;flex-wrap:wrap;gap:2px 9px}
.ohKick{font-size:10px;letter-spacing:.11em;text-transform:uppercase;font-weight:600;color:var(--oh-ink45)}
.ohWhen{font-size:14.5px;font-weight:650;color:var(--oh-ink)}
.ohRow2{font-size:12.5px;line-height:1.45;color:var(--oh-ink70)}
.ohWho{font-weight:600;color:var(--oh-ink)}
.ohRow3{font-size:11.5px;line-height:1.5;color:var(--oh-ink45);font-variant-numeric:tabular-nums}
.ohDot{margin:0 6px;color:var(--oh-ink25)}
.ohMiss{font-style:italic;font-weight:500;color:var(--oh-ink45)}
.ohSw{display:block;width:24px;height:16px;margin-top:6px;border-radius:5px;background:var(--sw);border:1px solid rgba(30,24,16,.16);box-sizing:border-box}
.ohNoCuts .ohWhen{font-weight:600;color:var(--oh-ink70)}
@media (prefers-reduced-motion:reduce){.ohReveal .ohL,.ohReveal .ohLine,.ohReveal .ohTag{animation:none!important}.ohPiece,.ohLine,.ohTag,.ohPill,.ohBadge,.ohItem,.ohNum,.ohLine .ohDash{transition:none}}`;
  function ensureCss() {
    if (!doc || !doc.head || doc.getElementById('optionsHistoryCss')) return;
    const s = doc.createElement('style');
    s.id = 'optionsHistoryCss';
    s.textContent = CSS;
    doc.head.appendChild(s);
  }
  ensureCss();

  const api = { svg, timeline, view, bind, filter, greyOf, friendly, ensureCss, css: CSS, GREEN, SHEET, TRAY };
  root.OptionsHistory = api;
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
})(typeof window !== 'undefined' ? window : globalThis);
