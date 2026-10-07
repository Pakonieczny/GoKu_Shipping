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
  const reduced = () => { try { if (root.Motion && root.Motion.reduced) return !!root.Motion.reduced(); return !!(root.matchMedia && root.matchMedia('(prefers-reduced-motion: reduce)').matches); } catch (_) { return false; } };

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
  /** "Oct 5, 8:57 AM" (this year) · "Oct 5, 2025, 8:57 AM": the short form written on a green line (always the day, never "Today"). */
  function shortDate(at, now) {
    if (!Number.isFinite(at)) return 'Date not recorded';
    const d = new Date(at), n = new Date(Number.isFinite(now) ? now : Date.now());
    const t = d.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' });
    const day = d.toLocaleDateString('en-US', d.getFullYear() === n.getFullYear() ? { month: 'short', day: 'numeric' } : { year: 'numeric', month: 'short', day: 'numeric' });
    return `${day}, ${t}`.replace(/[  ]/g, ' ');
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
    const now = Number.isFinite(+o.now) && o.now != null ? +o.now : undefined, short = o.dates === 'short' || o.compact === true, when = at => (short ? shortDate(at, now) : friendly(at, now));
    const rect = `M0 0L${r3(W)} 0L${r3(W)} ${r3(H)}L0 ${r3(H)}Z`, partial = h.partial === true;
    const stamp = x => (x && typeof x === 'object' ? { at: toMs(x.at), by: String(x.by == null ? '' : x.by).trim(), reason: String(x.reason == null ? '' : x.reason).trim() } : null);
    const made = stamp(h.made), deleted = stamp(h.deleted), kind = st.kind === 'new' || h.kind === 'new' ? 'new' : '';
    let prev = [[[0, 0], [W, 0], [W, H], [0, H]]], prevArea = total, prevSegs = [];
    const cuts = (Array.isArray(h.cuts) ? h.cuts : []).map((c0, i) => {
      const c = c0 || {}, k = i + 1, own = ringsOf(c), rings = own || prev;
      const area = own ? areaOf(own) : Number.isFinite(+c.areaMm2) && c.areaMm2 != null ? +c.areaMm2 : prevArea;
      const segs = parseD(rings.length ? edgesOf(rings, W, H) : ''), mine = subtract(segs, prevSegs).map(info);
      const at = toMs(c.at), by = String(c.by == null ? '' : c.by).trim(), sheetName = String(c.sheetName || '').trim(), setName = String(c.setName || '').trim();
      const away = Math.max(0, prevArea - area);
      const rev = Math.floor(+c.revision), label = partial && Number.isFinite(rev) && rev >= 1 ? rev : k;   // an incomplete history keeps the real cut numbers
      const out = { k, label, at, when: when(at), by, sheetName, setName, area, away, pct: total ? away / total * 100 : 0, layerD: pathOf(rings), segs: mine, lineD: segsToD(mine.map(s => [s.a, s.b])),
        pieceD: (k === 1 ? rect : pathOf(prev)) + pathOf(rings) };
      prev = rings; prevArea = area; prevSegs = segs;
      return out;
    });
    return { W, H, total, n: cuts.length, cuts, left: prevArea, st, now, made, deleted, kind, partial, when };
  }
  const selOf = (v, n) => { const k = Math.round(+v); return v != null && v !== '' && Number.isFinite(k) && k >= 1 && k <= n ? k : null; };
  const whatOf = c => [c.sheetName, c.setName].filter(Boolean).join(', ');

  /* ── the labels: each cut line gets a badge on it and a card of date, time and person beside it, in a spot nothing else uses ── */
  function placeTags(m, K, fw, fh, compact, small) {   // small: a thumbnail on a phone, the labels shrink so four of them still fit
    const R = (small ? 9 : 11) * K, placed = [], samples = [], FS1 = compact ? (small ? 10.5 : 13.5) : 13, FS2 = 12, PADX = small ? 6 : 9, PH = (compact ? (small ? 21 : 27) : 42) * K;
    const box = (cx, cy, hw, hh) => ({ x0: cx - hw, y0: cy - hh, x1: cx + hw, y1: cy + hh });
    const ov = (a, b) => Math.max(0, Math.min(a.x1, b.x1) - Math.max(a.x0, b.x0)) * Math.max(0, Math.min(a.y1, b.y1) - Math.max(a.y0, b.y0));
    const area = b => (b.x1 - b.x0) * (b.y1 - b.y0), fr = { x0: 0, y0: 0, x1: fw, y1: fh };
    for (const c of m.cuts) for (const s of c.segs) for (let t = 0; t <= s.len; t += 1.2) samples.push([s.a[0] + s.u[0] * t, s.a[1] + s.u[1] * t]);
    const tags = [], spots = compact ? [.5, .3, .7, .14, .86] : [.5, .28, .72];
    // the longest cut first: it has the most room; the short ones then take what is left
    for (const c of m.cuts.slice().sort((a, b) => b.segs.reduce((t, s) => t + s.len, 0) - a.segs.reduce((t, s) => t + s.len, 0))) {
      if (!c.segs.length) continue;
      const l1 = clip(c.when, 28), l2 = compact ? '' : clip(c.by || 'Person not recorded', 28);
      const pw = (Math.max(l1.length * FS1 * .6, l2.length * FS2 * .56) + 2 * PADX) * K, hw = pw / 2, hh = PH / 2;
      let best = null;
      search: for (const s of c.segs.slice().sort((a, b) => b.len - a.len).slice(0, compact ? 8 : 6)) for (const t of spots) {
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
      tags.push({ c, l1, l2, FS1, FS2, PADX, pill: best.pill, ax: best.ax, ay: best.ay, R, compact, small });
    }
    return tags.sort((a, b) => a.c.k - b.c.k);
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
    const compact = o.compact === true;   // the card's thumbnail: the whole drawing is ONE button, so nothing inside it is a control, and a label is just the short date
    const tags = placeTags(m, K, fw, fh, compact, compact && px < 420).map(t => {
      const c = t.c, label = `Cut ${c.label}, ${c.when}, ${c.by || 'person not recorded'}`, x = t.pill.x0, y = t.pill.y0, pw = t.pill.x1 - t.pill.x0, ph = t.pill.y1 - t.pill.y0;
      const open = compact ? `<g class="ohTag" id="${pfx}-tag-${c.k}" data-cut="${c.k}" aria-hidden="true">`
        : `<g class="ohTag${on(c.k)}" id="${pfx}-tag-${c.k}" data-cut="${c.k}" role="button" tabindex="0" aria-pressed="${c.k === sel}" aria-label="${esc(label)}" ${dly(c.k - 1)}><title>${esc(label)}</title>`;
      const ty1 = compact ? y + ph / 2 + t.FS1 * K * .34 : y + 18 * K;
      return open + `<rect class="ohPill" x="${r3(x)}" y="${r3(y)}" width="${r3(pw)}" height="${r3(ph)}" rx="${r3((compact ? 7 : 8) * K)}" ry="${r3((compact ? 7 : 8) * K)}"/>`
        + `<text class="ohT1${Number.isFinite(c.at) ? '' : ' ohMiss'}" x="${r3(x + t.PADX * K)}" y="${r3(ty1)}" font-size="${r3(t.FS1 * K)}">${esc(t.l1)}</text>`
        + (compact ? '' : `<text class="ohT2${c.by ? '' : ' ohMiss'}" x="${r3(x + t.PADX * K)}" y="${r3(y + 34 * K)}" font-size="${r3(t.FS2 * K)}">${esc(t.l2)}</text>`)
        + `<circle class="ohBadge" cx="${r3(t.ax)}" cy="${r3(t.ay)}" r="${r3(t.R)}"/>`
        + `<text class="ohBadgeN" x="${r3(t.ax)}" y="${r3(t.ay + (t.small ? 10 : 12) * K * .36)}" font-size="${r3((t.small ? 10 : 12) * K)}" text-anchor="middle">${c.label}</text></g>`;
    }).join('');
    // a deleted sheet is stamped across, in red and quiet; a sheet that was made and never cut says it is new (no cut lines to draw)
    const sx = r3(W / 2), sy = r3(H / 2), SF = r3(Math.min(W * .095, H * .19));
    const stamp = m.deleted ? `<g class="ohcStamp" transform="rotate(-12 ${sx} ${sy})" pointer-events="none" aria-hidden="true"><rect class="ohcStampBox" x="${r3(W / 2 - SF * 3.1)}" y="${r3(H / 2 - SF * .85)}" width="${r3(SF * 6.2)}" height="${r3(SF * 1.7)}" rx="${r3(SF * .22)}"/><text class="ohcStampT" x="${sx}" y="${r3(H / 2 + SF * .36)}" font-size="${SF}" text-anchor="middle" letter-spacing="${r3(SF * .12)}">DELETED</text></g>`
      : !n && (m.kind === 'new' || m.made) ? `<g class="ohcNew" pointer-events="none" aria-hidden="true"><rect class="ohcNewBox" x="${r3(W / 2 - SF * 3.3)}" y="${r3(H / 2 - SF * .62)}" width="${r3(SF * 6.6)}" height="${r3(SF * 1.24)}" rx="${r3(SF * .2)}"/><text class="ohcNewT" x="${sx}" y="${r3(H / 2 + SF * .24)}" font-size="${r3(SF * .64)}" text-anchor="middle" letter-spacing="${r3(SF * .08)}">NEW SHEET</text></g>` : '';
    const label = `Sheet history at true scale: ${num(W)} by ${num(H)} millimetres, ${n ? plural(n, 'cut') : 'never cut'}${m.deleted ? ', deleted' : ''}`;
    return `<svg class="ohSvg${compact ? ' ohCompact' : ''}${o.reveal === false || (compact && o.reveal !== true) ? '' : ' ohReveal'}${reduced() ? ' ohStill' : ''}${sel ? ' ohHasSel' : ''}" viewBox="0 0 ${r3(fw)} ${r3(fh)}" style="aspect-ratio:${r3(fw)}/${r3(fh)}" ${compact ? 'aria-hidden="true"' : `role="group" aria-label="${esc(label)}"`}>`
      + (frame ? `<rect class="ohTray" x="0" y="0" width="${r3(fw)}" height="${r3(fh)}" fill="${TRAY}"/>` : '')
      + `<g class="ohLayers">${layers}</g><g class="ohPieces">${pieces}</g>`
      + `<rect class="ohBorder" x=".25" y=".25" width="${r3(Math.max(0, W - .5))}" height="${r3(Math.max(0, H - .5))}" fill="none" stroke="${EDGE}" stroke-width="1" vector-effect="non-scaling-stroke" pointer-events="none"/>`
      + `<g class="ohLines">${lines}</g>${stamp}<g class="ohTags">${tags}</g></svg>`;
  }

  /* ── the list ── */
  const areaWords = c => `${fmtInt(c.away)} mm² cut away<span class="ohDot" aria-hidden="true">·</span>${(Math.round(c.pct * 10) / 10).toLocaleString('en-US')}% of the sheet`;
  function timeline(history, o = {}) {
    ensureCss();
    const m = model(history, o), { W, H, n } = m, sel = selOf(o.selected, n), word = metalWord(m.st), isNew = m.kind === 'new';
    const swatch = c => `<span class="ohSw" style="--sw:${c}" aria-hidden="true"></span>`, dot = '<span class="ohDot" aria-hidden="true">·</span>';
    const madeRow = m.made ? `<span class="ohRow2">Made by <span class="ohWho${m.made.by ? '' : ' ohMiss'}">${esc(m.made.by || 'Person not recorded')}</span>${dot}<span class="ohWhat${Number.isFinite(m.made.at) ? '' : ' ohMiss'}">${esc(m.when(m.made.at))}</span></span>` : '';
    let html = `<li class="ohStep"><div class="ohItem ohStatic"><span class="ohNum ohNumO" aria-hidden="true"></span><span class="ohBody"><span class="ohRow1"><b class="ohWhen">${isNew ? 'New sheet' : 'Original sheet'} ${num(W)} × ${num(H)} mm</b></span>${madeRow}`
      + `<span class="ohRow3">${word ? esc(word) + dot : ''}${fmtInt(m.total)} mm² to start with</span></span>${swatch(SHEET)}</div></li>`;
    if (m.partial) html += `<li class="ohStep"><div class="ohItem ohStatic ohNote"><span class="ohNum ohNumO" aria-hidden="true"></span><span class="ohBody"><span class="ohRow2 ohMiss">Earlier cuts of this sheet are not shown here.</span></span></div></li>`;
    for (const c of m.cuts) {
      const what = whatOf(c), name = `Cut ${c.label}, ${c.when}, ${c.by || 'person not recorded'}${what ? ', ' + what : ''}, ${fmtInt(c.away)} square millimetres cut away`;
      html += `<li class="ohStep"><button type="button" class="ohItem${c.k === sel ? ' ohOn' : ''}" data-cut="${c.k}" aria-pressed="${c.k === sel}" aria-label="${esc(name)}">`
        + `<span class="ohNum" aria-hidden="true">${c.label}</span><span class="ohBody">`
        + `<span class="ohRow1"><span class="ohKick">Cut ${c.label}</span><b class="ohWhen${Number.isFinite(c.at) ? '' : ' ohMiss'}">${esc(c.when)}</b></span>`
        + `<span class="ohRow2"><span class="ohWho${c.by ? '' : ' ohMiss'}">${esc(c.by || 'Person not recorded')}</span>${dot}<span class="ohWhat${what ? '' : ' ohMiss'}">${esc(what || 'Sheet not recorded')}</span></span>`
        + `<span class="ohRow3">${areaWords(c)}</span></span>${swatch(greyOf(c.k, n))}</button></li>`;
    }
    html += n ? `<li class="ohStep"><div class="ohItem ohStatic ohNow"><span class="ohNum ohNumNow" aria-hidden="true"></span><span class="ohBody"><span class="ohRow1"><b class="ohWhen">Left on the sheet now</b></span>`
      + `<span class="ohRow3">${fmtInt(m.left)} mm²${dot}${(Math.round(m.left / (m.total || 1) * 1000) / 10).toLocaleString('en-US')}% of the sheet${m.st.ownerSheetName ? `${dot}on ${esc(m.st.ownerSheetName)}` : ''}</span></span>${swatch(SHEET)}</div></li>`
      : `<li class="ohStep"><div class="ohItem ohStatic ohNoCuts"><span class="ohNum ohNumNow" aria-hidden="true"></span><span class="ohBody"><span class="ohRow1"><b class="ohWhen">No cut has been made on this sheet yet</b></span></span></div></li>`;
    if (m.deleted) html += `<li class="ohStep"><div class="ohItem ohStatic ohDel"><span class="ohNum ohNumDel" aria-hidden="true">×</span><span class="ohBody"><span class="ohRow1"><span class="ohKick ohKickDel">Deleted</span><b class="ohWhen${Number.isFinite(m.deleted.at) ? '' : ' ohMiss'}">${esc(m.when(m.deleted.at))}</b></span>`
      + `<span class="ohRow2">By <span class="ohWho${m.deleted.by ? '' : ' ohMiss'}">${esc(m.deleted.by || 'Person not recorded')}</span></span>`
      + `<span class="ohRow2 ohReason">Reason: ${m.deleted.reason ? esc(m.deleted.reason) : '<span class="ohMiss">not recorded</span>'}</span></span></div></li>`;
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

  /* ── the cards (Paul 7 Oct, round 2): the thumbnail of every partial / new sheet IS that sheet's history ──
     groups(items)                   the repository list -> one { stockId, latest, history } per physical sheet (history in the sheetHistory shape), built from the list alone
     card(group, { actions, selected, enlarged, now })     one card as HTML: the large thumbnail (button), the facts, the window's `actions` slot
     bindCards(root, { onEnlarge, onCollapse, getHistory, holdEscape, groupOf, now })      click / Enter / Space on a thumbnail enlarges that card IN PLACE (full width, grow animation),
                                     shows the whole detail (drawing + timeline), asks getHistory(group) ONCE for the server's exact history and redraws when it arrives */
  const sid = v => String(v == null ? '' : v);
  const revOf = c => { const r = Math.floor(+(c && c.revision)); return c && c.revision != null && c.revision !== '' && Number.isFinite(r) && r >= 0 ? r : null; };
  const statusOf = c => sid(c && c.status).trim() || 'available';
  const msOrNull = v => { const t = toMs(v); return Number.isFinite(t) ? t : null; };
  const personOf = v => sid(v).trim();
  const mmWord = (w, h) => `${num(+w || 0)} × ${num(+h || 0)} mm`;
  const sizeOf = c => ({ w: +c.wMm || +(c.bboxMm && c.bboxMm.w) || 0, h: +c.hMm || +(c.bboxMm && c.bboxMm.h) || 0 });

  /** The physical sheet's own size: the real sheet size every record carries; else the made sheet; else the extent of all the outlines. */
  function sheetSizeOf(recs) {
    for (const c of recs) { const w = +c.sheetWMm, h = +c.sheetHMm; if (w > 0 && h > 0) return { w, h }; }
    const m = recs.find(c => c.kind === 'new');
    if (m) { const s = sizeOf(m); if (s.w > 0 && s.h > 0) return s; }
    let w = 0, h = 0;
    for (const c of recs) for (const r of ringsOf(c) || []) for (const p of r) { w = Math.max(w, p[0]); h = Math.max(h, p[1]); }
    return w > 0 && h > 0 ? { w: r3(w), h: r3(h) } : { w: REF.w, h: REF.h };
  }
  function buildGroup(g) {
    const recs = g.recs.slice().sort((a, b) => (revOf(a) == null ? -1 : revOf(a)) - (revOf(b) == null ? -1 : revOf(b)) || (toMs(a.cutAt) || 0) - (toMs(b.cutAt) || 0));
    const latest = recs[recs.length - 1], R = revOf(latest), cutRecs = recs.filter(c => c.kind !== 'new' && revOf(c) !== 0), madeRec = recs.find(c => c.kind === 'new');
    const have = new Set(cutRecs.map(revOf).filter(r => r != null && r >= 1));
    let missing = 0;
    if (R != null && R >= 1) for (let r = 1; r <= R; r++) if (!have.has(r)) missing++;
    const size = sheetSizeOf(recs), st = statusOf(latest);
    const cuts = cutRecs.map((c, i) => { const at = msOrNull(c.cutAt), by = personOf(c.cutBy); return { n: i + 1, revision: revOf(c), at, by, sheetId: sid(c.sourceSheetId), sheetName: sid(c.sourceSheet), setName: sid(c.sourceSet), via: sid(c.via),
      rings: ringsOf(c), areaMm2: Number.isFinite(+c.areaMm2) ? +c.areaMm2 : null, bboxMm: c.bboxMm || null, exact: at != null && !!by }; });
    const made = madeRec ? { at: msOrNull(madeRec.madeAt != null ? madeRec.madeAt : madeRec.cutAt), by: personOf(madeRec.madeBy != null ? madeRec.madeBy : madeRec.cutBy) } : null;   // (a made sheet's record keeps its making in cutAt / by too: it is not a cut)
    const deleted = st === 'deleted' || latest.deletedAt != null ? { at: msOrNull(latest.deletedAt != null ? latest.deletedAt : latest.statusAt), by: personOf(latest.deletedBy != null ? latest.deletedBy : latest.statusBy), reason: sid(latest.deletedReason != null ? latest.deletedReason : latest.reason).trim() } : null;
    return { stockId: g.stockId, latest,
      history: { ok: true, stock: { id: g.stockId, metal: latest.metal || 'rose', code: latest.code || '', wMm: size.w, hMm: size.h, revision: R == null ? 0 : R, ownerSheetId: st === 'inUse' ? latest.inUseBySheetId || null : null, ownerSheetName: st === 'inUse' ? latest.inUseBySheetName || null : null, kind: madeRec ? 'new' : '' },
        cuts, made, deleted, partial: missing > 0, missing } };
  }
  /** The repository list (searchAll or partialList cards) -> one group per physical sheet, in the order each sheet first appears in the list. Reads nothing: the earlier revisions of a stock
   *  are the 'used' records of the same stockId (their outline, cutAt, cutBy, sourceSheet / sourceSet say what each cut left). `history.partial` = some earlier revisions are not in the list. */
  function groups(items) {
    const by = new Map(), order = [];
    (Array.isArray(items) ? items : []).forEach((c, i) => {
      if (!c || typeof c !== 'object') return;
      const key = c.stockId ? 's:' + sid(c.stockId) : c.id ? 'i:' + sid(c.id) : 'x:' + i;
      let g = by.get(key);
      if (!g) { g = { stockId: c.stockId ? sid(c.stockId) : c.id ? sid(c.id) : 'x' + i, recs: [] }; by.set(key, g); order.push(g); }
      g.recs.push(c);
    });
    return order.map(buildGroup);
  }

  const REG = new Map(), DETAIL = new Map(), PENDING = new Set(), FAILED = new Set(), LIVE = new Set();   // last group drawn per stock · server histories · loading · gave up · bound roots
  const keyOf = g => `${g.stockId}|${g.latest ? revOf(g.latest) : ''}|${g.latest ? statusOf(g.latest) : ''}`;
  const histOf = g => g.history || { stock: {}, cuts: [] };
  const idOf = (g, tag) => `ohc-${sid(g.stockId).replace(/[^\w-]/g, '_')}-${tag}`;
  /** The history the detail draws: the server's exact one once it came (kept per sheet and revision), else what the list holds. The server's missing made / deleted fall back to the list's. */
  function detailOf(g) {
    const base = histOf(g), d = DETAIL.get(keyOf(g)) || g.detail;
    if (!d || !Array.isArray(d.cuts)) return base;
    return Object.assign({}, d, { stock: Object.assign({}, base.stock || {}, d.stock || {}), made: d.made || base.made || null, deleted: d.deleted || base.deleted || null, partial: d.partial === true });
  }
  const thumbPx = () => { const w = +root.innerWidth || 0; return w && w < 720 ? Math.max(280, Math.min(560, w - 110)) : 540; };
  const cutWords = (g, c) => { const n = Math.max(histOf(g).cuts.length, revOf(c) || 0); return n ? plural(n, 'cut') : 'never cut'; };
  const STATUS_WORD = { available: 'Available', inUse: 'In use', used: 'Used', discarded: 'Discarded', deleted: 'Deleted' };
  function fitWords(c) {
    const p = PSU(); try { if (p && typeof p.fitWords === 'function') return p.fitWords(c); } catch (_) { /* the local words */ }
    const e = c && c.estimate;
    if (!e || !Number.isFinite(+e.pieces)) return { main: 'Fit not estimated', sub: '' };
    const n = Math.round(+e.pieces), low = Number.isFinite(+e.low) ? Math.round(+e.low) : n, high = Number.isFinite(+e.high) ? Math.round(+e.high) : n;
    if (n <= 0) return { main: 'Too small for a regular piece', sub: '' };
    return { main: `About ${plural(n, 'piece')}`, sub: low !== high && high > 0 ? `roughly ${low} to ${high}` : '' };
  }

  function titleOf(c) { const s = sizeOf(c); return `${c.kind === 'new' ? 'New sheet' : 'Partial sheet'} ${mmWord(s.w, s.h)}`; }
  function headHtml(g, o) {
    const c = g.latest, st = statusOf(c), s = sizeOf(c), code = c.code || (METAL[c.metal] && METAL[c.metal].code) || '', word = (METAL[c.metal] && METAL[c.metal].word) || '';
    const area = Number.isFinite(+c.areaMm2) ? +c.areaMm2 : areaOf(ringsOf(c) || []);
    return `<div class="ohcHead"><div class="ohcHeadMain"><div class="ohcTitle" role="heading" aria-level="3">${esc(titleOf(c))}</div>`
      + `<div class="ohcSub">${code ? `<span class="ohcMetal">${esc(code)}</span>` : ''}${esc([word, area > 0 ? `${fmtInt(area)} mm²` : ''].filter(Boolean).join(' · '))}</div></div>`
      + `<span class="ohcChip" data-s="${esc(st)}">${esc(STATUS_WORD[st] || st)}</span>${o.closeBtn ? '<button type="button" class="ohcClose" data-oh-close aria-expanded="true" aria-label="Close the detailed view">×</button>' : ''}</div>`;
  }
  function factsHtml(g, o) {
    const c = g.latest, h = histOf(g), st = statusOf(c), now = o.now, rows = [], D = at => shortDate(msOrNull(at), now);
    const row = (k, main, sub) => rows.push(`<div class="ohcFact"><dt>${k}</dt><dd>${main}${sub ? `<small>${sub}</small>` : ''}</dd></div>`);
    const who = v => (v ? `<b>${esc(v)}</b>` : '<b class="ohMiss">Person not recorded</b>'), when = at => (Number.isFinite(msOrNull(at)) ? `<b>${esc(D(at))}</b>` : '<b class="ohMiss">Date not recorded</b>');
    if (st === 'available' || st === 'inUse') { const f = fitWords(c); row('Fits', `<b>${esc(f.main)}</b>`, esc(f.sub)); }
    if (st === 'inUse') row('In use on', `<b>${esc(c.inUseBySheetName || 'a sheet')}</b>`, [c.inUseBy ? 'by ' + c.inUseBy : '', Number.isFinite(msOrNull(c.inUseAt)) ? 'since ' + D(c.inUseAt) : ''].filter(Boolean).map(esc).join(' · '));
    if (st === 'used') row('Used on', `<b>${esc(c.usedBySheetName || 'a later sheet')}</b>`, [c.usedBy ? 'by ' + c.usedBy : '', Number.isFinite(msOrNull(c.usedAt)) ? D(c.usedAt) : ''].filter(Boolean).map(esc).join(' · '));
    if (st === 'discarded') row('Discarded', '<b>Not reusable</b>', [c.reason || '', c.statusBy ? 'by ' + c.statusBy : ''].filter(Boolean).map(esc).join(' · '));
    if (st === 'deleted') { const d = h.deleted || {}; row('Deleted by', who(d.by), esc(Number.isFinite(d.at) ? D(d.at) : 'Date not recorded')); row('Reason', `<b class="ohcReason">${d.reason ? esc(d.reason) : '<span class="ohMiss">not recorded</span>'}</b>`); }
    const never = c.kind === 'new' && !h.cuts.length && (st === 'available' || st === 'deleted');
    if (never) row('Last used', '<b class="ohMiss">Not used yet</b>');
    else if (Number.isFinite(msOrNull(c.lastUsedAt))) {
      const first = Number.isFinite(msOrNull(c.cutAt)) && msOrNull(c.lastUsedAt) === msOrNull(c.cutAt) && (!c.lastUsedSheet || c.lastUsedSheet === c.sourceSheet);
      row('Last used', when(c.lastUsedAt), first ? 'when it was cut' : [c.lastUsedBy ? 'by ' + c.lastUsedBy : '', c.lastUsedSheet ? 'on ' + c.lastUsedSheet : ''].filter(Boolean).map(esc).join(' · '));
    } else row('Last used', '<b class="ohMiss">Date not recorded</b>');
    if (c.kind === 'new') { const at = c.madeAt != null ? c.madeAt : c.cutAt; row('Made by', who(personOf(c.madeBy != null ? c.madeBy : c.cutBy)), esc(Number.isFinite(msOrNull(at)) ? D(at) : 'Date not recorded')); }
    else {
      row('Cut by', who(personOf(c.cutBy)), [Number.isFinite(msOrNull(c.cutAt)) ? D(c.cutAt) : '', c.sourceSheet ? 'from ' + [c.sourceSheet, c.sourceSet].filter(Boolean).join(', ') : ''].filter(Boolean).map(esc).join(' · '));
      if (h.made) row('New sheet made by', who(h.made.by), esc(Number.isFinite(h.made.at) ? D(h.made.at) : 'Date not recorded'));
    }
    return `<dl class="ohcFacts">${rows.join('')}</dl>`;
  }
  function liveHtml(g) {
    const k = keyOf(g);
    if (PENDING.has(k)) return '<span class="ohcLoad"><i class="ohcSpin" aria-hidden="true"></i>Loading the full history of this sheet…</span>';
    if (FAILED.has(k)) return '<span class="ohcNote">The full history could not be loaded, so this shows what the list holds.</span>';
    return histOf(g).partial === true && !DETAIL.has(k) && !g.detail ? '<span class="ohcNote">Earlier cuts of this sheet are not shown here.</span>' : '';
  }
  const detailHtml = (g, o) => view(detailOf(g), { width: o.width || 800, dates: 'short', now: o.now, id: idOf(g, 'd'), reveal: o.reveal, selected: o.selected });
  function inner(g, o) {
    const c = g.latest, h = histOf(g), actions = o.keepActions ? '<div class="ohcActions" data-oh-keep></div>' : o.actions ? `<div class="ohcActions">${o.actions}</div>` : '';
    if (o.enlarged) return `<div class="ohcIn">${headHtml(g, { closeBtn: true })}<div class="ohcLive" aria-live="polite">${liveHtml(g)}</div><div class="ohcDetail" data-oh-detail>${detailHtml(g, o)}</div>${factsHtml(g, o)}${actions}</div>`;
    const code = c.code || (METAL[c.metal] && METAL[c.metal].code) || '', st = statusOf(c), s = sizeOf(c);
    const label = `${code ? code + ' ' : ''}${c.kind === 'new' ? 'new' : 'partial'} sheet, ${num(s.w)} by ${num(s.h)} mm, ${cutWords(g, c)}${st === 'available' ? '' : ', ' + (STATUS_WORD[st] || st).toLowerCase()}, press to enlarge`;
    const thumb = svg(h, { compact: true, width: o.width || thumbPx(), dates: 'short', now: o.now, id: idOf(g, 't'), reveal: o.reveal === true });
    return `<div class="ohcIn"><div class="ohcFig"><div class="ohcThumb" role="button" tabindex="0" aria-expanded="false" aria-label="${esc(label)}" data-oh-thumb>`
      + `<div class="ohcStage">${thumb}</div><div class="ohcCap"><span class="ohcLens" aria-hidden="true"><svg viewBox="0 0 16 16" width="14" height="14" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round"><circle cx="6.8" cy="6.8" r="4.6"/><path d="M10.4 10.4L14 14"/></svg></span><span>${esc(cutWords(g, c))} · click to enlarge</span>`
      + `${h.partial === true ? '<span class="ohcCapNote">Earlier cuts not shown</span>' : ''}</div></div></div>`
      + `<div class="ohcInfo">${headHtml(g, {})}${factsHtml(g, o)}</div>${actions}</div>`;
  }
  const cardClass = (g, o) => `ohc ohcS-${statusOf(g.latest)}${o.enlarged ? ' ohcBig' : ''}${o.selected ? ' ohcSel' : ''}`;
  /** One sheet's card as HTML. `actions` = the window's buttons (raw html, kept as they are when the card enlarges); `enlarged: true` draws it open (after a repaint). */
  function card(group, o = {}) {
    if (!group || !group.latest) return '';
    ensureCss();
    REG.set(group.stockId, group);
    return `<article class="${cardClass(group, o)}" data-stock="${esc(group.stockId)}" data-id="${esc(group.latest.id)}" data-status="${esc(statusOf(group.latest))}" data-mkey="oh-${esc(group.stockId)}"${o.enlarged ? ' data-enlarged="true"' : ''}>${inner(group, o)}</article>`;
  }

  /* ── wiring: enlarge in place, collapse, the server's exact history, Esc ── */
  const safe = (f, ...a) => { if (typeof f !== 'function') return; try { f(...a); } catch (_) { /* a handler's own trouble stays its own */ } };
  function grow(el, ms) {
    if (!el || !el.isConnected || reduced()) return;
    const M = root.Motion;
    try { if (M && typeof M.grow === 'function') { M.grow(el, ms ? { ms, room: false } : {}); return; } } catch (_) { /* the plain fade */ }
    try { if (el.animate) el.animate([{ opacity: 0, transform: 'translateY(-6px)' }, { opacity: 1, transform: 'none' }], { duration: ms || 420, easing: 'cubic-bezier(.3,.1,.2,1)' }); } catch (_) { /* nothing to animate */ }
  }
  function toView(el, block) {
    try { if (!el.scrollIntoView) return; const go = () => el.scrollIntoView({ block, behavior: reduced() ? 'auto' : 'smooth' }); if (root.requestAnimationFrame) root.requestAnimationFrame(go); else go(); } catch (_) { /* not scrollable */ }
  }
  const detailPx = el => { const cw = el.clientWidth || 0; if (!cw) return 800; const inner = cw - 40; return Math.max(300, Math.min(900, inner >= 820 ? inner - 364 - 32 : inner - 32)); };

  function bindCards(rootEl, o = {}) {
    const none = { enlarge() {}, collapse: () => false, enlarged: () => null, destroy() {} };
    if (!rootEl || !rootEl.addEventListener) return none;
    if (typeof rootEl._ohcOff === 'function') rootEl._ohcOff();
    const scope = (rootEl.closest && rootEl.closest('dialog')) || rootEl, now = Number.isFinite(+o.now) && o.now != null ? +o.now : undefined;
    const cardOf = t => { const el = t && t.closest ? t.closest('.ohc') : null; return el && rootEl.contains(el) ? el : null; };
    const groupFor = el => { const id = el && el.getAttribute('data-stock'); return (typeof o.groupOf === 'function' && o.groupOf(id)) || REG.get(id) || null; };
    const bigOnes = () => Array.from(rootEl.querySelectorAll('.ohc.ohcBig'));
    const repaint = (el, g, big) => {   // the card's inside is drawn again; the window's actions node is moved over as it is (its listeners, its answer in place)
      const keep = el.querySelector('.ohcActions'), sel = el.classList.contains('ohcSel');
      el.className = cardClass(g, { enlarged: big, selected: sel });
      if (big) el.setAttribute('data-enlarged', 'true'); else el.removeAttribute('data-enlarged');
      el.innerHTML = inner(g, { enlarged: big, now, keepActions: !!keep, width: big ? detailPx(el) : undefined, reveal: big });
      const slot = el.querySelector('.ohcActions'); if (keep && slot) slot.replaceWith(keep);
      if (big) wireDetail(el, g);
    };
    const wireDetail = (el, g, selected) => { const d = el.querySelector('[data-oh-detail]'); if (d) el._ohcB = bind(d, detailOf(g), { selected }); };
    const paintDetail = (el, g) => {   // only the drawing and timeline: nothing else under the user's hands moves
      const d = el.querySelector('[data-oh-detail]'); if (!d) return;
      const selected = el._ohcB ? el._ohcB.selected() : null;
      d.innerHTML = detailHtml(g, { now, width: detailPx(el), reveal: false, selected });
      wireDetail(el, g, selected);
      const live = el.querySelector('.ohcLive'); if (live) live.innerHTML = liveHtml(g);
    };
    const refresh = stockId => { for (const el of bigOnes()) { if (el.getAttribute('data-stock') !== stockId) continue; const g = groupFor(el); if (g) paintDetail(el, g); } };
    const ensureDetail = (el, g, again) => {   // the server's exact history, asked for once per sheet and revision; the list's data is on screen meanwhile
      const k = keyOf(g);
      if (typeof o.getHistory !== 'function' || DETAIL.has(k) || PENDING.has(k)) return;
      if (FAILED.has(k) && !again) return;
      FAILED.delete(k); PENDING.add(k);
      const live = el.querySelector('.ohcLive'); if (live) live.innerHTML = liveHtml(g);
      let p; try { p = Promise.resolve(o.getHistory(g)); } catch (e) { p = Promise.reject(e); }
      p.then(h => { if (h && Array.isArray(h.cuts) && h.ok !== false) DETAIL.set(k, h); else FAILED.add(k); }, () => FAILED.add(k))
        .then(() => { PENDING.delete(k); LIVE.forEach(f => f(g.stockId)); });
    };
    const collapse = (el, silent) => {
      const g = groupFor(el); if (!g) return false;
      const had = el.contains(rootEl.ownerDocument.activeElement) || rootEl.ownerDocument.activeElement === rootEl.ownerDocument.body;
      repaint(el, g, false); grow(el, 380);
      if (had) { const t = el.querySelector('.ohcThumb'); if (t && t.focus) t.focus({ preventScroll: true }); }
      toView(el, 'nearest');
      if (!silent) safe(o.onCollapse, g, el);
      return true;
    };
    const enlarge = el => {
      const g = groupFor(el); if (!g) return;
      for (const other of bigOnes()) if (other !== el) collapse(other);
      el.classList.add('ohcBig');   // the card takes the window's full width first, so the drawing is made for the width it really gets
      repaint(el, g, true); grow(el); toView(el, 'start');
      const x = el.querySelector('.ohcClose'); if (x && x.focus) x.focus({ preventScroll: true });
      safe(o.onEnlarge, g, el);
      ensureDetail(el, g, true);
    };
    const onClick = e => {
      const el = cardOf(e.target); if (!el) return;
      if (e.target.closest('[data-oh-close]')) { collapse(el); return; }
      if (e.target.closest('[data-oh-thumb]')) { enlarge(el); return; }
      // "click again": a click on the open drawing where it is not a cut (the sheet, the tray) closes it; a click on a line, label, piece or list row only picks that cut
      if (el.classList.contains('ohcBig') && e.target.closest('.ohStage') && !e.target.closest('[data-cut], .ohTag, .ohItem')) collapse(el);
    };
    const onKey = e => {
      if ((e.key === 'Enter' || e.key === ' ') && e.target && e.target.hasAttribute && e.target.hasAttribute('data-oh-thumb')) { e.preventDefault(); const el = cardOf(e.target); if (el) enlarge(el); }
    };
    const onEsc = e => {
      if (e.key !== 'Escape' || e.defaultPrevented || e.altKey || e.ctrlKey || e.metaKey || e.isComposing) return;
      const dlg = e.target && e.target.closest ? e.target.closest('dialog') : null;
      if (dlg && dlg !== scope) return;   // an Esc inside another pop-up is that pop-up's
      const open = bigOnes(); if (!open.length) return;
      if (typeof o.holdEscape === 'function') { let hold = false; try { hold = !!o.holdEscape(e); } catch (_) { hold = false; } if (hold) return; }   // the window's own pending answer goes first
      e.preventDefault(); e.stopPropagation();   // only when a card is open: otherwise the window's own Esc order runs
      open.forEach(el => collapse(el));
    };
    rootEl.addEventListener('click', onClick); rootEl.addEventListener('keydown', onKey); scope.addEventListener('keydown', onEsc, true);
    LIVE.add(refresh);
    const destroy = () => { rootEl.removeEventListener('click', onClick); rootEl.removeEventListener('keydown', onKey); scope.removeEventListener('keydown', onEsc, true); LIVE.delete(refresh); if (rootEl._ohcOff === destroy) { rootEl._ohcOff = null; rootEl._ohcApi = null; } };
    rootEl._ohcOff = destroy;
    // cards that were drawn open (a repaint of the window's list): their detail is wired again, and asked for if it never came
    for (const el of bigOnes()) { const g = groupFor(el); if (g) { wireDetail(el, g); ensureDetail(el, g, false); } }
    const api = {
      enlarge: id => { const el = rootEl.querySelector(`.ohc[data-stock="${String(id).replace(/["\\]/g, '')}"]`); if (el && !el.classList.contains('ohcBig')) enlarge(el); },
      collapse: () => { const open = bigOnes(); open.forEach(el => collapse(el)); return open.length > 0; },
      enlarged: () => { const el = bigOnes()[0]; return el ? el.getAttribute('data-stock') : null; },
      destroy };
    rootEl._ohcApi = api;
    return api;
  }
  /** Collapse the enlarged card inside `rootEl` (true when there was one): for a window that runs its own Esc order. */
  const collapseCards = rootEl => !!(rootEl && rootEl._ohcApi && rootEl._ohcApi.collapse());

  /* ── styles (the app's own tokens, with fallbacks so the file also looks right alone) ── */
  const CSS = `
.ohView,.ohTl,.ohSvg,.ohLegend,.ohStage,.ohSide{--oh-ink:var(--ink,#1c1a17);--oh-ink70:var(--ink70,#5b554c);--oh-ink45:var(--ink45,#938c80);--oh-ink25:var(--ink25,#c4bdb0);--oh-line:var(--line,#e4ddd0);--oh-card:var(--card,#fffefb);--oh-card2:var(--card2,#faf7f1);--oh-gold:var(--gold,#a9823f);--oh-goldLine:var(--goldLine,#e3d3a6);--oh-green:${GREEN};--oh-clay:var(--clay,#b0563f)}
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
.ohNumDel{background:var(--oh-clay);font-size:17px;line-height:1}
.ohKickDel{color:var(--oh-clay)}
.ohReason{white-space:pre-wrap;overflow-wrap:anywhere;color:var(--oh-ink)}
.ohNote .ohRow2{font-size:12px}
.ohcStamp{opacity:.62}
.ohcStampBox{fill:rgba(176,86,63,.07);stroke:var(--oh-clay);stroke-width:2.4px;vector-effect:non-scaling-stroke}
.ohcStampT{fill:var(--oh-clay);font-family:var(--sans,system-ui,sans-serif);font-weight:800}
.ohcNewBox{fill:none;stroke:var(--oh-ink25);stroke-width:1.6px;stroke-dasharray:5 4;vector-effect:non-scaling-stroke}
.ohcNewT{fill:var(--oh-ink45);font-family:var(--sans,system-ui,sans-serif);font-weight:700}
.ohSvg.ohCompact *{pointer-events:none}
/* the cards */
.ohcGrid{display:grid;grid-template-columns:repeat(auto-fill,minmax(min(100%,500px),1fr));gap:18px;min-width:0}
.ohc{--oh-ink:var(--ink,#1c1a17);--oh-ink70:var(--ink70,#5b554c);--oh-ink45:var(--ink45,#938c80);--oh-ink25:var(--ink25,#c4bdb0);--oh-line:var(--line,#e4ddd0);--oh-card:var(--card,#fffefb);--oh-card2:var(--card2,#faf7f1);--oh-gold:var(--gold,#a9823f);--oh-goldLine:var(--goldLine,#e3d3a6);--oh-green:${GREEN};--oh-clay:var(--clay,#b0563f);
  container:ohc/inline-size;position:relative;min-width:0;box-sizing:border-box;background:var(--oh-card);border:1px solid var(--oh-line);border-radius:16px;box-shadow:var(--sh,0 1px 2px rgba(30,26,20,.04),0 9px 28px rgba(30,26,20,.06));padding:20px;font-family:var(--sans,system-ui,sans-serif);color:var(--oh-ink);scroll-margin:12px;transition:border-color .14s,box-shadow .14s}
.ohc.ohcBig{grid-column:1/-1;width:100%}
.ohc.ohcSel{border-color:var(--oh-ink);box-shadow:0 0 0 2px var(--oh-goldLine)}
.ohcIn{display:grid;gap:16px;min-width:0}
.ohcFig{min-width:0}
.ohcThumb{display:block;position:relative;box-sizing:border-box;width:100%;max-width:560px;border:1px solid var(--oh-line);border-radius:13px;background:var(--oh-card2);padding:12px 12px 10px;cursor:zoom-in;outline:none;transition:border-color .14s,box-shadow .14s}
.ohcThumb:hover{border-color:var(--oh-ink45);box-shadow:0 0 0 2px var(--oh-goldLine)}
.ohcThumb:focus-visible{border-color:var(--oh-gold);box-shadow:0 0 0 3px var(--oh-goldLine)}
.ohcStage{min-width:0}
.ohcStage .ohSvg,.ohcDetail .ohSvg{width:auto;max-width:100%;margin:0 auto}
.ohcStage .ohSvg{max-height:380px}
.ohcDetail .ohSvg{max-height:72vh;cursor:zoom-out}
.ohcDetail .ohSvg .ohLine,.ohcDetail .ohSvg .ohTag,.ohcDetail .ohSvg .ohPiece{cursor:pointer}
.ohcCap{display:flex;align-items:center;flex-wrap:wrap;gap:4px 8px;margin:9px 2px 0;font-size:12px;font-weight:600;color:var(--oh-ink70)}
.ohcLens{display:inline-flex;color:var(--oh-ink45)}
.ohcThumb:hover .ohcLens,.ohcThumb:focus-visible .ohcLens{color:var(--oh-gold)}
.ohcCapNote{margin-left:auto;font-weight:500;font-style:italic;color:var(--oh-ink45)}
.ohcInfo{display:grid;gap:14px;align-content:start;min-width:0}
.ohcHead,.ohcBigHead{display:flex;align-items:flex-start;gap:12px;min-width:0}
.ohcHeadMain{flex:1 1 auto;min-width:0}
.ohcTitle{font:500 22px/1.2 var(--serif,Georgia,serif);color:var(--oh-ink);letter-spacing:.005em;overflow-wrap:anywhere}
.ohcSub{display:flex;flex-wrap:wrap;align-items:center;gap:4px 8px;margin-top:5px;font-size:12.5px;color:var(--oh-ink70);font-variant-numeric:tabular-nums}
.ohcMetal{display:inline-block;padding:2px 8px;border-radius:6px;background:var(--oh-ink);color:#fff;font:700 11px/1.3 var(--mono,ui-monospace,Menlo,Consolas,monospace);letter-spacing:.04em}
.ohcChip{flex:none;display:inline-block;padding:4px 12px;border-radius:999px;border:1px solid var(--oh-line);background:var(--oh-card2);color:var(--oh-ink70);font:600 11.5px/1.4 var(--sans,system-ui,sans-serif);white-space:nowrap}
.ohcChip[data-s=available]{background:#e7eddf;border-color:#c9d6bd;color:#46603f}
.ohcChip[data-s=inUse]{background:#f0e6cd;border-color:var(--oh-goldLine);color:#7a5a1f}
.ohcChip[data-s=used]{background:#ece8e0;border-color:var(--oh-line);color:var(--oh-ink70)}
.ohcChip[data-s=discarded]{background:#f1ede6;border-color:var(--oh-line);color:var(--oh-ink45)}
.ohcChip[data-s=deleted]{background:#f4e3dc;border-color:#e3bfb2;color:#8c3d28}
.ohcClose{flex:none;width:38px;height:38px;border-radius:50%;border:1px solid var(--oh-line);background:var(--oh-card);color:var(--oh-ink);font:400 24px/1 var(--sans,system-ui,sans-serif);cursor:pointer;padding:0;transition:background-color .13s,border-color .13s}
.ohcClose:hover{background:var(--oh-card2);border-color:var(--oh-ink45)}
.ohcClose:focus-visible{outline:2px solid var(--oh-gold);outline-offset:2px}
.ohcFacts{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px 22px;margin:0;padding:0}
.ohcFact{min-width:0;margin:0}
.ohcFact dt{font-size:10px;letter-spacing:.11em;text-transform:uppercase;font-weight:600;color:var(--oh-ink45);margin:0 0 3px}
.ohcFact dd{margin:0;display:grid;gap:1px;font-size:13.5px;line-height:1.4;color:var(--oh-ink);min-width:0;overflow-wrap:anywhere}
.ohcFact dd b{font-weight:650}
.ohcFact dd small{font-size:12px;color:var(--oh-ink70)}
.ohcReason{white-space:pre-wrap;font-weight:500}
.ohc .ohMiss{font-style:italic;font-weight:500;color:var(--oh-ink45)}
.ohcActions{display:flex;flex-wrap:wrap;align-items:center;gap:10px;min-width:0}
.ohcLive{min-height:0;font-size:12.5px;color:var(--oh-ink70)}
.ohcLive:empty{display:none}
.ohcLoad{display:inline-flex;align-items:center;gap:9px;font-weight:600}
.ohcSpin{display:inline-block;width:14px;height:14px;border-radius:50%;border:2px solid var(--oh-line);border-top-color:var(--oh-gold);animation:ohcSpin .9s linear infinite}
@keyframes ohcSpin{to{transform:rotate(360deg)}}
.ohcNote{font-style:italic;color:var(--oh-ink45)}
.ohcDetail{min-width:0}
.ohc .ohView{grid-template-columns:minmax(0,1fr) minmax(300px,340px)}
@container ohc (max-width:860px){.ohc .ohView{grid-template-columns:minmax(0,1fr)}.ohc .ohSide{max-height:none;overflow:visible}}
@container ohc (min-width:900px){.ohc:not(.ohcBig) .ohcIn{grid-template-columns:minmax(0,540px) minmax(0,1fr);gap:22px 26px;align-items:start}.ohc:not(.ohcBig) .ohcActions{grid-column:1/-1}}
@media (max-width:560px){.ohc{padding:14px}.ohcTitle{font-size:19px}}
@media (prefers-reduced-motion:reduce){.ohReveal .ohL,.ohReveal .ohLine,.ohReveal .ohTag{animation:none!important}.ohPiece,.ohLine,.ohTag,.ohPill,.ohBadge,.ohItem,.ohNum,.ohLine .ohDash,.ohc,.ohcThumb,.ohcClose{transition:none}.ohcSpin{animation-duration:1.8s}}`;
  function ensureCss() {
    if (!doc || !doc.head || doc.getElementById('optionsHistoryCss')) return;
    const s = doc.createElement('style');
    s.id = 'optionsHistoryCss';
    s.textContent = CSS;
    doc.head.appendChild(s);
  }
  ensureCss();

  const api = { svg, timeline, view, bind, filter, groups, card, bindCards, collapse: collapseCards, shortDate, greyOf, friendly, ensureCss, css: CSS, GREEN, SHEET, TRAY };
  root.OptionsHistory = api;
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
})(typeof window !== 'undefined' ? window : globalThis);
