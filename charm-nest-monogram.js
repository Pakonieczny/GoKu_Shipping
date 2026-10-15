/*  charm-nest-monogram.js — the pure MONOGRAM engine (Paul, 15 Oct 2026).
 *  Three buyer letters, taken from three alphabets (Left / Middle / Right), become ONE interconnected charm piece:
 *  the centre letter bigger than the two sides, each side overlapping the centre until the three are one piece, every
 *  letter keeping its shape and its inner openings (counters). Nothing here touches the page, the network or a font:
 *  rings in (points, y up), rings out. Works as window.CharmNestMonogram and through require().
 *
 *    lettersOf(text)            → { ok, letters: ['A','P','V'] | null, why }          what the buyer wrote
 *    familiesOf(skus)           → [{ base, label, letters:{A:'SKU'}, count, missing }]  the 26-letter alphabets a SKU list holds
 *    build({left,middle,right}, opts) → { ok, why, rings, bboxPt, wMm, hMm, placed, report }
 *    toPdf(result, { title })   → Promise<Uint8Array>   one page, one charm, holes as separate stroked paths
 *    svgPath(result)            → SVG path data (even-odd, y flipped for the screen)
 *
 *  Geometry: the vendored Clipper 6.4.2 (integer scale 1000 per pt), exactly as charm-nest-vector.js loads it. build() never throws.  */
(function (root) {
  'use strict';
  const MM = 25.4 / 72, SC = 1000, SLIVER_PT2 = 0.05 / (MM * MM), JOIN_NOTE_PT2 = 1.0 / (MM * MM);
  const STEP = 0.04, MIN_OVERLAP = 0.02, HOLES_MIN = 0.9, OVERLAP_MAX_PCT = 35;
  const clipper = () => root.ClipperLib || (typeof require === 'function' ? require('./vendor/clipper-6.4.2.js') : null);
  const pdfLib = () => root.PDFLib || (typeof require === 'function' ? require('./vendor/pdf-lib-1.17.1.min.js') : null);

  /* ═══ 1 · what the buyer wrote ═══════════════════════════════════════════════════════════════════════════════════ */
  const FOLD = { 'Ø': 'O', 'ø': 'o', 'Ł': 'L', 'ł': 'l', 'Đ': 'D', 'đ': 'd', 'Ð': 'D', 'ð': 'd', 'Ħ': 'H', 'ħ': 'h', 'ı': 'i' };
  // Words that may stand round the letters. Only KNOWN words are ever dropped: "Initials: ASK" is ASK, but "Jane Doe" is a name, not the letters D O E (never guess).
  //   LABEL words can never be initials; STOP words are short and could be (a buyer who wants A N D), so they go only when the letters are otherwise not three.
  const LABEL = new Set(('initials initial monogram monograms monogrammed letters letter please thanks thank personalization personalisation personalized personalised custom customized ' +
    'engrave engraved engraving charm charms necklace name names text order these this with using hello kindly request write make want need like').split(' '));
  const STOP = new Set('my is to of in on at by it me we us so as or if an no for the and are you our her his she him has was not but can its nor per pls plz'.split(' '));
  const plural = n => n + ' letter' + (n === 1 ? '' : 's');

  function lettersOf(text) {
    const bad = why => ({ ok: false, letters: null, why });
    if (text == null) return bad('nothing written, a monogram needs 3 letters');
    let s;
    try { s = String(text); } catch (e) { return bad('nothing readable was written'); }
    s = s.normalize('NFKD').replace(/\p{M}+/gu, '').replace(/[ØøŁłĐđÐðĦħı]/g, c => FOLD[c]);
    const runs = s.match(/[A-Za-z]+/g) || [];
    const count = rs => rs.reduce((n, r) => n + r.length, 0);
    const pick = rs => ({ ok: true, letters: rs.join('').toUpperCase().split(''), why: '' });
    if (!runs.length) return bad(/\d/.test(s) ? 'only digits, a monogram needs 3 letters' : 'no letters, a monogram needs 3');
    if (count(runs) === 3) return pick(runs);
    const noLabel = runs.filter(r => !LABEL.has(r.toLowerCase()));
    if (noLabel.length < runs.length && count(noLabel) === 3) return pick(noLabel);
    const core = noLabel.filter(r => !STOP.has(r.toLowerCase()));
    if (core.length < runs.length && count(core) === 3) return pick(core);
    if (!noLabel.length) return bad('no letters written, only words');
    const n = count(core) || count(noLabel);
    return bad(plural(n) + ', a monogram needs 3');
  }

  /* ═══ 2 · the 26-letter alphabets a SKU list holds ═══════════════════════════════════════════════════════════════
     Three shapes (case does not matter, the SKU text itself is kept):
       letter   STEM_A  STEM-A  STEM A  STEMA (only when the stem is 4+ characters)       a single trailing A–Z
       index    STEM0 … STEM25  or  STEM1 … STEM26  (with or without _ - space)           the shop's INDEXED families: 0 = A (or 1 = A)
       code     STEM_A_1234  (a 4-digit shop code shared by the whole alphabet)           the letter just before the code
     A family needs 20+ distinct letters; a stem shared by fewer is not one.                                                          */
  const SEP = '_- ';
  const isSep = c => c !== undefined && SEP.includes(c);
  const LETTER = /^[A-Z]$/;
  function shapeOf(u) {
    const n = u.length; let m;
    if ((m = /^(.*[^\d])(\d{4})$/.exec(u))) {                       // code: STEM_A_1234
      const rest = m[1];
      if (isSep(rest[rest.length - 1])) {
        const r2 = rest.slice(0, -1), L = r2[r2.length - 1];
        if (L && LETTER.test(L) && r2.length >= 2) {
          const p = r2[r2.length - 2];
          if (isSep(p) && r2.length >= 3) return { shape: 'code', stem: r2.slice(0, -2), sep: p, key: L, tail: m[2] };
          if (!isSep(p) && /[A-Z0-9]/.test(p) && r2.length - 1 >= 4) return { shape: 'code', stem: r2.slice(0, -1), sep: '', key: L, tail: m[2] };
        }
      }
      return null;
    }
    if ((m = /^(.*[^\d])(\d{1,2})$/.exec(u))) {                     // index: STEM_12
      let stem = m[1], sep = '';
      if (isSep(stem[stem.length - 1])) { sep = stem[stem.length - 1]; stem = stem.slice(0, -1); }
      if (!stem.trim()) return null;
      return { shape: 'index', stem, sep, key: +m[2], tail: '' };
    }
    if (n >= 2 && LETTER.test(u[n - 1])) {                          // letter: STEM_A / STEMA
      const p = u[n - 2];
      if (isSep(p)) { const stem = u.slice(0, n - 2); return stem.trim() ? { shape: 'letter', stem, sep: p, key: u[n - 1], tail: '' } : null; }
      if (/[A-Z0-9]/.test(p) && n - 1 >= 4) return { shape: 'letter', stem: u.slice(0, n - 1), sep: '', key: u[n - 1], tail: '' };
    }
    return null;
  }
  const ALPHA = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
  const pretty = stem => stem.replace(/[_\-\s]+/g, ' ').trim().toUpperCase();

  function familiesOf(skus) {
    const groups = new Map();
    for (const raw of Array.isArray(skus) ? skus : []) {
      if (typeof raw !== 'string') continue;
      const t = raw.trim(); if (!t || t.length > 120) continue;
      const u = t.toUpperCase(), f = shapeOf(u); if (!f) continue;
      const gk = f.shape + '|' + f.stem + '|' + f.sep + '|' + f.tail;
      let g = groups.get(gk); if (!g) groups.set(gk, g = { f, members: new Map() });
      const cur = g.members.get(f.key);
      if (cur === undefined || raw < cur) g.members.set(f.key, raw);   // two spellings of one letter: the smaller text, always the same one
    }
    const out = [];
    for (const { f, members } of groups.values()) {
      let letters = {}, shape = f.shape;
      if (f.shape === 'index') {
        const idx = [...members.keys()];
        if (idx.some(i => i > 26)) continue;                         // a numbered series that runs past 26 is a design series, not an alphabet
        const has0 = members.has(0), has26 = members.has(26);
        if (has0 && has26) continue;                                  // 27 members: no way to say which end is A
        const off = has0 ? 0 : 1; shape = off ? 'index1' : 'index0';
        for (const [i, sku] of members) { const k = i - off; if (k >= 0 && k < 26) letters[ALPHA[k]] = sku; }
      } else for (const [L, sku] of members) letters[L] = sku;
      const have = Object.keys(letters).sort();
      if (have.length < 20) continue;
      const sorted = {}; for (const L of have) sorted[L] = letters[L];
      const base = (f.stem + f.sep + (f.shape === 'index' ? '{N}' : '{L}') + (f.shape === 'code' ? f.sep + f.tail : ''));
      out.push({ base, label: pretty(f.stem) + (f.shape === 'code' ? ' ' + f.tail : ''), letters: sorted, count: have.length, missing: [...ALPHA].filter(L => !letters[L]), shape });
    }
    const seen = new Map(); for (const x of out) seen.set(x.label, (seen.get(x.label) || 0) + 1);
    for (const x of out) if (seen.get(x.label) > 1) x.label += ' (' + x.base + ')';
    return out.sort((a, b) => b.count - a.count || (a.label < b.label ? -1 : a.label > b.label ? 1 : 0) || (a.base < b.base ? -1 : a.base > b.base ? 1 : 0));
  }

  /* ═══ 3 · geometry ═══════════════════════════════════════════════════════════════════════════════════════════════ */
  const SLOTS = ['left', 'middle', 'right'];
  const finite = v => typeof v === 'number' && Number.isFinite(v);
  const bboxOf = paths => { let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity; for (const p of paths) for (const q of p) { if (q.X < x0) x0 = q.X; if (q.X > x1) x1 = q.X; if (q.Y < y0) y0 = q.Y; if (q.Y > y1) y1 = q.Y; } return [x0, y0, x1, y1]; };
  const shift = (paths, dx, dy) => paths.map(p => p.map(q => ({ X: q.X + dx, Y: q.Y + dy })));
  const scaled = (paths, s) => s === 1 ? paths : paths.map(p => p.map(q => ({ X: Math.round(q.X * s), Y: Math.round(q.Y * s) })));
  function round3(v) { return Math.round(v * 1000) / 1000; }

  function engine(C) {
    const area = p => C.Clipper.Area(p);
    const clean = paths => { const out = []; for (const p of paths) { const c = C.Clipper.CleanPolygon(p.map(q => ({ X: q.X, Y: q.Y })), 0.01); if (c && c.length >= 3 && Math.abs(area(c)) > 0) out.push(c); } return out; };
    const run = (type, subj, clip, sf, cf, simple) => {
      const c = new C.Clipper(simple ? C.Clipper.ioStrictlySimple : 0), sol = new C.Paths();
      if (subj.length) c.AddPaths(subj, C.PolyType.ptSubject, true);
      if (clip && clip.length) c.AddPaths(clip, C.PolyType.ptClip, true);
      if (!c.Execute(type, sol, sf, cf)) return [];
      return sol;
    };
    const netArea = paths => paths.reduce((a, p) => a + area(p), 0);
    return {
      area, clean, netArea,
      regionOf: rings => run(C.ClipType.ctUnion, rings, null, C.PolyFillType.pftEvenOdd, C.PolyFillType.pftEvenOdd, false),
      union: regions => run(C.ClipType.ctUnion, regions, null, C.PolyFillType.pftNonZero, C.PolyFillType.pftNonZero, true),
      meet: (a, b) => run(C.ClipType.ctIntersection, a, b, C.PolyFillType.pftNonZero, C.PolyFillType.pftNonZero, false),
      meetEO: (a, b) => run(C.ClipType.ctIntersection, a, b, C.PolyFillType.pftNonZero, C.PolyFillType.pftEvenOdd, false),
      merge: counters => run(C.ClipType.ctUnion, counters, null, C.PolyFillType.pftNonZero, C.PolyFillType.pftNonZero, false)
    };
  }

  /** One letter as the user drew it → its filled region (even-odd over its rings), normalised so its own bbox starts at 0,0 (integer 0.001 pt). */
  function prepLetter(E, spec, slot) {
    const name = () => `the ${slot} letter` + (spec && spec.letter ? ` (${String(spec.letter).slice(0, 3)})` : '');
    if (!spec || typeof spec !== 'object' || !Array.isArray(spec.rings)) return { why: `${name()} is missing` };
    const raw = [];
    for (const ring of spec.rings) {
      if (!Array.isArray(ring)) continue;
      const pts = [];
      for (const p of ring) {
        const x = Array.isArray(p) ? p[0] : p && p.x, y = Array.isArray(p) ? p[1] : p && p.y;
        if (!finite(x) || !finite(y)) return { why: `${name()} has a bad coordinate` };
        if (Math.abs(x) > 1e6 || Math.abs(y) > 1e6) return { why: `${name()} is far too large` };
        pts.push([x, y]);
      }
      if (pts.length >= 3) raw.push(pts);
    }
    if (!raw.length) return { why: `${name()} has no outline` };
    let x0 = Infinity, y0 = Infinity; for (const r of raw) for (const p of r) { if (p[0] < x0) x0 = p[0]; if (p[1] < y0) y0 = p[1]; }
    const ints = E.clean(raw.map(r => r.map(p => ({ X: Math.round((p[0] - x0) * SC), Y: Math.round((p[1] - y0) * SC) }))));
    if (!ints.length) return { why: `${name()} has no outline` };
    let region = E.regionOf(ints);
    if (!region.length) return { why: `${name()} has no outline` };
    const b = bboxOf(region); region = shift(region, -b[0], -b[1]);
    const w = b[2] - b[0], h = b[3] - b[1], a = E.netArea(region);
    if (w < 0.05 * SC || h < 0.05 * SC || a < 0.01 * SC * SC) return { why: `${name()} has no outline` };
    return { region, w, h, area: a, name: name() };
  }

  function normOpts(o) {
    o = o && typeof o === 'object' ? o : {};
    const num = (v, d, lo, hi) => (finite(v) ? Math.min(hi, Math.max(lo, v)) : d);
    const overlap = num(o.overlap, 0.18, 0, 0.9);
    return {
      overlap, maxOverlap: Math.max(overlap, num(o.maxOverlap, 0.38, 0, 0.95)),
      centerRatio: Math.max(1.2, num(o.centerRatio, 1.5, 1.2, 5)), valign: o.valign === 'baseline' ? 'baseline' : 'center'
    };
  }

  function failure(why, extra) {
    return { ok: false, why, rings: [], bboxPt: null, wMm: 0, hMm: 0, placed: [], report: Object.assign({ connected: false, overlapPct: null, holesKept: null, centerScale: null, tries: 0, notes: [] }, extra || {}) };
  }

  function build(letters, options) {
    try { return buildInner(letters, options); }
    catch (e) { return failure('the letters could not be combined (' + String(e && e.message || e).slice(0, 120) + ')'); }
  }

  function buildInner(letters, options) {
    const C = clipper(); if (!C) return failure('the geometry library is not loaded, refresh the page');
    const E = engine(C), o = normOpts(options);
    if (!letters || typeof letters !== 'object') return failure('the three letters are missing');
    const L = [];
    for (const slot of SLOTS) { const p = prepLetter(E, letters[slot], slot); if (p.why) return failure(p.why); p.slot = slot; p.letter = letters[slot].letter == null ? '' : String(letters[slot].letter); L.push(p); }
    const [left, mid, right] = L;
    const notes = [];

    // size: sides keep their relative size unless they differ by more than 10 % (then the smaller one is raised to the larger); the middle is the biggest
    let sl = 1, sr = 1;
    if (left.h > right.h * 1.1) sr = left.h / right.h; else if (right.h > left.h * 1.1) sl = right.h / left.h;
    const sideH = Math.max(left.h * sl, right.h * sr);
    const cs = mid.h >= 1.25 * sideH ? 1 : Math.max(1, o.centerRatio * sideH / mid.h);
    const placed0 = [[left, sl], [mid, cs], [right, sr]].map(([p, s]) => ({ p, s, region: scaled(p.region, s), w: Math.round(p.w * s), h: Math.round(p.h * s) }));
    const [PL, PM, PR] = placed0;
    if (PM.h < 1.2 * Math.max(PL.h, PR.h) - 2) notes.push('the middle letter is less than 1.2 times the side letters');
    if (cs !== 1) notes.push(`middle letter scaled ${round3(cs)} times to stand out`);
    if (sl !== 1 || sr !== 1) notes.push(`the ${sl !== 1 ? 'left' : 'right'} letter was raised ${round3(Math.max(sl, sr))} times to match the other side`);
    const area0 = p => E.area(p);

    function place(ov) {
      const dyOf = P => (o.valign === 'baseline' ? 0 : Math.round((PM.h - P.h) / 2));
      const at = [
        [Math.round(ov * PL.w) - PL.w, dyOf(PL)], [0, 0], [PM.w - Math.round(ov * PR.w), dyOf(PR)]
      ];
      return placed0.map((P, i) => Object.assign({}, P, { dx: at[i][0], dy: at[i][1], rg: shift(P.region, at[i][0], at[i][1]) }));
    }
    function attempt(ov) {
      const pl = place(ov);
      let rings = E.clean(E.union([].concat(pl[0].rg, pl[1].rg, pl[2].rg)));
      rings = rings.filter(r => Math.abs(area0(r)) >= SLIVER_PT2 * SC * SC);
      const outers = rings.filter(r => area0(r) > 0).length;
      return { ov, pl, rings, outers, connected: outers === 1 };
    }
    function guards(att) {
      const [a, b, c] = att.pl;
      const reg = x => x.rg, pct = (p, q) => { const i = E.netArea(E.meet(reg(p), reg(q))), s = Math.min(p.p.area * p.s * p.s, q.p.area * q.s * q.s); return { i, pct: s > 0 ? Math.max(0, i / s * 100) : 0 }; };
      const lm = pct(a, b), mr = pct(b, c);
      // counters: the inner openings of the three letters, placed. Only a counter that reaches another letter's box can have been covered.
      const boxes = att.pl.map(x => bboxOf(x.rg)), all = [], near = [];
      att.pl.forEach((x, i) => { for (const r of x.rg) if (area0(r) < 0) {
        const cr = r.slice().reverse(), bb = bboxOf([r]); all.push(cr);
        if (boxes.some((q, j) => j !== i && bb[0] < q[2] && bb[2] > q[0] && bb[1] < q[3] && bb[3] > q[1])) near.push(cr); } });
      let kept = 1;
      if (all.length) {
        const tot = E.netArea(E.merge(all));
        if (tot > 0 && near.length) kept = Math.min(1, Math.max(0, 1 - E.netArea(E.meetEO(E.merge(near), att.rings)) / tot));
      }
      const over = Math.max(0, Math.max(lm.pct, mr.pct) - OVERLAP_MAX_PCT);
      return { holesKept: kept, lm, mr, ok: kept >= HOLES_MIN && over === 0, penalty: Math.max(0, HOLES_MIN - kept) + over / 100 };
    }

    // connect: from the start overlap upward until the three letters are one piece
    let tries = 0, first = null, lastTried = o.overlap;
    const at = k => Math.round((o.overlap + k * STEP) * 1e6) / 1e6;
    for (let k = 0; at(k) <= o.maxOverlap + 1e-9; k++) { const att = attempt(at(k)); tries++; lastTried = at(k); if (att.connected) { first = att; break; } if (k > 40) break; }
    if (!first) {
      // the letters do not touch, or a letter is made of separate parts that stay loose even though the main bodies join?
      const bodies = place(lastTried).map(P => { let top = null, ta = 0; for (const r of P.rg) { const a = area0(r); if (a > ta) { ta = a; top = r; } } return top; }).filter(Boolean);
      const joined = E.union(bodies).filter(r => area0(r) > 0).length === 1;
      return failure(joined ? 'a letter is made of separate parts that stay loose, it cannot be one charm' : 'the letters do not touch', { tries });
    }
    let best = { att: first, g: guards(first) };
    if (!best.g.ok) {
      for (let k = Math.round((first.ov - o.overlap) / STEP) - 1; at(k) >= MIN_OVERLAP - 1e-9; k--) {
        const att = attempt(at(k)); tries++;
        if (!att.connected) break;
        const g = guards(att);
        if (g.penalty < best.g.penalty - 1e-12) best = { att, g };
        if (g.ok) break;
      }
    }
    const { att, g } = best;
    if (att.ov > o.overlap + 1e-9) notes.push(`overlap raised from ${Math.round(o.overlap * 100)} % to ${Math.round(att.ov * 100)} % so the letters join`);
    else if (att.ov < o.overlap - 1e-9) notes.push(`overlap lowered from ${Math.round(o.overlap * 100)} % to ${Math.round(att.ov * 100)} % to keep the inner openings`);
    if (g.holesKept < HOLES_MIN) notes.push(`only ${Math.round(g.holesKept * 100)} % of the inner openings stay open`);
    if (g.lm.pct > OVERLAP_MAX_PCT) notes.push(`the left and middle letters overlap by ${Math.round(g.lm.pct)} % of the smaller letter`);
    if (g.mr.pct > OVERLAP_MAX_PCT) notes.push(`the middle and right letters overlap by ${Math.round(g.mr.pct)} % of the smaller letter`);
    if (g.lm.i < JOIN_NOTE_PT2 * SC * SC) notes.push('the join between the left and middle letters is thin');
    if (g.mr.i < JOIN_NOTE_PT2 * SC * SC) notes.push('the join between the middle and right letters is thin');

    // the finished piece: outer ring CCW first, then the holes CW, largest first; everything shifted so the piece starts at 0,0
    const b = bboxOf(att.rings), W = b[2] - b[0], H = b[3] - b[1];
    const toPt = r => { const pts = r.map(q => [(q.X - b[0]) / SC, (q.Y - b[1]) / SC]); return pts; };
    const outer = att.rings.find(r => area0(r) > 0);
    const holes = att.rings.filter(r => area0(r) < 0).sort((p, q) => area0(p) - area0(q) || bboxOf([p])[1] - bboxOf([q])[1] || bboxOf([p])[0] - bboxOf([q])[0]);
    const rings = [{ pts: toPt(outer), hole: false }].concat(holes.map(h => ({ pts: toPt(h), hole: true })));
    const placed = att.pl.map(P => ({ letter: P.p.letter, slot: P.p.slot, scale: round3(P.s), bboxPt: [P.dx, P.dy, P.dx + P.w, P.dy + P.h].map((v, i) => round3((v - b[i % 2]) / SC)) }));
    return {
      ok: true, why: '', rings, bboxPt: [0, 0, round3(W / SC), round3(H / SC)], wMm: Math.round(W / SC * MM * 1e4) / 1e4, hMm: Math.round(H / SC * MM * 1e4) / 1e4, placed,
      report: { connected: true, overlapPct: { leftMiddle: Math.round(g.lm.pct * 10) / 10, middleRight: Math.round(g.mr.pct * 10) / 10 }, holesKept: round3(g.holesKept), centerScale: round3(cs), overlap: att.ov, joinMm2: { leftMiddle: Math.round(g.lm.i / SC / SC * MM * MM * 100) / 100, middleRight: Math.round(g.mr.i / SC / SC * MM * MM * 100) / 100 }, tries, notes }
    };
  }
  /* ═══ 4 · the preview and the file ═══════════════════════════════════════════════════════════════════════════════ */
  const num = v => { const s = (Math.round(v * 1000) / 1000).toFixed(3).replace(/\.?0+$/, ''); return s === '-0' ? '0' : s; };
  function svgPath(result) {
    if (!result || !result.ok || !Array.isArray(result.rings)) return '';
    const b = result.bboxPt, H = b[3] - b[1];
    return result.rings.map(r => r.pts.map((p, i) => (i ? 'L' : 'M') + num(p[0] - b[0]) + ' ' + num(H - (p[1] - b[1]))).join('') + 'Z').join('');
  }

  async function toPdf(result, o) {
    if (!result || !result.ok || !Array.isArray(result.rings) || !result.rings.length) throw new Error('there is no finished monogram to write' + (result && result.why ? ': ' + result.why : ''));
    const PL = pdfLib(); if (!PL) throw new Error('the PDF library is not loaded, refresh the page');
    const { PDFDocument, PDFName } = PL, b = result.bboxPt, bw = b[2] - b[0], bh = b[3] - b[1];
    const pad = Math.max(0.5 / 2 + 2, 0.125 * Math.max(bw, bh)), W = bw + 2 * pad, H = bh + 2 * pad;   // the page rule of buildSingleCharm
    const doc = await PDFDocument.create({ updateMetadata: false }), page = doc.addPage([W, H]);
    const ops = ['q 0 0 0 RG 0.5 w 1 J 1 j'];
    for (const r of result.rings) ops.push(r.pts.map((p, i) => `${num(p[0] - b[0] + pad)} ${num(p[1] - b[1] + pad)} ${i ? 'l' : 'm'}`).join(' ') + ' h S');
    ops.push('Q');
    page.node.set(PDFName.of('Contents'), doc.context.obj([doc.context.register(doc.context.flateStream(ops.join('\n') + '\n'))]));
    const title = o && o.title != null ? String(o.title).slice(0, 200) : 'Monogram';
    doc.setTitle(title); doc.setProducer('Charm Nest monogram'); doc.setCreator('Charm Nest monogram');
    return await doc.save({ useObjectStreams: false });
  }

  const api = { lettersOf, familiesOf, build, toPdf, svgPath };
  root.CharmNestMonogram = api;
  if (typeof module === 'object' && module.exports) module.exports = api;
})(typeof window !== 'undefined' ? window : globalThis);
