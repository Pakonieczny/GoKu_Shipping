/*  charm-nest-monogram.js — the pure MONOGRAM engine (Paul, 15 Oct 2026).
 *  Three buyer letters, taken from three alphabets (Left / Middle / Right), become ONE interconnected charm piece:
 *  the centre letter bigger than the two sides, each side overlapping the centre until the three are one piece, every
 *  letter keeping its shape and its inner openings (counters). Nothing here touches the page, the network or a font:
 *  rings in (points, y up), rings out. Works as window.CharmNestMonogram and through require().
 *
 *    lettersOf(text)            → { ok, letters: ['A','P','V'] | null, why }          what the buyer wrote
 *    familiesOf(skus)           → [{ base, label, name, letters:{A:'SKU'}, count, missing }]  the 26-letter alphabets a SKU list holds
 *    slotsOf(families, saved)   → { left, middle, right, ok, sets, why }               which alphabet is the Left, the Middle and the Right (by the words in their names, or the saved choice)
 *    build({left,middle,right}, opts) → { ok, why, rings, bboxPt, wMm, hMm, placed, report }
 *        a letter is { letter, rings, fill? }: fill 'auto' reads a font's overlapping contours as ink, the default reads a ring inside another as a counter (fillOf);
 *        opts { overlap, maxOverlap, centerRatio, valign, reach }; report { connected, joinMm: { leftMiddle, middleRight }, holesKept, centerScale, overlap, warnings[], notes[], … }
 *    toPdf(result, { title })   → Promise<Uint8Array>   one page, one charm, holes as separate stroked paths
 *    svgPath(result)            → SVG path data (even-odd, y flipped for the screen)
 *
 *  Geometry: the vendored Clipper 6.4.2 (integer scale 1000 per pt), exactly as charm-nest-vector.js loads it. build() never throws.  */
(function (root) {
  'use strict';
  const MM = 25.4 / 72, SC = 1000, SLIVER_PT2 = 0.05 / (MM * MM);
  const STEP = 0.04, MIN_OVERLAP = 0.02, HOLES_MIN = 0.9, OVERLAP_MAX_PCT = 35;
  /* A join that holds (MONOGRAMUI, Paul, 15 Oct 2026, on the real Left / Mid / Right alphabets: thin script strokes): "the letters touch" is not enough, two hairlines that only graze are one piece for the file and
     break in the hand. A join is the overlap of two letters; its width is the widest disc that fits inside it (a disc 0.3 mm across: two crossing strokes of 0.3 mm). Under NECK_FLOOR_MM it is called a hairline.
     When the first overlap that connects gives a weaker join than NECK_OK_MM, each side is searched again over a wider overlap (up to REACH_HI of the side letter's width) and a small move up or down (REACH_DY of the
     middle's height per step, REACH_DYN steps each way), nearest to the asked overlap first, for the first placement whose join holds and whose letters keep their shape (35 % overlap, 90 % of the inner openings). */
  const NECK_OK_MM = 0.3, NECK_FLOOR_MM = 0.15, REACH_LO = 0.06, REACH_HI = 0.8, REACH_DY = 0.05, REACH_DYN = 3;
  /* The inner openings that count when it is asked whether a neighbour's stroke has closed them: those of 0.5 mm2 and more (a quarter of the counters of the real Left / Mid / Right alphabets are smaller: the eyes of a flourish,
     0.5 mm across, which a laser cannot keep open anyway and nobody reads as part of the letter). A smaller one that survives is still a hole of the piece. */
  const COUNTER_MIN_PT2 = 0.5 / (MM * MM);
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
  const nice = label => label.toLowerCase().replace(/(^|[ (])([a-z])/g, (m, a, b) => a + b.toUpperCase());   // MONOGRAM LEFT -> Monogram Left

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
      out.push({ base, label: pretty(f.stem) + (f.shape === 'code' ? ' ' + f.tail : ''), letters: sorted, count: have.length, missing: [...ALPHA].filter(L => !letters[L]), shape, stem: pretty(f.stem) });
    }
    const seen = new Map(); for (const x of out) seen.set(x.label, (seen.get(x.label) || 0) + 1);
    for (const x of out) if (seen.get(x.label) > 1) x.label += ' (' + x.base + ')';
    for (const x of out) x.name = nice(x.label);
    return out.sort((a, b) => b.count - a.count || (a.label < b.label ? -1 : a.label > b.label ? 1 : 0) || (a.base < b.base ? -1 : a.base > b.base ? 1 : 0));
  }

  /* Which alphabet is the Left one, the Middle one and the Right one (MONOGRAMUI, Paul, 15 Oct 2026: "Make sure the system understands these three alphabets"). His file names them Monogram_Left_A … Monogram_Mid_Z …
     Monogram_Right_Z; the library upper-cases a SKU, so the families come as MONOGRAM_LEFT_{L}, MONOGRAM_MID_{L}, MONOGRAM_RIGHT_{L}. A family says its place with a WORD of its stem: LEFT; MID, MIDDLE, CENTER, CENTRE or
     CENTRAL; RIGHT (never a bare letter: "LETTER M" is not a middle). Families that differ only by that word are one SET ("Monogram"); several complete sets are all offered, the first (the biggest, then the one
     called Monogram, then by name) is taken. `saved` ({ left, middle, right }: family bases a person chose) wins for the slot it names. Never guesses: a word that two families share leaves that slot empty.
       → { left, middle, right: family | null, ok, sets: [{ name, left, middle, right }], by: { left: 'name'|'saved'|'', ... }, why } */
  const SLOT_WORDS = { left: ['LEFT'], middle: ['MID', 'MIDDLE', 'CENTER', 'CENTRE', 'CENTRAL'], right: ['RIGHT'] };
  function slotOfFamily(f) {
    const words = String(f.stem || f.label || '').toUpperCase().split(/[^A-Z0-9]+/).filter(Boolean), hit = [];
    for (const slot of Object.keys(SLOT_WORDS)) if (words.some(w => SLOT_WORDS[slot].includes(w))) hit.push(slot);
    if (hit.length !== 1) return null;                                 // no place word, or two ("LEFT RIGHT"): not placed by its name
    const rest = words.filter(w => !SLOT_WORDS[hit[0]].includes(w)).join(' ');
    return { slot: hit[0], set: rest };
  }
  function slotsOf(fams, saved) {
    fams = Array.isArray(fams) ? fams.filter(f => f && typeof f === 'object' && f.letters) : []; saved = saved && typeof saved === 'object' ? saved : {};
    const sets = new Map();
    for (const f of fams) { const p = slotOfFamily(f); if (!p) continue; let g = sets.get(p.set); if (!g) sets.set(p.set, g = { name: p.set, left: [], middle: [], right: [] }); g[p.slot].push(f); }
    const full = [...sets.values()].filter(g => g.left.length === 1 && g.middle.length === 1 && g.right.length === 1)
      .map(g => ({ name: g.name ? nice(g.name) : '', left: g.left[0], middle: g.middle[0], right: g.right[0] }));
    full.sort((a, b) => Math.min(b.left.count, b.middle.count, b.right.count) - Math.min(a.left.count, a.middle.count, a.right.count) || (/MONOGRAM/i.test(b.name) ? 1 : 0) - (/MONOGRAM/i.test(a.name) ? 1 : 0) || (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
    const out = { left: null, middle: null, right: null, ok: false, sets: full, by: { left: '', middle: '', right: '' }, why: '' };
    for (const slot of ['left', 'middle', 'right']) {
      const s = saved[slot] && fams.find(f => f.base === saved[slot]);
      if (s) { out[slot] = s; out.by[slot] = 'saved'; }
      else if (full.length) { out[slot] = full[0][slot]; out.by[slot] = 'name'; }
      else {   // no complete set: a slot a single family names is still that family
        const named = fams.filter(f => { const p = slotOfFamily(f); return p && p.slot === slot; });
        if (named.length === 1) { out[slot] = named[0]; out.by[slot] = 'name'; }
      }
    }
    out.ok = !!(out.left && out.middle && out.right);
    if (!out.ok) {
      const miss = ['left', 'middle', 'right'].filter(k => !out[k]).map(k => k === 'middle' ? 'Mid' : k[0].toUpperCase() + k.slice(1));
      out.why = !fams.length ? 'no 26-letter alphabet in the library' : `no ${miss.join(' / ')} alphabet found: name them like Monogram_Left_A, Monogram_Mid_A, Monogram_Right_A, or choose them`;
    }
    return out;
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
      regionNZ: rings => run(C.ClipType.ctUnion, rings, null, C.PolyFillType.pftNonZero, C.PolyFillType.pftNonZero, false),
      union: regions => run(C.ClipType.ctUnion, regions, null, C.PolyFillType.pftNonZero, C.PolyFillType.pftNonZero, true),
      meet: (a, b) => run(C.ClipType.ctIntersection, a, b, C.PolyFillType.pftNonZero, C.PolyFillType.pftNonZero, false),
      meetEO: (a, b) => run(C.ClipType.ctIntersection, a, b, C.PolyFillType.pftNonZero, C.PolyFillType.pftEvenOdd, false),
      merge: counters => run(C.ClipType.ctUnion, counters, null, C.PolyFillType.pftNonZero, C.PolyFillType.pftNonZero, false),
      // the paths pulled inward by rPt (what is left when every edge moves in): empty when no disc of that radius fits
      erode: (paths, rPt) => { const co = new C.ClipperOffset(2, 0.02 * SC), out = new C.Paths(); co.AddPaths(paths, C.JoinType.jtMiter, C.EndType.etClosedPolygon); co.Execute(out, -rPt * SC); return out; }
    };
  }

  /* How a letter's rings make its filled shape. A letter is a list of rings, an outline and its counters; a ring inside another is a counter, whichever way it runs (even-odd: the default of every caller that
     does not say). A font's glyphs are different: their contours may OVERLAP (the tail of a Q lies across its loop, a flourish crosses a stem), the overlap is solid ink (non-zero winding) and an even-odd reading
     turns it into holes and cuts the letter in pieces (the real Mid Q: three rings that cross each other). `fill: 'nonzero'` reads the rings by their direction; `fill: 'auto'` (what the page asks for) reads them
     by direction when two rings partly cross each other (overlapping contours: only a font does that) and as counters otherwise, so a file drawn either way gives the letter its designer meant. */
  function fillOf(E, ints, mode) {
    if (mode === 'nonzero') return E.regionNZ(ints);
    const eo = E.regionOf(ints); if (mode !== 'auto' || ints.length < 2) return eo;
    const nz = E.regionNZ(ints), ae = E.netArea(eo), an = E.netArea(nz);
    if (Math.abs(ae - an) <= 1e-4 * Math.max(Math.abs(ae), Math.abs(an), 1)) return eo;      // (the same shape either way: nothing overlaps)
    for (let i = 0; i < ints.length; i++) for (let j = i + 1; j < ints.length; j++) {
      const x = E.netArea(E.meet([ints[i]], [ints[j]]));
      if (x <= 0) continue;
      const si = x / Math.abs(E.area(ints[i])), sj = x / Math.abs(E.area(ints[j]));
      if (Math.max(si, sj) < 0.995 && Math.min(si, sj) > 0.002) return nz;                    // two rings that partly cross: overlapping contours
    }
    return eo;
  }
  /** One letter as the user drew it → its filled region (see fillOf), normalised so its own bbox starts at 0,0 (integer 0.001 pt). */
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
    let region = fillOf(E, ints, spec.fill);
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
      centerRatio: Math.max(1.2, num(o.centerRatio, 1.5, 1.2, 5)), valign: o.valign === 'baseline' ? 'baseline' : 'center', reach: o.reach !== false
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
    const placed0 = [[left, sl], [mid, cs], [right, sr]].map(([p, s]) => {
      const region = scaled(p.region, s), counters = region.filter(r => E.area(r) <= -COUNTER_MIN_PT2 * SC * SC).map(r => r.slice().reverse());
      const bodies = region.filter(r => E.area(r) >= SLIVER_PT2 * SC * SC);   // (the separate parts a letter is made of: a stroke apart from the rest, a dot; a speck is not one)
      return { p, s, region, w: Math.round(p.w * s), h: Math.round(p.h * s), ar: E.netArea(region), counters, cbox: counters.map(c => bboxOf([c])), cAr: E.netArea(counters), bodies };
    });
    const [PL, PM, PR] = placed0;
    if (PM.h < 1.2 * Math.max(PL.h, PR.h) - 2) notes.push('the middle letter is less than 1.2 times the side letters');
    if (cs !== 1) notes.push(`middle letter scaled ${round3(cs)} times to stand out`);
    if (sl !== 1 || sr !== 1) notes.push(`the ${sl !== 1 ? 'left' : 'right'} letter was raised ${round3(Math.max(sl, sr))} times to match the other side`);
    const area0 = p => E.area(p);
    const dyOf = P => (o.valign === 'baseline' ? 0 : Math.round((PM.h - P.h) / 2));
    const xOf = (P, side, ov) => side === 'left' ? Math.round(ov * PL.w) - PL.w : PM.w - Math.round(ov * PR.w);

    // a placement: where each side letter stands (x, y of its own box) beside the middle at 0,0
    function placeAt(L, R) {
      const at = [L, [0, 0], R];
      return placed0.map((P, i) => Object.assign({}, P, { dx: at[i][0], dy: at[i][1], rg: shift(P.region, at[i][0], at[i][1]) }));
    }
    const place = ov => placeAt([xOf(PL, 'left', ov), dyOf(PL)], [xOf(PR, 'right', ov), dyOf(PR)]);
    function attemptPl(pl, ov) {
      let rings = E.clean(E.union([].concat(pl[0].rg, pl[1].rg, pl[2].rg)));
      rings = rings.filter(r => Math.abs(area0(r)) >= SLIVER_PT2 * SC * SC);
      const outers = rings.filter(r => area0(r) > 0).length;
      return { ov, pl, rings, outers, connected: outers === 1 };
    }
    const attempt = ov => attemptPl(place(ov), ov);
    function guards(att) {
      const [a, b, c] = att.pl;
      const reg = x => x.rg, pct = (p, q) => { const i = E.netArea(E.meet(reg(p), reg(q))), s = Math.min(p.p.area * p.s * p.s, q.p.area * q.s * q.s); return { i, pct: s > 0 ? Math.max(0, i / s * 100) : 0 }; };
      const lm = pct(a, b), mr = pct(b, c);
      // counters: the inner openings of the three letters, placed. Only a counter that reaches another letter's box can have been covered.
      const boxes = att.pl.map(x => bboxOf(x.rg)), all = [], near = [];
      att.pl.forEach((x, i) => { for (const r of x.rg) if (area0(r) <= -COUNTER_MIN_PT2 * SC * SC) {
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

    /* the join of one side letter P (placed with its box at x, y beside the middle) and the middle: how much they overlap, how wide the overlap is (the widest disc inside it, up to capMm), how much of their
       overlap is of the smaller letter, and how much of the two letters' inner openings the other one leaves open. null when they do not overlap at all. */
    function pairQ(P, x, y, capMm) {
      if (x >= PM.w || x + P.w <= 0 || y >= PM.h || y + P.h <= 0) return null;
      const rg = shift(P.region, x, y), inter = E.meet(rg, PM.region), i = E.netArea(inter);
      if (!(i > 0)) return null;
      const pct = Math.max(0, i / Math.min(P.ar, PM.ar) * 100);
      const widest = paths => { let w = 0; for (let r = 0.05; r * 2 <= capMm + 1e-9; r += 0.05) { if (E.netArea(E.erode(paths, r / MM)) > 0) w = Math.round(r * 2 * 100) / 100; else break; } return w; };
      let w = widest(inter), loose = 0;
      if (P.bodies.length > 1) {   // a letter of several separate parts: each must be held by the middle letter, and the join is as wide as its weakest part's
        w = Infinity;
        for (const b of P.bodies) { const ib = E.meet(shift([b], x, y), PM.region); if (E.netArea(ib) > 0) w = Math.min(w, widest(ib)); else loose++; }
        if (loose || w === Infinity) w = 0;
      }
      // inner openings: a counter reaching the other letter's box, taken by the other letter's body
      const inBox = (b, q) => b[0] < q[2] && b[2] > q[0] && b[1] < q[3] && b[3] > q[1];
      const pBox = [x, y, x + P.w, y + P.h], mBox = [0, 0, PM.w, PM.h];
      const pc = [], mc = [];
      P.counters.forEach((c, k) => { const b = P.cbox[k]; if (inBox([b[0] + x, b[1] + y, b[2] + x, b[3] + y], mBox)) pc.push(c.map(q => ({ X: q.X + x, Y: q.Y + y }))); });
      PM.counters.forEach((c, k) => { if (inBox(PM.cbox[k], pBox)) mc.push(c); });
      const tot = P.cAr + PM.cAr; let cov = 0;
      if (tot > 0) { if (pc.length) cov += E.netArea(E.meet(pc, PM.region)); if (mc.length) cov += E.netArea(E.meet(mc, rg)); }
      const kept = tot > 0 ? Math.min(1, Math.max(0, 1 - cov / tot)) : 1;
      const valid = !loose && pct <= OVERLAP_MAX_PCT && kept >= HOLES_MIN;
      return { i, pct, neck: w, kept, loose, valid, good: valid && w >= NECK_OK_MM - 1e-9 };
    }
    // the widths of the two joins of a finished placement (a side that does not reach the middle is 0)
    const joinsOf = pl => [0, 2].map(k => { const q = pairQ(pl[k], pl[k].dx, pl[k].dy, 1.0); return q ? q.neck : 0; });

    // reach: the placement of one side nearest the asked overlap whose join holds (see NECK_OK_MM); else the best that stays valid; else the least bad that touches
    let searched = 0;
    function reachSide(P, side) {
      const dyStep = Math.max(1, Math.round(REACH_DY * PM.h)), cands = [];
      for (let k = 0; REACH_LO + k * STEP <= REACH_HI + 1e-9; k++) {
        const ov = Math.round((REACH_LO + k * STEP) * 1e6) / 1e6;
        for (let j = -REACH_DYN; j <= REACH_DYN; j++) cands.push({ ov, j, cost: (ov >= o.overlap ? ov - o.overlap : 1.5 * (o.overlap - ov)) / STEP + 2.5 * Math.abs(j) });
      }
      cands.sort((a, b) => a.cost - b.cost || b.ov - a.ov || a.j - b.j);
      let valid = null, any = null;
      for (const c of cands) {
        const x = xOf(P, side, c.ov), y = dyOf(P) + c.j * dyStep, q = pairQ(P, x, y, NECK_OK_MM); searched++;
        if (!q) continue;
        const hit = { x, y, ov: c.ov, j: c.j, q };
        if (q.good) return hit;
        if (q.valid && (!valid || q.neck > valid.q.neck + 1e-9 || (q.neck > valid.q.neck - 1e-9 && q.i > valid.q.i))) valid = hit;
        const pen = q.loose * 10 + Math.max(0, HOLES_MIN - q.kept) + Math.max(0, q.pct - OVERLAP_MAX_PCT) / 100;
        if (!any || pen < any.pen - 1e-12 || (pen < any.pen + 1e-12 && q.neck > any.q.neck)) any = Object.assign({ pen }, hit);
      }
      return valid || any;
    }

    // connect: from the start overlap upward until the three letters are one piece
    let tries = 0, first = null, lastTried = o.overlap;
    const at = k => Math.round((o.overlap + k * STEP) * 1e6) / 1e6;
    for (let k = 0; at(k) <= o.maxOverlap + 1e-9; k++) { const att = attempt(at(k)); tries++; lastTried = at(k); if (att.connected) { first = att; break; } if (k > 40) break; }
    let best = null;
    if (first) {
      best = { att: first, g: guards(first) };
      if (!best.g.ok) {
        for (let k = Math.round((first.ov - o.overlap) / STEP) - 1; at(k) >= MIN_OVERLAP - 1e-9; k--) {
          const att = attempt(at(k)); tries++;
          if (!att.connected) break;
          const g = guards(att);
          if (g.penalty < best.g.penalty - 1e-12) best = { att, g };
          if (g.ok) break;
        }
      }
      best.joins = joinsOf(best.att.pl);
    }
    // a join that only grazes (or no join at all): each side is searched over a wider reach
    let reached = false, reachedSides = false;
    if (o.reach && (!best || Math.min(...best.joins) < NECK_OK_MM - 1e-9)) {
      const keep = best ? best.att.pl : null, ask = (k, P, side) => (keep && joinsOf(keep)[k / 2] >= NECK_OK_MM - 1e-9) ? { x: keep[k].dx, y: keep[k].dy, keep: true } : reachSide(P, side);
      const hl = ask(0, PL, 'left'), hr = ask(2, PR, 'right');
      if (hl && hr) {
        reachedSides = true;
        const pl = placeAt([hl.x, hl.y], [hr.x, hr.y]), att = attemptPl(pl, null); tries++;
        if (att.connected) {
          const g = guards(att), joins = joinsOf(pl), cand = { att, g, joins };
          const mj = b => Math.min(Math.min(...b.joins), NECK_OK_MM);
          if (!best || (g.ok && !best.g.ok) || (g.ok === best.g.ok && (mj(cand) > mj(best) + 1e-9 || (!g.ok && g.penalty < best.g.penalty - 1e-12 && mj(cand) >= mj(best) - 1e-9)))) { best = cand; reached = true; }
        }
      }
    }
    if (!best) {
      // the letters do not touch, or a letter is made of separate parts that stay loose even though the main bodies join?
      const bodies = place(lastTried).map(P => { let top = null, ta = 0; for (const r of P.rg) { const a = area0(r); if (a > ta) { ta = a; top = r; } } return top; }).filter(Boolean);
      const joined = E.union(bodies).filter(r => area0(r) > 0).length === 1 || (reachedSides && [PL, PM, PR].some(P => P.bodies.length > 1));
      return failure(joined ? 'a letter is made of separate parts that stay loose, it cannot be one charm' : 'the letters do not touch', { tries, searched });
    }
    const { att, g, joins } = best, warnings = [];
    const moved = att.pl.map((P, i) => i === 1 ? 0 : P.dy - dyOf(P));
    const ovs = [0, 2].map(k => { const P = att.pl[k]; return Math.round((k === 0 ? (P.dx + P.w) : (PM.w - P.dx)) / P.w * 100) / 100; });
    if (reached) {
      if (Math.abs(ovs[0] - ovs[1]) < 0.005 && !moved[0] && !moved[2]) notes.push(`overlap ${ovs[0] > o.overlap ? 'raised' : 'lowered'} from ${Math.round(o.overlap * 100)} % to ${Math.round(ovs[0] * 100)} % so the letters join firmly`);
      else notes.push(`the side letters were set ${Math.round(ovs[0] * 100)} % (left) and ${Math.round(ovs[1] * 100)} % (right) of their width into the middle one` + (moved[0] || moved[2] ? ' and moved up or down a little' : '') + ' so their strokes cross firmly');
    } else {
      if (att.ov > o.overlap + 1e-9) notes.push(`overlap raised from ${Math.round(o.overlap * 100)} % to ${Math.round(att.ov * 100)} % so the letters join`);
      else if (att.ov < o.overlap - 1e-9) notes.push(`overlap lowered from ${Math.round(o.overlap * 100)} % to ${Math.round(att.ov * 100)} % to keep the inner openings`);
    }
    if (g.holesKept < HOLES_MIN) { notes.push(`only ${Math.round(g.holesKept * 100)} % of the inner openings stay open`); warnings.push(`inner openings: only ${Math.round(g.holesKept * 100)} % stay open, look at the picture`); }
    if (g.lm.pct > OVERLAP_MAX_PCT) { notes.push(`the left and middle letters overlap by ${Math.round(g.lm.pct)} % of the smaller letter`); warnings.push('the Left and Middle letters overlap a lot, look at the picture'); }
    if (g.mr.pct > OVERLAP_MAX_PCT) { notes.push(`the middle and right letters overlap by ${Math.round(g.mr.pct)} % of the smaller letter`); warnings.push('the Middle and Right letters overlap a lot, look at the picture'); }
    [['left and middle', 0], ['middle and right', 1]].forEach(([nm, k]) => {
      const w = joins[k];
      if (w < NECK_OK_MM - 1e-9) { notes.push(`the join between the ${nm} letters is thin (${w.toFixed(2)} mm)`); warnings.push(`thin join between the ${nm.replace('left', 'Left').replace('middle', 'Middle').replace('right', 'Right')} letters (${w.toFixed(2)} mm), check before cutting`); }
    });

    // the finished piece: outer ring CCW first, then the holes CW, largest first; everything shifted so the piece starts at 0,0
    const b = bboxOf(att.rings), W = b[2] - b[0], H = b[3] - b[1];
    const toPt = r => { const pts = r.map(q => [(q.X - b[0]) / SC, (q.Y - b[1]) / SC]); return pts; };
    const outer = att.rings.find(r => area0(r) > 0);
    const holes = att.rings.filter(r => area0(r) < 0).sort((p, q) => area0(p) - area0(q) || bboxOf([p])[1] - bboxOf([q])[1] || bboxOf([p])[0] - bboxOf([q])[0]);
    const rings = [{ pts: toPt(outer), hole: false }].concat(holes.map(h => ({ pts: toPt(h), hole: true })));
    const placed = att.pl.map(P => ({ letter: P.p.letter, slot: P.p.slot, scale: round3(P.s), bboxPt: [P.dx, P.dy, P.dx + P.w, P.dy + P.h].map((v, i) => round3((v - b[i % 2]) / SC)) }));
    return {
      ok: true, why: '', rings, bboxPt: [0, 0, round3(W / SC), round3(H / SC)], wMm: Math.round(W / SC * MM * 1e4) / 1e4, hMm: Math.round(H / SC * MM * 1e4) / 1e4, placed,
      report: { connected: true, overlapPct: { leftMiddle: Math.round(g.lm.pct * 10) / 10, middleRight: Math.round(g.mr.pct * 10) / 10 }, holesKept: round3(g.holesKept), centerScale: round3(cs), overlap: att.ov == null ? round3((ovs[0] + ovs[1]) / 2) : att.ov, reach: reached, searched, warnings, joinMm: { leftMiddle: joins[0], middleRight: joins[1] }, moveMm: { left: Math.round(moved[0] / SC * MM * 100) / 100, right: Math.round(moved[2] / SC * MM * 100) / 100 }, joinMm2: { leftMiddle: Math.round(g.lm.i / SC / SC * MM * MM * 100) / 100, middleRight: Math.round(g.mr.i / SC / SC * MM * MM * 100) / 100 }, tries, notes }
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

  const api = { lettersOf, familiesOf, slotsOf, build, toPdf, svgPath };
  root.CharmNestMonogram = api;
  if (typeof module === 'object' && module.exports) module.exports = api;
})(typeof window !== 'undefined' ? window : globalThis);
