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
     When the first overlap that connects gives a weaker join than NECK_OK_MM, each side is searched again over a wider overlap (up to REACH_HI = 80 % of the side letter's width; opts.reach: a number sets it) and a small move up or down (REACH_DY of the
     middle's height per step, REACH_DYN steps each way), nearest to the asked overlap first, for the first placement whose join holds and whose letters keep their shape (35 % overlap, 90 % of the inner openings). */
  const NECK_OK_MM = 0.3, NECK_FLOOR_MM = 0.15, REACH_LO = 0.06, REACH_HI = 0.8, REACH_DY = 0.05, REACH_DYN = 3, REACH_STEP = 0.02;
  /* A part of a letter that no overlap can hold (a Q's tail, a flourish drawn apart from its letter) is tied on rather than left loose or dropped: a bar BRIDGE_W_MM wide across the shortest gap to the rest of the piece, when
     that gap is no longer than BRIDGE_MAX_MM; a longer one is no letter's own part and the monogram is refused, plainly. The notes and the warnings say so. */
  const BRIDGE_MAX_MM = 3, BRIDGE_W_MM = 0.4;
  /* Two solid contacts per pair (MONOGRAMUI, Paul, 15 Oct 2026). Natural first (the reach search, with each side's contacts counted), then a letter's own hairline that reaches over its neighbour is shortened (opts.allowTrim,
     at most TRIM_MAX_PCT of the letter's width and TRIM_INK_PCT of its ink, never a counter, never a new loose part, in steps of TRIM_STEP_MM, the smallest that gives two contacts), then a tie bar of opts.barMm across the nearest gap. */
  const TRIM_MAX_PCT = 30, TRIM_INK_PCT = 10, TRIM_STEP_MM = 0.5, TRIM_TIP_MM = 0.3;
  /* Size and layout norms (MONOGRAMUI, Paul, 15 Oct 2026: "some side letters are drastically smaller than the centre letter ... the system must examine every letter being added and enforce a set ratio of size and width";
     "squished together it just looks like a bunch of gibberish"). Every number is a default of the options of build() (opts.norms overrides one by name) and is exported as CharmNestMonogram.NORMS.
     A letter is measured by its BODY: the central 90 % of its ink area along each axis (BODY_TRIM cuts 5 % off each end: a flourish, a hairline, a Q's tail does not count). Data of Paul's alphabets (78 letters, the
     body at the file's font size, in pt): LEFT height 25.8/28.6/29.8 width 7.5/9.8/13.4, MID height 29.4/33.3/34.1 width 9.3/12.9/14.9, RIGHT height 23.8/27.3/30.0 width 6.9/9.1/12.4 (min/median/max): the heights are
     nearly constant (one font size), the widths run 1 to 2. So: the sides' body height is SIDE_HEIGHT_RATIO of the centre's, the sides' body width is held to SIDE_WIDTH_MIN..MAX of the centre's body width and the
     centre's to MID_WIDTH_MIN..MAX of the median centre (MID_BODY_W_OVER_EM_MEDIAN of the font size: 12.9 pt of 53), by a horizontal-only stretch of at most STRETCH_CAP (a letter that needs more is clamped and said). The finished
     piece is laid out for ASPECT_TARGET (width / height, a round piece), never outside ASPECT_MIN..MAX where the letters can stretch to it. The legibility limits are measured on every word and reported: OVERLAP_SIDE_MAX / OVERLAP_MID_MAX
     (the part of a letter's ink that lies inside the other letters' ink), SIDE_VISIBLE_MIN_MM (the part of a side letter that stands out beside the centre letter), SIDE_HEIGHT_MIN (its ink height against the centre's), GAP_MIN_MM
     (strokes of different letters that do not touch keep this much apart: the sliver gaps that read as gibberish) and COUNTERS_MIN (the part of the inner openings that stays open). */
  const NORMS = {
    SIDE_HEIGHT_RATIO: 0.80, SIDE_HEIGHT_TOL: 0.04, SIDE_WIDTH_MIN: 0.60, SIDE_WIDTH_MAX: 0.95, MID_WIDTH_MIN: 0.84, MID_WIDTH_MAX: 1.11, MID_BODY_W_OVER_EM_MEDIAN: 0.2434, MID_BODY_W_OVER_INKH_MEDIAN: 0.348, STRETCH_CAP: 0.25, BODY_TRIM: 0.05,
    ASPECT_TARGET: 0.92, ASPECT_MIN: 0.82, ASPECT_MAX: 1.00, OVERLAP_SIDE_MAX: 0.25, OVERLAP_MID_MAX: 0.35, SIDE_VISIBLE_MIN_MM: 3.5, SIDE_HEIGHT_MIN: 0.72, GAP_MIN_MM: 0.35, COUNTERS_MIN: 0.9, BOX_OVERLAP_MAX: 0.7, BOX_OVERLAP_MIN: 0.06
  };
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
  const scaled = (paths, sx, sy) => { sy = sy == null ? sx : sy; return sx === 1 && sy === 1 ? paths : paths.map(p => p.map(q => ({ X: Math.round(q.X * sx), Y: Math.round(q.Y * sy) }))); };
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
      diff: (a, b) => run(C.ClipType.ctDifference, a, b, C.PolyFillType.pftNonZero, C.PolyFillType.pftNonZero, false),
      meetEO: (a, b) => run(C.ClipType.ctIntersection, a, b, C.PolyFillType.pftNonZero, C.PolyFillType.pftEvenOdd, false),
      merge: counters => run(C.ClipType.ctUnion, counters, null, C.PolyFillType.pftNonZero, C.PolyFillType.pftNonZero, false),
      // the paths pulled inward by rPt (what is left when every edge moves in): empty when no disc of that radius fits
      grow: (paths, rPt) => { const co = new C.ClipperOffset(2, 0.02 * SC), out = new C.Paths(); co.AddPaths(paths, C.JoinType.jtRound, C.EndType.etClosedPolygon); co.Execute(out, rPt * SC); return out; },
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
  /* The BODY of a letter: the span, along one axis, between the two places that cut BODY_TRIM (5 %) of the ink area off each end (so a flourish, a tail or a hairline does not count). 0.001 pt in, 0.001 pt out; scanlines over the outlines. */
  function centralSpan(region, axis, trim) {
    const b = bboxOf(region), lo = axis ? b[1] : b[0], hi = axis ? b[3] : b[2], n = 480, st = (hi - lo) / n; if (!(st > 0)) return 0;
    const len = new Array(n).fill(0);
    for (let i = 0; i < n; i++) {
      const c = lo + (i + 0.5) * st, hits = [];
      for (const r of region) for (let k = 0, m = r.length; k < m; k++) {
        const p = r[k], q = r[(k + 1) % m], a = axis ? p.Y : p.X, bq = axis ? q.Y : q.X;
        if ((a <= c) !== (bq <= c)) { const t = (c - a) / (bq - a); hits.push(axis ? p.X + t * (q.X - p.X) : p.Y + t * (q.Y - p.Y)); }
      }
      hits.sort((x, y) => x - y); let l = 0; for (let k = 0; k + 1 < hits.length; k += 2) l += hits[k + 1] - hits[k]; len[i] = l;
    }
    const tot = len.reduce((x, y) => x + y, 0); if (!(tot > 0)) return 0;
    const at = target => { let acc = 0; for (let i = 0; i < n; i++) { if (acc + len[i] >= target) return lo + (i + (len[i] > 0 ? (target - acc) / len[i] : 0)) * st; acc += len[i]; } return hi; };
    return at(tot * (1 - trim)) - at(tot * trim);
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
    // the typographic metrics of a glyph from the master file (MONOGRAMFILE: baselinePt = the baseline's height above the bottom of the outline, may be negative; emPt = the font size): where its baseline is in its own box
    const mt = spec.metrics && typeof spec.metrics === 'object' ? spec.metrics : null, bl = mt && finite(mt.baselinePt) && Math.abs(mt.baselinePt) < 1e4 ? mt.baselinePt : null;
    return { region, w, h, area: a, name: name(), base: bl == null ? null : Math.round(bl * SC), em: mt && finite(mt.emPt) && mt.emPt > 0 ? mt.emPt : null };
  }

  function normOpts(o) {
    o = o && typeof o === 'object' ? o : {};
    const num = (v, d, lo, hi) => (finite(v) ? Math.min(hi, Math.max(lo, v)) : d);
    const overlap = num(o.overlap, 0.18, 0, 0.9);
    return {
      overlap, maxOverlap: Math.max(overlap, num(o.maxOverlap, 0.38, 0, 0.95)),
      centerRatio: Math.max(1.2, num(o.centerRatio, 1.25, 1.2, 5)), sideWiden: num(o.sideWiden, 1.15, 1, 1.5), contacts: Math.round(num(o.contacts, 2, 1, 3)), contactMm: num(o.contactMm, 0.3, 0.1, 2), contactGapMm: num(o.contactGapMm, 1.5, 0.3, 6), barMm: num(o.barMm, 0.6, Math.max(0.2, num(o.contactMm, 0.3, 0.1, 2)), 2), allowTrim: o.allowTrim !== false, normalise: o.normalise !== false, norms: Object.assign({}, NORMS, o.norms && typeof o.norms === 'object' ? o.norms : {}),
      valign: o.valign === 'baseline' ? 'baseline' : 'center', reach: o.reach !== false, reachMax: typeof o.reach === 'number' && Number.isFinite(o.reach) ? Math.min(0.95, Math.max(0.1, o.reach)) : REACH_HI
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
    // the searches (overlap, contacts) work on a coarser copy of every letter (vertices within 0.02 mm of their neighbours' line dropped: a tenth of the points, ten times quicker); the piece itself is cut from the full outlines
    const SIMP = 0.02 / MM * SC, simp = paths => paths.map(q => C.Clipper.CleanPolygon(q.map(v => ({ X: v.X, Y: v.Y })), SIMP)).filter(q => q && q.length >= 3);
    if (!letters || typeof letters !== 'object') return failure('the three letters are missing');
    const L = [];
    for (const slot of SLOTS) { const p = prepLetter(E, letters[slot], slot); if (p.why) return failure(p.why); p.slot = slot; p.letter = letters[slot].letter == null ? '' : String(letters[slot].letter); L.push(p); }
    const [left, mid, right] = L;
    const notes = [];

    /* size. With the glyph metrics of all three letters (Paul's alphabet file): the sides keep the size they are drawn at (one font size: one scale for both, never each by its own box), the middle is scaled so that its height above the
       baseline is centerRatio (1.25: the sides are 80 % of it, Paul's choice) times the sides'. Without: the sides keep their relative size unless they differ by more than 10 % (then the smaller one is raised to the larger), the middle is centerRatio times the taller side.
       The sides are widened sideWiden (1.15) times across, never taller: a big middle letter wants wider neighbours. */
    const metricsOn = left.base != null && mid.base != null && right.base != null;
    let sl = 1, sr = 1, sideH, midNom = mid.h, cs;
    if (metricsOn) {
      if (left.em && right.em && Math.abs(left.em - right.em) > 1e-6) { if (left.em > right.em) sr = left.em / right.em; else sl = right.em / left.em; }
      sideH = ((left.h - left.base) * sl + (right.h - right.base) * sr) / 2; midNom = mid.h - mid.base;
    } else {
      if (left.h > right.h * 1.1) sr = left.h / right.h; else if (right.h > left.h * 1.1) sl = right.h / left.h;
      sideH = Math.max(left.h * sl, right.h * sr);
    }
    cs = Math.max(1, o.centerRatio * sideH / midNom);
    const wide = o.sideWiden, N = o.normalise ? o.norms : null;
    // the body of every letter (see NORMS): the central 90 % of its ink along each axis
    for (const p of L) { p.bw = Math.max(0.2 * p.w, centralSpan(p.region, 0, NORMS.BODY_TRIM)); p.bh = Math.max(0.2 * p.h, centralSpan(p.region, 1, NORMS.BODY_TRIM)); }
    // the plan: each letter's uniform scale (s), horizontal factor (f) and what was asked of it (to say it)
    const plan = [[left, sl, wide], [mid, cs, 1], [right, sr, wide]].map(([p, s, f]) => ({ p, s, f, need: f, capped: false }));
    let layout = null;
    if (N) {
      const [pl, pm, pr] = plan, cap = N.STRETCH_CAP, clampF = (v) => Math.min(1 + cap, Math.max(1 - cap, v));
      // heights: the sides' BODY is SIDE_HEIGHT_RATIO of the centre's (1 / centerRatio), the ink of a side never taller than 95 % of the centre's, never shrunk more than 15 % from its drawn size
      const midBH = mid.bh * cs, ratio = 1 / o.centerRatio;
      for (const q of [pl, pr]) { let v = ratio * midBH / q.p.bh; v = Math.min(v, 0.95 * mid.h * cs / q.p.h); q.s = Math.min(1.25, Math.max(0.85 * q.s, v)); }
      // the centre's width: into MID_WIDTH_MIN..MAX of the median centre
      const med = (mid.em ? N.MID_BODY_W_OVER_EM_MEDIAN * mid.em * SC : N.MID_BODY_W_OVER_INKH_MEDIAN * mid.h) * cs, mBW = mid.bw * cs;
      pm.need = Math.min(Math.max(mBW, N.MID_WIDTH_MIN * med), N.MID_WIDTH_MAX * med) / mBW; pm.f = clampF(pm.need);
      // the sides' width: into SIDE_WIDTH_MIN..MAX of the centre's body width (widened `wide` when that is inside)
      const mBW2 = mBW * pm.f;
      for (const q of [pl, pr]) { const bw = q.p.bw * q.s; q.need = Math.min(Math.max(wide, N.SIDE_WIDTH_MIN * mBW2 / bw, (N.SIDE_VISIBLE_MIN_MM + 0.4) / MM * SC / bw), N.SIDE_WIDTH_MAX * mBW2 / bw); q.f = clampF(q.need); if (q.need > q.f + 0.005) { const boost = Math.min(q.need / q.f, (ratio + N.SIDE_HEIGHT_TOL) / ratio, 1.05); q.s *= boost; q.f = clampF(q.need / boost) ; } }
      // the whole: laid out for ASPECT_TARGET; when even the largest overlap leaves it outside the band the three are stretched together (within the caps)
      const Hp = mid.h * cs, wOf = q => q.p.w * q.s * q.f, ovOf = () => 1 - (N.ASPECT_TARGET * Hp - wOf(pm)) / (wOf(pl) + wOf(pr));
      let ov = ovOf(), fa = 1;
      const widthAt = o2 => wOf(pm) + (1 - o2) * (wOf(pl) + wOf(pr));
      if (ov > N.BOX_OVERLAP_MAX) { const want = Math.min(N.ASPECT_MAX * Hp, Math.max(N.ASPECT_MIN * Hp, widthAt(N.BOX_OVERLAP_MAX))), lo = Math.max(...plan.map(q => (1 - cap) / q.f)); fa = Math.max(lo, want / widthAt(N.BOX_OVERLAP_MAX)); }
      else if (ov < N.BOX_OVERLAP_MIN) { const want = Math.min(N.ASPECT_MAX * Hp, widthAt(N.BOX_OVERLAP_MIN)), hi = Math.min(...plan.map(q => (1 + cap) / q.f)); fa = Math.min(hi, want / widthAt(N.BOX_OVERLAP_MIN)); }
      if (fa !== 1) { for (const q of plan) q.f *= fa; ov = ovOf(); }
      layout = { overlap: Math.min(N.BOX_OVERLAP_MAX, Math.max(N.BOX_OVERLAP_MIN, ov)), aspectAsked: N.ASPECT_TARGET, commonStretch: fa };
      sl = pl.s; sr = pr.s;
      if (!finite(options && options.overlap)) { o.overlap = layout.overlap; o.maxOverlap = Math.max(o.maxOverlap, layout.overlap + 0.2); }
      if (!(options && typeof options.reach === 'number')) o.reachMax = Math.min(o.reachMax, N.BOX_OVERLAP_MAX);
    }
    const placed0 = plan.map(({ p, s, f }) => {
      const sx = s * f, region = scaled(p.region, sx, s), counters = region.filter(r => E.area(r) <= -COUNTER_MIN_PT2 * SC * SC).map(r => r.slice().reverse());
      const bodies = region.filter(r => E.area(r) >= SLIVER_PT2 * SC * SC);   // (the separate parts a letter is made of: a stroke apart from the rest, a dot; a speck is not one)
      return { p, s, sx, region, sreg: simp(region), sbodies: simp(bodies), w: Math.round(p.w * sx), h: Math.round(p.h * s), base: p.base == null ? null : Math.round(p.base * s), ar: E.netArea(region), counters, cbox: counters.map(c => bboxOf([c])), cAr: E.netArea(counters), bodies };
    });
    let [PL, PM, PR] = placed0;   // (a side letter that is trimmed is swapped for its trimmed self in placed0 and here)
    if (PM.h < 1.2 * Math.max(PL.h, PR.h) - 2 && !metricsOn) notes.push('the middle letter is less than 1.2 times the side letters');
    if (cs !== 1) notes.push(`middle letter scaled ${round3(cs)} times to stand out`);
    if (!N && wide !== 1) notes.push(`the side letters were widened ${round3(wide)} times across (not taller) to fit the bigger middle letter`);
    if (N) for (const q of plan) { const nm = q === plan[1] ? 'centre' : q === plan[0] ? 'left' : 'right'; if (Math.abs(q.f - 1) > 0.005 && (q === plan[1] || Math.abs(q.f - wide) > 0.005)) notes.push(`the ${nm} letter ${q.p.letter || ''} was stretched ${q.f > 1 ? '' : 'in '}${Math.round(Math.abs(q.f - 1) * 100)} % across`) }
    if (!N && (sl !== 1 || sr !== 1)) notes.push(`the ${sl !== 1 ? 'left' : 'right'} letter was raised ${round3(Math.max(sl, sr))} times to match the other side`);
    const area0 = p => E.area(p);
    /* where a side letter stands vertically beside the middle one (at 0). With metrics: both on ONE baseline, raised so that the sides' height above it is centred on the middle letter's (the middle stands beyond both,
       equally above and below, as in a circle monogram); valign 'baseline': on the middle letter's own baseline. Without metrics: each side's box centred on the middle's box (the same centre for both). */
    const sideNom = metricsOn ? ((PL.h - PL.base) + (PR.h - PR.base)) / 2 : 0, midBase = metricsOn ? PM.base : 0, midTop = metricsOn ? PM.h - PM.base : 0;
    let baselineY = metricsOn ? (o.valign === 'baseline' ? midBase : midBase + (midTop - sideNom) / 2) : 0;
    if (metricsOn && N && o.valign !== 'baseline') {      // (the sides never stand out beyond the centre letter's ink above or below: the baseline gives way, both sides together, so they stay level)
      const lo = Math.max(PL.base, PR.base), hi = PM.h - Math.max(PL.h - PL.base, PR.h - PR.base);
      if (lo <= hi) baselineY = Math.min(hi, Math.max(lo, baselineY));
    }
    const dyOf = P => metricsOn ? Math.round(baselineY - P.base) : (o.valign === 'baseline' ? 0 : Math.round((PM.h - P.h) / 2));
    const xOf = (P, side, ov) => side === 'left' ? Math.round(ov * P.w) - P.w : PM.w - Math.round(ov * P.w);

    const setSide = (k, V) => { placed0[k] = V; if (k === 0) PL = V; else PR = V; };
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
    /* crowding: the length (mm) of one letter's outline that lies within GAP_MIN_MM of another letter's ink without touching it (away from where they cross): strokes that run side by side a hair apart are what reads as gibberish */
    const GAPSC = o.norms.GAP_MIN_MM / MM, CROWD_TOTAL_MM = 3, CROWD_PAIR_MM = 1.5;
    const outlineLen = (rings, zone) => {
      if (!zone.length || !rings.length) return 0;
      const c = new C.Clipper(0), pt = new C.PolyTree(); c.AddPaths(rings.map(r => r.concat([r[0]])), C.PolyType.ptSubject, false); c.AddPaths(zone, C.PolyType.ptClip, true);
      if (!c.Execute(C.ClipType.ctIntersection, pt, C.PolyFillType.pftNonZero, C.PolyFillType.pftNonZero)) return 0;
      let l = 0; for (const q of C.Clipper.OpenPathsFromPolyTree(pt)) for (let i = 1; i < q.length; i++) l += Math.hypot(q[i].X - q[i - 1].X, q[i].Y - q[i - 1].Y); return l / SC * MM;
    };
    const crowdOf = (A, B, inter) => { const touch = E.grow(inter, GAPSC), near = (X, Y) => E.diff(E.diff(E.grow(Y, GAPSC), Y), touch); return Math.max(outlineLen(A, near(A, B)), outlineLen(B, near(B, A))); };
    function guards(att) {
      const [a, b, c] = att.pl;
      const reg = x => x.rg, pct = (p, q, own) => { const i = E.netArea(E.meet(reg(p), reg(q))), s = own ? own.ar : Math.min(p.ar, q.ar); return { i, pct: s > 0 ? Math.max(0, i / s * 100) : 0 }; };
      const lm = pct(a, b, o.normalise ? a : null), mr = pct(b, c, o.normalise ? c : null);
      // counters: the inner openings of the three letters, placed. Only a counter that reaches another letter's box can have been covered.
      const boxes = att.pl.map(x => bboxOf(x.rg)), all = [], near = [], per = [];
      att.pl.forEach((x, i) => { const ai = [], ni = []; for (const r of x.rg) if (area0(r) <= -COUNTER_MIN_PT2 * SC * SC) {
        const cr = r.slice().reverse(), bb = bboxOf([r]); all.push(cr); ai.push(cr);
        if (boxes.some((q, j) => j !== i && bb[0] < q[2] && bb[2] > q[0] && bb[1] < q[3] && bb[3] > q[1])) { near.push(cr); ni.push(cr); } }
        if (ai.length && ni.length) { const tt = E.netArea(E.merge(ai)); if (tt > 0) per.push(Math.min(1, Math.max(0, 1 - E.netArea(E.meetEO(E.merge(ni), att.rings)) / tt))); } });
      let kept = 1;
      if (o.normalise) kept = per.length ? Math.min(...per) : 1;
      else if (all.length) {
        const tot = E.netArea(E.merge(all));
        if (tot > 0 && near.length) kept = Math.min(1, Math.max(0, 1 - E.netArea(E.meetEO(E.merge(near), att.rings)) / tot));
      }
      let over = Math.max(0, Math.max(lm.pct, mr.pct) - OVERLAP_MAX_PCT), midPct = 0, crowd = { lm: 0, mr: 0 };
      if (o.normalise) {
        over = Math.max(0, Math.max(lm.pct, mr.pct) - o.norms.OVERLAP_SIDE_MAX * 100);
        midPct = (lm.i + mr.i) / Math.max(1, b.ar) * 100; over = Math.max(over, midPct - o.norms.OVERLAP_MID_MAX * 100);
        const il = E.meet(a.rg, b.rg), ir = E.meet(c.rg, b.rg); crowd = { lm: crowdOf(a.rg, b.rg, il), mr: crowdOf(c.rg, b.rg, ir), lr: crowdOf(a.rg, c.rg, E.meet(a.rg, c.rg)) };
      }
      const crowded = crowd.lm + crowd.mr + (crowd.lr || 0) > CROWD_TOTAL_MM;
      return { holesKept: kept, lm, mr, midPct, crowd, ok: kept >= HOLES_MIN && over === 0 && !crowded, penalty: Math.max(0, HOLES_MIN - kept) + over / 100 + (crowded ? (crowd.lm + crowd.mr + (crowd.lr || 0)) / CROWD_TOTAL_MM * 0.05 : 0) };
    }

    /* the join of one side letter P (placed with its box at x, y beside the middle) and the middle: how much they overlap, how wide the overlap is (the widest disc inside it, up to capMm), how much of their
       overlap is of the smaller letter, and how much of the two letters' inner openings the other one leaves open. null when they do not overlap at all. */
    /* Separate solid contacts of two letters (MONOGRAMUI, Paul, 15 Oct 2026: "at least two points of contact between each pair of letters"): the overlap of the two letters' ink; a contact is a part of it where a disc of
       opts.contactMm (0.3 mm) fits (a hairline crossing is not solid); two solid parts nearer than opts.contactGapMm (1.5 mm) are ONE contact (a long crossing is one contact, not two). */
    const need = o.contacts, LINK_MM = 0.2;
    const coreR = () => Math.max(0.02, o.contactMm / 2 - 0.01) / MM;
    function clustersOf(inter, coarse) {      // (coarse: the search's simplified outlines, a little more generous: a candidate that passes is checked again on the full outlines)
      if (!(E.netArea(inter) > 0)) return [];
      const cores = E.erode(inter, Math.max(0.01 / MM, coreR() - (coarse ? 0.02 / MM : 0))).filter(r => area0(r) > 0);   // the solid parts: a disc of contactMm fits
      if (!cores.length) return [];
      const links = E.erode(inter, LINK_MM / 2 / MM).filter(r => area0(r) > 0);      // parts joined by a link this wide (a long crossing, a thin neck) are ONE contact
      const grown = E.grow(links.length ? links : cores, o.contactGapMm / 2 / MM).filter(r => area0(r) > 0);
      return grown.map(g => ({ ring: g, cores: cores.filter(c => C.Clipper.PointInPolygon(c[0], g) !== 0) })).filter(c => c.cores.length);
    }
    const contactsAt = (pl, k) => clustersOf(E.meet(pl[k].rg, pl[1].rg));
    const ncOf = pl => need > 1 ? [contactsAt(pl, 0).length, contactsAt(pl, 2).length] : [1, 1];
    const widestOf = (paths, capMm) => { let w = 0; for (let r = 0.05; r * 2 <= capMm + 1e-9; r += 0.05) { if (E.netArea(E.erode(paths, r / MM)) > 0) w = Math.round(r * 2 * 100) / 100; else break; } return w; };
    function pairQ(P, x, y, capMm, tied, wantNc, full) {      // (full: on the full outlines, not the coarse copies the search runs on)
      if (x >= PM.w || x + P.w <= 0 || y >= PM.h || y + P.h <= 0) return null;
      const Preg = full ? P.region : P.sreg, Mreg = full ? PM.region : PM.sreg, rg = shift(Preg, x, y), inter = E.meet(rg, Mreg), i = E.netArea(inter);
      if (!(i > 0)) return null;
      const pct = Math.max(0, i / (o.normalise ? P.ar : Math.min(P.ar, PM.ar)) * 100);
      const widest = paths => widestOf(paths, capMm);
      let w = widest(inter), loose = 0;
      if (P.bodies.length > 1) {   // a letter of several separate parts: each must be held by the middle letter, and the join is as wide as its weakest part's
        w = Infinity;
        for (const b of (full ? P.bodies : P.sbodies)) {
          const ib = E.meet(shift([b], x, y), Mreg), wb = E.netArea(ib) > 0 ? widest(ib) : -1;
          if (tied && wb < NECK_FLOOR_MM) continue;                       // (tied: a part tied on with a bar is held by its letter, not by the middle one)
          if (wb < 0) loose++; else w = Math.min(w, wb);
        }
        if (loose || w === Infinity) w = 0;
      }
      // inner openings: a counter reaching the other letter's box, taken by the other letter's body
      const inBox = (b, q) => b[0] < q[2] && b[2] > q[0] && b[1] < q[3] && b[3] > q[1];
      const pBox = [x, y, x + P.w, y + P.h], mBox = [0, 0, PM.w, PM.h];
      const pc = [], mc = [];
      P.counters.forEach((c, k) => { const b = P.cbox[k]; if (inBox([b[0] + x, b[1] + y, b[2] + x, b[3] + y], mBox)) pc.push(c.map(q => ({ X: q.X + x, Y: q.Y + y }))); });
      PM.counters.forEach((c, k) => { if (inBox(PM.cbox[k], pBox)) mc.push(c); });
      let kept = 1;
      if (o.normalise) {      // (every letter keeps COUNTERS_MIN of its own inner openings, not the three together)
        if (P.cAr > 0 && pc.length) kept = Math.min(kept, 1 - E.netArea(E.meet(pc, Mreg)) / P.cAr);
        if (PM.cAr > 0 && mc.length) kept = Math.min(kept, 1 - E.netArea(E.meet(mc, rg)) / PM.cAr);
        kept = Math.min(1, Math.max(0, kept));
      } else {
        const tot = P.cAr + PM.cAr; let cov = 0;
        if (tot > 0) { if (pc.length) cov += E.netArea(E.meet(pc, Mreg)); if (mc.length) cov += E.netArea(E.meet(mc, rg)); }
        kept = tot > 0 ? Math.min(1, Math.max(0, 1 - cov / tot)) : 1;
      }
      const isLeft = x + P.w / 2 < PM.w / 2, visMm = ((isLeft ? -x : x + P.w - PM.w)) / SC * MM, visNeed = Math.min(o.norms.SIDE_VISIBLE_MIN_MM, 0.7 * P.w / SC * MM);
      const valid = !loose && pct <= (o.normalise ? o.norms.OVERLAP_SIDE_MAX * 100 : OVERLAP_MAX_PCT) && kept >= HOLES_MIN;
      const nc = !valid ? 0 : (wantNc && need > 1 ? (w >= Math.min(NECK_OK_MM, o.contactMm) - (full ? 0 : 0.1) - 1e-9 ? clustersOf(inter, !full).length : 0) : need);   // (separate solid contacts; not asked for: counted as enough)
      let good = valid && w >= NECK_OK_MM - (full ? 0 : 0.1) - 1e-9 && nc >= need, crowd = 0;
      if (good && o.normalise) { crowd = crowdOf(rg, Mreg, inter); if (crowd > CROWD_PAIR_MM * (full ? 1 : 1.2)) good = false; }
      const crowdFail = valid && !good && crowd > 0;
      return { i, pct, neck: w, kept, loose, valid, nc, good, crowd, crowdFail };
    }
    // the widths of the two joins of a finished placement (a side that does not reach the middle is 0)
    const joinsOf = (pl, tied) => [0, 2].map(k => { const q = pairQ(pl[k], pl[k].dx, pl[k].dy, 1.0, tied); return q ? q.neck : 0; });

    /* Parts a letter is drawn in that no overlap holds are tied on (see BRIDGE_MAX_MM): a part that stays loose, and a part that only grazes the middle letter. */
    // the shortest gap between two outlines: every vertex of one against every edge of the other, both ways
    const toSeg = (p, a, b) => { const dx = b.X - a.X, dy = b.Y - a.Y, l2 = dx * dx + dy * dy, t = l2 ? Math.max(0, Math.min(1, ((p.X - a.X) * dx + (p.Y - a.Y) * dy) / l2)) : 0, X = a.X + t * dx, Y = a.Y + t * dy; return { d: Math.hypot(p.X - X, p.Y - Y), X, Y }; };
    const near = (a, b) => { let best = null;
      for (const [u, v, flip] of [[a, b, false], [b, a, true]]) { const su = Math.max(1, Math.floor(u.length / 500));
        for (let i = 0; i < u.length; i += su) for (let j = 0; j < v.length; j++) { const h = toSeg(u[i], v[j], v[(j + 1) % v.length]); if (!best || h.d < best.d) best = { d: h.d, p: flip ? { X: h.X, Y: h.Y } : u[i], q: flip ? u[i] : { X: h.X, Y: h.Y } }; } }
      return best; };
    const BAR_W = BRIDGE_W_MM / MM * SC, BAR_MAX = BRIDGE_MAX_MM / MM * SC;
    const barOf = (n, w) => {   // a bar across the gap, reaching half its width into both parts
      w = w || BAR_W;
      const dx = n.q.X - n.p.X, dy = n.q.Y - n.p.Y, len = Math.hypot(dx, dy) || 1, ux = dx / len, uy = dy / len, e = w / 2;
      const ax = n.p.X - ux * e, ay = n.p.Y - uy * e, bx = n.q.X + ux * e, by = n.q.Y + uy * e, nx = -uy * w / 2, ny = ux * w / 2;
      const bar = [{ X: Math.round(ax + nx), Y: Math.round(ay + ny) }, { X: Math.round(bx + nx), Y: Math.round(by + ny) }, { X: Math.round(bx - nx), Y: Math.round(by - ny) }, { X: Math.round(ax - nx), Y: Math.round(ay - ny) }];
      return area0(bar) > 0 ? bar : bar.reverse(); };
    const withBars = (pl, k, bars) => { const pl2 = pl.slice(); pl2[k] = Object.assign({}, pl[k], { rg: pl[k].rg.concat(bars) }); return pl2; };
    // pieces that stay loose: each is tied to the nearest piece of the finished shape; null when one is too far
    function mend(pl, att) {
      const comps = att.rings.filter(r => area0(r) > 0).sort((a, b) => area0(b) - area0(a));
      if (comps.length < 2 || comps.length > 8) return null;
      const bars = [], tied = [comps[0]];
      for (const c of comps.slice(1)) {
        let best = null; for (const t of tied) { const n = near(c, t); if (n && (!best || n.d < best.d)) best = n; }
        if (!best || best.d > BAR_MAX) return null;
        bars.push(barOf(best)); tied.push(c);
      }
      const att2 = attemptPl(withBars(pl, 1, bars), null);
      return att2.connected ? { att: att2, bars: bars.length } : null;
    }
    // parts of a side letter that touch the middle letter only by a graze (narrower than NECK_FLOOR_MM): each is tied to a part of its own letter that is held, when that is near enough; null when there is nothing to tie
    function tieWeak(pl) {
      let out = pl, n = 0;
      for (const k of [0, 2]) {
        const P = pl[k]; if (P.bodies.length < 2) continue;
        const necks = P.bodies.map(b => { const ib = E.meet(shift([b], P.dx, P.dy), PM.region); return E.netArea(ib) > 0 ? widestOf(ib, 1.0) : 0; });
        const strong = P.bodies.filter((_, i) => necks[i] >= NECK_FLOOR_MM - 1e-9);
        if (!strong.length || strong.length === P.bodies.length) continue;
        const bars = [];
        for (let i = 0; i < P.bodies.length; i++) {
          if (necks[i] >= NECK_FLOOR_MM - 1e-9) continue;
          let best = null; for (const t of strong) { const nr = near(shift([P.bodies[i]], P.dx, P.dy)[0], shift([t], P.dx, P.dy)[0]); if (nr && (!best || nr.d < best.d)) best = nr; }
          if (!best || best.d > BAR_MAX) return null;
          bars.push(barOf(best));
        }
        out = withBars(out, k, bars); n += bars.length;
      }
      return n ? { pl: out, bars: n } : null;
    }

    // reach: the placement of one side nearest the asked overlap whose join holds (see NECK_OK_MM) and whose contacts are as many as asked; else the best that stays valid; else the least bad that touches
    let searched = 0;
    const rank = q => Math.min(q.nc, need) * 10 + Math.min(q.neck, NECK_OK_MM) - 0.15 * (q.crowd || 0);
    function reachSide(P, side, jFix) {   // jFix: both sides move up or down by the same step, so they stay level with each other
      const dyStep = Math.max(1, Math.round(REACH_DY * PM.h)), cands = [];
      for (let k = 0; REACH_LO + k * REACH_STEP <= o.reachMax + 1e-9; k++) {
        const ov = Math.round((REACH_LO + k * REACH_STEP) * 1e6) / 1e6;
        cands.push({ ov, cost: (ov >= o.overlap ? ov - o.overlap : (o.normalise ? 1 : 1.5) * (o.overlap - ov)) / STEP });
      }
      cands.sort((a, b) => a.cost - b.cost || b.ov - a.ov);
      let valid = null, any = null, soft = null;
      for (const c of cands) {
        const x = xOf(P, side, c.ov), y = dyOf(P) + jFix * dyStep, q = pairQ(P, x, y, NECK_OK_MM, false, true); searched++;
        if (!q) continue;
        const hit = { x, y, ov: c.ov, j: jFix, q };
        if (q.good) { const qf = pairQ(P, x, y, NECK_OK_MM, false, true, true); if (qf && qf.good) return Object.assign({}, hit, { q: qf }); q.good = false; }      // (good on the coarse outlines, not on the full ones: only a candidate)
        if (q.crowdFail && q.nc >= need && q.neck >= NECK_OK_MM - 0.1 - 1e-9 && (!soft || q.crowd < soft.q.crowd)) soft = hit;      // (touches as asked but strokes run too close: the least crowded of these)
        if (q.valid && (!valid || rank(q) > rank(valid.q) + 1e-9 || (rank(q) > rank(valid.q) - 1e-9 && q.i > valid.q.i))) valid = hit;
        const pen = q.loose * 10 + Math.max(0, HOLES_MIN - q.kept) + Math.max(0, q.pct - OVERLAP_MAX_PCT) / 100;
        if (!any || pen < any.pen - 1e-12 || (pen < any.pen + 1e-12 && q.neck > any.q.neck)) any = Object.assign({ pen }, hit);
      }
      return soft || valid || any;
    }
    // both sides at once, with one shared vertical step j (nearest 0 first): the first j where both joins hold, else the j whose weaker side is best
    const hitCache = new Map(), reachOf = (P, nm, j) => { const key = nm + '|' + (P.cutPt || 0) + '|' + j; if (!hitCache.has(key)) hitCache.set(key, reachSide(P, nm, j)); return hitCache.get(key); };
    function reachBoth(keep, jMax) {
      let bestPair = null, kOk = null;
      for (const j of [0, 1, -1, 2, -2, 3, -3].filter(v => Math.abs(v) <= Math.min(REACH_DYN, jMax == null ? 9 : jMax))) {
        if (N && j !== 0) { const dyS = Math.max(1, Math.round(REACH_DY * PM.h)), tol = 1.0 / MM * SC; if (![PL, PR].every(P => dyOf(P) + j * dyS >= -tol && dyOf(P) + j * dyS + P.h <= PM.h + tol)) continue; }      // (a nudge never lifts a side letter out beyond the centre letter's ink)
        if (j === 0 && keep && !kOk) { kOk = [0, 2].map(k => { const q = pairQ(keep[k], keep[k].dx, keep[k].dy, NECK_OK_MM, false, true, true); return q && q.good ? { x: keep[k].dx, y: keep[k].dy, ov: null, j: 0, q, keep: true } : null; }); }
        const l = j === 0 && kOk && kOk[0] || reachOf(PL, 'left', j), r = j === 0 && kOk && kOk[1] || reachOf(PR, 'right', j);
        if (!l || !r) continue;
        if (l.q.good && r.q.good) return { hl: l, hr: r };
        const sc = h => h.q.valid ? rank(h.q) : -1, score = Math.min(sc(l), sc(r)) + 0.001 * (sc(l) + sc(r));
        if (!bestPair || score > bestPair.score + 1e-9) bestPair = { hl: l, hr: r, score };
      }
      return bestPair;
    }
    // a candidate finished placement from the reach search: the placement, its guards, its joins and its contacts
    function runReach(keepPl, jMax) {
      const both = reachBoth(keepPl, jMax); if (!both) return null;
      const pl = placeAt([both.hl.x, both.hl.y], [both.hr.x, both.hr.y]); let att = attemptPl(pl, null), bars = 0; tries++;
      if (!att.connected) { const m = mend(pl, att); if (m) { att = m.att; bars = m.bars; } }
      if (!att.connected) return { sides: true };
      return { att, g: guards(att), joins: joinsOf(pl, bars > 0), bars, nc: ncOf(att.pl), sides: true };
    }
    const mj = b => Math.min(Math.min(...b.joins), NECK_OK_MM), ncm = b => Math.min(b.nc[0], b.nc[1], need);
    const better = (c, b) => !b || (c.g.ok && !b.g.ok) || (c.g.ok === b.g.ok && (ncm(c) > ncm(b) || (ncm(c) === ncm(b) && ((c.bars || 0) < (b.bars || 0) || mj(c) > mj(b) + 1e-9 || (!c.g.ok && c.g.penalty < b.g.penalty - 1e-12 && mj(c) >= mj(b) - 1e-9)))));

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
      best.joins = joinsOf(best.att.pl); best.nc = ncOf(best.att.pl);
    }
    // a join that only grazes (or no join at all) or too few contacts: each side is searched over a wider reach
    let reached = false, reachedSides = false;
    if (o.reach && (!best || Math.min(...best.joins) < NECK_OK_MM - 1e-9 || ncm(best) < need)) {
      const c = runReach(best ? best.att.pl : null);
      if (c) { reachedSides = true; if (c.att && better(c, best)) { best = c; reached = true; } }
    }
    if (best && o.reach && !best.bars && [PL, PR].some(P => P.bodies.length > 1)) {      // (a part that only grazes the middle letter is tied to its own letter)
      const t = tieWeak(best.att.pl);
      if (t) { const att2 = attemptPl(t.pl, best.att.ov); tries++; if (att2.connected) { const g2 = guards(att2); if (g2.ok || !best.g.ok) best = { att: att2, g: g2, joins: joinsOf(t.pl, true), nc: ncOf(t.pl), bars: t.bars, reachedBefore: reached }; } }
    }
    if (!best) {
      // the letters do not touch, or a letter is made of separate parts that stay loose even though the main bodies join?
      const bodies = place(lastTried).map(P => { let top = null, ta = 0; for (const r of P.rg) { const a = area0(r); if (a > ta) { ta = a; top = r; } } return top; }).filter(Boolean);
      const joined = E.union(bodies).filter(r => area0(r) > 0).length === 1 || (reachedSides && [PL, PM, PR].some(P => P.bodies.length > 1));
      return failure(joined ? 'a letter is made of separate parts that stay loose, it cannot be one charm' : 'the letters do not touch', { tries, searched });
    }
    /* trim: a side letter that cannot reach two solid contacts and has a hairline reaching over towards the middle letter (ink that no 0.3 mm disc fits in, beyond its solid body) is shortened, smallest cut first */
    function extensionOf(P, side) {
      const rPt = o.contactMm / 2 / MM, body = E.grow(E.erode(P.region, rPt), rPt).filter(r => area0(r) > 0);
      if (!body.length) return null;
      const ib = bboxOf(P.region.filter(r => area0(r) > 0)), bb = bboxOf(body), len = side === 'right' ? bb[0] - ib[0] : ib[2] - bb[2];
      const max = Math.min(len - 0.1 / MM * SC, TRIM_MAX_PCT / 100 * P.w);
      return max >= 0.4 / MM * SC ? { len, max } : null;
    }
    function trimmedOf(P, side, cut) {
      const BIG = 1e8, ib = bboxOf(P.region.filter(r => area0(r) > 0));
      const x0 = side === 'right' ? ib[0] + cut : -BIG, x1 = side === 'right' ? BIG : ib[2] - cut;
      let reg = E.meet(P.region, [[{ X: x0, Y: -BIG }, { X: x1, Y: -BIG }, { X: x1, Y: BIG }, { X: x0, Y: BIG }]]);
      reg = E.clean(reg).filter(r => !(area0(r) > 0 && area0(r) < SLIVER_PT2 * SC * SC));
      // capped clean: a stroke the cut line crosses ends in a round cap at least TRIM_TIP_MM across, never a knife edge
      { const xc = side === 'right' ? x0 : x1, e = 0.03 * SC, strip = E.meet(reg, [[{ X: side === 'right' ? xc : xc - e, Y: -BIG }, { X: side === 'right' ? xc + e : xc, Y: -BIG }, { X: side === 'right' ? xc + e : xc, Y: BIG }, { X: side === 'right' ? xc : xc - e, Y: BIG }]]), caps = [];
        for (const comp of strip.filter(r => area0(r) > 0)) {
          const cb = bboxOf([comp]), ext = cb[3] - cb[1]; if (ext >= TRIM_TIP_MM / MM * SC) continue;
          const R = Math.max(TRIM_TIP_MM / 2 / MM * SC, ext / 2 + 0.02 / MM * SC), cy = (cb[1] + cb[3]) / 2;
          caps.push(Array.from({ length: 24 }, (_, i) => ({ X: Math.round(xc + R * Math.cos(i / 24 * 2 * Math.PI)), Y: Math.round(cy + R * Math.sin(i / 24 * 2 * Math.PI)) })));
        }
        if (caps.length) reg = E.clean(E.union([].concat(reg, caps))); }
      const pb = bboxOf(reg.filter(r => area0(r) > 0)); reg = shift(reg, -pb[0], 0);
      const ar = E.netArea(reg);
      if (!(ar > 0) || 1 - ar / P.ar > TRIM_INK_PCT / 100) return null;
      const counters = reg.filter(r => area0(r) <= -COUNTER_MIN_PT2 * SC * SC).map(r => r.slice().reverse());
      const bodies = reg.filter(r => area0(r) >= SLIVER_PT2 * SC * SC);
      if (counters.length !== P.counters.length || Math.abs(E.netArea(counters) - P.cAr) > 0.005 * Math.max(P.cAr, 1) || bodies.length > P.bodies.length) return null;
      return Object.assign({}, P, { region: reg, w: pb[2] - pb[0], ar, counters, cbox: counters.map(c => bboxOf([c])), cAr: E.netArea(counters), bodies, sreg: simp(reg), sbodies: simp(bodies), cutPt: (P.w - (pb[2] - pb[0])) / SC });
    }
    let altered = [];
    if (best && o.reach && o.allowTrim && need > 1 && ncm(best) < need) {
      const sides = [[0, PL, 'left'], [2, PR, 'right']].filter(([k]) => best.nc[k / 2] < need).map(([k, P, nm]) => ({ k, P, nm, e: extensionOf(P, nm) })).filter(x => x.e);
      const sum = b => Math.min(b.nc[0], need) + Math.min(b.nc[1], need);
      let done = null;
      for (let st = 1, last = Math.max(0, ...sides.map(x => Math.ceil(x.e.max / (TRIM_STEP_MM / MM * SC)))); st <= last && !done; st++) {
        const vs = sides.map(x => { const cut = Math.min(st * TRIM_STEP_MM / MM * SC, x.e.max); return { x, cut, v: trimmedOf(x.P, x.nm, cut) }; });
        if (vs.some(t => !t.v)) break;      // (more than the limits allow: no trim)
        for (const t of vs) setSide(t.x.k, t.v);
        const c = runReach(null, 1); tries++;
        if (c && c.att && c.g.ok && vs.every(t => c.nc[t.x.k / 2] >= need) && sum(c) > sum(best)) done = { c, vs };
        else for (const t of vs) setSide(t.x.k, t.x.P);
      }
      if (done) {
        best = done.c; reached = true; reachedSides = true;
        altered = done.vs.map(t => { const letter = (t.x.P.p.letter || '').toString(), what = t.x.nm === 'right' ? 'entry stroke shortened' : 'exit stroke shortened'; return { kind: 'trim', slot: t.x.nm, letter, what, mm: Math.round(t.cut / SC * MM * 10) / 10, text: `${letter} ${what}` }; });
      }
    }
    /* tie bars: what is still short of two solid contacts gets a bar of opts.barMm across the nearest gap between a solid part of the side letter and a solid part of the middle one, away from the contacts it has */
    function tieCandidates(pl, k, cl, taken) {
      const exist = [].concat(...cl.map(c => c.cores)), gapSC = (o.contactGapMm + 0.1) / MM * SC, spacing = 0.8 / MM * SC, out = [];
      const nearEx = pt => { let d = Infinity; for (const r of exist) for (let i = 0; i < r.length; i++) { const h = toSeg(pt, r[i], r[(i + 1) % r.length]); if (h.d < d) d = h.d; } return d; };
      for (const rr of [o.contactMm / 2, 0.15]) {      // (a bar is fixed in the thickest ink first, then in strokes 0.3 mm wide, never in a thinner one: the contact it makes must be solid too)
        const rPt = rr / MM, cP = E.erode(pl[k].rg, rPt).filter(r => area0(r) > 0), cM = E.erode(pl[1].rg, rPt).filter(r => area0(r) > 0);
        if (!cP.length || !cM.length) continue;
        const pts = [].concat(...cP), stride = Math.max(1, Math.floor(pts.length / 300)), found = [];
        for (let i = 0; i < pts.length; i += stride) {
          const pt = pts[i]; let bd = Infinity, bq = null, inside = false;
          for (const r of cM) { if (C.Clipper.PointInPolygon(pt, r) !== 0) { inside = true; break; } for (let j = 0; j < r.length; j++) { const h = toSeg(pt, r[j], r[(j + 1) % r.length]); if (h.d < bd) { bd = h.d; bq = { X: h.X, Y: h.Y }; } } }
          if (inside || !bq || bd > BAR_MAX || bd <= 0) continue;
          if (exist.length && nearEx(bq) < gapSC) continue;
          if (taken.some(t => Math.hypot(t.X - bq.X, t.Y - bq.Y) < gapSC)) continue;
          found.push({ d: bd, p: pt, q: bq });
        }
        found.sort((a, b) => a.d - b.d);
        for (const f of found) if (!out.some(g => Math.hypot(g.q.X - f.q.X, g.q.Y - f.q.Y) < spacing)) out.push(f);
        if (out.length >= 4) break;
      }
      return out;
    }
    // a tie bar counts as one contact of its own width (it is wider than a stroke); natural contacts are counted before it is placed, so the bar is never counted twice
    function solidOne(pl, k, cl) {
      if (cl.some(c => { const b = bboxOf(c.cores); return Math.hypot(b[2] - b[0], b[3] - b[1]) >= 2 * o.contactGapMm / MM * SC; })) return true;
      const inter = E.meet(pl[k].rg, pl[1].rg);
      return cl.some(c => widestOf(E.meet(inter, [c.ring]), 2 * o.contactMm + 0.1) >= 2 * o.contactMm - 1e-9);
    }
    function addContacts(pl0, g0, nc0) {
      let pl = pl0; const added = [0, 0], areas = [[], []], long = [false, false]; let fail = null;
      for (const k of [0, 2]) {
        const cl = contactsAt(pl0, k), taken = [];
        for (let it = 0; it < need && cl.length + added[k / 2] < need; it++) {
          let done = null, fallback = null;
          for (const c of tieCandidates(pl, k, cl, taken).slice(0, 8)) {
            const bar = barOf(c, o.barMm / MM * SC), pl2 = withBars(pl, k, [bar]), g2 = guards(attemptPl(pl2, null));
            if (g2.holesKept >= Math.min(HOLES_MIN, g0.holesKept) - 1e-9) { done = { pl2, c, bar }; break; }
            if (!fallback || g2.holesKept > fallback.h) fallback = { pl2, c, bar, h: g2.holesKept };
          }
          if (!done && fallback) done = fallback;
          if (!done && solidOne(pl0, k, cl)) { long[k / 2] = true; break; }   // (solid letters: one contact that is long (two contacts' length) or wide (twice the asked width) holds as two, there is no gap to bar across)
          if (!done) { fail = k; break; }
          pl = done.pl2; taken.push(done.c.q); added[k / 2]++; areas[k / 2].push(Math.round(E.netArea(E.meet([done.bar], pl[1].rg)) / SC / SC * MM * MM * 100) / 100);
        }
        if (fail != null) break;
      }
      return { pl, added, fail, long, areas };
    }
    const contacts = { leftMiddle: [], middleRight: [] };      // (the natural contacts, in mm2 of the overlap: before any tie bar)
    [['leftMiddle', 0], ['middleRight', 2]].forEach(([nm, k]) => { const inter = E.meet(best.att.pl[k].rg, best.att.pl[1].rg); contacts[nm] = clustersOf(inter).map(c => Math.round(E.netArea(E.meet(inter, [c.ring])) / SC / SC * MM * MM * 100) / 100); });
    let tieBars = [0, 0], longContact = [false, false];
    if (need > 1 && ncm(best) < need) {
      const dc = addContacts(best.att.pl, best.g);
      if (dc.fail != null) return failure(`the ${dc.fail === 0 ? 'Left and Middle' : 'Middle and Right'} letters can touch in only one place and no tie bar fits between them`, { tries, searched });
      const att2 = attemptPl(dc.pl, best.att.ov); tries++;
      best = Object.assign({}, best, { att: att2, g: guards(att2), nc: [dc.long[0] ? need : contacts.leftMiddle.length + dc.added[0], dc.long[1] ? need : contacts.middleRight.length + dc.added[1]] }); tieBars = dc.added; longContact = dc.long; contacts.leftMiddle.push(...dc.areas[0]); contacts.middleRight.push(...dc.areas[1]);
    }
    const { att, g } = best, warnings = [], bars = best.bars || 0;
    const joins = best.joins.map((w, k) => Math.max(w, tieBars[k] ? o.barMm : 0));      // (a tie bar is a join of its own width)
    // are the two sides level? (the same baseline with metrics, the same centre without; both measured on the finished placement)
    const [pL, , pR] = att.pl, mmOf = v => Math.round(Math.abs(v) / SC * MM * 100) / 100;
    const level = { metrics: metricsOn, baselineDiffMm: metricsOn ? mmOf((pL.dy + pL.base) - (pR.dy + pR.base)) : null, centreDiffMm: mmOf((pL.dy + pL.h / 2) - (pR.dy + pR.h / 2)) };
    if (metricsOn) notes.push(`the three letters stand on one baseline, the side letters centred on the middle letter (sides ${Math.round(sideNom / midTop * 100)} % of its height above the baseline)`);
    // a letter drawn in several separate parts (a flourish apart from its letter) is said so: each part is held by the middle letter, or tied on
    const parts = { left: PL.bodies.length, middle: PM.bodies.length, right: PR.bodies.length };
    for (const [slot, k] of [['left', PL], ['middle', PM], ['right', PR]]) if (k.bodies.length > 1) notes.push(`the ${slot} letter is drawn in ${k.bodies.length} separate parts` + (bars ? ': a part that no overlap reaches was tied to the rest with a ' + BRIDGE_W_MM + ' mm bar' : ', each held by a neighbour'));
    if (bars) warnings.push(`a loose part of a letter was tied on with a ${BRIDGE_W_MM} mm bar, look at the picture`);
    for (const a of altered.slice()) notes.push(`${a.letter}: ${a.what} ${a.mm.toFixed(1)} mm so the letter meets its neighbour in ${need} solid places`);
    [['Left and Middle', 0], ['Middle and Right', 1]].forEach(([nm, i]) => { if (tieBars[i]) { notes.push(`the ${nm} letters met in fewer than ${need} solid places: ${tieBars[i]} tie bar${tieBars[i] > 1 ? 's' : ''} of ${o.barMm} mm added`); altered.push({ kind: 'bar', pair: nm, n: tieBars[i], mm: o.barMm, what: 'tie bar added', text: `${tieBars[i] > 1 ? tieBars[i] + ' tie bars' : 'tie bar'} between the ${nm} letters` }); } });
    [['leftMiddle', 'Left and Middle', 0], ['middleRight', 'Middle and Right', 1]].forEach(([nm, lab, i]) => { const n = contacts[nm].length; if (longContact[i]) notes.push(`the ${lab} letters are joined by one long or wide solid contact, there is no gap for a second one`); else if (need > 1 && n < need) warnings.push(`only ${n} solid contact${n === 1 ? '' : 's'} between the ${lab} letters, check before cutting`); });
    const moved = att.pl.map((P, i) => i === 1 ? 0 : P.dy - dyOf(P));
    const ovs = [0, 2].map(k => { const P = att.pl[k]; return Math.round((k === 0 ? (P.dx + P.w) : (PM.w - P.dx)) / P.w * 100) / 100; });
    if (reached) {
      if (Math.abs(ovs[0] - ovs[1]) < 0.005 && !moved[0] && !moved[2]) { if (Math.abs(ovs[0] - o.overlap) >= 0.005) notes.push(`overlap ${ovs[0] > o.overlap ? 'raised' : 'lowered'} from ${Math.round(o.overlap * 100)} % to ${Math.round(ovs[0] * 100)} % so the letters join firmly`); }
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
      if (w < NECK_OK_MM - 0.1 - 1e-9) { notes.push(`the join between the ${nm} letters is thin (${w.toFixed(2)} mm)`); warnings.push(`thin join between the ${nm.replace('left', 'Left').replace('middle', 'Middle').replace('right', 'Right')} letters (${w.toFixed(2)} mm), check before cutting`); }
    });

    // the finished piece: outer ring CCW first, then the holes CW, largest first; everything shifted so the piece starts at 0,0
    const b = bboxOf(att.rings), H = b[3] - b[1];
    // the finished piece is stretched across as a whole when its width / height is off the ASPECT_TARGET (within what each letter has left of its STRETCH_CAP): positions and contacts keep their places, only the width changes
    let fx = 1; const aspect0 = (b[2] - b[0]) / H;
    if (N && (aspect0 < N.ASPECT_MIN + 0.04 || aspect0 > N.ASPECT_MAX - 0.04)) {
      const medW = (mid.em ? N.MID_BODY_W_OVER_EM_MEDIAN * mid.em * SC : N.MID_BODY_W_OVER_INKH_MEDIAN * mid.h) * cs, mW = mid.bw * cs * plan[1].f;
      const capHi = Math.min(...plan.map(q => (1 + N.STRETCH_CAP) / q.f)), capLo = Math.max(...plan.map(q => (1 - N.STRETCH_CAP) / q.f));      // (the cap is never passed)
      const bandHi = N.MID_WIDTH_MAX * medW / mW, bandLo = N.MID_WIDTH_MIN * medW / mW;
      fx = Math.min(capHi, Math.max(capLo, Math.min(bandHi, Math.max(bandLo, N.ASPECT_TARGET / aspect0))));
      if (Math.abs(fx - 1) < 0.005) fx = 1;
    }
    const W = (b[2] - b[0]) * fx;
    const toPt = r => { const pts = r.map(q => [(q.X - b[0]) / SC * fx, (q.Y - b[1]) / SC]); return pts; };
    // sizes and limits of this word (NORMS): what was measured, what was stretched, which limit is not met (the word is then for a person to look at)
    let sizes = null; const limits = [];
    if (N) {
      const mBH = mid.bh * cs, mBW = mid.bw * cs * plan[1].f, med = (mid.em ? N.MID_BODY_W_OVER_EM_MEDIAN * mid.em * SC : N.MID_BODY_W_OVER_INKH_MEDIAN * mid.h) * cs, r2 = v => Math.round(v * 100) / 100;
      const fxF = (plan[0].f * fx), stretch = plan.map((q, i) => ({ slot: SLOTS[i], letter: q.p.letter, asked: r2(q.need), applied: r2(q.f * fx), capped: Math.abs(q.need - q.f) > 0.005 }));
      sizes = { sideHeightL: r2(left.bh * plan[0].s / mBH), sideHeightR: r2(right.bh * plan[2].s / mBH), sideWidthL: r2(left.bw * plan[0].s * plan[0].f / mBW), sideWidthR: r2(right.bw * plan[2].s * plan[2].f / mBW), centreWidthVsMedian: r2(mBW * fx / med), stretch, aspect: r2((b[2] - b[0]) * fx / H), aspectBeforeStretch: r2(aspect0), overlapFraction: { left: r2(g.lm.pct / 100), middle: r2(g.midPct / 100), right: r2(g.mr.pct / 100) }, crowdMm: r2((g.crowd.lm || 0) + (g.crowd.mr || 0) + (g.crowd.lr || 0)), constants: o.norms };
      const lim = (key, bad, text, value) => { if (bad) { limits.push({ key, value, text }); warnings.push(text); } };
      lim('aspect', sizes.aspect < N.ASPECT_MIN - 1e-9 || sizes.aspect > N.ASPECT_MAX + 1e-9, `overall width / height ${sizes.aspect} is outside ${N.ASPECT_MIN} to ${N.ASPECT_MAX}`, sizes.aspect);
      lim('overlapSide', Math.max(sizes.overlapFraction.left, sizes.overlapFraction.right) > N.OVERLAP_SIDE_MAX + 1e-9, `a side letter lies ${Math.round(Math.max(sizes.overlapFraction.left, sizes.overlapFraction.right) * 100)} % inside its neighbour (limit ${Math.round(N.OVERLAP_SIDE_MAX * 100)} %)`, Math.max(sizes.overlapFraction.left, sizes.overlapFraction.right));
      lim('overlapCentre', sizes.overlapFraction.middle > N.OVERLAP_MID_MAX + 1e-9, `the centre letter lies ${Math.round(sizes.overlapFraction.middle * 100)} % inside the side letters (limit ${Math.round(N.OVERLAP_MID_MAX * 100)} %)`, sizes.overlapFraction.middle);
      lim('crowding', sizes.crowdMm > 3 + 1e-9, `${sizes.crowdMm} mm of strokes of different letters lie closer than ${N.GAP_MIN_MM} mm without touching (limit 3 mm)`, sizes.crowdMm);
      lim('counters', g.holesKept < N.COUNTERS_MIN - 1e-9, `a letter keeps only ${Math.round(g.holesKept * 100)} % of its inner openings (limit ${Math.round(N.COUNTERS_MIN * 100)} %)`, g.holesKept);
      for (const q of stretch) if (q.capped) notes.push(`${q.letter}: ${q.asked > q.applied ? 'narrow, stretched' : 'wide, squeezed'} ${Math.round(Math.abs(q.applied - 1) * 100)} % of ${Math.round(Math.abs(q.asked - 1) * 100)} % needed`);
    }
    const outer = att.rings.find(r => area0(r) > 0);
    const holes = att.rings.filter(r => area0(r) < 0).sort((p, q) => area0(p) - area0(q) || bboxOf([p])[1] - bboxOf([q])[1] || bboxOf([p])[0] - bboxOf([q])[0]);
    const rings = [{ pts: toPt(outer), hole: false }].concat(holes.map(h => ({ pts: toPt(h), hole: true })));
    const placed = att.pl.map(P => ({ letter: P.p.letter, slot: P.p.slot, scale: round3(P.s), scaleX: round3(P.sx * fx), trimMm: P.cutPt ? Math.round(P.cutPt * MM * 10) / 10 : 0, bboxPt: [P.dx, P.dy, P.dx + P.w, P.dy + P.h].map((v, i) => round3((v - b[i % 2]) / SC * (i % 2 ? 1 : fx))) }));
    return {
      ok: true, why: '', rings, bboxPt: [0, 0, round3(W / SC), round3(H / SC)], wMm: Math.round(W / SC * MM * 1e4) / 1e4, hMm: Math.round(H / SC * MM * 1e4) / 1e4, placed,
      report: { connected: true, sizes, limits, review: limits.length > 0, overlapPct: { leftMiddle: Math.round(g.lm.pct * 10) / 10, middleRight: Math.round(g.mr.pct * 10) / 10 }, holesKept: round3(g.holesKept), centerScale: round3(cs), overlap: att.ov == null ? round3((ovs[0] + ovs[1]) / 2) : att.ov, reach: reached, searched, warnings, parts, bridges: bars, contacts, tieBars: { leftMiddle: tieBars[0], middleRight: tieBars[1] }, altered, level, joinMm: { leftMiddle: joins[0], middleRight: joins[1] }, moveMm: { left: Math.round(moved[0] / SC * MM * 100) / 100, right: Math.round(moved[2] / SC * MM * 100) / 100 }, joinMm2: { leftMiddle: Math.round(g.lm.i / SC / SC * MM * MM * 100) / 100, middleRight: Math.round(g.mr.i / SC / SC * MM * MM * 100) / 100 }, tries, notes }
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

  const api = { lettersOf, familiesOf, slotsOf, build, toPdf, svgPath, NORMS };
  root.CharmNestMonogram = api;
  if (typeof module === 'object' && module.exports) module.exports = api;
})(typeof window !== 'undefined' ? window : globalThis);
