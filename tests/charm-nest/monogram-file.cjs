// Paul's alphabet file, as he has it (MONOGRAMFILE, 15 Oct 2026): SKUs_MONOGRAM_2024-1015.ai holds NO vector path. Its 78 letters are live text in three
// embedded fonts (MASTER CIRCLE LEFT / MIDDLE / RIGHT), one text object per letter, with a SKU label in small Myriad under each ("Monogram_Left_A" ...) and the
// words LEFT / MIDDLE / RIGHT over the three rows. "You need to update the system so it can correctly interpret this file in its current form."
// The reader (charm-nest-pdf.js section 3c) draws such text as outlines when it is the design, and only then. This test asks, on the REAL file:
//   · 78 charms, 78 distinct labelled SKUs MONOGRAM_LEFT_A ... MONOGRAM_RIGHT_Z, the three headers are no charm and no SKU, nothing unlabelled;
//   · every letter's outline agrees with the live text (box, counters by an independent count from the font program), and with how pdf.js would draw it;
//   · each per-SKU file written by buildSingleCharm reads back as the same letter (one charm, the same cut lines) and carries no font and no text;
//   · the same file twice gives the same bytes and hashes; a font that cannot be read, a Type 3 font, a font that is not embedded leave the text as text;
//   · a CID font (Identity-H), a font with only a Macintosh cmap, a bare CFF (Type1C) font and a scaled page read the same letters;
//   · text that is NOT the design (a coloured note, a word, a small letter, a letter with no label, a letter standing ON a vector outline) is left alone, and a master that
//     has real paths is read as before;
//   · the three ways of indexing (the local script, the server route, the Master tab) write the same 78 SKUs, and a second run writes nothing.
//   PW_DIR=/opt/node-tools/node_modules node tests/charm-nest/monogram-file.cjs        (MONO_SHOTS=<dir> also writes the contact sheet of the 78 letters)
'use strict';
const fs = require('fs'), os = require('os'), path = require('path'), assert = require('assert');
const { CharmNestPDF: P, Geom, PDFLib } = require('../../netlify/functions/_charmNestPdf.js');
const opentype = require('../../vendor/opentype-1.3.4.min.js');
const R = require('./_masterRoutes.cjs');
const FILE = path.join(__dirname, 'fixtures', 'SKUs_MONOGRAM_2024-1015.ai');
const { PDFDocument, PDFName, PDFArray, PDFString } = PDFLib;
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const MM = 25.4 / 72, PATTERN = P.SKU_PATTERN_DEFAULT;
const ROWS = [['LEFT', 'TT0', 'MASTERCIRCLELEFT'], ['MID', 'TT1', 'MASTERCIRCLEMIDDLE'], ['RIGHT', 'TT2', 'MASTERCIRCLERIGHT']];
const LETTERS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('');
const SKUS = ROWS.flatMap(([r]) => LETTERS.map(l => `MONOGRAM_${r}_${l}`));

/* ── helpers: read the file the way the app does, and make variants of it ── */
const readAll = async (bytes, o) => {
  o = o || {};
  const parsed = await P.parseSource(bytes, 'monogram.ai'), g = P.groupCharms(parsed, { minPt: 6 });
  const lab = P.labelCharms(parsed, g.charms, { pattern: PATTERN, gapPt: 18.14, widen: 0.25 });
  return { parsed, g, lab, live: g.charms.filter(c => c.mergedInto == null), bySku: new Map([...lab.labels].map(([i, l]) => [l.sku, g.charms.find(c => c.index === i)])) };
};
const dec = s => PDFLib.decodePDFRawStream(s).decode();
const open = bytes => PDFDocument.load(bytes, { ignoreEncryption: true, updateMetadata: false });
const contentOf = doc => { const c = doc.context.lookup(doc.getPage(0).node.get(PDFName.of('Contents'))); const parts = c instanceof PDFArray ? c.asArray().map(r => dec(doc.context.lookup(r))) : [dec(c)]; return Buffer.concat(parts.map(p => Buffer.from(p))).toString('latin1'); };
const setContent = (doc, text) => doc.getPage(0).node.set(PDFName.of('Contents'), doc.context.register(doc.context.flateStream(Buffer.from(text, 'latin1'))));
const fontOf = (doc, name) => { const fd = doc.context.lookup(doc.getPage(0).node.Resources().get(PDFName.of('Font'))); const ref = fd.get(PDFName.of(name)); const dict = doc.context.lookup(ref); return { fd, ref, dict, desc: doc.context.lookup(dict.get(PDFName.of('FontDescriptor'))) }; };
const save = async doc => new Uint8Array(await doc.save({ useObjectStreams: false }));
const fontProgram = (doc, name) => { const f = fontOf(doc, name); const raw = dec(doc.context.lookup(f.desc.get(PDFName.of('FontFile2')))); return Buffer.from(raw); };
const parseFont = raw => opentype.parse(raw.buffer.slice(raw.byteOffset, raw.byteOffset + raw.byteLength));
const variant = {
  async edit(bytes, fn) { const doc = await open(bytes); setContent(doc, fn(contentOf(doc))); return save(doc); },
  async corrupt(bytes, name) { const doc = await open(bytes); const f = fontOf(doc, name); doc.context.assign(f.desc.get(PDFName.of('FontFile2')), doc.context.flateStream(Buffer.from('this is not a font program at all, only text'.repeat(40)))); return save(doc); },
  async notEmbedded(bytes, name) { const doc = await open(bytes); fontOf(doc, name).desc.delete(PDFName.of('FontFile2')); return save(doc); },
  async type3(bytes, name) { const doc = await open(bytes); const f = fontOf(doc, name); f.dict.set(PDFName.of('Subtype'), PDFName.of('Type3')); f.dict.delete(PDFName.of('FontDescriptor')); return save(doc); },
  // the font as a Type0 / CIDFontType2 / Identity-H font: its strings in the content become 2-byte glyph ids
  async cid(bytes, name) {
    const doc = await open(bytes), f = fontOf(doc, name), font = parseFont(fontProgram(doc, name)), ctx = doc.context;
    const W = []; for (let code = 65; code <= 90; code++) { const g = font.charToGlyph(String.fromCharCode(code)); W.push(g.index, [Math.round(g.advanceWidth / font.unitsPerEm * 1000)]); }
    const cidRef = ctx.register(ctx.obj({ Type: 'Font', Subtype: 'CIDFontType2', BaseFont: 'AAAAAA+' + name, CIDSystemInfo: { Registry: PDFString.of('Adobe'), Ordering: PDFString.of('Identity'), Supplement: 0 }, FontDescriptor: f.dict.get(PDFName.of('FontDescriptor')), DW: 1000, W, CIDToGIDMap: PDFName.of('Identity') }));
    ctx.assign(f.ref, ctx.obj({ Type: 'Font', Subtype: 'Type0', BaseFont: 'AAAAAA+' + name, Encoding: PDFName.of('Identity-H'), DescendantFonts: [cidRef] }));
    let cur = null;
    setContent(doc, contentOf(doc).split('\n').map(l => { const m = /^\/(\w+) 1 Tf$/.exec(l.trim()); if (m) cur = m[1]; const t = /^\((.)\)Tj$/.exec(l.trim()); return t && cur === name ? '<' + font.charToGlyph(t[1]).index.toString(16).padStart(4, '0') + '>Tj' : l; }).join('\n'));
    return save(doc);
  },
  // the font program with only a Macintosh (1,0) cmap, which opentype.js refuses to read
  async mac(bytes, name) {
    const doc = await open(bytes), f = fontOf(doc, name), raw = fontProgram(doc, name), font = parseFont(raw);
    const cnt = raw.readUInt16BE(4), tabs = []; for (let i = 0; i < cnt; i++) { const o = 12 + 16 * i; tabs.push({ tag: raw.toString('latin1', o, o + 4), off: raw.readUInt32BE(o + 8), len: raw.readUInt32BE(o + 12) }); }
    const sub = Buffer.alloc(262); sub.writeUInt16BE(0, 0); sub.writeUInt16BE(262, 2); for (let c = 0; c < 256; c++) { const g = font.charToGlyph(String.fromCharCode(c)).index; sub[6 + c] = g < 256 ? g : 0; }
    const cm = Buffer.alloc(12 + 262); cm.writeUInt16BE(1, 2); cm.writeUInt16BE(1, 4); cm.writeUInt32BE(12, 8); sub.copy(cm, 12);
    const list = tabs.map(t => t.tag === 'cmap' ? ['cmap', cm] : [t.tag, raw.subarray(t.off, t.off + t.len)]).sort((a, b) => (a[0] < b[0] ? -1 : 1));
    let off = 12 + 16 * list.length; const hdr = Buffer.alloc(off); raw.copy(hdr, 0, 0, 12); hdr.writeUInt16BE(list.length, 4); const body = [];
    list.forEach(([t, d], i) => { const o = 12 + 16 * i; hdr.write(t, o, 'latin1'); hdr.writeUInt32BE(off, o + 8); hdr.writeUInt32BE(d.length, o + 12); const pad = (4 - d.length % 4) % 4; body.push(d, Buffer.alloc(pad)); off += d.length + pad; });
    doc.context.assign(f.desc.get(PDFName.of('FontFile2')), doc.context.flateStream(Buffer.concat([hdr, ...body])));
    return save(doc);
  }
};
// the counters of a glyph counted from the font program itself, with no code of the reader: contours that turn the other way from the biggest one
const contourFacts = (font, ch) => {
  const g = font.charToGlyph(ch), subs = []; let cur = null;
  for (const c of g.path.commands) { if (c.type === 'M') { cur = [[c.x, c.y]]; subs.push(cur); } else if (c.type === 'Z') cur = null; else if (cur) { if (c.type === 'Q') cur.push([c.x1, c.y1]); if (c.type === 'C') cur.push([c.x1, c.y1], [c.x2, c.y2]); cur.push([c.x, c.y]); } }
  const area = p => { let a = 0; for (let i = 0; i < p.length; i++) { const q = p[(i + 1) % p.length]; a += p[i][0] * q[1] - q[0] * p[i][1]; } return a / 2; };
  const ar = subs.map(area).filter(a => Math.abs(a) > 1e-3); let big = 0; ar.forEach((a, i) => { if (Math.abs(a) > Math.abs(ar[big])) big = i; });
  const holes = ar.filter(a => Math.sign(a) !== Math.sign(ar[big])).length;
  // the lowest ink, exactly (a quadratic curve's own extremum; opentype.js's getBoundingBox is not exact), font units, y up, the baseline is 0
  let low = Infinity, lx = 0, ly = 0;
  for (const c of g.path.commands) {
    if (c.type === 'M' || c.type === 'L') { low = Math.min(low, c.y); lx = c.x; ly = c.y; }
    else if (c.type === 'Q') { const d = ly - 2 * c.y1 + c.y, t = d ? (ly - c.y1) / d : -1; let m = Math.min(ly, c.y); if (t > 0 && t < 1) { const u = 1 - t; m = Math.min(m, u * u * ly + 2 * u * t * c.y1 + t * t * c.y); } low = Math.min(low, m); lx = c.x; ly = c.y; }
    else if (c.type === 'C') { low = Math.min(low, c.y); lx = c.x; ly = c.y; } }
  return { holes, islands: ar.length - holes - 1, contours: ar.length, adv: g.advanceWidth / font.unitsPerEm, low };
};

(async () => {
  const bytes = new Uint8Array(fs.readFileSync(FILE));
  const doc0 = await open(bytes), fonts = Object.fromEntries(ROWS.map(([r, tt]) => [r, parseFont(fontProgram(doc0, tt))]));

  /* ── 1 · the real file ── */
  const t0 = Date.now();
  const A = await readAll(bytes), parseMs = Date.now() - t0;
  ok(parseMs < 4000, `the whole file reads in ${parseMs} ms (budget: a few seconds)`);
  ok(JSON.stringify(A.parsed.counts) === JSON.stringify({ paths: 0, top: 3, xobjects: 0, text: 3 }), 'the file as drawn: no path, three text objects: ' + JSON.stringify(A.parsed.counts));
  const td = A.parsed.textDesign;
  ok(td && td.converted === 78 && td.kept.length === 0, 'all 78 letters were drawn as outlines, nothing left as text: ' + JSON.stringify(td && { c: td.converted, k: td.kept }));
  ok(A.live.length === 78, `78 charms (${A.live.length})`);
  ok(A.lab.labels.size === 78 && A.lab.skuCount === 78 && A.lab.unlabelled.length === 0 && A.lab.orphans.length === 0 && A.lab.duplicates.length === 0 && A.lab.undecodable.length === 0,
    'every charm labelled, no orphan label, no duplicate: ' + JSON.stringify({ l: A.lab.labels.size, u: A.lab.unlabelled, o: A.lab.orphans.map(x => x.sku), d: A.lab.duplicates.length }));
  ok([...A.bySku.keys()].sort().join('|') === SKUS.slice().sort().join('|'), 'the 78 SKUs are exactly MONOGRAM_LEFT_A ... MONOGRAM_RIGHT_Z');
  ok([...A.lab.labels.values()].every(l => l.size == null && !(l.extra || []).length), 'no label was read as a size, none was merged with another');
  ok(A.lab.captions.map(c => c.str).sort().join() === 'LEFT,MIDDLE,RIGHT', 'the headers LEFT / MIDDLE / RIGHT are captions: no charm, no SKU, no orphan label');
  ok(![...A.bySku.keys()].some(k => /^(LEFT|MIDDLE|RIGHT)$/.test(k)) && A.live.every(c => c.outline.glyphChar), 'no charm is a header word');
  ok(A.g.stray.length === 0 && A.g.markers.length === 0 && A.g.sampleText.length === 0, 'nothing of a letter was taken out of it as marker, stray ink or sample text');
  ok(A.g.charms.reduce((s, c) => s + c.members.length, 0) === td.contours, `every one of the ${td.contours} contours is in a charm`);
  // each letter: the right glyph, its box inside the live text's box, its counters counted independently
  const pieces = A.parsed.segments.find(s => s.kind === 'text' && s.pieces && s.pieces[0].font === 'TT0').pieces, table = [];
  let islanded = [];
  ROWS.forEach(([r, tt], ri) => LETTERS.forEach((L, li) => {
    const sku = `MONOGRAM_${r}_${L}`, c = A.bySku.get(sku), piece = pieces[ri * 26 + li], f = contourFacts(fonts[r], L);
    ok(c && c.outline.glyphChar === L && piece.str === L && piece.font === tt, `${sku}: the charm is the ${L} of ${tt}`);
    const ink = c.bbox, pb = piece.bbox;
    ok(ink[0] >= pb[0] - 1 && ink[2] <= pb[2] + 1 && ink[1] >= pb[1] - 1 && ink[3] <= pb[3] + 1, `${sku}: its ink ${ink.map(v => v.toFixed(1))} is inside the live text's box ${pb.map(v => v.toFixed(1))}`);
    const holes = c.members.filter(m => m.glyphHole).length, isl = c.members.filter(m => !m.glyphHole).length - 1;
    ok(holes === f.holes && isl === f.islands && c.members.length === f.contours, `${sku}: ${holes} counter(s) and ${isl} detached piece(s) as the font program has them (${f.holes}, ${f.islands})`);
    ok(P.cutLinesOf(c).length === holes + isl, `${sku}: cutLinesOf = counters + detached pieces`);
    const w = (ink[2] - ink[0]) * MM, h = (ink[3] - ink[1]) * MM;
    ok(w > 2.5 && w < 9 && h > 8 && h < 16, `${sku}: plausible size ${w.toFixed(1)} x ${h.toFixed(1)} mm`);
    if (isl) islanded.push(sku);
    table.push([sku, w.toFixed(1), h.toFixed(1), holes, isl].join('\t'));
  }));
  if (process.env.CN_VERBOSE) console.log('SKU\twidth mm\theight mm\tholes\tdetached\n' + table.join('\n'));
  ok(islanded.join() === 'MONOGRAM_LEFT_Q,MONOGRAM_MID_Q,MONOGRAM_RIGHT_T', 'the letters with a detached piece (a Q tail, a T flourish) are exactly those three, each still ONE charm: ' + islanded);
  const holesOf = sku => A.bySku.get(sku).members.filter(m => m.glyphHole).length;
  for (const L of ['A', 'B', 'D', 'P', 'R']) for (const [r] of ROWS) ok(holesOf(`MONOGRAM_${r}_${L}`) >= 1, `MONOGRAM_${r}_${L} has a counter`);
  for (const L of ['O', 'Q']) for (const r of ['LEFT', 'RIGHT']) ok(holesOf(`MONOGRAM_${r}_${L}`) === 1, `MONOGRAM_${r}_${L} has its one counter`);
  ok(holesOf('MONOGRAM_MID_O') === 0 && holesOf('MONOGRAM_MID_Q') === 0, "the Middle O and Q are open ribbons with no closed counter (the font's own: the independent count above agrees, and the picture shows it)");
  // the Middle alphabet is the bigger one
  const med = r => { const hs = LETTERS.map(L => A.bySku.get(`MONOGRAM_${r}_${L}`).bbox).map(b => b[3] - b[1]).sort((a, b) => a - b); return hs[13]; };
  ok(med('MID') > 1.05 * med('LEFT') && med('MID') > 1.05 * med('RIGHT'), `the Middle letters are taller than the sides (median ${(med('MID') * MM).toFixed(1)} vs ${(med('LEFT') * MM).toFixed(1)} / ${(med('RIGHT') * MM).toFixed(1)} mm)`);

  /* ── 2 · the same file twice: the same everything ── */
  const B2 = await readAll(bytes);
  const hashOf = (rd, sku) => { const c = rd.bySku.get(sku), sil = Geom.silhouetteBits(c, 6, {}); return P.fnv(P.signature(sil.bits, sil.w, sil.h) + '|' + Math.round((sil.bboxOuter[2] - sil.bboxOuter[0]) * 2) + 'x' + Math.round((sil.bboxOuter[3] - sil.bboxOuter[1]) * 2) + '|' + c.members.length); };
  ok(JSON.stringify(A.parsed.segments.filter(s => s.textGlyph)) === JSON.stringify(B2.parsed.segments.filter(s => s.textGlyph)), 'the outlines are the same to the last digit on a second read');
  ok(SKUS.every(s => hashOf(A, s) === hashOf(B2, s)), 'every SKU has the same charm hash on a second read');
  ok(new Set(SKUS.map(s => hashOf(A, s))).size === 78, 'the 78 letters have 78 different hashes');

  /* ── 3 · what each per-SKU file holds ── */
  let bad = [], maxBytes = 0; const baseR = {}, baseY = {};
  for (const sku of SKUS) {
    const c = A.bySku.get(sku); P.integrateRings(c);
    const one = await P.buildSingleCharm(c, A.parsed); maxBytes = Math.max(maxBytes, one.length);
    const p2 = await P.parseSource(new Uint8Array(one), sku + '.ai'), g2 = P.groupCharms(p2, { minPt: 6 }), live = g2.charms.filter(x => x.mergedInto == null);
    // how the app reads a master copy back (charm-nest-bridge readMasterCharm): the biggest charm, and whatever else the file draws joins it
    const big = live.reduce((a, b) => ((b.bbox[2] - b.bbox[0]) * (b.bbox[3] - b.bbox[1]) > (a.bbox[2] - a.bbox[0]) * (a.bbox[3] - a.bbox[1]) ? b : a));
    const all = live.flatMap(x => x.members), cuts = all.filter(m => m !== big.outline && P.isCutLine(m)).length;
    const onePiece = c.members.length === 1 + P.cutLinesOf(c).length;
    // the baseline travels with the letter: the Subject of its file is {"glyph":{baselinePt,emPt,advancePt,font}} (baselinePt = baseline y minus the lowest ink y, y up)
    { const [r, tt, nm] = ROWS[Math.floor(SKUS.indexOf(sku) / 26)], L = sku.slice(-1), f = contourFacts(fonts[r], L), g = p2.glyph, subj = p2.doc.getSubject();
      if (!g || JSON.stringify(Object.keys(g)) !== '["baselinePt","emPt","advancePt","font"]' || subj !== JSON.stringify({ glyph: g }) || g.font !== nm || Math.abs(g.emPt - 52.9943) > 1e-3 || Math.abs(g.advancePt - f.adv * 52.9943) > 0.06 || Math.abs(g.baselinePt - (-f.low * g.emPt / fonts[r].unitsPerEm)) > 0.01) bad.push(`${sku}: glyph metrics ${subj}`);
      else { (baseR[r] = baseR[r] || []).push(g.baselinePt); (baseY[r] = baseY[r] || []).push(c.bbox[1] + g.baselinePt); } }
    const want = sku === 'MONOGRAM_RIGHT_T' ? 2 : 1;                      // (the T's flourish is a piece of 12 pt of its own: two charms in the file, which the app folds into the biggest as it reads a master copy)
    if (live.length !== want || all.length !== c.members.length || cuts !== P.cutLinesOf(c).length || p2.counts.text || p2.textDesign || !onePiece) bad.push(`${sku}: ${live.length} charm(s), ${all.length}/${c.members.length} members, ${cuts}/${P.cutLinesOf(c).length} cut lines, text ${p2.counts.text}`);
  }
  ok(!bad.length, 'every per-SKU file reads back as its letter, with its counters, and no text:\n' + bad.join('\n'));
  { const rng = a => `${Math.min(...a).toFixed(3)} .. ${Math.max(...a).toFixed(3)}`;
    for (const [r] of ROWS) ok(baseR[r] && baseR[r].length === 26 && Math.max(...baseY[r]) - Math.min(...baseY[r]) < 0.05, `${r}: baselinePt in every one of 26 files, and the baseline (the letter's bottom in the master + baselinePt) is one line for the alphabet: ${rng(baseY[r])}`);
    console.log('baselinePt ranges (pt):', ROWS.map(([r]) => r + ' ' + rng(baseR[r])).join(' | '), '| baseline y:', ROWS.map(([r]) => r + ' ' + (baseY[r][0]).toFixed(3)).join(' | ')); }
  ok(maxBytes < 30000, `a per-SKU file carries no font (largest ${maxBytes} bytes)`);

  /* ── 3b · the monogram engine takes these letters (charm-nest-monogram.js, written by another hand: only what a monogram needs of them is asked) ── */
  { const M = require('../../charm-nest-monogram.js'), fam = M.familiesOf(SKUS);
    ok(fam.length === 3 && fam.every(f => f.count === 26 && !f.missing.length) && fam.map(f => f.base).sort().join() === 'MONOGRAM_LEFT_{L},MONOGRAM_MID_{L},MONOGRAM_RIGHT_{L}', 'the engine reads the three alphabets of 26 letters from the SKU names: ' + fam.map(f => f.base + ' x' + f.count));
    // a letter as the library gives it: its per-SKU file read back (the biggest charm, whatever else the file draws joined to it), outline and cut lines as rings
    const glyph = async (sku, letter) => {
      const c = A.bySku.get(sku); P.integrateRings(c);
      const p2 = await P.parseSource(new Uint8Array(await P.buildSingleCharm(c, A.parsed)), sku + '.ai'), live = P.groupCharms(p2, { minPt: 6 }).charms.filter(x => x.mergedInto == null);
      const big = live.reduce((a, b) => ((b.bbox[2] - b.bbox[0]) * (b.bbox[3] - b.bbox[1]) > (a.bbox[2] - a.bbox[0]) * (a.bbox[3] - a.bbox[1]) ? b : a));
      for (const x of live) if (x !== big) for (const m of x.members) if (!big.members.includes(m)) big.members.push(m);
      const rings = Geom.flatten(big.outline, 16).filter(r => r.length > 2); for (const m of P.cutLinesOf(big)) for (const r of Geom.flatten(m, 16)) if (r.length > 2) rings.push(r);
      return { letter, rings, sku };
    };
    for (const w of ['APV', 'OSR']) {
      const res = M.build({ left: await glyph('MONOGRAM_LEFT_' + w[0], w[0]), middle: await glyph('MONOGRAM_MID_' + w[1], w[1]), right: await glyph('MONOGRAM_RIGHT_' + w[2], w[2]) });
      ok(res.ok && res.report.connected && res.report.holesKept >= 0.9 && res.report.centerScale > 1.2 && res.placed[1].bboxPt[3] - res.placed[1].bboxPt[1] > 1.3 * (res.placed[0].bboxPt[3] - res.placed[0].bboxPt[1]), `${w}: one connected piece, the counters kept, the middle letter bigger: ` + JSON.stringify(res.report));
    } }

  /* ── 4 · text that cannot be drawn stays text, said ── */
  { const b = await variant.corrupt(bytes, 'TT0'), rd = await readAll(b);
    ok(rd.parsed.textDesign.converted === 52 && rd.live.length === 52 && rd.lab.labels.size === 52, 'a font program that cannot be parsed: its 26 letters stay text, the other 52 are drawn');
    ok(rd.parsed.textDesign.kept.length === 1 && rd.parsed.textDesign.kept[0].font === 'MASTERCIRCLELEFT' && rd.parsed.textDesign.kept[0].n === 26, 'it is said: ' + JSON.stringify(rd.parsed.textDesign.kept));
    ok(P.liveTextNotes(rd.parsed).some(x => x.kind === 'warn' && /live text could not be drawn as outlines: font MASTERCIRCLELEFT/.test(x.text)), 'the report line names the font');
    ok(rd.lab.orphans.length === 27, 'their 26 labels, and the word LEFT that now has no drawn row under it, are orphan labels, as for any label with nothing above it: ' + rd.lab.orphans.length); }
  { const rd = await readAll(await variant.notEmbedded(bytes, 'TT1'));
    ok(rd.parsed.textDesign.converted === 52 && /not embedded/.test(rd.parsed.textDesign.kept[0].why), 'a font that is not embedded: stays text, said'); }
  { const rd = await readAll(await variant.type3(bytes, 'TT2'));
    ok(rd.parsed.textDesign.converted === 52 && /Type 3/.test(rd.parsed.textDesign.kept[0].why), 'a Type 3 font: stays text, said'); }

  /* ── 5 · the same letters from other kinds of font and page ── */
  const same = (rd, label, k) => {
    const o = rd.parsed.segments.filter(s => s.textGlyph), r = A.parsed.segments.filter(s => s.textGlyph); let d = 0;
    ok(o.length === r.length, `${label}: ${o.length} contours`);
    for (let i = 0; i < o.length; i++) for (let j = 0; j < 4; j++) d = Math.max(d, Math.abs(o[i].bbox[j] - r[i].bbox[j] * (k || 1)));
    return d;
  };
  { const rd = await readAll(await variant.cid(bytes, 'TT1'));
    ok(rd.live.length === 78 && rd.lab.labels.size === 78 && same(rd, 'Type0 Identity-H') < 1e-6, 'a composite (CID, Identity-H) font reads the same 78 letters'); }
  { const rd = await readAll(await variant.mac(bytes, 'TT2'));
    ok(rd.live.length === 78 && same(rd, 'Macintosh cmap only') < 1e-6, 'a font with only a Macintosh (1,0) cmap reads the same 78 letters'); }
  { const rd = await readAll(await variant.edit(bytes, t => 'q 0.75 0 0 0.75 100 50 cm\n' + t + '\nQ'));
    ok(rd.live.length === 78, 'a page drawn through a scaling matrix still has its 78 letters');
    const o = rd.parsed.segments.filter(s => s.textGlyph), r = A.parsed.segments.filter(s => s.textGlyph); let d = 0;
    for (let i = 0; i < o.length; i++) { d = Math.max(d, Math.abs(o[i].bbox[0] - (r[i].bbox[0] * 0.75 + 100)), Math.abs(o[i].bbox[3] - (r[i].bbox[3] * 0.75 + 50))); }
    ok(d < 1e-6, 'the outlines follow the matrix (three quarters size, moved): off by ' + d); }
  // a bare CFF (the Myriad Type1C subset of the file, as a design font): the letters A and R set big, each with its label under it
  { const rd = await readAll(await variant.edit(bytes, t => t + '\n0 g\nBT /T1_0 1 Tf 40 0 0 40 600 100 Tm (A)Tj ET\nBT /T1_0 1 Tf 3.3 0 0 3.3 604 95 Tm (CFF_A)Tj ET\nBT /T1_0 1 Tf 40 0 0 40 700 100 Tm (R)Tj ET\nBT /T1_0 1 Tf 3.3 0 0 3.3 711 95 Tm (CFF_R)Tj ET\n'));
    const a = rd.bySku.get('CFF_A'), r = rd.bySku.get('CFF_R');
    ok(a && r && a.outline.glyphChar === 'A' && r.outline.glyphChar === 'R', 'a bare CFF (Type1C) font: its A and R are drawn and labelled');
    ok(P.cutLinesOf(a).length === 1 && P.cutLinesOf(r).length === 1 && (a.bbox[3] - a.bbox[1]) > 24 && (a.bbox[3] - a.bbox[1]) < 30, 'the CFF A has its triangle, the R its bowl, and the A is a 40 pt cap A (' + (a.bbox[3] - a.bbox[1]).toFixed(1) + ' pt)'); }

  /* ── 6 · text that is not the design is left alone ── */
  { const rd = await readAll(await variant.edit(bytes, t => t.replace('/CS0 cs 0 0 0  scn', '/CS0 cs 1 0 0  scn')));
    ok(!rd.parsed.textDesign && rd.live.length === 0 && !rd.parsed.segments.some(s => s.textGlyph), 'red text is a note, not the design: nothing is drawn'); }
  { const rd = await readAll(await variant.edit(bytes, t => t.replace('/TT1 1 Tf', '/TT1 0.3 Tf')));
    ok(rd.parsed.textDesign.converted === 52 && !rd.bySku.has('MONOGRAM_MID_A'), 'the Middle row set at 16 pt is not the design (the 20 pt rule): the other two rows are'); }
  { const rd = await readAll(await variant.edit(bytes, t => t.replace('(A)Tj\n0.696 0 Td\n(B)Tj', '(AB)Tj\n0.696 0 Td')));
    ok(rd.parsed.textDesign.converted === 76 && !rd.bySku.has('MONOGRAM_LEFT_A') && !rd.bySku.has('MONOGRAM_LEFT_B') && rd.bySku.has('MONOGRAM_LEFT_C'), 'a text object of two letters is a word: it stays text'); }
  { const rd = await readAll(await variant.edit(bytes, t => t.replace('-261.682 -44.411 Td', '-261.682 -444.411 Td')));
    ok(rd.parsed.textDesign.converted === 52 && rd.lab.labels.size === 52 && rd.lab.orphans.length === 27, 'a letter with no SKU label under it is not the design: the Right row stays text (its 26 labels and the word RIGHT are orphan labels)'); }
  // a master with real paths: a vector charm and its label beside the letters, and a closed outline around the first A (the A is sample text on it)
  { const rd = await readAll(await variant.edit(bytes, t => t + '\n0 g 0 G 0.5 w\n700 40 m 740 40 l 740 80 l 700 80 l h S\nBT /T1_0 1 Tf 3.3 0 0 3.3 714 35 Tm (VEC_A)Tj ET\n'));
    ok(rd.live.length === 79 && rd.lab.labels.size === 79 && rd.bySku.has('VEC_A') && rd.parsed.textDesign.converted === 78, 'a master with a real path: its charm is read as before, and the 78 letters are drawn beside it');
    { const v = await P.parseSource(new Uint8Array(await P.buildSingleCharm(rd.bySku.get('VEC_A'), rd.parsed)), 'v.ai'); ok(!v.glyph && !v.doc.getSubject(), 'an ordinary charm writes no glyph Subject'); }
    const on = await readAll(await variant.edit(bytes, t => t + '\n0 g 0 G 0.5 w\n25 444.6 m 48 444.6 l 48 484 l 25 484 l h S\n'));
    ok(on.parsed.textDesign.converted === 77 && !on.parsed.segments.some(s => s.textGlyph && s.bbox[0] < 48 && s.bbox[2] > 25 && s.bbox[1] > 440 && s.bbox[3] < 490) && on.bySku.get('MONOGRAM_LEFT_A').outline.kind === 'path' && !on.bySku.get('MONOGRAM_LEFT_A').outline.textGlyph,
      'a letter standing ON a vector outline is sample text: it stays text, and the label under the outline belongs to the outline'); }

  /* ── 7 · the three ways of indexing ── */
  const { start, REPO_TEST } = require('./bridge-server.cjs'), { main } = require('../../scripts/index-master.cjs');
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cn-mono-')), file = path.join(tmp, 'SKUs_MONOGRAM_2024-1015.ai'); fs.copyFileSync(FILE, file);
  const viaScript = {}, viaServer = {};
  { const srv = await start({ receipts: [] }), { st, sorterOrigin } = srv;
    st.repoStrict = true; process.env.CHARM_REPO_PASSWORD = REPO_TEST.password;
    const lines = []; const log = l => lines.push(String(l));
    const dry = await main(['node', 'x', file, '--dry'], log);
    ok(dry.dry && dry.charms === 78 && dry.labelled === 78 && dry.written === 0 && st.list('Charm_Master_Index').length === 0 && st.blobs.size === 0, 'local script, dry run: 78 charms, 78 labelled, nothing written');
    ok(lines.some(l => /78 letter\(s\) drawn as live text/.test(l)) && lines.some(l => /“LEFT”, “MIDDLE”, “RIGHT”/.test(l)), 'the dry run says that the letters were read from live text and what the headers are');
    const rep = await main(['node', 'x', file, '--origin', sorterOrigin, '--upload-master'], log);
    ok(rep.written === 78 && rep.unlabelled.length === 0 && rep.orphans.length === 0, 'local script, real run: 78 SKUs written, none unlabelled');
    const ix = st.list('Charm_Master_Index');
    ok(ix.length === 78 && SKUS.every(s => ix.some(e => e.sku === s)), 'the library holds MONOGRAM_LEFT_A ... MONOGRAM_RIGHT_Z');
    const fileOf = e => e.aiPath; ok(ix.every(e => fileOf(e) && st.blobs.has(fileOf(e)) && e.widthPt > 8 && e.heightPt > 25 && e.areaPt2 > 20 && e.charmHash && e.members >= 1), 'each entry has its design file, a size, an area and a hash');
    ok(ix.every(e => e.holes === P.cutLinesOf(A.bySku.get(e.sku)).length), 'each entry has the holes of its letter');
    for (const e of ix) viaScript[e.sku] = { hash: e.charmHash, holes: e.holes, w: e.widthPt, h: e.heightPt };
    { const b1 = st.blobs.get(fileOf(ix[0])), p2 = await P.parseSource(new Uint8Array(b1.buf), 'one.ai'); ok(p2.counts.text === 0 && P.groupCharms(p2, { minPt: 6 }).charms.length >= 1, 'a stored design file is a plain drawing'); }
    const before = { blobs: st.blobs.size, calls: st.calls.length };
    const again = await main(['node', 'x', file, '--origin', sorterOrigin], log);
    ok(again.written === 0 && again.held === 78, 'local script, same file again: 78 held, nothing written: ' + JSON.stringify({ w: again.written, h: again.held }));
    ok(st.blobs.size === before.blobs, 'and no file was uploaded');
    srv.close(); }
  { const srv = await start({ receipts: [] }), id = 'job-mono';
    srv.st.blobs.set('charmnest/uploads/' + id + '.ai', { buf: Buffer.from(bytes), generation: 1, meta: { contentType: 'application/pdf', metadata: {} } });
    srv.st.put('Charm_Nest_Jobs', id, { id, kind: 'master', status: 'pending', path: 'charmnest/uploads/' + id + '.ai', name: 'SKUs_MONOGRAM_2024-1015.ai', opts: {} });
    await srv.st.handlers['charmMaster-background'].handler({ body: JSON.stringify({ id }) });
    const job = srv.st.doc('Charm_Nest_Jobs', id), ix = srv.st.list('Charm_Master_Index');
    ok(job.status === 'done' && job.result.labelled === 78 && job.result.charms === 78 && job.result.unlabelled.length === 0 && job.result.orphans.length === 0, 'server route: 78 labelled, none unlabelled: ' + JSON.stringify({ s: job.status, e: job.error }));
    ok(ix.length === 78 && SKUS.every(s => ix.some(e => e.sku === s)), 'server route: the library holds the same 78 SKUs');
    ok((job.result.liveText || []).some(t => /78 letter\(s\)/.test(t)), 'server route: the job result says the letters were read from live text');
    for (const e of ix) { viaServer[e.sku] = { hash: e.charmHash, holes: e.holes, w: e.widthPt, h: e.heightPt }; }
    srv.close(); }
  ok(SKUS.every(s => viaScript[s] && viaServer[s] && viaScript[s].hash === viaServer[s].hash && viaScript[s].holes === viaServer[s].holes && Math.abs(viaScript[s].w - viaServer[s].w) < 1e-9), 'the script and the server route wrote the same hash, holes and size for all 78');

  /* ── 8 · the Master tab (headless Chromium over the same stand-in library) ── */
  let shots = null;
  if (!process.env.NO_BROWSER) {
    const br = await R.browser({});
    try {
      const r = await br.upload(bytes, 'SKUs_MONOGRAM_2024-1015.ai');
      ok(r.state === 'done' && !r.error && r.written === 78 && r.entries.length === 78, `Master tab: 78 SKUs written (${r.state} ${r.error || ''} ${r.written})`);
      ok(r.unlabelled.length === 0 && r.orphans.length === 0 && r.blocked.length === 0, 'Master tab: none unlabelled, no orphan label, none blocked');
      ok(r.log.some(l => /78 charm outline\(s\)/.test(l)) && r.log.some(l => /78 letter\(s\) drawn as live text/.test(l)) && r.log.some(l => /78 labelled \(78 SKU line\(s\)\) · 0 unlabelled · 0 orphan label\(s\)/.test(l)) && r.log.some(l => /“LEFT”, “MIDDLE”, “RIGHT”/.test(l)), 'Master tab: the report says what happened: ' + r.log.join(' / '));
      ok(!br.errors.length, 'Master tab: no page error: ' + br.errors.join(' | '));
      const inLib = await br.page.evaluate(() => [...B.master.entries.keys()].sort());
      ok(inLib.join('|') === SKUS.slice().sort().join('|'), 'Master tab: the library shows the 78 SKUs');
      for (const s of SKUS) { const e = br.index(s); ok(e && e.aiPath && br.blob(e.aiPath) && e.holes === viaScript[s].holes && e.widthPt > 8 && e.heightPt > 25, `Master tab: ${s} has its file, its holes (${e && e.holes}) and a size`); }
      { let withGlyph = 0; for (const s of SKUS) { const p2 = await P.parseSource(new Uint8Array(br.blob(br.index(s).aiPath).buf), s + '.ai'); if (p2.glyph && p2.doc.getSubject() === JSON.stringify({ glyph: p2.glyph }) && p2.glyph.emPt > 52 && p2.glyph.font) withGlyph++; }
        ok(withGlyph === 78, `Master tab: all 78 stored design files carry the glyph baseline (${withGlyph})`); }
      ok(SKUS.every(s => { const e = br.index(s); return Math.abs(e.widthPt - viaScript[s].w) < 0.6 && Math.abs(e.heightPt - viaScript[s].h) < 0.6; }), 'Master tab: the sizes agree with the script to the pixel of its silhouette');
      const again = await br.upload(bytes, 'SKUs_MONOGRAM_2024-1015.ai');
      ok(again.state === 'done' && again.written === 0 && again.held === 78, `Master tab, same file again: 78 held, nothing written (${again.written}/${again.held})`);
      if (process.env.MONO_SHOTS) shots = SKUS.map(s => ({ sku: s, bytes: br.blob(br.index(s).aiPath).buf }));
    } finally { await br.close(); }
  }

  /* ── 9 · the contact sheet (optional) ── */
  if (shots && process.env.MONO_SHOTS) {
    const { chromium } = require(path.join(process.env.PW_DIR || path.join(__dirname, '../../node_modules'), 'playwright-core'));
    const cell = 92, parts = [];
    for (const { sku, bytes: bb } of shots) {
      const m = /MONOGRAM_(LEFT|MID|RIGHT)_([A-Z])/.exec(sku), ri = ['LEFT', 'MID', 'RIGHT'].indexOf(m[1]), ci = m[2].charCodeAt(0) - 65;
      const p2 = await P.parseSource(new Uint8Array(bb), sku + '.ai'), g2 = P.groupCharms(p2, { minPt: 6 }), live = g2.charms.filter(c => c.mergedInto == null);
      const bx = live.reduce((a, c) => [Math.min(a[0], c.bbox[0]), Math.min(a[1], c.bbox[1]), Math.max(a[2], c.bbox[2]), Math.max(a[3], c.bbox[3])], [1e9, 1e9, -1e9, -1e9]);
      const w = bx[2] - bx[0], h = bx[3] - bx[1], s = Math.min((cell - 14) / w, (cell - 26) / h), ox = ci * cell + cell / 2 - (bx[0] + w / 2) * s, oy = ri * (cell + 14) + 4 + (cell - 20) / 2 + (bx[1] + h / 2) * s;
      const d = live.flatMap(c => c.members).map(mm => Geom.svgPathOf(mm)).join(' ');
      parts.push(`<g transform="translate(${ox} ${oy}) scale(${s} ${-s})"><path d="${d}" fill="#d9d4c8" stroke="#000" stroke-width="${1.1 / s}" fill-rule="evenodd"/></g><text x="${ci * cell + cell / 2}" y="${ri * (cell + 14) + cell - 4}" font-size="10" text-anchor="middle" font-family="sans-serif" fill="#333">${m[1]}_${m[2]}</text>`);
    }
    const W = 26 * cell, H = 3 * (cell + 14), svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}"><rect width="100%" height="100%" fill="#fff"/>${parts.join('')}</svg>`;
    fs.mkdirSync(process.env.MONO_SHOTS, { recursive: true });
    const b2 = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
    const pg = await b2.newPage({ viewport: { width: W, height: H } }); await pg.setContent('<body style="margin:0">' + svg + '</body>'); await pg.screenshot({ path: path.join(process.env.MONO_SHOTS, 'alphabets.png') }); await b2.close();
  }
  console.log(`monogram-file: ${n} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
