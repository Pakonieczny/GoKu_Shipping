#!/usr/bin/env node
/* Which designs still need a person to say which way they face (PAIRMIRROR, 9 Oct 2026).
 *
 * A pair of earrings is a Left and a Right that are mirror images (Paul, 9 Oct 18:47). A design that looks the same in a mirror needs nothing. A design that does not
 * (a mitten, an animal in profile) has a drawing that faces one way, and nothing in the shape says which way: a person says so in the Master tab (the "faces" box on the
 * card). This lists the designs that do not look the same in a mirror and have no word yet, grouped by artwork, earrings first, strongest first.
 *
 *   node scripts/facing-report.cjs --index dump.json --out report.json
 *       dump.json: the masterList answer saved to a file ({ entries: [...] }), read after the master was indexed with the `sym` field (scripts/index-master.cjs).
 *   node scripts/facing-report.cjs --features f.jsonl --out report.json [--files files.json]
 *       f.jsonl: one line per design file { sku, skus, cutD90, inkFrac, inkE90 } (the distance of the cut line and of the engraving from their own mirror image,
 *       as shares of the drawing's size; charm-nest-pair.js symmetryOf's measure) from a geometry pass over the master files. files.json: optional, { skus, widthPt, heightPt } per file.
 * Reads files only. Writes nothing to the cloud, never calls the app.                                                                                                      */
'use strict';
const fs = require('node:fs'), path = require('node:path');
const Pair = require('../charm-nest-pair.js');
const MM = 25.4 / 72, CUT_DIR = 0.065, ART_DIR = 0.14, CUT_OK = 0.04, ART_OK = 0.10;   // (the same bars as charm-nest-pair.js symmetryOf)

const levelOf = (cut, art) => (cut >= CUT_DIR || (art != null && art >= ART_DIR) ? 'directional' : cut < CUT_OK && (art == null || art < ART_OK) ? 'symmetric' : 'slight');
/** The artwork a SKU belongs to: the huggie and hoop versions of one animal read as one family (HUGGIE HOOPS- GIRAFFE, GIRAFFE (HUGGIE), GIRAFFE_2069). */
function familyOf(skus) {
  const clean = s => String(s).toUpperCase().replace(/\(\s*HUGGIE[^)]*\)?/g, ' ').replace(/HUGGIE\s*HOOPS?\s*-?/g, ' ').replace(/[-_ ]*(DISC|NECKLACE|EARRINGS?|STUD|CHARM)\b/g, ' ').replace(/[_-]?\d{3,}/g, ' ').replace(/[^A-Z ]+/g, ' ').replace(/\s+/g, ' ').trim();
  const names = skus.map(clean).filter(Boolean).sort((a, b) => a.length - b.length);
  return names[0] || String(skus[0] || '').toUpperCase();
}
/** earring (a huggie, hoop, stud or earring SKU), necklace (a disc or necklace SKU with none of those), or either. */
function useOf(skus) {
  const t = skus.join(' ').toUpperCase();
  if (/HUGGIE|HOOP|EARRING|STUD/.test(t)) return 'earring';
  if (/DISC|NECKLACE|PENDANT/.test(t)) return 'necklace';
  return 'either';
}

function designsFromFeatures(rows, files) {
  const size = new Map(); for (const f of files || []) for (const s of f.skus || []) size.set(s, [f.widthPt, f.heightPt]);
  return rows.map(r => {
    const art = r.inkFrac > 0.01 && r.inkE90 != null ? r.inkE90 : null, level = levelOf(r.cutD90, art), wh = size.get((r.skus || [r.sku])[0]);
    return { skus: r.skus || [r.sku], level, cut: r.cutD90, art, mm: wh ? [+(wh[0] * MM).toFixed(1), +(wh[1] * MM).toFixed(1)] : null, facing: r.facing || null };
  });
}
function designsFromIndex(entries) {
  const byFile = new Map();
  for (const e of entries || []) { const key = e.aiPath || e.sku; if (!byFile.has(key)) byFile.set(key, { skus: [], e }); byFile.get(key).skus.push(e.sku); }
  return [...byFile.values()].map(({ skus, e }) => ({ skus, level: e.sym || null, cut: null, art: null, mm: e.widthPt ? [+(e.widthPt * MM).toFixed(1), +(e.heightPt * MM).toFixed(1)] : null, facing: e.facing || null, mismatched: !!(e.pair && e.pair.mismatched) }));
}

/** designs: [{ skus, level, cut, art, mm, facing }] -> the report. */
function report(designs, meta) {
  const out = [], lettering = [], slight = [], counts = { files: designs.length, skus: 0, directional: 0, slight: 0, symmetric: 0, notMeasured: 0, needAWord: 0, wordSet: 0, readsOneWayByName: 0 }   // (directional = does not look the same in a mirror; needAWord = directional, no word yet, not lettering);
  for (const d of designs) {
    counts.skus += d.skus.length;
    if (d.level === 'directional') counts.directional++; else if (d.level === 'slight') counts.slight++; else if (d.level === 'symmetric') counts.symmetric++; else counts.notMeasured++;
    if (d.facing === 'L' || d.facing === 'R' || d.facing === 'X') { counts.wordSet++; continue; }
    if (d.level === 'slight') slight.push({ skus: d.skus, cut: d.cut, art: d.art });   // (nearly the same in a mirror: a hoop off to one side, a wobble; the Right is still cut turned over, no word is asked)
    if (d.level !== 'directional') continue;
    const reads = d.skus.some(s => Pair.readsOneWay({ sku: s }));
    if (reads) { counts.readsOneWayByName++; lettering.push({ skus: d.skus, mm: d.mm }); continue; }   // (a letter, number or script by name: the Right is mirrored like any other earring, and which way a letter faces is of no interest, so it is listed apart)
    const score = Math.max(d.cut != null ? d.cut / CUT_DIR : 0, d.art != null ? d.art / ART_DIR : 0);
    out.push({ skus: d.skus, family: familyOf(d.skus), use: useOf(d.skus), why: [d.cut != null && d.cut >= CUT_DIR ? 'shape' : null, d.art != null && d.art >= ART_DIR ? 'engraving' : null].filter(Boolean), cut: d.cut, art: d.art, score: +score.toFixed(2),
      readsOneWay: reads, mismatched: !!d.mismatched, mm: d.mm, guess: null });
  }
  counts.needAWord = out.length;
  const rank = d => (d.use === 'earring' ? 0 : d.use === 'either' ? 1 : 2);
  out.sort((a, b) => rank(a) - rank(b) || b.score - a.score || (a.family < b.family ? -1 : 1));
  out.forEach((d, i) => { d.id = i + 1; });
  const fam = new Map(); for (const d of out) { if (!fam.has(d.family)) fam.set(d.family, []); fam.get(d.family).push(d.id); }
  const families = [...fam.entries()].map(([family, ids]) => ({ family, designs: ids })).sort((a, b) => Math.min(...a.designs) - Math.min(...b.designs));
  const bySku = {}; for (const d of out) for (const s of d.skus) bySku[s] = d.id;
  return Object.assign({ counts, designs: out, families, bySku, readsOneWayByName: lettering, slight }, meta || {});
}

if (require.main === module) {
  const a = process.argv.slice(2), arg = k => { const i = a.indexOf(k); return i >= 0 ? a[i + 1] : null; };
  const readJsonl = f => fs.readFileSync(f, 'utf8').split('\n').filter(l => l.trim()).map(l => JSON.parse(l));
  let designs;
  if (arg('--index')) designs = designsFromIndex(JSON.parse(fs.readFileSync(arg('--index'), 'utf8')).entries);
  else if (arg('--features')) designs = designsFromFeatures(arg('--features').split(',').flatMap(readJsonl), arg('--files') ? JSON.parse(fs.readFileSync(arg('--files'), 'utf8')) : []);
  else { console.error('usage: node scripts/facing-report.cjs (--index dump.json | --features f.jsonl[,g.jsonl] [--files files.json]) --out report.json'); process.exit(2); }
  const rep = report(designs, { generated: new Date().toISOString(), source: arg('--index') ? 'the master index (field sym)' : 'a geometry pass over the master files' });
  const o = arg('--out'); if (o) fs.writeFileSync(path.resolve(o), JSON.stringify(rep, null, 1)); console.log(JSON.stringify(rep.counts));
}
module.exports = { report, levelOf, familyOf, useOf, designsFromFeatures, designsFromIndex };
