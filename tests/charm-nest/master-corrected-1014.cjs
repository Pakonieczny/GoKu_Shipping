// Paul's master "SKUs_CORRECTED_2024-1014.ai" (15 Oct 2026), as he has it. Dropped in the Master tab it came back with these errors, and these are what this test holds fixed:
//   · "13 line(s) with no charm above": the 12 zodiac NAMES (ARIES … PISCES, cyan) written ABOVE each zodiac charm, which has its own SKU (Zodiac_Huggie-N) under it, are titles, not SKUs;
//   · RUNE_HUGGIE_CHARM-10 (a bar 5.7 pt wide, under the 6 pt a charm outline needed, with its ring on top) was "no charm above" and its charm "with no SKU under it" (#131):
//     the bar is the charm's outline now, its ring is welded into it, and the label stands under it;
//   · "1 design(s) held as ONE body of a pair" (ACORN_4721): a design the library holds as one body while the sheet draws two is built again from both bodies, with no tick;
//   · the 26 keys HEARTKEY_NECKLACE_A…Z lost the letter set inside each one (live text, 16 pt, on the LABELS layer): each letter is welded into its key's cut line, as an
//     OUTLINE (one hairline stroke, no fill), so the heart window is cut round the letter and a counter the letter closes off stays a hole of its own;
//   · and nothing else moves: a letter is welded only into the key whose SKU ends in that very letter.
// Offline: the real file, the stand-in site of bridge-server.cjs, nothing is sent anywhere.
//   node tests/charm-nest/master-corrected-1014.cjs
'use strict';
const fs = require('fs'), os = require('os'), path = require('path'), assert = require('assert');
const { CharmNestPDF: P, Geom: G, PDFLib } = require('../../netlify/functions/_charmNestPdf.js');
const V = require('../../charm-nest-vector.js'), Pair = require('../../charm-nest-pair.js');
const FILE = path.join(__dirname, 'fixtures', 'SKUs_CORRECTED_2024-1014.ai');
const MM = 25.4 / 72;
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const LETTERS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('');
const read = async () => {
  const buf = fs.readFileSync(FILE), parsed = await P.parseSource(new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength), 'SKUs_CORRECTED_2024-1014.ai');
  const g = P.groupCharms(parsed, { minPt: 6 }), lab = P.labelCharms(parsed, g.charms, { pattern: P.SKU_PATTERN_DEFAULT, gapPt: 6.4 / MM, widen: 0.25 });
  const bySku = new Map([...lab.labels].map(([i, l]) => [l.sku, g.charms.find(c => c.index === i)]));
  return { parsed, g, lab, bySku, live: g.charms.filter(c => c.mergedInto == null) };
};
const region = c => { const t = V.boolean(c.outline.subpaths.map(sp => V.flatten(sp).points), [], 'union', 'evenodd'), a = p => Math.abs(p.reduce((v, q, i) => v + q[0] * p[(i + 1) % p.length][1] - q[1] * p[(i + 1) % p.length][0], 0)) / 2; let area = 0; for (const p of t) area += (p.hole ? -1 : 1) * a(p.points); return { area, lumps: t.filter(p => !p.hole).length, windows: t.filter(p => p.hole).length }; };
const dec = s => PDFLib.decodePDFRawStream(s).decode();
const contentOf = async bytes => { const doc = await PDFLib.PDFDocument.load(bytes, { ignoreEncryption: true, updateMetadata: false }), { PDFName, PDFArray } = PDFLib; const c = doc.context.lookup(doc.getPage(0).node.get(PDFName.of('Contents'))); const parts = c instanceof PDFArray ? c.asArray().map(r => dec(doc.context.lookup(r))) : [dec(c)]; return Buffer.concat(parts.map(p => Buffer.from(p))).toString('latin1'); };

(async () => {
  const W = await read(), { lab, bySku, g, parsed } = W;

  // 1 · the zodiac names are titles, nothing is left "with no charm above"
  const SIGNS = ['ARIES', 'TAURUS', 'GEMINI', 'CANCER', 'LEO', 'VIRGO', 'LIBRA', 'SCORPIO', 'SAGITTARIUS', 'CAPRICORN', 'AQUARIUS', 'PISCES'];
  ok(lab.orphans.length === 0, 'no line is left with no charm above it: ' + lab.orphans.map(o => o.sku).join(', '));
  ok(lab.titles.length === 12 && SIGNS.every(s => lab.titles.some(t => t.str.toUpperCase() === s && /^ZODIAC_HUGGIE-\d+$/.test(t.over))), 'the 12 zodiac names are titles, each over its own Zodiac_Huggie charm: ' + lab.titles.map(t => t.str + '>' + t.over).join(' '));
  ok(lab.titles.every(t => bySku.has(t.over)) && !SIGNS.some(s => bySku.has(s)), 'and none of them became a SKU: the charms keep their own');
  { // a name that stands over no labelled charm is still an orphan: the same sheet with the twelve zodiac charms taken out of the grouping
    const zodiac = new Set(SIGNS.map((s, i) => lab.titles.find(t => t.str.toUpperCase() === s).over).map(sku => bySku.get(sku).index));
    const bare = await read(), cut = P.labelCharms(bare.parsed, bare.g.charms.filter(c => !zodiac.has(c.index)), { pattern: P.SKU_PATTERN_DEFAULT, gapPt: 6.4 / MM, widen: 0.25 });
    ok(cut.titles.length === 0 && SIGNS.every(s => cut.orphans.some(o => o.sku === s)), 'control: with no labelled charm under them the 12 names are orphans, as before: ' + cut.titles.length + ' titles, ' + cut.orphans.length + ' orphans');
  }

  // 2 · RUNE_HUGGIE_CHARM-10: a thin body with its ring
  const rune = bySku.get('RUNE_HUGGIE_CHARM-10'); ok(!!rune, 'RUNE_HUGGIE_CHARM-10 has its charm');
  const rb = rune.outline.bbox; ok(rb[3] - rb[1] > 20 && rb[2] - rb[0] < 6, 'the charm\'s outline is the bar (5.7 pt wide, 22 pt tall), not its 9 pt ring: ' + rb.map(v => v.toFixed(1)));
  ok(!lab.unlabelled.includes(rune.index) && lab.unlabelled.every(i => [89, 128].includes(i)), 'no charm is left with no SKU but the two bodies that are the other half of a pair: ' + lab.unlabelled);
  { const r = P.integrateRings(rune); ok(r.welded === 1 && r.left.length === 0, 'its ring is welded into the bar: ' + JSON.stringify(r)); const b = rune.outline.bbox; ok(b[3] > 1079 && b[1] < 1048.5, 'one outline from the bar\'s foot to the ring\'s top: ' + b.map(v => v.toFixed(1))); }
  ok(P.READER_VERSION >= 3 && P.LETTERS_READER_VERSION === 3, 'the reader version moved with the drawing, so a design an older reader wrote is known as older');

  // 3 · the keys: 26 letters, each welded into its own key
  ok(lab.inlays.length === 26 && LETTERS.every(L => lab.inlays.some(x => x.sku === 'HEARTKEY_NECKLACE_' + L && x.char === L)), 'each key has its own letter picked: ' + lab.inlays.map(x => x.char).join(''));
  ok(lab.inlays.every(x => /^HEARTKEY_NECKLACE_[A-Z]$/.test(x.sku)), 'and no other design took one (the 16 Bar_Necklace, Initial_Disc … are untouched)');
  ok(parsed.textDesign && parsed.textDesign.converted === 0, 'no letter of this sheet became a charm of its own');
  const keys = LETTERS.map(L => bySku.get('HEARTKEY_NECKLACE_' + L)); ok(keys.every(Boolean), 'all 26 keys are charms');
  const before = keys.map(region); ok(before.every(b => b.windows === 3 && b.lumps === 1), 'a key as drawn: one piece and three windows (the ring, the heart, the key bit)');
  const built = [];
  for (const [i, c] of keys.entries()) {
    const L = LETTERS[i], r = P.integrateRings(c), a = region(c), o = c.outline;
    ok(r.letters === 1 && r.left.length === 0 && r.islands.length === 0, `${L}: the letter is welded, none left over, none standing clear of the metal: ` + JSON.stringify(r));
    ok(o.stroke === true && o.fill === false && o.paintOp === 'S' && o.synthetic === true && c.members.length === 1, `${L}: the result is ONE stroked outline with no fill`);
    ok(a.lumps === 1 && a.area > before[i].area + 5 && a.windows > before[i].windows, `${L}: still one piece, with more metal (${before[i].area.toFixed(1)} → ${a.area.toFixed(1)} pt²) and more windows (${before[i].windows} → ${a.windows})`);
    ok(P.integrateRings(c).letters === 0 && region(c).area === a.area, `${L}: welding twice changes nothing`);
    const bytes = new Uint8Array(await P.buildSingleCharm(c, parsed)), txt = await contentOf(bytes), paints = (txt.match(/(^|\s)(S|s|f\*?|F|B\*?|b\*?)(?=\s|$)/g) || []).map(s => s.trim());
    ok(paints.length === 1 && paints[0] === 'S', `${L}: its file paints ONE stroke and no fill: ` + paints.join(','));
    built.push(bytes);
  }
  { // the file read back is the same key with its letter: one charm, the same windows, the same metal
    const back = await P.parseSource(built[1], 'B.ai'), bg = P.groupCharms(back, { minPt: 6 }), live = bg.charms.filter(c => c.mergedInto == null);
    ok(live.length === 1 && region(live[0]).windows === region(keys[1]).windows && Math.abs(region(live[0]).area - region(keys[1]).area) < 1, 'the per-SKU file of B reads back as the same key with its B');
    ok(!(back.segments.concat(back.nested)).some(s => s.kind === 'text'), 'and carries no text: the letter is outline only');
  }
  { // a letter belongs to the key whose SKU ends in it, and only to that
    const fake = Object.assign({}, parsed, { textDesign: Object.assign({}, parsed.textDesign, { inlays: parsed.textDesign.inlays.map(x => Object.assign({}, x, { char: 'Z' })) }) });
    const lz = P.labelCharms(fake, (await read()).g.charms, { pattern: P.SKU_PATTERN_DEFAULT, gapPt: 6.4 / MM, widen: 0.25 });
    ok(lz.inlays.length === 1 && lz.inlays[0].sku === 'HEARTKEY_NECKLACE_Z', 'every glyph read as "Z": only the Z key takes one (its own), the other 25 stand where they are: ' + lz.inlays.map(x => x.sku));
    const none = P.labelCharms(Object.assign({}, parsed, { textDesign: Object.assign({}, parsed.textDesign, { inlays: [] }) }), (await read()).g.charms, { pattern: P.SKU_PATTERN_DEFAULT, gapPt: 6.4 / MM, widen: 0.25 });
    ok(none.inlays.length === 0, 'a master with no such letter welds nothing');
  }

  // 4 · what the library holds wrong is built again, without a tick (charm-nest-pair.js staleHeld)
  { const w = await read(), labelsOf = c => { const l = w.lab.labels.get(c.index); return l ? [l].concat(l.extra || []).map(x => ({ sku: x.sku, size: x.size, bbox: x.bbox })) : []; };
    const idx = sku => w.bySku.get(sku).index, held = new Map([['HEARTKEY_NECKLACE_B', { sku: 'HEARTKEY_NECKLACE_B', readerV: 2 }], ['HEARTKEY_NECKLACE_C', { sku: 'HEARTKEY_NECKLACE_C', readerV: P.READER_VERSION }], ['ACORN_4721', { sku: 'ACORN_4721' }], ['SPIDER - WEB_1', { sku: 'SPIDER - WEB_1', pair: { v: 1, bodies: 2 } }]]);
    const st = Pair.staleHeld(w.live, labelsOf, held, P.LETTERS_READER_VERSION);
    ok(st.has(idx('HEARTKEY_NECKLACE_B')) && st.get(idx('HEARTKEY_NECKLACE_B')).kind === 'letter', 'a key the library holds without its letter (an older reader) is built again');
    ok(!st.has(idx('HEARTKEY_NECKLACE_C')), 'a key written by this reader is held');
    ok(st.has(idx('ACORN_4721')) && st.get(idx('ACORN_4721')).kind === 'pair', 'ACORN_4721, held as one body while the sheet draws two, is built again');
    ok(!st.has(idx('SPIDER - WEB_1')) && st.size === 2, 'SPIDER - WEB_1, held as the pair it is, is held; nothing else on the sheet is touched: ' + [...st.keys()]);
    ok(Pair.staleHeld(w.live, labelsOf, new Map(), 3).size === 0 && Pair.staleHeld(w.live, labelsOf, null, 3).size === 0, 'a library that holds nothing has nothing wrong');
  }

  // 5 · the local indexer, end to end against the stand-in site: the report, the held rule, the second run
  const { start, REPO_TEST } = require('./bridge-server.cjs'), { main } = require('../../scripts/index-master.cjs');
  const srv = await start({ receipts: [] }), { st, sorterOrigin } = srv; st.repoStrict = true; process.env.CHARM_REPO_PASSWORD = REPO_TEST.password;
  const lines = []; const log = l => lines.push(String(l));
  try {
    const dry = await main(['node', 'x', FILE, '--dry', '--work-dir', fs.mkdtempSync(path.join(os.tmpdir(), 'cn-1014-'))], log);
    ok(dry.labelled === 128 && dry.orphans.length === 0 && dry.titles.length === 12 && dry.inlays.length === 26 && dry.islands.length === 0 && dry.blocked.length === 0, 'the dry run reports 128 labelled, no orphan, 12 titles, 26 letters welded, none blocked: ' + JSON.stringify({ l: dry.labelled, o: dry.orphans.length, t: dry.titles.length, i: dry.inlays.length, b: dry.blocked.length }));
    ok(lines.some(l => /26 letter\(s\) set inside their design are welded into its cut line, as outlines/.test(l)) && lines.some(l => /12 name\(s\) written above a charm that has its own SKU under it are titles/.test(l)), 'and says so');
    const work = fs.mkdtempSync(path.join(os.tmpdir(), 'cn-1014-w-'));
    const first = await main(['node', 'x', FILE, '--origin', sorterOrigin, '--work-dir', work], log);
    ok(first.written === 191 && first.blocked.length === 0, 'the first run writes all 191 SKU lines, none blocked: ' + first.written);
    const ix = sku => st.doc('Charm_Master_Index', sku);
    ok(LETTERS.every(L => ix('HEARTKEY_NECKLACE_' + L) && ix('HEARTKEY_NECKLACE_' + L).readerV === P.READER_VERSION) && ix('RUNE_HUGGIE_CHARM-10') && ix('ACORN_4721').pair.bodies === 2, 'the library holds the 26 keys at the new reader, RUNE_HUGGIE_CHARM-10, and ACORN_4721 as a pair of two bodies');
    const again = await main(['node', 'x', FILE, '--origin', sorterOrigin, '--work-dir', work], log);
    ok(again.written === 0 && again.held === 128, 'the same sheet again writes nothing: all 128 charms are held: ' + JSON.stringify({ w: again.written, h: again.held }));
    // the library as an earlier reader left it: B without its letter, ACORN_4721 as one body
    ix('HEARTKEY_NECKLACE_B').readerV = 2; delete ix('ACORN_4721').pair;
    const fixed = await main(['node', 'x', FILE, '--origin', sorterOrigin, '--work-dir', work], log);
    ok(fixed.written === 2 && fixed.held === 126, 'the two designs the library holds wrong are built again, the other 126 charms stay held: ' + JSON.stringify({ w: fixed.written, h: fixed.held }));
    ok(ix('HEARTKEY_NECKLACE_B').readerV === P.READER_VERSION && ix('ACORN_4721').pair.bodies === 2, 'and they are right now');
    ok(lines.some(l => /2 design\(s\) the library already holds are built again, because it holds them wrong/.test(l)), 'and the run says why');
    const last = await main(['node', 'x', FILE, '--origin', sorterOrigin, '--work-dir', work], log);
    ok(last.written === 0 && last.held === 128, 'a run after that writes nothing again');
  } finally { srv.close(); }
  console.log('master-corrected-1014 OK ·', n, 'checks');
})().catch(e => { console.error(e); process.exit(1); });
