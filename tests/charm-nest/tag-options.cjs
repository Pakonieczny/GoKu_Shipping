// TAGSKU (Paul, 11 Oct 2026 13:17 UTC): listing 4458930445 "Mini Tag Initial Charm Add On Charm": its Charm Type drop-down offers eight values, Tag1..Tag4 x (front engrave | front+back),
// and "the tag designs and font options of this listing should be mapped to SKUs in our repository". Order 4195966039 (Tag1 front+back, SKU MINI_0445, Ace) was held with
// `option "Charm Type: Tag1 (front+back)" not mapped — Etsy's SKU list for listing 4458930445 is not loaded yet`; the older 4171852053 (Tag1 front engrave, no SKU) asked which design Tag1 is.
//   · Etsy's own table for the listing (read once, cache only, 0 Etsy calls): 32 products, ONE SKU (Mini_0445) on all of them, so the SKU cannot say which tag the buyer chose.
//   · the tag number is the SHAPE: the four master designs named by their size in millimetres (5X8.5, 5X8.8, 4.6X8.8, 5X9) are the four shapes of the listing's picture (the evidence below);
//   · the bracket is the ENGRAVING: front engrave = the front face, front+back / front + back = both faces (spec.tagOption, and `engrave` on the option's entry: CONTRACT section 13);
//   · a shape the master has no design for waits with ONE plain line naming it; a person's saved answer and the option's own SKU still decide first. No saved option map was needed: no live write.
// No network, no Etsy, no AI, no Firestore.   node tests/charm-nest/tag-options.cjs
const assert = require('assert/strict'), path = require('path'), fs = require('fs');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js'));
const EV = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/mini-tag-evidence.json'), 'utf8'));
const step = name => console.log('  ✓', name);
const eq = (a, b, m) => assert.deepStrictEqual(a, b, m), ok = (c, m) => assert(c, m);

/* ── the world: the real master, twice: the tag part of it as the live index answered (committed fixture), and the whole saved list when this machine has it (6,856 SKUs) ── */
const SAVED = '/mnt/project-files/plans/flag-fix-1011/master-skus-1011.json';
const worlds = [];
const mkLib = skus => { const lib = new Map(skus.map(s => [s, { sku: s }])), loose = {}; for (const k of lib.keys()) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; } return { lib, loose }; };
worlds.unshift(Object.assign({ name: 'the tag designs of the real master (fixture)', skus: Object.keys(EV.master).map(s => s.toUpperCase()) }, mkLib(Object.keys(EV.master).map(s => s.toUpperCase()))));
try { const all = JSON.parse(fs.readFileSync(SAVED, 'utf8')); if (Array.isArray(all) && all.length > 6000) { const skus = all.map(e => String(e.sku).toUpperCase()); worlds.push(Object.assign({ name: 'the whole saved master (' + all.length + ' SKUs)', skus }, mkLib(skus))); } } catch (_) { /* the saved list is not on this machine: the fixture world alone runs */ }
const ctxOf = (w, extra) => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, masterEntry: s => w.lib.get(String(s || '').toUpperCase()) || null, masterLoose: s => w.loose[O.looseKey(s)] || '' }, extra || {});
const TITLE = 'Mini Tag Initial Charm Add On Charm Personalized Letter Charm Minimalist Tiny Pendant Gift for Her Custom Initial Charm';
const VALUES = [['Tag1 (front engrave)', 1, 'front'], ['Tag1 (front+back)', 1, 'both'], ['Tag2 (front engrave)', 2, 'front'], ['Tag2 (front+back)', 2, 'both'], ['Tag3 (front engrave)', 3, 'front'], ['Tag3 (front + back)', 3, 'both'], ['Tag4 (front engrave)', 4, 'front'], ['Tag4 (front + back)', 4, 'both']];
const SHAPE = { 1: '5X8.5', 2: '5X8.8', 3: '4.6X8.8', 4: '5X9' };
const lineOf = (value, o) => Object.assign({ transactionId: '5214878738', listingId: '4458930445', productId: '9001', sku: 'Mini_0445', title: TITLE, quantity: 1, metalKey: 'gold', metalLabel: '', personalization: ['front - W\nback - 1. 27. 24'], buyerMessage: '',
  variations: [{ name: 'Metal Choice', value: 'Gold Filled' }, { name: 'Charm Type', value }, { name: 'Font Choice', value: 'Ace' }] }, o || {});
const orderOf = rid => ({ receiptId: String(rid), updateTs: 1, staffNote: '', buyerMessage: '' });
const read = (w, value, extra, lineExtra, rid) => O.interpretLine(orderOf(rid || '4195966039'), lineOf(value, lineExtra), ctxOf(w, extra));
const askedAbout = sp => sp.problems.filter(p => p.kind === 'needsMapping');
const tagOpt = sp => (sp.options.find(o => o.name === 'Charm Type') || {});

/* ── 1 · the evidence: which master design is each of the picture's four tags ── */
function evidence() {
  const rms = (a, b) => Math.sqrt(a.reduce((s, x, i) => s + (x - b[i]) ** 2, 0) / a.length);
  const portrait = Object.entries(EV.master).filter(([, m]) => m.aspect > 0.4 && m.aspect < 0.8);   // (the two landscape TINY INITIAL TAG bars are 2.9 wide to 1 high: no tag of the picture)
  eq(portrait.length, 7, 'seven portrait candidates: the four sized tags, MINI_0445 and TINY INITIAL TAG 2 / 3');
  for (const n of [1, 2, 3, 4]) {
    const want = SHAPE[n], mine = rms(EV.etsy[n].profile, EV.master[want].profile), near = portrait.map(([s, m]) => [s, rms(EV.etsy[n].profile, m.profile)]).sort((a, b) => a[1] - b[1]);
    ok(mine <= 0.021, `Tag${n}: ${want} has the picture's outline (rms ${mine.toFixed(3)})`);
    ok(mine - near[0][1] <= 0.006, `Tag${n}: nothing is clearly nearer than ${want}: ${JSON.stringify(near.slice(0, 3))}`);
    for (const [s, d] of near) if (d < mine) ok(rms(EV.master[s].profile, EV.master[want].profile) <= 0.02, `Tag${n}: a nearer design (${s}) is the same outline as ${want}, not another shape`);
    ok(EV.master[want].engravable === true && EV.master[want].holes === 1, `${want} has an engraving area and one hole`);
    ok(/OBJECTS/.test(EV.master[want].masterName), `${want} is one of the sized tags of the OBJECTS file`);
  }
  // the four tags are four different outlines, in the picture and in the master
  const pairs = [[1, 2], [1, 3], [2, 3], [2, 4], [3, 4]];
  for (const [a, b] of pairs) { ok(rms(EV.etsy[a].profile, EV.etsy[b].profile) > 0.05, `the picture's Tag${a} and Tag${b} differ`); ok(rms(EV.master[SHAPE[a]].profile, EV.master[SHAPE[b]].profile) > 0.05, `${SHAPE[a]} and ${SHAPE[b]} differ`); }
  eq(O.TAG_SHAPES[1].sku, SHAPE[1]); eq(O.TAG_SHAPES[2].sku, SHAPE[2]); eq(O.TAG_SHAPES[3].sku, SHAPE[3]); eq(O.TAG_SHAPES[4].sku, SHAPE[4]);
  eq(new Set([1, 2, 3, 4].map(n => O.TAG_SHAPES[n].sku)).size, 4, 'four different designs');
  // MINI_0445 (the listing's one shared SKU) is the chamfered outline, i.e. Tag 4's shape, and has no engraving area: it is NOT Tag 1
  ok(rms(EV.master['MINI_0445'].profile, EV.master['5X9'].profile) <= 0.01 && rms(EV.master['MINI_0445'].profile, EV.master['5X8.5'].profile) >= 0.02 && EV.master['MINI_0445'].engravable === false, 'MINI_0445 is the chamfered tag with no engraving area');
  step('evidence: Tag1 = 5X8.5 (plain rounded), Tag2 = 5X8.8 (pointed, rounded tip), Tag3 = 4.6X8.8 (pointed, flat tip), Tag4 = 5X9 (chamfered); the picture\'s outlines against the master\'s thumbnails; MINI_0445 is the chamfered shape');
}

/* ── 2 · the helper: the tag and the engraving of a value ── */
function helper() {
  for (const [v, tag, mode] of VALUES) { const t = O.tagOptionOf(v); eq([t.tag, t.mode], [tag, mode], v); }
  const same = { 'Tag3 (front + back)': 'both', 'TAG3 (FRONT+BACK)': 'both', 'Tag 3 [front and back]': 'both', 'tag3 - both sides': 'both', 'Tag2 front engrave': 'front', 'Tag 2 (Front Engraving)': 'front', 'Tag2 (front only)': 'front', 'Tag4 (back engrave)': 'back', ' Tag1  ( front + back ) ': 'both', 'Tag1 (front &amp; back)': 'both' };
  for (const [v, mode] of Object.entries(same)) eq(O.tagOptionOf(v).mode, mode, v);
  eq(O.tagOptionOf('Tag1'), { tag: 1, mode: null, words: '' }, 'a bare tag number has no engraving');
  eq(O.tagOptionOf('Tag1 (gold)').mode, null);
  for (const v of ['Necklace CHARM', 'Gold Filled', 'Ace', '', null, undefined, 'Tags 1', 'Tag 1: J', 'Tag11.5 (front)']) ok(!O.tagOptionOf(v) || (v === 'Tag 1: J' && O.tagOptionOf(v).mode === null) , 'not a tag choice: ' + v);
  step('helper: tagOptionOf("Tag3 (front + back)") = { tag: 3, mode: "both" }; the eight values, spacing / case / "and" / "both sides" spellings; no tag in "Necklace CHARM" or a number alone');
}

/* ── 3 · the eight values resolve with no question, with no table, an empty store, and Etsy's real table (one SKU on all 32 products) ── */
function eightValues() {
  const real = { '4458930445': { at: 1791650616248, n: 32, uni: 'Mini_0445' } };
  for (const w of worlds) {
    for (const [value, tag, mode] of VALUES) {
      for (const extra of [{}, { listingSkus: {} }, { listingSkus: real }, { listingSkus: { '4458930445': undefined } }]) {
        const sp = read(w, value, extra);
        eq(sp.problems, [], `${value}: no question (${w.name}): ${JSON.stringify(sp.problems)}`);
        eq(sp.designSku, SHAPE[tag], `${value}: the charm is the tag's shape`); eq(sp.skuSource, 'option', value);
        const o = tagOpt(sp);
        ok(o.mapped && o.mapped.field === 'design' && o.mapped.value === SHAPE[tag] && o.mapped.source === 'rule:tag-shape' && o.mapped.engrave === mode, `${value}: the option's entry names the design and the engraving: ${JSON.stringify(o)}`);
        eq(sp.tagOption, { name: 'Charm Type', value, tag, mode, shape: O.TAG_SHAPES[tag].shape, sku: SHAPE[tag], how: 'rule:tag-shape' }, `${value}: spec.tagOption`);
        eq(sp.form, 'charm', `${value}: a loose charm to add to a chain, as the title says`);
        ok(!sp.noDesign && !sp.special && !sp.ownDesign, `${value}: a regular line`);
        eq(sp.pieceCount, 1, value); eq(sp.pair.kind, 'single', `${value}: one charm, not a pair`); eq(sp.pair.sides, [null], `${value}: nothing mirrored`);
        eq(sp.material, 'gold', `${value}: the metal still comes from its own option`);
        eq(O.purchaseDetails(lineOf(value), sp).type, 'Charm only', `${value}: the order window's type reads Charm only, not "Type not specified"`);
      }
    }
  }
  step('the eight Charm Type values: Tag1 5X8.5, Tag2 5X8.8, Tag3 4.6X8.8, Tag4 5X9, mode front / both on the option\'s entry and on spec.tagOption; no problem with no table, an empty store or Etsy\'s one-SKU table; one charm, nothing mirrored, form charm; in the tag fixture and the whole saved master');
}

/* ── 4 · the two real orders ── */
function orders() {
  const w = worlds[worlds.length - 1];
  // 4195966039: Tag1 (front+back), SKU MINI_0445 (the listing's shared SKU), Ace, Gold Filled, "front - W / back - 1. 27. 24"
  const sp = read(w, 'Tag1 (front+back)', { listingSkus: {} }, {}, '4195966039');
  eq(sp.boughtSku, 'MINI_0445', 'the transaction carried the listing\'s one SKU'); eq(sp.designSku, '5X8.5'); eq(sp.tagOption.mode, 'both'); eq(sp.personalization, ['front - W\nback - 1. 27. 24']);
  const stored = { state: 'held', hold: null, poolIds: [], spec: sp, problems: [], reason: 'option "Charm Type: Tag1 (front+back)" not mapped — Etsy\'s SKU list for listing 4458930445 is not loaded yet' };
  ok(O.staleQuestionHold(stored), 'the stored held row (the Review row) is stale: it reads clean now');
  const before = O.interpretLine(orderOf('4195966039'), lineOf('Tag1 (front+back)'), ctxOf({ lib: new Map([['MINI_0445', {}]]), loose: {} }, { listingSkus: {} }));   // (a master without the four shapes: what waited before)
  ok(before.problems.some(p => p.kind === 'needsMapping' && /Tag1/.test(p.why)), 'without the shape in the master the row says so: ' + JSON.stringify(before.problems));
  ok(O.questionGone(Object.assign({}, stored, { spec: sp }), before), 'a row held for that question goes back to the intake when it reads clean');
  // 4171852053 (Sep 17 sandbox snapshot): Tag1 (front engrave), no SKU on the transaction, Vibur
  const old = read(w, 'Tag1 (front engrave)', {}, { transactionId: '5214878738', sku: '', personalization: [], variations: [{ name: 'Metal Choice', value: 'Gold Filled' }, { name: 'Charm Type', value: 'Tag1 (front engrave)' }, { name: 'Font Choice', value: 'Vibur' }] }, '4171852053');
  eq(old.designSku, '5X8.5'); eq(old.tagOption.mode, 'front'); eq(old.problems, [], 'no SKU on the transaction and no question: the tag number is the charm');
  // the snapshot's lines of the listing (counts only): 1 line, the one above
  const snap = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/addon-snapshot-lines.json'), 'utf8')), lines = (Array.isArray(snap) ? snap : snap.lines).filter(l => String(l.listing || l.listingId) === '4458930445');
  eq(lines.length, 1, 'one line of the listing in the Sep 17 snapshot');
  for (const l of lines) { const vars = (l.vars || []).map(([name, value]) => ({ name, value })); const r = O.interpretLine(orderOf(l.rid), { transactionId: l.tid, listingId: '4458930445', productId: '1', sku: l.sku || '', title: l.title, quantity: 1, metalKey: l.mk || 'gold', personalization: [], buyerMessage: '', variations: vars }, ctxOf(w)); eq(r.problems, []); eq(r.designSku, '5X8.5'); }
  step('order 4195966039 (Tag1 front+back, MINI_0445 on the transaction) reads 5X8.5 + both faces, its stored "not mapped" row is stale; 4171852053 (Tag1 front engrave, no SKU) reads 5X8.5 + front; the snapshot\'s one line of the listing is clean');
}

/* ── 5 · a shape with no master design waits, with ONE plain line naming it; never a look-alike ── */
function missingShape() {
  const full = worlds[0], without = skus => mkLib(full.skus.filter(s => !skus.includes(s)));
  for (const [gone, tag] of [['4.6X8.8', 3], ['5X8.5', 1], ['5X9', 4], ['5X8.8', 2]]) {
    const w = without([gone]);
    for (const [value, t, mode] of VALUES) {
      const sp = read(w, value, { listingSkus: {} }), asks = askedAbout(sp);
      if (t !== tag) { eq(sp.problems, [], `${value}: the other shapes are not affected by a missing ${gone}`); eq(sp.designSku, SHAPE[t]); continue; }
      eq(asks.length, 1, `${value}: ONE question`); eq(sp.problems.length, 1, value);
      ok(asks[0].optionName === 'Charm Type' && asks[0].optionValue === value && asks[0].why.includes('Tag' + tag) && asks[0].why.includes(O.TAG_SHAPES[tag].shape) && asks[0].why.includes(gone) && asks[0].why.length < 200, `${value}: one plain line naming the shape: ${asks[0].why}`);
      ok(!/Etsy|SKU list/.test(asks[0].why), 'it is not Etsy\'s "not loaded yet" line');
      eq(sp.designSku, null, `${value}: no look-alike on the row`); eq(sp.skuSource, null); eq(sp.tagOption.how, 'missing'); eq(sp.tagOption.mode, mode, 'the engraving is still read');
      ok(!sp.options.find(o => o.name === 'Charm Type').mapped, value + ': the option stays unmapped');
    }
  }
  const sp = read(without(['5X8.5', '5X8.8', '4.6X8.8', '5X9']), 'Tag2 (front+back)', {}); ok(askedAbout(sp)[0].why.includes('Tag2') && sp.designSku === null, 'a master with none of the four: each value names its own shape');
  // the picture has four tags: Tag5 is no shape of the listing
  const t5 = read(full, 'Tag5 (front+back)', {}); eq(askedAbout(t5).length, 1); ok(/Tag5 is not one of the listing's four tag shapes/.test(askedAbout(t5)[0].why), askedAbout(t5)[0].why); eq(t5.designSku, null);
  // no master loaded at all: nothing is claimed about a missing design, the ordinary hold stays
  const nm = O.interpretLine(orderOf('1'), lineOf('Tag3 (front+back)'), { optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] } });
  ok(!nm.problems.some(p => p.why && /master has no design/.test(p.why)), 'with no master to ask the rule says nothing about a missing design'); eq(nm.tagOption.how, 'unchecked');
  step('a missing shape waits with one plain line naming it (Tag3: pointed top with a flat tip, 4.6X8.8), no vector of a look-alike, the other shapes unaffected; Tag5 and a page with no master say the plain thing');
}

/* ── 6 · who decides first ── */
function precedence() {
  const w = worlds[worlds.length - 1];
  // a person's saved answer for the value stands (Review, optionMapPut): the rule steps aside, the engraving is still told
  const saved = { '4458930445': { 'charm type': { 'tag1 (front+back)': { field: 'design', value: 'MINI_0445', by: 'Ana_M', at: 1 } } } };
  const a = read(w, 'Tag1 (front+back)', { optionMaps: saved }), ao = tagOpt(a);
  eq(a.designSku, 'MINI_0445'); eq(a.skuSource, 'option'); eq(a.problems, []); ok(ao.mapped.source === 'listing' && ao.mapped.value === 'MINI_0445' && ao.mapped.engrave === 'both', JSON.stringify(ao)); eq(a.tagOption.how, 'answer'); eq(a.tagOption.mode, 'both');
  // a saved answer may carry the engraving itself
  const saved2 = { '4458930445': { 'charm type': { 'tag2 (front engrave)': { field: 'design', value: '5X8.8', engrave: 'both', by: 'Ana_M', at: 1 } } } };
  const b = read(w, 'Tag2 (front engrave)', { optionMaps: saved2 }); eq(b.tagOption.mode, 'both', 'a person\'s word for the engraving wins'); eq(tagOpt(b).mapped.engrave, 'both');
  // a saved answer that only ignores the option leaves the charm to the transaction's SKU, and the engraving is told
  const ig = read(w, 'Tag1 (front+back)', { optionMaps: { '4458930445': { 'charm type': { 'tag1 (front+back)': { field: 'ignore', value: null, by: 'x', at: 1 } } } } });
  eq(ig.designSku, 'MINI_0445'); eq(ig.tagOption.how, 'answer');
  // Etsy's own SKU for the value (a table that ties one SKU to this value alone) decides the charm: the rule never overrides it
  const tbl = { '4458930445': { at: 1, n: 2, products: [{ id: '1', sku: '5X9', d: 0, pv: [['514', '201']] }, { id: '2', sku: '5X8.8', d: 0, pv: [['514', '202']] }], names: { '514:201': 'Tag1 (front+back)', '514:202': 'Tag2 (front+back)' }, props: { '514': 'Charm Type' } } };
  const v1 = { name: 'Charm Type', value: 'Tag1 (front+back)', propertyId: '514', valueId: '201' };
  const c = read(w, 'Tag1 (front+back)', { listingSkus: tbl }, { sku: '5X9', variations: [{ name: 'Metal Choice', value: 'Gold Filled' }, v1] });
  eq(c.designSku, '5X9', 'Etsy\'s own SKU for the value is the charm'); eq(c.problems, []); ok(tagOpt(c).mapped.source === 'sku' && tagOpt(c).mapped.engrave === 'both', JSON.stringify(tagOpt(c))); eq(c.tagOption.how, 'sku');
  // an option's own SKU on the transaction (an indexed master family) is FLAGWHY's: left to it
  const fam = mkLib([...w.skus, ...Array.from({ length: 6 }, (_, i) => 'TAGFAM-' + i)]);
  const d = read(fam, 'Tag2 (front engrave)', {}, { sku: 'TAGFAM-3' }); eq(d.designSku, 'TAGFAM-3'); eq(d.problems, []); ok(tagOpt(d).mapped.source === 'sku:index' && tagOpt(d).mapped.engrave === 'front', JSON.stringify(tagOpt(d))); eq(d.tagOption.how, 'sku');
  // a value that is no tag choice is untouched: a Charm Type that names the form, a number alone, a note-like label
  const f = read(w, 'Necklace CHARM', { listingSkus: {} }); ok(!f.tagOption, 'no tag in "Necklace CHARM"'); eq(f.designSku, 'MINI_0445'); ok(f.form === 'necklace' || f.form === 'charm', 'the form rule is as before: ' + f.form);
  const g = read(w, 'Tag1', { listingSkus: {} }); ok(!g.tagOption && askedAbout(g).length === 1, 'a bare "Tag1" says no engraving, so it is not read as this listing\'s choice: it asks as before');
  step('who decides first: a person\'s saved answer (and its engrave word), then Etsy\'s own SKU for the value, then an indexed family SKU (FLAGWHY), then the tag number; a bare Tag1 or Necklace CHARM is untouched');
}

/* ── 7 · the source ── */
function source() {
  const src = fs.readFileSync(path.join(root, 'charm-nest-orders.js'), 'utf8'), html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
  ok(/rule:tag-shape/.test(src) && /tagOptionOf, tagOptionRead, TAG_SHAPES/.test(src), 'the rule and its helper are in charm-nest-orders.js');
  const tok = (html.match(/charm-nest-orders\.js\?v=([^"]*)"/) || [])[1] || '';
  ok(/-ts1$/.test(tok) && /-fw2-as1/.test(tok), 'the sorter page\'s cache token for charm-nest-orders.js keeps every earlier suffix and ends with mine: ' + tok);
  step('source: the rule, its helper and exports are in; the cache token keeps its history and ends -ts1');
}

[evidence, helper, eightValues, orders, missingShape, precedence, source].forEach(t => t());
console.log('tag-options: all passed');
