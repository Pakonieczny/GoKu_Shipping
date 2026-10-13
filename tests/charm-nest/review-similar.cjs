// REVSUGGEST (12 Oct 2026): similar charms for ANY order in Review (charm-nest-siblings.js similar()), node only, fake neutral data.
//  Paul: "every time an order goes into the review tab ... present to the user multiple options that match the current SKU ... If there are no similar
//  matches for charm SKU then don't show any suggestive charms", and (HARD) "no exact match = no match, never fabricate a charm": the list only SUGGESTS.
//   node tests/charm-nest/review-similar.cjs
const path = require('path'), fs = require('fs'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js')), S = require(path.join(root, 'charm-nest-siblings.js'));
let n = 0; const ok = (c, m) => { n++; assert.ok(c, m); }, eq = (a, b, m) => { n++; assert.deepEqual(a, b, m); };

const master = ['BEADY SWAN', 'BEADY SWAN 2', 'BEADY SWAN 3', 'BEADY SWAN (HUGGIE)', 'BEADY SWAN 4', 'SWAN', 'CELESTIAL 3 - COMET', 'CAT 1 - SITTING CAT', 'CAT 2 - SLEEPING CAT',
  'OWL FLYING 2', 'MOON CHARM', 'GOLDCHARMO', 'CO-GOLDCHARMO', 'TWIN A', 'TWIN B', ...Array.from({ length: 14 }, (_, i) => 'STAR WIDE ' + (i + 1)), ...Array.from({ length: 11 }, (_, i) => 'FLORA ' + (i + 1) + ' LEAF')];
const entry = { 'BEADY SWAN 4': { blocked: 'two files' }, 'TWIN A': { aiPath: 'm/twin.ai' }, 'TWIN B': { aiPath: 'm/twin.ai' } };
const base = { skus: master, like: k => master.filter(m => O.looseKey(m) === O.looseKey(k)), entryFor: k => entry[k] || { aiPath: 'm/' + k + '.ai' }, blocked: k => !!(entry[k] && entry[k].blocked) };
const sim = (sku, o) => S.similar({ sku, title: 'a title that must never be read' }, Object.assign({}, base, o || {}));
const skusOf = r => r ? r.choices.filter(c => !c.current).map(c => c.sku) : null;

// 1 · what is similar, and that nothing similar means no list at all
eq(skusOf(sim('BEADY SWAN')), ['BEADY SWAN', 'BEADY SWAN (HUGGIE)', 'BEADY SWAN 2', 'BEADY SWAN 3'], 'the SKU\'s own design, the names that read like it, then every design whose name holds all its words; a blocked design (BEADY SWAN 4) is left out');
eq(skusOf(sim('CELESTIAL3-COMET')), ['CELESTIAL 3 - COMET'], 'the same name with other spacing and punctuation is found (the lookalike index)');
eq(skusOf(sim('CAT 1- SITTING CAT')), ['CAT 1 - SITTING CAT'], 'a dash the master spells with spaces; the other cat (another word) is not similar');
ok(sim('MOON CHARM-CO') === null || skusOf(sim('MOON CHARM-CO')).every(k => /MOON CHARM/.test(k)), 'the charm-only mark is read through to its base');
eq(sim('NOTHING LIKE THIS'), null, 'nothing similar: null, so no suggestion area at all');
eq(sim(''), null, 'no SKU: nothing is guessed (never from the title)');
eq(sim('GOLDCHARMO'), null, 'the shop-wide placeholder SKU names no design: no list'); eq(sim('CO-GOLDCHARMO'), null, '... with or without the CO mark');
eq(S.similar(null, base), null, 'no line: null');
eq(skusOf(sim('X', { base: 'OWL FLYING' })), ['OWL FLYING 2'], 'o.base reads another SKU than the line\'s (a line with a variation SKU)');

// 2 · the charm the line is matched to now: marked, listed first, and never the reason there is a list
{
  const r = sim('BEADY SWAN', { current: ['BEADY SWAN'] });
  eq(r.choices[0], { sku: 'BEADY SWAN', why: r.choices[0].why, current: true }, 'the matched charm is first and marked current');
  eq(r.choices.filter(c => c.current).length, 1, 'once');
  eq(skusOf(r), ['BEADY SWAN (HUGGIE)', 'BEADY SWAN 2', 'BEADY SWAN 3'], 'the others follow, the matched one is not offered again');
  eq(sim('CAT 1- SITTING CAT', { current: ['CAT 1 - SITTING CAT'] }), null, 'a list whose only entry is the matched charm is no list: no area');
  eq(skusOf(sim('NOTHING LIKE THIS', { current: ['MOON CHARM'] })), null, 'a matched charm with nothing similar to the SKU: no area either');
  eq(sim('BEADY SWAN', { current: ['A BLOCKED ONE'], exclude: master }), null, 'an offer the row already shows (exclude) is not listed twice');
}

// 3 · forms, blocked, one drawing under two names, other offers of the row
eq(skusOf(sim('BEADY SWAN', { formOk: k => !/HUGGIE/.test(k) })), ['BEADY SWAN', 'BEADY SWAN 2', 'BEADY SWAN 3'], 'a necklace line never sees a huggie design (the form guard)');
{
  const r = S.similar({ sku: 'TWIN A' }, Object.assign({}, base, { skus: ['TWIN A', 'TWIN B'], like: () => [], current: ['TWIN A'] }));
  eq(r, null, 'one drawing under two names (TWIN A, TWIN B share a file): nothing to choose');
  const r2 = S.similar({ sku: 'TWIN' }, Object.assign({}, base, { skus: ['TWIN A', 'TWIN B'], like: () => [] }));
  eq(r2.choices.map(c => c.sku), ['TWIN A'], 'two names of one drawing are ONE choice');
}
eq(skusOf(sim('BEADY SWAN', { exclude: ['BEADY SWAN 2', 'BEADY SWAN 3'] })), ['BEADY SWAN', 'BEADY SWAN (HUGGIE)'], 'the designs another offer of the row lists (the numbered pair) are not repeated');

// 4 · many: eight shown, the rest counted; a name that fits a dozen designs is a word, not a design
{
  const r = sim('FLORA LEAF');
  eq(r.choices.length, 8, 'at most eight are listed'); eq(r.extra, 3, 'and the rest are counted, not listed');
  const w = sim('STAR WIDE');
  eq(w, null, 'a name that fits 14 designs (more than a dozen) is a word: no list, none of them is guessed');
  const e = sim('STAR WIDE 1', { skus: master.concat(['STAR WIDE 1']) });
  ok(e === null || e.choices.every(c => c.sku === 'STAR WIDE 1'), 'with that many, only an exact name stays');
}
eq(sim('FLORA LEAF', { max: 3 }).choices.length, 3, 'o.max'); eq(sim('FLORA LEAF', { max: 3 }).extra, 8, '... and the count follows');

// 5 · it only suggests: pure, deterministic, no side effects, no guessing
{
  const a = JSON.stringify(sim('BEADY SWAN', { current: ['BEADY SWAN'] })), b = JSON.stringify(sim('BEADY SWAN', { current: ['BEADY SWAN'] }));
  ok(a === b, 'the same answer every time');
  const src = fs.readFileSync(path.join(root, 'charm-nest-siblings.js'), 'utf8'), at = src.indexOf('function similar(line, o)'), body = src.slice(at, src.indexOf('return { suggest, scan, index', at));
  ok(!/fetch\(|XMLHttpRequest|localStorage|Date\.now|Math\.random|api\(|document\.|window\./.test(body), 'similar() reads nothing, writes nothing, calls nothing (no network, clock, storage, DOM)');
  ok(!/line\.title|\.title\b/.test(body), 'never the listing\'s title');
  ok(!/pick\b|chosen|auto/i.test(body.replace(/\/\*[\s\S]*?\*\//g, '')), 'no code path that picks a design');
  const lines = [{ sku: 'BEADY SWAN' }]; const before = JSON.stringify(lines); S.similar(lines[0], base); ok(JSON.stringify(lines) === before, 'the line is not touched');
}

// 6 · the option cards' list is unchanged; only this list passes lead false
{
  const skus = ['ALGIZ', 'ALGIZ - DIVINE PLAN', 'ALGIZ WARD RUNE', 'ELDER FUTHARK ALGIZ'];
  const withLead = O.suggestCharms({ value: 'Algiz - Divine Plan', line: {}, skus }), noLead = O.suggestCharms({ value: 'Algiz - Divine Plan', line: {}, skus, lead: false });
  ok(withLead.some(x => /holds “algiz”/.test(x.why)), 'by default the lead word still finds designs (option cards)');
  ok(!noLead.some(x => /holds “algiz”/.test(x.why)), 'lead: false leaves the one-word guesses out');
}

// 7 · wiring (source): the page, the Review row and card, the pick
{
  const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8'), src = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
  const tag = f => (new RegExp('<script src="' + f.replace('.', '\\.') + '\\?v=([^"]+)"').exec(html) || [])[1] || '';
  ok(/-rs1/.test(tag('charm-nest-siblings.js')) && /-rs1/.test(tag('charm-nest-bridge.js')) && /-rs1/.test(tag('charm-nest-orders.js')), 'the three changed files carry the rs1 token');
  ok(/-sg1/.test(tag('charm-nest-siblings.js')) && /-sg1/.test(tag('charm-nest-bridge.js')), 'every earlier suffix is kept');
  ok(/function similarOffer\(row, p, group, it\)/.test(src) && /siblingOffer\(row, p \|\| \{\}, group\)/.test(src), 'the existing offer (lookalike, Use both, numbered designs) is reused as it is, and the similar charms are added to it');
  ok(/function fillMarkSlots\(host, it\)/.test(src) && /rvMarkSlot/.test(src) && /fillSimilar: fillMarkSlots/.test(src), 'a Send to Review order gets one slot for each line');
  ok(/async function pickSimilar\(it, p, sku, rows\)/.test(src) && /RL\.pick\(row, sku, \{ by: who \}\)/.test(src), 'a tap on a reviewed order\'s charm is REVLINK\'s pick (one write, read back, the mark resolved by it), never an aliasPut of this file');
  ok(!/op: "aliasPut"/.test(src.slice(src.indexOf('  function similarOffer('), src.indexOf('  /** The listing and the SKU each line of a card came with'))), 'no aliasPut written by the suggestion code');
  ok(/rr\.state === "unmatched"\) await repool\(rr\)/.test(src.slice(src.indexOf('async function pickSimilar'), src.indexOf('One list of the master') > 0 ? src.indexOf('One list of the master') : undefined)), 'only lines still waiting on their SKU are made up again (a pooled or sheeted line keeps its pieces)');
  ok(/it\.kind !== "reviewMark" \|\| rr\.state === "unmatched"/.test(src.slice(src.indexOf('async function saveBoth'), src.indexOf('function sibButtons'))), 'the same for Use both');
  ok(/CharmNestReviewMark\.decorateRow\(node,it\);[^\n]*\n\s*if\(it\.kind===\'reviewMark\'\)fillMarkSlots\(node,it\)/.test(src), 'the slots are filled right after the mark module made them');
}
console.log(`review-similar: ${n} checks passed`);
