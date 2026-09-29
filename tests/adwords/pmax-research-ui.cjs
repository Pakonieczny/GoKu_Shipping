// Product ads (Performance Max) research on the page: the headline, the occasion chip and the two fit lines on the card,
// the dated reasoning in "Why this recommendation", the banner that names unavailable sources, and the scan funnel that
// says how many ideas came out and why the others did not. Rendering only: no network, no provider, no clock but the fixed one.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const model = require('../../assets/pmax-recommendation.js');
const html = fs.readFileSync(path.resolve(__dirname, '../../brites-adwords.html'), 'utf8');
const sample = JSON.parse(fs.readFileSync(path.resolve(__dirname, 'fixtures/pmax-research-sample.json'), 'utf8'));
const clone = x => JSON.parse(JSON.stringify(x));

// The day the sample was written. `new Date()` inside the page code follows this clock.
let NOW = Date.parse('2026-09-29T17:05:00Z');
class FixedDate extends Date { constructor(...a) { if (a.length) super(...a); else super(NOW); } static now() { return NOW; } }

function fn(name) {
  const m = new RegExp('^(?:async )?function ' + name + '\\(', 'm').exec(html);
  assert(m, name);
  const rest = html.slice(m.index), next = /\n(?:async )?function \w+\(/.exec(rest);
  return next ? rest.slice(0, next.index) : rest;
}
// The page's own escaper (it does not escape quotes; attributes go through pmaxAttr / cmdAttr).
const ctx = { window: { BritesPmaxRecommendation: model }, Intl, Date: FixedDate, console, Math, Number, String, Object, Array, JSON, isFinite, parseInt,
  apvEnc: encodeURIComponent, friendlyResearchError: String, researchNeedsRefresh: () => false, wireOpportunityDeletion: () => {},
  PMAXOPPS: [], PMAXFUNNEL: null, PMAXAT: 1790700600000, PMAXERR: null };
vm.createContext(ctx);
vm.runInContext(fn('esc'), ctx);
vm.runInContext(html.slice(html.indexOf('function pmaxProductChoices('), html.indexOf('function renderOpportunities(){')), ctx);

let count = 0;
function test(name, run) { run(); count++; console.log('✓ ' + name); }

const fresh = () => clone(sample.opportunity);
const legacy = () => clone(sample.legacyOpportunity);
const ids = fresh().itemIds;
const titles = fresh().productTitles;

// The Product ads section as drawn for a list, with no browser: the markup the page would put in #pmaxSec.
function drawn(list, funnel, extra) {
  Object.assign(ctx, { PMAXOPPS: list, PMAXFUNNEL: funnel || null, PMAXAT: 1790700600000, PMAXERR: null }, extra || {});
  let placed = null;
  ctx.document = { getElementById: () => null, createElement: () => ({ id: '', innerHTML: '', querySelectorAll: () => [], querySelector: () => null, remove() {} }) };
  ctx.renderPmaxSection({ parentNode: { insertBefore: el => { placed = el; } } });
  return placed.innerHTML;
}
// The card's first block (title, summary, fit lines, counts), without its controls or folds.
const cardHead = markup => markup.split('<div class="pmxActions">')[0];
const text = markup => markup.replace(/<[^>]+>/g, ' ').replace(/&amp;/g, '&').replace(/\s+/g, ' ');
const has = (markup, ...parts) => parts.forEach(p => assert(markup.includes(p), 'missing: ' + p));
// Reads markup as a browser would, tag by tag: hostile text must stay text. Every "<" starts a well-formed tag from the page's own
// vocabulary, no attribute is an event handler, and every link is a plain https address.
const TAGS = new Set('div span b i small p ul ol li a h3 h4 header section details summary article label input button select option table thead tbody tr th td caption pre br'.split(' '));
function tags(markup) {
  const re = /<(\/?)([a-zA-Z][a-zA-Z0-9]*)((?:\s+[a-zA-Z_:][-a-zA-Z0-9_:.]*(?:\s*=\s*(?:"[^"]*"|'[^']*'|[^\s"'=<>]+))?)*)\s*\/?>/y, found = [];
  for (let i = markup.indexOf('<'); i >= 0; i = markup.indexOf('<', i + 1)) {
    re.lastIndex = i; const m = re.exec(markup);
    assert(m, 'a stray "<" in the markup near: ' + markup.slice(i, i + 70));
    const attrs = {}; for (const a of m[3].matchAll(/([a-zA-Z_:][-\w:.]*)(?:\s*=\s*("[^"]*"|'[^']*'|[^\s"'=<>]+))?/g)) attrs[a[1].toLowerCase()] = (a[2] || '').replace(/^["']|["']$/g, '');
    found.push({ name: m[2].toLowerCase(), closing: !!m[1], attrs });
  }
  return found;
}
function safeMarkup(markup, label) {
  for (const t of tags(markup)) {
    assert(TAGS.has(t.name), (label || '') + ' unexpected element <' + t.name + '>');
    for (const [k, v] of Object.entries(t.attrs)) {
      assert(!/^on/.test(k), (label || '') + ' event handler ' + k);
      if (k === 'href') assert(/^https:\/\/[^\s"'<>]+$/.test(v.replace(/&(amp|quot);/g, '')), (label || '') + ' unsafe link ' + v);
    }
  }
}
// Dates print in the viewer's locale; the test only needs the day and month.
const dayOf = (month, day) => new RegExp('(?:' + month + ' ' + day + '\\b|\\b' + day + ' ' + month + ')');

test('the sample follows the contract, so the tests below read the same shape the engine saves', () => {
  const r = fresh().research;
  assert.equal(r.version, 1);
  assert(r.headline.length <= 140 && r.whyNow.summary.length <= 420 && r.whyNow.timing.length <= 4 && r.keywords.length <= 12 && r.creativeAngles.length <= 4 && r.sources.length <= 8);
  assert.deepEqual(r.whyNow.timing.map(t => t.role), ['main', 'too-late', 'also']);
  for (const f of r.listingFit) assert(ids.includes(f.itemId));
  assert.equal(r.keywords.filter(k => typeof k.monthlySearches === 'number').length, 4);
  assert.equal(sample.researchStatus.pmax.unavailable.length, 2);
});

test('the markup reader really catches an injected element, handler or link', () => {
  safeMarkup('<div class="a"><span title="x &quot; y">t &lt;b&gt;</span><a href="https://ok.example/?a=1&amp;b=2" target="_blank">l</a></div>');
  for (const bad of ['<div><svg onload="x"></svg></div>', '<span title="a" onmouseover="x">t</span>', '<a href="javascript:alert(1)">l</a>', '<a href="https://ok.example/a b">l</a>', '<div>a < b</div>', '<span title=""><img src=x></span>'])
    assert.throws(() => safeMarkup(bad), bad);
});

test('the card leads with the research headline, not the generic sentence', () => {
  const markup = drawn([fresh(), legacy()], sample.funnel), head = cardHead(markup).split('<article class="pmxrow pmxOpportunity">')[1];
  const summary = /data-pmx-summary="0">([^<]*)</.exec(markup);
  assert.equal(summary[1], sample.opportunity.research.headline);
  assert(!/Test these products because buyers have already chosen them/.test(head), 'the first card no longer carries the fixed sentence');
  assert.match(markup, /class="pmxRationale is-headline" data-pmx-summary="0"/);
});

test('the occasion chip names the best-fit window and its days away, and only that', () => {
  const chip = /<span class="pmxOccasion"[^>]*>([^<]*)<\/span>/.exec(drawn([fresh()]));
  assert.equal(chip[1], 'Christmas · 87 days');
  const o = fresh(); o.research.whyNow.timing = o.research.whyNow.timing.filter(t => t.role === 'too-late');
  assert(!drawn([o]).includes('pmxOccasion'), 'a too-late window alone makes no chip');
  const noMain = fresh(); noMain.research.whyNow.timing.forEach(t => { t.role = 'also'; });
  assert(!drawn([noMain]).includes('pmxOccasion'));
  // The count follows today, not the day the research was written.
  NOW += 3 * 86400000; assert.match(drawn([fresh()]), /Christmas · 84 days/);
  NOW += 83 * 86400000; assert.match(drawn([fresh()]), /Christmas · 1 day</);
  NOW += 86400000; assert.match(drawn([fresh()]), /Christmas · today</);
  NOW += 86400000; assert(!drawn([fresh()]).includes('pmxOccasion'), 'a passed occasion shows no chip');
  NOW = Date.parse('2026-09-29T17:05:00Z');
});

test('Fits and Search themes are visible on the card, lead listing first', () => {
  const head = cardHead(drawn([fresh()]));
  has(head, 'data-pmx-facts="0"', '>Fits<', '>Search themes<');
  const fits = /class="pmxFact is-fits">([\s\S]*?)<\/div>/.exec(head)[1], themes = /class="pmxFact is-themes">([\s\S]*?)<\/div>/.exec(head)[1];
  assert(fits.indexOf(titles[0]) >= 0 && fits.indexOf(titles[0]) < fits.indexOf(titles[1]) && fits.indexOf(titles[1]) < fits.indexOf(titles[2]));
  for (const t of sample.opportunity.research.keywords.map(k => k.text)) assert(themes.includes(t), t);
  // More than the card can hold shows the rest as a count.
  const many = fresh(); many.research.keywords = Array.from({ length: 9 }, (_, n) => ({ text: 'theme ' + n, reason: 'r', kind: 'product', monthlySearches: null, competition: null }));
  assert.match(cardHead(drawn([many])), /\+4 more/);
  const lots = fresh(); lots.research.listingFit = ids.concat(['shopify_US_7304_9004', 'shopify_US_7305_9005']).map((itemId, n) => ({ itemId, title: 'Listing ' + n, reason: 'r', role: n ? 'support' : 'hero' }));
  lots.itemIds = lots.research.listingFit.map(x => x.itemId); lots.offerDetails = lots.itemIds.map(itemId => ({ itemId, title: 'Listing ' + lots.itemIds.indexOf(itemId), evidenceIds: [] }));
  lots.recommendationSchema = 1; ctx.PMAX_UI = {}; ctx.pmaxUi(lots).ids = lots.itemIds.map(id => id.split('_')[2]).slice(0, 4);
  assert.match(cardHead(drawn([lots])), /Listing 0[\s\S]*Listing 1[\s\S]*Listing 2[\s\S]*\+1 more/);
  ctx.PMAX_UI = {};
});

test('ticking and unticking products changes Fits at once, and the choice survives a redraw', () => {
  ctx.PMAX_UI = {};
  const o = fresh(), nodes = {}, node = s => nodes[s] || (nodes[s] = { innerHTML: '', textContent: '', dataset: {}, disabled: false, setAttribute() {} }), checks = [{ value: '7302' }, { value: '7303' }];
  const sec = { querySelectorAll: () => checks, querySelector: node };
  ctx.PMAXOPPS = [o]; node('.pmx-bud[data-i="0"]').value = '12';
  ctx.pmaxUpdateSelection(sec, 0);
  let facts = node('[data-pmx-facts="0"]').innerHTML;
  assert(!facts.includes(titles[0]), 'the unticked lead listing drops out');
  has(facts, titles[1], titles[2]);
  assert(facts.indexOf(titles[1]) < facts.indexOf(titles[2]), 'the remaining support listings keep their order');
  has(node('[data-pmx-why="0"]').innerHTML, 'Listings that fit', 'Dainty Initial Charm Necklace with Birthstone');
  assert(!node('[data-pmx-why="0"]').innerHTML.includes('Best seller with 31 orders'), 'its reason drops out of the fold too');
  assert.equal(node('[data-pmx-summary="0"]').textContent, o.research.headline);
  // Redrawn from the saved choice, the card shows the same two listings.
  assert(!cardHead(drawn([o])).includes(titles[0]));
  checks.length = 0; ctx.pmaxUpdateSelection(sec, 0);
  assert.equal(node('[data-pmx-facts="0"]').innerHTML, '', 'nothing selected, nothing claimed to fit');
  assert.match(node('[data-pmx-summary="0"]').textContent, /Select products/);
  assert.match(node('[data-pmx-why="0"]').innerHTML, /None of these listings is selected/);
  ctx.PMAX_UI = {};
});

test('an open panel stays open when the card is drawn again', () => {
  ctx.PMAX_UI = {}; const o = fresh();
  assert(/data-pmx-panel="why" data-i="0">/.test(drawn([o])), 'closed to begin with');
  ctx.pmaxUi(o).open = { why: true, products: true };
  const again = drawn([o]);
  assert.match(again, /data-pmx-panel="why" data-i="0" open>/); assert.match(again, /data-pmx-panel="products" data-i="0" open>/);
  ctx.PMAX_UI = {};
});

test('the fold opens with the dated reasoning above the existing evidence and forecast', () => {
  const o = fresh(), r = ctx.pmaxRecommendation(o, o.itemIds, 12), fold = ctx.pmaxRecommendationHtml(o, r), plain = text(fold);
  const order = ['>Why now<', '>Listings that fit<', '>Search themes<', '>Creative angles<', '>What we could not check<', '>Purchases support a reach test<', '>Your sales history<', '>What this test could deliver<'].map(k => fold.indexOf(k));
  assert(order.every(n => n >= 0), JSON.stringify(order));
  assert.deepEqual(order.slice().sort((a, b) => a - b), order, 'blocks appear in reading order');
  assert(!fold.includes('pmxRecommendationIntro'), 'the headline is not stated twice');
  assert(!/no recurring seasonal uplift/.test(plain), 'the season is dated, so the no-uplift caveat is gone');
  // Why now: summary, each window with its date, days, role wording and start date.
  has(plain, sample.opportunity.research.whyNow.summary);
  assert.match(plain, /Christmas US.* 87 days away best fit Start by Nov 13\./);
  assert.match(plain, dayOf('Dec', 25)); assert.match(plain, dayOf('Nov', 13));
  assert.match(plain, /Canadian Thanksgiving CA.* 13 days away too soon to learn in time Ideal start was Aug 31\./);
  assert.match(plain, /Valentine's Day US.*138 days away later Start by Jan 3, 2027\./);
  for (const line of sample.opportunity.research.whyNow.evidence) assert(plain.includes(line), line);
  // The outside read is labelled as such and links its pages in a new tab.
  has(fold, 'Outside read', 'from public web pages, not your store data', sample.opportunity.research.whyNow.marketRead);
  const links = fold.match(/<a class="srcLink"[^>]*>/g);
  assert.equal(links.length, 2);
  for (const a of links) assert(/target="_blank"/.test(a) && /rel="noopener noreferrer"/.test(a) && /href="https:\/\//.test(a), a);
  assert.match(fold, /href="https:\/\/trends\.example\.org\/explore\?q=birth%20flower%20necklace&amp;geo=US"/);
  has(plain, 'Holiday gift shopping outlook', 'Caution Birth flower necklaces are widely advertised');
  // Listings that fit, with their reasons and roles.
  for (const f of sample.opportunity.research.listingFit) has(plain, f.title, f.reason);
  has(plain, 'lead listing', 'supporting');
  // Search themes: volume only where it is a number, competition as a quiet word.
  for (const k of sample.opportunity.research.keywords) has(plain, k.text, k.reason);
  assert.equal((plain.match(/searches a month/g) || []).length, 4, 'the keyword with no volume says nothing about volume');
  has(plain, 'about 4,400 searches a month · medium competition', 'about 880 searches a month · low competition');
  assert.match(plain, /birthstone charm gift Fits the initial/);
  for (const a of sample.opportunity.research.creativeAngles) has(plain, a);
  for (const l of sample.opportunity.research.limits) has(plain, l);
  assert(!/Written from your store data/.test(plain));
  // No internal field names or ids reach the reader.
  assert(!/monthlySearches|listingFit|whyNow|startBy|shopify_US_|null|undefined|NaN|\[object/.test(plain), 'no leaks');
});

test('a note says so when the words were written from store data alone', () => {
  const o = fresh(); o.research.source = 'computed'; o.research.model = null; o.research.whyNow.marketRead = null; o.research.whyNow.caution = null; o.research.sources = [];
  const fold = ctx.pmaxRecommendationHtml(o, ctx.pmaxRecommendation(o, o.itemIds, 12));
  assert.equal((fold.match(/Written from your store data; the AI market read was unavailable\./g) || []).length, 1);
  assert(!fold.includes('Outside read') && !fold.includes('Caution') && !fold.includes('srcLink'));
  has(fold, 'Christmas', 'Listings that fit', 'Search themes');
  assert(!ctx.pmaxRecommendationHtml(fresh(), ctx.pmaxRecommendation(fresh(), ids, 12)).includes('Written from your store data'));
});

test('a suggestion saved before research keeps its card and says how to get the reasoning', () => {
  const markup = drawn([legacy()]), head = cardHead(markup);
  has(head, 'Saved suggestion: refresh research for the reasoning.', 'Test these products because buyers have already chosen them');
  assert(!head.includes('pmxOccasion') && !/>Fits</.test(head) && !head.includes('is-headline'));
  assert(!drawn([fresh()]).includes('refresh research for the reasoning'), 'a researched card carries no such tag');
  const o = legacy(), fold = ctx.pmaxRecommendationHtml(o, ctx.pmaxRecommendation(o, o.itemIds, 10));
  has(fold, 'pmxRecommendationIntro', 'Purchases support a reach test');
  assert(!/>Why now<|>Listings that fit<|>Creative angles<|>What we could not check</.test(fold));
  assert.match(fold, /no recurring seasonal uplift/);
});

test('hostile text in the headline, reasons, keywords, titles and sources is escaped, and unsafe links are dropped', () => {
  const evil = '<img src=x onerror=alert(1)>', o = fresh(), r = o.research;
  r.headline = evil + ' "quoted" headline'; r.whyNow.summary = evil; r.whyNow.marketRead = '<script>alert(2)</script>'; r.whyNow.caution = evil; r.whyNow.evidence = [evil];
  r.whyNow.timing[0].label = '<b>Xmas</b>'; r.whyNow.timing[0].note = evil; r.whyNow.timing[0].market = '<u>US</u>';
  r.listingFit[0].title = '"><svg onload=alert(3)>'; r.listingFit[0].reason = evil;
  r.keywords[0].text = '<script>alert(4)</script>'; r.keywords[0].reason = evil; r.keywords[1].text = 'a" onmouseover="alert(5)';
  r.creativeAngles = [evil]; r.limits = [evil];
  r.sources = [{ title: evil, url: 'https://ok.example/p?a="b"&c=d' }, { title: 'js', url: 'javascript:alert(6)' }, { title: 'spaced', url: 'https://bad.example/a b' }, { title: 'creds', url: 'https://user:pw@bad.example/' }, { title: 'data', url: 'data:text/html,<b>' }];
  o.offerDetails[0].title = r.listingFit[0].title;
  const markup = drawn([o]), fold = ctx.pmaxRecommendationHtml(o, ctx.pmaxRecommendation(o, o.itemIds, 12));
  safeMarkup(markup, 'card'); safeMarkup(fold, 'fold');
  has(cardHead(markup), '&lt;img src=x onerror=alert(1)&gt; "quoted" headline');
  has(fold, '&lt;script&gt;alert(2)&lt;/script&gt;', '&lt;b&gt;Xmas&lt;/b&gt;', '&lt;svg onload=alert(3)&gt;');
  assert.equal((fold.match(/<a class="srcLink"/g) || []).length, 1, 'only the plain https link survives');
  assert.match(fold, /href="https:\/\/ok\.example\/p\?a=&quot;b&quot;&amp;c=d"/);
  // A quote in a keyword cannot end the attribute it is repeated in.
  assert(/<span class="pmxFactItem" title="a&quot; onmouseover=&quot;alert\(5\)">/.test(markup), 'the quote is escaped inside the tooltip');
});

// ---- the status banner ----
function banner(status, checks, extra) {
  const nodes = {}, node = id => nodes[id] || (nodes[id] = { id, innerHTML: '', style: {}, querySelector: () => null, dataset: {} });
  const b = { esc: ctx.esc, $: s => node(s.replace(/^#/, '')), document: { getElementById: id => id === 'v-bench' ? { dataset: { lane: 'product' } } : null }, navigator: {}, toast() {}, Date, Math, Number, String, Object, Array, JSON, isFinite, Intl, console,
    SCAN_AUDIT: { engineVersion: '14.0.0', checks }, RESEARCH_STATUS: { pmax: status }, OPP_RECONCILIATION: clone(sample.reconciliation), OPPSAT: null, PMAXAT: 1790700600000, PMAXERR: null, OPP_SCANNING: false, OPP_LAST_ERROR: null, friendlyResearchError: String, oppSourceUrl: () => null, cmdAttr: s => String(s) };
  vm.createContext(b);
  for (const n of ['esc', 'auditTime', 'auditStatus', 'timeago', 'currentResearchChannel', 'researchState', 'researchCheckChannel', 'renderScanAudit']) {
    const m = new RegExp('^function ' + n + '\\(', 'm').exec(html); assert(m, n);
    const rest = html.slice(m.index), next = /\n(?:async )?function \w+\(/.exec(rest); vm.runInContext(next ? rest.slice(0, next.index) : rest, b);
  }
  Object.assign(b, extra || {});
  b.renderScanAudit();
  return node('scanAudit').innerHTML;
}
const auditChecks = () => clone(sample.scanAudit.checks);
const tile = (markup, name) => new RegExp('<b>' + name + '</b><span><i[^>]*>[^<]*</i>([^<]*)</span>').exec(markup)[1];

test('the banner lists each unavailable source and flips only its own tile to Partial data', () => {
  const markup = banner(clone(sample.researchStatus.pmax), auditChecks());
  has(markup, '<span class="researchHealthState partial">Some data unavailable</span>');
  const list = /<ul class="researchUnavailable">([\s\S]*?)<\/ul>/.exec(markup)[1];
  assert.deepEqual(list.match(/<li>[\s\S]*?<\/li>/g).map(x => text(x).trim()), ['Paid product history : Google returned no spend for these products', 'Keyword volumes : planner rate-limited']);
  assert.equal(tile(markup, 'Sales evidence'), 'Partial data');
  assert.equal(tile(markup, 'Product availability'), 'Checked');
  assert.equal(tile(markup, 'Campaign match'), 'Checked', 'a source that belongs to no tile only appears in the list');
  assert(!/<details class="researchTechnical" open/.test(markup) && /<details class="researchTechnical">/.test(markup), 'the research details stay closed');
});

test('each source id maps to its tile, and an error state names its sources too', () => {
  const map = { pmax_merchant_catalogue: 'Product availability', pmax_store_signals: 'Sales evidence', pmax_merchant_organic_30d: 'Sales evidence', pmax_merchant_organic_90d: 'Sales evidence', pmax_paid_product_reporting: 'Sales evidence', campaign_reconciliation: 'Campaign match' };
  for (const [id, name] of Object.entries(map)) {
    const markup = banner({ status: 'error', checkedAt: 1790700600000, message: 'Research failed.', unavailable: [{ id, label: 'Source', reason: 'why' }] }, auditChecks());
    for (const other of ['Product availability', 'Sales evidence', 'Campaign match']) assert.equal(tile(markup, other), other === name ? 'Partial data' : 'Checked', id + ' / ' + other);
    has(markup, '<span class="researchHealthState error">Needs attention</span>', '<li><b>Source</b>: why</li>');
  }
  // A source that never reported still reads as partial rather than "Not verified".
  const missing = banner({ status: 'partial', checkedAt: 1790700600000, message: 'm', unavailable: [{ id: 'pmax_merchant_organic_90d', label: 'Organic sales, 90 days', reason: 'report unavailable' }] }, auditChecks().filter(c => c.id !== 'pmax_merchant_organic_90d'));
  assert.equal(tile(missing, 'Sales evidence'), 'Partial data');
});

test('a ready state, or a partial state with nothing named, shows the old banner', () => {
  const ready = banner({ status: 'ready', checkedAt: 1790700600000, message: 'Fine.', unavailable: clone(sample.researchStatus.pmax.unavailable) }, auditChecks());
  assert(!ready.includes('researchUnavailable') && tile(ready, 'Sales evidence') === 'Checked');
  const bare = banner({ status: 'partial', checkedAt: 1790700600000, message: 'Some source data was unavailable.' }, auditChecks());
  assert(!bare.includes('researchUnavailable') && tile(bare, 'Sales evidence') === 'Checked');
  has(bare, 'Some data unavailable');
  const hostile = banner({ status: 'partial', checkedAt: 1790700600000, message: 'm', unavailable: [{ id: 'x', label: '<img src=x onerror=1>', reason: '<script>1</script>' }] }, auditChecks());
  safeMarkup(hostile, 'banner'); has(hostile, '&lt;img src=x onerror=1&gt;', '&lt;script&gt;1&lt;/script&gt;');
});

// ---- the funnel ----
test('with no ideas the empty state gives the verdict, the counts and each reason', () => {
  const markup = drawn([], sample.funnelNone);
  has(markup, 'No eligible feed opportunities this time.', sample.funnelNone.verdict, 'pmxStages', 'Why collections were left out');
  assert(!markup.includes('The latest scan found no Merchant Center products with enough evidence'), 'the generic sentence is gone');
  const plain = text(markup);
  assert.match(plain, /9 Collections looked at 6 With store sales in 90 days 0 Matched to a live product 0 Without a campaign or draft 0 After removing overlaps 0 Shown/);
  has(plain, 'Birthstone Charms US feed its feed products could not be checked because the Merchant Center product list was unavailable');
  const failed = drawn([], sample.funnelNone, { PMAXERR: 'HTTP 503 unavailable' });
  has(failed, 'Feed scan needs attention.', sample.funnelNone.verdict);
  // An older run saved no funnel: the wording it always had.
  has(drawn([], null), 'The latest scan found no Merchant Center products with enough evidence to recommend.');
  has(drawn([], null, { PMAXERR: 'x' }), 'Feed scan needs attention.');
  assert(!drawn([], null).includes('pmxStages'));
});

test('with ideas a closed fold says how they were chosen, and it keeps its state across redraws', () => {
  ctx.PMAX_FOLD = {};
  const closed = drawn([fresh()], sample.funnel), fold = /<details class="pmxHow" data-pmx-how([^>]*)>([\s\S]*?)<\/details>/.exec(closed);
  assert.equal(fold[1], '', 'closed by default');
  const plain = text(fold[2]);
  has(plain, 'How these were chosen', sample.funnel.verdict, 'Left out');
  assert.match(plain, /9 Collections looked at 6 With store sales in 90 days 4 Matched to a live product 3 Without a campaign or draft 2 After removing overlaps 2 Shown/);
  for (const s of sample.funnel.skipped) has(plain, s.title, s.feedLabel + ' feed', s.reason);
  assert(closed.indexOf('pmxHow') > closed.indexOf('pmxList'), 'it sits under the list of ideas');
  ctx.PMAX_FOLD.how = true;
  assert.match(drawn([fresh()], sample.funnel), /<details class="pmxHow" data-pmx-how open>/);
  assert(!drawn([fresh()], null).includes('pmxHow'), 'nothing when the run saved no funnel');
  assert(!drawn([fresh()], { at: 1 }).includes('pmxHow'), 'nothing for a funnel with nothing in it');
  ctx.PMAX_FOLD = {};
});

test('the funnel text is escaped', () => {
  const f = clone(sample.funnel); f.verdict = '<img src=x onerror=1>'; f.stages[0].label = '<b>x</b>'; f.stages[0].note = '"><i onclick=1>'; f.skipped[0].title = '<script>1</script>'; f.skipped[0].reason = '<svg onload=1>'; f.skipped[1].feedLabel = '"><u>';
  for (const out of [drawn([fresh()], f), drawn([], f)]) {
    safeMarkup(out, 'funnel');
    has(out, '&lt;img src=x onerror=1&gt;', '&lt;script&gt;1&lt;/script&gt;', '&lt;svg onload=1&gt;', '&lt;b&gt;x&lt;/b&gt;');
  }
});

// ---- the recommendation module ----
test('buildRecommendation uses the headline, and only dated timing removes the no-uplift caveat', () => {
  const c = fresh(), plainCandidate = clone(c); delete plainCandidate.research;
  const opts = { selectedItemIds: c.itemIds, dailyBudget: 12, days: 30, now: Date.parse('2026-09-11T00:00:00Z') };
  const withR = model.buildRecommendation(c, opts), without = model.buildRecommendation(plainCandidate, opts);
  assert.equal(withR.summary, c.research.headline);
  assert.match(without.summary, /^Test these products because buyers have already chosen them/);
  assert.match(without.seasonality.detail, /no recurring seasonal uplift or holiday lift is assumed\.$/);
  assert.equal(without.seasonality.title, 'Why now · demand and seasonality');
  assert(!/recurring seasonal/.test(withR.seasonality.detail));
  assert.deepEqual([withR.reasons.length, withR.forecast.status], [without.reasons.length, without.forecast.status], 'the evidence itself is unchanged');
  const undated = clone(c); undated.research.whyNow.timing = [{ label: 'Someday', date: 'soon', role: 'main' }];
  assert.match(model.buildRecommendation(undated, opts).seasonality.detail, /no recurring seasonal uplift/, 'a window with no date does not date the season');
  const none = clone(c); none.research.whyNow.timing = [];
  assert.match(model.buildRecommendation(none, opts).seasonality.detail, /no recurring seasonal uplift/);
  const blank = clone(c); blank.research.headline = '   ';
  assert.equal(model.buildRecommendation(blank, opts).summary, without.summary, 'an empty headline falls back to the old sentence');
  assert.match(model.buildRecommendation(c, { ...opts, selectedItemIds: [] }).summary, /^Select products/);
});

test('candidates without research read exactly as before, so the selector prompt is unchanged', () => {
  const plainCandidate = fresh(); delete plainCandidate.research;
  const before = model.buildRecommendation(plainCandidate, { dailyBudget: 12, days: 30, now: NOW });
  const same = model.buildRecommendation(Object.assign({}, plainCandidate, { research: undefined }), { dailyBudget: 12, days: 30, now: NOW });
  assert.equal(JSON.stringify(same), JSON.stringify(before));
  assert.deepEqual(Object.keys(before.seasonality).sort(), ['coverage', 'datedTiming', 'detail', 'months', 'status', 'title', 'totals', 'trend']);
  assert.equal(before.seasonality.datedTiming, false);
  assert(Array.isArray(before.reasons) && before.reasons.every(r => r.title && r.detail));
});

console.log(`${count} Product ads research UI checks passed.`);
