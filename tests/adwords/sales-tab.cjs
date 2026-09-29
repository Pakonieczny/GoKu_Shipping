// The Sales tab: honest totals, plain source names, drill-downs from every panel to its orders,
// and state that survives a refresh. Synthetic orders only.
process.env.TZ = 'America/New_York';
const assert = require('assert/strict'), fs = require('fs'), vm = require('vm'), { JSDOM } = require('jsdom');
const html = fs.readFileSync('brites-adwords.html', 'utf8');
const cut = (a, b) => { const i = html.indexOf(a), j = html.indexOf(b, i); assert(i >= 0 && j > i, 'located ' + a); return html.slice(i, j); };
const dom = new JSDOM('<section id="v-sales"><div id="salesCtrls"></div><p id="salesCoverage"></p><div id="salesKpis"></div><div id="salesSources"></div><div id="salesProducts"></div><span id="salesTrendSub"></span><div id="salesTrend"></div><div id="salesOrdersCard"><span id="salesOrdMeta"></span><div id="salesTable"></div></div><div id="salesConv"></div></section><div id="cmdSales"></div>');
const { window } = dom, document = window.document;
let passed = 0; const check = (ok, name) => { assert.ok(ok, name); passed++; console.log('PASS', name); };

const today = new Date(), ago = d => new Date(today.getFullYear(), today.getMonth(), today.getDate() - d, 12).getTime(); // local noon, d days ago
const line = (title, qty, lineRevenue, extra) => Object.assign({ title, qty, lineRevenue }, extra || {});
const orders = [
  { orderId: '1001', value: 100, currency: 'USD', financialStatus: 'PAID', source: 'google', medium: 'product_sync', campaign: 'sag_organic', items: [line('Moon Necklace', 1, 80, { variantId: '1', handle: 'moon-necklace' }), line('Gift Wrapping', 1, 20)], ts: ago(1) },
  { orderId: '1002', value: 90, currency: 'USD', financialStatus: 'PAID', source: 'google', medium: 'paid_pmax', campaign: '555', hasClickId: true, items: [line('Moon Necklace', 2, 100, { variantId: '2' })], ts: ago(2) },
  { orderId: '1003', value: 40, currency: 'USD', financialStatus: 'PAID', source: 'shopify_email', medium: 'email', campaign: 'Autumn edit_1234567890', items: [line('Star Studs', 1, 40), line('Engraving on your custom charm', 1, 5)], ts: ago(3) },
  { orderId: '1004', value: 70, currency: 'USD', financialStatus: 'REFUNDED', items: [line('Star Studs', 1, 70)], ts: ago(3) },
  { orderId: '1005', value: 30, currency: 'USD', financialStatus: 'PAID', cancelledAt: '2026-01-01T00:00:00Z', items: [line('Star Studs', 1, 30)], ts: ago(4) },
  { orderId: '1006', value: 25, currency: 'USD', financialStatus: 'PAID', test: true, items: [line('Star Studs', 1, 25)], ts: ago(4) },
  { orderId: '1007', value: 60, currency: 'USD', financialStatus: 'PAID', source: 'google', medium: 'product_sync', campaign: 'sag_organic', hasClickId: true, items: [line('Sun Ring', 1, 60)], ts: ago(5) },
  { orderId: '1008', value: 50, currency: 'USD', financialStatus: 'PAID', items: [line('Sun Ring', 1, 50)], ts: ago(5) },
  { orderId: '1009', value: 20, currency: 'USD', financialStatus: 'PAID', source: 'chatgpt.com', items: [line('Sun Ring', 1, 20)], ts: ago(6) }
];
const browsed = [], toasts = [], went = [];
window.BritesGroups = { browseCampaign: id => browsed.push(id) };
const pending = { validated: false, healthy: true, failedCount: 0, queueDepth: 15, actionConfigured: true, configuredAction: { status: 'ENABLED' }, actions: [], recentConversions: 10,
  dataManager: { configured: true, confirmed: 0, processing: 15, unknown: 0, retryable: 0, missingScopes: [] }, reasons: ['15 conversion(s) submitted to Google Data Manager; asynchronous processing is still pending.'] };
const ctx = { window, document, URL, DASH: { currency: 'USD', recentOrders: orders, lastMetrics: [{ id: '555', name: 'Moon PMax', channel: 'PERFORMANCE_MAX' }], conversionHealth: pending },
  $: s => document.querySelector(s), esc: s => String(s == null ? '' : s).replace(/[&<>]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c])),
  money: n => { n = Number(n) || 0; return '$' + (Math.round(n * 100) / 100).toLocaleString('en-US', { minimumFractionDigits: n % 1 ? 2 : 0, maximumFractionDigits: 2 }); },
  _MON: ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'], timeago: () => '1m', btnBusy: () => () => {}, actStart: () => 0, actEnd: () => {},
  toast: m => toasts.push(m), api: async () => ({}), reload: async () => {}, go: v => went.push(v), ensureBench: async () => {}, dailyRangeYmd: () => ({ start: '2026-09-01', end: '2026-09-28' }),
  reportCached: async () => ({ groups: [] }), campaignPipeline: c => c.channel === 'PERFORMANCE_MAX' ? 'Performance Max' : 'Search · text ads' };
vm.createContext(ctx);
vm.runInContext(cut('var _MONF=', '/* ============ Sales revenue chart') + cut('/* ============ Sales revenue chart', '/* ---- nav ---- */') + cut('var salesRange=', 'var cmdRange=') + cut('function reportNumber(', '// A removed campaign cannot be edited'), ctx);
const $ = s => document.querySelector(s), $$ = s => [...document.querySelectorAll(s)], text = s => ($(s) || {}).textContent || '';

// Totals leave out refunded, cancelled and test orders, and say so.
ctx.renderSales();
const kpi = {}; $$('#salesKpis .kpi').forEach(k => { kpi[k.querySelector('.lab').textContent] = [k.querySelector('.val').textContent, k.querySelector('.delta').textContent]; });
check(kpi['Recorded revenue'][0] === '$360' && kpi['Orders'][0] === '6', 'revenue and orders count only paid, real orders');
check(/Not counted in this range: 1 refunded, 1 cancelled, 1 test order\./.test(text('#salesCoverage')), 'the page says which orders it left out');
check(kpi['Units sold'][0] === '7' && /excludes add-ons/.test(kpi['Units sold'][1]), 'gift wrap is revenue, not a unit sold');
check(kpi['Google Ads'][0] === '$150' && kpi['Google Ads'][1] === '42% of revenue' && kpi['Organic'][0] === '$100', 'the Google Ads and Organic tiles show their share of revenue');

// Plain source names; empty groups hidden; the legend adds up to revenue.
const chips = $$('#salesCtrls .segF').map(b => b.textContent);
check(JSON.stringify(chips) === JSON.stringify(['All6', 'Google Ads2', 'Organic1', 'Direct / other3']), 'source filter shows order counts and hides empty groups');
const names = $$('#salesSources .salesRow .salesRowTop>span:first-child').map(s => s.textContent);
check(['Google free listings', 'Google Ads · Performance Max', 'Google ad + free listing', 'Direct or untracked', 'Shopify Email', 'ChatGPT'].every(n => names.includes(n)), 'every source has a plain name');
check(!/sag_organic|product_sync|shopify_email|chatgpt\.com|paid_pmax/.test($('#v-sales').textContent), 'no raw tracking value reaches the page');
check($$('#salesSources .salesLeg b').reduce((s, b) => s + Number(b.textContent.replace(/[^0-9.]/g, '')), 0) === 360, 'the legend adds up to revenue');

// Variants of one listing share one row, and listing revenue follows the order's value.
const moon = $$('#salesProducts .salesRow').filter(b => /Moon Necklace/.test(b.textContent));
check(moon.length === 1 && /3 sold · \$170/.test(moon[0].textContent), 'one row per listing, with units and allocated revenue');
check(!$$('#salesProducts .salesRow').some(b => /Gift Wrapping|Engraving/.test(b.textContent)), 'add-ons are not listed as listings');

// Each sale names what earned it; only Google Ads campaigns get a campaign tree.
const origin = id => ctx.saleOriginHtml(orders.find(o => o.orderId === id));
check(/Email campaign “Autumn edit”\. Not an ad\./.test(origin('1003')) && !/1234567890|<details/.test(origin('1003')), 'an email campaign is named in plain words, never as a Google campaign');
check(/mixed attribution/.test(origin('1007')) && /Free Google Shopping listing/.test(origin('1001')) && /Referred by ChatGPT/.test(origin('1009')) && /No traffic source was recorded/.test(origin('1008')), 'every other shape gets one honest sentence');
check(/Moon PMax/.test(origin('1002')) && /saleCampaignOpen/.test(origin('1002')) && /Asset group not recorded\. Exact ad was not captured/.test(origin('1002')), 'a Google Ads sale links to its campaign and says what is missing');

// Clicking a listing lists its orders; an opened order stays open across a refresh; the campaign opens in the console.
moon[0].click();
check($$('#salesTable .srow').length === 2 && /Orders that include “Moon Necklace”/.test(text('.salesPick')) && $('.salesPick .salesAdvertise') && /^2 of 6 orders/.test(text('#salesOrdMeta')), 'a listing narrows the orders and offers to build a campaign');
$('.salesPick .salesAdvertise').click();
check(went.includes('bench') && toasts.some(t => /Moon Necklace/.test(t)), 'building a campaign opens the builder with the listing named');
$('.salesPick .salesPickClear').click();
check(!ctx.salesPick && $$('#salesTable .srow').length === 6, 'Show all orders clears the selection');
const row = $$('#salesTable .srow').find(r => r.dataset.k === '1002'); row.click();
ctx.renderSales(); ctx.renderSales();
const again = $$('#salesTable .srow').find(r => r.dataset.k === '1002'), det = $(`#salesTable .sdet[data-d="${again.dataset.i}"]`);
check(again.getAttribute('aria-expanded') === 'true' && !det.hidden, 'an opened order stays open across refreshes');
det.querySelector('.saleCampaignOpen').click();
check(browsed.join() === '555', 'Open campaign opens that campaign in the console');
again.dispatchEvent(new window.KeyboardEvent('keydown', { key: 'Enter', bubbles: true }));
check(again.getAttribute('aria-expanded') === 'false' && det.hidden, 'Enter closes a focused order');

// A source in the legend filters every panel; clicking it again clears the filter.
$('#salesSources .salesLeg[data-f="organic"]').click();
check(ctx.salesFilter === 'organic' && /Organic only/.test(text('#salesKpis')) && $$('#salesTable .srow').length === 1, 'the legend filters the whole tab');
$('#salesSources .salesLeg.on').click();
check(ctx.salesFilter === 'all', 'clicking the active legend row clears the filter');

// The chart: calendar days across a daylight-saving change, and weeks when bars would be too thin to tap.
const dst = ctx.salesSeries({ daily: {} }, { start: '2026-10-25', end: '2026-11-07' });
check(dst.length === 14 && new Set(dst.map(p => p.x)).size === 14, 'a daylight-saving change neither skips nor repeats a day');
check(ctx.salesSeries({ daily: {} }, { start: '2026-07-01', end: '2026-09-28' }, 40)[0].meta.mode === 'week', 'a narrow chart groups 90 days into weeks');
const bars = $$('#bc-salesTrend .bcBand[tabindex]');
bars[bars.length - 1].dispatchEvent(new window.MouseEvent('click', { bubbles: true }));
check(ctx.salesPick && ctx.salesPick.type === 'day' && $$('#salesTable .srow').length === 1, 'clicking a bar lists that day’s orders');
ctx.salesSetPick(null);

// Conversion uploads still processing at Google are pending: not failed, not lost.
const conv = text('#salesConv');
check(/Conversion uploads pending/.test(conv) && /15 awaiting Google/.test(conv) && /None has been rejected/.test(conv), 'processing uploads read as pending');
check(!/need review|not yet sent|\d+ rejected by Google|submitted to Google Data Manager/.test(conv), 'they are not called failed, unsent or counted twice');

// The Overview card states what it left out in one short line.
ctx.renderSalesSummary();
check(/Not counted: 1 refunded, 1 cancelled, 1 test order\./.test(text('#cmdSales')) && !/Covers all/.test(text('#cmdSales')), 'the Overview card names exclusions without the full coverage note');
console.log(passed + ' Sales tab checks passed.');
