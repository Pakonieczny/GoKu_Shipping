// The Shopify half of revenue attribution: two things outside this repository
// that are silent when broken. A webhook that stopped firing and a snippet a
// theme update removed look exactly like a quiet week of trading.
const assert = require('assert/strict'), path = require('path');
const FN = path.resolve(__dirname, '../../netlify/functions');
const { shopifyAttribution, REQUIRED_TOPICS, RENDER_TAG } = require(path.join(FN, '_shopifyAttribution.js'));
let passed = 0;
const check = (cond, name) => { assert.ok(cond, name); passed++; console.log('PASS', name); };

const SNIPPET_V2 = '<script>/* brites-gclid-capture/2 */</script>';
const LAYOUT_OK = "<body>{% render 'brites-gclid-capture' %}</body>";

function store(o = {}) {
  const webhooks = o.webhooks === undefined
    ? [{ topic: 'orders/paid', address: 'https://goldenspike.app/.netlify/functions/shopifyOrderWebhook' },
       { topic: 'refunds/create', address: 'https://goldenspike.app/.netlify/functions/shopifyOrderWebhook' }]
    : o.webhooks;
  const assets = o.assets === undefined ? { 'snippets/brites-gclid-capture.liquid': SNIPPET_V2, 'layout/theme.liquid': LAYOUT_OK } : o.assets;
  return async p => {
    if (o.throwOn && p.includes(o.throwOn)) throw new Error(o.throwWith || 'HTTP 403');
    if (p.startsWith('webhooks.json')) return { webhooks };
    if (p === 'themes.json') return { themes: o.themes === undefined ? [{ id: 9, name: 'Dawn', role: 'main' }, { id: 8, name: 'Old', role: 'unpublished' }] : o.themes };
    if (p.includes('assets.json')) {
      const key = decodeURIComponent((p.match(/asset\[key\]=(.+)$/) || [])[1] || '');
      if (!(key in assets)) throw new Error('HTTP 404');
      return { asset: { key, value: assets[key] } };
    }
    return {};
  };
}
const run = o => shopifyAttribution({ request: store(o), expectedHost: 'goldenspike.app', expectedVersion: '2', ordersArriving: (o || {}).ordersArriving });

(async () => {
  // 1. A correctly wired store is reported healthy, and only then.
  const good = await run();
  check(good.summary.healthy === true, 'a fully wired store reports healthy');
  check(good.sections.snippet.present && good.sections.snippet.rendered === true, 'the snippet is found and confirmed rendered');
  check(good.sections.snippet.installedVersion === '2' && good.sections.snippet.problems.length === 0, 'the installed version matches what this repository ships');
  check(good.sections.webhooks.topics.every(t => t.ok), 'both required webhooks are recognised');

  // 2. A missing orders webhook stops every conversion, permanently.
  const noOrders = await run({ webhooks: [{ topic: 'refunds/create', address: 'https://goldenspike.app/x' }], ordersArriving: false });
  check(noOrders.summary.blocking.join(' ').includes('orders/paid'), 'a missing orders/paid webhook is blocking');
  check(noOrders.summary.healthy === false, 'a store missing a required webhook is not healthy');
  for (const t of REQUIRED_TOPICS) check(/Without/.test(t.why), t.topic + ' explains what breaks without it');

  // 2b. Shopify's webhooks.json lists only the webhooks the querying app owns.
  //     When orders are demonstrably reaching the conversion queue, a webhook
  //     this API cannot see is firing, and reporting it as missing would send
  //     someone chasing a problem that does not exist.
  const invisible = await run({ webhooks: [], ordersArriving: true });
  check(invisible.summary.blocking.length === 0, 'an invisible webhook is not blocking while orders are arriving');
  check(invisible.summary.warnings.join(' ').includes('orders are reaching the conversion queue'), 'it is reported as a warning that explains why it is not alarming');
  check(/another app or in the admin/.test(invisible.sections.webhooks.detail), 'the row explains that this API cannot see another app\'s webhooks');
  const silent = await run({ webhooks: [], ordersArriving: false });
  check(silent.summary.blocking.join(' ').includes('no evidence of webhook'), 'with no orders arriving and no visible webhook, it IS blocking');

  // 3. A webhook pointing somewhere else is as dead as one not registered.
  const elsewhere = await run({ webhooks: [{ topic: 'orders/paid', address: 'https://someone-elses-app.example/hook' }, { topic: 'refunds/create', address: 'https://goldenspike.app/x' }], ordersArriving: false });
  check(elsewhere.summary.blocking.join(' ').includes('orders/paid'), 'a webhook registered to another address is not counted');
  check(elsewhere.sections.webhooks.topics[0].registered === 1 && elsewhere.sections.webhooks.topics[0].addressed === 0, 'the row distinguishes registered from addressed to this app');

  // 4. The snippet must be in the PUBLISHED theme and actually rendered.
  const missing = await run({ assets: { 'layout/theme.liquid': LAYOUT_OK } });
  check(missing.sections.snippet.present === false && missing.summary.blocking.join(' ').includes('snippet is not installed'), 'an absent snippet is blocking');
  check(/NOT INSTALLED/.test(missing.sections.snippet.detail) && /Dawn/.test(missing.sections.snippet.detail), 'the failure names the published theme it looked in');

  const unrendered = await run({ assets: { 'snippets/brites-gclid-capture.liquid': SNIPPET_V2, 'layout/theme.liquid': '<body></body>' } });
  check(unrendered.sections.snippet.rendered === false && unrendered.summary.blocking.join(' ').includes('never rendered'),
    'a snippet present but never rendered is blocking, not passing');

  check(RENDER_TAG.test("{%- render 'brites-gclid-capture' -%}"), 'the render tag is recognised with whitespace trimming');
  check(!RENDER_TAG.test("{% render 'something-else' %}"), 'an unrelated render tag is not mistaken for it');

  // 5. An older installed copy is a warning, not a pass: it loses click ids.
  const old = await run({ assets: { 'snippets/brites-gclid-capture.liquid': '<script>/* brites-gclid-capture/1 */</script>', 'layout/theme.liquid': LAYOUT_OK } });
  check(old.sections.snippet.installedVersion === '1' && /v2 is available/.test(old.sections.snippet.problems.join(' ')), 'an outdated snippet names the version that fixes it');
  check(old.summary.healthy === true && old.summary.warnings.length === 1, 'an outdated but working snippet warns rather than blocks');

  const unversioned = await run({ assets: { 'snippets/brites-gclid-capture.liquid': '<script>/* old */</script>', 'layout/theme.liquid': LAYOUT_OK } });
  check(/no version marker/.test(unversioned.sections.snippet.problems.join(' ')), 'a snippet predating versioning is identified');

  // 6. An unread section leaves the question open; it never reads as healthy.
  const blocked = await run({ throwOn: 'webhooks.json', throwWith: 'HTTP 403 read_orders scope missing' });
  check(blocked.sections.webhooks.status === 'unavailable' && /403/.test(blocked.sections.webhooks.detail), 'an unreadable section records its reason');
  check(blocked.summary.healthy === false && blocked.summary.unavailable === 1, 'an unread section is never reported as healthy');

  // 7. No published theme at all is an error, not a silent pass.
  const noTheme = await run({ themes: [{ id: 8, name: 'Old', role: 'unpublished' }] });
  check(noTheme.sections.snippet.status === 'unavailable' && /No published theme/.test(noTheme.sections.snippet.detail), 'a store with no published theme is reported, not assumed fine');

  await assert.rejects(() => shopifyAttribution({}), /request function is required/); passed++;
  console.log('PASS the checker refuses to run without a Shopify request function');
  console.log(passed + ' Shopify attribution checks passed.');
})().catch(e => { console.error(e); process.exitCode = 1; });
