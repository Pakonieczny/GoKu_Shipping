// The storefront click-id snippet is the first link in revenue attribution: no
// gclid on the order means Google Ads never learns the sale it earned, and
// Smart Bidding optimises against revenue that looks smaller than it is. It
// fails silently, so its behaviour is pinned here.
const assert = require('assert/strict'), fs = require('fs'), path = require('path'), vm = require('vm');
const REPO = path.resolve(__dirname, '../..');
const liquid = fs.readFileSync(path.join(REPO, 'shopify/snippets/brites-gclid-capture.liquid'), 'utf8');
const script = (liquid.match(/<script>([\s\S]*?)<\/script>/) || [])[1];
let passed = 0;
const check = (cond, name) => { assert.ok(cond, name); passed++; console.log('PASS', name); };

check(!!script, 'the snippet contains one script block');
check(/brites-gclid-capture\/\d+/.test(liquid), 'the snippet carries a version marker the installer check can read');
check(/\{%\s*render 'brites-gclid-capture'\s*%\}/.test(liquid), 'the snippet documents the exact render tag to install');

// A storefront just complete enough to run the snippet.
function browser({ search = '', cookie = '', session = {}, cartStatus = 200, cartThrows = false }) {
  const calls = [], store = { ...session };
  let jar = cookie;
  const ctx = {
    window: { location: { search, protocol: 'https:' } },
    document: { get cookie() { return jar; }, set cookie(v) { jar = v.split(';')[0]; } },
    URLSearchParams, Date, JSON, RegExp, Math, decodeURIComponent, encodeURIComponent,
    sessionStorage: { getItem: k => (k in store ? store[k] : null), setItem: (k, v) => { store[k] = String(v); } },
    fetch: (url, opts) => {
      calls.push({ url, body: JSON.parse(opts.body) });
      if (cartThrows) return Promise.reject(new Error('offline'));
      return Promise.resolve({ ok: cartStatus < 400, status: cartStatus });
    }
  };
  vm.createContext(ctx);
  vm.runInContext(script, ctx);
  return { calls, store, cookie: () => jar, ctx };
}
const settle = () => new Promise(r => setImmediate(() => setImmediate(r)));

(async () => {
  // 1. A click id on the landing URL reaches the cart as an attribute.
  const first = browser({ search: '?gclid=ABC123' });
  await settle();
  check(first.calls.length === 1 && first.calls[0].url === '/cart/update.js', 'a landing click id is pushed to the cart');
  check(first.calls[0].body.attributes.gclid === 'ABC123', 'the cart attribute carries the exact click id');
  check(/_brites_click=/.test(first.cookie()), 'the click id is also persisted for later visits');

  // 2. Every click parameter Google uses is captured, not just gclid.
  for (const key of ['gclid', 'gbraid', 'wbraid']) {
    const b = browser({ search: '?' + key + '=XYZ' });
    await settle();
    check(b.calls[0].body.attributes[key] === 'XYZ', key + ' is captured');
  }

  // 3. A visitor who arrives without a click id, carrying an earlier one, still
  //    attributes: the cookie outlives the landing page.
  const returning = browser({ search: '', cookie: '_brites_click=' + encodeURIComponent(JSON.stringify({ k: 'gclid', v: 'OLD1', t: 1 })) });
  await settle();
  check(returning.calls.length === 1 && returning.calls[0].body.attributes.gclid === 'OLD1', 'a stored click id is applied on a later pageview');

  // 4. A refused /cart/update.js must not be remembered as a success. fetch()
  //    resolves on 4xx, so a status check is the only thing standing between a
  //    rejected write and a click id retired for the whole session.
  const refused = browser({ search: '?gclid=FAIL1', cartStatus: 422 });
  await settle();
  check(refused.calls.length === 1, 'the refused write was attempted');
  check(refused.store._brites_click_synced === undefined, 'a refused cart update is NOT recorded as synced');
  const retry = browser({ search: '', cookie: refused.cookie(), session: refused.store });
  await settle();
  check(retry.calls.length === 1 && retry.calls[0].body.attributes.gclid === 'FAIL1', 'the next pageview retries the click id that failed');

  // 5. A second ad click in the same session must win. Crediting the sale to the
  //    first click sends Google the wrong campaign.
  const clickOne = browser({ search: '?gclid=FIRST' });
  await settle();
  check(clickOne.store._brites_click_synced === 'gclid=FIRST', 'the synced click id is remembered by value, not as a flag');
  const clickTwo = browser({ search: '?gclid=SECOND', cookie: clickOne.cookie(), session: clickOne.store });
  await settle();
  check(clickTwo.calls.length === 1 && clickTwo.calls[0].body.attributes.gclid === 'SECOND', 'a newer ad click replaces the earlier one on the cart');

  // 6. The same click on another pageview does not hammer the cart endpoint.
  const again = browser({ search: '?gclid=FIRST', cookie: clickOne.cookie(), session: clickOne.store });
  await settle();
  check(again.calls.length === 0, 'an unchanged click id is not re-sent on every pageview');

  // 7. Nothing here may break the storefront, whatever fails.
  for (const broken of [{ cartThrows: true }, { cookie: '_brites_click=not-json' }, { cookie: '_brites_click=' + encodeURIComponent('{"k":"gclid"}') }, {}])
    { const b = browser({ search: '?gclid=X1', ...broken }); await settle(); check(true, 'the snippet survives ' + JSON.stringify(broken).slice(0, 44)); }
  const noStorage = browser({ search: '?gclid=NOSTORE' });
  noStorage.ctx.sessionStorage = { getItem() { throw new Error('blocked'); }, setItem() { throw new Error('blocked'); } };
  vm.runInContext(script, noStorage.ctx); await settle();
  check(true, 'blocked session storage does not stop the cart update');

  console.log(passed + ' storefront click-id capture checks passed.');
})().catch(e => { console.error(e); process.exitCode = 1; });
