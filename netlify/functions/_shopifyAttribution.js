'use strict';

// Revenue attribution depends on two things nobody was watching, both outside
// this repository and both silent when broken:
//
//   1. snippets/brites-gclid-capture.liquid installed AND rendered by the
//      published theme. Without it a paid order carries no click id, Google Ads
//      under-reports the revenue it earned, and Smart Bidding optimises toward
//      a number that is too small.
//   2. the orders/paid and refunds/create webhooks registered. Without the
//      first, nothing uploads. Without the second, refunds never retract and
//      reported ROAS stays permanently inflated.
//
// Every call here is a GET. `request(path)` is injected so this is testable
// without network access and the caller owns credentials.

const SNIPPET = 'brites-gclid-capture';
const RENDER_TAG = /\{%-?\s*render\s+'brites-gclid-capture'\s*-?%\}/;
const REQUIRED_TOPICS = [
  { topic: 'orders/paid', why: 'Every sale. Without it no conversion is ever uploaded to Google Ads.' },
  { topic: 'refunds/create', why: 'Refunds. Without it returned money is never retracted and reported ROAS stays inflated.' }
];

function text(v, n) { return String(v == null ? '' : v).slice(0, n || 300); }

async function section(out, id, label, work) {
  try { out[id] = { id, label, status: 'available', ...await work() }; }
  catch (error) { out[id] = { id, label, status: 'unavailable', detail: text(error && error.message || error, 300) }; }
  return out[id];
}

// A webhook registered to the wrong address is as dead as one not registered.
async function webhooks(request, expectedHost) {
  const data = await request('webhooks.json?limit=250');
  const all = (data.webhooks || []).map(w => ({ topic: text(w.topic, 60), address: text(w.address, 200) }));
  const rows = REQUIRED_TOPICS.map(required => {
    const matches = all.filter(w => w.topic === required.topic);
    const onTarget = expectedHost ? matches.filter(w => w.address.includes(expectedHost)) : matches;
    return {
      topic: required.topic, why: required.why,
      registered: matches.length, addressed: onTarget.length,
      addresses: matches.map(w => w.address).slice(0, 4),
      ok: onTarget.length > 0
    };
  });
  const missing = rows.filter(r => !r.ok);
  return {
    topics: rows, totalWebhooks: all.length, missing: missing.map(r => r.topic),
    detail: missing.length
      ? missing.length + ' required webhook(s) not registered to this app: ' + missing.map(r => r.topic).join(', ')
      : 'Both required webhooks are registered to this app.'
  };
}

// The published theme is the only one that serves customers. A snippet present
// in an unpublished theme attributes nothing.
async function snippet(request, expectedVersion) {
  const themes = await request('themes.json');
  const live = (themes.themes || []).find(t => t.role === 'main');
  if (!live) throw new Error('No published theme was returned for this store.');

  const readAsset = async key => {
    try {
      const got = await request('themes/' + live.id + '/assets.json?asset[key]=' + encodeURIComponent(key));
      return (got.asset || {}).value || null;
    } catch (e) { return null; }
  };

  const body = await readAsset('snippets/' + SNIPPET + '.liquid');
  const layout = await readAsset('layout/theme.liquid');
  const installedVersion = body ? (body.match(/brites-gclid-capture\/(\d+)/) || [])[1] || null : null;
  const rendered = layout ? RENDER_TAG.test(layout) : null;

  const problems = [];
  if (!body) problems.push('the snippet is not in the published theme');
  if (body && rendered === false) problems.push('the snippet exists but layout/theme.liquid never renders it');
  if (body && expectedVersion && installedVersion && Number(installedVersion) < Number(expectedVersion))
    problems.push('the installed snippet is v' + installedVersion + '; v' + expectedVersion + ' is available and fixes silent click-id loss');
  if (body && !installedVersion) problems.push('the installed snippet carries no version marker, so it predates versioning');

  return {
    theme: { id: live.id, name: text(live.name, 80) },
    present: !!body, rendered, installedVersion, expectedVersion: expectedVersion || null,
    layoutReadable: layout !== null,
    problems,
    detail: !body ? 'NOT INSTALLED in the published theme "' + text(live.name, 60) + '" — paid orders carry no click id.'
      : problems.length ? problems.join('; ')
        : 'Installed and rendered in "' + text(live.name, 60) + '" at v' + installedVersion + '.'
  };
}

async function shopifyAttribution(input) {
  const { request, expectedHost, expectedVersion } = input || {};
  if (typeof request !== 'function') throw new Error('A Shopify Admin API request function is required.');
  const out = {};
  await section(out, 'webhooks', 'Order and refund webhooks', () => webhooks(request, expectedHost));
  await section(out, 'snippet', 'Click-id capture snippet', () => snippet(request, expectedVersion));

  const blocking = [];
  if (out.webhooks.status === 'available' && out.webhooks.missing.length)
    blocking.push('missing webhook: ' + out.webhooks.missing.join(', '));
  if (out.snippet.status === 'available' && !out.snippet.present)
    blocking.push('the click-id snippet is not installed');
  if (out.snippet.status === 'available' && out.snippet.rendered === false)
    blocking.push('the click-id snippet is never rendered');

  const unavailable = Object.values(out).filter(s => s.status === 'unavailable');
  return {
    checkedAt: Date.now(), sections: out,
    summary: {
      read: Object.keys(out).length - unavailable.length, unavailable: unavailable.length,
      blocking, warnings: out.snippet.status === 'available' ? out.snippet.problems.filter(p => !/not in the published theme|never renders/.test(p)) : [],
      // An unread section leaves the question open; it never reads as healthy.
      healthy: blocking.length === 0 && unavailable.length === 0
    },
    note: 'Shopify reports registration and theme contents. It does not report whether a given order actually carried a click id; the conversion queue does.'
  };
}

module.exports = { shopifyAttribution, webhooks, snippet, REQUIRED_TOPICS, SNIPPET, RENDER_TAG };
