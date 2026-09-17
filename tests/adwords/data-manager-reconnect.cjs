// A Data Manager connection saved with the wrong scopes could not be replaced
// from the console: the form only rendered while unconfigured. Google requires
// two scopes, a credential carrying one still signs in, and the refusal that
// follows never mentions scopes — so the operator had no way to act on the one
// thing they could actually fix.
const assert = require('assert/strict'), fs = require('fs'), path = require('path'), vm = require('vm');
const REPO = path.resolve(__dirname, '../..');
const html = fs.readFileSync(path.join(REPO, 'brites-adwords.html'), 'utf8');
let passed = 0;
const check = (cond, name) => { assert.ok(cond, name); passed++; console.log('PASS', name); };

const start = html.indexOf('function convHealthHtml');
const rest = html.slice(start);
const end = /\n(?:async )?function \w+\(/.exec(rest);
const source = end ? rest.slice(0, end.index) : rest;
check(source.length > 400, 'the conversion health renderer was located');

const ctx = {
  esc: s => String(s == null ? '' : s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c])),
  money: n => '$' + n, timeago: () => '1m', Date, Number, String, Object, Array, Math, JSON, console
};
vm.createContext(ctx);
vm.runInContext(source + '\nthis.render=convHealthHtml;', ctx);

const base = { status: 'CONVERSION_TRACKING_MANAGED_BY_SELF', actions: [], reasons: [], queueDepth: 0, healthy: true, validated: true };
const render = h => ctx.render(Object.assign({}, base, h));

// 1. Never connected: the form is offered, as before.
const fresh = render({ dataManager: { configured: false, confirmed: 0, processing: 0, unknown: 0, retryable: 0 } });
check(/class="dmConnection"/.test(fresh), 'an unconnected account is offered the connection form');
check(/Connect Google conversion tracking/.test(fresh), 'it is labelled as connecting');

// 2. Connected but missing a scope: the form is offered again, opened, and
//    says which scope is missing and why it matters.
const missing = render({ dataManager: { configured: true, confirmed: 0, processing: 0, unknown: 0, retryable: 15, missingScopes: ['https://www.googleapis.com/auth/cloud-platform'] } });
check(/class="dmConnection"/.test(missing), 'a connection missing a scope can be replaced from the console');
check(/Reconnect Google conversion tracking/.test(missing), 'it is labelled as reconnecting, not connecting');
check(/<details open data-dm-reconnect>/.test(missing), 'it is open, because it needs attention rather than discovery');
check(/cloud-platform/.test(missing), 'the missing scope is named');
check(/never mentions scopes/.test(missing), 'it explains why the refusal did not reveal this');
check(/auth\/datamanager/.test(missing) && /auth\/cloud-platform/.test(missing), 'both required scopes are listed to consent with');

// 3. Connected and complete: no form, because there is nothing to fix.
const healthy = render({ dataManager: { configured: true, confirmed: 4, processing: 0, unknown: 0, retryable: 0, missingScopes: [] } });
check(!/class="dmConnection"/.test(healthy), 'a healthy connection is not asked to reconnect for no reason');
check(!/data-dm-reconnect/.test(healthy), 'and shows no reconnect notice');

// 4. Older health payloads predate missingScopes; absence must not be read as
//    a missing scope, or every account would be told to reconnect.
const legacy = render({ dataManager: { configured: true, confirmed: 2, processing: 0, unknown: 0, retryable: 0 } });
check(!/class="dmConnection"/.test(legacy), 'a health payload without the scope list is not treated as missing one');

// 5. Whatever the state, the markup stays well formed.
for (const [label, out] of [['fresh', fresh], ['missing', missing], ['healthy', healthy]]) {
  const opens = (out.match(/<details/g) || []).length, closes = (out.match(/<\/details>/g) || []).length;
  check(opens === closes, label + ' renders balanced <details> elements');
  check(!/undefined|\[object Object\]/.test(out), label + ' renders no undefined or raw objects');
}

console.log(passed + ' Data Manager reconnection checks passed.');
