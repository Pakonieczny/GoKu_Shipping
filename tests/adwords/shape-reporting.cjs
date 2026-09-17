// "Which aspect ratio earns" is a different question from "how did this picture
// do", and Google answers it with a different resource. asset_field_type_view
// groups results by the slot an asset filled; one impression can combine several
// assets, so shapes compare with each other and never sum into a total.
const assert = require('assert/strict'), fs = require('fs'), path = require('path'), vm = require('vm');
const REPO = path.resolve(__dirname, '../..'), FN = path.join(REPO, 'netlify/functions');
const policy = require(path.join(REPO, 'brites-ad-format-policy.js'));
const C = require(path.join(FN, '_googleConnections.js'));
let passed = 0;
const check = (cond, name) => { assert.ok(cond, name); passed++; console.log('PASS', name); };

// 1. One vocabulary, both directions, for every shape the app produces.
for (const spec of policy.images) {
  check(policy.shapeForFieldType(spec.fieldType) === spec.key, spec.fieldType + ' maps to the ' + spec.key + ' shape');
  check(policy.fieldTypeFor(spec.key) === spec.fieldType, 'the ' + spec.key + ' shape maps back to ' + spec.fieldType);
}
check(policy.shapeForFieldType('YOUTUBE_VIDEO') === 'video' && policy.fieldTypeFor('video') === 'YOUTUBE_VIDEO', 'video is part of the same vocabulary');
check(policy.shapeForFieldType('marketing_image') === 'landscape', 'field-type matching is case-insensitive');

// 2. Text slots share the resource and are not shapes. Reporting a headline as
//    an aspect ratio would be worse than omitting it.
for (const notAShape of ['HEADLINE', 'DESCRIPTION', 'LONG_HEADLINE', 'BUSINESS_NAME', 'CALL_TO_ACTION_SELECTION', '', null])
  check(policy.shapeForFieldType(notAShape) === null, JSON.stringify(notAShape) + ' is not reported as a shape');
check(policy.fieldTypeFor('nonsense') === null, 'an unknown shape yields no field type');
check(/Portrait 4:5/.test(policy.fieldTypeLabel('PORTRAIT_MARKETING_IMAGE')), 'a slot has a plain-language label with its ratio');
check(policy.fieldTypeLabel('HEADLINE') === 'Headline', 'an unmapped slot still gets a readable label');

// 3. The delivery report asks Google for it, with the device split.
const autopilot = fs.readFileSync(path.join(FN, 'googleAdsAutopilot.js'), 'utf8');
const delivery = autopilot.slice(autopilot.indexOf('async function _adDesignDeliveryFresh'), autopilot.indexOf('async function _prepareFirstAdDesignApproval'));
const shapeQuery = (delivery.match(/SELECT [^`]*FROM asset_field_type_view[^`]*/) || [''])[0];
check(/asset_field_type_view\.field_type/.test(shapeQuery), 'the delivery report groups by the asset slot');
check(/segments\.device/.test(shapeQuery), 'per-shape results are split by device');
for (const m of ['metrics.impressions', 'metrics.clicks', 'metrics.conversions', 'metrics.conversions_value', 'metrics.cost_micros'])
  check(shapeQuery.includes(m), 'per-shape results include ' + m);
check(/shapeRows,shapeError/.test(delivery), 'per-shape rows and their failure reason both reach the caller');
check(/catch\(e\)\{shapeError=/.test(delivery), 'a per-shape failure degrades instead of losing the whole report');

// 4. The aggregation: text slots dropped, shapes kept per device, never summed.
const helper = delivery.slice(delivery.indexOf('let shapeRows=[],shapeError=null;'), delivery.indexOf('let deviceRows='));
const ctx = { require: id => require(id.startsWith('../../') ? path.join(REPO, id.replace('../../', '')) : id), Map, Number, String, Object, console,
  fromMicros: m => m == null ? null : Math.round(Number(m) / 1e6 * 100) / 100 };
vm.createContext(ctx);
const raw = [
  { assetFieldTypeView: { fieldType: 'SQUARE_MARKETING_IMAGE' }, segments: { device: 'MOBILE' }, metrics: { impressions: 100, clicks: 10, conversions: 2, conversionsValue: 50, costMicros: 4000000 } },
  { assetFieldTypeView: { fieldType: 'SQUARE_MARKETING_IMAGE' }, segments: { device: 'DESKTOP' }, metrics: { impressions: 40, clicks: 2, conversions: 0, conversionsValue: 0, costMicros: 1000000 } },
  { assetFieldTypeView: { fieldType: 'HEADLINE' }, segments: { device: 'MOBILE' }, metrics: { impressions: 999, clicks: 99, conversions: 9, conversionsValue: 900, costMicros: 9000000 } }
];
ctx.gaql = async () => raw; ctx.filter = 'campaign.id = 1'; ctx.dates = "segments.date BETWEEN '2026-09-01' AND '2026-09-07'";
vm.runInContext('(async()=>{' + helper + ' this.out=shapeRows; this.err=shapeError;})()', ctx);
setTimeout(() => {
  const out = ctx.out || [];
  check(out.length === 2, 'each shape-and-device combination is one row');
  check(!out.some(r => r.shape === null || r.fieldType === 'HEADLINE'), 'a text slot never appears as a shape');
  const mobile = out.find(r => r.device === 'mobile'), desktop = out.find(r => r.device === 'desktop');
  check(mobile.shape === 'square' && mobile.impressions === 100 && mobile.ctr === 0.1, 'mobile results are attributed to the square shape with its own CTR');
  check(desktop.impressions === 40 && desktop.ctr === 0.05, 'desktop results stay separate from mobile');
  check(mobile.label === 'Square 1:1', 'each row carries the plain-language shape label');
  check(out[0].impressions >= out[1].impressions, 'rows are ordered by delivery');

  // 5. The campaign analysis reads it too, and states the limitation.
  const analysis = fs.readFileSync(path.join(FN, 'googleAdsAdAnalysis.js'), 'utf8');
  check(/read\('shapes','Google Ads'/.test(analysis), 'the campaign analysis reads per-shape results');
  check(/FROM asset_field_type_view/.test(analysis) && /segments\.device/.test(analysis), 'the analysis asks for the slot and the device');
  check(/shapes:c\.data\.shapes\|\|null/.test(analysis), 'per-shape results reach the analysis payload');
  check(/never be summed into a campaign total/.test(analysis), 'the analysis states that shapes must not be summed');

  // 6. The console renders it, and reads the slot names from the one vocabulary.
  const html = fs.readFileSync(path.join(REPO, 'brites-adwords.html'), 'utf8');
  check(/adDesignShapeHtml/.test(html) && /Performance by shape/.test(html), 'the console renders a per-shape report');
  check(/brites-ad-format-policy\.js/.test(html), 'the console loads the shared shape vocabulary');
  check(!/\{square:'SQUARE_MARKETING_IMAGE',landscape:'MARKETING_IMAGE'/.test(html), 'the console no longer restates the slot names it can look up');
  check(/BritesAdFormatPolicy\.fieldTypeFor\(s\.format\)/.test(html), 'tile metrics resolve their slot through the vocabulary');
check(/typeof BritesAdFormatPolicy!=='undefined'/.test(html), 'the page tolerates the vocabulary being absent instead of throwing');
  for (const [, js] of html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)) if (js.trim()) new vm.Script(js);
  check(true, 'every inline script still parses');

  // 7. The connection probe verifies exactly what the reports depend on.
  const probe = C.ADS_RESOURCES.find(r => r.resource === 'asset_field_type_view');
  check(probe.used === true, 'the catalog records per-shape reporting as a connection in use');
  check(/segments\.device/.test(probe.query) && /metrics\.conversions_value/.test(probe.query), 'the probe exercises the device split and the revenue the report shows');
  check(C.DEVICE_SPLIT_WANTED.includes('asset_field_type_view'), 'the device split is confirmed for the resource the shape report uses');

  console.log(passed + ' per-shape reporting checks passed.');
}, 50);
