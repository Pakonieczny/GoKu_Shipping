// Every reported image statistic must be attributable to the shape it was
// earned by. Google reports metrics against an asset; the aspect ratio only
// exists if the asset's own pixel dimensions are requested and carried through.
const assert = require('assert/strict'), fs = require('fs'), path = require('path'), vm = require('vm');
const REPO = path.resolve(__dirname, '../..');
const policy = require(path.join(REPO, 'brites-ad-format-policy.js'));
let passed = 0;
const check = (cond, name) => { assert.ok(cond, name); passed++; console.log('PASS', name); };

// 1. Google's published ratios classify correctly, at their recommended and
//    minimum sizes alike — a 600×314 landscape is the same shape as 1200×628.
for (const spec of policy.images.filter(s => s.key !== 'logo')) {
  for (const [w, h] of [[spec.recommendedWidth, spec.recommendedHeight], [spec.minWidth, spec.minHeight]]) {
    const got = policy.shapeFor(w, h);
    check(got && got.shape === spec.key, spec.key + ' is recognised at ' + w + '×' + h);
  }
}
check(policy.shapeFor(1200, 628).orientation === 'landscape' && policy.shapeFor(960, 1200).orientation === 'portrait'
  && policy.shapeFor(1200, 1200).orientation === 'square', 'orientation is reported beside the named shape');

// 2. A ratio Google does not define is named 'other', never forced into a shape.
check(policy.shapeFor(1456, 816).shape === 'other', 'an undefined ratio is not forced into a Google shape');
check(policy.shapeFor(300, 250).shape === 'other', 'a display banner ratio is not mistaken for a marketing image');

// 3. Missing or nonsense dimensions produce nothing, never a guess.
for (const [w, h] of [[0, 100], [100, 0], [null, null], [undefined, 5], ['x', 'y'], [-5, 5], [NaN, 10]])
  check(policy.shapeFor(w, h) === null, 'no shape is invented from ' + JSON.stringify([w, h]));

// 4. Tolerance is proportional, so a legitimate rounding of 1.91:1 still lands.
check(policy.shapeFor(1201, 628).shape === 'landscape', 'a one-pixel rounding still resolves to landscape');
check(policy.shapeFor(1200, 700).shape === 'other', 'a materially different ratio does not claim to be landscape');

// 5. The live delivery report asks Google for the dimensions. Without this the
//    rows below could only ever be null.
const autopilot = fs.readFileSync(path.join(REPO, 'netlify/functions/googleAdsAutopilot.js'), 'utf8');
const delivery = autopilot.slice(autopilot.indexOf('async function _adDesignDeliveryFresh'), autopilot.indexOf('async function _prepareFirstAdDesignApproval'));
check(delivery.length > 1000, 'the delivery report was located');
const queries = [...delivery.matchAll(/SELECT [^`]*?FROM (asset_group_asset|ad_group_asset)/g)];
check(queries.length === 2, 'both the Performance Max and Search image queries were found');
for (const [q, from] of queries.map(m => [m[0], m[1]])) {
  check(/asset\.image_asset\.full_size\.width_pixels/.test(q) && /asset\.image_asset\.full_size\.height_pixels/.test(q),
    from + ' requests the image pixel dimensions');
  check(/metrics\.impressions/.test(q) && /metrics\.conversions/.test(q),
    from + ' requests the statistics those dimensions describe');
}

// 6. Both the totals rows and the per-device rows carry the shape. A mobile and
//    a desktop number for the same image are useless if neither names the shape.
check(/deviceMetrics=deviceRows[\s\S]*?_assetShape\(a\)/.test(delivery), 'per-device rows carry the shape');
check(/rows:rows\.filter[\s\S]*?_assetShape\(a\)/.test(delivery), 'image rows carry the shape');
check(/device:String\(r\.segments\.device\)\.toLowerCase\(\)/.test(delivery), 'per-device rows still name the device');

// 7. The helper itself: real dimensions produce a shape, absent ones produce
//    explicit nulls rather than an inferred shape.
const helperSrc = autopilot.slice(autopilot.indexOf('function _assetShape('), autopilot.indexOf('const _adDeliveryReads=new Map();'));
const ctx = { require: id => require(id.startsWith('../../') ? path.join(REPO, id.replace('../../', '')) : id), module: {}, console };
vm.createContext(ctx);
vm.runInContext(helperSrc + '\nthis.fn=_assetShape;', ctx);
const shapeOf = ctx.fn;
check(shapeOf({ imageAsset: { fullSize: { widthPixels: 1200, heightPixels: 628 } } }).shape === 'landscape', 'the helper shapes a real Google asset');
check(shapeOf({ imageAsset: { fullSize: { widthPixels: '960', heightPixels: '1200' } } }).shape === 'portrait', 'Google string-encoded pixel counts are handled');
check(shapeOf({ imageAsset: { fullSize: { widthPixels: 1200, heightPixels: 628 } } }).ratio === 1.911, 'the exact ratio is reported beside the shape');
for (const asset of [{}, { imageAsset: {} }, { imageAsset: { fullSize: {} } }, null]) {
  const got = shapeOf(asset);
  check(got.shape === null && got.ratio === null && got.width === null && got.height === null,
    'an asset without dimensions is left unshaped rather than guessed');
}

console.log(passed + ' asset shape and statistics-linkage checks passed.');
