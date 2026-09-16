// Video assets are generated, uploaded and attached, so their outcomes must be
// readable too. Before this, nothing in the application ever queried a video
// statistic: films were published and then never measured.
const assert = require('assert/strict'), fs = require('fs'), path = require('path');
const REPO = path.resolve(__dirname, '../..'), FN = path.join(REPO, 'netlify/functions');
const C = require(path.join(FN, '_googleConnections.js'));
const analysis = fs.readFileSync(path.join(FN, 'googleAdsAdAnalysis.js'), 'utf8');
const autopilot = fs.readFileSync(path.join(FN, 'googleAdsAutopilot.js'), 'utf8');
let passed = 0;
const check = (cond, name) => { assert.ok(cond, name); passed++; console.log('PASS', name); };

// 1. The campaign analysis reads video outcomes.
const read = analysis.slice(analysis.indexOf("read('video'"), analysis.indexOf("read('video'") + 900);
check(read.length > 100, 'the analysis performs a video read');
check(/FROM video\b/.test(read), 'the read queries the video resource');
for (const metric of ['metrics.video_quartile_p25_rate', 'metrics.video_quartile_p50_rate',
  'metrics.video_quartile_p75_rate', 'metrics.video_quartile_p100_rate'])
  check(read.includes(metric), 'the read requests ' + metric);
// The live field catalog has no metrics.video_views in this API version.
// Asking for it would fail the entire read, losing the quartiles as well.
check(!/metrics\.video_views|metrics\.video_view_rate/.test(read), 'the read does not request a video metric this API version lacks');
for (const attribute of ['video.id', 'video.title', 'video.duration_millis'])
  check(read.includes(attribute), 'the read identifies the video by ' + attribute);
check(/metrics\.conversions,\s*metrics\.conversions_value/.test(read), 'video outcomes are reported beside conversions and value');
check(read.includes('${RANGE}'), 'video outcomes use the same campaign and date range as every other breakdown');

// 2. It is scoped to campaigns that actually link a video, and says so plainly
//    when they do not, rather than reporting missing evidence.
check(/hasVideo\?read\('video'/.test(analysis), 'the video read only runs when the saved version links a video');
check(/const hasVideo=.*assetLinks.*YOUTUBE_VIDEO/.test(analysis), 'linked videos are detected from the saved asset links');
check(/id:'video'.*status:'not_applicable'/.test(analysis), 'a campaign with no video records not-applicable, not missing evidence');

// 3. The result reaches the analysis payload; an unread breakdown is dead code.
check(/video:c\.data\.video\|\|null/.test(analysis), 'video outcomes are carried into the analysis payload');

// 4. Views are delivery, not proof of purchase. The limitation is stated.
check(/not proof that a view caused a purchase/.test(analysis), 'video view measures carry their stated limitation');

// 5. Google Ads' own record of YouTube processing is still read, so a film that
//    failed to transcode cannot be mistaken for one that is serving.
check(/FROM you_tube_video_upload/.test(autopilot), 'the YouTube upload status is read back from Google Ads');

// 6. The connection catalog treats video as a wired connection, so a video
//    reporting failure is reported as a failure and not as an unused capability.
const probe = C.ADS_RESOURCES.find(r => r.resource === 'video');
check(probe && probe.used === true, 'the catalog records video as a connection in use');
check(probe.query.includes('metrics.video_quartile_p100_rate') && !probe.query.includes('metrics.video_views'),
  'the video connection probe verifies the same statistics the analysis depends on');
check(C.ADS_RESOURCES.some(r => r.resource === 'you_tube_video_upload' && r.used === true),
  'the catalog verifies the YouTube upload connection too');

// 7. Every aspect ratio Google serves video in is produced, so no vertical or
//    square inventory is silently forfeited.
const policy = require(path.join(REPO, 'brites-ad-format-policy.js'));
for (const spec of C.REQUIRED_VIDEO_FORMATS) {
  const made = policy.video.formats.find(f => f.key === spec.key);
  check(made && Math.abs(made.width / made.height - spec.ratio) < 0.02, 'a ' + spec.key + ' master is produced for ' + spec.why);
}
check(policy.video.seconds >= policy.video.googleMinimumSeconds, 'films meet the Google minimum length they declare');

console.log(passed + ' video statistics checks passed.');
