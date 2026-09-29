// One vocabulary for campaign styles: the icon, the name, the budget guidance
// and the settings applied on the operator's behalf. Server and browser read
// the same file, so a campaign cannot be drawn one way in the console and
// described another in a report.
const assert = require('assert/strict'), fs = require('fs'), path = require('path'), vm = require('vm');
const REPO = path.resolve(__dirname, '../..');
const styles = require(path.join(REPO, 'brites-campaign-styles.js'));
const server = require(path.join(REPO, 'netlify/functions/googleAdsCampaignStyles.js'));
const html = fs.readFileSync(path.join(REPO, 'brites-adwords.html'), 'utf8');
let passed = 0;
const check = (cond, name) => { assert.ok(cond, name); passed++; console.log('PASS', name); };

// 1. Every style the operator can publish has an identity, and the server
//    agrees with the browser about what it is called.
check(styles.STYLES.length === server.STYLES.length, 'the browser and the publisher know the same styles');
for (const key of server.STYLES) {
  const d = styles.describe(key);
  check(d.known && d.name === server.NAMES[key], key + ' is named identically on both sides');
  check(styles.ICONS[d.icon], key + ' has an icon');
  check(/^#[0-9a-f]{6}$/i.test(d.accent), key + ' has an accent colour');
}

// 2. Existing campaigns are identified by their Google channel, including the
//    two Display styles, which Google only distinguishes by campaign name.
check(styles.describe({ channel: 'PERFORMANCE_MAX' }).name === 'Performance Max', 'a Performance Max campaign is identified');
check(styles.describe({ channel: 'SEARCH' }).name === 'Search · text ads', 'a Search campaign is identified');
check(styles.describe({ channel: 'DISPLAY', name: 'BA · Fixed Display · 12' }).key === 'fixed_display', 'a Fixed Display campaign is told apart by its name');
check(styles.describe({ channel: 'DISPLAY', name: 'BA · Responsive Display · 12' }).key === 'responsive_display', 'a Responsive Display campaign is told apart by its name');
check(styles.describe({ channel: 'DISPLAY', name: 'something else' }).known === false, 'an unidentifiable Display campaign does not claim a style it may not be');
check(styles.describe({ channel: 'LOCAL_SERVICES' }).name === 'LOCAL_SERVICES', 'an unfamiliar channel keeps Google\'s own word for it');
check(styles.describe({}).name === 'Campaign type unavailable' && styles.describe(null).known === false, 'a campaign with no channel is reported as unknown, not guessed');

// 3. The badge is safe to put anywhere: escaped, labelled, and readable
//    without colour.
const hostile = styles.badge({ channel: 'DISPLAY', name: '<img src=x onerror=alert(1)>· Fixed Display ·' });
check(!/<img/.test(hostile), 'a campaign name cannot inject markup through the badge');
const badge = styles.badge('pmax');
check(/<svg /.test(badge) && /aria-hidden="true"/.test(badge), 'the icon is decorative and does not announce itself twice');
check(/Performance Max<\/span>/.test(badge), 'the badge carries a readable label beside its icon');
check(/title="Performance Max"/.test(badge), 'the type is discoverable on hover where the label is clipped');
check(/stroke="currentColor"/.test(styles.iconSvg('pmax')), 'icons inherit colour, so they work in either theme');
check(styles.iconSvg('no-such-icon').includes(styles.ICONS.unknown), 'an unknown icon falls back rather than rendering nothing');

// 4. The console draws them everywhere a campaign type is named, through the
//    one helper, and tolerates the vocabulary being absent.
check(/function campaignBadgeHtml/.test(html), 'the console has one badge helper');
check(/function campaignTable\([^]*?campaignBadgeHtml\(c\)/.test(html) && (html.match(/campaignBadgeHtml\((?:c|x\.c),\{label:false/g) || []).length >= 2,
  'the campaign table and the daily charts use it');
check(!/esc\(campaignPipeline\(c\)\)/.test(html), 'no place still prints the bare text where the icon belongs');
check(/typeof BritesCampaignStyles==='undefined'/.test(html), 'the console degrades to text rather than throwing if the vocabulary is missing');
check(/brites-campaign-styles\.js/.test(html), 'the console loads the shared vocabulary');
check(/campaignStyleIcon\(s\.key,16\)/.test(html), 'the publish chooser shows each style\'s icon');

// 5. Budget guidance is present, specific, and offered as one click.
for (const s of styles.STYLES) {
  check(s.recommendedDaily >= s.minimumSensibleDaily && s.minimumSensibleDaily > 0, s.key + ' has a recommended budget at or above its sensible minimum');
  check(typeof s.budgetNote === 'string' && s.budgetNote.length > 40, s.key + ' explains why that budget, rather than asserting a number');
}
check(/3× your cost per conversion/.test(styles.byKey.pmax.budgetNote) && styles.byKey.pmax.evaluationDays === 42, 'Performance Max budget guidance is Google\'s published rule (3× cost per conversion, 6 weeks), not an invented threshold');
check(styles.budgetCeilingMessage(40, 28, 100, 'CAD') === null, 'budgets inside the free part of the ceiling pass');
const overCeiling = styles.budgetCeilingMessage(90, 28, 100, 'CAD');
check(/CAD 90\.00/.test(overCeiling) && /CAD 72\.00/.test(overCeiling) && /Controls/.test(overCeiling), 'budgets over the ceiling are refused in the account currency, naming what is free and where to change it');
check(styles.budgetCeilingMessage(500, 0, 0, 'CAD') === null, 'no ceiling configured means no refusal');
check(/data-style-recommend/.test(html) && /data-recommended=/.test(html), 'the recommendation can be taken in one click');
check(/aria-describedby="budget-hint-/.test(html), 'the hint is associated with its input for a screen reader');
check(/A daily budget is an average/.test(html), 'the page states what a daily budget actually means');

// 6. What is set up automatically is disclosed, and every line is true of the
//    publishing code. An invented reassurance is worse than none.
const publisher = fs.readFileSync(path.join(REPO, 'netlify/functions/googleAdsCampaignStyles.js'), 'utf8');
const autopilot = fs.readFileSync(path.join(REPO, 'netlify/functions/googleAdsAutopilot.js'), 'utf8');
check(/what we set up for you/i.test(html), 'the chooser discloses what is configured on the operator\'s behalf');
for (const s of styles.STYLES) {
  check(s.autoSettings.length >= 4, s.key + ' discloses its automatic settings');
  check(s.autoSettings.some(l => /paused/i.test(l)), s.key + ' states that it starts paused');
  check(s.autoSettings.some(l => /never shared/i.test(l)), s.key + ' states that its budget is its own');
}
check(/status:'PAUSED'/.test(publisher) && /status: ?"PAUSED"/.test(autopilot), 'the publishing code really does create campaigns paused');
check(/explicitlyShared:false/.test(publisher) && /explicitlyShared: ?false/.test(autopilot), 'budgets really are unshared');
check(/maximizeConversions:\{\}/.test(publisher), 'Display bidding really is maximize conversions, as disclosed');
check(/brandGuidelinesEnabled: ?false/.test(autopilot), 'Performance Max brand guidelines really are off, so both logos may link to the asset group');
check(/assetAutomationStatus: ?"OPTED_OUT"/.test(autopilot), 'automatically generated creative really is opted out, as disclosed');
// Bidding is disclosed for every style, and matches what each builder sends.
check(styles.byKey.pmax.autoSettings.some(l => /conversion value/i.test(l)) && /maximizeConversionValue:/.test(autopilot), 'Performance Max discloses the conversion-value bidding its builder sends');
for (const key of ['responsive_display', 'fixed_display']) {
  check(styles.byKey[key].autoSettings.some(l => /maximize conversions/i.test(l)), key + ' discloses maximize-conversions bidding');
  check(styles.byKey[key].autoSettings.some(l => /no audience or placement targeting/i.test(l)), key + ' discloses that it has no audience or placement targeting');
}
const displayLane = server.displayOps({ customerId: '1', style: 'responsive_display', name: 'n', dailyBudget: 5, countries: ['2124'], destination: 'https://britesjewelry.com/products/x', images: [{ shape: 'square', resourceName: 'a/1' }, { shape: 'landscape', resourceName: 'a/2' }], copy: { headlines: ['H'], descriptions: ['D'], longHeadlines: ['L'] }, logo: 'a/3' });
check(!displayLane.some(o => o.adGroupCriterionOperation), 'the Display builder really adds no audience or placement targeting, as disclosed');
// PMax may not promise that only approved assets serve: the feed and a generated video can.
check(!styles.byKey.pmax.autoSettings.some(l => /only the assets you approved/i.test(l)) && styles.byKey.pmax.autoSettings.some(l => /product feed/i.test(l) && /video/i.test(l)), 'Performance Max states that feed data and a generated video can still serve');
check(!styles.STYLES.some(s => s.autoSettings.some(l => /standard delivery/i.test(l))), 'filler with no alternative (standard delivery) is not disclosed as a choice');

// 7. Opportunities name the campaign they would become in plain words, from the same file the server reads.
const thumbs = require(path.join(REPO, 'assets/ad-preview-thumbs.js'));
for (const [key, name, channel] of [['search', 'Search text ad', 'SEARCH'], ['pmax', 'Product ad (Performance Max)', 'PERFORMANCE_MAX'], ['responsive_display', 'Responsive banner ad', null], ['fixed_display', 'Fixed banner ad', null]]) {
  const k = styles.opportunityKind(key);
  check(k && k.key === key && k.name === name, key + ' is called "' + name + '" wherever an opportunity is shown');
  check([k.tagline, k.where, k.suits].every(l => typeof l === 'string' && l.length > 25 && l.length <= 100 && /[.]$/.test(l)), key + ' explains itself in three short sentences: what it is, where it shows, who it suits');
  check(styles.ICONS[k.icon] && /^#[0-9a-f]{6}$/i.test(k.accent), key + ' wears an icon and colour of the campaign it becomes');
  if (channel) check(k.icon === styles.describe(channel).icon && k.accent === styles.describe(channel).accent, key + ' wears the same mark as the live ' + channel + ' campaign');
  const b = styles.opportunityBadge(key);
  check(/<svg /.test(b) && /aria-hidden="true"/.test(b) && b.includes('<span>' + name + '</span>') && !/title=/.test(b), key + ' badge: decorative icon, the plain name, and no hover-only tooltip to repeat it');
}
check(styles.opportunityKind('product').key === 'pmax' && styles.opportunityKind('Performance-Max').key === 'pmax' && styles.opportunityKind(' SEARCH ').key === 'search', 'the words the console has used for a type all reach the same plain name');
check(styles.opportunityKind('video') === null && styles.opportunityKind('') === null && styles.opportunityKind(null) === null && styles.opportunityKind('__proto__') === null && styles.opportunityKind('constructor') === null, 'an unknown type has no name rather than a guessed one');
check(styles.opportunityBadge('video') === '' && styles.opportunityBadge(undefined) === '', 'and no badge');
const hostileLabel = styles.opportunityBadge('search', { text: '<img src=x onerror=alert(1)>' });
check(!/<img/.test(hostileLabel), 'a caller-supplied label is escaped');
check(thumbs.KINDS.every(kind => styles.opportunityKind(kind) && styles.opportunityKind(kind).name), 'every ad-preview kind is a type an opportunity can name');
check(publisher.includes("require('../../brites-campaign-styles')"), 'the publisher requires the very file the browser loads, so the two cannot describe a type differently');

// 8. Every script and stylesheet the console loads from this site is in the public build list. A file missing there works
//    on a developer machine and 404s on the live site, and the page then fails only for the operator.
const manifest = fs.readFileSync(path.join(REPO, 'scripts/build-public.cjs'), 'utf8');
const listed = new Set([...manifest.matchAll(/^\s*"([^"]+)",?\s*$/gm)].map(m => m[1]));
const loaded = [...html.matchAll(/<script[^>]*\ssrc="([^"]+)"/g), ...html.matchAll(/<link[^>]*rel="stylesheet"[^>]*\shref="([^"]+)"/g)].map(m => m[1]).filter(u => u.startsWith('/') && !u.startsWith('//'));
check(loaded.length >= 8 && loaded.some(u => u.startsWith('/assets/opportunity-timeline.js')) && loaded.some(u => u.startsWith('/assets/ad-preview-thumbs.js')), 'the console loads the timeline and the ad previews (' + loaded.length + ' local files)');
for (const url of loaded) {
  const file = decodeURIComponent(url.split(/[?#]/)[0].replace(/^\//, ''));
  check(listed.has(file) && fs.existsSync(path.join(REPO, file)), file + ' exists and is in scripts/build-public.cjs, so it is published');
}

// 9. The page still parses.
for (const [, js] of html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)) if (js.trim()) new vm.Script(js);
check(true, 'every inline script parses');

console.log(passed + ' campaign identity, budget guidance and auto-setup checks passed.');
