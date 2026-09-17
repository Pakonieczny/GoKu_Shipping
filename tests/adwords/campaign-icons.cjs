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
check((html.match(/campaignBadgeHtml\(c\)/g) || []).length >= 2, 'the campaign table and the spend report both use it');
check(!/esc\(campaignPipeline\(c\)\)/.test(html), 'no place still prints the bare text where the icon belongs');
check(/typeof BritesCampaignStyles==='undefined'/.test(html), 'the console degrades to text rather than throwing if the vocabulary is missing');
check(/brites-campaign-styles\.js/.test(html), 'the console loads the shared vocabulary');
check(/campaignStyleIcon\(s\.key,16\)/.test(html), 'the publish chooser shows each style\'s icon');

// 5. Budget guidance is present, specific, and offered as one click.
for (const s of styles.STYLES) {
  check(s.recommendedDaily >= s.minimumSensibleDaily && s.minimumSensibleDaily > 0, s.key + ' has a recommended budget at or above its sensible minimum');
  check(typeof s.budgetNote === 'string' && s.budgetNote.length > 40, s.key + ' explains why that budget, rather than asserting a number');
}
check(/data-style-recommend/.test(html) && /data-recommended=/.test(html), 'the recommendation can be taken in one click');
check(/aria-describedby="budget-hint-/.test(html), 'the hint is associated with its input for a screen reader');
check(/A daily budget is an average/.test(html), 'the page states what a daily budget actually means');

// 6. What is set up automatically is disclosed, and every line is true of the
//    publishing code. An invented reassurance is worse than none.
const publisher = fs.readFileSync(path.join(REPO, 'netlify/functions/googleAdsCampaignStyles.js'), 'utf8');
const autopilot = fs.readFileSync(path.join(REPO, 'netlify/functions/googleAdsAutopilot.js'), 'utf8');
check(/What we set up for you/.test(html), 'the chooser discloses what is configured on the operator\'s behalf');
for (const s of styles.STYLES) {
  check(s.autoSettings.length >= 4, s.key + ' discloses its automatic settings');
  check(s.autoSettings.some(l => /paused/i.test(l)), s.key + ' states that it starts paused');
  check(s.autoSettings.some(l => /never shared/i.test(l)), s.key + ' states that its budget is its own');
}
check(/status:'PAUSED'/.test(publisher) && /status: ?"PAUSED"/.test(autopilot), 'the publishing code really does create campaigns paused');
check(/explicitlyShared:false/.test(publisher) && /explicitlyShared: ?false/.test(autopilot), 'budgets really are unshared');
check(/maximizeConversions:\{\}/.test(publisher), 'Display bidding really is maximize conversions, as disclosed');
check(/brandGuidelinesEnabled: ?false/.test(autopilot), 'Performance Max brand guidelines really are off, as disclosed');
check(/assetAutomationStatus: ?"OPTED_OUT"/.test(autopilot), 'automatically generated creative really is opted out, as disclosed');

// 7. The page still parses.
for (const [, js] of html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)) if (js.trim()) new vm.Script(js);
check(true, 'every inline script parses');

console.log(passed + ' campaign identity, budget guidance and auto-setup checks passed.');
