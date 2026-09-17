// Overview, Sales, Approvals and Opportunities are four windows onto one
// account. A figure shown in two of them must be the same figure, named the
// same way, and must change in both at the same moment. These are the places
// where they had drifted apart.
const assert = require('assert/strict'), fs = require('fs'), path = require('path');
const REPO = path.resolve(__dirname, '../..');
const html = fs.readFileSync(path.join(REPO, 'brites-adwords.html'), 'utf8');
const groups = fs.readFileSync(path.join(REPO, 'brites-groups.js'), 'utf8');
let passed = 0;
const check = (cond, name) => { assert.ok(cond, name); passed++; console.log('PASS', name); };

// 1. The conversion basis is one setting, so changing it anywhere must redraw
//    everywhere. Overview used to redraw only itself: switch the basis there,
//    open Opportunities, and the two tabs showed different measurements of the
//    same campaign with no sign that they disagreed.
check(/function setConvBasis\(v\)/.test(html), 'the basis changes in exactly one place');
const setter = html.match(/function setConvBasis\(v\)\{[\s\S]*?\n\}/)[0];
check(/renderCommand\(\)/.test(setter), 'Overview is redrawn on a basis change');
check(/renderGrowthPerformance\(\)/.test(setter), 'the Opportunities lanes are redrawn on a basis change');
check(/renderPerformanceDialog\(\)/.test(setter), 'an open campaign dialog is redrawn on a basis change');
check(!/convBasis\s*=\s*(this\.value|v)\s*;\s*render(Command|GrowthPerformance)/.test(html.replace(setter, '')),
  'no control assigns the basis directly and redraws only its own tab');
const assignments = html.match(/convBasis\s*=(?![=])/g) || [];
check(assignments.length === 2, 'the basis is assigned only at its declaration and inside the shared setter');

// 2. Both tabs must call each basis the same thing. "Order date" in one place
//    and "Conversion date" in another reads as two different measurements.
check(/var BASIS_NAMES=\{click:"ad-click date",conversion:"conversion date"\}/.test(html), 'each basis has one name');
check(/mk\("conversion",basisName\("conversion"\)\)\+mk\("click",basisName\("click"\)\)/.test(html), 'the Overview toggle uses that name');
check(/esc\(basisName\('click'\)\)/.test(html) && /esc\(basisName\('conversion'\)\)/.test(html), 'the Opportunities and dialog controls use that name');
check(/basis='conversions '\+basisLabel\(\);/.test(html), 'the scope line under each report uses that name');
check(!/'conversions by conversion date'/.test(html) && !/mk\("conversion","order date"\)/.test(html), 'no tab keeps a private wording for a shared basis');

// 3. A budget or end date the operator changed and Google confirmed is carried
//    over every fetch of the dashboard. The ten-minute background pull skipped
//    it, so a verified change silently reverted while the tab sat open.
const live = html.match(/async function livePull\(\)\{[\s\S]*?\n\}/)[0];
check(/applyBudgetOverrides\(\);applyEndDateOverrides\(\);/.test(live),
  'the background pull carries the same verified overrides a manual refresh does');
const reload = html.match(/async function reload\(toastOk\)\{[\s\S]*?applyBudgetOverrides/)[0];
check(/applyBudgetOverrides/.test(reload), 'the manual refresh still carries them');
check(/cmdReport=r;cmdMetrics=r\.snapshot;applyEndDateOverrides\(\);/.test(html),
  'a fresh Overview report re-applies them, so Overview and Controls cannot show different end dates');

// 4. The Approvals badge counts what the Approvals tab lists. With a product
//    group selected the tab filtered and the badge did not, so the two
//    disagreed about how many drafts were waiting.
check(/function pendingInScope\(\)/.test(html), 'one function decides which drafts are in scope');
const badge = html.match(/function updateBadges\(\)\{[\s\S]*?\n/)[0];
check(/pendingInScope\(\)\.length/.test(badge), 'the badge counts them');
check(/var p=pendingInScope\(\);/.test(html), 'the Approvals list shows the same ones');
check(/changed:function\(\)\{if\(DASH\)\{renderApprovals\(\);updateBadges\(\);\}\}/.test(html),
  'changing the product group updates the list and the badge together');
check(/persist\(\);options\.changed\(\);const seq=\+\+state\.seq;/.test(groups),
  'selecting a group announces the new scope immediately, not only if its detail loads');

// 5. Google repeats UNKNOWN in primaryStatusReasons for reasons the API version
//    does not name. A status tooltip reading "Campaign paused · unknown ·
//    unknown" buries the one reason that does say something.
const reason = new Function('REASON_LABEL', 'return ' + html.match(/function reasonText\(rs\)\{[\s\S]*?\n.*?join\(" \\u00b7 "\);\}/)[0].replace('function reasonText', 'function reasonText') + '; reasonText')({ CAMPAIGN_PAUSED: 'Campaign paused' });
check(reason(['CAMPAIGN_PAUSED', 'UNKNOWN', 'UNKNOWN']) === 'Campaign paused', 'an unnamed reason is not shown as the word unknown');
check(reason(['CAMPAIGN_PAUSED', 'CAMPAIGN_PAUSED']) === 'Campaign paused', 'a reason Google repeats is shown once');
check(reason(['UNKNOWN']) === '', 'a status with nothing but unnamed reasons carries no tooltip at all');
check(reason(['CAMPAIGN_ENDED']) === 'campaign ended', 'a reason with no label of ours still reaches the operator in Google\'s own words');


console.log(passed + ' cross-tab coherence checks passed.');
