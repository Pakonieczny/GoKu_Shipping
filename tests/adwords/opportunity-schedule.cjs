// How long should a recommended campaign run? The schedule planner (server, pure) and the timeline that draws it
// (browser + node). Covers: dated campaigns against Google's learning periods and the order cutoff, undated ones,
// Smart Bidding versus manual bids, year and leap-day boundaries, time-zone independence, and a renderer that is
// escaped, accessible and readable without colour.
const assert = require('assert/strict'), fs = require('fs'), path = require('path'), vm = require('vm');
const REPO = path.resolve(__dirname, '../..');
const SRC = fs.readFileSync(path.join(REPO, 'netlify/functions/_googleAdsSchedule.js'), 'utf8');
const S = require(path.join(REPO, 'netlify/functions/_googleAdsSchedule.js'));
const TL = require(path.join(REPO, 'assets/opportunity-timeline.js'));
const styles = require(path.join(REPO, 'brites-campaign-styles.js'));
const { buildSchedule, formatRange, LEARNING } = S;
let passed = 0;
const check = (cond, name) => { assert.ok(cond, name); passed++; };
const eq = (a, b, name) => { assert.deepEqual(a, b, name); passed++; };

// An independent calendar for the checks: whole days from Date.UTC, never the module's own helpers.
const day = ymd => { const [y, m, d] = ymd.split('-').map(Number); return Date.UTC(y, m - 1, d) / 86400000; };
const ymd = n => new Date(n * 86400000).toISOString().slice(0, 10);
const span = p => day(p.to) - day(p.from) + 1;
const key = (s, k) => (s.phases.find(p => p.key === k) || null);
// Only what sits inside a tag can act as an attribute; escaped text between tags is just text.
const handlers = html => (html.match(/<[^>]*>/g) || []).some(tag => /\son[a-z]+\s*=/i.test(tag.replace(/"[^"]*"/g, '""')));
const EMOJI = /[\u{1F000}-\u{1FAFF}\u{2600}-\u{27BF}\u{2B00}-\u{2BFF}\u{FE0F}]/u;

const XMAS = { label: 'Christmas', date: '2026-12-25', market: 'US' };
const HALLOWEEN = { label: 'Halloween', date: '2026-10-31', market: 'US' };
const BLACK_FRIDAY = { label: 'Black Friday', date: '2026-11-27', market: 'US' };
const T = '2026-09-29';

(async () => {
  // 1. Dates are plain calendar days: formatting, leap days and refusals.
  eq(formatRange('2026-11-06', '2026-12-04'), 'Nov 6 to Dec 4', 'a range reads "Nov 6 to Dec 4"');
  eq(formatRange('2026-12-20', '2027-01-08'), 'Dec 20 to Jan 8', 'a range across New Year needs no year');
  eq(formatRange('2026-11-06', '2026-11-06'), 'Nov 6', 'a one-day range is one date');
  eq(formatRange('2026-01-05', '2027-12-30'), 'Jan 5, 2026 to Dec 30, 2027', 'a range so long the month alone would mislead carries its years');
  eq(formatRange('nope', '2026-12-04'), 'Dec 4', 'an unreadable date is left out, not guessed');
  eq(S.addDays('2028-02-28', 1), '2028-02-29', 'the leap day exists in 2028');
  eq(S.addDays('2027-02-28', 1), '2027-03-01', 'and not in 2027');
  eq(S.addDays('2026-12-31', 1), '2027-01-01', 'the year rolls over');
  eq(S.daysBetween('2026-09-29', '2026-12-25'), 87, 'Sep 29 to Dec 25 is 87 days');
  eq(buildSchedule({ kind: 'search', today: T, event: { label: 'X', date: '2027-02-29' } }), null, 'an impossible event date is refused, not turned into a different day');
  eq(buildSchedule({ kind: 'search', today: '2026-9-29' }), null, 'a today that is not YYYY-MM-DD is refused');
  eq(buildSchedule({ kind: 'billboard', today: T }), null, 'an unknown campaign kind is refused');
  eq(buildSchedule(), null, 'no input is refused');

  // 2. Christmas from Sep 29: Search with Smart Bidding, 14-day order cutoff. Learning, then selling, then the cutoff.
  {
    const s = buildSchedule({ kind: 'search', today: T, event: XMAS, orderCutoffDays: 14 });
    eq([s.version, s.kind, s.today, s.timeSensitive, s.verdict], [1, 'search', T, true, 'good'], 'Christmas is a good fit for Search from Sep 29');
    eq(s.event, { label: 'Christmas', date: '2026-12-25', daysAway: 87, market: 'US' }, 'the event is 87 days away');
    eq(s.end, '2026-12-11', 'it stops 14 days before Christmas (the last order that can still arrive)');
    eq([s.start, s.days], ['2026-11-07', 35], 'it starts 2 weeks of learning plus 3 weeks of selling before that');
    eq(day(s.end) - day(s.start) + 1, s.days, 'the day count is inclusive of both ends');
    eq(s.phases.map(p => p.key), ['learning', 'selling', 'cutoff'], 'the bar is learning, then selling, then the cutoff');
    eq(s.phases.map(p => [p.from, p.to, p.days]), [['2026-11-07', '2026-11-20', 14], ['2026-11-21', '2026-12-11', 21], ['2026-12-12', '2026-12-25', 14]], 'the segments have the right dates');
    check(s.phases.every((p, i) => p.days === span(p) && (i === 0 || day(p.from) === day(s.phases[i - 1].to) + 1)), 'the segments touch and their day counts are right');
    eq([s.learning.days, s.learning.endsOn, s.judgeAfter], [14, '2026-11-20', '2026-11-21'], 'learning ends Nov 20; judge from Nov 21');
    eq(s.headline, 'Runs Nov 7 to Dec 11 · 35 days', 'the headline is dates and length');
    eq(s.why[0], 'Christmas is Dec 25, 87 days away.', 'the first reason is the event and how far off it is');
    eq(s.why[1], 'Google needs about 2 weeks to learn a Smart Bidding Search campaign; judge it from Nov 21.', 'the second is the learning period and the date to judge from');
    eq(s.why[2], 'Stops Dec 11 so gifts can still arrive by Dec 25.', 'the third is why it stops when it does');
    check(s.why.length <= 3 && s.why.every(w => w.length <= 140) && s.basis.length <= 200 && s.learning.note.length <= 160 && s.headline.length <= 120, 'the texts fit the contract limits');
    check(/^Ends Dec 11 \(Christmas Dec 25 less a 14-day order cutoff\)/.test(s.basis), 'the basis states the arithmetic');
    check(![s.headline, s.basis, s.learning.note, ...s.why].some(t => EMOJI.test(t)), 'no emoji');
  }

  // 3. Halloween. From Sep 29 with the default 7-day cutoff there is room; a longer cutoff or a later start takes it away.
  {
    const good = buildSchedule({ kind: 'search', today: T, event: HALLOWEEN, orderCutoffDays: 7 });
    eq([good.verdict, good.start, good.end, good.days], ['good', '2026-09-29', '2026-10-24', 26], 'Halloween from Sep 29 (7-day cutoff): 26 days, 2 weeks learning, 12 days to sell');
    eq(good.phases.map(p => p.days), [14, 12, 7], 'learning 14, selling 12, cutoff 7');
    const tight = buildSchedule({ kind: 'search', today: T, event: HALLOWEEN, orderCutoffDays: 14 });
    eq([tight.verdict, tight.end, tight.phases.map(p => p.days)], ['tight', '2026-10-17', [14, 5, 14]], 'with a 14-day cutoff only 5 days are left to sell: tight');
    check(/^Tight fit: Google finishes learning Oct 13, leaving only 5 days to sell\.$/.test(tight.why[1]), 'a tight plan says how little selling time there is');
    const short = buildSchedule({ kind: 'search', today: T, event: HALLOWEEN, orderCutoffDays: 21 });
    eq([short.verdict, short.end], ['too_short', '2026-10-10'], 'with a 21-day cutoff the last order date leaves 12 days, less than the 14 Google needs to learn');
    eq(short.why[0], 'Too late to test this for Halloween: it would still be learning when the last gift orders can arrive.', 'it says so plainly');
    eq(short.headline, 'Would run Sep 29 to Oct 10 · 12 days', 'and still shows the dates it would have');
    eq(short.phases.map(p => p.key), ['learning', 'cutoff'], 'the bar never shows selling that cannot happen');
    const late = buildSchedule({ kind: 'search', today: '2026-10-12', event: HALLOWEEN, orderCutoffDays: 7 });
    eq([late.verdict, late.days], ['too_short', 13], 'Halloween from Oct 12: 13 days, still learning at the last order date');
    const two = buildSchedule({ kind: 'search', today: '2026-10-09', event: HALLOWEEN, orderCutoffDays: 7 });
    eq(two.verdict, 'too_short', 'learning that ends 2 days before the last order is still too late');
    eq(two.why[0], 'Too late to test this for Halloween: learning would end only 2 days before the last gift orders can arrive.', 'and says how many days are left');
    // Manual bids have no bidding model to train: the same date that is too late for Smart Bidding still works, tightly.
    const manual = buildSchedule({ kind: 'search', today: '2026-10-12', event: HALLOWEEN, orderCutoffDays: 7, smartBidding: false });
    eq([manual.verdict, manual.learning.days, manual.phases.map(p => p.days)], ['tight', 7, [7, 6, 7]], 'with manual bids the same Halloween is tight, not too late');
  }

  // 4. Black Friday for Product ads (Performance Max). Google's 6-week figure is when to JUDGE, not how long learning
  //    blocks selling, so Black Friday from Sep 29 is a good fit; it only becomes too late when the 3-week learning phase
  //    no longer fits before the last order date.
  {
    const s = buildSchedule({ kind: 'pmax', today: T, event: BLACK_FRIDAY, orderCutoffDays: 7 });
    eq([s.verdict, s.start, s.end, s.days], ['good', '2026-10-10', '2026-11-20', 42], 'Black Friday from Sep 29: a 42-day run ending on the last order date');
    eq(s.phases.map(p => [p.key, p.days]), [['learning', 21], ['selling', 21], ['cutoff', 7]], '3 weeks learning, 3 weeks selling, 7-day cutoff');
    eq(s.judgeAfter, '2026-11-21', 'the fair 6-week read lands the day after the run ends');
    check(/before it is fair to judge; judge it from Nov 21\.$/.test(s.why[1]) && /Performance Max/.test(s.why[1]), 'the learning line names Performance Max and the judge date');
    eq(styles.byKey.pmax.evaluationDays, LEARNING.pmax.judgeAfter, 'the 6 weeks is the same figure the console already cites for Performance Max');
    const verdictOn = today => buildSchedule({ kind: 'pmax', today, event: BLACK_FRIDAY, orderCutoffDays: 7 }).verdict;
    eq(['2026-10-21', '2026-10-22', '2026-10-26', '2026-10-27'].map(verdictOn), ['good', 'tight', 'tight', 'too_short'], 'it goes good, tight, too short as 10 days, then 5 days, of selling after learning stop fitting');
    const gone = buildSchedule({ kind: 'pmax', today: '2026-10-30', event: BLACK_FRIDAY, orderCutoffDays: 7 });
    eq(gone.why[0], 'Too late to test this for Black Friday: learning would end only 1 day before the last gift orders can arrive.', 'one day left reads "1 day"');
    eq(buildSchedule({ kind: 'video', today: T, event: BLACK_FRIDAY, orderCutoffDays: 7 }).learning.days, 14, 'Video learns in about 2 weeks');
    eq(buildSchedule({ kind: 'demand_gen', today: T, event: BLACK_FRIDAY, orderCutoffDays: 7 }).judgeAfter, '2026-11-14', 'Demand Gen is judged after about 3 weeks (start Oct 24)');
  }

  // 5. Evergreen campaigns start today and run the usual length for their kind.
  {
    const s = buildSchedule({ kind: 'search', today: T });
    eq([s.verdict, s.timeSensitive, s.event, s.start, s.end, s.days], ['evergreen', false, null, T, '2026-10-26', 28], 'Search: 28 days from today, the same default planCampaign() uses');
    eq(s.phases.map(p => [p.key, p.days]), [['learning', 14], ['selling', 14]], 'no cutoff for an undated campaign');
    eq(s.headline, 'Runs Sep 29 to Oct 26 · 28 days', 'the evergreen headline');
    const days = k => buildSchedule({ kind: k, today: T }).days;
    eq(['search', 'pmax', 'display', 'demand_gen', 'video'].map(days), [28, 42, 21, 28, 21], 'the default lengths');
    const p10 = buildSchedule({ kind: 'pmax', today: T, requestedDays: 10 });
    eq([p10.days, p10.end, p10.phases.map(p => p.key)], [10, '2026-10-08', ['learning']], 'a requested length is honoured');
    check(/ends before Google has had 6 weeks/.test(p10.why[2]), 'and a run shorter than the read time says so');
    const win = buildSchedule({ kind: 'search', today: T, startDate: '2026-09-01', endDate: '2026-10-10' });
    eq([win.start, win.end, win.days, win.verdict], [T, '2026-10-10', 12, 'evergreen'], 'a chosen window replaces the plan; a past start moves to today');
    eq(buildSchedule({ kind: 'responsive_display', today: T }).kind, 'display', 'the console\'s Display style keys map to display');
    eq(buildSchedule({ kind: 'performance_max', today: T }).kind, 'pmax', 'and Performance Max');
    eq(buildSchedule({ kind: 'search', today: T, requestedDays: 'abc' }).days, 28, 'a nonsense length falls back to the default');
  }

  // 6. The order cutoff moves the last day and appears as its own segment.
  {
    const at = c => buildSchedule({ kind: 'search', today: T, event: XMAS, orderCutoffDays: c });
    eq([0, 7, 14, 30].map(c => at(c).end), ['2026-12-25', '2026-12-18', '2026-12-11', '2026-11-25'], 'the run ends event minus cutoff (cutoff capped at 30)');
    eq(at(0).phases.map(p => p.key), ['learning', 'selling'], 'no cutoff segment when orders arrive instantly');
    eq(at(0).why[2], 'Runs through Dec 25, the day itself.', 'and the last line says it runs through the day');
    eq([7, 14, 30].map(c => key(at(c), 'cutoff').days), [7, 14, 30], 'the cutoff segment is as long as the cutoff');
    eq(at(-3).end, '2026-12-25', 'a negative cutoff counts as none');
    eq(at('7').end, '2026-12-18', 'a number in text is read');
    eq(at('soon').end, '2026-12-25', 'a word is ignored');
    eq(at(99).end, at(30).end, 'the cutoff is capped at 30 days, as in Controls');
  }

  // 7. Smart Bidding versus manual bids (Search only).
  {
    const smart = buildSchedule({ kind: 'search', today: T, event: XMAS, orderCutoffDays: 14 });
    const manual = buildSchedule({ kind: 'search', today: T, event: XMAS, orderCutoffDays: 14, smartBidding: false });
    eq([smart.learning.days, manual.learning.days], [14, 7], 'manual bids learn in about a week, Smart Bidding in about two');
    eq([smart.days, manual.days, manual.start], [35, 28, '2026-11-14'], 'so the manual plan starts a week later');
    check(smart.learning.note !== manual.learning.note && /manual|yourself/.test(manual.learning.note), 'each has its own plain note');
    check(/manual bids/.test(manual.why[1]) && /Smart Bidding/.test(smart.why[1]), 'and its own line');
    eq(buildSchedule({ kind: 'pmax', today: T, event: XMAS, orderCutoffDays: 14, smartBidding: false }).learning.days, 21, 'only Search has a manual-bid option; Product ads ignore the flag');
  }

  // 8. Year boundary, leap day, and time zones.
  {
    const jan = buildSchedule({ kind: 'search', today: '2026-12-10', event: { label: 'New Year Sale', date: '2027-01-15' }, orderCutoffDays: 5 });
    eq([jan.verdict, jan.event.daysAway, jan.start, jan.end, jan.days], ['good', 36, '2026-12-10', '2027-01-10', 32], 'a January event from December: dates roll over the year');
    eq(jan.phases.map(p => [p.from, p.to, p.days]), [['2026-12-10', '2026-12-23', 14], ['2026-12-24', '2027-01-10', 18], ['2027-01-11', '2027-01-15', 5]], 'segments cross New Year correctly');
    eq(jan.headline, 'Runs Dec 10 to Jan 10 · 32 days', 'the headline reads across the year');
    const leap = buildSchedule({ kind: 'search', today: '2028-01-20', event: { label: 'Leap Day Sale', date: '2028-02-29' } });
    eq([leap.event.daysAway, leap.start, leap.end, leap.days], [40, '2028-01-26', '2028-02-29', 35], 'a run that ends on Feb 29 counts the leap day');
    const noLeap = buildSchedule({ kind: 'search', today: '2027-01-20', event: { label: 'Sale', date: '2027-02-28' } });
    eq([noLeap.event.daysAway, noLeap.start, noLeap.end], [39, '2027-01-25', '2027-02-28'], 'and 2027 has none');
    const run = () => JSON.stringify(buildSchedule({ kind: 'pmax', today: '2026-10-20', event: BLACK_FRIDAY, orderCutoffDays: 7 })) + JSON.stringify(buildSchedule({ kind: 'search', today: '2026-10-25', event: XMAS, orderCutoffDays: 14 })) + JSON.stringify(buildSchedule({ kind: 'search', today: '2027-03-01', event: { label: 'Spring', date: '2027-03-20' } }));
    const zone = process.env.TZ, out = [];
    for (const tz of ['UTC', 'America/Toronto', 'Pacific/Auckland', 'Pacific/Kiritimati', 'America/St_Johns']) { process.env.TZ = tz; out.push(run()); }
    if (zone === undefined) delete process.env.TZ; else process.env.TZ = zone;
    check(out.every(x => x === out[0]), 'the schedule is identical in every server time zone, across clock changes');
    check(!/Date\.now|new Date\(\s*\)|new Date\(\s*[^0-9\s\w(]/.test(SRC.replace(/\/\*[\s\S]*?\*\//g, '')) && !/Date\.now\(/.test(SRC), 'the module never reads the clock');
    const frozen = Object.freeze({ kind: 'search', today: T, event: Object.freeze({ label: 'Christmas', date: '2026-12-25' }) });
    check(buildSchedule(frozen).verdict === 'good', 'input objects are only read, never changed');
  }

  // 9. A window that has already closed, and one chosen for a draft.
  {
    const closed = buildSchedule({ kind: 'search', today: '2026-10-28', event: HALLOWEEN, orderCutoffDays: 7 });
    eq([closed.verdict, closed.days, closed.headline], ['too_short', 0, 'Last orders were due Oct 24'], 'after the last order date nothing can run');
    eq(closed.phases.map(p => [p.key, p.from, p.to]), [['cutoff', '2026-10-28', '2026-10-31']], 'the whole time left is the cutoff');
    eq(closed.why[0], 'Too late to test this for Halloween: the last gift orders that can arrive were due Oct 24.', 'and it says so');
    const over = buildSchedule({ kind: 'search', today: '2026-11-02', event: HALLOWEEN, orderCutoffDays: 7 });
    eq([over.event.daysAway, over.phases, over.headline, over.why[1]], [-2, [], 'Halloween has passed', 'Halloween was Oct 31, 2 days ago.'], 'after the event: it has passed');
    const chosen = buildSchedule({ kind: 'search', today: T, event: XMAS, orderCutoffDays: 14, startDate: '2026-11-20', endDate: '2026-12-20' });
    eq([chosen.verdict, chosen.start, chosen.end, chosen.days, chosen.phases.map(p => [p.key, p.days])], ['tight', '2026-11-20', '2026-12-20', 31, [['learning', 14], ['selling', 8], ['cutoff', 14]]], 'a chosen window that runs past the last order date is judged on the part that can sell');
    eq(chosen.why[2], 'Runs past Dec 11: gifts ordered after that may not arrive by Dec 25.', 'and warns');
    const early = buildSchedule({ kind: 'search', today: T, event: XMAS, orderCutoffDays: 14, startDate: '2026-11-10', endDate: '2026-12-04' });
    eq(early.why[2], 'Stops Dec 4; orders until Dec 11 could still arrive by Dec 25.', 'a chosen window that stops early says what it leaves');
    const asked = buildSchedule({ kind: 'search', today: T, event: XMAS, orderCutoffDays: 14, requestedDays: 20 });
    eq([asked.start, asked.end, asked.days, asked.verdict], ['2026-11-22', '2026-12-11', 20, 'tight'], 'a requested length counts back from the last order date');
    eq(buildSchedule({ kind: 'search', today: T, event: XMAS, orderCutoffDays: 14, minSellingDays: 25 }).verdict, 'tight', 'a stricter minimum selling time turns 21 days into tight');
  }

  // 10. The table of learning periods.
  {
    eq(Object.keys(LEARNING).sort(), ['demand_gen', 'display', 'pmax', 'search', 'search_manual', 'video'], 'every kind has a row, plus manual-bid Search');
    for (const [k, r] of Object.entries(LEARNING)) {
      check(r.days > 0 && r.judgeAfter >= r.days && r.sellDays > 0 && r.defaultDays > r.days, k + ': sensible day counts');
      check(r.note.length >= 40 && r.note.length <= 160 && /\.$/.test(r.note) && !/[\r\n]/.test(r.note) && !EMOJI.test(r.note), k + ': one plain sentence of at most 160 characters');
      check(!/guarantee|promise|always|will\b/i.test(r.note), k + ': typical guidance, never a promise');
    }
    check(Object.isFrozen(LEARNING) && Object.isFrozen(LEARNING.pmax), 'the table cannot be changed by a caller');
    eq([LEARNING.search.days, LEARNING.search_manual.days, LEARNING.pmax.days, LEARNING.pmax.judgeAfter], [14, 7, 21, 42], 'Search 14 (manual 7), Product ads 21 to learn and 42 to judge');
    const auto = fs.readFileSync(path.join(REPO, 'netlify/functions/googleAdsAutopilot.js'), 'utf8');
    check(/const FLOOR = 21, DEFAULT = 28/.test(auto) && LEARNING.search.defaultDays === 28, 'Search\'s undated test length agrees with planCampaign() (28 days)');
    check(/function _orderCutoffDays[^]*?Math\.min\(30,/.test(auto), 'the 30-day cap on the order cutoff is the one Controls enforces');
    const entries = JSON.parse(fs.readFileSync(path.join(REPO, 'scripts/netlify-function-entries.json'), 'utf8'));
    check(entries.modules.includes('_googleAdsSchedule.js'), 'the helper is classified as a module for the Netlify build');
  }

  // 11. Invariants over a sweep of dates, events, kinds and cutoffs.
  {
    const events = [XMAS, HALLOWEEN, BLACK_FRIDAY, { label: 'New Year Sale', date: '2027-01-15' }, { label: 'Leap Day Sale', date: '2028-02-29' }, { label: "Mother's Day", date: '2027-05-09' }];
    const rank = { good: 3, tight: 2, too_short: 1 };
    let n = 0;
    for (const ev of events) for (const kind of ['search', 'pmax', 'display', 'demand_gen', 'video']) for (const cut of [0, 7, 14, 21]) for (const smart of [true, false]) {
      let prev = 4;
      for (let d = day('2026-09-29'); d <= day(ev.date) + 3; d += 3) {
        const s = buildSchedule({ kind, today: ymd(d), event: ev, orderCutoffDays: cut, smartBidding: smart });
        n++;
        const last = day(ev.date) - cut;
        check(rank[s.verdict], 'a dated plan is good, tight or too_short');
        check(rank[s.verdict] <= prev, 'as the days pass a plan never gets better');
        prev = rank[s.verdict];
        check(day(s.start) >= d && day(s.end) >= day(s.start) && (s.days === 0 || s.days === day(s.end) - day(s.start) + 1), 'start is not in the past and days match the dates');
        if (last >= d) check(s.phases.every(p => p.days === span(p) && p.days > 0 && day(p.from) >= day(s.start) - 0 && day(p.to) <= day(ev.date)), 'every segment lies between the start and the event');
        check(s.phases.every((p, i) => i === 0 || day(p.from) > day(s.phases[i - 1].to)), 'segments do not overlap');
        check(s.why.length >= 2 && s.why.length <= 3 && s.why.every(w => w.length <= 140 && !EMOJI.test(w)) && s.basis.length <= 200 && s.headline.length <= 120, 'texts fit the contract');
        if (s.verdict === 'good') check(key(s, 'selling') && key(s, 'selling').days >= 10, 'good means at least 10 selling days');
        if (s.verdict === 'too_short') check(!key(s, 'selling') || key(s, 'selling').days < 5, 'too short means fewer than 5 selling days');
        if (s.verdict !== 'too_short') check(day(s.end) <= last || s.days === 0, 'the planned run ends by the last order date');
        eq(s.learning.endsOn, ymd(day(s.start) + s.learning.days - 1), 'learning ends where the table says');
        eq(s.judgeAfter, ymd(day(s.start) + LEARNING[kind === 'search' && !smart ? 'search_manual' : kind].judgeAfter), 'judge-after follows the table');
      }
    }
    check(n > 3000, 'the sweep ran over thousands of plans (' + n + ')');
  }

  // 12. The timeline: escaped, accessible, readable without colour.
  {
    const s = buildSchedule({ kind: 'search', today: T, event: XMAS, orderCutoffDays: 14 });
    const out = TL.html(s);
    const label = /aria-label="([^"]*)"/.exec(out);
    check(/role="img"/.test(out) && label && (out.match(/aria-label=/g) || []).length === 1, 'the picture is one role="img" with one label');
    eq(label[1], 'Schedule. Runs Nov 7 to Dec 11 · 35 days. Good fit: enough time to learn and then sell. Google learns from Nov 7 to Nov 20, then it sells from Nov 21 to Dec 11, then new orders cannot arrive in time from Dec 12 to Dec 25. Christmas is on Dec 25. Today is Sep 29.', 'the label is a full sentence a screen reader can read on its own');
    eq(TL.ariaLabel(s), label[1], 'ariaLabel() gives the same sentence');
    for (const word of ['Learning', 'Selling', 'Order cutoff', 'Today', 'Christmas', 'Good fit', 'Nov 7 to Nov 20', 'Nov 21 to Dec 11', 'Dec 12 to Dec 25']) check(out.includes(word), 'written on the picture: ' + word);
    check(out.includes('oppTl__seg--learning') && out.includes('oppTl__seg--selling') && out.includes('oppTl__seg--cutoff') && out.includes('oppTl__pin--today') && out.includes('oppTl__pin--event'), 'each part has its own mark (dotted, solid, hatched, ring, diamond)');
    const css = TL.css;
    check(/oppTl__seg--learning[^{]*\{[^}]*radial-gradient/.test(css) && /oppTl__seg--cutoff[^{]*\{[^}]*repeating-linear-gradient/.test(css) && /oppTl__pin--event[^{]*\{[^}]*rotate\(45deg\)/.test(css), 'the marks differ by pattern and shape, not colour alone');
    const lis = ((/<ul class="oppTl__why">([\s\S]*?)<\/ul>/.exec(out) || [])[1] || '').split('<li>').length - 1;
    eq(lis, 2, 'at most two reason lines are shown');
    check(out.includes(s.why[0]) && out.includes(s.why[1]) && !out.includes('Stops Dec 11'), 'the first two reasons, in order');
    // Positions: the segments sit on one line from today to the event.
    const segs = [...out.matchAll(/oppTl__seg--(\w+)" style="left:([\d.]+)%;width:([\d.]+)%"/g)].map(m => ({ k: m[1], left: +m[2], width: +m[3] }));
    eq(segs.map(x => x.k), ['learning', 'selling', 'cutoff'], 'three segments');
    check(Math.abs(segs[0].left - 39 / 88 * 100) < 0.02 && Math.abs(segs[2].left + segs[2].width - 100) < 0.05, 'they start where the run starts and end at the event');
    check(segs.every((x, i) => i === 0 || Math.abs(x.left - (segs[i - 1].left + segs[i - 1].width)) < 0.03), 'and touch');
    check(!EMOJI.test(out) && !EMOJI.test(css), 'no emoji');
    check(!/prefers-color-scheme/.test(css) && /var\(--ink,/.test(css) && /var\(--card,/.test(css) && /var\(--line,/.test(css), 'colours come from the console\'s own variables, so the timeline follows the page\'s theme');
    check(/@media \(max-width:360px\)/.test(css) && /min-width:0/.test(css) && /overflow-wrap:anywhere/.test(css), 'it is written to fit a 320px screen');
    check(!handlers(out) && !/<script|javascript:|<a\b|<img/i.test(out), 'no handlers, scripts, links or images');

    // Compact: the bar and the headline only, and a tag only when the news is not good.
    const compact = TL.html(s, { compact: true });
    check(compact.includes('oppTl--compact') && compact.includes('Runs Nov 7 to Dec 11') && !compact.includes('oppTl__why') && !compact.includes('oppTl__legend') && !compact.includes('oppTl__ends') && !compact.includes('oppTl__tag'), 'compact is the bar and the headline');
    check(/aria-label="Schedule\. Runs Nov 7/.test(compact), 'compact keeps the full label');
    const tightC = TL.html(buildSchedule({ kind: 'search', today: T, event: HALLOWEEN, orderCutoffDays: 14 }), { compact: true });
    check(tightC.includes('Tight fit') && TL.html(buildSchedule({ kind: 'search', today: '2026-10-12', event: HALLOWEEN, orderCutoffDays: 7 }), { compact: true }).includes('Too late'), 'compact still flags tight and too late');

    // Other verdicts read differently.
    const ever = TL.html(buildSchedule({ kind: 'pmax', today: T }));
    check(ever.includes('Ongoing') && ever.includes('oppTl__pin--end') && !ever.includes('oppTl__pin--event') && !ever.includes('Order cutoff'), 'an undated plan shows where it ends, not an event');
    check(/Not tied to a date/.test(TL.ariaLabel(buildSchedule({ kind: 'pmax', today: T }))), 'and its label says so');
    const late = TL.html(buildSchedule({ kind: 'search', today: '2026-10-12', event: HALLOWEEN, orderCutoffDays: 7 }));
    check(late.includes('Too late') && late.includes('Would run Oct 12 to Oct 24') && /Too late to test this in time/.test(late), 'a plan that cannot work says so in words');
    const closed = TL.ariaLabel(buildSchedule({ kind: 'search', today: '2026-11-02', event: HALLOWEEN, orderCutoffDays: 7 }));
    check(/no time left/i.test(closed), 'a closed window has a sentence too');

    // Hostile text stays text.
    const bad = JSON.parse(JSON.stringify(s));
    bad.headline = '<img src=x onerror=alert(1)> "quoted" & more';
    bad.why = ['<script>alert(2)</script>', "it's <b>bold</b>", 'third'];
    bad.event.label = '"><svg onload=alert(3)>';
    bad.phases[0].label = '<i>Learning</i>';
    bad.learning.note = '" onmouseover="alert(4)';
    const hostile = TL.html(bad);
    check(!/<img|<script|<svg|<b>bold|<i>Learning/i.test(hostile), 'markup in any text is escaped');
    check(!handlers(hostile), 'and cannot become an attribute');
    check((hostile.match(/aria-label="/g) || []).length === 1 && !/aria-label="[^"]*"[^ >]*"/.test(hostile), 'the label attribute cannot be broken out of');
    check(hostile.includes('&lt;img src=x onerror=alert(1)&gt; &quot;quoted&quot; &amp; more') && hostile.includes('it&#39;s &lt;b&gt;bold&lt;/b&gt;'), 'the text is still readable, as text');
    const junk = { today: '2026-09-29', start: '2026-10-01', end: '2026-10-02', verdict: '"><x>', phases: [{ key: 'x" onclick="', from: '2026-10-01', to: '2026-10-02' }, null, { key: 'selling', from: 'bad', to: '2026-10-02' }], event: { date: 'never' }, why: 'not a list' };
    const jout = TL.html(junk);
    check(jout.includes('oppTl--evergreen') && !jout.includes('oppTl__seg') && !/onclick|<x>/.test(jout), 'a partial or hostile schedule draws nothing it cannot vouch for');
    eq([TL.html(null), TL.html({}), TL.html({ today: 'x' }), TL.html('text'), TL.ariaLabel(null)], ['', '', '', '', ''], 'no usable schedule, nothing drawn');

    // Both runtimes: the same file in a browser-like global and injected once.
    const ctx = vm.createContext({});
    vm.runInContext(fs.readFileSync(path.join(REPO, 'assets/opportunity-timeline.js'), 'utf8'), ctx);
    check(ctx.BritesOppTimeline && typeof ctx.BritesOppTimeline.html === 'function' && ctx.BritesOppTimeline.html(s) === out, 'in a browser it is window.BritesOppTimeline and draws the same markup');
    const made = [], doc = { head: { appendChild: el => made.push(el) }, createElement: () => ({}), getElementById: id => made.find(e => e.id === id) || null };
    check(TL.injectCss(doc) === true && TL.injectCss(doc) === false && made.length === 1 && made[0].id === 'britesOppTimelineCss' && made[0].textContent === css, 'the stylesheet is added once');
    eq(TL.injectCss(null), false, 'and nothing happens without a document');
  }

  console.log('PASS ' + passed + ' opportunity-schedule checks (learning periods, order cutoff, Christmas/Halloween/Black Friday, manual bids, year and leap boundaries, time zones, timeline markup and escaping)');
  require('./suite-guard.cjs').done();
})().catch(e => { console.error(e); process.exit(1); });
