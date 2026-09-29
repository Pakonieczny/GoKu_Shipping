/* A thin timeline for one recommended campaign: today, when it starts and stops, how long Google needs to learn, and,
   for a dated campaign, the event itself and the order cutoff before it. It draws the `schedule` object built by
   netlify/functions/_googleAdsSchedule.js (contract: plans/pmax-research-contract-2026-09-29.md, ADDENDUM 17:10) and
   never computes a date of its own beyond laying the given ones on a line.

   Browser:  window.BritesOppTimeline.injectCss(document);  el.innerHTML = BritesOppTimeline.html(schedule, { compact: false });
   Node:     require('./opportunity-timeline.js').html(schedule)

   Every string is escaped, there are no inline event handlers, and the picture is one role="img" with a full-sentence
   label. Learning is drawn dotted, selling solid, the order cutoff hatched, today as a ring and the event as a diamond,
   and each has a written label, so nothing depends on colour. All colours come from the console's own variables
   (--ink, --line, --card, ...) with light fallbacks, so it follows the page if the page changes theme. */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.BritesOppTimeline = factory();
})(typeof globalThis !== 'undefined' ? globalThis : this, function () {
  'use strict';

  var MS_DAY = 86400000;
  var MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  var PHASE_KEYS = ['learning', 'selling', 'cutoff'];
  var PHASE_WORDS = { learning: 'Learning', selling: 'Selling', cutoff: 'Order cutoff' };
  var PHASE_SAY = { learning: 'Google learns', selling: 'it sells', cutoff: 'new orders cannot arrive in time' };
  var VERDICTS = {
    good: { tag: 'Good fit', say: 'Good fit: enough time to learn and then sell.' },
    tight: { tag: 'Tight fit', say: 'Tight fit: little selling time is left after learning.' },
    too_short: { tag: 'Too late', say: 'Too late to test this in time.' },
    evergreen: { tag: 'Ongoing', say: 'Not tied to a date.' }
  };

  function esc(v) {
    return String(v == null ? '' : v).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; });
  }
  function dn(ymd) {
    var m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(ymd == null ? '' : ymd));
    if (!m) return null;
    var t = Date.UTC(+m[1], +m[2] - 1, +m[3]), d = new Date(t);
    return d.getUTCFullYear() === +m[1] && d.getUTCMonth() === +m[2] - 1 && d.getUTCDate() === +m[3] ? t / MS_DAY : null;
  }
  function fmt(n) { var d = new Date(n * MS_DAY); return MONTHS[d.getUTCMonth()] + ' ' + d.getUTCDate(); }
  function span(a, b) { return a === b ? fmt(a) : fmt(a) + ' to ' + fmt(b); }
  function pct(x) { return (Math.round(x * 100) / 100) + '%'; }
  function text(v, max) { var s = String(v == null ? '' : v).replace(/\s+/g, ' ').trim(); return s.length > max ? s.slice(0, max - 1) + '…' : s; }

  /* Reads the schedule defensively: a saved or partial one must never throw or draw a wrong date. */
  function read(s) {
    if (!s || typeof s !== 'object') return null;
    var today = dn(s.today), start = dn(s.start), end = dn(s.end);
    if (today == null || start == null || end == null) return null;
    var ev = s.event && typeof s.event === 'object' ? { day: dn(s.event.date), label: text(s.event.label, 40) || 'The event' } : null;
    if (ev && ev.day == null) ev = null;
    var phases = (Array.isArray(s.phases) ? s.phases : []).map(function (p) {
      var from = p && dn(p.from), to = p && dn(p.to);
      return p && PHASE_KEYS.indexOf(p.key) >= 0 && from != null && to != null && to >= from ? { key: p.key, from: from, to: to, label: text(p.label, 24) || PHASE_WORDS[p.key] } : null;
    }).filter(Boolean);
    var verdict = VERDICTS[s.verdict] ? s.verdict : 'evergreen';
    return { today: today, start: start, end: end, ev: ev, phases: phases, verdict: verdict, days: Number(s.days),
      headline: text(s.headline, 120), why: (Array.isArray(s.why) ? s.why : []).map(function (w) { return text(w, 140); }).filter(Boolean),
      learningNote: s.learning && s.learning.note ? text(s.learning.note, 160) : '' };
  }

  /* The full-sentence alternative to the picture. */
  function ariaLabel(schedule) {
    var r = read(schedule);
    if (!r) return '';
    var bits = [r.headline || ('Runs ' + span(r.start, r.end)), VERDICTS[r.verdict].say];
    var clauses = r.phases.map(function (p) { return PHASE_SAY[p.key] + ' from ' + span(p.from, p.to); });
    if (clauses.length) bits.push(clauses.join(', then ') + '.');
    else if (r.verdict === 'too_short') bits.push('There is no time left to run it.');
    if (r.ev) bits.push(r.ev.label + ' is on ' + fmt(r.ev.day) + '.');
    bits.push('Today is ' + fmt(r.today) + '.');
    return 'Schedule. ' + bits.map(function (b) { return String(b).replace(/\.+$/, '') + '.'; }).join(' ');
  }

  /* html(schedule, { compact }) -> an escaped HTML string, or '' when there is no usable schedule. */
  function html(schedule, options) {
    var r = read(schedule), compact = !!(options && options.compact);
    if (!r) return '';
    var d0 = Math.min(r.today, r.start);
    var d1 = r.ev ? Math.max(r.ev.day, r.end) : r.end + Math.max(2, Math.round((r.end - d0 + 1) * 0.1));
    r.phases.forEach(function (p) { d1 = Math.max(d1, p.to); });
    d1 = Math.max(d1, d0 + (r.ev ? 1 : 6));
    var total = d1 - d0 + 1;
    var at = function (day) { return (day - d0) / total * 100; };

    var segs = r.phases.map(function (p) {
      return '<span class="oppTl__seg oppTl__seg--' + p.key + '" style="left:' + pct(at(p.from)) + ';width:' + pct((p.to - p.from + 1) / total * 100) + '"></span>';
    }).join('');
    var pins = '<span class="oppTl__pin oppTl__pin--today" style="left:' + pct(at(r.today)) + '"></span>';
    if (r.ev && r.ev.day >= d0) pins += '<span class="oppTl__pin oppTl__pin--event" style="left:' + pct(at(r.ev.day + 1)) + '"></span>';
    else if (!r.ev && r.verdict === 'evergreen') pins += '<span class="oppTl__pin oppTl__pin--end" style="left:' + pct(at(r.end + 1)) + '"></span>';
    var track = '<div class="oppTl__track"><div class="oppTl__bar">' + segs + '</div>' + pins + '</div>';

    var tag = VERDICTS[r.verdict].tag;
    var showTag = !compact || r.verdict === 'tight' || r.verdict === 'too_short';
    var top = '<div class="oppTl__top"><span class="oppTl__head">' + esc(r.headline || ('Runs ' + span(r.start, r.end))) + '</span>' +
      (showTag ? '<span class="oppTl__tag">' + esc(tag) + '</span>' : '') + '</div>';

    var figure;
    if (compact) {
      figure = track;
    } else {
      var endsRight = r.ev ? '<span>' + esc(r.ev.label) + ' · <b>' + esc(fmt(r.ev.day)) + '</b></span>'
        : (r.verdict === 'evergreen' ? '<span>Ends · <b>' + esc(fmt(r.end)) + '</b></span>' : '');
      // With no event the right label sits under the end of the run, not the end of the margin beyond it.
      var padRight = !r.ev ? ' style="padding-right:' + pct(Math.max(0, 100 - at(r.end + 1))) + '"' : '';
      var ends = '<div class="oppTl__ends"' + padRight + '><span>Today · <b>' + esc(fmt(r.today)) + '</b></span>' + endsRight + '</div>';
      var legend = r.phases.length ? '<ul class="oppTl__legend">' + r.phases.map(function (p) {
        var title = p.key === 'learning' && r.learningNote ? ' title="' + esc(r.learningNote) + '"' : '';
        return '<li' + title + '><i class="oppTl__sw oppTl__sw--' + p.key + '" aria-hidden="true"></i><span class="oppTl__lab">' + esc(p.label) + '</span> <span class="oppTl__dt">' + esc(span(p.from, p.to)) + '</span></li>';
      }).join('') + '</ul>' : '';
      figure = ends + track + legend;
    }

    var why = !compact && r.why.length ? '<ul class="oppTl__why">' + r.why.slice(0, 2).map(function (w) { return '<li>' + esc(w) + '</li>'; }).join('') + '</ul>' : '';
    return '<div class="oppTl oppTl--' + r.verdict + (compact ? ' oppTl--compact' : '') + '" data-verdict="' + r.verdict + '">' + top +
      '<div class="oppTl__fig" role="img" aria-label="' + esc(ariaLabel(schedule)) + '">' + figure + '</div>' + why + '</div>';
  }

  var css = [
    '.oppTl{--tl-ink:var(--ink,#1c1a17);--tl-ink2:var(--ink-70,#5b554c);--tl-mute:var(--ink-45,#938c80);--tl-faint:var(--ink-25,#c4bdb0);--tl-line:var(--line,#e4ddd0);--tl-line2:var(--line-2,#efe9dd);--tl-card:var(--card,#fffefb);',
    '--tl-gold:var(--opp-gold,#c8922f);--tl-ok:#3c5a39;--tl-warn:#7a5a1d;--tl-bad:#8a3a26;--tl-flat:var(--tl-ink2);',
    'display:grid;gap:11px;min-width:0;max-width:100%;font-family:var(--sans,-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,system-ui,sans-serif);color:var(--tl-ink);text-align:left}',
    '[data-theme="dark"] .oppTl{--tl-ok:#a9c9a4;--tl-warn:#e2c27a;--tl-bad:#e6a08c}',
    '.oppTl *{box-sizing:border-box}',
    '.oppTl--compact{gap:7px}',
    '.oppTl__top{display:flex;align-items:baseline;justify-content:space-between;flex-wrap:wrap;gap:4px 10px;min-width:0}',
    '.oppTl__head{min-width:0;font-size:13px;font-weight:600;line-height:1.35;letter-spacing:.005em;font-variant-numeric:tabular-nums;overflow-wrap:anywhere}',
    '.oppTl--compact .oppTl__head{font-size:12.5px}',
    '.oppTl__tag{flex:none;padding:2px 8px;border:1px solid currentColor;border-radius:99px;font-size:9.5px;font-weight:700;letter-spacing:.14em;line-height:1.4;text-transform:uppercase;white-space:nowrap}',
    '.oppTl__tag{border-color:color-mix(in srgb,currentColor 42%,transparent)}',
    '.oppTl--good .oppTl__tag{color:var(--tl-ok)}.oppTl--tight .oppTl__tag{color:var(--tl-warn)}.oppTl--too_short .oppTl__tag{color:var(--tl-bad)}.oppTl--evergreen .oppTl__tag{color:var(--tl-flat)}',
    '.oppTl__fig{display:grid;gap:7px;min-width:0}',
    '.oppTl__ends{display:flex;flex-wrap:wrap;justify-content:space-between;gap:2px 12px;font-size:10px;font-weight:700;line-height:1.4;letter-spacing:.14em;text-transform:uppercase;color:var(--tl-mute)}',
    '.oppTl__ends b{font-weight:700;color:var(--tl-ink2)}',
    '.oppTl__ends>span+span{margin-left:auto;text-align:right}',
    '.oppTl__track{position:relative;margin:4px 6px 3px}',
    '.oppTl__bar{position:relative;height:8px;border-radius:99px;overflow:hidden;background-color:var(--tl-line2);box-shadow:inset 0 0 0 1px var(--tl-line)}',
    '.oppTl--compact .oppTl__bar{height:6px}',
    '.oppTl__seg{position:absolute;top:0;bottom:0;min-width:3px}',
    /* learning: soft and dotted; selling: solid; order cutoff: hatched, no fill */
    '.oppTl__seg--learning,.oppTl__sw--learning{background-color:#f0dfb4;background-color:color-mix(in srgb,var(--tl-gold) 34%,var(--tl-card));background-image:radial-gradient(circle,var(--tl-gold) 0.8px,transparent 1.1px);background-size:4px 4px}',
    '.oppTl__seg--selling,.oppTl__sw--selling{background-color:var(--tl-gold)}',
    '.oppTl__seg--cutoff,.oppTl__sw--cutoff{background-color:transparent;background-image:repeating-linear-gradient(135deg,var(--tl-faint) 0,var(--tl-faint) 1.4px,transparent 1.4px,transparent 4.5px)}',
    '.oppTl--too_short .oppTl__seg{opacity:.55}',
    '.oppTl__pin{position:absolute;top:50%;width:11px;height:11px;transform:translate(-50%,-50%);border:2px solid var(--tl-ink);background:var(--tl-card)}',
    '.oppTl__pin--today{border-radius:50%}',
    '.oppTl__pin--event{transform:translate(-50%,-50%) rotate(45deg);background:var(--tl-gold);border-radius:2px}',
    '.oppTl__pin--end{width:2px;height:15px;border:0;border-radius:2px;background:var(--tl-ink)}',
    '.oppTl__legend{display:flex;flex-wrap:wrap;gap:5px 18px;margin:0;padding:0;list-style:none;font-size:11.5px;line-height:1.4;color:var(--tl-ink2)}',
    '.oppTl__legend li{display:inline-flex;align-items:center;flex-wrap:wrap;gap:0 6px;min-width:0}',
    '.oppTl__sw{display:inline-block;flex:none;width:16px;height:8px;border-radius:3px;box-shadow:inset 0 0 0 1px var(--tl-line)}',
    '.oppTl__lab{font-size:9.5px;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:var(--tl-mute)}',
    '.oppTl__dt{font-family:var(--mono,ui-monospace,"SF Mono",Menlo,Consolas,monospace);font-size:11px;font-variant-numeric:tabular-nums;color:var(--tl-ink2)}',
    '.oppTl__why{display:grid;gap:5px;margin:0;padding:0;list-style:none;font-size:12px;line-height:1.5;color:var(--tl-ink2)}',
    '.oppTl__why li{position:relative;padding-left:14px;overflow-wrap:anywhere}',
    '.oppTl__why li::before{content:"";position:absolute;left:0;top:.78em;width:7px;height:1px;background:var(--tl-gold)}',
    '@media (forced-colors:active){.oppTl__seg,.oppTl__sw{border:1px solid CanvasText}.oppTl__pin{border-color:CanvasText}}',
    '@media (max-width:360px){.oppTl__legend{gap:5px 12px}.oppTl__head{font-size:12.5px}}'
  ].join('\n');

  /* Adds the stylesheet once per document; safe to call again. */
  function injectCss(doc) {
    doc = doc || (typeof document !== 'undefined' ? document : null);
    if (!doc || !doc.createElement || !doc.head) return false;
    if (doc.getElementById && doc.getElementById('britesOppTimelineCss')) return false;
    var el = doc.createElement('style');
    el.id = 'britesOppTimelineCss';
    el.textContent = css;
    doc.head.appendChild(el);
    return true;
  }

  return { html: html, css: css, injectCss: injectCss, ariaLabel: ariaLabel, VERSION: 1 };
});
