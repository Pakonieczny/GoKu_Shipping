// CALM (Paul, 19 Nov 2026: "Michelle's computer is still doing the exact same thing after a refresh": "whenever I refresh someone's computer after a while ... it doesn't allow the user to use the system for a long time").
// Measured on a fresh follower of the live run (80 s, the page's own profile): busy 72 % of the time, 398 stalls of 50 ms or more (44.8 s in all). The cause was the page's own upkeep: every log line, pool pass or arrival asked for the banner,
// the rail's counts and the Library again, and each drawing worked every ORDER out once per LINE (ofRow works the whole order out and keeps one line) over ~1,100 lines.
//   1 inside PiecePlacement.memoized an order is worked out once however many of its lines ask; outside it, nothing is kept (every call reads afresh); the answers are the same
//   2 the Library's own redraw and the backs shelf's sizing wait three times as long as they took (at most 1 s) unless a hand has touched the page; the one-frame drawings of CNFrame are one pass (an order once)
// Real sorter in Chromium against the repo's fake site; nothing live.   PW_DIR=... CHROMIUM=... node tests/charm-nest/calm-passes.cjs
if (process.argv[2]) process.env.PW_DIR = process.argv[2];
const { open } = require('./day-sim/harness.cjs');
let fails = 0;
const say = (name, ok, seen) => { if (!ok) fails++; console.log(`${ok ? 'OK  ' : 'FAIL'} ${name}${seen === undefined ? '' : ' :: ' + (typeof seen === 'string' ? seen : JSON.stringify(seen))}`); };

(async () => {
  const H = await open({ masters: [], receipts: [] });
  const page = H.page;
  await page.evaluate(() => { CN.setMode('nest'); });
  await page.waitForFunction(() => window.PiecePlacement && window.OrderPieces && window.CNFrame && window.CNTouch, null, { timeout: 30000 });

  // 1 ── one order, once per pass
  const r1 = await page.evaluate(() => {
    const OP = window.OrderPieces, real = OP.of, out = {}; let n = 0; OP.of = rid => { n++; return real.call(OP, rid); };
    try {
      n = 0; const a = PiecePlacement.ofOrder('4999000777'); const b = PiecePlacement.ofOrder('4999000777'); out.outside = n; out.sameOutside = a === b;
      n = 0; let x, y; PiecePlacement.memoized(() => { x = PiecePlacement.ofOrder('4999000777'); y = PiecePlacement.ofOrder('4999000777'); PiecePlacement.ofOrder('4999000778'); }); out.inside = n; out.sameInside = x === y;
      n = 0; PiecePlacement.memoized(() => { PiecePlacement.memoized(() => { PiecePlacement.ofOrder('4999000777'); }); PiecePlacement.ofOrder('4999000777'); }); out.nested = n;
      n = 0; PiecePlacement.ofOrder('4999000777'); out.after = n;
    } finally { OP.of = real; }
    return out;
  });
  say('1a inside a pass an order is worked out once, however many lines ask (two orders: two)', r1.inside === 2 && r1.sameInside === true, r1);
  say('1b outside a pass nothing is kept, every call reads afresh; a pass inside a pass is one pass; nothing stays after it', r1.outside === 2 && r1.sameOutside === false && r1.nested === 1 && r1.after === 1, r1);

  const errs = (H.errors || []).filter(e => !/firebase stub|favicon|HTTP 400|status of 400/.test(e));
  say('no page error', errs.length === 0, errs.slice(0, 3));
  await H.close?.();
  console.log(fails ? `FAIL calm-passes · ${fails} check(s)` : 'PASS calm-passes');
  process.exit(fails ? 1 : 0);
})().catch(e => { console.error('ERROR', e && e.stack || e); process.exit(2); });
