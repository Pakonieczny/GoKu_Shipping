// The Design Station's Etsy call queue, on a simulated clock: a backlog is paced under the watchdog's own limits instead
// of tripping its brake part-way through, and one failed call does not fail every call after it.
const assert = require('node:assert/strict');
const fs = require('node:fs'), vm = require('node:vm');
const html = fs.readFileSync(require('node:path').join(__dirname, '../../design-1.html'), 'utf8');
const cut = (from, to) => { const a = html.indexOf(from), b = html.indexOf(to, a); assert(a >= 0 && b > a, `missing ${from}`); return html.slice(a, b); };
const code = cut('function etsyBucketKey(t)', 'function etsyMeterSnapshot()') + cut('/** Serialises starts `spacingMs` apart', 'const isEtsyPath');
const clock = { t: Date.UTC(2026, 8, 23, 13, 0, 0) };
const c = vm.createContext({ assert, console, clock, Date: { now: () => clock.t }, sleep: async ms => { clock.t += ms; },
  ETSY_GUARD: { burstPerMinute: 60, per10Min: 150, sameOrderPer10Min: 4, brakeMs: 300000 }, etsyMeter: { minute: [], buckets: {} } });
vm.runInContext(code, c);
(async () => {
  await vm.runInContext(`(async () => {
    // one network error fails its own caller only
    const q = makeSpacingQueue(250);
    await assert.rejects(q(async () => { throw new Error('network blip'); }), /network blip/);
    assert.equal(await q(async () => 'read'), 'read', 'a later call runs after a failed one');

    // a 200-order backlog, every read counted as the fetch wrapper counts it
    const record = () => { etsyMeter.minute.push(clock.t); const k = etsyBucketKey(clock.t); etsyMeter.buckets[k] = (etsyMeter.buckets[k] || 0) + 1; };
    const run = makeSpacingQueue(250, etsyHeadroom), start = clock.t, times = [];
    await Promise.all(Array.from({ length: 200 }, () => run(async () => { record(); times.push(clock.t); })));
    assert.equal(times.length, 200, 'every read of the backlog is made');
    let worstMinute = 0; for (const t of times) worstMinute = Math.max(worstMinute, times.filter(x => x > t - 60000 && x <= t).length);
    assert(worstMinute < ETSY_GUARD.burstPerMinute, 'never more than the burst limit in a minute: ' + worstMinute);
    assert(Math.max(...Object.values(etsyMeter.buckets)) < ETSY_GUARD.per10Min, 'never more than the 10-minute limit');
    assert(clock.t - start < 25 * 60000, 'the backlog still arrives within a few intake intervals: ' + Math.round((clock.t - start) / 60000) + ' min');

    // a quiet page is not slowed down
    etsyMeter.minute = []; etsyMeter.buckets = {}; clock.t += 3600000;
    const t0 = clock.t; for (let i = 0; i < 10; i++) await run(async () => record());
    assert(clock.t - t0 <= 10 * 250, 'ten reads on a quiet page are only spaced, not held');
  })()`, c);
  console.log('Etsy pacing OK: a backlog stays under the watchdog limits, and a failed call does not block later calls');
})().catch(e => { console.error(e); process.exitCode = 1; });
