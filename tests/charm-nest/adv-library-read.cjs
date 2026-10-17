// ADV-MASTER (Paul, 15 Oct 2026: "resilient against lost internet connections"): the Library tab (it opens on Sets) when the internet goes while it is open.
// The real page in headless Chromium over the in-memory stand-in of the site; the two list calls of the Library (setList, listSheets) are answered here with two sets of two sheets, or dropped.
// What must hold: sets already on screen STAY when their refresh fails or the cloud is flagged offline, with the time of the list, why it was not refreshed and a Retry (the Sheets list always did this);
// a Library with nothing on screen says it could not load, with Try again - never an empty list that looks real; Retry reads it again.
//   PW_DIR=/opt/node-tools/node_modules node tests/charm-nest/adv-library-read.cjs
'use strict';
const assert = require('assert');
const R = require('./_masterRoutes.cjs');
let n = 0; const ok = (c, m) => { assert(c, m); n++; console.log('  ok  ' + m); };
const body = route => { try { return JSON.parse(route.request().postData() || '{}'); } catch (_) { return {}; } };
const PER = 2, SETS = 2, ORDER_PIECES = 3;
function sheet(k, i) {
  const id = `s${k}x${i}`, pool = Array.from({ length: ORDER_PIECES }, (_, q) => `${4100000000 + (k * PER + i) * ORDER_PIECES + q}_t${q}_1`), orders = pool.map(p => p.split('_')[0]);
  return { id, metal: 'gold', metalLabel: 'GF 14/20', setId: 'set-' + k, setSeq: k + 1, runId: 'run1', sheetIndex: i + 1, status: 'complete', poolIds: pool, placedCount: pool.length, charmCount: pool.length, density: .7,
    verification: { ok: true }, preview: 'https://example.com/p.png', outputs: { ai: 'https://example.com/f.ai' }, orders, label: { files: [{ path: 'qr.png', url: 'https://example.com/qr.png', payload: 'x', orders }] },
    backPool: [], engraving: {}, orderReadiness: Object.fromEntries(orders.map(o => [o, { ready: true }])), updatedAt: 1, day: '2026-10-03', folder: `GF_Oct.03.26_Set-${k + 1}_Sheet-${i + 1}` };
}
const sheets = [], rawSets = [];
for (let k = 0; k < SETS; k++) { const mine = Array.from({ length: PER }, (_, i) => sheet(k, i)); sheets.push(...mine); rawSets.push({ setId: 'set-' + k, seq: k + 1, day: '2026-10-03', runId: 'run1', sheetIds: mine.map(s => s.id), materials: ['gold'], status: 'labelled', updatedAt: 1 }); }

(async () => {
  const br = await R.browser(), page = br.page, st = { down: false, asked: 0 };
  const lib = async route => {
    const b = body(route);
    if (b.op === 'setList' || b.op === 'listSheets') { st.asked++; if (st.down) return route.abort('internetdisconnected'); return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(b.op === 'setList' ? { sets: rawSets, sheets: [] } : { sheets }) }); }
    return route.fallback();
  };
  const cards = () => page.evaluate(() => document.querySelectorAll('#libBody .setCard').length);
  const note = () => page.evaluate(() => { const s = document.querySelector('#libBody .libStale'); return s ? { text: s.textContent.replace(/\s+/g, ' '), retry: !!s.querySelector('button') } : null; });
  const leave = () => page.evaluate(() => { CN.setMode('master'); CN.setMode('library'); });
  const ready = async () => { await page.waitForFunction(() => window.CN && window.CN.S && window.RunCtl && window.DesignLink && CN.S.cloud.ok !== null, null, { timeout: 60000 }); await page.evaluate(({ station }) => { const s = CN.S.settings; s.dsOrigin = station; s.review = 'off'; s.naming = 'off'; s.notify = 'off'; s.sound = 'off'; }, { station: br.srv.stationOrigin }); };
  try {
    await page.route(/charmNestLibrary/, lib);
    await page.evaluate(() => CN.setMode('library'));
    await page.waitForFunction(() => document.querySelectorAll('#libBody .setCard').length === 2, null, { timeout: 60000 });
    ok(await cards() === 2 && !(await note()), 'the Library opens on its two sets');

    // 1 · the internet goes, then the list is refreshed: the sets stay, with the time, the reason and a Retry
    st.down = true; await page.evaluate(() => { CN.S.library.loadedAt = 0; }); await leave();
    await page.waitForFunction(() => document.querySelector('#libBody .libStale'), null, { timeout: 20000 });
    let nt = await note();
    ok(await cards() === 2 && nt && /not refreshed/.test(nt.text) && nt.retry && !/Could not load/.test(await page.evaluate(() => document.getElementById('libBody').innerText)), 'a refresh that fails keeps the 2 sets on screen and says so: "' + (nt && nt.text) + '"');

    // 2 · the connection is back: Retry reads the list again and the note goes
    st.down = false; const before = st.asked;
    await page.evaluate(() => document.querySelector('#libBody .libStale button').click());
    await page.waitForFunction(() => !document.querySelector('#libBody .libStale') && document.querySelectorAll('#libBody .setCard').length === 2, null, { timeout: 20000 });
    ok(st.asked > before && await cards() === 2, 'Retry reads it again: the 2 sets, no note');

    // 3 · the cloud is flagged offline (a probe failed) while the sets are shown: they stay, with the reason
    await page.evaluate(() => { CN.S.cloud.ok = false; }); await leave();
    await page.waitForFunction(() => document.querySelector('#libBody .libStale'), null, { timeout: 20000 });
    nt = await note();
    ok(await cards() === 2 && nt && /the cloud is offline/.test(nt.text) && nt.retry, 'offline: the sets stay on screen, "' + (nt && nt.text) + '"');
    await page.evaluate(() => { CN.S.cloud.ok = true; });

    // 4 · a Library with nothing on screen (a fresh page, the internet down): it says it could not load, with Try again - not an empty list
    st.down = true; await page.reload(); await ready();
    await page.evaluate(() => CN.setMode('library'));
    // (the reloaded page opens in the Library, so its own first read (the cloud check) and the one asked for just above each paint "Loading sets…" then the error, one after the other:
    //  wait until the error has stood still for a moment, or this reads the first error while the second read has already put "Loading sets…" over it)
    await page.waitForFunction(() => { const t = document.getElementById('libBody').innerText, now = performance.now(); if (t !== window.__libT) { window.__libT = t; window.__libAt = now; return false; } return /Could not load/.test(t) && now - window.__libAt > 800; }, null, { timeout: 30000, polling: 100 });
    ok(await cards() === 0 && await page.evaluate(() => !!document.querySelector('#libBody .libEmpty button')) && !/No sets yet/.test(await page.evaluate(() => document.getElementById('libBody').innerText)), 'nothing on screen: "Could not load sets", a Try again button, never "No sets yet"');
    st.down = false; await page.evaluate(() => document.querySelector('#libBody .libEmpty button').click());
    await page.waitForFunction(() => document.querySelectorAll('#libBody .setCard').length === 2, null, { timeout: 30000 });
    ok(await cards() === 2, 'Try again reads it: the 2 sets');
    const bad = br.errors.filter(e => !/ERR_INTERNET_DISCONNECTED|Failed to load resource|net::ERR|status of 500/.test(e));
    ok(bad.length === 0, 'no page error: ' + bad.join(' | '));
  } finally { await br.close(); }
  console.log(`adv-library-read: ${n} checks passed`);
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
