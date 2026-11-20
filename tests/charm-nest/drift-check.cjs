// DRIFT (Paul, 18 Nov 2026: "Make computers report how far behind the cloud they are ... rebuild itself ... small 'out of date' mark ... see in one list which computer is behind", and "I can't have it screw up my
// production process"): charm-nest-drift.js and the three cloud calls it uses. Real sorter in Chromium against the repo's fake site; nothing live.
//   SERVER  1 sheetRevsGet answers the run's sheets (archived left out) with the few fields a computer compares   2 computerReport / computerList round trip, a bad computer is refused, a report is cleaned
//           3 none of the three moves the placement counter or names a key on the changes feed (nothing for another computer to read)
//   PAGE    4 a FOLLOWING computer rebuilds a clean sheet that is behind, through the feed's own path (_changed), and only that one: a sheet with a change of its own waiting, a cut sheet, one already current and one never read are left alone
//           5 what it found is reported (counts, the sheet, who, role) and no mark shows once nothing is behind   6 a sheet that cannot be rebuilt shows "Out of date" and is not tried again at once
//           7 a sheet the cloud lacks / has and this page lacks is listed again (Recall.follow), once, not at every check   8 the computer that CARRIES the run is measured and reported, never rebuilt
//           9 "report" measures and reports only, "off" does nothing at all   10 a cloud that does not know the calls stops it, without an error   11 Settings → Computers lists the computers with who is behind
//   PW_DIR=... CHROMIUM=... node tests/charm-nest/drift-check.cjs
const path = require('path');
if (process.argv[2]) process.env.PW_DIR = process.argv[2];
const { open } = require('./day-sim/harness.cjs');
let fails = 0;
const say = (name, ok, seen) => { if (!ok) fails++; console.log(`${ok ? 'OK  ' : 'FAIL'} ${name}${seen === undefined ? '' : ' :: ' + (typeof seen === 'string' ? seen : JSON.stringify(seen))}`); };
const RUN = 'run-drift-t', SHEETS = 'Charm_Nest_Sheets';

(async () => {
  const H = await open({ masters: [], receipts: [] });
  const { st, page } = H;
  await page.evaluate(() => { CN.setMode('nest'); });
  await page.waitForFunction(() => window.CharmNestDrift && window.CharmNestCloudSheets && window.CharmNestRunFollow, null, { timeout: 30000 });

  // ── SERVER ──
  const now = Date.now();
  st.put(SHEETS, 'gf-a', { runId: RUN, layoutRev: 5, charmCount: 2, draft: true, fileBase: 'GF_A', setId: 'set-1', updatedAt: now - 1000, metal: 'gold' });
  st.put(SHEETS, 'gf-a-old', { runId: RUN, layoutRev: 2, charmCount: 1, draft: true, fileBase: 'GF_A', setId: 'set-1', updatedAt: now - 900000, metal: 'gold' });
  st.put(SHEETS, 'rg-cut', { runId: RUN, layoutRev: 9, charmCount: 4, fileBase: 'RG_CUT', roseCutAt: now - 5000, updatedAt: now - 5000, metal: 'rose' });
  st.put(SHEETS, 'gone', { runId: RUN, layoutRev: 3, charmCount: 1, fileBase: 'GONE', archived: true, updatedAt: now, metal: 'gold' });
  st.put(SHEETS, 'other-run', { runId: 'run-other', layoutRev: 3, charmCount: 1, fileBase: 'OTHER', updatedAt: now, metal: 'gold' });
  const gen0 = JSON.stringify(st.doc('Charm_Nest_Rev', 'placement') || null), feedSeen = [];
  page.on('response', r => { if (/charmNestLibrary/.test(r.url())) { const h = r.headers()['x-cn-feed']; if (h) feedSeen.push(h); } });
  const s1 = await page.evaluate(RUN => CN.api('charmNestLibrary', { op: 'sheetRevsGet', runId: RUN }), RUN);
  const ids = (s1.sheets || []).map(x => x.id).sort();
  const a = (s1.sheets || []).find(x => x.id === 'gf-a'), c = (s1.sheets || []).find(x => x.id === 'rg-cut');
  say('1 sheetRevsGet answers the run\'s sheets, leaves out the archived one and other runs', JSON.stringify(ids) === JSON.stringify(['gf-a', 'gf-a-old', 'rg-cut']), ids);
  say('1b it carries revision, pieces, draft, cut, name and set', a && a.rev === 5 && a.ch === 2 && a.d === 1 && a.fb === 'GF_A' && a.set === 'set-1' && c && c.cut === 1, [a, c]);
  const bad = await page.evaluate(() => CN.api('charmNestLibrary', { op: 'computerReport', dev: 'x y', report: {} }).then(r => r, e => ({ error: String(e.message || e) })));
  say('2a a bad computer id is refused', !!bad.error, bad);
  const put = await page.evaluate(() => CN.api('charmNestLibrary', { op: 'computerReport', dev: 'dtestcomp01', by: 'Tester_T', report: { role: 'follower', runId: 'r', build: 'b', behindN: 2, stillN: 1, still: ['GF_A 5→9'], freezeMs: 4200, freezes: 3, feed: 'push', mode: 'on', junk: 'x'.repeat(5000), checked: -4 } }));
  const lst = await page.evaluate(() => CN.api('charmNestLibrary', { op: 'computerList' }));
  const me = (lst.computers || []).find(x => x.dev === 'dtestcomp01');
  say('2b a report is stored and listed with how long ago', put.ok && me && me.by === 'Tester_T' && me.stillN === 1 && me.freezeMs === 4200 && me.ageS < 30 && me.role === 'follower', me);
  say('2c a report holds no stray field and no negative count', me && !('junk' in me) && me.checked === 0 && !('expireAt' in me), me && Object.keys(me));
  say('3 none of them moves the placement counter or names a key on the changes feed', JSON.stringify(st.doc('Charm_Nest_Rev', 'placement') || null) === gen0 && feedSeen.length === 0, { feedSeen });

  // ── PAGE ──
  const setup = async opts => page.evaluate(o => {
    const RUN = 'run-drift-t', CS = CharmNestCloudSheets, D = CharmNestDrift, prim = S.sheets.gold;
    window.__calls = { api: [], changed: [], follow: [], reports: [] };
    const cloud = o.cloud || [{ id: 'p-behind', rev: 9, ch: 3, d: 1, cut: 0, fb: 'B', set: '', upd: 5 }, { id: 'p-dirty', rev: 8, ch: 3, d: 1, cut: 0, fb: 'D', set: '', upd: 5 }, { id: 'p-cut', rev: 8, ch: 3, d: 0, cut: 1, fb: 'C', set: '', upd: 5 }, { id: 'p-same', rev: 4, ch: 3, d: 1, cut: 0, fb: 'S', set: '', upd: 5 }, { id: 'p-new', rev: 7, ch: 3, d: 1, cut: 0, fb: 'N', set: '', upd: 5 }];
    window.__realApi = window.__realApi || CN.api; window.__realChanged = window.__realChanged || CS._changed; window.__realFollow = window.__realFollow || Recall.follow; window.__realState = window.__realState || Recall.state;
    CN.api = async (fn, body) => { window.__calls.api.push(body && body.op); if (body && body.op === 'sheetRevsGet') return o.unsupported ? { error: 'unknown op sheetRevsGet' } : { sheets: cloud }; if (body && body.op === 'computerReport') { window.__calls.reports.push(body.report); return { ok: true }; } if (body && body.op === 'computerList') return { computers: o.list || [] }; return {}; };
    CS._changed = async keys => { window.__calls.changed.push(keys); const id = keys[0].slice(2), p = allSheets().find(x => x.sheetId === id), c = cloud.find(x => x.id === id); if (p && c && !o.stuck) { p.layoutRev = c.rev; } };
    Recall.follow = async (id, op) => { window.__calls.follow.push([id, !!(op && op.force)]); };
    Recall.state = () => ({ runId: o.opened ? RUN : null, setId: null, live: null }); CharmNestRunFollow.state = () => ({ runId: o.noRun ? null : RUN, retired: o.retired || [] }); D._quiet(); document.querySelectorAll('dialog[data-t]').forEach(d => d.remove()); if (o.dialog) { const d = document.createElement('dialog'); d.dataset.t = '1'; document.body.appendChild(d); d.setAttribute('open', ''); }
    B.run = o.carrier ? { runId: RUN, status: 'running' } : null;
    const mk = (id, extra) => Object.assign({ metal: 'gold', sheetId: id, runId: RUN, followed: true, status: 'complete', charms: [], placements: [], layoutRev: 4, persistedDone: true, dirty: false, recalled: null, roseCutAt: null, laserDoneAt: null, problem: null }, extra || {});
    prim.pages = [prim]; prim.active = 0;
    const list = [mk('p-behind'), mk('p-dirty', { layoutRev: 1, dirty: true }), mk('p-cut', { layoutRev: 1 }), mk('p-same'), mk('p-new', { layoutRev: undefined }), ...(o.extra || []).map(id => mk(id))];
    Object.assign(prim, list[0]); for (const p of list.slice(1)) { p.el = null; prim.pages.push(p); }
    localStorage.setItem('cn.drift', o.mode || 'on');
    const pill = document.getElementById('cnDriftPill'); if (pill) pill.hidden = true;
    return true;
  }, opts || {});
  const out = () => page.evaluate(() => { const p = document.getElementById('cnDriftPill'); return { calls: window.__calls, pill: !!(p && !p.hidden), pillText: p ? p.textContent : '', rev: Object.fromEntries(allSheets().filter(x => x.sheetId).map(x => [x.sheetId, x.layoutRev])) }; });
  const run = (why = 'test') => page.evaluate(w => CharmNestDrift.check(w), why);

  await setup();
  const r4 = await run(); const o4 = await out();
  say('4a the one clean sheet that is behind is read again through the feed\'s own path', JSON.stringify(o4.calls.changed) === JSON.stringify([['s:p-behind']]), o4.calls.changed);
  say('4b a sheet with a change of its own waiting, a cut one, a current one and one never read are left alone', o4.rev['p-dirty'] === 1 && o4.rev['p-cut'] === 1 && o4.rev['p-same'] === 4 && o4.rev['p-new'] === undefined && o4.rev['p-behind'] === 9, o4.rev);
  const rp = o4.calls.reports[0] || {};
  say('5a it reports what it found: role, counts, the sheet healed', rp.role === 'follower' && rp.behindN === 1 && rp.rebuilt === 1 && rp.stillN === 0 && rp.inHandN === 1 && rp.unverifiedN === 1 && /^(p-behind|B) /.test((rp.healed || [])[0] || ''), rp);
  // (SHEETNAMES, 20 Nov 2026: a following computer takes each sheet's name, set and draft mark from its record: the healed sheet is told by the cloud's name, B)
  say('5a2 a follower takes the sheets\' names from their records and says how many', rp.misnamedN >= 1 && rp.renamed === rp.misnamedN, { misnamedN: rp.misnamedN, renamed: rp.renamed, misnamed: rp.misnamed });
  say('5b nothing is behind afterwards: no mark', !o4.pill, o4.pillText);
  const mine = o4.calls.api.filter(op => /^(sheetRevsGet|computerReport|computerList|getSheet|getMany|listSheets|runGet)$/.test(op));   // (the page's other upkeep, shares and marks, runs on its own beat and is not this check's)
  say('4c the check asks the cloud for one sheet list and tells it one report, and reads no sheet record itself', JSON.stringify(mine) === JSON.stringify(['sheetRevsGet', 'computerReport']), o4.calls.api);

  await setup({ stuck: true });
  await run(); let o6 = await out();
  say('6a a sheet that cannot be rebuilt shows the mark', o6.pill && /Out of date · 1/.test(o6.pillText), o6.pillText);
  await run(); o6 = await out();
  say('6b and is not tried again at the next check', o6.calls.changed.length === 1, o6.calls.changed);

  await setup({ extra: ['p-ghost'], cloud: [{ id: 'p-behind', rev: 4, ch: 3, d: 1, cut: 0, fb: 'B', set: '', upd: 5 }, { id: 'p-cloudonly', rev: 4, ch: 3, d: 1, cut: 0, fb: 'X', set: '', upd: 5 }] });
  await run(); let o7 = await out();
  say('7a sheets the page lacks or has extra are listed again, once', JSON.stringify(o7.calls.follow) === JSON.stringify([['run-drift-t', true]]), o7.calls.follow);
  await run(); o7 = await out();
  say('7b not again at the next check', o7.calls.follow.length === 1, o7.calls.follow);

  await setup({ carrier: true });
  await run(); const o8a = await out(); await run(); const o8 = await out();
  say('8a the computer that carries the run is measured and reported, never rebuilt', o8.calls.changed.length === 0 && o8.calls.follow.length === 0 && (o8.calls.reports[0] || {}).role === 'carrier' && (o8.calls.reports[0] || {}).behindN === 1, { changed: o8.calls.changed, rep: o8.calls.reports[0] && [o8.calls.reports[0].role, o8.calls.reports[0].behindN] });
  say('8b its mark needs the same sheet behind at two looks in a row', !o8a.pill && o8.pill && o8.calls.reports[1].stillN === 1 && o8.calls.reports[0].stillN === 0, [o8a.pill, o8.pill, o8.calls.reports.map(r => r.stillN)]);

  await setup({ retired: ['p-cloudonly'], cloud: ['p-behind', 'p-dirty', 'p-cut', 'p-same', 'p-new'].map((id, i) => ({ id, rev: id === 'p-cut' ? 1 : 1, ch: 3, d: 1, cut: id === 'p-cut' ? 1 : 0, fb: 'F' + i, set: '', upd: 5 })).concat([{ id: 'p-cloudonly', rev: 4, ch: 3, d: 1, cut: 0, fb: 'X', set: '', upd: 5 }]) });
  await run(); const o8c = await out();
  say('8c a sheet the run has let go of is not "missing" on this page', o8c.calls.follow.length === 0 && o8c.calls.reports[0].missingN === 0 && !o8c.pill, [o8c.calls.follow, o8c.calls.reports[0].missingN]);
  await setup({ dialog: true, extra: ['p-ghost'], cloud: [{ id: 'p-behind', rev: 4, ch: 3, d: 1, cut: 0, fb: 'B', set: '', upd: 5 }] });
  await run(); const o8d = await out();
  say('8d the cards are not built again while a window is open', o8d.calls.follow.length === 0 && o8d.pill, [o8d.calls.follow, o8d.pill]);

  await setup({ opened: true, extra: ['p-ghost'], cloud: [{ id: 'p-behind', rev: 9, ch: 3, d: 1, cut: 0, fb: 'B', set: '', upd: 5 }] });
  await run(); const o8e = await out();
  say('8e a set opened on purpose is not a computer that is behind: nothing rebuilt, no mark, and the report says why', o8e.calls.changed.length === 0 && o8e.calls.follow.length === 0 && !o8e.pill && /opened on purpose/.test(o8e.calls.reports[0].note || '') && o8e.calls.reports[0].stillN === 0, [o8e.calls.reports[0].note, o8e.pill]);

  // 8f approvals made here whose back files are not saved are counted, reported, and marked once they have waited ten minutes (Ana's and Michelle's waited hours)
  await setup();
  await page.evaluate(() => {
    const pg = allSheets().find(p => p.sheetId === 'p-same'); pg.charms = [{ poolId: 'pool-1' }, { poolId: 'pool-2' }]; pg.backPool = [{ poolId: 'pool-2', approvedAt: Date.now() - 1000 }];
    B.engrave.items.set('t-wait-1', { state: 'approved', copies: ['pool-1'], backPending: 'Reconnect to save the approved back files', approvedAt: Date.now() - 3600000 });   // counts: on a sheet, no saved back
    B.engrave.items.set('t-wait-2', { state: 'approved', copies: ['pool-gone'], backPending: 'x', approvedAt: Date.now() - 7200000 });                                          // not counted: its piece is on no sheet any more
    B.engrave.items.set('t-wait-3', { state: 'approved', copies: ['pool-2'], backPending: 'x', approvedAt: Date.now() - 3600000 });                                              // not counted: the sheet already holds its saved back
    B.engrave.items.set('t-wait-4', { state: 'review', copies: ['pool-1'], backPending: 'x', approvedAt: Date.now() - 3600000 });   // not counted (STALEBACK): not an approval in force (back in Review): nothing waits to be saved
  });
  await run(); const o8f = await out();
  await page.evaluate(() => { for (const k of ['t-wait-1', 't-wait-2', 't-wait-3']) B.engrave.items.delete(k); const pg = allSheets().find(p => p.sheetId === 'p-same'); pg.charms = []; pg.backPool = []; });
  const rf = o8f.calls.reports[0] || {};
  say('8f approvals waiting to be saved: only one whose piece is on a sheet and whose back the cloud lacks is counted (not a replaced piece, not one already saved), with its age, and the mark says so', rf.backWaitN === 1 && rf.backWaitMin >= 59 && rf.backWaitMin < 90 && o8f.pill && /Approvals not saved · 1/.test(o8f.pillText), [rf.backWaitN, rf.backWaitMin, o8f.pillText]);
  await setup({ cloud: ['p-behind', 'p-dirty', 'p-cut', 'p-same', 'p-new'].map((id, i) => ({ id, rev: 1, ch: 3, d: 1, cut: id === 'p-cut' ? 1 : 0, fb: 'G' + i, set: '', upd: 5 })) });
  await run(); const o8g = await out();
  say('8g with none waiting there is no such mark', (o8g.calls.reports[0] || {}).backWaitN === 0 && !o8g.pill, o8g.pillText);

  await setup({ mode: 'report' });
  await run(); const o9 = await out();
  say('9a "report" measures and reports, rebuilds nothing', o9.calls.changed.length === 0 && o9.calls.reports.length === 1 && o9.calls.reports[0].behindN === 1, o9.calls);
  await setup({ mode: 'off' });
  await run(); const o9b = await out();
  say('9b "off" does nothing at all', o9b.calls.api.length === 0 && o9b.calls.changed.length === 0, o9b.calls.api);

  await setup({ unsupported: true });
  await run(); const o10 = await out(); await run(); const o10b = await out();
  say('10 a cloud that does not know the calls stops it quietly', o10.calls.reports.length === 0 && o10b.calls.api.length === 1, o10b.calls.api);

  // 11 the list in Settings (a fresh page state: the stop above is for the page's life, so the list is read through list())
  const lst11 = [{ dev: 'dA', by: 'Paul_K', role: 'carrier', ageS: 20, stillN: 0, feed: 'push', mode: 'on', freezeMs: 0, build: 'drift-1118a/x' }, { dev: 'dB', by: 'Michelle_R', role: 'follower', ageS: 40, stillN: 2, still: ['RG 6→32', 'RG2 22→24'], feed: 'polling', mode: 'on', freezeMs: 5200, build: 'old' }, { dev: 'dC', by: 'Ana_M', role: 'follower', ageS: 4000, stillN: 0, mode: 'on', build: 'x' }];
  const html = await page.evaluate(async rows => { CN.api = async (fn, body) => (body.op === 'computerList' ? { computers: rows } : {}); const host = document.getElementById('stComputers'); if (!host) return null; host.querySelector('[data-r]').click ? null : null; const body = host.querySelector('[data-body]'); body.innerHTML = ''; const l = await CharmNestDrift.list(); return { n: l.length, hostHasSelect: !!host.querySelector('[data-m]') }; }, lst11);
  say('11a Settings has the Computers row with its switch and reads the list', html && html.n === 3 && html.hostHasSelect, html);
  const popText = await page.evaluate(async rows => { CN.api = async (fn, body) => (body.op === 'computerList' ? { computers: rows } : {}); const pill = document.getElementById('cnDriftPill'); CharmNestDrift._paint({ stillN: 1, still: ['x'], role: 'follower' }); pill.click(); await new Promise(r => setTimeout(r, 400)); const t = document.getElementById('cnDriftPop').innerText; return t; }, lst11);
  say('11b the list says who is behind, who is not seen, and the freeze', /Michelle_R/.test(popText) && /2 behind/.test(popText) && /5\.2 s/.test(popText) && /not seen/.test(popText) && /Paul_K/.test(popText), popText.replace(/\s+/g, ' ').slice(0, 300));

  const errs = (H.errors || []).filter(e => !/firebase stub|favicon|HTTP 400|status of 400/.test(e));   // (the one 400 is the refused computer id of 2a, asked for on purpose)
  say('no page error', errs.length === 0, errs.slice(0, 3));
  await H.close?.();
  console.log(fails ? `FAIL drift-check · ${fails} check(s)` : 'PASS drift-check');
  process.exit(fails ? 1 : 0);
})().catch(e => { console.error('ERROR', e && e.stack || e); process.exit(2); });
