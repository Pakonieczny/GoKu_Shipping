// FOLLOWROSE / RECALLPICTURE / RESTOREDSTALE (Paul, 18 Nov 2026: "figure out why her nesting is not updating and it's not showing the charms on the sheets on some sheets", Michelle's computer).
// What Michelle's saved workspace and the cloud showed: her GF, SS, 10K and 14K cards held the cloud's sheets to the revision, her Rose Gold card did not (the draft at 7 pieces, revision 6; the cloud: 27, revision 32),
// and her three CUT Rose Gold sheets were drawn as the green line alone.
//   1 a saved (recalled) Rose Gold sheet that is cut and has its cut history loaded, with no pieces on the page, is drawn from its saved picture (drawRecalled)
//   2 a followed Rose Gold sheet whose contour plan was LOADED from the cloud is not work in hand: Recall.follow replaces the Rose Gold card with the cloud's sheets (a plan being made here still keeps the card)
//   3 a followed sheet restored from this computer's workspace, whose record nothing had read, is read once by learn(): a newer revision is rebuilt, an equal one is left, a sheet with changes of its own waiting is left
// Real sorter in Chromium against the repo's fake site; nothing live.   PW_DIR=... CHROMIUM=... node tests/charm-nest/follow-rose-card.cjs
const path = require('path');
if (process.argv[2]) process.env.PW_DIR = process.argv[2];
const { open } = require('./day-sim/harness.cjs');
let fails = 0;
const say = (name, ok, seen) => { if (!ok) fails++; console.log(`${ok ? 'OK  ' : 'FAIL'} ${name}${seen === undefined ? '' : ' :: ' + (typeof seen === 'string' ? seen : JSON.stringify(seen))}`); };

(async () => {
  const H = await open({ masters: [], receipts: [] });
  const page = H.page;
  await page.evaluate(() => { CN.setMode('nest'); });
  await page.waitForTimeout(800);

  // 1 ─ the saved picture of a cut Rose Gold sheet
  const r1 = await page.evaluate(async () => {
    const sh = S.sheets.rose; let drawn = 0; const was = window.drawRecalled; window.drawRecalled = () => { drawn++; };
    Object.assign(sh, { recalled: { id: 'rose-t-cut', metal: 'rose', placedCount: 3, charmCount: 3, density: 0.4 }, charms: [], placements: [], roseCutAt: 1700000000000, roseHistory: [{ revision: 1 }, { revision: 2 }], sheetId: 'rose-t-cut', status: 'complete' });
    const cv = sh.el && sh.el.querySelector('[data-r="canvas"]'); const wide = !!cv && cv.parentElement.clientWidth >= 40;
    drawPreviewNow(sh); window.drawRecalled = was;
    Object.assign(sh, { recalled: null, roseCutAt: null, roseHistory: [], sheetId: null, status: 'idle' });
    return { drawn, wide };
  });
  say('1 a cut Rose Gold sheet with its cut history and no pieces is drawn from its saved picture', r1.wide && r1.drawn === 1, r1);

  // 2 ─ Recall.follow and the Rose Gold card
  const r2 = await page.evaluate(async () => {
    const RUN = 'run-follow-rose-t', rec = (id, extra) => Object.assign({ id, metal: 'rose', runId: RUN, fileBase: 'RG_' + id, status: 'complete', charmCount: 0, placedCount: 0, draft: true, layoutRev: 3, createdAt: Date.now() }, extra || {});
    const cloud = [rec('rose-t-a', { draft: false, roseCutAt: 1700000000000 }), rec('rose-t-b'), rec('rose-t-c')];
    const realApi = window.api, out = {};
    window.api = async (fn, body) => { if (body && body.op === 'listSheets') return { sheets: cloud }; return {}; };
    const prim = S.sheets.rose, stale = id => ({ metal: 'rose', sheetId: id, runId: RUN, followed: true, status: 'complete', charms: [], placements: [], rosePlan: { loaded: 'from the cloud' }, persistedDone: true, dirty: false });
    const stage = flags => { prim.pages = [prim]; Object.assign(prim, stale('rose-t-old-1'), flags || {}); const q = Object.assign({ el: null }, stale('rose-t-old-2')); prim.pages.push(q); prim.active = 0; return q; };
    const idsOf = () => S.sheets.rose.pages.map(p => p.sheetId);
    try {
      stage(); await Recall.follow(RUN, { force: true }); out.loaded = idsOf();
      const q = stage(); q._rosePlanning = true; await Recall.follow(RUN, { force: true }); out.planning = idsOf();
    } finally { window.api = realApi; }
    return out;
  });
  say('2a followed Rose Gold sheets with a plan loaded from the cloud are replaced by the cloud\'s sheets', JSON.stringify(r2.loaded) === JSON.stringify(['rose-t-a', 'rose-t-b', 'rose-t-c']), r2.loaded);
  say('2b a contour being planned on this computer still keeps the card', JSON.stringify(r2.planning) === JSON.stringify(['rose-t-old-1', 'rose-t-old-2']), r2.planning);

  // 3 ─ a restored followed sheet is read once
  const r3 = await page.evaluate(async () => {
    const CS = window.CharmNestCloudSheets, realApi = CN.api, realRebuild = Recall.rebuild, asked = [], out = {};
    CN.api = async (fn, body) => { if (body && body.op === 'getSheet') { asked.push(body.id); const rev = body.id === 'rose-t-same' ? 6 : 32; return { sheet: { id: body.id, metal: 'rose', layoutRev: rev, charmCount: 27, preview: '' }, rev: 'r' + rev }; } return {}; };
    Recall.rebuild = async pg => { pg.rebuilt = (pg.rebuilt || 0) + 1; };
    const prim = S.sheets.rose, mk = (id, o) => Object.assign({ metal: 'rose', sheetId: id, runId: 'run-t3', status: 'complete', charms: [], placements: [], layoutRev: 6, persistedDone: true, dirty: false, followed: true, recalled: null, roseCutAt: null, saveOwed: undefined, problem: null }, o || {});
    prim.pages = [prim]; prim.active = 0; Object.assign(prim, mk('rose-t-stale'));
    for (const p of [mk('rose-t-same'), mk('rose-t-dirty', { dirty: true }), mk('rose-t-own', { followed: false })]) { p.el = null; prim.pages.push(p); }
    try { await CS.learn(); } finally { CN.api = realApi; Recall.rebuild = realRebuild; }
    const by = id => prim.pages.find(p => p.sheetId === id);
    Object.assign(out, { stale: [by('rose-t-stale').rebuilt || 0, by('rose-t-stale').layoutRev], same: [by('rose-t-same').rebuilt || 0, by('rose-t-same').layoutRev], dirty: by('rose-t-dirty').rebuilt || 0, own: by('rose-t-own').rebuilt || 0, asked });
    prim.pages = [prim]; Object.assign(prim, { sheetId: null, runId: null, followed: false, layoutRev: undefined });
    return out;
  });
  say('3a a followed sheet saved at revision 6 while the cloud holds 32 is rebuilt', r3.stale[0] === 1 && r3.stale[1] === 32, r3.stale);
  say('3b one at the cloud\'s own revision is read, not rebuilt', r3.same[0] === 0 && r3.asked.includes('rose-t-same'), r3.same);
  say('3c a sheet with changes of its own waiting is left alone', r3.dirty === 0 && !r3.asked.includes('rose-t-dirty'), r3.dirty);
  say('3d a sheet that is not a followed one is left alone', r3.own === 0 && !r3.asked.includes('rose-t-own'), r3.own);

  const errs = (H.errors || []).filter(e => !/firebase stub|favicon/.test(e));
  say('no page error', errs.length === 0, errs.slice(0, 3));
  await H.close?.();
  console.log(fails ? `FAIL follow-rose-card · ${fails} check(s)` : 'PASS follow-rose-card');
  process.exit(fails ? 1 : 0);
})().catch(e => { console.error('ERROR', e && e.stack || e); process.exit(2); });
