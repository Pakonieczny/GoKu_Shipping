// Why a Gold or Silver sheet went to the laser below the 75% target (Paul, 9 Oct: "the silver sheet did not finish being filled and
// then the system decided to start a new sheet"; an SS sheet at 59% with a free pocket, released). The page's own topupSettle over
// fake sheets, no solver and no browser (as manual-intake.cjs): the owed-35-orders rule, the 75% target and the no-room exit are as
// they were for Silver and for Gold alike, and a sheet released below the target now says which of them (or which missing Auto run)
// released it, on its record (the gap fill it already carries) and on its card; the card's "8m 60s" reads "9m 0s".
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm');
const html = fs.readFileSync('charm-nest-1.html', 'utf8');
const FEED = html.slice(html.indexOf('/* A big batch goes onto a sheet'), html.indexOf('/* A stopped run starts none'));
const GAP = html.slice(html.indexOf('/* Gap fill (Paul, 24 Sep)'), html.indexOf('function usableArea('));
const fmtLine = html.slice(html.indexOf('const fmt = { pct'), html.indexOf('\n', html.indexOf('const fmt = { pct'))).replace('const fmt =', 'fmt =');
assert(FEED.length > 500 && GAP.length > 2000 && fmtLine.length > 100, 'the page slices were found');
const logs = [];
const ctx = { MM_PER_PT: 1, S: { settings: { maxFill: .8, runMode: 'auto' } }, window: { B: { run: { runId: 'run-1', status: 'processed' } } }, log: (sh, m) => logs.push(m), renderCard() {}, activeCharms: p => p.charms.filter(x => !x.excluded) };
vm.createContext(ctx); vm.runInContext(fmtLine, ctx); vm.runInContext(FEED, ctx); vm.runInContext(GAP, ctx);
const sheet = (metal, extra = {}) => { const charms = Array.from({ length: 45 }, (_, i) => ({ id: 'p' + i, order: 'o' + i })); return { metal, runId: 'run-1', rejects: ['late'], charms, placements: charms.map(c => ({ id: c.id })), verification: { ok: true }, ...extra }; };
const res = (density, extra = {}) => ({ density, placedPt2: density * 100, usablePt2: 100, freePt2: (1 - density) * 100, endedBy: 'no-room', smallRoom: true, ...extra });
// one update of the live run: the orders that reach the sheet try its gaps; those that miss move on to the next sheet
const update = (sh, orders) => { sh.nestInitial = sh.placements.map(p => ({ ...p })); sh.rejects = []; for (const o of orders) { sh.charms.push({ id: 'c-' + o, order: o }); sh.rejects.push('c-' + o); } };
const moveOn = sh => { sh.charms = sh.charms.filter(c => !sh.rejects.includes(c.id)); };
const card = (sh, pct) => ctx.releasedBelowTarget(Object.assign({ releaseFull: true }, sh), pct);

for (const metal of ['silver', 'gold']) {
  // ── 1. Auto: the first miss at 59% opens the gap fill (nothing released); 34 orders tried keep it open; the 35th releases it, and the record says so ──
  const sh = sheet(metal);
  assert.equal(ctx.topupSettle(sh, res(.59), true), false, metal + ': the sheet that missed an order at 59% is not released');
  assert(sh.topup && !sh.topup.closedAt && !sh.releaseWhy); assert.equal(card(sh, .59), '', 'nothing to explain while it is filling');
  for (let i = 0; i < 34; i++) { moveOn(sh); update(sh, ['x' + i]); assert.equal(ctx.topupSettle(sh, res(.59), false), false); }
  assert.equal(sh.topup.tried.length, 34, '34 orders tried: still waiting');
  moveOn(sh); update(sh, ['last']);
  assert.equal(ctx.topupSettle(sh, res(.59), false), true, 'the 35th order releases it');
  assert.equal(sh.topup.why, '35 later orders tried');
  assert.equal(card(sh, .59), 'released at 59% · 35 later orders tried', 'the card says which rule released it');
  assert.equal(card(sh, .75), '', 'a sheet at the target needs no explaining'); assert.equal(card({ ...sh, releaseFull: false }, .59), '', 'a sheet still filling needs none');
  assert.equal(card({ ...sh, metal: 'rose' }, .59), '', 'only Gold and Silver wait for a gap fill');
  // ── 2. 75% shown full releases it as before ──
  const reached = sheet(metal); ctx.topupSettle(reached, res(.70), true); moveOn(reached); update(reached, ['a']);
  assert.equal(ctx.topupSettle(reached, res(.745), false), true); assert.match(reached.topup.why, /75% full/); assert.equal(card(reached, .75), '');
  // ── 3. not even the smallest charms fit: no 35-order wait, and the reason is kept ──
  logs.length = 0; const tight = sheet(metal);
  assert.equal(ctx.topupSettle(tight, res(.59, { smallRoom: false }), true), true);
  assert.equal(tight.releaseWhy, 'no room left for even the smallest charms'); assert.equal(card(tight, .59), 'released at 59% · no room left for even the smallest charms');
  assert.equal(logs.length, 1); assert.match(logs[0], /Released at 59% without a gap fill: no room left for even the smallest charms/);
  ctx.topupSettle(tight, res(.59, { smallRoom: false }), true); assert.equal(logs.length, 1, 'the log says it once');
  // ── 4. no live Auto run to bring later orders: released at the first miss, as before, and now says that ──
  ctx.S.settings.runMode = 'manual'; logs.length = 0; const manual = sheet(metal);
  assert.equal(ctx.topupSettle(manual, res(.59), true), true, 'Manual mode releases as before'); assert.match(manual.releaseWhy, /Manual mode brings no later orders/);
  assert.match(card(manual, .59), /^released at 59% · Manual mode/); assert.match(logs[0], /without a gap fill: Manual mode/);
  ctx.S.settings.runMode = 'auto';
  const other = sheet(metal, { runId: 'run-0' }); assert.equal(ctx.topupSettle(other, res(.59), true), true); assert.match(other.releaseWhy, /not a sheet of the live Auto run/);
  // ── 5. a sheet at the ceiling, and one that missed nothing, are simply full: no note ──
  const top = sheet(metal); assert.equal(ctx.topupSettle(top, res(.797), true), true); assert.equal(top.releaseWhy, undefined);
  // ── 6. the gap fill that was running when the run stopped brings orders: released, and the card says why ──
  const run = sheet(metal); ctx.topupSettle(run, res(.59), true); ctx.window.B.run.status = 'complete'; moveOn(run); update(run, ['z']);
  assert.equal(ctx.topupSettle(run, res(.59), false), true); assert.equal(run.topup.why, 'the run stopped bringing orders'); ctx.window.B.run.status = 'processed';
}
// ── 7. what the card's timing line reads ──
assert.equal(ctx.fmt.s(539600), '9m 0s', 'was "8m 60s"'); assert.equal(ctx.fmt.s(59600), '1m 0s'); assert.equal(ctx.fmt.s(14000), '14s'); assert.equal(ctx.fmt.s(125000), '2m 5s'); assert.equal(ctx.fmt.s(0), '0s');
// ── 8. the card and the sheet's own reset ──
assert(/releasedBelowTarget\(sh, sh\.sat \? sh\.sat\.fullPct : sh\.density\)/.test(html), 'the sheet card shows it');
assert(/delete sh\.topup; delete sh\.releaseWhy;/.test(html), 'a sheet made dirty forgets an old reason with its old release');
console.log('PASS: a Gold or Silver sheet released below 75% says why (35 orders tried, no room, no live Auto run); the 35-order rule, the 75% target and the no-room exit are unchanged; the timing line reads whole seconds');
