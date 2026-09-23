// Compare a simulated day (day.cjs output) with the intended process: PASS and FAIL lines, then what happened.
//   node tests/charm-nest/day-sim/analyze.cjs out.json
const d = require(require('path').resolve(process.argv[2]));
const out = { ok: [], bad: [], info: [] };
const ok = (m) => out.ok.push(m), bad = (m) => out.bad.push(m), info = (m) => out.info.push(m);
const S = d.server, rows = d.sorter.rows, sheets = S.Charm_Nest_Sheets, sets = S.Charm_Nest_Sets;
const lastAt = d.ticks.length ? d.ticks[d.ticks.length - 1].at : 0;
// a cancel or change scheduled after the last check never happened in this run
const happened = d.events.filter(e => e.at <= lastAt);
const cancelled = new Set(happened.filter(e => e.kind === 'cancel').map(e => String(e.rid)));
const changed = happened.filter(e => e.kind === 'change').map(e => String(e.rid));
const byOrder = new Map(d.orders.map(o => [o.rid, o]));
const completed = new Set(S['Design_Completed Orders'].map(x => x._id));

// 1 · intake: every order that was open at a check reached the sorter once
const cancelAt = new Map(happened.filter(e => e.kind === 'cancel').map(e => [String(e.rid), e.at]));
// An order cut and committed before the buyer cancelled it was right to go: the cancel is the shop's to handle.
const committedAt = rid => { const s = sets.find(x => (x.committed || []).map(String).includes(rid)); return s ? s.committedAt : null; };
const cutBeforeCancel = rid => committedAt(rid) != null && cancelAt.has(rid) && committedAt(rid) <= cancelAt.get(rid);
const expectedSeen = d.orders.filter(o => o.at <= lastAt && !(cancelAt.has(o.rid) && cancelAt.get(o.rid) <= o.at + 10 * 60000));
const seenRids = new Set(rows.map(r => r.rid));
const missing = expectedSeen.filter(o => !seenRids.has(o.rid));
missing.length ? bad(`intake: ${missing.length} order(s) never reached the sorter: ${missing.map(o => o.rid).join(', ')}`) : ok(`intake: all ${expectedSeen.length} orders reached the sorter`);
const dupKeys = rows.length - new Set(rows.map(r => r.key)).size;
dupKeys ? bad(`intake: ${dupKeys} duplicate order line(s)`) : ok('intake: no duplicate order lines');
const ledger = S.Charm_Nest_Arrivals.length;
ledger === seenRids.size ? ok(`ledger: ${ledger} unique receipts recorded`) : bad(`ledger: ${ledger} receipts recorded vs ${seenRids.size} seen`);
info(`arrival counter at the end: ${JSON.stringify({ count24: d.sorter.arrivals.count24, count1: d.sorter.arrivals.count1 })}`);

// 2 · line interpretation
const expectLine = new Map(); for (const o of d.orders) for (const l of o.lines) expectLine.set(`${o.rid}_${l.tx}`, l);
const METAL = { GF: 'gold', SS: 'silver', RG: 'rose', '14K': 'gold14k', '10K': 'gold10k' };
let wrongMetal = [];
for (const r of rows) { const l = expectLine.get(r.key); if (!l) continue; const want = METAL[l.metal] || null; if (want && r.material !== want) wrongMetal.push(`${r.key} ${l.metal}→${r.material}`); }
wrongMetal.length ? bad(`metal: ${wrongMetal.length} line(s) read as the wrong metal: ${wrongMetal.slice(0, 8).join('; ')}`) : ok('metal: every line read as its ordered metal');
const unknownSku = rows.filter(r => /NOPE/.test(r.sku || expectLine.get(r.key)?.sku || ''));
unknownSku.every(r => ['unmatched', 'held'].includes(r.state)) ? ok(`unknown SKU: ${unknownSku.length} line(s) held (${unknownSku.map(r => r.state).join(',')})`) : bad(`unknown SKU lines not held: ${unknownSku.map(r => r.key + ':' + r.state).join(', ')}`);
const noMetal = rows.filter(r => expectLine.get(r.key)?.metal === '??');
noMetal.every(r => r.state === 'held' || r.state === 'unmatched') ? ok(`unclear metal ("Solid Gold"): ${noMetal.length} line(s) held for a decision`) : bad(`unclear metal lines: ${noMetal.map(r => r.key + ':' + r.state + ':' + r.reason).join(', ')}`);

// 3 · what was released and committed
const setById = new Map(sets.map(s => [s.setId, s]));
const committedSets = sets.filter(s => s.committedAt);
info(`sets: ${sets.map(s => `${s.name}(${s.day}) ${s.status} sheets=${(s.sheetIds || []).length} committed=${(s.committed || []).length}`).join(' · ')}`);
const seqsByDay = {}; for (const s of sets) (seqsByDay[s.day] ||= []).push(s.seq);
for (const [day, seqs] of Object.entries(seqsByDay)) { const u = new Set(seqs); u.size === seqs.length ? ok(`set numbers on ${day}: ${seqs.sort((a, b) => a - b).join(', ')} (unique)`) : bad(`set numbers repeat on ${day}: ${seqs.join(', ')}`); }
const inSet = sheets.filter(sh => sh.setId && !sh.draft);
for (const sh of inSet) {
  const set = setById.get(sh.setId);
  if (!set) { bad(`sheet ${sh.fileBase} names set ${sh.setId}, which has no record`); continue; }
  if (!(set.sheetIds || []).includes(sh.id || sh._id)) bad(`sheet ${sh.fileBase} says it is in ${set.name} but ${set.name}'s record does not list it (record lists ${(set.sheetIds || []).length} sheet(s))`);
  if (['gold', 'silver'].includes(sh.metal) && !sh.releaseFull) bad(`partial ${sh.metal} sheet ${sh.fileBase} released (not full)`);
  if (sh.metal === 'rose' && set.seq % 2) bad(`rose sheet ${sh.fileBase} in odd set ${set.name}`);
  if (['gold10k', 'gold14k'].includes(sh.metal)) bad(`solid sheet ${sh.fileBase} released without being selected`);
}
// every arranged sheet passes its own check: one that fails is held for a person, and its orders wait
const unverified = sheets.filter(sh => (sh.placements || []).length && sh.verification && sh.verification.ok === false);
unverified.length ? bad(`${unverified.length} sheet(s) failed their check and are held: ${unverified.map(sh => `${sh.fileBase} ${JSON.stringify(sh.verification)}`).join('; ')}`) : ok(`every arranged sheet passed its check (${sheets.filter(sh => (sh.placements || []).length).length})`);
const fileBases = inSet.map(sh => sh.fileBase); const dupFb = fileBases.filter((f, i) => fileBases.indexOf(f) !== i);
dupFb.length ? bad(`sheet names used twice: ${[...new Set(dupFb)].join(', ')}`) : ok(`${inSet.length} released sheet(s), each with its own name: ${fileBases.join(', ')}`);
// a piece on two released sheets would be cut twice
const pieceSheets = new Map(); for (const sh of inSet) for (const id of sh.poolIds || []) (pieceSheets.get(id) || pieceSheets.set(id, []).get(id)).push(sh.fileBase);
const twice = [...pieceSheets].filter(([, v]) => v.length > 1);
twice.length ? bad(`${twice.length} piece(s) on two released sheets: ${twice.slice(0, 5).map(([k, v]) => k + '→' + v.join('+')).join('; ')}`) : ok('no piece is on two released sheets');
// committed orders: every line released on a committed sheet, never cancelled, engraving approved
const releasedPieces = new Set(inSet.filter(sh => committedSets.some(s => s.setId === sh.setId)).flatMap(sh => sh.poolIds || []));
for (const rid of completed) {
  const o = byOrder.get(rid); if (!o) { bad(`committed ${rid}, not an order of the day`); continue; }
  if (cancelled.has(rid)) cutBeforeCancel(rid) ? info(`order ${rid} was cancelled after it was cut and committed`) : bad(`cancelled order ${rid} was committed`);
  for (const l of o.lines) for (let c = 1; c <= l.qty; c++) { const id = `${rid}_${l.tx}_${c}`; if (!releasedPieces.has(id)) bad(`committed order ${rid} has piece ${id} (${l.metal}) that is not on a committed sheet`); }
}
info(`committed orders: ${completed.size} of ${d.orders.length}`);
// a committed order is finished here: a later Etsy read must not turn it back into a live or gone line
const reread = [...completed].filter(rid => rows.some(r => r.rid === rid && r.state === 'gone'));
reread.length ? bad(`committed order(s) read again from Etsy and marked gone: ${reread.join(', ')}`) : ok('no committed order was read again and marked gone');
// every piece on a committed sheet belongs to an order that is committed or held for a reason
const heldCommittedPieces = [...releasedPieces].map(id => id.split('_')[0]).filter(rid => !completed.has(rid));
if (heldCommittedPieces.length) info(`pieces cut for orders not (yet) marked complete: ${[...new Set(heldCommittedPieces)].map(rid => rid + ':' + (rows.filter(r => r.rid === rid).map(r => r.state + (r.reason ? '(' + r.reason + ')' : '')).join('/'))).join('; ')}`);
// 4 · engraving
const backs = S.Charm_Pool_Back, words = t => String(t ?? '').replace(/\s+/g, ' ').trim();   // the fitter may break a name over two lines
for (const rid of completed) for (const l of byOrder.get(rid)?.lines || []) if (l.personal) {
  const text = changed.includes(rid) ? 'ROSE' : l.personal;
  for (let c = 1; c <= l.qty; c++) { const b = backs.find(x => x.poolId === `${rid}_${l.tx}_${c}`); if (!b) bad(`committed ${rid}: no back file for ${l.personal}`); else if (words(b.text) !== words(text)) bad(`committed ${rid}: back says ${JSON.stringify(b.text)}, order says ${JSON.stringify(text)}`); }
}
const personalRows = rows.filter(r => expectLine.get(r.key)?.personal && !['gone', 'unmatched', 'held'].includes(r.state));
const noEng = personalRows.filter(r => !r.engrave || !r.engrave.needed);
noEng.length ? bad(`engraving: ${noEng.length} personalized line(s) with no engraving job: ${noEng.slice(0, 8).map(r => r.key + ':' + r.state + ':' + JSON.stringify(r.engrave)).join('; ')}`) : ok(`engraving: all ${personalRows.length} personalized lines have an engraving job`);
const engStates = {}; for (const r of personalRows) { const k = r.engrave ? r.engrave.state : 'none'; engStates[k] = (engStates[k] || 0) + 1; } info(`engraving states of personalized lines: ${JSON.stringify(engStates)}`);
for (const rid of changed) { const r = rows.find(x => x.rid === rid && x.engrave && x.engrave.needed); info(`changed order ${rid}: engraving now ${JSON.stringify(r && r.engrave)} state ${r && r.state}`); }
// 5 · cancellations
for (const rid of cancelled) { const rs = rows.filter(r => r.rid === rid); info(`cancelled ${rid}: ${rs.map(r => r.state + (r.reason ? '(' + r.reason + ')' : '')).join(', ') || 'never pulled'} · on working sheets: ${sheets.filter(sh => (sh.poolIds || []).some(id => id.startsWith(rid + '_'))).map(sh => sh.fileBase).join(', ') || 'none'}`); if (completed.has(rid) && !cutBeforeCancel(rid)) bad(`cancelled ${rid} committed`); }
// 6 · where everything ended up
const state = {}; for (const r of rows) state[r.state] = (state[r.state] || 0) + 1;
info(`line states at the end: ${JSON.stringify(state)}`);
for (const sh of d.sorter.sheets.filter(x => x.placements.length)) info(`card ${sh.fileBase}: ${sh.metal} ${sh.status}${sh.draft ? ' draft' : ''}${sh.full ? ' full' : ''} ${sh.placements.length} pcs${sh.setId ? ' in ' + sh.setId : ''}${sh.metal === 'rose' ? ` green lines ${sh.lines}${sh.roseCutAt ? ' cut' : ''}` : ''}`);
// 6b · partial sheets: a GF/SS sheet that stops taking orders before it is full leaves its pieces waiting on two partials
const lastSheets = (d.ticks.length ? d.ticks[d.ticks.length - 1].snap.sheets : []);
for (const metal of ['gold', 'silver']) {
  const open = lastSheets.filter(x => x.metal === metal && x.n > 0 && !x.set);
  if (open.length > 1) bad(`${metal}: ${open.length} unreleased partial sheets at the end (${open.map(x => x.n + ' pcs/' + x.fill + '%' + (x.closed ? ' closed' + (x.hold ? ':' + x.hold : '') + (x.fin ? ':finalized' : '') : '')).join(', ')}) — pieces wait on two half-empty sheets`);
}
const closedEarly = new Map();
for (const t of d.ticks) for (const x of t.snap.sheets || []) if (['gold', 'silver'].includes(x.metal) && x.closed && !x.full && !x.set && x.n > 0) closedEarly.set(x.metal + ':' + (x.fb || x.n), `${x.metal} ${x.n} pcs ${x.fill}% closed at t${t.tick}${x.hold ? ' hold=' + x.hold : ''}${x.fin ? ' finalized' : ''}${x.problem ? ' problem=' + x.problem : ''}`);
if (closedEarly.size) bad(`sheets closed to arrivals before they were full: ${[...closedEarly.values()].slice(0, 6).join(' | ')}`);
// 6c · a SKU label is never charm material: it would be drawn beside the charm on the sheet and in the laser file
const labelled = d.sorter.sheets.filter(x => Array.isArray(x.textCharms));
if (labelled.length) { const withText = labelled.filter(x => x.textCharms.length); withText.length ? bad(`${withText.reduce((n, x) => n + x.textCharms.length, 0)} placed charm(s) carry text beside them (a SKU label): ${withText.map(x => x.fileBase + ':' + [...new Set(x.textCharms)].join('/')).join('; ')}`) : ok('no placed charm carries its SKU label onto a sheet'); }
// 7 · the run and the page
const stops = d.ticks.filter(t => t.snap.run && t.snap.run.status === 'stopped');
stops.length ? bad(`run stopped ${stops.length} time(s): ${[...new Set(stops.map(t => t.snap.run.stoppedBy))].join(' | ')}`) : ok('the run never stopped');
const timeouts = d.ticks.filter(t => t.idle && t.idle.timeout);
if (timeouts.length) bad(`${timeouts.length} tick(s) never settled: ${JSON.stringify(timeouts[0].idle)}`);
const alarms = d.ticks.filter(t => t.snap.etsy && t.snap.etsy.alarm);
alarms.length ? bad(`Etsy watchdog alarms: ${JSON.stringify(alarms[0].snap.etsy.alarm)}`) : ok('no Etsy watchdog alarm');
// A cancelled order is read once more at the next check and Etsy answers 404, which is how the sorter learns it is gone:
// expected once for each cancelled order, with the browser's console line for it.
const readGone = new Set(); let goneLines = 0;
const errs = (d.errors || []).filter(e => !/favicon/.test(e)).filter(e => {
  const m = /^HTTP 404 \S*etsyOrderProxy\?orderId=(\d+)/.exec(e);
  if (m && cancelled.has(m[1]) && !readGone.has(m[1])) { readGone.add(m[1]); goneLines++; return false; }
  return true;
}).filter(e => !(goneLines > 0 && /^console: Failed to load resource: the server responded with a status of 404/.test(e) && goneLines--));
if (readGone.size) info(`cancelled order(s) read once and found gone (HTTP 404): ${[...readGone].join(', ')}`);
errs.length ? bad(`${errs.length} page error(s): ${[...new Set(errs)].slice(0, 6).join(' | ')}`) : ok('no page errors');
const warns = d.sorter.events.filter(e => /^(warn|bad|error)/.test(e));
info(`${warns.length} warning(s) in the activity log${warns.length ? ': ' + [...new Set(warns.map(w => w.replace(/\d{6,}/g, '#')))].slice(0, 12).join(' || ') : ''}`);
const slow = d.ticks.slice().sort((a, b) => b.ms - a.ms).slice(0, 3).map(t => `t${t.tick} ${Math.round(t.ms / 1000)}s`);
info(`slowest ticks (real time): ${slow.join(', ')}`);
console.log('PASS\n  ' + out.ok.join('\n  '));
console.log('FAIL\n  ' + (out.bad.join('\n  ') || '(none)'));
console.log('INFO\n  ' + out.info.join('\n  '));
