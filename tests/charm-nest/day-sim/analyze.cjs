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
// RUN_MODE=manual (day.cjs): the run is off, so lines stay pulled and nothing is held, engraved or laid out; only the intake is judged
const manual = d.ticks.length > 0 && d.ticks.every(t => !t.snap.run);

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
manual ? info(`unknown SKU: ${unknownSku.length} line(s) (the run is off: none is held yet)`) : unknownSku.every(r => ['unmatched', 'held'].includes(r.state)) ? ok(`unknown SKU: ${unknownSku.length} line(s) held (${unknownSku.map(r => r.state).join(',')})`) : bad(`unknown SKU lines not held: ${unknownSku.map(r => r.key + ':' + r.state).join(', ')}`);
const noMetal = rows.filter(r => expectLine.get(r.key)?.metal === '??');
manual ? info(`unclear metal ("Solid Gold"): ${noMetal.length} line(s) (the run is off)`) : noMetal.every(r => r.state === 'held' || r.state === 'unmatched') ? ok(`unclear metal ("Solid Gold"): ${noMetal.length} line(s) held for a decision`) : bad(`unclear metal lines: ${noMetal.map(r => r.key + ':' + r.state + ':' + r.reason).join(', ')}`);

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
manual ? info(`engraving: ${personalRows.length - noEng.length} of ${personalRows.length} personalized lines have a job (the run is off: jobs are made for the lines a person opens)`) : noEng.length ? bad(`engraving: ${noEng.length} personalized line(s) with no engraving job: ${noEng.slice(0, 8).map(r => r.key + ':' + r.state + ':' + JSON.stringify(r.engrave)).join('; ')}`) : ok(`engraving: all ${personalRows.length} personalized lines have an engraving job`);
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
const realErrs = errs.filter(e => !/firebaseOrders\?messagesFor=/.test(e)).filter(e => !/^console: Failed to load resource: the server responded with a status of 400/.test(e));
if (realErrs.length !== errs.length) info(`${errs.length - realErrs.length} HTTP 400 on firebaseOrders?messagesFor (the stand-in does not serve the order messages thread) ignored`);
realErrs.length ? bad(`${realErrs.length} page error(s): ${[...new Set(realErrs)].slice(0, 6).join(' | ')}`) : ok('no page errors');
// The stand-in site does not answer the order messages read (firebaseOrders?messagesFor=...): a gap of the fake, not of the page.
const messagesGap = (d.errors || []).filter(e => /firebaseOrders\?messagesFor=/.test(e) || /status of 400/.test(e)).length;
const warns = d.sorter.events.filter(e => /^(warn|bad|error)/.test(e));
info(`${warns.length} warning(s) in the activity log${warns.length ? ': ' + [...new Set(warns.map(w => w.replace(/\d{6,}/g, '#')))].slice(0, 12).join(' || ') : ''}`);
const slow = d.ticks.slice().sort((a, b) => b.ms - a.ms).slice(0, 3).map(t => `t${t.tick} ${Math.round(t.ms / 1000)}s`);
info(`slowest ticks (real time): ${slow.join(', ')}`);

// 8 · production size: time per check, the page's memory and browser storage, and a page crash (day.cjs with BACKLOG / CRASH_HOUR)
{
  const cfg = d.config || {}, tk = d.ticks.filter(t => t.mem);
  const simH = d.ticks.length ? (lastAt - d.t0) / 3600000 : 0;
  info(`coverage: ${simH.toFixed(1)} simulated hours, ${d.ticks.length} checks, ended by ${d.endedBy || 'the day'} after ${d.wallS || '?'} s${cfg.TICK_MIN ? ` (a check every ${cfg.TICK_MIN} min)` : ''}${cfg.BACKLOG ? `; ${cfg.BACKLOG} orders pulled at the first check, ${cfg.ORDERS} arrivals` : ''}`);
  if (d.endedBy === 'deadline') bad(`the day was cut at the wall-clock limit after ${simH.toFixed(1)} of ${cfg.HOURS} simulated hours`);
  if (tk.length) {
    const med = a => { const x = a.filter(v => v != null && isFinite(v)).sort((p, q) => p - q); return x.length ? x[Math.floor(x.length / 2)] : null; };
    info('per check · tick hour pieces rows | check ms | busy s | heap MB | DOM nodes | sorter ls chars | station ls chars | usage MB (sorter/station)\n    ' + tk.map(t => `t${t.tick} ${String(t.hour).padStart(2, '0')}h pcs=${t.mem.pieces} rows=${t.mem.rows} | ${t.check ? Math.round(t.check.checkMs) : '-'} | ${t.check && t.check.busyS != null ? t.check.busyS : '-'} | ${t.mem.heapMB} | ${t.mem.nodes} | ${t.mem.sorter && t.mem.sorter.lsChars} | ${t.mem.station && t.mem.station.lsChars} | ${t.mem.sorter && t.mem.sorter.usageMB}/${t.mem.station && t.mem.station.usageMB}`).join('\n    '));
    // time per check after the first pull: the last third must not be slower than twice the first third
    const later = tk.filter(t => t.tick >= 1 && t.check && t.check.checked), third = Math.floor(later.length / 3);
    if (third >= 2) {
      const a = med(later.slice(0, third).map(t => t.check.checkMs)), b = med(later.slice(-third).map(t => t.check.checkMs));
      const a2 = med(later.slice(0, third).map(t => t.check.busyS)), b2 = med(later.slice(-third).map(t => t.check.busyS));
      (b > 2 * a + 2000) ? bad(`time per check grows with the pool: median ${Math.round(a)} ms early, ${Math.round(b)} ms late`) : ok(`time per check stays bounded: median ${Math.round(a)} ms early vs ${Math.round(b)} ms late (${later.length} checks; the pool grew ${later[0].mem.rows}→${later.at(-1).mem.rows} lines)`);
      (b2 > 2 * a2 + 1) ? bad(`the page's busy time per check grows: ${a2} s early, ${b2} s late`) : ok(`page busy time per check stays bounded: ${a2} s early vs ${b2} s late`);
    } else info('time per check: too few checks after the first pull to compare early and late');
    // Firestore refuses a document over 1 MiB; the stand-in does not, so the biggest document of the day is looked at here
    const dk = {}; for (const t of tk) for (const [c, v] of Object.entries(t.docs || {})) if (!dk[c] || v.maxKB > dk[c].maxKB) dk[c] = Object.assign({ tick: t.tick }, v);
    if (Object.keys(dk).length) { info('largest stored document of the day: ' + Object.entries(dk).map(([c, v]) => `${c} ${v.maxKB} KB (${v.n} docs, t${v.tick})`).join(' · ')); const big = Object.entries(dk).filter(([, v]) => v.maxKB > 900); big.length ? bad(`a stored document nears Firestore's 1 MiB limit: ${big.map(([c, v]) => c + ' ' + v.maxKB + ' KB').join(', ')}`) : ok('no stored document nears Firestore\'s 1 MiB limit (largest ' + Math.max(...Object.values(dk).map(v => v.maxKB)) + ' KB)'); }
    const first = tk.find(t => t.tick === 0) || tk.find(t => t.tick === 1);
    if (first && first.check) info(`first pull: ${Math.round(first.check.checkMs)} ms of wall time${first.check.simMin != null ? `, ${first.check.simMin} simulated minutes, ${first.check.reads} Etsy reads` : ''} for ${first.mem.rows} lines${first.check.checkError ? '; the check failed on the way: ' + first.check.checkError : ''}`);
    const slowest = tk.filter(t => t.check && t.tick >= 1).sort((p, q) => q.check.checkMs - p.check.checkMs)[0];
    if (slowest) info(`slowest check: t${slowest.tick} ${Math.round(slowest.check.checkMs)} ms`);
    // memory: after a garbage collection the heap must follow the pool, not the number of checks
    const h = tk.filter(t => t.tick >= 2 && t.mem.heapMB), hLast = h.at(-1), hBase = h[0];
    if (h.length >= 4) { const g = hLast.mem.heapMB / hBase.mem.heapMB, rg = hLast.mem.rows / Math.max(1, hBase.mem.rows); (g > 2.5 * Math.max(1, rg)) ? bad(`page memory grows: ${hBase.mem.heapMB} MB at t${hBase.tick} → ${hLast.mem.heapMB} MB at t${hLast.tick} while the lines grew ${rg.toFixed(2)}x`) : ok(`page memory bounded after garbage collection: ${hBase.mem.heapMB} MB at t${hBase.tick} → ${hLast.mem.heapMB} MB at t${hLast.tick} (lines ${hBase.mem.rows}→${hLast.mem.rows}); DOM nodes ${hBase.mem.nodes}→${hLast.mem.nodes}`); }
    // browser storage: localStorage against its 5 MiB quota, and growth across checks where no order arrived
    for (const [name, get] of [['sorter', t => t.mem.sorter && t.mem.sorter.lsChars], ['station', t => t.mem.station && t.mem.station.lsChars]]) {
      const v = tk.map(get).filter(x => x != null); if (!v.length) continue; const mx = Math.max(...v);
      mx > 0.8 * 5242880 ? bad(`${name} localStorage reached ${mx} chars of its 5.2M quota`) : ok(`${name} localStorage peak ${mx} chars (${(100 * mx / 5242880).toFixed(1)}% of the quota), last ${v.at(-1)}`);
      const quiet = tk.filter(t => !(t.arrived || []).length && get(t) != null && !(t.acted || []).length);
      let up = 0; for (let i = 1; i < quiet.length; i++) if (quiet[i].tick === quiet[i - 1].tick + 1 && get(quiet[i]) > get(quiet[i - 1]) * 1.02) up++;
      up >= 3 ? bad(`${name} localStorage keeps growing by more than 2% at ${up} checks where no order arrived`) : ok(`${name} localStorage does not grow while no order arrives (${quiet.length} such checks)`);
    }
    const idb = tk.filter(t => t.mem.idbSorter);
    if (idb.length) { const f = idb[0], l = idb.at(-1); const sum = o => Object.entries(o || {}).map(([db, st]) => `${db}: ` + Object.entries(st).map(([n, v]) => `${n} ${v.n}rec/${v.kb}KB`).join(', ')).join(' ; '); info(`IndexedDB (sorter) at t${f.tick}: ${sum(f.mem.idbSorter)}\n    at t${l.tick}: ${sum(l.mem.idbSorter)}`); info(`IndexedDB (station) at t${idb[0].tick}: ${sum(idb[0].mem.idbStation)}\n    at t${l.tick}: ${sum(l.mem.idbStation)}`); const kb = o => Object.values(o || {}).reduce((n, st) => n + Object.values(st).reduce((m, v) => m + (v.kb || 0), 0), 0); info(`IndexedDB (sorter) holds ${kb(f.mem.idbSorter)} KB at t${f.tick} (${f.mem.rows} lines) and ${kb(l.mem.idbSorter)} KB at t${l.tick} (${l.mem.rows} lines): ${(kb(l.mem.idbSorter) / Math.max(1, l.mem.rows)).toFixed(1)} KB a line`); }
    if (d.baseline) info(`before the first pull: heap ${d.baseline.heapMB} MB, sorter localStorage ${d.baseline.sorter && d.baseline.sorter.lsChars} chars, station ${d.baseline.station && d.baseline.station.lsChars}`);
  }
  if (d.crash) {
    const c = d.crash, lost = c.lost || {}, n = Object.values(lost).reduce((m, v) => m + v.length, 0);
    info(`page crash: before ${c.before.rows} lines / ${c.before.done} committed orders / ${c.before.pieces} saved piece placements / ${c.before.backs} back files; the reopened page: ${JSON.stringify(c.back)}`);
    c.lost ? (n ? bad(`after the page crash and reopen, lost: ${Object.entries(lost).filter(([, v]) => v.length).map(([k, v]) => k + ' ' + v.length + ' (' + v.slice(0, 3).join(', ') + ')').join('; ')}`) : ok(`a page crash and reopen lost nothing (lines, committed orders, placed pieces, back files, sets, approvals)`)) : bad('the page crash was not checked afterwards');
    if (c.after) info(`after the crash and settling: ${c.after.rows} lines, ${c.after.done} committed orders`);
  } else if (cfg.CRASH_HOUR >= 0) bad('the day ended before the hour of the planned page crash');
}

console.log('PASS\n  ' + out.ok.join('\n  '));
console.log('FAIL\n  ' + (out.bad.join('\n  ') || '(none)'));
console.log('INFO\n  ' + out.info.join('\n  '));
