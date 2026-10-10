#!/usr/bin/env node
/* DRY RUN ONLY. Prints what the one sheet-name rule (charm-nest-sheet-name.js) changes for a Library read, and what number a sheet
 * would take if it joined a set. It reads a JSON file and writes nothing anywhere: no network, no Firestore, no document edit.
 *
 *   node scripts/sets-number-dryrun.cjs --sheets <listSheets.json> [--sets <setGet-or-setList.json>] [--would-join <sheetId>:<setId>]... [--json]
 *
 *   --sheets      the answer of op listSheets (or an object with a `sheets` array, or the array); one read, kept in a file
 *   --sets        a set record (op setGet: {set}) or a list ({sets:[...]}), for the set numbers and the highest numbers each set gave out
 *   --would-join  repeatable: the number, file name and QR label text the sheet would get from the app's own set-member move
 *                 (op flowApply, step setMember, charm-nest-set-edit.js), and whether that move is allowed for that set now
 *
 * Why there is no "apply": a sheet's number is the one thing nothing may rewrite (a QR label or a laser file may carry it), and the
 * app has no operation that renumbers a sheet; a name that reads wrong is read from the record by the rule, so the fix for the two
 * "SS Sheet 1" is the rule, not a write. A sheet joins or leaves a set only through the Library's own move (drag and drop, or Include),
 * which checks every rule again on the server. This script prints the numbers that move would give; Paul decides the move. */
'use strict';
const fs = require('node:fs');
const path = require('node:path');
const SN = require('../charm-nest-sheet-name.js');
const SE = require('../charm-nest-set-edit.js');
const OR = require('../charm-nest-orders.js');

const legacyName = r => `${SN.CODE[r.metal] || ''} Sheet ${r.sheetIndex || (SN.fileTag(r) || {}).no || r.page || 1}`.trim();   // what the cards said before: sheetIndex, else the file's number, else the page
const readJson = f => JSON.parse(fs.readFileSync(path.resolve(f), 'utf8'));

function whyOf(r) {
  const set = SN.setNoOf(r), no = SN.numberOf(r);
  if (SN.isCut(r) && !SN.inSet(r)) return `cut: it keeps the number its file and label carry (${no || r.page || 1})`;
  if (SN.inSet(r)) return `in Set ${set || '?'}: its number there is ${no || '?'} (its page ${r.page || '?'} in the run is not a number in the set)`;
  const was = SN.was(r);
  return `outside every set: a provisional name from its page ${SN.draftNo(r)} in the run; it gets its number when it joins a set${was ? `; it was ${was} (its file name keeps that)` : ''}`;
}
// the QR label picture(s) a sheet already carries: the text that was printed on it when the list has it, else the picture's file name (which carries the set and number)
const labelOf = r => ((r.label && r.label.files) || []).map(f => f.label || String(f.path || '').split('/').pop()).filter(Boolean);

/** rows: Library rows; sets: set records ({setId, seq, day, sheetIds, sheetNos, committedAt...}). Returns { rows, clashesBefore, clashesAfter, joins }. */
function plan(rows, sets, wouldJoin) {
  const out = rows.filter(r => r && r.id && !r.archived).map(r => {
    const before = legacyName(r), after = SN.name(r), labels = labelOf(r);
    return { id: r.id, metal: r.metal, page: r.page || null, sheetIndex: r.sheetIndex || null, set: SN.inSet(r) ? SN.setNoOf(r) || null : null, file: r.fileBase || r.folder || null, before, after, changes: before !== after,
      why: whyOf(r), stored: 'none (the name is read from the record)', qrLabels: labels, printedSetLabels: false };
  });
  const dupes = key => { const by = new Map(); for (const x of out) { const k = key(x); (by.get(k) || by.set(k, []).get(k)).push(x.id); } return [...by].filter(([, ids]) => ids.length > 1).map(([name, ids]) => ({ name, ids })); };
  const setOf = id => (sets || []).find(s => s && s.setId === id) || null;
  const joins = [];
  for (const spec of wouldJoin || []) {
    const [sheetId, setId] = String(spec).split(':'), rec = rows.find(r => r.id === sheetId), set = setOf(setId);
    if (!rec || !set) { joins.push({ sheetId, setId, error: !rec ? 'that sheet is not in the read' : 'that set is not in the read' }); continue; }
    const members = rows.filter(r => !r.archived && r.id !== sheetId && r.setId === setId && SN.inSet(r)), nos = SN.raise(set.sheetNos, members);
    const fileFor = n => OR.sheetName(rec.metal, set.day, set.seq, n);
    const n = SE.indexFor(rec, members, nos, fileFor), v = SE.verifyMoves({ moves: [{ id: sheetId, to: setId }], recs: { [sheetId]: rec }, sets: { [setId]: { doc: set, members } }, others: rows });
    const joined = Object.assign({}, rec, { setId, draft: false, solidIncluded: rec.solidIncluded === false ? true : rec.solidIncluded, sheetIndex: n, setSeq: set.seq, fileBase: fileFor(n) });
    joins.push({ sheetId, setId, allowed: v.ok, why: v.ok ? '' : v.reasons.map(x => x.label).join('; '), number: n, name: SN.name(joined), fileBase: fileFor(n), qrLabelText: `${OR.CARD_TAG[rec.metal] || rec.metal} · ${set.name || 'Set-' + set.seq} · Sheet ${n}`, qrLabelRemade: true, setLabelsRemade: true });
  }
  return { rows: out, clashesBefore: dupes(x => x.before), clashesAfter: dupes(x => x.after), joins };
}

function main(argv) {
  const get = k => { const i = argv.indexOf(k); return i >= 0 ? argv[i + 1] : null; }, all = k => argv.flatMap((a, i) => (a === k ? [argv[i + 1]] : []));
  if (!get('--sheets')) { console.error('usage: node scripts/sets-number-dryrun.cjs --sheets <listSheets.json> [--sets <file>] [--would-join <sheetId>:<setId>]... [--json]'); process.exit(2); }
  const sj = readJson(get('--sheets')), rows = Array.isArray(sj) ? sj : sj.sheets || [];
  let sets = []; if (get('--sets')) { const x = readJson(get('--sets')); sets = Array.isArray(x) ? x : x.sets ? x.sets : x.set ? [x.set] : []; }
  const p = plan(rows, sets, all('--would-join'));
  if (argv.includes('--json')) { console.log(JSON.stringify(p, null, 1)); return; }
  console.log('DRY RUN: nothing is written. Names as the cards said them, and as the one rule reads them.\n');
  for (const x of p.rows) console.log(`${x.id}\n   ${x.before}  ->  ${x.after}${x.changes ? '' : '   (unchanged)'}\n   why: ${x.why}\n   stored: ${x.stored}${x.qrLabels.length ? `\n   QR label picture on file: ${x.qrLabels.join(' | ')}${x.changes ? '' : ' (same number: stays as printed)'}` : ''}`);
  console.log(`\nnames shared by two sheets before: ${p.clashesBefore.length ? p.clashesBefore.map(c => `${c.name} (${c.ids.join(', ')})`).join('; ') : 'none'}`);
  console.log(`names shared by two sheets after:  ${p.clashesAfter.length ? p.clashesAfter.map(c => `${c.name} (${c.ids.join(', ')})`).join('; ') : 'none'}`);
  for (const j of p.joins) console.log(j.error ? `\nwould join ${j.sheetId}:${j.setId}: ${j.error}` : `\nwould join ${j.sheetId} -> ${j.setId}: ${j.allowed ? 'allowed by the Library' : 'not by the Library move (' + j.why + '): that set takes sheets through its own run page (Include / drag and drop there)'}; the number it would take is ${j.number}: ${j.name}, file ${j.fileBase}; its QR label ("${j.qrLabelText} · k orders") and the set's labels are remade by the page`);
}
if (require.main === module) main(process.argv.slice(2));
module.exports = { plan, legacyName };
