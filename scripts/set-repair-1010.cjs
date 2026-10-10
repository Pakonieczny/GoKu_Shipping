#!/usr/bin/env node
/* set-repair-1010: the STAGED repair for the sandbox Library of 10 Oct 2026 (Paul's image 1: "Set-1" with three completed GF sheets and a
 * partial SS sheet, and a completed GF sheet and a completed SS sheet outside every set). SETFORM, 2026-10-10.
 *
 * THIS SCRIPT WRITES NOTHING, ever. It has no write path at all: it reads two cached answers of the Library's own read ops (op listSheets and
 * op setList, GET, sandbox) and prints, as plain lines, exactly which sheet would move from where to where, and why, by the rules the app
 * itself uses (charm-nest-set-rules.js, charm-nest-shared-orders.js, charm-nest-orders.js sheetRelease). It exists so that Paul can decide.
 *
 *   node scripts/set-repair-1010.cjs [--cache /tmp/SETFORM/cache]        dry run over the cache (default)
 *   node scripts/set-repair-1010.cjs --refresh                           first read the two lists again (two GETs, sandbox, small) into the cache
 *
 * HOW THE MOVES ARE MADE (when Paul says so): never by this script and never by writing documents. The page's own assembly (Gate.assembleNow)
 * joins a completed sheet to the open set, with its QR label, by itself the moment nothing holds it back. What holds the completed GF Sheet 1
 * and SS Sheet 1 back is the Rose Gold Sheet 1 they share orders with (the cardinal rule): a Rose Gold sheet joins a set only by its own Cut
 * Sheet press. That press adds a green dash line and records a cut, so it is Paul's own explicit yes, never part of a repair. After it the
 * assembly joins the three sheets together on its own; this script's output lists those joins.
 */
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '..');
const Rules = require(path.join(root, 'charm-nest-set-rules.js'));
const SO = require(path.join(root, 'charm-nest-shared-orders.js')).core;
const O = require(path.join(root, 'charm-nest-orders.js'));

const idOf = s => String(s.id || s.sheetId);
const label = s => `${Rules.metalClass(s)} Sheet ${s.sheetIndex || s.page}${s.sheetIndex ? '' : ' (not in a set)'}`;
const pct = s => `${Math.round((+s.density || 0) * 100)}%`;
const inSet = s => (s.setId && !s.draft && s.solidIncluded !== false) ? String(s.setId) : null;

/** The one open (not yet committed) set of a list of set records, or null. */
const openSetOf = sets => (sets || []).filter(x => x && !(+x.committedAt > 0) && !/^complete|superseded/.test(String(x.status || ''))).sort((a, b) => (+a.seq || 0) - (+b.seq || 0))[0] || null;

/** What the release rule (charm-nest-orders.js sheetRelease, the one the page's assembly reads) says of a sheet for the open set of number `seq`. */
const release = (m, seq) => O.sheetRelease({ material: m.metal, verified: !!(m.verification && m.verification.ok), placed: +m.placedCount, stopped: m.endedBy === 'stopped', dirty: false, full: !!m.releaseFull, topup: false },
  { seq, selected: ['gold10k', 'gold14k'].includes(m.metal) ? { [m.metal]: m.solidIncluded === true } : {} });

/** why a sheet cannot join a set by itself (the page's Gate.cannotJoin): '' when it can */
function cannotJoin(s) {
  if (s.metal === 'rose') return 'a Rose Gold sheet joins a set only by its own Cut Sheet press (it adds the green dash line and records a cut: Paul\'s explicit yes)';
  if (s.endedBy === 'stopped') return 'its nesting was stopped';
  if (!(s.verification && s.verification.ok)) return 'its layout is not verified';
  if (!(+s.placedCount > 0)) return 'nothing is placed on it';
  return '';
}

/**
 * plan(sheets, sets) -> { open, before, moves[], waits[], stays[], afterIfAllMove, afterNow, writes: [] }
 *   moves   [{ sheetId, label, to, toName, asLabel, why, needs:[{key,detail}] }]  needs: what only a person can say before the move can happen
 *   waits   [{ sheetId, label, why }] completed sheets that stay out, with the reason
 *   stays   [{ sheetId, label, why }] sheets that are not touched (partial sheets outside, sheets of other metals)
 */
function plan(sheets, sets) {
  const all = (sheets || []).filter(s => s && !s.archived), open = openSetOf(sets);
  const out = { open: open ? { setId: open.setId, name: open.name || `Set-${open.seq}`, seq: open.seq } : null, before: null, moves: [], waits: [], stays: [], writes: [] };
  const members = open ? all.filter(s => inSet(s) === String(open.setId)) : [];
  const waiting = all.filter(s => !inSet(s) && !(+s.laserDoneAt > 0));
  out.before = Rules.gate(open || {}, members);
  const core = all.map(s => SO.sheetOf(s, { label: label(s) })).filter(Boolean), byId = new Map(all.map(s => [idOf(s), s]));
  const groupOf = id => SO.groupOf(core, id);
  const taken = new Map();   // metal -> highest sheetIndex given so far in the open set
  for (const m of members) taken.set(m.metal, Math.max(taken.get(m.metal) || 0, +m.sheetIndex || 0));
  const decided = new Set();
  for (const s of waiting) {
    const id = idOf(s); if (decided.has(id)) continue;
    const g = groupOf(id), mates = g.ids.map(i => byId.get(i)).filter(Boolean), orders = g.orders;
    const ofGroup = mates.filter(m => inSet(m) !== (open && String(open.setId)));
    const seq = open ? +open.seq || 2 : 2, wantsIn = ofGroup.filter(m => Rules.isCompleted(m) && release(m, seq).include);
    if (!wantsIn.length) { for (const m of ofGroup) { decided.add(idOf(m)); out.stays.push({ sheetId: idOf(m), label: label(m), why: Rules.isCompleted(m) ? release(m, seq).reason.toLowerCase() + ' (it joins only by its own choice, never by being complete)' : `still filling (${pct(m)}): a partial sheet never makes a set on its own and never takes the place of a completed one` }); } continue; }
    for (const m of ofGroup) decided.add(idOf(m));
    const stuck = ofGroup.filter(m => cannotJoin(m));
    for (const m of ofGroup) {
      if (!open) { out.waits.push({ sheetId: idOf(m), label: label(m), why: 'there is no open set' }); continue; }
      const idx = (taken.get(m.metal) || 0) + 1; taken.set(m.metal, idx);
      const needs = stuck.length ? stuck.map(x => ({ key: x === m ? 'press' : 'mate', detail: x === m ? cannotJoin(m) : `${label(x)} must join with it: ${cannotJoin(x)}` })) : [];
      const why = Rules.isCompleted(m) ? `completed (${pct(m)}) and waiting outside every set: it joins the open set` : `shares ${orders.length === 1 ? 'order ' + orders[0] : 'orders ' + orders.join(', ')} with ${mates.filter(x => x !== m).map(label).join(' and ')}: sheets that share a multi-piece order are always in one set`;
      out.moves.push({ sheetId: idOf(m), label: label(m), to: String(open.setId), toName: out.open.name, asLabel: `${Rules.metalClass(m)} Sheet ${idx}`, why, needs, group: g.ids.length > 1 ? orders : [] });
    }
  }
  for (const s of waiting) if (!decided.has(idOf(s))) out.stays.push({ sheetId: idOf(s), label: label(s), why: Rules.isCompleted(s) ? 'completed, but another set rule holds it' : Rules.whyNotCompleted(s) });
  const joined = out.moves.map(m => byId.get(m.sheetId));
  out.afterIfAllMove = Rules.validSet(members.concat(joined));
  out.afterNow = Rules.validSet(members.concat(out.moves.filter(m => !m.needs.length).map(m => byId.get(m.sheetId))));
  return out;
}

function say(p) {
  const L = [];
  L.push(`DRY RUN. Nothing is written, now or by this script ever.`);
  L.push(p.open ? `Open set: ${p.open.name}. ${p.before.applies && !p.before.ok ? p.before.line : 'It meets the principle.'}` : 'There is no open set.');
  if (!p.moves.length) L.push('Nothing would move.');
  for (const m of p.moves) {
    L.push(`${m.label} moves from no set to ${m.toName} as ${m.asLabel}: ${m.why}.`);
    for (const n of m.needs) L.push(`   only after: ${n.detail}`);
  }
  for (const w of p.waits) L.push(`${w.label} stays out: ${w.why}.`);
  for (const s of p.stays) L.push(`${s.label} is not touched: ${s.why}.`);
  L.push(`${p.open ? p.open.name : 'The open set'} after these moves: ${p.afterIfAllMove.ok ? 'valid, ' + p.afterIfAllMove.have.GF + ' completed GF and ' + p.afterIfAllMove.have.SS + ' completed SS sheet(s)' : p.afterIfAllMove.reason}`);
  L.push(`${p.open ? p.open.name : 'The open set'} if nothing is decided (what can move on its own): ${p.afterNow.ok ? 'valid' : p.afterNow.reason + ' It stays as it is, flagged, never dissolved.'}`);
  L.push('Everything else stays exactly as it is: no seal, label, approval, cut, order, sheet record or file is touched.');
  return L;
}

module.exports = { plan, say, openSetOf, cannotJoin };

if (require.main === module) {
  const args = process.argv.slice(2), cache = args.includes('--cache') ? args[args.indexOf('--cache') + 1] : '/tmp/SETFORM/cache';
  const read = f => JSON.parse(fs.readFileSync(path.join(cache, f), 'utf8'));
  if (args.includes('--refresh')) {
    // two small reads through the Library's own GET read ops (sandbox): never a write op
    const { execFileSync } = require('child_process'), base = 'https://goldenspike.app/.netlify/functions/charmNestLibrary';
    fs.mkdirSync(cache, { recursive: true });
    for (const [f, q] of [['sheets.json', 'op=listSheets&sandbox=1&excludeDone=1&limit=60'], ['sets.json', 'op=setList&sandbox=1&excludeDone=1&limit=5']]) fs.writeFileSync(path.join(cache, f), execFileSync('curl', ['-sS', '-m', '60', `${base}?${q}`]));
  }
  console.log(say(plan(read('sheets.json').sheets, read('sets.json').sets)).join('\n'));
}
