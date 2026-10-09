// ADVLIFE fuzz (pairs-1009 phase 2): random sequences of what the PAGE does to an order line (hold and cancel of the whole line that is not cut, release,
// a sheet cut at the laser, Delete sheet, move a sheet out of or into a set) through the REAL server ops over the strict fake Firestore. After every op:
// a taken-off piece is on no editable sheet; no piece is on two sheets; a row that names a sheet is listed by it; a row on no sheet is listed by none;
// a line is taken off whole (or what is left is on cut sheets); side and mirror never change; getOrderPieces has nothing to repair.
// A stale tab naming the whole line (staleHold, staleCancel) is in; a stale SHEET SAVE is not (it can re-list a released piece: ADVLIFE-findings.md finding 12).
//   node tests/charm-nest/pairs-adv-life-fuzz.cjs [seed] [runs]
const F = require('./pairs-fixtures.cjs');
const NOW = Date.now();
let seed = +process.argv[2] || 7; const rnd = () => (seed = (seed * 1664525 + 1013904223) % 4294967296) / 4294967296, pick = a => a[Math.floor(rnd() * a.length)];
(async () => {
  const bad = [];
  for (let run = 0; run < (+process.argv[3] || 150); run++) {
    const orders = [
      { rid: F.rid(1), lines: [{ n: 10, kind: 'pair', on: ['sh-gf1', 'sh-gf2'] }] },
      { rid: F.rid(2), lines: [{ n: 10, kind: 'pair', qty: 2, on: ['sh-gf1', 'sh-gf1', 'sh-gf2', 'sh-gf3'] }, { n: 11, kind: 'single', on: 'sh-gf3' }] },
      { rid: F.rid(3), lines: [{ n: 10, kind: 'discs', discs: 3, on: ['sh-gf2', 'sh-gf3', 'sh-gf3'] }] },
      { rid: F.rid(4), lines: [{ n: 10, kind: 'mismatched', on: ['sh-gf3', 'sh-gf3'] }, { n: 11, kind: 'single', on: 'sh-gf1' }] }];
    const w = F.world({ orders, tracked: orders.flatMap(o => o.lines.map(l => F.groupKey(o.rid, F.tx(l.n)))) });
    for (const s of w.sets) s.committedAt = NOW - 3600000, s.status = 'committed';
    const fsx = F.fakeFirestore(), fns = F.functions(fsx, ['charmNestLibrary']); fsx.seed(w);
    const code = process.env.CHARM_NEST_DELETE_CODE;
    const lines = orders.flatMap(o => o.lines.map(l => ({ rid: o.rid, n: l.n, ids: w.pool.filter(p => p.orderId === o.rid && p.transactionId === F.tx(l.n)).map(p => p.poolId) })));
    const log = [];
    const check = (why) => {
      const pool = new Map(fsx.list('Charm_Pool').map(r => [r.poolId || r._id, r])), sheets = fsx.list('Charm_Nest_Sheets').filter(s => !s.archived);
      const listed = new Map();
      for (const s of sheets) for (const id of s.poolIds || []) { (listed.get(id) || listed.set(id, []).get(id)).push(s); }
      const cutOf = s => +s.laserDoneAt > 0;
      for (const [id, r] of pool) {
        const takenOff = ['abandoned', 'superseded'].includes(r.state);
        const L = listed.get(id) || [];
        if (takenOff && L.length) bad.push(`run ${run} after ${why}: ${id} is ${r.state} but still listed on ${L.map(s => s.id + (cutOf(s) ? ' (cut)' : ''))}`);
        if (L.length > 1) bad.push(`run ${run} after ${why}: ${id} listed on ${L.map(s => s.id)}`);
        if (!takenOff && r.sheetId && !L.some(s => s.id === r.sheetId) && fsx.get('Charm_Nest_Sheets', r.sheetId)) bad.push(`run ${run} after ${why}: ${id} row says ${r.sheetId} (state ${r.state}) but that sheet does not list it`);
        if (!takenOff && !r.sheetId && L.length) bad.push(`run ${run} after ${why}: ${id} row has no sheet (state ${r.state}) but ${L.map(s => s.id)} lists it`);
      }
      // a line is whole: all its non-kept rows share the take-off state
      for (const ln of lines) { const st = ln.ids.map(id => pool.get(id)).filter(Boolean); const off = st.filter(r => ['abandoned', 'superseded'].includes(r.state)); if (off.length && off.length < st.length) { const liveOnes = st.filter(r => !['abandoned', 'superseded'].includes(r.state)); const allCut = liveOnes.every(r => (listed.get(r.poolId) || []).some(cutOf)); if (!allCut) bad.push(`run ${run} after ${why}: line ${ln.rid}:${ln.n} half taken off: ${st.map(r => r.poolId.slice(-3) + ':' + r.state + ':' + (r.sheetId || '-')).join(' ')}`); } }
      // sides never change
      for (const p of w.pool) { const r = pool.get(p.poolId); if (r && (r.side !== p.side || r.mirror !== p.mirror)) bad.push(`run ${run} after ${why}: ${p.poolId} side/mirror ${p.side}/${p.mirror} became ${r.side}/${r.mirror}`); }
    };
    const checkPieces = async (why) => { for (const o of orders) { const r = await fns.lib('getOrderPieces', { orderId: o.rid }); const x = r.orders && r.orders[o.rid]; if (x && x.repaired && x.repaired.length) bad.push(`run ${run} after ${why}: getOrderPieces ${o.rid} repaired ${JSON.stringify(x.repaired.map(y => [y.kind, y.poolId.slice(-3)]))}`); } };
    for (let step = 0; step < 8; step++) {
      const op = pick(['hold', 'hold', 'cancel', 'staleHold', 'staleCancel', 'release', 'release', 'cut', 'delete', 'move']);
      const ln = pick(lines), rows = ln.ids.map(id => fsx.get('Charm_Pool', id));
      let what = op + ' ' + ln.rid.slice(-2) + ':' + ln.n;
      try {
        if (op === 'staleHold' || op === 'staleCancel') {
          // a tab that does not know a sheet was cut meanwhile names the WHOLE line: the pieces only a cut sheet lists are left alone (kept), the rest comes off
          const patch = op === 'staleHold' ? { state: 'abandoned', sheetId: null, setId: null, heldBy: 'Paul', heldReason: 'on hold', heldAt: NOW } : { state: 'abandoned', sheetId: null, setId: null, removedBy: 'Paul', removedReason: 'cancelled: x', removedAt: NOW };
          await fns.lib('poolUpdate', { poolIds: ln.ids, patch, by: 'Paul' });
        } else if (op === 'hold' || op === 'cancel') {
          // page-like: name every piece of the line that is not cut-listed and not already off
          const sheets = fsx.list('Charm_Nest_Sheets');
          const cut = id => sheets.some(s => +s.laserDoneAt > 0 && (s.poolIds || []).includes(id));
          const names = ln.ids.filter(id => { const r = fsx.get('Charm_Pool', id); return r && !['abandoned', 'superseded'].includes(r.state) && !cut(id); });
          // together rule: if a piece of the line is cut, nothing of the line is named
          const anyCut = ln.ids.some(cut); const use = anyCut ? [] : names;
          if (use.length) { const patch = op === 'hold' ? { state: 'abandoned', sheetId: null, setId: null, heldBy: 'Paul', heldReason: 'on hold', heldAt: NOW } : { state: 'abandoned', sheetId: null, setId: null, removedBy: 'Paul', removedReason: 'cancelled: x', removedAt: NOW }; await fns.lib('poolUpdate', { poolIds: use, patch, by: 'Paul' }); }
        } else if (op === 'release') {
          const off = ln.ids.filter(id => { const r = fsx.get('Charm_Pool', id); return r && ['abandoned', 'superseded'].includes(r.state); });
          if (off.length) {
            const pools = ln.ids.map(id => { const r = fsx.get('Charm_Pool', id); return Object.assign({}, r, { state: 'ready', sheetId: null, setId: null, sheetName: null, quantity: ln.ids.length }); });
            const rr = await fns.lib('poolPut', { pools });
            // place the unplaced ones on an open (uncut) sheet: the mate's sheet if it is uncut, else gf4
            const sheets = fsx.list('Charm_Nest_Sheets'); const cut = s => +s.laserDoneAt > 0;
            const mateSheet = sheets.find(s => !cut(s) && (s.poolIds || []).some(id => ln.ids.includes(id)));
            const target = mateSheet ? mateSheet.id : 'sh-gf4'; const tsheet = fsx.get('Charm_Nest_Sheets', target);
            const unplaced = ln.ids.filter(id => { const r = fsx.get('Charm_Pool', id); return r && !r.sheetId && !['abandoned', 'superseded'].includes(r.state); });
            if (unplaced.length && tsheet && !cut(tsheet)) { const put = await fns.lib('putSheet', { sheet: { id: target, poolIds: [...(tsheet.poolIds || []), ...unplaced] } }); if (put.error) what += ' PUTERR ' + put.error.slice(0, 80); else await fns.lib('poolUpdate', { poolIds: unplaced, patch: { state: 'written', sheetId: target, setId: tsheet.setId || null } }); }
          }
        } else if (op === 'cut') {
          const s = pick(fsx.list('Charm_Nest_Sheets')); if (s && !(+s.laserDoneAt > 0)) { fsx.put('Charm_Nest_Sheets', s.id, Object.assign({}, s, { laserDoneAt: NOW - 1000, laserDoneBy: 'Laser Lee' })); what += ' ' + s.id; }
        } else if (op === 'delete') {
          const s = pick(fsx.list('Charm_Nest_Sheets').filter(s => !(+s.laserDoneAt > 0))); if (s) { await fns.lib('deleteSheet', { id: s.id, code }); what += ' ' + s.id; }
        } else if (op === 'move') {
          const s = pick(fsx.list('Charm_Nest_Sheets')); if (!s) continue; const to = s.setId ? null : 'set-1'; const r = await fns.lib('flowApply', { by: 'Paul', steps: [{ type: 'setMember', moves: [{ sheetId: s.id, to }] }] }); what += ' ' + s.id + '->' + to + (r.error ? ' (' + r.status + ')' : '');
        }
      } catch (e) { bad.push(`run ${run} step ${step} ${what}: threw ${e.message}`); }
      log.push(what); check(log.join(' | ')); await checkPieces(log.join(' | '));
    }
    fns.restore();
  }
  console.log('pairs-adv-life-fuzz: ' + bad.length + ' violations'); for (const b of bad.slice(0, 12)) console.log(' - ' + b.slice(0, 600));
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
