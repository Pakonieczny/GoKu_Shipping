// ONEBAD + OWNFILE (Paul, 23 Nov 2026: "why would you stop a sheet from being fully filled just because there's a custom order?"): SS Draft 11 placed nothing for 16 hours: one custom piece (order 4241324766) waiting on it was
// "not in the library", and the writer threw away every finished nest of the sheet. The real _charmNestFinishHost.js makePieces over the in-memory cloud (_cloudFakes.cjs):
//   1  a WAITING piece that cannot be made never stops the sheet (waiting, by name); a piece that HAS a place and cannot be made still does (missing)
//   2  a custom piece whose file is not in the sheet's own list is read from its record's own drawing (aiPath), in the nest, the writer and the engraving
//   ONEBAD_ROOT=<another checkout> NODE_PATH=<deps> node tests/charm-nest/one-bad-piece.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const fs = require('fs'), path = require('path'), root = process.env.ONEBAD_ROOT ? path.resolve(process.env.ONEBAD_ROOT) : path.join(__dirname, '../..');
const F = require(path.join(root, 'tests/charm-nest/_cloudFakes.cjs'))({ now: Date.UTC(2026, 10, 23, 18, 12, 0) });
const log0 = console.log.bind(console); console.log = () => {}; console.warn = () => {}; let n = 0, failed = 0; const ok = (m, c, d) => { n++; log0((c ? 'OK   ' : 'FAIL ') + m + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 500))); if (!c) failed++; };
(async () => {
  const Host = F.load('_charmNestFinishHost.js'), HAWK = '4241324766_5302266048_1', SEAL = '4243367845_1_1', asked = [];
  F.seed('Charm_Pool/' + HAWK, { poolId: HAWK, orderId: '4241324766', sku: 'CUSTOM-H-014-433731', custom: true, aiPath: 'charmnest/custom/4241324766/648442e4bb11-NEW1.pdf', charmHash: 'aa9b632c' });
  F.seed('Charm_Pool/' + SEAL, { poolId: SEAL, orderId: '4243367845', sku: 'SEAL 1' });
  const bucket = { name: 'b', file: p => ({ name: p, getMetadata: async () => { asked.push(p); throw new Error('no such file'); }, download: async () => { asked.push(p); throw new Error('no such file'); }, exists: async () => { asked.push(p); return [false]; }, createReadStream: () => { asked.push(p); throw new Error('no such file'); } }) };
  const rec = o => Object.assign({ id: 'd-57', metal: 'silver', sources: [], placements: [], charms: [{ id: 'pool:hawk', poolId: HAWK, custom: true, sku: 'CUSTOM-H-014-433731', sourceId: 'pool:x', hash: 'aa9b632c' }, { id: 'pool:seal', poolId: SEAL, sku: 'SEAL 1' }] }, o || {});
  const make = r => Host.makePieces({ db: F.db, bucket, rec: r, S: { settings: {} }, read: async () => ({ ok: true }), log: () => {} });
  let m = await make(rec());
  ok('1 · two waiting pieces that cannot be made: the sheet is not stopped', (m.missing || []).length === 0 && (m.waiting || []).length === 2, { missing: m.missing, waiting: m.waiting });
  ok('1 · each is said by name for a person', (m.waiting || []).some(t => /4241324766/.test(t)) && (m.waiting || []).some(t => /4243367845/.test(t)), m.waiting);
  ok('2 · the custom piece is looked for in its own record\'s drawing, not called "no design in the library"', asked.some(p => p === 'charmnest/custom/4241324766/648442e4bb11-NEW1.pdf') && !(m.waiting || []).concat(m.missing || []).some(t => /CUSTOM-H-014-433731: no design in the library/.test(t)), { asked, w: m.waiting, mi: m.missing });
  m = await make(rec({ placements: [{ id: 'pool:seal', cxPt: 20, cyPt: 20, angle: 0, scale: 0.971 }] }));
  ok('1 · a piece that HAS a place and cannot be made still stops the write (the sheet is never cut without it)', (m.missing || []).length === 1 && /4243367845/.test(m.missing[0]) && (m.waiting || []).length === 1, { missing: m.missing, waiting: m.waiting });
  const src = f => fs.readFileSync(path.join(root, 'netlify/functions/' + f), 'utf8');
  ok('2 · the same in the nest and in the engraving', /own = \{ path: String\(pool\.aiPath\)/.test(src('_charmNestNestPieces.js')) && /own = \{ path: String\(pool\.aiPath\)/.test(src('_charmEngravePiece.js')));
  ok('1 · the writer stops only for a placed piece, and says the waiting ones', /if \(made\.missing\.length\) throw new Stop/.test(src('_charmNestFinish.js')) && /made\.waiting/.test(src('_charmNestFinish.js')));
  { const L = /const WRITER_FIELDS = \[([^\]]*)\]/.exec(src('_charmNestFinish.js'))[1]; ok('1 · the writer never writes the sheet\'s list of pieces: the waiting piece stays on it', !/"charms"|"poolIds"/.test(L)); }
  log0(`${failed ? 'FAIL' : 'PASS'} one-bad-piece (${n - failed} of ${n})`); process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL one-bad-piece: ' + (e && e.stack || e)); process.exit(1); });
