// KEEPREV helper (order 4242795385, 22 Nov 2026): the APPROVECLOUD confirm loop vs the cloud saving the approved back, on the REAL charm-nest-engrave-sync.js (the page around it stood in, an in-memory engraveSyncPut/Get with ENGREV). Run by engrave-keeprev.cjs.
// usage: node sim-confirm-race.cjs <path to charm-nest-engrave-sync.js> [variant]
//   variant "feedFirst"  : the feed's notice of the finalize reaches the page before the loop's next send (the order seen on 22 Nov)
//   variant "readFirst"  : the finalize lands between the loop's send and its read-back
const fs = require('fs'), vm = require('vm');
const file = process.argv[2], variant = process.argv[3] || 'feedFirst';
const KEY = '4242795385_5305127482', ORDER = '4242795385', DEV = 'dlbj6s3ua';
const PLACE3 = { centre: [16.97, 17.12], angle: 0, size: 6.565, lines: ['A+E+T'], weight: 'Semibold', lineGap: 0.216, fittedMax: 8.33, upAngle: 90, cx: 16.89, cy: 20.67, fontKey: 'source-sans-3' };
const PLACE1 = Object.assign({}, PLACE3, { centre: [15.59, 16.52], size: 6.9 }), PLACE2 = Object.assign({}, PLACE3, { centre: [15.76, 16.3], size: 6.9 });
const T1 = 1795398545130, T2 = 1795398559174, T5 = 1795403642166;

// ── the cloud (Charm_Nest_Engrave/<line>, ENGREV) ──
const cloud = { rev: 10, rec: null, at: 0, dev: null };
const cjob = (key, state, place, extra) => Object.assign({ key, slot: key.split('#')[1], text: 'A+E+T', lines: ['A+E+T'], lineInput: ['A+E+T'], fontKey: 'source-sans-3', state, place }, extra || {});
cloud.rec = { v: 1, override: null, engrave: { needed: true, state: 'review' }, raised: true, jobs: [
  cjob(KEY + '#C1', 'written', PLACE1, { approvedAt: T1, approvedBy: 'Michelle_R' }), cjob(KEY + '#C2', 'written', PLACE2, { approvedAt: T2, approvedBy: 'Michelle_R' }), cjob(KEY + '#C3', 'review', PLACE3, { approvedAt: null, approvedBy: null })] };
const writes = [];
function engraveSyncPut(b) {
  const out = [], stale = [];
  for (const l of b.lines) {
    if (l.baseRev != null && l.baseRev !== cloud.rev) { stale.push({ key: l.key, rev: cloud.rev }); continue; }
    const rec = JSON.parse(JSON.stringify(l.rec));
    // keepApproving (the real server keeps a job's `approving` while the cloud writes its file)
    for (const nj of rec.jobs) { const cj = cloud.rec.jobs.find(x => x.key === nj.key && x.approving); if (cj && ['review', 'ready', 'fitting'].includes(nj.state) && !(+nj.approvedAt >= +cj.approving.at)) nj.approving = cj.approving; }
    cloud.rec = rec; cloud.rev++; cloud.dev = b.dev; writes.push({ by: b.dev, rev: cloud.rev, c3: rec.jobs.find(j => j.key.endsWith('#C3')).state });
    out.push({ key: l.key, at: Date.now(), rev: cloud.rev });
  }
  return Object.assign({ ok: true, lines: out }, stale.length ? { stale } : {});
}
const engraveSyncGet = () => ({ lines: [{ key: KEY, order: ORDER, rec: JSON.parse(JSON.stringify(cloud.rec)), at: Date.now(), rev: cloud.rev, by: 'x', dev: cloud.dev, sig: null }] });
const cloudJob3 = () => cloud.rec.jobs.find(j => j.key.endsWith('#C3'));
function cloudApprovePress() { const j = cloudJob3(); j.approving = { at: T5, by: 'Michelle_R', text: 'A+E+T' }; j.state = 'review'; cloud.rev++; cloud.dev = 'cn-dev-other'; writes.push({ by: 'engraveApprove', rev: cloud.rev, c3: 'review+approving' }); }
function cloudFinalize() { const j = cloudJob3(); if (!(j.approving && j.approving.at === T5)) { writes.push({ by: 'finalize', skipped: true }); return; } delete j.approving; Object.assign(j, { state: 'written', approvedAt: T5, approvedBy: 'Michelle_R' }); cloud.rev++; cloud.dev = null; writes.push({ by: 'cloud finalize', rev: cloud.rev, c3: 'written' }); }

// ── the page (Michelle's): its line and jobs ──
let now = 1795403640000;
const line = { key: KEY, state: 'pooled', order: { receiptId: ORDER }, spec: { engraveCandidate: true }, poolIds: [KEY + '_1', KEY + '_2', KEY + '_3'], engrave: { needed: true, state: 'review' } };
const pjob = (cj, pid) => ({ key: cj.key, slot: cj.slot, text: cj.text, lines: cj.lines.slice(), lineInput: cj.lineInput.slice(), fontKey: cj.fontKey, state: cj.state, approvedAt: cj.approvedAt, approvedBy: cj.approvedBy, copies: [pid], row: { parentRow: line },
  fit: { centre: cj.place.centre.slice(), size: cj.place.size, angle: 0, lines: cj.lines.slice(), weight: 'Semibold', fittedMax: 8.33, lineGap: 0.216 }, view: { upAngle: 90, cx: 16.89, cy: 20.67 } });
const jobs = cloud.rec.jobs.map((cj, i) => pjob(cj, line.poolIds[i])), items = new Map(jobs.map(j => [j.key, j]));
const sb = { console, JSON, Math, Promise, Map, Set, Object, Array, String, Number, Error, setTimeout, clearTimeout, setInterval, clearInterval };
sb.self = sb;
sb.Engrave = { items: () => items, jobsOf: () => jobs, ensureJobs: () => {}, render: () => {},
  fitJob: async j => { await new Promise(r => setTimeout(r, 5)); const k = j.keepPlace; j.fit = { centre: k.centre.slice(), size: k.size, angle: k.angle || 0, lines: k.lines.slice(), weight: k.weight, fittedMax: k.fittedMax, lineGap: k.lineGap }; j.view = { upAngle: 90, cx: 16.89, cy: 20.67 }; j.state = 'review'; return j; } };
sb.Orders = { rows: () => [line], render: () => {} };
sb.Session = { schedule: () => {} };
sb.Pool = { charmOf: () => ({ outline: {} }), previewCharmOf: () => null };
sb.CharmNestMadeThumb = { state: () => ({ dev: DEV }), changed: () => {} };
sb.CN = { api: async (fn, body) => { await new Promise(r => setTimeout(r, 1)); if (body.op === 'engraveSyncPut') return engraveSyncPut(body); if (body.op === 'engraveSyncGet') return engraveSyncGet(body); throw new Error('op ' + body.op); } };
vm.createContext(sb);
vm.runInContext(fs.readFileSync(file, 'utf8'), sb, { filename: 'charm-nest-engrave-sync.js' });
vm.runInContext('Date.now = () => globalThis.__now;', sb);
const at = ms => { now = ms; sb.__now = ms; };
const ES = sb.CharmNestEngraveSync, want = [{ key: KEY + '#C3', face: 'back', states: ['approved', 'written'], approvedAt: 0 }];
const settle = () => new Promise(r => setTimeout(r, 80));

(async () => {
  at(now); await ES.pull([ORDER], 'keys'); await settle();          // the page has read the line (rev 10)
  at(now + 1000); cloudApprovePress();                               // 22:14:02 Approve: engraveApprove (rev 11)
  at(now + 1000); let r = await ES.confirm(KEY, want); await settle(); console.log('  loop 1:', r.ok ? 'ok' : r.why);
  at(now + 2000); r = await ES.confirm(KEY, want); await settle(); console.log('  loop 2:', r.ok ? 'ok' : r.why);   // LOCAL_AT = now (own press for 10 s)
  if (variant === 'feedFirst') {
    at(now + 4000); cloudFinalize();                                  // 22:14:27 the cloud saved the file and wrote C3 written
    at(now + 5000); await ES.pull([ORDER], 'keys'); await settle(); console.log('  feed take:', ES.why(KEY));
    at(now + 10000); r = await ES.confirm(KEY, want); await settle(); console.log('  loop 3:', r.ok ? 'ok' : r.why);
  } else if (variant === 'sendFirst') {
    at(now + 4000); cloudFinalize();                                  // the finalize, then a send of the page BEFORE the feed's notice (several loops sending every 1-4 s)
    at(now + 4500); r = await ES.confirm(KEY, want); await settle(); console.log('  loop 3:', r.ok ? 'ok' : r.why);
    at(now + 5000); await ES.pull([ORDER], 'keys'); await settle(); console.log('  feed take:', ES.why(KEY));
  } else {
    // the finalize lands between the loop's send and its read: patch the api once
    const api0 = sb.CN.api; let armed = true;
    sb.CN.api = async (fn, body) => { const res = await api0(fn, body); if (armed && body.op === 'engraveSyncPut') { armed = false; cloudFinalize(); } return res; };
    at(now + 10000); r = await ES.confirm(KEY, want); await settle(); console.log('  loop 3:', r.ok ? 'ok' : r.why);
    sb.CN.api = api0;
    // a later write of this line by the page (any press on the line within 20 s: here, the sweep's own send path through confirm)
    at(now + 15000); r = await ES.confirm(KEY, want); await settle(); console.log('  later write:', r.ok ? 'ok' : r.why);
  }
  for (let i = 0; i < 3; i++) { at(now + 18000 + i * 8000); r = await ES.confirm(KEY, want); await settle(); }
  const c3 = cloudJob3(), p3 = items.get(KEY + '#C3');
  console.log('  writes:', writes.map(w => `${w.by}${w.rev ? '@' + w.rev : ''}:${w.c3 || ''}`).join('  '));
  console.log(`RESULT ${variant}: cloud C3 = ${c3.state} approvedAt=${c3.approvedAt} · page C3 = ${p3.state} approvedAt=${p3.approvedAt} · C1/C2 cloud = ${cloud.rec.jobs[0].state}/${cloud.rec.jobs[1].state}`);
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
