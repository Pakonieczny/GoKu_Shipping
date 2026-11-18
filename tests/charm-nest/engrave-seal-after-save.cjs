// SEALAFTERSAVE (Paul, 17 Nov 2026, option 2): "the approval seal and the engraveApproved timeline event only AFTER the back/front record is saved in the cloud".
// Before: the page stamped the seal, wrote the event and published "approved" first and saved the back a moment later; a save that failed left a seal with the OLD words in the files
// (order 4230448685's gold line: engraveApproved 'From the window / to the wall' 16:44:27, the sheet's backPool still the 16:43:58 record, the .dxf with the old words).
// Now a press on a placed piece waits in job.approving; writeBacksOnce finalizes it (state, seal, event) only after backPut answered. A failed save: no seal, no event, the reason, the old back kept.
// Controlled fixtures over the REAL production functions (charm-nest-bridge.js: the ENGSAVED/SEALAFTERSAVE section, writeBacks, writeBacksNow, writeBacksOnce): no browser, no network.
//   node tests/charm-nest/engrave-seal-after-save.cjs          (SAS_OLD=<file>: run the same checks against another copy of the bridge, e.g. `git show HEAD~1:charm-nest-bridge.js > /tmp/old.js`)
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const file = process.env.SAS_OLD || path.join(__dirname, '../../charm-nest-bridge.js');
const source = fs.readFileSync(file, 'utf8');
const a = source.indexOf('  /* ENGSAVED (Paul, 17 Nov 2026'), b = source.indexOf('  async function verifyBackFile', a);
assert(a > 0 && b > a, 'the ENGSAVED section and writeBacksOnce exist in the bridge');
const h0 = source.indexOf('  /* SEALAFTERSAVE (Paul'), h1 = source.indexOf('  async function prepareApproval(');
const section = (h0 >= 0 && h1 > h0 ? source.slice(h0, h1) : '') + source.slice(a, b);   // (the approval helpers sit before prepareApproval, shared by the approval code and the back writers)
let fails = 0; const check = (id, ok, ev) => { if (!ok) fails++; console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}${ev ? '  ' + ev : ''}`); };

const POOL = '4230448685_5289245250_1', AT = 1794951867705, OLD_AT = 1794843596745;
const rec = (approvedAt, text) => ({ poolId: POOL, sheetId: 'gold-s7', approvedAt, text, outputs: { ai: { url: 'https://x/a.ai' }, png: { url: 'https://x/a.png' } } });
function world(o = {}) {
  const log = [], said = [], added = [], uploads = [];
  const sheet = { sheetId: 'gold-s7', fileBase: 'GF_Nov.13.26_Set-1_Sheet-1', ...(o.noFolder ? { cloud: o.noFolder === 'none' ? {} : { ai: { path: 'charmnest/sets/x/GF_Nov.13.26_Set-1_Sheet-1.ai' } } } : { folderPath: 'charmnest/sets/x' }), metal: 'gold', backPool: (o.backPool || [rec(OLD_AT, 'From the window\nto the wall')]).slice(), charms: [{ poolId: POOL }] };
  const job = Object.assign({ key: 'k1', state: 'review', copies: [POOL], backs: [], stagedBacks: [], text: 'From the window\nto the wall', lines: ['From the window', 'to the wall'], lineInput: ['From the window', 'to the wall'], fontName: 'Source Sans 3', verify: { geometry: { ok: true } },
    fit: { glyphs: [], size: 8, capMm: 1.2, metrics: {}, centre: [0, 0], rect: null, angle: 0, weight: 1, small: false, thin: false }, view: { cutMembers: [], cx: 0, cy: 0, angleDeg: 0, upAngle: 0, checks: {}, detail: {} },
    row: { order: { receiptId: '4230448685' }, spec: { designSku: 'THOR_98338' }, line: { transactionId: '5289245250' }, engrave: { state: 'review', approved: false } } }, o.job || {});
  const cloud = { ok: o.cloudOk !== false };
  const ctx = vm.createContext({ console, Promise, setTimeout, clearTimeout, Date, Blob, Uint8Array, Map, Set, JSON, Object, Array, String, Number, Math, Error,
    log, said, added, job, sheet, uploads,
    window: { CharmNestOperations: null, LaserReview: { saved() {} } }, B: { pool: { rows: new Map() } }, S: { cloud }, PT: 1,
    faceOf: x => (x && x.face === 'front' ? 'front' : 'back'), isFront: j => j && j.face === 'front', faceWord: () => 'back',
    sheetFor: () => sheet, allSheets: () => [sheet], jobsOf: () => [job], hasPlacement: () => true, loadFonts: async () => {}, restoreWritten() {},
    charmFor: () => ({ sourceId: 's', mirrored: false }), sourceOf: () => ({ parsed: {} }), fitOpts: () => ({ lineGap: 1 }), fileViewOf: () => ({}), fontIdOf: () => 'f', pieceFields: () => ({}),
    renderBack: () => ({ toBlob: cb => cb(new Blob(['x'])), _sizePt: { w: 1, h: 1 } }),
    P: { buildBackFile: async () => ({ bytes: new Uint8Array(1), reference: {}, wPt: 1, hPt: 1 }) },
    verifyBackFile: async () => (o.verifyFails ? { ok: false, why: 'extent differs' } : { ok: true }),
    uploadBytes: async p => { uploads.push(p); return { path: p, url: 'https://x/' + p }; },
    api: async (name, body) => { log.push(body && body.op); if (body && body.op === 'backPut') { if (o.putFails) throw new Error('backPut refused'); } return {}; },
    CNEngravingSeals: { keep: () => [], add: () => { if (o.sealThrows) throw new Error('test: the seal record could not be read'); log.push('seal'); }, list: () => [] }, timelineApproved: () => log.push('event'), CNListActivity: { touch() {} },
    Session: { flushNow: async () => true, schedule() {} }, Pool: { update: async () => { log.push('poolUpdate'); } }, refreshBacks() {}, scheduleBackOutputs() {}, artChanged() {}, toast() {}, syncEditedBack: async () => {}, render() {},
    retryBacksLater() { log.push('retryLater'); }, agent() {},
    Review: { add: x => added.push(x), remove() {} }, notApproved: (j, t) => said.push(t), approveSay: (j, t) => said.push(t), backQueue: Promise.resolve() });
  vm.runInContext(`${section}; this.fns = { writeBacks: typeof writeBacks==='function'?writeBacks:null, finalizeApproval: typeof finalizeApproval==='function'?finalizeApproval:null };`, ctx);
  return { api: ctx.fns, log, said, added, job, sheet, uploads };
}
const idx = (log, x) => log.indexOf(x);

(async () => {
  // 1 · a save that works: the back is recorded FIRST, then the approval, its seal and its event
  { const w = world({ job: { approving: { at: AT, by: 'Ana_M', text: 'From the window\nto the wall' } } });
    await w.api.writeBacks(w.job);
    check('1a backPut is the first write; the seal and the event come after it, in that order', idx(w.log, 'backPut') >= 0 && idx(w.log, 'seal') > idx(w.log, 'backPut') && idx(w.log, 'event') > idx(w.log, 'seal'), JSON.stringify(w.log) + JSON.stringify(w.said) + (w.job.backPending || ''));
    check('1b the approval is made: state written, approvedAt/By set, the line record says approved, job.approving gone', w.job.state === 'written' && w.job.approvedAt === AT && w.job.approvedBy === 'Ana_M' && w.job.row.engrave.approved === true && !w.job.approving, JSON.stringify(w.job.row.engrave));
    check('1c the record that was saved is THIS approval\'s (new words, approvedAt) and replaced the old one in the sheet\'s list', w.sheet.backPool.length === 1 && w.sheet.backPool[0].approvedAt === AT && w.job.backs.length === 1, JSON.stringify(w.sheet.backPool.map(x => x.approvedAt))); }
  // 2 · a back that cannot be saved (its re-check fails): no seal, no event, the reason, the old back kept
  { const w = world({ verifyFails: true, job: { approving: { at: AT, by: 'Ana_M' }, approvalIntent: { phase: 'verified', at: AT, by: 'Ana_M' } } });
    const r = await w.api.writeBacks(w.job);
    check('2a no seal, no event, no backPut, no Pool write', r === false && !w.log.includes('seal') && !w.log.includes('event') && !w.log.includes('backPut') && !w.log.includes('poolUpdate'), JSON.stringify(w.log));
    check('2b the job was never approved: state review, no approvedAt, no approving, no intent, the line record says not approved with the reason', w.job.state === 'review' && !w.job.approvedAt && !w.job.approving && !w.job.approvalIntent && w.job.row.engrave.approved === false && /Not saved/.test(w.job.row.engrave.reason || ''), JSON.stringify(w.job.row.engrave));
    check('2c the person is told plainly, and that the back approved before is kept (its words named)', w.said.some(t => /Not saved/.test(t) && /approved before \(From the window \/ to the wall\) is kept/.test(t)), JSON.stringify(w.said));
    check('2d the old record is intact in the sheet and a Review card asks for the placement', w.sheet.backPool.length === 1 && w.sheet.backPool[0].approvedAt === OLD_AT && w.added.length === 1); }
  // 3 · the cloud is away: the approval waits, nothing is stamped, and it says it will be saved by itself
  { const w = world({ cloudOk: false, job: { approving: { at: AT, by: 'Ana_M' } } });
    const r = await w.api.writeBacks(w.job);
    check('3a no seal, no event, still waiting in job.approving, retry scheduled', r === false && !w.log.includes('seal') && !w.log.includes('event') && !!w.job.approving && w.job.state === 'review' && w.log.includes('retryLater'), JSON.stringify(w.log));
    check('3b the line record is untouched (not approved) and the person is told it is saved when the cloud is back', w.job.row.engrave.approved === false && w.said.some(t => /Not approved yet: saved when the cloud is back/.test(t) && /tried again by itself/.test(t)), JSON.stringify(w.said)); }
  // 4 · the save itself is refused by the cloud (backPut throws): same as 3 (nothing stamped)
  { const w = world({ putFails: true, job: { approving: { at: AT, by: 'Ana_M' } } });
    await w.api.writeBacks(w.job);
    check('4 backPut refused: no seal, no event, job still waiting', !w.log.includes('seal') && !w.log.includes('event') && !!w.job.approving && w.job.state === 'review', JSON.stringify(w.log)); }
  // 5 · an approval whose back is ALREADY saved just stamps: no file built, no second backPut
  { const w = world({ backPool: [rec(AT, 'From the window\nto the wall')], job: { approving: { at: AT, by: 'Ana_M' } } });
    await w.api.writeBacks(w.job);
    check('5 the saved back is adopted (no backPut), then the seal and the event', !w.log.includes('backPut') && w.log.includes('seal') && w.log.includes('event') && w.job.state === 'written', JSON.stringify(w.log) + JSON.stringify(w.said)); }
  // 6 · an approval that is already made (old path: a piece approved before it was on a sheet, an edit of an approved back) is written as before: no second seal, no second event
  { const w = world({ job: { state: 'approved', approvedAt: AT, approvedBy: 'Ana_M' } });
    await w.api.writeBacks(w.job);
    check('6 a made approval: its back is written, finalize is not run again (no seal, no event from the writer)', w.log.includes('backPut') && !w.log.includes('seal') && !w.log.includes('event') && w.job.state === 'written', JSON.stringify(w.log)); }
  // 6b · the seal itself cannot be made after the save: said in words like any approval that threw, nothing half made (no state without its seal), the next press approves again
  { const w = world({ sealThrows: true, job: { approving: { at: AT, by: 'Ana_M' }, approvalIntent: { phase: 'verified', at: AT, by: 'Ana_M' } } });
    const r = await w.api.writeBacks(w.job);
    check('6b the seal fails after the save: no event, state review, no approvedAt, nothing waiting, the card says what went wrong', r === false && !w.log.includes('event') && w.job.state === 'review' && !w.job.approvedAt && !w.job.approving && !w.job.approvalIntent && w.said.some(t => /^Not approved: something went wrong while approving \(test: the seal record could not be read\)/.test(t)), JSON.stringify({ log: w.log, said: w.said, state: w.job.state, at: w.job.approvedAt })); }
  // 8 · BACKFOLDER (Paul, 18 Nov 2026: Ana's and Michelle's approvals all ended "Not approved yet ... Reconnect to save the approved back files"): a sheet this computer only FOLLOWS has no folderPath of its own; its back files go in
  //     the folder its record's files are in. With no saved files at all (a sheet never saved) it still waits, as before.
  { const w = world({ noFolder: 'record', job: { approving: { at: AT, by: 'Ana_M', text: 'From the window\nto the wall' } } });
    await w.api.writeBacks(w.job);
    check('8a a followed sheet (no folderPath, its record names the folder): the back is uploaded into that folder, saved, then the seal and the event', w.uploads.length === 2 && w.uploads.every(u => u.startsWith('charmnest/sets/x/back/')) && idx(w.log, 'backPut') >= 0 && idx(w.log, 'seal') > idx(w.log, 'backPut') && w.job.state === 'written', JSON.stringify([w.uploads, w.log, w.said])); }
  { const w = world({ noFolder: 'none', job: { approving: { at: AT, by: 'Ana_M' } } });
    await w.api.writeBacks(w.job);
    check('8b a sheet with no saved files and no folder still waits: nothing uploaded, nothing stamped, retry scheduled', w.uploads.length === 0 && !w.log.includes('seal') && !!w.job.approving && w.log.includes('retryLater'), JSON.stringify([w.uploads, w.log])); }
  // 7 · the press itself (source): a placed piece is held until saved; the old stamping stays only for the other paths
  { const once = source.slice(source.indexOf('  async function approveOnce('), source.indexOf('  async function approveOnce(') + 14000), h = once.indexOf('if(hold){'), seal = once.indexOf('CNEngravingSeals.add(job,"engraveApproved",by,approvedAt)'), settle = once.indexOf('settleApproval(job)');
    check('7a approveOnce waits for saveBacks inside the hold branch BEFORE any seal is made, and the old seal code sits in the !hold branch', h > 0 && seal > h && once.indexOf('await saveBacks(job)', h) > h && once.indexOf('await saveBacks(job)', h) < seal && once.indexOf('if(!hold){') > h && once.indexOf('if(!hold){') < seal, `hold ${h} save ${once.indexOf('await saveBacks(job)', h)} seal ${seal}`);
    check('7b the old settleApproval stays for pieces not on a sheet and for edits of an approved back only', settle > seal && /hold=!job\.editingBack && !unplacedPre\(job\)/.test(once));
    const rec2 = source.slice(source.indexOf('  function recoverApprovals()'), source.indexOf('  function recoverApprovals()') + 2200);
    check('7c a recovered approval of a placed piece saves its back first (approving, then saveBacks, no settleApproval before it)', rec2.indexOf('job.approving={') > 0 && rec2.indexOf('saveBacks(job)') > rec2.indexOf('job.approving={') && rec2.indexOf('saveBacks(job)') < rec2.indexOf('if(settleApproval(job))'));
    const wb = source.slice(source.indexOf('  async function writeBacksOnce'), source.indexOf('  async function verifyBackFile')), put = wb.indexOf('op: "backPut"'), fin = wb.indexOf('finalizeApproval(job)');
    check('7d finalizeApproval runs in writeBacksOnce after the backPut call and after the record is recorded', put > 0 && fin > put); }
  console.log(fails ? `FAIL engrave-seal-after-save: ${fails}` : 'PASS engrave-seal-after-save'); process.exit(fails ? 1 : 0);
})().catch(e => { console.error('ERR', e && e.stack || e); process.exit(1); });
