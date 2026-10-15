// The Charm Repo's password door (netlify/functions/_charmNestRepoAuth.js; plans/repo-protect-1011): offline, on the in-memory
// Firestore / Storage of _sandboxFakes.cjs, with throwaway credentials made here (nothing real, nothing printed).
//   node tests/charm-nest/repo-auth.cjs
'use strict';
const assert = require('assert'), path = require('path'), crypto = require('crypto'), fs = require('fs');
const F = require('./_sandboxFakes.cjs'); F.install();
const fnDir = path.join(__dirname, '../../netlify/functions');
const Lib = require(path.join(fnDir, 'charmNestLibrary.js')), Out = require(path.join(fnDir, 'charmNestOutput.js'));
const Check = require(path.join(fnDir, 'charmNestCheck.js'));
const RA = require(path.join(fnDir, '_charmNestRepoAuth.js')), Admins = require(path.join(fnDir, '_stationAdmins.js'));
const { store, blobs, writes } = F;
/* Storage with the download token kept on the object (the shared fake keeps no metadata) */
F.bucket.file = p => ({ name: p,
  async save(buf, o) { blobs.set(p, { buf: Buffer.from(buf), contentType: o && o.contentType, meta: Object.assign({}, o && o.metadata && o.metadata.metadata) }); },
  async exists() { return [blobs.has(p)]; },
  async download() { return [blobs.get(p).buf]; },
  async delete() { blobs.delete(p); },
  async getMetadata() { const b = blobs.get(p); if (!b) throw Object.assign(new Error('no blob'), { code: 404 }); return [{ contentType: b.contentType, size: b.buf.length, metadata: b.meta || {} }]; },
  async setMetadata(m) { const b = blobs.get(p); if (!b) throw Object.assign(new Error('no blob'), { code: 404 }); b.meta = Object.assign(b.meta || {}, m.metadata || {}); if (m.contentType) b.contentType = m.contentType; },
  async getSignedUrl() { return ['https://storage.example/' + p + '?sig=write']; } });

/* ── throwaway credentials, made in the fake Firestore only ── */
const rnd = n => crypto.randomBytes(n).toString('hex');
const PAUL_PW = 'test-' + rnd(6), SHELL_PW = 'test-' + rnd(6), WRONG = 'wrong-' + rnd(4);
function account(pw, role, extra) { const salt = crypto.randomBytes(32), hash = crypto.pbkdf2Sync(pw, salt, 200000, 64, 'sha512'); return Object.assign({ role, passwordHash: hash.toString('hex'), salt: salt.toString('hex'), iterations: 200000, digest: 'sha512' }, extra || {}); }
const paulDoc = account(PAUL_PW, 'owner', { username: 'paul', displayName: 'Paul K' });
store.set('EtsyMail_Operators/paul', paulDoc);
store.set('EtsyMail_Operators/shell', account(SHELL_PW, 'owner', { username: 'shell', displayName: 'Shell' }));
store.set('EtsyMail_Operators/ana', account('ana-' + rnd(4), 'operator', { username: 'ana', displayName: 'Ana' }));
store.set('config/stationAdmins', { names: ['Paul K'] });

/* ── a clock the test moves ── */
let T = 1800000000000; RA.deps.now = () => T; Admins.deps.now = () => T;
const adv = ms => { T += ms; };

const call = (h, body, ip, method) => h.handler({ httpMethod: method || 'POST', headers: Object.assign({ 'user-agent': 'repo-auth-test/1.0 ' + 'x'.repeat(200) }, ip ? { 'x-nf-client-connection-ip': ip } : {}), body: method === 'GET' ? undefined : JSON.stringify(body), queryStringParameters: method === 'GET' ? body : undefined }).then(r => ({ status: r.statusCode, body: JSON.parse(r.body || '{}') }));
const lib = (body, ip) => call(Lib, body, ip), out = (body, ip) => call(Out, body, ip);
const unlock = (action, o, ip) => lib(Object.assign({ op: 'repoUnlock', password: PAUL_PW, action, by: 'Ana', target: 'T-1', tab: 'tab1' }, o || {}), ip);
const docs = coll => [...store].filter(([k]) => k.startsWith(coll + '/')).map(([k, v]) => Object.assign({ _id: k.slice(coll.length + 1) }, v));
const entry = { sku: 'RA-TEST-01', charmHash: 'abcdef01', widthPt: 40, heightPt: 50, areaPt2: 800, aiPath: 'charmnest/master/ra-test-01.ai', thumbPath: 'charmnest/master/ra-test-01.png' };
const put = (token, o) => lib(Object.assign({ op: 'masterPutIndex', masterHash: 'aaaa1111', masterName: 'A.ai', entries: [entry] }, token ? { repoToken: token } : {}, o || {}));

(async () => {
  let r, n = 0; const ok = (c, m) => { assert(c, m); n++; };

  /* 1 · a wrong password: refused, recorded, alerted; nothing secret anywhere */
  r = await unlock('upload', { password: WRONG });
  ok(r.status === 200 && r.body.ok === false && r.body.error === 'wrong' && !r.body.token, 'wrong password: ' + JSON.stringify(r.body));
  let att = docs('CharmRepo_AuthAttempts'), alr = docs('CharmRepo_Alerts');
  ok(att.length === 1 && alr.length === 1 && att[0]._id === alr[0]._id, 'one attempt and one alert, same id');
  ok(att[0].ok === false && att[0].reason === 'wrong' && att[0].action === 'upload' && att[0].by === 'Ana' && att[0].target === 'T-1' && att[0].tab === 'tab1' && att[0].at === T, 'the attempt says who, what, when: ' + JSON.stringify(att[0]));
  ok(att[0].ua.length === 80, 'the user agent is cut at 80');
  ok(alr[0].closed === false && alr[0].closedAt === null && alr[0].closedBy === '', 'the alert starts open');
  ok(Object.values(att[0]).every(v => typeof v !== 'object' || v === null), 'flat fields only');

  /* 2 · the right password: a token; only its sha256 is kept; it is recorded and alerted too */
  r = await unlock('modify');
  ok(r.status === 200 && r.body.ok === true && typeof r.body.token === 'string' && r.body.once === false && r.body.action === 'modify', 'right password: ' + JSON.stringify(Object.keys(r.body)));
  ok(r.body.expiresAt === T + 5 * 60000, 'a modify token lives 5 minutes');
  const modTok = r.body.token, tid = crypto.createHash('sha256').update(modTok).digest('hex');
  ok(store.has('CharmRepo_Tokens/' + tid) && docs('CharmRepo_Tokens').length === 1, 'only sha256(token) is the document id');
  ok(!JSON.stringify([...store]).includes(modTok), 'the token itself is stored nowhere');
  att = docs('CharmRepo_AuthAttempts'); ok(att.length === 2 && att.some(a => a.ok === true && a.reason === 'ok') && docs('CharmRepo_Alerts').length === 2, 'the right attempt is recorded and alerted as well');
  r = await unlock('upload'); const upTok = r.body.token; ok(r.body.expiresAt === T + 15 * 60000 && r.body.once === false, 'an upload token lives 15 minutes');
  r = await unlock('download'); const dlTok = r.body.token;
  r = await unlock('delete'); ok(r.body.once === true && r.body.expiresAt === T + 2 * 60000, 'a delete token is single use'); const delTok = r.body.token;
  r = await unlock('history'); ok(r.body.once === true, 'a history token is single use'); const histTok = r.body.token;
  r = await unlock('nonsense'); ok(r.body.ok === true && r.body.action === 'other', 'an unknown action is read as other'); const otherTok = r.body.token;

  /* 3 · Shell's password and an operator's are no key; nothing leaks */
  r = await unlock('upload', { password: SHELL_PW }); ok(r.body.ok === false && r.body.error === 'wrong', "Shell's Master Admin password does not unlock");
  r = await unlock('upload', { password: '' }); ok(r.body.error === 'wrong', 'an empty password is wrong');
  r = await unlock('upload', { password: 'x'.repeat(5000) }); ok(r.body.error === 'wrong', 'an over-long password is wrong (and not hashed)');
  const everything = JSON.stringify([...store].filter(([k]) => /^CharmRepo_/.test(k)));
  for (const secret of [PAUL_PW, SHELL_PW, WRONG, paulDoc.passwordHash, paulDoc.salt]) ok(!everything.includes(secret), 'no password, hash or salt in any CharmRepo_ document');

  /* 4 · every protected op answers 401 without a token and writes nothing */
  const before = writes.length, blobsBefore = blobs.size;
  const PROTECTED = [
    ['masterPutIndex', { masterHash: 'aaaa1111', entries: [entry] }], ['masterPutFile', { file: { masterHash: 'aaaa1111', name: 'A.ai' } }], ['startMaster', { path: 'charmnest/master/files/x.pdf', name: 'x.pdf' }],
    ['masterPatch', { sku: 'RA-TEST-01', patch: { engravable: false } }], ['masterRemoveFile', { masterHash: 'aaaa1111' }], ['masterRemoveSku', { sku: 'RA-TEST-01' }], ['masterRemoveSkus', { skus: ['RA-TEST-01'] }], ['masterDownload', { sku: 'RA-TEST-01' }]];
  for (const [op, b] of PROTECTED) {
    r = await lib(Object.assign({ op }, b)); ok(r.status === 401 && r.body.error === 'repo-locked' && r.body.why === 'missing', op + ' without a token: ' + r.status + ' ' + JSON.stringify(r.body));
    r = await lib(Object.assign({ op, sandbox: true }, b)); ok(r.status === 401, op + ' in the sandbox flag is protected too');
    r = await lib(Object.assign({ op, repoToken: 'not-a-token' }, b)); ok(r.status === 401 && r.body.why === 'invalid', op + ' with junk: ' + JSON.stringify(r.body));
  }
  r = await call(Lib, { op: 'masterRemoveFile', masterHash: 'aaaa1111' }, null, 'GET'); ok(r.status === 401, 'a GET is protected the same');
  r = await call(Lib, { op: 'masterRemoveFile', masterHash: 'aaaa1111', repoToken: delTok }, null, 'GET'); ok(r.status === 401 && r.body.why === 'post-only', 'a token in a URL is refused, and not spent: ' + JSON.stringify(r.body));
  const nTries = docs('CharmRepo_AuthAttempts').length;
  r = await call(Lib, { op: 'repoUnlock', password: PAUL_PW, action: 'upload' }, null, 'GET'); ok(r.status === 405 && !r.body.token, 'a password in a URL is refused');
  ok(docs('CharmRepo_AuthAttempts').length === nTries, 'and is not an attempt');
  for (const [op, p, c] of [['sign', 'charmnest/master/files/x.pdf', 'application/pdf'], ['put', 'charmnest/master/ra.ai', 'application/illustrator'], ['finalize', 'charmnest/master/ra.png', 'image/png'], ['sign', 'charmnest/sandbox/master/ra.ai', 'application/pdf'], ['sign', 'charmnest/master/../master/ra.ai', 'application/pdf']]) {
    r = await out({ op, path: p, contentType: c, base64: Buffer.from('x').toString('base64') }); ok(r.status === 401 && r.body.error === 'repo-locked' && r.body.need === 'upload', 'charmNestOutput ' + op + ' ' + p + ' needs the upload token: ' + r.status);
    r = await out({ op, path: p, contentType: c, sandbox: true, base64: 'eA==' }); ok(r.status === 401, 'and in the sandbox flag');
  }
  r = await out({ op: 'url', path: 'charmnest/master/files/abc-master.pdf' }); ok(r.status === 401 && r.body.need === 'download', 'the whole master file link needs the download token');
  ok(writes.length === before && blobs.size === blobsBefore, 'a refused call wrote nothing');

  /* 5 · a token opens what it should: scope */
  r = await put(modTok); ok(r.status === 401 && r.body.why === 'scope' && r.body.need === 'upload', 'a modify token does not upload: ' + JSON.stringify(r.body));
  r = await put(dlTok); ok(r.status === 401 && r.body.why === 'scope', 'a download token does not upload');
  r = await put(delTok); ok(r.status === 401 && r.body.why === 'scope', 'a delete token does not upload (and is not spent by that)');
  r = await put(upTok); ok(r.status === 200 && r.body.ok === true && r.body.written === 1, 'an upload token writes the index: ' + JSON.stringify(r.body));
  r = await put(upTok, { entries: [Object.assign({}, entry, { sku: 'RA-TEST-02' })] }); ok(r.status === 200, 'and again, an upload token is reusable');
  r = await lib({ op: 'masterPutFile', repoToken: upTok, file: { masterHash: 'aaaa1111', name: 'A.ai', charms: 2, skus: ['RA-TEST-01', 'RA-TEST-02'] } }); ok(r.status === 200 && r.body.ok, 'the file record');
  r = await lib({ op: 'masterPatch', repoToken: modTok, sku: 'RA-TEST-01', patch: { engravable: false } }); ok(r.status === 200 && r.body.ok, 'a modify token patches');
  r = await lib({ op: 'masterPatch', repoToken: upTok, sku: 'RA-TEST-02', patch: { engravable: false } }); ok(r.status === 200, 'an upload token patches too (the upload flow patches)');
  r = await lib({ op: 'masterPatch', repoToken: otherTok, sku: 'RA-TEST-02', patch: { upAngle: 10 } }); ok(r.status === 200, 'an other token opens upload / modify / download ops');
  r = await lib({ op: 'masterPatch', repoToken: dlTok, sku: 'RA-TEST-02', patch: { upAngle: 10 } }); ok(r.status === 401 && r.body.why === 'scope', 'a download token does not patch');
  r = await lib({ op: 'masterRemoveSku', repoToken: otherTok, sku: 'RA-TEST-02' }); ok(r.status === 401 && r.body.why === 'scope' && r.body.once === true, 'an other token never deletes');
  r = await lib({ op: 'masterRemoveSku', repoToken: upTok, sku: 'RA-TEST-02' }); ok(r.status === 401, 'an upload token never deletes');
  r = await lib({ op: 'masterRemoveSkus', repoToken: upTok, skus: ['RA-TEST-02'] }); ok(r.status === 401 && r.body.why === 'scope', 'an upload token never deletes several SKUs either');
  r = await out({ op: 'put', repoToken: upTok, path: 'charmnest/master/ra-test-01.ai', contentType: 'application/illustrator', base64: Buffer.from('%PDF-test').toString('base64') });
  ok(r.status === 200 && r.body.ok && blobs.has('charmnest/master/ra-test-01.ai'), 'charmNestOutput put under master/ with an upload token');
  r = await out({ op: 'sign', repoToken: upTok, path: 'charmnest/master/files/ra.pdf', contentType: 'application/pdf' }); ok(r.status === 200 && r.body.uploadUrl, 'sign under master/ with an upload token');
  r = await out({ op: 'finalize', repoToken: modTok, path: 'charmnest/master/ra-test-01.ai', contentType: 'application/illustrator' }); ok(r.status === 401, 'a modify token does not finalize an upload');

  /* 6 · single use: a delete token is spent by its first use */
  r = await lib({ op: 'masterRemoveFile', repoToken: delTok, masterHash: 'aaaa1111' }); ok(r.status === 200 && r.body.ok && r.body.removed === 2, 'a delete token removes a master file: ' + JSON.stringify(r.body));
  r = await lib({ op: 'masterRemoveFile', repoToken: delTok, masterHash: 'aaaa1111' }); ok(r.status === 401 && r.body.why === 'used', 'the same token a second time: ' + JSON.stringify(r.body));
  r = await lib({ op: 'masterRemoveSku', repoToken: histTok, sku: 'RA-TEST-01' }); ok(r.status === 401 && r.body.why === 'scope', 'a history token removes a file record only, not a SKU');
  r = await lib({ op: 'masterRemoveFile', repoToken: histTok, masterHash: 'aaaa1111' }); ok(r.status === 200, 'a history token removes a file record');
  r = await lib({ op: 'masterRemoveFile', repoToken: histTok, masterHash: 'aaaa1111' }); ok(r.status === 401 && r.body.why === 'used', 'and is spent');
  // two calls racing on one single-use token: on Firestore's own transaction rules (the stand-in with st.atomic) only one is let through
  { const BS = require('./bridge-server.cjs'), st = BS.makeState({}); st.atomic = true; const adb = BS.fakeAdmin(st, 'http://x').firestore();
    const t = await RA.issue(adb, { action: 'delete', by: 'Ana', tab: 't' });
    const need = { op: 'masterRemoveSku', need: 'delete-sku' };
    const res = await Promise.all([RA.authorize(adb, need, { repoToken: t.token }), RA.authorize(adb, need, { repoToken: t.token }), RA.authorize(adb, need, { repoToken: t.token })]);
    ok(res.filter(x => x === null).length === 1 && res.filter(x => x && x.body.why === 'used').length === 2, 'three calls racing on one single-use token: exactly one passes (' + res.map(x => (x ? x.body.why : 'pass')) + ')'); }

  /* 7 · expiry */
  r = await unlock('modify'); const shortTok = r.body.token;
  r = await lib({ op: 'masterPatch', repoToken: shortTok, sku: 'RA-TEST-01', patch: { engravable: true } }); ok(r.status === 200 || r.body.error === 'not indexed: RA-TEST-01', 'valid before expiry');
  adv(5 * 60000 + 1);
  r = await lib({ op: 'masterPatch', repoToken: shortTok, sku: 'RA-TEST-01', patch: { engravable: true } }); ok(r.status === 401 && r.body.why === 'expired', 'expired after 5 minutes: ' + JSON.stringify(r.body));
  r = await lib({ op: 'masterPatch', repoToken: modTok, sku: 'RA-TEST-01', patch: { engravable: true } }); ok(r.status === 401 && r.body.why === 'expired', 'a token cached while valid is not valid after its time');
  adv(70 * 60000); r = await unlock('modify'); ok(r.body.ok, 'unlock after a long time'); ok(docs('CharmRepo_Tokens').every(t => t.exp > T - 60 * 60000), 'old tokens are cleaned up');

  /* 8 · a forged token document is no token (the HMAC) */
  const fake = rnd(32) + '.' + rnd(16); store.set('CharmRepo_Tokens/' + crypto.createHash('sha256').update(fake).digest('hex'), { exp: T + 600000, action: 'upload', once: false, used: false });
  r = await put(fake); ok(r.status === 401 && r.body.why === 'invalid', 'a token written straight into Firestore does not pass');

  /* 9 · lockout: 5 wrong in 10 minutes lock a source for 15 minutes; others are not locked; every try is recorded */
  adv(60 * 60000);
  const A = '10.0.0.1', B = '10.0.0.2'; const base = docs('CharmRepo_AuthAttempts').length;
  for (let i = 1; i <= 5; i++) { r = await unlock('upload', { password: WRONG + i }, A); ok(r.body.error === 'wrong', 'wrong ' + i); if (i === 5) ok(r.body.lockedNow === true && r.body.retryAfterSec === 900, 'the fifth says it locked: ' + JSON.stringify(r.body)); adv(1000); }
  r = await unlock('upload', { password: PAUL_PW }, A); ok(r.status === 200 && r.body.ok === false && r.body.error === 'locked' && r.body.retryAfterSec > 0 && r.body.retryAfterSec <= 900 && !r.body.token, 'locked: even the right password is refused: ' + JSON.stringify(r.body));
  r = await unlock('upload', { password: WRONG }, A); ok(r.body.error === 'locked', 'a wrong one while locked is locked, and recorded');
  r = await unlock('upload', { password: PAUL_PW }, B); ok(r.body.ok === true, 'another source is not locked by this one');
  const recent = docs('CharmRepo_AuthAttempts').length - base; ok(recent === 8, 'all eight tries are recorded (' + recent + ')');
  ok(docs('CharmRepo_AuthAttempts').filter(a => a.reason === 'locked').length === 2 && docs('CharmRepo_Alerts').length === docs('CharmRepo_AuthAttempts').length, 'locked tries are recorded and alerted');
  adv(14 * 60000); r = await unlock('upload', { password: PAUL_PW }, A); ok(r.body.error === 'locked', 'still locked inside 15 minutes');
  adv(2 * 60000); r = await unlock('upload', { password: PAUL_PW }, A); ok(r.body.ok === true, 'unlocked after 15 minutes');
  // five wrong spread over more than ten minutes do not lock
  for (let i = 0; i < 5; i++) { r = await unlock('upload', { password: WRONG }, '10.0.0.3'); ok(r.body.error === 'wrong' && !r.body.lockedNow, 'slow wrong ' + i); adv(3 * 60000); }
  r = await unlock('upload', { password: PAUL_PW }, '10.0.0.3'); ok(r.body.ok === true, 'five wrong over fifteen minutes never locked');
  // a right password in between clears the count
  for (let i = 0; i < 4; i++) await unlock('upload', { password: WRONG }, '10.0.0.4'); await unlock('upload', { password: PAUL_PW }, '10.0.0.4');
  for (let i = 0; i < 4; i++) { r = await unlock('upload', { password: WRONG }, '10.0.0.4'); ok(!r.body.lockedNow, 'count cleared by a right password'); }
  // many sources together lock everyone
  adv(30 * 60000);
  for (let i = 0; i < 20; i++) { r = await unlock('upload', { password: WRONG }, '10.9.0.' + i); }
  ok(r.body.lockedNow === true, 'twenty wrong from twenty sources lock everyone: ' + JSON.stringify(r.body));
  r = await unlock('upload', { password: PAUL_PW }, '10.8.8.8'); ok(r.body.error === 'locked', 'a new source is locked by the global lock');
  adv(16 * 60000); r = await unlock('upload', { password: PAUL_PW }, '10.8.8.8'); ok(r.body.ok === true, 'and free again after 15 minutes');

  /* 10 · free ops are unchanged (no token needed), and the Charm Repo reads work */
  for (const [op, b] of [['masterList', {}], ['masterGet', { sku: 'RA-TEST-01' }], ['masterGetMany', { skus: ['RA-TEST-01'] }], ['masterListFiles', {}], ['listCharms', {}], ['lookupCharms', { hashes: ['abcdef01'] }], ['aliasGet', {}], ['optionMapGet', {}], ['noDesignGet', {}], ['ping', { calibration: false }], ['getJob', { id: 'job-none' }], ['poolList', {}], ['listSheets', {}], ['renameCharm', { hash: 'abcdef0123456789', name: 'A Name' }], ['putCharms', { charms: [{ hash: 'abcdef0123456789', name: 'x', namedBy: 'fallback' }] }]]) {
    r = await lib(Object.assign({ op }, b)); ok(r.status !== 401 && r.body.error !== 'repo-locked', op + ' stays free: ' + r.status + ' ' + JSON.stringify(r.body).slice(0, 80));
  }
  r = await lib({ op: 'masterList' }); ok(r.status === 200 && Array.isArray(r.body.entries), 'masterList answers');
  for (const p of ['charmnest/sheets/2026-10-11/gold-1/gold-1.ai', 'charmnest/charms/abcdef01.ai', 'charmnest/sources/2026-10-11/x.pdf', 'charmnest/custom/123/x.pdf', 'charmnest/diag/probe.txt', 'charmnest/sandbox/orders-pull/x.json']) {
    r = await out({ op: 'put', path: p, contentType: 'application/json', base64: Buffer.from('{}').toString('base64') }); ok(r.status === 200, 'charmNestOutput put on ' + p + ' stays free');
    r = await out({ op: 'sign', path: p, contentType: 'application/pdf' }); ok(r.status === 200, 'sign on ' + p + ' stays free');
    r = await out({ op: 'url', path: p }); ok(r.status === 200, 'url on ' + p + ' stays free (the nester mints these itself)');
  }
  r = await out({ op: 'url', path: 'charmnest/master/ra-test-01.ai' }); ok(r.status === 200, 'url on a per-SKU master .ai stays free: the nester falls back on it');
  blobs.set('charmnest/master/ra-test-01.png', { buf: Buffer.from('png'), contentType: 'image/png' });
  r = await out({ op: 'url', path: 'charmnest/master/ra-test-01.png' }); ok(r.status === 200, 'and a thumbnail');
  r = await call(Lib, { op: 'ping', calibration: false }, null, 'OPTIONS'); ok(r.status === 204, 'OPTIONS is untouched');

  /* 11 · internal calls are not asked: the ops in-process, the check, the background indexer */
  const o = Lib.ops;
  r = await o.masterPutIndex({ masterHash: 'bbbb2222', masterName: 'B.ai', entries: [Object.assign({}, entry, { sku: 'RA-INT-01' })] }); ok(r.ok && r.written === 1, 'in-process masterPutIndex needs no token');
  r = await o.masterPatch({ sku: 'RA-INT-01', patch: { engravable: false } }); ok(r.ok, 'in-process masterPatch');
  r = await o.masterRemoveSku({ sku: 'RA-INT-01' }); ok(r.ok, 'in-process masterRemoveSku');
  ok(!('repoUnlock' in o) && !('masterDownload' in o), "the door's own ops are not in OPS (the sandbox guard and the check list stay as they were)");
  ok(typeof Check.handler === 'function', 'charmNestCheck loads (it runs OPS in-process)');
  const bg = require(path.join(fnDir, 'charmMaster-background.js'));
  r = await bg.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify({ id: 'job-missing' }) }); ok(r.statusCode === 404, 'the background indexer is not behind the token (it only runs a job startMaster made)');

  /* 12 · the download link */
  store.set('Charm_Master_Index/RA-TEST-01', Object.assign({ sku: 'RA-TEST-01' }, entry));
  store.set('Charm_Master_Index/RA-SIZED-01', { sku: 'RA-SIZED-01', sizes: { S: { aiPath: 'charmnest/master/ra-sized-s.ai' }, M: { aiPath: 'charmnest/master/ra-sized-m.ai' } } });
  store.set('Charm_Master_Files/cccc3333', { masterHash: 'cccc3333', path: 'charmnest/master/files/cccc-m.pdf', name: 'M.pdf' });
  blobs.set('charmnest/master/files/cccc-m.pdf', { buf: Buffer.from('%PDF'), contentType: 'application/pdf' });
  blobs.set('charmnest/master/ra-sized-s.ai', { buf: Buffer.from('%PDF'), contentType: 'application/illustrator' });
  r = await unlock('download'); const dl = r.body.token;
  r = await lib({ op: 'masterDownload', repoToken: dl, sku: 'RA-TEST-01' }); ok(r.status === 200 && /^https:\/\/firebasestorage\.googleapis\.com\/v0\/b\/test-bucket\/o\/charmnest%2Fmaster%2Fra-test-01\.ai\?alt=media&token=/.test(r.body.url) && r.body.name === 'RA-TEST-01.ai', 'a download link for a SKU: ' + JSON.stringify(r.body).slice(0, 120));
  r = await lib({ op: 'masterDownload', repoToken: dl, sku: 'RA-SIZED-01' }); ok(r.status === 400 && r.body.sizes.length === 2, 'a sized SKU asks which size');
  r = await lib({ op: 'masterDownload', repoToken: dl, sku: 'RA-SIZED-01', size: 's' }); ok(r.status === 200 && r.body.path === 'charmnest/master/ra-sized-s.ai', 'a sized SKU with its size');
  r = await lib({ op: 'masterDownload', repoToken: dl, masterHash: 'cccc3333' }); ok(r.status === 200 && r.body.path === 'charmnest/master/files/cccc-m.pdf', 'the whole master file');
  r = await lib({ op: 'masterDownload', repoToken: dl, path: 'charmnest/sheets/2026-10-11/x.ai' }); ok(r.status === 400, 'only charm files: a sheet path is refused');
  r = await lib({ op: 'masterDownload', repoToken: dl, sku: 'NOPE-9' }); ok(r.status === 404, 'unknown SKU');
  r = await out({ op: 'url', repoToken: dl, path: 'charmnest/master/files/cccc-m.pdf' }); ok(r.status === 200 && r.body.url.includes('token='), 'charmNestOutput url on the master file with a download token');
  r = await unlock('upload'); r = await out({ op: 'url', repoToken: r.body.token, path: 'charmnest/master/files/cccc-m.pdf' }); ok(r.status === 401 && r.body.why === 'scope', 'an upload token does not download');

  /* 13 · the Alert: the master admin only */
  r = await lib({ op: 'repoAlerts' }); ok(r.status === 403 && r.body.error === 'admin-only', 'no name: 403');
  r = await lib({ op: 'repoAlerts', by: 'Ana' }); ok(r.status === 403, 'another login: 403');
  r = await lib({ op: 'repoAlerts', by: 'Paul' }); ok(r.status === 403, 'a bare "Paul" is not "Paul K" (the list folds, it does not guess)');
  r = await lib({ op: 'repoAlerts', by: 'paul k' }); ok(r.status === 200 && r.body.ok === true, 'the master admin by the stationAdmins list (folded)');
  const list = r.body.alerts;
  ok(list.length > 10 && list.length === r.body.total && r.body.more === false, 'open alerts listed: ' + list.length);
  ok(list.every((a, i) => i === 0 || list[i - 1].at >= a.at), 'newest first');
  ok(Object.keys(list[0]).sort().join() === 'action,at,by,id,ok,reason,target', 'the shape: ' + Object.keys(list[0]));
  ok(list.some(a => a.ok === false && a.reason === 'locked') && list.some(a => a.ok === true && a.reason === 'ok') && list.some(a => a.reason === 'wrong'), 'right, wrong and locked attempts are all there');
  const keep = docs('CharmRepo_AuthAttempts').length;
  r = await lib({ op: 'repoAlertClose', by: 'Ana', ids: [list[0].id] }); ok(r.status === 403, 'another login cannot close');
  r = await lib({ op: 'repoAlertClose', by: 'Paul K', ids: [list[0].id, list[1].id, 'does-not-exist', '../x'] }); ok(r.status === 200 && r.body.closed === 2, 'close two: ' + JSON.stringify(r.body));
  ok(!store.has('CharmRepo_Alerts/does-not-exist'), 'closing an unknown id makes no stray document');
  const closed = store.get('CharmRepo_Alerts/' + list[0].id); ok(closed.closed === true && closed.closedAt === T && closed.closedBy === 'Paul K', 'closed with who and when');
  ok(docs('CharmRepo_AuthAttempts').length === keep && store.has('CharmRepo_AuthAttempts/' + list[0].id) && store.get('CharmRepo_AuthAttempts/' + list[0].id).closed === undefined, 'closing never touches the attempt record');
  r = await lib({ op: 'repoAlerts', by: 'Paul K' }); ok(r.body.total === list.length - 2 && !r.body.alerts.some(a => a.id === list[0].id), 'a closed alert is gone from the list');
  r = await lib({ op: 'repoAlertClose', by: 'Paul K', ids: [list[0].id] }); ok(r.body.closed === 0, 'closing twice is harmless');
  r = await lib({ op: 'repoAlertClose', by: 'Paul K', all: true }); ok(r.body.closed === list.length - 2, 'close all');
  r = await lib({ op: 'repoAlerts', by: 'Paul K' }); ok(r.body.alerts.length === 0 && r.body.total === 0, 'nothing open');
  r = await unlock('upload', { password: WRONG }); r = await lib({ op: 'repoAlerts', by: 'Paul K' }); ok(r.body.alerts.length === 1 && r.body.alerts[0].reason === 'wrong', 'a new attempt raises a new alert after the old ones were closed');
  store.delete('config/stationAdmins'); Admins.forget(F.db);
  r = await lib({ op: 'repoAlerts', by: 'Paul K' }); ok(r.status === 200, 'with the list missing the built-in fallback ("Paul K", "Paul") still names the master admin');
  r = await lib({ op: 'repoAlerts', by: 'Ana' }); ok(r.status === 403, 'and not anybody else');
  store.set('config/stationAdmins', { names: ['Paul K'] }); Admins.forget(F.db);

  /* 14 · which account unlocks */
  store.set('config/repoProtect', { operators: ['shell'] }); RA.forget(); adv(60000);
  r = await unlock('upload', { password: SHELL_PW }, '10.7.7.7'); ok(r.body.ok === true, 'config/repoProtect.operators names the account');
  r = await unlock('upload', { password: PAUL_PW }, '10.7.7.7'); ok(r.body.error === 'wrong', 'and then only that one');
  store.delete('config/repoProtect'); adv(60000);
  const paulKeep = store.get('EtsyMail_Operators/paul'); store.delete('EtsyMail_Operators/paul'); adv(60000);
  r = await unlock('upload', { password: SHELL_PW }, '10.7.7.8'); ok(r.status === 503 && r.body.error === 'unavailable' && !r.body.token, "with no account called paul, Shell's account is still not accepted as Paul's");
  store.set('EtsyMail_Operators/pk', Object.assign({}, paulKeep, { username: 'pk', displayName: 'Paul K' })); adv(60000);
  r = await unlock('upload', { password: PAUL_PW }, '10.7.7.9'); ok(r.body.ok === true, 'an owner account whose display name is Paul K is found when none is called paul');
  store.delete('EtsyMail_Operators/pk'); adv(60000);
  r = await unlock('upload', { password: PAUL_PW }, '10.7.7.10'); ok(r.status === 503 && r.body.error === 'unavailable' && !r.body.token, 'no Paul account at all: unavailable, no token');
  ok(docs('CharmRepo_AuthAttempts').some(a => a.reason === 'unavailable' && a.ok === false), 'and the try is recorded');
  store.set('EtsyMail_Operators/paul', Object.assign({}, paulKeep, { revokedAt: 1 })); adv(60000);
  r = await unlock('upload', { password: PAUL_PW }, '10.7.7.11'); ok(r.status === 503, 'a revoked account does not unlock');
  store.set('EtsyMail_Operators/paul', paulKeep); adv(60000);
  r = await unlock('upload', { password: PAUL_PW }, '10.7.7.12'); ok(r.body.ok === true, 'restored');

  /* 15 · a failure to record is never a pass */
  const realBatch = F.db.batch; F.db.batch = () => ({ set() {}, update() {}, delete() {}, async commit() { throw new Error('write failed'); } });
  r = await unlock('upload', {}, '10.6.6.6'); ok(r.status === 503 && r.body.error === 'unavailable' && !r.body.token, 'if the attempt cannot be recorded, no token is given');
  F.db.batch = realBatch;
  r = await unlock('delete', {}, '10.6.6.7'); const t15 = r.body.token;
  const realTx = F.db.runTransaction; F.db.runTransaction = async () => { throw new Error('tx failed'); };
  r = await lib({ op: 'masterRemoveSku', repoToken: t15, sku: 'X-1' });
  ok(r.status === 503 && r.body.error === 'repo-unavailable', 'if a single-use token cannot be spent, the op is refused (503), not passed');
  F.db.runTransaction = realTx;

  /* 15b · CN.api tags every call sandbox:true in the sandbox workspace: the door's own ops ignore it (real collections, never Sandbox_ copies) */
  adv(40 * 60000);
  const beforeAtt = docs('CharmRepo_AuthAttempts').length, beforeAlr = docs('CharmRepo_Alerts').length;
  r = await unlock('upload', { password: WRONG, sandbox: true }, '10.7.7.1');
  ok(r.status === 200 && r.body.error === 'wrong', 'repoUnlock with sandbox:true: a wrong password is still wrong');
  r = await unlock('upload', { sandbox: true }, '10.7.7.2'); ok(r.body.ok === true && typeof r.body.token === 'string', 'repoUnlock with sandbox:true: the right password gives a token');
  const sbTok = r.body.token;
  ok(docs('CharmRepo_AuthAttempts').length === beforeAtt + 2 && docs('CharmRepo_Alerts').length === beforeAlr + 2, 'both sandbox-flagged tries are in the real CharmRepo_ collections');
  ok(![...store.keys()].some(k => /^Sandbox_.*Repo/i.test(k)), 'no Sandbox_ copy of any CharmRepo_ collection is ever made');
  r = await put(sbTok, { sandbox: true }); ok(r.status === 200 && r.body.ok, 'and that token opens a protected op sent with sandbox:true (the token lives in the real CharmRepo_Tokens)');
  r = await lib({ op: 'repoAlerts', by: 'Paul K', sandbox: true });
  ok(r.status === 200 && r.body.ok && r.body.alerts.some(a => a.reason === 'wrong') && r.body.alerts.some(a => a.reason === 'ok'), 'repoAlerts with sandbox:true lists the real alerts');
  r = await lib({ op: 'repoAlerts', by: 'Ana', sandbox: true }); ok(r.status === 403, 'repoAlerts with sandbox:true is still master-admin only');
  const sbIds = (await lib({ op: 'repoAlerts', by: 'Paul K', sandbox: true })).body.alerts.map(a => a.id);
  r = await lib({ op: 'repoAlertClose', by: 'Paul K', ids: sbIds, sandbox: true }); ok(r.status === 200 && r.body.closed === sbIds.length, 'repoAlertClose with sandbox:true closes the real alerts');
  ok(docs('CharmRepo_Alerts').every(a => !sbIds.includes(a._id) || a.closed === true) && docs('CharmRepo_AuthAttempts').length === beforeAtt + 2, 'closed for real; the attempt records stay');
  r = await lib({ op: 'repoAlerts', by: 'Paul K' }); ok(!r.body.alerts.some(a => sbIds.includes(a.id)), 'and a plain (production) read no longer shows them');

  /* 16 · the sorter's api() lets a repo-locked 401 through without calling the cloud locked */
  const html = fs.readFileSync(path.join(__dirname, '../../charm-nest-1.html'), 'utf8');
  ok(/if \(res\.status === 401 && !\(data && data\.error === "repo-locked"\)\) \{ S\.cloud\.locked = true;/.test(html), "api() does not treat a repo-locked 401 as the cloud's passcode");

  /* 17 · nothing secret anywhere, at the end of it all */
  const all = JSON.stringify([...store].filter(([k]) => /^CharmRepo_/.test(k)));
  for (const secret of [PAUL_PW, SHELL_PW, WRONG, paulDoc.passwordHash, paulDoc.salt, modTok, upTok, delTok, dlTok]) ok(!all.includes(secret), 'no secret, hash or token in any CharmRepo_ document');
  ok(docs('CharmRepo_Tokens').every(t => Object.keys(t).sort().join() === '_id,action,at,by,exp,once,tab,used' || Object.keys(t).includes('usedAt')), 'token documents hold expiry, scope and use only');
  console.log(`repo-auth: ${n} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
