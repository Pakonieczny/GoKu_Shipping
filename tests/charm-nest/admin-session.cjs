// The Settings menu's lock and the standing unlock (netlify/functions/_charmNestRepoAuth.js, ADMINSESSION; plans/save-to-repo-1011/ADMINSESSION-settings.md):
// offline, on the in-memory Firestore of _sandboxFakes.cjs, with throwaway credentials made here (nothing real, nothing printed).
//   node tests/charm-nest/admin-session.cjs
// Proves, on the server: a right password buys ONE session for Settings and the repo (upload / change / download), kept as sha256 only; a wrong password or a
// typed name alone buys nothing; the Settings buttons that change shared state (purgeHistory, sandboxReset, sandboxPullOrders) refuse a call without it;
// it ends at the earliest of 12 hours / 5 pm Toronto / midnight; Lock now deletes it; a changed password or a revoked account ends it; a delete takes a
// single-use token from it with no password and raises no Alert; the old lockout, the Alert per wrong try and the short tokens are unchanged.
'use strict';
const assert = require('assert'), path = require('path'), crypto = require('crypto');
const F = require('./_sandboxFakes.cjs'); F.install();
const fnDir = path.join(__dirname, '../../netlify/functions');
const Lib = require(path.join(fnDir, 'charmNestLibrary.js')), Out = require(path.join(fnDir, 'charmNestOutput.js'));
const RA = require(path.join(fnDir, '_charmNestRepoAuth.js')), Admins = require(path.join(fnDir, '_stationAdmins.js'));
const { store, writes } = F;

/* ── throwaway credentials, made in the fake Firestore only ── */
const rnd = n => crypto.randomBytes(n).toString('hex');
const PAUL_PW = 'test-' + rnd(6), WRONG = 'wrong-' + rnd(4);
function account(pw, role, extra) { const salt = crypto.randomBytes(32), hash = crypto.pbkdf2Sync(pw, salt, 200000, 64, 'sha512'); return Object.assign({ role, passwordHash: hash.toString('hex'), salt: salt.toString('hex'), iterations: 200000, digest: 'sha512' }, extra || {}); }
const paulDoc = account(PAUL_PW, 'owner', { username: 'paul', displayName: 'Paul K' });
store.set('EtsyMail_Operators/paul', paulDoc);
store.set('config/stationAdmins', { names: ['Paul K'] });

/* ── a clock the test moves: 2026-10-12 14:00 UTC = 10:00 am in Toronto ── */
const T0 = Date.parse('2026-10-12T14:00:00Z'); let T = T0; RA.deps.now = () => T; Admins.deps.now = () => T;
const adv = ms => { T += ms; }, setT = iso => { T = Date.parse(iso); };
const HOUR = 3600000, MIN = 60000;

const call = (h, body, ip, method) => h.handler({ httpMethod: method || 'POST', headers: Object.assign({ 'user-agent': 'admin-session-test/1.0' }, ip ? { 'x-nf-client-connection-ip': ip } : {}), body: method === 'GET' ? undefined : JSON.stringify(body), queryStringParameters: method === 'GET' ? body : {} }).then(r => ({ status: r.statusCode, body: JSON.parse(r.body || '{}') }));
const lib = (body, ip) => call(Lib, body, ip), out = (body, ip) => call(Out, body, ip);
const unlock = (action, o, ip) => lib(Object.assign({ op: 'repoUnlock', password: PAUL_PW, action, by: 'Ana', target: 'settings', tab: 'tab1' }, o || {}), ip);
const docs = coll => [...store].filter(([k]) => k.startsWith(coll + '/')).map(([k, v]) => Object.assign({ _id: k.slice(coll.length + 1) }, v));
const sha = s => crypto.createHash('sha256').update(s).digest('hex');
const entry = { sku: 'AS-TEST-01', charmHash: 'abcdef01', widthPt: 40, heightPt: 50, areaPt2: 800, aiPath: 'charmnest/master/as-test-01.ai', thumbPath: 'charmnest/master/as-test-01.png' };
const put = token => lib({ op: 'masterPutIndex', masterHash: 'bbbb2222', masterName: 'B.ai', entries: [entry], ...(token ? { repoToken: token } : {}) });
const SETTINGS_OPS = [['purgeHistory', { code: 'not-the-code' }], ['sandboxReset', { sandbox: true }], ['sandboxPullOrders', {}]];

(async () => {
  let r, n = 0; const ok = (c, m) => { assert(c, m); n++; };

  /* 1 · how long one password lasts: the earliest of 12 hours, the next 5:00 pm Toronto and the coming Toronto midnight */
  const toronto = t => new Date(t).toLocaleString('en-US', { timeZone: 'America/Toronto', hour12: false });
  ok(RA.sessionEnd(Date.parse('2026-10-12T14:00:00Z')) === Date.parse('2026-10-12T21:00:00Z'), '10:00 am Toronto: until 5:00 pm (' + toronto(RA.sessionEnd(Date.parse('2026-10-12T14:00:00Z'))) + ')');
  ok(RA.sessionEnd(Date.parse('2026-10-12T05:30:00Z')) === Date.parse('2026-10-12T17:30:00Z'), '1:30 am Toronto: 12 hours (before 5 pm)');
  ok(RA.sessionEnd(Date.parse('2026-10-12T22:30:00Z')) === Date.parse('2026-10-13T04:00:00Z'), '6:30 pm Toronto: until midnight, never past it');
  ok(RA.sessionEnd(Date.parse('2026-12-15T15:00:00Z')) === Date.parse('2026-12-15T22:00:00Z'), 'in winter (EST) 5:00 pm is 22:00 UTC');

  /* 2 · Settings: a wrong password, a typed name, no password at all: nothing is given, every wrong try is recorded and alerted */
  r = await unlock('settings', { password: WRONG });
  ok(r.status === 200 && r.body.ok === false && r.body.error === 'wrong' && !r.body.token, 'a wrong password for Settings: ' + JSON.stringify(r.body));
  r = await unlock('settings', { password: undefined, by: 'Paul K' });
  ok(r.body.ok === false && r.body.error === 'wrong' && !r.body.token, 'the name Paul K alone opens nothing');
  r = await unlock('settings', { password: '', by: 'Paul K', sessionToken: 'not-a-token' });
  ok(r.body.ok === false && !r.body.token, 'a made-up session token is no password either');
  ok(docs('CharmRepo_AuthAttempts').length === 3 && docs('CharmRepo_Alerts').length === 3 && docs('CharmRepo_AuthAttempts').every(a => a.ok === false && a.action === 'settings'), 'three tries, three attempts, three alerts, all recorded as not ok');
  ok(docs('CharmRepo_Tokens').length === 0, 'and no token exists');

  /* 3 · the right password: ONE session token; only its sha256 is kept; the answer says when it ends */
  r = await unlock('settings');
  ok(r.status === 200 && r.body.ok === true && r.body.session === true && r.body.once === false && r.body.action === 'settings', 'right password: a session ' + JSON.stringify(Object.keys(r.body)));
  const SES = r.body.token, sesId = sha(SES);
  ok(r.body.expiresAt === Date.parse('2026-10-12T21:00:00Z'), 'ends at 5:00 pm Toronto');
  const tokDoc = store.get('CharmRepo_Tokens/' + sesId);
  ok(tokDoc && tokDoc.action === 'session' && tokDoc.once === false && tokDoc.used === false && tokDoc.exp === r.body.expiresAt, 'the stored document says session, reusable, with the end time');
  ok(docs('CharmRepo_Tokens').length === 1 && !JSON.stringify([...store]).includes(SES), 'only the sha256 is a key; the token is stored nowhere');
  ok(docs('CharmRepo_AuthAttempts').length === 4 && docs('CharmRepo_Alerts').length === 4 && docs('CharmRepo_Alerts').filter(a => a.ok === true).length === 1, 'the right try is recorded and alerted once');

  /* 4 · the session opens the Settings buttons AND the repo (upload / change / download) */
  for (const [op, b] of SETTINGS_OPS) {
    r = await lib(Object.assign({ op }, b)); ok(r.status === 401 && r.body.error === 'repo-locked' && r.body.need === 'settings' && r.body.why === 'missing', op + ' without a token is refused: ' + r.status + ' ' + JSON.stringify(r.body));
    r = await lib(Object.assign({ op, sandbox: true, repoToken: 'junk' }, b)); ok(r.status === 401 && r.body.why === 'invalid', op + ' with junk is refused');
    r = await call(Lib, Object.assign({ op, repoToken: SES }, b), null, 'GET'); ok(r.status === 401 && r.body.why === 'post-only', op + ' in a URL is refused');
  }
  r = await lib({ op: 'purgeHistory', code: 'not-the-code', repoToken: SES }); ok(r.status === 403 && /passcode/i.test(r.body.error), 'purgeHistory with the session reaches its own passcode rule, which still refuses: ' + r.status);
  r = await lib({ op: 'sandboxPullOrders', repoToken: SES }); ok(r.status === 403 && /only in the sandbox/i.test(r.body.error), 'sandboxPullOrders with the session reaches the op');
  r = await lib({ op: 'sandboxReset', sandbox: true, repoToken: SES }); ok(r.status === 200 && r.body.ok !== false, 'sandboxReset with the session runs: ' + r.status + ' ' + JSON.stringify(r.body).slice(0, 80));
  r = await put(SES); ok(r.status === 200 && r.body.ok, 'the session uploads (masterPutIndex) with no other password: ' + r.status);
  r = await lib({ op: 'masterPatch', sku: 'AS-TEST-01', patch: { engravable: false }, repoToken: SES }); ok(r.status !== 401, 'the session changes a charm (masterPatch): ' + r.status);
  r = await out({ op: 'put', path: 'charmnest/master/as-test-01.ai', contentType: 'application/illustrator', base64: Buffer.from('x').toString('base64'), repoToken: SES }); ok(r.status !== 401, 'the session writes a master file (charmNestOutput put): ' + r.status);
  r = await lib({ op: 'masterRemoveFile', masterHash: 'bbbb2222', repoToken: SES }); ok(r.status === 401 && r.body.why === 'scope' && r.body.need === 'delete', 'but the session alone never deletes: ' + JSON.stringify(r.body));
  r = await lib({ op: 'masterRemoveSku', sku: 'AS-TEST-01', repoToken: SES }); ok(r.status === 401 && r.body.why === 'scope', 'nor removes a SKU');
  ok(store.has('Charm_Master_Index/AS-TEST-01'), 'the refused deletes removed nothing');
  r = await lib({ op: 'sandboxStatus', light: true }); ok(r.status === 200, 'the sandbox page\'s own all-day reads stay free (sandboxStatus)');
  r = await lib({ op: 'sandboxStream', action: 'get', sandbox: true }); ok(r.status !== 401, 'and the order stream (sandboxStream) stays free');

  /* 5 · the old short tokens do not open Settings; a session is asked for with action settings, or session:true on a repo action */
  r = await unlock('upload'); const upTok = r.body.token; ok(r.body.expiresAt === T + 15 * MIN && !r.body.session, 'an upload unlock without the flag is the old 15-minute token');
  r = await lib({ op: 'sandboxReset', sandbox: true, repoToken: upTok }); ok(r.status === 401 && r.body.why === 'scope' && r.body.need === 'settings', 'an upload token does not open the Settings buttons');
  r = await unlock('upload', { session: true }); ok(r.body.ok && r.body.session === true && r.body.action === 'upload' && r.body.expiresAt === Date.parse('2026-10-12T21:00:00Z'), 'upload with session:true is a session');
  const SES2 = r.body.token; ok(store.get('CharmRepo_Tokens/' + sha(SES2)).for === 'upload', 'and remembers what it was asked for');
  r = await lib({ op: 'sandboxReset', sandbox: true, repoToken: SES2 }); ok(r.status === 200, 'so it opens Settings too: one password for both');
  r = await unlock('delete', { session: true }); ok(r.body.ok && r.body.once === true && !r.body.session && r.body.expiresAt === T + 2 * MIN, 'session:true on a delete changes nothing: a single-use token');
  const delWithPw = r.body.token;
  r = await lib({ op: 'masterRemoveSku', sku: 'NOT-THERE', repoToken: delWithPw }); ok(r.status !== 401, 'and it deletes once');

  /* 6 · a delete from the session: no password, one single-use token, recorded (not alerted) */
  const alertsBefore = docs('CharmRepo_Alerts').length, attemptsBefore = docs('CharmRepo_AuthAttempts').length;
  r = await lib({ op: 'repoUnlock', action: 'delete', sessionToken: SES, by: 'Ana', target: 'file X', tab: 'tab1' });
  ok(r.status === 200 && r.body.ok === true && r.body.once === true && r.body.viaSession === true && typeof r.body.token === 'string' && r.body.expiresAt === T + 2 * MIN, 'a delete token with no password: ' + JSON.stringify(Object.keys(r.body)));
  const DEL = r.body.token;
  ok(docs('CharmRepo_Alerts').length === alertsBefore && docs('CharmRepo_AuthAttempts').length === attemptsBefore + 1, 'one attempt record, no Alert');
  const rec = docs('CharmRepo_AuthAttempts').pop(); ok(rec.reason === 'session' && rec.ok === true && rec.action === 'delete' && rec.target === 'file X' && rec.by === 'Ana', 'it says who deleted what: ' + rec.reason);
  r = await lib({ op: 'masterRemoveFile', masterHash: 'bbbb2222', repoToken: DEL }); ok(r.status === 200, 'the single-use token removes the file');
  r = await lib({ op: 'masterRemoveFile', masterHash: 'bbbb2222', repoToken: DEL }); ok(r.status === 401 && r.body.why === 'used', 'and cannot be used twice');
  r = await lib({ op: 'repoUnlock', action: 'delete', sessionToken: 'a'.repeat(64) + '.' + 'b'.repeat(32), by: 'Ana' }); ok(r.status === 200 && r.body.ok === false && r.body.error === 'session' && !r.body.token, 'a forged session token gets no delete token');
  r = await lib({ op: 'repoUnlock', action: 'delete', sessionToken: 'junk', by: 'Ana' }); ok(r.body.ok === false && r.body.error === 'session', 'a junk one neither');
  r = await lib({ op: 'repoUnlock', action: 'delete', sessionToken: upTok, by: 'Ana' }); ok(r.body.ok === false && r.body.error === 'session', 'a short repo token is not a session');
  r = await lib({ op: 'repoUnlock', action: 'upload', sessionToken: SES, by: 'Ana' }); ok(r.body.ok === false && r.body.error === 'wrong' && !r.body.token, 'a session buys no new upload/settings token without the password (it already opens them)');

  /* 7 · Lock now: deleted on the server, the token opens nothing from then on */
  r = await lib({ op: 'repoSession', sessionToken: SES }); ok(r.status === 200 && r.body.active === true && r.body.expiresAt === Date.parse('2026-10-12T21:00:00Z'), 'the status call says it stands');
  r = await call(Lib, { op: 'repoSession', sessionToken: SES }, null, 'GET'); ok(r.status === 405, 'a token is never read from a URL (repoSession)');
  r = await call(Lib, { op: 'repoLock', sessionToken: SES }, null, 'GET'); ok(r.status === 405 && store.has('CharmRepo_Tokens/' + sesId), 'nor a Lock now from a URL');
  r = await lib({ op: 'repoLock', sessionToken: SES }); ok(r.status === 200 && r.body.ok === true && r.body.closed === true, 'Lock now');
  ok(!store.has('CharmRepo_Tokens/' + sesId), 'the session document is gone');
  r = await lib({ op: 'sandboxReset', sandbox: true, repoToken: SES }); ok(r.status === 401 && r.body.why === 'invalid', 'the old token is refused at once on the same instance');
  r = await put(SES); ok(r.status === 401, 'also for the repo');
  r = await lib({ op: 'repoSession', sessionToken: SES }); ok(r.body.active === false && r.body.why === 'invalid', 'the status call says it ended');
  r = await lib({ op: 'repoUnlock', action: 'delete', sessionToken: SES, by: 'Ana' }); ok(r.body.ok === false && r.body.error === 'session', 'and it buys no delete token');
  r = await lib({ op: 'repoLock', sessionToken: SES }); ok(r.status === 200 && r.body.closed === false, 'Lock now twice is harmless');
  r = await lib({ op: 'repoLock', sessionToken: 'junk' }); ok(r.status === 200 && r.body.closed === false && store.has('CharmRepo_Tokens/' + sha(SES2)), 'a junk token locks nothing else');

  /* 8 · it ends by the clock */
  r = await unlock('settings'); const SES3 = r.body.token;
  adv(6 * HOUR + 59 * MIN); r = await lib({ op: 'sandboxReset', sandbox: true, repoToken: SES3 }); ok(r.status === 200, 'at 4:59 pm it still stands');
  adv(2 * MIN + 10000); r = await lib({ op: 'sandboxReset', sandbox: true, repoToken: SES3 }); ok(r.status === 401 && r.body.why === 'expired', 'after 5:00 pm it asks again: ' + r.body.why);
  r = await put(SES3); ok(r.status === 401 && r.body.why === 'expired', 'also for the repo');
  r = await lib({ op: 'repoSession', sessionToken: SES3 }); ok(r.body.active === false && r.body.why === 'expired', 'the status call agrees');
  r = await lib({ op: 'repoUnlock', action: 'delete', sessionToken: SES3, by: 'Ana' }); ok(r.body.ok === false && r.body.error === 'session', 'an ended session buys no delete token');
  setT('2026-10-12T22:30:00Z'); r = await unlock('settings'); const SES4 = r.body.token;
  ok(r.body.expiresAt === Date.parse('2026-10-13T04:00:00Z'), 'unlocked at 6:30 pm: until midnight');
  adv(5 * HOUR + 31 * MIN); r = await lib({ op: 'sandboxReset', sandbox: true, repoToken: SES4 }); ok(r.status === 401, 'and not past midnight');

  /* 9 · a changed password or a revoked account ends every session */
  setT('2026-10-12T14:00:00Z'); r = await unlock('settings'); const SES5 = r.body.token;
  r = await lib({ op: 'sandboxReset', sandbox: true, repoToken: SES5 }); ok(r.status === 200, 'a fresh session works');
  adv(10 * MIN);
  store.set('EtsyMail_Operators/paul', Object.assign({}, paulDoc, { passwordChangedAt: { _seconds: Math.floor((T - 5 * MIN) / 1000), _nanoseconds: 0 } }));
  RA.forget(); await new Promise(res => setTimeout(res, 0));
  // (the operator list is read again after 30 s)
  adv(31000); r = await lib({ op: 'sandboxReset', sandbox: true, repoToken: SES5 }); ok(r.status === 401 && r.body.why === 'revoked', 'the password was changed after the session began: it ends (' + JSON.stringify(r) + ' T=' + T + ')');
  r = await lib({ op: 'repoSession', sessionToken: SES5 }); ok(r.body.active === false && r.body.why === 'revoked', 'the status call says revoked');
  store.set('EtsyMail_Operators/paul', paulDoc); adv(31000);
  r = await unlock('settings'); const SES6 = r.body.token; ok(r.body.ok === true, 'unlock again (account back as it was)');
  adv(31000); store.set('EtsyMail_Operators/paul', Object.assign({}, paulDoc, { revokedAt: { _seconds: Math.floor(T / 1000), _nanoseconds: 0 } }));
  adv(31000); r = await lib({ op: 'sandboxReset', sandbox: true, repoToken: SES6 }); ok(r.status === 401 && r.body.why === 'revoked', 'a revoked account ends its sessions');
  r = await unlock('settings'); ok(r.status === 503 && r.body.error === 'unavailable', 'and unlocks nothing (no account to check against)');
  store.set('EtsyMail_Operators/paul', paulDoc); adv(31000);

  /* 10 · the lockout and the Alert per wrong try are the old ones; a session use makes no Alert */
  const nAlerts = docs('CharmRepo_Alerts').length;
  r = await unlock('settings'); const SES7 = r.body.token; ok(r.body.ok, 'unlocked');
  const alertsAfterUnlock = docs('CharmRepo_Alerts').length; ok(alertsAfterUnlock === nAlerts + 1, 'one Alert for the unlock');
  for (let i = 0; i < 6; i++) { r = await lib({ op: 'sandboxReset', sandbox: true, repoToken: SES7 }); ok(r.status === 200, 'use ' + i); }
  r = await put(SES7); ok(r.status === 200, 'and the repo');
  ok(docs('CharmRepo_Alerts').length === alertsAfterUnlock, 'using the session raised no Alert');
  for (let i = 0; i < 5; i++) r = await unlock('settings', { password: WRONG }, '9.9.9.9');
  ok(r.body.error === 'wrong' && r.body.lockedNow === true && r.body.retryAfterSec > 0, '5 wrong tries lock the source: ' + JSON.stringify(r.body));
  r = await unlock('settings', {}, '9.9.9.9'); ok(r.body.ok === false && r.body.error === 'locked', 'the right password is refused while locked');
  r = await unlock('settings', { password: WRONG }, '9.9.9.9'); ok(r.body.error === 'locked', 'a try during the lock is recorded too');
  ok(docs('CharmRepo_Alerts').length === alertsAfterUnlock + 7, 'every wrong or locked try raised its Alert (' + (docs('CharmRepo_Alerts').length - alertsAfterUnlock) + ')');
  r = await lib({ op: 'sandboxReset', sandbox: true, repoToken: SES7 }, '9.9.9.9'); ok(r.status === 200, 'a session already held keeps working while a stranger is locked out');
  r = await lib({ op: 'repoUnlock', action: 'delete', sessionToken: SES7, by: 'Ana' }, '9.9.9.9'); ok(r.body.ok === true, 'and takes its delete token (no password is guessed)');

  /* 10b · SANDBOXN's Settings row (how many newest orders the sandbox pulls) is a repo "modify" write: a session opens it, nothing else does */
  ok(RA.requirement('charmNestLibrary', 'sandboxPullLimitSet', {}).need === 'modify', 'the pull-number write needs a "modify" token');
  r = await lib({ op: 'sandboxPullLimitSet', limit: 120, by: 'Paul' }); ok(r.status === 401 && r.body.error === 'repo-locked' && r.body.need === 'modify', 'with no token it is refused');
  r = await lib({ op: 'sandboxPullLimitSet', limit: 120, by: 'Paul', repoToken: SES7 }); ok(r.status === 200 && r.body.ok === true, 'a session unlock lets it in (' + r.status + ')');

  /* 11 · nothing secret anywhere */
  const everything = JSON.stringify([...store].filter(([k]) => /^CharmRepo_/.test(k)));
  for (const secret of [PAUL_PW, WRONG, paulDoc.passwordHash, paulDoc.salt, SES, SES2, SES3, SES4, SES5, SES6, SES7, DEL]) ok(!everything.includes(secret), 'no password, hash, salt or token in any CharmRepo_ document');
  ok(docs('CharmRepo_AuthAttempts').concat(docs('CharmRepo_Alerts')).every(d => Object.values(d).every(v => typeof v !== 'object' || v === null)), 'flat fields only');
  ok(Object.keys(RA.SETTINGS_OPS).sort().join() === 'purgeHistory,sandboxPullOrders,sandboxReset', 'the locked Settings ops are exactly these three (SANDBOXN\'s sandboxPullLimitSet is a repo "modify" op)');
  console.log('\nadmin-session: ' + n + ' checks passed'); process.exit(0);
})().catch(e => { console.error('FAIL ' + (e && e.stack || e)); process.exit(1); });
