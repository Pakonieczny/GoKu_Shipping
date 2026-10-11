// The Charm Repo's password door (netlify/functions/_charmNestRepoAuth.js) asks an HTTP call to a protected op for a repoToken, and the Settings
// menu's buttons (purgeHistory, sandboxReset, sandboxPullOrders) ask for the standing unlock. A test that is not about the door answers it the way a signed-in
// master admin would: it asks the door for a token on the SAME in-memory Firestore the handler uses (no password involved) and puts it in the body.
// Nothing real is read or written.
//   const stamp = require('./_repoToken.cjs');   body = await stamp('charmNestLibrary', body);   // before h.handler(...)
//   await stamp.prime();   // a test that compares the whole database before and after: make the unlock first, so it is part of "before"
'use strict';
const path = require('path');
const fnDir = path.join(__dirname, '../../netlify/functions');
const sessions = new WeakMap();   // db → { token, exp }: one standing unlock per database, reused (using it writes nothing)
const dbOf = db => db || require(path.join(fnDir, 'firebaseAdmin.js')).firestore();
/* a standing unlock only counts while the operator account it was bought with exists (a revoked account or a changed password ends it): a database with none gets
   a throwaway `paul`, its password thrown away at once, so nobody can unlock for real with it */
async function ensureOperator(d) {
  const ref = d.collection('EtsyMail_Operators').doc('paul'), s = await ref.get(); if (s.exists) return;
  const crypto = require('crypto'), salt = crypto.randomBytes(32), hash = crypto.pbkdf2Sync(crypto.randomBytes(8).toString('hex'), salt, 1000, 64, 'sha512');
  await ref.set({ username: 'paul', displayName: 'Paul K', role: 'owner', passwordHash: hash.toString('hex'), salt: salt.toString('hex'), iterations: 1000, digest: 'sha512' });
}
async function session(db) {
  const RA = require(path.join(fnDir, '_charmNestRepoAuth.js')), d = dbOf(db), now = RA.deps.now(), c = sessions.get(d);
  // (a test that clears its in-memory database loses the unlock record with it: look before reusing)
  if (c && c.exp > now + 60000 && (await d.collection('CharmRepo_Tokens').doc(require('crypto').createHash('sha256').update(c.token, 'utf8').digest('hex')).get()).exists) return c.token;
  await ensureOperator(d);
  const t = await RA.issue(d, { action: 'settings', by: 'test' });
  sessions.set(d, { token: t.token, exp: t.exp });
  return t.token;
}
async function stamp(fn, body, db) {
  if (!body || typeof body !== 'object' || body.repoToken) return body;
  const RA = require(path.join(fnDir, '_charmNestRepoAuth.js'));
  const need = RA.requirement(fn, body.op, body); if (!need) return body;
  // (a Settings-class need is served by the standing unlock only; the repo needs keep the short 'other' token they always had)
  if (need.need === 'settings') return Object.assign({}, body, { repoToken: await session(db) });
  const t = await RA.issue(dbOf(db), { action: need.need.startsWith('delete') ? 'delete' : 'other', by: 'test' });
  return Object.assign({}, body, { repoToken: t.token });
}
stamp.prime = db => session(db);
stamp.session = session;   // (the standing unlock itself: the stand-in server answers the page's session questions with it)
module.exports = stamp;
