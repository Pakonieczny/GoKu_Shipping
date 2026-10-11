// The Charm Repo's password door (netlify/functions/_charmNestRepoAuth.js) asks an HTTP call to a protected op for a repoToken.
// A test that is not about the door answers it the way a signed-in master admin would: it asks the door for a token on the SAME
// in-memory Firestore the handler uses (no password involved) and puts it in the body. Nothing real is read or written.
//   const stamp = require('./_repoToken.cjs');   body = await stamp('charmNestLibrary', body);   // before h.handler(...)
'use strict';
const path = require('path');
const fnDir = path.join(__dirname, '../../netlify/functions');
module.exports = async function stamp(fn, body, db) {
  if (!body || typeof body !== 'object' || body.repoToken) return body;
  const RA = require(path.join(fnDir, '_charmNestRepoAuth.js'));
  const need = RA.requirement(fn, body.op, body); if (!need) return body;
  const t = await RA.issue(db || require(path.join(fnDir, 'firebaseAdmin.js')).firestore(), { action: need.need.startsWith('delete') ? 'delete' : 'other', by: 'test' });
  return Object.assign({}, body, { repoToken: t.token });
};
