// RV1 review of FC5's employee revision (Station_Rev/employee, netlify/functions/_employeeRev.js): the SESSION door of firebaseOrders must count every
// change of the sign-in list, because the console keeps what it read (up to a minute) while the counter has not moved.
//   * a start, an end and a beat that ends the session are counted; a plain keep-alive beat is not (FC5)
//   * a beat that CREATES the session document (its start never arrived: a blip, a 429, a cold start) is a new sign-in and must be counted too;
//     it was not, so the person showed on "Signed in now" up to a minute late
//   * the sandbox is never counted
//   node tests/stations/rv1-session-door-rev.cjs
'use strict';
const path = require('path'), assert = require('assert'), Module = require('module');
const root = path.join(__dirname, '../..');

class Ts { constructor(m) { this.m = m; } toMillis() { return this.m; } static fromMillis(m) { return new Ts(m); } }
const clone = v => v instanceof Ts ? v : Array.isArray(v) ? v.map(clone) : v && typeof v === 'object' ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, clone(x)])) : v;
const colls = new Map(), writes = [];
const data = n => { if (!colls.has(n)) colls.set(n, new Map()); return colls.get(n); };
function query(name) {
  const q = { where: () => q, orderBy: () => q, limit: () => q, select: () => q, get: async () => ({ docs: [...data(name)].map(([id, d]) => ({ id, data: () => clone(d) })), size: data(name).size, empty: !data(name).size }) };
  return q;
}
const ref = (name, id) => ({ id, name,
  get: async () => { const d = data(name).get(id); return { exists: !!d, id, data: () => clone(d) }; },
  set: async (v, o) => { writes.push({ name, id }); data(name).set(id, o && o.merge ? Object.assign({}, data(name).get(id) || {}, clone(v)) : clone(v)); },
  update: async v => { writes.push({ name, id }); if (!data(name).has(id)) throw Object.assign(new Error('5 NOT_FOUND'), { code: 5 }); data(name).set(id, Object.assign({}, data(name).get(id), clone(v))); } });
const db = {
  collection: name => Object.assign(query(name), { doc: id => ref(name, id) }),
  getAll: async (...a) => Promise.all(a.filter(x => x && x.get).map(r => r.get())),
  runTransaction: async fn => fn({ get: r => r.get(), getAll: (...a) => Promise.all(a.filter(x => x && x.get).map(r => r.get())), set: (r, v, o) => r.set(v, o), update: (r, v) => r.update(v) })
};
const fakeAdmin = { firestore: Object.assign(() => db, { FieldValue: { serverTimestamp: () => 'ts', increment: n => ({ __inc: n }), delete: () => null }, Timestamp: Ts }) };
const realLoad = Module._load;
Module._load = function (req, ...rest) { if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin; return realLoad.call(this, req, ...rest); };
process.env.EDIT_PASSCODE = 'synthetic-pass-rv1';
const door = require(path.join(root, 'netlify/functions/firebaseOrders.js'));
Module._load = realLoad;
const logs = []; console.warn = console.log = console.error = console.info = (...a) => logs.push(a.join(' '));
const say = (...a) => process.stdout.write(a.join(' ') + '\n');

let NOW = Date.parse('2026-10-07T14:00:00Z'), ip = 0;
Date.now = () => NOW;
const sess = o => Object.assign({ id: 'sorting__s1__Ana_Test__t1', event: 'beat', person: 'Ana Test', station: 'sorting', device: 'sorting-1', computerId: 'pc-TESTAAAA', computerLabel: 'Sorting', at: NOW, sentAt: NOW }, o);
async function post(session, sandbox) {
  const r = await door.handler({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '198.51.100.' + (++ip % 250) }, queryStringParameters: sandbox ? { sandbox: '1' } : {}, body: JSON.stringify({ session }) });
  assert.strictEqual(r.statusCode, 200, r.body);
  return JSON.parse(r.body);
}
const counted = () => writes.filter(w => w.name === 'Station_Rev').length;
const tick = ms => { NOW += ms; };

(async () => {
  assert.strictEqual(counted(), 0);
  await post(sess({ event: 'start' }));
  assert.strictEqual(counted(), 1, 'a start is a counted change');
  tick(30000); await post(sess({ event: 'beat' }));
  assert.strictEqual(counted(), 1, 'a plain keep-alive beat is not');
  say('ok   start counted, a plain beat not');

  // a beat for a session whose start never arrived creates the document: a new sign-in
  tick(1000);
  const out = await post(sess({ id: 'sorting__s2__Bo_Test__t2', person: 'Bo Test', device: 'sorting-2', computerId: 'pc-TESTBBBB', event: 'beat' }));
  assert.ok(data('Station_Sessions').has('sorting__s2__Bo_Test__t2'), 'the beat created the session document');
  assert.strictEqual(out.ended, false);
  assert.strictEqual(counted(), 2, 'a beat that creates the session is a new sign-in: counted (else the console shows the person up to a minute late)');
  say('ok   a beat that creates a session is counted');
  tick(30000); await post(sess({ id: 'sorting__s2__Bo_Test__t2', person: 'Bo Test', device: 'sorting-2', computerId: 'pc-TESTBBBB', event: 'beat' }));
  assert.strictEqual(counted(), 2, 'and its next beat is not');

  tick(1000); await post(sess({ event: 'end', reason: 'signOut' }));
  assert.strictEqual(counted(), 3, 'an end is counted');
  tick(1000); await post(sess({ event: 'beat' }));
  assert.strictEqual(counted(), 3, 'a beat on a session already ended changes nothing');
  say('ok   end counted, a beat after the end not');

  // the sandbox is never counted, and creates no revision document
  const before = counted();
  await post(sess({ id: 'sorting__s3__Cy_Test__t3', person: 'Cy Test', event: 'start' }), true);
  await post(sess({ id: 'sorting__s4__Di_Test__t4', person: 'Di Test', event: 'beat' }), true);
  assert.strictEqual(counted(), before, 'the sandbox does not move the production counter');
  say('ok   the sandbox is not counted');
  say('rv1-session-door-rev: all checks passed');
})().catch(e => { process.stdout.write('FAILED: ' + (e && e.stack || e) + '\n' + logs.slice(-5).join('\n') + '\n'); process.exit(1); });
