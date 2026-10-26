// The tests' stand-in for the server's Admin list (netlify/functions/_stationAdmins.js door): the sorter asks
// POST firebaseOrders { stationAdmin: "<name>" } once per sign-in and gets { ok: true, admin: true | false }. The Employee efficiency
// screen is the master admin's alone (charm-nest-master-admin.js), so a test that opens it signs in as a name this door calls the Admin.
//   const door = require('./master-admin-door.cjs');
//   await door.admin(ctx);                          // every sorter page of this context: "Tester" and "Paul K" are Admins
//   await door.admin(ctx, { names: ['Paul K'], delayMs: 800, log: asked => ... });
// Registered after a test's own catch-all route, so it answers first and hands every other request on (route.fallback()).
// Fake names only, nothing leaves the machine, no Firestore.
const fold = n => String(n || '').normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase().replace(/['‘’`´]/g, '').replace(/[^a-z0-9]+/g, ' ').trim();
async function admin(ctx, opts = {}) {
  const names = new Set((opts.names || ['Tester', 'Paul K']).map(fold));
  await ctx.route(u => /\/\.netlify\/functions\/firebaseOrders/.test(u.pathname), async route => {
    const req = route.request();
    let b = null; if (req.method() === 'POST') { try { b = JSON.parse(req.postData() || 'null'); } catch (_) {} }
    if (!b || typeof b !== 'object' || b.stationAdmin === undefined) return route.fallback();
    const who = String(b.stationAdmin);
    if (opts.log) { try { opts.log(who); } catch (_) {} }
    if (opts.delayMs) await new Promise(r => setTimeout(r, opts.delayMs));
    return route.fulfill({ status: 200, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ ok: true, admin: names.has(fold(who)) }) });
  });
  // The repo Alert pill (charm-nest-repo-alerts.js, Paul 11 Oct) asks charmNestLibrary { op: "repoAlerts", by } for the master admin, and the
  // server repeats the Admin check against its own list, where this door's names are not: so the door answers that one read too, for its
  // own Admins only (no attempts, nothing to close), and hands every other request on.
  await ctx.route(u => /\/\.netlify\/functions\/charmNestLibrary/.test(u.pathname), async route => {
    const req = route.request();
    let b = null; if (req.method() === 'POST') { try { b = JSON.parse(req.postData() || 'null'); } catch (_) {} }
    if (!b || typeof b !== 'object' || b.op !== 'repoAlerts' || !names.has(fold(b.by))) return route.fallback();
    return route.fulfill({ status: 200, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ ok: true, alerts: [], total: 0, more: false }) });
  });
}
module.exports = { admin, fold };
