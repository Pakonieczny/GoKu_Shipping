/* The pages' firebase compat SDK, replaced for the all-stations end-to-end test (served in place of gstatic's firebase-app-compat.js).
 * A Firestore client that talks to the fake shop's /__fs door over the page's own origin, so what one page writes (a phone scanner's
 * order number) is what another page reads (the desktop's onSnapshot), as on the real site. Documents change when polled (300 ms,
 * with the real timers captured before the test's fake clock was installed: a clock that jumps minutes must not run the polling). */
(function () {
  "use strict";
  var rst = window.__rst || window.setTimeout.bind(window), rsi = window.__rsi || window.setInterval.bind(window);
  var FS = location.origin + "/__fs/";
  function call(op, body) {
    return fetch(FS + op, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) }).then(function (r) { return r.json(); });
  }
  function Ts(ms) { this.ms = ms; this.seconds = Math.floor(ms / 1000); this.nanoseconds = (ms % 1000) * 1e6; }
  Ts.prototype.toMillis = function () { return this.ms; };
  Ts.prototype.toDate = function () { return new Date(this.ms); };
  Ts.now = function () { return new Ts(Date.now()); };
  Ts.fromMillis = function (m) { return new Ts(m); };
  Ts.fromDate = function (d) { return new Ts(d.getTime()); };
  var FV = {
    delete: function () { return { __fv: "delete" }; }, serverTimestamp: function () { return { __fv: "ts" }; },
    increment: function (n) { return { __fv: "inc", n: n }; }, arrayUnion: function () { return { __fv: "union", items: [].slice.call(arguments) }; }
  };
  function wire(v) {
    if (v instanceof Ts) return { __ts: v.ms };
    if (v instanceof Date) return { __ts: v.getTime() };
    if (Array.isArray(v)) return v.map(wire);
    if (v && typeof v === "object") { var o = {}; Object.keys(v).forEach(function (k) { if (v[k] !== undefined) o[k] = wire(v[k]); }); return o; }
    return v;
  }
  function revive(v) {
    if (Array.isArray(v)) return v.map(revive);
    if (v && typeof v === "object") {
      if (v.__ts != null) return new Ts(v.__ts);
      var o = {}; Object.keys(v).forEach(function (k) { o[k] = revive(v[k]); }); return o;
    }
    return v;
  }
  function snapOf(ref, r) {
    var d = r && r.exists ? revive(r.data) : undefined;
    return { id: ref.id, ref: ref, exists: !!(r && r.exists), metadata: { hasPendingWrites: false, fromCache: false }, data: function () { return d; },
      get: function (f) { var x = d; String(f).split(".").forEach(function (p) { x = x == null ? undefined : x[p]; }); return x; } };
  }
  function listen(fetcher, cb, errcb) {
    var last = null, dead = false, prev = {};
    function tick() {
      if (dead) return;
      fetcher().then(function (out) {
        if (dead) return;
        var key = JSON.stringify(out.key);
        if (key !== last) {
          last = key;
          var snap = out.snap, now = {};
          if (snap.docs) snap.docs.forEach(function (d) { now[d.id] = JSON.stringify(d.data()); });
          snap.docChanges = function () {
            var ch = []; Object.keys(now).forEach(function (id) { if (!(id in prev)) ch.push({ type: "added", doc: snap.docs.filter(function (d) { return d.id === id; })[0] }); else if (prev[id] !== now[id]) ch.push({ type: "modified", doc: snap.docs.filter(function (d) { return d.id === id; })[0] }); });
            Object.keys(prev).forEach(function (id) { if (!(id in now)) ch.push({ type: "removed", doc: { id: id, data: function () { return JSON.parse(prev[id]); } } }); });
            return ch;
          };
          var p = prev; prev = now; void p;
          try { cb(snap); } catch (e) { try { console.error(e); } catch (_) {} }
        }
      }, function (e) { if (errcb) try { errcb(e); } catch (_) {} });
      rst(tick, 300);
    }
    rst(tick, 0);
    return function () { dead = true; };
  }
  function docRef(path) {
    var parts = path.split("/"), id = parts[parts.length - 1];
    var ref = { id: id, path: path,
      collection: function (n) { return colRef(path + "/" + n); },
      get: function () { return call("get", { path: path }).then(function (r) { return snapOf(ref, r); }); },
      set: function (data, opts) { return call("set", { path: path, data: wire(data), merge: !!(opts && opts.merge) }); },
      update: function (data) { return call("update", { path: path, data: wire(data) }); },
      delete: function () { return call("delete", { path: path }); },
      onSnapshot: function (cb, errcb) { return listen(function () { return call("get", { path: path }).then(function (r) { return { key: r, snap: snapOf(ref, r) }; }); }, cb, errcb); } };
    Object.defineProperty(ref, "parent", { get: function () { return colRef(parts.slice(0, -1).join("/")); } });
    return ref;
  }
  function colRef(path, spec) {
    spec = spec || { where: [], order: [], limit: 0 };
    var parts = path.split("/"), q = { id: parts[parts.length - 1], path: path };
    function next(patch) { return colRef(path, { where: patch.where || spec.where, order: patch.order || spec.order, limit: patch.limit != null ? patch.limit : spec.limit }); }
    q.doc = function (id) { return docRef(path + "/" + (id || ("auto" + Date.now().toString(36) + Math.random().toString(36).slice(2, 6)))); };
    q.add = function (data) { return call("add", { path: path, data: wire(data) }).then(function (r) { return docRef(path + "/" + r.id); }); };
    q.where = function (f, o, v) { return next({ where: spec.where.concat([[f, o, wire(v)]]) }); };
    q.orderBy = function (f, d) { return next({ order: spec.order.concat([[f, d || "asc"]]) }); };
    q.limit = function (n) { return next({ limit: n }); };
    q.limitToLast = function (n) { return next({ limit: n }); };
    q.startAfter = function () { return q; };
    function run() {
      return call("query", { path: path, where: spec.where, order: spec.order, limit: spec.limit }).then(function (r) {
        var docs = r.docs.map(function (d) { return snapOf(docRef(path + "/" + d.id), { exists: true, data: d.data }); });
        return { key: r, snap: { docs: docs, empty: !docs.length, size: docs.length, forEach: function (f) { docs.forEach(f); }, metadata: {} } };
      });
    }
    q.get = function () { return run().then(function (o) { return o.snap; }); };
    q.onSnapshot = function (cb, errcb) { return listen(run, cb, errcb); };
    return q;
  }
  var db = { collection: function (n) { return colRef(n); }, doc: function (p) { return docRef(p); },
    batch: function () { var ops = []; return { set: function (r, d, o) { ops.push(function () { return r.set(d, o); }); }, update: function (r, d) { ops.push(function () { return r.update(d); }); }, delete: function (r) { ops.push(function () { return r.delete(); }); }, commit: function () { return Promise.all(ops.map(function (f) { return f(); })); } }; },
    runTransaction: function (fn) { return Promise.resolve(fn({ get: function (r) { return r.get(); }, set: function (r, d, o) { return r.set(d, o); }, update: function (r, d) { return r.update(d); }, delete: function (r) { return r.delete(); } })); },
    enablePersistence: function () { return Promise.resolve(); }, settings: function () {} };
  var firestore = function () { return db; };
  firestore.FieldValue = FV; firestore.Timestamp = Ts; firestore.FieldPath = { documentId: function () { return "__name__"; } };
  var auth = function () { return { signInAnonymously: function () { return Promise.resolve({ user: { uid: "anon" } }); }, onAuthStateChanged: function (cb) { rst(function () { cb({ uid: "anon" }); }, 0); return function () {}; }, currentUser: { uid: "anon" } }; };
  var app = { options: { projectId: "fake", storageBucket: "fake.appspot.com" }, name: "[DEFAULT]" };
  window.firebase = { apps: [app], initializeApp: function () { return app; }, app: function () { return app; }, firestore: firestore, auth: auth,
    storage: function () { return { ref: function () { return { put: function () { return Promise.reject(new Error("no storage in the test")); }, getDownloadURL: function () { return Promise.resolve("/__pic/x.png"); } }; } }; } };
  void rsi;
})();
