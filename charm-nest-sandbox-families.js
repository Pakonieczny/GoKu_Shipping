/* charm-nest-sandbox-families.js — the ONE registry of everything a sandbox run leaves behind (Paul, 10 Oct 2026:
   "completely wipe ALL sandbox data and order info; the only thing that remains is the employee efficiency and the Charm
   repo"). The server's wipe (charmNestLibrary.js sandboxWipe: Reset sandbox records, Reset the sandbox..., Purge all run
   history...), its status count (op sandboxStatus), the Settings line "In the sandbox now ... kept ..." and the guard test
   (tests/charm-nest/sandbox-wipe-guard.cjs) all read THIS list, so a family added here is wiped, counted and tested
   everywhere, and a sandbox write that is not listed here fails the guard test.

   UMD: window.CharmNestSandboxFamilies in the page, module.exports on the server and in tests.

   families()   → [{ key, label, store, ... }]  every family the wipe clears; the server ones first, then the browser's.
                  store: "firestore"  a Sandbox_<name> collection, wiped whole (key = the name without the prefix, as
                                      sandboxStatus's records{} names it; subs = subcollections deleted with each document)
                         "shared"     a place production also uses, cut by the sandbox's own marks only (docs by id prefix
                                      and flag, or one field removed from a shared document)
                         "doc"        one named document of a shared collection that only the sandbox ever writes
                         "storage"    a Storage prefix (every file under it goes, but `keep`)
                         "browser"    a browser-side store (WIPEBROWSER's entries; the server never touches them)
   protected()  → the keep list: [{ key, label, store, why }]: what a sandbox wipe never deletes.
   server() / browser()   the two halves of families(); firestoreCollections() the Sandbox_ names the wipe clears.
   isProtected(store, path) → true when a Firestore path "Collection/doc" or a Storage name is on the keep list.
   Nothing here is a secret; there is no passcode in this file. */
(function (root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api; else root.CharmNestSandboxFamilies = api;
})(typeof self !== "undefined" ? self : globalThis, function () {
  "use strict";
  const PREFIX = "Sandbox_";
  const fs = (key, label, extra) => Object.assign({ key, label, store: "firestore" }, extra || {});

  /* ── the server: Firestore collections (each is Sandbox_<key>) ── */
  const SERVER = [
    // the sorter's own records
    fs("Charm_Nest_Sheets", "sheets"),
    fs("Charm_Nest_Sets", "sets"),
    fs("Charm_Nest_Counters", "set counters"),
    fs("Charm_Nest_Runs", "runs"),
    fs("Charm_Nest_Run_Lines", "run lines"),
    fs("Charm_Nest_Run_Live", "live run parts"),
    fs("Charm_Nest_Release", "release records"),
    fs("Charm_Pool", "pool rows"),
    fs("Charm_Pool_Back", "back pool rows"),
    fs("Charm_Nest_Arrivals", "arrival records"),
    fs("Charm_Nest_Cancelled", "cancelled orders"),
    fs("Charm_Nest_Cancelled_History", "cancel history"),
    fs("Charm_Custom_Orders", "custom orders"),
    fs("Charm_Custom_Sheet", "custom seals"),
    fs("Design_Bridge", "bridge logs", { subs: ["log"] }),
    fs("Order_Timeline", "timeline events"),
    // cuts and leftovers
    fs("Charm_Nest_Rose_Stock", "Rose Gold stock records", { subs: ["cuts"] }),
    fs("Charm_Nest_Rose_Rehearsals", "Rose Gold rehearsals"),
    fs("Charm_Nest_Remnants", "leftovers"),
    // the learned maps a person answered in the sandbox
    fs("Charm_Sku_Aliases", "listing aliases"),
    fs("Charm_Sku_NoDesign", "no-design SKUs"),
    fs("Charm_Option_Map", "option maps"),
    // the stations' copies of the orders
    fs("Brites_Orders", "station orders", { subs: ["messages"] }),
    fs("Design_Completed Orders", "finished orders"),
    fs("Design_RealTime_Selected_Orders", "order locks"),
    fs("Design_Order_Archive", "order archive"),
    // engraving jobs, shape guidance and the paid readings the sandbox saved
    fs("Charm_Nest_Agent", "engraving jobs"),
    fs("Charm_Nest_Agent_Cache", "saved engraving readings"),
    fs("Charm_Nest_Shape_Guidance", "shape guidance"),
    // the stream and the snapshot pointer (Charm_Sandbox is the sandbox's own collection, no prefix; the pull budget doc stays)
    { key: "Charm_Sandbox", label: "stream and snapshot", store: "doc", collection: "Charm_Sandbox", keepDocs: ["pulls"] },
    // places production uses too, cut by the sandbox's own marks
    { key: "EtsyMail_OrderLinks", label: "customer-mail links", store: "shared", collection: "EtsyMail_OrderLinks", idPrefix: "olsb_", flag: { sandbox: true } },
    { key: "Charm_Nest_CustomRead.decidedSandbox", label: "sandbox line decisions", store: "shared", collection: "Charm_Nest_CustomRead", field: "decidedSandbox" },
    // Storage
    { key: "Storage:charmnest/sandbox/", label: "sandbox files", store: "storage", prefix: "charmnest/sandbox/", keep: ["charmnest/sandbox/master/"] },
    { key: "Storage:design-archive/sandbox/", label: "design-archive files", store: "storage", prefix: "design-archive/sandbox/", keep: [] }
  ];

  /* ── the browser: WIPEBROWSER adds its entries here (store "browser"; key = the store's own name) ── */
  const BROWSER = [];

  /* ── what a wipe NEVER deletes (production, the Charm repo, employee efficiency, shared caches, settings) ── */
  const PROTECTED = [
    { key: "Charm_Master_Index", label: "Charm repo: master index", store: "firestore", why: "the Charm repo" },
    { key: "Charm_Master_Files", label: "Charm repo: master files", store: "firestore", why: "the Charm repo" },
    { key: "Storage:charmnest/master/", label: "Charm repo files", store: "storage", prefix: "charmnest/master/", why: "the Charm repo" },
    { key: "Storage:charmnest/sandbox/master/", label: "master files the shared index points to", store: "storage", prefix: "charmnest/sandbox/master/", why: "the Charm repo" },
    { key: "Station_Activity", label: "employee efficiency: activity", store: "firestore", also: PREFIX + "Station_Activity", why: "employee efficiency stays whole" },
    { key: "Efficiency_Daily", label: "employee efficiency: daily rollups", store: "firestore", also: PREFIX + "Efficiency_Daily", why: "employee efficiency stays whole" },
    { key: "Station_Sessions", label: "employee efficiency: sign-in sessions", store: "firestore", also: PREFIX + "Station_Sessions", why: "employee efficiency stays whole" },
    { key: "Station_Live", label: "employee efficiency: live presence", store: "firestore", also: PREFIX + "Station_Live", why: "employee efficiency stays whole" },
    { key: "Laser_Sheet_Times", label: "employee efficiency: laser sheet times", store: "firestore", also: PREFIX + "Laser_Sheet_Times", why: "employee efficiency stays whole" },
    { key: "Station_Rev", label: "employee efficiency: data revision", store: "firestore", why: "employee efficiency stays whole" },
    { key: "config", label: "config documents and passcodes", store: "firestore", why: "settings and passcodes" },
    { key: "Charm_Sandbox/pulls", label: "the daily Etsy pull budget", store: "firestore", why: "an Etsy call budget, not sandbox data" },
    { key: "Charm_Nest_Rev", label: "production revision counters", store: "firestore", why: "production" }
  ];

  const clone = list => list.map(f => Object.assign({}, f));
  const names = () => SERVER.filter(f => f.store === "firestore").map(f => PREFIX + f.key);
  function isProtected(store, path) {
    const p = String(path || "");
    return PROTECTED.some(f => {
      if (store === "storage") return f.store === "storage" && p.startsWith(f.prefix);
      if (f.store !== "firestore") return false;
      const coll = p.split("/")[0];
      return f.key.indexOf("/") < 0 ? (coll === f.key || coll === f.also) : p === f.key || p.startsWith(f.key + "/");
    });
  }
  return {
    PREFIX,
    families: () => clone(SERVER).concat(clone(BROWSER)),
    server: () => clone(SERVER),
    browser: () => clone(BROWSER),
    protected: () => clone(PROTECTED),
    firestoreCollections: names,
    isProtected
  };
});
