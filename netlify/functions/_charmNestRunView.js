"use strict";
/* RUNVIEW (cloud plan hard-parts.md 0.3): one run's records as the view the ported browser rules read. It is the set assembly's own
   loadView (_charmNestAssembly.js: the run's sheets as pages, its sets, the assembly's memory) plus what the sweeps and the placement
   rules ask of it: pagesOf(metal), in page order, and the run's settings. Nothing is written here. */
const Assembly = require("./_charmNestAssembly");

async function loadView(o) {
  const V = await Assembly.loadView({ db: o.db, run: o.run, now: o.now, ids: o.ids, sets: o.sets });
  V.settings = (o.run && o.run.settings) || {};
  V.pagesOf = metal => (V.pages || []).filter(p => p && p.metal === metal).sort((a, b) => (+a.page || 0) - (+b.page || 0));
  return V;
}

module.exports = { loadView };
