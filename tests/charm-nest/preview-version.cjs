/* The Library keeps a sheet's picture by the time the picture was saved (previewAt, the server's), not by the record's update time:
   a seal or a step on the record changes updatedAt, and every open page used to load the sheet's picture again for it.
   The real handler over the in-memory Firestore; offline, no production record. */
const assert = require('node:assert/strict'), path = require('node:path');
(async () => {
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] });
  const call = async b => { const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(b) }); return { status: r.status, ...await r.json() }; };
  const row = async () => (await call({ op: 'listSheets', limit: 10 })).sheets.find(x => x.id === 'prev-sheet-1');
  const base = { id: 'prev-sheet-1', metal: 'gold', day: '2026-10-06', status: 'complete', charmCount: 2, placedCount: 2, poolIds: [], orders: [], charms: [], placements: [] };
  const preview = { path: 'charmnest/x/preview.png', url: 'https://firebasestorage.googleapis.com/v0/b/b/o/charmnest%2Fx%2Fpreview.png?alt=media&token=t' };
  try {
    assert.equal((await call({ op: 'putSheet', sheet: { ...base, outputs: null, saving: true } })).status, 200);
    assert(!('previewAt' in await row()), 'a record with no picture yet has no picture time');
    assert.equal((await call({ op: 'putSheet', sheet: { ...base, previewAt: 5, outputs: { preview }, saving: false } })).status, 200);
    const saved = await row(); assert(saved.previewAt > 1.7e12, 'the save that carries the picture stamps its time (the server\'s own, not the page\'s 5)');
    await new Promise(r => setTimeout(r, 15));
    assert.equal((await call({ op: 'putSheet', sheet: { id: 'prev-sheet-1', previewAt: 7, releaseFull: true } })).status, 200);
    const later = await row(); assert.equal(later.previewAt, saved.previewAt, 'a save with no picture, or a page echoing a time, leaves the picture time alone');
    assert(later.updatedAt > saved.updatedAt, 'while the record\'s own update time moved on');
    await new Promise(r => setTimeout(r, 15));
    await call({ op: 'putSheet', sheet: { ...base, outputs: { preview }, saving: false } });
    assert((await row()).previewAt > saved.previewAt, 'a picture saved again has a new time');
    console.log('Preview version OK: previewAt is the server\'s, stamped by the save that carries the picture only');
  } finally { srv.close && srv.close(); }
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
