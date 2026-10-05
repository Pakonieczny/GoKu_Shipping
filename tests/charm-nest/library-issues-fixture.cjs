/* Shared fixtures for the Library '!' issues panel tests (charm-nest-library-issues.js): sheet records, the order rows,
   fake issues in the exact shape of CharmNestReadiness.issues (round 2, interface B), offline "listing photos", and a
   browser page that carries the app's own shell and CSS (charm-nest-1.html with its scripts taken out) around the REAL
   LaserReview / set card code, so a screenshot shows what the Library shows. Nothing here reaches a network. */
const fs = require('node:fs'), path = require('node:path');
const root = path.join(__dirname, '../..');
const read = f => fs.readFileSync(path.join(root, f), 'utf8');

const back = (id, sheetId) => ({ poolId: id, sheetId, approvedAt: 10, approvedBy: 'Paul', verified: { geometry: { ok: true }, file: { ok: true } }, outputs: { ai: { path: id + '.ai', url: 'https://example.com/' + id + '.ai' } } });
/* a gold sheet whose every back is saved and whose QR label covers every order (Paul's screenshot: 28 / 28 backs, QR label present) */
function sheet(id, { n = 28, metal = 'gold', setId = 'set1', index = 1, base = 1000, seq = 1, preview = 'https://example.com/p.png' } = {}) {
  const pool = [...Array(n)].map((_, i) => `${base + i}_t${i}_1`), orders = pool.map(p => p.split('_')[0]);
  return { id, metal, setId, setSeq: seq, runId: 'run1', sheetIndex: index, status: 'complete', poolIds: pool, placedCount: n, charmCount: n, density: .73, verification: { ok: true }, preview, outputs: { ai: 'https://example.com/f.ai' },
    orders, label: { files: [{ path: 'qr.png', url: 'https://example.com/qr.png', payload: 'x', orders }] }, backPool: pool.map(p => back(p, id)), engraving: {}, orderReadiness: Object.fromEntries(orders.map(o => [o, { ready: true }])), updatedAt: 1, day: '2026-10-03', folder: `GF_Oct.03.26_Set-${seq}_Sheet-${index}` };
}

const NAMES = ['Nathaly Soto', 'Emily Chambers', 'Leslie Suhr', 'Jechelle Aragones', 'Yera Espinosa Madariaga', 'Stephanie Cooper', 'Calvin Ly', 'Heike Wagener', 'Nicole Offermann', 'Sunny Makowiak', 'Sarah Löhe', 'Chanel Sargeant', 'Tim Wright', 'Gregory Horvitz', 'Shari L. Morrison', 'Kathleen Henry', 'Susan Pforr', 'Nikki Boyles', 'Nicole', 'Maximiliana Alexandria von Habsburg-Lothringen-Esterházy'];
const KEYS = ['pooled', 'otherSheetNotReady', 'noSku', 'pooled', 'otherSheetNotReady', 'noSku', 'unmatched', 'noDesign', 'held'];
/* the issues of a sheet, in the exact shape of CharmNestReadiness.issues: `n` orders (cycling the reasons unless `keys` says), and optionally the sheet's own blocker.
   Round 7 made `mates` the SET's wait (a mate sheet of the same set that is not ready: quiet, never an issue); round 8 (Paul: "only related items to that particular sheet") took it out of every
   list: nothing sends such an entry any more, a test passes `mates` only to prove the panel drops it. The set's wait is said under the Approve button (setGate's reason).
   `trouble` the real set trouble (a sheet of the set that cannot be found), and a key 'split' an order split between two sets (key otherSheetNotReady, split:true). */
function issues(sheetId, n, { keys, own = null, mates = [], trouble = [], names = NAMES, start = 4170250000, lid = 'L' } = {}) {
  const out = [];
  if (own) out.push({ step: own, key: { engraving: 'approvalsNeeded', backFiles: 'backFilesMissing', qr: 'qrMissing', nesting: 'layout' }[own], label: { engraving: 'Approvals needed', backFiles: 'Back files missing', qr: 'QR label missing', nesting: 'Layout not ready' }[own], open: { type: 'sheet', id: sheetId } });
  for (let i = 0; i < n; i++) {
    const pick = (keys || KEYS)[i % (keys || KEYS).length], split = pick === 'split', key = split ? 'otherSheetNotReady' : pick, id = String(start + i * 137), sheetLabel = key === 'otherSheetNotReady' ? 'SS Sheet 1' : null;
    out.push({ step: 'orders', key, orderId: id, orderLabel: 'Order ' + id, customer: names[i % names.length], listingId: lid + (i % 12), thumb: null, pieceCount: 2, ...(split ? { split: true, sets: ['Set 1', 'Set 2'] } : {}), pieces: [{ index: 2, key: id + '_b', label: 'Charm', sheetLabel, why: key, ...(split ? { split: true, setLabel: 'Set 2' } : {}) }], open: { type: 'order', id } });
  }
  for (const m of mates) out.push({ step: 'laser', key: 'waitsOnSheet', quiet: true, label: m.label, stepKey: 'engraving', stepLabel: m.stepLabel || 'Engraving', why: m.why || 'back engravings 7 of 25', counter: m.counter === undefined ? { done: 7, of: 25 } : m.counter, text: `${m.label} · back engravings 7 of 25`, sheetId: sheetId, sheetLabel: 'GF Sheet 1', open: { type: 'sheet', id: m.id } });
  for (const m of trouble) out.push({ step: 'laser', key: 'missingSheet', label: 'Sheet missing', sheetId, open: { type: 'sheet', id: m.id } });
  return out;
}

/* offline "Etsy photos": soft squares with a charm silhouette */
const PAL = [['#f3e6d3', '#c8a24e'], ['#e6edf2', '#8d95a0'], ['#f4e3dc', '#c08578'], ['#e7eddf', '#5f7a5b'], ['#efe9f4', '#8a78a8'], ['#f6eedc', '#a9823f']];
const SHAPE = ['M50 22c10-14 34-8 34 12 0 22-26 38-34 46-8-8-34-24-34-46 0-20 24-26 34-12z', 'M50 14l10 26 28 2-22 18 8 28-24-16-24 16 8-28-22-18 28-2z', 'M50 16c18 0 30 14 30 32S66 84 50 84 20 66 20 48s12-32 30-32z', 'M50 12c14 22 30 36 30 54a30 30 0 0 1-60 0c0-18 16-32 30-54z'];
function photo(i) {
  const [bg, fg] = PAL[i % PAL.length], d = SHAPE[i % SHAPE.length];
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><rect width="100" height="100" fill="${bg}"/><circle cx="50" cy="12" r="5" fill="none" stroke="${fg}" stroke-width="2.5"/><path d="${d}" fill="${fg}" opacity=".92" transform="translate(0 4) scale(.96)"/><path d="${d}" fill="none" stroke="#fff" stroke-opacity=".5" stroke-width="2" transform="translate(5 9) scale(.9)"/></svg>`;
  return 'data:image/svg+xml;utf8,' + encodeURIComponent(svg);
}
/* a sheet picture in the style of the Library's own: hairline charms in blue and red on white */
function sheetPicture() {
  let s = '';
  for (let i = 0; i < 70; i++) { const x = (i * 53) % 480 + 14, y = Math.floor(i / 9) * 52 + 24, r = 9 + (i * 7) % 11, c = i % 5 === 0 ? '#2540e0' : i % 7 === 0 ? '#e0524a' : '#444'; s += `<circle cx="${x}" cy="${y}" r="${r}" fill="${i % 4 === 0 ? c : 'none'}" fill-opacity=".55" stroke="${c}" stroke-width="1.2"/>`; }
  return 'data:image/svg+xml;utf8,' + encodeURIComponent(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 513 434"><rect width="513" height="434" fill="#fff"/>${s}</svg>`);
}

/* the page: the app's shell and CSS around the real Library code */
function shell() {
  let html = read('charm-nest-1.html');
  html = html.replace(/<script[\s\S]*?<\/script>/g, '').replace(/<link[^>]*>/g, '');
  html = html.replace('class="lib hidden" id="libView"', 'class="lib" id="libView"').replace('<div class="sheets" id="sheets">', '<div class="sheets hidden" id="sheets">');
  return html;
}
const STUBS = `
window.__calls = []; window.__feed = {}; window.__photos = new Map();
window.matchMedia = window.matchMedia || (() => ({ matches: false }));
Object.assign(window, {
  S: { mode: 'library', library: { rows: [], kind: 'sets', metal: 'all' }, cloud: { ok: true } }, api: async () => ({ sheets: window.__sheets || [], sets: window.__sets || [] }), allSheets: () => window.__sheets || [],   /* (the Library's live read answers with these: a read that found nothing would archive every record) */
  Orders: { rows: () => window.__rows || [] }, Engrave: { items: () => new Map(), backsMarkup: () => '' },
  esc: x => String(x ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c])),
  el: (tag, cls) => { const e = document.createElement(tag); e.className = cls; return e; }, Gate: { projectLibraryRecords: x => x }, RoseStock: {}, CODE: { gold: 'GF', silver: 'SS', rose: 'RG', gold10k: '10K', gold14k: '14K' },
  toast: () => null, CNEmployee: { name: () => 'Tester' }, cors: x => x, pvRatio: () => '', dayShort: x => x, metalOf: r => ({ label: r.metal }),
  sheetHead: r => '<div class="h"><span class="sw" style="--c:#c8a24e">GF</span><span class="nm">Sheet ' + r.sheetIndex + '</span><span class="tm">Oct 2</span></div>',
  openLibrarySheet: id => { window.__calls.push(['sheet', id]); return true; },
  openOrderFrom: (btn, rid, o) => { window.__calls.push(['order', rid, (o && o.poolId) || null]); return window.__openWindow ? window.__openWindow() : true; },
  setMode: m => { window.__calls.push(['mode', m]); },
  LibraryFlow: { approve: async () => { window.__calls.push(['approve']); return { ok: true, auto: [], needs: [], confirm: [], notes: [] }; } },   /* (so the sheets carry their Approve button; a press is only recorded) */
  ListMedia: { peek: id => window.__photos.get(id) || null, listing: id => new Promise(r => setTimeout(() => r(window.__photoOf ? window.__photoOf(id) : null), 120 + (parseInt(String(id).replace(/\\D/g, ''), 10) || 0) * 25)) }
});`;
async function openPage(browser, { width = 1440, height = 900, fake = true, errors = [], touch = false } = {}) {
  const context = await browser.newContext({ viewport: { width, height }, deviceScaleFactor: 2, ...(touch ? { hasTouch: true } : {}) });
  const page = await context.newPage();
  page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 3).join(' | ')); });
  await page.route(u => !/^about:|^data:/.test(u.href), r => r.abort());
  await page.setContent(shell(), { waitUntil: 'domcontentloaded' });
  await page.addScriptTag({ content: STUBS });
  for (const f of ['charm-nest-orders.js']) await page.addScriptTag({ content: read(f) });
  await page.evaluate(() => { window.O = window.CharmNestOrders; });
  for (const f of ['charm-nest-readiness.js', 'charm-nest-activity.js', 'charm-nest-motion.js']) await page.addScriptTag({ content: read(f) });
  const bridge = read('charm-nest-bridge.js');
  await page.addScriptTag({ content: bridge.slice(bridge.indexOf('const LaserReview ='), bridge.indexOf('/* ═══ 22 · Sets — one run')) });
  const a = bridge.indexOf('  function libraryGroups('), b = bridge.indexOf('  /** A set card whose completion', a), c = bridge.indexOf('  async function renderLibrary(body, opts)', b), e = bridge.indexOf('  return { releaseIssue', c);
  await page.addScriptTag({ content: 'window.Sets=(()=>{' + bridge.slice(a, b) + bridge.slice(c, e) + ';return {libraryCard};})();' });
  if (fake) await page.addScriptTag({ content: 'CharmNestReadiness.issues=(s)=>window.__feed[s.id||s.sheetId]||[];' });   // (the exact shape of issues(), set by the test: window.__feed[sheetId] = [...])
  await page.addScriptTag({ content: read('charm-nest-library-issues.js') });
  await page.addScriptTag({ content: read('charm-nest-rail-tip.js') });   // (the small card over a rail circle: it only answers the zoom engine's "dotzoom", so every other test is unchanged by it)
  return { page, context };
}
module.exports = { root, read, back, sheet, issues, photo, sheetPicture, shell, openPage, NAMES };
