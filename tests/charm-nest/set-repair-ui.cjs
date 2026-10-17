// SETREPAIR (Paul, 17 Oct 2026: "I want you to fix it once and for all."): what the owner SEES in the Library when a set breaks his rules.
// Real page (charm-nest-1.html, real modules) in headless Chromium over the local stand-in for the site (bridge-server.cjs: the real charmNestLibrary function over an
// in-memory Firestore). Nothing live is ever called. Fixture: the owner's picture on the cloud:
//   Set-1  committed (GF Sheet 1 + SS Sheet 1 sharing an order)                      -> no line, no button
//   Set-2  uncommitted, 14 sheets (GF 1-8, SS 1-4, RG 16, RG 29), chains A, B, C       -> ONE red line naming the rules and the sheets, ONE button "Rebuild this set correctly",
//                                                                                        Approve for laser cutting stays grey with the rules named
//   Set-5  uncommitted, GF + SS sharing an order: the minimum                        -> no line, no button
// The run that makes Set-2 is not open on this screen: pressing Rebuild says so in plain words and writes NOTHING (the safe refusal); the page does not scroll sideways at 390 px.
//   SHOTS=<dir> PW_DIR=<dir with playwright-core> CHROMIUM=<chrome> node tests/charm-nest/set-repair-ui.cjs
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start, Timestamp } = require('./bridge-server.cjs');
const SHOTS = process.env.SHOTS || '';
if (SHOTS) fs.mkdirSync(SHOTS, { recursive: true });
const SHEETS = 'Charm_Nest_Sheets', SETS = 'Charm_Nest_Sets', RUNS = 'Charm_Nest_Runs';
const fails = [], ok = [];
const check = (c, m) => { if (c) ok.push(m); else fails.push(m); };

function seed(st, blobUrl) {
  st.docs.clear();
  const day = '2026-10-17', now = Date.now(); let n = 0, o = 4200000000; const lines = {}, sheets = {};
  const mk = (id, metal, setId, setSeq, idx, extra = {}) => {
    n++;
    const own = String(3700000000 + n * 10), poolIds = [`${own}_1_1`], orders = [own];
    lines[`${own}_1`] = { orderId: own, state: 'written', quantity: 1, poolIds: [`${own}_1_1`], engraveCandidate: false };
    const p = `charmnest/sets/${day}/Set-${setSeq}/${id}/preview.png`;
    const rec = { id, metal, metalLabel: metal, day, setId: setId || null, setSeq: setSeq || null, sheetIndex: idx, page: idx, fileBase: `${metal.slice(0, 2).toUpperCase()}_${day}_Set-${setSeq}_Sheet-${idx}`, folder: `${metal}_${id}`, orders, poolIds, listings: [], placedCount: 1, charmCount: 1, density: .76, freePt2: 2000, stock: { wIn: 6, hIn: 5 },
      outputs: { preview: { path: p, url: blobUrl(p) }, ai: { path: `${id}.ai`, url: blobUrl(`${id}.ai`) } }, verification: { ok: true }, names: orders.join(' '), status: 'complete', updatedAt: Timestamp.fromMillis(now - n * 1000), createdAt: Timestamp.fromMillis(now - 86400000 + n), archived: false, runId: 'run-own', releaseFull: true, draft: false,
      backPool: [], engraving: {}, label: { files: [{ path: `${id}-qr.png`, url: blobUrl(p), payload: orders[0], orders }] }, ...extra };
    sheets[id] = rec; return rec;
  };
  // a multi-piece order shared by two sheets: one piece on each
  const share = (a, b) => { o += 7; const k = String(o); a.poolIds.push(`${k}_1_1`); b.poolIds.push(`${k}_1_2`); a.orders.push(k); b.orders.push(k); lines[`${k}_1`] = { orderId: k, state: 'written', quantity: 2, poolIds: [`${k}_1_1`, `${k}_1_2`], engraveCandidate: false }; };
  const S2 = 'set-own-2', gf = {}, ss = {};
  for (let i = 1; i <= 8; i++) gf[i] = mk('gf' + i, 'gold', S2, 2, i);
  for (let i = 1; i <= 4; i++) ss[i] = mk('ss' + i, 'silver', S2, 2, i);
  const rg16 = mk('rg16', 'rose', S2, 2, 16, { releaseFull: false, density: .58 }), rg29 = mk('rg29', 'rose', S2, 2, 29, { releaseFull: false, density: .29 });
  [[ss[4], gf[7]], [gf[7], rg29], [rg29, gf[6]], [gf[6], ss[3]], [gf[5], rg16], [rg16, gf[4]], [gf[4], ss[2]], [ss[2], gf[3]], [ss[1], gf[2]], [gf[2], gf[1]]].forEach(([a, b]) => share(a, b));
  const g0 = mk('s1-gf', 'gold', 'set-own-1', 1, 1, { runId: 'run-old' }), s0 = mk('s1-ss', 'silver', 'set-own-1', 1, 1, { runId: 'run-old' }); share(g0, s0);
  const g5 = mk('s5-gf', 'gold', 'set-own-5', 5, 1, { runId: 'run-own' }), s5 = mk('s5-ss', 'silver', 'set-own-5', 5, 1, { runId: 'run-own' }); share(g5, s5);
  for (const r of Object.values(sheets)) st.put(SHEETS, r.id, r);
  const setDoc = (setId, seq, ids, extra) => st.put(SETS, setId, { setId, seq, day, runId: 'run-own', group: 'dispatch', name: `Set-${seq}`, sheetIds: ids, materials: ['gold', 'silver'], orders: {}, labels: null, labelFiles: [], status: 'open', updatedAt: Timestamp.fromMillis(now), createdAt: Timestamp.fromMillis(now - 86400000), ...extra });
  setDoc('set-own-1', 1, ['s1-gf', 's1-ss'], { runId: 'run-old', status: 'complete', committedAt: now - 3 * 86400000, completedAt: now - 3 * 86400000, day: '2026-10-14' });
  setDoc(S2, 2, Object.values(sheets).filter(r => r.setId === S2).map(r => r.id), { sheetNos: { gold: 8, silver: 4, rose: 29 } });
  setDoc('set-own-5', 5, ['s5-gf', 's5-ss']);
  st.put(RUNS, 'run-own', { runId: 'run-own', status: 'running', lines });
}
const until = async (fn, ms = 20000, what = '') => { const t0 = Date.now(); for (;;) { let v; try { v = await fn(); } catch (_) { v = false; } if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await new Promise(r => setTimeout(r, 60)); } };

(async () => {
  const srv = await start({ receipts: [] }), { st, sorterOrigin } = srv;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
  try {
    for (const [width, height] of [[1280, 900], [390, 844]]) {
      const W = `${width}px`;
      seed(st, srv.blobUrl);
      const ctx = await browser.newContext({ viewport: { width, height }, deviceScaleFactor: 1 });
      await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
      await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
      await ctx.route(/charmNestLibrary/, r => { let b = null; try { b = r.request().postDataJSON(); } catch (_) { /* a get */ } return b && b.op === 'runList' ? r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ runs: [] }) }) : r.continue(); });
      await ctx.addInitScript(() => { try { if (!localStorage.getItem('cn.employee')) localStorage.setItem('cn.employee', 'Tester'); } catch (_) { /* about:blank */ } window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {}; });
      const page = await ctx.newPage(), errors = [];
      page.on('pageerror', e => errors.push('page: ' + e.message));
      page.on('console', m => { if (m.type() === 'error' && !/firebase stub|Failed to load resource|CORS policy|net::ERR/.test(m.text())) errors.push('console: ' + m.text().slice(0, 300)); });
      await page.goto(`${sorterOrigin}/charm-nest-1.html#library`);
      await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.LibraryDone && window.SetRepair, null, { timeout: 60000 });
      await page.waitForSelector('#libBody .setCard .libCard', { timeout: 30000 });
      await page.evaluate(() => document.getElementById('app').classList.add('railOff'));
      await until(() => page.evaluate(() => document.querySelectorAll('#libBody .setCard').length >= 3 && document.querySelectorAll('#libBody .libCard').length >= 18), 30000, 'the three set cards are drawn');
      await page.waitForTimeout(900);
      const cards = await page.evaluate(() => [...document.querySelectorAll('#libBody .setCard[data-laser-card="set"]')].map(c => ({
        setId: c._laserSet && c._laserSet.setId, title: (c.querySelector('[data-set-title]') || {}).textContent || '', sheets: c.querySelectorAll('.librarySheet').length,
        links: (c.querySelector('[data-set-links]') || {}).textContent || '',
        lines: [...c.querySelectorAll('[data-set-break]')].map(e => e.querySelector('[data-set-break-line]').textContent), buttons: [...c.querySelectorAll('[data-rebuild-set]')].map(b => b.textContent.trim()),
        approve: (() => { const b = c.querySelector(':scope > .approveBox[data-level="set"]'); return b ? { aria: b.querySelector('[data-approve-btn]').getAttribute('aria-disabled'), why: (b.querySelector('[data-approve-why]') || {}).textContent || '', label: b.querySelector('[data-approve-btn]').textContent.trim() } : null; })()
      })));
      const by = id => cards.find(c => c.setId === id) || {};
      check(cards.length === 3, `${W}: three set cards on screen (${cards.map(c => c.title).join(' | ')})`);
      const two = by('set-own-2');
      check(two.sheets === 14, `${W}: Set 2 holds the owner's 14 sheets: ${two.sheets}`);
      check(two.lines.length === 1 && two.buttons.length === 1, `${W}: Set 2 shows ONE red line and ONE button: ${two.lines.length} line(s), ${two.buttons.length} button(s)`);
      check(two.buttons[0] === 'Rebuild this set correctly', `${W}: the button reads "Rebuild this set correctly": ${two.buttons[0]}`);
      check(/^Set 2 breaks your rules: it is bigger than the minimum \(GF Sheet 3, GF Sheet 4, GF Sheet 5, GF Sheet 6 and 7 more are not needed for it to be valid\); Rose Gold sheets RG Sheet 16 and RG Sheet 29 are not cut yet \(no green line\)$/.test(two.lines[0] || ''), `${W}: the line names the rules and the sheets: ${two.lines[0]}`);
      check(two.approve && two.approve.aria === 'true' && two.approve.label === 'Approve for laser cutting', `${W}: Approve for laser cutting stays grey on Set 2: ${JSON.stringify(two.approve)}`);
      check(two.approve && /breaks your rules: bigger than the minimum; RG Sheet 16 and RG Sheet 29 not cut yet/.test(two.approve.why), `${W}: and says the rules under the button: ${two.approve && two.approve.why}`);
      check(/sheets that share an order stay in one set\.$/.test(two.links) && !/no order with the other sheets/.test(two.links), `${W}: the older "share no order" sentence is gone, the links stay: ${two.links.slice(-80)}`);
      for (const id of ['set-own-1', 'set-own-5']) check(by(id).lines.length === 0 && by(id).buttons.length === 0, `${W}: ${by(id).title} (right, or committed) shows no line and no button: ${JSON.stringify([by(id).lines, by(id).buttons])}`);
      check(!by('set-own-1').approve || by('set-own-1').approve.why === '' || !/breaks your rules/.test(by('set-own-1').approve.why), `${W}: the committed Set 1 is never blocked by the rules`);
      check(!by('set-own-5').approve || !/breaks your rules/.test(by('set-own-5').approve.why), `${W}: the right Set 5 is not blocked by the rules`);
      const sw = await page.evaluate(() => [document.documentElement.scrollWidth, innerWidth]);
      check(sw[0] <= sw[1] + 1, `${W}: the page does not scroll sideways (${sw[0]} > ${sw[1]})`);
      if (SHOTS) { const el = await page.$('#libBody .setCard[data-laser-card="set"] [data-set-break]'); if (el) { await el.scrollIntoViewIfNeeded(); await page.screenshot({ path: path.join(SHOTS, `set-repair-w${width}.png`) }); } }
      // the press, with the run that makes Set 2 not open on this screen: a plain refusal, and not one write
      const calls0 = st.calls.length, docs0 = JSON.stringify([...st.docs.entries()].filter(([k]) => /^Charm_Nest_(Sheets|Sets)\//.test(k)));
      await page.click('[data-rebuild-set="set-own-2"]');
      const say = await until(() => page.evaluate(() => { const e = document.querySelector('[data-set-break-say]'); return e && !e.hidden ? e.textContent : ''; }), 20000, 'the refusal is said under the line');
      check(/^Set 2 is made by a run that is not open on this screen: open that run here, then rebuild its set$/.test(say), `${W}: the refusal is plain: ${say}`);
      check(await page.evaluate(() => !document.querySelector('dialog.setRepairDlg')), `${W}: no window opened for a refused press`);
      const writes = st.calls.slice(calls0).filter(c => /^(putSheet|setUpdate|setAllocate|flowApply|poolUpdate|deleteSheet|laserDone|runPut)$/.test(c.op || ''));
      check(writes.length === 0, `${W}: a refused press writes nothing: ${writes.map(c => c.op).join(',')}`);
      check(JSON.stringify([...st.docs.entries()].filter(([k]) => /^Charm_Nest_(Sheets|Sets)\//.test(k))) === docs0, `${W}: not one sheet or set record changed`);
      check(errors.length === 0, `${W}: no page error: ${errors.slice(0, 3).join(' | ')}`);
      await ctx.close();
    }
  } finally { await browser.close(); srv.close(); }
  for (const m of ok) console.log('ok   ' + m);
  for (const m of fails) console.log('FAIL ' + m);
  if (fails.length) { console.log(`\n${fails.length} FAILED, ${ok.length} passed`); process.exit(1); }
  console.log(`\nPASS: set-repair-ui: ${ok.length} checks: the Library says in ONE red line which of the owner's rules Set 2 breaks and offers ONE button; Approve for laser cutting stays grey with the rules named; a right set and a committed set show nothing; a refused press says why and writes nothing`);
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
