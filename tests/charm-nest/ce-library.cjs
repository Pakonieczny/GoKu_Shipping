// THE DESIGN LIBRARY AT THE EDITOR'S RIGHT HAND (charm-nest-ce-library.js; Paul, 28 Oct 2026: "this repository must be searchable, and you must integrated on the right
// hand side in the same way we do it in the website, charm editor ... we've spent a lot of time making it really efficient and fast and see if you can improve it").
// One real Chromium page: the charm editor harness (fixtures/charm-editor-harness.html), the page's picture store (charm-nest-thumbs.js) and the library, over STAND-INS
// of the design library (3,000 designs) and the charm library (500 masters) shaped as CharmNestDesigns and CharmNestCeMasters answer. No server, no network.
//   PW_DIR=/opt/node-tools/node_modules CHROMIUM=/opt/pw-browsers/chromium-1243/chrome-linux64/chrome node tests/charm-nest/ce-library.cjs     (SHOTS=<dir> keeps screenshots)
// 1 the panel opens beside the charm, newest design first, with ~30 elements for 3,000 designs
// 2 search as you type: every keystroke's work (the input's handlers and the layout after them) under 16 ms; name, order number (#…), person, two words; the answers are
//   the same as a plain scan of the data; the filters (Mine, Order, No order, Today, Week) match the same scan
// 3 scrolling: wheel and scrollbar jumps from the top to the end of 3,000 designs, every scroll's work under 30 ms, the elements stay ~30; a fast pass asks for no pictures;
//   once it stops, the pictures on screen arrive
// 4 drag a design onto the charm: one group, centred where it was let go, its geometry exactly the design's, its cut line left out (the charm has one), one undo step; ⊕ puts
//   one in the middle
// 5 open a design with unsaved changes: Cancel stays; Keep and open hands the old session's result back and opens the design; Leave them hands back null
// 6 a design's versions: the list, newest first; an older one opened
// 7 Recent; the charm library (search, open, drag a master); the list view; the phone width; no key reached the page; no error
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); process.exit(0); }
const SHOTS = process.env.SHOTS || ''; if (SHOTS) fs.mkdirSync(SHOTS, { recursive: true });
const HARNESS = 'file://' + path.join(__dirname, 'fixtures', 'charm-editor-harness.html');
const results = [], failures = [];
const pass = m => { results.push(m); console.log('  ok   ' + m); };
const fail = (m, e) => { failures.push(m + ' :: ' + String(e && e.message || e).split('\n')[0].slice(0, 400)); console.log('  FAIL ' + m + '\n         ' + String(e && e.stack || e).split('\n').slice(0, 12).join('\n         ')); };
async function step(name, fn) { try { await fn(); pass(name); } catch (e) { fail(name, e); } }
const settle = page => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
const st = page => page.evaluate(() => window.CharmNestCeLibrary.probe.state());
const wait = ms => new Promise(r => setTimeout(r, ms));
const stats = a => { const s = a.slice().sort((x, y) => x - y); return { n: s.length, max: +(s[s.length - 1] || 0).toFixed(2), med: +(s[Math.floor(s.length / 2)] || 0).toFixed(2), p95: +(s[Math.floor(s.length * 0.95)] || 0).toFixed(2) }; };
async function shot(page, name) { if (SHOTS) await page.screenshot({ path: path.join(SHOTS, name) }); }

/* the stand-ins, in the page: CharmNestDesigns (rows, list, get with history, put, onChange) and CharmNestCeMasters (search, onChange, open, openDesign, faces) */
const STANDINS = `(function () {
  const PT = 72 / 25.4, N = 3000, NOW = Date.now(), H = 3600e3;
  const people = ['Paul', 'Maria', 'Jon', 'Ana', 'Kim', 'Lee'];
  const words = ['Heart', 'Star', 'Moon', 'Calendar', 'Paw', 'Name tag', 'Initial', 'Bar', 'Map', 'Cross', 'Leaf', 'Tree'];
  const pic = t => 'data:image/svg+xml,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 40 40"><rect width="40" height="40" fill="#f4ecd9"/><circle cx="20" cy="20" r="14" fill="none" stroke="#a8823a"/><text x="20" y="24" font-size="9" text-anchor="middle" font-family="sans-serif">' + t + '</text></svg>');
  const rows = [];
  for (let i = 0; i < N; i++) {
    const custom = i % 5 === 0;
    rows.push({ id: 'dz' + String(i).padStart(5, '0'), name: words[i % words.length] + ' ' + i, version: 1 + (i % 4), thumbUrl: pic(i), widthMm: 20, heightMm: 24,
      orderId: custom ? (i % 10 === 0 ? 'CUSTOM-20261028-' + i : null) : String(4200000000 + i * 7), customName: custom ? 'Fix ' + i : null, by: people[i % people.length],
      at: NOW - i * H * 0.9, source: i % 3 ? { kind: 'master', sku: 'SKU' + (i % 200) } : { kind: 'new' }, hash: 'h' + i });
  }
  const P = (x, y) => [+(x * PT).toFixed(4), +(y * PT).toFixed(4)];
  const K = 0.5522847498;
  const circle = (cx, cy, r) => { const k = r * K; return [[['m', P(cx + r, cy)], ['c', P(cx + r, cy + k), P(cx + k, cy + r), P(cx, cy + r)], ['c', P(cx - k, cy + r), P(cx - r, cy + k), P(cx - r, cy)], ['c', P(cx - r, cy - k), P(cx - k, cy - r), P(cx, cy - r)], ['c', P(cx + k, cy - r), P(cx + r, cy - k), P(cx + r, cy)], ['h']]]; };
  const rect = (x0, y0, x1, y1) => [[['m', P(x0, y0)], ['l', P(x1, y0)], ['l', P(x1, y1)], ['l', P(x0, y1)], ['h']]];
  /** a design's drawing: its cut circle, a filled square, a line ring and its words (the version in them, so two versions differ) */
  function facesOf(id, v) {
    const n = +id.slice(2);
    return { front: { face: 'front', unitPt: true, frame: { x0: 0, y0: 0, wPt: 20 * PT, hPt: 24 * PT }, items: [
      { id: id + '-cut', kind: 'path', role: 'cut', name: 'Cut', stroke: true, fill: false, lwPt: 0.25, subpaths: circle(10, 12, 9.5) },
      { id: id + '-sq', kind: 'path', role: 'hatch', name: 'Square', stroke: false, fill: true, subpaths: rect(6 + (n % 3) * 0.5, 8, 11, 13 + v * 0.25) },
      { id: id + '-ring', kind: 'path', role: 'line', name: 'Ring', stroke: true, fill: false, lwPt: 0.25, subpaths: circle(13, 15, 2.5) },
      { id: id + '-t', kind: 'text', role: 'hatch', name: 'Words', text: 'V' + v, fontKey: 'sans', sizePt: 8, align: 'center', tx: 10 * PT, ty: 19 * PT, sx: 1, sy: 1, rot: 0 }
    ] } };
  }
  const calls = window.__calls = { get: [], put: [], openDesign: [], open: [], faces: [], list: 0 };
  const listeners = new Set();
  window.CharmNestDesigns = {
    rows: () => rows.slice(),
    list: () => { calls.list++; return Promise.resolve(rows.slice()); },
    get: (id, version, opts) => {
      calls.get.push({ id, version: version || 0, history: !!(opts && opts.history) });
      const r = rows.find(x => x.id === id); if (!r) return Promise.resolve(null);
      const v = version || r.version;
      const d = Object.assign({}, r, { version: v, faces: facesOf(id, v) });
      if (opts && opts.history) d.history = Array.from({ length: r.version }, (_, k) => ({ version: r.version - k, by: people[(k + 1) % 6], at: r.at - k * 86400e3, thumbUrl: pic(id + 'v' + (r.version - k)) }));
      return new Promise(res => setTimeout(() => res(d), 5));
    },
    put: d => {
      calls.put.push(d);
      const r = d.id && rows.find(x => x.id === d.id), v = r ? r.version + 1 : 1, id = d.id || 'dnew' + calls.put.length;
      const row = { id, name: d.name, version: v, thumbUrl: pic('new'), widthMm: d.widthMm, heightMm: d.heightMm, orderId: d.orderId || null, customName: d.customName || null, by: d.by, at: Date.now(), source: d.source };
      if (r) Object.assign(r, row); else rows.unshift(row);
      for (const fn of listeners) fn({ kind: 'put', row });
      return Promise.resolve({ id, version: v, at: row.at, hash: 'x', row });
    },
    onChange: fn => { listeners.add(fn); return () => listeners.delete(fn); }
  };
  const masters = Array.from({ length: 500 }, (_, i) => ({ sku: 'SKU' + i, name: 'SKU' + i, thumbUrl: pic('M' + i), widthMm: 18, heightMm: 18, file: 'master-' + i + '.ai' }));
  const mfront = sku => ({ face: 'front', unitPt: true, frame: { x0: 0, y0: 0, wPt: 18 * PT, hPt: 18 * PT }, items: [
    { id: sku + '-cut', kind: 'path', role: 'cut', name: 'Cut', stroke: true, fill: false, lwPt: 0.25, subpaths: circle(9, 9, 8.5) },
    { id: sku + '-dot', kind: 'path', role: 'hatch', name: 'Dot', stroke: false, fill: true, subpaths: circle(9, 9, 2) } ] });
  // (as charm-nest-ce-masters.js: the editor opened on the design or master; its Save a version of it)
  const edit = (title, faces, design) => CharmEditor.open({ title, subtitle: 'design mode', faces, fonts: window.fixture.fonts, startFace: 'front', lockRoles: [] })
    .then(res => { calls.lastEdit = { title, res }; if (res) return window.CharmNestDesigns.put(Object.assign({}, design, { faces: res.faces, by: 'Paul' })); return null; });
  window.CharmNestCeMasters = {
    search: (q, o) => (q ? masters.filter(m => m.sku.toLowerCase().includes(String(q).toLowerCase())) : masters).slice(0, (o && o.limit) || 60).map(m => Object.assign({}, m)),
    onChange: () => () => {},
    open: sku => { calls.open.push(sku); return edit(sku, { front: mfront(sku) }, { name: sku, source: { kind: 'master', sku } }); },
    openDesign: d => { calls.openDesign.push({ id: d.id, version: d.version }); return edit(d.name, d.faces, { id: d.id, name: d.name, source: { kind: 'design' } }); },
    faces: sku => { calls.faces.push(sku); return Promise.resolve({ front: mfront(sku) }); },
    current: () => null
  };
  window.CNEmployee = { name: () => 'Paul' };
  window.__rows = rows;
  /* the plain answer to compare with: every word in a design's own words, the filters as Paul would read them */
  window.__scan = (q, f) => {
    f = f || {};
    const toks = String(q || '').toLowerCase().split(/\\s+/).map(t => t.replace(/^#+/, '')).filter(Boolean);
    const d0 = new Date(); d0.setHours(0, 0, 0, 0); const w0 = new Date(d0); w0.setDate(w0.getDate() - ((w0.getDay() + 6) % 7));
    return rows.filter(r => {
      const words = [r.name, r.customName, r.source && r.source.sku, r.orderId, r.by, 'v' + r.version, r.id].filter(Boolean).join(' ').toLowerCase();
      if (!toks.every(t => words.includes(t))) return false;
      const custom = !r.orderId || /^CUSTOM-/.test(r.orderId);
      if (f.mine && r.by !== 'Paul') return false;
      if (f.order === 'with' && custom) return false; if (f.order === 'none' && !custom) return false;
      if (f.when === 'today' && r.at < d0.getTime()) return false; if (f.when === 'week' && r.at < w0.getTime()) return false;
      return true;
    }).length;
  };
})();`;

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1243/chrome-linux64/chrome', args: ['--no-sandbox'] });
  const newPage = async (w, h) => {
    const ctx = await browser.newContext({ viewport: { width: w, height: h }, reducedMotion: 'reduce' });
    await ctx.route(u => !u.href.startsWith('file://') && !u.href.startsWith('data:'), r => r.abort());   // nothing leaves the machine
    const page = await ctx.newPage();
    page.__errs = [];
    page.on('pageerror', e => page.__errs.push('pageerror: ' + e.message));
    page.on('console', m => { if (m.type() === 'error') page.__errs.push('console: ' + m.text()); });
    await page.goto(HARNESS);
    await page.addScriptTag({ path: path.join(root, 'charm-nest-thumbs.js') });
    await page.addScriptTag({ content: STANDINS });
    await page.addScriptTag({ path: path.join(root, 'charm-nest-ce-library.js') });
    return page;
  };
  const openIt = async page => {
    await page.evaluate(() => { try { localStorage.removeItem('ce.xpanel'); } catch (_) {} window.openEditor(); });
    await page.waitForSelector('.ce-root .ce-xpanel .cel-cell:not(.is-free)');
    await settle(page);
  };
  const cellAt = (page, i) => page.evaluate(i => { const c = [...document.querySelectorAll('.cel-cell:not(.is-free)')].find(x => +x.dataset.i === i); if (!c) return null; const r = c.getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height * 0.35, key: c._key }; }, i);
  const stageMid = page => page.evaluate(() => { const r = document.querySelector('.ce-stage').getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height / 2 }; });

  /* ════ the main page, 1280 × 800 ════ */
  const page = await newPage(1280, 800);
  try {
    await openIt(page);
    await shot(page, 'lib-1-open.png');

    await step('1 · the panel opens beside the charm: Designs 3,000 newest first, Charm library 500, ~30 elements', async () => {
      const s = await st(page);
      assert.equal(s.tab, 'designs'); assert.equal(s.total, 3000); assert.equal(s.n, 3000);
      assert.equal(s.keys[0], 'd:dz00000'); assert.equal(s.keys[1], 'd:dz00001');
      const n = await page.evaluate(() => ({ designs: document.querySelector('[data-n="designs"]').textContent, masters: document.querySelector('[data-n="masters"]').textContent, cells: document.querySelectorAll('.cel-cell').length,
        inPanel: (() => { const p = document.querySelector('.ce-xpanel').getBoundingClientRect(), s = document.querySelector('.ce-stage').getBoundingClientRect(); return p.left >= s.right - 1; })() }));
      assert.equal(n.designs, '3000'); assert.equal(n.masters, '500');
      assert.ok(n.cells <= 40, 'elements in the list: ' + n.cells);
      assert.ok(n.inPanel, 'the panel is on the right of the work area');
      console.log(`         ${n.cells} elements for 3,000 designs (${s.lay.cols} to a row, rows of ${s.lay.rowH} px)`);
    });

    // measure, in the page, the work of every input and every scroll: from the window's capture (before any handler) to after the last handler, with the layout forced
    await page.evaluate(() => {
      const W = window.__work = { input: [], scroll: [] };
      let t0 = 0, s0 = 0;
      const lay = () => { const c = document.querySelector('.cel-cell:not(.is-free)'); if (c) c.getBoundingClientRect(); return document.querySelector('.cel-list').scrollHeight; };
      window.addEventListener('input', () => { t0 = performance.now(); }, true);
      window.addEventListener('input', () => { lay(); W.input.push(performance.now() - t0); });
      window.addEventListener('scroll', e => { if (e.target && e.target.classList && e.target.classList.contains('cel-list')) s0 = performance.now(); }, true);
      document.querySelector('.cel-list').addEventListener('scroll', () => { lay(); W.scroll.push(performance.now() - s0); }, { passive: true });
    });

    await step('2 · search as you type: every keystroke under 16 ms of work; the answers match a plain scan', async () => {
      await page.click('.cel-search input');
      const tries = [['heart 42', 'two words'], ['#4200000035', 'an order number'], ['maria', 'a person'], ['custom-20261028', 'an orderless piece'], ['sku17 v2', 'a SKU and a version']];
      const all = [];
      for (const [q, what] of tries) {
        await page.evaluate(() => { const i = document.querySelector('.cel-search input'); i.value = ''; i.dispatchEvent(new Event('input', { bubbles: true })); window.__work.input.length = 0; });
        await page.keyboard.type(q, { delay: 25 });
        await settle(page);
        const s = await st(page), want = await page.evaluate(q => window.__scan(q), q), w = await page.evaluate(() => window.__work.input.slice());
        assert.equal(s.n, want, `"${q}": ${s.n} shown, the scan finds ${want}`);
        assert.ok(want > 0, `"${q}" finds something`);
        all.push(...w);
        console.log(`         "${q}" (${what}): ${s.n} found · per keystroke ${stats(w).med} ms median, ${stats(w).max} ms most`);
      }
      // backspace all the way (a query that loses letters searches everything again)
      await page.evaluate(() => { window.__work.input.length = 0; });
      for (let i = 0; i < 8; i++) await page.keyboard.press('Backspace');
      await settle(page);
      const back = await page.evaluate(() => window.__work.input.slice()); all.push(...back);
      const sb = await st(page), wantB = await page.evaluate(q => window.__scan(q), sb.q);
      assert.equal(sb.n, wantB);
      const S = stats(all);
      console.log(`         ${S.n} keystrokes: median ${S.med} ms, 95% ${S.p95} ms, most ${S.max} ms (budget 16 ms); backspacing ${stats(back).max} ms most`);
      assert.ok(S.max < 16, 'a keystroke took ' + S.max + ' ms');
      assert.equal(await page.evaluate(() => document.querySelector('.cel-search input') === document.activeElement), true, 'the search keeps the keys');
      assert.equal(await page.evaluate(() => window.__pageKeys), 0, 'no key reached the page under the editor');
    });

    await step('2 · the filters: Mine, Order, No order, Today, Week match a plain scan (and combine)', async () => {
      await page.evaluate(() => { const i = document.querySelector('.cel-search input'); i.value = ''; i.dispatchEvent(new Event('input', { bubbles: true })); });
      const chip = (k, v) => page.click(`.cel-chip[data-k="${k}"]${v ? `[data-v="${v}"]` : ''}`);
      const check = async (f, label) => { await settle(page); const s = await st(page), want = await page.evaluate(f => window.__scan('', f), f); assert.equal(s.n, want, `${label}: ${s.n} shown, the scan finds ${want}`); return s.n; };
      await chip('mine'); const mine = await check({ mine: true }, 'Mine');
      await chip('order', 'with'); const mo = await check({ mine: true, order: 'with' }, 'Mine + Order');
      await chip('order', 'none'); const mn = await check({ mine: true, order: 'none' }, 'Mine + No order');
      await chip('mine'); await chip('order', 'none');
      await chip('when', 'today'); const today = await check({ when: 'today' }, 'Today');
      await chip('when', 'week'); const week = await check({ when: 'week' }, 'Week');
      await page.click('.cel-search input'); await page.keyboard.type('heart');
      const hw = await check({ when: 'week' }, 'Week + "heart"');
      await page.evaluate(() => { const i = document.querySelector('.cel-search input'); i.value = ''; i.dispatchEvent(new Event('input', { bubbles: true })); });
      await chip('when', 'week'); await check({}, 'no filter');
      console.log(`         Mine ${mine} · Mine+Order ${mo} · Mine+No order ${mn} · Today ${today} · Week ${week} · Week+"heart" ${hw}`);
      assert.equal(mine, 500);
    });

    await step('3 · scrolling 3,000 designs: every scroll under 30 ms of work, ~30 elements; a fast pass asks for no picture; the pictures on screen arrive once it stops', async () => {
      await page.evaluate(() => { document.querySelector('.cel-list').scrollTop = 0; window.__work.scroll.length = 0; });
      await settle(page); await wait(400);
      const asked0 = (await page.evaluate(() => window.CharmNestCeLibrary.probe.pics())).asked;
      // the wheel, a notch at a time, quickly
      const L = await page.evaluate(() => { const r = document.querySelector('.cel-list').getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height / 2 }; });
      await page.mouse.move(L.x, L.y);
      const frames = await page.evaluate(() => { window.__gaps = []; let last = performance.now(), on = true; const f = t => { window.__gaps.push(t - last); last = t; if (on) requestAnimationFrame(f); }; requestAnimationFrame(f); window.__stopGaps = () => { on = false; }; return true; });
      for (let i = 0; i < 60; i++) await page.mouse.wheel(0, 360);
      await settle(page);
      // the scrollbar dragged: big jumps, one a frame, all the way to the end
      await page.evaluate(async () => { const l = document.querySelector('.cel-list'); for (let i = 0; i < 40 && l.scrollTop + l.clientHeight < l.scrollHeight - 2; i++) { l.scrollTop += 11000; await new Promise(r => requestAnimationFrame(r)); } l.scrollTop = l.scrollHeight; await new Promise(r => requestAnimationFrame(r)); });
      await settle(page);
      const passed = await page.evaluate(() => window.CharmNestCeLibrary.probe.pics().asked);
      const gaps = await page.evaluate(() => { window.__stopGaps(); return window.__gaps.slice(2); });
      const w = await page.evaluate(() => window.__work.scroll.slice()), s = await st(page);
      const S = stats(w);
      console.log(`         ${S.n} scrolls: median ${S.med} ms, 95% ${S.p95} ms, most ${S.max} ms of work (budget 30 ms) · ${s.cells} elements · rows ${s.first}–${s.last} of ${s.n} at the end`);
      console.log(`         frames while scrolling (the machine's own pace, not a budget): median ${stats(gaps).med} ms, most ${stats(gaps).max} ms`);
      assert.ok(S.n >= 50, 'scrolls measured: ' + S.n);
      assert.ok(S.max < 30, 'a scroll took ' + S.max + ' ms');
      assert.ok(s.cells <= 48, 'elements: ' + s.cells);
      assert.equal(s.last, 2999, 'the end of the list is reached');
      // pictures: the fast pass asked for (almost) none; the screen at the end gets its own
      const during = passed - asked0;
      await page.waitForFunction(() => [...document.querySelectorAll('.cel-cell:not(.is-free)')].filter(c => { const r = c.getBoundingClientRect(), l = document.querySelector('.cel-list').getBoundingClientRect(); return r.bottom > l.top && r.top < l.bottom; }).every(c => c.classList.contains('is-pic')), null, { timeout: 5000 });
      const after = (await page.evaluate(() => window.CharmNestCeLibrary.probe.pics())).asked - asked0;
      console.log(`         pictures asked for: ${during} during the pass over ${s.last + 1} designs, ${after} in all once it stopped`);
      assert.ok(during <= 24, 'pictures asked for while passing: ' + during);
      await shot(page, 'lib-2-end.png');
    });

    let undo0 = 0;
    await step('4 · drag a design onto the charm: one group where it was let go, the design\'s exact geometry, its cut line left out, one undo step', async () => {
      await page.evaluate(() => { document.querySelector('.cel-list').scrollTop = 0; });
      await settle(page); await wait(150);
      undo0 = await page.evaluate(() => CharmEditor.current.history().undo);
      const n0 = await page.evaluate(() => CharmEditor.current.items().length);
      const a = await cellAt(page, 0), b = await stageMid(page);
      const drop = { x: b.x - 60, y: b.y + 40 };
      await page.mouse.move(a.x, a.y); await page.mouse.down();
      await page.mouse.move(a.x - 20, a.y, { steps: 3 });
      const ghost = await page.evaluate(() => !!document.querySelector('.cel-ghost'));
      await page.mouse.move(drop.x, drop.y, { steps: 12 });
      const lit = await page.evaluate(() => document.querySelector('.ce-stage').classList.contains('cel-over') && document.querySelector('.cel-ghost').classList.contains('is-over'));
      await page.mouse.up();
      await page.waitForFunction(n => CharmEditor.current.items().length === n + 1, n0, { timeout: 3000 });
      await settle(page);
      assert.ok(ghost, 'a ghost follows the hand'); assert.ok(lit, 'the work area lights up under it');
      const r = await page.evaluate(d => {
        const C = CharmEditor.current, its = C.items(), g = its[its.length - 1], bx = C.box(g.id), at = C.ptOf(d.x, d.y);
        return { g: { kind: g.kind, name: g.name, n: g.children.length, roles: g.children.map(c => c.role), sx: g.sx, sy: g.sy, rot: g.rot, design: g.design, sq: g.children.find(c => c.name === 'Square').subpaths },
          c: [(bx[0] + bx[2]) / 2, (bx[1] + bx[3]) / 2], at, sel: C.selection(), undo: C.history().undo };
      }, drop);
      const want = await page.evaluate(() => window.CharmNestDesigns.get('dz00000').then(d => d.faces.front.items.find(i => i.name === 'Square').subpaths));
      assert.equal(r.g.kind, 'group'); assert.equal(r.g.name, 'Heart 0 · v1');
      assert.equal(r.g.n, 3, 'square, ring and words (the cut line left out)'); assert.ok(!r.g.roles.includes('cut'));
      assert.deepEqual(r.g.sq, want, 'the geometry is the design\'s own, not redrawn');
      assert.equal(r.g.sx, 1); assert.equal(r.g.sy, 1); assert.equal(r.g.rot, 0);
      assert.deepEqual(r.g.design, { id: 'dz00000', version: 1 });
      assert.ok(Math.abs(r.c[0] - r.at[0]) < 0.5 && Math.abs(r.c[1] - r.at[1]) < 0.5, `centred where it was let go: ${r.c.map(v => v.toFixed(2))} vs ${r.at.map(v => v.toFixed(2))}`);
      assert.equal(r.undo, undo0 + 1, 'one undo step');
      assert.equal(r.sel.length, 1, 'the group is selected');
      const said = await page.evaluate(() => document.querySelector('.ce-say') ? document.querySelector('.ce-say').textContent : '');
      assert.match(said, /cut line was left out/);
      console.log(`         placed "${r.g.name}" (${r.g.n} parts) at ${r.c.map(v => v.toFixed(2)).join(', ')} pt; said: "${said}"`);
      await shot(page, 'lib-3-placed.png');
      // one undo takes it away, one redo puts it back
      await page.evaluate(() => document.querySelector('.ce-win').focus());
      await page.keyboard.press('Control+z'); await settle(page);
      assert.equal(await page.evaluate(() => CharmEditor.current.items().length), n0, 'undone in one step');
      await page.keyboard.press('Control+Shift+z'); await settle(page);
      assert.equal(await page.evaluate(() => CharmEditor.current.items().length), n0 + 1, 'and back');
    });

    await step('4 · ⊕ puts a design in the middle of the charm', async () => {
      const n0 = await page.evaluate(() => CharmEditor.current.items().length);
      const c = await cellAt(page, 1);
      await page.mouse.move(c.x, c.y); await settle(page);
      await page.evaluate(() => { const c = [...document.querySelectorAll('.cel-cell:not(.is-free)')].find(x => +x.dataset.i === 1); c.querySelector('.cel-b--place').click(); });
      await page.waitForFunction(n => CharmEditor.current.items().length === n + 1, n0, { timeout: 3000 });
      const r = await page.evaluate(() => { const C = CharmEditor.current, its = C.items(), g = its[its.length - 1], b = C.box(g.id), f = C.frame(); return { name: g.name, c: [(b[0] + b[2]) / 2, (b[1] + b[3]) / 2], m: [f.x0 + f.wPt / 2, f.y0 + f.hPt / 2] }; });
      assert.equal(r.name, 'Star 1 · v2');
      assert.ok(Math.abs(r.c[0] - r.m[0]) < 0.5 && Math.abs(r.c[1] - r.m[1]) < 0.5, 'in the middle');
    });

    await step('5 · open a design with unsaved changes: Cancel stays, nothing opened', async () => {
      assert.equal(await page.evaluate(() => CharmEditor.current.dirty()), true);
      const c = await cellAt(page, 2);
      await page.mouse.click(c.x, c.y);
      await page.waitForSelector('.cel-ask3');
      const words = await page.evaluate(() => document.querySelector('.cel-ask3').textContent);
      assert.match(words, /Keep your changes\?/); assert.match(words, /Calendar 86188/); assert.match(words, /Moon 2/);
      await page.click('.cel-ask3 [data-k="cancel"]'); await settle(page);
      assert.equal(await page.evaluate(() => !!document.querySelector('.cel-ask3')), false);
      assert.equal(await page.evaluate(() => document.querySelector('.ce-id h2').textContent), 'Calendar 86188');
      assert.equal(await page.evaluate(() => window.__calls.openDesign.length), 0);
      // Escape on the question is Cancel too
      await page.mouse.click(c.x, c.y); await page.waitForSelector('.cel-ask3');
      await page.keyboard.press('Escape'); await settle(page);
      assert.equal(await page.evaluate(() => !!document.querySelector('.cel-ask3')), false);
      assert.equal(await page.evaluate(() => !!CharmEditor.current && window.__closed === false), true, 'the editor is still open on the charm');
    });

    await step('5 · Keep and open: the charm\'s changes are handed back as Save hands them, and the design opens in the editor', async () => {
      const c = await cellAt(page, 2);
      await page.mouse.click(c.x, c.y);
      await page.waitForSelector('.cel-ask3');
      await shot(page, 'lib-4-ask.png');
      await page.click('.cel-ask3 [data-k="keep"]');
      await page.waitForFunction(() => window.__closed === true && CharmEditor.current && document.querySelector('.ce-id h2').textContent === 'Moon 2', null, { timeout: 4000 });
      await page.waitForSelector('.ce-root .ce-xpanel .cel-cell:not(.is-free)');
      const r = await page.evaluate(() => ({ res: window.__result && window.__result.changes.front.added.map(a => a.name), od: window.__calls.openDesign.slice(), items: CharmEditor.current.items().map(i => i.name), cur: document.querySelector('.cel-cell.is-cur') ? 1 : 0, s: window.CharmNestCeLibrary.probe.state() }));
      assert.deepEqual(r.res, ['Heart 0 · v1', 'Star 1 · v2'], 'the old session saved with the two placed designs');
      assert.deepEqual(r.od, [{ id: 'dz00002', version: 3 }], 'the masters module opened design dz00002, its latest version');
      assert.deepEqual(r.items, ['Cut', 'Square', 'Ring', 'Words']);
      assert.equal(r.s.tab, 'designs', 'the library opens again where it was');
    });

    await step('5 · Leave them: the session is handed back as Cancel (nothing asked twice) and the other design opens', async () => {
      await page.evaluate(() => { CharmEditor.current.ctx.select(['dz00002-sq']); document.querySelector('.ce-win').focus(); });
      await page.keyboard.press('Shift+ArrowRight'); await settle(page);
      assert.equal(await page.evaluate(() => CharmEditor.current.dirty()), true);
      const c = await cellAt(page, 4);
      await page.mouse.click(c.x, c.y);
      await page.waitForSelector('.cel-ask3');
      await page.click('.cel-ask3 [data-k="discard"]');
      await page.waitForFunction(() => CharmEditor.current && document.querySelector('.ce-id h2').textContent === 'Paw 4', null, { timeout: 4000 });
      const r = await page.evaluate(() => ({ last: window.__calls.lastEdit, puts: window.__calls.put.length, asks: document.querySelectorAll('.ce-ask').length }));
      assert.equal(r.last.title, 'Moon 2'); assert.equal(r.last.res, null, 'left: null, as Cancel');
      assert.equal(r.puts, 0, 'nothing saved'); assert.equal(r.asks, 0);
    });

    await step('6 · a design\'s versions, newest first; an older one opened', async () => {
      await page.waitForSelector('.ce-root .ce-xpanel .cel-cell:not(.is-free)');
      const c = await cellAt(page, 3);
      await page.mouse.move(c.x, c.y);
      await page.evaluate(() => { const c = [...document.querySelectorAll('.cel-cell:not(.is-free)')].find(x => +x.dataset.i === 3); c.querySelector('.cel-b--hist').click(); });
      await page.waitForSelector('.cel-hist .cel-vrow');
      const rows = await page.evaluate(() => [...document.querySelectorAll('.cel-vrow')].map(b => b.querySelector('.cel-nm').textContent));
      assert.deepEqual(rows, ['Version 4 · latest', 'Version 3', 'Version 2', 'Version 1']);
      await shot(page, 'lib-5-versions.png');
      await page.click('.cel-vrow[data-v="1"]');
      await page.waitForFunction(() => window.__calls.openDesign.length === 3, null, { timeout: 4000 });
      const r = await page.evaluate(() => ({ od: window.__calls.openDesign[2], get: window.__calls.get.filter(g => g.id === 'dz00003'), words: CharmEditor.current.items().find(i => i.name === 'Words').text }));
      assert.deepEqual(r.od, { id: 'dz00003', version: 1 });
      assert.ok(r.get.some(g => g.history), 'the versions were read once'); assert.ok(r.get.some(g => g.version === 1));
      assert.equal(r.words, 'V1', 'version 1 is what is open');
    });

    await step('7 · Recent: what was opened and placed here, newest first', async () => {
      await page.waitForSelector('.ce-root .ce-xpanel .cel-cell:not(.is-free)');
      await page.click('.cel-tab[data-t="recent"]'); await settle(page);
      const s = await st(page);
      assert.equal(s.tab, 'recent');
      assert.deepEqual(s.keys.slice(0, 5), ['d:dz00003', 'd:dz00004', 'd:dz00002', 'd:dz00001', 'd:dz00000']);
      const sub = await page.evaluate(() => document.querySelector('.cel-cell:not(.is-free) .cel-s2').textContent);
      assert.match(sub, /^opened /);
    });

    await step('7 · the charm library: 500 masters, searched as typed, opened through the masters module, dragged onto the charm', async () => {
      await page.click('.cel-tab[data-t="masters"]'); await settle(page);
      let s = await st(page); assert.equal(s.total, 500); assert.equal(s.keys[0], 'm:SKU0');
      await page.click('.cel-search input');
      await page.evaluate(() => { const i = document.querySelector('.cel-search input'); i.value = ''; i.dispatchEvent(new Event('input', { bubbles: true })); });
      await page.keyboard.type('sku12');
      s = await st(page);
      assert.equal(s.n, 11, 'SKU12 and SKU120 … SKU129'); assert.equal(s.keys[0], 'm:SKU12');
      // drag the first onto the charm (the editor holds design dz00003 v1, which has its cut line: the master's is left out)
      const n0 = await page.evaluate(() => CharmEditor.current.items().length);
      const a = await cellAt(page, 0), b = await stageMid(page);
      await page.mouse.move(a.x, a.y); await page.mouse.down(); await page.mouse.move(b.x, b.y, { steps: 12 }); await page.mouse.up();
      await page.waitForFunction(n => CharmEditor.current.items().length === n + 1, n0, { timeout: 3000 });
      const g = await page.evaluate(() => { const its = CharmEditor.current.items(); const g = its[its.length - 1]; return { name: g.name, n: g.children.length, design: g.design }; });
      assert.deepEqual(g, { name: 'SKU12', n: 1, design: { sku: 'SKU12' } });
      // open it: the charm has a change, Keep and open saves it as a version of dz00003 through the masters module's own save
      const c = await cellAt(page, 0);
      await page.mouse.click(c.x, c.y);
      await page.waitForSelector('.cel-ask3'); await page.click('.cel-ask3 [data-k="keep"]');
      await page.waitForFunction(() => window.__calls.open.length === 1 && CharmEditor.current && document.querySelector('.ce-id h2').textContent === 'SKU12', null, { timeout: 4000 });
      const r = await page.evaluate(() => ({ open: window.__calls.open, put: window.__calls.put.map(p => ({ id: p.id, name: p.name })) }));
      assert.deepEqual(r.open, ['SKU12']); assert.deepEqual(r.put, [{ id: 'dz00003', name: 'Fix 3' === 'x' ? '' : 'Calendar 3' }]);
    });

    await step('7 · a saved version is in the list at once (the module\'s change feed), newest first', async () => {
      await page.waitForSelector('.ce-root .ce-xpanel .cel-cell:not(.is-free)');
      await page.click('.cel-tab[data-t="designs"]');
      await page.evaluate(() => { const i = document.querySelector('.cel-search input'); i.value = ''; i.dispatchEvent(new Event('input', { bubbles: true })); });
      await settle(page);
      const s = await st(page);
      assert.equal(s.keys[0], 'd:dz00003'); assert.equal(s.total, 3000);
      const v = await page.evaluate(() => document.querySelector('.cel-cell:not(.is-free) .cel-v').textContent);
      assert.equal(v, 'v5');
    });

    await step('7 · the list view: one to a row, the same virtual list', async () => {
      await page.click('.cel-view'); await settle(page);
      const s = await st(page);
      assert.equal(s.view, 'list'); assert.equal(s.lay.cols, 1); assert.equal(s.lay.rowH, 58);
      assert.ok(s.cells <= 48, 'elements: ' + s.cells);
      await shot(page, 'lib-6-list.png');
      await page.click('.cel-view'); await settle(page);
      assert.equal((await st(page)).view, 'tiles');
    });

    await step('nothing went wrong in the page; no key reached it', async () => {
      assert.deepEqual(page.__errs, []);
      assert.equal(await page.evaluate(() => window.__pageKeys), 0);
    });
  } catch (e) { fail('the main page', e); }

  /* ════ the phone width ════ */
  await step('7 · at phone width (390 × 844) the library is a drawer over the room; its tiles fit inside it', async () => {
    const p = await newPage(390, 844);
    await p.evaluate(() => { try { localStorage.setItem('ce.xpanel', 'library'); } catch (_) {} window.openEditor(); });
    await p.waitForSelector('.ce-root .ce-xpanel .cel-cell:not(.is-free)');
    await settle(p);
    const r = await p.evaluate(() => {
      const panel = document.querySelector('.ce-xpanel').getBoundingClientRect();
      const out = [...document.querySelectorAll('.cel-cell:not(.is-free), .cel-chip, .cel-search input, .cel-tab')].filter(e => { const b = e.getBoundingClientRect(); return b.width && (b.right > panel.right + 0.5 || b.left < panel.left - 0.5); }).map(e => e.className);
      return { panel: [panel.left, panel.right], vw: innerWidth, out, hscroll: document.querySelector('.cel-list').scrollWidth > document.querySelector('.cel-list').clientWidth + 1 };
    });
    await shot(p, 'lib-7-phone.png');
    assert.ok(r.panel[1] <= r.vw + 0.5, 'the drawer is on screen');
    assert.deepEqual(r.out.filter(c => !/cel-chip/.test(c)), [], 'nothing outside the drawer');
    assert.equal(r.hscroll, false, 'no sideways scroll');
    assert.deepEqual(p.__errs, []);
    await p.context().close();
  });

  await browser.close();
  console.log(`\n${results.length} passed, ${failures.length} failed`);
  if (failures.length) { for (const f of failures) console.log('  FAIL ' + f); process.exit(1); }
})().catch(e => { console.error(e); process.exit(1); });
