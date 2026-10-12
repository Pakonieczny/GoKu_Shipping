// ENGFONT (engrave-ui-1012): the live engraving preview (charm-nest-engrave-preview.js) and the save path that carries a picked font to the job. Offline: no Etsy, no paid reading,
// no live write; the preview is proved in the preinstalled Chromium against a tiny local static server (the repo's own scripts and vendor/fonts), the rest in node.
//   A  node:   the cap heights the preview lays rows out with are the ones of the vendor font files; its rows are the rows of charm-nest-geom.js layoutLines; a font word is the installed font
//              (Ace -> Jost Slashed Zero, Pristina -> Marck Script, an unknown word -> Source Sans 3); a font picked on its own (no new words) re-seeds the face's job, and only that face
//   B  browser: picking font A then font B changes the computed font-family of the preview's text node IN THE SAME TICK and document.fonts reports each as loaded; the words stay hidden and the
//              small "Loading font" shows only while the font is really being fetched (a delayed file) and both are gone once it is ready; typing (input, keyup) and a drop-down pick
//              change the text at once; empty words show the quiet empty slot; several lines are several rows; Ace is drawn in capitals; the back flips only the drawing; a position
//              and a size are drawn; words longer than the box are scaled down; a font that cannot be fetched says so and still shows the words; no page error
//   PLAYWRIGHT_BROWSERS_PATH=/opt/pw-browsers node tests/charm-nest/engrave-preview.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>; without a browser B is skipped)
'use strict';
const fs = require('fs'), path = require('path'), http = require('http'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js')), G = require(path.join(root, 'charm-nest-geom.js')), F = require(path.join(root, 'charm-nest-engrave-faces.js'));
const P = require(path.join(root, 'charm-nest-engrave-preview.js')), S = require(path.join(root, 'charm-nest-engrave-setup.js'));
let n = 0;
const ok = (msg, cond) => { assert.ok(cond, msg); n++; };
const eq = (msg, a, b) => { assert.deepEqual(a, b, msg); n++; };

(async () => {
  /* ── A · node ── */
  const opentype = require(path.join(root, 'vendor/opentype-1.3.4.min.js'));
  const parse = file => { const b = fs.readFileSync(path.join(root, file)); return opentype.parse(b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength)); };
  for (const f of O.ENGRAVING_FONTS) {
    ok(`the preview knows the cap height of ${f.id}`, P.CAP_PER_EM[f.id] > 0);
    for (const [w, file] of Object.entries(f.files)) { const font = parse(file); ok(`${f.id} ${w}: cap height as the job measures it`, Math.abs(P.CAP_PER_EM[f.id] - G.capPerEm(font)) < 0.002); }
  }
  { const font = parse('vendor/fonts/SourceSans3-Regular.otf'), lines = ['Name One', 'Name Two', 'N'], size = 11, gap = 0.216;
    const job = G.layoutLines(lines, font, size, gap, 0, [0, 0]), mine = P.layoutOf({ lines, capPerEm: G.capPerEm(font), lineGap: gap });
    ok('the same block height as layoutLines', Math.abs(job.totalH - mine.totalH * size) < 1e-9);
    mine.rows.forEach((r, i) => ok(`row ${i}: the same baseline as layoutLines`, Math.abs(r.y * size - job.rows[i].y) < 1e-9));
    eq('rows keep their text', mine.rows.map(r => r.text), lines); }
  eq('the font family of an id', P.fontFamilyOf('jost-ace'), "'CNEng jost-ace', serif"); eq('an unknown id is the app\'s own font', P.fontFamilyOf('nope'), "'CNEng source-sans-3', serif");
  const spec = f => ({ spec: { font: { asked: 'Ace', id: f, name: f, source: 'option' } } });
  eq('a shop word is its installed font (Ace is Jost Slashed Zero)', P.fontIdOf(spec('vibur'), 'back', 'Ace'), 'jost-ace');
  eq('Pristina is Marck Script', P.fontIdOf(spec('vibur'), 'back', 'Pristina'), 'marck-script');
  eq('no word: the line\'s font', P.fontIdOf(spec('vibur'), 'front', ''), 'vibur');
  eq('a word the shop does not offer, and no line: the app\'s own font', P.fontIdOf(null, 'back', 'Cooper Black'), 'source-sans-3');
  { // a font picked on its own re-seeds the face's job (it is fitted again in that font); a change to the other face does not touch it
    const LK = '1000000001_2000000001', reading = { v: 1, mode: 'back', source: 'default', front: null, back: { text: 'Name One', lines: ['Name One'], fontWord: null }, textSource: 'single', needsPerson: false, confidence: 0.9 };
    const mk = () => ({ key: LK, state: 'pooled', order: { receiptId: '1000000001' }, line: { transactionId: '2000000001' }, poolIds: [LK + '_1'], spec: { designSku: '5X8.5', engraveCandidate: true, personalization: ['Name One'], font: { asked: 'Ace', id: 'jost-ace', name: 'Jost Slashed Zero', source: 'option' }, engraving: reading } });
    const env = jobs => { const log = []; return { log, who: () => 'Test Operator', now: () => 1760190000000, store: (l, ov) => { if (ov) l.engravingOverride = ov; else delete l.engravingOverride; }, jobsOf: () => jobs.slice(), ensureJobs: () => jobs, slots: () => [null],
      remove: () => {}, skip: async () => {}, setNone: () => {}, reopen: () => {}, seed: async (j, seed) => { log.push([j.key, seed.fontWord, seed.lines.join('/')]); j.lines = seed.lines.slice(); j.text = seed.text; j.faceFontWord = seed.fontWord || null; }, changed: () => {} }; };
    const back = () => ({ key: F.jobKey(LK, null, 'back'), face: 'back', slot: null, state: 'review', lines: ['Name One'], text: 'Name One' });
    let jobs = [back()], e = env(jobs), res = await S.apply(mk(), { back: { text: 'Name One', fontWord: 'Pristina' } }, e);
    ok('the save went through', res.ok); eq('only a font picked: the job is seeded again with that font', e.log, [[jobs[0].key, 'Pristina', 'Name One']]);
    res = await S.apply(mk(), { back: { text: 'Name One', fontWord: 'Pristina' } }, e); eq('the same pick again seeds nothing more', e.log.length, 1);
    res = await S.apply(mk(), { back: { text: 'Name One', fontWord: null } }, e); eq('back to the line\'s font seeds it once more, with no font word', e.log[1], [jobs[0].key, null, 'Name One']);
    jobs = [back()]; e = env(jobs); res = await S.apply(mk(), { mode: 'back' }, e); eq('a save that touches no words and no font seeds nothing', e.log, []);
    jobs = [back()]; e = env(jobs); res = await S.apply(mk(), { mode: 'both', front: { text: 'W', fontWord: 'Stylish' } }, e); eq('a pick for the front does not re-seed the back', e.log, []);
  }

  /* ── B · browser ── */
  let chromium; const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  for (const p of [path.join(pwDir, 'playwright-core'), '/opt/node22/lib/node_modules/playwright/node_modules/playwright-core', 'playwright-core']) { try { ({ chromium } = require(p)); break; } catch (_) { /* next */ } }
  const exe = process.env.CHROMIUM || ['/opt/pw-browsers/chromium', '/opt/pw-browsers/chromium-1194/chrome-linux/chrome'].find(p => fs.existsSync(p));
  if (!chromium || !exe) { console.log(`engrave-preview: ${n} checks passed (no Chromium here: the browser part is skipped)`); return; }
  const MIME = { '.js': 'text/javascript', '.ttf': 'font/ttf', '.otf': 'font/otf', '.html': 'text/html; charset=utf-8' };
  const PAGE = '<!doctype html><meta charset="utf-8"><title>t</title><body><div id="host" style="width:360px;height:240px"></div><textarea id="ta"></textarea><select id="sel"><option value="">The line\'s font</option><option value="Stylish">Stylish</option><option value="Pristina">Pristina</option><option value="Ace">Ace</option></select>'
    + '<script src="/charm-nest-orders.js"></script><script src="/charm-nest-engrave-preview.js"></script></body>';
  const srv = http.createServer((req, res) => {
    const u = decodeURIComponent(req.url.split('?')[0]); if (u === '/__t.html') { res.writeHead(200, { 'content-type': MIME['.html'] }); return res.end(PAGE); }
    const f = path.join(root, u); if (!f.startsWith(root) || !fs.existsSync(f) || !fs.statSync(f).isFile()) { res.writeHead(404); return res.end(); }
    res.writeHead(200, { 'content-type': MIME[path.extname(f)] || 'application/octet-stream' }); res.end(fs.readFileSync(f));
  });
  await new Promise(r => srv.listen(0, '127.0.0.1', r)); const origin = `http://127.0.0.1:${srv.address().port}`;
  const browser = await chromium.launch({ executablePath: exe, args: ['--no-sandbox'] });
  try {
    const page = await (await browser.newContext({ viewport: { width: 500, height: 600 } })).newPage(), errors = [];
    page.on('pageerror', e => errors.push(e.message));
    await page.route('**/vendor/fonts/MarckScript-Regular.ttf', async r => { await new Promise(x => setTimeout(x, 700)); r.continue(); });   // (a slow font: the wait line can be seen)
    await page.route('**/vendor/fonts/Mynerve-Regular.ttf', r => r.abort());                                                                          // (a font that cannot be had)
    await page.goto(origin + '/__t.html', { waitUntil: 'load' });
    const info = () => page.evaluate(() => { const el = document.getElementById('host'), t = el.querySelector('.engpvText'), w = el.querySelector('.engpvWait'), pv = el.querySelector('.engpv'), cs = t && getComputedStyle(t);
      return { text: t && t.textContent, fam: cs && cs.fontFamily, vis: cs && cs.visibility, waitShown: !!w && !w.hidden, waitText: w ? w.textContent.trim() : '', fail: !!(w && w.hasAttribute('data-fail')), slot: !!el.querySelector('.engpvSlot:not([hidden])'),
        font: pv && pv.dataset.font, state: pv && pv.dataset.state, size: pv && pv.dataset.size, style: t && t.getAttribute('style'), fs: cs && parseFloat(cs.fontSize), h: t && t.getBoundingClientRect().height,
        drawT: getComputedStyle(el.querySelector('.engpvDraw')).transform, loaded: [...document.fonts].filter(f => f.status === 'loaded').map(f => f.family) }; });

    // B1 · font A, then font B: the family changes in the same tick, document.fonts says loaded once each is ready
    await page.evaluate(() => { window.__draws = 0; CharmNestEngravePreview.render(document.getElementById('host'), { text: 'Name One', fontWord: 'Stylish', face: 'back', drawing: window.__d = host => { window.__draws++; host.innerHTML = '<svg viewBox="0 0 24 30" width="24" height="30"><path d="M6 0H24V30H6V28H0V18H6Z" fill="#eee" stroke="#000"/></svg>'; } }); });
    let i = await info(); ok('font A is the text node\'s family at once', /CNEng playwrite-us-trad/.test(i.fam) && i.text === 'Name One');
    await page.evaluate(() => CharmNestEngravePreview.ensure('playwrite-us-trad')); i = await info();
    const dd = await page.evaluate(() => ({ check: document.fonts.check("12px 'CNEng playwrite-us-trad'"), ready: CharmNestEngravePreview.ready('playwrite-us-trad'), face: [...document.fonts].some(f => f.family.replace(/["']/g, '') === 'CNEng playwrite-us-trad' && f.status === 'loaded') }));
    ok('document.fonts reports font A loaded', dd.check && dd.ready && dd.face && i.font === 'ready'); ok('and the words are shown, no wait line', i.vis === 'visible' && !i.waitShown);
    const sync = await page.evaluate(() => { const el = document.getElementById('host'); CharmNestEngravePreview.update(el, { fontWord: 'Pristina' }); const t = el.querySelector('.engpvText'), w = el.querySelector('.engpvWait'), cs = getComputedStyle(t);
      return { fam: cs.fontFamily, vis: cs.visibility, waitShown: !w.hidden, waitText: w.textContent.trim(), label: w.getAttribute('aria-label'), status: CharmNestEngravePreview.fontStatus('marck-script'), ready: CharmNestEngravePreview.ready('marck-script') }; });
    ok('font B is the family in the same tick (nothing awaited)', /CNEng marck-script/.test(sync.fam));
    ok('while B is really being fetched: the words wait (no wrong font shown) and the small labelled spinner says so', sync.vis === 'hidden' && sync.waitShown && /Loading font/.test(sync.waitText) && sync.label === 'Loading font' && sync.status === 'loading' && !sync.ready);
    await page.evaluate(() => CharmNestEngravePreview.ensure('marck-script')); i = await info();
    ok('document.fonts reports font B loaded; the words are drawn and the label is gone', i.loaded.includes('CNEng marck-script') && i.loaded.includes('CNEng playwrite-us-trad') && i.vis === 'visible' && !i.waitShown && i.font === 'ready');
    const back = await page.evaluate(() => { const el = document.getElementById('host'); CharmNestEngravePreview.update(el, { fontWord: 'Stylish' }); const w = el.querySelector('.engpvWait'), cs = getComputedStyle(el.querySelector('.engpvText')); return { fam: cs.fontFamily, vis: cs.visibility, waitShown: !w.hidden }; });
    ok('back to a font already loaded: changed at once and no label at all', /CNEng playwrite-us-trad/.test(back.fam) && back.vis === 'visible' && !back.waitShown);
    eq('the drawing was drawn once through every font change', await page.evaluate(() => window.__draws), 1);
    i = await info(); eq('the BACK flips only the drawing (the charm turned over)', i.drawT, 'matrix(-1, 0, 0, 1, 0, 0)');
    await page.evaluate(() => CharmNestEngravePreview.update(document.getElementById('host'), { face: 'front' })); i = await info();
    ok('the FRONT is not flipped, the words are still there', i.drawT === 'none' && i.text === 'Name One'); eq('a new face draws the drawing again', await page.evaluate(() => window.__draws), 2);

    // B2 · typing (input, keyup) and a drop-down pick, wired by link(): the text is changed in the same tick
    const typed = await page.evaluate(() => { const el = document.getElementById('host'), ta = document.getElementById('ta'), sel = document.getElementById('sel'), P = CharmNestEngravePreview, out = {};
      P.render(el, { text: '', fontId: 'source-sans-3', face: 'front' }); P.link(el, { text: ta, font: sel }, { fallbackFontId: 'source-sans-3' });
      const read = () => el.querySelector('.engpvText').textContent;
      ta.value = 'Name'; ta.dispatchEvent(new Event('input', { bubbles: true })); out.afterInput = read();
      ta.value = 'Name One'; ta.dispatchEvent(new KeyboardEvent('keyup', { bubbles: true })); out.afterKeyup = read();
      sel.value = 'Stylish'; sel.dispatchEvent(new Event('change', { bubbles: true })); out.stylish = getComputedStyle(el.querySelector('.engpvText')).fontFamily;
      sel.value = ''; sel.dispatchEvent(new Event('change', { bubbles: true })); out.line = getComputedStyle(el.querySelector('.engpvText')).fontFamily;
      ta.value = ''; ta.dispatchEvent(new Event('input', { bubbles: true })); out.empty = [el.querySelector('.engpv').dataset.state, !el.querySelector('.engpvSlot').hidden, el.querySelector('.engpvText').textContent];
      return out; });
    eq('typing shows the words at once (input)', typed.afterInput, 'Name'); eq('and on keyup', typed.afterKeyup, 'Name One');
    ok('a drop-down pick changes the family at once', /CNEng playwrite-us-trad/.test(typed.stylish)); ok('and the line\'s own font is back on the empty choice', /CNEng source-sans-3/.test(typed.line));
    eq('empty words: the quiet empty slot, no words written', typed.empty, ['empty', true, '']);

    // B3 · several rows, capitals for Ace, a position, a size, words longer than the box
    await page.evaluate(() => { const P = CharmNestEngravePreview; P.render(document.getElementById('host'), { text: 'Name One', fontId: 'jost-ace', face: 'back' }); return P.ensure('jost-ace'); });
    await page.evaluate(() => CharmNestEngravePreview.update(document.getElementById('host'), { text: 'Name One' })); const one = await info();
    await page.evaluate(() => CharmNestEngravePreview.update(document.getElementById('host'), { text: 'Name One\nname two' })); i = await info();
    eq('two lines are two rows, in capitals for Ace (it has no lowercase)', i.text, 'NAME ONE\nNAME TWO'); ok('and the block is twice as tall as one row', Math.abs(i.h / one.h - 2) < 0.15); ok('Ace is the family', /CNEng jost-ace/.test(i.fam));
    await page.evaluate(() => CharmNestEngravePreview.update(document.getElementById('host'), { text: 'Name One', position: { x: 0.25, y: 0.75, size: 0.1, angle: 90 } })); i = await info();
    ok('a position is laid as given: left, top, a quarter turn counter-clockwise', /left:\s*25%/.test(i.style) && /top:\s*75%/.test(i.style) && /rotate\(-90deg\)/.test(i.style)); ok('the size is a fraction of the box\'s height (240 px)', Math.abs(i.fs - 24) < 0.6);
    await page.evaluate(() => CharmNestEngravePreview.update(document.getElementById('host'), { text: 'A very long line of words that cannot fit in this small box', position: { x: 0.5, y: 0.5, size: 0.3, angle: 0 } })); i = await info();
    ok('words longer than the box are scaled down to it', +i.size > 0 && +i.size < 0.3 && /scale\(/.test(i.style));
    await page.evaluate(() => CharmNestEngravePreview.update(document.getElementById('host'), { position: null, text: 'Name One' })); i = await info();
    ok('position null is the middle at the automatic size', /left:\s*50%/.test(i.style) && Math.abs(i.fs - 48) < 1);

    // B4 · a font that cannot be fetched says so, and the words are still shown
    await page.evaluate(() => CharmNestEngravePreview.render(document.getElementById('host'), { text: 'Name One', fontWord: 'Angelina' }));
    await page.evaluate(() => CharmNestEngravePreview.ensure('mynerve')); i = await info();
    ok('a font that could not be had: "Font not available", no spinner, the words still drawn', i.font === 'failed' && i.waitShown && /Font not available/.test(i.waitText) && i.fail && i.text === 'Name One' && i.vis === 'visible');
    ok('no page error', errors.length === 0);
    if (errors.length) console.log('  page errors:', errors.slice(0, 3));
  } finally { await browser.close(); srv.close(); }
  console.log(`engrave-preview: ${n} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
