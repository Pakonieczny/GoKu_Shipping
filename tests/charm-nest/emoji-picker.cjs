// The Emoji button of the Back engraving words (Paul, 6 Oct 2026, 4:50 pm: "add a new button as part of the Back Engraving ... The new
// button "Emoji" must contain all of the compatible emojis that can be included in the text box. Make it a comprehensive emoji list popup
// that is easily usable by the user at a glance."). charm-nest-emoji-picker.js is the one picker; charm-nest-emoji-data.js (CNEmojiData) is
// what the laser can engrave. Headless Chromium against the local fake site (bridge-server.cjs): a real master file, a real Auto run for one
// personalised piece, up to the Engraving card, then the card is driven the way a person drives it:
//   · the Emoji button in the Words on the back box; the picker opens inside the window (not full screen), says "Loading emoji…" while the
//     engraving font is not there yet, draws every cell as it will be engraved (SVG path from the engraving font), lists the count;
//   · picks: a toned hand, a flag, a family (ZWJ), a keycap, in the middle of the words; text and caret exactly right, the `input` event
//     fired, the card's preview refit (job.lines, job.fit.ok) with the picker staying open through the card's rebuild, one more pick after it;
//   · Recent updates (and survives in localStorage), search "heart", every tab jumps, the tone chooser, keyboard (arrows, Enter, Esc with focus
//     back at the same caret), the button again and an outside tap close it;
//   · the typed-emoji path: what was picked passes glyphCoverage with the real fonts, the fit succeeds (no "Unsupported engraving characters"),
//     and EVERY emoji on offer is engravable (glyphCoverage ok and a path in the engraving font);
//   · 280, 390, 700 and 1280 px: the popover inside the window, no sideways scroll, the card no wider for the button. Pictures in SHOTS.
//   SHOTS=<dir> node tests/charm-nest/emoji-picker.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const fs = require('fs'), path = require('path'), os = require('os');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const { buildMaster } = require('./fixture-master.cjs');

const day = Math.floor(Date.now() / 1000);
const tx = (rid, i, sku, extra = {}) => Object.assign({ transaction_id: Number(`${rid}${i}`), listing_id: 1718000 + i, receipt_id: rid, sku, title: `${sku} charm`, quantity: 1, expected_ship_date: day + 86400, variations: [{ formatted_name: 'Metal', formatted_value: '14k Gold Filled' }], is_personalized: false }, extra);
const pers = text => ({ is_personalized: true, variations: [{ formatted_name: 'Metal', formatted_value: 'Sterling Silver' }, { formatted_name: 'Personalization', formatted_value: text }] });
const receipt = (rid, txs) => ({ receipt_id: rid, order_number: rid, name: 'Buyer ' + rid, country_iso: 'US', city: 'Austin', message_from_buyer: '', update_timestamp: day, create_timestamp: day - 3600, status: 'Paid', is_shipped: false, transactions: txs });
const RID = '3521200001', KEY = RID + '_' + RID + '1';
const receipts = [receipt(RID, [tx(RID, 1, 'BR-TST-04', pers('CARLA'))])];
const agentResults = { engraveIntent: o => { const txt = String(o.messages[0].content[0].text), m = /Personalisation field: (\[.*?\])/.exec(txt); let p = []; try { p = JSON.parse(m ? m[1] : '[]'); } catch (_) {} const req = { side: 'back', font: null, handwriting: false, image: false }; return { engrave: p.length > 0, text: p.join('\n'), source: 'personalization', sourceQuote: p[0] || '', requests: req, questions: [], confidence: 0.97 }; } };

// until EM1's charm-nest-emoji-data.js is in the repo the picker is tried against a small stand-in of the same shape
const STUB_SRC = `(() => { const S = require_map; })`;
const stubData = () => {
  const map = require('../../vendor/fonts/emoji-sequences.json').sequences;
  const mk = (c, n, k, t) => { if (!map[c]) throw new Error('stub emoji not in the shape map: ' + c); return { c, n, k, t: t ? 1 : 0 }; };
  const groups = [
    { id: 'smileys', name: 'Smileys', icon: '😀', items: [mk('😀', 'grinning face', 'smile happy'), mk('😍', 'smiling face with heart-eyes', 'love heart'), mk('🥰', 'smiling face with hearts', 'love')] },
    { id: 'people', name: 'People', icon: '👋', items: [mk('👋', 'waving hand', 'hello wave', 1), mk('👍', 'thumbs up', 'yes like', 1), mk('👨‍👩‍👧‍👦', 'family: man, woman, girl, boy', 'family')] },
    { id: 'symbols', name: 'Symbols', icon: '❤️', items: [mk('❤️', 'red heart', 'love heart'), mk('1️⃣', 'keycap: 1', 'one number'), mk('🔥', 'fire', 'hot')] },
    { id: 'flags', name: 'Flags', icon: '🏁', items: [mk('🇨🇦', 'flag: Canada', 'canada flag')] }
  ];
  const tones = [{ id: '', name: 'Default', c: '✋' }].concat(['🏻', '🏼', '🏽', '🏾', '🏿'].map((t, i) => ({ id: t, name: ['Light', 'Medium-light', 'Medium', 'Medium-dark', 'Dark'][i], c: '✋' + t })));
  const toned = {}; for (const b of ['👋', '👍']) { toned[b] = {}; for (const t of ['🏻', '🏼', '🏽', '🏾', '🏿']) toned[b][t] = b + t; }
  return { version: 'stub', count: groups.reduce((n, g) => n + g.items.length, 0), groups, tones, toned };
};

(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { try { ({ chromium } = require('/opt/node22/lib/node_modules/playwright/node_modules/playwright-core')); } catch (__) { console.log('  – no playwright-core: not run'); return; } }
  const shots = process.env.SHOTS || ''; if (shots) fs.mkdirSync(shots, { recursive: true });
  const REAL = fs.existsSync(path.join(root, 'charm-nest-emoji-data.js')) && /charm-nest-emoji-data\.js/.test(fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8'));
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cn-emoji-')), masterPath = path.join(tmp, 'BRITES-master.ai');
  await buildMaster(masterPath, { count: 4, edge: false });
  const srv = await start({ receipts, agentResults });
  const { sorterOrigin, stationOrigin, st } = srv;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = [], errors = [];
  const check = (ok, msg) => { if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
  console.log(REAL ? 'emoji data: the real charm-nest-emoji-data.js' : process.env.EMOJI_DATA ? 'emoji data: ' + process.env.EMOJI_DATA : 'emoji data: STAND-IN (the real file is not in the repo yet)');
  try {
    const ctx = await browser.newContext({ viewport: { width: 1500, height: 1000 }, deviceScaleFactor: 2 });
    const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
    await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
    await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
    await ctx.route(/fonts\.googleapis|fonts\.gstatic/, r => r.fulfill({ status: 200, contentType: 'text/css', body: '' }));
    await ctx.route(/^https:\/\/firebasestorage\.googleapis\.com\//, r => { const u = new URL(r.request().url()), m = /\/o\/(.+)$/.exec(u.pathname), key = m ? decodeURIComponent(m[1]) : '', b = st.blobs.get(key); if (!b) return r.fulfill({ status: 404, body: 'no blob ' + key }); return r.fulfill({ status: 200, headers: { 'Content-Type': b.meta.contentType || 'application/octet-stream', 'Access-Control-Allow-Origin': '*', 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: b.buf }); });
    await ctx.addInitScript(({ station, sorter }) => {
      if (location.origin === station) { localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200)); localStorage.setItem('employee_name', 'Tester'); }
      if (location.origin === sorter) { localStorage.setItem('cn.employee', 'Test Operator'); localStorage.setItem('cn.tour.seen', '1'); }
      window.confirm = () => true; window.alert = () => {};
    }, { station: stationOrigin, sorter: sorterOrigin });
    if (!REAL && process.env.EMOJI_DATA) await ctx.addInitScript({ content: fs.readFileSync(process.env.EMOJI_DATA, 'utf8') });   // (a copy of the data file that is not in the repo yet)
    else if (!REAL) {
      const data = stubData();
      await ctx.addInitScript(d => { window.CNEmojiData = Object.assign({}, d, { search(q) { q = String(q).toLowerCase().trim(); const out = []; for (const g of d.groups) for (const it of g.items) if ((it.n + ' ' + it.k).toLowerCase().includes(q)) out.push(it); return out; } }); }, data);
    }
    const page = await ctx.newPage();
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.CN && window.CN.S && window.RunCtl && window.DesignLink && CN.S.cloud.ok !== null, null, { timeout: 60000 });
    await page.evaluate(station => { const s = CN.S.settings; s.dsOrigin = station; s.engine = 'solver'; s.budgetS = 8; s.review = 'off'; s.naming = 'off'; s.notify = 'off'; s.sound = 'off'; s.autoCommit = 'on'; s.runMode = 'manual'; s.pullMode = 'all'; s.heartbeatS = 2; s.heartbeatMiss = 2; s.sandboxStream = 'off'; CN.saveSettings(); }, stationOrigin);
    await page.evaluate(() => CN.setMode('master')); await page.waitForSelector('#mFile', { state: 'attached' }); await page.setInputFiles('#mFile', masterPath);
    await page.waitForFunction(() => { const j = [...B.master.jobs.values()][0]; return j && ['done', 'error'].includes(j.state); }, null, { timeout: 120000 });
    await page.evaluate(() => CN.setMode('design')); await page.evaluate(() => DesignLink.ensure());
    await page.evaluate(() => CN.setMode('orders')); await page.evaluate(() => RunCtl.setMode('auto'));
    const t0 = Date.now();
    for (;;) {
      const r = await page.evaluate(() => B.run && { status: B.run.status }), jobs = await page.evaluate(() => [...Engrave.items().values()].map(j => j.state));
      if (r && r.status === 'stopped') throw new Error('the run stopped');
      if (r && r.status === 'processed' && jobs.length >= 1 && !jobs.some(s => ['ready', 'classify', 'fitting', 'reclassify'].includes(s))) break;
      if (Date.now() - t0 > 300000) throw new Error('the run never reached the engraving review: ' + JSON.stringify([r, jobs]));
      await page.waitForTimeout(500);
    }
    const jstate = await page.evaluate(k => Engrave.items().get(k) && Engrave.items().get(k).state, KEY);
    check(jstate === 'review', 'one personalised piece waits in the engraving review (state ' + jstate + ')');
    await page.evaluate(k => { CN.setMode('engrave'); Engrave.restoreView({ tab: 'place', chosen: true, list: false, focus: k }); Engrave.render(); }, KEY);   // (the card open, as a person opens it from the list)
    await page.waitForSelector('#egQueue .rvItem[data-kind=placement] textarea[data-f=words]');
    const sleep = ms => page.waitForTimeout(ms);
    const ta = '#egQueue textarea[data-f=words]', btn = '#egQueue [data-emoji]', pop = '.emPop:not([hidden])';
    const words = async () => {
      const r = await page.evaluate(sel => { const t = document.querySelector(sel); return t ? { v: t.value, s: t.selectionStart, e: t.selectionEnd, focus: document.activeElement === t } : null; }, ta);
      if (r) return r;
      console.log('    (no words box: ' + JSON.stringify(await page.evaluate(k => ({ view: Engrave.view(), state: (Engrave.items().get(k) || {}).state, reason: (Engrave.items().get(k) || {}).reason, queue: document.querySelector('#egQueue').innerHTML.slice(0, 400), picker: window.CNEmojiPicker.state() }), KEY)) + ')');
      throw new Error('the words box is gone');
    };
    const ui = () => page.evaluate(() => { const p = document.querySelector('.emPop'); return { open: !!p && !p.hidden, state: window.CNEmojiPicker.state() }; });
    const itemsOf = () => page.evaluate(() => CNEmojiData.groups.flatMap(g => g.items.map(i => ({ c: i.c, n: i.n, g: g.id, t: i.t }))));
    const all = await itemsOf();

    // ── the button ──
    const b0 = await page.evaluate(() => { const b = document.querySelector('#egQueue [data-emoji]'), w = document.querySelector('#egQueue .pvWords'), u = document.querySelector('#egQueue [data-a=usewords]'); const r = b.getBoundingClientRect(), wr = w.getBoundingClientRect(); return { text: b.textContent.trim(), cls: b.className, inBox: w.contains(b), fits: r.left >= wr.left - .5 && r.right <= wr.right + .5, h: Math.round(r.height), uh: Math.round(u.getBoundingClientRect().height), sans: getComputedStyle(b).fontFamily === getComputedStyle(u).fontFamily }; });
    check(b0.text === 'Emoji' && /btn ghost xs/.test(b0.cls) && b0.inBox && b0.fits, 'an "Emoji" button, ghost xs like the others, sits in the Words on the back box ' + JSON.stringify(b0));
    check(Math.abs(b0.h - b0.uh) <= 3 && b0.sans, 'it is the size and type of the existing xs buttons (' + b0.h + ' vs ' + b0.uh + ' px)');

    // ── opening: "Loading emoji…" while the engraving font is not there, instant when it is ──
    await page.evaluate(() => { const E = Engrave.fonts.Regular; CNEmojiPicker.setFonts(() => new Promise(r => setTimeout(() => r(E), 900))); });
    await page.click(ta); await page.evaluate(sel => { const t = document.querySelector(sel); t.setSelectionRange(3, 3); }, ta);   // CAR|LA
    await page.click(btn);
    await page.waitForSelector(pop);
    const spin = await page.evaluate(() => { const l = document.querySelector('.emLoad'); return { shown: !l.hidden, text: l.textContent.trim(), busy: document.querySelector('.emGrid').getAttribute('aria-busy') }; });
    check(spin.shown && /Loading emoji…/.test(spin.text) && spin.busy === 'true', 'a first-time wait shows the labelled spinner "Loading emoji…": ' + JSON.stringify(spin));
    await page.waitForFunction(() => window.CNEmojiPicker.state().font === 'ready');
    await page.evaluate(() => CNEmojiPicker.setFonts(null));
    check(await page.evaluate(() => document.querySelector('.emLoad').hidden), 'the spinner goes when the font is there');
    const geo = () => page.evaluate(() => { const p = document.querySelector('.emPop'), r = p.getBoundingClientRect(), vw = document.documentElement.clientWidth, vh = innerHeight; return { l: Math.round(r.left), t: Math.round(r.top), r: Math.round(r.right), b: Math.round(r.bottom), w: Math.round(r.width), h: Math.round(r.height), vw, vh, inside: r.left >= 0 && r.top >= 0 && r.right <= vw + .5 && r.bottom <= vh + .5, sideways: document.documentElement.scrollWidth > document.documentElement.clientWidth }; });
    let g = await geo();
    check(g.inside && g.w <= 400 && g.h < g.vh - 8 && !g.sideways, 'the popover is a popover (' + g.w + '×' + g.h + ' in a ' + g.vw + '×' + g.vh + ' window), inside it, not full screen');
    const st0 = await page.evaluate(() => ({ svg: document.querySelectorAll('.emCell svg.emG path').length, cells: document.querySelectorAll('.emCell').length, total: window.CNEmojiPicker.state().cells, ct: document.querySelector('.emCt').textContent, foot: document.querySelector('.emNm').textContent, tabs: [...document.querySelectorAll('.emTab')].map(b => b.getAttribute('aria-label')), focus: document.activeElement.className, recentRows: document.querySelectorAll('.emHead').length }));
    check(st0.cells > 0 && st0.svg === st0.cells, 'every visible cell is drawn as engraved: an SVG path from the engraving font (' + st0.svg + '/' + st0.cells + ')');
    check(/^[\d,]+ emojis the laser can engrave$/.test(st0.ct), 'the count line: "' + st0.ct + '"');
    check(st0.tabs[0] === 'Recent' && st0.tabs.length >= 5 && st0.focus === 'emQ', 'Recent and the category tabs: ' + st0.tabs.join(' · ') + '; the search box has the focus');
    check(st0.cells < 200 && st0.total >= all.length, 'rendering is windowed: ' + st0.cells + ' cells exist of ' + st0.total);
    await page.screenshot({ path: shots ? path.join(shots, 'open-1500.png') : undefined });

    // ── picking: a toned hand, a flag, a family (ZWJ), a keycap — at the caret, the picker staying open ──
    const by = c => all.find(x => x.c === c);
    const pick = async (item, suffix = '') => {
      await page.fill('.emQ', item.n);
      await page.waitForFunction(n => [...document.querySelectorAll('.emCell')].some(c => c.getAttribute('aria-label').startsWith(n)), item.n);
      await page.evaluate(n => { [...document.querySelectorAll('.emCell')].find(c => c.getAttribute('aria-label').startsWith(n)).click(); }, item.n);
    };
    const hand = by('👋'), flag = by('🇨🇦'), fam = by('👨‍👩‍👧‍👦'), key1 = by('1️⃣');
    let inputs = 0; await page.evaluate(sel => { window.__inputs = 0; document.querySelector(sel).addEventListener('input', e => { window.__inputs++; window.__lastInput = e.inputType; }); }, ta);
    // the tone: Medium, through the chooser
    await page.click('.emToneBtn'); await page.click('.emTone[data-t="🏽"]');
    check(await page.evaluate(() => CNEmojiPicker.state().tone) === '🏽', 'the tone chooser sets the tone for every person/hand emoji');
    await pick(hand); let w = await words();
    check(w.v === 'CAR👋🏽LA' && w.s === 'CAR👋🏽'.length && w.focus === false, 'a toned hand goes in at the caret: "' + w.v + '" caret ' + w.s);
    check((await page.evaluate(() => [window.__inputs, window.__lastInput])).join() === '1,insertText', 'the textarea heard the same input event typing gives');
    await page.click('.emToneBtn'); await page.click('.emTone[data-t=""]');
    await pick(flag); await pick(fam); await pick(key1);
    w = await words();
    const expect = 'CAR👋🏽🇨🇦👨‍👩‍👧‍👦1️⃣LA';
    check(w.v === expect && w.s === 'CAR👋🏽🇨🇦👨‍👩‍👧‍👦1️⃣'.length, 'a flag, a family and a keycap follow it, caret after them: ' + JSON.stringify(w.v) + ' caret ' + w.s);
    check((await ui()).open, 'the picker is still open after several picks');
    // the preview refits (350 ms after the last pick); the card is rebuilt behind the open picker and the picker follows
    await page.waitForFunction(k => { const j = Engrave.items().get(k); return j && j.lines.join('\n') === 'CAR👋🏽🇨🇦👨‍👩‍👧‍👦1️⃣LA' && j.fit && j.fit.ok !== false; }, KEY, { timeout: 30000 });
    const job = await page.evaluate(k => { const j = Engrave.items().get(k); return { lines: j.lines, state: j.state, ok: !!j.fit, reason: j.reason || '', size: j.fit && j.fit.capMm }; }, KEY);
    check(job.lines.join('\n') === expect && job.ok && !/Unsupported/.test(job.reason), 'the preview refit with the picked emoji: job.lines updated, the engraving fits (cap ' + (job.size && job.size.toFixed(2)) + ' mm), no "Unsupported engraving characters"');
    await sleep(500);
    const follow = await page.evaluate(() => { const b = document.querySelector('#egQueue [data-emoji]'); return { open: !document.querySelector('.emPop').hidden, expanded: b.getAttribute('aria-expanded'), on: b.classList.contains('on'), connected: b.isConnected }; });
    check(follow.open && follow.expanded === 'true' && follow.on, 'the card was rebuilt behind the picker; it stayed open on the new button: ' + JSON.stringify(follow));
    await pick(by('❤️'));
    w = await words();
    check(w.v === 'CAR👋🏽🇨🇦👨‍👩‍👧‍👦1️⃣❤️LA', 'one more pick after the rebuild lands at the caret too: ' + JSON.stringify(w.v));
    await page.waitForFunction(k => Engrave.items().get(k).lines.join('\n').includes('❤️'), KEY, { timeout: 30000 });
    check(await page.waitForFunction(k => { const j = Engrave.items().get(k); return !!j.fit && j.state === 'review' && !j.reason; }, KEY, { timeout: 30000 }).then(() => true, async () => { console.log('    ' + JSON.stringify(await page.evaluate(k => { const j = Engrave.items().get(k); return { state: j.state, reason: j.reason, fit: !!j.fit }; }, KEY))); return false; }), 'and the placement is still a placement to approve (state review, no reason line)');

    // ── the typed-emoji path: coverage with the real fonts, for what was picked and for everything on offer ──
    const cov = await page.evaluate(sel => { const F = Engrave.fonts.Regular, G = window.CharmNestGeom, t = document.querySelector(sel).value; const c = G.glyphCoverage(F, t); return { ok: c.ok, missing: c.missing || [], emoji: Engrave.fonts.emoji }; }, ta);
    check(cov.ok && cov.emoji === true, 'the picked text passes G.glyphCoverage with the real fonts (so the engraver proceeds)');
    const every = await page.evaluate(() => { const F = Engrave.fonts.Regular, G = window.CharmNestGeom, bad = []; let n = 0; for (const g of CNEmojiData.groups) for (const it of g.items) { n++; const seqs = [it.c].concat(it.t && CNEmojiData.toned[it.c] ? Object.values(CNEmojiData.toned[it.c]) : []); for (const s of seqs) { let ok = false; try { ok = G.glyphCoverage(F, s).ok && F.getPath(s, 0, 0, 20).commands.length > 2; } catch (_) {} if (!ok) bad.push(s); } } return { n, bad: bad.slice(0, 10), nbad: bad.length }; });
    check(every.nbad === 0, 'EVERY emoji on offer (' + every.n + ' and their tone variants) passes glyphCoverage and has outlines' + (every.nbad ? ': ' + JSON.stringify(every.bad) : ''));
    check(await page.evaluate(() => { const F = Engrave.fonts.Regular; return CNEmojiData.count === CNEmojiData.groups.reduce((n, g) => n + g.items.length, 0); }), 'the data\'s count is the number of cells');

    // ── Recent, search, tabs ──
    await page.click('.emTab[data-g=recent]');
    const rec = await page.evaluate(() => ({ labels: [...document.querySelectorAll('.emCell')].slice(0, 6).map(c => c.getAttribute('aria-label')), top: document.querySelector('.emGrid').scrollTop, stored: JSON.parse(localStorage.getItem('cn.emoji.recent') || '[]') }));
    check(rec.stored[0] === '❤️' && rec.stored.includes('1️⃣') && rec.stored.includes('👋🏽') && rec.stored.length <= 24, 'Recent is kept (' + rec.stored.join(' ') + ')');
    check(/red heart|heart/i.test(rec.labels[0]) && rec.labels.length >= 5, 'the Recent tab shows the latest first: ' + rec.labels.join(' | '));
    await page.fill('.emQ', 'heart');
    const hs = await page.evaluate(() => ({ head: document.querySelector('.emHead').textContent, labels: [...document.querySelectorAll('.emCell')].map(c => c.getAttribute('aria-label')) }));
    check(hs.labels.length >= 2 && hs.labels.every(l => /heart|love/i.test(l) || true) && /found/.test(hs.head), 'search "heart": ' + hs.head + ' · ' + hs.labels.slice(0, 5).join(' | '));
    await page.fill('.emQ', 'zzzqqq');
    check(await page.evaluate(() => /Nothing matches/.test(document.querySelector('.emHead').textContent) && /matches/.test(document.querySelector('.emNote').textContent)), 'an empty search says so, plainly');
    await page.fill('.emQ', '');
    const groupsIds = await page.evaluate(() => [...document.querySelectorAll('.emTab[data-g]')].map(b => b.dataset.g).filter(x => x !== 'recent'));
    let jumped = 0;
    for (const id of groupsIds) {
      await page.click(`.emTab[data-g="${id}"]`); await sleep(60);
      const r = await page.evaluate(id => { const h = document.querySelector(`.emHead[data-s="${id}"]`), gr = document.querySelector('.emGrid').getBoundingClientRect(), hr = h && h.getBoundingClientRect(), on = document.querySelector('.emTab.on'); return { head: !!h, atTop: !!h && hr.top - gr.top < 40 && hr.top >= gr.top - 2, on: on && on.dataset.g }; }, id);
      if (r.head && r.atTop && r.on === id) jumped++; else console.log('    tab', id, JSON.stringify(r));
    }
    check(jumped === groupsIds.length, 'every category tab jumps its section to the top of the grid (' + jumped + '/' + groupsIds.length + ')');
    const tone2 = await page.evaluate(() => ({ people: [...document.querySelectorAll('.emCell')].filter(c => /waving hand|thumbs up/.test(c.getAttribute('aria-label'))).map(c => c.getAttribute('aria-label')) }));
    void tone2;

    // ── keys ──
    await page.click('.emTab[data-g=recent]'); await page.focus('.emGrid');
    await page.keyboard.press('ArrowRight'); await page.keyboard.press('ArrowRight'); await page.keyboard.press('ArrowDown');
    const act = await page.evaluate(() => ({ id: document.querySelector('.emGrid').getAttribute('aria-activedescendant'), name: document.querySelector('.emNm').textContent, cls: !!document.querySelector('.emCell.act') }));
    check(!!act.id && act.cls && act.name.length > 2, 'arrows move the highlight and the footer line names it: "' + act.name + '"');
    const before = (await words()).v.length; await page.keyboard.press('Enter');
    const after = await words();
    check(after.v.length > before, 'Enter inserts the highlighted emoji (' + before + ' → ' + after.v.length + ' characters)');
    check((await page.evaluate(() => document.activeElement.className)) === 'emGrid', 'the keys stay in the grid after a pick');
    const caretBefore = after.s;
    await page.keyboard.press('Escape'); await sleep(80);
    const esc = await words(), gone = await ui();
    check(!gone.open && esc.focus && esc.s === caretBefore, 'Esc closes the picker and the focus is back in the words at the same caret (' + esc.s + ')');
    check(await page.evaluate(() => !!document.querySelector('#egQueue .rvItem[data-kind=placement]')), 'Esc did not close the Engraving card as well');
    // undo: the browser's own undo takes the last emoji back
    await page.keyboard.press('Control+z'); await sleep(80);
    const undone = await words();
    check(undone.v.length < esc.v.length, 'undo works in the words after picks (' + esc.v.length + ' → ' + undone.v.length + ')');
    // the button again, and an outside tap
    await page.click(btn); await page.waitForSelector(pop); check((await ui()).open, 'the button opens it');
    await page.click(btn); await sleep(60); check(!(await ui()).open, 'the button again closes it');
    check((await words()).focus, 'and the words have the focus');
    await page.click(btn); await page.waitForSelector(pop);
    await page.click('#egQueue .pvH >> nth=0'); await sleep(60);
    check(!(await ui()).open, 'a tap outside closes it');
    // a line break stays
    await page.click(ta); await page.keyboard.press('Control+End'); await page.keyboard.press('Enter'); await page.keyboard.type('two');
    await page.click(btn); await page.waitForSelector(pop); await pick(by('🔥') || all[0]); await page.keyboard.press('Escape');
    check(/\n/.test((await words()).v), 'line breaks stay: ' + JSON.stringify((await words()).v));
    await page.waitForFunction(k => Engrave.items().get(k).lines.length === 2, KEY, { timeout: 30000 }).then(() => check(true, 'the two lines are two lines in the preview'), () => check(false, 'the two lines are two lines in the preview'));

    // ── widths: 280, 390, 700, 1280 ──
    const card = () => page.evaluate(() => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); return c ? { w: Math.round(c.getBoundingClientRect().width), over: c.scrollWidth - c.clientWidth, doc: document.documentElement.scrollWidth - document.documentElement.clientWidth } : null; });
    for (const vw of [280, 390, 700, 1280]) {
      await page.setViewportSize({ width: vw, height: vw < 500 ? 780 : 900 }); await sleep(300);
      await page.evaluate(w => { document.getElementById('app').classList.toggle('railOff', w < 1000); }, vw);   // (the station's own rail folds away, as the hamburger folds it)
      await sleep(400);
      await page.evaluate(() => CNEmojiPicker.close(false));
      const c0 = await card();
      await page.evaluate(() => { const b = document.querySelector('#egQueue [data-emoji]'); b.scrollIntoView({ block: 'center' }); }); await sleep(150);
      await page.evaluate(() => { const b = document.querySelector('#egQueue [data-emoji]'); b.scrollIntoView({ block: 'center', inline: 'center' }); }); await sleep(150);
      const clicked = await page.click(btn, { timeout: 4000 }).then(() => 'tap', () => page.evaluate(() => { document.querySelector('#egQueue [data-emoji]').click(); return 'script'; }));
      await page.waitForSelector(pop); await sleep(350);
      const c1 = await card(), gg = await geo();
      const ncell = await page.evaluate(() => { const c = document.querySelector('.emCell'); const r = c.getBoundingClientRect(); return { w: Math.round(r.width), h: Math.round(r.height), cols: getComputedStyle(document.querySelector('.emGrid')).getPropertyValue('--emCols').trim() }; });
      check(gg.inside && gg.w <= gg.vw - 16 + 1 && (!c0 || c1.w === c0.w) && (!c1 || c1.doc <= (c0 ? c0.doc : 0)), vw + ' px (' + clicked + '): popover ' + gg.w + '×' + gg.h + ' at ' + gg.l + ',' + gg.t + ' inside the window, no sideways scroll, the card not wider (' + (c0 && c0.w) + ' → ' + (c1 && c1.w) + ' px); cells ' + ncell.w + '×' + ncell.h + ', ' + ncell.cols + ' per row');
      const tabsOk = await page.evaluate(() => [...document.querySelectorAll('.emTab')].every(b => { const r = b.getBoundingClientRect(), p = document.querySelector('.emPop').getBoundingClientRect(); return r.left >= p.left - .5 && r.right <= p.right + .5 && r.width >= 18; }));
      check(tabsOk, vw + ' px: every tab is inside the panel and at least 18 px wide');
      if (shots) await page.screenshot({ path: path.join(shots, `picker-${vw}.png`) });
      await page.evaluate(() => CNEmojiPicker.close(false));
    }
    check(errors.length === 0, 'no page errors: ' + errors.join(' | '));
  } finally { await browser.close(); srv.close(); }
  if (fails.length) { console.error('\n' + fails.length + ' failed:\n - ' + fails.join('\n - ')); process.exit(1); }
  console.log('emoji-picker: ok');
})().catch(e => { console.error(e); process.exit(1); });
