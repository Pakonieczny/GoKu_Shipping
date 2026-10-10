// FONTMAP in the real page (the second of the two suites; the first is font-map.cjs). Headless Chromium against the local fake site (bridge-server.cjs, nothing live, no Etsy, no AI):
// a real master file, a real Auto run over five orders, each with a different font, up to the Engraving card, and then the first one approved:
//   · Mini Tag listing 1714117116, "Font Choice (see listing photo for choices): Vibur"  -> Playwrite US Modern (the listing's one font)
//   · listing 234758391, "Font: Stylish"                                               -> Playwrite US Trad
//   · listing 1008014571, "Fonts: 16"/ Typewriter"                                      -> Crimson Text (two weights: Semibold under 2.2 mm)
//   · a listing with "Font: Cooper Black" (a word the shop does not offer)              -> Source Sans 3, "Requested font: Cooper Black"
//   · no drop-down, the buyer's note says "font: Pristina"                                 -> Marck Script
// What it proves in the page itself: the font files are fetched from the site and parsed (loadFontSet), the background worker is given each font once and fits with it, the piece's
// card says "Font: Stylish -> Playwrite US Trad", the fitted words are drawn from that font (not Source Sans 3), no job is blocked, and the approved back record names its font.
//   SHOTS=<dir> node tests/charm-nest/font-map-browser.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>; without a browser the suite is skipped)
const fs = require('fs'), path = require('path'), os = require('os');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const { buildMaster } = require('./fixture-master.cjs');

const day = Math.floor(Date.now() / 1000);
const tx = (rid, i, listing, sku, vars, text) => ({ transaction_id: Number(`${rid}${i}`), listing_id: listing, receipt_id: rid, sku, title: `${sku} charm`, quantity: 1, expected_ship_date: day + 86400, is_personalized: true,
  variations: [{ formatted_name: 'Metal', formatted_value: 'Sterling Silver' }].concat(vars, [{ formatted_name: 'Personalization', formatted_value: text || 'CARLA' }]) });
const receipt = (rid, txs) => ({ receipt_id: rid, order_number: rid, name: 'Buyer ' + rid, country_iso: 'US', city: 'Austin', message_from_buyer: '', update_timestamp: day, create_timestamp: day - 3600, status: 'Paid', is_shipped: false, transactions: txs });
const SCENES = [
  { rid: '3521300001', listing: 1714117116, name: 'Font Choice (see listing photo for choices)', value: 'Vibur', font: 'playwrite-us-modern', fontName: 'Playwrite US Modern', card: /Font\s+Vibur → Playwrite US Modern \(this listing engraves in one font\)/i, shot: 'tag-vibur' },
  { rid: '3521300002', listing: 234758391, name: 'Font', value: 'Stylish', font: 'playwrite-us-trad', fontName: 'Playwrite US Trad', card: /Font\s+Stylish → Playwrite US Trad/i, shot: 'disc-stylish' },
  { rid: '3521300003', listing: 1008014571, name: 'Fonts', value: '16"/ Typewriter', font: 'crimson-text', fontName: 'Crimson Text', card: /Font\s+Typewriter → Crimson Text/i, shot: 'disc-typewriter' },
  { rid: '3521300004', listing: 1999000111, name: 'Font', value: 'Cooper Black', font: 'source-sans-3', fontName: 'Source Sans 3', card: /Requested font: Cooper Black \(engraved in Source Sans 3\)/, shot: 'unknown-cooper' },
  { rid: '3521300005', listing: 1999000222, text: 'ANNA', note: 'Pristina', font: 'marck-script', fontName: 'Marck Script', card: /Font\s+Pristina → Marck Script \(asked in the note\)/i, shot: 'note-pristina' }   // no drop-down: the buyer wrote "font: Pristina" in the note
];
const receipts = SCENES.map(s => receipt(s.rid, [tx(s.rid, 1, s.listing, 'BR-TST-04', s.name ? [{ formatted_name: s.name, formatted_value: s.value }] : [], s.text)]));
const keyOf = s => s.rid + '_' + s.rid + '1';
const agentResults = { engraveIntent: o => { const txt = String(o.messages[0].content[0].text), m = /Personalisation field: (\[.*?\])/.exec(txt); let p = []; try { p = JSON.parse(m ? m[1] : '[]'); } catch (_) {} const req = { side: 'back', font: p[0] === 'ANNA' ? 'Pristina' : null, handwriting: false, image: false }; return { engrave: p.length > 0, text: p.join('\n'), source: 'personalization', sourceQuote: p[0] || '', requests: req, questions: [], confidence: 0.99 }; } };

(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { try { ({ chromium } = require('/opt/node22/lib/node_modules/playwright/node_modules/playwright-core')); } catch (__) { console.log('  – no playwright-core: not run'); return; } }
  const exe = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome'; if (!fs.existsSync(exe)) { console.log('  – no Chromium: not run'); return; }
  const shots = process.env.SHOTS || ''; if (shots) fs.mkdirSync(shots, { recursive: true });
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cn-fontmap-')), masterPath = path.join(tmp, 'BRITES-master.ai');
  await buildMaster(masterPath, { count: 4, edge: false });
  const srv = await start({ receipts, agentResults });
  const { sorterOrigin, stationOrigin } = srv;
  const browser = await chromium.launch({ executablePath: exe, args: ['--no-sandbox'] });
  const fails = [], errors = [], fontGets = [];
  const check = (ok, msg) => { if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
  try {
    const ctx = await browser.newContext({ viewport: { width: 1500, height: 1000 }, deviceScaleFactor: 2 });
    const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
    await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
    await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
    await ctx.route(/fonts\.googleapis|fonts\.gstatic/, r => r.fulfill({ status: 200, contentType: 'text/css', body: '' }));
    await ctx.addInitScript(({ station, sorter }) => {
      if (location.origin === station) { localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200)); localStorage.setItem('employee_name', 'Test Operator'); }
      if (location.origin === sorter) { localStorage.setItem('cn.employee', 'Test Operator'); localStorage.setItem('cn.tour.seen', '1'); }
      window.confirm = () => true; window.alert = () => {};
    }, { station: stationOrigin, sorter: sorterOrigin });
    const page = await ctx.newPage();
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    page.on('response', r => { const u = r.url(); if (/\/vendor\/fonts\/[^/]+\.(ttf|otf)$/.test(u)) fontGets.push(u.split('/').pop() + ' ' + r.status()); });
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
      if (r && r.status === 'processed' && jobs.length >= SCENES.length && !jobs.some(s => ['ready', 'classify', 'fitting', 'reclassify'].includes(s))) break;
      if (Date.now() - t0 > 300000) throw new Error('the run never reached the engraving review: ' + JSON.stringify([r, jobs]));
      await page.waitForTimeout(500);
    }
    const info = await page.evaluate(keys => keys.map(k => { const j = Engrave.items().get(k); return j && { state: j.state, reason: j.reason, fontKey: j.fontKey, fontName: j.fontName, fontAsked: j.fontAsked, fallback: j.fontFallback || null, ok: !!(j.fit && j.fit.ok), weight: j.fit && j.fit.weight, capMm: j.fit && j.fit.capMm, cmds: j.fit && j.fit.cmds && j.fit.cmds.length, sig: j.fit && JSON.stringify(j.fit.cmds).length, specFont: j.row.spec.font && j.row.spec.font.id }; }), SCENES.map(keyOf));
    SCENES.forEach((s, i) => {
      const j = info[i]; check(!!j, `${s.rid}: an engraving job exists`); if (!j) return;
      check(j.state === 'review' && j.ok, `${s.rid} (${s.value}): placed and waiting in the engraving review, not blocked (state ${j.state}${j.reason ? ': ' + j.reason : ''})`);
      check(j.fontKey === s.font && j.fontName === s.fontName && !j.fallback, `${s.rid}: engraves in ${s.fontName} (job font ${j.fontName})`);
    });
    // the drawn words are the font's own: the same text in different fonts is a different drawing
    check(new Set(info.slice(0, 4).map(j => j.sig)).size === 4, 'the fonts give different drawings of the same word CARLA: ' + info.slice(0, 4).map(j => j.cmds + ' segments').join(', '));
    check(info[0].weight === 'Regular', 'Playwrite US Modern has one weight: the record says Regular (' + info[0].weight + ')');
    check(info[2].weight === (info[2].capMm < 2.2 ? 'Semibold' : 'Regular'), 'Crimson Text follows the weight rule (Semibold under 2.2 mm cap height): ' + info[2].weight + ' at cap ' + (info[2].capMm || 0).toFixed(2) + ' mm');
    const got = [...new Set(fontGets)]; console.log('    font files fetched by the page: ' + got.join(', '));
    check(['PlaywriteUSModern-Regular.ttf 200', 'PlaywriteUSTrad-Regular.ttf 200', 'CrimsonText-Regular.ttf 200', 'CrimsonText-SemiBold.ttf 200'].every(f => got.includes(f)) && !got.some(f => / (4|5)\d\d$/.test(f)), 'the fonts the pieces needed were served (200)');
    check(got.includes('MarckScript-Regular.ttf 200') && !got.some(f => /^(Mynerve|Jost|AlegreyaSans)/.test(f)), 'Marck Script (the note) was served too; the fonts no piece uses were not downloaded');

    // the card of each piece says which font it is engraved in
    for (const s of SCENES) {
      await page.evaluate(k => { CN.setMode('engrave'); Engrave.restoreView({ tab: 'place', chosen: true, list: false, focus: k }); Engrave.render(); }, keyOf(s));
      await page.waitForSelector('#egQueue .rvItem[data-kind=placement] textarea[data-f=words]');
      const text = await page.evaluate(() => document.querySelector('#egQueue .rvItem').innerText.replace(/\s+/g, ' '));
      check(s.card.test(text), `${s.rid}: the engraving card says "${(s.card.exec(text) || [''])[0] || 'nothing: ' + text.slice(text.indexOf('From the order'), text.indexOf('From the order') + 160)}"`);
      if (shots) await (await page.$('#egQueue .rvItem')).screenshot({ path: path.join(shots, `card-${s.shot}.png`) });
    }

    // the first piece approved: the saved back says its font, and the sheet's viewers can draw it in that font
    const ap = await page.evaluate(async k => { const j = Engrave.items().get(k); try { await Engrave.approve(j, 'Test Operator'); } catch (e) { return { error: String(e && e.message || e) }; } return { state: j.state, back: (j.backs || []).map(b => ({ font: b.font, fontKey: b.fontKey, weight: b.weight })), reason: j.reason }; }, keyOf(SCENES[0]));
    console.log('    approval: ' + JSON.stringify(ap));
    if (ap.error) check(false, 'approving the Vibur tag: ' + ap.error);
    else check(['approved', 'written'].includes(ap.state) && ap.back.length ? ap.back.every(b => b.font === 'Playwrite US Modern' && b.fontKey === 'playwrite-us-modern') : ['approved', 'written'].includes(ap.state), 'the approved back record names its font: ' + JSON.stringify(ap.back));
    check(errors.length === 0, 'no page errors: ' + errors.slice(0, 2).join(' | '));
  } finally { await browser.close(); srv.close(); }
  if (fails.length) { console.error('\n' + fails.length + ' failed:\n - ' + fails.join('\n - ')); process.exit(1); }
  console.log('font-map-browser: ok');
})().catch(e => { console.error(e); process.exit(1); });
