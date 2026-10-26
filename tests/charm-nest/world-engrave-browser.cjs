// WORLD-ENGRAVE in the real page (the second of the two suites; the first is world-engrave.cjs). Headless Chromium against the local fake site (bridge-server.cjs, nothing live, no Etsy, no AI):
// a real master file and a real Auto run over orders whose personalization is Arabic, Hebrew (with Latin and a number), Hindi, Japanese, Korean, Traditional Chinese, and Latin only.
//   A  a run of Latin / accent / heart words: the page asks for NO script asset (no manifest, no engine, no font file) and engraves as before
//   B  a run of script words: each job is placed and waiting in the engraving review, set in the script font the manifest names, the card says "Set in Noto Sans ...", the background worker was given the engine
//      and each font once, the page draws what the worker fitted (the same outlines), exactly the fonts the words needed were fetched (once each, 200), the approved back is written, no page error
//   SHOTS=<dir> node tests/charm-nest/world-engrave-browser.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>; without a browser the suite is skipped)
const fs = require('fs'), path = require('path'), os = require('os');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const { buildMaster } = require('./fixture-master.cjs');

const day = Math.floor(Date.now() / 1000);
const tx = (rid, i, listing, sku, vars, text) => ({ transaction_id: Number(`${rid}${i}`), listing_id: listing, receipt_id: rid, sku, title: `${sku} charm`, quantity: 1, expected_ship_date: day + 86400, is_personalized: true,
  variations: [{ formatted_name: 'Metal', formatted_value: 'Sterling Silver' }].concat(vars, [{ formatted_name: 'Personalization', formatted_value: text }]) });
const receipt = (rid, txs) => ({ receipt_id: rid, order_number: rid, name: 'Buyer ' + rid, country_iso: 'US', city: 'Austin', message_from_buyer: '', update_timestamp: day, create_timestamp: day - 3600, status: 'Paid', is_shipped: false, transactions: txs });
const STYLISH = [{ formatted_name: 'Font', formatted_value: 'Stylish' }];
const A = [  // Latin only: nothing of the script machinery may be asked for
  { rid: '3622000001', listing: 1999000222, text: 'CARLA' }, { rid: '3622000002', listing: 1999000222, text: 'Café ♡ 2024' }, { rid: '3622000003', listing: 234758391, vars: STYLISH, text: 'Zoë & Łukasz' }];
const B = [
  { rid: '3622000011', listing: 234758391, vars: STYLISH, text: 'مرحبا', fonts: ['arabic'], font: 'playwrite-us-trad', note: /Set in Noto Sans Arabic \(Playwrite US Trad has no letters for it\)/ },
  { rid: '3622000012', listing: 1999000222, text: 'Anna שלום 2024', fonts: ['hebrew'], font: 'source-sans-3', note: /Set in Noto Sans Hebrew/ },
  { rid: '3622000013', listing: 1999000222, text: 'नमस्ते', fonts: ['devanagari'], font: 'source-sans-3', note: /Set in Noto Sans Devanagari/ },
  { rid: '3622000014', listing: 1999000222, text: '日本語のテキスト', fonts: ['cjk-jp'], font: 'source-sans-3', note: /Set in Noto Sans JP/ },
  { rid: '3622000015', listing: 1999000222, text: '안녕하세요', fonts: ['cjk-kr'], font: 'source-sans-3', note: /Set in Noto Sans KR/ },
  { rid: '3622000016', listing: 1999000222, text: '愛與和平', fonts: ['cjk-tc'], font: 'source-sans-3', note: /Set in Noto Sans TC/ }];
const keyOf = s => s.rid + '_' + s.rid + '1';
const agentResults = { engraveIntent: o => { const txt = String(o.messages[0].content[0].text), m = /Personalisation field: (\[.*?\])/.exec(txt); let p = []; try { p = JSON.parse(m ? m[1] : '[]'); } catch (_) {} return { engrave: p.length > 0, text: p.join('\n'), source: 'personalization', sourceQuote: p[0] || '', requests: { side: 'back', font: null, handwriting: false, image: false }, questions: [], confidence: 0.99 }; } };

(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { try { ({ chromium } = require('/opt/node22/lib/node_modules/playwright/node_modules/playwright-core')); } catch (__) { console.log('  – no playwright-core: not run'); return; } }
  const exe = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome'; if (!fs.existsSync(exe)) { console.log('  – no Chromium: not run'); return; }
  const shots = process.env.SHOTS || ''; if (shots) fs.mkdirSync(shots, { recursive: true });
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cn-world-')), masterPath = path.join(tmp, 'BRITES-master.ai');
  await buildMaster(masterPath, { count: 4, edge: false });
  const fails = [], errors = [];
  const check = (ok, msg) => { if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };

  async function run(label, scenes, after) {
    console.log('\n' + label);
    const receipts = scenes.map(s => receipt(s.rid, [tx(s.rid, 1, s.listing, 'BR-TST-04', s.vars || [], s.text)]));
    const srv = await start({ receipts, agentResults }); const { sorterOrigin, stationOrigin } = srv;
    const gets = [], browser = await chromium.launch({ executablePath: exe, args: ['--no-sandbox'] });   // (a browser, and so a cache, per run)
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
        // what the page sends its background workers (the engine and each script font go once)
        const W = window.Worker; window.__wmsgs = [];
        window.Worker = class extends W { constructor(...a) { super(...a); const p = this.postMessage.bind(this); this.postMessage = (m, t) => { try { if (m && m.type) window.__wmsgs.push(m.type === 'scripts' ? 'scripts:' + (m.fonts || []).map(f => f.id).join(',') + (m.wasm ? '+wasm' : '') : m.type); } catch (_) {} return p(m, t); }; } };
      }, { station: stationOrigin, sorter: sorterOrigin });
      const page = await ctx.newPage(); page.setDefaultTimeout(30000);
      page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
      page.on('response', r => { const u = r.url(); if (/\/vendor\/fonts\/scripts\/|script-fonts\.json|hb\.wasm/.test(u)) gets.push(u.replace(/^.*?\/(vendor\/|charm-nest)/, '$1').replace(/\?.*/, '') + ' ' + r.status()); });
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
        if (r && r.status === 'processed' && jobs.length >= scenes.length && !jobs.some(s => ['ready', 'classify', 'fitting', 'reclassify'].includes(s))) break;
        if (Date.now() - t0 > 300000) throw new Error('the run never reached the engraving review: ' + JSON.stringify([r, jobs]));
        await page.waitForTimeout(500);
      }
      const info = await page.evaluate(keys => keys.map(k => { const j = Engrave.items().get(k); if (!j) return null;
        let same = null; try {   /* the line gap is read back from the fit's own layout */ const f = j.fit, font = Engrave.fontForWeight(f.weight, j), gap = f.lines.length > 1 ? (f.layout.totalH - f.lines.length * f.layout.capPt) / (f.lines.length - 1) / f.size : .216, L = CharmNestGeom.layoutLines(f.lines, font, f.size, gap, f.angle || 0, f.centre); same = L.cmds.length === f.cmds.length && L.cmds.every((c, i) => c.type === f.cmds[i].type && ['x', 'y', 'x1', 'y1', 'x2', 'y2'].every(k => (c[k] === undefined) === (f.cmds[i][k] === undefined) && (c[k] === undefined || Math.abs(c[k] - f.cmds[i][k]) < 1e-6))); }   /* (to a millionth of a point: the line gap is read back from the layout) */ catch (e) { same = 'error: ' + e.message; }
        return { state: j.state, reason: j.reason, text: j.text, lineInput: j.lineInput, lines: j.fit && j.fit.lines, fontKey: j.fontKey, fontName: j.fontName, scriptFonts: (j.scriptFonts || []).map(f => f.id), ok: !!(j.fit && j.fit.ok), cmds: j.fit && j.fit.cmds && j.fit.cmds.length, capMm: j.fit && j.fit.capMm, same }; }), scenes.map(keyOf));
      scenes.forEach((s, i) => { const j = info[i]; check(!!j, `${s.rid}: an engraving job exists`); if (j) check(j.state === 'review' && j.ok && j.cmds > 8, `${s.rid} (${s.text}): placed and waiting in the engraving review, not blocked (state ${j.state}${j.reason ? ': ' + j.reason : ''}, ${j.cmds} segments, cap ${(j.capMm || 0).toFixed(2)} mm)`); });
      const msgs = await page.evaluate(() => window.__wmsgs.slice());
      await after({ page, info, scenes, gets, msgs, sorterOrigin });
    } finally { await browser.close().catch(() => {}); srv.close(); }
  }

  {
    await run('A · Latin, accents, a heart: no script asset is asked for', A, async ({ info, scenes, gets, msgs }) => {
      // (ENGAUTO, charm-nest-engrave-card.js wordsOfJob: job.lines / job.text are the fit's layout, "Café ♡" / "2024" on two lines; job.lineInput keeps the words as
      //  they were given, and that is what the card shows. The layout may only break the words where a space was: rejoined, they are the words. 26 Oct 2026)
      scenes.forEach((s, i) => { const j = info[i]; check(j && j.scriptFonts.length === 0 && Array.isArray(j.lineInput) && j.lineInput.join('\n') === s.text && j.text.split('\n').join(' ') === s.text, `${s.rid}: words kept as typed, no script font (given ${JSON.stringify(j && j.lineInput)}, laid out ${JSON.stringify(j && j.text)})`); });
      check(gets.length === 0, 'the page requested no manifest, no engine file, no script font and no shaper script on demand (' + JSON.stringify(gets) + ')');
      check(!msgs.some(m => /^scripts/.test(m)), 'the background worker was sent no script font (' + msgs.join(' ') + ')');
    });
    // (run B: a fresh browser, so a fresh cache)
    await run('B · Arabic, Hebrew, Hindi, Japanese, Korean, Traditional Chinese', B, async ({ page, info, scenes, gets, msgs }) => {
      scenes.forEach((s, i) => {
        const j = info[i]; if (!j) return;
        check(JSON.stringify(j.scriptFonts) === JSON.stringify(s.fonts) && j.fontKey === s.font, `${s.rid}: set in ${s.fonts.join(' + ')} (the job says ${JSON.stringify(j.scriptFonts)}), the line's own font ${j.fontKey}`);
        check(j.same === true, `${s.rid}: the page draws exactly what the background worker fitted (${j.same})`);
      });
      const uniq = a => [...new Set(a)].sort();
      const assets = gets.filter(g => /vendor\/fonts\/scripts\//.test(g));
      console.log('    requests: ' + gets.join(', ')); console.log('    worker messages: ' + msgs.join(' | '));
      check(JSON.stringify(uniq(assets)) === JSON.stringify(uniq(['NotoSansArabic', 'NotoSansHebrew', 'NotoSansDevanagari', 'NotoSansJP', 'NotoSansKR', 'NotoSansSC', 'NotoSansTC'].map(n => `vendor/fonts/scripts/${n}-Regular.ttf 200`))), 'exactly the fonts the words needed were fetched (Simplified too, for the Traditional line it was tried first), none of the others');
      check(assets.length === uniq(assets).length, 'each font was fetched once');
      check(gets.filter(g => /hb\.wasm 200/.test(g)).length === 1 && gets.filter(g => /script-fonts\.json 200/.test(g)).length === 1, 'the engine and the manifest were fetched once each');
      check(!gets.some(g => / (4|5)\d\d$/.test(g)), 'nothing failed');
      const scriptMsgs = msgs.filter(m => /^scripts/.test(m)), sentIds = scriptMsgs.flatMap(m => m.replace(/^scripts:/, '').replace(/\+wasm$/, '').split(',')).filter(Boolean);
      check(sentIds.length === new Set(sentIds).size && scriptMsgs.filter(m => /\+wasm/.test(m)).length === 1, 'the worker got each font once and the engine once: ' + scriptMsgs.join(' '));
      check(['arabic', 'hebrew', 'devanagari', 'cjk-jp', 'cjk-kr', 'cjk-tc'].every(id => sentIds.includes(id)), 'the worker had every font its jobs needed');
      // the cards say it
      for (const s of scenes) {
        await page.evaluate(k => { CN.setMode('engrave'); Engrave.restoreView({ tab: 'place', chosen: true, list: false, focus: k }); Engrave.render(); }, keyOf(s));
        await page.waitForSelector('#egQueue .rvItem[data-kind=placement]');
        const text = await page.evaluate(() => document.querySelector('#egQueue .rvItem').innerText.replace(/\s+/g, ' '));
        check(s.note.test(text), `${s.rid}: the card says "${(s.note.exec(text) || [''])[0] || 'nothing: ' + text.slice(0, 300)}"`);
        if (shots) await (await page.$('#egQueue .rvItem')).screenshot({ path: path.join(shots, `card-${s.rid}.png`) });
      }
      // approve the Hebrew + Latin + number line: its back is written
      const ap = await page.evaluate(async k => { const j = Engrave.items().get(k); try { await Engrave.approve(j, 'Test Operator'); } catch (e) { return { error: String(e && e.message || e) }; } return { state: j.state, reason: j.reason }; }, keyOf(scenes[1]));
      check(!ap.error && ['approved', 'written'].includes(ap.state), 'the Hebrew line is approved and its back is written: ' + JSON.stringify(ap));
    });
  }
  check(errors.length === 0, 'no page errors: ' + errors.slice(0, 2).join(' | '));
  if (fails.length) { console.error('\n' + fails.length + ' failed:\n - ' + fails.join('\n - ')); process.exit(1); }
  console.log('world-engrave-browser: ok');
})().catch(e => { console.error(e); process.exit(1); });
