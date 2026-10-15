// MONOGRAM UI in the real page (MONOGRAMUI, Paul, 15 Oct 2026): the Engrave tab's Monograms, over the real sorter (charm-nest-1.html) in headless Chromium on the fake loopback site (bridge-server.cjs): no Etsy, no paid AI,
// nothing live. The master library holds three 26-letter alphabets named exactly as Paul's file names them (Monogram_Left_A … Monogram_Right_Z, upper-cased by the library: MONOGRAM_LEFT_A …), made from the
// vendored fonts with their real outlines and counters (_monogramAlphabets.cjs); orders go through the real intake (Orders.interpretAll).
//   1  the orders: the two monogram lines (4200683017 "APV", 4202783520 "O S R") and a title that says monogram with three letters are custom monograms; a bracelet with a name, a regular charm are not
//   2  the Engrave tab: a Monograms tab with the count and a card per line; the preview is drawn; letter 1 comes from the Left alphabet, 2 from the Middle, 3 from the Right (the fetched SKUs say so); the card says so
//      plainly ("The buyer wrote A P V · Left A · Middle P · Right V"); the Left / Middle / Right alphabets are found by their names (no picking); ONE connected charm, the centre bigger, the counters kept
//   3  editing a letter: spinner and progress bar, one debounced remake, the newest edit always wins (a stale answer never overwrites), swap two neighbours, paste three letters, Esc takes an edit back
//   4  a missing letter is a plain message naming it; a letter file that fails to load once gives Try again and no stuck spinner; odd input ("A.P", "Jane Doe", 4 letters, digits, emoji) is a calm message and no build
//   5  Approve: Monogram APV.ai on the order's custom designs (one piece, the right size), changing the letters REPLACES it (never two, the metal kept), the same letters again change nothing
//   6  the Review card's Build monogram opens the right card; no alphabet in the library: the quiet setup with the picker (and a chosen alphabet is used); a missing right alphabet is said
//   7  keys (Tab order, Enter approves, Esc), a re-render never takes a caret away, switching tabs keeps the cards, phone width (390), reduced motion, a second tab, a reload keeps the approved design
//   Screenshots (1440 and 390 px) go to $SHOTS (default /tmp/mono-shots/ui-*.png, and ui2-* for the picture as the cut piece).
//   MONOPREVIEW (Paul, 15 Oct 2026): every run that renders (screenshots, pixels) is seeded with Paul's REAL alphabets (the fixture, through the local indexer), never letters made up by the test; the preview is the cut piece
//   (filled with the order's metal, one thin cut line round the outline and every counter) and a check fails if it were ever drawn from block letters again (ui2-1440-ready / building / warning / approved, ui2-390-ready, each also per order: -APV, -JQX, -OSR).
//   8  Paul's own file (fixtures/SKUs_MONOGRAM_2024-1015.ai, 78 letters of three embedded fonts) indexed by the local script into the fake library: the three families found, 8 words built from his real letters (one piece, joins, counters), one approved and read back as ONE piece
//   node tests/charm-nest/monogram-ui.cjs [playwright-core dir]      MONO_REAL=0 leaves Paul's own file out (stand-in alphabets, no pictures), MONO_FONTS=<dir with LEFT.ttf MIDDLE.ttf RIGHT.ttf> makes the stand-in alphabets from his fonts
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const Base = require('./_orderFactsWorld.cjs');
const A = require('./_monogramAlphabets.cjs');
const SHOTS = process.env.SHOTS || '/tmp/mono-shots';
const V = (name, value) => ({ name, value });
const MONO_TITLE = 'Monogram Necklace Personalized Jewelry Initials Pendant';
const FX = {
  apv: { rid: '4200683017', sku: 'MONOGRAM_5351', listing: '997905351', title: MONO_TITLE, buyer: 'Name One', placed: { day: 0, h: 9, m: 10 }, ship: 5, variations: [V('Metal choice', 'Gold'), V('Chain length', '16 inches')], personalization: ['APV'], metalKey: 'gold' },
  osr: { rid: '4202783520', sku: 'MONOGRAM_9317', listing: '606439317', title: 'Monogram Necklace Dainty Monogram Necklace', buyer: 'Name Two', placed: { day: 0, h: 9, m: 20 }, ship: 5, variations: [V('Metal choice', 'Silver')], personalization: ['O S R'], metalKey: 'silver' },
  klm: { rid: '4200900001', sku: 'NEWMONO_1', listing: '888000001', title: 'Custom Monogram Pendant Necklace, 3 Initials', buyer: 'Name Three', placed: { day: 0, h: 9, m: 30 }, ship: 5, variations: [V('Metal choice', 'Rose Gold')], personalization: ['J.Q.X'], metalKey: 'rose' },
  amy: { rid: '4200900002', sku: 'NEWMONO_2', listing: '888000002', title: 'Monogram Bracelet Personalized Name Bracelet', buyer: 'Name Four', placed: { day: 0, h: 9, m: 40 }, ship: 5, variations: [V('Metal choice', 'Gold')], personalization: ['Amy'] },
  four: { rid: '4200900003', sku: 'NEWMONO_3', listing: '888000003', title: 'Monogram Necklace Four Initials', buyer: 'Name Five', placed: { day: 0, h: 9, m: 50 }, ship: 5, variations: [V('Metal choice', 'Gold')], personalization: ['ABCD'] },
  plain: { rid: '4200900004', sku: 'HEART_CHARM', listing: '888000004', title: 'Heart Charm Necklace', buyer: 'Name Six', placed: { day: 0, h: 10, m: 0 }, ship: 5, variations: [V('Metal choice', 'Gold')], personalization: ['A P V'] }
};
// listing-table monograms whose words are odd: the card shows what the buyer wrote, nothing is built, a person types the letters
const ODD = {
  odd2: { rid: '4200900012', words: 'A.P', want: /Type the three letters/, boxes: ['A', 'P', ''] },
  odd3: { rid: '4200900013', words: 'Jane Doe', want: /7 letters, a monogram needs 3/, boxes: ['', '', ''] },
  odd4: { rid: '4200900014', words: 'ABCD', want: /4 letters, a monogram needs 3/, boxes: ['', '', ''] },
  odd5: { rid: '4200900015', words: '123', want: /only digits, a monogram needs 3 letters/i, boxes: ['', '', ''] },
  odd6: { rid: '4200900016', words: '😀😀😀', want: /no letters, a monogram needs 3/i, boxes: ['', '', ''] }
};
Object.entries(ODD).forEach(([k, o], i) => { FX[k] = { rid: o.rid, sku: 'MONOGRAM_5351', listing: i % 2 ? '606439317' : '997905351', title: MONO_TITLE, buyer: 'Name ' + (7 + i), placed: { day: 0, h: 11, m: i }, ship: 5, variations: [V('Metal choice', 'Gold')], personalization: [o.words] }; });

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  try { require(path.join(pwDir, 'playwright-core')); } catch (_) { console.log('monogram-ui: no playwright-core, skipped'); return; }
  process.env.PW_DIR = pwDir;
  fs.mkdirSync(SHOTS, { recursive: true });
  const T0 = Date.now(), fails = []; let nCheck = 0;
  const check = (ok, msg) => { nCheck++; if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const w = await Base.world({ viewport: { width: 1440, height: 900 } });
  if (!w) { console.log('monogram-ui: no playwright-core, skipped'); return; }
  const { page, srv } = w, st = srv.st;
  let blobHold = null, blobFail = null, blobSeen = [];
  await w.context.route(/Monogram_\w+_\w\.ai/i, async route => {
    const url = route.request().url(); blobSeen.push(url);
    if (blobFail && blobFail.test(url)) return route.abort('failed');
    if (blobHold && blobHold.test(url)) { await new Promise(res => { blobHold.waiting.push(res); }); }
    return route.continue();
  });
  const holdOn = re => { re.waiting = []; blobHold = re; };
  const release = () => { const b = blobHold; blobHold = null; if (b) b.waiting.forEach(f => f()); };
  const card = rid => `.moCard[data-rid="${rid}"]`;
  const read = rid => page.evaluate(sel => {
    const h = document.querySelector(sel); if (!h) return null; const q = s => h.querySelector(s);
    return { state: h.dataset.state, st: q('.moSt').textContent, says: q('.moSays').textContent, meta: q('.moMeta').textContent, msg: q('.moMsg').hidden ? '' : q('.moMsg').textContent, kind: q('.moMsg').dataset.kind || '', retry: !!q('[data-mo="retry"]'),
      spin: !q('.moSpin').hidden, busy: q('.moPic').getAttribute('aria-busy'), hint: q('.moArt .moHint') ? q('.moArt .moHint').textContent : '', d: q('.moArt svg path') ? q('.moArt svg path').getAttribute('d') : '', ins: [...h.querySelectorAll('input')].map(i => i.value),
      approve: q('[data-mo="approve"]') ? { text: q('[data-mo="approve"]').textContent, hidden: q('[data-mo="approve"]').hidden, disabled: q('[data-mo="approve"]').disabled } : null, open: q('[data-mo="open"]') ? { hidden: q('[data-mo="open"]').hidden } : null };
  }, card(rid));
  const until = async (rid, fn, what, ms = 20000) => { const t0 = Date.now(); let r; for (;;) { r = await read(rid); if (r && fn(r)) return r; if (Date.now() - t0 > ms) { console.log('   (waiting for ' + what + ', last: ' + JSON.stringify(r).slice(0, 300) + ')'); return r; } await sleep(60); } };
  const ready = (rid, ms) => until(rid, r => !r.spin && (r.state === 'ready' || r.state === 'approved'), 'a finished monogram', ms);
  const builds = () => page.evaluate(() => window.__builds.slice());
  const fetched = () => page.evaluate(() => window.__fetched.slice());
  const typeIn = async (rid, i, text) => { await page.click(`${card(rid)} [data-mo-l="${i}"]`); await page.keyboard.type(text); };
  const openTab = () => page.evaluate(() => { CN.setMode('engrave'); Engrave.restoreView({ tab: 'mono', chosen: true }); Engrave.render(); });
  const shot = async name => { try { await page.screenshot({ path: path.join(SHOTS, name + '.png') }); } catch (e) { console.log('   (no screenshot ' + name + ')'); } };
  const seedOrders = names => page.evaluate(async ({ fixtures, orderSrc }) => {
    const orderOf = eval('(' + orderSrc + ')'), now = Date.now(); await Orders.loadMaps(true);
    for (const fx of fixtures) { const order = orderOf(fx, now); for (const line of order.lines) { if (fx.metalKey) line.metalKey = fx.metalKey; const key = CharmNestOrders.lineKey(order, line); if (B.orders.byKey.get(key)) continue; const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); } }
    Orders.interpretAll();
  }, { fixtures: names.map(n => FX[n]), orderSrc: Base.orderOf.toString() });
  const instrument = () => page.evaluate(() => {
    window.__builds = []; window.__fetched = [];
    if (!window.__wrapped) {
      window.__wrapped = true; const eb = CharmNestMonogram.build; CharmNestMonogram.build = function (l, o) { window.__builds.push(l.left.letter + l.middle.letter + l.right.letter); return eb.apply(this, arguments); };
      const mc = Pool.masterCharm; Pool.masterCharm = function (e, s) { window.__fetched.push(e.sku); return mc.apply(this, arguments); };
    }
  });
  const entries = () => page.evaluate(() => Object.values(CustomSheet.entries()).map(e => ({ ck: e.ck, files: e.files.map(f => ({ id: f.id, name: f.name, state: f.state, metal: f.metal, qty: f.qty, pieces: f.pieces, wMm: f.wMm, hMm: f.hMm })), sent: !!e.sent })));
  // (the page may still be reading the library it found empty when the alphabets are put in: read again until it holds what the fake site holds)
  const reloadMaster = async () => {
    for (let i = 0; i < 8; i++) {
      const want = [...st.docs.keys()].filter(k => k.startsWith('Charm_Master_Index/')).length;
      const have = await page.evaluate(async () => { await Master.load(true); return B.master.entries.size; });
      if (have === want) return; await sleep(400);
    }
  };

  // Paul's own file, through the local indexer into the fake library (the same route that makes his real SKUs): his three script alphabets. MONO_REAL=0 / a missing file: stand-in alphabets from the vendored fonts, and nothing is rendered for a picture.
  const REAL = process.env.MONO_FILE || path.join(__dirname, 'fixtures', 'SKUs_MONOGRAM_2024-1015.ai');
  const REAL_OK = process.env.MONO_REAL !== '0' && fs.existsSync(REAL);
  const TRIO = [['APV', '4200683017'], ['JQX', '4200900001'], ['OSR', '4202783520']];       // (the two monogram orders and a third: the pictures are taken of these three)
  const seedLibrary = async () => {
    if (!REAL_OK) { const set = await A.make(); A.seed(st, set); return null; }
    const tmp = fs.mkdtempSync(path.join(require('os').tmpdir(), 'cn-mono-ui-')), file = path.join(tmp, path.basename(REAL)); fs.copyFileSync(REAL, file);
    const { main } = require('../../scripts/index-master.cjs'), { REPO_TEST } = require('./bridge-server.cjs');
    st.repoStrict = true; process.env.CHARM_REPO_PASSWORD = REPO_TEST.password;
    const lines = []; const rep = await main(['node', 'x', file, '--origin', srv.sorterOrigin], l => lines.push(String(l)));
    console.log('   the indexer: ' + lines.filter(l => !l.startsWith('  ')).slice(0, 8).join(' | '));
    return rep;
  };
  // The picture of the three cards, as a person sees them (the other cards put away for the picture): the page, then each card. 1440 wide, tall enough for the three; 390 for the phone (the rail folded as a person would).
  const cardsShot = async (name, wide = true) => {
    if (!REAL_OK) return;
    const rail0 = await page.evaluate(() => document.getElementById('app').classList.contains('railOff'));
    try {
      await page.evaluate(() => { document.querySelectorAll('.cnNbX, .toast .x').forEach(b => b.click()); if (document.activeElement && document.activeElement.blur) document.activeElement.blur(); });
      await page.setViewportSize(wide ? { width: 1440, height: 1500 } : { width: 390, height: 2000 }); await sleep(450);
      if (!wide && await page.evaluate(() => !document.getElementById('app').classList.contains('railOff'))) { await page.click('#btnRail'); await sleep(450); }
      await page.evaluate(ids => { document.querySelectorAll('#egMono .moCard').forEach(c => { c.style.display = ids.includes(c.dataset.rid) ? '' : 'none'; }); const f = document.querySelector('#egMono .moCard[data-rid="' + ids[0] + '"]'); if (f) f.scrollIntoView({ block: 'start' }); }, TRIO.map(t => t[1]));
      await sleep(250); await shot(name);
      for (const [word, rid] of TRIO) await page.locator(card(rid)).screenshot({ path: path.join(SHOTS, `${name}-${word}.png`) }).catch(() => console.log('   (no screenshot ' + name + '-' + word + ')'));
    } finally {
      await page.evaluate(() => { document.querySelectorAll('#egMono .moCard').forEach(c => { c.style.display = ''; }); }).catch(() => {});
      await page.setViewportSize({ width: 1440, height: 900 }); await sleep(350);
      if (!wide && !rail0 && await page.evaluate(() => document.getElementById('app').classList.contains('railOff'))) { await page.click('#btnRail'); await sleep(350); }
    }
  };
  const pasteWord = (rid, wd) => page.evaluate(({ sel, wd }) => { const t = document.querySelector(sel + ' [data-mo-l="0"]'); t.focus(); const dt = new DataTransfer(); dt.setData('text', wd); t.dispatchEvent(new ClipboardEvent('paste', { clipboardData: dt, bubbles: true, cancelable: true })); }, { sel: card(rid), wd });
  // what the picture of a card is: its path, how many points it has, and whether it is exactly the geometry the engine made (the cut piece)
  const pictureOf = rid => page.evaluate(sel => { const h = document.querySelector(sel), pa = h && h.querySelector('.moArt svg path'); if (!pa) return null; const ui = Engrave.monogramUI(), c = [...ui.cards.values()].find(x => x.host === h), d = pa.getAttribute('d');
    const cs = getComputedStyle(pa), bb = pa.getBoundingClientRect(), box = h.querySelector('.moPic').getBoundingClientRect();
    return { points: (d.match(/[ML]/g) || []).length, subpaths: (d.match(/M/g) || []).length, same: d === CharmNestMonogram.svgPath(c.result), fill: pa.getAttribute('fill'), stroke: pa.getAttribute('stroke'), rule: pa.getAttribute('fill-rule'), nonScaling: pa.getAttribute('vector-effect'), box: [Math.round(box.width), Math.round(box.height)],
      margins: [bb.left - box.left, box.right - bb.right, bb.top - box.top, box.bottom - bb.bottom].map(v => Math.round(v)), dpr: devicePixelRatio, rings: c.result.rings.length, holes: c.result.rings.filter(r => r.hole).length }; }, card(rid));
  const TONE = { gold: '#d9bd78', silver: '#c8ccd2', rose: '#e2b4a1' };
  const POINT_FLOOR = 1500;    // (points in the path of a monogram of Paul's script letters; the block serif stand-ins make a few hundred)

  if (process.env.MONO_ONLY_REAL === '1') await w.close();   // (MONO_ONLY_REAL=1: nothing here)
  else try {
    // ═══ the library: Paul's three alphabets (his file, indexed), named like his file ═══
    const rep0 = await seedLibrary();
    if (REAL_OK) check(!!rep0 && rep0.written === 78, `Paul's file indexed into 78 SKUs (${rep0 && rep0.written})`);
    await reloadMaster();
    check(await page.evaluate(() => [...B.master.entries.keys()].filter(k => /^MONOGRAM_(LEFT|MID|RIGHT)_[A-Z]$/.test(k)).length) === 78, 'the library holds 78 letter SKUs: MONOGRAM_LEFT_A … MONOGRAM_MID_A … MONOGRAM_RIGHT_Z');
    check(await page.evaluate(() => { const f = CharmNestMonogram.familiesOf([...B.master.entries.keys()]); return f.length === 3 && f.every(x => x.count === 26 && x.missing.length === 0) && f.map(x => x.name).sort().join('|') === 'Monogram Left|Monogram Mid|Monogram Right'; }), 'familiesOf finds exactly the three families from the upper-cased SKU list and names them Monogram Left / Mid / Right');

    // ═══ 1 · the orders ═══
    await seedOrders(['apv', 'osr', 'klm', 'amy', 'four', 'plain']);
    const specs = await page.evaluate(() => Orders.rows().map(r => ({ rid: r.order.receiptId, mono: !!(r.spec && r.spec.special && r.spec.special.monogram), label: r.spec && r.spec.special && r.spec.special.label, words: r.spec && r.spec.special && r.spec.special.words, own: !!(r.spec && r.spec.ownDesign), problems: (r.spec.problems || []).length })));
    const by = rid => specs.find(s => s.rid === rid);
    check(by('4200683017').mono && by('4200683017').words === 'APV' && by('4200683017').label === 'Custom monogram' && by('4200683017').own && by('4200683017').problems === 0, 'order 4200683017 (listing 997905351) is a custom monogram with the letters APV, no question asked');
    check(by('4202783520').mono && by('4202783520').words === 'O S R' && by('4202783520').problems === 0, 'order 4202783520 (listing 606439317) is a custom monogram with the letters O S R');
    check(by('4200900001').mono && by('4200900001').words === 'J.Q.X', 'a title that says monogram with three letters in the box (J.Q.X) is read the same way');
    check(!by('4200900002').mono && !by('4200900003').mono && !by('4200900004').mono, 'a monogram bracelet with a name, a title with four letters, a regular charm: not monograms');

    // ═══ 2 · the Engrave tab ═══
    await instrument();
    await openTab();
    await page.waitForSelector(`${card('4200683017')} svg path`, { timeout: 30000 });
    await ready('4200683017'); await ready('4202783520'); await ready('4200900001');
    const tab = await page.evaluate(() => { const b = document.querySelector('#engraveView .egTab[data-tab="mono"]'); return b ? { text: b.textContent.replace(/\s+/g, ' ').trim(), count: b.querySelector('b') && b.querySelector('b').textContent, on: b.classList.contains('on') || b.getAttribute('aria-selected') === 'true' || b.dataset.on === '1' } : null; });
    check(tab && /^Monograms/.test(tab.text) && tab.count === '3', 'the Engrave tab has a Monograms tab with the count: ' + JSON.stringify(tab));
    check(await page.evaluate(() => document.querySelectorAll('#egMono .moCard').length) === 3, 'one card per monogram line (3), none for the bracelet, the four letters or the regular charm');
    let a = await read('4200683017');
    check(a.state === 'ready' && a.d.length > 500 && a.ins.join('') === 'APV' && a.says === 'The buyer wrote A P V · Left A · Middle P · Right V', 'APV: the preview is drawn and the card says it plainly: "' + a.says + '"');
    check(/^Actual size \d+\.\d × \d+\.\d mm · Gold$/.test(a.meta), 'and its actual size and metal: ' + a.meta);
    const f1 = await fetched();
    check(['MONOGRAM_LEFT_A', 'MONOGRAM_MID_P', 'MONOGRAM_RIGHT_V'].every(s => f1.includes(s)) && !f1.includes('MONOGRAM_LEFT_P') && !f1.includes('MONOGRAM_MID_A') && !f1.includes('MONOGRAM_RIGHT_A'), 'letter 1 came from the Left alphabet, letter 2 from the Middle, letter 3 from the Right: ' + [...new Set(f1)].filter(s => /A$|P$|V$/.test(s)).sort().join(' '));
    check(await page.evaluate(() => { const d = document.querySelector('#egMono details.moAbcs'); return !!d && !d.open && /Monogram Left · Monogram Mid · Monogram Right/.test(d.textContent); }), 'the three alphabets were found by their names, nothing to pick: "Alphabets · Monogram Left · Monogram Mid · Monogram Right" (closed)');
    const rs = await page.evaluate(() => { const c = Engrave.monogramUI().cards.get('4200683017_42006830171'); const r = c.result, pl = r.placed, hOf = p => p.bboxPt[3] - p.bboxPt[1];
      return { ok: r.ok, connected: r.report.connected, outer: r.rings.filter(x => !x.hole).length, holes: r.rings.filter(x => x.hole).length, kept: r.report.holesKept, slots: pl.map(p => p.slot + ':' + p.letter).join(' '), mid: hOf(pl[1]), side: Math.max(hOf(pl[0]), hOf(pl[2])), cs: r.report.centerScale }; });
    check(rs.ok && rs.connected && rs.outer === 1 && rs.holes >= 1 && rs.kept >= (REAL_OK ? 0.5 : 0.9), 'ONE connected charm with its counters: ' + JSON.stringify({ outer: rs.outer, holes: rs.holes, kept: rs.kept }));
    check(rs.slots === 'left:A middle:P right:V' && rs.mid >= 1.2 * rs.side, `the letters sit Left A, Middle P, Right V and the centre is bigger than the sides (${rs.mid.toFixed(1)} vs ${rs.side.toFixed(1)} pt, x${(rs.mid / rs.side).toFixed(2)})`);
    a = await read('4202783520');
    check(a.ins.join('') === 'OSR' && a.says === 'The buyer wrote O S R · Left O · Middle S · Right R' && a.state === 'ready', 'OSR: "' + a.says + '"');
    await shot('ui-1440-ready');
    // the picture is the CUT piece of Paul's real script letters: the real glyph outlines (thousands of points, block letters never reach it), filled with the order's metal, a thin cut line round the outline and every counter, centred with an even margin
    if (REAL_OK) {
      const pics = {}; for (const [wd, rid] of TRIO) pics[wd] = await pictureOf(rid);
      console.log('   the pictures:', JSON.stringify(pics));
      check(TRIO.every(([wd]) => pics[wd] && pics[wd].same && pics[wd].points >= POINT_FLOOR && pics[wd].holes >= 1), `the picture of each order is the geometry the engine cut from Paul's real letters, ${POINT_FLOOR}+ points (block serif letters stay under it): ` + TRIO.map(([wd]) => wd + ' ' + (pics[wd] && pics[wd].points)).join(' · '));
      check(pics.APV.fill === TONE.gold && pics.OSR.fill === TONE.silver && pics.JQX.fill === TONE.rose && TRIO.every(([wd]) => /^#[0-9a-f]{6}$/.test(pics[wd].stroke) && pics[wd].rule === 'evenodd' && pics[wd].nonScaling === 'non-scaling-stroke'), `filled with the order's metal (APV gold, OSR silver, JQX rose gold), one thin dark cut line, counters open: ` + TRIO.map(([wd]) => wd + ' ' + pics[wd].fill + '/' + pics[wd].stroke).join(' · '));
      check(TRIO.every(([wd]) => { const m = pics[wd].margins; return Math.abs(m[0] - m[1]) <= 3 && Math.abs(m[2] - m[3]) <= 3 && Math.min(...m) >= 6; }), 'centred with an even margin, inside the picture box: ' + TRIO.map(([wd]) => wd + ' ' + pics[wd].margins.join('/')).join(' · '));
      await cardsShot('ui2-1440-ready'); await cardsShot('ui2-390-ready', false);
      // a word whose join has a warning (the first of these that the engine warns about; the cards show it quietly beside the size)
      const cands = ['AQV', 'ATB', 'HFT', 'TTT', 'WIW', 'ZMT', 'LDH', 'JQX', 'OSR', 'APV']; { let x = 7; while (cands.length < 46) { const L = () => String.fromCharCode(65 + (x = (x * 1103515245 + 12345) % 2147483648) % 26); cands.push(L() + L() + L()); } }
      const HELD = 'BCDKGHUSQ';      // (the middle letters other steps hold back or time: looking at a word fetches its letters, and a letter already fetched cannot be held)
      for (let i = cands.length - 1; i >= 0; i--) if (HELD.includes(cands[i][1]) && !['JQX', 'OSR', 'APV'].includes(cands[i])) cands.splice(i, 1);
      const warns = await page.evaluate(async cands => { const ui = Engrave.monogramUI(), abc = ui.alphabets(), out = [], t0 = Date.now();
        for (const wd of cands) { if (Date.now() - t0 > 45000 || out.length >= 3) break; try { const g = await Promise.all([ui.glyph('left', wd[0], abc), ui.glyph('middle', wd[1], abc), ui.glyph('right', wd[2], abc)]); const r = CharmNestMonogram.build({ left: g[0], middle: g[1], right: g[2] }); if (r && r.ok && r.report.warnings.length) out.push([wd, r.report.warnings[0]]); } catch (_) { /* a letter that does not build is not the one wanted */ } await new Promise(r => setTimeout(r, 0)); }
        return out; }, cands);
      console.log('   words the engine warns about:', JSON.stringify(warns));
      if (warns.length) {
        for (let i = 0; i < warns.length; i++) await pasteWord(TRIO[i][1], warns[i][0]);
        for (let i = 0; i < warns.length; i++) await until(TRIO[i][1], r => !r.spin && r.state === 'ready' && r.ins.join('') === warns[i][0], 'the warning word', 40000);
        const wr = await Promise.all(warns.map((x, i) => read(TRIO[i][1])));
        check(wr.every((r, i) => r.state === 'ready' && r.meta.includes(warns[i][1].charAt(0).toUpperCase() + warns[i][1].slice(1))), 'a word whose join has a warning shows it beside the size: ' + wr.map((r, i) => warns[i][0] + ' "' + r.meta.slice(0, 120) + '"').join(' | '));
        await cardsShot('ui2-1440-warning');
        for (let i = 0; i < warns.length; i++) await pasteWord(TRIO[i][1], TRIO[i][0]);
        for (let i = 0; i < warns.length; i++) await until(TRIO[i][1], r => !r.spin && r.state === 'ready' && r.ins.join('') === TRIO[i][0], 'the buyer\'s letters again', 40000);
      } else console.log('   (the engine warns about none of the words tried: no warning picture)');
    }

    // ═══ 3 · editing the letters ═══
    const b0 = (await builds()).length;
    await page.click(`${card('4200683017')} [data-mo-l="1"]`); await page.keyboard.type('S');
    a = await read('4200683017');
    check(a.state === 'busy' && a.spin && a.busy === 'true' && a.approve.disabled, 'a letter typed: the card is busy at once (spinner on the picture, Approve waits): ' + a.st);
    a = await ready('4200683017');
    check(a.state === 'ready' && a.ins.join('') === 'ASV' && a.says === 'The buyer wrote A P V · Left A · Middle S · Right V' && !a.spin && a.busy === 'false', 'the monogram was remade for A S V and the card says: "' + a.says + '"');
    check((await builds()).slice(b0).join() === 'ASV', 'one build for the edit: ' + (await builds()).slice(b0).join());
    { const bb = (await builds()).length; await typeIn('4200683017', 1, 'T'); await page.keyboard.type('X'); await ready('4200683017');   // two edits within the wait: one remake of the last
      const nb = (await builds()).slice(bb); check(nb.join() === 'ATX', 'two quick edits make ONE monogram (the last): ' + nb.join()); }
    // the progress bar while the letters are fetched (one letter file held back)
    holdOn(/Monogram_Mid_[BCD]\.ai/i); await typeIn('4200683017', 1, 'B'); await typeIn('4202783520', 1, 'C'); await typeIn('4200900001', 1, 'D');
    await page.waitForFunction(() => /Building monogram/.test((document.querySelector('.cnp.on') || {}).textContent || ''), null, { timeout: 8000 }).catch(() => {});
    a = await read('4200683017');
    check(a.state === 'busy' && a.spin, 'while a letter file is on its way the spinner stays: ' + a.st);
    check(await page.evaluate(() => { const n = document.querySelector('.cnp.on'); return !!n && /Building monogram [A-Z]{3}/.test(n.textContent); }), 'and the page\'s progress bar says "Building monogram …"');
    await shot('ui-1440-building');
    if (REAL_OK) { const bp = await Promise.all(TRIO.map(([, rid]) => pictureOf(rid)));
      check(bp.every(x => x && x.points >= POINT_FLOOR && x.same), 'while the three are remade each keeps its old picture (the real letters) under the spinner: ' + bp.map(x => x && x.points).join(' ')); }
    await cardsShot('ui2-1440-building');
    release(); await ready('4200683017'); await ready('4202783520'); await ready('4200900001');
    await typeIn('4202783520', 1, 'S'); await typeIn('4200900001', 1, 'Q'); await ready('4202783520'); await ready('4200900001');
    // a second edit while the first is still being fetched: only the newest is ever shown
    holdOn(/Monogram_Mid_K\.ai/i);
    const bs = (await builds()).length;
    await typeIn('4200683017', 1, 'K');
    await sleep(900);   // (the build for K is on its way and held)
    check((await read('4200683017')).state === 'busy', 'the first edit (K) is still being fetched');
    await typeIn('4200683017', 1, 'U'); const u = await ready('4200683017');
    check(u.state === 'ready' && u.ins.join('') === 'AUX' && (await builds()).slice(bs).join() === 'AUX', 'the second edit (U) finished first and is shown: ' + (await builds()).slice(bs).join());
    release(); await sleep(1200);
    { const after = await read('4200683017'), nb = (await builds()).slice(bs);
      check(after.d === u.d && after.ins.join('') === 'AUX' && after.state === 'ready' && !nb.includes('AKX'), 'the older edit finishing later never overwrites: the picture is unchanged and no build was made for it: ' + nb.join()); }
    // swap
    await page.click(`${card('4200683017')} [data-mo-swap="0"]`); a = await ready('4200683017');
    check(a.ins.join('') === 'UAX' && a.says.endsWith('Left U · Middle A · Right X') && (await builds()).slice(-1)[0] === 'UAX', 'swap Left and Middle: U A X, remade: ' + a.says);
    await page.click(`${card('4200683017')} [data-mo-swap="0"]`); await ready('4200683017');
    // paste
    await page.evaluate(() => { const t = document.querySelector('.moCard[data-rid="4200683017"] [data-mo-l="0"]'); t.focus(); const dt = new DataTransfer(); dt.setData('text', 'P.V.A'); t.dispatchEvent(new ClipboardEvent('paste', { clipboardData: dt, bubbles: true, cancelable: true })); });
    a = await ready('4200683017'); check(a.ins.join('') === 'PVA' && a.state === 'ready', 'three letters pasted into any box fill all three: ' + a.ins.join(''));
    // Esc takes an edit back
    await typeIn('4200683017', 0, 'Q'); await page.keyboard.press('Escape'); a = await ready('4200683017');
    check(a.ins.join('') === 'PVA' && a.state === 'ready', 'Esc in a letter box takes the edit back: ' + a.ins.join(''));
    // back to the buyer's own letters
    await page.evaluate(() => { const t = document.querySelector('.moCard[data-rid="4200683017"] [data-mo-l="0"]'); t.focus(); const dt = new DataTransfer(); dt.setData('text', 'APV'); t.dispatchEvent(new ClipboardEvent('paste', { clipboardData: dt, bubbles: true, cancelable: true })); });
    await ready('4200683017');
    // a render of the whole tab never takes a caret from a letter box
    await page.click(`${card('4200683017')} [data-mo-l="2"]`);
    const same = await page.evaluate(() => { const n = document.activeElement; Engrave.render(); Engrave.render(); return n === document.activeElement && n.matches('[data-mo-l="2"]'); });
    check(same, 'a re-render of the Engrave tab leaves the focused letter box (and its caret) alone');

    // ═══ 4 · a missing letter, a load that fails once, odd input ═══
    const midY = st.docs.get('Charm_Master_Index/MONOGRAM_MID_Y');
    st.docs.delete('Charm_Master_Index/MONOGRAM_MID_Y'); await reloadMaster();
    check(await page.evaluate(() => CharmNestMonogram.familiesOf([...B.master.entries.keys()]).find(f => f.base === 'MONOGRAM_MID_{L}').missing.join('')) === 'Y', 'the library now lacks MONOGRAM_MID_Y (familiesOf names it)');
    const nbq = (await builds()).length;
    await typeIn('4200683017', 1, 'Y');
    a = await until('4200683017', r => r.state === 'alert', 'the missing-letter message');
    check(a.kind === 'missing' && /Middle alphabet has no “Y”/.test(a.msg) && /MONOGRAM_MID_Y/.test(a.msg) && /Monogram Mid lacks Y/.test(a.msg), 'a missing letter is a plain message naming it: "' + a.msg + '"');
    check(!a.spin && a.busy === 'false' && a.approve.disabled && !(await builds()).slice(nbq).some(x => x[1] === 'Y'), 'no spinner, Approve waits, nothing built with the missing letter: ' + JSON.stringify([a.spin, a.busy, a.approve, (await builds()).slice(nbq)]));
    await shot('ui-1440-missing');
    st.docs.set('Charm_Master_Index/MONOGRAM_MID_Y', midY); await reloadMaster();          // (the letter is back: only the message of the missing one was wanted)
    await typeIn('4200683017', 1, 'P'); a = await ready('4200683017'); check(a.state === 'ready' && !a.msg, 'typing another letter takes the card out of it');
    // a letter file that fails to load: Try again
    blobFail = /Monogram_Mid_Z\.ai/i; await typeIn('4200683017', 1, 'Z');
    a = await until('4200683017', r => r.state === 'alert', 'the load failure', 40000);
    check(a.kind === 'load' && a.retry && /could not be loaded/.test(a.msg) && !a.spin && a.busy === 'false', 'a letter file that does not load: a plain message and Try again, no stuck spinner: "' + a.msg + '"');
    check(await page.evaluate(() => !document.querySelector('.cnp.on')), 'and the page\'s progress bar is gone');
    blobFail = null; await page.click(`${card('4200683017')} [data-mo="retry"]`); a = await ready('4200683017', 30000);
    check(a.state === 'ready' && a.ins.join('') === 'AZV' && !a.msg, 'Try again loads it and the monogram is made: ' + a.ins.join(''));
    await typeIn('4200683017', 1, 'P'); await ready('4200683017');
    // typing what is not a letter
    const nbo = (await builds()).length;
    await page.click(`${card('4200683017')} [data-mo-l="1"]`); await page.keyboard.type('7'); a = await read('4200683017');
    check(a.ins.join('') === 'APV' && /Letters A to Z only/.test(a.meta), 'a digit is refused calmly: the box keeps its letter, "Letters A to Z only"');
    await page.keyboard.type('😀'); a = await read('4200683017'); check(a.ins.join('') === 'APV' && /Letters A to Z only/.test(a.meta), 'an emoji too');
    await page.keyboard.press('Backspace'); a = await read('4200683017');
    check(a.state === 'words' && a.ins[1] === '' && a.approve.disabled, 'an emptied box: the card waits for the three letters (no build): ' + JSON.stringify(a.ins) + ' ' + a.st);
    check((await builds()).length === nbo && /Type the three letters/.test(a.hint), 'nothing was built for what is not three letters: "' + a.hint + '"');
    await typeIn('4200683017', 1, 'p'); a = await ready('4200683017'); check(a.ins.join('') === 'APV', 'a small letter is made a capital');
    // odd words on listing-table monograms: shown calmly, nothing built
    await seedOrders(Object.keys(ODD)); await page.evaluate(() => Engrave.render());
    await sleep(400);
    for (const [k, o] of Object.entries(ODD)) {
      const r = await read(o.rid);
      check(r && r.state === 'words' && o.want.test(r.hint) && r.ins.join('|') === o.boxes.join('|') && r.approve.disabled && !r.spin, `the buyer's "${o.words}": ${r && r.hint}`);
    }
    check((await builds()).slice(nbo).every(x => x === 'APV'), 'and no monogram was built for any of them: ' + ((await builds()).slice(nbo).join() || 'no build at all'));
    await page.evaluate(() => { document.querySelector('#egMono .moCard[data-rid="4200900012"]').scrollIntoView({ block: 'start' }); }); await sleep(200);
    await shot('ui-1440-odd');
    // a person types the letters of "A.P": the card builds
    await typeIn('4200900012', 2, 'v'); a = await ready('4200900012');
    check(a.state === 'ready' && a.ins.join('') === 'APV', 'for "A.P" a person adds the third letter and the monogram is made');
    // Paul's own Mid Q is drawn the way a converted font leaves a letter (its loop and a tail that cross each other: three rings that cross): the crossing is ink, not a hole (the stand-in alphabets have no such letter)
    if (REAL_OK) {
      await typeIn('4200683017', 1, 'Q'); a = await ready('4200683017', 30000);
      const q = await page.evaluate(async () => { const ui = Engrave.monogramUI(), c = ui.cards.get('4200683017_42006830171'), abc = ui.alphabets(), g = await ui.glyph('middle', 'Q', abc), L = await ui.glyph('left', 'A', abc), R = await ui.glyph('right', 'V', abc);
        const one = CharmNestMonogram.build({ left: L, middle: Object.assign({}, g, { fill: 'nonzero' }), right: R }), even = CharmNestMonogram.build({ left: L, middle: Object.assign({}, g, { fill: 'evenodd' }), right: R });
        return { fill: g.fill, rings: g.rings.length, same: CharmNestMonogram.svgPath(one) === CharmNestMonogram.svgPath(c.result), differs: CharmNestMonogram.svgPath(even) !== CharmNestMonogram.svgPath(c.result), outer: c.result.rings.filter(x => !x.hole).length }; });
      check(a.state === 'ready' && q.rings >= 2 && q.fill === 'auto' && q.same && q.differs && q.outer === 1, 'Paul\'s Mid Q (rings that cross each other) is read as ink where they cross: the card shows the piece the non-zero reading makes, not the even-odd one with holes at the crossings: ' + JSON.stringify(q));
      await typeIn('4200683017', 1, 'P'); await ready('4200683017');
    }

    // ═══ 5 · Approve ═══
    const key1 = '4200683017_42006830171';
    await page.click(`${card('4200683017')} [data-mo="approve"]`);
    a = await until('4200683017', r => r.state === 'approved', 'the approval');
    const e1 = await entries();
    check(e1.length === 1 && e1[0].files.length === 1 && e1[0].files[0].name === 'Monogram APV.ai', 'Approve puts Monogram APV.ai on the order\'s custom designs: ' + JSON.stringify(e1.map(e => e.files.map(f => f.name))));
    await page.waitForFunction(() => Object.values(CustomSheet.entries()).every(e => e.files.every(f => f.state === 'ready')), null, { timeout: 30000 });
    const e2 = await entries(), f = e2[0].files[0], size = await page.evaluate(() => { const r = Engrave.monogramUI().cards.get('4200683017_42006830171').result; return { w: r.wMm, h: r.hMm }; });
    check(f.state === 'ready' && f.pieces === 1 && Math.abs(f.wMm - size.w) < 0.3 && Math.abs(f.hMm - size.h) < 0.3, `read back as ONE piece of ${f.wMm.toFixed(1)} × ${f.hMm.toFixed(1)} mm (the preview says ${size.w.toFixed(1)} × ${size.h.toFixed(1)})`);
    check(await page.evaluate(() => !document.querySelector('#cuDlg[open]')), 'the designs window did not open over the tab');
    check(a.approve.hidden && !a.open.hidden && a.st === 'Approved', 'the card says Approved and offers Set metal · Send to Sheet (no second Approve): ' + JSON.stringify([a.state, a.st, a.approve, a.open, a.ins]));
    await shot('ui-1440-approved');
    // changing the letters replaces the design
    await page.click(`${card('4200683017')} [data-mo="open"]`); await page.waitForSelector('#cuDlg[open] .cuFile', { timeout: 10000 });
    check(await page.evaluate(() => document.querySelectorAll('#cuDlg .cuFile').length === 1 && /Monogram APV\.ai/.test(document.querySelector('#cuDlg .cuFile').textContent)), 'Set metal · Send to Sheet opens the designs window with the monogram');
    await page.click('#cuDlg .cuFile .cuM[data-m="silver"]'); await page.click('#cuDlg [data-x]'); await sleep(300);
    await typeIn('4200683017', 1, 'S'); a = await ready('4200683017');
    check(a.state === 'ready' && !a.approve.hidden && a.approve.text === 'Replace approved' && a.st === 'Ready to approve', 'after a letter change the button says Replace approved: ' + JSON.stringify([a.state, a.st, a.approve]));
    await page.click(`${card('4200683017')} [data-mo="approve"]`); a = await until('4200683017', r => r.state === 'approved', 'the replacement');
    await sleep(500);
    const e3 = await entries();
    check(e3.length === 1 && e3[0].files.length === 1 && e3[0].files[0].name === 'Monogram ASV.ai', 'the approved design is REPLACED, never two: ' + JSON.stringify(e3.map(e => e.files.map(f => f.name))));
    check(e3[0].files[0].metal === 'silver', 'and the metal a person set carries over to it: ' + e3[0].files[0].metal);
    await page.waitForFunction(() => Object.values(CustomSheet.entries()).every(e => e.files.every(f => f.state === 'ready')), null, { timeout: 30000 });
    // the same letters again: nothing changes
    await page.evaluate(() => Engrave.monogramUI().approve(Engrave.monogramUI().cards.get('4200683017_42006830171')));
    await sleep(300); check(JSON.stringify((await entries()).map(e => e.files.map(f => f.name))) === JSON.stringify([['Monogram ASV.ai']]), 'approving the same letters again changes nothing');
    // approving another order's monogram does not touch this one
    await page.click(`${card('4202783520')} [data-mo="approve"]`); await until('4202783520', r => r.state === 'approved', 'the second approval');
    const e4 = await entries(); check(e4.length === 2 && e4.map(e => e.files.map(f => f.name).join()).sort().join('|') === 'Monogram ASV.ai|Monogram OSR.ai', 'a second order\'s monogram is its own design: ' + JSON.stringify(e4.map(e => e.files.map(f => f.name))));
    // the Review tab shows the design on the order's card
    await page.evaluate(() => { CN.setMode('review'); Review.render(); });
    await page.waitForSelector('#rvList .reviewListRow[data-rid="4200683017"]', { timeout: 15000 });
    check(await page.evaluate(() => /Monogram ASV\.ai|1 design/.test(document.querySelector('#rvList .reviewListRow[data-rid="4200683017"]').textContent)), 'the order\'s Review card shows its design');

    // ═══ 6 · Build monogram from the Review card ═══
    await page.evaluate(() => { CN.setMode('review'); Review.render(); });
    await page.waitForSelector('#rvList .reviewListRow[data-rid="4200900001"]', { timeout: 15000 });
    check(await page.evaluate(() => !document.querySelector('#rvList .reviewListRow[data-rid="4200683017"] [data-cu-mono]')), 'an order with its monogram already approved offers no Build monogram');
    check(await page.evaluate(() => !!document.querySelector('#rvList .reviewListRow[data-rid="4200900001"] [data-cu-mono]')), 'an order with no design yet offers Build monogram');
    await shot('ui-1440-review');
    await page.click('#rvList .reviewListRow[data-rid="4200900001"] [data-cu-mono]');
    await page.waitForSelector('#egMono .moCard[data-rid="4200900001"].moFocus', { timeout: 10000 });
    check(await page.evaluate(() => { const v = document.getElementById('engraveView'); return !v.classList.contains('hidden') && !!v.querySelector('#egMono') && Engrave.view().tab === 'mono'; }), 'Build monogram opens the Engrave tab on Monograms, on that order\'s card (highlighted)');
    check(await page.evaluate(() => { const r = document.querySelector('#egMono .moCard[data-rid="4200900001"]').getBoundingClientRect(); return r.top >= 0 && r.bottom <= innerHeight + 5; }), 'the card is scrolled into view');
    // switching tabs keeps what the cards hold
    const nbt = (await builds()).length;
    await page.click('#engraveView .egTab[data-tab="place"]'); await page.click('#engraveView .egTab[data-tab="mono"]'); await sleep(400);
    a = await read('4200683017'); check(a && a.state === 'approved' && (await builds()).length === nbt, 'Placements and back: the cards are as they were, nothing is built again');

    // the tab's search box finds a monogram by its order, buyer or letters, and the cards that are not shown keep what they hold
    { const shown = () => page.evaluate(() => [...document.querySelectorAll('#egMono .moCard')].map(c => c.dataset.rid)), nb2 = (await builds()).length;
      await page.fill('#egQ', 'OSR'); await sleep(300); const f1 = await shown();
      await page.fill('#egQ', 'zzz'); await sleep(300); const f2 = await page.evaluate(() => [document.querySelectorAll('#egMono .moCard').length, (document.querySelector('#egMono .moNone') || {}).textContent]);
      await page.fill('#egQ', 'Name Three'); await sleep(300); const f3 = await shown();
      await page.fill('#egQ', ''); await sleep(400); const f4 = await shown(); const keptApv = await read('4200683017');
      check(f1.join() === '4202783520' && f2[0] === 0 && /No monogram matches “zzz”/.test(f2[1]) && f3.join() === '4200900001' && f4.length === 8 && keptApv.state === 'approved' && (await builds()).length === nb2, 'the search box finds a monogram by its letters (OSR), by its buyer, says so when none matches, and clears: ' + JSON.stringify([f1, f2, f3, f4.length]) + ' (nothing built again)'); }

    // ═══ the alphabets: none, partial, chosen by hand ═══
    const keep = new Map(); for (const [k, v] of st.docs) if (/^Charm_Master_Index\/MONOGRAM_/.test(k)) keep.set(k, v);
    const dropFam = word => { for (const k of [...st.docs.keys()]) if (new RegExp('^Charm_Master_Index/MONOGRAM_' + word + '_').test(k)) st.docs.delete(k); };
    dropFam('RIGHT'); await reloadMaster(); await page.evaluate(() => { Engrave.restoreView({ tab: 'mono', chosen: true }); Engrave.render(); }); await sleep(300);
    await page.evaluate(() => Engrave.monogramUI().rebuild(Engrave.monogramUI().cards.get('4200900012_42009000121')));
    a = await until('4200900012', r => r.state === 'alert' || r.hint, 'the setup state');
    const ab = await page.evaluate(() => { const d = document.querySelector('#egMono details.moAbcs'); return { open: d.open, text: d.textContent.replace(/\s+/g, ' '), selects: d.querySelectorAll('select').length, opts: [...d.querySelectorAll('select')[2].options].map(o => o.textContent) }; });
    check(ab.open && /No Right alphabet found/.test(ab.text) && ab.selects === 3 && ab.opts.length === 3, 'with no Right alphabet in the library the Alphabets are open and say so, with the picker: ' + ab.text.slice(0, 150));
    dropFam('LEFT'); dropFam('MID'); await reloadMaster(); await page.evaluate(() => Engrave.render()); await sleep(200);
    await page.evaluate(() => Engrave.monogramUI().rebuild(Engrave.monogramUI().cards.get('4200900012_42009000121')));
    a = await until('4200900012', r => r.hint, 'the setup state with no alphabet');
    const ab2 = await page.evaluate(() => { const d = document.querySelector('#egMono details.moAbcs'); return { open: d.open, text: d.textContent.replace(/\s+/g, ' ') }; });
    check(ab2.open && /No 26-letter alphabet is in the charm library yet/.test(ab2.text) && /Monogram_Left_A/.test(ab2.text) && /Choose the three alphabets/.test(a.hint) && a.approve.disabled, 'with no alphabet at all: the quiet setup state, telling what to add: ' + ab2.text.slice(0, 140));
    await shot('ui-1440-setup');
    // three alphabets with no Left / Mid / Right in their names: chosen by hand, kept
    const FANCY = { ONE: 'LEFT', TWO: 'MID', THREE: 'RIGHT' };
    for (const [nm, from] of Object.entries(FANCY)) for (const L of A.LETTERS) { const e = keep.get(`Charm_Master_Index/MONOGRAM_${from}_${L}`); st.put('Charm_Master_Index', `FANCY_${nm}_${L}`, Object.assign({}, e, { sku: `FANCY_${nm}_${L}` })); }
    await reloadMaster(); await page.evaluate(() => Engrave.render()); await sleep(300);
    const picks = await page.evaluate(() => { const d = document.querySelector('#egMono details.moAbcs'); return [...d.querySelectorAll('select')].map(s => [...s.options].map(o => o.value + '=' + o.textContent.trim())); });
    check(picks[0].length === 4 && picks[0].some(o => /FANCY_ONE_\{L\}=Fancy One · 26 letters/.test(o)), 'the three lettered families are offered in each picker: ' + JSON.stringify(picks[0]));
    const nf = (await fetched()).length;
    await page.selectOption('#egMono select[data-mo-abc="left"]', 'FANCY_ONE_{L}'); await page.selectOption('#egMono select[data-mo-abc="middle"]', 'FANCY_TWO_{L}'); await page.selectOption('#egMono select[data-mo-abc="right"]', 'FANCY_THREE_{L}');
    await page.waitForFunction(n => window.__fetched.slice(n).includes('FANCY_THREE_R'), nf, { timeout: 30000 }).catch(() => {}); a = await ready('4202783520', 30000);
    const f2 = (await fetched()).slice(nf);
    check(!a.spin && ['FANCY_ONE_O', 'FANCY_TWO_S', 'FANCY_THREE_R'].every(s => f2.includes(s)), 'the chosen alphabets are used for the letters: ' + [...new Set(f2)].join(' '));
    check(await page.evaluate(() => JSON.stringify(S.settings.mono) === JSON.stringify({ left: 'FANCY_ONE_{L}', middle: 'FANCY_TWO_{L}', right: 'FANCY_THREE_{L}' })), 'and the choice is kept in the settings');
    // everything back: Monogram Left / Mid / Right again, chosen "Automatic"
    for (const [k, v] of keep) st.docs.set(k, v);
    for (const slot of ['left', 'middle', 'right']) await page.selectOption(`#egMono select[data-mo-abc="${slot}"]`, '');
    for (const [nm] of Object.entries(FANCY)) for (const L of A.LETTERS) st.docs.delete(`Charm_Master_Index/FANCY_${nm}_${L}`);
    await reloadMaster(); await page.evaluate(() => Engrave.render()); await sleep(300);
    await page.evaluate(() => Engrave.monogramUI().rebuild(Engrave.monogramUI().cards.get('4202783520_42027835201')));
    a = await ready('4202783520', 30000); check(a.state === 'ready' || a.state === 'approved', 'the Monogram alphabets back, Automatic: the card is made again');

    // ═══ 7 · keys, phone, motion, a second tab, a reload ═══
    await page.evaluate(() => { CN.setMode('engrave'); Engrave.restoreView({ tab: 'mono', chosen: true }); Engrave.render(); });
    await sleep(300);
    // Tab order within a card that waits for its approval (J.Q.X)
    await page.focus(`${card('4200900001')} [data-mo-l="0"]`);
    const order = []; for (let i = 0; i < 7; i++) { order.push(await page.evaluate(() => { const n = document.activeElement; return n.dataset.moL != null ? 'L' + n.dataset.moL : n.dataset.moSwap != null ? 'swap' + n.dataset.moSwap : n.dataset.mo || n.tagName; })); await page.keyboard.press('Tab'); }
    check(order.slice(0, 6).join(' ') === 'L0 swap0 L1 swap1 L2 approve', 'Tab goes Left, swap, Middle, swap, Right, Approve: ' + order.join(' '));
    // Enter approves (Klm)
    await typeIn('4200900001', 2, 'N'); await ready('4200900001'); await page.keyboard.press('Enter'); a = await until('4200900001', r => r.state === 'approved', 'approval by Enter');
    check(a.state === 'approved' && (await entries()).some(e => e.files.some(f => f.name === 'Monogram JQN.ai')), 'Enter in a letter box approves the ready monogram: Monogram JQN.ai');
    // Enter while it is still being made approves it once it is made
    await typeIn('4200900001', 2, 'P'); await page.keyboard.press('Enter'); a = await until('4200900001', r => r.state === 'approved' && r.approve.hidden, 'approval after the remake', 30000);
    await sleep(500); const ek = (await entries()).find(e => e.files.some(f => /^Monogram JQ/.test(f.name)));
    check(ek && ek.files.length === 1 && ek.files[0].name === 'Monogram JQP.ai', 'Enter while the monogram is still being made approves it when it is ready, and replaces the earlier one: ' + JSON.stringify(ek && ek.files.map(f => f.name)));
    // the three orders approved: their pictures (the metal a person set on a design is the metal of its picture)
    if (REAL_OK) {
      const st3 = await Promise.all(TRIO.map(([, rid]) => read(rid)));
      console.log('   states before the approved pictures:', JSON.stringify(st3.map(r => r && [r.state, r.st])));
      const pa = await Promise.all(TRIO.map(([, rid]) => pictureOf(rid)));
      check(st3.every(r => r && r.state === 'approved') && pa.every(x => x && x.points >= POINT_FLOOR && x.same), 'the three approved cards still show the real letters: ' + st3.map(r => r && r.state).join(' ') + ' ' + pa.map(x => x && x.points).join(' '));
      check(pa[0].fill === TONE.silver, 'the metal a person set on the design (silver) is the metal of the picture: ' + pa[0].fill);
      await cardsShot('ui2-1440-approved');
    }

    // sent to the sheets: the card is final (boxes locked, no Approve), and it is no longer counted as waiting
    await page.evaluate(() => { CN.setMode('review'); Review.render(); });
    await page.waitForSelector('#rvList .reviewListRow[data-rid="4200900001"] [data-cu-send]', { timeout: 15000 });
    await page.click('#rvList .reviewListRow[data-rid="4200900001"] [data-cu-send]');
    await page.click('[data-role="laser"]', { timeout: 2500 }).catch(() => {});       // (the page's own "Laser or Design?" bar, when it asks)
    await page.waitForFunction(() => CustomSheet.decisionOf(B.orders.byKey.get('4200900001_42009000011')) && !document.querySelector('#rvList .cuStat, #rvList .btn.working'), null, { timeout: 40000 }).catch(() => {});
    await page.waitForFunction(() => !window.SendTour || !SendTour.playing(), null, { timeout: 40000 }).catch(() => {}); await sleep(900);   // (the send tour brings the Review tab back by itself when it ends)
    await page.evaluate(() => { CN.setMode('engrave'); Engrave.restoreView({ tab: 'mono', chosen: true }); Engrave.render(); }); await sleep(500);
    a = await read('4200900001'); const cnt = await page.evaluate(() => document.querySelector('#engraveView .egTab[data-tab="mono"] b').textContent);
    check(a && a.state === 'sent' && a.st === 'On the sheets' && a.approve.hidden && a.open.hidden && (await page.evaluate(() => [...document.querySelectorAll('.moCard[data-rid="4200900001"] input')].every(i => i.disabled))), 'a monogram sent to the sheets is final: "On the sheets", boxes locked, no Approve: ' + JSON.stringify([a && a.state, a && a.st]));
    check(+cnt === 7, 'and the tab no longer counts it as waiting (' + cnt + ' left)');
    await page.evaluate(() => { document.querySelector('#egMono .moCard[data-rid="4200683017"]').scrollIntoView({ block: 'start' }); });
    // reduced motion
    await page.emulateMedia({ reducedMotion: 'reduce' });
    holdOn(/Monogram_Mid_G\.ai/i); await typeIn('4202783520', 1, 'G'); await sleep(500);
    const rm = await page.evaluate(() => { const i = document.querySelector('.moCard[data-rid="4202783520"] .moSpin i'), s = document.querySelector('.moCard[data-rid="4202783520"] .moArt svg'); return { anim: getComputedStyle(i).animationName, shown: !document.querySelector('.moCard[data-rid="4202783520"] .moSpin').hidden, tr: s ? getComputedStyle(s).transitionDuration : '0s' }; });
    check(rm.anim === 'none' && rm.shown && /^0s/.test(rm.tr), 'reduced motion: the spinner is still shown but does not spin, the picture does not fade: ' + JSON.stringify(rm) + (rm.shown ? '' : ' ' + JSON.stringify(await read('4202783520')) + JSON.stringify(await page.evaluate(() => [...document.querySelectorAll('.moCard')].map(c => c.dataset.rid + ':' + c.dataset.state)))));
    release(); await ready('4202783520'); await page.emulateMedia({ reducedMotion: 'no-preference' });
    const mv = await page.evaluate(() => getComputedStyle(document.querySelector('.moSpin i')).animationName); check(mv === 'spin', 'with motion allowed the spinner spins: ' + mv);

    // phone width (the rail folded away, as a person on a phone would: the ☰ button; the page's own bars and toasts put away for the pictures)
    await page.evaluate(() => { document.querySelectorAll('.cnNbX, .toast .x').forEach(b => b.click()); });
    await page.setViewportSize({ width: 390, height: 844 }); await sleep(400);
    if (await page.evaluate(() => !document.getElementById('app').classList.contains('railOff'))) await page.click('#btnRail');
    await sleep(500);
    await page.evaluate(() => { document.querySelector('#egMono .moCard[data-rid="4200683017"]').scrollIntoView({ block: 'start' }); });
    const ph = await page.evaluate(() => { const c = document.querySelector('#egMono .moCard[data-rid="4200683017"]'), r = c.getBoundingClientRect(), ins = [...c.querySelectorAll('input')].map(i => i.getBoundingClientRect()), sc = document.querySelector('#egMono');
      return { left: r.left, right: r.right, w: innerWidth, ins: ins.map(b => [Math.round(b.left), Math.round(b.right)]), overflowX: Math.max(document.documentElement.scrollWidth, document.body.scrollWidth) - innerWidth, paneOver: sc.scrollWidth - sc.clientWidth, svg: (() => { const s = c.querySelector('.moArt svg'); const b = s && s.getBoundingClientRect(); return b ? [Math.round(b.width), Math.round(b.height)] : null; })() }; });
    check(ph.left >= 0 && ph.right <= ph.w + 1 && ph.ins.every(b => b[0] >= 0 && b[1] <= ph.w) && ph.overflowX <= 1 && ph.paneOver <= 1, 'phone width 390: the card and its three letter boxes fit, no sideways scroll: ' + JSON.stringify({ l: ph.left, r: ph.right, ins: ph.ins, overflowX: ph.overflowX, paneOver: ph.paneOver, svg: ph.svg }));
    await shot('ui-390-ready');
    holdOn(/Monogram_Mid_H\.ai/i); await typeIn('4200683017', 1, 'H'); await sleep(700); await shot('ui-390-building'); release(); await ready('4200683017');
    await page.evaluate(() => { document.querySelector('#egMono .moCard[data-rid="4200900012"]').scrollIntoView({ block: 'start' }); });
    st.docs.delete('Charm_Master_Index/MONOGRAM_MID_Y'); await reloadMaster(); await typeIn('4200900012', 1, 'Y'); await until('4200900012', r => r.state === 'alert', 'the missing letter on the phone'); await shot('ui-390-missing');
    await page.setViewportSize({ width: 1440, height: 900 }); await sleep(300);
    st.docs.set('Charm_Master_Index/MONOGRAM_MID_Y', midY); await reloadMaster();

    // a reload keeps the approved design; a second tab sees it
    await page.evaluate(async () => { try { await Session.flushNow(); } catch (_) {} });
    const apvNow = (await entries()).map(e => e.files.map(f => f.name).join()).sort();
    const page2 = await w.context.newPage(); page2.setDefaultTimeout(30000); page2.on('pageerror', e => w.errors.push('page2: ' + e.message));
    await page2.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page2.waitForFunction(() => window.CN && window.Orders && window.CustomSheet && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page2.evaluate(async () => { await Master.load(true); });
    await page2.evaluate(async ({ fixtures, orderSrc }) => {
      const orderOf = eval('(' + orderSrc + ')'), now = Date.now(); await Orders.loadMaps(true);
      for (const fx of fixtures) { const order = orderOf(fx, now); for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); if (B.orders.byKey.get(key)) continue; const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); } }
      Orders.interpretAll(); try { await CustomSheet.load(); } catch (_) {} CN.setMode('engrave'); Engrave.restoreView({ tab: 'mono', chosen: true }); Engrave.render();
    }, { fixtures: ['apv', 'osr', 'klm'].map(n => FX[n]), orderSrc: Base.orderOf.toString() });
    await page2.waitForSelector('.moCard[data-rid="4200683017"] svg', { timeout: 30000 });
    await sleep(1500);
    const p2 = await page2.evaluate(() => ({ files: Object.values(CustomSheet.entries()).map(e => e.files.map(f => f.name).join()).sort(), st: document.querySelector('.moCard[data-rid="4200683017"]').dataset.state, ins: [...document.querySelectorAll('.moCard[data-rid="4200683017"] input')].map(i => i.value).join('') }));
    console.log('   second tab / reload after the approvals:', JSON.stringify({ firstTab: apvNow, secondTab: p2.files, card: p2.st, letters: p2.ins }));
    check(p2.files.join('|') === apvNow.join('|') && p2.st === 'approved' && p2.ins === 'ASV', 'a second tab (a reload reads the same workspace) has the approved designs and shows the card as approved with the approved letters, not the buyer\'s: ' + JSON.stringify({ files: p2.files, card: p2.st, letters: p2.ins }));
    await page2.close();
    // ═══ Paul's own letters (the hard ones too: a Mid Q and a Mid T, a Right T apart from its flourish): ten words, each one piece with its counters, the centre bigger; every picture is kept as real-file-<word>.png ═══
    if (REAL_OK) {
      await page.evaluate(() => { CN.setMode('engrave'); Engrave.restoreView({ tab: 'mono', chosen: true }); Engrave.render(); }); await sleep(400);
      const sel = card('4200683017');
      await page.evaluate(s => document.querySelector(s).scrollIntoView({ block: 'start' }), sel); await sleep(200);
      for (const wd of ['APV', 'OSR', 'AQV', 'ATB', 'ZMT', 'HFT', 'JQX', 'TTT', 'WIW', 'LDH']) {
        await pasteWord('4200683017', wd);
        await page.waitForFunction(({ sel, wd }) => { const h = document.querySelector(sel); return h && h.querySelector('.moSpin').hidden && [...h.querySelectorAll('input')].map(i => i.value).join('') === wd && (h.dataset.state === 'ready' || h.dataset.state === 'approved'); }, { sel, wd }, { timeout: 90000 }).catch(() => {});
        const r = await page.evaluate(() => { const c = Engrave.monogramUI().cards.get('4200683017_42006830171'); if (!c.result) return null; const hOf = p => p.bboxPt[3] - p.bboxPt[1], pl = c.result.placed; return { ok: c.result.ok, letters: c.resultLetters, rings: c.result.rings.length, outer: c.result.rings.filter(x => !x.hole).length, report: c.result.report, wMm: c.result.wMm, hMm: c.result.hMm, centre: hOf(pl[1]) / Math.max(hOf(pl[0]), hOf(pl[2])) }; });
        await page.locator(sel + ' .moPic').screenshot({ path: path.join(SHOTS, 'real-file-' + wd + '.png') }).catch(() => {});
        const pic = await pictureOf('4200683017');
        check(!!r && r.letters === wd && r.outer === 1 && r.report.connected && r.report.holesKept >= 0.5 && r.centre >= 1.2 && !!pic && pic.same && pic.points >= POINT_FLOOR, `${wd} on Paul's letters: one piece, centre x${r && r.centre.toFixed(2)}, ${r && r.rings - 1} holes (kept ${r && r.report.holesKept}), ${r && r.wMm.toFixed(1)} × ${r && r.hMm.toFixed(1)} mm, ${pic && pic.points} points, joins ${JSON.stringify(r && r.report.joinMm)}`);
      }
    }
    check(w.errors.length === 0, 'no page error: ' + JSON.stringify(w.errors.slice(0, 3)));
  } catch (e) { fails.push('crash: ' + (e && e.stack || e)); console.error(e); try { await shot('ui-crash'); } catch (_) {} }
  finally { await w.close(); }
  console.log(`   (${Math.round((Date.now() - T0) / 1000)} s)`);
  console.log(fails.length ? `\nmonogram-ui: ${fails.length} FAILED of ${nCheck}:\n - ${fails.join('\n - ')}` : `\nmonogram-ui OK: ${nCheck} checks`);
  process.exit(fails.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
