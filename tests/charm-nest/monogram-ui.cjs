// MONOGRAM WIDGET in the real page (MONOGRAMUI 15 Oct 2026, MUW 16 Oct 2026): charm-nest-monogram-ui.js mountLine(), the ONE monogram component of an order line (the Engrave detail and the Order details window, in place of Front / Back), over the real sorter
// (charm-nest-1.html) in headless Chromium on the fake loopback site (bridge-server.cjs): no Etsy, no paid AI, nothing live. The master library holds three 26-letter alphabets named exactly as Paul's file names them (Monogram_Left_A … Monogram_Right_Z,
// upper-cased by the library: MONOGRAM_LEFT_A …), made from the vendored fonts with their real outlines and counters (_monogramAlphabets.cjs); orders go through the real intake (Orders.interpretAll). Every monogram line is mounted FOUR times in a
// test page (Engrave detail 760 px, Order window compact 380 px, each again at phone width 372 px): one state, four views.
//   1  the orders: the two monogram lines (4200683017 "APV", 4202783520 "O S R") and a title that says monogram with three letters are custom monograms; a bracelet with a name, a regular charm are not
//   2  the widget: the preview of the cut geometry in the line's metal, drawn from the real letters; letter 1 comes from the Left alphabet, 2 from the Middle, 3 from the Right; "The buyer wrote A P V"; ONE size line; ONE connected charm, the centre bigger,
//      the counters kept; the four mounts show one state
//   3  editing a letter: spinner, the widget's bar and the page's progress bar, one debounced remake, the newest edit always wins (a stale answer never overwrites), typing a letter back is instant, swap two neighbours, rotate, paste three letters, Esc
//   4  a missing letter is a plain message naming it; a letter file that fails to load once gives Try again and no stuck spinner; odd input ("A.P", "Jane Doe", 4 letters, digits, emoji) is a calm message and no build
//   5  Approve (MONOSEND, Paul 18 Oct 2026): the green Approve (the engraving cards' btn sage) is the ONE press: Monogram APV.ai on the order's custom designs (one piece, the right size), the seal, and the send to the nesting queue on the buyer's metal (GF), no designs window, no "Send to Sheet →" link; "On the sheet · Monogram APV" is final and a second press is nothing; a metal the order does not give (N.O.M) is never guessed: approved, not sent, said on the card, the designs window opens; Change there REPLACES the design (never two, the metal a person set kept) and that press sends it; monogram:approved
//   6  the page is told (monogram:change, onChange, onApproved), a host can hold Approve (gate), update / destroy, status(row) for the card's dot
//   7  the thumbnail for the Engrave piece card (thumbHtml / fillThumbs / thumb): the cut piece, still, repainted with every edit, built quietly (no bar, no AI)
//   8  the size option: the widest the finished piece may be (charm-nest-monogram-fit.js), the line says so, the file written is that size
//   9  the AI assembly line ("Assembled with AI: …"; MONOAPPROVE, 20 Oct 2026: a flagged word shows no red line, no Improve again, and ONE Approve press approves it); the AI itself is stubbed (tests/charm-nest/monogram-ai.cjs holds the rest)
//   10 the alphabets: none, partial (the pickers appear inside the widget), chosen by hand; keys (Tab order, Enter approves, Esc), sent to the sheets, reduced motion, phone width 400, a second tab, a reload keeps the approved design
//   12 (17 Oct 2026) MONOROUTE: a monogram line that asks nothing is NOT a card of the Review tab (approved or not; its custom card still exists and holds the design; a line with a question or a hold still is listed), and an approved one goes
//      to the sheets from its own Approve (MONOSEND); MONOLETTERS: after a rotate or a swap the buyer's line offers "Use these" (one press puts the buyer's letters back); MONOBIG: the compact preview is 139 px beside the letters,
//      159 px over them, inside the card from 300 px up, and the compact widget keeps one (taller) height in every state
//   11 Paul's own file (fixtures/SKUs_MONOGRAM_2024-1015.ai, 78 letters of three embedded fonts) indexed by the local script into the fake library: the three families found, 10 words built from his real letters (one piece, joins, counters)
//   Screenshots (the cases at 760 / 380 / 372 px, and the page at 1440 and 400) go to $SHOTS (default /tmp/mu/MU1/shots/<state>-<order>-<case>.png).
//   node tests/charm-nest/monogram-ui.cjs [playwright-core dir]      MONO_REAL=0 leaves Paul's own file out (stand-in alphabets, no pictures), MONO_FONTS=<dir with LEFT.ttf MIDDLE.ttf RIGHT.ttf> makes the stand-in alphabets from his fonts
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const Base = require('./_orderFactsWorld.cjs');
const A = require('./_monogramAlphabets.cjs');
const SHOTS = process.env.SHOTS || '/tmp/mu/MU1/shots';
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
FX.ev = { rid: '4200900021', sku: 'MONOGRAM_5351', listing: '997905351', title: MONO_TITLE, buyer: 'Name Ev', placed: { day: 0, h: 12, m: 1 }, ship: 5, variations: [V('Metal choice', 'Gold')], personalization: ['E.M.T'] };
FX.thm = { rid: '4200900022', sku: 'MONOGRAM_5351', listing: '997905351', title: MONO_TITLE, buyer: 'Name Thm', placed: { day: 0, h: 12, m: 2 }, ship: 5, variations: [V('Metal choice', 'Gold')], personalization: ['T.H.M'] };
FX.siz = { rid: '4200900024', sku: 'MONOGRAM_5351', listing: '997905351', title: MONO_TITLE, buyer: 'Name Siz', placed: { day: 0, h: 12, m: 3 }, ship: 5, variations: [V('Metal choice', 'Silver')], personalization: ['A.S.D'] };
FX.ai1 = { rid: '4200900025', sku: 'MONOGRAM_5351', listing: '997905351', title: MONO_TITLE, buyer: 'Name Ai1', placed: { day: 0, h: 12, m: 4 }, ship: 5, variations: [V('Metal choice', 'Gold')], personalization: ['P.Q.R'] };
FX.trb = { rid: '4200900031', sku: 'MONOGRAM_5351', listing: '997905351', title: MONO_TITLE, buyer: 'Name Trb', placed: { day: 0, h: 12, m: 6 }, ship: 5, variations: [V('Metal choice', 'Gold')], personalization: ['T.R.B'] };   // (MONOROUTE: a monogram line with a question staged on it)
FX.hld = { rid: '4200900032', sku: 'MONOGRAM_5351', listing: '997905351', title: MONO_TITLE, buyer: 'Name Hld', placed: { day: 0, h: 12, m: 7 }, ship: 5, variations: [V('Metal choice', 'Gold')], personalization: ['H.L.D'] };   // (MONOROUTE: a monogram line a person held)
FX.nom = { rid: '4200900041', sku: 'MONOGRAM_5351', listing: '997905351', title: MONO_TITLE, buyer: 'Name Nom', placed: { day: 0, h: 12, m: 8 }, ship: 5, variations: [V('Metal choice', 'Moonlight')], personalization: ['N.O.M'], metalKey: 'moonlight' };   // (MONOSEND: a metal no map knows: the line has none, nothing is guessed)
FX.ai2 = { rid: '4200900026', sku: 'MONOGRAM_5351', listing: '997905351', title: MONO_TITLE, buyer: 'Name Ai2', placed: { day: 0, h: 12, m: 5 }, ship: 5, variations: [V('Metal choice', 'Rose Gold')], personalization: ['W.N.K'] };

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  try { require(path.join(pwDir, 'playwright-core')); } catch (_) { (process.env.SKIP_BROWSER === '1' ? console.log : m => { console.error('FAIL: ' + m + ' (no browser here: set PW_DIR and CHROMIUM, or SKIP_BROWSER=1 to skip it on purpose)'); process.exit(1); })('monogram-ui: no playwright-core, skipped'); return; }
  process.env.PW_DIR = pwDir;
  fs.mkdirSync(SHOTS, { recursive: true });
  const T0 = Date.now(), fails = []; let nCheck = 0;
  const check = (ok, msg) => { nCheck++; if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const w = await Base.world({ viewport: { width: 1440, height: 900 } });
  if (!w) { (process.env.SKIP_BROWSER === '1' ? console.log : m => { console.error('FAIL: ' + m + ' (no browser here: set PW_DIR and CHROMIUM, or SKIP_BROWSER=1 to skip it on purpose)'); process.exit(1); })('monogram-ui: no playwright-core, skipped'); return; }
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
  // ── the widget under test: every monogram line is mounted four times in a test page (the Engrave detail at 760 px, the Order window's compact block at 380 px, and each again at phone width 372 px): ONE state, four views ──
  const CASES = { detail: { mode: 'detail', w: 760 }, compact: { mode: 'compact', w: 380 }, dphone: { mode: 'detail', w: 372 }, cphone: { mode: 'compact', w: 372 } };
  const W = (rid, k = 'detail') => `.mwCase[data-rid="${rid}"][data-case="${k}"] .mw`;
  const mountCases = (rids, which = Object.keys(CASES)) => page.evaluate(({ rids, which, CASES }) => {
    let rootEl = document.getElementById('mwRoot');
    if (!rootEl) { rootEl = document.createElement('div'); rootEl.id = 'mwRoot'; rootEl.style.cssText = 'position:fixed;inset:0;z-index:2147483000;overflow:auto;background:#faf7f2;padding:16px;display:flex;flex-wrap:wrap;gap:14px;align-content:flex-start'; document.body.append(rootEl); }
    rootEl.style.display = 'flex';
    for (const rid of rids) for (const k of which) {
      let cs = rootEl.querySelector(`.mwCase[data-rid="${rid}"][data-case="${k}"]`);
      if (!cs) {
        cs = document.createElement('div'); cs.className = 'mwCase'; cs.dataset.rid = rid; cs.dataset.case = k;
        cs.style.cssText = `box-sizing:border-box;width:${CASES[k].w}px;max-width:100%;border:1px solid var(--line);border-radius:12px;background:var(--card);padding:${CASES[k].mode === 'compact' ? 10 : 16}px;`;
        const host = document.createElement('div'); host.className = 'mwHost'; cs.append(host); rootEl.append(cs);
      }
      const row = Orders.rows().find(r => String(r.order.receiptId) === rid && r.spec && r.spec.special && r.spec.special.monogram);
      const w = CharmNestMonogramUI.mountLine(cs.querySelector('.mwHost'), { rid, lineKey: row && row.key, mode: CASES[k].mode });
      if (!w) throw new Error('no widget for ' + rid);
    }
  }, { rids, which, CASES });
  const hideCases = (on = true) => page.evaluate(on => { const r = document.getElementById('mwRoot'); if (r) r.style.display = on ? 'none' : 'flex'; }, on);
  const read = (rid, k = 'detail') => page.evaluate(sel => {
    const h = document.querySelector(sel); if (!h) return null; const q = s => h.querySelector(s), txt = (s, inner) => { const e = q(s); return !e || e.hidden ? '' : (inner ? q(inner) : e).textContent.replace(/\s+/g, ' ').trim(); };
    const btn = s => { const e = q(s); return e ? { text: e.textContent.replace(/\s+/g, ' ').trim(), hidden: e.hidden, disabled: !!e.disabled, title: e.title || '' } : null; };
    return { state: h.dataset.state, busy: h.dataset.busy, mode: h.dataset.mode, says: txt('[data-mw-says]'), saysHidden: q('[data-mw-says]').hidden, size: txt('[data-mw-size]'), asm: txt('[data-mw-assembly]', '[data-mw-assembly] > span'), asmStatus: q('[data-mw-assembly]').hidden ? '' : q('[data-mw-assembly]').dataset.status,
      note: txt('[data-mw-note]'), msg: txt('[data-mw-msg]'), kind: q('[data-mw-msg]').hidden ? '' : q('[data-mw-msg]').dataset.kind, retry: !!q('[data-mw-retry]'), spin: !q('[data-mw-spin]').hidden, spinTx: q('[data-mw-spin] span').textContent, aria: q('[data-mw-pic]').getAttribute('aria-busy'),
      hint: q('.mwHint') ? q('.mwHint').textContent : '', d: q('[data-mw-pic] svg path') ? q('[data-mw-pic] svg path').getAttribute('d') : '', ins: [...h.querySelectorAll('[data-mw-letter]')].map(i => i.value), locked: [...h.querySelectorAll('[data-mw-letter]')].every(i => i.disabled), invalid: [...h.querySelectorAll('[data-mw-letter]')].map(i => i.getAttribute('aria-invalid')),
      buyerBtn: (() => { const e = q('[data-mw-buyer]'); return e && !e.closest('[hidden]') ? e.textContent.replace(/\s+/g, ' ').trim() : null; })(), approve: btn('[data-mw-approve]'), ok: btn('[data-mw-approved]'), change: btn('[data-mw-change]'), sheetLink: !!q('[data-mw-sheet]'), queue: h.dataset.queue || '', again: btn('[data-mw-again]'), bar: q('[data-mw-bar]') ? { ind: q('[data-mw-bar]').classList.contains('ind'), w: q('[data-mw-bar] b').style.width } : null };
  }, W(rid, k));
  const until = async (rid, fn, what, ms = 20000, k = 'detail') => { const t0 = Date.now(); let r; for (;;) { r = await read(rid, k); if (r && fn(r)) return r; if (Date.now() - t0 > ms) { console.log('   (waiting for ' + what + ', last: ' + JSON.stringify(r).slice(0, 360) + ')'); return r; } await sleep(60); } };
  const ready = (rid, ms, k) => until(rid, r => !r.spin && (r.state === 'ready' || r.state === 'approved' || r.state === 'check'), 'a finished monogram', ms, k);
  const builds = () => page.evaluate(() => window.__builds.slice());
  const fetched = () => page.evaluate(() => window.__fetched.slice());
  const typeIn = async (rid, i, text, k = 'detail') => { await page.click(`${W(rid, k)} [data-mw-letter="${i}"]`); await page.keyboard.type(text); };
  const HH = {};      // (the compact widget's height in every state: the Order window must not jump)
  const heightOf = (rid, k = 'compact') => page.evaluate(sel => document.querySelector(sel).getBoundingClientRect().height, W(rid, k));
  const wstate = (rid, k = 'detail') => page.evaluate(({ rid, k }) => { const w = CharmNestMonogramUI.widgets().find(x => x.el && x.el.matches(`.mwCase[data-rid="${rid}"][data-case="${k}"] .mw`)); return w ? w.state() : null; }, { rid, k });
  const shot = async name => { try { await page.screenshot({ path: path.join(SHOTS, name + '.png') }); } catch (e) { console.log('   (no screenshot ' + name + ')'); } };
  // the pictures of a state: every case of the given lines, 1440-wide (760 / 380) and phone-wide (372), each as the element alone (no page around it)
  const caseShots = async (name, rids, which = Object.keys(CASES)) => {
    for (const rid of rids) for (const k of which) { const loc = page.locator(`.mwCase[data-rid="${rid}"][data-case="${k}"]`); if (await loc.count()) await loc.screenshot({ path: path.join(SHOTS, `${name}-${rid}-${k}.png`) }).catch(() => console.log('   (no screenshot ' + name + '-' + rid + '-' + k + ')')); }
  };
  const seedOrders = names => page.evaluate(async ({ fixtures, orderSrc }) => {
    const orderOf = eval('(' + orderSrc + ')'), now = Date.now(); await Orders.loadMaps(true);
    for (const fx of fixtures) { const order = orderOf(fx, now); for (const line of order.lines) { if (fx.metalKey) line.metalKey = fx.metalKey; const key = CharmNestOrders.lineKey(order, line); if (B.orders.byKey.get(key)) continue; const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); } }
    Orders.interpretAll();
  }, { fixtures: names.map(n => FX[n]), orderSrc: Base.orderOf.toString() });
  const instrument = () => page.evaluate(() => {
    window.__builds = []; window.__fetched = []; window.__bars = [];
    if (!window.__wrapped) {
      window.__wrapped = true; const eb = CharmNestMonogram.build; CharmNestMonogram.build = function (l, o) {
        window.__builds.push(l.left.letter + l.middle.letter + l.right.letter); const r = eb.apply(this, arguments);
        // (MONOAI: this test is about the card, not the engine's rules: every word is staged as meeting them, so no card is flagged "Needs a person" (a second press on Approve) and nothing asks the AI; that flow is tests/charm-nest/monogram-ai.cjs)
        if (r && r.ok && r.report && r.report.metrics && Array.isArray(r.report.metrics.violations)) {
          const tag = l.left.letter + l.middle.letter + l.right.letter, a = r.report.adjust, fixed = !!(a && a.L && a.L.dx >= 0.3);
          // (words in window.__violate break a rule until the (stubbed) model moves the left letter: the AI section; every other word meets them)
          r.report.metrics = Object.assign({}, r.report.metrics, window.__violate && window.__violate.includes(tag) && !fixed ? { violations: [{ rule: 'contacts', key: 'contacts', text: 'the left and middle letters are not held by one contact near the top and one near the bottom' }], distance: 3, ok: false } : { violations: [], ok: true });
        }
        return r;
      };
      // (the page builds a word that breaks a rule through the engine's own search first, Engine.autoFit, and asks the AI only if that still fails: the verdict is staged there too, the same for every word; the engine's letters change, this test must not depend on them)
      const ef = CharmNestMonogram.autoFit; if (ef) CharmNestMonogram.autoFit = function (l, o) {
        const r = ef.apply(this, arguments);
        if (r && r.ok && r.report && r.report.metrics && Array.isArray(r.report.metrics.violations)) {
          const tag = l.left.letter + l.middle.letter + l.right.letter;
          r.report.metrics = Object.assign({}, r.report.metrics, window.__violate && window.__violate.includes(tag) ? { violations: [{ rule: 'contacts', key: 'contacts', text: 'the left and middle letters are not held by one contact near the top and one near the bottom' }], distance: 3, ok: false } : { violations: [], ok: true });
        }
        return r;
      };
      const ps = CNProgress.start; CNProgress.start = function (label) { window.__bars.push(String(label)); return ps.apply(this, arguments); };
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
  const pasteWord = (rid, wd, k = 'detail') => page.evaluate(({ sel, wd }) => { const t = document.querySelector(sel + ' [data-mw-letter="0"]'); t.focus(); const dt = new DataTransfer(); dt.setData('text', wd); t.dispatchEvent(new ClipboardEvent('paste', { clipboardData: dt, bubbles: true, cancelable: true })); }, { sel: W(rid, k), wd });
  // what the picture of a widget is: its path, how many points it has, and whether it is exactly the geometry the engine made (the cut piece)
  const pictureOf = rid => page.evaluate(sel => { const h = document.querySelector(sel), pa = h && h.querySelector('[data-mw-pic] svg path'); if (!pa) return null; const ui = Engrave.monogramUI(), c = ui.cards.get(h.dataset.key), d = pa.getAttribute('d');
    const cs = getComputedStyle(pa), bb = pa.getBoundingClientRect(), box = h.querySelector('[data-mw-pic]').getBoundingClientRect();
    return { points: (d.match(/[ML]/g) || []).length, subpaths: (d.match(/M/g) || []).length, same: d === CharmNestMonogram.svgPath(c.result), fill: pa.getAttribute('fill'), stroke: pa.getAttribute('stroke'), rule: pa.getAttribute('fill-rule'), nonScaling: pa.getAttribute('vector-effect'), box: [Math.round(box.width), Math.round(box.height)],
      margins: [bb.left - box.left, box.right - bb.right, bb.top - box.top, box.bottom - bb.bottom].map(v => Math.round(v)), dpr: devicePixelRatio, rings: c.result.rings.length, holes: c.result.rings.filter(r => r.hole).length }; }, W(rid));
  const TONE = { gold: '#d9bd78', silver: '#c8ccd2', rose: '#e2b4a1' };
  // (the sides are normalised to 0.80 of the centre by BODY height, their ink reaches 0.85 to 0.95 of the centre's: the centre is bigger by 1.05 or more, not by the 1.2 of the first rule)
  const POINT_FLOOR = 1500;    // (points in the path of a monogram of Paul's script letters; the block serif stand-ins make a few hundred)
  const COMPACT_MAX_H = 285;   // (the compact widget's reserved height in the stacked layout, MONOBIG: picture 159 px; ~277 measured)
  const PIC_WIDE = 139, PIC_STACKED = 159;      // (MONOBIG, Paul 17 Oct 2026: the compact preview, beside the letters / over them when the card is too narrow for both)

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

    // ═══ 2 · the widget, in both densities (four mounts of every line: one state) ═══
    await instrument();
    await page.evaluate(() => { Engrave.monogramUI(); });
    const T3 = TRIO.map(t => t[1]);
    await mountCases(T3);
    await page.waitForSelector(`${W('4200683017')} svg path`, { timeout: 30000 });
    for (const r of T3) await ready(r);
    check(await page.evaluate(() => CharmNestMonogramUI.widgets().length) === 12 && await page.evaluate(() => CharmNestMonogramUI.widgets().every(w => w.el && w.el.classList.contains('mw'))), 'three monogram lines mounted four times each (Engrave detail, Order window compact, each at phone width): 12 live widgets');
    check(await page.evaluate(() => CharmNestMonogramUI.isMonogram(Orders.rows().find(r => r.order.receiptId === '4200683017')) && !CharmNestMonogramUI.isMonogram(Orders.rows().find(r => r.order.receiptId === '4200900004')) && CharmNestMonogramUI.mountLine(document.createElement('div'), { rid: '4200900004' }) === null), 'a regular charm is no monogram line: isMonogram false, mountLine gives null (the caller keeps its own card)');
    let a = await read('4200683017');
    check(a.state === 'ready' && a.d.length > 500 && a.ins.join('') === 'APV' && a.saysHidden && a.says === '' && a.buyerBtn === null, 'APV: the preview is drawn from the cut geometry; the letters are the buyer\'s own, so the widget does not repeat the words the order\'s strip already shows (and has no "Use these" to offer)');
    check(/^\d+\.\d × \d+\.\d mm · Gold$/.test(a.size) && a.asm === '' && a.msg === '' && !a.ok.text.length === false, 'one size line, in the line\'s metal, nothing else to say: "' + a.size + '"');
    const four = await Promise.all(Object.keys(CASES).map(k => read('4200683017', k)));
    check(four.every(x => x.state === 'ready' && x.ins.join('') === 'APV' && x.d === a.d && x.size === a.size) && four[1].saysHidden && four[1].mode === 'compact' && four[0].mode === 'detail', 'all four mounts show ONE state (same picture, letters, size); the compact one keeps the buyer\'s line to itself until it matters');
    const f1 = await fetched();
    check(['MONOGRAM_LEFT_A', 'MONOGRAM_MID_P', 'MONOGRAM_RIGHT_V'].every(s => f1.includes(s)) && !f1.includes('MONOGRAM_LEFT_P') && !f1.includes('MONOGRAM_MID_A') && !f1.includes('MONOGRAM_RIGHT_A'), 'letter 1 came from the Left alphabet, letter 2 from the Middle, letter 3 from the Right: ' + [...new Set(f1)].filter(s => /A$|P$|V$/.test(s)).sort().join(' '));
    check((await fetched()).filter(s => s === 'MONOGRAM_MID_P').length === 1, 'and four mounts fetched each letter file once');
    check(await page.evaluate(() => { const abc = Engrave.monogramUI().alphabets(); return abc.ok && ['left', 'middle', 'right'].map(k => abc[k].name).join(' · ') === 'Monogram Left · Monogram Mid · Monogram Right' && ['left', 'middle', 'right'].every(k => abc.by[k] === 'name'); }), 'the three alphabets were found by their names, nothing to pick: Monogram Left · Monogram Mid · Monogram Right');
    const rs = await page.evaluate(() => { const c = Engrave.monogramUI().cards.get('4200683017_42006830171'); const r = c.result, pl = r.placed, hOf = p => p.bboxPt[3] - p.bboxPt[1];
      return { ok: r.ok, connected: r.report.connected, outer: r.rings.filter(x => !x.hole).length, holes: r.rings.filter(x => x.hole).length, kept: r.report.holesKept, slots: pl.map(p => p.slot + ':' + p.letter).join(' '), mid: hOf(pl[1]), side: Math.max(hOf(pl[0]), hOf(pl[2])), cs: r.report.centerScale }; });
    check(rs.ok && rs.connected && rs.outer === 1 && rs.holes >= 1 && rs.kept >= (REAL_OK ? 0.5 : 0.9), 'ONE connected charm with its counters: ' + JSON.stringify({ outer: rs.outer, holes: rs.holes, kept: rs.kept }));
    check(rs.slots === 'left:A middle:P right:V' && rs.mid >= 1.05 * rs.side, `the letters sit Left A, Middle P, Right V and the centre is bigger than the sides (${rs.mid.toFixed(1)} vs ${rs.side.toFixed(1)} pt, x${(rs.mid / rs.side).toFixed(2)})`);
    a = await read('4202783520');
    check(a.ins.join('') === 'OSR' && a.saysHidden && a.state === 'ready' && /Silver$/.test(a.size), 'OSR: "' + a.says + '", ' + a.size);
    await shot('mw-1440-ready'); await caseShots('ready', T3); HH.ready = [await heightOf('4200683017'), await heightOf('4200683017', 'cphone')];
    // MONOBIG (Paul, 17 Oct 2026: "the preview thumbnail for the monogram is too small to see the necessary detail. Please enlarge by 66% and ensure it still fits within the bounds of the monogram card"): the compact picture is
    // 139 px (was 84) beside the letters, 159 px (was 96) over them when the card is too narrow for both; in a card of any width from a 320 px phone up it stays inside the card, nothing sideways
    { const widths = [440, 420, 395, 380, 340, 320, 300];
      const probe = await page.evaluate(async ({ rid, widths }) => {
        const rootEl = document.getElementById('mwRoot'), row = Orders.rows().find(r => String(r.order.receiptId) === rid && r.spec && r.spec.special && r.spec.special.monogram), out = [];
        for (const wpx of widths) {
          const cs = document.createElement('div'); cs.className = 'mwCase'; cs.dataset.rid = rid; cs.dataset.case = 'probe';
          cs.style.cssText = `box-sizing:border-box;width:${wpx}px;max-width:100%;border:1px solid var(--line);border-radius:12px;background:var(--card);padding:10px;`;
          const host = document.createElement('div'); host.className = 'mwHost'; cs.append(host); rootEl.append(cs);
          const w = CharmNestMonogramUI.mountLine(host, { rid, lineKey: row.key, mode: 'compact' });
          const t0 = Date.now(); while (Date.now() - t0 < 15000) { const m = cs.querySelector('.mw'); if (m && m.querySelector('[data-mw-pic] svg path') && m.querySelector('[data-mw-spin]').hidden) break; await new Promise(r => setTimeout(r, 50)); }
          const m = cs.querySelector('.mw'), pic = m.querySelector('[data-mw-pic]').getBoundingClientRect(), box = cs.getBoundingClientRect(), mr = m.getBoundingClientRect(), ls = m.querySelector('.mwLtrs').getBoundingClientRect();
          const out1 = [...m.querySelectorAll('*')].filter(n => { const r = n.getBoundingClientRect(); return r.width > 0 && (r.right > box.right - 11 + 1 || r.left < box.left + 11 - 1); }).length;
          out.push({ wpx, picW: Math.round(pic.width * 10) / 10, picH: Math.round(pic.height * 10) / 10, stacked: pic.bottom <= ls.top + 1 && pic.right > ls.left, beside: pic.right <= ls.left + 1, inside: pic.left >= box.left + 11 - 1 && pic.right <= box.right - 11 + 1, runs: out1, over: m.scrollWidth - m.clientWidth, mwW: Math.round(mr.width), h: Math.round(mr.height * 10) / 10 });
          w.destroy(); cs.remove();
        }
        return out;
      }, { rid: '4200683017', widths });
      console.log('   the compact picture by card width:', JSON.stringify(probe));
      const wide = probe.filter(p => p.beside), stk = probe.filter(p => p.stacked);
      check(wide.length >= 2 && wide.every(p => Math.abs(p.picW - PIC_WIDE) <= 1), `the compact picture is ${PIC_WIDE} px wide beside the letters (cards ${wide.map(p => p.wpx).join('/')} px): ` + wide.map(p => p.picW).join('/'));
      check(stk.length >= 3 && stk.every(p => Math.abs(p.picW - PIC_STACKED) <= 1), `and ${PIC_STACKED} px wide over the letters when the card is too narrow for both (cards ${stk.map(p => p.wpx).join('/')} px): ` + stk.map(p => p.picW).join('/'));
      check(probe.every(p => (p.beside || p.stacked) && p.inside && p.runs === 0 && p.over <= 1), 'in every card width from 300 to 440 px the picture and everything else stay inside the card: ' + probe.map(p => p.wpx + ':' + (p.inside && p.runs === 0 && p.over <= 1 ? 'in' : 'OUT')).join(' '));
      check(probe.every(p => p.picH >= p.picW * 0.7 && p.picH <= p.picW * 0.8), 'and the picture keeps its 4:3 shape (' + probe.map(p => p.picW + '×' + p.picH).join(' ') + ')'); }
    // the picture is the CUT piece of Paul's real script letters: the real glyph outlines (thousands of points, block letters never reach it), filled with the order's metal, a thin cut line round the outline and every counter, centred with an even margin
    if (REAL_OK) {
      const pics = {}; for (const [wd, rid] of TRIO) pics[wd] = await pictureOf(rid);
      console.log('   the pictures:', JSON.stringify(pics));
      check(TRIO.every(([wd]) => pics[wd] && pics[wd].same && pics[wd].points >= POINT_FLOOR && pics[wd].holes >= 1), `the picture of each order is the geometry the engine cut from Paul's real letters, ${POINT_FLOOR}+ points (block serif letters stay under it): ` + TRIO.map(([wd]) => wd + ' ' + (pics[wd] && pics[wd].points)).join(' · '));
      check(pics.APV.fill === TONE.gold && pics.OSR.fill === TONE.silver && pics.JQX.fill === TONE.rose && TRIO.every(([wd]) => /^#[0-9a-f]{6}$/.test(pics[wd].stroke) && pics[wd].rule === 'evenodd' && pics[wd].nonScaling === 'non-scaling-stroke'), `filled with the order's metal (APV gold, OSR silver, JQX rose gold), one thin dark cut line, counters open: ` + TRIO.map(([wd]) => wd + ' ' + pics[wd].fill + '/' + pics[wd].stroke).join(' · '));
      check(TRIO.every(([wd]) => { const m = pics[wd].margins; return Math.abs(m[0] - m[1]) <= 3 && Math.abs(m[2] - m[3]) <= 3 && Math.min(...m) >= 6; }), 'centred with an even margin, inside the picture box: ' + TRIO.map(([wd]) => wd + ' ' + pics[wd].margins.join('/')).join(' · '));
      // a word whose join has a warning (the first of these that the engine warns about; the widget shows it quietly under the size)
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
        check(wr.every((r, i) => r.state === 'ready' && (r.note.includes(warns[i][1].charAt(0).toUpperCase() + warns[i][1].slice(1)) || (/overlap|joined|close together|inner openings|too wide|too high|hidden|balance|differ/.test(r.note) && !/\d/.test(r.note)))), 'a word whose join has a warning shows it (as the engine wrote it, or in plain words when the engine states a rule with numbers), once, beside the size: ' + wr.map((r, i) => warns[i][0] + ' "' + r.note.slice(0, 120) + '"').join(' | '));
        await caseShots('warning', T3.slice(0, warns.length), ['detail', 'compact']); HH.warning = [await heightOf(TRIO[0][1]), await heightOf(TRIO[0][1], 'cphone')];
        for (let i = 0; i < warns.length; i++) await pasteWord(TRIO[i][1], TRIO[i][0]);
        for (let i = 0; i < warns.length; i++) await until(TRIO[i][1], r => !r.spin && r.state === 'ready' && r.ins.join('') === TRIO[i][0], 'the buyer\'s letters again', 40000);
      } else console.log('   (the engine warns about none of the words tried: no warning picture)');
    }

    // ═══ 3 · editing the letters ═══
    const b0 = (await builds()).length;
    await page.click(`${W('4200683017')} [data-mw-letter="1"]`); await page.keyboard.type('S');
    a = await read('4200683017'); let ac = await read('4200683017', 'compact');
    check(a.state === 'building' && a.spin && a.aria === 'true' && a.approve.disabled && ac.state === 'building' && ac.spin && ac.ins.join('') === 'ASV', 'a letter typed: the widget is busy at once (spinner on the picture, Approve waits), and so is the compact one, already showing A S V: ' + a.state);
    a = await ready('4200683017');
    // (MONOLETTERS, 17 Oct 2026: a card whose letters differ from the buyer's says what they wrote AND offers one press to put them back: "The buyer wrote A P V · Use these")
    check(a.state === 'ready' && a.ins.join('') === 'ASV' && a.says === 'The buyer wrote A P V · Use these' && a.buyerBtn === 'Use these' && !a.spin && a.aria === 'false', 'the monogram was remade for A S V; the buyer\'s own line still says A P V, with a "Use these" button: "' + a.says + '"');
    check((await builds()).slice(b0).join() === 'ASV', 'one build for the edit (not one per mount): ' + (await builds()).slice(b0).join());
    check((await read('4200683017', 'cphone')).saysHidden === false, 'and the compact widget now shows what the buyer wrote, because the letters differ from it');
    { const bb = (await builds()).length; await typeIn('4200683017', 1, 'T'); await page.keyboard.type('X'); await ready('4200683017');   // two edits within the wait: one remake of the last
      const nb = (await builds()).slice(bb); check(nb.join() === 'ATX', 'two quick edits make ONE monogram (the last): ' + nb.join()); }
    // the progress bar while the letters are fetched (one letter file held back)
    holdOn(/Monogram_Mid_[BCD]\.ai/i); await typeIn('4200683017', 1, 'B'); await typeIn('4202783520', 1, 'C'); await typeIn('4200900001', 1, 'D');
    await page.waitForFunction(() => /Building monogram/.test((document.querySelector('.cnp.on') || {}).textContent || ''), null, { timeout: 8000 }).catch(() => {});
    a = await read('4200683017');
    check(a.state === 'building' && a.busy === 'building' && a.spin && /Making the monogram/.test(a.spinTx) && a.bar && !a.bar.ind && /^\d+%$/.test(a.bar.w), 'while a letter file is on its way the spinner stays and the widget\'s own bar shows how far it is: ' + JSON.stringify([a.state, a.spinTx, a.bar]));
    check(await page.evaluate(() => { const n = document.querySelector('.cnp.on'); return !!n && /Building monogram [A-Z]{3}/.test(n.textContent); }), 'and the page\'s progress bar says "Building monogram …"');
    await shot('mw-1440-building'); await caseShots('building', T3); HH.building = [await heightOf('4200683017'), await heightOf('4200683017', 'cphone')];
    if (REAL_OK) { const bp = await Promise.all(TRIO.map(([, rid]) => pictureOf(rid)));
      check(bp.every(x => x && x.points >= POINT_FLOOR && x.same), 'while the three are remade each keeps its old picture (the real letters) under the spinner: ' + bp.map(x => x && x.points).join(' ')); }
    release(); await ready('4200683017'); await ready('4202783520'); await ready('4200900001');
    await typeIn('4202783520', 1, 'S'); await typeIn('4200900001', 1, 'Q'); await ready('4202783520'); await ready('4200900001');
    // a second edit while the first is still being fetched: only the newest is ever shown
    holdOn(/Monogram_Mid_K\.ai/i);
    const bs = (await builds()).length;
    await typeIn('4200683017', 1, 'K');
    await sleep(900);   // (the build for K is on its way and held)
    check((await read('4200683017')).state === 'building', 'the first edit (K) is still being fetched');
    await typeIn('4200683017', 1, 'U'); const u = await ready('4200683017');
    check(u.state === 'ready' && u.ins.join('') === 'AUX' && (await builds()).slice(bs).join() === 'AUX', 'the second edit (U) finished first and is shown: ' + (await builds()).slice(bs).join());
    release(); await sleep(1200);
    { const after = await read('4200683017'), nb = (await builds()).slice(bs);
      check(after.d === u.d && after.ins.join('') === 'AUX' && after.state === 'ready' && !nb.includes('AKX'), 'the older edit finishing later never overwrites: the picture is unchanged and no build was made for it: ' + nb.join()); }
    // typing a letter back is instant (the monogram is remembered)
    { const bk = (await builds()).length; await typeIn('4200683017', 1, 'S'); await ready('4200683017'); await typeIn('4200683017', 1, 'U'); const t0 = Date.now(); await until('4200683017', r => r.state === 'ready' && r.ins.join('') === 'AUX' && r.d === u.d, 'the remembered monogram', 8000); const dt = Date.now() - t0;
      check((await builds()).slice(bk).join() === 'ASX' && dt < 1500, `typing a letter back shows the monogram made before, no new build (${dt} ms): ` + (await builds()).slice(bk).join()); }
    // swap and rotate
    await page.click(`${W('4200683017')} [data-mw-swap="0"]`); a = await ready('4200683017');
    check(a.ins.join('') === 'UAX' && (await builds()).slice(-1)[0] === 'UAX', 'swap Left and Middle: U A X, remade');
    await page.click(`${W('4200683017')} [data-mw-swap="0"]`); await ready('4200683017');
    await page.click(`${W('4200683017')} [data-mw-rotate]`); a = await ready('4200683017');
    check(a.ins.join('') === 'UXA' && (await builds()).slice(-1)[0] === 'UXA', 'rotate moves every letter one place: A U X becomes U X A, remade');
    await page.click(`${W('4200683017')} [data-mw-rotate]`); await page.click(`${W('4200683017')} [data-mw-rotate]`); a = await ready('4200683017'); check(a.ins.join('') === 'AUX', 'three rotations are back where they began');
    // "Use these" (MONOLETTERS, 17 Oct 2026: "the system did not use the buyer's requested letters"): a rotate press turns A P V into P V A; the line says what the buyer wrote and one press puts the buyer's letters back
    await page.click(`${W('4200683017')} [data-mw-rotate]`); a = await ready('4200683017');
    check(a.ins.join('') === 'UXA' && !a.saysHidden && a.buyerBtn === 'Use these' && /^The buyer wrote A P V/.test(a.says), 'after a rotate press (U X A) the widget offers the buyer\'s letters back: "' + a.says + '"');
    ac = await read('4200683017', 'compact');
    check(ac.ins.join('') === 'UXA' && !ac.saysHidden && ac.buyerBtn === 'Use these', 'and the compact widget of the line offers them too (one state)');
    { const bu = (await builds()).length;
      await page.click(`${W('4200683017')} [data-mw-buyer]`); a = await ready('4200683017'); ac = await read('4200683017', 'compact');
      check(a.ins.join('') === 'APV' && ac.ins.join('') === 'APV' && a.state === 'ready' && a.saysHidden && a.buyerBtn === null && ac.buyerBtn === null && !a.invalid.includes('true'), 'Use these puts the buyer\'s A P V back in every mount; the line and the button go away: ' + a.ins.join('') + '/' + ac.ins.join(''));
      check((await builds()).slice(bu).every(x => x === 'APV') && (await builds()).length - bu <= 1, 'and at most one build was made for it (the monogram of A P V is remembered): ' + ((await builds()).slice(bu).join() || 'none'));
      // the same press from the compact widget (a swap of the first two letters first)
      await page.click(`${W('4200683017', 'compact')} [data-mw-swap="0"]`); await ready('4200683017', 20000, 'compact'); const cs = await read('4200683017', 'compact');
      check(cs.ins.join('') === 'PAV' && cs.buyerBtn === 'Use these', 'a swap in the compact widget (P A V) offers them there');
      await page.click(`${W('4200683017', 'compact')} [data-mw-buyer]`); const cb = await ready('4200683017', 20000, 'compact');
      check(cb.ins.join('') === 'APV' && cb.buyerBtn === null && (await read('4200683017')).ins.join('') === 'APV', 'and pressed there it puts A P V back in the detail widget too'); }
    // paste
    await pasteWord('4200683017', 'P.V.A');
    a = await ready('4200683017'); check(a.ins.join('') === 'PVA' && a.state === 'ready', 'three letters pasted into any box fill all three: ' + a.ins.join(''));
    // Esc takes an edit back
    await typeIn('4200683017', 0, 'Q'); await page.keyboard.press('Escape'); a = await ready('4200683017');
    check(a.ins.join('') === 'PVA' && a.state === 'ready', 'Esc in a letter box takes the edit back: ' + a.ins.join(''));
    // back to the buyer's own letters
    await pasteWord('4200683017', 'APV'); await ready('4200683017');
    // a repaint never takes a caret from a letter box
    await page.click(`${W('4200683017')} [data-mw-letter="2"]`);
    const same = await page.evaluate(() => { const n = document.activeElement; CharmNestMonogramUI.widgets().forEach(w => w.refresh()); CharmNestMonogramUI.widgets().forEach(w => w.refresh()); return n === document.activeElement && n.matches('[data-mw-letter="2"]'); });
    check(same, 'a refresh of the widgets leaves the focused letter box (and its caret) alone');

    // ═══ 4 · a missing letter, a load that fails once, odd input ═══
    const midY = st.docs.get('Charm_Master_Index/MONOGRAM_MID_Y');
    st.docs.delete('Charm_Master_Index/MONOGRAM_MID_Y'); await reloadMaster();
    check(await page.evaluate(() => CharmNestMonogram.familiesOf([...B.master.entries.keys()]).find(f => f.base === 'MONOGRAM_MID_{L}').missing.join('')) === 'Y', 'the library now lacks MONOGRAM_MID_Y (familiesOf names it)');
    const nbq = (await builds()).length;
    await typeIn('4200683017', 1, 'Y');
    a = await until('4200683017', r => r.state === 'error', 'the missing-letter message');
    check(a.kind === 'missing' && /Middle alphabet has no “Y”/.test(a.msg) && /Monogram Mid has no Y yet/.test(a.msg) && !/MONOGRAM_MID_Y/.test(a.msg), 'a missing letter is a plain message naming it: "' + a.msg + '"');
    check(!a.spin && a.aria === 'false' && a.approve.disabled && !(await builds()).slice(nbq).some(x => x[1] === 'Y') && (await read('4200683017', 'compact')).kind === 'missing', 'no spinner, Approve waits, nothing built with the missing letter (the compact widget says it too): ' + JSON.stringify([a.spin, a.aria, a.approve, (await builds()).slice(nbq)]));
    await shot('mw-1440-missing'); await caseShots('missing', ['4200683017']);
    st.docs.set('Charm_Master_Index/MONOGRAM_MID_Y', midY); await reloadMaster();          // (the letter is back: only the message of the missing one was wanted)
    await typeIn('4200683017', 1, 'P'); a = await ready('4200683017'); check(a.state === 'ready' && !a.msg, 'typing another letter takes the widget out of it');
    // a letter file that fails to load: Try again
    blobFail = /Monogram_Mid_Z\.ai/i; await typeIn('4200683017', 1, 'Z');
    a = await until('4200683017', r => r.state === 'error', 'the load failure', 40000);
    check(a.kind === 'load' && a.retry && /could not be loaded/.test(a.msg) && !a.spin && a.aria === 'false', 'a letter file that does not load: a plain message and Try again, no stuck spinner: "' + a.msg + '"');
    check(await page.evaluate(() => !document.querySelector('.cnp.on')), 'and the page\'s progress bar is gone');
    await caseShots('load', ['4200683017']);
    blobFail = null; await page.click(`${W('4200683017')} [data-mw-retry]`); a = await ready('4200683017', 30000);
    check(a.state === 'ready' && a.ins.join('') === 'AZV' && !a.msg, 'Try again loads it and the monogram is made: ' + a.ins.join(''));
    await typeIn('4200683017', 1, 'P'); await ready('4200683017');
    // typing what is not a letter
    const nbo = (await builds()).length;
    await page.click(`${W('4200683017')} [data-mw-letter="1"]`); await page.keyboard.type('7'); a = await read('4200683017');
    check(a.ins.join('') === 'APV' && /Letters A to Z only/.test(a.note), 'a digit is refused calmly: the box keeps its letter, "Letters A to Z only"');
    await page.keyboard.type('😀'); a = await read('4200683017'); check(a.ins.join('') === 'APV' && /Letters A to Z only/.test(a.note), 'an emoji too');
    await page.keyboard.press('Backspace'); a = await read('4200683017');
    check(a.state === 'letters' && a.ins[1] === '' && a.approve.disabled && a.invalid[1] === 'true', 'an emptied box: the widget waits for the three letters (no build): ' + JSON.stringify(a.ins) + ' ' + a.state);
    check((await builds()).length === nbo && /Type the three letters/.test(a.hint), 'nothing was built for what is not three letters: "' + a.hint + '"');
    await typeIn('4200683017', 1, 'p'); a = await ready('4200683017'); check(a.ins.join('') === 'APV', 'a small letter is made a capital');
    // odd words on listing-table monograms: shown calmly, nothing built
    await seedOrders(Object.keys(ODD)); await mountCases(Object.values(ODD).map(o => o.rid)); await sleep(500);
    for (const [k, o] of Object.entries(ODD)) {
      const r = await read(o.rid);
      check(r && r.state === 'letters' && r.kind === 'letters' && o.want.test(r.msg) && /Type the three letters/.test(r.msg) && r.ins.join('|') === o.boxes.join('|') && r.approve.disabled && !r.spin, `the buyer's "${o.words}": ${r && r.msg}`);
    }
    check((await builds()).slice(nbo).every(x => x === 'APV'), 'and no monogram was built for any of them: ' + ((await builds()).slice(nbo).join() || 'no build at all'));
    await shot('mw-1440-letters'); await caseShots('letters', ['4200900012', '4200900013']);
    // a person types the letters of "A.P": the widget builds
    await typeIn('4200900012', 2, 'v'); a = await ready('4200900012');
    check(a.state === 'ready' && a.ins.join('') === 'APV', 'for "A.P" a person adds the third letter and the monogram is made');
    // Paul's own Mid Q is drawn the way a converted font leaves a letter (its loop and a tail that cross each other: three rings that cross): the crossing is ink, not a hole (the stand-in alphabets have no such letter)
    if (REAL_OK) {
      await typeIn('4200683017', 1, 'Q'); a = await ready('4200683017', 30000);
      const q = await page.evaluate(async () => { const ui = Engrave.monogramUI(), c = ui.cards.get('4200683017_42006830171'), abc = ui.alphabets(), g = await ui.glyph('middle', 'Q', abc), L = await ui.glyph('left', 'A', abc), R = await ui.glyph('right', 'V', abc);
        const one = CharmNestMonogram.build({ left: L, middle: Object.assign({}, g, { fill: 'nonzero' }), right: R }), even = CharmNestMonogram.build({ left: L, middle: Object.assign({}, g, { fill: 'evenodd' }), right: R });
        const rawOne = CharmNestMonogram.build({ left: L, middle: Object.assign({}, g, { fill: 'nonzero', edited: false }), right: R }), rawEven = CharmNestMonogram.build({ left: L, middle: Object.assign({}, g, { fill: 'evenodd', edited: false }), right: R });
        return { fill: g.fill, rings: g.rings.length, same: CharmNestMonogram.svgPath(one) === CharmNestMonogram.svgPath(c.result), differs: CharmNestMonogram.svgPath(rawEven) !== CharmNestMonogram.svgPath(rawOne), outer: c.result.rings.filter(x => !x.hole).length }; });      // (the Mid Q is one of the edited letters now: the glyph itself, edited: false, is what crosses)
      check(a.state === 'ready' && q.rings >= 2 && q.fill === 'auto' && q.same && q.differs && q.outer === 1, 'Paul\'s Mid Q (rings that cross each other) is read as ink where they cross: the widget shows the piece the non-zero reading makes, not the even-odd one with holes at the crossings: ' + JSON.stringify(q));
      await typeIn('4200683017', 1, 'P'); await ready('4200683017');
    }

    // ═══ 5 · Approve: ONE press approves and puts the monogram on the next sheet of the buyer's metal (MONOSEND, Paul 18 Oct 2026: no "Send to Sheet →" link, no designs window); Change and replace ═══
    await page.evaluate(() => { window.__ev = []; const r = document.getElementById('mwRoot'); for (const t of ['monogram:change', 'monogram:approved']) r.addEventListener(t, e => window.__ev.push({ t, c: ((e.target.closest && e.target.closest('.mwCase')) || { dataset: {} }).dataset.case, rid: e.detail.rid, state: e.detail.state, letters: e.detail.letters && e.detail.letters.join ? e.detail.letters.join('') : e.detail.letters, name: e.detail.name, replaced: e.detail.replaced })); });
    // (the designs window is watched from here on: it must never open for a monogram whose metal the order gives)
    await page.evaluate(() => { window.__dlgOpened = 0; if (!window.__smWrapped) { window.__smWrapped = true; const so = HTMLDialogElement.prototype.showModal; HTMLDialogElement.prototype.showModal = function () { if (this.id === 'cuDlg') window.__dlgOpened++; return so.apply(this, arguments); }; } });
    a = await read('4200683017');
    check(a.state === 'ready' && !a.approve.hidden && !a.approve.disabled && a.approve.text === 'Approve' && a.ok.hidden && a.change.hidden && !a.sheetLink, 'a ready monogram offers one Approve (no Approved badge, no Change, and no "Send to Sheet →" link at all)');
    const green = await page.evaluate(sel => { const b = document.querySelector(sel + ' [data-mw-approve]'), probe = document.createElement('button'); probe.className = 'btn sage sm egApproveButton'; document.body.append(probe); const bg = getComputedStyle(b).backgroundColor, eg = getComputedStyle(probe).backgroundColor; probe.remove(); return { cls: b.className, bg, eg }; }, W('4200683017'));
    check(/\bbtn\b/.test(green.cls) && /\bsage\b/.test(green.cls) && green.bg === green.eg && green.bg === 'rgb(95, 122, 91)', 'the Approve button is the engraving cards\' green Approve (btn sage, the colour of .egApproveButton): ' + JSON.stringify(green));
    const apvKey = await page.evaluate(() => Orders.rows().find(r => r.order.receiptId === '4200683017').key);
    // (what the send recorded, and what the normal intake makes of it: CustomSheet.prepare is what the next run places on a sheet of that metal; this fake site opens no run, so the line waits "pulled" for it as every sent custom order does)
    const queued = k => page.evaluate(async k => { const row = B.orders.byKey.get(k), dec = CustomSheet.decisionOf(row), eng = row.engrave; let prep = null; try { const p = await CustomSheet.prepare(row, null); prep = p ? { n: p.charms.length, metal: p.sp.material, ids: p.pools.map(x => x.poolId) } : null; } catch (e) { prep = { error: String(e.message || e) }; } row.engrave = eng; return { dec: dec ? { id: dec.id, by: dec.by, n: (dec.lines[k] || []).length } : null, state: row.state, prep, seals: CharmNestMonogramUI.sealsOf(row).length }; }, k);
    await page.click(`${W('4200683017')} [data-mw-approve]`);
    a = await until('4200683017', r => r.state === 'sent', 'the approval and the send', 60000);
    await page.click('[data-role="laser"]', { timeout: 2500 }).catch(() => {});       // (the page's own "Laser or Design?" bar, asked with the first piece of work sent: it takes the keyboard while it waits)
    const e1 = await entries();
    check(e1.length === 1 && e1[0].files.length === 1 && e1[0].files[0].name === 'Monogram APV.ai', 'Approve puts Monogram APV.ai on the order\'s custom designs: ' + JSON.stringify(e1.map(e => e.files.map(f => f.name))));
    const f = e1[0].files[0], size = await page.evaluate(() => { const r = Engrave.monogramUI().cards.get('4200683017_42006830171').result; return { w: r.wMm, h: r.hMm }; });
    check(f.state === 'ready' && f.pieces === 1 && f.qty === 1 && Math.abs(f.wMm - size.w) < 0.3 && Math.abs(f.hMm - size.h) < 0.3, `read back as ONE piece of ${f.wMm.toFixed(1)} × ${f.hMm.toFixed(1)} mm, one copy (the preview says ${size.w.toFixed(1)} × ${size.h.toFixed(1)})`);
    const q1 = await queued(apvKey);
    check(!!q1.dec && q1.dec.n === 1 && ['pulled', 'pooled', 'written'].includes(q1.state) && !!q1.prep && q1.prep.n === 1 && q1.prep.metal === 'gold' && f.metal === 'gold', 'the same press sent it to the nesting queue: ONE piece, on the buyer\'s metal read from the order (GF): ' + JSON.stringify(Object.assign({ metal: f.metal }, q1)));
    check(q1.seals === 1 && !!q1.dec && !!q1.dec.by && q1.dec.by === await page.evaluate(() => B.employee), 'one seal was pressed, and the send is recorded under the person signed in: ' + JSON.stringify({ seals: q1.seals, by: q1.dec && q1.dec.by }));
    check(await page.evaluate(() => window.__dlgOpened === 0 && !document.querySelector('#cuDlg[open]')), 'the designs window never opened (no modal for a monogram)');
    check(a.approve.hidden && !a.ok.hidden && a.ok.text === 'On the sheet · Monogram APV' && a.change.hidden && a.locked && !a.sheetLink, 'the widget says On the sheet · Monogram APV, final: boxes locked, no Approve, no Change, no link: ' + JSON.stringify([a.state, a.ok, a.change, a.locked]));
    ac = await read('4200683017', 'cphone'); check(ac.state === 'sent' && ac.change.hidden && ac.approve.hidden && !ac.sheetLink, 'the compact widget agrees (On the sheet)');
    check(await page.evaluate(() => { const ev = window.__ev.filter(e => e.t === 'monogram:approved' && e.c === 'detail'); return ev.length === 1 && ev[0].rid === '4200683017' && ev[0].letters === 'APV' && ev[0].name === 'Monogram APV.ai' && ev[0].replaced === false; }), 'the page was told once per mount: monogram:approved { rid, letters APV, name, replaced false } (on the widget host)');
    await shot('mw-1440-approved'); await caseShots('approved', ['4200683017']); HH.approved = [await heightOf('4200683017'), await heightOf('4200683017', 'cphone')];
    // a second press is nothing: no second design, piece, seal or send
    const before2 = JSON.stringify([await entries(), q1]);
    await page.evaluate(() => Engrave.monogramUI().approve(Engrave.monogramUI().cards.get('4200683017_42006830171')));
    await page.evaluate(sel => { const b = document.querySelector(sel + ' [data-mw-approve]'); if (b) b.click(); }, W('4200683017')); await sleep(600);
    check(JSON.stringify([await entries(), await queued(apvKey)]) === before2 && await page.evaluate(() => window.__dlgOpened === 0), 'a second press (the service\'s approve, a click on the hidden button) changes nothing: the same design, piece, decision and one seal');
    // the buyer's metal cannot be read (an option nobody knows): nothing is guessed; the seal is pressed, the monogram is approved but NOT sent, the card says why, and the designs window opens for a person to pick it
    await seedOrders(['nom']); const NOM = FX.nom.rid;
    check(await page.evaluate(rid => { const r = Orders.rows().find(x => x.order.receiptId === rid); return !!r && !!r.spec.special && !!r.spec.special.monogram && !r.spec.material; }, NOM), 'order ' + NOM + ' is a monogram whose metal cannot be read from the order');
    await mountCases([NOM]); a = await ready(NOM);
    await page.click(`${W(NOM)} [data-mw-approve]`);
    await page.waitForSelector('#cuDlg[open] .cuFile', { timeout: 30000 }).catch(() => {});
    a = await until(NOM, r => r.state === 'approved' && r.queue === 'metal', 'the metal question', 20000);
    const nm = await page.evaluate(rid => { const row = Orders.rows().find(x => x.order.receiptId === rid), e = Object.values(CustomSheet.entries()).find(x => x.rid === rid), d = document.getElementById('cuDlg'); return { open: !!(d && d.open), files: e ? e.files.map(F => ({ name: F.name, metal: F.metal })) : null, sent: !!CustomSheet.decisionOf(row), ids: (row.poolIds || []).length, seals: CharmNestMonogramUI.sealsOf(row).length, sub: d ? d.querySelector('#cuDlgSub').textContent : '' }; }, NOM);
    check(nm.files && nm.files.length === 1 && nm.files[0].name === 'Monogram NOM.ai' && !nm.files[0].metal && !nm.sent && nm.ids === 0 && nm.seals === 1, 'with no metal on the order the monogram is approved (its seal) but not sent, and its design has NO metal (never another line\'s, never a guess): ' + JSON.stringify(nm));
    check(nm.open && /no metal on the order/.test(nm.sub) && /buyer’s metal could not be read/.test(a.note) && !a.approve.hidden && !a.ok.hidden && a.ok.text === 'Approved · Monogram NOM', 'and the designs window opens (the fallback) while the card says so plainly, keeping Approve to finish it: ' + JSON.stringify({ note: a.note, sub: nm.sub }));
    HH.metal = [await heightOf(NOM), await heightOf(NOM, 'cphone')];
    await page.click('#cuDlg .cuFile .cuM[data-m="silver"]'); await page.click('#cuDlg [data-x]'); await sleep(300);
    a = await read(NOM); check(a.state === 'approved' && a.queue === 'idle' && !a.approve.hidden && /not on a sheet yet/.test(a.note), 'a person picked silver and closed the window without sending: the card is approved, not on a sheet yet, and Approve finishes it: ' + JSON.stringify([a.queue, a.note]));
    // Change and replace (on a monogram not on a sheet: one on a sheet is final)
    check(a.locked, 'an approved monogram cannot be typed over by accident (boxes locked)');
    await page.click(`${W(NOM)} [data-mw-change]`); a = await read(NOM);
    check(!a.locked && a.state === 'approved' && /Change a letter to replace/.test(a.note) && a.change.hidden, 'Change unlocks the boxes and says how to replace: "' + a.note + '"');
    await caseShots('change', [NOM], ['detail', 'compact']); HH.change = [await heightOf(NOM), await heightOf(NOM, 'cphone')];
    { const all = Object.values(HH).flat(); const sig = Object.entries(HH).map(([k, v]) => k + ' ' + v.map(x => Math.round(x)).join('/')).join(' · ');
      // (MONOBIG, 17 Oct 2026: the compact picture is 139 px beside the letters and 159 px over them, was 84 / 96: the reserved height of the stacked widget is ~277 px now, was <= 176; what is kept is that it is ONE height in every state)
      check(Object.keys(HH).length >= 4 && Math.max(...all) <= COMPACT_MAX_H && Math.max(...all) - Math.min(...all) <= 1, `the compact widget keeps ONE height (<= ${COMPACT_MAX_H} px) in every state, so the Order window never jumps: ` + sig); }
    await typeIn(NOM, 1, 'S'); a = await ready(NOM);
    check(a.state === 'ready' && !a.approve.hidden && a.approve.text === 'Replace approved', 'after a letter change the button says Replace approved: ' + JSON.stringify([a.state, a.approve]));
    await page.click(`${W(NOM)} [data-mw-approve]`); a = await until(NOM, r => r.state === 'sent', 'the replacement and its send', 60000);
    const e3 = (await entries()).filter(e => e.files.some(x => /^Monogram N/.test(x.name)));
    check(e3.length === 1 && e3[0].files.length === 1 && e3[0].files[0].name === 'Monogram NSM.ai' && e3[0].sent, 'the approved design is REPLACED, never two, and that press sent it: ' + JSON.stringify(e3.map(e => [e.sent, e.files.map(f => f.name)])));
    const qn = await queued(await page.evaluate(rid => Orders.rows().find(x => x.order.receiptId === rid).key, NOM));
    check(e3[0].files[0].metal === 'silver' && !!qn.dec && qn.dec.n === 1 && !!qn.prep && qn.prep.n === 1 && qn.prep.metal === 'silver', 'and the metal a person set carries over to it: one silver piece: ' + JSON.stringify([e3[0].files[0].metal, qn]));
    check(await page.evaluate(rid => { const ev = window.__ev.filter(e => e.t === 'monogram:approved' && e.c === 'detail' && e.rid === rid); return ev.length === 2 && ev[1].letters === 'NSM' && ev[1].replaced === true; }, NOM), 'the second approval is told as a replacement (monogram:approved replaced true)');
    await page.evaluate(() => { window.__dlgOpened = 0; });
    // approving another order's monogram does not touch this one
    await page.click(`${W('4202783520')} [data-mw-approve]`); await until('4202783520', r => r.state === 'sent', 'the second approval', 60000);
    const e4 = await entries(); check(e4.length === 3 && e4.map(e => e.files.map(f => f.name).join()).sort().join('|') === 'Monogram APV.ai|Monogram NSM.ai|Monogram OSR.ai' && e4.every(e => e.sent), 'a second order\'s monogram is its own design, sent on its own: ' + JSON.stringify(e4.map(e => e.files.map(f => f.name + ':' + f.metal))));
    // MONOROUTE (Paul, 17 Oct 2026: "Monogram orders should be treated exactly like back engraving and should be routed to the Engraving tab and NOT the Review tab unless something else is wrong"):
    // a monogram line that asks nothing is NOT listed in the Review tab, approved or not (it used to be a Custom Orders card with its design on it, or with Build monogram); its custom card still exists and
    // holds the design (the monogram's Approve attaches it there: Review.customItemFor), and the Engraving tab's monogram service lists the line
    await hideCases(true);
    await page.evaluate(() => { CN.setMode('review'); Review.render(); });
    await sleep(600);
    const rvIn = rid => page.evaluate(rid => document.querySelectorAll(`#rvList [data-rid="${rid}"]`).length, rid);
    check(await rvIn('4200683017') === 0 && await rvIn('4202783520') === 0 && await rvIn('4200900001') === 0, 'the Review tab lists none of the monogram lines: not the two approved and sent ones (APV, OSR) and not the one still waiting for its approval (J.Q.X)');
    check(await page.evaluate(() => !document.querySelector('#rvList [data-cu-mono]')), 'and no Review card offers "Build monogram" for a line that asks nothing');
    const cardOfApv = await page.evaluate(() => { const row = Orders.rows().find(r => r.order.receiptId === '4200683017'), it = Review.customItemFor(row.key, row), c = it && CustomSheet.cardOf(it); return { has: !!it, files: c ? c.files.map(f => f.name) : null, rows: it ? (it.rows || []).map(r => r.key) : null, key: row.key }; });
    check(cardOfApv.has && cardOfApv.files.join() === 'Monogram APV.ai' && cardOfApv.rows.join() === cardOfApv.key, 'its custom card still exists (Review.customItemFor finds it) and holds the approved design, though Review does not list it: ' + JSON.stringify(cardOfApv.files));
    check(await page.evaluate(() => { const ui = Engrave.monogramUI(), rows = ui.rows().map(r => r.order.receiptId); return ['4200683017', '4202783520', '4200900001'].every(r => rows.includes(r)); }), 'the Engraving tab\'s monogram service lists the three lines (the place a monogram is looked after now)');
    // ... unless something else is wrong: a question the line raises, or a hold, keeps its card in Review as before (the intake itself raises none for a monogram, so the question is staged the way the reader's failure line is)
    await seedOrders(['trb', 'hld']);
    const trb = await page.evaluate(() => { const row = Orders.rows().find(r => r.order.receiptId === '4200900031'); row.problems = [{ kind: 'needsMapping', listingId: String(row.line.listingId), optionName: 'Pendant finish', optionValue: 'Mystery coating', title: row.line.title }]; Review.syncOrderItems(); Review.render(); return row.key; });
    await page.waitForSelector('#rvList [data-rid="4200900031"]', { timeout: 15000 }).catch(() => {});
    check(await rvIn('4200900031') >= 1, 'a monogram line WITH a real problem (an option nobody mapped) is still listed in Review');
    check(await page.evaluate(() => { const b = document.querySelector('#rvList [data-rid="4200900031"] [data-cu-mono]'); return !!b && b.textContent.trim() === 'Build monogram' && !/Monograms/.test(b.title); }), 'its card offers Build monogram (the line has no design yet), which opens the Engraving tab\'s monogram');
    await page.evaluate(k => { const row = B.orders.byKey.get(k); row.problems = []; Review.syncOrderItems(); Review.render(); }, trb);
    await sleep(300); check(await rvIn('4200900031') === 0, 'the problem answered, the line leaves Review again (the Engraving tab\'s)');
    // a person who sends the order to Review (Send to Review, charm-nest-review-mark.js) says something is wrong with it: that card is listed whatever the line is, until Mark OK
    const mk = await page.evaluate(() => CharmNestReviewMark.mark('4200900032', { by: 'Test Operator' }));
    await page.evaluate(() => { Review.syncOrderItems(); Review.render(); }); await sleep(700);
    check(!!mk.ok && await rvIn('4200900032') >= 1, 'a monogram order a person sent to Review is listed there, marked, whatever the line says: ' + JSON.stringify(mk));
    await page.evaluate(() => CharmNestReviewMark.resolve('4200900032', { by: 'Test Operator', how: 'ok' })); await page.evaluate(() => { Review.syncOrderItems(); Review.render(); }); await sleep(700);
    check(await rvIn('4200900032') === 0, 'Mark OK takes it out of Review again (the Engraving tab\'s, as before)');
    // a hold a person pressed on the line is a hold (its pill says held, the piece placement says "On hold"); with none the line waits in the Engraving tab (the pill says where, the placement does not call it a hold)
    { const sts = await page.evaluate(() => { const row = Orders.rows().find(r => r.order.receiptId === '4200900032'), snap = () => ({ pill: Orders.statePill(row)[1], place: (PiecePlacement.ofRow(row) || {}).state }); row.state = 'held'; const free = snap(); row.hold = 'held by a person: wait for the buyer\'s reply'; const held = snap(); row.hold = null; return { free, held }; });
      check(sts.free.pill === 'monogram · Engraving' && sts.free.place !== 'hold' && /held/i.test(sts.held.pill) && sts.held.place === 'hold', 'a monogram line with no hold waits in the Engraving tab (pill "monogram · Engraving", placement not "On hold"); with a hold a person pressed it is held and On hold: ' + JSON.stringify(sts)); }
    await hideCases(false);

    // ═══ 6 · the page is told, a host can hold Approve, a widget can be moved and removed ═══
    check(await page.evaluate(() => { const ev = window.__ev.filter(e => e.t === 'monogram:change' && e.rid === '4200900041' && e.c === 'detail'); return ev.length >= 6 && ev.some(e => e.state === 'building') && ev.some(e => e.state === 'ready') && ev.some(e => e.state === 'approved') && ev.some(e => e.state === 'sent'); }), 'monogram:change was told with each real change (building, ready, approved, sent) for the line (N.O.M)');
    await seedOrders(['ev']); const EV = FX.ev.rid;
    const cb = await page.evaluate(async rid => {
      window.__cb = { changes: [], approved: [] }; const row = Orders.rows().find(r => r.order.receiptId === rid), host = document.createElement('div'); document.getElementById('mwRoot').append(host);
      window.__gateOn = 'Approve the first piece before this one';
      const w = CharmNestMonogramUI.mountLine(host, { row, mode: 'compact', gate: () => window.__gateOn, onChange: s => window.__cb.changes.push(s.state), onApproved: i => window.__cb.approved.push(i) });
      window.__evw = w; window.__evhost = host; return { mode: w.mode, state: w.state().state };
    }, EV);
    await page.waitForFunction(() => window.__evw.state().state === 'ready', null, { timeout: 30000 });
    const g1 = await page.evaluate(() => { const s = window.__evw.state(), b = window.__evhost.querySelector('[data-mw-approve]'); return { can: s.canApprove, why: s.approveWhy, dis: b.disabled, title: b.title, changes: window.__cb.changes.join(',') }; });
    check(!g1.can && g1.dis && g1.title === 'Approve the first piece before this one' && g1.why === g1.title && /ready/.test(g1.changes), 'a host that says "not yet" (gate) keeps Approve disabled with its reason as the title; onChange was called: ' + JSON.stringify(g1));
    await page.evaluate(() => { window.__gateOn = ''; window.__evw.refresh(); });
    check(await page.evaluate(() => { const b = window.__evhost.querySelector('[data-mw-approve]'); return !b.disabled && window.__evw.state().canApprove; }), 'and a refresh with the gate open enables it');
    await page.evaluate(() => window.__evw.approve());
    await page.waitForFunction(() => window.__cb.approved.length === 1, null, { timeout: 20000 });
    const ap1 = await page.evaluate(() => window.__cb.approved[0]);
    check(ap1.rid === EV && ap1.letters === 'EMT' && ap1.name === 'Monogram EMT.ai' && ap1.replaced === false && !!ap1.ck, 'onApproved gets { rid, lineKey, letters, name, replaced, ck }: ' + JSON.stringify(ap1));
    // update() re-points / re-shapes the same widget; destroy() leaves nothing behind
    const upd = await page.evaluate(() => { const w = window.__evw, el0 = w.el; w.update({ mode: 'detail' }); const a1 = { mode: w.mode, same: w.el === el0, attr: w.el.dataset.mode }; w.update({ mode: 'compact' }); return a1; });
    check(upd.mode === 'detail' && upd.attr === 'detail' && !upd.same, 'update({ mode }) reshapes the widget (detail <-> compact)');
    const gone = await page.evaluate(() => { const n0 = CharmNestMonogramUI.widgets().length, w = window.__evw, h = window.__evhost; w.destroy(); w.destroy(); return { n0, n1: CharmNestMonogramUI.widgets().length, empty: h.children.length === 0, views: [...Engrave.monogramUI().cards.values()].reduce((s, c) => s + c.views.size, 0) }; });
    check(gone.n1 === gone.n0 - 1 && gone.empty, 'destroy() removes the widget and its markup (twice is harmless): ' + JSON.stringify(gone));
    // status() for a list: the words of the card's dot, read without any mount
    const stt = await page.evaluate(() => { const S = r => CharmNestMonogramUI.status(Orders.rows().find(x => x.order.receiptId === r)); return { apv: S('4200683017'), jqx: S('4200900001'), odd: S('4200900013'), plain: CharmNestMonogramUI.status(Orders.rows().find(x => x.order.receiptId === '4200900004')) }; });
    check(stt.apv.state === 'sent' && stt.apv.label === 'On the sheets' && stt.apv.letters === 'APV' && stt.apv.approved && stt.apv.sent && stt.apv.queue === '' && stt.jqx.state === 'ready' && stt.jqx.label === 'Ready to approve' && stt.odd.state === 'letters' && stt.odd.label === 'Letters to confirm' && stt.plain === null, 'status(row) gives the card\'s dot and word without a mount: ' + JSON.stringify([stt.apv.label, stt.jqx.label, stt.odd.label, stt.plain]));

    // ═══ 7 · the thumbnail for the piece card ═══
    await seedOrders(['thm']); const THM = FX.thm.rid;
    const barsBefore = await page.evaluate(() => (window.__bars || []).length);
    const th = await page.evaluate(async rid => {
      const row = Orders.rows().find(r => r.order.receiptId === rid), div = document.createElement('div'); div.id = 'thumbTest'; div.style.cssText = 'position:fixed;left:10px;top:10px;z-index:2147483600;display:flex;gap:10px;padding:10px;background:#fff';
      div.innerHTML = `<button class="engThumb" style="width:104px;height:104px" id="thmA">${CharmNestMonogramUI.thumbHtml(row, { px: 104 })}</button><span id="thmB" style="display:block;width:64px"></span><div id="thmC" style="width:40px"></div>`;
      document.body.append(div); CharmNestMonogramUI.fillThumbs(div);
      const el = CharmNestMonogramUI.thumb(row, { px: 64, into: div.querySelector('#thmB') }), el2 = CharmNestMonogramUI.thumb(row, { px: 40, into: div.querySelector('#thmC'), onReady: () => { window.__thmReady = (window.__thmReady || 0) + 1; } });
      const first = div.querySelector('[data-mw-thumb]');
      return { html: first.outerHTML.slice(0, 160), into: el.parentNode.id, key: first.dataset.mwThumb, txt: first.textContent, n: div.querySelectorAll('[data-mw-thumb]').length };
    }, THM);
    check(/class="mwThumb"/.test(th.html) && /--mw-px:104px/.test(th.html) && th.into === 'thmB' && th.n === 3 && /T H M|T–M|[A-Z–] [A-Z–] [A-Z–]/.test(th.txt.replace(/\s+/g, ' ')), 'thumbHtml / fillThumbs / thumb({ into }) give the small element; before the picture exists it shows the three letters: "' + th.txt + '"');
    await page.waitForFunction(() => document.querySelectorAll('#thumbTest svg path').length === 3 && window.__thmReady === 1, null, { timeout: 40000 });
    const th2 = await page.evaluate(rid => { const row = Orders.rows().find(r => r.order.receiptId === rid), c = Engrave.monogramUI().cards.get(row.key), ds = [...document.querySelectorAll('#thumbTest svg path')].map(p => p.getAttribute('d')); return { same: ds.every(d => d === CharmNestMonogram.svgPath(c.result)), fill: document.querySelector('#thumbTest svg path').getAttribute('fill'), pts: ds[0].length, state: document.querySelector('#thumbTest [data-mw-thumb]').dataset.state, inputs: document.querySelectorAll('#thumbTest input, #thumbTest button button').length, bars: (window.__bars || []).filter(b => /Building monogram/.test(b)).length }; }, THM);
    check(th2.same && th2.fill === TONE.gold && th2.pts > 500 && th2.inputs === 0, 'the thumbnail is the cut piece itself (the very path the engine made), in the line\'s metal, and nothing else (no inputs): ' + JSON.stringify(th2));
    check(th2.bars === barsBefore - barsBefore + (await page.evaluate(() => (window.__bars || []).filter(b => /Building monogram/.test(b)).length)) && !(await page.evaluate(() => (window.__bars || []).some(b => /Building monogram THM/.test(b)))), 'a thumbnail builds quietly: no progress bar was started for it (and no AI call)');
    // an edit made in a widget repaints the thumbnail; opening the widget on the line shows the same state
    await mountCases([THM], ['detail']); await ready(THM);
    const d0 = await page.evaluate(() => document.querySelector('#thumbTest svg path').getAttribute('d'));
    await typeIn(THM, 2, 'S'); await ready(THM);
    await page.waitForFunction(d0 => document.querySelector('#thumbTest svg path').getAttribute('d') !== d0, d0, { timeout: 20000 });
    check(await page.evaluate(() => { const c = Engrave.monogramUI().cards.get(Orders.rows().find(r => r.order.receiptId === '4200900022').key); return [...document.querySelectorAll('#thumbTest svg path')].every(p => p.getAttribute('d') === CharmNestMonogram.svgPath(c.result)); }), 'an edit in the widget repaints every thumbnail of the line at once');
    await shot('mw-1440-thumb'); await page.locator('#thumbTest').screenshot({ path: path.join(SHOTS, 'thumb-ready.png') }).catch(() => {});
    await page.evaluate(() => document.getElementById('thumbTest').remove());
    // a thumbnail for an unreadable line shows the letters it has, not a picture
    const un = await page.evaluate(() => { const row = Orders.rows().find(r => r.order.receiptId === '4200900013'), el = CharmNestMonogramUI.thumb(row, { px: 80 }); return { tx: el.textContent, state: el.dataset.state, svg: el.querySelectorAll('svg').length }; });
    check(un.state === 'letters' && un.svg === 0, 'an unreadable line has no picture to show: its thumbnail is plain (' + un.tx + ')');

    // ═══ 8 · the size the buyer chose ═══
    await seedOrders(['siz']); const SIZ = FX.siz.rid;
    const natural = await page.evaluate(async rid => { const ui = Engrave.monogramUI(), abc = ui.alphabets(), g = await Promise.all([ui.glyph('left', 'A', abc), ui.glyph('middle', 'S', abc), ui.glyph('right', 'D', abc)]); const r = CharmNestMonogram.build({ left: g[0], middle: g[1], right: g[2] }); return { w: r.wMm, h: r.hMm }; }, SIZ);
    const wantMm = Math.max(10, Math.floor(natural.w) - 3);
    await page.evaluate(({ rid, mm }) => { const row = Orders.rows().find(r => r.order.receiptId === rid); row.spec.size = mm + 'MM'; }, { rid: SIZ, mm: wantMm });
    await mountCases([SIZ], ['detail', 'compact']); a = await ready(SIZ);
    const sz1 = await page.evaluate(() => { const c = Engrave.monogramUI().cards.get(Orders.rows().find(r => r.order.receiptId === '4200900024').key); return { w: c.result.wMm, h: c.result.hMm, fit: c.result.report.fit }; });
    check(sz1.w <= wantMm + 0.01 && sz1.w > wantMm - 0.2 && sz1.fit && sz1.fit.applied && a.size.includes(`scaled to the buyer’s ${wantMm} mm`) && a.size.startsWith(sz1.w.toFixed(1) + ' × ' + sz1.h.toFixed(1)), `the size option (${wantMm}MM, the piece is ${natural.w.toFixed(1)} mm wide as drawn) is the widest the piece may be: ${sz1.w.toFixed(2)} × ${sz1.h.toFixed(2)} mm, the line says "${a.size}"`);
    await caseShots('sized', [SIZ], ['detail', 'compact']);
    const nbf = (await builds()).length;
    await page.evaluate(({ rid, mm }) => { const row = Orders.rows().find(r => r.order.receiptId === rid); row.spec.size = (mm + 10) + 'MM'; CharmNestMonogramUI.widgets().forEach(w => w.refresh()); }, { rid: SIZ, mm: wantMm });
    await typeIn(SIZ, 0, 'B'); await typeIn(SIZ, 0, 'A'); await ready(SIZ);
    a = await read(SIZ);
    check(a.size.includes(`within the buyer’s ${wantMm + 10} mm`) && !a.size.includes('scaled'), 'a larger size option leaves the piece as drawn ("within the buyer\'s ' + (wantMm + 10) + ' mm"): ' + a.size);
    await page.click(`${W(SIZ)} [data-mw-approve]`); await until(SIZ, r => r.state === 'sent', 'the sized approval and its send', 60000);
    await page.waitForFunction(() => Object.values(CustomSheet.entries()).every(e => e.files.every(f => f.state === 'ready')), null, { timeout: 30000 });
    check((await entries()).some(e => e.files.some(f => f.name === 'Monogram ASD.ai' && Math.abs(f.wMm - natural.w) < 0.4)), 'and the design file written is the size shown (' + natural.w.toFixed(1) + ' mm)');

    // ═══ 9 · the AI assembly line ═══
    await seedOrders(['ai1', 'ai2']);
    await page.evaluate(() => {
      const IDENT = { scale: 1, sx: 1, dx: 0, dy: 0 }; window.__violate = ['PQR', 'WNK']; window.__calls = [];
      CN.setAgent(async (mode, payload) => { window.__calls.push(payload.letters.join('')); if (payload.letters.join('') !== 'PQR') throw new Error('network down'); return { adjust: { L: { scale: 1, sx: 1, dx: 2, dy: 0 }, M: IDENT, R: IDENT }, why: 'Moved the P toward the Q so a lower contact joins them.', model: 'fake', usage: {} }; });
    });
    await mountCases([FX.ai1.rid, FX.ai2.rid], ['detail', 'compact']);
    a = await until(FX.ai1.rid, r => /Assembled with AI/.test(r.asm) && !r.spin, 'PQR assembled', 40000);
    check(a.state === 'ready' && a.asm === 'Assembled with AI: Moved the P toward the Q so a lower contact joins them.' && a.asmStatus === 'assembled' && !a.again && !a.approve.disabled, 'a word the AI fixed says ONE calm line: "' + a.asm + '"');
    // (MONOAPPROVE, Paul 20 Oct 2026: "Remove this red text and Improve again link from the UI completely" / "if the user presses it that means they approve!!")
    await page.waitForFunction(() => window.__calls.includes('WNK'), null, { timeout: 60000 }).catch(() => {});
    a = await until(FX.ai2.rid, r => r.state === 'check' && !r.spin, 'WNK flagged', 60000);
    const noRed = r => !r.asm && !r.note && !r.again && !/Needs a person|Improve again|Approve anyway/.test(JSON.stringify(r));
    check(a.state === 'check' && noRed(a) && !a.approve.hidden && !a.approve.disabled && a.approve.text === 'Approve', 'a word whose AI call failed shows no red line and no Improve again, only the plain Approve: ' + JSON.stringify({ asm: a.asm, note: a.note, approve: a.approve.text }));
    { const k = await read(FX.ai2.rid, 'compact'); check(k.state === 'check' && noRed(k) && k.approve.text === 'Approve', 'the compact widget the same'); }
    check(await page.evaluate(() => !document.querySelector('[data-mw-again]') && ![...document.querySelectorAll('.mw')].some(h => /Needs a person|Improve again|Approve anyway/.test(h.textContent))), 'no widget on the page has an Improve again link or says "Needs a person" / "Approve anyway"');
    await caseShots('ai', [FX.ai1.rid, FX.ai2.rid], ['detail', 'compact']);
    await page.click(`${W(FX.ai2.rid)} [data-mw-approve]`); a = await until(FX.ai2.rid, r => r.state === 'approved' || r.state === 'sent', 'WNK approved by one press', 30000);
    check(a.state === 'approved' || a.state === 'sent', 'ONE Approve press approves a flagged word (no "Approve anyway", no second press): ' + a.state);
    await page.evaluate(() => { window.__violate = []; });

    // ═══ the alphabets: none, partial, chosen by hand (the widget asks only when it has to) ═══
    const keep = new Map(); for (const [k, v] of st.docs) if (/^Charm_Master_Index\/MONOGRAM_/.test(k)) keep.set(k, v);
    const dropFam = word => { for (const k of [...st.docs.keys()]) if (new RegExp('^Charm_Master_Index/MONOGRAM_' + word + '_').test(k)) st.docs.delete(k); };
    const refreshAll = () => page.evaluate(() => CharmNestMonogramUI.widgets().forEach(w => w.refresh()));
    const SET = '4200900012';
    dropFam('RIGHT'); await reloadMaster(); await refreshAll();
    a = await until(SET, r => r.state === 'error' && r.kind === 'setup', 'the setup state');
    const ab = await page.evaluate(sel => { const m = document.querySelector(sel + ' [data-mw-msg]'); return { text: m.textContent.replace(/\s+/g, ' '), selects: m.querySelectorAll('select').length, opts: [...m.querySelectorAll('select')[2].options].map(o => o.textContent) }; }, W(SET));
    check(/Choose the Right alphabet\./.test(ab.text) && !/name them like/.test(ab.text) && ab.selects === 3 && ab.opts.length === 3 && a.approve.disabled && !a.spin, 'with no Right alphabet in the library the widget says so, with the three pickers: ' + ab.text.slice(0, 150));
    await shot('mw-1440-setup-partial'); await caseShots('setup', [SET], ['detail', 'compact']);
    dropFam('LEFT'); dropFam('MID'); await reloadMaster(); await refreshAll();
    a = await until(SET, r => r.state === 'error' && /not in the charm library yet/.test(r.msg), 'the setup state with no alphabet');
    check(/alphabets are not in the charm library yet/.test(a.msg) && /Monogram_Left_A/.test(a.msg) && /Choose the alphabets/.test(a.hint) && a.approve.disabled, 'with no alphabet at all: the quiet setup state, telling what to add: ' + a.msg.slice(0, 160));
    await caseShots('setup-none', [SET], ['detail', 'compact']);
    // three alphabets with no Left / Mid / Right in their names: chosen by hand, kept
    const FANCY = { ONE: 'LEFT', TWO: 'MID', THREE: 'RIGHT' };
    for (const [nm, from] of Object.entries(FANCY)) for (const L of A.LETTERS) { const e = keep.get(`Charm_Master_Index/MONOGRAM_${from}_${L}`); st.put('Charm_Master_Index', `FANCY_${nm}_${L}`, Object.assign({}, e, { sku: `FANCY_${nm}_${L}` })); }
    await reloadMaster(); await refreshAll(); await sleep(300);
    const picks = await page.evaluate(sel => { const m = document.querySelector(sel + ' [data-mw-msg]'); return [...m.querySelectorAll('select')].map(s => [...s.options].map(o => o.value + '=' + o.textContent.trim())); }, W(SET));
    check(picks[0].length === 4 && picks[0].some(o => /FANCY_ONE_\{L\}=Fancy One · 26 letters/.test(o)), 'the three lettered families are offered in each picker: ' + JSON.stringify(picks[0]));
    const nf = (await fetched()).length;
    await page.selectOption(`${W(SET)} select[data-mw-abc="left"]`, 'FANCY_ONE_{L}');
    await page.waitForFunction(sel => !!document.querySelector(sel + ' select[data-mw-abc="middle"]'), W(SET), { timeout: 10000 }).catch(() => {});
    await page.selectOption(`${W(SET)} select[data-mw-abc="middle"]`, 'FANCY_TWO_{L}').catch(() => {});
    await page.waitForFunction(sel => !!document.querySelector(sel + ' select[data-mw-abc="right"]'), W(SET), { timeout: 10000 }).catch(() => {});
    await page.selectOption(`${W(SET)} select[data-mw-abc="right"]`, 'FANCY_THREE_{L}').catch(() => {});
    await page.waitForFunction(n => window.__fetched.slice(n).includes('FANCY_THREE_V'), nf, { timeout: 30000 }).catch(() => {}); a = await ready(SET, 30000);
    const f2 = (await fetched()).slice(nf);
    check(!a.spin && ['FANCY_ONE_A', 'FANCY_TWO_P', 'FANCY_THREE_V'].every(s => f2.includes(s)), 'the chosen alphabets are used for the letters: ' + [...new Set(f2)].join(' '));
    check(await page.evaluate(() => { const m = S.settings.mono || {}; return m.left === 'FANCY_ONE_{L}' && m.middle === 'FANCY_TWO_{L}' && m.right === 'FANCY_THREE_{L}'; }), 'and the choice is kept in the settings (beside the AI section\'s day counters, which the same settings object holds)');
    // everything back: Monogram Left / Mid / Right again, chosen "Automatic"
    for (const [k, v] of keep) st.docs.set(k, v);
    for (const slot of ['left', 'middle', 'right']) await page.evaluate(slot => Engrave.monogramUI().setAlphabet(slot, ''), slot);
    for (const [nm] of Object.entries(FANCY)) for (const L of A.LETTERS) st.docs.delete(`Charm_Master_Index/FANCY_${nm}_${L}`);
    await reloadMaster(); await refreshAll();
    a = await ready(SET, 30000); check(a.state === 'ready' || a.state === 'approved', 'the Monogram alphabets back, Automatic: the monogram is made again');

    // ═══ 10 · keys, phone, motion, a second tab, a reload ═══
    await page.evaluate(() => { document.activeElement && document.activeElement.blur && document.activeElement.blur(); });
    // Tab order within a widget that waits for its approval (J.Q.X)
    await page.focus(`${W('4200900001')} [data-mw-letter="0"]`);
    const order = []; for (let i = 0; i < 8; i++) { order.push(await page.evaluate(() => { const n = document.activeElement; return n.dataset.mwLetter != null ? 'L' + n.dataset.mwLetter : n.dataset.mwSwap != null ? 'swap' + n.dataset.mwSwap : n.hasAttribute('data-mw-rotate') ? 'rotate' : n.hasAttribute('data-mw-approve') ? 'approve' : n.tagName; })); await page.keyboard.press('Tab'); }
    check(order.slice(0, 7).join(' ') === 'L0 swap0 L1 swap1 L2 rotate approve', 'Tab goes Left, swap, Middle, swap, Right, rotate, Approve: ' + order.join(' '));
    // Enter approves (Klm): pressed while the monogram is still being made, it approves it once it is made; and (MONOSEND, Paul 18 Oct 2026) that one press also puts it on the next sheet of the buyer's metal, with no designs window
    await hideCases(true); await page.evaluate(() => { CN.setMode('review'); Review.render(); }); await sleep(400);
    check(await page.evaluate(() => !document.querySelector('#rvList [data-rid="4200900001"]')), 'before it is approved the monogram J.Q.X has no card in the Review tab');
    await hideCases(false); await page.evaluate(() => { window.__dlgOpened = 0; });
    await typeIn('4200900001', 2, 'N'); await page.keyboard.press('Enter'); a = await until('4200900001', r => r.state === 'sent', 'approval by Enter and its send', 60000);
    const ek = (await entries()).find(e => e.files.some(f => /^Monogram JQ/.test(f.name)));
    check(ek && ek.files.length === 1 && ek.files[0].name === 'Monogram JQN.ai' && ek.sent && ek.files[0].metal === 'rose', 'Enter in a letter box (pressed while the monogram is still being made) approves it when it is ready, and that press sends it on the buyer\'s metal (RG): ' + JSON.stringify(ek && ek.files.map(f => f.name + ':' + f.metal)));
    { const qk = await queued('4200900001_42009000011'); check(await page.evaluate(() => window.__dlgOpened === 0) && !!qk.dec && qk.dec.n === 1 && !!qk.prep && qk.prep.n === 1 && qk.prep.metal === 'rose', 'one piece sent (on RG), and the designs window never opened: ' + JSON.stringify(qk)); }
    if (REAL_OK) {
      const st3 = await Promise.all(TRIO.map(([, rid]) => read(rid)));
      const pa = await Promise.all(TRIO.map(([, rid]) => pictureOf(rid)));
      check(st3.every(r => r && r.state === 'sent') && pa.every(x => x && x.points >= POINT_FLOOR && x.same), 'the three monograms on the sheets still show the real letters: ' + st3.map(r => r && r.state).join(' ') + ' ' + pa.map(x => x && x.points).join(' '));
      check(pa[0].fill === TONE.gold && pa[2].fill === TONE.silver, 'each is drawn in the buyer\'s metal it was sent on (GF, SS): ' + pa[0].fill + ' ' + pa[2].fill);
      await caseShots('approved-3', T3);
    }
    // sent to the sheets: the widget is final (boxes locked, no Approve, no Change), and it is no longer counted as waiting
    await page.evaluate(() => CharmNestMonogramUI.widgets().forEach(w => w.refresh())); await sleep(300);
    a = await until('4200900001', r => r.state === 'sent', 'the sent state', 10000);
    check(a && a.state === 'sent' && a.locked && a.approve.hidden && a.change.hidden && !a.sheetLink && /^On the sheet · Monogram JQN$/.test(a.ok.text), 'a monogram sent to the sheets is final: "On the sheet · Monogram JQN", boxes locked, no Approve, no Change: ' + JSON.stringify([a && a.state, a && a.ok]));
    check(await page.evaluate(() => CharmNestMonogramUI.status(Orders.rows().find(r => r.order.receiptId === '4200900001')).label === 'On the sheets' && Engrave.monogramUI().count() === Engrave.monogramUI().rows().length - Engrave.monogramUI().rows().filter(r => CharmNestMonogramUI.status(r).sent).length), 'status says "On the sheets" and count() no longer counts it (nor any other monogram on the sheets) as waiting');
    await caseShots('sent', ['4200900001'], ['detail', 'compact']);
    // reduced motion (on T.H.S, not approved: the monograms above are on the sheets, final)
    await page.emulateMedia({ reducedMotion: 'reduce' });
    holdOn(/Monogram_Mid_G\.ai/i); await typeIn('4200900022', 1, 'G').catch(async () => { await page.click(`${W('4200900022')} [data-mw-change]`); await typeIn('4200900022', 1, 'G'); }); await sleep(500);
    const rm = await page.evaluate(() => { const h = document.querySelector('.mwCase[data-rid="4200900022"][data-case="detail"]'), i = h.querySelector('[data-mw-spin] i'), s = h.querySelector('[data-mw-pic] svg'), b = h.querySelector('[data-mw-bar]'); return { anim: getComputedStyle(i).animationName, shown: !h.querySelector('[data-mw-spin]').hidden, tr: s ? getComputedStyle(s).transitionDuration : '0s', barAnim: getComputedStyle(b.firstChild).animationName }; });
    check(rm.anim === 'none' && rm.shown && /^0s/.test(rm.tr) && rm.barAnim === 'none', 'reduced motion: the spinner is still shown but does not spin, the picture does not fade, the bar does not slide: ' + JSON.stringify(rm));
    release(); await ready('4200900022'); await page.emulateMedia({ reducedMotion: 'no-preference' });
    const mv = await page.evaluate(() => getComputedStyle(document.querySelector('[data-mw-spin] i')).animationName); check(mv === 'spin', 'with motion allowed the spinner spins: ' + mv);

    // phone width: the real 400 px viewport, both densities (the page's own bars and toasts put away)
    await page.evaluate(() => { document.querySelectorAll('.cnNbX, .toast .x').forEach(b => b.click()); });
    await page.setViewportSize({ width: 400, height: 844 }); await sleep(500);
    const ph = await page.evaluate(() => { const root = document.getElementById('mwRoot'), out = {}; for (const k of ['detail', 'compact']) { const c = document.querySelector(`.mwCase[data-rid="4202783520"][data-case="${k}"]`), m = c.querySelector('.mw'), r = c.getBoundingClientRect(), ins = [...c.querySelectorAll('input')].map(i => i.getBoundingClientRect()), btns = [...c.querySelectorAll('button:not([hidden])')].map(b => b.getBoundingClientRect()), pic = c.querySelector('[data-mw-pic]').getBoundingClientRect();
      out[k] = { left: Math.round(r.left), right: Math.round(r.right), ins: ins.map(b => [Math.round(b.left), Math.round(b.right)]), inside: [...ins, ...btns].every(b => b.left >= r.left - 1 && b.right <= r.right + 1), overflow: m.scrollWidth - m.clientWidth, pic: [Math.round(pic.width), Math.round(pic.height)] }; }
      out.w = innerWidth; out.over = Math.max(document.documentElement.scrollWidth, document.body.scrollWidth) - innerWidth; out.rootOver = root.scrollWidth - root.clientWidth; return out; });
    check(['detail', 'compact'].every(k => ph[k].left >= 0 && ph[k].right <= ph.w + 1 && ph[k].inside && ph[k].overflow <= 1) && ph.over <= 1 && ph.rootOver <= 1, 'phone width 400: both densities and all their boxes and buttons fit, no sideways scroll: ' + JSON.stringify(ph));
    await page.evaluate(() => { document.getElementById('mwRoot').scrollTo(0, 0); }); await shot('mw-400-all');
    await caseShots('phone400', ['4200683017'], ['detail', 'compact']);
    await page.setViewportSize({ width: 1440, height: 900 }); await sleep(300);

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
      Orders.interpretAll(); try { await CustomSheet.load(); } catch (_) {} Engrave.monogramUI();
      const host = document.createElement('div'); host.id = 'p2host'; document.body.append(host); const row = Orders.rows().find(r => r.order.receiptId === '4200683017'); CharmNestMonogramUI.mountLine(host, { row, mode: 'compact' });
    }, { fixtures: ['apv', 'osr', 'klm'].map(n => FX[n]), orderSrc: Base.orderOf.toString() });
    await page2.waitForSelector('#p2host svg path', { timeout: 30000 });
    await sleep(1500);
    const p2 = await page2.evaluate(() => ({ files: Object.values(CustomSheet.entries()).map(e => e.files.map(f => f.name).join()).sort(), st: document.querySelector('#p2host .mw').dataset.state, ins: [...document.querySelectorAll('#p2host input')].map(i => i.value).join('') }));
    console.log('   second tab / reload after the approvals:', JSON.stringify({ firstTab: apvNow, secondTab: p2.files, widget: p2.st, letters: p2.ins }));
    check(p2.files.join('|') === apvNow.join('|') && p2.st === 'sent' && p2.ins === 'APV', 'a second tab (a reload reads the same workspace) has the sent designs and shows the widget as on the sheet, final: ' + JSON.stringify({ files: p2.files, widget: p2.st, letters: p2.ins }));
    await page2.close();
    // ═══ Paul's own letters (on A.P + V, never approved: the monograms above are on the sheets, final) (the hard ones too: a Mid Q and a Mid T, a Right T apart from its flourish): ten words, each one piece with its counters, the centre bigger; every picture is kept as real-file-<word>.png ═══
    if (REAL_OK) {
      const sel = W('4200900012');
      await page.evaluate(s => document.querySelector(s).scrollIntoView({ block: 'start' }), sel); await sleep(200);
      await page.click(`${sel} [data-mw-change]`).catch(() => {});
      for (const wd of ['APV', 'OSR', 'AQV', 'ATB', 'ZMT', 'HFT', 'JQX', 'TTT', 'WIW', 'LDH']) {
        await pasteWord('4200900012', wd);
        await page.waitForFunction(({ sel, wd }) => { const h = document.querySelector(sel); return h && h.querySelector('[data-mw-spin]').hidden && [...h.querySelectorAll('[data-mw-letter]')].map(i => i.value).join('') === wd && ['ready', 'approved', 'check'].includes(h.dataset.state); }, { sel, wd }, { timeout: 90000 }).catch(() => {});
        const r = await page.evaluate(() => { const c = Engrave.monogramUI().cards.get(Orders.rows().find(x => x.order.receiptId === '4200900012').key); if (!c.result) return null; const hOf = p => p.bboxPt[3] - p.bboxPt[1], pl = c.result.placed; return { ok: c.result.ok, letters: c.resultLetters, rings: c.result.rings.length, outer: c.result.rings.filter(x => !x.hole).length, report: c.result.report, wMm: c.result.wMm, hMm: c.result.hMm, centre: hOf(pl[1]) / Math.max(hOf(pl[0]), hOf(pl[2])) }; });
        await page.locator(sel + ' [data-mw-pic]').screenshot({ path: path.join(SHOTS, 'real-file-' + wd + '.png') }).catch(() => {});
        const pic = await pictureOf('4200900012');
        check(!!r && r.letters === wd && r.outer === 1 && r.report.connected && r.report.holesKept >= 0.5 && r.centre >= 1.05 && !!pic && pic.same && pic.points >= POINT_FLOOR, `${wd} on Paul's letters: one piece, centre x${r && r.centre.toFixed(2)}, ${r && r.rings - 1} holes (kept ${r && r.report.holesKept}), ${r && r.wMm.toFixed(1)} × ${r && r.hMm.toFixed(1)} mm, ${pic && pic.points} points, joins ${JSON.stringify(r && r.report.joinMm)}`);
      }
    }

    check(w.errors.length === 0, 'no page error: ' + JSON.stringify(w.errors.slice(0, 3)));
  } catch (e) { fails.push('crash: ' + (e && e.stack || e)); console.error(e); try { await shot('ui-crash'); } catch (_) {} }
  finally { await w.close(); }
  console.log(`   (${Math.round((Date.now() - T0) / 1000)} s)`);
  console.log(fails.length ? `\nmonogram-ui: ${fails.length} FAILED of ${nCheck}:\n - ${fails.join('\n - ')}` : `\nmonogram-ui OK: ${nCheck} checks`);
  process.exit(fails.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
