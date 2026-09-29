// Adversarial (28 Sep, wave 5; Paul: "some of the animations were not exactly visible and they were jerky"): the order
// view's decision box (#owFix, a custom order's question) used to open and fold away on transform, opacity and
// clip-path only. Since Paul, 29 Sep 01:01 ("Remove this from the UI and the review tab, from all pop-up modals": the
// red "This line is waiting on a decision" box) there is no box to fold: measured in a real Chromium on a custom order's
// own line, the question it raised answered and asked again,
// - no decision box is ever shown: #owFix stays empty (no question, no "waiting on a decision"), and no copy of a box
//   folds anywhere;
// - what was under the box stands where it stood (#owFix keeps one height, the field under it never moves), and nothing
//   in the view animates any property but transform, opacity and clip-path; nothing is left moved.
// Frame times are gated only when the load per core is under 1.2 (the machine is shared).
// Headless Chromium against the fake site (bridge-server.cjs); every request that is not to the loopback is aborted; the
// custom reading is answered by the fake site's stub model: no paid call, no Etsy call.
//   node tests/charm-nest/adv-owfix-fold.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), os = require('os'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const order = (rid, lines) => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + String(rid).slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
const line = (tid, sku, title, variations, extra) => Object.assign({ transactionId: String(tid), listingId: String(1800000000 + (tid % 100000)), sku, title, quantity: 1, expectedShipDate: SHIP, variations: variations.map(([name, value]) => ({ name, value })), metalKey: '', metalLabel: '', personalization: [], buyerMessage: '' }, extra || {});
const ORDERS = [
  order(4174476673, [line(41744766731, 'CUSTOM_6673', 'CUSTOM CHARM', [['Price', '28']])]),   // custom, asks for its metal
  order(4178000001, [line(41780000011, 'BLOOMING_20239', 'Blooming Flower Charm Necklace', [['Metal', '14k Gold Filled']], { metalKey: 'gold', metalLabel: 'GF 14/20' })])
];
const A = '4174476673_41744766731';
const OK_PROPS = ['transform', 'opacity', 'clipPath'];

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const load = () => os.loadavg()[0] / os.cpus().length;
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 950 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.route(/\/\.netlify\/functions\/etsyMailOrderLink/, r => r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ engagements: [], active: null, conversation: null, ok: true, n: 0 }) }));
    await context.addInitScript(() => {
      try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); localStorage.setItem('cn.mail.station', JSON.stringify('k-adv-test')); sessionStorage.setItem('__seeded', '1'); } } catch (_) {}
      window.prompt = () => 'Test Operator';
      // every frame while measuring: #owFix's height, where the field under it is drawn, the box and its folding copy,
      // and every property animated in the view
      window.__f = {
        begin() { const m = this; m.on = true; m.t0 = performance.now(); m.fr = []; m.props = new Set(); m.targets = new Set();
          const loop = t => { if (!m.on) return;
            const d = document.getElementById('orderWin'), fix = document.getElementById('owFix'), next = fix && fix.nextElementSibling, box = fix && fix.querySelector(':scope > .owFix');
            const g = d && d.querySelector('.motionLayer .mGhost'), gs = g && getComputedStyle(g), bs = box && getComputedStyle(box);
            for (const a of document.getAnimations()) { const n = a.effect && a.effect.target; if (!n || !n.closest || !n.closest('#orderWin') || a.playState !== 'running') continue;
              for (const k of a.effect.getKeyframes()) for (const p of Object.keys(k)) if (!['offset', 'easing', 'composite', 'computedOffset'].includes(p)) { m.props.add(p); if (!OK.includes(p)) m.targets.add((n.id || n.className || n.tagName) + ':' + p); } }
            m.fr.push({ t: t - m.t0, fixH: Math.round(fix.getBoundingClientRect().height * 2) / 2, nextTop: next ? Math.round(next.getBoundingClientRect().top * 10) / 10 : null,
              ghost: g ? { op: +gs.opacity, clip: gs.clipPath, tf: gs.transform } : null, box: box ? { op: +bs.opacity, clip: bs.clipPath } : null });
            requestAnimationFrame(loop); };
          const OK = ['transform', 'opacity', 'clipPath'];
          requestAnimationFrame(loop); },
        end() { this.on = false; return { fr: this.fr, props: [...this.props], targets: [...this.targets] }; }
      };
    });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.OrderWin && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async orders => {
      await Orders.loadMaps(true);
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('orders'); Orders.render();
    }, ORDERS);

    await page.evaluate(k => OrderWin.open(k), A);
    await page.waitForFunction(k => OrderWin.isOpen() && OrderWin.key() === k && !document.querySelector('#orderWin').classList.contains('owLoading'), A);
    await page.waitForTimeout(1200);   // the view has landed
    const asked = () => page.evaluate(() => ({ fix: document.getElementById('owFix').innerHTML, controls: document.querySelectorAll('#orderWin .cuStep, #orderWin .owFixCard, #orderWin .rvItem, #orderWin [data-f=mat]').length,
      words: /waiting on a decision|decide below/i.test(document.getElementById('orderWin').textContent) }));
    const none = { fix: '', controls: 0, words: false };
    assert.deepEqual(await asked(), none, 'the custom order\'s line shows no decision box');
    assert(await page.evaluate(k => Review.items().some(it => (it.rows || [it.row]).some(r => r && r.key === k) && !it.info && !it.done), A), 'its line does wait on a decision (in Review): the window asks nothing all the same');

    // the question answered (the line no longer raises it), then asked again: nothing opens or folds, nothing moves
    const measure = async fn => { await page.evaluate(() => __f.begin()); await page.evaluate(fn, A); await page.waitForTimeout(1150); return page.evaluate(() => __f.end()); };
    const l0 = load();
    const fold = await measure(k => { const r = Orders.rows().find(x => x.key === k); r.problems = []; Review.syncOrderItems(); OrderWin.paint(); });
    const folded = Object.assign(await asked(), await page.evaluate(() => ({ ghosts: document.querySelectorAll('#orderWin .motionLayer .mGhost').length })));
    const openM = await measure(() => { Orders.interpretAll(); Review.syncOrderItems(); OrderWin.paint(); });
    const l1 = load();
    const end = await page.evaluate(() => {
      const fix = document.getElementById('owFix');
      const moving = [...fix.parentNode.children].filter(n => getComputedStyle(n).transform !== 'none').map(n => n.id || n.className);
      return { fix: fix.innerHTML, moving, ghosts: document.querySelectorAll('#orderWin .motionLayer .mGhost').length };
    });

    const report = (name, m) => {
      const hs = [...new Set(m.fr.map(f => f.fixH))], tops = m.fr.map(f => f.nextTop);
      const dts = m.fr.slice(1).map((f, i) => f.t - m.fr[i].t).filter((_, i) => m.fr[i + 1].t < 800);
      console.log(`  ${name.padEnd(8)} #owFix heights ${JSON.stringify(hs)} · field under it drawn at ${new Set(tops).size} position(s) · animated: ${m.props.join(',') || 'nothing'} · worst frame ${Math.round(Math.max(0, ...dts))} ms`);
      return { hs, positions: new Set(tops).size, over34: dts.filter(x => x > 34).length, box: m.fr.some(f => f.box), ghost: m.fr.some(f => f.ghost) };
    };
    const F = report('answered', fold), O = report('asked', openM);

    for (const [name, m, R] of [['answered', fold, F], ['asked again', openM, O]]) {
      assert.deepEqual(m.targets, [], `${name}: nothing in the view animates a property but ${OK_PROPS.join(', ')}`);
      assert(!R.box && !R.ghost, `${name}: no decision box, and no copy of one folding, in any frame`);
      assert.deepEqual(R.hs, [0], `${name}: #owFix takes no room (what was under the box stands where it stood)`);
      assert.equal(R.positions, 1, `${name}: the field under it never moves`);
    }
    assert.deepEqual(folded, Object.assign({ ghosts: 0 }, none), 'answered: nothing asked, no copy');
    // ends as it began: nothing asked, nothing left moved, no copy left behind
    assert.equal(end.fix, '', 'asked again: still no decision box');
    assert.deepEqual(await asked(), none, 'asked again: nothing asked');
    assert.deepEqual(end.moving, [], 'nothing under it is left moved');
    assert.equal(end.ghosts, 0, 'no copy left behind');
    assert.deepEqual(errors, [], 'no page errors');
    const busy = Math.max(l0, l1);
    if (busy < 1.2) { assert(F.over34 <= 2 && O.over34 <= 2, `at most two frames over 34 ms (answered ${F.over34}, asked ${O.over34})`); console.log(`  ✓ frame times held (load per core ${busy.toFixed(2)})`); }
    else console.log(`  – load per core ${busy.toFixed(2)} ≥ 1.2: frame times not gated`);
    console.log('  ✓ no decision box: the question answered or asked again, nothing opens or folds, nothing under it moves');
    await context.close();
  } finally { await browser.close(); await srv.close(); }
}
main().then(() => console.log('adv-owfix-fold: ok'), e => { console.error(e); process.exit(1); });
