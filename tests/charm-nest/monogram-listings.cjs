// The seven Etsy listings Paul named for the monogram alphabets (15 and 16 Oct 2026): 997905351, 606439317, 1844317367, 1009221671, 1844317391, 1008031671, 603425508.
// "These listings have their own SKUs, but those SKUs must be ignored, and these listings must be filed as monograms, and then automatically routed to the monogram process flow."
// In the real sorter (charm-nest-1.html, headless Chromium, the fake loopback site: no Etsy, nothing live), over the three alphabets in the library, an order of each listing goes through the real intake:
//   · it is filed as a custom monogram (special.monogram) by the LISTING NUMBER alone (the titles here never say "monogram"), asks nothing (no SKU, metal or option question), is its own design, has no engraving;
//   · its own SKU is ignored: some of those SKUs ARE designs of the library, and no design of them is ever fetched, matched or asked about;
//   · with no action by anybody the monogram widget (charm-nest-monogram-ui.js mountLine) shows each, the buyer's three letters in it, built from the Left, Middle and Right alphabets;
//   · nothing else is pulled in: a listing next to one of them, a regular charm, are not monograms.
//   PW_DIR=<dir with playwright-core> node tests/charm-nest/monogram-listings.cjs
'use strict';
const Base = require('./_orderFactsWorld.cjs'), A = require('./_monogramAlphabets.cjs');
const V = (name, value) => ({ name, value });
const TITLE = 'Dainty Personalized Pendant Necklace';              // (no "monogram" in it: only the listing number can do it)
const LISTINGS = [['997905351', 'APV'], ['606439317', 'O S R'], ['1844317367', 'K.L.M'], ['1009221671', 'Initials: T R Y'], ['1844317391', 'J Q X'], ['1008031671', 'b e c'], ['603425508', 'HFD']];
const WORDS = ['APV', 'OSR', 'KLM', 'TRY', 'JQX', 'BEC', 'HFD'];
const FX = {}; LISTINGS.forEach(([listing, words], i) => { FX['m' + i] = { rid: '42010000' + (10 + i), sku: 'OWNSKU_' + (10 + i), listing, title: TITLE, buyer: 'Buyer ' + i, placed: { day: 0, h: 9, m: 10 + i }, ship: 5, variations: [V('Metal choice', i % 2 ? 'Silver' : 'Gold')], personalization: [words], metalKey: i % 2 ? 'silver' : 'gold' }; });
FX.near = { rid: '4201000090', sku: 'OWNSKU_90', listing: '1844317368', title: TITLE, buyer: 'Buyer N', placed: { day: 0, h: 11, m: 0 }, ship: 5, variations: [V('Metal choice', 'Gold')], personalization: ['A P V'], metalKey: 'gold' };   // (one off from 1844317367)
FX.plain = { rid: '4201000091', sku: 'HEART_CHARM', listing: '888000004', title: 'Heart Charm Necklace', buyer: 'Buyer P', placed: { day: 0, h: 11, m: 5 }, ship: 5, variations: [V('Metal choice', 'Gold')], personalization: ['A P V'] };

(async () => {
  const w = await Base.world({ viewport: { width: 1440, height: 900 } });
  if (!w) { console.log('monogram-listings: no playwright-core, skipped'); return; }
  const { page, srv } = w, st = srv.st, sleep = ms => new Promise(r => setTimeout(r, ms)); let n = 0; const fails = [];
  const check = (ok, msg) => { n++; if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
  try {
    A.seed(st, await A.make());                                    // the three alphabets, in the library
    // three of the listings' own SKUs ARE designs of the library (a catalogue charm of that name): they must still be ignored
    for (const sku of ['OWNSKU_12', 'OWNSKU_14', 'OWNSKU_16']) st.put('Charm_Master_Index', sku, { sku, masterHash: 'own1', widthPt: 40, heightPt: 50, areaPt2: 1500, members: 2, holes: 1, upAngle: 0, aiPath: 'charmnest/master/' + sku + '.ai', thumbPath: 'charmnest/master/' + sku + '.png' });
    for (let i = 0; i < 8; i++) { const want = [...st.docs.keys()].filter(k => k.startsWith('Charm_Master_Index/')).length, have = await page.evaluate(async () => { await Master.load(true); return B.master.entries.size; }); if (have === want) break; await sleep(400); }
    check(await page.evaluate(() => ['OWNSKU_12', 'OWNSKU_14', 'OWNSKU_16'].every(s => !!B.master.entries.get(s))), 'three of the listings\' own SKUs are designs of the library (they must be ignored)');

    await page.evaluate(async ({ fixtures, orderSrc }) => {
      const orderOf = eval('(' + orderSrc + ')'), now = Date.now(); await Orders.loadMaps(true);
      for (const fx of fixtures) { const order = orderOf(fx, now); for (const line of order.lines) { if (fx.metalKey) line.metalKey = fx.metalKey; const key = CharmNestOrders.lineKey(order, line); if (B.orders.byKey.get(key)) continue; const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); } }
      Orders.interpretAll();
    }, { fixtures: Object.values(FX), orderSrc: Base.orderOf.toString() });

    // ═══ 1 · the intake: filed as monograms by the listing number ═══
    const specs = await page.evaluate(() => Orders.rows().map(r => ({ rid: r.order.receiptId, listing: r.line.listingId, mono: !!(r.spec && r.spec.special && r.spec.special.monogram), label: r.spec && r.spec.special && r.spec.special.label, words: r.spec && r.spec.special && r.spec.special.words, own: !!(r.spec && r.spec.ownDesign), noDesign: !!(r.spec && r.spec.noDesign), engrave: !!(r.spec && r.spec.engraveCandidate), problems: (r.spec && r.spec.problems || []).length, why: r.spec && r.spec.special && r.spec.special.why })));
    const by = rid => specs.find(s => s.rid === rid) || {};
    LISTINGS.forEach(([listing], i) => { const s = by(FX['m' + i].rid); check(s.mono && s.listing === listing && s.label === 'Custom monogram' && s.own && !s.noDesign && !s.engrave && s.problems === 0, `listing ${listing}: filed as a custom monogram, its own design, no question, no engraving` + (s.mono ? '' : ' — NOT a monogram: ' + JSON.stringify(s))); });
    check(!by(FX.near.rid).mono, 'listing 1844317368 (one off from a named one) is NOT a monogram: only the named listing numbers are');
    check(!by(FX.plain.rid).mono, 'a regular charm listing is not a monogram');

    // ═══ 2 · routed to the monogram flow, nothing pressed ═══
    await page.evaluate(() => { window.__fetched = []; const mc = Pool.masterCharm; Pool.masterCharm = function (e, s) { window.__fetched.push(e.sku); return mc.apply(this, arguments); }; });
    // (the monogram lines mounted as the Engrave detail mounts them: charm-nest-monogram-ui.js mountLine, one widget per line that the order reading calls a monogram)
    const nMono = await page.evaluate(() => { Engrave.monogramUI(); const rootEl = document.createElement('div'); rootEl.id = 'mwRoot'; rootEl.style.cssText = 'position:fixed;inset:0;z-index:2147483000;overflow:auto;background:#faf7f2;padding:16px;display:flex;flex-wrap:wrap;gap:14px;align-content:flex-start'; document.body.append(rootEl);
      const rows = Orders.rows().filter(r => CharmNestMonogramUI.isMonogram(r)); for (const r of rows) { const cs = document.createElement('div'); cs.className = 'mwCase'; cs.dataset.rid = r.order.receiptId; cs.style.cssText = 'box-sizing:border-box;width:760px;max-width:100%;border:1px solid var(--line);border-radius:12px;background:var(--card);padding:16px'; const host = document.createElement('div'); cs.append(host); rootEl.append(cs); CharmNestMonogramUI.mountLine(host, { row: r, mode: 'detail' }); } return [rows.length, Engrave.monogramUI().count()]; });
    await page.waitForFunction(() => document.querySelectorAll('.mwCase .mw [data-mw-pic] svg path').length >= 7, null, { timeout: 90000 });
    check(nMono[0] === 7 && nMono[1] === 7, 'seven monogram lines, and the service counts 7 waiting: ' + JSON.stringify(nMono));
    const cards = await page.evaluate(() => [...document.querySelectorAll('.mwCase')].map(c => { const h = c.querySelector('.mw'); return { rid: c.dataset.rid, says: h.querySelector('[data-mw-says]').textContent, ins: [...h.querySelectorAll('[data-mw-letter]')].map(i => i.value).join(''), drawn: !!h.querySelector('[data-mw-pic] svg path') }; }));
    check(cards.length === 7 && FX && LISTINGS.every((_, i) => cards.some(c => c.rid === FX['m' + i].rid)), 'one widget per listing (7), none for the listing next door or the regular charm: ' + cards.map(c => c.rid.slice(-2)).join(' '));
    LISTINGS.forEach(([listing], i) => { const c = cards.find(x => x.rid === FX['m' + i].rid) || {}; check(c.drawn && c.ins === WORDS[i] && new RegExp('^The buyer wrote ' + WORDS[i].split('').join(' ') + '$').test(c.says || ''), `listing ${listing}: its card is drawn with the buyer's letters: "${c.says}"`); });
    const fetched = await page.evaluate(() => [...new Set(window.__fetched)]);
    check(!fetched.some(s => /^OWNSKU_/.test(s)), 'no design of the listings\' own SKUs was ever fetched: ' + fetched.filter(s => !/^MONOGRAM_/.test(s)).join(' '));
    check(WORDS.every(wd => fetched.includes('MONOGRAM_LEFT_' + wd[0]) && fetched.includes('MONOGRAM_MID_' + wd[1]) && fetched.includes('MONOGRAM_RIGHT_' + wd[2])), 'each word came from the Left, Middle and Right alphabets');
    check(w.errors.length === 0, 'no page error: ' + w.errors.slice(0, 2).join(' | '));
  } finally { await w.close(); }
  if (fails.length) { console.error('\nmonogram-listings FAILED:\n - ' + fails.join('\n - ')); process.exit(1); }
  console.log('monogram-listings OK ·', n, 'checks');
})().catch(e => { console.error(e); process.exit(1); });
