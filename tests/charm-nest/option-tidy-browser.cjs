// OPTTIDY in the REAL page (Paul, 13 Oct 2026, order 4202291826: "intelligently strip out the needless/filler text from these options and only show the relevant information from each dropdown selection").
// The pure reader is tests/charm-nest/option-tidy.cjs; this is every place the order's option name/value pairs are drawn, with the order of the screenshot (its option names and values exactly, as the app holds them
// after the intake decoded what it knows: the name keeps its `&middot;`, the value is `GOLD·Set·ENGRAVED` / `16"&18"·Set·ANGELINA`). The headless Chromium page over the fake site (bridge-server.cjs, _orderFactsWorld.cjs): no Etsy, no
// network, nothing written anywhere.
//   1  the lists' "Selected options" (the order's row): "Metal  Gold · Engraved", "Length  16" & 18" · Font: Angelina", the original in the hover
//   2  the order window's Details list (the Overview facts): the Metal and Length cells, the Chain cell, the original in each cell's hover; the Metal chip's card
//   3  the Engrave tab's OPTIONS box (CharmNestEngraveSaid)
//   4  the Review card's Options row; a Needs-mapping card's title is decoded (it names the exact option a person is deciding on); the Engraving card's "from the ... option" line and the option its reason quotes
//   5  an order whose options need no tidying (Zodiac Sign, Birth Flower) reads word for word in the list and in Details; the engraving options keep their words
//   6  none of the filler is drawn anywhere ("&middot;", "#Characters", "Set", capitals), no page error
//   PW_DIR=<playwright node_modules> node tests/charm-nest/option-tidy-browser.cjs
const assert = require('assert/strict');
const W = require('./_orderFactsWorld.cjs');
let n = 0; const ok = (name, c) => { assert(!!c, name); n++; console.log('  ✓ ' + name); };

const TITLE = 'Bar Necklace SET of 2, Gold, Rose Gold, Silver, Custom Gold Bar Set, Engraved Necklace, Customized Name Bar Set, Personalized Bar Necklace';
// (the screenshot's two options, as Etsy sends them)
const BAR_OPTIONS = [{ name: 'Metal &middot; #Characters?', value: 'GOLD·Set·ENGRAVED' }, { name: 'Necklace Length&middot;FONT', value: '16&quot;&amp;18&quot;·Set·ANGELINA' }];
W.FIXTURES.barSet = { rid: '4202291826', sku: 'BAR_1548', listing: '532401548', title: TITLE, buyer: 'Bethany Fixture', placed: { day: -1, h: 10, m: 5 }, ship: 8, variations: BAR_OPTIONS };
W.FIXTURES.custom = { rid: '4202291828', sku: 'CUSTOM-N-024-341231', listing: '4577100359', title: 'Custom Silver Necklace Option B, Made to Order for Allison', buyer: 'Cedar Fixture', placed: { day: -1, h: 11, m: 5 }, ship: 8, variations: BAR_OPTIONS };
W.FIXTURES.zodiac = { rid: '4202291829', sku: 'ZODIAC_FIXTURE', listing: '4577100360', title: 'Zodiac sign charm necklace', buyer: 'Dune Fixture', placed: { day: -1, h: 12, m: 5 }, ship: 8,
  variations: [{ name: 'Zodiac Sign', value: 'Pisces' }, { name: 'Birth Flower', value: 'Morning Glory' }, { name: 'Font choice', value: 'Ace' }, { name: 'Charm Type', value: 'Tag1 (front+back)' }] };

async function open(page, name) {
  await page.evaluate(k => OrderWin.openOrder(k.split('_')[0]), W.keyOf(name));
  await page.waitForFunction(() => { const d = document.getElementById('orderWin'); return d.open && document.querySelector('#owMeta .owfBar') && !d.getAnimations({ subtree: true }).some(a => a.playState === 'running' && a.effect && a.effect.getTiming().iterations !== Infinity); }, null, { timeout: 15000 });
}
const shut = async page => { await page.evaluate(() => { const d = document.getElementById('orderWin'); if (d.open) d.dispatchEvent(new Event('cancel', { cancelable: true })); }); await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 8000 }).catch(() => {}); };
const details = async page => { if (await page.evaluate(() => document.querySelector('#owMeta .owfDetails').hidden)) { await page.click('#owMeta .owfMore'); await page.waitForTimeout(300); } return page.evaluate(() => [...document.querySelectorAll('#owMeta .owfDetails .m')].map(m => ({ label: m.querySelector('i').textContent, text: m.querySelector('span') ? m.querySelector('span').textContent : m.innerText, title: m.title }))); };

(async () => {
  const w = await W.world({ facts: true });
  if (!w) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const { page } = w;
  try {
    await W.seed(page, ['barSet', 'custom', 'zodiac']);
    // (what the page holds after the intake: Etsy's standard entities in a value are decoded by the app's one decoder, the stored name keeps its &middot;)
    await page.evaluate(() => { for (const r of Orders.rows()) CharmNestOrders.cleanLine(r.line); Orders.interpretAll(); });
    const stored = await page.evaluate(() => Orders.rows().find(r => r.order.receiptId === '4202291826').line.variations.map(v => [v.name, v.value]));
    ok('the order of the screenshot is held as Etsy gave it: ' + JSON.stringify(stored), JSON.stringify(stored) === JSON.stringify([['Metal &middot; #Characters?', 'GOLD·Set·ENGRAVED'], ['Necklace Length&middot;FONT', '16"&18"·Set·ANGELINA']]));

    /* 1 · the lists' "Selected options" */
    await page.evaluate(() => { CN.setMode('orders'); Orders.render(); });
    const list = await page.evaluate(() => { const rows = [...document.querySelectorAll('.purchaseChoices')]; return rows.map(c => ({ text: c.innerText.replace(/\s+/g, ' ').trim(), dts: [...c.querySelectorAll('dt')].map(x => x.textContent), dds: [...c.querySelectorAll('dd')].map(x => x.textContent), titles: [...c.querySelectorAll('dl > div')].map(x => x.title) })); });
    const bar = list.find(r => /Gold · Engraved/.test(r.text)), shown = bar || list.find(r => /Characters|GOLD/.test(r.text));
    ok('1a the list says "Metal: Gold · Engraved" and "Length: 16" & 18" · Font: Angelina": ' + JSON.stringify(shown), bar && JSON.stringify(bar.dts) === JSON.stringify(['Metal', 'Length']) && JSON.stringify(bar.dds) === JSON.stringify(['Gold · Engraved', '16" & 18" · Font: Angelina']));
    ok('1b the original is the hover of each choice', bar.titles[0] === 'Metal · #Characters?: GOLD·Set·ENGRAVED' && bar.titles[1] === 'Necklace Length·FONT: 16"&18"·Set·ANGELINA');
    const zl = list.find(r => /Zodiac Sign/.test(r.text));
    ok('5a an order that needs no tidying reads word for word in the list: ' + JSON.stringify(zl && zl.dts.map((d, i) => d + '=' + zl.dds[i])), zl && JSON.stringify(zl.dts) === JSON.stringify(['Zodiac Sign', 'Birth Flower', 'Font', 'Charm Type']) && JSON.stringify(zl.dds) === JSON.stringify(['Pisces', 'Morning Glory', 'Ace', 'Tag1 (front+back)']) && zl.titles.every((t, i) => i === 2 || !t));

    /* 2 · the order window's Details list and the Metal card */
    await open(page, 'barSet');
    const cells = await details(page), by = l => cells.find(c => c.label === l);
    ok('2a Details: Metal "Gold · Engraved" and Length "16" & 18" · Font: Angelina": ' + JSON.stringify(cells.filter(c => /Metal|Length|Chain/.test(c.label))), by('Length') && by('Length').text === '16" & 18" · Font: Angelina' && cells.filter(c => c.label === 'Metal').some(c => c.text === 'Gold · Engraved'));
    ok('2b Details: no cell is labelled with the raw name', !cells.some(c => /middot|#Characters|Necklace Length/.test(c.label)));
    ok('2c Details: the Chain cell is the length and the font without "Set": "' + (by('Chain') || {}).text + '"', by('Chain') && by('Chain').text === '16" & 18" · Angelina' && by('Chain').title === '16"&18"·Set·ANGELINA');
    ok('2d the original is the hover of each tidied cell', cells.find(c => c.text === 'Gold · Engraved').title === 'Metal · #Characters?: GOLD·Set·ENGRAVED' && by('Length').title === 'Necklace Length·FONT: 16"&18"·Set·ANGELINA');
    ok('2e the plain cells keep their words and have no hover', by('Listing').text === '532401548' && by('Title').text === TITLE && !by('Title').title);
    await page.evaluate(() => document.querySelector('[data-owf-chip=metal]').click()); await page.waitForTimeout(450);
    const pop = await page.evaluate(() => { const p = document.querySelector('#owMeta .owfPop'); return p && !p.hidden ? [...p.querySelectorAll('.owfRow')].map(r => (r.querySelector('dt') ? r.querySelector('dt').textContent + ': ' : '') + r.querySelector('dd').textContent) : null; });
    ok('2f the Metal chip\'s card (titled Metal) says the choice "Gold · Engraved" too: ' + JSON.stringify(pop), pop && pop.includes('Gold · Engraved') && !pop.some(t => /middot|#Characters|\bSet\b/.test(t)));
    await shut(page);

    /* 3 · the Engrave tab's OPTIONS box */
    const said = await page.evaluate(() => { const r = Orders.rows().find(x => x.order.receiptId === '4202291826'); const d = document.createElement('div'); d.innerHTML = CharmNestEngraveSaid.html(r); const lbl = d.querySelector('.lbl'); return { text: d.querySelector('.owSaid').textContent, label: lbl.textContent, title: lbl.title, rows: CharmNestEngraveSaid.rowsOf(r) }; });
    ok('3a the OPTIONS box: "Metal: Gold · Engraved" / "Length: 16" & 18" · Font: Angelina": ' + JSON.stringify(said.text), said.label === 'Options' && said.rows.length === 1 && said.rows[0][1] === 'Metal: Gold · Engraved\nLength: 16" & 18" · Font: Angelina');
    ok('3b the original reads in the label\'s hover', said.title === 'As ordered:\nMetal · #Characters?: GOLD·Set·ENGRAVED\nNecklace Length·FONT: 16"&18"·Set·ANGELINA');
    const zs = await page.evaluate(() => { const r = Orders.rows().find(x => x.order.receiptId === '4202291829'); return CharmNestEngraveSaid.rowsOf(r); });
    ok('5b the engraving options of the other order keep their words: ' + JSON.stringify(zs), JSON.stringify(zs) === JSON.stringify([['Options', 'Font: Ace\nCharm Type: Tag1 (front+back)']]));

    /* 4 · the Review card's Options row */
    const card = await page.evaluate(() => { const it = Review.items().find(x => x.row && x.row.order.receiptId === '4202291828'); if (!it) return null; const c = Review.card(it); return { kind: it.kind, qs: [...c.querySelectorAll('.ev q')].map(q => [q.textContent, q.title]) }; });
    ok('4 the Review card says the options the same way, the original in the hover: ' + JSON.stringify(card), card && JSON.stringify(card.qs) === JSON.stringify([['Metal: Gold · Engraved', 'Metal · #Characters?: GOLD·Set·ENGRAVED'], ['Length: 16" & 18" · Font: Angelina', 'Necklace Length·FONT: 16"&18"·Set·ANGELINA']]));

    // (a Needs-mapping card is where a person decides what ONE option means: its title is the option as it is, only decoded, so what is saved is what is read)
    const head = await page.evaluate(() => { const row = Orders.rows().find(r => r.order.receiptId === '4202291828'); const c = Review.card({ kind: 'needsMapping', key: 'k-tidy', row, problem: { kind: 'needsMapping', optionName: 'Metal &middot; #Characters?', optionValue: 'GOLD·Set·ENGRAVED', listingId: '4577100359', title: 'T' }, rows: [row] }); return c.querySelector('.ttl').textContent; });
    ok('4b a Needs-mapping card\'s title is the option decoded (no "&middot;"), never tidied: "' + head + '"', head === 'Metal · #Characters?: GOLD·Set·ENGRAVED');

    // (the Engraving card's "from the ... option" line and the reason it quotes: the option said as the display reader says it; the buyer's own quoted words are never touched)
    const su = await page.evaluate(() => {
      const r = Orders.rows().find(x => x.order.receiptId === '4202291826'), was = r.spec.engraving, out = {};
      const eng = why => ({ v: 1, mode: 'back', source: 'option', front: null, back: { text: 'Ginger', lines: ['Ginger'], fontWord: null }, textSource: 'labelled', needsPerson: false, confidence: 0.9, why, option: { name: 'Necklace Length&middot;FONT', value: '16"&18"\u00b7Set\u00b7ANGELINA' } });
      try {
        r.spec.engraving = eng('the option \u201cMetal &middot; #Characters?: GOLD\u00b7Set\u00b7ENGRAVED\u201d asks for the back'); let vm = CharmNestEngraveSetup.read(r); out.from = vm.from; out.why = vm.why;
        r.spec.engraving = eng('the buyer\'s note says \u201cFont: TYPEWRITE &amp; Mom\u201d'); vm = CharmNestEngraveSetup.read(r); out.note = vm.why;
      } finally { r.spec.engraving = was; }
      return out;
    });
    ok('4c the Engraving card names the option as the display reader does: ' + JSON.stringify(su), su.from === 'from the Length \u00b7 Font option \u00b7 90% sure' && su.why === 'the option \u201cMetal: Gold \u00b7 Engraved\u201d asks for the back' && su.note === 'the buyer\'s note says \u201cFont: TYPEWRITE & Mom\u201d');

    /* 5 · the other order in Details */
    await open(page, 'zodiac');
    const zc = await details(page);
    ok('5c Details: Zodiac Sign "Pisces", Birth Flower "Morning Glory" word for word, no hover: ' + JSON.stringify(zc.filter(c => /Zodiac|Birth|Font|Charm Type/.test(c.label)).map(c => c.label + '=' + c.text)), ['Zodiac Sign=Pisces', 'Birth Flower=Morning Glory', 'Font=Ace', 'Charm Type=Tag1 (front+back)'].every(t => zc.some(c => c.label + '=' + c.text === t)) && zc.filter(c => /Zodiac|Birth|Charm Type/.test(c.label)).every(c => !c.title));
    await shut(page);

    /* 6 · none of the filler anywhere */
    const everything = await page.evaluate(() => document.body.innerText);
    ok('6a no "&middot;" and no "#Characters" is drawn anywhere on the page', !/&middot;|#Characters/.test(everything));
    ok('6b no page error: ' + w.errors.join(' | '), w.errors.length === 0);
  } finally { await w.close(); }
  console.log(`option-tidy-browser: ${n} checks ok`);
})().catch(e => { console.error(e); process.exit(1); });
