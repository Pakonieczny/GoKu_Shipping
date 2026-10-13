// OPTTIDY (Paul, 13 Oct 2026, order 4202291826, the OPTIONS box of the order window): "You need to intelligently strip out the needless/filler text from these options and only show the relevant
// information from each dropdown selection." The box read `Metal &middot; #Characters?:  GOLD·Set·ENGRAVED` and `Necklace Length&middot;FONT:  16"&18"·Set·ANGELINA`.
//
// The ONE display reader is CharmNestOrders.tidyOption / optionView / optionViews (charm-nest-orders.js section 2d); the Engrave box (charm-nest-engrave-said.js) draws with it. This file is the pure part (node, no browser, no
// network, nothing written): option-tidy-browser.cjs is the same order in the real page (the order window's Details list, the lists' Selected options, the Review card).
//   1  the two lines of the screenshot, exactly: "Metal: Gold · Engraved" and "Length: 16" & 18" · Font: Angelina"
//   2  the whole listing 532401548 (SET of 2): every metal x engraved/blank, every length pair x every font (the 18"& 20" typed with a space, TYPEWRTE, CUSTOM · (add note))
//   3  REAL option names and values from the fixtures (font-audit-values, adv-count-lines, addon-snapshot-lines, font-option-lines): Length/Font/Alignment, Metal? Font Choice?, Metal Choice &middot; Engraving Options?,
//      Number of Discs / Metal ...: tidied; and the options that need no tidying (Zodiac Sign, Birth Flower, Viking Rune Symbol, Charm Size, Charm Type, Price ...) come out word for word
//   4  nothing is hidden that Paul did not name: every word of every real value is still there (only "Set", and a font word in the shop's own spelling, may change)
//   5  DISPLAY ONLY: the stored/matched side is untouched (the decoder, purchaseOptions, norm, cleanLine, the input objects), the original stays in `raw` / `was`
//   6  safety: markup in a name or value is text (the Engrave box escapes it), the customer's own personalisation words are never tidied, a placeholder is dropped, "Rose" is a flower on a Birth Flower option
//   7  the Engrave tab's OPTIONS box (CharmNestEngraveSaid): the two lines, the original in the label's hover, nothing extra when nothing was tidied
//   node tests/charm-nest/option-tidy.cjs
const assert = require('node:assert/strict'), path = require('path'), fs = require('fs');
const root = path.join(__dirname, '../..'), fx = f => JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures', f), 'utf8'));
const O = global.CharmNestOrders = require(path.join(root, 'charm-nest-orders.js'));
const Said = require(path.join(root, 'charm-nest-engrave-said.js'));
let n = 0; const ok = name => { n++; console.log('  ✓ ' + name); };

assert.equal(typeof O.tidyOption, 'function', 'CharmNestOrders.tidyOption is the one display reader of an option');
const line = (name, value, ctx) => { const t = O.optionView(name, value, ctx); return t.line; };
const TITLE = 'Bar Necklace SET of 2, Gold, Rose Gold, Silver, Custom Gold Bar Set, Engraved Necklace, Customized Name Bar Set, Personalized Bar Necklace';

/* ── 1 · the screenshot, exactly ── */
{
  const a = O.tidyOption('Metal &middot; #Characters?', 'GOLD·Set·ENGRAVED'), b = O.tidyOption('Necklace Length&middot;FONT', '16"&18"·Set·ANGELINA');
  assert.equal(a.line, 'Metal: Gold · Engraved');
  assert.deepEqual([a.label, a.text], ['Metal', 'Gold · Engraved']);
  assert.equal(b.line, 'Length: 16" & 18" · Font: Angelina');
  assert.deepEqual([b.label, b.text], ['Length', '16" & 18" · Font: Angelina']);
  // (the same with the listing's title: "SET of 2" already says it is a set, and as Etsy really sends the value: the quotes and the ampersand escaped)
  assert.equal(line('Metal &middot; #Characters?', 'GOLD&middot;Set&middot;ENGRAVED', { title: TITLE }), 'Metal: Gold · Engraved');
  assert.equal(line('Necklace Length&middot;FONT', '16&quot;&amp;18&quot;&middot;Set&middot;ANGELINA', { title: TITLE }), 'Length: 16" & 18" · Font: Angelina');
  for (const t of [a, b]) assert.ok(!/&middot;|#|\?|\bSet\b|[A-Z]{4,}/.test(t.line.replace(/\bANGELINA\b/g, '')) && !/Characters/.test(t.line), 'no entity, no question label, no "Set", no capitals left: ' + t.line);
  ok('the two lines of the screenshot: "Metal: Gold · Engraved" and "Length: 16" & 18" · Font: Angelina"');
}

/* ── 2 · the whole listing 532401548 ── */
{
  const metals = { GOLD: 'Gold', ROSE: 'Rose gold', SILVER: 'Silver' }, kinds = { ENGRAVED: 'Engraved', BLANK: 'Blank' };
  let count = 0;
  for (const [m, mt] of Object.entries(metals)) for (const [k, kt] of Object.entries(kinds)) { assert.equal(line('Metal · #Characters?', `${m}·Set·${k}`, { title: TITLE }), `Metal: ${mt} · ${kt}`); assert.equal(line('Metal · #Characters?', `${m}·Set·${k}`), `Metal: ${mt} · ${kt}`, 'the same without the title: the name has no slot for "Set"'); count++; }
  const lengths = [['15"&17"', '15" & 17"'], ['16"&18"', '16" & 18"'], ['17"&19"', '17" & 19"'], ['18"&20"', '18" & 20"'], ['18"& 20"', '18" & 20"']];
  const fonts = { ANGELINA: 'Angelina', COMIC: 'Comic', STYLISH: 'Stylish', TYPEWRTE: 'Typewriter', PRISTINA: 'Pristina' };
  for (const [lv, lt] of lengths) for (const [f, ft] of Object.entries(fonts)) { assert.equal(line('Necklace Length·FONT', `${lv}·Set·${f}`, { title: TITLE }), `Length: ${lt} · Font: ${ft}`); assert.equal(line('Necklace Length&middot;FONT', `${lv}·Set·${f}`), `Length: ${lt} · Font: ${ft}`); count++; }
  // CUSTOM · (add note): the bracket is a remark, not a font: the pair is NOT made up
  assert.equal(line('Necklace Length·FONT', 'CUSTOM · (add note)', { title: TITLE }), 'Length · Font: Custom · (add note)');
  ok(`the whole listing (${count + 1} choices): every metal and engraved/blank, every pair of lengths, every font (TYPEWRTE is Typewriter), "CUSTOM · (add note)" is never paired`);
}

/* ── 3 · real names and values from the fixtures ── */
const pairs = new Map();   // "name\nvalue" -> [name, value]
{
  const walk = (x, key) => {
    if (Array.isArray(x)) { if (x.length === 2 && typeof x[0] === 'string' && typeof x[1] === 'string' && key === 'vars') pairs.set(x[0] + '\n' + x[1], x); for (const y of x) walk(y, key); }
    else if (x && typeof x === 'object') { if (typeof x.name === 'string' && typeof x.value === 'string') pairs.set(x.name + '\n' + x.value, [x.name, x.value]); for (const [k, v] of Object.entries(x)) walk(v, k); }
  };
  for (const f of ['font-audit-values.json', 'adv-count-lines.json', 'addon-snapshot-lines.json', 'font-option-lines.json']) walk(fx(f), '');
  const real = (name, value) => { assert.ok(pairs.has(name + '\n' + value), `the fixtures hold the real option ${JSON.stringify(name)} = ${JSON.stringify(value)}`); return [name, value]; };
  const WANT = [
    [['Necklace Length', '16"·STYLISH·Center'], 'Length: 16" · Stylish · Center'],                                  // (a plain "Necklace Length" whose value is fused: nothing to pair with, nothing lost)
    [['LENGTH / FONT / ALIGNMENT', '16"·PRISTINA·Center'], 'Length: 16" · Font: Pristina · Alignment: Center'],
    [['Necklace Length /Font /Alignment', '18"·TYPEWRITE·Center'], 'Length: 18" · Font: Typewriter · Alignment: Center'],
    [['Necklace Length & Font Choice:', '17"/Typewriter'], 'Length: 17" · Font: Typewriter'],                         // (a slash in the value, "&" and a colon in the name)
    [['Necklace Length and Font', '14" ·NO FONT·'], 'Length: 14" · Font: No font'],
    [['Metal? Font Choice?', 'Gold • Comic'], 'Metal: Gold · Font: Comic'],
    [['Metal Choice &middot; Engraving Options?', 'GOLD·6-10·Characters'], 'Metal: Gold · Engraving: 6-10 Characters'],   // (REAL "&middot;"; "6-10" and "Characters" are one choice)
    [['Metal Choice &middot; Engraving Options?', 'SILVR·6-10·Character'], 'Metal: Silver · Engraving: 6-10 Character'],
    [['Lengths of Necklaces · Font Choice?', 'set·15"16.5"·TypeWrt'], 'Length: 15" & 16.5" · Font: Typewriter'],       // (two lengths with no separator; "set" has no slot)
    [['Metal Choice / Necklace Length', '14k Solid Gold - 16"'], 'Metal: 14k Solid Gold · Length: 16"'],
    [['Font/ Chain Length', 'stylish • 20 inches'], 'Font: Stylish · Length: 20 inches'],
    [['Chain Length', '7 inch • Typewritr'], 'Length: 7 inch · Typewriter'],
    [['Number of Discs / Metal', '3 discs • gold'], 'Number of Discs: 3 discs · Metal: Gold'],
    [['Metal Choice :', 'Silver • 1 symbol'], 'Metal: Silver · 1 symbol'],
    [['Font Choice (see listing photo for choices)', 'Vibur'], 'Font: Vibur'],
    [['Font Choice', 'Britannic Bold'], 'Font: Britannic Bold'],
    [['Metal Choice', '14K SOLID GOLD'], 'Metal: 14K SOLID GOLD']   // (only the label's filler goes; a value that is not a whole token stays as it is)
  ];
  for (const [[nm, v], want] of WANT) assert.equal(line(...real(nm, v)), want, `${nm} = ${v}`);
  // the options that need no tidying come out word for word as "name: value" (Zodiac, Birth Flower, a rune, a size, a type, a price ...)
  const SAME = [['Zodiac Sign', 'Pisces'], ['Birth Flower', 'Morning Glory'], ['Viking Rune Symbol', 'Algiz - Divine Plan'], ['Goddess Symbol', 'Vesta'], ['Charm Size', '14mm + engraving'], ['Charm Type', 'Tag1 (front engrave)'],
    ['Charm Type', 'CHARM + Engraving'], ['Price', 'Gold Filled - Single'], ['Price', '20"-21"'], ['Metal', 'Rose Gold Filled'], ['Metal', 'RoseGold'], ['Metal', '4 Silver / 2 Gold'], ['Metal', 'Gold - Queen'], ['Length', '17 Inches'],
    ['Necklace Length in inches', 'Charm Only-No Chain'], ['Ring size', '8 US'], ['Fonts', '16"/ Typewriter'], ['Necklace Options', 'ROSEGOLD - 2 Disc']];
  for (const [nm, v] of SAME) { const t = O.optionView(...real(nm, v)); assert.equal(t.line, nm + ': ' + v, `${nm} = ${v} is shown word for word`); assert.equal(t.changed, false); }
  ok(`${WANT.length} real option names and values from the fixtures are tidied (Length / Font / Alignment, Metal? Font Choice?, Metal Choice &middot; Engraving Options?, Number of Discs / Metal ...)`);
  ok(`${SAME.length} real options that need no tidying (Zodiac Sign, Birth Flower, Viking Rune Symbol, Charm Size, Charm Type, Price, Metal ...) come out word for word`);
}

/* ── 4 · nothing is hidden that Paul did not name: every word of every real value is still on the line ── */
{
  const words = s => O.shownText(s).toLowerCase().match(/[a-z0-9]+(?:\.\d+)?/g) || [];   // (words and numbers: the separators, the inch marks and the "&" between two lengths are not words)
  const FILLER = new Set(['set']);   // ("Set": Paul named it)
  let checked = 0; const lost = [];
  for (const [name, value] of pairs.values()) {
    const t = O.optionView(name, value), got = new Set(words(t.line));
    for (const w of words(value)) {
      if (got.has(w) || FILLER.has(w)) continue;
      const canon = O.fontOfWord(w) || /^silvr$/.test(w);   // (a font word in the shop's own spelling: TypeWrt -> Typewriter; the typo SILVR -> Silver)
      if (!canon) lost.push(`${name} = ${value}: ${w}`);
    }
    checked++;
  }
  assert.deepEqual(lost, [], 'a word of a value went missing');
  assert.ok(checked > 150, 'a real sample: ' + checked);
  // and an option that cannot be paired keeps every word of the name too
  const odd = O.optionView('How Many Characters & Metal Color?', '1-5 Character-SILVER');
  assert.equal(odd.line, 'How Many Characters & Metal Color: 1-5 Character-SILVER', 'a name with a natural "&" that does not pair is said whole');
  assert.equal(line('METAL - ENGRAVING', 'Rose'), 'Metal - Engraving: Rose', 'and so is "METAL - ENGRAVING" (only its capitals go)');
  assert.equal(line('Metal · Engraving · Size', 'GOLD·ENGRAVED'), 'Metal · Engraving · Size: Gold · Engraved', 'a name fused with · that does not pair with its value keeps every part of the name');
  ok(`nothing is hidden: every word of the ${checked} real values is on its line (only "Set", the shop's font spellings and the typo SILVR change)`);
}

/* ── 5 · display only ── */
{
  const input = { name: 'Metal &middot; #Characters?', value: 'GOLD·Set·ENGRAVED' }, copy = JSON.stringify(input);
  const t = O.tidyOption(input.name, input.value);
  assert.equal(JSON.stringify(input), copy, 'the input is not touched');
  assert.deepEqual(t.raw, { name: 'Metal &middot; #Characters?', value: 'GOLD·Set·ENGRAVED' }, 'the raw name and value stay available exactly as Etsy gave them');
  assert.equal(t.was, 'Metal · #Characters?: GOLD·Set·ENGRAVED', 'and decoded as the hover shows them');
  assert.equal(t.changed, true);
  // the stored / matched side: the one decoder does not learn the entity, the option text, the matching normaliser and the line cleaner read as before
  assert.equal(O.decodeEntities('Metal &middot; x'), 'Metal &middot; x', 'decodeEntities (SKUs, option names, matching) is exactly what it was');
  const line1 = { title: TITLE, variations: [{ name: 'Metal &middot; #Characters?', value: 'GOLD·Set·ENGRAVED' }, { name: 'Necklace Length&middot;FONT', value: '16&quot;&amp;18&quot;·Set·ANGELINA' }] };
  assert.deepEqual(O.purchaseOptions(line1, {}), [{ name: 'Metal &middot; #Characters?', value: 'GOLD·Set·ENGRAVED' }, { name: 'Necklace Length&middot;FONT', value: '16"&18"·Set·ANGELINA' }], 'purchaseOptions (which the matching reads) is what it was');
  assert.equal(O.norm('Metal &middot; #Characters?'), 'metal &middot; #characters?', 'norm (option-map keys) is what it was');
  O.cleanLine(line1);
  assert.deepEqual(line1.variations, [{ name: 'Metal &middot; #Characters?', value: 'GOLD·Set·ENGRAVED' }, { name: 'Necklace Length&middot;FONT', value: '16"&18"·Set·ANGELINA' }], 'cleanLine reads as it always did: the standard entities of a value are decoded, the stored name keeps its &middot;');
  const stored = JSON.stringify(line1);
  O.optionViews(O.purchaseOptions(line1, {}), line1);
  assert.equal(JSON.stringify(line1), stored, 'reading the views changes nothing');
  // optionViews: the two lines of the order, with the title and every option's name
  const views = O.optionViews(O.purchaseOptions(line1, {}), line1);
  assert.deepEqual(views.map(v => v.line), ['Metal: Gold · Engraved', 'Length: 16" & 18" · Font: Angelina']);
  ok('display only: the decoder, purchaseOptions, norm and cleanLine read exactly as before; the input is untouched; `raw` and `was` keep the original');
}

/* ── 6 · safety and the edges ── */
{
  // markup is text: tidyOption answers text, the Engrave box escapes it
  const evil = O.optionView('<img src=x onerror=alert(1)>', '&lt;script&gt;alert(1)&lt;/script&gt;');
  assert.equal(evil.line, '<img src=x onerror=alert(1)>: <script>alert(1)</script>', 'decoded to TEXT');
  const row = { line: { variations: [{ name: '<img src=x onerror=alert(1)> font', value: '&lt;script&gt;alert(1)&lt;/script&gt;' }], personalization: [] }, spec: { options: [] } };
  const html = Said.html(row);
  assert.ok(html.includes('&lt;img src=x onerror=alert(1)&gt;') && html.includes('&lt;script&gt;alert(1)&lt;/script&gt;') && !/<img|<script/.test(html), 'and escaped when drawn: ' + html);
  // the customer's own words are decoded, never tidied
  assert.equal(line('Personalization', 'SILVER &amp; Mom'), 'Personalization: SILVER & Mom');
  assert.equal(line('Custom text', 'GOLD·Set·ENGRAVED'), 'Custom text: GOLD·Set·ENGRAVED', 'a personalisation option is the buyer\'s own text');
  // a placeholder is no choice
  assert.equal(line('Metal · Length', 'Gold · Select an option · 16"'), 'Metal: Gold · Length: 16"');
  assert.equal(O.tidyOption('Size', 'Select an option'), null, 'only a placeholder: nothing to tidy');
  assert.equal(O.optionView('Size', 'Select an option').line, 'Size: Select an option', 'and shown as it is, never hidden');
  assert.equal(O.tidyOption('', ''), null);
  // Rose is a metal only on a metal option
  assert.equal(line('Birth Flower', 'Rose'), 'Birth Flower: Rose');
  assert.equal(line('Metal Choice', 'ROSE'), 'Metal: Rose gold');
  // "Set": dropped when the title says it, kept when nothing says so
  assert.equal(line('Necklace Length', '16"·Set', { title: 'Bar Necklace SET of 2' }), 'Length: 16"');
  assert.equal(line('Necklace Length', '16"·Set', { title: 'Bar Necklace' }), 'Length: 16" · Set');
  assert.equal(line('Pack', 'Set', { title: 'Bar Necklace SET of 2' }), 'Pack: Set', 'a value that is only "Set" stays');
  // a "Necklace Length" is a "Length" unless another option names another noun
  assert.deepEqual(O.optionViews([{ name: 'Necklace Length', value: '16"' }, { name: 'Bracelet Length', value: '7"' }], {}).map(v => v.line), ['Necklace Length: 16"', 'Bracelet Length: 7"']);
  // a count-question label stays when its answer is a number
  assert.equal(line('Metal · #Characters?', 'GOLD·6'), 'Metal: Gold · Characters: 6');
  // entities: named, numeric, hex, double-escaped
  assert.equal(O.shownText('A&nbsp;&middot;&nbsp;B &amp;middot; &#183; &#xB7; &amp;amp;'), 'A · B · · · &');
  assert.equal(O.shownText('16&quot;'), '16"');
  assert.equal(O.shownText('&unknown; &lt;b&gt;'), '&unknown; <b>', 'an entity it does not know stays as written');
  ok('markup is text and escaped when drawn; the buyer\'s own words are never tidied; a placeholder is dropped; Rose is a metal only on a metal option; "Set" follows the title; entities decode to text');
}

/* ── 7 · the Engrave tab's OPTIONS box ── */
{
  const row = { line: { title: TITLE, variations: [{ name: 'Metal &middot; #Characters?', value: 'GOLD·Set·ENGRAVED' }, { name: 'Necklace Length&middot;FONT', value: '16&quot;&amp;18&quot;·Set·ANGELINA' }], personalization: [] }, spec: { options: [] } };
  assert.deepEqual(Said.choicesOf(row).map(c => c.name), ['Metal &middot; #Characters?', 'Necklace Length&middot;FONT'], 'the choices are still the line\'s own (the filter reads them as they are)');
  assert.deepEqual(Said.rowsOf(row), [['Options', 'Metal: Gold · Engraved\nLength: 16" & 18" · Font: Angelina']]);
  const html = Said.html(row);
  assert.ok(html.includes('Metal: Gold · Engraved\nLength: 16&quot; &amp; 18&quot; · Font: Angelina') || html.includes('Metal: Gold · Engraved\nLength: 16" &amp; 18" · Font: Angelina'), html);
  assert.ok(!/&amp;middot;|&middot;|#Characters|ENGRAVED|ANGELINA|\bSet\b/.test(html.replace(/title="[^"]*"/g, '')), 'none of the filler is drawn');
  assert.ok(html.includes('title="As ordered:\nMetal · #Characters?: GOLD·Set·ENGRAVED\nNecklace Length·FONT: 16&quot;&amp;18&quot;·Set·ANGELINA"'), 'the original reads in the label\'s hover (decoded to text, escaped in the attribute): ' + html);
  // an option that needs no tidying: the same box as before, no hover
  const plain = { line: { variations: [{ name: 'Font choice', value: 'Ace' }, { name: 'Charm Type', value: 'Tag1 (front+back)' }], personalization: [] }, spec: { options: [] } };
  assert.deepEqual(Said.rowsOf(plain), [['Options', 'Font: Ace\nCharm Type: Tag1 (front+back)']]);
  const html2 = Said.html({ line: { variations: [{ name: 'Charm Type', value: 'Tag1 (front+back)' }], personalization: [] }, spec: { options: [] } });
  assert.ok(html2.includes('Charm Type: Tag1 (front+back)') && !/title=/.test(html2), 'nothing tidied: no hover');
  ok('the Engrave tab\'s OPTIONS box says the two lines, hides none of the order, keeps the original in the label\'s hover');
}

console.log(`option-tidy: ${n} groups ok`);
