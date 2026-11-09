// TW1 (Paul, 9 Nov 2026: "The title of the charm listing should not be bold ... Only personalization and things that actually matter should be bold. If there's no personalization, don't say the word
// "none" or "no personalization"."). The REAL page over the repo's fake site (no Etsy, nothing live, nothing written), with three made-up orders:
//   plain   a long listing title, the personalisation box holding Etsy's own filler ("Not requested on this item.")
//   pers    the buyer wrote words to engrave
//   none    the buyer wrote "none" in the box
// What it proves:
//   1  an Orders row with no personalisation says no placeholder ("none", "no personalisation", "Not requested", "No title", "wrote nothing") and its listing title is regular weight, muted, its whole title its hover
//   2  a row with personalisation shows the buyer's words, bold
//   3  the order window of an order whose buyer wrote nothing has no "What the customer wrote" block (and no "— the customer wrote nothing —"); one with words shows the block, the words bold
//   4  the order number stays bold; the purchase line (type · options) is not bold
//   node tests/charm-nest/orders-title-regular.cjs
'use strict';
const { world, seed, FIXTURES } = require('./_orderFactsWorld.cjs');
const fails = []; let nCheck = 0;
const check = (ok, msg) => { nCheck++; if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };

const TITLE = 'Jupiter Charm Planet Pendant Add On Charm Space Jewelry Celestial Charm Jupiter Jewelry Planet Charm for Space Lover';
const mk = (rid, sku, buyer, h, pers) => ({ rid, sku, title: TITLE, buyer, placed: { day: 0, h, m: 30 }, ship: 7, personalization: pers, variations: [{ name: 'Metal choice', value: 'Gold' }] });
const ID = { plain: '4221578379', pers: '4221578380', none: '4221578381' };
FIXTURES.twPlain = mk(ID.plain, 'SPACE1-JUPITER', 'Plain Buyer', 1, ['Not requested on this item.']);
FIXTURES.twPers = mk(ID.pers, 'SPACE1-JUPITER', 'Words Buyer', 2, ['Ava & Leo']);
FIXTURES.twNone = mk(ID.none, 'SPACE1-JUPITER', 'None Buyer', 3, ['none']);
const PLACEHOLDER = /\bnone\b|no personali[sz]ation|not requested|no title|wrote nothing|no engraving|no message|no note/i;

(async () => {
  const w = await world({ viewport: { width: 1500, height: 900 }, facts: false });
  if (!w) { console.log('SKIP: no browser (set PW_DIR and CHROMIUM)'); return; }
  const { page } = w;
  const sleep = ms => page.waitForTimeout(ms);
  try {
    await seed(page, ['twPlain', 'twPers', 'twNone']);
    await sleep(900);
    const rowOf = rid => page.evaluate(rid => {
      const n = document.querySelector(`#ordItems [data-rid="${rid}"]`); if (!n) return null;
      const wt = el => el ? +getComputedStyle(el).fontWeight : 0;
      const id = n.querySelector('.engravingIdentity'), title = n.querySelector('.rowExcerpt.ordTitle'), said = n.querySelector('.rowExcerpt.ordSaidText') || n.querySelector('.ordEngId .w');
      const bold = [...n.querySelectorAll('.engravingIdentity *, .purchaseSummary *')].filter(e => e.offsetParent && e.childElementCount === 0 && e.textContent.trim() && +getComputedStyle(e).fontWeight >= 600).map(e => e.textContent.trim());
      return { text: id ? id.innerText : '', sum: n.querySelector('.purchaseSummary')?.innerText || '', title: title ? { text: title.textContent, hover: title.title, weight: wt(title), color: getComputedStyle(title).color, clamp: getComputedStyle(title).webkitLineClamp } : null,
        said: said ? { text: said.textContent, weight: wt(said) } : null, num: wt(n.querySelector('.onum')), bold, ink: getComputedStyle(document.body).getPropertyValue('--ink').trim() };
    }, rid);

    // ═══ 1 · a row with no personalisation ═══
    for (const k of ['plain', 'none']) {
      const r = await rowOf(ID[k]);
      check(!!r, `${k}: the order has its row in the Orders list`);
      if (!r) continue;
      check(!PLACEHOLDER.test(r.text) && !PLACEHOLDER.test(r.sum), `${k}: the row says no placeholder for the missing personalisation: "${r.text.replace(/\s+/g, ' ')}" · "${r.sum.replace(/\s+/g, ' ')}"`);
      check(!!r.title && r.title.text === TITLE && r.title.hover === TITLE, `${k}: the listing title is the row's line, the whole title its hover`);
      check(!!r.title && r.title.weight < 600, `${k}: the listing title is NOT bold (weight ${r.title && r.title.weight})`);
      check(!!r.title && String(r.title.clamp) === '2', `${k}: the title is two lines at most (line clamp ${r.title && r.title.clamp})`);
      check(!r.said, `${k}: no buyer's words are drawn`);
      check(r.num >= 600, `${k}: the order number stays bold`);
      check(r.bold.every(t => t.replace(/\D/g, '').startsWith(ID[k].slice(0, 6)) || /^\d/.test(t)), `${k}: nothing else in the row is bold (bold: ${JSON.stringify(r.bold)})`);
    }

    // ═══ 2 · a row with personalisation ═══
    {
      const r = await rowOf(ID.pers);
      check(!!r && !!r.said && /Ava & Leo/.test(r.said.text), 'pers: the buyer\'s words are in the row: ' + (r && r.said && r.said.text));
      check(!!r && !!r.said && r.said.weight >= 600, `pers: the buyer's words are bold (weight ${r && r.said && r.said.weight})`);
      check(!!r && !PLACEHOLDER.test(r.text), 'pers: no placeholder in the row');
    }

    // ═══ 3 · the order window ═══
    const win = async rid => {
      await page.evaluate(async rid => { const r = Orders.rows().find(x => String(x.order.receiptId) === rid); await OrderWin.open(r.key); }, rid);
      await sleep(1200);
      return page.evaluate(() => { const n = document.getElementById('owNotes'), f = n && n.closest('.owField'), b = n && n.querySelector('.owSaidT');
        const vis = !!(f && !f.hidden && f.getClientRects().length);
        return { vis, text: n ? n.textContent : '', words: b ? { text: b.textContent, weight: +getComputedStyle(b).fontWeight } : null, body: (document.querySelector('#orderWin .owInfoMain') || {}).innerText || '' }; });
    };
    const close = async () => { await page.evaluate(() => { const d = document.getElementById('orderWin'); if (d && d.open) d.close(); }); await sleep(600); };
    {
      const o = await win(ID.plain);
      check(!o.vis, 'plain: the order window has no "What the customer wrote" block when the buyer wrote nothing');
      check(!/wrote nothing|No notes|not requested/i.test(o.text) && !/wrote nothing|— No notes —/i.test(o.body), 'plain: no "— the customer wrote nothing —" / "— No notes —" / Etsy filler anywhere in the window\'s overview');
      await close();
    }
    {
      const o = await win(ID.none);
      check(!o.vis, 'none: "none" in the personalisation box is no words: no "What the customer wrote" block');
      await close();
    }
    {
      const o = await win(ID.pers);
      check(o.vis && /Ava & Leo/.test(o.text), 'pers: the order window shows "What the customer wrote" with the buyer\'s words');
      check(!!o.words && o.words.weight >= 600, `pers: the buyer's words in the window are bold (weight ${o.words && o.words.weight})`);
      await close();
    }
    check(!w.errors.length, 'no page error: ' + w.errors.slice(0, 2).join(' | '));
  } catch (e) { fails.push('threw: ' + (e && e.stack || e)); console.error(e); }
  finally { await w.close(); }
  console.log(`\n${nCheck - fails.length}/${nCheck} checks passed`);
  if (fails.length) { console.log('FAILED:\n  ' + fails.join('\n  ')); process.exit(1); }
})();
