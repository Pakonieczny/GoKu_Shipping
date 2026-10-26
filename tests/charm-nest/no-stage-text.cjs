// "Remove the 2/5 nested text from all of the UI" (Paul, 12 Oct 2026, two pictures: a green "2/5 NESTED" pill on an Orders-list card, and in the order window the
// green "2/5 nested" pill beside Details and the Details fact "STATE  2/5 nested"). The numbered stage text ("1/5 pooled" ... "5/5 complete") is printed nowhere now;
// the data is not touched (Orders.statePill still answers it), the timeline bar and the piece rows' chips still say where the order is, and an exception (held) is not
// a numbered stage and still says so.
// Proved in the REAL page (headless Chromium over the fake site, bridge-server.cjs; made-up buyer names; nothing real is read or written):
//  1 the data: the five stages answer "1/5 pooled", "2/5 nested", "3/5 written", "4/5 labelled", "5/5 complete"; a held line answers "held"
//  2 the Orders list, as cards and as rows, for each stage: no state pill, no N/5 and no stage word on the card; "Qty .. · Ship by .." is the first thing of its
//    block (no gap where the pill was); the held card keeps its pill
//  3 the order window, for each stage: no pill beside Details (only Details), no STATE fact in the Details grid, no N/5 anywhere in the window; the timeline bar
//    and the piece row's chip are still there with their words; a held order keeps its pill, its card and its State fact
//   node tests/charm-nest/no-stage-text.cjs     (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const assert = require('assert/strict');
const W = require('./_orderFactsWorld.cjs');
let n = 0; const ok = (name, c) => { assert(c !== false, name); n++; console.log('  ok ' + name); };

// the fixtures and the state each is put at (the stage text "N/5 <word>" each one answers as data)
const STAGES = [['single', 'pooled', '1/5 pooled'], ['pairMatching', 'nested', '2/5 nested'], ['mismatched', 'written', '3/5 written'], ['qty3', 'labelled', '4/5 labelled'], ['late', 'committed', '5/5 complete'], ['message', 'held', 'held']];
const STAGE_WORDS = /\b(?:[1-5]\/5|pooled|nested|written|labelled|complete)\b/i;

async function open(page, name) {
  await page.evaluate(k => OrderWin.openOrder(k.split('_')[0]), W.keyOf(name));
  await page.waitForFunction(() => { const d = document.getElementById('orderWin'); return d.open && document.querySelector('#owMeta .owfBar') && !d.getAnimations({ subtree: true }).some(a => a.playState === 'running' && a.effect && a.effect.getTiming().iterations !== Infinity); }, null, { timeout: 15000 });
}
const shut = async page => { await page.evaluate(() => { const d = document.getElementById('orderWin'); if (d.open) d.dispatchEvent(new Event('cancel', { cancelable: true })); }); await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 8000 }).catch(() => {}); };

async function main() {
  const w = await W.world({ facts: true });
  if (!w) { console.log('  - no playwright-core: the browser checks were not run'); return; }
  const { page } = w;
  try {
    await W.seed(page, STAGES.map(s => s[0]));
    // each line at its stage (where the pieces are comes from the page's own line state: the cloud's placement answer is left out, as in a page that has not read it)
    const data = await page.evaluate(stages => {
      window.PiecePlacement && (PiecePlacement.ofRow = () => null);
      const out = [];
      for (const [fx, state] of stages) { const row = Orders.rows().find(r => String(r.order.receiptId) === fx.rid); row.state = state; out.push([fx.rid, Orders.statePill(row)[1]]); }
      Orders.render();
      return out;
    }, STAGES.map(([name, state]) => [{ rid: W.FIXTURES[name].rid }, state]));
    assert.deepEqual(data.map(d => d[1]), STAGES.map(s => s[2]));
    ok('1 the data is untouched: Orders.statePill still answers ' + STAGES.map(s => '"' + s[2] + '"').join(', '));

    /* 2 · the Orders list, as cards and as rows */
    for (const view of ['cards', 'list']) {
      await page.evaluate(v => { Orders.view().view = v; CN.setMode('orders'); Orders.render(); }, view);
      await page.waitForFunction(count => document.querySelectorAll('#ordItems [data-rid]:not(button)').length >= count, STAGES.length, { timeout: 15000 });
      const cards = await page.evaluate(() => [...document.querySelectorAll('#ordItems [data-rid]:not(button)')].map(c => {
        const ra = c.querySelector('.rowActions'), facts = ra && ra.querySelector(':scope > .rowFacts'), fr = facts && facts.getBoundingClientRect(), ar = ra && ra.getBoundingClientRect();
        return { rid: c.dataset.rid, text: c.innerText.replace(/\s+/g, ' ').trim(), pills: [...c.querySelectorAll('.ost')].map(p => p.textContent.trim()), facts: facts ? facts.innerText.replace(/\s+/g, ' ').trim() : null, firstIsFacts: !!ra && ra.firstElementChild === facts, shown: !!fr && fr.width > 0 && fr.height > 0, topGap: fr && ar ? fr.top - ar.top : null };
      }));
      assert.equal(cards.length, STAGES.length, view + ': a card for each order');
      for (const [name, state, said] of STAGES) {
        const c = cards.find(x => x.rid === W.FIXTURES[name].rid); assert(c, view + ' ' + state + ': its card');
        assert(c.facts && /^Qty \d+ .*Ship by [A-Z][a-z]{2} \d+$/.test(c.facts), view + ' ' + state + ': the Qty and Ship by line is there: ' + c.facts); assert(c.shown, view + ' ' + state + ': it is drawn');
        if (state === 'held') {
          assert.deepEqual(c.pills, ['held'], view + ': the held card keeps its pill (an exception is not a numbered stage)');
        } else {
          assert.deepEqual(c.pills, [], view + ' ' + state + ': no state pill (it said "' + said + '")');
          assert(!STAGE_WORDS.test(c.text), view + ' ' + state + ': no N/5 and no stage word on the card: ' + c.text);
          assert(c.firstIsFacts, view + ' ' + state + ': the Qty and Ship by line is the first thing of its block, no gap where the pill was');
          if (view === 'cards') assert(c.topGap <= 14, view + ' ' + state + ': ... and sits at the top of it (' + c.topGap + ' px)');
        }
      }
      ok('2' + (view === 'cards' ? 'a' : 'b') + ' the Orders ' + (view === 'cards' ? 'cards' : 'rows') + ': no pill, no N/5, no stage word for pooled, nested, written, labelled and complete; Qty and Ship by stay put; the held ' + (view === 'cards' ? 'card' : 'row') + ' keeps its pill');
    }

    /* 3 · the order window, stage by stage */
    for (const [name, state, said] of STAGES) {
      await open(page, name);
      const r = await page.evaluate(() => {
        const host = document.getElementById('owMeta'), end = host.querySelector('.owfEnd'), win = document.getElementById('orderWin');
        const pcs = [...win.querySelectorAll('.owPcRow .st')].map(x => x.textContent.replace(/\s+/g, ' ').trim()), dots = win.querySelectorAll('.owPcRow .steps [data-pdot]').length;
        return {
          chips: [...host.querySelectorAll('.owfBar [data-owf-chip]')].map(c => c.dataset.owfChip), pill: [...host.querySelectorAll('.owfState')].map(c => c.textContent.trim()), end: end ? [...end.children].map(c => c.className.split(' ')[0]) : null, detailsInRow: !!host.querySelector('.owfCore > .owfTail > .owfMore'),
          meta: host.innerText.replace(/\s+/g, ' ').trim(), win: win.innerText.replace(/\s+/g, ' ').trim(),
          stops: [...win.querySelectorAll('#owRail .tlStop')].map(s => s.textContent.replace(/\s+/g, ' ').trim()), pcs, dots
        };
      });
      await page.click('#owMeta .owfMore'); await page.waitForTimeout(260);
      const det = await page.evaluate(() => { const d = document.querySelector('#owMeta .owfDetails'); return { shown: !d.hidden, labels: [...d.querySelectorAll('.m i')].map(i => i.textContent), cells: [...d.querySelectorAll('.m')].map(m => m.innerText.replace(/\s+/g, ' ').trim()), bare: [...d.querySelectorAll('.m')].filter(m => m.innerText.replace(/\s+/g, ' ').trim().length <= m.querySelector('i').textContent.length).map(m => m.querySelector('i').textContent) }; });
      assert(det.shown, state + ': Details opens');
      if (state === 'held') {
        assert(r.chips.includes('state') && r.pill.length === 1 && /held/.test(r.pill[0]), state + ': the held order keeps its pill: ' + JSON.stringify(r.pill));
        assert(det.labels.includes('State') && det.cells.some(c => /^State held$/i.test(c)), state + ': ... and its State fact: ' + JSON.stringify(det.cells));
      } else {
        assert(!r.chips.includes('state') && r.pill.length === 0, state + ': no pill beside Details (it said "' + said + '")');
        // (Details sits in the first row, right after the metal chip: DETAILSPOS, 12 Oct 2026; with no pill there is nothing at the end of the line)
        assert.equal(r.end, null, state + ': nothing at the end of the line (no pill): ' + JSON.stringify(r.end));
        assert(r.detailsInRow, state + ': Details is in the first row, after the metal chip');
        assert(!det.labels.includes('State'), state + ': no STATE fact in the Details grid: ' + det.labels.join(','));
        assert(det.labels.includes('Ship by') && det.labels.includes('Order') && det.labels.includes('Metal'), state + ': the rest of the grid is there, closed up: ' + det.labels.join(','));
        assert.deepEqual(det.bare, [], state + ': no cell is left empty');
        assert(!/\b[1-5]\/5\b/.test(r.win), state + ': no N/5 anywhere in the window: ' + (r.win.match(/.{20}\b[1-5]\/5\b.{20}/) || [])[0]);
        assert(!STAGE_WORDS.test(r.meta + ' ' + det.cells.join(' ')), state + ': no stage word in the facts or the grid: ' + r.meta);
      }
      assert(r.stops.length >= 3 && r.stops.every(s => s.length > 0), state + ': the timeline bar still shows its steps: ' + JSON.stringify(r.stops));
      assert(r.pcs.length >= 1 && r.pcs.every(s => s.length > 0) && r.dots >= 5, state + ': the piece row still says its step (its status words and its step dots): ' + JSON.stringify(r.pcs) + ' ' + r.dots + ' dots');
      await page.click('#owMeta .owfMore'); await page.waitForTimeout(150);
      await shut(page);
    }
    ok('3 the order window at pooled, nested, written, labelled and complete: no pill beside Details, no STATE fact, no N/5; the timeline bar and the piece row (status, step dots) still show; the held order keeps its pill and fact');

    assert.deepEqual(w.errors.filter(e => !/net::|Failed to load resource|favicon/i.test(e)), [], 'no page error');
    assert.deepEqual(w.warns, [], 'no order view warning');
    ok('4 no page error, no order view warning');
  } finally { await w.close(); }
  console.log('PASS: ' + n + ' checks');
}
main().catch(e => { console.error(e); process.exit(1); });
