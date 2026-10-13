// SETBAR-C in the REAL page (Paul, 13 Oct 2026, order 4202291826, listing 532401548 "Bar Necklace SET of 2"): what the buyer typed
//     16"= 33*0'45" N 96*45'53" W (the asterisk is supposed to be the little degree circle-I just don't have that on my keyboard) 18"= Ginger
// is the 16" necklace's coordinates (every * a degree sign, the bracket is a note to the shop) and the 18" necklace's name. This builds the real sorter page (charm-nest-1.html) over the repo's offline fake site, with the
// REAL drawings, the REAL intake / pool / classifier / fit and the real installed fonts (tests/charm-nest/_engraveRealWorld.cjs), and checks what the classifier does with such words. The set listing's two bar pieces are
// SETBAR-A's work, so a line of TWO DISCS stands in for the two pieces here (D1, D2: the same slots, the same classifier); the page's hook (CharmNestEngraveRead.slotInfo) is what SETBAR-A's bar-and-chain mapping plugs into.
// Offline: no Etsy, no Firebase, no paid AI (the reader is a stub that COUNTS its calls: the rules must not need it), nothing real written, every name made up.
//   PW_DIR=/opt/node-tools/node_modules node tests/charm-nest/engrave-read-world.cjs
// What a pass does NOT prove: the live order (the live listing is not here), Etsy's real option text beyond the values the brief gives, a browser other than headless Chromium.
'use strict';
const R = require('./_engraveRealWorld.cjs');
let pass = 0, fail = 0;
const ok = (c, m, extra) => { if (c) { pass++; console.log('  PASS ' + m); } else { fail++; console.log('  FAIL ' + m + (extra !== undefined ? ' :: ' + (typeof extra === 'string' ? extra : JSON.stringify(extra)) : '')); } };
const V = (name, value) => ({ name, value });
const BUYER = `16"= 33*0'45" N 96*45'53" W (the asterisk is supposed to be the little degree circle-I just don't have that on my keyboard) 18"= Ginger`;
const COORDS = `33°0'45" N 96°45'53" W`;
const SET = [V('Metal · #Characters?', 'GOLD·Set·ENGRAVED'), V('Necklace Length·FONT', '16"&18"·Set·ANGELINA'), V('Number of Discs', '2 Discs')];
const SETBLANK = [V('Metal · #Characters?', 'GOLD·Set·BLANK'), V('Necklace Length·FONT', '16"&18"·Set·ANGELINA'), V('Number of Discs', '2 Discs')];
const mk = (rid, o) => Object.assign({ rid, sku: 'COMET_9427', listing: '18' + rid.slice(-6), title: 'Bar Necklace SET of 2, Gold, Custom Gold Bar Set, Engraved Necklace', buyer: 'Name One', placed: { day: 0, h: 9, m: 10 }, ship: 5, variations: SET, personalization: [BUYER] }, o);
const ORD = {
  exact: mk('4290009001', {}),
  entities: mk('4290009002', { personalization: ['16&quot;= Mom&#39;s &amp; Dad 18&quot;= Ginger&#39;s'] }),
  oneLen: mk('4290009003', { personalization: ['16"= Ginger'] }),
  names: mk('4290009004', { personalization: ['Ginger'] }),
  blankWords: mk('4290009005', { variations: SETBLANK }),
  blankNote: mk('4290009006', { variations: SETBLANK, personalization: [], buyerMessage: 'Hi, could you please engrave Ginger on the bar?' }),
  engravedNone: mk('4290009007', { personalization: [] }),
  layout: mk('4290009008', { personalization: ['16"= Anna 18"= Ben. Please swap the bars so the thin one is on the 18"'] }),
  leftRight: mk('4290009009', { personalization: ['Left: Anna Right: Ben'] }),
  // one necklace, Angelina (Mynerve), coordinates with asterisks and a note: the words are cleaned, a person confirms, the REAL fit draws the degree signs
  angelina: { rid: '4290009010', sku: 'COMET_9427', listing: '1890009010', title: 'Coordinates Necklace', buyer: 'Name Two', placed: { day: 0, h: 9, m: 20 }, ship: 5, variations: [V('Metal choice', 'Gold'), V('Font Choice', 'Angelina')], personalization: [`33*0'45" N 96*45'53" W (I don't have the degree sign on my keyboard)`] }
};
Object.assign(R.ORDERS, ORD);
Object.assign(R.EXPECT_JOBS, { exact: 2, entities: 2, oneLen: 2, names: 2, blankWords: 2, blankNote: 2, engravedNone: 2, layout: 2, leftRight: 2, angelina: 1 });
const NAMES = Object.keys(ORD);
const flat = x => String(x == null ? '' : x).replace(/\s+/g, ' ').trim();   // (the fit lays the words out on the bar: the same words on two lines are the same words)
const said = j => [j.reason].concat(j.questions || []).filter(Boolean).join(' | ');   // (the reason line is gone once the preview is drawn: the reading stays on the card as its note)
const waits = j => ['words', 'review'].includes(j.state) && !j.approvedAt;   // ("Words to confirm", or its preview drawn and waiting for Approve: either way a person has not approved)

(async () => {
  const w = await R.world({ viewport: { width: 1440, height: 900 } });
  if (!w) { console.log('SKIP: no playwright-core'); return; }
  try {
    const asked = [];
    w.srv.st.agentResults.engraveIntent = o => {
      const txt = String(o.messages[0].content[0].text); asked.push(txt);
      const m = /Personalisation field: (\[.*?\])/.exec(txt); let p = []; try { p = JSON.parse(m ? m[1] : '[]'); } catch (_) {}
      const req = { side: 'back', font: null, handwriting: false, image: false };
      return /slots/i.test(txt) ? { engrave: true, text: p.join('\n'), source: 'personalization', sourceQuote: p[0] || '', requests: req, questions: [], confidence: 0.97, pieces: [{ slot: 'D1', text: p[0] || '' }, { slot: 'D2', text: p[0] || '' }] }
        : { engrave: true, text: p.join('\n'), source: 'personalization', sourceQuote: p[0] || '', requests: req, questions: [], confidence: 0.97 };
    };
    // the hook SETBAR-A's mapping plugs into: the thinner bar (D1) hangs on the shorter chain (Paul, 13 Oct). Set for the first pass; cleared for the second.
    await w.page.evaluate(() => { if (window.CharmNestEngraveRead) CharmNestEngraveRead.slotInfo = (row, slot) => ({ rank: slot === 'D1' ? 0 : 1 }); });
    const made = await R.build(w, NAMES);
    const job = (n, slot) => (made[n].jobs.find(j => (j.slot || null) === slot) || {});
    for (const n of NAMES) ok(made[n] && made[n].jobs.length === R.EXPECT_JOBS[n], `real world: "${n}" has its ${R.EXPECT_JOBS[n]} Engrave job(s)`, made[n] && made[n].jobs.map(j => j.slot));

    console.log('the exact buyer text, the hook set (the thinner bar on the shorter chain)');
    ok(flat(job('exact', 'D1').text) === COORDS, 'the 16" piece: the coordinates with real degree signs', job('exact', 'D1').text);
    ok(flat(job('exact', 'D2').text) === 'Ginger', 'the 18" piece: Ginger', job('exact', 'D2').text);
    ok(['D1', 'D2'].every(s => waits(job('exact', s))), 'both wait for a person (Words to confirm, or the preview waiting for Approve): nothing is approved, nothing is cut', made.exact.jobs.map(j => [j.state, j.approvedAt]));
    ok(!made.exact.jobs.some(j => /\*/.test(j.text)) && !/Ginger/.test(job('exact', 'D1').text) && !/33/.test(job('exact', 'D2').text), 'no asterisk, and neither piece carries the other\'s words');
    ok(/left out as a note to the shop/.test(said(job('exact', 'D1'))) && /2 asterisks read as the degree sign/.test(said(job('exact', 'D1'))), 'the card says what it left out and what it fixed', said(job('exact', 'D1')));
    ok(!asked.some(t => /Order 4290009001/.test(t)), 'the paid reader was not asked');

    console.log('entities, one length, Left / Right, names only');
    ok(flat(job('entities', 'D1').text) === "Mom's & Dad" && flat(job('entities', 'D2').text) === "Ginger's", 'entities read as the characters', [job('entities', 'D1').text, job('entities', 'D2').text]);
    ok(flat(job('oneLen', 'D1').text) === 'Ginger' && job('oneLen', 'D2').text === '' && /words for 16" only/.test(said(job('oneLen', 'D2'))), 'one length: the 16" piece has the words, the 18" piece is asked, never given them', [job('oneLen', 'D1').text, job('oneLen', 'D2').text, said(job('oneLen', 'D2'))]);
    ok(job('leftRight', 'D1').text === '' && job('leftRight', 'D2').text === '' && /Left/.test(said(job('leftRight', 'D1'))) && /Anna/.test(said(job('leftRight', 'D1'))), 'Left / Right on a set: no bar is "left", so both wordings wait beside the card for a person', said(job('leftRight', 'D1')));
    ok(asked.some(t => /Order 4290009004/.test(t)) && !asked.some(t => /Order 4290009003/.test(t)), 'names only still goes to the reader exactly as before (and only that one did)');
    ok(job('names', 'D1').text === 'Ginger' && job('names', 'D2').text === 'Ginger' && ['review', 'ready', 'fitting', 'blocked'].includes(job('names', 'D1').state), 'names only: the usual path (the reader\'s answer goes to the fit)', made.names.jobs.map(j => [j.state, j.text]));

    console.log('to engrave or not');
    ok(waits(job('blankWords', 'D1')) && job('blankWords', 'D1').text === '' && /blank \(no engraving\), but the buyer wrote words/i.test(said(job('blankWords', 'D1'))) && /33°0'45"/.test(said(job('blankWords', 'D1'))), 'BLANK with words: nothing engraved, a person decides, the reason says BLANK and shows the reading', said(job('blankWords', 'D1')));
    ok(waits(job('blankNote', 'D1')) && job('blankNote', 'D1').text === '' && /BLANK \(no engraving\), but a note asks for engraving/.test(said(job('blankNote', 'D1'))), 'BLANK with a note that asks for engraving: waits for a person', said(job('blankNote', 'D1')));
    ok(!asked.some(t => /Order 4290009006/.test(t)), 'and no paid reading was bought for it');
    ok(waits(job('engravedNone', 'D1')) && /ENGRAVED\) but wrote no words/.test(said(job('engravedNone', 'D1'))), 'ENGRAVED with nothing written: asked, not cut plain in silence', said(job('engravedNone', 'D1')));
    ok(waits(job('layout', 'D1')) && job('layout', 'D1').text === '' && /different layout/.test(said(job('layout', 'D1'))) && /16" → Anna · 18" → Ben/.test(said(job('layout', 'D1'))), 'a layout request: nothing assigned, a person decides, the wordings are on the card', said(job('layout', 'D1')));

    console.log('the hook cleared (until SETBAR-A\'s mapping is plugged in)');
    const again = await w.page.evaluate(async () => {
      if (window.CharmNestEngraveRead) CharmNestEngraveRead.slotInfo = null;
      const row = Orders.rows().find(r => r.order.receiptId === '4290009001'); const js = Engrave.jobsOf(row);
      for (const j of js) { j.state = 'classify'; j.text = null; j.lines = []; j.decision = null; }
      await Engrave.classify(row);
      return Engrave.jobsOf(row).map(j => ({ slot: j.slot, state: j.state, text: j.text, reason: j.reason }));
    });
    ok(again.every(j => j.state === 'words' && j.text === '' && /Say which words go on this piece/.test(j.reason) && j.reason.includes(COORDS) && /“18"” as “Ginger”/.test(j.reason)), 'neither piece is given a wording; both wordings, fixed, are on each card', again);

    console.log('one necklace, Angelina: cleaned, confirmed by a person, drawn by the real fit');
    const a = job('angelina', null);
    ok(waits(a) && flat(a.text) === COORDS && /confirm the words/.test(said(a)), 'the note is left out, the asterisks are degree signs, the words wait to be confirmed', a);
    const fit = await w.page.evaluate(async key => {
      const j = Engrave.items().get(key); await Engrave.decideWords(j, { text: j.text, by: 'Test Operator' });
      for (let i = 0; i < 60 && !(j.fit && (j.fit.glyphs || j.fit.cmds)) && j.state !== 'blocked'; i++) { try { await Engrave.fitAll(); } catch (_) {} await new Promise(r => setTimeout(r, 300)); }
      return { state: j.state, reason: j.reason, fontKey: j.fontKey || null, text: j.text, lines: j.lines, fitted: !!(j.fit && ((j.fit.glyphs && j.fit.glyphs.length) || j.fit.cmds)), missing: j.missing || null };
    }, a.key);
    ok(fit.fitted && flat(fit.text) === COORDS && fit.fontKey === 'mynerve' && !fit.missing && !/Unsupported/.test(fit.reason || ''), 'the real fit draws the degree signs in Mynerve (Angelina)', fit);

    ok(w.errors.length === 0, 'no page error', w.errors);
  } finally { await w.close(); }
  console.log(fail ? `\nFAILED: ${pass} passed, ${fail} failed` : `\nPASSED: ${pass} passed, 0 failed`);
  process.exitCode = fail ? 1 : 0;
})().catch(e => { console.error(e); process.exit(1); });
