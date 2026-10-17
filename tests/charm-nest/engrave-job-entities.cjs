// ENGENT (Paul's brief 13 Oct 2026, after ENGTEXT found it): Etsy hands the customer's words over HTML-escaped ("Name One&#39;s", "Name One &amp; Date One"). The order window's words block already read them decoded (that block under the cards is gone since NOSAID, 17 Oct 2026; the Overview's "What the customer wrote" box reads them decoded), but the
// ENGRAVING JOB's own words (the card's face words and title, the picture, the fit, job.text, the laser file) carried the raw entity, and a front + back job in a capitals font even upper-cased "&amp;" to "&AMP;". A buyer who
// writes "Mom's" or "Mom & Dad" must get exactly that on the card, in the picture and on the laser job.
// This test builds the REAL page over the REAL-drawing world (tests/charm-nest/_engraveRealWorld.cjs: real .ai drawings, real intake / pool / classifier / fit, offline fake site, no Etsy, no paid AI, nothing real written, every name made up)
// with orders whose personalisation carries the entities, and checks:
//   new     a back job, a front job, a front + back job (Ace: capitals) and a quote / "<" job: spec, job.text / lines / quote, the fit's lines, row.engrave.text, the order window's card (words, title attribute, picture), the Engrave
//           tab's card, words box and picture all read the real characters; no entity, no "&AMP;"; what is stored (the order's own text) is untouched; the reader was asked with the real characters (no paid call: a stub answers)
//   fonts   every installed engraving font draws ' & " < > (glyph present, or the page's own "Unsupported engraving characters" holds the job)
//   stored  a job saved WITH the raw entity (its fit drawn from it, as before the fix) comes back from the saved workspace (Session capture, reload, restore) reading decoded and fitted again from the real characters; an
//           APPROVED job saved the same way is left exactly as it was (words, fit, approval); the order's stored text is not rewritten
//   boxes   the Overview's "What the customer wrote" box (#owNotes) and the Engrave tab's box (CharmNestEngraveSaid) print the real characters; markup in a note stays text
// Run:  PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/engrave-job-entities.cjs
// What a pass does NOT prove: a live order (the fake site has the real drawings and fonts but not the live data), a browser other than headless Chromium, or the laser file itself (the fit and the job words are checked, no Approve is pressed).
'use strict';
const R = require('./_engraveRealWorld.cjs');
const Base = require('./_orderFactsWorld.cjs');
// (the world's order has no staff note: this test's orders may carry one, `staffNote` on the fixture; the function is evaluated in the page, so it is rebuilt from the world's own source)
Base.orderOf = eval('(' + Base.orderOf.toString().replace("staffNote: ''", "staffNote: fx.staffNote || ''") + ')');
let pass = 0, fail = 0;
const ok = (c, m, extra) => { if (c) { pass++; console.log('  PASS ' + m); } else { fail++; console.log('  FAIL ' + m + (extra !== undefined ? ' :: ' + (typeof extra === 'string' ? extra : JSON.stringify(extra)) : '')); } };

// the made-up words, as Etsy hands them over
const ENT = 'Name One&#39;s &amp; Date One';
Object.assign(R.ORDERS, {
  entBack: Object.assign({}, R.ORDERS.back, { rid: '4290000301', buyer: 'Name Nine', personalization: [ENT] }),
  entFront: Object.assign({}, R.ORDERS.front, { rid: '4290000302', buyer: 'Name Ten', personalization: [ENT] }),
  entBoth: Object.assign({}, R.ORDERS.both, { rid: '4290000303', buyer: 'Name Eleven', personalization: ['front - Name One&#39;s\nback - Name Two &amp; Date One'] }),
  entQuote: Object.assign({}, R.ORDERS.back, { rid: '4290000304', buyer: 'Name Twelve', personalization: ['&quot;Date One&quot; &lt;3'], buyerMessage: 'Hello &lt;b&gt;bold&lt;/b&gt;, it&#x27;s Name Two &amp;amp; Name One', staffNote: 'Mind Name One&amp;#39;s font' }),
});
Object.assign(R.EXPECT_JOBS, { entBack: 1, entFront: 1, entBoth: 2, entQuote: 1 });
const NAMES = ['entBack', 'entFront', 'entBoth', 'entQuote'];
const WANT = {   // what each job must read, words joined with single spaces
  entBack: { back: "Name One's & Date One" },
  entFront: { front: "Name One's & Date One" },
  entBoth: { front: "NAME ONE'S", back: 'NAME TWO & DATE ONE' },
  entQuote: { back: '"Date One" <3' },
};
const ENTITY = /&(?:#\d+|#x[0-9a-f]+|amp|quot|lt|gt|apos);/i;
const flat = s => String(s == null ? '' : s).replace(/\s+/g, ' ').trim();
const asked = [];   // what the (stub) reader was asked

/** the order's lines, spec and jobs as the page holds them */
const readOrder = (w, rid) => w.page.evaluate(rid => {
  const rows = Orders.rows().filter(r => r.order.receiptId === rid);
  const jobs = [...Engrave.items().values()].filter(j => rows.includes(j.row) || (j.row && j.row.parentRow && rows.includes(j.row.parentRow)));
  const fl = f => f && f.lines ? f.lines.slice() : null;
  return {
    spec: rows.map(r => ({ p: r.spec.personalization, b: r.spec.buyerMessage, s: r.spec.staffNote })),
    stored: rows.map(r => ({ p: r.line.personalization, b: r.order.buyerMessage, s: r.order.staffNote })),
    eng: rows.map(r => r.engrave && r.engrave.text),
    jobs: jobs.map(j => ({ key: j.key, face: j.face === 'front' ? 'front' : 'back', state: j.state, text: j.text, lines: (j.lines || []).slice(), lineInput: j.lineInput ? j.lineInput.slice() : null, quote: j.quote, fit: fl(j.fit), fitted: !!(j.fit && (j.fit.glyphs || j.fit.cmds)), fontKey: j.fontKey || null, approvedAt: j.approvedAt || 0, reason: j.reason || null })),
  };
}, rid);
const jobOf = (o, face) => o.jobs.find(j => j.face === face);
const noEntity = x => !ENTITY.test(JSON.stringify(x)) && !/&(?:AMP|QUOT|LT|GT|APOS);/.test(JSON.stringify(x));

(async () => {
  const w = await R.world({ viewport: { width: 1440, height: 900 }, shots: '/tmp/engent-shots' });
  if (!w) { console.log('SKIP: no playwright-core'); return; }
  try {
    // the paid reader is a stub (offline): it answers with the personalisation it was asked about, and records what it was asked
    w.srv.st.agentResults.engraveIntent = o => {
      const txt = String(o.messages[0].content[0].text), m = /Personalisation field: (\[.*?\])/.exec(txt); let p = []; try { p = JSON.parse(m ? m[1] : '[]'); } catch (_) {}
      asked.push(txt);
      const req = { side: 'back', font: null, handwriting: false, image: false };
      if (p.length) return { engrave: true, text: p.join('\n'), source: 'personalization', sourceQuote: p[0], requests: req, questions: [], confidence: 0.97 };
      return { engrave: false, text: '', source: 'none', sourceQuote: '', requests: req, questions: [], confidence: 0.9 };
    };
    const made = await R.build(w, NAMES);
    for (const n of NAMES) ok(made[n] && made[n].jobs.length === R.EXPECT_JOBS[n], `real world: order "${n}" has its ${R.EXPECT_JOBS[n]} Engrave job(s) (real drawing, real fit)`, made[n] && made[n].jobs.map(j => j.key));

    // ═══ new orders: the job reads the real characters ═══
    console.log('new orders (the entity comes in with the order)');
    const live = {};
    for (const n of NAMES) {
      const o = live[n] = await readOrder(w, R.ORDERS[n].rid);
      const tag = n;
      ok(o.spec.every(s => noEntity(s)) && o.spec.length >= 1, `${tag}: the line's reading (spec) holds the real characters`, o.spec);
      for (const [face, want] of Object.entries(WANT[n])) {
        const j = jobOf(o, face);
        ok(!!j && flat(j.text) === want, `${tag}: the ${face} job's words read "${want}"`, j && j.text);
        ok(!!j && flat(j.lines.join(' ')) === want, `${tag}: and its lines`, j && j.lines);
        ok(!!j && j.fitted && flat((j.fit || []).join(' ')) === want, `${tag}: the ${face} fit (what is drawn and cut) is laid out from the real characters`, j && j.fit);
        ok(!!j && noEntity([j.text, j.lines, j.quote, j.fit]), `${tag}: no entity (and no "&AMP;") in the ${face} job's text, lines, quote or fit`, j);
      }
      ok(noEntity(o.eng), `${tag}: the line's engraving summary holds the real characters`, o.eng);
      ok(JSON.stringify(o.stored[0].p) === JSON.stringify(R.ORDERS[n].personalization), `${tag}: what is stored (the order's own text) is untouched`, o.stored[0].p);
    }
    ok(live.entBoth.jobs.length === 2 && !live.entBoth.jobs.some(j => /AMP/.test(j.text + j.lines.join())), 'entBoth: the front + back job in capitals (Ace) reads "&", never "&AMP;"', live.entBoth.jobs.map(j => j.text));
    ok(live.entBoth.jobs.every(j => j.fontKey === 'jost-ace'), 'entBoth: it is the capitals font (the case that used to turn "&amp;" into "&AMP;")', live.entBoth.jobs.map(j => j.fontKey));
    // the reader (stubbed: no paid call) was asked with the real characters
    ok(asked.length >= 1 && asked.every(t => !ENTITY.test(t)), `the reader was asked with the real characters, not the entities (${asked.length} stubbed asks)`, asked.map(t => t.slice(0, 200)));
    const q = live.entQuote;
    ok(q.spec[0].b.indexOf('<b>bold</b>') >= 0 && q.spec[0].b.indexOf("it's Name Two & Name One") >= 0 && q.spec[0].s === "Mind Name One's font", 'entQuote: the buyer message and the staff note read decoded too (markup stays text)', q.spec[0]);

    // ═══ the screens ═══
    console.log('the order window and the Engrave tab');
    for (const n of NAMES) {
      await R.openOrderWindow(w, n);
      const win = await w.page.evaluate(() => {
        const cards = [...document.querySelectorAll('#owEng .engCard')];
        return { cards: cards.map(c => flat(c.innerText)), titles: cards.flatMap(c => [...c.querySelectorAll('[title]')].map(x => x.title)), pv: cards.flatMap(c => [...c.querySelectorAll('.engpvText')].map(x => x.textContent)), notes: (document.getElementById('owNotes') || {}).innerText || '', notesHtml: (document.getElementById('owNotes') || {}).innerHTML || '', saidBlocks: document.querySelectorAll('#orderWin #owEngSaid, #orderWin .engSaid, #orderWin .esRow, #orderWin [data-said]').length };
        function flat(s) { return String(s).replace(/\s+/g, ' ').trim(); }
      });
      const wantAll = Object.values(WANT[n]);
      ok(win.cards.length >= 1 && wantAll.every(t => win.cards.join(' ').indexOf(t) >= 0), `${n}: the order window's card(s) print the words as the customer wrote them`, win.cards);
      ok(wantAll.every(t => win.titles.some(x => flat(x).indexOf(t) >= 0)) && wantAll.every(t => flat(win.pv.join(' ')).indexOf(t) >= 0), `${n}: the card's title attribute and the picture carry them too`, { titles: win.titles, pv: win.pv });
      ok(noEntity([win.cards, win.titles, win.pv, win.notes]), `${n}: no entity printed raw in the card, its title, the picture or the Overview's "What the customer wrote"`, win);
      ok(win.saidBlocks === 0, `${n}: NOSAID (Paul, 17 Oct 2026): no second copy of the customer's words under the cards; the Overview's "What the customer wrote" (#owNotes) is where they are`, win.saidBlocks);
      if (n === 'entBack') ok(flat(win.notes) === "PERSONALISATION Name One's & Date One", `${n}: the Overview's "What the customer wrote" (#owNotes) reads "Name One's & Date One"`, win.notes);
      if (n === 'entQuote') ok(flat(win.notes).indexOf('<b>bold</b>') >= 0 && win.notesHtml.indexOf('<b>') < 0, `${n}: markup in the buyer message is shown as text in #owNotes, never drawn`, win.notesHtml);
    }
    for (const [n, face] of [['entBack', 'back'], ['entFront', 'front'], ['entBoth', 'back'], ['entQuote', 'back']]) {
      const key = await R.openEngraveDetail(w, n, { face });
      ok(!!key, `${n}: the Engrave detail opens on the ${face} job`);
      const d = await w.page.evaluate(() => ({
        said: [...document.querySelectorAll('.pvSaid')].map(e => e.innerText.replace(/\s+/g, ' ')).join(' | '), saidHtml: [...document.querySelectorAll('.pvSaid')].map(e => e.innerHTML).join(''),
        words: [...document.querySelectorAll('textarea')].map(e => e.value).filter(Boolean), pv: [...document.querySelectorAll('.engpvText')].map(e => e.textContent), card: [...document.querySelectorAll('.engCard')].map(e => e.innerText.replace(/\s+/g, ' ')),
      }));
      const want = WANT[n][face];
      ok(d.words.some(t => flat(t).toUpperCase() === want.toUpperCase()) && d.pv.some(t => flat(t) === want), `${n}: the words box and the picture show "${want}"`, d);
      ok(noEntity([d.said, d.words, d.pv, d.card]), `${n}: nothing in the Engrave tab (card, words box, picture, "What the customer wrote") prints an entity`, d);
      if (n === 'entBack') ok(d.said.indexOf("Name One's & Date One") >= 0, `${n}: the Engrave tab's box reads "Name One's & Date One"`, d.said);
      if (n === 'entQuote') ok(d.said.indexOf('<b>bold</b>') >= 0 && d.saidHtml.indexOf('<b>') < 0 && d.said.indexOf("Mind Name One's font") >= 0, `${n}: the Engrave tab's box shows the note's markup as text and the staff note decoded`, d.said);
    }

    // ═══ the paid readings keep their keys ═══
    console.log('the paid readings are keyed by the words as Etsy gave them');
    const pr = await w.page.evaluate(() => {
      const O = CharmNestOrders, r = Orders.rows().find(x => x.order.receiptId === '4290000301'), raw = O.rawWords(r.order, r.line);
      const line = Object.assign({}, r.line, { personalization: ['Name One', 'Date One'] }), order = Object.assign({}, r.order, { buyerMessage: 'Hello', staffNote: 'Note' });
      const sp = O.interpretLine(order, line, Orders.ctx()), rw = O.rawWords(order, line);
      return { raw, specP: r.spec.personalization, same: JSON.stringify([sp.personalization, sp.buyerMessage, sp.staffNote, sp.messages]) === JSON.stringify([rw.personalization, rw.buyerMessage, rw.staffNote, rw.messages]), key: ([...Engrave.items().values()].find(j => j.key === '4290000301_42900003011') || {}).claudeJob };
    });
    ok(pr.raw.personalization[0] === ENT && pr.specP[0] === "Name One's & Date One", 'rawWords answers the words as Etsy gave them (what the custom-order reader\'s payload hash and the engraving reader\'s ask key are made from), while the spec reads them decoded', pr);
    ok(pr.same === true, 'for words with no entity rawWords is exactly what the line reader takes (no drift: the keys of readings already paid for stay the same)');
    ok(!!(pr.key && pr.key.id && pr.key.ask), 'the engraving reader\'s job is remembered under its key (a reload takes it up again instead of asking again)', pr.key);

    // ═══ fonts ═══
    console.log('every engraving font draws the characters');
    const cov = await w.page.evaluate(async () => {
      const res = {};
      for (const f of CharmNestOrders.ENGRAVING_FONTS) for (const [wt, p] of Object.entries(f.files)) {
        try { const r = await fetch(p); if (!r.ok) { res[f.id + ':' + wt] = 'HTTP ' + r.status; continue; } const font = opentype.parse(await r.arrayBuffer()); const c = CharmNestGeom.glyphCoverage(font, 'Name One\'s & Date One "<>3'); res[f.id + ':' + wt] = c.ok ? 'ok' : 'missing ' + JSON.stringify(c.missing); }
        catch (e) { res[f.id + ':' + wt] = 'ERR ' + e.message; }
      }
      return res;
    });
    const bad = Object.entries(cov).filter(([, v]) => v !== 'ok');
    ok(Object.keys(cov).length >= 15 && !bad.length, `all ${Object.keys(cov).length} font files (11 fonts, Regular and Semibold) have the apostrophe, ampersand, quotes, < and >`, bad);

    // ═══ a job saved with the raw entity ═══
    console.log('a job saved with the raw entity (the saved workspace comes back)');
    // make the jobs what they were before the fix: their words raw, their fit drawn from the raw words; the front of entBoth APPROVED that way
    const RAW = { entBack: { back: ['Name One&#39;s', '&amp; Date One'] }, entBoth: { front: ['NAME', 'ONE&#39;S'], back: ['NAME TWO', '&AMP; DATE ONE'] } };
    const pre = await w.page.evaluate(async RAW => {
      const out = {};
      for (const [n, rid] of [['entBack', '4290000301'], ['entBoth', '4290000303']]) {
        const rows = Orders.rows().filter(r => r.order.receiptId === rid);
        for (const j of [...Engrave.items().values()].filter(j => rows.includes(j.row) || (j.row && j.row.parentRow && rows.includes(j.row.parentRow)))) {
          const face = j.face === 'front' ? 'front' : 'back', lines = RAW[n][face];
          Object.assign(j, { text: lines.join('\n'), lines: lines.slice(), lineInput: null, quote: n === 'entBack' ? 'Name One&#39;s &amp; Date One' : j.quote, fit: null, state: 'ready' });
          j.row.engrave = Object.assign({}, j.row.engrave, { text: lines.join(' ') });
          await Engrave.fitJob(j);   // (the fit worker lays out the raw words: the picture the old page drew)
          out[n + ':' + face] = { key: j.key, state: j.state, fit: j.fit && j.fit.lines ? j.fit.lines.join(' ') : null };
        }
      }
      // the front of entBoth is approved with those raw words (a person approved it before the fix): it must come back exactly as it was
      const fr = [...Engrave.items().values()].find(j => j.key === '4290000303_42900003031~front');
      Object.assign(fr, { state: 'approved', approvedAt: Date.now() - 60000, approvedBy: 'Test Operator' });
      out.before = { text: fr.text, lines: fr.lines.slice(), fit: JSON.stringify(fr.fit && fr.fit.lines), approvedAt: fr.approvedAt, state: fr.state };
      Session.schedule();
      return out;
    }, RAW);
    ok(/&#39;|&amp;/.test(pre['entBack:back'].fit || '') && /&AMP;/.test(pre['entBoth:back'].fit || ''), 'premise: the fits were drawn from the raw words (as the old page drew them)', pre);
    const flushed = await w.page.evaluate(() => Session.flushNow());
    ok(flushed === true, 'the workspace checkpoint was written to this (test) browser');
    await w.page.reload();
    await w.page.waitForFunction(() => window.CN && CN.S && window.RunCtl && window.Engrave && window.B && B.orders && B.orders.rows.length > 0 && Engrave.items().size > 0, null, { timeout: 120000 });
    await w.page.evaluate(() => CN.setMode('orders'));
    // the undecided jobs are fitted again by the page itself (free, offline)
    await w.page.waitForFunction(() => { const js = [...Engrave.items().values()].filter(j => j.key === '4290000301_42900003011' || j.key === '4290000303_42900003031'); return js.length === 2 && js.every(j => j.state === 'review' && j.fit); }, null, { timeout: 120000 }).catch(() => {});
    await R.settle(w.page, 500);
    const after = {};
    for (const n of ['entBack', 'entBoth']) after[n] = await readOrder(w, R.ORDERS[n].rid);
    const b = jobOf(after.entBack, 'back'), bb = jobOf(after.entBoth, 'back'), bf = jobOf(after.entBoth, 'front');
    ok(!!b && flat(b.text) === "Name One's & Date One" && noEntity([b.text, b.lines, b.quote]), 'saved raw back job: it reads "Name One\'s & Date One" after the workspace comes back', b);
    ok(!!b && b.state === 'review' && b.fitted && flat(b.fit.join(' ')) === "Name One's & Date One", 'and its fit was drawn again from the real characters (reviewable again, nothing approved)', b);
    ok(!!bb && flat(bb.text) === 'NAME TWO & DATE ONE' && !/AMP/.test(bb.text + bb.lines.join()) && bb.state === 'review' && bb.fitted && flat(bb.fit.join(' ')) === 'NAME TWO & DATE ONE', 'saved raw back job in capitals: "&AMP;" reads "&" and its fit is drawn again', bb);
    const was = pre.before;
    ok(!!bf && bf.state === 'approved' && bf.approvedAt === was.approvedAt && bf.text === was.text && JSON.stringify(bf.lines) === JSON.stringify(was.lines) && JSON.stringify(bf.fit) === was.fit && /&#39;/.test(bf.text + bf.lines.join()), 'an APPROVED job saved with the raw words is left exactly as it was (words, fit, approval untouched)', { was, bf });
    ok(after.entBack.stored[0].p[0] === ENT && after.entBoth.stored[0].p[0] === R.ORDERS.entBoth.personalization[0], 'the order\'s stored text was not rewritten', [after.entBack.stored[0].p, after.entBoth.stored[0].p]);

    console.log('page errors:', JSON.stringify(w.errors));
    ok(!(w.errors || []).length, 'no page error');
  } finally { await w.close(); }
  console.log(`\n${fail ? 'FAILED' : 'PASSED'}: ${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
