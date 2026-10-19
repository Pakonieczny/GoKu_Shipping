// BROKENHEART (Paul, 19 Oct 2026, order 4204452679): the broken heart "Best Friends Necklace Set of 2" (Etsy SKU Best_4003, the library design BEST_4003) on the REAL page. The engraving-UI real world (_engraveRealWorld.cjs:
// the real sorter page over the fake site, the real intake, pool, classifier and fit) with a made-up drawing standing in for the master's BEST 4003: a heart broken in two along a zigzag, the LEFT half and the RIGHT half
// drawn as two closed outlines, each with its own hole, in one file. Two orders of the listing (the buyers made up), its drop-down "Personalization: SILVER·Front":
//   · heart: "Mama / Daughter": 2 pool pieces (the left half cut from the left body, the right half from the right body, one group); two FRONT jobs, the left half "Mama", the right half "Daughter"; the Engrave tab says
//     "LEFT HALF · FRONT" / "RIGHT HALF · FRONT" with their words; the engraving preview of each half is that half's own outline; the order window lists the Left half and the Right half, "2 halves"; its Vector design
//     is the whole heart (both halves), no Left / Right chips
//   · real (the shop owner's order 4204452679): "Sisters": the same two pieces, and the words wait for a person: neither half is given "Sisters"
//   node tests/charm-nest/broken-heart-browser.cjs [playwright-core dir]        (PW_DIR=<dir> CHROMIUM=<chrome> SHOTS=<dir>)
'use strict';
const fs = require('fs'), os = require('os'), path = require('path');
if (process.argv[2]) process.env.PW_DIR = process.argv[2];
const R = require('./_engraveRealWorld.cjs'), Base = require('./_orderFactsWorld.cjs');
const SHOTS = process.env.SHOTS || path.join(os.tmpdir(), 'broken-heart');
const fails = []; let nCheck = 0;
const check = (ok, msg) => { nCheck++; if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
const DESIGN = 'BEST_4003';

/** A made-up drawing of a heart broken in two along a zigzag, as the master draws BEST 4003: the LEFT half and the RIGHT half, two closed outlines with a hair of kerf between them, each with its own hole, in the CUT layer. */
function brokenHeartAi() {
  const f = x => (Math.round(x * 1000) / 1000).toString();
  const poly = pts => pts.map((p, i) => `${f(p[0])} ${f(p[1])} ${i ? 'l' : 'm'}`).join(' ') + ' h';
  const circle = (cx, cy, r) => { const k = 0.5522847498 * r; return `${f(cx + r)} ${f(cy)} m ${f(cx + r)} ${f(cy + k)} ${f(cx + k)} ${f(cy + r)} ${f(cx)} ${f(cy + r)} c ${f(cx - k)} ${f(cy + r)} ${f(cx - r)} ${f(cy + k)} ${f(cx - r)} ${f(cy)} c ${f(cx - r)} ${f(cy - k)} ${f(cx - k)} ${f(cy - r)} ${f(cx)} ${f(cy - r)} c ${f(cx + k)} ${f(cy - r)} ${f(cx + r)} ${f(cy - k)} ${f(cx + r)} ${f(cy)} c h`; };
  const HX = t => 30 + 1.5 * 16 * Math.pow(Math.sin(t), 3), HY = t => 26 + 1.5 * (13 * Math.cos(t) - 5 * Math.cos(2 * t) - 2 * Math.cos(3 * t) - Math.cos(4 * t));
  const arc = (a, b, k) => Array.from({ length: k + 1 }, (_, i) => { const t = a + (b - a) * i / k; return [HX(t), HY(t)]; });
  const top = HY(0), tip = HY(Math.PI);
  const zig = (dx, down) => { const out = []; for (let i = 0; i <= 10; i++) { const y = top - (top - tip) * i / 10, a = i === 0 || i >= 9 ? 0 : (i % 2 ? 1.8 : -1.8); out.push([(i === 0 || i === 10 ? 30 : 31.2) + a + dx, y]); } return down ? out : out.reverse(); };
  const left = arc(Math.PI, 2 * Math.PI, 48).map(([x, y]) => [Math.min(x, 29.5), y]).concat(zig(-0.5, true));
  const right = arc(0, Math.PI, 48).map(([x, y]) => [Math.max(x, 30.5), y]).concat(zig(0.5, false));
  const content = ['/OC /ocCut BDC',
    `q 0 0 0 RG 0.283 w ${poly(left)} S Q`, `q 0 0 0 RG 0.283 w ${circle(17, 37, 1.3)} S Q`,
    `q 0 0 0 RG 0.283 w ${poly(right)} S Q`, `q 0 0 0 RG 0.283 w ${circle(43, 37, 1.3)} S Q`, 'EMC'].join('\n') + '\n';
  const objs = [
    '<< /Type /Pages /Kids [ 4 0 R ] /Count 1 >>',
    '<< /Type /Catalog /Pages 1 0 R /OCProperties << /OCGs [ 5 0 R ] /D << /Order [ 5 0 R ] /ON [ 5 0 R ] /BaseState /ON >> >> >>',
    '<< /Producer (BROKENHEART test drawing) /Title (BEST_4003_TEST) >>',
    '<< /Type /Page /Parent 1 0 R /Resources << /Properties << /ocCut 5 0 R >> >> /MediaBox [ 0 0 60 50 ] /Contents 6 0 R >>',
    '<< /Type /OCG /Name (CUT) >>',
    `<< /Length ${Buffer.byteLength(content)} >>\nstream\n${content}endstream`
  ];
  let out = '%PDF-1.7\n'; const at = [];
  objs.forEach((o, i) => { at.push(Buffer.byteLength(out)); out += `${i + 1} 0 obj\n${o}\nendobj\n`; });
  const x0 = Buffer.byteLength(out);
  out += `xref\n0 ${objs.length + 1}\n0000000000 65535 f \n` + at.map(n => String(n).padStart(10, '0') + ' 00000 n \n').join('') + `trailer\n<< /Size ${objs.length + 1} /Root 2 0 R /Info 3 0 R >>\nstartxref\n${x0}\n%%EOF\n`;
  return Buffer.from(out, 'latin1');
}

(async () => {
  R.DESIGNS[DESIGN] = { file: 'BEST_4003.ai', make: brokenHeartAi, entry: { sku: DESIGN, widthPt: 48, heightPt: 39, areaPt2: 1100, members: 4, holes: 2, engravable: true, engravableBy: 'index', upAngle: 90, upSource: 'drawn', charmHash: 'b3574003', backKeepOut: [], pair: { v: 1, bodies: 2, mismatched: true } } };
  const TITLE = 'Best Friends Necklace Set of 2, Set of 2 Necklaces, Best Friends Forever Personalized Jewelry, BFF Jewelry';
  const DROP = { name: 'Personalization', value: 'SILVER·Front', valueId: '4417', propertyId: '513' };
  R.ORDERS.heart = { rid: '4204452601', sku: 'Best_4003', listing: '1904452601', title: TITLE, buyer: 'Jo Test', placed: { day: 0, h: 11, m: 30 }, ship: 5, variations: [DROP], personalization: ['Mama\nDaughter'], metalKey: 'silver', metalLabel: 'Sterling Silver' };
  R.ORDERS.real = { rid: '4204452679', sku: 'Best_4003', listing: '1904452679', title: TITLE, buyer: 'Sam Test', placed: { day: 0, h: 11, m: 40 }, ship: 5, variations: [DROP], personalization: ['Sisters'], metalKey: 'silver', metalLabel: 'Sterling Silver' };
  R.EXPECT_JOBS.heart = 2; R.EXPECT_JOBS.real = 2;
  // (the fake orders are gold by default: these are Silver, as the order's drop-down; the rest of the fixture is the shared one)
  const base = Base.orderOf;
  Base.orderOf = new Function('return function orderOf(fx, now) { const o = (' + base.toString() + ')(fx, now); for (const l of o.lines) if (fx.metalKey) { l.metalKey = fx.metalKey; l.metalLabel = fx.metalLabel || ""; } return o; }')();
  const w = await R.world({ viewport: { width: 1600, height: 1000 }, shots: SHOTS });
  if (!w) { console.log('no playwright-core (pass its node_modules dir as the first argument)'); return; }
  try {
    const made = await R.build(w, ['heart', 'real']), m = made.heart, real = made.real;
    console.log(JSON.stringify(m)); console.log(JSON.stringify(real));
    check(m.state === 'pooled' || m.state === 'written', `the line is pooled, nothing asked (state ${m.state}${m.why ? ': ' + m.why : ''})`);
    check(m.pieceCount === 2 && (m.poolIds || []).length === 2, `two pieces per heart: the left half and the right half (${m.pieceCount}, ${(m.poolIds || []).length} pool ids)`);
    const factsOf = key => w.page.evaluate(key => {
      const row = B.orders.byKey.get(key), sp = row.spec, ids = row.poolIds || [];
      const charms = allSheets().flatMap(s => s.charms || []).filter(c => ids.includes(c.poolId)).sort((a, b) => ids.indexOf(a.poolId) - ids.indexOf(b.poolId));
      const pools = ids.map(id => B.pool.rows.get(id)).filter(Boolean);
      return { design: sp.designSku, material: sp.material, problems: (sp.problems || []).length, problemList: (sp.problems || []).map(p => [p.kind, p.optionName || '', p.optionValue || '', p.reason || p.why || '']), bodies: sp.pair.pieceBodies, labels: sp.pair.labels, eng: sp.engraving ? { mode: sp.engraving.mode, needsPerson: !!sp.engraving.needsPerson, why: sp.engraving.why } : null,
        charms: charms.map(c => ({ oneBody: c.oneBody, members: (c.members || []).length, w: Math.round(c.widthPt), h: Math.round(c.heightPt), cx: Math.round((c.bbox[0] + c.bbox[2]) / 2), groupKey: c.groupKey || null })),
        pools: pools.map(p => ({ engrave: p.engrave, groupSize: p.groupSize || 0, sku: p.sku })) };
    }, key);
    const facts = await factsOf(m.key);
    console.log(JSON.stringify(facts));
    check(facts.design === DESIGN && facts.material === 'silver' && facts.problems === 0, `design ${facts.design}, metal ${facts.material}, no question`);
    check(JSON.stringify(facts.bodies) === '[0,1]' && JSON.stringify(facts.labels) === '["Left half","Right half"]', `pieceBodies ${JSON.stringify(facts.bodies)}, labels ${JSON.stringify(facts.labels)}`);
    check(facts.charms.length === 2 && facts.charms.map(c => c.oneBody).join() === '0,1' && facts.charms[0].cx < facts.charms[1].cx, `the left half is the left body, the right half the right body (${facts.charms.map(c => c.oneBody + '@' + c.cx).join(', ')})`);
    check(facts.charms.length === 2 && facts.charms.every(c => c.w < 40) && new Set(facts.charms.map(c => c.groupKey)).size === 1, `each piece is ONE half, never the whole heart (sizes ${facts.charms.map(c => c.w + 'x' + c.h).join(', ')}), one group`);
    const jobs = m.jobs.map(j => [j.slot, j.face, String(j.text || '').trim()]).sort();
    check(m.jobs.length === 2 && m.jobs.every(j => j.face === 'front') && JSON.stringify(jobs.map(j => [j[0], j[2]])) === JSON.stringify([['D1', 'Mama'], ['D2', 'Daughter']]), `two FRONT jobs: the left half "Mama", the right half "Daughter" (${JSON.stringify(jobs)})`);
    // the real order: "Sisters" is one text for two halves: never guessed
    const rf = await factsOf(real.key);
    console.log(JSON.stringify(rf));
    check(real.pieceCount === 2 && rf.charms.map(c => c.oneBody).join() === '0,1', `the real order (Sisters) is two halves too (${real.pieceCount}, bodies ${rf.charms.map(c => c.oneBody).join()})`);
    check(real.jobs.length === 2 && real.jobs.every(j => j.face === 'front' && !/Sisters|^Sis$|^ters$/.test(String(j.text || '').trim())) && rf.eng && rf.eng.needsPerson && /Say which words go on each half/.test(rf.eng.why), `"Sisters" goes on neither half: a person says which words go on each (${JSON.stringify(real.jobs.map(j => [j.slot, j.state, j.text]))}; ${rf.eng && rf.eng.why})`);

    // the Engrave tab: the order's row
    await R.openEngraveDetail(w, 'heart', { face: 'front' });
    await w.page.evaluate(async () => { try { Engrave.restoreView({ tab: 'place', list: true, chosen: false }); Engrave.render(); } catch (_) {} await new Promise(r => setTimeout(r, 500)); });
    const tab = await w.page.evaluate(rid => { const host = document.querySelector('#egQueue') || document.querySelector('main') || document.body; const t = host.innerText.replace(/[ \t]+/g, ' '); return { text: t.split('\n').map(x => x.trim()).filter(Boolean).filter(x => /HALF|Mama|Daughter|Sisters|DISC|4204452601|4204452679/i.test(x)), n: t.includes(rid) ? 1 : 0 }; }, '4204452601');
    console.log('Engrave tab:', JSON.stringify(tab));
    const tabText = tab.text.join(' | ');
    check(/LEFT HALF · FRONT/.test(tabText) && /RIGHT HALF · FRONT/.test(tabText), 'the Engrave tab names "LEFT HALF · FRONT" and "RIGHT HALF · FRONT"');
    check(/\bMama\b/.test(tabText) && /\bDaughter\b/.test(tabText) && !/DISC \d/.test(tabText) && !/EAR\b/.test(tabText), 'with their words Mama and Daughter; never "DISC 1 of 2", never an ear');
    fs.mkdirSync(SHOTS, { recursive: true });
    console.log('shot', await R.shot(w, 'heart-engrave-tab'));
    for (const [slot, body, word] of [['D1', 0, 'left'], ['D2', 1, 'right']]) {
      const jk = await R.openEngraveDetail(w, 'heart', { face: 'front', slot });
      const pv = await w.page.evaluate(k => { const j = Engrave.items().get(k); const c = j && (j.copies || []).map(id => Pool.charmOf(id)).find(Boolean); return j ? { slot: j.slot, text: j.text, oneBody: c ? c.oneBody : null, w: c ? Math.round(c.widthPt) : 0 } : null; }, jk);
      check(pv && pv.oneBody === body && pv.w < 40, `the ${word} half's engraving is fitted on its own outline (${JSON.stringify(pv)})`);
      console.log('shot', await R.shot(w, `heart-engrave-detail-${word}-half`));
    }

    // the order window: its pieces
    await R.openOrderWindow(w, 'heart');
    await w.page.evaluate(async () => { for (const b of document.querySelectorAll('#orderWin .owPcX')) if (b.getAttribute('aria-expanded') !== 'true') b.click(); await new Promise(r => setTimeout(r, 400)); });
    const win = await w.page.evaluate(() => { const ow = document.getElementById('orderWin'); return ow ? ow.innerText.replace(/[ \t]+/g, ' ') : ''; });
    const lines = win.split('\n').map(s => s.trim()).filter(Boolean);
    console.log('order window (excerpt):', JSON.stringify(lines.filter(s => /half|halves|piece|disc|Mama|Daughter/i.test(s)).slice(0, 30)));
    check(/Left half/.test(win) && /Right half/.test(win), 'the order window lists the Left half and the Right half');
    check(!/Piece 1 of 2|Disc 1 of 2|Half 1 of 2|halfs/i.test(win) && !/Left ear|earring/i.test(win.replace(/Best Friends[^\n]*/g, '')), 'never "Piece 1 of 2", "Disc 1 of 2", "halfs" or an ear');
    check(/2 halves/i.test(win), 'the quantity says "2 halves"');
    check(!/NEEDS A DECISION/i.test(win) && !/not mapped/i.test(win), 'no "NEEDS A DECISION", no "not mapped"');
    console.log('shot', await R.shot(w, 'heart-order-window'));
    // the Vector design: the WHOLE heart (both halves), as drawn: its inked box has the whole design's proportions (one half is about 0.6 as wide as tall, the heart 1.2)
    const pic = await w.page.evaluate(async d => {
      const pick = () => [...document.querySelectorAll('#owVector')].map(h => h.querySelector('img,canvas')).find(Boolean);
      let im = pick(); for (let i = 0; i < 60 && !im; i++) { await new Promise(r => setTimeout(r, 100)); im = pick(); }
      if (!im) return { none: document.querySelectorAll('#owVector').length }; if (im.decode) await im.decode().catch(() => {});
      const W = im.naturalWidth || im.width, H = im.naturalHeight || im.height, c = document.createElement('canvas'); c.width = W; c.height = H; const x = c.getContext('2d'); x.drawImage(im, 0, 0);
      const px = x.getImageData(0, 0, W, H).data; let l = W, r = -1, t = H, b = -1;
      for (let y = 0; y < H; y++) for (let k = 0; k < W; k++) { const o = (y * W + k) * 4; if (px[o + 3] > 40 && px[o] + px[o + 1] + px[o + 2] < 600) { l = Math.min(l, k); r = Math.max(r, k); t = Math.min(t, y); b = Math.max(b, y); } }
      const row = B.orders.byKey.get([...B.orders.byKey.keys()].find(k => k.startsWith('4204452601'))), ids = row.poolIds || [], half = ids.map(id => Pool.charmOf(id)).find(Boolean);
      const host = document.getElementById('owVector'), chips = host ? [...host.querySelectorAll('*')].filter(e => /^(Left|Right|L|R)$/.test((e.textContent || '').trim()) && !e.children.length).length : -1;
      const M = window.Master || (window.B && B.master), e = M && M.entryFor ? M.entryFor(d) : null;
      return { ink: r >= l ? (r - l + 1) / (b - t + 1) : 0, whole: e ? e.widthPt / e.heightPt : 0, half: half ? half.widthPt / half.heightPt : 0, chips };
    }, DESIGN);
    console.log('vector picture:', JSON.stringify(pic));
    check(pic && pic.whole > 0 && pic.ink > 0 && pic.half > 0 && Math.abs(pic.ink - pic.whole) < Math.abs(pic.ink - pic.half), `the Vector design shows the whole heart, both halves (${JSON.stringify(pic)})`);
    check(pic && pic.chips === 0, 'no Left / Right chips on the Vector design');
    check(!w.errors.length, 'no page error: ' + w.errors.slice(0, 3).join(' | '));
  } finally { await w.close(); }
  console.log(`broken-heart-browser: ${nCheck - fails.length} of ${nCheck} checks passed`);
  if (fails.length) { console.log('FAILED:\n  ' + fails.join('\n  ')); process.exitCode = 1; }
})().catch(e => { console.error(e); process.exit(1); });
