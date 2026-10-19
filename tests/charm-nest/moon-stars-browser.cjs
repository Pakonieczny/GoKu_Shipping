// MOONSTARS (Paul, 19 Oct 2026, order 4203628477): the moon and stars necklace on the REAL page. The engraving-UI real world (_engraveRealWorld.cjs: the real sorter page over the fake site, the real intake, pool, classifier and fit)
// with a made-up two-body drawing standing in for the master's "TINY MOON & STARS 2" (a star on the left, a crescent moon on the right, one file, a "Left + Right pair" entry) and the order of the screenshots
// (listing 1095123288, Etsy SKU MOON_3288, "Necklace Options: Silver • 2 stars ($43.20)", "First Star: M / Second Star: T"; the buyer's name made up).
//   · the line resolves with no decision: design TINY MOON & STARS 2, Silver, 3 pool pieces (the stars cut from the left body, the moon from the right body, one group)
//   · two FRONT engraving jobs (star 1 "M", star 2 "T"), none for the moon; the Engrave tab's row says "STAR 1 of 2 · FRONT" / "STAR 2 of 2 · FRONT" with their words
//   · the order window lists the three pieces: Star 1 of 2, Star 2 of 2, Moon
//   node tests/charm-nest/moon-stars-browser.cjs [playwright-core dir]        (PW_DIR=<dir> CHROMIUM=<chrome> SHOTS=<dir>)
'use strict';
const fs = require('fs'), os = require('os'), path = require('path');
if (process.argv[2]) process.env.PW_DIR = process.argv[2];
const R = require('./_engraveRealWorld.cjs'), Base = require('./_orderFactsWorld.cjs');
const SHOTS = process.env.SHOTS || path.join(os.tmpdir(), 'moon-stars');
const fails = []; let nCheck = 0;
const check = (ok, msg) => { nCheck++; if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
const DESIGN = 'TINY MOON & STARS 2';

/** A made-up drawing of two bodies side by side in one file, as the master draws TINY MOON & STARS 2: a five-pointed STAR on the left and a crescent MOON on the right, each with its own hole, in the CUT layer; nothing else. */
function moonStarsAi() {
  const f = x => (Math.round(x * 1000) / 1000).toString();
  const poly = pts => pts.map((p, i) => `${f(p[0])} ${f(p[1])} ${i ? 'l' : 'm'}`).join(' ') + ' h';
  const circle = (cx, cy, r) => { const k = 0.5522847498 * r; return `${f(cx + r)} ${f(cy)} m ${f(cx + r)} ${f(cy + k)} ${f(cx + k)} ${f(cy + r)} ${f(cx)} ${f(cy + r)} c ${f(cx - k)} ${f(cy + r)} ${f(cx - r)} ${f(cy + k)} ${f(cx - r)} ${f(cy)} c ${f(cx - r)} ${f(cy - k)} ${f(cx - k)} ${f(cy - r)} ${f(cx)} ${f(cy - r)} c ${f(cx + k)} ${f(cy - r)} ${f(cx + r)} ${f(cy - k)} ${f(cx + r)} ${f(cy)} c h`; };
  const star = Array.from({ length: 10 }, (_, i) => { const a = Math.PI / 2 + i * Math.PI / 5, q = i % 2 ? 6.4 : 15; return [17 + q * Math.cos(a), 21 + q * Math.sin(a)]; });
  const moon = []; for (let i = 0; i <= 24; i++) { const a = -Math.PI / 2 + i * Math.PI / 24; moon.push([40 + 19 * Math.cos(a), 21 + 19 * Math.sin(a)]); } for (let i = 24; i >= 0; i--) { const a = -Math.PI / 2 + i * Math.PI / 24; moon.push([40 + 8 * Math.cos(a), 21 + 19 * Math.sin(a)]); }
  const content = ['/OC /ocCut BDC',
    `q 0 0 0 RG 0.283 w ${poly(star)} S Q`, `q 0 0 0 RG 0.283 w ${circle(17, 30.5, 1.3)} S Q`,
    `q 0 0 0 RG 0.283 w ${poly(moon)} S Q`, `q 0 0 0 RG 0.283 w ${circle(52, 33, 1.3)} S Q`, 'EMC'].join('\n') + '\n';
  const objs = [
    '<< /Type /Pages /Kids [ 4 0 R ] /Count 1 >>',
    '<< /Type /Catalog /Pages 1 0 R /OCProperties << /OCGs [ 5 0 R ] /D << /Order [ 5 0 R ] /ON [ 5 0 R ] /BaseState /ON >> >> >>',
    '<< /Producer (MOONSTARS test drawing) /Title (TINY_MOON_STARS_TEST) >>',
    '<< /Type /Page /Parent 1 0 R /Resources << /Properties << /ocCut 5 0 R >> >> /MediaBox [ 0 0 64 44 ] /Contents 6 0 R >>',
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
  R.DESIGNS[DESIGN] = { file: 'TINY_MOON_STARS_2.ai', make: moonStarsAi, entry: { sku: DESIGN, widthPt: 47, heightPt: 38, areaPt2: 700, members: 4, holes: 2, engravable: true, engravableBy: 'index', upAngle: 90, upSource: 'drawn', charmHash: 'm00n5tar', backKeepOut: [], pair: { v: 1, bodies: 2, mismatched: true } } };
  R.ORDERS.moon = { rid: '4203628477', sku: 'MOON_3288', listing: '1095123288', title: 'Moon Necklace Gold Crescent Wedding Gift For Her Initial Necklace Celestial Jewelry Friendship Necklace Personalized Jewelry Gift For Mom', buyer: 'Emily Test', placed: { day: 0, h: 11, m: 30 }, ship: 5,
    variations: [{ name: 'Necklace Options', value: 'Silver • 2 stars ($43.20)' }], personalization: ['First Star: M\nSecond Star: T'], metalKey: 'silver', metalLabel: 'Sterling Silver' };
  R.EXPECT_JOBS.moon = 2;
  // (the fake orders are gold by default: this one is Silver, as the screenshot's; the rest of the fixture is the shared one)
  const base = Base.orderOf;
  Base.orderOf = new Function('return function orderOf(fx, now) { const o = (' + base.toString() + ')(fx, now); for (const l of o.lines) if (fx.metalKey) { l.metalKey = fx.metalKey; l.metalLabel = fx.metalLabel || ""; } return o; }')();
  const w = await R.world({ viewport: { width: 1600, height: 1000 }, shots: SHOTS });
  if (!w) { console.log('no playwright-core (pass its node_modules dir as the first argument)'); return; }
  try {
    const made = await R.build(w, ['moon']), m = made.moon;
    console.log(JSON.stringify(m));
    check(m.state === 'pooled' || m.state === 'written', `the line is pooled, nothing asked (state ${m.state}${m.why ? ': ' + m.why : ''})`);
    check(m.pieceCount === 3 && (m.poolIds || []).length === 3, `three pieces: 2 stars and the moon (${m.pieceCount}, ${(m.poolIds || []).length} pool ids)`);
    const facts = await w.page.evaluate(key => {
      const row = B.orders.byKey.get(key), sp = row.spec, ids = row.poolIds || [];
      const charms = allSheets().flatMap(s => s.charms || []).filter(c => ids.includes(c.poolId)).sort((a, b) => ids.indexOf(a.poolId) - ids.indexOf(b.poolId));
      const pools = ids.map(id => B.pool.rows.get(id)).filter(Boolean);
      return { design: sp.designSku, material: sp.material, problems: (sp.problems || []).length, bodies: sp.pair.pieceBodies, charms: charms.map(c => ({ oneBody: c.oneBody, members: (c.members || []).length, w: Math.round(c.widthPt), h: Math.round(c.heightPt), groupKey: c.groupKey || null })),
        pools: pools.map(p => ({ engrave: p.engrave, groupSize: p.groupSize || 0, sku: p.sku })), engrave: row.engrave && row.engrave.pieces ? Object.fromEntries(Object.entries(row.engrave.pieces).map(([k, v]) => [k.split('_').pop(), [v.needed, v.state, v.approved]])) : null };
    }, m.key);
    console.log(JSON.stringify(facts));
    check(facts.design === DESIGN && facts.material === 'silver' && facts.problems === 0, `design ${facts.design}, metal ${facts.material}, no question`);
    check(facts.charms.length === 3 && facts.charms.map(c => c.oneBody).join() === '0,0,1', `the stars are the left body, the moon the right body (${facts.charms.map(c => c.oneBody).join()})`);
    check(facts.charms.length === 3 && facts.charms[0].w === facts.charms[1].w && facts.charms[0].w !== facts.charms[2].w && new Set(facts.charms.map(c => c.groupKey)).size === 1, `each piece one body (sizes ${facts.charms.map(c => c.w + 'x' + c.h).join(', ')}), one group`);
    check(facts.pools.length === 3 && facts.pools[2].engrave === false, `the moon's pool record is not engraved (${facts.pools.map(p => p.engrave).join()}; this world pools before the engraving is read, so every record says false here)`);
    const jobs = m.jobs.map(j => [j.slot, j.face, j.text]).sort();
    check(m.jobs.length === 2 && m.jobs.every(j => j.face === 'front') && JSON.stringify(jobs.map(j => [j[0], String(j[2] || '').trim()])) === JSON.stringify([['D1', 'M'], ['D2', 'T']]), `two FRONT jobs: star 1 "M", star 2 "T", none for the moon (${JSON.stringify(jobs)})`);
    check(facts.engrave && JSON.stringify(facts.engrave['3']) === JSON.stringify([false, 'none', true]), `the moon's decision: nothing to engrave (${JSON.stringify(facts.engrave)})`);

    // the Engrave tab: the order's row
    await R.openEngraveDetail(w, 'moon', { face: 'front' });
    await w.page.evaluate(async () => { try { Engrave.restoreView({ tab: 'place', list: true, chosen: false }); Engrave.render(); } catch (_) {} await new Promise(r => setTimeout(r, 500)); });
    const tab = await w.page.evaluate(rid => { const host = document.querySelector('#egQueue') || document.querySelector('main') || document.body; const t = host.innerText.replace(/[ \t]+/g, ' '); return { text: t.split('\n').map(x => x.trim()).filter(Boolean).filter(x => /STAR|MOON|^[MT]$|4203628477/i.test(x)), n: t.includes(rid) ? 1 : 0 }; }, '4203628477');
    console.log('Engrave tab:', JSON.stringify(tab));
    const tabText = tab.text.join(' | ');
    check(/STAR 1 of 2/i.test(tabText) && /STAR 2 of 2/i.test(tabText), 'the Engrave tab names "STAR 1 of 2" and "STAR 2 of 2"');
    check(/\bM\b/.test(tabText) && /\bT\b/.test(tabText) && !/MOON\b(?! &)/.test(tabText.replace(/TINY MOON & STARS 2/gi, '')), 'with their words M and T, and no moon engraving');
    fs.mkdirSync(SHOTS, { recursive: true });
    console.log('shot', await R.shot(w, 'moon-engrave-tab'));
    await R.openEngraveDetail(w, 'moon', { face: 'front', slot: 'D1' });
    console.log('shot', await R.shot(w, 'moon-engrave-detail-star1'));

    // the order window: its pieces
    await R.openOrderWindow(w, 'moon');
    await w.page.evaluate(async () => { for (const b of document.querySelectorAll('#orderWin .owPcX')) if (b.getAttribute('aria-expanded') !== 'true') b.click(); await new Promise(r => setTimeout(r, 400)); });
    const win = await w.page.evaluate(() => { const ow = document.getElementById('orderWin'); return ow ? ow.innerText.replace(/[ \t]+/g, ' ') : ''; });
    const lines = win.split('\n').map(s => s.trim()).filter(Boolean);
    console.log('order window (excerpt):', JSON.stringify(lines.filter(s => /star|moon|piece|stars/i.test(s)).slice(0, 30)));
    check(/Star 1 of 2/.test(win) && /Star 2 of 2/.test(win) && lines.some(s => /^Moon\b/.test(s) || /\bMoon\b/.test(s) && !/MOON & STARS|Moon Necklace/i.test(s)), 'the order window lists Star 1 of 2, Star 2 of 2 and the Moon');
    check(!/NEEDS A DECISION/i.test(win) && !/not mapped/i.test(win), 'no "NEEDS A DECISION", no "not mapped"');
    check(/2 stars \+ 1 moon/i.test(win) && !/\b3 stars\b/.test(win), 'the quantity says "2 stars + 1 moon", never "3 stars"');
    console.log('shot', await R.shot(w, 'moon-order-window'));
    // (Paul, 19 Oct 2026: "The vector thumbnail should show both the moon and stars": the picture's inked box has the WHOLE design's proportions, not one star's)
    const pic = await w.page.evaluate(async d => {
      const host = document.querySelector('#orderWin [data-vector]'); for (let i = 0; i < 40 && host && host.getAttribute('aria-busy') === 'true'; i++) await new Promise(r => setTimeout(r, 100));
      const im = host && host.querySelector('img,canvas'); if (!im) return null; if (im.decode) await im.decode().catch(() => {});
      const W = im.naturalWidth || im.width, H = im.naturalHeight || im.height, c = document.createElement('canvas'); c.width = W; c.height = H; const x = c.getContext('2d'); x.drawImage(im, 0, 0);
      const px = x.getImageData(0, 0, W, H).data; let l = W, r = -1, t = H, b = -1;
      for (let y = 0; y < H; y++) for (let k = 0; k < W; k++) { const o = (y * W + k) * 4; if (px[o + 3] > 40 && px[o] + px[o + 1] + px[o + 2] < 600) { l = Math.min(l, k); r = Math.max(r, k); t = Math.min(t, y); b = Math.max(b, y); } }
      const e = Master.entryFor(d); return { ink: r >= l ? (r - l + 1) / (b - t + 1) : 0, whole: e ? e.widthPt / e.heightPt : 0 };
    }, DESIGN);
    console.log('vector picture:', JSON.stringify(pic));
    check(pic && pic.whole > 0 && Math.abs(pic.ink / pic.whole - 1) < 0.2, `the Vector design shows the whole design, the moon and the star (inked ${pic && pic.ink.toFixed(2)} wide per tall, the design ${pic && pic.whole.toFixed(2)})`);
    check(!w.errors.length, 'no page error: ' + w.errors.slice(0, 3).join(' | '));
  } finally { await w.close(); }
  console.log(`moon-stars-browser: ${nCheck - fails.length} of ${nCheck} checks passed`);
  if (fails.length) { console.log('FAILED:\n  ' + fails.join('\n  ')); process.exitCode = 1; }
})().catch(e => { console.error(e); process.exit(1); });
