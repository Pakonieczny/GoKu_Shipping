// Draws the page's seals (charm-nest-motion.js, Seal.face) to a PNG at the sizes they appear at (24, 50 and 84 px, and 150 px to look at): a quick visual check of a seal family, no page, no server.
//   PW_DIR=<dir holding playwright-core> CHROMIUM=<chrome binary> node scripts/render-seal.cjs <out.png> [family[:LETTERS] ...]
//   families: the keys of Seal.FAMILY (received prepared engraving laser finishing fulfilment exceptions cancelled monogram release); default: every family, and the monogram seal with letters, wide letters, and none.
//   "monogram:APV" draws the monogram seal with those three letters; "monogram" alone draws its pictogram.
// (MONOSEAL, Paul 17 Oct 2026: written to look at the new monogram seal; any family works.)
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '..');
const out = process.argv[2];
if (!out) { console.log('usage: node scripts/render-seal.cjs <out.png> [family[:LETTERS] ...]'); process.exit(1); }
const want = process.argv.slice(3);
let chromium;
try { ({ chromium } = require(path.join(process.env.PW_DIR || path.join(root, 'node_modules'), 'playwright-core'))); } catch (_) { console.log('render-seal: no playwright-core (set PW_DIR)'); process.exit(1); }
(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const page = await browser.newPage({ viewport: { width: 760, height: 700 }, deviceScaleFactor: 2 });
  // the page's own seal rules (size, rotation, ink blend), taken from the sorter's stylesheet
  const css = (fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8').match(/:root\{--seal-base:50px\}[\s\S]*?\.seal\.wobble/) || [''])[0].replace(/\.seal\.wobble$/, '');
  await page.setContent(`<html><head><style>body{margin:0;background:#fffefb;font:12px system-ui}${css}.row{display:flex;gap:18px;align-items:center;padding:8px 14px;border-bottom:1px solid #eee}.row b{width:110px}.cell{display:inline-block}</style></head><body><div id=o></div></body></html>`);
  await page.addScriptTag({ content: fs.readFileSync(path.join(root, 'charm-nest-motion.js'), 'utf8') });
  await page.evaluate(wanted => {
    const S = window.Seal, at = Date.UTC(2026, 9, 17, 18, 5), o = document.getElementById('o');
    const row = (label, family, extra) => {
      const r = document.createElement('div'); r.className = 'row';
      r.innerHTML = `<b>${label}</b>` + [24, 50, 84, 150].map(px => `<span class="cell" style="width:${px}px;height:${px}px">${S.face(Object.assign({ family, action: S.FAMILY[family].name.toUpperCase(), at, by: 'Test Operator' }, extra), {})}</span>`).join('');
      o.appendChild(r); r.querySelectorAll('svg').forEach(s => { s.style.width = s.parentNode.style.width; s.style.height = s.parentNode.style.height; });
    };
    const list = wanted.length ? wanted : [...Object.keys(S.FAMILY), 'monogram:APV', 'monogram:WMW'];
    for (const item of list) { const [family, letters] = item.split(':'); if (!S.FAMILY[family]) continue; row(item, family, family === 'monogram' ? { action: 'MONOGRAM', icon: 'monogram', letters } : {}); }
  }, want);
  await page.screenshot({ path: out, fullPage: true });
  await browser.close();
  console.log('render-seal: wrote ' + out);
})().catch(e => { console.error(e); process.exit(1); });
