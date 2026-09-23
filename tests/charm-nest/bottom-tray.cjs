// The orders counter, the progress bar and the Design Station strip (or its pill) share the bottom of the sorter page.
// They used to be pinned to the same corner and lay over each other (23 Sep). Opens the page in headless Chromium with
// all three showing, the counter carrying the long sandbox text, and checks at five widths that none overlaps another,
// that all stay on screen and that their bottoms line up where they share a row.
//   node tests/charm-nest/bottom-tray.cjs [playwright-core dir]
const http = require('http'), fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json' };
const server = http.createServer((req, res) => {
  const u = decodeURIComponent(req.url.split('?')[0]);
  if (u.startsWith('/.netlify/functions/')) { res.writeHead(404, { 'Content-Type': 'application/json' }); return res.end('{"error":"no functions in the test server"}'); }
  const f = path.join(root, u === '/' ? 'charm-nest-1.html' : u);
  if (!f.startsWith(root) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end(); }
  res.writeHead(200, { 'Content-Type': MIME[path.extname(f)] || 'application/octet-stream' }); fs.createReadStream(f).pipe(res);
}).listen(0);
(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    for (const [w, h] of [[1500, 900], [1200, 800], [980, 800], [700, 800], [420, 800]]) for (const mode of ['pilled', 'pip']) {
      const page = await browser.newPage({ viewport: { width: w, height: h } });
      await page.goto(`http://127.0.0.1:${server.address().port}/charm-nest-1.html`);
      await page.waitForFunction(() => window.CN && window.Dock && window.Arrivals && window.CNProgress);
      const boxes = await page.evaluate(async mode => {
        Dock.ensure(); const f = document.createElement('iframe'); f.id = 'dsFrame'; Dock._D.body.appendChild(f);
        B.run = { status: 'running', step: 'nesting sheet 1' }; Dock._D.hiddenByUser = mode === 'pilled'; Dock.layout();
        Arrivals.paint(); Arrivals.paint = () => {};   // keep the long text below in place
        document.getElementById('arrivalCounter').textContent = 'Sandbox 50x · sim Wed 20:20 · Orders received · 24h 9 · 1h 9 · waiting for the sorter: a sheet is nesting';
        CNProgress.start('Preparing 8 order line(s)', { total: 8 }).set(3, 8, 'READER_97110'); CNProgress.start('another');
        await new Promise(r => setTimeout(r, 300));
        const r = el => { const b = el.getBoundingClientRect(); return { x: b.left, y: b.top, r: b.right, b: b.bottom }; };
        return { counter: r(document.getElementById('arrivalCounter')), progress: r(document.querySelector('.cnp .cnpRow')), corner: r(mode === 'pilled' ? Dock._D.pill : Dock._D.el), vw: innerWidth, vh: innerHeight };
      }, mode);
      await page.close();
      const at = `${w}px, station ${mode === 'pip' ? 'strip' : 'pill'}`, names = ['counter', 'progress', 'corner'];
      for (const n of names) {
        const b = boxes[n]; assert(b.r - b.x > 20 && b.b - b.y > 10, `${n} shows (${at})`);
        assert(b.x >= -0.5 && b.r <= boxes.vw + 0.5 && b.b <= boxes.vh + 0.5, `${n} stays on screen (${at}): ${JSON.stringify(b)}`);
      }
      for (let i = 0; i < 3; i++) for (let j = i + 1; j < 3; j++) {
        const a = boxes[names[i]], b = boxes[names[j]];
        assert(!(a.x < b.r - 0.5 && b.x < a.r - 0.5 && a.y < b.b - 0.5 && b.y < a.b - 0.5), `${names[i]} and ${names[j]} do not overlap (${at}): ${JSON.stringify([a, b])}`);
      }
      if (boxes.counter.b > boxes.corner.y) assert(Math.abs(boxes.counter.b - boxes.corner.b) < 1.5, `counter and station line up at the bottom (${at})`);
    }
  } finally { await browser.close(); server.close(); }
  console.log('bottom tray OK · counter, progress bar and station strip or pill never overlap, from 1500 to 420 px wide');
})().catch(e => { console.error(e); process.exit(1); });
