// What the sorter is busy with sits at the right of its top bar, small (Paul, 24 Sep): the progress bar, the orders counter
// and the Design Station strip used to share the bottom of the screen. Opens the page in headless Chromium with a run on,
// two tasks and a failed order check, and checks at five widths that nothing is left at the bottom, that the line and the
// chip sit inside the top bar without covering a tab or a tool, and that the station frame stays laid out out of sight.
//   node tests/charm-nest/top-status.cjs [playwright-core dir]
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
    for (const [w, h] of [[1500, 900], [1200, 800], [980, 800], [700, 800], [420, 800]]) {
      const page = await browser.newPage({ viewport: { width: w, height: h } });
      const errors = []; page.on('pageerror', e => errors.push(e.message));
      await page.goto(`http://127.0.0.1:${server.address().port}/charm-nest-1.html`);
      await page.waitForFunction(() => window.CN && window.Dock && window.Arrivals && window.CNProgress);
      const got = await page.evaluate(async () => {
        const app = document.getElementById('app'); if (innerWidth < 760 && !app.classList.contains('railOff')) document.getElementById('btnRail').click();
        Dock.ensure(); const f = document.createElement('iframe'); f.id = 'dsFrame'; Dock._D.body.appendChild(f);
        B.run = { status: 'running', step: 'nesting sheet 1' }; Dock.layout();
        CNProgress.start('Talking to the cloud');
        const t = CNProgress.start('Preparing 8 order line(s)', { total: 8 }); t.set(3, 8, 'READER_97110');
        Arrivals.state().error = 'orders.snapshot: the Design Station did not answer in time'; Arrivals.paint();
        await new Promise(r => setTimeout(r, 300));
        const box = el => { if (!el) return null; const b = el.getBoundingClientRect(); return { x: b.left, y: b.top, r: b.right, b: b.bottom, w: b.width, h: b.height }; };
        const bar = document.querySelector('.topbar'), line = document.querySelector('#cnpSlot > .cnp.mini.on'), row = line && line.querySelector('.cnpRow'), chip = document.getElementById('ordersChip');
        // the tabs as far as they show (the strip scrolls), and the tools
        const seg = document.getElementById('modeSeg').getBoundingClientRect(), clip = b => ({ ...b, x: Math.max(b.x, seg.left), r: Math.min(b.r, seg.right) });
        const others = [...document.querySelectorAll('#modeSeg button')].map(e => ({ name: e.dataset.mode, ...clip(box(e)) })).filter(b => b.r > b.x)
          .concat([...document.querySelectorAll('.topTools > *')].filter(e => e.getClientRects().length).map(e => ({ name: e.id || e.textContent.trim().slice(0, 14), ...box(e) })));
        const out = { bar: box(bar), tight: !!line && line.classList.contains('tight'), row: box(row), track: box(line && line.querySelector('.cnpTrack')), label: box(row && row.querySelector('.cnpLabel')),
          chip: box(chip), chipText: chip && chip.textContent, chipTitle: chip && chip.title, title: line && line.title,
          others, vw: innerWidth, dock: box(document.getElementById('dsDock')), frame: { w: f.offsetWidth, h: f.offsetHeight },
          gone: ['#bottomTray', '#arrivalCounter', '#dsDockPill', '#dsDock .dockBar'].filter(s => document.querySelector(s)),
          fixedCnp: getComputedStyle(document.querySelector('.cnp')).position, pad: parseFloat(getComputedStyle(document.querySelector('.stage')).paddingBottom) };
        Arrivals.state().error = null; Arrivals.paint(); out.chipAfter = !!document.getElementById('ordersChip');
        return out;
      });
      await page.close();
      const at = `${w}px`;
      assert.deepStrictEqual(errors, [], `no page errors (${at})`);
      assert.deepStrictEqual(got.gone, [], `nothing of the bottom tray or the station strip is left (${at})`);
      assert.notStrictEqual(got.fixedCnp, 'fixed', `the progress line is part of the top bar, not pinned to the screen (${at})`);
      assert(got.pad <= 16.5, `the stage keeps no room at the bottom for a tray (${at}): ${got.pad}`);
      assert(got.dock.w === 0 && got.dock.h === 0, `the station shows nothing off its own tab (${at}): ${JSON.stringify(got.dock)}`);
      assert(got.frame.w >= 980 && got.frame.h >= 600, `and its frame stays laid out, so it keeps hydrating (${at}): ${JSON.stringify(got.frame)}`);
      assert(got.chip, `the chip shows (${at})`);
      assert(got.chip.y >= got.bar.y - 0.5 && got.chip.b <= got.bar.b + 0.5 && got.chip.x >= -0.5 && got.chip.r <= got.vw + 0.5, `the chip sits inside the top bar, on screen (${at}): ${JSON.stringify([got.chip, got.bar])}`);
      for (const o of got.others) if (o.name !== 'ordersChip') assert(!(got.chip.x < o.r - 0.5 && o.x < got.chip.r - 0.5 && got.chip.y < o.b - 0.5 && o.y < got.chip.b - 0.5), `the chip covers no tab or tool (${at}): ${o.name}`);
      if (got.tight) {
        // no room for words beside the tabs: the bar alone, along the bottom edge of the top bar
        assert(got.track.h >= 1.5 && got.track.h <= 3 && Math.abs(got.track.b - got.bar.b) <= 1.5 && got.track.w >= got.bar.w - 1, `with no room, the bar runs along the top bar's edge (${at}): ${JSON.stringify([got.track, got.bar])}`);
      } else {
        const b = got.row;
        assert(b && b.y >= got.bar.y - 0.5 && b.b <= got.bar.b + 0.5 && b.x >= -0.5 && b.r <= got.vw + 0.5, `the line sits inside the top bar, on screen (${at}): ${JSON.stringify([b, got.bar])}`);
        assert(b.h <= 26, `the line is small (${at}): ${b.h}px tall`);
        for (const o of got.others) assert(!(b.x < o.r - 0.5 && o.x < b.r - 0.5 && b.y < o.b - 0.5 && o.y < b.b - 0.5), `the line covers no tab or tool (${at}): ${o.name} ${JSON.stringify([b, o])}`);
        assert(got.label.w >= 30, `the line names the task (${at}): ${got.label.w}px`);
        // on the right, up against the tools, whatever room the tabs leave
        const next = got.others.filter(o => o.x >= b.r - 0.5 && o.y < b.b && o.b > b.y).sort((p, q) => p.x - q.x)[0];
        assert(next && next.x - b.r <= 24, `on the right, beside the tools (${at}): ${JSON.stringify([b, next])}`);
      }
      if (w >= 1500) assert(!got.tight && got.label.w >= 90, `on a wide screen the words show (${at})`);
      assert(got.bar.b < 140, `and the top bar is at the top (${at})`);
      assert.strictEqual(got.chipText, 'Order check failed', `a failed order check says so (${at})`);
      assert(/did not answer in time/.test(got.chipTitle) && /Orders received · 24h/.test(got.chipTitle), `and its tooltip says why and what was received (${at}): ${got.chipTitle}`);
      assert(/Talking to the cloud|Preparing 8 order line/.test(got.title) && /\d+s/.test(got.title), `the line's tooltip has the task and its time (${at}): ${got.title}`);
      assert.strictEqual(got.chipAfter, false, `the chip goes when the checks work again (${at})`);
    }
  } finally { await browser.close(); server.close(); }
  console.log('top status OK · the progress line and the orders chip sit small in the top bar, nothing is left at the bottom, the station frame stays laid out unseen, 1500 to 420 px');
})().catch(e => { console.error(e); process.exit(1); });
