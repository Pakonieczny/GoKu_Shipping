// The shared line-icon set (charm-nest-icons.js, Paul 12 Oct 2026: "strong iconography so a person can tie a fact to the thing it is about").
// Offline and pure: nothing is fetched, nothing is written live. It proves
//   1 every required name is there, and each icon is a well-formed inline svg on the one grid (viewBox 0 0 24 24, no fill, stroke
//     currentColor, 1.6 wide, round caps and joins, class "ic ic-<name>"), small, with no fixed colour anywhere, and each one different;
//   2 an unknown name (or one that is only a property of every object) gives "" and has() false; the size helpers (a number, a key of
//     sizes, a bad value) and the title (role img, label, tooltip, escaped; without it aria-hidden) behave as written;
//   3 the module loads both ways (require, and as window.CharmNestIcons in a page), and the sorter page loads it right before the
//     bridge with its own cache token while the served-file list names it;
//   4 (when a playwright-core directory is given) in headless Chromium every icon really draws, and takes the colour of its text.
//   node tests/charm-nest/icons.cjs [playwright-core dir]   (CN_SHOTS=dir saves the contact sheet, light and dark, 16 and 24 px)
const fs = require('fs'), path = require('path'), vm = require('vm'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const src = fs.readFileSync(path.join(root, 'charm-nest-icons.js'), 'utf8');
const Icons = require(path.join(root, 'charm-nest-icons.js'));

const REQUIRED = ['order', 'buyer', 'purchased', 'shipby', 'message', 'note', 'metal', 'listing', 'title', 'quantity', 'pair', 'piece', 'mirror', 'state', 'sheet',
  'qr', 'link', 'copy', 'check', 'alert', 'hold', 'clock', 'info', 'chevron-down', 'chevron-right', 'more', 'gift', 'engrave', 'custom', 'cut', 'flag', 'eye', 'lock'];

let pass = 0, fail = 0;
async function t(name, fn) { try { await fn(); pass++; console.log('ok   ' + name); } catch (e) { fail++; console.log('FAIL ' + name + '\n     ' + (e && e.message || e).split('\n').join('\n     ')); } }

/** A small well-formedness check for the svg strings: every tag closed in order, attributes quoted, nothing left over. */
function wellFormed(s) {
  const stack = []; let i = 0;
  const tag = /<(\/?)([a-zA-Z][\w:-]*)((?:\s+[a-zA-Z:-]+="[^"<]*")*)\s*(\/?)>/y;
  while (i < s.length) {
    if (s[i] === '<') {
      tag.lastIndex = i; const m = tag.exec(s); if (!m) return 'bad tag at ' + i + ': ' + s.slice(i, i + 40);
      if (m[1]) { if (stack.pop() !== m[2]) return 'closing </' + m[2] + '> out of order'; } else if (!m[4]) stack.push(m[2]);
      i = tag.lastIndex;
    } else { const j = s.indexOf('<', i); const text = s.slice(i, j < 0 ? s.length : j); if (/[<>]|&(?!(?:amp|lt|gt|quot|#39);)/.test(text)) return 'raw text: ' + text; i = j < 0 ? s.length : j; }
  }
  return stack.length ? 'unclosed <' + stack[stack.length - 1] + '>' : '';
}
const attr = (s, a) => { const m = new RegExp('<svg[^>]*?\\s' + a + '="([^"]*)"').exec(s); return m ? m[1] : null; };
const inner = s => s.replace(/^<svg[^>]*>/, '').replace(/<\/svg>$/, '');

(async () => {
  await t('1a every required name is in the set, once, and names / has() agree', () => {
    for (const n of REQUIRED) { assert.ok(Icons.has(n), 'missing ' + n); assert.ok(Icons.names.includes(n), n + ' not in names'); }
    assert.equal(new Set(Icons.names).size, Icons.names.length, 'a name twice');
    for (const n of Icons.names) assert.ok(Icons.has(n) && Icons.svg(n) !== '', n);
    assert.ok(Object.isFrozen(Icons.names), 'names is frozen');
  });
  await t('1b each icon is a well-formed svg on the one grid, the one weight, round, classed', () => {
    for (const n of Icons.names) {
      const s = Icons.svg(n); assert.equal(wellFormed(s), '', n + ': ' + wellFormed(s));
      assert.ok(s.startsWith('<svg ') && s.endsWith('</svg>'), n);
      assert.equal(attr(s, 'viewBox'), '0 0 24 24', n); assert.equal(attr(s, 'fill'), 'none', n); assert.equal(attr(s, 'stroke'), 'currentColor', n);
      assert.equal(attr(s, 'stroke-width'), '1.6', n); assert.equal(attr(s, 'stroke-linecap'), 'round', n); assert.equal(attr(s, 'stroke-linejoin'), 'round', n);
      assert.equal(attr(s, 'class'), 'ic ic-' + n, n); assert.ok(inner(s).length > 10, n + ' is empty');
    }
  });
  await t('1c no fixed colour: every fill / stroke is none or currentColor, no style, no colour word, no hex, no gradient, no script', () => {
    for (const n of Icons.names) {
      const s = Icons.svg(n);
      for (const m of s.matchAll(/\s(fill|stroke)="([^"]*)"/g)) assert.ok(m[2] === 'none' || m[2] === 'currentColor', n + ' ' + m[1] + '=' + m[2]);
      assert.ok(!/style=|#[0-9a-f]{3,8}\b|rgb|hsl|url\(|<script|<style|<defs|gradient|on\w+=/i.test(s), n + ' carries a fixed paint or a script');
    }
  });
  await t('1d each icon is small (under 500 bytes of drawing) and no two icons are the same drawing', () => {
    const seen = new Map();
    for (const n of Icons.names) {
      const body = inner(Icons.svg(n)); assert.ok(Buffer.byteLength(body) < 500, n + ' is ' + Buffer.byteLength(body) + ' bytes');
      assert.ok(!seen.has(body), n + ' draws the same as ' + seen.get(body)); seen.set(body, n);
    }
  });
  await t('2a an unknown name gives "" and has() false, and so does anything that is only a property of every object', () => {
    for (const n of ['', 'nope', 'Order', 'order ', 'chevron', 'constructor', '__proto__', 'toString', 'hasOwnProperty', 'valueOf', null, undefined, 7, {}, [], ['order']]) {
      assert.equal(Icons.svg(n), '', String(n)); assert.equal(Icons.svg(n, { size: 20, title: 'x' }), '', String(n)); assert.equal(Icons.has(n), false, String(n));
    }
    assert.equal(Icons.svg(), '');
  });
  await t('2b sizes: 16 by default, a number, a key of sizes, a numeric string, and a bad value falls back to 16', () => {
    const size = s => [attr(s, 'width'), attr(s, 'height')];
    assert.deepEqual(size(Icons.svg('order')), ['16', '16']); assert.deepEqual(size(Icons.svg('order', {})), ['16', '16']); assert.deepEqual(size(Icons.svg('order', null)), ['16', '16']);
    assert.deepEqual(size(Icons.svg('order', { size: 24 })), ['24', '24']); assert.deepEqual(size(Icons.svg('order', { size: 13.5 })), ['13.5', '13.5']);
    assert.deepEqual(size(Icons.svg('order', { size: '20' })), ['20', '20']);
    for (const [k, v] of Object.entries(Icons.sizes)) assert.deepEqual(size(Icons.svg('order', { size: k })), [String(v), String(v)], k);
    assert.deepEqual(Icons.sizes, { xs: 12, sm: 14, md: 16, lg: 20, xl: 24 });
    for (const bad of [0, -4, NaN, Infinity, '', 'huge', 'constructor', 9999, {}, [], true]) assert.deepEqual(size(Icons.svg('order', { size: bad })), ['16', '16'], String(bad));
    assert.equal(Icons.px('lg'), 20); assert.equal(Icons.px(18), 18); assert.equal(Icons.px('x'), 16);
    assert.equal(attr(Icons.svg('order', { size: 24 }), 'viewBox'), '0 0 24 24', 'the grid does not change with the size');
  });
  await t('2c a title makes it a labelled image with a hover tooltip (escaped); without one it is hidden from a screen reader', () => {
    const plain = Icons.svg('clock'); assert.equal(attr(plain, 'aria-hidden'), 'true'); assert.equal(attr(plain, 'focusable'), 'false');
    assert.ok(!/<title>|role=|aria-label/.test(plain)); assert.equal(Icons.svg('clock', { title: '' }), plain); assert.equal(Icons.svg('clock', { title: '   ' }), plain);
    const s = Icons.svg('clock', { title: 'Ship by 14 Oct' });
    assert.equal(attr(s, 'role'), 'img'); assert.equal(attr(s, 'aria-label'), 'Ship by 14 Oct'); assert.ok(s.includes('<title>Ship by 14 Oct</title>')); assert.ok(!/aria-hidden/.test(s));
    const e = Icons.svg('message', { title: `"Rush" <b>& 'now'</b>` });
    assert.equal(wellFormed(e), '', wellFormed(e)); assert.ok(!/<b>/.test(e), 'the title is escaped'); assert.ok(e.includes('&lt;b&gt;&amp;') && e.includes('&quot;Rush&quot;') && e.includes('&#39;now&#39;'));
  });
  await t('2d the weight and an extra class can be asked for; nothing else in the markup changes with them', () => {
    const s = Icons.svg('flag', { weight: 2, cls: 'owFactIc big' }); assert.equal(attr(s, 'stroke-width'), '2'); assert.equal(attr(s, 'class'), 'ic ic-flag owFactIc big');
    assert.equal(attr(Icons.svg('flag', { weight: -1 }), 'stroke-width'), '1.6'); assert.equal(attr(Icons.svg('flag', { weight: 'x' }), 'stroke-width'), '1.6');
    assert.equal(attr(Icons.svg('flag', { cls: '"><script>' }), 'class'), 'ic ic-flag', 'a class with markup in it is dropped');
    assert.equal(inner(Icons.svg('flag', { weight: 2 })), inner(Icons.svg('flag')));
    assert.equal(Icons.svg('flag'), Icons.svg('flag'), 'the same call gives the same string');
  });
  await t('3a loaded as a page script it makes window.CharmNestIcons; the same module, and no document is needed', () => {
    const win = {}; win.self = win; vm.runInNewContext(src, win, { filename: 'charm-nest-icons.js' });
    assert.ok(win.CharmNestIcons && typeof win.CharmNestIcons.svg === 'function'); assert.equal(win.CharmNestIcons.svg('lock'), Icons.svg('lock'));
    assert.deepEqual([...win.CharmNestIcons.names], [...Icons.names]); assert.equal(win.CharmNestIcons.has('lock'), true); assert.equal(win.CharmNestIcons.has('nope'), false);
  });
  await t('3b the sorter page loads it right before the bridge with its own token, keeping every other token; the served list names it', () => {
    const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
    const tags = [...html.matchAll(/<script src="([^"?]+)(\?v=[^"]*)?"><\/script>/g)];
    const at = f => tags.findIndex(m => m[1] === f);
    assert.ok(at('charm-nest-icons.js') >= 0, 'the page has no script tag for the icons'); assert.equal(tags.filter(m => m[1] === 'charm-nest-icons.js').length, 1);
    assert.equal(at('charm-nest-icons.js') + 1, at('charm-nest-bridge.js'), 'the icons are the tag right before the bridge');
    assert.match(tags[at('charm-nest-icons.js')][2] || '', /^\?v=2026\d{4}-[a-z0-9-]+$/, 'a fresh ?v= token');
    assert.match(tags[at('charm-nest-bridge.js')][2], /^\?v=20261005-lis2-cardinal4-.*-sui1(?:-[a-z0-9]+)*$/, 'the bridge token keeps its earlier suffixes');
    const list = fs.readFileSync(path.join(root, 'scripts/build-public.cjs'), 'utf8'); assert.ok(list.includes('"charm-nest-icons.js"'), 'scripts/build-public.cjs does not serve it');
    assert.ok(/\.ic\{[^}]*flex:none/.test(html), 'the page has the .ic base rule');
  });

  const pw = process.argv[2];
  if (pw) {
    await t('4 in headless Chromium every icon draws, takes the colour of its text, and the contact sheet is saved (light and dark, 16 and 24 px)', async () => {
      process.env.PLAYWRIGHT_BROWSERS_PATH = process.env.PLAYWRIGHT_BROWSERS_PATH || '/opt/pw-browsers';
      const { chromium } = require(path.resolve(pw));
      const themes = { light: { bg: '#f6f2ea', fg: '#1c1a17', mute: '#938c80' }, dark: { bg: '#1c1a17', fg: '#ece6da', mute: '#8d8678' } };
      const cell = n => `<div class="c"><div class="r">${Icons.svg(n, { size: 16 })}${Icons.svg(n, { size: 24 })}</div><div class="n">${n}</div></div>`;
      const panel = k => `<section id="${k}" style="background:${themes[k].bg};color:${themes[k].fg}"><h2 style="color:${themes[k].mute}">${k}</h2><div class="g">${Icons.names.map(cell).join('')}</div></section>`;
      const html = `<!doctype html><meta charset=utf-8><style>body{margin:0;font:12px/1.2 ui-monospace,Menlo,monospace;display:flex;width:1180px}section{flex:1;padding:18px 20px}
h2{margin:0 0 12px;font:600 12px ui-monospace;letter-spacing:.14em;text-transform:uppercase}.g{display:grid;grid-template-columns:repeat(4,1fr);gap:12px 10px}
.c{display:flex;flex-direction:column;gap:6px;padding:8px 10px;border:1px solid rgba(128,120,104,.25);border-radius:8px}.r{display:flex;align-items:center;gap:14px}.n{font-size:10.5px;opacity:.7}
#red{position:absolute;left:-9999px;top:0;color:#c00}.ic{flex:none}</style>${panel('light')}${panel('dark')}<div id="red">${Icons.names.map(n => Icons.svg(n, { size: 24 })).join('')}</div>`;
      const b = await chromium.launch();
      try {
        const p = await b.newPage({ deviceScaleFactor: 2, viewport: { width: 1180, height: 600 } }); await p.setContent(html);
        const r = await p.evaluate(() => {
          const out = []; const names = [...document.querySelectorAll('#light .ic')].map(s => s.classList[1].slice(3));
          for (const s of document.querySelectorAll('#light .ic, #dark .ic')) { const b = s.getBBox(); const cs = getComputedStyle(s); out.push({ n: s.classList[1], w: s.getBoundingClientRect().width, bw: b.width, bh: b.height, stroke: cs.stroke, color: cs.color }); }
          const red = [...document.querySelectorAll('#red .ic')].map(s => getComputedStyle(s).stroke);
          return { out, red, names };
        });
        for (const o of r.out) { assert.ok(Math.max(o.bw, o.bh) > 4, o.n + ' draws nothing (box ' + o.bw + 'x' + o.bh + ')'); assert.ok(o.bw <= 24.01 && o.bh <= 24.01, o.n + ' leaves the grid'); assert.equal(o.stroke, o.color, o.n + ' does not take its text colour'); }
        assert.ok(r.red.every(c => c === 'rgb(204, 0, 0)'), 'an icon in red text is not red');
        assert.equal(r.out.length, Icons.names.length * 4);
        // and in pixels: each icon (24 px, light) differs from an empty box of the same size
        const blank = await p.evaluate(() => { const d = document.createElement('div'); d.id = 'blank'; d.style.cssText = 'width:24px;height:24px'; document.querySelector('#light .c').appendChild(d); return true; });
        const empty = await p.locator('#blank').screenshot(); await p.evaluate(() => document.getElementById('blank').remove());
        for (const n of Icons.names) { const shot = await p.locator('#light .ic-' + n).nth(1).screenshot(); assert.ok(!shot.equals(empty), n + ' paints no pixel'); }
        if (process.env.CN_SHOTS) { fs.mkdirSync(process.env.CN_SHOTS, { recursive: true }); await p.screenshot({ path: path.join(process.env.CN_SHOTS, 'ICONS-sheet.png'), fullPage: true }); }
      } finally { await b.close(); }
    });
  }
  console.log(`\n${pass} passed, ${fail} failed`); process.exit(fail ? 1 : 0);
})();
