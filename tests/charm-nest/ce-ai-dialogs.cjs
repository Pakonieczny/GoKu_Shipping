/* THE CHARM EDITOR'S DIALOGS AS ILLUSTRATOR'S (charm-nest-ce-ai-dialogs.js; Paul, 28 Oct 2026: "replicate the UI for illustrator ... I'm not looking for you to change the
 * functionality of things"). No network, no live service: the editor's harness page served from 127.0.0.1 with the files whose dialogs these are.
 *
 *   A  (node) the pure parts: which dialog is which, the ↑ ↓ step of a field (a tenth of a mm, a whole %, °, step; ⇧ ten times)
 *   B  each dialog drawn as Illustrator's: marked aid--<kind>, Medium Dark, the name in a title bar across the top, the buttons Spectrum's pills with no capitals, the default
 *      (Enter) blue and at the right, Cancel to its left, the Preview checkbox at the bottom left on the buttons' row; a screenshot of each (SHOTS=<dir>)
 *        B1 Offset Path… (ops): ↑ and ⇧↑ step its field (0.5 → 0.55 → 1.05 mm), Enter applies it (one undo step, as before)
 *        B2 Move… (the transform dialog, its tabs), Copy left of Cancel / OK; Esc cancels (nothing changes)
 *        B3 Simplify: its Apply says OK (the same hook), Preview at the left; Esc leaves the paths as they were
 *        B4 Warp Options: labels with colons on the left, ↑ on a number field with no steps of its own steps it (and its slider) by 1, ⇧↑ by 10; Esc cancels
 *        B5 the Image step (Image Trace): its card, the side drawn as Illustrator's panel, Place the default; Esc cancels
 *        B6 the laser check's list: a titled dark list
 *        B7 Leave without saving?: an alert, Keep editing the default (Enter: it stays open), Discard at the left; Esc keeps editing
 *        B8 Not saved yet (a save the caller refuses): one blue button
 *        B11 Send to sheet… opened over the editor: the page's <dialog> as an Illustrator dialog
 *        B9 Make blend… (Blend Options): Steps steps by 1, ⇧ by 10      B10 an .svg dropped: the import's roles card as Illustrator's Place options
 *   C  nothing on the page went wrong; the dialogs' hooks (data-db, data-x, data-cw-act, data-do, data-ask) are all still there
 *   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/ce-ai-dialogs.cjs     (SHOTS=<dir> keeps the screenshots)
 */
'use strict';
const assert = require('node:assert/strict'), path = require('node:path'), fs = require('node:fs'), http = require('node:http');
const ROOT = path.join(__dirname, '..', '..');
const D = require(path.join(ROOT, 'charm-nest-ce-ai-dialogs.js'));
let n = 0; const failures = [];
const ok = (c, label) => { assert.ok(c, label); n++; console.log('  ok   ' + label); };

/* ── A · the pure parts ── */
ok(D.stepFor('mm', '0.5', false) === 0.1 && D.stepFor('mm', '0.5', true) === 1 && D.stepFor('%', '50', false) === 1 && D.stepFor('°', '30', true) === 10 && D.stepFor('', '12', false) === 1 && D.stepFor('', '1.25', false) === 0.1,
  'A1 a field steps a tenth of a mm, a whole %, ° or count, ⇧ ten times');
ok(D.stepValue('0.5', 1, 0.1) === '0.6' && D.stepValue('1,5', -1, 1) === '0.5' && D.stepValue('−2', 1, 0.1) === '-1.9' && D.stepValue('', 1, 1) === '1', 'A2 the value stepped as typed (comma, minus sign, empty)');
ok(D.KINDS.length >= 10 && D.KINDS.some(k => k[1] === 'newdoc') && D.KINDS.some(k => k[1] === 'sendsheet'), 'A3 every dialog of the editor has its kind');

(async () => {
  let chromium; try { ({ chromium } = require(path.join(process.env.PW_DIR || path.join(ROOT, 'node_modules'), 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser pass was not run'); return; }
  const types = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png' };
  const server = http.createServer((req, res) => {
    const p = path.normalize(path.join(ROOT, decodeURIComponent(req.url.split('?')[0])));
    if (!p.startsWith(ROOT) || !fs.existsSync(p) || fs.statSync(p).isDirectory()) { res.writeHead(404); res.end(); return; }
    res.writeHead(200, { 'content-type': types[path.extname(p)] || 'application/octet-stream' }); fs.createReadStream(p).pipe(res);
  });
  await new Promise(r => server.listen(0, '127.0.0.1', r));
  const base = `http://127.0.0.1:${server.address().port}/`;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1243/chrome-linux64/chrome', args: ['--no-sandbox'] });
  const SHOTS = process.env.SHOTS || ''; if (SHOTS) fs.mkdirSync(SHOTS, { recursive: true });
  let page = null;
  const tidy = () => (page ? page.evaluate(() => { for (const s of ['.cops-modal [data-db="cancel"]', '.cep-pop [data-x="no"]', '.cw-panel [data-cw-act="cancel"]', '.cei [data-do="cancel"]', '.ce-ask [data-ask="no"]', '.ce-refuse [data-ask]']) { const b = document.querySelector(s); if (b) b.click(); } }).catch(() => {}) : null);
  const step = async (name, fn) => { try { await fn(); n++; console.log('  ok   ' + name); } catch (e) { failures.push(name); console.log('  FAIL ' + name + '\n         ' + String(e && e.stack || e).split('\n').slice(0, 12).join('\n         ')); await tidy(); } };
  try {
    const ctxB = await browser.newContext({ viewport: { width: 1380, height: 860 }, reducedMotion: 'reduce' });
    await ctxB.route(u => !u.href.startsWith(base), r => r.abort());
    page = await ctxB.newPage(); page.setDefaultTimeout(20000);
    const errs = []; page.on('pageerror', e => errs.push(e.message)); page.on('console', m => { if (m.type() === 'error') errs.push(m.text()); });
    await page.goto(base + 'tests/charm-nest/fixtures/charm-editor-harness.html');
    for (const f of ['vendor/pdf-lib-1.17.1.min.js', 'charm-nest-geom.js', 'charm-nest-pdf.js', 'charm-nest-art-edit.js', 'charm-nest-dxf.js', 'charm-nest-ce-import.js', 'charm-nest-vector.js', 'charm-nest-ce-ops.js', 'charm-nest-ce-paths.js', 'charm-nest-ce-warp.js', 'charm-nest-ce-extras.js', 'charm-nest-ce-trace.js', 'charm-nest-ce-image.js', 'charm-nest-ce-ai-dialogs.js']) await page.addScriptTag({ url: base + f });
    const settle = () => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
    const shot = async (name, sel) => { if (!SHOTS) return; const el = sel ? await page.$(sel) : null; await (el || page).screenshot({ path: path.join(SHOTS, name + '.png') }); };
    const open = async extra => { await page.evaluate(x => { window.openEditor(x); }, extra || null); await page.waitForSelector('.ce-root .ce-items path'); await settle(); };
    const hist = () => page.evaluate(() => window.CharmEditor.current.history().undo);
    /** A command run as its menu item runs it (the editor's own data-xcmd hook: one undo step when it ends), with `sel` selected first. */
    const run = (id, sel) => page.evaluate(([id, sel]) => {
      const C = window.CharmEditor; if (sel) C.current.ctx.select(sel);
      const b = document.createElement('button'); b.type = 'button'; b.hidden = true; b.dataset.xcmd = id; document.querySelector('.ce-root').appendChild(b); b.click(); b.remove();
    }, [id, sel || null]);
    /** What the eye sees of a dialog: its kind, its card's colour, its title bar, its buttons in the order they stand (left to right) and how they look, the Preview box's place. */
    const look = (rootSel, cardSel, titleSel, btnSel, prevSel) => page.evaluate(([rootSel, cardSel, titleSel, btnSel, prevSel]) => {
      const r = document.querySelector(rootSel); if (!r) return null;
      const card = cardSel ? r.querySelector(cardSel) || r : r, t = titleSel ? r.querySelector(titleSel) : null, cs = e => getComputedStyle(e);
      const bs = [...r.querySelectorAll(btnSel)].filter(b => b.offsetParent).map(b => { const c = cs(b), bb = b.getBoundingClientRect(); return { text: b.textContent.trim(), x: Math.round(bb.left), y: Math.round(bb.top + bb.height / 2), bg: c.backgroundColor, tt: c.textTransform, ls: c.letterSpacing, rad: parseFloat(c.borderTopLeftRadius) }; }).sort((a, b) => a.x - b.x);
      const p = prevSel ? r.querySelector(prevSel) : null, pb = p && p.getBoundingClientRect();
      const tb = t && t.getBoundingClientRect(), cb = card.getBoundingClientRect();
      return { cls: r.className, bg: cs(card).backgroundColor, title: t ? { text: t.textContent.trim(), align: cs(t).textAlign, top: Math.round(tb.top - cb.top), w: Math.round(tb.width), cw: Math.round(cb.width), bg: cs(t).backgroundColor, size: cs(t).fontSize } : null,
        buttons: bs, prev: pb ? { x: Math.round(pb.left), y: Math.round(pb.top + pb.height / 2) } : null };
    }, [rootSel, cardSel, titleSel, btnSel, prevSel]);
    const BLUE = 'rgb(55, 142, 240)', GREY = 'rgb(83, 83, 83)';
    const sameRow = (a, b) => Math.abs(a.y - b.y) <= 6;
    await open();

    await step('B1 Offset Path… is an Illustrator dialog: aid--ops, Medium Dark, its name in a title bar across the top; Preview at the bottom left on the buttons\' row; Cancel then OK (blue, the default) at the right, pills with no capitals; ↑ and ⇧↑ step its field by its own step and ten of them (0.5 → 0.55 → 1.05 mm); Enter applies it, one undo step', async () => {
      const h0 = await hist();
      await run('ops.offset', ['heart-fill']); await page.waitForSelector('.cops-modal .cops-card'); await settle();
      const L = await look('.cops-modal', '.cops-card', '.cops-card > h3', '.ce-ask__b button', '.cops-check');
      assert.ok(/\baid--ops\b/.test(L.cls), L.cls); assert.equal(L.bg, GREY);
      assert.ok(L.title && L.title.text === 'Offset Path' && L.title.align === 'center' && L.title.top <= 1 && L.title.w >= L.title.cw - 2, JSON.stringify(L.title));
      const names = L.buttons.map(b => b.text); assert.deepEqual(names, ['Cancel', 'OK'], names.join());
      assert.equal(L.buttons[1].bg, BLUE); assert.ok(L.buttons.every(b => b.tt === 'none' && (b.ls === 'normal' || parseFloat(b.ls) === 0) && b.rad >= 10), JSON.stringify(L.buttons));
      assert.ok(L.prev && L.prev.x < L.buttons[0].x && sameRow(L.prev, L.buttons[0]), JSON.stringify(L.prev));
      const f = await page.$('.cops-card [data-dnum="mm"]'); await f.click(); await page.keyboard.press('Control+a'); await page.keyboard.type('0.5');
      await page.keyboard.press('ArrowUp'); const v1 = await f.inputValue(); await page.keyboard.press('Shift+ArrowUp'); const v2 = await f.inputValue();
      assert.equal(v1, '0.55'); assert.equal(v2, '1.05');   // (its own step, 0.05 mm; ⇧ ten of them)
      await shot('B1-offset-path', '.cops-card');
      await page.keyboard.press('Enter'); await settle();
      assert.equal(await page.$('.cops-modal'), null); assert.equal(await hist(), h0 + 1);
    });
    await step('B2 Move… (the transform dialog and its tabs): Copy, Cancel, OK in Illustrator\'s order (OK blue at the right); Esc cancels and nothing changes', async () => {
      const h0 = await hist();
      await run('ops.move', ['heart-fill']); await page.waitForSelector('.cops-modal .cops-card'); await settle();
      const L = await look('.cops-modal', '.cops-card', '.cops-card > h3', '.ce-ask__b button', '.cops-check');
      assert.deepEqual(L.buttons.map(b => b.text), ['Copy', 'Cancel', 'OK']); assert.equal(L.buttons[2].bg, BLUE);
      const labels = await page.$$eval('.cops-card .cops-f > span:first-child', els => els.map(e => getComputedStyle(e, '::after').content + '|' + getComputedStyle(e).textAlign));
      assert.ok(labels.length >= 2 && labels.every(x => x === '":"|right'), labels.join());
      await shot('B2-move', '.cops-card');
      await page.click('.cops-card [data-dtab="rotate"]'); await settle(); await shot('B2-rotate', '.cops-card');
      await page.keyboard.press('Escape'); await settle();
      assert.equal(await page.$('.cops-modal'), null); assert.equal(await hist(), h0);
    });
    await step('B3 Simplify: its Apply says OK (the same data-x hook), Preview at the left, Cancel then OK; Esc leaves the paths as they were', async () => {
      const h0 = await hist();
      await run('pathSimplify', ['heart-line']); await page.waitForSelector('.cep-pop'); await settle();
      const L = await look('.cep-pop', null, '.cep-pop > h4', '.cep-pop__b button', '.aid-prev');
      assert.ok(/\baid--simplify\b/.test(L.cls), L.cls);
      assert.deepEqual(L.buttons.map(b => b.text), ['Cancel', 'OK']); assert.equal(L.buttons[1].bg, BLUE);
      assert.ok(await page.$('.cep-pop [data-x="yes"]') && L.prev && L.prev.x < L.buttons[0].x && sameRow(L.prev, L.buttons[0]), JSON.stringify(L.prev));
      await shot('B3-simplify', '.cep-pop');
      await page.keyboard.press('Escape'); await settle();
      assert.equal(await page.$('.cep-pop'), null); assert.equal(await hist(), h0);
    });
    await step('B4 Warp Options: labels end in a colon at the left, Preview at the bottom left, Cancel then OK; ↑ on its Bend field steps it (and its slider) by 1, ⇧↑ by 10; Esc cancels', async () => {
      const h0 = await hist();
      await run('cw-warp', ['heart-fill']); await page.waitForSelector('.cw-panel'); await settle();
      const L = await look('.cw-panel', null, '.cw-head', '.cw-foot button', '.cw-check');
      assert.ok(/\baid--warp\b/.test(L.cls), L.cls); assert.equal(L.bg, GREY);
      assert.deepEqual(L.buttons.map(b => b.text), ['Cancel', 'OK']); assert.equal(L.buttons[1].bg, BLUE);
      assert.ok(L.prev && L.prev.x < L.buttons[0].x && sameRow(L.prev, L.buttons[0]), JSON.stringify(L.prev));
      const n0 = await page.$('.cw-panel input[data-cw-n="bend"]'); assert.ok(n0, 'the Bend field');
      const before = +(await n0.inputValue());
      await n0.click(); await page.keyboard.press('ArrowUp'); await settle();
      const a1 = await page.evaluate(() => [document.querySelector('.cw-panel input[data-cw-n="bend"]').value, document.querySelector('.cw-panel input[type=range][data-cw="bend"]').value]);
      await page.keyboard.press('Shift+ArrowUp'); await settle();
      const a2 = await page.evaluate(() => document.querySelector('.cw-panel input[type=range][data-cw="bend"]').value);
      assert.equal(+a1[0], before + 1); assert.equal(+a1[1], before + 1); assert.equal(+a2, Math.min(100, before + 11));
      await shot('B4-warp', '.cw-panel');
      await page.keyboard.press('Escape'); await settle();
      assert.equal(await page.$('.cw-panel'), null); assert.equal(await hist(), h0);
    });
    await step('B9 Make blend… (Blend Options): aid--warp, Spacing and Steps with colons, ↑ on Steps (no steps of its own) steps by 1 and ⇧↑ by 10; Esc cancels and nothing changes', async () => {
      const h0 = await hist();
      await run('cw-blend-make', ['day-0-0', 'day-0-6']); await page.waitForSelector('.cw-panel'); await settle();
      const L = await look('.cw-panel', null, '.cw-head', '.cw-foot button', '.cw-check');
      assert.ok(/\baid--warp\b/.test(L.cls), L.cls); assert.deepEqual(L.buttons.map(b => b.text), ['Cancel', 'OK']); assert.equal(L.buttons[1].bg, BLUE);
      const f = await page.$('.cw-panel input[data-cw-n="steps"]'); const v0 = +(await f.inputValue());
      await f.click(); await page.keyboard.press('ArrowUp'); const v1 = +(await f.inputValue()); await page.keyboard.press('Shift+ArrowUp'); const v2 = +(await f.inputValue());
      assert.equal(v1, v0 + 1); assert.equal(v2, v0 + 11);
      await shot('B9-blend-options', '.cw-panel');
      await page.keyboard.press('Escape'); await settle();
      assert.equal(await page.$('.cw-panel'), null); assert.equal(await hist(), h0);
    });
    await step('B10 an .svg dropped on the charm: its roles card drawn as Illustrator\'s Place options (aid--import, "Place" in its title bar, the default blue at the right); Esc cancels', async () => {
      const h0 = await hist();
      await page.evaluate(() => {
        const svg = '<svg xmlns="http://www.w3.org/2000/svg" width="20mm" height="20mm" viewBox="0 0 20 20"><circle cx="10" cy="10" r="8" fill="none" stroke="#000" stroke-width="0.2"/><rect x="6" y="6" width="8" height="8" fill="#000"/></svg>';
        const dt = new DataTransfer(); dt.items.add(new File([svg], 'badge.svg', { type: 'image/svg+xml' }));
        const st = document.querySelector('.ce-root .ce-stage'), r = st.getBoundingClientRect();
        st.dispatchEvent(new DragEvent('drop', { bubbles: true, cancelable: true, dataTransfer: dt, clientX: r.left + r.width / 2, clientY: r.top + r.height / 2 }));
      });
      await page.waitForSelector('.cei-root .cei-card [data-a]:not([data-a="cancel"])'); await settle(); await page.waitForTimeout(300);
      const L = await look('.cei-root', '.cei-card', '.cei-card > .cei-eye', '.cei-foot button', null);
      assert.ok(/\baid--import\b/.test(L.cls), L.cls); assert.equal(L.bg, GREY); assert.ok(L.title && L.title.align === 'center', JSON.stringify(L.title));
      const last = L.buttons[L.buttons.length - 1]; assert.ok(L.buttons[0].text === 'Cancel' && last.bg === BLUE, JSON.stringify(L.buttons));
      await shot('B10-place-svg', '.cei-card');
      await page.keyboard.press('Escape'); await settle();
      assert.equal(await page.$('.cei-root'), null); assert.equal(await hist(), h0);
    });
    await step('B5 the Image step as Illustrator\'s Image Trace: aid--image, its title bar, the side panel grey with titled sections, Cancel then Place on charm (blue, the default); Esc cancels', async () => {
      const h0 = await hist();
      await page.evaluate(async () => {
        const c = document.createElement('canvas'); c.width = 240; c.height = 180; const g = c.getContext('2d');
        g.fillStyle = '#fff'; g.fillRect(0, 0, 240, 180); g.fillStyle = '#222'; g.beginPath(); g.arc(80, 90, 50, 0, 7); g.fill(); g.fillStyle = '#888'; g.fillRect(140, 40, 70, 100);
        const blob = await new Promise(r => c.toBlob(r, 'image/png')); window.__run = window.CharmNestCeImage.run(window.CharmEditor.current.ctx, new File([blob], 'dot.png', { type: 'image/png' }));
      });
      await page.waitForSelector('.cei .cei__card'); await settle(); await page.waitForTimeout(600);
      const L = await look('.cei', '.cei__card', '.cei__head', '.cei__foot .ce-actions button', null);
      assert.ok(/\baid--image\b/.test(L.cls), L.cls); assert.equal(L.bg, GREY);
      assert.deepEqual(L.buttons.map(b => b.text), ['Cancel', 'Place on charm']); assert.equal(L.buttons[1].bg, BLUE);
      const side = await page.evaluate(() => getComputedStyle(document.querySelector('.cei__side')).backgroundColor); assert.equal(side, GREY);
      await shot('B5-image-trace', '.cei__card');
      await page.keyboard.press('Escape'); await settle();
      assert.equal(await page.$('.cei'), null); assert.equal(await hist(), h0);
    });
    await step('B6 the laser check: its list titled "Laser Check", Illustrator\'s dark menu', async () => {
      await run('cx.check'); await page.waitForFunction(() => { const l = document.querySelector('.cx-list'); return l && !l.hidden; }); await settle();
      const L = await page.evaluate(() => { const l = document.querySelector('.cx-list'); return { cls: l.className, bg: getComputedStyle(l).backgroundColor, t: getComputedStyle(l, '::before').content }; });
      assert.ok(/\baid--check\b/.test(L.cls) && L.bg === 'rgb(61, 61, 61)' && L.t === '"Laser Check"', JSON.stringify(L));
      await shot('B6-laser-check', '.cx-list');
      await page.keyboard.press('Escape'); await page.evaluate(() => { const l = document.querySelector('.cx-list'); if (l) l.hidden = true; }); await settle();
    });
    await step('B7 Leave without saving? is an Illustrator alert: the title bar, the mark at the left, Discard changes at the left, Keep editing (focused: Enter) blue at the right; Esc keeps editing, the work kept', async () => {
      await page.click('.ce-root [data-act="cancel"]'); await page.waitForSelector('.ce-ask:not(.cops-modal) .ce-ask__card'); await settle();
      const L = await look('.ce-ask:not(.cops-modal)', '.ce-ask__card', '.ce-ask__card > h3', '.ce-ask__b button', null);
      assert.ok(/\baid--alert\b/.test(L.cls), L.cls); assert.equal(L.bg, GREY); assert.equal(L.title.text, 'Leave without saving?');
      assert.deepEqual(L.buttons.map(b => b.text), ['Discard changes', 'Keep editing']); assert.equal(L.buttons[1].bg, BLUE);
      const order = await page.$$eval('.ce-ask__b [data-ask]', els => els.map(e => e.dataset.ask)); assert.deepEqual(order, ['no', 'yes']);   // (the hooks and their order as they were)
      assert.equal(await page.evaluate(() => document.activeElement && document.activeElement.dataset.ask), 'no');
      await shot('B7-leave-alert', '.ce-ask__card');
      await page.keyboard.press('Escape'); await settle();
      assert.equal(await page.$('.ce-ask'), null); assert.ok(await page.evaluate(() => !!window.CharmEditor.current && window.CharmEditor.current.dirty()));
    });
    await step('B8 Not saved yet (a save the caller refuses): the alert in red, its one button blue', async () => {
      await page.evaluate(() => window.CharmEditor.current.end(false)); await page.waitForFunction(() => !window.CharmEditor.current);
      await page.evaluate(() => { window.openEditor({ check: async () => ({ lines: [{ face: 'front', id: 'heart-fill', text: 'This heart is too small to engrave (0.3 mm)' }] }) }); });
      await page.waitForSelector('.ce-root .ce-items path'); await settle();
      await page.click('.ce-root [data-act="save"]'); await page.waitForSelector('.ce-refuse .ce-ask__card'); await settle();
      const L = await look('.ce-refuse', '.ce-ask__card', '.ce-ask__card > h3', '.ce-ask__b button', null);
      assert.ok(/\baid--alert\b/.test(L.cls) && L.title.text === 'Not saved yet', JSON.stringify(L.title)); assert.deepEqual(L.buttons.map(b => b.text), ['Keep editing']); assert.equal(L.buttons[0].bg, BLUE);
      await shot('B8-not-saved', '.ce-ask__card');
      await page.keyboard.press('Escape'); await settle();
      await page.evaluate(() => window.CharmEditor.current.end(false));
    });
    await step('B11 Send to sheet… over the editor: the page\'s <dialog> drawn as an Illustrator dialog (aid--sendsheet, Medium Dark, Cancel then Send to sheet blue at the right); Esc closes it', async () => {
      await open();
      // (the page's own dialog styles, as charm-nest-1.html has them: the harness page has none)
      const css = [...fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8').matchAll(/<style[^>]*>([\s\S]*?)<\/style>/g)].map(m => m[1]).join('\n');
      await page.addStyleTag({ content: css });
      await page.addScriptTag({ url: base + 'charm-nest-custom-piece.js' });
      await page.evaluate(() => { window.__cp = window.CharmNestCustomPiece.open({ designId: 'design-test', version: 1, design: { id: 'design-test', version: 1, name: 'Heart tag', faces: { front: { face: 'front', unitPt: true, frame: null, items: [] } } } }); });
      await page.waitForSelector('dialog.cpDlg[open]'); await settle(); await page.waitForTimeout(250);
      const L = await look('dialog.cpDlg', '.dlg', '.dlgHead', '.dlgFoot button', null);
      assert.ok(/\baid--sendsheet\b/.test(L.cls), L.cls); assert.equal(L.bg, GREY);
      assert.deepEqual(L.buttons.map(b => b.text), ['Cancel', 'Send to sheet']); assert.equal(L.buttons[1].bg, BLUE);
      await shot('B11-send-to-sheet', 'dialog.cpDlg .dlg');
      await page.keyboard.press('Escape'); await settle();
      assert.equal(await page.$('dialog.cpDlg[open]'), null);
      await page.evaluate(() => window.CharmEditor.current.end(false));
    });
    await step('C1 no errors on the page', async () => { assert.deepEqual(errs, []); });
  } finally { await browser.close(); server.close(); }
  console.log(`\nce-ai-dialogs: ${n} checks passed${failures.length ? `, ${failures.length} FAILED: ${failures.join(' | ')}` : ''}`);
  if (failures.length) process.exitCode = 1;
})().catch(e => { console.error(e); process.exitCode = 1; });
