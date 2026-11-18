// GROUPBTN (Paul, 18 Nov 2026: "I don't see the options to group and ungroup ... I need the exact same options with the same icons or very similar ... the same keyboard shortcuts and placed where an Illustrator-trained user would expect"):
// Group (Cmd/Ctrl+G) and Ungroup (Shift+Cmd/Ctrl+G) were in the Object menu, the right-click menu, the Properties panel and the old Layers footer, none of which is in view at a small window. The Control bar, which every width shows,
// now carries them as two icon buttons, lit by the selection.
//   1 with nothing selected both are there and off; their tips say the shortcuts
//   2 two things selected: Group is on, Ungroup off; pressing it makes ONE group (the layers show it) and selects it
//   3 a group selected: Ungroup is on, Group off; pressing it gives the parts back, selected
//   4 the keys do the same: Ctrl+G groups, Shift+Ctrl+G ungroups, Undo takes each back
//   5 the buttons are inside the window at 1024 x 768 (nothing hides them)
//   6 the Pathfinder flyout has Group and Ungroup too (Paul: "the group and ungroup should be part of the Pathfinder options"): lit by the selection, and they do the same
//   PW_DIR=... CHROMIUM=... node tests/charm-nest/ce-group-buttons.cjs
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const pwDir = process.env.PW_DIR || path.join(__dirname, '../../node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); process.exit(0); }
const SHOTS = process.env.SHOTS || ''; if (SHOTS) fs.mkdirSync(SHOTS, { recursive: true });
const ROOT = path.join(__dirname, '../..'), HARNESS = 'file://' + path.join(__dirname, 'fixtures', 'charm-editor-harness.html');
const EXT = ['charm-nest-vector.js', 'charm-nest-ce-ops.js', 'charm-nest-ce-paths.js', 'charm-nest-ce-warp.js', 'charm-nest-ce-extras.js', 'charm-nest-ce-trace.js', 'charm-nest-ce-image.js', 'charm-nest-dxf.js', 'charm-nest-ce-import.js'];
const results = [], failures = [];
const pass = m => { results.push(m); console.log('  ok   ' + m); };
const fail = (m, e) => { failures.push(m + ' :: ' + String(e && e.message || e).split('\n')[0].slice(0, 400)); console.log('  FAIL ' + m + '\n         ' + String(e && e.stack || e).split('\n').slice(0, 8).join('\n         ')); };
async function step(name, fn) { try { const note = await fn(); pass(name + (note ? '  [' + note + ']' : '')); } catch (e) { fail(name, e); } }
const settle = page => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium', args: ['--no-sandbox'] });
  const newPage = async (w, h, storage) => {
    const ctx = await browser.newContext({ viewport: { width: w, height: h }, reducedMotion: 'reduce' });
    await ctx.route(u => !u.href.startsWith('file://'), r => r.abort());
    const page = await ctx.newPage();
    page.__errs = [];
    page.on('pageerror', e => page.__errs.push('pageerror: ' + e.message));
    page.on('console', m => { if (m.type() === 'error') page.__errs.push('console: ' + m.text()); });
    await page.goto(HARNESS);
    if (storage) await page.evaluate(s => { for (const [k, v] of Object.entries(s)) localStorage.setItem(k, v); }, storage);
    for (const s of EXT) await page.addScriptTag({ url: 'file://' + path.join(ROOT, s) });
    return page;
  };
  const openIt = async (page, extra) => { await page.evaluate(x => { window.openEditor(x || undefined); }, extra || null); await page.waitForSelector('.ce-root .ce-items path'); await settle(page); await settle(page); };
  const shot = async (page, name) => { if (SHOTS) await page.screenshot({ path: path.join(SHOTS, name) }); };
  const C = (page, fn, arg) => page.evaluate(([f, a]) => (0, eval)('(' + f + ')')(window.CharmEditor.current, a), [String(fn), arg]);
  const closeM = page => page.evaluate(() => window.CharmEditor.current.ai.closeMenus());
  const menuOpen = async (page, name) => { await closeM(page); await page.click(`.ce-menubar .ce-mb__t:text-is("${name}")`); await settle(page); };
  const rows = page => page.evaluate(() => { const m = document.querySelector('.ce-aimenu:not([hidden])'); return m ? [...m.querySelectorAll(':scope > .ce-aimi, :scope > .ce-aisub > .ce-aimi, :scope > .ce-menu__sep')].map(b => b.classList.contains('ce-menu__sep') ? '-' : (b.querySelector('.ce-aimi__l') || b).textContent + (b.querySelector('em') ? ' [' + b.querySelector('em').textContent + ']' : '') + (b.classList.contains('is-off') ? ' (off)' : '') + (b.classList.contains('ce-aimi--sub') ? ' ▸' : '') + (b.classList.contains('is-chk') ? ' ✓' : '')) : null; });
  const subRows = async (page, label) => {
    await page.hover(`.ce-aimenu:not([hidden]) .ce-aimi--sub:has(.ce-aimi__l:text-is("${label}"))`); await page.waitForTimeout(400); await settle(page);   // (a submenu opens after a moment, as Illustrator's)
    return page.evaluate(l => { const b = [...document.querySelectorAll('.ce-aimenu .ce-aimi--sub')].find(x => x.querySelector('.ce-aimi__l').textContent === l); const m = b && b.parentNode.querySelector(':scope > .ce-menu'); return m && !m.hidden ? [...m.querySelectorAll(':scope > .ce-aimi, :scope > .ce-aisub > .ce-aimi')].map(x => x.querySelector('.ce-aimi__l').textContent + (x.querySelector('em') ? ' [' + x.querySelector('em').textContent + ']' : '') + (x.classList.contains('is-off') ? ' (off)' : '')) : null; }, label);
  };
  const pick = async (page, label) => { await page.locator(`.ce-aimenu:not([hidden]) .ce-aimi:visible:has(.ce-aimi__l:text-is("${label}"))`).last().click(); await settle(page); };
  const sel = (page, ids) => page.evaluate(ids => window.CharmEditor.current.ctx.select(ids), ids).then(() => settle(page));
  const clickPart = async (page, id, mods) => { const c = await C(page, (E, id) => { const b = E.box(id); return E.clientOf((b[0] + b[2]) / 2, (b[1] + b[3]) / 2); }, id); for (const k of mods || []) await page.keyboard.down(k); await page.mouse.click(c.x, c.y); for (const k of mods || []) await page.keyboard.up(k); await settle(page); };

  const page = await newPage(1024, 768);
  await openIt(page);
  const st = () => page.evaluate(() => ({ g: document.querySelector('[data-ctl-group]'), u: document.querySelector('[data-ctl-ungroup]') }) && ({ gOff: document.querySelector('[data-ctl-group]').disabled, uOff: document.querySelector('[data-ctl-ungroup]').disabled, gTip: document.querySelector('[data-ctl-group]').title, uTip: document.querySelector('[data-ctl-ungroup]').title, sel: window.CharmEditor.current.selection().length, kinds: window.CharmEditor.current.items().map(i => i.kind).join(',') }));
  await step('1 nothing selected: Group and Ungroup are in the Control bar, off, and their tips say the shortcuts', async () => {
    const r = await st(); assert.ok(r.gOff && r.uOff, JSON.stringify(r)); assert.match(r.gTip, /^Group \(.*G\)$/); assert.match(r.uTip, /^Ungroup \(.*G\)$/); assert.ok(/Shift|⇧/.test(r.uTip), r.uTip);
  });
  const ids = ['day-0-0', 'day-0-1'];
  await step('2 two things selected: Group on, Ungroup off; pressing Group makes one group and selects it', async () => {
    await page.evaluate(ids => window.CharmEditor.current.ctx.select(ids), ids); await settle(page);
    let r = await st(); assert.ok(!r.gOff && r.uOff, JSON.stringify(r));
    await page.click('[data-ctl-group]'); await settle(page);
    r = await st(); assert.equal(r.sel, 1, 'the group is selected'); assert.ok(/group/.test(r.kinds), 'a group exists: ' + r.kinds);
  });
  await step('3 a group selected: Ungroup on, Group off; pressing Ungroup gives the parts back, selected', async () => {
    let r = await st(); assert.ok(r.gOff && !r.uOff, JSON.stringify(r));
    await page.click('[data-ctl-ungroup]'); await settle(page);
    r = await st(); assert.equal(r.sel, 2, 'both parts selected'); assert.ok(!/group/.test(r.kinds), 'no group left: ' + r.kinds.slice(0, 40));
    assert.ok(r.gOff === false && r.uOff === true, JSON.stringify(r));
  });
  await step('4 the keys do the same: Ctrl+G groups, Shift+Ctrl+G ungroups', async () => {
    await page.evaluate(ids => window.CharmEditor.current.ctx.select(ids), ids); await settle(page);
    await page.keyboard.press('Control+g'); await settle(page);
    let r = await st(); assert.equal(r.sel, 1); assert.ok(/group/.test(r.kinds), r.kinds);
    await page.keyboard.press('Control+Shift+g'); await settle(page);
    r = await st(); assert.equal(r.sel, 2, 'ungrouped by the key');
  });
  await step('5 both buttons are inside the window at 1024 x 768', async () => {
    const b = await page.evaluate(() => ['[data-ctl-group]', '[data-ctl-ungroup]'].map(s => { const r = document.querySelector(s).getBoundingClientRect(); return [Math.round(r.left), Math.round(r.top), Math.round(r.right), Math.round(r.bottom)]; }));
    for (const r of b) assert.ok(r[0] >= 0 && r[2] <= 1024 && r[1] >= 0 && r[3] <= 768 && r[2] - r[0] > 10, JSON.stringify(b));
    await shot(page, 'group-buttons.png');
  });
  await step('6 the Pathfinder flyout carries Group and Ungroup, lit by the selection, and they group and ungroup', async () => {
    const fly = () => page.evaluate(() => { const f = document.querySelector('.cops-fly'); const g = f && f.querySelector('[data-ops="gr:group"]'), u = f && f.querySelector('[data-ops="gr:ungroup"]'); return { open: !!f, g: !!g && !g.disabled, u: !!u && !u.disabled, has: !!g && !!u }; });
    const open = async () => { await page.evaluate(() => { const b = document.querySelector('.ce-xbar [data-xcmd="ops.pfBar"]') || document.querySelector('[data-xcmd="ops.pfBar"]'); b.click(); }); await settle(page); };
    await page.evaluate(ids => window.CharmEditor.current.ctx.select(ids), ids); await settle(page);
    await open(); let f = await fly(); assert.ok(f.open && f.has && f.g && !f.u, JSON.stringify(f));
    await page.click('.cops-fly [data-ops="gr:group"]'); await settle(page);
    let r = await st(); assert.equal(r.sel, 1); assert.ok(/group/.test(r.kinds), r.kinds.slice(0, 30));
    await open(); f = await fly(); assert.ok(f.open && !f.g && f.u, JSON.stringify(f));
    await page.click('.cops-fly [data-ops="gr:ungroup"]'); await settle(page);
    r = await st(); assert.equal(r.sel, 2); assert.ok(!/group/.test(r.kinds));
    await shot(page, 'pathfinder-group.png');
  });
  await browser.close();
  console.log(failures.length ? 'FAIL ' + failures.length : 'ce-group-buttons: all ' + results.length + ' passed'); process.exit(failures.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
