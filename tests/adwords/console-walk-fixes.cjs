// What the browser walk (tests/adwords/browser) found, checked offline: toasts never take taps, phone controls
// are thumb-sized, dialogs and sheets ease in (not under reduced motion), panels ease to their new height in
// 150–400 ms, the country sheet keeps
// Tab inside it, the sales bar tooltip flips below near the top, image alt text reads as words, the re-image
// progress line uses what the engine sends, and the sign-in page neither scrolls sideways nor replaces the
// console with the shop. Synthetic data only.
const assert = require('assert/strict'), fs = require('fs'), path = require('path'), vm = require('vm'), { JSDOM } = require('jsdom');
const REPO = path.resolve(__dirname, '../..');
const html = fs.readFileSync(path.join(REPO, 'brites-adwords.html'), 'utf8'), groups = fs.readFileSync(path.join(REPO, 'brites-groups.js'), 'utf8');
const css = [...html.matchAll(/<style[^>]*>([\s\S]*?)<\/style>/g)].map(m => m[1]).join('\n');
const markup = html.replace(/<script[\s\S]*?<\/script>/g, '').replace(/<style[\s\S]*?<\/style>/g, '');
function pick(name) { const m = new RegExp('^(?:async )?function ' + name + '\\(', 'm').exec(html); assert(m, name); const rest = html.slice(m.index), next = /\n(?:async )?function \w+\(/.exec(rest.slice(1)); return next ? rest.slice(0, next.index + 1) : rest; }
// The declarations of the first rule whose selector list is exactly `sel`.
function rule(sel, within) { const src = within || css, at = src.indexOf(sel + '{'); assert(at >= 0 && /[}\s]/.test(src[at - 1] || '}'), 'rule ' + sel); return src.slice(at + sel.length + 1, src.indexOf('}', at)); }
function block(open, after) { const i = css.indexOf(open, after ? css.indexOf(after) : 0); assert(i >= 0 && (!after || css.includes(after)), open); let d = 0; for (let j = i + open.length - 1; j < css.length; j++) { if (css[j] === '{') d++; else if (css[j] === '}' && !--d) return css.slice(i + open.length, j); } throw Error('unclosed ' + open); }
let passed = 0; const test = async (name, fn) => { await fn(); passed++; console.log('PASS', name); };

function page(body) {
  const dom = new JSDOM('<!doctype html><body>' + (body || '') + '</body>', { pretendToBeVisual: true }), { window } = dom, H = new Map(), anims = [];
  // jsdom has no layout: heights come from H, anything attached and not display:none has a box, and animate() is recorded.
  window.Element.prototype.getBoundingClientRect = function () { const h = H.get(this) || 0; return { top: 0, left: 0, right: 0, bottom: h, width: 0, height: h }; };
  window.Element.prototype.getClientRects = function () { for (let e = this; e; e = e.parentElement) if (e.style && e.style.display === 'none' || e.hidden) return []; return this.isConnected ? [{}] : []; };
  window.Element.prototype.animate = function (frames, opts) { const a = { frames, opts, el: this, cancel() { this.cancelled = true; if (this.oncancel) this.oncancel(); }, finish() { if (this.onfinish) this.onfinish(); } }; anims.push(a); return a; };
  let still = false; window.matchMedia = q => ({ matches: /reduced-motion/.test(q) ? still : false });
  const ctx = { window, document: window.document, getComputedStyle: window.getComputedStyle.bind(window), MutationObserver: window.MutationObserver, setTimeout, clearTimeout, console, Promise,
    $: s => window.document.querySelector(s), esc: s => String(s == null ? '' : s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c])), _cmdRestoring: false };
  vm.createContext(ctx);
  return { window, document: window.document, ctx, H, anims, setStill: v => { still = v; }, run: src => vm.runInContext(src, ctx) };
}
const settle = () => new Promise(r => setTimeout(r, 0));

(async () => {
  await test('toasts let taps through to the controls under them and carry no controls of their own', () => {
    assert.match(rule('.toasts'), /pointer-events:none/);
    assert.doesNotMatch(pick('toast'), /<button|<a\s/);
  });

  // (The date picker's phone layout and leftward opening are checked in console-inline-actions.cjs.)
  await test('on phones picker controls, breadcrumbs, listing links and the Approvals edit links are at least 32 px', () => {
    const phone = block('@media(max-width:760px){', '/* Phones: controls are at least 32 px');
    assert.match(phone, /\.rpPreset,\.rpNav,[^{]*,#groupContext \.bg-back\{min-height:32px\}/);
    assert.match(phone, /\.rpNav,[^{]*\{min-width:32px;min-height:32px\}/);
    assert.match(phone, /\.draft \.apEdit>summary,\.draft \.apLink\{display:inline-block;min-width:32px;min-height:32px;/);
    assert.match(phone, /\.reportListing a,\.reportListings>a,\.bg-products a\{display:inline-block;min-height:32px;/);
  });

  await test('every dialog and sheet that eases in stands still under reduced motion', () => {
    const shown = css.match(/([^{}]+)\{animation:dlgIn \.2s ease-out\}/)[1].split(',').map(s => s.trim());
    for (const s of ['#adDesignDialog[open]', 'dialog.bg-split[open]', '.ovsheet>div']) assert(shown.includes(s), s + ' eases in');
    const still = css.match(/@media \(prefers-reduced-motion:reduce\)\{([^{}]+)\{animation:none\}\}/)[1].split(',').map(s => s.trim());
    for (const s of shown) assert(still.includes(s), s + ' stands still under reduced motion');
  });

  await test('a panel a click opens or fills eases to its new height in 150–400 ms, and stands still when restored or under reduced motion', async () => {
    const p = page('<div class="card" id="card"><details id="d"><summary id="s">Why</summary><p>Because</p></details><button id="b">Load</button><div id="more"></div></div>');
    p.run(['cmdStill', 'easeHeight', 'easeNow', 'easeWatch', 'easeClick'].map(pick).join('\n'));
    const d = p.document.getElementById('d'), card = p.document.getElementById('card');
    p.H.set(d, 40); p.document.getElementById('s').click(); p.H.set(d, 200); d.open = true; await settle();
    assert.equal(p.anims.length, 1); let a = p.anims[0];
    assert.equal(a.el, d); assert.equal(a.frames[0].height, '40px'); assert.equal(a.frames[1].height, '200px');
    assert(a.opts.duration >= 150 && a.opts.duration <= 400, 'duration ' + a.opts.duration);
    // A button whose result arrives later: the card eases when the content lands, capped at 400 ms.
    p.H.set(card, 300); p.document.getElementById('b').click(); await settle(); assert.equal(p.anims.length, 1);
    p.H.set(card, 2400); p.document.getElementById('more').innerHTML = '<p>Loaded</p>'; await settle();
    a = p.anims[1]; assert.equal(a.el, card); assert.equal(a.opts.duration, 400);
    // Less than 8 px is not worth moving.
    p.H.set(card, 2404); p.document.getElementById('more').innerHTML += '<i></i>'; await settle(); assert.equal(p.anims.length, 2);
    p.ctx._cmdRestoring = true; p.H.set(d, 200); p.document.getElementById('s').click(); p.H.set(d, 40); d.open = false; await settle(); assert.equal(p.anims.length, 2);
    p.ctx._cmdRestoring = false; p.setStill(true); p.H.set(d, 40); p.document.getElementById('s').click(); p.H.set(d, 200); d.open = true; await settle(); assert.equal(p.anims.length, 2);
  });

  await test('the smallest block holding a change eases, and an inline expander eases through the block that holds it', async () => {
    const p = page('<div class="card" id="card"><div id="act"><button id="gen">Create review draft</button><span id="msg"></span></div><p>Below</p>' +
      '<dl id="dl"><dd id="dd"><span>Canada</span> <details id="inl" style="display:inline"><summary id="inls">Edit</summary><form>Countries</form></details></dd></dl></div>');
    p.run(['cmdStill', 'easeHeight', 'easeNow', 'easeWatch', 'easeClick'].map(pick).join('\n'));
    const act = p.document.getElementById('act'), dd = p.document.getElementById('dd');
    p.H.set(act, 30); p.H.set(p.document.getElementById('card'), 400); p.document.getElementById('gen').click();
    p.H.set(act, 66); p.H.set(p.document.getElementById('card'), 436); p.document.getElementById('msg').textContent = 'Generating the draft…'; await settle();
    assert.equal(p.anims.length, 1); assert.equal(p.anims[0].el, act); assert.equal(p.anims[0].frames[0].height, '30px'); assert.equal(p.anims[0].frames[1].height, '66px');
    p.H.set(dd, 20); p.document.getElementById('inls').click(); p.H.set(dd, 160); p.document.getElementById('inl').open = true; await settle();
    assert.equal(p.anims.length, 2); assert.equal(p.anims[1].el, dd); assert.equal(p.anims[1].frames[0].height, '20px'); assert.equal(p.anims[1].frames[1].height, '160px');
  });

  await test('a campaign row opening under its control grows from nothing, and what loads into it later grows from where it stands', async () => {
    const p = page('<table><tbody><tr class="crow" id="r" aria-controls="det-1" role="button"><td>Campaign</td></tr><tr class="cdet" id="det-1" style="display:none"><td><div id="inner">Detail</div></td></tr></tbody></table>');
    p.run(['cmdStill', 'easeHeight', 'easeNow', 'easeWatch', 'easeClick'].map(pick).join('\n'));
    const det = p.document.getElementById('det-1'), inner = p.document.getElementById('inner');
    p.document.getElementById('r').addEventListener('click', () => { det.style.display = ''; });
    p.H.set(inner, 320); p.document.getElementById('r').click(); await settle();
    assert.equal(p.anims.length, 1); assert.equal(p.anims[0].el, inner);
    assert.equal(p.anims[0].frames[0].height, '0px'); assert.equal(p.anims[0].frames[1].height, '320px');
    p.anims[0].finish(); p.H.set(inner, 500); inner.innerHTML += '<p>Groups</p>'; await settle();
    assert.equal(p.anims.length, 2); assert.equal(p.anims[1].frames[0].height, '320px'); assert.equal(p.anims[1].frames[1].height, '500px');
  });

  await test('Tab and Shift+Tab stay inside the country sheet; Escape closes it and returns focus', async () => {
    const p = page('<button id="opener">Countries</button>');
    p.ctx.COUNTRIES = [{ id: 2124, name: 'Canada', code: 'CA' }, { id: 2840, name: 'United States', code: 'US' }];
    p.ctx.ensureCountries = () => Promise.resolve(p.ctx.COUNTRIES); p.ctx.btnBusy = () => () => {};
    p.run(html.match(/const elFrom=h=>\{[^\n]*\};/)[0].replace('const elFrom', 'var elFrom') + '\n' + pick('openCountryEditor'));
    const opener = p.document.getElementById('opener'); opener.focus();
    p.ctx.openCountryEditor({ title: 'Target countries' }); await settle();
    const sheet = p.document.querySelector('.ovsheet'), box = sheet.firstChild, all = [...box.querySelectorAll('button,input')];
    assert(all.length >= 7, 'search, presets, both countries, Cancel and Save');
    const tab = shift => { const e = new p.window.KeyboardEvent('keydown', { key: 'Tab', shiftKey: !!shift, bubbles: true, cancelable: true }); (p.document.activeElement || p.document.body).dispatchEvent(e); return e.defaultPrevented; };
    all[all.length - 1].focus(); assert(tab(), 'Tab past Save is held'); assert.equal(p.document.activeElement, all[0]);
    assert(tab(true), 'Shift+Tab before the search box is held'); assert.equal(p.document.activeElement, all[all.length - 1]);
    all[2].focus(); assert(!tab(), 'Tab between controls moves normally');
    p.document.activeElement.dispatchEvent(new p.window.KeyboardEvent('keydown', { key: 'Escape', bubbles: true, cancelable: true }));
    assert(!sheet.isConnected); assert.equal(p.document.activeElement, opener);
    assert(!tab(), 'the closed sheet no longer holds Tab');
  });

  await test('the sales bar tooltip sits above its bar, and flips below when that would leave the screen', () => {
    const p = page('<div class="bigchart" id="bc-t" data-w="940" data-h="280"><svg><rect class="bcBar" data-i="0"></rect><rect class="bcBand" data-i="0" data-cx="100" data-top="4"></rect></svg><div class="bcTip"></div></div>');
    p.ctx.money = n => '$' + n;
    p.run(pick('wireBigChart')); p.ctx.wireBigChart('t', [{ x: 'Mon', y: 5 }]);
    const box = p.document.getElementById('bc-t'), tip = box.querySelector('.bcTip'); let top = 120;
    tip.getBoundingClientRect = () => ({ top, left: 0, right: 0, bottom: top + 40, width: 0, height: 40 });
    box._show(0, false); assert.match(tip.style.transform, /translate\(-50%,\s*calc\(-100% - 8px\)\)/);
    top = -30; box._show(0, false); assert.match(tip.style.transform, /translate\(-50%,\s*8px\)/);
  });

  await test('Google image alt text names the image in words, never the raw field type', () => {
    const fieldLabel = vm.runInNewContext('(' + groups.match(/\n\s*const fieldLabel=(.+);\n/)[1] + ')', { String });
    assert.equal(fieldLabel('MARKETING_IMAGE'), 'Landscape image'); assert.equal(fieldLabel('SQUARE_MARKETING_IMAGE'), 'Square image');
    assert.equal(fieldLabel('PORTRAIT_MARKETING_IMAGE'), 'Portrait image'); assert.equal(fieldLabel('LANDSCAPE_LOGO'), 'Landscape logo');
    assert.equal(fieldLabel('BUSINESS_LOGO'), 'Business logo'); assert.equal(fieldLabel(undefined), 'Google image');
    assert.match(groups, /photoHtml\(a\.url,fieldLabel\(a\.fieldType\)\)/); assert.doesNotMatch(groups, /photoHtml\(a\.url,a\.fieldType\)/);
  });

  await test('the re-image progress line and summary use what the engine sends', async () => {
    const said = [], calls = [], statuses = [{ phase: 'running', startedAt: 1 }, { phase: 'running', campaign: 'Moon PMax', assetGroup: 'Moon charms', done: 2, total: 5 },
      { ok: true, queued: 2, results: [{ approvalId: 'a' }, { approvalId: 'b' }, { skipped: 'A creative refresh is already awaiting review.' }], campaigns: 1 }];
    const msg = { set textContent(v) { said.push(v); }, get textContent() { return said[said.length - 1] || ''; } }, btn = { disabled: false, textContent: '' };
    const ctx = { $: s => s === '#pmxBackfillMsg' ? msg : btn, confirm: () => true, actStart: () => 1, actEnd: () => {}, reload: async () => {}, toast: (m, ok) => said.push('toast:' + m),
      setInterval: () => 1, clearInterval: () => {}, setTimeout: f => { f(); return 1; }, Date, Promise, String, Error,
      api: async (action, input) => { calls.push(action); if (action === 'genStatus') { assert(/-pmxbf$/.test(input.genId)); return statuses.shift(); } return { ok: true, started: true }; } };
    vm.createContext(ctx); vm.runInContext(pick('pmaxBackfillImages'), ctx); await ctx.pmaxBackfillImages();
    assert.deepEqual(calls, ['pmaxBackfillImages', 'genStatus', 'genStatus', 'genStatus']);
    assert(said.includes(' scanning Moon PMax · Moon charms (2 groups so far)…'), said.join(' | '));
    assert(said.includes(' ✓ 2 asset groups queued to Approvals across 1 campaign (3 scanned)'), said.join(' | '));
    assert(!said.some(s => /undefined|NaN/.test(s)), said.join(' | '));
  });

  await test('the sign-in page cannot scroll sideways, and its shop link opens beside the console', () => {
    assert.match(markup, /<input class="vh" id="who"/);
    const vh = rule('.vh'); for (const d of ['width:1px!important', 'height:1px!important', 'padding:0!important', 'overflow:hidden']) assert(vh.includes(d), d);
    const links = [...markup.matchAll(/<a\s[^>]*href="https?:[^"]*"[^>]*>/g)].map(m => m[0]);
    assert(links.length >= 1); for (const a of links) { assert.match(a, /target="_blank"/, a); assert.match(a, /rel="noopener/, a); }
  });

  await test('an Opportunities card never widens its column on a phone, and the brand-safety card names no environment variable', () => {
    assert.match(rule('.oppCardList'), /grid-template-columns:minmax\(0,1fr\)/);
    assert.match(rule('.oppCardActions .opDx[open]'), /min-width:0/);
    assert.doesNotMatch(markup, /GADS_TERM_EXCLUSIONS/);
    assert.match(markup, /Brand-safety — excluded terms<\/h3><span class="sub">Read-only · set in Netlify’s environment variables<\/span>/);
  });

  console.log(passed + ' console walk checks passed');
})().catch(e => { console.error(e); process.exitCode = 1; });
