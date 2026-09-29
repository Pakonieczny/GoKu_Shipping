#!/usr/bin/env node
'use strict';
// Real-browser click-through of the Brites Ad Autopilot console.
// Serves the repository root on 127.0.0.1, answers every console API action from
// synthetic fixtures (fixtures.cjs), blocks every other network request, and
// walks each tab at desktop and phone sizes. See README.md.
const fs = require('fs'), path = require('path'), http = require('http'), { execSync } = require('child_process');
const { createFixtures } = require('./fixtures.cjs');
const { pageAudit, listControls, openOverlays } = require('./audit.cjs');

const REPO = path.resolve(__dirname, '../../..');
const OUT = path.resolve(process.env.BRITES_HARNESS_OUT || path.join(REPO, 'tmp', 'adwords-browser'));
const PASSCODE = 'harness-passcode-not-real';
const ARGS = process.argv.slice(2);
const argVal = (name, dflt) => { const a = ARGS.find(x => x.startsWith('--' + name + '=')); return a ? a.split('=').slice(1).join('=') : dflt; };
const ONLY_TABS = argVal('tabs', '') ? argVal('tabs', '').split(',') : null;
const ONLY_VP = argVal('viewports', '') ? argVal('viewports', '').split(',') : null;
const BUDGET = Number(argVal('budget', '100'));        // actions per tab per viewport
const FAMILY_CAP = Number(argVal('family-cap', '2')); // same control repeated across rows
const SHOTS = argVal('screenshots', '1') !== '0';
const VIEWPORTS = [
  { name: 'desktop', width: 1440, height: 900, phone: false },
  { name: 'phone', width: 390, height: 844, phone: true }
].filter(v => !ONLY_VP || ONLY_VP.includes(v.name));
const TABS = ['groups', 'command', 'sales', 'approvals', 'bench', 'controls'].filter(t => !ONLY_TABS || ONLY_TABS.includes(t));
const TAB_NAMES = { groups: 'Products & groups', command: 'Overview', sales: 'Sales', approvals: 'Approvals', bench: 'Opportunities', controls: 'Controls', gate: 'Sign-in', shell: 'Shell (topbar/nav)' };

function loadPlaywright() {
  const candidates = [process.env.PLAYWRIGHT_MODULE, 'playwright'];
  try { candidates.push(path.join(execSync('npm root -g', { encoding: 'utf8' }).trim(), 'playwright')); } catch (e) {}
  for (const c of candidates.filter(Boolean)) { try { return require(c); } catch (e) {} }
  throw new Error('Playwright is not available. Set PLAYWRIGHT_MODULE to its install path.');
}

// Actions the server's handleAction accepts, read from its source so the list
// cannot drift from what production answers.
function serverActions() {
  const src = fs.readFileSync(path.join(REPO, 'netlify/functions/googleAdsAutopilotKick.js'), 'utf8');
  const body = src.slice(src.indexOf('async function handleAction'), src.indexOf('exports.handler'));
  const out = new Set();
  for (const m of body.matchAll(/\ba\s*===?\s*["']([A-Za-z0-9_]+)["']/g)) out.add(m[1]);
  for (const m of body.matchAll(/\[([^\]]*)\]\.includes\(a\)/g)) for (const s of m[1].matchAll(/["']([A-Za-z0-9_]+)["']/g)) out.add(s[1]);
  return out;
}

function staticServer() {
  const types = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.cjs': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml', '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.webp': 'image/webp', '.json': 'application/json', '.woff2': 'font/woff2', '.ico': 'image/x-icon' };
  const misses = [];
  const server = http.createServer((req, res) => {
    let p = decodeURIComponent(new URL(req.url, 'http://x').pathname);
    if (p === '/') p = '/brites-adwords.html';
    const file = path.join(REPO, p);
    if (!file.startsWith(REPO + path.sep) || /(^|\/)\.(git|env|netlify)|node_modules|secrets/.test(p)) { res.writeHead(403); return res.end(); }
    fs.readFile(file, (err, data) => {
      if (err) { misses.push(p); res.writeHead(404); return res.end('not found'); }
      res.writeHead(200, { 'Content-Type': types[path.extname(file).toLowerCase()] || 'application/octet-stream', 'Cache-Control': 'no-store' });
      res.end(data);
    });
  });
  return new Promise(resolve => server.listen(0, '127.0.0.1', () => resolve({ server, port: server.address().port, misses })));
}

// A small placeholder image for product photos the fixtures reference.
function placeholderSvg(label) {
  const hue = [...label].reduce((h, c) => (h * 31 + c.charCodeAt(0)) % 360, 7);
  return `<svg xmlns="http://www.w3.org/2000/svg" width="640" height="640" viewBox="0 0 640 640"><rect width="640" height="640" fill="hsl(${hue},35%,86%)"/><circle cx="320" cy="300" r="120" fill="none" stroke="hsl(${hue},40%,40%)" stroke-width="18"/><text x="320" y="560" font-family="sans-serif" font-size="34" text-anchor="middle" fill="#333">harness image</text></svg>`;
}

// ------------------------------------------------------------------ findings
const findings = new Map(); // key -> finding
const coverage = {};        // vp|tab -> { clicked, selects, inputs, charts, dialogs, skipped, families:Set }
const apiLog = [];
const notes = [];
function cov(vp, tab) { const k = vp + '|' + tab; return coverage[k] || (coverage[k] = { clicked: 0, selects: 0, inputs: 0, charts: 0, dialogs: 0, disabled: 0, skipped: 0, vanished: 0, families: new Set() }); }
function add(kind, vp, tab, detail, key) {
  const k = kind + '|' + (key || JSON.stringify(detail));
  const f = findings.get(k);
  if (f) { f.count++; if (!f.viewports.includes(vp)) f.viewports.push(vp); if (!f.tabs.includes(tab)) f.tabs.push(tab); return f; }
  const nf = { kind, viewports: [vp], tabs: [tab], detail, count: 1 };
  findings.set(k, nf); return nf;
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const { chromium } = loadPlaywright();
  const known = serverActions();
  const { server, port, misses } = await staticServer();
  const base = 'http://127.0.0.1:' + port;
  const browser = await chromium.launch({ headless: true, args: ['--disable-gpu', '--no-first-run', '--disable-extensions', '--disable-background-networking', '--js-flags=--max-old-space-size=512'] });
  const started = Date.now();
  let shots = 0;
  try {
    for (const vp of VIEWPORTS) await runViewport(browser, vp, base, known, () => shots++ < 60);
  } finally {
    await browser.close().catch(() => {});
    server.close();
  }
  misses.forEach(p => add('static 404', 'any', 'any', { path: p }, p));
  report(started);
}

async function runViewport(browser, vp, base, known, canShoot) {
  const fx = createFixtures();
  const context = await browser.newContext({ viewport: { width: vp.width, height: vp.height }, deviceScaleFactor: 1, isMobile: vp.phone, hasTouch: vp.phone, locale: 'en-CA', timezoneId: 'UTC', serviceWorkers: 'block', reducedMotion: 'no-preference' });
  const state = { tab: 'gate', inflight: 0, lastApi: Date.now(), errors: [], lastAction: '(page load)', activeOverlays: new Set(), crawledDialogs: new Set() };
  await context.addInitScript(() => {
    window.__hShifts = [];
    try { new PerformanceObserver(list => { for (const e of list.getEntries()) window.__hShifts.push({ t: e.startTime, v: e.value, src: (e.sources || []).map(s => { const n = s.node; if (!n || n.nodeType !== 1) return n && n.parentElement ? n.parentElement.tagName.toLowerCase() : '?'; return n.tagName.toLowerCase() + (n.id ? '#' + n.id : '') + (n.classList.length ? '.' + [...n.classList].slice(0, 2).join('.') : ''); }) }); }).observe({ type: 'layout-shift', buffered: true }); } catch (e) {}
  });
  await context.route('**/*', async route => {
    const req = route.request(), url = new URL(req.url());
    if (url.protocol === 'data:' || url.protocol === 'blob:') return route.continue();
    if (url.hostname === '127.0.0.1' && url.pathname.startsWith('/.netlify/functions/')) return fakeApi(route, req, url);
    if (url.hostname === '127.0.0.1') return route.continue();
    if (/(^|\.)cdn\.shopify\.com$|storage\.googleapis\.com$|firebasestorage\.googleapis\.com$|^images\.test$/.test(url.hostname) && req.resourceType() === 'image') {
      return route.fulfill({ status: 200, contentType: 'image/svg+xml', headers: { 'Access-Control-Allow-Origin': '*' }, body: placeholderSvg(url.pathname) });
    }
    add('blocked external request', vp.name, state.tab, { url: url.origin + url.pathname.slice(0, 80), type: req.resourceType(), after: state.lastAction }, url.origin + url.pathname.slice(0, 80));
    return route.abort('blockedbyclient');
  });
  async function fakeApi(route, req, url) {
    state.inflight++; state.lastApi = Date.now();
    try {
      const fn = url.pathname.split('/').pop();
      let body = {}; try { body = JSON.parse(req.postData() || '{}'); } catch (e) {}
      const action = body.action;
      apiLog.push({ vp: vp.name, tab: state.tab, fn, action, after: state.lastAction });
      if (fn !== 'googleAdsAutopilotApi') add('request to another function', vp.name, state.tab, { fn, action }, fn + '|' + action);
      if ((req.headers()['x-edit-passcode'] || body.passcode) !== PASSCODE) return route.fulfill({ status: 401, contentType: 'application/json', body: JSON.stringify({ error: 'unauthorized' }) });
      if (!known.has(action)) { add('unknown API action', vp.name, state.tab, { action, after: state.lastAction }, action); return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ error: 'unknown action' }) }); }
      const r = fx.respond(action, body);
      if (!r) { add('harness: no fixture', vp.name, state.tab, { action }, action); return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ ok: true }) }); }
      return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(r.body) });
    } catch (e) {
      add('harness: fixture error', vp.name, state.tab, { error: String(e && e.stack || e).slice(0, 300) }, String(e && e.message));
      return route.fulfill({ status: 500, contentType: 'application/json', body: JSON.stringify({ error: 'harness fixture failed' }) });
    } finally { state.inflight--; state.lastApi = Date.now(); }
  }

  const page = await context.newPage();
  context.on('page', p => { if (p !== page) { add('popup window opened', vp.name, state.tab, { url: p.url(), after: state.lastAction }, state.lastAction); p.close().catch(() => {}); } });
  page.on('console', m => { if (m.type() !== 'error' && m.type() !== 'warning') return; const loc = m.location() || {}; const text = m.text().slice(0, 240);
    if (/Failed to load resource: net::ERR_BLOCKED_BY_CLIENT/.test(text)) return;
    if (state.tab === 'gate' && /status of 401/.test(text)) return; // the wrong-passcode path is exercised on purpose
    add('console ' + m.type(), vp.name, state.tab, { text, at: (loc.url || '').replace(base, '') + ':' + (loc.lineNumber + 1), after: state.lastAction }, text.replace(/\d+/g, '#')); });
  page.on('pageerror', e => { const top = String(e.stack || '').split('\n').slice(0, 3).join(' | ').replace(new RegExp(base, 'g'), ''); add('page error', vp.name, state.tab, { message: String(e.message).slice(0, 240), stack: top.slice(0, 300), after: state.lastAction }, String(e.message).replace(/\d+/g, '#')); });
  page.on('dialog', async d => { const msg = d.message(); add('native ' + d.type() + '()', vp.name, state.tab, { message: msg.slice(0, 160), after: state.lastAction }, d.type() + '|' + msg.slice(0, 60)); state.nativeDialogs = (state.nativeDialogs || 0) + 1;
    for (const m of msg.matchAll(/(US|CA|C)?\$\s?([\d,]+(?:\.\d+)?)/g)) { const val = Number(m[2].replace(/,/g, '')); if (!m[1] && !/\b(CAD|USD)\b/.test(msg) && fx.cadTracers.some(t => Math.abs(t - val) < 0.006)) add('CAD amount shown with bare $', vp.name, state.tab, { amount: m[0], where: 'native ' + d.type() + '()', text: msg.replace(/\s+/g, ' ').slice(0, 110), after: state.lastAction }, 'native|' + msg.slice(0, 30).replace(/\d/g, '#')); }
    // A budget prompt gets a CAD tracer amount far enough from the current budget to reach the large-change confirm.
    const answer = d.type() === 'prompt' ? (/budget/i.test(msg) ? '17.47' : (d.defaultValue() || '12')) : undefined;
    try { if (d.type() === 'confirm' && /delete|remove|clear|permanent|discard/i.test(msg)) await d.dismiss(); else await d.accept(answer); } catch (e) {} });
  page.on('requestfailed', r => { const u = r.url(); if (u.startsWith(base) && !/\/\.netlify\//.test(u)) add('request failed', vp.name, state.tab, { url: u.replace(base, ''), error: r.failure() && r.failure().errorText }, u); });

  const settle = async (min = 250, max = 5000) => {
    const t0 = Date.now(); await page.waitForTimeout(min);
    while (Date.now() - t0 < max) { if (state.inflight === 0 && Date.now() - state.lastApi > 300) break; await page.waitForTimeout(80); }
  };
  const audit = async (tab, scopes, full, label) => {
    let res; try { res = await page.evaluate(pageAudit, { scopes, minTap: vp.phone && full ? 32 : 0, cadTracers: fx.cadTracers }); } catch (e) { notes.push('audit failed on ' + tab + ': ' + e.message); return; }
    const ctx = label || state.lastAction;
    if (res.docOverflow) { const f = add('page wider than viewport', vp.name, tab, Object.assign({ after: ctx }, res.docOverflow), (res.docOverflow.culprits[0] || {}).sel || 'doc'); await shoot(f, (res.docOverflow.culprits[0] || {}).sel, tab); }
    for (const o of res.overflow) { const f = add('element overflows its container', vp.name, tab, Object.assign({ after: ctx }, o), o.container + '|' + o.sel); await shoot(f, o.sel, tab); }
    for (const c of res.clipped) { const f = add(c.ellipsis ? (c.titled ? 'text truncated (ellipsis, has tooltip)' : 'text truncated (ellipsis, no tooltip)') : 'text clipped (hard cut)', vp.name, tab, Object.assign({ after: ctx }, c), c.sel + '|' + c.text.slice(0, 30)); if (!c.ellipsis) await shoot(f, c.sel, tab); }
    for (const o of res.overlaps) { const f = add('overlapping text', vp.name, tab, Object.assign({ after: ctx }, o), o.a + '|' + o.b + '|' + o.at.slice(0, 12)); await shoot(f, o.a, tab); }
    for (const s of res.smallTargets) add(s.inline ? 'tap target < 32px (inline link)' : 'tap target < 32px', vp.name, tab, s, s.sel + '|' + s.text);
    for (const l of res.leaks) add('placeholder/escape leak: ' + l.kind, vp.name, tab, Object.assign({ after: ctx }, l), l.match + '|' + l.sel);
    for (const d of res.bareDollar) add(d.cadTracer ? 'CAD amount shown with bare $' : 'info: $ amount with no currency code nearby', vp.name, tab, Object.assign({ after: ctx }, d), d.sel + '|' + (d.cadTracer ? d.amount : d.amount.replace(/\d/g, '#')) + '|' + d.labelled);
  };
  async function shoot(f, sel, tab) {
    if (!SHOTS || f.count > 1 || f.shot || !sel || !canShoot()) return;
    try {
      const box = await page.evaluate(s => { const el = document.querySelector(s); if (!el) return null; el.scrollIntoView({ block: 'center', inline: 'nearest' }); const r = el.getBoundingClientRect(); el.style.outline = '3px solid #e0197d'; setTimeout(() => { el.style.outline = ''; }, 600); return { x: r.left, y: r.top, w: r.width, h: r.height }; }, sel);
      if (!box) return;
      const file = path.join(OUT, [vp.name, tab, f.kind.replace(/[^a-z0-9]+/gi, '-').slice(0, 40), findings.size].join('_') + '.png');
      const pad = 60, x = Math.max(0, box.x - pad), y = Math.max(0, box.y - pad);
      await page.screenshot({ path: file, clip: { x, y, width: Math.max(40, Math.min(vp.width - x, box.w + pad * 2)), height: Math.max(40, Math.min(vp.height - y, box.h + pad * 2)) } });
      f.shot = path.relative(REPO, file);
    } catch (e) {}
  }
  const activeView = () => page.evaluate(() => { const v = [...document.querySelectorAll('.view')].find(x => !x.classList.contains('hidden')); return v ? v.id.replace(/^v-/, '') : (document.getElementById('gate').style.display !== 'none' ? 'gate' : '?'); }).catch(() => '?');
  const openNav = async () => { if (!vp.phone) return; const open = await page.evaluate(() => document.body.classList.contains('navOpen')); if (!open) { await page.locator('#navBurger').tap({ timeout: 3000 }); await page.waitForTimeout(320); } };
  // A modal left open by a control that also switched views makes the rail inert.
  async function closeOverlays() {
    for (let i = 0; i < 3; i++) { if (!(await page.evaluate(() => !!document.querySelector('dialog[open],.ovsheet')).catch(() => false))) return; await page.keyboard.press('Escape'); await page.waitForTimeout(250); }
    await page.evaluate(() => { document.querySelectorAll('dialog[open]').forEach(d => d.close()); document.querySelectorAll('.ovsheet').forEach(o => o.remove()); }).catch(() => {});
  }
  async function goTab(tab) {
    state.tab = tab; state.lastAction = 'open tab ' + tab;
    await closeOverlays();
    await openNav();
    const b = page.locator('#nav button[data-v="' + tab + '"]');
    if (vp.phone) await b.tap({ timeout: 4000 }); else await b.click({ timeout: 4000 });
    await settle(400, 6000);
  }
  async function ensureSignedIn() {
    const onGate = await page.evaluate(() => getComputedStyle(document.getElementById('app')).display === 'none').catch(() => true);
    if (!onGate) return;
    await page.waitForTimeout(500);
    if (await page.evaluate(() => getComputedStyle(document.getElementById('app')).display === 'none').catch(() => true)) {
      await page.fill('#pc', PASSCODE); await page.click('.signin__submit');
      await page.waitForFunction(() => getComputedStyle(document.getElementById('app')).display !== 'none', null, { timeout: 15000 });
    }
    await settle(600, 8000);
  }

  // Everything that happens after one interaction.
  async function afterAction(tab, scopes, c, depth) {
    await settle(300, 5000);
    if (!page.url().startsWith(base)) { add('navigated away', vp.name, tab, { url: page.url(), after: state.lastAction }, state.lastAction); await page.goto(base + '/brites-adwords.html'); }
    if (tab !== 'gate') await ensureSignedIn();
    // Only overlays this action opened; the dialog being crawled stays open.
    const ov = (await page.evaluate(openOverlays).catch(() => [])).filter(o => !state.activeOverlays.has(o.sel));
    if (ov.length) await handleOverlays(tab, ov, depth);
    const v = await activeView();
    if (tab !== 'gate' && tab !== 'shell' && v !== tab && v !== '?') { cov(vp.name, tab).navigations = (cov(vp.name, tab).navigations || 0) + 1; await goTab(tab); }
    await audit(tab, scopes, false);
  }

  // Expand/collapse motion: sample the height of the region a control opens or
  // closes. A region that reaches its final height by the first sample jumped.
  async function markRegion(id) {
    return page.evaluate(id => {
      document.querySelectorAll('[data-harness-region]').forEach(n => n.removeAttribute('data-harness-region'));
      const el = document.querySelector('[data-harness-id="' + id + '"]'); if (!el) return null;
      const ctl = el.getAttribute('aria-controls'), reg = el.closest('details') || (ctl && document.getElementById(ctl)) || el.closest('.draft,.card,article,section,.view');
      if (!reg) return null; reg.setAttribute('data-harness-region', '1');
      return { h: reg.getBoundingClientRect().height, doc: document.documentElement.scrollHeight };
    }, id).catch(() => null);
  }
  const regionHeight = () => page.evaluate(() => { const r = document.querySelector('[data-harness-region]'); return r ? { h: r.getBoundingClientRect().height, doc: document.documentElement.scrollHeight, anim: document.getAnimations().filter(a => a.playState === 'running' && a.effect && a.effect.getComputedTiming().iterations !== Infinity && !/^(background|background-color|color|border-color|box-shadow|outline|outline-color|filter|text-decoration-color|fill|stroke)$/.test(a.transitionProperty || '')).map(a => (a.animationName || a.transitionProperty) + '@' + (a.effect.target ? a.effect.target.tagName.toLowerCase() + '.' + String(a.effect.target.className || '').split(' ')[0] : '')) } : null; }).catch(() => null);
  async function measureMotion(t0, before) {
    const s1 = await regionHeight(); await page.waitForTimeout(90);
    const s2 = await regionHeight(); await page.waitForTimeout(260);
    const s3 = await regionHeight();
    const shifts = await page.evaluate(t => (window.__hShifts || []).filter(s => s.t >= t), t0).catch(() => []);
    const out = { shift: shifts.reduce((s, x) => s + x.v, 0), sources: [...new Set(shifts.flatMap(s => s.src))].slice(0, 5), animated: [...new Set([].concat(s1 && s1.anim || [], s2 && s2.anim || []))] };
    if (before && s1 && s3) {
      const delta = s3.h - before.h, docDelta = s3.doc - before.doc;
      out.delta = Math.round(delta); out.docDelta = Math.round(docDelta);
      out.jumped = Math.abs(delta) > 24 && Math.abs(s1.h - s3.h) <= 2 && (!s2 || Math.abs(s2.h - s3.h) <= 2);
    }
    return out;
  }

  async function exercise(tab, scopes, c, depth) {
    const C = cov(vp.name, tab); C.families.add(c.family);
    const loc = page.locator('[data-harness-id="' + c.id + '"]');
    if (!(await loc.count())) { C.vanished++; return; }
    state.lastAction = (c.chart ? 'chart mark' : c.tag + (c.type ? '[' + c.type + ']' : '')) + ' "' + c.text + '"' + (c.row ? ' (row ' + c.row + ')' : '');
    if (process.env.HARNESS_DEBUG) console.error('[' + vp.name + '/' + tab + (depth ? '/dialog' : '') + '] ' + state.lastAction + (c.disabled ? ' (disabled)' : ''));
    if (c.disabled) { C.disabled++; return; }
    if (c.tag === 'a' && c.href && !/^#|^javascript:/i.test(c.href)) {
      C.skipped++;
      const rel = await loc.getAttribute('rel');
      if (/^https?:/i.test(c.href) && (c.target !== '_blank' || !/noopener/.test(rel || ''))) add('external link opens in the same tab', vp.name, tab, { href: c.href, text: c.text }, c.href);
      return;
    }
    if (/sign out/i.test(c.text) || c.type === 'file' || c.type === 'password') { C.skipped++; return; }
    const t0 = await page.evaluate(() => performance.now());
    const before = c.chart || c.tag === 'select' || c.tag === 'input' || c.inPopover ? null : await markRegion(c.id);
    try {
      if (c.tag === 'select') {
        const opts = await loc.evaluate(s => [...s.options].map(o => o.value));
        const orig = await loc.inputValue();
        for (const v of opts.slice(0, 6)) { if (v === orig) continue; state.lastAction = 'select "' + c.text + '" = ' + v; await loc.selectOption(v, { timeout: 2500 }); C.selects++; await afterAction(tab, scopes, c, depth); if (!(await loc.count())) return; }
        if (await loc.count()) { await loc.selectOption(orig, { timeout: 2500 }).catch(() => {}); await settle(); }
        return;
      }
      if (c.tag === 'input' && /^(date|number|text|search|email|url|range|tel)$/.test(c.type || 'text') || c.tag === 'textarea') {
        C.inputs++;
        const orig = await loc.inputValue().catch(() => null);
        if (c.type === 'date') await loc.fill(new Date(Date.now() + 12 * 86400000).toISOString().slice(0, 10), { timeout: 2500 });
        else if (c.type === 'range') { await loc.focus({ timeout: 2500 }); await page.keyboard.press('ArrowRight'); await page.keyboard.press('ArrowRight'); }
        else if (c.type === 'number') { await loc.fill('12', { timeout: 2500 }); }
        else await loc.fill(c.type === 'search' || /search|ctyq/i.test(c.text) ? 'ca' : 'harness', { timeout: 2500 });
        await loc.dispatchEvent('change').catch(() => {});
        await afterAction(tab, scopes, c, depth);
        // Put filters and ranges back so the rest of the view stays reachable.
        if (orig != null && c.type !== 'range' && (await loc.count()) && (await loc.inputValue().catch(() => orig)) !== orig) {
          await loc.fill(orig, { timeout: 2500 }).catch(() => {}); await loc.dispatchEvent('change').catch(() => {}); await settle(250, 4000);
        }
        return;
      }
      if (c.chart) { C.charts++; await probeChartMark(tab, loc, c); return; }
      // A styled checkbox is operated through its label, as a person would.
      const target = c.tag === 'input' && /^(checkbox|radio)$/.test(c.type) && !(await loc.isVisible()) ? loc.locator('xpath=ancestor::label[1]') : loc;
      if (vp.phone) await target.tap({ timeout: 3000 }); else await target.click({ timeout: 3000 });
      C.clicked++;
    } catch (e) {
      const msg = String(e.message || e).split('\n');
      const why = (msg.find(l => /intercepts pointer events/.test(l)) || msg.find(l => /outside of the viewport|not visible|not stable|detached/.test(l)) || msg[0]).trim().slice(0, 200);
      if (/intercepts pointer events/.test(why)) add('control covered by another element', vp.name, tab, { control: c.text, family: c.family, why }, c.family + '|' + why.replace(/\d+/g, '#').slice(0, 80));
      else if (!/detached|not attached/.test(why)) add('control could not be operated', vp.name, tab, { control: c.text, family: c.family, why }, c.family);
      return;
    }
    const motion = await measureMotion(t0, before);
    const left = tab !== 'gate' && tab !== 'shell' && depth === 0 && (await activeView()) !== tab; // navigation, not expansion
    if (left || c.inPopover) { /* the popover and view changes are judged elsewhere */ }
    else if (motion.jumped) add('expands/collapses with no transition (jumps)', vp.name, tab, { control: c.text, tag: c.tag, heightChange: motion.delta, pageHeightChange: motion.docDelta, shift: Math.round(motion.shift * 1000) / 1000 }, c.family);
    else if (motion.shift > 0.05 && !motion.animated.length) add('content below shifts with no transition', vp.name, tab, { control: c.text, tag: c.tag, shift: Math.round(motion.shift * 1000) / 1000, moved: motion.sources }, c.family);
    await afterAction(tab, scopes, c, depth);
  }

  async function probeChartMark(tab, loc, c) {
    await loc.scrollIntoViewIfNeeded().catch(() => {});
    const box = await loc.boundingBox(); if (!box) return;
    const x = box.x + box.width / 2, y = box.y + Math.min(box.height / 2, 60);
    // Page changes outside the chart and its tooltip: a detail panel, a list, a dialog.
    const watch = () => page.evaluate(() => { window.__hMut = 0; window.__hObs && window.__hObs.disconnect(); window.__hObs = new MutationObserver(list => { for (const m of list) { const t = m.target.nodeType === 1 ? m.target : m.target.parentElement; if (t && !t.closest('[data-harness-tip],.lc-cross,.lc-focus,.bcCross,.bcFocus,svg,.bcTip,#actbar,.toasts')) window.__hMut++; } }); window.__hObs.observe(document.body, { subtree: true, childList: true, attributes: true, characterData: true }); });
    const changes = () => page.evaluate(() => { const n = window.__hMut; window.__hObs.disconnect(); return n; });
    let tapChanged = 0;
    if (vp.phone) { await watch(); await loc.tap({ timeout: 2500, position: { x: box.width / 2, y: Math.min(box.height / 2, 60) } }).catch(() => {}); }
    else await page.mouse.move(x, y);
    await page.waitForTimeout(220);
    if (vp.phone) tapChanged = await changes().catch(() => 0);
    const tip = await page.evaluate(() => {
      const cands = [...document.querySelectorAll('body > div, .bcTip')].filter(d => { const s = getComputedStyle(d); return s.display !== 'none' && (s.pointerEvents === 'none') && d.textContent.trim() && (s.position === 'fixed' || s.position === 'absolute'); });
      const t = cands[cands.length - 1]; if (!t) return null; t.setAttribute('data-harness-tip', '1'); const r = t.getBoundingClientRect();
      return { text: t.textContent.replace(/\s+/g, ' ').trim().slice(0, 120), left: Math.round(r.left), right: Math.round(r.right), top: Math.round(r.top), bottom: Math.round(r.bottom), vw: innerWidth, vh: innerHeight };
    });
    const where = c.text + ' in ' + (await loc.evaluate(el => { const h = el.closest('.card,.bigchart,section,article'); const t = h && h.querySelector('h3,h4,.card__h'); return (t ? t.textContent : h ? h.className : '').replace(/\s+/g, ' ').trim().slice(0, 50); }).catch(() => ''));
    if (!tip && !tapChanged) add(vp.phone ? 'chart mark does nothing on tap' : 'chart mark shows no tooltip', vp.name, tab, { chart: where, how: vp.phone ? 'tap' : 'hover' }, where);
    else if (tip) {
      if (tip.left < 0 || tip.right > tip.vw || tip.top < 0 || tip.bottom > tip.vh) add('chart tooltip off-screen', vp.name, tab, { chart: where, tip }, where);
      for (const [name, re] of [['NaN', /\bNaN\b/], ['undefined', /\bundefined\b/], ['null', /\bnull\b/]]) if (re.test(tip.text)) add('placeholder/escape leak: ' + name, vp.name, tab, { where: 'chart tooltip', text: tip.text }, where + name);
    }
    if (!vp.phone) {
      await watch();
      await page.mouse.click(x, y).catch(() => {});
      await page.waitForTimeout(400);
      const mut = await changes();
      const ov = (await page.evaluate(openOverlays).catch(() => [])).filter(o => !state.activeOverlays.has(o.sel));
      if (!mut && !ov.length) add('chart is hover-only (click shows no detail)', vp.name, tab, { chart: where }, where);
      if (ov.length) await handleOverlays(tab, ov, 1);
    }
  }

  async function handleOverlays(tab, list, depth) {
    for (const ov of list) {
      if (state.activeOverlays.has(ov.sel)) continue;
      state.activeOverlays.add(ov.sel);
      try { await handleOverlay(tab, ov, depth); } finally { state.activeOverlays.delete(ov.sel); }
    }
  }
  async function handleOverlay(tab, ov, depth) {
    {
      const C = cov(vp.name, tab); C.dialogs++;
      const label = ov.kind + ' ' + ov.id + ' "' + ov.label + '"';
      const ovSel = ov.kind === 'dialog' ? ov.sel : ov.sel;
      // Entrance motion: does the overlay animate in, or pop?
      const anim = await page.evaluate(s => { const el = document.querySelector(s); if (!el) return null; return el.getAnimations({ subtree: true }).filter(a => a.effect && a.effect.getComputedTiming().iterations !== Infinity).length; }, ovSel).catch(() => null);
      if (anim === 0 && ov.kind !== 'popover') add('dialog appears without transition', vp.name, tab, { overlay: label, opener: state.lastAction }, label);
      // Focus must move into a dialog when it opens (checked before the crawl moves it).
      const focus = await page.evaluate(s => { const el = document.querySelector(s); return !!(el && el.contains(document.activeElement)); }, ovSel).catch(() => false);
      // Audit the dialog content fully.
      await audit(tab, [ovSel], true, 'inside ' + label);
      // Focus containment, before the crawl can close it: Tab once past every focusable inside.
      const stops = await page.evaluate(s => { const el = document.querySelector(s); return el ? el.querySelectorAll('a[href],button:not([disabled]),input:not([disabled]):not([type=hidden]),select,textarea,summary,[tabindex]:not([tabindex="-1"])').length : 0; }, ovSel).catch(() => 0);
      let escaped = 0, presses = Math.min(60, stops + 3);
      for (let i = 0; i < presses; i++) { await page.keyboard.press('Tab'); const inside = await page.evaluate(s => { const el = document.querySelector(s), a = document.activeElement; return !el || el.contains(a) || a === document.body; }, ovSel).catch(() => true); if (!inside) escaped++; }
      if (ov.kind !== 'popover' && (escaped || !focus)) add(escaped ? 'dialog does not trap focus' : 'dialog opens without moving focus into it', vp.name, tab, { overlay: label, tabsEscaped: escaped + '/' + presses, opener: state.lastAction }, label);
      // Crawl inside dialogs one level deep.
      // Each kind of dialog is crawled once per viewport; later openings are audited only.
      if (depth < 1 && ov.kind !== 'popover' && !state.crawledDialogs.has(ov.id)) { state.crawledDialogs.add(ov.id); await crawl(tab, [ovSel], Math.min(30, BUDGET), depth + 1); }
      if (!(await page.locator(ovSel).count()) || !(await isOpen(ovSel))) return;
      await page.keyboard.press('Escape'); await page.waitForTimeout(250);
      if (await isOpen(ovSel)) {
        if (ov.kind !== 'popover') add('Escape does not close dialog', vp.name, tab, { overlay: label }, label);
        const closer = page.locator(ovSel + ' button').filter({ hasText: /^\s*(close|cancel|done|back|×|✕|hide)/i }).first();
        if (await closer.count()) { await closer.click({ timeout: 2500 }).catch(() => {}); await settle(250, 3000); }
        else if (ov.kind === 'overlay') { await page.mouse.click(5, vp.height - 5).catch(() => {}); await page.waitForTimeout(250); }
        else if (ov.kind === 'popover') { await page.mouse.click(vp.width - 5, vp.height - 5).catch(() => {}); await page.waitForTimeout(250); }
        if (await isOpen(ovSel)) {
          add('dialog cannot be closed', vp.name, tab, { overlay: label, opener: state.lastAction }, label);
          await page.evaluate(s => { const el = document.querySelector(s); if (!el) return; if (el.tagName === 'DIALOG') el.close(); else if (el.classList.contains('rpPop')) el.style.display = 'none'; else el.remove(); }, ovSel).catch(() => {});
        }
      }
      await settle(200, 3000);
    }
  }
  const isOpen = s => page.evaluate(s => { const el = document.querySelector(s); if (!el) return false; if (el.tagName === 'DIALOG') return el.open; return el.style.display !== 'none' && el.getBoundingClientRect().width > 0; }, s).catch(() => false);

  async function crawl(tab, scopes, budget, depth) {
    const visited = new Set(), fam = new Map(); let n = 0, waited = false;
    while (n < budget) {
      let controls; try { controls = await page.evaluate(listControls, { scopes }); } catch (e) { break; }
      if (!controls.length) break;
      // Tabs and filters wait until the content they reveal has been exercised;
      // back/close/leave controls go last.
      let next = null;
      const passOf = c => c.backLike ? 2 : c.tabLike ? 1 : 0;
      for (const pass of [0, 1, 2]) {
        for (const c of controls) {
          if (passOf(c) !== pass || (depth === 0 && c.inDialog)) continue;
          const sig = c.family + '#' + c.row;
          if (visited.has(sig)) continue;
          if ((fam.get(c.family) || 0) >= FAMILY_CAP) { visited.add(sig); continue; }
          next = c; break;
        }
        if (next) break;
      }
      if (!next) break;
      // Before leaving, give content that renders on a timer (job polls) one chance to appear.
      if (next.backLike && !waited) { waited = true; await page.waitForTimeout(1600); await settle(200, 4000); continue; }
      if (!next.backLike) waited = false;
      visited.add(next.family + '#' + next.row); fam.set(next.family, (fam.get(next.family) || 0) + 1);
      await exercise(tab, scopes, next, depth); n++;
    }
    if (n >= budget) notes.push(`${vp.name}/${tab}: action budget (${budget}) reached${depth ? ' inside a dialog' : ''}`);
    return n;
  }

  // --------------------------------------------------------------- the walk
  await page.goto(base + '/brites-adwords.html', { waitUntil: 'load' });
  await settle(300);
  state.tab = 'gate';
  await audit('gate', ['#gate'], true, 'sign-in page');
  await crawl('gate', ['#gate'], 12, 0);
  await page.fill('#pc', PASSCODE);
  state.lastAction = 'sign in';
  await page.click('.signin__submit');
  await page.waitForFunction(() => getComputedStyle(document.getElementById('app')).display !== 'none', null, { timeout: 20000 });
  await settle(800, 8000);

  // Shell: top bar, breadcrumbs and (phone) the navigation drawer.
  state.tab = 'shell';
  await audit('shell', ['.topbar', '#groupContext'], true, 'top bar');
  if (vp.phone) { await openNav(); await audit('shell', ['.rail'], true, 'navigation drawer'); await page.locator('#navScrim').tap({ position: { x: vp.width - 20, y: 400 } }).catch(() => {}); await page.waitForTimeout(300); }
  for (const tab of TABS) {
    try {
      await goTab(tab);
      await audit(tab, ['#v-' + tab, '#groupContext'], true, 'tab opened');
      if (tab === 'command') await chartResize(tab);
      await crawl(tab, ['#v-' + tab, '#groupContext'], BUDGET, 0);
      await audit(tab, ['#v-' + tab, '#groupContext'], true, 'after exercising the tab');
    } catch (e) { notes.push(`${vp.name}/${tab}: walk stopped — ${String(e.message).split('\n')[0]}`); try { await page.keyboard.press('Escape'); await ensureSignedIn(); } catch (e2) {} }
  }
  // Top bar buttons last (Refresh / Reset failed AI trigger reloads).
  state.tab = 'shell';
  await crawl('shell', ['.topbar', '#groupContext'], 10, 0);
  await context.close();

  async function chartResize(tab) {
    const measure = () => page.evaluate(() => ['chartSpendRev', 'chartClicks', 'salesTrend'].map(id => { const host = document.getElementById(id), svg = host && host.querySelector('svg'); if (!svg || !host.offsetParent) return null; const vb = svg.viewBox && svg.viewBox.baseVal, r = svg.getBoundingClientRect(), hr = host.getBoundingClientRect(); return { id, host: Math.round(hr.width), svg: Math.round(r.width), vb: vb ? Math.round(vb.width) : null }; }).filter(Boolean));
    await settle(400, 6000);
    const before = await measure();
    if (vp.phone) { before.forEach(m => { if (m.svg > m.host + 1) add('chart wider than its card', vp.name, tab, m, m.id); if (m.vb && m.svg / m.vb < 0.75) add('chart text scaled down (fixed viewBox)', vp.name, tab, Object.assign({ scale: Math.round(m.svg / m.vb * 100) / 100 }, m), m.id); }); return; }
    await page.setViewportSize({ width: 1024, height: vp.height }); await page.waitForTimeout(700);
    const mid = await measure();
    await page.setViewportSize({ width: 760, height: vp.height }); await page.waitForTimeout(700);
    const small = await measure();
    await page.setViewportSize({ width: vp.width, height: vp.height }); await page.waitForTimeout(700);
    const after = await measure();
    for (const set of [before, mid, small, after]) set.forEach(m => { if (m.svg > m.host + 1) add('chart wider than its card', vp.name, tab, m, m.id); if (m.vb && (m.svg / m.vb > 1.35 || m.svg / m.vb < 0.75)) add('chart text scaled (viewBox not redrawn for width)', vp.name, tab, Object.assign({ scale: Math.round(m.svg / m.vb * 100) / 100 }, m), m.id + '|' + (m.svg / m.vb > 1 ? 'up' : 'down')); });
    notes.push(`${vp.name} chart widths 1440/1024/760/1440: ` + before.map(m => m.id + ' ' + [m, (mid.find(x => x.id === m.id) || {}), (small.find(x => x.id === m.id) || {}), (after.find(x => x.id === m.id) || {})].map(x => x.svg + '/' + x.vb).join(' → ')).join('; '));
  }
}

function report(started) {
  const list = [...findings.values()];
  const byTab = {};
  list.forEach(f => f.tabs.forEach(t => { (byTab[t] = byTab[t] || []).push(f); }));
  const covOut = {};
  Object.entries(coverage).forEach(([k, v]) => { covOut[k] = Object.assign({}, v, { families: v.families.size }); });
  const actions = {}; apiLog.forEach(a => { actions[a.action] = (actions[a.action] || 0) + 1; });
  const out = { startedAt: new Date(started).toISOString(), seconds: Math.round((Date.now() - started) / 1000), viewports: VIEWPORTS.map(v => v.name), tabs: TABS, coverage: covOut, apiActions: actions, notes, findings: list };
  fs.writeFileSync(path.join(OUT, 'report.json'), JSON.stringify(out, null, 1));
  const lines = [];
  lines.push(`Brites console browser walk · ${out.seconds}s · ${list.length} distinct findings · report ${path.relative(REPO, path.join(OUT, 'report.json'))}`);
  for (const tab of ['gate', 'shell'].concat(TABS).concat(['any'])) {
    const fs2 = (byTab[tab] || []); if (!fs2.length) continue;
    lines.push('\n== ' + (TAB_NAMES[tab] || tab) + ' ==');
    const kinds = {}; fs2.forEach(f => { (kinds[f.kind] = kinds[f.kind] || []).push(f); });
    Object.entries(kinds).sort((a, b) => a[0].localeCompare(b[0])).forEach(([k, arr]) => {
      lines.push(`  ${k} (${arr.length})`);
      arr.slice(0, 8).forEach(f => lines.push('    - [' + f.viewports.join(',') + '] ' + JSON.stringify(f.detail).slice(0, 260)));
      if (arr.length > 8) lines.push('    … ' + (arr.length - 8) + ' more in report.json');
    });
  }
  lines.push('\nCoverage (clicked / selects / inputs / chart marks / dialogs / disabled / skipped / vanished / control families):');
  Object.entries(covOut).forEach(([k, v]) => lines.push(`  ${k}: ${v.clicked} / ${v.selects} / ${v.inputs} / ${v.charts} / ${v.dialogs} / ${v.disabled} / ${v.skipped} / ${v.vanished} / ${v.families}`));
  lines.push('API actions answered: ' + Object.entries(actions).sort((a, b) => b[1] - a[1]).map(([a, n]) => a + '×' + n).join(' '));
  if (notes.length) lines.push('Notes:\n  ' + notes.join('\n  '));
  console.log(lines.join('\n'));
  fs.writeFileSync(path.join(OUT, 'summary.txt'), lines.join('\n'));
}

main().catch(e => { console.error(e); process.exitCode = 1; });
