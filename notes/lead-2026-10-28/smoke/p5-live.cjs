// read-only live smoke test of Phase 5 (Sandbox world): the page listens to Sandbox_Charm_Nest_Push/counter; no write op may leave it
const { chromium } = require('playwright');
const READ = /^(get|list|ping|lookup|history|check|changes)|(Get|List|Check|Status|Preview|Read|Url|Info|Changes)$/;
(async () => {
  const b = await chromium.launch(); const ctx = await b.newContext();
  await ctx.addInitScript(() => { try {
    if (!localStorage.getItem('cn.settings')) localStorage.setItem('cn.settings', JSON.stringify({ v: 26, sandbox: 'on', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' }));
    localStorage.setItem('cn.employee', 'Smoke Test'); localStorage.setItem('cn.push', 'on'); } catch (e) {} });
  const p = await ctx.newPage(); const fb = [], blocked = [], fn = [], errs = [];
  p.on('pageerror', e => errs.push(String(e).slice(0, 200)));
  p.on('console', m => { if (m.type() === 'error') errs.push('console: ' + m.text().slice(0, 200)); });
  await p.route('**/*', async r => {
    const q = r.request(), u = q.url();
    if (/\/\.netlify\/functions\//.test(u) && q.method() !== 'GET') {
      let op = ''; try { op = JSON.parse(q.postData() || '{}').op || ''; } catch (e) {}
      if (!READ.test(op)) { blocked.push(op + ' ' + u.split('/').pop()); return r.abort(); }
      fn.push(op);
    }
    return r.continue();
  });
  p.on('response', async res => { const u = res.url(); if (/firestore\.googleapis\.com/.test(u)) fb.push(res.request().method() + ' ' + res.status() + ' ' + u.replace(/^https:\/\/firestore\.googleapis\.com/, '').slice(0, 150)); });
  p.on('requestfailed', r => { if (/firestore\.googleapis\.com/.test(r.url())) fb.push('FAILED ' + (r.failure() || {}).errorText + ' ' + r.url().slice(0, 120)); });
  await p.goto('https://goldenspike.app/charm-nest-1.html?push=on', { waitUntil: 'domcontentloaded', timeout: 90000 });
  const t0 = Date.now(); let st;
  for (;;) {
    st = await p.evaluate(() => { const s = window.CNSync && window.CNSync.push ? window.CNSync.push.status() : null; const el = document.getElementById('stLiveUpdates'); return { world: window.CNSync && window.CNSync.world, s: s && { text: s.text, healthy: s.healthy, listening: s.listening, why: s.why, snapshots: s.snapshots, prefix: s.prefix, off: s.off, loaded: s.loaded }, row: el ? el.textContent : null }; }).catch(e => ({ err: String(e).slice(0, 120) }));
    if ((st.s && st.s.healthy) || Date.now() - t0 > 45000) break; await p.waitForTimeout(1000);
  }
  await p.waitForTimeout(8000);
  const st2 = await p.evaluate(() => { const s = window.CNSync && window.CNSync.push ? window.CNSync.push.status() : null; return s && { text: s.text, snapshots: s.snapshots, healthy: s.healthy }; }).catch(() => null);
  console.log('after ' + (Date.now() - t0) + ' ms', JSON.stringify(st), '\nlater', JSON.stringify(st2));
  console.log('firestore requests (' + fb.length + '):\n' + fb.slice(0, 20).join('\n'));
  console.log('function reads sent: ' + fn.length + ' · WRITES BLOCKED: ' + JSON.stringify(blocked));
  console.log('page errors: ' + errs.length + ' ' + errs.slice(0, 4).join(' | '));
  await b.close();
})().catch(e => { console.log('FAILED', e); process.exit(1); });
