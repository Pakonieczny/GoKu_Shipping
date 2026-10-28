const { chromium } = require('/opt/node-tools/node_modules/playwright-core');
(async () => {
  const b = await chromium.launch({ executablePath: '/opt/pw-browsers/chromium-1243/chrome-linux64/chrome' });
  for (const u of ['charm-nest-1.html', 'shipping-1.html', 'assembly-1.html', 'weld-1.html', 'design-message.html', 'sorting.html', 'etsy-mail-1.html']) {
    const p = await b.newPage(), errs = [];
    p.on('pageerror', e => errs.push('pageerror: ' + e.message.slice(0, 160)));
    p.on('console', m => { if (m.type() === 'error') errs.push('console: ' + m.text().slice(0, 160)); });
    const r = await p.goto('https://goldenspike.app/' + u, { waitUntil: 'load', timeout: 45000 }).catch(e => ({ status: () => 'ERR ' + e.message.slice(0, 80) }));
    await p.waitForTimeout(4000);
    console.log(u, r.status(), errs.length ? errs.join(' | ') : 'no errors');
    await p.close();
  }
  await b.close();
})();
