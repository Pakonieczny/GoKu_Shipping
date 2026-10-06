// The Employee efficiency portal (Real view) as Paul reads it: the sorter app (charm-nest-1.html) with the console open, on its own browser profile,
// signed in as the Admin. Readers turn what is ON SCREEN into plain objects, so a check says what the page says and can set it beside the shop's own
// answer (World.eff: the real employeeEfficiency reader over the same Firestore).
'use strict';
const { sleep } = require('./world.cjs');
const V = '#efficiencyView';

function Portal(W, cast) {
  const P = { page: null, ctx: null };
  P.V = V;

  P.open = async function (o) {
    o = o || {};
    const ctx = P.ctx = await W.context({ label: 'portal', ctx: { viewport: { width: 1440, height: 1000 } } });
    await ctx.addInitScript(([pass, who]) => {
      try { if (location.hostname === '127.0.0.1') {
        localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', sandbox: 'off', sandboxStream: 'off' }));
        if (who) localStorage.setItem('cn.employee', who);
        if (!sessionStorage.getItem('__seeded')) { sessionStorage.setItem('__seeded', '1'); sessionStorage.setItem('cn.eff.key', pass); }
      } } catch (_) {}
      window.confirm = () => true; window.prompt = () => null;
    }, [W.pass, o.who || '']);
    const page = P.page = await W.open(ctx, 'charm-nest-1.html', { sorter: true });
    await page.waitForFunction(() => window.Efficiency && window.EfficiencyStations && window.EfficiencyEmployee && window.CN && window.OrderWin && document.readyState === 'complete', null, { timeout: 90000 });
    // the console polls faster than its shipped timings (the test is short); nothing else about it changes
    await page.evaluate(() => { try { Object.assign(Efficiency.options, { pollMs: 2500, liveMs: 800, growMs: 0, nudgeMs: 1000 }); Object.assign(EfficiencyEmployee.options, { liveMs: 800, rangeMs: 1500, calMs: 1500, ordersMs: 1500, tickMs: 500 }); } catch (_) {} });
    await page.evaluate(() => { Efficiency.open(); });
    await page.waitForSelector(`${V}:not(.hidden)`, { timeout: 30000 });
    await page.waitForSelector(`${V} .efTabBtn`, { timeout: 30000 });
    return page;
  };
  P.tab = async function (name) {
    const page = P.page;
    await page.click(`${V} .efTabBtn[data-tab="${name}"]`);
    await sleep(300);
  };
  const view = () => P.page.locator(V).getAttribute('data-view');
  P.view = view;

  /** the Stations tab as data: every station card with its state, the people chips, today's counts and the orders in hand */
  P.board = async function () {
    await P.tab('stations');
    await P.page.waitForSelector(`${V} .es .esSt`, { timeout: 30000 });
    return P.page.$$eval(`${V} .es .esSt`, rows => rows.map(r => {
      const txt = el => (el ? el.innerText.replace(/\s+/g, ' ').trim() : '');
      const shown = el => !!el && !el.hidden && el.offsetParent !== null;
      // people: the chips of the station, plus (the Welding card) the people of its two task groups; one entry per name, with the task when a group says it
      const people = [...r.querySelectorAll('.esPeople .esPer')].map(c => ({ name: c.dataset.name || txt(c.querySelector('.esPn')), text: txt(c), task: c.dataset.task || '', role: c.dataset.role || '' }));
      for (const c of r.querySelectorAll('.esWeld .esWp')) { const nm = c.dataset.name; const had = people.find(p => p.name === nm); if (had) { had.task = had.task || c.dataset.task || ''; } else people.push({ name: nm, text: txt(c), task: c.dataset.task || '', role: '' }); }
      // the Welding card's groups (WS2: .esGrp[data-task=welding|matching]); an older draft used [data-group]
      const groups = [...r.querySelectorAll('.esGrp[data-task], [data-group]')].map(g => ({ group: g.dataset.task || g.dataset.group, text: txt(g), people: [...g.querySelectorAll('.esWp, .esPer')].map(c => c.dataset.name || txt(c.querySelector('.esPn'))).filter((n, i, a) => n && a.indexOf(n) === i) }));
      const wl = r.querySelector('.esWeld');
      return {
        key: r.dataset.key, state: r.dataset.state, name: txt(r.querySelector('.esStName')), stateWord: txt(r.querySelector('.esStState')),
        people, groups,
        weld: wl ? { visible: shown(wl), text: txt(wl), matchedHead: txt(r.querySelector('.esMtN')), matchedToday: txt(r.querySelector('.esCntW')), rows: [...r.querySelectorAll('.esMr')].map(m => ({ order: txt(m.querySelector('.esMrId')), who: txt(m.querySelector('.esMrW')), text: txt(m) })) } : null,
        counts: txt(r.querySelector('.esCnt')), countsVisible: shown(r.querySelector('.esCnt')),
        cards: [...r.querySelectorAll('.esCard')].map(c => ({ rid: c.dataset.rid, who: txt(c.querySelector('.esWho')), text: txt(c) })),
        idle: txt(r.querySelector('.esIdle')), text: txt(r)
      };
    }));
  };
  P.signedInNow = async function () {
    await P.tab('overview');
    await P.page.waitForSelector(`${V} .efBody:not(.hidden)`, { timeout: 30000 });
    return P.page.$$eval(`${V} .efSiGrid .efSi`, cs => cs.map(c => ({ name: (c.querySelector('.efSiNm') || {}).textContent.trim(), text: c.innerText.replace(/\s+/g, ' ').trim() })));
  };
  P.overviewText = async function () { await P.tab('overview'); return P.page.locator(V).innerText().then(s => s.replace(/\s+/g, ' ').trim()); };
  P.kpi = async function (k) { await P.tab('overview'); return P.page.locator(`${V} .efKpi[data-k="${k}"] .efKV`).first().innerText().then(s => s.replace(/,/g, '').trim()); };

  /** the Overview's Stations card: one row per station with the people named and today's pieces and orders */
  P.overviewStations = async function () {
    await P.tab('overview');
    await P.page.waitForSelector(`${V} .efStations .efSR`, { timeout: 30000 });
    return P.page.$$eval(`${V} .efStations .efSR`, rows => rows.map(r => {
      const t = el => (el ? el.innerText.replace(/\s+/g, ' ').trim() : '');
      const n = c => { const b = r.querySelector(`.efSV[data-c="${c}"] b`); return b ? Number(String(b.textContent).replace(/[^\d.]/g, '')) : null; };
      // (the Welding row draws "matched" and the time on task in the two columns where the others draw pieces and orders: the labels and the raw text say which)
      return { key: r.dataset.station, on: /\bon\b/.test(r.className), name: t(r.querySelector('.efSN')), who: t(r.querySelector('.efSW')), parts: n('parts'), orders: n('orders'),
        weld: r.dataset.weld === '1', partsLabel: t(r.querySelector('.efSV[data-c="parts"] small')), ordersLabel: t(r.querySelector('.efSV[data-c="orders"] small')), partsText: t(r.querySelector('.efSV[data-c="parts"]')), ordersText: t(r.querySelector('.efSV[data-c="orders"]')) };
    }));
  };
  /** the People tab: a card per person (the Overview's roster) with the four figures it shows */
  P.rosterCards = async function () {
    await P.tab('people');
    await P.page.waitForSelector(`${V} .efRoster .efRc`, { timeout: 30000 });
    return P.page.$$eval(`${V} .efRoster .efRc`, cards => cards.map(c => {
      const t = el => (el ? el.innerText.replace(/\s+/g, ' ').trim() : '');
      return { name: c.dataset.name, shown: t(c.querySelector('.efRcName')), when: t(c.querySelector('.efRcWhen')), live: !!c.querySelector('.efRcLive'),
        figs: [...c.querySelectorAll('.efRcFig div')].map(d => ({ label: t(d.querySelector('span')), value: t(d.querySelector('b')) })), chips: [...c.querySelectorAll('.efRcChips > *')].map(t), text: t(c) };
    }));
  };
  /** one person's page as it reads: header line, the station chips with their time, the figures, the station mix, and the whole text */
  P.personPage = async function (name, o) {
    o = o || {};
    await P.tab('people');
    await P.page.waitForSelector(`${V} .efRoster .efRc[data-name="${name}"]`, { timeout: 30000 });
    await P.page.click(`${V} .efRoster .efRc[data-name="${name}"]`);
    await P.page.waitForSelector(`${V} .efp`, { timeout: 30000 });
    if (o.range) { await P.page.click(`${V} .efpSeg button[data-range="${o.range}"]`).catch(() => {}); }
    await sleep(o.settle || 2500);
    return P.readPerson();
  };
  P.readPerson = async function () {
    return P.page.evaluate(sel => {
      const root = document.querySelector(sel + ' .efp'); const t = el => (el ? el.innerText.replace(/\s+/g, ' ').trim() : '');
      const kpis = {}; root.querySelectorAll('.efpK[data-k]').forEach(k => { kpis[k.dataset.k] = t(k.querySelector('.efpKV')); });
      return { name: t(root.querySelector('.efpName')), where: t(root.querySelector('.efpWhere')), chips: t(root.querySelector('.efpChips')), chipList: [...root.querySelectorAll('.efpChips > *')].map(t),
        kpis, mix: t(root.querySelector('[data-c="mix"]')), now: t(root.querySelector('.efpNowS')), text: t(root) };
    }, V);
  };
  P.back = async function () {
    await P.page.click(`${V} .efpBack`).catch(() => {});
    await sleep(400);
  };

  return P;
}
module.exports = { Portal, V };
