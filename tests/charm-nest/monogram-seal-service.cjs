// MONOSEAL (Paul, 17 Oct 2026: "a brand new seal that is specific for monograms and the stamping, an animation, whenever a monogram is approved ... in all the same places as the engraving stamp and seal animations"):
// the pieces of the monogram seal that need no browser, in node (jsdom for the faces and the widget's DOM). The page-level run is tests/charm-nest/monogram-seal.cjs (the real sorter, motion on).
//   A  the seal's FACE: Seal.FAMILY.monogram (an arch), kind monogramApproved, the three letters of THIS monogram in the icon place, the pictogram when they are not known, every text inside the outline at three times of the day,
//      the aria text, the model, a process seal that turns right like the engraving approval
//   B  the SERVICE over fakes (a fake engine and CustomSheet, the real Seal faces; Seal.press is a gate this test holds): who presses (a name, a promise, a name bar put away = nothing), the design is added, the stamp is laid unseen
//      and pressed BEFORE the card says Approved (describe() says "not approved" and the Approve button reads "Approved" while the stamp comes down; the page is told nothing, no event is written until the ink is down),
//      then the timeline event (type, id, station, small data), the kept stamp (settings.mono.seals), a second approval adds a NEW seal and the earlier one stays, a reopened service shows the seals at rest and presses nothing,
//      another computer's stamps come from the timeline (feedEvents / loadSeals, one press = one seal), a line of two pieces keeps each piece's own, a page with no Seal still records
//   C  the TIMELINE tables: the server keeps the type (clean), as a person's action and not a milestone; the client knows it (TYPES, record); the timeline UI draws the monogram stamp, as a person's seal and never the Engraved step
//   node tests/charm-nest/monogram-seal-service.cjs
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { JSDOM } = require('jsdom'), { createCanvas, Path2D } = require('@napi-rs/canvas');
const fails = []; let nCheck = 0;
const check = (ok, msg) => { nCheck++; if (!ok) fails.push(msg); if (!ok || process.env.VERBOSE) console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
const section = t => console.log('\n── ' + t);
const tick = async (n = 30) => { for (let i = 0; i < n; i++) await Promise.resolve(); await new Promise(r => setTimeout(r, 0)); };

(async () => {
  const dom = new JSDOM('<main id="host"></main><main id="timeline"></main>', { url: 'http://127.0.0.1', runScripts: 'outside-only', pretendToBeVisual: true }), w = dom.window, doc = w.document;
  w.matchMedia = () => ({ matches: false }); w.requestAnimationFrame = () => 1; w.setInterval = () => 1;
  w.Element.prototype.getAnimations = () => []; w.Element.prototype.animate = () => ({ finished: Promise.resolve(), cancel() {}, playState: 'finished' });
  w.eval(fs.readFileSync(path.join(root, 'charm-nest-motion.js'), 'utf8'));
  const Seal = w.Seal, AT = Date.UTC(2026, 9, 17, 18, 5);

  // ═══ A · the face ═══
  section('A · the face');
  {
    const F = Seal.FAMILY, names = Object.keys(F);
    check(names.length === 9 && names[8] === 'monogram' && F.monogram.name === 'Monogram' && /^M8 104V60A52 52/.test(F.monogram.d), 'nine families, the ninth is the monogram arch: ' + names.join(','));
    check(new Set(names.map(k => F[k].d)).size === 9 && new Set(names.map(k => F[k].ink)).size === 9, 'its outline and its ink differ from the other eight (' + F.monogram.ink + ')');
    const ctx = createCanvas(120, 120).getContext('2d'), failures = []; let texts = 0;
    for (const letters of [undefined, 'APV', 'WMW', 'IJI', 'QGO']) for (const hour of [2, 10, 23]) {
      const holder = doc.createElement('div'); holder.innerHTML = Seal.face({ family: 'monogram', action: 'MONOGRAM', icon: 'monogram', letters, at: AT + hour * 3600000, by: 'Original Person' });
      const svg = holder.firstChild, path2d = new Path2D(svg.querySelector('[data-seal-outline]').getAttribute('d'));
      for (const t of svg.querySelectorAll('text')) {
        const size = +t.getAttribute('font-size'), y = +t.getAttribute('y'), x = +t.getAttribute('x'), anchor = t.getAttribute('text-anchor'), fitted = t.hasAttribute('data-seal-text');
        const width = fitted ? +t.getAttribute('textLength') : (ctx.font = `700 ${size}px serif`, ctx.measureText(t.textContent).width);
        ctx.font = `700 ${size}px Arial`; const m = ctx.measureText(t.textContent), top = y - m.actualBoundingBoxAscent, bottom = y + m.actualBoundingBoxDescent;
        const left = fitted ? 60 - width / 2 : anchor === 'end' ? x - width : anchor === 'start' ? x : x - width / 2, right = left + width;
        for (const xx of [left, left + 1, right - 1, right]) for (const yy of [top, top + 1, bottom - 1, bottom]) if (!ctx.isPointInPath(path2d, xx, yy)) failures.push([letters, t.textContent, xx, yy]);
        texts++;
      }
    }
    check(failures.length === 0 && texts >= 15 * 4, 'every text of the seal (the word, the date, the time, the three letters) stays inside the arch at three times of the day, with and without letters (' + texts + ' checked)' + (failures.length ? ' ' + JSON.stringify(failures.slice(0, 3)) : ''));
    const holder = doc.createElement('div');
    holder.innerHTML = Seal.face({ family: 'monogram', action: 'MONOGRAM', icon: 'monogram', letters: 'APV', at: AT, by: 'Original Person' });
    const txt = [...holder.querySelectorAll('text')].map(t => t.textContent);
    check(txt.slice(0, 3).join('') === 'APV' && txt.includes('MONOGRAM') && txt.includes('17 OCT 2026') && txt.length === 6 && +holder.querySelectorAll('text')[1].getAttribute('font-size') === 25 && +holder.querySelectorAll('text')[0].getAttribute('font-size') === 14, 'with letters: a small A, a large P, a small V, then MONOGRAM, the date and the time: ' + txt.join(' | '));
    holder.innerHTML = Seal.face({ family: 'monogram', action: 'MONOGRAM', icon: 'monogram', at: AT, by: 'X' });
    check([...holder.querySelectorAll('text')].length === 3 && holder.querySelector('path[d^="M0.8 20"]'), 'without letters: the pictogram of three letters instead (no letter text)');
    holder.innerHTML = Seal.face({ family: 'monogram', action: 'MONOGRAM', letters: 'a1b', at: AT }); check([...holder.querySelectorAll('text')].length === 3, 'letters that are not three capitals are never drawn');
    const st = { id: 'monogramApproved:' + AT + ':Sam', how: 'monogramApproved', at: AT, by: 'Sam', letters: 'apv', n: 2, of: 3 }, html = Seal.html(st, 50, 'monogramSeal');
    const m = Seal.modelOf(st);
    check(m.family === 'monogram' && m.action === 'MONOGRAM' && m.icon === 'monogram' && m.letters === 'APV' && m.of === 3 && m.by === 'Sam' && m.at === AT, 'modelOf: family, action, icon, the capitals of the letters, the piece, who and when: ' + JSON.stringify(m));
    check(/class="seal seal-monogramApproved monogramSeal"/.test(html) && html.includes(`data-process-seal="${st.id}"`) && /data-seal-family="monogram"/.test(html) && /--rot:(\d|1[0-2])deg/.test(html), 'Seal.html: kind monogramApproved, a process seal, turned right like the engraving approval (' + (/--rot:(-?\d+)deg/.exec(html) || [])[1] + 'deg)');
    check(/^Monogram APV approved by Sam · .*2026.* · piece 2 of 3$/.test(Seal.titleOf(st)) && /^Monogram approved · operator not recorded · /.test(Seal.titleOf({ how: 'monogramApproved', at: AT })) && !/piece/.test(Seal.titleOf({ how: 'monogramApproved', at: AT, by: 'Sam', letters: 'APV', n: 1, of: 1 })), 'titleOf: who, when, the letters and the piece of a line of several (none for one piece, "operator not recorded" without a name): ' + Seal.titleOf(st));
    check(Seal.INK.monogramApproved === F.monogram.ink, 'INK names the kind so the stamp tool takes the seal\'s colour');
    const tool = doc.createElement('div'); tool.innerHTML = Seal.tool('monogramApproved', Seal.html(st, 50)); check(tool.querySelector('svg').dataset.stampFamily === 'monogram' && tool.querySelector('[data-stamp-contact]').getAttribute('fill') === F.monogram.ink && tool.querySelector('[data-stamp-contact]').getAttribute('d') === F.monogram.d, 'the wooden stamp is made from the seal\'s own arch, in its ink');
  }

  // ═══ B · the service over fakes ═══
  section('B · the service');
  const UI = require(path.join(root, 'charm-nest-monogram-ui.js'));
  const lineOf = (rid, tx, qty, words) => ({ key: `${rid}_${tx}`, state: 'pulled', order: { receiptId: rid }, line: { transactionId: tx, quantity: qty, sku: 'MONOGRAM_5351' }, poolIds: [], spec: { quantity: qty, personalization: [words], special: { monogram: true, words }, material: 'gold' } });
  const fakeResult = () => ({ ok: true, bboxPt: [0, 0, 40, 20], wMm: 14, hMm: 7, report: { metrics: { violations: [] } } });
  const Engine = { lettersOf: t => { const L = String(t || '').replace(/[^A-Za-z]/g, '').toUpperCase(); return L.length === 3 ? { ok: true, letters: L.split(''), why: '' } : { ok: false, letters: null, why: 'not three letters' }; },
    toPdf: async () => new Uint8Array([1, 2, 3]), svgPath: () => 'M0 0H10V10Z', familiesOf: () => [], slotsOf: () => ({ ok: true, left: { base: 'L{L}', count: 26, name: 'L' }, middle: { base: 'M{L}', count: 26, name: 'M' }, right: { base: 'R{L}', count: 26, name: 'R' }, sets: [], by: {} }) };
  function world(rows, o = {}) {
    const store = o.store || {}, settings = o.settings || {}, log = { presses: [], records: [], changes: 0, saves: 0, toasts: [], who: 0 }, hold = { gate: null };
    let ids = 0;
    const CS = { entries: () => store, cardOf: it => (it ? { ck: it.key } : null), open() {},
      add(it, files, opts = {}) { const e = store[it.key] || (store[it.key] = { ck: it.key, rid: it.rid, files: [], sent: null }); for (const f of files) e.files.push(Object.assign({ id: 'f' + (++ids), name: f.name, size: f.size, state: 'ready', metal: 'gold', qty: opts.qty || 1, pieces: 1, wMm: 14, hMm: 7 }, opts.mono ? { mono: Object.assign({ line: String(opts.mono.line), slot: opts.mono.slot || null }, opts.mono.hoops ? { hoops: opts.mono.hoops } : {}) } : {})); },
      remove(ck, id) { const e = store[ck]; if (!e) return; e.files = e.files.filter(f => f.id !== id); if (!e.files.length) delete store[ck]; } };
    // the real Seal faces; Seal.press is a gate: it records what it was asked to press and waits for the test to let go
    const SealFake = Object.assign({}, Seal, { press: el => { const rec = { el, pending: el.classList.contains('pending'), at: log.presses.length }; log.presses.push(rec); return new Promise(res => { hold.gate = () => { el.classList.remove('pending'); res(); }; if (o.autoPress) hold.gate(); }); }, fit() {} });
    const d = { doc, Engine, rows: () => rows, skus: () => [], settings: () => settings, saveSettings() { log.saves++; }, toast: m => log.toasts.push(m), changed: () => { log.changes++; }, CNProgress: null,
      itemOf: row => ({ key: 'custom:' + row.order.receiptId, rid: row.order.receiptId, rows: [row], row }), CustomSheet: CS, Master: { entryFor: () => null, fetchEntry: async () => null }, Pool: {}, Geom: {}, P: {},
      Seal: o.noSeal ? null : SealFake, who: o.who === undefined ? () => 'Ann Operator' : o.who, record: (row, ev) => { log.records.push({ row, ev }); return true; }, events: o.events };
    if (o.noWho) delete d.who; if (o.noRecord) delete d.record;
    const ui = UI.make(d);
    // a piece whose monogram is made (the build is not what this test is about)
    const ready = (row, n, letters) => { const c = (ui.status(row, n), ui.cards.get(ui.pieceKey(row, n))); c.status = 'ready'; c.result = fakeResult(); c.resultLetters = letters; c.resultKey = 'k-' + letters; c.draft = letters.split(''); c.aiKey = c.resultKey; c.ai = null; return c; };
    return { ui, d, store, settings, log, hold, ready, CS };
  }
  const widgetOf = (host, ui, row, o) => { host.replaceChildren(); return ui.mountLine(host, Object.assign({ row, lineKey: row.key, rid: row.order.receiptId, mode: 'detail' }, o || {})); };
  const mwOf = host => host.querySelector('.mw'), rowOf = host => host.querySelector('.mwSeal') || (host.parentElement && host.parentElement.querySelector('.mwSeal'));
  const btn = host => host.querySelector('[data-mw-approve]'), okEl = host => host.querySelector('[data-mw-approved]');
  {
    const one = lineOf('4200683017', '111', 1, 'APV'), host = doc.getElementById('host');
    // — a name bar put away: nothing happens —
    let W = world([one], { who: () => Promise.resolve('') });
    let c = W.ready(one, null, 'APV'); let wd = widgetOf(host, W.ui, one); await tick();
    check(mwOf(host).dataset.state === 'ready' && !btn(host).disabled && !btn(host).hidden, 'a ready monogram: Approve is there');
    btn(host).click(); await tick(60);
    check(Object.keys(W.store).length === 0 && W.log.presses.length === 0 && W.log.records.length === 0 && !c.seals.length && mwOf(host).dataset.state === 'ready', 'who says nobody (the name bar put away): nothing is added, stamped or recorded, the card is as it was');
    // — a name arriving late —
    let release, asked = 0; W = world([one], { who: () => (asked++ ? 'Ann Operator' : new Promise(r => { release = r; })) });   // (the name bar the first time; this person is signed in after)
    c = W.ready(one, null, 'APV'); wd = widgetOf(host, W.ui, one); await tick();
    btn(host).click(); await tick(); btn(host).click(); await tick();
    check(c.asking && Object.keys(W.store).length === 0, 'while the name bar is open nothing is approved yet (and a second press asks nothing more)');
    release('Ann Operator'); await tick(80);
    check(W.log.presses.length === 1 && Object.keys(W.store).length === 1 && W.store['custom:4200683017'].files.length === 1, 'the name arrives: ONE design is added and ONE seal is pressed');
    // — while the stamp comes down —
    const press = W.log.presses[0], seal = press.el;
    check(seal.closest('.mwSeal') && seal.dataset.processSeal.startsWith('monogramApproved:') && seal.querySelector('svg').dataset.sealFamily === 'monogram' && press.pending, 'the new seal sits unseen (pending) in the widget\'s seal row, a monogram seal');
    check(btn(host).textContent === 'Approved' && btn(host).disabled && !btn(host).hidden && okEl(host).hidden && mwOf(host).dataset.state === 'ready', 'the Approve button reads "Approved", cannot be pressed, and the card does not yet say Approved: ' + btn(host).textContent);
    check(!W.ui.status(one).approved && W.ui.describe(one).approved === null && W.ui.describe(one).stamping === true && !W.ui.describe(one).canApprove && W.log.changes === 0 && W.log.records.length === 0, 'the page is told nothing while the ink is not down: not approved, no change, no timeline event');
    check([...host.querySelectorAll('[data-mw-letter]')].every(i => i.disabled), 'the letter boxes wait too');
    W.hold.gate(); await tick(80);
    // — the ink is down —
    check(!btn(host).textContent.includes('Approved') || btn(host).hidden, 'the ink is down: the Approve button goes') ; check(btn(host).hidden && !okEl(host).hidden && /Approved · Monogram APV/.test(okEl(host).textContent) && mwOf(host).dataset.state === 'approved', 'and only now the card says "Approved · Monogram APV"');
    check(W.ui.status(one).approved === true && W.ui.status(one).seals === 1 && W.log.changes === 1, 'the page is told once, after the stamp: approved, one seal');
    check(W.log.records.length === 1, 'ONE timeline event is written');
    const ev = W.log.records[0].ev, st = W.ui.sealsOf(one, null)[0];
    check(ev.type === 'monogramApproved' && ev.orderId === '4200683017' && ev.id === `${one.key}-${st.at}` && ev.at === st.at && ev.by === 'Ann Operator' && ev.station === 'sorter' && ev.device === 'charm-nest-1' && ev.lineKey === one.key && ev.transactionId === '111', 'the event: type, order, id "<line>-<time>", the stamp\'s own time and name, station sorter, device charm-nest-1: ' + JSON.stringify([ev.id, ev.at, ev.by]));
    check(ev.data.letters === 'APV' && ev.data.slot === null && ev.data.n === 1 && ev.data.of === 1 && ev.data.poolId === one.key + '_1' && ev.data.name === 'Monogram APV.ai' && ev.data.signedIn === true && JSON.stringify(ev.data).length < 400 && /^Monogram APV$/.test(ev.text), 'its data: letters, slot, piece, pool id, file, signed in; small: ' + JSON.stringify(ev.data));
    check(st.how === 'monogramApproved' && st.by === 'Ann Operator' && st.letters === 'APV' && st.n === 1 && !('of' in st) && st.id === `monogramApproved:${st.at}:Ann Operator`, 'the stamp: ' + JSON.stringify(st));
    check(JSON.stringify(W.settings.mono.seals[one.key]) === JSON.stringify([st]) && W.log.saves >= 1, 'kept on this computer: settings.mono.seals[pieceKey] holds the stamp and the settings were saved');
    check(!/Ann Operator|monogramApproved/.test(JSON.stringify(W.store)), 'the custom-sheet record (the design files) carries nothing of the seal');
    check(rowOf(host).children.length === 1 && !rowOf(host).children[0].classList.contains('pending') && rowOf(host).dataset.sealCount === '1' && !rowOf(host).hidden, 'the row holds the one seal at rest');
    // — Change, another word: a NEW seal, the earlier stays —
    btn(host).hidden = false;   // (the widget's Change: unlock and type)
    host.querySelector('[data-mw-change]').click(); await tick();
    W.ui.cards.get(one.key).resultLetters = 'AQV'; W.ui.cards.get(one.key).resultKey = 'k-AQV'; W.ui.cards.get(one.key).draft = ['A', 'Q', 'V']; W.ui.cards.get(one.key).status = 'ready'; W.ui.cards.get(one.key).result = fakeResult(); wd.refresh(); await tick();
    check(rowOf(host).children.length === 1, 'while the changed monogram waits, the earlier seal stays');
    btn(host).click(); await tick(60);
    check(W.log.presses.length === 2 && rowOf(host).children.length === 2, 'approved again: a NEW seal is pressed beside the earlier one (2 seals)');
    W.hold.gate(); await tick(80);
    const both = W.ui.sealsOf(one, null);
    check(both.length === 2 && both[0].letters === 'APV' && both[1].letters === 'AQV' && both[0].at <= both[1].at && W.log.records.length === 2 && W.log.records[1].ev.data.replaced === true && [...rowOf(host).children].map(s => /Monogram (\w+)/.exec(s.getAttribute('aria-label'))[1]).join() === 'APV,AQV', 'two stamps, oldest first, two events (the second says it replaced a design): ' + both.map(s => s.letters).join());
    check(Object.keys(W.store).length === 1 && W.store['custom:4200683017'].files.length === 1 && W.store['custom:4200683017'].files[0].name === 'Monogram AQV.ai', 'the design was replaced, never two');
    // — reopened: a new service over the same settings and designs shows the seals at rest, presses nothing —
    const W2 = world([one], { store: W.store, settings: W.settings, who: () => 'Ann Operator' });
    const c2 = W2.ready(one, null, 'AQV'); host.replaceChildren(); widgetOf(host, W2.ui, one); await tick(40);
    check(W2.ui.sealsOf(one, null).map(s => s.letters).join() === 'APV,AQV' && rowOf(host).children.length === 2 && [...rowOf(host).children].every(s => !s.classList.contains('pending')) && W2.log.presses.length === 0 && W2.log.records.length === 0, 'a service made again (a reload) shows both seals at rest: nothing pressed, nothing recorded');
    check(mwOf(host).dataset.state === 'approved' && c2.stamping === false, 'and the card says Approved (the design is on the order)');
    // — another computer: only the timeline knows —
    const W3 = world([one], { settings: {}, who: () => 'Zed', events: async rid => [{ type: 'monogramApproved', at: AT, by: 'Other Person', lineKey: one.key, orderId: rid, data: { letters: 'APV', n: 1, of: 1 } }, { type: 'monogramApproved', at: AT + 99 * 36e5, by: 'Third Person', lineKey: 'other-line', data: { letters: 'ZZZ' } }, { type: 'designDropped', at: AT, lineKey: one.key }] });
    W3.ready(one, null, 'APV'); host.replaceChildren(); widgetOf(host, W3.ui, one); await tick(60);
    check(W3.ui.sealsOf(one, null).length === 1 && W3.ui.sealsOf(one, null)[0].by === 'Other Person' && rowOf(host).children.length === 1 && !rowOf(host).children[0].classList.contains('pending') && W3.log.presses.length === 0, 'another computer\'s stamp comes from the order\'s timeline (read once when the card is opened): a seal at rest, no stamp comes down, other lines\' and other types\' events are not read');
    check(/approved by Other Person/.test(rowOf(host).children[0].getAttribute('aria-label')) && !btn(host).hidden, 'its seal says who approved it; this computer\'s card still offers Approve (the design file is on that other computer)');
    // the same press known twice is one seal
    W3.ui.feedEvents('4200683017', [{ type: 'monogramApproved', at: AT + 400, by: 'Other Person', lineKey: one.key, data: { letters: 'APV', n: 1 } }]); check(W3.ui.sealsOf(one, null).length === 1, 'the same press seen twice (a second apart) is ONE seal');
    W3.ui.feedEvents('4200683017', [{ type: 'monogramApproved', at: AT, by: 'Other Person', lineKey: one.key, data: { letters: 'APV', n: 1 } }, { type: 'monogramApproved', at: AT + 5 * 60000, by: 'Later Person', lineKey: one.key, data: { letters: 'AQV', n: 1 } }]);
    check(W3.ui.sealsOf(one, null).map(s => s.by).join() === 'Other Person,Later Person' && rowOf(host).children.length === 2, 'a stamp that arrives later is drawn by the open card (oldest first)');
    const html = UI.sealHtml(one, { ui: W3.ui }), htmlNone = UI.sealHtml(one, { ui: world([one], {}).ui });
    check(/^<span class="sealRow mwSeal"[^>]*data-seal-count="2"/.test(html) && (html.match(/class="seal seal-monogramApproved/g) || []).length === 2 && htmlNone === '', 'sealHtml(row): one row of the piece\'s seals for any surface, "" when there are none');
    check(UI.sealsOf({ key: 'x' }, {}).length === 0 && UI.sealHtml({ key: 'x' }, {}) === '', 'the module-level readers answer [] and "" when no service is made');
    host.replaceChildren();

    // — a line of two pieces —
    const two = lineOf('4290000141', '222', 2, 'JQX'), W4 = world([two], { who: () => 'Ann Operator', autoPress: true });
    W4.ready(two, 2, 'JQX'); const w4 = widgetOf(host, W4.ui, two, { slot: 'C2' }); await tick(); btn(host).click(); await tick(100);
    const e4 = W4.log.records.map(r => r.ev);
    check(W4.ui.sealsOf(two, { slot: 'C2' }).length === 1 && W4.ui.sealsOf(two, { slot: 'C1' }).length === 0 && W4.ui.sealsOf(two, 'C2')[0].of === 2 && W4.ui.sealsOf(two, 'C2')[0].n === 2, 'piece 2 has its seal (piece 2 of 2), piece 1 has none');
    check(e4.length === 1 && e4[0].id === `${two.key}#C2-${e4[0].at}` && e4[0].data.slot === 'C2' && e4[0].data.n === 2 && e4[0].data.of === 2 && e4[0].data.poolId === two.key + '_2' && /Piece 2 of 2/.test(e4[0].text) && W4.store['custom:4290000141'].files[0].name === 'Monogram JQX - piece 2.ai', 'its event names the piece: slot C2, pool id <line>_2, id "<line>#C2-<time>"');
    check(/piece 2 of 2/.test(rowOf(host).children[0].getAttribute('aria-label')) && JSON.parse(rowOf(host).children[0].querySelector('svg').dataset.sealModel).of === 2, 'and its seal says piece 2 of 2');
    check(UI.stampsFromEvents(e4.map(e => Object.assign({}, e, { by: e.by })), two.key, 1).length === 0 && UI.stampsFromEvents(e4, two.key, 2).length === 1 && UI.stampsFromEvents(e4, two.key).length === 1, 'stampsFromEvents separates the pieces of the line by slot / n');
    check(/data-seal-count="1"/.test(W4.ui.sealHtml(two, 'C2')) && W4.ui.sealHtml(two, 'C1') === '' && W4.ui.sealHtml(two, { slot: 'C2', size: 22 }).includes('data-seal-max="22"') && UI.sealHtml(two, { ui: W4.ui, slot: 'C2' }) !== '', 'the readers take the slot as a string (sealsOf(row, "C2"), sealHtml(row, "C2")) or as { slot, size, cls }');
    w4.destroy(); check(!host.querySelector('.mwSeal'), 'destroy takes the seal row away with the widget');
    // the compact widget holds its row as the host's next child (the card lays it across its corner)
    const W5 = world([one], { who: () => 'Ann Operator', autoPress: true }); W5.ready(one, null, 'APV'); const w5 = widgetOf(host, W5.ui, one, { mode: 'compact' }); await tick(); btn(host).click(); await tick(100);
    check(host.lastElementChild.classList.contains('mwSeal') && host.firstElementChild.classList.contains('mw') && host.lastElementChild.children.length === 1 && W5.log.presses.length === 1, 'the compact widget keeps its seal row as the host\'s next child (outside .mw), and presses in it');
    w5.destroy(); check(host.children.length === 0, 'and destroy leaves the host empty');
    // — two widgets on one piece: the stamp is pressed where the person pressed —
    const W6 = world([one], { who: () => 'Ann Operator' }); W6.ready(one, null, 'APV'); const hostA = doc.createElement('div'), hostB = doc.createElement('div'); doc.body.append(hostA, hostB);
    widgetOf(hostA, W6.ui, one); widgetOf(hostB, W6.ui, one, { mode: 'compact' }); await tick();
    btn(hostB).click(); await tick(80);
    check(W6.log.presses.length === 1 && hostB.contains(W6.log.presses[0].el) && !hostA.querySelector('.mwSeal .seal.pending') && hostA.querySelectorAll('.mwSeal .seal').length === 1, 'two widgets of one piece: the stamp is pressed in the widget Approve was pressed in; the other shows the seal (not pending)');
    W6.hold.gate(); await tick(60); check(btn(hostA).hidden && btn(hostB).hidden && !okEl(hostA).hidden && !okEl(hostB).hidden, 'and both say Approved once it is down');
    hostA.remove(); hostB.remove();
    // — a page with no Seal, and one with no name or timeline deps: the approval still stands and is recorded —
    const W7 = world([one], { noSeal: true }), c7 = W7.ready(one, null, 'APV'); host.replaceChildren(); widgetOf(host, W7.ui, one); await tick(); btn(host).click(); await tick(80);
    check(W7.log.presses.length === 0 && mwOf(host).dataset.state === 'approved' && W7.log.records.length === 1 && W7.ui.sealsOf(one, null).length === 1 && rowOf(host).hidden, 'a page with no Seal: approved at once, the stamp and the event still recorded, no row drawn');
    const W8 = world([one], { noWho: true, noRecord: true, noSeal: true }); W8.ready(one, null, 'APV'); await W8.ui.approve(W8.ui.cards.get(one.key)); await tick(20);
    check(W8.ui.status(one).approved === true && W8.ui.sealsOf(one, null)[0].by === '', 'a service made with no who / record (an older host): the approval is exactly what it was, the stamp has no name');
    host.replaceChildren();
  }

  // ═══ C · the timeline ═══
  section('C · the timeline');
  {
    const T = require(path.join(root, 'netlify/functions/_orderTimeline.js'));
    const ev = { orderId: '4200683017', type: 'monogramApproved', id: 'L#C2-1760000000000', at: Date.now() - 1000, by: 'Ann Operator', station: 'sorter', device: 'charm-nest-1', lineKey: 'L', transactionId: '111', text: 'Monogram JQX · Piece 2 of 2', data: { letters: 'JQX', slot: 'C2', n: 2, of: 2, poolId: 'L_2', name: 'Monogram JQX - piece 2.ai', ck: 'custom:4200683017', signedIn: true } };
    const k = T.clean(ev);
    check(k && k.doc.type === 'monogramApproved' && k.key === '4200683017~monogramApproved~L_C2-1760000000000' && k.doc.milestone === false && k.doc.by === 'Ann Operator' && k.doc.data.letters === 'JQX' && k.doc.data.poolId === 'L_2' && k.doc.station === 'sorter' && k.doc.lineKey === 'L' && k.doc.text.length > 0, 'server clean() keeps the type, its small data and its key, as a person\'s action (milestone false): ' + (k && k.key));
    check(T.TYPES.has('monogramApproved') && !T.MILESTONES.has('monogramApproved') && !T.STATION_TYPES.has('monogramApproved') && T.clean(ev, { stationOnly: true }) === null, 'the server knows the type, not as a milestone and not as a station\'s event');
    check(T.clean(Object.assign({}, ev, { type: 'monogramApprovedX' })) === null, 'an unknown type is still dropped');
    // the client
    w.eval(fs.readFileSync(path.join(root, 'order-timeline.js'), 'utf8'));
    const OT = w.OrderTimeline; OT.config({ mode: 'sorter', by: 'Ann Operator' });
    check(OT.TYPES.monogramApproved && !OT.TYPES.monogramApproved.milestone && OT.TYPES.monogramApproved.label === 'Monogram approved', 'the client knows the type (a stamp of the list, not a milestone)');
    const rec = OT.record({ orderId: '4200683017', type: 'monogramApproved', id: 'L-1', at: Date.now(), by: 'Ann', data: { letters: 'ABC' } }); check(rec && rec.type === 'monogramApproved' && rec.by === 'Ann', 'OrderTimeline.record keeps it (it returned null for an unknown type)');
    OT.discard(false);
    // the timeline UI
    w.localStorage.setItem('cn.employee', 'Current Viewer');
    const answer = [{ id: 'arrive', type: 'arrived', orderId: '4200683017', at: AT - 172800000, by: 'Paul', source: 'sorter' }];
    w.OrderTimeline = { get: async () => ({ events: answer.slice(), cancelled: null }), onRecord: () => () => {}, TYPES: OT.TYPES };
    w.eval(fs.readFileSync(path.join(root, 'charm-nest-timeline-ui.js'), 'utf8'));
    const U = w.OrderTimelineUI, me = Object.assign({}, ev, { at: AT, orderId: '4200683017', key: 'monogramApproved~L' });
    check(U.sealed(me) && U.sealsOf([answer[0], me]).includes(me) && !U.sealsOf([answer[0], me]).some(e => e.same), 'the timeline UI draws a stamp for it (a person\'s action: it never folds into another seal)');
    check(U.stepOf(me) === null && U.stepOf({ type: 'engraveApproved' }) === 2, 'it is no rail step: stepOf says none (the Engraved step is 2 and stays the engraving\'s)');
    const fm = U.faceModel(me); check(fm.family === 'monogram' && fm.action === 'MONOGRAM' && fm.icon === 'monogram' && fm.letters === 'JQX' && fm.by === 'Ann Operator' && fm.at === AT && U.faceModel(Object.assign({}, me, { data: {} })).letters === undefined, 'faceModel: the monogram family, MONOGRAM, the letters the event says: ' + JSON.stringify(fm));
    check(/data-seal-family="monogram"/.test(U.stampSvg(me, true)) && /Ann Operator/.test(U.stampSvg(me, true, { hover: true })), 'stampSvg draws the shared monogram face; its hover reads the saved signer');
    answer.push(me); w.Seal.press = async () => {};
    const tl = U.mount(doc.getElementById('timeline'), { orderId: '4200683017', live: false }); await tick(80);
    const marks = [...doc.querySelectorAll('#timeline svg[data-seal-family="monogram"]')];
    check(marks.length >= 1, 'a mounted order timeline shows the monogram stamp (' + marks.length + ')');
    const engraved = doc.querySelector('#timeline [data-step="engraved"], #timeline .tlStop[data-k="engraved"]'); check(!engraved || !engraved.querySelector('svg[data-seal-family="monogram"]'), 'and it is not drawn as the Engraved step');
    try { tl && tl.destroy && tl.destroy(); } catch (_) { /* gone */ }
  }

  console.log(`\nmonogram-seal-service: ${nCheck - fails.length}/${nCheck} checks passed`);
  w.close();
  if (fails.length) { console.log('FAILED:\n - ' + fails.join('\n - ')); process.exitCode = 1; }
})().catch(e => { console.error(e); process.exitCode = 1; });
