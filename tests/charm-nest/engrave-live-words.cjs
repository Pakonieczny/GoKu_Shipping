// LIVEWORDS (Paul, 29 Oct 2026, the Design & Engrave card: "1. When I click the Back/Front buttons the system does not display the Back preview. 2. ... when I click on the Back/Front vector preview
// thumbnails the main engraving preview area does not get toggled between the Back/Front. 3. Remove the Back+Front option, it's not needed. 4. Remove the process that ask the user why the made text
// engraving changes including asking the user to click save ... As soon as the user types a letter into the text box, that letter must immediately appear in all charm vector preview images and main
// Engraving preview in real-time. The user just needs to press the green approve when done typing").
// On the real sorter page over the repo's fake site (_engraveRealWorld.cjs: offline, made-up names), the order "both" (front "W", back "Name One", both faces fitted for real):
//   L1  the face buttons are None | Front | Back (no "Front + back"), Front and Back both pressed (both faces engraved); no Save, no Cancel, no "Why change it?", no "Changed: ..." line
//   L2  the card shows BOTH faces as thumbnails (a picture each), the face shown marked
//   L3  a typed letter is on the face's thumbnail and in the work area's drawing within two frames, nothing pressed
//   L4  a click on the Back thumbnail shows the back in the work area (the card of the back's job, or Engrave.showFace's own turn) and marks the Back thumbnail
//   L5  a press on the Front face button shows the front again
//   L6  Approve with the words still staged: the new words are kept (no question asked), fitted, and approved: the front job approved with the new words, no reason recorded
//   L7  nothing erred
//   PW_DIR=<playwright node_modules> node tests/charm-nest/engrave-live-words.cjs      (SHOTS=<dir> for screenshots)
'use strict';
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : evidence || ''}`.trimEnd()); };
const until = async (f, ms) => { const t0 = Date.now(); for (;;) { const v = await f(); if (v || Date.now() - t0 > ms) return v; await new Promise(r => setTimeout(r, 100)); } };

(async () => {
  const R = require('./_engraveRealWorld.cjs');
  const w = await R.world({ shots: process.env.SHOTS || '/tmp/engrave-live-words-shots' });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  const page = w.page, errors = [];
  page.on('pageerror', e => errors.push(e.message));
  await page.waitForFunction(() => !window.CNRole || ['ask', 'role', 'admin'].includes(CNRole.state()), null, { timeout: 20000 }).catch(() => {}).then(() => page.evaluate(() => { try { if (window.CNRole && CNRole.state() === 'ask') CNRole.choose('design'); } catch (_) { /* none */ } }));
  const Q = '#egQueue .rvItem[data-kind=placement]';
  try {
    const made = await R.build(w, ['both']);
    console.log(`   built: ${made.both.key} ${JSON.stringify(made.both.jobs.map(j => [j.face, j.state, j.text, j.fitted]))}`);
    const keys = await page.evaluate(key => { const row = B.orders.byKey.get(key), js = Engrave.jobsOf(row); return { front: (js.find(j => j.face === 'front') || {}).key, back: (js.find(j => j.face !== 'front') || {}).key }; }, made.both.key);
    await R.openEngraveDetail(w, 'both', { face: 'front' }); await R.settle(page, 700);
    const shownKey = () => page.evaluate(q => (document.querySelector(q) || {}).dataset?.key || '', Q);

    // L1 · the controls
    const ui = await page.evaluate(q => {
      const c = document.querySelector(q), p = c && c.querySelector('.pvSetup .engp');
      const segs = p ? [...p.querySelectorAll('.engpMode:not(.engpFaceSw) .engpSeg')].map(b => [b.textContent.trim(), b.getAttribute('aria-pressed')]) : [];
      const acts = p ? [...p.querySelectorAll('button')].map(b => b.textContent.trim()).filter(t => /^(Save|Cancel)$/.test(t)) : [];
      return { segs, acts, why: !!(c && c.querySelector('.engpWhy,.engpWhyLine')), changed: /Changed:/.test(c ? c.textContent : '') };
    }, Q);
    check('L1 None | Front | Back toggles, both faces pressed', ui.segs.map(s => s[0]).join('|') === 'None|Front|Back' && ui.segs[1][1] === 'true' && ui.segs[2][1] === 'true' && ui.segs[0][1] === 'false', JSON.stringify(ui.segs));
    check('L1 no Save, no Cancel, no reason prompt, no "Changed:" line', !ui.acts.length && !ui.why && !ui.changed, JSON.stringify(ui));

    // L2 · both thumbnails
    const th = await page.evaluate(q => [...document.querySelectorAll(q + ' .pvSetup .engFace')].map(f => ({ face: f.dataset.face, pic: !!f.querySelector('.engThumb') && !!f.querySelector('.engThumb').firstElementChild, cur: f.getAttribute('aria-current') === 'true' })), Q);
    check('L2 the card shows a Front and a Back thumbnail, the Front marked', th.length === 2 && th.every(t => t.pic) && th.find(t => t.face === 'front')?.cur && !th.find(t => t.face === 'back')?.cur, JSON.stringify(th));

    // L3 · a typed letter, at once
    await page.click(Q + ' .pvSetup .engpWords'); await page.keyboard.press('End');
    const live = await page.evaluate(([q, fk]) => new Promise(res => {
      const ta = document.querySelector(q + ' .pvSetup .engpWords'); ta.value = ta.value + 'X'; ta.dispatchEvent(new Event('input', { bubbles: true }));
      requestAnimationFrame(() => requestAnimationFrame(() => {
        const t = document.querySelector(q + ' .pvSetup .engFace[data-face="front"] .engpvText'), d = Engrave.draftOf(fk);
        res({ value: ta.value, thumb: t ? t.textContent : null, draft: d ? d.lines : null });
      }));
    }), [Q, keys.front]);
    check('L3 the letter is on the Front thumbnail within two frames', /X/i.test(live.thumb || ''), JSON.stringify(live));
    check('L3 and in the work area\'s drawing (its draft) within two frames, nothing saved', Array.isArray(live.draft) && /X/i.test(live.draft.join(' ')), JSON.stringify(live));

    // L4 · the Back thumbnail shows the back
    await page.click(Q + ' .pvSetup .engFace[data-face="back"] .engThumb');
    const toBack = await until(async () => { const k = await shownKey(); const cur = await page.evaluate(q => document.querySelector(q + ' .pvSetup .engFace[aria-current=true]')?.dataset.face || '', Q); return (k === keys.back || await page.evaluate(() => { try { return (Engrave.viewFaceOf && document.querySelector('#egQueue .rvItem[data-kind=placement] canvas')?._viewFace) || ''; } catch (_) { return ''; } }) === 'back') && cur === 'back' ? { k, cur } : null; }, 8000);
    check('L4 a click on the Back thumbnail shows the back in the work area and marks the Back thumbnail', !!toBack, JSON.stringify({ shown: await shownKey(), want: keys.back }));
    const kept = await page.evaluate(q => [...document.querySelectorAll(q + ' .pvSetup .engFace[data-face="front"] .engpvText')].map(t => t.textContent).join(''), Q);
    check('L4 the staged front words stay on the Front thumbnail while the back is shown', /X/i.test(kept), kept);

    // L5 · the Front face button shows the front
    await page.click(Q + ' .pvSetup .engpSeg[data-mode="front"]');
    const toFront = await until(async () => (await shownKey()) === keys.front, 8000);
    check('L5 a press on the Front face button shows the front in the work area', !!toFront, await shownKey());
    const still = await page.evaluate(q => document.querySelector(q + ' .pvSetup .engpWords')?.value || '', Q);
    check('L5 the typed words are still in the box', /X$/.test(still), still);

    // L6 · Approve keeps the words and approves them
    await page.click(Q + ' [data-a="approve"]');
    const done = await until(() => page.evaluate(([key, fk]) => { const row = B.orders.byKey.get(key), j = Engrave.jobsOf(row).find(x => x.key === fk); return j && ['approved', 'written'].includes(j.state) ? { state: j.state, text: j.text, ov: row.engravingOverride || null } : null; }, [made.both.key, keys.front]), 90000);
    const why = await page.evaluate(() => [...document.querySelectorAll('.egApproveWhy')].map(n => n.textContent).join(' | '));
    check('L6 Approve with words staged: the front is approved with the NEW words', !!done && /X/i.test(done.text || ''), JSON.stringify(done) + ' ' + why);
    check('L6 the words were kept with no question: in the override, no reason recorded', !!done && done.ov && done.ov.front && /X/i.test(done.ov.front.text || '') && !done.ov.reason, JSON.stringify(done && done.ov));
    if (w.shots) await R.shot(w, 'live-words-approved').catch(() => {});
    // L8 · STAGEALL (Paul, 29 Oct 2026: "it's not approved until the Approve button is pressed again ... otherwise those updates to the engraving are discarded"): words typed and the card left with no Approve are
    //      discarded (never kept quietly): the job and the override are as approved, and the box shows the approved words when the card is opened again
    const kept8 = await page.evaluate(([key, fk]) => { const row = B.orders.byKey.get(key), j = Engrave.jobsOf(row).find(x => x.key === fk); return { text: j.text, state: j.state, ov: JSON.stringify(row.engravingOverride || null) }; }, [made.both.key, keys.front]);
    await page.evaluate(fk => { Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: fk }); Engrave.render(); }, keys.front); await R.settle(page, 700);
    await page.click(Q + ' .pvSetup .engpWords'); await page.keyboard.press('End'); await page.keyboard.type('ZZ'); await R.settle(page, 300);
    await page.evaluate(() => { Engrave.restoreView({ tab: 'place', list: true, chosen: true, focus: null }); Engrave.render(); }); await R.settle(page, 900);
    await page.evaluate(fk => { Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: fk }); Engrave.render(); }, keys.front); await R.settle(page, 700);
    const left = await page.evaluate(([q, key, fk]) => { const row = B.orders.byKey.get(key), j = Engrave.jobsOf(row).find(x => x.key === fk); return { text: j.text, state: j.state, ov: JSON.stringify(row.engravingOverride || null), box: (document.querySelector(q + ' .pvSetup .engpWords') || {}).value || '' }; }, [Q, made.both.key, keys.front]);
    check('L8 words typed and the card left with no Approve are discarded: the job, the override and the box are as approved', left.text === kept8.text && left.state === kept8.state && left.ov === kept8.ov && !/ZZ/.test(left.box), JSON.stringify({ kept8, left }));
    check('L7 no page errors', errors.length === 0, errors.join(' | ').slice(0, 400));
  } catch (e) { console.error(e); check('ran', false, String(e && e.stack || e)); }
  finally { await w.close(); }
  const bad = RESULTS.filter(r => !r.ok);
  console.log(bad.length ? `engrave-live-words: ${bad.length} failed` : 'engrave-live-words: ok');
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
