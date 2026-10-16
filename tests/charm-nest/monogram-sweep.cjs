// MONOGRAM sweep (MU4, 16 Oct 2026): Paul's REAL 78 letters (fixtures/SKUs_MONOGRAM_2024-1015.ai) through the engine, in the words that matter, with the edited letters ON (the default). Engine + file only, no browser, no network.
//   · (MONO_SWEEP_SPREAD=1 adds the 26 spread words that use every one of the 78 letters: monogram-pipeline.cjs always builds them),
//   · Paul's 50-word list (fixtures/monogram-words50.json) and the 20 worst words (fixtures/monogram-worst20.json),
//   for EACH word: no exception, a finished piece, no NaN / Infinity, ONE connected piece (one outer ring), every counter inside it, 14.8 mm tall (+- 0.5 mm), report.metrics present; a word that still breaks a rule is given to
//   Engine.autoFit exactly as the card does, and the result must be sound and not worse; the piece is then fitted to 16 / 18 / 20 mm wide (opts.fit) and must stay sound at the scaled height; and written as the design file
//   (Monogram XYZ.ai) which must read back through the cut-file preflight as ONE piece of the same size.
//   node tests/charm-nest/monogram-sweep.cjs        MONO_SWEEP_QUICK=1: every third word only        MONO_SWEEP_AUTOFIT_MS=4000: the time autoFit may take per word
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
global.ClipperLib = require(path.join(root, 'vendor/clipper-6.4.2.js'));
const M = require(path.join(root, 'charm-nest-monogram.js')), F = require(path.join(root, 'charm-nest-monogram-fit.js'));
const R = require('./_monogramReal.cjs');
const T0 = Date.now(), fails = []; let nCheck = 0;
const check = (ok, msg) => { nCheck++; if (!ok) { fails.push(msg); console.log('  ✗ ' + msg); } };
const A = [...'ABCDEFGHIJKLMNOPQRSTUVWXYZ'];
const QUICK = process.env.MONO_SWEEP_QUICK === '1', AF_MS = +process.env.MONO_SWEEP_AUTOFIT_MS || 4000;
const thin = l => (QUICK ? l.filter((_, i) => i % 3 === 0) : l);
const SPREAD = A.map((_, i) => A[i] + A[(i * 7 + 3) % 26] + A[(i * 11 + 5) % 26]);        // (each slot runs through all 26 letters: 78 letter files)
const W50 = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/monogram-words50.json'), 'utf8')), W20 = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/monogram-worst20.json'), 'utf8'));
const nv = r => (r && r.ok && r.report && r.report.metrics && r.report.metrics.violations ? r.report.metrics.violations.length : 99);
const dist = r => (r && r.report && r.report.metrics && Number.isFinite(+r.report.metrics.distance) ? +r.report.metrics.distance : 0);
const clone = r => JSON.parse(JSON.stringify(r));

(async () => {
  const L = await R.load();
  check(L.skus.length === 78, 'Paul\'s file holds 78 labelled letters: ' + L.skus.length);
  check(SPREAD.length === 26 && ['left', 'middle', 'right'].every(s => new Set(SPREAD.map(w => w[['left', 'middle', 'right'].indexOf(s)])).size === 26), 'the spread uses every letter in every slot (78 letter files)');
  check(W50.length === 50 && W20.length === 20 && [...W50, ...W20].every(w => /^[A-Z]{3}$/.test(w)), 'Paul\'s 50-word list and the 20 worst are 3 capital letters each');
  const stat = { words: 0, broken: 0, fitted: 0, files: 0, hMin: 99, hMax: 0, wMin: 99, wMax: 0, ms: 0, slowest: ['', 0], autoFit: 0, autoWorse: 0 };
  const seen = new Set();
  for (const [set, list] of [['spread', process.env.MONO_SWEEP_SPREAD === '1' ? thin(SPREAD) : []], ['50', thin(W50)], ['worst20', thin(W20)]]) {
    for (const w of list) {
      const id = `${set}:${w}`, t0 = Date.now(); let r = null;
      try { r = M.build(await L.word(w)); } catch (e) { check(false, `${id}: the engine threw: ${e && e.message}`); continue; }
      const ms = Date.now() - t0; stat.ms += ms; if (ms > stat.slowest[1]) stat.slowest = [w, ms];
      let bad = R.sound(r); check(bad.length === 0, `${id}: ${bad.join('; ')}`);
      if (bad.length) continue;
      stat.words++; stat.hMin = Math.min(stat.hMin, r.hMm); stat.hMax = Math.max(stat.hMax, r.hMm); stat.wMin = Math.min(stat.wMin, r.wMm); stat.wMax = Math.max(stat.wMax, r.wMm);
      check(r.report.metrics.violations.every(v => v && (v.text || v.rule || typeof v === 'string')), `${id}: every broken rule is named in words`);
      // a word that breaks a rule: the card runs autoFit (deterministic, bounded); the result must be sound and not worse
      if (nv(r)) {
        stat.broken++;
        try { const f = M.autoFit(await L.word(w), { autoFitMs: AF_MS }); stat.autoFit++;
          bad = R.sound(f); check(bad.length === 0, `${id} autoFit: ${bad.join('; ')}`);
          if (!bad.length) { const worse = nv(f) > nv(r) || (nv(f) === nv(r) && dist(f) > dist(r) + 1e-6); if (worse) stat.autoWorse++; check(f.report.autoFit && f.report.autoFit.tried >= 1, `${id} autoFit: report.autoFit is there`); if (!worse) r = f; }
        } catch (e) { check(false, `${id}: autoFit threw: ${e && e.message}`); }
      }
      // the piece size: 16 / 18 / 20 mm wide (a copy: applyFit scales in place)
      for (const mm of F.ENVELOPES) {
        const c = clone(r); F.applyFit(c, { maxWidthMm: mm }); stat.fitted++;
        const sc = c.report.fit && c.report.fit.scale || 1; bad = R.sound(c, { scale: sc });
        check(bad.length === 0 && c.wMm <= mm + 1e-3 && Math.abs(c.hMm - r.hMm * sc) < 2e-3, `${id} at ${mm} mm: ${bad.join('; ') || (c.wMm.toFixed(2) + ' wide, scale ' + sc)}`);
      }
      // the design file
      if (!seen.has(w)) {
        seen.add(w);
        try {
          const bytes = await M.toPdf(r, { title: 'Monogram ' + w }), pf = await R.preflight(bytes, `Monogram ${w}.ai`); stat.files++;
          check(Buffer.from(bytes.slice(0, 5)).toString() === '%PDF-' && pf.pieces === 1 && Math.abs(pf.wMm - r.wMm) < 0.05 && Math.abs(pf.hMm - r.hMm) < 0.05, `${id}: Monogram ${w}.ai reads back as ONE piece of ${r.wMm.toFixed(2)} x ${r.hMm.toFixed(2)} mm (${pf.pieces} piece, ${pf.wMm.toFixed(2)} x ${pf.hMm.toFixed(2)})`);
        } catch (e) { check(false, `${id}: the design file does not read back: ${e && e.message}`); }
      }
    }
  }
  check(stat.words > 0 && stat.autoWorse === 0, `autoFit never leaves a word worse than the plain build (${stat.autoWorse} of ${stat.autoFit})`);
  const line = `${stat.words} words (Paul's 50, the 20 worst${process.env.MONO_SWEEP_SPREAD === '1' ? ', the 26 spread words' : ''}): ${stat.broken} still break a rule after the plain build (autoFit run on ${stat.autoFit}), ${stat.fitted} fits to 16/18/20 mm, ${stat.files} design files read back; ${stat.hMin.toFixed(2)}-${stat.hMax.toFixed(2)} mm tall, ${stat.wMin.toFixed(1)}-${stat.wMax.toFixed(1)} mm wide, build ${Math.round(stat.ms / Math.max(1, stat.words))} ms on average, slowest ${stat.slowest[0]} ${stat.slowest[1]} ms`;
  console.log('  ' + line);
  console.log(`   (${Math.round((Date.now() - T0) / 1000)} s)`);
  console.log(fails.length ? `\nmonogram-sweep: ${fails.length} FAILED of ${nCheck}:\n - ${fails.join('\n - ')}` : `\nmonogram-sweep OK: ${nCheck} checks`);
  process.exit(fails.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
