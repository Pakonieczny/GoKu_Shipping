#!/usr/bin/env node
// Monogram FAST BENCH (package E2 of the MX tournament; committed as scripts/monogram-bench.cjs). OFFLINE, node only: no browser, no PNG, no network.
// It runs the SAME judge as the review sheet (the frozen make-sheet copy: its checkWord + applyBands are loaded from that file, never re-implemented, so pass/fail is the sheet's),
// builds every word with Engine.autoFit (or Engine.build), in up to 2 worker processes with a shared work queue, and writes metrics only.
//
//   run     node fastbench.cjs [--engine=<charm-nest-monogram.js>] [--bench=A|B|<words.json>] [--label=<id>] [--shards=2] [--no-autofit] [--out=<dir>]
//           (defaults: engine ./charm-nest-monogram.js of the cwd, bench A = /tmp/mx/bench20.json, label from the worktree name wt-<ID>, 2 shards, autofit on)
//           --words=ESG,HEY,...      ad-hoc words instead of a bench (not written to the leaderboard unless --log)
//           --judge=<file>           default: /tmp/mx/make-sheet-frozen.cjs if it exists, else /tmp/mx/make-sheet-now.cjs
//           --compare-sheet=<summary.json | sheet out dir>   print every word whose status differs from a real sheet run of the same engine
//           --no-log                 do not add the run to /tmp/mx/leaderboard.json
//   diff    node fastbench.cjs diff <runA> <runB>     per-word changes (status, reasons, side body offset, ESG distance, ms) between two runs
//   board   node fastbench.cjs board [A|B]            the leaderboard, best first
//   <run> = a run file (/tmp/mx/runs/*.json), a label (latest run of that label on bench A; label:B for bench B), or "base" (= label E2-base ... the first run of that label)
// Score order (brief): pass count (PASS only), fewer FAIL reasons, lower mean |side body offset|, lower median ESG distance, lower CPU ms per word (wall ms is also recorded; it is inflated when the machine is busy).
'use strict';
const fs = require('fs'), path = require('path'), os = require('os'), cp = require('child_process'), crypto = require('crypto'), Module = require('module');

const MX = process.env.MX_DIR || '/tmp/mx';
const BENCH = { A: path.join(MX, 'bench20.json'), B: '/tmp/mono-sheet-50/words50b.json' };
const LB = path.join(MX, 'leaderboard.json'), RUNS = path.join(MX, 'runs'), CACHE = path.join(MX, 'cache');
const arg = (n, d) => { const a = process.argv.find(x => x.startsWith('--' + n + '=')); return a ? a.slice(n.length + 3) : d; };
const flag = n => process.argv.includes('--' + n);
const r2 = v => Math.round(v * 100) / 100;
const median = a => { const b = a.filter(Number.isFinite).sort((x, y) => x - y), n = b.length; return n ? (n % 2 ? b[(n - 1) / 2] : (b[n / 2 - 1] + b[n / 2]) / 2) : null; };
const mean = a => { const b = a.filter(Number.isFinite); return b.length ? b.reduce((x, y) => x + y, 0) / b.length : null; };

/* ── the judge: the sheet's own file, compiled with a few more exports (applyBands is not exported by it) ── */
function loadJudge(judgePath, engine, outDir, repo) {
  let src = fs.readFileSync(judgePath, 'utf8').replace(/^#!.*\n/, '');
  src += '\nmodule.exports.__x = { applyBands, CAL, LIMIT_TEXT, ESG_PROFILE, VERT, BAR, MIN_CONTACTS };\n';
  const argv = process.argv; process.argv = [argv[0], judgePath, engine, outDir].concat(repo ? [repo] : [], flag('no-autofit') ? [] : ['--autofit']);
  try { const m = new Module(judgePath, null); m.filename = judgePath; m.paths = Module._nodeModulePaths(path.dirname(judgePath)); m._compile(src, judgePath); return m.exports; }
  finally { process.argv = argv; }
}
function defaultJudge() { const f = path.join(MX, 'make-sheet-frozen.cjs'); return fs.existsSync(f) ? f : path.join(MX, 'make-sheet-now.cjs'); }
async function lettersCached(J, repo) {
  const fx = path.join(repo, 'tests/charm-nest/fixtures/SKUs_MONOGRAM_2024-1015.ai'), fallback = '/home/user/GoKu_Shipping/tests/charm-nest/fixtures/SKUs_MONOGRAM_2024-1015.ai';
  const f = fs.existsSync(fx) ? fx : fallback, st = fs.statSync(f), key = crypto.createHash('sha1').update(st.size + ':' + st.mtimeMs + ':' + f.length).digest('hex').slice(0, 10);
  const cf = path.join(CACHE, `letters-${st.size}-${key}.json`);
  try { return JSON.parse(fs.readFileSync(cf, 'utf8')); } catch (_) { /* no cache yet */ }
  const letters = await J.loadLetters();
  try { fs.mkdirSync(CACHE, { recursive: true }); const tmp = cf + '.' + process.pid; fs.writeFileSync(tmp, JSON.stringify(letters)); fs.renameSync(tmp, cf); } catch (_) { /* cache is optional */ }
  return letters;
}

/* ── worker: builds the words the parent sends ── */
async function workerMain() {
  const cfg = JSON.parse(process.env.FB_CFG);
  try { os.setPriority(0, cfg.nice); } catch (_) { /* optional */ }
  const J = loadJudge(cfg.judge, cfg.engine, cfg.scratch, cfg.repo), letters = await lettersCached(J, cfg.repoGuess);
  const rc = new Map(), regionCache = s => { if (!rc.has(s.sku)) rc.set(s.sku, J.regionOfLetter(s)); return rc.get(s.sku); };
  process.on('message', async m => {
    if (m.stop) { process.exit(0); }
    try { const c0 = process.cpuUsage(), { rec } = await J.checkWord(m.word, letters, regionCache), c1 = process.cpuUsage(c0); rec.cpuMs = Math.round((c1.user + c1.system) / 1000); delete rec.contactPaths; delete rec.tintPaths; if (rec.patches) rec.patches = rec.patches.map(q => { const y = Object.assign({}, q); delete y.path; return y; }); process.send({ word: m.word, rec }); }
    catch (e) { process.send({ word: m.word, rec: { word: m.word, ok: false, ms: 0, status: 'FAIL', reasons: ['crash: ' + String(e && e.message || e).slice(0, 120)], warnings: [], notes: [], why: 'crash' } }); }
  });
  process.send({ ready: true });
}

/* ── helpers on records ── */
const slimWord = r => ({ word: r.word, status: r.status, ok: r.ok, reasons: r.reasons || [], warnings: (r.warnings || []).map(w => String(w).slice(0, 140)), kinds: [...new Set((r.limits || []).map(l => l.key))], ms: r.ms, cpuMs: r.cpuMs == null ? null : r.cpuMs, severity: r.severity == null ? null : r.severity, offL: r.bodyOff ? r.bodyOff.L : null, offR: r.bodyOff ? r.bodyOff.R : null, esgDist: r.esgDist == null ? null : r.esgDist, wMm: r.wMm, hMm: r.hMm, counts: r.counts || null, ratios: r.ratios || null, longestBarMm: r.longestBarMm == null ? null : r.longestBarMm, adjust: r.adjust || null, balance: r.balance ? { LM: r.balance.LM && r.balance.LM.ok, MR: r.balance.MR && r.balance.MR.ok } : null });
function aggregate(words, esg, hey) {
  const ok = words.filter(w => w.ok), count = s => words.filter(w => w.status === s).length;
  const offs = ok.filter(w => w.offL != null).map(w => (Math.abs(w.offL) + Math.abs(w.offR)) / 2), reasons = {};
  for (const w of words) for (const k of w.kinds) reasons[k] = (reasons[k] || 0) + 1;
  return { total: words.length, pass: count('PASS'), warn: count('WARN'), fail: count('FAIL'), failReasons: words.reduce((a, w) => a + (w.status === 'FAIL' ? w.reasons.length : 0), 0), meanAbsSideOffsetMm: offs.length ? r2(mean(offs)) : null, medianEsgDist: r2(median(words.map(w => w.esgDist))), msPerWord: Math.round(mean(words.map(w => w.ms))), cpuMsPerWord: Math.round(mean(words.map(w => w.cpuMs))), esgPass: esg ? esg.status !== 'FAIL' : null, heyPass: hey ? hey.status !== 'FAIL' : null, reasonKinds: reasons };
}
const scoreCmp = (a, b) => (b.pass - a.pass) || (a.failReasons - b.failReasons) || ((a.meanAbsSideOffsetMm ?? 9) - (b.meanAbsSideOffsetMm ?? 9)) || ((a.medianEsgDist ?? 9) - (b.medianEsgDist ?? 9)) || ((a.cpuMsPerWord ?? a.msPerWord) - (b.cpuMsPerWord ?? b.msPerWord));

/* ── leaderboard (file lock + atomic rename, many agents write it) ── */
function withLock(fn) {
  const lock = LB + '.lock'; fs.mkdirSync(MX, { recursive: true });
  for (let i = 0; i < 200; i++) {
    try { fs.mkdirSync(lock); break; } catch (e) { try { if (Date.now() - fs.statSync(lock).mtimeMs > 20000) fs.rmdirSync(lock); } catch (_) { /* gone */ } Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 100); }
  }
  try { return fn(); } finally { try { fs.rmdirSync(lock); } catch (_) { /* gone */ } }
}
function readLB() { try { return JSON.parse(fs.readFileSync(LB, 'utf8')); } catch (_) { return { runs: [] }; } }
function ranking(runs, bench) {
  const latest = new Map(); for (const r of runs.filter(r => r.bench === bench)) latest.set(r.label, r);   // the latest run of each label
  return [...latest.values()].sort((a, b) => scoreCmp(a.summary, b.summary)).map((r, i) => ({ rank: i + 1, label: r.label, commit: r.engine.commit, sha: r.engine.sha, ...pick(r.summary, ['pass', 'warn', 'fail', 'failReasons', 'meanAbsSideOffsetMm', 'medianEsgDist', 'msPerWord', 'cpuMsPerWord', 'esgPass', 'heyPass']), time: r.time, runFile: r.runFile }));
}
const pick = (o, ks) => Object.fromEntries(ks.map(k => [k, o[k]]));
function logRun(run) {
  withLock(() => {
    const lb = readLB(); lb.runs = (lb.runs || []).concat([{ ...run, words: run.words.map(w => ({ word: w.word, engine: run.engine.commit, pass: w.status === 'PASS', status: w.status, reasons: w.reasons, ms: w.ms, cpuMs: w.cpuMs, offL: w.offL, offR: w.offR, esgDist: w.esgDist })) }]).slice(-300);
    lb.updated = new Date().toISOString(); lb.scoreOrder = 'pass count desc, fewer FAIL reasons, lower mean |side body offset|, lower median ESG distance, lower CPU ms per word';
    lb.ranking = { A: ranking(lb.runs, 'A'), B: ranking(lb.runs, 'B') };
    const tmp = LB + '.' + process.pid; fs.writeFileSync(tmp, JSON.stringify(lb, null, 1)); fs.renameSync(tmp, LB);
  });
}
function engineInfo(engine) {
  const dir = path.dirname(engine), g = a => { try { return cp.execFileSync('git', ['-C', dir, ...a], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim(); } catch (_) { return ''; } };
  const sha = crypto.createHash('sha1').update(fs.readFileSync(engine)).digest('hex').slice(0, 8), file = g(['log', '-1', '--format=%h', '--', engine]), head = g(['rev-parse', '--short', 'HEAD']), dirty = g(['status', '--porcelain', '--', engine]) !== '';
  return { file: engine, commit: (file || head || '?') + (dirty ? '+edits' : ''), head, sha, dirty };
}
function resolveRun(spec) {
  if (fs.existsSync(spec)) return JSON.parse(fs.readFileSync(spec, 'utf8'));
  const [label, bench] = spec.split(':'), runs = readLB().runs.filter(r => r.label === label && r.bench === (bench || 'A'));
  if (!runs.length) throw new Error('no run for ' + spec);
  const r = runs[runs.length - 1]; try { return JSON.parse(fs.readFileSync(r.runFile, 'utf8')); } catch (_) { return r; }
}

/* ── run ── */
async function runMain() {
  const t0 = Date.now(), cwd = process.cwd(), engine = path.resolve(arg('engine', path.join(cwd, 'charm-nest-monogram.js')));
  const judge = path.resolve(arg('judge', defaultJudge())), adhoc = arg('words', null);
  const benchName = adhoc ? 'adhoc' : arg('bench', 'A'), benchFile = BENCH[benchName] || path.resolve(benchName);
  const words = adhoc ? adhoc.split(',').map(w => w.trim().toUpperCase()).filter(Boolean) : (j => (Array.isArray(j) ? j : j.words).map(w => String(w).toUpperCase()))(JSON.parse(fs.readFileSync(benchFile, 'utf8')));
  const benchKey = BENCH[benchName] ? benchName : adhoc ? 'adhoc' : path.basename(benchFile);
  const wt = /wt-([A-Za-z0-9]+)/.exec(engine), label = arg('label', wt ? wt[1] : path.basename(path.dirname(engine)));
  const guess = path.dirname(engine), repoGuess = fs.existsSync(path.join(guess, 'netlify/functions/_charmNestPdf.js')) ? guess : '/home/user/GoKu_Shipping';
  const shards = Math.max(1, Math.min(2, +arg('shards', 2))), scratch = path.resolve(arg('out', path.join(MX, 'runs', 'scratch-' + label)));   // (the judge wants an output dir; it only creates it)
  fs.mkdirSync(scratch, { recursive: true });
  const info = engineInfo(engine), autofit = !flag('no-autofit');
  // work list: the ESG model first, HEY (second approved word) as calibration when it is not in the list, then the words
  const queue = ['ESG'].concat(words.includes('HEY') ? [] : ['HEY']).concat(words), got = new Map(), cfg = { judge, engine, scratch, repo: repoGuess === guess ? undefined : repoGuess, repoGuess, nice: +arg('nice', 5) };
  console.log(`fastbench ${label}: engine ${info.commit} (${info.sha}) judge ${path.basename(judge)} bench ${benchKey} (${words.length} words) ${autofit ? 'autoFit' : 'build'} ${shards} shard(s)`);
  await new Promise((resolve, reject) => {
    let next = 0, live = 0, done = 0;
    const kids = [];
    for (let i = 0; i < shards; i++) {
      const k = cp.fork(__filename, ['--worker'].concat(process.argv.slice(2).filter(a => a === '--no-autofit')), { env: Object.assign({}, process.env, { FB_CFG: JSON.stringify(cfg) }), silent: false }); kids.push(k); live++;
      const feed = () => { if (next < queue.length) k.send({ word: queue[next++] }); else k.send({ stop: true }); };
      k.on('message', m => { if (m.ready) return feed(); got.set(m.word, m.rec); done++; process.stdout.write(`${m.word}${m.rec.ok ? '' : '!'} `); feed(); });
      k.on('exit', code => { live--; if (live === 0) { if (done < queue.length) reject(new Error(`a worker exited (code ${code}) before all ${queue.length} words were built`)); else resolve(); } });
      k.on('error', reject);
    }
  });
  console.log('\nbuilt (status comes after the bands are applied) in', Date.now() - t0, 'ms');
  const J = loadJudge(judge, engine, scratch, cfg.repo), model = got.get('ESG'), heyRec = words.includes('HEY') ? null : got.get('HEY');
  model.isModel = true; model.label = 'ESG model'; if (heyRec) heyRec.isCalib = true;
  const recs = words.map(w => got.get(w));
  J.__x.applyBands(recs, [model].concat(heyRec ? [heyRec] : []));
  const slim = recs.map(slimWord), esg = slimWord(model), hey = slimWord(heyRec || recs.find(r => r.word === 'HEY'));
  const wall = Date.now() - t0, sum = aggregate(slim, esg, hey); sum.wallSec = Math.round(wall / 100) / 10;
  const stamp = new Date().toISOString().replace(/[-:]/g, '').slice(0, 15), runFile = path.join(RUNS, `${label}-${benchKey}-${stamp}.json`);
  const run = { label, bench: benchKey, benchFile, time: new Date().toISOString(), engine: info, judge: path.basename(judge), autofit, shards, runFile, summary: sum, esg, hey, words: slim, limitText: J.__x.LIMIT_TEXT };
  fs.mkdirSync(RUNS, { recursive: true }); fs.writeFileSync(runFile, JSON.stringify(run, null, 1)); try { fs.copyFileSync(runFile, path.join(scratch, 'fastbench.json')); } catch (_) { /* scratch optional */ }
  if (!flag('no-log') && (benchKey !== 'adhoc' || flag('log'))) { logRun(run); console.log('leaderboard updated:', LB); }
  console.log(`RESULT ${label} ${benchKey}: PASS ${sum.pass} WARN ${sum.warn} FAIL ${sum.fail} of ${sum.total} | FAIL reasons ${sum.failReasons} | mean|side off| ${sum.meanAbsSideOffsetMm} mm | median ESG dist ${sum.medianEsgDist} | ${sum.msPerWord} ms/word (${sum.cpuMsPerWord} ms cpu) | wall ${sum.wallSec} s | ESG ${esg.status} HEY ${hey.status}`);
  for (const w of slim.filter(w => w.status === 'FAIL')) console.log(`  FAIL ${w.word}: ${w.reasons.join('; ').slice(0, 200)}`);
  console.log('run file:', runFile);
  const cs = arg('compare-sheet', null); if (cs) compareSheet(slim, esg, hey, cs);
}
function compareSheet(slim, esg, hey, cs) {
  const f = fs.existsSync(path.join(cs, 'summary.json')) ? path.join(cs, 'summary.json') : cs, S = JSON.parse(fs.readFileSync(f, 'utf8')), diffs = [];
  for (const w of slim) { const s = S.perWord && S.perWord[w.word]; if (s && s.status !== w.status) diffs.push(`${w.word}: sheet ${s.status} fastbench ${w.status}`); }
  const sp = S.pass, sf = S.fail; console.log(`compare with sheet ${f}: sheet PASS ${S.pass} WARN ${S.warn} FAIL ${S.fail} | status differences: ${diffs.length}${diffs.length ? ' -> ' + diffs.join('; ') : ' (identical)'}`);
}

/* ── diff / board ── */
function diffMain() {
  const [a, b] = process.argv.slice(3).filter(x => !x.startsWith('--')).map(resolveRun), A = new Map(a.words.map(w => [w.word, w])), B = new Map(b.words.map(w => [w.word, w]));
  const sg = v => (v > 0 ? '+' : '') + r2(v), line = [];
  console.log(`A ${a.label} ${a.engine.commit} (${a.engine.sha}) ${a.bench} ${a.time.slice(0, 19)}  ->  B ${b.label} ${b.engine.commit} (${b.engine.sha}) ${b.bench} ${b.time.slice(0, 19)}`);
  const sa = a.summary, sb = b.summary, row = (n, x, y) => `${n.padEnd(22)} ${String(x).padStart(8)} -> ${String(y).padStart(8)}   (${typeof x === 'number' && typeof y === 'number' ? sg(y - x) : ''})`;
  for (const [n, k] of [['PASS', 'pass'], ['WARN', 'warn'], ['FAIL', 'fail'], ['FAIL reasons', 'failReasons'], ['mean |side off| mm', 'meanAbsSideOffsetMm'], ['median ESG dist', 'medianEsgDist'], ['ms/word (wall)', 'msPerWord'], ['ms/word (cpu)', 'cpuMsPerWord']]) line.push(row(n, sa[k], sb[k]));
  console.log(line.join('\n'));
  const fixed = [], broke = [], changed = [];
  for (const [w, x] of A) { const y = B.get(w); if (!y) continue;
    if (x.status !== y.status) (y.status === 'PASS' || (y.status === 'WARN' && x.status === 'FAIL') ? fixed : broke).push(`${w} ${x.status}->${y.status}${y.status === 'FAIL' ? ' [' + y.reasons.join('; ').slice(0, 110) + ']' : ''}`);
    else { const d = [], dl = y.offL != null && x.offL != null ? (Math.abs(y.offL) + Math.abs(y.offR)) / 2 - (Math.abs(x.offL) + Math.abs(x.offR)) / 2 : 0; if (Math.abs(dl) >= 0.1) d.push(`side off ${sg(dl)}`); if (y.esgDist != null && x.esgDist != null && Math.abs(y.esgDist - x.esgDist) >= 0.3) d.push(`ESG dist ${sg(y.esgDist - x.esgDist)}`); if (JSON.stringify(x.reasons) !== JSON.stringify(y.reasons) && y.status === 'FAIL') d.push('reasons changed: ' + y.reasons.join('; ').slice(0, 110)); if (d.length) changed.push(`${w} ${y.status}: ${d.join(', ')}`); } }
  const only = [...A.keys()].filter(w => !B.has(w)).concat([...B.keys()].filter(w => !A.has(w)));
  console.log(`\nFIXED/better (${fixed.length}): ${fixed.join(' | ') || '-'}\nBROKEN/worse (${broke.length}): ${broke.join(' | ') || '-'}\nOther changes (${changed.length}):\n  ${changed.join('\n  ') || '-'}${only.length ? '\nnot in both runs: ' + only.join(' ') : ''}`);
}
function boardMain() {
  const lb = readLB(), want = process.argv[3] && !process.argv[3].startsWith('--') ? process.argv[3] : null;
  for (const b of ['A', 'B']) { if (want && want !== b) continue; const rk = lb.ranking && lb.ranking[b] || ranking(lb.runs || [], b); console.log(`\nBENCHMARK ${b}  (best first: pass, fewer FAIL reasons, mean|side off|, median ESG dist, ms/word)`); console.log('rank label     commit        pass warn fail reasons off(mm) esgMed cpu/word ESG HEY'); for (const r of rk) console.log(`${String(r.rank).padStart(4)} ${r.label.padEnd(9)} ${String(r.commit).padEnd(13)} ${String(r.pass).padStart(4)} ${String(r.warn).padStart(4)} ${String(r.fail).padStart(4)} ${String(r.failReasons).padStart(7)} ${String(r.meanAbsSideOffsetMm).padStart(7)} ${String(r.medianEsgDist).padStart(6)} ${String(r.cpuMsPerWord ?? r.msPerWord).padStart(8)}  ${r.esgPass ? 'ok ' : 'FAIL'} ${r.heyPass ? 'ok' : 'FAIL'}`); }
}

if (require.main === module) {
  const cmd = process.argv[2];
  (flag('worker') ? workerMain() : cmd === 'diff' ? Promise.resolve(diffMain()) : cmd === 'board' ? Promise.resolve(boardMain()) : runMain()).catch(e => { console.error(e); process.exit(1); });
}
module.exports = { loadJudge, aggregate, scoreCmp, ranking, resolveRun };
