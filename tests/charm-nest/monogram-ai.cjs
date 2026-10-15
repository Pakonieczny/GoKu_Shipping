// MONOAI (Paul, 15 Oct 2026): the AI assembler of a monogram that still breaks a rule. OFFLINE: the model is a fake `anthropic` that returns canned adjustments; NO real API call, no Anthropic key, no live service anywhere.
//   1  the server side (_charmNestMonogramAssist.js): skipped without a key, the kill switch, a bad request (no call, no allowance used), the request itself (cached system prompt, the ESG reference and the candidate as the two pictures,
//      model / effort from the environment, a schema with no unsupported keyword), clamping of absurd numbers (scale 9, dx 50 ...), malformed JSON, a refusal, a failed call, the day's 3 calls per order, the usage logged
//   2  the background function (charmNestMonogramAssist-background.js) through a fake Firestore: the job record goes pending -> done, a wrong mode is refused
//   3  the loop of the page (assistLoop) with a fake engine: a round is kept only if the rules broken drop (or stay equal while the distance to ESG drops), the engine clamps, never more than 3 rounds, the model's failures leave the result
//   4  the real page (charm-nest-1.html, fake loopback site, the page's agent call stubbed): a failing word is assembled, the 2x PNG of the candidate and the payload the server accepts, the card's one line, the cache (a second open makes
//      no call), 3 rounds at most, "Needs a person" and Approve needing a second press, Improve again and the day's limit, a failing call keeps the automatic result, the kill switch
//   node tests/charm-nest/monogram-ai.cjs [playwright-core dir]        MONOAI_NO_PAGE=1 leaves out part 4
const assert = require('assert'), fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..'), fnDir = path.join(root, 'netlify/functions');
const T0 = Date.now(), fails = []; let nCheck = 0;
const check = (ok, msg) => { nCheck++; if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
const sleep = ms => new Promise(r => setTimeout(r, ms));
const PNG = 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==';
const IDENT = { scale: 1, sx: 1, dx: 0, dy: 0 }, ADJ0 = { L: IDENT, M: IDENT, R: IDENT };

// ── a fake Firestore (documents, merge, a transaction, arrayUnion) and a fake `anthropic` ──
function fakeDb() {
  const docs = new Map(); let lock = Promise.resolve();
  const merge = (cur, patch) => { const out = Object.assign({}, cur); for (const [k, v] of Object.entries(patch)) out[k] = v && v.__union ? (out[k] || []).concat(v.items) : v; return out; };
  const doc = (coll, id) => ({ coll, id, get: async () => ({ exists: docs.has(coll + '/' + id), id, data: () => docs.get(coll + '/' + id) }), set: async (v, o) => { const k = coll + '/' + id; docs.set(k, o && o.merge ? merge(docs.get(k) || {}, v) : merge({}, v)); } });
  const db = { docs, collection: c => ({ doc: id => doc(c, id) }), runTransaction: fn => { const run = lock.then(async () => { const writes = []; const out = await fn({ get: r => r.get(), set: (r, v, o) => writes.push([r, v, o]) }); for (const [r, v, o] of writes) await r.set(v, o); return out; }); lock = run.catch(() => {}); return run; } };   // (transactions one after the other, as Firestore's retries make them)
  const FV = { arrayUnion: (...items) => ({ __union: true, items }), serverTimestamp: () => Date.now(), delete: () => null };
  return { db, FV };
}
function fakeAnthropic(answer) {
  const calls = [];
  return { calls, callClaudeRaw: async a => { calls.push(a); const r = await answer(a, calls.length); return r; } };
}
const reply = (obj, extra) => ({ stop_reason: 'end_turn', usage: { input_tokens: 1200, output_tokens: 80, cache_read_input_tokens: 900 }, content: [{ type: 'text', text: typeof obj === 'string' ? obj : JSON.stringify(obj) }], ...extra });
const good = { adjust: { L: { scale: 1.05, sx: 1.1, dx: 0.4, dy: -0.3 }, M: IDENT, R: { scale: 1, sx: 1.15, dx: -0.2, dy: -0.3 } }, why: 'Widened the right letter and moved both sides down a little so a lower contact joins them.' };
const body = (o = {}) => Object.assign({ orderKey: '4200683017_42006830171', letters: ['G', 'W', 'X'], round: 1, adjust: ADJ0, metrics: { ok: false, ratios: { sideHeightL: 0.8 }, contacts: [{ pair: 'LM', zone: 'upper', wMm: 0.4 }] }, violations: [{ rule: 'contacts', text: 'the left and middle letters are not held by one contact near the top and one near the bottom' }], image: PNG }, o);

// the real model client (_etsyMailAnthropic.js: node-fetch to api.anthropic.com) is never loaded here: a stand-in module takes its place in the require cache, and any call that reaches it is a bug of this test
const clientPath = path.join(fnDir, '_etsyMailAnthropic.js');
const clientStub = { __stub: true, strayCalls: 0, callClaudeRaw: async () => { clientStub.strayCalls++; throw new Error('no real model call in this test'); } };
require.cache[require.resolve(clientPath)] = { id: clientPath, filename: clientPath, loaded: true, exports: clientStub };

(async () => {
  const saved = Object.assign({}, process.env);
  const freshAssist = env => { Object.assign(process.env, env || {}); delete require.cache[require.resolve(path.join(fnDir, '_charmNestMonogramAssist.js'))]; return require(path.join(fnDir, '_charmNestMonogramAssist.js')); };
  delete process.env.ANTHROPIC_API_KEY; delete process.env.CHARM_NEST_MONOGRAM_AI; delete process.env.CHARM_NEST_MONOGRAM_MODEL; delete process.env.CHARM_NEST_MONOGRAM_EFFORT;
  let A = freshAssist();

  // ═══ 1 · the server side ═══
  let { db, FV } = fakeDb(), fa = fakeAnthropic(async () => reply(good));
  let r = await A.run(body(), { db, FV });
  check(r.skipped && /ANTHROPIC_API_KEY/.test(r.skipped) && !r.error, 'no ANTHROPIC_API_KEY: { skipped }, no error, nothing asked: ' + JSON.stringify(r));
  process.env.CHARM_NEST_MONOGRAM_AI = 'off';
  r = await A.run(body(), { db, FV, anthropic: fa });
  check(r.skipped && /switched off/.test(r.skipped) && fa.calls.length === 0, 'CHARM_NEST_MONOGRAM_AI=off: { skipped }, the model is never called');
  delete process.env.CHARM_NEST_MONOGRAM_AI;

  r = await A.run(body({ image: 'data:image/jpeg;base64,/9j/' }), { db, FV, anthropic: fa });
  check(r.error && fa.calls.length === 0 && ![...db.docs.keys()].some(k => k.startsWith('Charm_Nest_MonoAssist/')), 'a picture that is not a PNG is refused before any call or any use of the day\'s allowance: ' + r.error);
  r = await A.run(body({ letters: ['G', 'W'] }), { db, FV, anthropic: fa });
  check(r.error && fa.calls.length === 0, 'two letters: refused: ' + r.error);
  r = await A.run(body({ orderKey: '' }), { db, FV, anthropic: fa });
  check(r.error && fa.calls.length === 0, 'no order: refused: ' + r.error);

  r = await A.run(body(), { db, FV, anthropic: fa, now: Date.UTC(2026, 9, 15, 12) });
  const q = fa.calls[0];
  check(r.adjust && r.adjust.L.dx === 0.4 && r.adjust.R.sx === 1.15 && /Widened the right letter/.test(r.why) && !r.error && r.usage.input_tokens === 1200 && r.usage.cache_read_input_tokens === 900, 'one round: the numbers and the one sentence come back with the usage: ' + JSON.stringify(r.adjust.L));
  check(Array.isArray(q.system) && q.system[0].cache_control && q.system[0].cache_control.type === 'ephemeral' && /ESG/.test(q.system[0].text) && /TWO solid contacts/.test(q.system[0].text) && /never to the point|NEVER be altered/.test(q.system[0].text) && /0\.85 to 1\.15/.test(q.system[0].text) && /-1\.5 to 1\.5/.test(q.system[0].text), 'the system prompt is cached (cache_control) and states the rules, the adjust fields, the bounds and the never-ruin-a-letter rule');
  const imgs = q.messages[0].content.filter(b => b.type === 'image'), refMod = require(path.join(fnDir, '_charmNestMonogramRef.js'));
  check(imgs.length === 2 && imgs.every(b => b.source.media_type === 'image/png') && 'data:image/png;base64,' + imgs[0].source.data === refMod.ESG_PNG_DATA_URL && 'data:image/png;base64,' + imgs[1].source.data === PNG, 'the request carries two PNGs: the ESG reference first, the candidate second');
  const txt = q.messages[0].content.filter(b => b.type === 'text').map(b => b.text).join('\n');
  check(/Left G, Middle W, Right X/.test(txt) && /scale 1, sx 1, dx 0 mm, dy 0 mm/.test(txt) && /not held by one contact near the top/.test(txt) && /"ratios"/.test(txt) && /Round 1 of 3/.test(txt), 'it names the three letters, the current adjust, the broken rule in words and the engine\'s measurements');
  check(q.model === 'claude-sonnet-5' && q.effort === 'medium' && q.outputFormat && q.outputFormat.type === 'json_schema', `model and effort default to ${q.model} / ${q.effort}, structured output asked`);
  check(!/"(?:maxItems|maximum|minimum|maxLength|minLength)":/.test(JSON.stringify(q.outputFormat.schema)) && q.outputFormat.schema.required.join() === 'adjust,why', 'the schema uses only supported keywords and asks { adjust, why }');
  check(Buffer.from(refMod.ESG_PNG_DATA_URL.split(',')[1], 'base64').length <= 60 * 1024, 'the ESG reference PNG constant is at most 60 KB (' + Buffer.from(refMod.ESG_PNG_DATA_URL.split(',')[1], 'base64').length + ' bytes)');
  const logged = db.docs.get('Charm_Nest_MonoAssist/4200683017_42006830171_2026-10-15');
  check(logged && logged.calls === 1 && logged.log.length === 1 && logged.log[0].usage.output_tokens === 80 && logged.log[0].ok === true && logged.log[0].round === 1, 'the call is counted and its usage logged on the order\'s day record');

  // absurd numbers are clamped to the contract's bounds (the engine clamps them again)
  ({ db, FV } = fakeDb());
  fa = fakeAnthropic(async () => reply({ adjust: { L: { scale: 9, sx: 0, dx: 50, dy: -9 }, M: { scale: -3, sx: 99, dx: -50, dy: 40 }, R: { scale: 'big', sx: null, dx: Infinity } }, why: '  Went   wild. ' }));
  r = await A.run(body({ adjust: { L: { scale: 1.1, sx: 1, dx: 0, dy: 0 }, M: IDENT, R: { scale: 1, sx: 1.2, dx: 0.5, dy: 0.5 } } }), { db, FV, anthropic: fa });
  check(r.adjust.L.scale === 1.15 && r.adjust.L.sx === 0.8 && r.adjust.L.dx === 2 && r.adjust.L.dy === -1.5 && r.adjust.M.scale === 0.85 && r.adjust.M.sx === 1.25 && r.adjust.M.dx === -2 && r.adjust.M.dy === 1.5 && r.clamped === true, 'scale 9, dx 50, sx 0 ... are clamped to 0.85..1.15, 0.8..1.25, +-2 mm, +-1.5 mm: ' + JSON.stringify(r.adjust.L));
  check(r.adjust.R.scale === 1 && r.adjust.R.sx === 1.2 && r.adjust.R.dx === 0.5 && r.adjust.R.dy === 0.5 && r.why === 'Went wild.', 'a field that is not a number keeps the current value (R scale "big" -> 1, sx null -> 1.2, dx Infinity -> 0.5), the sentence is tidied');
  fa = fakeAnthropic(async () => reply({ adjust: { L: { scale: 1.1 } }, why: '' }));
  r = await A.run(body({ round: 2, adjust: { L: { scale: 1, sx: 1.1, dx: 0.3, dy: 0 }, M: IDENT, R: IDENT } }), { db, FV, anthropic: fa });
  check(r.adjust.L.scale === 1.1 && r.adjust.L.sx === 1.1 && r.adjust.L.dx === 0.3 && r.adjust.M.scale === 1 && r.why === 'Adjusted the letters.', 'a missing letter or field keeps the current adjust (only the asked change is made)');
  const e1 = await A.run(body({ round: 3 }), { db: fakeDb().db, FV, anthropic: fakeAnthropic(async () => reply('this is not json')) });
  const e2 = await A.run(body({ round: 3 }), { db: fakeDb().db, FV, anthropic: fakeAnthropic(async () => reply({ nothing: true })) });
  const e3 = await A.run(body({ round: 3 }), { db: fakeDb().db, FV, anthropic: fakeAnthropic(async () => reply('{"adjust":', { stop_reason: 'max_tokens' })) });
  check(e1.error && !e1.adjust && e2.error && !e2.adjust && /cut off/.test(e3.error), 'malformed JSON, JSON without an adjust, an answer cut off: an { error } with no numbers: ' + [e1.error, e2.error, e3.error].join(' | '));
  ({ db, FV } = fakeDb());
  let rf = await A.run(body(), { db, FV, anthropic: fakeAnthropic(async () => reply('', { stop_reason: 'refusal', content: [] })) });
  check(rf.declined === true && /declined/.test(rf.error) && !rf.adjust, 'a refusal: { declined, error }, no numbers');
  rf = await A.run(body(), { db, FV, anthropic: fakeAnthropic(async () => { throw new Error('socket hang up'); }) });
  check(/model call failed/.test(rf.error) && !rf.adjust, 'a model call that throws: an { error } (never an exception): ' + rf.error);

  // the day's allowance: 3 calls per order per day, reserved before the call, other orders and other days have their own
  ({ db, FV } = fakeDb()); fa = fakeAnthropic(async () => reply(good));
  const day = Date.UTC(2026, 9, 15, 20), next = Date.UTC(2026, 9, 16, 1);
  const got = []; for (let i = 0; i < 4; i++) got.push(await A.run(body({ round: Math.min(3, i + 1) }), { db, FV, anthropic: fa, now: day }));
  check(got.slice(0, 3).every(x => x.adjust) && got[3].skipped && got[3].limited === true && fa.calls.length === 3, 'the 4th call for one order on one day is refused ({ skipped, limited }) and the model is not called: ' + (got[3].skipped || ''));
  const other = await A.run(body({ orderKey: '4202783520_42027835201' }), { db, FV, anthropic: fa, now: day }), tomorrow = await A.run(body(), { db, FV, anthropic: fa, now: next });
  check(other.adjust && tomorrow.adjust && fa.calls.length === 5, 'another order, or the next day, has its own 3 calls');
  const conc = fakeAnthropic(async () => { await sleep(10); return reply(good); });
  ({ db, FV } = fakeDb());
  const par = await Promise.all([1, 2, 3, 4, 5].map(() => A.run(body(), { db, FV, anthropic: conc, now: day })));
  check(par.filter(x => x.adjust).length === 3 && conc.calls.length === 3, 'five at once: exactly 3 get through (the allowance is taken in a transaction)');
  ({ db, FV } = fakeDb()); await A.run(body(), { db, FV, anthropic: fa, now: day, sandbox: true });
  check([...db.docs.keys()].some(k => k.startsWith('Sandbox_Charm_Nest_MonoAssist/')) && ![...db.docs.keys()].some(k => k.startsWith('Charm_Nest_MonoAssist/')), 'the sandbox counts in its own collection');
  A = freshAssist({ CHARM_NEST_MONOGRAM_MODEL: 'claude-test-9', CHARM_NEST_MONOGRAM_EFFORT: 'high' });
  ({ db, FV } = fakeDb()); fa = fakeAnthropic(async () => reply(good)); await A.run(body(), { db, FV, anthropic: fa });
  check(fa.calls[0].model === 'claude-test-9' && fa.calls[0].effort === 'high', 'CHARM_NEST_MONOGRAM_MODEL and CHARM_NEST_MONOGRAM_EFFORT choose the model and the effort');
  A = freshAssist({ CHARM_NEST_MONOGRAM_MODEL: '', CHARM_NEST_MONOGRAM_EFFORT: 'extreme' });
  check(A.MODEL === 'claude-sonnet-5' && A.EFFORT === 'medium', 'an unset model or an unknown effort falls back to the defaults');
  delete process.env.CHARM_NEST_MONOGRAM_MODEL; delete process.env.CHARM_NEST_MONOGRAM_EFFORT; A = freshAssist();

  // ═══ 2 · the background function, through a fake Firestore and Storage ═══
  {
    const { db: bdb, FV: bFV } = fakeDb(), parked = new Map(), deleted = [];
    const payload = body({ adjust: { L: { scale: 1, sx: 1, dx: 0, dy: 0 }, M: IDENT, R: IDENT } });
    parked.set('charmnest/agent/agent-m1.json', payload);
    const admin = { firestore: Object.assign(() => bdb, { FieldValue: bFV }), storage: () => ({ bucket: () => ({ file: p => ({ download: async () => [Buffer.from(JSON.stringify(parked.get(p)))], delete: async () => { deleted.push(p); } }) }) }) };
    const fbPath = path.join(fnDir, 'firebaseAdmin.js'); require.cache[require.resolve(fbPath)] = { id: fbPath, filename: fbPath, loaded: true, exports: admin };
    const anthro = require(clientPath), real = anthro.callClaudeRaw; let called = 0;
    anthro.callClaudeRaw = async () => { called++; return reply({ adjust: { L: { scale: 3, sx: 1, dx: 9, dy: 0 }, M: IDENT, R: IDENT }, why: 'Moved the left letter toward the middle one.' }); };
    process.env.ANTHROPIC_API_KEY = 'fake-not-used';
    try {
      const bg = require(path.join(fnDir, 'charmNestMonogramAssist-background.js'));
      await bdb.collection('Charm_Nest_Agent').doc('agent-m1').set({ id: 'agent-m1', mode: 'monoAssist', status: 'pending', payloadPath: 'charmnest/agent/agent-m1.json' });
      await bdb.collection('Charm_Nest_Agent').doc('agent-g1').set({ id: 'agent-g1', mode: 'grouping', status: 'pending', payloadPath: 'charmnest/agent/agent-m1.json' });
      let h = await bg.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify({ id: 'agent-g1', mode: 'monoAssist' }) });
      check(h.statusCode === 200 && h.body === 'already handled' && called === 0 && bdb.docs.get('Charm_Nest_Agent/agent-g1').status === 'pending', 'a job of another mode is not run by this function');
      h = await bg.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify({ id: 'agent-m1', mode: 'grouping' }) });
      check(h.statusCode === 400 && called === 0, 'a kick that does not say monoAssist is a bad payload');
      h = await bg.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify({ id: 'agent-m1', mode: 'monoAssist' }) });
      const job = bdb.docs.get('Charm_Nest_Agent/agent-m1');
      check(h.statusCode === 200 && job.status === 'done' && job.result.adjust.L.scale === 1.15 && job.result.adjust.L.dx === 2 && job.result.why === 'Moved the left letter toward the middle one.' && called === 1 && deleted.includes('charmnest/agent/agent-m1.json'), 'the job goes pending -> done with the clamped numbers; its parked payload is deleted (getAgent reads the record)');
      h = await bg.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify({ id: 'agent-m1', mode: 'monoAssist' }) });
      check(h.body === 'already handled' && called === 1, 'a second kick of a finished job does nothing');
      h = await bg.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify({ id: 'nope', mode: 'monoAssist' }) });
      check(h.statusCode === 404, 'an unknown job is 404');
      const lib = fs.readFileSync(path.join(fnDir, 'charmNestLibrary.js'), 'utf8');
      check(/"customRead", "monoAssist"\]\.includes\(b\.mode\)/.test(lib) && /mode === "monoAssist" \? "charmNestMonogramAssist-background"/.test(lib), 'charmNestLibrary startAgent accepts mode monoAssist and kicks charmNestMonogramAssist-background (getAgent reads the same Charm_Nest_Agent job)');
      const man = JSON.parse(fs.readFileSync(path.join(root, 'scripts/netlify-function-entries.json'), 'utf8'));
      check(man.endpoints.includes('charmNestMonogramAssist-background.js') && man.modules.includes('_charmNestMonogramAssist.js') && man.modules.includes('_charmNestMonogramRef.js'), 'the function and its two helpers are in the Netlify function manifest');
    } finally { anthro.callClaudeRaw = real; delete process.env.ANTHROPIC_API_KEY; delete require.cache[require.resolve(fbPath)]; }
  }

  // ═══ 3 · the page's loop, with a fake engine ═══
  global.ClipperLib = require(path.join(root, 'vendor/clipper-6.4.2.js'));
  const UI = require(path.join(root, 'charm-nest-monogram-ui.js'));
  const clampTo = (v, lo, hi) => Math.min(hi, Math.max(lo, Number.isFinite(+v) ? +v : 0));
  // a fake engine: the left letter's dx is what fixes the word (the engine clamps it to +-2 like the real one); fewer rules broken as dx grows, the distance to ESG is how far dx is from 1.5
  const texts = n => Array.from({ length: n }, (_, i) => ({ rule: 'contacts', key: 'contacts', text: 'rule ' + (i + 1) + ' is broken' }));
  const engine = { builds: [], build(parts, o) { const a = o && o.adjust ? { L: { scale: 1, sx: 1, dx: clampTo(o.adjust.L && o.adjust.L.dx, -2, 2), dy: 0 }, M: IDENT, R: IDENT } : ADJ0; this.builds.push(a); const n = Math.max(0, 3 - Math.floor(Math.max(a.L.dx, 0) / 0.5)); return { ok: true, bboxPt: [0, 0, 10, 10], rings: [], report: { adjust: a, metrics: { violations: texts(n), distance: Math.abs(a.L.dx - 1.5) } } }; } };
  const base = engine.build({}, null);
  const asker = (answers, log) => async req => { log.push(req); const a = answers[Math.min(log.length - 1, answers.length - 1)]; if (a instanceof Error) throw a; return typeof a === 'function' ? a(req) : a; };
  const ans = (dx, why) => ({ adjust: { L: { scale: 1, sx: 1, dx, dy: 0 }, M: IDENT, R: IDENT }, why: why || 'moved ' + dx });
  let log = [], out = await UI.assistLoop({ E: engine, parts: {}, base, ask: asker([ans(1.0, 'Moved the A right.'), ans(50, 'Moved it much further.')], log) });
  check(out.status === 'assembled' && out.calls === 2 && out.rounds === 2 && out.accepted === 2 && out.remaining.length === 0 && out.adjust.L.dx === 2 && out.why === 'Moved it much further.', 'a better round is kept; the engine clamps dx 50 to 2 (the loop only ever uses what Engine.build reports); it stops as soon as no rule is broken: ' + JSON.stringify({ s: out.status, c: out.calls, dx: out.adjust.L.dx }));
  check(log[1].adjust.L.dx === 1 && log[1].violations.length === 1 && log[0].violations.length === 3 && log[0].round === 1 && log[1].round === 2, 'each round is asked from the best result so far (its adjust, its rules)');
  log = []; out = await UI.assistLoop({ E: engine, parts: {}, base, ask: asker([ans(-1), ans(-1.5), ans(-0.5)], log) });
  check(out.status === 'needs' && out.calls === 3 && out.accepted === 0 && out.res === base && out.remaining.length === 3 && out.why === '', 'rounds that make it worse are never kept: after the 3 rounds the automatic result stays, "needs a person"');
  check(log[1].history.length === 1 && log[1].history[0].adjust.L.dx === -1 && log[2].history.length === 2, 'a refused try is told to the model in the next round (so it does something else)');
  log = []; out = await UI.assistLoop({ E: engine, parts: {}, base, rounds: 10, ask: asker([ans(-1)], log) });
  check(log.length === 3 && out.calls === 3, 'never more than 3 rounds, whatever is asked (rounds: 10 -> 3 calls)');
  log = []; out = await UI.assistLoop({ E: engine, parts: {}, base, rounds: 2, ask: asker([ans(-1)], log) });
  check(log.length === 2, 'a smaller allowance (the day\'s calls left) is respected');
  log = []; out = await UI.assistLoop({ E: engine, parts: {}, base, ask: asker([ans(0.2, 'a little nudge'), ans(-9)], log) });
  check(out.accepted === 1 && out.why === 'a little nudge' && out.adjust.L.dx === 0.2 && out.remaining.length === 3, 'the same number of broken rules with a smaller distance to ESG is kept (0.2); a later worse one is not');
  log = []; out = await UI.assistLoop({ E: engine, parts: {}, base, ask: asker([ans(-0.2), ans(0)], log) });
  check(out.accepted === 0, 'the same number of broken rules with a LARGER distance (or an equal one) is not kept');
  for (const [name, a, want] of [['skipped (no key / kill switch)', { skipped: 'ANTHROPIC_API_KEY is not set' }, 'skipped'], ['an error', { error: 'the model call failed' }, 'failed'], ['a refusal', { declined: true, error: 'the model declined' }, 'failed'], ['the day limit', { skipped: 'limit', limited: true }, 'limited'], ['a throw', new Error('offline'), 'failed'], ['no numbers', { why: 'hm' }, 'failed']]) {
    log = []; out = await UI.assistLoop({ E: engine, parts: {}, base, ask: asker([a], log) });
    check(out.status === want && out.calls === 1 && out.res === base && out.accepted === 0, `${name}: the loop stops after one call, status ${want}, the automatic result is left as it is`);
  }
  log = []; out = await UI.assistLoop({ E: engine, parts: {}, base, ask: asker([ans(1.0), new Error('network down')], log) });
  check(out.status === 'failed' && out.accepted === 1 && out.adjust.L.dx === 1 && out.res !== base, 'a failure after a kept round keeps that round (an engine result), and says failed');
  let n = 0; log = []; out = await UI.assistLoop({ E: engine, parts: {}, base, cancelled: () => n++ > 0, ask: asker([ans(1.0), ans(2.0)], log) });
  check(out.status === 'cancelled' && log.length === 1 && out.accepted === 0, 'a word edited while the model is thinking: the answer is dropped and no more rounds are asked');
  const okBase = { ok: true, report: { adjust: ADJ0, metrics: { violations: [], distance: 0.4 } } };
  log = []; out = await UI.assistLoop({ E: engine, parts: {}, base: okBase, ask: asker([ans(1)], log) });
  check(out.status === 'passed' && log.length === 0, 'an ESG-like word (no rule broken) never calls the AI');
  log = []; out = await UI.assistLoop({ E: engine, parts: {}, base: { ok: true, report: {} }, ask: asker([ans(1)], log) });
  check(out.status === 'unsupported' && log.length === 0, 'an engine that reports no rules is never asked either');
  const badEngine = { build: () => ({ ok: false, why: 'no' }) };
  log = []; out = await UI.assistLoop({ E: badEngine, parts: {}, base, ask: asker([ans(1)], log) });
  check(out.accepted === 0 && out.status === 'needs' && out.res === base, 'an answer the engine cannot build is refused like a worse one');

  if (process.env.MONOAI_NO_PAGE === '1') { finish(); return; }
  await pageTests();
  finish();

  function finish() {
    Object.assign(process.env, saved);
    check(require(clientPath).__stub === true && clientStub.strayCalls === 0, 'the real model client was never loaded and nothing reached it: no real API call anywhere in this test');
    console.log(`   (${Math.round((Date.now() - T0) / 1000)} s)`);
    console.log(fails.length ? `\nmonogram-ai: ${fails.length} FAILED of ${nCheck}:\n - ${fails.join('\n - ')}` : `\nmonogram-ai OK: ${nCheck} checks (no real model call anywhere)`);
    process.exit(fails.length ? 1 : 0);
  }

  // ═══ 4 · the real page ═══
  async function pageTests() {
    const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
    try { require(path.join(pwDir, 'playwright-core')); } catch (_) { console.log('monogram-ai: no playwright-core, the page part is skipped'); return; }
    process.env.PW_DIR = pwDir;
    const Base = require('./_orderFactsWorld.cjs'), Alpha = require('./_monogramAlphabets.cjs');
    const V = (name, value) => ({ name, value }), TITLE = 'Monogram Necklace Personalized Jewelry Initials Pendant';
    const FX = {
      apv: { rid: '4200683017', sku: 'MONOGRAM_5351', listing: '997905351', title: TITLE, buyer: 'Name One', placed: { day: 0, h: 9, m: 10 }, ship: 5, variations: [V('Metal choice', 'Gold')], personalization: ['APV'], metalKey: 'gold' },
      osr: { rid: '4202783520', sku: 'MONOGRAM_9317', listing: '606439317', title: 'Monogram Necklace Dainty Monogram Necklace', buyer: 'Name Two', placed: { day: 0, h: 9, m: 20 }, ship: 5, variations: [V('Metal choice', 'Silver')], personalization: ['O S R'], metalKey: 'silver' },
      klm: { rid: '4200900001', sku: 'NEWMONO_1', listing: '888000001', title: 'Custom Monogram Pendant Necklace, 3 Initials', buyer: 'Name Three', placed: { day: 0, h: 9, m: 30 }, ship: 5, variations: [V('Metal choice', 'Rose Gold')], personalization: ['J.Q.X'], metalKey: 'rose' }
    };
    const w = await Base.world({ viewport: { width: 1440, height: 900 } });
    if (!w) { console.log('monogram-ai: no playwright-core, the page part is skipped'); return; }
    const { page, srv } = w, st = srv.st;
    try {
      const set = await Alpha.make(); Alpha.seed(st, set);
      for (let i = 0; i < 8; i++) { const want = [...st.docs.keys()].filter(k => k.startsWith('Charm_Master_Index/')).length; const have = await page.evaluate(async () => { await Master.load(true); return B.master.entries.size; }); if (have === want) break; await sleep(400); }
      await page.evaluate(async ({ fixtures, orderSrc }) => {
        const orderOf = eval('(' + orderSrc + ')'), now = Date.now(); await Orders.loadMaps(true);
        for (const fx of fixtures) { const order = orderOf(fx, now); for (const line of order.lines) { if (fx.metalKey) line.metalKey = fx.metalKey; const key = CharmNestOrders.lineKey(order, line); if (B.orders.byKey.get(key)) continue; const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); } }
        Orders.interpretAll();
      }, { fixtures: Object.values(FX), orderSrc: Base.orderOf.toString() });
      // the engine made to fail the rules until the left letter is moved right by 0.3 mm or more (the real engine does the building and the clamping of the adjust; only the verdict is staged)
      // and the page's agent call stubbed: every call is recorded and answered from the queue, nothing leaves the page
      await page.evaluate(() => {
        window.__calls = []; window.__answers = {}; window.__fix = true;
        const E = CharmNestMonogram, eb = E.build;
        E.build = function (l, o) {
          const r = eb.apply(this, arguments); if (!r || !r.ok || !window.__fix) return r;
          const a = r.report.adjust, fixed = !!(a && a.L && a.L.dx >= 0.3), tag = l.left.letter + l.middle.letter + l.right.letter;
          r.report.metrics = Object.assign({}, r.report.metrics, fixed ? { violations: [], distance: 0.5, ok: true } : { violations: [{ rule: 'contacts', key: 'contacts', text: 'the left and middle letters are not held by one contact near the top and one near the bottom' }], distance: 3, ok: false });
          window.__lastBuild = { tag, adjust: a }; return r;
        };
        CN.setAgent(async (mode, payload, opts) => {
          window.__calls.push({ mode, payload, opts }); const q = window.__answers[payload.letters.join('')], next = q && q.shift();
          if (next === 'throw') throw new Error('network down'); return next || { skipped: 'nothing queued' };
        });
      });
      const card = rid => `.moCard[data-rid="${rid}"]`;
      const read = rid => page.evaluate(sel => { const h = document.querySelector(sel); if (!h) return null; const q = s => h.querySelector(s); return { state: h.dataset.state, st: q('.moSt').textContent, meta: q('.moMeta').textContent, spin: !q('.moSpin').hidden, spinTxt: q('.moSpin span').textContent, d: q('.moArt svg path') ? q('.moArt svg path').getAttribute('d') : '', approve: { text: q('[data-mo="approve"]').textContent, hidden: q('[data-mo="approve"]').hidden, disabled: q('[data-mo="approve"]').disabled }, again: !q('[data-mo="again"]').hidden, ins: [...h.querySelectorAll('input[data-mo-l]')].map(i => i.value).join('') }; }, card(rid));
      const until = async (rid, fn, what, ms = 25000) => { const t0 = Date.now(); let r; for (;;) { r = await read(rid); if (r && fn(r)) return r; if (Date.now() - t0 > ms) { console.log('   (waiting for ' + what + ', last: ' + JSON.stringify(r).slice(0, 300) + ')'); return r; } await sleep(80); } };
      const idle = rid => until(rid, r => !r.spin && r.state !== 'busy', 'an idle card');
      const calls = () => page.evaluate(() => window.__calls.length);
      const queue = (word, items) => page.evaluate(({ word, items }) => { (window.__answers[word] = window.__answers[word] || []).push(...items); }, { word, items });
      const settings = () => page.evaluate(() => JSON.parse(JSON.stringify((S.settings && S.settings.mono) || {})));
      const clickApprove = rid => page.click(`${card(rid)} [data-mo="approve"]`);
      const files = () => page.evaluate(() => Object.values(CustomSheet.entries()).map(e => e.files.map(f => f.name).join()).filter(Boolean).sort());
      // the three words all break the staged rule; what the (stubbed) model answers is queued per word BEFORE the tab opens
      //   APV: the first answer fixes it (absurd numbers: the engine clamps them) · OSR: nothing it says helps (3 rounds) · JQX: the call fails
      await queue('APV', [{ adjust: { L: { scale: 9, sx: 1, dx: 50, dy: 0 }, M: IDENT, R: IDENT }, why: 'Moved the A toward the P so a lower contact joins them.', model: 'fake', usage: {} }]);
      await queue('OSR', [ans(-1.0, 'tried left'), ans(-0.5, 'tried less'), ans(0.1, 'tried a nudge')]);
      await queue('JQX', ['throw']);
      await page.evaluate(() => { CN.setMode('engrave'); Engrave.restoreView({ tab: 'mono', chosen: true }); Engrave.render(); });

      // ── APV: breaks a rule, the first answer fixes it ──
      await page.waitForSelector(`${card('4200683017')} svg path`, { timeout: 30000 });
      let a = await until('4200683017', r => /Assembled with AI/.test(r.meta) && !r.spin, 'APV assembled');
      check((await page.evaluate(() => window.__calls.filter(c => c.payload.letters.join('') === 'APV').length)) === 1 && /Assembled with AI: Moved the A toward the P so a lower contact joins them\./.test(a.meta), 'a word that breaks a rule is assembled with the AI, one call, and the card says one calm line: "Assembled with AI: Moved the A toward the P so a lower contact joins them."');
      check(a.state === 'ready' && a.approve.text === 'Approve' && !a.approve.disabled && !a.again, 'the card is ready, Approve is one press, no "Improve again" on a word that passes now');
      const c1 = await page.evaluate(() => window.__calls.find(c => c.payload.letters.join('') === 'APV'));
      check(c1.mode === 'monoAssist' && c1.opts.background === true && c1.payload.letters.join('') === 'APV' && c1.payload.round === 1 && c1.payload.orderKey === '4200683017_42006830171', 'the call is mode monoAssist, in the background, with the three letters, the order line and round 1');
      const png = Buffer.from(c1.payload.image.split(',')[1] || '', 'base64'), pw = png.readUInt32BE(16), ph = png.readUInt32BE(20);
      check(/^data:image\/png;base64,/.test(c1.payload.image) && png.slice(1, 4).toString() === 'PNG' && Math.max(pw, ph) === 560 && png.length < 400000, `the candidate goes as a 2x PNG data URL (${pw} x ${ph} px, ${(png.length / 1024).toFixed(0)} KB)`);
      fs.mkdirSync(process.env.SHOTS || '/tmp/mono-shots', { recursive: true }); fs.writeFileSync(path.join(process.env.SHOTS || '/tmp/mono-shots', 'ai-candidate-APV.png'), png);
      check(c1.payload.violations.length === 1 && /one contact near the top/.test(c1.payload.violations[0]) && c1.payload.adjust.L.dx === 0 && c1.payload.metrics && c1.payload.metrics.distance === 3 && Array.isArray(c1.payload.metrics.contacts), 'it sends the violated rule in words, the current adjust and the engine\'s metrics (with the contacts)');
      const accepted = A.cleanRequest(c1.payload);
      check(!accepted.error && accepted.req.letters.join('') === 'APV', 'the server accepts exactly the payload the page sent: ' + (accepted.error || 'ok'));
      const s1 = await settings(), rec = Object.values(s1.assist || {})[0];
      check(Object.keys(s1.assist || {}).length === 1 && rec.status === 'assembled' && rec.adjust.L.dx === 2 && rec.adjust.L.scale <= 1.15 && /Moved the A/.test(rec.why) && rec.rounds === 1, 'the final adjust (clamped by the engine: dx 2, scale <= 1.15), the sentence and the rounds are kept in the page\'s settings: ' + JSON.stringify(rec));
      check((s1.assistDay['4200683017_42006830171'] || {}).n === 1, 'the day\'s calls for the order are counted on the page too');
      await clickApprove('4200683017'); await until('4200683017', r => r.state === 'approved', 'APV approved');
      check((await files()).join('|') === 'Monogram APV.ai', 'Approve is one press for a word that now passes: Monogram APV.ai is on the order\'s designs');

      // ── reopen: the kept answer is applied again through the engine, no call ──
      const dBefore = a.d, callsBefore = await calls();   // (the OSR and JQX cards are still working or done: wait for them first)
      await until('4202783520', r => /Needs a person/.test(r.meta) && !r.spin, 'OSR done', 40000); await until('4200900001', r => !r.spin && /could not help|Needs a person/.test(r.meta), 'JQX done', 40000);
      const callsBefore2 = await calls();
      await page.evaluate(async () => { const ui = Engrave.monogramUI(), c = ui.cards.get('4200683017_42006830171'); ui.results.clear(); await ui.rebuild(c); });
      a = await until('4200683017', r => /Assembled with AI/.test(r.meta) && !r.spin, 'APV reopened');
      check(await calls() === callsBefore2 && a.d === dBefore, 'reopening makes no call: the kept adjust is built again by the engine and gives the same picture');
      check((await page.evaluate(() => window.__lastBuild.adjust.L.dx)) === 2, 'the engine was asked for the kept adjust (dx 2)');
      await page.evaluate(async () => { const ui = Engrave.monogramUI(), c = ui.cards.get('4200683017_42006830171'); await ui.rebuild(c); });      // (this time the finished monogram comes from the page's memory, results kept)
      a = await until('4200683017', r => !r.spin, 'APV from memory');
      check(await calls() === callsBefore2 && /Assembled with AI: Moved the A toward the P/.test(a.meta) && a.d === dBefore, 'a monogram the AI made pass keeps its line when it comes from the page\'s memory too (typing the letters back): still no call');

      // ── OSR: nothing the model says helps: 3 rounds, then "Needs a person"; Approve asks twice ──
      a = await until('4202783520', r => /Needs a person/.test(r.meta) && !r.spin, 'OSR needs a person', 40000);
      const osrCalls = await page.evaluate(() => window.__calls.filter(c => c.payload.letters.join('') === 'OSR'));
      check(osrCalls.length === 3 && osrCalls.map(c => c.payload.round).join() === '1,2,3', 'never more than 3 rounds for a word: exactly 3 calls, rounds 1, 2, 3 (' + osrCalls.length + ')');
      check(/Needs a person: the left and middle letters are not held by one contact near the top and one near the bottom/.test(a.meta) && !/Assembled with AI/.test(a.meta) && a.st === 'Needs a person', 'the card says "Needs a person:" and names the limit: ' + a.meta);
      check(osrCalls[2].payload.history.length === 2 && osrCalls[1].payload.history.length === 1, 'the refused tries went to the model in the following rounds');
      check(a.approve.text === 'Approve' && !a.approve.disabled && a.again, 'a flagged card still has Approve, and a quiet "Improve again"');
      await clickApprove('4202783520'); a = await until('4202783520', r => r.approve.text === 'Approve anyway', 'confirm');
      check(a.approve.text === 'Approve anyway' && /approve it anyway/.test(a.meta) && (await files()).join('|') === 'Monogram APV.ai', 'the first press only asks "Approve anyway" (nothing approved yet)');
      await clickApprove('4202783520'); await until('4202783520', r => r.state === 'approved', 'OSR approved');
      check((await files()).join('|') === 'Monogram APV.ai|Monogram OSR.ai', 'the second press approves it: Monogram OSR.ai');
      const s2 = await settings(); const osrRec = Object.entries(s2.assist).find(([k]) => k.startsWith('4202783520'));
      check(osrRec && osrRec[1].status === 'needs' && /not held by one contact/.test(osrRec[1].limit) && s2.assistDay['4202783520_42027835201'].n === 3, 'the outcome ("needs") and the limit are kept too, 3 calls counted for the day');

      // ── Improve again: the day's 3 calls are used, so no call is made ──
      const cBefore = await calls();
      await page.evaluate(() => { const ui = Engrave.monogramUI(), c = ui.cards.get('4202783520_42027835201'); c.confirmed = false; ui.results.clear(); });
      await page.evaluate(async () => { const ui = Engrave.monogramUI(); ui.cards.get('4202783520_42027835201').draft = ['O', 'S', 'R']; await ui.rebuild(ui.cards.get('4202783520_42027835201')); });
      a = await idle('4202783520');
      if (a.again) await page.click(`${card('4202783520')} [data-mo="again"]`); await sleep(500); a = await idle('4202783520');
      check(await calls() === cBefore && /3 tries for this order today|Needs a person/.test(a.meta), 'Improve again after the 3 calls of the day: no call is made, the card says so: ' + a.meta.slice(0, 160));

      // ── JQX: the model call fails: the automatic result stays and the card says so once ──
      a = await until('4200900001', r => /could not help this time/.test(r.meta) && !r.spin, 'JQX failed');
      check(a.state === 'ready' && a.d.length > 500 && /Needs a person/.test(a.meta) && (a.meta.match(/could not help/g) || []).length === 1, 'a failing call leaves the automatic monogram in place, says so once and still lets a person decide: ' + a.meta.slice(0, 200));
      const jqx = await settings(); check(!Object.keys(jqx.assist).some(k => k.startsWith('4200900001')), 'a failed call keeps no answer (it may be tried again later)');

      // ── the kill switch: with AI assist Off, a flagged word makes no call ──
      await page.evaluate(() => { const b = document.querySelector('[data-mo-ai]'); if (!b || b.textContent !== 'On') throw new Error('no AI assist switch, or it is not On'); b.click(); });
      const k0 = await calls();
      await page.evaluate(async () => { const ui = Engrave.monogramUI(), c = ui.cards.get('4200900001_42009000011'); ui.results.clear(); c.ai = null; await ui.rebuild(c); });
      a = await idle('4200900001');
      check(await calls() === k0 && /Needs a person/.test(a.meta) && !a.again && (await settings()).ai === 'off', 'AI assist Off: no call, the card says Needs a person, no "Improve again"');
      check(w.errors.length === 0, 'no page error: ' + JSON.stringify(w.errors.slice(0, 3)));
    } catch (e) { fails.push('page crash: ' + (e && e.stack || e)); console.error(e); }
    finally { await w.close(); }
  }
})().catch(e => { console.error(e); process.exit(1); });
