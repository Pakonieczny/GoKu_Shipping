// A structured-output schema too large for Anthropic to compile ("The compiled grammar is too large")
// is refused with a 400 before any answer starts, so it is not billed. The shared client then asks
// once more with the schema in the instructions, keeps only the JSON, checks it against the schema,
// and remembers the schema name for the warm lambda. Offline: a fake Anthropic, no request leaves.
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), { Readable } = require('node:stream');
const repo = path.resolve(__dirname, '../..'), fn = name => path.join(repo, 'netlify/functions', name);
const C = require(fn('_googleAdsClaude.js'));
const GRAMMAR = 'The compiled grammar is too large, which would cause performance issues. Simplify your tool schemas or reduce the number of strict tools.';
const J = JSON.stringify, quiet = () => {}, noSleep = async () => {}, env = { ANTHROPIC_API_KEY: 'fixture-key' };
let passed = 0; const check = (v, msg) => { assert(v, msg); passed++; console.log('PASS', msg); };

const sse = events => Readable.from([Buffer.from(events.map(e => `event: ${e.type}\ndata: ${JSON.stringify(e)}\n\n`).join(''), 'utf8')]);
// One streamed answer: a thinking block, then the text.
const answer = (text, { input = 900, output = 300, stop = 'end_turn' } = {}) => () => ({ ok: true, status: 200, headers: { get: () => null }, body: sse([
  { type: 'message_start', message: { id: 'msg_fixture', type: 'message', role: 'assistant', model: 'claude-sonnet-5-5', content: [], stop_reason: null, usage: { input_tokens: input, output_tokens: 1 } } },
  { type: 'content_block_start', index: 0, content_block: { type: 'thinking', thinking: '' } }, { type: 'content_block_delta', index: 0, delta: { type: 'signature_delta', signature: 'sig' } }, { type: 'content_block_stop', index: 0 },
  { type: 'content_block_start', index: 1, content_block: { type: 'text', text: '' } }, { type: 'content_block_delta', index: 1, delta: { type: 'text_delta', text } }, { type: 'content_block_stop', index: 1 },
  { type: 'message_delta', delta: { stop_reason: stop, stop_sequence: null }, usage: { output_tokens: output } }, { type: 'message_stop' }]) });
const refusal = (status, type, message) => () => ({ ok: false, status, statusText: 'error', headers: { get: () => null }, text: async () => J({ type: 'error', error: { type, message } }) });
const refused = (message = GRAMMAR) => refusal(400, 'invalid_request_error', message);
function anthropic(replies) {
  const calls = [];
  const fetch = async (url, init) => { assert.equal(url, 'https://api.anthropic.com/v1/messages'); calls.push(JSON.parse(init.body)); const next = replies.shift(); if (!next) throw Error('unexpected extra request'); return next(); };
  fetch.calls = calls; return fetch;
}
const client = fetch => C.createClaudeClient({ env, fetch, sleep: noSleep, log: quiet });
const failure = promise => promise.then(() => null, e => e);
// A minimal answer that satisfies a structured-output schema (arrays may be empty: the grammar has no minItems).
function example(s, root = s) {
  if (s.$ref) { const m = /^#\/(\$defs|definitions)\/(.+)$/.exec(s.$ref); return example(root[m[1]][m[2]], root); }
  if (s.anyOf) return example(s.anyOf[0], root);
  if (s.const !== undefined) return s.const;
  if (s.enum) return s.enum[0];
  const t = [].concat(s.type || (s.properties ? 'object' : 'string'))[0];
  if (t === 'object') return Object.fromEntries((s.required || Object.keys(s.properties || {})).map(k => [k, example(s.properties[k], root)]));
  return t === 'array' ? [] : t === 'number' || t === 'integer' ? 1 : t === 'boolean' ? true : t === 'null' ? null : 'x';
}

const review = { type: 'object', properties: { pass: { type: 'boolean' }, productFaithful: { type: 'boolean' }, mobileReadable: { type: 'boolean' }, issues: { type: 'array', items: { type: 'string' } }, score: { type: 'number', minimum: 0, maximum: 100 } }, required: ['pass', 'productFaithful', 'mobileReadable', 'issues', 'score'] };
const good = { pass: true, productFaithful: true, mobileReadable: true, issues: [], score: 96 };

(async () => {
  // ── The shared client: one retry, JSON parsed and checked, cost of one answer ──
  {
    const fetch = anthropic([refused(), answer('Here is the review:\n```json\n' + J(good) + '\n```')]);
    const out = await client(fetch).json({ prompt: 'Review the images.', effort: 'high', label: 'fixture-review', schema: review });
    const [first, second] = fetch.calls;
    check(fetch.calls.length === 2, 'client: the grammar refusal is followed by exactly one more request');
    check(first.output_config.format.type === 'json_schema' && first.output_config.format.schema.additionalProperties === false, 'client: the first request asks for structured output');
    check(!second.output_config.format && second.output_config.effort === 'high' && J(second.messages) === J(first.messages) && J(second.thinking) === J(first.thinking) && second.max_tokens === first.max_tokens,
      'client: the retry drops only output_config.format; prompt, thinking, effort and room are unchanged');
    check(second.system.startsWith(first.system) && second.system.includes('Return only one JSON object that matches this JSON schema') && second.system.includes(J(first.output_config.format.schema)) && /score: minimum 0, maximum 100/.test(second.system),
      'client: the retry keeps the instructions and adds the same schema, with the rules the schema cannot express');
    check(J(out.data) === J(good) && out.text === J(good) && out.message.schemaInPrompt === true && J(out.message.schemaErrors) === '[]', 'client: the fenced answer is reduced to its JSON, parsed and checked against the schema');
    check(out.usage.input_tokens === 900 && out.usage.output_tokens === 300 && out.costUsd === C.estimateCostUsd({ input_tokens: 900, output_tokens: 300 }), 'client: usage and cost are those of the one answer (the refusal is not billed)');
  }
  {
    const fetch = anthropic([answer(J(good))]);
    const out = await client(fetch).json({ prompt: 'Review again.', label: 'fixture-review', schema: review });
    check(fetch.calls.length === 1 && !fetch.calls[0].output_config.format && fetch.calls[0].system.includes('matches this JSON schema') && out.data.pass === true,
      'client: later requests for that schema in the warm lambda skip the refused attempt, from a new client too');
    const other = anthropic([answer(J(good))]);
    await client(other).json({ prompt: 'Review.', label: 'fixture-other-schema', schema: review });
    check(other.calls.length === 1 && other.calls[0].output_config.format, 'client: other schemas still ask for structured output first');
  }
  {
    const fetch = anthropic([refused(), answer(J({ pass: 'yes', productFaithful: true, issues: [], score: 96, extra: 1 }))]);
    const e = await failure(client(fetch).json({ prompt: 'Review.', label: 'fixture-strays', schema: review }));
    check(e && e.code === 'CLAUDE_BAD_JSON' && e.schemaErrors.includes('$.pass should be boolean, not string') && e.schemaErrors.includes('$.mobileReadable is missing') && e.schemaErrors.includes('$.extra is not in the schema'),
      'client: an answer that strays from the schema is refused with the reasons');
    check(fetch.calls.length === 2 && e.usage.output_tokens === 300 && e.costUsd > 0, 'client: that is still one paid answer, reported with its cost, and nothing is re-sent');
  }
  {
    let fetch = anthropic([refused(), refused()]);
    let e = await failure(client(fetch).json({ prompt: 'Review.', label: 'fixture-twice', schema: review }));
    check(e && e.status === 400 && fetch.calls.length === 2, 'client: no second retry: a refused retry is reported, never sent again');
    fetch = anthropic([refused(), refused('messages.0.content.1: image exceeds 5 MB maximum')]);
    e = await failure(client(fetch).json({ prompt: 'Review.', label: 'fixture-then-other', schema: review }));
    check(e && /image exceeds/.test(e.message) && fetch.calls.length === 2, 'client: another refusal of the retry is reported as it is');
    fetch = anthropic([refused(), refusal(529, 'overloaded_error', 'Overloaded')(), answer(J(good))].map(r => typeof r === 'function' ? r : () => r));
    const out = await client(fetch).json({ prompt: 'Review.', label: 'fixture-busy', schema: review });
    check(fetch.calls.length === 3 && !fetch.calls[1].output_config.format && !fetch.calls[2].output_config.format && out.data.score === 96, 'client: an overload on the retry is retried as usual, still without the grammar');
  }
  {
    let fetch = anthropic([refused('max_tokens: 200000 > 128000, which is the maximum allowed number of output tokens')]);
    let e = await failure(client(fetch).json({ prompt: 'Review.', label: 'fixture-other-400', schema: review }));
    check(e && e.status === 400 && fetch.calls.length === 1 && !C._test.grammarLimited.has('fixture-other-400'), 'client: other 400s are not retried and nothing is remembered');
    fetch = anthropic([refused()]);
    e = await failure(client(fetch).json({ prompt: 'No schema.', label: 'fixture-no-schema' }));
    check(e && e.status === 400 && fetch.calls.length === 1, 'client: a request without a schema is not retried');
    const limit = (status, message) => C._test.grammarLimit({ status, message: `Claude request failed (${status}): ${message}` });
    check(limit(400, GRAMMAR) && limit(400, 'Schemas contains too many optional parameters (31), which would make grammar compilation inefficient.') && limit(400, 'Schema is too complex for grammar compilation') && limit(400, 'Too many parameters with union types (19)')
      && !limit(400, 'prompt is too long: 250000 tokens > 200000 maximum') && !limit(400, 'messages.0.content.1: image exceeds 5 MB maximum') && !limit(500, GRAMMAR) && !C._test.grammarLimit(null),
      'client: only a 400 about the schema grammar starts the fallback');
  }
  {
    // Analyze Ad builds its own body and sends it with retryUnknown:false; its label keys the memory.
    const body = { max_tokens: 20000, thinking: { type: 'adaptive' }, output_config: { effort: 'high', format: { type: 'json_schema', schema: C.sanitizeSchema(review).schema } }, messages: [{ role: 'user', content: [{ type: 'text', text: 'Analyze.' }] }] };
    const fetch = anthropic([refused(), answer('```json\n' + J(good) + '\n```')]);
    const msg = await client(fetch).message(body, { retryUnknown: false, label: 'fixture-analysis' });
    check(fetch.calls.length === 2 && !fetch.calls[1].output_config.format && fetch.calls[1].system.includes('matches this JSON schema') && C.finalText(msg) === J(good) && msg.content[0].type === 'thinking',
      'client: a hand-built body sent with retryUnknown:false gets the same single retry, and its text is the bare JSON');
    check(body.output_config.format && !('system' in body), 'client: the caller\'s own body object is left as it was');
  }

  // ── Ad Design "Plan scenes": the real brites_responsive_ad request through the Ad Design adapter ──
  const { createAdDesignAdapters } = require(fn('googleAdsAdDesignAdapters.js')), research = require(fn('googleAdsAdDesignResearch.js')), sharp = require('sharp');
  const png = await sharp({ create: { width: 64, height: 64, channels: 3, background: '#eeeeee' } }).png().toBuffer();
  const evidence = { sources: [{ id: 'landing', status: 'available', url: 'https://britesjewelry.com/products/fixture', title: 'Fixture charm' }, { id: 'product:1', status: 'available', title: 'Fixture charm' }] };
  const scenes = research.buildResponsiveRequest({ evidence, request: { productId: 'gid://shopify/Product/1', groupRef: 'g', artboard: 'square', device: 'mobile', instruction: '', includeAnimation: false }, screenshotDataUrl: 'data:image/png;base64,' + png.toString('base64'), sources: [] });
  const docs = new Map(), db = { collection: name => ({ doc: id => ({ get: async () => ({ exists: docs.has(name + '/' + id), data: () => docs.get(name + '/' + id) }), set: async v => { docs.set(name + '/' + id, v); } }) }) };
  const deps = fetch => ({ fb: () => ({ db }), env: { ANTHROPIC_API_KEY: 'fixture', OPENAI_API_KEY: 'fixture' }, fetch, sleep: noSleep, log: quiet, responsePollWindowMs: 0 });
  const schema = C.sanitizeSchema(scenes.text.format.schema).schema, plan = example(schema);
  {
    const fetch = anthropic([refused(), answer('The plan:\n```json\n' + J(plan) + '\n```', { input: 30000, output: 4000 })]);
    const res = await createAdDesignAdapters(deps(fetch)).responses({ ...scenes, background: true }, 'plan-scenes');
    const [first, second] = fetch.calls;
    check(scenes.text.format.name === 'brites_responsive_ad' && fetch.calls.length === 2, 'Plan scenes: the grammar refusal is followed by exactly one more request');
    check(J(first.output_config.format.schema).includes('"scenePlans"') && !second.output_config.format && second.system.includes(J(first.output_config.format.schema)) && J(second.messages) === J(first.messages),
      'Plan scenes: the real responsive schema moves from output_config.format into the instructions; the prompt and images are unchanged');
    const parsed = research.parseResponse(res);
    check(res.status === 'completed' && res.output_text === J(plan) && J(parsed) === J(plan) && C.schemaErrors(parsed, schema).length === 0, 'Plan scenes: the answer reaches the existing parser as plain JSON that matches the schema');
    check(res.usage.output_tokens === 4000 && res.estimatedUsd === C.estimateCostUsd({ input_tokens: 30000, output_tokens: 4000 }), 'Plan scenes: the cost is that of the one answer');
    assert.throws(() => research.validateResponsivePlan({ output: parsed, request: {}, evidence }), /complete Google text assets/); passed++;
    console.log('PASS Plan scenes: the existing plan validation still applies (an empty plan is refused)');
    const receipt = [...docs.values()].find(v => v.requestId === 'plan-scenes');
    check(receipt && receipt.completedAt && receipt.response.output_text === J(plan), 'Plan scenes: the durable receipt keeps the paid answer');
    const again = anthropic([answer(J(plan))]);
    await createAdDesignAdapters(deps(again)).responses({ ...scenes, background: true }, 'plan-scenes-2');
    check(again.calls.length === 1 && !again.calls[0].output_config.format, 'Plan scenes: the next plan in this warm lambda goes straight to the instructions');
  }

  // ── Every strict schema in the Adwords app reaches Claude through this client, so each gets the fallback ──
  const files = fs.readdirSync(fn('')).filter(f => /^_?googleAds.*\.js$/.test(f));
  check(files.filter(f => fs.readFileSync(fn(f), 'utf8').includes('api.anthropic.com')).join() === '_googleAdsClaude.js', 'coverage: _googleAdsClaude.js is the only Adwords module that calls Anthropic');
  const names = [...new Set(files.flatMap(f => [...fs.readFileSync(fn(f), 'utf8').matchAll(/type:\s*['"]json_schema['"],\s*name:\s*['"]([A-Za-z0-9_]+)['"]/g)].map(m => m[1])))].sort();
  for (const name of ['ad_design_composition', 'ad_design_concept', 'ad_design_quality', 'brites_editor_design', 'brites_motion_copy_fix', 'brites_motion_treatment', 'brites_responsive_ad', 'brites_subject_focus', 'video_product_bounds']) assert(names.includes(name), name);
  for (const name of names) {
    const request = { model: research.MODEL, input: [{ role: 'user', content: [{ type: 'input_text', text: 'Fixture for ' + name }] }], text: { format: { type: 'json_schema', name: name + '_fixture', strict: true, schema: review } } };
    const fetch = anthropic([refused(), answer(J(good)), answer(J(good))]), a = createAdDesignAdapters(deps(fetch));
    const one = research.parseResponse(await a.responses(request, name + '-1')), two = research.parseResponse(await a.responses(request, name + '-2'));
    assert(fetch.calls.length === 3 && fetch.calls[0].output_config.format && !fetch.calls[1].output_config.format && !fetch.calls[2].output_config.format && one.pass && two.pass, name);
  }
  check(names.length >= 9, `coverage: all ${names.length} named Adwords schemas (${names.join(', ')}) take the one retry through the Ad Design adapter, then skip the refused attempt`);

  console.log(`${passed} grammar fallback checks passed.`);
})().catch(e => { console.error(e); process.exitCode = 1; });
