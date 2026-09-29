// Offline checks for the shared Sonnet 5.5 client. A fake fetch replays
// recorded-style SSE streams; nothing here reaches the Anthropic API.
const assert = require('node:assert/strict');
const { Readable } = require('node:stream');
const C = require('../../netlify/functions/_googleAdsClaude.js');

function sse(events, { split = 0 } = {}) {
  const text = events.map(e => `event: ${e.type}\ndata: ${JSON.stringify(e)}\n\n`).join('');
  const bytes = Buffer.from(text, 'utf8');
  if (!split) return Readable.from([bytes]);
  const parts = [];
  for (let i = 0; i < bytes.length; i += split) parts.push(bytes.subarray(i, i + split));
  return Readable.from(parts);
}
function streamOf({ id = 'msg_1', blocks = [], stop = 'end_turn', usage = {}, outUsage = {}, stopDetails, cut = false }) {
  const events = [{ type: 'message_start', message: { id, type: 'message', role: 'assistant', model: 'claude-sonnet-5-5', content: [], stop_reason: null, usage: { input_tokens: 100, output_tokens: 1, ...usage } } }];
  blocks.forEach((b, index) => {
    if (b.type === 'text') {
      events.push({ type: 'content_block_start', index, content_block: { type: 'text', text: '' } });
      const t = b.text, half = Math.ceil(t.length / 2);
      events.push({ type: 'content_block_delta', index, delta: { type: 'text_delta', text: t.slice(0, half) } });
      events.push({ type: 'content_block_delta', index, delta: { type: 'text_delta', text: t.slice(half) } });
      for (const citation of b.citations || []) events.push({ type: 'content_block_delta', index, delta: { type: 'citations_delta', citation } });
    } else if (b.type === 'thinking') {
      events.push({ type: 'content_block_start', index, content_block: { type: 'thinking', thinking: '' } });
      events.push({ type: 'content_block_delta', index, delta: { type: 'signature_delta', signature: 'sig' } });
    } else if (b.type === 'server_tool_use') {
      events.push({ type: 'content_block_start', index, content_block: { type: 'server_tool_use', id: b.id, name: 'web_search', input: {} } });
      events.push({ type: 'content_block_delta', index, delta: { type: 'input_json_delta', partial_json: '{"query":' } });
      events.push({ type: 'content_block_delta', index, delta: { type: 'input_json_delta', partial_json: JSON.stringify(b.query) + '}' } });
    } else {
      events.push({ type: 'content_block_start', index, content_block: b });
    }
    events.push({ type: 'content_block_stop', index });
  });
  if (!cut) {
    events.push({ type: 'message_delta', delta: { stop_reason: stop, stop_sequence: null, ...(stopDetails ? { stop_details: stopDetails } : {}) }, usage: { output_tokens: 50, ...outUsage } });
    events.push({ type: 'message_stop' });
  }
  return events;
}
function fakeFetch(replies) {
  const calls = [];
  const fn = async (url, init) => {
    calls.push({ url, init, body: JSON.parse(init.body) });
    const next = replies.shift();
    if (!next) throw new Error('unexpected extra request');
    if (next.throw) throw next.throw;
    if (next.status && next.status !== 200) {
      return { ok: false, status: next.status, statusText: 'err', headers: { get: k => (next.headers || {})[k.toLowerCase()] || null }, text: async () => JSON.stringify({ type: 'error', error: { type: next.errorType || 'invalid_request_error', message: next.message || 'bad' } }) };
    }
    return { ok: true, status: 200, headers: { get: () => null }, body: sse(next.events, { split: next.split || 0 }) };
  };
  fn.calls = calls;
  return fn;
}
const quiet = () => {};
const noSleep = async () => {};
const env = { ANTHROPIC_API_KEY: 'test-key' };

(async () => {
  // 1. A streamed JSON answer, split into tiny chunks (including inside a
  //    multi-byte character), is assembled, parsed and priced.
  {
    const fetch = fakeFetch([{ events: streamOf({ blocks: [{ type: 'thinking' }, { type: 'text', text: '{"headline":"Café gifts ✨","ok":true}' }], usage: { input_tokens: 1000000, cache_read_input_tokens: 1000000 }, outUsage: { output_tokens: 1000000 } }), split: 7 }]);
    const claude = C.createClaudeClient({ env, fetch, sleep: noSleep, log: quiet });
    const out = await claude.json({ system: 'Brites test', prompt: 'Write it', maxTokens: 4000, effort: 'minimal' });
    assert.deepEqual(out.data, { headline: 'Café gifts ✨', ok: true });
    const { init, body, url } = fetch.calls[0];
    assert.equal(url, 'https://api.anthropic.com/v1/messages');
    assert.equal(init.headers['x-api-key'], 'test-key');
    assert.equal(init.headers['anthropic-version'], '2023-06-01');
    assert.equal(body.model, 'claude-sonnet-5-5');
    assert.equal(body.stream, true);
    assert.deepEqual(body.thinking, { type: 'adaptive' });
    assert.equal(body.output_config.effort, 'low', 'minimal maps to low');
    assert.equal(body.max_tokens, 16000, 'thinking needs room: never below 16k');
    assert.equal(body.temperature, undefined);
    assert.match(body.system, /Brites test/);
    assert.equal(out.costUsd, 2 + 0.2 + 10, 'input $2 + cache read $0.20 + output $10 per million');
  }

  // 2. Structured output: unsupported bounds leave the schema and become rules.
  {
    const schema = { type: 'object', properties: { headlines: { type: 'array', minItems: 3, maxItems: 15, items: { type: 'string', maxLength: 30 } }, score: { type: 'number', minimum: 0, maximum: 100 }, nested: { type: 'object', properties: { a: { type: 'string', format: 'color' } }, required: ['a'] } }, required: ['headlines', 'score', 'nested'] };
    const fetch = fakeFetch([{ events: streamOf({ blocks: [{ type: 'text', text: '{"headlines":["a","b","c"],"score":5,"nested":{"a":"x"}}' }] }) }]);
    const claude = C.createClaudeClient({ env, fetch, sleep: noSleep, log: quiet });
    const out = await claude.json({ prompt: 'x', schema });
    assert.equal(out.data.score, 5);
    const sent = fetch.calls[0].body.output_config.format;
    assert.equal(sent.type, 'json_schema');
    assert.equal(sent.schema.additionalProperties, false);
    assert.equal(sent.schema.properties.nested.additionalProperties, false);
    assert.equal(sent.schema.properties.headlines.minItems, undefined);
    assert.equal(sent.schema.properties.headlines.items.maxLength, undefined);
    assert.equal(sent.schema.properties.score.maximum, undefined);
    assert.equal(sent.schema.properties.nested.properties.a.format, undefined);
    const system = fetch.calls[0].body.system;
    assert.match(system, /headlines: 3 to 15 items/);
    assert.match(system, /headlines\[\]: at most 30 characters/);
    assert.match(system, /score: minimum 0, maximum 100/);
    assert.equal(schema.properties.headlines.minItems, 3, 'caller schema is not mutated');
  }

  // 3. Overload (529) and a dropped stream are retried; a 400 is not.
  {
    const fetch = fakeFetch([
      { status: 529, errorType: 'overloaded_error', message: 'Overloaded' },
      { events: streamOf({ blocks: [{ type: 'text', text: '{"a":' }], cut: true }) },
      { events: [...streamOf({ blocks: [] }).slice(0, 1), { type: 'error', error: { type: 'overloaded_error', message: 'Overloaded' } }] },
      { events: streamOf({ blocks: [{ type: 'text', text: '{"a":1}' }] }) }
    ]);
    const waits = [];
    const claude = C.createClaudeClient({ env, fetch, sleep: async ms => { waits.push(ms); }, log: quiet });
    const out = await claude.json({ prompt: 'x' });
    assert.deepEqual(out.data, { a: 1 });
    assert.equal(fetch.calls.length, 4);
    assert.equal(waits.length, 3);
    const bad = fakeFetch([{ status: 400, message: 'max_tokens: too large' }]);
    const claude2 = C.createClaudeClient({ env, fetch: bad, sleep: noSleep, log: quiet });
    await assert.rejects(claude2.json({ prompt: 'x' }), e => e.status === 400 && e.definiteResponse === true && /too large/.test(e.message));
    assert.equal(bad.calls.length, 1);
    const limited = fakeFetch([{ status: 429, headers: { 'retry-after': '3' }, errorType: 'rate_limit_error', message: 'rate limited' }, { events: streamOf({ blocks: [{ type: 'text', text: '{"b":2}' }] }) }]);
    const seen = [];
    await C.createClaudeClient({ env, fetch: limited, sleep: async ms => seen.push(ms), log: quiet }).json({ prompt: 'x' });
    assert.deepEqual(seen, [3000], 'retry-after is honoured');
  }

  // 4. No key: nothing is sent.
  {
    const fetch = fakeFetch([]);
    const claude = C.createClaudeClient({ env: {}, fetch, sleep: noSleep, log: quiet });
    await assert.rejects(claude.json({ prompt: 'x' }), e => e.code === 'CLAUDE_NOT_CONFIGURED' && e.notDispatched === true);
    assert.equal(fetch.calls.length, 0);
  }

  // 5. A refusal is reported, never parsed as an answer.
  {
    const fetch = fakeFetch([{ events: streamOf({ blocks: [], stop: 'refusal', stopDetails: { type: 'refusal', category: 'general_harms' } }) }]);
    const claude = C.createClaudeClient({ env, fetch, sleep: noSleep, log: quiet });
    await assert.rejects(claude.json({ prompt: 'x' }), e => e.code === 'CLAUDE_REFUSAL' && /general_harms/.test(e.message));
  }

  // 6. Truncated JSON (max_tokens) is asked once more with double room and lower effort.
  {
    const fetch = fakeFetch([
      { events: streamOf({ blocks: [{ type: 'text', text: '{"list":[1,2' }], stop: 'max_tokens' }) },
      { events: streamOf({ blocks: [{ type: 'text', text: '{"list":[1,2,3]}' }] }) }
    ]);
    const claude = C.createClaudeClient({ env, fetch, sleep: noSleep, log: quiet });
    const out = await claude.json({ prompt: 'x', maxTokens: 20000, effort: 'high' });
    assert.deepEqual(out.data, { list: [1, 2, 3] });
    assert.equal(fetch.calls[1].body.max_tokens, 40000);
    assert.equal(fetch.calls[1].body.output_config.effort, 'medium');
  }

  // 7. Web search: tool shape, pause_turn resumed, JSON read after the last result, sources kept.
  {
    const result = { type: 'web_search_tool_result', tool_use_id: 'srv_1', content: [{ type: 'web_search_result', url: 'https://example.org/mothers-day-2027', title: "Mother's Day 2027", page_age: '2026-09-01' }] };
    const fetch = fakeFetch([
      { events: streamOf({ id: 'msg_a', blocks: [{ type: 'text', text: 'Checking dates.' }, { type: 'server_tool_use', id: 'srv_1', query: "mother's day 2027 date" }, result], stop: 'pause_turn', outUsage: { server_tool_use: { web_search_requests: 1 } } }) },
      { events: streamOf({ id: 'msg_b', blocks: [{ type: 'text', text: 'Here is the answer: {"date":"2027-05-09",', citations: [{ type: 'web_search_result_location', url: 'https://example.org/mothers-day-2027', title: "Mother's Day 2027", cited_text: 'May 9' }] }, { type: 'text', text: '"ok":true}' }], outUsage: { server_tool_use: { web_search_requests: 1 } } }) }
    ]);
    const claude = C.createClaudeClient({ env, fetch, sleep: noSleep, log: quiet });
    const out = await claude.json({ prompt: 'When is it?', webSearch: { maxUses: 4, userLocation: { country: 'US' } }, schema: { type: 'object', properties: { date: { type: 'string' } } } });
    assert.deepEqual(out.data, { date: '2027-05-09', ok: true });
    const first = fetch.calls[0].body;
    assert.deepEqual(first.tools, [{ type: 'web_search_20260209', name: 'web_search', max_uses: 4, user_location: { type: 'approximate', country: 'US' } }]);
    assert.equal(first.output_config.format, undefined, 'structured output is not combined with cited search results');
    assert.match(first.system, /JSON schema/);
    const second = fetch.calls[1].body;
    assert.equal(second.messages.length, 2);
    assert.equal(second.messages[1].role, 'assistant');
    assert.equal(second.messages[1].content[1].type, 'server_tool_use');
    assert.deepEqual(second.messages[1].content[1].input, { query: "mother's day 2027 date" });
    assert.equal(out.message.content.length, 5, 'both halves of the paused turn are kept');
    assert.equal(out.usage.server_tool_use.web_search_requests, 2);
    assert.equal(out.sources[0].url, 'https://example.org/mothers-day-2027');
    assert.ok(out.costUsd >= 0.02, 'two searches cost at least $0.02');
  }

  // 8. The OpenAI Responses bridge used by Ad Design.
  {
    const png = 'data:image/png;base64,' + Buffer.from('fake-png').toString('base64');
    const request = {
      model: 'gpt-6-astra', store: false, background: true, reasoning: { effort: 'high' }, max_output_tokens: 12000,
      input: [
        { role: 'developer', content: 'You are the Brites creative director.' },
        { role: 'user', content: [{ type: 'input_text', text: 'Design this.' }, { type: 'input_image', image_url: png, detail: 'high' }, { type: 'input_text', text: '' }, { type: 'input_image', image_url: 'https://cdn.shopify.com/a.jpg', detail: 'low' }] },
        { role: 'user', content: 'Second note.' }
      ],
      text: { format: { type: 'json_schema', name: 'brites_design', strict: true, schema: { type: 'object', additionalProperties: false, properties: { scenes: { type: 'array', minItems: 5, maxItems: 6, items: { type: 'string' } } }, required: ['scenes'] } } }
    };
    const body = C.fromResponsesRequest(request);
    assert.equal(body.model, 'claude-sonnet-5-5');
    assert.equal(body.max_tokens, 18000);
    assert.equal(body.output_config.effort, 'high');
    assert.match(body.system, /creative director/);
    assert.match(body.system, /scenes: 5 to 6 items/);
    assert.equal(body.messages.length, 1, 'consecutive user turns merge');
    assert.deepEqual(body.messages[0].content.map(b => b.type), ['text', 'image', 'image', 'text']);
    assert.deepEqual(body.messages[0].content[1].source, { type: 'base64', media_type: 'image/png', data: Buffer.from('fake-png').toString('base64') });
    assert.deepEqual(body.messages[0].content[2].source, { type: 'url', url: 'https://cdn.shopify.com/a.jpg' });
    assert.equal(body.background, undefined);
    assert.equal(body.store, undefined);
    assert.equal(body.output_config.format.schema.properties.scenes.minItems, undefined);

    const fetch = fakeFetch([{ events: streamOf({ blocks: [{ type: 'thinking' }, { type: 'text', text: '{"scenes":["a","b","c","d","e"]}' }], usage: { input_tokens: 5000, cache_read_input_tokens: 1000 } }) }]);
    const claude = C.createClaudeClient({ env, fetch, sleep: noSleep, log: quiet });
    const res = await claude.responses(request);
    assert.equal(res.status, 'completed');
    assert.equal(res.model, 'claude-sonnet-5-5');
    assert.deepEqual(JSON.parse(res.output_text), { scenes: ['a', 'b', 'c', 'd', 'e'] });
    assert.equal(res.output[0].content[0].type, 'output_text');
    assert.equal(res.usage.input_tokens, 6000);
    assert.equal(res.usage.input_tokens_details.cached_tokens, 1000);
    assert.equal(res.usage.output_tokens, 50);
    assert.equal(res.estimatedUsd, (5000 * 2 + 1000 * 0.2 + 50 * 10) / 1e6);

    const cut = C.toResponsesResult({ id: 'msg_x', model: 'claude-sonnet-5-5', stop_reason: 'max_tokens', content: [{ type: 'text', text: '{"a":' }], usage: { input_tokens: 1, output_tokens: 1 } });
    assert.equal(cut.status, 'incomplete');
    assert.deepEqual(cut.incomplete_details, { reason: 'max_output_tokens' });
    const refused = C.toResponsesResult({ id: 'msg_y', stop_reason: 'refusal', stop_details: { category: 'cyber' }, content: [], usage: {} });
    assert.equal(refused.output[0].content[0].type, 'refusal');
    assert.equal(refused.output_text, '');
  }

  // 9. Images Claude cannot read fail before any request.
  {
    assert.throws(() => C.imageBlock('data:image/svg+xml;base64,PHN2Zz4='), e => e.code === 'CLAUDE_IMAGE_TYPE');
    assert.throws(() => C.imageBlock('data:image/jpeg;base64,' + 'A'.repeat(5 * 1024 * 1024 + 4)), e => e.code === 'CLAUDE_IMAGE_TOO_LARGE');
    assert.throws(() => C.imageBlock('http://insecure.example/a.jpg'), e => e.code === 'CLAUDE_IMAGE_SOURCE');
    assert.equal(C.imageBlock('data:image/jpg;base64,QUJD').source.media_type, 'image/jpeg');
  }

  // 10. Pricing and effort helpers.
  {
    assert.equal(C.estimateCostUsd({ input_tokens: 1e6 }), 2);
    assert.equal(C.estimateCostUsd({ output_tokens: 1e6 }), 10);
    assert.equal(C.estimateCostUsd({ cache_creation_input_tokens: 1e6 }), 2.5);
    assert.equal(C.estimateCostUsd({ cache_creation_input_tokens: 1e6, cache_creation: { ephemeral_1h_input_tokens: 1e6, ephemeral_5m_input_tokens: 0 } }), 4);
    assert.equal(C.estimateCostUsd({ server_tool_use: { web_search_requests: 3 } }), 0.03);
    assert.equal(C.estimateCostUsd(null), null);
    assert.equal(C.effortFor('xhigh'), 'xhigh');
    assert.equal(C.effortFor('bogus'), 'high');
    assert.equal(C.MODEL, 'claude-sonnet-5-5');
  }

  // 11. retryUnknown:false (paid analysis): a rejected request (error status,
  //     nothing generated) is retried; a stream that fails after the request was
  //     accepted is never sent again, because it may already be billed.
  {
    const rejectedFirst = fakeFetch([{ status: 529, errorType: 'overloaded_error', message: 'Overloaded' }, { events: streamOf({ blocks: [{ type: 'text', text: '{"ok":true}' }] }) }]);
    const out = await C.createClaudeClient({ env, fetch: rejectedFirst, sleep: noSleep, log: quiet }).json({ prompt: 'x', retryUnknown: false });
    assert.deepEqual(out.data, { ok: true });
    assert.equal(rejectedFirst.calls.length, 2, 'an overloaded rejection is safe to retry');
    const cut = fakeFetch([{ events: streamOf({ blocks: [{ type: 'text', text: '{"a":' }], cut: true }) }, { events: streamOf({ blocks: [{ type: 'text', text: '{"a":1}' }] }) }]);
    await assert.rejects(C.createClaudeClient({ env, fetch: cut, sleep: noSleep, log: quiet }).message({ messages: [{ role: 'user', content: 'x' }] }, { retryUnknown: false }), e => e.code === 'CLAUDE_STREAM_CUT' && !e.rejected && !e.definiteResponse);
    assert.equal(cut.calls.length, 1, 'a possibly billed stream is not repeated');
    const midStream = fakeFetch([{ events: [...streamOf({ blocks: [] }).slice(0, 1), { type: 'error', error: { type: 'overloaded_error', message: 'Overloaded' } }] }, { events: streamOf({ blocks: [] }) }]);
    await assert.rejects(C.createClaudeClient({ env, fetch: midStream, sleep: noSleep, log: quiet }).message({ messages: [{ role: 'user', content: 'x' }] }, { retryUnknown: false }), e => /stream error/.test(e.message) && !e.rejected);
    assert.equal(midStream.calls.length, 1);
    const bad = fakeFetch([{ status: 400, message: 'bad image' }]);
    await assert.rejects(C.createClaudeClient({ env, fetch: bad, sleep: noSleep, log: quiet }).json({ prompt: 'x', retryUnknown: false }), e => e.rejected === true && e.definiteResponse === true);
  }

  // 12. Unparseable JSON keeps the text and the web sources it read, for salvage.
  {
    const result = { type: 'web_search_tool_result', tool_use_id: 'srv_1', content: [{ type: 'web_search_result', url: 'https://example.org/holidays', title: 'Holidays' }] };
    const fetch = fakeFetch([{ events: streamOf({ blocks: [{ type: 'server_tool_use', id: 'srv_1', query: 'holidays' }, result, { type: 'text', text: 'No JSON here.' }], outUsage: { server_tool_use: { web_search_requests: 1 } } }) }]);
    await assert.rejects(C.createClaudeClient({ env, fetch, sleep: noSleep, log: quiet }).json({ prompt: 'x', webSearch: true }), e => e.code === 'CLAUDE_BAD_JSON' && e.text === 'No JSON here.' && e.sources[0].url === 'https://example.org/holidays');
  }

  // 13. Many search results: the page the answer cites is listed first and never
  //     cut by the cap; a truncation retry reports the searches of both passes.
  {
    const results = Array.from({ length: 70 }, (_, i) => ({ type: 'web_search_result', url: 'https://example.org/page-' + i, title: 'Page ' + i, page_age: i === 65 ? '2026-09-20' : null }));
    const msg = { content: [{ type: 'web_search_tool_result', content: results }, { type: 'text', text: '{"date":"2026-10-05"}', citations: [{ type: 'web_search_result_location', url: 'https://example.org/page-65', title: 'Page 65', cited_text: 'October 5' }] }] };
    const sources = C.sourcesOf(msg);
    assert.equal(sources.length, 60);
    assert.deepEqual(sources[0], { url: 'https://example.org/page-65', title: 'Page 65', pageAge: '2026-09-20', cited: true });
    assert.equal(sources.filter(s => s.url === 'https://example.org/page-65').length, 1);
    const search = { type: 'web_search_tool_result', tool_use_id: 'srv_1', content: [{ type: 'web_search_result', url: 'https://example.org/a', title: 'A' }] };
    const fetch = fakeFetch([
      { events: streamOf({ blocks: [{ type: 'server_tool_use', id: 'srv_1', query: 'a' }, search, { type: 'text', text: '{"a":' }], stop: 'max_tokens', outUsage: { server_tool_use: { web_search_requests: 1 } } }) },
      { events: streamOf({ blocks: [{ type: 'server_tool_use', id: 'srv_1', query: 'a' }, search, { type: 'text', text: '{"a":1}' }], outUsage: { server_tool_use: { web_search_requests: 1 } } }) }
    ]);
    const out = await C.createClaudeClient({ env, fetch, sleep: noSleep, log: quiet }).json({ prompt: 'x', webSearch: true });
    assert.deepEqual(out.data, { a: 1 });
    assert.equal(out.usage.server_tool_use.web_search_requests, 2, 'both passes searched');
    assert.equal(out.usage.input_tokens, 200);
    assert.equal(out.costUsd, C.estimateCostUsd(out.usage), 'reported usage and cost agree');
  }

  // 14. Every client above was given its fetch, so node-fetch was never loaded. A
  //     module-level load would also bypass other suites' per-module fetch stubs,
  //     because Node caches a package's resolution per directory.
  assert.ok(!Object.keys(require.cache).some(k => /[\\/]node-fetch[\\/]/.test(k)), 'an injected fetch never loads node-fetch');

  console.log('claude-client: Sonnet 5.5 client streams, retries, prices and bridges offline.');
  require('./suite-guard.cjs').done();
})().catch(error => { console.error(error); process.exit(1); });
