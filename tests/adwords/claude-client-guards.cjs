// Offline guards for the shared Sonnet 5.5 client: spend totals that survive
// null counters, a request that never answers, and timeoutMs as the bound on a
// whole call. Fakes and a local HTTP server only; nothing reaches Anthropic.
const assert = require('node:assert/strict');
const http = require('node:http');
const { Readable, PassThrough } = require('node:stream');
const nodeFetch = require('node-fetch');
const C = require('../../netlify/functions/_googleAdsClaude.js');

const env = { ANTHROPIC_API_KEY: 'test-key' };
const quiet = () => {};
const noSleep = async () => {};
const ask = { messages: [{ role: 'user', content: 'x' }] };
const sse = events => events.map(e => `event: ${e.type}\ndata: ${JSON.stringify(e)}\n\n`).join('');
const start = usage => ({ type: 'message_start', message: { id: 'msg_1', type: 'message', role: 'assistant', model: 'claude-sonnet-5-5', content: [], stop_reason: null, usage: { input_tokens: 100, output_tokens: 1, ...usage } } });
const text = t => [{ type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } }, { type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: t } }, { type: 'content_block_stop', index: 0 }];
const end = (usage, stop = 'end_turn') => [{ type: 'message_delta', delta: { stop_reason: stop, stop_sequence: null }, usage }, { type: 'message_stop' }];
const ok = body => ({ ok: true, status: 200, headers: { get: () => null }, body });
const abortError = () => Object.assign(new Error('The user aborted a request.'), { name: 'AbortError', type: 'aborted' });
// node-fetch v2 on abort: the pending request rejects, an open body is destroyed.
const neverAnswers = init => new Promise((resolve, reject) => init.signal.addEventListener('abort', () => reject(abortError())));
function slowStream(init, events, endAfterMs) {
  const s = new PassThrough();
  s.write(sse(events));
  if (endAfterMs != null) setTimeout(() => s.end(), endAfterMs);
  init.signal.addEventListener('abort', () => s.destroy(abortError()));
  return ok(s);
}
function fakeFetch(replies) {
  const calls = [];
  const fn = async (url, init) => { calls.push(JSON.parse(init.body)); const next = replies.shift(); if (!next) throw new Error('unexpected extra request'); return next(init); };
  fn.calls = calls;
  return fn;
}
const within = (promise, ms) => { let timer; return Promise.race([promise.then(() => 'resolved', e => e), new Promise(r => { timer = setTimeout(() => r('still waiting after ' + ms + ' ms'), ms); })]).finally(() => clearTimeout(timer)); };

(async () => {
  // 1. The final message_delta may carry null counters (MessageDeltaUsage types
  //    input and cache tokens as number | null). A null must not erase the
  //    input already counted, or the spend is reported far too low.
  {
    const events = [start({ input_tokens: 1000000, cache_read_input_tokens: 1000000, cache_creation_input_tokens: 0 }), ...text('{"a":1}'),
      ...end({ output_tokens: 1000000, input_tokens: null, cache_read_input_tokens: null, cache_creation_input_tokens: null, server_tool_use: null })];
    const fetch = fakeFetch([() => ok(Readable.from([Buffer.from(sse(events))]))]);
    const out = await C.createClaudeClient({ env, fetch, sleep: noSleep, log: quiet }).json({ prompt: 'x' });
    assert.deepEqual(out.data, { a: 1 });
    assert.equal(out.usage.input_tokens, 1000000, 'input counted at message_start is kept');
    assert.equal(out.usage.cache_read_input_tokens, 1000000);
    assert.equal(out.costUsd, 2 + 0.2 + 10, 'input $2 + cache read $0.20 + output $10');
  }

  // 2. A request that never gets response headers is idle too: idleMs ends it
  //    (real node-fetch against a local server that accepts and never answers).
  {
    const server = http.createServer(req => { req.resume(); });
    await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
    const local = 'http://127.0.0.1:' + server.address().port + '/v1/messages';
    const claude = C.createClaudeClient({ env, fetch: (url, init) => nodeFetch(local, init), sleep: noSleep, log: quiet });
    const t0 = Date.now();
    const outcome = await within(claude.message(ask, { idleMs: 300, retries: 0 }), 3000);
    const took = Date.now() - t0;
    if (server.closeAllConnections) server.closeAllConnections();
    server.close();
    assert.ok(outcome instanceof Error, 'a silent server must not hold the call open: ' + outcome);
    assert.equal(outcome.code, 'CLAUDE_IDLE');
    assert.ok(took < 2000, 'ended by idleMs, not by chance (' + took + ' ms)');
    // With a retry left, the next attempt answers.
    const fetch = fakeFetch([neverAnswers, () => ok(Readable.from([Buffer.from(sse([start(), ...text('{"b":2}'), ...end({ output_tokens: 5 })]))]))]);
    const out = await C.createClaudeClient({ env, fetch, sleep: noSleep, log: quiet }).json({ prompt: 'x', idleMs: 200, retries: 1 });
    assert.deepEqual(out.data, { b: 2 });
    assert.equal(fetch.calls.length, 2);
  }

  // 3. timeoutMs bounds the whole call: a retry gets only the time that is left.
  {
    const slowReject = () => new Promise(resolve => setTimeout(() => resolve({ ok: false, status: 529, statusText: 'err', headers: { get: k => (String(k).toLowerCase() === 'retry-after' ? '0' : null) }, text: async () => '{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}' }), 800));
    const fetch = fakeFetch([slowReject, init => slowStream(init, [start()])]);
    const t0 = Date.now();
    await assert.rejects(C.createClaudeClient({ env, fetch, sleep: noSleep, log: quiet }).message(ask, { timeoutMs: 2600, idleMs: 0 }), e => e.code === 'CLAUDE_TIMEOUT');
    const took = Date.now() - t0;
    assert.equal(fetch.calls.length, 2);
    assert.ok(took < 3000, 'timeoutMs 2600 includes the retry, not 800 + 2600 ms (' + took + ' ms)');
  }

  // 4. No retry is sent without time to finish before the deadline; the last
  //    error is reported as it was.
  {
    const cut = init => slowStream(init, [start(), ...text('{"c":')], 300);
    const fetch = fakeFetch([cut, cut, cut, cut, cut]);
    const t0 = Date.now();
    await assert.rejects(C.createClaudeClient({ env, fetch, sleep: noSleep, log: quiet }).message(ask, { timeoutMs: 700, idleMs: 0 }), e => e.code === 'CLAUDE_STREAM_CUT');
    const took = Date.now() - t0;
    assert.equal(fetch.calls.length, 1, 'no paid retry that cannot finish');
    assert.ok(took < 1000, 'cut attempts do not run past a 700 ms limit (' + took + ' ms)');
  }

  // 5. A pause_turn continuation shares the same deadline, and none is sent
  //    without time to finish; the paid first turn stays on the error.
  {
    const paused = init => slowStream(init, [start({ input_tokens: 1000000 }), ...text('Searching.'), ...end({ output_tokens: 5 }, 'pause_turn')], 800);
    let fetch = fakeFetch([paused, init => slowStream(init, [start()])]);
    let t0 = Date.now();
    await assert.rejects(C.createClaudeClient({ env, fetch, sleep: noSleep, log: quiet }).message(ask, { timeoutMs: 2600, idleMs: 0 }), e => e.code === 'CLAUDE_TIMEOUT');
    let took = Date.now() - t0;
    assert.equal(fetch.calls.length, 2);
    assert.ok(took < 3000, 'the continuation gets the time left, not a new 2600 ms (' + took + ' ms)');
    fetch = fakeFetch([paused, init => slowStream(init, [start()])]);
    t0 = Date.now();
    await assert.rejects(C.createClaudeClient({ env, fetch, sleep: noSleep, log: quiet }).message(ask, { timeoutMs: 1200, idleMs: 0 }), e => e.code === 'CLAUDE_TIMEOUT' && e.costUsd >= 2);
    took = Date.now() - t0;
    assert.equal(fetch.calls.length, 1, 'no continuation with under a second left');
    assert.ok(took < 1500, 'stopped when the first turn ended, not 800 + 1200 ms (' + took + ' ms)');
  }

  console.log('claude-client-guards: spend survives null counters; silent servers and slow retries stay inside their limits.');
  require('./suite-guard.cjs').done();
})().catch(error => { console.error(error); process.exit(1); });
