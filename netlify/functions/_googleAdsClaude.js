// Shared Claude client for every Brites Google Ads AI call: opportunity
// research, ad copy, reviews, analysis and the Ad Design text steps.
// Owner, 2026-09-29: every AI in the Adwords console runs on Claude Sonnet 5.5.
// Image and video generation stay with their own providers because Claude
// reads images but does not draw or film.
//
// Raw HTTPS with node-fetch, the same as _etsyMailAnthropic.js (the repo does
// not ship the Anthropic SDK). Requests always stream, so a long answer never
// trips an idle-connection timeout; the stream is assembled into the same
// message object a non-streaming call returns.
const nodeFetch = require('node-fetch');
const { StringDecoder } = require('string_decoder');

const MODEL = 'claude-sonnet-5-5';
const MODEL_LABEL = 'Claude Sonnet 5.5';
const API_URL = 'https://api.anthropic.com/v1/messages';
const API_VERSION = '2023-06-01';
const MAX_OUTPUT_TOKENS = 128000;
// List prices in USD per million tokens, and per web search.
const PRICE = Object.freeze({ input: 2, output: 10, cacheRead: 0.2, cacheWrite: 2.5, cacheWrite1h: 4, search: 0.01 });
const EFFORTS = ['low', 'medium', 'high', 'xhigh', 'max'];
const RETRY_STATUS = [429, 500, 502, 503, 504, 529];
const RETRY_BASE_MS = 1250, RETRY_MAX_MS = 12000;
const IMAGE_TYPES = ['image/jpeg', 'image/png', 'image/gif', 'image/webp'];
const MAX_IMAGE_BASE64 = 5 * 1024 * 1024;

function claudeError(message, fields) { return Object.assign(new Error(message), fields || {}); }
function effortFor(value) {
  const v = String(value || '').toLowerCase();
  if (v === 'minimal' || v === 'none') return 'low';
  return EFFORTS.includes(v) ? v : 'high';
}
function lowerEffort(value) { const i = EFFORTS.indexOf(effortFor(value)); return EFFORTS[Math.max(0, i - 1)]; }
function clampTokens(n, floor = 1024) { const v = Math.round(Number(n) || 0); return Math.min(MAX_OUTPUT_TOKENS, Math.max(floor, v)); }
function isRetryable(status, message) {
  if (RETRY_STATUS.includes(Number(status))) return true;
  return /overloaded|rate.?limit|too many requests|econnreset|econnrefused|etimedout|enotfound|eai_again|socket hang up|network error|fetch failed|failed, reason:|premature close|aborted/i.test(String(message || ''));
}
function retryDelayMs(attempt, retryAfter) {
  const seconds = Number(retryAfter);
  if (retryAfter != null && Number.isFinite(seconds) && seconds >= 0) return Math.min(60000, seconds * 1000);
  return Math.min(RETRY_BASE_MS * Math.pow(2, Math.max(0, attempt - 1)), RETRY_MAX_MS) + Math.floor(Math.random() * 700);
}

// ---------------------------------------------------------------- usage/cost
function zeroUsage() { return { input_tokens: 0, output_tokens: 0, cache_read_input_tokens: 0, cache_creation_input_tokens: 0, cache_creation: { ephemeral_5m_input_tokens: 0, ephemeral_1h_input_tokens: 0 }, server_tool_use: { web_search_requests: 0, web_fetch_requests: 0 } }; }
const count = v => (Number.isFinite(Number(v)) && Number(v) > 0 ? Number(v) : 0);
function addUsage(total, usage) {
  if (!usage) return total;
  for (const key of ['input_tokens', 'output_tokens', 'cache_read_input_tokens', 'cache_creation_input_tokens']) total[key] += count(usage[key]);
  const cc = usage.cache_creation || {}, oneHour = count(cc.ephemeral_1h_input_tokens);
  total.cache_creation.ephemeral_1h_input_tokens += oneHour;
  total.cache_creation.ephemeral_5m_input_tokens += cc.ephemeral_5m_input_tokens != null ? count(cc.ephemeral_5m_input_tokens) : Math.max(0, count(usage.cache_creation_input_tokens) - oneHour);
  const tools = usage.server_tool_use || {};
  total.server_tool_use.web_search_requests += count(tools.web_search_requests);
  total.server_tool_use.web_fetch_requests += count(tools.web_fetch_requests);
  return total;
}
// Estimated USD for one Sonnet 5.5 usage object (list price; the invoice is final).
function estimateCostUsd(usage) {
  if (!usage || typeof usage !== 'object') return null;
  const u = addUsage(zeroUsage(), usage);
  const usd = (u.input_tokens * PRICE.input + u.output_tokens * PRICE.output + u.cache_read_input_tokens * PRICE.cacheRead +
    u.cache_creation.ephemeral_5m_input_tokens * PRICE.cacheWrite + u.cache_creation.ephemeral_1h_input_tokens * PRICE.cacheWrite1h) / 1e6 +
    u.server_tool_use.web_search_requests * PRICE.search;
  return Math.round(usd * 1e6) / 1e6;
}

// ------------------------------------------------------------ JSON schemas
// Structured outputs accept a subset of JSON Schema: no length, size or number
// bounds, and every object closed. Strip what the API rejects and hand the
// bounds to the model as plain rules instead, so they still shape the answer.
const DROP_KEYS = ['minLength', 'maxLength', 'minimum', 'maximum', 'exclusiveMinimum', 'exclusiveMaximum', 'multipleOf', 'pattern', 'minItems', 'maxItems', 'uniqueItems', 'minProperties', 'maxProperties', 'contains', 'minContains', 'maxContains', 'patternProperties', 'propertyNames', 'unevaluatedProperties', 'dependentRequired', 'dependentSchemas', 'if', 'then', 'else', 'not', 'default', 'examples', '$schema', '$id', 'title', 'strict', 'name'];
const FORMATS = ['date-time', 'time', 'date', 'duration', 'email', 'hostname', 'uri', 'ipv4', 'ipv6', 'uuid'];
function sanitizeSchema(schema) {
  const notes = [];
  function describe(path, s) {
    const where = path || 'the answer', rules = [];
    if (s.minLength != null && s.maxLength != null) rules.push(`${s.minLength} to ${s.maxLength} characters`);
    else if (s.maxLength != null) rules.push(`at most ${s.maxLength} characters`);
    else if (s.minLength != null) rules.push(`at least ${s.minLength} characters`);
    if (s.minItems != null && s.maxItems != null) rules.push(s.minItems === s.maxItems ? `exactly ${s.minItems} items` : `${s.minItems} to ${s.maxItems} items`);
    else if (s.maxItems != null) rules.push(`at most ${s.maxItems} items`);
    else if (s.minItems != null && s.minItems > 0) rules.push(`at least ${s.minItems} items`);
    if (s.minimum != null) rules.push(`minimum ${s.minimum}`);
    if (s.maximum != null) rules.push(`maximum ${s.maximum}`);
    if (s.exclusiveMinimum != null) rules.push(`greater than ${s.exclusiveMinimum}`);
    if (s.exclusiveMaximum != null) rules.push(`less than ${s.exclusiveMaximum}`);
    if (s.multipleOf != null) rules.push(`a multiple of ${s.multipleOf}`);
    if (s.pattern != null) rules.push(`matching the pattern ${s.pattern}`);
    if (s.uniqueItems) rules.push('no repeated items');
    if (typeof s.format === 'string' && !FORMATS.includes(s.format)) rules.push(`in ${s.format} format`);
    if (rules.length) notes.push(`${where}: ${rules.join(', ')}`);
  }
  function walk(s, path) {
    if (Array.isArray(s)) return s.map((v, i) => walk(v, path));
    if (!s || typeof s !== 'object') return s;
    describe(path, s);
    const out = {};
    for (const [key, value] of Object.entries(s)) {
      if (DROP_KEYS.includes(key)) continue;
      if (key === 'format' && (typeof value !== 'string' || !FORMATS.includes(value))) continue;
      if (key === 'properties' && value && typeof value === 'object') { out.properties = {}; for (const [k, v] of Object.entries(value)) out.properties[k] = walk(v, path ? `${path}.${k}` : k); continue; }
      if (key === 'items') { out.items = walk(value, `${path || 'items'}[]`); continue; }
      if (key === 'oneOf') { out.anyOf = walk(value, path); continue; }
      if (key === '$defs' || key === 'definitions') { out[key] = {}; for (const [k, v] of Object.entries(value || {})) out[key][k] = walk(v, `${k}`); continue; }
      out[key] = typeof value === 'object' ? walk(value, path) : value;
    }
    const isObject = out.type === 'object' || (Array.isArray(out.type) && out.type.includes('object')) || (out.properties && !out.type);
    if (isObject) out.additionalProperties = false;
    return out;
  }
  return { schema: walk(schema, ''), notes };
}
function rulesNote(notes) {
  return notes.length ? `Output rules the JSON schema cannot express. Follow each one exactly:\n${notes.slice(0, 80).map(n => '- ' + n).join('\n')}` : '';
}

// --------------------------------------------------------------- content
// An OpenAI-style image reference (data URL or https URL) as a Claude image block.
function imageBlock(src) {
  const url = typeof src === 'string' ? src : src && (src.url || src.image_url);
  const data = /^data:(image\/[a-z+.-]+);base64,([\s\S]+)$/i.exec(String(url || ''));
  if (data) {
    const mediaType = data[1].toLowerCase() === 'image/jpg' ? 'image/jpeg' : data[1].toLowerCase();
    if (!IMAGE_TYPES.includes(mediaType)) throw claudeError(`Claude reads JPEG, PNG, GIF and WebP images, not ${mediaType}.`, { code: 'CLAUDE_IMAGE_TYPE', definiteResponse: true });
    const base64 = data[2].replace(/\s+/g, '');
    if (base64.length > MAX_IMAGE_BASE64) throw claudeError('An image is larger than the 5 MB Claude accepts. Resize it before sending.', { code: 'CLAUDE_IMAGE_TOO_LARGE', definiteResponse: true });
    return { type: 'image', source: { type: 'base64', media_type: mediaType, data: base64 } };
  }
  if (/^https:\/\//i.test(String(url || ''))) return { type: 'image', source: { type: 'url', url: String(url) } };
  throw claudeError('An image must be an https URL or a base64 data URL.', { code: 'CLAUDE_IMAGE_SOURCE', definiteResponse: true });
}
function textOf(content) {
  if (typeof content === 'string') return content;
  return (content || []).filter(p => p && ['input_text', 'output_text', 'text'].includes(p.type)).map(p => String(p.text || '')).join('\n\n');
}
function partsToBlocks(content) {
  if (typeof content === 'string') return content ? [{ type: 'text', text: content }] : [];
  const blocks = [];
  for (const part of content || []) {
    if (!part) continue;
    if (['input_text', 'output_text', 'text'].includes(part.type)) { if (String(part.text || '')) blocks.push({ type: 'text', text: String(part.text) }); }
    else if (part.type === 'input_image' || part.type === 'image_url') blocks.push(imageBlock(part.image_url));
    else if (part.type === 'image') blocks.push(part);
    else throw claudeError(`Claude cannot read a ${part.type} input part.`, { code: 'CLAUDE_INPUT_PART', definiteResponse: true });
  }
  return blocks;
}
function webSearchTool(options) {
  const o = options && typeof options === 'object' ? options : {};
  const tool = { type: 'web_search_20260209', name: 'web_search', max_uses: Math.max(1, Math.min(20, Math.round(Number(o.maxUses) || 5))) };
  if (Array.isArray(o.allowedDomains) && o.allowedDomains.length) tool.allowed_domains = o.allowedDomains.slice(0, 50);
  else if (Array.isArray(o.blockedDomains) && o.blockedDomains.length) tool.blocked_domains = o.blockedDomains.slice(0, 50);
  if (o.userLocation && typeof o.userLocation === 'object') tool.user_location = { type: 'approximate', ...o.userLocation };
  return tool;
}

// -------------------------------------------------- OpenAI Responses bridge
// The Ad Design pipeline builds OpenAI Responses requests and reads Responses
// results, and saved jobs keep that shape. These two functions let it run on
// Claude without rewriting every builder and reader.
function fromResponsesRequest(request) {
  const r = request || {};
  if (r.previous_response_id) throw claudeError('Claude does not continue a stored OpenAI response.', { code: 'CLAUDE_UNSUPPORTED', definiteResponse: true });
  const system = [], messages = [];
  if (r.instructions) system.push(String(r.instructions));
  const input = typeof r.input === 'string' ? [{ role: 'user', content: r.input }] : Array.isArray(r.input) ? r.input : [];
  for (const item of input) {
    if (!item) continue;
    if (item.role === 'developer' || item.role === 'system') { const t = textOf(item.content); if (t) system.push(t); continue; }
    const role = item.role === 'assistant' ? 'assistant' : 'user', blocks = partsToBlocks(item.content);
    if (!blocks.length) continue;
    const last = messages[messages.length - 1];
    if (last && last.role === role) last.content.push(...blocks); else messages.push({ role, content: blocks });
  }
  if (!messages.length || messages[0].role !== 'user') messages.unshift({ role: 'user', content: [{ type: 'text', text: 'Follow the instructions.' }] });
  const format = r.text && r.text.format, schemaOn = format && format.type === 'json_schema' && format.schema;
  const body = { model: MODEL, max_tokens: clampTokens(Math.max(16000, Math.round((Number(r.max_output_tokens) || 16000) * 1.5))), messages, thinking: { type: 'adaptive' }, output_config: { effort: effortFor(r.reasoning && r.reasoning.effort) } };
  const tools = (r.tools || []).filter(t => t && /^web_search/.test(t.type)).map(t => webSearchTool({ maxUses: t.max_uses, userLocation: t.user_location && { country: t.user_location.country, region: t.user_location.region, city: t.user_location.city, timezone: t.user_location.timezone } }));
  if (tools.length) body.tools = tools.slice(0, 1);
  if (schemaOn && !tools.length) {
    const clean = sanitizeSchema(format.schema);
    body.output_config.format = { type: 'json_schema', schema: clean.schema };
    if (clean.notes.length) system.push(rulesNote(clean.notes));
  } else if (format && (format.type === 'json_schema' || format.type === 'json_object')) {
    system.push('Return only one JSON object, with no prose or markdown around it.' + (schemaOn ? ' It must match this JSON schema: ' + JSON.stringify(format.schema) : ''));
  }
  if (system.length) body.system = system.join('\n\n');
  return body;
}
function finalText(message) {
  const blocks = (message && message.content) || [];
  let start = 0;
  blocks.forEach((b, i) => { if (b && (b.type === 'server_tool_use' || /_tool_result$/.test(String(b.type)))) start = i + 1; });
  const tail = blocks.slice(start).filter(b => b && b.type === 'text').map(b => b.text || '').join('');
  return tail.trim() ? tail : blocks.filter(b => b && b.type === 'text').map(b => b.text || '').join('');
}
function toResponsesResult(message, extra) {
  const m = message || {}, text = finalText(m), refusal = m.stop_reason === 'refusal';
  const incomplete = refusal || m.stop_reason === 'max_tokens' || m.stop_reason === 'pause_turn' || m.stop_reason === 'model_context_window_exceeded';
  const u = addUsage(zeroUsage(), m.usage), inputTotal = u.input_tokens + u.cache_read_input_tokens + u.cache_creation_input_tokens;
  const content = refusal ? [{ type: 'refusal', refusal: text || 'Claude declined this request.' }] : [{ type: 'output_text', text, annotations: [] }];
  return {
    id: m.id || null, object: 'response', provider: 'anthropic', model: m.model || MODEL,
    status: incomplete ? 'incomplete' : 'completed',
    incomplete_details: m.stop_reason === 'max_tokens' ? { reason: 'max_output_tokens' } : refusal ? { reason: 'content_filter', category: (m.stop_details && m.stop_details.category) || null } : incomplete ? { reason: m.stop_reason } : null,
    output: [{ type: 'message', id: m.id || null, status: incomplete ? 'incomplete' : 'completed', role: 'assistant', content }],
    output_text: refusal ? '' : text,
    usage: { input_tokens: inputTotal, input_tokens_details: { cached_tokens: u.cache_read_input_tokens }, output_tokens: u.output_tokens, output_tokens_details: { reasoning_tokens: 0 }, total_tokens: inputTotal + u.output_tokens },
    claudeUsage: u, stop_reason: m.stop_reason || null, estimatedUsd: estimateCostUsd(u),
    ...(extra || {})
  };
}

// ---------------------------------------------------------- JSON answers
function parseJsonText(text) {
  const cleaned = String(text || '').replace(/```(?:json)?/gi, '').trim();
  if (!cleaned) return undefined;
  try { return JSON.parse(cleaned); } catch (_) { /* fall through to the widest {...} or [...] span */ }
  for (const [open, close] of [['{', '}'], ['[', ']']]) {
    const a = cleaned.indexOf(open), b = cleaned.lastIndexOf(close);
    if (a >= 0 && b > a) { try { return JSON.parse(cleaned.slice(a, b + 1)); } catch (_) { /* try the next shape */ } }
  }
  return undefined;
}
function sourcesOf(message) {
  const seen = new Map();
  for (const b of (message && message.content) || []) {
    if (b && b.type === 'web_search_tool_result' && Array.isArray(b.content)) for (const r of b.content) if (r && r.url && !seen.has(r.url)) seen.set(r.url, { url: r.url, title: r.title || '', pageAge: r.page_age || null });
    if (b && b.type === 'text' && Array.isArray(b.citations)) for (const c of b.citations) if (c && c.url && !seen.has(c.url)) seen.set(c.url, { url: c.url, title: c.title || '', pageAge: null });
  }
  return [...seen.values()].slice(0, 40);
}

// ---------------------------------------------------------------- client
function createClaudeClient(deps) {
  const D = deps || {};
  const env = D.env || process.env, fetch = D.fetch || nodeFetch;
  const sleep = D.sleep || (ms => new Promise(resolve => setTimeout(resolve, ms)));
  const log = D.log || ((...args) => console.log(...args));

  async function readStream(res, { idleMs, deadline, controller }) {
    const decoder = new StringDecoder('utf8');
    let buffer = '', message = null, done = false, idle = null;
    const blocks = [];
    const arm = () => { if (!idleMs || !controller) return; clearTimeout(idle); idle = setTimeout(() => controller.abort(), idleMs); };
    const handle = evt => {
      switch (evt.type) {
        case 'message_start': message = { ...(evt.message || {}), content: [] }; message.usage = { ...((evt.message && evt.message.usage) || {}) }; break;
        case 'content_block_start': {
          const b = JSON.parse(JSON.stringify(evt.content_block || {}));
          if (b.type === 'tool_use' || b.type === 'server_tool_use') b._json = '';
          blocks[evt.index] = b; break;
        }
        case 'content_block_delta': {
          const b = blocks[evt.index], d = evt.delta || {};
          if (!b) break;
          if (d.type === 'text_delta') b.text = (b.text || '') + (d.text || '');
          else if (d.type === 'thinking_delta') b.thinking = (b.thinking || '') + (d.thinking || '');
          else if (d.type === 'signature_delta') b.signature = (b.signature || '') + (d.signature || '');
          else if (d.type === 'input_json_delta') b._json = (b._json || '') + (d.partial_json || '');
          else if (d.type === 'citations_delta' && d.citation) (b.citations = b.citations || []).push(d.citation);
          break;
        }
        case 'content_block_stop': {
          const b = blocks[evt.index];
          if (b && b._json !== undefined) { const raw = b._json; delete b._json; if (raw) { try { b.input = JSON.parse(raw); } catch (_) { b.input = {}; b.inputParseError = true; } } else if (b.input == null) b.input = {}; }
          break;
        }
        case 'message_delta':
          if (message) { Object.assign(message, evt.delta || {}); if (evt.usage) message.usage = { ...message.usage, ...evt.usage }; }
          break;
        case 'message_stop': done = true; break;
        case 'error': {
          const e = evt.error || {};
          throw claudeError('Claude stream error: ' + (e.message || e.type || 'unknown'), { type: e.type, status: e.type === 'overloaded_error' ? 529 : e.type === 'rate_limit_error' ? 429 : 500, retryable: ['overloaded_error', 'api_error', 'rate_limit_error'].includes(e.type) });
        }
        default: break;
      }
    };
    const flush = chunk => {
      buffer += chunk;
      let cut;
      while ((cut = buffer.search(/\r?\n\r?\n/)) >= 0) {
        const raw = buffer.slice(0, cut);
        buffer = buffer.slice(cut).replace(/^\r?\n\r?\n/, '');
        const data = raw.split(/\r?\n/).filter(line => line.startsWith('data:')).map(line => line.slice(5).replace(/^ /, '')).join('\n');
        if (!data || data === '[DONE]') continue;
        let evt; try { evt = JSON.parse(data); } catch (_) { continue; }
        handle(evt);
      }
    };
    try {
      arm();
      for await (const chunk of res.body) {
        arm();
        flush(typeof chunk === 'string' ? chunk : decoder.write(chunk));
        if (deadline && Date.now() > deadline) throw claudeError('Claude did not finish within the time allowed.', { code: 'CLAUDE_TIMEOUT', retryable: false });
      }
      flush(decoder.end() + '\n\n');
    } finally { clearTimeout(idle); }
    if (!message) throw claudeError('Claude closed the stream before answering.', { retryable: true, code: 'CLAUDE_STREAM_EMPTY' });
    if (!done && !message.stop_reason) throw claudeError('Claude stream ended early.', { retryable: true, code: 'CLAUDE_STREAM_CUT' });
    message.content = blocks.filter(Boolean);
    return message;
  }

  // One request, streamed and retried on overload, rate limits and dropped
  // connections. pause_turn (a long server-side search) is resumed until done.
  async function message(body, opts) {
    const o = opts || {};
    const apiKey = env.ANTHROPIC_API_KEY;
    if (!apiKey) throw claudeError('Claude is not connected: ANTHROPIC_API_KEY is missing. No AI request was sent.', { code: 'CLAUDE_NOT_CONFIGURED', notDispatched: true, definiteResponse: true });
    const retries = o.retries == null ? 4 : Math.max(0, Number(o.retries) || 0);
    const idleMs = o.idleMs == null ? 240000 : Number(o.idleMs) || 0;
    const maxContinuations = o.maxContinuations == null ? 4 : Math.max(0, Number(o.maxContinuations) || 0);
    const payload = { ...body, model: MODEL, stream: true };
    if (!payload.max_tokens) payload.max_tokens = 16000;
    const headers = { 'Content-Type': 'application/json', 'x-api-key': apiKey, 'anthropic-version': API_VERSION };
    const total = zeroUsage(), prior = [];
    let messages = (payload.messages || []).slice(), last = null;
    for (let turn = 0; turn <= maxContinuations; turn++) {
      let attempt = 0, result = null;
      while (!result) {
        attempt++;
        const deadline = o.timeoutMs ? Date.now() + Number(o.timeoutMs) : 0;
        const controller = typeof AbortController === 'function' ? new AbortController() : null;
        const timer = deadline && controller ? setTimeout(() => controller.abort(), Number(o.timeoutMs)) : null;
        try {
          const res = await fetch(API_URL, { method: 'POST', headers, body: JSON.stringify({ ...payload, messages }), ...(controller ? { signal: controller.signal } : {}) });
          if (!res.ok) {
            const raw = await res.text().catch(() => '');
            let data = null; try { data = JSON.parse(raw); } catch (_) { /* keep the raw text */ }
            const err = (data && data.error) || {};
            throw claudeError('Claude request failed (' + res.status + '): ' + String(err.message || raw || res.statusText || '').slice(0, 400), {
              status: res.status, type: err.type || null, requestId: res.headers && res.headers.get ? res.headers.get('request-id') : null,
              retryAfter: res.headers && res.headers.get ? res.headers.get('retry-after') : null,
              retryable: isRetryable(res.status, err.message || raw), definiteResponse: res.status >= 400 && res.status < 500 && res.status !== 408 && res.status !== 429
            });
          }
          result = await readStream(res, { idleMs, deadline, controller });
        } catch (error) {
          const aborted = error && (error.name === 'AbortError' || error.type === 'aborted');
          const timedOut = aborted && deadline && Date.now() >= deadline - 50;
          if (timedOut || (error && error.code === 'CLAUDE_TIMEOUT')) throw claudeError('Claude did not finish within ' + Math.round(Number(o.timeoutMs) / 1000) + ' seconds.', { code: 'CLAUDE_TIMEOUT', cause: error });
          const retryable = aborted || (error && error.retryable) || (error && error.status == null && isRetryable(null, error.message));
          if (!retryable || attempt > retries) { if (aborted) throw claudeError('Claude stopped sending data.', { code: 'CLAUDE_IDLE', cause: error }); throw error; }
          const wait = retryDelayMs(attempt, error && error.retryAfter);
          log(`[claude] retry ${attempt}/${retries} after ${error && (error.status || error.code || error.message)}; waiting ${wait}ms`);
          await sleep(wait);
        } finally { if (timer) clearTimeout(timer); }
      }
      addUsage(total, result.usage);
      last = result;
      if (result.stop_reason !== 'pause_turn' || turn === maxContinuations) break;
      prior.push(...result.content);
      messages = messages.concat([{ role: 'assistant', content: result.content }]);
    }
    const final = { ...last, content: prior.concat(last.content || []), usage: total };
    final.costUsd = estimateCostUsd(total);
    log(`[claude] ${o.label || 'request'} model=${final.model || MODEL} stop=${final.stop_reason} in=${total.input_tokens} cache_read=${total.cache_read_input_tokens} out=${total.output_tokens} searches=${total.server_tool_use.web_search_requests} usd=${final.costUsd}`);
    return final;
  }

  // Ask for one JSON answer. schema: JSON Schema for structured output (not
  // combined with web search, which returns cited text). images: data or
  // https URLs. webSearch: true or {maxUses, allowedDomains, userLocation}.
  async function json(options) {
    const o = options || {};
    const system = [o.system || 'Return only the exact JSON shape requested, with no prose or markdown.'];
    const content = [];
    if (Array.isArray(o.content)) content.push(...partsToBlocks(o.content));
    if (o.prompt) content.push({ type: 'text', text: String(o.prompt) });
    for (const img of o.images || []) content.push(imageBlock(img));
    if (!content.length) throw claudeError('Nothing to ask Claude.', { code: 'CLAUDE_EMPTY_PROMPT', definiteResponse: true });
    // Thinking counts toward max_tokens, so never size a request below 16k;
    // only the tokens actually written are billed.
    const body = { max_tokens: clampTokens(Math.max(16000, Number(o.maxTokens) || 0)), messages: [{ role: 'user', content }], thinking: { type: 'adaptive' }, output_config: { effort: effortFor(o.effort) } };
    if (o.webSearch) {
      body.tools = [webSearchTool(o.webSearch)];
      system.push('Use web search to check facts that may have changed since your training, such as dates, prices and current trends. After searching, answer with the JSON only.' + (o.schema ? ' It must match this JSON schema: ' + JSON.stringify(o.schema) : ''));
    } else if (o.schema) {
      const clean = sanitizeSchema(o.schema);
      body.output_config.format = { type: 'json_schema', schema: clean.schema };
      if (clean.notes.length) system.push(rulesNote(clean.notes));
    }
    body.system = system.filter(Boolean).join('\n\n');
    let msg = await message(body, o);
    if (msg.stop_reason === 'refusal') throw claudeError('Claude declined this request' + (msg.stop_details && msg.stop_details.category ? ' (' + msg.stop_details.category + ')' : '') + '.', { code: 'CLAUDE_REFUSAL', definiteResponse: true, usage: msg.usage, costUsd: msg.costUsd });
    let text = finalText(msg), data = parseJsonText(text);
    if (data === undefined && msg.stop_reason === 'max_tokens' && o.retryOnTruncation !== false && body.max_tokens < 64000) {
      const firstCost = msg.costUsd || 0;
      body.max_tokens = clampTokens(Math.min(64000, body.max_tokens * 2));
      body.output_config = { ...body.output_config, effort: lowerEffort(body.output_config.effort) };
      msg = await message(body, o);
      msg.costUsd = Math.round(((msg.costUsd || 0) + firstCost) * 1e6) / 1e6;
      if (msg.stop_reason === 'refusal') throw claudeError('Claude declined this request.', { code: 'CLAUDE_REFUSAL', definiteResponse: true });
      text = finalText(msg); data = parseJsonText(text);
    }
    if (data === undefined) {
      const u = msg.usage || {};
      throw claudeError(`Claude returned unparseable JSON (stop: ${msg.stop_reason || '?'}, ${text.length} chars; input ${u.input_tokens || 0} tok, output ${u.output_tokens || 0} tok, effort ${body.output_config.effort}).`, { code: 'CLAUDE_BAD_JSON', text, stopReason: msg.stop_reason, usage: msg.usage, costUsd: msg.costUsd });
    }
    return { data, text, message: msg, usage: msg.usage, costUsd: msg.costUsd, model: msg.model || MODEL, sources: sourcesOf(msg) };
  }

  // Run an OpenAI Responses-shaped request on Claude and return a
  // Responses-shaped result (status, output_text, usage, estimatedUsd).
  async function responses(request, opts) {
    const msg = await message(fromResponsesRequest(request), opts);
    return toResponsesResult(msg, { sources: sourcesOf(msg) });
  }

  return { message, json, responses, available: () => !!env.ANTHROPIC_API_KEY };
}

let shared = null;
function client() { return shared || (shared = createClaudeClient()); }

module.exports = {
  MODEL, MODEL_LABEL, PRICE,
  createClaudeClient, client,
  claudeJSON: options => client().json(options),
  claudeMessage: (body, opts) => client().message(body, opts),
  claudeResponses: (request, opts) => client().responses(request, opts),
  available: env => !!((env || process.env).ANTHROPIC_API_KEY),
  estimateCostUsd, sanitizeSchema, rulesNote, imageBlock, fromResponsesRequest, toResponsesResult, finalText, parseJsonText, sourcesOf, effortFor, webSearchTool,
  _test: { zeroUsage, addUsage, isRetryable, retryDelayMs, lowerEffort, partsToBlocks, clampTokens }
};
