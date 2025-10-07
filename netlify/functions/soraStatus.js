'use strict';
const fetch = require('node-fetch');

// Proxy -> OpenAI Videos API: GET /v1/videos/{id}
// Returns the job object; when complete, includes a normalized { video_url } if found.
exports.handler = async (event) => {
  try {
    const JSON_HEADERS = { 'Content-Type': 'application/json' };

    if (event.httpMethod !== 'GET') {
      return { statusCode: 405, headers: JSON_HEADERS, body: JSON.stringify({ error: 'Method Not Allowed' }) };
    }

    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) {
      return { statusCode: 500, headers: JSON_HEADERS, body: JSON.stringify({ error: 'Missing OPENAI_API_KEY' }) };
    }

    const id = event.queryStringParameters?.id;

    // Friendly health-check
    if (id === 'ping') {
      return { statusCode: 200, headers: JSON_HEADERS, body: JSON.stringify({ ok: true, ping: true }) };
    }
    if (!id) {
      return { statusCode: 400, headers: JSON_HEADERS, body: JSON.stringify({ error: 'Missing "id"' }) };
    }

    const resp = await fetch(`https://api.openai.com/v1/videos/${encodeURIComponent(id)}`, {
      method: 'GET',
      headers: { 'Authorization': `Bearer ${apiKey}` }
    });

    const text = await resp.text();
    let data;
    try { data = JSON.parse(text); } catch { data = { raw: text }; }

    if (!resp.ok) {
      return { statusCode: resp.status, headers: JSON_HEADERS, body: JSON.stringify({ error: data }) };
    }

    // -------- Normalize: attempt to locate a usable video URL ----------
    function deepScanForVideoUrl(obj) {
      const seen = new Set();
      function recur(o) {
        if (!o || typeof o !== 'object' || seen.has(o)) return null;
        seen.add(o);
        if (typeof o.video_url === 'string' && /^https?:\/\//.test(o.video_url)) return o.video_url;
        if (typeof o.url === 'string' && /^https?:\/\//.test(o.url) && /mp4|video/.test((o.mime||'') + ' ' + (o.type||'') + ' ' + (o.kind||'') + ' ' + (o.role||''))) return o.url;
        if (typeof o.download_url === 'string' && /^https?:\/\//.test(o.download_url)) return o.download_url;
        if (o.video && typeof o.video.url === 'string') return o.video.url;
        for (const k in o) {
          const v = o[k];
          if (Array.isArray(v)) {
            for (const it of v) {
              const r = recur(it);
              if (r) return r;
            }
          } else if (typeof v === 'object') {
            const r = recur(v);
            if (r) return r;
          }
        }
        return null;
      }
      return recur(obj);
    }

    const status = String(data.status || '').toLowerCase();
    let video_url = null;
    if (status === 'completed' || status === 'succeeded') {
      video_url = deepScanForVideoUrl(data);
    }

    const out = { ...data, ...(video_url ? { video_url } : {}) };
    return { statusCode: 200, headers: JSON_HEADERS, body: JSON.stringify(out) };
  } catch (err) {
    return { statusCode: 500, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ error: String(err.message || err) }) };
  }
};