 'use strict';
const fetch = require('node-fetch');

// Proxy -> OpenAI Videos API: POST /v1/videos
// Body: { prompt, model="sora-2", size="720x1280", seconds:"4"|"8"|"12" }
// Returns: job object { id, status, ... }
exports.handler = async (event) => {
  try {
    const JSON_HEADERS = { 'Content-Type': 'application/json' };

    if (event.httpMethod !== 'POST') {
      return { statusCode: 405, headers: JSON_HEADERS, body: JSON.stringify({ error: 'Method Not Allowed' }) };
    }

    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) {
      return { statusCode: 500, headers: JSON_HEADERS, body: JSON.stringify({ error: 'Missing OPENAI_API_KEY env' }) };
    }

    let body = {};
    try { body = JSON.parse(event.body || '{}'); } catch { body = {}; }

    const prompt  = (body.prompt ?? '').trim();
    const model   = body.model   || 'sora-2';
    const size    = body.size    || '720x1280';

    // IMPORTANT: Seconds must be a STRING: "4" | "8" | "12"
    const seconds = String(body.seconds ?? '4').trim();

    // Health-check path
    if (prompt === '__ping__') {
      return { statusCode: 200, headers: JSON_HEADERS, body: JSON.stringify({ ok: true, ping: true }) };
    }

    if (!prompt) {
      return { statusCode: 400, headers: JSON_HEADERS, body: JSON.stringify({ error: 'Missing "prompt"' }) };
    }
    if (!['4', '8', '12'].includes(seconds)) {
      return { statusCode: 400, headers: JSON_HEADERS, body: JSON.stringify({ error: 'seconds must be "4", "8", or "12"' }) };
    }

    const payload = { model, prompt, size, seconds };

    const resp = await fetch('https://api.openai.com/v1/videos', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${apiKey}`
      },
      body: JSON.stringify(payload)
    });

    const text = await resp.text();
    let data;
    try { data = JSON.parse(text); } catch { data = { raw: text }; }

    if (!resp.ok) {
      return { statusCode: resp.status, headers: JSON_HEADERS, body: JSON.stringify({ error: data }) };
    }
    return { statusCode: 200, headers: JSON_HEADERS, body: JSON.stringify(data) };
  } catch (err) {
    return { statusCode: 500, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ error: String(err.message || err) }) };
  }
};