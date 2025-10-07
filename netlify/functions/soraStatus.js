 'use strict';
const fetch = require('node-fetch');

// Proxy -> OpenAI Videos API: GET /v1/videos/{id}
// Returns the job object; when complete, it includes downloadable asset(s)
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

    // Health-check path: friendly 200 OK for ?id=ping
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
    return { statusCode: 200, headers: JSON_HEADERS, body: JSON.stringify(data) };
  } catch (err) {
    return { statusCode: 500, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ error: String(err.message || err) }) };
  }
};