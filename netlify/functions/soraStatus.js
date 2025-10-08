// netlify/functions/soraStatus.js
'use strict';

const fetch = require('node-fetch');

const API_BASE = process.env.OPENAI_BASE_URL || 'https://api.openai.com/v1';
const API_KEY  = process.env.OPENAI_API_KEY;

const JSON_HEADERS = { 'Content-Type': 'application/json' };

const json = (code, obj) => ({
  statusCode: code,
  headers: JSON_HEADERS,
  body: JSON.stringify(obj)
});

function pickVideoUrl(job) {
  const candidates = [];
  const push = (u) => { if (typeof u === 'string') candidates.push(u); };

  if (job.video_url) push(job.video_url);
  for (const k of ['assets','files','results','output','outputs']) {
    const arr = job[k];
    if (Array.isArray(arr)) {
      for (const a of arr) push(a?.url || a?.download_url || a?.signed_url || a?.cdn_url || a?.href);
    }
  }
  if (job.output?.video?.url) push(job.output.video.url);
  if (job.result?.video?.url) push(job.result.video.url);

  return candidates.find(u => /\.(mp4|webm|mov)(\?|#|$)/i.test(u)) || candidates[0] || null;
}

exports.handler = async (event) => {
  try {
    if (!API_KEY) return json(500, { error: 'Missing OPENAI_API_KEY' });

    const id = event.queryStringParameters?.id;
    const content = (event.queryStringParameters?.content || '').toLowerCase();

    // ping
    if (id === 'ping') return json(200, { ok: true });
    if (!id) return json(400, { error: 'Missing id' });

    // Binary proxy branch (optional content variant)
    if (content) {
      const qs = (content && content !== 'video') ? ('?variant=' + encodeURIComponent(content)) : '';
      const url = `${API_BASE}/videos/${encodeURIComponent(id)}/content${qs}`;

      const headers = { 'Authorization': `Bearer ${API_KEY}` };
      const range = event.headers?.range || event.headers?.Range;
      if (range) headers['Range'] = range;

      const resp = await fetch(url, { method: 'GET', headers });
      const ab = await resp.arrayBuffer();
      const buf = Buffer.from(ab);

      const passthrough = (name, fallback) => resp.headers.get(name) || fallback || undefined;
      const outHeaders = {
        'Content-Type'   : passthrough('content-type', content === 'thumbnail' ? 'image/webp' : 'video/mp4'),
        'Accept-Ranges'  : passthrough('accept-ranges'),
        'Content-Range'  : passthrough('content-range'),
        'Cache-Control'  : passthrough('cache-control', 'public, max-age=31536000, immutable'),
        'Content-Length' : String(buf.length)
      };

      return {
        statusCode: resp.status,
        headers: outHeaders,
        isBase64Encoded: true,
        body: buf.toString('base64')
      };
    }

    // JSON status
    const r = await fetch(`${API_BASE}/videos/${encodeURIComponent(id)}`, {
      method: 'GET',
      headers: { 'Authorization': `Bearer ${API_KEY}` }
    });

    const j = await r.json().catch(() => ({}));
    if (!r.ok) return json(r.status, { error: j.error || j });

    // convenience: add a best-effort video_url on terminal states
    const statusRaw = (j.status || j.state || '').toString().toLowerCase();
    const terminal = new Set(['completed','succeeded','ready','finished','done','complete']);
    const video_url = terminal.has(statusRaw) ? pickVideoUrl(j) : null;

    return json(200, video_url ? { ...j, video_url } : j);

  } catch (err) {
    return json(500, {
      error: 'soraStatus internal error',
      message: err?.message || String(err)
    });
  }
};