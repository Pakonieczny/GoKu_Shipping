// netlify/functions/soraCreate.js
'use strict';

const fetch = require('node-fetch');
const FormData = require('form-data');

const API_BASE = process.env.OPENAI_BASE_URL || 'https://api.openai.com/v1';
const API_KEY  = process.env.OPENAI_API_KEY;

const JSON_HEADERS = { 'Content-Type': 'application/json' };

const json = (code, obj) => ({
  statusCode: code,
  headers: JSON_HEADERS,
  body: JSON.stringify(obj)
});

function dataUrlToBuffer(dataUrl) {
  const m = /^data:([^;]+);base64,(.*)$/i.exec(dataUrl || '');
  if (!m) throw new Error('Bad data_url for input_reference');
  return { mime: m[1], buf: Buffer.from(m[2], 'base64') };
}

exports.handler = async (event) => {
  try {
    if (event.httpMethod !== 'POST') {
      return json(405, { error: 'Method Not Allowed' });
    }
    if (!API_KEY) {
      return json(500, { error: 'Missing OPENAI_API_KEY' });
    }

    let body = {};
    try { body = JSON.parse(event.body || '{}'); }
    catch { return json(400, { error: 'Invalid JSON body' }); }

    const {
      prompt = '',
      model  = 'sora-2',
      size   = '720x1280',
      seconds = '4',         // MUST be string per Videos API
      mode   = 'video',
      input_reference = null // { filename, content_type, data_url }
    } = body;

    // health check
    if (prompt === '__ping__') return json(200, { ok: true, ping: true });

    if (!prompt.trim()) return json(400, { error: 'Missing prompt' });

    // ---- IMAGE MODE (gpt-image-1) ----
    if (mode === 'image') {
      // map video-ish sizes to image sizes
      const sizeMap = {
        '1792x1024': '1792x1024',
        '1024x1792': '1024x1792',
        '1280x720' : '1024x576',
        '720x1280' : '576x1024'
      };
      const imgSize = sizeMap[size] || '1024x1024';

      const r = await fetch(`${API_BASE}/images`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${API_KEY}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          model: 'gpt-image-1',
          prompt,
          size: imgSize,
          n: 1
        })
      });

      const j = await r.json().catch(() => ({}));
      if (!r.ok) return json(r.status, { error: j.error || j });

      const urls = (j.data || []).map(d => d.url).filter(Boolean);
      return json(200, { images: urls, raw: j });
    }

    // ---- VIDEO MODE (Sora Videos API) ----
    if (!['4','8','12'].includes(String(seconds))) {
      return json(400, { error: "Invalid 'seconds' — must be '4' | '8' | '12' (string)" });
    }

    const fd = new FormData();
    fd.append('model', model);
    fd.append('prompt', prompt);
    fd.append('size', size);
    fd.append('seconds', String(seconds));

    // Single reference image supported — send as multipart file
    if (input_reference?.data_url || input_reference?.dataUrl) {
      const data_url = input_reference.data_url || input_reference.dataUrl;
      const filename = input_reference.filename || 'reference.png';
      const content_type = input_reference.content_type || input_reference.contentType || 'image/png';
      const { buf } = dataUrlToBuffer(data_url);
      fd.append('input_reference', buf, { filename, contentType: content_type });
    }

    const r = await fetch(`${API_BASE}/videos`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${API_KEY}`,
        ...fd.getHeaders()
      },
      body: fd
    });

    const j = await r.json().catch(() => ({}));
    if (!r.ok) {
      return json(r.status, { error: j.error || j || 'Video API error' });
    }

    // normalize shape
    return json(200, {
      id: j.id || j.job_id || j.video?.id || null,
      status: j.status || 'created',
      ...j
    });

  } catch (err) {
    return json(500, {
      error: 'soraCreate internal error',
      message: err?.message || String(err)
    });
  }
};