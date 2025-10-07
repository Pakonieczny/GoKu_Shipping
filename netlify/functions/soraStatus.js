'use strict';
const fetch = require('node-fetch');

// GET /.netlify/functions/soraStatus?id=<videoId>[&content=video|thumbnail|spritesheet]
// - Without `content`: returns JSON status (and we still try to normalize { video_url } if present).
// - With `content`: proxies the binary from OpenAI (Content-Type: video/mp4, image/webp, etc.).
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
    const content = (event.queryStringParameters?.content || '').toLowerCase(); // '', 'video', 'thumbnail', 'spritesheet', etc.

    // health check
    if (id === 'ping') {
      return { statusCode: 200, headers: JSON_HEADERS, body: JSON.stringify({ ok: true, ping: true }) };
    }
    if (!id) {
      return { statusCode: 400, headers: JSON_HEADERS, body: JSON.stringify({ error: 'Missing "id"' }) };
    }

    // ---------- BINARY PROXY BRANCH (downloads) ----------
    if (content) {
      // Build /v1/videos/{id}/content with optional variant
      const qs = (content && content !== 'video') ? ('?variant=' + encodeURIComponent(content)) : '';
      const url = `https://api.openai.com/v1/videos/${encodeURIComponent(id)}/content${qs}`;

      // Forward Range for seeking support if browser sends it
      const headers = { 'Authorization': `Bearer ${apiKey}` };
      const range = event.headers?.range || event.headers?.Range;
      if (range) headers['Range'] = range;

      const resp = await fetch(url, { method: 'GET', headers });
      const ab = await resp.arrayBuffer();
      const buf = Buffer.from(ab);

      // Pass through useful headers
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

    // ---------- JSON STATUS BRANCH (what you already had) ----------
    const statusResp = await fetch(`https://api.openai.com/v1/videos/${encodeURIComponent(id)}`, {
      method: 'GET',
      headers: { 'Authorization': `Bearer ${apiKey}` }
    });

    const text = await statusResp.text();
    let data;
    try { data = JSON.parse(text); } catch { data = { raw: text }; }

    if (!statusResp.ok) {
      return { statusCode: statusResp.status, headers: JSON_HEADERS, body: JSON.stringify({ error: data }) };
    }

    // URL pickers & scanners (unchanged; keeps working if API ever embeds a URL).
    function isHttpish(s) { return typeof s === 'string' && /^https?:\/\//.test(s); }
    function looksLikeVideoUrl(s) { return isHttpish(s) && /\.(mp4|webm|mov)(\?|#|$)/i.test(s); }
    function isVideoishMeta(o) {
      const meta = `${o?.mime||''} ${o?.type||''} ${o?.kind||''} ${o?.role||''} ${o?.content_type||''}`;
      return /(video|mp4|webm|quicktime|mov)/i.test(meta);
    }
    function pickFromRecord(o) {
      if (!o || typeof o !== 'object') return null;
      const candidates = [o.video_url, o.download_url, o.cdn_url, o.signed_url, o.file_url, o.preview_url, o.stream_url, o.url, o.uri, o.href, o.source, o.src];
      for (const c of candidates) if (isHttpish(c) && (looksLikeVideoUrl(c) || isVideoishMeta(o))) return c;
      if (typeof o?.video === 'string' && isHttpish(o.video)) return o.video;
      if (o?.video && isHttpish(o.video.url)) return o.video.url;
      if (o?.media && isHttpish(o.media.url)) return o.media.url;
      return null;
    }
    function deepScanForVideoUrl(obj) {
      const seen = new Set(); const stack = [obj];
      while (stack.length) {
        const cur = stack.pop();
        if (!cur || typeof cur !== 'object' || seen.has(cur)) continue;
        seen.add(cur);
        const direct = pickFromRecord(cur); if (direct) return direct;
        for (const k in cur) {
          const v = cur[k]; if (!v) continue;
          if (Array.isArray(v)) {
            for (const it of v) {
              const hit = pickFromRecord(it); if (hit) return hit;
              if (it && typeof it === 'object') stack.push(it);
              else if (typeof it === 'string' && looksLikeVideoUrl(it)) return it;
            }
          } else if (typeof v === 'object') { stack.push(v); }
          else if (typeof v === 'string' && looksLikeVideoUrl(v)) { return v; }
        }
      }
      return null;
    }
    function pickVideoUrl(job) {
      try {
        if (job.video_url && isHttpish(job.video_url)) return job.video_url;
        const arrays = [
          job.assets, job.files, job.results, job.output, job.outputs,
          job.result?.files, job.result?.assets, job.result?.outputs,
          job.output?.files, job.output?.assets, job.output?.outputs,
          job.render?.outputs, job.media?.sources
        ].filter(Array.isArray);
        for (const arr of arrays) {
          let hit = arr.find(a => isVideoishMeta(a) && pickFromRecord(a));
          if (!hit) hit = arr.find(a => { const s = pickFromRecord(a) || a; return typeof s === 'string' && looksLikeVideoUrl(s); });
          if (hit) {
            const chosen = pickFromRecord(hit) || hit.url || hit.uri || hit.href || hit.download_url || hit.signed_url || hit.cdn_url || hit.file_url || null;
            if (chosen) return chosen;
          }
        }
        if (job.output?.video?.url && isHttpish(job.output.video.url)) return job.output.video.url;
        if (job.result?.video?.url && isHttpish(job.result.video.url)) return job.result.video.url;
        const deep = deepScanForVideoUrl(job); if (deep) return deep;
      } catch (_) {}
      return null;
    }

    const statusRaw = (data.status || data.state || '').toString().toLowerCase();
    const terminal = new Set(['completed','succeeded','ready','finished','done','complete']);
    const video_url = terminal.has(statusRaw) ? pickVideoUrl(data) : null;

    const out = { ...data, ...(video_url ? { video_url } : {}) };
    return { statusCode: 200, headers: JSON_HEADERS, body: JSON.stringify(out) };
  } catch (err) {
    return { statusCode: 500, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ error: String(err?.message || err) }) };
  }
};