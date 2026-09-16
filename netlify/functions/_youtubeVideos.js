'use strict';

// YouTube-side state for the videos this application uploads through Google Ads.
//
// Google Ads reports that a video asset is attached and serving. It does not
// report that YouTube finished transcoding it, rejected it for a copyright
// claim, or made it unembeddable — and an ad carrying such a video earns
// nothing while still looking healthy in Ads.
//
// The uploads are created UNLISTED, so videos.list resolves them from an API
// key alone; an OAuth access token is accepted too and is required only if the
// privacy of an upload is ever changed to private.
// https://developers.google.com/youtube/v3/docs/videos/list

const ENDPOINT = 'https://youtube.googleapis.com/youtube/v3/videos';
const PARTS = 'snippet,status,contentDetails,processingDetails';
const BATCH = 50;

function text(v, n) { return String(v == null ? '' : v).slice(0, n || 200); }

// ISO-8601 duration, only the forms YouTube returns for a short ad film.
function seconds(iso) {
  const m = /^P(?:(\d+)D)?T(?:(\d+)H)?(?:(\d+)M)?(?:([\d.]+)S)?$/.exec(String(iso || ''));
  if (!m) return null;
  return (Number(m[1]) || 0) * 86400 + (Number(m[2]) || 0) * 3600 + (Number(m[3]) || 0) * 60 + (Number(m[4]) || 0);
}

// What would stop this video earning, in the operator's terms.
function problemsFor(video, minimumSeconds) {
  const problems = [];
  if (video.uploadStatus === 'rejected') problems.push('YouTube rejected the upload' + (video.rejectionReason ? ' (' + video.rejectionReason + ')' : ''));
  else if (video.uploadStatus === 'failed') problems.push('The upload failed on YouTube' + (video.failureReason ? ' (' + video.failureReason + ')' : ''));
  else if (video.uploadStatus && video.uploadStatus !== 'processed') problems.push('YouTube has not finished processing this video (' + video.uploadStatus + ')');
  if (video.processingStatus === 'failed') problems.push('YouTube processing failed.');
  if (video.privacyStatus === 'private') problems.push('The video is private, so an ad cannot play it.');
  if (video.embeddable === false) problems.push('The video is not embeddable, so it cannot serve in an ad.');
  if (Number.isFinite(minimumSeconds) && Number.isFinite(video.seconds) && video.seconds < minimumSeconds)
    problems.push('The video is ' + video.seconds + 's, below the ' + minimumSeconds + 's Google requires.');
  return problems;
}

async function videoStatus(input) {
  const { fetch, apiKey, accessToken, videoIds, minimumSeconds = 10 } = input || {};
  if (typeof fetch !== 'function') throw new Error('A fetch implementation is required.');
  if (!apiKey && !accessToken) throw new Error('Set YOUTUBE_API_KEY, or supply an OAuth access token, to read YouTube video state.');
  const ids = [...new Set((videoIds || []).map(id => String(id || '').trim()).filter(id => /^[A-Za-z0-9_-]{6,20}$/.test(id)))];
  if (!ids.length) return { videos: [], missing: [], requested: 0, note: 'No YouTube video IDs were supplied.' };

  const found = new Map();
  for (let i = 0; i < ids.length; i += BATCH) {
    const chunk = ids.slice(i, i + BATCH);
    const url = ENDPOINT + '?part=' + encodeURIComponent(PARTS) + '&id=' + encodeURIComponent(chunk.join(',')) + (apiKey && !accessToken ? '&key=' + encodeURIComponent(apiKey) : '');
    const res = await fetch(url, { timeout: 20000, ...(accessToken ? { headers: { Authorization: 'Bearer ' + accessToken } } : {}) });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error('YouTube Data API: ' + (((data || {}).error || {}).message || 'HTTP ' + res.status));
    for (const item of data.items || []) {
      const status = item.status || {}, details = item.contentDetails || {}, processing = item.processingDetails || {};
      const video = {
        id: item.id,
        title: text((item.snippet || {}).title, 160),
        privacyStatus: status.privacyStatus || null,
        uploadStatus: status.uploadStatus || null,
        rejectionReason: status.rejectionReason || null,
        failureReason: status.failureReason || null,
        processingStatus: processing.processingStatus || null,
        embeddable: status.embeddable === undefined ? null : !!status.embeddable,
        duration: details.duration || null,
        seconds: seconds(details.duration),
        thumbnail: (((item.snippet || {}).thumbnails || {}).high || {}).url || null
      };
      video.problems = problemsFor(video, minimumSeconds);
      video.serviceable = video.problems.length === 0;
      found.set(item.id, video);
    }
  }

  // A video Google Ads knows about that YouTube will not return is itself the
  // finding: it was deleted, made private, or never finished publishing.
  const missing = ids.filter(id => !found.has(id));
  const videos = ids.filter(id => found.has(id)).map(id => found.get(id));
  const unserviceable = videos.filter(v => !v.serviceable);
  return {
    requested: ids.length, videos, missing,
    unserviceable: unserviceable.length,
    detail: missing.length || unserviceable.length
      ? unserviceable.length + ' of ' + ids.length + ' video(s) cannot serve' + (missing.length ? ', and ' + missing.length + ' could not be found on YouTube' : '')
      : 'All ' + ids.length + ' video(s) are processed and serviceable.',
    note: 'YouTube reports processing and availability. It does not report whether Google Ads chose to serve the video.'
  };
}

module.exports = { videoStatus, problemsFor, seconds };
