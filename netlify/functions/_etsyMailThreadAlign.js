/*  netlify/functions/_etsyMailThreadAlign.js
 *
 *  Matches a scrape of an Etsy conversation against the messages already
 *  stored, by message ORDER and content, never by time.
 *
 *  Why: the scraper reads times off Etsy's page, and the messages at the top
 *  of a partial (incremental) scrape have no day header above them, so they
 *  were stamped with the time of the next day's first message. The old
 *  dedupe needed the same minute, so every re-scrape stored those messages
 *  again ("Original is absolutely fine..." on Aug 26 AND Aug 27). Order and
 *  content are what the page shows reliably, so matching uses those.
 *
 *  Pure functions, no Firestore: etsyMailSnapshot.js calls them, and
 *  tests/etsy-mail/thread-align.cjs runs them in Node.
 */

"use strict";

// "Translate to English" is Etsy's button label, rendered on some scrapes
// of a customer message and not on others.
function normalizeText(s) {
  return String(s == null ? "" : s)
    .replace(/^\s*(?:Message|Subject):\s*/i, "")
    .replace(/\s*Translate to English\s*$/i, "")
    .toLowerCase()
    .replace(/[‘’]/g, "'")
    .replace(/[“”]/g, '"')
    .replace(/https?:\/\/(?:www\.)?/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

// The image's own id in an Etsy conversation-image URL:
//   https://i.etsystatic.com/icm/f32f7b/910124043/icm_fullxfull.910124043_r4hk7.png
// Size variants (150x150, fullxfull) and query strings share it.
function imageId(url) {
  const clean = String(url || "").split(/[?#]/)[0];
  if (!clean) return "";
  const m = clean.match(/\/(\d{5,})\/[^/]+$/) || clean.match(/[._](\d{6,})_/);
  if (m) return m[1];
  const parts = clean.split("/").filter(Boolean);
  return parts.length >= 2 ? parts[parts.length - 2] : clean;
}

function roleOf(senderRole) {
  return senderRole === "staff" || senderRole === "shop_owner" ? "s" : "c";
}

// Text wins; an image-only message is keyed by its image ids.
function fingerprint(m) {
  if (!m) return "";
  const role = roleOf(m.senderRole);
  const txt = normalizeText(m.text != null && m.text !== "" ? m.text : "");
  if (txt) return role + "|T:" + txt;
  const urls = Array.isArray(m.imageUrls) ? m.imageUrls : [];
  const ids = urls.map(imageId).filter(Boolean).sort();
  if (ids.length) return role + "|I:" + ids.join(",");
  return "";
}

function msOf(v) {
  if (v == null) return null;
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  if (typeof v.toMillis === "function") return v.toMillis();
  if (typeof v === "object" && typeof v.ms === "number") return v.ms;
  if (typeof v === "object" && typeof v._seconds === "number") return v._seconds * 1000;
  return null;
}

// Stored messages sorted the way the inbox shows them.
function sortStored(stored) {
  return stored.slice().sort((a, b) =>
    (a.tsMs - b.tsMs) || ((a.createdMs || 0) - (b.createdMs || 0)) || String(a.id).localeCompare(String(b.id)));
}

/**
 * alignScrape(stored, incoming)
 *   stored:   [{ id, fp, tsMs, createdMs }]  scraped (source "etsy") messages already saved
 *   incoming: [{ fp, tsMs }]                  this scrape, in page order (oldest first)
 *
 * Returns
 *   matchOf:     incoming index -> index into the returned `sorted` (or -1)
 *   sorted:      the stored list in timeline order
 *   newTs:       incoming index -> time to store for an unmatched message,
 *                kept between its matched neighbours so it lands in place
 *   duplicates:  stored ids this scrape proves are extra copies
 *   gap:         nothing in this scrape matched what is stored, so older
 *                messages above the visible part may be missing
 *
 * Matching is a heaviest common subsequence: the most messages in the same
 * order, and among equal choices the copy saved first (the original, whose
 * time was read with its day header in view).
 */
function alignScrape(storedIn, incoming, { scrapedAt = Date.now() } = {}) {
  const sorted = sortStored(storedIn.filter(s => s && s.fp && s.tsMs != null));
  const n = incoming.length, m = sorted.length;
  const matchOf = new Array(n).fill(-1);

  if (n && m) {
    // Earlier-saved copies get a small bonus (below one match's weight).
    const order = sorted.map((s, j) => j).sort((a, b) =>
      ((sorted[a].createdMs || 0) - (sorted[b].createdMs || 0)) || (a - b));
    const bonus = new Float64Array(m);
    order.forEach((j, rank) => { bonus[j] = m - rank; });
    const W = (m + 1) * (n + 1) + 1;          // one match outweighs every bonus together
    const cols = m + 1;
    const dp = new Float64Array((n + 1) * cols);
    for (let i = n - 1; i >= 0; i--) {
      const fpI = incoming[i].fp;
      for (let j = m - 1; j >= 0; j--) {
        let best = Math.max(dp[(i + 1) * cols + j], dp[i * cols + j + 1]);
        if (fpI && fpI === sorted[j].fp) {
          const take = W + bonus[j] + dp[(i + 1) * cols + j + 1];
          if (take > best) best = take;
        }
        dp[i * cols + j] = best;
      }
    }
    let i = 0, j = 0;
    while (i < n && j < m) {
      const here = dp[i * cols + j];
      if (incoming[i].fp && incoming[i].fp === sorted[j].fp
          && here === W + bonus[j] + dp[(i + 1) * cols + j + 1]) {
        matchOf[i] = j; i++; j++;
      } else if (here === dp[(i + 1) * cols + j]) {
        i++;
      } else {
        j++;
      }
    }
  }

  const matched = matchOf.filter(j => j >= 0);
  const gap = m > 0 && n > 0 && matched.length === 0;

  // Times for unmatched (new) messages: inside the matched neighbours.
  const newTs = new Array(n).fill(null);
  let prevTs = null;
  for (let i = 0; i < n; i++) {
    if (matchOf[i] >= 0) { prevTs = sorted[matchOf[i]].tsMs; continue; }
    let upper = null, nextIdx = -1;
    for (let k = i + 1; k < n; k++) if (matchOf[k] >= 0) { upper = sorted[matchOf[k]].tsMs; nextIdx = matchOf[k]; break; }
    let lower = prevTs;
    if (lower == null && nextIdx > 0) lower = sorted[nextIdx - 1].tsMs;
    let t = incoming[i].tsMs;
    if (t == null) t = upper != null ? upper - 1 : (lower != null ? lower + 1 : scrapedAt);
    // A time past the next stored message is a borrowed one (they only run
    // late): place it right after the message above it.
    if (upper != null && t > upper) t = lower != null ? lower + 1 : upper - 1;
    if (upper != null && t === upper) t = upper - 1;     // same stamp as the one below: sort just above it
    if (lower != null && t <= lower) t = lower + 1;
    if (upper != null && t > upper) t = upper;          // no room between them: share the minute
    newTs[i] = t;
    prevTs = t;
  }

  // Extra copies: inside the stretch this scrape covers, a message stored
  // more often than the page shows it. Only when the scrape clearly lines up.
  const duplicates = [];
  if (matched.length >= 3 || (matched.length >= 2 && matched.length * 2 >= n)) {
    const lo = Math.min(...matched), hi = Math.max(...matched);
    const onPage = new Map();
    for (const it of incoming) if (it.fp) onPage.set(it.fp, (onPage.get(it.fp) || 0) + 1);
    const matchedSet = new Set(matched);
    const matchedFps = new Set(matched.map(j => sorted[j].fp));
    const inSpan = new Map();
    for (let j = lo; j <= hi; j++) {
      const fp = sorted[j].fp;
      if (!matchedFps.has(fp)) continue;
      if (!inSpan.has(fp)) inSpan.set(fp, []);
      inSpan.get(fp).push(j);
    }
    for (const [fp, js] of inSpan) {
      let extra = js.length - (onPage.get(fp) || 0);
      if (extra <= 0) continue;
      const spare = js.filter(j => !matchedSet.has(j))
        .sort((a, b) => (sorted[b].createdMs || 0) - (sorted[a].createdMs || 0));   // newest copies go first
      for (const j of spare) { if (extra-- <= 0) break; duplicates.push(sorted[j].id); }
    }
  }

  return { sorted, matchOf, newTs, duplicates, gap };
}

/**
 * findLegacyDuplicates(stored, others) — copies saved by the old time-based
 * dedupe, found without a new scrape.
 *
 * The old bug always left the same shape. A partial scrape starts part-way
 * through a day; the messages above the first day header on the page got the
 * time of the first message below it, did not match what was stored, and
 * were saved again. So in that scrape's batch the copies are the TOP of the
 * page (the scraper adds each message's page position to its time in ms,
 * which gives the order), all at one borrowed minute, each an exact repeat
 * of an earlier-saved message with an earlier time. A copy d of message o
 * counts only when:
 *   - it is in that leading run: nothing new sits above it in its scrape;
 *   - every message stored strictly between o's and d's times was re-saved
 *     in d's scrape at d's minute too (they were displaced together); and
 *   - a message that is not a copy sits at d's minute below it (the message
 *     whose time was borrowed): saved earlier, or lower on the same page, or
 *     our own "just sent" stand-in.
 * An auto-reply to a new message sits below that message, so it is kept.
 * A scrape that read no times at all (every message near the scrape time,
 * one second apart) is judged by order alone: its leading run of repeats.
 *   stored: [{ id, fp, tsMs, createdMs }]  (source "etsy" only)
 *   others: [{ fp, tsMs }]  our own "just sent" stand-ins
 * Returns the ids of the copies.
 */
function findLegacyDuplicates(storedIn, others = []) {
  const all = sortStored(storedIn.filter(s => s && s.fp && s.tsMs != null && s.createdMs != null));
  const minute = t => Math.floor(t / 60000);
  const pageIdx = t => t % 60000;             // position the scraper added (0..999)
  const batches = new Map();
  for (const s of all) {
    if (!batches.has(s.createdMs)) batches.set(s.createdMs, []);
    batches.get(s.createdMs).push(s);
  }
  const removed = new Set();
  const live = () => all.filter(s => !removed.has(s.id));

  for (const bk of Array.from(batches.keys()).sort((a, b) => a - b)) {
    const batch = batches.get(bk);
    // A scrape that found no times on the page stamped every message with
    // the scrape time minus one second per message still to come: then the
    // times say nothing, only the order does.
    const noTimes = batch.every(x => x.tsMs <= bk && bk - x.tsMs < 15 * 60000 && pageIdx(x.tsMs) >= 1000);
    if (!noTimes && batch.some(x => pageIdx(x.tsMs) >= 1000)) continue;   // page order unknown
    batch.sort((a, b) => noTimes ? a.tsMs - b.tsMs : pageIdx(a.tsMs) - pageIdx(b.tsMs));
    const earlier = live().filter(s => s.createdMs < bk);
    const hasEarlierCopy = (x, beforeMin) => earlier.some(e => e.fp === x.fp && minute(e.tsMs) < beforeMin);
    const used = new Map();                                // fp -> earlier copies already paired
    for (const d of batch) {
      const dMin = minute(d.tsMs);
      const copies = earlier.filter(o => o.fp === d.fp && (noTimes || minute(o.tsMs) < dMin));
      if (copies.length <= (used.get(d.fp) || 0)) break;  // the first new message ends the run
      used.set(d.fp, (used.get(d.fp) || 0) + 1);
      if (noTimes) { removed.add(d.id); continue; }
      const oMin = minute(copies[copies.length - 1].tsMs);
      const displacedFps = new Set(batch.filter(x => minute(x.tsMs) === dMin).map(x => x.fp));
      const between = live().filter(x => x.id !== d.id && x.createdMs <= bk
        && minute(x.tsMs) > oMin && minute(x.tsMs) < dMin);
      if (!between.every(x => displacedFps.has(x.fp))) break;
      const owner = live().some(x => x.id !== d.id && minute(x.tsMs) === dMin
          && (x.createdMs < bk || (x.createdMs === bk && pageIdx(x.tsMs) > pageIdx(d.tsMs)))
          && !hasEarlierCopy(x, dMin))
        || (others || []).some(x => x && x.tsMs != null && minute(x.tsMs) === dMin && x.fp !== d.fp);
      if (!owner) break;
      removed.add(d.id);
    }
  }
  return Array.from(removed);
}

module.exports = { normalizeText, imageId, fingerprint, msOf, sortStored, alignScrape, findLegacyDuplicates };
