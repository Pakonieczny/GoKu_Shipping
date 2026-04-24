/* netlify/functions/_etsyMailCarrierUsps.js
 *
 * USPS tracking driver — headless-Chromium approach.
 *
 * WHY THIS APPROACH (after trying everything else):
 *   - Apify actors: substantial_sponge is "Under maintenance", Howlers'
 *     multi-carrier requires a USPS Web Tools User ID (service shut down
 *     January 2026)
 *   - 17TRACK: USPS tracking blocked on free tier due to USPS policy changes
 *   - USPS direct API: requires MID application + weeks of approval
 *
 *   We open the public USPS tracking page with Puppeteer, wait for the
 *   tracking-details panel to render, extract structured data from the DOM,
 *   and screenshot just that panel. Zero third-party dependencies, zero
 *   per-lookup cost.
 *
 * ARCHITECTURE:
 *   This file ALSO does the rendering (full bypass of _etsyMailTrackingRender).
 *   The "image" we return in the normalized result IS a PNG screenshot of
 *   USPS's actual tracking card — higher customer credibility than a
 *   re-rendered SVG that might not reflect reality.
 *
 *   Caller (_etsyMailTracking.js) uses our imageBuffer directly instead of
 *   asking the SVG renderer. We detect this via the returned `imageBuffer`
 *   field (a Buffer) on the result object. If absent, caller falls back to
 *   SVG rendering with the extracted events.
 *
 * IMPORTANT: This driver is ONLY safe to call from inside a background
 * function (etsyMailTrackingSnapshot-background.js), which has a 15-min
 * execution budget. Chromium cold-start + page load can take 10-20 sec.
 *
 * Env vars:
 *   (none required — no API keys)
 *
 * Optional env vars:
 *   CHROME_EXECUTABLE_PATH         Override chromium path (local dev)
 *   USPS_PAGE_TIMEOUT_MS           Page.goto timeout (default: 20000)
 *   USPS_SELECTOR_WAIT_MS          Wait for tracking panel (default: 12000)
 *   USPS_USER_AGENT                Override the browser UA
 *   USPS_DEBUG_RETURN_HTML         "1" to include HTML in result for debugging
 *
 * Dependencies (must be added to package.json and marked external in netlify.toml):
 *   @sparticuz/chromium-min   ^131.0.0
 *   puppeteer-core            ^24.0.0
 *
 * ABOUT @sparticuz/chromium-min vs @sparticuz/chromium:
 *   The non-`-min` package includes Chromium binaries inside node_modules,
 *   adding ~150MB to the install. On Netlify's free/pro build tier this
 *   consistently hits ENOSPC (out of disk) errors during `npm install` when
 *   combined with other production deps (firebase-admin, sharp, etc).
 *   The `-min` variant downloads Chromium from a CDN at function cold-start,
 *   caching it to /tmp. First invocation after a deploy pays ~3-5 extra
 *   seconds for the download; warm invocations reuse the cached binary.
 */

const chromium  = require("@sparticuz/chromium-min");
const puppeteer = require("puppeteer-core");

// Chromium pack tar URL. This is the binary Chromium-min will download at
// first cold start. Sparticuz hosts these on GitHub releases. We pin to a
// specific version that's known compatible with puppeteer-core 24.x.
const DEFAULT_CHROMIUM_PACK_URL =
  "https://github.com/Sparticuz/chromium/releases/download/v131.0.1/chromium-v131.0.1-pack.tar";
const CHROMIUM_PACK_URL = process.env.CHROMIUM_PACK_URL || DEFAULT_CHROMIUM_PACK_URL;

const DEFAULT_PAGE_TIMEOUT     = 20000;
const DEFAULT_SELECTOR_TIMEOUT = 12000;
const DEFAULT_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

const PAGE_TIMEOUT     = Number(process.env.USPS_PAGE_TIMEOUT_MS)    || DEFAULT_PAGE_TIMEOUT;
const SELECTOR_TIMEOUT = Number(process.env.USPS_SELECTOR_WAIT_MS)   || DEFAULT_SELECTOR_TIMEOUT;
const USER_AGENT       = process.env.USPS_USER_AGENT                  || DEFAULT_UA;
const DEBUG_RETURN_HTML = /^(1|true|yes)$/i.test(process.env.USPS_DEBUG_RETURN_HTML || "");

const USPS_URL = (code) => `https://tools.usps.com/go/TrackConfirmAction?tLabels=${encodeURIComponent(code)}`;

/** Launch a headless Chromium browser configured for Netlify. */
async function launchBrowser() {
  console.log("[usps] launching browser");

  // @sparticuz/chromium-min requires us to tell it where to download the
  // Chromium pack tar from. It'll cache to /tmp/chromium-pack after the
  // first cold-start download, so warm invocations are fast.
  //
  // IMPORTANT: `executablePath()` is a FUNCTION in chromium 117+. Calling it
  // as a property (`chromium.executablePath`) hands Puppeteer undefined and
  // silently fails.
  const executablePath = process.env.CHROME_EXECUTABLE_PATH ||
                         await chromium.executablePath(CHROMIUM_PACK_URL);

  console.log(`[usps] chromium path: ${executablePath}`);

  const browser = await puppeteer.launch({
    args            : chromium.args,
    defaultViewport : { width: 1280, height: 1800, deviceScaleFactor: 2 },
    executablePath,
    headless        : chromium.headless
  });

  return browser;
}

/**
 * Extract structured tracking data from the loaded USPS page.
 * Runs INSIDE the page context (browser JS, not Node).
 *
 * We read the page's DOM directly instead of regex'ing HTML — more resilient
 * to whitespace changes. The functions passed to page.evaluate() are
 * serialized and run in the browser, so they can't reference any closure
 * variables from Node.
 */
async function extractData(page) {
  return await page.evaluate(() => {
    // Helpers (redefined in browser context)
    const txt = (el) => (el ? (el.textContent || "").replace(/\s+/g, " ").trim() : "");
    const q   = (sel) => document.querySelector(sel);
    const qa  = (sel) => Array.from(document.querySelectorAll(sel));

    // USPS's current page structure (as of 2026):
    //   - Overall status banner:         .tracking-progress-bar-status-text, .tb-status, .bannerhdr
    //   - Delivery date:                 .delivery_date, .tb-date, .day or similar
    //   - Tracking number:               .delivery_num
    //   - Scan events (history):         .tb-step, .tracking_history-inner, .tracking-history .history-step
    //
    // We try multiple selectors because USPS periodically rotates class names.
    // If none match, we return minimal data so caller can surface "no events".

    // Tracking number
    const trackingNumberEl =
      q(".delivery_num") ||
      q(".tb-num") ||
      q("[data-testid='tracking-number']");
    const trackingNumber = txt(trackingNumberEl);

    // Overall status (big text at top of card)
    const statusEl =
      q(".tracking-progress-bar-status-text") ||
      q(".tb-status-detail h3") ||
      q(".bannerhdr") ||
      q(".delivery-information-banner");
    const statusText = txt(statusEl) || "In Transit";

    // Delivery date / ETA
    const deliveryEl =
      q(".day") ||
      q(".month_year") ||
      q(".tb-date") ||
      q(".delivery_date") ||
      q(".banner_content .date");
    const deliveryRaw = txt(deliveryEl);

    // Scan history events
    const eventSelectors = [
      ".tb-step",
      ".tracking_history-inner .tracking_step",
      ".tracking-history .history-step",
      ".history_entry"
    ];

    let rawEvents = [];
    for (const sel of eventSelectors) {
      const found = qa(sel);
      if (found.length > 0) {
        rawEvents = found;
        break;
      }
    }

    const events = rawEvents.map(row => {
      // Try many field shapes
      const title =
        txt(row.querySelector(".tb-status-detail")) ||
        txt(row.querySelector(".tracking_event_status")) ||
        txt(row.querySelector(".step_status")) ||
        txt(row.querySelector("h3")) ||
        txt(row);

      const location =
        txt(row.querySelector(".tb-location")) ||
        txt(row.querySelector(".tracking_event_location")) ||
        txt(row.querySelector(".step_location")) ||
        txt(row.querySelector(".location")) || null;

      const dateTime =
        txt(row.querySelector(".tb-date")) ||
        txt(row.querySelector(".tracking_event_date")) ||
        txt(row.querySelector(".step_date")) ||
        txt(row.querySelector(".date_time")) ||
        null;

      return { title, location, at: dateTime };
    }).filter(e => e.title);

    // Also capture page title + URL for diagnostic purposes
    return {
      trackingNumber,
      statusText,
      deliveryRaw,
      events,
      pageTitle: document.title,
      pageUrl  : location.href
    };
  });
}

/** Find the CSS selector for the tracking card and return its bounding box. */
async function getCardBoundingBox(page) {
  return await page.evaluate(() => {
    // Try each known wrapper for USPS's tracking card
    const selectors = [
      ".tb-container",
      ".tracking_summary",
      ".tracking-number-detail-panel",
      ".banner_content",
      "#trackingResultsHeader"
    ];

    for (const sel of selectors) {
      const el = document.querySelector(sel);
      if (el) {
        const r = el.getBoundingClientRect();
        if (r.width > 100 && r.height > 100) {
          // Include a bit of breathing room
          return {
            x     : Math.max(0, Math.floor(r.x) - 16),
            y     : Math.max(0, Math.floor(r.y) - 16),
            width : Math.min(window.innerWidth, Math.ceil(r.width) + 32),
            height: Math.min(window.innerHeight, Math.ceil(r.height) + 32),
            selector: sel
          };
        }
      }
    }
    return null;
  });
}

/** Normalize extracted status text into our canonical status key. */
function normalizeStatusKey(statusText) {
  const s = String(statusText || "").toLowerCase();
  if (s.includes("deliver") && !s.includes("not deliver")) return "delivered";
  if (s.includes("out for delivery"))                       return "out_for_delivery";
  if (s.includes("in transit") || s.includes("moving"))     return "in_transit";
  if (s.includes("arrived") || s.includes("departed"))      return "in_transit";
  if (s.includes("pre-shipment") || s.includes("label created")) return "pre_shipment";
  if (s.includes("return"))                                 return "returned";
  if (s.includes("exception") || s.includes("alert") ||
      s.includes("attempt") || s.includes("notice"))        return "exception";
  return "in_transit";
}

/**
 * Main lookup function.
 *
 * @param {string} trackingCode
 * @returns {Promise<object>}  Normalized tracking result with imageBuffer
 */
async function lookup(trackingCode) {
  const code = String(trackingCode || "").trim();
  if (!code) {
    throw Object.assign(new Error("Missing tracking code"), { code: "INVALID_INPUT" });
  }

  const startedAt = Date.now();
  console.log(`[usps] looking up ${code}`);

  let browser;
  try {
    browser = await launchBrowser();
    const page = await browser.newPage();

    // Set a human UA so USPS doesn't serve a bot-blocked variant
    await page.setUserAgent(USER_AGENT);
    await page.setExtraHTTPHeaders({
      "Accept-Language": "en-US,en;q=0.9",
      "Accept"         : "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    });

    const url = USPS_URL(code);
    console.log(`[usps] navigating to ${url}`);

    const response = await page.goto(url, {
      waitUntil: "domcontentloaded",
      timeout  : PAGE_TIMEOUT
    });

    const httpStatus = response ? response.status() : 0;
    console.log(`[usps] page loaded, HTTP ${httpStatus}`);

    if (httpStatus >= 400) {
      throw Object.assign(
        new Error(`USPS returned HTTP ${httpStatus} for ${code}`),
        { code: "USPS_HTTP_ERROR" }
      );
    }

    // Wait for the tracking panel to appear. USPS's page lazy-loads some
    // content after initial DOMContentLoaded, so we need to wait.
    try {
      await page.waitForSelector(
        ".tb-container, .tracking_summary, .tracking-number-detail-panel, .banner_content",
        { timeout: SELECTOR_TIMEOUT }
      );
      console.log("[usps] tracking panel detected");
    } catch (e) {
      console.warn(`[usps] tracking panel selector timeout (${SELECTOR_TIMEOUT}ms): ${e.message}`);
      // Continue — maybe the page structure is slightly different, data
      // extraction below will surface a meaningful error.
    }

    // Brief settle for any remaining async renders (banners, map, etc.)
    await new Promise(r => setTimeout(r, 1500));

    // Extract structured data
    const data = await extractData(page);
    console.log(`[usps] extracted: status="${data.statusText}" events=${data.events.length} pageTitle="${data.pageTitle}"`);

    // If we got zero events AND no status, we likely hit a blocked/error page
    if (data.events.length === 0 && !data.statusText) {
      // Dump HTML for debugging if enabled
      let pageHtml = null;
      if (DEBUG_RETURN_HTML) {
        pageHtml = await page.content();
      }
      throw Object.assign(
        new Error(`USPS page had no tracking data — possibly blocked or tracking # not found`),
        { code: "USPS_NO_DATA", pageHtml, pageTitle: data.pageTitle }
      );
    }

    // Screenshot the tracking card
    const box = await getCardBoundingBox(page);
    let imageBuffer;
    let imageWidth, imageHeight;

    if (box) {
      console.log(`[usps] screenshotting card (selector=${box.selector}, ${box.width}x${box.height})`);
      imageBuffer = await page.screenshot({
        clip: { x: box.x, y: box.y, width: box.width, height: box.height },
        type: "png"
      });
      imageWidth = box.width;
      imageHeight = box.height;
    } else {
      console.log("[usps] no card bbox found — taking viewport screenshot");
      imageBuffer = await page.screenshot({ type: "png", fullPage: false });
      const vp = page.viewport();
      imageWidth = vp.width;
      imageHeight = vp.height;
    }

    const durationMs = Date.now() - startedAt;
    console.log(`[usps] complete in ${durationMs}ms (${imageBuffer.length} bytes)`);

    // Shape normalized result. imageBuffer is the screenshot — caller will
    // use this directly instead of rendering SVG.
    const statusKey = normalizeStatusKey(data.statusText);
    return {
      carrier          : "usps",
      carrierDisplay   : "USPS",
      trackingCode     : code,
      status           : data.statusText || "In Transit",
      statusKey,
      estimatedDelivery: data.deliveryRaw || null,
      destination      : data.events[0]?.location || null,
      origin           : data.events[data.events.length - 1]?.location || null,
      shipDate         : data.events[data.events.length - 1]?.at || null,
      resolvedAt       : statusKey === "delivered" ? data.events[0]?.at : null,
      events           : data.events.map(e => ({
        at       : e.at,
        title    : e.title,
        subtitle : null,
        location : e.location,
        status   : null
      })),
      // The magic field: a raw PNG buffer the caller will use as-is
      imageBuffer,
      imageWidth,
      imageHeight,
      imageMimeType    : "image/png",
      imageSource      : "usps_screenshot",
      durationMs
    };

  } finally {
    if (browser) {
      try { await browser.close(); console.log("[usps] browser closed"); }
      catch (e) { console.warn("[usps] error closing browser:", e.message); }
    }
  }
}

module.exports = { lookup };
