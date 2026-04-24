/* netlify/functions/_etsyMailTrackingRender.js
 *
 * Shared SVG → PNG timeline renderer for carrier tracking results.
 *
 * Input: the normalized tracking result from _etsyMailCarriers (usps.js or
 * chitchats.js). Output: both an SVG string and a PNG buffer.
 *
 * Visual design — intentionally close to the USPS tracking page that users
 * already recognize, but rendered with our OWN code (no copyrighted assets,
 * no carrier branding reproduced). Shown as a vertical timeline:
 *
 *   ┌──────────────────────────────────────────────────────────┐
 *   │ Tracking: 4206 5248 8986 ...                             │
 *   │ ◉  Expected Delivery                                     │
 *   │ │   MONDAY, April 27 by 9:00 PM                          │
 *   │                                                          │
 *   │ ●  IN TRANSIT                                            │
 *   │ │    ● Arrived at USPS Regional Origin Facility          │
 *   │ │       Northwest Rochester NY Distribution Center       │
 *   │ │       April 23, 2026 · 8:02 pm                         │
 *   │                                                          │
 *   │      ● Accepted at USPS Origin Facility                  │
 *   │      │  Niagara Falls, NY 14304                          │
 *   │      │  April 23, 2026 · 6:47 pm                         │
 *   │ ...                                                      │
 *   │                                                          │
 *   │                     via USPS tracking                    │
 *   └──────────────────────────────────────────────────────────┘
 *
 * Width: 800px. Height: computed from number of events (min 400, max 2400).
 * Exports PNG at 2× scale for retina sharpness.
 *
 * Sharp (already in package.json as "sharp": "^0.33.5") handles the SVG→PNG
 * rasterization.
 */

const sharp = require("sharp");

// ─── Design tokens ──────────────────────────────────────────────────────
const WIDTH          = 800;
const HEADER_HEIGHT  = 140;
const EVENT_HEIGHT   = 88;     // vertical space per event row
const PADDING_X      = 40;
const FOOTER_HEIGHT  = 50;
const MAX_EVENTS     = 20;     // cap to keep image sane

// Palette — muted navy + soft accents, works for both USPS and Chit Chats
const COLOR_BG         = "#ffffff";
const COLOR_BG_ACCENT  = "#f4f7fb";
const COLOR_TEXT_DARK  = "#1e293b";
const COLOR_TEXT_MUTED = "#64748b";
const COLOR_PRIMARY    = "#1e40af";
const COLOR_TIMELINE   = "#cbd5e1";
const COLOR_SUCCESS    = "#16a34a";
const COLOR_WARNING    = "#f59e0b";
const COLOR_ERROR      = "#dc2626";

const statusColor = {
  delivered       : COLOR_SUCCESS,
  out_for_delivery: COLOR_PRIMARY,
  in_transit      : COLOR_PRIMARY,
  pre_shipment    : COLOR_TEXT_MUTED,
  exception       : COLOR_ERROR,
  returned        : COLOR_ERROR,
  rerouted        : COLOR_WARNING
};

// ─── XML escaping ───────────────────────────────────────────────────────
function esc(s) {
  return String(s || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

// ─── Date formatting ────────────────────────────────────────────────────
function fmtDate(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  if (isNaN(d.getTime())) return String(iso);
  return d.toLocaleDateString("en-US", {
    weekday: "long",
    month  : "long",
    day    : "numeric",
    year   : "numeric"
  });
}

function fmtDateShort(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  if (isNaN(d.getTime())) return String(iso);
  return d.toLocaleDateString("en-US", {
    month: "long",
    day  : "numeric",
    year : "numeric"
  });
}

function fmtTime(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  if (isNaN(d.getTime())) return "";
  return d.toLocaleTimeString("en-US", {
    hour  : "numeric",
    minute: "2-digit",
    hour12: true
  }).toLowerCase();
}

function fmtDateTime(iso) {
  const d = fmtDateShort(iso);
  const t = fmtTime(iso);
  if (d && t) return `${d} · ${t}`;
  return d || t || "";
}

// ─── Tracking code formatting ────────────────────────────────────────────
function formatTrackingCode(code) {
  // Insert spaces every 4 chars for readability (ignore for short codes)
  const s = String(code || "");
  if (s.length <= 10) return s;
  return s.replace(/(.{4})/g, "$1 ").trim();
}

// ─── Expected-delivery display ───────────────────────────────────────────
function formatExpectedDelivery(iso) {
  if (!iso) return null;
  const d = new Date(iso);
  if (isNaN(d.getTime())) return null;

  const weekday = d.toLocaleDateString("en-US", { weekday: "long" }).toUpperCase();
  const monthDay = d.toLocaleDateString("en-US", { month: "long", day: "numeric" });
  const time = d.toLocaleTimeString("en-US", { hour: "numeric", hour12: true });

  return {
    weekday,
    monthDay,
    time: time.includes("12 AM") ? null : time
  };
}

// ─── Text measurement (rough, for wrapping) ──────────────────────────────
function estimateWidth(text, fontSize) {
  // Heuristic: avg character width ≈ 0.55 × fontSize for sans-serif
  return Math.ceil(String(text).length * fontSize * 0.55);
}

function wrapText(text, maxWidth, fontSize) {
  const words = String(text || "").split(/\s+/);
  const lines = [];
  let current = "";
  for (const word of words) {
    const candidate = current ? `${current} ${word}` : word;
    if (estimateWidth(candidate, fontSize) > maxWidth && current) {
      lines.push(current);
      current = word;
    } else {
      current = candidate;
    }
  }
  if (current) lines.push(current);
  return lines;
}

// ─── SVG building blocks ────────────────────────────────────────────────
function buildHeader(tracking) {
  const code = formatTrackingCode(tracking.trackingCode);
  const statusCol = statusColor[tracking.statusKey] || COLOR_PRIMARY;
  const ed = formatExpectedDelivery(tracking.estimatedDelivery);

  let y = 32;
  const elements = [];

  // Title row — "Tracking Number"
  elements.push(`
    <text x="${PADDING_X}" y="${y}" font-family="Helvetica,Arial,sans-serif"
          font-size="12" font-weight="600" fill="${COLOR_TEXT_MUTED}"
          letter-spacing="1.5">TRACKING NUMBER</text>
  `);
  y += 22;

  elements.push(`
    <text x="${PADDING_X}" y="${y}" font-family="'Helvetica Neue',Helvetica,Arial,sans-serif"
          font-size="22" font-weight="700" fill="${COLOR_TEXT_DARK}">
      ${esc(code)}
    </text>
  `);
  y += 14;

  // Status pill (right side)
  const pillWidth = Math.min(220, estimateWidth(tracking.status, 13) + 32);
  const pillX = WIDTH - PADDING_X - pillWidth;
  elements.push(`
    <rect x="${pillX}" y="24" width="${pillWidth}" height="32" rx="16" fill="${statusCol}"/>
    <text x="${pillX + pillWidth / 2}" y="45" font-family="Helvetica,Arial,sans-serif"
          font-size="13" font-weight="600" fill="#ffffff" text-anchor="middle"
          letter-spacing="0.5">
      ${esc(tracking.status.toUpperCase())}
    </text>
  `);

  // Expected delivery row (if available)
  if (ed) {
    y += 26;
    elements.push(`
      <text x="${PADDING_X}" y="${y}" font-family="Helvetica,Arial,sans-serif"
            font-size="12" font-weight="600" fill="${COLOR_TEXT_MUTED}"
            letter-spacing="1.2">EXPECTED DELIVERY BY</text>
    `);
    y += 22;
    const timeSuffix = ed.time ? `  by  ${ed.time}` : "";
    elements.push(`
      <text x="${PADDING_X}" y="${y}" font-family="'Helvetica Neue',Helvetica,Arial,sans-serif"
            font-size="18" font-weight="600" fill="${COLOR_TEXT_DARK}">
        ${esc(ed.weekday)}, ${esc(ed.monthDay)}${esc(timeSuffix)}
      </text>
    `);
  } else if (tracking.destination) {
    y += 26;
    elements.push(`
      <text x="${PADDING_X}" y="${y}" font-family="Helvetica,Arial,sans-serif"
            font-size="13" fill="${COLOR_TEXT_MUTED}">
        Destination: ${esc(tracking.destination)}
      </text>
    `);
  }

  return {
    svg: elements.join("\n"),
    height: y + 20
  };
}

function buildEvent(event, x, y, isFirst, isLast, availableWidth) {
  const dotX = x + 16;
  const textX = x + 44;
  const dotColor = isFirst ? COLOR_PRIMARY : COLOR_TIMELINE;
  const dotRadius = isFirst ? 7 : 5;

  const titleLines = wrapText(event.title, availableWidth - 44, 15);
  let localY = y + 8;
  const elements = [];

  // Timeline dot
  elements.push(`
    <circle cx="${dotX}" cy="${localY + 6}" r="${dotRadius}"
            fill="${dotColor}"
            ${isFirst ? `stroke="${COLOR_PRIMARY}" stroke-width="3" fill="#ffffff"` : ""}/>
  `);

  // Title
  for (const line of titleLines) {
    elements.push(`
      <text x="${textX}" y="${localY + 12}" font-family="'Helvetica Neue',Helvetica,Arial,sans-serif"
            font-size="15" font-weight="${isFirst ? "700" : "600"}" fill="${COLOR_TEXT_DARK}">
        ${esc(line)}
      </text>
    `);
    localY += 20;
  }

  // Location
  if (event.location) {
    elements.push(`
      <text x="${textX}" y="${localY + 10}" font-family="Helvetica,Arial,sans-serif"
            font-size="13" fill="${COLOR_TEXT_DARK}">
        ${esc(event.location)}
      </text>
    `);
    localY += 18;
  }

  // Date/time
  const dateTime = fmtDateTime(event.at);
  if (dateTime) {
    elements.push(`
      <text x="${textX}" y="${localY + 10}" font-family="Helvetica,Arial,sans-serif"
            font-size="12" fill="${COLOR_TEXT_MUTED}">
        ${esc(dateTime)}
      </text>
    `);
    localY += 18;
  }

  // Vertical timeline connector line (except for last)
  if (!isLast) {
    const lineStartY = y + 20;
    const lineEndY = y + EVENT_HEIGHT + 4;
    elements.push(`
      <line x1="${dotX}" y1="${lineStartY}" x2="${dotX}" y2="${lineEndY}"
            stroke="${COLOR_TIMELINE}" stroke-width="2"/>
    `);
  }

  const actualHeight = Math.max(EVENT_HEIGHT, localY - y + 12);
  return {
    svg: elements.join("\n"),
    height: actualHeight
  };
}

function buildFooter(tracking, y) {
  const footerText = `via ${tracking.carrierDisplay} tracking`;
  const elements = [];

  // Separator line
  elements.push(`
    <line x1="${PADDING_X}" y1="${y}" x2="${WIDTH - PADDING_X}" y2="${y}"
          stroke="${COLOR_TIMELINE}" stroke-width="1" opacity="0.5"/>
  `);

  // Footer text (centered)
  elements.push(`
    <text x="${WIDTH / 2}" y="${y + 24}" font-family="Helvetica,Arial,sans-serif"
          font-size="12" font-weight="500" fill="${COLOR_TEXT_MUTED}"
          text-anchor="middle" letter-spacing="0.5">
      ${esc(footerText)}
    </text>
  `);

  return elements.join("\n");
}

// ─── Main SVG assembly ───────────────────────────────────────────────────
function buildSvg(tracking) {
  const events = (tracking.events || []).slice(0, MAX_EVENTS);

  const header = buildHeader(tracking);
  let currentY = header.height + 10;

  // Start-of-events separator
  const eventsStartY = currentY;
  currentY += 8;

  // Build each event (newest first)
  const eventSvgs = [];
  const availableWidth = WIDTH - PADDING_X * 2 - 16;
  for (let i = 0; i < events.length; i++) {
    const e = buildEvent(
      events[i],
      PADDING_X,
      currentY,
      i === 0,
      i === events.length - 1,
      availableWidth
    );
    eventSvgs.push(e.svg);
    currentY += e.height;
  }

  // Empty state if no events
  if (events.length === 0) {
    eventSvgs.push(`
      <text x="${WIDTH / 2}" y="${currentY + 40}"
            font-family="Helvetica,Arial,sans-serif" font-size="14" fill="${COLOR_TEXT_MUTED}"
            text-anchor="middle">
        No tracking events yet
      </text>
    `);
    currentY += 80;
  }

  currentY += 8;
  const footer = buildFooter(tracking, currentY);
  currentY += FOOTER_HEIGHT;

  const totalHeight = Math.max(400, Math.min(currentY + 12, 2400));

  const svg = `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${WIDTH} ${totalHeight}"
     width="${WIDTH}" height="${totalHeight}">
  <defs>
    <linearGradient id="headerGrad" x1="0%" y1="0%" x2="0%" y2="100%">
      <stop offset="0%" stop-color="${COLOR_BG_ACCENT}" stop-opacity="1"/>
      <stop offset="100%" stop-color="${COLOR_BG}" stop-opacity="1"/>
    </linearGradient>
  </defs>

  <!-- Background -->
  <rect x="0" y="0" width="${WIDTH}" height="${totalHeight}" fill="${COLOR_BG}"/>

  <!-- Header region background -->
  <rect x="0" y="0" width="${WIDTH}" height="${header.height + 10}"
        fill="url(#headerGrad)"/>

  <!-- Header content -->
  ${header.svg}

  <!-- Events -->
  ${eventSvgs.join("\n")}

  <!-- Footer -->
  ${footer}
</svg>`;

  return { svg, width: WIDTH, height: totalHeight };
}

/**
 * Render a tracking result to both SVG (string) and PNG (Buffer).
 *
 * Rendering notes:
 *   Sharp rasterizes SVG at its native viewBox size by default (72 DPI).
 *   For sharp output on retina/high-DPI displays we render at 2× density
 *   via the `density` option. This tells sharp's SVG engine (librsvg) to
 *   render at higher DPI FROM THE START, rather than rasterizing at base
 *   resolution and upscaling the bitmap (which would blur text).
 *
 *   density: 144 = 2× the default 72 DPI → produces a PNG at 2× the SVG
 *   viewBox dimensions (1600 × 2× height).
 *
 * @param {object} tracking  Normalized tracking result from _etsyMailCarriers
 * @returns {Promise<{svg: string, png: Buffer, width: number, height: number}>}
 */
async function render(tracking) {
  const { svg, width, height } = buildSvg(tracking);

  const png = await sharp(Buffer.from(svg, "utf-8"), {
    density: 144    // 2× default 72 DPI for crisp rendering at retina scale
  })
    .png({ compressionLevel: 9 })
    .toBuffer();

  return { svg, png, width, height };
}

module.exports = { render, buildSvg };
