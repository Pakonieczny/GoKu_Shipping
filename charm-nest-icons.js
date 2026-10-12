/*  charm-nest-icons.js — the one line-icon set of the Charm Sorter (Paul, 12 Oct 2026: "strong iconography so a person can tie a
 *  fact to the thing it is about: who bought it, when, when it has to ship, the buyer's message").
 *  ═════════════════════════════════════════════════════════════════
 *  Pure strings, no DOM needed to build them (so the page, the node tests and a server can all use it).
 *    CharmNestIcons.svg(name, { size = 16, title = "", weight = 1.6, cls = "" })  → an inline <svg> string, or "" for an unknown name
 *    CharmNestIcons.has(name)  → true / false          CharmNestIcons.names → the frozen list of names
 *    CharmNestIcons.sizes      → { xs: 12, sm: 14, md: 16, lg: 20, xl: 24 }   (size may be a number of px or one of these keys)
 *  One grid and one hand: viewBox 0 0 24 24, no fill, stroke currentColor, 1.6 wide, round caps and joins, so an icon takes the colour of the
 *  text it sits in (and follows a light or dark theme with it), and the set sits next to the sorter's own search / mail / rail icons without
 *  restyling them. Every icon carries class "ic ic-<name>". Without a title the svg is aria-hidden (the words beside it say it); with a
 *  title it is role="img" with that label and a hover tooltip. Nothing here sets a colour; a few dots are zero-length strokes drawn wider
 *  (round caps), so no icon needs a fill.
 *  ═════════════════════════════════════════════════════════════════ */
(function (root, factory) {
  if (typeof module === "object" && module.exports) module.exports = factory();
  else root.CharmNestIcons = factory();
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

  const SIZES = Object.freeze({ xs: 12, sm: 14, md: 16, lg: 20, xl: 24 });
  const WEIGHT = 1.6;

  // The inside of each icon (paths only). A dot is `M x y h.01` drawn with a wider stroke-width; a dash is a stroke-dasharray.
  const PATHS = {
    // — the facts of an order —
    order: '<path d="M6 3.5h12v17l-2-1.4-2 1.4-2-1.4-2 1.4-2-1.4-2 1.4z"/><path d="M9.5 8.5h5M9.5 12.5h5"/>',
    buyer: '<circle cx="12" cy="8" r="3.7"/><path d="M4.8 20.5c.5-3.9 3.4-6.2 7.2-6.2s6.7 2.3 7.2 6.2"/>',
    purchased: '<rect x="3.5" y="5" width="17" height="15.5" rx="3"/><path d="M3.5 10h17M8 3v4M16 3v4"/>',
    shipby: '<path d="M5.5 17.5h-1A1.5 1.5 0 0 1 3 16V6.5A1.5 1.5 0 0 1 4.5 5h8A1.5 1.5 0 0 1 14 6.5v11M9.5 17.5h6"/><path d="M14 9h3.7l3.3 3.9V16a1.5 1.5 0 0 1-1.5 1.5"/><circle cx="7.5" cy="17.5" r="2"/><circle cx="17.5" cy="17.5" r="2"/>',
    message: '<path d="M20 6.5v7a2.5 2.5 0 0 1-2.5 2.5H11l-4 3.6V16h-.5A2.5 2.5 0 0 1 4 13.5v-7A2.5 2.5 0 0 1 6.5 4h11A2.5 2.5 0 0 1 20 6.5z"/><path d="M8.5 8.5h7M8.5 11.8h4"/>',
    note: '<path d="M14 3.5H7.5A2.5 2.5 0 0 0 5 6v12a2.5 2.5 0 0 0 2.5 2.5h9A2.5 2.5 0 0 0 19 18V8.5z"/><path d="M14 3.5v3.3A1.7 1.7 0 0 0 15.7 8.5H19M8.5 13h7M8.5 16.5h4"/>',
    metal: '<path d="M7 4h10l4 5.5-9 10.5-9-10.5z"/><path d="M3 9.5h18M9.5 4 8.3 9.5 12 20M14.5 4l1.2 5.5L12 20"/>',
    listing: '<path d="M3.5 12.2V6a2.5 2.5 0 0 1 2.5-2.5h6.2a2.5 2.5 0 0 1 1.8.7l6.8 6.8a2.5 2.5 0 0 1 0 3.5l-5.6 5.6a2.5 2.5 0 0 1-3.5 0l-6.8-6.8a2.5 2.5 0 0 1-.6-1.8z"/><circle cx="8.5" cy="8.5" r="1.2"/>',
    title: '<path d="M4 6.5h16M4 12h16M4 17.5h10"/>',
    quantity: '<path d="M12 3 20.5 7.6 12 12.2 3.5 7.6z"/><path d="M3.5 11.9 12 16.5l8.5-4.6M3.5 16.2 12 20.8l8.5-4.6"/>',
    pair: '<ellipse cx="6.6" cy="7.4" rx="1.4" ry="1.9"/><ellipse cx="17.4" cy="7.4" rx="1.4" ry="1.9"/><circle cx="6.6" cy="14.8" r="3.9"/><circle cx="17.4" cy="14.8" r="3.9"/><path d="M4.2 14.8a2.4 2.4 0 0 1 1.5-2.2M15 14.8a2.4 2.4 0 0 1 1.5-2.2"/>',
    piece: '<ellipse cx="12" cy="5" rx="1.9" ry="2.5"/><circle cx="12" cy="14.8" r="6.4"/><path d="M7.6 14.8a4.4 4.4 0 0 1 2.4-3.9"/>',
    mirror: '<path d="M9 6.5 3.5 18H9zM15 6.5 20.5 18H15z"/><path d="M12 3v18" stroke-dasharray="2 3"/>',
    state: '<path d="M3 12h4l2.6-7 4.8 14 2.6-7H21"/>',
    sheet: '<path d="M7 7V5.5A2 2 0 0 1 9 3.5h9.5a2 2 0 0 1 2 2V15a2 2 0 0 1-2 2H17.5"/><rect x="3.5" y="7" width="14" height="13.5" rx="2"/><circle cx="8.6" cy="12.6" r="1.9"/><rect x="11.4" y="14.4" width="3.4" height="3.4" rx=".9"/>',
    // — things you do with a fact —
    qr: '<rect x="3.5" y="3.5" width="6.5" height="6.5" rx="1.5"/><rect x="14" y="3.5" width="6.5" height="6.5" rx="1.5"/><rect x="3.5" y="14" width="6.5" height="6.5" rx="1.5"/><path d="M14 14h3v3h-3zM20.5 14v.01M17.5 20.5h3v-3.5"/>',
    link: '<path d="M10.5 13.5a4 4 0 0 0 5.7 0l3.3-3.3a4 4 0 0 0-5.7-5.7l-1 1"/><path d="M13.5 10.5a4 4 0 0 0-5.7 0l-3.3 3.3a4 4 0 0 0 5.7 5.7l1-1"/>',
    copy: '<rect x="8.5" y="8.5" width="12" height="12" rx="2.4"/><path d="M15.5 8.5V5.9a2.4 2.4 0 0 0-2.4-2.4H5.9a2.4 2.4 0 0 0-2.4 2.4v7.2a2.4 2.4 0 0 0 2.4 2.4h2.6"/>',
    check: '<path d="m4.5 12.5 4.8 4.8L19.5 7"/>',
    alert: '<path d="M10.5 4.7 2.9 18a1.7 1.7 0 0 0 1.5 2.5h15.2a1.7 1.7 0 0 0 1.5-2.5L13.5 4.7a1.7 1.7 0 0 0-3 0z"/><path d="M12 9.6v4.2M12 17.2v.01"/>',
    hold: '<circle cx="12" cy="12" r="9"/><path d="M10 9v6M14 9v6"/>',
    clock: '<circle cx="12" cy="12" r="9"/><path d="M12 7v5.2l3.2 2"/>',
    info: '<circle cx="12" cy="12" r="9"/><path d="M12 11v5.4M12 7.8v.01"/>',
    "chevron-down": '<path d="m6 9.5 6 6 6-6"/>',
    "chevron-right": '<path d="m9.5 6 6 6-6 6"/>',
    "chevron-up": '<path d="m6 14.5 6-6 6 6"/>',
    close: '<path d="M6 6l12 12M18 6 6 18"/>',
    more: '<path d="M5.5 12v.01M12 12v.01M18.5 12v.01" stroke-width="3.2"/>',
    // — kinds of order and of work —
    gift: '<rect x="3.5" y="8" width="17" height="4.5" rx="1.3"/><path d="M5 12.5V19a1.5 1.5 0 0 0 1.5 1.5h11A1.5 1.5 0 0 0 19 19v-6.5M12 8v12.5"/><path d="M12 8c-3 0-5-1.3-5-2.6 0-1.2 1-1.9 2-1.9 1.7 0 3 2 3 4.5zm0 0c3 0 5-1.3 5-2.6 0-1.2-1-1.9-2-1.9-1.7 0-3 2-3 4.5z"/>',
    engrave: '<path d="M8 3.5h8l1.8 11L12 21l-5.8-6.5z"/><path d="M12 21v-7.6"/><circle cx="12" cy="11.6" r="1.5"/>',
    custom: '<path d="M10 7.5Q10.7 13.3 16.5 14Q10.7 14.7 10 20.5Q9.3 14.7 3.5 14Q9.3 13.3 10 7.5z"/><path d="M18 3Q18.4 5.6 21 6Q18.4 6.4 18 9Q17.6 6.4 15 6Q17.6 5.6 18 3z"/>',
    cut: '<circle cx="6" cy="6" r="2.6"/><circle cx="6" cy="18" r="2.6"/><path d="M8 7.6 20.5 17.5M8 16.4 20.5 6.5"/>',
    flag: '<path d="M5.5 21V3.5"/><path d="M5.5 4.5c3.2-1.6 6.3 1.4 13-.4v8.2c-6.7 1.8-9.8-1.2-13 .4"/>',
    eye: '<path d="M2.5 12S6 5.5 12 5.5 21.5 12 21.5 12 18 18.5 12 18.5 2.5 12 2.5 12z"/><circle cx="12" cy="12" r="3"/>',
    lock: '<rect x="5" y="10.5" width="14" height="10" rx="2.4"/><path d="M8 10.5v-3a4 4 0 0 1 8 0v3M12 14.6v2.2"/>'
  };
  const NAMES = Object.freeze(Object.keys(PATHS));

  const has = name => typeof name === "string" && Object.prototype.hasOwnProperty.call(PATHS, name);
  const esc = s => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  /** A size as pixels: a positive number (kept to a quarter pixel), or a key of SIZES; anything else is the standard 16. */
  const px = s => {
    if (typeof s === "string" && Object.prototype.hasOwnProperty.call(SIZES, s)) return SIZES[s];
    const n = typeof s === "number" ? s : typeof s === "string" && s.trim() !== "" ? Number(s) : NaN;
    return Number.isFinite(n) && n > 0 && n <= 512 ? Math.round(n * 4) / 4 : SIZES.md;
  };

  function svg(name, opts) {
    if (!has(name)) return "";
    const o = opts && typeof opts === "object" ? opts : {};
    const size = px(o.size == null ? SIZES.md : o.size);
    const title = o.title == null ? "" : String(o.title).trim();
    const w = typeof o.weight === "number" && o.weight > 0 && o.weight <= 4 ? o.weight : WEIGHT;
    const cls = "ic ic-" + name + (typeof o.cls === "string" && /^[\w\- ]+$/.test(o.cls.trim()) && o.cls.trim() ? " " + o.cls.trim() : "");
    const a11y = title ? ' role="img" aria-label="' + esc(title) + '"' : ' aria-hidden="true" focusable="false"';
    return '<svg class="' + cls + '" width="' + size + '" height="' + size + '" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="' + w +
      '" stroke-linecap="round" stroke-linejoin="round"' + a11y + ">" + (title ? "<title>" + esc(title) + "</title>" : "") + PATHS[name] + "</svg>";
  }

  return Object.freeze({ svg, has, names: NAMES, sizes: SIZES, px });
});
