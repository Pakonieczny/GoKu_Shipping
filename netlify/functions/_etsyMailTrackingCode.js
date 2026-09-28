/*  netlify/functions/_etsyMailTrackingCode.js
 *
 *  A label bought through USPS carries the full barcode: "420", the
 *  destination ZIP (5 or 9 digits), then the tracking number. Etsy stores
 *  that barcode as the tracking code, but USPS.com and customers only know
 *  the tracking number, and a customer given the barcode is told it is
 *  invalid. cleanTrackingCode returns the tracking number; any other code
 *  comes back as it was (spaces removed).
 */

"use strict";

// [ZIP digits, tracking-number digits], most common first: the 22-digit
// IMpb number (starting 92-95) behind a ZIP+4 or a 5-digit ZIP.
const SHAPES = [[9, 22], [5, 22], [5, 26], [9, 26], [5, 20], [9, 20]];

function cleanTrackingCode(code) {
  const s = String(code == null ? "" : code).replace(/\s+/g, "");
  if (!/^420\d+$/.test(s)) return s;
  for (const [zip, n] of SHAPES) {
    if (s.length !== 3 + zip + n) continue;
    const rest = s.slice(3 + zip);
    if (/^9[1-5]/.test(rest)) return rest;
  }
  return s;
}

/** Replace every USPS barcode in a text with its tracking number. */
function cleanTrackingCodesInText(text) {
  return String(text == null ? "" : text).replace(/\b420\d{25,31}\b/g, m => cleanTrackingCode(m));
}

module.exports = { cleanTrackingCode, cleanTrackingCodesInText };
