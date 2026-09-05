#!/usr/bin/env node
/*  scripts/investor/csp-hashes.js
 *  ---------------------------------------------------------------------------
 *  Recompute the per-block CSP hashes for the operator console and write them
 *  into the root netlify.toml (blueprint §13: strict Investor CSP with
 *  per-block hashes for the single inline script and style). Run after any
 *  edit to investor.html:  node scripts/investor/csp-hashes.js
 *  The deploy attestation (fixture console_csp_hashes_…) refuses a mismatch.
 * ---------------------------------------------------------------------------
 */
"use strict";
const fs = require("fs"), path = require("path"), crypto = require("crypto");
const root = path.resolve(__dirname, "..", "..");
const html = fs.readFileSync(path.join(root, "investor.html"), "utf8");
if ((html.match(/<script>/g) || []).length !== 1 || (html.match(/<style>/g) || []).length !== 1) throw new Error("investor.html must carry exactly one <script> and one <style> block");
const script = html.slice(html.indexOf("<script>") + "<script>".length, html.lastIndexOf("</script>"));
const style = html.slice(html.indexOf("<style>") + "<style>".length, html.indexOf("</style>"));
const h = (s) => `'sha256-${crypto.createHash("sha256").update(s, "utf8").digest("base64")}'`;
const csp = `default-src 'self'; script-src 'self' ${h(script)}; style-src 'self' ${h(style)}; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'`;
const tomlPath = path.join(root, "netlify.toml");
let toml = fs.readFileSync(tomlPath, "utf8");
const start = toml.indexOf('for = "/investor.html"');
if (start < 0) throw new Error("netlify.toml has no /investor.html header block");
const end = toml.indexOf("[[", start);
const block = toml.slice(start, end < 0 ? toml.length : end);
const line = `    Content-Security-Policy = "${csp}"\n`;
const next = /Content-Security-Policy\s*=\s*"[^"]*"\n/.test(block) ? block.replace(/    Content-Security-Policy\s*=\s*"[^"]*"\n/, line) : block.replace(/(X-Robots-Tag = "[^"]*"\n)/, `$1${line}`);
if (next === block) throw new Error("could not place the CSP line");
toml = toml.slice(0, start) + next + toml.slice(end < 0 ? toml.length : end);
fs.writeFileSync(tomlPath, toml);
console.log(`CSP updated: script ${h(script).slice(0, 24)}… style ${h(style).slice(0, 24)}…`);
