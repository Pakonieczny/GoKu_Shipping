#!/usr/bin/env node
/* Recompute the Content-Security-Policy hashes for investor.html.
   The page allows exactly one inline <script> and one inline <style>, each
   pinned by a sha256 hash in the CSP meta tag. Any edit to either block
   changes its hash and the browser refuses to run it, so run this after
   editing the page:  node scripts/investorCsp.js  */
const fs = require("fs"), path = require("path"), crypto = require("crypto");
const file = path.resolve(__dirname, "..", "investor.html");
let html = fs.readFileSync(file, "utf8");
const script = /<script>([\s\S]*?)<\/script>/.exec(html), style = /<style>([\s\S]*?)<\/style>/.exec(html);
if (!script || !style) throw new Error("investor.html must have one inline <script> and one inline <style>");
const sha = (s) => "sha256-" + crypto.createHash("sha256").update(s).digest("base64");
const meta = /(<meta http-equiv="Content-Security-Policy" content=")([^"]*)(")/.exec(html);
if (!meta) throw new Error("CSP meta tag not found");
const next = meta[2].replace(/script-src 'self' 'sha256-[^']+'/, "script-src 'self' '" + sha(script[1]) + "'").replace(/style-src 'self' 'sha256-[^']+'/, "style-src 'self' '" + sha(style[1]) + "'");
if (next === meta[2]) { console.log("CSP hashes already current"); process.exit(0); }
html = html.replace(meta[0], meta[1] + next + meta[3]);
fs.writeFileSync(file, html);
console.log("CSP hashes updated\n  script " + sha(script[1]) + "\n  style  " + sha(style[1]));
