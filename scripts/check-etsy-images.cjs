// Build guard: no page may hand an Etsy CDN picture straight to the browser.
//
// Every page is served with Cross-Origin-Embedder-Policy: require-corp (netlify.toml, for "/*"). Under it the browser
// refuses a cross-origin <img>, background-image or fetch whose server sends no Cross-Origin-Resource-Policy header,
// and Etsy's image CDN (i.etsystatic.com) does not send one reliably. The result is a listing whose picture is
// silently missing for no visible reason (console: ERR_BLOCKED_BY_RESPONSE.NotSameOriginAfterDefaultedToSameOriginByCoep).
// It hit the Assembly, Shipping and Welding stations in October 2026.
//
// The fix is always the same: show the picture through our own same-origin imageProxy function, which sends
// Cross-Origin-Resource-Policy itself (or through etsyMailImage for a mirrored one).
//
// Two rules, both cheap and with no false positives on the repository as it stands:
//   1. A line that puts an Etsy picture address into a picture sink (<img, .src =, setAttribute("src"), background)
//      must name the proxy on that line.
//   2. A page that reads Etsy picture addresses at all must name the proxy somewhere. A new page copied from an old
//      one that never needed pictures cannot quietly start showing raw ones.
// It cannot see an address that is stored in a variable and used far away from where it was read; the page
// that does that must still go through the proxy, and a reviewer has to look. The escape hatch for a line that is
// genuinely fine (a link someone clicks, never loaded as a picture) is the comment  etsy-image-ok  on that line.
const fs = require('node:fs');
const path = require('node:path');

const ETSY_PICTURE = /\burl_(?:fullxfull|570xN|340x270|170x135|75x75)\b|etsystatic\.com|\bil_(?:fullxfull|570xN|340x270|170x135|75x75)\b/;
const SINK = /<img\b|\.src\s*=|setAttribute\(\s*["']src["']|background(?:-image)?\s*[:=]/;
const PROXY = /imageProxy|etsyPictureSrc|etsyCdnSrc|etsyMailImage|etsyMailMirrorImage|etsy-image-ok/;

function scan(name, text) {
  const found = [];
  let reads = false;
  const lines = text.split('\n');
  lines.forEach((line, i) => {
    if (!ETSY_PICTURE.test(line)) return;
    reads = true;
    if (SINK.test(line) && !PROXY.test(line)) {
      found.push({ file: name, line: i + 1, rule: 1, text: line.trim().slice(0, 160) });
    }
  });
  if (reads && !PROXY.test(text)) {
    found.push({ file: name, line: 0, rule: 2, text: 'reads Etsy picture addresses but never mentions imageProxy' });
  }
  return found;
}

// Public pages and the scripts they load live in the repository root and in assets/. Server code (netlify/),
// tests, docs and vendored libraries are not pages.
function publicFiles(root) {
  const out = [];
  for (const dir of ['', 'assets']) {
    const base = path.join(root, dir);
    if (!fs.existsSync(base)) continue;
    for (const name of fs.readdirSync(base)) {
      if (!/\.(html|js)$/i.test(name)) continue;
      const file = path.join(base, name);
      if (fs.lstatSync(file).isFile()) out.push(path.join(dir, name));
    }
  }
  return out;
}

function check(root) {
  return publicFiles(root).flatMap(rel => scan(rel, fs.readFileSync(path.join(root, rel), 'utf8')));
}

function explain(problems) {
  return [
    'An Etsy picture would reach the browser without going through imageProxy, so it is blocked by the site-wide',
    'Cross-Origin-Embedder-Policy: require-corp and the listing shows no picture (see scripts/check-etsy-images.cjs):',
    ...problems.map(p => '  ' + p.file + (p.line ? ':' + p.line : '') + '  [rule ' + p.rule + ']  ' + p.text),
    'Use "/.netlify/functions/imageProxy?url=" + encodeURIComponent(address), as etsyPictureSrc() does in assembly-1.html.'
  ].join('\n');
}

module.exports = { scan, check, publicFiles, explain };

if (require.main === module) {
  const problems = check(path.resolve(__dirname, '..'));
  if (problems.length) { console.error(explain(problems)); process.exit(1); }
  console.log('Etsy pictures: every page that shows one goes through imageProxy.');
}
