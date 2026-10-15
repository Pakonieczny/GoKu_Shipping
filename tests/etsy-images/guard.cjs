// The Etsy-picture build guard (scripts/check-etsy-images.cjs): it must flag the October 2026 bug, leave the fixes
// alone, and find nothing in the repository as it stands.
const assert = require('node:assert');
const path = require('node:path');
const { scan, check } = require('../../scripts/check-etsy-images.cjs');

const rules = (name, text) => scan(name, text).map(p => p.rule).join(',');

// The exact line that hid pictures on the Assembly, Shipping and Welding stations.
const bug = `cell.innerHTML = '<img src="' + images[0].url_fullxfull + '" alt="x">';`;
assert.strictEqual(rules('a.html', bug), '1,2', 'raw Etsy address in an <img> is flagged by both rules');
assert.strictEqual(rules('a.html', `img.src = item.url_570xN;`), '1,2', 'a .src assignment is a sink too');
assert.strictEqual(rules('a.html', `el.style.background = "url(" + x.url_170x135 + ")";`), '1,2', 'a CSS background is a sink too');
assert.strictEqual(rules('a.html', `<img src="https://i.etsystatic.com/1/r/il/a/2/il_fullxfull.2_x.jpg">`), '1,2', 'a literal CDN address');

// Through the proxy: fine.
const fixed = `cell.innerHTML = '<img src="' + etsyPictureSrc(images[0].url_fullxfull) + '" alt="x">';\n` +
  `function etsyPictureSrc(u){ return "/.netlify/functions/imageProxy?url=" + encodeURIComponent(u); }`;
assert.strictEqual(rules('a.html', fixed), '', 'proxied picture passes');
assert.strictEqual(rules('a.html', `img.src = "/.netlify/functions/imageProxy?url=" + encodeURIComponent(i.url_570xN);`), '');
assert.strictEqual(rules('a.html', `img.src = window.etsyCdnSrc(a.url_170x135);`), '', 'the mail page helper counts (an undefined helper is a loud ReferenceError, not a silent miss)');

// Reading an address without drawing it is not a violation, so long as the page knows about the proxy.
assert.strictEqual(rules('a.html', `const u = first?.url_fullxfull; fetch(FN("imageProxy") + "?url=" + u);`), '');
// A page that reads addresses and has no proxy anywhere is rule 2 only.
assert.strictEqual(rules('a.html', `const u = first?.url_fullxfull;`), '2');
// Pages with nothing to do with Etsy pictures are untouched.
assert.strictEqual(rules('a.html', `<img src="/assets/logo.png"> img.src = dataUrl;`), '');
// The escape hatch is explicit and on the line.
assert.strictEqual(rules('a.html', `a.href = x.url_fullxfull; // etsy-image-ok: a link, never loaded as a picture\nimg.src = "imageProxy";`), '');

// The repository as it stands.
const problems = check(path.resolve(__dirname, '../..'));
assert.deepStrictEqual(problems, [], 'no page in the repository shows a raw Etsy picture:\n' + problems.map(p => `${p.file}:${p.line} ${p.text}`).join('\n'));

console.log('etsy-images guard: all checks passed');
