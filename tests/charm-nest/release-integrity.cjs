// Release integrity of the site as netlify.toml publishes it (offline; reads files only, builds nothing):
//   1. every file a Charm Sorter page, worker or script loads by a literal path is in the public manifest of scripts/build-public.cjs
//      (a file missing there is a 404 on the live site, which the repo-root test servers never show);
//   2. every Netlify function the sorter's pages and scripts call exists as a staged endpoint (scripts/netlify-function-entries.json);
//   3. every relative require of a staged endpoint (and of what it requires) resolves, so the bundle cannot crash for a missing file;
//   4. every redirect in netlify.toml points at a published file or a staged endpoint.
//   node tests/charm-nest/release-integrity.cjs
const fs = require('node:fs'), path = require('node:path'), assert = require('node:assert/strict');
const root = path.join(__dirname, '../..');
const read = f => fs.readFileSync(path.join(root, f), 'utf8');
const assets = new Set(JSON.parse(read('scripts/build-public.cjs').match(/const assets = (\[[\s\S]*?\]);/)[1]));
const fontDirs = fs.readdirSync(path.join(root, 'vendor/fonts')).filter(n => /\.(otf|ttf)$/i.test(n)).map(n => 'vendor/fonts/' + n);   // (copied by the build's optional step)
const published = f => assets.has(f) || fontDirs.includes(f);
const man = JSON.parse(read('scripts/netlify-function-entries.json'));
const endpoints = new Set(man.endpoints.map(e => e.replace(/\.js$/, '')));
const bare = u => decodeURIComponent(u.replace(/[?#].*$/, '')).replace(/^\.\//, '').replace(/^\//, '');
const isLocal = u => u && !/^(https?:|\/\/|data:|blob:|mailto:|#|javascript:)/i.test(u) && !u.startsWith('/.netlify/') && !/[${}]/.test(u);   // (a template placeholder is built at run time, not a path)

// 1. pages, workers and scripts of the sorter
const sorterJs = fs.readdirSync(root).filter(f => /^(charm-nest-.*\.js|station-.*\.js|qr-piece-label\.js|send-flight\.js|cn-poll\.js|order-timeline\.js)$/.test(f) && assets.has(f));
const missing = [];
for (const page of ['charm-nest-1.html', 'charm-nest-check.html']) {
  assert(assets.has(page), page + ' is not published');
  const html = read(page);
  for (const m of html.matchAll(/<(?:script|link|img|iframe)\b[^>]*?\b(?:src|href)\s*=\s*"([^"]+)"/g)) if (isLocal(m[1]) && !published(bare(m[1]))) missing.push(page + ' -> ' + m[1]);
}
for (const f of ['charm-nest-1.html', ...sorterJs]) {
  const text = read(f);
  for (const m of text.matchAll(/(?:new\s+Worker|importScripts)\(\s*(?:["'][^"']*["']\s*,\s*)*["']([^"']+\.js[^"']*)["']/g)) if (isLocal(m[1]) && !published(bare(m[1]))) missing.push(f + ' -> ' + m[1]);
  for (const m of text.matchAll(/importScripts\(([^)]*)\)/g)) for (const s of m[1].matchAll(/["']([^"']+)["']/g)) if (isLocal(s[1]) && !published(bare(s[1]))) missing.push(f + ' -> ' + s[1]);
  for (const m of text.matchAll(/\bimport\(\s*["']([^"']+)["']\s*\)/g)) if (isLocal(m[1]) && !published(bare(m[1]))) missing.push(f + ' -> ' + m[1]);
  for (const m of text.matchAll(/["'`]((?:\.\/)?(?:vendor|lib)\/[\w./-]+\.(?:js|mjs|json|ttf|otf|wasm))(?:\?[^"'`]*)?["'`]/g)) if (!published(bare(m[1]))) missing.push(f + ' -> ' + m[1]);
}
assert.deepEqual([...new Set(missing)], [], 'Loaded by the sorter but not in the public manifest (a 404 on the live site): ' + [...new Set(missing)].join(', '));

// 2. the functions the sorter calls
const called = new Map();
for (const f of ['charm-nest-1.html', 'charm-nest-check.html', ...sorterJs]) {
  const text = read(f);
  for (const re of [/\/\.netlify\/functions\/([A-Za-z][\w-]*)/g, /\$\{(?:FN|fnBase\(\))\}\/([A-Za-z][\w-]*)/g, /(?<![\w.])api(?:All)?\(\s*["'`]([A-Za-z][\w-]*)["'`]/g])
    for (const m of text.matchAll(re)) if (!called.has(m[1])) called.set(m[1], f);
}
called.delete('x');                                   // (cn-poll.js: the example in its comment)
called.delete('getSheet'); for (const op of ['roseGet', 'roseClaim', 'roseRelease', 'roseRecordCut', 'rosePlan', 'roseList']) called.delete(op);   // (charm-nest-rose-ui.js: api(op) is an op of charmNestLibrary, not a function)
called.delete('preview');                             // (charm-nest-1.html: `${base}/preview.png`, a storage path)
for (const must of ['charmNestLibrary', 'charmNestOutput', 'charmNestAsset', 'firebaseOrders', 'authGate']) assert(called.has(must), 'the scan no longer sees the call of ' + must);
const unknown = [...called].filter(([n]) => !endpoints.has(n)).map(([n, f]) => n + ' (' + f + ')');
assert.deepEqual(unknown, [], 'Called by the sorter but not a staged function: ' + unknown.join(', '));

// 3. relative requires of the staged endpoints
const seen = new Set(), broken = [];
const resolve = (from, spec) => { const base = path.resolve(path.dirname(from), spec); return [base, base + '.js', base + '.json', path.join(base, 'index.js')].find(c => { try { return fs.statSync(c).isFile(); } catch (_) { return false; } }); };
const visit = file => {
  if (seen.has(file) || file.endsWith('.json')) return; seen.add(file);
  for (const line of fs.readFileSync(file, 'utf8').split('\n')) {
    if (/^\s*(\*|\/\/|\/\*)/.test(line)) continue;      // a comment line
    for (const m of line.matchAll(/\brequire\(\s*["'](\.{1,2}\/[^"']+)["']\s*\)/g)) { const t = resolve(file, m[1]); if (!t) broken.push(path.relative(root, file) + ' requires ' + m[1]); else visit(t); }
  }
};
for (const e of man.endpoints) visit(path.join(root, 'netlify/functions', e));
assert.deepEqual(broken, [], 'A staged function requires a file that is not there (the bundle would not build): ' + broken.join('; '));

// 4. redirects
const toml = read('netlify.toml'), badRedirect = [];
for (const m of toml.matchAll(/\[\[redirects\]\]([\s\S]*?)(?=\n\[\[|\n\[[a-z]|$)/g)) {
  const to = (m[1].match(/^\s*to\s*=\s*"([^"]+)"/m) || [])[1], from = (m[1].match(/^\s*from\s*=\s*"([^"]+)"/m) || [])[1];
  if (!to || /^https?:/.test(to)) continue;
  const fn = to.match(/^\/\.netlify\/functions\/([\w-]+)/);
  if (fn ? !endpoints.has(fn[1]) : !assets.has(bare(to))) badRedirect.push(from + ' -> ' + to);
}
// (etsy-mail-2 points at a page that was never committed: an older, separate matter of the mail inbox; the sorter's own are all there)
assert.deepEqual(badRedirect.filter(r => !/etsy-mail-2/.test(r)), [], 'Redirect to a file or function that is not published');
assert(toml.includes('from = "https://brites-charm-sorter.goldenspike.app"') && assets.has('charm-nest-1.html'));

console.log(`Release integrity OK: ${sorterJs.length} sorter scripts + 2 pages load only published files; ${called.size} functions called all staged; ${seen.size} files of ${man.endpoints.length} endpoints' require graphs resolve`);
