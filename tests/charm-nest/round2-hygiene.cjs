/* Round 2 release hygiene for the sorter page (static, no browser): the mistakes eleven workers can make at the same time that only show
   once the page is live (round2.md "Hard rules").
     1. every local script / stylesheet charm-nest-1.html loads exists, is listed in scripts/build-public.cjs (or it 404s live) and parses;
     2. a script or stylesheet whose content changed since the round started has a new ?v= tag in the page (or browsers keep the old file);
     3. a new function endpoint (netlify/functions/<name>.js, not a _helper) is listed in scripts/netlify-function-entries.json;
     4. no id="..." is written twice in the page's own markup beyond what the round started with.
   BASE is the commit the round started from (R2_BASE, default b80193f3); without it in the clone the ?v= / new-file checks are skipped.
     node tests/charm-nest/round2-hygiene.cjs */
const fs = require('fs'), path = require('path'), vm = require('vm'), { execSync } = require('child_process');
const root = path.join(__dirname, '../..');
const BASE = process.env.R2_BASE || 'b80193f3';
const git = cmd => execSync('git ' + cmd, { cwd: root, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'], maxBuffer: 1 << 26 });
let haveBase = true; try { git(`cat-file -e ${BASE}^{commit}`); } catch (_) { haveBase = false; }
const fails = [], notes = [];
const fail = m => { fails.push(m); console.log('FAIL ' + m); }, ok = m => console.log('ok   ' + m);
const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
const refs = [...html.matchAll(/<(?:script|link)\b[^>]*?\b(?:src|href)="([^"#]+?)(\?[^"]*)?"[^>]*>/g)].map(m => ({ file: m[1], q: m[2] || '', tag: m[0] })).filter(r => !/^(https?:)?\/\//.test(r.file) && !r.file.startsWith('data:') && /\.(js|css|png|json|mjs)$/.test(r.file));
const build = fs.readFileSync(path.join(root, 'scripts/build-public.cjs'), 'utf8');

// 1
for (const r of refs) {
  const p = path.join(root, r.file);
  if (!fs.existsSync(p)) { fail(`charm-nest-1.html loads ${r.file}, which does not exist`); continue; }
  if (!build.includes(`"${r.file}"`) && !build.includes(`'${r.file}'`)) fail(`${r.file} is loaded by charm-nest-1.html but is not listed in scripts/build-public.cjs (it would 404 live)`);
  if (/\.js$/.test(r.file) && !/type="module"/.test(r.tag)) { try { new vm.Script(fs.readFileSync(p, 'utf8'), { filename: r.file }); } catch (e) { fail(`${r.file} does not parse: ${e.message}`); } }
}
if (!fails.length) ok(`${refs.length} local scripts / styles of charm-nest-1.html exist, are in build-public.cjs and parse`);

// 2 + 3 + 4
if (haveBase) {
  const baseHtml = git(`show ${BASE}:charm-nest-1.html`);
  const baseRefs = new Map([...baseHtml.matchAll(/<(?:script|link)\b[^>]*?\b(?:src|href)="([^"#]+?)(\?[^"]*)?"[^>]*>/g)].map(m => [m[1], m[2] || '']));
  const changed = new Set(git(`diff --name-only ${BASE} HEAD`).split('\n').filter(Boolean));
  const before = fails.length;
  for (const r of refs) {
    if (!changed.has(r.file)) continue;
    if (!baseRefs.has(r.file)) continue;                                  // (a new file: its tag is its first)
    if (baseRefs.get(r.file) === r.q) fail(`${r.file} changed since ${BASE} but its ?v= tag in charm-nest-1.html is still "${r.q}"`);
  }
  if (fails.length === before) ok(`every changed script / style has a new ?v= tag (${[...changed].filter(f => refs.some(r => r.file === f)).length} changed)`);
  const added = git(`diff --name-only --diff-filter=A ${BASE} HEAD -- netlify/functions`).split('\n').filter(Boolean).map(f => path.basename(f));
  const entries = new Set(JSON.parse(fs.readFileSync(path.join(root, 'scripts/netlify-function-entries.json'), 'utf8')).endpoints || []);
  const b3 = fails.length;
  for (const f of added) if (!f.startsWith('_') && /\.js$/.test(f) && !entries.has(f)) fail(`new function ${f} is not in scripts/netlify-function-entries.json`);
  if (fails.length === b3) ok(`new functions since ${BASE}: ${added.filter(f => !f.startsWith('_')).join(', ') || 'none'}; all listed`);
  const dups = h => { const m = {}; for (const x of h.matchAll(/\sid="([^"]+)"/g)) m[x[1]] = (m[x[1]] || 0) + 1; return new Set(Object.keys(m).filter(k => m[k] > 1)); };
  const was = dups(baseHtml), now = dups(html), fresh = [...now].filter(k => !was.has(k));
  if (fresh.length) fail(`id written twice in charm-nest-1.html's markup: ${fresh.join(', ')}`); else ok(`no new doubled id in the page's markup (${now.size} that were there before)`);
} else notes.push(`base ${BASE} not in this clone: the ?v=, new-function and doubled-id checks were skipped`);
for (const n of notes) console.log('note ' + n);
if (fails.length) { console.log(`\nround2-hygiene: ${fails.length} problem(s)`); process.exitCode = 1; } else console.log('\nround2-hygiene OK');
