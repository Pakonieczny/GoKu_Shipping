// Paul, 5 Oct 2026: "I see the word 'lines' being used in place of 'Pieces' ... find every place the word 'lines' is
// being used and replace with 'Pieces'".
//
// This test reads the TEXT the person can see (string literals of the page scripts and of the server answers that
// reach the screen, the text and the title / aria-label / placeholder / alt of the pages) and fails when the word
// "line" or "lines" is left in it where it means an item of an order. It does not read code (a field called `lines`,
// a CSS class, a comment, a log that is not shown).
//
// Where the word is right it is listed in words-lines-allowlist.json, each with its reason:
//   · "phrases"  the legitimate uses (the green dash line, a line of engraving text, "one per line", a line sheet,
//                CSS names such as line-height or --line, "back in line", an SVG <line>)
//   · "literals" a whole string that is a name for code, or an AI prompt that is not shown to a person
//
// A new text that says "line" for an order's piece is a failure; say "piece" / "pieces" (or add a reason to the file).
// No dependency: a small lexer reads the scripts (checked against a real parser when this was written).
const fs = require('fs');
const path = require('path');
const assert = require('assert');

const ROOT = path.resolve(__dirname, '..', '..');
const ALLOW = JSON.parse(fs.readFileSync(path.join(__dirname, 'words-lines-allowlist.json'), 'utf8'));
const WORD = /\blines?\b/i;

// ── which files carry text a person reads ────────────────────────────────────────────────────────────────────────
function scopeFiles() {
  const out = [];
  const add = (dir, re) => { for (const n of fs.readdirSync(path.join(ROOT, dir))) if (re.test(n) && fs.statSync(path.join(ROOT, dir, n)).isFile()) out.push(path.posix.join(dir === '.' ? '' : dir, n)); };
  for (const [dir, src] of ALLOW.scope) add(dir, new RegExp(src));
  return out.filter(f => !ALLOW.outOfScope.files.includes(f)).sort();
}

// ── a small JavaScript lexer: the text of every string and template literal, with its line ──────────────────────
const KEYWORDS_BEFORE_REGEX = new Set(['return', 'typeof', 'case', 'do', 'else', 'in', 'of', 'void', 'delete', 'throw', 'new', 'instanceof', 'yield', 'await']);
function jsStrings(src, baseLine) {
  const out = [];
  let i = 0, line = baseLine || 1, last = '', lastWord = '';
  const tpl = [];                 // the templates whose ${ ... } is being read: { acc, at, depth }
  const n = src.length;
  const regexAllowed = () => last === '' || '(,=:[!&|?{};+-*%<>~^'.includes(last) || KEYWORDS_BEFORE_REGEX.has(lastWord);
  const push = (s, at) => out.push({ text: s, line: at });
  const esc = ch => (ch === 'n' || ch === 't' || ch === 'r' ? ' ' : ch);
  // A template literal is one text: what is written between its ${ } is the mark \u0001, so "line${n === 1 ? '' : 's'}"
  // reads "line\u0001" and a phrase that crosses an expression can still be recognised.
  function readTemplate(f) {      // i is just after the opening backtick (or the closing brace of a ${ })
    while (i < n) {
      const c = src[i];
      if (c === '\\') { f.acc += esc(src[i + 1]); if (src[i + 1] === '\n') line++; i += 2; continue; }
      if (c === '`') { i++; push(f.acc, f.at); return false; }
      if (c === '$' && src[i + 1] === '{') { i += 2; f.acc += '\u0001'; f.depth = 0; tpl.push(f); return true; }
      if (c === '\n') line++;
      f.acc += c; i++;
    }
    push(f.acc, f.at); return false;
  }
  while (i < n) {
    const c = src[i];
    if (c === '\n') { line++; i++; continue; }
    if (c === ' ' || c === '\t' || c === '\r') { i++; continue; }
    if (c === '/' && src[i + 1] === '/') { while (i < n && src[i] !== '\n') i++; continue; }
    if (c === '/' && src[i + 1] === '*') { i += 2; while (i < n && !(src[i] === '*' && src[i + 1] === '/')) { if (src[i] === '\n') line++; i++; } i += 2; continue; }
    if (c === '"' || c === "'") {
      let s = '', at = line; i++;
      while (i < n && src[i] !== c && src[i] !== '\n') { if (src[i] === '\\') { s += esc(src[i + 1]); i += 2; continue; } s += src[i++]; }
      i++; push(s, at); last = 'a'; lastWord = ''; continue;
    }
    if (c === '`') { i++; if (readTemplate({ acc: '', at: line, depth: 0 })) { last = '{'; lastWord = ''; } else { last = 'a'; lastWord = ''; } continue; }
    if (c === '/' && regexAllowed()) {      // a regular expression literal: skipped, its text is not a message
      i++; let cls = false;
      while (i < n && src[i] !== '\n') { if (src[i] === '\\') { i += 2; continue; } if (src[i] === '[') cls = true; else if (src[i] === ']') cls = false; else if (src[i] === '/' && !cls) break; i++; }
      i++; while (i < n && /[a-z]/i.test(src[i])) i++;
      last = 'a'; lastWord = ''; continue;
    }
    if (c === '{' && tpl.length) { tpl[tpl.length - 1].depth++; last = c; lastWord = ''; i++; continue; }
    if (c === '}' && tpl.length) {
      if (tpl[tpl.length - 1].depth === 0) { const f = tpl.pop(); i++; if (readTemplate(f)) { last = '{'; } else { last = 'a'; } lastWord = ''; continue; }
      tpl[tpl.length - 1].depth--; last = c; lastWord = ''; i++; continue;
    }
    if (/[A-Za-z_$]/.test(c)) { let j = i; while (j < n && /[\w$]/.test(src[j])) j++; lastWord = src.slice(i, j); last = 'a'; i = j; continue; }
    if (/[0-9]/.test(c)) { let j = i; while (j < n && /[\w.]/.test(src[j])) j++; i = j; last = 'a'; lastWord = ''; continue; }
    last = c; lastWord = ''; i++;
  }
  return out;
}

function textsOf(file) {
  const src = fs.readFileSync(path.join(ROOT, file), 'utf8');
  if (/\.js$|\.cjs$/.test(file)) return jsStrings(src, 1);
  const out = [];
  const lineAt = idx => src.slice(0, idx).split('\n').length;
  const blank = s => s.replace(/[^\n]/g, ' ');
  let markup = src.replace(/<!--[\s\S]*?-->/g, blank);
  const sre = /<script\b([^>]*)>([\s\S]*?)<\/script>/gi; let m;
  while ((m = sre.exec(markup))) {
    if (/type\s*=\s*["']?(application\/(ld\+)?json|text\/(template|html))/i.test(m[1])) continue;
    out.push(...jsStrings(m[2], lineAt(m.index + m[0].indexOf('>') + 1)));
  }
  markup = markup.replace(/<script\b[\s\S]*?<\/script>/gi, blank).replace(/<style\b[\s\S]*?<\/style>/gi, blank);
  const tre = />([^<>]+)</g;
  while ((m = tre.exec(markup))) out.push({ text: m[1].replace(/&nbsp;/g, ' '), line: lineAt(m.index) });
  const are = /\s([a-zA-Z:-]+)\s*=\s*("([^"]*)"|'([^']*)')/g;
  while ((m = are.exec(markup))) if (!/^(class|id|style|for|name|href|src|type|rel|d|viewBox|points)$/i.test(m[1])) out.push({ text: m[3] != null ? m[3] : m[4], line: lineAt(m.index) });
  return out;
}

// ── the judgement ─────────────────────────────────────────────────────────────────────────────────────────────────
const PHRASES = ALLOW.phrases.map(p => ({ re: new RegExp(p.pattern, 'gi'), why: p.why }));
const norm = s => s.replace(/\s+/g, ' ').trim();
function leftover(file, text) {
  if (!WORD.test(text)) return null;
  const t = norm(text);
  for (const l of ALLOW.literals) {
    if (l.file !== file) continue;
    if (l.exact != null && t === l.exact) return null;
    if (l.startsWith != null && t.startsWith(l.startsWith)) return null;
  }
  let rest = t;
  for (const p of PHRASES) rest = rest.replace(p.re, ' ');
  return WORD.test(rest) ? t : null;
}

function scan() {
  const found = [];
  for (const f of scopeFiles()) for (const s of textsOf(f)) { const bad = leftover(f, s.text); if (bad) found.push({ file: f, line: s.line, text: bad }); }
  return found;
}

module.exports = { jsStrings, scopeFiles, textsOf, leftover, scan };
if (require.main !== module) return;

// ── the checker checks itself first: it must flag a piece called a line, and let the right uses through ──────────
const probe = (text) => leftover('probe.js', text);
for (const bad of ["One of its other lines is still 'pooled'", '3 lines pulled', 'Skip line', 'This line has no SKU', '1 line · 2 orders', 'Line items', 'its line is completed', 'Previous line'])
  assert(probe(bad), 'the test must flag: ' + bad);
for (const good of ['The green dash line has not been calculated', 'Add the green dash line', 'Copy every order number, one per line', 'Line spacing', 'Engraving line count',
  'Shift+Enter new line', 'is back in line', 'the line sheet', 'a closed cut line', '.cnBox{line-height:1.3;border:1px solid var(--line,#ddd)}', '<line x1="0" y1="0"/>', 'B&W line art'])
  assert(!probe(good), 'the test must let through: ' + good);
// the lexer reads template literals with a nested ${ } and a regular expression before a quote
const lexed = jsStrings("const a = x.replace(/[\"']line/g, ''); const b = `n ${f(`inner line`)} lines`; const c = 'it\\'s a line';", 1).map(s => s.text);
assert(lexed.includes('inner line') && lexed.includes('n \u0001 lines') && lexed.includes("it's a line") && !lexed.some(t => /["']line/.test(t)), 'lexer: ' + JSON.stringify(lexed));

const files = scopeFiles();
assert(files.length > 40, 'the scope reaches the order pages, the stations and the server answers (' + files.length + ' files)');
for (const must of ['charm-nest-bridge.js', 'charm-nest-1.html', 'charm-nest-readiness.js', 'weld-1.html', 'shipping-1.html', 'assembly-1.html', 'netlify/functions/charmNestLibrary.js', 'netlify/functions/_orderTimeline.js'])
  assert(files.includes(must), 'in scope: ' + must);
const found = scan();
if (found.length) {
  console.error('The word "line" / "lines" is left in text a person reads (say "piece" / "pieces"; if it is right, add its reason to tests/charm-nest/words-lines-allowlist.json):');
  for (const f of found) console.error(`  ${f.file}:${f.line}  ${f.text.slice(0, 200)}`);
  process.exit(1);
}
console.log(`words-lines OK: ${files.length} files read, no order piece is called a line (${ALLOW.phrases.length} reasoned phrases, ${ALLOW.literals.length} reasoned literals)`);
