// The app is "Brites Manager" (Paul, 9 Nov 2026: "Rename this application to "Brites Manager" ... The only thing that
// doesn't get renamed is the URL."). The built main page's tab title and sidebar logo say so, and no visible text node of
// its static HTML still carries the old names "Charm Nest" / "Charm Sorter".
//   node tests/charm-nest/app-name.cjs
'use strict';
const assert = require('assert');
const fs = require('fs');
const path = require('path');
const root = path.join(__dirname, '..', '..');
const built = path.join(root, 'public-site', 'charm-nest-1.html');
const file = fs.existsSync(built) ? built : path.join(root, 'charm-nest-1.html');
const html = fs.readFileSync(file, 'utf8');

const title = (/<title>([^<]*)<\/title>/.exec(html) || [])[1];
assert.strictEqual(title, 'Brites Manager', 'tab title: ' + title);
assert(/<div class="logo">Brites <b>Manager<\/b><\/div>/.test(html), 'sidebar logo reads Brites + Manager in the two-tone style');
assert(/document\.title = \(running \? "⏳ " : ""\) \+ "Brites Manager";/.test(html), 'document.title keeps the running prefix and says Brites Manager');

// static visible text: strip scripts, styles, comments and tags; also check visible attributes (title, placeholder, aria-label, value of visible inputs)
const noCode = html.replace(/<script\b[\s\S]*?<\/script>/gi, ' ').replace(/<style\b[\s\S]*?<\/style>/gi, ' ').replace(/<!--[\s\S]*?-->/g, ' ');
const text = noCode.replace(/<[^>]*>/g, '\n');
const OLD = /charm\s+nest|charm\s+sorter/i;
const bad = text.split('\n').map(s => s.trim()).filter(s => OLD.test(s));
assert.deepStrictEqual(bad, [], 'visible text nodes with the old name: ' + JSON.stringify(bad));
const attrs = [...noCode.matchAll(/\s(title|placeholder|aria-label|alt)="([^"]*)"/gi)].map(m => m[2]).filter(s => OLD.test(s));
assert.deepStrictEqual(attrs, [], 'visible attributes with the old name: ' + JSON.stringify(attrs));
console.log(`  ✓ ${path.relative(root, file)}: title and logo say Brites Manager; no visible "Charm Nest" / "Charm Sorter"`);
