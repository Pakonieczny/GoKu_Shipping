// Reads opportunity-card markup the way a browser would, tag by tag, so a test can prove that hostile text stayed text.
// Every "<" must start a well-formed tag from the vocabulary the cards use, no attribute is an event handler, every link and
// picture is a plain https address, and no style attribute reaches for a resource. Not a sanitizer: a check on the page's own output.
const assert = require('node:assert/strict');
const TAGS = new Set('div span b i em small p ul ol li a h3 h4 header section details summary article label input button select option table thead tbody tr th td caption pre br svg path circle rect line polyline polygon ellipse g img'.split(' '));
const VOID = new Set('br input img path circle rect line polyline polygon ellipse'.split(' '));

function tags(markup) {
  const re = /<(\/?)([a-zA-Z][a-zA-Z0-9]*)((?:\s+[a-zA-Z_:][-a-zA-Z0-9_:.]*(?:\s*=\s*(?:"[^"]*"|'[^']*'|[^\s"'=<>]+))?)*)\s*\/?>/y, found = [];
  for (let i = markup.indexOf('<'); i >= 0; i = markup.indexOf('<', i + 1)) {
    re.lastIndex = i; const m = re.exec(markup);
    assert(m, 'a stray "<" in the markup near: ' + markup.slice(i, i + 70));
    const attrs = {};
    for (const a of m[3].matchAll(/([a-zA-Z_:][-\w:.]*)(?:\s*=\s*("[^"]*"|'[^']*'|[^\s"'=<>]+))?/g)) attrs[a[1].toLowerCase()] = (a[2] || '').replace(/^["']|["']$/g, '');
    found.push({ name: m[2].toLowerCase(), closing: !!m[1], attrs });
  }
  return found;
}
function safeMarkup(markup, label) {
  const at = label || 'markup', open = [];
  for (const t of tags(markup)) {
    assert(TAGS.has(t.name), at + ': unexpected element <' + t.name + '>');
    for (const [k, v] of Object.entries(t.attrs)) {
      assert(!/^on/.test(k), at + ': event handler ' + k);
      if (k === 'href' || k === 'src') assert(/^https:\/\/[^\s"'<>]+$/.test(v.replace(/&(amp|quot);/g, '')), at + ': unsafe ' + k + ' ' + v);
      if (k === 'style') assert(!/url\s*\(|expression|javascript:|@import/i.test(v), at + ': a style reaches for a resource: ' + v);
    }
    if (VOID.has(t.name)) continue;
    if (!t.closing) open.push(t.name);
    else assert.equal(open.pop(), t.name, at + ': </' + t.name + '> closes something that is not open');
  }
  assert.deepEqual(open, [], at + ': elements left open');
}
// The visible words of a piece of markup.
const text = markup => markup.replace(/<[^>]+>/g, ' ').replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/\s+/g, ' ').trim();
// Each part must appear, in this reading order, after the one before it.
function inOrder(markup, parts, label) {
  let last = -1;
  for (const p of parts) {
    const at = markup.indexOf(p, last + 1);
    assert(at > last, (label || 'card') + ': "' + p + '" is missing or out of order');
    last = at;
  }
}
module.exports = { safeMarkup, tags, text, inOrder };
