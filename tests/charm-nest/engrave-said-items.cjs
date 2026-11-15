// SMARTNOTE (Paul, 15 Nov 2026): the customer note itemized, links clickable, plain asks read by rules. Pure node: no network, no AI.
const assert = require('assert/strict'), path = require('path'), root = path.join(__dirname, '../..');
global.CharmNestOrders = require(path.join(root, 'charm-nest-orders.js'));
const Said = require(path.join(root, 'charm-nest-engrave-said.js'));
const IMG = 'https://i.etsystatic.com/ipf/6cf562/8887190291/ipf_fullxfull.8887190291_iotp8cok.jpg?version=0';
const row = (variations, spec) => ({ line: { variations: variations || [], personalization: [] }, spec: Object.assign({ options: [] }, spec) });

// the owner's example
{
  const r = row([{ name: 'Metal', value: 'Gold Writing-FRONT' }, { name: 'Send us your photo for handwriting here', value: IMG }]);
  const items = Said.itemsOf(r);
  assert.equal(items.length, 2); assert.deepEqual(items.map(i => i.kind), ['option', 'file']);
  assert.equal(items[1].label, 'Customer file'); assert.deepEqual(items[1].urls, [IMG]);
  const h = Said.html(r);
  assert.ok(h.includes('class="owSaid"') && h.includes('data-said'));
  assert.ok(h.includes(`<a class="owLink" href="${IMG}" target="_blank" rel="noopener noreferrer" referrerpolicy="no-referrer">`));
  assert.ok(h.includes('<img src="/.netlify/functions/imageProxy?url=' + encodeURIComponent(IMG).replace(/&/g, '&amp;') + '" loading="lazy" alt="">') || /<img src="\/\.netlify\/functions\/imageProxy\?url=[^"]+" loading="lazy" alt="">/.test(h), 'thumbnail (through the site\'s imageProxy)');
  assert.equal((h.match(/class="owI"/g) || []).length, 2, 'separate items');
  assert.ok(/Gold Writing-FRONT/.test(h));
}
// linkify: trailing punctuation, non-image link has no thumbnail, png by extension
{
  const h = Said.linkify('see https://example.com/a?x=1&y=2, thanks (https://example.com/p.png).');
  assert.ok(h.includes('href="https://example.com/a?x=1&amp;y=2" target'), h);
  assert.ok(h.includes('</a>, thanks'), h); assert.equal((h.match(/<img /g) || []).length, 0);   // (a picture that is not Etsy's cannot be drawn in the page: it stays a link)
  assert.ok(h.endsWith('</a>).'), h);
}
// XSS stays inert
{
  for (const bad of ['<script>alert(1)</script>', 'https://x.com/"onmouseover="alert(1)', 'javascript:alert(1)', 'data:text/html,<b>x</b>', '"><img src=x onerror=alert(1)>']) {
    const h = Said.linkify(bad);
    assert.ok(!/<script|<img src=x|<b>/i.test(h), bad + ' -> ' + h);
    assert.ok(!/ onmouseover=|href="javascript|href="data/i.test(h.replace(/&quot;onmouseover=/g, '')), bad + ' -> ' + h);
    assert.ok(!/<a [^>]*onmouseover/i.test(h));
    const full = Said.html(row([], { buyerMessage: bad }));
    assert.ok(!/<script|onerror=alert\(1\)>/.test(full.replace(/&lt;[^]*?&gt;/g, '')), 'html ' + bad);
  }
  assert.ok(!Said.linkify('javascript:alert(1)').includes('<a '));
  assert.equal(Said.itemsOf(row([], { buyerMessage: 'ftp://x.com javascript:alert(1)' }))[0].urls.length, 0);
}
// phrase table
{
  const ask = (t, cl) => Said.itemsOf(row([], { buyerMessage: t })).filter(i => i.kind === 'instruction').map(i => i.type + ':' + i.value);
  const a = ask('Please put the text on the left side in ALL CAPS, on one line, make it smaller');
  for (const x of ['position:left', 'case:all caps', 'layout:one line', 'size:small']) assert.ok(a.includes(x), x + ' in ' + a);
  assert.ok(ask('centered please').includes('position:centre'));
  assert.ok(ask('write it vertically').includes('layout:vertical'));
  assert.ok(ask('lowercase two lines').includes('case:lowercase') && ask('lowercase two lines').includes('layout:two lines'));
  assert.deepEqual(ask('I love my small dog, thank you so much'), [], 'small about a dog is not an ask');
  assert.ok(ask('engrave on the back please').includes('side:back'));
  const it = Said.itemsOf(row([], { buyerMessage: 'on the left' })).find(i => i.type === 'position');
  assert.equal(it.label, 'Asked'); assert.equal(it.text, 'left side');
  // applied
  const r = row([], { buyerMessage: 'on the left, all caps' });
  assert.ok(!/applied/.test(Said.html(r)), 'nothing said without opts.applied');
  const h = Said.html(r, { applied: [{ type: 'position', value: 'left', ok: true }, { type: 'case', value: 'all caps', ok: false, why: 'font has no lowercase' }] });
  assert.ok(h.includes('owOk">applied') && h.includes('could not apply: font has no lowercase'));
}
// personalisation lines itemized; rowsOf unchanged
{
  const r = row([{ name: 'Font', value: 'Angelina' }], { personalization: ['front - N', 'back - Name One'], buyerMessage: 'hi', staffNote: 'checked' });
  const items = Said.itemsOf(r);
  assert.equal(items.filter(i => i.kind === 'personalisation').length, 2);
  assert.deepEqual(Said.rowsOf(r), [['Personalisation', 'front - N\nback - Name One'], ['Buyer message', 'hi'], ['Staff note', 'checked'], ['Options', 'Font: Angelina']]);
  assert.equal(Said.html(row([])), '');
}
console.log('engrave-said-items: ok');
