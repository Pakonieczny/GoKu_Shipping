/* The tracking picture (etsyMailTrackingImage) is asked about every time, as before, but a browser that holds the current version is
   told so (304) and nothing is downloaded from Storage; the doc is read for its picture address alone. Offline: the real handler,
   a fake Firestore and a fake download. */
const assert = require('node:assert/strict'), Module = require('node:module'), path = require('node:path');
const file = path.join(__dirname, '../../netlify/functions/etsyMailTrackingImage.js');
let downloads = 0, reads = 0, masks = [], updateTime = { seconds: 1790000000, nanoseconds: 5 }, exists = true;
const png = Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]);
const fakes = {
  './firebaseAdmin': { firestore: () => ({ collection: () => ({ doc: id => ({ id }) }), getAll: async (ref, opts) => { reads++; masks.push(opts && opts.fieldMask); return [{ exists, updateTime: exists ? updateTime : undefined, data: () => ({ imageUrl: 'https://storage.example/t.png' }) }]; } }) },
  'node-fetch': async () => { downloads++; return { ok: true, status: 200, buffer: async () => png }; }
};
const load = Module._load; Module._load = function (request, ...rest) { return fakes[request] || load.call(this, request, ...rest); };
const { handler } = require(file); Module._load = load;
(async () => {
  const get = (headers = {}) => handler({ httpMethod: 'GET', queryStringParameters: { trackingCode: '9400111899223' }, headers });
  const first = await get();
  assert.equal(first.statusCode, 200); assert.equal(first.isBase64Encoded, true); assert.equal(downloads, 1);
  const tag = first.headers.ETag; assert(/^"tc-1790000000\.5"$/.test(tag), 'the version is the doc\'s own update time');
  assert.equal(first.headers['Cache-Control'], 'private, no-cache', 'asked about every time, never shared');
  assert.deepEqual(masks, [['imageUrl']], 'only the picture address is read from the doc');
  const same = await get({ 'if-none-match': tag });
  assert.equal(same.statusCode, 304); assert.equal(same.body, ''); assert.equal(downloads, 1, 'nothing is downloaded for a version the browser holds');
  assert.equal(same.headers.ETag, tag);
  updateTime = { seconds: 1790000100, nanoseconds: 9 };
  const changed = await get({ 'if-none-match': tag });
  assert.equal(changed.statusCode, 200); assert.equal(downloads, 2, 'a refreshed snapshot is downloaded again'); assert.notEqual(changed.headers.ETag, tag);
  exists = false;
  const gone = await get({ 'if-none-match': tag });
  assert.equal(gone.statusCode, 404); assert.match(gone.headers['Cache-Control'], /no-store/);
  console.log('Tracking image OK: 304 for a held version, one small read, a refreshed picture comes again');
})().catch(e => { console.error(e); process.exitCode = 1; });
