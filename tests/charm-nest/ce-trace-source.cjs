// TRACESRC (Paul, 28 Oct 2026: "When I Press Image Trace on an existing already vectorized image, it does not open the actual chosen image"; his Re-trace dialog showed an empty
// preview with a broken "Vector" handle and "The picture this was traced from is not on this computer"). The picture a traced image was made from used to be kept only in the
// browser that traced it (IndexedDB, charm-nest-ce-image.js); now it is kept in the cloud too (charmNestLibrary.js traceSrcPut / traceSrcGet), named by its own hash, and its
// name travels in the image's own `group` name, which a piece's record and a design's version keep.
//   S  the server ops over the fake site's Storage (the real charmNestLibrary handler, bridge-server.cjs): a picture is stored by its own hash, once (the same bytes again are
//      answered, nothing written); a non-picture, the wrong kind, a key that is not its hash, too many bytes or too many pixels are refused; the sandbox's and the live
//      pictures are apart; neither op names anything in the changes feed (no placement counter, no Library revision)
//   A  "computer A" (one browser context: the editor harness over the fake site): a PNG dropped and placed with 3 levels and a crop; the picture is sent once, in the background,
//      as the very file chosen; the same picture placed again is not sent again; the image's name says where it is
//   B  "computer B" (a fresh context: empty IndexedDB, nothing in memory), the piece's parts as a saved record gives them back (flat, each with the image's name), and once more
//      grouped round with another part: Properties ▸ Image Trace… opens the step at once with the item's vector and "Fetching the picture…" (no split, no handles), then the
//      picture itself with its kept settings and crop; Object ▸ Image Trace ▸ Re-trace… and the right-click menu open the same step with the picture; Place re-traces it
//      where it is under the same name, and B sends nothing
//   C  no picture anywhere: (1) a name the cloud does not hold: the step shows the item's vector and asks for the picture; (2) a trace made before this (Paul's: the parts
//      name only their group's id, no settings): the vector shown, its levels read off its greys, the picture tools and the other views held, no handle; the file chosen,
//      the step works as ever and Place keeps the picture's name (already in the cloud: not sent again)
//   PW_DIR=/opt/node-tools/node_modules CHROMIUM=/opt/pw-browsers/chromium node tests/charm-nest/ce-trace-source.cjs      (SHOTS=<dir> keeps screenshots)
'use strict';
const path = require('node:path'), fs = require('node:fs'), crypto = require('node:crypto'), zlib = require('node:zlib');
const ROOT = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : evidence || ''}`.trimEnd()); };
const SHOTS = process.env.SHOTS || ''; if (SHOTS) fs.mkdirSync(SHOTS, { recursive: true });
const sha = b => crypto.createHash('sha256').update(b).digest('hex');
const sleep = ms => new Promise(r => setTimeout(r, ms));
const sameCrop = (a, b) => (!a && !b) || (!!a && !!b && a.every((v, i) => Math.abs(v - b[i]) < 0.002));

/* a real PNG (grey, w × h, f(x, y) → 0..255), made here */
const CRC = (() => { const t = new Int32Array(256); for (let n = 0; n < 256; n++) { let c = n; for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1; t[n] = c; } return b => { let c = -1; for (const x of b) c = t[(c ^ x) & 255] ^ (c >>> 8); return (c ^ -1) >>> 0; }; })();
function png(w, h, f) {
  const chunk = (type, data) => { const len = Buffer.alloc(4); len.writeUInt32BE(data.length); const td = Buffer.concat([Buffer.from(type, 'latin1'), data]); const c = Buffer.alloc(4); c.writeUInt32BE(CRC(td)); return Buffer.concat([len, td, c]); };
  const ihdr = Buffer.alloc(13); ihdr.writeUInt32BE(w, 0); ihdr.writeUInt32BE(h, 4); ihdr[8] = 8; ihdr[9] = 0; ihdr[10] = 0; ihdr[11] = 0; ihdr[12] = 0;
  const raw = Buffer.alloc((w + 1) * h); for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) raw[y * (w + 1) + 1 + x] = f(x, y);
  return Buffer.concat([Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]), chunk('IHDR', ihdr), chunk('IDAT', zlib.deflateSync(raw)), chunk('IEND', Buffer.alloc(0))]);
}
/* a JPEG's and a WebP's headers (the server reads the kind and size off them, nothing decoded) */
const jpegHead = (w, h) => Buffer.concat([Buffer.from([0xff, 0xd8, 0xff, 0xe0, 0, 16]), Buffer.from('JFIF\0\x01\x01\0\0\x01\0\x01\0\0', 'latin1'), Buffer.from([0xff, 0xc0, 0, 17, 8, h >> 8, h & 255, w >> 8, w & 255, 3, 1, 0x22, 0, 2, 0x11, 1, 3, 0x11, 1]), Buffer.alloc(40, 0x55), Buffer.from([0xff, 0xd9])]);
function webpLossless(w, h) {
  const b = Buffer.alloc(40); b.write('RIFF', 0, 'latin1'); b.writeUInt32LE(32, 4); b.write('WEBP', 8, 'latin1'); b.write('VP8L', 12, 'latin1'); b.writeUInt32LE(20, 16); b[20] = 0x2f;
  const v = ((w - 1) & 0x3fff) | (((h - 1) & 0x3fff) << 14); b.writeUInt32LE(v >>> 0, 21); return b;
}

async function partS(srv) {
  const st = srv.st, url = `${srv.sorterOrigin}/.netlify/functions/charmNestLibrary`;
  const call = async body => { const r = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }); return { status: r.status, feed: r.headers.get('x-cn-feed'), body: await r.json() }; };
  const pic = png(120, 80, (x, y) => (x + y) % 256), key = sha(pic).slice(0, 16), b64 = pic.toString('base64');
  const SB = `charmnest/sandbox/trace-src/${key}.png`, LIVE = `charmnest/trace-src/${key}.png`;
  const a = await call({ op: 'traceSrcPut', sandbox: true, key, ext: 'png', data: 'data:image/png;base64,' + b64, name: 'face photo.png', w0: 4000, h0: 3000 });
  const blob = st.blobs.get(SB), gen0 = blob && blob.generation;
  check(`S1 traceSrcPut keeps a PNG by its own hash (${a.body.file && a.body.file.path}), its kind, name, original size and SHA-256 with it, and a download address (${String(a.body.file && a.body.file.url).slice(0, 60)}…)`,
    a.status === 200 && a.body.ok && a.body.had === false && a.body.file.path === SB && !!blob && blob.buf.equals(pic) && blob.meta.contentType === 'image/png' && blob.meta.metadata.sha256 === sha(pic)
      && blob.meta.metadata.name === 'face photo.png' && a.body.file.w === 4000 && a.body.file.h === 3000 && /^https:\/\/firebasestorage\.googleapis\.com\/v0\/b\/test-bucket\/o\/charmnest%2Fsandbox%2Ftrace-src%2F/.test(a.body.file.url), JSON.stringify(a));
  const b = await call({ op: 'traceSrcPut', sandbox: true, key, ext: 'png', data: b64, name: 'other name.png' });
  const n = [...st.blobs.keys()].filter(k => k.includes('/trace-src/')).length;
  check(`S2 the same picture sent again is answered as kept (had), nothing written (${n} file, its generation ${st.blobs.get(SB).generation} as before)`,
    b.status === 200 && b.body.ok && b.body.had === true && b.body.file.path === SB && n === 1 && st.blobs.get(SB).generation === gen0 && st.blobs.get(SB).meta.metadata.name === 'face photo.png', JSON.stringify(b.body));
  const g = await call({ op: 'traceSrcGet', sandbox: true, key, ext: 'png' }), gl = await call({ op: 'traceSrcGet', key, ext: 'png' });
  const l = await call({ op: 'traceSrcPut', key, ext: 'png', data: b64 }), gl2 = await call({ op: 'traceSrcGet', key, ext: 'png' });
  check(`S3 the sandbox's pictures and the live ones are apart: the live world has none until one is sent to it (${l.body.file && l.body.file.path}); traceSrcGet answers each world's own`,
    g.body.file && g.body.file.path === SB && g.body.file.name === 'face photo.png' && gl.body.file === null && l.body.ok && l.body.had === false && l.body.file.path === LIVE && st.blobs.has(LIVE) && gl2.body.file && gl2.body.file.path === LIVE,
    JSON.stringify({ g: g.body, gl: gl.body, l: l.body }));
  const refused = [];
  const no = async (why, body) => { const r = await call(Object.assign({ op: 'traceSrcPut', sandbox: true }, body)); refused.push([why, r.status, r.body.error]); return r.status === 400 && !r.body.ok; };
  const text = Buffer.from('this is not a picture at all, only words'), jpg = jpegHead(640, 480), big = png(3200, 10, () => 0), webp = webpLossless(300, 200);
  const r1 = await no('words', { key: sha(text).slice(0, 16), ext: 'png', data: text.toString('base64') });
  const r2 = await no('a JPEG named png', { key: sha(jpg).slice(0, 16), ext: 'png', data: jpg.toString('base64') });
  const r3 = await no('a key not its hash', { key: '0123456789abcdef', ext: 'png', data: b64 });
  const r4 = await no('a bad key', { key: 'ABC', ext: 'png', data: b64 });
  const r5 = await no('a kind not kept', { key, ext: 'gif', data: b64 });
  const r6 = await no('3200 px', { key: sha(big).slice(0, 16), ext: 'png', data: big.toString('base64') });
  const huge = Buffer.concat([pic.subarray(0, 33), Buffer.alloc(4000001 - 33)]);
  const r7 = await no('4 MB and a byte', { key: sha(huge).slice(0, 16), ext: 'png', data: huge.toString('base64') });
  const okJ = await call({ op: 'traceSrcPut', sandbox: true, key: sha(jpg).slice(0, 16), ext: 'jpg', data: jpg.toString('base64') });
  const okW = await call({ op: 'traceSrcPut', sandbox: true, key: sha(webp).slice(0, 16), ext: 'webp', data: webp.toString('base64') });
  check(`S4 refused: words, a JPEG named PNG, a key that is not the picture's hash, a bad key, a GIF, 3200 px on a side, 4 MB and a byte (${refused.map(x => x[0] + ' ' + x[1]).join(', ')}); a JPEG (${okJ.body.file && okJ.body.file.w} × ${okJ.body.file && okJ.body.file.h}) and a WebP (${okW.body.file && okW.body.file.w} × ${okW.body.file && okW.body.file.h}) are kept`,
    r1 && r2 && r3 && r4 && r5 && r6 && r7 && okJ.body.ok && okJ.body.file.w === 640 && okJ.body.file.h === 480 && okW.body.ok && okW.body.file.w === 300 && okW.body.file.h === 200
      && [...st.blobs.keys()].filter(k => k.includes('/trace-src/')).length === 4, JSON.stringify(refused));
  check(`S5 neither op names anything in the changes feed (no placement counter, no Library revision): X-CN-Feed ${JSON.stringify([a.feed, g.feed])}`, !a.feed && !g.feed && !l.feed, JSON.stringify([a.feed, g.feed, l.feed]));
  // (the read goes through the same-origin asset door, as the page reads it)
  const asset = await fetch(`${srv.sorterOrigin}/.netlify/functions/charmNestAsset?url=${encodeURIComponent(a.body.file.url)}`);
  const got = Buffer.from(await asset.arrayBuffer());
  check(`S6 the kept picture is read back through charmNestAsset, the same bytes (${got.length})`, asset.status === 200 && got.equals(pic), String(asset.status));
  for (const k of [...st.blobs.keys()]) if (k.includes('/trace-src/')) st.blobs.delete(k);
  st.calls.length = 0;
}

/* ═══ the browser: the editor harness over the fake site, one context a computer ═══ */
const settle = page => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
const previewDone = page => page.waitForFunction(() => { const c = CharmNestCeImage.current(); return c && !c.working() && document.querySelector('.cei [data-wait]').hidden; }, null, { timeout: 30000 });
async function computer(browser, srv, opts = {}) {
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 }, reducedMotion: 'reduce' });
  await ctx.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => r.abort());
  if (opts.slowAsset) await ctx.route(/charmNestAsset/, async r => { await sleep(opts.slowAsset); r.continue(); });
  const page = await ctx.newPage(); page.__errs = [];
  page.on('pageerror', e => page.__errs.push('pageerror: ' + e.message));
  page.on('console', m => { if (m.type() === 'error' && !/Failed to load resource/.test(m.text())) page.__errs.push('console: ' + m.text()); });
  await page.goto(`${srv.sorterOrigin}/tests/charm-nest/fixtures/charm-editor-harness.html`);
  for (const f of ['charm-nest-vector.js', 'charm-nest-ce-ops.js', 'charm-nest-ce-extras.js', 'charm-nest-assets.js', 'charm-nest-ce-trace.js', 'charm-nest-ce-image.js']) await page.addScriptTag({ url: '/' + f });
  await page.evaluate(() => CharmNestCeImage.setHost({ sandbox: true, api: async (fn, body) => {
    const r = await fetch('/.netlify/functions/' + fn, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(Object.assign({}, body, { sandbox: true })) });
    let d = null; try { d = await r.json(); } catch (e) { d = null; }
    if (!r.ok) throw Object.assign(new Error((d && d.error) || 'HTTP ' + r.status), { status: r.status, answered: !!d, transient: r.status >= 500 });
    return d;
  } }));
  return page;
}
const open = async (page, extra) => { await page.evaluate(x => { window.openEditor(x || undefined); }, extra || null); await page.waitForSelector('.ce-root .ce-items path'); await settle(page); await settle(page); };
const shot = async (page, name) => { if (SHOTS) await page.screenshot({ path: path.join(SHOTS, name) }); };
/** What the step shows: its name line, the view, whether the split's handles are seen, its vector's paths, the note, Place, the settings. */
const stepState = page => page.evaluate(() => {
  const q = s => document.querySelector('.cei ' + s), seen = el => !!el && getComputedStyle(el).display !== 'none' && getComputedStyle(el).visibility !== 'hidden' && el.getBoundingClientRect().width > 0;
  const sheet = q('.cei__sheet'), r = sheet.getBoundingClientRect(), c = CharmNestCeImage.current();
  return { name: q('[data-name]').textContent, mode: sheet.dataset.mode, sheetSeen: !sheet.hidden && r.width > 40 && r.height > 20, handles: [...document.querySelectorAll('.cei .cei__div, .cei .cei__lab')].filter(seen).length,
    vecPaths: document.querySelectorAll('.cei__vec path').length, wait: q('[data-wait]').hidden ? '' : q('[data-wait]').textContent, note: q('[data-note]').textContent, place: !q('[data-do="place"]').disabled,
    off: [...document.querySelectorAll('.cei button[data-mode]')].filter(b => b.disabled).map(b => b.dataset.mode), tools: [...document.querySelectorAll('.cei .cei__row [data-do]')].filter(b => b.disabled).length,
    pick: !!q('[data-do="pick"]'), st: c && c.settings(), origInk: (() => { const cv = q('.cei__orig'); if (!cv.width) return 0; const d = cv.getContext('2d').getImageData(0, 0, cv.width, cv.height).data; let n = 0; for (let i = 0; i < d.length; i += 4) if (d[i] < 128) n++; return n; })() };
});
const puts = st => st.calls.filter(c => c.name === 'charmNestLibrary' && c.op === 'traceSrcPut').length;

async function partABC(browser, srv) {
  const st = srv.st;
  /* ── A ── */
  const A = await computer(browser, srv);
  await open(A);
  const logo = await A.evaluate(() => new Promise(res => {
    const cv = document.createElement('canvas'); cv.width = 240; cv.height = 160; const g = cv.getContext('2d'); g.fillStyle = '#fff'; g.fillRect(0, 0, 240, 160);
    g.fillStyle = '#000'; g.beginPath(); g.arc(70, 80, 45, 0, 7); g.fill(); g.fillStyle = 'rgb(128,128,128)'; g.fillRect(140, 40, 70, 70); g.fillStyle = 'rgb(200,200,200)'; g.fillRect(20, 132, 200, 18);
    cv.toBlob(b => { window.__logo = new File([b], 'logo.png', { type: 'image/png' }); b.arrayBuffer().then(a => res(Array.from(new Uint8Array(a)))); }, 'image/png');
  })).then(a => Buffer.from(a));
  const drop = page => page.evaluate(() => { const s = document.querySelector('.ce-root .ce-stage'), r = s.getBoundingClientRect(), dt = new DataTransfer(); dt.items.add(window.__logo); const at = { bubbles: true, cancelable: true, clientX: r.left + r.width / 2, clientY: r.top + r.height / 2, dataTransfer: dt }; s.dispatchEvent(new DragEvent('dragenter', at)); s.dispatchEvent(new DragEvent('dragover', at)); s.dispatchEvent(new DragEvent('drop', at)); });
  await drop(A); await A.waitForSelector('.ce-root .cei .cei__card', { timeout: 20000 }); await previewDone(A);
  await A.focus('.cei [data-k="levels"]'); await A.keyboard.press('ArrowLeft'); await previewDone(A);
  // a crop: a tenth off the right (white there)
  await A.click('.cei [data-do="crop"]'); await settle(A);
  { const h = await A.locator('.cei__crop i[data-h="se"]').boundingBox(), sh = await A.locator('.cei__sheet').boundingBox(); await A.mouse.move(h.x + h.width / 2, h.y + h.height / 2); await A.mouse.down(); await A.mouse.move(h.x + h.width / 2 - sh.width * 0.08, h.y + h.height / 2, { steps: 4 }); await A.mouse.up(); }
  await A.click('.cei [data-do="crop"]'); await previewDone(A);
  const stA = await A.evaluate(() => CharmNestCeImage.current().settings());
  await A.click('.cei [data-do="place"]'); await A.waitForFunction(() => !document.querySelector('.cei'), null, { timeout: 60000 }); await settle(A);
  await A.waitForFunction(() => CharmNestCeImage.cloudStats().puts >= 1 && !CharmNestCeImage.cloudStats().queued, null, { timeout: 30000 });
  const g1 = await A.evaluate(() => { const E = CharmEditor.current, id = E.selection()[0], g = E.items().find(i => i.id === id); return JSON.parse(JSON.stringify(g)); });
  const code1 = g1.group, src1 = code1.split(':')[0], ref1 = /^s([0-9a-f]{16})p[0-9a-z]{2}$/.exec(src1), path1 = ref1 && `charmnest/sandbox/trace-src/${ref1[1]}.png`, kept = path1 && st.blobs.get(path1);
  check(`A1 placed (${g1.children.length} greys, ${stA.levels} levels, crop ${stA.crop && stA.crop.map(v => v.toFixed(2)).join(',')}): the picture is sent once in the background, as the very file chosen (${kept && kept.buf.length} bytes, its hash ${ref1 && ref1[1]}), and the image's name says where (${src1}; trace.cloud ${JSON.stringify(g1.trace && g1.trace.cloud)})`,
    !!ref1 && !!kept && kept.buf.equals(logo) && sha(logo).slice(0, 16) === ref1[1] && puts(st) === 1 && g1.trace && g1.trace.cloud && g1.trace.cloud.key === ref1[1] && stA.levels === 3 && !!stA.crop && kept.meta.metadata.name === 'logo.png',
    JSON.stringify({ code1, n: puts(st), keys: [...st.blobs.keys()].filter(k => k.includes('trace-src')) }));
  // the same picture placed again: a second image, the same picture: nothing sent again
  await drop(A); await A.waitForSelector('.ce-root .cei .cei__card', { timeout: 20000 }); await previewDone(A);
  await A.click('.cei [data-do="place"]'); await A.waitForFunction(() => !document.querySelector('.cei'), null, { timeout: 60000 }); await settle(A); await sleep(600);
  const g2 = await A.evaluate(() => { const E = CharmEditor.current, id = E.selection()[0], g = E.items().find(i => i.id === id); return { id: g.id, group: g.group }; });
  const s2 = await A.evaluate(() => CharmNestCeImage.cloudStats());
  check(`A2 the same picture placed again is a second image (${g2.id}) of the same picture (${g2.group.split(':')[0].slice(0, 18)}…): not sent again (${puts(st)} put, ${st.blobs.size && [...st.blobs.keys()].filter(k => k.includes('trace-src')).length} file)`,
    g2.id !== g1.id && g2.group.split(':')[0].slice(0, 18) === src1.slice(0, 18) && puts(st) === 1 && s2.puts === 1 && !s2.queued && [...st.blobs.keys()].filter(k => k.includes('trace-src')).length === 1, JSON.stringify({ g2, s2 }));
  check('A3 no page error on computer A', A.__errs.length === 0, JSON.stringify(A.__errs).slice(0, 400));
  await A.context().close();

  /* the piece as a saved record gives it back: the image's parts flat, each with its name (charm-nest-art-edit.js: the record's `group`); and the same parts grouped with a heart */
  const flat = g1.children.map(c => Object.assign({}, c, { group: code1 }));
  /* ── B ── */
  const B = await computer(browser, srv, { slowAsset: 900 });
  await B.evaluate(() => new Promise(r => { const q = indexedDB.deleteDatabase('cn-ce-image'); q.onsuccess = q.onerror = q.onblocked = () => r(); }));
  await open(B, null);
  await B.evaluate(items => { const E = CharmEditor.current; E.ctx.addItems(items, { select: false }); E.ctx.commit('test: the piece as saved'); }, flat);
  await settle(B);
  const ids = flat.map(c => c.id);
  await B.evaluate(id => CharmEditor.current.ctx.select([id]), ids[0]); await settle(B); await settle(B);
  const btn = B.locator('.ce-root [data-aim="x:imageRetrace"]:visible').first();
  const btnSeen = await btn.count();
  await btn.click();
  await B.waitForSelector('.ce-root .cei .cei__card', { timeout: 20000 }); await settle(B);
  const l0 = await stepState(B);
  await shot(B, 'tracesrc-B-fetching.png');
  await B.waitForFunction(() => /^logo\.png/.test(document.querySelector('.cei [data-name]').textContent), null, { timeout: 30000 }); await previewDone(B);
  const l1 = await stepState(B);
  await shot(B, 'tracesrc-B-picture.png');
  check(`B1 computer B (empty IndexedDB), the piece's parts as saved: Properties ▸ Image Trace… opens the step at once with the item's vector (${l0.vecPaths} greys, the ${l0.mode} view, ${l0.handles} handles) and "${l0.wait}", Place held`,
    btnSeen > 0 && l0.mode === 'vec' && l0.sheetSeen && l0.handles === 0 && l0.vecPaths === flat.length && /Fetching the picture/.test(l0.wait) && /Fetching the picture/.test(l0.note) && !l0.place && l0.off.join() === 'split,orig,grey' && l0.tools === 6,
    JSON.stringify(l0));
  check(`B2 … then the picture itself from the cloud (${l1.name}), with its kept settings (${l1.st.levels} levels, crop ${l1.st.crop && l1.st.crop.map(v => v.toFixed(2)).join(',')}), the split and its handles back, Place ready`,
    /^logo\.png · 240 × 160 px$/.test(l1.name) && l1.st.levels === 3 && sameCrop(l1.st.crop, stA.crop) && l1.mode === 'split' && l1.handles >= 2 && l1.origInk > 500 && l1.place && !l1.off.length && !l1.tools && !l1.pick,
    JSON.stringify(l1));
  await B.click('.cei [data-do="cancel"]'); await B.waitForFunction(() => !document.querySelector('.cei'));
  // Object ▸ Image Trace ▸ Re-trace…
  await B.evaluate(() => CharmEditor.current.ai.closeMenus());
  await B.click('.ce-menubar .ce-mb__t:text-is("Object")'); await settle(B);
  await B.hover('.ce-aimenu:not([hidden]) .ce-aimi--sub:has(.ce-aimi__l:text-is("Image Trace"))'); await B.waitForTimeout(450); await settle(B);
  await B.locator('.ce-aimenu:not([hidden]) .ce-aimi:visible:has(.ce-aimi__l:text-is("Re-trace…"))').last().click();
  await B.waitForSelector('.ce-root .cei .cei__card', { timeout: 20000 }); await previewDone(B);
  const m1 = await stepState(B);
  await B.click('.cei [data-do="cancel"]'); await B.waitForFunction(() => !document.querySelector('.cei'));
  // the right-click menu, on the part
  const at = await B.evaluate(id => { const E = CharmEditor.current, b = E.box(id); return E.ctx.clientOf((b[0] + b[2]) / 2, (b[1] + b[3]) / 2); }, ids[ids.length - 1]);
  await B.evaluate(id => CharmEditor.current.ctx.select([id]), ids[ids.length - 1]); await settle(B);
  await B.mouse.click(at.x, at.y, { button: 'right' }); await settle(B);
  const ctxKind = await B.evaluate(() => { const m = document.querySelector('.ce-aictx'); return m && m.dataset.ctxkind; });
  await B.hover('.ce-aimenu:not([hidden]) .ce-aimi--sub:has(.ce-aimi__l:text-is("Image Trace"))'); await B.waitForTimeout(450); await settle(B);
  await B.locator('.ce-aimenu:not([hidden]) .ce-aimi:visible:has(.ce-aimi__l:text-is("Re-trace…"))').last().click();
  await B.waitForSelector('.ce-root .cei .cei__card', { timeout: 20000 }); await previewDone(B);
  const r1 = await stepState(B);
  check(`B3 Object ▸ Image Trace ▸ Re-trace… (${m1.name}, ${m1.st.levels} levels) and the right-click menu (${ctxKind}: ${r1.name}) open the same step with the picture`,
    /^logo\.png/.test(m1.name) && m1.st.levels === 3 && m1.place && m1.mode === 'split' && ctxKind === 'image' && /^logo\.png/.test(r1.name) && r1.st.levels === 3 && r1.place && sameCrop(r1.st.crop, stA.crop), JSON.stringify({ m1, r1, ctxKind }));
  await B.click('.cei [data-do="place"]'); await B.waitForFunction(() => !document.querySelector('.cei'), null, { timeout: 60000 }); await settle(B); await sleep(500);
  const re = await B.evaluate(ids => { const E = CharmEditor.current, g = E.items().find(i => i.id === E.selection()[0]); return { kind: g && g.kind, kids: g && g.children.length, group: g && g.group, left: E.items().filter(i => ids.includes(i.id)).length, cs: CharmNestCeImage.cloudStats() }; }, ids);
  check(`B4 Re-trace on computer B: ONE group (${re.kids} greys) in the parts' place, under the same picture's name (${String(re.group).split(':')[0]}); B fetched the picture once and sent nothing (${puts(st)} put in all)`,
    re.kind === 'group' && re.left === 0 && String(re.group).split(':')[0] === src1 && re.cs.fetched === 1 && re.cs.puts === 0 && puts(st) === 1, JSON.stringify(re));
  // the parts grouped with another part (⌘G round them and the heart): still its picture
  await B.evaluate(() => { const E = CharmEditor.current, g = E.items().find(i => i.id === E.selection()[0]); E.ctx.select([g.id, 'heart-fill']); });
  await B.keyboard.press('Control+g'); await settle(B);
  const outer = await B.evaluate(() => { const E = CharmEditor.current, id = E.selection()[0], g = E.items().find(i => i.id === id); return { id, kind: g && g.kind, n: g && g.children && g.children.length }; });
  await B.evaluate(() => { CharmNestCeImage.sources().clear(); });
  await B.evaluate(() => { CharmEditor.commands.imageRetrace.run(CharmEditor.current.ctx); });
  await B.waitForSelector('.ce-root .cei .cei__card', { timeout: 20000 }); await B.waitForFunction(() => /^logo\.png/.test(document.querySelector('.cei [data-name]').textContent), null, { timeout: 30000 });
  const gr = await stepState(B);
  await B.click('.cei [data-do="cancel"]'); await B.waitForFunction(() => !document.querySelector('.cei'));
  check(`B5 grouped with another part (${outer.kind} of ${outer.n}), Re-trace still opens its picture (${gr.name}) with its settings (${gr.st.levels} levels)`, outer.kind === 'group' && outer.n === 2 && /^logo\.png/.test(gr.name) && gr.st.levels === 3, JSON.stringify({ outer, gr }));
  check('B6 no page error on computer B', B.__errs.length === 0, JSON.stringify(B.__errs).slice(0, 400));
  await B.context().close();

  /* ── C · no picture anywhere ── */
  // (1) a name the cloud does not hold (its file gone)
  const kept1 = st.blobs.get(path1); st.blobs.delete(path1);
  const C1 = await computer(browser, srv);
  await open(C1);
  await C1.evaluate(items => { const E = CharmEditor.current; E.ctx.addItems(items, { select: false }); E.ctx.select([items[0].id]); }, flat); await settle(C1);
  await C1.evaluate(() => { CharmEditor.commands.imageRetrace.run(CharmEditor.current.ctx); });
  await C1.waitForSelector('.ce-root .cei .cei__card', { timeout: 20000 });
  await C1.waitForFunction(() => !!document.querySelector('.cei [data-do="pick"]'), null, { timeout: 30000 }); await settle(C1);
  const n1 = await stepState(C1);
  await shot(C1, 'tracesrc-C-nowhere.png');
  check(`C1 the picture's name is in the parts but the cloud has no such picture: the item's vector (${n1.vecPaths} greys, ${n1.handles} handles), "${n1.note.slice(0, 60)}…", Choose the picture…, Place held`,
    n1.mode === 'vec' && n1.sheetSeen && n1.handles === 0 && n1.vecPaths === flat.length && /not in the cloud/.test(n1.note) && n1.pick && !n1.place && n1.st.levels === 3, JSON.stringify(n1));
  await C1.click('.cei [data-do="cancel"]');
  await C1.context().close();
  st.blobs.set(path1, kept1);
  // (2) Paul's: a trace made before its settings and picture were kept: the parts name only their group's id
  const legacy = g1.children.map((c, i) => Object.assign({}, c, { id: 'cemvlegacy' + i, group: 'cemvlegacygrp' }));
  const C2 = await computer(browser, srv);
  await open(C2);
  await C2.evaluate(items => { const E = CharmEditor.current; E.ctx.addItems(items, { select: false }); E.ctx.select([items[0].id]); }, legacy); await settle(C2);
  const pb = C2.locator('.ce-root [data-aim="x:imageRetrace"]:visible').first(); await pb.click();
  await C2.waitForSelector('.ce-root .cei .cei__card', { timeout: 20000 }); await settle(C2); await settle(C2);
  const n2 = await stepState(C2);
  await shot(C2, 'tracesrc-C-legacy.png');
  const gets0 = st.calls.filter(c => c.op === 'traceSrcGet').length;
  check(`C2 a trace from before (its parts name only their group): the item's vector shown (${n2.vecPaths} greys, the ${n2.mode} view, no handle), its levels read off its greys (${n2.st.levels}), the other views (${n2.off.join(', ')}) and the picture tools (${n2.tools}) held; "${n2.note.slice(0, 70)}…"`,
    n2.mode === 'vec' && n2.sheetSeen && n2.handles === 0 && n2.vecPaths === legacy.length && n2.st.levels === 3 && n2.off.join() === 'split,orig,grey' && n2.tools === 6 && /not kept anywhere/.test(n2.note) && n2.pick && !n2.place && !n2.wait.includes('Fetching'),
    JSON.stringify(n2));
  // the file chosen: the step works as ever; Place keeps the picture's name, already in the cloud: not sent again
  await C2.setInputFiles('.cei [data-pick]', { name: 'logo.png', mimeType: 'image/png', buffer: logo });
  await C2.waitForFunction(() => /^logo\.png/.test(document.querySelector('.cei [data-name]').textContent), null, { timeout: 20000 }); await previewDone(C2);
  const n3 = await stepState(C2);
  await C2.click('.cei [data-do="place"]'); await C2.waitForFunction(() => !document.querySelector('.cei'), null, { timeout: 60000 }); await settle(C2);
  await C2.waitForFunction(() => !CharmNestCeImage.cloudStats().queued && !CharmNestCeImage.cloudStats().busy, null, { timeout: 30000 }); await sleep(300);
  const placed = await C2.evaluate(() => { const E = CharmEditor.current, g = E.items().find(i => i.id === E.selection()[0]); return { group: g && g.group, kind: g && g.kind, cs: CharmNestCeImage.cloudStats() }; });
  check(`C3 the picture chosen: the split back with its handles (${n3.handles}), Place ready; placed under the picture's own name (${String(placed.group).split(':')[0]}), which the cloud already holds: asked (${placed.cs.gets}), not sent again (${puts(st)} put in all)`,
    n3.mode === 'split' && n3.handles >= 2 && n3.place && !n3.pick && placed.kind === 'group' && String(placed.group).split(':')[0].slice(0, 18) === src1.slice(0, 18) && placed.cs.had >= 1 && placed.cs.puts === 0 && puts(st) === 1 && st.calls.filter(c => c.op === 'traceSrcGet').length > gets0,
    JSON.stringify({ n3, placed }));
  check('C4 no page error on the computers without the picture', C1.__errs.length === 0 && C2.__errs.length === 0, JSON.stringify(C1.__errs.concat(C2.__errs)).slice(0, 400));
  await C2.context().close();
}

(async () => {
  const srv = await start({ receipts: [] });
  try {
    await partS(srv);
    const pwDir = process.env.PW_DIR || path.join(ROOT, 'node_modules');
    let chromium = null; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { chromium = null; }
    if (!chromium) console.log('SKIP the browser parts: no playwright-core');
    else {
      const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium', args: ['--no-sandbox'] });
      try { await partABC(browser, srv); } finally { await browser.close(); }
    }
  } finally { srv.close(); }
  const bad = RESULTS.filter(r => !r.ok);
  console.log(`\n${RESULTS.length - bad.length}/${RESULTS.length} checks pass`);
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
