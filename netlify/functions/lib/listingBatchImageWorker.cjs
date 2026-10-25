'use strict';
// Child process of batch_submit (lib/listingBatchPrepare.cjs): checks and converts the reference
// images of ONE listing set, then exits. The image decoder's native memory is only ever returned to
// the system when the process that used it ends: in the collector itself every set left 60-150 MB
// behind, and the fourth or fifth set of a run went past Netlify's 1024 MB and the run was killed
// mid-preparation (2026-10-25, part102 and part125 of the 630-set run). Here it ends with the set.
//
// Message in:  { items: [{ file, out, contentType, path, role, canonical }] }
// Messages out: { ready, sharp }, then { i, ok, file, mime, from, action, detail } per item or
// { i, error: { message, inputImage, permanent } } at the first unusable file, then { done, maxRssMb }.
const fs = require('node:fs');

// If memory runs out anyway, the system stops this process and not the collector that started it.
try { fs.writeFileSync('/proc/self/oom_score_adj', '1000'); } catch (_) {}

let sharp = null;
for (const name of [process.env.LG1_SHARP_PATH, 'sharp']) {
  if (!name) continue;
  try { sharp = require(name); break; } catch (_) {}
}
const { normalizeBatchInputImage } = require('./listingBatchImages.cjs');

process.on('message', async (message) => {
  const items = Array.isArray(message?.items) ? message.items : [];
  for (const [i, item] of items.entries()) {
    try {
      const buf = fs.readFileSync(item.file);
      const img = await normalizeBatchInputImage(buf, { contentType: item.contentType, path: item.path,
        role: item.role, canonical: item.canonical === true, sharp });
      let file = item.file;
      if (img.buffer !== buf) { fs.writeFileSync(item.out, img.buffer, { mode: 0o600 }); file = item.out; }
      process.send({ i, ok: true, file, mime: img.mime, from: img.from, action: img.action, detail: img.detail });
    } catch (err) {
      process.send({ i, error: { message: String(err?.message || err), inputImage: err?.inputImage === true,
        permanent: err?.permanent === true } });
      break;
    }
  }
  // Its peak memory goes on the job record (prepCheckRssMb): what shows the margin left under the limit.
  process.send({ done: true, maxRssMb: Math.round(process.resourceUsage().maxRSS / 1024) }, () => process.exit(0));
});
process.send({ ready: true, sharp: !!sharp });
