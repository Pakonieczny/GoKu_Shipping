"use strict";

// How batch_submit now prepares a set's OpenAI input file (netlify/functions/lib/listingBatchPrepare.cjs).
// 2026-10-25: the 630-set refill sent 2-5 sets per ten-minute run, then the run was killed during the
// next set's "downloading reference images" (part102, then part125). Measured here on pictures of the
// live sizes (an 8000 px charm, 2048-5000 px templates): checking the references in the collector's own
// process left 60-150 MB of the image decoder's memory behind per set, 600 MB after one set and 950-990 MB
// after six, against Netlify's 1024 MB.
// Review fixes: a child killed before or after it is ready is an interruption (admission backs the set off,
// no strike) and never switches this process to checking in place; every byte of the JSONL is written.
// Checked:
//   1. a line written with appendJsonlLine is byte for byte JSON.stringify of the same line with the
//      base64 in it (sizes that are and are not multiples of 3, larger than one piece, a missing charm);
//   2. checkReferenceFiles in a child: conversions are written to their own files and are real PNGs, an
//      unusable file throws InputImageError (permanent) naming its path and role, a child that is killed
//      throws a passing error and is NOT redone in this process, a missing worker checks in this process;
//   3. this process's memory stays flat over twelve sets checked in children.
// Needs sharp (a dependency of the site); skipped where it is not installed.
// Usage: node tests/listing-batch/prepare-memory.cjs   (from the repository root)

const assert = require("node:assert/strict");
const fs = require("node:fs");
const fsp = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");

let sharp;
try { sharp = require("sharp"); }
catch { console.log("listing batch preparation: sharp is not installed here, skipped"); process.exit(0); }
const prepPath = path.resolve("netlify/functions/lib/listingBatchPrepare.cjs");
const { checkReferenceFiles, appendJsonlLine, imageToken, findImageWorker } = require(prepPath);
const { InputImageError } = require(path.resolve("netlify/functions/lib/listingBatchImages.cjs"));

const mb = (n) => Math.round(n / 1048576);
const quiet = { warn: () => {} };

async function lines(dir) {
  // Files of awkward sizes: one byte past a 3 MiB piece, an exact multiple of 3, tiny.
  const sizes = [3 * 1024 * 1024 * 2 + 1, 3 * 1000, 5];
  const files = [];
  for (const [i, n] of sizes.entries()) {
    const buf = Buffer.alloc(n);
    for (let k = 0; k < n; k++) buf[k] = (k * 31 + i * 7) & 255;
    const file = path.join(dir, `bytes_${i}`);
    await fsp.writeFile(file, buf);
    files.push({ file, buf });
  }
  const build = (ref, charm) => ({ custom_id: "s0_slot0", method: "POST", url: "/v1/images/edits",
    body: { prompt: "Keep the charm — exactly “as is”.\nLine two", images: [
      { image_url: `data:image/png;base64,${ref}` }, ...(charm ? [{ image_url: `data:image/webp;base64,${charm}` }] : [])] } });
  const out = path.join(dir, "out.jsonl");
  const fh = await fsp.open(out, "w");
  let written = 0, expected = "";
  for (const [a, b] of [[0, 1], [1, 2], [2, null]]) {
    const ta = imageToken(), tb = b === null ? null : imageToken();
    written += await appendJsonlLine(fh, build(ta, tb), [{ token: ta, file: files[a].file }, { token: tb, file: b === null ? null : files[b].file }]);
    expected += JSON.stringify(build(files[a].buf.toString("base64"), b === null ? null : files[b].buf.toString("base64"))) + "\n";
  }
  await fh.close();
  const got = await fsp.readFile(out);
  assert.equal(written, got.length, "the bytes written are counted");
  assert(got.equals(Buffer.from(expected, "utf8")), "streamed lines are exactly the lines with the images inline");
  // A line with no placeholder in it (a stubbed builder) is written as it is.
  const fh2 = await fsp.open(out, "w");
  await appendJsonlLine(fh2, { request: "test" }, [{ token: imageToken(), file: files[2].file }]);
  await fh2.close();
  assert.equal(await fsp.readFile(out, "utf8"), '{"request":"test"}\n');
}

async function picture(file, w, h, channels, fill) {
  await sharp({ create: { width: w, height: h, channels, background: fill } }).png().toFile(file);
}

async function childChecks(dir) {
  assert(findImageWorker(), "the worker is found next to the module");
  const charm = path.join(dir, "charm.png"), tpl = path.join(dir, "tpl.png"), html = path.join(dir, "page.png");
  await picture(charm, 8000, 8000, 4, { r: 200, g: 160, b: 70, alpha: 0.5 });
  await picture(tpl, 1024, 1024, 3, { r: 20, g: 40, b: 60 });
  await fsp.writeFile(html, "<!DOCTYPE html><html><body>not found</body></html>");
  const item = (file, role, n) => ({ file, out: path.join(dir, `out_${n}`), contentType: "image/png",
    path: `listing-generator-1/${role === "Charm image" ? "Charm_Maker/New_Charms" : "Regular_Necklace/Templates"}/${path.basename(file)}`,
    role, canonical: role === "Charm image" });

  const { results, mode, childRssMb } = await checkReferenceFiles([item(charm, "Charm image", 0), item(tpl, "Template image", 1)], { log: quiet });
  assert.equal(mode, "child", "checked in a child process");
  assert(childRssMb > 20, `the child reports its peak memory (${childRssMb} MB)`);
  assert.equal(results[0].action, "converted");
  assert.equal(results[0].file, path.join(dir, "out_0"), "the converted charm is its own file");
  const meta = await sharp(await fsp.readFile(results[0].file)).metadata();
  assert.equal(meta.format, "png");
  assert(Math.max(meta.width, meta.height) <= 2048, `the 8000 px charm is fitted inside 2048 px (${meta.width})`);
  assert.equal(results[1].action, "as-is");
  assert.equal(results[1].file, tpl, "a clean template is sent from its own file");

  await assert.rejects(checkReferenceFiles([item(tpl, "Template image", 2), item(html, "Charm image", 3)], { log: quiet }),
    (err) => err instanceof InputImageError && err.permanent === true && err.role === "Charm image" &&
      /page\.png/.test(err.path) && /web page/.test(err.message), "an unusable file stops the set, naming it");

  // A child that is killed mid-check (out of memory) is not redone here: the set is tried again later.
  const killer = path.join(dir, "killer.cjs");
  await fsp.writeFile(killer, "process.on('message',()=>process.kill(process.pid,'SIGKILL'));process.send({ready:true,sharp:true});");
  await assert.rejects(checkReferenceFiles([item(tpl, "Template image", 4)], { worker: killer, log: quiet }),
    (err) => !(err instanceof InputImageError) && !err.permanent && err.interrupted === true &&
      /SIGKILL/.test(err.message) && /tried again/.test(err.message), "a killed check is an interruption, tried again later");
  // Killed before it was ready (out of memory while loading the decoder): the same, and the next set is
  // still checked in a child (it does not switch this process to checking in place for good).
  const early = path.join(dir, "early.cjs");
  await fsp.writeFile(early, "process.kill(process.pid,'SIGKILL');");
  await assert.rejects(checkReferenceFiles([item(tpl, "Template image", 7)], { worker: early, log: quiet }),
    (err) => err.interrupted === true && !err.childUnavailable);
  assert.equal((await checkReferenceFiles([item(tpl, "Template image", 8)], { log: quiet })).mode, "child",
    "the next set is checked in a child again");
  const stuck = path.join(dir, "stuck.cjs");
  await fsp.writeFile(stuck, "process.on('message',()=>{});process.send({ready:true,sharp:true});setInterval(()=>{},1000);");
  await assert.rejects(checkReferenceFiles([item(tpl, "Template image", 5)], { worker: stuck, timeoutMs: 300, log: quiet }),
    /reference check timed out/);

  // No worker file (a deploy without it): checked in this process, with the same results.
  const fresh = require("node:child_process").spawnSync(process.execPath, ["-e", `
    const { checkReferenceFiles } = require(${JSON.stringify(prepPath)});
    checkReferenceFiles([${JSON.stringify(item(tpl, "Template image", 6))}], { worker: null, log: { warn: () => {} } })
      .then((r) => console.log(JSON.stringify({ mode: r.mode, action: r.results[0].action })));`], { encoding: "utf8", env: process.env });
  assert.deepEqual(JSON.parse(fresh.stdout.trim()), { mode: "in-process", action: "as-is" }, fresh.stderr);
}

async function memory(dir) {
  // References of the live sizes (batch_input_check, 2026-10-25): an 8000 px charm, two 2048 px
  // models, 4000 px background, engraving and product shots, the 5000 px size guide.
  const noise = (w, h, ch, amount) => { const b = Buffer.alloc(w * h * ch); for (let i = 0; i < b.length; i++) b[i] = (128 + (Math.random() - 0.5) * amount) | 0; return b; };
  const make = async (name, w, h, ch, amount) => { const f = path.join(dir, name);
    await sharp(noise(w, h, ch, amount), { raw: { width: w, height: h, channels: ch } }).png().toFile(f); return f; };
  const charm = path.join(dir, "big-charm.png");
  await sharp(Buffer.from(`<svg xmlns="http://www.w3.org/2000/svg" width="8000" height="8000"><circle cx="4000" cy="4000" r="1500" fill="#c9a24a"/></svg>`)).png().toFile(charm);
  const refs = [await make("model.png", 2048, 2048, 3, 60), await make("bg.png", 4000, 4000, 3, 8),
    await make("guide.png", 5000, 5000, 3, 1), await make("engrave.png", 4000, 4000, 4, 1), await make("product.png", 4000, 4000, 3, 16)];
  const before = process.memoryUsage().rss;
  let peak = before;
  const sampler = setInterval(() => { peak = Math.max(peak, process.memoryUsage().rss); }, 20);
  const after = [];
  let childPeak = 0;
  for (let set = 0; set < 12; set++) {
    const items = [charm, ...refs].map((file, i) => ({ file, out: path.join(dir, `m_out_${set}_${i}`), contentType: "image/png",
      path: `listing-generator-1/x/${path.basename(file)}`, role: i ? "Template image" : "Charm image", canonical: !i }));
    const { results, mode, childRssMb } = await checkReferenceFiles(items, { log: quiet });
    assert.equal(mode, "child");
    childPeak = Math.max(childPeak, childRssMb);
    const out = path.join(dir, "m.jsonl"), fh = await fsp.open(out, "w");
    for (const r of results.slice(1)) {
      const a = imageToken(), b = imageToken();
      await appendJsonlLine(fh, { body: { images: [{ image_url: `data:${r.mime};base64,${a}` }, { image_url: `data:image/png;base64,${b}` }] } },
        [{ token: a, file: r.file }, { token: b, file: results[0].file }]);
    }
    await fh.close();
    for (const r of results) if (r.file.includes("m_out_")) await fsp.rm(r.file, { force: true });
    after.push(process.memoryUsage().rss);
  }
  clearInterval(sampler);
  const growth = after[after.length - 1] - after[0];
  console.log(`  this process over 12 sets: ${mb(before)} MB before, ${after.map(mb).join(" ")} MB after each, peak ${mb(peak)} MB; each set's child peaked at ${childPeak} MB at most`);
  assert(growth < 40 * 1048576, `memory does not grow set by set (${mb(growth)} MB over 11 sets)`);
  assert(peak - before < 150 * 1048576, `the collector holds no images (${mb(peak - before)} MB above its start)`);
}

(async () => {
  const dir = await fsp.mkdtemp(path.join(os.tmpdir(), "lg1-prepare-"));
  try {
    await lines(dir);
    await childChecks(dir);
    await memory(dir);
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
  console.log("Listing batch preparation: streamed JSONL is byte-identical, references checked in a child that ends with the set, kills and timeouts are retried later, no-worker fallback, flat memory over 12 sets passed");
})().catch((err) => { console.error(err); process.exitCode = 1; });
