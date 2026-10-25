"use strict";

// streamOpenAIBatchFile (geminiImageProxy-background.js) reads an OpenAI batch result file line by line.
// One result line is one 2048 px image as base64 (5-11 MB). The old reader appended every 16 KB chunk to a
// string and searched it for "\n" from the start again, so a six-image job took about 12 s to read and a
// collection could run out of its time limit while saving nothing.
// Checked, with a fake fetch answering a ReadableStream (nothing touches a network): every line is parsed
// whatever the chunking (a multi-byte UTF-8 character split across a chunk boundary, CRLF line ends with the
// CR and LF in different chunks, an empty line, a last line with no newline, chunks that are views into a
// larger buffer), and six 8 MB lines in 16 KB chunks are read in under 2 s.
// Usage: node tests/listing-batch/stream-split.cjs   (from the repository root)

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const source = fs.readFileSync(path.resolve("netlify/functions/geminiImageProxy-background.js"), "utf8");
const start = source.indexOf("async function* streamOpenAIBatchFile(apiKey, fileId) {");
const end = source.indexOf("\n}\n", start) + 3;
assert(start > 0 && end > start, "streamOpenAIBatchFile exists");

// The old reader can take many seconds on the large case; stop it with a clear failure instead of waiting.
const watchdog = setTimeout(() => { console.error("stream-split: reading did not finish within 20 s"); process.exit(1); }, 20000);

const requests = [];
let current = null;
function fakeFetch(url, options) {
  requests.push({ url: String(url), auth: options?.headers?.Authorization });
  const { buffer, cuts } = current;
  let i = 0;
  const body = new ReadableStream({
    async pull(controller) {
      if (i >= cuts.length - 1) { controller.close(); return; }
      // views into the one buffer (byteOffset > 0), as a network stack hands them over
      controller.enqueue(new Uint8Array(buffer.buffer, buffer.byteOffset + cuts[i], cuts[i + 1] - cuts[i]));
      i++;
      if (i % 256 === 0) await new Promise((resolve) => setImmediate(resolve));   // let the watchdog run
    },
  });
  return Promise.resolve({ ok: true, status: 200, body });
}
const streamOpenAIBatchFile = vm.runInNewContext(`${source.slice(start, end)}; streamOpenAIBatchFile`, {
  fetch: (...args) => fakeFetch(...args), Buffer, TextDecoder, AbortSignal, setTimeout, clearTimeout, console,
});

// chunk boundaries every `size` bytes plus the extra ones asked for
const cutsFor = (length, size, extra = []) => [...new Set([0, length, ...extra.filter((x) => x > 0 && x < length),
  ...Array.from({ length: Math.floor(length / size) }, (_, k) => (k + 1) * size).filter((x) => x < length)])].sort((a, b) => a - b);
async function readAll(buffer, cuts) {
  current = { buffer, cuts };
  const out = [];
  for await (const line of streamOpenAIBatchFile("sk-test", "file-abc123")) out.push(line);
  return JSON.parse(JSON.stringify(out));   // the objects were made in the vm context: compare them as plain data
}
async function readAllRaw(buffer, cuts) {
  current = { buffer, cuts };
  const out = [];
  for await (const line of streamOpenAIBatchFile("sk-test", "file-abc123")) out.push(line);
  return out;
}

(async () => {
  // (1) the awkward cases, small, cut at every size from 1 to 9 bytes and at 16 KB
  const fancy = { custom_id: "s0_slot2", note: "Charm “Ünïcødé” 🌙 – crescent ☾ 漢字", response: { status_code: 200 } };
  const lines = [
    { custom_id: "s0_slot0", response: { status_code: 200, body: { data: [{ b64_json: "AAAA" }] } } },
    { custom_id: "s0_slot1", error: { code: "invalid_image", message: "Invalid input image file. Please check the uploaded image." } },
    fancy,
    { custom_id: "s0_slot3", response: { status_code: 200, body: { data: [{ b64_json: "BBBB" }] } } },
  ];
  const text = JSON.stringify(lines[0]) + "\r\n" + JSON.stringify(lines[1]) + "\n\n" + "\r\n" +
    JSON.stringify(lines[2]) + "\n" + JSON.stringify(lines[3]);                 // no newline at the end
  const small = Buffer.concat([Buffer.alloc(5, 0x20), Buffer.from(text, "utf8"), Buffer.alloc(3, 0x20)]).subarray(5, 5 + Buffer.byteLength(text));
  const moon = small.indexOf(Buffer.from("🌙")), quote = small.indexOf(Buffer.from("“")), han = small.indexOf(Buffer.from("漢"));
  const cr = small.indexOf(Buffer.from("\r\n"));
  assert(moon > 0 && quote > 0 && han > 0 && cr > 0);
  for (const size of [1, 2, 3, 4, 5, 7, 9, 16384]) {
    const got = await readAll(small, cutsFor(small.length, size, [moon + 1, moon + 2, moon + 3, quote + 1, han + 2, cr + 1]));
    assert.deepEqual(got, lines, `${size}-byte chunks: every line parsed, characters intact`);
  }
  assert.equal(requests[0].url, "https://api.openai.com/v1/files/file-abc123/content");
  assert.equal(requests[0].auth, "Bearer sk-test");
  await assert.rejects(async () => { for await (const _ of streamOpenAIBatchFile("sk-test", "../etc")) { /* never */ } }, /Invalid OpenAI result file ID/);

  // (2) six images of 8 MB each, 16 KB chunks, with the same awkward line ends and a split character
  const MB8 = 8 * 1024 * 1024;
  const b64 = (k) => Buffer.alloc(MB8 * 3 / 4, k + 1).toString("base64").slice(0, MB8 - k);   // ~8 MB of base64
  const big = Array.from({ length: 6 }, (_, k) => ({ custom_id: `s0_slot${k}`, note: k === 3 ? "Ünïcødé 🌙" : undefined,
    response: { status_code: 200, body: { data: [{ b64_json: b64(k) }] } } }));
  const parts = [];
  big.forEach((line, k) => {
    parts.push(Buffer.from(JSON.stringify(line), "utf8"));
    if (k === 1) parts.push(Buffer.from("\r\n\r\n"));            // CRLF and an empty line
    else if (k < 5) parts.push(Buffer.from("\n"));                // the last line has no newline
  });
  const file = Buffer.concat(parts);
  const bigMoon = file.indexOf(Buffer.from("🌙"));
  const cuts = cutsFor(file.length, 16384, [bigMoon + 2, file.indexOf(Buffer.from("\r\n")) + 1]);
  assert(cuts.length > 3000, `${cuts.length - 1} chunks of 16 KB`);
  const t0 = process.hrtime.bigint();
  const got = await readAllRaw(file, cuts);
  const ms = Number(process.hrtime.bigint() - t0) / 1e6;
  assert.equal(got.length, 6, "six lines");
  got.forEach((line, k) => {
    assert.equal(line.custom_id, `s0_slot${k}`);
    assert.equal(line.response.body.data[0].b64_json.length, MB8 - k, `line ${k}: the whole image`);
    assert.equal(line.response.body.data[0].b64_json, big[k].response.body.data[0].b64_json, `line ${k}: the same bytes`);
  });
  assert.equal(got[3].note, "Ünïcødé 🌙", "a character split across a chunk boundary survives in a large line");
  console.log(`  6 x 8 MB in ${cuts.length - 1} chunks of 16 KB: ${ms.toFixed(0)} ms`);
  assert(ms < 2000, `six 8 MB lines are read in under 2 s (took ${ms.toFixed(0)} ms)`);
  clearTimeout(watchdog);
  console.log(`Listing batch result stream: lines split on bytes (UTF-8, CRLF, empty, unterminated, 1-16384 byte chunks), 48 MB read in ${ms.toFixed(0)} ms passed`);
})().catch((err) => { clearTimeout(watchdog); console.error(err); process.exitCode = 1; });
