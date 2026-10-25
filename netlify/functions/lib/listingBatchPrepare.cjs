'use strict';
// How batch_submit prepares a listing set's OpenAI input file without the collector running out of
// memory (2026-10-25). Before, every reference was read into memory, checked by the image decoder in
// the collector's own process, base64-encoded and joined into one JSONL string: the decoder's native
// memory is never handed back while the process lives, and a 630-set refill was killed by Netlify's
// 1024 MB limit on its third to fifth set, every run, mid-preparation (2-5 sets sent per ten minutes).
// Now the references are downloaded to files, checked in a child process that ends with the set
// (lib/listingBatchImageWorker.cjs), and the JSONL is written to a file line by line, the images
// streamed into it in small base64 pieces. The collector itself holds a few MB per set, whatever its size.
const fs = require('node:fs');
const fsp = require('node:fs/promises');
const path = require('node:path');
const crypto = require('node:crypto');
const { fork } = require('node:child_process');
const { normalizeBatchInputImage, InputImageError } = require('./listingBatchImages.cjs');

const WORKER = 'listingBatchImageWorker.cjs';
// The worker is shipped next to this file (netlify.toml included_files); a bundle has no files of its
// own, so the places a deployed function keeps included files are searched too.
function findImageWorker() {
  const roots = [__dirname, path.join(__dirname, 'lib'), path.join(__dirname, '../functions/lib'),
    path.join(process.cwd(), 'netlify/functions/lib'),
    process.env.LAMBDA_TASK_ROOT ? path.join(process.env.LAMBDA_TASK_ROOT, 'netlify/functions/lib') : null];
  // …and every folder above the bundle, in case it sits deeper than expected.
  for (let dir = __dirname, up = 0; up < 6; up++, dir = path.dirname(dir)) roots.push(path.join(dir, 'netlify/functions/lib'));
  for (const dir of roots) {
    if (!dir) continue;
    const file = path.join(dir, WORKER);
    if (fs.existsSync(file) && fs.existsSync(path.join(dir, 'listingBatchImages.cjs'))) return file;
  }
  return null;
}

let sharpPath;
function resolveSharp() {
  if (sharpPath === undefined) { try { sharpPath = require.resolve('sharp'); } catch (_) { sharpPath = null; } }
  return sharpPath;
}

// Once the child cannot start here for a reason that will not change (no worker file, no decoder in it,
// the script fails to load), this process checks in place. A child stopped by a signal, or a fork refused
// for lack of memory, is not such a reason: that set is tried again later, still in a child.
let childUnavailable = null;
// A stopped worker (out of memory, most often) says nothing against the set: admission backs it off as
// an interruption (interruptionPatch), not a preparation failure that stops the set after three.
const interrupted = (message) => Object.assign(new Error(message), { interrupted: true });

const asError = (e, item) => {
  const err = e.inputImage ? new InputImageError(e.message) : new Error(e.message);
  if (!e.inputImage && e.permanent) err.permanent = true;
  return Object.assign(err, { path: item?.path, role: item?.role });
};

const transientSpawn = (err) => ['ENOMEM', 'EAGAIN', 'EMFILE', 'ENFILE'].includes(err?.code);

function runChild(worker, items, timeoutMs) {
  return new Promise((resolve, reject) => {
    const results = [];
    let maxRssMb = null;
    let child = null, ready = false, finished = false;
    const end = (settle, value) => {
      if (finished) return;
      finished = true;
      clearTimeout(timer);
      if (settle === reject) { try { child?.kill('SIGKILL'); } catch (_) {} }
      settle(value);
    };
    const timer = setTimeout(() => end(reject, new Error('Listing reference check timed out; remaining listings can continue.')), timeoutMs);
    try {
      child = fork(worker, [], { execArgv: [], stdio: ['ignore', 'inherit', 'inherit', 'ipc'],
        env: { ...process.env, LG1_SHARP_PATH: resolveSharp() || '', MALLOC_ARENA_MAX: '2' } });
    } catch (err) { end(reject, transientSpawn(err) ? interrupted(`The reference image check could not start (${err.code}); this listing is tried again later.`)
      : Object.assign(err, { childUnavailable: true })); return; }
    child.on('error', (err) => end(reject, transientSpawn(err)
      ? interrupted(`The reference image check could not start (${err.code}); this listing is tried again later.`)
      : Object.assign(err, { childUnavailable: !ready })));
    child.on('message', (msg) => {
      if (msg?.ready) {
        if (!msg.sharp) { end(reject, Object.assign(new Error('the image decoder does not load in a child process'), { childUnavailable: true })); return; }
        ready = true;
        child.send({ items });
      } else if (msg?.error) end(reject, asError(msg.error, items[msg.i]));
      else if (msg?.ok) results[msg.i] = msg;
      else if (msg?.done) { maxRssMb = Number(msg.maxRssMb) || null; end(resolve, { results, maxRssMb }); }
    });
    child.on('exit', (code, signal) => end(reject, ready || signal
      // Killed while checking or while starting (most often out of memory): no fault of the set's files,
      // and nothing was sent; the set is tried again later.
      ? interrupted(`The reference image check stopped (${signal || `exit ${code}`}) before it finished; this listing is tried again later.`)
      : Object.assign(new Error(`the image check process did not start (exit ${code})`), { childUnavailable: true })));
  });
}

async function checkInProcess(items) {
  const results = [];
  for (const item of items) {
    const buf = await fsp.readFile(item.file);
    let img;
    try {
      img = await normalizeBatchInputImage(buf, { contentType: item.contentType, path: item.path, role: item.role, canonical: item.canonical });
    } catch (err) { throw Object.assign(err, { path: item.path, role: item.role }); }
    let file = item.file;
    if (img.buffer !== buf) { await fsp.writeFile(item.out, img.buffer, { mode: 0o600 }); file = item.out; }
    results.push({ file, mime: img.mime, from: img.from, action: img.action, detail: img.detail });
  }
  return results;
}

// items: [{ file, out, contentType, path, role, canonical }], each `file` already on disk.
// Answers { results: [{ file, mime, from, action, detail }] (the file to send, in item order), mode,
// childRssMb } (the child's peak memory; null when checked in this process).
async function checkReferenceFiles(items, { timeoutMs = 30000 + 20000 * items.length, worker, log = console } = {}) {
  const workerFile = worker === undefined ? findImageWorker() : worker;
  if (workerFile && !childUnavailable) {
    try {
      const { results, maxRssMb } = await runChild(workerFile, items, timeoutMs);
      return { results, mode: 'child', childRssMb: maxRssMb };
    }
    catch (err) {
      if (!err?.childUnavailable) throw err;
      childUnavailable = String(err?.message || err);
      log.warn?.(`[batch_submit] reference check runs in place: ${childUnavailable}`);
    }
  } else if (!workerFile && !childUnavailable) {
    childUnavailable = `${WORKER} is not deployed with this function`;
    log.warn?.(`[batch_submit] reference check runs in place: ${childUnavailable}`);
  }
  return { results: await checkInProcess(items), mode: 'in-process', childRssMb: null };
}

// A placeholder for an image inside a JSONL line. Letters and digits only, so JSON.stringify leaves
// it unchanged and it can be found again in the text.
const imageToken = () => `LG1IMG${crypto.randomBytes(12).toString('hex')}`;

// Every byte of `text` is written (a single write may write fewer; a full disk throws).
async function writeAll(fh, text) {
  const buf = Buffer.from(text, 'utf8');
  for (let at = 0; at < buf.length;) {
    const { bytesWritten } = await fh.write(buf, at, buf.length - at);
    if (!bytesWritten) throw new Error('The batch input file could not be written (disk full?)');
    at += bytesWritten;
  }
  return buf.length;
}

const PIECE = 3 * 1024 * 1024; // a multiple of 3 bytes: the base64 of the pieces joins up exactly
async function writeBase64(fh, file) {
  const src = await fsp.open(file, 'r');
  const buf = Buffer.allocUnsafe(PIECE);
  let bytes = 0;
  try {
    for (;;) {
      let filled = 0;
      while (filled < PIECE) {
        const { bytesRead } = await src.read(buf, filled, PIECE - filled, null);
        if (!bytesRead) break;
        filled += bytesRead;
      }
      if (!filled) break;
      bytes += await writeAll(fh, buf.toString('base64', 0, filled));
      if (filled < PIECE) break;
    }
  } finally { await src.close(); }
  return bytes;
}

// Writes `line` (whose image data are imageToken() placeholders) as one JSONL line, each placeholder
// replaced by the base64 of its file. images: [{ token, file }]. Answers the bytes written.
async function appendJsonlLine(fh, line, images = []) {
  const text = JSON.stringify(line) + '\n';
  const files = new Map(images.filter((img) => img?.token && img.file).map((img) => [img.token, img.file]));
  let at = 0, bytes = 0;
  while (files.size) {
    let next = -1, token = null;
    for (const t of files.keys()) {
      const i = text.indexOf(t, at);
      if (i >= 0 && (next < 0 || i < next)) { next = i; token = t; }
    }
    if (next < 0) break;
    bytes += await writeAll(fh, text.slice(at, next));
    bytes += await writeBase64(fh, files.get(token));
    at = next + token.length;
  }
  bytes += await writeAll(fh, text.slice(at));
  return bytes;
}

module.exports = { checkReferenceFiles, appendJsonlLine, imageToken, findImageWorker };
