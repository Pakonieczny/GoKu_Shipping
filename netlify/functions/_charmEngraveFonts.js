/*  netlify/functions/_charmEngraveFonts.js — the engraving fonts in the cloud (cloud plan v2, build step 11: "fit tasks call CharmNestEngraveFit.calculate directly, with fonts
 *  read from vendor/fonts and hb.wasm").
 *
 *  The page reads the same files over the network (charm-nest-bridge.js Engrave: loadFonts, loadFontSet, ensureScriptFonts) and hands them to its fitting worker
 *  (charm-nest-engrave-worker.js). Here they are read from the function's own bundle (netlify.toml included_files vendor/**) and made the very same way:
 *    · Source Sans 3 Regular + Semibold (O.DEFAULT_FONT), parsed by the vendored opentype.js, wrapped with the shaped emoji font (NotoEmoji + its shape map, checked against
 *      the map's sha-256 as the page checks it) and DejaVu Sans for the symbols (CharmNestText.withEmoji);
 *    · every other shop font of charm-nest-orders.js ENGRAVING_FONTS, read when a piece asks for it, wrapped with Source Sans 3 itself and the symbol font as its fallbacks;
 *    · the script fonts (Arabic, Hebrew, Indic, CJK: vendor/fonts/script-fonts.json) and the shaping engine (vendor/hb.wasm, charm-nest-shape.js), only when a line has a
 *      letter of such a script, each checked against the manifest's size and sha-256.
 *  A font that cannot be read is said in words and the piece waits (never a cut set in the wrong font). Nothing here reaches the network, Firestore or Etsy.
 *
 *    load()                        → the default set { Regular, Semibold, sansRaw, emojiFont, symbolFont, map }
 *    setOf(fontKey)                → the set a font id engraves with ({ Regular, Semibold? }), or null when its files cannot be read (o.why says why)
 *    forJob(job)                   → { fonts (pinned to the job's script fonts, as the worker pins them), fontKey, fontName, scriptFonts, scriptIds, fallback, lines, lineInput, text }
 *                                    the page's ensureJobFont for a job of the cloud: the font the job carries, capitals for a font with no lowercase, script fonts loaded */
"use strict";
const fs = require("fs"), path = require("path"), crypto = require("crypto");
const opentype = require("../../vendor/opentype-1.3.4.min.js");
const T = require("../../charm-nest-text.js");
const Shape = require("../../charm-nest-shape.js");
const O = require("../../charm-nest-orders.js");

const FONT_FILES = { Regular: "vendor/fonts/SourceSans3-Regular.otf", Semibold: "vendor/fonts/SourceSans3-Semibold.otf" };
const SYMBOL_FILE = "vendor/fonts/DejaVuSans.ttf", EMOJI_MAP = "vendor/fonts/emoji-sequences.json", EMOJI_FONT = "vendor/fonts/NotoEmoji-Regular.ttf";
const SCRIPT_MANIFEST = "vendor/fonts/script-fonts.json", SHAPE_WASM = "vendor/hb.wasm";

/* The folder the vendored files are in: the repository's root when the function runs from its source (netlify dev, the tests), the bundle's root once deployed
   (Netlify keeps included_files at their repository paths under the task root). */
let ROOT = null;
function rootDir() {
  if (ROOT) return ROOT;
  const tries = [path.join(__dirname, "..", ".."), process.env.LAMBDA_TASK_ROOT || "", process.cwd(), __dirname, path.join(__dirname, "..")].filter(Boolean);
  ROOT = tries.find(r => { try { return fs.existsSync(path.join(r, FONT_FILES.Regular)); } catch (_) { return false; } }) || tries[0];
  return ROOT;
}
const read = rel => fs.readFileSync(path.join(rootDir(), String(rel).replace(/\?.*$/, "")));
const ab = buf => buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
const sha256 = buf => crypto.createHash("sha256").update(buf).digest("hex");

let BASE = null, BASE_ERR = null;
/** Source Sans 3 with its emoji and symbols, made once per instance (the page's loadFonts). Throws, in words, when the face itself cannot be read. */
function load() {
  if (BASE) return BASE;
  if (BASE_ERR && Date.now() - BASE_ERR.at < 60000) throw new Error(BASE_ERR.why);
  try {
    const F = {};
    for (const [w, p] of Object.entries(FONT_FILES)) {
      try { const buf = read(p); if (buf.length < 1000) throw new Error("empty file"); F[w] = opentype.parse(ab(buf)); }
      catch (e) { if (w === "Regular") throw new Error(`Source Sans 3 is not available (${p}: ${e.message})`); F.semiboldMissing = `${p}: ${e.message}`; }
    }
    F.sansRaw = F.Regular;
    try {
      const map = JSON.parse(read(EMOJI_MAP).toString("utf8")), bytes = read(EMOJI_FONT);
      if (map.fontSha256 && sha256(bytes) !== map.fontSha256) throw new Error("Emoji font version differs from its shape map");
      const emoji = opentype.parse(ab(bytes));
      let symbol = null; try { const sb = read(SYMBOL_FILE); if (sb.length >= 1000) symbol = opentype.parse(ab(sb)); } catch (_) { symbol = null; }
      F.emojiFont = emoji; F.symbolFont = symbol; F.map = map;
      for (const w of ["Regular", "Semibold"]) if (F[w]) F[w] = T.withEmoji(F[w], emoji, map, opentype.Path, symbol ? [symbol] : []);
      F.emoji = true;
    } catch (e) { F.emojiError = e.message; }
    F.ok = !!F.Regular;
    BASE = F; BASE_ERR = null;
    return BASE;
  } catch (e) { BASE_ERR = { at: Date.now(), why: e.message }; throw e; }
}

const SETS = new Map();
/** The set a font id engraves with: Source Sans 3 for the default id; any other shop font read from its files and wrapped like the page wraps it. null (and SETS keeps why) when
 *  its Regular cannot be read: the caller falls back to Source Sans 3 and says so, as the page does (job.fontFallback). */
function setOf(id) {
  const base = load();
  const meta = O.fontById(id);
  if (!meta || meta.id === O.DEFAULT_FONT) return base;
  const had = SETS.get(meta.id); if (had && (had.ok || Date.now() - had.triedAt < 60000)) return had.ok ? had : null;
  const set = { id: meta.id, name: meta.name, ok: false, triedAt: Date.now() };
  try {
    for (const [w, p] of Object.entries(meta.files)) {
      try { const buf = read(p); if (buf.length < 1000) throw new Error("empty file"); set[w] = opentype.parse(ab(buf)); }
      catch (e) { if (w === "Regular") throw e; set.semiboldMissing = `${p}: ${e.message}`; }
    }
    if (base.emojiFont && base.map) for (const w of ["Regular", "Semibold"]) if (set[w]) set[w] = T.withEmoji(set[w], base.emojiFont, base.map, opentype.Path, [base.sansRaw, base.symbolFont].filter(Boolean));
    set.ok = true;
  } catch (e) { set.error = `${meta.name}: ${e.message}`; }
  SETS.set(meta.id, set);
  return set.ok ? set : null;
}

/* ── the script fonts (WORLD, Paul 19 Oct 2026) ── */
const SCRIPTS = { manifest: null, ready: false, loaded: new Map(), failed: new Map() };
async function shaper() {
  const set = T.sharedScripts; if (!set) return false;
  if (!Shape.ready) { const wasm = read(SHAPE_WASM); if (wasm.length < 10000) throw new Error(`${SHAPE_WASM}: empty file`); await Shape.init({ wasm: ab(wasm) }); }
  if (set.shape !== Shape) set.attach(Shape);
  return true;
}
function manifest() {
  if (SCRIPTS.manifest) return SCRIPTS.manifest;
  const m = JSON.parse(read(SCRIPT_MANIFEST).toString("utf8"));
  if (!m || !Array.isArray(m.fonts) || !m.fonts.length) throw new Error(`${SCRIPT_MANIFEST}: no fonts listed`);
  SCRIPTS.manifest = m; if (T.sharedScripts) T.sharedScripts.declare(m.fonts);
  return m;
}
function loadScriptFont(entry) {
  if (SCRIPTS.loaded.has(entry.id)) return true;
  const failed = SCRIPTS.failed.get(entry.id); if (failed && Date.now() - failed.at < 60000) return false;
  try {
    const bytes = read(entry.file);
    if (bytes.length < 1000 || (entry.bytes && bytes.length !== entry.bytes)) throw new Error("the file is not the one the manifest lists");
    if (entry.sha256 && sha256(bytes) !== entry.sha256) throw new Error("font version differs from the manifest");
    const m = Shape.addFont(entry.id, ab(bytes));
    T.sharedScripts.use(Object.assign({}, entry, { capEm: m.capHeight / m.unitsPerEm }));
    SCRIPTS.loaded.set(entry.id, true); SCRIPTS.failed.delete(entry.id);
    return true;
  } catch (e) { SCRIPTS.failed.set(entry.id, { at: Date.now(), error: `${entry.name}: ${e.message}` }); return false; }
}
/** The page's ensureScriptFonts: the script fonts the words need, loaded (the first fonts, then the next of a chain for what they lack). A Latin line costs one test. → [{ id, name, scripts }] */
async function ensureScripts(text, font) {
  if (!T.scriptHint || !T.scriptHint(text) || !font || typeof font.coverage !== "function") return [];
  try {
    manifest();
    const set = T.sharedScripts, asked = new Set();
    for (let round = 0; round < 6; round++) {
      const want = set.needs(text, font.coverage(text).missing, asked); if (!want.length) break;
      if (!await shaper()) break;
      want.forEach(e => asked.add(e.id));
      for (const e of want) loadScriptFont(e);
    }
  } catch (e) { SCRIPTS.error = e.message; }
  return typeof font.scriptFonts === "function" ? font.scriptFonts(text) : [];
}
/** Why words in a script are still refused ("" when that is not the cause). */
const scriptWhy = () => SCRIPTS.error || [...SCRIPTS.failed.values()].map(f => f.error)[0] || "";

/** The fonts of one job, as the page's ensureJobFont and the worker give them to calculate. job: its record's fields (fontKey, lines, lineInput, text, scriptFonts?).
 *  spec (optional): the world line's spec, read only when the job carries no font yet (the font its line asks for: O.pieceFont). pieceIndex: the slot's disc index or null. */
async function forJob(job, o = {}) {
  load();
  let fontKey = job.fontKey || null, asked = job.fontAsked || "";
  if (!fontKey) { try { const p = O.pieceFont(o.spec || null, o.pieceIndex == null ? null : o.pieceIndex, job.face === "front" ? "front" : "back"); fontKey = p.id; asked = p.asked || ""; } catch (_) { fontKey = O.DEFAULT_FONT; } }
  const askedKey = fontKey;
  let set = setOf(fontKey), fallback = null;
  if (!set) { const had = SETS.get((O.fontById(fontKey) || {}).id); fallback = `${(O.fontById(fontKey) || {}).name || fontKey} could not be loaded${had && had.error ? ` (${had.error})` : ""}`; fontKey = O.DEFAULT_FONT; set = load(); }
  // a font with no lowercase (Ace): the words are engraved in capitals, kept in what is saved too
  let lines = (job.lines || []).slice(), lineInput = Array.isArray(job.lineInput) ? job.lineInput.slice() : null, text = job.text;
  if (O.fontCaps(askedKey)) { const up = t => O.capsText(t, askedKey); lines = lines.map(up); if (lineInput) lineInput = lineInput.map(up); text = up(text); }
  const words = (lineInput || lines).join("\n");
  const used = await ensureScripts(words, set.Regular);
  const ids = used.map(f => f.id), pin = f => (f && ids.length && typeof f.pin === "function" ? f.pin(ids) : f);
  const fonts = ids.length ? { Regular: pin(set.Regular), Semibold: set.Semibold ? pin(set.Semibold) : undefined } : { Regular: set.Regular, Semibold: set.Semibold };
  return { fonts, fontKey, fontAsked: asked, fontName: (O.fontById(fontKey) || O.fontById(O.DEFAULT_FONT)).name, scriptFonts: used, scriptIds: ids, fallback, lines, lineInput, text, capsApplied: O.fontCaps(askedKey) };
}

module.exports = { load, setOf, forJob, ensureScripts, scriptWhy, rootDir, FONT_FILES };
