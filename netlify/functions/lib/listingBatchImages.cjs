'use strict';
// Every reference image of a listing request passes through here before it is sent to OpenAI.
//
// 2026-10-23/24: OpenAI answered all six requests of 505 of the 506 Regular Necklace jobs with
// "Invalid input image file. Please check the uploaded image." The bytes of each reference went
// out as data:<Storage contentType>;base64,... and nothing looked at what the bytes were: the
// contentType of an uploaded charm comes from its file NAME (_mimeForExt / File.type at upload),
// so a JPEG or WebP saved as .png, a 16-bit or CMYK file, an animated image, a HEIC or a
// half-uploaded file all reached OpenAI as "image/png". A browser shows such files without
// complaint (it reads the bytes, not the label), which is why nobody could see anything wrong.
//
// Now the real format is read from the bytes. A clean PNG, JPEG or WebP is sent as it is, under
// its real type. Anything else that can be decoded is re-encoded once as an 8-bit sRGB PNG.
// A file that cannot be decoded at all throws InputImageError BEFORE any paid request: the set
// stops with a message naming the file, instead of 505 jobs failing at OpenAI.

const OPENAI_MIME = { png: 'image/png', jpeg: 'image/jpeg', webp: 'image/webp' };
// A 4096 px side is twice the 2048 px listing output, so nothing useful is lost by fitting larger
// references inside it. OpenAI takes up to 50 MB per image, but every one of a set's six requests
// carries the charm again as base64 (+33%) and the whole input file must stay under 190 MB.
const MAX_SIDE = 4096;
const CHARM_MAX_SIDE = 2048;
const MIN_SIDE = 32;
const MAX_BYTES = 15 * 1024 * 1024;

class InputImageError extends Error {
  constructor(message) {
    super(message);
    this.name = 'InputImageError';
    // Deterministic: the same bytes fail the same way, so retrying the download cannot help.
    this.inputImage = true;
    this.permanent = true;
  }
}

function sniffImageType(buf) {
  if (!Buffer.isBuffer(buf) || buf.length < 4) return null;
  if (buf.length >= 8 && buf.readUInt32BE(0) === 0x89504e47 && buf.readUInt32BE(4) === 0x0d0a1a0a) return 'png';
  if (buf[0] === 0xff && buf[1] === 0xd8 && buf[2] === 0xff) return 'jpeg';
  if (buf.length >= 12 && buf.toString('latin1', 0, 4) === 'RIFF' && buf.toString('latin1', 8, 12) === 'WEBP') return 'webp';
  if (/^GIF8[79]a/.test(buf.toString('latin1', 0, 6))) return 'gif';
  if (buf.length >= 12 && buf.toString('latin1', 4, 8) === 'ftyp') {
    const brand = buf.toString('latin1', 8, 12);
    if (/^avi[fs]$/.test(brand)) return 'avif';
    return 'heif';
  }
  if ((buf[0] === 0x49 && buf[1] === 0x49 && buf[2] === 0x2a && buf[3] === 0) ||
      (buf[0] === 0x4d && buf[1] === 0x4d && buf[2] === 0 && buf[3] === 0x2a)) return 'tiff';
  if (buf[0] === 0x42 && buf[1] === 0x4d && buf.length > 26) return 'bmp';
  const head = buf.toString('utf8', 0, Math.min(buf.length, 512)).replace(/^﻿/, '').trimStart().toLowerCase();
  if (head.startsWith('data:image/')) return 'dataurl';
  if (head.startsWith('<svg') || head.startsWith('<?xml') && head.includes('<svg')) return 'svg';
  if (head.startsWith('<!doctype html') || head.startsWith('<html') || head.startsWith('<head')) return 'html';
  if (head.startsWith('{') || head.startsWith('[')) return 'json';
  return null;
}

// An animated PNG (APNG) carries an acTL chunk before its first IDAT; the decoder reports one page.
function isAnimatedPng(buf) {
  for (let at = 8; at + 8 <= buf.length;) {
    const len = buf.readUInt32BE(at), type = buf.toString('latin1', at + 4, at + 8);
    if (type === 'acTL') return true;
    if (type === 'IDAT' || type === 'IEND') return false;
    at += 12 + len;
  }
  return false;
}

const describe = (path, role) => `${role || 'Reference image'} ${path ? `"${String(path).split('/').pop()}"` : ''}`.trim();
const what = type => ({ html: 'a web page (HTML), not an image', json: 'JSON text, not an image', dataurl: 'a data URL',
  heif: 'HEIC/HEIF', avif: 'AVIF', svg: 'SVG', gif: 'GIF', tiff: 'TIFF', bmp: 'BMP' }[type] || (type ? type.toUpperCase() : 'not a recognisable image'));

let _sharp;
function loadSharp() {
  if (_sharp !== undefined) return _sharp;
  try { _sharp = require('sharp'); } catch (_) { _sharp = null; }
  return _sharp;
}

// buf: the bytes read from Storage. contentType: the label Storage carries (may be wrong).
// canonical: always re-encode as an 8-bit sRGB PNG, even a file that looks clean. Used for the charm:
// one per set, it was the only input the 505 rejected jobs had in common (every request of a set
// failed, whichever template it used), so it is never sent in a form nobody has checked.
// Returns { buffer, mime, action, from, detail } where action is 'as-is', 'relabelled' or 'converted'.
async function normalizeBatchInputImage(buf, { contentType = '', path = '', role = '', canonical = false,
  sharp = loadSharp(), depth = 0 } = {}) {
  // A re-encoded charm is also fitted inside the 2048 px listing output: a charm drawn larger than
  // the whole picture it goes into carries no detail the model can use, only request size.
  const maxSide = canonical ? CHARM_MAX_SIDE : MAX_SIDE;
  const label = describe(path, role);
  if (!Buffer.isBuffer(buf) || !buf.length) throw new InputImageError(`${label} is empty (0 bytes). Upload it again.`);
  const from = String(contentType || '').toLowerCase() || 'unknown';
  const type = sniffImageType(buf);
  if (type === 'dataurl' && depth === 0) {
    // A file whose content is the text "data:image/...;base64,..." (an upload of a copied data URL).
    const text = buf.toString('utf8').trim();
    const comma = text.indexOf(',');
    if (comma > 0 && /;base64$/i.test(text.slice(0, comma))) {
      const inner = Buffer.from(text.slice(comma + 1).replace(/\s+/g, ''), 'base64');
      const out = await normalizeBatchInputImage(inner, { contentType, path, role, canonical, sharp, depth: 1 });
      return { ...out, action: 'converted', detail: `stored as data-URL text; ${out.detail}` };
    }
  }
  if (type === 'html' || type === 'json' || type === 'dataurl') {
    throw new InputImageError(`${label} is ${what(type)} (${buf.length} bytes, labelled ${from}). Replace the file with a PNG or JPEG.`);
  }
  if (!sharp) {
    // Without the decoder the real type is still sent; only formats OpenAI reads directly pass.
    if (OPENAI_MIME[type]) {
      const mime = OPENAI_MIME[type];
      return { buffer: buf, mime, from, action: mime === from ? 'as-is' : 'relabelled', detail: `${type} (not decoded: sharp unavailable)` };
    }
    throw new InputImageError(`${label} is ${what(type)}, labelled ${from}. OpenAI accepts PNG, JPEG or WebP.`);
  }
  let meta;
  try { meta = await sharp(buf, { failOn: 'none' }).metadata(); }
  catch (err) {
    throw new InputImageError(`${label} cannot be read as an image (${what(type)}, ${buf.length} bytes, labelled ${from}): ${err?.message || err}. Replace the file with a PNG or JPEG.`);
  }
  const width = Number(meta.width || 0), height = Number(meta.height || 0);
  if (!width || !height) throw new InputImageError(`${label} has no readable size (${what(type)}, labelled ${from}). Replace the file.`);
  if (Math.max(width, height) < MIN_SIDE) throw new InputImageError(`${label} is only ${width}x${height} pixels; it is a placeholder, not a usable reference. Replace the file.`);
  // A complete decode is the only proof the file is whole: a cut-off upload has a valid header and
  // a browser still shows the part that arrived. Such a file is refused, not patched: the model
  // would be shown half a charm, and the listing made from it would be wrong.
  try { await sharp(buf, { failOn: 'warning' }).raw().toBuffer(); }
  catch (err) {
    throw new InputImageError(`${label} is damaged or incomplete (${what(type)}, ${buf.length} bytes, labelled ${from}): ${
      String(err?.message || err).split('\n')[0]}. Upload the file again.`);
  }
  const reasons = [];
  if (!OPENAI_MIME[type]) reasons.push(`${what(type)} file`);
  else if (meta.format && meta.format !== type && !(type === 'jpeg' && meta.format === 'jpg')) reasons.push(`bytes read as ${meta.format}`);
  if (Number(meta.pages || 1) > 1 || type === 'png' && isAnimatedPng(buf)) reasons.push('animated');
  if (meta.depth && meta.depth !== 'uchar') reasons.push(`${meta.depth === 'ushort' ? '16' : meta.depth}-bit`);
  if (meta.space && meta.space !== 'srgb') reasons.push(meta.space === 'b-w' ? 'greyscale' : `${meta.space} colour`);
  if (meta.isPalette || meta.paletteBitDepth) reasons.push('palette colour');
  if (type === 'png' && meta.isProgressive) reasons.push('interlaced');
  if (canonical && !reasons.length) reasons.push('charm re-encoded');
  if (Number(meta.orientation || 1) > 1) reasons.push(`EXIF orientation ${meta.orientation}`);
  if (Math.max(width, height) > maxSide) reasons.push(`${width}x${height} pixels`);
  if (buf.length > MAX_BYTES) reasons.push(`${Math.round(buf.length / 1048576)} MB`);
  if (!reasons.length) {
    const mime = OPENAI_MIME[type];
    return { buffer: buf, mime, from, action: mime === from ? 'as-is' : 'relabelled', detail: `${type} ${width}x${height}` };
  }
  const pipeline = () => sharp(buf, { failOn: 'none', pages: 1 })
    .rotate()
    .resize({ width: maxSide, height: maxSide, fit: 'inside', withoutEnlargement: true })
    .toColourspace('srgb');
  let out, mime = 'image/png';
  try {
    out = await pipeline().png({ compressionLevel: 6 }).toBuffer();
    // A photograph-like picture can still be too large as PNG: near-lossless WebP keeps it usable.
    if (out.length > MAX_BYTES) { out = await pipeline().webp({ quality: 92, alphaQuality: 100 }).toBuffer(); mime = 'image/webp'; }
  } catch (err) {
    throw new InputImageError(`${label} could not be converted (${reasons.join(', ')}; labelled ${from}): ${err?.message || err}. Replace the file with a PNG or JPEG.`);
  }
  if (OPENAI_MIME[sniffImageType(out)] !== mime || out.length > MAX_BYTES) {
    throw new InputImageError(`${label} could not be converted to a usable image (${reasons.join(', ')}). Replace the file.`);
  }
  return { buffer: out, mime, from, action: 'converted', detail: `${reasons.join(', ')} -> ${mime.slice(6)}` };
}

module.exports = { normalizeBatchInputImage, sniffImageType, InputImageError, MAX_SIDE, CHARM_MAX_SIDE, MIN_SIDE, MAX_BYTES };
