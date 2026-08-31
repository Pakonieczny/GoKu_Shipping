/* Investor_AI — conservative visible-text extraction for public HTML/XML.
 *
 * This is intentionally not a browser renderer. Content explicitly marked as
 * executable, templated, embedded, accessibility-only, or visually hidden is
 * excluded before it can become evidence or consume a model context window.
 *
 * The previous implementation walked the document with regular expressions
 * over an already entity-decoded string. That produced three failures, each
 * of which is now covered by an executed test:
 *
 *   1. Entities were decoded BEFORE tags were identified, so prose that
 *      merely quoted markup ("use of &lt;span class=..hidden..&gt;") became a
 *      real hidden element and deleted the sentence that followed it.
 *   2. A hidden element was closed by the FIRST matching close tag rather
 *      than its own, so `<div hidden>a<div>b</div>PAYLOAD</div>` released
 *      PAYLOAD back into the visible text.
 *   3. A hidden element with no close tag deleted the remainder of the
 *      document, silently discarding disclosures that followed it.
 *
 * The scanner below tokenises the raw markup once, decodes entities only
 * inside text nodes, matches hidden subtrees by depth, and bounds an
 * unbalanced hidden element at the first sibling block boundary instead of
 * at end-of-input. When that bounding happens the document is reported as
 * structurally unbalanced so callers can grade it as weaker evidence.
 */
"use strict";

const VOID = new Set([
  "area", "base", "br", "col", "embed", "hr", "img", "input",
  "link", "meta", "param", "source", "track", "wbr",
]);

/* Removed wholesale: their content is never rendered as prose. */
const RAW = new Set([
  "script", "style", "noscript", "template", "iframe",
  "object", "canvas", "textarea", "ix:hidden",
]);

const BLOCK = new Set([
  "address", "article", "aside", "blockquote", "body", "dd", "details", "div",
  "dl", "dt", "fieldset", "figcaption", "figure", "footer", "form", "h1", "h2",
  "h3", "h4", "h5", "h6", "header", "hr", "li", "main", "nav", "ol", "p", "pre",
  "section", "table", "tbody", "td", "tfoot", "th", "thead", "tr", "ul",
]);

const BREAK = new Set([
  "br", "p", "div", "tr", "li", "h1", "h2", "h3", "h4", "h5", "h6",
  "table", "section", "article",
]);

function decodeEntities(value) {
  return String(value || "")
    .replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, "$1")
    .replace(/&#(\d+);/g, (_, n) => {
      const cp = Number(n); return Number.isInteger(cp) && cp >= 0 && cp <= 0x10ffff
        ? String.fromCodePoint(cp) : " ";
    })
    .replace(/&#x([0-9a-f]+);/gi, (_, n) => {
      const cp = parseInt(n, 16); return Number.isInteger(cp) && cp >= 0 && cp <= 0x10ffff
        ? String.fromCodePoint(cp) : " ";
    })
    .replace(/&nbsp;|&#160;/gi, " ").replace(/\u00a0/g, " ")
    .replace(/&amp;/gi, "&").replace(/&lt;/gi, "<").replace(/&gt;/gi, ">")
    .replace(/&quot;/gi, "\"").replace(/&apos;/gi, "'");
}

const HIDDEN_CLASSES = new Set([
  "hidden", "d-none", "sr-only", "visually-hidden", "visuallyhidden",
  "screen-reader-only", "is-hidden", "u-hidden", "js-hidden",
]);

function invisibleAttribute(attrs) {
  const a = String(attrs || "");
  if (/\bhidden(?:\s*=\s*(?:"hidden"|'hidden'|hidden))?(?:\s|>|\/|$)/i.test(a)) return true;
  if (/\baria-hidden\s*=\s*(?:"true"|'true'|true\b)/i.test(a)) return true;

  /* Read the attribute VALUE, then test it.
     The v8.4 pattern tried to match inside the quotes in one expression and
     terminated `opacity:0` / `font-size:0` on a character class that consumed
     the closing quote, leaving nothing for the rest of the pattern to match.
     Both declarations therefore never matched and their content was published
     as visible text. Reproduced against v8.4 before this was changed. */
  const styleMatch = /\bstyle\s*=\s*(?:"([^"]*)"|'([^']*)')/i.exec(a);
  const style = styleMatch ? (styleMatch[1] != null ? styleMatch[1] : styleMatch[2]) : "";
  if (/(?:^|;)\s*(?:display\s*:\s*none|visibility\s*:\s*hidden|opacity\s*:\s*0(?:\.0+)?|font-size\s*:\s*0(?:\.0+)?(?:px|em|rem|pt|%)?)\s*(?:!\s*important\s*)?(?:;|$)/i.test(style)) return true;

  /* CSS class names are whitespace-separated tokens and are compared whole.
     A \b-anchored pattern also matched inside "hidden-menu-toggle", because
     "-" is a word boundary — that deleted visible content whose class merely
     contained one of these words. */
  const classMatch = /\bclass\s*=\s*(?:"([^"]*)"|'([^']*)')/i.exec(a);
  const classes = classMatch ? (classMatch[1] != null ? classMatch[1] : classMatch[2]) : "";
  if (classes && classes.split(/\s+/).some((t) => HIDDEN_CLASSES.has(t.toLowerCase()))) return true;
  return false;
}

/* Read one markup construct starting at html[i] === "<".
   Attribute values are quote-aware so a ">" inside an attribute cannot end
   the tag early. Returns null for a stray "<" that is ordinary text. */
function readTag(html, i) {
  if (html.startsWith("<!--", i)) {
    const e = html.indexOf("-->", i + 4);
    return { kind: "comment", end: e < 0 ? html.length : e + 3 };
  }
  if (html.startsWith("<![CDATA[", i)) {
    const e = html.indexOf("]]>", i + 9);
    const stop = e < 0 ? html.length : e;
    return { kind: "cdata", text: html.slice(i + 9, stop), end: e < 0 ? html.length : e + 3 };
  }
  if (html.startsWith("<!", i) || html.startsWith("<?", i)) {
    const e = html.indexOf(">", i);
    return { kind: "decl", end: e < 0 ? html.length : e + 1 };
  }
  const head = /^<(\/?)\s*([a-z][a-z0-9:._-]*)/i.exec(html.slice(i, i + 96));
  if (!head) return null;
  let j = i + head[0].length;
  let quote = null;
  for (; j < html.length; j += 1) {
    const c = html[j];
    if (quote) { if (c === quote) quote = null; continue; }
    if (c === "\"" || c === "'") { quote = c; continue; }
    if (c === ">") break;
  }
  const attrs = html.slice(i + head[0].length, j);
  return {
    kind: "tag",
    closing: head[1] === "/",
    name: head[2].toLowerCase(),
    attrs,
    selfClosing: /\/\s*$/.test(attrs),
    end: Math.min(j + 1, html.length),
  };
}

/* Consume a raw-text element (script/style/...) including its close tag. */
function skipRaw(html, open) {
  const escaped = open.name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const close = new RegExp("</\\s*" + escaped + "\\s*>", "i");
  const m = close.exec(html.slice(open.end));
  return m ? open.end + m.index + m[0].length : html.length;
}

/* Consume a hidden subtree.
 *
 * Ends at the element's OWN close tag (depth-matched). If the element is
 * never closed, the removal is bounded at the first sibling block boundary —
 * an ancestor's close tag, or a block-level element opening at depth zero —
 * so a malformed hidden wrapper can no longer erase the rest of a filing. */
function skipHidden(html, open) {
  const inner = [];
  let i = open.end;
  while (i < html.length) {
    const lt = html.indexOf("<", i);
    if (lt < 0) break;
    const t = readTag(html, lt);
    if (!t) { i = lt + 1; continue; }
    if (t.kind !== "tag") { i = t.end; continue; }

    if (t.closing) {
      if (inner.length === 0) {
        if (t.name === open.name) return { end: t.end, unbalanced: false };
        /* A close tag for something that was open before us: we are inside
           an unclosed hidden element and this is its container ending. */
        return { end: lt, unbalanced: true };
      }
      const at = inner.lastIndexOf(t.name);
      if (at >= 0) inner.length = at;
      i = t.end;
      continue;
    }

    if (RAW.has(t.name)) { i = skipRaw(html, t); continue; }
    if (VOID.has(t.name) || t.selfClosing) { i = t.end; continue; }

    if (inner.length === 0 && BLOCK.has(t.name) && t.name !== open.name) {
      /* A new sibling block started while we are still "inside" a hidden
         element that never closed. Treat that as the boundary. */
      return { end: lt, unbalanced: true };
    }
    inner.push(t.name);
    i = t.end;
  }
  return { end: html.length, unbalanced: true };
}

/* Tokenise once; decode entities only inside text nodes. */
function scan(value, { preserveBreaks = false } = {}) {
  const html = String(value || "");
  const out = [];
  let unbalancedHidden = false;
  let hiddenRemoved = 0;
  let i = 0;

  while (i < html.length) {
    const lt = html.indexOf("<", i);
    if (lt < 0) { out.push(decodeEntities(html.slice(i))); break; }
    if (lt > i) out.push(decodeEntities(html.slice(i, lt)));

    const t = readTag(html, lt);
    if (!t) { out.push("<"); i = lt + 1; continue; }
    if (t.kind === "cdata") { out.push(decodeEntities(t.text)); i = t.end; continue; }
    if (t.kind !== "tag") { out.push(" "); i = t.end; continue; }

    if (!t.closing && RAW.has(t.name)) { out.push(" "); i = skipRaw(html, t); continue; }

    if (!t.closing && !VOID.has(t.name) && !t.selfClosing && invisibleAttribute(t.attrs)) {
      const r = skipHidden(html, t);
      unbalancedHidden = unbalancedHidden || r.unbalanced;
      hiddenRemoved += 1;
      out.push(" ");
      i = r.end;
      continue;
    }

    out.push(preserveBreaks && BREAK.has(t.name) ? "\n" : " ");
    i = t.end;
  }

  let text = out.join("");
  text = preserveBreaks
    ? text.replace(/[ \t]+/g, " ").replace(/\n[ \t]*/g, "\n").replace(/\n\n+/g, "\n").trim()
    : text.replace(/\s+/g, " ").trim();
  return { text, unbalancedHidden, hiddenRemoved };
}

function visibleText(value, opts = {}) {
  return scan(value, opts).text;
}

/* Retained for callers that only want the hidden content gone. Tags are
   stripped here too — the tokeniser cannot hand back partially-parsed
   markup, and no caller in this repository relied on the tags surviving. */
function removeInvisibleElements(value) {
  return scan(value).text;
}

module.exports = {
  decodeEntities, invisibleAttribute, removeInvisibleElements, visibleText, scan,
  BLOCK, VOID, RAW,
};
