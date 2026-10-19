/* CharmNestShape: real text shaping (HarfBuzz) and UAX #9 bidirectional ordering for the engraving of Arabic, Hebrew, Indic, CJK ... lines.
   (WORLD, Paul 19 Oct 2026: opentype.js 1.3.4 sets one glyph after another, left to right: no Arabic joining, no Indic reordering/conjuncts, no mark positioning, no right-to-left.
    This file adds all of that without any network, DOM or Web Worker dependence: pure JS + one WebAssembly module, deterministic, the same result in the page, in the engraving Worker and in Node.)

   UMD: window.CharmNestShape | module.exports | self.CharmNestShape. Page: classic <script>; Worker: importScripts('charm-nest-shape.js'); Node 22: require().
   Nothing is loaded at require time. The caller hands in the bytes (it already has fetch / cache rules):  await CharmNestShape.init({wasm: ArrayBuffer|Uint8Array of vendor/hb.wasm})
   (a WebAssembly.Module is accepted too; {WebAssembly} overrides the global when a sandbox does not expose it; in Node, init() with no wasm reads vendor/hb.wasm next to this file).

   API (all x/y in px at `size`, y DOWN, the same convention as opentype.js font.getPath(text, x, y, size) with x = y = 0):
   CharmNestShape.init({wasm, WebAssembly})   async, idempotent (a failed init may be retried) -> Promise<CharmNestShape>
   CharmNestShape.ready                       boolean
   CharmNestShape.addFont(id, bytes, {variations, warm}?)  sync after init. bytes = ArrayBuffer|Uint8Array of a TTF/OTF (first face of a collection). Idempotent per id.
                                              -> { id, unitsPerEm, ascender, descender (negative), lineGap, capHeight, xHeight }   (font units; capHeight/xHeight from OS/2, else measured on H / x, else .7 / .5 * ascender)
                                              variations (optional): 'wght=400,wdth=100' for a variable font (HarfBuzz default instance otherwise);
                                              the font is warmed up with one shaping of a space (warm:false skips it): about 10 ms for a Noto Sans Indic/Arabic face, ~100 ms for a 9 MB CJK face, once, off the first line
   CharmNestShape.removeFont(id)              frees the font's wasm memory (a 6 MB CJK face is copied into the module)
   CharmNestShape.hasFont(id)                 boolean
   CharmNestShape.covers(id, text)            true when every code point of `text` (default-ignorable code points and line breaks aside) has a glyph in that font's cmap
                                              (a code point the cmap lacks still counts when its canonical decomposition is all in the cmap, HarfBuzz composes/decomposes the same way)
   CharmNestShape.scriptOf(text)              the dominant ISO 15924 script among the code points that NEED a script font ('Arab','Hebr','Deva','Beng','Hani','Hira','Kana','Hang',...), ties go to the first met;
                                              null when the text is only Latin / Greek / Cyrillic / Common (digits, punctuation, symbols, emoji, spaces) / Inherited: the base face draws those
   CharmNestShape.scriptsOf(text)             { Arab: 4, Latn: 7, ... } count of every non-Common/Inherited code point by script, Latn / Grek / Cyrl included (Japanese is {Hani, Hira, Kana}: ask this one to tell it from Chinese)
   CharmNestShape.scriptOfCodePoint(cp)       the real ISO 15924 code of one code point ('Latn', 'Zyyy' common, 'Zinh' inherited, 'Zzzz' unassigned ...), Unicode Scripts.txt (fonttools 4.67 = Unicode 18.0 Scripts.txt)
   CharmNestShape.bidi(text, {dir}?)          UAX #9 on a whole line, paragraph direction by first strong character (rules P2/P3; dir:'ltr'|'rtl' forces it), explicit embeddings/isolates/marks honoured, brackets paired (N0):
                                              { baseRtl, runs:[{start, end, text, level, rtl}] in LOGICAL order (start/end = UTF-16 offsets into `text`, end exclusive; each run is a maximal same-level span),
                                                visual:[run indexes] left-to-right DRAWING order (rule L2, trailing white space at the paragraph level per L1) }
                                              run.text already has mirrored characters substituted where odd-level text needs them (rule L4: ( ) [ ] { } < > and the guillemets ...). A text with several paragraphs (line breaks) lists
                                              them one after the other, `visual` ordering each paragraph on its own.
   CharmNestShape.shapeRun(text, {fontId, size, rtl, script, lang, features, mirror})
                                              ONE uniform-direction run (one font) shaped by HarfBuzz with the font's own GSUB/GPOS (ligatures, Arabic joining, Indic reordering and conjuncts, mark positioning, kerning)
                                              -> { advance, commands, glyphs:[{id, cluster, x, y, adv}], missing:[code points], dotted:[cluster offsets] }
                                              rtl: true/false (default: from the first script of the text); script: ISO 15924 ('Arab') to force it (below); lang: BCP 47 ('ar','fa','ur','hi','ja','zh-Hans'...; default none);
                                              features: 'kern=0,+ss01,-liga' or an array of such strings (the font's default features otherwise).
                                              script: forces ONE script for the whole run; without it the run is itemized by script like a browser does (Han / kana / Latin / digits ... each shaped with its own
                                              script table of the font; common and inherited characters take the script before them). Direction stays one for the whole run.
                                              dotted: where HarfBuzz had to insert the dotted circle U+25CC that the text did not contain (a vowel sign or virama with no consonant to sit on: an invalid
                                              sequence a browser shows the same way); noDottedCircle:true suppresses it. A caller that refuses loose marks refuses a non-empty `dotted`.
                                              `text` is what is to be DRAWN: a run from bidi() (brackets already mirrored) goes in as it is. HarfBuzz's own mirroring of brackets in right-to-left runs is cancelled
                                              for that (mirror:true gives it back, for logical text that was not through bidi()).
                                              commands: opentype.js Path commands [{type:'M'|'L'|'Q'|'C'|'Z', x, y, x1, y1, x2, y2}] with the pen at the origin and the baseline at y = 0, glyphs in drawing order from left to right
                                              (an rtl run comes out visual: glyphs[0] is the one of the LAST character of the text, drawn at the left, the pen only ever advances right), so
                                              path.extend(commands translated by (x, y)) is all a caller does. glyphs[].x / y = where the glyph origin was drawn (including HarfBuzz x/y offsets), adv = its advance;
                                              cluster = UTF-16 offset in `text` of the first character of the cluster (a ligature glyph covers several). advance = the run's width.
                                              A code point the font lacks (.notdef) draws nothing and is listed in `missing` (its advance is kept); it never throws on that: decide beforehand with covers().
                                              An unknown font id or a call before init() throws.
   CharmNestShape.glyphExtents(id, cp)        { xMin, yMin, xMax, yMax } of a code point's glyph in font units (y up), or null: lets a caller measure a reference glyph (H, x, an ideograph ...)

   Why the bidi-js copy is inside this file: one script to load in the page and in the Worker, no second cache token. It sits verbatim between the BEGIN/END markers below
   (bidi-js 1.1.0, MIT, Unicode Bidirectional Algorithm 13.0; vendor/bidi-js-LICENSE.txt); tests/charm-nest/world-shape.cjs checks its sha256.
   bidi() passes all 91,699 rows of Unicode's BidiCharacterTest-13.0.0.txt (and 91,707 of the 18.0 file): levels, paragraph level and visual order. One thing bidi-js gets wrong is corrected around it: it classes every
   UTF-16 unit on its own, so emoji and every other supplementary-plane character count as left-to-right letters; those are replaced by BMP stand-ins of their real Bidi_Class (DerivedBidiClass-18.0.0.txt) before the call.
   The WebAssembly module is vendor/hb.wasm = dist/harfbuzz.wasm of npm harfbuzzjs 1.6.3, HarfBuzz 14.6.0 (MIT; vendor/harfbuzz-LICENSE.txt), 441 KB, sha256 5f67e3bd...; the ~100 lines that call its C
   exports (blob/face/font/buffer/shape/draw) are below, harfbuzzjs's own 80 KB JavaScript wrapper is not needed. Its five Emscripten imports are stubbed here.
   Rebuild (the data below is generated, nothing else is hand-edited): npm install harfbuzzjs@1.6.3 bidi-js@1.1.0; copy node_modules/harfbuzzjs/dist/harfbuzz.wasm to vendor/hb.wasm; paste node_modules/bidi-js/dist/bidi.min.js
   (without its final newline) between the markers; SCRIPT_DATA = the ranges of fontTools.unicodedata.Scripts (pip install fonttools; 1732 ranges, 'rows' = base36 "delta-from-previous-start.script-index");
   ASTRAL = the non-L ranges at U+10000..10FFFF of DerivedBidiClass.txt (@missing defaults first, then the listed lines; 289 ranges, base36 "delta.length-1.class-index" in the order L R AL EN ES ET AN CS NSM BN B S WS ON ...).
   Check after any change: node tests/charm-nest/world-shape.cjs (BIDI_CHAR_TEST=/path/BidiCharacterTest.txt for the whole conformance file; PW_DIR=<playwright dir> also runs Chromium).

   Limits (documented, not hidden):
   - Horizontal text only: vertical writing (tategaki, vert/vrt2 glyph forms and vertical advances) and Mongolian/Phags-pa are not done; a CJK line is set left to right.
   - One font per run: the caller splits a line into runs by script/font (a Hebrew word inside an Arabic line is its own run); no font fallback inside shapeRun (missing -> reported).
   - Features are the font's defaults (what a browser applies), plus the optional `features` string; stylistic sets (ss01 ...), small caps, swashes and other optional features are only on when asked.
   - Arabic: joining, mandatory ligatures (lam-alef), kerning and mark positioning are HarfBuzz's; tatweel justification (kashida) is not applied; stacked tashkil quality depends on the font's mark2mark lookups
     (Noto Sans Arabic has them, a font without them overprints marks).
   - Indic: HarfBuzz's USE / Indic shapers do reordering, reph, conjunct and half forms with the font's GSUB; an invalid sequence (an orphan vowel sign) gets the dotted circle U+25CC like in a browser.
   - Colour fonts (COLR/CBDT/sbix) are drawn as their plain outline (this is a one-colour laser).
   - Glyph outlines are the font's unhinted outlines; the draw order of contours is the font's.
*/
(function(root,factory){if(typeof module==='object'&&module.exports)module.exports=factory();else root.CharmNestShape=factory();})(typeof self!=='undefined'?self:this,function(){
  'use strict';
  const nodeRequire=(typeof module==='object'&&module&&typeof require==='function')?require:null;

  /* ---------- bidi-js (verbatim; run once at init so the first line pays nothing) ---------- */
  // BEGIN bidi-js 1.1.0 dist/bidi.min.js
  // (the UMD wrapper below finds its own `module` and `exports`, never ours, and leaks no global; no eval, no Function)
  const bidiUmd=function(module,exports,define){
!function(r,e){"object"==typeof exports&&"undefined"!=typeof module?module.exports=e():"function"==typeof define&&define.amd?define(e):(r="undefined"!=typeof globalThis?globalThis:r||self).bidi_js=e()}(this,function(){"use strict";return function(){return function(r){var e={R:"13k,1a,2,3,3,2+1j,ch+16,a+1,5+2,2+n,5,a,4,6+16,4+3,h+1b,4mo,179q,2+9,2+11,2i9+7y,2+68,4,3+4,5+13,4+3,2+4k,3+29,8+cf,1t+7z,w+17,3+3m,1t+3z,16o1+5r,8+30,8+mc,29+1r,29+4v,75+73",EN:"1c+9,3d+1,6,187+9,513,4+5,7+9,sf+j,175h+9,qw+q,161f+1d,4xt+a,25i+9",ES:"17,2,6dp+1,f+1,av,16vr,mx+1,4o,2",ET:"z+2,3h+3,b+1,ym,3e+1,2o,p4+1,8,6u,7c,g6,1wc,1n9+4,30+1b,2n,6d,qhx+1,h0m,a+1,49+2,63+1,4+1,6bb+3,12jj",AN:"16o+5,2j+9,2+1,35,ed,1ff2+9,87+u",CS:"18,2+1,b,2u,12k,55v,l,17v0,2,3,53,2+1,b",B:"a,3,f+2,2v,690",S:"9,2,k",WS:"c,k,4f4,1vk+a,u,1j,335",ON:"x+1,4+4,h+5,r+5,r+3,z,5+3,2+1,2+1,5,2+2,3+4,o,w,ci+1,8+d,3+d,6+8,2+g,39+1,9,6+1,2,33,b8,3+1,3c+1,7+1,5r,b,7h+3,sa+5,2,3i+6,jg+3,ur+9,2v,ij+1,9g+9,7+a,8m,4+1,49+x,14u,2+2,c+2,e+2,e+2,e+1,i+n,e+e,2+p,u+2,e+2,36+1,2+3,2+1,b,2+2,6+5,2,2,2,h+1,5+4,6+3,3+f,16+2,5+3l,3+81,1y+p,2+40,q+a,m+13,2r+ch,2+9e,75+hf,3+v,2+2w,6e+5,f+6,75+2a,1a+p,2+2g,d+5x,r+b,6+3,4+o,g,6+1,6+2,2k+1,4,2j,5h+z,1m+1,1e+f,t+2,1f+e,d+3,4o+3,2s+1,w,535+1r,h3l+1i,93+2,2s,b+1,3l+x,2v,4g+3,21+3,kz+1,g5v+1,5a,j+9,n+v,2,3,2+8,2+1,3+2,2,3,46+1,4+4,h+5,r+5,r+a,3h+2,4+6,b+4,78,1r+24,4+c,4,1hb,ey+6,103+j,16j+c,1ux+7,5+g,fsh,jdq+1t,4,57+2e,p1,1m,1m,1m,1m,4kt+1,7j+17,5+2r,d+e,3+e,2+e,2+10,m+4,w,1n+5,1q,4z+5,4b+rb,9+c,4+c,4+37,d+2g,8+b,l+b,5+1j,9+9,7+13,9+t,3+1,27+3c,2+29,2+3q,d+d,3+4,4+2,6+6,a+o,8+6,a+2,e+6,16+42,2+1i",BN:"0+8,6+d,2s+5,2+p,e,4m9,1kt+2,2b+5,5+5,17q9+v,7k,6p+8,6+1,119d+3,440+7,96s+1,1ekf+1,1ekf+1,1ekf+1,1ekf+1,1ekf+1,1ekf+1,1ekf+1,1ekf+1,1ekf+1,1ekf+1,1ekf+1,1ekf+75,6p+2rz,1ben+1,1ekf+1,1ekf+1",NSM:"lc+33,7o+6,7c+18,2,2+1,2+1,2,21+a,1d+k,h,2u+6,3+5,3+1,2+3,10,v+q,2k+a,1n+8,a,p+3,2+8,2+2,2+4,18+2,3c+e,2+v,1k,2,5+7,5,4+6,b+1,u,1n,5+3,9,l+1,r,3+1,1m,5+1,5+1,3+2,4,v+1,4,c+1,1m,5+4,2+1,5,l+1,n+5,2,1n,3,2+3,9,8+1,c+1,v,1q,d,1f,4,1m+2,6+2,2+3,8+1,c+1,u,1n,g+1,l+1,t+1,1m+1,5+3,9,l+1,u,21,8+2,2,2j,3+6,d+7,2r,3+8,c+5,23+1,s,2,2,1k+d,2+4,2+1,6+a,2+z,a,2v+3,2+5,2+1,3+1,q+1,5+2,h+3,e,3+1,7,g,jk+2,qb+2,u+2,u+1,v+1,1t+1,2+6,9,3+a,a,1a+2,3c+1,z,3b+2,5+1,a,7+2,64+1,3,1n,2+6,2,2,3+7,7+9,3,1d+g,1s+3,1d,2+4,2,6,15+8,d+1,x+3,3+1,2+2,1l,2+1,4,2+2,1n+7,3+1,49+2,2+c,2+6,5,7,4+1,5j+1l,2+4,k1+w,2db+2,3y,2p+v,ff+3,30+1,n9x+3,2+9,x+1,29+1,7l,4,5,q+1,6,48+1,r+h,e,13+7,q+a,1b+2,1d,3+3,3+1,14,1w+5,3+1,3+1,d,9,1c,1g,2+2,3+1,6+1,2,17+1,9,6n,3,5,fn5,ki+f,h+f,r2,6b,46+4,1af+2,2+1,6+3,15+2,5,4m+1,fy+3,as+1,4a+a,4x,1j+e,1l+2,1e+3,3+1,1y+2,11+4,2+7,1r,d+1,1h+8,b+3,3,2o+2,3,2+1,7,4h,4+7,m+1,1m+1,4,12+6,4+4,5g+7,3+2,2,o,2d+5,2,5+1,2+1,6n+3,7+1,2+1,s+1,2e+7,3,2+1,2z,2,3+5,2,2u+2,3+3,2+4,78+8,2+1,75+1,2,5,41+3,3+1,5,x+5,3+1,15+5,3+3,9,a+5,3+2,1b+c,2+1,bb+6,2+5,2d+l,3+6,2+1,2+1,3f+5,4,2+1,2+6,2,21+1,4,2,9o+1,f0c+4,1o+6,t5,1s+3,2a,f5l+1,43t+2,i+7,3+6,v+3,45+2,1j0+1i,5+1d,9,f,n+4,2+e,11t+6,2+g,3+6,2+1,2+4,7a+6,c6+3,15t+6,32+6,gzhy+6n",AL:"16w,3,2,e+1b,z+2,2+2s,g+1,8+1,b+m,2+t,s+2i,c+e,4h+f,1d+1e,1bwe+dp,3+3z,x+c,2+1,35+3y,2rm+z,5+7,b+5,dt+l,c+u,17nl+27,1t+27,4x+6n,3+d",LRO:"6ct",RLO:"6cu",LRE:"6cq",RLE:"6cr",PDF:"6cs",LRI:"6ee",RLI:"6ef",FSI:"6eg",PDI:"6eh"},f={},a={};f.L=1,a[1]="L",Object.keys(e).forEach(function(r,e){f[r]=1<<e+1,a[f[r]]=r}),Object.freeze(f);var n=f.LRI|f.RLI|f.FSI,i=f.L|f.R|f.AL,v=f.B|f.S|f.WS|f.ON|f.FSI|f.LRI|f.RLI|f.PDI,o=f.BN|f.RLE|f.LRE|f.RLO|f.LRO|f.PDF,t=f.S|f.WS|f.B|n|f.PDI|o,u=null;function l(r){return function(){if(!u){u=new Map;var r=0;for(var a in e)if(e.hasOwnProperty(a))for(var n=e[a],i="",v=void 0,o=!1,t=0,l=0;l<=n.length+1;l+=1){var c=n[l];if(","!==c&&l!==n.length)"+"===c?(o=!0,t=r=t+parseInt(i,36),i=""):i+=c;else{o?v=r+parseInt(i,36):(t=r=t+parseInt(i,36),v=r),o=!1,i="",t=v;for(var d=r;d<v+1;d+=1)u.set(d,f[a])}}}}(),u.get(r.codePointAt(0))||f.L}var c,d,s,b="14>1,1e>2,u>2,2wt>1,1>1,1ge>1,1wp>1,1j>1,f>1,hm>1,1>1,u>1,u6>1,1>1,+5,28>1,w>1,1>1,+3,b8>1,1>1,+3,1>3,-1>-1,3>1,1>1,+2,1s>1,1>1,x>1,th>1,1>1,+2,db>1,1>1,+3,3>1,1>1,+2,14qm>1,1>1,+1,4q>1,1e>2,u>2,2>1,+1",h="6f1>-6dx,6dy>-6dx,6ec>-6ed,6ee>-6ed,6ww>2jj,-2ji>2jj,14r4>-1e7l,1e7m>-1e7l,1e7m>-1e5c,1e5d>-1e5b,1e5c>-14qx,14qy>-14qx,14vn>-1ecg,1ech>-1ecg,1edu>-1ecg,1eci>-1ecg,1eda>-1ecg,1eci>-1ecg,1eci>-168q,168r>-168q,168s>-14ye,14yf>-14ye";function k(r,e){var f,a=0,n=new Map,i=e&&new Map;return r.split(",").forEach(function r(v){if(-1!==v.indexOf("+"))for(var o=+v;o--;)r(f);else{f=v;var t=v.split(">"),u=t[0],l=t[1];u=String.fromCodePoint(a+=parseInt(u,36)),l=String.fromCodePoint(a+=parseInt(l,36)),n.set(u,l),e&&i.set(l,u)}}),{map:n,reverseMap:i}}function m(){if(!c){var r=k(b,!0),e=r.map,f=r.reverseMap;c=e,d=f,s=k(h,!1).map}}function j(r){return m(),c.get(r)||null}function p(r){return m(),d.get(r)||null}function g(r){return m(),s.get(r)||null}var q=f.L,w=f.R,y=f.EN,x=f.ES,_=f.ET,M=f.AN,I=f.CS,z=f.B,L=f.S,S=f.ON,R=f.BN,O=f.NSM,E=f.AL,N=f.LRO,T=f.RLO,A=f.LRE,D=f.RLE,P=f.PDF,W=f.LRI,B=f.RLI,F=f.FSI,U=f.PDI;var C;function G(r){return function(){if(!C){var r=k("14>1,j>2,t>2,u>2,1a>g,2v3>1,1>1,1ge>1,1wd>1,b>1,1j>1,f>1,ai>3,-2>3,+1,8>1k0,-1jq>1y7,-1y6>1hf,-1he>1h6,-1h5>1ha,-1h8>1qi,-1pu>1,6>3u,-3s>7,6>1,1>1,f>1,1>1,+2,3>1,1>1,+13,4>1,1>1,6>1eo,-1ee>1,3>1mg,-1me>1mk,-1mj>1mi,-1mg>1mi,-1md>1,1>1,+2,1>10k,-103>1,1>1,4>1,5>1,1>1,+10,3>1,1>8,-7>8,+1,-6>7,+1,a>1,1>1,u>1,u6>1,1>1,+5,26>1,1>1,2>1,2>2,8>1,7>1,4>1,1>1,+5,b8>1,1>1,+3,1>3,-2>1,2>1,1>1,+2,c>1,3>1,1>1,+2,h>1,3>1,a>1,1>1,2>1,3>1,1>1,d>1,f>1,3>1,1a>1,1>1,6>1,7>1,13>1,k>1,1>1,+19,4>1,1>1,+2,2>1,1>1,+18,m>1,a>1,1>1,lk>1,1>1,4>1,2>1,f>1,3>1,1>1,+3,db>1,1>1,+3,3>1,1>1,+2,14qm>1,1>1,+1,6>1,4j>1,j>2,t>2,u>2,2>1,+1",!0),e=r.map;r.reverseMap.forEach(function(r,f){e.set(f,r)}),C=e}}(),C.get(r)||null}function H(r,e,f,a){var n=r.length;f=Math.max(0,null==f?0:+f),a=Math.min(n-1,null==a?n-1:+a);var i=[];return e.paragraphs.forEach(function(n){var v=Math.max(f,n.start),o=Math.min(a,n.end);if(v<o){for(var u=e.levels.slice(v,o+1),c=o;c>=v&&l(r[c])&t;c--)u[c]=n.level;for(var d=n.level,s=1/0,b=0;b<u.length;b++){var h=u[b];h>d&&(d=h),h<s&&(s=1|h)}for(var k=d;k>=s;k--)for(var m=0;m<u.length;m++)if(u[m]>=k){for(var j=m;m+1<u.length&&u[m+1]>=k;)m++;m>j&&i.push([j+v,m+v])}}}),i}function J(r,e,f,a){for(var n=H(r,e,f,a),i=[],v=0;v<r.length;v++)i[v]=v;return n.forEach(function(r){for(var e=r[0],f=r[1],a=i.slice(e,f+1),n=a.length;n--;)i[f-n]=a[n]}),i}return r.closingToOpeningBracket=p,r.getBidiCharType=l,r.getBidiCharTypeName=function(r){return a[l(r)]},r.getCanonicalBracket=g,r.getEmbeddingLevels=function(r,e){for(var f=new Uint32Array(r.length),a=0;a<r.length;a++)f[a]=l(r[a]);var u=new Map;function c(r,e){var a=f[r];f[r]=e,u.set(a,u.get(a)-1),a&v&&u.set(v,u.get(v)-1),u.set(e,(u.get(e)||0)+1),e&v&&u.set(v,(u.get(v)||0)+1)}for(var d=new Uint8Array(r.length),s=new Map,b=[],h=null,k=0;k<r.length;k++)h||b.push(h={start:k,end:r.length-1,level:"rtl"===e?1:"ltr"===e?0:Fe(k,!1)}),f[k]&z&&(h.end=k,h=null);for(var m=D|A|T|N|n|U|P|z,C=function(r){return r+(1&r?1:2)},G=function(r){return r+(1&r?2:1)},H=0;H<b.length;H++){var J=[{i:(h=b[H]).level,v:0,o:0}],K=void 0,Q=0,V=0,X=0;u.clear();for(var Y=h.start;Y<=h.end;Y++){var Z=f[Y];if(K=J[J.length-1],u.set(Z,(u.get(Z)||0)+1),Z&v&&u.set(v,(u.get(v)||0)+1),Z&m)if(Z&(D|A)){d[Y]=K.i;var $=(Z===D?G:C)(K.i);$<=125&&!Q&&!V?J.push({i:$,v:0,o:0}):Q||V++}else if(Z&(T|N)){d[Y]=K.i;var rr=(Z===T?G:C)(K.i);rr<=125&&!Q&&!V?J.push({i:rr,v:Z&T?w:q,o:0}):Q||V++}else if(Z&n){Z&F&&(Z=1===Fe(Y+1,!0)?B:W),d[Y]=K.i,K.v&&c(Y,K.v);var er=(Z===B?G:C)(K.i);er<=125&&0===Q&&0===V?(X++,J.push({i:er,v:0,o:1,t:Y})):Q++}else if(Z&U){if(Q>0)Q--;else if(X>0){for(V=0;!J[J.length-1].o;)J.pop();var fr=J[J.length-1].t;null!=fr&&(s.set(fr,Y),s.set(Y,fr)),J.pop(),X--}K=J[J.length-1],d[Y]=K.i,K.v&&c(Y,K.v)}else Z&P?(0===Q&&(V>0?V--:!K.o&&J.length>1&&(J.pop(),K=J[J.length-1])),d[Y]=K.i):Z&z&&(d[Y]=h.level);else d[Y]=K.i,K.v&&Z!==R&&c(Y,K.v)}for(var ar=[],nr=null,ir=h.start;ir<=h.end;ir++){var vr=f[ir];if(!(vr&o)){var or=d[ir],tr=vr&n,ur=vr===U;nr&&or===nr.i?(nr.u=ir,nr.l=tr):ar.push(nr={h:ir,u:ir,i:or,k:ur,l:tr})}}for(var lr=[],cr=0;cr<ar.length;cr++){var dr=ar[cr];if(!dr.k||dr.k&&!s.has(dr.h)){for(var sr=[nr=dr],br=void 0;nr&&nr.l&&null!=(br=s.get(nr.u));)for(var hr=cr+1;hr<ar.length;hr++)if(ar[hr].h===br){sr.push(nr=ar[hr]);break}for(var kr=[],mr=0;mr<sr.length;mr++)for(var jr=sr[mr],pr=jr.h;pr<=jr.u;pr++)kr.push(pr);for(var gr=d[kr[0]],qr=h.level,wr=kr[0]-1;wr>=0;wr--)if(!(f[wr]&o)){qr=d[wr];break}var yr=kr[kr.length-1],xr=d[yr],_r=h.level;if(!(f[yr]&n))for(var Mr=yr+1;Mr<=h.end;Mr++)if(!(f[Mr]&o)){_r=d[Mr];break}lr.push({m:kr,j:Math.max(qr,gr)%2?w:q,p:Math.max(_r,xr)%2?w:q})}}for(var Ir=0;Ir<lr.length;Ir++){var zr=lr[Ir],Lr=zr.m,Sr=zr.j,Rr=zr.p,Or=1&d[Lr[0]]?w:q;if(u.get(O))for(var Er=0;Er<Lr.length;Er++){var Nr=Lr[Er];if(f[Nr]&O){for(var Tr=Sr,Ar=Er-1;Ar>=0;Ar--)if(!(f[Lr[Ar]]&o)){Tr=f[Lr[Ar]];break}c(Nr,Tr&(n|U)?S:Tr)}}if(u.get(y))for(var Dr=0;Dr<Lr.length;Dr++){var Pr=Lr[Dr];if(f[Pr]&y)for(var Wr=Dr-1;Wr>=-1;Wr--){var Br=-1===Wr?Sr:f[Lr[Wr]];if(Br&i){Br===E&&c(Pr,M);break}}}if(u.get(E))for(var Fr=0;Fr<Lr.length;Fr++){var Ur=Lr[Fr];f[Ur]&E&&c(Ur,w)}if(u.get(x)||u.get(I))for(var Cr=1;Cr<Lr.length-1;Cr++){var Gr=Lr[Cr];if(f[Gr]&(x|I)){for(var Hr=0,Jr=0,Kr=Cr-1;Kr>=0&&(Hr=f[Lr[Kr]])&o;Kr--);for(var Qr=Cr+1;Qr<Lr.length&&(Jr=f[Lr[Qr]])&o;Qr++);Hr===Jr&&(f[Gr]===x?Hr===y:Hr&(y|M))&&c(Gr,Hr)}}if(u.get(y))for(var Vr=0;Vr<Lr.length;Vr++){var Xr=Lr[Vr];if(f[Xr]&y){for(var Yr=Vr-1;Yr>=0&&f[Lr[Yr]]&(_|o);Yr--)c(Lr[Yr],y);for(Vr++;Vr<Lr.length&&f[Lr[Vr]]&(_|o|y);Vr++)f[Lr[Vr]]!==y&&c(Lr[Vr],y)}}if(u.get(_)||u.get(x)||u.get(I))for(var Zr=0;Zr<Lr.length;Zr++){var $r=Lr[Zr];if(f[$r]&(_|x|I)){c($r,S);for(var re=Zr-1;re>=0&&f[Lr[re]]&o;re--)c(Lr[re],S);for(var ee=Zr+1;ee<Lr.length&&f[Lr[ee]]&o;ee++)c(Lr[ee],S)}}if(u.get(y))for(var fe=0,ae=Sr;fe<Lr.length;fe++){var ne=Lr[fe],ie=f[ne];ie&y?ae===q&&c(ne,q):ie&i&&(ae=ie)}if(u.get(v)){for(var ve=w|y|M,oe=ve|q,te=[],ue=[],le=0;le<Lr.length;le++)if(f[Lr[le]]&v){var ce=r[Lr[le]],de=void 0;if(null!==j(ce)){if(!(ue.length<63))break;ue.push({char:ce,seqIndex:le})}else if(null!==(de=p(ce)))for(var se=ue.length-1;se>=0;se--){var be=ue[se].char;if(be===de||be===p(g(ce))||j(g(be))===ce){te.push([ue[se].seqIndex,le]),ue.length=se;break}}}te.sort(function(r,e){return r[0]-e[0]});for(var he=0;he<te.length;he++){for(var ke=te[he],me=ke[0],je=ke[1],pe=!1,ge=0,qe=me+1;qe<je;qe++){var we=Lr[qe];if(f[we]&oe){pe=!0;var ye=f[we]&ve?w:q;if(ye===Or){ge=ye;break}}}if(pe&&!ge){ge=Sr;for(var xe=me-1;xe>=0;xe--){var _e=Lr[xe];if(f[_e]&oe){var Me=f[_e]&ve?w:q;ge=Me!==Or?Me:Or;break}}}if(ge){if(f[Lr[me]]=f[Lr[je]]=ge,ge!==Or)for(var Ie=me+1;Ie<Lr.length;Ie++)if(!(f[Lr[Ie]]&o)){l(r[Lr[Ie]])&O&&(f[Lr[Ie]]=ge);break}if(ge!==Or)for(var ze=je+1;ze<Lr.length;ze++)if(!(f[Lr[ze]]&o)){l(r[Lr[ze]])&O&&(f[Lr[ze]]=ge);break}}}for(var Le=0;Le<Lr.length;Le++)if(f[Lr[Le]]&v){for(var Se=Le,Re=Le,Oe=Sr,Ee=Le-1;Ee>=0;Ee--){if(!(f[Lr[Ee]]&o)){Oe=f[Lr[Ee]]&ve?w:q;break}Se=Ee}for(var Ne=Rr,Te=Le+1;Te<Lr.length;Te++){if(!(f[Lr[Te]]&(v|o))){Ne=f[Lr[Te]]&ve?w:q;break}Re=Te}for(var Ae=Se;Ae<=Re;Ae++)f[Lr[Ae]]=Oe===Ne?Oe:Or;Le=Re}}}for(var De=h.start;De<=h.end;De++){var Pe=d[De],We=f[De];if(1&Pe?We&(q|y|M)&&d[De]++:We&w?d[De]++:We&(M|y)&&(d[De]+=2),We&o&&(d[De]=0===De?h.level:d[De-1]),De===h.end||l(r[De])&(L|z))for(var Be=De;Be>=0&&l(r[Be])&t;Be--)d[Be]=h.level}}return{levels:d,paragraphs:b};function Fe(e,a){for(var i=e;i<r.length;i++){var v=f[i];if(v&(w|E))return 1;if(v&(z|q)||a&&v===U)return 0;if(v&n){var o=Ue(i);i=-1===o?r.length:o}}return 0}function Ue(e){for(var a=1,i=e+1;i<r.length;i++){var v=f[i];if(v&z)break;if(v&U){if(0===--a)return i}else v&n&&a++}return-1}},r.getMirroredCharacter=G,r.getMirroredCharactersMap=function(r,e,f,a){var n=r.length;f=Math.max(0,null==f?0:+f),a=Math.min(n-1,null==a?n-1:+a);for(var i=new Map,v=f;v<=a;v++)if(1&e[v]){var o=G(r[v]);null!==o&&i.set(v,o)}return i},r.getReorderSegments=H,r.getReorderedIndices=J,r.getReorderedString=function(r,e,f,a){var n=J(r,e,f,a),i=[].concat(r);return n.forEach(function(f,a){i[a]=(1&e.levels[f]?G(r[f]):null)||r[f]}),i.join("")},r.openingToClosingBracket=j,Object.defineProperty(r,"q",{value:!0}),r}({})}});
  };
  // END bidi-js
  let bidiLib=null;
  const getBidi=()=>{
    if(!bidiLib){const m={exports:{}};bidiUmd.call({},m,m.exports,undefined);bidiLib=m.exports();}
    return bidiLib;
  };

  /* ---------- Unicode scripts ---------- */
  const SCRIPT_DATA={"names":["Zyyy","Latn","Bopo","Zinh","Grek","Zzzz","Copt","Cyrl","Armn","Hebr","Arab","Syrc","Thaa","Nkoo","Samr","Mand","Deva","Beng","Guru","Gujr","Orya","Taml","Telu","Knda","Mlym","Sinh","Thai","Laoo","Tibt","Mymr","Geor","Hang","Ethi","Cher","Cans","Ogam","Runr","Tglg","Hano","Buhd","Tagb","Khmr","Mong","Limb","Tale","Talu","Bugi","Lana","Bali","Sund","Batk","Lepc","Olck","Brai","Glag","Tfng","Hani","Hira","Kana","Yiii","Lisu","Vaii","Bamu","Sylo","Phag","Saur","Kali","Rjng","Java","Cham","Tavt","Mtei","Linb","Lyci","Cari","Ital","Goth","Perm","Ugar","Xpeo","Dsrt","Shaw","Osma","Osge","Elba","Aghb","Vith","Todr","Lina","Cprt","Armi","Palm","Nbat","Hatr","Phnx","Lydi","Sidt","Mero","Merc","Khar","Sarb","Narb","Mani","Avst","Prti","Phli","Phlp","Orkh","Hung","Rohg","Gara","Yezi","Sogo","Sogd","Ougr","Chrs","Elym","Brah","Kthi","Sora","Cakm","Mahj","Shrd","Khoj","Mult","Sind","Gran","Tutg","Newa","Tirh","Sidd","Modi","Takr","Ahom","Dogr","Wara","Diak","Nand","Zanb","Soyo","Pauc","Sunu","Bhks","Marc","Gonm","Gong","Tols","Maka","Kawi","Xsux","Pcun","Cpmn","Egyp","Hluw","Gukh","Mroo","Tnsa","Bass","Hmng","Krai","Medf","Berf","Plrd","Tang","Nshu","Kits","Jurc","Dupl","Sgnw","Hmnp","Toto","Wcho","Nagm","Onao","Tayo","Mend","Adlm","Seal"],"rows":"0.0,1t.1,q.0,6.1,q.0,1b.1,1.0,f.1,1.0,5.1,n.0,1.1,v.0,1.1,ch.0,13.1,5.0,5.2,2.0,k.3,34.4,4.0,1.4,3.5,2.4,4.0,1.4,1.5,4.4,1.0,1.4,1.0,1.4,3.5,1.4,1.5,1.4,k.5,1.4,1r.6,e.4,g.7,3p.3,2.7,4p.5,1.8,12.5,1.8,1k.5,1.9,1l.5,6.9,r.5,4.9,6.5,b.a,5.0,1.a,6.0,1.a,e.0,1.a,3.0,1.a,w.0,1.a,a.3,b.a,q.3,1.a,30.0,1.a,y.b,e.5,1.b,1o.5,2.b,3.a,1c.c,1e.5,e.d,1n.5,2.d,3.e,1a.5,2.e,f.5,1.f,s.5,2.f,1.5,1.b,b.5,5.a,y.5,5.a,23.0,1.a,t.g,29.3,4.g,f.0,2.g,q.h,4.5,1.h,8.5,2.h,2.5,2.h,m.5,1.h,7.5,1.h,1.5,3.h,4.5,2.h,9.5,2.h,2.5,2.h,4.5,8.h,1.5,4.h,2.5,1.h,5.5,2.h,p.5,2.i,3.5,1.i,6.5,4.i,2.5,2.i,m.5,1.i,7.5,1.i,2.5,1.i,2.5,1.i,2.5,2.i,1.5,1.i,5.5,4.i,2.5,2.i,3.5,3.i,1.5,7.i,4.5,1.i,1.5,7.i,h.5,a.j,3.5,1.j,9.5,1.j,3.5,1.j,m.5,1.j,7.5,1.j,2.5,1.j,5.5,2.j,a.5,1.j,3.5,1.j,3.5,2.j,1.5,f.j,4.5,2.j,c.5,7.j,7.5,1.k,3.5,1.k,8.5,2.k,2.5,2.k,m.5,1.k,7.5,1.k,2.5,1.k,5.5,2.k,9.5,2.k,2.5,2.k,3.5,5.k,5.5,4.k,2.5,1.k,5.5,2.k,i.5,a.l,2.5,1.l,6.5,3.l,3.5,1.l,4.5,3.l,2.5,1.l,1.5,1.l,2.5,3.l,2.5,3.l,3.5,3.l,c.5,4.l,5.5,3.l,3.5,1.l,4.5,2.l,1.5,6.l,1.5,e.l,l.5,5.m,d.5,1.m,3.5,1.m,n.5,1.m,g.5,2.m,9.5,1.m,3.5,1.m,4.5,7.m,2.5,1.m,3.5,1.m,2.5,2.m,4.5,2.m,a.5,7.m,9.n,d.5,1.n,3.5,1.n,n.5,1.n,a.5,1.n,5.5,2.n,9.5,1.n,3.5,1.n,4.5,7.n,2.5,5.n,3.5,1.n,4.5,2.n,a.5,1.n,3.5,c.o,d.5,1.o,3.5,1.o,1f.5,1.o,3.5,1.o,6.5,4.o,g.5,2.o,q.5,1.p,3.5,1.p,i.5,3.p,o.5,1.p,9.5,1.p,1.5,2.p,7.5,3.p,1.5,4.p,6.5,1.p,1.5,1.p,8.5,6.p,a.5,2.p,3.5,c.q,1m.5,4.0,1.q,s.5,11.r,2.5,1.r,1.5,1.r,5.5,1.r,o.5,1.r,1.5,1.r,n.5,2.r,5.5,1.r,1.5,1.r,7.5,1.r,a.5,2.r,4.5,w.s,20.5,1.s,10.5,4.s,13.5,1.s,10.5,1.s,f.5,1.s,7.0,4.s,2.5,11.t,4g.u,12.5,1.u,1.5,5.u,1.5,2.u,17.0,1.u,4.v,74.w,21.5,1.w,4.5,2.w,7.5,1.w,1.5,1.w,4.5,2.w,15.5,1.w,4.5,2.w,x.5,1.w,4.5,2.w,7.5,1.w,1.5,1.w,4.5,2.w,f.5,1.w,1l.5,1.w,4.5,2.w,1v.5,2.w,w.5,3.w,q.5,6.x,2e.5,2.x,6.5,2.y,hs.z,t.5,3.10,23.0,3.10,b.5,7.11,m.5,9.11,1.12,l.0,2.5,9.13,k.5,c.14,d.5,1.14,3.5,1.14,2.5,c.15,2m.5,2.15,a.5,6.15,a.5,6.16,2.0,2.16,1.0,1.16,k.5,6.16,2h.5,7.16,17.5,5.y,1y.5,a.17,v.5,1.17,c.5,4.17,c.5,4.17,1.5,3.17,c.18,u.5,2.18,5.5,b.19,18.5,4.19,q.5,6.19,b.5,3.19,2.15,w.1a,s.5,2.1a,2.1b,1r.5,1.1b,t.5,2.1b,b.5,6.1b,a.5,6.1b,e.5,2.3,1t.5,f.1c,25.5,1.1c,1e.1d,1s.1e,1g.5,8.1e,4.1f,1k.5,3.1f,f.5,3.1f,3.1g,1c.7,b.5,5.u,17.5,2.u,3.1d,8.5,8.3,3.0,1.3,d.0,1.3,7.0,4.3,1.0,6.3,1.0,3.3,2.0,1.5,5.1,12.4,5.7,1.1,1d.4,5.1,4.4,5.1,d.7,1.1,1y.4,1.3,1s.1,74.4,m.5,2.4,6.5,2.4,12.5,2.4,6.5,2.4,8.5,1.4,1.5,1.4,1.5,1.4,1.5,1.4,v.5,2.4,1h.5,1.4,f.5,1.4,e.5,2.4,6.5,1.4,j.5,2.4,3.5,1.4,9.5,1.0,c.3,2.0,2f.5,1.0,b.1,1.5,2.0,b.1,1.0,g.1,g.0,11.5,b.3,x.5,f.0,12.4,1.0,3.1,2.0,6.1,1.0,r.1,1.0,h.1,15.0,3.5,4.0,ii.5,m.0,b.5,l.0,ps.1h,74.0,hg.5,2.0,3u.1i,2o.1,w.6,38.5,5.6,7.u,12.5,1.u,1.5,5.u,1.5,2.1j,1k.5,7.1j,2.5,e.1j,1.w,n.5,9.w,7.5,1.w,7.5,1.w,7.5,1.w,7.5,1.w,7.5,1.w,7.5,1.w,7.5,1.w,7.5,1.7,w.0,2m.5,2.0,4.5,s.1k,q.5,1.1k,2h.5,c.1k,5y.5,q.0,l.1k,1.0,1.1k,1.0,p.1k,9.3,4.v,2.0,8.1k,4.0,4.5,1.1l,2e.5,2.3,2.0,2.1l,3.0,1.1m,2i.0,2.1m,3.5,5.2,17.5,1.v,2m.5,1.0,g.2,w.0,12.5,9.0,1.1m,g.v,v.5,1.0,1s.v,v.0,29.1m,1b.0,1.1m,2g.0,4o.1k,534.0,1s.1k,g74.1n,wd.5,3.1n,1j.5,9.1o,1c.1p,8c.5,k.7,2o.1q,2g.5,8.0,y.1,2u.0,3.1,2b.5,4.1,1.5,e.1,f.1r,19.5,3.0,a.5,6.1s,1k.5,8.1t,1y.5,8.1t,c.5,6.g,w.1u,1a.0,1.1u,1.1v,10.5,b.1v,1.v,t.5,3.1w,26.5,1.0,1.1w,a.5,4.1w,2.t,v.5,1.1x,1j.5,9.1x,e.5,2.1x,a.5,2.1x,4.t,w.1y,1v.5,o.1y,5.1z,n.5,a.w,6.5,2.w,6.5,2.w,6.5,9.w,7.5,1.w,7.5,1.1,17.0,1.1,9.4,1.1,4.0,2.1,2.5,2.x,28.1z,1a.5,2.1z,a.5,6.v,8mc.5,c.v,n.5,4.v,1d.5,6is.1k,a6.5,2.1k,2y.5,12.1,7.5,c.8,5.5,5.9,q.5,1.9,5.5,1.9,1.5,1.9,2.5,1.9,2.5,1.9,a.a,dq.0,2.a,40.5,w.a,g.3,g.0,a.5,6.3,e.7,2.0,z.5,1.0,j.5,1.0,4.5,4.a,5.5,1.a,3r.5,2.0,1.5,1.0,w.1,q.0,6.1,q.0,b.1m,a.0,1.1m,19.0,2.v,v.5,3.v,6.5,2.v,6.5,2.v,6.5,2.v,3.5,3.0,7.5,1.0,7.5,a.0,5.5,2.20,c.5,1.20,q.5,1.20,j.5,1.20,2.5,1.20,f.5,2.20,e.5,y.20,3f.5,5.0,3.5,4.0,19.5,3.0,9.4,27.5,1.0,d.5,3.4,1.5,1b.0,19.3,1.5,3m.21,t.5,3.22,1d.5,f.3,1.0,r.5,4.23,10.5,9.23,3.24,r.5,5.25,17.5,5.26,u.5,1.26,1.27,10.5,4.27,e.5,16.28,28.29,1c.2a,u.5,2.2a,a.5,6.2b,10.5,4.2b,10.5,4.2c,14.5,8.2d,1g.5,b.2d,1.2e,b.5,1.2e,f.5,1.2e,7.5,1.2e,2.5,1.2e,b.5,1.2e,f.5,1.2e,7.5,1.2e,2.5,3.2f,1g.5,c.2g,8n.5,9.2g,m.5,a.2g,8.5,o.1,6.5,1.1,16.5,1.1,e.5,1s.2h,6.5,2.2h,1.5,1.2h,18.5,1.2h,2.5,3.2h,1.5,2.2h,1.2i,m.5,1.2i,9.2j,w.2k,v.5,8.2k,9.5,1c.2l,j.5,1.2l,2.5,5.2l,5.2m,s.5,3.2m,1.2n,q.5,5.2n,1.2o,q.5,12.2p,w.2q,o.5,4.2q,k.5,2.2q,1a.2r,4.5,1.2r,2.5,5.2r,8.5,1.2r,3.5,1.2r,t.5,2.2r,3.5,4.2r,a.5,7.2r,9.5,7.2s,w.2t,w.5,w.2u,13.5,4.2u,c.5,9.2v,1i.5,3.2v,7.2w,m.5,2.2w,8.2x,j.5,5.2x,8.2y,i.5,7.2y,4.5,c.2y,7.5,28.2z,21.5,1j.30,1f.5,d.30,1f.5,7.30,6.31,14.5,8.31,a.5,6.32,12.5,3.32,t.5,8.32,2.5,5s.a,v.5,1.33,16.5,1.33,3.5,2.33,2.5,g.a,6.5,1.a,12.5,1.a,g.34,14.5,8.35,16.5,m.36,q.5,12.37,s.5,k.38,n.5,9.39,26.5,4.39,10.5,9.39,1.3a,1v.5,a.3a,1.5,2.3b,p.5,7.3b,a.5,6.3c,1h.5,1.3c,i.5,8.3d,13.5,9.3e,2o.5,1.p,k.5,b.3f,i.5,1.3f,1b.5,1q.3g,7.5,1.3g,1.5,1.3g,4.5,1.3g,f.5,1.3g,b.5,6.3h,1n.5,5.3h,a.5,6.3i,4.5,1.3i,8.5,2.3i,2.5,2.3i,m.5,1.3i,7.5,1.3i,2.5,1.3i,5.5,1.3,1.3i,9.5,2.3i,2.5,2.3i,3.5,2.3i,1.5,6.3i,1.5,5.3i,7.5,2.3i,7.5,3.3i,5.5,b.3j,a.5,1.3j,1.5,2.3j,1.5,1.3j,12.5,1.3j,a.5,1.3j,1.5,2.3j,1.5,1.3j,4.5,1.3j,a.5,1.3j,2.5,8.3j,2.5,t.3k,2k.5,1.3k,5.5,u.3l,20.5,8.3l,a.5,4m.3m,1i.5,2.3m,12.5,y.3n,1x.5,b.3n,a.5,6.16,d.5,j.3o,1m.5,6.3o,a.5,6.t,k.5,s.3p,r.5,2.3p,f.5,4.3p,n.5,55.3q,1o.5,2s.3r,2b.5,c.3r,1.3s,7.5,2.3s,1.5,2.3s,8.5,1.3s,2.5,1.3s,u.5,1.3s,2.5,2.3s,c.5,9.3s,a.5,1y.3t,8.5,2.3t,1a.5,2.3t,b.5,r.3u,20.5,8.3v,2b.5,d.y,g.3w,1l.5,7.g,b.5,2d.3e,8.5,2g.3x,y.5,e.3x,a.5,6.3y,9.5,1.3y,19.5,1.3y,e.5,a.3y,t.5,3.3z,w.5,2.3z,m.5,1.3z,e.5,21.40,7.5,1.40,2.5,1.40,18.5,3.40,1.5,1.40,2.5,1.40,9.5,8.40,a.5,6.41,6.5,1.41,2.5,1.41,11.5,1.41,2.5,1.41,6.5,7.41,a.5,6.42,18.5,4.42,a.5,6.h,2.5,6m.43,p.5,7.44,h.5,1.44,15.5,3.44,t.5,2d.1o,1.5,f.l,1e.5,d.l,1.45,pm.5,2u.45,90.5,c.45,2g.46,4k.45,1n.5,1s9.47,2r.5,d.48,uu.5,a.48,32z.5,5.49,g7.5,5a1.4a,1m.5,1c6.1q,ft.5,7.4b,v.5,1.4b,a.5,4.4b,2.4c,27.5,1.4c,a.5,6.4d,u.5,2.4d,6.5,a.4e,1y.5,a.4e,a.5,1.4e,7.5,1.4e,l.5,5.4e,j.5,c0.4f,1m.5,5i.4g,2j.5,5.4h,p.5,2.4h,p.5,18.4i,23.5,4.4i,1l.5,7.4i,h.5,1s.4j,1.4k,1.1k,2.4l,1.5,b.1k,7.5,9.4j,5c0.4l,d7.5,10.4l,1.4j,x.5,2n.4j,37.5,d.4m,pe.5,e.4m,1f.5,5y5.1m,4.5,1.1m,7.5,1.1m,2.5,1.1m,1.1l,7z.1m,3.1l,1.1m,5.5,9.1l,1.5,t.1l,3.5,2.1m,1.5,e.1m,5.5,7.4k,b0.5,1s4.4n,2z.5,5.4n,d.5,3.4n,9.5,7.4n,a.5,2.4n,4.0,4.5,318.0,71.5,3.0,c4.5,6.0,n.5,1.0,3.5,8.0,x.5,2.3,1a.5,2.3,n.5,9.0,38.5,1o.0,6u.5,a.0,13.3,2.0,1q.3,3.0,h.3,8.0,2.3,7.0,u.3,4.0,2a.4,1y.5,a.0,b.3,2.0,11.5,1q.0,k.5,c.0,k.5,c.0,2f.5,9.0,p.5,3r.0,2d.5,1.0,1z.5,1.0,2.5,2.0,1.5,2.0,2.5,2.0,4.5,1.0,c.5,1.0,1.5,1.0,7.5,1.0,1t.5,1.0,4.5,2.0,8.5,1.0,7.5,1.0,s.5,1.0,4.5,1.0,5.5,1.0,1.5,3.0,7.5,1.0,9h.5,1.0,84.5,2.0,1e.4o,i4.5,f.4o,5.5,1.4o,f.5,28.0,t.5,rn.1,3m.5,e.1,7.5,1i.1,12.4,2.1,b.1i,7.5,1.1i,h.5,2.1i,7.5,1.1i,2.5,1.1i,5.5,5.7,1q.5,x.7,1.5,34.4p,19.5,3.4p,e.5,2.4p,a.5,4.4p,2.5,8w.4q,v.5,h.4r,1m.5,5.4r,1.5,cw.4s,16.5,5y.4t,17.5,4.4t,1.5,5c.4u,v.5,1.4u,m.5,8.4u,2.5,68.w,7.5,1.w,4.5,1.w,2.5,1.w,f.5,1.4v,5h.5,2.4v,g.5,15.4w,24.5,4.4w,a.5,4.4w,2.5,lt.0,1w.5,24.0,1p.5,5e.a,4.5,1.a,r.5,1.a,2.5,1.a,1.5,2.a,1.5,1.a,a.5,1.a,4.5,1.a,1.5,1.a,1.5,6.a,1.5,4.a,1.5,1.a,1.5,1.a,1.5,1.a,3.5,1.a,2.5,1.a,1.5,2.a,1.5,1.a,1.5,1.a,1.5,1.a,1.5,1.a,1.5,1.a,2.5,1.a,1.5,2.a,4.5,1.a,7.5,1.a,4.5,1.a,4.5,1.a,1.5,1.a,a.5,1.a,h.5,5.a,3.5,1.a,5.5,1.a,h.5,1g.a,2.5,7i.0,18.5,4.0,2s.5,c.0,f.5,2.0,f.5,1.0,f.5,1.0,11.5,a.0,4v.5,1j.0,q.1l,1.0,2.5,d.0,18.5,4.0,9.5,7.0,2.5,e.0,6.5,4a.0,re.5,2.0,h.5,3.0,d.5,3.0,64.5,4.0,c.5,4.0,s.5,4.0,1k.5,8.0,a.5,6.0,14.5,8.0,u.5,2.0,c.5,4.0,2.5,e.0,9.5,13.0,9k.5,8.0,e.5,2.0,d.5,3.0,1z.5,1.0,1.5,3.0,i.5,1.0,d.5,3.0,c.5,5.0,43.5,1.0,2v.5,sl.1k,wyo.5,w.1k,3dr.5,1.1k,4ge.5,2.1k,5rl.5,f.1k,ha.5,1wi.1k,f2.5,15u.1k,3t7.5,5.1k,6ju.5,upy.4x,8qo.5,e2f5.0,1.5,u.0,2o.5,3k.3,6o.5"};   // (Unicode 18.0 Scripts.txt)
  let scriptStarts=null, scriptIdx=null;
  const loadScripts=()=>{
    if(scriptStarts)return;
    const rows=SCRIPT_DATA.rows.split(','), s=new Array(rows.length), v=new Array(rows.length);
    let at=0;
    for(let i=0;i<rows.length;i++){const p=rows[i].indexOf('.');at+=parseInt(rows[i].slice(0,p),36);s[i]=at;v[i]=parseInt(rows[i].slice(p+1),36);}
    scriptStarts=s;scriptIdx=v;
  };
  const scriptOfCodePoint=cp=>{
    loadScripts();
    let lo=0,hi=scriptStarts.length-1;
    while(lo<hi){const mid=(lo+hi+1)>>1;if(scriptStarts[mid]<=cp)lo=mid;else hi=mid-1;}
    return SCRIPT_DATA.names[scriptIdx[lo]];
  };
  // What the base face (Source Sans 3 / the customer's face) already draws, or has nothing to draw.
  const BASE_SCRIPTS={Latn:1,Grek:1,Cyrl:1,Zyyy:1,Zinh:1,Zzzz:1};
  const RTL_SCRIPTS={Arab:1,Hebr:1,Syrc:1,Thaa:1,Nkoo:1,Adlm:1,Mand:1,Samr:1,Rohg:1,Mend:1,Phnx:1,Armi:1,Avst:1,Lydi:1,Nbat:1,Palm:1,Phli:1,Phlp:1,Prti:1,Sarb:1,Sogd:1,Sogo:1,Hung:1,Yezi:1,Chrs:1,Elym:1,Hatr:1,Mani:1,Narb:1,Orkh:1,Ougr:1};
  const scriptsOf=text=>{
    const out={};
    for(const ch of String(text==null?'':text)){const s=scriptOfCodePoint(ch.codePointAt(0));if(s!=='Zyyy'&&s!=='Zinh'&&s!=='Zzzz')out[s]=(out[s]||0)+1;}
    return out;
  };
  const scriptOf=text=>{
    let best=null,bestN=0;const counts=scriptsOf(text);
    for(const s in counts)if(!BASE_SCRIPTS[s]&&counts[s]>bestN){best=s;bestN=counts[s];}   // (object keys keep insertion order: a tie goes to the first met)
    return best;
  };
  // First script of the text that is not common/inherited (the script HarfBuzz would guess), else null.
  const firstScript=text=>{
    for(const ch of text){const s=scriptOfCodePoint(ch.codePointAt(0));if(s!=='Zyyy'&&s!=='Zinh'&&s!=='Zzzz')return s;}
    return null;
  };

  /* ---------- the WebAssembly module ---------- */
  let X=null;              // {ex, mem, table, U8, I32, F32, buf, scratch, dfuncs, buffer}
  let initPromise=null;
  const fonts=Object.create(null);

  const toU8=b=>b==null?null:(ArrayBuffer.isView(b)?new Uint8Array(b.buffer,b.byteOffset,b.byteLength):new Uint8Array(b));
  // The module's memory can grow (and its ArrayBuffer is then replaced) during any call: always read through these.
  const u8=()=>{const b=X.mem.buffer;if(X.buf!==b){X.buf=b;X.U8=new Uint8Array(b);X.U16=new Uint16Array(b);X.I32=new Int32Array(b);X.U32=new Uint32Array(b);}return X.U8;};
  const i32=()=>{u8();return X.I32;};
  const u16=()=>{u8();return X.U16;};
  const u32=()=>{u8();return X.U32;};
  const tag=s=>((s.charCodeAt(0)&255)<<24|(s.charCodeAt(1)&255)<<16|(s.charCodeAt(2)&255)<<8|(s.charCodeAt(3)&255))>>>0;
  const scriptTag=s=>{s=String(s);return tag(s.charAt(0).toUpperCase()+s.slice(1,4).toLowerCase());};   // ('arab' | 'Arab' -> HB_SCRIPT_ARABIC)
  const asciiPtr=s=>{const p=X.ex.malloc(s.length+1),h=u8();for(let i=0;i<s.length;i++)h[p+i]=s.charCodeAt(i)&127;h[p+s.length]=0;return p;};

  // A JS function the module can call through its function table (Emscripten's addFunction: a 1-function wasm wrapper imports the JS function and exports it).
  const lebLen=a=>[a.length%128|128,a.length>>7,...a];
  const WASM_TYPE={i:127,p:127,f:125,d:124,j:126};
  const addFunction=(fn,sig,WA)=>{
    const types=s=>lebLen(Array.from(s,t=>WASM_TYPE[t]));
    const bytes=Uint8Array.of(0,97,115,109,1,0,0,0,1,...lebLen([1,96,...types(sig.slice(1)),...types(sig[0]==='v'?'':sig[0])]),2,7,1,1,101,1,102,0,0,7,5,1,1,102,0,0);
    const wrapped=new WA.Instance(new WA.Module(bytes),{e:{f:fn}}).exports.f;
    const idx=X.table.grow(1);
    X.table.set(idx,wrapped);
    return idx;
  };

  let OUT=null;   // the outline being drawn: flat [op, coords...] with op 0 M(2) 1 L(2) 2 Q(4) 3 C(6) 4 Z(0)
  const setupDraw=WA=>{
    const e=X.ex, d=e.hb_draw_funcs_create();
    e.hb_draw_funcs_set_move_to_func(d,addFunction((a,b,c,x,y)=>{OUT.push(0,x,y);},'vpppffp',WA),0,0);
    e.hb_draw_funcs_set_line_to_func(d,addFunction((a,b,c,x,y)=>{OUT.push(1,x,y);},'vpppffp',WA),0,0);
    e.hb_draw_funcs_set_quadratic_to_func(d,addFunction((a,b,c,x1,y1,x,y)=>{OUT.push(2,x1,y1,x,y);},'vpppffffp',WA),0,0);
    e.hb_draw_funcs_set_cubic_to_func(d,addFunction((a,b,c,x1,y1,x2,y2,x,y)=>{OUT.push(3,x1,y1,x2,y2,x,y);},'vpppffffffp',WA),0,0);
    e.hb_draw_funcs_set_close_path_func(d,addFunction(()=>{OUT.push(4);},'vpppp',WA),0,0);
    return d;
  };

  const instantiate=async(source,WA)=>{
    let mem=null;
    const imports={
      wasi_snapshot_preview1:{proc_exit(code){throw new Error('HarfBuzz exited '+code);}},
      env:{
        _emscripten_runtime_keepalive_clear(){}, _setitimer_js(){return 0;}, _abort_js(){throw new Error('HarfBuzz aborted');},
        emscripten_resize_heap(wanted){
          const old=mem.buffer.byteLength;wanted=wanted>>>0;
          if(wanted<=old)return 1;
          const target=Math.min(Math.max(wanted,old+(old>>2)),2147483648-65536);
          try{mem.grow(Math.ceil((target-old)/65536));return 1;}catch(_){return 0;}
        }
      }
    };
    let instance;
    if(WA.Module&&source instanceof WA.Module)instance=await WA.instantiate(source,imports);
    else{const r=await WA.instantiate(source,imports);instance=r.instance;}
    mem=instance.exports.memory;
    instance.exports.__wasm_call_ctors();
    X={ex:instance.exports,mem,table:instance.exports.__indirect_function_table,buf:null};
    u8();
    X.scratch=X.ex.malloc(1024);         // out-parameters of the C calls
    X.dfuncs=setupDraw(WA);
    X.buffer=X.ex.hb_buffer_create();
    api.ready=true;
  };

  const init=opts=>{
    if(initPromise)return initPromise;
    initPromise=(async()=>{
      opts=opts||{};
      const WA=opts.WebAssembly||(typeof WebAssembly!=='undefined'?WebAssembly:null);
      if(!WA)throw new Error('CharmNestShape.init: WebAssembly is not available here');
      let source=opts.wasm;
      if(source==null&&nodeRequire){try{source=nodeRequire('fs').readFileSync(nodeRequire('path').join(__dirname,'vendor','hb.wasm'));}catch(_){}}
      if(source==null)throw new Error('CharmNestShape.init: pass {wasm} (the bytes of vendor/hb.wasm)');
      if(!(WA.Module&&source instanceof WA.Module))source=toU8(source);
      await instantiate(source,WA);
      {const B=getBidi();B.getMirroredCharacter('(');B.getEmbeddingLevels('a \u05D0 (1)');loadScripts();}   // (the one-time table decoding happens here, not in the first line that is set)
      return api;
    })().catch(e=>{initPromise=null;throw e;});
    return initPromise;
  };

  const need=()=>{if(!X)throw new Error('CharmNestShape: call init() first');};
  const fontOf=id=>{need();const f=fonts[id];if(!f)throw new Error('CharmNestShape: unknown font '+id);return f;};

  /* ---------- fonts ---------- */
  const glyphExtentsGid=(f,gid)=>{
    const e=X.ex;
    if(!e.hb_font_get_glyph_extents(f.font,gid,X.scratch))return null;
    const h=i32(),o=X.scratch>>2,xb=h[o],yb=h[o+1],w=h[o+2],ht=h[o+3];
    return {xMin:xb,yMin:yb+ht,xMax:xb+w,yMax:yb};
  };
  const nominal=(f,cp)=>{
    if(!X.ex.hb_font_get_nominal_glyph(f.font,cp,X.scratch))return 0;
    return u32()[X.scratch>>2];
  };
  const metric=(f,name)=>{
    if(!X.ex.hb_ot_metrics_get_position(f.font,tag(name),X.scratch))return 0;
    return i32()[X.scratch>>2];
  };

  const addFont=(id,bytes,opts)=>{
    need();
    if(fonts[id])return Object.assign({},fonts[id].info);
    const data=toU8(bytes);
    if(!data||data.length<12)throw new Error('CharmNestShape.addFont: no font data for '+id);
    const e=X.ex, ptr=e.malloc(data.length);
    if(!ptr)throw new Error('CharmNestShape.addFont: out of memory for '+id);
    u8().set(data,ptr);
    const blob=e.hb_blob_create(ptr,data.length,2,0,0), face=e.hb_face_create(blob,0), font=e.hb_font_create(face);
    const upem=e.hb_face_get_upem(face), magic=String.fromCharCode(data[0],data[1],data[2],data[3]);
    if(!upem||!(magic==='\0\u0001\0\0'||magic==='OTTO'||magic==='true'||magic==='ttcf')){e.hb_font_destroy(font);e.hb_face_destroy(face);e.hb_blob_destroy(blob);e.free(ptr);throw new Error('CharmNestShape.addFont: '+id+' is not a usable font');}
    const f={id,ptr,blob,face,font,upem,cache:new Map(),info:null};
    if(opts&&opts.variations){
      const vs=String(opts.variations).split(',').map(s=>s.trim()).filter(Boolean);
      const arr=e.malloc(vs.length*8);
      vs.forEach((v,i)=>{const p=asciiPtr(v);e.hb_variation_from_string(p,-1,arr+i*8);e.free(p);});
      e.hb_font_set_variations(font,arr,vs.length);
      e.free(arr);
    }
    fonts[id]=f;
    let asc=metric(f,'hasc'),desc=metric(f,'hdsc'),gap=metric(f,'hlgp');
    if(!asc&&!desc){if(e.hb_font_get_h_extents(font,X.scratch)){const h=i32(),o=X.scratch>>2;asc=h[o];desc=h[o+1];gap=h[o+2];}}
    if(!asc)asc=Math.round(upem*.8);
    if(desc>0)desc=-desc;
    let cap=metric(f,'cpht'),xh=metric(f,'xhgt');
    if(!(cap>0)){const g=nominal(f,72),x=g&&glyphExtentsGid(f,g);cap=x&&x.yMax>0?x.yMax:0;}
    if(!(cap>0))cap=Math.round(asc*.7);
    if(!(xh>0)){const g=nominal(f,120),x=g&&glyphExtentsGid(f,g);xh=x&&x.yMax>0?x.yMax:0;}
    if(!(xh>0))xh=Math.round(asc*.5);
    f.info={id,unitsPerEm:upem,ascender:asc,descender:desc,lineGap:gap||0,capHeight:cap,xHeight:xh};
    // HarfBuzz reads and checks a font's GSUB/GPOS/GDEF on its first shaping (about 100 ms for a CJK face): do it now, while the font loads, not in the first line that is fitted.
    if(!(opts&&opts.warm===false))try{shapeRun(' ',{fontId:id,size:10});}catch(_){}
    return Object.assign({},f.info);
  };
  const removeFont=id=>{
    const f=fonts[id];if(!f)return false;
    const e=X.ex;
    e.hb_font_destroy(f.font);e.hb_face_destroy(f.face);e.hb_blob_destroy(f.blob);e.free(f.ptr);
    delete fonts[id];return true;
  };
  const hasFont=id=>!!fonts[id];

  // Nothing to draw and nothing to cut (zero-width joiners, variation selectors, bidi marks ...) and the line breaks the caller already handles.
  const IGNORABLE=/^[\p{Default_Ignorable_Code_Point}\n\r\u0085\u2028\u2029]$/u;
  const covers=(id,text)=>{
    const f=fontOf(id);
    for(const ch of String(text==null?'':text)){
      if(IGNORABLE.test(ch))continue;
      const cp=ch.codePointAt(0);
      if(nominal(f,cp))continue;
      // (HarfBuzz decomposes a character the font lacks when the font has its parts)
      const d=ch.normalize('NFD');
      if(d!==ch&&[...d].every(c=>IGNORABLE.test(c)||nominal(f,c.codePointAt(0))))continue;
      return false;
    }
    return true;
  };
  const glyphExtents=(id,cp)=>{
    const f=fontOf(id),g=nominal(f,cp);
    return g?glyphExtentsGid(f,g):null;
  };

  /* ---------- shaping ---------- */
  const languages=Object.create(null);
  const languagePtr=lang=>{
    lang=String(lang);
    if(languages[lang]===undefined){const p=asciiPtr(lang);languages[lang]=X.ex.hb_language_from_string(p,-1);X.ex.free(p);}
    return languages[lang];
  };
  const outlineOf=(f,gid)=>{
    let o=f.cache.get(gid);
    if(o)return o;
    OUT=[];
    X.ex.hb_font_draw_glyph(f.font,gid,X.dfuncs,0);
    o=OUT;OUT=null;
    if(f.cache.size>6000)f.cache.clear();
    f.cache.set(gid,o);
    return o;
  };

  // HarfBuzz mirrors ( ) [ ] < > ... by itself when the direction is right-to-left, but bidi() has already mirrored its run texts (rule L4): shapeRun draws the characters it is given,
  // so they are mirrored once more on the way in and HarfBuzz's own pass puts them back. ({mirror:true} leaves HarfBuzz's pass to work on logical text.)
  const mirrorText=text=>{
    let out=null;
    for(let i=0;i<text.length;i++){
      const c=text.charCodeAt(i);
      if((c>=0x590&&c<0x2000)||(c>=0xD800&&c<=0xDFFF))continue;
      const m=getBidi().getMirroredCharacter(text[i]);
      if(m&&m.length===1){if(out===null)out=text.split('');out[i]=m;}
    }
    return out===null?text:out.join('');
  };
  // Script itemization like a browser's: every character has the script of its own, a common or inherited one (digits, spaces, punctuation, combining marks) takes that of the character before it
  // (the first ones that of the character after), and HarfBuzz shapes one script at a time (it picks the font's GSUB/GPOS script table by it: Latin kerning inside a CJK face, kana inside Han ...).
  const itemize=text=>{
    const out=[];let cur=null,from=0,at=0;
    for(const ch of text){
      const sc=scriptOfCodePoint(ch.codePointAt(0));
      if(sc!=='Zyyy'&&sc!=='Zinh'&&sc!=='Zzzz'){
        if(cur===null)cur=sc;
        else if(sc!==cur){out.push([from,at,cur]);from=at;cur=sc;}
      }
      at+=ch.length;
    }
    out.push([from,at,cur]);
    return out;
  };
  const shapeRun=(text,opts)=>{
    opts=opts||{};
    const f=fontOf(opts.fontId);
    text=String(text==null?'':text);
    const size=+opts.size>0?+opts.size:1, k=size/f.upem;
    const result={advance:0,commands:[],glyphs:[],missing:[],dotted:[]};
    if(!text)return result;
    const e=X.ex, n=text.length, buf=X.buffer;
    // opts.script forces one script for the whole run; without it the run is itemized by script
    const items=opts.script?[[0,n,opts.script]]:itemize(text), first=opts.script||firstScript(text);
    const rtl=opts.rtl===undefined||opts.rtl===null?!!(first&&RTL_SCRIPTS[first]):!!opts.rtl;
    const given=rtl&&!opts.mirror?mirrorText(text):text;
    const tp=e.malloc(n*2+2);
    if(!tp)throw new Error('CharmNestShape.shapeRun: out of memory');
    const h=u16(), o16=tp>>1;
    for(let i=0;i<n;i++)h[o16+i]=given.charCodeAt(i);
    let feats=0,nfeat=0;
    if(opts.features){
      const list=(Array.isArray(opts.features)?opts.features:String(opts.features).split(',')).map(s=>String(s).trim()).filter(Boolean);
      feats=e.malloc(list.length*16+16);
      for(const s of list){const p=asciiPtr(s);if(e.hb_feature_from_string(p,-1,feats+nfeat*16))nfeat++;e.free(p);}
    }
    const parts=[];
    for(const [from,to,script] of items){
      e.hb_buffer_reset(buf);
      e.hb_buffer_add_utf16(buf,tp,n,from,to-from);    // (the whole text goes in as context, the item as the content: clusters stay offsets into `text`)
      e.hb_buffer_set_direction(buf,rtl?5:4);
      if(opts.noDottedCircle)e.hb_buffer_set_flags(buf,16);   // (HB_BUFFER_FLAG_DO_NOT_INSERT_DOTTED_CIRCLE)
      if(script)e.hb_buffer_set_script(buf,scriptTag(script));
      if(opts.lang)e.hb_buffer_set_language(buf,languagePtr(opts.lang));
      e.hb_shape(f.font,buf,feats,nfeat);
      const len=e.hb_buffer_get_length(buf), ip=e.hb_buffer_get_glyph_infos(buf,0)>>2, pp=e.hb_buffer_get_glyph_positions(buf,0)>>2, hh=i32(), part=new Array(len);
      for(let i=0;i<len;i++){const a=ip+i*5,b=pp+i*5;part[i]={gid:hh[a],cluster:hh[a+2],adv:hh[b],xo:hh[b+2],yo:hh[b+3]};}
      parts.push(part);
    }
    if(feats)e.free(feats);
    e.free(tp);
    if(rtl)parts.reverse();    // (the items are in logical order; a right-to-left run is drawn from its last item to its first)
    const infos=[].concat(...parts);
    let pen=0;const missing=new Set(), circle=nominal(f,0x25CC);
    for(const g of infos){
      const ox=pen+g.xo*k, oy=0-g.yo*k;
      result.glyphs.push({id:g.gid,cluster:g.cluster,x:ox,y:oy,adv:g.adv*k});
      if(!g.gid){const cp=text.codePointAt(g.cluster);if(cp!==undefined)missing.add(cp);}
      else if(g.gid===circle&&text.codePointAt(g.cluster)!==0x25CC)result.dotted.push(g.cluster);
      else{
        const o=outlineOf(f,g.gid), cmds=result.commands;
        for(let i=0;i<o.length;){
          switch(o[i]){
            case 0:cmds.push({type:'M',x:ox+o[i+1]*k,y:oy-o[i+2]*k});i+=3;break;
            case 1:cmds.push({type:'L',x:ox+o[i+1]*k,y:oy-o[i+2]*k});i+=3;break;
            case 2:cmds.push({type:'Q',x1:ox+o[i+1]*k,y1:oy-o[i+2]*k,x:ox+o[i+3]*k,y:oy-o[i+4]*k});i+=5;break;
            case 3:cmds.push({type:'C',x1:ox+o[i+1]*k,y1:oy-o[i+2]*k,x2:ox+o[i+3]*k,y2:oy-o[i+4]*k,x:ox+o[i+5]*k,y:oy-o[i+6]*k});i+=7;break;
            default:cmds.push({type:'Z'});i+=1;
          }
        }
      }
      pen+=g.adv*k;
    }
    result.advance=pen;result.missing=[...missing];
    return result;
  };

  /* ---------- bidi ---------- */
  // bidi-js 1.1.0 looks every UTF-16 unit up on its own, so a surrogate half is a plain left-to-right letter to it: two emoji inside Hebrew would be set in the wrong order, an Adlam or Phoenician
  // letter would not be right-to-left. The text it is given therefore has every supplementary-plane character replaced by two BMP stand-ins of the same Bidi_Class (same string length, same indexes).
  const ASTRAL={"rows":"75.0.d,1r.24.d,28.c.d,g.0.d,2l.0.8,6b.0.8,1.q.3,45.4.8,wa.7y.1,7z.0.d,1.68.1,69.2.8,3.0.1,1.1.8,2.4.1,5.3.8,4.13.1,14.2.8,3.3.1,4.0.8,1.4k.1,4l.1.8,2.29.1,2a.6.d,7.cf.1,cg.z.2,10.3.8,4.7.2,8.9.6,a.5.2,6.9.6,a.u.1,v.4.8,5.0.d,1.6o.1,6p.u.6,v.17.1,18.1.8,2.i.1,j.a.2,b.4.8,5.8.d,9.m.2,n.f.8,g.1b.1,1c.l.2,m.a.8,b.u.2,v.h.1,i.3.8,4.3d.1,3f.0.8,1j.e.8,q.j.d,u.0.8,3.1.8,c.2.8,1g.3.8,6.1.8,9.0.8,1q.2.8,13.4.8,6.7.8,1y.0.8,d.1.8,1i.8.8,j.3.8,6.0.8,2o.2.8,5.0.8,2.1.8,8.0.8,3.0.8,4e.0.8,4.7.8,t.1.8,1n.1.8,5.0.8,12.6.8,a.4.8,23.5.8,j.0.8,2.0.8,2.0.8,f.1.8,2f.7.8,a.2.8,4.0.8,o.0.8,2d.5.8,7.0.8,5.1.8,3.1.8,6o.3.8,a.1.8,3.1.8,t.1.8,2f.7.8,a.0.8,2.1.8,x.c.d,23.0.8,2.0.8,3.5.8,7.0.8,2u.0.8,2.0.8,3.3.8,5.4.8,7c.8.8,a.1.8,76.1.8,3.0.8,5.0.8,41.3.8,6.1.8,6.0.8,x.5.8,8.1.8,16.5.8,8.3.8,c.0.8,a.5.8,8.2.8,1d.c.8,e.1.8,5k.0.8,2.2.8,4.0.8,5m.6.8,8.5.8,2i.l.8,o.6.8,8.1.8,3.1.8,3g.5.8,9.0.8,2.1.8,3.6.8,8.0.8,21.1.8,5.0.8,2.0.8,2h.0.8,77.1.8,d.1.8,1i.4.8,a.0.8,2.0.8,o.0.8,3f.7.d,8.3.5,4.g.d,40v.0.8,7.e.8,8uv.b.8,f.2.8,1xf.4.8,1s.6.8,tb.0.8,1s.3.8,2b.0.d,2.0.8,f5l.1.8,3.3.9,31c.5x.d,6o.9.3,a.2.d,6.c3.d,ca.m.d,o.2.d,b.w.d,z.19.8,1c.m.8,dz.1.8,1s.2.8,c.7.9,8.7.8,a.6.8,11.3.8,1r.1.d,n.1t.d,1u.2.8,3.0.d,m.1.8,4l.2e.d,qp.0.d,q.0.d,w.0.d,q.0.d,w.0.d,q.0.d,w.0.d,q.0.d,w.0.d,q.0.d,b.1d.3,fm.1i.8,1n.1d.8,1m.0.8,f.0.8,n.4.8,6.e.8,2n.s.d,zk.6.8,8.g.8,j.6.8,8.1.8,3.4.8,2x.0.8,4h.6.8,am.0.8,1q.3.8,j.0.5,dp.3.8,76.1.8,6t.0.8,3.0.8,8.1.8,7.0.8,7f.5r.1,5s.6.8,7.30.1,31.6.8,7.mc.1,md.27.2,28.1r.1,1s.27.2,28.4v.1,4w.6n.2,6o.1.d,2.d.2,e.73.1,74.17.d,1c.2r.d,34.e.d,h.e.d,g.e.d,g.10.d,1b.a.3,b.4.d,10.0.d,1n.5.d,1v.0.d,4z.5.d,4g.rd.d,rg.g.d,k.c.d,g.61.d,63.0.d,5.b.d,g.r.d,w.1j.d,1s.9.d,g.13.d,1c.t.d,w.b.d,g.1.d,g.8.d,1c.9j.d,9s.d.d,g.c.d,g.1y.d,20.0.d,4.h.d,j.c.d,g.b.d,h.42.d,44.2j.d,2k.9.3,a.0.d,sk.1.9,1ekg.1.9,1ekg.1.9,1ekg.1.9,1ekg.1.9,1ekg.1.9,1ekg.1.9,1ekg.1.9,1ekg.1.9,1ekg.1.9,1ekg.1.9,1ekg.1.9,1ekg.75.9,76.6n.8,6o.2rz.9,1e6m.1.9,1ekg.1.9,1ekg.1.9"};   // (non-L ranges of the planes 1-16, DerivedBidiClass-18.0.0.txt: "delta-from-previous-start.length-1.class-index" in base 36)
  const ASTRAL_STANDIN=['a','\u05D0','\u0627','0','+','$','\u0660',',','\u0300','\u00AD','\u2029','\u001F',' ','!'];   // (L R AL EN ES ET AN CS NSM BN B S WS ON)
  let astralRanges=null;
  const astralClass=cp=>{
    if(!astralRanges){let at=0x10000;astralRanges=ASTRAL.rows.split(',').map(r=>{const p=r.split('.').map(x=>parseInt(x,36));at+=p[0];return [at,at+p[1],p[2]];});}
    let lo=0,hi=astralRanges.length-1;
    while(lo<=hi){const mid=(lo+hi)>>1,r=astralRanges[mid];if(cp<r[0])hi=mid-1;else if(cp>r[1])lo=mid+1;else return r[2];}
    return 0;
  };
  const SURROGATE_PAIR=/[\uD800-\uDBFF][\uDC00-\uDFFF]/;
  const astralStandIns=text=>text.replace(/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,pair=>{const c=ASTRAL_STANDIN[astralClass(pair.codePointAt(0))]||'a';return c+c;});
  // Text that can neither be right-to-left nor carry an explicit direction control is one left-to-right run (levels all 0).
  const MAYBE_BIDI=/[\u0590-\u08FF\u200E\u200F\u202A-\u202E\u2066-\u2069\uFB1D-\uFDFF\uFE70-\uFEFF\uD802\uD803\uD83A\uD83B]/;
  const bidi=(text,opts)=>{
    text=String(text==null?'':text);
    const dir=opts&&(opts.dir==='ltr'||opts.dir==='rtl')?opts.dir:undefined;
    if(!text)return {baseRtl:dir==='rtl',runs:[],visual:[]};
    if(!dir&&!MAYBE_BIDI.test(text))return {baseRtl:false,runs:[{start:0,end:text.length,text,level:0,rtl:false}],visual:[0]};
    const B=getBidi(), seen=SURROGATE_PAIR.test(text)?astralStandIns(text):text, res=B.getEmbeddingLevels(seen,dir), levels=res.levels, paragraphs=res.paragraphs;
    const mirror=B.getMirroredCharactersMap(seen,levels,0,text.length-1);
    const runs=[], paraOf=[];
    let start=0,pi=0;
    const flush=end=>{
      let t=text.slice(start,end);
      const level=levels[start];
      if(level&1){let out='',from=start;for(let i=start;i<end;i++){const m=mirror.get(i);if(m&&m.length===1){out+=text.slice(from,i)+m;from=i+1;}}t=out+text.slice(from,end);}
      runs.push({start,end,text:t,level,rtl:!!(level&1)});
      paraOf.push(pi);
    };
    for(let i=1;i<=text.length;i++){
      const pStart=pi+1<paragraphs.length&&i===paragraphs[pi+1].start;
      if(i===text.length||levels[i]!==levels[start]||pStart){flush(i);start=i;if(pStart)pi++;}
    }
    // L2, one paragraph at a time: from the highest level down to the lowest odd level, reverse every contiguous sequence at that level or higher.
    const visual=[];
    for(let p=0,at=0;p<paragraphs.length;p++){
      const idx=[];while(at<runs.length&&paraOf[at]===p)idx.push(at++);
      if(!idx.length)continue;
      let max=0,minOdd=Infinity;
      for(const i of idx){max=Math.max(max,runs[i].level);if(runs[i].level&1)minOdd=Math.min(minOdd,runs[i].level);}
      for(let lvl=max;lvl>=minOdd&&lvl>0;lvl--){
        for(let a=0;a<idx.length;){
          if(runs[idx[a]].level<lvl){a++;continue;}
          let b=a;while(b+1<idx.length&&runs[idx[b+1]].level>=lvl)b++;
          for(let i=a,j=b;i<j;i++,j--){const t=idx[i];idx[i]=idx[j];idx[j]=t;}
          a=b+1;
        }
      }
      visual.push(...idx);
    }
    return {baseRtl:!!(paragraphs[0].level&1),runs,visual};
  };

  const api={ready:false,init,addFont,removeFont,hasFont,covers,scriptOf,scriptsOf,scriptOfCodePoint,bidi,shapeRun,glyphExtents,version:'1.0 (HarfBuzz 14.6.0, bidi-js 1.1.0)'};
  return api;
});
