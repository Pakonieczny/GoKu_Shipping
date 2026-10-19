/* Source Sans text + shaped monochrome emoji; every displayed glyph is exported as an outline. */
(function(root,factory){if(typeof module==='object'&&module.exports)module.exports=factory();else root.CharmNestText=factory();})(typeof self!=='undefined'?self:this,function(){
  'use strict';
  const graphemes = text => [...new Intl.Segmenter(undefined,{granularity:'grapheme'}).segment(text)].map(x=>x.segment);
  const PICTO=/\p{Extended_Pictographic}|\p{Regional_Indicator}|⃣/u;
  // Nothing to draw and nothing to cut: variation selectors, zero-width joiners and spaces, the soft hyphen, bidi marks. A phone keyboard appends one to a ♡ or an ❤; they were refused as "no glyph".
  const IGNORABLE=/\p{Default_Ignorable_Code_Point}/gu;
  // What a fallback font may draw: scripts set one glyph after another, left to right (letters, digits, punctuation, symbols). Hebrew, Arabic, Indic, Thai ... need shaping or run right to left and would be cut back to front or unjoined: a person decides.
  const PLAIN_SCRIPTS=/^[\p{Script=Latin}\p{Script=Greek}\p{Script=Cyrillic}\p{Script=Common}]+$/u, SHAPED=/[֐-ࣿיִ-﷿ﹰ-﻿]/;
  /* `fallbacks`: opentype fonts (not wrapped ones) tried, in this order, for a character the engraving font itself lacks: Source Sans 3 for a script face, then a symbol font (♡ ☆ ✿ ✓ ∞ ♪ ...).
     Whatever the font or the emoji font already sets is set exactly as before; the fallbacks only look at what used to be refused. */
  function withEmoji(base, emoji, data, Path, fallbacks) {
    const map=data.sequences, cap=(base.tables.os2.sCapHeight || base.ascender*.7)/base.unitsPerEm;
    const lookup = text => map[text] || map[text.replace(/︎/g,'️')];
    // Words are set composed ("e" + U+0301 is "é"): a loose accent is drawn after its letter, not over it (no mark positioning here).
    const nfc = text => { text=String(text == null ? '' : text); try { return text.normalize('NFC'); } catch (_) { return text; } };
    // (a fallback is drawn at the size that gives it the same capital height as the engraving font, so a ♡ stands beside the letters)
    // (a font whose OS/2 table has no capital height, DejaVu's has none, is measured on its H)
    const capOf = f => {let c=f.tables && f.tables.os2 && f.tables.os2.sCapHeight;if(!c)try{const g=f.charToGlyph('H');c=g && g.index ? g.getBoundingBox().y2 : 0;}catch(_){c=0;}return (c||f.ascender*.7)/f.unitsPerEm;};
    const fonts=(fallbacks||[]).filter(f=>f && f.charToGlyphIndex && f.getPath && f.getAdvanceWidth).map(font=>{const k=cap/capOf(font);return {font,k:isFinite(k)&&k>.5&&k<2?k:1};});
    const covers=(font,text)=>[...text].every(ch=>font.charToGlyphIndex(ch));
    const own=text=>({text,font:base,k:1});
    // The segments that set one grapheme cluster: [] when there is nothing to draw, null when it cannot be set exactly here (a person decides).
    const plan = (cluster,depth) => {
      const shape=lookup(cluster);
      // Emoji sequences take precedence even when the text font supports their first codepoint.
      if(shape && PICTO.test(cluster)) return [{shape}];
      if(shape || /^[\r\n]+$/.test(cluster)) return [own(cluster)];
      // A tab has no glyph (the font's .notdef box would be cut in its place) and an accent that does not compose with its letter cannot sit on it: a person decides.
      if(/[\t̀-ͯ]/.test(cluster)) return null;
      if(covers(base,cluster)) return [own(cluster)];
      const core=cluster.replace(IGNORABLE,'');
      if(core!==cluster){const parts=graphemes(core).map(c=>plan(c,depth));return parts.every(Boolean)?parts.flat():null;}
      if(/^\p{Zs}+$/u.test(cluster)) return [own(' '.repeat([...cluster].length))];   // (a no-break, thin, em or ideographic space is a space)
      if(!SHAPED.test(cluster) && PLAIN_SCRIPTS.test(cluster)) for(const f of fonts) if(covers(f.font,cluster)) return [{text:cluster,font:f.font,k:f.k}];
      // Last: the plain form of a styled character (fullwidth ＡＢＣ, math-styled 𝓗𝓮𝓵𝓵𝓸, circled Ⓐ, ㎏, ℌ ...), only when no font has the character itself.
      if(!depth){const plain=cluster.normalize('NFKC');if(plain!==cluster){const parts=graphemes(plain).map(c=>plan(c,1));if(parts.every(Boolean))return parts.flat();}}
      return null;
    };
    const tokenize = text => {
      const runs=[]; let cur=null;
      for(const cluster of graphemes(nfc(text))) {
        for(const seg of plan(cluster,0) || [own(cluster)]) {   // (a cluster that cannot be set never reaches the cut file: getPath refuses it first)
          if(seg.shape) {cur=null;runs.push(seg);}
          else if(cur && cur.font===seg.font) cur.text+=seg.text;
          else {cur={text:seg.text,font:seg.font,k:seg.k};runs.push(cur);}
        }
      } return runs;
    };
    const coverage = text => {const missing=[];for(const cluster of graphemes(nfc(text))){
      if(!plan(cluster,0)) missing.push(cluster);
    }return {ok:!missing.length,missing:[...new Set(missing)]};};
    const measure = (run,size,x,y,draw) => {
      if(!run.shape) {const s=size*run.k;if(draw)draw.extend(run.font.getPath(run.text,x,y,s,{kerning:true}).commands);return run.font.getAdvanceWidth(run.text,s,{kerning:true});}
      // Scale all emoji by the same font metrics: flags/skin tones/ZWJ sequences retain their intended shape.
      const scale=size*cap/(emoji.ascender-emoji.descender), fontSize=scale*emoji.unitsPerEm;
      let advance=0;
      for(const [id,width,dx,dy] of run.shape){if(draw)draw.extend(emoji.glyphs.get(id).getPath(x+advance+dx*scale,y+emoji.descender*scale-dy*scale,fontSize).commands);advance+=width*scale;}
      return advance+size*.06;
    };
    return {names:base.names,tables:base.tables,ascender:base.ascender,unitsPerEm:base.unitsPerEm,coverage,
      charToGlyphIndex:ch=>base.charToGlyphIndex(ch)||(lookup(ch)?1:0)||(ch!=='\n'&&ch!=='\r'&&plan(ch,0)?1:0),
      getAdvanceWidth:(text,size)=>tokenize(text).reduce((x,r)=>x+measure(r,size,0,0),0),
      getPath:(text,x,y,size)=>{const check=coverage(text);if(!check.ok)throw new Error('Unsupported engraving characters: '+check.missing.map(c=>c+' ('+[...c].map(x=>'U+'+x.codePointAt(0).toString(16).toUpperCase()).join(' ')+')').join(', '));const path=new Path();for(const run of tokenize(text))x+=measure(run,size,x,y,path);return path;}
    };
  }
  return {withEmoji,graphemes};
});
