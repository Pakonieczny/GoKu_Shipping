/* Source Sans text + shaped monochrome emoji; every displayed glyph is exported as an outline. */
(function(root,factory){if(typeof module==='object'&&module.exports)module.exports=factory();else root.CharmNestText=factory();})(typeof self!=='undefined'?self:this,function(){
  'use strict';
  const graphemes = text => [...new Intl.Segmenter(undefined,{granularity:'grapheme'}).segment(text)].map(x=>x.segment);
  function withEmoji(base, emoji, data, Path) {
    const map=data.sequences, cap=(base.tables.os2.sCapHeight || base.ascender*.7)/base.unitsPerEm;
    const lookup = text => map[text] || map[text.replace(/\uFE0E/g,'\uFE0F')];
    // Words are set composed ("e" + U+0301 is "é"): a loose accent is drawn after its letter, not over it (no mark positioning here).
    const nfc = text => { text=String(text == null ? '' : text); try { return text.normalize('NFC'); } catch (_) { return text; } };
    const tokenize = text => {
      const runs=[]; let plain=''; const flush=()=>{if(plain){runs.push({text:plain});plain='';}};
      for(const cluster of graphemes(nfc(text))) {
        const shape=lookup(cluster);
        // Emoji sequences take precedence even when the text font supports their first codepoint.
        if(shape && (/\p{Extended_Pictographic}|\p{Regional_Indicator}|\u20E3/u.test(cluster))) {flush();runs.push({text:cluster,shape});}
        else plain+=cluster;
      } flush();return runs;
    };
    // A tab has no glyph (the font's .notdef box would be cut in its place) and an accent that does not compose with its letter cannot sit on it: a person decides.
    const coverage = text => {const missing=[];for(const cluster of graphemes(nfc(text))){
      if (/^[\r\n]+$/.test(cluster) || lookup(cluster)) continue;
      if (/[\t\u0300-\u036f]/.test(cluster) || [...cluster].some(ch=>!base.charToGlyphIndex(ch))) missing.push(cluster);
    }return {ok:!missing.length,missing:[...new Set(missing)]};};
    const measure = (run,size,x,y,draw) => {
      if(!run.shape) {if(draw)draw.extend(base.getPath(run.text,x,y,size,{kerning:true}).commands);return base.getAdvanceWidth(run.text,size,{kerning:true});}
      // Scale all emoji by the same font metrics: flags/skin tones/ZWJ sequences retain their intended shape.
      const scale=size*cap/(emoji.ascender-emoji.descender), fontSize=scale*emoji.unitsPerEm;
      let advance=0;
      for(const [id,width,dx,dy] of run.shape){if(draw)draw.extend(emoji.glyphs.get(id).getPath(x+advance+dx*scale,y+emoji.descender*scale-dy*scale,fontSize).commands);advance+=width*scale;}
      return advance+size*.06;
    };
    return {names:base.names,tables:base.tables,ascender:base.ascender,unitsPerEm:base.unitsPerEm,coverage,
      charToGlyphIndex:ch=>base.charToGlyphIndex(ch)||(lookup(ch)?1:0),
      getAdvanceWidth:(text,size)=>tokenize(text).reduce((x,r)=>x+measure(r,size,0,0),0),
      getPath:(text,x,y,size)=>{const check=coverage(text);if(!check.ok)throw new Error('Unsupported engraving characters: '+check.missing.map(c=>c+' ('+[...c].map(x=>'U+'+x.codePointAt(0).toString(16).toUpperCase()).join(' ')+')').join(', '));const path=new Path();for(const run of tokenize(text))x+=measure(run,size,x,y,path);return path;}
    };
  }
  return {withEmoji,graphemes};
});
