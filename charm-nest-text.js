/* Source Sans text + shaped monochrome emoji; every displayed glyph is exported as an outline. */
(function(root,factory){if(typeof module==='object'&&module.exports)module.exports=factory();else root.CharmNestText=factory();})(typeof self!=='undefined'?self:this,function(){
  'use strict';
  const graphemes = text => [...new Intl.Segmenter(undefined,{granularity:'grapheme'}).segment(text)].map(x=>x.segment);
  const PICTO=/\p{Extended_Pictographic}|\p{Regional_Indicator}|⃣/u;
  // Nothing to draw and nothing to cut: variation selectors, zero-width joiners and spaces, the soft hyphen, bidi marks. A phone keyboard appends one to a ♡ or an ❤; they were refused as "no glyph".
  const IGNORABLE=/\p{Default_Ignorable_Code_Point}/gu;
  // What a fallback font may draw: scripts set one glyph after another, left to right (letters, digits, punctuation, symbols). Hebrew, Arabic, Indic, Thai ... need shaping or run right to left and would be cut back to front or unjoined: a person decides (unless their script font is loaded, see below: then the shaper sets them).
  const PLAIN_SCRIPTS=/^[\p{Script=Latin}\p{Script=Greek}\p{Script=Cyrillic}\p{Script=Common}]+$/u, SHAPED=/[֐-ࣿיִ-﷿ﹰ-﻿]/;
  /* ═══ world scripts (WORLD, Paul 19 Oct 2026: "add the compatibility with the Arabic, Hebrew, Chinese, Japanese, Korean, Indian letters") ═══
     Source Sans 3 and the shop's faces draw Latin, Greek and Cyrillic. A line in Arabic, Hebrew, an Indian script, Chinese, Japanese or Korean is set by a SCRIPT FONT (Noto Sans <script>, vendor/fonts/script-fonts.json)
     through CharmNestShape (HarfBuzz: joining, conjuncts, mark positioning; UAX #9: right to left, numbers inside it). The script set below is the list of those fonts the page has loaded; withEmoji takes it as its sixth argument (the page's and the worker's one set, CharmNestText.sharedScripts, when none is given; null: none).
     With no script set, or none of its fonts loaded, nothing here runs and every line is set (or refused) exactly as before. */
  let wrappers = 0;
  const REF = 1000;                                                    // (a script run is shaped once at 1000 px and scaled to the size asked: shaping does not depend on the size)
  const reRe = new Map(), rx = (prop, code) => { const k = prop + '=' + code; let r = reRe.get(k); if (r === undefined) { try { r = new RegExp('\\p{' + k + '}', 'u'); } catch (_) { r = null; } reRe.set(k, r); } return r; };
  // right-to-left letters, numbers and formatting characters: only a line with one needs a bidi pass (all levels are 0 without)
  const RTL = /[\u0590-\u08FF\u200E\u200F\u061C\u202A-\u202E\u2066-\u2069\uFB1D-\uFDFF\uFE70-\uFEFF]|[\u{10800}-\u{10FFF}\u{1E800}-\u{1EFFF}]/u;
  // Arabic presentation forms (pasted from a PDF) are the letters they stand for; the font joins them itself
  const ARABIC_FORMS = /[\uFB50-\uFDFF\uFE70-\uFEFE]+/g;
  // every character a script font could be asked for (any letter that is not Latin, Greek or Cyrillic; Thai, Khmer ... too, a new manifest entry needs no code): a cheap test so that a Latin line never reaches the manifest or the shaper
  const SCRIPTY = /[^\p{Script=Latin}\p{Script=Greek}\p{Script=Cyrillic}\p{Script=Common}\p{Script=Inherited}]/u;
  const langOf = e => { const l = String((e.langs && e.langs[0]) || '').toLowerCase().split('-')[0]; if (l) return l; const m = /(?:^|[-_])(jp|ja|kr|ko|sc|tc)$/i.exec(e.id || ''); return m ? ({jp:'ja',kr:'ko',sc:'zh',tc:'zh'})[m[1].toLowerCase()] : ''; };
  const hasLang = (e, l) => (e.langs || []).some(x => String(x).toLowerCase().split('-')[0] === l) || langOf(e) === l;
  // Japanese kana mean a Japanese line, Hangul a Korean one: that line's ideographs come from the same font. A line of ideographs alone starts with the first font of the chain (Simplified), then the next that has them.
  const prefer = (cands, code, ctx) => { const want = code === 'Hira' || code === 'Kana' ? 'ja' : code === 'Hang' ? 'ko' : code === 'Hani' ? (ctx.ja ? 'ja' : ctx.ko ? 'ko' : '') : ''; return want ? cands.filter(e => hasLang(e, want)).concat(cands.filter(e => !hasLang(e, want))) : cands; };
  const inRanges = (e, cp) => (e.ranges || []).some(r => cp >= r[0] && cp <= r[1]);
  const codeOfChar = (ch, codes) => { for (const c of codes) { const r = rx('sc', c); if (r && r.test(ch)) return c; } return null; };
  const lineContext = text => ({ ja: /[\p{sc=Hira}\p{sc=Kana}]/u.test(text), ko: /\p{sc=Hang}/u.test(text) });
  const orderedByManifest = (a, b) => (a.order || 0) - (b.order || 0);
  // strong: a character that belongs to one of these scripts by itself (a letter, a vowel sign, a digit); not the neutrals (punctuation) the scripts share
  const deriveOf = list => { const sorted = list.slice().sort(orderedByManifest), codes = [...new Set(sorted.flatMap(e => e.scripts || []))].filter(c => rx('sc', c)); return { sorted, codes, strong: codes.length ? new RegExp('[' + codes.map(c => '\\p{sc=' + c + '}').join('') + ']', 'u') : /(?!)/u }; };
  /** The script fonts of a page or a worker. declare(): every font the manifest lists (what a line may ask to load); use(): a font the shaper holds, with its measured capital height; the engine is CharmNestShape once init() resolved. */
  function scriptSet() {
    const declared = [], loaded = new Map(), views = new Map(); let memo = null;
    const set = {
      shape: null, version: 0, cache: new Map(), declared, loaded,
      attach(shape) { set.shape = shape; set.version++; memo = null; return set; },
      declare(list) { declared.length = 0; (list || []).forEach((e, i) => declared.push(Object.assign({}, e, { order: i }))); set.version++; memo = null; return set; },
      use(entry) { const d = declared.find(e => e.id === entry.id); loaded.set(entry.id, Object.assign({ order: d ? d.order : loaded.size }, d || {}, entry)); set.version++; memo = null; return set; },
      drop(id) { loaded.delete(id); set.version++; memo = null; },
      isLoaded: id => loaded.has(id),
      sorted() { return memo ? memo.sorted : (set.derive().sorted); },
      derive() { return memo = deriveOf([...loaded.values()]); },
      codes() { return (memo || set.derive()).codes; },
      strongRe() { return (memo || set.derive()).strong; },
      entriesFor(code) { return set.sorted().filter(e => (e.scripts || []).includes(code)); },
      has(text) { return !!(set.shape && set.shape.ready && loaded.size && set.strongRe().test(text)); },
      /** The same set limited to some of its fonts (a job pinned to the fonts its words were set in: "日本" on a line of its own stays Japanese when the page also holds the Chinese font). Follows the set as it changes. */
      view(ids) {
        const want = new Set(ids), key = [...want].sort().join('|'); let v = views.get(key); if (v) return v;
        let m = null; const derive = () => (m && m.v === set.version ? m : (m = Object.assign(deriveOf([...loaded.values()].filter(e => want.has(e.id))), { v: set.version })));
        v = { cache: set.cache, get shape() { return set.shape; }, get version() { return set.version + '|' + key; }, sorted: () => derive().sorted, codes: () => derive().codes, strongRe: () => derive().strong,
          entriesFor: code => derive().sorted.filter(e => (e.scripts || []).includes(code)), has: text => !!(set.shape && set.shape.ready && derive().sorted.length && derive().strong.test(text)), view: set.view };
        views.set(key, v); return v;
      },
      /** The manifest fonts a text still needs loaded (not loaded, not in `skip`): one per script cluster no loaded font covers, in the order of the chain; and, only when the text has such a cluster, the font whose ranges hold a character
       *  (`missing`: the clusters the engraving font still refuses) such as a fullwidth comma, a danda or a corner bracket. */
      needs(text, missing, skip) {
        const out = [], t = String(text == null ? '' : text).normalize('NFC').replace(ARABIC_FORMS, m => m.normalize('NFKC'));
        const codes = [...new Set(declared.flatMap(e => e.scripts || []))].filter(c => rx('sc', c));
        if (!codes.length) return out;
        const strong = new RegExp('[' + codes.map(c => '\\p{sc=' + c + '}').join('') + ']', 'u'), ctx = lineContext(t);
        const ok = e => !loaded.has(e.id) && !(skip && skip.has(e.id)), add = e => { if (e && !out.includes(e)) out.push(e); };
        let any = false;
        for (const c of graphemes(t)) {
          const first = String.fromCodePoint(c.codePointAt(0)); if (!strong.test(first)) continue;
          any = true; const code = codeOfChar(first, codes);
          const cands = prefer(declared.filter(e => (e.scripts || []).includes(code)), code, ctx);
          // the chain in order: a loaded font that has the cluster ends it, a loaded one that lacks it is passed, the first one not loaded yet is the next to load (a later font that happens to be loaded for another job does not settle it)
          for (const e of cands) { if (loaded.has(e.id)) { if (set.shape && set.shape.ready && set.shape.covers(e.id, c)) break; continue; } if (skip && skip.has(e.id)) continue; add(e); break; }
        }
        if (any) for (const c of missing || []) {
          if (strong.test(c)) continue;
          const cps = [...c].map(x => x.codePointAt(0)); if (!cps.some(cp => cp > 0x2000)) continue;
          if (declared.some(e => loaded.has(e.id) && set.shape && set.shape.ready && set.shape.covers(e.id, c))) continue;
          add(declared.find(e => ok(e) && cps.some(cp => inRanges(e, cp))));
        }
        return out;
      }
    };
    return set;
  }
  /** Visual order of items that carry a bidi level (UAX #9 rule L2): reverse every run of items at a level or higher, from the highest level down to the lowest odd one. */
  function reorder(items) {
    let max = 0, odd = 99; for (const it of items) { if (it.level > max) max = it.level; if ((it.level & 1) && it.level < odd) odd = it.level; }
    for (let l = max; l >= odd; l--) for (let i = 0; i < items.length;) {
      if (items[i].level < l) { i++; continue; }
      let j = i; while (j < items.length && items[j].level >= l) j++;
      const run = items.slice(i, j).reverse(); for (let k = i; k < j; k++) items[k] = run[k - i]; i = j;
    }
    return items;
  }
  /* `fallbacks`: opentype fonts (not wrapped ones) tried, in this order, for a character the engraving font itself lacks: Source Sans 3 for a script face, then a symbol font (♡ ☆ ✿ ✓ ∞ ♪ ...).
     Whatever the font or the emoji font already sets is set exactly as before; the fallbacks only look at what used to be refused. */
  function withEmoji(base, emoji, data, Path, fallbacks, scripts) {
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
    const plan = (cluster,depth,claim) => {
      const shape=lookup(cluster);
      // Emoji sequences take precedence even when the text font supports their first codepoint.
      if(shape && PICTO.test(cluster)) return [{shape}];
      if(shape || /^[\r\n]+$/.test(cluster)) return [own(cluster)];
      // A tab has no glyph (the font's .notdef box would be cut in its place) and an accent that does not compose with its letter cannot sit on it: a person decides.
      if(/[\t̀-ͯ]/.test(cluster)) return null;
      if(covers(base,cluster)) return [own(cluster)];
      const core=cluster.replace(IGNORABLE,'');
      if(core!==cluster){const parts=graphemes(core).map(c=>plan(c,depth,claim));return parts.every(Boolean)?parts.flat():null;}
      if(/^\p{Zs}+$/u.test(cluster)) return [own(' '.repeat([...cluster].length))];   // (a no-break, thin, em or ideographic space is a space)
      if(!SHAPED.test(cluster) && PLAIN_SCRIPTS.test(cluster)) for(const f of fonts) if(covers(f.font,cluster)) return [{text:cluster,font:f.font,k:f.k}];
      // (WORLD, Paul 19 Oct 2026: a character next to a script run that no other font has, a fullwidth comma, a corner bracket, a danda, is the script font's own glyph; only asked for inside a script line)
      if(claim){const seg=claim(cluster);if(seg)return [seg];}
      // Last: the plain form of a styled character (fullwidth ＡＢＣ, math-styled 𝓗𝓮𝓵𝓵𝓸, circled Ⓐ, ㎏, ℌ ...), only when no font has the character itself.
      if(!depth){const plain=cluster.normalize('NFKC');if(plain!==cluster){const parts=graphemes(plain).map(c=>plan(c,1));if(parts.every(Boolean))return parts.flat();}}
      return null;
    };
    /* ── (WORLD, Paul 19 Oct 2026) a line with letters of a loaded script: shaped runs of the script font, the rest as above, all in the visual order of UAX #9 ──
       scripts.has(text) is false for every line without such a letter (and for every page that has no script font yet), so a Latin line takes exactly the code below this block. */
    const build = scripts => {
    const wid = ++wrappers;   // (the script set's cache is shared by every face: a line's runs hold THIS face's own font, so they are cached per wrapper)
    const live = text => !!(scripts && scripts.has(text));
    const prep = text => text.replace(ARABIC_FORMS, m => m.normalize('NFKC'));
    // (a script font is drawn at the size that gives ITS capital H the height of this face's: Noto Sans next to Source Sans 3 keeps the proportions Noto designed between its scripts and its Latin)
    const kOf = e => {const c=e.capEm,k=c>0?cap/c:1;return isFinite(k)&&k>.6&&k<1.5?k:1;};
    const isScript = it => it.parts.length===1 && !!it.parts[0].script;
    /* Every cluster of the line, in logical order: a letter of a script font -> that font (Hebrew, Arabic, Indic; Japanese kana -> the Japanese font, Hangul -> the Korean, ideographs -> the line's own kind else the first font of the chain that has them),
       anything else as plan() sets it, with `claim` for a neutral next to a script run that only the script font has. A vowel sign or niqqud with no letter under it, and everything plan() refuses, is `missing`. */
    const classify = text => {
      const shape=scripts.shape,codes=scripts.codes(),strong=scripts.strongRe(),ctx=lineContext(text),cl=[],missing=[]; let at=0;
      for(const g of graphemes(text)){cl.push({text:g,at});at+=g.length;}
      for(const it of cl){const first=String.fromCodePoint(it.text.codePointAt(0));if(strong.test(first) && !/\p{M}/u.test(first))it.code=codeOfChar(first,codes);else if(strong.test(it.text))it.bad=true;}
      const han=cl.filter(it=>it.code==='Hani'),hanFirst=han.length && !ctx.ja && !ctx.ko ? scripts.entriesFor('Hani').find(e=>han.every(it=>shape.covers(e.id,it.text))) : null;
      for(const it of cl) if(it.code) {
        let cands=prefer(scripts.entriesFor(it.code),it.code,ctx);
        if(hanFirst && it.code==='Hani')cands=[hanFirst].concat(cands.filter(e=>e!==hanFirst));
        it.entry=cands.find(e=>shape.covers(e.id,it.text)) || null; if(!it.entry)it.bad=true;
      }
      cl.forEach((it,n)=>{
        if(it.code || it.bad)return;
        const claim=c=>{if(/\p{M}/u.test(c))return null;const pool=[cl[n-1] && cl[n-1].entry,cl[n+1] && cl[n+1].code && cl[n+1].entry].filter(Boolean),e=pool.find(e=>shape.covers(e.id,c));return e?{text:c,script:e}:null;};
        const parts=plan(it.text,0,claim);
        if(!parts)it.bad=true; else {it.parts=parts;if(parts.length===1 && parts[0].script)it.entry=parts[0].script;}
      });
      const list=[];
      for(const it of cl) {
        if(it.bad){missing.push(it.text);list.push({at:it.at,parts:[own(it.text)]});}
        else if(it.code)list.push({at:it.at,parts:[{script:it.entry,text:it.text,code:it.code}]});
        else if(it.parts.length)list.push({at:it.at,parts:it.parts.map(q=>q.script?{script:q.script,text:q.text,code:null}:q),space:it.parts.every(q=>!q.script && q.font===base && /^\p{Zs}+$/u.test(q.text))});
      }
      return {list,missing};
    };
    // The line as drawn from left to right: the bidi levels of the whole line, spaces inside a script phrase joined to it, runs of one font joined, then rule L2.
    const visual = text => {
      const key=wid+'\u0001'+scripts.version+'\u0001'+text,hit=scripts.cache.get(key); if(hit)return hit;
      let disp=text,levels=null;
      if(RTL.test(text)){const bd=scripts.shape.bidi(text),joined=bd.runs.map(r=>r.text).join('');if(joined.length===text.length)disp=joined;levels=new Uint8Array(text.length);for(const r of bd.runs)levels.fill(r.level,r.start,r.end);}   // (disp: brackets already mirrored where UAX #9 asks)
      const {list,missing}=classify(disp);
      for(const it of list)it.level=levels?levels[it.at]:0;
      for(let i=1;i<list.length-1;i++) {
        if(!list[i].space)continue;
        let j=i;while(j<list.length && list[j].space)j++;
        const a=list[i-1],b=list[j];
        if(b && isScript(a) && isScript(b) && a.parts[0].script===b.parts[0].script && list.slice(i,j).every(q=>q.level===a.level) && a.level===b.level)
          for(let k=i;k<j;k++)list[k]={at:list[k].at,level:a.level,parts:[{script:a.parts[0].script,text:' '.repeat(list[k].parts.reduce((n,q)=>n+q.text.length,0)),code:null}]};
        i=j;
      }
      const merged=[];
      for(const it of list) {
        const last=merged[merged.length-1];
        if(last && isScript(last) && isScript(it) && last.level===it.level && last.parts[0].script===it.parts[0].script){last.parts[0].text+=it.parts[0].text;last.parts[0].code=last.parts[0].code || it.parts[0].code;}
        else merged.push(it);
      }
      const runs=[]; let cur=null;
      for(const it of reorder(merged)) for(const seg of it.parts) {
        if(seg.script){cur=null;runs.push({script:seg.script,text:seg.text,code:seg.code || (seg.script.scripts||[])[0],rtl:(it.level&1)===1});}
        else if(seg.shape){cur=null;runs.push(seg);}
        else if(cur && cur.font===seg.font)cur.text+=seg.text;
        else {cur={text:seg.text,font:seg.font,k:seg.k};runs.push(cur);}
      }
      const out={runs,missing}; if(scripts.cache.size>600)scripts.cache.clear(); scripts.cache.set(key,out); return out;
    };
    // The outline of one script run at 1000 px (HarfBuzz output is the same at every size), scaled to the size asked
    const shaped = run => {
      const e=run.script,key=e.id+(run.rtl?'<':'>')+run.code+'\u0001'+run.text; let sh=scripts.cache.get(key);
      if(!sh){const r=scripts.shape.shapeRun(run.text,{fontId:e.id,size:REF,rtl:run.rtl,script:run.code,lang:(e.langs && e.langs[0]) || undefined});sh={advance:r.advance,commands:r.commands};scripts.cache.set(key,sh);}
      return sh;
    };
    const tokenize = text => {
      const t=nfc(text);
      if(live(t))return visual(prep(t)).runs;
      const runs=[]; let cur=null;
      for(const cluster of graphemes(t)) {
        for(const seg of plan(cluster,0) || [own(cluster)]) {   // (a cluster that cannot be set never reaches the cut file: getPath refuses it first)
          if(seg.shape) {cur=null;runs.push(seg);}
          else if(cur && cur.font===seg.font) cur.text+=seg.text;
          else {cur={text:seg.text,font:seg.font,k:seg.k};runs.push(cur);}
        }
      } return runs;
    };
    const coverage = text => {
      const t=nfc(text);
      if(live(t)){const v=visual(prep(t));return {ok:!v.missing.length,missing:[...new Set(v.missing)]};}
      const missing=[];for(const cluster of graphemes(t)){
      if(!plan(cluster,0)) missing.push(cluster);
    }return {ok:!missing.length,missing:[...new Set(missing)]};};
    // The script fonts a line is set in, in the order they are drawn: [{id,name,scripts}] (the card says "Set in Noto Sans Arabic")
    const scriptFonts = text => {const t=nfc(text);if(!live(t))return [];const seen=[];for(const r of visual(prep(t)).runs)if(r.script && !seen.includes(r.script))seen.push(r.script);return seen.map(e=>({id:e.id,name:e.name,scripts:e.scripts}));};
    const measure = (run,size,x,y,draw) => {
      if(run.script) {const sh=shaped(run),sc=size*kOf(run.script)/REF;if(draw)draw.extend(sh.commands.map(c=>{const o={type:c.type};if(c.type!=='Z'){o.x=x+c.x*sc;o.y=y+c.y*sc;}if(c.type==='C'||c.type==='Q'){o.x1=x+c.x1*sc;o.y1=y+c.y1*sc;}if(c.type==='C'){o.x2=x+c.x2*sc;o.y2=y+c.y2*sc;}return o;}));return sh.advance*sc;}
      if(!run.shape) {const s=size*run.k;if(draw)draw.extend(run.font.getPath(run.text,x,y,s,{kerning:true}).commands);return run.font.getAdvanceWidth(run.text,s,{kerning:true});}
      // Scale all emoji by the same font metrics: flags/skin tones/ZWJ sequences retain their intended shape.
      const scale=size*cap/(emoji.ascender-emoji.descender), fontSize=scale*emoji.unitsPerEm;
      let advance=0;
      for(const [id,width,dx,dy] of run.shape){if(draw)draw.extend(emoji.glyphs.get(id).getPath(x+advance+dx*scale,y+emoji.descender*scale-dy*scale,fontSize).commands);advance+=width*scale;}
      return advance+size*.06;
    };
    return {names:base.names,tables:base.tables,ascender:base.ascender,unitsPerEm:base.unitsPerEm,coverage,scriptFonts,pin:ids=>pinned(ids),
      charToGlyphIndex:ch=>base.charToGlyphIndex(ch)||(lookup(ch)?1:0)||(ch!=='\n'&&ch!=='\r'&&plan(ch,0)?1:0),
      getAdvanceWidth:(text,size)=>tokenize(text).reduce((x,r)=>x+measure(r,size,0,0),0),
      getPath:(text,x,y,size)=>{const check=coverage(text);if(!check.ok)throw new Error('Unsupported engraving characters: '+check.missing.map(c=>c+' ('+[...c].map(x=>'U+'+x.codePointAt(0).toString(16).toUpperCase()).join(' ')+')').join(', '));const path=new Path();for(const run of tokenize(text))x+=measure(run,size,x,y,path);return path;}
    };
    };   // (end of build: one wrapper per script set, the page's or a pinned view of it)
    // (WORLD, Paul 19 Oct 2026) `scripts` undefined: the page's / worker's one script set (sharedScripts); null: none. wrapper.pin(ids): the same face limited to those script fonts, for a job whose words were set in them.
    const root = scripts === undefined ? sharedScripts : scripts, pins = new Map(), main = build(root);
    const pinned = ids => { if (!root || !root.view || !ids || !ids.length) return main; const key = [...ids].sort().join('|'); let w = pins.get(key); if (!w) { w = build(root.view(ids)); pins.set(key, w); } return w; };
    return main;
  }
  const sharedScripts = scriptSet();
  return {withEmoji,graphemes,scripts:scriptSet,sharedScripts,scriptHint:text=>SCRIPTY.test(String(text==null?'':text))};
});
