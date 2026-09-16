// The film model draws whatever nouns it is shown, prohibition or not. These
// checks hold the two defences that follow from that: the vocabulary of
// lettering and overlays never reaches it, and a film that still comes back
// with lettering is held rather than delivered.
const assert=require('assert');
const motion=require('../../netlify/functions/googleAdsAdMotion.js');
const BANNED=/\b(?:text|lettering|letters?|words?|caption|captions|subtitles?|headlines?|messaging|message|typography|typeface|fonts?|glyphs?|logos?|wordmark|watermark|signage|labels?|tags?|overlay|print(?:ed|ing)?|writing|written|slogan|tagline|inscription)\b/i;
const job=orientation=>({title:'Snake Charm',plan:{copy:{headline:'Made to be worn',shortHeadline:'Snake Charm',description:'Solid gold',cta:'Shop now'}},creativeDirection:{
 setting:'A sunlit stone ledge with an open book beside the piece, ferns behind',
 props:'A folded map and a brass key',
 lighting:'Bright open daylight',
 opening:'The piece already in frame',
 middle:'The camera tracks left',
 ending:'Held product view',
 portrait:'Keep the upper third clear for the headline overlay, piece low in frame',
 landscape:'Leave the left third for the brand messaging',
 identity:'Exact catalog reference'}});
for(const orientation of ['portrait','landscape']){
 const prompt=motion.motionPrompt(job(orientation),orientation,'fallback');
 const hit=BANNED.exec(prompt);
 assert(!hit,orientation+' film prompt must not name lettering or overlays, found: '+(hit&&hit[0]));
 assert(!/\bbook\b|\bmap\b/i.test(prompt),orientation+' film prompt must not carry a prop that bears writing in real life');
 assert(/blank and unmarked/.test(prompt),orientation+' film prompt states positively that surfaces are bare');
}
// A director who describes the overlay loses that clause, not the whole scene.
assert.equal(motion.scrubText('Keep the upper third clear for the headline overlay, water rippling below.'),'water rippling below.');
assert.equal(motion.scrubText('A sunlit desk with an open book beside the piece, warm linen underneath.'),'warm linen underneath.');
// A clean scene survives untouched.
const clean='Morning light across wet stone, the piece catching a bright highlight.';
assert.equal(motion.scrubText(clean),clean,'a scene with no lettering risk is passed through unchanged');
// Whole-sentence removal when the sentence is only about the prop.
assert.equal(motion.scrubText('The piece rests on pale stone. A printed gift tag sits beside it.'),'The piece rests on pale stone.');
console.log('PASS the film model is never shown lettering vocabulary or a prop that carries writing');
