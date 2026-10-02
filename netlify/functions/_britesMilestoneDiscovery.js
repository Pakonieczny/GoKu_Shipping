'use strict';

// These are bounded retrieval hypotheses, never universal meanings. A suggested
// connection is usable only after the exact product's approved public meanings
// and current catalogue/variant checks have both survived the existing guards.
const CONTEXTS=Object.freeze({
  remembrance:{pattern:'memorial|remembrance|in memory of|mourning|death celebration|death of (?:a |my |our |her |his |their )?(?:loved one|mother|father|mom|dad|friend|sister|brother|grandmother|grandfather|partner|wife|husband|dog|cat|pet)|remember(?:ing)? (?:my |our |her |his |their |a )?(?:mother|father|mom|dad|friend|sister|brother|grandmother|grandfather|partner|wife|husband|dog|cat|pet)|bereave(?:ment|d)?|grief|grieving|passed away|(?:mother|father|mom|dad|friend|sister|brother|grandmother|grandfather|partner|wife|husband|dog|cat|pet) (?:has |had )?(?:died|passed)|(?:lost|loss of) (?:my |our |her |his |their |a )?(?:mother|father|mom|dad|friend|sister|brother|grandmother|grandfather|partner|wife|husband|dog|cat|pet)',motifs:['cardinal','heart','tree'],evidence:/\b(?:remembrance|remember(?:ing)?|memor(?:y|ies|ial)|mourning|bereavement|grief|loved ones?|continuing bonds?|loss)\b/i,label:'remembrance',intro:'I can help you find a gentle way to mark that memory.',question:'Would a symbol connected with a happy memory, a favourite animal or something they loved feel right?'},
  graduation:{pattern:'graduat(?:ion|e[ds]?|ing)|finished (?:school|university|college)|complet(?:ed|ing) (?:a |my |the |her |his |their |our )?(?:degree|studies)',motifs:['star','butterfly','owl'],evidence:/\b(?:achievement|accomplishment|wisdom|learning|knowledge|new beginnings?|transformation|growth|ambition)\b/i,label:'graduation',intro:'That is a milestone worth marking in a way that feels personal.',question:'Would you like a symbol for the achievement, a fresh beginning or something they love?'},
  wedding:{pattern:'wedding|getting married|just married|engagement|engaged|anniversary',motifs:['heart','circle','tree'],evidence:/\b(?:love|devotion|commitment|partnership|union|connection|togetherness|family|continuity)\b/i,label:'love and commitment',intro:'We can look for a piece that feels connected to your relationship.',question:'Would a personal symbol for your relationship or something you both love feel right?'},
  birth:{pattern:'new baby|newborn|birth of|birth celebration|baby (?:was |is |has )?(?:born|arrived)|became (?:a |an )?(?:mother|father|parent)|new (?:mother|father|parent)|motherhood|fatherhood',motifs:['tree','heart','bunny'],evidence:/\b(?:family|parenthood|motherhood|fatherhood|new beginnings?|renewal|growth|connection|love)\b/i,label:'a new chapter in the family',intro:'We can choose a keepsake to mark that new chapter in the family.',question:'Would a symbol for family, a new beginning or a personal memory feel right?'},
  achievement:{pattern:'new job|promot(?:ion|ed)|achievement|accomplishment|goal (?:achieved|reached)|achieved (?:a |my |the |her |his |their |our )?(?:major |personal |important |long-standing )?goal|milestone|retirement|retir(?:ed|ing)',motifs:['star','mountain','sunflower'],evidence:/\b(?:achievement|accomplishment|ambition|growth|resilience|strength|perseverance|courage|optimism|new beginnings?|hope)\b/i,label:'that achievement or new chapter',intro:'We can find a personal way to mark that achievement or new chapter.',question:'Would a symbol for the achievement, a fresh beginning or a personal interest feel right?'},
  recovery:{pattern:'recover(?:ed|ing|y)|courage after|after (?:an? |my )?(?:illness|treatment)|surviv(?:ed|or)',motifs:['phoenix','lotus','butterfly'],evidence:/\b(?:renewal|resilience|strength|courage|transformation|new beginnings?|hope|perseverance)\b/i,label:'a personal new chapter',intro:'We can look for a personal reminder of what this chapter means to you. Jewellery cannot promise healing or protection.',question:'Would strength, a fresh beginning or a personal interest feel most meaningful?'},
  season:{pattern:'spring|summer|autumn|winter|season(?:al|s)?|fall renewal',motifs:['flower','leaf','butterfly'],evidence:/\b(?:renewal|new beginnings?|nature|seasons?|growth|change|transformation|spring|summer|autumn|winter)\b/i,label:'the season or a fresh beginning',intro:'We can look for a personal connection to the season or a fresh beginning.',question:'Would a favourite part of the season or a personal symbol of a fresh beginning feel right?'},
  christmas:{pattern:'christmas|holiday gift|festive',motifs:['star','snowflake','tree'],evidence:/\b(?:family|connection|togetherness|hope|winter|season|christmas|celebration)\b/i,label:'a personal holiday connection',intro:'We can choose something that feels personal for the holidays.',question:'Would a favourite animal, interest or a personal holiday symbol feel right?'}
});
// Public predictive search remains the first discovery signal. These caps bound
// the private approved-dossier scan, mirror reads and exact live rechecks used
// only to repair an implicit milestone recall miss. They are not configurable
// by shopper input.
const RECALL_BOUNDS=Object.freeze({researchScan:60,supplementReads:60,mirrorReads:36,liveHandles:12});
const validMilestone=value=>typeof value==='string'&&Object.prototype.hasOwnProperty.call(CONTEXTS,value)?value:null;
function parseMilestone(message,previous,negatedAt){
  const text=String(message||'').slice(0,2000).toLowerCase().replace(/[’‘]/g,"'");
  const reset=/\b(?:start (?:fresh|over|again)|reset (?:everything|preferences)|forget (?:everything|the previous|all that)|new gift|different gift|different person|skip (?:the )?(?:occasion|gift details?)|(?:rather|prefer) not (?:to )?(?:say|share|give))\b/.test(text);
  let value=reset?null:validMilestone(previous);
  const hits=Object.entries(CONTEXTS).flatMap(([name,c])=>[...text.matchAll(new RegExp('\\b(?:'+c.pattern+')\\b','g'))].map(hit=>({name,index:hit.index,negative:negatedAt(text,hit.index)}))).sort((a,b)=>a.index-b.index);
  for(const hit of hits){if(hit.negative){if(value===hit.name)value=null;}else value=hit.name;}
  // Explicit bereavement in a holiday/season gift stays gentle. A replacement
  // introduced by “instead/actually” still follows the final affirmative hit.
  const remembrance=hits.find(x=>x.name==='remembrance'&&!x.negative);
  if(remembrance&&hits.some(x=>!x.negative&&['christmas','season'].includes(x.name))&&!/\b(?:instead|actually|not a memorial)\b/.test(text))value='remembrance';
  return value;
}
function contextResidual(query,milestone){
  const ignored=new Set(('gentle reminder reminders finally major important long-standing mourning bereaved commemorate welcoming welcome symbol symbols keepsake keepsakes courage strength meaningful personal memory remember remembering remembrance memorial memories mother father mom dad friend grandmother grandfather sister brother partner wife husband passed away died death loss lost grieving grief bereavement after recovering recovered recovery illness treatment survivor survive new newborn baby birth born arrived parent parents parenthood motherhood fatherhood became graduation graduate graduated graduating finished school university college completed completing degree studies goal goals achieved reached achievement accomplishment milestone promotion promoted job career retirement retired retiring spring summer autumn winter fall renewal season seasons seasonal christmas holiday holidays festive celebrate celebrating celebration mark honor honour keep close bring daughter son wedding married getting engagement engaged anniversary relationship hope fresh beginning beginnings chapter special proud today yesterday recently months years she her he him they their them us our birthday gift gifts something symbol celebrate birth seasonal january february march april may june july august september october november december'.split(' ')));
  let text=String(query||'').toLowerCase();
  // Remove complete life-context phrases, not arbitrary unfamiliar motifs.
  // “journey together” is relationship framing; “journey origami” still leaves
  // an explicit searchable interest. Do this before the four-token query cap.
  if(milestone==='graduation')text=text.replace(/\bjust (?=graduat(?:ed|ing|ion|e)\b)/g,'').replace(/\bmarks? (?:a |the |new )?(?:beginning|chapter|achievement|milestone)\b/g,'');
  if(milestone==='birth')text=text.replace(/\b(?:had|has|have) (?:a |the |new )?(?:baby|newborn)\b/g,'').replace(/\bbecoming(?: (?:a |an )?(?:mother|father|parent))?\b/g,'');
  if(milestone==='wedding')text=text.replace(/\b(?:our )?journey together\b/g,'');
  const words=text.match(/[a-z][a-z-]{2,}/g)||[];
  return words.filter(word=>!ignored.has(word)).join(' ');
}
function contextOnlyQuery(query,milestone){return !contextResidual(query,milestone);}
function discoveryIntent(intent){
  const milestone=validMilestone(intent?.milestone);
  if(!milestone||intent.interests?.length||!contextOnlyQuery(intent.query,milestone)||intent.personalization)return null;
  const spec=CONTEXTS[milestone],excluded=new Set(intent.excludedInterests||[]);
  const motifs=spec.motifs.filter(x=>!excluded.has(x)).slice(0,3);
  return {milestone,motifs,label:spec.label,intro:spec.intro,question:spec.question};
}
function matchingMeanings(meanings,milestone){
  const spec=CONTEXTS[validMilestone(milestone)];if(!spec)return [];
  return (Array.isArray(meanings)?meanings:[]).filter(m=>{
    if(m?.kind!=='interpretation'||!Array.isArray(m.sources)||!m.sources.length)return false;
    const text=String(m.text||'');
    if(/\b(?:guarantee[ds]?|heals?|cures?|treats?|prevents?|protects?|afterlife|heaven)\b/i.test(text))return false;
    const positive=new RegExp(spec.evidence.source,'ig');
    return [...text.matchAll(positive)].some(hit=>!/(?:\b(?:not|no|never|without|isn't|doesn't|is not|does not)\s+(?:[a-z]+\s+){0,4})$/i.test(text.slice(0,hit.index).split(/[,;.!?]|\bbut\b/i).at(-1)));
  });
}
function presentation(milestone){const spec=CONTEXTS[validMilestone(milestone)];return spec?{label:spec.label,intro:spec.intro,question:spec.question}:null;}
function recallBounds(){return {...RECALL_BOUNDS};}
module.exports={validMilestone,parseMilestone,contextOnlyQuery,contextResidual,discoveryIntent,matchingMeanings,presentation,recallBounds};
