(function(root,factory){
  'use strict';
  var api=factory();if(typeof module==='object'&&module.exports)module.exports=api;else root.BritesCatalogueIntents=api;
})(typeof globalThis!=='undefined'?globalThis:this,function(){
  'use strict';
  // One deterministic, read-only vocabulary for the checked browser inventory
  // and the server search. Names and explicit motif tags establish a design;
  // body cross-sells never turn a necklace into an earring or supply its motif.
  var CATEGORY_PATTERNS={
    necklaces:/\b(?:necklaces?|pendants?)\b/i,earrings:/\b(?:earrings?|studs?|huggies?|hoops?)\b/i,
    bracelets:/\bbracelets?\b/i,rings:/\brings?\b/i,charms:/\bcharms?\b/i,
    'regular-necklaces':/\b(?:regular|classic|standard)(?:\s+[a-z]+){0,4}\s+necklaces?\b/i,
    'beady-necklaces':/\b(?:beady|beaded|bead[- ]chain)\b/i,
    'stud-earrings':/\bstuds?\b/i,'hoop-earrings':/\b(?:hoops?|huggies?)\b/i,
    'charm-only':/\b(?:charms?[ -]only|(?:loose|standalone|individual)\s+charms?|only\s+charms?)\b/i
  };
  var CATEGORY_LABELS={necklaces:'necklaces',earrings:'earrings',bracelets:'bracelets',rings:'rings',charms:'charms',
    'regular-necklaces':'regular necklaces','beady-necklaces':'beady necklaces','stud-earrings':'stud earrings','hoop-earrings':'hoop earrings','charm-only':'charm only'};
  var THEME_WORDS={animals:'animal animals wildlife fauna creature creatures',birds:'bird birds avian',pets:'pet pets',insects:'insect insects bug bugs',ocean:'ocean marine sea sealife',flowers:'flower flowers floral blossom blossoms botanical',nature:'nature woodland forest',celestial:'celestial astronomy space cosmic'};
  var THEME_MOTIFS={
    animals:'bunny cat dog horse wolf fox elephant dolphin whale owl turtle penguin sheep deer frog fish bird butterfly bee dragonfly hummingbird cardinal lion tiger rhino rhinoceros giraffe bear panda koala sloth monkey mouse squirrel hedgehog hamster gerbil goat pig cow moose elk otter raccoon bat snake lizard crocodile alligator seahorse octopus crab lobster shark ray swan duck goose eagle peacock parrot rooster chicken dove flamingo toucan penguin snail ladybug ant beetle moth unicorn dragon phoenix paw',
    birds:'bird hummingbird cardinal owl penguin swan duck goose eagle peacock parrot rooster chicken dove flamingo toucan phoenix',
    pets:'bunny cat dog horse hamster gerbil mouse paw',insects:'butterfly bee dragonfly ladybug ant beetle moth',
    ocean:'fish dolphin whale turtle seahorse octopus crab lobster shark ray coral seashell shell starfish',
    flowers:'flower sunflower daisy rose lotus dandelion tulip lily orchid poppy blossom hibiscus violet forget me not',
    nature:'tree leaf clover mushroom mountain flower sunflower daisy rose lotus dandelion tulip lily orchid poppy blossom pine acorn fern',
    celestial:'star moon sun planet saturn astronaut rocket comet constellation zodiac'
  };
  var THEME_TOKENS={},THEME_ALIASES={};Object.keys(THEME_WORDS).forEach(function(theme){THEME_TOKENS[theme]=new Set(THEME_MOTIFS[theme].split(' '));THEME_WORDS[theme].split(' ').forEach(function(word){THEME_ALIASES[word]=theme;});});
  var NAMED_MOTIFS=new Set(Object.values(THEME_MOTIFS).join(' ').split(' ').concat('heart circle square triangle diamond initial book apple tooth stethoscope ballet skating camera anchor airplane music guitar piano science volleyball basketball baseball soccer rune badge slate'.split(' ')));
  var ALIASES={rabbits:'bunny',rabbit:'bunny',bunnies:'bunny',puppies:'dog',puppy:'dog',kittens:'cat',kitten:'cat',wolves:'wolf',foxes:'fox',phoenixes:'phoenix',fishes:'fish',mice:'mouse',geese:'goose',leaves:'leaf',teeth:'tooth',rhinoceros:'rhino',rhinos:'rhino',honeybee:'bee',honeybees:'bee',ladybird:'ladybug',ladybirds:'ladybug',octopuses:'octopus',octopi:'octopus',octopodes:'octopus',lotuses:'lotus',irises:'iris',hibiscuses:'hibiscus',cacti:'cactus',cactuses:'cactus'};
  var CONTEXT=new Set(('short brief concise quick few listing listings remaining rest again a an the and or but however then for of on in to at by with from as my me you your our their her his its it is am are be have has do does did got can could would will please show find search looking look want wants need give get tell see browse list recommend suggest offer offering sell selling carry carries available availability stock selection collection collections catalogue catalog products product jewelry jewellery pieces piece items item options option pair pairs sets set these those this that what which how many kind kinds any all some more another else something anything instead actually meant mean no not dont don t without except excluding avoid just only ones one there really about themed theme design designs i we they let lets s im m for').split(' '));
  var PRIVATE_OR_CODE=/https?:\/\/|www\.|\/products\/|javascript\s*:|<\s*script\b|\b(?:api keys?|credentials?|passwords?|system prompt|private (?:records|data)|owner data|source code|repository|execute code|run (?:code|script)|ignore (?:all |previous |the )?(?:instructions|rules))\b/i;
  function clean(value,max){return typeof value==='string'?value.replace(/\u0000/g,'').normalize('NFKC').replace(/[’‘]/g,"'").trim().slice(0,max||2000):'';}
  function word(value){var w=String(value||'').toLowerCase();if(ALIASES[w])return ALIASES[w];if(w.length>4&&w.endsWith('ies'))w=w.slice(0,-3)+'y';else if(w.length>3&&w.endsWith('s')&&!/(?:ss|us|is|cosmos|mars)$/.test(w))w=w.slice(0,-1);return ALIASES[w]||w;}
  function words(value){return (clean(value,16000).toLowerCase().match(/[\p{L}\p{N}]+/gu)||[]).map(word);}
  function negativeAt(value,index){
    var prefix=value.slice(0,index).split(/[,;.!?]|\b(?:but|however|instead(?!\s+of\b)|actually|i (?:want|prefer|like|meant|mean)|we (?:want|prefer))\b/i).at(-1).slice(-90);
    return /\b(?:not|no|never|without|avoid|except|excluding|rather than|instead\s+of|don'?t|do not)\s+(?:\w+\s+){0,5}$/i.test(prefix);
  }
  function maskedNegatives(raw,pattern){var spans=[];for(var hit of raw.matchAll(new RegExp(pattern,'gi')))if(negativeAt(raw,hit.index))spans.push([hit.index,hit.index+hit[0].length]);for(var span of spans.reverse())raw=raw.slice(0,span[0])+' '.repeat(span[1]-span[0])+raw.slice(span[1]);return raw;}
  function material(value){
    var text=clean(value,1000).toLowerCase().replace(/(\d{1,2}\s*k(?:t)?)(?=[a-z])/g,'$1 ').replace(/gold[- ]?filled/g,'gold filled').replace(/gold[- ]?plated/g,'gold plated').replace(/rosegold/g,'rose gold');
    var karats=[...new Set([...text.matchAll(/\b(8|9|10|12|14|18|20|22|24)\s*(?:k(?:t)?|karats?|carats?)\b/g)].map(function(m){return Number(m[1]);}).concat(/\b14\s*\/\s*20\b/.test(text)?[14]:[]))];
    var silver=/\b(?:sterling(?:\s+silver)?|silver)\b/.test(text),gold=/\bgold\b|\bgf\b/.test(text)||karats.length>0,clear=/\b(?:any|either|both)\s+(?:metals?|materials?)\b/.test(text);
    if(!silver&&!gold&&!clear)return null;if(clear||silver&&gold)return {explicit:true,material:null,label:''};
    var family=silver?'silver':'gold',color=family==='gold'?/\brose\s+gold\b/.test(text)?'rose':/\bwhite\s+gold\b/.test(text)?'white':'yellow':null,kind=family==='silver'?/\bsterling\b/.test(text)?'sterling':'any':/\bgold\s+filled\b|\bgf\b/.test(text)?'filled':/\bplated\b|\bvermeil\b/.test(text)?'plated':/\bsolid\b/.test(text)?'solid':'any';
    var label=family==='silver'?(kind==='sterling'?'sterling silver':'silver'):(karats.length?karats.map(function(k){return k+'k';}).join(' or ')+' ':'')+(color==='yellow'?'':color+' ')+(kind==='solid'?'solid ':'')+'gold'+(kind==='filled'?' filled':kind==='plated'?' plated':'');
    return {explicit:true,material:{family:family,color:color,kind:kind,karats:karats},label:label};
  }
  function materialMatches(value,request){
    if(!request)return true;var actual=material(value);if(!actual?.material)return false;var m=actual.material;
    if(m.family!==request.family||request.color&&m.color!==request.color||request.kind!=='any'&&m.kind!==request.kind)return false;
    return !request.karats.length||m.karats.length===1&&request.karats.includes(m.karats[0]);
  }
  function stripMaterial(value){return value.replace(/\b(?:8|9|10|12|14|18|20|22|24)\s*(?:k(?:t)?|karats?|carats?)\b|\b14\s*\/\s*20\b/g,' ').replace(/\b(?:(?:solid|rose|white|yellow)\s+){0,2}(?:gold(?:\s*[- ]?\s*(?:filled|plated))?|sterling(?:\s+silver)?|silver|gf)\b(?:\s+(?:filled|plated|solid))?/g,' ');}
  function budget(value,defaultCurrency){
    var raw=clean(value,2000).toLowerCase(),amount='(?:(?:usd|cad|aud|nzd|eur|gbp)\\s*)?(?:(?:us|ca|c|au|a|nz)?[$£€]\\s*)?(\\d+(?:\\.\\d{1,2})?)(?:\\s*(?:usd|cad|aud|nzd|eur|gbp|dollars?))?';
    var spans=[],max=null,min=null;
    for(var pair of [['max','under|below|max(?:imum)?|up to|less than|at most|within|budget(?: of| is)?'],['min','over|above|min(?:imum)?|more than|at least']])for(var hit of raw.matchAll(new RegExp('\\b(?:'+pair[1]+')\\s*(?:(?:is|of|to|:)\\s*)?'+amount,'gi'))){spans.push([hit.index,hit.index+hit[0].length]);if(!negativeAt(raw,hit.index)){if(pair[0]==='max')max=Number(hit[1]);else min=Number(hit[1]);}}
    var range=new RegExp('\\bbetween\\s*'+amount+'\\s*(?:and|to|[-–])\\s*'+amount,'i').exec(raw);if(range){spans.push([range.index,range.index+range[0].length]);if(!negativeAt(raw,range.index)){min=Number(range[1]);max=Number(range[2]);}}
    var currencyMentions=[...raw.matchAll(/\b(?:usd|cad|aud|nzd|eur|gbp)\b|\b(?:ca|c|us|au|a|nz)\$|[£€]/g)].filter(function(m){return !negativeAt(raw,m.index);}).map(function(m){var code=m[0];return /^(?:ca|c)\$$/.test(code)?'CAD':code==='us$'?'USD':/^(?:au|a)\$$/.test(code)?'AUD':code==='nz$'?'NZD':code==='£'?'GBP':code==='€'?'EUR':code.toUpperCase();});
    var codes=[...new Set(currencyMentions)],explicit=max!==null||min!==null,currency=codes[0]||(explicit?defaultCurrency||'USD':null);
    for(var s of spans.sort(function(a,b){return b[0]-a[0];}))raw=raw.slice(0,s[0])+' '.repeat(s[1]-s[0])+raw.slice(s[1]);
    return {max:max,min:min,currency:currency,invalid:codes.length>1||max!==null&&min!==null&&min>max||max!==null&&max>1000000||min!==null&&min>1000000,explicit:explicit,text:raw};
  }
  function plan(query,options){
    options=options||{};var raw=clean(query,2000).toLowerCase().replace(/(\d{1,2}\s*k(?:t)?)(?=[a-z])/g,'$1 '),b=budget(raw,options.currency||'USD');
    var sort=/\b(?:cheapest|least expensive|lowest[- ]priced|price low to high)\b/.test(raw)?'price-asc':/\b(?:most expensive|highest[- ]priced|price high to low)\b/.test(raw)?'price-desc':null;
    var m=material(maskedNegatives(raw,'\\b(?:(?:solid|rose|white|yellow)\\s+){0,2}(?:gold(?:[- ]?(?:filled|plated))?|sterling(?:\\s+silver)?|silver|gf)\\b'));
    var categories=[],excludedCategories=[],categorySpans=[];
    Object.keys(CATEGORY_PATTERNS).forEach(function(category){var pattern=CATEGORY_PATTERNS[category];for(var hit of raw.matchAll(new RegExp(pattern.source,'gi'))){categorySpans.push([hit.index,hit.index+hit[0].length]);var list=negativeAt(raw,hit.index)?excludedCategories:categories;if(!list.includes(category))list.push(category);}});
    if(categories.includes('stud-earrings')||categories.includes('hoop-earrings'))categories=categories.filter(function(c){return c!=='earrings';});
    if(categories.includes('regular-necklaces')||categories.includes('beady-necklaces'))categories=categories.filter(function(c){return c!=='necklaces';});
    if(categories.includes('charm-only'))categories=categories.filter(function(c){return c!=='charms';});
    var terms=[],themes=[],excludedTerms=[],excludedThemes=[],text=stripMaterial(b.text).replace(/\b(?:usd|cad|aud|nzd|eur|gbp|dollars?)\b/g,' ').replace(/\b(?:cheapest|least expensive|lowest[- ]priced|price low to high|most expensive|highest[- ]priced|price high to low)\b/g,' ');
    for(var hit of text.matchAll(/[\p{L}\p{N}]+/gu)){var term=word(hit[0]),theme=THEME_ALIASES[hit[0]]||THEME_ALIASES[term],negative=negativeAt(text,hit.index);
      if(theme){var themeList=negative?excludedThemes:themes;if(!themeList.includes(theme))themeList.push(theme);continue;}
      if(CONTEXT.has(hit[0])||CONTEXT.has(term)||Object.values(CATEGORY_PATTERNS).some(function(p){return p.test(hit[0]);})||['regular','classic','standard','beady','beaded','bead','chain','loose','standalone','individual','previous','original','full','whole','entire','complete','reset','clear','switch'].includes(term))continue;
      var list=negative?excludedTerms:terms;if(!list.includes(term))list.push(term);
    }
    // A requested literal flower is narrower than the flower family, but all
    // independently named design words remain conjunctive (Rhino Profile).
    return {schema:1,categories:categories,terms:terms.slice(0,16),themes:themes,excludedTerms:excludedTerms.slice(0,16),excludedThemes:excludedThemes,excludedCategories:excludedCategories,max:b.max,min:b.min,currency:b.currency,invalid:b.invalid||PRIVATE_OR_CODE.test(raw)||clean(query,2100).length>250,material:m?.material||null,materialLabel:m?.label||null,materialExplicit:!!m,budgetExplicit:b.explicit,sort:sort};
  }
  function motifWords(product){
    var tags=(Array.isArray(product?.tags)?product.tags:[]).slice(0,80).flatMap(function(tag){var value=clean(tag,100),m=value.match(/^(?:motif|symbol|theme)\s*:\s*([\p{L}\p{N}\s-]+)$/iu);return m?[m[1]]:/^[\p{L}\p{N}-]+$/u.test(value)?[value]:[];});
    var finishFree=function(text){return text.replace(/\brose[ -]+gold\b/gi,' ');},primary=words(finishFree(clean(product?.title,300))+' '+clean(product?.type||product?.productType,100)+' '+tags.map(finishFree).join(' '));
    // A legacy slug cannot override a different currently named motif. A
    // canonical handle can supplement a generic title with no named design.
    return new Set(primary.concat(primary.some(function(t){return NAMED_MOTIFS.has(t);})?[]:words(finishFree(clean(product?.handle,180)))));
  }
  function themeMatches(product,theme,cachedWords){var tokens=cachedWords||motifWords(product),set=THEME_TOKENS[theme];return !!set&&([...tokens].some(function(t){return set.has(t);})||[...tokens].some(function(t){return THEME_ALIASES[t]===theme;}));}
  function motifMatches(product,value){var theme=THEME_ALIASES[String(value||'').toLowerCase()]||THEME_ALIASES[word(value)],tokens=motifWords(product);return theme?themeMatches(product,theme,tokens):words(value).every(function(t){return tokens.has(t);});}
  function categoryMatches(product,category){
    if(!category||category==='all'||category==='available')return true;if(!CATEGORY_PATTERNS[category])return false;
    var title=clean(product?.title,300),type=clean(product?.type||product?.productType,100),text=title+' '+type;
    var finished=/\b(?:necklaces?|earrings?|studs?|huggies?|hoops?|bracelets?|rings?)\b/i.test(title),charmAxis=(product?.options||[]).some(function(o){return /\bcharm type\b/i.test(o?.name||'');});
    if(/^(?:custom\s+)?(?:design\s+fees?\b|engraving(?:\s+(?:on|for|services?)\b|$))/i.test(title)||/\bchain\s+extenders?\b/i.test(title)&&!/\b(?:earrings?|studs?|huggies?|hoops?|bracelets?|rings?)\b/i.test(title))return false;
    if(category==='stud-earrings')return categoryMatches(product,'earrings')&&/\bstuds?\b/i.test(text);
    if(category==='hoop-earrings')return categoryMatches(product,'earrings')&&/\b(?:hoops?|huggies?)\b/i.test(text);
    if(category==='charm-only')return !finished&&!/\b(?:engraving|design fee|chain extender)\b/i.test(title)&&/\bcharms?\b/i.test(text)||charmAxis&&/\bcharms?\b/i.test(type)&&!/\bearrings?\b/i.test(title);
    if(category==='regular-necklaces'||category==='beady-necklaces'){
      if(!categoryMatches(product,'necklaces')||/\bnecklace\s+charms?\b/i.test(title))return false;
      var chains=(product?.options||[]).filter(function(o){return /\bchain\b/i.test(o?.name||'');}).flatMap(function(o){return Array.isArray(o.values)?o.values:[];}),beady=/\b(?:beads?|beady|beaded)\b/i.test(text)||chains.some(function(v){return /\b(?:beads?|beady|beaded)\b/i.test(String(v));});return category==='beady-necklaces'?beady:!beady;
    }
    var structural=/^(?:necklaces?|pendants?)$/i.test(type)?'necklaces':/^(?:earrings?|(?:studs?|huggies?|hoops?)(?:\s+earrings?)?)$/i.test(type)?'earrings':/^bracelets?$/i.test(type)?'bracelets':/^rings?$/i.test(type)?'rings':null;
    if(structural)return category===structural;
    if(category==='earrings'&&charmAxis&&/\bcharms?\b/i.test(type))return false;
    if(category==='necklaces'&&/\b(?:necklace|pendant)\s+charms?\b/i.test(title))return false;
    if(category==='necklaces'&&/\bearrings?\b/i.test(title)&&!/\bnecklaces?\b/i.test(title))return false;
    var studio=/^(?:custom\s+charm\s+)?studio(?:\s+(?:services?|components?|add[ -]?ons?))?$/i.test(type);
    var tags=(Array.isArray(product?.tags)?product.tags:[]).slice(0,80).flatMap(function(tag){var m=clean(tag,100).match(/^(?:product[ _-]?type|category)\s*:\s*(necklaces?|pendants?|earrings?|studs?|huggies?|hoops?|bracelets?|rings?|charms?)$/i);return m?[m[1]]:[];});
    return CATEGORY_PATTERNS[category].test(title+' '+(studio?'':type)+' '+tags.join(' '));
  }
  function matchingVariants(product,p,options){
    options=options||{};if(p.invalid)return [];return (Array.isArray(product?.variants)?product.variants:[]).filter(function(v){
      if(options.available!==false&&(v.available!==true||v.availabilityKnown===false))return false;
      if(!Number.isFinite(v.price)||v.price<0||p.currency&&product.currency!==p.currency||p.max!==null&&p.max!==undefined&&v.price>p.max||p.min!==null&&p.min!==undefined&&v.price<p.min)return false;
      if(!p.material)return true;var evidence=(Array.isArray(v.options)?v.options:[]).filter(function(o){return /metal|material|finish/i.test(o?.name||'');});return evidence.length?evidence.some(function(o){return materialMatches(o.value,p.material);}):materialMatches(v.title,p.material);
    });
  }
  function match(product,p,options){
    p=typeof p==='string'?plan(p):p||plan('');options=options||{};if(p.invalid)return false;var classify=options.categoryMatches||categoryMatches;
    if(p.categories?.length&&!p.categories.some(function(c){return classify(product,c);})||p.excludedCategories?.some(function(c){return classify(product,c);}))return false;
    var tokens=options.tokens||motifWords(product);
    if(p.terms?.some(function(t){return !tokens.has(word(t));})||p.themes?.some(function(t){return !themeMatches(product,t,tokens);})||p.excludedTerms?.some(function(t){return tokens.has(word(t));})||p.excludedThemes?.some(function(t){return themeMatches(product,t,tokens);}))return false;
    var variantCriteria=!!p.material||p.max!==null&&p.max!==undefined||p.min!==null&&p.min!==undefined||!!p.currency;
    return options.variants===false||!variantCriteria||matchingVariants(product,p).length>0;
  }
  function queryFor(p){
    var bits=[...(p.terms||[]),...(p.themes||[]).map(function(t){return t==='animals'?'animal':t==='birds'?'bird':t==='pets'?'pet':t==='insects'?'insect':t;}),...(p.categories||[]).map(function(c){return CATEGORY_LABELS[c];})];
    if(p.materialLabel)bits.push(p.materialLabel);if(p.max!==null&&p.max!==undefined)bits.push('under '+p.max+' '+(p.currency||'USD'));if(p.min!==null&&p.min!==undefined)bits.push('at least '+p.min+' '+(p.currency||'USD'));
    if(p.excludedTerms?.length)bits.push('without '+p.excludedTerms.join(' or '));if(p.excludedThemes?.length)bits.push('without '+p.excludedThemes.map(function(t){return t==='animals'?'animal':t;}).join(' or '));
    if(p.excludedCategories?.length)bits.push('without '+p.excludedCategories.map(function(c){return CATEGORY_LABELS[c];}).filter(Boolean).join(' or '));
    return bits.filter(Boolean).join(' ').slice(0,250);
  }
  function priorPlan(prior,options){
    if(typeof prior==='string')return plan(prior,options);if(prior?.schema===1&&Array.isArray(prior.categories)&&Array.isArray(prior.terms))return {...prior};
    if(prior?.plan)return priorPlan(prior.plan,options);if(!prior||typeof prior!=='object')return plan('',options);
    var type=prior.storeCategory||({necklace:'necklaces',pendant:'necklaces',earrings:'earrings',studs:'stud-earrings',huggie:'hoop-earrings',charm:'charms',bracelet:'bracelets',ring:'rings'}[prior.type]),q=[prior.query||''].concat(type?CATEGORY_LABELS[type]||type:[]).join(' '),p=plan(q,{currency:prior.currency||options?.currency||'USD'});
    if(prior.materialQuery||prior.metal&&!p.material){var m=material(prior.materialQuery||prior.metal);p.material=m?.material||null;p.materialLabel=m?.label||null;}if(Number.isFinite(prior.budget)){p.max=prior.budget;p.currency=prior.budgetCurrency||prior.currency||'USD';}if(Number.isFinite(prior.minBudget))p.min=prior.minBudget;return p;
  }
  function discovery(message,prior,options){
    options=options||{};var raw=clean(message,2100).toLowerCase(),p=plan(raw,options),none={recognized:false,denied:false,mode:'none',query:'',plan:p};
    if(!raw||p.invalid||PRIVATE_OR_CODE.test(raw))return none;
    var previous=priorPlan(prior,options),hasPrior=!!(previous.terms.length||previous.themes.length||previous.categories.length),polite=raw.replace(/^(?:(?:please|can you|could you|would you|will you)\s+)+/,'');
    // Continuations refer only to the completed, checked discovery. They never
    // supply an offset, product identity or new authority for a page control.
    if(/^(?:(?:show|list)(?: me)?|let me see)?\s*(?:the )?(?:rest|remaining(?: (?:ones|pieces|items|listings|matches|results))?|all (?:the )?(?:remaining|other)(?: (?:ones|pieces|items|listings|matches|results))?)(?:\s+please)?[.!?]*$/.test(polite))return {recognized:true,denied:false,mode:hasPrior?'rest':'need-prior',query:queryFor(previous),plan:previous};
    if(/^(?:(?:(?:show|list)(?: me)?|let me see)\s+(?:some |the )?(?:more|next(?: (?:six|6))?|other)(?: (?:ones|pieces|items|listings|matches|results))?|(?:more|next)(?: (?:ones|pieces|items|listings|matches|results))?|what else(?: do you have| have you got)?)(?:\s+please)?[.!?]*$/.test(polite))return {recognized:true,denied:false,mode:hasPrior?'more':'need-prior',query:queryFor(previous),plan:previous};
    if(hasPrior&&/^(?:in )?(?:any|either|both) (?:metals?|materials?)(?:\s+please)?[.!?]*$/.test(polite)){p={...previous,material:null,materialLabel:null,materialExplicit:true};return {recognized:true,denied:false,mode:'refine',query:queryFor(p),plan:p};}
    if(hasPrior&&/^(?:at )?(?:any price|any budget|no (?:price|budget) limit)(?:\s+please)?[.!?]*$/.test(polite)){p={...previous,min:null,max:null,currency:null,budgetExplicit:true};return {recognized:true,denied:false,mode:'refine',query:queryFor(p),plan:p};}
    var scoped=/\bonly\s*[.!?]*$/.test(polite)||/^(?:(?:show|list)(?: me)?\s+)?(?:just|only|the)\s+/.test(polite)||/^(?:of|from|among) (?:those|these|them)\b/.test(polite)||/\b(?:of|from|among) (?:those|these|them)\s*[.!?]*$/.test(polite);
    // A current-product question is knowledge; a broad type/theme question is
    // discovery even when it starts "Do you have". This does not grant a click.
    if(/^(?:please\s+)?(?:what am i (?:looking at|viewing)\b|(?:what|which) (?:piece|product|listing|item) is (?:this|it)\b|what is (?:this|the current|that)(?: (?:piece|product|listing|item))?\b)/.test(raw))return none;
    if(/\b(?:this|that|these|those|it|its|they|them|their|current|selected|same|first|second|third|fourth|fifth|sixth)\b/.test(raw)&&!(hasPrior&&scoped)&&!/\b(?:i meant|i mean|instead|actually|not those|not these|other|different)\b/.test(raw))return none;
    var withoutSort=raw.replace(/\bprice (?:low to high|high to low)\b/g,' ');
    if(/\b(?:cart|bag|checkout|check out|payment|order status|shipping|deliver(?:y|ies|ed)?|returns?|refund|discounts?|coupons?|promotions?|promos?|sales?|offers|gift wrap|gift wrapping|gift note|sourcing|engraving|menus?|dropdowns?|options?|choices?|choose|pick|select|gallery|images?|photos?|pictures?|viewer|dimensions?|measurements?|details?|prices?|costs?|sizes?|sizing|lengths?|width|height|diameter|thickness|weight|how (?:much|big|large)|(?:which|what)\s+(?:metal|material)\b|made (?:of|from)|tell me(?: more)? about|describe|explain)\b/.test(withoutSort)||/^(?:(?:please|could you|can you|would you|will you)\s+)*(?:open|view|go|take|navigate|highlight|scroll|zoom|enlarge|select|choose|pick|set|use|change|help|enable|disable|sort|filter|reset|clear|add|put|remove|delete|undo|close|back|forward)\b/.test(raw))return none;
    if(/\b(?:i said|i asked|you said|you asked|earlier|yesterday|quoted|quote|if i|whether i)\b/.test(raw))return none;
    var correction=/\b(?:instead|actually|i meant|i mean|not those|not these)\b/.test(raw)||/^no[, ]+(?:i )?(?:want|would like|need)/.test(raw);
    var prohibited=/\b(?:do not|don'?t|never)\s+(?:\w+\s+){0,4}(?:show|find|search|browse|recommend|suggest)\b/.test(raw)||/^(?:no|not)\s+(?!i\b)/.test(raw)&&!correction;
    if(prohibited&&!/\b(?:but|instead)\b/.test(raw))return {recognized:true,denied:true,mode:'none',query:'',plan:p};
    if(scoped)p=plan(raw.replace(/\b(?:of|from|among) (?:those|these|them)\b/g,' '),options);
    var meaningful=p.terms.length||p.themes.length||p.categories.length,excluded=p.excludedTerms.length||p.excludedThemes.length||p.excludedCategories.length,refinable=p.materialExplicit||p.budgetExplicit||!!p.sort;
    // Deliberate short narrowing changes the format while retaining the
    // shopper's checked motif, exact material and item-price constraints.
    // "Show earrings" remains a fresh category request; "just earrings" is
    // the explicit continuation of the previous discovery.
    if(hasPrior&&scoped&&p.categories.length&&!p.terms.length&&!p.themes.length){
      p={...previous,categories:p.categories,...(p.materialExplicit?{material:p.material,materialLabel:p.materialLabel,materialExplicit:true}:{}),...(p.budgetExplicit?{min:p.min,max:p.max,currency:p.currency,budgetExplicit:true}:{}),...(p.sort?{sort:p.sort}:{}),excludedTerms:Array.from(new Set(previous.excludedTerms.concat(p.excludedTerms))),excludedThemes:Array.from(new Set(previous.excludedThemes.concat(p.excludedThemes))),excludedCategories:Array.from(new Set(previous.excludedCategories.concat(p.excludedCategories))),invalid:p.invalid};return {recognized:true,denied:false,mode:'refine-category',query:queryFor(p),plan:p};
    }
    var inventoryNoun='(?:jewelry|jewellery|products?|pieces?|items?|selection|collection|catalogue|catalog|shop|store)';
    var fullBrowse=new RegExp('^(?:show (?:me )?|browse |list (?:me )?|let me see )(?:(?:your|the) )?(?:full|whole|entire|complete) '+inventoryNoun+'(?:\\s+(?:please|today|here|now))?[.!?]*$').test(polite);
    var broad=fullBrowse||/^(?:what (?:do you (?:have|sell|stock|carry|offer)|have you got|is available|else (?:do you have|have you got))|what(?:'s| is) (?:in (?:your|the) (?:shop|store|catalogue|catalog|collection|selection)|available)|(?:show|list) (?:me )?(?:all|everything|what (?:you have|you\x27ve got))|browse (?:all|everything)|let me see (?:all|everything))(?:\s+(?:please|today|here|now))?[.!?]*$/.test(polite)||new RegExp('^(?:what (?:kinds?|types?) (?:of )?'+inventoryNoun+' do you (?:sell|have|stock|carry|offer)|what '+inventoryNoun+' (?:do you (?:have|sell|stock|carry|offer)|is available)|do you (?:have|sell|stock|carry|offer) (?:any )?'+inventoryNoun+'|(?:show (?:me )?|browse |list (?:me )?|let me see )(?:all (?:of )?)?(?:(?:your|the) )?'+inventoryNoun+')(?:\\s+(?:please|today|here|now))?[.!?]*$').test(polite);
    if(broad){
      if(/\belse\b/.test(raw)&&hasPrior)return {recognized:true,denied:false,mode:'more',query:queryFor(previous),plan:previous};
      // An explicit full browse must retain the host's loaded collection and
      // pagination. A bounded assistant overview is not the whole inventory.
      var browseAll=fullBrowse||/^(?:show(?: me)?|browse|list(?: me)?|let me see)\s+(?:all\b|everything\b)/.test(polite);
      return {recognized:true,denied:false,mode:'browse',browseAll:browseAll,query:'',plan:plan('',options)};
    }
    var browseWords=/\b(?:show|find|search|look for|browse|list|recommend|suggest|what|which|have|got|offer|looking for|want|need)\b/.test(raw);
    if(hasPrior&&(excluded&&!meaningful||scoped&&meaningful)){
      p={...previous,...(p.categories.length?{categories:p.categories}:{}),...(p.terms.length?{terms:previous.terms.concat(p.terms)}:{}),...(p.themes.length?{themes:Array.from(new Set(previous.themes.concat(p.themes)))}:{}),excludedTerms:Array.from(new Set(previous.excludedTerms.concat(p.excludedTerms))),excludedThemes:Array.from(new Set(previous.excludedThemes.concat(p.excludedThemes))),excludedCategories:Array.from(new Set(previous.excludedCategories.concat(p.excludedCategories))),...(p.materialExplicit?{material:p.material,materialLabel:p.materialLabel,materialExplicit:true}:{}),...(p.budgetExplicit?{min:p.min,max:p.max,currency:p.currency,budgetExplicit:true}:{}),invalid:p.invalid};return {recognized:true,denied:false,mode:'refine',query:queryFor(p),plan:p};
    }
    if(!meaningful&&!refinable)return none;
    if(!browseWords&&!correction&&!refinable&&!p.categories.length&&!p.themes.length&&!options.allowBareMotif)return none;
    var refine=!meaningful&&refinable;
    if(refine){if(!previous.terms.length&&!previous.themes.length&&!previous.categories.length&&!/\b(?:show|find|browse|jewelry|jewellery|products?|pieces?|items?|what do you have|anything|everything)\b/.test(raw))return none;p={...previous,...(p.materialExplicit?{material:p.material,materialLabel:p.materialLabel,materialExplicit:true}:{}),...(p.budgetExplicit?{max:p.max,min:p.min,currency:p.currency,budgetExplicit:true}:{}),...(p.sort?{sort:p.sort}:{}),invalid:p.invalid};}
    else if(correction){if(!p.categories.length)p.categories=previous.categories||[];if(!p.materialExplicit){p.material=previous.material;p.materialLabel=previous.materialLabel;}if(!p.budgetExplicit){p.max=previous.max;p.min=previous.min;p.currency=previous.currency;}}
    if(!refine&&!correction&&p.categories.length&&!p.terms.length&&!p.themes.length){if(!p.materialExplicit){p.material=previous.material;p.materialLabel=previous.materialLabel;}if(!p.budgetExplicit){p.min=previous.min;p.max=previous.max;p.currency=previous.currency;}}
    return {recognized:true,denied:false,mode:refine?'refine':'search',query:queryFor(p),plan:p};
  }
  function makeIndex(products,options){
    options=options||{};var rows=[],ids=new Map(),handles=new Map(),conflicted=new Set(),clock=typeof options.now==='function'?options.now:function(){return Number.isFinite(options.now)?options.now:Date.now();},maxAge=Number.isFinite(options.maxAgeMs)?Math.max(0,options.maxAgeMs):300000;
    for(var p of (Array.isArray(products)?products:[]).slice(0,12000)){
      if(!/^gid:\/\/shopify\/Product\/[1-9]\d*$/.test(p?.id||'')||!/^[a-z0-9_-]{1,180}$/.test(p?.handle||'')||!clean(p.title,300))continue;
      var same=ids.get(p.id),other=handles.get(p.handle);if(same&&same.handle!==p.handle||other&&other!==p.id){conflicted.add(p.id);if(other)conflicted.add(other);if(same)conflicted.add(same.id);continue;}
      if(!same){ids.set(p.id,p);handles.set(p.handle,p.id);rows.push(p);}else if(Number(p.checkedAt)>Number(same.checkedAt)){rows[rows.indexOf(same)]=p;ids.set(p.id,p);}
    }
    rows=rows.filter(function(p){return !conflicted.has(p.id);});
    var lookup=rows.map(function(p){return {product:p,tokens:motifWords(p)};});
    function search(value,params){
      params=params||{};var p=typeof value==='string'?plan(value,params):value||plan(''),now=clock(),stale=0,matched=[],available=[],unavailable=[],unknown=[],held=[],eligible=[];
      for(var item of lookup){var product=item.product;if(!Number.isFinite(product.checkedAt)||now-product.checkedAt>maxAge||product.checkedAt>now+60000){stale++;continue;}if(!match(product,p,{variants:false,categoryMatches:params.categoryMatches,tokens:item.tokens}))continue;matched.push(product);
        if(product.recommendationHold===true||product.cartHold===true){held.push(product);continue;}
        var variants=matchingVariants(product,p),known=(product.variants||[]).filter(function(v){return typeof v.available==='boolean'&&v.availabilityKnown!==false;}),published=matchingVariants(product,p,{available:false});
        if(variants.length){available.push(product);eligible.push(product);}else if(known.length&&known.length===(product.variants||[]).length){if(published.length&&published.every(function(v){return v.available===false;}))unavailable.push(product);}else unknown.push(product);
      }
      var limit=Number.isInteger(params.limit)?Math.max(1,Math.min(200,params.limit)):24,status=p.invalid?'invalid':eligible.length?'matches':unknown.length?'unconfirmed':unavailable.length?'unavailable':held.length===matched.length&&held.length?'held':matched.length?'options_unavailable':!rows.length?'empty':stale===rows.length?'stale':'no_matches';
      return {plan:p,products:eligible.slice(0,limit),total:matched.length,availableCount:available.length,unavailableProducts:unavailable.slice(0,limit),unconfirmedProducts:unknown.slice(0,limit),heldCount:held.length,status:status,coverage:options.coverage||'loaded_collection',indexedCount:rows.length,staleCount:stale,checkedAt:now};
    }
    return Object.freeze({search:search,size:rows.length});
  }
  function searchTerms(p){p=typeof p==='string'?plan(p):p;return [...(p?.terms||[]),...(p?.themes||[]).flatMap(function(t){return THEME_MOTIFS[t]?.split(' ')||[];})].slice(0,90);}
  return Object.freeze({schema:1,plan:plan,discovery:discovery,match:match,motifMatches:motifMatches,categoryMatches:categoryMatches,matchingVariants:matchingVariants,material:material,materialMatches:materialMatches,makeIndex:makeIndex,queryFor:queryFor,searchTerms:searchTerms,words:words,themeNames:Object.freeze(Object.keys(THEME_WORDS))});
});
