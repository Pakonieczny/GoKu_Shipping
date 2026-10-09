/* Partial sheets, the nesting side (Paul, 7 Oct 2026): a sheet already in use moves ALL its pieces onto a partial sheet he picks (the leftover's exact
   outline is the stock, as Rose Gold's remnant profile is today); what does not fit continues on the next partial sheet, then the next, with no limit
   on how many take part; and the per-metal policy (reuse partial sheets by itself, or always start a brand new sheet at a size he sets) is kept by
   the nester. window.PartialNest. The contract for the Partial Sheet panel is /mnt/project-files/plans/partial-sheets/contract.md.

   How it works, in the app's own machinery (nothing is re-invented):
   - preview(sh, ids): a REAL trial pack, in a one-off worker running CharmNestSolver on each partial's profile (the same profile the nest packs
     against), quick (a few seconds at most per partial, it stops as soon as everything fits and settles), never writes or claims anything. A multi-piece
     order is never split (as the page's keepOrdersWhole): an order with a piece that does not fit moves on whole.
   - seat(sh, ids): ONE server transaction gives back the physical sheet the sheet holds (a fresh uncut gold sheet is deleted, a leftover goes back to the
     list) and claims the first partial (roseClaim with swap + exact + partialId; the answer is the stock the solver packs against; a refused claim loses
     nothing), then the sheet is marked changed and nested by hand again, exactly as the Nest button does (the saved record, files, QR and label follow the
     rules of any re-nest). The partials still to come are kept on the sheet (sh._partialChain).
   - the chain: when that nest ends with pieces that did not fit, the page's own overflow (overflowToNextSheet) asks nextPage(sh): a new sheet page of the
     metal, made with the next partial as its stock (claimed at its nest start, nestClaim). When the listed partials run out the policy decides: automatic
     takes the best fit of the metal's other available partials, otherwise (and for 'new') a new sheet as the nester makes it today.
   - incoming pieces take the same road: they nest on the open sheet first; what does not fit overflows to the next page of the chain.
   Only what is physically or logically impossible stops a sheet (Paul, 7 Oct 2026: "I'm prevented from using All, new sheets and existing partial sheets.
   There's no reason why I should be prevented."): a sheet in a committed or the current set, approved for Laser cutting but not cut, opened from a saved set,
   released full, or holding a saved green line that was never cut moves its pieces onto any partial sheet or made sheet; it keeps its place in its set (seat).
   And "This sheet is already in use so why can't I choose to use it?": so does a sheet with a RECORDED CUT (Cut Sheet) that the laser has not marked
   Completed. Cut Sheet draws the green line and records the cut; the metal is only really cut when the sheet is Completed (laserDoneAt). The server sets the
   recorded cut aside in the same transaction as the claim (the cut's own record, the leftover it made and the history stay for good; the sheet's cut mark goes,
   so it owes a new Cut Sheet and its approval on the new seat; the leftover that cut made is claimed when it is the partial chosen, otherwise it is no longer
   free metal and shows Discarded). Only a Completed (laser cut) sheet, a sheet busy right now, one with nothing to move and no cloud refuse. */
(function init(){
  'use strict';
  if(!window.CN||!window.RoseStock||!window.CharmNestRose){setTimeout(init,150);return;}
  const C=window.CN,S=C.S,R=window.CharmNestRose,MM=25.4/72;
  const cuts=metal=>!!metal&&(metal==='rose'||R.cuts(metal));
  const listeners=new Set();
  // the last answers of preview, so the commit does what was shown: the same sheet, the same partials asked, the same pieces
  const seen=new Map(),sigOf=(sh,ids)=>[sh.sheetId||sh.page,(ids||[]).join(),piecesOf(sh).map(c=>c.id).join()].join('|');
  const emit=e=>{for(const f of [...listeners]){try{f(e);}catch(_){}}};
  const refuse=(code,why)=>({ok:false,code,why});
  const label=sh=>{try{const v=window.SheetEvents&&window.SheetEvents.label&&window.SheetEvents.label(sh);if(v)return v;}catch(_){}return `${R.cutCode(sh.metal)} Sheet ${sh.page||1}`;};
  const metalWord=m=>R.cutWord(m);
  const pagesOf=m=>(S.sheets[m]&&S.sheets[m].pages)||[];
  const inSet=sh=>!!sh.setId&&!sh.draft;
  const piecesOf=sh=>C.activeCharms(sh);
  const plural=(n,one,many)=>n===1?one:many||one+'s';
  const mm1=v=>Math.round(v*10)/10;

  /* ── pairs and groups (PAIRPARTIAL, Paul 9 Oct: a pair, a disc necklace or any order of several pieces is never split across sheets) ──
     The never-split unit here is the page's own (keepOrdersWhole): the order (receipt), `c.order`. A group (one order line) is the unit the words speak of: "1 of the 2 pieces". */
  const PP=()=>window.CharmNestPartial||null;
  const orderOf=c=>c.order||c.id;
  const groupKey=c=>{const P=PP();return P&&P.groupKeyOf?P.groupKeyOf(c):String(orderOf(c));};
  const pieceWords=list=>{const P=PP();return P&&P.pieceWords?P.pieceWords(list):`${list.length} ${plural(list.length,'piece')}`;};
  // the pieces of each order: Map order -> [piece]
  const unitsOf=list=>{const m=new Map();for(const c of list){const k=orderOf(c);if(!m.has(k))m.set(k,[]);m.get(k).push(c);}return m;};
  const sizesOf=list=>[...unitsOf(list).values()].map(u=>u.length);
  const smallestUnit=list=>{const z=sizesOf(list);return z.length?Math.min(...z):0;};
  const largestUnit=list=>{const z=sizesOf(list);return z.length?Math.max(...z):0;};
  const orderWord=k=>String(k).split('/')[0];
  /* what a trial put only PART of on a partial: [{ order, placed, of, side? }] (placed pieces in the trial, pieces of the order in `charms`; side = "Left" | "Right", the piece of a
     2-piece earring pair that DID fit, so the words can name the one that did not: Amendment 2, every earring piece has its side) */
  const SIDE={L:'Left',R:'Right'};
  function splitsOf(charms,trialIds){
    const out=[];
    for(const [k,u] of unitsOf(charms)){
      const here=u.filter(c=>trialIds.has(c.id)),placed=here.length;
      if(placed>0&&placed<u.length){
        const row={order:orderWord(k),placed,of:u.length};
        if(u.length===2&&placed===1){const a=SIDE[here[0].side],b=SIDE[u.find(c=>c!==here[0]).side];if(a&&b&&a!==b)row.side=a;}
        out.push(row);
      }
    }
    return out;
  }
  const splitWords=list=>{
    if(!list||!list.length)return '';
    if(list.length===1){const s=list[0],which=s.side?` (the ${s.side} one; the ${s.side==='Left'?'Right':'Left'} one does not)`:'';return `Only ${s.placed} of the ${s.of} pieces of order ${s.order} fit${s.placed===1?'s':''} on it${which}, and ${s.of===2?'a pair':'an order'} is never split across sheets, so the order moves on whole.`;}
    const s=list[0];return `Only part of ${list.length} orders fits on it (for example order ${s.order}: ${s.placed} of its ${s.of} pieces), and an order is never split across sheets, so those orders move on whole.`;
  };
  /* the groups of this sheet that also have pieces on ANOTHER sheet (a pair already split, R3): [{ group, order, here, elsewhere, sheets }] */
  function sharedOf(sh,all){
    const mine=new Map();for(const c of all){const k=groupKey(c);mine.set(k,(mine.get(k)||0)+1);}
    const there=new Map();
    try{
      for(const p of (C.allSheets?C.allSheets():[])){
        if(p===sh)continue;
        for(const c of (C.activeCharms?C.activeCharms(p):(p.charms||[]))){const k=groupKey(c);if(!mine.has(k))continue;const o=there.get(k)||{n:0,sheets:new Set()};o.n++;o.sheets.add(label(p));there.set(k,o);}
      }
    }catch(_){return [];}
    return [...there].map(([k,o])=>({group:k,order:String(k).split(':')[0],here:mine.get(k),elsewhere:o.n,sheets:[...o.sheets]}));
  }
  const sharedWords=list=>{
    if(!list||!list.length)return '';
    if(list.length===1){const o=list[0];return `Order ${o.order} also has ${o.elsewhere} ${plural(o.elsewhere,'piece')} on ${o.sheets.join(' and ')}. A piece of it that does not fit here moves to another sheet, so the order would sit on more sheets than before.`;}
    const o=list[0];return `${list.length} orders on this sheet also have pieces on other sheets (for example order ${o.order} on ${o.sheets.join(' and ')}). Pieces that do not fit here move to another sheet, so those orders would sit on more sheets than before.`;
  };

  /* ── the policy (PartialSheets.policy: PS3's cache; auto when it is not loaded) ── */
  function policy(metal){
    try{const p=(window.PartialSheets&&window.PartialSheets.policy&&window.PartialSheets.policy(metal))||(typeof window.partialPolicy==='function'&&window.partialPolicy(metal));
      if(p&&(p.mode==='new'||p.mode==='auto'))return {mode:p.mode,wMm:+p.wMm||100,hMm:+p.hMm||50};}catch(_){}
    return {mode:'auto',wMm:100,hMm:50};
  }
  const mode=metal=>cuts(metal)?policy(metal).mode:'auto';
  // 'new': a sheet that was never nested takes the size the person stipulated (the sheet's own stock, as stockFor reads it; it is saved with the
  // sheet's record like any size). A sheet that has been nested keeps the size it has.
  function newSheet(sh){
    if(!sh||!cuts(sh.metal)||sh.sheetId||sh.roseStock||sh.keptStock||sh.newStock||sh.recalled||(sh.placements&&sh.placements.length))return false;
    const p=policy(sh.metal);if(p.mode!=='new'||!(p.wMm>=5&&p.wMm<=500&&p.hMm>=5&&p.hMm<=500))return false;
    sh.newStock={wPt:p.wMm/MM,hPt:p.hMm/MM,wIn:p.wMm/25.4,hIn:p.hMm/25.4};return true;
  }

  /* ── the partials: cards from PS3's list (the available ones of the metal), their stock as the solver packs against it ── */
  const cache={},stocks=new Map();
  async function cardsOf(metal){
    const PS=window.PartialSheets;let items;
    if(PS&&PS.list){const a=await PS.list(metal);items=(a&&a.items)||[];}
    else{   // PS3's module is not on this page: the leftovers of the metal as the stock list gives them (a card without its outline picture)
      const r=await C.api('charmNestLibrary',{op:'roseList',metal},{quiet:true});
      items=((r&&r.stocks)||[]).filter(s=>s.revision>0&&s.profileJson).map(s=>({id:s.id+'-'+s.revision,stockId:s.id,metal,status:'available',revision:s.revision,wPt:s.wPt,hPt:s.hPt,profileJson:s.profileJson,sheetWMm:s.wPt*MM,sheetHMm:s.hPt*MM,lastUsedAt:s.lastCutAt||0}));
    }
    cache[metal]={at:Date.now(),items};return items;
  }
  const stockKey=c=>(c.stockId||c.id)+':'+(c.revision==null?'':c.revision);
  // a sheet a person made (Options, New sheet: kind 'new') is a partial like any other, but revision 0 with no profile: the whole rectangle, uncut
  const madeCard=c=>!!c&&(c.kind==='new'||(c.revision===0&&c.status!=null&&!c.profileJson));
  async function stockOf(card){
    if((card.profileJson||madeCard(card))&&card.wPt&&card.hPt)return {id:card.stockId||card.id,metal:card.metal,wPt:card.wPt,hPt:card.hPt,revision:card.revision||0,profileJson:card.profileJson};
    const k=stockKey(card);if(stocks.has(k))return stocks.get(k);
    // PS3's page cache of the physical sheets under partials (ONE op for several ids, a revision's profile never changes)
    try{
      const PS=window.PartialSheets;
      if(PS&&PS.stocks){const r=(await PS.stocks([card.id]))[card.id];
        if(r&&(r.profileJson||(madeCard(card)&&!(r.revision>0)))&&r.current!==false){const out={id:r.stockId,metal:card.metal||r.metal,wPt:r.wPt,hPt:r.hPt,revision:r.revision||0,profileJson:r.profileJson||null};stocks.set(k,out);return out;}}
    }catch(_){}
    const r=await C.api('charmNestLibrary',{op:'roseGet',stockId:card.stockId||card.id,metal:card.metal,noCuts:true},{quiet:true});   // ONE document read: the physical sheet's size and profile, not its cuts
    const s=r&&r.stock;if(!s||(!s.profileJson&&!(madeCard(card)&&!(s.revision>0))))throw new Error('This partial sheet could not be read');
    const out={id:s.id,metal:card.metal,wPt:s.wPt,hPt:s.hPt,revision:s.revision||0,profileJson:s.profileJson||null};stocks.set(k,out);return out;
  }
  const cardSize=c=>({wMm:c.wMm!=null?c.wMm:c.bboxMm?c.bboxMm.w:c.sheetWMm||0,hMm:c.hMm!=null?c.hMm:c.bboxMm?c.bboxMm.h:c.sheetHMm||0});
  const cutOf=sh=>!!sh.roseCutAt||!!(sh.recalled&&sh.recalled.roseCutAt);   // (a recorded cut: Cut Sheet was pressed; the sheet is only cut for good when doneOf says so)
  function partialOf(sh){
    if(!sh)return null;
    if(sh._partialId)return sh._partialId;
    const s=sh.roseStock;if(!(s&&s.id))return null;
    // (a cut sheet's stock is the leftover its cut made; the sheet itself sits on the revision it was cut on, which roseRevision keeps)
    const rev=cutOf(sh)&&Number.isFinite(+sh.roseRevision)?+sh.roseRevision:(s.revision||0);
    return rev>0||s.made?s.id+'-'+rev:null;   // (a made sheet, revision 0, is recognised by the stock's own made mark, or by _partialId above)
  }

  /* ── what can be seated: only what is physically or logically impossible refuses (see the head of this file) ── */
  const doneOf=sh=>+sh.laserDoneAt>0||+(sh.recalled&&sh.recalled.laserDoneAt)>0;   // (a sheet opened from a saved set is drawn from its record: the mark is on the record)
  // a sheet opened from a saved set has no pieces on the page until it is rebuilt from its record (Recall.rebuild, "Rebuild to edit"); seating does that first
  const unopened=sh=>!!sh.recalled&&!(sh.charms&&sh.charms.length);
  const canOpen=()=>!!(window.Recall&&typeof window.Recall.rebuild==='function');
  function canSeat(sh){
    if(!sh||!sh.metal||!cuts(sh.metal))return refuse('metal','Partial sheets are for Rose Gold, 10K Gold and 14K Gold sheets.');
    // (a recorded cut alone is no reason: it is set aside by the move. Completed means the laser really cut the metal.)
    if(doneOf(sh))return refuse('laser','This sheet is Completed (laser cut): its metal has been cut, so its pieces cannot move to another partial sheet.');
    // (Gate.holding: the sheet is being rewritten by something else right now, the sheet window or an order's release)
    if(['nesting','finishing','queued'].includes(sh.status)||sh._operationStarting||sh._partialBusy||(sh.persisted&&!sh.persistedDone)||(window.Gate&&window.Gate.holding&&window.Gate.holding(sh)))return refuse('busy','This sheet is nesting or saving right now. Wait a moment, then choose the partial sheet.');
    if(unopened(sh)){
      if(!(+sh.recalled.placedCount>0||+sh.recalled.charmCount>0))return refuse('empty','This sheet has no pieces to move yet.');
      if(!canOpen())return refuse('empty','Press Rebuild to edit on this saved sheet first, then choose the partial sheet.');
    }else if(!piecesOf(sh).length)return refuse('empty','This sheet has no pieces to move yet.');
    if(!S.cloud||!S.cloud.ok)return refuse('offline','Reconnect to use a partial sheet.');
    return {ok:true};
  }
  // Rebuild to edit, the page's own (Recall.rebuild reads the saved record and the master files and changes nothing in the cloud); every piece must come back, or nothing moves
  async function openSaved(sh){
    const want=+(sh.recalled&&sh.recalled.placedCount)||0;
    await window.Recall.rebuild(sh,{cutOk:true});   // (cutOk: a saved sheet with a recorded cut that is not Completed may be opened for this move alone; Rebuild to edit still refuses it)
    const got=piecesOf(sh).length;
    if(sh.recalled||!got)throw new Error('This saved sheet could not be opened, so nothing was moved');
    if(got<want)throw new Error(`Only ${got} of the ${want} pieces of this saved sheet could be read back, so nothing was moved`);
  }

  /* ── the trial pack: the real solver, one-off worker, quick ── */
  const COARSE=Array.from({length:36},(_,i)=>i*10);
  function runTrial(sh,stock,charms,o){
    const maxMs=Math.max(1500,+o.maxMs||6000);
    const proxy=Object.create(sh);
    Object.assign(proxy,{charms,placements:[],rejects:[],roseStock:stock,roseProtected:null,rosePlan:null,appendOnly:false,nestInitial:null,feedWait:null,topup:null,intakePhase:null,intakeBudgetMs:null,nestFlow:'standard'});
    const job=C.buildJob(proxy);
    Object.assign(job,{careful:false,roomCheck:false,angles:COARSE,timeBudgetMs:maxMs,stallMs:Math.min(2000,maxMs),fullBudget:false,maxTrials:+o.maxTrials||80,protectedRose:null,lockedPlacements:null,initialLayout:null,seed:+o.seed||1});
    for(const p of job.pieces)p.pinned=null;
    return new Promise((resolve,reject)=>{
      const w=C.solverWorker?C.solverWorker():new Worker('charm-nest-worker.js'),id='partial-trial:'+Math.random().toString(36).slice(2);
      let done=false,t1=null,t2=null;
      const end=(fn,v)=>{if(done)return;done=true;clearTimeout(t1);clearTimeout(t2);try{w.terminate();}catch(_){}fn(v);};
      w.onmessage=e=>{const m=e.data||{};if(m.jobId&&m.jobId!==id)return;if(m.type==='done')end(resolve,m.result);else if(m.type==='error')end(reject,new Error(String(m.message||'The trial pack failed').split('\n')[0]));};
      w.onerror=e=>end(reject,new Error((e&&e.message)||'The trial pack failed'));
      // the solver ends by itself inside its budget; this only guards a worker that never answers (stop at the budget, give up a moment later)
      t1=setTimeout(()=>{try{w.postMessage({type:'stop',jobId:id});}catch(_){}},maxMs+2500);
      t2=setTimeout(()=>end(reject,new Error('The trial pack took too long')),maxMs+9000);
      w.postMessage({type:'solve',jobId:id,job});
    });
  }
  // A multi-piece order is never split across sheets: an order with a piece left out moves on whole (the page's keepOrdersWhole). `charms` must be EVERYTHING still
  // to place (the preview passes `rest`, not the trial's head): an order whose other piece was never tried is a piece left out too, so a pair cannot be split by the head's cut.
  function wholeOrders(charms,placedIds){
    const bad=new Set();for(const c of charms)if(!placedIds.has(c.id))bad.add(orderOf(c));
    return new Set(charms.filter(c=>placedIds.has(c.id)&&!bad.has(orderOf(c))).map(c=>c.id));
  }
  // the footprint a piece takes (with its spacing), for the auto order
  const footMm2=c=>{try{return C.inflatedArea(c)*MM*MM;}catch(_){return (+c.areaPt2||0)*MM*MM;}};
  // A quick trial only needs the pieces that could plausibly fit: the oldest ones, up to about one and a half times the partial's usable area.
  // The rest cannot fit it by area and simply continue. (A trial of every piece spends its few seconds on pieces that cannot go in.)
  function headFor(stock,charms,card){
    let usable=0;
    try{const Sv=window.CharmNestSolver;if(Sv&&Sv.makeSheetGrid)usable=Sv.makeSheetGrid({wPt:stock.wPt,hPt:stock.hPt,insetPt:+S.settings.insetPt||0,remnant:JSON.parse(stock.profileJson)},+S.settings.clearancePt||0,2).usableCells/4;}catch(_){}
    if(!(usable>0))usable=(card&&card.areaMm2?card.areaMm2:0)/(MM*MM);
    if(!(usable>0))return charms;
    // whole orders only: the cut falls between orders, never inside one (the pieces of a pair stay together; the list keeps its own order)
    const units=unitsOf(charms),take=new Set();let sum=0;
    for(const c of charms){const k=orderOf(c);if(take.has(k))continue;if(take.size&&sum>=usable*1.5)break;take.add(k);for(const m of units.get(k))sum+=footMm2(m)/(MM*MM);}
    return charms.filter(c=>take.has(orderOf(c)));
  }
  // the estimate of how many of THESE pieces the card holds (their own average footprint and sides stand in for the metal's typical piece)
  function estimateOf(card,pieces){
    try{
      const P=PP();
      if(P&&card&&card.outline&&P.estimateFit&&pieces.length){
        const area=pieces.reduce((n,c)=>n+footMm2(c),0)/Math.max(1,pieces.length),sides=pieces.map(c=>[(+c.w||0)/(c.scale||1)*MM,(+c.h||0)/(c.scale||1)*MM]).filter(s=>s[0]>0&&s[1]>0);
        const typical={areaMm2:area,...(sides.length?{minMm:sides.reduce((n,s)=>n+Math.min(...s),0)/sides.length,maxMm:sides.reduce((n,s)=>n+Math.max(...s),0)/sides.length}:{})};
        return P.estimateFit(card.outline,typical,{sheetWMm:card.sheetWMm,sheetHMm:card.sheetHMm});
      }
    }catch(_){}
    return null;
  }
  function capacityMm2(card,pieces){
    const e=estimateOf(card,pieces);if(e)return e.packMm2||0;
    const s=cardSize(card);return (card.areaMm2||s.wMm*s.hMm*.7)*.75;
  }
  // 'automatic' = best fit of the available list: the tightest partial that takes everything left, otherwise the biggest one, then again for the rest.
  // With pairs (an order of 2 or more pieces is never split): a partial whose estimated room cannot hold even the smallest such order is skipped, and "takes everything"
  // also needs room for the largest order whole.
  function bestFit(cards,pieces,taken){
    const need=pieces.reduce((n,c)=>n+footMm2(c),0),small=smallestUnit(pieces),large=largestUnit(pieces);
    let rows=cards.filter(c=>!taken.has(c.id)).map(c=>{const e=small>1||large>1?estimateOf(c,pieces):null;return {c,cap:e?e.packMm2||0:capacityMm2(c,pieces),e};}).filter(r=>r.cap>0);
    if(large>1)rows=rows.filter(r=>!r.e||r.e.high>=small);   // (a card that cannot hold the smallest order whole would only strand a piece of it)
    if(!rows.length)return null;
    const enough=rows.filter(r=>r.cap>=need&&(!r.e||r.e.pieces>=large)).sort((a,b)=>a.cap-b.cap);
    return (enough[0]||rows.sort((a,b)=>b.cap-a.cap)[0]).c;
  }

  function words(pieces,links,left,next,sh,pw){
    const n=pieces,all=pw&&pw!==`${n} ${plural(n,'piece')}`?pw:null;   // (pw: "8 pieces (3 pairs and 2 single pieces)", only when something is in a group)
    if(!links.length)return `None of the ${all||`${n} ${plural(n,'piece')}`} fit${n===1?'s':''} on ${next.listed?'the partial sheet'+(next.listed>1?'s':''):'a partial sheet'} you chose.`;
    if(!left){
      if(links.length===1)return n===1?'The piece fits on this partial sheet.':`All ${all||`${n} pieces`} fit on this partial sheet.`;
      return `All ${all||`${n} pieces`} fit on ${links.length} partial sheets: ${links.map(l=>`${l.placed} on ${l.label}`).join(', ')}.`;
    }
    const placed=n-left,first=links.length===1?`${placed} of ${n} ${plural(n,'piece')} fit${placed===1?'s':''} on ${links[0].label}`:`${placed} of ${n} pieces fit on ${links.length} partial sheets (${links.map(l=>`${l.placed} on ${l.label}`).join(', ')})`;
    const rest=`the other ${left} continue${left===1?'s':''}`;
    if(next.kind==='partial')return `${first}; ${rest} on the next partial sheet that fits best.`;
    return `${first}; ${rest} on a new ${metalWord(sh.metal)} sheet${next.size?` (${next.size})`:''}.`;
  }

  /* preview(sh, partialIds, {onStep, maxMs, maxLinks}) -> the answer the panel shows. Reads only; writes and claims nothing. */
  async function preview(sh,ids,o={}){
    o=o||{};const step=(key,text,more)=>{try{o.onStep&&o.onStep({key,text,...(more||{})});}catch(_){}};
    const can=canSeat(sh);if(!can.ok)return {ok:false,code:can.code,why:can.why,metal:sh&&sh.metal,sheetId:sh&&sh.sheetId||null,pieces:sh&&sh.metal?piecesOf(sh).length:0,links:[],fitsAll:false,words:can.why};
    step('check','Checking the sheet…');
    const refusal=(code,why)=>({...refuse(code,why),metal:sh.metal,sheetId:sh.sheetId||null,pieces:piecesOf(sh).length,links:[],fitsAll:false,words:why});
    try{
      if(ids&&ids.length&&ids[0]===partialOf(sh))return refusal('same','This sheet already sits on that partial sheet.');   // (the partial a sheet holds is in use: it is not on the available list)
      if(unopened(sh)){step('open','Opening the sheet from its saved set…');await openSaved(sh);}
      const metal=sh.metal,all=piecesOf(sh),pol=policy(metal),list=await cardsOf(metal);
      let order=[];
      for(const id of ids||[]){const c=list.find(x=>x.id===id);if(!c)return refusal('gone','That partial sheet is not available any more. Choose another.');order.push(c);}
      const listed=order.length,maxLinks=Math.max(1,+o.maxLinks||4);
      if(!listed&&pol.mode==='new')return refusal('choose','This metal makes a new sheet by itself. Choose a partial sheet to use one.');
      let rest=all.slice(),links=[],skipped=[],taken=new Set(order.map(c=>c.id)),auto=!listed,splits=[];
      for(let k=0;rest.length&&k<maxLinks;k++){
        let card=order[k];
        if(!card){   // beyond the person's own list: automatic = best fit from the metal's available list; 'new' stops here
          if(pol.mode==='new')break;
          card=bestFit(list,rest,taken);if(!card)break;taken.add(card.id);order.push(card);
        }
        if(k===0&&partialOf(sh)===card.id)return refusal('same','This sheet already sits on that partial sheet.');
        const lab='Partial '+(links.length+skipped.length+1);
        step('trial',`Trying the pieces on ${lab}…`,{n:k+1,partialId:card.id});
        const stock=await stockOf(card),head=headFor(stock,rest,card),r=await runTrial(sh,stock,head,{maxMs:o.maxMs,seed:k+1});
        const trialIds=new Set((r.placements||[]).map(p=>p.id)),placedIds=wholeOrders(rest,trialIds);   // (against everything still to place: an order is whole only when every piece of it was placed)
        const size=cardSize(card),cut=splitsOf(head,trialIds);   // (cut: orders this partial could take only part of, so they stay whole and move on)
        for(const x of cut)if(!splits.some(y=>y.order===x.order))splits.push(x);
        if(!placedIds.size){skipped.push({partialId:card.id,why:cut.length?splitWords(cut):'No piece fits on it.',...(cut.length?{splits:cut}:{})});continue;}
        links.push({n:links.length+1,partialId:card.id,stockId:stock.id,label:lab,wMm:mm1(size.wMm),hMm:mm1(size.hMm),areaMm2:card.areaMm2!=null?Math.round(card.areaMm2):null,placed:placedIds.size,pieceIds:rest.filter(c=>placedIds.has(c.id)).map(c=>c.id),placements:(r.placements||[]).filter(p=>placedIds.has(p.id)).map(p=>({id:p.id,cxPt:p.cxPt,cyPt:p.cyPt,angle:p.angle})),densityPct:Math.round(100*(r.density||0)),filled:rest.length>placedIds.size||(r.density||0)>=(+S.settings.maxFill||.8)-.02,outline:card.outline||null});
        rest=rest.filter(c=>!placedIds.has(c.id));
      }
      const left=rest.length;
      let nextInfo={kind:'none',listed};
      if(left){
        const more=pol.mode==='auto'?bestFit(list,rest,taken):null,sz=pol.mode==='new'?`${mm1(pol.wMm)} x ${mm1(pol.hMm)} mm`:'';
        nextInfo=more?{kind:'partial',listed,partialId:more.id}:{kind:'new',listed,size:sz};
      }
      const pw=pieceWords(all),shared=sharedOf(sh,all),sw=splitWords(splits),hw=sharedWords(shared);
      const out={ok:true,metal,sheetId:sh.sheetId||null,pieces:all.length,fitsAll:!left,links,skipped,
        continues:{n:left,ids:rest.map(c=>c.id),next:left?(nextInfo.kind==='partial'?'partial':'new'):'none',...(nextInfo.partialId?{nextPartialId:nextInfo.partialId}:{})},
        words:words(all.length,links,left,nextInfo,sh,pw),
        note:'A quick trial pack on the exact leftover outline. The nest itself can place a piece more or less.',auto};
      // pairs (R3): what the partials could hold only part of, and the orders of this sheet that already have pieces on another sheet, said plainly
      if(splits.length){out.splits=splits;out.splitWords=sw;if(!links.length)out.words+=' '+sw;}
      if(shared.length){out.shared=shared;out.sharedWords=hw;}
      out.continues.words=left?out.words.replace(/^[^;]*; /,''):'';
      // a sheet with a recorded cut (not Completed) is told what the move does to it, before it is pressed
      if(cutOf(sh)){
        const own=sh.roseStock&&sh.roseStock.id&&sh.roseStock.revision>0?sh.roseStock.id+'-'+sh.roseStock.revision:null;
        out.words+=own&&order[0]&&order[0].id===own?' This is the leftover its own cut made: the sheet takes it back, the recorded cut stays in the history, and the sheet needs Cut Sheet again.':' Its recorded cut is set aside (it stays in the history): the leftover that cut made is no longer offered as free metal, and the sheet needs Cut Sheet again.';
      }
      seen.set(sigOf(sh,ids),out);if(seen.size>20)seen.delete(seen.keys().next().value);
      step('done',out.words);return out;
    }catch(e){return refusal('failed','The trial pack could not run: '+(e&&e.message||e));}
  }

  /* A sheet in a set stays in it while its pieces are nested again here: the mark the sheet window and an order's release already put on a sheet they rewrite
     in place (Gate.keep: Gate.assemble does not take the sheet out of its set meanwhile, the nest keeps releaseFull, no arrival goes onto it). It is let go once
     the sheet is at rest again, as charm-nest-order-release.js does, or by itself after ten minutes (Gate.holding). The sheet's record, files, QR label and
     timeline are made again by the nest's own save, exactly as when pieces are added or taken off. */
  function keepInSet(sh){
    const G=window.Gate;if(!G||!G.keep||!(sh.releaseFull||inSet(sh)))return;
    try{G.keep(sh);}catch(_){return;}
    const mark=sh.keepRelease;if(!mark)return;
    const t0=Date.now(),atRest=()=>!['nesting','finishing','queued'].includes(sh.status)&&!sh._operationStarting&&!sh._partialBusy&&sh.persistedDone!==false&&!(sh.persisted&&!sh.persistedDone);
    const tick=()=>{if(sh.keepRelease!==mark)return;if(atRest()||Date.now()-t0>600000){delete sh.keepRelease;return;}setTimeout(tick,2000);};
    setTimeout(tick,2000);
  }

  /* seat(sh, partialIds, {onStep, preview}) -> the commit. The sheet's pieces are nested again, from scratch, on the first partial; the rest follow
     through the page's own overflow. Resolves when the sheet's nest has STARTED (the nest then runs like any by-hand nest; the chain grows as it ends). */
  async function seat(sh,ids,o={}){
    o=o||{};const step=(key,text,more)=>{try{o.onStep&&o.onStep({key,text,...(more||{})});}catch(_){}};
    const can=canSeat(sh);if(!can.ok){step('failed',can.why);return can;}
    const metal=sh.metal;let pieces=piecesOf(sh);
    const fail=(code,why)=>{step('failed',why);sh._partialBusy=false;return refuse(code,why);};
    sh._partialBusy=true;
    try{
      step('check','Checking the sheet…');
      // a sheet opened from a saved set: the physical sheet its record names is read before the page lets go of the record (Recall.rebuild clears it)
      const savedStock=sh.recalled&&sh.recalled.roseStockId||null,wasCut=cutOf(sh);   // (wasCut: a recorded cut that is not Completed; the move sets it aside, see the head of this file)
      if(unopened(sh)){step('open','Opening the sheet from its saved set…');await openSaved(sh);pieces=piecesOf(sh);}
      if(ids&&ids.length&&ids[0]===partialOf(sh))return fail('same','This sheet already sits on that partial sheet.');
      const list=await cardsOf(metal),pol=policy(metal);
      const asked=ids||[],shown=o.preview&&o.preview.ok?o.preview:seen.get(sigOf(sh,asked))||null;
      // what was previewed is what is done: the partials of its chain, in its order (the person's own first, then the ones the trial added)
      const planned=shown&&shown.links&&shown.links.length&&shown.links[0].partialId===asked[0]?shown.links.map(l=>l.partialId):asked;
      const order=[];for(const id of planned){const c=list.find(x=>x.id===id);if(!c){if(asked.includes(id))return fail('gone','That partial sheet is not available any more. Choose another.');continue;}order.push(c);}
      if(!order.length){
        if(pol.mode==='new')return fail('choose','This metal makes a new sheet by itself. Choose a partial sheet to use one.');
        const c=bestFit(list,pieces,new Set());
        if(!c)return fail('none',list.length&&smallestUnit(pieces)>1?`No partial sheet available for this metal can hold even one whole order of this sheet (every order has ${smallestUnit(pieces)} or more pieces that are never split across sheets).`:'There is no partial sheet available for this metal.');
        order.push(c);
      }
      const first=order[0];
      if(partialOf(sh)===first.id)return fail('same','This sheet already sits on that partial sheet.');
      // the pair rule, before anything is claimed: a partial whose most generous estimate holds fewer pieces than the smallest order still to place cannot take even one order
      // whole, and an order is never split across sheets (a refusal here is certain: the estimate's upper bound is the generous one)
      {const unit=smallestUnit(pieces),e=unit>1?estimateOf(first,pieces):null;
        if(e&&e.high<unit)return fail('pairs',`This partial sheet can hold about ${e.high} ${plural(e.high,'piece')} at most, and every order on this sheet has ${unit} or more pieces that are never split across sheets. Choose a bigger partial sheet.`);}
      const chain=[];for(const c of order.slice(1))chain.push({id:c.id,stock:await stockOf(c)});
      // (what the sheet is in, before it moves: for the history line below)
      const sets=(window.Sets&&window.Sets.ofRun&&window.Sets.ofRun(sh.runId))||[],committed=sets.some(x=>x.committedAt&&(x.sheetIds||[]).includes(sh.sheetId)),inCurrent=inSet(sh)&&!committed;
      const approved=!!(sh.processReady||sets.some(x=>x.processReady&&(x.sheetIds||[]).includes(sh.sheetId)));   // (the sheet's or its set's ready seal: its layout is checked, and sealed, again after the nest)
      const stock={...await stockOf(first),partialId:first.id},held=(sh.roseStock&&sh.roseStock.id)||savedStock,RS=window.RoseStock;
      // 1. ONE transaction on the server: the physical sheet this sheet holds goes back (a fresh uncut gold sheet is deleted, a leftover returns to the
      // list) and the chosen partial is reserved for it. Another sheet may have taken it a moment ago: the claim is then refused and nothing changed.
      step('claim',wasCut?'Setting the recorded cut aside and reserving the partial sheet…':held?'Giving the old sheet back and reserving the partial sheet…':'Reserving the partial sheet…');
      // (swap also when the sheet holds a saved green line that was never cut, or a recorded cut that is not Completed: the server drops that line / sets that cut aside with the old
      // seat, seatOn drops this page's copy)
      let moved=null;
      try{await RS.seatOn(sh,stock,{swap:!!(held||wasCut||sh.rosePlan||sh.roseProtected),aside:x=>{moved=x||null;}});}
      catch(e){
        window.PartialSheets&&window.PartialSheets.changed&&window.PartialSheets.changed();
        return fail('taken','That partial sheet could not be reserved: '+e.message);
      }
      window.PartialSheets&&window.PartialSheets.changed&&window.PartialSheets.changed();
      // 2. nest everything again, from scratch, on the leftover's outline (pins go, as with Apply size)
      step('nest','Nesting the pieces again…');
      for(const c of sh.charms){c.pinned=null;delete c.arrivalPin;}
      Object.assign(sh,{feedWait:null,best:null,bestKey:null,nestInitial:null,_beforeNest:null,_partialId:first.id,_partialChain:chain,_partialNext:null});
      keepInSet(sh);   // (before sheetDirty, which lets go of releaseFull: the mark remembers it)
      C.sheetDirty(sh);
      sh._byHand=true;
      C.startNest(sh);
      const sz=cardSize(first);
      try{C.agent({metal},'nest',`Partial sheet: ${label(sh)} moved onto a partial sheet of ${mm1(sz.wMm)} x ${mm1(sz.hMm)} mm${chain.length?`, with ${chain.length} more partial ${plural(chain.length,'sheet')} to take what does not fit`:''}; its ${pieceWords(pieces)} are nested again${committed?'; it stays in its committed set':inCurrent?'; it stays in its set':''}${approved?'; it was approved for Laser cutting, so its new layout is checked and approved again':''}${!wasCut&&held&&held!==stock.id?'; the sheet it sat on went back to the list':''}${wasCut?`; its recorded cut was set aside (the cut and its history stay), so it needs Cut Sheet again${moved&&moved.how==='claimed'?'; it sits on the leftover that cut made':moved&&moved.how==='discarded'?'; the leftover that cut made is no longer free metal and shows Discarded':''}`:''}`);}catch(_){}
      emit({type:'seated',metal,sheetId:sh.sheetId||null});
      step('done',`${label(sh)} is nesting on the partial sheet.`);
      sh._partialBusy=false;
      seen.delete(sigOf(sh,asked));
      const pv=shown;
      return {ok:true,started:true,sheets:[sh],links:pv?pv.links:[],moved:pv?Math.max(0,pieces.length-pv.continues.n):pieces.length,continues:pv?pv.continues.n:null};   // (moved = what the previewed partials hold; the panel says the rest continue)
    }catch(e){return fail('failed',e&&e.message||String(e));}
  }
  const useOn=(sh,id,o)=>seat(sh,[id],o);

  /* the page's own overflow asks for the next sheet (charm-nest-1.html overflowToNextSheet): a page made for the next partial of the chain.
     Returns that page, or null (the page then does what it does today). Synchronous: the claim happens when that page nests (nestClaim). */
  function nextPage(sh,moving){
    try{
      if(!sh||!sh._partialId||!cuts(sh.metal))return null;
      const prim=S.sheets[sh.metal];if(!prim)return null;
      // incoming pieces and later overflows keep filling the chain's sheets in order: the first one still open takes them; only when every sheet made so
      // far is closed (cut, sent, finished) does the chain's next partial get its page
      const open=p=>!p.roseCutAt&&!p.recalled&&!p.laserDoneAt&&!p.releaseFull&&!p.intakeFinalized;
      let tail=sh;
      for(let guard=0;guard<1000;guard++){const nx=tail._partialNext;if(!nx||!prim.pages.includes(nx))break;if(open(nx))return nx;tail=nx;}
      let step=tail._partialChain&&tail._partialChain.shift();
      if(!step&&policy(sh.metal).mode==='auto'){   // the listed partials are used up: automatic takes the best fit of the others, if the list is known
        const cards=(cache[sh.metal]&&cache[sh.metal].items)||[],taken=new Set(prim.pages.map(partialOf).filter(Boolean)),c=cards.length&&moving&&moving.length?bestFit(cards,moving,taken):null;
        // (the page is made with the partial's id and size; the claim at its nest start brings the stock with its profile, which is what the solver packs against)
        const wPt=c&&(c.wPt||(c.sheetWMm?c.sheetWMm/MM:0)),hPt=c&&(c.hPt||(c.sheetHMm?c.sheetHMm/MM:0));
        if(c&&wPt>0&&hPt>0)step={id:c.id,stock:{id:c.stockId||c.id,metal:c.metal||sh.metal,wPt,hPt,revision:c.revision||0,profileJson:c.profileJson||null}};
      }
      if(!step)return null;
      const pg=C.addPage(sh.metal);
      pg.roseStock={...step.stock};pg._partialId=step.id;pg._partialPending=true;pg._partialChain=tail._partialChain||[];pg.sheetId=pg.sheetId||`${sh.metal}-s${pg.page}-${Date.now().toString(36)}`;
      tail._partialNext=pg;
      emit({type:'continued',metal:sh.metal,sheetId:pg.sheetId});
      try{C.agent({metal:sh.metal},'nest',`Partial sheet: ${label(sh)} is full, the rest of its pieces continue on ${label(pg)} (the next partial sheet)`);}catch(_){}
      return pg;
    }catch(e){console.warn('partial sheets: next page',e);return null;}
  }
  // nestClaim hooks (charm-nest-rose-ui.js): a chain page claims exactly its partial; when another sheet took it meanwhile the page uses the metal's own rule
  const pending=sh=>!!sh&&sh._partialPending===true;
  const claimed=sh=>{if(sh){sh._partialPending=false;emit({type:'claimed',metal:sh.metal,sheetId:sh.sheetId||null});}};
  function lost(sh,e){
    if(!sh)return;
    sh._partialPending=false;const was=sh._partialId;sh._partialId=null;delete sh.roseStock;delete sh.roseRevision;
    try{C.toast(`${label(sh)}: the partial sheet was taken by another sheet (${e&&e.message||'reserved'}). Using the next one the nester finds.`,'bad',7000);}catch(_){}
    emit({type:'released',metal:sh.metal,sheetId:sh.sheetId||null,partialId:was});
  }

  /* the sheets of the metal that sit on a partial, in page order (for the chain strip) */
  function chain(sh){
    if(!sh||!sh.metal)return [];
    return pagesOf(sh.metal).filter(p=>p.roseStock&&p.roseStock.id&&(p._partialId||p.roseStock.revision>0||p.roseStock.profileJson||p.roseStock.made)).map(p=>({
      sheet:p,sheetId:p.sheetId||null,label:label(p),page:p.page,partialId:partialOf(p),stockId:p.roseStock.id,
      wMm:mm1(p.roseStock.wPt*MM),hMm:mm1(p.roseStock.hPt*MM),placed:(p.placements||[]).length,waiting:Math.max(0,piecesOf(p).length-(p.placements||[]).length),
      state:p.roseCutAt?'cut':(p.movedOn||p.releaseFull)?'full':'open'}));
  }
  const on=fn=>{if(typeof fn!=='function')return ()=>{};listeners.add(fn);return ()=>listeners.delete(fn);};

  // Cut Sheet, said after the cut: an order of this sheet with pieces on another sheet (a pair split across two sheets, R3). '' when none.
  function cutNote(sh){
    const list=sharedOf(sh,piecesOf(sh));
    if(!list.length)return '';
    if(list.length===1){const o=list[0];return `Order ${o.order}: ${o.here} ${plural(o.here,'piece')} of it ${o.here===1?'is':'are'} cut on this sheet, the other ${o.elsewhere} ${o.elsewhere===1?'is':'are'} on ${o.sheets.join(' and ')}.`;}
    const o=list[0];return `${list.length} orders on this sheet have pieces on other sheets (for example order ${o.order} on ${o.sheets.join(' and ')}).`;
  }
  window.PartialNest={cutNote,canSeat,preview,seat,useOn,chain,partialOf,on,nextPage,pending,claimed,lost,mode,newSheet,policy,
    _cards:cardsOf,_bestFit:bestFit,_wholeOrders:wholeOrders,_headFor:headFor,_splitsOf:splitsOf,_sharedOf:sharedOf,_estimateOf:estimateOf};

  /* window.PartialEngine: the shape the Partial Sheet panel (charm-nest-partial-ui.js) reads (contract.md, PS1's section). A thin face on the functions above:
     preview(sheet, partialId) -> { ok, pieces, fitsAll, fits (on THIS partial), rest, chain:[{partialId,name,wMm,hMm,areaMm2,fits}], then:'new'|'wait'|null, note, words }
       | { ok:false, reason }          useOn(sheet, partialId, {onStep}) -> { ok, used:[partialId...], placed, rest } | { ok:false, error }
     chain(metal) -> [{ n, partialId, name, wMm, hMm, areaMm2, pieces, sheetPage, full }]       on(fn) -> fn({metal}) when the chain changes
     The commit uses the chain the person was just shown (the last preview of the same sheet, partial and pieces), so what happens is what was previewed. */
  const nameOf=c=>c?([c.sourceSheet,c.sourceSet].filter(Boolean).join(' · ')||c.id):'';
  const cardOf=(metal,id)=>((cache[metal]&&cache[metal].items)||[]).find(c=>c.id===id)||null;
  const engine={
    async preview(sh,partialId){
      const ids=(Array.isArray(partialId)?partialId:partialId?[partialId]:[]).filter(Boolean),pv=await preview(sh,ids);
      if(!pv||!pv.ok)return {ok:false,reason:(pv&&pv.why)||'This sheet cannot move to a partial sheet.',code:pv&&pv.code};
      const chainRows=pv.links.map(l=>{const c=cardOf(sh.metal,l.partialId);return {partialId:l.partialId,name:nameOf(c)||l.label,wMm:l.wMm,hMm:l.hMm,areaMm2:l.areaMm2,fits:l.placed};});
      const own=ids.length?pv.links.find(l=>l.partialId===ids[0]):pv.links[0],fits=own&&pv.links[0]===own?own.placed:0;   // (a partial that takes nothing is not in the links: 0 fit on it)
      return {ok:true,pieces:pv.pieces,fitsAll:pv.fitsAll,fits,rest:pv.pieces-fits,chain:chainRows,then:pv.continues.n?(pv.continues.next==='partial'?'wait':'new'):null,note:pv.note,words:pv.words,...(pv.splitWords?{splitWords:pv.splitWords}:{}),...(pv.sharedWords?{sharedWords:pv.sharedWords}:{})};
    },
    async useOn(sh,partialId,o={}){
      const ids=(Array.isArray(partialId)?partialId:[partialId]).filter(Boolean),pv=seen.get(sigOf(sh,ids))||null;
      const order=pv&&pv.links.length&&pv.links[0].partialId===ids[0]?pv.links.map(l=>l.partialId):ids;   // the chain as it was previewed, the person's pick first
      const map={check:'start',claim:'claimed',nest:'nesting',done:'done'};
      const r=await seat(sh,order,{preview:pv,onStep:s=>{try{o.onStep&&o.onStep({key:map[s.key]||s.key,sheet:sh,partialId:ids[0],text:s.text});}catch(_){}}});
      if(!r.ok)return {ok:false,error:r.why,code:r.code};
      seen.delete(sigOf(sh,ids));
      const first=pv&&pv.links[0]?pv.links[0].placed:null;
      return {ok:true,used:order,placed:first,rest:first==null?null:piecesOf(sh).length-first};
    },
    chain(metal){
      const pg=pagesOf(metal)[0];if(!pg)return [];
      return chain(pg).map((r,i)=>{const c=cardOf(metal,r.partialId),sz=c?cardSize(c):null;return {n:i+1,partialId:r.partialId,name:nameOf(c)||r.label,wMm:sz?mm1(sz.wMm):r.wMm,hMm:sz?mm1(sz.hMm):r.hMm,areaMm2:c&&c.areaMm2!=null?Math.round(c.areaMm2):null,pieces:r.placed,sheetPage:r.page,full:r.state!=='open'};});
    },
    on:fn=>on(e=>fn({metal:e&&e.metal}))
  };
  window.PartialEngine=engine;
})();
