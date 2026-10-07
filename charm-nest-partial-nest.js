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
   A recorded cut is permanent: a sheet with one keeps its stock and cannot be seated again (canSeat says so in words). */
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
  async function stockOf(card){
    if(card.profileJson&&card.wPt&&card.hPt)return {id:card.stockId||card.id,metal:card.metal,wPt:card.wPt,hPt:card.hPt,revision:card.revision||0,profileJson:card.profileJson};
    const k=stockKey(card);if(stocks.has(k))return stocks.get(k);
    // PS3's page cache of the physical sheets under partials (ONE op for several ids, a revision's profile never changes)
    try{
      const PS=window.PartialSheets;
      if(PS&&PS.stocks){const r=(await PS.stocks([card.id]))[card.id];
        if(r&&r.profileJson&&r.current!==false){const out={id:r.stockId,metal:card.metal||r.metal,wPt:r.wPt,hPt:r.hPt,revision:r.revision||0,profileJson:r.profileJson};stocks.set(k,out);return out;}}
    }catch(_){}
    const r=await C.api('charmNestLibrary',{op:'roseGet',stockId:card.stockId||card.id,metal:card.metal,noCuts:true},{quiet:true});   // ONE document read: the physical sheet's size and profile, not its cuts
    const s=r&&r.stock;if(!s||!s.profileJson)throw new Error('This partial sheet could not be read');
    const out={id:s.id,metal:card.metal,wPt:s.wPt,hPt:s.hPt,revision:s.revision||0,profileJson:s.profileJson};stocks.set(k,out);return out;
  }
  const cardSize=c=>({wMm:c.wMm!=null?c.wMm:c.bboxMm?c.bboxMm.w:c.sheetWMm||0,hMm:c.hMm!=null?c.hMm:c.bboxMm?c.bboxMm.h:c.sheetHMm||0});
  function partialOf(sh){
    if(!sh)return null;
    if(sh._partialId)return sh._partialId;
    const s=sh.roseStock;return s&&s.id&&s.revision>0?s.id+'-'+s.revision:null;
  }

  /* ── what can be seated ── */
  function canSeat(sh){
    if(!sh||!sh.metal||!cuts(sh.metal))return refuse('metal','Partial sheets are for Rose Gold, 10K Gold and 14K Gold sheets.');
    if(sh.roseCutAt)return refuse('cut','This sheet has a recorded cut. A recorded cut is permanent, so it keeps its metal and cannot move to another partial sheet. A partial sheet can be used on a new sheet.');
    if(sh.recalled)return refuse('recalled','This sheet was opened from a saved set. Its layout is kept as it was saved, so its pieces cannot move to another partial sheet.');
    const sets=(window.Sets&&window.Sets.ofRun&&window.Sets.ofRun(sh.runId))||[];
    if(+sh.laserDoneAt>0||sh.releaseFull||(window.Gate&&window.Gate.holding&&window.Gate.holding(sh)))return refuse('laser','This sheet has gone on to Laser cutting. Its layout is kept as it is, so it cannot move to another partial sheet.');
    if(sets.some(s=>s.committedAt&&(s.sheetIds||[]).includes(sh.sheetId)))return refuse('set','This sheet belongs to a committed set. Its layout is kept as it is, so it cannot move to another partial sheet.');
    if(sh.rosePlan||sh.roseProtected)return refuse('line','This sheet already has a green cut line saved, so its pieces stay where they are. A partial sheet can be used on a new sheet.');
    if(['nesting','finishing','queued'].includes(sh.status)||sh._operationStarting||sh._partialBusy||(sh.persisted&&!sh.persistedDone))return refuse('busy','Wait until this sheet has finished nesting and saving, then choose the partial sheet.');
    if(inSet(sh)&&sh.roseStock)return refuse('set',`${label(sh)} is in the current set. Take it out of the set first (Options, Include in current set), then choose the partial sheet.`);
    if(!piecesOf(sh).length)return refuse('empty','This sheet has no pieces to move yet.');
    if(!S.cloud||!S.cloud.ok)return refuse('offline','Reconnect to use a partial sheet.');
    return {ok:true};
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
  // A multi-piece order is never split across sheets: an order with a piece left out moves on whole (the page's keepOrdersWhole).
  function wholeOrders(charms,placedIds){
    const bad=new Set();for(const c of charms)if(!placedIds.has(c.id))bad.add(c.order||c.id);
    return new Set(charms.filter(c=>placedIds.has(c.id)&&!bad.has(c.order||c.id)).map(c=>c.id));
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
    const out=[];let sum=0;
    for(const c of charms){if(out.length&&sum>=usable*1.5)break;out.push(c);sum+=footMm2(c)/(MM*MM);}
    return out;
  }
  function capacityMm2(card,pieces){
    try{
      const P=window.CharmNestPartial;
      if(P&&card.outline&&P.estimateFit){
        const area=pieces.reduce((n,c)=>n+footMm2(c),0)/Math.max(1,pieces.length),sides=pieces.map(c=>[(+c.w||0)/(c.scale||1)*MM,(+c.h||0)/(c.scale||1)*MM]).filter(s=>s[0]>0&&s[1]>0);
        const typical={areaMm2:area,...(sides.length?{minMm:sides.reduce((n,s)=>n+Math.min(...s),0)/sides.length,maxMm:sides.reduce((n,s)=>n+Math.max(...s),0)/sides.length}:{})};
        return P.estimateFit(card.outline,typical,{sheetWMm:card.sheetWMm,sheetHMm:card.sheetHMm}).packMm2||0;
      }
    }catch(_){}
    const s=cardSize(card);return (card.areaMm2||s.wMm*s.hMm*.7)*.65;
  }
  // 'automatic' = best fit of the available list: the tightest partial that takes everything left, otherwise the biggest one, then again for the rest
  function bestFit(cards,pieces,taken){
    const need=pieces.reduce((n,c)=>n+footMm2(c),0),rows=cards.filter(c=>!taken.has(c.id)).map(c=>({c,cap:capacityMm2(c,pieces)})).filter(r=>r.cap>0);
    if(!rows.length)return null;
    const enough=rows.filter(r=>r.cap>=need).sort((a,b)=>a.cap-b.cap);
    return (enough[0]||rows.sort((a,b)=>b.cap-a.cap)[0]).c;
  }

  function words(pieces,links,left,next,sh){
    const n=pieces;
    if(!links.length)return `None of the ${n} ${plural(n,'piece')} fit${n===1?'s':''} on ${next.listed?'the partial sheet'+(next.listed>1?'s':''):'a partial sheet'} you chose.`;
    if(!left){
      if(links.length===1)return n===1?'The piece fits on this partial sheet.':`All ${n} pieces fit on this partial sheet.`;
      return `All ${n} pieces fit on ${links.length} partial sheets: ${links.map(l=>`${l.placed} on ${l.label}`).join(', ')}.`;
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
      const metal=sh.metal,all=piecesOf(sh),pol=policy(metal),list=await cardsOf(metal);
      let order=[];
      for(const id of ids||[]){const c=list.find(x=>x.id===id);if(!c)return refusal('gone','That partial sheet is not available any more. Choose another.');order.push(c);}
      const listed=order.length,maxLinks=Math.max(1,+o.maxLinks||4);
      if(!listed&&pol.mode==='new')return refusal('choose','This metal makes a new sheet by itself. Choose a partial sheet to use one.');
      let rest=all.slice(),links=[],skipped=[],taken=new Set(order.map(c=>c.id)),auto=!listed;
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
        const placedIds=wholeOrders(head,new Set((r.placements||[]).map(p=>p.id)));
        const size=cardSize(card);
        if(!placedIds.size){skipped.push({partialId:card.id,why:'No piece fits on it.'});continue;}
        links.push({n:links.length+1,partialId:card.id,stockId:stock.id,label:lab,wMm:mm1(size.wMm),hMm:mm1(size.hMm),areaMm2:card.areaMm2!=null?Math.round(card.areaMm2):null,placed:placedIds.size,pieceIds:rest.filter(c=>placedIds.has(c.id)).map(c=>c.id),placements:(r.placements||[]).filter(p=>placedIds.has(p.id)).map(p=>({id:p.id,cxPt:p.cxPt,cyPt:p.cyPt,angle:p.angle})),densityPct:Math.round(100*(r.density||0)),filled:rest.length>placedIds.size||(r.density||0)>=(+S.settings.maxFill||.8)-.02,outline:card.outline||null});
        rest=rest.filter(c=>!placedIds.has(c.id));
      }
      const left=rest.length;
      let nextInfo={kind:'none',listed};
      if(left){
        const more=pol.mode==='auto'?bestFit(list,rest,taken):null,sz=pol.mode==='new'?`${mm1(pol.wMm)} x ${mm1(pol.hMm)} mm`:'';
        nextInfo=more?{kind:'partial',listed,partialId:more.id}:{kind:'new',listed,size:sz};
      }
      const out={ok:true,metal,sheetId:sh.sheetId||null,pieces:all.length,fitsAll:!left,links,skipped,
        continues:{n:left,ids:rest.map(c=>c.id),next:left?(nextInfo.kind==='partial'?'partial':'new'):'none',...(nextInfo.partialId?{nextPartialId:nextInfo.partialId}:{})},
        words:words(all.length,links,left,nextInfo,sh),
        note:'A quick trial pack on the exact leftover outline. The nest itself can place a piece more or less.',auto};
      out.continues.words=left?out.words.replace(/^[^;]*; /,''):'';
      seen.set(sigOf(sh,ids),out);if(seen.size>20)seen.delete(seen.keys().next().value);
      step('done',out.words);return out;
    }catch(e){return refusal('failed','The trial pack could not run: '+(e&&e.message||e));}
  }

  /* seat(sh, partialIds, {onStep, preview}) -> the commit. The sheet's pieces are nested again, from scratch, on the first partial; the rest follow
     through the page's own overflow. Resolves when the sheet's nest has STARTED (the nest then runs like any by-hand nest; the chain grows as it ends). */
  async function seat(sh,ids,o={}){
    o=o||{};const step=(key,text,more)=>{try{o.onStep&&o.onStep({key,text,...(more||{})});}catch(_){}};
    const can=canSeat(sh);if(!can.ok){step('failed',can.why);return can;}
    const metal=sh.metal,pieces=piecesOf(sh);
    const fail=(code,why)=>{step('failed',why);sh._partialBusy=false;return refuse(code,why);};
    sh._partialBusy=true;
    try{
      step('check','Checking the sheet…');
      if(ids&&ids.length&&ids[0]===partialOf(sh))return fail('same','This sheet already sits on that partial sheet.');
      const list=await cardsOf(metal),pol=policy(metal);
      const asked=ids||[],shown=o.preview&&o.preview.ok?o.preview:seen.get(sigOf(sh,asked))||null;
      // what was previewed is what is done: the partials of its chain, in its order (the person's own first, then the ones the trial added)
      const planned=shown&&shown.links&&shown.links.length&&shown.links[0].partialId===asked[0]?shown.links.map(l=>l.partialId):asked;
      const order=[];for(const id of planned){const c=list.find(x=>x.id===id);if(!c){if(asked.includes(id))return fail('gone','That partial sheet is not available any more. Choose another.');continue;}order.push(c);}
      if(!order.length){
        if(pol.mode==='new')return fail('choose','This metal makes a new sheet by itself. Choose a partial sheet to use one.');
        const c=bestFit(list,pieces,new Set());if(!c)return fail('none','There is no partial sheet available for this metal.');order.push(c);
      }
      const first=order[0];
      if(partialOf(sh)===first.id)return fail('same','This sheet already sits on that partial sheet.');
      const chain=[];for(const c of order.slice(1))chain.push({id:c.id,stock:await stockOf(c)});
      const stock={...await stockOf(first),partialId:first.id},held=sh.roseStock&&sh.roseStock.id,RS=window.RoseStock;
      // 1. ONE transaction on the server: the physical sheet this sheet holds goes back (a fresh uncut gold sheet is deleted, a leftover returns to the
      // list) and the chosen partial is reserved for it. Another sheet may have taken it a moment ago: the claim is then refused and nothing changed.
      step('claim',held?'Giving the old sheet back and reserving the partial sheet…':'Reserving the partial sheet…');
      try{await RS.seatOn(sh,stock,{swap:!!held});}
      catch(e){
        window.PartialSheets&&window.PartialSheets.changed&&window.PartialSheets.changed();
        return fail('taken','That partial sheet could not be reserved: '+e.message);
      }
      window.PartialSheets&&window.PartialSheets.changed&&window.PartialSheets.changed();
      // 2. nest everything again, from scratch, on the leftover's outline (pins go, as with Apply size)
      step('nest','Nesting the pieces again…');
      for(const c of sh.charms){c.pinned=null;delete c.arrivalPin;}
      Object.assign(sh,{feedWait:null,best:null,bestKey:null,nestInitial:null,_beforeNest:null,_partialId:first.id,_partialChain:chain,_partialNext:null});
      C.sheetDirty(sh);
      sh._byHand=true;
      C.startNest(sh);
      const sz=cardSize(first);
      try{C.agent({metal},'nest',`Partial sheet: ${label(sh)} moved onto a partial sheet of ${mm1(sz.wMm)} x ${mm1(sz.hMm)} mm${chain.length?`, with ${chain.length} more partial ${plural(chain.length,'sheet')} to take what does not fit`:''}; its ${pieces.length} ${plural(pieces.length,'piece')} are nested again`);}catch(_){}
      emit({type:'seated',metal,sheetId:sh.sheetId||null});
      step('done',`${label(sh)} is nesting on the partial sheet.`);
      sh._partialBusy=false;
      seen.delete(sigOf(sh,asked));
      const pv=shown;
      return {ok:true,started:true,sheets:[sh],links:pv?pv.links:[],moved:pieces.length,continues:pv?pv.continues.n:null};
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
    return pagesOf(sh.metal).filter(p=>p.roseStock&&p.roseStock.id&&(p._partialId||p.roseStock.revision>0||p.roseStock.profileJson)).map(p=>({
      sheet:p,sheetId:p.sheetId||null,label:label(p),page:p.page,partialId:partialOf(p),stockId:p.roseStock.id,
      wMm:mm1(p.roseStock.wPt*MM),hMm:mm1(p.roseStock.hPt*MM),placed:(p.placements||[]).length,waiting:Math.max(0,piecesOf(p).length-(p.placements||[]).length),
      state:p.roseCutAt?'cut':(p.movedOn||p.releaseFull)?'full':'open'}));
  }
  const on=fn=>{if(typeof fn!=='function')return ()=>{};listeners.add(fn);return ()=>listeners.delete(fn);};

  window.PartialNest={canSeat,preview,seat,useOn,chain,partialOf,on,nextPage,pending,claimed,lost,mode,newSheet,policy,
    _cards:cardsOf,_bestFit:bestFit,_wholeOrders:wholeOrders};

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
      return {ok:true,pieces:pv.pieces,fitsAll:pv.fitsAll,fits,rest:pv.pieces-fits,chain:chainRows,then:pv.continues.n?(pv.continues.next==='partial'?'wait':'new'):null,note:pv.note,words:pv.words};
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
