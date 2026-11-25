/* One download path for Nest, Library sheets and complete sets. */
(function() {
  'use strict';
  let busy=false;
  const esc=v=>String(v).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  /* DLPROGRESS (Paul, 25 Nov 2026: "add some sort of progress bar to provide feedback to the user as to the status of the download or the preparation of the download because sometimes it
     takes a while and the user may think it's broken"). One panel per download, beside the button that was pressed: the step it is on, a bar with its percentage, the sheet and the time
     spent; it ends green with what was saved, or red with why it stopped. It is drawn inside the open window when there is one (the sheet window is a modal dialog: anything drawn on the page
     under it is hidden, which is why a download pressed there said nothing at all). A step that cannot count (writing the .dxf) still moves, slowing as it nears its end, never standing still. */
  const Progress=(()=>{
    let el=null,styled=false,timer=null,st=null;
    const CSS=`.cnxp{position:fixed;z-index:2147483000;width:300px;max-width:calc(100vw - 16px);box-sizing:border-box;background:var(--velvet,#2b2622);color:#f4ecdc;border:1px solid var(--velvetLine,rgba(255,255,255,.12));border-radius:11px;padding:9px 12px 10px;box-shadow:0 10px 30px rgba(0,0,0,.28);font:12px/1.35 ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif;pointer-events:none;opacity:0;transition:opacity .15s ease}
.cnxp.on{opacity:1}.cnxp.bad{pointer-events:auto;cursor:pointer}
.cnxp .cnxpHead{display:flex;align-items:baseline;gap:8px}.cnxp .cnxpTitle{font-weight:650;flex:1 1 auto;min-width:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.cnxp .cnxpPct{font-weight:700;font-variant-numeric:tabular-nums}.cnxp .cnxpStep{margin-top:2px;opacity:.85;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.cnxp.bad .cnxpStep{white-space:normal;max-height:7.5em;overflow:auto}
.cnxp .cnxpTrack{position:relative;height:6px;border-radius:9px;background:rgba(255,255,255,.16);overflow:hidden;margin-top:7px}
.cnxp .cnxpFill{position:absolute;inset:0 auto 0 0;width:0;border-radius:9px;background:var(--gold2,#caa861);transition:width .2s ease-out}
.cnxp.ok .cnxpFill{background:#6fbf73}.cnxp.bad .cnxpFill{background:#e0574f}
.cnxp .cnxpMeta{margin-top:5px;opacity:.6;font-variant-numeric:tabular-nums;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}`;
    const secs=ms=>{const s=Math.max(0,Math.round(ms/1000));return s<60?s+'s':Math.floor(s/60)+'m '+String(s%60).padStart(2,'0')+'s';};
    const topModal=()=>[...document.querySelectorAll('dialog')].filter(d=>{try{return d.matches(':modal');}catch(_){return false;}}).pop()||null;
    function mount(){
      if(!styled){const s=document.createElement('style');s.textContent=CSS;document.head.appendChild(s);styled=true;}
      if(!el){el=document.createElement('div');el.className='cnxp';el.setAttribute('role','status');el.setAttribute('aria-live','polite');
        el.innerHTML='<div class="cnxpHead"><span class="cnxpTitle"></span><span class="cnxpPct"></span></div><div class="cnxpStep"></div><div class="cnxpTrack"><i class="cnxpFill"></i></div><div class="cnxpMeta"></div>';
        el.addEventListener('click',()=>{if(el.classList.contains('bad'))hide();});}
      const home=topModal()||document.body;if(el.parentNode!==home)home.appendChild(el);
      return el;
    }
    const put=(sel,text)=>{const n=el.querySelector(sel);if(n.textContent!==text)n.textContent=text;};
    /** beside the pressed button (under it, or over it near the bottom of the screen); a button that is gone (a menu that closed) or none: the bottom middle */
    function place(){
      const a=st&&st.anchor,r=a&&a.isConnected?a.getBoundingClientRect():null,w=el.offsetWidth||300,h=el.offsetHeight||70,vw=innerWidth,vh=innerHeight;
      if(!r||!(r.width||r.height)){el.style.left=Math.max(8,(vw-w)/2)+'px';el.style.top=(vh-h-16)+'px';return;}
      const left=Math.min(Math.max(8,r.left+r.width/2-w/2),vw-w-8),below=r.bottom+8,top=below+h>vh-8?Math.max(8,r.top-8-h):below;
      el.style.left=left+'px';el.style.top=top+'px';
    }
    /** the bar's value: the step's start, moving toward its end (90 % of the way after the time the step is expected to take, then slower), never past it and never back */
    function value(now){if(!st)return 0;if(st.final!=null)return st.final;const k=1-Math.exp(-2.3*(now-st.t1)/Math.max(200,st.expect));return Math.max(st.shown||0,st.from+(st.to-st.from)*Math.min(.97,k));}
    function draw(){
      if(!st||!el)return;const now=Date.now(),v=value(now);st.shown=v;
      put('.cnxpTitle',st.title);put('.cnxpPct',Math.floor(v*100)+'%');put('.cnxpStep',st.step||'');put('.cnxpMeta',(st.meta?st.meta+' · ':'')+secs((st.endAt||now)-st.t0));
      const fill=el.querySelector('.cnxpFill'),wd=(v*100).toFixed(1)+'%';if(fill.style.width!==wd)fill.style.width=wd;
      place();
    }
    function hide(){if(timer){clearInterval(timer);timer=null;}if(st&&st.hideT)clearTimeout(st.hideT);st=null;if(el){el.classList.remove('on','ok','bad');}}
    /** start(anchor, title) → { step(from, to, text, expectMs), meta(text), done(title, text), fail(text) } */
    // (the panel never stops a download: a page it cannot draw on, a test's stand-in document included, gets a panel that does nothing)
    const NOOP={step(){return NOOP;},meta(){return NOOP;},done(){},fail(){}};
    function start(anchor,title){
      try{hide();mount();}catch(_){return NOOP;}
      el.classList.remove('ok','bad');
      st={anchor,title:String(title),step:'',meta:'',t0:Date.now(),t1:Date.now(),from:0,to:.05,expect:600,shown:0,final:null};
      try{draw();(typeof requestAnimationFrame==='function'?requestAnimationFrame:f=>setTimeout(f,0))(()=>{if(el)el.classList.add('on');});timer=setInterval(()=>{try{draw();}catch(_){hide();}},120);}catch(_){hide();return NOOP;}
      const api={
        step(from,to,text,expect){if(!st)return api;st.from=Math.max(st.shown||0,from);st.to=Math.max(st.from,to);st.t1=Date.now();st.expect=expect||800;if(text!=null)st.step=String(text);draw();return api;},
        meta(text){if(st){st.meta=String(text||'');draw();}return api;},
        done(t,text){if(!st)return;mount();st.final=1;st.endAt=Date.now();st.title=String(t);st.step=String(text||'');el.classList.add('ok');draw();clearInterval(timer);timer=null;const me=st;st.hideT=setTimeout(()=>{if(st===me)hide();},3200);},
        fail(text){if(!st)return;mount();st.final=Math.max(st.shown||0,.02);st.endAt=Date.now();st.title='Download stopped';st.step=String(text||'');el.classList.add('bad');draw();clearInterval(timer);timer=null;const me=st;st.hideT=setTimeout(()=>{if(st===me)hide();},15000);}
      };
      return api;
    }
    /** a press while a download is being made: the panel says so for a moment, then goes on with its own step */
    function nudge(){if(!st||!el||st.final!=null)return;const was=st.step;st.step='Still preparing the download you asked for first…';draw();setTimeout(()=>{if(st&&st.step==='Still preparing the download you asked for first…'){st.step=was;draw();}},1800);}
    return {start,nudge,hide};
  })();
  // the button a person just pressed (any caller: the Library, the sheet window and its menu, a Nest card, the runs window): the panel is put beside it
  let lastPress=null;
  const pressedNow=()=>lastPress&&Date.now()-lastPress.at<2500&&lastPress.el&&lastPress.el.isConnected?lastPress.el:null;
  const fileSize=n=>n>=1048576?(n/1048576).toFixed(1)+' MB':Math.max(1,Math.round(n/1024))+' KB';
  const get=id=>CN.api('charmNestLibrary',{op:'getSheet',id},{label:'Preparing export'}).then(r=>{if(!r.sheet)throw new Error('Sheet not found: '+id);return r.sheet;});
  const bytes=output=>CharmNestAssets.bytes(output);
  const revision=sheet=>JSON.stringify({updatedAt:sheet.updatedAt,rosePlanHash:sheet.rosePlanHash,front:sheet.outputs?.ai,poolIds:sheet.poolIds,placements:sheet.placements,backs:(sheet.backPool||[]).map(b=>[b.poolId,b.approvedAt,b.outputs?.ai?.url])});
  // (front + back engraving, 11 Oct 2026: the sheet's flat list holds the records of both faces; a front record says face:"front", a back says nothing)
  const isFront=r=>!!r&&r.face==='front';
  /* BACKSMISSING (Paul, 16 Nov 2026: a downloaded DXF "does not save some of the back engraving that was already approved on the sheet"). A download holds the sheet's saved drawing and the engraving
     files its record lists (backPool), nothing else: an engraving the sheet needs whose file is not saved (an approval still waiting to be written, or one withdrawn and waiting for Approve again) is
     not in it, and nothing said so. The cloud's own word for each piece (sheet.engraving, as getSheet answers it) is read against the record's list: every piece whose engraving is needed and has no
     saved record is named. FRONTLIKEBACK (Paul, 25 Nov 2026: "there is no difference between back or front engraving"): the FRONT is read exactly as the back (it was not looked at: a front made only
     of the charm editor's parts was in the sheet's drawing, and is not any more). A sheet with no such word (one not saved yet) names none. → [{ poolId, order, sku, approved, face }] */
  function missingBacks(sheet) {
    const eng=sheet&&sheet.engraving&&typeof sheet.engraving==='object'?sheet.engraving:null;if(!eng)return [];
    const savedOf=front=>new Set((sheet.backPool||sheet.backs||[]).filter(r=>r&&isFront(r)===front&&!r.invalidated&&!r.pending&&(r.outputs?.ai||r.ai)).map(r=>String(r.poolId)));
    const saved={back:savedOf(false),front:savedOf(true)};
    const on=Array.isArray(sheet.poolIds)?new Set(sheet.poolIds.map(String)):null,skuOf=new Map((sheet.charms||[]).filter(c=>c&&c.poolId).map(c=>[String(c.poolId),c.sku||'']));
    return Object.keys(eng).filter(id=>!on||on.has(id)).flatMap(id=>{const e=eng[id]||{},faces=e.faces&&typeof e.faces==='object'?e.faces:{back:e};
      return ['back','front'].map(face=>{const f=faces[face];return f&&f.needed&&!saved[face].has(id)?{poolId:id,order:id.split('_')[0],sku:skuOf.get(id)||'',approved:!!f.approved,face}:null;});}).filter(Boolean);
  }
  /** report(stage, fraction, words): DLPROGRESS's steps of one sheet: "files" (the sheet's drawing and its engravings are read), "compose" (the engravings put on it), "convert" (the .ai or .dxf written) */
  async function build(sheet,format,report) {
    const say=typeof report==='function'?report:()=>{};
    const records=sheet.backPool||sheet.backs||[];
    const backs=CharmNestBacks.forSheet(sheet,records.filter(r=>!isFront(r)));
    const fronts=CharmNestBacks.forSheet(sheet,records.filter(isFront));
    const prepared=[],preparedFronts=[],count=backs.length+fronts.length;let read=0;
    const readOne=()=>{read++;say('files',read/(count+1),'Reading the engravings · '+read+' of '+count);};
    say('files',0,count?'Reading the engravings · 0 of '+count:'Reading the sheet drawing');
    for(const b of backs) {
      if(b.pending)throw new Error('An engraving is still saving. Try again in a moment.');
      prepared.push({...b,bytes:await bytes(b.outputs?.ai||b.ai)});readOne();
    }
    for(const b of fronts) {
      if(b.pending)throw new Error('An engraving is still saving. Try again in a moment.');
      preparedFronts.push({...b,bytes:await bytes(b.outputs?.ai||b.ai)});readOne();
    }
    say('files',count/(count+1),'Reading the sheet drawing');
    const sheetBytes=await bytes(sheet.outputs?.ai);
    say('compose',0,count?'Putting '+count+' engraving'+(count===1?'':'s')+' on the sheet':'Laying out the sheet');
    const composed=preparedFronts.length?await CharmNestExport.compose(sheetBytes,sheet,prepared,preparedFronts):await CharmNestExport.compose(sheetBytes,sheet,prepared);
    // (earring pieces on this sheet, each with its side: the manifest says how many, and the zip's IMPORT.txt explains the Left / Right layers; none: as before)
    const earPieces=(sheet.charms||[]).filter(c=>c&&(c.side==='L'||c.side==='R')&&(sheet.placements||[]).some(p=>p.id===c.id)).length;
    // (front engravings: the manifest lists each one and whether it sits on its charm or was left out (FRONTONCHARM: never above the sheet); a sheet with none has no such key)
    let data=composed.ai,metadata={sheetId:sheet.id||sheet.sheetId,fileBase:sheet.fileBase,units:'mm',backs:composed.layout,...(earPieces?{pairPieces:earPieces}:{}),...(composed.frontLayout?{fronts:composed.frontLayout}:{}),...(composed.contourMissing?{contourMissing:composed.contourMissing}:{})};
    const noBack=missingBacks(sheet);if(noBack.length)metadata.backsMissing=noBack;
    // (AIMATCH, Paul 17 Nov 2026: the .ai has to match the .dxf: the same layers and the same paths, the back engravings above the sheet; never the sheet and its backs as forms inside a page)
    say('convert',0,format==='ai'?'Writing the .ai file (Illustrator layers)':'Writing the .dxf file (millimetres, layers and colours)');
    if(format==='ai'&&CharmNestExport.productionAi){
      // (AINATIVE, Paul 17 Nov 2026: "they can't be under one directory ... they have to be organized just like the DXF"): the file Illustrator opens with every layer a TOP-LEVEL layer (charm-nest-ai-native.js);
      // when it cannot be made, the plain .ai (the .dxf's layers as one page's layers) is written as before
      let ai=null;
      try{if(CharmNestExport.productionAiNative)ai=await CharmNestExport.productionAiNative(composed.ai,sheet.fileBase);}
      catch(e){console.warn('[export] the native .ai was not made, the plain .ai is written:',e&&e.message);ai=null;}
      if(!ai)ai=await CharmNestExport.productionAi(composed.ai,sheet.fileBase);
      data=ai instanceof Uint8Array?ai:new Uint8Array(ai);
    }
    if(format==='dxf') {
      const result=CharmNestExport.productionDxf ? await CharmNestExport.productionDxf(composed.ai) : await (async()=>{const parsed=await CharmNestPDF.parseSource(composed.ai,'Production sheet');return CharmNestExport.dxf(CharmNestExport.productionPaths(parsed),CharmNestExport.layerNames(parsed));})();
      data=new TextEncoder().encode(result.text);
      metadata={...metadata,curveToleranceMm:.002,layers:result.layers,colors:result.colors,entities:result.entityCount,...(composed.frontLayout?{frontLayers:result.layers.filter(([name])=>CharmNestExport.FRONT_LAYER.test(name)).map(([name,cad])=>({layer:name,dxfLayer:cad}))}:{})};
    }
    return {data,metadata,name:(sheet.fileBase||sheet.folder||sheet.id||sheet.sheetId||'sheet').replace(/[\\/:*?"<>|]/g,'_')+'.'+format};
  }
  /** IMPORT.txt of a multi-sheet download. The first paragraphs are what they always were; the front paragraphs are added only when a sheet in the download has a front engraving. */
  function importText(files) {
    let text='Import at 1:1 in millimetres. Back engravings are above the cut sheet, at the parent scale.\nDXF for laser software (LaserStar): every charm is one layer named for the charm, and under it are its jobs as blocks: <charm> - CUT (black, colour 7), <charm> - ENGRAVE (red, colour 1) and <charm> - HATCH (blue, colour 5), the front engraving put on it included; each back above the sheet is one layer, BACK <order>, with its blocks. Colours are colour numbers only: map the pens by colour. Filled artwork is its closed outline plus crosshatch lines both ways every 0.02 mm (holes left clear), never a HATCH entity. SHEET and BACK CUT OUTLINE are reference layers, not extra cuts. Verify pen mapping and dimensions in your LaserStar version before marking.\n'+(files.some(f=>f.metadata.pairPieces)?'A layer ending Left or Right is one ear of an earring pair. The Right is the Left turned over (a mirror image), drawn here exactly as it is cut: do not flip it. The layer with the same order number and the other word is the other ear; the two go together (the sheet label says where the other ear is when it is on another sheet).\n':'');
    const fronts=files.flatMap(f=>(f.metadata.fronts||[]).map(e=>({...e,sheetId:f.metadata.sheetId})));
    if(fronts.length) {
      text+='FRONT engravings: a layer named FRONT <order number> / ENGRAVE is the engraving for the FRONT face of a charm (the face you see when the charm hangs from its hoop), drawn as that face reads, not mirrored: do not flip it. It is put ON its charm in the cut sheet, at the charm\'s own position and turn, so the laser engraves it in the same run as the cut. The back engravings (BACK layers) are the other face and stay above the sheet. FRONT layers ending Left or Right are that ear of an earring pair.\n';
      const above=fronts.filter(e=>!e.placed);
      // (FRONTONCHARM, Paul 25 Nov 2026: a front is never laid above the sheet as a back is; one that could not be put on its charm is not in the files, and is named here)
      if(above.length)text+='These front engravings are NOT in these files: they could not be put on their charm, and a front engraving is never laid above the sheet like a back. Save each one again (approve its front again) and download again.\n'+above.map(e=>'  '+(e.sheetId?e.sheetId+' · ':'')+e.poolId+(e.side?' '+(e.side==='L'?'Left':'Right'):'')+': '+(e.reason||'not placed')).join('\n')+'\n';
    }
    return text;
  }
  async function run(ids,format='ai',live) {
    if(busy){Progress.nudge();return;}busy=true;refresh();
    const n=live?1:[...new Set(ids||[])].length,ext='.'+format;
    const bar=Progress.start(pressedNow(),'Preparing '+ext+(n>1?' · '+n+' sheets':''));
    // (DLPROGRESS: each sheet is its share of the bar; within it: read 0–45 %, put the engravings on 45–60 %, write the file 60–95 %, check nothing changed 95–100 %. The zip and the save are the last 5 %.)
    const STAGE={get:[0,.08,700],files:[.08,.45,2500],compose:[.45,.6,1500],convert:[.6,.95,format==='dxf'?2600:2200],check:[.95,1,700]};
    const share=n>1?.95:1;
    const reportFor=i=>(stage,frac,words)=>{const [s0,s1,ms]=STAGE[stage]||STAGE.files,a0=share*i/n,a1=share*(i+1)/n,at=f=>a0+(a1-a0)*f,lo=s0+(s1-s0)*Math.max(0,Math.min(1,frac||0));
      bar.step(at(lo),at(stage==='files'?Math.min(s1,lo+(s1-s0)/3):s1),words,ms);};
    const sheetMeta=(i,sheet)=>bar.meta((n>1?'Sheet '+(i+1)+' of '+n+' · ':'')+((sheet&&(sheet.fileBase||sheet.folder||sheet.id||sheet.sheetId))||''));
    try {
      CN.toast('Preparing '+format.toUpperCase()+' export…','');
      const files=[];
      if(live) {
        const id=live.sheetId||live.recalled?.id;
        const hasLocal = live.outputs?.ai instanceof Uint8Array;
        reportFor(0)('get',0,'Reading the sheet');
        const saved=id && (!hasLocal || live.cloud || live.recalled)?await get(id):null;
        const sheet=saved || (hasLocal?{...live,id,poolIds:undefined,backPool:window.Engrave?Engrave.sheetBacks(live):live.backPool}:null);
        if(!sheet)throw new Error('Nest this sheet before downloading it.');
        sheetMeta(0,sheet);
        const rev=saved&&revision(saved);files.push(await build(sheet,format,reportFor(0)));
        if(saved){reportFor(0)('check',0,'Checking nobody changed the sheet meanwhile');if(revision(await get(id))!==rev)throw new Error('The sheet changed during export. Download again to include the latest edits.');}
      } else {
        let i=0;
        for(const id of [...new Set(ids)]) {
          reportFor(i)('get',0,n>1?'Reading sheet '+(i+1)+' of '+n:'Reading the sheet');
          const sheet=await get(id),rev=revision(sheet);sheetMeta(i,sheet);files.push(await build(sheet,format,reportFor(i)));
          reportFor(i)('check',0,'Checking nobody changed the sheet meanwhile');
          if(revision(await get(id))!==rev)throw new Error('A selected sheet changed during export. Please download again.');
          i++;
        }
      }
      if(!files.length)throw new Error('Select at least one sheet.');
      let saved;
      if(files.length===1){bar.step(share,1,'Saving '+files[0].name,300);CN.download(files[0].data,files[0].name,format==='ai'?'application/illustrator':'application/dxf');saved={name:files[0].name,size:files[0].data.length};}
      else {
        bar.step(share,.995,'Zipping '+files.length+' sheets',1500);
        const zip=new JSZip(),used=new Set();
        for(const file of files){let name=file.name;if(used.has(name))name=file.metadata.sheetId+'_'+name;used.add(name);zip.file(name,file.data);}
        zip.file('sheet-manifest.json',JSON.stringify(files.map(f=>f.metadata),null,2));
        zip.file('IMPORT.txt',importText(files));
        const zipped=await zip.generateAsync({type:'uint8array'});
        CN.download(zipped,'charm-sheets-'+format+'.zip','application/zip');saved={name:'charm-sheets-'+format+'.zip',size:zipped.length};
      }
      bar.done('Downloaded '+ext+(files.length>1?' · '+files.length+' sheets':''),saved.name+' · '+fileSize(saved.size)+' · in your Downloads');
      CN.toast('Downloaded '+files.length+' sheet'+(files.length===1?'':'s')+' · '+format.toUpperCase(),'ok');
      // (FRONTONCHARM: a front engraving that could not be put on its charm is left out of the files, never laid above the sheet: it is named at once, whatever the download (a single file has no IMPORT.txt))
      const leftFronts=files.flatMap(f=>(f.metadata.fronts||[]).filter(e=>!e.placed));
      // (DLALWAYS: a sheet downloaded before its separation contour was prepared says so: the file has no contour layer)
      const noContour=files.filter(f=>f.metadata.contourMissing);
      if(noContour.length)CN.toast((noContour.length===1?'Downloaded without its ':'Downloaded '+noContour.length+' sheets without their ')+noContour[0].metadata.contourMissing+' separation contour: prepare it before laser cutting','bad',12000);
      // (BACKSMISSING: a back engraving the sheet needs that is not in the file is named, never left out in silence)
      const noBacks=files.flatMap(f=>f.metadata.backsMissing||[]);
      if(noBacks.length)CN.toast(noBacks.length+' engraving'+(noBacks.length===1?' this sheet needs is':'s this sheet needs are')+' NOT in this download: '+noBacks.slice(0,6).map(m=>m.order+(m.sku?' '+m.sku:'')+' '+(m.face==='front'?'front':'back')+(m.approved?' (approved, its file is not saved yet)':' (waiting for Approve)')).join(' · ')+(noBacks.length>6?' · and '+(noBacks.length-6)+' more':'')+'. Approve or save them in Engrave, then download again','bad',20000);
      if(leftFronts.length)CN.toast(leftFronts.length+' front engraving'+(leftFronts.length===1?' is':'s are')+' NOT in this download (it could not be put on its charm; a front is never laid above the sheet): '+leftFronts.slice(0,6).map(e=>String(e.poolId||'').split('_')[0]+(e.side?' '+(e.side==='L'?'Left':'Right'):'')+(e.reason?' ('+e.reason+')':'')).join(' · ')+(leftFronts.length>6?' · and '+(leftFronts.length-6)+' more':'')+'. Approve its front again, then download again','bad',20000);
    } catch(e){bar.fail(e.message);CN.toast('Export stopped: '+e.message,'bad',12000);}
    finally{busy=false;refresh();}
  }
  const sheetControls=id=>`<span class="sheetExport" data-export-controls><button class="btn ghost xs" data-export-one="${esc(id)}" data-format="ai" title="Download the sheet with its engravings">.ai</button><button class="btn ghost xs" data-export-one="${esc(id)}" data-format="dxf" title="DXF · millimetres · original colours and layers">.dxf</button></span>`;
  function refresh() {
    document.querySelectorAll('[data-export-one],[data-export-set]').forEach(b=>{b.disabled=busy;});
  }
  // the Library's two tabs: Current (#libBody) and a set opened in the Completed list (#libDone)
  const bodies=()=>['libBody','libDone'].map(id=>document.getElementById(id)).filter(Boolean);
  function sync() {
    for(const body of bodies()){
    body.querySelectorAll('.libCard[data-id]').forEach(card=>{if(!card.querySelector('[data-export-controls]'))card.querySelector('.h')?.insertAdjacentHTML('beforeend',sheetControls(card.dataset.id));});
    body.querySelectorAll('.setCard,.libSet').forEach(group=>{
      const head=group.querySelector('.sh,.fanHead');if(!head)return;
      // (Paul, 26 Oct 2026: a 10K / 14K sheet drawn greyed in a set, waiting for a person's approval and green cut line, is no sheet of the set: never in its download until it is cut)
      const ids=[...new Set([...group.querySelectorAll('.libCard[data-id]')].filter(c=>!c.closest('[data-gold-pending]')).map(c=>c.dataset.id))];
      if(!head.querySelector('[data-export-set]'))head.insertAdjacentHTML('beforeend',`<span class="sheetExport"><button class="btn ghost xs" data-export-set="${esc(JSON.stringify(ids))}" data-format="ai">Download .ai</button><button class="btn ghost xs" data-export-set="${esc(JSON.stringify(ids))}" data-format="dxf">.dxf</button></span>`);
    });
    }refresh();
  }
  function mount() {
    for(const type of ['pointerdown','keydown'])document.addEventListener(type,e=>{if(type==='keydown'&&e.key!=='Enter'&&e.key!==' ')return;const b=e.target&&e.target.closest&&e.target.closest('button,[role="button"],a');if(b)lastPress={el:b,at:Date.now()};},true);
    document.addEventListener('click',e=>{
      const one=e.target.closest('[data-export-one]'),set=e.target.closest('[data-export-set]');
      if(!one&&!set)return;e.stopImmediatePropagation();
      if(one){e.preventDefault();run([one.dataset.exportOne],one.dataset.format);}
      if(set){e.preventDefault();run(JSON.parse(set.dataset.exportSet),set.dataset.format || 'ai');}
    },true);
    let queued=false;const seen=new MutationObserver(()=>{if(!queued){queued=true;requestAnimationFrame(()=>{queued=false;sync();});}});for(const body of bodies())seen.observe(body,{childList:true,subtree:true});sync();
  }
  window.ProductionExports={run,live:(sheet,format)=>run([],format,sheet),sheetControls,build,missingBacks};
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',mount);else mount();
})();
