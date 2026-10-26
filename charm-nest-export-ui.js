/* One download path for Nest, Library sheets and complete sets. */
(function() {
  'use strict';
  let busy=false;
  const esc=v=>String(v).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const get=id=>CN.api('charmNestLibrary',{op:'getSheet',id},{label:'Preparing export'}).then(r=>{if(!r.sheet)throw new Error('Sheet not found: '+id);return r.sheet;});
  const bytes=output=>CharmNestAssets.bytes(output);
  const revision=sheet=>JSON.stringify({updatedAt:sheet.updatedAt,rosePlanHash:sheet.rosePlanHash,front:sheet.outputs?.ai,poolIds:sheet.poolIds,placements:sheet.placements,backs:(sheet.backPool||[]).map(b=>[b.poolId,b.approvedAt,b.outputs?.ai?.url])});
  // (front + back engraving, 11 Oct 2026: the sheet's flat list holds the records of both faces; a front record says face:"front", a back says nothing)
  const isFront=r=>!!r&&r.face==='front';
  async function build(sheet,format) {
    const records=sheet.backPool||sheet.backs||[];
    const backs=CharmNestBacks.forSheet(sheet,records.filter(r=>!isFront(r)));
    const fronts=CharmNestBacks.forSheet(sheet,records.filter(isFront));
    const prepared=[],preparedFronts=[];
    for(const b of backs) {
      if(b.pending)throw new Error('An engraving is still saving. Try again in a moment.');
      prepared.push({...b,bytes:await bytes(b.outputs?.ai||b.ai)});
    }
    for(const b of fronts) {
      if(b.pending)throw new Error('An engraving is still saving. Try again in a moment.');
      preparedFronts.push({...b,bytes:await bytes(b.outputs?.ai||b.ai)});
    }
    const composed=preparedFronts.length?await CharmNestExport.compose(await bytes(sheet.outputs?.ai),sheet,prepared,preparedFronts):await CharmNestExport.compose(await bytes(sheet.outputs?.ai),sheet,prepared);
    // (earring pieces on this sheet, each with its side: the manifest says how many, and the zip's IMPORT.txt explains the Left / Right layers; none: as before)
    const earPieces=(sheet.charms||[]).filter(c=>c&&(c.side==='L'||c.side==='R')&&(sheet.placements||[]).some(p=>p.id===c.id)).length;
    // (front engravings: the manifest lists each one and whether it sits on its charm or above the sheet; a sheet with none has no such key)
    let data=composed.ai,metadata={sheetId:sheet.id||sheet.sheetId,fileBase:sheet.fileBase,units:'mm',backs:composed.layout,...(earPieces?{pairPieces:earPieces}:{}),...(composed.frontLayout?{fronts:composed.frontLayout}:{})};
    if(format==='dxf') {
      const result=CharmNestExport.productionDxf ? await CharmNestExport.productionDxf(composed.ai) : await (async()=>{const parsed=await CharmNestPDF.parseSource(composed.ai,'Production sheet');return CharmNestExport.dxf(CharmNestExport.productionPaths(parsed),CharmNestExport.layerNames(parsed));})();
      data=new TextEncoder().encode(result.text);
      metadata={...metadata,curveToleranceMm:.002,layers:result.layers,colors:result.colors,entities:result.entityCount,...(composed.frontLayout?{frontLayers:result.layers.filter(([name])=>CharmNestExport.FRONT_LAYER.test(name)).map(([name,cad])=>({layer:name,dxfLayer:cad}))}:{})};
    }
    return {data,metadata,name:(sheet.fileBase||sheet.folder||sheet.id||sheet.sheetId||'sheet').replace(/[\\/:*?"<>|]/g,'_')+'.'+format};
  }
  /** IMPORT.txt of a multi-sheet download. The first paragraphs are what they always were; the front paragraphs are added only when a sheet in the download has a front engraving. */
  function importText(files) {
    let text='Import at 1:1 in millimetres. Back engravings are above the cut sheet, at the parent scale.\nDXF preserves named layers and RGB colours, with indexed colour fallback. Filled artwork is exported as solid hatches, with its holes retained. SHEET and BACK CUT OUTLINE are reference layers, not extra cuts. Verify pen mapping and dimensions in your LaserStar version before marking.\n'+(files.some(f=>f.metadata.pairPieces)?'A layer ending Left or Right is one ear of an earring pair. The Right is the Left turned over (a mirror image), drawn here exactly as it is cut: do not flip it. The layer with the same order number and the other word is the other ear; the two go together (the sheet label says where the other ear is when it is on another sheet).\n':'');
    const fronts=files.flatMap(f=>(f.metadata.fronts||[]).map(e=>({...e,sheetId:f.metadata.sheetId})));
    if(fronts.length) {
      text+='FRONT engravings: a layer named FRONT <order number> / ENGRAVE is the engraving for the FRONT face of a charm (the face you see when the charm hangs from its hoop), drawn as that face reads, not mirrored: do not flip it. It is put ON its charm in the cut sheet, at the charm\'s own position and turn, so the laser engraves it in the same run as the cut. The back engravings (BACK layers) are the other face and stay above the sheet. FRONT layers ending Left or Right are that ear of an earring pair.\n';
      const above=fronts.filter(e=>!e.placed);
      if(above.length)text+='These front engravings could NOT be put on their charm, so they are laid above the cut sheet like the backs, at the parent scale (layer FRONT <order number> / ENGRAVE, with the charm\'s reference outline, which is not a cut, in the layer beside it). Do not engrave them from there as if they were on the charm: move each onto its charm, or ask for the engraving to be saved again.\n'+above.map(e=>'  '+(e.sheetId?e.sheetId+' · ':'')+e.poolId+(e.side?' '+(e.side==='L'?'Left':'Right'):'')+': '+(e.reason||'not placed')).join('\n')+'\n';
    }
    return text;
  }
  async function run(ids,format='ai',live) {
    if(busy)return;busy=true;refresh();
    try {
      CN.toast('Preparing '+format.toUpperCase()+' export…','');
      const files=[];
      if(live) {
        const id=live.sheetId||live.recalled?.id;
        const hasLocal = live.outputs?.ai instanceof Uint8Array;
        const saved=id && (!hasLocal || live.cloud || live.recalled)?await get(id):null;
        const sheet=saved || (hasLocal?{...live,id,poolIds:undefined,backPool:window.Engrave?Engrave.sheetBacks(live):live.backPool}:null);
        if(!sheet)throw new Error('Nest this sheet before downloading it.');
        const rev=saved&&revision(saved);files.push(await build(sheet,format));
        if(saved && revision(await get(id))!==rev)throw new Error('The sheet changed during export. Download again to include the latest edits.');
      } else {
        for(const id of [...new Set(ids)]) {
          const sheet=await get(id),rev=revision(sheet);files.push(await build(sheet,format));
          if(revision(await get(id))!==rev)throw new Error('A selected sheet changed during export. Please download again.');
        }
      }
      if(!files.length)throw new Error('Select at least one sheet.');
      if(files.length===1)CN.download(files[0].data,files[0].name,format==='ai'?'application/illustrator':'application/dxf');
      else {
        const zip=new JSZip(),used=new Set();
        for(const file of files){let name=file.name;if(used.has(name))name=file.metadata.sheetId+'_'+name;used.add(name);zip.file(name,file.data);}
        zip.file('sheet-manifest.json',JSON.stringify(files.map(f=>f.metadata),null,2));
        zip.file('IMPORT.txt',importText(files));
        CN.download(await zip.generateAsync({type:'uint8array'}),'charm-sheets-'+format+'.zip','application/zip');
      }
      CN.toast('Downloaded '+files.length+' sheet'+(files.length===1?'':'s')+' · '+format.toUpperCase(),'ok');
      // (a front engraving that is laid above the sheet instead of on its charm is said at once, whatever the download: a single file has no IMPORT.txt)
      const aboveFronts=files.reduce((n,f)=>n+(f.metadata.fronts||[]).filter(e=>!e.placed).length,0);
      if(aboveFronts)CN.toast(aboveFronts+' front engraving'+(aboveFronts===1?' is':'s are')+' above the sheet, not on '+(aboveFronts===1?'its charm':'their charms')+' (see IMPORT.txt or the layers named FRONT)','bad',12000);
    } catch(e){CN.toast('Export stopped: '+e.message,'bad',12000);}
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
    document.addEventListener('click',e=>{
      const one=e.target.closest('[data-export-one]'),set=e.target.closest('[data-export-set]');
      if(!one&&!set)return;e.stopImmediatePropagation();
      if(one){e.preventDefault();run([one.dataset.exportOne],one.dataset.format);}
      if(set){e.preventDefault();run(JSON.parse(set.dataset.exportSet),set.dataset.format || 'ai');}
    },true);
    let queued=false;const seen=new MutationObserver(()=>{if(!queued){queued=true;requestAnimationFrame(()=>{queued=false;sync();});}});for(const body of bodies())seen.observe(body,{childList:true,subtree:true});sync();
  }
  window.ProductionExports={run,live:(sheet,format)=>run([],format,sheet),sheetControls,build};
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',mount);else mount();
})();
