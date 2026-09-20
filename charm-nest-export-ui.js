/* One download path for Nest, Library sheets and complete sets. */
(function() {
  'use strict';
  let busy=false;
  const esc=v=>String(v).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const get=id=>CN.api('charmNestLibrary',{op:'getSheet',id},{label:'Preparing export'}).then(r=>{if(!r.sheet)throw new Error('Sheet not found: '+id);return r.sheet;});
  const bytes=output=>CharmNestAssets.bytes(output);
  const revision=sheet=>JSON.stringify({updatedAt:sheet.updatedAt,front:sheet.outputs?.ai,poolIds:sheet.poolIds,placements:sheet.placements,backs:(sheet.backPool||[]).map(b=>[b.poolId,b.approvedAt,b.outputs?.ai?.url])});
  async function build(sheet,format) {
    const backs=CharmNestBacks.forSheet(sheet,sheet.backPool||sheet.backs||[]);
    const prepared=[];
    for(const b of backs) {
      if(b.pending)throw new Error('An engraving is still saving. Try again in a moment.');
      prepared.push({...b,bytes:await bytes(b.outputs?.ai||b.ai)});
    }
    const composed=await CharmNestExport.compose(await bytes(sheet.outputs?.ai),sheet,prepared);
    let data=composed.ai,metadata={sheetId:sheet.id||sheet.sheetId,fileBase:sheet.fileBase,units:'mm',backs:composed.layout};
    if(format==='dxf') {
      const parsed=await CharmNestPDF.parseSource(composed.ai,'Production sheet');
      const result=CharmNestExport.dxf(CharmNestExport.productionPaths(parsed),CharmNestExport.layerNames(parsed));
      data=new TextEncoder().encode(result.text);
      metadata={...metadata,curveToleranceMm:.002,layers:result.layers,colors:result.colors,entities:result.entityCount};
    }
    return {data,metadata,name:(sheet.fileBase||sheet.folder||sheet.id||sheet.sheetId||'sheet').replace(/[\\/:*?"<>|]/g,'_')+'.'+format};
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
        zip.file('IMPORT.txt','Import at 1:1 in millimetres. Back engravings are above the cut sheet, at the parent scale.\nDXF preserves named layers and RGB colours, with indexed colour fallback. Filled artwork is exported as solid hatches, with its holes retained. SHEET and BACK CUT OUTLINE are reference layers, not extra cuts. Verify pen mapping and dimensions in your LaserStar version before marking.\n');
        CN.download(await zip.generateAsync({type:'uint8array'}),'charm-sheets-'+format+'.zip','application/zip');
      }
      CN.toast('Downloaded '+files.length+' sheet'+(files.length===1?'':'s')+' · '+format.toUpperCase(),'ok');
    } catch(e){CN.toast('Export stopped: '+e.message,'bad',12000);}
    finally{busy=false;refresh();}
  }
  const sheetControls=id=>`<span class="sheetExport" data-export-controls><button class="btn ghost xs" data-export-one="${esc(id)}" data-format="ai" title="Download sheet with back engravings">.ai</button><button class="btn ghost xs" data-export-one="${esc(id)}" data-format="dxf" title="DXF · millimetres · original colours and layers">.dxf</button></span>`;
  function refresh() {
    document.querySelectorAll('[data-export-one],[data-export-set]').forEach(b=>{b.disabled=busy;});
  }
  function sync() {
    const body=document.getElementById('libBody');if(!body)return;
    body.querySelectorAll('.libCard[data-id]').forEach(card=>{if(!card.querySelector('[data-export-controls]'))card.querySelector('.h')?.insertAdjacentHTML('beforeend',sheetControls(card.dataset.id));});
    body.querySelectorAll('.setCard,.libSet').forEach(group=>{
      const head=group.querySelector('.sh,.fanHead');if(!head)return;
      const ids=[...new Set([...group.querySelectorAll('.libCard[data-id]')].map(c=>c.dataset.id))];
      if(!head.querySelector('[data-export-set]'))head.insertAdjacentHTML('beforeend',`<span class="sheetExport"><button class="btn ghost xs" data-export-set="${esc(JSON.stringify(ids))}" data-format="ai">Download .ai</button><button class="btn ghost xs" data-export-set="${esc(JSON.stringify(ids))}" data-format="dxf">.dxf</button></span>`);
    });refresh();
  }
  function mount() {
    document.addEventListener('click',e=>{
      const one=e.target.closest('[data-export-one]'),set=e.target.closest('[data-export-set]');
      if(!one&&!set)return;e.stopImmediatePropagation();
      if(one){e.preventDefault();run([one.dataset.exportOne],one.dataset.format);}
      if(set){e.preventDefault();run(JSON.parse(set.dataset.exportSet),set.dataset.format || 'ai');}
    },true);
    let queued=false;new MutationObserver(()=>{if(!queued){queued=true;requestAnimationFrame(()=>{queued=false;sync();});}}).observe(document.getElementById('libBody'),{childList:true,subtree:true});sync();
  }
  window.ProductionExports={run,live:(sheet,format)=>run([],format,sheet),sheetControls,build};
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',mount);else mount();
})();
