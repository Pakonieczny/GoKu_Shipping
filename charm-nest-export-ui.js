/* One download path for Nest, Library sheets, sets and arbitrary selections. */
(function() {
  'use strict';
  const selected=new Set();let busy=false;
  const esc=v=>String(v).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const get=id=>CN.api('charmNestLibrary',{op:'getSheet',id},{label:'Preparing export'}).then(r=>{if(!r.sheet)throw new Error('Sheet not found: '+id);return r.sheet;});
  const bytes=async output=>{
    if(output instanceof Uint8Array)return output;
    const url=typeof output==='string'?output:output?.url;
    if(!url)throw new Error('Artwork is still saving. Try again when saving finishes.');
    const r=await fetch(CharmNestBacks.previewUrl(url),{cache:'no-store'});
    if(!r.ok)throw new Error('Could not read artwork ('+r.status+').');
    return new Uint8Array(await r.arrayBuffer());
  };
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
      const result=CharmNestExport.dxf(CharmNestExport.leaves(parsed),CharmNestExport.layerNames(parsed));
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
        zip.file('IMPORT.txt','Import at 1:1 in millimetres. Back engravings are above the cut sheet, at the parent scale.\nDXF preserves named layers and RGB colours, with indexed colour fallback. Filled artwork is exported as closed contours; apply the intended hatch settings in EZCAD. SHEET and BACK CUT OUTLINE are reference layers, not extra cuts. Verify pen mapping and dimensions in your LaserStar version before marking.\n');
        CN.download(await zip.generateAsync({type:'uint8array'}),'charm-sheets-'+format+'.zip','application/zip');
      }
      CN.toast('Downloaded '+files.length+' sheet'+(files.length===1?'':'s')+' · '+format.toUpperCase(),'ok');
    } catch(e){CN.toast('Export stopped: '+e.message,'bad',12000);}
    finally{busy=false;refresh();}
  }
  const sheetControls=id=>`<span class="sheetExport" data-export-controls><input type="checkbox" data-export-id="${esc(id)}" aria-label="Select sheet for download"><button class="btn ghost xs" data-export-one="${esc(id)}" data-format="ai" title="Download sheet with back engravings">.ai</button><button class="btn ghost xs" data-export-one="${esc(id)}" data-format="dxf" title="DXF · millimetres · original colours and layers">.dxf</button></span>`;
  function refresh() {
    document.querySelectorAll('[data-export-one],[data-export-set]').forEach(b=>{b.disabled=busy;});
    document.querySelectorAll('[data-export-id]').forEach(c=>{c.checked=selected.has(c.dataset.exportId);c.disabled=busy;});
    document.querySelectorAll('[data-export-group]').forEach(c=>{const ids=JSON.parse(c.dataset.exportGroup);c.checked=ids.length>0&&ids.every(id=>selected.has(id));c.indeterminate=!c.checked&&ids.some(id=>selected.has(id));c.disabled=busy;});
    const visible=document.getElementById('exportVisible'),ids=[...new Set([...document.querySelectorAll('#libBody .libCard[data-id]')].map(c=>c.dataset.id))];
    if(visible){visible.checked=ids.length>0&&ids.every(id=>selected.has(id));visible.indeterminate=!visible.checked&&ids.some(id=>selected.has(id));visible.disabled=busy;}
    const btn=document.getElementById('downloadSelected');if(btn){const text=busy?'Preparing…':'Download'+(selected.size?' ('+selected.size+')':'');if(btn.textContent!==text)btn.textContent=text;btn.disabled=busy||!selected.size;}
  }
  function sync() {
    const body=document.getElementById('libBody');if(!body)return;
    body.querySelectorAll('.libCard[data-id]').forEach(card=>{if(!card.querySelector('[data-export-controls]'))card.querySelector('.h')?.insertAdjacentHTML('beforeend',sheetControls(card.dataset.id));});
    body.querySelectorAll('.setCard,.libSet').forEach(group=>{
      const head=group.querySelector('.sh,.fanHead');if(!head)return;
      const ids=[...new Set([...group.querySelectorAll('.libCard[data-id]')].map(c=>c.dataset.id))];
      if(!head.querySelector('[data-export-group]'))head.insertAdjacentHTML('afterbegin',`<input type="checkbox" data-export-group="${esc(JSON.stringify(ids))}" aria-label="Select all sheets in this group">`);
      if(!head.querySelector('[data-export-set]'))head.insertAdjacentHTML('beforeend',`<button class="btn ghost xs" data-export-set="${esc(JSON.stringify(ids))}">Download sheets</button>`);
    });refresh();
  }
  function mount() {
    const bar=document.querySelector('#libraryView .libBar')||document.getElementById('libKind')?.parentElement;
    if(!bar)return;
    bar.insertAdjacentHTML('afterend',`<div class="libraryDownloads"><label><input type="checkbox" id="exportVisible"> Select visible</label><select id="exportFormat" aria-label="Download format"><option value="ai">Illustrator .ai</option><option value="dxf">Laser DXF .dxf</option></select><button class="btn sage xs" id="downloadSelected" disabled>Download</button><button class="btn ghost xs" id="clearExportSelection">Clear selection</button><details><summary>DXF import</summary><p>Use millimetres at 1:1. Colours and layers are retained; filled artwork uses closed contours for EZCAD hatching. Check pen mapping in your LaserStar version. Sheet outlines and back outlines are references.</p></details></div>`);
    document.getElementById('downloadSelected').onclick=()=>run([...selected],document.getElementById('exportFormat').value);
    document.getElementById('clearExportSelection').onclick=()=>{selected.clear();document.getElementById('exportVisible').checked=false;refresh();};
    document.getElementById('exportVisible').onchange=e=>{document.querySelectorAll('#libBody .libCard[data-id]').forEach(c=>e.target.checked?selected.add(c.dataset.id):selected.delete(c.dataset.id));refresh();};
    document.addEventListener('click',e=>{
      const one=e.target.closest('[data-export-one]'),set=e.target.closest('[data-export-set]'),check=e.target.closest('[data-export-id],[data-export-group]');
      if(!one&&!set&&!check)return;e.stopImmediatePropagation();
      if(one){e.preventDefault();run([one.dataset.exportOne],one.dataset.format);}
      if(set){e.preventDefault();run(JSON.parse(set.dataset.exportSet),set.dataset.format || document.getElementById('exportFormat').value);}
    },true);
    document.addEventListener('change',e=>{const c=e.target;if(c.dataset.exportId){c.checked?selected.add(c.dataset.exportId):selected.delete(c.dataset.exportId);refresh();}if(c.dataset.exportGroup){JSON.parse(c.dataset.exportGroup).forEach(id=>c.checked?selected.add(id):selected.delete(id));refresh();}});
    let queued=false;new MutationObserver(()=>{if(!queued){queued=true;requestAnimationFrame(()=>{queued=false;sync();});}}).observe(document.getElementById('libBody'),{childList:true,subtree:true});sync();
  }
  window.ProductionExports={run,live:(sheet,format)=>run([],format,sheet),sheetControls,build};
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',mount);else mount();
})();
