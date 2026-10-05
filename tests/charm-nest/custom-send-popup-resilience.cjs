/* Actual OrderWin rendering/lookup functions, with controlled saved records.
 * No live orders, network requests, or synthetic approval stamps are written.
 * (5 Oct, round 8: the tan Custom Orders bar is gone from the order window; what it drew for a piece sent to its sheet, or completed by hand,
 * is on that piece's own row under "Its pieces" (paintPieceSum, pieceCtl, wirePcAct), which is what runs here. No Reopen button in the window.) */
const assert = require('node:assert/strict');
const fs = require('node:fs');
const { JSDOM } = require('jsdom');
const source = fs.readFileSync('charm-nest-bridge.js', 'utf8');
const section = (from, to) => {
  const start = source.indexOf(from), end = source.indexOf(to, start + from.length);
  assert(start >= 0 && end > start, 'production function boundaries exist: ' + from);
  return source.slice(start, end);
};
const at = Date.UTC(2026, 9, 2, 2, 59), key = '4175152234_1';
const makeRow = (k = key) => ({ key:k, state:'pooled', order:{receiptId:k.split('_')[0]}, line:{transactionId:k.split('_')[1]}, spec:{}, poolIds:[k + '_1'] });
const sentRecord = () => ({ id:'original-send', state:'decided', how:'sheet', at, by:'Paul', decidedAt:at, receiptId:'4175152234', category:'Custom designs', stamps:[{how:'sheet', id:'original-send', at, by:'Paul'}] });
function fixture() {
  const dom = new JSDOM('<dialog id="orderWin" open><header><span id="owNow"></span></header><main><div id="owFix"></div><div class="owPcSum" id="owPcSum" hidden></div></main></dialog><div id="toasts"></div>', {runScripts:'outside-only', pretendToBeVisual:true, url:'https://sorter.test'});
  const w = dom.window, d = w.document;
  w.matchMedia = () => ({matches:true});
  w.Element.prototype.getAnimations = () => [];
  w.Element.prototype.animate = () => ({finished:Promise.resolve(), cancel(){}, playState:'finished'});
  w.eval(fs.readFileSync('charm-nest-motion.js', 'utf8'));
  const row = makeRow(), rec = sentRecord(), rows = new Map([[row.key,row]]), calls = {views:[], designs:[], prints:[], completes:[], reopens:[], notices:[], reads:[], hydration:[]};
  const item = {kind:'customOrder', key:'csent:custom:4175152234', info:true, decided:true, done:false, row, rows:[row], record:rec, why:'Its designs are on the sheets'};
  const state = {row, item, sent:{sent:{at,by:'Paul',lines:{[key]:[{f:'file',i:0}]}}}, files:[{id:'file'}], ready:true, apiRecord:rec, cloudRecords:{}};
  w.B = {maps:{customKept:{}, customSent:{}}};
  w.W = {key:row.key, dlg:d.querySelector('#orderWin'), closing:false, pieces:[], piece:null, events:null, cancelled:null};
  w.byId = id => d.getElementById(id);
  w.el = (tag, cls, html) => { const n = d.createElement(tag); if (cls) n.className = cls; if (html != null) n.innerHTML = html; return n; };
  w.esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  w.tryDo = fn => { try { return fn(); } catch (_) { return null; } };
  w.rowOf = k => rows.get(k);
  w.inPull = k => rows.get(k);
  w.employeeName = () => 'Paul'; w.askEmployee = () => 'Paul';
  w.setView = view => calls.views.push(view);
  w.Review = {
    customItemFor(k, fallback) { calls.lookup = {k,fallback}; return state.item; },
    pieceItemFor:() => state.item && !state.item.decided && !state.item.foldedInto ? state.item : null,   // (a card in the Review tab, not one sent to its sheet)
    actFor:() => state.item,
    printable:it => !it.decided
  };
  w.CustomSheet = {
    sentOf:() => state.sent,
    decisionOf:r => state.sent?.sent || r._customSentDecision || w.B.maps.customSent[r.key],
    cardOf:() => ({files:state.files, sent:!!state.sent, open:state.ready, why:'', busy:''}),
    stamp:() => JSON.stringify(state.files),
    hydrate:records => { calls.hydration.push(records); state.files = records[0].files; },
    open:it => calls.designs.push(it),
    send:async() => {}
  };
  w.CustomPrint = {
    statusHtml:() => '', stamp:it => String(it.key), freshOf:() => 0, failNote:() => '', wire:() => null,
    buttonHtml:(it, act, cls, label) => `<button data-cu-${act}>${label}</button>`,
    keptButtonHtml:(it, act, cls, label) => `<button data-cu-${act}>${label}</button>`,
    print:it => calls.prints.push(it), complete:it => calls.completes.push(it), reopen:it => calls.reopens.push(it)
  };
  w.Seal.add = () => { throw Error('historical rendering must never add a seal'); };
  w.Seal.press = () => { throw Error('historical rendering must never replay a stamp'); };
  w.Motion.note = (b, opts) => { calls.notices.push(opts); return d.createElement('span'); };
  w.eval(section('  function paintSend(', '  /** The order\'s notes as they stand now:'));
  // (a piece's row: the timeline's summary and the piece list are answered here; the controls and their wiring are the production code)
  w.OrderTimelineUI = {STAGES:['arrived'], summary:(events, pieces) => ({each:pieces.map(p => ({p, D:{step:0, stages:[{}], hand:false, cancelled:false, W:null}, steps:[]})), rail:[], step:0})};
  w.piecesOf = rows => rows.map(x => ({key:x.key, name:'Piece', metal:null, form:'', qty:1, line:{}}));
  w.colorOf = () => '#ccc'; w.pieceMeta = () => ''; w.pickPiece = () => {};
  w.eval(section('  function paintPieceSum(', '  /* ── Where it is now'));
  w.Orders = {
    loadMaps:async() => {},
    rowFromRecord:(k, l) => Object.assign(makeRow(k), {state:l.state || 'pooled'})
  };
  w.specOf = r => { r.spec = {}; return r; };
  w.api = async(name, arg) => {
    calls.reads.push(arg);
    if (arg.op === 'poolList') return {pools:[{poolId:key + '_1', lineKey:key, runId:'run', sheetId:'sheet', state:'placed'}]};
    if (arg.op === 'runGet') return {run:{lines:{[key]:{orderId:'4175152234', state:'pooled'}}}};
    if (arg.op === 'customGet') { if (state.apiRecord instanceof Error) throw state.apiRecord; return {record:state.apiRecord}; }
    if (arg.op === 'customSheetGet') { if (state.cloudRecords instanceof Error) throw state.cloudRecords; return {records:state.cloudRecords}; }
    throw Error('unexpected lookup: ' + arg.op);
  };
  w.eval(section('  async function sheetRead(', '  async function sheetsFor('));
  w.eval(section('  async function lookUp(', '  /** The look-up\'s spinner line put away'));
  return {dom,w,d,row,rec,item,rows,state,calls,bar:() => d.getElementById('owPcSum'),close:() => dom.window.close()};
}
async function main() {
  let cases = 0;
  {
    const f = fixture();
    try {
      f.w.paintPieceSum();
      assert(!f.bar().hidden, 'an ordinary sent piece has its row with its sent controls, as a special custom piece has its own');
      assert.match(f.bar().textContent, /Sent to sheet/);
      const seal = f.bar().querySelector('.seal-sheet');
      assert(seal, 'actual shared SENT TO SHEET seal is rendered inline');
      assert.equal(seal.dataset.at, String(at));
      assert.match(seal.getAttribute('aria-label'), /by Paul/);
      assert.doesNotMatch(seal.querySelector('svg').textContent, /Paul/, 'signer stays in shared hover detail');
      assert.equal(f.bar().querySelector('.seal.pending'), null, 'old saved decision is never an invisible pending stamp');
      assert.equal(f.bar().querySelector('[data-cu-complete], [data-cu-print], [data-cu-reopen], [data-cu-done]'), null, 'sent designs offer no hand-completion controls');
      f.bar().querySelector('[data-cu-open-sheet]').click(); f.bar().querySelector('[data-cu-history]').click(); f.bar().querySelector('[data-cu-view-designs]').click();
      assert.deepEqual(f.calls.views, ['sheet','timeline']); assert.equal(f.calls.designs[0],f.item);
      const snapshot = JSON.stringify(f.rec);
      f.w.paintPieceSum(); assert.equal(f.bar().querySelector('.seal-sheet'), seal, 'unchanged redraw keeps the historical seal node');
      assert.equal(JSON.stringify(f.rec), snapshot, 'rendering changes no saved signer, date, or history');
      cases++;
    } finally { f.close(); }
  }
  {
    const f = fixture();
    try {
      f.rec.stamps.unshift({how:'print',at:at-60000,by:'Seth'}); f.w.paintPieceSum();
      // (the bar draws one small seal, the latest the card above does not show, and the Timeline through History holds the rest, since 2 Oct:
      //  each seal once on the overview. Display only: every seal stays in the record, none is replaced, and each is reachable)
      assert.equal(f.bar().querySelectorAll('.seal').length,1, 'one seal in the bar');
      assert(f.bar().querySelector('.seal-sheet'), 'the latest is the sent seal');
      assert.deepEqual(f.rec.stamps.map(x => [x.how,x.by]),[['print','Seth'],['sheet','Paul']], 'previous QR history remains in the record beside the sent seal');
      f.bar().querySelector('[data-cu-history]').click(); assert.equal(f.calls.views[f.calls.views.length - 1],'timeline','and is reachable through History');
      f.rec.stamps.push({how:'engraveApproved',at:at+60000,by:'Paul'}); f.w.paintPieceSum();
      assert.equal(f.bar().querySelectorAll('.seal').length,1, 'still one seal in the bar: the latest real history');
      assert(f.bar().querySelector('.seal-engraveApproved'), 'additional real history becomes the seal shown');
      assert.deepEqual(f.rec.stamps.map(x => x.how),['print','sheet','engraveApproved'], 'every earlier seal is still in the record, none replaced');
      assert(f.bar().querySelector('[data-cu-history]'), 'History still holds them all');
      cases++;
    } finally { f.close(); }
  }
  {
    const f = fixture();
    try {
      f.state.files = []; f.w.paintPieceSum();
      assert(f.bar().querySelector('[data-cu-open-sheet]')); assert(f.bar().querySelector('[data-cu-history]'));
      assert.equal(f.bar().querySelector('[data-cu-view-designs]'),null,'unavailable source designs are not offered as a broken action');
      f.w.W.key = 'another-order'; f.bar().querySelector('[data-cu-open-sheet]').click(); f.bar().querySelector('[data-cu-history]').click();
      assert.deepEqual(f.calls.views,[], 'buttons from a superseded order cannot navigate the new order');
      cases++;
    } finally { f.close(); }
  }
  {
    const f = fixture();
    try {
      f.state.sent = null; f.state.item = null; f.w.paintPieceSum();
      assert(f.bar().hidden, 'an unfinished send with no durable decision shows no fictional historical seal');
      cases++;
    } finally { f.close(); }
  }
  {
    const f = fixture();
    try {
      const [old] = await f.w.lookUp('4175152234',() => {});
      assert.equal(old._customSentDecision, f.rec, 'outside-pull lookup preserves the original sent decision');
      assert.equal(f.w.B.maps.customSent[old.key], f.rec);
      assert.equal(old.spec.customDone, undefined, 'send is not falsely read as hand completion');
      f.rows.set(old.key,old); f.w.W.key = old.key; f.state.sent = null;
      f.w.paintPieceSum();
      assert.equal(f.calls.lookup.fallback,old,'popup lookup passes its historical row to Review');
      assert(f.bar().querySelector('.seal-sheet'),'outside-pull popup shows the same saved signature');
      cases++;
    } finally { f.close(); }
  }
  {
    const f = fixture();
    try {
      f.state.apiRecord = {how:'button',state:'complete',completedAt:at,completedBy:'Seth',stamps:[{how:'button',at,by:'Seth'}]};
      const [old] = await f.w.lookUp('4175152234',() => {});
      assert.equal(old.spec.customDone, f.state.apiRecord, 'actual hand completion still loads'); assert.equal(old._customSentDecision,undefined);
      f.row.spec.customDone = f.state.apiRecord; f.state.sent = null; f.item.decided = false; f.item.done = true; f.item.record = f.state.apiRecord;
      f.w.paintPieceSum();
      assert(f.bar().querySelector('.seal-button')); assert(f.bar().querySelector('[data-cu-print]'));
      // the Complete Order button in its done state, and no Reopen in the order window (it stays in the Review tab)
      assert.equal(f.bar().querySelector('[data-cu-done]').textContent, 'Completed'); assert(f.bar().querySelector('[data-cu-done]').disabled);
      assert.equal(f.bar().querySelector('[data-cu-reopen]'), null, 'no Reopen button in the order window'); assert(![...f.bar().querySelectorAll('button')].some(b => /^\s*Reopen\s*$/.test(b.textContent)));
      f.bar().querySelector('[data-cu-print]').click(); assert.equal(f.calls.prints[0],f.item,'historical completed action remains functional');
      cases++;
    } finally { f.close(); }
  }
  {
    const f = fixture();
    try {
      f.state.apiRecord = {state:'open',how:'button',stamps:[{how:'button',at,by:'Seth'}]};
      const [old] = await f.w.lookUp('4175152234',() => {});
      assert.equal(f.w.B.maps.customKept[old.key], f.state.apiRecord, 'reopened hand-completion history is retained');
      assert.equal(old.spec.customDone,undefined); assert.equal(old._customSentDecision,undefined);
      f.state.apiRecord = Error('temporarily offline'); const [again] = await f.w.lookUp('4175152234',() => {});
      assert.equal(again.spec.customDone,undefined,'failed optional history lookup invents no completion');
      cases++;
    } finally { f.close(); }
  }
  {
    const f = fixture();
    try {
      // Use Review's actual saved-decision adapter too: a historical row is not
      // present in the current pull, and its original signer must still appear.
      f.w.customKey = r => 'ord:custom:' + r.order.receiptId;
      f.w.fmtT = t => String(t);
      f.w.Orders.statePill = () => ['ok','On a sheet'];
      f.w.eval(section('  const infoItems = new Map();', '  /** Special lines that ask nothing'));
      const [old] = await f.w.lookUp('4175152234',() => {});
      f.rows.set(old.key,old); f.w.W.key = old.key; f.state.sent = null;
      f.w.Review.customItemFor = (k, fallback) => f.w.sentItemFor(fallback || f.rows.get(k));
      f.w.paintPieceSum();
      const seal = f.bar().querySelector('.seal-sheet');
      assert(seal, 'actual Review adapter connects a cloud sent record to actual popup shared rendering');
      assert.equal(seal.dataset.at, String(at)); assert.match(seal.getAttribute('aria-label'), /by Paul/);
      assert.equal(f.bar().querySelectorAll('.seal-sheet').length,1, 'adapter and saved stamp produce one historical decision');
      assert.equal(old.spec.customDone,undefined);
      f.w.paintPieceSum(); assert.equal(f.bar().querySelector('.seal-sheet'),seal, 'adapting a new record object does not restamp it');
      cases++;
    } finally { f.close(); }
  }
  for (const legacy of [false,true]) {
    const f = fixture();
    try {
      const decision = {id:legacy ? 'recorded-legacy-receipt' : 'cloud-receipt',at:at-1000,by:'Seth',lines:{[key]:[{f:'cloud-file',i:0}]}}, ck = 'custom:4175152234:CUSTOM';
      const cloud = {ck,rid:'4175152234',phase:'sent',sent:decision,files:[{id:'cloud-file',name:'Original.ai',state:'ready',metal:'gold',qty:1,pieces:1,cloud:{path:'custom/original.pdf',url:'https://saved.test/original.pdf'}}]};
      f.state.apiRecord = null; f.state.cloudRecords = {[ck]:cloud}; f.state.sent = null; f.state.files = [];
      f.w.B.customDesigns = {}; f.w.B.orders = {byKey:new Map()}; f.w.B.pool = {rows:new Map()}; f.w.allSheets = () => [];
      f.w.all = () => f.w.B.customDesigns; f.w.tasks = new Map(); f.w.busy = new Map(); f.w.ver = 0;
      f.w.Session = {schedule(){}}; f.w.redraw = () => {};
      f.w.eval(section('  function hydrate(', '  async function load(opts='));
      f.w.CustomSheet.hydrate = records => f.w.hydrate(records);
      f.w.sentOf = r => Object.values(f.w.B.customDesigns).find(e => Object.hasOwn(e.sent?.lines || {},r.key));
      f.w.decisionOf = r => f.w.sentOf(r)?.sent || r._customSentDecision || f.w.B.maps.customSent[r.key];
      f.w.CustomSheet.sentOf = f.w.sentOf; f.w.CustomSheet.decisionOf = f.w.decisionOf;
      f.w.CustomSheet.legacyKeys = f.w.legacyKeys;
      if (legacy) {
        const api = f.w.api;
        f.w.api = async (name,arg) => {
          const result = await api(name,arg);
          if (arg.op === 'poolList') result.pools[0].custom = true;
          if (arg.op === 'customSheetGet') {
            assert.deepEqual([...arg.legacyLineKeys],[key], 'a regular old order with custom pool metadata requests its actual known legacy line key');
          }
          return result;
        };
      }
      f.w.ckOf = it => String(it.key).replace(/^[a-z]+:/,''); f.w.linesOf = it => it.rows || [it.row]; f.w.openLines = () => []; f.w.notReady = () => '';
      f.w.eval(section('  function cardOf(it) {', '  // the card\'s own designs only'));
      f.w.CustomSheet.cardOf = f.w.cardOf;
      f.w.CustomSheet.stamp = it => JSON.stringify(f.w.cardOf(it)?.files);
      f.w.customKey = r => 'ord:custom:' + r.order.receiptId; f.w.fmtT = t => String(t); f.w.Orders.statePill = () => ['ok','On a sheet'];
      f.w.eval(section('  const infoItems = new Map();', '  /** Special lines that ask nothing'));
      f.w.Review.customItemFor = (k, fallback) => f.w.sentItemFor(fallback || f.rows.get(k));
      const [old] = await f.w.lookUp('4175152234',() => {}); f.rows.set(old.key,old); f.w.W.key = old.key;
      assert.equal(old._customSentDecision,decision, 'new cloud receipt reaches a recovered historical row');
      assert.equal(f.w.B.maps.customSent[old.key],decision); assert.equal(old.spec.customDone,undefined);
      const read = f.calls.reads.filter(x => x.op === 'customSheetGet'); assert.equal(read.length,1,'all recovered line keys share one cloud read'); assert.deepEqual([...read[0].lineKeys],[key]);
      assert.deepEqual([...read[0].legacyLineKeys],legacy ? [key] : [],'legacy discovery targets custom pool rows only');
      assert.equal(f.w.B.customDesigns[ck].sent.id,decision.id); assert.equal(f.w.B.customDesigns[ck].sent.by,'Seth');
      assert.equal(f.w.B.orders.byKey.size,0,'looking at an old order never adopts it into live intake or re-pools it');
      if(legacy){assert.equal(old.spec.special,undefined,'ordinary legacy custom designs need no special/custom-order classification');assert.equal(f.w.B.pool.rows.size,0,'legacy lookup does not add pool copies');}
      f.w.paintPieceSum();
      const seal = f.bar().querySelector('.seal-sheet'); assert.equal(seal.dataset.at,String(decision.at)); assert.match(seal.getAttribute('aria-label'),/by Seth/);
      const designs = f.bar().querySelector('[data-cu-view-designs]'); assert(designs,'cloud-only source files expose a working View designs action'); designs.click();
      assert.equal(f.calls.designs[0].record.id,decision.id); assert(f.bar().querySelector('[data-cu-open-sheet]')); assert(f.bar().querySelector('[data-cu-history]'));
      cases++;
    } finally { f.close(); }
  }
  {
    const f = fixture();
    try {
      f.state.apiRecord = null;
      f.state.cloudRecords = {
        pending:{ck:'pending',rid:'4175152234',phase:'pending',sent:{id:'not-complete',at,by:'Paul',lines:{[key]:[]}},files:[]},
        otherOrder:{ck:'other-order',rid:'4175423829',phase:'sent',sent:{id:'other',at,by:'Seth',lines:{[key]:[]}},files:[]},
        otherLine:{ck:'other-line',rid:'4175152234',phase:'sent',sent:{id:'other-line',at,by:'Seth',lines:{'4175152234_2':[]}},files:[]}
      };
      const [old] = await f.w.lookUp('4175152234',() => {});
      assert.equal(old._customSentDecision,undefined,'pending, different-order and unrelated-line receipts cannot earn a sent seal');
      assert.equal(f.calls.hydration.length,0,'unrelated source files are not loaded into this historical popup');
      cases++;
    } finally { f.close(); }
  }
  {
    const f = fixture();
    try {
      f.state.apiRecord = null; const api = f.w.api, timeout = f.w.setTimeout;
      f.w.setTimeout = (fn,ms,...args) => timeout(fn,ms === 12000 ? 0 : ms,...args);
      f.w.api = (name,arg) => arg.op === 'customSheetGet' ? new Promise(() => {}) : api(name,arg);
      const [old] = await f.w.lookUp('4175152234',() => {});
      assert.equal(old.state,'pooled','an unresponsive optional send-history read cannot strand the order view');
      assert.equal(old._customSentDecision,undefined); assert.equal(f.calls.hydration.length,0);
      cases++;
    } finally { f.close(); }
  }
  {
    const f = fixture();
    try {
      const current = {id:'new-authoritative',at:at+1000,by:'Seth',lines:{[key]:[{f:'file',i:0}]}};
      f.state.cloudRecords = {current:{ck:'custom:4175152234',rid:'4175152234',phase:'sent',sent:current,files:[{id:'file'}]}};
      let resolve; const api = f.w.api;
      f.w.api = (name,arg) => arg.op === 'customGet' ? new Promise(r => { resolve = r; }) : api(name,arg);
      const pending = f.w.lookUp('4175152234',() => {}); for(let i=0;i<30 && !resolve;i++)await Promise.resolve();
      assert(resolve); resolve({record:f.rec}); const [old] = await pending;
      assert.equal(old._customSentDecision,current,'a slow legacy customGet reply cannot replace the current authoritative send');
      assert.equal(f.w.B.maps.customSent[old.key].by,'Seth'); assert.equal(f.w.B.maps.customSent[old.key].id,'new-authoritative');
      cases++;
    } finally { f.close(); }
  }
  for (const sameOrder of [true,false]) {
    const f = fixture();
    try {
      f.state.sent = null; f.item.decided = false;
      let resolve; f.w.CustomSheet.send = () => new Promise(r => { resolve = r; });
      f.w.paintSend(f.row); const button = f.d.querySelector('#owSendSheet'), pending = button.onclick();
      if (!sameOrder) { const newer = makeRow('4175423829_2'); f.rows.set(newer.key,newer); f.w.W.key = newer.key; }
      const note = f.d.createElement('span'); note.className = 'toast bad'; note.dataset.msg = 'original order upload delayed'; f.d.querySelector('#toasts').append(note);
      resolve(); await pending;
      assert.equal(f.calls.notices.length, sameOrder ? 1 : 0, 'late send notice is confined to the original order');
      assert.equal(button.disabled,false,'send button releases its lock after settlement');
      cases++;
    } finally { f.close(); }
  }
  console.log(`custom-send-popup-resilience: ${cases} production rendering/lookup/race cases passed`);
}
main().catch(e => { console.error(e); process.exitCode = 1; });
