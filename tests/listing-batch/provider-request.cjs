'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync('netlify/functions/geminiImageProxy-background.js', 'utf8');
const cut = name => {
  const start = source.indexOf(`async function ${name}(`);
  const end = source.indexOf('\n}\n', start) + 2;
  assert(start >= 0 && end > start);
  return source.slice(start, end);
};
async function scenario(path, options = {}, stalled = false, ignoresAbort = false, stallsBeforeHeaders = false) {
  const timers = new Map();
  let next = 0, signal;
  const request = vm.runInNewContext(`${cut('readUpstreamJson')}\n${cut('studioFetchWithTimeout')}\n${cut('openAIBatchRequest')}\nopenAIBatchRequest;`, {
    AbortController,
    setTimeout: (fn, ms) => { timers.set(++next, {fn, ms}); return next; },
    clearTimeout: id => timers.delete(id),
    fetch: async (_url, opts) => {
      signal = opts.signal;
      if(stallsBeforeHeaders) return new Promise(()=>{});
      return {ok: true, status: 200, text: () => stalled ? new Promise((_, reject) => {
        if(!ignoresAbort) signal.addEventListener('abort', () => reject(Object.assign(new Error('aborted'), {name: 'AbortError'})), {once: true});
      }) : Promise.resolve('{"id":"test-result"}')};
    },
  });
  const pending = request('test-key', path, options);
  await new Promise(setImmediate);
  if (stalled || stallsBeforeHeaders) {
    assert.equal(timers.size, 1, 'deadline remains active after headers while the response body is unfinished');
    const timer = [...timers.values()][0];
    assert.equal(timer.ms, path === '/files' ? 120000 : 30000);
    timer.fn();
    let rejected;
    pending.catch(error=>{rejected=error;});
    await new Promise(setImmediate);
    assert(rejected,'hard deadline rejects even when cancellation is ignored');
    await assert.rejects(pending, error => error.status === 504 && /timed out/.test(error.message));
    assert.equal(signal.aborted, true, 'timeout aborts the unfinished response');
  } else assert.equal((await pending).id, 'test-result');
  assert.equal(timers.size, 0, 'deadline is cleared after the entire request settles');
}
(async () => {
  await scenario('/files', {method: 'POST'}, true);
  await scenario('/batches/test-job', {}, true);
  await scenario('/batches', {method: 'POST'}, true);
  await scenario('/files', {method: 'POST'}, true, true);
  await scenario('/batches', {method: 'POST'}, true, true);
  await scenario('/files', {method: 'POST'}, false, true, true);
  await scenario('/files', {method: 'POST'});
  await scenario('/batches/test-job');
  const tempDirs=[], fsAsync=require('node:fs/promises');
  let uploadFails=false;
  const upload = vm.runInNewContext(`${cut('uploadOpenAIBatchFile')}\nuploadOpenAIBatchFile;`, {
    require:name=>name==='node:fs/promises' ? {...fsAsync,mkdtemp:async prefix=>{
      const dir=await fsAsync.mkdtemp(prefix);tempDirs.push(dir);return dir;
    }} : require(name), FormData,
    openAIBatchRequest:async (_key,_path,opts)=>{
      const file=opts.body.get('file');
      assert.equal(file.name,'listing-images.jsonl');
      assert.equal(await file.text(),'test JSONL\n');
      if(uploadFails) throw Object.assign(new Error('Upload timed out'),{status:504});
      return {id:'file-test'};
    },
  });
  assert.equal(await upload('test-key',Buffer.from('test JSONL\n'),'test'),'file-test');
  uploadFails=true;
  await assert.rejects(upload('test-key',Buffer.from('test JSONL\n'),'test'),/Upload timed out/);
  for(const dir of tempDirs) await assert.rejects(fsAsync.access(dir),error=>error.code==='ENOENT',
    'temporary input files are removed after success and failure');
  console.log('Batch provider requests: hard full-response deadlines even when cancellation is ignored, file-backed uploads and successful cleanup passed');
})().catch(error => {console.error(error); process.exitCode = 1;});
