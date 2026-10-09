(function (root, factory) {
  'use strict';
  var exported = factory();
  if (typeof module === 'object' && module.exports) module.exports = exported;
  else root.BritesConciergeMemory = exported;
})(typeof window !== 'undefined' ? window : globalThis, function () {
  'use strict';

  var MAX_ROW = 2000, MAX_BATCH = 65536, MAX_CHUNKS = 12;
  var firebaseRestores = new WeakMap();
  function firebaseConfig(value) {
    if (!value || typeof value !== 'object' || Array.isArray(value) || typeof value.apiKey !== 'string' || !/^[A-Za-z0-9_-]{10,256}$/.test(value.apiKey) || !/^[a-z0-9-]{4,100}$/.test(value.projectId || '') || typeof value.authDomain !== 'string' || !/^[a-z0-9.-]{4,253}$/i.test(value.authDomain)) return null;
    var result = {};
    ['apiKey', 'projectId', 'authDomain', 'appId', 'storageBucket', 'messagingSenderId', 'measurementId'].forEach(function (key) { if (typeof value[key] === 'string' && value[key].length <= 512) result[key] = value[key]; });
    return result;
  }
  function restoreFirebase(env, suppliedConfig, suppliedVersion, later, cancel) {
    var config = firebaseConfig(suppliedConfig), version = suppliedVersion || '9.23.0';
    if (!config || !/^\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(version)) return Promise.reject(new Error('account_restore_configuration_invalid'));
    var prior = firebaseRestores.get(env);
    if (prior) return prior.projectId === config.projectId && prior.version === version ? prior.promise : Promise.reject(new Error('account_restore_project_mismatch'));
    function script(file) {
      return new Promise(function (resolve, reject) {
        if (!env.document?.createElement || !env.document.head?.appendChild) { reject(new Error('account_restore_unavailable')); return; }
        var source = 'https://www.gstatic.com/firebasejs/' + version + '/' + file;
        var existing = Array.from(env.document.querySelectorAll?.('script[src]') || []).find(function (node) { return node.src === source; });
        var node = existing || env.document.createElement('script'), finished = false, timer;
        function complete(error) {
          if (finished) return; finished = true; cancel(timer);
          if (existing) { node.removeEventListener?.('load', loaded); node.removeEventListener?.('error', failed); } else node.onload = node.onerror = null;
          if (error) { if (!existing) node.remove?.(); reject(new Error('account_restore_unavailable')); } else resolve();
        }
        function loaded() { complete(); } function failed() { complete(true); }
        if (existing) { node.addEventListener?.('load', loaded, { once: true }); node.addEventListener?.('error', failed, { once: true }); }
        else { node.src = source; node.async = false; node.referrerPolicy = 'no-referrer'; node.onload = loaded; node.onerror = failed; }
        timer = later(function () { complete(true); }, 12000); if (!existing) env.document.head.appendChild(node);
      });
    }
    var promise = (async function () {
      if (!env.firebase || typeof env.firebase.initializeApp !== 'function') await script('firebase-app-compat.js');
      if (!env.firebase || !Array.isArray(env.firebase.apps) || typeof env.firebase.initializeApp !== 'function') throw new Error('account_restore_unavailable');
      var apps = env.firebase.apps, matching = apps.find(function (app) { return app?.options?.projectId === config.projectId; });
      if (apps.length && !matching) throw new Error('account_restore_project_mismatch');
      if (typeof env.firebase.auth !== 'function') await script('firebase-auth-compat.js');
      if (typeof env.firebase.auth !== 'function') throw new Error('account_restore_unavailable');
      // The SDK restores this origin's existing session. It never signs a
      // shopper in, creates a credential, or changes their persistence policy.
      matching = env.firebase.apps.find(function (app) { return app?.options?.projectId === config.projectId; });
      if (env.firebase.apps.length && !matching) throw new Error('account_restore_project_mismatch');
      return matching || env.firebase.initializeApp(config);
    })();
    firebaseRestores.set(env, { projectId: config.projectId, version: version, promise: promise });
    promise.catch(function () { if (firebaseRestores.get(env)?.promise === promise) firebaseRestores.delete(env); });
    return promise;
  }
  function bytes(value) {
    var text = typeof value === 'string' ? value : JSON.stringify(value), total = 0;
    for (var point of text) { var code = point.codePointAt(0); total += code < 128 ? 1 : code < 2048 ? 2 : code < 65536 ? 3 : 4; }
    return total;
  }
  function redact(value, hostRedact) {
    var text = typeof value === 'string' ? value : '';
    if (hostRedact) { try { var result = hostRedact(text); if (typeof result === 'string') text = result; } catch (_) {} }
    return text
      .replace(/\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/gi, '[private contact]')
      .replace(/\+?\d(?:[ ()-]*\d){9,18}\b/g, '[private number]')
      .replace(/\bBearer\s+[A-Za-z0-9._~+\/-]+=*/gi, 'Bearer [private credential]')
      .replace(/\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b/g, '[private credential]')
      .replace(/\b(?:sk-|AIza)[A-Za-z0-9_.-]{15,}/g, '[private credential]')
      .replace(/\b((?:password|passcode|api[ _-]?key|secret|access[ _-]?token|refresh[ _-]?token)\s*(?:is|:|=)\s*)[^\s,;]+/gi, '$1[private credential]')
      .replace(/https?:\/\/[^\s]+/gi, function (value) { try { var url = new URL(value); return url.origin + url.pathname; } catch (_) { return '[private link]'; } })
      .replace(/\b\d{1,6}\s+(?:[A-Za-z][A-Za-z'-]*\s+){0,5}(?:street|st\.?|avenue|ave\.?|road|rd\.?|lane|ln\.?|drive|dr\.?|boulevard|blvd\.?)\b/gi, '[private address]')
      .replace(/[\u0000-\u001f\u007f-\u009f\u202a-\u202e\u2066-\u2069]/g, ' ').trim();
  }
  function rows(value, hostRedact) {
    return (Array.isArray(value) ? value : []).flatMap(function (row) {
      if (!row || !['user', 'assistant'].includes(row.role) || typeof row.content !== 'string') return [];
      var text = redact(row.content, hostRedact), result = [];
      while (text) {
        var end = Math.min(MAX_ROW, text.length);
        if (end < text.length && /[\uD800-\uDBFF]/.test(text.charAt(end - 1))) end--;
        result.push({ role: row.role, content: text.slice(0, end) }); text = text.slice(end);
      }
      return result;
    });
  }
  function mark(row) {
    var text = row.role + ':' + row.content, a = 2166136261, b = 5381;
    for (var i = 0; i < text.length; i++) { a = Math.imul(a ^ text.charCodeAt(i), 16777619); b = Math.imul(b, 33) ^ text.charCodeAt(i); }
    return (a >>> 0).toString(36) + '.' + (b >>> 0).toString(36) + '.' + text.length;
  }
  function safeHandles(value) { return Array.from(new Set((Array.isArray(value) ? value : []).filter(function (v) { return typeof v === 'string' && /^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(v) && v.length <= 180; }))).slice(0, 12); }
  function safePreferences(value, hostRedact) {
    var result = {};
    ['style', 'occasion', 'recipient', 'relationship', 'metal', 'material', 'color', 'colour', 'category', 'meaning', 'theme', 'intent', 'budget', 'maxPrice', 'currency', 'budgetCurrency'].forEach(function (key) {
      if (!value || !Object.prototype.hasOwnProperty.call(value, key)) return;
      if (typeof value[key] === 'string' && (!['currency', 'budgetCurrency'].includes(key) || /^[A-Z]{3}$/.test(value[key]))) result[key] = redact(value[key], hostRedact).slice(0, 120);
      else if (['budget', 'maxPrice'].includes(key) && typeof value[key] === 'number' && Number.isFinite(value[key]) && value[key] >= 0) result[key] = Math.min(value[key], 100000);
    });
    return result;
  }
  function randomId(env) {
    try { var values = new Uint32Array(4); env.crypto.getRandomValues(values); return Array.from(values, function (v) { return v.toString(36); }).join('_'); } catch (_) { return Date.now().toString(36) + '_' + Math.random().toString(36).slice(2); }
  }
  function create(options) {
    options = options || {};
    var env = options.environment || (typeof window !== 'undefined' ? window : globalThis);
    var now = options.now || Date.now, later = options.setTimeout || env.setTimeout.bind(env), cancel = options.clearTimeout || env.clearTimeout.bind(env);
    var transport = options.fetch || (env.fetch && env.fetch.bind(env));
    var storage; try { storage = options.storage || env.sessionStorage; } catch (_) {}
    var storagePrefix = options.storageKey || 'brites-concierge-cloud-v1';
    var endpoint = String(options.apiBase || '').replace(/\/$/, '') + '/api/concierge-memory';
    var syncInterval = Math.max(1000, options.syncIntervalMs || 60000), maxJournalBytes = Math.max(8192, Math.min(1048576, options.maxJournalBytes || 524288));
    var hostRedact = options.redactText || function (text) { return env.BritesStorefrontBridge?.redactRequestText?.(text) || text; };
    var uid = null, user = null, epoch = 0, disposed = false, paused = false, auth = null, unsubscribe = null, customUnsubscribe = null, customAccount = null, accountNotified = false, notifiedPending = false;
    var authGate = null, authGateResolve = null, authGateTimer = null, authPending = false;
    var restorePromise = null, restorePending = false, restoreBlocked = false, restoredApp = null, cleanupPending = false;
    var pollTimer = null, syncTimer = null, syncFlight = null, accountRead = null, generation = null, ready = false, requests = new Set();
    var stream = randomId(env), nextSeq = 0, pending = [], sourceMarks = [], imported = new Map(), chunks = new Map(), lastBrowse = '', lastError = '', capacity = false;
    var lookupSerial = 0, searchLimited = false, searchCursor = null, searchQuery = '', searchPages = 0;

    function resetSearch() { lookupSerial++; searchLimited = false; searchCursor = null; searchQuery = ''; searchPages = 0; }

    function report(code) { lastError = code; try { options.onError?.({ code: code, uid: uid }); } catch (_) {} }
    function notifyAccount(previous) { notifiedPending = restorePending || authPending; try { options.onAccountChanged?.({ uid: uid, previousUid: previous, signedIn: !!uid, identityPending: notifiedPending }); } catch (_) {} }
    function journal() { return { schema: 1, uid: uid, stream: stream, nextSeq: nextSeq, generation: generation, pending: pending, sourceMarks: sourceMarks.slice(-4096), lastBrowse: lastBrowse, cleanupPending: cleanupPending }; }
    function journalSize() { return bytes(journal()); }
    function persist() {
      if (!uid || !storage) return;
      try { storage.setItem(storagePrefix + ':' + uid, JSON.stringify(journal())); } catch (_) { report('local_queue_unavailable'); }
    }
    function loadJournal() {
      if (!storage || !uid) return;
      try {
        var held = JSON.parse(storage.getItem(storagePrefix + ':' + uid) || 'null');
        if (!held || held.schema !== 1 || held.uid !== uid || bytes(held) > maxJournalBytes || !/^[a-z0-9_.-]{1,70}$/i.test(held.stream || '') || !Number.isSafeInteger(held.nextSeq) || held.nextSeq < 0 || !Array.isArray(held.pending) || held.pending.length > 1024) return;
        var clean = held.pending.map(function (chunk) { return cleanChunk(chunk); });
        if (clean.some(function (chunk) { return !chunk; })) return;
        stream = held.stream; nextSeq = held.nextSeq; pending = clean;
        generation = typeof held.generation === 'string' || held.generation === 0 ? held.generation : null;
        cleanupPending = held.cleanupPending === true;
        sourceMarks = (Array.isArray(held.sourceMarks) ? held.sourceMarks : []).filter(function (value) { return typeof value === 'string' && /^[a-z0-9]+\.[a-z0-9]+\.[0-9]+$/.test(value); }).slice(-4096);
        lastBrowse = typeof held.lastBrowse === 'string' ? held.lastBrowse.slice(0, 3000) : '';
      } catch (_) {}
    }
    function cleanChunk(value) {
      if (!value || !/^[A-Za-z0-9_.-]{1,96}$/.test(value.id || '') || !['conversation', 'browse'].includes(value.kind) || !Number.isSafeInteger(value.at) || value.at <= 0) return null;
      var result = { id: value.id, kind: value.kind, at: value.at };
      if (value.kind === 'conversation') {
        if (!Array.isArray(value.messages) || value.messages.length < 1 || value.messages.length > 12) return null;
        result.messages = rows(value.messages, hostRedact); if (result.messages.length !== value.messages.length) return null;
      } else {
        if (!['product', 'collection', 'catalogue'].includes(value.pageKind)) return null;
        result.pageKind = value.pageKind; result.handles = safeHandles(value.handles);
      }
      var preferences = safePreferences(value.preferences, hostRedact); if (Object.keys(preferences).length) result.preferences = preferences;
      return bytes(result) < MAX_BATCH - 1024 ? result : null;
    }
    function nextChunk(kind, data) { return Object.assign({ id: stream + '.' + nextSeq++, kind: kind, at: Math.max(1, Math.floor(now())) }, data); }
    function queueRows(values, preferences) {
      var accepted = 0;
      for (var index = 0; index < values.length;) {
        var part = values.slice(index, index + 12), chunk = nextChunk('conversation', { messages: part });
        if (preferences && Object.keys(preferences).length) chunk.preferences = preferences;
        while (part.length > 1 && (bytes(chunk) > MAX_BATCH - 2048 || journalSize() + bytes(chunk) + (accepted + part.length) * 40 > maxJournalBytes)) { part.pop(); chunk.messages = part; }
        pending.push(chunk);
        if (journalSize() + (accepted + part.length) * 40 > maxJournalBytes || pending.length > 1024) { pending.pop(); nextSeq--; capacity = true; break; }
        accepted += part.length; index += part.length;
      }
      return accepted;
    }
    function capture() {
      if (!uid || paused || typeof options.getLocalMemory !== 'function') return { queued: 0, remaining: 0 };
      var value; try { value = options.getLocalMemory() || {}; } catch (_) { return { queued: 0, remaining: 0 }; }
      var values = rows(Array.isArray(value) ? value : value.transcript || value.history, hostRedact), marks = values.map(mark);
      var overlap = Math.min(sourceMarks.length, marks.length);
      while (overlap && !sourceMarks.slice(-overlap).every(function (value, i) { return value === marks[i]; })) overlap--;
      var start = overlap, accepted = 0;
      while (start < values.length) {
        var importedCount = imported.get(marks[start]) || 0;
        if (importedCount) { imported.set(marks[start], importedCount - 1); start++; accepted++; continue; }
        var end = start + 1;
        while (end < values.length && !(imported.get(marks[end]) > 0)) end++;
        var added = queueRows(values.slice(start, end), safePreferences(value.preferences, hostRedact)); start += added; accepted += added;
        if (start < end) break;
      }
      sourceMarks = marks.slice(0, overlap + accepted).slice(-4096);
      if (accepted) persist();
      return { queued: accepted, remaining: values.length - overlap - accepted };
    }
    function schedule(delay) {
      if (disposed || !uid || paused) return;
      cancel(syncTimer); syncTimer = later(function () { syncTimer = null; sync().catch(function () {}); }, delay == null ? syncInterval : delay);
    }
    function append(value, metadata) {
      if (disposed || !uid || paused) return { queued: 0, remaining: 0, signedIn: false };
      var result;
      if (typeof options.getLocalMemory === 'function') result = capture();
      else { var values = rows(value, hostRedact), accepted = queueRows(values); result = { queued: accepted, remaining: values.length - accepted }; persist(); }
      if (metadata?.pageKind) browse(metadata);
      if (result.remaining) report('local_queue_full');
      if (!syncTimer) schedule(capacity && ready ? 0 : syncInterval);
      return result;
    }
    function browse(value) {
      if (disposed || !uid || paused || !value || !['product', 'collection', 'catalogue'].includes(value.pageKind)) return false;
      var data = { pageKind: value.pageKind, handles: safeHandles(value.handles || (value.handle ? [value.handle] : [])) }, stamp = JSON.stringify(data);
      if (stamp === lastBrowse) return false;
      var preferences = safePreferences(value.preferences, hostRedact); if (Object.keys(preferences).length) data.preferences = preferences;
      var chunk = nextChunk('browse', data); pending.push(chunk);
      if (journalSize() > maxJournalBytes || pending.length > 1024) { pending.pop(); nextSeq--; capacity = true; report('local_queue_full'); schedule(0); return false; }
      lastBrowse = stamp; persist(); if (!syncTimer) schedule(); return true;
    }
    function stale(requestEpoch) { return disposed || requestEpoch !== epoch || !uid; }
    async function request(body, requestEpoch, requestOptions) {
      requestOptions = requestOptions || {};
      var callerSignal = requestOptions.signal;
      if (stale(requestEpoch) || callerSignal?.aborted || paused && body.action !== 'clear' || !transport || !user || typeof user.getIdToken !== 'function') return null;
      var Controller = options.AbortController || env.AbortController, controller = Controller ? new Controller() : null;
      var stopped = false, timeout, resolveCancelled;
      var cancelled = new Promise(function (resolve) { resolveCancelled = resolve; });
      function stopRequest() { if (stopped) return; stopped = true; resolveCancelled(null); controller?.abort(); }
      callerSignal?.addEventListener?.('abort', stopRequest, { once: true });
      controller?.signal.addEventListener?.('abort', stopRequest, { once: true });
      if (controller) requests.add(controller);
      try {
        var activeUser = user;
        var operation = (async function () {
          var token = await activeUser.getIdToken(); if (stale(requestEpoch) || stopped) return null;
          if (typeof token !== 'string' || !token) throw new Error('account_token_unavailable');
          var response = await transport(endpoint, { method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + token }, body: JSON.stringify(body), signal: controller?.signal, credentials: 'omit' });
          if (stale(requestEpoch) || stopped) return null;
          var result = await response.json(); if (stale(requestEpoch) || stopped) return null;
          if (!response.ok || result?.ok !== true) {
            if (response.status === 409 && result?.resetRequired) return { resetRequired: true, generation: result.generation };
            throw new Error(response.status === 401 || response.status === 403 ? 'account_access_unavailable' : 'cloud_memory_unavailable');
          }
          return result;
        })();
        var duration = Math.max(1000, Math.min(30000, options.requestTimeoutMs || 12000));
        if (Number.isFinite(requestOptions.timeoutMs)) duration = Math.max(1, Math.min(duration, requestOptions.timeoutMs));
        var deadline = new Promise(function (_, reject) { timeout = later(function () { reject(new Error('cloud_memory_unavailable')); stopRequest(); }, duration); });
        return await Promise.race([operation, deadline, cancelled]);
      } finally { cancel(timeout); callerSignal?.removeEventListener?.('abort', stopRequest); controller?.signal.removeEventListener?.('abort', stopRequest); if (controller) requests.delete(controller); }
    }
    function mergeRead(result) {
      if (!result) return null;
      var readChunks = (Array.isArray(result.chunks) ? result.chunks : []).map(cleanChunk).filter(Boolean);
      readChunks.forEach(function (chunk) {
        if (!chunks.has(chunk.id)) (chunk.messages || []).forEach(function (row) { var key = mark(row); imported.set(key, (imported.get(key) || 0) + 1); });
        chunks.set(chunk.id, chunk);
      });
      trimCache();
      var combined = new Map(readChunks.concat(pending).map(function (chunk) { return [chunk.id, chunk]; }));
      var chronological = Array.from(combined.values()).sort(function (a, b) { return a.at - b.at || a.id.localeCompare(b.id); });
      var hydrated = { chunks: readChunks, history: chronological.flatMap(function (chunk) { return chunk.messages || []; }), preferences: safePreferences(result.preferences, hostRedact), purchases: result.purchases || { status: 'not_requested' }, nextCursor: result.nextCursor || null, generation: generation, uid: uid };
      try { options.onHydrate?.(hydrated); } catch (_) {}
      return hydrated;
    }
    async function read(value) {
      if (disposed || !uid || paused) return null;
      var spec = typeof value === 'string' ? { query: value } : value || {}, requestEpoch = epoch;
      var body = { action: 'read', query: redact(spec.query || '', hostRedact).slice(0, 300), limit: Math.max(1, Math.min(20, Math.floor(spec.limit || 20))) };
      if (typeof spec.cursor === 'string' && spec.cursor.length <= 1000) body.cursor = spec.cursor;
      try {
        var result = await request(body, requestEpoch, { signal: spec.signal, timeoutMs: spec.timeoutMs }); if (!result || stale(requestEpoch)) return null;
        var nextGeneration = typeof result.generation === 'string' || result.generation === 0 ? result.generation : 0;
        var generationChanged = generation != null && generation !== nextGeneration;
        if (generationChanged || result.resetRequired) {
          pending = []; sourceMarks = []; imported.clear(); chunks.clear(); lastBrowse = ''; capacity = false;
          try { options.onReset?.({ uid: uid, reason: 'history_cleared' }); } catch (_) {}
          // Cleared local words must not be resurrected by the next background capture.
          try { var local = options.getLocalMemory?.() || {}; sourceMarks = rows(local.transcript || local.history || local, hostRedact).map(mark).slice(-4096); } catch (_) {}
        }
        generation = nextGeneration; ready = true; persist(); lastError = '';
        var hydrated = mergeRead(result.resetRequired ? { chunks: [], preferences: {} } : result);
        if (hydrated) { hydrated.generationChanged = generationChanged; hydrated.resetRequired = result.resetRequired === true; }
        return hydrated;
      } catch (error) { if (!stale(requestEpoch)) report(error.message === 'account_access_unavailable' ? error.message : 'cloud_memory_unavailable'); return null; }
    }
    async function sync() {
      if (disposed || !uid || paused) return { saved: 0, signedIn: false };
      if (syncFlight) return syncFlight;
      var requestEpoch = epoch;
      var work = (async function () {
        if (!ready) { await read(); if (!ready || stale(requestEpoch)) { schedule(); return { saved: 0, pending: pending.length }; } }
        capture(); if (!pending.length) { capacity = false; schedule(); return { saved: 0, pending: 0 }; }
        var batch = [], body = { action: 'sync', generation: generation, chunks: batch };
        for (var chunk of pending.slice(0, MAX_CHUNKS)) { batch.push(chunk); if (bytes(body) > MAX_BATCH) { batch.pop(); break; } }
        if (!batch.length) { report('local_queue_full'); schedule(); return { saved: 0, pending: pending.length }; }
        try {
          var result = await request(body, requestEpoch); if (!result || stale(requestEpoch)) return { saved: 0 };
          if (result.resetRequired) { ready = false; await read(); schedule(); return { saved: 0, resetRequired: true }; }
          var ids = new Set(batch.map(function (chunk) { return chunk.id; }));
          pending = pending.filter(function (chunk) { return !ids.has(chunk.id); });
          batch.forEach(function (chunk) { chunks.set(chunk.id, chunk); });
          trimCache();
          capacity = false; lastError = ''; persist(); capture();
          schedule(Math.max(syncInterval, Number(result.syncAfterMs) || 0));
          try { options.onSynced?.({ saved: result.saved || 0, duplicates: result.duplicates || 0, pending: pending.length }); } catch (_) {}
          return { saved: result.saved || 0, duplicates: result.duplicates || 0, pending: pending.length };
        } catch (error) { if (!stale(requestEpoch)) { report(error.message === 'account_access_unavailable' ? error.message : 'cloud_memory_unavailable'); schedule(); } return { saved: 0, pending: pending.length, retry: true }; }
      })();
      syncFlight = work;
      try { return await work; } finally { if (syncFlight === work) syncFlight = null; }
    }
    function cachedRelevant(query, maximum, lookupOptions) {
      var terms = redact(query || '', hostRedact).toLowerCase().match(/[a-z0-9]{3,}/g) || [], stop = new Set('the this that what which how can could please you your have did earlier before remember about with from there again'.split(' '));
      terms = terms.filter(function (term) { return !stop.has(term); });
      var combined = new Map(Array.from(chunks.values()).concat(pending).map(function (chunk) { return [chunk.id, chunk]; }));
      var excluded = new Set(rows([{ role: 'user', content: lookupOptions?.excludeContent || '' }], hostRedact).map(mark));
      var values = Array.from(combined.values()).sort(function (a, b) { return a.at - b.at || a.id.localeCompare(b.id); }).flatMap(function (chunk) { return chunk.messages || []; }).filter(function (row) { return row.role !== 'user' || !excluded.has(mark(row)); });
      var ranked = values.map(function (row, index) { return { row: row, index: index, score: terms.reduce(function (score, term) { return score + (row.content.toLowerCase().includes(term) ? 1 : 0); }, 0) }; }).filter(function (item) { return !terms.length || item.score; }).sort(function (a, b) { return b.score - a.score || b.index - a.index; }).slice(0, Math.max(1, Math.min(20, maximum || 6))).sort(function (a, b) { return a.index - b.index; });
      return ranked.map(function (item) { return { role: item.row.role, content: item.row.content.slice(0, 600) }; });
    }
    async function relevant(query, maximum, lookupOptions) {
      lookupOptions = lookupOptions || {};
      var safeQuery = redact(query || '', hostRedact).slice(0, 300), requestEpoch = epoch, serial = ++lookupSerial;
      searchLimited = false; searchCursor = null; searchQuery = safeQuery; searchPages = 0;
      if (disposed || paused || lookupOptions.signal?.aborted) return [];
      if (!uid || !safeQuery) return cachedRelevant(safeQuery, maximum, lookupOptions);
      var cursor = typeof lookupOptions.cursor === 'string' && lookupOptions.cursor.length <= 1000 ? lookupOptions.cursor : null;
      var pages = Math.max(1, Math.min(4, Math.floor(lookupOptions.maxPages || options.maxLookupPages || 4)));
      var deadline = now() + Math.max(1000, Math.min(12000, lookupOptions.timeoutMs || options.lookupTimeoutMs || 12000));
      function active() { return serial === lookupSerial && !stale(requestEpoch) && !paused && !lookupOptions.signal?.aborted; }
      for (var index = 0; index < pages && active(); index++) {
        var remaining = deadline - now();
        if (remaining <= 0) { searchLimited = true; break; }
        var result = await read({ query: safeQuery, limit: 20, cursor: cursor, signal: lookupOptions.signal, timeoutMs: remaining });
        if (!active()) return [];
        searchPages = index + 1;
        if (!result) { searchLimited = true; break; }
        cursor = result.nextCursor; searchCursor = cursor; searchLimited = !!cursor;
        // A clear invalidates the cursor's archive generation. Do not keep
        // scanning the old archive or present its context after the reset.
        if (result.resetRequired || result.generationChanged) { searchLimited = true; searchCursor = null; break; }
        if (cachedRelevant(safeQuery, maximum, lookupOptions).length || !cursor) break;
      }
      return active() ? cachedRelevant(safeQuery, maximum, lookupOptions) : [];
    }
    async function purchases() {
      if (disposed || !uid || paused) return { status: 'unavailable', items: [] };
      var requestEpoch = epoch;
      try { var result = await request({ action: 'purchases' }, requestEpoch); return result?.purchases || { status: 'unavailable', items: [] }; } catch (_) { if (!stale(requestEpoch)) report('purchase_history_unavailable'); return { status: 'unavailable', items: [] }; }
    }
    async function clear(value) {
      if (disposed || !uid) return false;
      var cleanupOnly = value?.cleanup === true;
      if (cleanupOnly && generation == null) { await read(); if (generation == null || !uid) return false; }
      paused = true; cancel(syncTimer); requests.forEach(function (controller) { controller.abort(); }); requests.clear();
      var requestEpoch = ++epoch, cleared = false; syncFlight = null; resetSearch();
      try {
        var result = await request(cleanupOnly ? { action: 'clear', cleanup: true, generation: generation } : { action: 'clear' }, requestEpoch); if (!result || stale(requestEpoch) || result.resetRequired) return false;
        cleared = true; cleanupPending = result.cleanupPending === true;
        if (!cleanupOnly) {
          pending = []; sourceMarks = []; imported.clear(); chunks.clear(); lastBrowse = ''; capacity = false;
          generation = typeof result.generation === 'string' || result.generation === 0 ? result.generation : generation; ready = true;
          try { options.onReset?.({ uid: uid, reason: 'history_cleared' }); } catch (_) {}
          try { var local = options.getLocalMemory?.() || {}; sourceMarks = rows(local.transcript || local.history || local, hostRedact).map(mark).slice(-4096); } catch (_) {}
        }
        persist();
        var rounds = Math.max(0, Math.min(8, Number(value?.maxCleanupBatches ?? options.clearCleanupBatches ?? 4) || 0));
        for (var index = 0; cleanupPending && index < rounds; index++) {
          result = await request({ action: 'clear', cleanup: true, generation: generation }, requestEpoch);
          if (!result || stale(requestEpoch) || result.resetRequired) break;
          cleanupPending = result.cleanupPending === true; persist();
        }
        if (cleanupPending) report('history_cleanup_pending'); else lastError = '';
        return true;
      } catch (_) { if (!stale(requestEpoch)) { if (cleared) { cleanupPending = true; persist(); report('history_cleanup_pending'); } else report('cloud_memory_unavailable'); } return cleared; }
      finally { if (!stale(requestEpoch)) { paused = false; schedule(); } }
    }
    function setAccount(nextUser) {
      if (disposed) return;
      // The studio's guest handoff uses a custom token, which can report
      // isAnonymous:false. Existing Brites member accounts have an email;
      // the transferred no-email guest must retain same-tab memory only.
      var identified = nextUser && typeof nextUser.email === 'string' && nextUser.email.trim().length > 0;
      var nextUid = identified && typeof nextUser.uid === 'string' && nextUser.uid.length > 0 && nextUser.uid.length <= 128 && !/[\u0000-\u001f\u007f]/.test(nextUser.uid) && !nextUser.isAnonymous && typeof nextUser.getIdToken === 'function' ? nextUser.uid : null;
      user = nextUid ? nextUser : null;
      if (nextUid === uid) { if (!accountNotified || !uid && notifiedPending && !restorePending && !authPending) { accountNotified = true; notifyAccount(null); } return; }
      persist(); var previous = uid; epoch++; resetSearch(); cancel(syncTimer); syncTimer = null;
      requests.forEach(function (controller) { controller.abort(); }); requests.clear(); syncFlight = null;
      uid = nextUid; generation = null; ready = false; paused = false; cleanupPending = false; stream = randomId(env); nextSeq = 0; pending = []; sourceMarks = []; imported.clear(); chunks.clear(); lastBrowse = ''; lastError = ''; capacity = false;
      if (uid) loadJournal(); accountNotified = true; notifyAccount(previous);
      accountRead = null;
      if (uid) { capture(); var accountEpoch = epoch; accountRead = read().then(function (result) { if (!stale(accountEpoch)) schedule(); return result; }); }
    }
    function refreshIdentity() {
      if (disposed) return;
      if (restorePending) return restorePromise;
      if (restoreBlocked) return Promise.resolve(null);
      var found = null;
      try {
        if (typeof options.getAuth === 'function') found = options.getAuth();
        else if (options.auth) found = options.auth;
        else if (restoredApp) found = typeof restoredApp.auth === 'function' ? restoredApp.auth() : env.firebase.auth(restoredApp);
        else if (typeof options.getCurrentUser !== 'function' && typeof options.subscribeAccount !== 'function' && env.firebase && typeof env.firebase.auth === 'function' && (!Array.isArray(env.firebase.apps) || env.firebase.apps.length)) found = env.firebase.auth();
      } catch (_) {}
      if (found !== auth) {
        finishAuthGate();
        try { unsubscribe?.(); } catch (_) {} unsubscribe = null; auth = found;
        if (auth && typeof auth.onAuthStateChanged === 'function') {
          authPending = true; authGate = new Promise(function (resolve) { authGateResolve = resolve; });
          authGateTimer = later(finishAuthGate, Math.max(1000, Math.min(30000, options.requestTimeoutMs || 12000)));
          if (!accountNotified && !auth.currentUser) setAccount(null);
          try { unsubscribe = auth.onAuthStateChanged(function (nextUser) { finishAuthGate(); setAccount(nextUser); }, function () { finishAuthGate(); setAccount(null); report('account_access_unavailable'); }); } catch (_) { finishAuthGate(); }
        }
      }
      try { if (!authPending || auth?.currentUser || typeof options.getCurrentUser === 'function') setAccount(typeof options.getCurrentUser === 'function' ? options.getCurrentUser() : auth?.currentUser || customAccount || null); } catch (_) { setAccount(null); }
      return authPending ? authGate.then(function () { return accountRead; }) : accountRead || Promise.resolve(null);
    }
    function poll() { if (disposed) return; refreshIdentity(); pollTimer = later(poll, Math.max(250, options.authPollMs || 1000)); }
    function status() { return { uid: uid, signedIn: !!uid, ready: ready, identityPending: restorePending || authPending, pending: pending.length, pendingBytes: uid ? journalSize() : 0, capacity: capacity, cleanupPending: cleanupPending, searchLimited: searchLimited, searchCursor: searchCursor, searchQuery: searchQuery, searchPages: searchPages, error: lastError }; }
    function dispose() {
      if (disposed) return; capture(); persist(); disposed = true; epoch++; resetSearch(); cancel(syncTimer); cancel(pollTimer); finishAuthGate();
      try { unsubscribe?.(); customUnsubscribe?.(); } catch (_) {}
      requests.forEach(function (controller) { controller.abort(); }); requests.clear();
      env.removeEventListener?.('pagehide', onPageHide);
    }
    function onPageHide() { if (!disposed && uid) { capture(); persist(); } }
    env.addEventListener?.('pagehide', onPageHide);
    if (typeof options.subscribeAccount === 'function') { try { customUnsubscribe = options.subscribeAccount(function (value) { customAccount = value; setAccount(value); }); } catch (_) {} }
    function trimCache() { while (chunks.size > 240) chunks.delete(chunks.keys().next().value); }
    function finishAuthGate() { authPending = false; cancel(authGateTimer); authGateTimer = null; var resolve = authGateResolve; authGateResolve = null; resolve?.(); }
    if (options.firebaseConfig && !options.getAuth && !options.auth && !options.getCurrentUser && !options.subscribeAccount) {
      restorePending = true; setAccount(null);
      restorePromise = restoreFirebase(env, options.firebaseConfig, options.firebaseVersion, later, cancel).then(function (app) { restorePending = false; if (disposed) return null; restoredApp = app; return refreshIdentity(); }, function (error) { restorePending = false; restoreBlocked = true; if (!disposed) { setAccount(null); report(/^account_restore_(?:configuration_invalid|project_mismatch|unavailable)$/.test(error?.message || '') ? error.message : 'account_restore_unavailable'); } return null; });
    }
    poll();
    function readyAccount() { return refreshIdentity() || Promise.resolve(null); }
    return { append: append, browse: browse, sync: sync, read: read, relevant: relevant, cachedRelevant: cachedRelevant, purchases: purchases, clear: clear, ready: readyAccount, refreshIdentity: refreshIdentity, status: status, dispose: dispose };
  }
  return { create: create, redactText: redact };
});
