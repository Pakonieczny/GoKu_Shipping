'use strict';

// Protected sandbox operator helper. Every target SKU comes from the sandbox
// queue, and every cache collection is fixed here. No provider, auth/refresh,
// write, automatic match, or customer-record serializer is imported.
const crypto = require('node:crypto');
const { FieldPath } = require('@google-cloud/firestore');
const LIMITS = Object.freeze({ queue: 100, ranks: 12, tokens: 12, aliasResults: 10, listingIds: 24,
  products: 100, receipts: 100, transactionsPerDocument: 1000, transactionsPerRequest: 10000,
  variations: 20, witnesses: 96, groupsPerRank: 8, witnessesPerPair: 3, responseBytes: 200000,
  budgetMs: 12000, maximumBudgetMs: 15000, cursorBytes: 8192, cursorLifetimeMs: 8 * 86400000 });
const METHODS = new Set(['aliasCandidates', 'listingVerification', 'receiptIdentityPage']);
const OPTION_KEYS = {
  aliasCandidates: new Set(['method', 'ranks', 'budgetMs']),
  listingVerification: new Set(['method', 'ranks', 'listingIds', 'budgetMs']),
  receiptIdentityPage: new Set(['method', 'ranks', 'fromSec', 'untilSec', 'cursor', 'limit', 'budgetMs'])
};
const STATE_FIELDS = ['rank', 'sku', 'productId', 'status'];
const LISTING_FIELDS = ['listingId', 'title', 'state', 'active', 'listingUrl', 'images', 'lastSyncedAt'];
const IMAGE_FIELDS = ['images'];
const PRICING_FIELDS = ['original_saved', 'original_inventory.products', 'chain_type', 'updated_at'];
const RECEIPT_FIELDS = ['created_timestamp', 'raw.transactions'];
const own = (o, k) => !!o && Object.prototype.hasOwnProperty.call(o, k);
const object = o => !!o && typeof o === 'object' && !Array.isArray(o);
const list = x => Array.isArray(x) ? x : [];
const digest = x => crypto.createHash('sha256').update(x).digest('hex');
const id = x => typeof x === 'string' && /^[1-9]\d{0,29}$/.test(x) ? x :
  Number.isSafeInteger(x) && x > 0 ? String(x) : null;
const listingId = x => { const n = id(x); return n && n.length >= 3 && n.length <= 20 ? n : null; };
const epochSeconds = x => Number.isSafeInteger(x) && x >= 0 && x <= 4102444800 ? x : null;
const sku = x => typeof x === 'string' && /^[\p{L}\p{N} ._'&()+-]{1,120}$/u.test(x) && x.trim() ? x.trim() : null;
const boolean = x => typeof x === 'boolean' ? x : null;
function failure(code) { const e = new Error(code); e.code = code; return e; }
function safeError(error) {
  const codes = new Set(['TIME_LIMIT', 'SOURCE_INVALID', 'SEEDS_INVALID', 'SEEDS_TOO_MANY', 'RANK_NOT_UNRESOLVED',
    'CURSOR_INVALID', 'CURSOR_EXPIRED', 'CURSOR_FOREIGN', 'CURSOR_SOURCE_CHANGED', 'CURSOR_SOURCE_MISSING',
    'CURSOR_KEY_REQUIRED', 'INVALID_OPTIONS', 'INVALID_WINDOW', 'INVALID_LISTING_IDS', 'SANDBOX_REQUIRED',
    'CACHE_NOT_FOUND', 'CACHE_ACCESS_DENIED', 'CACHE_RESOURCE_LIMIT', 'CACHE_INDEX_UNAVAILABLE', 'CACHE_UNAVAILABLE', 'CACHE_READ_FAILED']);
  if (codes.has(error?.code)) return error.code;
  const grpc = { 4: 'TIME_LIMIT', 5: 'CACHE_NOT_FOUND', 7: 'CACHE_ACCESS_DENIED', 8: 'CACHE_RESOURCE_LIMIT', 9: 'CACHE_INDEX_UNAVAILABLE', 14: 'CACHE_UNAVAILABLE' };
  if (typeof error?.code === 'number' && own(grpc, error.code)) return grpc[error.code];
  const named = { 'permission-denied': 'CACHE_ACCESS_DENIED', unavailable: 'CACHE_UNAVAILABLE',
    'deadline-exceeded': 'TIME_LIMIT', 'failed-precondition': 'CACHE_INDEX_UNAVAILABLE', 'resource-exhausted': 'CACHE_RESOURCE_LIMIT' };
  return typeof error?.code === 'string' && own(named, error.code) ? named[error.code] : 'CACHE_READ_FAILED';
}
function millis(x) {
  try {
    const n = typeof x === 'number' ? x : x instanceof Date ? x.getTime() :
      typeof x?.toMillis === 'function' ? x.toMillis() : Number.isSafeInteger(x?.seconds ?? x?._seconds) ? (x.seconds ?? x._seconds) * 1000 : null;
    return typeof n === 'number' && Number.isFinite(n) && n >= 0 && n <= 4102444800000 ? n : null;
  } catch { return null; }
}
function titleWords(value) {
  const text = typeof value === 'string' ? value.slice(0, 2000) : '';
  return ['necklace', 'charm', 'earrings', 'bracelet', 'extender', 'beady', 'beaded', 'cable'].filter(word => new RegExp('\\b' + word + '\\b', 'i').test(text));
}
function photoTokens(images) {
  const found = new Set();
  for (const image of list(images).slice(0, 20)) {
    for (const value of typeof image === 'string' ? [image] : [image?.url_fullxfull, image?.url_570xN, image?.url]) {
      try {
        const u = new URL(value);
        if (u.protocol !== 'https:' || u.username || u.password || u.port || u.hostname !== 'i.etsystatic.com') continue;
        const m = u.pathname.match(/\b(\d{5,20}_[a-z0-9]{3,8})\.(?:jpg|jpeg|png|webp)$/i);
        if (m) found.add(m[1]);
      } catch { /* Unknown URLs never escape. */ }
    }
  }
  return [...found].slice(0, 4);
}
function selectedOptions(values) {
  const safe = [];
  for (const v of list(values).slice(0, LIMITS.variations)) {
    if (!object(v)) continue;
    const rawName = typeof v.formatted_name === 'string' ? v.formatted_name : typeof v.property_name === 'string' ? v.property_name : '';
    const rawValue = typeof v.formatted_value === 'string' ? v.formatted_value : typeof v.value === 'string' ? v.value : '';
    const name = rawName.trim().toLowerCase();
    const value = rawValue.trim();
    let kind = 'other', safeValue = null;
    if (['metal', 'metal choice', 'material'].includes(name)) {
      kind = 'metal';
      if (/^(?:sterling silver|(?:14k|14 karat|14 kt) (?:solid gold|gold filled|gold fill|rose gold filled|rosegold filled)|gold filled|rose gold filled|gold vermeil|silver)$/i.test(value)) safeValue = value.toLowerCase();
    } else if (['length', 'chain length', 'necklace length', 'bracelet length'].includes(name)) {
      kind = 'length';
      if (/^\d{1,2}(?:\.\d{1,2})?\s*(?:["\u2033]|inches|inch|in|cm)$/i.test(value)) safeValue = value.toLowerCase();
    } else if (['chain', 'chain type', 'chain style'].includes(name)) {
      kind = 'chain';
      if (/^(?:cable|beady|beaded|regular)(?: chain)?$/i.test(value)) safeValue = value.toLowerCase();
    } else if (['engraving', 'engrave', 'engraving option'].includes(name)) {
      kind = 'engraving_toggle';
      if (/^(?:none|no|yes|engraved|no engraving|with engraving|without engraving)$/i.test(value)) safeValue = value.toLowerCase();
    }
    safe.push({ propertyId: id(v.property_id), valueId: id(v.value_id), kind, value: safeValue,
      unreturnedValuePresent: !!rawValue && safeValue === null });
  }
  return safe;
}

function createHistoricalLookup({ db, namespace = 'Brites_Growth_Sandbox', cursorSecret, now = Date.now } = {}) {
  const secretReady = typeof cursorSecret === 'string' && cursorSecret.length >= 16;
  const key = secretReady ? crypto.createHash('sha256').update('brites-history-v1\0' + cursorSecret).digest() : null;
  const hashPrivate = value => key ? crypto.createHmac('sha256', key).update(value).digest('hex') : null;
  function response(method) {
    return { schema: 1, method, mode: 'cache_only_read_only', checkedAt: now(),
      status: 'observed', identityBindingPerformed: false, originalRanksCountsChanged: false,
      customerDataReturned: false, providerCalls: 0, storeWrites: 0, limits: LIMITS };
  }
  async function bounded(task, deadline) {
    const remaining = deadline - Date.now();
    if (remaining <= 0) throw failure('TIME_LIMIT');
    let timer;
    try {
      return await Promise.race([Promise.resolve().then(task), new Promise((_, reject) => {
        timer = setTimeout(() => reject(failure('TIME_LIMIT')), remaining);
      })]);
    } finally { clearTimeout(timer); }
  }
  async function seeds(options, deadline) {
    const snap = await bounded(() => db.collection(namespace + '_Queue').where('rank', '<=', 100)
      .orderBy('rank').select(...STATE_FIELDS).limit(LIMITS.queue).get(), deadline);
    if (!snap || !Array.isArray(snap.docs) || snap.docs.length > LIMITS.queue) throw failure('SOURCE_INVALID');
    const result = [];
    for (const doc of snap.docs) {
      const d = doc.data();
      if (!object(d) || !Number.isInteger(d.rank) || d.rank < 1 || d.rank > 100) throw failure('SEEDS_INVALID');
      if (d.productId || d.status === 'complete') continue;
      if (typeof d.sku !== 'string' || d.sku.length > 500) throw failure('SEEDS_INVALID');
      const tokens = [...new Set(d.sku.split(/[\n,;|]+/).map(sku))];
      if (!tokens.length || tokens.some(t => !t)) throw failure('SEEDS_INVALID');
      result.push({ rank: d.rank, tokens });
    }
    result.sort((a, b) => a.rank - b.rank);
    if (new Set(result.map(r => r.rank)).size !== result.length) throw failure('SEEDS_INVALID');
    if (result.length > LIMITS.ranks) throw failure('SEEDS_TOO_MANY');
    if (options.ranks != null) {
      if (!Array.isArray(options.ranks) || !options.ranks.length || options.ranks.length > LIMITS.ranks ||
          options.ranks.some(rank => !Number.isInteger(rank) || !result.some(r => r.rank === rank))) throw failure('RANK_NOT_UNRESOLVED');
    }
    const selected = options.ranks == null ? result : result.filter(r => options.ranks.includes(r.rank));
    const originalTokens = [...new Set(selected.flatMap(r => r.tokens))];
    const tokens = [...new Set(originalTokens.map(t => t.toUpperCase()))];
    if (originalTokens.length > LIMITS.tokens) throw failure('SEEDS_TOO_MANY');
    return { rows: selected, tokens, digest: digest(JSON.stringify(selected)), queueRowsObserved: snap.docs.length };
  }
  function seal(payload, seedDigest) {
    if (!key) throw failure('CURSOR_KEY_REQUIRED');
    const nonce = crypto.randomBytes(12), cipher = crypto.createCipheriv('aes-256-gcm', key, nonce);
    cipher.setAAD(Buffer.from(namespace + '\0' + seedDigest));
    const value = { ...payload, version: 1, namespace, digest: seedDigest, expiresAt: now() + LIMITS.cursorLifetimeMs };
    const bytes = Buffer.concat([cipher.update(JSON.stringify(value), 'utf8'), cipher.final()]);
    const token = 'h1.' + Buffer.concat([nonce, cipher.getAuthTag(), bytes]).toString('base64url');
    if (token.length > LIMITS.cursorBytes) throw failure('CURSOR_INVALID');
    return token;
  }
  function open(token, seedDigest, fromSec, untilSec) {
    if (!key) throw failure('CURSOR_KEY_REQUIRED');
    if (typeof token !== 'string' || token.length > LIMITS.cursorBytes || !/^h1\.[A-Za-z0-9_-]+$/.test(token)) throw failure('CURSOR_INVALID');
    let d;
    try {
      const bytes = Buffer.from(token.slice(3), 'base64url');
      if (bytes.length < 29) throw Error('short');
      const decipher = crypto.createDecipheriv('aes-256-gcm', key, bytes.subarray(0, 12));
      decipher.setAAD(Buffer.from(namespace + '\0' + seedDigest));
      decipher.setAuthTag(bytes.subarray(12, 28));
      d = JSON.parse(Buffer.concat([decipher.update(bytes.subarray(28)), decipher.final()]).toString('utf8'));
    } catch { throw failure('CURSOR_INVALID'); }
    if (!object(d) || d.version !== 1 || d.namespace !== namespace || d.digest !== seedDigest || d.fromSec !== fromSec || d.untilSec !== untilSec) throw failure('CURSOR_FOREIGN');
    if (!Number.isSafeInteger(d.expiresAt) || d.expiresAt <= now()) throw failure('CURSOR_EXPIRED');
    if (!['after', 'within'].includes(d.kind) || !id(d.documentId) || epochSeconds(d.createdTimestamp) === null ||
        d.createdTimestamp < fromSec || d.createdTimestamp > untilSec ||
        (d.kind === 'within' && (!Number.isSafeInteger(d.offset) || d.offset < 0 || typeof d.transactionsHash !== 'string' || !/^[a-f0-9]{64}$/.test(d.transactionsHash)))) throw failure('CURSOR_INVALID');
    return d;
  }
  async function aliases(seed, deadline, base) {
    const jobs = [];
    if (seed.tokens.length) {
      for (const field of ['sku', 'huggie']) jobs.push({ kind: 'alias_' + field, token: null,
        query: db.collection('Charm_Sku_Aliases').where(field, 'in', seed.tokens).select('listingId', field, 'v').limit(LIMITS.aliasResults) });
      for (const token of seed.tokens) {
        const path = new FieldPath('bySku', token);
        // Alias writer accepts only nonempty SKU strings. A string range avoids
        // relying on caller interpolation or null-filter coercion.
        jobs.push({ kind: 'alias_bySku_key', token, query: db.collection('Charm_Sku_Aliases')
          .where(path, '>', '').orderBy(path).select('listingId', path).limit(LIMITS.aliasResults) });
      }
    }
    const observations = [], candidates = new Map();
    for (let i = 0; i < jobs.length; i += 3) {
      const batch = await Promise.all(jobs.slice(i, i + 3).map(async job => {
        try {
          const snap = await bounded(() => job.query.get(), deadline);
          if (!Array.isArray(snap?.docs) || snap.docs.length > LIMITS.aliasResults) throw failure('SOURCE_INVALID');
          const rows = [];
          for (const doc of snap.docs) {
            const d = doc.data(), targetId = listingId(doc.id);
            if (!object(d) || !targetId || listingId(d.listingId) !== targetId) { rows.push({ status: 'inconsistent_identity' }); continue; }
            const value = job.token ? d.bySku?.[job.token] : d[job.kind === 'alias_huggie' ? 'huggie' : 'sku'];
            if (!sku(value)) { rows.push({ status: 'invalid_alias_value' }); continue; }
            const token = job.token || value.toUpperCase();
            if (!seed.tokens.includes(token)) { rows.push({ status: 'unrelated_alias' }); continue; }
            const ranks = seed.rows.filter(r => r.tokens.some(t => t.toUpperCase() === token)).map(r => r.rank);
            rows.push({ status: 'candidate', listingId: targetId, ranks, lookupKind: job.kind, originalCaseVerified: false,
              historicalPurchaseVerified: false, aliasVersion2: d.v === 2 });
          }
          return { kind: job.kind, token: job.token, status: 'observed', sampleCount: snap.docs.length,
            possiblyCapped: snap.docs.length === LIMITS.aliasResults, rows };
        } catch (e) { return { kind: job.kind, token: job.token, status: 'error', sampleCount: null, possiblyCapped: null, error: safeError(e), rows: [] }; }
      }));
      observations.push(...batch);
      for (const row of batch.flatMap(x => x.rows)) if (row.status === 'candidate') {
        const key = row.listingId + '/' + row.lookupKind + '/' + row.ranks.join(',');
        candidates.set(key, row);
      }
    }
    const ids = [...new Set([...candidates.values()].map(c => c.listingId))];
    const acceptedIds = new Set(ids.slice(0, LIMITS.listingIds));
    return { ...base, status: observations.some(o => o.status === 'error') ? 'partial_or_unavailable' : 'observed',
      unresolvedRanks: seed.rows.map(r => r.rank), queueRowsObserved: seed.queueRowsObserved,
      queries: observations.map(({ rows, ...observation }) => ({ ...observation, inconsistentRows: rows.filter(r => r.status !== 'candidate').length })),
      candidates: [...candidates.values()].filter(c => acceptedIds.has(c.listingId)),
      candidateIdsCapped: ids.length > LIMITS.listingIds, candidateEvidenceOnly: true };
  }
  async function projected(collection, ids, fields, deadline) {
    try {
      const snaps = await bounded(() => db.getAll(...ids.map(n => db.collection(collection).doc(n)), { fieldMask: fields }), deadline);
      if (!Array.isArray(snaps) || snaps.length !== ids.length) throw failure('SOURCE_INVALID');
      return ids.map((n, i) => {
        const s = snaps[i];
        if (!s || s.id !== n || typeof s.exists !== 'boolean') throw failure('SOURCE_INVALID');
        if (!s.exists) return { id: n, status: 'absent', data: null };
        const d = s.data();
        if (!object(d)) throw failure('SOURCE_INVALID');
        return { id: n, status: 'observed', data: d };
      });
    } catch (e) { return ids.map(n => ({ id: n, status: 'error', data: null, error: safeError(e) })); }
  }
  async function listings(options, seed, deadline, base) {
    const ids = [...new Set(options.listingIds)];
    const [catalogue, images, pricing] = await Promise.all([
      projected('EtsyMail_Listings', ids, LISTING_FIELDS, deadline),
      projected('Etsy_Listing_Image_Cache', ids, IMAGE_FIELDS, deadline),
      projected('EtsyPricing_Listings', ids, PRICING_FIELDS, deadline)
    ]);
    return { ...base, status: [...catalogue, ...images, ...pricing].some(x => x.status === 'error') ? 'partial_or_unavailable' : 'observed',
      candidateEvidenceOnly: true, listings: ids.map((n, i) => {
        const c = catalogue[i], im = images[i], p = pricing[i], d = c.data, products = list(p.data?.original_inventory?.products);
        const matches = products.slice(0, LIMITS.products).flatMap(product => {
          const matched = matching(product?.sku, seed);
          return matched.map(m => ({ ...m, etsyProductId: id(product.product_id),
            selectedPropertyIds: list(product.property_values).slice(0, LIMITS.variations).map(v => id(v?.property_id)).filter(Boolean) }));
        });
        return { listingId: n, canonicalUrl: 'https://www.etsy.com/listing/' + n,
          catalogueStatus: c.status, catalogueError: c.error || null,
          catalogueIdentifierAgrees: d ? listingId(d.listingId) === n : null,
          catalogueActive: d ? boolean(d.active) : null,
          catalogueState: d && ['active', 'inactive', 'draft', 'expired', 'sold_out'].includes(d.state) ? d.state : null,
          titleWords: d ? titleWords(d.title) : [], titleFingerprint: d && typeof d.title === 'string' ? hashPrivate(d.title.slice(0, 2000)) : null,
          photoAssetTokens: [...new Set([...photoTokens(d?.images), ...photoTokens(im.data?.images)])].slice(0, 4),
          lastSyncedAt: d ? millis(d.lastSyncedAt) : null, imageCacheStatus: im.status, imageCacheError: im.error || null,
          pricingCacheStatus: p.status, pricingCacheError: p.error || null,
          originalSaved: p.data ? boolean(p.data.original_saved) : null,
          originalInventoryPresent: p.data ? object(p.data.original_inventory) : null,
          originalInventoryMatches: matches,
          originalInventoryInspectionTruncated: p.data ? products.length > LIMITS.products : null,
          currentChainType: p.data && ['regular', 'beady'].includes(p.data.chain_type) ? p.data.chain_type : null,
          currentChainIsHistoricalProof: false };
      }) };
  }
  function matching(value, seed) {
    const raw = sku(value);
    if (!raw) return [];
    return seed.rows.flatMap(row => {
      // When a stored row explicitly contains multiple case spellings, prefer
      // that row's exact token instead of duplicating it as a weaker witness.
      const tokens = row.tokens.includes(raw) ? [raw] : row.tokens.filter(token => raw.toUpperCase() === token.toUpperCase());
      return tokens.map(token => ({ rank: row.rank, originalSkuToken: token, observedSku: raw,
        caseRelation: raw === token ? 'case_sensitive_exact' : 'case_variant_unconfirmed', rankedIdentityConfirmed: false }));
    });
  }
  async function receiptPage(options, seed, deadline, base) {
    if (!key) throw failure('CURSOR_KEY_REQUIRED');
    const fromSec = options.fromSec, untilSec = options.untilSec;
    const cursor = own(options, 'cursor') ? open(options.cursor, seed.digest, fromSec, untilSec) : null;
    const limit = options.limit == null ? LIMITS.receipts : options.limit;
    let docs, terminal = false;
    if (cursor?.kind === 'within') {
      const result = await projected('EtsyMail_Receipts', [cursor.documentId], RECEIPT_FIELDS, deadline);
      if (result[0].status === 'error') throw failure(result[0].error);
      if (result[0].status === 'absent') throw failure('CURSOR_SOURCE_MISSING');
      docs = [{ id: result[0].id, data: () => result[0].data }];
    } else {
      let query = db.collection('EtsyMail_Receipts').where('created_timestamp', '>=', fromSec).where('created_timestamp', '<=', untilSec)
        .orderBy('created_timestamp', 'desc').orderBy(FieldPath.documentId(), 'desc').select(...RECEIPT_FIELDS).limit(limit);
      if (cursor) query = query.startAfter(cursor.createdTimestamp, cursor.documentId);
      const snap = await bounded(() => query.get(), deadline);
      if (!Array.isArray(snap?.docs) || snap.docs.length > limit) throw failure('SOURCE_INVALID');
      docs = snap.docs; terminal = docs.length < limit;
    }
    const witnesses = [], seenWitnesses = new Set(), groups = new Map(), pairCounts = new Map();
    let documentsInspected = 0, transactionsInspected = 0, position = cursor, stopped = false, truncated = false, invalidArrays = 0, nonResumable = false;
    for (const doc of docs) {
      if (Date.now() >= deadline) { stopped = truncated = true; break; }
      const d = doc.data(), docId = id(doc.id), created = epochSeconds(d?.created_timestamp);
      if (!object(d) || !docId || created === null || created < fromSec || created > untilSec) throw failure('SOURCE_INVALID');
      const transactions = d.raw?.transactions;
      const arrayHash = hashPrivate(JSON.stringify(transactions ?? null));
      const start = cursor?.kind === 'within' && cursor.documentId === docId ? cursor.offset : 0;
      if (cursor?.kind === 'within' && (created !== cursor.createdTimestamp || arrayHash !== cursor.transactionsHash)) throw failure('CURSOR_SOURCE_CHANGED');
      if (!Array.isArray(transactions)) {
        invalidArrays++; documentsInspected++;
        stopped = nonResumable = true;
        break;
      }
      if (start > transactions.length) throw failure('CURSOR_SOURCE_CHANGED');
      documentsInspected++;
      let visitedThisDocument = 0, completed = true;
      for (let offset = start; offset < transactions.length; offset++) {
        if (Date.now() >= deadline || transactionsInspected >= LIMITS.transactionsPerRequest || visitedThisDocument >= LIMITS.transactionsPerDocument) {
          position = { kind: 'within', documentId: docId, createdTimestamp: created, offset, transactionsHash: arrayHash };
          stopped = truncated = true; completed = false; break;
        }
        const t = transactions[offset];
        const matches = matching(t?.sku, seed);
        const productId = id(t?.product_id), etsyListingId = listingId(t?.listing_id);
        const proposed = [], proposedIds = new Set();
        for (const match of matches) {
          if (!etsyListingId) continue;
          const pair = match.rank + '/' + etsyListingId + '/' + (productId || 'unknown');
          const distinct = groups.get(match.rank) || new Set();
          const transactionKey = id(t?.transaction_id) ? 'id:' + id(t.transaction_id) : 'offset:' + offset;
          const witnessId = hashPrivate([docId, transactionKey, match.rank, etsyListingId, productId || 'unknown', match.observedSku].join('\0'));
          if (seenWitnesses.has(witnessId) || proposedIds.has(witnessId)) continue;
          if ((!distinct.has(pair) && distinct.size >= LIMITS.groupsPerRank) || witnesses.length + proposed.length >= LIMITS.witnesses) {
            position = { kind: 'within', documentId: docId, createdTimestamp: created, offset, transactionsHash: arrayHash };
            stopped = truncated = true; completed = false; break;
          }
          if ((pairCounts.get(pair) || 0) >= LIMITS.witnessesPerPair) continue;
          proposedIds.add(witnessId);
          proposed.push({ match, pair, distinct, witnessId, witness: { ...match, witnessFingerprint: witnessId,
            etsyListingId, etsyProductId: productId, source: 'stored_receipt_transaction', withinRequestedWindow: true,
            observedTitleWords: titleWords(t.title), titleFingerprint: typeof t.title === 'string' ? hashPrivate(t.title.slice(0, 2000)) : null,
            selectedOptions: selectedOptions(t.variations), variationsInspectionTruncated: list(t.variations).length > LIMITS.variations } });
        }
        if (!completed) break;
        const projectedBytes = Buffer.byteLength(JSON.stringify(witnesses.concat(proposed.map(x => x.witness))));
        if (projectedBytes > LIMITS.responseBytes - 12000) {
          position = { kind: 'within', documentId: docId, createdTimestamp: created, offset, transactionsHash: arrayHash };
          stopped = truncated = true; completed = false; break;
        }
        for (const p of proposed) {
          p.distinct.add(p.pair); groups.set(p.match.rank, p.distinct);
          pairCounts.set(p.pair, (pairCounts.get(p.pair) || 0) + 1);
          seenWitnesses.add(p.witnessId); witnesses.push(p.witness);
        }
        transactionsInspected++; visitedThisDocument++;
      }
      if (!completed) break;
      position = { kind: 'after', documentId: docId, createdTimestamp: created };
    }
    const allRead = !stopped && invalidArrays === 0;
    const exhausted = cursor?.kind === 'within' ? false : terminal && allRead;
    const nextCursor = exhausted || nonResumable ? null : position ? seal({ ...position, fromSec, untilSec }, seed.digest) : null;
    return { ...base, status: stopped || invalidArrays ? 'incomplete' : 'observed', fromSec, untilSec,
      receiptDocumentsObserved: docs.length, receiptDocumentsInspected: documentsInspected,
      transactionsInspected, invalidTransactionArrays: invalidArrays, inspectionTruncated: truncated,
      windowExhausted: invalidArrays ? null : exhausted, nextCursor, witnesses,
      scannedEntireHistory: false, witnessCountsAreOrderCounts: false, exactHistoricalEvidenceIsCurrentShopifyMatch: false,
      note: 'Case variants, aliases and historical selections remain unconfirmed ranked identity evidence.' };
  }
  async function read(options = {}) {
    const suppliedMethod = object(options) && own(options, 'method') ? options.method : 'aliasCandidates';
    const method = METHODS.has(suppliedMethod) ? suppliedMethod : 'invalid', base = response(method);
    try {
      if (namespace !== 'Brites_Growth_Sandbox') throw failure('SANDBOX_REQUIRED');
      if (!object(options) || !METHODS.has(method) || Object.keys(options).some(k => !OPTION_KEYS[method].has(k))) throw failure('INVALID_OPTIONS');
      if (options.budgetMs != null && (!Number.isInteger(options.budgetMs) || options.budgetMs < 10 || options.budgetMs > LIMITS.maximumBudgetMs)) throw failure('INVALID_OPTIONS');
      if (method === 'listingVerification' && (!Array.isArray(options.listingIds) || !options.listingIds.length || options.listingIds.length > LIMITS.listingIds || options.listingIds.some(n => listingId(n) !== n))) throw failure('INVALID_LISTING_IDS');
      if (method === 'receiptIdentityPage' && (epochSeconds(options.fromSec) === null || epochSeconds(options.untilSec) === null || options.fromSec > options.untilSec || options.untilSec > Math.floor(now() / 1000) ||
          (options.limit != null && (!Number.isInteger(options.limit) || options.limit < 1 || options.limit > LIMITS.receipts)))) throw failure('INVALID_WINDOW');
      if (method === 'receiptIdentityPage' && own(options, 'cursor') && (typeof options.cursor !== 'string' || !options.cursor || options.cursor.length > LIMITS.cursorBytes)) throw failure('CURSOR_INVALID');
      const deadline = Date.now() + (options.budgetMs || LIMITS.budgetMs);
      const seed = await seeds(options, deadline);
      if (!seed.rows.length) return { ...base, status: 'no_unresolved_seeds', candidates: [], witnesses: [], nextCursor: null, windowExhausted: null };
      if (method === 'aliasCandidates') return await aliases(seed, deadline, base);
      if (method === 'listingVerification') return await listings(options, seed, deadline, base);
      return await receiptPage(options, seed, deadline, base);
    } catch (e) {
      return { ...base, status: 'unavailable', code: safeError(e), receiptDocumentsObserved: null,
        transactionsInspected: null, windowExhausted: null, nextCursor: null, candidates: [], witnesses: [] };
    }
  }
  return { read };
}

module.exports = { createHistoricalLookup, LIMITS };
