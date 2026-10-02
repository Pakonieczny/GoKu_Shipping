'use strict';

// Google offline-conversion migration. A receipt means queued for processing;
// only exact-destination diagnostics can mark the original order as uploaded.
// https://developers.google.com/data-manager/api/devguides/events/google-ads/offline/upgrade/field-mappings
const crypto = require('node:crypto');
const { rowFingerprint } = require('./_britesGrowthReceiptReconciliation');
const CREDENTIAL_DOC = 'config/googleAdsDataManager';
function credentialKey(env){if(!env.FIREBASE_PRIVATE_KEY)throw Error('Server encryption key unavailable.');return Buffer.from(crypto.hkdfSync('sha256',Buffer.from(env.FIREBASE_PRIVATE_KEY.replace(/\\n/g,'\n')),Buffer.from(env.FIREBASE_PROJECT_ID||''),Buffer.from('Brites Data Manager credentials v1'),32));}
function sealCredentials(value,env){const iv=crypto.randomBytes(12),cipher=crypto.createCipheriv('aes-256-gcm',credentialKey(env),iv);cipher.setAAD(Buffer.from(CREDENTIAL_DOC));const encrypted=Buffer.concat([cipher.update(JSON.stringify(value),'utf8'),cipher.final()]);return {version:1,iv:iv.toString('base64'),tag:cipher.getAuthTag().toString('base64'),ciphertext:encrypted.toString('base64')};}
function openCredentials(value,env){if(value?.version!==1)throw Error('Unsupported connection record.');try{const decipher=crypto.createDecipheriv('aes-256-gcm',credentialKey(env),Buffer.from(value.iv,'base64'));decipher.setAAD(Buffer.from(CREDENTIAL_DOC));decipher.setAuthTag(Buffer.from(value.tag,'base64'));return JSON.parse(Buffer.concat([decipher.update(Buffer.from(value.ciphertext,'base64')),decipher.final()]).toString('utf8'));}catch(_){throw Error('Saved connection could not be decrypted. Reconnect after server key rotation.');}}
const BASE = 'https://datamanager.googleapis.com/v1';
const SCOPE = 'https://www.googleapis.com/auth/datamanager';
// https://developers.google.com/data-manager/api/devguides/quickstart/set-up-access
const COMPANION_SCOPE = 'https://www.googleapis.com/auth/cloud-platform';
let grantedScopes = null;
const MIGRATION_ERROR = /Data Manager API|CUSTOMER_NOT_ALLOWLISTED_FOR_THIS_FEATURE/i;
// Google's generic refusal, recorded before its error detail was kept.
const UNDIAGNOSED_ERROR = /^\s*There was a problem with the request\.?\s*$/i;
// A refusal that named a required field missing, which the payload this
// version builds now always carries. Judged by rebuilding the event and
// checking the field is there, not by a clock: the sale was refused for a
// request the application no longer sends.
const CORRECTED_REFUSALS = [{ pattern: /event_source: Required field is missing/i, supplied: event => !!event.eventSource }];
const CHECK_DELAY = 30 * 60 * 1000;
// Google: diagnostics "may take up to 24 hours". A receipt older than this that
// is still unconfirmed is stuck, not slow.
const PROCESSING_WINDOW = 24 * 3600 * 1000;
// Data Manager's ConsentStatus is CONSENT_GRANTED/CONSENT_DENIED; the Google Ads
// API spells it GRANTED/DENIED. Either stored form is sent in Data Manager's.
// https://developers.google.com/data-manager/api/reference/rest/v1/Consent
const DM_CONSENT = { GRANTED: 'CONSENT_GRANTED', DENIED: 'CONSENT_DENIED', CONSENT_GRANTED: 'CONSENT_GRANTED', CONSENT_DENIED: 'CONSENT_DENIED' };
// Google's EU user consent policy covers the EEA, the UK and Switzerland: a
// conversion from those buyers without ad_user_data consent is not reported.
const CONSENT_REGION = new Set('AT BE BG HR CY CZ DK EE FI FR DE GR HU IE IT LV LT LU MT NL PL PT RO SK SI ES SE IS LI NO GB CH'.split(' '));
const needsConsent = row => CONSENT_REGION.has(String(row.buyerCountry || '').toUpperCase()) && !DM_CONSENT[String(row.consent?.adUserData || '').toUpperCase()];
// The value Google should hold is the sale net of refunds already recorded when
// it is sent; later refunds are conversion adjustments.
const netValue = row => Math.round(Math.max(0, Number(row.value) - Math.max(0, Number(row.refundedTotal) || 0)) * 100) / 100;
const fullyRefunded = row => Number(row.value) > 0 && netValue(row) <= 0.005;

function destination(action, login) {
  const match = String(action || '').match(/^customers\/(\d+)\/conversionActions\/(\d+)$/);
  if (!match) throw Error('Configure the full Google Ads conversion action resource.');
  const result = { operatingAccount: { accountType: 'GOOGLE_ADS', accountId: match[1] }, productDestinationId: match[2] };
  if (login) {
    const accountId = String(login).replace(/-/g, '');
    if (!/^\d+$/.test(accountId)) throw Error('Invalid Google Ads manager account.');
    result.loginAccount = { accountType: 'GOOGLE_ADS', accountId };
  }
  return result;
}

function eventFor(row) {
  const raw = String(row.conversionDateTime || '').replace(' ', 'T');
  if (!/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?(?:Z|[+-]\d\d:\d\d)$/.test(raw) || !Number.isFinite(Date.parse(raw))) {
    throw Error('The original conversion timestamp must include its time zone.');
  }
  const value = Number(row.value), currency = String(row.currency || '').toUpperCase();
  if (row.value == null || !Number.isFinite(value) || value < 0 || !/^[A-Z]{3}$/.test(currency)) throw Error('The original conversion value and currency are required.');
  const transactionId = String(row.orderId || '').trim();
  if (!transactionId || transactionId.length > 256) throw Error('The original order ID is required.');
  // Preserve the originally selected click identifier through the migration.
  // An endpoint restriction is not evidence that a different click should win.
  const kind = ['gclid', 'gbraid', 'wbraid'].find(k => row[k]);
  if (!kind) throw Error('No Google click identifier was captured for this order.');
  const event = { transactionId, eventTimestamp: new Date(raw).toISOString(), conversionValue: netValue(row),
    currency, eventSource: 'WEB', adIdentifiers: { [kind]: String(row[kind]) } };
  const consent = {};
  for (const key of ['adUserData', 'adPersonalization']) {
    const status = DM_CONSENT[String(row.consent?.[key] || '').toUpperCase()];
    if (status) consent[key] = status;
  }
  if (Object.keys(consent).length) event.consent = consent;
  return event;
}

function summarizeDiagnostics(data, target) {
  const rows = Array.isArray(data?.requestStatusPerDestination) ? data.requestStatusPerDestination : [];
  // accountType replaced the deprecated `product`; Google accepts either.
  const type = d => d?.operatingAccount?.accountType || d?.operatingAccount?.product;
  const exact = r => type(r?.destination) === 'GOOGLE_ADS' &&
    String(r.destination.operatingAccount.accountId) === target.operatingAccount.accountId &&
    String(r.destination.productDestinationId) === target.productDestinationId;
  // A request receipt identifies its submitted payload, but a sparse or
  // mismatched response is not confirmation of the saved account and action.
  // Keep it unresolved rather than inferring success from a lone status row.
  const found = rows.filter(exact);
  if (found.length !== 1) return { state: 'processing', status: rows.length ? 'DESTINATION_UNCONFIRMED' : 'NO_STATUS', error: 'Diagnostics have not confirmed the exact conversion destination.' };
  const row = found[0], state = row.requestStatus;
  const errors = (row.errorInfo?.errorCounts || []).map(e => `${e.reason || 'Processing error'} (${e.recordCount || e.count || '?'})`).join('; ');
  const warnings = (row.warningInfo?.warningCounts || []).map(e => String(e.reason || 'Processing warning'));
  if (state === 'SUCCESS' && !errors && Number(row.eventsIngestionStatus?.recordCount) === 1) return { state: 'success', status: state, warnings };
  // The enum is FAILED; the diagnostics guide's prose calls it FAILURE.
  if (['FAILED', 'FAILURE', 'PARTIAL_SUCCESS'].includes(state)) return { state: 'failed', status: state, error: errors || 'Google did not process this conversion successfully.', warnings };
  return { state: 'processing', status: state === 'SUCCESS' ? 'SUCCESS with ' + (row.eventsIngestionStatus?.recordCount ?? 'no') + ' event count' : (state || null), error: errors || null, warnings };
}

function createDataManager({ env, fetch, fb, COL, ledger, now = Date.now }) {
  let token = null, expires = 0, connection = null, loadedAt = 0;
  const credentials=()=>connection||{refresh:env.GADS_DATAMANAGER_REFRESH_TOKEN,client:env.GADS_DATAMANAGER_CLIENT_ID||env.GADS_CLIENT_ID,secret:env.GADS_DATAMANAGER_CLIENT_SECRET||env.GADS_CLIENT_SECRET};
  async function loadConnection(){if(configured()&&!connection)return;if(!env.FIREBASE_PRIVATE_KEY||now()-loadedAt<300000)return;const f=fb();if(!f)throw Error('Credential storage unavailable.');const snapshot=await f.db.doc(CREDENTIAL_DOC).get();connection=snapshot.exists?openCredentials(snapshot.data(),env):null;loadedAt=now();}
  async function saveCredentials(value){const next={client:String(value?.client||'').trim(),secret:String(value?.secret||'').trim(),refresh:String(value?.refresh||'').trim()};if(!next.client.endsWith('.apps.googleusercontent.com')||!next.secret||!next.refresh||Object.values(next).some(v=>v.length>4096))throw Error('Complete all three Google connection fields.');const encrypted=sealCredentials(next,env),f=fb();if(!f)throw Error('Credential storage unavailable.');const prior=connection;connection=next;token=null;expires=0;try{await mintToken(true);await f.db.doc(CREDENTIAL_DOC).set({...encrypted,updatedAt:now(),provider:'google_datamanager'});loadedAt=now();return {ok:true,configured:true,storage:'firebase_encrypted'};}catch(e){connection=prior;token=null;expires=0;throw e;}}
  const configured = () => !!(credentials().refresh&&credentials().client&&credentials().secret);
  async function mintToken(requireScope=false) {
    if (!configured()) throw Error('Connect Google Data Manager in Sales before uploading conversions.');
    if (token && now() < expires - 60000) return token;
    const response = await fetch('https://oauth2.googleapis.com/token', { method: 'POST', timeout: 15000,
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: new URLSearchParams({
        client_id: credentials().client,
        client_secret: credentials().secret,
        refresh_token: credentials().refresh, grant_type: 'refresh_token'
      }) });
    const data = await response.json().catch(() => ({}));
    if (!response.ok || !data.access_token) throw Error('Google Data Manager authorization failed. Reconnect its OAuth credentials.');
    if (data.scope) grantedScopes = String(data.scope).split(/\s+/).filter(Boolean);
    if ((requireScope||data.scope) && !String(data.scope||'').split(/\s+/).includes(SCOPE)) throw Error('The credentials do not include the Google Data Manager scope.');
    token = data.access_token; expires = now() + Number(data.expires_in || 3600) * 1000;
    return token;
  }
// Google's error.message is usually generic; error.details names the cause.
// Keeping only the message left a refusal undiagnosable.
function errorDetail(data, status) {
  const error = (data && data.error) || {};
  const parts = [];
  if (error.status) parts.push(error.status);
  for (const detail of error.details || []) {
    if (detail.reason) parts.push('reason:' + detail.reason);
    for (const violation of detail.fieldViolations || [])
      parts.push((violation.field ? violation.field + ': ' : '') + String(violation.description || ''));
    for (const inner of detail.errors || []) {
      const code = inner.errorCode ? Object.keys(inner.errorCode).map(k => k + ':' + inner.errorCode[k]).join(',') : '';
      if (code) parts.push(code);
      else if (inner.message) parts.push(String(inner.message));
    }
  }
  const message = String(error.message || '').trim();
  const named = parts.filter(Boolean).join(' · ');
  if (named && message) return named + ' — ' + message;
  return named || message || ('Google Data Manager returned HTTP ' + status);
}

  async function request(route, body, auth, timeoutMs = 20000) {
    const response = await fetch(BASE + route, { method: body ? 'POST' : 'GET', timeout: Math.max(1, Math.min(20000, timeoutMs)),
      headers: { Authorization: 'Bearer ' + auth, 'Content-Type': 'application/json' },
      ...(body ? { body: JSON.stringify(body) } : {}) });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      const error = Error(errorDetail(data, response.status).slice(0, 500));
      error.definiteRejection = response.status >= 400 && response.status < 500;
      error.status = response.status;
      throw error;
    }
    return data;
  }
  // Receipt-only observation, shared by uploads and the health screen. Never
  // builds a new event or calls events:ingest. A transactional lease prevents
  // overlapping refreshes from confirming and auditing the same receipt twice.
  // Pin the complete original row, including monetary, click, refund, consent
  // and order fields. Only the two lease fields are changed by this observer
  // before the provider read; every other concurrent change invalidates it.
  const receiptRowFingerprint = row => {
    const original = { ...row };
    delete original.dmReconcileLease;
    delete original.dmReconcileLeaseUntil;
    return rowFingerprint(original);
  };
  async function uniqueSavedReceipt(tx, doc, requestId) {
    // Include terminal rows: a confirmed/failed duplicate still means the
    // provider receipt cannot safely be assigned to this one saved order.
    const matches = await tx.get(fb().db.collection(COL.convQueue).where('dmRequestId', '==', requestId).limit(2));
    if (!Array.isArray(matches?.docs) || matches.docs.length !== 1) return false;
    const match = matches.docs[0];
    return match?.id === doc.id && match.exists !== false && typeof match.data === 'function' && match.data()?.dmRequestId === requestId;
  }
  async function checkReceipt(doc, auth, { force = false, timeoutMs = 20000, deadlineAt = Infinity } = {}) {
    const f = fb(), lease = crypto.randomUUID();
    const claim = await f.db.runTransaction(async tx => {
      const snapshot = await tx.get(doc.ref), row = snapshot.exists ? snapshot.data() : null;
      const stamp = now();
      if (!row || !row.dmRequestId || row.uploaded || ['success', 'failed'].includes(row.dmState) || row.dmReconcileLeaseUntil > stamp ||
          (!force && stamp < Number(row.dmNextCheckAt || 0)) || (force && row.dmCheckedAt && stamp - row.dmCheckedAt < 60000)) return null;
      let fingerprint;
      try { fingerprint = receiptRowFingerprint(row); } catch { return null; }
      if (!await uniqueSavedReceipt(tx, doc, row.dmRequestId)) return null;
      tx.update(doc.ref, { dmReconcileLease: lease, dmReconcileLeaseUntil: stamp + 90000 });
      return { row, fingerprint };
    });
    if (!claim) return { state: 'skipped' };
    const { row: claimed, fingerprint: originalFingerprint } = claim;
    // Time spent claiming a contended receipt also consumes the refresh budget.
    // Release only our own lease if no request can start before the deadline.
    if (now() >= deadlineAt) {
      await f.db.runTransaction(async tx => {
        const current = await tx.get(doc.ref);
        if (current.exists && current.data().dmReconcileLease === lease) tx.update(doc.ref, { dmReconcileLease: null, dmReconcileLeaseUntil: 0 });
      });
      return { state: 'skipped' };
    }
    const attempt = Number(claimed.dmChecks || 0) + 1;
    const patch = { dmChecks: attempt, dmCheckedAt: now(), dmNextCheckAt: now() + Math.min(3600000, CHECK_DELAY * Math.pow(1.3, attempt)), dmReconcileLease: null, dmReconcileLeaseUntil: 0 };
    let status;
    try {
      if (!claimed.dmDestination) throw Error('This receipt is missing its original conversion destination.');
      const answer = await request('/requestStatus:retrieve?requestId=' + encodeURIComponent(claimed.dmRequestId), null, auth, Math.min(timeoutMs, deadlineAt - now()));
      status = summarizeDiagnostics(answer, claimed.dmDestination);
      Object.assign(patch, { dmWarnings: status.warnings || [], dmLastStatus: status.status || null, dmCheckError: null, uploadError: status.error || null });
      if (status.state === 'success') Object.assign(patch, { uploaded: true, failed: false, uploadedAt: f.FV.serverTimestamp(), dmState: 'success' });
      else if (status.state === 'failed') Object.assign(patch, { uploaded: false, failed: true, dmState: 'failed' });
    } catch (error) { status = { state: 'processing', error: error.message, httpStatus: error.status || null }; patch.dmCheckError = String(error.message || error).slice(0, 300); }
    const saved = await f.db.runTransaction(async tx => {
      const current = await tx.get(doc.ref), row = current.exists ? current.data() : null;
      if (!row || row.dmReconcileLease !== lease || row.dmRequestId !== claimed.dmRequestId || row.uploaded || ['success', 'failed'].includes(row.dmState) ||
          !(row.dmReconcileLeaseUntil > now())) return false;
      try {
        if (rowFingerprint(row.dmDestination) !== rowFingerprint(claimed.dmDestination) || receiptRowFingerprint(row) !== originalFingerprint) return false;
      } catch { return false; }
      if (!await uniqueSavedReceipt(tx, doc, claimed.dmRequestId)) return false;
      tx.update(doc.ref, patch); return true;
    });
    if (saved && status.state === 'success') {
      try { await ledger({ kind: 'uploadConversions', transport: 'data_manager', requestId: claimed.dmRequestId, count: 1, accepted: 1, ok: true, processingVerified: true, validateOnly: false }); }
      catch (error) { status.error = 'Receipt confirmed; audit write failed: ' + String(error.message).slice(0,200); }
    }
    return saved ? status : { state: 'skipped' };
  }
  async function reconcile({ force = false, limit = 15, maxMs = 12000 } = {}) {
    await loadConnection();
    const result = { receiptOnly: true, attempted: 0, checked: 0, confirmed: 0, failed: 0, processing: 0, errors: [] };
    if (!configured()) return { ...result, blocked: true };
    const f = fb(); if (!f) return { ...result, blocked: true };
    const maximum = Number.isFinite(Number(limit)) ? Math.min(50, Math.max(0, Math.floor(Number(limit)))) : 15;
    const budgetMs = Number.isFinite(Number(maxMs)) ? Math.min(120000, Math.max(0, Number(maxMs))) : 12000;
    if (!maximum || !budgetMs) return { ...result, stopped: !maximum ? 'limit' : 'time_limit' };
    const auth = await mintToken(), started = now();
    const pending = await f.db.collection(COL.convQueue).where('uploaded', '==', false).limit(500).get();
    const candidates = pending.docs.filter(d => { const row = d.data(); return row.dmRequestId && !['success', 'failed'].includes(row.dmState) &&
      !(row.dmReconcileLeaseUntil > now()) && (force ? !(row.dmCheckedAt && now() - row.dmCheckedAt < 60000) : !(now() < Number(row.dmNextCheckAt || 0))); }).sort((a,b) => Number(a.data().dmCheckedAt||0) - Number(b.data().dmCheckedAt||0));
    for (const doc of candidates) {
      if (result.attempted >= maximum || now()-started >= budgetMs) { result.stopped = result.attempted >= maximum ? 'limit' : 'time_limit'; break; }
      result.attempted++;
      try {
        const status = await checkReceipt(doc, auth, { force, deadlineAt: started + budgetMs });
        if (status.state === 'skipped') continue;
        result.checked++;
        if (status.state === 'success') result.confirmed++;
        else if (status.state === 'failed') result.failed++;
        else result.processing++;
        if (status.error) result.errors.push({ orderId: doc.data().orderId, error: status.error });
        if ([401, 403, 429].includes(status.httpStatus)) { result.stopped = status.httpStatus === 429 ? 'rate_limit' : 'authorization'; result.blocked = true; break; }
      } catch (error) { result.errors.push({ orderId: doc.data().orderId, error: String(error.message).slice(0,300) }); }
    }
    result.errors = result.errors.slice(0,10); return result;
  }
  const eligibleLegacyFailure = row => row.failed === true && MIGRATION_ERROR.test(row.uploadError || '') && !row.dmState;
  // Refused because the account is not allowlisted, not because the sale is
  // bad. It becomes uploadable the moment access is granted, with no flag and
  // no one remembering, and it is safe to re-send because it never reached
  // Google: a definite rejection carries no receipt.
  // Rejected by a condition that has since changed: the account was not
  // allowlisted, the reason recorded was too vague to act on, or the payload
  // that was refused is not the payload this version sends. None of these mean
  // the sale is bad, and re-sending is safe because a definite rejection
  // carries no receipt.
  const correctedRefusal = row => {
    const reason = row.uploadError || '';
    const matched = CORRECTED_REFUSALS.filter(c => c.pattern.test(reason));
    if (!matched.length) return false;
    let event; try { event = eventFor(row); } catch (_) { return false; }
    return matched.every(c => c.supplied(event));
  };
  const accountLevelRejection = row => row.dmState === 'failed' && row.dmDefiniteRejection === true
    && !row.dmRequestId && (MIGRATION_ERROR.test(row.uploadError || '') || UNDIAGNOSED_ERROR.test(row.uploadError || '') || correctedRefusal(row));
  const retryable = row => row.dmState === 'failed' && row.dmDefiniteRejection === true && !row.dmRequestId;
  async function run({ ctrl = {}, limit = 50, retryRejected = false } = {}) {
    await loadConnection();
    const started = now();
    const result = { transport: 'data_manager', uploaded: 0, submitted: 0, processing: 0, rejected: 0, validated: 0, validateOnly: !!ctrl.dryRun, errors: [] };
    if (!configured()) return { ...result, blocked: true, error: 'Google requires Data Manager for this conversion integration. Authorize its Data Manager scope and save the connection in Firebase.' };
    const target = destination(env.GADS_CONVERSION_ACTION, env.GADS_LOGIN_CUSTOMER_ID);
    const f = fb(); if (!f) throw Error('Conversion queue storage is unavailable.');
    // Refresh before claiming any order: a missing/expired credential must not
    // consume the order's retry budget or change its processing state.
    const auth = await mintToken(), queue = f.db.collection(COL.convQueue);
    const pending = await queue.where('uploaded', '==', false).limit(500).get();
    const rejected = await queue.where('failed', '==', true).limit(50).get();
    const docs = new Map(pending.docs.map(d => [d.id, d]));
    rejected.docs.filter(d => eligibleLegacyFailure(d.data()) || accountLevelRejection(d.data())).forEach(d => docs.set(d.id, d));
    const rows = [...docs.values()].sort((a, b) => Number(!!b.data().dmRequestId) - Number(!!a.data().dmRequestId));
    const maximum = Math.min(100, Math.max(1, Number(limit) || 50));
    let handled = 0;
    for (const doc of rows) {
      let row = doc.data();
      if (handled >= maximum || now() - started > 120000) break;
      if (row.dmState === 'submitting' || row.dmState === 'submission_unknown' || (row.dmState === 'failed' && !accountLevelRejection(row) && !(retryRejected && retryable(row)))) continue;
      if (row.dmRequestId) {
        result.processing++;
        if (ctrl.dryRun || now() < Number(row.dmNextCheckAt || 0)) continue;
        handled++;
        const checked = await checkReceipt(doc, auth);
        if (checked.state === 'success') { result.uploaded++; result.processing--; }
        if (checked.state === 'failed') { result.rejected++; result.processing--; }
        if (checked.error) result.errors.push({ orderId: row.orderId, error: checked.error });
        continue;
      }
      if (row.uploaded && !eligibleLegacyFailure(row)) continue;
      if (row.failed && !eligibleLegacyFailure(row) && !accountLevelRejection(row) && !(retryRejected && retryable(row))) continue;
      // Refunded in full before it was ever sent: there is no sale for Google
      // to count, and nothing sent means nothing to retract later.
      if (fullyRefunded(row)) {
        if (ctrl.dryRun) continue;
        try {
          const closed = await f.db.runTransaction(async tx => {
            const current = await tx.get(doc.ref), latest = current.exists ? current.data() : null;
            if (!latest || latest.dmRequestId || ['submitting', 'submission_unknown', 'processing', 'success'].includes(latest.dmState) || !fullyRefunded(latest)) return false;
            tx.update(doc.ref, { uploaded: true, failed: false, dmState: 'not_sent_refunded', uploadError: null, dmClosedAt: now() });
            return true;
          });
          if (closed) result.refundedBeforeUpload = (result.refundedBeforeUpload || 0) + 1;
        } catch (error) { result.errors.push({ orderId: row.orderId, error: error.message }); }
        continue;
      }
      let event;
      try { event = eventFor(row); } catch (error) { result.errors.push({ orderId: row.orderId, error: error.message }); continue; }
      handled++;
      const body = { destinations: [target], events: [event], encoding: 'HEX', validateOnly: !!ctrl.dryRun };
      if (ctrl.dryRun) {
        try { await request('/events:ingest', body, auth); result.validated++; }
        catch (error) { result.rejected++; result.errors.push({ orderId: row.orderId, error: error.message }); }
        continue;
      }
      let claimed = false;
      // A contended or failed claim leaves this order untouched; it must not end
      // the run for every order behind it.
      try { claimed = await f.db.runTransaction(async tx => {
        const current = await tx.get(doc.ref); if (!current.exists) return false;
        const latest = current.data();
        if ((latest.dmState && !accountLevelRejection(latest) && !(retryRejected && retryable(latest))) || latest.dmRequestId || (latest.uploaded && !eligibleLegacyFailure(latest))) return false;
        if (latest.failed && !eligibleLegacyFailure(latest) && !accountLevelRejection(latest) && !(retryRejected && retryable(latest))) return false;
        // Guard changes to original financial data (a refund included) between read and claim.
        if (JSON.stringify(eventFor(latest)) !== JSON.stringify(event)) return false;
        tx.update(doc.ref, { dmState: 'submitting', dmStartedAt: now(), dmDestination: target, uploaded: false });
        return true;
      }); } catch (error) { result.errors.push({ orderId: row.orderId, error: error.message }); continue; }
      if (!claimed) continue;
      try {
        const response = await request('/events:ingest', body, auth);
        if (!response.requestId || typeof response.requestId !== 'string') throw Error('Google returned no request receipt; reconcile this order before retrying.');
        await doc.ref.update({ dmState: 'processing', dmRequestId: response.requestId, dmSubmittedAt: now(),
          dmNextCheckAt: now() + CHECK_DELAY, dmChecks: 0, dmFieldWarnings: response.fieldWarnings || [],
          dmValue: event.conversionValue, failed: false, uploadError: null, uploaded: false });
        result.submitted++; result.processing++;
        // The durable receipt is authoritative even if auxiliary audit logging
        // fails. Never turn a confirmed receipt into an unknown submission.
        try { await ledger({ kind: 'dataManagerSubmission', requestId: response.requestId, count: 1, ok: true, processingVerified: false }); }
        catch (error) { result.errors.push({ orderId: row.orderId, error: 'Receipt saved; audit logging failed: ' + error.message }); }
      } catch (error) {
        // Timeout/5xx/missing receipt can mean accepted-but-unconfirmed. Never
        // automatically replay those events or silently mark them uploaded.
        await doc.ref.update({ dmState: error.definiteRejection ? 'failed' : 'submission_unknown', dmDefiniteRejection: !!error.definiteRejection, failed: true,
          uploaded: false, uploadError: error.message, dmFailedAt: now() });
        result.rejected++; result.errors.push({ orderId: row.orderId, error: error.message });
        if ([401, 403, 429].includes(error.status)) break;
      }
    }
    result.errors = result.errors.slice(0, 10);
    return result;
  }
  async function health({ probeScopes = false } = {}) {
    await loadConnection();
    if (probeScopes && configured()) { try { await mintToken(); } catch (error) { /* the rows below report it */ } }
    const info = { configured: configured(), transport: 'data_manager', scopes: grantedScopes, missingScopes: grantedScopes ? [SCOPE, COMPANION_SCOPE].filter(s => !grantedScopes.includes(s)) : null, processing: 0, unknown: 0, confirmed: 0, retryable: 0, awaitingAccess: 0, awaitingRetry: 0, blocked: !configured() };
    const f = fb(); if (!f) return info;
    const queue = f.db.collection(COL.convQueue);
    const rows = await queue.where('uploaded', '==', false).limit(500).get();
    Object.assign(info, { staleProcessing: 0, oldestProcessingAt: null, latestSubmittedAt: null, staleReasons: {}, consentMissing: 0, unsent: 0, oldestUnsentAt: null, unsendable: 0, unsendableReasons: {} });
    rows.forEach(d => { const x = d.data(); if (x.dmRequestId && x.dmState === 'processing') info.processing++; if (['submitting', 'submission_unknown'].includes(x.dmState)) info.unknown++; if (retryable(x)) info.retryable++; if (accountLevelRejection(x)) { if (correctedRefusal(x)) info.awaitingRetry++; else info.awaitingAccess++; }
      if (x.dmRequestId && x.dmState === 'processing') {
        const at = Number(x.dmSubmittedAt) || 0;
        if (at && (!info.oldestProcessingAt || at < info.oldestProcessingAt)) info.oldestProcessingAt = at;
        if (at > (info.latestSubmittedAt || 0)) info.latestSubmittedAt = at;
        if (at && now() - at > PROCESSING_WINDOW) {
          info.staleProcessing++;
          const why = String(x.dmCheckError ? 'the status request failed: ' + x.dmCheckError : x.uploadError ? x.uploadError : x.dmLastStatus ? 'Google still answers ' + x.dmLastStatus : Number(x.dmChecks) ? 'checked ' + x.dmChecks + ' time(s); Google has not finished' : 'its status has never been checked').slice(0, 200);
          info.staleReasons[why] = (info.staleReasons[why] || 0) + 1;
        }
      }
      // Queued and never sent: waiting for the next sync, or unsendable as stored (the run
      // skips those without a trace, so the queue would grow with no stated cause).
      if (!x.dmRequestId && !x.dmState && !x.failed) {
        let bad = null; try { eventFor(x); } catch (error) { bad = String(error.message).slice(0, 200); }
        if (bad) { info.unsendable++; info.unsendableReasons[bad] = (info.unsendableReasons[bad] || 0) + 1; }
        else { info.unsent++; const created = x.createdAt && typeof x.createdAt.toMillis === 'function' ? x.createdAt.toMillis() : Number(x.createdAt) || 0; if (created && (!info.oldestUnsentAt || created < info.oldestUnsentAt)) info.oldestUnsentAt = created; }
      }
      if (needsConsent(x)) info.consentMissing++; });
    if (!env.GADS_CONVERSION_ACTION) return { ...info, blocked: true };
    const confirmed = await queue.where('dmState', '==', 'success').limit(500).get();
    const target = destination(env.GADS_CONVERSION_ACTION, env.GADS_LOGIN_CUSTOMER_ID);
    info.confirmed = confirmed.docs.filter(d => {
      const original = d.data().dmDestination;
      return original?.operatingAccount?.accountId === target.operatingAccount.accountId && original?.productDestinationId === target.productDestinationId;
    }).length;
    // A capped read is a lower bound, never a total.
    info.confirmedComplete = confirmed.docs.length < 500;
    return info;
  }
  return { run, health, reconcile, configured, saveCredentials };
}
module.exports = { sealCredentials, openCredentials, createDataManager, destination, eventFor, summarizeDiagnostics, MIGRATION_ERROR, netValue, fullyRefunded, needsConsent, CONSENT_REGION };
