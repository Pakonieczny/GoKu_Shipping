'use strict';

// Google offline-conversion migration. A receipt means queued for processing;
// only exact-destination diagnostics can mark the original order as uploaded.
// https://developers.google.com/data-manager/api/devguides/events/google-ads/offline/upgrade/field-mappings
const crypto = require('node:crypto');
const CREDENTIAL_DOC = 'config/googleAdsDataManager';
function credentialKey(env){if(!env.FIREBASE_PRIVATE_KEY)throw Error('Server encryption key unavailable.');return Buffer.from(crypto.hkdfSync('sha256',Buffer.from(env.FIREBASE_PRIVATE_KEY.replace(/\\n/g,'\n')),Buffer.from(env.FIREBASE_PROJECT_ID||''),Buffer.from('Brites Data Manager credentials v1'),32));}
function sealCredentials(value,env){const iv=crypto.randomBytes(12),cipher=crypto.createCipheriv('aes-256-gcm',credentialKey(env),iv);cipher.setAAD(Buffer.from(CREDENTIAL_DOC));const encrypted=Buffer.concat([cipher.update(JSON.stringify(value),'utf8'),cipher.final()]);return {version:1,iv:iv.toString('base64'),tag:cipher.getAuthTag().toString('base64'),ciphertext:encrypted.toString('base64')};}
function openCredentials(value,env){if(value?.version!==1)throw Error('Unsupported connection record.');try{const decipher=crypto.createDecipheriv('aes-256-gcm',credentialKey(env),Buffer.from(value.iv,'base64'));decipher.setAAD(Buffer.from(CREDENTIAL_DOC));decipher.setAuthTag(Buffer.from(value.tag,'base64'));return JSON.parse(Buffer.concat([decipher.update(Buffer.from(value.ciphertext,'base64')),decipher.final()]).toString('utf8'));}catch(_){throw Error('Saved connection could not be decrypted. Reconnect after server key rotation.');}}
const BASE = 'https://datamanager.googleapis.com/v1';
const SCOPE = 'https://www.googleapis.com/auth/datamanager';
const MIGRATION_ERROR = /Data Manager API|CUSTOMER_NOT_ALLOWLISTED_FOR_THIS_FEATURE/i;
const CHECK_DELAY = 30 * 60 * 1000;

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
  const event = { transactionId, eventTimestamp: new Date(raw).toISOString(), conversionValue: value,
    currency, adIdentifiers: { [kind]: String(row[kind]) } };
  const consent = {};
  for (const key of ['adUserData', 'adPersonalization']) {
    if (['GRANTED', 'DENIED'].includes(row.consent?.[key])) consent[key] = row.consent[key];
  }
  if (Object.keys(consent).length) event.consent = consent;
  return event;
}

function summarizeDiagnostics(data, target) {
  const rows = data?.requestStatusPerDestination || [];
  const found = rows.filter(r => r.destination?.operatingAccount?.accountType === 'GOOGLE_ADS' &&
    String(r.destination.operatingAccount.accountId) === target.operatingAccount.accountId &&
    String(r.destination.productDestinationId) === target.productDestinationId);
  if (found.length !== 1) return { state: 'processing', error: 'Diagnostics have not confirmed the exact conversion destination.' };
  const row = found[0], state = row.requestStatus;
  const errors = (row.errorInfo?.errorCounts || []).map(e => `${e.reason || 'Processing error'} (${e.recordCount || e.count || '?'})`).join('; ');
  const warnings = (row.warningInfo?.warningCounts || []).map(e => String(e.reason || 'Processing warning'));
  if (state === 'SUCCESS' && !errors && Number(row.eventsIngestionStatus?.recordCount) === 1) return { state: 'success', warnings };
  if (['FAILED', 'PARTIAL_SUCCESS'].includes(state)) return { state: 'failed', error: errors || 'Google did not process this conversion successfully.', warnings };
  return { state: 'processing', error: errors || null, warnings };
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

  async function request(route, body, auth) {
    const response = await fetch(BASE + route, { method: body ? 'POST' : 'GET', timeout: 20000,
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
  const eligibleLegacyFailure = row => row.failed === true && MIGRATION_ERROR.test(row.uploadError || '') && !row.dmState;
  // Refused because the account is not allowlisted, not because the sale is
  // bad. It becomes uploadable the moment access is granted, with no flag and
  // no one remembering, and it is safe to re-send because it never reached
  // Google: a definite rejection carries no receipt.
  const accountLevelRejection = row => row.dmState === 'failed' && row.dmDefiniteRejection === true
    && !row.dmRequestId && MIGRATION_ERROR.test(row.uploadError || '');
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
        try {
          const savedTarget = row.dmDestination;
          if (!savedTarget) throw Error('This receipt is missing its original conversion destination.');
          const data = await request('/requestStatus:retrieve?requestId=' + encodeURIComponent(row.dmRequestId), null, auth);
          const state = summarizeDiagnostics(data, savedTarget), attempt = Number(row.dmChecks || 0) + 1;
          const patch = { dmChecks: attempt, dmCheckedAt: now(), dmWarnings: state.warnings || [],
            dmNextCheckAt: now() + Math.min(3600000, CHECK_DELAY * Math.pow(1.3, attempt)), uploadError: state.error || null };
          if (state.state === 'success') {
            Object.assign(patch, { uploaded: true, failed: false, uploadedAt: f.FV.serverTimestamp(), dmState: 'success' });
            result.uploaded++; result.processing--;
          } else if (state.state === 'failed') {
            Object.assign(patch, { uploaded: false, failed: true, dmState: 'failed' }); result.rejected++; result.processing--;
          }
          await doc.ref.update(patch);
          if (state.state === 'success') await ledger({ kind: 'uploadConversions', transport: 'data_manager', requestId: row.dmRequestId, count: 1, accepted: 1, ok: true, processingVerified: true, validateOnly: false });
        } catch (error) { result.errors.push({ orderId: row.orderId, error: error.message }); }
        continue;
      }
      if (row.uploaded && !eligibleLegacyFailure(row)) continue;
      if (row.failed && !eligibleLegacyFailure(row) && !accountLevelRejection(row) && !(retryRejected && retryable(row))) continue;
      let event;
      try { event = eventFor(row); } catch (error) { result.errors.push({ orderId: row.orderId, error: error.message }); continue; }
      handled++;
      const body = { destinations: [target], events: [event], encoding: 'HEX', validateOnly: !!ctrl.dryRun };
      if (ctrl.dryRun) {
        try { await request('/events:ingest', body, auth); result.validated++; }
        catch (error) { result.rejected++; result.errors.push({ orderId: row.orderId, error: error.message }); }
        continue;
      }
      const claimed = await f.db.runTransaction(async tx => {
        const current = await tx.get(doc.ref); if (!current.exists) return false;
        const latest = current.data();
        if ((latest.dmState && !accountLevelRejection(latest) && !(retryRejected && retryable(latest))) || latest.dmRequestId || (latest.uploaded && !eligibleLegacyFailure(latest))) return false;
        if (latest.failed && !eligibleLegacyFailure(latest) && !accountLevelRejection(latest) && !(retryRejected && retryable(latest))) return false;
        // Guard changes to original financial data between read and claim.
        if (JSON.stringify(eventFor(latest)) !== JSON.stringify(event)) return false;
        tx.update(doc.ref, { dmState: 'submitting', dmStartedAt: now(), dmDestination: target, uploaded: false });
        return true;
      });
      if (!claimed) continue;
      try {
        const response = await request('/events:ingest', body, auth);
        if (!response.requestId || typeof response.requestId !== 'string') throw Error('Google returned no request receipt; reconcile this order before retrying.');
        await doc.ref.update({ dmState: 'processing', dmRequestId: response.requestId, dmSubmittedAt: now(),
          dmNextCheckAt: now() + CHECK_DELAY, dmChecks: 0, dmFieldWarnings: response.fieldWarnings || [],
          failed: false, uploadError: null, uploaded: false });
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
  async function health() {
    await loadConnection();
    const info = { configured: configured(), transport: 'data_manager', processing: 0, unknown: 0, confirmed: 0, retryable: 0, awaitingAccess: 0, blocked: !configured() };
    const f = fb(); if (!f) return info;
    const queue = f.db.collection(COL.convQueue);
    const rows = await queue.where('uploaded', '==', false).limit(500).get();
    rows.forEach(d => { const x = d.data(); if (x.dmRequestId && x.dmState === 'processing') info.processing++; if (['submitting', 'submission_unknown'].includes(x.dmState)) info.unknown++; if (retryable(x)) info.retryable++; if (accountLevelRejection(x)) info.awaitingAccess++; });
    if (!env.GADS_CONVERSION_ACTION) return { ...info, blocked: true };
    const confirmed = await queue.where('dmState', '==', 'success').limit(50).get();
    const target = destination(env.GADS_CONVERSION_ACTION, env.GADS_LOGIN_CUSTOMER_ID);
    info.confirmed = confirmed.docs.filter(d => {
      const original = d.data().dmDestination;
      return original?.operatingAccount?.accountId === target.operatingAccount.accountId && original?.productDestinationId === target.productDestinationId;
    }).length;
    return info;
  }
  return { run, health, configured, saveCredentials };
}
module.exports = { sealCredentials, openCredentials, createDataManager, destination, eventFor, summarizeDiagnostics, MIGRATION_ERROR };
