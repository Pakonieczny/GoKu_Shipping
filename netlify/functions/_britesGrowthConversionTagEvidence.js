'use strict';
// Read-only mapping evidence, not proof of checkout dispatch or deduplication.
// Official field/shape references inspected 2026-10-02:
// https://developers.google.com/google-ads/api/fields/v24/conversion_action#conversion_action.tag_snippets
// https://developers.google.com/google-ads/api/reference/rpc/v24/TagSnippet
const QUERY = "SELECT conversion_action.resource_name, conversion_action.id, conversion_action.name, conversion_action.status, conversion_action.type, conversion_action.category, conversion_action.tag_snippets FROM conversion_action WHERE conversion_action.category = 'PURCHASE' AND conversion_action.status IN ('ENABLED', 'REMOVED') ORDER BY conversion_action.id LIMIT 201";
const LIMITS = Object.freeze({actions:200, snippets:12, snippetChars:100000, destinations:24});
const object = value => value && typeof value === 'object' && !Array.isArray(value);
const text = (value, max) => typeof value === 'string' ? value.replace(/[\u0000-\u001f\u007f]/g, ' ').slice(0, max) : '';
function base() {
  return {schema:1, evidenceType:'conversion_action_tag_destinations', readOnly:true,
    providerWrites:false, conversionUploads:0, runtimeDispatchVerified:false,
    transactionIdentityVerified:false, duplicateCountingVerified:false,
    limitation:'Returned event-snippet destinations describe configuration only. Missing destinations do not prove a tag is absent. Checkout dispatch, transaction identity and duplicate counting require separate evidence.'};
}
function project(rows) {
  const result = {...base(), state:'available', complete:true, truncated:false, actions:[], malformedRows:0};
  if (!Array.isArray(rows)) return {...result, state:'unavailable', complete:false};
  if (rows.length > LIMITS.actions) { result.complete=false; result.truncated=true; }
  const seen = new Set();
  for (const row of rows.slice(0, LIMITS.actions)) {
    const action = object(row) && (row.conversionAction || row.conversion_action);
    const resourceName = action && (action.resourceName || action.resource_name);
    const match = typeof resourceName === 'string' && /^customers\/(\d{1,20})\/conversionActions\/(\d{1,20})$/.exec(resourceName);
    const id = action && (typeof action.id === 'string' ? action.id : Number.isSafeInteger(action.id) ? String(action.id) : null);
    if (!object(action) || !match || match[2] !== id || !['ENABLED','REMOVED'].includes(action.status) || action.category !== 'PURCHASE' || typeof action.name !== 'string' || typeof action.type !== 'string' || !/^[A-Z][A-Z0-9_]{0,99}$/.test(action.type) || seen.has(resourceName)) {
      result.complete=false; result.malformedRows++; continue;
    }
    seen.add(resourceName);
    const entry = {resourceName, id, name:text(action.name,300), status:action.status, type:action.type,
      category:'PURCHASE', destinations:[], destinationState:'no_destination_returned', complete:true};
    const snippets = action.tagSnippets ?? action.tag_snippets ?? [];
    if (!Array.isArray(snippets)) entry.complete=false;
    else {
      if (snippets.length > LIMITS.snippets) entry.complete=false;
      const destinations = new Set();
      for (const snippet of snippets.slice(0,LIMITS.snippets)) {
        const event = object(snippet) ? snippet.eventSnippet ?? snippet.event_snippet ?? '' : null;
        if (typeof event !== 'string' || event.length > LIMITS.snippetChars) { entry.complete=false; continue; }
        // Read only a quoted send_to value. Global tags, other fields, partial
        // IDs/labels and substrings cannot establish a destination mapping.
        const regex = /(['"])send_to\1\s*:\s*(['"])(AW-\d{5,20}\/[A-Za-z0-9_-]{1,100})\2/g;
        const matches = [...event.matchAll(regex)];
        if ([...event.matchAll(/(['"])send_to\1\s*:/g)].length !== matches.length) entry.complete=false;
        for (const matched of matches) destinations.add(matched[3]);
      }
      if (destinations.size > LIMITS.destinations) entry.complete=false;
      entry.destinations = [...destinations].sort().slice(0,LIMITS.destinations).map(destination => {
        const [conversionId,label] = destination.split('/'); return {destination,conversionId,label};
      });
    }
    entry.destinationState = entry.destinations.length ? 'destination_returned' : 'no_destination_returned';
    if (!entry.complete) result.complete=false;
    result.actions.push(entry);
  }
  return result;
}
async function read({gaql,now=Date.now}={}) {
  try {
    if (typeof gaql !== 'function') throw Error('Unavailable');
    return {...project(await gaql(QUERY)), checkedAt:now()};
  } catch {
    // Provider errors can contain request/account details or snippets.
    return {...base(),state:'unavailable',complete:false,truncated:false,actions:[],
      error:'Conversion-action tag evidence is unavailable. Retry this read later.',checkedAt:now()};
  }
}
module.exports = {QUERY,LIMITS,project,read};
