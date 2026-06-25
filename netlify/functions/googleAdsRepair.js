// netlify/functions/googleAdsRepair.js
// ─────────────────────────────────────────────────────────────────────────────
// ONE-CLICK REPAIR for approvals stuck in APPROVED (approved, but the apply errored
// because the saved payload was frozen with the rejected Campaign.text_guidelines
// field). Fully self-contained — does NOT call googleAdsAutopilot.js, so it works
// even if that file hasn't been redeployed.
//
// Open it in a browser (no query string needed):
//   https://goldenspike.app/.netlify/functions/googleAdsRepair
// Then click a button. "Preview" validates without creating; "Create campaigns"
// actually builds them (PAUSED).
//
// Gate matches the rest of the tooling: if EDIT_PASSCODE is set, it's required;
// if it's unset, this tool is open (just like the console). GET only ever shows the
// page — campaigns are created only when you click a button (a POST). Delete this
// function once your campaigns exist.
// ─────────────────────────────────────────────────────────────────────────────

const fetch = require("node-fetch");
const ENV = process.env;
const V = ENV.GADS_API_VERSION || "v24";
const CID = (ENV.GADS_CUSTOMER_ID || "").replace(/\D/g, "");
const LOGIN = (ENV.GADS_LOGIN_CUSTOMER_ID || "").replace(/\D/g, "");
const APPROVALS = "Brites_GAds_Approvals";

// trim + strip accidental surrounding quotes so a value pasted as "abc" still matches abc
function gateVal() { return (ENV.EDIT_PASSCODE || "").trim().replace(/^["']|["']$/g, ""); }

function db() {
  try { const admin = require("./firebaseAdmin"); return { d: admin.firestore(), admin }; }
  catch (e) { return { d: null, admin: null }; }
}
const esc = s => String(s == null ? "" : s).replace(/[&<>"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
const json = (code, obj) => ({ statusCode: code, headers: { "Content-Type": "application/json" }, body: JSON.stringify(obj, null, 2) });
const htmlOut = body => ({ statusCode: 200, headers: { "Content-Type": "text/html; charset=utf-8" }, body });

async function mintToken() {
  const res = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST", headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      client_id: ENV.GADS_CLIENT_ID, client_secret: ENV.GADS_CLIENT_SECRET,
      refresh_token: ENV.GADS_REFRESH_TOKEN, grant_type: "refresh_token"
    })
  });
  const d = await res.json().catch(() => ({}));
  if (!res.ok || !d.access_token) throw new Error("OAuth: " + (d.error_description || d.error || res.status));
  return d.access_token;
}

// Heal frozen payloads built by the old code:
//  1) strip Campaign.text_guidelines (rejected free-text field)
//  2) un-nest the extra resource layer the old builder put inside create
//     (REST: an operation's `create` field IS the resource — no inner
//      adGroupAd / adGroupCriterion / adGroup wrapper).
// Summarize the payload so the result shows the actual shape (a too-few-descriptions
// or odd-bidding issue is a common cause of the generic "invalid argument").
function payloadInfo(ops) {
  const info = { headlines: 0, descriptions: 0, keywords: 0, budget: null, bidding: null, finalUrl: null, ops: (ops || []).length };
  (ops || []).forEach(op => {
    if (op.campaignBudgetOperation && op.campaignBudgetOperation.create) info.budget = (Number(op.campaignBudgetOperation.create.amountMicros) || 0) / 1e6;
    if (op.campaignOperation && op.campaignOperation.create) {
      const c = op.campaignOperation.create;
      info.bidding = c.maximizeConversionValue ? "maximizeConversionValue" : c.maximizeConversions ? "maximizeConversions" :
        c.targetRoas ? "targetRoas" : c.targetSpend ? "targetSpend" : c.manualCpc ? "manualCpc" : c.targetCpa ? "targetCpa" : "(none set)";
    }
    if (op.adGroupAdOperation && op.adGroupAdOperation.create) {
      const cr = op.adGroupAdOperation.create; const ad = (cr.adGroupAd ? cr.adGroupAd : cr).ad || {}; const rsa = ad.responsiveSearchAd || {};
      info.headlines = (rsa.headlines || []).length; info.descriptions = (rsa.descriptions || []).length; info.finalUrl = (ad.finalUrls || [])[0] || null;
    }
    if (op.adGroupCriterionOperation) info.keywords++;
  });
  return info;
}

// Google's editorial "Symbols" policy disapproves decorative symbols/emoji in ad copy
// (★, dingbats, arrows, emoji…). Strip them; keep letters, digits, normal punctuation,
// and currency ($ € £, which are category Sc, not So).
function cleanAdText(s) {
  return String(s == null ? "" : s).replace(/[\p{So}\p{Sk}\p{Extended_Pictographic}\u2190-\u21FF\u27F0-\u27FF\u2900-\u297F\u2B00-\u2BFF]/gu, "").replace(/\s{2,}/g, " ").trim();
}

function normalize(ops) {
  let fixed = 0;
  (Array.isArray(ops) ? ops : []).forEach(op => {
    if (!op) return;
    const cc = op.campaignOperation && (op.campaignOperation.create || op.campaignOperation.update);
    if (cc) {
      if (cc.text_guidelines !== undefined) { delete cc.text_guidelines; fixed++; }
      if (cc.textGuidelines !== undefined) { delete cc.textGuidelines; fixed++; }
    }
    // newer required field the old builder predates (non-political advertiser)
    const ccreate = op.campaignOperation && op.campaignOperation.create;
    if (ccreate && ccreate.containsEuPoliticalAdvertising === undefined && ccreate.contains_eu_political_advertising === undefined) {
      ccreate.containsEuPoliticalAdvertising = "DOES_NOT_CONTAIN_EU_POLITICAL_ADVERTISING"; fixed++;
    }
    const aa = op.adGroupAdOperation && op.adGroupAdOperation.create;
    if (aa && aa.adGroupAd && typeof aa.adGroupAd === "object") { op.adGroupAdOperation.create = aa.adGroupAd; fixed++; }
    // strip prohibited symbols from the responsive search ad copy
    const adc = op.adGroupAdOperation && op.adGroupAdOperation.create;
    const rsa = adc && adc.ad && adc.ad.responsiveSearchAd;
    if (rsa) {
      ["headlines", "descriptions"].forEach(k => {
        if (Array.isArray(rsa[k])) {
          rsa[k].forEach(t => { if (t && typeof t.text === "string") { const c = cleanAdText(t.text); if (c !== t.text) { t.text = c; fixed++; } } });
          rsa[k] = rsa[k].filter(t => t && t.text && t.text.trim());
        }
      });
    }
    const ac = op.adGroupCriterionOperation && op.adGroupCriterionOperation.create;
    if (ac && ac.adGroupCriterion && typeof ac.adGroupCriterion === "object") { op.adGroupCriterionOperation.create = ac.adGroupCriterion; fixed++; }
    const ag = op.adGroupOperation && op.adGroupOperation.create;
    if (ag && ag.adGroup && typeof ag.adGroup === "object" && ag.adGroup.name) { op.adGroupOperation.create = ag.adGroup; fixed++; }
  });
  return fixed;
}

// Google Ads failures hide the useful detail in error.details[].errors[] —
// error.message is just the generic "Request contains an invalid argument."
function extractAdsError(data) {
  const parts = [];
  const det = (data && data.error && data.error.details) || [];
  det.forEach(d => {
    (d.errors || []).forEach(e => {
      const path = ((e.location && e.location.fieldPathElements) || [])
        .map(f => f.fieldName + (f.index != null ? "[" + f.index + "]" : "")).join(".");
      const code = e.errorCode ? Object.entries(e.errorCode).map(([k, v]) => k + ":" + v).join(",") : "";
      let extra = "";
      const pf = e.details && e.details.policyFindingDetails;
      if (pf && pf.policyTopicEntries) {
        extra = "  {policy: " + pf.policyTopicEntries.map(t => {
          const ev = (t.evidences || []).map(x => (x.textList && x.textList.texts || []).join("/")).filter(Boolean).join(" / ");
          return (t.topic || "?") + "(" + (t.type || "") + ")" + (ev ? " ← \"" + ev + "\"" : "");
        }).join(", ") + "}";
      }
      parts.push((path ? path + " → " : "") + (e.message || "") + (code ? "  [" + code + "]" : "") + extra);
    });
  });
  if (parts.length) return parts.join("  ||  ");
  return (data && data.error && data.error.message) || JSON.stringify(data).slice(0, 800);
}

async function applyOne(token, ops, dry) {
  const res = await fetch(`https://googleads.googleapis.com/${V}/customers/${CID}/googleAds:mutate`, {
    method: "POST",
    headers: {
      "Authorization": "Bearer " + token, "developer-token": ENV.GADS_DEVELOPER_TOKEN,
      "login-customer-id": LOGIN, "Content-Type": "application/json"
    },
    body: JSON.stringify({ mutateOperations: ops, partialFailure: false, validateOnly: !!dry })
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error("HTTP " + res.status + " — " + extractAdsError(data).slice(0, 1800));
  if (data.partialFailureError) throw new Error("partial failure — " + extractAdsError({ error: data.partialFailureError }).slice(0, 1800));
  let campaign = null;
  (data.mutateOperationResponses || []).forEach(r => {
    if (r && r.campaignResult && r.campaignResult.resourceName) campaign = r.campaignResult.resourceName;
  });
  return campaign;
}

async function run(dry) {
  const out = { ran: true, dry: !!dry, found: 0, created: 0, validated: 0, items: [], fatal: null };
  const { d, admin } = db();
  if (!d) { out.fatal = "Firestore unavailable (./firebaseAdmin not found)."; return out; }
  if (!CID || !ENV.GADS_DEVELOPER_TOKEN || !ENV.GADS_CLIENT_ID) { out.fatal = "Missing Google Ads env vars."; return out; }

  let token;
  try { token = await mintToken(); } catch (e) { out.fatal = e.message; return out; }

  let snap;
  try { snap = await d.collection(APPROVALS).where("status", "==", "APPROVED").limit(25).get(); }
  catch (e) { out.fatal = "Approvals read failed: " + e.message; return out; }

  const docs = [];
  snap.forEach(x => docs.push({ id: x.id, data: x.data() }));
  out.found = docs.length;

  for (const doc of docs) {
    const it = doc.data || {};
    const item = { id: doc.id, summary: (it.summary || "").slice(0, 90), fixed: 0, info: null, status: "", campaign: null, error: null };
    const p = it.payload || {};
    const ops = p.mutateOperations || null;
    if (!ops) { item.status = "skipped"; item.error = "no mutateOperations in payload (can't repair this shape)."; out.items.push(item); continue; }
    item.fixed = normalize(ops);
    item.info = payloadInfo(ops);
    try {
      const campaign = await applyOne(token, ops, dry);
      if (dry) { item.status = "validated"; out.validated++; }
      else {
        item.status = "created"; item.campaign = campaign; out.created++;
        try { await d.collection(APPROVALS).doc(doc.id).set({ status: "APPLIED", appliedAt: admin.firestore.FieldValue.serverTimestamp(), repairedAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true }); }
        catch (e) { item.error = "created OK but status update failed: " + e.message; }
      }
    } catch (e) { item.status = "error"; item.error = e.message; }
    out.items.push(item);
  }
  return out;
}

function page(gateSet) {
  const auth = gateSet
    ? `<input id="k" type="password" placeholder="Console passcode (EDIT_PASSCODE)" autocomplete="off"
         style="flex:1;min-width:240px;padding:11px 13px;border:1px solid #ccc;border-radius:9px;font-size:14px"
         onkeydown="if(event.key==='Enter')go(true)">`
    : `<div style="flex:1;min-width:200px;font-size:13px;color:#666;background:#f3f0e9;border:1px solid #e3ddcf;border-radius:9px;padding:10px 13px">No passcode is set on this site, so this tool is open. You can lock it later by adding <code>EDIT_PASSCODE</code> in Netlify.</div>`;
  return htmlOut(`<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Ad Autopilot · repair</title>
<body style="font-family:-apple-system,Segoe UI,sans-serif;max-width:760px;margin:40px auto;color:#1a1a1a;padding:0 18px">
<h2 style="font-weight:600;margin-bottom:4px">Ad Autopilot — repair stuck approvals</h2>
<p style="color:#666;font-size:14px;margin-top:0">Re-applies drafts that were approved but errored on send. Campaigns are created <b>PAUSED</b> — nothing spends until you enable it.</p>
<div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin:18px 0">
  ${auth}
  <button onclick="go(true)"  style="padding:11px 16px;border:1px solid #c9a23a;background:#f3e7c4;color:#5a4a1d;border-radius:9px;font-weight:600;font-size:13.5px;cursor:pointer">Preview (dry run)</button>
  <button onclick="go(false)" style="padding:11px 16px;border:none;background:#1a1a1a;color:#fff;border-radius:9px;font-weight:600;font-size:13.5px;cursor:pointer">Create campaigns</button>
</div>
<div id="out" style="margin-top:10px"></div>
<script>
function esc(s){return String(s==null?'':s).replace(/[&<>]/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;'}[c];});}
function row(i){var col=i.status==='created'?'#3c7a39':i.status==='validated'?'#7a5a1d':i.status==='error'?'#b3402f':'#8a8a8a';
  var info=i.info?'<div style="font-size:10.5px;color:#999;margin-top:3px">'+i.info.headlines+' headlines · '+i.info.descriptions+' descriptions · '+i.info.keywords+' keywords · '+esc(i.info.bidding||'')+(i.info.budget!=null?' · $'+i.info.budget+'/day':'')+'</div>':'';
  return '<tr><td style="font-size:12px;padding:7px 9px;border-bottom:1px solid #eee">'+esc(i.summary)+info+'</td>'+
    '<td style="padding:7px 9px;border-bottom:1px solid #eee"><b style="color:'+col+'">'+esc(i.status||'?')+'</b></td>'+
    '<td style="font-family:monospace;font-size:11px;padding:7px 9px;border-bottom:1px solid #eee">'+esc(i.campaign||(i.error?'':'\u2014'))+'</td>'+
    '<td style="font-size:11px;color:#b3402f;padding:7px 9px;border-bottom:1px solid #eee">'+esc(i.error||'')+'</td></tr>';}
async function go(dry){
  var inp=document.getElementById('k');
  var key=inp?inp.value.trim():'';
  var out=document.getElementById('out');
  if(inp&&!key){out.innerHTML='<div style="color:#b3402f;font-size:13px">Enter your passcode first.</div>';return;}
  out.innerHTML='<div style="color:#888;font-size:13px">Working\u2026</div>';
  try{
    var res=await fetch(location.pathname,{method:'POST',headers:{'Content-Type':'application/json','x-edit-passcode':key},body:JSON.stringify({dry:dry})});
    var d=await res.json();
    if(res.status===401){out.innerHTML='<div style="background:#fbeae7;border:1px solid #e2b6ad;color:#b3402f;padding:11px 13px;border-radius:8px;font-size:13.5px">Passcode did not match. Check the EDIT_PASSCODE value in Netlify (watch for trailing spaces).</div>';return;}
    if(d.fatal){out.innerHTML='<div style="background:#fbeae7;border:1px solid #e2b6ad;color:#b3402f;padding:11px 13px;border-radius:8px;font-size:13.5px">Could not run: '+esc(d.fatal)+'</div>';return;}
    var head='<div style="background:'+((!dry&&d.created)?'#eaf3e8':'#f6f3ec')+';border:1px solid #d9d2c2;padding:11px 13px;border-radius:8px;font-size:14px;margin-bottom:12px">Found <b>'+d.found+'</b> stuck approval(s). '+(dry?('Validated <b>'+d.validated+'</b> \u2014 dry run, nothing created.'):('Created <b>'+d.created+'</b> campaign(s) in Google Ads (PAUSED).'))+'</div>';
    var rows=(d.items||[]).map(row).join('')||'<tr><td colspan="4" style="padding:9px">Nothing stuck in APPROVED \u2014 nothing to repair.</td></tr>';
    out.innerHTML=head+'<table style="border-collapse:collapse;width:100%;border:1px solid #eee;font-size:13px"><thead><tr style="background:#f3f0e9;text-align:left"><th style="padding:7px 9px">Draft</th><th style="padding:7px 9px">Result</th><th style="padding:7px 9px">New campaign</th><th style="padding:7px 9px">Error</th></tr></thead><tbody>'+rows+'</tbody></table>'+((dry&&d.found)?'<div style="font-size:12px;color:#666;margin-top:10px">Looks good? Click <b>Create campaigns</b> to build them for real.</div>':'')+((!dry&&d.created)?'<div style="font-size:12px;color:#666;margin-top:10px">Open Google Ads or the console Performance tab to enable them.</div>':'');
  }catch(e){out.innerHTML='<div style="color:#b3402f;font-size:13px">Request failed: '+esc(e.message)+'</div>';}
}
</script></body>`);
}

exports.handler = async (event) => {
  const method = (event.httpMethod || "GET").toUpperCase();
  const q = (event && event.queryStringParameters) || {};
  const headers = event.headers || {};
  let body = {};
  try { if (event.body) body = JSON.parse(event.body); } catch (e) {}

  // self-check: confirm config WITHOUT revealing the passcode
  if (q.check === "1") {
    const g = gateVal();
    return json(200, { editPasscodeSet: !!g, passcodeLength: g.length, open: !g, customerId: CID || null, hasDevToken: !!ENV.GADS_DEVELOPER_TOKEN, ready: !!(CID && ENV.GADS_DEVELOPER_TOKEN) });
  }

  const gate = gateVal();

  // GET only ever shows the page — it never creates anything. Creation happens on the
  // POST that a button fires.
  if (method !== "POST") return page(!!gate);

  // POST: gate matches the console — required only if EDIT_PASSCODE is set.
  const key = (headers["x-edit-passcode"] || headers["X-Edit-Passcode"] || body.key || "").trim();
  if (gate && key !== gate) return json(401, { error: "wrong passcode", editPasscodeSet: true });

  const dry = body.dry === true || body.dry === "1" || q.dry === "1";
  const r = await run(dry);
  return json(200, r);
};
