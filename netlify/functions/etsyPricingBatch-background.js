// netlify/functions/etsyPricingBatch-background.js
// Server-driven batch processor for the Etsy Pricing Console.
//
// Runs entirely without a browser: obtains an Etsy token from the site's
// server-side token store (etsyAuth.js, same mechanism as EtsyMail's crons),
// then for each queued listing performs the identical pipeline the console's
// Save button uses — by calling this site's own battle-tested functions over
// HTTP (etsyListingInventoryDetailProxy for the snapshot,
// etsyUpdateListingInventoryProxy for the staleness-checked, read-back-
// verified write with the Personalization field). Progress and results are
// written to Firestore in real time (EtsyPricing_Runs +
// EtsyPricing_Listings), so any browser can attach later and watch.
//
// Netlify background functions get ~15 minutes; if time runs short the
// function re-invokes itself with the same run_id and continues where the
// run doc says it left off. Stop is a flag on the run doc, checked before
// every listing.
//
// The rebuild planner below is a byte-faithful port of the console's
// planStandardRebuild — keep the two in sync if the scheme ever changes.

const admin = require("./firebaseAdmin");

/* Server-side Etsy token manager.
   The site's shipped etsyAuth.js points at "config/etsy/oauth" — a
   THREE-segment path, which Firestore rejects for documents (even number
   of segments required), so it can never work. This uses a valid doc,
   seeded by the console: the browser pushes its own OAuth tokens (which
   carry the listing-write scopes) via etsyPricingStore's saveServerToken
   whenever they are issued or refreshed. */
const TOKEN_DOC = "EtsyPricing_Config/etsyOauth";
async function getValidEtsyAccessToken() {
  const db = admin.firestore();
  const snap = await db.doc(TOKEN_DOC).get();
  const tok = snap.exists ? snap.data() : null;
  if (!tok || !tok.refresh_token) throw new Error("No Etsy token on the server yet. Open the pricing console once while connected to Etsy \u2014 it hands its token to the server automatically \u2014 then retry.");
  if (tok.access_token && tok.expires_at && Date.now() < Number(tok.expires_at) - 120000) return tok.access_token;
  const res = await fetch("https://api.etsy.com/v3/public/oauth/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ grant_type: "refresh_token", client_id: process.env.CLIENT_ID, refresh_token: tok.refresh_token })
  });
  if (!res.ok) throw new Error("Etsy token refresh failed: HTTP " + res.status + " " + (await res.text()).slice(0, 200));
  const j = await res.json();
  const stored = {
    access_token: j.access_token,
    refresh_token: j.refresh_token || tok.refresh_token,
    expires_at: Date.now() + Math.max(0, (Number(j.expires_in) || 3600) - 90) * 1000,
    updated_at: Date.now()
  };
  await db.doc(TOKEN_DOC).set(stored, { merge: true });
  return stored.access_token;
}

const SITE = (process.env.URL || "").replace(/\/$/, "");
const FN = SITE + "/.netlify/functions";
const TIME_BUDGET_MS = 13 * 60 * 1000; // leave headroom under Netlify's 15 min

/* ---------------- Pricing scheme (mirror of the console) ---------------- */
const CANON_ORDER=['Silver','Gold','Rose','Silver + Engrave','Gold + Engrave','Rose + Engrave','10k Solid Gold','10k Gold + Engrave','14k Solid Gold','14k Gold + Engrave','Gold-Charm Only','Silver-Charm Only','Rose-Charm Only','10k Solid-Charm Only','14k Solid-Charm Only'];
const CHARM_ONLY_METALS=['Gold-Charm Only','Silver-Charm Only','Rose-Charm Only','10k Solid-Charm Only','14k Solid-Charm Only'];
const NO_CHAIN_VALUE='Charm Only-No Chain';
const REGULAR_PRICES={'Silver':[39.69,40.00,40.31],'Gold':[46.56,46.88,47.19],'Rose':[49.69,50.00,50.31],'Silver + Engrave':[46.56,46.88,47.19],'Gold + Engrave':[51.56,51.88,52.19],'Rose + Engrave':[54.69,55.00,55.31],'10k Solid Gold':[252.81,253.13,253.44],'10k Gold + Engrave':[266.56,266.88,267.19],'14k Solid Gold':[315.94,316.56,316.88],'14k Gold + Engrave':[333.13,333.75,334.06]};
const BEADY_FLAT_PRICES={'Silver':[56.56,56.88,57.19],'Gold':[65.94,66.25,66.56],'Silver + Engrave':[63.75,64.06,64.38],'Gold + Engrave':[73.13,73.44,73.75]};
const BEADY_SOLID_BY_LENGTH={'10k Solid Gold':{14:[439.69,440.00,440.31],16:[470.63,470.94,471.25],18:[501.56,501.88,502.19]},'10k Gold + Engrave':{14:[467.81,468.13,468.44],16:[491.88,492.19,492.50],18:[515.63,515.94,516.25]},'14k Solid Gold':{14:[549.69,550.00,550.31],16:[588.44,588.75,589.06],18:[627.19,627.50,627.81]},'14k Gold + Engrave':{14:[584.69,585.00,585.31],16:[614.69,615.00,615.31],18:[644.69,645.00,645.31]}};
const CHARM_ONLY_PRICE_POOLS={'Gold-Charm Only':[28.13,28.75,29.38],'Silver-Charm Only':[27.81,28.44,29.06],'Rose-Charm Only':[29.06,29.38,30.00],'10k Solid-Charm Only':[110.63,111.56,112.50],'14k Solid-Charm Only':[138.44,139.38,140.63]};
const ENGRAVE_INSTRUCTIONS='To include back engraving on your piece, choose the "+ engrave" option and leave us your instructions here.';
function normOpt(v){return String(v).toLowerCase().replace(/[\u2013\u2014]/g,'-').replace(/\s*-\s*/g,'-').replace(/\s*\+\s*/g,' + ').replace(/\s+/g,' ').trim()}
const CANON_ALIASES=(()=>{const m=new Map();const add=(c,...alts)=>{m.set(normOpt(c),c);for(const a of alts)m.set(normOpt(a),c)};
  add('Silver');add('Gold');add('Rose','rose gold');
  add('Silver + Engrave');add('Gold + Engrave');add('Rose + Engrave','rose gold + engrave');
  add('10k Solid Gold');add('10k Gold + Engrave');add('14k Solid Gold');add('14k Gold + Engrave');
  add('Gold-Charm Only','charm only gold','gold charm only');
  add('Silver-Charm Only','charm only silver','silver charm only');
  add('Rose-Charm Only','charm only rose','rose charm only');
  add('10k Solid-Charm Only','charm only 10k solid','10k solid charm only');
  add('14k Solid-Charm Only','charm only 14k solid','14k solid charm only');
  return m})();
function canonFor(v){return CANON_ALIASES.get(normOpt(v))||null}
function isNoChainVal(v){const c=String(v);return /^no\s*chain/i.test(c)||/charm\s*only.?no\s*chain/i.test(c)}
function parseLen(v){const m=String(v).match(/(\d+)/);return m?parseInt(m[1],10):null}
function titleCaseOpt(v){return String(v).split(/\s+/).map(w=>/^[a-z]/i.test(w)?w[0].toUpperCase()+w.slice(1).toLowerCase():w.toLowerCase()).join(' ')}
function firstOffering(p){return (p&&p.offerings||[])[0]||{}}
function propValue2(p,id){const v=(p.property_values||[]).find(x=>Number(x.property_id)===Number(id));return v?(v.values||[]).join('/'):''}
function deep(v){return JSON.parse(JSON.stringify(v))}
function priceFor(opt,lengthValue,version,chainType){
  if(CHARM_ONLY_PRICE_POOLS[opt])return CHARM_ONLY_PRICE_POOLS[opt][version];
  if(chainType==='beady'){
    if(BEADY_SOLID_BY_LENGTH[opt]){const len=parseLen(lengthValue);const col=BEADY_SOLID_BY_LENGTH[opt][len];
      if(!col)throw new Error('No Beady '+opt+' price for chain length "'+lengthValue+'" (sheet covers 14/16/18 only).');
      return col[version]}
    const flat=BEADY_FLAT_PRICES[opt];
    if(!flat)throw new Error('No Beady price for "'+opt+'".');
    return flat[version];
  }
  const reg=REGULAR_PRICES[opt];
  if(!reg)throw new Error('No Regular price for "'+opt+'".');
  return reg[version];
}
function planStandardRebuild(products,chainType,engraving){
  const propsMap=new Map();
  for(const p of (products||[]))for(const v of (p.property_values||[])){const id=Number(v.property_id);
    if(!propsMap.has(id))propsMap.set(id,{property_id:id,property_name:v.property_name||'Variation',values:[]});
    for(const val of (v.values||[]))if(!propsMap.get(id).values.includes(val))propsMap.get(id).values.push(val)}
  const props=[...propsMap.values()];
  const find=res=>{for(const re of res){const h=props.find(p=>re.test(String(p.property_name||'')));if(h)return h}return null};
  const metalProp=find([/metal/i,/material/i,/colou?r/i]);
  if(!metalProp)return {error:'No metal dropdown found. Dropdowns: '+props.map(p=>p.property_name).join(', ')};
  const lengthProp=find([/chain\s*length/i,/length/i,/chain/i,/size/i]);
  if(!lengthProp)return {error:'No chain-length dropdown found. Dropdowns: '+props.map(p=>p.property_name).join(', ')};
  if(metalProp.property_id===lengthProp.property_id)return {error:'Metal and chain-length detection matched the same dropdown.'};
  const skipRose=chainType==='beady';
  const targetMetals=CANON_ORDER.filter(o=>!(skipRose&&/rose/i.test(o))&&!(!engraving&&/engrave/i.test(o)));
  const realLengths=[...new Set(lengthProp.values.filter(v=>!isNoChainVal(v)&&parseLen(v)!==20).map(titleCaseOpt))];
  if(!realLengths.length)return {error:'No usable chain lengths (only 20-inch or no-chain values).'};
  if(chainType==='beady'){const bad=realLengths.filter(l=>![14,16,18].includes(parseLen(l)));
    if(bad.length)return {error:'Beady pricing covers only 14/16/18-inch chains; listing also has: '+bad.join(', ')}}
  const allLengths=[...realLengths,NO_CHAIN_VALUE];
  const tmpl=(products||[])[0];
  if(!tmpl)return {error:'Listing has no inventory products to rebuild from.'};
  const enabledRow=products.find(p=>firstOffering(p).is_enabled!==false)||tmpl;
  const baseSku=String(enabledRow.sku||'').trim();
  const baseQty=Math.max(1,Number(firstOffering(enabledRow).quantity)||1);
  const plan=[];
  try{
    for(const opt of targetMetals){
      const version=Math.floor(Math.random()*3);
      const isCharm=CHARM_ONLY_METALS.includes(opt);
      const priceBy={};
      for(const len of allLengths){const isNC=isNoChainVal(len);
        priceBy[len]=priceFor(opt,isCharm?null:(isNC?realLengths[0]:len),version,chainType)}
      plan.push({opt,priceBy,isCharm});
    }
  }catch(e){return {error:e.message}}
  const rows=[];
  for(const {opt,priceBy,isCharm} of plan){
    const sku=isCharm?((baseSku?baseSku+'-CO':'CO-'+opt.replace(/[^A-Za-z0-9]+/g,'').slice(0,10)).slice(0,32)):baseSku;
    for(const len of allLengths){const isNC=isNoChainVal(len);const enabled=isCharm?isNC:!isNC;
      const c=deep(tmpl);c.product_id=null;
      if(c.offerings&&c.offerings[0]){c.offerings[0].offering_id=null;c.offerings[0].price=priceBy[len];c.offerings[0].is_enabled=enabled;c.offerings[0].quantity=enabled?baseQty:0}
      c.sku=sku;
      const mv=c.property_values.find(v=>Number(v.property_id)===Number(metalProp.property_id));mv.values=[opt];mv.value_ids=[];
      const lv=c.property_values.find(v=>Number(v.property_id)===Number(lengthProp.property_id));lv.values=[len];lv.value_ids=[];
      rows.push(c);
    }
  }
  return {rows};
}
function compactHealth(h){return h?{error_count:h.error_count||0,warning_count:h.warning_count||0,product_count:h.product_count||0,min_price:h.min_price??null,max_price:h.max_price??null}:null}

/* ---------------- Worker ---------------- */
async function callFn(path, opts) {
  const r = await fetch(FN + path, opts);
  const t = await r.text();
  let d; try { d = JSON.parse(t); } catch { d = { error: t.slice(0, 300) }; }
  if (!r.ok) { const e = new Error(d.error || ("HTTP " + r.status)); e.data = d; throw e; }
  return d;
}

exports.handler = async (event) => {
  let runId;
  try { runId = JSON.parse(event.body || "{}").run_id; } catch { /* noop */ }
  if (!runId) return { statusCode: 400, body: "missing run_id" };

  const db = admin.firestore();
  const runRef = db.collection("EtsyPricing_Runs").doc(String(runId));
  const started = Date.now();

  const snap = await runRef.get();
  if (!snap.exists) return { statusCode: 404, body: "run not found" };
  let run = snap.data();
  if (["done", "stopped"].includes(run.status)) return { statusCode: 200, body: "already finished" };
  if (run.paused) return { statusCode: 200, body: "paused" };
  await runRef.set({ status: "running", updated_at: Date.now() }, { merge: true });

  let accessToken;
  try { accessToken = await getValidEtsyAccessToken(); }
  catch (e) {
    await runRef.set({ status: "done", fatal_error: "Server Etsy token unavailable: " + e.message, updated_at: Date.now() }, { merge: true });
    return { statusCode: 200, body: "no token" };
  }
  const authHeaders = { "access-token": accessToken, "Content-Type": "application/json" };

  const ids = run.ids || [];
  for (let i = run.done; i < ids.length; i++) {
    // Stop flag + time budget, checked per listing.
    const fresh = (await runRef.get()).data();
    if (fresh.stop) { await runRef.set({ status: "stopped", current: "", updated_at: Date.now() }, { merge: true }); return { statusCode: 200, body: "stopped" }; }
    if (fresh.paused) { await runRef.set({ status: "paused", current: "Paused \u2014 " + run.done + " of " + ids.length + " completed", updated_at: Date.now() }, { merge: true }); return { statusCode: 200, body: "paused" }; }
    if (Date.now() - started > TIME_BUDGET_MS) {
      // Self-chain into a new invocation and exit cleanly.
      fetch(FN + "/etsyPricingBatch-background", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ run_id: runId }) }).catch(() => {});
      await runRef.set({ current: "re-queued (time budget)", updated_at: Date.now() }, { merge: true });
      return { statusCode: 200, body: "chained" };
    }

    const id = String(ids[i]);
    const prepSnap = await db.collection("EtsyPricing_Listings").doc(id).get();
    const d = prepSnap.exists ? prepSnap.data() : {};
    await runRef.set({ current: "#" + id + (d.title ? " \u00b7 " + d.title : ""), updated_at: Date.now() }, { merge: true });

    try {
      const detail = await callFn("/etsyListingInventoryDetailProxy?listingId=" + encodeURIComponent(id) + "&inventory_only=1", { headers: authHeaders });
      const plan = planStandardRebuild(detail.inventory.products, d.chain_type === "beady" ? "beady" : "regular", d.engraving !== false);
      if (plan.error) throw new Error(plan.error);
      const pers = (d.engraving !== false) ? { enabled: true, required: true, max_chars: 1000, instructions: ENGRAVE_INSTRUCTIONS } : null;
      const res = await callFn("/etsyUpdateListingInventoryProxy", {
        method: "POST", headers: authHeaders,
        body: JSON.stringify({ listing_id: Number(id), expected_snapshot_hash: detail.snapshot_hash, inventory: { products: plan.rows }, auto_on_property: true, personalization: pers })
      });
      if (!res.verified) throw new Error(res.verification_error || "Etsy verification did not match.");

      const patch = {
        batched: true,
        last_batch: { at: Date.now(), ok: true },
        last_save: { at: Date.now(), verified: true },
        health: compactHealth(res.fresh && res.fresh.pricing_health),
        scanned: true,
        approval: { mode: "updated", at: Date.now(), hash: (res.fresh && res.fresh.snapshot_hash) || null },
        updated_at: Date.now()
      };
      if (!d.original_saved && res.previous_inventory) {
        patch.original_inventory = res.previous_inventory;
        patch.original_snapshot_hash = res.previous_snapshot_hash || null;
        patch.original_saved = true;
      }
      await db.collection("EtsyPricing_Listings").doc(id).set(patch, { merge: true });
      run.ok = (run.ok || 0) + 1;
      run.consec_fail = 0;
    } catch (e) {
      run.fail = (run.fail || 0) + 1;
      run.consec_fail = (run.consec_fail || 0) + 1;
      const msg = String(e.message).slice(0, 400);
      // NOTE: `batched` is deliberately NOT set on failure, so the listing
      // stays in the prepared/ready queue and is re-processed on the next
      // batch run — failed listings are never lost.
      await db.collection("EtsyPricing_Listings").doc(id).set({ last_batch: { at: Date.now(), ok: false, error: msg }, updated_at: Date.now() }, { merge: true });
      await runRef.set({ errors: admin.firestore.FieldValue.arrayUnion("#" + id + (d.title ? " \u00b7 " + d.title : "") + ": " + msg), updated_at: Date.now() }, { merge: true });
    }
    run.done = i + 1;
    await runRef.set({ done: run.done, ok: run.ok || 0, fail: run.fail || 0, consec_fail: run.consec_fail || 0, updated_at: Date.now() }, { merge: true });
    if ((run.consec_fail || 0) >= 3) {
      await runRef.set({ status: "stopped", stop_reason: "Auto-halted: 3 consecutive listings failed to update on Etsy. Un-attempted and failed listings remain queued and will be re-processed on the next run.", current: "", updated_at: Date.now() }, { merge: true });
      return { statusCode: 200, body: "auto-halted" };
    }
  }

  await runRef.set({ status: "done", current: "", finished_at: Date.now(), updated_at: Date.now() }, { merge: true });
  return { statusCode: 200, body: "done" };
};
