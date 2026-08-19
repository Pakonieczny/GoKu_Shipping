/* =============================================================================
   BRITES CUSTOM CHARM STUDIO — production client
   -----------------------------------------------------------------------------
   Ported from custom-charm-studio.html (Charm_Designer.html). Layout, stepper,
   zoom/pan, conversation thread, order card, pricing modal, bespoke dropdown
   and size chips are the prototype's own code, transplanted unchanged. Every
   SIMULATED piece named in the integration guide §10.11 has been replaced:

     runGeneration()              -> geminiImageProxy-background
                                    custom_charm_generate / _refine + onSnapshot
     precheckImage()              -> custom_charm_precheck
     readUpload()/resizeToMax()   -> client resize + britesAuth upload_ref (XHR
                                    progress); the browser never writes Storage
     filteredCatalog()/renderCharmGrid() -> Shopify predictive search
     ensureSession/saveSession/loadSession/logEvent -> customSessions/{sid},
                                    debounced 500 ms + onSnapshot
     renderCredits/spendCredit    -> mirror of users/{uid}.wallet, never mutated
                                    locally; the server owns the quota
     completeAuth()/submitCode()  -> britesAuth send_code / verify_code and
                                    Google Identity Services linkWithCredential
     buyTier()                    -> Shopify cart permalink carrying studio_uid
     addToCartBtn                 -> /cart/add.js, charm + design-fee lines
     charmSVG()/versionMedia()    -> deleted; the generated PNG's download URL
     ASSETS.* base64              -> Shopify Files URLs from section settings

   One IIFE, no module scope, modern syntax only (porting rule 6).
   ========================================================================== */
(function () {
"use strict";

const ROOT = document.getElementById("bjStudio");
if (!ROOT) return;

/* ── Porting rule 3 — neutralise two site-wide snippets on this page ────────
   brites-scroll-restore.liquid sets history.scrollRestoration = "manual" and
   replays a saved scroll offset for six hours; brites-reveal.liquid animates
   elements as they enter the viewport. Both fight a multi-step, no-scroll
   wizard, so the studio opts out of each. (brites-reveal excludes anything
   inside .bj-no-reveal — see the one-line EXCLUDE patch in the install
   guide.) */
try { history.scrollRestoration = "auto"; } catch (e) {}
ROOT.classList.add("bj-no-reveal");
document.body.classList.add("bj-studio-page");

/* ── DOM helpers — scoped to the studio so nothing on the page is touched ── */
const $  = (s) => ROOT.querySelector(s);
const $$ = (s) => Array.from(ROOT.querySelectorAll(s));

/* ── Everything Liquid handed us ──────────────────────────────────────────── */
const D              = ROOT.dataset;
const FN_BASE        = D.fnBase || "https://goldenspike.app/.netlify/functions";
const FN_BASE_IMAGE  = D.fnBaseImage || FN_BASE;      // geminiImageProxy host
const IMAGE_FN       = D.imageFn || "geminiImageProxy-background";
const CART_ADD_URL   = D.cartAdd || "/cart/add.js";
const STUDIO_PAGE    = D.pageUrl || location.pathname;
const CHARM_FILTER   = new RegExp(D.charmFilter || "charm only|charm -", "i");
const SEARCH_LIMIT   = parseInt(D.searchLimit || "24", 10);
const VARIANTS       = window.BJ_STUDIO_VARIANTS || {};
const ASSETS         = Object.freeze(JSON.parse(D.assets || "{}"));
const FB_CFG         = JSON.parse(D.firebase || "{}");
const FB_VER         = D.firebaseVersion || "9.23.0";
const GOOGLE_CLIENT  = D.googleClientId || "";

/* Email + code sign-in is hidden until a verified sending domain exists —
   Continue with Google covers accounts, and guests need none at all. One
   CONFIG value flips it back; nothing else changes. Must sit AFTER D is
   declared: reading it above the const is a temporal-dead-zone throw that
   takes the whole IIFE down. */
if ((D.emailSignin || "off") !== "on") ROOT.classList.add("bj-no-email-auth");

const attrText = (value = "") => String(value).replace(/[&<>"']/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const assetImg = (src, alt = "", loading = "eager") =>
  `<img class="asset-img" src="${attrText(src)}" alt="${attrText(alt)}" loading="${loading}" decoding="${loading === "eager" ? "sync" : "async"}">`;
/* Shopify image transforms: each surface pulls only the size it needs
   (porting rule 5 — banner ~1800, tiles ~720, catalogue ~540, dock ~260). */
const sized = (url, w) => {
  if (!url || !/cdn\.shopify\.com/.test(url)) return url;
  if (/[?&]width=\d+/.test(url)) return url.replace(/([?&])width=\d+/, "$1width=" + w);
  return url + (url.includes("?") ? "&" : "?") + "width=" + w;
};

/* Catalogue items now come from predictive search, so `image` is a real URL. */
const productMedia = (item, loading = "lazy", w = 540) =>
  assetImg(sized(item && item.image, w) || ASSETS.startSearch,
           (item && item.title) || "Brites handcrafted jewelry", loading);
/* A version is a generated 2K PNG in Storage; its download URL is the media. */
const versionMedia = (version) =>
  assetImg((version && version.url) || ASSETS.customPreview || "",
           "Your custom charm design", "eager");
const approvedMedia = versionMedia;

/* =============================================================================
   CONFIG — defaults; config/customStudio in Firestore overrides them at boot,
   so limits and prices are tunable without a deploy (§10.4).
   ========================================================================== */
const CONFIG = {
  currency: "$",
  guestFreeCredits: 3,
  signupBonusCredits: 7,
  refineCost: 1,
  guestFreeUploads: 3,
  signupBonusUploads: 7,
  maxUploadPx: 2048,
  maxUploadBytes: 15 * 1024 * 1024,
  generateCost: 1,
  /* The metal render offers two renderers. "Standard" is the Gemini path the
     studio has always used and costs generateCost; "High" routes to
     gpt-image-2 and costs this instead. The server holds the same two numbers
     in config/customStudio and is the only thing that actually charges — this
     copy exists so the buttons can price themselves honestly. */
  renderHighCost: 2,
  /* Whether the Standard/High choice exists at all. The server holds the same
     key and is the only thing that decides what a press actually costs; this
     copy exists so the control is never on screen offering a tier the server
     would silently downgrade. Default false — see renderTiersOn(). */
  renderQualityTiers: false,
  /* The Full/Short prompt control. Hidden unless this is true — the same
     value the server reads before it will honour a promptMode from the
     browser, so the button can never offer a mode the server ignores. */
  renderPromptModeUI: false,
  designFee: 9.99,
  engravingFee: 5.00,
  extenderHeartFee: 20.00,
  maxInstruction: 400,
  packs: [
    { id:"p10",  handle:"studio-pack-10",  name:"Starter",  credits:10,  price:4.99,  per:"50¢ / design",  feats:["10 design credits","Credits never expire","All metals & formats","Designs saved to your account"] },
    { id:"p25",  handle:"studio-pack-25",  name:"Creator",  credits:25,  price:9.99,  per:"40¢ / design", featured:true, flag:"Most popular", feats:["25 design credits","Credits never expire","Priority generation queue","Designs saved to your account"] },
    { id:"p60",  handle:"studio-pack-60",  name:"Studio",   credits:60,  price:19.99, per:"33¢ / design", feats:["60 design credits","Credits never expire","Priority generation queue","Early access to new formats"] },
    { id:"p150", handle:"studio-pack-150", name:"Bulk",     credits:150, price:39.99, per:"27¢ / design", flag:"Best value", feats:["150 design credits","Perfect for gifting sprees & events","Priority generation queue","Dedicated email support"] },
  ],
  plans: [
    { id:"m40",  handle:"studio-plan-hobbyist", name:"Hobbyist", credits:40,  price:7.99,  per:"40 designs / month", feats:["40 credits refreshed monthly","Unused credits roll 1 month","Priority generation queue","Member-only seasonal drops"] },
    { id:"m150", handle:"studio-plan-designer", name:"Designer", credits:150, price:19.99, per:"150 designs / month", featured:true, flag:"Most popular", feats:["150 credits refreshed monthly","Unused credits roll 1 month","Fastest queue + HD previews","10% off all charm orders"] },
    { id:"m500", handle:"studio-plan-atelier",  name:"Atelier",  credits:500, price:49.99, per:"500 designs / month", feats:["500 credits refreshed monthly","For resellers & power creators","Bulk design export","15% off all charm orders + support line"] },
  ],
};

/* =============================================================================
   TRANSPORT — the exact base the storefront already calls (emailCapture,
   reviews, shopifyEditor, openaiProxy all use it).
   ========================================================================== */
const postJson = async (fn, body, base = FN_BASE, headers = {}) => {
  const res = await fetch(`${base}/${fn}`, {
    method: "POST",
    headers: Object.assign({ "Content-Type": "application/json" }, headers),
    body: JSON.stringify(body),
  });
  const raw = await res.text().catch(() => "");
  let j = null; try { j = raw ? JSON.parse(raw) : null; } catch (e) {}
  if (!res.ok) {
    const err = new Error((j && (j.error && (j.error.message || j.error))) || `HTTP ${res.status}`);
    err.status = res.status; err.payload = j;
    throw err;
  }
  return j;
};
/* Studio calls carry the Firebase ID token — geminiImageProxy-background has
   no caller authentication of its own (§10.0 fact 2), so the studio kinds
   verify the token themselves. */
const postAuthed = async (fn, body, base) => {
  const token = await idToken();
  return postJson(fn, body, base, token ? { Authorization: "Bearer " + token } : {});
};

/* =============================================================================
   FIREBASE — same SDK version and project the Listing Generator uses (§10.3).
   Loaded only on this page, from the studio IIFE, so no other template pays
   for it.
   ========================================================================== */
let fbApp = null, auth = null, db = null, fbReady = null;
function loadScript(src) {
  return new Promise((resolve, reject) => {
    const s = document.createElement("script");
    s.src = src; s.async = false;
    s.onload = resolve;
    s.onerror = () => reject(new Error("Could not load " + src));
    document.head.appendChild(s);
  });
}
async function bootFirebase() {
  if (fbReady) return fbReady;
  fbReady = (async () => {
    if (!FB_CFG.apiKey) throw new Error("Firebase is not configured on this section");
    const base = `https://www.gstatic.com/firebasejs/${FB_VER}/`;
    if (!window.firebase || !window.firebase.initializeApp) {
      await loadScript(base + "firebase-app-compat.js");
    }
    /* idempotent: if anything else on the page already brought the SDK in,
       reuse it rather than loading a second, conflicting copy */
    if (!window.firebase.auth)      await loadScript(base + "firebase-auth-compat.js");
    if (!window.firebase.firestore) await loadScript(base + "firebase-firestore-compat.js");
    fbApp = window.firebase.apps && window.firebase.apps.length
      ? window.firebase.app()
      : window.firebase.initializeApp(FB_CFG);
    auth = window.firebase.auth();
    db = window.firebase.firestore();
    db.settings({ ignoreUndefinedProperties: true });
    return fbApp;
  })();
  return fbReady;
}
async function idToken(force) {
  try {
    await bootFirebase();
    const u = auth && auth.currentUser;
    return u ? await u.getIdToken(!!force) : "";
  } catch (e) { return ""; }
}
const svTime = () => window.firebase.firestore.FieldValue.serverTimestamp();
/* ── HOW YOU DELETE A KEY OUT OF A MAP ────────────────────────────────────
   Every session write is a set(…, { merge: true }), and merge MERGES nested
   maps: writing a markups map with a key missing leaves that key exactly
   where it was in the document, and the next snapshot puts it straight back
   on screen. Deleting anything held in a map — an imported .svg, a sketch —
   therefore looked like it worked and silently did not. A delete sentinel is
   the only thing merge treats as "remove this". */
const svDelete = () => window.firebase.firestore.FieldValue.delete();

/* =============================================================================
   APP STATE — the wallet fields are a MIRROR of users/{uid}.wallet and are
   never written locally; the server owns the quota (§10.6).
   ========================================================================== */
/* the three ways a design can start; step 1 shows exactly one phase per
   mode, and "no mode yet" is what puts the source picker on screen */
const START_MODES = ["search", "upload", "draw"];
const state = {
  uid: null,
  user: null,                       // {name,email,provider} — null = guest
  credits: CONFIG.guestFreeCredits,
  isMember: false,
  uploadsUsed: 0,
  purchasedCredits: 0,              // purchased allowance, extends uploads too
  designing: false,
  step: 1,
  maxStep: 1,
  startMode: null,                  // 'search' | 'upload'
  reference: null,                  // {type:'catalog',item} | {type:'upload',url,path,name}
  metal: "gold",
  desc: "",
  versions: [],
  thread: [],
  currentVersion: -1,
  approved: false,
  unlimited: false,                 // users/{uid}.wallet.unlimited — staff/testing accounts
  stageView: "bw",                  // 'bw' | 'spec' | 'charm' — which stage preview the studio shows
  renderQuality: "low",             // 'low' (Gemini, 1 credit) | 'high' (gpt-image-2, 2 credits)
  promptMode: "full",               // 'full' | 'short' — diagnostic, no price difference
  markups: {},                      // {versionN|"draw": {items:[], mode, crop, …}} — the drawing studio's vector data
  refPlan: [],                      // the three instructions read off the reference's own pixels
  refPlanKey: "",                   // …and which reference they were read from
  signatures: [],                   // reusable hand signatures, kept on the design session
  assets: [],                       // pictures imported into drawings on this session
  mkHistory: {},                    // per-drawing undo/redo snapshots (JSON strings)
  toldAboutFill: false,             // the engraving consequence, explained once
  metalCoach: 0,                    // the "start here" note on the engraving control
  metalSeen: {},                    // which of the three instructions have been used
  metalAsked: {},                   // designs already asked "nothing is marked?"
  sessions: [],
  activeSessionId: null,
  activeTag: null,
};

/* =============================================================================
   UTILITIES (prototype, unchanged)
   ========================================================================== */
function toast(msg, kind = "") {
  const t = document.createElement("div");
  t.className = "toast" + (kind ? " toast--" + kind : "");
  t.innerHTML = msg;
  $("#toastStack").appendChild(t);
  setTimeout(() => { t.classList.add("out"); setTimeout(() => t.remove(), 450); }, 3400);
}
function fmt(n) { return CONFIG.currency + Number(n).toFixed(2); }
function escapeHtml(s) { return String(s).replace(/[&<>"']/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])); }
/* Firestore hands timestamps back as Timestamp objects; sessions written in
   this tab still hold plain numbers. One reader for both. */
function asDate(v) {
  if (!v) return new Date();
  if (typeof v === "number") return new Date(v);
  if (v instanceof Date) return v;
  if (typeof v.toDate === "function") return v.toDate();
  if (typeof v.seconds === "number") return new Date(v.seconds * 1000);
  return new Date(v);
}
const now = () => Date.now();
const tFmt = (d) => asDate(d).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });

/* credits — cosmetic readout of the server-owned wallet */
function renderCredits(bump = false) {
  const n = $("#creditNum"), r = $("#railCredits");
  /* An unlimited account has no number to show — a balance that never moves
     reads as a bug. It shows the symbol instead, and every gate that would
     have opened the pricing modal checks the same flag. */
  const txt = state.unlimited ? "∞" : String(state.credits);
  if (n) n.textContent = txt;
  if (r) r.textContent = txt;
  const l = $("#creditLbl");
  if (l) l.textContent = state.unlimited ? "unlimited designs"
                       : state.isMember ? "member designs left" : "designs left";
  if (bump && n) {
    n.classList.remove("bump"); void n.offsetWidth; n.classList.add("bump");
    setTimeout(() => n.classList.remove("bump"), 900);
    /* The number lives behind the menu now, so a change the shopper cannot see
       gets a gold dot on the button instead — the one piece of state the
       closed control carries on its face. It clears when the panel opens. */
    if (typeof markMenuNews === "function") markMenuNews();
  }
}
/* The client meter never decides anything: custom_charm_generate performs the
   atomic wallet decrement in a Firestore transaction BEFORE Gemini is called,
   and the users/{uid} snapshot below pushes the new balance back to us. */
function spendCredit() { renderCredits(true); }

/* ── WHAT A RENDER COSTS ──────────────────────────────────────────────────
   One place, because three surfaces ask: the button's own price tag, the
   gate that opens the pricing modal, and the meter that ticks down after a
   success. The server charges independently from config/customStudio and is
   the only authority — if the two ever disagree the balance the wallet
   snapshot pushes back wins, which is exactly the right way round. */
const RENDER_QUALITIES = ["low", "high"];
/* ── IS THERE A CHOICE AT ALL? ────────────────────────────────────────────
   Mirrored from config/customStudio, the same key the server reads. Default
   FALSE: while the control is off screen there is one renderer, and the
   default has to be the safe answer for the case where the config read fails
   — a studio that quietly offers a two-credit tier nobody can see is worse
   than one that never offers it. */
function renderTiersOn() { return CONFIG.renderQualityTiers === true; }
/* Always visible. It was gated behind a Firestore boolean, which meant
   reaching a UI toggle required opening the Firebase console — the exact
   thing the toggle exists to avoid. */
function promptModeOn() { return true; }
/* Read here rather than sanitised at every call site, and forced to "full"
   while the control is hidden: a stale session doc must not keep sending a
   mode nobody can see. The stored value is left alone so flipping the config
   restores each design's own choice. */
function promptMode() {
  if (!promptModeOn()) return "full";
  return state.promptMode === "short" ? "short" : "full";
}
function renderQuality() {
  /* Read HERE rather than sanitised at every call site. state.renderQuality
     is persisted per design, so sessions saved while the control existed still
     carry "high" — and every one of those would otherwise price a button, send
     a request and expect a charge for a tier that is not on offer. With the
     tier parked this returns "low" for all of them, matching what the server
     will actually do with the request. The stored value is left alone, so
     turning the tier back on restores each design's own choice. */
  if (!renderTiersOn()) return "low";
  return state.renderQuality === "high" ? "high" : "low";
}
function renderCostFor(q) {
  const base = Number(CONFIG.generateCost) || 1;
  if (q !== "high") return base;
  return Math.max(base, Number(CONFIG.renderHighCost) || base * 2);
}
function renderCost() { return renderCostFor(renderQuality()); }
const creditWord = (n) => n + (n === 1 ? " credit" : " credits");


/* =============================================================================
   WALLET — users/{uid}.wallet, streamed. Provisioned server-side on the first
   wallet_get so a brand-new anonymous visitor really does have 3 credits in
   Firestore, which is what custom_charm_generate checks. Nothing here writes.
   ========================================================================== */
let _walletUnsub = null;
function applyWallet(w) {
  w = w || {};
  state.credits         = Number(w.credits != null ? w.credits : CONFIG.guestFreeCredits);
  state.uploadsUsed     = Number(w.uploadsUsed || 0);
  state.purchasedCredits = Number(w.purchasedAllowance || 0);
  state.isMember        = w.tier === "member";
  state.unlimited       = w.unlimited === true;
  renderCredits();
  renderUploadAllowance();
}
function watchWallet(uid) {
  if (_walletUnsub) { _walletUnsub(); _walletUnsub = null; }
  _walletUnsub = db.doc("users/" + uid).onSnapshot((snap) => {
    const d = snap.exists ? snap.data() : {};
    applyWallet(d.wallet);
    if (d.profile && d.profile.email && !state.user) {
      setAccountChip(d.profile.name || d.profile.email.split("@")[0], d.profile.email, d.profile.provider);
    }
  }, () => { /* offline — the cached balance stands */ });
}
async function refreshWallet() {
  try {
    const r = await postAuthed("britesAuth", { kind: "wallet_get" });
    if (r && r.wallet) applyWallet(r.wallet);
  } catch (e) { /* the snapshot is the primary path */ }
}

/* =============================================================================
   CONFIG MIRROR — config/customStudio, read once at boot and merged over the
   built-in defaults (§10.4). Prices and limits are tunable with no deploy.
   ========================================================================== */
async function loadRemoteConfig() {
  try {
    const snap = await db.doc("config/customStudio").get();
    if (!snap.exists) return;
    const c = snap.data() || {};
    ["guestFreeCredits","signupBonusCredits","guestFreeUploads","signupBonusUploads",
     "maxUploadPx","maxUploadBytes","generateCost","renderHighCost","designFee","engravingFee",
     "extenderHeartFee","maxInstruction"].forEach((k) => {
      if (typeof c[k] === "number") CONFIG[k] = c[k];
    });
    /* booleans, which the numeric loop above would silently drop */
    if (typeof c.renderQualityTiers === "boolean") CONFIG.renderQualityTiers = c.renderQualityTiers;
    if (typeof c.renderPromptModeUI === "boolean") CONFIG.renderPromptModeUI = c.renderPromptModeUI;
    if (Array.isArray(c.sizes) && c.sizes.length) {
      SIZES.length = 0; c.sizes.forEach((s) => SIZES.push(s));
    }
    if (c.types) Object.keys(c.types).forEach((k) => {
      if (TYPES[k]) Object.assign(TYPES[k], c.types[k]);
    });
    if (c.metalPremium) Object.assign(METAL_PREMIUM, c.metalPremium);
    if (c.metalFactor)  Object.assign(METAL_FACTOR,  c.metalFactor);
    if (typeof c.rosePremiumOverGoldFilled === "number") ROSE_PREMIUM = c.rosePremiumOverGoldFilled;
    if (Array.isArray(c.packs) && c.packs.length) CONFIG.packs = c.packs;
    if (Array.isArray(c.plans) && c.plans.length) CONFIG.plans = c.plans;
  } catch (e) { /* defaults stand */ }
}

/* =============================================================================
   DESIGN SESSIONS — the persistence spine, now Firestore-backed.
   Every design is a SESSION: reference, uploads, every message, every version,
   a full event timeline and the wizard position. Written debounced (~500 ms)
   on every mutation to customSessions/{sid}; streamed back with onSnapshot so
   My designs is live and cross-device. Guest sessions ride the anonymous uid
   and survive sign-up untouched, because the uid never changes.
   ========================================================================== */
function activeSession() { return state.sessions.find((s) => s.id === state.activeSessionId) || null; }
function sessionById(id) { return (state.sessions || []).find((s) => s.id === id) || null; }

/* ═══════════ ONE PROJECT AT A TIME, AND ONLY ONE ═════════════════════════
   Everything slow in this studio — a generation, a metal render, a trace, an
   upload — begins in one project and lands some seconds or minutes later, by
   which time the customer may well be in a different one. Every one of those
   continuations writes to `state`, and `state` is by definition whatever
   design is open NOW: the version, the message, the layer sheet and the
   render URL all arrived in the wrong project, and the project they belonged
   to was saved without them.

   So a project switch stamps a new EPOCH. Work carries the epoch it started
   in. If the epoch has moved on by the time it finishes, it does not touch
   `state` at all — it files itself on the SESSION OBJECT it was started with
   and writes that document directly. Nothing lands in the new design, and
   nothing is lost from the old one.

   Three rules, and they are the whole contract:
     1. read the epoch once, at the top, before the first await
     2. after any await, `if (!sameProject(ep))` divert — never mutate state
     3. every divert ends in saveOrphan(), which writes the old document */
let projectEpoch = 1;
function epochNow() { return projectEpoch; }
function sameProject(ep) { return ep === projectEpoch; }
/* File a result on a project the customer has already left. The session
   object still holds its own arrays, so this is a normal save of a document
   that simply is not the open one. */
function saveOrphan(s) {
  if (!s) return Promise.resolve(false);
  s.updatedAt = now();
  s.name = s.name || "Custom charm — custom";
  SD("orphan: filing on", s.id, "(customer has moved on)");
  return writeSessionDocHard(s);
}

/* Keys removed from a map since the last successful write, per session. They
   have to travel as delete sentinels or merge quietly resurrects them. */
const _gone = { markups: new Set(), mkHistory: new Set(), sid: "" };
function forgetKey(where, key) {
  const s = activeSession();
  if (!s || !key) return;
  if (_gone.sid !== s.id) { _gone.markups.clear(); _gone.mkHistory.clear(); _gone.sid = s.id; }
  _gone[where].add(String(key));
}
/* the map as it should END UP: what is there now, plus a tombstone for
   everything that has gone since the last write */
function withDeletes(map, where, sid, delVal) {
  const out = Object.assign({}, map || {});
  const forSid = sid || (activeSession() || {}).id;
  if (_gone.sid === forSid) {
    const DEL = delVal || svDelete();
    _gone[where].forEach((k) => { if (!(k in out)) out[k] = DEL; });
  }
  return out;
}

/* ═══════════ CHANGING THE REFERENCE AFTER WORK EXISTS ════════════════════
   Every version, every message and every markup belongs to the reference it
   came from. Swapping the reference underneath them produced the worst
   possible outcome: the next send was still a REFINEMENT, so the studio was
   handed the OLD drawing as "the current design, keep it" and the new
   reference barely registered — the customer saw their old charm come back
   wearing someone else's clothes.

   So a new reference starts a new design. Nothing is lost: the previous one
   keeps its own reference, versions and conversation and is sitting in My
   designs. The customer gets a clean sheet and a first generation, which is
   what "I changed my mind about the reference" actually means.            */
function refKey(r) {
  if (!r) return "";
  if (r.type === "catalog") return "c:" + ((r.item && (r.item.gid || r.item.id)) || "");
  return "u:" + (r.path || r.url || "");
}
function rebaseOnReference(next, opts) {
  const had = state.versions.length > 0 || (state.thread || []).length > 0;
  /* ── sameDesign: THE REFERENCE MOVED, THE DESIGN DID NOT ──────────────────
     refKey() for an upload is its storage path, and re-submitting the SAME
     artwork mints a new path every time — so a path comparison always reads
     "different reference" and re-based a design that never changed. That wiped
     versions, thread and every metal render into a separate My-designs entry,
     and the next send filed v1 into a brand-new session: regenerating simply
     never accumulated. Callers that know the artwork descends from the design
     already open say so explicitly; every other caller still re-bases. */
  if (!had || (opts && opts.sameDesign) ||
      refKey(state.reference) === refKey(next)) return false;
  const oldName = designName();
  /* A HAND-DRAWN reference is the exception: the sketch does not belong to
     the design being left behind, it IS the new one. Carrying it across is
     what keeps every object in it live and editable afterwards — without
     this, submitting your own drawing threw the drawing away. */
  const keepSketch = !!(opts && opts.keepSketch);
  const sketch = keepSketch ? (state.markups || {}).draw : null;
  const sketchHist = keepSketch ? (state.mkHistory || {}).draw : null;
  /* close the book on the old one before opening a new session */
  saveSession(true);
  state.activeSessionId = null;
  state.versions = [];
  state.thread = [];
  state.markups = sketch ? { draw: sketch } : {};
  state.mkHistory = sketchHist ? { draw: sketchHist } : {};
  state.currentVersion = -1;
  state.approved = false;
  state.maxStep = Math.min(state.maxStep || 1, 2);
  state.reference = next;
  ensureSession();
  logEvent("reference", "Started fresh from a new reference");
  saveSession();
  toast(`New design started — “${oldName}” is safe in My designs`, "gold");
  return true;
}
/* Every path that sets a reference goes through here, so none of them can
   forget the re-base, the save or the stage refresh. */
function setReference(next, label, opts) {
  if (!rebaseOnReference(next, opts)) {
    state.reference = next;
    ensureSession();
    if (label) logEvent("reference", label);
    saveSession();
  }
  renderPickBar();
  if (typeof renderStage === "function") renderStage();
  updateNavArrows();
  /* Been to step 2 already? Then choosing a reference IS the answer to the
     only question step 1 asks, and sitting still afterwards reads as broken. */
  if (state.step === 1 && (state.maxStep || 1) >= 2) {
    setTimeout(() => gotoStep(2), 260);
  } else {
    $("#navNext").focus();
  }
}
/* Change the reference INSIDE the current design session, preserving its
   versions and conversation. This is for the MarkUp modal's reference-side
   re-draw only: the customer is evolving the same design, not starting a new
   one, and kicking that work into a fresh session makes the history look like
   it vanished. */
function replaceReferenceInPlace(next, label, opts) {
  state.reference = next;
  ensureSession();
  if (opts && opts.clearDrawSheet) {
    if (state.markups && state.markups.draw) { delete state.markups.draw; forgetKey("markups", "draw"); }
    if (state.mkHistory && state.mkHistory.draw) { delete state.mkHistory.draw; forgetKey("mkHistory", "draw"); }
  }
  if (label) logEvent("reference", label);
  saveSession();
  renderPickBar();
  if (typeof renderStage === "function") renderStage();
  updateNavArrows();
}

function newSessionId() {
  return db ? db.collection("customSessions").doc().id
            : "d" + Date.now().toString(36) + Math.random().toString(36).slice(2, 7);
}
function ensureSession() {
  let s = activeSession();
  if (s) return s;
  s = { id: newSessionId(), uid: state.uid, createdAt: now(), updatedAt: now(),
        name: "New design", status: "draft", step: 1, maxStep: 1, startMode: null,
        reference: null, metal: "gold", desc: "", versions: [], thread: [],
        currentVersion: -1, approved: false, history: [], markups: {}, signatures: [], assets: [], mkHistory: {} };
  state.sessions.unshift(s);
  state.activeSessionId = s.id;
  logEvent("created", "Design session started");
  return s;
}
function logEvent(type, label, vn) {
  const s = activeSession(); if (!s) return;
  s.history.push({ type, label, vn: vn == null ? null : vn, at: now() });
}

/* The session doc is the unit of persistence, so the reference is stored by
   PATH (uploads) or product GID (catalogue) — never as a data URL, which
   would blow the 1 MB document limit and defeat resumability. */
function refForDoc(r) {
  if (!r) return null;
  if (r.type === "catalog") {
    return { type: "catalog", productGid: r.item.gid || null, productId: r.item.id || null,
             item: { id: r.item.id, gid: r.item.gid || null, title: r.item.title,
                     price: r.item.price, url: r.item.url, image: r.item.image } };
  }
  /* `drawn` and `marks` say the reference came out of the sketchpad rather
     than a camera. Dropping them here is how step 1 forgot, after a reload,
     that the customer had already drawn something — and offered them a blank
     sheet on top of their own work. */
  return { type: "upload", path: r.path, url: r.url, name: r.name, w: r.w, h: r.h,
           resized: !!r.resized, drawn: !!r.drawn, marks: Number(r.marks) || 0,
           /* the customer's own account of every filled area, captured when
              the sketch was flattened — small, flat, and the only thing that
              survives the picture becoming a picture */
           zones: Array.isArray(r.zones) ? r.zones.slice(0, 40) : [] };
}

/* whatever is in the brief box right now, sent or not */
function descDraft() {
  const el = $("#descInput");
  return el ? String(el.value || "").slice(0, 4000) : "";
}
let _saveTimer = null, _savePending = false, _flushBusy = false, _flushAgain = false;
function saveSession(immediate) {
  const s = activeSession(); if (!s) return;
  s.uid = state.uid;
  s.startMode = state.startMode;
  s.reference = refForDoc(state.reference);
  s.metal = state.metal;
  s.renderQuality = state.renderQuality;
  s.promptMode = state.promptMode;
  s.desc = state.desc;
  /* ── TYPED BUT NOT SENT IS STILL PROGRESS ───────────────────────────────
     state.desc only ever grows when Send is pressed, so a paragraph sitting
     in the box belonged to nothing and was the one thing leaving a project
     genuinely destroyed. It rides on the session and goes back in the box
     when the design is reopened. */
  s.draftText = descDraft();
  s.markups = state.markups || {};
  s.signatures = state.signatures || [];
  s.assets = state.assets || [];
  s.mkHistory = state.mkHistory || {};
  s.versions = state.versions.map((v) => Object.assign({}, v));
  s.thread = (state.thread || []).map((m) => Object.assign({}, m));
  s.currentVersion = state.currentVersion;
  s.approved = state.approved;
  s.step = state.step;
  s.maxStep = state.maxStep;
  if (s.status !== "ordered") s.status = state.approved ? "approved" : "draft";
  s.name = designName();
  s.updatedAt = now();
  if ($("#designsDrawer").classList.contains("is-open")) renderDrawer();
  _savePending = true;
  clearTimeout(_saveTimer);
  if (immediate) return flushSession();
  _saveTimer = setTimeout(flushSession, 500);
}
/* The undo/redo stacks are the heaviest thing on the session document, so
   they live under a byte budget: each drawing keeps its most recent
   snapshots, and if the whole bundle still outweighs the budget the OLDEST
   snapshots go first — recent undo is what "continue where I left off"
   actually needs. The items themselves are never touched by this. */
function packHistory(h) {
  const out = {};
  Object.keys(h || {}).forEach((k) => {
    const e = h[k] || {};
    out[k] = { u: (e.u || []).slice(-10), r: (e.r || []).slice(-6) };
  });
  let guard = 0;
  while (JSON.stringify(out).length > 200000 && guard++ < 400) {
    let fatKey = null, fatLen = -1;
    Object.keys(out).forEach((k) => {
      const len = out[k].u.length + out[k].r.length;
      if (len > fatLen) { fatLen = len; fatKey = k; }
    });
    if (!fatKey || fatLen <= 0) break;
    if (out[fatKey].u.length) out[fatKey].u.shift();
    else out[fatKey].r.pop();
    if (!out[fatKey].u.length && !out[fatKey].r.length) delete out[fatKey];
  }
  return out;
}

/* ═══════════ THE DOCUMENT, AND HOW IT GETS THERE ═════════════════════════
   Three functions where there was one, because leaving a project mid-flight
   turned out to need each of them separately:

     buildSessionDoc(s)   the document, built from a SESSION OBJECT and never
                          from `state` — which is what lets a generation that
                          finished after the customer moved on file itself on
                          the design it actually belongs to
     writeSessionDoc      one attempt
     writeSessionDocHard  three attempts, then the browser's own shelf
     flushSessionNow      awaitable, and about a SPECIFIC session

   The debounced flushSession() keeps exactly the behaviour it had; it just
   calls them now. */
function buildSessionDoc(s, plain) {
  /* A plain document is for the outbox, which is JSON on a shelf and cannot
     hold a Firestore sentinel. The marker is swapped back for a real delete
     when the shelf is drained. */
  const DEL = plain ? OUTBOX_DEL : svDelete();
  const doc = {
    uid: s.uid || state.uid, name: s.name, status: s.status, step: s.step, maxStep: s.maxStep,
    startMode: s.startMode, reference: s.reference, desc: s.desc,
    /* typed but not sent — see saveSession */
    draftText: String(s.draftText || ""),
    metal: s.metal,
    /* NEVER PERSISTED UNTIL NOW. saveSession set it and loadSession read it,
       but the document omitted it, so High reverted to Standard on reopen. */
    renderQuality: s.renderQuality === "high" ? "high" : "low",
    promptMode: s.promptMode === "short" ? "short" : "full",
    currentVersion: s.currentVersion, approved: s.approved,
    thread: s.thread, versions: s.versions, history: s.history,
    markups: withDeletes(s.markups, "markups", s.id, DEL),
    signatures: s.signatures || [],
    assets: s.assets || [],
    mkHistory: withDeletes(packHistory(s.mkHistory), "mkHistory", s.id, DEL),
    createdAt: s.createdAt, updatedAt: s.updatedAt,
  };
  if (!plain) doc.updatedAtServer = svTime();
  /* ── THE ONE MEGABYTE ─────────────────────────────────────────────────
     A session is ONE Firestore document with a hard ceiling, and the way it
     is reached is never dramatic: a few very detailed fills, a handful of
     imported documents, a long conversation. Past the ceiling EVERY write
     fails, for ever, behind a toast the customer sees once a minute — and
     everything they do afterwards is lost.

     So the document is weighed before it is sent, and if it is close to the
     edge the two things that are large AND expendable are shed: the undo
     history first, because it is a convenience, and then the oldest
     imported documents, because the artwork they carry is already on the
     sheet. What is never shed is the customer's actual design. */
  try {
    const LIMIT = 950 * 1024;
    let size = 0;
    try { size = JSON.stringify(doc).length; } catch (er) { size = 0; }
    if (size > LIMIT) {
      const live = s.id === state.activeSessionId;
      doc.mkHistory = {};
      /* WHAT MAY BE SHED, IN THE ORDER IT MAY BE SHED.

         First the imported documents, oldest first: the artwork they carry
         is already on a sheet, and the entry is a convenience.

         Then — and this is the one the guard used to miss entirely — the
         OBJECTS TAKEN OUT OF OLD GENERATED DRAWINGS. Every version of every
         design is automatically split into editable parts a second or two
         after it arrives, and those parts live under the version's own key.
         They are worth tens of kilobytes each, they were not shed-able, and
         a busy design therefore crossed the ceiling at about the ninth
         version and could never be saved again — the guard ran, freed
         nothing, and the write failed anyway.

         They can go because they are DERIVED: the drawing itself is a file
         in storage, and opening an old version re-traces it. What is never
         shed is a version the customer has actually marked up (`dirty`),
         the one they are looking at, or their own sketch. */
      const cur = String(s.currentVersion != null ? (s.versions || [])[s.currentVersion] &&
                         (s.versions || [])[s.currentVersion].n : "");
      const shed = [];
      Object.keys(doc.markups || {}).forEach((k) => {
        const m = doc.markups[k];
        if (!m || !m.items) return;
        if (k.indexOf("imp:") === 0) { shed.push({ k, rank: 0, at: Number(m.impAt) || 0 }); return; }
        if (k === "draw" || k === cur) return;              /* never */
        if (m.dirty) return;                                /* they marked it up */
        if (!m.traced) return;                              /* not derived */
        shed.push({ k, rank: 1, at: Number(m.updatedAt) || 0 });
      });
      shed.sort((a, b) => (a.rank - b.rank) || (a.at - b.at));
      while (shed.length && JSON.stringify(doc).length > LIMIT) {
        const k = shed.shift().k;
        doc.markups[k] = DEL;
        if (s.markups) delete s.markups[k];
        /* `state` is only the same design while this session is the open one
           — shedding out of a document being written in the background must
           never reach into the project the customer is looking at now */
        if (live && state.markups) delete state.markups[k];
      }
      s.mkHistory = {};
      if (live && (!buildSessionDoc._shedAt || Date.now() - buildSessionDoc._shedAt > 60000)) {
        buildSessionDoc._shedAt = Date.now();
        toast("This design is very full, so we've let go of its undo history and " +
              "the loose pieces of some older versions. Nothing you have drawn is " +
              "lost — open an older version and it comes apart again.", "note");
      }
    }
  } catch (e) { /* a doc that cannot be weighed is still a doc worth sending */ }
  return doc;
}

/* one attempt, and an honest boolean */
async function writeSessionDoc(s, doc) {
  if (!db || !state.uid || !s) return false;
  try {
    await db.collection("customSessions").doc(s.id).set(doc, { merge: true });
    /* the tombstones have landed; they need not travel again */
    if (_gone.sid === s.id) { _gone.markups.clear(); _gone.mkHistory.clear(); }
    outboxDrop(s.id);
    return true;
  } catch (e) {
    console.warn("[studio] session save failed:", e && e.message);
    return false;
  }
}

/* THREE GOES, THEN THE SHELF. This is the one used when a design is being
   left behind, so "it failed" is not an acceptable resting place: the
   document is rebuilt and retried, and if the network is simply gone the
   whole thing is parked in localStorage and written on the next drain. */
async function writeSessionDocHard(s) {
  if (!s) return false;
  for (let i = 0; i < 3; i++) {
    if (await writeSessionDoc(s, buildSessionDoc(s))) return true;
    /* shelved after the FIRST failure rather than the third: two more
       attempts take three seconds, and a tab can close in one. A successful
       write drops the shelf copy again (see writeSessionDoc). */
    outboxPut(s);
    await new Promise((r) => setTimeout(r, 400 + i * 900));
  }
  return false;
}

/* Awaitable, and about a session by NAME rather than "whatever is active
   when the timer happens to fire". That distinction is the bug: leaving a
   project inside the 500 ms debounce window meant the flush arrived after
   activeSessionId had already been cleared, found nothing to save, and
   dropped the last edits of the design being left behind on the floor. */
async function flushSessionNow(sOpt) {
  const s = sOpt || activeSession();
  if (!s) return true;
  if (s.id === (activeSession() || {}).id) { _savePending = false; clearTimeout(_saveTimer); }
  /* let any write already in the air land first, so the two cannot interleave */
  for (let i = 0; i < 60 && _flushBusy; i++) await new Promise((r) => setTimeout(r, 50));
  _flushBusy = true;
  try { return await writeSessionDocHard(s); }
  finally { _flushBusy = false; }
}

async function flushSession() {
  clearTimeout(_saveTimer);
  if (!_savePending || !db || !state.uid) return;
  if (_flushBusy) { _flushAgain = true; return; }
  const s = activeSession(); if (!s) return;
  _savePending = false;
  _flushBusy = true;
  try {
    const ok = await writeSessionDoc(s, buildSessionDoc(s));
    if (!ok) {
      /* A silent failure here already cost real work once (a nested array made
         every write fail while the UI carried on as if saved). Say so — once a
         minute at most — keep the dirty flag so the next mutation retries, and
         put a copy on the shelf immediately rather than only after three goes:
         the tab can be closed at any moment. */
      _savePending = true;
      outboxPut(s);
      if (!flushSession._warnedAt || Date.now() - flushSession._warnedAt > 60000) {
        flushSession._warnedAt = Date.now();
        toast("Your design isn't syncing right now — we'll keep retrying", "err");
      }
    }
  }
  finally {
    _flushBusy = false;
    if (_flushAgain || _savePending) {
      _flushAgain = false;
      clearTimeout(_saveTimer);
      _saveTimer = setTimeout(flushSession, 120);
    }
  }
}

/* ═══════════ THE SHELF ═══════════════════════════════════════════════════
   Firestore is the store; this is the thing that stands between a customer's
   work and a dropped connection at the exact moment they walk away from a
   design. A document that will not write is parked in localStorage — keyed
   by session id, stamped with the uid that owns it — and drained on a timer,
   when the network comes back, and at boot. It is deliberately small: three
   documents at most, newest kept.

   It never writes over newer work. A shelved copy of the design that is
   CURRENTLY OPEN is thrown away rather than sent, because the live one is by
   definition more recent; and a deleted design's entry is removed by
   deleteDesign, so the shelf can never resurrect it. */
const OUTBOX_KEY = "bjStudioOutbox";
const OUTBOX_DEL = "__BJ_DELETE__";
function outboxRead() {
  try { return JSON.parse(window.localStorage.getItem(OUTBOX_KEY) || "{}") || {}; }
  catch (e) { return {}; }
}
function outboxWrite(o) {
  try { window.localStorage.setItem(OUTBOX_KEY, JSON.stringify(o)); return true; }
  catch (e) { return false; }
}
function outboxPut(s) {
  if (!s || !s.id) return;
  try {
    const o = outboxRead();
    o[s.id] = { uid: state.uid || "", at: Date.now(), doc: buildSessionDoc(s, true) };
    let keys = Object.keys(o).sort((a, b) => (o[a].at || 0) - (o[b].at || 0));
    while (keys.length > 3) delete o[keys.shift()];
    /* a full quota is not a reason to keep nothing — shed to the newest one */
    while (!outboxWrite(o) && keys.length > 1) delete o[keys.shift()];
    SD("outbox: shelved", s.id);
  } catch (e) { /* no shelf available; the retry loop is still running */ }
}
function outboxDrop(sid) {
  try { const o = outboxRead(); if (o[sid]) { delete o[sid]; outboxWrite(o); } } catch (e) {}
}
function outboxHydrate(v) {
  if (v === OUTBOX_DEL) return svDelete();
  if (Array.isArray(v)) return v.map(outboxHydrate);
  if (v && typeof v === "object") {
    const out = {};
    Object.keys(v).forEach((k) => { out[k] = outboxHydrate(v[k]); });
    return out;
  }
  return v;
}
let _drainBusy = false;
async function outboxDrain() {
  if (_drainBusy || !db || !state.uid) return;
  const o = outboxRead();
  const ids = Object.keys(o);
  if (!ids.length) return;
  _drainBusy = true;
  try {
    for (let i = 0; i < ids.length; i++) {
      const sid = ids[i], e = o[sid];
      if (!e || !e.doc) { outboxDrop(sid); continue; }
      /* never write one account's work into another's */
      if (e.uid && e.uid !== state.uid) continue;
      /* the open design is newer than any copy of it on a shelf */
      if (sid === state.activeSessionId) { outboxDrop(sid); saveSession(); continue; }
      try {
        await db.collection("customSessions").doc(sid).set(outboxHydrate(e.doc), { merge: true });
        outboxDrop(sid);
        SD("outbox: recovered", sid);
      } catch (err) { /* leave it on the shelf and try again later */ }
    }
  } finally { _drainBusy = false; }
}
setInterval(() => { outboxDrain(); }, 30000);
window.addEventListener("online", () => { outboxDrain(); });


/* ═══════════ DELETING THINGS, PERMANENTLY ════════════════════════════════
   Two doors, because there are two things a customer means by "delete":
   a whole DESIGN out of My designs, and one IMAGE out of the repository.

   Both write to the same session documents everything else reads, so the
   listener carries the change to every open tab and to the library without
   anything extra being wired. The stored FILE is a separate matter: the
   studio has no Storage SDK loaded, so britesAuth is asked to remove the
   object. That call is best-effort by design — if the function has not been
   redeployed the record still goes, and the orphaned bytes are invisible to
   everyone and cost fractions of a cent.                                   */
/* Deleting is the one thing in the studio that cannot be undone, so it is
   the one thing that asks first — and says exactly what will go. Built on
   demand rather than shipped in the markup: it is a handful of nodes, and
   an always-present dialog is a thing every future layout has to step
   around. Returns a promise that resolves true only if they meant it. */
function askConfirm(opts) {
  const o = opts || {};
  return new Promise((resolve) => {
    const wrap = document.createElement("div");
    wrap.className = "bj-ask";
    wrap.innerHTML = `
      <div class="bj-ask__scrim" data-no></div>
      <div class="bj-ask__box" role="alertdialog" aria-modal="true" aria-labelledby="bjAskT">
        <h3 class="bj-ask__title" id="bjAskT"></h3>
        <p class="bj-ask__body"></p>
        <div class="bj-ask__row">
          <button class="btn btn--ghost" type="button" data-no></button>
          <button class="btn ${o.danger === false ? "btn--gold" : "btn--danger"}" type="button" data-yes></button>
        </div>
      </div>`;
    wrap.querySelector(".bj-ask__title").textContent = o.title || "Are you sure?";
    wrap.querySelector(".bj-ask__body").textContent = o.body || "";
    wrap.querySelector("[data-no]:not(.bj-ask__scrim)").textContent = o.no || "Keep it";
    wrap.querySelector("[data-yes]").textContent = o.yes || "Delete for good";
    /* on document.body, not inside .bj-studio: the designs drawer and the
       studio modal both sit in stacking contexts of their own, and a dialog
       that can be painted over by the thing it is asking about is not a
       dialog. Its styles are written without the studio prefix for the same
       reason, and carry their own colours rather than inheriting variables
       that only exist further down the tree. */
    document.body.appendChild(wrap);
    const done = (v) => {
      document.removeEventListener("keydown", key, true);
      wrap.remove();
      resolve(v);
    };
    const key = (e) => { if (e.key === "Escape") { e.preventDefault(); done(false); } };
    document.addEventListener("keydown", key, true);
    wrap.querySelectorAll("[data-no]").forEach((b) => b.addEventListener("click", () => done(false)));
    wrap.querySelector("[data-yes]").addEventListener("click", () => done(true));
    setTimeout(() => { try { wrap.querySelector("[data-yes]").focus(); } catch (e) {} }, 20);
  });
}

async function forgetStoredObject(path) {
  if (!path || /^data:/.test(String(path))) return false;
  try {
    const r = await postAuthed("britesAuth", { kind: "asset_delete", path });
    return !!(r && r.ok);
  } catch (e) { return false; }
}
async function deleteDesign(sid) {
  const s = (state.sessions || []).find((x) => x.id === sid);
  if (!s) return false;
  if (!db || !state.uid) { toast("You're offline — try again in a moment", "err"); return false; }
  const wasActive = state.activeSessionId === sid;
  /* the bytes first, quietly and in parallel; the record is what matters */
  const paths = [];
  (s.versions || []).forEach((v) => { if (v.path) paths.push(v.path); if (v.renderPath) paths.push(v.renderPath); });
  if (s.reference && s.reference.path) paths.push(s.reference.path);
  (s.assets || []).forEach((a) => { if (a.path) paths.push(a.path); });
  try {
    /* the ACTIVE design must stop writing itself back before the row goes,
       or the debounced save would quietly recreate the document */
    if (wasActive) { _savePending = false; clearTimeout(_saveTimer); state.activeSessionId = null; }
    /* a copy waiting on the shelf would write the document straight back */
    outboxDrop(sid);
    await db.collection("customSessions").doc(sid).delete();
  } catch (e) {
    console.warn("[studio] delete failed:", e && e.message);
    toast("That design couldn't be deleted — " + ((e && e.message) || "try again"), "err");
    if (wasActive) state.activeSessionId = sid;
    return false;
  }
  state.sessions = (state.sessions || []).filter((x) => x.id !== sid);
  outboxDrop(sid);
  if (wasActive && typeof startOverStudio === "function") startOverStudio(false);
  /* unconditionally, not only while the drawer is open: a list that still
     shows a row for a document that no longer exists is a list that lies */
  renderDrawer();
  if (typeof mkLibRefresh === "function") mkLibRefresh();
  if (typeof refreshLibraryEntry === "function") refreshLibraryEntry();
  paths.forEach((p) => { forgetStoredObject(p); });
  return true;
}

/* One image out of the repository. Where it lives decides what happens to
   it: an added picture leaves its session's asset list, a reference leaves
   the design it was the reference FOR, and a generated version leaves that
   design's version list — which is why the confirmation says so first. */
async function deleteLibraryImage(entry) {
  const s = (state.sessions || []).find((x) => x.id === (entry && entry.sid));
  if (!s) return false;
  if (!db || !state.uid) { toast("You're offline — try again in a moment", "err"); return false; }
  const live = activeSession() && activeSession().id === s.id;
  const paths = [];
  const patch = {};
  if (entry.kind === "upload" && entry.sub === "added to a drawing") {
    const next = (s.assets || []).filter((a) => (a.path || a.url) !== (entry.path || entry.url));
    s.assets = next; patch.assets = next;
    if (live) state.assets = next;
    if (entry.path) paths.push(entry.path);
  } else if (entry.kind === "design" && entry.impKey) {
    /* an imported document IS its layers — there is no separate picture to
       delete, only the objects and (for a Photoshop file) the one small
       composite that was uploaded to show it */
    const rec = (s.markups || {})[entry.impKey] || {};
    const m = Object.assign({}, s.markups || {}); delete m[entry.impKey];
    s.markups = m;
    /* a tombstone, not an omission — see svDelete */
    patch.markups = Object.assign({}, m, { [entry.impKey]: svDelete() });
    if (live) { state.markups = m; forgetKey("markups", entry.impKey); }
    if (entry.path) paths.push(entry.path);
    /* a Photoshop document uploaded one PNG per layer on the way in; they
       are recorded on the entry precisely so that deleting it takes them
       too, rather than leaving files nothing can ever reach again */
    (rec.impFiles || []).forEach((f) => { if (f) paths.push(f); });
  } else if (entry.kind === "sketch" && entry.local) {
    /* a sketch that was never submitted IS its layers — deleting the image
       means deleting the drawing */
    const m = Object.assign({}, s.markups || {}); delete m.draw;
    const h = Object.assign({}, s.mkHistory || {}); delete h.draw;
    s.markups = m; s.mkHistory = h;
    patch.markups = Object.assign({}, m, { draw: svDelete() });
    patch.mkHistory = Object.assign({}, h, { draw: svDelete() });
    if (live) {
      state.markups = m; state.mkHistory = h;
      forgetKey("markups", "draw"); forgetKey("mkHistory", "draw");
    }
  } else if (s.reference && (s.reference.path || s.reference.url) === (entry.path || entry.url)) {
    s.reference = null; patch.reference = null;
    if (live) state.reference = null;
    if (entry.path) paths.push(entry.path);
  } else {
    const before = (s.versions || []).length;
    const next = (s.versions || []).filter((v) => (v.path || v.url) !== (entry.path || entry.url));
    if (next.length === before) { toast("That image is already gone", "err"); return false; }
    (s.versions || []).forEach((v) => {
      if ((v.path || v.url) === (entry.path || entry.url)) {
        if (v.path) paths.push(v.path);
        if (v.renderPath) paths.push(v.renderPath);
      }
    });
    s.versions = next; patch.versions = next;
    patch.currentVersion = Math.min(s.currentVersion || 0, next.length - 1);
    s.currentVersion = patch.currentVersion;
    if (live) {
      state.versions = next;
      state.currentVersion = patch.currentVersion;
      if (typeof renderStage === "function") renderStage();   // paints the strip too
    }
  }
  patch.updatedAt = now(); patch.updatedAtServer = svTime();
  s.updatedAt = patch.updatedAt;
  try { await db.collection("customSessions").doc(s.id).set(patch, { merge: true }); }
  catch (e) {
    console.warn("[studio] image delete failed:", e && e.message);
    toast("That image couldn't be deleted — " + ((e && e.message) || "try again"), "err");
    return false;
  }
  if (typeof mkLibRefresh === "function") mkLibRefresh();
  if (typeof refreshLibraryEntry === "function") refreshLibraryEntry();
  renderDrawer();
  paths.forEach((p) => { forgetStoredObject(p); });
  return true;
}

/* nothing is ever lost to a tab close mid-debounce. flushSession only writes
   when something is pending, so the extra shelf copy costs nothing on a
   clean exit and is the whole story on a dirty one. */
window.addEventListener("pagehide", () => {
  try { flushSession(); } catch (e) {}
  try { const s = activeSession(); if (s && _savePending) outboxPut(s); } catch (e) {}
});
document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "hidden") { try { flushSession(); } catch (e) {} }
  else { try { outboxDrain(); } catch (e) {} }
});

/* ONE listener, for everything.
   My designs reads it, and so does the image library in the drawing studio —
   every uploaded and generated picture on the account is derived from these
   documents rather than stored a second time. That is deliberate: a separate
   assets collection would mean another write on every generation, another
   listener per visitor and another thing to fall out of step, and it would
   buy nothing this query does not already deliver in real time. The window
   starts small and widens only when somebody actually asks for older work. */
let _sessionsUnsub = null, _sessionUid = null;
const SESSION_WINDOW_STEP = 60;
function watchSessions(uid) {
  if (_sessionsUnsub) { _sessionsUnsub(); _sessionsUnsub = null; }
  _sessionUid = uid;
  if (!state.sessionWindow) state.sessionWindow = SESSION_WINDOW_STEP;
  _sessionsUnsub = db.collection("customSessions")
    .where("uid", "==", uid).orderBy("updatedAt", "desc").limit(state.sessionWindow)
    .onSnapshot((qs) => {
      const remote = qs.docs.map((d) => Object.assign({ id: d.id }, d.data()))
        .map((s) => Object.assign(s, {
          versions: s.versions || [], thread: s.thread || [], history: s.history || [],
        }));
      state.sessionsComplete = qs.docs.length < state.sessionWindow;
      // the session being edited in this tab always wins over its own echo
      const live = activeSession();
      state.sessions = remote.map((s) => (live && s.id === live.id) ? live : s);
      if (live && !state.sessions.some((s) => s.id === live.id)) state.sessions.unshift(live);
      if ($("#designsDrawer").classList.contains("is-open")) renderDrawer();
      /* the library is a view over exactly this data, so it refreshes here
         and nowhere else — one source, one moment of truth */
      if (typeof mkLibRefresh === "function") mkLibRefresh();
      if (typeof refreshLibraryEntry === "function") refreshLibraryEntry();
    }, (e) => console.warn("[studio] sessions listener:", e.message));
}
/* "Load older designs" — a wider window on the SAME query, swapped in place.
   Costs one re-subscription, only when a customer asks for it, never on load. */
async function widenSessionWindow() {
  if (!db || !_sessionUid || state.sessionsComplete) return false;
  state.sessionWindow = (state.sessionWindow || SESSION_WINDOW_STEP) + SESSION_WINDOW_STEP * 2;
  watchSessions(_sessionUid);
  await new Promise((r) => setTimeout(r, 700));
  return true;
}

function restoreStartModeUI() {
  const mode = state.startMode;
  $("#phaseSource").hidden = START_MODES.indexOf(mode) >= 0;
  $("#phaseSearch").hidden = mode !== "search";
  $("#phaseUpload").hidden = mode !== "upload";
  $("#phaseDraw").hidden = mode !== "draw";
  if (mode === "search") renderCharmGrid();
  if (mode === "upload") resetUploadStage(!state.reference || state.reference.type !== "upload");
  if (mode === "draw") renderDrawStage();
}
function loadSession(id) {
  const s = state.sessions.find((x) => x.id === id); if (!s) return;
  /* ── OPENING ONE DESIGN MEANS LEAVING ANOTHER ─────────────────────────
     This used to swap activeSessionId and nothing else, so a design left
     inside its 500 ms save debounce lost its last edits — the flush fired
     afterwards, found the NEW session active, and wrote that one instead.
     Same freeze as the menu shortcuts, quietly: the toast here is the one
     about picking up where you left off. */
  if (id !== state.activeSessionId) leaveCurrentProject({ quiet: true });
  state.activeSessionId = id;
  state.startMode = s.startMode;
  state.reference = s.reference ? JSON.parse(JSON.stringify(s.reference)) : null;
  state.metal = s.metal || "gold";
  state.renderQuality = s.renderQuality === "high" ? "high" : "low";
  state.promptMode = s.promptMode === "short" ? "short" : "full";
  state.desc = s.desc || "";
  state.versions = (s.versions || []).map((v) => Object.assign({}, v));
  state.thread = (s.thread || []).map((m) => Object.assign({}, m));
  state.currentVersion = s.currentVersion;
  state.approved = !!s.approved;
  state.markups = s.markups ? JSON.parse(JSON.stringify(s.markups)) : {};
  state.signatures = s.signatures ? JSON.parse(JSON.stringify(s.signatures)) : [];
  state.assets = s.assets ? JSON.parse(JSON.stringify(s.assets)) : [];
  state.mkHistory = s.mkHistory ? JSON.parse(JSON.stringify(s.mkHistory)) : {};
  /* land on the half of the pair this version actually has */
  const _cv = (s.versions || [])[s.currentVersion];
  state.stageView = _cv && _cv.renderUrl ? "charm" : "bw";
  state.maxStep = s.maxStep || (s.approved ? 3 : ((s.versions || []).length || s.desc || s.reference) ? 2 : 1);
  state.designing = true;
  /* the brief they typed and never sent, back in the box where they left it */
  $("#descInput").value = String(s.draftText || "");
  $("#charCount").textContent = String(($("#descInput").value || "").length);
  try { MK_TRACED.clear(); } catch (e) {}
  try { __mkClip = null; } catch (e) {}
  try { __mkPrev = { key: "", frame: 0, at: null, cv: null }; } catch (e) {}
  try { mkRegionForget(); } catch (e) {}
  try { _gone.markups.clear(); _gone.mkHistory.clear(); _gone.sid = ""; } catch (e) {}
  restoreStartModeUI();
  renderRefStrip();
  autoGrow();
  renderStage();
  renderThread();
  if (s.approved && state.versions.length) refreshApproveStage();
  $("#sessionModal").classList.remove("is-open");
  $("#designsDrawer").classList.remove("is-open");
  gotoStep(Math.min(3, s.step || (state.versions.length ? 2 : (s.desc || s.reference ? 2 : 1))), true);
  toast("Picked up right where you left off ✦", "gold");
}

/* =============================================================================
   ZOOM & PAN — ported from Listing_Generator_1.html attachPreviewBoxListeners:
   wheel zoom (snap back at ≤1, max 8×), damped drag-pan with clamped offsets,
   click-to-zoom toward the clicked point, click again to reset, ResizeObserver
   re-clamp. Works on <img> or <svg> media; queries at event time so re-rendered
   stages keep working with one attachment.               [prototype, verbatim]
   ========================================================================== */
function attachZoomPan(box, mediaSel = "img, svg") {
  if (!box || box.dataset.zoomAttached) return;
  box.dataset.zoomAttached = "1";
  box.style.cursor = "zoom-in";
  const getMedia = () => {
    const m = box.querySelector(mediaSel);
    return (m && !m.closest(".stage-empty") && !m.closest(".gen-overlay")) ? m : null;
  };
  function dims(el) {
    const r = el.getBoundingClientRect();
    const s = parseFloat(el.dataset.scale) || 1;
    return { w: (r.width / s) || el.clientWidth || 120, h: (r.height / s) || el.clientHeight || 120 };
  }
  function clampAndApply(img, scale, offX, offY) {
    const viewportW = box.clientWidth || 120;
    const viewportH = box.clientHeight || 120;
    const d = dims(img);
    const scaledW = d.w * scale, scaledH = d.h * scale;
    const maxX = Math.max(0, (scaledW - viewportW) / 2 / scale);
    const maxY = Math.max(0, (scaledH - viewportH) / 2 / scale);
    offX = Math.max(-maxX, Math.min(maxX, offX));
    offY = Math.max(-maxY, Math.min(maxY, offY));
    img.dataset.scale = scale;
    img.dataset.offsetX = offX;
    img.dataset.offsetY = offY;
    img.style.transition = "none";
    img.style.transform = `scale(${scale}) translate(${offX}px, ${offY}px)`;
    box.style.cursor = scale > 1 ? "grab" : "zoom-in";
  }
  let isDragging = false, isMouseDown = false;
  let dragStartX = 0, dragStartY = 0, lastX = 0, lastY = 0;
  const dragThreshold = 3, MAX_SCALE = 8;
  if (typeof ResizeObserver !== "undefined") {
    const ro = new ResizeObserver(() => {
      const img = getMedia(); if (!img) return;
      clampAndApply(img, parseFloat(img.dataset.scale) || 1,
        parseFloat(img.dataset.offsetX) || 0, parseFloat(img.dataset.offsetY) || 0);
    });
    ro.observe(box);
  }
  box.addEventListener("wheel", (ev) => {
    const img = getMedia(); if (!img) return;
    ev.preventDefault();
    let currentScale = parseFloat(img.dataset.scale) || 1;
    let offX = parseFloat(img.dataset.offsetX) || 0;
    let offY = parseFloat(img.dataset.offsetY) || 0;
    if (ev.deltaY < 0) { currentScale *= 1.1; }
    else {
      currentScale /= 1.1;
      if (currentScale <= 1) { currentScale = 1; offX = 0; offY = 0; }
    }
    if (currentScale > MAX_SCALE) currentScale = MAX_SCALE;
    clampAndApply(img, currentScale, offX, offY);
  }, { passive: false });
  box.addEventListener("mousedown", (ev) => {
    const img = getMedia(); if (!img) return;
    if ((parseFloat(img.dataset.scale) || 1) <= 1) return;
    ev.preventDefault();
    isMouseDown = true; isDragging = false;
    dragStartX = ev.clientX; dragStartY = ev.clientY;
    lastX = ev.clientX; lastY = ev.clientY;
  });
  box.addEventListener("mousemove", (ev) => {
    if (!isMouseDown) return;
    ev.preventDefault();
    if (Math.abs(ev.clientX - dragStartX) > dragThreshold ||
        Math.abs(ev.clientY - dragStartY) > dragThreshold) isDragging = true;
    const img = getMedia(); if (!img) return;
    const scaleNow = parseFloat(img.dataset.scale) || 1;
    let offX = (parseFloat(img.dataset.offsetX) || 0) + (ev.clientX - lastX) * 0.5;
    let offY = (parseFloat(img.dataset.offsetY) || 0) + (ev.clientY - lastY) * 0.5;
    clampAndApply(img, scaleNow, offX, offY);
    lastX = ev.clientX; lastY = ev.clientY;
  });
  box.addEventListener("mouseup",    () => { isMouseDown = false; });
  box.addEventListener("mouseleave", () => { isMouseDown = false; });
  box.addEventListener("click", (ev) => {
    if (isDragging) { isDragging = false; return; }
    const img = getMedia(); if (!img) return;
    const s0 = parseFloat(img.dataset.scale) || 1;
    const offX0 = parseFloat(img.dataset.offsetX) || 0;
    const offY0 = parseFloat(img.dataset.offsetY) || 0;
    const rect = box.getBoundingClientRect();
    const cx = rect.left + box.clientWidth / 2;
    const cy = rect.top + box.clientHeight / 2;
    const px = (ev.clientX - cx) / s0 - offX0;
    const py = (ev.clientY - cy) / s0 - offY0;
    let targetOffX = -px, targetOffY = -py;
    const d = dims(img);
    const halfW = d.w / 2, halfH = d.h / 2;
    const requiredScaleFor = (delta, half) => {
      const ratio = Math.abs(delta) / half;
      return ratio >= 1 ? Infinity : 1 / (1 - ratio);
    };
    const DESIRED = 1.33;
    let s1 = Math.max(DESIRED, s0 * 1.5, requiredScaleFor(targetOffX, halfW), requiredScaleFor(targetOffY, halfH));
    if (!Number.isFinite(s1)) s1 = MAX_SCALE;
    if (s1 > MAX_SCALE) s1 = MAX_SCALE;
    if (s0 > 1.2 && Math.abs(s1 - s0) < 0.5) { s1 = 1; targetOffX = 0; targetOffY = 0; }
    clampAndApply(img, s1, targetOffX, targetOffY);
  });
}

/* =============================================================================
   STEP NAVIGATION                                        [prototype, verbatim]
   ========================================================================== */
function updateDesignMode() {
  const designing = state.designing || state.step > 1 || !!state.startMode ||
                    !!state.reference || !!state.desc.trim() || state.versions.length > 0;
  ROOT.classList.toggle("is-designing", designing);
  document.body.classList.toggle("bj-designing", designing);
  const showSub = designing && state.step === 1 && (state.maxStep || 1) === 1;
  const st = $("#stepper"), sb = $("#subStepperBar");
  if (st && sb) {
    st.style.display = showSub ? "none" : "";
    sb.hidden = !showSub;
    if (showSub) renderSubStepper();
  }
  updateNavArrows();
  /* the bar only exists in designing mode, so its height must be re-measured
     the moment the mode flips — the menu button centres against it */
  if (typeof measureBar === "function") requestAnimationFrame(() => measureBar());
}
function beginDesign() { state.designing = true; gotoStep(1, true); }
function renderSubStepper() {
  const mode = state.startMode;
  const picked = START_MODES.indexOf(mode) >= 0;
  $("#subSteps").innerHTML = picked
    ? `<span class="sub is-done"><i>✓</i>Source</span><span class="sub-line is-done"></span><span class="sub is-active"><i>2</i>Your pick</span>`
    : `<span class="sub is-active"><i>1</i>Source</span><span class="sub-line"></span><span class="sub"><i>2</i>Your pick</span>`;
}
function renderStepper() {
  const n = state.step, max = state.maxStep || 1;
  const showSub = ROOT.classList.contains("is-designing") && n === 1 && (state.maxStep || 1) === 1;
  $("#stepper").style.display = showSub ? "none" : "";
  $("#subStepperBar").hidden = !showSub;
  if (showSub) renderSubStepper();
  $$("#stepper .step").forEach((el) => {
    const sn = +el.dataset.step;
    el.classList.toggle("is-active", sn === n);
    el.classList.toggle("is-done", sn < n);
    el.classList.toggle("is-reach", sn > n && sn <= max);
  });
  $$("#stepper .step-sep").forEach((sep, i) => sep.classList.toggle("is-done", i < n - 1));
}
function gotoStep(n, force = false) {
  n = Math.max(1, Math.min(3, n));
  if (n === state.step && !force) return;
  state.step = n;
  state.maxStep = Math.max(state.maxStep || 1, n);
  const s = activeSession(); if (s) { s.step = n; s.maxStep = state.maxStep; s.updatedAt = now(); }
  $$(".step-view").forEach((v) => v.classList.remove("is-visible"));
  $("#view-" + n).classList.add("is-visible");
  if (n === 2) requestAnimationFrame(() => { autoGrow(); if (!genBusy) $("#descInput").focus(); });
  if (n === 3 && state.versions[state.currentVersion]) {
    if (!state.approved) {
      state.approved = true;
      ensureSession();
      logEvent("approve", "Design approved ♥", state.versions[state.currentVersion].n);
      saveSession();
    }
    refreshApproveStage();
  }
  renderStepper();
  updateDesignMode();
  syncBrief();
  /* the wizard owns the viewport: return to the top of the studio, not of the
     document, so the theme header stays put */
  const top = ROOT.getBoundingClientRect().top + window.scrollY - headerOffset();
  window.scrollTo({ top: Math.max(0, top), behavior: "smooth" });
}
function headerOffset() {
  const cs = getComputedStyle(document.documentElement);
  const h = parseFloat(cs.getPropertyValue("--bj-header-h")) || 92;
  const a = parseFloat(cs.getPropertyValue("--bj-announce-h")) || 0;
  return h + a;
}
$$("#stepper .step").forEach((s) => s.addEventListener("click", () => {
  const n = +s.dataset.step;
  if (n !== state.step && n <= (state.maxStep || 1)) gotoStep(n);
}));

function canGoPrev() {
  if (!ROOT.classList.contains("is-designing")) return false;
  if (state.step === 1) return !!state.startMode;
  return state.step > 1;
}
function canGoNext() {
  if (!ROOT.classList.contains("is-designing")) return false;
  if (state.step === 1) return !!state.startMode && !!state.reference;
  if (state.step === 2) return state.approved;
  return false;
}
function updateNavArrows() {
  const p = $("#navPrev"), n = $("#navNext");
  if (!p || !n) return;
  p.disabled = !canGoPrev();
  n.disabled = !canGoNext();
  n.style.visibility = state.step === 3 ? "hidden" : "";
}
$("#navPrev").addEventListener("click", () => {
  if (!canGoPrev()) return;
  if (state.step === 1) { setStartMode(null); return; }
  gotoStep(state.step - 1);
});
$("#navNext").addEventListener("click", () => {
  if (!canGoNext()) {
    if (state.step === 1 && !state.startMode) toast("Choose how you'd like to start", "err");
    else if (state.step === 1) toast("Pick a Brites design or upload an image first", "err");
    else if (state.step === 2) {
      toast(state.versions.length ? "Approve your design to continue — approving is free"
                                  : "Describe your charm to generate a preview", "err");
      $("#descInput").focus();
    }
    return;
  }
  gotoStep(state.step + 1);
});


/* =============================================================================
   STEP 1 — START MODE, SEARCH, UPLOAD
   ========================================================================== */
function setStartMode(mode) {
  state.startMode = mode;
  $("#phaseSource").hidden = START_MODES.indexOf(mode) >= 0;
  $("#phaseSearch").hidden = mode !== "search";
  $("#phaseUpload").hidden = mode !== "upload";
  $("#phaseDraw").hidden = mode !== "draw";
  updateDesignMode();
  renderPickBar();
  if (mode === "search") { renderCharmGrid(); setTimeout(() => $("#charmSearch").focus(), 60); }
  if (mode === "upload") resetUploadStage(!state.reference || state.reference.type !== "upload");
  if (mode === "draw") renderDrawStage();
}
$("#startSearch").addEventListener("click", () => setStartMode("search"));
$("#startUpload").addEventListener("click", () => setStartMode("upload"));
/* The third way in. Nothing new is needed downstream: a sketch becomes a
   reference by the same route an upload takes, so the picker, the brief, the
   generator and the order all keep treating it as an uploaded image. */
$("#startDraw").addEventListener("click", () => { setStartMode("draw"); openCompose(); });
$("#beginBtn").addEventListener("click", beginDesign);

/* ── The Charm Only catalogue ──────────────────────────────────────────────
   Three Shopify facts shape this.

   1. /search/suggest.json silently clamps resources[limit] to 10 however large
      a limit you ask for, so predictive search can never back a picker.
   2. /collections/<handle>/products.json caps at 250 products per request and
      returns every variant and image of each one — for a ~1,200-product
      collection of 25-variant charms that is well over 10 MB a shopper.
   3. An alternate template renders whatever Liquid you want at
      ?view=<name>, paginated by Shopify itself.

   So the picker reads a purpose-built JSON feed — templates/collection.studio
   .liquid and templates/search.studio.liquid — which returns five fields per
   product, 50 at a time, a few KB a page. Browsing pulls a page at a time as
   the shopper scrolls; searching hands the query to Shopify's own search, so
   it covers the whole collection rather than the first screenful.

   If those two templates are not installed the very first request comes back
   as HTML, and everything falls back to products.json + predictive search —
   heavier and shallower, but never broken. */
const FEED_VIEW  = D.feedView || "studio";
const FEED_PAGE  = 50;    // Shopify's paginate maximum
const JSON_PAGE  = 250;   // products.json's maximum, fallback path only
const FEED_MAX   = 60;    // page ceiling, so a bad feed can never loop forever
const GRID_BATCH = Math.max(8, SEARCH_LIMIT);   // cards mounted per lazy batch
/* Appended to every search. Shopify's storefront search understands
   field:value terms, so scoping the query server-side is what keeps each page
   dense: without it a search for "heart" returns 220 products of every type,
   the feed filters most of them away, and the picker has to walk five sparse
   pages to fill one screen. With it, Shopify returns the 74 that are charms
   and every page is usable. */
const SEARCH_SCOPE = (D.searchScope || "").trim();

let _feedMissing = false;    // the alternate templates are not installed
const _feedCache = new Map();   // "q\npage" -> result, so retyping costs nothing

const COLLECTION = () => D.charmCollection || "charms-only";
const scoped = (q) => SEARCH_SCOPE ? q + " " + SEARCH_SCOPE : q;
const feedUrl = (q, page) => q
  ? `/search?view=${encodeURIComponent(FEED_VIEW)}&type=product&page=${page}&q=${encodeURIComponent(scoped(q))}`
  : `/collections/${encodeURIComponent(COLLECTION())}?view=${encodeURIComponent(FEED_VIEW)}&page=${page}`;

/* The feed's keys are one letter each — at 1,200 products the difference is
   real. Full-length keys are accepted too so the template can be edited. */
const feedItem = (p) => ({
  id: p.id,
  gid: "gid://shopify/Product/" + p.id,
  title: p.t || p.title || "",
  url: "/products/" + (p.h || p.handle || ""),
  image: p.i || p.image || "",
  price: p.p == null ? null : Number(p.p),
});

/* A missing template and a bad minute are NOT the same failure. HTML coming
   back means the file was never uploaded, and there is no point asking again —
   that latches. A 429, a 5xx or a dropped connection is transient: retry once,
   then report an empty page and leave the feed enabled, because latching there
   would strand search on ten predictive-search results for the rest of the
   session — which is exactly what a slow page under a burst of requests looks
   like. */
async function fetchFeedPage(q, page) {
  const key = q + "\n" + page;
  if (_feedCache.has(key)) return _feedCache.get(key);
  if (!_feedMissing) {
    for (let attempt = 0; attempt < 2; attempt++) {
      try {
        const r = await fetch(feedUrl(q, page), { cache: "no-store" });
        if (!r.ok) throw new Error("feed " + r.status);
        const text = (await r.text()).trim();
        if (text.charAt(0) !== "{") { _feedMissing = true; break; }   // never uploaded
        const j = JSON.parse(text);
        const res = {
          items: (j.products || []).map(feedItem).filter((i) => !isStudioProduct(i)),
          total: j.total == null ? null : Number(j.total),
          done: !j.pages || page >= Number(j.pages),
          ok: true,
        };
        _feedCache.set(key, res);
        return res;
      } catch (e) {
        if (_feedMissing) break;
        if (attempt === 0) { await new Promise((r) => setTimeout(r, 400)); continue; }
        return { items: [], total: null, done: false, ok: false };   // transient
      }
    }
  }
  const res = await fetchLegacyPage(q, page);
  _feedCache.set(key, res);
  return res;
}

/* Fallback: the endpoints every theme has, whether or not the feed is installed. */
async function fetchLegacyPage(q, page) {
  if (q) {
    if (page > 1) return { items: [], total: null, done: true };
    const items = await searchCharms(q);       // predictive search, 10 at most
    return { items: items, total: items.length, done: true };
  }
  const r = await fetch(`/collections/${encodeURIComponent(COLLECTION())}/products.json?limit=${JSON_PAGE}&page=${page}`,
                        { cache: "no-store" });
  if (!r.ok) throw new Error("collection " + r.status);
  const j = await r.json();
  const raw = j.products || [];
  return {
    items: raw.map((p) => ({
      id: p.id,
      gid: "gid://shopify/Product/" + p.id,
      title: p.title,
      url: "/products/" + p.handle,
      image: (p.images && p.images[0] && p.images[0].src) || "",
      price: p.variants && p.variants[0] ? Math.round(Number(p.variants[0].price) * 100) : null,
    })).filter((i) => !isStudioProduct(i)),
    total: null,
    done: raw.length < JSON_PAGE,
  };
}

/* ── Paging state ─────────────────────────────────────────────────────────
   One query in flight at a time; a query change resets the cursor. */
let _feedQ = null, _feedPage = 0, _feedDone = false, _feedTotal = null,
    _feedInflight = null, _feedEmptyRun = 0, _feedStalled = false;

function resetFeed(q) {
  _feedQ = q; _feedPage = 0; _feedDone = false; _feedTotal = null;
  _feedInflight = null; _feedEmptyRun = 0; _feedStalled = false;
  _gridItems = []; _gridShown = 0; _gridQuery = q;
  const grid = $("#charmGrid"); if (grid) grid.innerHTML = "";
  const s = $("#charmMore"); if (s) s.hidden = true;
}

/* Pulls one more page and repaints. Returns true if anything new arrived.

   Two things can legitimately produce a page with nothing on it: a transient
   failure (retried once inside fetchFeedPage, then reported as an empty,
   NOT-done page), and a search page whose products were all filtered out. So
   emptiness never ends the walk — only the feed's own page count does, or the
   ceiling. What emptiness does do is bound how many pages a single pass will
   chase, so a broad query cannot fire twenty requests in a row. */
async function feedMore() {
  if (_feedDone || _feedPage >= FEED_MAX) return false;
  if (_feedInflight) return _feedInflight;
  const q = _feedQ, page = _feedPage + 1;
  _feedInflight = (async () => {
    let res;
    try { res = await fetchFeedPage(q, page); }
    catch (e) { res = { items: [], total: null, done: false, ok: false }; }
    if (q !== _feedQ) { _feedInflight = null; return false; }   // the query moved on
    if (res.ok === false) {                    // transient: stop this pass, keep the cursor
      _feedInflight = null;
      _feedStalled = true;
      paintGrid();
      return false;
    }
    _feedStalled = false;
    _feedPage = page;
    _feedDone = !!res.done;
    if (res.total != null) _feedTotal = res.total;
    const seen = new Set(_gridItems.map((i) => i.id));
    const fresh = res.items.filter((i) => !seen.has(i.id));
    _gridItems = _gridItems.concat(fresh);
    _feedEmptyRun = fresh.length ? 0 : _feedEmptyRun + 1;
    _feedInflight = null;
    paintGrid();
    if (!fresh.length && !_feedDone && _feedEmptyRun < 8) return feedMore();
    return fresh.length > 0;
  })();
  return _feedInflight;
}

/* Fallback searcher, used only when the search feed template is missing.
   Predictive search covers the whole store, so CHARM_FILTER keeps non-charm
   products out. It is never applied to the feed, whose search template already
   restricts results to the Charm Only collection. */
const _searchCache = new Map();
let searchTimer = null;

async function searchCharms(q) {
  const key = q.toLowerCase();
  if (_searchCache.has(key)) return _searchCache.get(key);
  const url = `/search/suggest.json?q=${encodeURIComponent(q)}`
            + `&resources[type]=product&resources[limit]=10`
            + `&resources[options][unavailable_products]=last`;
  let products = [];
  try {
    const data = await (await fetch(url, { cache: "no-store" })).json();
    products = (data.resources && data.resources.results && data.resources.results.products) || [];
  } catch (e) { return []; }
  const visible = products
    .filter((p) => CHARM_FILTER.test(p.title))
    .map((p) => ({
      id: p.id,
      gid: "gid://shopify/Product/" + p.id,
      title: p.title,
      url: p.url,
      image: (p.featured_image && (p.featured_image.url || p.featured_image)) || p.image || "",
      price: p.price,
    }))
    .filter((i) => !isStudioProduct(i));
  _searchCache.set(key, visible);
  return visible;
}
const money = (cents) => cents == null ? "" : CONFIG.currency + (Number(cents) / 100).toFixed(2);
/* The studio's OWN hidden products must never appear as a reference. They have
   to stay published to the Online Store — /cart/add.js refuses a variant that
   isn't — so they are reachable by storefront search, and "Custom Charm —
   Necklace Charm" is exactly the kind of title a charm search surfaces.
   BJ_STUDIO_VARIANTS is keyed by their handles, so it doubles as the exclusion
   list and can never drift from the products themselves. */
const STUDIO_HANDLES = new Set(Object.keys(VARIANTS));
const handleOf = (url) => {
  const m = /\/products\/([^/?#]+)/.exec(String(url || ""));
  return m ? m[1] : "";
};
const isStudioProduct = (item) =>
  STUDIO_HANDLES.has(handleOf(item && item.url)) ||
  /^(custom charm|studio design pack|studio membership|custom design fee)\b/i.test((item && item.title) || "");
const listingUrl = (item) => (item && item.url) || "/collections/" + (D.charmCollection || "charms-only");

$("#charmSearch").addEventListener("input", () => {
  $("#searchClear").style.display = $("#charmSearch").value ? "block" : "none";
  clearTimeout(searchTimer);
  searchTimer = setTimeout(renderCharmGrid, 280);   // a server-rendered search costs a round trip
});
$("#searchClear").addEventListener("click", () => {
  $("#charmSearch").value = ""; $("#searchClear").style.display = "none"; renderCharmGrid();
});

/* ── The grid mounts in batches ────────────────────────────────────────────
   1,200 products must never become 1,200 cards, 1,200 images and 1,200
   zoom/pan listeners at once. A sentinel after the grid pulls the next
   GRID_BATCH in as it scrolls into view; the first batch loads eagerly and
   everything after it is a lazy image. */
let _gridItems = [], _gridShown = 0, _gridQuery = null, _gridIO = null;

function charmCardHtml(item, idx, selId) {
  return `
    <div class="charm-card charm-card--slim${selId === String(item.id) ? " is-selected" : ""}"
         data-id="${attrText(item.id)}" data-idx="${idx}">
      <div class="charm-card__img zoom-box" title="Scroll to zoom · drag to pan">
        ${productMedia(item, idx < GRID_BATCH ? "eager" : "lazy", 540)}
      </div>
      <div class="charm-card__meta">
        <div class="charm-card__title">${escapeHtml(item.title)}</div>
      </div>
      <div class="charm-card__btns">
        <a class="cc-buy" href="${attrText(listingUrl(item))}" target="_blank" rel="noopener">Buy Now</a>
        <button class="cc-use" type="button">${selId === String(item.id) ? "✓ Selected" : "Use as Reference"}</button>
      </div>
    </div>`;
}

function gridSentinel() {
  let s = $("#charmMore");
  if (!s) {
    const grid = $("#charmGrid"); if (!grid) return null;
    s = document.createElement("div");
    s.id = "charmMore"; s.className = "charm-more"; s.setAttribute("aria-hidden", "true");
    grid.insertAdjacentElement("afterend", s);
  }
  return s;
}

function mountGridBatch() {
  const grid = $("#charmGrid"); if (!grid) return 0;
  const next = _gridItems.slice(_gridShown, _gridShown + GRID_BATCH);
  if (!next.length) return 0;
  const selId = state.reference && state.reference.item ? String(state.reference.item.id) : "";
  const holder = document.createElement("div");
  holder.innerHTML = next.map((item, n) => charmCardHtml(item, _gridShown + n, selId)).join("");
  Array.from(holder.children).forEach((card) => {
    const item = _gridItems[Number(card.dataset.idx)];
    card.querySelector(".cc-use").addEventListener("click", () => selectCatalogRef(item));
    attachZoomPan(card.querySelector(".charm-card__img"));
    grid.appendChild(card);
  });
  _gridShown += next.length;
  renderGridMeta();
  return next.length;
}

/* Selecting a reference must not remount 1,200 cards — repaint the state only. */
function syncGridSelection() {
  const selId = state.reference && state.reference.item ? String(state.reference.item.id) : "";
  $$("#charmGrid .charm-card").forEach((card) => {
    const on = card.dataset.id === selId;
    card.classList.toggle("is-selected", on);
    const btn = card.querySelector(".cc-use");
    if (btn) btn.textContent = on ? "✓ Selected" : "Use as Reference";
  });
}

function renderGridMeta() {
  const meta = $("#searchMeta"); if (!meta) return;
  const loaded = _gridItems.length;
  if (!loaded && !_feedDone) { meta.textContent = "Loading Brites designs…"; return; }
  if (!loaded) { meta.textContent = ""; return; }
  /* While pages are still coming the feed's own total is the honest number —
     the whole collection from the first paint, not "50 so far". Once the last
     page is in, the loaded count is the honest one: the search feed filters
     each page down to the reference collection, so its total counts products
     that were then dropped. */
  const total = _feedDone ? loaded
              : (_feedTotal != null && _feedTotal >= loaded ? _feedTotal : loaded);
  const noun = `Brites design${total === 1 ? "" : "s"}`;
  const head = _gridQuery ? `${total.toLocaleString()} ${noun} found` : `${total.toLocaleString()} ${noun}`;
  meta.textContent = _gridShown < total ? `${head} — showing ${_gridShown.toLocaleString()}` : head;
}

/* Is the sentinel inside the viewport, or within a screenful below it? */
function sentinelNear() {
  const s = $("#charmMore");
  if (!s || s.hidden) return false;
  const r = s.getBoundingClientRect();
  if (!r.width && !r.height) return false;              // not laid out (panel hidden)
  const h = window.innerHeight || document.documentElement.clientHeight || 0;
  return r.top <= h + 600 && r.bottom >= -600;
}

/* ── The pump ──────────────────────────────────────────────────────────────
   IntersectionObserver only fires when intersection CHANGES. Mounting a batch
   below a sentinel that is already on screen leaves it on screen, so the
   observer stays silent and the grid stalls — which is exactly what a short
   result set does, because its sentinel never leaves the viewport at all.
   So the observer only wakes the pump, and the pump keeps mounting (and
   fetching) until the sentinel has been pushed a screen below the fold. */
let _pumping = false;
async function pumpGrid() {
  if (_pumping) return;
  _pumping = true;
  try {
    for (let guard = 0; guard < 400 && sentinelNear(); guard++) {
      if (mountGridBatch()) {                    // more already in hand
        armGridSentinel();
        yield_();
        continue;
      }
      if (_feedDone) break;                      // nothing left anywhere
      const grew = await feedMore();             // pull the next page
      if (!grew) break;
    }
  } finally {
    _pumping = false;
    /* A page that failed transiently leaves the cursor where it was. Scrolling
       resumes it, but a shopper looking at a half-filled grid should not have
       to — so try again shortly, once. */
    if (_feedStalled && !_feedDone && !_pumpRetry) {
      _pumpRetry = setTimeout(() => { _pumpRetry = 0; pumpGrid(); }, 1500);
    }
  }
}
let _pumpRetry = 0;
const yield_ = () => new Promise((r) =>
  (window.requestAnimationFrame || window.setTimeout)(() => r(), 0));

function armGridSentinel() {
  const s = gridSentinel(); if (!s) return;
  s.hidden = _gridShown >= _gridItems.length && _feedDone;
  if (!_gridIO) {
    if (!("IntersectionObserver" in window)) {          // no observer: mount it all
      (async () => { while (mountGridBatch() || await feedMore()) {} })();
      return;
    }
    _gridIO = new IntersectionObserver((entries) => {
      if (entries.some((e) => e.isIntersecting)) pumpGrid();
    }, { rootMargin: "600px 0px" });
    _gridIO.observe(s);
  }
}

/* Paint whatever the cursor has pulled so far. Every arriving page calls this,
   so the grid grows underneath the shopper without ever remounting. */
function paintGrid() {
  const grid = $("#charmGrid"); if (!grid) return;
  if (!_gridItems.length) {
    renderGridMeta();
    if (!_feedDone) return;                       // still loading — leave it be
    grid.innerHTML = `<div class="search-empty"><b>No matches — and that's perfect.</b>
      If we don't already make it, this is exactly what the Custom Studio is for.<br>
      <button class="btn btn--tertiary" id="emptyUploadBtn" type="button" style="margin-top:14px;">Upload your own image instead</button></div>`;
    const b = $("#emptyUploadBtn"); if (b) b.addEventListener("click", () => setStartMode("upload"));
    return;
  }
  if (_gridShown === 0) mountGridBatch(); else { syncGridSelection(); renderGridMeta(); }
  armGridSentinel();
  pumpGrid();                                     // fill the screen, then stop
}

async function renderCharmGrid() {
  const grid = $("#charmGrid"); if (!grid) return;
  const q = $("#charmSearch").value.trim();
  if (q !== _feedQ) { resetFeed(q); renderGridMeta(); await feedMore(); }
  else paintGrid();
}

/* A scroll can outrun the observer on a fast wheel or a momentum flick, and a
   resize can pull the sentinel back into view without any scroll at all. */
let _pumpTick = 0;
const wakePump = () => {
  if (_pumpTick) return;
  _pumpTick = (window.requestAnimationFrame || window.setTimeout)(() => { _pumpTick = 0; pumpGrid(); }, 0);
};
window.addEventListener("scroll", wakePump, { passive: true });
window.addEventListener("resize", wakePump, { passive: true });

function selectCatalogRef(item) {
  /* Only the product GID + image URL are stored; custom_charm_generate fetches
     the image server-side rather than trusting a client-supplied URL (§10.9). */
  setReference({ type: "catalog", item }, `Reference: ${item.title}`);
  renderCharmGrid();
}

/* ── Upload: client downscale → britesAuth upload_ref → precheck ───────────
   Decision 4 of the design guide: the browser NEVER writes to Firebase
   Storage. The resized image is POSTed to britesAuth, which verifies the ID
   token, enforces the MIME/size caps and the allowance, and writes the object
   with the Admin SDK. Real transfer progress comes from XHR upload events, so
   the three-stage bar stays truthful. */
const dz = $("#dropzone"), fileInput = $("#fileInput");
dz.addEventListener("click", () => fileInput.click());
["dragenter", "dragover"].forEach((ev) => dz.addEventListener(ev, (e) => { e.preventDefault(); dz.classList.add("is-drag"); }));
["dragleave", "drop"].forEach((ev) => dz.addEventListener(ev, (e) => { e.preventDefault(); dz.classList.remove("is-drag"); }));
/* readUpload may now have to flatten a design file before it has a picture to
   send, so it is a promise — and a promise nobody catches is a console error
   the customer never sees an explanation for */
const startUpload = (f) => {
  if (!f) return;
  Promise.resolve(readUpload(f)).catch((err) => {
    console.warn("[studio] reference upload:", (err && err.message) || err);
    toast("We couldn't read that file — try another image.", "err");
  });
};
dz.addEventListener("drop", (e) => { startUpload(e.dataTransfer.files && e.dataTransfer.files[0]); });
fileInput.addEventListener("change", () => { startUpload(fileInput.files && fileInput.files[0]); });

function resetUploadStage(clear = true) {
  renderUploadAllowance();
  if (typeof refreshLibraryEntry === "function") refreshLibraryEntry();
  if ($("#uploadFail")) $("#uploadFail").hidden = true;
  if (clear) {
    $("#dropzone").hidden = false;
    $("#uploadProgress").hidden = true;
    $("#uploadDone").hidden = true;
  } else {
    $("#dropzone").hidden = true;
    $("#uploadProgress").hidden = true;
    $("#uploadDone").hidden = false;
    /* a reference reused from the library has no file to describe, so the
       "received & checked" card is filled from the reference itself */
    const r = state.reference;
    if (r && r.type === "upload") {
      $("#doneThumb").innerHTML = `<img src="${attrText(r.url)}" alt="Your reference image">`;
      $("#doneName").textContent = r.name || "Your image";
      if (!r.w) $("#doneMeta").textContent = "From your library — no upload needed";
    }
  }
}
function uploadLimit() {
  return CONFIG.guestFreeUploads
    + (state.user ? CONFIG.signupBonusUploads : 0)
    + (state.purchasedCredits || 0);
}
function uploadsLeft() { return Math.max(0, uploadLimit() - (state.uploadsUsed || 0)); }
function renderUploadAllowance() {
  const el = $("#upAllowance"); if (!el) return;
  const left = uploadsLeft();
  el.innerHTML = left > 0
    ? `<b>${left}</b> image upload${left === 1 ? "" : "s"} left`
    : (state.user ? `No uploads left — add credits to continue`
                  : `No uploads left — create a free account for ${CONFIG.signupBonusUploads} more`);
}

/* Downscale anything larger than 2K on its long edge, preserving aspect ratio.
                                                        [prototype, verbatim] */
/* Does any pixel see through? Sampled rather than exhaustive: a grid of a
   few thousand points finds a transparent background instantly and costs
   nothing on a photograph that has none. */
function imageHasAlpha(img) {
  const S = Math.min(160, Math.max(24, Math.round(Math.max(img.naturalWidth, img.naturalHeight) / 8)));
  const c = document.createElement("canvas");
  c.width = S; c.height = S;
  const x = c.getContext("2d", { willReadFrequently: true });
  try {
    x.drawImage(img, 0, 0, S, S);
    const d = x.getImageData(0, 0, S, S).data;
    for (let i = 3; i < d.length; i += 4) if (d[i] < 250) return true;
  } catch (e) { /* tainted — assume opaque, which is the safe guess */ }
  return false;
}

/* ═══════════ TRANSPARENCY, AND THE BLACK SQUARE ══════════════════════════
   This used to end `toDataURL("image/jpeg")` for every picture over the size
   limit. JPEG HAS NO ALPHA CHANNEL: every transparent pixel in a PNG came
   back BLACK, which is why a logo with no background arrived sitting on a
   black square.

   So the encoding follows the picture rather than the other way round. A
   picture with transparency stays a PNG and keeps it. One without is still a
   JPEG, because a photograph as PNG is several times the bytes for nothing —
   and it is flattened onto WHITE, never onto the black a bare canvas gives
   you. If a transparent PNG comes out too heavy at full size it is made
   smaller, still a PNG: the transparency is the thing being protected.     */
function resizeToMax(dataUrl, maxPx, opts) {
  const budget = (opts && opts.maxBytes) || 0;
  return new Promise((resolve) => {
    const img = new Image();
    img.onload = () => {
      const w = img.naturalWidth, h = img.naturalHeight;
      const alpha = imageHasAlpha(img);
      const small = Math.max(w, h) <= maxPx;
      /* nothing to do: already small enough, and re-encoding could only lose */
      if (small && (!budget || dataUrl.length <= budget)) {
        resolve({ dataUrl, w, h, resized: false, alpha });
        return;
      }
      const draw = (nw, nh) => {
        const c = document.createElement("canvas");
        c.width = nw; c.height = nh;
        const x = c.getContext("2d");
        if (!alpha) { x.fillStyle = "#ffffff"; x.fillRect(0, 0, nw, nh); }
        x.drawImage(img, 0, 0, nw, nh);
        return c;
      };
      let scale = Math.min(1, maxPx / Math.max(w, h));
      for (let tries = 0; tries < 5; tries++) {
        const nw = Math.max(1, Math.round(w * scale)), nh = Math.max(1, Math.round(h * scale));
        const out = alpha ? draw(nw, nh).toDataURL("image/png")
                          : draw(nw, nh).toDataURL("image/jpeg", 0.92);
        if (!budget || out.length <= budget || tries === 4) {
          resolve({ dataUrl: out, w: nw, h: nh, resized: true, fromW: w, fromH: h, alpha });
          return;
        }
        scale *= 0.75;
      }
    };
    img.onerror = () => resolve({ dataUrl, w: 0, h: 0, resized: false, alpha: false });
    img.src = dataUrl;
  });
}

/* Imported raster art becomes manufacturing artwork before it reaches the
   sheet: visible artwork is black, white is transparent. Neutral anti-aliased
   edge pixels keep proportional alpha so fine linework does not get fattened
   into a coarse silhouette during conversion. */
function rasterShapesBlack(dataUrl, opts) {
  const cfg = opts || {};
  const whiteDelta = cfg.whiteDelta == null ? 12 : Number(cfg.whiteDelta);
  const fullInkDelta = cfg.fullInkDelta == null ? 92 : Number(cfg.fullInkDelta);
  const whiteChroma = cfg.whiteChroma == null ? 20 : Number(cfg.whiteChroma);
  const minAlpha = cfg.minAlpha == null ? 6 : Number(cfg.minAlpha);
  return new Promise((resolve, reject) => {
    const img = new Image();
    img.onload = () => {
      const w = img.naturalWidth || img.width || 0;
      const h = img.naturalHeight || img.height || 0;
      if (!w || !h) { resolve({ dataUrl, w: 0, h: 0, kept: 0 }); return; }
      const c = document.createElement("canvas");
      c.width = w; c.height = h;
      const x = c.getContext("2d", { willReadFrequently: true });
      x.clearRect(0, 0, w, h);
      x.drawImage(img, 0, 0, w, h);
      const im = x.getImageData(0, 0, w, h);
      const d = im.data;
      let kept = 0;
      for (let i = 0; i < d.length; i += 4) {
        const a = d[i + 3];
        if (a < minAlpha) { d[i + 3] = 0; continue; }
        const alpha = a / 255;
        const r = 255 - (255 - d[i]) * alpha;
        const g = 255 - (255 - d[i + 1]) * alpha;
        const b = 255 - (255 - d[i + 2]) * alpha;
        const maxc = Math.max(r, g, b), minc = Math.min(r, g, b);
        const fromWhite = 255 - minc;
        const chroma = maxc - minc;
        if (fromWhite <= whiteDelta && chroma <= whiteChroma) {
          d[i] = d[i + 1] = d[i + 2] = 0; d[i + 3] = 0;
          continue;
        }
        /* Saturated colour is intentional artwork. Neutral near-white is
           usually anti-aliasing, so taper its alpha smoothly instead of
           snapping it to an opaque black pixel. */
        let strength = chroma > 28 ? 1 : (fromWhite - whiteDelta) / Math.max(1, fullInkDelta - whiteDelta);
        strength = Math.max(0, Math.min(1, strength));
        d[i] = d[i + 1] = d[i + 2] = 0;
        d[i + 3] = Math.round(a * strength);
        if (d[i + 3] >= minAlpha) kept++;
      }
      x.putImageData(im, 0, 0);
      resolve({ dataUrl: c.toDataURL("image/png"), w, h, kept });
    };
    img.onerror = () => reject(new Error("shape_convert_failed"));
    img.src = dataUrl;
  });
}

/* XHR rather than fetch for this one call, so upload.onprogress can drive the
   real transfer bar (§10.5). */
function postUpload(body, onProgress) {
  return new Promise(async (resolve, reject) => {
    const token = await idToken();
    const xhr = new XMLHttpRequest();
    xhr.open("POST", `${FN_BASE}/britesAuth`);
    xhr.setRequestHeader("Content-Type", "application/json");
    if (token) xhr.setRequestHeader("Authorization", "Bearer " + token);
    xhr.upload.onprogress = (e) => {
      if (e.lengthComputable && onProgress) onProgress(e.loaded / e.total);
    };
    xhr.onload = () => {
      let j = null; try { j = JSON.parse(xhr.responseText || "null"); } catch (e) {}
      if (xhr.status >= 200 && xhr.status < 300 && j && j.ok !== false) resolve(j);
      else reject(Object.assign(new Error((j && j.error) || "HTTP " + xhr.status), { status: xhr.status, payload: j }));
    };
    xhr.onerror = () => reject(new Error("network"));
    xhr.send(JSON.stringify(body));
  });
}

/* Suitability pre-check — custom_charm_precheck on geminiImageProxy. A
   text-only vision pass: is the subject clear, translatable into a flat cut
   charm, and free of people / NSFW / obvious trademarks. Costs no credit; the
   upload allowance is metered server-side only when the image is ACCEPTED. */
async function precheckImage(storagePath) {
  try {
    const r = await postAuthed(IMAGE_FN, {
      kind: "custom_charm_precheck",
      input_storage_path: storagePath,
    }, FN_BASE_IMAGE);
    if (r && r.ok) {
      const usable = r.usable != null ? r.usable : (r.verdict === "ok" || r.verdict === true);
      return { suitable: !!usable, reason: r.reason || "" };
    }
    /* Netlify background functions answer 202 with no body. An empty response
       means the checker didn't answer, which is the degraded path — never a
       rejection. Generation applies the same doctrine again, server-side. */
    if (!r) return { suitable: true, reason: "", degraded: true };
    return { suitable: false, reason: r.reason || "We couldn't read that image clearly — try another one." };
  } catch (e) {
    if (e.status === 402) return { suitable: false, reason: "No image uploads left on this account." };
    /* Never fail a customer's upload because the checker was unavailable:
       generation applies the same constraints again, server-side. */
    console.warn("[studio] precheck unavailable:", e.message);
    return { suitable: true, reason: "", degraded: true };
  }
}

let uploadBusy = false;
async function readUpload(file) {
  if (uploadBusy) return;
  /* A DESIGN FILE IS WELCOME HERE TOO. The reference is one picture, and a
     .psd or .ai is not one — so it is flattened into one, faithfully, and
     that goes up instead. The customer chose a file and got a reference;
     which of the two it was is our problem, not theirs. (The layers are not
     lost: open the same file in the drawing studio and every one of them
     comes across, editable.) */
  if (typeof isDesignFile === "function" && isDesignFile(file)) {
    /* flattening takes a moment, and uploadBusy is not set until there is
       something to upload — so this holds the door on its own */
    if (uploadBusy) return;
    uploadBusy = true;
    toast(`Reading “${String(file.name).slice(0, 24)}” — flattening it to a reference…`, "gold");
    let flat = null;
    try { flat = await designFlatten(file); } catch (e) { flat = null; }
    uploadBusy = false;
    /* two different failures deserve two different sentences: one of them
       the customer can act on immediately, and the other one cannot */
    if (flat && flat.tooBig) {
      toast(`That file is ${flat.tooBig} MB, which is more than we can open here. ` +
            "Send us a smaller version and we'll take it from there.", "err");
      return;
    }
    if (!flat) {
      toast("We couldn't turn that design file into a picture. Export it as a " +
            "JPG or PNG and we'll take it from there.", "err");
      return;
    }
    file = flat;
  }
  if (!/^image\/(jpeg|png)$/.test(file.type)) {          // JPG/PNG only — no HEIC
    toast("Please choose a JPG or PNG image", "err"); return;
  }
  if (file.size > CONFIG.maxUploadBytes) {
    toast("That image is over 15 MB — try a smaller file", "err"); return;
  }
  if (uploadsLeft() <= 0) {
    if (!state.user) openAuth("outOfCredits"); else openPricing("out");
    return;
  }

  const lbl = $("#upLabel"), fill = $("#upBarFill"), pct = $("#upPct");
  const setBar = (v, note) => { fill.style.width = v + "%"; pct.textContent = note || (Math.round(v) + "%"); };

  uploadBusy = true;
  /* the design this upload was started for */
  const ep = epochNow();
  $("#dropzone").hidden = true; $("#uploadDone").hidden = true; $("#uploadFail").hidden = true;
  $("#uploadProgress").hidden = false;
  $("#upThumb").innerHTML = "";
  lbl.textContent = "Uploading your image…";
  setBar(0);

  const rd = new FileReader();
  /* stage 1 — the file coming into the page: real FileReader progress */
  rd.onprogress = (e) => { if (e.lengthComputable) setBar((e.loaded / e.total) * 55); };
  rd.onerror = () => { failUpload("We couldn't read that file — try another image."); };
  rd.onload = async () => {
    try {
      $("#upThumb").innerHTML = `<img src="${rd.result}" alt="">`;
      setBar(55);

      /* stage 2 — downscale over 2048 px, then the real transfer */
      lbl.textContent = "Preparing your image…";
      setBar(60, "Optimising resolution");
      const meta = await resizeToMax(rd.result, CONFIG.maxUploadPx);
      setBar(64, meta.resized ? `Resized to ${meta.w} × ${meta.h}` : `${meta.w} × ${meta.h}`);

      const sess = ensureSession();
      const up = await postUpload({
        kind: "upload_ref",
        sessionId: sess.id,
        filename: file.name,
        dataUrl: meta.dataUrl,
      }, (frac) => setBar(64 + frac * 24));
      setBar(88);

      /* stage 3 — can this become a charm? */
      lbl.textContent = "Checking your image…";
      setBar(92, "Making sure it reads clearly");
      const verdict = await precheckImage(up.path);
      setBar(100, "100%");
      await new Promise((r) => setTimeout(r, 220));
      $("#uploadProgress").hidden = true;

      if (!verdict.suitable) {
        $("#failThumb").innerHTML = `<img src="${meta.dataUrl}" alt="">`;
        $("#failReason").textContent = verdict.reason;
        $("#uploadFail").hidden = false;
        uploadBusy = false;
        return;                                  // no allowance spent on a rejection
      }

      /* ── STARTED IN ANOTHER DESIGN ─────────────────────────────────────
         The file is uploaded and it is in the library, so nothing is lost —
         but it must not become the reference of a design the customer chose
         after starting it. */
      if (!sameProject(ep)) {
        uploadBusy = false;
        SD("upload: finished after a project switch — left in the library");
        return;
      }
      logEvent("upload", `Uploaded image: ${file.name} (${meta.w}×${meta.h}${meta.resized ? ", resized" : ""})`);
      setReference({ type: "upload", url: up.url, path: up.path, name: file.name,
                     w: meta.w, h: meta.h, resized: meta.resized });
      refreshWallet();                            // uploadsUsed moved server-side
      $("#doneThumb").innerHTML = `<img src="${attrText(up.url)}" alt="Your reference image">`;
      $("#doneName").textContent = file.name;
      $("#doneMeta").textContent = meta.resized
        ? `${meta.fromW} × ${meta.fromH} → resized to ${meta.w} × ${meta.h}`
        : `${meta.w} × ${meta.h}`;
      $("#uploadDone").hidden = false;
      renderUploadAllowance();
      renderPickBar();
      $("#navNext").focus();
      uploadBusy = false;
    } catch (err) {
      if (!sameProject(ep)) { uploadBusy = false; return; }
      if (err.status === 401) { uploadBusy = false; $("#uploadProgress").hidden = true; resetUploadStage(true); openAuth("default"); return; }
      failUpload(err.status === 402
        ? "You've used every image upload on this account — add credits to keep going."
        : "That upload didn't go through — please try again.");
    }
  };
  function failUpload(reason) {
    $("#uploadProgress").hidden = true;
    $("#failThumb").innerHTML = $("#upThumb").innerHTML;
    $("#failReason").textContent = reason;
    $("#uploadFail").hidden = false;
    uploadBusy = false;
  }
  rd.readAsDataURL(file);
}
/* ═══════════ THE SKETCHPAD AS A REFERENCE ════════════════════════════════
   Whatever comes out of the drawing studio is flattened to a PNG and pushed
   through upload_ref — the same function, the same allowance, the same
   suitability check as a photograph. That is deliberate: nothing downstream
   should have to know or care that this picture was drawn rather than taken.
   ========================================================================== */
function drawnRefExists() {
  return !!(state.reference && state.reference.type === "upload" && state.reference.drawn);
}
function renderDrawStage() {
  const has = drawnRefExists();
  $("#drawInvite").hidden = has;
  $("#drawDone").hidden = !has;
  if (has) {
    $("#drawThumb").innerHTML = `<img src="${attrText(state.reference.url)}" alt="Your drawing">`;
    $("#drawMeta").textContent = state.reference.marks
      ? `${state.reference.marks} mark${state.reference.marks === 1 ? "" : "s"} · you can keep drawing any time`
      : "You can keep drawing any time";
  }
  /* the exact/interpret chips beside the finished sketch mirror the same
     choice inside the sketchpad — one value, two places to see it */
  const chips = $("#drawModeChips");
  if (chips) {
    chips.hidden = !has;
    const mode = (((state.markups || {}).draw || {}).mode === "exact") ? "exact" : "interpret";
    $$("#drawModeChips .mk-mode").forEach((b) => {
      const on = b.dataset.mode === mode;
      b.classList.toggle("is-on", on);
      b.setAttribute("aria-checked", on ? "true" : "false");
    });
  }
  const el = $("#drawAllowance");
  if (el) {
    const left = uploadsLeft();
    el.innerHTML = state.unlimited ? ""
      : left > 0 ? `<b>${left}</b> image slot${left === 1 ? "" : "s"} left — a finished sketch uses one`
                 : (state.user ? "No image slots left — add credits to continue"
                               : `No image slots left — create a free account for ${CONFIG.signupBonusUploads} more`);
  }
  renderPickBar();
}
$$("#drawModeChips .mk-mode").forEach((b) => b.addEventListener("click", () => {
  if (!state.markups) state.markups = {};
  if (!state.markups.draw) state.markups.draw = { items: [], mode: "interpret" };
  state.markups.draw.mode = b.dataset.mode === "exact" ? "exact" : "interpret";
  saveSession();
  renderDrawStage();
}));
$("#drawOpen").addEventListener("click", () => openCompose());
$("#drawEdit").addEventListener("click", () => openCompose());
$("#drawRedo").addEventListener("click", () => {
  if (state.markups) delete state.markups.draw;
  if (drawnRefExists()) state.reference = null;
  saveSession();
  renderDrawStage();
  openCompose();
});

let composeBusy = false;
async function submitComposedDrawing() {
  if (composeBusy) return;
  await mkComposeReady();                 // placed pictures must be in the frame
  const shot = mkComposeToDataUrl(1400);
  if (!shot.count) { toast("Draw something first — anything at all", "err"); return; }
  if (shot.tainted) toast("One picture couldn't be included — everything else is here", "err");
  if (uploadsLeft() <= 0) {
    if (!state.user) openAuth("outOfCredits"); else openPricing("out");
    return;
  }
  composeBusy = true;
  const ep = epochNow();
  const btn = $("#markupUseDrawing");
  const was = btn.innerHTML;
  btn.disabled = true;
  btn.textContent = "Saving your drawing…";
  try {
    const sess = ensureSession();
    const up = await postUpload({
      kind: "upload_ref",
      sessionId: sess.id,
      filename: "my-sketch.png",
      dataUrl: shot.dataUrl,
    });
    /* A hand drawing is exempt from the photographic suitability check: it is
       already a line drawing of a charm, which is precisely what that check
       exists to insist upon. */
    /* the sketch is uploaded and lives in the library; it belongs to the
       design it was drawn in, and that design is no longer on screen */
    if (!sameProject(ep)) {
      SD("sketch: uploaded after a project switch — left in the library");
      return;
    }
    logEvent("upload", `Drew a reference by hand (${shot.count} marks)`);
    state.startMode = "draw";
    closeMarkup();
    /* the sketch keeps its own session; setReference only re-bases when the
       reference actually CHANGES, and a sketch submitted from its own design
       is the same design it always was */
    /* THE FILL LAW, CAPTURED WITH THE PICTURE. This is the moment the sketch
       becomes a flat PNG, and the moment "that area is a hole" stops being
       anything but a blue patch of pixels. So the customer's own account of
       every filled area is taken NOW, from the artwork that is actually
       going up, and travels on the reference. Rebuilding it later from the
       live sketchpad would describe whatever they have drawn since — and the
       workshop is told the list wins over the picture. */
    let zones = [];
    try { zones = mkZonesPayload(mkItemsOf((state.markups || {}).draw).map(mkPack)); }
    catch (e) { zones = []; }
    /* sameDesign: submitting your own sketch is never a change of mind about
       the reference — it IS this design, so it keeps its session, its versions
       and its renders. keepSketch alone only saved the sketchpad. */
    setReference({ type: "upload", url: up.url, path: up.path, name: "Your drawing",
                   w: 1400, h: 1400, resized: false, drawn: true, marks: shot.count,
                   zones },
                 null, { keepSketch: true, sameDesign: true });
    refreshWallet();
    setStartMode("draw");
    toast("That's your reference — describe it next ✦", "gold");
  } catch (err) {
    console.warn("[studio] sketch upload:", err && err.message);
    if (!sameProject(ep)) return;
    toast(err && err.status === 402
      ? "You've used every image slot on this account — add credits to keep going."
      : "That didn't save — try once more.", "err");
  } finally {
    composeBusy = false;
    btn.disabled = false;
    btn.innerHTML = was;
  }
}

/* ═══════════ THE LIBRARY, AS A REFERENCE PICKER ══════════════════════════
   The same derived repository the drawing studio shows, offered where a
   reference is chosen. Everything in it already lives in Storage under this
   uid, so picking one is instant and spends no upload allowance — which is
   exactly why it belongs beside the dropzone rather than behind it.
   ========================================================================== */
let _libPickFilter = "all", _libPickQ = "";
function libPickList() {
  const q = _libPickQ.trim().toLowerCase();
  let list = mkLibraryItems();
  if (_libPickFilter !== "all") list = list.filter((a) => a.kind === _libPickFilter);
  if (q) list = list.filter((a) => (a.title + " " + a.sub).toLowerCase().indexOf(q) >= 0);
  return list.sort((a, b) => b.at - a.at);
}
let _libPick = [];
function renderLibPick() {
  const host = $("#libPickGrid"); if (!host) return;
  _libPick = libPickList();
  const total = mkLibraryItems().length;
  $("#libPickCount").textContent = _libPick.length === total
    ? `${total} image${total === 1 ? "" : "s"}` : `${_libPick.length} of ${total}`;
  host.innerHTML = _libPick.length
    ? _libPick.map((a, i) => `<figure class="mk-asset" data-p="${i}" tabindex="0" role="button"
          aria-label="${attrText("Use " + a.title + " as the reference")}">
        <span class="mk-asset__img"><img src="${attrText(a.pairUrl || a.url)}" alt="" loading="lazy" draggable="false"></span>
        ${a.pairUrl ? '<span class="mk-asset__pair">PAIR</span>' : ""}
        <figcaption><b>${escapeHtml(a.title)}</b><span>${escapeHtml(a.sub)}${a.at ? " · " + mkLibDate(a.at) : ""}</span></figcaption>
      </figure>`).join("")
    : `<p class="mk-lib__empty">${total ? "Nothing matches that." : "Nothing here yet — your uploads and finished charms collect here as you go."}</p>`;
}
function refreshLibraryEntry() {
  const btn = $("#upFromLibrary"); if (!btn) return;
  const n = mkLibraryItems().length;
  btn.hidden = n === 0;
  $("#upLibraryCount").textContent = n ? ` · ${n} image${n === 1 ? "" : "s"}` : "";
}
$("#upFromLibrary").addEventListener("click", () => {
  renderLibPick();
  $("#libPickModal").classList.add("is-open");
});
$("#libPickSearch").addEventListener("input", () => {
  _libPickQ = $("#libPickSearch").value; renderLibPick();
});
$$("#libPickFilters .mk-chip").forEach((b) => b.addEventListener("click", () => {
  _libPickFilter = b.dataset.filter;
  $$("#libPickFilters .mk-chip").forEach((x) => x.classList.toggle("is-on", x === b));
  renderLibPick();
}));
let _libPickBusy = false;
$("#libPickGrid").addEventListener("click", async (e) => {
  const fig = e.target.closest("[data-p]"); if (!fig) return;
  const a = _libPick[+fig.dataset.p]; if (!a) return;
  /* choosing a tile can now involve an upload, and two clicks land in the
     same tick — before any style recalculation could hide the modal — so
     the door is held here rather than by the closing animation */
  if (_libPickBusy) return;
  _libPickBusy = true;
  setTimeout(() => { _libPickBusy = false; }, 1200);
  /* the DRAWING half of a pair, never the metal render: a photograph of a
     finished charm is a picture of an answer, and the line drawing is the
     thing a new design can actually be built from */
  $("#libPickModal").classList.remove("is-open");
  const ep = epochNow();
  state.startMode = "upload";
  logEvent("upload", `Reused an image from the library: ${a.title}`);

  /* SOME TILES HAVE NO FILE BEHIND THEM. A sketch that was never submitted,
     and now an imported .ai or .svg, are drawn on the spot from their own
     objects — their `url` is a base64 picture, tens of kilobytes of it. Two
     things go wrong if that is stored as the reference: it lands in the
     session document, which has a hard 1 MB ceiling and everything else the
     customer owns in it; and the workshop's generator fetches the reference
     server-side BY PATH, so a picture that exists only in this tab is a
     reference the AI can never actually see. So it is uploaded first, once,
     and the reference points at the file like every other one. */
  let url = a.url, path = a.path || "";
  /* the test is whether a FILE exists, which is what `path` means — not what
     the url happens to look like. An entry with a path is already in storage
     and must never be uploaded a second time. */
  if (!path) {
    try {
      toast("Getting that ready…", "gold");
      const sess = ensureSession();
      const { up } = await mkUploadPicture(sess.id, (a.title || "artwork") + ".png", url);
      url = up.url; path = up.path;
      _libPickBusy = false;
    } catch (err) {
      _libPickBusy = false;
      console.warn("[studio] library reference upload:", (err && err.message) || err);
      toast("We couldn't use that one as your reference just now — try again in a moment.", "err");
      return;
    }
  }
  /* picking from the library can involve an upload, which awaits */
  if (!sameProject(ep)) return;
  setReference({ type: "upload", url, path,
                 name: a.title, w: 0, h: 0, resized: false,
                 drawn: a.kind === "sketch", marks: 0 });
  setStartMode("upload");
  resetUploadStage(false);
  toast("That's your reference — no upload needed ✦", "gold");
});

$("#uploadRetry").addEventListener("click", () => { fileInput.value = ""; resetUploadStage(true); });
$("#uploadRedo").addEventListener("click", () => {
  state.reference = null; fileInput.value = "";
  saveSession(); resetUploadStage(true); renderPickBar();
});

/* ── reference changed ─────────────────────────────────────────────────────
   The chosen-design bar this used to paint is gone (it duplicated the picker's
   own selected state), but its two side effects are load-bearing and every
   caller still needs them: updateDesignMode decides whether the wizard chrome
   is up, and syncBrief keeps step 2's brief in step with the reference. */
function renderPickBar() {
  updateDesignMode();
  syncBrief();
}
const renderRefStrip = renderPickBar;

/* The reference chip in the corner of the stage opens the full-size view;
   Change lives inside THAT, because swapping the reference restarts the
   design's whole basis and should cost one deliberate step, not sit as a
   button you can brush past while reaching for something else. */
$("#refChip").addEventListener("click", openRefLightbox);
$("#refLightboxChange").addEventListener("click", () => {
  $("#refLightbox").classList.remove("is-open");
  gotoStep(1);
});


/* =============================================================================
   STEP 2 — DESCRIBE / CONVERSATION STUDIO
   ========================================================================== */
$("#descInput").addEventListener("input", () => {
  $("#charCount").textContent = $("#descInput").value.length;
  autoGrow();
  updateDesignMode();
});
function syncBrief() {
  const r = state.reference;
  const stage = $("#refPreviewStage");
  if (stage) {
    const link = $("#refPreviewLink");
    if (r && r.type === "catalog") {
      $("#refPreviewCap").textContent = "Your reference — " + r.item.title;
      link.href = listingUrl(r.item); link.hidden = false;
      stage.innerHTML = productMedia(r.item, "eager", 900);
    } else if (r && r.type === "upload") {
      $("#refPreviewCap").textContent = "Your reference — " + r.name;
      link.hidden = true;
      stage.innerHTML = `<img src="${attrText(r.url)}" alt="Your reference image">`;
    } else {
      $("#refPreviewCap").textContent = "Your reference";
      link.hidden = true;
      stage.innerHTML = `<div class="ref-preview__empty">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.2"><rect x="3" y="5" width="18" height="15"/><circle cx="9" cy="10" r="1.8"/><path d="m4.5 18 5-5 3.5 3.5L17 12l2.5 2.5"/></svg>
        <span>Pick a reference on the previous step</span></div>`;
    }
  }
  const name = refName();
  const railRef = $("#railRef"); if (railRef) railRef.textContent = name;
  renderRefDock(name);
  if (state.step === 2 && !state.versions.length) renderStage();
}
function refName() {
  const r = state.reference;
  return !r ? "No reference" : (r.type === "catalog" ? r.item.title : r.name);
}
function refMediaHTML(r, w) {
  if (!r) return "";
  return r.type === "catalog" ? productMedia(r.item, "eager", w || 260)
       : `<img src="${attrText(r.url)}" alt="Your reference image">`;
}
function renderRefDock(name) {
  const chip = $("#refChip"); if (!chip) return;
  const r = state.reference;
  const grid = ROOT.querySelector("#view-2 .gen-grid");
  chip.hidden = !r;
  if (grid) grid.classList.toggle("is-norref", !r);
  if (!r) return;
  $("#refChipImg").innerHTML = refMediaHTML(r, 160);
  chip.title = name + " — click to enlarge";
}
function openRefLightbox() {
  const r = state.reference; if (!r) return;
  const stage = $("#refLightboxStage");
  stage.innerHTML = refMediaHTML(r, 1200);
  $("#refLightboxName").textContent = r.type === "catalog" ? r.item.title : r.name;
  $("#refLightbox").classList.add("is-open");
  attachZoomPan(stage);
}

/* =============================================================================
   GENERATION — custom_charm_generate / custom_charm_refine on
   geminiImageProxy-background, with the version doc's status streamed back
   through onSnapshot to drive this same staged progress UI:
     queued → planning → generating → polishing → done | failed
   A failed generation refunds the credit server-side and marks the version
   `failed`; the wallet snapshot pushes the restored balance back to the meter.
   ========================================================================== */
const GEN_STAGES = [
  ["Reading your inspiration…", 14],
  ["Sketching the silhouette…", 38],
  ["Inking the outline…", 62],
  ["Placing the engraving…", 82],
  ["Cleaning up the drawing…", 96],
];
/* Server stage names map onto the customer-facing narration above. */
const STAGE_INDEX = { queued: 0, planning: 1, generating: 2, engraving: 3, polishing: 4, done: 4 };
/* The render step has its own narration — it is a different act: the approved
   drawing is being manufactured, not designed. */
const RENDER_STAGES = [
  ["Reading your drawing…", 16],
  ["Cutting the flat metal…", 44],
  ["Engraving your lines…", 72],
  ["Polishing under the lights…", 96],
];
const RENDER_INDEX = { queued: 0, planning: 0, rendering: 1, engraving: 2, polishing: 3, done: 3 };
let genBusy = false;
let renderBusy = false;
const vSVG = (v) => versionMedia(v);
/* the pair, resolved: which image the big stage should show right now */
const pairUrl = (v) => {
  if (!v) return "";
  if (state.stageView === "charm" && v.renderUrl) return v.renderUrl;
  if (state.stageView === "spec" && (v.renderSpecUrl || v.renderSpecURL)) return v.renderSpecUrl || v.renderSpecURL;
  return v.url;
};

/* Every progress controller currently on screen. A project switch aborts
   them all: an overlay belonging to a design nobody is looking at any more
   is a studio that appears to be busy doing nothing. */
const _liveProgs = new Set();
function abortLiveProgress() {
  _liveProgs.forEach((p) => { try { p.abort(); } catch (e) {} });
  _liveProgs.clear();
  const ov = $("#genOverlay"); if (ov) ov.classList.remove("is-visible");
}
function genProgress(stages = GEN_STAGES, index = STAGE_INDEX) {
  const ov = $("#genOverlay"), fill = $("#genBarFill"), pct = $("#genPct"), lbl = $("#genStageLbl");
  let i = -1, timer = null, done = false;
  /* THE BAR BELONGS TO A PROJECT. paint() writes into state.thread — the
     thinking bubble — so a controller left running across a switch narrates
     the old generation into the new design's conversation. */
  const ep = epochNow();
  const paint = (idx) => {
    if (done || idx <= i || !sameProject(ep)) return;
    i = Math.min(idx, stages.length - 1);
    const [txt, p] = stages[i];
    lbl.textContent = txt; fill.style.width = p + "%"; pct.textContent = p + "%";
    const think = state.thread[state.thread.length - 1];
    if (think && think.role === "thinking") { think.text = txt; setThinkingText(txt); }
  };
  const self = {
    start() {
      if (!sameProject(ep)) return;
      _liveProgs.add(self);
      ov.classList.add("is-visible");
      paint(0);
      /* the bar advances on its own between real status pushes so it never
         reads as stalled; a real push always wins because paint() only ever
         moves forward */
      timer = setInterval(() => paint(i + 1), 2600);
    },
    stage(name) { if (index[name] != null) paint(index[name]); },
    finish() {
      done = true; clearInterval(timer);
      _liveProgs.delete(self);
      /* the overlay is shared, and after a switch it belongs to whatever is
         on screen now — a finished old run must not touch it at all */
      if (!sameProject(ep)) return Promise.resolve();
      fill.style.width = "100%"; pct.textContent = "100%";
      return new Promise((r) => setTimeout(() => { ov.classList.remove("is-visible"); r(); }, 350));
    },
    abort() {
      done = true; clearInterval(timer);
      _liveProgs.delete(self);
      ov.classList.remove("is-visible");
    },
  };
  return self;
}

/* The whole ordered exchange is what the server receives as the CUSTOMER
   DIRECTION LOG — it is what makes a resumed design continue rather than
   cold-start (§4b). */
function directionLog() {
  return (state.thread || [])
    .filter((m) => m.role === "me" || (m.role === "studio" && m.vn))
    .map((m) => ({ role: m.role === "me" ? "customer" : "studio", text: String(m.text || "").slice(0, CONFIG.maxInstruction), vn: m.vn || null, at: m.at || null }));
}

/* How long to wait for the version doc to settle before telling the customer
   something went wrong. Generation normally lands well inside this. */
const GEN_TIMEOUT_MS = 180000;

/* geminiImageProxy is deployed as a Netlify BACKGROUND function: it answers
   202 immediately and Netlify discards the function's return value. So the
   VERSION DOC is the source of truth for both progress and completion (§10.11),
   and the HTTP response is treated as an acknowledgement — used when it happens
   to carry a body, ignored when it doesn't. The one exception is a refusal
   (401 / 402 / 429): those are decided before any work starts, so they arrive
   on the HTTP call and should be acted on at once. */
function watchVersion(sessionId, n, prog) {
  let settle = null, unsub = null, poll = null, settled = false;
  const done = new Promise((resolve) => { settle = resolve; });
  const finish = (v) => {
    if (settled) return;
    settled = true;
    if (unsub) { try { unsub(); } catch (e) {} }
    if (poll) clearInterval(poll);
    settle(v);
  };
  const apply = (d) => {
    if (!d) return;
    if (d.stage) prog.stage(d.stage);
    else if (d.status) prog.stage(d.status);
    if (d.status === "done") finish({ ok: true, n, downloadURL: d.downloadURL, storagePath: d.storagePath });
    if (d.status === "failed") finish({ ok: false, error: d.error || "generation_failed" });
  };
  try {
    unsub = db.collection("customSessions").doc(sessionId).collection("versions").doc(String(n))
      .onSnapshot((snap) => { if (snap.exists) apply(snap.data()); },
                  () => { /* listener refused — the poll below carries it */ });
  } catch (e) { /* same */ }
  /* Fallback for browsers where the snapshot listener is blocked. */
  poll = setInterval(async () => {
    try {
      const r = await postAuthed(IMAGE_FN, { kind: "custom_session_status", sessionId, versionNumber: n }, FN_BASE_IMAGE);
      if (r && r.version) apply(r.version);
    } catch (e) { /* keep waiting */ }
  }, 5000);

  return { done, finish, cancel: () => finish(null) };
}

/* ═══════════ DETERMINISTIC EXACT MODE ════════════════════════════════════
   "Exactly as drawn" now means exactly that. When the reference is the
   customer's own vector work — a sketch, or an imported .svg/.ai/.psd taken
   apart into items — the production drawing is COMPOSED from those items by
   the studio's own painter, not generated: the very objects carrying the
   very instructions, painted in the three colours, with the silhouette and
   hoop constructed around them. No model is called, no credit is spent, and
   the same design produces the same drawing every single time. The failure
   modes this thread spent a week on — lost cut-outs, invented rings,
   engraved digits, drifting geometry — are not fixed on this path; they are
   structurally impossible, because nothing interprets anything.

   The AI keeps the two jobs judgement is actually wanted for: "for what I
   mean" redraws, and the gold render. */

function mkComposeEligible() {
  if (!state.reference || !state.reference.drawn) return false;
  const m = (state.markups || {}).draw || {};
  if (m.mode !== "exact") return false;
  return mkItemsOf(m).some((it) => !it.hid && it.t !== "note");
}

/* the charm's own shape, built from the artwork: the items painted solid
   with a fat round stroke ARE the dilation, flooding from the border finds
   the outside, and everything not outside is the body — holes filled by
   construction. Exact, and it uses only the painter the sheet itself uses. */
function mkComposeSilhouette(items, S) {
  const M = 280;                                  /* mask resolution: shape, not detail */
  const cv = document.createElement("canvas");
  cv.width = M; cv.height = M;
  const ctx = cv.getContext("2d", { willReadFrequently: true });
  ctx.fillStyle = "#fff"; ctx.fillRect(0, 0, M, M);
  const fat = items.map((it) => {
    const one = mkPack(it);
    one.c = "#000"; if (one.f !== "none") one.f = "#000";
    one.o = 1;
    one.w = (Number(one.w) || 0.006) + 0.052;     /* the offset margin, as stroke */
    return one;
  });
  try { mkPaint(ctx, M, M, fat, { export: true }); } catch (e) { return null; }
  let d;
  try { d = ctx.getImageData(0, 0, M, M).data; } catch (e) { return null; }
  const ink = new Uint8Array(M * M);
  for (let i = 0; i < M * M; i++) ink[i] = (d[i * 4] + d[i * 4 + 1] + d[i * 4 + 2] < 690) ? 1 : 0;
  /* outside = white reachable from the border */
  const outside = new Uint8Array(M * M);
  const st = [];
  for (let x = 0; x < M; x++) { st.push(x, (M - 1) * M + x); }
  for (let y = 0; y < M; y++) { st.push(y * M, y * M + M - 1); }
  while (st.length) {
    const q = st.pop();
    if (q < 0 || q >= M * M || outside[q] || ink[q]) continue;
    outside[q] = 1;
    const x = q % M;
    if (x > 0) st.push(q - 1);
    if (x < M - 1) st.push(q + 1);
    st.push(q - M, q + M);
  }
  const body = new Uint8Array(M * M);
  let minY = M, cx = 0, cy = 0, area = 0;
  for (let i = 0; i < M * M; i++) {
    if (!outside[i]) {
      body[i] = 1;
      const y = (i / M) | 0;
      if (y < minY) minY = y;
    }
    if (ink[i]) { cx += i % M; cy += (i / M) | 0; area++; }
  }
  if (!area) return null;
  return { body, M, top: minY / M, cx: cx / area / M, cy: cy / area / M };
}

/* body mask → one smooth Path2D at output scale, via the tracer the studio
   already trusts (marching squares + RDP), with a light corner-cut so the
   offset reads as a cut edge rather than a traced blob */
function mkComposeBodyPath(sil, S) {
  const cs = mkTraceMask(sil.body, sil.M).filter((c) => c.length >= 12)
    .map((c) => mkRdp(c, 1.4));
  if (!cs.length) return null;
  const path = new Path2D();
  cs.forEach((c0) => {
    /* Chaikin corner cutting, twice: the mask's staircase becomes a curve */
    let p = c0;
    for (let r = 0; r < 2; r++) {
      const q = [];
      for (let i = 0; i < p.length; i += 2) {
        const j = (i + 2) % p.length;
        q.push(p[i] * .75 + p[j] * .25, p[i + 1] * .75 + p[j + 1] * .25,
               p[i] * .25 + p[j] * .75, p[i + 1] * .25 + p[j + 1] * .75);
      }
      p = q;
    }
    const k = S / sil.M;
    path.moveTo(p[0] * k, p[1] * k);
    for (let i = 2; i < p.length; i += 2) path.lineTo(p[i] * k, p[i + 1] * k);
    path.closePath();
  });
  return path;
}

/* the whole drawing: silhouette + hoop + the items themselves.
   ASYNC, AND IT HAS TO BE. Placed pictures live in Firebase Storage on a
   bucket with no CORS rule, so a plain cross-origin <img> is refused
   outright — mkImageFor already knows this and falls back to fetching the
   bytes same-origin through britesAuth, but only if something WAITS for it.
   Painting first and asking later is what produced a wall of CORS errors
   and a blank compose: the pictures simply were not there yet. Every other
   exporter in the studio awaits this; so does this one now. */
async function mkComposeDrawing(S) {
  const items = mkItemsOf((state.markups || {}).draw)
    .filter((it) => !it.hid && it.t !== "note").map(mkPack);
  if (!items.length) return null;
  try { await mkAwaitImages(items, 12000); } catch (e) { /* paint what we have */ }
  const sil = mkComposeSilhouette(items, S);
  if (!sil) return null;
  const cv = document.createElement("canvas");
  cv.width = S; cv.height = S;
  const ctx = cv.getContext("2d");
  ctx.fillStyle = "#ffffff"; ctx.fillRect(0, 0, S, S);
  const LW = Math.max(3, S * 0.0044);
  const body = mkComposeBodyPath(sil, S);
  /* the hoop: on the body's top edge, directly above the centre of mass */
  const R = S * 0.055, hy = sil.top * S - R * 0.55, hx = sil.cx * S;
  ctx.lineWidth = LW; ctx.strokeStyle = MK_CUTOUT; ctx.lineJoin = "round";
  if (body) ctx.stroke(body);
  ctx.beginPath(); ctx.arc(hx, hy, R, 0, Math.PI * 2); ctx.stroke();
  ctx.beginPath(); ctx.arc(hx, hy, R * 0.46, 0, Math.PI * 2);
  ctx.fillStyle = "#ffffff"; ctx.fill(); ctx.stroke();
  /* the artwork itself — the painter already speaks the three colours */
  try { mkPaint(ctx, S, S, items, { export: true }); } catch (e) { return null; }
  /* ── AND IF ONE PICTURE STILL TAINTED THE CANVAS ────────────────────────
     A single cross-origin image that slipped past the same-origin fallback
     poisons the whole canvas permanently, and toDataURL throws — which would
     lose the customer's entire drawing over one asset. So it is tested here,
     and on failure the sheet is re-composed without the pictures: their
     geometry is gone, but every engraving instruction, every drawn shape and
     the silhouette all survive, and the customer is told which happened. */
  let url = "", tainted = false;
  try {
    url = cv.toDataURL("image/png");
  } catch (e) {
    tainted = true;
    const safe = items.filter((it) => it.t !== "img");
    ctx.fillStyle = "#ffffff"; ctx.fillRect(0, 0, S, S);
    ctx.lineWidth = LW; ctx.strokeStyle = MK_CUTOUT; ctx.lineJoin = "round";
    if (body) ctx.stroke(body);
    ctx.beginPath(); ctx.arc(hx, hy, R, 0, Math.PI * 2); ctx.stroke();
    ctx.beginPath(); ctx.arc(hx, hy, R * 0.46, 0, Math.PI * 2);
    ctx.fillStyle = "#ffffff"; ctx.fill(); ctx.stroke();
    try { mkPaint(ctx, S, S, safe, { export: true }); } catch (e2) { return null; }
    try { url = cv.toDataURL("image/png"); } catch (e2) { return null; }
  }
  return { cv, items, url, tainted };
}

/* A deterministic export of ANY studio sheet. Used when a marked-up drawing
   is in “Exactly as drawn” mode: the updated drawing itself becomes the next
   B/W version, with no model involved and no opportunity to reinterpret it. */
async function mkItemsToDataUrl(items, SIZE) {
  const list = (items || []).map(mkPack);
  if (!list.length) return null;
  try { await mkAwaitImages(list, 12000); } catch (e) { /* paint what we have */ }
  const cv = document.createElement("canvas");
  cv.width = SIZE; cv.height = SIZE;
  const ctx = cv.getContext("2d");
  ctx.fillStyle = "#ffffff";
  ctx.fillRect(0, 0, SIZE, SIZE);
  try { mkPaint(ctx, SIZE, SIZE, list, { export: true }); }
  catch (e) { return null; }
  try {
    return { cv, items: list, url: cv.toDataURL("image/png"), tainted: false };
  } catch (e) {
    const safe = list.filter((it) => it.t !== "img");
    ctx.fillStyle = "#ffffff"; ctx.fillRect(0, 0, SIZE, SIZE);
    try { mkPaint(ctx, SIZE, SIZE, safe, { export: true }); } catch (e2) { return null; }
    try { return { cv, items: safe, url: cv.toDataURL("image/png"), tainted: true }; }
    catch (e2) { return null; }
  }
}


async function runComposedGeneration(ctx) {
  SD("compose: start");
  if (genBusy) { SD("compose: BLOCKED — genBusy"); return; }
  if (uploadsLeft() <= 0) {
    SD("compose: BLOCKED — no upload slots left");
    if (!state.user) openAuth("outOfCredits"); else openPricing("out");
    return;
  }
  const sess = ensureSession();
  /* which project this belongs to, read once, before anything can await */
  const ep = epochNow();
  genBusy = true;
  /* NOTHING between the flag and the try. One thrown line here — a missing
     node, a progress overlay that is not on this page — latched genBusy true
     with no catch to clear it, and every later click on Generate returned
     silently at the top of runGeneration. A studio that has stopped working
     and will not say why is this exact shape. */
  let prog = null;
  try {
    $("#sendBtn").disabled = true;
    const se = $("#stageEmpty"); if (se) se.style.display = "none";
    prog = genProgress();
    prog.start();
    SD("compose: painting the sheet");
    const made = (ctx && Array.isArray(ctx.items) && ctx.items.length)
      ? await mkItemsToDataUrl(ctx.items, 1400)
      : await mkComposeDrawing(1400);
    SD("compose: painted:", made ? ("items=" + made.items.length +
        " bytes=" + (made.url ? made.url.length : 0) + " tainted=" + !!made.tainted)
        : "NULL — nothing to paint");
    if (!made || !made.url) throw new Error("compose failed");
    if (made.tainted) {
      toast("One placed picture couldn't be read back from storage — everything " +
            "else composed exactly. Re-upload that image if you need it in the drawing.", "err");
    }
    const n = state.versions.length + 1;
    /* ── FILED WHERE A DRAWING BELONGS ────────────────────────────────────
       The render does not take a picture. It reads a FIXED Storage path and
       refuses any version whose Firestore doc is not marked done — so a
       composed drawing parked in the uploads folder was invisible to it, and
       the customer got a progress bar that ran to its timeout. This files it
       exactly where a generated drawing is filed, in the same shape, and
       costs no generation credit because no model ran. */
    await flushSession();                     // the server reads the session doc
    /* ── THE REQUEST CANNOT BE ALLOWED TO JUST… NOT COME BACK ─────────────
       The trace showed the filing call reaching the network and never
       settling: no response, no error, no timeout — a promise left pending
       for ever, which is the one failure a plain `await fetch` cannot report.
       Netlify names any function ending in `-background` a background
       function and answers it with a bare 202 and no body, so if IMAGE_FN
       resolves to the background function rather than the kick, the filing
       call can also come back "successful" with nothing in it.

       So this call is made explicitly rather than through the shared
       transport: its URL is logged, it is aborted after 45 seconds, and the
       raw status and body are reported whatever happens. */
    const url = `${FN_BASE_IMAGE}/${IMAGE_FN}`;
    SD("compose: filing version", n, "→ POST", url,
       "(" + Math.round(made.url.length / 1024) + " KB)");
    const tok = await idToken();
    const ctl = new AbortController();
    const abort = setTimeout(() => ctl.abort(), 45000);
    let res, raw = "", up = null;
    try {
      res = await fetch(url, {
        method: "POST",
        headers: Object.assign({ "Content-Type": "application/json" },
                               tok ? { Authorization: "Bearer " + tok } : {}),
        body: JSON.stringify({
          kind: "custom_charm_compose",
          sessionId: sess.id,
          versionNumber: n,
          dataUrl: made.url,
        }),
        signal: ctl.signal,
      });
      raw = await res.text().catch(() => "");
      try { up = raw ? JSON.parse(raw) : null; } catch (e) { up = null; }
      SD("compose: filing answered", res.status, raw.slice(0, 300) || "(empty body)");
    } catch (e) {
      SD("compose: filing THREW", e && e.name, e && e.message);
      const err = new Error(e && e.name === "AbortError"
        ? "compose_timeout" : "compose_network");
      throw err;
    } finally {
      clearTimeout(abort);
    }
    if (res.status === 202) {
      /* a bare 202 means this hit a BACKGROUND function, which can never
         return the URL the version needs — say so precisely rather than
         failing in a way that looks like the drawing was the problem */
      throw new Error("compose_wrong_endpoint");
    }
    if (!res.ok || !up || !up.ok || !up.downloadURL) {
      const e = new Error((up && up.error && String(up.error)) ||
                          ("compose_http_" + res.status));
      e.status = res.status;
      throw e;
    }
    const v = {
      n, kind: "bw", composed: 1,
      zones: mkZonesPayload(made.items).slice(0, 40),
      url: up.downloadURL, path: up.storagePath || "",
      src: (ctx && ctx.fromMarkup) ? "ai" : "ref",
      label: (ctx && ctx.label) || "Generated",
      instructions: state.desc,
      at: now(),
    };
    const msg = (ctx && ctx.fromMarkup)
      ? "Updated — re-drawn exactly from your marked-up drawing."
      : (ctx && ctx.fromReference)
      ? "Updated — re-drawn exactly from your reference."
      : "Composed exactly from your drawing — every engraving choice placed by the studio itself, nothing interpreted, and no generation credit spent.";
    /* ── THE CUSTOMER LEFT WHILE THIS WAS COMPOSING ───────────────────────
       The drawing is finished and filed in Storage; the only question is
       which design it belongs to, and the answer is never "whichever one is
       open now". It goes on the session it was composed for, together with
       the layer sheet the items already give us for free. */
    if (!sameProject(ep)) {
      if (!sess.markups) sess.markups = {};
      sess.markups[String(n)] = {
        items: made.items.map(mkPack), traced: 1, dirty: 0,
        plan: v.zones.slice(0, 40), mode: "exact",
        crop: [], baseRot: 0, guides: 0, gridN: 12, groups: [],
      };
      await orphanVersion(sess, v, msg);
      if (prog) { try { prog.abort(); } catch (e) {} prog = null; }
      return;
    }
    state.versions.push(v);
    state.currentVersion = state.versions.length - 1;
    state.stageView = "bw";
    const last = state.thread[state.thread.length - 1];
    if (last && last.role === "thinking") { last.role = "studio"; last.vn = n; last.text = msg; }
    else pushMsg({ role: "studio", vn: n, text: msg });
    logEvent("generate", (ctx && ctx.fromMarkup)
      ? "Re-drew deterministically from the marked-up drawing"
      : (ctx && ctx.fromReference)
      ? "Re-drew deterministically from the reference"
      : "Composed the drawing deterministically (exact mode)", n);
    /* Finish the deterministic B/W handoff before any slow persistence work.
       The version already exists at this point; the UI must be allowed to
       show it immediately and must not still think a generation is busy. */
    renderThread();
    saveSession();
    if (prog) { await prog.finish(); prog = null; }
    genBusy = false;
    const sb = $("#sendBtn"); if (sb) sb.disabled = false;
    renderStage();
    refreshWallet();
    await flushSession();
    /* prog.finish() and the flush above both await, and a customer can leave
       inside 350 ms — the seeded sheet must not follow them */
    if (!sameProject(ep)) return;
    /* the layers, immediately: the items ARE the truth, so the sheet is
       seeded from them rather than traced back off the pixels */
    if (!state.markups) state.markups = {};
    state.markups[String(n)] = {
      items: made.items.map(mkPack), traced: 1, dirty: 0,
      plan: v.zones.slice(0, 40), mode: "exact",
      crop: [], baseRot: 0, guides: 0, gridN: 12, groups: [],
    };
    saveSession();
  } catch (err) {
    console.warn("[studio] compose FAILED:", err && (err.stack || err.message), err);
    const _why = String((err && err.message) || "");
    if (!sameProject(ep)) {
      await orphanFail(sess, "That drawing didn't come through. Nothing was charged — send it again and we'll try afresh.");
      try { if (prog) prog.abort(); } catch (e) {}
      return;
    }
    toast(_why === "compose_wrong_endpoint"
      ? "The studio is posting to the background function, which can't answer. Set the section's image function to geminiImageProxyKick."
      : _why === "compose_timeout"
      ? "Filing the drawing timed out after 45 seconds — the function didn't answer."
      : _why === "compose_network"
      ? "Couldn't reach the studio function to file the drawing — check the network tab."
      : _why === "unknown_kind"
      ? "The studio composed your drawing, but the server hasn't been updated to file it yet. Switch to \"For what I mean\" for now."
      : _why === "image_too_large"
      ? "That drawing came out too large to file — simplify it slightly, or use \"For what I mean\"."
      : _why === "rate_limited"
      ? "That's a lot of drawings in one go — give it a minute and try again."
      : err && err.status === 402
      ? "You've used every image slot on this account — add credits to keep going."
      : "That didn't compose — try once more, or switch to \"For what I mean\".", "err");
  } finally {
    /* Success finishes the progress controller above. On every error path,
       abort it here so an exception can never strand the overlay on screen. */
    try { if (prog) prog.abort(); } catch (e) {}
    /* AFTER A SWITCH THESE FLAGS BELONG TO SOMEBODY ELSE. Clearing genBusy
       here would let the customer fire a second generation on top of the one
       they just started in the new design. */
    if (sameProject(ep)) {
      genBusy = false;
      const sb = $("#sendBtn"); if (sb) sb.disabled = false;
      /* If failure or an early exit painted controls while genBusy was true,
         repaint once with the final state. */
      try { renderStage(); } catch (e) {}
    }
  }
}

async function runGeneration(label, extraDesc = "", opts) {
  SD("runGeneration:", label, "fromReference=" + !!(opts && opts.fromReference));
  if (genBusy) {
    SD("runGeneration: BLOCKED — genBusy already true");
    toast("Still working on the last drawing — one moment.", "err"); return;
  }
  /* ── EXACT MODE NEVER REACHES A MODEL ─────────────────────────────────
     First generations and from-reference re-draws of a vector-backed design
     in exact mode are composed locally. Refinements and mark-up re-draws
     still go to the model: they are requests for judgement by definition. */
  const __fromRef = !!(opts && opts.fromReference);
  let __elig = false;
  try { __elig = mkComposeEligible(); }
  catch (e) { SD("mkComposeEligible THREW:", e && e.message); __elig = false; }
  const __compose = (label !== "Refined" || __fromRef) && __elig;
  SD("route:", __compose ? "COMPOSE (no model)" : "MODEL", "{eligible:" + __elig + "}");
  if (__compose) return runComposedGeneration({ label, fromReference: __fromRef });
  if (!state.unlimited && state.credits < CONFIG.generateCost) { openPricing("out"); return; }
  const sess = ensureSession();
  /* which project this belongs to, read once, before anything can await */
  const ep = epochNow();
  await flushSession();                       // the server reads the session doc
  if (!sameProject(ep)) return;               // left before it even started

  genBusy = true;
  $("#sendBtn").disabled = true;
  $("#stageEmpty").style.display = "none";
  $("#stageFlag").classList.remove("is-visible");

  const n = state.versions.length + 1;
  /* A DRAW FROM THE REFERENCE IS NOT A REFINEMENT. The customer has just
     changed the picture the whole design descends from and asked for it to
     be drawn again — sending the previous drawing alongside would hand the
     model the very output it is meant to be replacing. */
  const fromRef = !!(opts && opts.fromReference);
  const refine = !fromRef && label === "Refined" && state.versions.length > 0;
  const prog = genProgress();
  prog.start();

  const watch = watchVersion(sess.id, n, prog);
  const timer = setTimeout(() => watch.finish({ ok: false, error: "timeout" }), GEN_TIMEOUT_MS);

  /* If the customer marked up the drawing they are refining, the marks go
     with the message: base drawing + their ink + their notes, composited into
     one image the designer reads as direction. */
  let markup = null;
  if (refine) {
    try { markup = await buildMarkupPayload(state.versions[state.currentVersion]); }
    catch (e) { console.warn("[studio] markup payload:", e && e.message); }
  }

  let ack = null, error = null;
  /* the account of this design's areas that produced THIS version — stored
     on it below, so the refine after it, the render after that and the next
     session all start from the same three instructions */
  let planZones = [];
  try {
    /* when the reference is the customer's OWN sketch, their exact/interpret
       choice from the sketchpad travels with the generation */
    const refMode = (state.reference && state.reference.drawn)
      ? ((((state.markups || {}).draw || {}).mode === "exact") ? "exact" : "interpret")
      : undefined;
    /* ── THE FILL LAW HAS TO TRAVEL WITH THE SKETCH ────────────────────
       When the customer draws their own charm, "use this as my reference"
       flattens the sheet to a PNG — and the moment it is a PNG, the fact
       that the cherry was CUT OUT and the ring was ENGRAVED is nothing but
       two colours in a picture for the model to interpret however it likes.
       Which is exactly what it did: both came back as plain metal.

       The marks were never the problem; the flattening was. So the same
       structured account of every filled area that a mark-up sends is now
       built from the sketch itself and sent on the FIRST generation too.
       A colour in a JPEG is a thing to be interpreted. "One area, cut clean
       through, upper right" is not. */
    /* THE PLAN, WHOLE, ON EVERY GENERATION — first, refined, or re-drawn
       from marks. It used to be assembled here and only for a sketch, which
       meant a design that started from an uploaded picture or a catalogue
       charm sent no account of its cut-outs at all, and a REFINE sent the
       previous drawing with nothing to say about it. mkFillPlan answers the
       question once, from the nearest truth available. */
    /* drawn FROM the reference: the reference's own plan is the truth, not
       the plan of the drawing this one is replacing */
    planZones = fromRef ? ((state.reference && Array.isArray(state.reference.zones)
                            && state.reference.zones.length)
                             ? state.reference.zones.slice(0, 40)
                             : mkFillPlan(null))
                        : mkFillPlan(state.versions[state.currentVersion]);
    if (!planZones.length && state.reference && !state.reference.drawn) {
      /* nobody has said anything yet, so ask the reference itself */
      try { planZones = await mkReadRefPlan(); } catch (e) { planZones = []; }
    }
    /* ONE LIST, NEVER TWO. A marked-up sheet is the more specific account of
       the two and it is the one the customer is looking at; sending the
       reference's list beside it hands the model two censuses of the same
       charm and invites it to split the difference. */
    const refZones = markup ? null : (planZones.length ? planZones : null);
    ack = await postAuthed(IMAGE_FN, Object.assign({
      kind: refine ? "custom_charm_refine" : "custom_charm_generate",
      sessionId: sess.id,
      versionNumber: n,
      instructions: state.desc,
      refineText: (function () {
        /* the audit's dictation rides with the customer's own words: a
           drawing that lost a declared cut-out is corrected by name on the
           very next attempt, whichever button that attempt comes from */
        const prev = state.versions[state.currentVersion];
        const note = prev ? mkAuditNote(prev) : "";
        return note ? ((extraDesc ? extraDesc + "\n\n" : "") + note) : extraDesc;
      })(),
      thread: directionLog(),
      metal: state.metal,
      refMode,
    }, refZones ? { refZones } : {},
       markup ? { markupImage: markup.image, markupNotes: markup.notes,
                  markupMode: markup.mode, markupZones: markup.zones } : {}), FN_BASE_IMAGE);
    if (markup && Array.isArray(markup.zones) && markup.zones.length) planZones = markup.zones;
  } catch (e) { error = e; }

  /* A refusal is decided before the function does any work, so it comes back
     on the HTTP call even from a background function. */
  if (error && [401, 402, 429].includes(error.status)) watch.finish({ ok: false, error: String(error.status) });

  /* 200 with a body (non-background alias) settles immediately; 202 with an
     empty body means "accepted, watch the doc". */
  const result = (ack && ack.ok && (ack.downloadURL || ack.storagePath))
    ? ack
    : await watch.done;
  clearTimeout(timer);
  watch.cancel();

  if (!result || !result.ok) {
    prog.abort();
    const why = (error && error.status) || (result && result.error) || "";
    /* ── FAILED, AND THE CUSTOMER IS SOMEWHERE ELSE ──────────────────────
       The apology belongs in the conversation it interrupted, not in the
       design they are looking at now — and the busy flags belong to that
       design's generation, not this one's. */
    if (!sameProject(ep)) {
      await orphanFail(sess, "That one didn't come through. Nothing was charged — send it again and we'll try afresh.");
      return;
    }
    genBusy = false;
    $("#sendBtn").disabled = false;
    const msg = String(why) === "402" || String(why).includes("out_of_credits")
      ? "You're out of design credits — top up and we'll pick this straight back up."
      : String(why) === "401"
        ? "Your session timed out — sign in again and we'll carry on."
        : String(why) === "429" || String(why).includes("rate_limited")
          ? "That's a lot of designs in one go — give it a minute and send it again."
          : String(why) === "timeout"
            ? "That one is taking longer than it should. Nothing was charged — send it again and we'll try afresh."
            : "That one didn't come through. Nothing was charged — send it again and we'll try afresh.";
    /* the thinking bubble becomes an honest explanation, not a silent stall */
    const last = state.thread[state.thread.length - 1];
    if (last && last.role === "thinking") { last.role = "studio"; last.text = msg; delete last.vn; }
    else pushMsg({ role: "studio", text: msg });
    renderThread(); saveSession();
    refreshWallet();                          // the refund lands here
    if (String(why) === "402") openPricing("out");
    else if (String(why) === "401") openAuth("default");
    else toast(msg, "err");
    return;
  }

  await prog.finish();
  if (sameProject(ep)) spendCredit();         // repaint from the wallet snapshot

  const v = {
    n: result.n || n,
    kind: "bw",                     // the design step draws; the render step manufactures
    /* THE FILL PLAN TRAVELS WITH THE DRAWING IT MADE. This is what stops the
       three instructions dying at the moment a design becomes black ink. */
    zones: (planZones || []).slice(0, 40),
    url: result.downloadURL || result.url,
    path: result.storagePath || result.path,
    metal: state.metal,
    label,
    instructions: state.desc,
    refine: extraDesc,
    at: now(),
  };
  /* ── THE DRAWING ARRIVED IN A DESIGN THE CUSTOMER HAS LEFT ────────────
     It was paid for and it exists; it simply is not this project's. File it
     on the session it was generated for and stop. It will be taken apart
     into layers the next time that design is opened — traceVersion runs off
     `state`, and `state` is somebody else's now. */
  if (!sameProject(ep)) {
    const _msg = (sess.versions || []).length === 0
      ? "Here's your charm as a production drawing — every black line is engraving, exactly where it will be cut."
      : "Updated — here's the new drawing.";
    await orphanVersion(sess, v, _msg, label === "Refined" ? "refine" : "generate");
    return;
  }
  state.versions.push(v);
  /* take it apart while the customer is reading the message about it: by
     the time they open the sheet, or look in My designs, or drag it out of
     the repository, every part of it is already its own object */
  traceVersionSoon(v);
  state.currentVersion = state.versions.length - 1;
  state.approved = false;
  state.stageView = "bw";           // a new drawing always shows as a drawing
  logEvent(label === "Refined" ? "refine" : "generate",
    label === "Refined" ? `Refined: “${extraDesc}”`
      : (state.versions.length === 1 && state.desc.trim() ? `Generated from: “${state.desc.trim()}”` : `Generated a design`),
    v.n);

  /* swap the thinking bubble for the finished result card */
  const last = state.thread[state.thread.length - 1];
  if (last && last.role === "thinking") {
    last.role = "studio"; last.vn = v.n;
    last.text = state.versions.length === 1
      ? "Here's your charm as a production drawing — every black line is engraving, exactly where it will be cut. Happy with it? See it in metal."
      : "Updated — here's the new drawing.";
  } else {
    pushMsg({ role: "studio", vn: v.n, text: "Here's the new version." });
  }
  /* genBusy comes down BEFORE the repaint: renderStage disables the render
     button while a generation is busy, so painting first and flipping the
     flag after left the button dead until some later repaint. */
  genBusy = false;
  $("#sendBtn").disabled = false;
  saveSession();
  renderStage();
  refreshWallet();
  if (!state.unlimited && state.credits === 0 && !state.user) setTimeout(() => openAuth("outOfCredits"), 1600);
  $("#descInput").focus();
}

/* The gold-mask segment lives in the section markup. This exists only for a
   browser holding a CACHED copy of the older liquid, which had two segments
   and no third: without it the mask would be unreachable on that page rather
   than merely unstyled. It uses the SAME class as its siblings — an earlier
   version built it with a class the stylesheet has never contained, so it
   rendered as unstyled body text between two segmented buttons. */
function ensureSpecToggle() {
  const wrap = $("#pairToggle");
  if (!wrap || $("#toggleSpec")) return;
  const b = document.createElement("button");
  b.type = "button";
  b.id = "toggleSpec";
  b.className = "pair-toggle__b";
  b.setAttribute("role", "tab");
  b.setAttribute("aria-selected", "false");
  b.title = "The gold mask — the exact instruction image sent to the renderer.";
  b.innerHTML = "Gold&nbsp;mask";
  b.hidden = true;
  const bw = $("#toggleBW");
  if (bw && bw.nextSibling) wrap.insertBefore(b, bw.nextSibling);
  else wrap.appendChild(b);
  b.addEventListener("click", () => { state.stageView = "spec"; renderStage(); });
}

function renderStage() {
  const stage = $("#stage");
  /* the reference chip sits on the stage now, so the stage refreshes it —
     one owner, and no path that changes the reference can leave it stale */
  renderRefDock(refName());
  stage.querySelectorAll(".result").forEach((e) => e.remove());
  const v = state.versions[state.currentVersion];
  const flag = $("#stageFlag");

  if (!v) {
    /* NOTHING GENERATED YET — so show what we ARE working from.
       An empty frame after changing the reference reads as "my new choice
       didn't take"; the reference itself, faded behind the invitation, says
       plainly which image the next generation will be built on. */
    const ref = state.reference;
    const refImg = ref ? (ref.type === "catalog" ? (ref.item && ref.item.image) : ref.url) : "";
    if (refImg) {
      const holder0 = document.createElement("div");
      holder0.className = "result stage-refpreview";
      holder0.innerHTML = `<img src="${attrText(refImg)}" alt="${attrText(refName() || "Your reference")}">`;
      stage.appendChild(holder0);
    }
    stage.classList.toggle("stage--refonly", !!refImg);
    $("#stageEmpty").style.display = "block";
    $("#stageEmptyMsg").textContent = refImg ? "Designing from this" : "Your charm appears here";
    $("#stageEmptySub").textContent = state.reference
      ? "Describe it in the conversation to begin"
      : "Pick a reference to begin";
    flag.classList.remove("is-visible");
    $("#railVersion").innerHTML = "";
    $("#versionStrip").innerHTML = "";
    $("#approveBtn").hidden = true;
    const rail0 = $("#pairRail");
    if (rail0) {
      rail0.hidden = true;
      const grid0 = rail0.closest(".gen-grid");
      if (grid0) grid0.classList.remove("has-rail");
    }
    stage.classList.remove("stage--paper");
    updateNavArrows();
    return;
  }

  $("#stageEmpty").style.display = "none";
  stage.classList.remove("stage--refonly");
  const hasRender = !!v.renderUrl;
  const hasSpec = !!(v.renderSpecUrl || v.renderSpecURL);
  ensureSpecToggle();
  if (state.stageView === "charm" && !hasRender) state.stageView = hasSpec ? "spec" : "bw";
  if (state.stageView === "spec" && !hasSpec) state.stageView = hasRender ? "charm" : "bw";
  const view = state.stageView === "charm" && hasRender ? "charm"
             : state.stageView === "spec" && hasSpec ? "spec"
             : "bw";

  const holder = document.createElement("div");
  holder.innerHTML = versionMedia({ url: pairUrl(v) });
  const el = holder.firstElementChild;
  el.classList.add("result");
  stage.appendChild(el);
  /* THE STAGE FOLLOWS THE PICTURE. The metal render used to arrive on pure
     black, so the stage was black. It now arrives on a white ground with a
     real cast shadow under it — and a white render inside a black frame
     reads as a mistake, not as a photograph. Both halves of the pair are
     white-backed now, so the stage is one surface throughout. */
  stage.classList.add("stage--paper");
  void view;
  flag.textContent = view === "bw"
    ? "Production drawing"
    : view === "spec"
      ? "Gold mask — what the renderer was told"
      : (state.approved ? "✓ Approved" : "Your charm");
  flag.classList.toggle("stage-flag--approved", view === "charm" && state.approved);
  flag.classList.add("is-visible");
  $("#railVersion").innerHTML = `<b>v${v.n}</b><span>of ${state.versions.length}</span>`;

  /* ── the pair rail: toggle, markup, render ─────────────────────────── */
  const rail = $("#pairRail");
  if (rail) {
    rail.hidden = false;
    /* the rail lives INSIDE the stage's height budget — see .has-rail */
    const grid = rail.closest(".gen-grid");
    if (grid) grid.classList.add("has-rail");
    const tog = $("#pairToggle");
    tog.hidden = !(hasRender || hasSpec);
    $("#toggleBW").classList.toggle("is-on", view === "bw");
    $("#toggleBW").setAttribute("aria-selected", view === "bw" ? "true" : "false");
    const tSpec = $("#toggleSpec");
    if (tSpec) {
      tSpec.hidden = !hasSpec;
      tSpec.classList.toggle("is-on", view === "spec");
      tSpec.setAttribute("aria-selected", view === "spec" ? "true" : "false");
    }
    $("#toggleCharm").classList.toggle("is-on", view === "charm");
    $("#toggleCharm").setAttribute("aria-selected", view === "charm" ? "true" : "false");
    $("#toggleCharm").hidden = !hasRender;

    /* ALWAYS visible. It used to hide itself whenever the metal render was on
       screen, on the reasoning that marks apply to the drawing — true, but the
       cure for that is to switch to the drawing when it is pressed, not to
       take the control away and leave people hunting for it. */
    const marked = markupHasContent((state.markups || {})[v.n]);
    const mkBtn = $("#markupBtn");
    mkBtn.hidden = false;
    mkBtn.classList.toggle("is-marked", marked);
    $("#markupBtnTxt").textContent = marked ? "Marks saved" : "Mark up";

    const rBtn = $("#renderBtn");
    rBtn.hidden = false;
    rBtn.disabled = renderBusy || genBusy;
    $("#renderBtnTxt").innerHTML = hasRender
      ? "Re-render&nbsp;✦"
      : `See it in ${escapeHtml((METAL_LABELS[state.metal] || "metal").replace(/ Filled$/, ""))}&nbsp;✦`;
    /* The button's price follows the chosen renderer, so the number on the
       button and the number the server will actually charge are the same
       number. */
    /* ── ONE RENDERER, SO NO CHOICE IS OFFERED ────────────────────────────
       The Standard/High control is hidden while the render pipeline is being
       corrected, and hidden is not the same as absent: it stays in the DOM,
       priced and wired, so bringing it back is a Firestore value rather than
       a deploy. `renderQualityTiers` is the SAME value the server reads to
       decide whether it will honour quality:"high" — one switch for both, so
       the button can never offer a tier the server would refuse, and the
       server can never charge for a tier the button never showed. */
    const q = renderQuality();
    $("#renderBtnCost").textContent = creditWord(renderCost());
    const qWrap = $("#renderQuality");
    if (qWrap) {
      qWrap.hidden = !renderTiersOn();
      const lo = $("#rqLow"), hi = $("#rqHigh"), loC = $("#rqLowCost"), hiC = $("#rqHighCost");
      if (loC) loC.textContent = creditWord(renderCostFor("low"));
      if (hiC) hiC.textContent = creditWord(renderCostFor("high"));
      [[lo, "low"], [hi, "high"]].forEach(([b, val]) => {
        if (!b) return;
        b.classList.toggle("is-on", q === val);
        b.setAttribute("aria-checked", q === val ? "true" : "false");
        b.disabled = renderBusy || genBusy;
      });
    }
    /* Same shape as the tier control above, and inert mid-render for the same
       reason: changing what a render in flight was asked for would be a lie
       about what is happening. */
    const pmWrap = $("#renderPromptMode");
    if (pmWrap) {
      pmWrap.hidden = false;
      const pm = promptMode();
      [[$("#pmFull"), "full"], [$("#pmShort"), "short"]].forEach(([b, val]) => {
        if (!b) return;
        b.classList.toggle("is-on", pm === val);
        b.setAttribute("aria-checked", pm === val ? "true" : "false");
        b.disabled = renderBusy || genBusy;
      });
    }
  }

  /* Approval is of the PAIR — drawing plus rendered charm — so it waits for
     the render. The disabled button says why rather than just refusing. */
  const ab = $("#approveBtn");
  ab.hidden = false;
  if (state.approved) {
    ab.disabled = false;
    ab.innerHTML = "✓&nbsp; Approved — press Next to order";
  } else if (hasRender) {
    ab.disabled = false;
    ab.innerHTML = "♥&nbsp; Approve this design";
  } else {
    ab.disabled = true;
    ab.innerHTML = "♥&nbsp; Approve — see it in metal first";
  }

  $("#versionStrip").innerHTML = state.versions.map((ver, i) => `
    <button class="version-thumb${i === state.currentVersion ? " is-current" : ""}" data-i="${i}" type="button" title="${attrText(ver.label)} · version ${ver.n}${ver.renderUrl ? " · rendered" : ""}">
      ${vSVG(ver)}${ver.renderUrl ? '<span class="v-gold" title="Rendered in metal"></span>' : ""}<span class="v-num">v${ver.n}</span>
    </button>`).join("");
  $$("#versionStrip .version-thumb").forEach((b) => b.addEventListener("click", () => selectVersion(+b.dataset.i)));
  renderThread();
  updateNavArrows();
}
function selectVersion(i) {
  if (i < 0 || i >= state.versions.length) return;
  state.currentVersion = i;
  state.approved = false;
  state.stageView = state.versions[i].renderUrl ? "charm" : ((state.versions[i].renderSpecUrl || state.versions[i].renderSpecURL) ? "spec" : "bw");
  renderStage(); saveSession();
}
$("#toggleBW").addEventListener("click", () => { state.stageView = "bw"; renderStage(); });
if ($("#toggleSpec")) $("#toggleSpec").addEventListener("click", () => { state.stageView = "spec"; renderStage(); });
$("#toggleCharm").addEventListener("click", () => { state.stageView = "charm"; renderStage(); });
$("#renderBtn").addEventListener("click", () => runRender());
/* ── the renderer choice ─────────────────────────────────────────────────
   It is a preference, not a property of one drawing: it persists on the
   session and survives version switches, so a customer who has decided they
   want the better renderer does not have to re-decide on every press. It is
   inert mid-render — changing the price of a render already being paid for
   would be a lie about what is happening. */
RENDER_QUALITIES.forEach((q) => {
  const b = $(q === "high" ? "#rqHigh" : "#rqLow");
  if (!b) return;
  b.addEventListener("click", () => {
    if (renderBusy || genBusy) return;
    if (renderQuality() === q) return;
    state.renderQuality = q;
    saveSession();
    renderStage();
    toast(q === "high"
      ? `High quality on — each render now costs ${creditWord(renderCostFor("high"))}.`
      : `Standard quality on — each render costs ${creditWord(renderCostFor("low"))}.`, "gold");
  });
});
/* ── the prompt-mode choice ──────────────────────────────────────────────
   A diagnostic, so it says what it is when pressed rather than pretending to
   be a quality setting. It costs the same either way. */
[["#pmFull", "full"], ["#pmShort", "short"]].forEach(([sel, val]) => {
  const b = $(sel);
  if (!b) return;
  b.addEventListener("click", () => {
    if (renderBusy || genBusy) return;
    if (promptMode() === val) return;
    state.promptMode = val;
    saveSession();
    renderStage();
    toast(val === "short"
      ? "Short prompt on — the three greyscale rules only. Same credit."
      : "Full prompt on — the complete instruction set. Same credit.", "gold");
  });
});
$("#markupBtn").addEventListener("click", () => openMarkup());

/* ═══════════ CONVERSATION — persistent thread per design session ══════════
   state.thread is written with the rest of the session doc, so reopening a
   design restores the whole exchange; the same array is sent to
   custom_charm_generate as the ordered CUSTOMER DIRECTION LOG.
                                                        [prototype, verbatim] */
function pushMsg(m) { state.thread.push(Object.assign({ at: Date.now() }, m)); }
function renderThread() {
  const box = $("#chatThread"); if (!box) return;
  if (!box.dataset.wired) {
    box.dataset.wired = "1";
    box.addEventListener("click", (e) => {
      const b = e.target.closest(".msg-result");
      if (b && box.contains(b)) selectVersion(+b.dataset.v - 1);
    });
  }
  if (!state.thread.length) {
    if (box.dataset.mode !== "empty") {
      box.dataset.mode = "empty";
      box.innerHTML = `<div class="chat__empty"><b>Tell us your idea</b>
        Describe the charm you want and we'll draw it.<br>Keep talking to refine it — every reply remembers the whole conversation.</div>`;
    }
    return;
  }
  const htmls = state.thread.map((m) => {
    if (m.role === "me") {
      return `<div class="msg msg--me"><span class="msg__who">You</span>
        <div class="msg__body">${escapeHtml(m.text)}</div></div>`;
    }
    if (m.role === "thinking") {
      return `<div class="msg msg--studio msg--thinking"><span class="msg__who">Studio</span>
        <div class="msg__body"><span class="dots"><i></i><i></i><i></i></span><span class="msg__txt">${escapeHtml(m.text || "Drawing your charm…")}</span></div></div>`;
    }
    const v = state.versions[(m.vn || 1) - 1];
    const isCur = v && state.versions.indexOf(v) === state.currentVersion;
    return `<div class="msg msg--studio"><span class="msg__who">Studio</span>
      <div class="msg__body">${escapeHtml(m.text || "Here it is.")}</div>
      ${(m.vn && v) ? `<button class="msg-result${isCur ? " is-current" : ""}" data-v="${m.vn}" type="button"
              title="${isCur ? "Showing now" : "Tap to view this version"}">
        <span class="msg-result__thumb">${vSVG(v)}</span>
        <span class="msg-result__v">Charm_v${v.n}</span>
      </button>` : ""}</div>`;
  });
  if (box.dataset.mode !== "thread") { box.dataset.mode = "thread"; box.innerHTML = ""; }
  const atBottom = box.scrollHeight - box.scrollTop - box.clientHeight < 40 || !box.children.length;
  let changed = false;
  htmls.forEach((h, i) => {
    const cur = box.children[i];
    if (cur && cur.dataset.sig === h) return;      // identical → leave the node alone
    const tmp = document.createElement("div");
    tmp.innerHTML = h.trim();
    const el = tmp.firstElementChild;
    if (!el) return;
    el.dataset.sig = h;
    if (cur) box.replaceChild(el, cur); else box.appendChild(el);
    changed = true;
  });
  while (box.children.length > htmls.length) { box.removeChild(box.lastElementChild); changed = true; }
  if (changed && atBottom) box.scrollTop = box.scrollHeight;
}
function setThinkingText(txt) {
  const box = $("#chatThread"); if (!box) return;
  const node = box.querySelector(".msg--thinking .msg__txt");
  if (!node) { renderThread(); return; }
  if (node.textContent === txt) return;
  const atBottom = box.scrollHeight - box.scrollTop - box.clientHeight < 24;
  node.textContent = txt;
  if (atBottom) box.scrollTop = box.scrollHeight;
}
function autoGrow() {
  const t = $("#descInput");
  if (!t || t.offsetParent === null) return;      // step hidden → scrollHeight is 0
  t.style.height = "auto";
  t.style.height = Math.max(34, Math.min(t.scrollHeight, 132)) + "px";
}
/* ═══════════ THE TRACE ═══════════════════════════════════════════════════
   Five rounds have now been spent guessing at a click that does nothing and
   reports nothing. Guessing from an absence is not debugging. Every decision
   point between the button and the network call now says what it decided, so
   the console names the line instead of implying it. */
const SD = (...a) => { try { console.info("[studio]", ...a); } catch (e) {} };

/* type __studioDiag() in the console: one object with everything that decides
   whether Generate does anything at all */
window.__studioDiag = function () {
  const d = {
    build: window.__studioBuild,
    genBusy, renderBusy,
    signedIn: !!state.user,
    credits: state.credits, unlimited: !!state.unlimited,
    versions: (state.versions || []).length,
    currentVersion: state.currentVersion,
    referenceType: state.reference ? state.reference.type : null,
    referenceDrawn: !!(state.reference && state.reference.drawn),
    sketchMode: (((state.markups || {}).draw) || {}).mode || null,
    sketchItems: mkItemsOf((state.markups || {}).draw).length,
    composeEligible: (function () { try { return mkComposeEligible(); }
                                    catch (e) { return "threw: " + e.message; } })(),
    uploadsLeft: (function () { try { return uploadsLeft(); } catch (e) { return "?"; } })(),
  };
  console.log(d);
  return d;
};

async function sendMessage() {
  SD("sendMessage: clicked");
  const t = $("#descInput");
  const text = t.value.trim();
  /* a click that does nothing must SAY WHY: a busy flag latched by an earlier
     failure otherwise reads as "the studio is broken", with an empty console */
  if (genBusy || renderBusy) {
    SD("sendMessage: BLOCKED — genBusy=" + genBusy + " renderBusy=" + renderBusy);
    toast(genBusy ? "Still working on the last drawing — one moment."
                  : "Still rendering the last charm — one moment.", "err");
    return;
  }
  if (!state.reference) {
    SD("sendMessage: BLOCKED — no reference on the design");
    toast("Pick a reference on the previous step first", "err"); gotoStep(1); return;
  }
  /* Words are OPTIONAL. The reference is the only mandatory input: an empty
     first send translates it faithfully, and an empty send on a marked-up
     drawing regenerates from the marks — the marks ARE the message. */
  if (!text) {
    if (!state.versions.length) {
      pushMsg({ role: "thinking", text: "Reading your inspiration…" });
      renderThread(); saveSession();
      logEvent("generate", "Generated from the reference");
      await runGeneration("Generated", "");
      return;
    }
    const cur = state.versions[state.currentVersion];
    if (cur && markupHasContent((state.markups || {})[cur.n])) {
      await regenerateFromMarkup();
      return;
    }
    toast("Tell us what to change, or mark the drawing up — an empty message " +
          "only regenerates when there are marks to follow.", "err");
    t.focus(); return;
  }
  const first = !state.thread.some((m) => m.role === "me");
  state.desc = state.desc ? (state.desc + " " + text) : text;   // running brief
  ensureSession();
  pushMsg({ role: "me", text, withRef: first });
  pushMsg({ role: "thinking", text: "Reading your inspiration…" });
  t.value = ""; $("#charCount").textContent = "0"; autoGrow();
  renderThread(); saveSession();
  logEvent(first ? "generate" : "refine", `${first ? "Asked for" : "Refined"}: “${text}”`);
  await runGeneration(first ? "Generated" : "Refined", first ? "" : text);
}
$("#sendBtn").addEventListener("click", sendMessage);
$$(".composer").forEach((c) => c.addEventListener("mousedown", (e) => {
  if (e.target === c) { e.preventDefault(); $("#descInput").focus(); }
}));
$("#descInput").addEventListener("keydown", (e) => {
  if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); sendMessage(); }
});
function designName() {
  const base = state.reference
    ? (state.reference.type === "catalog" ? state.reference.item.title
       : state.reference.drawn ? "My sketch" : "Custom upload")
    /* a sketch with no reference yet is still a design, and it needs a name
       to be findable in My designs before it is ever submitted */
    : (state.desc.trim().split(/\s+/).slice(0, 4).join(" ")
       || (markupHasContent((state.markups || {}).draw) ? "My sketch" : "Custom charm"));
  return `${base} — custom`;
}
$("#approveBtn").addEventListener("click", () => {
  if (!state.versions.length) return;
  const v = state.versions[state.currentVersion];
  if (v && !v.renderUrl) { toast("See it in metal first — approval is of the finished pair", "err"); return; }
  if (!state.user) { openAuth("approve"); return; }
  approveNow();
});
let _orderView = "charm";
function refreshApproveStage() {
  const v = state.versions[state.currentVersion];
  if (!v) return;
  const fs = $("#finalStage");
  fs.querySelectorAll(".result").forEach((e) => e.remove());
  const holder = document.createElement("div");
  holder.innerHTML = versionMedia({ url: _orderView === "bw" ? v.url : (v.renderUrl || v.url) });
  const el = holder.firstElementChild; el.classList.add("result");
  fs.appendChild(el);
  fs.classList.add("stage--paper");
  void _orderView;
  /* the approved SET, side by side: the charm and the drawing it is cut
     from. Click either to put it on the big stage — the pair is never
     hidden, so there is no doubt about what was chosen. */
  const pair = $("#orderPair");
  if (pair) {
    if (v.renderUrl) {
      pair.hidden = false;
      $("#pairCharmT").innerHTML = versionMedia({ url: v.renderUrl });
      $("#pairBWT").innerHTML = versionMedia({ url: v.url });
      $("#pairCharmT").classList.toggle("is-on", _orderView === "charm");
      $("#pairBWT").classList.toggle("is-on", _orderView === "bw");
    } else {
      pair.hidden = true;
    }
  }
  updateOrder();
}
$("#pairCharmT").addEventListener("click", () => { _orderView = "charm"; refreshApproveStage(); });
$("#pairBWT").addEventListener("click", () => { _orderView = "bw"; refreshApproveStage(); });
function approveNow() {
  state.approved = true;
  _orderView = "charm";
  pushMsg({ role: "studio", text: `Version ${state.versions[state.currentVersion] ? state.versions[state.currentVersion].n : ""} approved ♥ — press Next to choose how to wear it.` });
  renderStage(); renderThread();
  ensureSession();
  logEvent("approve", "Design approved ♥", state.versions[state.currentVersion] && state.versions[state.currentVersion].n);
  saveSession();
  refreshApproveStage();
  updateNavArrows();
  $("#navNext").focus();
  toast("Design approved — press Next to order it ♥", "gold");
}
/* ═════════════════════════════════════════════════════════════════════════
   LEAVING A PROJECT, AND STARTING A NEW ONE

   Four doors lead out of a design — the three "Start a design" rows in the
   menu, Start over, opening something out of My designs, and deleting the
   one you are in — and every one of them used to be a different amount of
   careful. The shortcuts were the worst of them: they navigated back to step
   1 and CARRIED THE WHOLE DESIGN WITH THEM, so "Your Custom Design" reopened
   the sketch that was already on the sheet instead of handing over a blank
   one.

   They all go through the same three-part move now:

     commit   — whatever a modal is holding goes back into the session, and
                the brief typed but never sent goes with it
     freeze   — the epoch moves, so everything still in the air files itself
                on the design it was started in and can never touch the new
                one; overlays and busy flags come down
     blank    — every per-design field, and the module-level scratch that is
                keyed by version number, goes back to nothing

   The write of the outgoing design is deliberately NOT awaited: it is three
   attempts and then localStorage, and the customer should not be made to
   watch it. What they must never see is a blank studio that turned out to
   have eaten the last thing they drew, which is what the awaited version
   risked whenever the network was slow. */

/* Anything a modal is holding that is not yet on the session. */
function commitOpenEditors() {
  /* the sketchpad keeps its own live copy of the drawing; closeMarkup is
     what puts it back (mkCloseEditor → mkPersist → saveSession) */
  try { if (__mk) closeMarkup(); } catch (e) { SD("leave: closeMarkup threw", e && e.message); }
  try { mkCloseLibZoom(); } catch (e) {}
  try { const lp = $("#libPickModal"); if (lp) lp.classList.remove("is-open"); } catch (e) {}
}

/* The outgoing design, written with everything it is owed. Not awaited by
   its caller — it retries, then shelves, and says so if it cannot.

   ── IT TAKES THE SESSION, NOT A DOCUMENT ──────────────────────────────────
   The first draft of this captured the document at the moment of the switch
   and re-sent that same object on every retry. That is a clobber waiting to
   happen: a generation finishing a second later files itself on this session
   and writes it, and a retry carrying the pre-switch snapshot then lands on
   top and takes the new version straight back out again — merge replaces an
   array field whole. Building the document at write time cannot lose that
   race, because `s` is the object the orphan wrote into. */
async function persistLeaving(s, name, quiet) {
  if (!s) return true;
  const ok = await writeSessionDocHard(s);
  if (!ok) {
    try {
      toast(`“${name}” couldn't reach the server just now — it's saved on this ` +
            `device and will sync itself as soon as it can.`, "err");
    } catch (e) {}
    return false;
  }
  if (!quiet) { try { toast(`“${name}” is saved in My designs ✦`, "gold"); } catch (e) {} }
  return true;
}

/* Nothing has happened in this design yet. ensureSession() mints a session
   the moment the sketchpad opens, so tapping "Your Custom Design" and then
   changing your mind to "From your photo" would otherwise file an empty
   "New design" in My designs every time. An untouched session has never been
   written to Firestore, so dropping it here is enough — there is nothing to
   delete. */
function sessionIsBlank(s) {
  if (!s) return true;
  if (s.reference) return false;
  if ((s.versions || []).length || (s.thread || []).length) return false;
  if (String(s.desc || "").trim() || String(s.draftText || "").trim()) return false;
  if ((s.assets || []).length || (s.signatures || []).length) return false;
  const m = s.markups || {};
  return !Object.keys(m).some((k) => {
    const e = m[k] || {};
    return (e.items || []).length || e.crop || e.baseRot || e.mode === "exact";
  });
}

/* Save the open design, then freeze everything still working on it. Returns
   its name for the toast. Everything from `saveSession()` to `projectEpoch++`
   is synchronous ON PURPOSE: a generation landing in a gap between the
   snapshot and the freeze would be filed by neither path. */
function leaveCurrentProject(opts) {
  const quiet = !!(opts && opts.quiet);
  const keep = !(opts && opts.keep === false);
  commitOpenEditors();

  const leaving = activeSession();
  let name = "", worth = false;
  if (leaving && keep) {
    saveSession();                        // state → session object, in full
    name = leaving.name || designName();
    worth = !sessionIsBlank(leaving);
    /* a session nobody ever put anything into is not "lost work", it is a
       row in My designs that should never have existed */
    if (!worth) state.sessions = (state.sessions || []).filter((x) => x.id !== leaving.id);
  }
  /* the debounce is now redundant at best and dangerous at worst: it fires
     on whatever is active when the timer comes up */
  _savePending = false;
  clearTimeout(_saveTimer);

  /* ── THE FREEZE ────────────────────────────────────────────────────────
     One line, and it is the line the whole file hangs off. */
  projectEpoch++;

  abortLiveProgress();
  genBusy = false;
  renderBusy = false;
  try { uploadBusy = false; } catch (e) {}
  try { composeBusy = false; } catch (e) {}
  try { refEditBusy = false; } catch (e) {}
  const sb = $("#sendBtn"); if (sb) sb.disabled = false;
  const rb = $("#renderBtn"); if (rb) rb.disabled = false;

  if (leaving && worth) {
    persistLeaving(leaving, name, quiet).catch((e) => {
      console.warn("[studio] leaving save:", e && e.message);
    });
  }
  return name;
}

/* Every per-design field back to nothing. The list is exhaustive on purpose:
   a field left behind is a field that bleeds into the next design. */
function resetProjectState() {
  state.activeSessionId = null;
  state.designing = true;
  state.step = 1;
  state.maxStep = 1;
  state.startMode = null;
  state.reference = null;
  state.metal = "gold";                   // what ensureSession gives a new one
  state.renderQuality = "low";            // never inherit a 2-credit setting
  state.promptMode = "full";              // a diagnostic never leaks into a new design
  state.desc = "";
  state.versions = [];
  state.thread = [];
  state.currentVersion = -1;
  state.approved = false;
  state.stageView = "bw";
  state.markups = {};
  state.mkHistory = {};
  state.assets = [];
  state.refPlan = [];
  state.refPlanKey = "";
  state.toldAboutFill = false;
  state.metalCoach = 0;
  state.metalSeen = {};
  state.metalAsked = {};
  state.activeTag = null;
  /* SIGNATURES STAY, and this is the one deliberate exception. A hand
     signature is the customer's own hand, reusable by design — the same
     reasoning that makes the image library account-wide rather than
     per-design. One line to change if it should travel no further. */
  state.signatures = state.signatures || [];

  /* ── MODULE SCRATCH KEYED BY VERSION NUMBER ────────────────────────────
     MK_TRACED is the sharp one. It holds "3" from the design just left, so
     version 3 of the NEW design would be treated as already taken apart and
     would open with no layers at all — a bug that would have looked like
     the tracer failing at random. */
  try { MK_TRACED.clear(); } catch (e) {}
  try { __mkClip = null; } catch (e) {}
  try { __mkPrev = { key: "", frame: 0, at: null, cv: null }; } catch (e) {}
  try { mkRegionForget(); } catch (e) {}
  try { _gone.markups.clear(); _gone.mkHistory.clear(); _gone.sid = ""; } catch (e) {}
  try { _orderView = "charm"; } catch (e) {}
  try {
    orderState.metal = "silver"; orderState.qty = 1; orderState.mm = DEFAULT_MM;
  } catch (e) {}

  /* ── AND THE SCREEN ────────────────────────────────────────────────────*/
  const dI = $("#descInput"); if (dI) dI.value = "";
  const cc = $("#charCount"); if (cc) cc.textContent = "0";
  const vs = $("#versionStrip"); if (vs) vs.innerHTML = "";
  const se = $("#stageEmpty"); if (se) se.style.display = "";
  const sf = $("#stageFlag"); if (sf) sf.classList.remove("is-visible");
  const op = $("#orderPair"); if (op) op.hidden = true;
  const et = $("#engraveToggle"); if (et) et.checked = false;
  const ei = $("#engraveInput"); if (ei) { ei.value = ""; ei.hidden = true; }
  const ex = $("#extToggle"); if (ex) ex.checked = false;
  const ht = $("#heartToggle"); if (ht) ht.checked = false;
  const ov = $("#genOverlay"); if (ov) ov.classList.remove("is-visible");
  ["#markupModal", "#libPickModal", "#sessionModal"].forEach((sel) => {
    const el = $(sel); if (el) el.classList.remove("is-open");
  });
  const dd = $("#designsDrawer"); if (dd) dd.classList.remove("is-open");
  try { fileInput.value = ""; } catch (e) {}

  renderRefStrip();
  renderStage();
  renderThread();
  try { resetUploadStage(true); } catch (e) {}
  try { autoGrow(); } catch (e) {}
  setStartMode(null);
  gotoStep(1, true);
  updateNavArrows();
}

/* Back to a blank studio. `keep` false is the delete case: the design being
   left behind is gone, so it must NOT be saved on the way out — but its work
   in flight still has to be frozen, or a generation lands in the blank
   studio that replaces it. */
function startOverStudio(keep) {
  leaveCurrentProject({ keep: keep !== false, quiet: keep === false });
  resetProjectState();
}
$("#startOverBtn").addEventListener("click", () => { startOverStudio(true); });

/* ── THE THREE SHORTCUTS ──────────────────────────────────────────────────
   Wholly synchronous, so there is no window in which the studio is half in
   one design and half in the next. The outgoing write runs behind it. */
let _switchBusy = false;
function startNewProject(mode) {
  if (_switchBusy) { SD("start: ignored — a switch is already running"); return; }
  _switchBusy = true;
  try {
    SD("start: NEW project ←", mode);
    closeMenu(false);
    leaveCurrentProject();
    resetProjectState();
    setStartMode(mode);
    /* the sketchpad opens on a blank sheet, because state.markups.draw was
       just cleared and openCompose loads from it */
    if (mode === "draw") openCompose();
    SD("start: ready — epoch", epochNow(), "session", state.activeSessionId || "(none yet)");
  } catch (e) {
    console.warn("[studio] start a design:", e && (e.stack || e.message));
  } finally {
    _switchBusy = false;
  }
}

/* A drawing or a render that arrived after the customer had moved on. It is
   filed on the session object it was made for, in the shape the live path
   would have used, and that document is written. Nothing here reads or
   writes `state` — that is the entire point. */
function orphanThread(s, msg, vn) {
  const t = (s.thread || []).slice();
  const last = t[t.length - 1];
  /* a thinking bubble frozen mid-thought is what a resumed design would
     otherwise show for ever */
  if (last && last.role === "thinking") {
    t[t.length - 1] = { role: "studio", vn: vn || null, text: msg, at: now() };
  } else {
    t.push({ role: "studio", vn: vn || null, text: msg, at: now() });
  }
  s.thread = t;
}
function orphanVersion(s, v, msg, evt) {
  if (!s || !v) return Promise.resolve(false);
  s.versions = (s.versions || []).concat([Object.assign({}, v)]);
  s.currentVersion = s.versions.length - 1;
  s.approved = false;
  orphanThread(s, msg || "Here's the new version.", v.n);
  s.history = (s.history || []).concat([
    { type: evt || "generate", label: "Finished after you started something else", vn: v.n, at: now() },
  ]);
  if (s.status !== "ordered") s.status = "draft";
  return saveOrphan(s);
}
function orphanFail(s, msg) {
  if (!s) return Promise.resolve(false);
  orphanThread(s, msg, null);
  return saveOrphan(s);
}

/* =============================================================================
   RENDER — custom_charm_render: the approved drawing becomes the charm.
   The Charm Maker's gold→line-art conversion run in reverse: the drawing is
   the structural truth, the output is the photograph. One credit, debited
   server-side exactly like a generation and refunded on failure. Progress
   streams through the SAME version doc the drawing lives on — renderStatus /
   renderStage / renderURL — so the listener plumbing is watchVersion's,
   pointed at the render fields.
   ========================================================================== */
function watchRender(sessionId, n, prog, renderRunId) {
  let settle = null, unsub = null, poll = null, settled = false;
  SD("render: watching", sessionId + "/versions/" + n, "runId=" + renderRunId);
  /* Elapsed seconds since the click, on every line. Render time is dominated
     by one upstream 2048² model call and varies by a factor of three or more
     between runs; without a clock on the trace there is no way to tell a slow
     model from a stalled pipeline. */
  const t0 = Date.now();
  const secs = () => Math.round((Date.now() - t0) / 100) / 10 + "s";
  let lastStage = "", lastSnapAt = 0, warnedRunId = false;
  const done = new Promise((resolve) => { settle = resolve; });
  const finish = (v) => {
    if (settled) return;
    settled = true;
    SD("render: watcher settled", v ? (v.ok ? "OK" : "FAIL " + (v.error || "")) : "cancelled",
       "after", secs());
    if (unsub) { try { unsub(); } catch (e) {} }
    if (poll) clearInterval(poll);
    settle(v);
  };
  const apply = (d, src) => {
    /* A poll already in flight when finish() ran still lands afterwards, and
       painted a stage onto a progress bar that had been handed off. */
    if (settled || !d) return;
    /* A version document keeps the result of its previous metal render. A
       watcher opened for a NEW click must ignore every old snapshot until
       the backend stamps this attempt's run id onto the document. URL/path
       comparison cannot do this because every re-render intentionally
       overwrites the same vN-charm.png path. */
    if (String(d.renderRunId || "") !== String(renderRunId || "")) {
      /* Loud on purpose, but ONCE. A doc that never carries this run's id is
         the exact shape of "the background function was never invoked": the
         watcher sits there ignoring a stale done from the previous render and
         the customer waits out the full timeout for nothing. Repeating it on
         every poll only buries the line that matters. */
      if (!warnedRunId) {
        warnedRunId = true;
        SD("render: ignoring stale doc — runId", JSON.stringify(String(d.renderRunId || "")),
           "!= this run", JSON.stringify(String(renderRunId || "")),
           "(status=" + (d.renderStatus || "none") + ")");
      }
      return;
    }
    /* ── A STAGE IS A TRANSITION, NOT A HEARTBEAT ──────────────────────────
       The poll re-reads the same document every five seconds, so an unchanged
       stage arrived over and over: the console filled with "stage rendering"
       and prog.stage() re-entered the same step, restarting its animation each
       time. Only movement is movement. */
    if (d.renderStage && d.renderStage !== lastStage) {
      lastStage = d.renderStage;
      SD("render: stage", d.renderStage, "(" + src + ", " + secs() + ")");
      prog.stage(d.renderStage);
    }
    if (d.renderStatus === "done" && d.renderURL) {
      finish({ ok: true, n, renderURL: d.renderURL, renderPath: d.renderPath,
               renderSpecURL: d.renderSpecURL || "", renderSpecPath: d.renderSpecPath || "",
               renderMetal: d.renderMetal, renderQuality: d.renderQuality,
               renderModel: d.renderModel, renderCost: d.renderCost,
               renderRunId: d.renderRunId });
    }
    if (d.renderStatus === "failed") finish({ ok: false, error: d.renderError || "render_failed" });
  };
  try {
    unsub = db.collection("customSessions").doc(sessionId).collection("versions").doc(String(n))
      .onSnapshot((snap) => { lastSnapAt = Date.now(); if (snap.exists) apply(snap.data(), "snapshot"); },
                  (e) => { SD("render: onSnapshot ERROR —", e && (e.code || e.message),
                              "(the 5s poll carries it)"); });
  } catch (e) { SD("render: could not open onSnapshot —", e && e.message); }
  /* ── THE POLL IS A FALLBACK, NOT A SECOND CHANNEL ──────────────────────
     It exists for browsers where the Firestore listener cannot open at all.
     Running it unconditionally meant every render also spent one function
     invocation and one Firestore read every five seconds for its whole
     length, alongside a listener that was already delivering. It now stands
     down while snapshots are arriving and picks up the moment they stop. */
  poll = setInterval(async () => {
    if (Date.now() - lastSnapAt < 15000) return;
    try {
      const r = await postAuthed(IMAGE_FN, { kind: "custom_session_status", sessionId, versionNumber: n }, FN_BASE_IMAGE);
      if (r && r.version) apply(r.version, "poll");
      else SD("render: poll — no version doc yet (" + secs() + ")");
    } catch (e) { SD("render: poll failed —", (e && e.status) || "", (e && e.message) || ""); }
  }, 5000);
  return { done, finish, cancel: () => finish(null) };
}

async function runRender() {
  SD("render: clicked");
  if (genBusy || renderBusy) {
    SD("render: BLOCKED — genBusy=" + genBusy + " renderBusy=" + renderBusy);
    return;
  }
  const v = state.versions[state.currentVersion];
  if (!v) {
    SD("render: BLOCKED — no current version (versions=" + (state.versions || []).length +
       " currentVersion=" + state.currentVersion + ")");
    return;
  }
  const cost = renderCost();
  const quality = renderQuality();
  const pMode = promptMode();
  if (!state.unlimited && state.credits < cost) {
    SD("render: BLOCKED — credits=" + state.credits + " cost=" + cost + " quality=" + quality);
    toast(cost > 1
      ? `High-quality rendering costs ${creditWord(cost)} — you have ${creditWord(state.credits)}. Switch to Standard, or top up.`
      : "You're out of design credits — top up and we'll cut this straight away.", "err");
    openPricing("out"); return;
  }
  const sess = ensureSession();
  /* which project this belongs to, read once, before anything can await */
  const ep = epochNow();
  SD("render: v" + v.n, "metal=" + state.metal, "quality=" + quality, "prompt=" + pMode, "cost=" + cost,
     "session=" + sess.id, "signedIn=" + !!state.user);
  await flushSession();                       // the server reads the session doc
  if (!sameProject(ep)) return;               // left before it even started

  renderBusy = true;
  $("#renderBtn").disabled = true;
  const prog = genProgress(RENDER_STAGES, RENDER_INDEX);
  prog.start();
  const renderRunId = "rr_" + Date.now().toString(36) + "_" + Math.random().toString(36).slice(2, 10);
  const watch = watchRender(sess.id, v.n, prog, renderRunId);
  const timer = setTimeout(() => watch.finish({ ok: false, error: "timeout" }), GEN_TIMEOUT_MS);

  let ack = null, error = null;
  try {
    /* THE SAME COUNT, ONE STEP LATER. The render reads the approved B/W
       drawing and turns it into metal — and "is that black area engraved or
       is it a hole?" is a question about the drawing's ink, which is exactly
       the question a picture cannot answer on its own. Whatever the customer
       declared, on the sketch or on the mark-up, travels here too, so the
       render is checked against a count rather than an impression. */
    /* ONE PLAN, ASKED FOR IN ONE PLACE. This used to assemble its own answer
       and could only ever reach two of the four sources, so a design begun
       from an uploaded picture rendered with no account of its holes at all
       and a refined one lost the account somewhere around version three. */
    let zones = [];
    try { zones = mkFillPlan(v); } catch (e) { zones = []; }
    SD("render: queueing → POST", `${FN_BASE_IMAGE}/${IMAGE_FN}`, "zones=" + zones.length);
    ack = await postAuthed(IMAGE_FN, {
      kind: "custom_charm_render",
      sessionId: sess.id,
      versionNumber: v.n,
      metal: state.metal,
      quality,
      promptMode: promptMode(),
      zones,
      renderRunId,
    }, FN_BASE_IMAGE);
    SD("render: queue answered", JSON.stringify(ack || null).slice(0, 300));
  } catch (e) {
    error = e;
    SD("render: queue THREW", (e && e.status) || "", (e && e.message) || "",
       e && e.payload ? JSON.stringify(e.payload).slice(0, 200) : "");
  }
  /* ── A REQUEST THAT NEVER LANDED CANNOT ARRIVE LATER ──────────────────────
     Only 401/402/429 used to settle the watcher, so every OTHER failure — a
     502 queue_failed from the kick, a 500, a dropped connection — left the
     browser watching a version doc that nobody was ever going to write, for
     the full three-minute timeout, with an empty console. A throw here means
     the work was never queued, whatever the status: say so at once. */
  if (error) {
    watch.finish({ ok: false, error: String(error.status || error.message || "queue_failed") });
  }

  const result = (ack && ack.ok && ack.renderURL &&
                  String(ack.renderRunId || "") === renderRunId) ? ack : await watch.done;
  clearTimeout(timer);
  watch.cancel();
  /* renderBusy belongs to whatever project is open now — see the note in
     runComposedGeneration's finally */
  if (sameProject(ep)) renderBusy = false;

  if (!result || !result.ok) {
    prog.abort();
    const why = (error && error.status) || (result && result.error) || "";
    SD("render: FAILED —", String(why) || "(no reason given)");
    if (!sameProject(ep)) {
      await orphanFail(sess, "That render didn't come through. Nothing was charged — try it again.");
      return;
    }
    /* renderStage() repaints the controls, but only when a version is on the
       stage. Clear the flag on the button itself so a failure can never leave
       Re-render dead. */
    const rb = $("#renderBtn"); if (rb) rb.disabled = false;
    const msg = String(why) === "402" || String(why).includes("out_of_credits")
      ? `That render costs ${creditWord(cost)} and your balance didn't cover it — top up, or switch to Standard.`
      : String(why) === "401"
        ? "Your session timed out — sign in again and we'll carry on."
        : String(why) === "429" || String(why).includes("rate_limited")
          ? "That's a lot of metal in one go — give it a minute and try again."
          /* The server refuses BEFORE debiting when a renderer's key is
             missing, so this is the one failure with a real remedy the
             customer can act on themselves. */
          : String(why).includes("high_unavailable")
            ? "High-quality rendering isn't available right now — Standard is, and it costs less."
            : String(why).includes("render_unavailable")
              ? "The renderer isn't available right now. Nothing was charged — try again shortly."
              : "That render didn't come through. Nothing was charged — try it again.";
    if (String(why) === "402") openPricing("out");
    else if (String(why) === "401") openAuth("default");
    else toast(msg, "err");
    refreshWallet();
    renderStage();
    return;
  }

  SD("render: DONE v" + v.n, result.renderMetal || state.metal,
     "quality=" + (result.renderQuality || quality),
     "model=" + (result.renderModel || "?"),
     String(result.renderURL || result.renderUrl || "").slice(0, 120));
  await prog.finish();
  /* ── THE METAL ARRIVED AFTER THEY MOVED ON ────────────────────────────
     `v` is a version OBJECT out of the design that has been left: mutating
     it here would write into a copy nothing saves, and the render — paid
     for, finished, sitting in Storage — would simply never appear on the
     design it belongs to. So the fields go onto that session's own copy of
     the version and the document is written. */
  if (!sameProject(ep)) {
    const ov = (sess.versions || []).find((x) => x && x.n === v.n);
    if (ov) {
      ov.renderUrl = result.renderURL || result.renderUrl;
      ov.renderPath = result.renderPath || "";
      ov.renderSpecUrl = result.renderSpecURL || result.renderSpecUrl || "";
      ov.renderSpecPath = result.renderSpecPath || "";
      ov.renderMetal = result.renderMetal || sess.metal || "gold";
      ov.renderQuality = result.renderQuality || quality;
      ov.renderModel = result.renderModel || "";
      ov.renderCost = Number(result.renderCost) || cost;
      ov.renderRunId = result.renderRunId || renderRunId;
      sess.history = (sess.history || []).concat([{ type: "render",
        label: "Rendered after you started something else", vn: v.n, at: now() }]);
      orphanThread(sess, "Here it is in metal — your drawing, made real.", v.n);
    }
    await saveOrphan(sess);
    return;
  }
  spendCredit();
  v.renderUrl = result.renderURL || result.renderUrl;
  v.renderPath = result.renderPath || "";
  v.renderSpecUrl = result.renderSpecURL || result.renderSpecUrl || "";
  v.renderSpecPath = result.renderSpecPath || "";
  v.renderMetal = result.renderMetal || state.metal;
  /* Which renderer made THIS picture, kept on the version rather than read
     back off the live toggle: the customer can switch quality afterwards and
     the record of what they are looking at must not switch with it. */
  v.renderQuality = result.renderQuality || quality;
  v.renderModel = result.renderModel || "";
  v.renderCost = Number(result.renderCost) || cost;
  v.renderRunId = result.renderRunId || renderRunId;
  state.stageView = "charm";
  logEvent("render", `Rendered in ${METAL_LABELS[state.metal] || state.metal}` +
                     (v.renderQuality === "high" ? " — high quality" : ""), v.n);
  pushMsg({ role: "studio", vn: v.n, text: "Here it is in metal — your drawing, made real. Approve the pair when you love it." });
  saveSession();
  renderStage();
  refreshWallet();
}


/* =============================================================================
   THE DRAWING STUDIO
   One surface, two jobs, one engine.

     · MARK UP  — draw on top of a production drawing to direct the next
                  revision. The drawing is the base layer.
     · COMPOSE  — a blank sheet the customer draws their charm on from
                  nothing, submitted afterwards as the reference image by the
                  same route an upload takes.

   ── WHY IT IS BUILT THIS WAY ──────────────────────────────────────────────
   The previous version kept two parallel models — canvas strokes and DOM
   contenteditable bubbles — and harvested the DOM into the model at commit
   time. Every note that was lost was lost in that harvest: a note typed and
   never blurred, a re-render that detached a live editor, a close path that
   ran before the harvest. So the harvest is gone. There is now:

     · ONE model — a flat array of items, every one of them plain data;
     · ONE painter — mkPaint, used for the screen AND for the composite the
       designer receives, so what you see is exactly what is sent;
     · ONE editor — a single <textarea>, whose value goes into the model on
       every keystroke;
     · and mkTouch() after EVERY mutation, which repaints and writes the
       session. There is no commit step to get wrong, and no state that lives
       only in the DOM. Closing, crashing or navigating away cannot lose a
       note that was typed a moment earlier.

   Geometry is stored NORMALIZED (0..1 against the square stage) and FLAT —
   Firestore rejects an array inside an array outright, and one nested array
   once made the whole session write fail silently.
   ========================================================================== */

/* ── vocabulary ──────────────────────────────────────────────────────────── */
const MK_PATH_TOOLS  = { draw: "ink", highlight: "hl", sign: "sign" };
const MK_SHAPE_TOOLS = ["line", "arrow", "rect", "round", "ellipse", "ring", "tri", "star", "heart", "bail"];
const MK_TEXT_TOOLS  = ["text", "callout", "label"];
const MK_SHAPE_LABEL = {
  line: "Line", arrow: "Arrow", rect: "Box", round: "Rounded", ellipse: "Circle",
  ring: "Ring", tri: "Triangle", star: "Star", heart: "Heart", bail: "Charm",
};
const MK_ALPHA       = { hl: 0.3 };

/* ═══════════ WHAT AN AREA BECOMES IN METAL ═══════════════════════════════
   Fill is not decoration and never was. It is the one instruction in the
   whole studio that changes what the workshop DOES to the piece, and it has
   exactly three answers:

     ENGRAVE      black   — the area is engraved or hatched. Metal stays;
                            the surface is cut into.
     CUT OUT      blue    — the metal is removed. A real hole through the
                            charm, with the world visible through it.
     OUTLINE ONLY no fill — only the boundary line is engraved. The interior
                            is left as flat polished metal.

   It used to be a twelve-colour palette where every colour meant "engrave",
   which asked one control to do two jobs — pick a colour AND state an
   intention — and left the second one invisible. Now the control states the
   intention and the colour follows from it, so a fill can only ever mean one
   of three things and the drawing says which at a glance.

   BLUE IS RESERVED. It is gone from the ink palette, so blue appears in a
   Brites drawing for exactly one reason and the workshop — and the
   designer — can never misread it. */
/* ── CHOSEN FOR A MACHINE'S EYE, NOT A DESIGNER'S ─────────────────────────
   These three are read back by an image model whose vision encoder downsamples
   hard, and saturation is the first thing that pass destroys — worst on thin
   marks, which blend toward the paper until the hue is nearly gone. The old
   trio failed exactly there: blue sat at L*48 and red at L*47, so with hue
   degraded a cut-out and an outline were the SAME SHADE, and blue was close
   enough to black to be read as engraving.

   The replacements are spaced on BOTH axes, so either one alone is enough to
   tell them apart:
     lightness ladder  black 0  ->  red 47  ->  blue 69  ->  paper 100
     worst-case separation between any two inks, measured at the thinness of a
     hairline, improves from dE 24.5 to 30.5.
   Change one of these and re-check the ladder; the gaps are the point. */
const MK_ENGRAVE = "#000000";
const MK_CUTOUT  = "#00b4ff";
/* ── AND THE THIRD ONE HAS A COLOUR NOW ───────────────────────────────────
   Outline used to be the absence of a colour, which is exactly what made it
   invisible: two of the three instructions announced themselves on the sheet
   and the third looked like nothing had been said at all. So the boundary of
   an outline-only area is drawn in a reserved RED, and the swatch on the bar
   is that same red. Three instructions, three colours, none of them
   guessable for anything else.

   RED IS RESERVED exactly as blue is — it is not in the ink palette and the
   pen does not draw in it, so red appears in a Brites drawing for one reason
   only. The STORED value is still "none": every unfilled shape, every line
   and every note in every design ever saved carries f:"none", and a colour
   is what that instruction LOOKS like, never what it is. */
const MK_OUTLINE = "#e00000";
const MK_FILL_MODES = [
  { id: "engrave", f: MK_ENGRAVE, label: "Engrave",
    say: "This area is engraved — cut into the surface, hatched solid. The metal stays." },
  { id: "cutout",  f: MK_CUTOUT,  label: "Cut out",
    say: "This area is cut clean through. A real hole in the charm — you can see through it." },
  { id: "none",    f: "none",     label: "Outline",
    say: "Only the outline is engraved, and it is drawn in red. Inside stays flat polished metal, untouched." },
];
/* Legacy is safe by construction: before this, EVERY fill meant engrave, and
   the old palette never held this blue. So anything that is not exactly the
   reserved blue was an engrave then and stays an engrave now. */
function mkNormFill(v) {
  const s = String(v == null ? "none" : v).trim().toLowerCase();
  if (!s || s === "none" || s === "transparent") return "none";
  return s === MK_CUTOUT ? MK_CUTOUT : MK_ENGRAVE;
}
function mkFillIntent(f) {
  const n = mkNormFill(f);
  return n === "none" ? "none" : (n === MK_CUTOUT ? "cutout" : "engrave");
}
/* ── WHAT THIS PARTICULAR OBJECT'S INSTRUCTION IS ─────────────────────────
   Nearly always just its fill. The exception is written into the studio's
   own vocabulary: the Ring and the Charm+bail tools are described in the
   toolbar as holes ("Drag a ring — the hole reads as a cut-out"), and the
   payload has promoted an unfilled one to `cutout` for a long time. The
   PICTURE never learned that, so the sheet drew an outline while the list
   beside it said "cut clean through". One reading, used by the painter, the
   tally and the payload alike, so they cannot disagree again. */
function mkItemIntent(it) {
  const i = mkFillIntent(it && it.f);
  if (i === "none" && it && (it.t === "ring" || it.t === "bail")) return "cutout";
  return i;
}
/* ONE PLACE decides what each of the three instructions looks like, so the
   sheet, the layer thumbnails, the composite the designer receives and the
   swatch on the bar can never drift apart. */
function mkIntentInk(intent) {
  return intent === "cutout" ? MK_CUTOUT : intent === "none" ? MK_OUTLINE : MK_ENGRAVE;
}
/* ── AND THE OTHER DIRECTION ──────────────────────────────────────────────
   Artwork arrives already coloured — an .svg path, a Photoshop shape layer,
   a photograph. Every one of those colours used to be flattened to ENGRAVE
   on the way in, which is why a design drawn with blue cut-outs came back
   with twenty-four layers and twenty-four of them engraved: the instruction
   was thrown away at the door and there was nothing downstream could do
   about it. The reserved blue and the reserved red are read as what they
   are; everything else is ink, and ink is engraved metal. */
function mkPixelIntent(r, g, b) {
  const lum = 0.299 * r + 0.587 * g + 0.114 * b;
  if (b - Math.max(r, g) > 38 && b > 80) return "cutout";
  if (r - Math.max(g, b) > 55 && r > 95 && lum < 210) return "none";
  return lum < 150 ? "engrave" : "";
}
function mkIntentFromColour(c) {
  try {
    let r, g, b;
    if (Array.isArray(c) && c.length >= 3) {
      const mx = Math.max(Number(c[0]) || 0, Number(c[1]) || 0, Number(c[2]) || 0);
      const k = mx <= 1.001 ? 255 : 1;
      r = (Number(c[0]) || 0) * k; g = (Number(c[1]) || 0) * k; b = (Number(c[2]) || 0) * k;
    } else if (c && typeof c === "object" && ("r" in c || "g" in c || "b" in c)) {
      r = Number(c.r) || 0; g = Number(c.g) || 0; b = Number(c.b) || 0;
    } else {
      const h = String(c || "").trim().replace(/^#/, "");
      if (!/^[0-9a-f]{6}$/i.test(h)) return "engrave";
      r = parseInt(h.slice(0, 2), 16); g = parseInt(h.slice(2, 4), 16); b = parseInt(h.slice(4, 6), 16);
    }
    return mkPixelIntent(r, g, b) || "engrave";
  } catch (e) { return "engrave"; }
}
/* the fill instruction currently showing on the bar: the selection's if
   there is one, otherwise the one the next mark will carry */
function sel0f() {
  if (!__mk) return "none";
  const s0 = __mk.items[__mk.sel];
  return s0 ? s0.f : __mk.fill;
}
function mkFillMode(id) {
  return MK_FILL_MODES.filter((m) => m.id === id)[0] || MK_FILL_MODES[2];
}
/* how many areas carry each instruction — the tally the footer shows and the
   payload sends, computed in one place so they can never disagree */
function mkMetalTally(items) {
  const t = { engrave: 0, cutout: 0, none: 0 };
  (items || []).forEach((it) => {
    if (it.hid) return;
    if (it.t === "img" || mkIsText(it) || it.t === "ink" || it.t === "hl" || it.t === "sign") return;
    t[mkItemIntent(it)]++;
  });
  return t;
}

/* The ink palette is gone: colour never meant anything on a line — it was
   only ever there so a customer could tell two of their own objects apart,
   and it cost more confusion with the engraving colours than it was worth. */
/* ONE face. The workshop engraves a single clean sans, so offering four in
   the UI was a promise the metal could not keep — a legacy item that carries
   another name is coerced back to sans in mkPack. */
const MK_FONTS = {
  sans: '"Nunito Sans", system-ui, sans-serif',
};
const MK_HINTS = {
  select:    "Click a mark to move, resize or rotate it · ⌫ deletes",
  draw:      "Click and drag to draw · scroll to zoom",
  highlight: "Drag a wide highlight over an area",
  sign:      "Sign across the sheet — we'll keep it for next time",
  text:      "Click where the note should point, then type",
  callout:   "Click the spot, then drag the note away from it",
  label:     "Click where the lettering goes, then type",
  bucket:    "Move over the sheet — the area lights up before you commit. Click to take it, click again to undo",
  wand:      "Click the background of a picture and it falls away to the object's edge",
  erase:     "Drag over any part you want to remove — adjust the eraser size below",
  pan:       "Drag to move around · scroll to zoom",
  line:      "Drag a line · hold shift to keep it straight",
  arrow:     "Drag from anywhere to the thing you mean",
  rect:      "Drag a box · hold shift for a square",
  round:     "Drag a rounded box · hold shift for a square",
  ellipse:   "Drag an oval · hold shift for a circle",
  ring:      "Drag a ring — the hole reads as a cut-out",
  tri:       "Drag a triangle",
  star:      "Drag a star",
  heart:     "Drag a heart",
  bail:      "Drag the charm — the loop it hangs from is drawn on top",
};

let __mk = null;                              /* the open editor's whole world */

/* ── what counts as marked ───────────────────────────────────────────────── */
function mkItemsOf(m) {
  if (!m) return [];
  if (Array.isArray(m.items)) return m.items;
  /* legacy shape: separate stroke and bubble arrays */
  const out = [];
  (m.strokes || []).forEach((st) => {
    const p = [];
    if (Array.isArray(st.p)) st.p.forEach((n) => p.push(Number(n) || 0));
    else if (Array.isArray(st.pts)) st.pts.forEach((pt) => { p.push(pt[0], pt[1]); });
    out.push({ t: st.tool === "highlight" ? "hl" : "ink", c: st.color, w: st.w, p });
  });
  (m.bubbles || []).forEach((b) => {
    out.push({ t: "text", p: [Number(b.x) || 0, Number(b.y) || 0], s: String(b.text || "") });
  });
  return out;
}
function markupHasContent(m) {
  return mkItemsOf(m).length > 0;
}

/* ── the storage shape: flat, typed, and never undefined ─────────────────
   Every field is written on every save, so a document can never come back
   with a hole in it. Layer state (name, opacity, hidden, locked) lives on
   the item because a layer IS an item — there is no second list to keep in
   step, and therefore no way for the two to disagree. */
function mkPack(it) {
  const o = it.o == null ? 1 : Number(it.o);
  return {
    id: String(it.id || mkId()),
    t:  String(it.t || "ink"),
    c:  String(it.c || MK_ENGRAVE),
    /* three states, never twelve: the invariant is enforced HERE, so it
       holds for the sheet, the layer list, the export and the payload
       alike, and a design made before this build cannot change meaning. */
    f:  mkNormFill(it.f),
    w:  Number(it.w) || 0.008,
    p:  (it.p || []).map((n) => Math.round((Number(n) || 0) * 1e4) / 1e4),
    s:  String(it.s || "").slice(0, 240),
    ff: "sans",                                    // the one face we engrave
    fs: Number(it.fs) || 0.036,
    r:  Number(it.r) || 0,
    /* ── layer state ── */
    n:   String(it.n || "").slice(0, 48),          // the customer's own name
    o:   isFinite(o) ? Math.min(1, Math.max(0, o)) : 1,
    hid: it.hid ? 1 : 0,
    lok: it.lok ? 1 : 0,
    /* which import a layer arrived with, so a merged group stays identifiable */
    g:  String(it.g || "").slice(0, 12),
    /* ── flood fills: the lengths of each traced contour, flat ── */
    q:  (it.q || []).map((n) => Math.max(0, Math.round(Number(n) || 0))),
    /* WHICH ENGINE DREW THIS FILL. A fill made before the mask was grown
       back to the true edge relied on a fat stroke to reach its own walls,
       and it stored one; a fill made since needs no stroke at all. Guessing
       from the stroke WIDTH cannot work, because Weight edits that width and
       a fill is selected the moment it lands — so nudging Weight to Heavy on
       a new fill would make it look like an old one and paint the rope back.
       One flag, written once, and the question stops being a guess. */
    gr: it.gr ? 1 : 0,
    /* WHICH FILLS THE BUCKET MADE. A bucket fill is an instruction the
       customer added to a drawing, and taking it back means removing it. A
       filled shape that arrived in an .svg, or came out of taking a
       generated drawing apart, is not an instruction — it is the ARTWORK.
       Both are t:"fill", and telling them apart is the difference between
       "un-engrave this area" and "delete the customer's logo". */
    bk: it.bk ? 1 : 0,
    /* a fill too intricate for contours, kept as run lengths — exactly the
       area that was previewed, never a simplification of it */
    m:  (it.m || []).map((n) => Math.max(0, Math.round(Number(n) || 0))),
    ms: (it.ms || []).slice(0, 2).map((n) => Math.max(1, Math.round(Number(n) || 1))),
    /* ── placed pictures ──
       A data URL must never reach the session document: one 2 MB picture
       inlined into a 1 MB Firestore document fails the write, and the failing
       write takes the whole design down with it — marks, thread, versions.
       Pictures are uploaded first and referenced by URL and path; anything
       still carrying its bytes is dropped here rather than at the database. */
    u:  /^data:/.test(String(it.u || "")) ? "" : String(it.u || "").slice(0, 900),
    sp: String(it.sp || "").slice(0, 300),
    tr: it.tr ? 1 : 0,          /* traced out of a generated picture */
    /* the background eraser, kept as what was ASKED for rather than what it
       produced: seed points, flat, and one tolerance for the picture */
    k:  (it.k || []).slice(0, 64).map((n) => Math.round((Number(n) || 0) * 1e4) / 1e4),
    kt: Math.max(0, Math.min(100, Number(it.kt) || 0)),
    /* manual eraser: non-destructive local-coordinate brush strokes. Each
       stroke keeps one radius plus a flat [u,v...] path inside the layer's
       own box, so moving/resizing/rotating the layer moves the erase with it. */
    er: (Array.isArray(it.er) ? it.er : []).slice(-80).map((st) => ({
      r: Math.max(0.001, Math.min(1, Number(st && st.r) || 0.02)),
      p: (Array.isArray(st && st.p) ? st.p : []).slice(-2400)
           .map((n) => Math.round((Number(n) || 0) * 1e4) / 1e4),
    })).filter((st) => st.p.length >= 2),
  };
}
let _mkSeq = 0;
function mkId() { return "i" + (++_mkSeq) + Math.random().toString(36).slice(2, 6); }

/* what a layer is called when the customer has not named it */
const MK_TYPE_NAME = {
  ink: "Pen stroke", hl: "Highlight", sign: "Signature", img: "Picture",
  fill: "Fill",
  text: "Note", callout: "Note", label: "Text",
  line: "Line", arrow: "Arrow", rect: "Rectangle", round: "Rounded box",
  ellipse: "Circle", ring: "Ring", tri: "Triangle", star: "Star",
  heart: "Heart", bail: "Charm + bail",
};
function mkLayerName(it) {
  if (it.n) return it.n;
  if (it.t === "text" || it.t === "callout" || it.t === "label") {
    const s = String(it.s || "").replace(/\s+/g, " ").trim();
    if (s) return s.slice(0, 28) + (s.length > 28 ? "…" : "");
  }
  return MK_TYPE_NAME[it.t] || "Layer";
}

/* ═══════════ placed pictures ═════════════════════════════════════════════
   A picture on the canvas is an item like any other; the only difference is
   that its pixels arrive asynchronously.

   THE CORS RULE
   Studio-owned Firebase objects already have a trusted Storage path. Fetch
   those bytes through britesAuth FIRST and load the returned data URL. This
   keeps every canvas exportable and, just as importantly, avoids deliberately
   firing a browser request that we already know the bucket may reject with a
   CORS error. Only URL-only images (for example an external catalogue image)
   need the anonymous-CORS attempt.

   Everything is cached by storage path for the life of the page, so a
   drawing with six pictures in it costs six loads once, not six per repaint. */
const MK_IMG = new Map();            // key → { img, state, waiters[] }

function mkImageKey(it) { return String(it.sp || it.u || ""); }

function mkImageFor(it, onReady) {
  const key = mkImageKey(it);
  if (!key) return null;
  let rec = MK_IMG.get(key);
  if (rec) {
    if (rec.state === "ready") return rec.img;
    if (onReady) rec.waiters.push(onReady);
    return null;
  }
  rec = { img: null, state: "loading", waiters: onReady ? [onReady] : [] };
  MK_IMG.set(key, rec);

  const settle = (img) => {
    rec.img = img;
    rec.state = img ? "ready" : "failed";
    const w = rec.waiters.splice(0);
    w.forEach((fn) => { try { fn(); } catch (e) {} });
  };
  const load = (src, crossOrigin) => new Promise((resolve) => {
    const img = new Image();
    if (crossOrigin) img.crossOrigin = "anonymous";
    img.onload = () => resolve(img);
    img.onerror = () => resolve(null);
    img.src = src;
  });

  (async () => {
    /* a data URL is already local — nothing to negotiate */
    if (/^data:/.test(it.u || "")) { settle(await load(it.u, false)); return; }

    /* Studio-owned Storage objects MUST take the same-origin path first.
       The old order intentionally generated a failed CORS request before
       reaching this fallback, filling the console with red errors and leaving
       one export path without the base bitmap at all. */
    if (it.sp) {
      try {
        const r = await postAuthed("britesAuth", { kind: "asset_fetch", path: it.sp });
        if (r && r.ok && r.dataUrl) { settle(await load(r.dataUrl, false)); return; }
      } catch (e) { console.warn("[studio] asset_fetch:", e && e.message); }
    }

    /* URL-only images have no server-side Storage path to fetch. Try CORS for
       those; if the remote host does not allow it, display-only loading is the
       final fallback and exporters will detect any resulting taint. */
    if (it.u) {
      const cors = await load(it.u, true);
      if (cors) { settle(cors); return; }
    }
    settle(it.u ? await load(it.u, false) : null);
  })();
  return null;
}
/* ═══════════ THE MAGIC BACKGROUND ERASER ═════════════════════════════════
   Click a patch of background and it falls away, out to the edge of the
   thing standing on it. The same flood the bucket uses, run over a
   picture's own pixels instead of the sheet: from each seed, every
   neighbouring pixel within a tolerance of the seed's COLOUR joins the
   region, and everything in it is made transparent. Where the object starts
   the colours stop matching, so that is where the erasure stops — nobody has
   to trace an outline by hand.

   Two decisions worth naming:

     · it is stored as PARAMETERS, not pixels. The item carries its seed
       points and one tolerance; the transparent version is computed from
       them and cached. So it costs no upload, no storage and no second copy
       of the picture, it travels inside the session document like every
       other property, and it is completely reversible — put the tolerance
       back, or clear the seeds, and the original picture is simply there
       again, because it was never altered.
     · the tolerance is per PICTURE, not per click. "How close a shade counts
       as background" is one judgement about one image, and a slider that
       re-runs every seed at once is the way a person actually finds it.   */
const MK_WAND = new Map();          /* signature → canvas */
/* pictures whose pixels cannot be read at all: a cross-origin bitmap with no
   CORS headers taints the canvas permanently, and no amount of retrying
   changes that. Recorded so the studio can SAY so instead of appearing to
   ignore the click. */
const MK_WAND_DEAD = new Set();
const MK_WAND_MAX = 1100;           /* the working size; bigger buys nothing */

function mkWandSig(it, img) {
  return mkImageKey(it) + "|" + (img.naturalWidth || img.width) + "x" + (img.naturalHeight || img.height) +
         "|" + (it.kt || 0) + "|" + (it.k || []).join(",");
}
/* the picture as the customer has cut it: transparent where the wand ran */
function mkWandCanvas(it, img) {
  const seeds = it.k || [];
  if (!seeds.length) return img;
  if (MK_WAND_DEAD.has(mkImageKey(it))) return img;
  const sig = mkWandSig(it, img);
  const hit = MK_WAND.get(sig);
  if (hit) return hit;

  const iw = img.naturalWidth || img.width, ih = img.naturalHeight || img.height;
  if (!iw || !ih) return img;
  const scale = Math.min(1, MK_WAND_MAX / Math.max(iw, ih));
  const W = Math.max(1, Math.round(iw * scale)), H = Math.max(1, Math.round(ih * scale));
  const cv = document.createElement("canvas");
  cv.width = W; cv.height = H;
  const ctx = cv.getContext("2d", { willReadFrequently: true });
  let data;
  try {
    ctx.drawImage(img, 0, 0, W, H);
    data = ctx.getImageData(0, 0, W, H);
  } catch (e) {
    /* a tainted picture cannot be read, so it cannot be cut */
    MK_WAND_DEAD.add(mkImageKey(it));
    return img;
  }
  const d = data.data;
  const N = W * H;
  /* tolerance is a percentage of the longest possible colour distance, which
     is what makes the slider mean the same thing on every picture */
  const tol = Math.max(0, Math.min(100, Number(it.kt) || 18));
  const lim = (tol / 100) * 441.673;          /* √(255²·3) */
  const gone = new Uint8Array(N);
  const stack = [];

  for (let s = 0; s + 1 < seeds.length; s += 2) {
    const sx = Math.min(W - 1, Math.max(0, Math.round(seeds[s] * W)));
    const sy = Math.min(H - 1, Math.max(0, Math.round(seeds[s + 1] * H)));
    const si = sy * W + sx;
    if (gone[si]) continue;
    const j0 = si * 4;
    const r0 = d[j0], g0 = d[j0 + 1], b0 = d[j0 + 2], a0 = d[j0 + 3];
    stack.length = 0;
    stack.push(si);
    gone[si] = 1;
    while (stack.length) {
      const i = stack.pop();
      const x = i % W, y = (i / W) | 0;
      for (let n = 0; n < 4; n++) {
        const nx = x + (n === 0 ? -1 : n === 1 ? 1 : 0);
        const ny = y + (n === 2 ? -1 : n === 3 ? 1 : 0);
        if (nx < 0 || ny < 0 || nx >= W || ny >= H) continue;
        const q = ny * W + nx;
        if (gone[q]) continue;
        const j = q * 4;
        /* an already-transparent pixel is background by definition */
        if (d[j + 3] < 16 && a0 < 16) { gone[q] = 1; stack.push(q); continue; }
        const dr = d[j] - r0, dg = d[j + 1] - g0, db = d[j + 2] - b0, da = d[j + 3] - a0;
        if (Math.sqrt(dr * dr + dg * dg + db * db) <= lim && Math.abs(da) <= 255 * (tol / 100) + 24) {
          gone[q] = 1; stack.push(q);
        }
      }
    }
  }
  /* Feather the cut by one pixel. A hard threshold leaves the anti-aliased
     rim of the object wearing a halo of the colour that was behind it; a
     single pass of "how many of my neighbours went?" softens the edge
     without eating into the object. */
  for (let i = 0; i < N; i++) {
    if (gone[i]) { d[i * 4 + 3] = 0; continue; }
    const x = i % W, y = (i / W) | 0;
    let near = 0;
    if (x > 0 && gone[i - 1]) near++;
    if (x < W - 1 && gone[i + 1]) near++;
    if (y > 0 && gone[i - W]) near++;
    if (y < H - 1 && gone[i + W]) near++;
    if (near >= 3) d[i * 4 + 3] = Math.round(d[i * 4 + 3] * 0.35);
    else if (near === 2) d[i * 4 + 3] = Math.round(d[i * 4 + 3] * 0.66);
  }
  ctx.putImageData(data, 0, 0);
  if (MK_WAND.size > 24) MK_WAND.clear();
  MK_WAND.set(sig, cv);
  return cv;
}
/* what the painter should draw for this picture: the cut version if it has
   been cut, the picture itself if it has not */
function mkPictureFor(it, onReady) {
  const img = mkImageFor(it, onReady);
  if (!img || !img.width) return img;
  if (!it.k || !it.k.length) return img;
  try { return mkWandCanvas(it, img); } catch (e) { return img; }
}

/* true once every picture in the list has resolved one way or the other —
   the export waits on this so a composite never ships with a hole in it */
function mkImagesSettled(items) {
  return (items || []).every((it) => {
    if (it.t !== "img") return true;
    const rec = MK_IMG.get(mkImageKey(it));
    return rec && rec.state !== "loading";
  });
}
async function mkAwaitImages(items, ms) {
  const t0 = Date.now();
  while (!mkImagesSettled(items) && Date.now() - t0 < (ms || 9000)) {
    items.forEach((it) => { if (it.t === "img") mkImageFor(it); });
    await new Promise((r) => setTimeout(r, 120));
  }
}

/* ═══════════ MANUAL ERASER — NON-DESTRUCTIVE LAYER MASK ════════════════
   A brush stroke never rewrites the imported geometry or bitmap. The layer
   is painted into its own temporary canvas, then the saved brush path is
   composited out with destination-out. This keeps Undo exact and makes the
   same erasure appear on screen, in exports and after reload. */
function mkErasePaintMask(ctx, W, H, it) {
  const strokes = Array.isArray(it && it.er) ? it.er : [];
  if (!strokes.length) return;
  const bb = mkBBox(it);
  const maxDim = Math.max(1e-6, bb.w, bb.h);
  ctx.save();
  if (it.r) {
    ctx.translate((bb.x + bb.w / 2) * W, (bb.y + bb.h / 2) * H);
    ctx.rotate(it.r);
    ctx.translate(-(bb.x + bb.w / 2) * W, -(bb.y + bb.h / 2) * H);
  }
  ctx.globalCompositeOperation = "destination-out";
  ctx.globalAlpha = 1;
  ctx.lineCap = "round";
  ctx.lineJoin = "round";
  strokes.forEach((st) => {
    const pts = Array.isArray(st.p) ? st.p : [];
    if (pts.length < 2) return;
    const radiusSheet = Math.max(0.0005, Number(st.r) || 0.02) * maxDim;
    const widthPx = Math.max(1, radiusSheet * 2 * ((W + H) / 2));
    ctx.lineWidth = widthPx;
    const sx = (u) => (bb.x + u * bb.w) * W;
    const sy = (v) => (bb.y + v * bb.h) * H;
    if (pts.length === 2) {
      ctx.beginPath();
      ctx.arc(sx(pts[0]), sy(pts[1]), widthPx / 2, 0, Math.PI * 2);
      ctx.fill();
      return;
    }
    ctx.beginPath();
    ctx.moveTo(sx(pts[0]), sy(pts[1]));
    for (let k = 2; k + 1 < pts.length; k += 2) ctx.lineTo(sx(pts[k]), sy(pts[k + 1]));
    ctx.stroke();
  });
  ctx.restore();
}
function mkPaintErasedLayer(ctx, W, H, it, opt) {
  const cv = document.createElement("canvas");
  cv.width = W; cv.height = H;
  const x = cv.getContext("2d");
  const clean = Object.assign({}, it, { er: [] });
  mkPaint(x, W, H, [clean], { export: true, _erasePass: true, onImage: opt && opt.onImage });
  mkErasePaintMask(x, W, H, it);
  ctx.drawImage(cv, 0, 0);
}

/* ═══════════ the painter — one function, screen and export alike ══════════
   Every pixel the designer receives is painted by this, at a different size.
   Nothing can drift between what the customer sees and what is sent.        */
function mkPaint(ctx, W, H, items, opt) {
  opt = opt || {};
  const px = (n) => Math.max(1, n * W);

  if (opt.grid) {
    ctx.save();
    ctx.strokeStyle = "rgba(28,29,29,.10)"; ctx.lineWidth = 1;
    const gn = Math.max(4, Math.min(48, opt.gridN || 12));
    for (let i = 1; i < gn; i++) {
      const g = (i / gn);
      ctx.beginPath(); ctx.moveTo(g * W, 0); ctx.lineTo(g * W, H); ctx.stroke();
      ctx.beginPath(); ctx.moveTo(0, g * H); ctx.lineTo(W, g * H); ctx.stroke();
    }
    ctx.restore();
  }
  if (opt.centre || opt.mirror) {
    ctx.save();
    ctx.strokeStyle = opt.mirror ? "rgba(165,138,82,.75)" : "rgba(28,29,29,.28)";
    ctx.lineWidth = 1; ctx.setLineDash([6, 6]);
    ctx.beginPath(); ctx.moveTo(W / 2, 0); ctx.lineTo(W / 2, H); ctx.stroke();
    if (opt.centre) { ctx.beginPath(); ctx.moveTo(0, H / 2); ctx.lineTo(W, H / 2); ctx.stroke(); }
    ctx.restore();
  }

  const notes = [];
  items.forEach((it) => {
    if (it.hid) return;
    if (it.t === "text" || it.t === "callout") notes.push(it);
  });

  items.forEach((it) => {
    if (it.hid) return;                        // a hidden layer paints nothing
    if (!opt._erasePass && Array.isArray(it.er) && it.er.length &&
        it.t !== "text" && it.t !== "callout" && it.t !== "label") {
      mkPaintErasedLayer(ctx, W, H, it, opt);
      return;
    }
    ctx.save();
    ctx.lineCap = "round"; ctx.lineJoin = "round";
    ctx.strokeStyle = it.c || "#d23a3a";
    ctx.fillStyle = it.f && it.f !== "none" ? it.f : "transparent";
    ctx.lineWidth = px(it.w || 0.008);
    ctx.globalAlpha = (MK_ALPHA[it.t] || 1) * (it.o == null ? 1 : it.o);

    const p = it.p || [];
    const bb = mkBBox(it);
    /* rotation is about the item's own centre, so a rotated item still lives
       where it was put */
    if (it.r) {
      ctx.translate((bb.x + bb.w / 2) * W, (bb.y + bb.h / 2) * H);
      ctx.rotate(it.r);
      ctx.translate(-(bb.x + bb.w / 2) * W, -(bb.y + bb.h / 2) * H);
    }

    if (it.t === "fill") {
      /* ── WHAT A FILL LOOKS LIKE IS DECIDED BY ITS INSTRUCTION ──────────
         This branch used to paint `it.c` unconditionally and never look at
         `it.f` at all — which meant the one control the whole engraving
         system rests on did NOTHING to any object that is itself a fill.
         An .svg shape, an .ai path, a part traced out of a generated
         drawing: select it, press Cut out or Outline, and the model changed
         while the sheet did not move a pixel. The customer's report was
         exactly that, in exactly those words, and it went further than they
         knew — the payload sent to the workshop reported the intention while
         the picture beside it showed the opposite.

         Three instructions, three appearances, the same three everywhere
         else in the studio:
           ENGRAVE  → the area, solid, in the engraving ink.
           CUT OUT  → the area, solid, in the reserved blue.
           OUTLINE  → the boundary line only; the inside is plain metal. */
      const intent = mkFillIntent(it.f);
      const ink = mkIntentInk(intent);

      if (it.m && it.m.length) {
        /* a masked fill: drawn into its own box, so moving or scaling the
           layer moves and scales the area exactly as any picture would */
        /* "edge" is mkRleCanvas's outline-only mode: it walks the mask and
           keeps a pixel only where a neighbour is unset, then widens that to
           a visible band. Handing it the OUTLINE INK instead asked for the
           same area painted solid red — the instruction was read correctly
           and then drawn as its own opposite. */
        const cv = mkRleCanvas(it, intent === "none" ? "edge" : ink);
        if (cv && cv.width && cv.height) {
          ctx.imageSmoothingEnabled = intent !== "none";
          ctx.drawImage(cv, bb.x * W, bb.y * H, Math.max(1, bb.w * W), Math.max(1, bb.h * H));
        }
      } else {
        /* one or more closed contours (outer edge plus any holes), painted
           evenodd so a shape sitting INSIDE the filled area keeps its white
           interior */
        const path = new Path2D();
        const q = (it.q && it.q.length) ? it.q : [Math.floor(p.length / 2)];
        let k = 0;
        q.forEach((len) => {
          for (let j = 0; j < len && k * 2 + 1 < p.length; j++, k++) {
            const X = p[k * 2] * W, Y = p[k * 2 + 1] * H;
            if (j === 0) path.moveTo(X, Y); else path.lineTo(X, Y);
          }
          path.closePath();
        });
        /* OUTLINE ONLY MEANS THE INSIDE IS PLAIN METAL. The block above says
           exactly that, and the stroke below already handles it — but the
           fill ran unconditionally, so an outline-only contour was flooded in
           the outline red before its boundary was ever drawn. */
        if (intent !== "none") {
          ctx.fillStyle = ink;
          ctx.fill(path, "evenodd");
        }
        /* THE OUTLINE IS ALWAYS DRAWN WHEN THERE IS NO FILL, whatever the
           object is. Skipping it — which the traced branch used to do
           unconditionally — would make "outline only" erase the layer from
           the sheet, which is the vanishing act this round set out to end,
           wearing a different hat.

           When the area IS filled, the stroke is only there to close the
           half-pixel seam a traced contour leaves, so a traced object (whose
           contours sit exactly on the pixels they came from) does without. */
        const seam = !it.tr;
        if (intent === "none" || seam) {
          ctx.strokeStyle = ink;
          /* A HAIRLINE FOR ANYTHING THIS BUILD MADE — but an old fill was
             traced from a flood that had NOT been grown back to the true
             edge, and it stored a fat stroke precisely because it needed one
             to reach its own walls. Repainting those at a hairline would
             pull every fill in every saved design back by half the old
             tolerance and leave a white seam round it.

             `it.w` is otherwise never read here: Weight edits the stroke of
             whatever is selected, and a fill is selected the moment it
             lands, so reading it made nudging Weight look like the fill
             getting more or less accurate. It never was. */
          const legacy = (!it.gr && Number(it.w) > 0.02) ? Number(it.w) : 0;
          const hair = Math.max(0.75, W / 900);
          ctx.lineWidth = intent === "none" ? Math.max(hair, W / 420)
                                            : (legacy ? px(legacy) : hair);
          ctx.stroke(path);
        }
      }
    } else if (it.t === "img") {
      /* letterboxed inside its own box, so a placed picture keeps its shape
         however the box is dragged — the alternative is a squashed photo the
         customer did not ask for */
      const img = mkPictureFor(it, opt.onImage);
      const bx = bb.x * W, by = bb.y * H, bw = bb.w * W, bh = bb.h * H;
      if (img && img.width) {
        const s = Math.min(bw / img.width, bh / img.height);
        const dw = img.width * s, dh = img.height * s;
        ctx.drawImage(img, bx + (bw - dw) / 2, by + (bh - dh) / 2, dw, dh);
      } else if (!opt.export) {
        ctx.setLineDash([6, 6]);
        ctx.strokeStyle = "rgba(28,29,29,.35)";
        ctx.lineWidth = 1;
        ctx.strokeRect(bx, by, bw, bh);
        ctx.setLineDash([]);
      }
    } else if (it.t === "ink" || it.t === "hl" || it.t === "sign") {
      if (p.length >= 2) {
        ctx.beginPath();
        ctx.moveTo(p[0] * W, p[1] * H);
        for (let i = 2; i + 1 < p.length; i += 2) ctx.lineTo(p[i] * W, p[i + 1] * H);
        if (p.length === 2) ctx.lineTo(p[0] * W + 0.1, p[1] * H + 0.1);  /* a dot is a mark */
        ctx.stroke();
      }
    } else if (MK_SHAPE_TOOLS.indexOf(it.t) >= 0) {
      mkPaintShape(ctx, it, bb, W, H);
    } else if (it.t === "label") {
      const size = Math.max(9, (it.fs || 0.05) * W);
      ctx.font = `600 ${size}px ${MK_FONTS[it.ff] || MK_FONTS.sans}`;
      ctx.textBaseline = "middle";
      ctx.fillStyle = it.c || "#1c1d1d";
      const lines = String(it.s || "").split("\n");
      lines.forEach((ln, i) => {
        const y = (p[1] || 0.5) * H + (i - (lines.length - 1) / 2) * size * 1.2;
        ctx.fillText(ln, (p[0] || 0.5) * W, y);
      });
      it._w = Math.max.apply(null, lines.map((l) => ctx.measureText(l).width)) / W;
      it._h = (lines.length * size * 1.2) / H;
    }
    ctx.restore();
  });

  /* Notes last and unrotated, so they are always legible and always on top —
     they are annotation, not artwork. Numbered to match the notes list that
     travels with the request. */
  notes.forEach((it, i) => {
    const p = it.p || [];
    const ax = (p[0] || 0.5) * W, ay = (p[1] || 0.5) * H;
    const bx = it.t === "callout" && p.length > 3 ? p[2] * W : ax;
    const by = it.t === "callout" && p.length > 3 ? p[3] * H : ay;
    const size = Math.max(9, (it.fs || 0.03) * W);
    ctx.save();
    ctx.font = `600 ${size}px ${MK_FONTS[it.ff] || MK_FONTS.sans}`;
    const label = (i + 1) + ". " + String(it.s || "").replace(/\n+/g, " ");
    const tw = ctx.measureText(label).width;
    const padX = size * 0.55, h = size * 1.9;
    /* keep the pill on the sheet whatever the pin does */
    let x = Math.min(W - tw - padX * 2 - 4, Math.max(4, bx - tw / 2 - padX));
    let y = Math.min(H - h - 4, Math.max(4, by - h - size * 0.8));
    if (it.t === "callout") {
      ctx.strokeStyle = it.c || "#1c1d1d";
      ctx.lineWidth = Math.max(1, size * 0.06);
      ctx.setLineDash([size * 0.28, size * 0.28]);
      ctx.beginPath(); ctx.moveTo(x + tw / 2 + padX, y + h); ctx.lineTo(ax, ay); ctx.stroke();
      ctx.setLineDash([]);
    }
    ctx.fillStyle = "rgba(28,29,29,.93)";
    ctx.beginPath();
    if (ctx.roundRect) ctx.roundRect(x, y, tw + padX * 2, h, h / 2);
    else ctx.rect(x, y, tw + padX * 2, h);
    ctx.fill();
    /* the pin itself */
    ctx.fillStyle = it.c || "#d23a3a";
    ctx.beginPath(); ctx.arc(ax, ay, Math.max(2.5, size * 0.16), 0, Math.PI * 2); ctx.fill();
    ctx.fillStyle = "#ffffff";
    ctx.textBaseline = "middle";
    ctx.fillText(label, x + padX, y + h / 2);
    ctx.restore();
    it._w = (tw + padX * 2) / W; it._h = h / H; it._bx = x / W; it._by = y / H;
  });
}

function mkPaintShape(ctx, it, bb, W, H) {
  const x = bb.x * W, y = bb.y * H, w = bb.w * W, h = bb.h * H;
  const p = it.p || [];
  /* A CLOSED SHAPE IS AN AREA, and an area carries one of the three
     instructions whether or not it is filled. It used to be stroked in the
     pen's own colour, so an outline-only circle looked identical to a circle
     nobody had said anything about — the single commonest way this control
     went unnoticed. A line and an arrow enclose nothing, so they stay ink. */
  const closed = it.t !== "line" && it.t !== "arrow";
  /* an unfilled ring or bail is a HOLE by the studio's own grammar, so it is
     stroked in the cut-out blue rather than the outline red — the list sent
     to the workshop has said so all along */
  const intent = mkItemIntent(it);
  const ink = mkIntentInk(intent);
  /* WHETHER IT IS FILLED IS THE INSTRUCTION, NOT THE SHAPE. `filled = closed`
     flooded every closed primitive in whatever ink its instruction had
     chosen, so an outline-only body came out solid red — and `intent` sat one
     line above, computed and unread. A ring or bail promoted to cut-out is
     still an OUTLINE on the page: mkItemIntent changes its colour, never its
     hollowness, so the promotion is read off `it.f` here, not off intent. */
  const filled = closed && mkFillIntent(it.f) !== "none";
  const path = new Path2D();
  switch (it.t) {
    case "line":
      path.moveTo((p[0] || 0) * W, (p[1] || 0) * H);
      path.lineTo((p[2] || 0) * W, (p[3] || 0) * H);
      break;
    case "arrow": {
      const x1 = (p[0] || 0) * W, y1 = (p[1] || 0) * H, x2 = (p[2] || 0) * W, y2 = (p[3] || 0) * H;
      const a = Math.atan2(y2 - y1, x2 - x1);
      const hd = Math.max(8, Math.hypot(x2 - x1, y2 - y1) * 0.22);
      path.moveTo(x1, y1); path.lineTo(x2, y2);
      path.moveTo(x2, y2); path.lineTo(x2 - hd * Math.cos(a - 0.42), y2 - hd * Math.sin(a - 0.42));
      path.moveTo(x2, y2); path.lineTo(x2 - hd * Math.cos(a + 0.42), y2 - hd * Math.sin(a + 0.42));
      break;
    }
    case "rect": path.rect(x, y, w, h); break;
    case "round": {
      const r = Math.min(w, h) * 0.22;
      if (path.roundRect) path.roundRect(x, y, w, h, r); else path.rect(x, y, w, h);
      break;
    }
    case "ellipse": path.ellipse(x + w / 2, y + h / 2, w / 2, h / 2, 0, 0, Math.PI * 2); break;
    case "ring":
      path.ellipse(x + w / 2, y + h / 2, w / 2, h / 2, 0, 0, Math.PI * 2);
      path.ellipse(x + w / 2, y + h / 2, w / 4, h / 4, 0, 0, Math.PI * 2);
      break;
    case "tri": path.moveTo(x + w / 2, y); path.lineTo(x + w, y + h); path.lineTo(x, y + h); path.closePath(); break;
    case "star": {
      const cx = x + w / 2, cy = y + h / 2;
      for (let i = 0; i < 10; i++) {
        const rr = i % 2 ? 0.42 : 1;
        const a = -Math.PI / 2 + (i * Math.PI) / 5;
        const sx = cx + Math.cos(a) * (w / 2) * rr, sy = cy + Math.sin(a) * (h / 2) * rr;
        if (i) path.lineTo(sx, sy); else path.moveTo(sx, sy);
      }
      path.closePath();
      break;
    }
    case "heart": {
      const cx = x + w / 2;
      path.moveTo(cx, y + h);
      path.bezierCurveTo(x - w * 0.12, y + h * 0.55, x + w * 0.08, y - h * 0.10, cx, y + h * 0.28);
      path.bezierCurveTo(x + w * 0.92, y - h * 0.10, x + w * 1.12, y + h * 0.55, cx, y + h);
      path.closePath();
      break;
    }
    case "bail": {
      /* the loop a charm actually hangs from — the one piece of geometry every
         charm has and nobody remembers to draw */
      const br = Math.min(w, h) * 0.13;
      path.rect(x, y + br * 2, w, h - br * 2);
      path.ellipse(x + w / 2, y + br, br, br, 0, 0, Math.PI * 2);
      break;
    }
  }
  if (filled) { ctx.fillStyle = ink; ctx.fill(path, "evenodd"); }
  if (closed) ctx.strokeStyle = ink;
  ctx.stroke(path);
}

/* ── geometry ────────────────────────────────────────────────────────────── */
function mkBBox(it) {
  const p = it.p || [];
  if (it.t === "text" || it.t === "callout") {
    const w = it._w || 0.2, h = it._h || 0.06;
    const bx = it._bx != null ? it._bx : (p[0] || 0) - w / 2;
    const by = it._by != null ? it._by : (p[1] || 0) - h;
    return { x: bx, y: by, w, h };
  }
  if (it.t === "label") {
    const w = it._w || 0.2, h = it._h || 0.06;
    return { x: (p[0] || 0) - w / 2, y: (p[1] || 0) - h / 2, w, h };
  }
  let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
  for (let i = 0; i + 1 < p.length; i += 2) {
    x0 = Math.min(x0, p[i]); x1 = Math.max(x1, p[i]);
    y0 = Math.min(y0, p[i + 1]); y1 = Math.max(y1, p[i + 1]);
  }
  if (!isFinite(x0)) return { x: 0, y: 0, w: 0, h: 0 };
  return { x: x0, y: y0, w: Math.max(1e-4, x1 - x0), h: Math.max(1e-4, y1 - y0) };
}
/* a point, brought back into an item's own unrotated frame */
function mkUnrotate(it, x, y) {
  if (!it.r) return [x, y];
  const bb = mkBBox(it), cx = bb.x + bb.w / 2, cy = bb.y + bb.h / 2;
  const c = Math.cos(-it.r), s = Math.sin(-it.r);
  const dx = x - cx, dy = y - cy;
  return [cx + dx * c - dy * s, cy + dx * s + dy * c];
}
/* Where a picture is actually drawn inside its box. mkPaint letterboxes it,
   so the box and the picture are not the same rectangle, and a click has to
   be converted through the same arithmetic or the wand would fire at the
   wrong pixel. */
function mkPictureRect(it) {
  const img = mkImageFor(it);
  const bb = mkBBox(it);
  if (!img || !img.width) return bb;
  const s = Math.min(bb.w / img.width, bb.h / img.height);
  const dw = img.width * s, dh = img.height * s;
  return { x: bb.x + (bb.w - dw) / 2, y: bb.y + (bb.h - dh) / 2, w: dw, h: dh };
}
/* a sheet point in the picture's own 0..1 space, or null if it missed */
function mkPictureLocal(it, x, y) {
  const [ux, uy] = mkUnrotate(it, x, y);
  const r = mkPictureRect(it);
  if (!r.w || !r.h) return null;
  const u = (ux - r.x) / r.w, v = (uy - r.y) / r.h;
  if (u < 0 || u > 1 || v < 0 || v > 1) return null;
  return [u, v];
}
/* the topmost picture under a point — the one a click would land on */
function mkHitPicture(x, y) {
  for (let i = __mk.items.length - 1; i >= 0; i--) {
    const it = __mk.items[i];
    if (it.t !== "img" || it.hid || it.lok) continue;
    if (mkPictureLocal(it, x, y)) return i;
  }
  return -1;
}
const MK_WAND_DEFAULT_TOL = 18;
function mkWandAt(x, y) {
  const i = mkHitPicture(x, y);
  if (i < 0) { toast("Click on a picture — this tool cuts a picture's background away", "err"); return false; }
  const it = __mk.items[i];
  const uv = mkPictureLocal(it, x, y);
  mkPushUndo();
  it.k = (it.k || []).concat([uv[0], uv[1]]).slice(-64);
  if (!it.kt) it.kt = MK_WAND_DEFAULT_TOL;
  __mk.sel = i;
  mkTouch();
  if (MK_WAND_DEAD.has(mkImageKey(it))) {
    toast("This picture's pixels can't be read from here, so its background can't be cut. " +
          "Re-import it from your device and it will work.", "err");
    return false;
  }
  return true;
}
/* the common case, with no aim required: the four corners of a picture are
   its background far more often than not */
function mkKillBackground() {
  if (!__mk) return false;
  let i = __mk.sel;
  if (!(__mk.items[i] && __mk.items[i].t === "img")) {
    i = -1;
    for (let k = __mk.items.length - 1; k >= 0; k--) {
      if (__mk.items[k].t === "img" && !__mk.items[k].hid) { i = k; break; }
    }
  }
  if (i < 0) { toast("Add a picture first, then its background can go", "err"); return false; }
  const it = __mk.items[i];
  mkPushUndo();
  it.k = [0.012, 0.012, 0.988, 0.012, 0.012, 0.988, 0.988, 0.988];
  it.kt = it.kt || MK_WAND_DEFAULT_TOL;
  __mk.sel = i;
  mkTouch();
  const img = mkImageFor(it);
  if (img && img.width) {
    toast("Background removed — drag “how close a shade counts” if it took too much or too little", "gold");
  }
  return true;
}
function mkRestoreBackground() {
  if (!__mk) return false;
  const list = mkSelItems().filter((o) => o.t === "img" && (o.k || []).length);
  const targets = list.length ? list : __mk.items.filter((o) => o.t === "img" && (o.k || []).length);
  if (!targets.length) { toast("No background has been removed here", "err"); return false; }
  mkPushUndo();
  targets.forEach((o) => { o.k = []; });
  mkTouch();
  toast("Background back — nothing was ever altered, so nothing was lost", "gold");
  return true;
}

function mkIsText(it) {
  return !!it && (it.t === "text" || it.t === "callout" || it.t === "label");
}
/* the text item under a point, if any — the thing a double-click reopens */
function mkHitText(x, y) {
  const i = mkHitItem(x, y);
  return (i >= 0 && mkIsText(__mk.items[i])) ? i : -1;
}
function mkEditItem(i) {
  const it = __mk && __mk.items[i];
  if (!mkIsText(it)) return false;
  if (it.lok) { toast("That layer is locked — unlock it to edit the words", "err"); return false; }
  __mk.sel = i;
  mkSetTool("select");
  __mk.sel = i;
  mkChrome();
  mkOpenEditor(it);
  return true;
}
function mkHitItem(x, y) {
  /* topmost first — the thing you can see is the thing you grab. A hidden
     layer is not there to be grabbed; a locked one is deliberately out of
     reach, which is the whole point of locking it. */
  for (let i = __mk.items.length - 1; i >= 0; i--) {
    const it = __mk.items[i];
    if (it.hid || it.lok) continue;
    const [ux, uy] = mkUnrotate(it, x, y);
    if (it.t === "ink" || it.t === "hl" || it.t === "sign") {
      const p = it.p || [], tol = Math.max(0.02, (it.w || 0.008) * 1.6);
      for (let k = 0; k + 1 < p.length; k += 2) {
        const dx = p[k] - ux, dy = p[k + 1] - uy;
        if (dx * dx + dy * dy < tol * tol) return i;
      }
      continue;
    }
    const bb = mkBBox(it), pad = 0.012;
    if (ux >= bb.x - pad && ux <= bb.x + bb.w + pad &&
        uy >= bb.y - pad && uy <= bb.y + bb.h + pad) return i;
  }
  return -1;
}

/* ═══════════ SELECTION — a SET, not an index ═════════════════════════════
   The sheet used to hold one selected index. Everything downstream — the
   dashed box, the handles, delete, the layer list, the object menu — read
   `__mk.sel` and got a number.

   Rather than rewrite forty call sites, `sel` is now a property with a
   getter and a setter over the real thing, which is `selIds`: an ORDERED
   LIST OF IDS. Reading `.sel` gives the anchor — the last thing picked, the
   one whose colour and weight the format bar shows. Writing `.sel = i`
   means what it always meant: select exactly that, and nothing else. Every
   line of the old code therefore keeps working unchanged and collapses a
   multi-selection to one, which is precisely what a plain click should do.

   Ids rather than indices, because indices move. Reorder the stack, group
   four things, delete something underneath, and an index quietly points at
   a different object; an id never does.                                    */
function mkDefineSel(mk) {
  Object.defineProperty(mk, "sel", {
    enumerable: false, configurable: true,
    get() {
      const ids = mk.selIds;
      if (!ids || !ids.length) return -1;
      for (let n = ids.length - 1; n >= 0; n--) {
        const id = ids[n];
        for (let i = 0; i < mk.items.length; i++) if (mk.items[i].id === id) return i;
      }
      return -1;
    },
    set(i) {
      const it = (Number(i) >= 0 && mk.items) ? mk.items[Number(i)] : null;
      mk.selIds = it ? [it.id] : [];
    },
  });
  return mk;
}
/* every selected index, bottom of the stack first */
function mkSelIdx(mk) {
  mk = mk || __mk;
  if (!mk || !mk.selIds || !mk.selIds.length) return [];
  const want = mk.selIds, out = [];
  for (let i = 0; i < mk.items.length; i++) {
    if (want.indexOf(mk.items[i].id) >= 0) out.push(i);
  }
  return out;
}
function mkSelItems() { return mkSelIdx().map((i) => __mk.items[i]); }
function mkSelCount() { return mkSelIdx().length; }
function mkIsSelIdx(i) {
  const it = __mk && __mk.items[i];
  return !!it && __mk.selIds.indexOf(it.id) >= 0;
}
/* set the selection from a list of indices; the LAST one becomes the anchor */
function mkSelSet(list) {
  if (!__mk) return;
  const ids = [];
  (list || []).forEach((i) => {
    const it = __mk.items[i];
    if (it && ids.indexOf(it.id) < 0) ids.push(it.id);
  });
  __mk.selIds = ids;
}
function mkSelAdd(list) {
  if (!__mk) return;
  const ids = __mk.selIds.slice();
  (list || []).forEach((i) => {
    const it = __mk.items[i];
    if (it && ids.indexOf(it.id) < 0) ids.push(it.id);
  });
  __mk.selIds = ids;
}
function mkSelDrop(list) {
  if (!__mk) return;
  const kill = [];
  (list || []).forEach((i) => { const it = __mk.items[i]; if (it) kill.push(it.id); });
  __mk.selIds = __mk.selIds.filter((id) => kill.indexOf(id) < 0);
}
/* shift-click: in or out, and the whole group goes with it */
function mkSelToggle(list) {
  if (!__mk) return;
  const every = (list || []).length && list.every((i) => mkIsSelIdx(i));
  if (every) mkSelDrop(list); else mkSelAdd(list);
}

/* ═══════════ GROUPS ══════════════════════════════════════════════════════
   A group is one field on the item — `g`, which mkPack has always carried —
   plus a name kept beside it. Two rules make the rest fall out:

     1. members of a group are CONTIGUOUS in the stack. Grouping closes the
        gaps, so a group can be dragged up or down the layer list as one
        block without anything having to be threaded between its members.
     2. clicking any member selects the whole group. Alt-click reaches the
        one object inside, the way it does in every editor.               */
function mkGroupOf(it) { return it && it.g ? String(it.g) : ""; }
function mkGroupIdx(g) {
  if (!__mk || !g) return [];
  const out = [];
  __mk.items.forEach((it, i) => { if (it.g === g) out.push(i); });
  return out;
}
function mkGroupName(g) {
  const m = (__mk && __mk.groups && __mk.groups[g]) || null;
  return (m && m.n) || "Group";
}
function mkGroupOpen(g) {
  const m = (__mk && __mk.groups && __mk.groups[g]) || null;
  return !(m && m.col);                       /* open unless collapsed */
}
/* the indices a click on `i` should select: the whole group, or just it */
function mkReachOf(i, alone) {
  const it = __mk && __mk.items[i];
  if (!it) return [];
  const g = mkGroupOf(it);
  if (!g || alone) return it.lok ? [] : [i];
  /* a locked member stays out of reach even inside a group — locking is a
     promise that the thing will not move, and a group cannot overrule it */
  return mkGroupIdx(g).filter((k) => !__mk.items[k].lok);
}

function mkGroupSelection() {
  if (!__mk) return false;
  /* pulling in every member of any group that is partly selected: you cannot
     group half of a group, and pretending otherwise would leave orphans */
  const seed = mkSelIdx();
  const set = {};
  seed.forEach((i) => {
    const g = mkGroupOf(__mk.items[i]);
    if (g) mkGroupIdx(g).forEach((k) => { set[k] = 1; });
    else set[i] = 1;
  });
  const idxs = Object.keys(set).map(Number).sort((a, b) => a - b);
  if (idxs.length < 2) { toast("Pick two or more layers first — shift-click to add", "err"); return false; }
  mkPushUndo();
  const gid = "g" + Math.random().toString(36).slice(2, 8);
  const objs = idxs.map((i) => __mk.items[i]);
  /* where the block lands: where the topmost member is now, once everything
     below it has been lifted out */
  const top = idxs[idxs.length - 1];
  const below = idxs.filter((i) => i < top).length;
  for (let k = idxs.length - 1; k >= 0; k--) __mk.items.splice(idxs[k], 1);
  const at = Math.max(0, Math.min(__mk.items.length, top - below));
  objs.forEach((o, k) => {
    /* a group of groups is ONE group — a hierarchy nobody asked for is a
       hierarchy nobody can find their way out of */
    o.g = gid;
    __mk.items.splice(at + k, 0, o);
  });
  if (!__mk.groups) __mk.groups = {};
  __mk.groups[gid] = { n: "Group", col: 0 };
  mkSelSet(objs.map((o) => __mk.items.indexOf(o)));
  mkTouch();
  mkRenderLayers();
  toast(`Grouped ${objs.length} layers — they move, resize and rotate together`, "gold");
  return true;
}

function mkUngroupSelection() {
  if (!__mk) return false;
  const gs = {};
  mkSelIdx().forEach((i) => { const g = mkGroupOf(__mk.items[i]); if (g) gs[g] = 1; });
  const list = Object.keys(gs);
  if (!list.length) { toast("Nothing grouped in that selection", "err"); return false; }
  mkPushUndo();
  const freed = [];
  list.forEach((g) => {
    mkGroupIdx(g).forEach((i) => { __mk.items[i].g = ""; freed.push(i); });
    if (__mk.groups) delete __mk.groups[g];
  });
  mkSelSet(freed);
  mkTouch();
  mkRenderLayers();
  toast(list.length === 1 ? "Ungrouped" : `Ungrouped ${list.length} groups`, "gold");
  return true;
}
function mkCanGroup()   { return mkSelIdx().length > 1; }
function mkCanUngroup() { return mkSelIdx().some((i) => !!mkGroupOf(__mk.items[i])); }

/* ── the box that has to hold everything ─────────────────────────────────
   With one object selected this is its own bounds, exactly as before. With
   several it is their union, which is what makes the marquee grow to
   encompass the whole selection.                                          */
function mkUnionBBox(items) {
  let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
  (items || []).forEach((it) => {
    const b = mkBBox(it);
    x0 = Math.min(x0, b.x); y0 = Math.min(y0, b.y);
    x1 = Math.max(x1, b.x + b.w); y1 = Math.max(y1, b.y + b.h);
  });
  if (!isFinite(x0)) return { x: 0, y: 0, w: 0, h: 0 };
  return { x: x0, y: y0, w: Math.max(1e-4, x1 - x0), h: Math.max(1e-4, y1 - y0) };
}
function mkSelBBox() { return mkUnionBBox(mkSelItems()); }

/* ═══════════ the one write path ══════════════════════════════════════════
   Called after EVERY mutation. Repaints, refreshes the chrome, and writes the
   session. There is deliberately no other way for work to reach storage, and
   deliberately no commit step — a note typed and abandoned is already saved.  */
function mkTouch(skipUndoButtons) {
  if (!__mk) return;
  /* the composed sheet the region engine works from is now out of date */
  mkRegionForget();
  mkRedraw();
  if (!skipUndoButtons) mkChrome();
  mkPersist();
  /* Every path that adds, removes, reorders or edits an item ends here, so
     the layer list is repainted from here too and can never lag the sheet.
     Coalesced to one frame: typing into a note calls mkTouch per keystroke,
     and rebuilding a dozen thumbnails per letter would be absurd. */
  mkRenderLayersSoon();
}
let _mkLayerFrame = 0;
function mkRenderLayersSoon() {
  if (!__mkDock.open || __mkDock.tab !== "layers") return;
  if (_mkLayerFrame) return;
  _mkLayerFrame = requestAnimationFrame(() => {
    _mkLayerFrame = 0;
    if (__mk && __mkDock.open && __mkDock.tab === "layers") mkRenderLayers();
  });
}
function mkPersist() {
  if (!__mk) return;
  const key = __mk.key;
  if (!state.markups) state.markups = {};
  const items = __mk.items.map(mkPack);
  /* A blank sheet with a CHOICE made on it is not nothing: pick "exactly as
     drawn" before drawing a line and that choice must still be there when you
     do. Storing only when items exist quietly threw it away. */
  const changed = !!(items.length || __mk.crop || __mk.baseRot ||
                     __mk.mode === "exact" || mkGuidesChosen());
  if (changed) {
    state.markups[key] = {
      items,
      mode: __mk.mode === "exact" ? "exact" : "interpret",
      crop: __mk.crop ? [__mk.crop.x, __mk.crop.y, __mk.crop.w] : [],
      baseRot: __mk.baseRot || 0,
      /* a flat list of {id,n}: Firestore refuses an array inside an array,
         and a map keyed by a generated id is harder to read in the console */
      groups: mkGroupList(),
      /* the sheet IS the drawing, taken apart — so the bitmap underneath is
         not painted, and an untouched trace is not a mark-up */
      traced: __mk.traced ? 1 : 0,
      /* CARRIED, NOT REBUILT. The account of which areas are holes belongs to
         the design, not to this save: dropping it here would mean the first
         time anybody nudged a layer, the plan the drawing was made from
         disappeared and the render went back to guessing. */
      plan: ((state.markups[key] || {}).plan || []).slice(0, 40),
      dirty: __mk.dirty ? 1 : 0,
      guides: mkGuideFlags(),
      gridN: Math.max(4, Math.min(48, __mk.gridN || 12)),
      updatedAt: now(),
    };
  } else {
    delete state.markups[key];
    /* a tombstone travels with the next save, or merge puts it straight back */
    if (typeof forgetKey === "function") forgetKey("markups", key);
  }
  /* the stacks persist beside the items — snapshots are already JSON strings,
     so this is a slice, not a serialization. flushSession budgets the total. */
  if (!state.mkHistory) state.mkHistory = {};
  if (__mk.undo.length || __mk.redo.length) {
    state.mkHistory[key] = { u: __mk.undo.slice(-10), r: __mk.redo.slice(-6) };
  } else if (!changed) {
    delete state.mkHistory[key];
  }
  saveSession();
}
/* only groups that still have members — deleting the last layer out of a
   group should not leave its name behind for ever */
function mkGroupList() {
  if (!__mk) return [];
  const live = {};
  __mk.items.forEach((it) => { if (it.g) live[it.g] = 1; });
  return Object.keys(live).map((id) => {
    const m = (__mk.groups || {})[id] || {};
    return { id, n: String(m.n || "Group").slice(0, 48), col: m.col ? 1 : 0 };
  });
}
/* bits 16 and 32 are stored INVERTED, so that every design saved before
   these existed — which has them as 0 — opens with them on */
function mkGuideFlags() {
  const g = __mk.guides || {};
  return (g.grid ? 1 : 0) | (g.mirror ? 4 : 0) | (g.snap ? 8 : 0) |
         (g.align ? 0 : 16) | (g.dims ? 0 : 32);
}
/* anything other than the defaults is a choice the customer made, and a
   choice made before the first stroke has to survive the first stroke */
function mkGuidesChosen() {
  return mkGuideFlags() !== 0 || (Number(__mk.gridN) || 12) !== 12;
}

function mkSnapshot() {
  return JSON.stringify({
    items: __mk.items.map(mkPack),
    crop: __mk.crop, baseRot: __mk.baseRot,
    groups: mkGroupList(),
  });
}
function mkPushUndo() {
  /* every user mutation comes through here, which makes it the honest place
     to record that a traced drawing has actually been touched */
  __mk.dirty = true;
  __mk.undo.push(mkSnapshot());
  if (__mk.undo.length > 60) __mk.undo.shift();
  __mk.redo = [];
}
function mkRestore(snap) {
  const s = JSON.parse(snap);
  __mk.items = s.items.map(mkPack);
  __mk.crop = s.crop || null;
  __mk.baseRot = s.baseRot || 0;
  __mk.groups = {};
  (Array.isArray(s.groups) ? s.groups : []).forEach((g) => {
    if (g && g.id) __mk.groups[String(g.id)] = { n: String(g.n || "Group"), col: g.col ? 1 : 0 };
  });
  __mk.items.forEach((it) => {
    if (it.g && !__mk.groups[it.g]) __mk.groups[it.g] = { n: "Group", col: 0 };
  });
  __mk.sel = -1;
  mkCloseEditor(true);
  mkApplyBase();
  mkTouch();
}

/* ── the chrome that reflects the model ──────────────────────────────────── */
function mkChrome() {
  if (!__mk) return;
  $("#mkUndo").disabled = !__mk.undo.length;
  $("#mkRedo").disabled = !__mk.redo.length;
  const sel = __mk.items[__mk.sel] || null;
  const nsel = mkSelCount();
  $("#mkDelete").disabled = !nsel;

  let marks = 0, notes = 0;
  __mk.items.forEach((it) => {
    if (it.t === "text" || it.t === "callout") notes++; else marks++;
  });
  /* while several things are selected the count line says so — it is the
     one place on screen that is always visible on every layout */
  mkPaintTally();
  $("#markupCount").textContent = nsel > 1
    ? `${nsel} layers selected${mkCanUngroup() ? " · grouped" : ""}`
    : ((marks || notes)
      ? `${marks} mark${marks === 1 ? "" : "s"}${notes ? ` · ${notes} note${notes === 1 ? "" : "s"}` : ""}`
      : (__mk.compose ? "Blank sheet — draw anything" : "No marks yet"));

  /* the property bar shows the SELECTION when there is one, otherwise what
     the tool is about to do — the same contract as a Mac format bar */
  const c  = sel ? sel.c  : __mk.color;
  const f  = sel ? sel.f  : __mk.fill;
  const w  = sel ? sel.w  : __mk.w;
  const ff = sel ? sel.ff : __mk.ff;
  const fs = sel ? sel.fs : __mk.fs;
  /* THE ENGRAVING CONTROL, WHICH IS ALSO THE BUCKET.
     Two of these three say "fill this area", so choosing one of them puts
     the bucket in your hand — and the chip says so by turning its colour
     swatch into a bucket. There is no separate Fill button any more: a tool
     whose only job was to apply one of two settings, sitting next to the
     control that chose the setting, was one control too many. */
  const filling = __mk.tool === "bucket";
  /* WHAT IS IN THE BUCKET, not what is selected. A fill is selected the
     instant it lands, so reading the selection here meant that re-aiming to
     Cut out left the Engrave chip lit and armed — and the next tap put a
     hole through the charm while the bar said "engrave". */
  const intent = mkFillIntent(filling ? __mk.fill : f);
  $$("#mkMetal .mk-metal__b").forEach((b) => {
    const on = b.dataset.fill === intent;
    b.classList.toggle("is-on", on);
    b.setAttribute("aria-checked", on ? "true" : "false");
    /* all three arm — see the control's own note. Outline was excluded here
       too, so even when it WAS holding the bucket the bar refused to say so. */
    const armed = filling && on;
    b.classList.toggle("is-armed", armed);
    const sw = b.querySelector(".mk-metal__sw");
    if (sw) sw.classList.toggle("mk-metal__sw--bucket", armed);
  });
  $("#mkWeightSw").style.height = Math.max(1, Math.round(w * 260)) + "px";
  $$('.mk-drop[data-drop="weight"] .mk-item--w').forEach((b) =>
    b.classList.toggle("is-on", Math.abs(parseFloat(b.dataset.w) - w) < 1e-6));
  $$(".mk-item--font").forEach((b) => b.classList.toggle("is-on", b.dataset.ff === ff));
  $$("#mkSizes .mk-size").forEach((b) => b.classList.toggle("is-on", Math.abs(parseFloat(b.dataset.fs) - fs) < 1e-6));
  $$(".mk-item--tog[data-guide]").forEach((b) => b.classList.toggle("is-on", !!(__mk.guides || {})[b.dataset.guide]));
  const gnEl = $("#mkGridN");
  if (gnEl && document.activeElement !== gnEl) {
    gnEl.value = String(__mk.gridN || 12);
    const gl = $("#mkGridNVal");
    if (gl) gl.textContent = String(__mk.gridN || 12);
  }
  const bmp = $("#mkShowBitmap");
  if (bmp) {
    bmp.classList.toggle("is-on", !!__mk.showBitmap);
    bmp.hidden = !__mk.traced;
  }
  const rt = $("#mkRetrace");
  if (rt) rt.hidden = !!__mk.compose;
  /* the tolerance slider shows the selected picture's own judgement */
  const trow = $("#mkTolRow");
  if (trow) trow.hidden = !(__mk.tool === "wand" || filling);
  const erow = $("#mkEraseRow");
  if (erow) erow.hidden = __mk.tool !== "erase";
  const es = $("#mkEraseSize");
  if (es && document.activeElement !== es) es.value = String(Math.round((__mk.eraseSize || 0.035) * 1000) / 10);
  const ev = $("#mkEraseSizeVal");
  if (ev) ev.textContent = (Math.round((__mk.eraseSize || 0.035) * 1000) / 10).toFixed(1) + "%";
  const wt = $("#mkWandTol");
  if (wt && document.activeElement !== wt) {
    const pic = mkSelItems().filter((o) => o.t === "img")[0] ||
                __mk.items.filter((o) => o.t === "img" && (o.k || []).length).slice(-1)[0];
    const v = filling ? (__mk.tol || MK_WAND_DEFAULT_TOL)
                      : ((pic && pic.kt) || MK_WAND_DEFAULT_TOL);
    wt.value = String(v);
    const lbl = $("#mkWandTolVal");
    if (lbl) lbl.textContent = String(v);
  }
  mkPaintMode();
  mkRenderSelection();
  /* the layer list is the model rendered, so it is refreshed wherever the
     model's chrome is — never on its own timer */
  if (typeof mkLayerChrome === "function") mkLayerChrome();
  if (typeof mkPaintLayerSel === "function") mkPaintLayerSel();
  const lc = $("#mkLayersCount");
  if (lc) lc.textContent = String(__mk.items.length);
}

/* What the workshop has been told, on screen at all times. Three numbers is
   all it takes, and it means nobody discovers on the finished charm that
   they never said which areas were engraved. */
/* Said once, in the one place it matters, the first time somebody opens a
   sheet. Not a modal, not a tour: a small note pinned to the control it is
   about, which goes away the moment they use it or dismiss it. */
function mkMetalCoachSeen() {
  const el = document.getElementById("mkMetalCoach");
  if (el) el.remove();
  if (state.metalCoach) return;
  state.metalCoach = 1;
  saveSession();
}
function mkMetalCoach() {
  if (state.metalCoach) return;
  const host = $("#mkMetal");
  if (!host || document.getElementById("mkMetalCoach")) return;
  const el = document.createElement("div");
  el.className = "mk-coach";
  el.id = "mkMetalCoach";
  el.innerHTML =
    '<b>Start here.</b> This is the one control that changes what the ' +
    'workshop <i>does</i> — engrave an area, cut it clean through, or leave ' +
    'it as an outline. Say it here and the design comes back the way you ' +
    'pictured it.<button type="button" class="mk-coach__x">Got it</button>';
  /* on the body and placed by script: the property row scrolls sideways,
     and a scroll container clips its children on BOTH axes however you write
     overflow — the same reason every menu in here is fixed */
  document.body.appendChild(el);
  const place = () => {
    const r = host.getBoundingClientRect();
    if (!r.width) { el.remove(); return; }
    const w = el.offsetWidth || 320;
    let left = r.left;
    if (left + w > window.innerWidth - 10) left = Math.max(10, window.innerWidth - 10 - w);
    el.style.left = left + "px";
    el.style.top = (r.bottom + 9) + "px";
    el.style.setProperty("--arrow", Math.max(10, Math.min(w - 22, r.left + 26 - left)) + "px");
  };
  place();
  el.querySelector(".mk-coach__x").addEventListener("click", mkMetalCoachSeen);
  window.addEventListener("resize", place);
}

/* Asked ONCE per design, and only when it would actually help: there are
   closed shapes on the sheet and not one of them has been given an
   instruction. Not a gate — the answer "outlines only" is a real answer and
   is taken at face value. */
async function mkMetalCheck(key) {
  if (!__mk) return true;
  const t = mkMetalTally(__mk.items);
  if (t.engrave || t.cutout) return true;         // they have said something
  if (!t.none) return true;                       // nothing shaped to say it about
  if (!state.metalAsked) state.metalAsked = {};
  const k = String(key || __mk.key || "?");
  if (state.metalAsked[k]) return true;
  state.metalAsked[k] = 1;
  saveSession();
  const yes = await askConfirm({
    title: "Nothing is marked to be engraved",
    body: `${t.none} shape${t.none === 1 ? " has" : "s have"} no instruction, so ` +
          "every one of them will come back as an engraved OUTLINE with flat polished " +
          "metal inside. If you want an area filled in — or cut clean through — say so " +
          "with “In metal” first. It is the single biggest difference to what you get back.",
    yes: "Outlines only, go ahead",
    no: "Let me mark it first",
    danger: false,
  });
  return yes;
}

function mkPaintTally() {
  const el = $("#markupMetal");
  if (!el || !__mk) return;
  const t = mkMetalTally(__mk.items);
  const total = t.engrave + t.cutout + t.none;
  if (!total) { el.hidden = true; el.textContent = ""; return; }
  el.hidden = false;
  const bit = (n, cls, word) => n
    ? `<span class="markup-metal__b"><i class="mk-metal__sw mk-metal__sw--${cls}"></i>${n} ${word}</span>` : "";
  el.innerHTML = bit(t.engrave, "engrave", "engraved") +
                 bit(t.cutout, "cutout", "cut out") +
                 bit(t.none, "none", t.none === 1 ? "outline" : "outlines");
}

function mkPaintMode() {
  const mode = (__mk && __mk.mode) || "interpret";
  $$("#markupMode .mk-mode").forEach((b) => {
    const on = b.dataset.mode === mode;
    b.classList.toggle("is-on", on);
    b.setAttribute("aria-checked", on ? "true" : "false");
  });
}

/* ── redraw ──────────────────────────────────────────────────────────────── */
function markupCanvas() { return $("#markupCanvas"); }
function mkRedraw() {
  const cv = markupCanvas(); if (!cv || !__mk) return;
  const ctx = cv.getContext("2d");
  ctx.clearRect(0, 0, cv.width, cv.height);
  const items = __mk.items.slice();
  if (__mk.live) items.push(__mk.live);
  mkPaint(ctx, cv.width, cv.height, items,
          Object.assign({}, __mk.guides || {}, { gridN: __mk.gridN || 12 }));
  mkRenderSelection();
}

/* ── selection furniture: a dashed box, four corners and a rotate grip ───── */
function mkRenderSelection() {
  const host = $("#markupOverlay"); if (!host) return;
  host.querySelectorAll(".mk-sel, .mk-handle, .mk-selone").forEach((e) => e.remove());
  if (!__mk || __mk.tool !== "select" || __mk.cropping) return;
  const list = mkSelItems();
  if (!list.length) return;
  const it = list[list.length - 1];
  const many = list.length > 1;
  /* ONE box around everything selected — the whole point of selecting more
     than one thing is that it then behaves as one thing. Each member also
     gets a thin outline of its own, so it stays obvious WHAT is in the
     selection and not merely how far it reaches. */
  const bb = many ? mkUnionBBox(list) : mkBBox(it);
  const pad = 0.008;
  if (many) {
    list.forEach((o) => {
      const b = mkBBox(o);
      const one = document.createElement("div");
      one.className = "mk-selone";
      one.style.left = (b.x * 100) + "%";
      one.style.top = (b.y * 100) + "%";
      one.style.width = (b.w * 100) + "%";
      one.style.height = (b.h * 100) + "%";
      if (o.r) one.style.transform = `rotate(${o.r}rad)`;
      host.appendChild(one);
    });
  }
  const box = document.createElement("div");
  box.className = "mk-sel" + (many ? " mk-sel--many" : "");
  box.style.left = ((bb.x - pad) * 100) + "%";
  box.style.top = ((bb.y - pad) * 100) + "%";
  box.style.width = ((bb.w + pad * 2) * 100) + "%";
  box.style.height = ((bb.h + pad * 2) * 100) + "%";
  if (!many && it.r) box.style.transform = `rotate(${it.r}rad)`;
  host.appendChild(box);

  const corners = [["nw", 0, 0], ["ne", 1, 0], ["se", 1, 1], ["sw", 0, 1]];
  corners.forEach(([h, fx, fy]) => {
    const el = document.createElement("button");
    el.type = "button";
    el.className = "mk-handle";
    el.dataset.h = h;
    el.style.left = ((bb.x - pad + (bb.w + pad * 2) * fx) * 100) + "%";
    el.style.top = ((bb.y - pad + (bb.h + pad * 2) * fy) * 100) + "%";
    host.appendChild(el);
  });
  const rot = document.createElement("button");
  rot.type = "button";
  rot.className = "mk-handle mk-handle--rot";
  rot.dataset.h = "rot";
  rot.style.left = ((bb.x + bb.w / 2) * 100) + "%";
  rot.style.top = ((bb.y - pad - 0.055) * 100) + "%";
  host.appendChild(rot);
}

/* ── the rubber band ─────────────────────────────────────────────────────
   Dragging across empty sheet with the Select tool sweeps up everything the
   box touches. The other half of "select several at once", and the one most
   people reach for before they think of shift.                            */
function mkMarqRect() {
  const m = __mk && __mk.marq;
  if (!m) return null;
  return { x: Math.min(m.x0, m.x), y: Math.min(m.y0, m.y),
           w: Math.abs(m.x - m.x0), h: Math.abs(m.y - m.y0) };
}
function mkPaintMarquee() {
  const host = $("#markupOverlay"); if (!host) return;
  host.querySelectorAll(".mk-marq").forEach((e) => e.remove());
  const m = __mk && __mk.marq;
  if (!m || !m.live) return;
  const r = mkMarqRect();
  const el = document.createElement("div");
  el.className = "mk-marq";
  el.style.left = (r.x * 100) + "%"; el.style.top = (r.y * 100) + "%";
  el.style.width = (r.w * 100) + "%"; el.style.height = (r.h * 100) + "%";
  host.appendChild(el);
}
function mkMarqueeSelect() {
  const m = __mk && __mk.marq; if (!m) return;
  const r = mkMarqRect();
  const hit = [];
  __mk.items.forEach((it, i) => {
    if (it.hid || it.lok) return;
    const b = mkBBox(it);
    /* touched, not enclosed — sweeping a box across a drawing to catch the
       strokes it crosses is what the hand expects */
    if (b.x < r.x + r.w && b.x + b.w > r.x && b.y < r.y + r.h && b.y + b.h > r.y) {
      mkReachOf(i, false).forEach((k) => { if (hit.indexOf(k) < 0) hit.push(k); });
    }
  });
  if (m.add) { mkSelSet(m.base || []); mkSelAdd(hit); }
  else mkSelSet(hit);
  mkRenderSelection();
  mkPaintLayerSel(true);
  const cnt = $("#markupCount");
  if (cnt) cnt.textContent = hit.length ? `${mkSelCount()} layers selected` : "Drag to select";
}

/* ═══════════ the single text editor ══════════════════════════════════════
   One <textarea>, always the live one, whose value lands in the model on
   every keystroke. A real form control rather than a contenteditable: native
   caret, native mobile keyboard, native IME, and a .value that is never a
   guess about what the DOM currently holds.                                */
function mkOpenEditor(it) {
  mkCloseEditor(true);
  const host = $("#markupOverlay");
  const ta = document.createElement("textarea");
  ta.className = "mk-editor";
  ta.rows = 1;
  ta.value = it.s || "";
  ta.placeholder = it.t === "label" ? "Words on the charm…" : "What should we change here?";
  ta.spellcheck = false;
  ta.maxLength = 240;
  const bb = mkBBox(it);
  const size = (it.t === "label" ? it.fs : Math.max(0.026, it.fs || 0.03));
  ta.style.left = (Math.min(0.72, Math.max(0.01, bb.x)) * 100) + "%";
  ta.style.top  = (Math.min(0.86, Math.max(0.01, bb.y)) * 100) + "%";
  ta.style.width = "28%";
  ta.style.fontFamily = MK_FONTS[it.ff] || MK_FONTS.sans;
  ta.style.fontSize = Math.max(11, size * ($("#markupStage").getBoundingClientRect().width || 400)) + "px";
  host.appendChild(ta);
  __mk.editing = { it, ta };

  const grow = () => { ta.style.height = "auto"; ta.style.height = Math.min(160, ta.scrollHeight) + "px"; };
  /* THE line that fixes note persistence: the model is written on every
     keystroke, and mkTouch writes the session. No blur, no close, no commit
     is ever load-bearing again. */
  ta.addEventListener("input", () => { it.s = ta.value.slice(0, 240); grow(); mkTouch(); });
  ta.addEventListener("keydown", (e) => {
    if (e.key === "Escape") { e.preventDefault(); mkCloseEditor(); }
    if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); mkCloseEditor(); }
    e.stopPropagation();                       /* ⌫ must delete text, not the item */
  });
  /* NO close-on-blur. Focus is a slippery thing to hang an editor's life on:
     a re-render, a mobile keyboard dismissing itself, or a stray blur with no
     new focus target all used to take the editor away mid-word — reopening
     text looked like it simply refused to work. The editor now closes on
     purpose only: Enter, Escape, a click somewhere else, or a tool change.
     Nothing is at risk in the meantime because the model is written on every
     keystroke. */
  grow();
  /* the frame delay matters: pointerdown handlers preventDefault to keep the
     mark where it was clicked, and focus set inside that same tick is undone */
  requestAnimationFrame(() => { try { ta.focus(); ta.select(); } catch (e) {} });
}
/* the click-away that used to be a blur */
(function wireEditorDismiss() {
  document.addEventListener("pointerdown", (e) => {
    if (!__mk || !__mk.editing) return;
    if (e.target.closest(".mk-editor")) return;      /* typing in it */
    if (e.target.closest("#mkCtx")) return;          /* its own menu */
    mkCloseEditor();
  }, true);
})();

function mkCloseEditor(silent) {
  if (!__mk || !__mk.editing) return;
  const { it, ta } = __mk.editing;
  __mk.editing = null;
  it.s = String(ta.value || "").slice(0, 240);
  if (ta.parentNode) ta.parentNode.removeChild(ta);
  /* an empty note is not a note — but it is also not a loss, because nothing
     was ever typed into it */
  if (!it.s.trim()) {
    const i = __mk.items.indexOf(it);
    if (i >= 0) __mk.items.splice(i, 1);
    if (__mk.sel === i) __mk.sel = -1;
  }
  if (!silent) mkTouch();
}

/* ═══════════ zoom & pan ══════════════════════════════════════════════════
   One transform on #markupView — the drawing, the ink canvas and the overlay
   all live inside it, so they can never drift apart. All pointer math reads
   the VIEW's rect, which already reflects the transform, so every tool is
   exact at any zoom.                                                        */
function mkApplyView() {
  const view = $("#markupView");
  if (!view || !__mk) return;
  view.style.transform = `translate(${__mk.px}px, ${__mk.py}px) scale(${__mk.z})`;
  const reset = $("#mkZoomReset");
  if (reset) {
    reset.hidden = __mk.z <= 1.01;
    reset.textContent = "⤾ " + Math.round(__mk.z * 100) + "%";
  }
}
function mkClampView() {
  const r = $("#markupStage").getBoundingClientRect();
  const minX = r.width - r.width * __mk.z, minY = r.height - r.height * __mk.z;
  __mk.px = Math.min(0, Math.max(minX, __mk.px));
  __mk.py = Math.min(0, Math.max(minY, __mk.py));
}
function mkZoomAt(clientX, clientY, factor) {
  const r = $("#markupStage").getBoundingClientRect();
  const cx = clientX - r.left, cy = clientY - r.top;
  const z2 = Math.min(8, Math.max(1, __mk.z * factor));
  if (z2 === __mk.z) return;
  __mk.px = cx - (cx - __mk.px) * (z2 / __mk.z);
  __mk.py = cy - (cy - __mk.py) * (z2 / __mk.z);
  __mk.z = z2;
  if (__mk.z === 1) { __mk.px = 0; __mk.py = 0; }
  mkClampView();
  mkApplyView();
}

/* ── the base layer: rotation, crop and fade, all as one CSS transform ───── */
function mkApplyBase() {
  const img = $("#markupBase"); if (!img || !__mk) return;
  const c = __mk.crop;
  const s = c ? 1 / c.w : 1;
  const tx = c ? -c.x * 100 : 0, ty = c ? -c.y * 100 : 0;
  /* right-to-left: rotate about the centre first, then crop-scale it */
  img.style.transformOrigin = "0 0";
  img.style.transform =
    `scale(${s}) translate(${tx}%, ${ty}%) ` +
    `translate(50%,50%) rotate(${__mk.baseRot || 0}deg) translate(-50%,-50%)`;
  img.style.opacity = String(__mk.baseFade != null ? __mk.baseFade : 1);
  /* THE GHOST RULE. Once the drawing has been taken apart, the vectors ARE
     the drawing — painting the bitmap as well would leave the original eye
     sitting there the moment you moved the traced one. */
  img.hidden = !!__mk.compose || (!!__mk.traced && !__mk.showBitmap);
}

/* ── the tool ────────────────────────────────────────────────────────────── */
function mkSetTool(tool) {
  if (!__mk) return;
  mkCloseEditor();
  /* so that choosing Outline puts the bucket down and hands back whatever
     was in the customer's hand before they reached for it */
  if (tool === "bucket" && __mk.tool && __mk.tool !== "bucket") __mk.lastDrawTool = __mk.tool;
  __mk.tool = tool;
  if (tool !== "select") __mk.sel = -1;
  $$(".mk-tool").forEach((b) => b.classList.toggle("is-on", b.dataset.tool === tool));
  /* a nested menu's button wears the sub-tool it is currently holding */
  const shapesBtn = $("#mkShapes");
  if (MK_SHAPE_TOOLS.indexOf(tool) >= 0) {
    shapesBtn.dataset.tool = tool;
    const src = $('.mk-drop[data-drop="shapes"] .mk-item[data-tool="' + tool + '"]');
    if (src) {
      shapesBtn.querySelector("[data-shapelbl]").textContent = MK_SHAPE_LABEL[tool] || "Shape";
      const ico = src.querySelector("svg");
      const slot = shapesBtn.querySelector("[data-shapeico]");
      if (ico && slot) slot.innerHTML = ico.innerHTML;
    }
  }
  /* the Note button wears whatever it is holding, in the words the menu
     uses — and it holds the signature tool too, which otherwise lit nothing
     on the whole bar while a stroke was being captured as a signature */
  const textBtn = $("#mkText");
  if (textBtn && (MK_TEXT_TOOLS.indexOf(tool) >= 0 || tool === "sign")) {
    textBtn.dataset.tool = tool;
    const lbl = textBtn.querySelector("[data-textlbl]");
    if (lbl) lbl.textContent = tool === "label" ? "Text" : tool === "sign" ? "Sign" : "Note";
    textBtn.classList.add("is-on");
  }
  /* Magic eraser is now a first-class button beside Picture. */
  const magicBtn = $("#mkMagicEraser");
  if (magicBtn) magicBtn.classList.toggle("is-on", tool === "wand");
  const eraseBtn = $("#mkEraser");
  if (eraseBtn) eraseBtn.classList.toggle("is-on", tool === "erase");
  if (tool !== "erase") mkEraserCursorClear();
  if (tool !== "bucket") mkFillPreviewClear();
  /* the bucket's hint names the instruction it is about to give, because
     "the area that will be engraved" is a lie while Cut out is in hand */
  $("#markupHint").textContent = tool === "bucket"
    ? (mkFillIntent(__mk.fill) === "cutout"
        ? "Move over the sheet — the area that will be CUT CLEAN THROUGH lights up. Click to take it, click again to undo"
        : "Move over the sheet — the area that will be ENGRAVED lights up. Click to take it, click again to undo")
    : (MK_HINTS[tool] || "");
  $("#markupStage").dataset.tool = tool;
  mkChrome();
}

/* ── mutations the property bar makes ────────────────────────────────────── */
function mkApplyProp(patch) {
  if (!__mk) return;
  /* a colour or a weight chosen while several layers are selected changes
     all of them — anything else would make multi-selection decorative */
  const list = mkSelItems();
  if (list.length) { mkPushUndo(); list.forEach((o) => Object.assign(o, patch)); }
  if (patch.c  != null) __mk.color = patch.c;
  if (patch.f  != null) __mk.fill  = patch.f;
  if (patch.w  != null) __mk.w     = patch.w;
  if (patch.ff != null) __mk.ff    = patch.ff;
  if (patch.fs != null) __mk.fs    = patch.fs;
  mkTouch();
}

function mkArrange(what) {
  if (!__mk) return;
  const canvasOp = what === "canvasL" || what === "canvasR" || what === "canvasFlip";
  const idxs = mkSelIdx();
  const list = idxs.map((i) => __mk.items[i]);
  if (!list.length && !canvasOp) { toast("Right-click an object to act on it", "err"); return; }
  mkPushUndo();
  const bb = list.length ? mkUnionBBox(list) : null;

  if (what === "rotL" || what === "rotR") {
    /* several objects turn about the SELECTION's centre, one about its own —
       which is the same thing when the selection is one object */
    const dr = (what === "rotL" ? -1 : 1) * Math.PI / 12;
    const cx = bb.x + bb.w / 2, cy = bb.y + bb.h / 2;
    const c = Math.cos(dr), sn = Math.sin(dr);
    list.forEach((it) => {
      it.r = (it.r || 0) + dr;
      if (list.length > 1) {
        const p = it.p || [];
        for (let k = 0; k + 1 < p.length; k += 2) {
          const dx = p[k] - cx, dy = p[k + 1] - cy;
          p[k] = cx + dx * c - dy * sn;
          p[k + 1] = cy + dx * sn + dy * c;
        }
      }
    });
  } else if (what === "flipH" || what === "flipV") {
    list.forEach((it) => {
      const p = it.p || [];
      for (let k = 0; k + 1 < p.length; k += 2) {
        if (what === "flipH") p[k] = bb.x * 2 + bb.w - p[k];
        else p[k + 1] = bb.y * 2 + bb.h - p[k + 1];
      }
    });
  } else if (what === "front" || what === "back") {
    /* by block, so a group arrives at the front intact */
    const blocks = mkBlocks();
    const mine = [], rest = [];
    blocks.forEach((b) => (b.idx.some((i) => idxs.indexOf(i) >= 0) ? mine : rest).push(b));
    mkApplyBlocks(what === "front" ? rest.concat(mine) : mine.concat(rest));
    __mk.selIds = list.map((o) => o.id);
  } else if (what === "dup") {
    mkLayerAct("dup");
    return;                                  /* it touches and repaints itself */
  } else if (canvasOp) {
    /* the whole sheet turns: every point AND the drawing underneath */
    __mk.items.forEach((o) => {
      const p = o.p || [];
      for (let k = 0; k + 1 < p.length; k += 2) {
        const x = p[k], y = p[k + 1];
        if (what === "canvasL")      { p[k] = y;     p[k + 1] = 1 - x; }
        else if (what === "canvasR") { p[k] = 1 - y; p[k + 1] = x; }
        else                          { p[k] = 1 - x; }
      }
      if (what !== "canvasFlip") o.r = (o.r || 0) + (what === "canvasL" ? -Math.PI / 2 : Math.PI / 2);
    });
    if (what === "canvasL") __mk.baseRot = ((__mk.baseRot || 0) - 90 + 360) % 360;
    else if (what === "canvasR") __mk.baseRot = ((__mk.baseRot || 0) + 90) % 360;
    else __mk.baseFlip = !__mk.baseFlip;
    mkApplyBase();
  }
  mkTouch();
}

function mkDeleteSelected() {
  if (!__mk) return;
  const idxs = mkSelIdx();
  if (!idxs.length) return;
  mkPushUndo();
  for (let k = idxs.length - 1; k >= 0; k--) __mk.items.splice(idxs[k], 1);
  mkSelSet([]);
  mkTouch();
}

/* ═══════════ crop ════════════════════════════════════════════════════════
   Square by construction, because the stage is square and every mark is
   stored normalized against it — a rectangular crop would put every existing
   mark somewhere other than where it was drawn.                             */
/* ═══════════ THE REGION ENGINE ═══════════════════════════════════════════
   One question, asked once, however the boundary happens to be made.

   "What area am I pointing at?" has three plausible answers in a studio like
   this one, and every editor that makes the customer CHOOSE between them is
   making them do the software's job:

     · an enclosed piece of paper, walled in by ink — and the ink may come
       from one object, from two grouped objects, from twelve ungrouped
       ones, or from the black in an imported picture. It makes no
       difference: the wall is the wall.
     · a region of one colour INSIDE a picture — the magic wand's question,
       which is the right one when the finger is on a photograph or a logo
       rather than on paper.
     · nothing at all, because the finger is on a line.

   So the engine works on the COMPOSED SHEET — everything visible, in the
   order it is painted — decides for itself which of the first two questions
   is being asked, and refuses to accept the third: land on a line and it
   quietly steps off it to the nearest fillable pixel rather than telling
   somebody to aim better.

   That composed sheet is computed once and cached, so the region under the
   cursor can be recomputed on every mouse move without costing anything —
   which is what makes the live preview possible, and the live preview is
   what makes this feature impossible to get wrong.
   ====================================================================== */
const MK_FILL_TOL = 0.016;            /* the gap a hand is forgiven, of sheet */
/* TWO RESOLUTIONS, AND THE REASON FOR BOTH.

   The preview redraws on every mouse move, so it runs at a size a flood fill
   can finish inside one animation frame. The COMMITTED fill runs once, when
   the customer clicks, and there is no reason on earth for it to be as
   coarse as the preview — that is what made a filled logo come out as a
   rough polygon in the shape of the thing rather than the thing. So the
   commit re-floods at the magic eraser's own working size, and the contour
   that gets stored is traced from THAT. Same engine, same answer, one of
   them simply drawn at the resolution the result deserves. */
const MK_REG_S  = 520;                /* the preview: still fast, more reliable for tiny enclosed areas */
const MK_FINE_S = 1400;               /* the commit: the eraser's own fidelity */
const MK_REG_CACHE = new Map();       /* size → the composed sheet at that size */

/* everything visible, painted flat, with a note of WHICH object owns each
   inked pixel — that ownership is what lets a fill know which group it
   belongs to and what it must sit on top of */
function mkRegionSheet(size) {
  if (!__mk) return null;
  const S = Math.max(64, Math.round(Number(size) || MK_REG_S));
  const vis = __mk.items.filter((it) => !it.hid);
  const sig = vis.map((it) => it.id + ":" + (it.p || []).length + ":" + it.f + ":" + it.o +
                              ":" + (it.k || []).length + ":" + (it.m || []).length).join("|") +
              "#" + (__mk.baseRef && !__mk.traced ? __mk.baseRef.u : "") + "#" + S;
  const MK_REG = MK_REG_CACHE.get(S) || { sig: "", data: null, S };
  MK_REG_CACHE.set(S, MK_REG);
  if (MK_REG.sig === sig && MK_REG.data) return MK_REG;

  const paint = (list, withBase) => {
    const cv = document.createElement("canvas");
    cv.width = S; cv.height = S;
    const ctx = cv.getContext("2d", { willReadFrequently: true });
    ctx.fillStyle = "#ffffff";
    ctx.fillRect(0, 0, S, S);
    if (withBase && __mk.baseRef && !__mk.traced) {
      const img = mkImageFor(__mk.baseRef);
      if (img && img.width) { try { ctx.drawImage(img, 0, 0, S, S); } catch (e) {} }
    }
    try { mkPaint(ctx, S, S, list, { export: true }); } catch (e) {}
    try { return ctx.getImageData(0, 0, S, S); } catch (e) { return null; }
  };
  /* the widened copy is what forgives a hand: a sketched loop almost never
     quite closes, so every stroke is fattened by the gap tolerance before
     the flood runs — the gap seals itself and the fill stays inside */
  const widened = vis.map((it) => {
    const c = mkPack(it);
    c.o = 1;
    c.w = (c.w || 0.008) + MK_FILL_TOL;
    c.__wide = 1;
    c.u = it.u; c.sp = it.sp;      /* transient copies keep their source */
    c.k = it.k; c.kt = it.kt;      /* …and their cut-away background */
    return c;
  });
  const data = paint(widened, true) || paint(widened.filter((i) => i.t !== "img"), true) ||
               paint(widened.filter((i) => i.t !== "img"), false);
  if (!data) return null;

  /* the true colours too, unwidened — the wand's question is about what the
     picture actually looks like, not about a fattened stencil of it */
  const trueData = paint(vis.map((it) => { const c = mkPack(it); c.u = it.u; c.sp = it.sp; c.k = it.k; c.kt = it.kt; return c; }), true) || data;

  /* THE VOID, once. Paper connected to the edge of the sheet is the space
     around the charm — computed here rather than guessed at per click, and
     used for two things: refusing to flood it, and knowing which way to
     step when a finger lands on a line. */
  const d = data.data;
  const td = trueData.data;
  const N = S * S;
  /* TWO VOIDS, NOT ONE.

     `outside` is the space around the charm as the SEALED sheet sees it, and
     that is the right answer for "may I flood here". But the mask is later
     grown back out against the sheet's TRUE colours, and paper that is
     hidden underneath a fattened stroke is not marked void — so growing
     against `outside` alone let a fill seep through the wall it was supposed
     to stop at, by half the fattening, which made the leak depend on line
     weight all over again. `outsideTrue` is the same flood run on the
     unwidened sheet: it knows the outside as the artwork really draws it,
     and it is what the grow is allowed to see. */
  const flood = (px) => {
    const out = new Uint8Array(N);
    const st = [];
    const seed = (i) => {
      const j = i * 4;
      if (out[i] || px[j] + px[j + 1] + px[j + 2] <= 690) return;
      out[i] = 1; st.push(i);
    };
    for (let x = 0; x < S; x++) { seed(x); seed((S - 1) * S + x); }
    for (let y = 0; y < S; y++) { seed(y * S); seed(y * S + S - 1); }
    while (st.length) {
      const i = st.pop();
      const cx = i % S, cy = (i / S) | 0;
      if (cx > 0) seed(i - 1);
      if (cx < S - 1) seed(i + 1);
      if (cy > 0) seed(i - S);
      if (cy < S - 1) seed(i + S);
    }
    return out;
  };
  const outside = flood(d);
  const outsideTrue = flood(td);

  MK_REG.sig = sig;
  MK_REG.S = S;
  MK_REG.data = d;
  MK_REG.truth = trueData.data;
  MK_REG.outside = outside;
  MK_REG.outsideTrue = outsideTrue;
  MK_REG.items = vis;
  return MK_REG;
}
/* every mutation invalidates it; recomputing is cheap and being wrong is not */
function mkRegionForget() { MK_REG_CACHE.clear(); }

/* the topmost visible picture whose drawn area contains this point */
function mkRegionPictureAt(x, y) {
  if (!__mk) return -1;
  for (let i = __mk.items.length - 1; i >= 0; i--) {
    const it = __mk.items[i];
    if (it.t !== "img" || it.hid) continue;
    if (mkPictureLocal(it, x, y)) return i;
  }
  return -1;
}

/* THE ANSWER. Returns the mask of the area under (x, y), what kind of area
   it is, how big it is, and which objects wall it in — or null if there is
   genuinely nothing there. */
function mkRegionAt(x, y, opts) {
  const o = opts || {};
  const sheet = mkRegionSheet(o.S || MK_REG_S);
  if (!sheet) return null;
  const S = sheet.S, d = sheet.data, truth = sheet.truth;
  const N = S * S;

  const isPaper = (i) => { const j = i * 4; return d[j] + d[j + 1] + d[j + 2] > 690; };
  let sx = Math.min(S - 1, Math.max(0, Math.round(x * S)));
  let sy = Math.min(S - 1, Math.max(0, Math.round(y * S)));
  let si = sy * S + sx;

  /* IS THE FINGER ON A PICTURE? Then the question is about colour, not
     about paper — filling part of a logo means "this shape here", and the
     shape is bounded by where the colour changes. */
  const picIdx = mkRegionPictureAt(x, y);
  const onPicture = picIdx >= 0 && !isPaper(si);

  let open;
  if (onPicture) {
    const pic = __mk.items[picIdx];
    const tol = Math.max(2, Math.min(70, Number(o.tol) || Number(__mk.tol) ||
                                          Number(pic && pic.kt) || MK_WAND_DEFAULT_TOL));
    const lim = (tol / 100) * 441.673;
    const j0 = si * 4;
    const r0 = truth[j0], g0 = truth[j0 + 1], b0 = truth[j0 + 2];
    open = new Uint8Array(N);
    for (let i = 0; i < N; i++) {
      const j = i * 4;
      const dr = truth[j] - r0, dg = truth[j + 1] - g0, db = truth[j + 2] - b0;
      open[i] = (dr * dr + dg * dg + db * db) <= lim * lim ? 1 : 0;
    }
  } else {
    open = new Uint8Array(N);
    for (let i = 0; i < N; i++) open[i] = isPaper(i) ? 1 : 0;
    /* ON A LINE? Step off it — and step INWARDS. Nobody should be told to
       aim better at a stroke they can barely see, and nobody who clicks the
       edge of a charm means the empty space beyond it. So the search prefers
       enclosed paper and only falls back to the void if there is genuinely
       nothing else within reach. */
    if (!open[si]) {
      const outs = sheet.outside;
      let best = -1, bestD = 1e9, any = -1, anyD = 1e9;
      const R = Math.max(4, Math.round(S * 0.04));
      for (let dy = -R; dy <= R; dy++) {
        const ny = sy + dy;
        if (ny < 0 || ny >= S) continue;
        for (let dx = -R; dx <= R; dx++) {
          const nx = sx + dx;
          if (nx < 0 || nx >= S) continue;
          const q = ny * S + nx;
          if (!open[q]) continue;
          const dd = dx * dx + dy * dy;
          if (dd < anyD) { anyD = dd; any = q; }
          if (outs && outs[q]) continue;
          if (dd < bestD) { bestD = dd; best = q; }
        }
      }
      let pick = best >= 0 ? best : any;
      /* ── A CORRIDOR THE SEALING SWALLOWED WHOLE ─────────────────────────
         The sealed sheet fattens every stroke so a nearly-closed loop counts
         as closed — but a genuinely narrow slot between two objects can be
         narrower than the fattening itself, and then there is NO open pixel
         to step to: the hover finds nothing in a place the eye can plainly
         see paper. When that happens, and the spot truly is paper (by the
         sheet's own unfattened colours) and not the void, the flood runs on
         the TRUE paper instead. It stops at real ink — the same evidence the
         magic eraser uses — so it is exact; and the background guard below
         still refuses it if the slot turns out to leak off the sheet. */
      if (pick < 0) {
        const jt = si * 4;
        const truePaper = truth[jt] + truth[jt + 1] + truth[jt + 2] > 690;
        const inVoid = sheet.outsideTrue && sheet.outsideTrue[si];
        if (truePaper && !inVoid) {
          for (let i = 0; i < N; i++) {
            const j = i * 4;
            open[i] = (truth[j] + truth[j + 1] + truth[j + 2] > 690 &&
                       !(sheet.outsideTrue && sheet.outsideTrue[i])) ? 1 : 0;
          }
          pick = si;
        }
      }
      if (pick < 0) return null;
      si = pick; sx = si % S; sy = (si / S) | 0;
    }
  }
  if (!open[si]) return null;

  /* the flood */
  const mask = new Uint8Array(N);
  const stack = [si];
  mask[si] = 1;
  let area = 0, x0 = S, y0 = S, x1 = -1, y1 = -1;
  let touchesEdge = false;
  while (stack.length) {
    const i = stack.pop();
    area++;
    const cx = i % S, cy = (i / S) | 0;
    if (cx === 0 || cy === 0 || cx === S - 1 || cy === S - 1) touchesEdge = true;
    if (cx < x0) x0 = cx;
    if (cx > x1) x1 = cx;
    if (cy < y0) y0 = cy;
    if (cy > y1) y1 = cy;
    if (cx > 0 && open[i - 1] && !mask[i - 1]) { mask[i - 1] = 1; stack.push(i - 1); }
    if (cx < S - 1 && open[i + 1] && !mask[i + 1]) { mask[i + 1] = 1; stack.push(i + 1); }
    if (cy > 0 && open[i - S] && !mask[i - S]) { mask[i - S] = 1; stack.push(i - S); }
    if (cy < S - 1 && open[i + S] && !mask[i + S]) { mask[i + S] = 1; stack.push(i + S); }
  }
  /* ── AND NOW GIVE BACK WHAT THE SEALING TOOK ─────────────────────────
     The flood above ran on a sheet where every stroke was FATTENED, because
     that is the only way a hand-drawn loop that does not quite close still
     counts as closed. But fattening the walls shrinks the room: the mask
     stops short of the real edge by however wide the tolerance made that
     line, and — worse — by MORE when the line is thicker. That is exactly
     the bug where changing the line Weight changed how accurate the fill
     looked, and it is why a filled logo came back a size too small and
     needed a fat outline painted round it to hide the gap.

     So the mask is now grown back out, one pixel at a time, into anything
     the sheet's TRUE colours say is still open. It stops dead at real ink.
     The result is the exact contour — the same boundary the magic eraser
     finds, because it is the same evidence — and it no longer depends in
     any way on how thick the lines around it happen to be. */
  const seedInVoid = !!(sheet.outside && sheet.outside[si]);
  if (!onPicture && !seedInVoid) {
    const grow = Math.ceil(MK_FILL_TOL * S) + 2;
    let frontier = [];
    for (let i = 0; i < N; i++) if (mask[i]) frontier.push(i);
    const isTruePaper = (i) => {
      const j = i * 4;
      return truth[j] + truth[j + 1] + truth[j + 2] > 690;
    };
    for (let step = 0; step < grow && frontier.length; step++) {
      const next = [];
      for (let n = 0; n < frontier.length; n++) {
        const i = frontier[n];
        const cx = i % S, cy = (i / S) | 0;
        const tryOne = (q) => {
          if (mask[q] || !isTruePaper(q)) return;
          /* never leak into the void around the charm. Tested against the
             TRUE outside, not the sealed one: paper hidden under a fattened
             stroke is not void, and treating it as fillable let the fill
             seep a fraction of the line weight past its own wall. */
          if (sheet.outsideTrue && sheet.outsideTrue[q]) return;
          if (sheet.outside && sheet.outside[q] && !seedInVoid) return;
          mask[q] = 1; area++; next.push(q);
          /* the grow can reach the sheet's border too, and if it does this
             region really is open — the guard below reads this flag */
          const qx = q % S;
          if (qx === 0 || qx === S - 1) touchesEdge = true;
          if ((q / S | 0) === 0 || (q / S | 0) === S - 1) touchesEdge = true;
          if (qx < x0) x0 = qx;
          if (q % S > x1) x1 = q % S;
          const qy = (q / S) | 0;
          if (qy < y0) y0 = qy;
          if (qy > y1) y1 = qy;
        };
        if (cx > 0) tryOne(i - 1);
        if (cx < S - 1) tryOne(i + 1);
        if (cy > 0) tryOne(i - S);
        if (cy < S - 1) tryOne(i + S);
      }
      frontier = next;
    }
  }

  /* THE BACKGROUND, DEFINED PROPERLY. Paper that reaches the edge of the
     sheet is the space AROUND the charm, not an area of it — and that is a
     fact about the region's shape rather than a guess about its size, so a
     charm that fills 95% of the sheet still fills correctly and a thin
     sliver of background is still refused. A region of a PICTURE is allowed
     to reach the edge: a photograph's sky legitimately does. */
  const openRegion = onPicture ? (area > N * 0.94) : (touchesEdge || seedInVoid);

  /* WHO WALLS IT IN. Every object whose own ink touches the edge of this
     region is a boundary of it — which tells the fill where in the stack it
     has to sit, and which group it belongs to. */
  /* WHO WALLS IT IN is asked by painting every candidate object alone at
     the working size, so at the commit resolution that is a 1200×1200
     canvas per candidate — and the answer is a list of object indices, which
     does not get any truer for being computed nine times larger. The fine
     pass therefore skips it and inherits the coarse pass's answer. */
  const walls = o.noWalls ? [] : mkRegionWalls(mask, S, x0, y0, x1, y1, onPicture ? picIdx : -1);

  return {
    S, mask, area, openRegion, onPicture,
    picIdx: onPicture ? picIdx : -1,
    frac: area / N,
    box: { x0, y0, x1, y1 },
    seed: [sx / S, sy / S],
    walls,
  };
}

/* which objects form the edge of a region. Asked by painting each candidate
   ALONE and seeing whether its ink lands on the region's rim — exact, and
   cheap because only the objects whose boxes overlap are even considered. */
function mkRegionWalls(mask, S, x0, y0, x1, y1, picIdx) {
  const out = [];
  if (!__mk) return out;
  /* the rim: every pixel just outside the region */
  const rim = [];
  for (let y = Math.max(0, y0 - 2); y <= Math.min(S - 1, y1 + 2); y++) {
    for (let x = Math.max(0, x0 - 2); x <= Math.min(S - 1, x1 + 2); x++) {
      const i = y * S + x;
      if (mask[i]) continue;
      if ((x > 0 && mask[i - 1]) || (x < S - 1 && mask[i + 1]) ||
          (y > 0 && mask[i - S]) || (y < S - 1 && mask[i + S])) rim.push(i);
    }
  }
  if (!rim.length) return out;
  const rx0 = (x0 - 3) / S, ry0 = (y0 - 3) / S, rx1 = (x1 + 3) / S, ry1 = (y1 + 3) / S;
  __mk.items.forEach((it, idx) => {
    if (it.hid || idx === picIdx) return;
    const bb = mkBBox(it);
    if (bb.x > rx1 || bb.x + bb.w < rx0 || bb.y > ry1 || bb.y + bb.h < ry0) return;
    const cv = document.createElement("canvas");
    cv.width = S; cv.height = S;
    const ctx = cv.getContext("2d", { willReadFrequently: true });
    ctx.fillStyle = "#ffffff";
    ctx.fillRect(0, 0, S, S);
    const one = mkPack(it);
    one.o = 1; one.w = (one.w || 0.008) + MK_FILL_TOL;
    one.u = it.u; one.sp = it.sp; one.k = it.k; one.kt = it.kt;
    try { mkPaint(ctx, S, S, [one], { export: true }); } catch (e) { return; }
    let px;
    try { px = ctx.getImageData(0, 0, S, S).data; } catch (e) { return; }
    let hits = 0;
    for (let n = 0; n < rim.length; n++) {
      const j = rim[n] * 4;
      if (px[j] + px[j + 1] + px[j + 2] <= 690) { hits++; if (hits > 3) break; }
    }
    if (hits > 3) out.push(idx);
  });
  if (picIdx >= 0) out.push(picIdx);
  return out;
}

/* ═══════════ THE PREVIEW ═════════════════════════════════════════════════
   The whole feature, really.

   Every flood-fill tool ever shipped asks the customer to click and find
   out. That is why a fill that lands somewhere unexpected reads as "nothing
   happened" — there was never any way to know what was about to happen.

   So the region under the cursor is computed as the cursor moves and shown,
   tinted, before anything is committed. What you see is exactly the area
   that will be engraved: not an approximation of it, not a highlight of the
   object it belongs to — the actual mask, the actual pixels. Aiming becomes
   obvious, the tolerance becomes something you can SEE rather than guess,
   and the click at the end can hold no surprises.

   It costs one flood fill per animation frame at 400×400 over a cached
   sheet, which is a fraction of a millisecond. */
let __mkPrev = { key: "", frame: 0, at: null, cv: null };

function mkFillPreviewClear() {
  const host = $("#markupOverlay");
  if (host) host.querySelectorAll(".mk-prevfill, .mk-prevtag").forEach((e) => e.remove());
  __mkPrev.key = "";
  __mkPrev.at = null;
}
function mkFillPreviewAt(x, y) {
  if (!__mk || __mk.tool !== "bucket" || __mk.cropping) { mkFillPreviewClear(); return; }
  __mkPrev.at = [x, y];
  if (__mkPrev.frame) return;
  __mkPrev.frame = requestAnimationFrame(() => {
    __mkPrev.frame = 0;
    const p = __mkPrev.at;
    if (!p || !__mk || __mk.tool !== "bucket") return;
    mkFillPreviewPaint(p[0], p[1]);
  });
}
function mkRegionNear(x, y, opts) {
  let best = null;
  const rings = [0.0025, 0.0045, 0.0075, 0.011];
  const dirs = 16;
  for (let r = 0; r < rings.length; r++) {
    const rad = rings[r];
    for (let i = 0; i < dirs; i++) {
      const a = (Math.PI * 2 * i) / dirs;
      const nx = Math.max(0, Math.min(0.999999, x + Math.cos(a) * rad));
      const ny = Math.max(0, Math.min(0.999999, y + Math.sin(a) * rad));
      const reg = mkRegionAt(nx, ny, opts);
      if (!reg || reg.openRegion) continue;
      const dx = nx - x, dy = ny - y;
      const cand = { reg, d2: dx * dx + dy * dy, area: reg.area || 0 };
      if (!best || cand.d2 < best.d2 - 1e-9 ||
          (Math.abs(cand.d2 - best.d2) < 1e-9 && cand.area < best.area)) best = cand;
    }
    if (best) break;
  }
  return best ? best.reg : null;
}

function mkFillPreviewPaint(x, y) {
  const host = $("#markupOverlay");
  if (!host) return;
  /* all three instructions preview the same way now — Outline included */
  const want = mkFillIntent(__mk.fill);

  /* over an existing fill the preview says what the click will UNDO, which
     is the other half of the one-gesture rule */
  const hit = mkFillUnderPoint(x, y);
  if (hit >= 0) {
    const it = __mk.items[hit];
    const same = mkFillIntent(it.f) === want;
    mkPreviewShow(null, mkBBox(it),
      same ? (it.bk ? "Click to take this back"
                    : "Click to leave “" + (mkLayerName(it) || "this") + "” as an outline")
           : "Click to make this " + (want === "cutout" ? "a cut-out"
                                    : want === "none" ? "outline only" : "engraved"),
      same ? "undo" : want);
    return;
  }

  let reg = mkRegionAt(x, y);
  if (!reg || reg.openRegion) reg = mkRegionNear(x, y);
  if (!reg || reg.openRegion) {
    mkFillPreviewClear();
    if (reg && reg.openRegion) mkPreviewTag("That's the space around the charm");
    return;
  }
  const key = reg.seed.join(",") + ":" + reg.area + ":" + want;
  if (key === __mkPrev.key) return;
  __mkPrev.key = key;

  /* ── THE MASK, DRAWN AS A SHAPE RATHER THAN AS ITS PIXELS ────────────────
     This used to putImageData the raw 400×400 flood mask and let the browser
     scale it up, which is exactly what "pixelated around the edges" looks
     like: every curve a staircase of 3-pixel steps. The commit has always
     vectorised the same mask through marching squares and drawn a true
     contour; the preview now does what the commit does — trace the mask,
     shave the pixel-grid staircase off with a sub-pixel simplify, and let
     the canvas draw one antialiased path at double resolution. What lights
     up is the same smooth boundary the click will produce. */
  const S = reg.S;
  const P = S * 2;
  const cv = document.createElement("canvas");
  cv.width = P; cv.height = P;
  const ctx = cv.getContext("2d");
  const tint = want === "cutout" ? "rgba(31,111,224," : want === "none" ? "rgba(216,31,42," : "rgba(28,29,29,";
  let vector = false;
  try {
    const cs = mkTraceMask(reg.mask, S).filter((c) => c.length >= 8)
      .map((c) => mkRdp(c, 0.9));                 /* < half a mask pixel */
    if (cs.length) {
      const path = new Path2D();
      const k = P / S;
      cs.forEach((c) => {
        path.moveTo(c[0] * k, c[1] * k);
        for (let i = 2; i < c.length; i += 2) path.lineTo(c[i] * k, c[i + 1] * k);
        path.closePath();
      });
      if (want === "none") {
        /* the third instruction previews as what it does: the boundary lit
           red, the inside left alone */
        ctx.strokeStyle = tint + "0.95)";
        ctx.lineWidth = Math.max(2.5, P * 0.006);
        ctx.lineJoin = "round";
        ctx.stroke(path);
        ctx.fillStyle = tint + "0.10)";
        ctx.fill(path, "evenodd");
      } else {
        ctx.fillStyle = tint + "0.46)";
        ctx.fill(path, "evenodd");
        ctx.strokeStyle = tint + "0.9)";
        ctx.lineWidth = Math.max(1.5, P * 0.003);
        ctx.lineJoin = "round";
        ctx.stroke(path);
      }
      vector = true;
    }
  } catch (e) { /* the bitmap below is a real answer */ }
  if (!vector) {
    const img = ctx.createImageData(S, S);
    const d = img.data;
    const rgb = want === "cutout" ? [31, 111, 224] : want === "none" ? [216, 31, 42] : [28, 29, 29];
    for (let i = 0; i < reg.mask.length; i++) {
      if (!reg.mask[i]) continue;
      const j = i * 4;
      d[j] = rgb[0]; d[j + 1] = rgb[1]; d[j + 2] = rgb[2]; d[j + 3] = 118;
    }
    const half = document.createElement("canvas");
    half.width = S; half.height = S;
    half.getContext("2d").putImageData(img, 0, 0);
    ctx.drawImage(half, 0, 0, P, P);
  }
  const pct = reg.frac * 100;
  mkPreviewShow(cv, null,
    (reg.onPicture ? "Colour area" : "Enclosed area") + " · " +
    (pct < 1 ? "<1" : Math.round(pct)) + "% of the sheet — click to " +
    (want === "cutout" ? "cut it through" : want === "none" ? "outline it" : "engrave it"), want);
}
function mkPreviewShow(canvas, box, label, kind) {
  const host = $("#markupOverlay");
  host.querySelectorAll(".mk-prevfill, .mk-prevtag").forEach((e) => e.remove());
  if (canvas) {
    const el = document.createElement("img");
    el.className = "mk-prevfill";
    el.src = canvas.toDataURL("image/png");
    host.appendChild(el);
  } else if (box) {
    const el = document.createElement("div");
    el.className = "mk-prevfill mk-prevfill--box mk-prevfill--" + (kind || "engrave");
    el.style.left = (box.x * 100) + "%";
    el.style.top = (box.y * 100) + "%";
    el.style.width = (box.w * 100) + "%";
    el.style.height = (box.h * 100) + "%";
    host.appendChild(el);
  }
  mkPreviewTag(label, kind);
}
function mkPreviewTag(label, kind) {
  const host = $("#markupOverlay");
  host.querySelectorAll(".mk-prevtag").forEach((e) => e.remove());
  if (!label) return;
  const tag = document.createElement("div");
  tag.className = "mk-prevtag mk-prevtag--" + (kind || "engrave");
  tag.textContent = label;
  host.appendChild(tag);
}

/* ═══════════ TURNING A REGION INTO A LAYER ═══════════════════════════════
   A region has to become an object: something in the layer list, something
   you can select, move, hide, rename and delete. Two ways to write one down,
   and the engine picks whichever tells the truth:

     · CONTOURS, while they stay compact. A ring between two circles is
       eighty numbers and stays a true vector — infinitely scalable, and
       exactly the same object the rest of the studio already understands.
     · A RUN-LENGTH MASK, when they do not. The inside of a photograph does
       not have a tidy outline, and the old code answered that by raising the
       simplification tolerance until the shape survived the byte budget —
       which is how a carefully aimed fill came back as an unrecognisable
       blob, or as nothing at all. A mask cannot lose detail: it IS the
       preview, written down.

   Either way the item carries a bounding box in `p`, so a masked fill moves,
   scales and rotates exactly like every other layer. */
const MK_RLE_MAX = 9000;          /* numbers; beyond this the mask is coarsened */

function mkMaskCrop(mask, S, box) {
  const x0 = Math.max(0, box.x0), y0 = Math.max(0, box.y0);
  const w = Math.min(S - 1, box.x1) - x0 + 1, h = Math.min(S - 1, box.y1) - y0 + 1;
  const out = new Uint8Array(Math.max(1, w * h));
  for (let y = 0; y < h; y++) {
    for (let x = 0; x < w; x++) out[y * w + x] = mask[(y + y0) * S + (x + x0)];
  }
  return { m: out, w: Math.max(1, w), h: Math.max(1, h) };
}
/* alternating run lengths, starting with OFF, row-major. Flat integers, so
   Firestore is happy and a diff of two fills is readable by eye. */
function mkRleEncode(m, w, h) {
  const runs = [];
  let cur = 0, n = 0;
  for (let i = 0, N = w * h; i < N; i++) {
    const v = m[i] ? 1 : 0;
    if (v === cur) { n++; continue; }
    runs.push(n); cur = v; n = 1;
  }
  runs.push(n);
  return runs;
}
function mkRleDecode(runs, w, h) {
  const m = new Uint8Array(w * h);
  let i = 0, v = 0;
  for (let k = 0; k < runs.length; k++) {
    const n = Math.max(0, Number(runs[k]) || 0);
    if (v) for (let j = 0; j < n && i + j < m.length; j++) m[i + j] = 1;
    i += n; v = v ? 0 : 1;
    if (i >= m.length) break;
  }
  return m;
}
/* halve a mask, keeping any pixel that was mostly on — used only when a
   region is so intricate that its runs would outgrow the document */
function mkMaskHalve(m, w, h) {
  const W = Math.max(1, w >> 1), H = Math.max(1, h >> 1);
  const o = new Uint8Array(W * H);
  for (let y = 0; y < H; y++) {
    for (let x = 0; x < W; x++) {
      const a = m[(y * 2) * w + x * 2] || 0;
      const b = m[(y * 2) * w + Math.min(w - 1, x * 2 + 1)] || 0;
      const c = m[Math.min(h - 1, y * 2 + 1) * w + x * 2] || 0;
      const d = m[Math.min(h - 1, y * 2 + 1) * w + Math.min(w - 1, x * 2 + 1)] || 0;
      o[y * W + x] = (a + b + c + d) >= 2 ? 1 : 0;
    }
  }
  return { m: o, w: W, h: H };
}

/* how many coordinate numbers a single fill may spend. A session is one
   Firestore document with a 1 MB ceiling, and a fill is one object among
   many — but 1,400 numbers was far too mean for a traced logo, and meanness
   is precisely what made the shape come back as a polygon roughly the right
   size. At five decimal places this is about 35 KB for the very worst case
   and a few hundred bytes for an ordinary one. */
const MK_FILL_MAX_PTS = 5200;

function mkRegionToItem(reg, colour, name) {
  if (!reg) return null;
  const S = reg.S;

  /* try the vector first: it is the better object in every way when it fits.

     THE TOLERANCE IS IN SHEET UNITS, NOT PIXELS. Written in pixels it meant
     one thing at the preview size and something four times looser at the
     commit size, so the same fill traced at higher resolution came out no
     more faithful — the simplifier gave back everything the extra pixels
     had bought. Expressed as a fraction of the sheet, "how far the outline
     may stray from the truth" means the same thing at any resolution: a
     quarter of a thousandth of the drawing, which on a 12 mm charm is a
     third of the width of a human hair. */
  let contours = mkTraceMask(reg.mask, S).filter((c) => c.length >= 8);
  const EPS_STEPS = [0.00025, 0.0005, 0.001, 0.002, 0.004];
  let r = null;
  for (let k = 0; k < EPS_STEPS.length; k++) {
    const eps = EPS_STEPS[k] * S;
    const cs = contours.map((c) => mkRdp(c, eps)).filter((c) => c.length >= 6);
    r = { cs, n: cs.reduce((t, c) => t + c.length, 0) };
    if (r.n <= MK_FILL_MAX_PTS) break;
  }
  if (r && r.cs.length && r.n <= MK_FILL_MAX_PTS) {
    const flat = [], q = [];
    r.cs.forEach((c) => {
      q.push(c.length / 2);
      for (let k = 0; k < c.length; k += 2) {
        flat.push(Math.round((c[k] / S) * 1e5) / 1e5, Math.round((c[k + 1] / S) * 1e5) / 1e5);
      }
    });
    /* A HAIRLINE, NOT A HALO. This used to carry a stroke of 0.0256 of the
       sheet — a fat rope drawn round the outline to hide the gap the
       fattened walls had left. The gap is gone (the mask is grown back to
       the true edge above), so the rope is not only unnecessary, it was the
       thing rounding off every corner and swallowing every fine detail. And
       because Weight edits the stroke of whatever is selected, that rope is
       exactly why nudging Weight appeared to change the accuracy of a fill:
       it was thinning the halo, not sharpening the shape. */
    return mkPack({ id: mkId(), t: "fill", c: colour, f: colour, w: 0.0012, gr: 1,
                    p: flat, q, n: name || "" });
  }

  /* otherwise the mask, exactly as previewed */
  let cropped = mkMaskCrop(reg.mask, S, reg.box);
  let runs = mkRleEncode(cropped.m, cropped.w, cropped.h);
  let guard = 0;
  while (runs.length > MK_RLE_MAX && guard++ < 4) {
    cropped = mkMaskHalve(cropped.m, cropped.w, cropped.h);
    runs = mkRleEncode(cropped.m, cropped.w, cropped.h);
  }
  const b = reg.box;
  return mkPack({
    id: mkId(), t: "fill", c: colour, f: colour, w: 0.0015, gr: 1, n: name || "",
    /* the bounding box IS the geometry as far as the rest of the studio is
       concerned, so a masked fill drags and scales like anything else */
    p: [b.x0 / S, b.y0 / S, (b.x1 + 1) / S, (b.y1 + 1) / S],
    m: runs, ms: [cropped.w, cropped.h],
  });
}

/* the painted form of a masked fill, cached on its own content */
const MK_RLE_CACHE = new Map();
function mkRleCanvas(it, mode) {
  const w = Math.max(1, (it.ms || [])[0] | 0), h = Math.max(1, (it.ms || [])[1] | 0);
  const edge = mode === "edge";
  const col = edge ? MK_OUTLINE : String(mode || it.c || MK_ENGRAVE);
  /* ── AND IT HAS TO BE VISIBLE ────────────────────────────────────────
     A one-pixel boundary on a mask that is a couple of hundred pixels wide
     becomes a sub-pixel hairline once it is drawn across the sheet — which
     is not "outline only", it is "nothing happened". The band is scaled to
     the mask so a red outline on a photograph region reads as clearly as a
     red outline on a traced shape. */
  const R = edge ? Math.max(1, Math.round(Math.max(w, h) / 150)) : 0;
  const key = it.id + ":" + w + "x" + h + ":" + (it.m || []).length + ":" + col + (edge ? ":e" + R : "");
  const hit = MK_RLE_CACHE.get(key);
  if (hit) return hit;
  const m = mkRleDecode(it.m || [], w, h);
  const cv = document.createElement("canvas");
  cv.width = w; cv.height = h;
  const ctx = cv.getContext("2d");
  const img = ctx.createImageData(w, h);
  const d = img.data;
  const r = parseInt(col.slice(1, 3), 16) || 0;
  const g = parseInt(col.slice(3, 5), 16) || 0;
  const b = parseInt(col.slice(5, 7), 16) || 0;
  /* OUTLINE ONLY, for an area that has no contours to stroke. A run-length
     mask is just pixels, so its boundary is computed the only way pixels
     have one: a pixel is on the edge when it is set and one of its four
     neighbours is not. Cached on the same key as the solid form, so
     switching an area between engraved and outline costs nothing. */
  for (let i = 0; i < m.length; i++) {
    if (!m[i]) continue;
    if (edge) {
      const x = i % w, y = (i / w) | 0;
      const on = (xx, yy) => (xx < 0 || yy < 0 || xx >= w || yy >= h) ? 0 : m[yy * w + xx];
      if (on(x - R, y) && on(x + R, y) && on(x, y - R) && on(x, y + R) &&
          on(x - 1, y) && on(x + 1, y) && on(x, y - 1) && on(x, y + 1)) continue;
    }
    const j = i * 4;
    d[j] = r; d[j + 1] = g; d[j + 2] = b; d[j + 3] = 255;
  }
  ctx.putImageData(img, 0, 0);
  if (MK_RLE_CACHE.size > 40) MK_RLE_CACHE.clear();
  MK_RLE_CACHE.set(key, cv);
  return cv;
}

/* ═══════════ THE BUCKET ══════════════════════════════════════════════════
   Flood fill that forgives the hand. A sketched loop almost never quite
   closes, so the region is computed against a version of the sheet whose
   every stroke is WIDENED by a small tolerance — the gap seals itself and
   the fill stays inside. The flooded mask is then traced back to closed
   vector contours (outer edge AND holes, so a shape sitting inside the
   filled area keeps its white interior) and stored as an ordinary layer:
   flat points plus a list of contour lengths, because Firestore refuses an
   array inside an array.                                                    */

/* The bucket, expressed in terms of the region engine. Kept as a function of
   its own because the tests, the picture path and the tool all ask the same
   question and must get the same answer. */
function mkBucketFill(x, y, opts) {  /* every fill it returns is marked bk:1 */
  if (!__mk) return null;
  const o = opts || {};
  /* THE PREVIEW IS CHEAP; THE ANSWER SHOULD NOT BE. The tinted shape under
     the cursor is recomputed on every mouse move and so runs small. This
     runs ONCE, on the click, and there is no reason for the thing that gets
     kept to be as coarse as the thing that was only ever a hint. So the
     region is asked again at the eraser's own resolution, and the contour
     that lands on the sheet is traced from that — nine times the evidence,
     for one flood fill the customer never waits on.

     If the fine pass disagrees about whether there is a region here at all
     (a hairline gap that seals at one size and not the other), the coarse
     answer stands: it is the one the customer was shown. */
  const coarse = mkRegionAt(x, y, o) || mkRegionNear(x, y, o);
  if (!coarse) return { onLine: true };
  if (coarse.openRegion) return { openRegion: true };
  let reg = coarse;
  if (!o.S && MK_FINE_S > MK_REG_S) {
    try {
      const fine = mkRegionAt(x, y, Object.assign({}, o, { S: MK_FINE_S, noWalls: 1 }));
      /* AND IT HAS TO BE THE SAME REGION. Agreeing about "is this an area at
         all" is not enough: a click landing within a pixel of a wall can seal
         one way at 400 and the other way at 1200, and the customer would be
         handed the room on the far side of a line from the one they were
         shown. So the two masks are compared where it counts — the fine one
         must cover the coarse one's own seed point, and the two must be
         substantially the same area. If they are not, the answer the
         customer SAW is the answer they get. */
      if (fine && !fine.openRegion && fine.onPicture === coarse.onPicture &&
          mkRegionAgree(coarse, fine)) {
        fine.walls = coarse.walls;          /* the same objects, already known */
        reg = fine;
      }
    } catch (e) { /* the coarse answer is a real answer */ }
  }
  const intent = mkFillIntent((opts && opts.fill) || __mk.fill);
  const colour = mkIntentInk(intent);
  const item = mkRegionToItem(reg, colour, mkRegionName(reg, intent));
  if (!item) return null;
  /* an OUTLINE area is the region's boundary, worked, with its inside left
     alone — the same contours, carrying the third instruction */
  if (intent === "none") item.f = "none";
  item.bk = 1;
  return { item, region: reg };
}

/* Do two masks, computed at different resolutions, describe the same area?
   Asked by sampling: the coarse mask's own seed must be inside the fine one,
   and the fine one's coverage of the coarse one must be overwhelming. */
function mkRegionAgree(a, b) {
  try {
    const sx = Math.min(b.S - 1, Math.max(0, Math.round(a.seed[0] * b.S)));
    const sy = Math.min(b.S - 1, Math.max(0, Math.round(a.seed[1] * b.S)));
    if (!b.mask[sy * b.S + sx]) return false;
    let hit = 0, n = 0;
    const step = Math.max(1, Math.floor(a.S / 120));
    for (let y = 0; y < a.S; y += step) {
      for (let x = 0; x < a.S; x += step) {
        if (!a.mask[y * a.S + x]) continue;
        n++;
        const bx = Math.min(b.S - 1, Math.round((x / a.S) * b.S));
        const by = Math.min(b.S - 1, Math.round((y / a.S) * b.S));
        if (b.mask[by * b.S + bx]) hit++;
      }
    }
    return n === 0 || hit / n > 0.9;
  } catch (e) { return false; }
}

/* what to call it in the layer list, so a sheet of fills stays legible */
function mkRegionName(reg, intent) {
  const what = intent === "cutout" ? "Cut-out" : intent === "none" ? "Outline" : "Engraved";
  if (reg.onPicture) {
    const pic = __mk.items[reg.picIdx];
    return what + " · " + String((pic && pic.n) || "picture").slice(0, 22);
  }
  const pct = Math.round(reg.frac * 100);
  return what + " area" + (pct >= 1 ? " · " + pct + "%" : "");
}

/* ── WHERE IT LANDS ──────────────────────────────────────────────────────
   Above everything that walls it in, so it can never be hidden behind the
   very objects that defined it — and INSIDE their group when they share
   one, so it travels with them from then on. Nobody is asked about either. */
function mkPlaceFill(item, reg) {
  const walls = (reg && reg.walls) || [];
  let at = 0;
  if (walls.length) at = Math.max.apply(null, walls) + 1;
  /* if every wall belongs to one group, the fill is part of that group */
  const gs = {};
  walls.forEach((i) => { const g = mkGroupOf(__mk.items[i]); if (g) gs[g] = (gs[g] || 0) + 1; });
  const names = Object.keys(gs);
  const g = (names.length === 1 && gs[names[0]] === walls.length) ? names[0] : "";
  if (g) {
    item.g = g;
    /* directly above the group's topmost member keeps the block contiguous,
       which is the rule the whole layer model rests on */
    const idx = mkGroupIdx(g);
    if (idx.length) at = idx[idx.length - 1] + 1;
  }
  at = Math.max(0, Math.min(__mk.items.length, at));
  /* NEVER LAND INSIDE SOMEBODY ELSE'S BLOCK. When the walls belong to two
     different groups the index above is simply "above the topmost wall",
     and that can fall in the middle of a group whose members must stay
     contiguous — the rule the whole layer model rests on. Slide up past the
     end of whatever block the index landed in. */
  if (!g) {
    const host = __mk.items[at] ? mkGroupOf(__mk.items[at]) : "";
    const below = at > 0 ? mkGroupOf(__mk.items[at - 1]) : "";
    if (host && host === below) {
      const idx = mkGroupIdx(host);
      if (idx.length) at = Math.max(at, idx[idx.length - 1] + 1);
    }
  }
  at = Math.max(0, Math.min(__mk.items.length, at));
  __mk.items.splice(at, 0, item);
  return at;
}

/* ── CLICKING A FILL THAT IS ALREADY THERE ──────────────────────────────
   The same click that engraves an area un-engraves it, and switches it
   between engraved and cut-out if that is what changed in the meantime.
   One gesture, no modifier, nothing to learn: the tool always does the
   obvious thing to whatever is under the finger. */
function mkFillUnderPoint(x, y) {
  for (let i = __mk.items.length - 1; i >= 0; i--) {
    const it = __mk.items[i];
    if (it.t !== "fill" || it.hid || it.lok) continue;
    if (mkFillHit(it, x, y)) return i;
  }
  return -1;
}
/* …and the one that is in the way but may not be touched, so the studio can
   say which rather than "nothing to fill just there" */
function mkFillBlockedAt(x, y) {
  for (let i = __mk.items.length - 1; i >= 0; i--) {
    const it = __mk.items[i];
    if (it.t !== "fill" || (!it.hid && !it.lok)) continue;
    if (mkFillHit(it, x, y)) return it;
  }
  return null;
}
function mkFillHit(it, x, y) {
  /* ── ASK WHERE THE OBJECT ACTUALLY IS ────────────────────────────────
     A rotated fill is DRAWN rotated but was tested against its unrotated
     points, so the customer could hover solid black and be told there was
     nothing there, while a click on bare metal on the opposite side of the
     shape deleted it. Every other hit test in the studio unrotates first;
     this one did not. */
  const [ux, uy] = mkUnrotate(it, x, y);
  const bb = mkBBox(it);
  /* ── AND WITH A LITTLE ROOM AT THE EDGE ──────────────────────────────
     The contour a flood is traced into sits a fraction inside the pixels it
     came from, and the hairline that closes that seam is painted OUTSIDE
     it. So there is a band, a pixel or two wide, that looks filled and
     tested empty — and a click there did not take the fill back, it made a
     second identical one on top of the first. Every click in that band
     added another. The test is given the same tolerance the painter uses. */
  const tol = 0.004;
  if (ux < bb.x - tol || ux > bb.x + bb.w + tol ||
      uy < bb.y - tol || uy > bb.y + bb.h + tol) return false;
  if (it.m && it.m.length) {
    const w = (it.ms || [])[0] | 0, h = (it.ms || [])[1] | 0;
    if (!w || !h) return false;
    const m = mkRleDecode(it.m, w, h);
    const at = (gx, gy) => {
      const px = Math.floor(((gx - bb.x) / (bb.w || 1e-6)) * w);
      const py = Math.floor(((gy - bb.y) / (bb.h || 1e-6)) * h);
      if (px < 0 || py < 0 || px >= w || py >= h) return false;
      return !!m[py * w + px];
    };
    if (at(ux, uy)) return true;
    return at(ux - tol, uy) || at(ux + tol, uy) || at(ux, uy - tol) || at(ux, uy + tol);
  }
  /* contours, even-odd, exactly as they are painted */
  const p = it.p || [];
  const q = (it.q && it.q.length) ? it.q : [Math.floor(p.length / 2)];
  const inPoly = (px, py) => {
    let inside = false, k = 0;
    q.forEach((len) => {
      const s0 = k;
      for (let a = 0; a < len; a++, k++) {
        const b = s0 + ((a + 1) % len);
        const xi = p[k * 2], yi = p[k * 2 + 1], xj = p[b * 2], yj = p[b * 2 + 1];
        if ((yi > py) !== (yj > py) &&
            px < ((xj - xi) * (py - yi)) / ((yj - yi) || 1e-9) + xi) inside = !inside;
      }
    });
    return inside;
  };
  if (inPoly(ux, uy)) return true;
  /* the band: four probes at the painter's own tolerance */
  return inPoly(ux - tol, uy) || inPoly(ux + tol, uy) ||
         inPoly(ux, uy - tol) || inPoly(ux, uy + tol);
}

function mkTraceMask(mask, S) {
  const val = (x, y) => (x >= 0 && y >= 0 && x < S && y < S && mask[y * S + x]) ? 1 : 0;
  /* directed segments per 2×2 case, in half-pixel units so keys stay ints */
  const segs = new Map();                 /* "x,y" start → [ex, ey] */
  const put = (x1, y1, x2, y2) => {
    const k = x1 + "," + y1;
    if (!segs.has(k)) segs.set(k, []);
    segs.get(k).push([x2, y2]);
  };
  for (let y = -1; y < S; y++) {
    for (let x = -1; x < S; x++) {
      const tl = val(x, y), tr = val(x + 1, y), br = val(x + 1, y + 1), bl = val(x, y + 1);
      const c = tl * 8 + tr * 4 + br * 2 + bl;
      if (c === 0 || c === 15) continue;
      /* edge midpoints of this 2×2 block, ×2 for integer keys */
      const T = [2 * x + 2, 2 * y + 1], R = [2 * x + 3, 2 * y + 2],
            B = [2 * x + 2, 2 * y + 3], L = [2 * x + 1, 2 * y + 2];
      /* filled kept on the LEFT of travel, so every loop closes */
      switch (c) {
        case 1:  put(B[0], B[1], L[0], L[1]); break;
        case 2:  put(R[0], R[1], B[0], B[1]); break;
        case 3:  put(R[0], R[1], L[0], L[1]); break;
        case 4:  put(T[0], T[1], R[0], R[1]); break;
        case 5:  put(T[0], T[1], L[0], L[1]); put(B[0], B[1], R[0], R[1]); break;
        case 6:  put(T[0], T[1], B[0], B[1]); break;
        case 7:  put(T[0], T[1], L[0], L[1]); break;
        case 8:  put(L[0], L[1], T[0], T[1]); break;
        case 9:  put(B[0], B[1], T[0], T[1]); break;
        case 10: put(L[0], L[1], B[0], B[1]); put(R[0], R[1], T[0], T[1]); break;
        case 11: put(R[0], R[1], T[0], T[1]); break;
        case 12: put(L[0], L[1], R[0], R[1]); break;
        case 13: put(B[0], B[1], R[0], R[1]); break;
        case 14: put(L[0], L[1], B[0], B[1]); break;
      }
    }
  }
  const contours = [];
  segs.forEach((ends, startKey) => {
    while (ends.length) {
      const first = startKey.split(",").map(Number);
      let cur = ends.pop();
      const pts = [first[0], first[1], cur[0], cur[1]];
      let guard = 0;
      while (guard++ < 200000) {
        const k = cur[0] + "," + cur[1];
        const nx = segs.get(k);
        if (!nx || !nx.length) break;
        cur = nx.pop();
        if (cur[0] === first[0] && cur[1] === first[1]) break;   /* closed */
        pts.push(cur[0], cur[1]);
      }
      if (pts.length >= 6) contours.push(pts.map((v) => v / 2));
    }
  });
  return contours;
}

/* Ramer–Douglas–Peucker on a flat closed contour, iterative. */
function mkRdp(pts, eps) {
  const n = pts.length / 2;
  if (n <= 4) return pts;
  const keep = new Uint8Array(n);
  keep[0] = 1; keep[n - 1] = 1;
  const stack = [[0, n - 1]];
  while (stack.length) {
    const [a, b] = stack.pop();
    if (b - a < 2) continue;
    const ax = pts[a * 2], ay = pts[a * 2 + 1], bx = pts[b * 2], by = pts[b * 2 + 1];
    const dx = bx - ax, dy = by - ay;
    const len = Math.hypot(dx, dy) || 1;
    let worst = -1, wd = eps;
    for (let i = a + 1; i < b; i++) {
      const dd = Math.abs(dy * (pts[i * 2] - ax) - dx * (pts[i * 2 + 1] - ay)) / len;
      if (dd > wd) { wd = dd; worst = i; }
    }
    if (worst >= 0) { keep[worst] = 1; stack.push([a, worst], [worst, b]); }
  }
  const out = [];
  for (let i = 0; i < n; i++) if (keep[i]) out.push(pts[i * 2], pts[i * 2 + 1]);
  return out;
}

/* ═══════════ TAKING A GENERATED DRAWING APART ════════════════════════════
   A generated design arrives as a picture: black lines on white, one flat
   bitmap. Everything in the studio downstream of it — select, move, resize,
   group, delete, re-generate from what you changed — works on OBJECTS. So
   the picture is taken apart the moment it lands.

   The method is the one the bucket already uses, run the other way round.
   The bucket floods a WHITE region and traces the mask it filled; this
   labels every connected region of INK and traces each one. The outline of
   the charm is one region, each eye is another, the mouth another. Each
   comes back as closed contours — outer edge plus any holes, painted
   evenodd — which is why a ring traces as a ring and keeps its middle.

   Two things make this honest rather than clever:

     · it is exact. The contours follow the pixels that were generated, so
       the vector drawing and the bitmap are the same drawing. Nothing is
       redrawn, guessed or prettified.
     · it knows when to decline. A photograph, a wash of grey, or anything
       that comes apart into hundreds of specks is not a line drawing, and
       the picture is left alone rather than turned into confetti.        */
const MK_TRACE_S = 1100;            /* the magic eraser's own working size —
                                       a traced part is held to the same
                                       standard as everything else here */
const MK_TRACE_INK = 150;           /* luminance below this is ink */
const MK_TRACE_MAXPARTS = 48;       /* more than this is not a line drawing */

/* ── WHY THIS BUDGET IS WHAT IT IS ────────────────────────────────────────
   The tolerance used to be quoted in PIXELS, and when the total ran over
   budget it was multiplied by 1.6, up to six times. Six times 1.6 is
   nineteen — nineteen pixels of permitted error on a 640-pixel sheet, which
   is three per cent of the drawing per vertex. At that setting a circle of
   any size comes back as an eleven-sided polygon, and that is precisely what
   a generated charm looked like after being taken apart: the outline and the
   inner ring visibly faceted, the very thing the fill engine had just been
   fixed for.

   Expressed as a fraction of the SHEET, "how far may the outline stray from
   the truth" means the same thing at any resolution. At 0.0005 — half a
   thousandth of the drawing — a circle needs a vertex roughly every
   sqrt(8·r·ε), which for a charm-sized circle is about sixty of them: far
   past the point where an eye can find a straight edge, and only a hundred
   and twenty numbers. Fidelity here is cheap; it was the runaway that was
   expensive. The ceiling is raised to match, and no single part may eat it. */
const MK_TRACE_BUDGET = 11000;      /* numbers, across every part */
/* …and within any ONE of them, so a single gnarly region cannot spend the
   whole allowance. Generous on purpose: a charm whose ink is one connected
   region — a hoop touching the ring, engraving touching the outline, which
   is most of them — is legitimately one enormous part, and capping it
   tightly was throwing away the very outline the customer was complaining
   about while two thirds of the shared budget sat unused. */
const MK_TRACE_PART_MAX = 8000;
/* Finer rungs near the bottom. The first step used to be a cliff: 0.0005 to
   0.0008 collapsed a clean annulus from 1996 numbers to 344 — a six-fold
   loss for one step, landing far under budget and taking the detail with
   it. Small steps mean the simplification stops as soon as it has to. */
const MK_TRACE_EPS_STEPS = [0.0005, 0.00062, 0.0008, 0.001, 0.0013, 0.0017,
                            0.0022, 0.0028, 0.0036, 0.005];

/* every connected region of ink, 8-connected so a diagonal hairline is one
   stroke rather than a dotted line of separate ones */
function mkInkComponents(img, S, opts) {
  const cfg = opts || {};
  const maxInkFraction = cfg.maxInkFraction == null ? 0.55 : Math.max(0.05, Math.min(0.995, Number(cfg.maxInkFraction)));
  const cv = document.createElement("canvas");
  cv.width = S; cv.height = S;
  const ctx = cv.getContext("2d", { willReadFrequently: true });
  ctx.fillStyle = "#ffffff";
  ctx.fillRect(0, 0, S, S);

  /* NEVER STRETCH THE SOURCE INTO THE TRACING SQUARE. The tracer uses a
     square working raster because its contour code is square-indexed, but
     the source image is letterboxed into that square at its native aspect
     ratio. `frame` is carried out with the trace so placement can preserve
     the complete original image format, including intentional blank margins. */
  const iw = img.naturalWidth || img.width || 0;
  const ih = img.naturalHeight || img.height || 0;
  if (!iw || !ih) return null;
  const sc = Math.min(S / iw, S / ih);
  const dw = Math.max(1, Math.round(iw * sc));
  const dh = Math.max(1, Math.round(ih * sc));
  const dx = Math.round((S - dw) / 2);
  const dy = Math.round((S - dh) / 2);
  try { ctx.drawImage(img, dx, dy, dw, dh); } catch (e) { return null; }
  let d;
  try { d = ctx.getImageData(0, 0, S, S).data; } catch (e) { return null; }

  const N = S * S;
  const ink = new Uint8Array(N);
  let inked = 0, mid = 0;
  for (let i = 0; i < N; i++) {
    const j = i * 4;
    const a = d[j + 3];
    const lum = a < 40 ? 255 : (d[j] * 0.299 + d[j + 1] * 0.587 + d[j + 2] * 0.114);
    const sat = a < 40 ? 0 : Math.max(d[j], d[j + 1], d[j + 2]) - Math.min(d[j], d[j + 1], d[j + 2]);
    if (lum < MK_TRACE_INK || (sat > 70 && lum < 214)) { ink[i] = 1; inked++; }
    else if (lum < 214) mid++;
  }
  if (!inked || inked > N * maxInkFraction) return null;

  const lab = new Int32Array(N);
  const comps = [];
  const stack = [];
  let next = 0;
  for (let seed = 0; seed < N; seed++) {
    if (!ink[seed] || lab[seed]) continue;
    next++;
    let area = 0, x0 = S, y0 = S, x1 = -1, y1 = -1;
    stack.length = 0; stack.push(seed); lab[seed] = next;
    let sr = 0, sg = 0, sb = 0;
    while (stack.length) {
      const q = stack.pop();
      area++;
      const j4 = q * 4;
      sr += d[j4]; sg += d[j4 + 1]; sb += d[j4 + 2];
      const px = q % S, py = (q / S) | 0;
      if (px < x0) x0 = px; if (px > x1) x1 = px;
      if (py < y0) y0 = py; if (py > y1) y1 = py;
      for (let yy = -1; yy <= 1; yy++) {
        const ny = py + yy; if (ny < 0 || ny >= S) continue;
        for (let xx = -1; xx <= 1; xx++) {
          if (!xx && !yy) continue;
          const nx = px + xx; if (nx < 0 || nx >= S) continue;
          const r = ny * S + nx;
          if (ink[r] && !lab[r]) { lab[r] = next; stack.push(r); }
        }
      }
    }
    comps.push({ id: next, area, x0, y0, x1, y1,
                 r: sr / area, g: sg / area, b: sb / area });
    if (comps.length > 4000) return null;
  }
  return {
    lab, comps, S, inked, mid: mid / N,
    frame: { x: dx / S, y: dy / S, w: dw / S, h: dh / S },
    sourceW: iw, sourceH: ih,
  };
}

/* what a part looks like, in the terms a person would use about it */
function mkNamePart(p, all, S) {
  const w = p.w, h = p.h, cx = p.cx, cy = p.cy;
  if (p.isOutline) return "Charm outline";
  if (p.isHoop) return "Hoop ring";
  if (p.eye) return p.eye;
  /* wide, low, and thin for its width: a mouth */
  if (cy > 0.52 && w > 0.16 && w > h * 1.6) return "Mouth";
  /* small, central, between the eyes and the mouth: a nose */
  if (Math.abs(cx - 0.5) < 0.12 && cy > 0.34 && cy < 0.68 && w < 0.22 && h < 0.26) return "Nose";
  if (p.holes) return "Ring";
  if (Math.max(w, h) < 0.06) return "Dot";
  if (w > h * 3 || h > w * 3) return "Line";
  return "Shape";
}

/* the two that are the same size, the same height up, and mirrored about the
   middle — a pair of eyes says so by its geometry, not by hope */
function mkFindEyes(parts) {
  const cands = parts.filter((p) => !p.isOutline && !p.isHoop &&
                                    p.cy < 0.62 && p.w < 0.34 && p.h < 0.34);
  let best = null;
  for (let i = 0; i < cands.length; i++) {
    for (let j = i + 1; j < cands.length; j++) {
      const a = cands[i], b = cands[j];
      const bigger = Math.max(a.area, b.area), smaller = Math.min(a.area, b.area);
      if (smaller < bigger * 0.55) continue;             /* not the same size */
      if (Math.abs(a.cy - b.cy) > 0.07) continue;        /* not level */
      if (Math.abs(a.cx - b.cx) < 0.08) continue;        /* not apart */
      const mirror = Math.abs((a.cx + b.cx) / 2 - 0.5);
      if (mirror > 0.09) continue;                       /* not centred */
      const score = mirror + Math.abs(a.cy - b.cy);
      if (!best || score < best.score) best = { a, b, score };
    }
  }
  if (!best) return;
  /* left and right as the WEARER sees them, which is how a person naming
     the parts of a face would say it */
  const left = best.a.cx < best.b.cx ? best.a : best.b;
  const right = left === best.a ? best.b : best.a;
  left.eye = "Left eye";
  right.eye = "Right eye";
}

/* Trace one loaded image into ready-made layer objects. Returns null when
   the picture is not linework — the caller then leaves the bitmap alone. */
function mkTraceImage(img, colour, tune) {
  const cfg = tune || {};
  const S = Math.max(420, Math.round(cfg.size || MK_TRACE_S));
  const eps0 = Math.max(0.00008, Number(cfg.eps) || MK_TRACE_EPS_STEPS[0]);
  const maxParts = Math.max(8, Math.round(cfg.maxParts || MK_TRACE_MAXPARTS));
  const budgetCap = Math.max(1200, Math.round(cfg.budget || MK_TRACE_BUDGET));
  const partCap = Math.max(900, Math.round(cfg.partMax || MK_TRACE_PART_MAX));
  const minAreaFraction = Math.max(0.000002, Number(cfg.minAreaFraction) || 0.00006);
  const highFidelity = !!cfg.highFidelity;
  const coordScale = highFidelity ? 1e5 : 1e4;
  const forceEngrave = !!cfg.forceEngrave;
  const cc = mkInkComponents(img, S, cfg);
  if (!cc) return null;
  if (cc.mid != null && cc.mid > (cfg.maxMidFraction == null ? 0.30 : Number(cfg.maxMidFraction))) return null;
  const minArea = Math.max(8, Math.round(S * S * minAreaFraction));
  let parts = cc.comps.filter((c) => c.area >= minArea);
  if (!parts.length || parts.length > maxParts) return null;

  parts.sort((a, b) => {
    const ea = (a.x1 - a.x0 + 1) * (a.y1 - a.y0 + 1);
    const eb = (b.x1 - b.x0 + 1) * (b.y1 - b.y0 + 1);
    return (eb - ea) || (b.area - a.area);
  });
  parts = parts.map((c) => ({
    src: c, area: c.area,
    x: c.x0 / S, y: c.y0 / S,
    w: (c.x1 - c.x0 + 1) / S, h: (c.y1 - c.y0 + 1) / S,
    cx: (c.x0 + c.x1 + 1) / 2 / S, cy: (c.y0 + c.y1 + 1) / 2 / S,
    intent: mkPixelIntent(c.r || 0, c.g || 0, c.b || 0) || "engrave",
  }));
  const big = parts[0];
  if (big.w > 0.55 && big.h > 0.55) big.isOutline = true;
  parts.forEach((p) => {
    if (p.isOutline) return;
    if (p.cy < 0.16 && p.w < 0.4 && Math.abs(p.cx - 0.5) < 0.22) p.isHoop = true;
  });
  mkFindEyes(parts);

  const raw = [];
  for (const p of parts) {
    const mask = new Uint8Array(S * S);
    const id = p.src.id;
    for (let y = p.src.y0; y <= p.src.y1; y++) {
      const row = y * S;
      for (let x = p.src.x0; x <= p.src.x1; x++) if (cc.lab[row + x] === id) mask[row + x] = 1;
    }
    raw.push({ p, cs: mkTraceMask(mask, S).filter((c) => c.length >= 8) });
  }

  const traced = [];
  let eps = eps0;
  const traceAll = () => {
    traced.length = 0;
    let total = 0;
    for (const r0 of raw) {
      let contours = r0.cs.map((c) => mkRdp(c, eps * S)).filter((c) => c.length >= 6);
      if (!contours.length) continue;
      let own = contours.reduce((t, c) => t + c.length, 0);
      let e2 = eps, guard = 0;
      while (own > partCap && guard++ < 10) {
        e2 *= highFidelity ? 1.45 : 1.7;
        contours = r0.cs.map((c) => mkRdp(c, e2 * S)).filter((c) => c.length >= 6);
        own = contours.reduce((t, c) => t + c.length, 0);
      }
      if (!contours.length) continue;
      const flat = [], q = [];
      contours.forEach((c) => {
        q.push(c.length / 2);
        for (let k = 0; k < c.length; k += 2) {
          flat.push(Math.round((c[k] / S) * coordScale) / coordScale,
                    Math.round((c[k + 1] / S) * coordScale) / coordScale);
        }
      });
      total += flat.length;
      traced.push({ p: r0.p, flat, q, holes: contours.length - 1 });
    }
    return total;
  };

  /* Honour a caller's genuinely finer tolerance. The old ladder silently
     snapped any value below 0.0005 back UP to 0.0005, defeating high-fidelity
     imports. Escalation now starts exactly where the caller asked. */
  const ladder = [eps0].concat(MK_TRACE_EPS_STEPS.filter((v) => v > eps0 + 1e-10));
  let step = 0, total = traceAll();
  while (total > budgetCap && step < ladder.length - 1) {
    eps = ladder[++step];
    total = traceAll();
  }
  let hard = 0;
  while (total > budgetCap && hard++ < 10) { eps *= highFidelity ? 1.35 : 1.5; total = traceAll(); }
  if (!traced.length) return null;

  const items = traced.map((t) => {
    t.p.holes = t.holes;
    const intent = forceEngrave ? "engrave" : (t.p.isOutline ? "engrave" : (t.p.intent || "engrave"));
    const ink = intent === "engrave" ? (colour || MK_ENGRAVE) : mkIntentInk(intent);
    const nm = mkNamePart(t.p, traced, S);
    return mkPack({
      id: mkId(), t: "fill", c: ink,
      f: intent === "none" ? "none" : ink,
      w: 0.0012,
      p: t.flat, q: t.q,
      n: intent === "cutout" ? "Cut out · " + nm
       : intent === "none" ? "Outline · " + nm : nm,
      tr: 1,
    });
  });
  const seen = {};
  items.forEach((it) => {
    const base = it.n;
    seen[base] = (seen[base] || 0) + 1;
    if (seen[base] > 1) it.n = base + " " + seen[base];
  });
  return { items, points: total, eps, frame: cc.frame, sourceW: cc.sourceW, sourceH: cc.sourceH };
}

/* ── doing it, at the moment a drawing appears ───────────────────────────
   Automatic, because "take this apart for me" is not a thing anyone should
   have to know to ask for, and idle-timed, because it must never make a
   finished generation feel slower than it is. */
const MK_TRACED = new Set();          /* version keys done this session */
function mkVersionKey(v) { return String((v && v.n) != null ? v.n : ""); }

async function traceVersion(v, opts) {
  const force = !!(opts && opts.force);
  if (!v || !v.url) return null;
  /* a trace writes state.markups[versionNumber] — the one key shape that is
     GUARANTEED to collide between two designs */
  const ep = epochNow();
  const key = mkVersionKey(v);
  if (!key) return null;
  const saved = (state.markups || {})[key];
  /* never over-write work: a drawing somebody has already marked up, or
     already traced, is left exactly as it is */
  if (!force && saved && (mkItemsOf(saved).length || saved.traced)) return null;
  if (!force && MK_TRACED.has(key)) return null;
  MK_TRACED.add(key);

  const ref = { u: v.url, sp: v.path || "" };
  let img = mkImageFor(ref);
  if (!img) {
    await mkAwaitImages([{ t: "img", u: ref.u, sp: ref.sp }], 12000);
    img = mkImageFor(ref);
  }
  if (!img || !img.width) { MK_TRACED.delete(key); return null; }
  /* fetching the picture awaits, and "version 4" of the design they left is
     not "version 4" of the one they are in now */
  if (!sameProject(ep)) { MK_TRACED.delete(key); return null; }

  const res = mkTraceImage(img, "#1c1d1d");
  if (!res) return null;

  /* ── AND NOW GIVE THE PARTS BACK THEIR INSTRUCTIONS ────────────────────
     The trace can only see ink, so every part of it arrives saying ENGRAVE.
     The customer's own account of which areas are holes is carried on the
     version this drawing came from — so it is projected back on here, at the
     one moment the layers come into existence, and the sheet opens showing
     blue where they said blue. */
  const plan = mkFillPlan(v);
  try { mkApplyPlanToTrace(res.items, plan); } catch (e) { /* never blocks a trace */ }

  /* ── THE RECEIPT ─────────────────────────────────────────────────────────
     The rulebook travels with every generation and is stored on every
     version — but a rulebook the model can quietly disobey is a request, not
     a rule. So the drawing is now CHECKED the moment it arrives: its own
     pixels, read back with the same classifier that reads a reference,
     against the plan it was generated from. Every declared cut-out the
     drawing failed to honour is written on the version, told to the
     customer, and — the part that closes the loop — DICTATED into the next
     generation as an explicit correction, in words, digit-free. The model
     no longer gets to lose an instruction twice. */
  try { mkAuditVersion(v, img, plan); } catch (e) { /* audit never blocks */ }

  if (!state.markups) state.markups = {};
  const prev = state.markups[key] || {};
  state.markups[key] = {
    items: res.items,
    /* kept beside the layers so a re-trace, a re-render and another device
       all start from the same account of the design */
    plan: (plan || []).slice(0, 40),
    mode: prev.mode === "exact" ? "exact" : "interpret",
    crop: prev.crop || [],
    baseRot: prev.baseRot || 0,
    guides: prev.guides || 0,
    gridN: prev.gridN || 12,
    groups: [],
    traced: 1,
    /* untouched by hand, so a re-generate is still a plain re-generate and
       not "here is a marked-up sheet" — see buildMarkupPayload */
    dirty: 0,
    updatedAt: now(),
  };
  saveSession();
  /* the sheet, if it happens to be open on this very version */
  if (__mk && __mk.key === key && !__mk.items.length) {
    __mk.items = res.items.map(mkPack);
    __mk.traced = true;
    __mk.dirty = false;
    mkApplyBase();
    mkTouch();
    mkRenderLayers();
  }
  if (typeof mkLibRefresh === "function") mkLibRefresh();
  return res;
}
/* fire and forget, off the critical path of a generation */
function traceVersionSoon(v) {
  const go = () => { traceVersion(v).catch(() => {}); };
  if (window.requestIdleCallback) window.requestIdleCallback(go, { timeout: 2500 });
  else setTimeout(go, 400);
}

function mkStartCrop() {
  if (!__mk) return;
  mkCloseEditor();
  __mk.cropping = { rect: null };
  __mk.sel = -1;
  /* the crop bar takes the property row's place, so the square stage keeps
     exactly the height it had — see the note on .markup-crop */
  $("#markupProps").hidden = true;
  const cb0 = $("#markupCropBar"); if (cb0) cb0.hidden = false;
  $("#markupStage").classList.add("is-cropping");
  const ca0 = $("#mkCropApply"); if (ca0) ca0.disabled = true;
  mkRedraw();
}
function mkEndCrop(apply) {
  if (!__mk || !__mk.cropping) return;
  const r = __mk.cropping.rect;
  __mk.cropping = null;
  const cb1 = $("#markupCropBar"); if (cb1) cb1.hidden = true;
  $("#markupProps").hidden = false;
  $("#markupStage").classList.remove("is-cropping");
  $("#markupOverlay").querySelectorAll(".mk-croprect").forEach((e) => e.remove());
  if (apply && r && r.w > 0.05) {
    mkPushUndo();
    __mk.items.forEach((it) => {
      const p = it.p || [];
      for (let k = 0; k + 1 < p.length; k += 2) {
        p[k] = (p[k] - r.x) / r.w;
        p[k + 1] = (p[k + 1] - r.y) / r.w;
      }
    });
    /* crops compose: a crop of a crop is a smaller window on the original */
    const prev = __mk.crop || { x: 0, y: 0, w: 1 };
    __mk.crop = { x: prev.x + r.x * prev.w, y: prev.y + r.y * prev.w, w: prev.w * r.w };
    mkApplyBase();
  }
  mkTouch();
}
function mkPaintCropRect() {
  const host = $("#markupOverlay");
  host.querySelectorAll(".mk-croprect").forEach((e) => e.remove());
  const r = __mk.cropping && __mk.cropping.rect;
  if (!r) return;
  const el = document.createElement("div");
  el.className = "mk-sel mk-croprect";
  el.style.left = (r.x * 100) + "%"; el.style.top = (r.y * 100) + "%";
  el.style.width = (r.w * 100) + "%"; el.style.height = (r.w * 100) + "%";
  el.style.boxShadow = "0 0 0 9999px rgba(28,29,29,.42)";
  host.appendChild(el);
}

/* ═══════════ opening and closing ═════════════════════════════════════════ */
function mkBlank(key, compose) {
  return mkDefineSel({
    key, compose: !!compose,
    selIds: [], groups: {},
    tool: "draw", live: null, editing: null, cropping: null,
    /* NOT RED ANY MORE. Red is now one of the three engraving instructions,
       and a pen that draws in it would put the reserved colour on the sheet
       for a reason that has nothing to do with the metal. Green reads as ink
       — which is what a drawn line is — and stands off a black drawing just
       as well as red did. */
    color: compose ? "#1c1d1d" : "#0f7a4a",
    fill: "none", w: compose ? 0.006 : 0.008, ff: "sans", fs: 0.036,
    mode: "interpret",
    items: [], undo: [], redo: [],
    crop: null, baseRot: 0, baseFade: 1,
    /* The grid is BACK, and off until asked for — it was removed along with
       a Guides button that has not come back, so it needed a home rather
       than an execution. Alignment and the read-outs are on, because they
       only appear while something is actually being dragged. */
    guides: { grid: true, mirror: false, snap: false, align: true, dims: true },
    gridN: 48,
    eraseSize: 0.035,
    eraseDraw: null,
    z: 1, px: 0, py: 0,
  });
}
function mkLoadInto(mk, saved) {
  /* the undo/redo stacks ride the session too, so "undo" still works after a
     reload or on another device — continuing where you left off includes
     being able to take the last thing back */
  const hh = (state.mkHistory || {})[mk.key];
  if (hh) {
    mk.undo = Array.isArray(hh.u) ? hh.u.filter((v) => typeof v === "string") : [];
    mk.redo = Array.isArray(hh.r) ? hh.r.filter((v) => typeof v === "string") : [];
  }
  if (!saved) return mk;
  mk.items = mkItemsOf(saved).map(mkPack);
  mk.mode = saved.mode === "exact" ? "exact" : "interpret";
  if (Array.isArray(saved.crop) && saved.crop.length === 3) {
    mk.crop = { x: saved.crop[0], y: saved.crop[1], w: saved.crop[2] };
  }
  mk.baseRot = Number(saved.baseRot) || 0;
  mk.traced = !!saved.traced;
  mk.dirty = !!saved.dirty;
  /* group names come back with the groups — an "Eyes" group that reloaded as
     "Group" would be a group you have to identify all over again */
  mk.groups = {};
  (Array.isArray(saved.groups) ? saved.groups : []).forEach((g2) => {
    if (g2 && g2.id) mk.groups[String(g2.id)] = { n: String(g2.n || "Group").slice(0, 48), col: g2.col ? 1 : 0 };
  });
  /* any group tag on an item that arrived without a name still IS a group —
     imported layers have carried a shared tag since the library landed */
  mk.items.forEach((it) => {
    if (it.g && !mk.groups[it.g]) mk.groups[it.g] = { n: "Group", col: 0 };
  });
  /* GRID, CENTRE LINES AND SNAP WERE SCAFFOLDING, and the scaffolding has
     come down with the Guides button. Only symmetry survived — it halves the
     work of drawing anything symmetrical, which is not a guide, it is a
     tool — and it now lives with the other whole-sheet settings. Anything
     saved with the old flags still opens; it just opens on clean paper. */
  /* WHITE INK WAS IN THE OLD PALETTE, and the palette is gone — so a mark
     saved in white is now invisible on white paper with no way to change
     it. Anything that pale becomes the engraving ink on the way in: it is
     the only reading that leaves the customer their work. */
  mk.items.forEach((it) => {
    if (it.t === "img" || it.t === "fill") return;
    const c = String(it.c || "").toLowerCase();
    if (/^#(f{3,6}|f[0-9a-f]f[0-9a-f]f[0-9a-f])$/.test(c) || c === "#ffffff" || c === "#fff") {
      it.c = MK_ENGRAVE;
    }
  });
  const g = Number(saved.guides) || 0;
  /* five flags in one integer, the way they have always been stored, with
     the two new ones defaulting ON for a design saved before they existed */
  mk.guides = {
    grid: !!(g & 1), mirror: !!(g & 4), snap: !!(g & 8),
    align: !(g & 16), dims: !(g & 32),
  };
  /* the same range the slider can express, so a saved value can never
     show one number on the control and mean another */
  mk.gridN = Math.max(4, Math.min(48, Number(saved.gridN) || 12));
  return mk;
}

/* Mark up the DRAWING of the current version. The button that opens this is
   always available — including while the metal render is on screen — because
   direction is always about the drawing, and hiding the way to give it was
   only ever a way to make people hunt for it. */
/* ═══════════ THE TWO SOURCES ═════════════════════════════════════════════
   A design is TWO pictures, and it always was: the reference the customer
   made, and the drawing the studio made from it. Every iteration used to be
   forced through the second one — so when the drawing lost something, the
   only available move was to ask the drawing to correct itself, while the
   reference, the picture that is actually right, sat behind a different
   screen and could not be reached without leaving.

   The switch in the modal's head changes BOTH the sheet you are working on
   and what the gold button will re-draw from, because those are the same
   decision. It is stored on the version, so a design reopens where it was
   left, on this device and on any other. */
let __mkSrc = "ai";
function mkRefSheetAvailable() {
  return !!(state.reference && (state.reference.url || state.reference.drawn));
}
function mkSrcSay() {
  const btn = $("#markupRegenLbl");
  if (btn) btn.textContent = __mkSrc === "ref"
    ? "Re-generate from my reference" : "Re-generate from marks";
  const box = $("#markupSrc");
  if (!box) return;
  box.hidden = !mkRefSheetAvailable();
  box.querySelectorAll(".mk-src").forEach((b) => {
    const on = b.dataset.src === __mkSrc;
    b.classList.toggle("is-on", on);
    b.setAttribute("aria-checked", on ? "true" : "false");
  });
}
/* switching persists what is on the sheet first: nobody's work is ever the
   price of looking at the other picture */
function mkSrcSet(which, quiet) {
  const want = which === "ref" ? "ref" : "ai";
  if (want === "ref" && !mkRefSheetAvailable()) {
    toast("There's no reference on this design yet", "err");
    return;
  }
  if (__mk) { mkCloseEditor(true); mkPersist(); }
  __mkSrc = want;
  const v = state.versions[state.currentVersion];
  if (v) { v.src = want; saveSession(); }
  mkRegionForget();                       /* the cached sheet is the old one */
  openMarkup(true);
  if (!quiet) {
    toast(want === "ref"
      ? "This is your own reference — change anything, then Re-generate and the drawing is made again from it"
      : "This is the studio's drawing — mark it up, then Re-generate to re-draw from your marks", "gold");
  }
}

function openMarkup(keepSrc) {
  const v = state.versions[state.currentVersion];
  if (!v) { toast("Generate a design first — then you can mark it up", "err"); return; }
  if (state.stageView !== "bw") { state.stageView = "bw"; renderStage(); }
  ensureSession();
  if (!keepSrc) __mkSrc = (v.src === "ref" && mkRefSheetAvailable()) ? "ref" : "ai";
  if (__mkSrc === "ref" && !mkRefSheetAvailable()) __mkSrc = "ai";

  const base = $("#markupBase");
  /* NO crossOrigin here. Firebase Storage download URLs serve fine to a plain
     <img>, but a CORS-mode request against a bucket with no CORS rule fails
     the load outright — which is exactly why this modal once opened with a
     broken image icon. The composite makes its own CORS attempt on a separate
     Image and degrades to marks-on-white if refused. */
  base.removeAttribute("crossorigin");

  if (__mkSrc === "ref") {
    /* ── THE REFERENCE SHEET ────────────────────────────────────────────
       For a design that began as a drawing this IS the sketch — the same
       "draw" sheet, every component still live — so it opens the way the
       sketchpad opens it: white paper, no bitmap underneath, because the
       items ARE the picture and painting the flattened PNG behind them
       would draw everything twice. For a design that began as an upload or
       a catalogue charm there are no items, so the reference itself is the
       bitmap and marks go on top of it. */
    const drawn = !!(state.reference && state.reference.drawn);
    __mk = mkLoadInto(mkBlank("draw", drawn), (state.markups || {}).draw);
    __mk.url = state.reference.url || "";
    if (!drawn) {
      __mk.baseRef = { u: state.reference.url || "", sp: state.reference.path || "" };
      base.hidden = false;
      base.src = state.reference.url || "";
    } else {
      base.removeAttribute("src");
      base.hidden = true;
    }
    $("#markupEyebrow").textContent = "Your own reference";
    $("#markupTitle").textContent = "Change your reference";
    $("#markupActions").hidden = false;
    $("#markupComposeActions").hidden = true;
    $("#markupMode").hidden = false;
    const lblR = document.querySelector("#markupMode .markup-mode__lbl");
    if (lblR) lblR.textContent = "Read my reference";
    $("#mkEditSketch").hidden = true;
    mkSrcSay();
    mkShow(drawn);
    return;
  }

  __mk = mkLoadInto(mkBlank(String(v.n), false), (state.markups || {})[v.n]);
  __mk.url = v.url;
  /* the drawing underneath, addressable the CORS-safe way — see mkBucketFill */
  __mk.baseRef = { u: v.url, sp: v.path || "" };
  base.hidden = false;
  $("#markupEyebrow").textContent = "Show us, right on the drawing";
  $("#markupTitle").textContent = `Mark up drawing v${v.n}`;
  $("#markupActions").hidden = false;
  $("#markupComposeActions").hidden = true;
  $("#markupMode").hidden = false;
  const lbl0 = document.querySelector("#markupMode .markup-mode__lbl");
  if (lbl0) lbl0.textContent = "Read my marks";
  /* a design that began as a hand drawing keeps every component live —
     this is the way back to them */
  $("#mkEditSketch").hidden = !(state.reference && state.reference.drawn &&
                                markupHasContent((state.markups || {}).draw));
  base.src = v.url;
  mkSrcSay();
  mkShow();
  /* the belt to the generation's braces: a drawing made before this build,
     or one whose trace was interrupted, comes apart the moment it is opened */
  if (!__mk.items.length && !__mk.traced) {
    traceVersion(v).then((res) => {
      if (!res || !__mk || __mk.key !== String(v.n)) return;
      toast(`Taken apart into ${res.items.length} objects — every one of them ` +
            `is in Layers, and yours to move`, "gold");
    }).catch(() => {});
  }
}

/* The sketchpad: a blank sheet, no base image, and a different pair of
   actions at the foot. Everything else — every tool, guide and gesture — is
   identical, because it is the same surface. */
function openCompose() {
  /* A DESIGN EXISTS THE MOMENT SOMEBODY DRAWS.
     Without this the sketchpad had no session to save into: saveSession()
     returns silently when there is no active one, so every stroke made
     before pressing "Use this as my reference" was written nowhere and the
     design never appeared in My Designs. */
  ensureSession();
  state.designing = true;
  if (!state.startMode) state.startMode = "draw";
  __mkSrc = "ai";
  if ($("#markupSrc")) $("#markupSrc").hidden = true;
  __mk = mkLoadInto(mkBlank("draw", true), (state.markups || {}).draw);
  $("#markupEyebrow").textContent = "Your own hand";
  $("#markupTitle").textContent = "Draw your charm";
  $("#markupActions").hidden = true;
  $("#markupComposeActions").hidden = false;
  /* the exact/interpret choice belongs HERE too: it decides whether the
     sketch is vectorized faithfully or read as a brief for the real thing */
  $("#markupMode").hidden = false;
  const lbl = document.querySelector("#markupMode .markup-mode__lbl");
  if (lbl) lbl.textContent = "Read my sketch";
  $("#mkEditSketch").hidden = true;
  const base = $("#markupBase");
  base.removeAttribute("src");
  base.hidden = true;
  /* A BLANK SHEET USED TO ARRIVE RULED, on the theory that a grid helps you
     start. It did — while there was a Guides button to turn it off with.
     There is not any more, so ruling the paper permanently would be handing
     everybody a grid they cannot put away. Clean paper it is. */
  mkShow(true);
}

function mkShow(full) {
  setTimeout(mkMetalCoach, 500);
  $("#markupModal").classList.add("is-open");
  const inner = document.querySelector("#markupModal .markup__inner");
  if (inner) inner.classList.toggle("is-full", !!full);
  if ($("#mkFull")) $("#mkFull").classList.toggle("is-on", !!full);
  __mk.z = 1; __mk.px = 0; __mk.py = 0;
  mkApplyView();
  mkApplyBase();
  mkSignList();

  const size = () => {
    const stage = $("#markupStage"), cv = markupCanvas();
    const r = stage.getBoundingClientRect();
    if (r.width < 2) return;                   /* still laying out */
    /* bitmap at 2× the CSS box: the canvas is transformed with the view when
       zooming, and a 1× bitmap goes soft the moment it is scaled */
    cv.width = Math.max(2, Math.round(r.width * 2));
    cv.height = Math.max(2, Math.round(r.height * 2));
    mkRedraw();
  };
  __mk.resize = size;
  const base = $("#markupBase");
  if (base.complete || __mk.compose) size(); else base.onload = size;
  setTimeout(size, 60);                        /* after the modal's transition */
  setTimeout(size, 260);
  if (window.ResizeObserver && !$("#markupStage").dataset.ro) {
    $("#markupStage").dataset.ro = "1";
    new ResizeObserver(() => { if (__mk && __mk.resize) __mk.resize(); }).observe($("#markupStage"));
  }
  mkSetTool("draw");
  mkChrome();
  /* the sketchpad opens with the library showing: on a blank sheet, "what
     have I already got" is the first question, and hunting for the answer is
     the difference between using the library and forgetting it exists */
  /* the sketchpad opens on the library, everything else on the layers —
     but it OPENS either way, because the panel is part of the room now */
  mkDockSet(true, (__mk.compose && !__mk.items.length) ? "library" : __mkDock.tab);
  /* ON A PHONE IT STARTS FOLDED. There is no room beside the sheet, so the
     panel is a sheet across the bottom — and a bottom sheet at full height
     is the whole screen. Folded, its tab bar is still perpetually there and
     one tap from full size, which is the point: nothing to find, nothing to
     remember, and the drawing you came to make is not underneath it. */
  mkDockAutoFold();
  /* the footer's height is not settled on the first frame */
  setTimeout(mkDockFoot, 80);
  setTimeout(mkDockFoot, 400);
}

function closeMarkup() {
  mkCloseEditor(true);
  mkPersist();
  $("#markupModal").classList.remove("is-open");
  mkCloseAllDrops();
  mkCloseLibZoom();
  /* the panel goes with the modal it lives in; it does not get "closed",
     because there is no longer a button anywhere to open it again */
  const dock = mkDockEl();
  if (dock) dock.hidden = true;
  /* the panel is always open WHILE THE SHEET IS, and not one moment longer:
     mkLibRefresh fires on every sessions snapshot, and rebuilding a library
     of sketch thumbnails into a hidden node is work nobody will ever see */
  __mkDock.open = false;
  const inner = document.querySelector("#markupModal .markup__inner");
  if (inner) inner.classList.remove("has-dock");
  /* …and the composed sheets it cached, which are megabytes */
  mkRegionForget();
  __mk = null;
}

/* ═══════════ signatures ══════════════════════════════════════════════════
   Drawn once with the Sign tool, kept on the design session, dropped in
   anywhere afterwards. Stored flat: one point list plus the length of each
   stroke, because Firestore will not take an array of arrays.               */
/* a row you can focus but cannot activate is a row that lies about being
   focusable — Enter and Space do what a click does */
function mkSignKeys(host) {
  if (!host) return;
  host.addEventListener("keydown", (e) => {
    if (e.key !== "Enter" && e.key !== " ") return;
    const row = e.target && e.target.closest ? e.target.closest("[role='button']") : null;
    if (!row) return;
    e.preventDefault();
    row.click();
  });
}
function mkSignList() {
  const host = $("#mkSignList"); if (!host) return;
  if (!host.dataset.keys) { host.dataset.keys = "1"; mkSignKeys(host); }
  const sigs = state.signatures || [];
  if (!sigs.length) {
    host.innerHTML = '<p class="mk-sign-empty">Nothing saved yet — choose <b>Add Signature</b> above and write across the sheet. We keep it for next time.</p>';
    return;
  }
  host.innerHTML = sigs.map((s, i) => {
    const p = s.p || [];
    let d = "", k = 0;
    (s.seg || [p.length / 2]).forEach((n) => {
      for (let j = 0; j < n; j++, k++) d += (j ? "L" : "M") + (p[k * 2] * 100).toFixed(1) + " " + (p[k * 2 + 1] * 100).toFixed(1) + " ";
    });
    return `<div class="mk-sign" data-sig="${i}" role="button" tabindex="0">
      <svg viewBox="0 0 100 100" preserveAspectRatio="xMidYMid meet" aria-hidden="true"><path d="${attrText(d)}" fill="none" stroke="#1c1d1d" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/></svg>
      <button class="mk-sign__x" type="button" data-sigx="${i}" aria-label="Delete this signature">×</button>
    </div>`;
  }).join("");
}
function mkSaveSignature(it) {
  if (!state.signatures) state.signatures = [];
  const p = (it.p || []).slice();
  if (p.length < 8) return;
  /* normalize into its own box so it can be dropped anywhere at any size */
  const bb = mkBBox(it), sc = Math.max(bb.w, bb.h) || 1;
  const q = [];
  for (let k = 0; k + 1 < p.length; k += 2) {
    q.push(Math.round(((p[k] - bb.x) / sc) * 1e4) / 1e4, Math.round(((p[k + 1] - bb.y) / sc) * 1e4) / 1e4);
  }
  state.signatures.unshift({ p: q, seg: [q.length / 2] });
  state.signatures = state.signatures.slice(0, 4);
  mkSignList();
  saveSession();
}
function mkPlaceSignature(i) {
  const s = (state.signatures || [])[i]; if (!s || !__mk) return;
  mkPushUndo();
  const size = 0.42, ox = 0.3, oy = 0.62;
  const p = [];
  for (let k = 0; k + 1 < (s.p || []).length; k += 2) {
    p.push(ox + s.p[k] * size, oy + s.p[k + 1] * size);
  }
  __mk.items.push(mkPack({ id: mkId(), t: "sign", c: __mk.color, w: __mk.w, p }));
  __mk.sel = __mk.items.length - 1;
  mkSetTool("select");
  mkTouch();
}

/* ═════════════════════════════════════════════════════════════════════════
   ALIGNMENT — WHAT THE HAND CANNOT SEE

   Placing anything by eye is guesswork, and guesswork on a 12 mm object is
   guesswork nobody can correct later. Every serious drawing tool solves this
   the same way, and it is worth saying exactly what "the same way" means,
   because the details are the feature:

     · A DRAGGED OBJECT LOOKS FOR AGREEMENT. Its left, centre and right — and
       its top, middle and bottom — are compared against the same six lines
       on every other visible object, and against the sheet's own edges and
       centre. Agreement within a few SCREEN pixels is treated as intent.

     · THE THRESHOLD IS IN SCREEN PIXELS, NOT DRAWING UNITS. Zoomed in, the
       hand is more precise and the snap must be finer; zoomed out, coarser.
       A fixed tolerance in sheet units feels sticky at one zoom and useless
       at another, which is the single most common way this feature is got
       wrong.

     · IT SNAPS, AND IT SHOWS WHY. A line is drawn through the two things
       that agree — spanning both of them, not the whole sheet — so the
       reason the object jumped is visible rather than mysterious.

     · EQUAL SPACING IS ITS OWN KIND OF AGREEMENT. Three things in a row want
       equal gaps; when the gaps match, both are marked, and the object snaps
       to keep them matching.

     · IT MEASURES. The distance to the nearest neighbour on each side, in
       plain per cent of the drawing, while the object is moving.

     · AND IT GETS OUT OF THE WAY. Hold ALT and nothing snaps at all — for
       the times when the right answer is "no, exactly there".
   ====================================================================== */
const MK_SNAP_PX = 7;             /* agreement, in screen pixels */
const MK_ALIGN_MAXOBJ = 60;       /* neighbours considered — the NEAREST 60 */

/* THE BOX AS IT IS DRAWN, rotation and all. mkBBox returns the box of an
   object's raw points, which is what everything that EDITS geometry needs —
   but it is not where a rotated object appears, and aligning to a box the
   customer cannot see is aligning to nothing. Measured on a 45° bar, the two
   disagree by 13% of the sheet: on a 12 mm charm, a millimetre and a half of
   confident, unexplainable misplacement. */
function mkDrawnBox(it) {
  const bb = mkBBox(it);
  if (!it.r) return bb;
  const cx = bb.x + bb.w / 2, cy = bb.y + bb.h / 2;
  const c = Math.cos(it.r), s = Math.sin(it.r);
  let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
  [[bb.x, bb.y], [bb.x + bb.w, bb.y], [bb.x + bb.w, bb.y + bb.h], [bb.x, bb.y + bb.h]]
    .forEach((q) => {
      const dx = q[0] - cx, dy = q[1] - cy;
      const X = cx + dx * c - dy * s, Y = cy + dx * s + dy * c;
      if (X < x0) x0 = X;
      if (X > x1) x1 = X;
      if (Y < y0) y0 = Y;
      if (Y > y1) y1 = Y;
    });
  return { x: x0, y: y0, w: x1 - x0, h: y1 - y0 };
}
function mkUnionDrawn(list) {
  let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
  list.forEach((it) => {
    const b = mkDrawnBox(it);
    if (b.x < x0) x0 = b.x;
    if (b.y < y0) y0 = b.y;
    if (b.x + b.w > x1) x1 = b.x + b.w;
    if (b.y + b.h > y1) y1 = b.y + b.h;
  });
  if (!isFinite(x0)) return { x: 0, y: 0, w: 0, h: 0 };
  return { x: x0, y: y0, w: x1 - x0, h: y1 - y0 };
}

/* the tolerance the hand actually has, converted into sheet units — and
   never wider than half a grid cell, because a snap radius bigger than the
   gap between two attractors means there is nowhere left to rest between
   them, which is exactly what dragging the spacing slider toward "fine"
   used to produce */
function mkSnapTol() {
  const v = $("#markupView");
  const w = v ? v.getBoundingClientRect().width : 0;
  let t = (w > 8) ? Math.min(0.05, Math.max(0.0008, MK_SNAP_PX / w)) : 0.006;
  if (__mk && __mk.guides && __mk.guides.snap) {
    const n = Math.max(4, Math.min(48, __mk.gridN || 12));
    t = Math.min(t, 0.42 / n);
  }
  return t;
}

/* every line worth agreeing with: the sheet, and the NEAREST visible objects
   that are not part of what is moving. Nearest, not first — taking the first
   sixty in stacking order meant the object right under the customer's hand
   could be ignored while sixty in a far corner supplied the guides. */
function mkAlignTargets(excludeIdx, near) {
  const out = { x: [], y: [], boxes: [] };
  if (!__mk) return out;
  const skip = new Set(excludeIdx || []);
  out.x.push({ v: 0, kind: "sheet" }, { v: 0.5, kind: "sheet-mid" }, { v: 1, kind: "sheet" });
  out.y.push({ v: 0, kind: "sheet" }, { v: 0.5, kind: "sheet-mid" }, { v: 1, kind: "sheet" });
  const cx = near ? near.x + near.w / 2 : 0.5;
  const cy = near ? near.y + near.h / 2 : 0.5;
  const cand = [];
  __mk.items.forEach((it, i) => {
    if (skip.has(i) || it.hid) return;
    const b = mkDrawnBox(it);
    if (!(b.w >= 0) || !(b.h >= 0)) return;
    /* AN OBJECT OFF THE SHEET IS NOT A THING TO LINE UP WITH. Its edges are
       outside the paper, and a guide drawn there lands on the surrounding
       chrome — the object jumps for a reason nothing on screen shows. */
    if (b.x + b.w < -0.02 || b.x > 1.02 || b.y + b.h < -0.02 || b.y > 1.02) return;
    const d = Math.abs(b.x + b.w / 2 - cx) + Math.abs(b.y + b.h / 2 - cy);
    cand.push({ b, d });
  });
  cand.sort((a, b) => a.d - b.d);
  cand.slice(0, MK_ALIGN_MAXOBJ).forEach(({ b }) => {
    out.boxes.push(b);
    out.x.push({ v: b.x, kind: "edge", b }, { v: b.x + b.w / 2, kind: "mid", b },
               { v: b.x + b.w, kind: "edge", b });
    out.y.push({ v: b.y, kind: "edge", b }, { v: b.y + b.h / 2, kind: "mid", b },
               { v: b.y + b.h, kind: "edge", b });
  });
  return out;
}

/* THE SOLVER. Given where the moving box is now, return the nudge that makes
   it agree with something — and EVERY target it then agrees with, so all of
   them can be drawn rather than only the one that won. */
function mkAlignSolve(bb, targets, tol, opts) {
  const o = opts || {};
  const res = { dx: 0, dy: 0, gx: [], gy: [] };
  const axis = (lo, mid, hi, list, allowed) => {
    let best = null;
    const mine = [{ v: lo, at: "lo" }, { v: mid, at: "mid" }, { v: hi, at: "hi" }];
    mine.forEach((m) => {
      if (allowed && allowed.indexOf(m.at) < 0) return;
      list.forEach((t) => {
        const ad = Math.abs(t.v - m.v);
        if (ad > tol) return;
        /* A CENTRE IS PREFERRED, BUT NEVER OVER A PERFECT EDGE. This used to
           SUBTRACT a quarter of the tolerance from a centre candidate's
           distance, which makes the rank negative — so a centre a whole
           pixel away beat an edge sitting at exactly zero, and picking an
           already-aligned object up and putting it straight back down
           knocked it off by a pixel and a half. A preference has to be a
           tie-break, not a discount. */
        const centre = t.kind === "mid" || t.kind === "sheet-mid" || m.at === "mid";
        if (!best) { best = { ad, centre, d: t.v - m.v }; return; }
        const close = Math.abs(ad - best.ad) < tol * 0.06;
        if (ad < best.ad - 1e-9 || (close && centre && !best.centre)) {
          best = { ad, centre, d: t.v - m.v };
        }
      });
    });
    if (!best) return null;
    /* everything that agrees at the winning value, not merely the winner:
       four things sharing an edge should show four connections, and showing
       one of them is the difference between a guide and a hint */
    const at = [];
    mine.forEach((m) => {
      if (allowed && allowed.indexOf(m.at) < 0) return;
      list.forEach((t) => {
        if (Math.abs((t.v - m.v) - best.d) < 1e-6) at.push(t);
      });
    });
    return { d: best.d, hits: at };
  };
  const bx = axis(bb.x, bb.x + bb.w / 2, bb.x + bb.w, targets.x, o.edgesX);
  const by = axis(bb.y, bb.y + bb.h / 2, bb.y + bb.h, targets.y, o.edgesY);
  if (bx) {
    res.dx = bx.d;
    bx.hits.forEach((t) => res.gx.push({ v: t.v, kind: t.kind, with: t.b || null }));
  }
  if (by) {
    res.dy = by.d;
    by.hits.forEach((t) => res.gy.push({ v: t.v, kind: t.kind, with: t.b || null }));
  }
  return res;
}

/* EQUAL SPACING. Looks along one axis for a neighbour on each side and, when
   the two gaps are within tolerance of each other, nudges so they match. */
function mkAlignSpacing(bb, boxes, tol, horiz) {
  const lo = horiz ? bb.x : bb.y;
  const size = horiz ? bb.w : bb.h;
  const hi = lo + size;
  const c0 = horiz ? bb.y : bb.x, c1 = horiz ? bb.y + bb.h : bb.x + bb.w;
  let before = null, after = null;
  boxes.forEach((b) => {
    const bl = horiz ? b.x : b.y, bs = horiz ? b.w : b.h;
    const d0 = horiz ? b.y : b.x, d1 = horiz ? b.y + b.h : b.x + b.w;
    /* IN THE SAME ROW MEANS IN THE SAME ROW. Any overlap at all used to
       count, so two objects sharing one ten-thousandth of the sheet were
       treated as a row and the object was dragged to space itself evenly
       between things it is nowhere near. */
    const over = Math.min(c1, d1) - Math.max(c0, d0);
    if (over < Math.min(c1 - c0, d1 - d0) * 0.4) return;
    if (bl + bs <= lo && (!before || bl + bs > (horiz ? before.x + before.w : before.y + before.h))) before = b;
    if (bl >= hi && (!after || bl < (horiz ? after.x : after.y))) after = b;
  });
  if (!before || !after) return null;
  const gapA = lo - (horiz ? before.x + before.w : before.y + before.h);
  const gapB = (horiz ? after.x : after.y) - hi;
  if (gapA < 0 || gapB < 0) return null;
  if (Math.abs(gapA - gapB) > tol * 2) return null;
  const want = (gapA + gapB) / 2;
  return { delta: want - gapA, gapA, gapB, before, after, horiz, want };
}

/* ── DRAWING THE REASON ────────────────────────────────────────────────── */
function mkAlignClear() {
  const host = $("#markupOverlay"); if (!host) return;
  host.querySelectorAll(".mk-gd, .mk-dim").forEach((e) => e.remove());
}
function mkAlignPaint(bb, hits, spacing) {
  const host = $("#markupOverlay"); if (!host || !__mk) return;
  mkAlignClear();
  if (!(__mk.guides && __mk.guides.align)) return;
  const CL = (n) => Math.min(1, Math.max(0, n));
  const pc = (n) => (n * 100 < 10 ? (n * 100).toFixed(1) : Math.round(n * 100)) + "%";
  const line = (vert, at, from, to, kind) => {
    if (at < -0.001 || at > 1.001) return;      /* never off the paper */
    const a = CL(at), f = CL(from), t = CL(to);
    if (t - f < 0.005) return;
    const el = document.createElement("i");
    el.className = "mk-gd" + (kind === "sheet-mid" ? " mk-gd--mid" : kind === "sheet" ? " mk-gd--sheet" : "");
    if (vert) {
      el.style.left = a * 100 + "%"; el.style.top = f * 100 + "%";
      el.style.height = (t - f) * 100 + "%"; el.style.width = "0";
    } else {
      el.style.top = a * 100 + "%"; el.style.left = f * 100 + "%";
      el.style.width = (t - f) * 100 + "%"; el.style.height = "0";
    }
    host.appendChild(el);
  };
  const badge = (x, y, text, cls) => {
    const el = document.createElement("i");
    el.className = "mk-dim" + (cls ? " " + cls : "");
    el.style.left = CL(Math.min(0.9, Math.max(0.1, x))) * 100 + "%";
    el.style.top = CL(Math.min(0.95, Math.max(0.05, y))) * 100 + "%";
    el.textContent = text;
    host.appendChild(el);
  };
  const seenX = {}, seenY = {};
  (hits.gx || []).forEach((g) => {
    const k = g.v.toFixed(5);
    if (seenX[k] && !g.with) return;
    seenX[k] = 1;
    const o = g.with;
    line(true, g.v, o ? Math.min(bb.y, o.y) - 0.02 : 0, o ? Math.max(bb.y + bb.h, o.y + o.h) + 0.02 : 1, g.kind);
  });
  (hits.gy || []).forEach((g) => {
    const k = g.v.toFixed(5);
    if (seenY[k] && !g.with) return;
    seenY[k] = 1;
    const o = g.with;
    line(false, g.v, o ? Math.min(bb.x, o.x) - 0.02 : 0, o ? Math.max(bb.x + bb.w, o.x + o.w) + 0.02 : 1, g.kind);
  });
  /* SPACING IS SHOWN AS A BAR, not only as a number — otherwise switching
     measurements off leaves an object moving for a reason nothing explains */
  (spacing || []).forEach((sp) => {
    if (!sp) return;
    const bar = (from, to, at) => {
      const el = document.createElement("i");
      el.className = "mk-gd mk-gd--gap";
      if (sp.horiz) {
        el.style.left = CL(from) * 100 + "%"; el.style.width = Math.max(0, CL(to) - CL(from)) * 100 + "%";
        el.style.top = CL(at) * 100 + "%"; el.style.height = "0";
      } else {
        el.style.top = CL(from) * 100 + "%"; el.style.height = Math.max(0, CL(to) - CL(from)) * 100 + "%";
        el.style.left = CL(at) * 100 + "%"; el.style.width = "0";
      }
      host.appendChild(el);
    };
    if (sp.horiz) {
      const y = bb.y + bb.h / 2;
      bar(sp.before.x + sp.before.w, bb.x, y);
      bar(bb.x + bb.w, sp.after.x, y);
      if (__mk.guides.dims) {
        /* THE REAL GAP, not the average of the two. When an edge snap wins
           the same frame the nudge is not applied, and printing the mean
           made both badges claim a match that was not there. */
        badge(sp.before.x + sp.before.w + (bb.x - (sp.before.x + sp.before.w)) / 2, y - 0.035,
              pc(Math.max(0, bb.x - (sp.before.x + sp.before.w))), "mk-dim--gap");
        badge(bb.x + bb.w + (sp.after.x - (bb.x + bb.w)) / 2, y - 0.035,
              pc(Math.max(0, sp.after.x - (bb.x + bb.w))), "mk-dim--gap");
      }
    } else {
      const x = bb.x + bb.w / 2;
      bar(sp.before.y + sp.before.h, bb.y, x);
      bar(bb.y + bb.h, sp.after.y, x);
      if (__mk.guides.dims) {
        badge(x + 0.05, sp.before.y + sp.before.h + (bb.y - (sp.before.y + sp.before.h)) / 2,
              pc(Math.max(0, bb.y - (sp.before.y + sp.before.h))), "mk-dim--gap");
        badge(x + 0.05, bb.y + bb.h + (sp.after.y - (bb.y + bb.h)) / 2,
              pc(Math.max(0, sp.after.y - (bb.y + bb.h))), "mk-dim--gap");
      }
    }
  });
}

/* the read-out that follows the selection — placed OUTSIDE it, so it never
   covers the very thing being positioned, and always on the paper */
function mkAlignReadout(bb, mode) {
  const host = $("#markupOverlay"); if (!host || !__mk) return;
  if (!(__mk.guides && __mk.guides.dims)) return;
  const el = document.createElement("i");
  el.className = "mk-dim mk-dim--size";
  const pc = (n) => Math.round(n * 1000) / 10;
  el.textContent = mode === "resize"
    ? pc(bb.w) + "% × " + pc(bb.h) + "%"
    : "x " + pc(bb.x) + "%  y " + pc(bb.y) + "%";
  /* clear the resize handles, which are twice the size on a touch screen —
     a fixed gap in sheet units put the read-out UNDER the corner handle on a
     phone, where the sheet is small and the handles are big */
  const vw = (() => { const v = $("#markupView");
                      return v ? v.getBoundingClientRect().width : 0; })();
  const coarse = !!(window.matchMedia && window.matchMedia("(pointer:coarse)").matches);
  const clearPx = (coarse ? 11 : 6) + 17;
  const gap = vw > 8 ? Math.max(0.03, Math.min(0.1, clearPx / vw)) : 0.035;
  const below = bb.y + bb.h + gap;
  const above = bb.y - gap;
  el.style.left = Math.min(0.84, Math.max(0.16, bb.x + bb.w / 2)) * 100 + "%";
  el.style.top = (below < 0.95 ? below : Math.max(0.05, above)) * 100 + "%";
  host.appendChild(el);
}

/* THE WHOLE THING, applied to a drag in progress. */
function mkAlignApply(list, mode, suspended, edgesX, edgesY) {
  const res = { dx: 0, dy: 0 };
  if (!__mk || !list.length) { mkAlignClear(); return res; }
  const bb = mkUnionDrawn(list);
  if (suspended || !(__mk.guides && (__mk.guides.align || __mk.guides.snap))) {
    mkAlignClear();
    mkAlignReadout(bb, mode);
    return res;
  }
  const tol = mkSnapTol();
  const idxs = list.map((o) => __mk.items.indexOf(o));
  const targets = __mk.guides.align ? mkAlignTargets(idxs, bb) : { x: [], y: [], boxes: [] };

  /* the grid is just another set of lines to agree with, which is why snap
     and alignment can be on together without arguing */
  if (__mk.guides.snap) {
    const n = Math.max(4, Math.min(48, __mk.gridN || 12));
    for (let i = 0; i <= n; i++) {
      targets.x.push({ v: i / n, kind: "grid" });
      targets.y.push({ v: i / n, kind: "grid" });
    }
  }
  const hits = mkAlignSolve(bb, targets, tol, { edgesX, edgesY });
  res.dx = hits.dx; res.dy = hits.dy;

  const moved = { x: bb.x + res.dx, y: bb.y + res.dy, w: bb.w, h: bb.h };
  const spacing = [];
  if (mode === "move" && __mk.guides.align) {
    const sh = mkAlignSpacing(moved, targets.boxes, tol, true);
    const sv = mkAlignSpacing(moved, targets.boxes, tol, false);
    if (sh && !hits.gx.length) { res.dx += sh.delta; moved.x += sh.delta; }
    if (sh) spacing.push(sh);
    if (sv && !hits.gy.length) { res.dy += sv.delta; moved.y += sv.delta; }
    if (sv) spacing.push(sv);
  }
  mkAlignPaint(moved, hits, spacing);
  mkAlignReadout(moved, mode);
  return res;
}

/* Manual eraser hit-testing is deliberately exact for traced fills: imported
   artwork is a stack of overlapping bounding boxes, so bbox-only hit testing
   would erase the oval frame while the pointer was actually on the bull. */
function mkEraseHit(x, y) {
  if (!__mk) return -1;
  for (let i = __mk.items.length - 1; i >= 0; i--) {
    const it = __mk.items[i];
    if (!it || it.hid || it.lok || it.t === "text" || it.t === "callout" || it.t === "label") continue;
    if (it.t === "fill") { if (mkFillHit(it, x, y)) return i; continue; }
    if (it.t === "img") { if (mkPictureLocal(it, x, y)) return i; continue; }
    const [ux, uy] = mkUnrotate(it, x, y);
    const b = mkBBox(it), pad = Math.max(0.008, (__mk.eraseSize || 0.035) / 2);
    if (ux >= b.x - pad && ux <= b.x + b.w + pad && uy >= b.y - pad && uy <= b.y + b.h + pad) return i;
  }
  return -1;
}
function mkEraseLocalPoint(it, x, y) {
  const [ux, uy] = mkUnrotate(it, x, y);
  const b = mkBBox(it);
  return [
    Math.max(-0.5, Math.min(1.5, (ux - b.x) / Math.max(1e-6, b.w))),
    Math.max(-0.5, Math.min(1.5, (uy - b.y) / Math.max(1e-6, b.h))),
  ];
}
function mkEraseAddPoint(x, y) {
  if (!__mk || !__mk.eraseDraw) return false;
  const hit = mkEraseHit(x, y);
  if (hit < 0) { __mk.eraseDraw.last = -1; return false; }
  const it = __mk.items[hit];
  const b = mkBBox(it), maxDim = Math.max(1e-6, b.w, b.h);
  const radiusLocal = ((__mk.eraseSize || 0.035) / 2) / maxDim;
  const pt = mkEraseLocalPoint(it, x, y);
  if (!Array.isArray(it.er)) it.er = [];
  let st = null;
  if (__mk.eraseDraw.last === hit && it.er.length) st = it.er[it.er.length - 1];
  if (!st || Math.abs((Number(st.r) || 0) - radiusLocal) > 0.002) {
    st = { r: radiusLocal, p: [] };
    it.er.push(st);
    if (it.er.length > 80) it.er.shift();
  }
  const q = st.p;
  if (q.length >= 2) {
    const du = pt[0] - q[q.length - 2], dv = pt[1] - q[q.length - 1];
    /* enough samples for a smooth stroke, without writing hundreds of almost
       identical points into Firestore during a slow finger drag */
    const minStep = Math.max(0.0012, radiusLocal * 0.12);
    if (du * du + dv * dv < minStep * minStep) return true;
  }
  q.push(pt[0], pt[1]);
  if (q.length > 2400) q.splice(0, q.length - 2400);
  __mk.eraseDraw.last = hit;
  return true;
}
function mkEraserCursorAt(x, y) {
  const host = $("#markupOverlay");
  if (!host || !__mk || __mk.tool !== "erase") return;
  let c = host.querySelector(".mk-eraser-cursor");
  if (!c) { c = document.createElement("i"); c.className = "mk-eraser-cursor"; host.appendChild(c); }
  const d = Math.max(0.008, __mk.eraseSize || 0.035);
  c.style.left = (x * 100) + "%"; c.style.top = (y * 100) + "%";
  c.style.width = (d * 100) + "%"; c.style.height = (d * 100) + "%";
}
function mkEraserCursorClear() {
  const c = document.querySelector("#markupOverlay .mk-eraser-cursor");
  if (c) c.remove();
}

/* ═══════════ pointer plumbing — one handler set, every tool ══════════════ */
(function wireMarkup() {
  const stage = $("#markupStage");
  if (!stage) return;

  /* normalized against the VIEW, not the stage: the view's rect reflects the
     zoom/pan transform, so these coordinates are image-true at any zoom */
  const norm = (e) => {
    const r = $("#markupView").getBoundingClientRect();
    let x = (e.clientX - r.left) / r.width, y = (e.clientY - r.top) / r.height;
    /* THE POINTER IS NOT QUANTISED. Snapping the cursor snaps wherever the
       object happened to be GRABBED, so an object picked up 3% from its own
       corner lands 3% off every grid line for ever. What gets snapped is the
       OBJECT — its edges and its centre — and that happens in mkAlignApply
       once the drag knows what it is moving. */
    return [Math.min(1, Math.max(0, x)), Math.min(1, Math.max(0, y))];
  };

  stage.addEventListener("wheel", (e) => {
    if (!__mk) return;
    e.preventDefault();
    mkZoomAt(e.clientX, e.clientY, Math.pow(1.0018, -e.deltaY * (e.ctrlKey ? 3 : 1)));
  }, { passive: false });
  $("#mkZoomReset").addEventListener("click", () => {
    if (!__mk) return;
    __mk.z = 1; __mk.px = 0; __mk.py = 0;
    mkApplyView();
  });

  /* ══════════ two fingers ═══════════════════════════════════════════════
     Pinch to zoom, two-finger drag to pan — on EVERY tool, so a phone never
     has to swap to a Move tool just to look somewhere else. The moment a
     second finger lands, whatever the first one was drawing is abandoned:
     the customer is navigating, not drawing, and half a stroke left behind
     by a pinch is a mark nobody asked for. */
  const PTRS = new Map();
  const twoFinger = () => PTRS.size >= 2;
  const gestureFrom = () => {
    const [a, b] = Array.from(PTRS.values());
    return { d: Math.hypot(a.x - b.x, a.y - b.y),
             cx: (a.x + b.x) / 2, cy: (a.y + b.y) / 2 };
  };
  const abandonLive = () => {
    if (!__mk) return;
    __mk.live = null; __mk.liveFrom = null;
    if (__mk.marq) { __mk.marq = null; mkPaintMarquee(); }
    if (__mk.drag) { __mk.drag = null; mkAlignClear(); }
    __mk.panFrom = null;
    if (__mk.cropFrom) { __mk.cropFrom = null; __mk.cropping && (__mk.cropping.rect = null); mkPaintCropRect(); }
    mkRedraw();
  };

  stage.addEventListener("pointerdown", (e) => {
    if (!__mk) return;
    if (e.target.closest(".markup-zoom-reset")) return;
    if (e.target.closest(".mk-editor")) return;
    PTRS.set(e.pointerId, { x: e.clientX, y: e.clientY });
    if (twoFinger()) {
      e.preventDefault();
      abandonLive();
      const g = gestureFrom();
      __mk.gesture = { d0: g.d, z0: __mk.z, px0: __mk.px, py0: __mk.py, cx: g.cx, cy: g.cy };
      return;
    }
    const handle = e.target.closest(".mk-handle");
    const [x, y] = norm(e);
    mkCloseAllDrops();

    /* ── crop ── */
    if (__mk.cropping) {
      e.preventDefault();
      __mk.cropFrom = { x, y };
      __mk.cropping.rect = { x, y, w: 0 };
      return;
    }

    /* ── a selection handle ── */
    if (handle && mkSelCount()) {
      e.preventDefault();
      const list = mkSelItems();
      const bb = list.length > 1 ? mkUnionBBox(list) : mkBBox(list[0]);
      mkPushUndo();
      __mk.drag = {
        kind: handle.dataset.h === "rot" ? "rotate" : "resize",
        h: handle.dataset.h, list, from: [x, y], bb,
        /* every member's starting geometry, so the whole selection is
           transformed from ONE origin instead of each drifting on its own */
        p0: list.map((o) => (o.p || []).slice()),
        r0: list.map((o) => o.r || 0),
        fs0: list.map((o) => o.fs || 0.036),
        a0: Math.atan2(y - (bb.y + bb.h / 2), x - (bb.x + bb.w / 2)),
        lock: !!__mk.lockRatio,
      };
      return;
    }

    if (__mk.tool === "pan") {
      e.preventDefault();
      try { stage.setPointerCapture && stage.setPointerCapture(e.pointerId); } catch (err) {}
      __mk.panFrom = { x: e.clientX, y: e.clientY, px: __mk.px, py: __mk.py };
      return;
    }

    if (__mk.tool === "select") {
      e.preventDefault();
      mkCloseEditor();
      const hit = mkHitItem(x, y);
      const additive = e.shiftKey || e.metaKey || e.ctrlKey || __mkMulti;
      /* alt reaches PAST the group to the one object inside it — the same
         gesture Illustrator, Figma and Keynote all use */
      const reach = mkReachOf(hit, e.altKey);

      if (hit < 0) {
        /* empty sheet: a click clears, a DRAG rubber-bands. Shift keeps what
           is already selected and adds to it. */
        const base = additive ? mkSelIdx() : [];
        if (!additive) mkSelSet([]);
        __mk.marq = { x0: x, y0: y, x, y, add: additive, live: false, base };
        mkChrome();
        mkPaintLayerSel(true);
        return;
      }
      if (additive) {
        mkSelToggle(reach);
        mkChrome();
        mkPaintLayerSel(true);
        return;                       /* a shift-click selects; it never drags */
      }
      /* already inside the selection? Then this is a drag of the WHOLE
         selection, not a demand to start a new one from this object. */
      if (!reach.every((i) => mkIsSelIdx(i))) mkSelSet(reach);
      const list = mkSelItems();
      if (list.length) {
        mkPushUndo();
        __mk.drag = { kind: "move", list, from: [x, y],
                      p0: list.map((o) => (o.p || []).slice()) };
      }
      mkChrome();
      /* picking something up on the sheet highlights its row in the layer
         list, and vice versa — one selection, two places you can see it */
      mkPaintLayerSel(true);
      return;
    }

    if (__mk.tool === "erase") {
      e.preventDefault();
      mkEraserCursorAt(x, y);
      if (mkEraseHit(x, y) < 0) return;
      try { stage.setPointerCapture && stage.setPointerCapture(e.pointerId); } catch (err) {}
      mkPushUndo();
      __mk.eraseDraw = { pointerId: e.pointerId, last: -1 };
      mkEraseAddPoint(x, y);
      mkRedraw();
      return;
    }

    if (__mk.tool === "wand") {
      e.preventDefault();
      mkWandAt(x, y);
      return;
    }

    if (__mk.tool === "bucket") {
      e.preventDefault();
      /* ── OUTLINE IS THE THIRD BUCKET, NOT AN EXIT ────────────────────────
         It used to be coerced to Engrave here, which is why Outline was the
         one instruction you could not simply hover-and-tap: the only way to
         say it was to fill an area with something else first and then
         convert it. All three instructions are now placed the same way. */
      const want = mkFillIntent(__mk.fill);

      /* ALREADY FILLED? Then this click is about that fill, not about a new
         one. Same instruction → take it back; different instruction → change
         it. One gesture, no modifier, nothing to learn. */
      const hitFill = mkFillUnderPoint(x, y);
      if (hitFill >= 0) {
        const it = __mk.items[hitFill];
        mkPushUndo();
        if (mkFillIntent(it.f) === want) {
          /* SAME INSTRUCTION AGAIN MEANS "UNDO THIS", and what undoing means
             depends entirely on what the thing IS.

             A fill the bucket made is an instruction and nothing else, so
             taking it back removes it. A filled shape that arrived in an
             .svg or .ai, or came out of taking a generated drawing apart, is
             the customer's ARTWORK — and deleting somebody's logo because
             they clicked it with the engraving tool is the worst thing this
             studio could do with a click. For those, "un-engrave" means what
             it says: the shape stays, its interior goes back to plain metal,
             and its outline is still engraved. */
          if (it.bk) {
            __mk.items.splice(hitFill, 1);
            mkSelSet([]);
            mkTouch();
            toast("Taken back — that area is plain metal again", "gold");
          } else {
            it.f = "none";
            mkSelSet([hitFill]);
            mkTouch();
            toast("Outline only — its inside is plain polished metal now", "gold");
          }
        } else {
          /* only `f` — the instruction. `c` is the object's own ink, and a
             traced part can be traced in any colour; overwriting it here
             threw that away permanently for no benefit, now that the
             painter takes a fill's colour from its INSTRUCTION. */
          it.f = want === "cutout" ? MK_CUTOUT : want === "none" ? "none" : MK_ENGRAVE;
          /* ONLY THE BUCKET'S OWN FILLS ARE NAMED BY THE BUCKET. Renaming an
             imported shape because its engraving instruction changed takes
             away the name the customer finds it by in the layer list —
             "logo 2" becomes "Cut-out area" and their own artwork is gone
             from under them. */
          if (it.bk) it.n = mkRegionName({ frac: 0, onPicture: false }, want);
          mkSelSet([hitFill]);
          mkTouch();
          toast(mkFillMode(want).say, "gold");
        }
        mkFillPreviewClear();
        return;
      }

      const blocked = mkFillBlockedAt(x, y);
      const res = mkBucketFill(x, y);
      if (blocked && (!res || res.onLine || res.openRegion)) {
        toast(blocked.lok
          ? `“${mkLayerName(blocked)}” is locked — unlock it in Layers and try again`
          : `“${mkLayerName(blocked)}” is hidden — turn it back on in Layers and try again`, "err");
        return;
      }
      if (!res) { toast("Nothing enclosed under there — try inside a shape", "err"); return; }
      if (res.onLine) { toast("Nothing to fill just there", "err"); return; }
      if (res.openRegion) {
        toast("That's the space around the charm, not an area of it — the outline has to close first", "err");
        return;
      }
      mkPushUndo();
      const at = mkPlaceFill(res.item, res.region);
      mkSelSet([at]);
      mkTouch();
      mkFillPreviewClear();
      toast(mkFillMode(want).say, "gold");
      return;
    }

    if (MK_TEXT_TOOLS.indexOf(__mk.tool) >= 0) {
      e.preventDefault();
      /* words already there? Then this is an edit, not a second note stacked
         on the first. Wanting to change what you wrote is the commonest
         thing anyone does with text. */
      const hitTxt = mkHitText(x, y);
      if (hitTxt >= 0) {
        const keep = __mk.tool;
        mkEditItem(hitTxt);
        __mk.tool = keep;
        return;
      }
      /* preventDefault is the note-tool fix: without it the browser moves
         focus on pointerup and the fresh editor is torn down before a
         keystroke can reach it. */
      mkPushUndo();
      const it = mkPack({
        id: mkId(), t: __mk.tool, c: __mk.color, w: __mk.w, ff: __mk.ff,
        fs: __mk.tool === "label" ? Math.max(0.05, __mk.fs) : __mk.fs,
        p: __mk.tool === "callout" ? [x, y, x + 0.16, y - 0.14] : [x, y], s: "",
      });
      __mk.items.push(it);
      __mk.sel = __mk.items.length - 1;
      mkTouch();
      mkOpenEditor(it);
      return;
    }

    /* ── everything that is drawn by dragging ── */
    e.preventDefault();
    try { stage.setPointerCapture && stage.setPointerCapture(e.pointerId); } catch (err) {}
    const kind = MK_PATH_TOOLS[__mk.tool];
    __mk.live = mkPack({
      id: mkId(),
      t: kind || __mk.tool,
      c: __mk.color,
      /* ── A PRIMITIVE IS BORN AN OUTLINE. ALWAYS. ─────────────────────────
         `__mk.fill` is the bucket's load, and it used to be this too — what
         a new shape came pre-filled with. Two jobs, one variable, and the
         moment the engraving chips became sticky (which the bucket needed),
         one visit to Engrave meant every circle drawn afterwards arrived
         solid black. The jobs are separate now: the chips ARE the bucket
         and nothing else, and a drawn shape starts as its outline — the
         instruction is given the same way it is given to everything else,
         by lighting the area up and tapping it. */
      f: "none",
      w: __mk.tool === "highlight" ? Math.max(0.03, __mk.w * 3) : __mk.w,
      p: [x, y, x, y],
    });
    if (kind) __mk.live.p = [x, y];
    __mk.liveFrom = [x, y];
  });

  stage.addEventListener("pointermove", (e) => {
    if (!__mk) return;
    if (PTRS.has(e.pointerId)) PTRS.set(e.pointerId, { x: e.clientX, y: e.clientY });

    /* pinch + two-finger pan, together, in one gesture */
    if (__mk.gesture && twoFinger()) {
      e.preventDefault();
      const g = gestureFrom(), G = __mk.gesture;
      const r = stage.getBoundingClientRect();
      const z = Math.min(8, Math.max(1, G.z0 * (g.d / Math.max(8, G.d0))));
      /* keep the point between the fingers under the fingers, then add
         whatever the midpoint itself travelled */
      const ax = G.cx - r.left, ay = G.cy - r.top;
      __mk.px = ax - (ax - G.px0) * (z / G.z0) + (g.cx - G.cx);
      __mk.py = ay - (ay - G.py0) * (z / G.z0) + (g.cy - G.cy);
      __mk.z = z;
      if (z === 1) { __mk.px = 0; __mk.py = 0; }
      mkClampView();
      mkApplyView();
      return;
    }

    const [x, y] = norm(e);

    if (__mk.tool === "erase") {
      mkEraserCursorAt(x, y);
      if (__mk.eraseDraw && __mk.eraseDraw.pointerId === e.pointerId) {
        e.preventDefault();
        let evs = (typeof e.getCoalescedEvents === "function") ? e.getCoalescedEvents() : [e];
        if (!evs || !evs.length) evs = [e];
        evs.forEach((ce) => { const q = norm(ce); mkEraseAddPoint(q[0], q[1]); });
        mkRedraw();
        return;
      }
    }

    /* the fill preview follows the cursor whenever the bucket is in hand */
    if (__mk.tool === "bucket" && !__mk.drag && !__mk.live && !__mk.marq) mkFillPreviewAt(x, y);

    if (__mk.cropping && __mk.cropFrom) {
      const f = __mk.cropFrom;
      /* square by construction — see mkStartCrop */
      const w = Math.min(Math.abs(x - f.x), Math.abs(y - f.y));
      __mk.cropping.rect = { x: x >= f.x ? f.x : f.x - w, y: y >= f.y ? f.y : f.y - w, w };
      const ca1 = $("#mkCropApply"); if (ca1) ca1.disabled = w < 0.05;
      mkPaintCropRect();
      return;
    }

    if (__mk.panFrom) {
      __mk.px = __mk.panFrom.px + (e.clientX - __mk.panFrom.x);
      __mk.py = __mk.panFrom.py + (e.clientY - __mk.panFrom.y);
      mkClampView();
      mkApplyView();
      return;
    }

    /* ── the rubber band ── */
    if (__mk.marq) {
      const m = __mk.marq;
      m.x = x; m.y = y;
      if (!m.live && (Math.abs(x - m.x0) > 0.012 || Math.abs(y - m.y0) > 0.012)) m.live = true;
      if (m.live) { mkPaintMarquee(); mkMarqueeSelect(); }
      return;
    }

    if (__mk.drag) {
      const d = __mk.drag;
      /* ONE transform, applied to every member from its own starting
         geometry. With a single object selected the maths is identical to
         what it always was — a union of one is just that one. */
      const list = d.list || [];
      if (d.kind === "move") {
        let dx = x - d.from[0], dy = y - d.from[1];
        const put = (ddx, ddy) => list.forEach((it, n) => {
          const p = it.p, s = d.p0[n];
          for (let k = 0; k + 1 < p.length; k += 2) { p[k] = s[k] + ddx; p[k + 1] = s[k + 1] + ddy; }
        });
        put(dx, dy);
        /* now that the objects are where the hand put them, ask what they
           agree with and take the nudge. ALT suspends the whole thing. */
        const nudge = mkAlignApply(list, "move", e.altKey);
        if (nudge.dx || nudge.dy) put(dx + nudge.dx, dy + nudge.dy);
      } else if (d.kind === "resize") {
        const bb = d.bb;
        /* the opposite corner is the anchor — the corner you are NOT holding
           stays exactly where it is, which is what makes a resize feel like a
           resize rather than a move */
        const ax = d.h === "nw" || d.h === "sw" ? bb.x + bb.w : bb.x;
        const ay = d.h === "nw" || d.h === "ne" ? bb.y + bb.h : bb.y;
        let sx = Math.max(0.02, Math.abs(x - ax) / Math.max(1e-4, bb.w));
        let sy = Math.max(0.02, Math.abs(y - ay) / Math.max(1e-4, bb.h));
        /* SHIFT — or a picture, or a two-finger pinch on a handle — holds the
           aspect ratio. One scale for both axes, taken from whichever the hand
           moved further, so the object follows the corner without distorting.
           Pictures default to locked because a stretched photograph is never
           what was meant; shift then RELEASES them, the same way it does in
           every editor people already know. */
        const anyImg = list.some((o) => o.t === "img");
        const wantLock = anyImg ? !e.shiftKey : (e.shiftKey || !!d.lock);
        if (wantLock) {
          const s = Math.abs(sx - 1) > Math.abs(sy - 1) ? sx : sy;
          sx = s; sy = s;
        }
        list.forEach((it, n) => {
          const p = it.p, s0 = d.p0[n];
          for (let k = 0; k + 1 < p.length; k += 2) {
            p[k] = ax + (s0[k] - ax) * sx;
            p[k + 1] = ay + (s0[k + 1] - ay) * sy;
          }
          /* type scales with its box rather than being stretched by it */
          if (it.t === "label" || it.t === "text" || it.t === "callout") {
            it.fs = Math.min(0.4, Math.max(0.012, (d.fs0[n] || 0.036) * (wantLock ? sx : (sx + sy) / 2)));
          }
        });
        /* ONLY THE CORNER IN THE HAND MAY SNAP — and ONE AXIS AT A TIME.
           The allowed ends used to be passed as a single flat list for both
           axes, so on the ne and sw handles (where the pair is mixed) each
           axis accepted BOTH ends, including the anchored one: the fixed
           corner moved, the object resized for an alignment that did not
           exist, and the guide was painted through nothing. */
        const eX = (d.h === "nw" || d.h === "sw") ? "lo" : "hi";
        const eY = (d.h === "nw" || d.h === "ne") ? "lo" : "hi";
        const nudge = mkAlignApply(list, "resize", e.altKey, [eX], [eY]);
        if (nudge.dx || nudge.dy) {
          const bb2 = mkUnionDrawn(list);
          let nx = bb2.w ? (bb2.w + (eX === "hi" ? nudge.dx : -nudge.dx)) / bb2.w : 1;
          let ny = bb2.h ? (bb2.h + (eY === "hi" ? nudge.dy : -nudge.dy)) / bb2.h : 1;
          /* A LOCKED RESIZE STILL DESERVES HELP. Pictures are locked by
             default, so "no snapping while locked" meant every imported
             logo got none at all. One scale, taken from whichever axis
             actually found an agreement. */
          if (wantLock) {
            const useX = Math.abs(nudge.dx) > 0 && (!nudge.dy || Math.abs(nudge.dx) <= Math.abs(nudge.dy));
            const k = useX ? nx : ny;
            nx = k; ny = k;
          }
          if (isFinite(nx) && isFinite(ny) && nx > 0.02 && ny > 0.02 &&
              nx < 50 && ny < 50) {
            list.forEach((it, n2) => {
              const p = it.p;
              for (let k = 0; k + 1 < p.length; k += 2) {
                p[k] = ax + (p[k] - ax) * nx;
                p[k + 1] = ay + (p[k + 1] - ay) * ny;
              }
              if (it.t === "label" || it.t === "text" || it.t === "callout") {
                it.fs = Math.min(0.4, Math.max(0.012, it.fs * (wantLock ? nx : (nx + ny) / 2)));
              }
              void n2;
            });
            /* and the read-out must describe the box that ENDED UP here,
               not the one the hand asked for: a resize scales the box and
               the preview was merely translating it */
            mkAlignReadout(mkUnionDrawn(list), "resize");
          }
        }
      } else if (d.kind === "rotate") {
        const bb = d.bb, cx = bb.x + bb.w / 2, cy = bb.y + bb.h / 2;
        let dr = Math.atan2(y - cy, x - cx) - d.a0;
        if (e.shiftKey && list.length === 1) {
          dr = Math.round((d.r0[0] + dr) / (Math.PI / 12)) * (Math.PI / 12) - d.r0[0];
        } else if (e.shiftKey) {
          dr = Math.round(dr / (Math.PI / 12)) * (Math.PI / 12);
        }
        const c = Math.cos(dr), s = Math.sin(dr);
        list.forEach((it, n) => {
          it.r = d.r0[n] + dr;
          /* several objects turn ABOUT THE SELECTION's centre, not each about
             its own — otherwise a grouped face would spin its eyes in place
             and leave them where they were */
          if (list.length > 1) {
            const p = it.p, s0 = d.p0[n];
            for (let k = 0; k + 1 < p.length; k += 2) {
              const dx = s0[k] - cx, dy = s0[k + 1] - cy;
              p[k] = cx + dx * c - dy * s;
              p[k + 1] = cy + dx * s + dy * c;
            }
          }
        });
      }
      mkRedraw();
      mkRenderSelection();
      return;
    }

    if (!__mk.live) return;
    const kind = MK_PATH_TOOLS[__mk.tool];
    if (kind) {
      const p = __mk.live.p;
      const lx = p[p.length - 2], ly = p[p.length - 1];
      const dx = x - lx, dy = y - ly;
      if (dx * dx + dy * dy < 0.000009) return;    /* decimate — the doc stays small */
      p.push(x, y);
    } else {
      const f = __mk.liveFrom;
      let ex = x, ey = y;
      if (e.shiftKey) {
        if (__mk.live.t === "line" || __mk.live.t === "arrow") {
          if (Math.abs(x - f[0]) > Math.abs(y - f[1])) ey = f[1]; else ex = f[0];
        } else {
          const s = Math.max(Math.abs(x - f[0]), Math.abs(y - f[1]));
          ex = f[0] + Math.sign(x - f[0] || 1) * s;
          ey = f[1] + Math.sign(y - f[1] || 1) * s;
        }
      }
      __mk.live.p = [f[0], f[1], ex, ey];
    }
    mkRedraw();
  });

  const commit = (e) => {
    if (!__mk) return;
    if (e && e.pointerId != null) PTRS.delete(e.pointerId);
    if (__mk.eraseDraw && (!e || e.pointerId == null || __mk.eraseDraw.pointerId === e.pointerId)) {
      __mk.eraseDraw = null;
      mkTouch();
      mkRedraw();
      return;
    }
    if (__mk.gesture) {
      /* the gesture ends when the SECOND finger leaves; the one still down
         must not suddenly start drawing from where it happens to be */
      if (PTRS.size < 2) __mk.gesture = null;
      return;
    }
    if (__mk.cropFrom) { __mk.cropFrom = null; return; }
    if (__mk.panFrom) { __mk.panFrom = null; return; }
    if (__mk.marq) {
      const live = __mk.marq.live;
      __mk.marq = null;
      mkPaintMarquee();
      if (live) { mkChrome(); mkRenderLayersSoon(); }
      return;
    }
    if (__mk.drag) { __mk.drag = null; mkAlignClear(); mkTouch(); return; }
    if (!__mk.live) return;
    const live = __mk.live;
    __mk.live = null;
    const kind = MK_PATH_TOOLS[__mk.tool];
    /* a shape dragged to nothing is a misfire, not a mark */
    if (!kind && Math.hypot(live.p[2] - live.p[0], live.p[3] - live.p[1]) < 0.02) { mkRedraw(); return; }
    if (kind && live.p.length > 1200) live.p = live.p.filter((_, i) => Math.floor(i / 2) % 2 === 0);
    mkPushUndo();
    __mk.items.push(live);
    /* symmetry: one hand, two halves — the single most useful thing you can
       give someone drawing a charm freehand */
    if (__mk.guides && __mk.guides.mirror) {
      const cp = mkPack(live); cp.id = mkId();
      for (let k = 0; k + 1 < cp.p.length; k += 2) cp.p[k] = 1 - cp.p[k];
      __mk.items.push(cp);
    }
    if (__mk.items.length > 400) __mk.items.shift();
    if (live.t === "sign") mkSaveSignature(live);
    mkTouch();
  };
  stage.addEventListener("pointerleave", () => {
    mkFillPreviewClear();
    if (!__mk || !__mk.eraseDraw) mkEraserCursorClear();
  });
  stage.addEventListener("pointerup", commit);
  stage.addEventListener("pointercancel", commit);
  /* A press that ENDS off the stage never reaches the listeners above — the
     finger slides out, the pointerup lands elsewhere, and the stale entry
     left in the gesture map makes the NEXT touch read as a second finger.
     The window sees every pointerup, so the map is purged there too. */
  window.addEventListener("pointerup", (e) => {
    PTRS.delete(e.pointerId);
    if (__mk && __mk.gesture && PTRS.size < 2) __mk.gesture = null;
  });
  window.addEventListener("pointercancel", (e) => {
    PTRS.delete(e.pointerId);
    if (__mk && __mk.gesture && PTRS.size < 2) __mk.gesture = null;
  });

  /* ── the toolbar ── */
  $$(".mk-tool").forEach((b) => b.addEventListener("click", (e) => {
    if (!__mk) return;
    if (e.target.closest("[data-caret]")) return;    /* the caret opens the menu */
    /* Picture and More are pure menus — they hold no tool of their own. Left
       to fall through, this set the active tool to `undefined`, which broke
       whatever was in hand AND stopped the menu opening: pressing "Picture"
       looked like it did nothing, which is exactly what it did. */
    if (!b.dataset.tool) return;
    mkSetTool(b.dataset.tool);
  }));
  $$(".mk-drop__menu .mk-item[data-tool]").forEach((b) => b.addEventListener("click", () => {
    mkCloseAllDrops();
    if (__mk) mkSetTool(b.dataset.tool);
  }));
  $("#mkUndo").addEventListener("click", () => {
    if (!__mk || !__mk.undo.length) return;
    __mk.redo.push(mkSnapshot());
    mkRestore(__mk.undo.pop());
  });
  $("#mkRedo").addEventListener("click", () => {
    if (!__mk || !__mk.redo.length) return;
    __mk.undo.push(mkSnapshot());
    mkRestore(__mk.redo.pop());
  });
  const eraseSize = $("#mkEraseSize");
  if (eraseSize) eraseSize.addEventListener("input", () => {
    if (!__mk) return;
    __mk.eraseSize = Math.max(0.008, Math.min(0.12, Number(eraseSize.value) / 100));
    const out = $("#mkEraseSizeVal");
    if (out) out.textContent = Number(eraseSize.value).toFixed(1) + "%";
  });
  $("#mkDelete").addEventListener("click", mkDeleteSelected);
  $("#mkEditSketch").addEventListener("click", () => {
    if (!__mk) return;
    closeMarkup();
    renderStage();
    openCompose();
    toast("Your sketch, every piece still yours — submit again when it's right ✎", "gold");
  });
  $("#mkFull").addEventListener("click", () => {
    const inner = document.querySelector("#markupModal .markup__inner");
    const on = !inner.classList.contains("is-full");
    inner.classList.toggle("is-full", on);
    $("#mkFull").classList.toggle("is-on", on);
    setTimeout(() => { if (__mk && __mk.resize) __mk.resize(); }, 60);
  });

  /* ── the property bar ── */
  $$('.mk-drop[data-drop="weight"] .mk-item--w').forEach((b) =>
    b.addEventListener("click", () => { mkCloseAllDrops(); mkApplyProp({ w: parseFloat(b.dataset.w) }); }));
  $$(".mk-item--font").forEach((b) =>
    b.addEventListener("click", () => { mkCloseAllDrops(); mkApplyProp({ ff: b.dataset.ff }); }));
  $$("#mkSizes .mk-size").forEach((b) =>
    b.addEventListener("click", () => mkApplyProp({ fs: parseFloat(b.dataset.fs) })));
  $$(".mk-item[data-arr]").forEach((b) =>
    b.addEventListener("click", () => { mkCloseAllDrops(); mkArrange(b.dataset.arr); }));

  /* ── the drawing, as objects or as the picture it came from ──────────── */
  const retrace = $("#mkRetrace");
  if (retrace) retrace.addEventListener("click", async () => {
    mkCloseAllDrops();
    if (!__mk || __mk.compose) { toast("There's no generated drawing here to take apart", "err"); return; }
    const v = state.versions[state.currentVersion];
    if (!v || String(v.n) !== __mk.key) { toast("Open the drawing you want taken apart first", "err"); return; }
    if (__mk.items.length && __mk.dirty) {
      const yes = await askConfirm({
        title: "Take this drawing apart again?",
        body: `The ${__mk.items.length} object${__mk.items.length === 1 ? "" : "s"} on this sheet ` +
              "will be replaced by a fresh trace of the generated picture. Anything you have " +
              "moved, added or deleted here goes with them.",
        yes: "Trace it again",
      });
      if (!yes) return;
    }
    mkPushUndo();
    __mk.items = [];
    __mk.traced = false;
    toast("Taking it apart…", "gold");
    const res = await traceVersion(v, { force: true });
    if (!res) {
      toast("This one doesn't come apart cleanly — it isn't plain linework. The picture is untouched.", "err");
      $("#mkUndo").click();
      return;
    }
    __mk.items = res.items.map(mkPack);
    __mk.traced = true;
    __mk.dirty = false;
    __mk.showBitmap = false;
    mkApplyBase();
    mkTouch();
    mkRenderLayers();
    toast(`${res.items.length} objects — all yours`, "gold");
  });
  const showBmp = $("#mkShowBitmap");
  if (showBmp) showBmp.addEventListener("click", () => {
    if (!__mk) return;
    if (!__mk.traced) { toast("This sheet is already the original picture", "err"); return; }
    __mk.showBitmap = !__mk.showBitmap;
    showBmp.classList.toggle("is-on", !!__mk.showBitmap);
    mkApplyBase();
    toast(__mk.showBitmap
      ? "Showing the picture it came from — your objects are still there, underneath"
      : "Back to the objects", "gold");
  });
  /* [data-guide], not every toggle in the studio: this used to catch "Show
     the original picture" too and set __mk.guides[undefined] on every click */
  $$(".mk-item--tog[data-guide]").forEach((b) => b.addEventListener("click", () => {
    if (!__mk) return;
    if (!__mk.guides) __mk.guides = {};
    __mk.guides[b.dataset.guide] = !__mk.guides[b.dataset.guide];
    mkTouch();
  }));
  const gn = $("#mkGridN");
  if (gn) {
    const show = () => {
      const lbl = $("#mkGridNVal");
      if (lbl) lbl.textContent = String(__mk ? (__mk.gridN || 12) : 12);
    };
    gn.addEventListener("input", () => {
      if (!__mk) return;
      __mk.gridN = Math.max(4, Math.min(48, Number(gn.value) || 12));
      /* a finer grid is only useful if you can see it */
      if (!__mk.guides.grid) __mk.guides.grid = true;
      show();
      mkTouch();
    });
    gn.addEventListener("pointerdown", (e) => e.stopPropagation());
  }
  const fade = $("#mkBaseFade");
  if (fade) fade.addEventListener("input", () => {
    if (!__mk) return;
    __mk.baseFade = Math.max(0.06, fade.value / 100);
    mkApplyBase();
  });

  /* swatches are built from one palette, so stroke and fill can never drift */


  /* ── the engraving control ─────────────────────────────────────────────
     One click sets what the next shape will be AND re-states whatever is
     selected, because those are the same intention expressed at different
     moments. The toast says what was chosen in the workshop's words, every
     time, until the customer has seen each of the three at least once. */
  $$("#mkMetal .mk-metal__b").forEach((b) => b.addEventListener("click", () => {
    if (!__mk) return;
    const mode = mkFillMode(b.dataset.fill);
    const want = mkFillIntent(mode.f);

    /* ── THIS CONTROL IS THE BUCKET. ALL THREE OF IT. ────────────────────
       Engrave, Cut out and Outline are three ways of saying the same kind
       of thing — "this area is worked like THIS" — so all three hand you
       the bucket and all three light up under the cursor. Outline used to
       be the exception in three separate places: it was excluded from
       `wantsFill`, so picking it PUT THE BUCKET DOWN; it was excluded from
       `armed` in mkChrome, so its chip could never show as held; and the
       pointer handler coerced it to Engrave. Three gates, one instruction,
       and the result was a control that looked like a set of three and
       behaved like a set of two.

       AND IT NEVER ACTS ON A SELECTION. Clicking a chip while something
       was selected re-stated that object immediately, which meant the
       gesture had two entirely different outcomes depending on a state the
       customer may not have noticed they were in — and made it impossible
       to simply pick up a colour and go hunting with it. Picking a chip now
       does exactly one thing: it drops the selection, picks up that
       instruction, and arms the hover. Changing an object that already has
       an instruction is done the same way as everything else — hover it and
       click, which the preview has always offered and now offers for all
       three. */
    mkSelSet([]);
    __mk.fill = mode.f;
    /* NOTHING TO FILL, NOTHING TO PICK UP. On an empty sheet the bucket has
       no area to work on, and arming it there means the customer's first
       stroke is swallowed and answered with "that's the space around the
       charm" — on a sheet where the coach mark points at this very control
       and says "start here". With nothing drawn yet the chip does what it
       has always done: it says what the next shape will be. */
    const fillable = __mk.items.length > 0 || !!(__mk.baseRef && !__mk.traced);
    if (fillable) {
      if (__mk.tool !== "bucket") mkSetTool("bucket");
      __mk.fill = mode.f;                    /* mkSetTool must not re-aim it */
      $("#markupHint").textContent = want === "cutout"
        ? "Move over the sheet — the area that will be CUT CLEAN THROUGH lights up in blue. Click to take it, click again to undo"
        : want === "none"
        ? "Move over the sheet — the area that will be OUTLINED lights up in red. Click to take it, click again to undo"
        : "Move over the sheet — the area that will be ENGRAVED lights up in black. Click to take it, click again to undo";
      __mkPrev.key = "";
      if (__mkPrev.at) mkFillPreviewPaint(__mkPrev.at[0], __mkPrev.at[1]);
    }
    mkChrome();

    if (!state.metalSeen) state.metalSeen = {};
    const first = !state.metalSeen[mode.id];
    state.metalSeen[mode.id] = 1;
    saveSession();
    toast(mode.say + (fillable ? " — now tap an area."
                               : " — draw something first, then tap it."), "gold");
    if (first) mkMetalCoachSeen();
  }));

  /* ── signatures ── */
  $("#mkSignList").addEventListener("click", (e) => {
    const x = e.target.closest("[data-sigx]");
    if (x) {
      e.stopPropagation();
      (state.signatures || []).splice(+x.dataset.sigx, 1);
      mkSignList(); saveSession();
      return;
    }
    const s = e.target.closest("[data-sig]");
    if (s) { mkCloseAllDrops(); mkPlaceSignature(+s.dataset.sig); }
  });
  $$('.mk-item[data-sign="new"]').forEach((b) =>
    b.addEventListener("click", () => { mkCloseAllDrops(); mkSetTool("sign"); toast("Write across the sheet — we'll keep it", "gold"); }));

  /* ── the mode control ── */
  $$("#markupMode .mk-mode").forEach((b) => b.addEventListener("click", () => {
    if (!__mk) return;
    __mk.mode = b.dataset.mode === "exact" ? "exact" : "interpret";
    mkTouch();
  }));

  /* ── closing ──────────────────────────────────────────────────────────
     Closing SAVES, but that is no longer load-bearing: everything was
     already written the moment it was made. This just makes it immediate. */
  $$("#markupModal [data-close]").forEach((el) =>
    el.addEventListener("click", () => {
      if (!__mk) return;
      closeMarkup();
      renderStage();
    }, true));

  $("#markupSave").addEventListener("click", () => {
    if (!__mk) return;
    const key = __mk.key, has = __mk.items.length > 0;
    closeMarkup();
    renderStage();
    if (has) {
      pushMsg({ role: "studio", text: "Marks saved on v" + key + " ✎ — press send (no words needed) and we'll follow them." });
      renderThread();
      toast("Marks saved — press send and we'll follow them ✎", "gold");
    }
  });
  /* the source switch: the sheet AND what the gold button will re-draw from */
  document.querySelectorAll("#markupSrc .mk-src").forEach((b) => {
    b.addEventListener("click", () => mkSrcSet(b.dataset.src));
  });
  /* The marks ARE the message: close and regenerate at once, no typing. */
  $("#markupRegen").addEventListener("click", async () => {
    if (!__mk) return;
    const has = __mk.items.length > 0;
    if (!has) {
      closeMarkup(); renderStage();
      toast("Draw or note something first — the marks are the message", "err");
      return;
    }
    if (!(await mkMetalCheck())) {
      SD("markupRegen: BLOCKED — metal check declined");
      mkSetTool("select"); return;
    }
    SD("markupRegen: source =", __mkSrc);
    /* ON THE REFERENCE, "re-generate" means something different and better:
       the reference itself has been changed, so it is re-submitted as the
       reference and the drawing is made again FROM IT — not a refinement of
       a drawing that already went wrong. */
    if (__mkSrc === "ref") { await regenerateFromReference(); return; }
    closeMarkup();
    renderStage();
    await regenerateFromMarkup();
  });

  /* ── compose actions ── */
  $("#markupComposeClose").addEventListener("click", () => {
    const drew = !!(__mk && __mk.items.length);
    closeMarkup();
    saveSession(true);                       /* write it now, not in 500ms */
    renderDrawStage();
    if (typeof renderDrawer === "function" &&
        $("#designsDrawer").classList.contains("is-open")) renderDrawer();
    toast(drew ? "Saved — it's in My designs, ready when you are ✦"
               : "Closed — nothing drawn yet", drew ? "gold" : "err");
  });
  /* ── PANNING, WITHOUT A TOOL TO HOLD ─────────────────────────────────
     The Move tool has gone from the bar, and it should: reaching for a tool
     in order to look at your own drawing is not a tool, it is a toll. But
     the CAPABILITY had to survive, and on a desktop pointer it very nearly
     did not — two fingers and a pinch are a phone's answer. So the two
     gestures every drawing application already uses are wired directly:
     hold SPACE and drag, or drag with the middle button. Neither needs a
     button, and neither takes anything out of your hand. */
  (function wirePan() {
    const stage = $("#markupStage");
    if (!stage) return;
    let space = false, pan = null;
    document.addEventListener("keydown", (e) => {
      if (e.code === "Space" && __mk && !__mk.editing &&
          $("#markupModal").classList.contains("is-open")) {
        const t = e.target;
        if (t && (/^(INPUT|TEXTAREA|SELECT)$/.test(t.tagName) || t.isContentEditable)) return;
        space = true;
        stage.classList.add("is-panning");
        e.preventDefault();
      }
    });
    document.addEventListener("keyup", (e) => {
      if (e.code === "Space") { space = false; stage.classList.remove("is-panning"); }
    });
    window.addEventListener("blur", () => { space = false; stage.classList.remove("is-panning"); });
    stage.addEventListener("pointerdown", (e) => {
      if (!__mk) return;
      if (!space && e.button !== 1) return;
      e.preventDefault(); e.stopPropagation();
      pan = { x: e.clientX, y: e.clientY, px: __mk.px, py: __mk.py };
      try { stage.setPointerCapture(e.pointerId); } catch (er) {}
    }, true);
    stage.addEventListener("pointermove", (e) => {
      if (!pan || !__mk) return;
      e.preventDefault(); e.stopPropagation();
      __mk.px = pan.px + (e.clientX - pan.x);
      __mk.py = pan.py + (e.clientY - pan.y);
      mkApplyView();
    }, true);
    const end = (e) => {
      if (!pan) return;
      pan = null;
      try { stage.releasePointerCapture(e.pointerId); } catch (er) {}
    };
    stage.addEventListener("pointerup", end, true);
    stage.addEventListener("pointercancel", end, true);
  })();

  $("#markupUseDrawing").addEventListener("click", async () => {
    if (__mk && !(await mkMetalCheck("draw"))) { mkSetTool("select"); return; }
    submitComposedDrawing();
  });

  /* ── keyboard, the way an editor is expected to behave ── */
  document.addEventListener("keydown", (e) => {
    if (!__mk || !$("#markupModal").classList.contains("is-open")) return;
    if (__mk.editing) return;                       /* the textarea owns the keys */
    /* AND SO DOES EVERY OTHER FIELD. The layer-name box, the library search,
       the fade slider — all of them live inside this modal, and the panel
       that holds two of them is now permanently on screen. Without this,
       typing "outline" into a layer name reached none of it and left the
       ERASE tool in the customer's hand, so the next click deleted the
       layer they were trying to name. */
    const tgt = e.target;
    if (tgt && (/^(INPUT|TEXTAREA|SELECT)$/.test(tgt.tagName) || tgt.isContentEditable)) return;
    const meta = e.metaKey || e.ctrlKey;
    if (meta && e.key.toLowerCase() === "z") {
      e.preventDefault();
      (e.shiftKey ? $("#mkRedo") : $("#mkUndo")).click();
    } else if (meta && e.key.toLowerCase() === "g") {
      e.preventDefault();
      if (e.shiftKey) mkUngroupSelection(); else mkGroupSelection();
    } else if (meta && e.key.toLowerCase() === "a") {
      e.preventDefault();
      mkSelSet(__mk.items.map((it, i) => (it.hid || it.lok) ? -1 : i).filter((i) => i >= 0));
      if (__mk.tool !== "select") mkSetTool("select");
      mkChrome(); mkPaintLayerSel();
    } else if (e.key === "Delete" || e.key === "Backspace") {
      if (__mk.sel >= 0) { e.preventDefault(); mkDeleteSelected(); }
    } else if (e.key === "Escape") {
      if (__mk.cropping) { e.preventDefault(); mkEndCrop(false); }
      else if (__mk.sel >= 0) { e.preventDefault(); __mk.sel = -1; mkChrome(); }
    } else if (!meta && /^[vpharts]$/i.test(e.key)) {
      /* no "n" and no "e": the pinned note and the eraser are both gone from
         the studio, and a shortcut to a tool with no button is a tool the
         customer can arm by accident and has no way to see they are holding */
      const map = { v: "select", p: "draw", h: "highlight", a: "arrow", r: "rect",
                    t: "label", s: "sign" };
      const t = map[e.key.toLowerCase()];
      if (t) { e.preventDefault(); mkSetTool(t); }
    }
  });
})();

/* ═══════════ nested menus ════════════════════════════════════════════════
   Fixed-position and placed by script: both toolbar rows scroll horizontally
   when they run out of room, and a scroll container clips its children on
   both axes however you write overflow-y. Out of flow is the only version
   that cannot be guillotined by anything.                                   */
function mkCloseAllDrops() {
  $$(".mk-drop__menu").forEach((m) => { m.hidden = true; });
  $$(".mk-drop").forEach((d) => d.classList.remove("is-open"));
  $$(".mk-drop__b").forEach((b) => b.setAttribute("aria-expanded", "false"));
}
(function wireDrops() {
  $$(".mk-drop").forEach((drop) => {
    const btn = drop.querySelector(".mk-drop__b");
    const menu = drop.querySelector(".mk-drop__menu");
    if (!btn || !menu) return;
    btn.setAttribute("aria-haspopup", "true");
    btn.setAttribute("aria-expanded", "false");
    btn.addEventListener("click", (e) => {
      /* on a tool button the caret opens; on a property button the whole
         thing opens, because a property button has nothing else to do */
      const isTool = btn.classList.contains("mk-tool");
      /* a button with no tool of its own is a menu, whole and entire */
      const menuOnly = !btn.dataset.tool;
      const wantMenu = !isTool || menuOnly || !!e.target.closest("[data-caret]") ||
                       btn.classList.contains("is-on");
      if (!wantMenu) return;
      e.preventDefault();
      e.stopPropagation();
      const wasOpen = !menu.hidden;
      mkCloseAllDrops();
      if (wasOpen) return;
      menu.hidden = false;
      drop.classList.add("is-open");
      btn.setAttribute("aria-expanded", "true");
      const r = btn.getBoundingClientRect();
      menu.style.visibility = "hidden";
      menu.style.left = "0px"; menu.style.top = "0px";
      const mr = menu.getBoundingClientRect();
      let left = r.left, top = r.bottom + 5;
      if (left + mr.width > window.innerWidth - 8) left = Math.max(8, window.innerWidth - 8 - mr.width);
      if (top + mr.height > window.innerHeight - 8) top = Math.max(8, r.top - 5 - mr.height);
      menu.style.left = left + "px";
      menu.style.top = top + "px";
      menu.style.visibility = "";
    });
  });
  document.addEventListener("pointerdown", (e) => {
    /* On a phone the open menu is a bottom sheet with a scrim, and the scrim
       is a pseudo-element of .mk-drop itself — so "did the tap land inside a
       .mk-drop" is not the question. The question is whether it landed on the
       menu or the button that opens it. Anything else closes. */
    const inMenu = e.target.closest(".mk-drop__menu");
    const onBtn  = e.target.closest(".mk-drop__b");
    if (inMenu || onBtn) return;
    mkCloseAllDrops();
  }, true);
  document.addEventListener("keydown", (e) => { if (e.key === "Escape") mkCloseAllDrops(); });
  window.addEventListener("resize", mkCloseAllDrops);
})();


/* ═══════════════════════════════════════════════════════════════════════════
   THE DOCK — layers, and everything this account has ever made

   Two panels behind one surface, because they answer two halves of the same
   question: what is on this sheet, and what else could be.

     LAYERS   — the item array, top-down, with the handles you would expect
                from any editor: reorder by dragging, rename, hide, lock,
                opacity, duplicate, copy/paste, delete. The list is not a
                mirror of the model — it IS the model, rendered. There is no
                second structure to fall out of step.

     LIBRARY  — every image on the account: uploads, hand-drawn sketches, and
                every generated drawing with its metal charm kept beside it as
                one entry rather than two. It is DERIVED from state.sessions,
                which the studio already streams from Firestore with a single
                listener, so the whole repository costs no extra reads, no
                extra writes, and no extra listener however many people are
                using the studio at once. It updates in real time for exactly
                the same reason.
   ══════════════════════════════════════════════════════════════════════════ */

let __mkClip = null;                 /* the copy buffer — packed items */
/* THE PANEL IS NOT A MODE. Layers and the library used to hide behind two
   toggle buttons, which meant the answer to "what is on my sheet?" was a
   click away at all times and the buttons were the second and third things
   a customer had to learn. It is simply always there now — which is also
   why those two buttons no longer exist. */
let __mkDock = { open: true, tab: "layers", filter: "all", sort: "new", q: "" };

function mkDockEl() { return $("#mkDock"); }
/* ONE BREAKPOINT, NOT TWO. The CSS turns the panel into a bottom sheet
   below this width; the fold has to use the SAME number or there is a band
   of screen sizes — 821 to 900 px, every small laptop and landscape tablet —
   where the sketchpad opens with the library covering the drawing. */
const MK_DOCK_SHEET = 900;
function mkDockIsSheet() {
  return !!(window.matchMedia && window.matchMedia("(max-width: " + MK_DOCK_SHEET + "px)").matches);
}
function mkDockAutoFold() { mkDockMin(mkDockIsSheet()); }
function mkDockSet(open, tab) {
  const el = mkDockEl(); if (!el) return;
  if (tab) __mkDock.tab = tab;
  __mkDock.open = !!open;
  el.hidden = !open;
  document.querySelector("#markupModal .markup__inner").classList.toggle("has-dock", !!open);
  $$("#mkDock .mk-dock__tab").forEach((b) =>
    b.classList.toggle("is-on", b.dataset.dock === __mkDock.tab));
  $$("#mkDock .mk-dock__tab").forEach((b) =>
    b.setAttribute("aria-selected", b.dataset.dock === __mkDock.tab ? "true" : "false"));
  $$("#mkDock .mk-pane").forEach((p) =>
    p.classList.toggle("is-on", p.dataset.pane === __mkDock.tab));
  if (open && __mkDock.tab === "layers") mkRenderLayers();
  if (open && __mkDock.tab === "library") mkRenderLibrary();
  /* the stage is sized from the room left over, and the dock just took some */
  setTimeout(() => { if (__mk && __mk.resize) __mk.resize(); }, 60);
  setTimeout(() => { if (__mk && __mk.resize) __mk.resize(); }, 320);
}
/* On a phone the panel is a sheet across the bottom, and a sheet that can
   never get out of the way is a sheet in the way. So it folds down to its
   own tab bar instead of closing — always visible, always one tap from
   full size, and never a thing you have to know how to bring back. */
/* the footer's real height, measured rather than guessed, so the bottom
   sheet is pinned above the two buttons that submit the customer's work */
function mkDockFoot() {
  const el = mkDockEl(); if (!el) return;
  const foot = document.querySelector("#markupModal .markup-foot");
  /* the gap the sheet must leave is the distance from the bottom of the
     WINDOW to the top of the footer — not the footer's height. The modal has
     padding under it, and measuring the height alone parked the sheet on the
     top third of both buttons. `position:fixed` is measured against the
     viewport, so this is the same coordinate space. */
  let gap = 0;
  if (foot && foot.getClientRects().length) {
    gap = Math.max(0, Math.round(window.innerHeight - foot.getBoundingClientRect().top));
  }
  el.style.setProperty("--bj-dockfoot", gap + "px");
}
function mkDockMin(on) {
  const el = mkDockEl(); if (!el) return;
  __mkDock.min = !!on;
  el.classList.toggle("is-min", !!on);
  mkDockFoot();
  const x = $("#mkDockClose");
  if (x) {
    x.setAttribute("aria-expanded", on ? "false" : "true");
    x.setAttribute("aria-label", on ? "Show the panel" : "Fold the panel down");
    x.textContent = on ? "⌃" : "⌄";
  }
  setTimeout(() => { if (__mk && __mk.resize) __mk.resize(); }, 60);
}

/* ── layer thumbnails ────────────────────────────────────────────────────
   Each one is the real painter, run on a 48px canvas with that item alone
   and its box mapped to fill the tile — so the thumbnail cannot show
   something the sheet does not. */
function mkPaintThumb(cv, it) {
  const ctx = cv.getContext("2d");
  const S = cv.width;
  ctx.clearRect(0, 0, S, S);
  const bb = mkBBox(it);
  const pad = 0.12;
  const scale = (1 - pad * 2) / Math.max(0.02, Math.max(bb.w, bb.h));
  const one = mkPack(it);
  one.u = it.u; one.sp = it.sp;      /* transient copy: keep the source */
  one.hid = 0; one.o = 1; one.r = 0;
  one.p = (it.p || []).map((v, i) => {
    const base = i % 2 ? bb.y : bb.x;
    const centre = i % 2 ? (0.5 - bb.h * scale / 2) : (0.5 - bb.w * scale / 2);
    return centre + (v - base) * scale;
  });
  if (one.fs) one.fs = Math.min(0.5, one.fs * scale);
  if (one.w) one.w = Math.max(0.006, one.w * Math.min(3, scale));
  try { mkPaint(ctx, S, S, [one], { onImage: () => mkPaintThumb(cv, it) }); } catch (e) {}
}

/* ── the blocks the stack is really made of ──────────────────────────────
   A group's members are contiguous, so the stack reads as an ordered list
   of BLOCKS: a lone layer is a block of one, a group is a block of many.
   Reordering — by drag, by ⌘], by "bring to front" — is a reordering of
   blocks, which is why a group never gets something threaded through it. */
function mkBlocks() {
  const out = [], seen = {};
  const items = __mk.items;
  for (let i = 0; i < items.length; i++) {
    const g = mkGroupOf(items[i]);
    if (!g) { out.push({ g: "", idx: [i] }); continue; }
    if (seen[g]) continue;
    seen[g] = 1;
    out.push({ g, idx: mkGroupIdx(g) });
  }
  return out;
}
function mkApplyBlocks(blocks) {
  const next = [];
  blocks.forEach((b) => b.idx.forEach((i) => next.push(__mk.items[i])));
  __mk.items = next;
}

const MK_EYE_ON  = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M3 12s3.5-6 9-6 9 6 9 6-3.5 6-9 6-9-6-9-6Z"/><circle cx="12" cy="12" r="2.4"/></svg>';
const MK_EYE_OFF = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M3 3l18 18"/><path d="M10.6 6.3A9.6 9.6 0 0 1 12 6c5 0 9 4.5 9 6a11 11 0 0 1-2.6 3.4"/><path d="M6.2 8.6A11 11 0 0 0 3 12c0 1.5 4 6 9 6a9.5 9.5 0 0 0 3.5-.7"/></svg>';
const MK_LOCK_ON  = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="4.5" y="10.5" width="15" height="10"/><path d="M8 10.5V7a4 4 0 0 1 8 0v3.5"/></svg>';
const MK_LOCK_OFF = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="4.5" y="10.5" width="15" height="10"/><path d="M8 10.5V7a4 4 0 0 1 7.5-2"/></svg>';

function mkLayerRow(i, inGroup) {
  const it = __mk.items[i];
  const on = mkIsSelIdx(i);
  return `<div class="mk-layer${on ? " is-sel" : ""}${it.hid ? " is-hidden" : ""}${inGroup ? " mk-layer--child" : ""}" data-i="${i}" role="option" aria-selected="${on}" tabindex="0">
       <span class="mk-layer__grip" aria-hidden="true" title="Drag to reorder"><i></i><i></i><i></i></span>
       <canvas class="mk-layer__thumb" width="96" height="96" aria-hidden="true"></canvas>
       <span class="mk-layer__body">
         <span class="mk-layer__name">${escapeHtml(mkLayerName(it))}</span>
         <span class="mk-layer__meta">${escapeHtml(it.tr ? "From the drawing" : (MK_TYPE_NAME[it.t] || "Layer"))}${it.o < 1 ? " · " + Math.round(it.o * 100) + "%" : ""}${it.lok ? " · locked" : ""}</span>
       </span>
       <button class="mk-layer__b" data-act="eye" type="button" title="${it.hid ? "Show" : "Hide"} this layer" aria-label="${it.hid ? "Show" : "Hide"} this layer">${it.hid ? MK_EYE_OFF : MK_EYE_ON}</button>
       <button class="mk-layer__b" data-act="lock" type="button" title="${it.lok ? "Unlock" : "Lock"} this layer" aria-label="${it.lok ? "Unlock" : "Lock"} this layer">${it.lok ? MK_LOCK_ON : MK_LOCK_OFF}</button>
     </div>`;
}

function mkGroupRow(g, idx) {
  const open = mkGroupOpen(g);
  const on = idx.every((i) => mkIsSelIdx(i));
  const hid = idx.every((i) => __mk.items[i].hid);
  const lok = idx.every((i) => __mk.items[i].lok);
  return `<div class="mk-layer mk-layer--group${on ? " is-sel" : ""}${hid ? " is-hidden" : ""}${open ? " is-open" : ""}" data-g="${escapeHtml(g)}" data-i="${idx[idx.length - 1]}" role="option" aria-selected="${on}" aria-expanded="${open}" tabindex="0">
       <span class="mk-layer__grip" aria-hidden="true" title="Drag the whole group"><i></i><i></i><i></i></span>
       <button class="mk-layer__tw" data-act="twist" type="button" aria-label="${open ? "Collapse" : "Expand"} this group">${open ? "▾" : "▸"}</button>
       <span class="mk-layer__folder" aria-hidden="true"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linejoin="round"><path d="M3 7.5A1.5 1.5 0 0 1 4.5 6h4l2 2.5h7A1.5 1.5 0 0 1 19 10v7a1.5 1.5 0 0 1-1.5 1.5h-13A1.5 1.5 0 0 1 3 17Z"/></svg></span>
       <span class="mk-layer__body">
         <span class="mk-layer__name">${escapeHtml(mkGroupName(g))}</span>
         <span class="mk-layer__meta">${idx.length} layers${lok ? " · locked" : ""}</span>
       </span>
       <button class="mk-layer__b" data-act="eye" type="button" title="${hid ? "Show" : "Hide"} this group" aria-label="${hid ? "Show" : "Hide"} this group">${hid ? MK_EYE_OFF : MK_EYE_ON}</button>
       <button class="mk-layer__b" data-act="lock" type="button" title="${lok ? "Unlock" : "Lock"} this group" aria-label="${lok ? "Unlock" : "Lock"} this group">${lok ? MK_LOCK_ON : MK_LOCK_OFF}</button>
     </div>`;
}

function mkRenderLayers() {
  const host = $("#mkLayers");
  if (!host || !__mk) return;
  const items = __mk.items;
  const lc0 = $("#mkLayersCount");
  if (lc0) lc0.textContent = String(items.length);
  if (!items.length) {
    host.innerHTML = '<p class="mk-layers__empty">Nothing on the sheet yet.<br>Every mark you make appears here as its own layer — drag them to change what sits in front.<br><br>Shift-click to pick several, then group them so they move as one.</p>';
    mkLayerChrome();
    return;
  }
  /* topmost first, the way every editor lists them and the way the sheet
     actually reads — the last thing painted is the thing you see */
  const rows = [];
  const blocks = mkBlocks();
  for (let b = blocks.length - 1; b >= 0; b--) {
    const bl = blocks[b];
    if (!bl.g) { rows.push(mkLayerRow(bl.idx[0], false)); continue; }
    rows.push(mkGroupRow(bl.g, bl.idx));
    if (!mkGroupOpen(bl.g)) continue;
    for (let k = bl.idx.length - 1; k >= 0; k--) rows.push(mkLayerRow(bl.idx[k], true));
  }
  host.innerHTML = rows.join("");
  host.querySelectorAll(".mk-layer:not(.mk-layer--group)").forEach((row) => {
    const it = items[+row.dataset.i];
    if (it) mkPaintThumb(row.querySelector(".mk-layer__thumb"), it);
  });
  mkLayerChrome();
}

/* Selection changes far more often than the stack does, and rebuilding a
   list of live thumbnails on every click would make a shift-click feel
   expensive. This repaints ONLY what is highlighted — which is what keeps
   the sheet and the list in step with each other in real time. */
function mkPaintLayerSel(reveal) {
  const host = $("#mkLayers");
  if (!host || !__mk || !__mkDock.open || __mkDock.tab !== "layers") return;
  if (!host.querySelector(".mk-layer")) { mkRenderLayersSoon(); return; }
  host.querySelectorAll(".mk-layer").forEach((row) => {
    const g = row.dataset.g;
    const on = g ? mkGroupIdx(g).every((i) => mkIsSelIdx(i))
                 : mkIsSelIdx(+row.dataset.i);
    row.classList.toggle("is-sel", !!on);
    row.setAttribute("aria-selected", on ? "true" : "false");
  });
  /* Scrolling the list is only ever right when the selection came from
     somewhere ELSE — picking a thing up on the sheet should reveal its row.
     Doing it on every repaint would move the list out from under a finger
     that is in the middle of dragging a row down it. */
  const first = reveal && host.querySelector(".mk-layer.is-sel");
  if (first && first.scrollIntoView) {
    const hb = host.getBoundingClientRect(), rb = first.getBoundingClientRect();
    if (rb.top < hb.top || rb.bottom > hb.bottom) first.scrollIntoView({ block: "nearest" });
  }
  mkLayerChrome();
}

function mkLayerChrome() {
  const it = __mk && __mk.items[__mk.sel];
  const has = !!it;
  const n = __mk ? mkSelCount() : 0;
  ["mkLayerUp", "mkLayerDown", "mkLayerDup", "mkLayerCopy", "mkLayerDel"].forEach((id) => {
    const b = $("#" + id); if (b) b.disabled = !has;
  });
  const gb = $("#mkLayerGroup"), ub = $("#mkLayerUngroup");
  if (gb) gb.disabled = !(__mk && mkCanGroup());
  if (ub) ub.disabled = !(__mk && mkCanUngroup());
  const pasteBtn = $("#mkLayerPaste");
  if (pasteBtn) pasteBtn.disabled = !(__mkClip && __mkClip.length);
  const foot = $("#mkLayerFoot");
  if (foot) {
    foot.hidden = !has;
    if (has) {
      const nameEl = $("#mkLayerName");
      const g = n > 1 ? mkGroupOf(it) : "";
      /* with a whole group selected the name field renames the GROUP — the
         thing you are actually looking at */
      const gAll = g && mkGroupIdx(g).length === n;
      foot.dataset.scope = gAll ? "group" : "layer";
      const lbl = $("#mkLayerNameLbl");
      if (lbl) lbl.textContent = gAll ? "Group name" : (n > 1 ? `Name (${n} selected)` : "Name");
      if (document.activeElement !== nameEl) nameEl.value = gAll ? mkGroupName(g) : (it.n || "");
      nameEl.placeholder = gAll ? "Group" : (MK_TYPE_NAME[it.t] || "Layer");
      $("#mkLayerOpacity").value = String(Math.round((it.o == null ? 1 : it.o) * 100));
      $("#mkLayerOpacityVal").textContent = Math.round((it.o == null ? 1 : it.o) * 100) + "%";
    }
  }
}

/* selection is one idea, wherever it is expressed */
function mkSelect(i, how) {
  if (!__mk) return;
  const reach = (i >= 0 && i < __mk.items.length) ? mkReachOf(i, how === "alone") : [];
  if (how === "toggle") mkSelToggle(reach);
  else if (how === "range") mkSelRangeTo(i);
  else mkSelSet(reach);
  if (mkSelCount() && __mk.tool !== "select") mkSetTool("select");
  mkChrome();
  mkPaintLayerSel();
}
/* shift-click in the list: everything between the anchor and here, in the
   order the LIST shows them, so what gets selected is what looks selected */
function mkSelRangeTo(i) {
  const anchor = __mk.sel;
  if (anchor < 0) { mkSelSet(mkReachOf(i, false)); return; }
  const a = Math.min(anchor, i), b = Math.max(anchor, i);
  const run = [];
  for (let k = a; k <= b; k++) mkReachOf(k, false).forEach((z) => { if (run.indexOf(z) < 0) run.push(z); });
  /* the anchor stays the anchor: it goes on last */
  mkSelSet(run.filter((z) => z !== anchor).concat([anchor]));
}
function mkSelectGroup(g, how) {
  if (!__mk || !g) return;
  const idx = mkGroupIdx(g);
  if (!idx.length) return;
  if (how === "toggle") mkSelToggle(idx);
  else mkSelSet(idx);
  if (mkSelCount() && __mk.tool !== "select") mkSetTool("select");
  mkChrome();
  mkPaintLayerSel();
}

function mkLayerAct(what) {
  if (!__mk) return;
  if (what === "paste") {
    if (!(__mkClip && __mkClip.length)) return;
    mkPushUndo();
    /* a copied GROUP pastes as a group again — of its own, so renaming the
       copy never renames what it was copied from */
    const remap = {};
    const fresh = [];
    __mkClip.forEach((c) => {
      const cp = mkPack(c);
      cp.id = mkId();
      if (cp.g) {
        if (!remap[cp.g]) remap[cp.g] = "g" + Math.random().toString(36).slice(2, 8);
        if (!__mk.groups) __mk.groups = {};
        __mk.groups[remap[cp.g]] = { n: mkGroupName(cp.g), col: 0 };
        cp.g = remap[cp.g];
      }
      for (let k = 0; k + 1 < cp.p.length; k += 2) { cp.p[k] += 0.035; cp.p[k + 1] += 0.035; }
      __mk.items.push(cp);
      fresh.push(__mk.items.length - 1);
    });
    mkSelSet(fresh);
    mkTouch();
    mkRenderLayers();
    toast(__mkClip.length === 1 ? "Pasted" : `Pasted ${__mkClip.length} layers`, "gold");
    return;
  }
  if (what === "group")   { mkGroupSelection(); return; }
  if (what === "ungroup") { mkUngroupSelection(); return; }

  const idxs = mkSelIdx();
  if (!idxs.length) return;
  const objs = idxs.map((i) => __mk.items[i]);

  if (what === "copy") {
    __mkClip = objs.map(mkPack);
    mkLayerChrome();
    toast(objs.length === 1 ? "Copied — paste it here or in another design"
                            : `Copied ${objs.length} layers`, "gold");
    return;
  }
  mkPushUndo();
  if (what === "up" || what === "down") {
    /* moving by BLOCK: a group travels whole, and a lone layer never lands
       in the middle of a group it was only passing */
    const blocks = mkBlocks();
    const mine = [];
    blocks.forEach((b, n) => { if (b.idx.some((i) => idxs.indexOf(i) >= 0)) mine.push(n); });
    const dir = what === "up" ? 1 : -1;
    const order = dir > 0 ? mine.slice().reverse() : mine.slice();
    let moved = false;
    order.forEach((n) => {
      const t = n + dir;
      if (t < 0 || t >= blocks.length || mine.indexOf(t) >= 0) return;
      const tmp = blocks[n]; blocks[n] = blocks[t]; blocks[t] = tmp;
      const a = mine.indexOf(n); if (a >= 0) mine[a] = t;
      moved = true;
    });
    if (moved) { const ids = objs.map((o) => o.id); mkApplyBlocks(blocks); __mk.selIds = ids; }
  } else if (what === "dup") {
    const remap = {};
    const fresh = [];
    objs.forEach((o) => {
      const cp = mkPack(o); cp.id = mkId();
      if (cp.g) {
        if (!remap[cp.g]) remap[cp.g] = "g" + Math.random().toString(36).slice(2, 8);
        if (!__mk.groups) __mk.groups = {};
        __mk.groups[remap[cp.g]] = { n: mkGroupName(cp.g), col: 0 };
        cp.g = remap[cp.g];
      }
      for (let k = 0; k + 1 < cp.p.length; k += 2) { cp.p[k] += 0.03; cp.p[k + 1] += 0.03; }
      __mk.items.push(cp);
      fresh.push(__mk.items.length - 1);
    });
    mkSelSet(fresh);
  } else if (what === "del") {
    for (let k = idxs.length - 1; k >= 0; k--) __mk.items.splice(idxs[k], 1);
    mkSelSet([]);
  }
  mkTouch();
  mkRenderLayers();
}

/* ── reordering by hand ──────────────────────────────────────────────────
   Pointer events, not HTML5 drag-and-drop: the HTML5 API does not fire on
   touch at all, and this has to work the same on a phone as on a desktop.
   The row lifts, the rest of the list opens a gap, and dropping writes the
   new order straight into the item array. */
(function wireLayerDrag() {
  const host = document.getElementById("mkLayers");
  if (!host) return;
  let drag = null;

  /* the rows a dragged row is reordered AMONG: its siblings. A top-level row
     moves among top-level rows; a row inside a group moves among that
     group's children. Nothing else on screen is a valid landing place. */
  const siblings = (row) => {
    const inGroup = row.classList.contains("mk-layer--child");
    const all = Array.from(host.querySelectorAll(".mk-layer"));
    if (!inGroup) return all.filter((r) => !r.classList.contains("mk-layer--child"));
    const g = mkGroupOf(__mk.items[+row.dataset.i]);
    return all.filter((r) => r.classList.contains("mk-layer--child") &&
                             mkGroupOf(__mk.items[+r.dataset.i]) === g);
  };

  host.addEventListener("pointerdown", (e) => {
    const row = e.target.closest(".mk-layer");
    if (!row || !__mk) return;
    if (e.target.closest(".mk-layer__b")) return;      // eye and lock are buttons
    if (e.target.closest(".mk-layer__tw")) return;     // so is the twisty
    const g = row.dataset.g || "";
    const i = +row.dataset.i;
    const add = e.metaKey || e.ctrlKey || __mkMulti;
    const how = e.shiftKey ? "range" : (add ? "toggle" : (e.altKey ? "alone" : ""));
    if (g) mkSelectGroup(g, (e.shiftKey || add) ? "toggle" : "");
    else mkSelect(i, how);
    /* a shift- or ⌘-click is a selection, never the start of a reorder */
    if (e.shiftKey || add) return;
    /* a press anywhere on the row can start a drag, but only after it has
       actually moved — otherwise selecting a layer on a phone would jitter */
    drag = { row, g, i, y0: e.clientY, moved: false, sibs: siblings(row) };
    try { host.setPointerCapture(e.pointerId); } catch (err) {}
  });

  host.addEventListener("pointermove", (e) => {
    if (!drag || !__mk) return;
    const dy = e.clientY - drag.y0;
    if (!drag.moved && Math.abs(dy) < 6) return;
    if (!drag.moved) {
      drag.moved = true;
      drag.row.classList.add("is-dragging");
      host.classList.add("is-reordering");
    }
    e.preventDefault();
    drag.row.style.transform = `translateY(${dy}px)`;
    /* Where would it land? Purely visual: how many siblings has it passed?
       The list paints TOPMOST FIRST, so the visual order is the reverse of
       the stack, and the conversion happens once, at the drop. */
    const sibs = drag.sibs;
    const from = sibs.indexOf(drag.row);
    let to = from;
    sibs.forEach((r, n) => {
      if (r === drag.row) return;
      const b = r.getBoundingClientRect();
      const mid = b.top + b.height / 2;
      if (e.clientY < mid && n < from) to = Math.min(to, n);
      if (e.clientY > mid && n > from) to = Math.max(to, n);
    });
    sibs.forEach((r, n) => {
      const between = (from < to && n > from && n <= to) || (from > to && n < from && n >= to);
      r.classList.toggle("is-shifted", between);
    });
    drag.from = from; drag.to = to;
  });

  const endDrag = () => {
    if (!drag) return;
    const d = drag; drag = null;
    host.classList.remove("is-reordering");
    if (d.row) { d.row.classList.remove("is-dragging"); d.row.style.transform = ""; }
    host.querySelectorAll(".is-shifted").forEach((r) => r.classList.remove("is-shifted"));
    if (!d.moved || d.to == null || d.to === d.from || !__mk) return;
    mkPushUndo();
    const ids = mkSelIdx().map((i) => __mk.items[i].id);
    if (d.g || !d.row.classList.contains("mk-layer--child")) {
      /* a TOP-LEVEL move: reorder blocks. The visual list runs top-first, so
         visual position n is block (count-1-n) — reverse, splice, reverse. */
      const blocks = mkBlocks().reverse();
      const b = blocks.splice(d.from, 1)[0];
      blocks.splice(d.to, 0, b);
      mkApplyBlocks(blocks.reverse());
    } else {
      /* INSIDE a group: reorder that group's members among themselves */
      const g = mkGroupOf(__mk.items[+d.row.dataset.i]);
      const blocks = mkBlocks();
      const bl = blocks.filter((z) => z.g === g)[0];
      if (bl) {
        const objs = bl.idx.map((i) => __mk.items[i]).reverse();   // top-first
        const o = objs.splice(d.from, 1)[0];
        objs.splice(d.to, 0, o);
        const back = objs.reverse();
        bl.idx.forEach((i, n) => { __mk.items[i] = back[n]; });
      }
    }
    __mk.selIds = ids;
    mkTouch();
    mkRenderLayers();
  };
  host.addEventListener("pointerup", endDrag);
  host.addEventListener("pointercancel", endDrag);

  host.addEventListener("click", (e) => {
    const tw = e.target.closest(".mk-layer__tw");
    if (tw && __mk) {
      const g = tw.closest(".mk-layer").dataset.g;
      if (!__mk.groups) __mk.groups = {};
      const m = __mk.groups[g] || (__mk.groups[g] = { n: "Group", col: 0 });
      m.col = m.col ? 0 : 1;
      mkPersist();
      mkRenderLayers();
      return;
    }
    const b = e.target.closest(".mk-layer__b");
    if (!b || !__mk) return;
    const row = b.closest(".mk-layer");
    /* on a group row the eye and the lock act on every member — one switch
       for the whole thing is the reason to have grouped it */
    const idxs = row.dataset.g ? mkGroupIdx(row.dataset.g) : [+row.dataset.i];
    if (!idxs.length) return;
    mkPushUndo();
    if (b.dataset.act === "eye") {
      const to = idxs.every((i) => __mk.items[i].hid) ? 0 : 1;
      idxs.forEach((i) => { __mk.items[i].hid = to; });
    }
    if (b.dataset.act === "lock") {
      const to = idxs.every((i) => __mk.items[i].lok) ? 0 : 1;
      idxs.forEach((i) => { __mk.items[i].lok = to; });
      if (to) mkSelDrop(idxs);
    }
    mkTouch();
    mkRenderLayers();
  });

  /* double-click a group's name to rename it in place */
  host.addEventListener("dblclick", (e) => {
    const row = e.target.closest(".mk-layer");
    if (!row || !__mk) return;
    if (row.dataset.g) {
      const nameEl = $("#mkLayerName");
      mkSelectGroup(row.dataset.g, "");
      if (nameEl) { nameEl.focus(); nameEl.select(); }
      return;
    }
    const i = +row.dataset.i;
    if (mkIsText(__mk.items[i])) mkEditItem(i);
  });
})();

/* ═══════════ the library ═════════════════════════════════════════════════
   Derived, never stored. Everything below is already in state.sessions,
   which arrives through the one Firestore listener the studio has always
   had — so this repository is real-time by construction and adds nothing to
   the read budget however many people are designing at once.                */
/* ═══════════ A SKETCH HAS A FACE ════════════════════════════════════════
   A drawing saved with Save & Close — never submitted as a reference — has
   no uploaded picture anywhere, because nothing was ever uploaded. It is a
   list of vector objects on a session document and nothing else. So the
   picture is DRAWN, here, from those objects: enough for a thumbnail in My
   designs and a tile in the library, costing no storage, no upload and no
   request. Cached on the content, so a list of forty designs paints once. */
const MK_SKETCH_CACHE = new Map();
function mkSketchThumb(sess, key, size) {
  if (!sess) return "";
  const k = key || "draw";
  const raw = mkItemsOf((sess.markups || {})[k]);
  if (!raw.length) return "";
  const S = size || 260;
  const ck = (sess.id || "?") + ":" + k + ":" + S + ":" + raw.length + ":" + (sess.updatedAt || 0);
  if (MK_SKETCH_CACHE.has(ck)) return MK_SKETCH_CACHE.get(ck);
  let url = "";
  try {
    const cv = document.createElement("canvas");
    cv.width = S; cv.height = S;
    const ctx = cv.getContext("2d");
    ctx.fillStyle = "#ffffff";
    ctx.fillRect(0, 0, S, S);
    /* pictures inside the sketch are skipped rather than risked: one
       cross-origin bitmap taints the canvas and toDataURL throws for ever */
    mkPaint(ctx, S, S, raw.map(mkPack).filter((it) => it.t !== "img"), { export: true });
    url = cv.toDataURL("image/png");
  } catch (e) { url = ""; }
  if (MK_SKETCH_CACHE.size > 80) MK_SKETCH_CACHE.clear();
  MK_SKETCH_CACHE.set(ck, url);
  return url;
}

function mkLibraryItems() {
  const out = [];
  const seen = new Set();
  const add = (o) => {
    /* an imported document is identified by WHICH import it is, not by the
       picture of it: two imports of the same artwork are two entries */
    const key = o.key || o.path || o.url;
    if (!key || seen.has(key)) return;
    seen.add(key);
    out.push(o);
  };
  /* how many live, editable objects sit behind a library entry — a sketch
     has its whole drawing, a generated version has whatever was marked on
     it, an uploaded photograph has none */
  const layersOf = (sess, key) => mkItemsOf((sess.markups || {})[key]).length;
  (state.sessions || []).forEach((s) => {
    const sname = s.name || "Design";
    (s.versions || []).forEach((v) => {
      if (!v || !v.url) return;
      add({
        kind: v.renderUrl ? "charm" : "drawing",
        url: v.url, path: v.path || "",
        pairUrl: v.renderUrl || "", pairPath: v.renderPath || "",
        at: Number(v.at) || Number(s.updatedAt) || 0,
        title: sname, sub: "v" + v.n, sid: s.id,
        srcSid: s.id, srcKey: String(v.n), layers: layersOf(s, String(v.n)),
      });
    });
    const r = s.reference;
    if (r && r.type === "upload" && r.url) {
      add({
        kind: r.drawn ? "sketch" : "upload",
        url: r.url, path: r.path || "", pairUrl: "", pairPath: "",
        at: Number(s.createdAt) || Number(s.updatedAt) || 0,
        title: r.name || (r.drawn ? "Your sketch" : "Uploaded image"),
        sub: r.drawn ? "drawn by hand" : "reference", sid: s.id,
        srcSid: s.id, srcKey: "draw", layers: r.drawn ? layersOf(s, "draw") : 0,
      });
    }
    (s.assets || []).forEach((a) => {
      if (!a || !a.url) return;
      add({
        kind: "upload", url: a.url, path: a.path || "", pairUrl: "", pairPath: "",
        at: Number(a.at) || Number(s.updatedAt) || 0,
        title: a.name || "Picture", sub: "added to a drawing", sid: s.id,
        srcSid: "", srcKey: "", layers: 0,
      });
    });
    /* THE SKETCH THAT WAS NEVER SUBMITTED. Save & Close keeps the drawing on
       the session, but nothing is uploaded until "use this as my reference",
       so there was no picture for the repository to show and the work looked
       lost. It was never lost — it just had no face. Now it has one, drawn
       from its own objects, and every layer is still live behind it. */
    /* AN IMPORTED DOCUMENT. A .psd or .ai is not a picture that happens to
       have layers — it IS its layers, and that is how it is kept: as a
       markup, exactly like a sketch drawn here. So the repository tile is
       drawn from those objects for nothing (vector art), or shows the one
       small composite that was uploaded for it (Photoshop), and it is badged
       with the format it came from so the file type is obvious at a glance. */
    Object.keys(s.markups || {}).forEach((k) => {
      if (k.indexOf("imp:") !== 0) return;
      const m = s.markups[k] || {};
      const n = mkItemsOf(m).length;
      if (!n) return;
      const u = m.impUrl || mkSketchThumb(s, k, 320);
      if (!u) return;
      add({
        kind: "design", url: u, path: m.impPath || "", pairUrl: "", pairPath: "",
        key: s.id + "|" + k,
        at: Number(m.impAt) || Number(m.updatedAt) || Number(s.updatedAt) || 0,
        title: m.impName || "Imported artwork",
        /* the badge already says PSD; the caption says something it doesn't */
        sub: n + (n === 1 ? " layer" : " layers"),
        src: String(m.impSrc || "").toLowerCase(),
        sid: s.id, srcSid: s.id, srcKey: k, layers: n,
        impKey: k, files: (m.impFiles || []).length,
        local: !m.impUrl,       /* drawn here and now — there is no stored file */
      });
    });

    const drawn = !!(s.reference && s.reference.type === "upload" && s.reference.drawn);
    const sketchLayers = layersOf(s, "draw");
    if (sketchLayers && !drawn) {
      const u = mkSketchThumb(s, "draw", 320);
      if (u) {
        add({
          kind: "sketch", url: u, path: "", pairUrl: "", pairPath: "",
          at: Number(s.updatedAt) || Number(s.createdAt) || 0,
          title: s.name || "My sketch", sub: "drawn by hand · not submitted",
          sid: s.id, srcSid: s.id, srcKey: "draw", layers: sketchLayers,
          local: true,      /* drawn here and now — there is no stored file */
        });
      }
    }
  });
  return out;
}
function mkLibFiltered() {
  const q = String(__mkDock.q || "").trim().toLowerCase();
  let list = mkLibraryItems();
  if (__mkDock.filter !== "all") list = list.filter((a) => a.kind === __mkDock.filter);
  if (q) list = list.filter((a) => (a.title + " " + a.sub).toLowerCase().indexOf(q) >= 0);
  list.sort((a, b) => __mkDock.sort === "old" ? a.at - b.at : b.at - a.at);
  return list;
}
function mkLibDate(ms) {
  if (!ms) return "";
  const d = new Date(ms);
  const now = new Date();
  const sameYear = d.getFullYear() === now.getFullYear();
  try {
    return d.toLocaleDateString(undefined,
      sameYear ? { day: "numeric", month: "short" } : { day: "numeric", month: "short", year: "numeric" });
  } catch (e) { return ""; }
}
let __mkLib = [];
function mkRenderLibrary() {
  const host = $("#mkLibGrid"); if (!host) return;
  __mkLib = mkLibFiltered();
  const total = mkLibraryItems().length;
  $("#mkLibCount").textContent = __mkLib.length === total
    ? `${total} image${total === 1 ? "" : "s"}`
    : `${__mkLib.length} of ${total}`;
  if (!__mkLib.length) {
    host.innerHTML = total
      ? '<p class="mk-lib__empty">Nothing matches that.</p>'
      : '<p class="mk-lib__empty">Your library fills itself: every image you upload and every charm we draw for you lands here, ready to drop into any design.</p>';
    return;
  }
  host.innerHTML = __mkLib.map((a, i) => {
    const shown = a.pairUrl || a.url;
    return `<figure class="mk-asset" data-a="${i}" tabindex="0" role="button"
                    aria-label="${attrText(a.title + " — tap to place, drag onto the sheet to position it")}">
      <span class="mk-asset__img"><img src="${attrText(shown)}" alt="" loading="lazy" draggable="false"></span>
      ${a.src ? `<span class="mk-asset__fmt" title="${attrText(a.src.toUpperCase() + " — every layer came across, still editable")}">${escapeHtml(a.src.toUpperCase())}</span>` : ""}
      ${a.pairUrl ? '<span class="mk-asset__pair" title="Charm and drawing, kept together">PAIR</span>' : ""}
      ${a.layers ? `<span class="mk-asset__layers" title="${a.layers} editable object${a.layers === 1 ? "" : "s"} — placing this brings them all in, still editable">✦&nbsp;${a.layers}</span>` : ""}
      <button class="mk-asset__zoom" data-zoom="${i}" type="button" aria-label="Preview larger">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="10.5" cy="10.5" r="6.5"/><path d="m15.5 15.5 5 5M10.5 8v5M8 10.5h5"/></svg>
      </button>
      <button class="mk-asset__kill" data-kill="${i}" type="button" title="Delete this image for good" aria-label="${attrText("Delete " + a.title + " permanently")}">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M4 6.5h16"/><path d="M9 6.5V4h6v2.5"/><path d="M6.5 6.5 7.5 20h9l1-13.5"/></svg>
      </button>
      <figcaption><b>${escapeHtml(a.title)}</b><span>${escapeHtml(a.sub)}${a.at ? " · " + mkLibDate(a.at) : ""}</span></figcaption>
    </figure>`;
  }).join("");
}
/* Deleting an image says what it will cost first, because where the picture
   lives decides what goes with it. */
async function mkConfirmKill(a) {
  if (!a) return;
  const design = (state.sessions || []).filter((s) => s.id === a.sid)[0];
  const dn = (design && design.name) || "a design";
  const body = a.kind === "design"
    ? `This ${String(a.src || "design").toUpperCase()} document and all ${a.layers} object${a.layers === 1 ? "" : "s"} that came in with it will be deleted from your library. Anything you have already placed on a sheet stays where it is.`
    : a.kind === "sketch" && a.local
    ? `This drawing and every object in it will be deleted from “${dn}”. It cannot be brought back.`
    : a.sub === "added to a drawing"
      ? "This picture will be removed from your library and from any drawing it was placed in."
      : a.kind === "charm" || a.kind === "drawing"
        ? `This version will be deleted from “${dn}” — the charm and the drawing it was cut from, together.`
        : `This image will be deleted from your library and from “${dn}”.`;
  const yes = await askConfirm({ title: `Delete “${a.title}”?`, body, yes: "Delete for good" });
  if (!yes) return;
  const gone = await deleteLibraryImage(a);
  if (gone) toast("Deleted", "gold");
}

/* the sessions listener already calls this whenever anything changes */
function mkLibRefresh() {
  if (__mkDock.open && __mkDock.tab === "library") mkRenderLibrary();
}

/* ── the preview ─────────────────────────────────────────────────────────
   A thumbnail is not enough to choose by, so this shows the whole image with
   the studio's own zoom/pan behind it — and, for a generated pair, a switch
   between the metal charm and the drawing it was cut from. */
let __mkZoomAsset = null, __mkZoomHalf = "charm";
function mkOpenLibZoom(a) {
  __mkZoomAsset = a;
  __mkZoomHalf = a.pairUrl ? "charm" : "bw";
  $("#mkZoomTitle").textContent = a.title + (a.sub ? " · " + a.sub : "");
  $("#mkZoomPair").hidden = !a.pairUrl;
  /* say what placing will actually do */
  const place = $("#mkZoomPlace"), flat = $("#mkZoomFlat");
  if (a.layers) {
    place.innerHTML = `Place ${a.layers} editable object${a.layers === 1 ? "" : "s"}&nbsp;→`;
    flat.hidden = false;
  } else {
    place.textContent = "Place on the sheet";
    flat.hidden = true;
  }
  /* a sketch drawn on the fly has no stored file, so "place it flat" would
     make a picture no document could ever hold. Its layers are the point. */
  if (a.local) flat.hidden = true;
  mkPaintLibZoom();
  $("#mkLibZoom").hidden = false;
}
function mkPaintLibZoom() {
  const a = __mkZoomAsset; if (!a) return;
  const url = (__mkZoomHalf === "charm" && a.pairUrl) ? a.pairUrl : a.url;
  const stage = $("#mkZoomStage");
  stage.innerHTML = `<img src="${attrText(url)}" alt="${attrText(a.title)}" draggable="false">`;
  stage.dataset.zoomAttached = "";
  attachZoomPan(stage);
  $$("#mkZoomPair .mk-chip").forEach((b) =>
    b.classList.toggle("is-on", b.dataset.half === __mkZoomHalf));
}
function mkCloseLibZoom() { $("#mkLibZoom").hidden = true; __mkZoomAsset = null; }

/* ── placing a picture ───────────────────────────────────────────────────
   Dropped, tapped or pasted, it all ends here: a new image layer, sized to a
   comfortable fraction of the sheet and then re-shaped to the picture's own
   proportions the moment its pixels arrive. */
/* ═══════════ BRINGING A LIBRARY ENTRY IN WITH ITS LAYERS ═════════════════
   A picture is a picture, but a drawing made in this studio is a stack of
   objects, and flattening it on the way in throws that away: the customer
   gets a bitmap they can only move and scale. When the entry still has its
   objects, they ALL come across — every stroke, shape, fill and note, with
   its own name, colour, opacity, lock and hidden state intact — scaled into
   the drop point as a group and immediately manipulable, exactly as they
   were in the drawing they came from. One undo step puts them all back.  */
function mkAssetLayers(a) {
  if (!a || !a.srcSid || !a.srcKey) return [];
  const src = (state.sessions || []).find((s) => s.id === a.srcSid);
  if (!src) return [];
  return mkItemsOf((src.markups || {})[a.srcKey]).map(mkPack);
}
function mkInsertLayers(a, cx, cy) {
  if (!__mk) return 0;
  const items = mkAssetLayers(a);
  if (!items.length) return 0;
  /* the group's own bounds, so it lands where it was dropped at a sane size */
  let x0 = 1, y0 = 1, x1 = 0, y1 = 0;
  items.forEach((it) => {
    const b = mkBBox(it);
    x0 = Math.min(x0, b.x); y0 = Math.min(y0, b.y);
    x1 = Math.max(x1, b.x + b.w); y1 = Math.max(y1, b.y + b.h);
  });
  const w = Math.max(1e-3, x1 - x0), h = Math.max(1e-3, y1 - y0);
  const SIZE = 0.62;
  const sc = Math.min(SIZE / w, SIZE / h, 1.6);
  const ox = (cx == null ? 0.5 : cx) - (x0 + w / 2) * sc;
  const oy = (cy == null ? 0.5 : cy) - (y0 + h / 2) * sc;
  mkPushUndo();
  const group = "g" + Math.random().toString(36).slice(2, 6);
  /* everything brought over from one library entry arrives as a real,
     named group — so it can be moved as the one picture it looks like */
  if (!__mk.groups) __mk.groups = {};
  __mk.groups[group] = { n: String(a.title || "Library").slice(0, 40), col: 0 };
  items.forEach((it) => {
    const cp = mkPack(it);
    cp.id = mkId();
    for (let k = 0; k + 1 < cp.p.length; k += 2) {
      cp.p[k] = cp.p[k] * sc + ox;
      cp.p[k + 1] = cp.p[k + 1] * sc + oy;
    }
    if (cp.fs) cp.fs = Math.max(0.012, cp.fs * sc);
    if (cp.w) cp.w = Math.max(0.002, cp.w * sc);
    /* the name says where it came from, so a merged sheet stays legible */
    cp.n = (cp.n || MK_TYPE_NAME[cp.t] || "Layer") + " · " + String(a.title || "library").slice(0, 18);
    cp.g = group;
    __mk.items.push(cp);
  });
  __mk.sel = __mk.items.length - 1;
  mkSetTool("select");
  mkTouch();
  mkRenderLayers();
  logEvent("upload", `Brought in ${items.length} object${items.length === 1 ? "" : "s"} from “${a.title}”`);
  return items.length;
}

function mkLoadImageUrl(src) {
  return new Promise((resolve, reject) => {
    const im = new Image();
    im.onload = () => resolve(im);
    im.onerror = () => reject(new Error("img_load_failed"));
    im.src = src;
  });
}
function mkTraceImportedRaster(img) {
  const nativeMax = Math.max(img.naturalWidth || img.width || 0, img.naturalHeight || img.height || 0);
  return mkTraceImage(img, MK_ENGRAVE, {
    /* Trace at the source's own useful resolution whenever possible. This
       avoids both destructive downsampling and pointless enlargement. */
    size: Math.max(1100, Math.min(2600, nativeMax || 2200)),
    eps: 0.00018,
    budget: 42000,
    partMax: 30000,
    maxParts: 220,
    minAreaFraction: 0.00001,
    maxInkFraction: 0.985,
    maxMidFraction: 0.42,
    highFidelity: true,
    forceEngrave: true,
  });
}
function mkPlaceItemsGroup(items, title, cx, cy, sourceFrame) {
  if (!__mk || !items || !items.length) return 0;
  let x0 = 1, y0 = 1, x1 = 0, y1 = 0;
  if (sourceFrame && Number(sourceFrame.w) > 0 && Number(sourceFrame.h) > 0) {
    x0 = Number(sourceFrame.x) || 0; y0 = Number(sourceFrame.y) || 0;
    x1 = x0 + Number(sourceFrame.w); y1 = y0 + Number(sourceFrame.h);
  } else {
    items.forEach((it) => {
      const b = mkBBox(it);
      x0 = Math.min(x0, b.x); y0 = Math.min(y0, b.y);
      x1 = Math.max(x1, b.x + b.w); y1 = Math.max(y1, b.y + b.h);
    });
  }
  const w = Math.max(1e-3, x1 - x0), h = Math.max(1e-3, y1 - y0);
  const SIZE = 0.62;
  const sc = Math.min(SIZE / w, SIZE / h, 1.6);
  const ox = (cx == null ? 0.5 : cx) - (x0 + w / 2) * sc;
  const oy = (cy == null ? 0.5 : cy) - (y0 + h / 2) * sc;
  mkPushUndo();
  const group = "g" + Math.random().toString(36).slice(2, 6);
  if (!__mk.groups) __mk.groups = {};
  __mk.groups[group] = { n: String(title || "Imported artwork").slice(0, 40), col: 0 };
  items.forEach((it) => {
    const cp = mkPack(it);
    cp.id = mkId();
    for (let k = 0; k + 1 < cp.p.length; k += 2) {
      cp.p[k] = cp.p[k] * sc + ox;
      cp.p[k + 1] = cp.p[k + 1] * sc + oy;
    }
    if (cp.fs) cp.fs = Math.max(0.012, cp.fs * sc);
    if (cp.w) cp.w = Math.max(0.0012, cp.w * sc);
    cp.n = (cp.n || MK_TYPE_NAME[cp.t] || "Layer") + " · " + String(title || "import").slice(0, 18);
    cp.g = group;
    __mk.items.push(cp);
  });
  __mk.sel = __mk.items.length - 1;
  mkSetTool("select");
  mkTouch();
  mkRenderLayers();
  return items.length;
}

/* the one door every "put this on my sheet" goes through */
function mkPlaceAsset(a, cx, cy, forcePicture) {
  if (!__mk || !a) return;
  if (!forcePicture && a.layers) {
    const n = mkInsertLayers(a, cx, cy);
    if (n) {
      toast(`${n} object${n === 1 ? "" : "s"} brought in — every one still editable ✦`, "gold");
      return;
    }
  }
  mkInsertImage(a, cx, cy);
  toast(cx == null ? "Placed — drag it where you want it" : "Placed", "gold");
}

function mkInsertImage(a, cx, cy, half) {
  if (!__mk) return null;
  const url = (half === "bw" || !a.pairUrl) ? a.url : a.pairUrl;
  const path = (half === "bw" || !a.pairUrl) ? a.path : a.pairPath;
  const w = 0.46, h = 0.46;
  const x = Math.min(0.98 - w / 2, Math.max(w / 2, cx == null ? 0.5 : cx));
  const y = Math.min(0.98 - h / 2, Math.max(h / 2, cy == null ? 0.5 : cy));
  mkPushUndo();
  const it = mkPack({
    id: mkId(), t: "img", u: url, sp: path,
    n: a.title ? String(a.title).slice(0, 48) : "",
    p: [x - w / 2, y - h / 2, x + w / 2, y + h / 2],
  });
  __mk.items.push(it);
  __mk.sel = __mk.items.length - 1;
  mkSetTool("select");
  mkTouch();
  mkRenderLayers();
  /* once the pixels land, make the box match the picture so the selection
     hugs it and a corner drag scales rather than crops */
  mkImageFor(it, () => {
    const img = mkImageFor(it);
    if (!img || !img.width || !__mk) return;
    const cur = mkBBox(it);
    const ar = img.width / img.height;
    let bw = cur.w, bh = cur.h;
    if (ar >= 1) bh = bw / ar; else bw = bh * ar;
    const ccx = cur.x + cur.w / 2, ccy = cur.y + cur.h / 2;
    it.p = [ccx - bw / 2, ccy - bh / 2, ccx + bw / 2, ccy + bh / 2];
    mkTouch();
    mkRenderLayers();
  });
  return it;
}

/* ── importing from the device ───────────────────────────────────────────
   Downscaled in the browser first (a 12-megapixel phone photo is nobody's
   idea of a charm sketch), then written to Storage through britesAuth —
   which is what makes it survive a reload and show up in the library. It
   does NOT spend a reference upload: see uploadAsset in britesAuth.js. */
/* ── multi-select without a keyboard ─────────────────────────────────────
   Most of our customers are holding a phone, and a phone has no shift key.
   With this on, every tap ADDS to the selection instead of replacing it —
   the same idea shift expresses on a desktop, said in a way a thumb can
   reach. The rubber band works on touch either way. */
let __mkMulti = false;
let __mkImporting = 0;
let __mkAssetKindDead = false;      /* britesAuth on this deploy has no upload_asset */

/* Upload one picture and hand back {path, url}. Two doors, tried in order:
   upload_asset — the right one, which costs no reference allowance;
   upload_ref   — the one that has been deployed since the beginning.

   The fallback exists because the studio's front end ships as theme files and
   its back end as a Netlify function, and the two do not have to land at the
   same moment. A customer whose store has yesterday's britesAuth should still
   be able to put a picture on their sketch — it just quietly spends an image
   slot instead, and the console says so once. */
async function mkUploadPicture(sessionId, filename, dataUrl) {
  if (!__mkAssetKindDead) {
    try {
      const r = await postAuthed("britesAuth", {
        kind: "upload_asset", sessionId, filename, dataUrl,
      });
      if (r && r.ok && r.path) return { up: r, via: "upload_asset" };
      throw Object.assign(new Error((r && r.error) || "empty_response"), { payload: r });
    } catch (err) {
      const why = String((err && err.message) || "");
      /* "this door does not exist here" arrives in more than one shape: a 400
         or 404 from the newer function, a 200 carrying {ok:false,
         error:"unknown_kind"} from an older one, or a bare 404 from a build
         that never had the kind at all. All three mean the same thing. */
      const notThere = /unknown_kind|not_found|unsupported_kind/i.test(why) ||
                       err.status === 404 ||
                       (err.status === 400 && /kind/i.test(why));
      if (!notThere) throw err;
      __mkAssetKindDead = true;
      console.warn("[studio] this deployment's britesAuth has no upload_asset " +
                   "(deploy britesAuth 1.8.0 to stop pictures costing an image slot). " +
                   "Falling back to upload_ref.");
    }
  }
  const r2 = await postAuthed("britesAuth", { kind: "upload_ref", sessionId, filename, dataUrl });
  if (!(r2 && r2.ok && r2.path)) {
    throw Object.assign(new Error((r2 && r2.error) || "empty_response"), { payload: r2 });
  }
  return { up: r2, via: "upload_ref" };
}

/* Every way this can fail, said in words a customer can act on. Anything
   unrecognised keeps its own code so the console still names it. */
function mkImportError(err) {
  const why = String((err && err.message) || "");
  const st = err && err.status;
  if (/asset_library_full/.test(why))  return "Your picture library is full — delete a few designs to make room.";
  if (/no_uploads_left/.test(why) || st === 402) return "You've used every image slot on this account — add credits to keep going.";
  if (/rate_limited/.test(why) || st === 429)    return "That's a lot of pictures at once — give it a minute.";
  if (/unsupported_type/.test(why) || st === 415) return "JPG and PNG only, I'm afraid.";
  if (/too_large/.test(why) || st === 413)        return "That picture is over the size limit — try a smaller one.";
  if (st === 401 || /sign_in|unauthor/i.test(why)) return "Your session timed out — sign in again and try once more.";
  if (/unknown_kind/.test(why))  return "The image service on this store needs updating — tell Brites the picture upload is out of date.";
  if (/network|Failed to fetch/i.test(why)) return "No connection to the image service — check your network and try again.";
  return "That picture didn't upload (" + (why || "unknown") + ") — try again.";
}

async function mkImportFiles(files, cx, cy) {
  if (!__mk) { toast("Open a drawing first", "err"); return; }
  const all = Array.from(files || []);
  if (!all.length) { toast("No file came through — try choosing it again", "err"); return; }
  const rest = [];
  for (const f of all) {
    let handled = false;
    try { handled = await designFileImport(f, cx, cy); } catch (e) { handled = false; }
    if (!handled) rest.push(f);
  }
  if (!rest.length) return;
  const isPic = (f) => /^image\/(jpeg|png)$/.test(f.type || "") ||
                       (!f.type && /\.(jpe?g|png)$/i.test(f.name || ""));
  const list = rest.filter(isPic).slice(0, 6);
  if (!list.length) {
    toast(rest.length ? "JPG, PNG, PSD, AI, PDF, SVG or EPS — that one is a " +
          ((rest[0].type || "unknown file").split("/").pop()) : "No file chosen", "err");
    return;
  }
  ensureSession();
  let placed = 0;
  for (let i = 0; i < list.length; i++) {
    const f = list[i];
    const label = list.length > 1 ? ` (${i + 1} of ${list.length})` : "";
    if (f.size > CONFIG.maxUploadBytes) {
      toast(`“${f.name}” is over the ${Math.round(CONFIG.maxUploadBytes / 1048576)} MB limit`, "err");
      continue;
    }
    __mkImporting++;
    mkImportChrome();
    toast(`Adding “${f.name.slice(0, 28)}”${label}…`, "gold");
    try {
      const dataUrl = await new Promise((res, rej) => {
        const rd = new FileReader();
        rd.onload = () => res(rd.result);
        rd.onerror = () => rej(new Error("unreadable_file"));
        rd.readAsDataURL(f);
      });
      /* Keep the original bytes whenever they already fit. A 2048px source
         is not re-encoded merely because it is being imported. */
      const meta = await resizeToMax(dataUrl, 2600, { maxBytes: Math.round(CONFIG.maxUploadBytes * 0.90) });
      toast(`Converting “${f.name.slice(0, 28)}” into black engraving shapes…`, "gold");
      const shaped = await rasterShapesBlack(meta.dataUrl);
      if (!shaped.kept) throw new Error("shape_empty");
      const shapedImg = await mkLoadImageUrl(shaped.dataUrl);
      toast(`Separating “${f.name.slice(0, 28)}” into individual layers…`, "gold");
      const traced = mkTraceImportedRaster(shapedImg);
      if (!(traced && traced.items && traced.items.length)) throw new Error("shape_trace_failed");

      const stem = f.name.replace(/\.[a-z]+$/i, "").slice(0, 40) || "Imported artwork";
      mkPlaceItemsGroup(traced.items, stem, cx == null ? 0.5 : cx, cy == null ? 0.5 : cy, traced.frame);
      try {
        const extm = String((f.name.match(/\.([a-z0-9]+)$/i) || ["", "png"])[1] || "png").toLowerCase();
        await impRegister(f, extm, traced.items, null);
      } catch (e) { console.warn("[studio] traced raster repository entry skipped:", (e && e.message) || e); }
      placed++;
    } catch (err) {
      console.warn("[studio] picture import failed:", (err && err.message) || err,
                   err && err.status ? "(HTTP " + err.status + ")" : "");
      const why = String((err && err.message) || "");
      if (/shape_trace_failed/.test(why)) toast("That image couldn't be separated cleanly enough into editable layers.", "err");
      else if (/shape_empty/.test(why)) toast("That image turns into blank white space once converted — use one with visible artwork.", "err");
      else toast(mkImportError(err), "err");
    } finally {
      __mkImporting--;
      mkImportChrome();
    }
  }
  if (placed) {
    toast(placed === 1 ? "Picture converted — every visible component is an editable engraving layer ✦"
                       : `${placed} pictures converted into editable engraving layers ✦`, "gold");
  }
  mkLibRefresh();
}
function mkImportChrome() {
  const b = $("#mkPicture");
  if (b) b.classList.toggle("is-busy", __mkImporting > 0);
}

/* ═══════════ THE OBJECT MENU ═════════════════════════════════════════════
   One menu, raised where the object is: a right-click on the sheet, a
   right-click on a layer row, or a long press on either for hands without a
   mouse button. Per-object commands used to live in a toolbar dropdown,
   which meant selecting a thing in one place and commanding it somewhere
   else — the main bar now carries only what acts on the WHOLE sheet.       */
function mkCtxOpen(clientX, clientY, index) {
  if (!__mk) return;
  const menu = $("#mkCtx");
  if (!menu) return;
  /* right-clicking INSIDE the selection commands the whole selection;
     right-clicking anything else selects that first, the way it does
     everywhere else a context menu exists */
  if (index != null && index >= 0 && !mkIsSelIdx(index)) {
    mkSelSet(mkReachOf(index, false));
    mkChrome();
    mkPaintLayerSel();
  }
  const it = __mk.items[__mk.sel];
  const n = mkSelCount();
  mkCloseAllDrops();
  $("#mkCtxName").textContent = !it ? "Nothing selected"
    : (n > 1 ? (mkCanUngroup() && mkGroupOf(it) && mkGroupIdx(mkGroupOf(it)).length === n
                ? mkGroupName(mkGroupOf(it)) + " · " + n + " layers"
                : n + " layers selected")
             : mkLayerName(it));
  const has = !!it;
  const rt = $("#mkCtxRatio");
  if (rt) rt.classList.toggle("is-on", !!__mk.lockRatio);
  menu.querySelectorAll("[data-ctx]").forEach((b) => {
    const act = b.dataset.ctx;
    b.hidden = (act === "edit") ? !(n === 1 && mkIsText(it)) : false;
    b.disabled = act === "paste" ? !(__mkClip && __mkClip.length)
               : act === "group" ? !mkCanGroup()
               : act === "ungroup" ? !mkCanUngroup()
               : act === "ratio" ? false
               : !has;
    b.style.opacity = b.disabled ? ".38" : "";
  });
  if (has) {
    $("#mkCtxHide").innerHTML = it.hid ? "◉&nbsp;Show" : "◎&nbsp;Hide";
    $("#mkCtxLock").innerHTML = it.lok ? "⚿&nbsp;Unlock" : "⚿&nbsp;Lock";
  }
  menu.hidden = false;
  menu.style.visibility = "hidden";
  menu.style.left = "0px"; menu.style.top = "0px";
  const r = menu.getBoundingClientRect();
  let left = clientX, top = clientY;
  if (left + r.width > window.innerWidth - 8) left = Math.max(8, window.innerWidth - 8 - r.width);
  if (top + r.height > window.innerHeight - 8) top = Math.max(8, window.innerHeight - 8 - r.height);
  menu.style.left = left + "px";
  menu.style.top = top + "px";
  menu.style.visibility = "";
}
function mkCtxClose() {
  const menu = $("#mkCtx");
  if (menu) menu.hidden = true;
}
function mkCtxDo(act) {
  if (!__mk) return;
  const i = __mk.sel, it = __mk.items[i];
  mkCtxClose();
  if (act === "ratio") {
    /* a sheet-wide preference, not an object one — but it is reached from an
       object, because resizing is something you do TO an object */
    __mk.lockRatio = !__mk.lockRatio;
    toast(__mk.lockRatio ? "Proportions locked while you resize"
                         : "Free resize — hold shift to keep proportions", "gold");
    return;
  }
  if (act === "paste") { mkLayerAct("paste"); return; }
  if (act === "group" || act === "ungroup") { mkLayerAct(act); return; }
  if (!it) return;
  if (act === "edit") { mkEditItem(i); return; }
  if (act === "copy" || act === "dup" || act === "up" || act === "down") { mkLayerAct(act); return; }
  if (act === "del") { mkLayerAct("del"); return; }
  if (act === "hide" || act === "lock") {
    const idxs = mkSelIdx();
    mkPushUndo();
    if (act === "hide") {
      const to = it.hid ? 0 : 1;
      idxs.forEach((k) => { __mk.items[k].hid = to; });
    } else {
      const to = it.lok ? 0 : 1;
      idxs.forEach((k) => { __mk.items[k].lok = to; });
      if (to) mkSelSet([]);
    }
    mkTouch();
    mkRenderLayersSoon();
    return;
  }
  mkArrange(act);                     /* rotL rotR flipH flipV front back */
}

(function wireCtx() {
  const stage = document.getElementById("markupStage");
  const menu = document.getElementById("mkCtx");
  if (!stage || !menu) return;

  const norm = (e) => {
    const r = $("#markupView").getBoundingClientRect();
    return [(e.clientX - r.left) / r.width, (e.clientY - r.top) / r.height];
  };

  stage.addEventListener("contextmenu", (e) => {
    if (!__mk) return;
    e.preventDefault();
    const [x, y] = norm(e);
    const hit = mkHitItem(x, y);
    mkCtxOpen(e.clientX, e.clientY, hit);
  });

  /* double-click reopens the words — the fastest way back into text */
  stage.addEventListener("dblclick", (e) => {
    if (!__mk) return;
    const [x, y] = norm(e);
    const i = mkHitText(x, y);
    if (i >= 0) { e.preventDefault(); mkEditItem(i); }
  });

  /* long press, for a hand with no mouse button */
  let press = null;
  const armPress = (e, host) => {
    if (!__mk || e.pointerType === "mouse") return;
    const row = host === "layers" ? e.target.closest(".mk-layer") : null;
    const at = { x: e.clientX, y: e.clientY };
    let index = -1;
    if (host === "layers") { if (!row) return; index = +row.dataset.i; }
    else { const [x, y] = norm(e); index = mkHitItem(x, y); }
    press = setTimeout(() => {
      press = null;
      if (!__mk) return;
      try { navigator.vibrate && navigator.vibrate(12); } catch (err) {}
      mkCtxOpen(at.x, at.y, index);
    }, 520);
  };
  const cancelPress = () => { if (press) { clearTimeout(press); press = null; } };
  stage.addEventListener("pointerdown", (e) => armPress(e, "stage"));
  ["pointermove", "pointerup", "pointercancel"].forEach((t) =>
    stage.addEventListener(t, cancelPress));

  const layers = document.getElementById("mkLayers");
  if (layers) {
    layers.addEventListener("contextmenu", (e) => {
      const row = e.target.closest(".mk-layer");
      if (!row || !__mk) return;
      e.preventDefault();
      mkCtxOpen(e.clientX, e.clientY, +row.dataset.i);
    });
    layers.addEventListener("dblclick", (e) => {
      const row = e.target.closest(".mk-layer");
      if (!row || !__mk) return;
      const i = +row.dataset.i;
      if (mkIsText(__mk.items[i])) { e.preventDefault(); mkEditItem(i); }
    });
    layers.addEventListener("pointerdown", (e) => armPress(e, "layers"));
    ["pointermove", "pointerup", "pointercancel"].forEach((t) =>
      layers.addEventListener(t, cancelPress));
  }

  menu.querySelectorAll("[data-ctx]").forEach((b) =>
    b.addEventListener("click", () => { if (!b.disabled) mkCtxDo(b.dataset.ctx); }));
  document.addEventListener("pointerdown", (e) => {
    if (e.target.closest("#mkCtx")) return;
    mkCtxClose();
  }, true);
  document.addEventListener("keydown", (e) => { if (e.key === "Escape") mkCtxClose(); });
  window.addEventListener("resize", mkCtxClose);
})();

/* ═══════════ wiring ══════════════════════════════════════════════════════ */
(function wireDock() {
  if (!document.getElementById("mkDock")) return;

  $("#mkDockClose").addEventListener("click", () => mkDockMin(!__mkDock.min));
  /* a window that crosses the breakpoint re-decides: folded is a phone
     answer, and leaving it folded on a desktop shows an empty white column
     with its unfold button styled away */
  let _dockRz = null;
  window.addEventListener("resize", () => {
    if (!__mk) return;
    clearTimeout(_dockRz);
    _dockRz = setTimeout(() => { mkDockAutoFold(); mkDockFoot(); }, 140);
  });
  $$("#mkDock .mk-dock__tab").forEach((b) =>
    b.addEventListener("click", () => { mkDockMin(false); mkDockSet(true, b.dataset.dock); }));

  $("#mkLayerUp").addEventListener("click", () => mkLayerAct("up"));
  $("#mkLayerDown").addEventListener("click", () => mkLayerAct("down"));
  $("#mkLayerDup").addEventListener("click", () => mkLayerAct("dup"));
  $("#mkLayerCopy").addEventListener("click", () => mkLayerAct("copy"));
  $("#mkLayerPaste").addEventListener("click", () => mkLayerAct("paste"));
  $("#mkLayerDel").addEventListener("click", () => mkLayerAct("del"));

  const gb = $("#mkLayerGroup"), ub = $("#mkLayerUngroup");
  if (gb) gb.addEventListener("click", () => mkLayerAct("group"));
  if (ub) ub.addEventListener("click", () => mkLayerAct("ungroup"));
  const mb = $("#mkMultiBtn");
  if (mb) mb.addEventListener("click", () => {
    __mkMulti = !__mkMulti;
    mb.setAttribute("aria-pressed", __mkMulti ? "true" : "false");
    mb.classList.toggle("is-on", __mkMulti);
    if (__mkMulti && __mk && __mk.tool !== "select") mkSetTool("select");
    toast(__mkMulti ? "Multi-select on — tap layers to add them, then group"
                    : "Multi-select off", "gold");
  });

  $("#mkLayerName").addEventListener("input", () => {
    const it = __mk && __mk.items[__mk.sel]; if (!it) return;
    const v = $("#mkLayerName").value.slice(0, 48);
    const foot = $("#mkLayerFoot");
    if (foot && foot.dataset.scope === "group") {
      /* with a whole group selected, this names the GROUP */
      const g = mkGroupOf(it);
      if (!__mk.groups) __mk.groups = {};
      __mk.groups[g] = Object.assign(__mk.groups[g] || { col: 0 }, { n: v || "Group" });
      mkPersist();
      const row = document.querySelector('#mkLayers .mk-layer[data-g="' + g + '"] .mk-layer__name');
      if (row) row.textContent = v || "Group";
      return;
    }
    mkSelItems().forEach((o) => { o.n = v; });
    mkPersist();
    mkSelIdx().forEach((i) => {
      const row = document.querySelector('#mkLayers .mk-layer[data-i="' + i + '"] .mk-layer__name');
      if (row) row.textContent = mkLayerName(__mk.items[i]);
    });
  });
  $("#mkLayerOpacity").addEventListener("input", () => {
    const list = __mk ? mkSelItems() : []; if (!list.length) return;
    const o = Math.max(0.05, Math.min(1, $("#mkLayerOpacity").value / 100));
    list.forEach((it) => { it.o = o; });
    $("#mkLayerOpacityVal").textContent = Math.round(o * 100) + "%";
    mkRedraw(); mkPersist();
  });

  /* ── library controls ── */
  $$("#mkLibFilters .mk-chip").forEach((b) => b.addEventListener("click", () => {
    __mkDock.filter = b.dataset.filter;
    $$("#mkLibFilters .mk-chip").forEach((x) => x.classList.toggle("is-on", x === b));
    mkRenderLibrary();
  }));
  $$(".mk-chip--sort").forEach((b) => b.addEventListener("click", () => {
    __mkDock.sort = b.dataset.sort;
    $$(".mk-chip--sort").forEach((x) => x.classList.toggle("is-on", x === b));
    mkRenderLibrary();
  }));
  $("#mkLibSearch").addEventListener("input", () => {
    __mkDock.q = $("#mkLibSearch").value;
    mkRenderLibrary();
  });
  $("#mkLibMore").addEventListener("click", async () => {
    const b = $("#mkLibMore");
    b.disabled = true; b.textContent = "Loading…";
    await widenSessionWindow();
    b.textContent = "Load older designs";
    b.disabled = false;
    mkRenderLibrary();
  });

  /* ── the library grid: tap to place, drag to position ─────────────────
     One gesture handler for mouse and touch alike. A press that never moves
     is a tap and drops the picture in the middle; a press that travels lifts
     a ghost and puts the picture down wherever it is released. */
  const grid = $("#mkLibGrid");
  let gdrag = null;
  grid.addEventListener("pointerdown", (e) => {
    const fig = e.target.closest(".mk-asset");
    if (!fig) return;
    if (e.target.closest(".mk-asset__zoom")) return;
    if (e.target.closest(".mk-asset__kill")) return;
    const a = __mkLib[+fig.dataset.a];
    if (!a) return;
    gdrag = { a, x0: e.clientX, y0: e.clientY, moved: false, ghost: null, id: e.pointerId };
    try { grid.setPointerCapture(e.pointerId); } catch (err) {}
  });
  grid.addEventListener("pointermove", (e) => {
    if (!gdrag) return;
    const dx = e.clientX - gdrag.x0, dy = e.clientY - gdrag.y0;
    if (!gdrag.moved && dx * dx + dy * dy < 64) return;
    if (!gdrag.moved) {
      gdrag.moved = true;
      const g = document.createElement("img");
      g.className = "mk-ghost";
      g.src = gdrag.a.pairUrl || gdrag.a.url;
      document.body.appendChild(g);
      gdrag.ghost = g;
      $("#markupStage").classList.add("is-dropzone");
    }
    e.preventDefault();
    gdrag.ghost.style.left = e.clientX + "px";
    gdrag.ghost.style.top = e.clientY + "px";
    const st = $("#markupStage").getBoundingClientRect();
    const over = e.clientX >= st.left && e.clientX <= st.right && e.clientY >= st.top && e.clientY <= st.bottom;
    gdrag.ghost.classList.toggle("is-over", over);
    $("#markupDropHint").hidden = !over;
  });
  const endGrid = (e) => {
    if (!gdrag) return;
    const g = gdrag; gdrag = null;
    $("#markupStage").classList.remove("is-dropzone");
    $("#markupDropHint").hidden = true;
    if (g.ghost) g.ghost.remove();
    if (!__mk) return;
    if (!g.moved) { mkPlaceAsset(g.a, null, null); return; }
    const v = $("#markupView").getBoundingClientRect();
    const cx = (e.clientX - v.left) / v.width, cy = (e.clientY - v.top) / v.height;
    if (cx < -0.05 || cx > 1.05 || cy < -0.05 || cy > 1.05) return;   // dropped outside
    mkPlaceAsset(g.a, Math.min(1, Math.max(0, cx)), Math.min(1, Math.max(0, cy)));
  };
  grid.addEventListener("pointerup", endGrid);
  grid.addEventListener("pointercancel", endGrid);
  grid.addEventListener("click", (e) => {
    const z = e.target.closest("[data-zoom]");
    if (z) { e.stopPropagation(); mkOpenLibZoom(__mkLib[+z.dataset.zoom]); return; }
    const k = e.target.closest("[data-kill]");
    if (k) { e.stopPropagation(); mkConfirmKill(__mkLib[+k.dataset.kill]); }
  });

  /* ── the preview ── */
  $$("#mkLibZoom [data-zoomclose]").forEach((el) => el.addEventListener("click", mkCloseLibZoom));
  $$("#mkZoomPair .mk-chip").forEach((b) => b.addEventListener("click", () => {
    __mkZoomHalf = b.dataset.half; mkPaintLibZoom();
  }));
  $("#mkZoomPlace").addEventListener("click", () => {
    if (!__mkZoomAsset) return;
    const a = __mkZoomAsset;
    mkCloseLibZoom();
    if (!__mk) { toast("Open a drawing first", "err"); return; }
    mkPlaceAsset(a, 0.5, 0.5);
  });
  /* the escape hatch: sometimes you want the flat picture, not the objects */
  $("#mkZoomFlat").addEventListener("click", () => {
    if (!__mkZoomAsset) return;
    const a = __mkZoomAsset;
    mkCloseLibZoom();
    if (!__mk) { toast("Open a drawing first", "err"); return; }
    mkInsertImage(a, 0.5, 0.5, __mkZoomHalf === "bw" ? "bw" : "charm");
    toast("Placed as a flat picture", "gold");
  });

  /* ── pictures from the device ── */
  $$('.mk-item[data-pic="file"]').forEach((b) => b.addEventListener("click", () => {
    mkCloseAllDrops();
    $("#mkFileInput").click();
  }));
  $$('.mk-item[data-pic="library"]').forEach((b) => b.addEventListener("click", () => {
    mkCloseAllDrops();
    mkDockSet(true, "library");
  }));
  $$('.mk-item[data-pic="killbg"]').forEach((b) => b.addEventListener("click", () => {
    mkCloseAllDrops();
    mkKillBackground();
  }));
  $$('.mk-item[data-pic="restorebg"]').forEach((b) => b.addEventListener("click", () => {
    mkCloseAllDrops();
    mkRestoreBackground();
  }));
  const wtol = $("#mkWandTol");
  if (wtol) {
    /* live: the whole point of a threshold is watching where it lands */
    const apply = (commit) => {
      if (!__mk) return;
      const v = Math.max(2, Math.min(70, Number(wtol.value) || MK_WAND_DEFAULT_TOL));
      $("#mkWandTolVal").textContent = String(v);
      /* with the bucket in hand the slider is about the NEXT fill, so it
         re-judges the preview under the cursor immediately — you watch the
         area grow and shrink and stop where it looks right */
      __mk.tol = v;
      if (__mk.tool === "bucket") {
        __mkPrev.key = "";
        if (__mkPrev.at) mkFillPreviewPaint(__mkPrev.at[0], __mkPrev.at[1]);
        if (commit) mkPersist();
        return;
      }
      const sel = mkSelItems().filter((o) => o.t === "img");
      const list = sel.length ? sel : __mk.items.filter((o) => o.t === "img" && (o.k || []).length);
      if (!list.length) return;
      if (commit) mkPushUndo();
      list.forEach((o) => { o.kt = v; });
      if (commit) mkTouch(); else mkRedraw();
    };
    wtol.addEventListener("input", () => apply(false));
    wtol.addEventListener("change", () => apply(true));
  }
  /* ═══ THE BUG THAT MADE "NOTHING HAPPENS" ═════════════════════════════
     `input.files` hands back the SAME FileList object every time, and
     setting `input.value = ""` EMPTIES THAT LIST IN PLACE. So this:

         const files = e.target.files;
         e.target.value = "";          // ← the list you are holding is now empty
         await mkImportFiles(files);   // ← length 0, returns silently

     …chose a file, reset the input so the same file could be chosen twice,
     and threw the file away in between. No error, no toast, no request:
     exactly "nothing seems to happen".

     Copy the list into a real array FIRST. The input is reset afterwards,
     once nothing depends on it any more. Verified in Chromium: the captured
     FileList goes 1 → 0 on the assignment; an Array.from copy survives. */
  $("#mkFileInput").addEventListener("change", async (e) => {
    const input = e.target;
    const files = Array.from(input.files || []);
    try {
      await mkImportFiles(files);
    } finally {
      /* now it is safe: re-arm the picker so the SAME file can be chosen
         again, which a plain `change` listener would otherwise never see */
      input.value = "";
    }
  });

  /* drag a file straight onto the sheet */
  const stage = $("#markupStage");
  ["dragenter", "dragover"].forEach((t) => stage.addEventListener(t, (e) => {
    if (!__mk) return;
    e.preventDefault();
    e.dataTransfer.dropEffect = "copy";
    stage.classList.add("is-dropzone");
    $("#markupDropHint").hidden = false;
  }));
  ["dragleave", "dragend"].forEach((t) => stage.addEventListener(t, () => {
    stage.classList.remove("is-dropzone");
    $("#markupDropHint").hidden = true;
  }));
  stage.addEventListener("drop", async (e) => {
    if (!__mk) return;
    e.preventDefault();
    stage.classList.remove("is-dropzone");
    $("#markupDropHint").hidden = true;
    const v = $("#markupView").getBoundingClientRect();
    const cx = (e.clientX - v.left) / v.width, cy = (e.clientY - v.top) / v.height;
    if (e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files.length) {
      await mkImportFiles(e.dataTransfer.files, cx, cy);
    }
  });

  /* and paste one */
  document.addEventListener("paste", async (e) => {
    if (!__mk || !$("#markupModal").classList.contains("is-open")) return;
    if (__mk.editing) return;                       // pasting text into a note
    const items = (e.clipboardData && e.clipboardData.items) || [];
    const files = [];
    for (let i = 0; i < items.length; i++) {
      if (items[i].kind === "file") { const f = items[i].getAsFile(); if (f) files.push(f); }
    }
    if (files.length) { e.preventDefault(); await mkImportFiles(files); return; }
    /* nothing on the system clipboard — our own copied layer, then */
    if (__mkClip && __mkClip.length) { e.preventDefault(); mkLayerAct("paste"); }
  });


  /* ── keyboard, the rest of it ── */
  document.addEventListener("keydown", (e) => {
    if (!__mk || !$("#markupModal").classList.contains("is-open")) return;
    if (__mk.editing) return;
    if (e.target && /^(INPUT|TEXTAREA)$/.test(e.target.tagName)) return;
    const meta = e.metaKey || e.ctrlKey;
    if (!meta) return;
    const k = e.key.toLowerCase();
    if (k === "c") { e.preventDefault(); mkLayerAct("copy"); }
    else if (k === "v") { e.preventDefault(); mkLayerAct("paste"); }
    else if (k === "d") { e.preventDefault(); mkLayerAct("dup"); }
    else if (k === "]") { e.preventDefault(); mkLayerAct("up"); }
    else if (k === "[") { e.preventDefault(); mkLayerAct("down"); }
  });
})();

/* ── regenerate driven by the marks alone ─────────────────────────────────
   The marked-up drawing carries the whole request, so no typed message is
   demanded. The thread still records the act, so the conversation stays an
   honest log of what happened. */
async function regenerateFromMarkup() {
  if (genBusy || renderBusy) return;
  const v = state.versions[state.currentVersion];
  if (!v) return;
  if (!markupHasContent((state.markups || {})[v.n])) {
    toast("Mark up the drawing first — the marks are the message", "err");
    return;
  }
  /* ── AN UNTOUCHED SHEET IS NOT A MARK-UP ────────────────────────────────
     Every drawing is taken apart into layers a second after it arrives, so
     the sheet is never empty and this button was never disabled. Pressing it
     without changing anything sent "follow my marks exactly" with no marks
     attached — a refinement whose only content was a list of areas — and
     what came back was a charm covered in things nobody had drawn. Nothing
     has changed, so there is nothing to re-draw from, and it says so instead
     of spending a credit finding out. */
  const _sheet = (state.markups || {})[v.n] || {};
  if (_sheet.traced && !_sheet.dirty) {
    toast("Nothing on this drawing has been changed yet. Move something, add a mark, " +
          "or set an area to Engrave, Cut out or Outline — then we'll re-draw from that. " +
          "To see this drawing in metal as it stands, use Re-render.", "err");
    return;
  }
  const mkMode = ((state.markups || {})[v.n] || {}).mode === "exact" ? "exact" : "interpret";
  pushMsg({ role: "me", text: mkMode === "exact"
    ? "✎ Follow my marks exactly as drawn"
    : "✎ Read my marks and draw what I mean, properly" });
  pushMsg({ role: "thinking", text: mkMode === "exact"
    ? "Re-drawing exactly from your marked-up drawing…"
    : "Reading your marks…" });
  renderThread(); saveSession();
  logEvent("refine", mkMode === "exact"
    ? "Re-drew exactly from the marked-up drawing"
    : "Refined from the marked-up drawing", v.n);
  if (mkMode === "exact") {
    mkPersist();
    const pack = mkItemsOf((state.markups || {})[v.n]).map(mkPack);
    closeMarkup();
    renderStage();
    await runComposedGeneration({ fromMarkup: true, label: "Refined", items: pack });
    return;
  }
  await runGeneration("Refined", "Follow the marked-up drawing exactly.");
}

/* ── RE-DRAW FROM THE REFERENCE ───────────────────────────────────────────
   The customer has just changed the picture the whole design descends from.
   So the changed picture becomes the reference — flattened, uploaded, and
   its fill plan captured at that exact moment, which is the same discipline
   submitComposedDrawing has always used — and then a fresh drawing is made
   from it. Not a refinement: a refinement asks a wrong drawing to fix
   itself, and the entire point of this switch is to stop having to do that. */
let refEditBusy = false;
async function regenerateFromReference() {
  SD("regenerateFromReference: clicked");
  if (refEditBusy || genBusy || renderBusy) {
    SD("regenerateFromReference: BLOCKED — refEditBusy=" + refEditBusy +
       " genBusy=" + genBusy + " renderBusy=" + renderBusy);
    toast(genBusy || refEditBusy ? "Still working on the last drawing — one moment."
                                 : "Still rendering the last charm — one moment.", "err");
    return;
  }
  if (!state.reference) {
    SD("regenerateFromReference: BLOCKED — no reference");
    toast("There's no reference on this design yet", "err"); return;
  }
  const drawn = !!state.reference.drawn;
  const dirty = !!(__mk && __mk.dirty);
  refEditBusy = true;
  const btn = $("#markupRegen");
  const was = btn.innerHTML;
  btn.disabled = true;
  btn.textContent = "Saving your reference…";
  try {
    mkPersist();
    /* UNCHANGED? Then there is nothing to upload and nothing to spend an
       upload slot on — the reference on file already IS this picture, and
       the drawing is simply made from it again. */
    if (dirty) {
      let shot;
      if (drawn) {
        await mkComposeReady();
        shot = mkComposeToDataUrl(1400);
      } else {
        /* marks over the reference bitmap: the same composite the designer
           receives, flattened into a new reference */
        shot = await mkRefCompositeDataUrl(1400);
      }
      if (!shot || !shot.count) { toast("Nothing on the sheet to send", "err"); return; }
      if (shot.tainted) toast("One picture couldn't be included — everything else is here", "err");
      if (uploadsLeft() <= 0) {
        if (!state.user) openAuth("outOfCredits"); else openPricing("out");
        return;
      }
      const sess = ensureSession();
      const up = await postUpload({
        kind: "upload_ref", sessionId: sess.id,
        filename: "my-reference.png", dataUrl: shot.dataUrl,
      });
      /* the plan, captured from the artwork that is actually going up — the
         same rule that keeps a sketch's holes from drifting */
      let zones = [];
      try { zones = mkZonesPayload(mkItemsOf((state.markups || {}).draw).map(mkPack)); }
      catch (e) { zones = []; }
      logEvent("upload", "Changed the reference in the studio");
      replaceReferenceInPlace({ type: "upload", url: up.url, path: up.path,
                                name: drawn ? "Your drawing" : "Your reference",
                                w: 1400, h: 1400, resized: false,
                                drawn: drawn, marks: drawn ? shot.count : 0, zones },
                              null, { clearDrawSheet: !drawn });
      state.refPlanKey = ""; state.refPlan = [];
      refreshWallet();
    }
    closeMarkup();
    if (dirty && !drawn) {
      if (state.markups && state.markups.draw) { delete state.markups.draw; forgetKey("markups", "draw"); }
      if (state.mkHistory && state.mkHistory.draw) { delete state.mkHistory.draw; forgetKey("mkHistory", "draw"); }
    }
    renderStage();
    pushMsg({ role: "me", text: dirty
      ? "✎ I've changed my reference — draw it again from that"
      : "✎ Draw it again from my reference" });
    pushMsg({ role: "thinking", text: "Working from your reference…" });
    renderThread(); saveSession();
    logEvent("refine", dirty ? "Re-drew from the changed reference"
                             : "Re-drew from the reference");
    /* refine:false — this descends from the reference, not from the drawing */
    await runGeneration("From my reference", "", { fromReference: true });
  } catch (err) {
    console.warn("[studio] reference re-draw:", err && err.message);
    toast(err && err.status === 402
      ? "You've used every image slot on this account — add credits to keep going."
      : "That didn't save — try once more.", "err");
  } finally {
    refEditBusy = false;
    btn.disabled = false;
    btn.innerHTML = was;
  }
}
/* the reference bitmap with its marks flattened onto it, for a design whose
   reference is a photograph or a catalogue charm rather than a drawing */
async function mkRefCompositeDataUrl(SIZE) {
  const items = mkItemsOf((state.markups || {}).draw).map(mkPack);
  await mkAwaitImages(items.concat([{ t: "img", u: state.reference.url || "",
                                      sp: state.reference.path || "" }]), 12000);
  const cv = document.createElement("canvas");
  cv.width = SIZE; cv.height = SIZE;
  const ctx = cv.getContext("2d");
  ctx.fillStyle = "#ffffff";
  ctx.fillRect(0, 0, SIZE, SIZE);
  const bg = mkImageFor({ u: state.reference.url || "", sp: state.reference.path || "" });
  let tainted = false;
  if (bg && bg.width) {
    try { ctx.drawImage(bg, 0, 0, SIZE, SIZE); } catch (e) { tainted = true; }
  }
  mkPaint(ctx, SIZE, SIZE, items, { export: true });
  try {
    return { dataUrl: cv.toDataURL("image/png"), count: items.length + 1, tainted };
  } catch (e) {
    return null;
  }
}

/* ═══════════ the composite the designer receives ═════════════════════════
   Base drawing + every mark, flattened into one JPEG by the SAME painter the
   screen uses. A background invocation caps the request at 256 KB, so this
   walks down through sizes and qualities until the base64 fits. If the base
   image taints the canvas (CORS), the marks still travel — drawn on a white
   field with identical geometry — because direction with a blank base beats
   no direction at all.                                                      */
async function buildMarkupPayload(v) {
  const m = v && (state.markups || {})[v.n];
  const items = mkItemsOf(m).map(mkPack);
  if (!items.length) return null;
  /* A drawing that has only been TAKEN APART has not been marked up. Every
     object is exactly where the generator put it, so sending it as direction
     would be sending the design back as a note about itself. The moment a
     hand actually moves something, `dirty` says so and this becomes a real
     mark-up again. */
  if (m.traced && !m.dirty) return null;

  /* Reuse the studio image loader instead of making a raw anonymous request
     to Firebase. Generated versions always carry their Storage path, so this
     resolves through britesAuth as a same-origin data URL and the base image
     cannot disappear from the refinement merely because bucket CORS is off. */
  const baseItem = { t: "img", u: v.url || "", sp: v.path || "" };
  await mkAwaitImages([baseItem], 12000);
  const base = mkImageFor(baseItem);
  /* a composite that shipped before a placed picture had loaded would send
     the designer a drawing with a hole in it */
  await mkAwaitImages(items);
  const crop = Array.isArray(m.crop) && m.crop.length === 3
    ? { x: m.crop[0], y: m.crop[1], w: m.crop[2] } : null;
  const rot = Number(m.baseRot) || 0;

  /* traced: the vectors are the drawing, so painting the bitmap too would
     send the designer everything twice */
  const tracedSheet = !!m.traced;
  const paint = (SIZE, withBaseWanted) => {
    const withBase = withBaseWanted && !tracedSheet;
    const cv = document.createElement("canvas");
    cv.width = SIZE; cv.height = SIZE;
    const ctx = cv.getContext("2d");
    ctx.fillStyle = "#ffffff";
    ctx.fillRect(0, 0, SIZE, SIZE);
    if (withBase && base) {
      /* rotate first, then window the crop onto it — the same order the
         on-screen CSS transform applies, so the export matches the screen */
      let src = base;
      if (rot) {
        const tmp = document.createElement("canvas");
        tmp.width = SIZE; tmp.height = SIZE;
        const tc = tmp.getContext("2d");
        tc.translate(SIZE / 2, SIZE / 2);
        tc.rotate((rot * Math.PI) / 180);
        tc.drawImage(base, -SIZE / 2, -SIZE / 2, SIZE, SIZE);
        src = tmp;
      }
      if (crop) {
        const sw = src.width || SIZE, sh = src.height || SIZE;
        ctx.drawImage(src, crop.x * sw, crop.y * sh, crop.w * sw, crop.w * sh, 0, 0, SIZE, SIZE);
      } else {
        ctx.drawImage(src, 0, 0, SIZE, SIZE);
      }
    }
    /* guides are scaffolding for the person drawing, never direction to the
       designer — they are deliberately not painted into the export */
    mkPaint(ctx, SIZE, SIZE, items, { export: true });
    return cv;
  };

  const LIMIT = 176000;
  for (const withBase of [true, false]) {
    for (const SIZE of [1024, 832, 640, 512]) {
      for (const q of [0.82, 0.66, 0.5]) {
        try {
          const data = paint(SIZE, withBase).toDataURL("image/jpeg", q);
          if (data.length <= LIMIT) {
            return { image: data, mode: m.mode === "exact" ? "exact" : "interpret",
                     notes: mkNotesPayload(items), zones: mkZonesPayload(items) };
          }
        } catch (e) {
          break;                               /* tainted canvas — retry without the base */
        }
      }
    }
  }
  return { image: null, mode: m.mode === "exact" ? "exact" : "interpret",
           notes: mkNotesPayload(items), zones: mkZonesPayload(items) };
}

/* The engraving instruction, sent as DATA as well as drawn in the picture.
   A colour in a JPEG is a thing to be interpreted; a list of regions with
   the word "cutout" beside them is not. Belt and braces, deliberately: this
   is the one instruction that changes the physical object. */
function mkZonesPayload(items) {
  const out = [];
  (items || []).forEach((it) => {
    if (it.hid) return;
    if (it.t === "img" || mkIsText(it) || it.t === "ink" || it.t === "hl" || it.t === "sign") return;
    /* THE STUDIO'S OWN GRAMMAR SAYS A RING IS A CUT-OUT — the tool hint says
       it, the prompt's vocabulary section says it, and mkItemIntent is now
       the one place that says it, so the sheet says it too. */
    let intent = mkItemIntent(it);
    const b = mkBBox(it);
    /* ── THE SILHOUETTE IS A CUT, NOT A CUT-OUT ─────────────────────────
       Production drawings now draw the charm's outer perimeter in the
       instruction blue — it IS where the laser cuts — so the traced
       "Charm outline" layer arrives classified as a cut-out. True of the
       edge, catastrophic in this list: "CUT CLEAN THROUGH: an area spanning
       nearly the whole drawing" tells the workshop to remove the face of
       the charm. A blue silhouette is therefore reported as what its
       black-ink predecessor was: engraving of the drawing's linework,
       which the ENGRAVED-SOLID summary covers without itemising. */
    if (intent === "cutout" && b.w > 0.85 && b.h > 0.85 &&
        it.tr && /Charm outline/.test(String(it.n || ""))) intent = "engrave";
    const r = (n) => Math.round(n * 100) / 100;
    /* more than one closed contour means a hole in the middle of it: the
       rim of a disc, the inside of an O. Its bounding box is the whole disc,
       so the workshop has to be told it is a BAND and not a face. */
    const ring = Array.isArray(it.q) && it.q.length > 1;
    /* ── NO NAMES. EVER. ────────────────────────────────────────────────
       Every area used to travel with its layer name, and the layer names in
       a real design are "logo 4", "logo 12", "Engraved · Cherry3D Logo.png".
       Handed to an image model inside a list it is told to obey, those are
       not identifiers — they are words, and the model engraved them: a
       finished charm came back with "logo 2 area · 9%" set around its rim in
       type nobody asked for. An area is identified by WHERE IT IS and HOW
       BIG IT IS, which is what the picture beside the list also says. */
    out.push({ intent, x: r(b.x), y: r(b.y), w: r(b.w), h: r(b.h), ring,
               where: mkWhereOf(b.x + b.w / 2, b.y + b.h / 2) });
  });
  return out.slice(0, 40);
}
function mkWhereOf(x, y) {
  const col = x < 0.34 ? "left" : x > 0.66 ? "right" : "centre";
  const row = y < 0.34 ? "top" : y > 0.66 ? "bottom" : "middle";
  return row === "middle" && col === "centre" ? "the centre" : `${row} ${col}`;
}

/* ═══════════ THE FILL PLAN ═══════════════════════════════════════════════
   WHAT THIS IS FOR, IN ONE PARAGRAPH.

   A customer marks an area to be cut clean through. The design step draws a
   black-and-white production drawing of the charm, and a B/W drawing has no
   blue in it — a hole and a polished disc are both white, an engraved area
   and a cut edge are both black. So the instruction died at that step, every
   time: the drawing came back, was taken apart into layers, and every layer
   said ENGRAVE because black is all a tracer can see. The render step then
   read that same drawing and made its own mind up. Three stages, and the one
   thing that changes the physical object survived none of them.

   The fix is to stop asking the picture. The three instructions are DATA,
   they are the customer's own, and they are carried forward explicitly:

     · the plan travels with every generation (refZones / markupZones) so the
       model is told, in words, which areas are holes;
     · the plan is STORED ON THE VERSION it produced (`v.zones`), so it is
       still there on the next refine, the next render, the next session, and
       on another device;
     · the plan is re-projected onto the traced layers the moment a drawing
       is taken apart, so the customer SEES blue where they said blue —
       including where the drawing correctly rendered the cut-out as an open
       hole, which has no ink of its own and therefore no layer of its own
       until this puts one there;
     · and where there is no plan at all — an uploaded picture, a catalogue
       charm — one is read off the reference's own pixels, because blue and
       red mean the same thing in an uploaded design as they do in a drawn
       one.
   ====================================================================== */

function mkPlanBox(z) {
  return { x: Number(z.x) || 0, y: Number(z.y) || 0,
           w: Math.max(1e-4, Number(z.w) || 0), h: Math.max(1e-4, Number(z.h) || 0) };
}
function mkBoxIoU(a, b) {
  const x0 = Math.max(a.x, b.x), y0 = Math.max(a.y, b.y);
  const x1 = Math.min(a.x + a.w, b.x + b.w), y1 = Math.min(a.y + a.h, b.y + b.h);
  const iw = x1 - x0, ih = y1 - y0;
  if (iw <= 0 || ih <= 0) return 0;
  const inter = iw * ih;
  const uni = a.w * a.h + b.w * b.h - inter;
  return uni > 0 ? inter / uni : 0;
}
/* the box of ONE contour of a multi-contour fill — the hole in a ring, the
   inside of an O — plus where its points sit in the flat array */
function mkContourBox(it, ci) {
  const p = it.p || [];
  const q = (it.q && it.q.length) ? it.q : [Math.floor(p.length / 2)];
  let k = 0;
  for (let n = 0; n < ci; n++) k += Number(q[n]) || 0;
  /* `q` counts POINTS, not coordinates — a triangular hole is three of them */
  const len = Number(q[ci]) || 0;
  if (len < 3) return null;
  let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
  for (let j = 0; j < len; j++) {
    const X = p[(k + j) * 2], Y = p[(k + j) * 2 + 1];
    if (!isFinite(X) || !isFinite(Y)) continue;
    if (X < x0) x0 = X;
    if (X > x1) x1 = X;
    if (Y < y0) y0 = Y;
    if (Y > y1) y1 = Y;
  }
  if (!isFinite(x0)) return null;
  return { x: x0, y: y0, w: x1 - x0, h: y1 - y0, at: k, len };
}

/* Re-project a plan onto the layers a drawing was just taken apart into.
   ONLY CUT-OUTS ARE MOVED, and the reason is worth stating: a traced layer
   is a region of INK, and ink is engraved metal — that is true of a solid
   engraved area and equally true of the thin band that IS an outline. So
   "engrave" and "outline" are already correct on arrival and touching them
   would be a lie. A cut-out is the one instruction black ink cannot express,
   and it is the one this repairs. */
function mkApplyPlanToTrace(items, plan) {
  if (!Array.isArray(items) || !items.length) return 0;
  const cuts = (Array.isArray(plan) ? plan : []).filter((z) =>
    z && z.intent === "cutout" && Number(z.w) > 0.004 && Number(z.h) > 0.004);
  if (!cuts.length) return 0;
  let done = 0;
  const used = {};
  cuts.forEach((z) => {
    const zb = mkPlanBox(z);
    /* 0 — ALREADY SAID. Colour-coded drawings arrive with their cut-outs
       traced blue, and re-projecting the plan on top of that must not stack
       a second blue layer on the first. An agreeing cut-out that already
       exists satisfies its zone outright. */
    for (let i = 0; i < items.length; i++) {
      const it = items[i];
      if (it.t === "fill" && mkFillIntent(it.f) === "cutout" &&
          mkBoxIoU(zb, mkBBox(it)) >= 0.34) return;
    }
    /* 1 — the drawing drew it as an area, so a layer already exists for it */
    let best = null;
    items.forEach((it, i) => {
      if (it.t !== "fill" || used[i]) return;
      if (mkFillIntent(it.f) === "cutout") return;
      const s = mkBoxIoU(zb, mkBBox(it));
      if (s < 0.34) return;
      /* THE CHARM'S OWN OUTLINE IS NOT A HOLE. Its box is the whole sheet,
         so a cut-out band that also spans the whole sheet — a rim, a ring —
         agrees with it as strongly as with the band itself. The band wins,
         and the outline is only ever taken when the customer's own area
         genuinely is the entire charm and nothing else is offered. */
      const outline = it.n === "Charm outline";
      if (outline && !(zb.w > 0.8 && zb.h > 0.8)) return;
      const rank = s - (outline ? 0.12 : 0);
      if (!best || rank > best.rank) best = { i, it, rank };
    });
    if (best) {
      best.it.f = MK_CUTOUT;
      best.it.n = "Cut out · " + String(best.it.n || "area").slice(0, 20);
      used[best.i] = 1;
      done++;
      return;
    }
    /* 2 — the drawing obeyed the contract and drew an OPEN HOLE, which is
       white paper inside somebody else's contour. There is no ink there and
       therefore no layer, so the customer had nothing to see and nothing to
       click. It gets its own layer, in blue, sitting directly on top of the
       shape it is cut out of. */
    for (let i = 0; i < items.length; i++) {
      const it = items[i];
      if (it.t !== "fill") continue;
      const q = it.q || [];
      if (q.length < 2) continue;
      let made = false;
      for (let ci = 1; ci < q.length; ci++) {
        const cb = mkContourBox(it, ci);
        if (!cb || mkBoxIoU(zb, cb) < 0.34) continue;
        const flat = (it.p || []).slice(cb.at * 2, (cb.at + cb.len) * 2);
        if (flat.length < 6) continue;
        items.splice(i + 1, 0, mkPack({
          id: mkId(), t: "fill", c: MK_CUTOUT, f: MK_CUTOUT, w: 0.0015,
          p: flat, q: [cb.len], tr: 1, n: "Cut out",
        }));
        done++;
        made = true;
        break;
      }
      if (made) return;
    }
  });
  return done;
}

/* ── reading a plan off a picture ─────────────────────────────────────────
   For a reference nobody drew here: an uploaded design, a charm out of the
   catalogue. Blue and red mean in an uploaded file exactly what they mean on
   the sheet, so the picture is read the same way the sheet is written. */
const MK_REF_S = 420;
function mkZonesFromImage(img) {
  const S = MK_REF_S;
  const cv = document.createElement("canvas");
  cv.width = S; cv.height = S;
  const ctx = cv.getContext("2d", { willReadFrequently: true });
  ctx.fillStyle = "#ffffff";
  ctx.fillRect(0, 0, S, S);
  try { ctx.drawImage(img, 0, 0, S, S); } catch (e) { return []; }
  let d;
  try { d = ctx.getImageData(0, 0, S, S).data; } catch (e) { return []; }
  const N = S * S;
  const KIND = { engrave: 1, cutout: 2, none: 3 };
  const cls = new Uint8Array(N);
  let inked = 0;
  for (let i = 0; i < N; i++) {
    const j = i * 4;
    if (d[j + 3] < 40) continue;                       /* transparent is paper */
    const k = KIND[mkPixelIntent(d[j], d[j + 1], d[j + 2])] || 0;
    cls[i] = k;
    if (k) inked++;
  }
  if (!inked || inked > N * 0.92) return [];
  const lab = new Int32Array(N);
  const out = [];
  const stack = [];
  let next = 0;
  const minArea = Math.max(24, Math.round(N * 0.0006));
  for (let seed = 0; seed < N && out.length < 60; seed++) {
    const k = cls[seed];
    if (!k || lab[seed]) continue;
    next++;
    let area = 0, x0 = S, y0 = S, x1 = -1, y1 = -1;
    stack.length = 0; stack.push(seed); lab[seed] = next;
    while (stack.length) {
      const q = stack.pop();
      area++;
      const px = q % S, py = (q / S) | 0;
      if (px < x0) x0 = px;
      if (px > x1) x1 = px;
      if (py < y0) y0 = py;
      if (py > y1) y1 = py;
      if (px > 0     && cls[q - 1] === k && !lab[q - 1]) { lab[q - 1] = next; stack.push(q - 1); }
      if (px < S - 1 && cls[q + 1] === k && !lab[q + 1]) { lab[q + 1] = next; stack.push(q + 1); }
      if (py > 0     && cls[q - S] === k && !lab[q - S]) { lab[q - S] = next; stack.push(q - S); }
      if (py < S - 1 && cls[q + S] === k && !lab[q + S]) { lab[q + S] = next; stack.push(q + S); }
    }
    if (area < minArea) continue;
    let intent = k === 2 ? "cutout" : k === 3 ? "none" : "engrave";
    /* ── A LINE IS NEVER AN AREA — on either colour channel. ─────────────
       A RED component read off pixels is ALWAYS a boundary line: red is a
       reserved word this studio only ever writes as the edge of an
       outline-only region, so there is no such thing as a genuine solid red
       area to find. But a thin red rectangle's BOUNDING BOX is the whole
       region it wraps — and reported as "OUTLINE ONLY: an area spanning
       nearly the whole drawing", it told the model to leave nearly the
       whole drawing as bare polished metal. It obeyed: a charm whose thick
       engraved band was simply erased, redrawn as one thin outline. The red
       lines still travel in the picture itself, and the law tells the model
       what red means — the LIST never speaks of them.
       The same test hardens the blue side: any blue component whose ink
       fills almost none of its own box is an edge (the silhouette, a cut
       line), whatever its size — a genuine blue area fills its box. */
    const boxFill = area / Math.max(1, (x1 - x0 + 1) * (y1 - y0 + 1));
    if (intent === "none") continue;
    if (intent === "cutout" && boxFill < 0.28 &&
        ((x1 - x0 + 1) > S * 0.4 || (y1 - y0 + 1) > S * 0.4)) intent = "engrave";
    const r = (v) => Math.round(v * 100) / 100;
    out.push({ intent, area,
               x: r(x0 / S), y: r(y0 / S),
               w: r((x1 - x0 + 1) / S), h: r((y1 - y0 + 1) / S), ring: false,
               where: mkWhereOf((x0 + x1) / 2 / S, (y0 + y1) / 2 / S) });
  }
  /* the biggest areas are the ones worth naming; a plan is not a census */
  out.sort((a, b) => b.area - a.area);
  return out.slice(0, 24).map((z) => { delete z.area; return z; });
}
/* cached per reference, because reading it costs one canvas and the answer
   cannot change while the reference does not */
async function mkReadRefPlan(force) {
  const r = state.reference;
  if (!r) return [];
  const key = refKey(r);
  if (!force && state.refPlanKey === key) return state.refPlan || [];
  const u = r.url || (r.item && r.item.image) || "";
  const sp = r.path || "";
  if (!u && !sp) return [];
  const probe = { t: "img", u, sp };
  let img = mkImageFor(probe);
  if (!img) {
    await mkAwaitImages([probe], 8000);
    img = mkImageFor(probe);
  }
  if (!img || !img.width) return [];
  let z = [];
  try { z = mkZonesFromImage(img); } catch (e) { z = []; }
  state.refPlanKey = key;
  state.refPlan = z;
  return z;
}

/* the audit: which of the plan's cut-outs does this drawing actually show? */
function mkAuditVersion(v, img, plan) {
  const cuts = (Array.isArray(plan) ? plan : []).filter((z) =>
    z && z.intent === "cutout" && Number(z.w) > 0.004 && Number(z.h) > 0.004);
  let seen = [];
  try { seen = mkZonesFromImage(img) || []; } catch (e) { return; }
  const seenCuts = seen.filter((z) => z.intent === "cutout");
  const missed = cuts.filter((z) =>
    !seenCuts.some((q) => mkBoxIoU(mkPlanBox(z), mkPlanBox(q)) >= 0.25));
  /* ── AND THE OTHER HALF: WHAT THE MODEL MADE UP ─────────────────────────
     A drawing that arrives with a blue area NOBODY declared is exactly as
     wrong as one missing a declared hole — a solid blue ring around a logo
     the customer never asked to cut is a physically different charm. Only
     substantial inventions are named: tiny blue flecks are anti-aliasing,
     and the hoop's own hole and cut edges never classify as areas at all
     (the line-vs-area guard has already dropped them). */
  const invented = seenCuts.filter((q) =>
    Math.max(Number(q.w) || 0, Number(q.h) || 0) > 0.06 &&
    !cuts.some((z) => mkBoxIoU(mkPlanBox(z), mkPlanBox(q)) >= 0.18));
  v.audit = (missed.length || invented.length)
    ? { missed: missed.slice(0, 8), invented: invented.slice(0, 8) } : null;
  if (missed.length || invented.length) {
    const bits = [];
    if (missed.length) bits.push("the cut-out at " +
      missed.slice(0, 3).map((z) => z.where || "the centre").join(", ") +
      (missed.length > 3 ? " (and more)" : "") + " didn't come through");
    if (invented.length) bits.push("a cut-out you never chose appeared at " +
      invented.slice(0, 3).map((z) => z.where || "the centre").join(", "));
    toast("Checked the drawing against your engraving choices — " +
          bits.join("; and ") +
          ". Generate again and it will be called out explicitly.", "err");
  }
  saveSession();
}
/* the correction, spoken the only safe way: ordinary words, no digits */
function mkAuditNote(v) {
  const a = v && v.audit;
  const missed = (a && Array.isArray(a.missed)) ? a.missed : [];
  const invented = (a && Array.isArray(a.invented)) ? a.invented : [];
  if (!missed.length && !invented.length) return "";
  const sizeWord = (z) => {
    const span = Math.max(Number(z.w) || 0, Number(z.h) || 0);
    return z.ring ? "a ring-shaped band" :
           span > 0.45 ? "a large area" : span > 0.2 ? "a medium-sized area" :
           span > 0.08 ? "a small area" : "a tiny area";
  };
  const at = (z) => sizeWord(z) + " at " + (z.where || "the centre");
  let out = "CORRECTION — THE PREVIOUS ATTEMPT BROKE THE GOLDEN RULE.";
  if (missed.length) out += " It failed to cut clean through the following " +
    "declared areas, and this attempt MUST show every one of them as a real " +
    "opening (solid blue in the drawing): " + missed.map(at).join("; ") + ".";
  if (invented.length) out += " It also INVENTED cut-outs the customer never " +
    "chose — this attempt must contain NO blue area at: " +
    invented.map(at).join("; ") + "; those places are engraved or polished " +
    "exactly as the reference shows them, never openings.";
  return out + " Nothing about this is optional.";
}

/* THE ONE ANSWER, asked by the generation, the render and the tracer alike.
   Nearest truth first: what a hand has actually done on this drawing, then
   what this drawing was made from, then the sketch it all started as, then
   the reference's own pixels. */
function mkFillPlan(v) {
  try {
    const m = v ? (state.markups || {})[mkVersionKey(v)] : null;
    if (m) {
      const items = mkItemsOf(m).map(mkPack);
      if (items.length) {
        const z = mkZonesPayload(items);
        /* An UNTOUCHED trace is not an instruction: every part of it says
           ENGRAVE because black ink is all a tracer can see, and reading that
           back as the customer's own count asserts "no holes anywhere" on a
           charm that has them. It becomes an instruction the moment a hand
           changes something — or the moment the plan above has been projected
           onto it and a cut-out is standing there in blue. */
        const meaningful = !m.traced || m.dirty || z.some((q) => q.intent === "cutout");
        if (meaningful && z.length) return z.slice(0, 40);
      }
      if (Array.isArray(m.plan) && m.plan.length) return m.plan.slice(0, 40);
    }
    if (v && Array.isArray(v.zones) && v.zones.length) return v.zones.slice(0, 40);
    if (state.reference && state.reference.drawn) {
      const rz = state.reference.zones;
      if (Array.isArray(rz) && rz.length) return rz.slice(0, 40);
      const sk = mkZonesPayload(mkItemsOf((state.markups || {}).draw).map(mkPack));
      if (sk.length) return sk.slice(0, 40);
    }
    /* the previous version's plan, for a design whose current sheet has none */
    for (let i = (state.versions || []).length - 1; i >= 0; i--) {
      const pv = state.versions[i];
      if (pv && Array.isArray(pv.zones) && pv.zones.length) return pv.zones.slice(0, 40);
    }
    /* the cached read of the reference's own pixels — but only while it is
       still THIS design's reference: loading another design out of My
       designs must not inherit the last one's holes */
    if (Array.isArray(state.refPlan) && state.refPlan.length &&
        state.refPlanKey === refKey(state.reference)) return state.refPlan.slice(0, 40);
  } catch (e) { /* a plan is help, never a gate */ }
  return [];
}

/* Each note travels WITH its anchor point, so the designer can be told not
   just what the customer wrote but WHERE they pinned it — "make this bigger"
   means nothing without the where. */
function mkNotesPayload(items) {
  return (items || [])
    .filter((it) => it.t === "text" || it.t === "callout" || it.t === "label")
    .map((it) => ({ text: String(it.s || "").trim(),
                    x: Number((it.p || [])[0]) || 0,
                    y: Number((it.p || [])[1]) || 0,
                    /* the height travels so "keep my text exactly this size"
                       is enforceable server-side, not just visible in pixels */
                    h: Math.round((Number(it.fs) || 0.036) * 1000) / 1000,
                    kind: it.t === "label" ? "lettering" : "note" }))
    .filter((n) => n.text);
}

/* ═══════════ the sketchpad as a reference image ══════════════════════════
   A drawing made here becomes the reference by exactly the route an upload
   takes — same function, same allowance, same suitability check — so nothing
   downstream needs to know or care where the picture came from.             */
function mkComposeToDataUrl(SIZE) {
  const items = mkItemsOf((state.markups || {}).draw).map(mkPack);
  const cv = document.createElement("canvas");
  cv.width = SIZE; cv.height = SIZE;
  const ctx = cv.getContext("2d");
  ctx.fillStyle = "#ffffff";
  ctx.fillRect(0, 0, SIZE, SIZE);
  mkPaint(ctx, SIZE, SIZE, items, { export: true });
  /* A canvas that has drawn a cross-origin picture without CORS headers is
     tainted and toDataURL throws. mkImageFor works hard to avoid that — it
     falls back to fetching the bytes through britesAuth, which is same-origin
     — but if a picture slipped through the CORS path anyway, the sketch is
     still the customer's work and must not be lost: re-paint without the
     pictures and say so, rather than failing the submit. */
  try {
    return { dataUrl: cv.toDataURL("image/png"), count: items.length, tainted: false };
  } catch (e) {
    const safe = items.filter((it) => it.t !== "img");
    ctx.fillStyle = "#ffffff";
    ctx.fillRect(0, 0, SIZE, SIZE);
    mkPaint(ctx, SIZE, SIZE, safe, { export: true });
    return { dataUrl: cv.toDataURL("image/png"), count: items.length, tainted: true };
  }
}
/* the sketchpad's submit waits on this first */
async function mkComposeReady() {
  await mkAwaitImages(mkItemsOf((state.markups || {}).draw));
}


/* =============================================================================
   STEP 3 — ORDER MATH + CART HAND-OFF
   ═══════════════════ PRICING MODEL — one place, fully derived ═══════════════
   price = base(type) + metalPremium + sizeDelta × metalFactor
   · sizeDelta scales with the metal because a larger charm is literally more
     material — the jump from 10 mm to 20 mm costs far more in solid gold than
     in gold fill, and the model reflects that instead of a flat surcharge.
   · 14k ROSE GOLD FILLED is always exactly 10% above 14k GOLD FILLED, computed
     from the gold-filled price rather than carried as its own constant, so the
     relationship holds at every size, format and quantity.
   The Shopify variant prices MUST be created to match this ladder exactly, or
   the cart will contradict the studio (§10.10).      [prototype, verbatim]
   ========================================================================== */
const TYPES = {
  charm:  { base:35, label:"Charm Only — Necklace Charm",     sized:true,  engravable:true,
            handle:"custom-charm-only" },
  huggie: { base:51, label:"Charm Only — Huggie Hoops (pair)", sized:false, engravable:false,
            note:"One size · roughly 5–7 mm each, set by the design", handle:"custom-charm-huggie" },
  chain:  { base:54, label:"Regular Chain + Charm Necklace",   sized:true,  engravable:true,
            handle:"custom-charm-chain" },
  beady:  { base:68, label:"Beady Chain + Charm Necklace",     sized:true,  engravable:true,
            handle:"custom-charm-beady" },
  studs:  { base:45, label:"Stud Earrings (pair)",             sized:false, engravable:false,
            note:"One size · roughly 5–7 mm each, set by the design", handle:"custom-charm-studs" },
};
const SIZES = [
  { mm:10, delta:-4 },
  { mm:12, delta:-2, popular:true },   // "Most loved" — also the pre-made default
  { mm:14, delta: 0 },
  { mm:18, delta: 5 },
  { mm:20, delta: 8 },
];
const DEFAULT_MM = (SIZES.find((s) => s.popular) || SIZES[2]).mm;
const METAL_LABELS  = { silver:"Sterling Silver", gold:"14k Gold Filled", rose:"14k Rose Gold Filled", solid10:"10k Solid Gold", solid14:"14k Solid Gold" };
const METAL_PREMIUM = { silver:0, gold:6, solid10:120, solid14:180 };   // rose is derived
const METAL_FACTOR  = { silver:1, gold:1, solid10:4, solid14:5 };       // size scales with material
let   ROSE_PREMIUM  = 0.10;                                            // always exactly +10%
const round2 = (n) => Math.round(n * 100) / 100;

function itemPrice(typeKey, metal, mm) {
  const t = TYPES[typeKey] || TYPES.charm;
  const size = t.sized ? (SIZES.find((s) => s.mm === mm) || SIZES[2]) : { delta: 0 };
  if (metal === "rose") return round2(itemPrice(typeKey, "gold", mm) * (1 + ROSE_PREMIUM));
  return round2(t.base + METAL_PREMIUM[metal] + size.delta * METAL_FACTOR[metal]);
}
function sizeDeltaFor(typeKey, metal, mm) {
  return round2(itemPrice(typeKey, metal, mm) - itemPrice(typeKey, metal, orderState.mm));
}
window.__itemPrice = itemPrice;                     // the Playwright pricing suite reads this
const orderState = { metal:"silver", qty:1, mm:DEFAULT_MM };

function renderSizes(typeKey) {
  const t = TYPES[typeKey];
  $("#sizeBlock").hidden = !t.sized;
  $("#oneSizeNote").hidden = !!t.sized;
  if (!t.sized) {
    $("#oneSizeNote").innerHTML =
      `<svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" stroke-width="1.4" style="color:var(--bj-gold);flex:none;"><path d="M4 12h16M4 9v6m16-6v6"/></svg>${t.note}`;
    return;
  }
  const maxMm = SIZES[SIZES.length - 1].mm;
  $("#sizeRow").innerHTML = SIZES.map((s) => {
    const d = sizeDeltaFor(typeKey, orderState.metal, s.mm);
    const px = 9 + (s.mm / maxMm) * 13;                    // dot scaled to true proportion
    return `<button class="size-opt${s.mm === orderState.mm ? " is-active" : ""}" data-mm="${s.mm}" type="button">
      ${s.popular ? '<span class="size-tag">Most loved</span>' : ""}
      <span class="size-dot" style="width:${px.toFixed(1)}px;height:${px.toFixed(1)}px;"></span>
      <span class="size-mm">${s.mm}mm</span>
      <span class="size-delta">${d === 0 ? "" : (d > 0 ? "+" : "−") + fmt(Math.abs(d))}</span>
    </button>`;
  }).join("");
  $$("#sizeRow .size-opt").forEach((b) => b.addEventListener("click", () => {
    orderState.mm = +b.dataset.mm; updateOrder();
  }));
  $("#sizeLive").textContent = `${orderState.mm} mm selected`;
}
function orderContext() {
  const opt = $("#orderFormat").selectedOptions[0];
  const typeKey = opt.value;
  const t = TYPES[typeKey];
  const isChainNecklace = ["chain", "beady"].includes(typeKey);
  const ext = isChainNecklace && $("#extToggle").checked;
  const heart = ext && $("#heartToggle").checked;
  const canEngrave = t.engravable !== false;
  const engraved = canEngrave && $("#engraveToggle").checked;
  return { opt, typeKey, t, isChainNecklace, ext, heart, canEngrave, engraved,
           unit: itemPrice(typeKey, orderState.metal, orderState.mm) };
}
function updateOrder() {
  const opt = $("#orderFormat").selectedOptions[0];
  const typeKey = opt.value;
  renderSizes(typeKey);
  syncFormatUI();
  const unit = itemPrice(typeKey, orderState.metal, orderState.mm);
  const isChainNecklace = ["chain", "beady"].includes(typeKey);
  $("#extenderBlock").hidden = !isChainNecklace;
  const ext = isChainNecklace && $("#extToggle").checked;
  $("#heartToggle").disabled = !ext;
  if (!ext) $("#heartToggle").checked = false;
  const heart = ext && $("#heartToggle").checked;
  /* Huggie Hoops and Stud Earrings cannot be engraved — the block disappears
     and any engraving already ticked is dropped from the order and the price. */
  const canEngrave = TYPES[typeKey] && TYPES[typeKey].engravable !== false;
  $("#engraveBlock").hidden = !canEngrave;
  if (!canEngrave) { $("#engraveToggle").checked = false; $("#engraveInput").value = ""; }
  const engraved = canEngrave && $("#engraveToggle").checked;
  $("#engraveInput").hidden = !engraved;
  $("#pdpPrice").textContent = CONFIG.currency + unit;
  const ov = state.versions[state.currentVersion];
  $("#orderVersionLbl").textContent = ov ? `Ordering version ${ov.n} of ${state.versions.length}` : "";
  $("#metalSelLbl").textContent = METAL_LABELS[orderState.metal];
  $("#qtyVal").textContent = orderState.qty;
  const total = CONFIG.designFee + unit * orderState.qty +
                (engraved ? CONFIG.engravingFee : 0) + (heart ? CONFIG.extenderHeartFee : 0);
  $("#orderTotal").textContent = fmt(total);
}
$("#orderFormat").addEventListener("change", updateOrder);

/* ── Bespoke dropdown ──────────────────────────────────────────────────────
   The native <select> stays the source of truth: this only paints it and
   writes back through .value + a dispatched change event, so every existing
   listener (and any automated test using selectOption) keeps working.
                                                        [prototype, verbatim] */
const fmtSel = { open:false, focus:0 };
function buildFormatMenu() {
  const sel = $("#orderFormat"), menu = $("#formatMenu");
  menu.innerHTML = [...sel.options].map((o, i) => `
    <li class="bj-select__opt" role="option" data-i="${i}" data-value="${o.value}"
        aria-selected="${o.selected ? "true" : "false"}" id="fmtOpt${i}">
      <span class="bj-select__tick">${o.selected ? "✓" : ""}</span>${o.textContent}
    </li>`).join("");
  menu.querySelectorAll(".bj-select__opt").forEach((li) => {
    li.addEventListener("click", () => chooseFormat(+li.dataset.i));
    li.addEventListener("mousemove", () => setFormatFocus(+li.dataset.i));
  });
}
function syncFormatUI() {
  const sel = $("#orderFormat");
  $("#formatValue").textContent = (sel.selectedOptions[0] && sel.selectedOptions[0].textContent) || "";
  $("#formatMenu").querySelectorAll(".bj-select__opt").forEach((li) => {
    const on = +li.dataset.i === sel.selectedIndex;
    li.setAttribute("aria-selected", on ? "true" : "false");
    li.querySelector(".bj-select__tick").textContent = on ? "✓" : "";
  });
}
function setFormatFocus(i) {
  fmtSel.focus = i;
  const menu = $("#formatMenu");
  menu.querySelectorAll(".bj-select__opt").forEach((li) => li.classList.toggle("is-focus", +li.dataset.i === i));
  const el = menu.querySelector(`.bj-select__opt[data-i="${i}"]`);
  if (el) { $("#formatBtn").setAttribute("aria-activedescendant", el.id); el.scrollIntoView({ block: "nearest" }); }
}
function openFormat() {
  if (fmtSel.open) return;
  fmtSel.open = true;
  $("#formatMenu").hidden = false;
  requestAnimationFrame(() => $("#formatSelect").classList.add("is-open"));
  $("#formatBtn").setAttribute("aria-expanded", "true");
  setFormatFocus($("#orderFormat").selectedIndex);
}
function closeFormat(refocus) {
  if (!fmtSel.open) return;
  fmtSel.open = false;
  $("#formatSelect").classList.remove("is-open");
  $("#formatBtn").setAttribute("aria-expanded", "false");
  $("#formatBtn").removeAttribute("aria-activedescendant");
  setTimeout(() => { if (!fmtSel.open) $("#formatMenu").hidden = true; }, 220);
  if (refocus) $("#formatBtn").focus();
}
function chooseFormat(i) {
  const sel = $("#orderFormat");
  if (i !== sel.selectedIndex) {
    sel.selectedIndex = i;
    sel.dispatchEvent(new Event("change", { bubbles: true }));   // drives updateOrder
  } else syncFormatUI();
  closeFormat(true);
}
$("#formatBtn").addEventListener("click", () => fmtSel.open ? closeFormat(true) : openFormat());
$("#formatBtn").addEventListener("keydown", (e) => {
  const last = $("#orderFormat").options.length - 1;
  if (["ArrowDown", "ArrowUp", "Enter", " ", "Home", "End"].includes(e.key)) e.preventDefault(); else return;
  if (!fmtSel.open) { openFormat(); if (e.key === "ArrowDown") setFormatFocus(Math.min(last, fmtSel.focus + 1)); return; }
  if (e.key === "ArrowDown") setFormatFocus(Math.min(last, fmtSel.focus + 1));
  else if (e.key === "ArrowUp") setFormatFocus(Math.max(0, fmtSel.focus - 1));
  else if (e.key === "Home") setFormatFocus(0);
  else if (e.key === "End") setFormatFocus(last);
  else chooseFormat(fmtSel.focus);                              // Enter / Space
});
$("#formatBtn").addEventListener("keyup", (e) => { if (e.key === "Escape") closeFormat(true); });
document.addEventListener("keydown", (e) => { if (e.key === "Escape" && fmtSel.open) closeFormat(true); });
document.addEventListener("click", (e) => { if (fmtSel.open && !e.target.closest("#formatSelect")) closeFormat(false); });
buildFormatMenu();
$$("#orderMetalRow .mp").forEach((b) => b.addEventListener("click", () => {
  $$("#orderMetalRow .mp").forEach((x) => x.classList.remove("is-active"));
  b.classList.add("is-active");
  orderState.metal = b.dataset.metal;
  updateOrder();
}));
$("#engraveToggle").addEventListener("change", () => {
  updateOrder();
  if ($("#engraveToggle").checked) $("#engraveInput").focus();
});
$("#extToggle").addEventListener("change", updateOrder);
$("#heartToggle").addEventListener("change", updateOrder);
$("#qtyMinus").addEventListener("click", () => { orderState.qty = Math.max(1, orderState.qty - 1); updateOrder(); });
$("#qtyPlus").addEventListener("click", () => { orderState.qty = Math.min(9, orderState.qty + 1); updateOrder(); });

/* ── Cart hand-off (§10.10) ────────────────────────────────────────────────
   The theme adds to cart with AJAX and a plain properties object, and
   brites-bundle-module already uses the multi-item { items: [...] } shape, so
   the studio uses the theme's own idiom. Two lines go in: the charm and the
   $9.99 custom design fee, which is present ONLY on studio orders and carries
   the same Design ID. Properties whose key starts with "_" are hidden by both
   cart templates; a value containing "uploads" + an image extension renders as
   a clickable thumbnail, which is why the customer-facing design copy lives
   under .../uploads/designs/... . */
function variantFor(handle, opts) {
  const list = VARIANTS[handle];
  if (!Array.isArray(list) || !list.length) return null;
  const want = opts.filter((o) => o != null && o !== "");
  if (!want.length) return list[0];
  for (const v of list) {
    const have = (v.o || []).filter((o) => o != null && o !== "");
    if (have.length !== want.length) continue;
    let ok = true;
    for (let i = 0; i < want.length; i++) if (String(have[i]) !== String(want[i])) { ok = false; break; }
    if (ok) return v;
  }
  return null;
}
function cartError(msg) {
  const btn = $("#addToCartBtn");
  btn.disabled = false;
  updateOrder();
  toast(msg, "err");
}
$("#addToCartBtn").addEventListener("click", async () => {
  const v = state.versions[state.currentVersion];
  if (!v) { toast("Approve a design first", "err"); return; }
  if (!state.user) { openAuth("approve"); return; }
  const ctx = orderContext();
  const s = ensureSession();

  const charmVariant = variantFor(ctx.t.handle,
    [METAL_LABELS[orderState.metal], ctx.t.sized ? orderState.mm + " mm" : null]);
  const feeVariant = variantFor(D.feeHandle || "custom-design-fee", []);
  if (!charmVariant) return cartError("That combination isn't available yet — please pick another metal or size.");

  /* The approved SET travels together: the finished charm and the production
     drawing it was cut from. Both URLs live under .../uploads/... so both
     cart templates render each as a clickable thumbnail — the customer sees
     exactly the pair they approved, at every step to checkout. */
  const designProps = Object.assign({
    "Your charm":   v.renderUrl || v.url,        // renders as a thumbnail in the cart
    "Design ID":    s.id,
    "Version":      "Charm_v" + v.n,
    "_design_path": v.renderPath || v.path || "", // hidden from the cart (leading underscore)
    "_uid":         state.uid || "",
  }, v.renderUrl ? {
    "Your drawing":  v.url,                      // the pair's second half
    "_drawing_path": v.path || "",
  } : {});
  const items = [{
    id: charmVariant.id,
    quantity: orderState.qty,
    properties: Object.assign({}, designProps, {
      "Charm size": ctx.t.sized ? orderState.mm + " mm" : "One size",
    },
    ctx.engraved && $("#engraveInput").value.trim() ? { "Engraving": $("#engraveInput").value.trim() } : {},
    ctx.ext ? { "Chain extender": ctx.heart ? "Yes + tiny heart" : "Yes" } : {}),
  }];
  /* one fee line per custom-designed piece, keyed on Design ID */
  if (feeVariant) items.push({ id: feeVariant.id, quantity: 1, properties: Object.assign({}, designProps) });
  if (ctx.engraved) {
    const eng = variantFor(D.engravingHandle || "custom-charm-engraving", []);
    if (eng) items.push({ id: eng.id, quantity: orderState.qty, properties: Object.assign({}, designProps) });
  }
  if (ctx.heart) {
    const hv = variantFor(D.heartHandle || "chain-extender-tiny-heart", []);
    if (hv) items.push({ id: hv.id, quantity: orderState.qty, properties: Object.assign({}, designProps) });
  }

  const btn = $("#addToCartBtn");
  btn.disabled = true; btn.textContent = "Adding…";
  try {
    const r = await fetch(CART_ADD_URL, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ items }),
    });
    if (!r.ok) { const j = await r.json().catch(() => ({})); throw new Error(j.description || j.message || "Could not add to cart"); }
    logEvent("order", `Added to cart — ${ctx.opt.textContent} · ${METAL_LABELS[orderState.metal]}`);
    s.status = "ordered";
    saveSession(true);
    toast("Added to cart ✦", "gold");
    btn.textContent = "Added ✓ — taking you to the cart";
    setTimeout(() => { window.location.href = "/cart"; }, 900);
  } catch (e) {
    cartError(e.message || "Could not add to cart — please try another option.");
  }
});


/* =============================================================================
   AUTH — two paths, both landing in the same Firebase Auth session so the
   guest's anonymous uid (and therefore every saved design and credit) carries
   across untouched.                                                     (§4c)
   ========================================================================== */
const AUTH_COPY = {
  default:      ["Keep designing","Create your free account","Ten seconds, no card. Your designs, credits and order history live safely in one place."],
  outOfCredits: ["You're out of free designs","Keep going — it's free","Create a free account and we'll add "+CONFIG.signupBonusCredits+" bonus design credits instantly. No card required."],
  saveDesign:   ["Don't lose this","Save your design","A free account keeps every version safe in your studio — on any device."],
  approve:      ["One last step","Approve with an account","Your approved artwork is stored full-resolution and reserved for you. Takes ten seconds."],
};
let authIntent = "default";
function openAuth(intent = "default") {
  authIntent = intent;
  const [eye, title, sub] = AUTH_COPY[intent] || AUTH_COPY.default;
  $("#authEyebrow").textContent = eye;
  $("#authTitle").textContent = title;
  $("#authSub").textContent = sub;
  $("#authBonusTxt").textContent = CONFIG.signupBonusCredits + " bonus design credits";
  $("#authModal").classList.add("is-open");
  mountGoogleButton();
}
function setAccountChip(name, email, provider) {
  state.user = { name, email, provider };
  const slot = $("#authSlot");
  if (slot) slot.innerHTML = `<div class="acct-chip"><span class="acct-chip__avatar">${escapeHtml(String(name || "?")[0].toUpperCase())}</span><span>${escapeHtml(name)}</span></div>`;
  renderUploadAllowance();
}
/* The signup bonus is granted SERVER-SIDE and exactly once per uid, keyed by a
   ledger entry with reason:"signup" — running it twice grants nothing further
   (§10.8 / acceptance test 3). */
async function completeAuth(name, email, provider) {
  setAccountChip(name, email, provider);
  $("#authModal").classList.remove("is-open");
  try {
    const r = await postAuthed("britesAuth", { kind: "wallet_grant_signup" });
    if (r && r.wallet) applyWallet(r.wallet);
  } catch (e) { /* the wallet snapshot will catch up */ }
  renderCredits(true);
  renderUploadAllowance();
  toast(`Welcome, ${escapeHtml(name)} — ${CONFIG.signupBonusCredits} bonus credits added ✦`, "gold");
  saveSession(true);
  if (authIntent === "approve") setTimeout(approveNow, 600);
  if (authIntent === "saveDesign") { ensureSession(); saveSession(true); toast("Design saved to your studio"); }
}

/* ═════════ IN-APP BROWSERS — the phone case Google will not serve ════════
   A shopper who taps our link in Instagram, Facebook, Messenger, Threads,
   TikTok, LINE or WeChat is not in Safari or Chrome. They are in an embedded
   WebView the host app owns, and Google refuses to run OAuth in one: the
   request comes back `403 disallowed_useragent`. That is policy, announced by
   Google and enforced at accounts.google.com, so it defeats the rendered GSI
   button and Firebase's popup and Firebase's redirect equally — there is no
   arrangement of our code that signs this shopper in where they stand.

   What our code CAN do is stop pretending. In a WebView we replace the button
   that will fail with the one action that works — leaving — and we make
   leaving cost one tap:

     · Android: an intent:// URI hands the URL to Chrome directly, with
       S.browser_fallback_url so a phone without Chrome still lands somewhere.
     · iOS: x-safari-https://, the scheme Instagram, Facebook, Messenger and
       Threads honour to punt a URL to Safari.
     · Anything else, and any of the above that silently no-ops: copy the link,
       plus the two taps to the host app's own "open in browser" item.

   And because a different browser means different storage, the escape URL
   carries a single-use handoff code so the guest's uid — every design, every
   credit — arrives on the other side. Guests who never sign in are untouched:
   nothing here gates designing, only the account step.                (§4c) */
const INAPP_APPS = [
  /* Threads before Instagram, Messenger before Facebook: the narrower token
     has to win, because the broader one also matches. */
  { key: "threads",   re: /Barcelona|Threads/i,               name: "Threads",   steps: ["Tap the ⋯ at the top right", "Choose “Open in browser”"] },
  { key: "instagram", re: /Instagram/i,                       name: "Instagram", steps: ["Tap the ⋯ at the top right", "Choose “Open in external browser”"] },
  /* WeChat announces itself as MicroMessenger, which CONTAINS "Messenger" — so
     it is tested first, and Messenger is matched on its real tokens only. */
  { key: "wechat",    re: /MicroMessenger/i,                  name: "WeChat",    steps: ["Tap the ⋯ at the top right", "Choose “Open in Browser”"] },
  { key: "messenger", re: /FB_IAB\/MESSENGER|MessengerForiOS|MessengerLite/i, name: "Messenger", steps: ["Tap the ⋯ at the top right", "Choose “Open in browser”"] },
  { key: "facebook",  re: /FBAN|FBAV|FB_IAB|FB4A|FBIOS/i,     name: "Facebook",  steps: ["Tap the ⋯ at the top right", "Choose “Open in browser”"] },
  { key: "tiktok",    re: /BytedanceWebview|musical_ly|TikTok|Bytedance/i, name: "TikTok", steps: ["Tap the ⋯ at the top right", "Choose “Open in browser”"] },
  { key: "snapchat",  re: /Snapchat/i,                        name: "Snapchat",  steps: ["Tap the ⋯ at the top right", "Choose “Open in browser”"] },
  { key: "pinterest", re: /Pinterest/i,                       name: "Pinterest", steps: ["Tap the ⋯ at the top right", "Choose “Open in browser”"] },
  { key: "linkedin",  re: /LinkedInApp/i,                     name: "LinkedIn",  steps: ["Tap the ⋯ at the top right", "Choose “Open in browser”"] },
  { key: "line",      re: /\bLine\//i,                        name: "LINE",      steps: ["Tap the ⋯ at the bottom right", "Choose “Open in other browser”"] },
  { key: "kakao",     re: /KAKAOTALK/i,                       name: "KakaoTalk", steps: ["Tap the ⋯ at the bottom right", "Choose “Open in other browser”"] },
];

/* A preview switch, so the escape can be seen on a phone that has none of
   these apps installed:

     ?bjinapp=1            the generic panel
     ?bjinapp=instagram    a named one — any key from INAPP_APPS
     &bjos=ios             the iOS wording, previewed from an Android handset

   Nothing reads it unless it is in the URL, and the escape URL drops it again,
   so a preview ENDS in the real experience instead of looping back into
   itself. (With bjos=ios forced on an Android phone the Open button will go
   nowhere, because x-safari- is not a scheme Android knows — that is the
   manual-route fallback doing its job, not a fault.) */
function readInAppForce(searchIn) {
  const q = String(searchIn != null ? searchIn : (typeof location !== "undefined" ? location.search : "") || "");
  const m = /[?&]bjinapp=([A-Za-z0-9_%-]+)/.exec(q);
  if (!m) return null;
  const osm = /[?&]bjos=(ios|android|other)/.exec(q);
  return { app: decodeURIComponent(m[1]).toLowerCase(), os: osm ? osm[1] : null };
}

/* Pure, and takes its inputs as arguments, so the regression pass can assert
   the whole truth table without spinning up eleven browser contexts. */
function detectInApp(uaIn, navIn, forceIn) {
  const nav = navIn || (typeof navigator !== "undefined" ? navigator : {});
  const ua  = String(uaIn != null ? uaIn : (nav.userAgent || ""));
  const ios = /iPad|iPhone|iPod/.test(ua);
  const android = /Android/.test(ua);
  let os = ios ? "ios" : android ? "android" : "other";
  let app = null;
  for (let i = 0; i < INAPP_APPS.length; i++) {
    if (INAPP_APPS[i].re.test(ua)) { app = INAPP_APPS[i]; break; }
  }
  let generic = false;
  if (!app) {
    if (android) {
      /* Android WebViews carry the "wv" token. The older ones give themselves
         away instead with a Version/x.y token that real Chrome dropped years
         ago and has never brought back. */
      generic = /;\s*wv[);]/.test(ua) || (/Chrome\//.test(ua) && /\bVersion\/\d/.test(ua));
    } else if (ios) {
      /* WKWebView omits the Safari token altogether. So does a page running
         from the home screen, which is a REAL browser and must not be caught
         here — hence the navigator.standalone guard. Third-party browsers all
         announce themselves and keep the Safari token besides. */
      generic = !/Safari/.test(ua) && !/CriOS|FxiOS|EdgiOS|OPiOS|Chrome/i.test(ua) && nav.standalone !== true;
    }
  }
  /* The preview switch, applied last so it can only ever ADD the panel — a
     shopper without the parameter in their URL cannot reach this branch. */
  const force = forceIn === undefined ? null : forceIn;
  if (force) {
    const hit = INAPP_APPS.filter((a) => a.key === force.app)[0];
    if (hit) app = hit; else generic = true;
    if (force.os) os = force.os;
  }

  const inApp = !!app || generic;
  return {
    inApp, os, preview: !!force,
    key:     app ? app.key : (inApp ? "webview" : ""),
    name:    app ? app.name : (inApp ? "this app" : ""),
    steps:   app ? app.steps : ["Open this browser's menu", "Choose “Open in browser”"],
    browser: os === "ios" ? "Safari" : os === "android" ? "Chrome" : "your browser",
  };
}
let _inApp = null;
function inAppInfo() {
  if (!_inApp) _inApp = detectInApp(undefined, undefined, readInAppForce());
  return _inApp;
}

/* The URL a shopper should end up on: this page, minus every parameter this
   feature owns. Dropping bjh stops a spent code being handed on; dropping
   bjinapp/bjos means a PREVIEW lands in the ordinary studio, which is the
   whole point of previewing. */
function cleanStudioUrl() {
  try {
    const u = new URL(location.href);
    ["bjh", "bja", "bjinapp", "bjos"].forEach((k) => u.searchParams.delete(k));
    u.hash = "";
    return u.toString();
  } catch (e) {
    return location.href.split("#")[0].split("?")[0];
  }
}

/* The URL that leaves. Android's intent:// is the only one that is a genuine
   hand-off rather than a request the host app may ignore, so it carries a
   fallback: a phone with Chrome uninstalled still gets the https URL. */
function inAppOpenUrl(url, os) {
  const u = String(url);
  if (os === "android") {
    return "intent://" + u.replace(/^https?:\/\//, "") +
           "#Intent;scheme=https;package=com.android.chrome;" +
           "S.browser_fallback_url=" + encodeURIComponent(u) + ";end";
  }
  if (os === "ios") return "x-safari-" + u;
  return u;
}

/* Minted eagerly, the moment we know the shopper is in a WebView, because the
   click that uses it may be a clipboard write — and a clipboard write that
   awaits a network round trip has left its user gesture behind and is refused.
   Codes live five minutes server-side; this cache retires at four. */
let _escapeUrl = null, _escapeMint = null;

/* The bar is primed at first paint, which is BEFORE anonymous auth has landed
   — and a mint with no ID token is a guaranteed 401. Wait briefly for the uid
   rather than burning the one prime we get. */
async function awaitUid(ms) {
  const t0 = Date.now();
  for (;;) {
    try { await bootFirebase(); } catch (e) { return false; }
    if (auth && auth.currentUser) return true;
    if (Date.now() - t0 >= ms) return false;
    await new Promise((r) => setTimeout(r, 150));
  }
}

function primeEscapeUrl() {
  if (_escapeMint) return _escapeMint;
  _escapeMint = (async () => {
    const plain = cleanStudioUrl();
    try {
      await awaitUid(6000);
      const r = await postAuthed("britesAuth", { kind: "handoff_mint" });
      if (r && r.ok && r.code) {
        /* A query parameter, not a fragment — and that choice is forced.
           Android's intent:// URI syntax CLAIMS the fragment for its own
           ";scheme=…;package=…;end" block, so a code parked there arrives
           mangled or not at all, on exactly the platform where the intent
           hand-off is the one route that genuinely works. A fragment would
           keep the code out of Shopify's access logs, which is worth
           something — but not worth breaking Android. The protections that
           actually carry the weight are unchanged either way: 32 random
           bytes, five minutes, single use server-side, and claimHandoff
           strips it from the address bar on arrival. This is the same
           bargain an OAuth authorization code makes, for the same reasons. */
        _escapeUrl = plain + (plain.indexOf("?") >= 0 ? "&" : "?") + "bjh=" + encodeURIComponent(r.code);
        _escapeMint = null;
        setTimeout(() => { _escapeUrl = null; }, 4 * 60 * 1000);   // server TTL is 5
        return _escapeUrl;
      }
    } catch (e) {
      /* The handoff is a bonus. Losing it costs the draft, not the sign-in,
         and a shopper stranded in a WebView is worse off than one who
         restarts. So: warn, hand back the plain URL, and cache NOTHING —
         caching the fallback would mean the retry never happens. */
      console.warn("[studio] handoff mint:", (e && e.message) || e);
    }
    _escapeUrl = null;
    _escapeMint = null;                       // retried on the next interaction
    return plain;
  })();
  return _escapeMint;
}
async function escapeUrl() {
  return _escapeUrl || (await primeEscapeUrl());
}

/* The other end of the handoff: claimed BEFORE anonymous sign-in, so the
   shopper lands on their own uid instead of a fresh empty one. Single-use and
   short-lived server-side; the fragment is stripped from the address bar
   either way, so a back-button or a shared screenshot re-plays nothing. */
/* The modal is now opened at FIRST PAINT, before the handoff has been claimed
   — which means a fast tap on Continue with Google could arrive while
   auth.currentUser is still the throwaway anonymous user, link Google to THAT
   uid, and quietly strand the design the shopper just carried across. This
   gate closes that window. It is created synchronously, from the URL alone, so
   there is no instant in which a click could beat it into existence; it
   resolves the moment the claim settles either way, and gives up after eight
   seconds so a hung claim can never wedge sign-in. */
let _handoffSettle = null;
const _handoffReady = /[?&#]bjh=/.test((typeof location !== "undefined" ? location.search + location.hash : ""))
  ? new Promise((res) => {
      _handoffSettle = res;
      setTimeout(() => res(false), 8000);
    })
  : Promise.resolve(false);

async function claimHandoff() {
  /* Query first, because that is what we mint; the fragment is still read so a
     link copied from an older build is not a dead end. */
  const m = /[?&]bjh=([A-Za-z0-9_%-]+)/.exec(location.search || "") ||
            /(?:^|[#&])bjh=([A-Za-z0-9_%-]+)/.exec(location.hash || "");
  if (!m) return false;
  const code = decodeURIComponent(m[1]);
  /* Out of the address bar immediately: a back-button, a bookmark or a
     screenshot must not carry a live code around after it has been spent. */
  try {
    const q = (location.search || "").replace(/([?&])bjh=[^&]*(&|$)/, "$1").replace(/[?&]$/, "");
    history.replaceState(null, "", location.pathname + q);
  } catch (e) {}
  try {
    const r = await postJson("britesAuth", { kind: "handoff_claim", code });
    if (!r || !r.token) return false;
    await auth.signInWithCustomToken(r.token);
    toast("Picked up where you left off — your designs came with you ✦", "gold");
    return true;
  } catch (e) {
    console.warn("[studio] handoff claim:", (e && e.message) || e);
    return false;
  } finally {
    if (_handoffSettle) _handoffSettle(true);
  }
}

/* Read once, and strip immediately: a reopened modal is a welcome, but a modal
   that reopens on every refresh for the rest of the visit is a trap. Unknown
   values collapse to the default copy rather than throwing. */
function takeAuthIntent() {
  const m = /[?&]bja=([A-Za-z]+)/.exec(location.search || "");
  if (!m) return "";
  try {
    const q = (location.search || "").replace(/([?&])bja=[^&]*(&|$)/, "$1").replace(/[?&]$/, "");
    history.replaceState(null, "", location.pathname + q + (location.hash || ""));
  } catch (e) {}
  return AUTH_COPY[m[1]] ? m[1] : "default";
}

/* Copy, with the two fallbacks that matter in a WebView: execCommand for the
   ones without the async clipboard, and — when even that is refused — showing
   the link in a selected field the shopper can long-press. */
async function copyEscapeLink() {
  const url = await escapeUrl();
  let copied = false;
  try {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      await navigator.clipboard.writeText(url); copied = true;
    }
  } catch (e) { /* fall through */ }
  if (!copied) {
    try {
      const ta = document.createElement("textarea");
      ta.value = url;
      ta.setAttribute("readonly", "");
      ta.style.cssText = "position:fixed;top:-1000px;opacity:0";
      document.body.appendChild(ta);
      ta.select(); ta.setSelectionRange(0, url.length);
      copied = document.execCommand("copy");
      document.body.removeChild(ta);
    } catch (e) { /* fall through */ }
  }
  const field = $("#inappLink");
  if (copied) {
    toast("Link copied — paste it into " + inAppInfo().browser);
  } else if (field) {
    field.hidden = false;
    field.value = url;
    try { field.focus(); field.select(); field.setSelectionRange(0, url.length); } catch (e) {}
    toast("Press and hold the link to copy it", "err");
  }
  return copied;
}

let _inAppOverride = false, _inAppWired = false;
function wireInApp() {
  if (_inAppWired) return;
  _inAppWired = true;
  const info = inAppInfo();
  /* `intent` is why they were leaving. A shopper who tapped Sign in, then
     Open in Chrome, has said what they want TWICE — landing them on the studio
     with the modal shut makes them say it a third time, which is where the
     journey quietly leaks. It rides across as ?bja= and reopens the modal on
     arrival, with the same wording it had before they left. Escapes from the
     bottom bar carry nothing, because that shopper was not signing in. */
  const go = async (btn, intent) => {
    if (btn) { btn.disabled = true; btn.classList.add("is-working"); }
    let url = await escapeUrl();
    if (btn) { btn.disabled = false; btn.classList.remove("is-working"); }
    if (intent) url += (url.indexOf("?") >= 0 ? "&" : "?") + "bja=" + encodeURIComponent(intent);
    const target = inAppOpenUrl(url, info.os);
    /* Recorded before the hand-off, because after it this page may be gone. It
       is what the regression pass asserts, and it is the one thing worth
       reading off a phone when an escape lands somewhere unexpected. */
    window.__lastEscape = { url, target, os: info.os, app: info.key };
    /* If the host app swallows the scheme nothing happens at all — no error,
       no navigation — so after a beat we surface the manual route rather than
       leaving the shopper tapping a button that appears dead. */
    const panel = $("#authInapp");
    if (panel) panel.classList.add("is-armed");
    try { window.location.href = target; }
    catch (e) { if (panel) panel.classList.add("is-stuck"); }
    setTimeout(() => {
      if (!document.hidden && panel) panel.classList.add("is-stuck");
    }, 1400);
  };
  const wire = (id, fn) => { const el = $("#" + id); if (el) el.addEventListener("click", fn); };
  wire("inappGo",    function () { go(this, authIntent || "default"); });
  wire("inappBarGo", function () { go(this, null); });
  wire("inappCopy",  () => { copyEscapeLink(); });
  wire("inappBarX",  () => { dismissInAppBar(); });
  wire("inappAnyway", () => {
    /* Detection is a user-agent guess, and a wrong guess must never be a wall.
       This is the escape from the escape hatch: it puts the real button back
       for the rest of the visit. */
    _inAppOverride = true;
    ROOT.classList.add("bj-inapp-override");
    mountGoogleButton();
    toast("Google may refuse this browser — if it does, use Open in " + info.browser, "err");
  });
}

function primeInAppPanel() {
  const info = inAppInfo();
  wireInApp();
  primeEscapeUrl();
  const set = (id, txt) => { const el = $("#" + id); if (el) el.textContent = txt; };
  const inWhat = info.key === "webview" ? "this in-app browser" : info.name + "'s browser";
  set("inappTitle", "Google won't sign you in here");
  set("inappSub", "You're in " + inWhat + ", and Google blocks sign-in inside those. " +
                  "Open this page in " + info.browser + " — your design and credits come with you.");
  set("inappGoTxt", "Open in " + info.browser);
  set("inappManualHead", "Or, from " + (info.key === "webview" ? "this browser" : info.name));
  const steps = $("#inappSteps");
  if (steps && !steps.dataset.filled) {
    steps.dataset.filled = "1";
    steps.innerHTML = info.steps.map((s) => "<li>" + escapeHtml(s) + "</li>").join("");
  }
}

function dismissInAppBar() {
  const bar = $("#inappBar");
  if (!bar) return;
  bar.classList.remove("is-in");
  setTimeout(() => { bar.hidden = true; }, 320);
  try { sessionStorage.setItem("bjInappBar", "1"); } catch (e) {}
}

/* One bar, once per visit, offered EARLY — the cheapest way to save a design
   from being stranded in an app's browser is for it never to be started
   there. It is dismissible, it never blocks anything, and it does not return. */
function applyInAppUi() {
  const info = inAppInfo();
  if (ROOT.classList.contains("bj-inapp") !== info.inApp) ROOT.classList.toggle("bj-inapp", info.inApp);
  if (!info.inApp) return;
  wireInApp();
  primeEscapeUrl();
  let dismissed = false;
  try { dismissed = sessionStorage.getItem("bjInappBar") === "1"; } catch (e) {}
  if (dismissed) return;
  const bar = $("#inappBar");
  if (!bar) return;
  const head = $("#inappBarHead"), sub = $("#inappBarSub");
  if (head) head.textContent = info.key === "webview"
    ? "You're in an in-app browser" : "You're in " + info.name + "'s browser";
  if (sub) sub.textContent = "Sign-in and saving work properly in " + info.browser + ".";
  bar.hidden = false;
  requestAnimationFrame(() => requestAnimationFrame(() => bar.classList.add("is-in")));
}

/* ═══════════════════ GOOGLE SIGN-IN ══════════════════════════════════════
   Google Identity Services with FedCM (no third-party cookies), rendered
   button plus One Tap, then GoogleAuthProvider.credential(...) exchanged
   through linkWithCredential on the anonymous user — falling back to
   signInWithCredential + a server-side merge when the Google account already
   exists (auth/credential-already-in-use). Google accounts arrive
   email_verified, so they skip the code step entirely. */
let _gisReady = null;
function loadGis() {
  if (_gisReady) return _gisReady;
  _gisReady = loadScript("https://accounts.google.com/gsi/client").then(() => {
    if (!window.google || !GOOGLE_CLIENT) throw new Error("Google sign-in is not configured");
    window.google.accounts.id.initialize({
      client_id: GOOGLE_CLIENT,
      use_fedcm_for_prompt: true,
      auto_select: false,
      cancel_on_tap_outside: true,
      callback: onGoogleCredential,
      /* An origin that is not on the OAuth client's Authorized JavaScript
         origins list does NOT throw — GSI renders a button that 403s and sends
         the shopper to Google's "Access blocked" page. It reports it here
         instead, so this is the only place we can catch it. */
      error_callback: (err) => {
        const t = err && err.type;
        if (t === "unregistered_origin" || t === "unknown") useGooglePopupFallback(t);
      },
    });
  });
  return _gisReady;
}
/* ── When Google will not accept this origin ───────────────────────────────
   Google Identity Services renders its button in an iframe served by Google,
   and Google will only serve it to an origin listed under the OAuth client's
   Authorized JavaScript origins. If the storefront is not on that list the
   iframe 403s, GSI reports `unregistered_origin`, and every click lands the
   shopper on Google's "Access blocked" page.

   Firebase's own popup flow has no such requirement: the OAuth exchange runs
   on the Firebase auth handler's origin (<project>.firebaseapp.com), which
   Firebase registers when you enable the provider — the storefront's origin
   never has to be on that list at all. So an origin Google refuses does not
   cost the shopper Google sign-in; it costs the rendered button, and our own
   branded button takes over using the popup instead. */
let _googlePopupMode = false;
function useGooglePopupFallback(reason) {
  if (_googlePopupMode) return;
  _googlePopupMode = true;
  const host = $("#googleMount"), btn = $("#googleBtn");
  if (host) { host.hidden = true; host.innerHTML = ""; host.dataset.mounted = ""; }
  if (btn) btn.hidden = false;                     // our own Continue with Google
  /* One Tap will keep retrying and keep aborting against a refused origin */
  try { window.google && window.google.accounts.id.cancel(); } catch (e) {}
  console.warn("[studio] Google's rendered button is unavailable (" + reason +
               ") — using Firebase popup sign-in instead. To restore the rendered " +
               "button, add " + location.origin + " to the OAuth client's " +
               "Authorized JavaScript origins.");
}

/* The popup path. Same rules as the credential path: link onto the anonymous
   user so the uid — and every design and credit hanging off it — survives, and
   fall back to a server-side merge when the Google account already exists. */
/* "Guest" is not the same question as "anonymous". A shopper who came through
   the in-app handoff is signed in with a CUSTOM token: same uid, same designs,
   same credits — but isAnonymous is false, because the provider is "custom".
   Asking isAnonymous would send them down the signInWithPopup branch, which
   mints a NEW uid and orphans everything they just carried across. What we
   actually need to know is whether this user has a real identity provider
   attached yet; if not, we link onto them and the uid survives. */
function isGuestUser(u) {
  return !!u && (u.isAnonymous || !(u.providerData && u.providerData.length));
}

/* ── When the Google account ALREADY EXISTS ────────────────────────────────
   linkWithPopup is the happy path: the anonymous uid gains a Google identity
   and nothing moves. But a shopper whose Google account is already in
   Firebase — anyone signing in for the SECOND time from a new device or a
   cleared browser — cannot link; Firebase throws credential-already-in-use
   and the fallback signInWithCredential SWITCHES this browser onto the
   existing uid. Every design made as a guest is still filed under the old
   anonymous uid at that point, and from then on the ownership checks do
   their job perfectly against the shopper: generation 403s, saves are
   refused by the rules, and the studio looks broken the moment they log in.

   So the guest's docs have to be re-filed server-side, and the server must
   demand PROOF that this caller really was that guest — otherwise
   "merge uid X into me" is a way to steal any guest's designs by guessing
   uids. The proof is the guest's own ID token, and it must be minted BEFORE
   the popup: after signInWithCredential the guest session is gone from this
   browser, and nothing can vouch for it after the fact. */
async function mergeGuestInto(guestUid, guestToken) {
  if (!guestUid || !guestToken) return true;
  if (auth.currentUser && auth.currentUser.uid === guestUid) return true;  // the link held — nothing moved
  try {
    const r = await postAuthed("britesAuth", { kind: "merge_guest", guestToken });
    if (r && r.ok) return true;
  } catch (e) {
    console.warn("[studio] guest merge:", (e && (e.message || e.status)) || e);
  }
  /* The merge failed, so the docs on the old uid can no longer be read or
     written by the account this browser now holds. Dragging the active
     session forward would 403 every save and every generation — drop it, let
     the sessions listener load what this account actually owns, and say so. */
  if (state.activeSessionId) {
    state.activeSessionId = null;
    state.sessions = [];
  }
  toast("Signed in — but the designs from your guest visit couldn't be carried over", "err");
  return false;
}

async function googleSignInViaPopup() {
  await bootFirebase();
  /* If a handoff is in flight, let it land first — linking onto the wrong uid
     is the one failure here that loses work rather than just time. */
  await _handoffReady;
  const GoogleAuthProvider = window.firebase.auth.GoogleAuthProvider;
  const provider = new GoogleAuthProvider();
  if (provider.setCustomParameters) provider.setCustomParameters({ prompt: "select_account" });
  const guest = isGuestUser(auth.currentUser) ? auth.currentUser : null;
  const guestUid = guest && guest.uid;
  let guestToken = "";
  if (guest) { try { guestToken = await guest.getIdToken(); } catch (e) {} }
  try {
    if (guest) {
      await auth.currentUser.linkWithPopup(provider);      // same uid — work survives
    } else {
      await auth.signInWithPopup(provider);
    }
  } catch (e) {
    if (e.code === "auth/credential-already-in-use" || e.code === "auth/email-already-in-use") {
      const cred = GoogleAuthProvider.credentialFromError ? GoogleAuthProvider.credentialFromError(e) : null;
      if (!cred) throw e;
      await auth.signInWithCredential(cred);
      await mergeGuestInto(guestUid, guestToken);
    } else throw e;
  }
  const u = auth.currentUser;
  await completeAuth(u.displayName || (u.email || "there").split("@")[0], u.email, "google");
}

/* ── Which Google button the shopper sees ──────────────────────────────────
   Two independent paths, and the ORDER matters:

     · our own button → Firebase's popup. Works from any origin, because the
       OAuth exchange happens on <project>.firebaseapp.com, not here.
     · Google's rendered button → the credential callback. Prettier, brings
       One Tap with it, and only works if this origin is on the OAuth client's
       Authorized JavaScript origins list.

   The one that always works is the DEFAULT, on screen from the moment the
   modal opens. Google's is an upgrade that has to earn its place: it is
   swapped in only after its iframe has demonstrably painted. Earlier builds
   had this the other way round — Google's button by default, ours revealed
   only if something reported a failure — which meant any failure GSI did not
   report left the shopper with a button that could not work. */
async function mountGoogleButton() {
  /* Popup only, deliberately. The rendered GSI button cannot work until this
     origin is on the OAuth client's Authorized JavaScript origins list, and
     ATTEMPTING it costs three red 403s in the console on every open of this
     modal — noise that reads as breakage. So GIS is not even loaded. Our own
     button drives Firebase's popup, whose OAuth exchange happens on
     gokudatabase.firebaseapp.com and needs no origin registration here.
     When the origins list is fixed, set cfg/data-google-rendered="1" to try
     the rendered button + One Tap again.

     Ahead of all of that: inside an app's embedded browser NO Google path
     works, so the button is replaced outright by the way out. */
  const blocked = inAppInfo().inApp && !_inAppOverride;
  const panel = $("#authInapp");
  if (panel) panel.hidden = !blocked;
  if (blocked) primeInAppPanel();
  if ($("#googleBtn")) $("#googleBtn").hidden = blocked;
  const host = $("#googleMount");
  if (host) host.hidden = true;
  if (blocked) return;
  if ((D.googleRendered || "") !== "1") return;
  if (!GOOGLE_CLIENT) return;
  try {
    await loadGis();
    if (host && !host.dataset.mounted) {
      host.dataset.mounted = "1";
      host.hidden = false;
      window.google.accounts.id.renderButton(host, {
        theme: "outline", size: "large", text: "continue_with",
        shape: "rectangular", logo_alignment: "left", width: 360,
      });
      setTimeout(() => {
        const painted = host.querySelector("iframe") && host.offsetHeight >= 20;
        if (painted) {
          $("#googleBtn").hidden = true;
          try { window.google.accounts.id.prompt(); } catch (e) {}
        } else {
          host.hidden = true;
          try { window.google.accounts.id.cancel(); } catch (e) {}
        }
      }, 1600);
    }
  } catch (e) { if (host) host.hidden = true; }
}
async function onGoogleCredential(res) {
  if (!res || !res.credential) return;
  try {
    await bootFirebase();
    await _handoffReady;                        // same reason as the popup path
    const provider = window.firebase.auth.GoogleAuthProvider;
    const cred = provider.credential(res.credential);
    const guest = isGuestUser(auth.currentUser) ? auth.currentUser : null;
    const guestUid = guest && guest.uid;
    let guestToken = "";
    if (guest) { try { guestToken = await guest.getIdToken(); } catch (e) {} }
    try {
      if (guest) {
        await auth.currentUser.linkWithCredential(cred);   // same uid — work survives
      } else {
        await auth.signInWithCredential(cred);
      }
    } catch (e) {
      if (e.code === "auth/credential-already-in-use") {
        const fromErr = provider.credentialFromError ? provider.credentialFromError(e) : cred;
        await auth.signInWithCredential(fromErr || cred);
        /* same hole as the popup path: the account existed, this browser just
           switched uids, and the guest's docs have to follow */
        await mergeGuestInto(guestUid, guestToken);
      } else throw e;
    }
    const u = auth.currentUser;
    await completeAuth(u.displayName || (u.email || "there").split("@")[0], u.email, "google");
  } catch (e) {
    toast("Google sign-in didn't complete — try the email option", "err");
  }
}
/* Fallback button for contexts where the GIS script is blocked: the server
   path (britesAuth google_verify) still works from a manual prompt. */
$("#googleBtn").addEventListener("click", async () => {
  /* Always the popup. This button exists BECAUSE the rendered one may not be
     available, so routing it through GSI would reintroduce the failure it is
     here to cover. */
  try { await googleSignInViaPopup(); }
  catch (e) {
    /* closing the window or double-clicking is not an error worth a toast */
    if (e && (e.code === "auth/popup-closed-by-user" || e.code === "auth/cancelled-popup-request")) return;
    if (e && e.code === "auth/popup-blocked") {
      toast("Your browser blocked the Google window — allow pop-ups, or use your email", "err");
      return;
    }
    console.warn("[studio] Google popup sign-in:", (e && e.message) || e);
    toast("Google sign-in didn't complete — try the email option", "err");
  }
});

/* ═════════════════ EMAIL SIGN-UP → 6-DIGIT VERIFICATION ══════════════════
   britesAuth kind:"send_code" issues a crypto-strong code, stored only as a
   peppered hash (10-minute TTL, 5 attempts, 60s resend cooldown, per-address
   and per-IP rate limits) and delivered over the existing Gmail pipeline.
   kind:"verify_code" returns a Firebase custom token; signInWithCustomToken()
   upgrades the anonymous session IN PLACE, so the uid — and every design and
   credit hanging off it — is unchanged. */
let _pendingEmail = "", _ttlTimer = null, _resendTimer = null;
const codeBoxes = () => $$("#codeRow .code-box");

async function requestCode(email) {
  const r = await postJson("britesAuth", {
    kind: "send_code",
    email,
    uid: state.uid || null,
    requestedFrom: location.host,
  });
  return r;
}
function openVerifyStage(email) {
  _pendingEmail = email;
  $("#verifyEmail").textContent = email;
  $(".auth-box").classList.add("is-verifying");
  $("#authTitle").textContent = "Verify your email";
  $("#authSub").textContent = "One quick step to keep your designs safe.";
  $("#verifyStage").classList.add("is-open");
  codeBoxes().forEach((b) => { b.value = ""; b.classList.remove("is-filled"); });
  setTimeout(() => codeBoxes()[0].focus(), 420);
  startTtl(10 * 60);
  startResendCooldown(60);
  setMsg("", "");
}
function closeVerifyStage() {
  $("#verifyStage").classList.remove("is-open");
  $(".auth-box").classList.remove("is-verifying");
  clearInterval(_ttlTimer); clearInterval(_resendTimer);
  const [eye, title, sub] = AUTH_COPY[authIntent] || AUTH_COPY.default;
  $("#authTitle").textContent = title; $("#authSub").textContent = sub;
}
function setMsg(text, cls) { const m = $("#verifyMsg"); m.textContent = text; m.className = "verify-msg " + (cls || ""); }
function startTtl(seconds) {
  clearInterval(_ttlTimer);
  let left = seconds;
  const tick = () => {
    const m = Math.floor(left / 60), s = String(left % 60).padStart(2, "0");
    $("#verifyTtl").textContent = `${m}:${s}`;
    if (left-- <= 0) { clearInterval(_ttlTimer); setMsg("That code expired — send a fresh one.", "err"); }
  };
  tick(); _ttlTimer = setInterval(tick, 1000);
}
function startResendCooldown(seconds) {
  clearInterval(_resendTimer);
  let left = seconds;
  const btn = $("#resendBtn");
  const tick = () => {
    btn.disabled = left > 0;
    btn.textContent = left > 0 ? `Resend in ${left}s` : "Resend code";
    if (left-- <= 0) clearInterval(_resendTimer);
  };
  tick(); _resendTimer = setInterval(tick, 1000);
}
/* one-box-per-digit with paste, auto-advance, backspace and arrow support
                                                        [prototype, verbatim] */
$("#codeRow").addEventListener("input", (e) => {
  const box = e.target; if (!box.classList.contains("code-box")) return;
  box.value = box.value.replace(/\D/g, "").slice(0, 1);
  box.classList.toggle("is-filled", !!box.value);
  $("#codeRow").classList.remove("is-error");
  const boxes = codeBoxes(), i = boxes.indexOf(box);
  if (box.value && i < boxes.length - 1) boxes[i + 1].focus();
  if (boxes.every((b) => b.value)) submitCode();
});
$("#codeRow").addEventListener("keydown", (e) => {
  const boxes = codeBoxes(), i = boxes.indexOf(e.target);
  if (i < 0) return;
  if (e.key === "Backspace" && !e.target.value && i > 0) { boxes[i - 1].focus(); boxes[i - 1].value = ""; boxes[i - 1].classList.remove("is-filled"); }
  if (e.key === "ArrowLeft" && i > 0) boxes[i - 1].focus();
  if (e.key === "ArrowRight" && i < boxes.length - 1) boxes[i + 1].focus();
  if (e.key === "Enter") submitCode();
});
$("#codeRow").addEventListener("paste", (e) => {
  const digits = (e.clipboardData.getData("text") || "").replace(/\D/g, "").slice(0, 6);
  if (!digits) return;
  e.preventDefault();
  const boxes = codeBoxes();
  boxes.forEach((b, i) => { b.value = digits[i] || ""; b.classList.toggle("is-filled", !!b.value); });
  (boxes[Math.min(digits.length, 5)] || boxes[5]).focus();
  if (digits.length === 6) submitCode();
});
let _verifying = false;
async function submitCode() {
  if (_verifying) return;
  const code = codeBoxes().map((b) => b.value).join("");
  if (code.length !== 6) { setMsg("Enter all six digits", "err"); return; }
  _verifying = true;
  setMsg("Checking your code…", "info");
  try {
    const r = await postJson("britesAuth", { kind: "verify_code", email: _pendingEmail, code, uid: state.uid || null });
    if (!r || !r.ok || !r.customToken) throw Object.assign(new Error("bad_code"), { payload: r });
    $("#codeRow").classList.add("is-ok");
    setMsg("✓ Email verified", "ok");
    await bootFirebase();
    await auth.signInWithCustomToken(r.customToken);     // uid unchanged
    await new Promise((res) => setTimeout(res, 500));
    clearInterval(_ttlTimer); clearInterval(_resendTimer);
    $("#codeRow").classList.remove("is-ok");
    closeVerifyStage();
    await completeAuth(_pendingEmail.split("@")[0], _pendingEmail, "email");
  } catch (e) {
    const p = e.payload || (e.status ? e.payload : null) || {};
    const why = p.error || (e.payload && e.payload.error) || "";
    const left = p.attemptsLeft;
    $("#codeRow").classList.add("is-error");
    setMsg(
      why === "code_expired" ? "That code expired — send a fresh one."
      : why === "too_many_attempts" ? "Too many tries — send a fresh code."
      : why === "code_used" ? "That code was already used — send a fresh one."
      : `That code doesn't match — check your inbox and try again.${left != null ? ` (${left} left)` : ""}`,
      "err");
    codeBoxes().forEach((b) => { b.value = ""; b.classList.remove("is-filled"); });
    codeBoxes()[0].focus();
  }
  _verifying = false;
}
$("#verifyGoBtn").addEventListener("click", submitCode);
$("#resendBtn").addEventListener("click", async () => {
  startResendCooldown(60);
  try {
    await requestCode(_pendingEmail);
    startTtl(10 * 60);
    codeBoxes().forEach((b) => { b.value = ""; b.classList.remove("is-filled"); });
    codeBoxes()[0].focus();
    setMsg("A fresh code is on its way.", "info");
  } catch (e) {
    setMsg("Couldn't send another code just yet — give it a moment.", "err");
  }
});
$("#changeEmailBtn").addEventListener("click", () => { closeVerifyStage(); $("#authEmail").focus(); });

$("#authGoBtn").addEventListener("click", async () => {
  const em = $("#authEmail").value.trim();
  if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(em)) { toast("Enter a valid email address", "err"); return; }
  const btn = $("#authGoBtn");
  btn.disabled = true; btn.textContent = "Sending…";
  try {
    await requestCode(em);
    openVerifyStage(em);
  } catch (e) {
    /* the server classifies its failures; say the true thing for each */
    const code = (e && e.message) || "";
    if (e.status === 429) {
      /* Two different walls with two different heights: a 60-second resend
         cooldown, and an hourly cap on the address. Calling both "try again in
         a minute" sends someone back to a door that will not open for an hour. */
      const ms = ((e.payload || {}).retryInMs) || 60000;
      const mins = Math.ceil(ms / 60000), secs = Math.ceil(ms / 1000);
      if (code === "cooldown") {
        toast(`A code was just sent — check your inbox, or try again in ${secs}s`, "err");
      } else {
        toast(`Too many codes for this address — try again in ${mins} min, or use Continue with Google`, "err");
      }
    } else if (code === "email_not_configured") {
      toast("Email sign-in isn't set up yet — use Continue with Google for now", "err");
    } else if (code === "email_send_failed") {
      toast("Our email service hiccuped — try again in a moment, or use Google", "err");
    } else {
      toast("We couldn't send that code — please try again", "err");
    }
    /* The server returns the provider's own rejection text and its build id.
       Printing them is the whole point of collecting them — an earlier build
       logged only the class, which meant the answer was sitting in the network
       tab where nobody was looking. */
    const p = (e && e.payload) || {};
    console.warn("[studio] send_code failed:", e.status, code,
                 "\n  detail:", p.detail || "(none returned)",
                 "\n  function build:", p.fn || "(not stamped — old britesAuth deployed)");
  }
  btn.disabled = false; btn.textContent = "Email me a code";
});
$("#signInBtn") && $("#signInBtn").addEventListener("click", () => openAuth("default"));

/* =============================================================================
   PRICING — packs and memberships rendered from CONFIG. Every purchase goes
   through the existing Shopify checkout (decision 2): the buy button is a cart
   permalink carrying studio_uid as a cart attribute, which survives checkout
   and lands in order.note_attributes, where shopifyOrderWebhook grants the
   credits transactionally and idempotently on the order id. Credits are NEVER
   added client-side.
   ========================================================================== */
function tierCard(t, kind) {
  return `<div class="tier${t.featured ? " is-featured" : ""}">
    ${t.flag ? `<span class="tier__flag">${escapeHtml(t.flag)}</span>` : ""}
    <h3>${escapeHtml(t.name)}</h3>
    <div class="tier__tag">${t.credits} design credits${kind === "plan" ? " / month" : ""}</div>
    <div class="tier__price">${fmt(t.price)}${kind === "plan" ? "<small> /mo</small>" : ""}</div>
    <div class="tier__per">${escapeHtml(t.per)}</div>
    <ul class="tier__feats">${t.feats.map((f) => `<li>${escapeHtml(f)}</li>`).join("")}</ul>
    <button class="btn${t.featured ? " btn--gold" : ""}" data-buy="${attrText(t.id)}" data-kind="${kind}" type="button">${kind === "plan" ? "Start membership" : "Buy pack"}</button>
  </div>`;
}
function renderPricing() {
  $("#pricingPacks").innerHTML = CONFIG.packs.map((p) => tierCard(p, "pack")).join("");
  $("#pricingPlans").innerHTML = CONFIG.plans.map((p) => tierCard(p, "plan")).join("");
  $$("[data-buy]").forEach((b) => b.addEventListener("click", () => buyTier(b.dataset.buy, b.dataset.kind)));
}
$$(".pricing-tab").forEach((t) => t.addEventListener("click", () => {
  $$(".pricing-tab").forEach((x) => x.classList.remove("is-active"));
  t.classList.add("is-active");
  $("#pricingPacks").style.display = t.dataset.ptab === "packs" ? "grid" : "none";
  $("#pricingPlans").style.display = t.dataset.ptab === "plans" ? "grid" : "none";
}));
function buyTier(id, kind) {
  const t = (kind === "pack" ? CONFIG.packs : CONFIG.plans).find((x) => x.id === id);
  if (!t) return;
  if (!state.uid) { toast("One moment — still setting up your studio", "err"); return; }
  const variant = variantFor(t.handle, []);
  if (!variant) {
    toast(kind === "plan"
      ? "Memberships open shortly — design packs are available now"
      : "That pack isn't available right now — please try another", "err");
    return;
  }
  saveSession(true);
  const ret = encodeURIComponent(STUDIO_PAGE);
  location.href = `/cart/${variant.id}:1?attributes[studio_uid]=${encodeURIComponent(state.uid)}&return_to=${ret}`;
}
function openPricing(context = "default") {
  if (context === "out") {
    $("#pricingEyebrow").textContent = "You're out of design credits";
    $("#pricingTitle").textContent = "Keep the ideas coming";
  } else {
    $("#pricingEyebrow").textContent = "Design credits";
    $("#pricingTitle").textContent = "Pricing";
  }
  $("#pricingModal").classList.add("is-open");
}
$("#getMoreLink").addEventListener("click", (e) => { e.preventDefault(); openPricing("out"); });
$("#creditPill").addEventListener("click", () => openPricing("default"));
$("#pricingBtn").addEventListener("click", () => openPricing("default"));

/* =============================================================================
   DESIGN FILES IN — .PSD, .AI, .PDF, .SVG, .EPS

   A charm often starts life in Illustrator or Photoshop, and the worst thing
   this studio could do with such a file is flatten it to a picture. Both
   formats are TREES — layers, groups, paths — and so is this studio's own
   layer model. So both are taken apart properly: what was a layer becomes a
   layer, what was a path becomes an editable path, what was type stays type.

   TWO IMPORTERS, ONE DOOR.

     · .PSD  needs a real Photoshop parser. ag-psd is vendored into the
       shop's own assets — ~830 KB — and fetched the first time, and only the
       first time, somebody actually opens a .psd.

     · .AI / .PDF / .SVG / .EPS need NOTHING AT ALL. A modern .ai file is a
       PDF; a PDF's artwork lives in FlateDecode streams, and every browser
       that matters can now inflate those natively with DecompressionStream.
       An SVG can be flattened by the browser's own geometry engine. A legacy
       .ai is PostScript, which shares its path operators with PDF. So the
       whole vector side is written here, weighs nothing, and has no library
       to go stale.

   Nothing here is on the critical path of drawing a charm.
   ========================================================================== */

/* ── loading a vendored library, once, on demand ─────────────────────────── */
const LIB = {};
function loadLib(key, url, globalName) {
  if (LIB[key]) return LIB[key];
  if (!url) return Promise.reject(new Error("not_configured"));
  if (window[globalName]) return (LIB[key] = Promise.resolve(window[globalName]));
  LIB[key] = new Promise((resolve, reject) => {
    const s = document.createElement("script");
    s.src = url;
    s.async = true;
    s.onload = () => {
      if (window[globalName]) resolve(window[globalName]);
      else reject(new Error("loaded_but_absent"));
    };
    s.onerror = () => { LIB[key] = null; reject(new Error("could_not_load")); };
    document.head.appendChild(s);
  });
  return LIB[key];
}
const loadPsdLib = () => loadLib("psd", D.psdUrl, "agPsd");

/* ═════════════════════════════════════════════════════════════════════════
   PHOTOSHOP

   A Photoshop file is a tree, and so is this studio's layer model, which is
   the whole reason the import is worth doing properly rather than flattening
   to a picture. Groups become groups. Opacity, visibility and stacking order
   survive. Text comes across as real, re-editable lettering. Vector shape
   layers come across as real, re-editable vectors. Everything else comes
   across as a picture layer you can still move, scale, mask and erase.

   RESILIENCE IS THE FEATURE. A .psd can carry 16- and 32-bit channels, CMYK
   and Lab, smart objects, adjustment layers, layer effects, clipping masks,
   linked files, 400 layers and a 30,000-pixel canvas. Every one of those is
   accounted for here — by handling it, by degrading it, or by stepping over
   it — and every single layer is converted inside its own try/catch, so one
   unreadable layer can never take the import, or the studio, down with it.
   ====================================================================== */

/* A .psd dropped straight onto the sheet, or chosen through the ordinary
   picture picker, goes the same way — nobody should have to find the right
   menu item to open a file they are already holding. Name OR type, because
   Windows, macOS and Linux disagree about which of the two a .psd has. */
function isPsdFile(f) {
  return !!f && (/\.psd$/i.test(f.name || "") || /photoshop/i.test(f.type || ""));
}

const PSD_MAX_LAYERS  = 28;      /* raster layers that get their own picture */
const PSD_MAX_PX      = 1400;    /* longest edge of any one rasterised layer */
const PSD_MAX_BYTES   = 220 * 1024 * 1024;   /* refuse absurd files early */

/* Photoshop's canvas is any shape; ours is square and normalised 0..1. One
   transform, computed once, maps every layer through the same contain-fit so
   relative positions and sizes are preserved exactly. */
function psdFit(psd) {
  const w = Math.max(1, Number(psd && psd.width) || 1);
  const h = Math.max(1, Number(psd && psd.height) || 1);
  const s = 1 / Math.max(w, h);
  return {
    w, h, s,
    ox: (1 - w * s) / 2,
    oy: (1 - h * s) / 2,
    x: (px) => (Number(px) || 0) * s + (1 - w * s) / 2,
    y: (py) => (Number(py) || 0) * s + (1 - h * s) / 2,
    d: (n) => (Number(n) || 0) * s,
  };
}

/* ── the plan ─────────────────────────────────────────────────────────────
   Pure: a parsed .psd in, a flat ordered list of intentions out. No canvas,
   no network, no side effects — which is what makes the awkward parts of the
   format testable rather than hopeful. Bottom-of-the-stack first, so the
   list can be pushed straight onto our items array in order. */
function psdPlan(psd, opts) {
  const o = opts || {};
  const fit = psdFit(psd);
  const plan = [];
  const warn = {};
  const note = (k) => { warn[k] = (warn[k] || 0) + 1; };
  let seen = 0;

  const walk = (nodes, path, inheritedOpacity, inheritedHidden) => {
    (nodes || []).forEach((n) => {
      if (!n || typeof n !== "object") return;
      if (seen++ > 800) { note("too_many"); return; }      /* a runaway file */
      const name = String(n.name == null ? "" : n.name).slice(0, 48) || "Layer";
      const hidden = !!(inheritedHidden || n.hidden);
      const op = (inheritedOpacity == null ? 1 : inheritedOpacity) *
                 (n.opacity == null ? 1 : Math.max(0, Math.min(1, Number(n.opacity))));

      /* A GROUP. Photoshop nests them arbitrarily; our model is one level
         deep, so nested groups are flattened and their path is kept in the
         name — "Face / Eyes / Left" stays legible and stays together. */
      if (Array.isArray(n.children)) {
        /* an EMPTY group is still a group, not a zero-sized layer — it
           contributes nothing and should be silent about it */
        if (n.children.length && path.length >= 1) note("nested_group");
        walk(n.children, path.concat([name]), op, hidden);
        return;
      }

      const group = path.join(" / ");
      const common = { name, group, hidden, opacity: op };

      /* An ADJUSTMENT layer changes the layers beneath it and has no pixels
         of its own. There is nothing here it could adjust, so it is stepped
         over rather than imported as an empty rectangle. */
      if (n.adjustment) { note("adjustment"); plan.push(Object.assign({ kind: "skip", why: "adjustment" }, common)); return; }

      /* TEXT. The one thing that must not arrive as a picture: lettering has
         to stay lettering or it can never be re-typed, re-placed, or engraved
         in the workshop's own face. */
      if (n.text && typeof n.text.text === "string" && n.text.text.trim()) {
        const t = n.text;
        const tr = Array.isArray(t.transform) && t.transform.length >= 6 ? t.transform : [1, 0, 0, 1, 0, 0];
        /* the transform's own scale, so text inside a scaled smart object
           still lands at the size it looks on screen */
        const sx = Math.hypot(Number(tr[0]) || 1, Number(tr[1]) || 0) || 1;
        const sy = Math.hypot(Number(tr[2]) || 0, Number(tr[3]) || 1) || 1;
        const st = t.style || {};
        const size = (Number(st.fontSize) || 24) * ((sx + sy) / 2);
        const box = {
          l: Number(t.left != null ? t.left : n.left) || 0,
          t: Number(t.top != null ? t.top : n.top) || 0,
          r: Number(t.right != null ? t.right : n.right) || 0,
          b: Number(t.bottom != null ? t.bottom : n.bottom) || 0,
        };
        /* tx/ty is the BASELINE of the first line; our lettering is drawn
           from a vertical centre, so it is nudged by roughly a cap height */
        const bx = Number(tr[4]) || box.l;
        const by = Number(tr[5]) || (box.t + size);
        const just = String((t.paragraphStyle || {}).justification || "left");
        const lines = t.text.replace(/\r\n?/g, "\n").split("\n");
        const widest = lines.reduce((m, l) => Math.max(m, l.length), 1);
        /* left-aligned in our painter, so centred and right-set Photoshop
           text is shifted back by an estimate of its own width */
        const approx = widest * size * 0.52;
        const left = just === "center" ? bx - approx / 2 : just === "right" ? bx - approx : bx;
        if (st.font && st.font.name) note("font_replaced");
        plan.push(Object.assign({
          kind: "text",
          text: t.text.slice(0, 240),
          /* clamped, like every imported coordinate: a centred headline whose
             width we can only estimate must not end up off the sheet */
          x: Math.max(-0.2, Math.min(1.2, fit.x(left))),
          y: Math.max(-0.2, Math.min(1.2, fit.y(by - size * 0.36))),
          fs: Math.max(0.012, Math.min(0.4, fit.d(size))),
          colour: psdColour(st.fillColor),
        }, common));
        return;
      }

      /* A VECTOR SHAPE layer — a rectangle, ellipse or pen path with a solid
         fill. These come across as real editable geometry rather than as a
         picture of geometry, which is the difference between "I can nudge
         that corner" and "I can move that photograph". */
      const vm = n.vectorMask || n.mask && n.mask.vectorMask;
      const paths = vm && Array.isArray(vm.paths) ? vm.paths : null;
      if (paths && paths.length && !n.canvas && !n.imageData) {
        const cs = psdVectorContours(paths, fit, psd);
        if (cs.flat.length >= 6) {
          plan.push(Object.assign({
            kind: "vector", p: cs.flat, q: cs.lens,
            colour: psdColour(n.vectorFill && n.vectorFill.color),
          }, common));
          return;
        }
        note("vector_unreadable");
      }

      /* Everything else is pixels — including smart objects, which Photoshop
         has already flattened for us, and layer effects, which it has not. */
      const l = Number(n.left) || 0, tp = Number(n.top) || 0;
      const r = Number(n.right) || 0, b = Number(n.bottom) || 0;
      const w = r - l, h = b - tp;
      if (!(w > 0 && h > 0)) { note("empty"); plan.push(Object.assign({ kind: "skip", why: "empty" }, common)); return; }
      if (n.effects) note("effects");
      if (n.clipping) note("clipping");
      /* ag-psd spells this "pass through", with a space — not "passThrough" */
      const bm = String(n.blendMode || "normal").replace(/\s/g, "").toLowerCase();
      if (bm !== "normal" && bm !== "passthrough") note("blend");
      plan.push(Object.assign({
        kind: "raster",
        x: fit.x(l), y: fit.y(tp), w: fit.d(w), h: fit.d(h),
        hasMask: !!(n.mask && (n.mask.canvas || n.mask.imageData)),
        src: n,
      }, common));
    });
  };
  walk((psd && psd.children) || [], [], 1, false);

  /* Hidden layers are carried across HIDDEN rather than dropped: a designer
     who turned an eye off in Photoshop still wants it in the file. */
  const rasters = plan.filter((e) => e.kind === "raster");
  const cap = Math.max(1, Number(o.maxLayers) || PSD_MAX_LAYERS);
  if (rasters.length > cap) {
    note("too_many_rasters");
    /* The ones that go are the ones already switched off, then the smallest,
       so what survives is what the design is actually made of. The sort puts
       the DOOMED first, which means hidden first (true before false) and
       small before large — get that backwards and the cull keeps every
       hidden layer and throws away the visible ones, which is exactly what
       an earlier version of this line did. */
    const doomed = rasters.slice()
      .sort((a, b) => ((b.hidden ? 1 : 0) - (a.hidden ? 1 : 0)) || (a.w * a.h - b.w * b.h))
      .slice(0, rasters.length - cap);
    doomed.forEach((e) => { e.kind = "skip"; e.why = "over_layer_limit"; });
  }
  return { plan, fit, warnings: warn,
           counts: {
             total: plan.length,
             raster: plan.filter((e) => e.kind === "raster").length,
             text: plan.filter((e) => e.kind === "text").length,
             vector: plan.filter((e) => e.kind === "vector").length,
             skipped: plan.filter((e) => e.kind === "skip").length,
           } };
}

/* Photoshop hands colour back in several dialects. Whatever arrives, one of
   our three engraving instructions has to come out the other side — and for
   an imported picture the honest default is an outline, because nobody has
   said anything about engraving yet. */
function psdColour(c) {
  if (!c || typeof c !== "object") return "#1c1d1d";
  const n = (v) => Math.max(0, Math.min(255, Math.round(Number(v) || 0)));
  if ("r" in c || "g" in c || "b" in c) {
    return "#" + [n(c.r), n(c.g), n(c.b)].map((v) => v.toString(16).padStart(2, "0")).join("");
  }
  return "#1c1d1d";
}

/* A Photoshop vector mask is a list of bezier knots. On disk each knot is
   six Y-FIRST fixed-point fractions of the DOCUMENT — which is Photoshop's
   ordering and nobody else's — but ag-psd has already undone both of those
   for us: what arrives is

       { linked, points: [cx1, cy1, ax, ay, cx2, cy2] }

   x-first, in document PIXELS, with the anchor in the middle and its two
   control points either side. (Reading it as anything else is not a subtle
   bug: every knot comes back undefined, the contour comes out empty, and a
   shape layer silently imports as nothing at all.) A subpath says `open`,
   not `closed`.

   Flattened here to the polylines our model speaks — sampled finely enough
   that a curve still reads as a curve. */
function psdVectorContours(paths, fit, psd) {
  const flat = [], lens = [];
  (paths || []).slice(0, 24).forEach((sub) => {
    const knots = (sub && (sub.knots || sub.points)) || [];
    if (knots.length < 2) return;
    const pts = [];
    const num = (v) => (typeof v === "number" && isFinite(v) ? v : null);
    const at = (k, which) => {
      const p = k && (Array.isArray(k) ? k : k.points);
      if (!Array.isArray(p) || p.length < 6) return null;
      const i = which === "leading" ? 0 : which === "anchor" ? 2 : 4;
      const x = num(p[i]), y = num(p[i + 1]);
      return x === null || y === null ? null : [x, y];
    };
    const closed = sub.open === undefined ? sub.closed !== false : !sub.open;
    const n = knots.length;
    for (let i = 0; i < n; i++) {
      const k0 = knots[i], k1 = knots[(i + 1) % n];
      if (!closed && i === n - 1) break;
      const p0 = at(k0, "anchor"), p3 = at(k1, "anchor");
      if (!p0 || !p3) continue;
      const c1 = at(k0, "trailing") || p0;
      const c2 = at(k1, "leading") || p3;
      const steps = 14;
      /* the loop stops one short of t=1 because a CLOSED contour gets that
         point from the next segment's start — an open one never does, so
         its last anchor has to be added explicitly below */
      for (let s = 0; s < steps; s++) {
        const t = s / steps, u = 1 - t;
        const x = u * u * u * p0[0] + 3 * u * u * t * c1[0] + 3 * u * t * t * c2[0] + t * t * t * p3[0];
        const y = u * u * u * p0[1] + 3 * u * u * t * c1[1] + 3 * u * t * t * c2[1] + t * t * t * p3[1];
        pts.push(x, y);
      }
      if (!closed && i === n - 2) pts.push(p3[0], p3[1]);
    }
    if (pts.length < 6) return;
    lens.push(pts.length / 2);
    for (let i = 0; i < pts.length; i += 2) {
      flat.push(Math.round(fit.x(pts[i]) * 1e4) / 1e4, Math.round(fit.y(pts[i + 1]) * 1e4) / 1e4);
    }
  });
  return { flat, lens };
}

/* ── rasterising one layer, safely ────────────────────────────────────────
   ag-psd hands back a canvas in a browser and an ImageData in Node. Either
   way this is the only place that touches pixels, it caps the size, it
   applies the layer's own mask if it has one, and it returns null rather
   than throwing when a layer turns out to be something nobody expected. */
function psdLayerToDataUrl(src, hasMask) {
  try {
    const cv = src.canvas || null;
    const id = src.imageData || null;
    let w = 0, h = 0;
    if (cv && cv.width) { w = cv.width; h = cv.height; }
    else if (id && id.width) { w = id.width; h = id.height; }
    if (!w || !h) return null;

    const scale = Math.min(1, PSD_MAX_PX / Math.max(w, h));
    const W = Math.max(1, Math.round(w * scale)), H = Math.max(1, Math.round(h * scale));
    const out = document.createElement("canvas");
    out.width = W; out.height = H;
    const ctx = out.getContext("2d");
    if (cv) {
      ctx.drawImage(cv, 0, 0, W, H);
    } else {
      const tmp = document.createElement("canvas");
      tmp.width = w; tmp.height = h;
      tmp.getContext("2d").putImageData(id, 0, 0);
      ctx.drawImage(tmp, 0, 0, W, H);
    }

    /* A LAYER MASK is greyscale, and greyscale is not alpha until somebody
       makes it so. Without this a masked layer arrives as the whole
       rectangle it was painted in, which is never what the file looks like
       in Photoshop. */
    if (hasMask && src.mask) {
      const m = src.mask;
      const mc = m.canvas || null, mi = m.imageData || null;
      const mw = mc ? mc.width : (mi ? mi.width : 0);
      const mh = mc ? mc.height : (mi ? mi.height : 0);
      if (mw && mh) {
        const mk = document.createElement("canvas");
        mk.width = W; mk.height = H;
        const mx = mk.getContext("2d", { willReadFrequently: true });
        /* the mask has its own origin in document space */
        const dx = ((Number(m.left) || 0) - (Number(src.left) || 0)) * scale;
        const dy = ((Number(m.top) || 0) - (Number(src.top) || 0)) * scale;
        if (mc) mx.drawImage(mc, dx, dy, mw * scale, mh * scale);
        else {
          const t2 = document.createElement("canvas");
          t2.width = mw; t2.height = mh;
          t2.getContext("2d").putImageData(mi, 0, 0);
          mx.drawImage(t2, dx, dy, mw * scale, mh * scale);
        }
        const md = mx.getImageData(0, 0, W, H);
        const dd = md.data;
        for (let i = 0; i < dd.length; i += 4) {
          dd[i + 3] = dd[i];                 /* luminance becomes alpha */
          dd[i] = dd[i + 1] = dd[i + 2] = 255;
        }
        mx.putImageData(md, 0, 0);
        ctx.globalCompositeOperation = "destination-in";
        ctx.drawImage(mk, 0, 0);
        ctx.globalCompositeOperation = "source-over";
      }
    }
    /* PNG, always: a Photoshop layer is transparent far more often than not,
       and this studio has already learned once what JPEG does to that */
    return { dataUrl: out.toDataURL("image/png"), w: W, h: H };
  } catch (e) {
    return null;
  }
}

/* ── the import itself ──────────────────────────────────────────────────── */
let __psdBusy = false;
async function psdImport(file, cx, cy) {
  if (!__mk) { toast("Open a drawing first", "err"); return; }
  if (__psdBusy) { toast("Still working on the last one…", "err"); return; }
  if (!file) return;
  if (file.size > PSD_MAX_BYTES) {
    toast(`That file is ${Math.round(file.size / 1048576)} MB, which is more than we can open here. ` +
          "Send us a smaller version and we'll take it from there.", "err");
    return;
  }
  __psdBusy = true;
  /* the studio's own busy chrome, driven by the same counter every other
     import uses — psdImport used to toggle a flag nothing read, so a
     multi-minute Photoshop import showed no sign of life at all */
  __mkImporting++;
  mkImportChrome();
  try {
  toast(`Opening “${file.name.slice(0, 28)}”…`, "gold");

  let lib, psd, planned;
  try {
    lib = await loadPsdLib();
  } catch (e) {
    toast("The Photoshop reader couldn't load — check your connection and try again.", "err");
    return;
  }
  try {
    const buf = await file.arrayBuffer();
    /* skipCompositeImageData: the flattened preview is the one thing we do
       NOT want — it is the picture we are here to take apart. */
    psd = lib.readPsd(buf, {
      skipCompositeImageData: true,
      skipThumbnail: true,
      skipLinkedFilesData: true,
      useImageData: false,
      throwForMissingFeatures: false,
      logMissingFeatures: false,
    });
  } catch (e) {
    console.warn("[studio] psd read failed:", (e && e.message) || e);
    toast("That file couldn't be read as a Photoshop document. If it was saved from " +
          "another program, try “Save As → Photoshop (.psd)” with compatibility on.", "err");
    return;
  }

  try {
    planned = psdPlan(psd, {});
  } catch (e) {
    console.warn("[studio] psd plan failed:", (e && e.message) || e);
    toast("That document's layer structure couldn't be read.", "err");
    return;
  }

  const { plan, counts, warnings } = planned;
  if (!plan.filter((e) => e.kind !== "skip").length) {
    toast("That .psd has no layers this studio can use — it may be a single " +
          "flattened background, in which case export it as a PNG instead.", "err");
    return;
  }

  const sess = ensureSession();
  const groupIds = {};
  const made = [];
  const madeItems = [];        /* what the repository entry will carry */
  const shots = [];            /* the bitmaps, for one composite thumbnail */
  let done = 0;
  const total = plan.filter((e) => e.kind !== "skip").length;

  mkPushUndo();
  for (const e of plan) {
    if (e.kind === "skip") continue;
    done++;
    try {
      const base = {
        id: mkId(), n: e.name, o: Math.max(0.05, Math.min(1, e.opacity || 1)),
        hid: e.hidden ? 1 : 0,
      };
      if (e.group) {
        if (!groupIds[e.group]) {
          groupIds[e.group] = "g" + Math.random().toString(36).slice(2, 8);
          if (!__mk.groups) __mk.groups = {};
          __mk.groups[groupIds[e.group]] = { n: e.group.slice(0, 48), col: 0 };
        }
        base.g = groupIds[e.group];
      }

      if (e.kind === "text") {
        /* ONE FACE. The workshop engraves a single clean sans, so whatever
           Photoshop was set to becomes that — silently and always, because
           offering the customer a face the metal cannot keep is a promise
           nobody can honour. */
        const it = mkPack(Object.assign({}, base, {
          t: "label", s: e.text, p: [e.x, e.y], fs: e.fs, ff: "sans",
          c: MK_ENGRAVE, f: "none",
        }));
        __mk.items.push(it);
        made.push(__mk.items.length - 1);
        madeItems.push(it);
        continue;
      }

      if (e.kind === "vector") {
        /* the same reading as the vector importer: a Photoshop shape layer
           filled with the reserved blue is a cut-out, not a blue engraving */
        const vi = mkIntentFromColour(e.colour);
        const vink = mkIntentInk(vi);
        const it = mkPack(Object.assign({}, base, {
          t: "fill", p: e.p, q: e.q, c: vink, f: vi === "none" ? "none" : vink, w: 0.002, gr: 1,
        }));
        __mk.items.push(it);
        made.push(__mk.items.length - 1);
        madeItems.push(it);
        continue;
      }

      /* Raster Photoshop layers use exactly the same high-fidelity black-shape
         pipeline as JPG/PNG imports. The trace is letterboxed internally, so
         map it back out through `frame` before restoring Photoshop document
         coordinates; this is what keeps a circle circular. */
      const pic = psdLayerToDataUrl(e.src, e.hasMask);
      if (!pic) continue;
      const shaped = await rasterShapesBlack(pic.dataUrl);
      if (!shaped.kept) continue;
      const shapedImg = await mkLoadImageUrl(shaped.dataUrl);
      const traced = mkTraceImportedRaster(shapedImg);
      if (!(traced && traced.items && traced.items.length)) {
        console.warn("[studio] PSD raster layer could not be separated:", e.name);
        continue;
      }
      const fr = traced.frame || { x: 0, y: 0, w: 1, h: 1 };
      traced.items.forEach((part, partIndex) => {
        const it = mkPack(Object.assign({}, part, base));
        it.id = mkId();
        for (let k = 0; k + 1 < it.p.length; k += 2) {
          const u = (it.p[k] - fr.x) / Math.max(1e-6, fr.w);
          const v = (it.p[k + 1] - fr.y) / Math.max(1e-6, fr.h);
          it.p[k] = e.x + u * e.w;
          it.p[k + 1] = e.y + v * e.h;
        }
        it.c = MK_ENGRAVE;
        it.f = MK_ENGRAVE;
        it.w = Math.max(0.0012, Number(it.w) || 0.0012);
        it.tr = 1;
        it.n = ((part.n && part.n !== "Shape") ? part.n : "Component " + (partIndex + 1)) +
               (e.name ? " · " + String(e.name).slice(0, 24) : "");
        __mk.items.push(it);
        made.push(__mk.items.length - 1);
        madeItems.push(it);
      });
      if (!e.hidden && shots.length < 40) {
        shots.push({ dataUrl: shaped.dataUrl, x: e.x, y: e.y, w: e.w, h: e.h, o: e.opacity });
      }
    } catch (err) {
      /* ONE LAYER, ONE FAILURE. A .psd is a thirty-year-old container full of
         features nobody documents any more; the guarantee this import makes
         is that no single strange layer can cost the customer the other
         twenty-nine. */
      console.warn("[studio] psd layer skipped:", e && e.name, (err && err.message) || err);
    }
  }

  mkSelSet(made);
  mkTouch();
  mkRenderLayers();
  saveSession();
  /* into the repository, badged PSD, carrying every one of those layers —
     so the document can be dropped into another design later and still come
     apart. Never allowed to cost the import if it fails. */
  try {
    const prev = await impPsdPreview(sess.id, String(file.name).replace(/\.[a-z0-9]+$/i, ""), shots);
    await impRegister(file, "psd", madeItems, prev);
  } catch (e) { console.warn("[studio] psd repository entry skipped:", (e && e.message) || e); }

  /* COUNT WHAT LANDED, NOT WHAT WAS PLANNED. If the uploads fail — an
     account out of slots, a library at its ceiling — nothing reaches the
     sheet, and the old message still cheerfully announced twenty-eight
     picture layers that were not there. */
  const got = {
    raster: madeItems.filter((i) => i.t === "img").length,
    text: madeItems.filter((i) => i.t === "label").length,
    vector: madeItems.filter((i) => i.t === "fill").length,
  };
  if (!madeItems.length) {
    toast("None of that file's layers could be brought in. If your account is " +
          "out of image slots, that is usually why.", "err");
    return;
  }
  const bits = [];
  if (got.raster) bits.push(`${got.raster} picture layer${got.raster === 1 ? "" : "s"}`);
  if (got.text) bits.push(`${got.text} text layer${got.text === 1 ? "" : "s"}`);
  if (got.vector) bits.push(`${got.vector} shape${got.vector === 1 ? "" : "s"}`);
  const ng = Object.keys(groupIds).length;
  toast(`“${file.name.slice(0, 24)}” opened — ${bits.join(", ")}` +
        `${ng ? `, in ${ng} group${ng === 1 ? "" : "s"}` : ""}. All in Layers.`, "gold");
  const said = psdWarnings(warnings, got);
  if (said) setTimeout(() => toast(said, "note"), 2600);
  } finally {
    __psdBusy = false;
    __mkImporting = Math.max(0, __mkImporting - 1);
    mkImportChrome();
  }
}

/* ── ONE SENTENCE, NOT A CHANGELOG ───────────────────────────────────────
   The honest thing is to say what did not survive. The unkind thing is to
   say all of it at once: a semicolon-joined list of adjustment layers,
   clipping masks and blend modes is a release note, and on a phone it was a
   red block covering a quarter of the screen. So these are ranked, and only
   the one that actually changes what the customer will SEE is said — the
   rest are visible in Layers, which is where somebody who cares will look. */
function psdWarnings(w, counts) {
  if (w.too_many_rasters) return `That file had more layers than we can take, so the largest ${PSD_MAX_LAYERS} came across.`;
  if (w.font_replaced) return "Your type came across in our engraving face — it's the one we cut.";
  if (w.effects) return "Glows, shadows and layer strokes don't come across — the shapes underneath them did.";
  if (w.blend) return "Blend modes don't come across, so a few layers will look flatter here than in Photoshop.";
  if (w.nested_group) return "Groups inside groups were opened out one level; their names kept the full path.";
  if (w.clipping) return "Clipping masks came across as ordinary layers.";
  if (w.adjustment) return "Adjustment layers have no artwork of their own, so they were left out.";
  return "";
}


/* ═════════════════════════════════════════════════════════════════════════
   ILLUSTRATOR, AND EVERY VECTOR FORMAT WORTH HAVING

   A modern .ai file IS a PDF. Illustrator has written PDF-compatible files
   by default since version 9, and the artwork sits in ordinary PDF content
   streams — the same operators a .pdf uses, compressed with the same Flate.
   Every browser that matters can now inflate that natively, so the whole
   thing is parsed here with no library, no worker and no download.

   Four doors into one importer:

     .ai   PDF-compatible (the default for a quarter of a century), or
           legacy PostScript for files old enough to predate it
     .pdf  the same parser, unchanged
     .svg  flattened by the browser's own geometry engine, which knows more
           about arcs and beziers than any parser this studio could carry
     .eps  PostScript, sharing its path operators with PDF

   WHAT COMES OUT is not a picture. Every filled path becomes a real fill
   object; every stroked path becomes a real stroke; type becomes lettering.
   All of it selectable, movable, re-fillable, and grouped under the file's
   own name so it travels as one until you decide otherwise.

   BULLETPROOFING IS THE POINT. A PDF in the wild may be linearised, have a
   broken cross-reference table, hide its objects inside object streams, be
   encrypted, use filters nobody has implemented since 1998, or simply be a
   .ai file saved without PDF compatibility. Every one of those is handled —
   by parsing around it, by degrading, or by saying so plainly — and every
   path is converted inside its own try/catch.
   ====================================================================== */

const VEC_MAX_OBJECTS = 900;      /* paths that become their own layer */
const VEC_MAX_POINTS  = 60000;    /* coordinate numbers, across the import */
const VEC_MAX_PATH_POINTS = 8000; /* …and within ONE path, so a single
                                     traced outline cannot exceed the whole
                                     budget by itself */
const VEC_CURVE_STEPS = 12;       /* segments per bezier — smooth, not heavy */
const VEC_MAX_BYTES   = 60 * 1024 * 1024;
/* a content stream longer than this is not artwork anybody drew; tokenising
   it would allocate gigabytes before the interpreter's own cap could help */
const VEC_MAX_STREAM  = 12 * 1024 * 1024;
const VEC_MAX_PDF_OBJECTS = 20000;

/* ── what IS this file? By its bytes, never by its name ─────────────────── */
function vecSniff(bytes, name) {
  const head = String.fromCharCode.apply(null, Array.prototype.slice.call(bytes, 0, 1024));
  const ext = (String(name || "").match(/\.([a-z0-9]+)$/i) || [])[1];
  if (/^%PDF-/.test(head)) return "pdf";
  /* an EPS may carry a binary preview header before the PostScript */
  if (bytes[0] === 0xC5 && bytes[1] === 0xD0 && bytes[2] === 0xD3 && bytes[3] === 0xC6) return "epsbin";
  if (/^%!PS/.test(head)) return "eps";
  if (/<svg[\s>]/i.test(head) || (/^\s*<\?xml/.test(head) && /<svg/i.test(head))) return "svg";
  /* a .ai that is neither: almost always PDF further in, or PostScript */
  if (head.indexOf("%PDF-") >= 0) return "pdf";
  if (head.indexOf("%!PS") >= 0) return "eps";
  if ((ext || "").toLowerCase() === "svg") return "svg";
  return "";
}
function isVectorFile(f) {
  return !!f && /\.(ai|pdf|svg|svgz|eps|ps)$/i.test(f.name || "");
}
/* .svgz is an SVG in a gzip coat, and DecompressionStream wears it off */
function isGzip(bytes) { return bytes[0] === 0x1f && bytes[1] === 0x8b; }
async function vecGunzip(bytes) {
  if (typeof DecompressionStream !== "function") return null;
  try {
    const st = new Blob([bytes]).stream().pipeThrough(new DecompressionStream("gzip"));
    return new Uint8Array(await new Response(st).arrayBuffer());
  } catch (e) { return null; }
}
function isDesignFile(f) {
  return isPsdFile(f) || isVectorFile(f);
}

/* ── inflating a PDF stream, with the platform's own zlib ────────────────── */
async function vecInflate(bytes) {
  if (typeof DecompressionStream !== "function") return null;
  const tryOne = async (fmt, buf) => {
    try {
      const st = new Blob([buf]).stream().pipeThrough(new DecompressionStream(fmt));
      return new Uint8Array(await new Response(st).arrayBuffer());
    } catch (e) { return null; }
  };
  /* FlateDecode is zlib-wrapped; a few writers emit raw deflate instead, and
     a stray leading byte is common enough to be worth one retry */
  return (await tryOne("deflate", bytes)) ||
         (await tryOne("deflate-raw", bytes)) ||
         (await tryOne("deflate", bytes.subarray(1)));
}

/* ── the PDF object soup ─────────────────────────────────────────────────
   Deliberately NOT xref-driven. A cross-reference table can be broken,
   rebuilt, linearised, incremental, or replaced by a compressed xref stream
   that would itself need inflating before anything could be found. Scanning
   for "N G obj … endobj" finds every top-level object in every one of those
   cases, and a PDF's content streams are always top-level objects. */
function vecPdfObjects(bytes) {
  const map = new Map();
  const s = vecLatin(bytes);

  /* EVERY TERMINATOR, FOUND ONCE. The obvious way to write this is an
     indexOf("endobj", start) inside the loop — and on a corrupt or hostile
     file with thousands of "N G obj" markers and no "endobj" at all, every
     one of those searches runs to the end of the file. That is quadratic:
     measured at 29 seconds of frozen tab on a 4 MB file. Both marker lists
     are gathered in one linear pass instead, and matched up with a pointer
     that only ever moves forward. */
  const ends = [];
  const endRe = /endobj\b/g;
  let e;
  while ((e = endRe.exec(s))) ends.push(e.index);

  const re = /(\d+)\s+(\d+)\s+obj\b/g;
  let m, ei = 0;
  while ((m = re.exec(s))) {
    const num = Number(m[1]);
    const start = m.index + m[0].length;
    while (ei < ends.length && ends[ei] < start) ei++;
    /* a truncated or partially-flushed file can hold complete dictionaries
       after its last endobj; the end of the file is their boundary */
    const end = ei < ends.length ? ends[ei] : s.length;
    /* later generations win, exactly as an incremental update intends */
    map.set(num, { start, end, dict: s.slice(start, Math.min(end, start + 4000)) });
    if (map.size > VEC_MAX_PDF_OBJECTS) break;
  }
  return { map, s };
}
/* latin1, so byte offsets and string offsets are the same number — which is
   the whole reason a PDF can be scanned as text and sliced as bytes */
function vecLatin(bytes) {
  let out = "";
  const CH = 0x8000;
  for (let i = 0; i < bytes.length; i += CH) {
    out += String.fromCharCode.apply(null, bytes.subarray(i, Math.min(bytes.length, i + CH)));
  }
  return out;
}

async function vecPdfStream(obj, ctx) {
  const s = ctx.s;
  const head = s.slice(obj.start, obj.end);
  const si = head.indexOf("stream");
  if (si < 0) return null;
  let from = obj.start + si + 6;
  if (s[from] === "\r") from++;
  if (s[from] === "\n") from++;
  let to = s.indexOf("endstream", from);
  if (to < 0) to = obj.end;
  /* /Length is often an indirect reference, and is often wrong in a repaired
     file — the endstream marker is the honest boundary */
  let raw = ctx.bytes.subarray(from, to);
  while (raw.length && (raw[raw.length - 1] === 10 || raw[raw.length - 1] === 13)) raw = raw.subarray(0, raw.length - 1);
  const dict = head.slice(0, si);
  /* /Filter is a PIPELINE and may be an array: [/ASCII85Decode /LZWDecode].
     An "[^/]*" between the key and the name cannot cross the slash of an
     earlier filter, so a two-filter array walked straight past these guards
     and handed LZW bytes to the tokeniser as if they were artwork. The whole
     filter clause is captured instead, and every name in it is inspected. */
  const filters = (/\/Filter\s*(\[[^\]]*\]|\/[A-Za-z0-9]+)/.exec(dict) || [, ""])[1];
  if (/DCTDecode|JPXDecode|CCITTFaxDecode|JBIG2Decode/.test(filters)) return null;
  /* filters are a PIPELINE, applied left to right, and Illustrator's own PDFs
     routinely wrap Flate in ASCII85. Both of the text armours are trivial, so
     they are undone here rather than refused. */
  if (/ASCIIHexDecode/.test(filters)) raw = vecUnhex(raw);
  if (/ASCII85Decode/.test(filters)) raw = vecUn85(raw);
  if (/LZWDecode|RunLengthDecode|Crypt/.test(filters)) { ctx.warn("filter"); return null; }
  if (/FlateDecode/.test(filters)) {
    const out = await vecInflate(raw);
    if (!out) return null;
    raw = out;
  }
  return vecLatin(raw);
}
/* ASCII85, as PostScript and PDF both spell it: five characters to four
   bytes, "z" for four zeroes, "~>" for the end. */
function vecUn85(bytes) {
  try {
    const out = [];
    let tuple = 0, n = 0;
    for (let i = 0; i < bytes.length; i++) {
      const c = bytes[i];
      if (c === 0x7e) break;                              /* ~> */
      if (c <= 0x20 || c === 0) continue;                 /* whitespace */
      if (c === 0x7a && n === 0) { out.push(0, 0, 0, 0); continue; }   /* z */
      if (c < 0x21 || c > 0x75) continue;                 /* not base-85 */
      tuple = tuple * 85 + (c - 0x21); n++;
      if (n === 5) {
        out.push((tuple >>> 24) & 255, (tuple >>> 16) & 255, (tuple >>> 8) & 255, tuple & 255);
        tuple = 0; n = 0;
      }
    }
    if (n > 1) {                                          /* a partial group */
      for (let k = n; k < 5; k++) tuple = tuple * 85 + 84;
      const b = [(tuple >>> 24) & 255, (tuple >>> 16) & 255, (tuple >>> 8) & 255, tuple & 255];
      for (let k = 0; k < n - 1; k++) out.push(b[k]);
    }
    return new Uint8Array(out);
  } catch (e) { return bytes; }
}
function vecUnhex(bytes) {
  try {
    const out = [];
    let hi = -1;
    for (let i = 0; i < bytes.length; i++) {
      const c = bytes[i];
      if (c === 0x3e) break;                              /* > */
      let v = -1;
      if (c >= 0x30 && c <= 0x39) v = c - 0x30;
      else if (c >= 0x41 && c <= 0x46) v = c - 55;
      else if (c >= 0x61 && c <= 0x66) v = c - 87;
      else continue;
      if (hi < 0) hi = v; else { out.push((hi << 4) | v); hi = -1; }
    }
    if (hi >= 0) out.push(hi << 4);
    return new Uint8Array(out);
  } catch (e) { return bytes; }
}

/* ── which streams are ARTWORK, and which are furniture ──────────────────
   A PDF is full of streams that are not pages: font programmes, colour
   profiles, embedded pictures, XML metadata, object stores — and, in an
   Illustrator file, a complete second copy of the whole document in
   Illustrator's own private PostScript dialect. That last one matters more
   than any of the others: parse it as well as the page and every shape
   arrives TWICE. So the page's own content streams are found and preferred,
   and the furniture is named and stepped over. */
const VEC_SKIP_DICT = /\/Type\s*\/(Font|FontDescriptor|Metadata|ObjStm|XRef|XObject\s*\/Subtype\s*\/Image)\b|\/Subtype\s*\/(Image|Type1C|CIDFontType0C|TrueType|XML)\b|\/FontFile\d?\b|\/AIPrivateData/;
function vecIsFurniture(dict) { return VEC_SKIP_DICT.test(dict); }
/* Illustrator's private copy announces itself in its first line */
function vecIsPrivate(str) {
  const head = str.slice(0, 400);
  return /%!PS-Adobe|%%AI\d|%AI\d?_|BeginPrivate|%%BeginProlog/.test(head);
}
/* the page tree, read the cheap way: every /Type /Page object names its
   /Contents, and that is where a page's artwork legally has to live */
function vecPdfPageStreams(ctx) {
  const want = [];
  ctx.map.forEach((obj, num) => {
    if (!/\/Type\s*\/Page\b/.test(obj.dict)) return;
    const m = /\/Contents\s*(\[[^\]]*\]|\d+\s+\d+\s+R)/.exec(obj.dict);
    if (!m) return;
    const re = /(\d+)\s+\d+\s+R/g;
    let r;
    while ((r = re.exec(m[1]))) want.push(Number(r[1]));
  });
  return want;
}

/* ── the content stream, read as graphics ────────────────────────────────
   A deliberate subset: transforms, paths, painting and colour. Everything
   else — clipping, shading, transparency groups, XObjects, ICC profiles —
   is stepped over, because a charm is a silhouette with marks on it and none
   of that survives into metal anyway. */
function vecTokens(str) {
  const out = [];
  /* the interpreter has its own operator cap, but that only helps AFTER this
     function has allocated a token object per token: 20 MB of stream measured
     at 6.8 million objects and 482 MB of heap. The cap has to be here. */
  if (str.length > VEC_MAX_STREAM) str = str.slice(0, VEC_MAX_STREAM);
  const n = str.length;
  let i = 0;
  while (i < n) {
    const c = str[i];
    if (c === "%") { while (i < n && str[i] !== "\n" && str[i] !== "\r") i++; continue; }
    if (c === " " || c === "\n" || c === "\r" || c === "\t" || c === "\f" || c === "\0") { i++; continue; }
    if (c === "(") {                       /* a literal string */
      let d = 1, j = i + 1, buf = "";
      while (j < n && d > 0) {
        if (str[j] === "\\") { buf += str[j + 1] || ""; j += 2; continue; }
        if (str[j] === "(") d++;
        else if (str[j] === ")") { d--; if (!d) break; }
        buf += str[j]; j++;
      }
      out.push({ t: "str", v: buf });
      i = j + 1; continue;
    }
    if (c === "<" && str[i + 1] !== "<") {
      const j = str.indexOf(">", i);
      out.push({ t: "str", v: "" });
      i = (j < 0 ? n : j + 1); continue;
    }
    if (c === "<" && str[i + 1] === "<") { out.push({ t: "op", v: "<<" }); i += 2; continue; }
    if (c === ">" && str[i + 1] === ">") { out.push({ t: "op", v: ">>" }); i += 2; continue; }
    if (c === "[" || c === "]") { out.push({ t: "op", v: c }); i++; continue; }
    if (c === "/") {
      let j = i + 1;
      while (j < n && !/[\s/[\]()<>{}%]/.test(str[j])) j++;
      out.push({ t: "name", v: str.slice(i + 1, j) });
      i = j; continue;
    }
    if (/[-+.\d]/.test(c)) {
      let j = i;
      while (j < n && /[-+.\deE]/.test(str[j])) j++;
      const v = parseFloat(str.slice(i, j));
      out.push({ t: "num", v: isFinite(v) ? v : 0 });
      i = j; continue;
    }
    let j = i;
    while (j < n && !/[\s/[\]()<>{}%]/.test(str[j])) j++;
    if (j === i) { i++; continue; }
    out.push({ t: "op", v: str.slice(i, j) });
    i = j;
  }
  return out;
}

const VEC_MUL = (a, b) => [
  a[0] * b[0] + a[1] * b[2], a[0] * b[1] + a[1] * b[3],
  a[2] * b[0] + a[3] * b[2], a[2] * b[1] + a[3] * b[3],
  a[4] * b[0] + a[5] * b[2] + b[4], a[4] * b[1] + a[5] * b[3] + b[5],
];
const VEC_APPLY = (m, x, y) => [m[0] * x + m[2] * y + m[4], m[1] * x + m[3] * y + m[5]];

/* one painted thing, in page coordinates */
function vecRunContent(str, opts) {
  const o = opts || {};
  const toks = vecTokens(str);
  const out = [];
  let ctm = (o.ctm || [1, 0, 0, 1, 0, 0]).slice();
  const stack = [];
  let path = [], sub = null, cur = [0, 0], start = [0, 0];
  let fillCol = [0, 0, 0], strokeCol = [0, 0, 0], lineW = 1;
  let text = null, tm = null, fontSize = 12;
  const st = [];
  let pending = 0;

  const flat = (x, y) => VEC_APPLY(ctm, x, y);
  const moveTo = (x, y) => { sub = [flat(x, y)]; path.push(sub); cur = [x, y]; start = [x, y]; };
  const lineTo = (x, y) => { if (!sub) moveTo(x, y); else { sub.push(flat(x, y)); cur = [x, y]; } };
  const curveTo = (x1, y1, x2, y2, x3, y3) => {
    if (!sub) moveTo(x1, y1);
    const p0 = cur;
    for (let s = 1; s <= VEC_CURVE_STEPS; s++) {
      const t = s / VEC_CURVE_STEPS, u = 1 - t;
      const X = u * u * u * p0[0] + 3 * u * u * t * x1 + 3 * u * t * t * x2 + t * t * t * x3;
      const Y = u * u * u * p0[1] + 3 * u * u * t * y1 + 3 * u * t * t * y2 + t * t * t * y3;
      sub.push(flat(X, Y));
    }
    cur = [x3, y3];
  };
  const paint = (kind) => {
    const subs = path.filter((sp) => sp.length >= 2);
    if (subs.length && out.length < VEC_MAX_OBJECTS) {
      out.push({ kind, subs, fill: fillCol.slice(), stroke: strokeCol.slice(),
                 w: lineW * Math.hypot(ctm[0], ctm[1]) });
    }
    path = []; sub = null;
  };

  for (let i = 0; i < toks.length; i++) {
    const t = toks[i];
    if (t.t !== "op") { st.push(t); continue; }
    const op = t.v;
    /* an array's brackets are operands, not operators. Clearing the stack on
       "]" would empty it one token before TJ ever saw what was inside. */
    if (op === "[" || op === "]") { st.push(t); continue; }
    const num = (k) => { const v = st[st.length - k]; return v && v.t === "num" ? v.v : 0; };
    try {
      switch (op) {
        case "q": stack.push({ ctm: ctm.slice(), f: fillCol.slice(), s: strokeCol.slice(), w: lineW }); break;
        case "Q": { const g = stack.pop(); if (g) { ctm = g.ctm; fillCol = g.f; strokeCol = g.s; lineW = g.w; } break; }
        case "cm": ctm = VEC_MUL([num(6), num(5), num(4), num(3), num(2), num(1)], ctm); break;
        case "w": lineW = num(1); break;
        case "m": moveTo(num(2), num(1)); break;
        case "l": lineTo(num(2), num(1)); break;
        case "c": curveTo(num(6), num(5), num(4), num(3), num(2), num(1)); break;
        case "v": curveTo(cur[0], cur[1], num(4), num(3), num(2), num(1)); break;
        case "y": curveTo(num(4), num(3), num(2), num(1), num(2), num(1)); break;
        case "h": if (sub && sub.length) { sub.push(sub[0].slice()); cur = start.slice(); } break;
        case "re": {
          const x = num(4), y = num(3), w = num(2), h = num(1);
          moveTo(x, y); lineTo(x + w, y); lineTo(x + w, y + h); lineTo(x, y + h);
          sub.push(sub[0].slice());
          break;
        }
        case "n": path = []; sub = null; break;
        case "f": case "F": case "f*": paint("fill"); break;
        case "S": case "s": paint("stroke"); break;
        case "B": case "B*": case "b": case "b*": paint("both"); break;
        case "g":  fillCol = [num(1), num(1), num(1)]; break;
        case "G":  strokeCol = [num(1), num(1), num(1)]; break;
        case "rg": fillCol = [num(3), num(2), num(1)]; break;
        case "RG": strokeCol = [num(3), num(2), num(1)]; break;
        case "k":  fillCol = vecCmyk(num(4), num(3), num(2), num(1)); break;
        case "K":  strokeCol = vecCmyk(num(4), num(3), num(2), num(1)); break;
        case "sc": case "scn": case "SC": case "SCN": {
          const nums = [];
          for (let k = st.length - 1; k >= 0 && st[k].t === "num" && nums.length < 4; k--) nums.unshift(st[k].v);
          const col = nums.length >= 4 ? vecCmyk(nums[0], nums[1], nums[2], nums[3])
                    : nums.length >= 3 ? [nums[0], nums[1], nums[2]]
                    : nums.length >= 1 ? [nums[0], nums[0], nums[0]] : [0, 0, 0];
          if (op === "sc" || op === "scn") fillCol = col; else strokeCol = col;
          break;
        }
        case "BT": text = []; tm = [1, 0, 0, 1, 0, 0]; break;
        case "ET": {
          if (text && text.length && o.onText) {
            try { o.onText(text); } catch (e) {}
          }
          text = null; break;
        }
        case "Tf": fontSize = num(1); break;
        case "Td": case "TD": if (tm) tm = VEC_MUL([1, 0, 0, 1, num(2), num(1)], tm); break;
        case "Tm": tm = [num(6), num(5), num(4), num(3), num(2), num(1)]; break;
        case "Tj": case "'": case "\"": {
          const sv = st[st.length - 1];
          if (text && tm && sv && sv.t === "str" && sv.v) {
            const at = VEC_APPLY(VEC_MUL(tm, ctm), 0, 0);
            text.push({ s: sv.v, x: at[0], y: at[1],
                        size: fontSize * Math.hypot(tm[0], tm[1]) * Math.hypot(ctm[0], ctm[1]) });
          }
          break;
        }
        case "TJ": {
          if (text && tm) {
            let joined = "";
            for (let k = st.length - 1; k >= 0; k--) {
              if (st[k].t === "op" && st[k].v === "[") break;
              if (st[k].t === "str") joined = st[k].v + joined;
            }
            if (joined) {
              const at = VEC_APPLY(VEC_MUL(tm, ctm), 0, 0);
              text.push({ s: joined, x: at[0], y: at[1],
                          size: fontSize * Math.hypot(tm[0], tm[1]) * Math.hypot(ctm[0], ctm[1]) });
            }
          }
          break;
        }
        default: break;
      }
    } catch (e) { /* one bad operator never stops the page */ }
    st.length = 0;
    if (++pending > 400000) break;             /* a runaway stream */
  }
  return out;
}
/* the naive conversion is the right one here: these numbers exist to decide
   dark from light, not to colour-manage a print run */
function vecCmyk(c, m, y, k) {
  return [(1 - Math.min(1, c + k)), (1 - Math.min(1, m + k)), (1 - Math.min(1, y + k))];
}

/* ── PostScript, for a .ai old enough to need it ─────────────────────────
   Illustrator's EPS uses the same single-letter path operators PDF does,
   which is not a coincidence: both descend from the same page description
   language. So the same interpreter reads it, with the small differences
   handled here rather than duplicated. */
function vecEpsContent(str) {
  /* the artwork sits between %%EndSetup and %%Trailer in an AI EPS; taking
     the whole file also works and simply reads a little more */
  const from = str.indexOf("%%EndSetup");
  const to = str.indexOf("%%Trailer");
  const body = str.slice(from > 0 ? from : 0, to > 0 ? to : str.length);
  /* Illustrator's operator set is PDF's with the case doubled: the UPPERCASE
     twin of every path operator means "and this segment is stroked", which
     changes nothing about the geometry. So the pairs are folded onto their
     PDF spelling, and only the genuinely different ones are translated.
     Getting this wrong is not academic — an earlier reading of it mapped Y
     (a curve) onto f (a fill), which quietly turned curves into paint. */
  const map = {
    L: "l", C: "c", V: "v", Y: "y",           /* path, stroked twin */
    N: "n", F: "f",                            /* no-paint, fill */
    b: "b", B: "B",                            /* close+fill+stroke */
    Xa: "rg", XA: "RG",                        /* RGB fill / stroke */
    Xx: "k", XX: "K",                          /* CMYK fill / stroke */
    xa: "rg", xA: "RG",
    u: "", U: "", "*u": "", "*U": "",          /* group / compound markers */
    A: "", D: "", d0: "", d1: "",              /* hints we do not need */
  };
  /* …and only OUTSIDE a literal string. "(A D F b U test)" is somebody's
     text, not five operators, and rewriting inside it changes their words. */
  let out = "", i = 0;
  while (i < body.length) {
    const open = body.indexOf("(", i);
    const upto = open < 0 ? body.length : open;
    out += body.slice(i, upto).replace(/(^|[\s\]])(\*?[A-Za-z][A-Za-z0-9*]?)(?=[\s[\]/(]|$)/g,
      (whole, pre, tok) => (Object.prototype.hasOwnProperty.call(map, tok)
        ? pre + map[tok] : whole));
    if (open < 0) break;
    /* copy the string through verbatim, honouring escapes and nesting */
    let j = open + 1, d = 1;
    while (j < body.length && d > 0) {
      if (body[j] === "\\") { j += 2; continue; }
      if (body[j] === "(") d++;
      else if (body[j] === ")") d--;
      j++;
    }
    out += body.slice(open, j);
    i = j;
  }
  return out;
}

/* ── SVG, flattened by the browser itself ────────────────────────────────
   Every geometry element in SVG answers getTotalLength and getPointAtLength,
   which means the browser's own renderer will walk any path, arc, ellipse or
   rounded rectangle for us — including the arc flags and the elliptical
   corner cases that trip up every hand-written path parser ever shipped. */
function vecFromSvg(text) {
  const out = { objs: [], texts: [], box: null, warn: {} };
  let doc;
  try { doc = new DOMParser().parseFromString(text, "image/svg+xml"); } catch (e) { return out; }
  if (!doc || !doc.documentElement || /parsererror/i.test(doc.documentElement.nodeName)) return out;
  const svg = doc.documentElement;
  if (!/svg/i.test(svg.nodeName)) return out;

  /* ── DISARM IT FIRST ───────────────────────────────────────────────────
     An SVG is not a picture. It is a document, and it may contain <script>,
     event-handler attributes, <foreignObject> with arbitrary HTML in it, and
     external references. The browser's own geometry engine is what makes
     this importer good, but using it means putting the file INTO the live
     page — and an imported <script> node has its "already started" flag
     clear, so it runs the instant it is inserted. In this shop's origin,
     with a signed-in customer's Firebase token in memory, that is not a
     rendering quirk; it is somebody else's code reading somebody's account.

     So nothing is inserted until it has been stripped: no scripts, no
     foreign content, no handlers, no external references, no stylesheet
     (an @import or a @font-face is a request to a host of somebody else's
     choosing, carrying this customer's address), and no animation that
     could move geometry out from under the measurement. Internal references
     — a gradient, a clip path, a #id on a <use> — are left alone, because
     those are how real artwork is built. */
  const live = document.importNode(svg, true);
  try {
    live.querySelectorAll(
      "script,foreignObject,iframe,object,embed,audio,video,animate,animateTransform," +
      "animateMotion,set,handler,style,link,use[href^='http'],use[href^='//']").forEach((n) => n.remove());
    const all = [live].concat(Array.prototype.slice.call(live.querySelectorAll("*")));
    all.forEach((el) => {
      Array.prototype.slice.call(el.attributes || []).forEach((a) => {
        const n = (a.name || "").toLowerCase();
        const v = String(a.value || "");
        /* every on* handler, and any reference that leaves this document */
        if (n.indexOf("on") === 0) { el.removeAttribute(a.name); return; }
        if ((n === "href" || n === "xlink:href" || n === "src") && !/^#/.test(v.trim())) {
          el.removeAttribute(a.name); return;
        }
        if (/^(javascript|data:text\/html)/i.test(v.trim().replace(/\s/g, ""))) { el.removeAttribute(a.name); return; }
        /* a style ATTRIBUTE can fetch too: fill:url(https://…) */
        if (n === "style" && /url\(\s*['"]?\s*(https?:)?\/\//i.test(v)) el.removeAttribute(a.name);
      });
    });
  } catch (e) { return out; }

  /* it has to be IN the document to be measured, and must never be seen */
  const host = document.createElement("div");
  host.setAttribute("aria-hidden", "true");
  host.style.cssText = "position:fixed;left:-99999px;top:0;width:1200px;height:1200px;opacity:0;pointer-events:none";
  live.setAttribute("width", "1000");
  live.setAttribute("height", "1000");
  host.appendChild(live);
  document.body.appendChild(host);

  try {
    const vb = (live.getAttribute("viewBox") || "").trim().split(/[\s,]+/).map(Number);
    const root = live.getCTM ? live.getCTM() : null;
    const nodes = live.querySelectorAll("path,rect,circle,ellipse,line,polyline,polygon,text,tspan,image");
    /* a <use> refers to a shape rather than drawing one, and the shape it
       refers to lives in <defs> where nothing is painted. Neither is walked:
       the definition would arrive at the wrong place and the reference would
       arrive twice. Illustrator only writes these for Symbols, so it is
       worth saying so rather than silently losing them. */
    if (live.querySelector("use")) out.warn.symbols = 1;
    const HIDDEN_HOST = "defs,clipPath,mask,symbol,marker,pattern";
    let points = 0;
    for (let i = 0; i < nodes.length && out.objs.length < VEC_MAX_OBJECTS; i++) {
      const el = nodes[i];
      try {
        const tag = el.tagName.toLowerCase();
        if (tag === "image") { out.warn.image = (out.warn.image || 0) + 1; continue; }
        if (el.closest && el.closest(HIDDEN_HOST)) continue;
        const cs = window.getComputedStyle(el);
        if (cs.display === "none" || cs.visibility === "hidden" || Number(cs.opacity) === 0) continue;
        const m = el.getCTM ? el.getCTM() : null;
        const to = (x, y) => {
          if (!m) return [x, y];
          return [m.a * x + m.c * y + m.e, m.b * x + m.d * y + m.f];
        };
        if (tag === "text" || tag === "tspan") {
          /* Illustrator wraps every run in a <tspan>, and textContent on the
             parent already includes it — so taking both imports the same
             words twice, exactly on top of each other, and the customer has
             to find and delete one of them. The outermost one wins. */
          if (tag === "tspan" && el.closest && el.closest("text")) continue;
          const s = (el.textContent || "").trim();
          if (!s) continue;
          const bb = el.getBBox();
          const p = to(bb.x, bb.y + bb.height * 0.5);
          out.texts.push({ s: s.slice(0, 240), x: p[0], y: p[1],
                           size: Math.max(1, bb.height) * (m ? Math.hypot(m.a, m.b) : 1) });
          continue;
        }
        if (typeof el.getTotalLength !== "function") continue;
        const len = el.getTotalLength();
        if (!(len > 0.01)) continue;
        /* one point every ~1.2 units of length, within sane bounds: enough to
           keep a curve curved without turning a logo into a million points */
        const steps = Math.max(8, Math.min(600, Math.round(len / 1.2)));
        const pts = [];
        for (let s = 0; s <= steps; s++) {
          const q = el.getPointAtLength((len * s) / steps);
          pts.push(to(q.x, q.y));
        }
        points += pts.length;
        if (points > VEC_MAX_POINTS) { out.warn.too_big = 1; break; }
        const fill = cs.fill && cs.fill !== "none" ? vecCssRgb(cs.fill) : null;
        const stroke = cs.stroke && cs.stroke !== "none" ? vecCssRgb(cs.stroke) : null;
        const closed = tag !== "line" && tag !== "polyline" && tag !== "path" ? true
                     : (tag === "path" ? /z\s*$/i.test(el.getAttribute("d") || "") : false);
        if (!fill && !stroke) continue;
        out.objs.push({
          kind: fill && stroke ? "both" : fill ? "fill" : "stroke",
          subs: [pts], fill: fill || [0, 0, 0], stroke: stroke || [0, 0, 0],
          w: parseFloat(cs.strokeWidth) || 1, closed,
        });
      } catch (e) { /* one element never stops the file */ }
    }
    /* every point above is in VIEWPORT coordinates, because that is what
       getCTM measures in — so the box has to be the viewBox mapped through
       the same matrix, translation included. A viewBox with a non-zero
       origin, or a preserveAspectRatio that centres the art, puts that
       translation in e/f, and dropping it slides the whole import. */
    if (vb.length === 4 && vb.every((n) => isFinite(n)) && vb[2] > 0 && vb[3] > 0) {
      const a = root ? root.a : 1, d = root ? root.d : 1;
      out.box = { x: root ? root.e : 0, y: root ? root.f : 0,
                  w: vb[2] * (a || 1), h: vb[3] * (d || 1) };
    }
  } catch (e) { /* nothing here may throw */ }
  host.remove();
  return out;
}
function vecCssRgb(v) {
  const m = /rgba?\(([^)]+)\)/.exec(String(v || ""));
  if (!m) return [0, 0, 0];
  const n = m[1].split(",").map((x) => parseFloat(x) || 0);
  if (n.length >= 4 && n[3] === 0) return null;
  return [(n[0] || 0) / 255, (n[1] || 0) / 255, (n[2] || 0) / 255];
}

/* ── the whole file, whatever it is ─────────────────────────────────────── */
async function vecParse(file) {
  let buf = new Uint8Array(await file.arrayBuffer());
  const warn = {};
  const note = (k) => { warn[k] = (warn[k] || 0) + 1; };
  /* .svgz, or anything else somebody gzipped on the way out of a design tool */
  if (isGzip(buf)) {
    const un = await vecGunzip(buf);
    if (un && un.length) buf = un;
  }
  let kind = vecSniff(buf, file.name);
  let objs = [], texts = [], box = null;

  if (kind === "svg") {
    const r = vecFromSvg(new TextDecoder().decode(buf));
    objs = r.objs; texts = r.texts; box = r.box;
    Object.keys(r.warn).forEach(note);
  } else if (kind === "pdf") {
    const ctx = vecPdfObjects(buf);
    ctx.bytes = buf; ctx.warn = note;
    if (/\/Encrypt\b/.test(ctx.s.slice(-4096)) || /\/Encrypt\s+\d+\s+\d+\s+R/.test(ctx.s)) note("encrypted");
    /* the page box, so the artwork lands where the designer placed it */
    const mb = /\/MediaBox\s*\[\s*([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)/.exec(ctx.s);
    if (mb) box = { x: Number(mb[1]), y: Number(mb[2]), w: Number(mb[3]) - Number(mb[1]), h: Number(mb[4]) - Number(mb[2]) };
    /* FIRST CHOICE: the page's own content streams, in page order. That is
       where the artwork legally lives, and reading only those is what stops
       an Illustrator file arriving twice — once as PDF, once as Illustrator's
       private copy of the same shapes. */
    const pageNums = vecPdfPageStreams(ctx);
    /* SECOND CHOICE, only if the page tree is unreadable: every stream that
       reads like artwork. A broken xref, an object stream, a linearised or
       repaired file all still land here. */
    const nums = pageNums.length ? pageNums
               : Array.from(ctx.map.keys()).sort((a, b) => a - b);
    if (!pageNums.length) note("no_pages");
    const seenObj = new Set();
    for (const n of nums) {
      if (objs.length >= VEC_MAX_OBJECTS) break;
      if (seenObj.has(n)) continue;
      seenObj.add(n);
      const obj = ctx.map.get(n);
      if (!obj || obj.dict.indexOf("stream") < 0) continue;
      if (vecIsFurniture(obj.dict)) continue;
      let str;
      try { str = await vecPdfStream(obj, ctx); } catch (e) { str = null; }
      if (!str || str.length < 8) continue;
      if (vecIsPrivate(str)) { note("private"); continue; }
      /* type is artwork too: a page that is only lettering has no m, re or
         cm anywhere in it, and used to be discarded as furniture */
      const head4 = str.slice(0, 4000);
      if (!/(^|[\s])(re|m)\s/.test(head4) && !/\scm\s/.test(head4) && !/\bBT\b/.test(head4)) continue;
      try {
        const got = vecRunContent(str, { onText: (t) => { texts = texts.concat(t); } });
        objs = objs.concat(got);
      } catch (e) { note("stream"); }
    }
    /* a page whose artwork sits inside a Form XObject — Illustrator writes
       these when the document has transparency groups */
    if (objs.length < 2) {
      for (const [n, obj] of ctx.map) {
        if (objs.length >= VEC_MAX_OBJECTS) break;
        if (seenObj.has(n) || !/\/Subtype\s*\/Form\b/.test(obj.dict)) continue;
        seenObj.add(n);
        let str;
        try { str = await vecPdfStream(obj, ctx); } catch (e) { str = null; }
        if (!str || str.length < 8 || vecIsPrivate(str)) continue;
        try {
          const got = vecRunContent(str, { onText: (t) => { texts = texts.concat(t); } });
          objs = objs.concat(got);
        } catch (e) { note("stream"); }
      }
    }
  } else if (kind === "eps" || kind === "epsbin") {
    let body = buf;
    if (kind === "epsbin") {
      /* a DOS EPS binary header: four little-endian offsets, the first of
         which is where the PostScript actually starts */
      const off = buf[4] | (buf[5] << 8) | (buf[6] << 16) | (buf[7] << 24);
      const len = buf[8] | (buf[9] << 8) | (buf[10] << 16) | (buf[11] << 24);
      if (off > 0 && off + len <= buf.length) body = buf.subarray(off, off + len);
    }
    const txt = vecLatin(body);
    const bb = /%%BoundingBox:\s*([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)/.exec(txt);
    if (bb) box = { x: Number(bb[1]), y: Number(bb[2]), w: Number(bb[3]) - Number(bb[1]), h: Number(bb[4]) - Number(bb[2]) };
    try { objs = vecRunContent(vecEpsContent(txt), {}); } catch (e) { note("stream"); }
  }

  /* no box, or a nonsense one? then the artwork's own bounds are the box */
  let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
  objs.forEach((o) => o.subs.forEach((sp) => sp.forEach((p) => {
    if (p[0] < x0) x0 = p[0];
    if (p[0] > x1) x1 = p[0];
    if (p[1] < y0) y0 = p[1];
    if (p[1] > y1) y1 = p[1];
  })));
  texts.forEach((t) => {
    if (t.x < x0) x0 = t.x;
    if (t.x > x1) x1 = t.x;
    if (t.y < y0) y0 = t.y;
    if (t.y > y1) y1 = t.y;
  });
  const haveArt = isFinite(x0) && x1 > x0 && y1 > y0;
  if (!box || !(box.w > 0 && box.h > 0) || (haveArt && (x1 - x0 > box.w * 3 || y1 - y0 > box.h * 3))) {
    box = haveArt ? { x: x0, y: y0, w: x1 - x0, h: y1 - y0 } : { x: 0, y: 0, w: 1, h: 1 };
  }
  return { kind, objs, texts, box, warnings: warn,
           flipY: kind !== "svg" };   /* PDF and PostScript measure upward */
}

/* ── turning artwork into layers ─────────────────────────────────────────
   Every path becomes an object of its own. The one decision worth naming is
   what a FILLED path means: under this studio's fill law an area is either
   engraved, cut through, or left as an outline, and nobody has said which
   yet. So a filled path arrives ENGRAVED — that is what a filled shape looks
   like, and it is the reading a designer expects — and a stroked path
   arrives as the stroke it is. Either can be changed afterwards with one
   click of the Fill tool, which is exactly why that tool now shows you the
   area before you commit. */
function vecToItems(parsed, opts) {
  const o = opts || {};
  const box = parsed.box;
  const s = 1 / Math.max(1e-6, Math.max(box.w, box.h));
  const ox = (1 - box.w * s) / 2, oy = (1 - box.h * s) / 2;
  const fx = (x) => (x - box.x) * s + ox;
  /* PDF and PostScript measure upward from the bottom-left; a canvas measures
     downward from the top-left. Flipping is a subtraction from the artwork's
     own height — and the same oy has to be added back afterwards, or every
     imported PDF sits hard against the top of the sheet instead of centred. */
  const fy = (y) => parsed.flipY ? (box.h * s - (y - box.y) * s) + oy
                                 : (y - box.y) * s + oy;
  const clamp = (v) => Math.round(Math.max(-0.5, Math.min(1.5, v)) * 1e4) / 1e4;
  const items = [];
  let points = 0, tooBig = 0, dropped = 0;
  const lum = (c) => (Array.isArray(c) && c.length >= 3)
    ? 0.299 * c[0] + 0.587 * c[1] + 0.114 * c[2] : 0;
  /* A WHITE FILL IS THE PAGE, NOT THE CHARM. Illustrator and every PDF
     writer put an opaque white artboard rectangle behind the artwork as a
     matter of course. Under this studio's fill law "filled" means ENGRAVED,
     so importing that rectangle faithfully means a solid engraved slab over
     the entire charm with the actual design buried under it. Near-white
     fills are therefore dropped, and only fills — a white STROKE is a
     deliberate line somebody drew, and it is kept. */
  const boxArea = Math.max(1e-9, box.w * box.h);
  const spans = (obj) => {
    /* how much of the page this path's bounding box covers */
    let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
    (obj.subs || []).forEach((sp) => sp.forEach((p) => {
      if (p[0] < x0) x0 = p[0];
      if (p[0] > x1) x1 = p[0];
      if (p[1] < y0) y0 = p[1];
      if (p[1] > y1) y1 = p[1];
    }));
    if (!isFinite(x0)) return 0;
    return ((x1 - x0) * (y1 - y0)) / boxArea;
  };
  /* TWO TESTS, NOT ONE. Colour alone is far too blunt: a reverse logo is
     white on purpose, and cream, ivory and pale grey are ordinary jewellery
     colours — dropping those loses the artwork and then reports the file as
     empty. An ARTBOARD is white AND the size of the page, and it is that
     second half that identifies it. */
  const isPaper = (obj) => lum(obj.fill) > 0.965 && spans(obj) > 0.9;

  (parsed.objs || []).forEach((obj) => {
    if (items.length >= VEC_MAX_OBJECTS || points > VEC_MAX_POINTS) return;
    try {
      const subs = (obj.subs || []).filter((sp) => sp.length >= 2);
      if (!subs.length) return;
      let wantsFill = obj.kind === "fill" || obj.kind === "both";
      if (wantsFill && isPaper(obj)) {
        /* white on white: if it was fill-and-stroke, the stroke is real and
           survives on its own; if it was only a fill, it was the page */
        if (obj.kind !== "both") { dropped++; return; }
        wantsFill = false;
      }
      /* ONE PATH CANNOT SPEND THE WHOLE BUDGET. The cap used to be tested
         only between objects, so a single Live-Traced outline — 120,000
         line segments in one compound path is an ordinary thing for a map
         or a detailed engraving — flattened in full and produced a megabyte
         of JSON from one layer, four times the nominal ceiling and by
         itself over what a Firestore document will hold. Long paths are
         thinned rather than refused: the shape survives, at a fidelity a
         12 mm charm cannot tell the difference from. */
      const thin = (sp) => {
        if (sp.length * 2 <= VEC_MAX_PATH_POINTS) return sp;
        tooBig = 1;
        const keep = Math.max(3, Math.floor(VEC_MAX_PATH_POINTS / 2));
        const step = sp.length / (keep - 1);
        const outp = [];
        for (let k = 0; k < keep - 1; k++) outp.push(sp[Math.floor(k * step)]);
        outp.push(sp[sp.length - 1]);      /* the closing point, exactly once */
        return outp;
      };
      if (wantsFill) {
        const flat = [], q = [];
        subs.forEach((raw) => {
          /* a fill's every subpath is a contour, painted even-odd, which is
             how a letter keeps the hole in its middle */
          const sp = thin(raw);
          q.push(sp.length);
          sp.forEach((p) => { flat.push(clamp(fx(p[0])), clamp(fy(p[1]))); });
        });
        points += flat.length;
        /* ── THE COLOUR IT WAS DRAWN IN IS AN INSTRUCTION ──────────────
           Every imported fill used to arrive marked ENGRAVE whatever colour
           it was, so a logo drawn with blue cut-outs opened as twenty-four
           layers and twenty-four of them engraved — the customer's own
           instruction thrown away at the door. The reserved blue and the
           reserved red are read as what they mean; every other colour is
           ink, and ink is engraved metal. */
        const fi = mkIntentFromColour(obj.fill);
        const fink = mkIntentInk(fi);
        items.push(mkPack({
          id: mkId(), t: "fill", c: fink, f: fi === "none" ? "none" : fink, w: 0.0015, gr: 1,
          p: flat, q, n: (o.name || "Shape") + " " + (items.length + 1),
        }));
      } else {
        subs.forEach((raw) => {
          if (items.length >= VEC_MAX_OBJECTS || points > VEC_MAX_POINTS) return;
          const sp = thin(raw);
          const flat = [];
          sp.forEach((p) => { flat.push(clamp(fx(p[0])), clamp(fy(p[1]))); });
          points += flat.length;
          items.push(mkPack({
            id: mkId(), t: "ink", c: MK_ENGRAVE, f: "none",
            w: Math.max(0.002, Math.min(0.06, (obj.w || 1) * s)),
            p: flat, n: (o.name || "Line") + " " + (items.length + 1),
          }));
        });
      }
    } catch (e) { /* one path never stops the file */ }
  });

  (parsed.texts || []).slice(0, 40).forEach((t) => {
    try {
      if (!t.s || !String(t.s).trim()) return;
      items.push(mkPack({
        id: mkId(), t: "label", c: MK_ENGRAVE, f: "none", ff: "sans",
        s: String(t.s).slice(0, 240),
        p: [clamp(fx(t.x)), clamp(fy(t.y))],
        fs: Math.max(0.012, Math.min(0.4, (t.size || 12) * s)),
        n: "Type · " + String(t.s).slice(0, 18),
      }));
    } catch (e) {}
  });
  /* what the caller has to be able to tell the customer */
  if (o.notes) {
    if (tooBig) o.notes.simplified = 1;
    if (dropped) o.notes.paper = dropped;
    if (points > VEC_MAX_POINTS || items.length >= VEC_MAX_OBJECTS) o.notes.too_big = 1;
  }
  return items;
}

/* ── the vector import ───────────────────────────────────────────────────── */
async function vecImport(file) {
  if (!__mk) { toast("Open a drawing first", "err"); return false; }
  if (file.size > VEC_MAX_BYTES) {
    toast(`That file is ${Math.round(file.size / 1048576)} MB, which is more than we can open here. ` +
          "Send us a smaller version and we'll take it from there.", "err");
    return false;
  }
  /* a customer has an SVG, not an "SVGZ" — the gzip is our business, and a
     badge nobody recognises is worse than no badge */
  const raw = (String(file.name).match(/\.([a-z0-9]+)$/i) || [, "file"])[1].toLowerCase();
  const label = (raw === "svgz" ? "svg" : raw === "ps" ? "eps" : raw).toUpperCase();
  toast(`Opening “${file.name.slice(0, 26)}”…`, "gold");

  let parsed;
  try { parsed = await vecParse(file); }
  catch (e) {
    console.warn("[studio] vector read failed:", (e && e.message) || e);
    toast(`We couldn't read that file. If it came from Illustrator, save it again with ` +
          `“Create PDF Compatible File” ticked and we'll get it.`, "err");
    return false;
  }
  if (!parsed.kind) {
    toast("That file isn't a vector document this studio recognises.", "err");
    return false;
  }
  const base = String(file.name).replace(/\.[a-z0-9]+$/i, "").slice(0, 30) || "Artwork";
  let items = [];
  const notes = parsed.warnings || {};
  try { items = vecToItems(parsed, { name: base, notes }); } catch (e) { items = []; }

  if (!items.length) {
    const w = parsed.warnings || {};
    toast(w.encrypted
      ? "That file is password-protected, so its artwork can't be read."
      : (parsed.kind === "pdf" || parsed.kind === "eps")
        ? `There were no shapes in that file we could open. Illustrator hides them ` +
          `unless “Create PDF Compatible File” is ticked when you save — tick it and try ` +
          `again, or save as SVG instead.`
        : "That file has no shapes this studio can use.", "err");
    return false;
  }

  mkPushUndo();
  /* one file, one group: it arrives as a single thing you can move, and
     comes apart the moment you want it to */
  const gid = "g" + Math.random().toString(36).slice(2, 8);
  if (!__mk.groups) __mk.groups = {};
  __mk.groups[gid] = { n: base + " · " + label, col: 0 };
  const made = [];
  items.forEach((it) => {
    it.g = gid;
    __mk.items.push(it);
    made.push(__mk.items.length - 1);
  });
  mkSelSet(made);
  mkTouch();
  mkRenderLayers();

  /* and into the repository, badged with what it came from */
  try { await impRegister(file, label.toLowerCase(), items); } catch (e) {}

  const bits = [];
  const nf = items.filter((i) => i.t === "fill").length;
  const nl = items.filter((i) => i.t === "ink").length;
  const nt = items.filter((i) => i.t === "label").length;
  if (nf) bits.push(`${nf} filled shape${nf === 1 ? "" : "s"}`);
  if (nl) bits.push(`${nl} line${nl === 1 ? "" : "s"}`);
  if (nt) bits.push(`${nt} piece${nt === 1 ? "" : "s"} of type`);
  toast(`“${file.name.slice(0, 22)}” opened — ${bits.join(", ")}, all editable.`, "gold");
  const said = vecWarnings(parsed.warnings, nf);
  if (said) setTimeout(() => toast(said, "note"), 2800);
  return true;
}
/* Same rule as psdWarnings: one sentence, the one that matters most. The
   fill note comes last because it is the least alarming and the easiest to
   act on — and it is only worth saying at all if something was filled. */
function vecWarnings(w, filled) {
  w = w || {};
  if (w.encrypted) return "Part of that file is password-protected, so some of it couldn't be opened.";
  if (w.too_big) return "That artwork was very detailed, so we brought across as much as a charm can carry.";
  if (w.simplified) return "One shape was extremely detailed — we smoothed it to something we can cut.";
  if (w.symbols) return "Anything you'd made into a Symbol didn't come across. Expand those and save again to bring them in.";
  if (w.image) return "Photos placed inside the artwork didn't come across — add those separately.";
  if (w.paper) return "The white page behind your artwork was left out, so only the design came across.";
  if (w.filter) return "A little of that artwork was stored in a form we can't read, and was left out.";
  if (filled) return "Filled shapes came in marked to be engraved — pick Cut out on the bar and tap one to change that.";
  return "";
}

/* ═════════════════════════════════════════════════════════════════════════
   THE REPOSITORY, AND ONE DOOR FOR EVERY DESIGN FILE
   ====================================================================== */

/* An imported document earns its own entry in the global library: a preview
   painted from the objects themselves, badged with the format it came from,
   and — crucially — carrying every one of those objects with it. Dragging it
   back out of the repository into another design brings the whole thing,
   still editable, rather than a flat picture of it. */
/* how many bytes of the session document the repository may hold. A session
   is ONE Firestore document with a hard 1 MB ceiling and the customer's
   whole design already in it; imported artwork is a guest there, and a guest
   with an appetite — three detailed logos measured 1.02 MB between them, at
   which point flushSession throws for ever and every later edit is lost with
   nothing but a recurring "isn't syncing" to show for it. So the repository
   is kept to a budget, oldest imports retired first, exactly like the undo
   history is. */
const IMP_BUDGET = 260 * 1024;

/* ONE ENTRY CANNOT EXCEED THE WHOLE BUDGET EITHER. Retiring older imports
   does nothing if the new one is by itself too big, and a perfectly legal
   import — 900 paths at the point ceiling — measured 561 KB against a 260 KB
   budget. So the stored copy is coarsened until it fits: the same shapes,
   the same structure, the same names, sampled more loosely. What is on the
   SHEET keeps its full fidelity; this is only the copy the repository holds
   so the document can be dropped into another design later, and no charm
   12 mm across can tell the difference. */
function impShrink(items) {
  let out = items.map(mkPack);
  for (let pass = 0; pass < 6; pass++) {
    let bytes = 0;
    try { bytes = JSON.stringify(out).length; } catch (e) { return out; }
    if (bytes <= IMP_BUDGET) return out;
    out = out.map((it) => {
      const p = it.p;
      if (!Array.isArray(p) || p.length < 24 || it.t === "img" || it.t === "label") return it;
      const q = Array.isArray(it.q) ? it.q.slice() : null;
      const half = [], nq = [];
      let at = 0;
      const runs = q && q.length ? q : [p.length / 2];
      runs.forEach((len) => {
        const n = Math.max(3, Math.floor(len / 2));
        const step = len / n;
        let kept = 0;
        for (let k = 0; k < n; k++) {
          const src = at + Math.floor(k * step) * 2;
          half.push(p[src], p[src + 1]);
          kept++;
        }
        nq.push(kept);
        at += len * 2;
      });
      const next = Object.assign({}, it, { p: half });
      if (q) next.q = nq;
      return next;
    });
  }
  return out;
}

function impTrim() {
  const keys = Object.keys(state.markups || {})
    .filter((k) => k.indexOf("imp:") === 0)
    .sort((a, b) => (Number((state.markups[a] || {}).impAt) || 0) -
                    (Number((state.markups[b] || {}).impAt) || 0));
  const size = (k) => { try { return JSON.stringify(state.markups[k]).length; } catch (e) { return 0; } };
  let total = keys.reduce((n, k) => n + size(k), 0);
  let dropped = 0;
  /* never retire the one just added — it is what the customer is looking at */
  while (total > IMP_BUDGET && keys.length > 1) {
    const k = keys.shift();
    total -= size(k);
    delete state.markups[k];
    if (typeof forgetKey === "function") forgetKey("markups", k);
    dropped++;
  }
  return dropped;
}

async function impRegister(file, kindLabel, items, preview) {
  ensureSession();
  const key = "imp:" + Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
  if (!state.markups) state.markups = {};
  /* THE OBJECTS ARE THE ENTRY. The document is stored the same way a sketch
     made in this studio is stored — as its layers — which is why dragging it
     back out of the repository later brings the artwork, still editable,
     rather than a flat picture of it.

     NO DATA URL EVER GOES IN HERE. A session is one Firestore document with
     a hard 1 MB ceiling, and a single base64 thumbnail is tens of kilobytes;
     a repository of them would silently stop saving somebody's work. Vector
     art needs no stored picture at all — its tile is drawn from these very
     objects, for nothing. A Photoshop file, whose layers ARE pictures, gets
     one small composite uploaded to storage like every other image. */
  state.markups[key] = {
    items: impShrink(items), mode: "interpret", crop: [], baseRot: 0,
    guides: 0, groups: [], traced: 0, dirty: 0, updatedAt: now(),
    impName: String(file && file.name || "Artwork").slice(0, 60),
    impSrc: String(kindLabel || "").toLowerCase().slice(0, 8),
    impAt: now(),
    impUrl: (preview && preview.url) || "",
    impPath: (preview && preview.path) || "",
    /* every file this import put in storage, so deleting the entry can take
       them with it — a Photoshop document uploads one PNG per layer, and
       without this list they would outlive the thing that referenced them */
    impFiles: items.map((i) => i && i.sp).filter(Boolean).slice(0, 60),
  };
  const retired = impTrim();
  if (retired) {
    toast(`Your library keeps the ${retired === 1 ? "most recent" : "most recent"} design files you open — ` +
          `${retired === 1 ? "the oldest one has" : "the oldest " + retired + " have"} made room for this one. ` +
          "Anything you've placed on a sheet stays where it is.", "note");
  }
  saveSession();
  if (typeof mkLibRefresh === "function") mkLibRefresh();
  return key;
}

/* A Photoshop document's layers are pictures, so its tile has to be one too.
   It is composed here from the layer bitmaps we already hold in memory —
   untainted, because this studio drew every one of them — and uploaded once,
   as a single small file, rather than stored inline. */
async function impPsdPreview(sessId, name, shots) {
  try {
    if (!shots || !shots.length) return null;
    const S = 320;
    const cv = document.createElement("canvas");
    cv.width = S; cv.height = S;
    const ctx = cv.getContext("2d");
    ctx.fillStyle = "#ffffff";
    ctx.fillRect(0, 0, S, S);
    for (const sh of shots) {
      try {
        const im = await new Promise((res, rej) => {
          const i = new Image();
          i.onload = () => res(i);
          i.onerror = () => rej(new Error("img"));
          i.src = sh.dataUrl;
        });
        ctx.globalAlpha = Math.max(0.05, Math.min(1, sh.o == null ? 1 : sh.o));
        ctx.drawImage(im, sh.x * S, sh.y * S, sh.w * S, sh.h * S);
      } catch (e) { /* one layer never costs the thumbnail */ }
    }
    ctx.globalAlpha = 1;
    return await mkUploadPicture(sessId, (name || "document") + "-preview.png", cv.toDataURL("image/png"))
      .then((r) => (r && r.up) || null);
  } catch (e) { return null; }
}

/* ── a design file where only a PICTURE will do ───────────────────────────
   Step one uploads ONE reference image, which the workshop's AI reads. A
   .psd or .ai cannot be that image, but there is no reason a customer should
   have to know it: the document is flattened here, faithfully, and the
   picture that comes out is uploaded in its place. Photoshop's own composite
   is used when there is one — nobody renders a Photoshop file better than
   Photoshop did — and vector art is drawn from its paths at a size worth
   sending. Returns a real PNG File, or null if this was never a design file. */
async function designFlatten(file) {
  if (!file) return null;
  /* THE SAME CEILINGS THE OTHER TWO DOORS ENFORCE. psdImport and vecImport
     both refuse an absurd file before reading it; this path did not, and it
     is the one on the shop's front page — so a 2 GB .psd dropped on the
     step-one dropzone went straight into arrayBuffer() with nothing in the
     way. readUpload's own size check runs afterwards, on the PNG that comes
     out, which is far too late to be any help. */
  const cap = isPsdFile(file) ? PSD_MAX_BYTES : VEC_MAX_BYTES;
  if (Number(file.size) > cap) return { tooBig: Math.round(file.size / 1048576) };
  const S = 1024;
  const finish = (cv) => new Promise((res) => {
    try {
      cv.toBlob((b) => {
        if (!b) return res(null);
        const base = String(file.name).replace(/\.[a-z0-9]+$/i, "") || "artwork";
        res(new File([b], base + ".png", { type: "image/png" }));
      }, "image/png");
    } catch (e) { res(null); }
  });

  if (isPsdFile(file)) {
    let lib;
    try { lib = await loadPsdLib(); } catch (e) { return null; }
    try {
      /* the composite this time — it IS the picture we want */
      const psd = lib.readPsd(await file.arrayBuffer(), {
        skipLayerImageData: true, skipThumbnail: true, skipLinkedFilesData: true,
        throwForMissingFeatures: false, logMissingFeatures: false,
      });
      const src = psd && (psd.canvas || psd.imageData);
      if (!src) return null;
      const w = src.width, h = src.height;
      if (!(w > 0 && h > 0)) return null;
      const k = Math.min(1, S / Math.max(w, h));
      const cv = document.createElement("canvas");
      cv.width = Math.max(1, Math.round(w * k));
      cv.height = Math.max(1, Math.round(h * k));
      const ctx = cv.getContext("2d");
      /* the AI reads a photograph, and a photograph has no transparency —
         so the checkerboard becomes white rather than black */
      ctx.fillStyle = "#ffffff";
      ctx.fillRect(0, 0, cv.width, cv.height);
      if (psd.canvas) ctx.drawImage(psd.canvas, 0, 0, cv.width, cv.height);
      else {
        const t = document.createElement("canvas");
        t.width = w; t.height = h;
        t.getContext("2d").putImageData(src, 0, 0);
        ctx.drawImage(t, 0, 0, cv.width, cv.height);
      }
      return await finish(cv);
    } catch (e) { return null; }
  }

  let head;
  try { head = new Uint8Array(await file.slice(0, 1024).arrayBuffer()); } catch (e) { return null; }
  if (!isVectorFile(file) && !vecSniff(head, file.name)) return null;
  try {
    const parsed = await vecParse(file);
    if (!parsed.kind) return null;
    const items = vecToItems(parsed, { name: "Shape" });
    if (!items.length) return null;
    const cv = document.createElement("canvas");
    cv.width = S; cv.height = S;
    const ctx = cv.getContext("2d");
    ctx.fillStyle = "#ffffff";
    ctx.fillRect(0, 0, S, S);
    mkPaint(ctx, S, S, items.map(mkPack), { export: true });
    return await finish(cv);
  } catch (e) { return null; }
}

/* ONE DOOR. Every place in the studio that accepts a file sends it here, and
   the router decides by CONTENT rather than by the name — a .psd renamed to
   .png still opens as a Photoshop document, and a .ai that is really a PDF
   is read as one. Nothing else in the app needs to know any of this. */
async function designFileImport(file, cx, cy) {
  if (!file) return false;
  try {
    if (isPsdFile(file)) { await psdImport(file, cx, cy); return true; }
    if (isVectorFile(file)) return await vecImport(file);
    /* named like an ordinary picture, but is it? the first bytes decide */
    const head = new Uint8Array(await file.slice(0, 1024).arrayBuffer());
    const sniff = vecSniff(head, file.name);
    if (sniff) return await vecImport(file);
    if (head[0] === 0x38 && head[1] === 0x42 && head[2] === 0x50 && head[3] === 0x53) {
      await psdImport(file, cx, cy);     /* "8BPS" — a Photoshop file in disguise */
      return true;
    }
  } catch (e) {
    console.warn("[studio] import router:", (e && e.message) || e);
    toast("That file couldn't be opened.", "err");
    return true;                          /* handled — do not fall through */
  }
  return false;                           /* an ordinary picture; not ours */
}

/* every accept attribute in the app, written once */
const DESIGN_ACCEPT = ".psd,.ai,.pdf,.svg,.svgz,.eps,.ps,image/vnd.adobe.photoshop,application/pdf,application/postscript,image/svg+xml";
const IMAGE_ACCEPT = "image/jpeg,image/png,.jpg,.jpeg,.png";

(function wireDesignFiles() {
  /* the dedicated picker, for people who go looking in the menu */
  const input = document.getElementById("mkDesignInput");
  document.addEventListener("click", (e) => {
    /* a synthetic click can carry document or window as its target, and
       neither has closest() — this listener runs on EVERY click in the shop */
    if (!e.target || typeof e.target.closest !== "function") return;
    if (e.target.closest('.mk-item[data-pic="design"]')) {
      mkCloseAllDrops();
      if (input) input.click();
    }
  });
  if (input) {
    input.accept = DESIGN_ACCEPT;
    input.addEventListener("change", async (e) => {
      const el = e.target;
      const files = Array.from(el.files || []);
      try { for (const f of files.slice(0, 4)) await designFileImport(f); }
      finally { el.value = ""; }
    });
  }
  /* and every ordinary picture picker in the studio accepts them too, so
     nobody has to know which menu item was the right one */
  const widen = (el, base) => {
    if (!el) return;
    const have = (el.accept || base).split(",").map((s) => s.trim()).filter(Boolean);
    DESIGN_ACCEPT.split(",").forEach((x) => { if (have.indexOf(x) < 0) have.push(x); });
    el.accept = have.join(",");
  };
  widen(document.getElementById("mkFileInput"), IMAGE_ACCEPT);
  widen(document.getElementById("fileInput"), IMAGE_ACCEPT);
})();

/* ── test surface ─────────────────────────────────────────────────────────
   The awkward parts of both formats are pure functions on purpose, so the
   harness can drive them directly with files that would be very hard to
   produce by hand through the interface. */
window.__isPsdFile      = isPsdFile;
window.__isVectorFile   = isVectorFile;
window.__isDesignFile   = isDesignFile;
window.__psdPlan        = psdPlan;
window.__psdFit         = psdFit;
window.__psdColour      = psdColour;
window.__psdWarnings    = psdWarnings;
window.__psdImport      = psdImport;
window.__vecSniff       = vecSniff;
window.__vecTokens      = vecTokens;
window.__vecRunContent  = vecRunContent;
window.__vecEpsContent  = vecEpsContent;
window.__vecPdfObjects  = vecPdfObjects;
window.__vecUn85        = vecUn85;
window.__vecUnhex       = vecUnhex;
window.__vecFromSvg     = vecFromSvg;
window.__vecParse       = vecParse;
window.__vecToItems     = vecToItems;
window.__vecImport      = vecImport;
window.__vecWarnings    = vecWarnings;
window.__designFileImport = designFileImport;
window.__designFlatten  = designFlatten;
window.__loadPsdLib     = loadPsdLib;
window.__impRegister    = impRegister;
window.__DESIGN_ACCEPT  = DESIGN_ACCEPT;


/* =============================================================================
   MY DESIGNS — drawer list → full session view → seamless continue.
   Backed by the customSessions listener, so the list is live and cross-device.
   ========================================================================== */
function sessionThumb(s) {
  const vs = s.versions || [];
  const v = vs[s.currentVersion] || vs[vs.length - 1];
  /* the charm when it exists, the drawing while it doesn't */
  if (v) return versionMedia({ url: v.renderUrl || v.url });
  if (s.reference && s.reference.type === "catalog")
    return `<span class="zoom-charm" style="display:block;width:100%;height:100%;">${productMedia(s.reference.item, "eager", 260)}</span>`;
  if (s.reference && s.reference.type === "upload")
    return `<img src="${attrText(s.reference.url)}" alt="">`;
  /* A sketch saved with Save & Close has no uploaded picture — nothing was
     ever uploaded — so the drawing is painted from its own objects rather
     than shown as the black square with a question mark, which read as
     "your work is gone" when it never was. */
  const drawn = typeof mkSketchThumb === "function" ? mkSketchThumb(s, "draw", 200) : "";
  if (drawn) return `<img src="${attrText(drawn)}" alt="">`;
  return `<svg viewBox="0 0 100 100"><rect width="100" height="100" fill="#000"/><text x="50" y="58" text-anchor="middle" fill="#cdb98a" font-size="30" font-family="Georgia">?</text></svg>`;
}
function statusChip(st) {
  const cls = st === "approved" ? " status-chip--approved" : st === "ordered" ? " status-chip--ordered" : "";
  return `<span class="status-chip${cls}">${escapeHtml(st || "draft")}</span>`;
}
function renderDrawer() {
  const body = $("#drawerBody");
  const list = [...state.sessions].sort((a, b) => asDate(b.updatedAt) - asDate(a.updatedAt));
  if (!list.length) {
    body.innerHTML = `<div class="drawer-empty">Nothing here yet.<br>Every design you start is saved automatically —<br>come back and continue any time.</div>`;
    return;
  }
  body.innerHTML = list.map((s) => {
    const nv = (s.versions || []).length;
    const nl = (typeof mkItemsOf === "function" ? mkItemsOf((s.markups || {}).draw).length : 0);
    /* a draft with no versions still has a size to it — say what IS there
       rather than "0 versions", which reads as "nothing" */
    const what = nv ? `${nv} version${nv === 1 ? "" : "s"}`
                    : (nl ? `sketch · ${nl} object${nl === 1 ? "" : "s"}` : "not started");
    return `
    <div class="saved-item" data-sid="${attrText(s.id)}" title="Open this design">
      <span class="thumb">${sessionThumb(s)}</span>
      <span style="flex:1; min-width:0;">
        <span class="nm">${escapeHtml(s.name || "Untitled design")}${s.id === state.activeSessionId ? ' <span style="color:var(--bj-gold); font-size:11px;">· current</span>' : ""}</span>
        <span class="st">${statusChip(s.status)} &nbsp;${what} · ${tFmt(s.updatedAt)}</span>
      </span>
      <span class="open-hint">Open →</span>
      <button class="saved-kill" type="button" data-kill="${attrText(s.id)}"
              title="Delete this design for good" aria-label="${attrText("Delete " + (s.name || "this design") + " permanently")}">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M4 6.5h16"/><path d="M9 6.5V4h6v2.5"/><path d="M6.5 6.5 7.5 20h9l1-13.5"/></svg>
      </button>
    </div>`; }).join("");
  $$("#drawerBody .saved-item").forEach((el) => el.addEventListener("click", (e) => {
    if (e.target.closest(".saved-kill")) return;
    openSessionView(el.dataset.sid);
  }));
  $$("#drawerBody .saved-kill").forEach((b) => b.addEventListener("click", async (e) => {
    e.stopPropagation();
    await confirmDeleteDesign(b.dataset.kill);
  }));
}
$("#myDesignsBtn").addEventListener("click", () => {
  renderDrawer(); $("#designsDrawer").classList.add("is-open");
});
$("#drawerClose").addEventListener("click", () => $("#designsDrawer").classList.remove("is-open"));

/* One door for "delete this design", used by the drawer row and by the
   design's own page, so the wording and the safety rail are the same in
   both places. */
async function confirmDeleteDesign(sid) {
  const s = (state.sessions || []).find((x) => x.id === sid);
  if (!s) return false;
  const nv = (s.versions || []).length;
  const nl = typeof mkItemsOf === "function" ? mkItemsOf((s.markups || {}).draw).length : 0;
  const bits = [];
  if (nv) bits.push(`${nv} version${nv === 1 ? "" : "s"}`);
  if (nl) bits.push(`a sketch of ${nl} object${nl === 1 ? "" : "s"}`);
  if ((s.thread || []).length) bits.push("the whole conversation");
  const said = bits.join(", ");
  const yes = await askConfirm({
    title: `Delete “${s.name || "this design"}”?`,
    body: (said ? said.charAt(0).toUpperCase() + said.slice(1) + " — all of it goes. " : "")
      + "This cannot be undone."
      + (s.status === "ordered" ? " Your order is not affected." : ""),
    yes: "Delete for good",
  });
  if (!yes) return false;
  const gone = await deleteDesign(sid);
  if (gone) toast("Design deleted", "gold");
  return gone;
}

const TL_ICONS = { created:"✦", reference:"⌕", upload:"⬆", instructions:"✎", metal:"◐", generate:"✦", refine:"↻", approve:"♥", order:"✓" };
function openSessionView(sid) {
  const s = state.sessions.find((x) => x.id === sid); if (!s) return;
  $("#designsDrawer").classList.remove("is-open");
  const vs = s.versions || [], hist = s.history || [], thread = s.thread || [];
  const v = vs[s.currentVersion] || vs[vs.length - 1];
  const refHtml = !s.reference
    ? `<div class="ctx-ref"><span class="nm" style="color:var(--bj-muted); font-weight:400; font-style:italic;">Designed from words alone</span></div>`
    : s.reference.type === "catalog"
      ? `<div class="ctx-ref"><span class="thumb zoom-charm">${productMedia(s.reference.item, "eager", 260)}</span>
           <span><span class="nm">${escapeHtml(s.reference.item.title)}</span><br>
           <a class="text-link" href="${attrText(listingUrl(s.reference.item))}" target="_blank" rel="noopener">View listing ↗</a></span></div>`
      : `<div class="ctx-ref"><span class="thumb"><img src="${attrText(s.reference.url)}" alt=""></span>
           <span><span class="nm">${escapeHtml(s.reference.name)}</span><br>
           <span style="font-size:11px; color:var(--bj-muted);">Your uploaded image</span></span></div>`;
  const tl = hist.map((h) => `
    <div class="tl-item">
      <span class="tl-ico">${TL_ICONS[h.type] || "·"}</span>
      <span class="tl-body">
        <div class="tl-txt">${escapeHtml(h.label)}</div>
        <div class="tl-time">${tFmt(h.at)}</div>
      </span>
      ${h.vn && vs[h.vn - 1] ? `<span class="tl-thumb">${versionMedia(vs[h.vn - 1])}</span>` : ""}
    </div>`).join("");
  const msgs = thread.filter((m) => m.role === "me").length;
  $("#sessionBody").innerHTML = `
    <div class="panel-head" style="margin-bottom:20px;">
      <span class="bj-eyebrow">Saved design · ${statusChip(s.status)}</span>
      <h2 style="position:relative; padding-bottom:18px;">${escapeHtml(s.name || "Untitled design")}</h2>
    </div>
    <div class="sv-grid">
      <div>
        <div class="sv-stage">
          ${s.approved ? '<div class="stage-flag stage-flag--approved is-visible">✓ Approved</div>' : ""}
          ${v ? versionMedia({ url: v.renderUrl || v.url }) : '<div class="stage-empty" style="display:flex;align-items:center;justify-content:center;height:100%;"><p>No preview generated yet</p></div>'}
        </div>
        <div class="sv-versions">${vs.map((ver, i) => `
          <span class="version-thumb${i === s.currentVersion ? " is-current" : ""}" style="cursor:default;">${versionMedia({ url: ver.renderUrl || ver.url })}${ver.renderUrl ? '<span class="v-gold" title="Rendered in metal"></span>' : ""}<span class="v-num">v${ver.n}</span></span>`).join("")}
        </div>
      </div>
      <div class="sv-context">
        <div class="ctx-block"><label>Reference</label>${refHtml}</div>
        <div class="ctx-block"><label>Instructions</label>
          <div class="tl-txt">${(s.desc || "").trim() ? `“${escapeHtml(s.desc.trim())}”` : "<i>None yet</i>"}</div>
          <div class="tl-time" style="margin-top:6px;">${vs.length} version${vs.length === 1 ? "" : "s"} · ${msgs} message${msgs === 1 ? "" : "s"} in the conversation</div>
        </div>
        <div class="ctx-block"><label>Full history</label>
          <div class="timeline">${tl || '<div class="tl-txt"><i>Just getting started</i></div>'}</div>
        </div>
      </div>
    </div>
    <div class="sv-actions">
      <button class="btn btn--gold" id="svContinueBtn" type="button">${s.status === "draft" ? "Continue designing →" : "Reopen this design →"}</button>
      ${s.approved ? '<button class="btn" id="svOrderBtn" type="button">Order this charm</button>' : ""}
      <button class="btn btn--danger" id="svDeleteBtn" type="button">Delete this design</button>
    </div>
    <p class="field-hint" style="text-align:center; margin-top:14px;">Everything is saved — reference, your words, every version. Continue exactly where you left off.</p>`;
  attachZoomPan(ROOT.querySelector("#sessionBody .sv-stage"));
  $("#svContinueBtn").addEventListener("click", () => loadSession(sid));
  const ob = $("#svOrderBtn");
  /* the order step is waypoint 3 — the prototype's `gotoStep(4)` predated the
     Describe/Preview merge and had no #view-4 to show */
  if (ob) ob.addEventListener("click", () => { loadSession(sid); gotoStep(3, true); });
  $("#svDeleteBtn").addEventListener("click", async () => {
    const gone = await confirmDeleteDesign(sid);
    if (gone) $("#sessionModal").classList.remove("is-open");
  });
  $("#sessionModal").classList.add("is-open");
}

/* =============================================================================
   VISUAL LIBRARY — Shopify Files URLs handed down from section settings,
   served through Shopify's image transforms so each surface pulls only the
   size it needs (porting rule 5).
   ========================================================================== */
function renderPictos() {
  const media = {
    search: [ASSETS.startSearch, "A curated selection of real Brites charm designs"],
    upload: [ASSETS.startUpload, "A reference sketch becoming a handcrafted wolf charm"],
    /* No CDN asset for the sketchpad card, and none should be required: a new
       section setting would ship blank on Paul's live theme and leave a hole
       in the grid. This one draws itself. */
    draw: [null, "A charm being sketched by hand on a sheet of paper"],
  };
  const INLINE_PICTO = {
    draw:
      '<svg class="picto-draw" viewBox="0 0 240 180" role="img" aria-label="A charm being sketched by hand">' +
      '<rect width="240" height="180" fill="#f7f5f1"/>' +
      '<g fill="none" stroke="#cfc7ba" stroke-width="1">' +
      '<path d="M0 30h240M0 60h240M0 90h240M0 120h240M0 150h240"/>' +
      '<path d="M30 0v180M60 0v180M90 0v180M120 0v180M150 0v180M180 0v180M210 0v180"/></g>' +
      '<g fill="none" stroke="#1c1d1d" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round">' +
      '<path d="M120 128c-26-19-44-34-44-53a22 22 0 0 1 44-11 22 22 0 0 1 44 11c0 19-18 34-44 53Z"/>' +
      '<circle cx="120" cy="47" r="6"/></g>' +
      '<path d="M120 108c-14-11-24-20-24-31" fill="none" stroke="#a58a52" stroke-width="2" stroke-linecap="round" stroke-dasharray="5 6"/>' +
      '<g fill="none" stroke="#1c1d1d" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">' +
      '<path d="m171 139 34-34a7.5 7.5 0 0 1 10.6 10.6l-34 34-14 3.4Z"/><path d="m199 111 10.6 10.6"/></g></svg>',
  };
  $$(".picto[data-picto]").forEach((el) => {
    const pair = media[el.dataset.picto] || media.search;
    if (!pair[0]) { el.innerHTML = INLINE_PICTO[el.dataset.picto] || ""; return; }
    el.innerHTML = assetImg(sized(pair[0], 720), pair[1], "lazy");
  });
  $$("[data-asset]").forEach((el) => {
    const src = ASSETS[el.dataset.asset];
    if (!src) { el.innerHTML = ""; return; }
    el.innerHTML = assetImg(sized(src, 720), el.dataset.alt || "", "eager");
  });
}
function renderHeroShow() {
  const host = $("#heroShow"); if (!host) return;
  if (!ASSETS.hero) { host.style.display = "none"; return; }
  host.innerHTML = `<div class="hero-story">${assetImg(
    sized(ASSETS.hero, 1800),
    "Brites bunny, hummingbird and sun designs shown from pencil sketch to finished flat charm",
    "eager")}</div>`;
}

/* =============================================================================
   FAQ                                                  [prototype, verbatim]
   ========================================================================== */
const FAQ = [
  ["Do I pay anything to try it?", "No — " + CONFIG.guestFreeCredits + " free designs for everyone, " + CONFIG.signupBonusCredits + " more with a free account."],
  ["What happens when I approve?", "The design is locked and saved to your account. It costs nothing and orders nothing."],
  ["What can I upload?", "Pets, drawings, tattoos, logos, sketches — anything that's yours to use."],
  ["Will the real charm match the preview?", "Yes — same flat cut, hoop and engraving rules our workshop uses. If it doesn't, we remake or refund."],
  ["Can I add a name?", "Yes — engraving is added when you order, just like our regular charms."],
];
function renderFaq() {
  $("#faqList").innerHTML = FAQ.map(([q, a], i) => `
    <div class="faq-item" data-i="${i}">
      <button class="faq-q" type="button">${escapeHtml(q)}<span class="chev">▾</span></button>
      <div class="faq-a"><p>${escapeHtml(a)}</p></div>
    </div>`).join("");
  $$(".faq-item").forEach((item) => {
    item.querySelector(".faq-q").addEventListener("click", () => {
      const open = item.classList.toggle("is-open");
      const a = item.querySelector(".faq-a");
      a.style.maxHeight = open ? a.scrollHeight + "px" : "0";
    });
  });
}

/* =============================================================================
   MODAL PLUMBING + INIT
   ========================================================================== */
$$("[data-close]").forEach((el) => el.addEventListener("click", () => {
  const m = $("#" + el.dataset.close);
  if (m) m.classList.remove("is-open");
  if (el.dataset.close === "authModal") closeVerifyStage();
}));
document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") {
    $$(".modal.is-open").forEach((m) => m.classList.remove("is-open"));
    $("#designsDrawer").classList.remove("is-open");
  }
});


/* =============================================================================
   STUDIO MENU
   One floating control, one panel from the right. The controls inside it kept
   their original ids, so everything that binds to #creditPill, #getMoreLink,
   #pricingBtn, #myDesignsBtn and #authSlot elsewhere in this file is unchanged
   — this section owns only the SURFACE.

   Three behaviours are worth stating plainly:
     · the button hides itself whenever another surface owns the screen (its
       own panel, the designs drawer, any modal), and comes back when that
       surface leaves — so it never floats over a dialog
     · a sub-menu animates between 0 and its MEASURED height, because `auto`
       cannot be transitioned; the height is set back to auto on arrival so the
       row can still grow if its content changes
     · focus is trapped while the panel is open and returned to the button on
       close, which is what makes it usable without a mouse
   ========================================================================== */
const MENU = {
  btn:   $("#studioMenuBtn"),
  panel: $("#studioMenu"),
  scrim: $("#studioMenuScrim"),
  close: $("#studioMenuClose"),
};
let _menuOpen = false, _menuLastFocus = null, _menuHideT = 0;

/* getClientRects, not offsetParent: offsetParent is null for a fixed element
   and for anything inside a collapsed sub-menu, and only one of those two
   should be skipped. */
const isShown = (el) => !!(el && el.isConnected && el.getClientRects().length);
const menuFocusables = () => $$("#studioMenu button:not([disabled]), #studioMenu a[href], #studioMenu input, #studioMenu select")
  .filter(isShown);

/* The button yields the screen to anything modal, and takes it back after.
   Every write is guarded by a read: classList.toggle rewrites the class
   attribute even when the token set does not change, and this function is
   driven by a MutationObserver watching class attributes — so writing
   unconditionally is an infinite loop, not a redundancy. */
function syncMenuButton() {
  if (!MENU.btn) return;
  const drawer = $("#designsDrawer");
  const busy = !!ROOT.querySelector(".modal.is-open")
            || !!(drawer && drawer.classList.contains("is-open"));
  const cl = MENU.btn.classList;
  const wantHidden = busy && !_menuOpen;
  if (cl.contains("is-hidden") !== wantHidden) cl.toggle("is-hidden", wantHidden);
  if (cl.contains("is-open") !== _menuOpen) cl.toggle("is-open", _menuOpen);
  const want = _menuOpen ? "true" : "false";
  if (MENU.btn.getAttribute("aria-expanded") !== want) MENU.btn.setAttribute("aria-expanded", want);
  if (_menuOpen && cl.contains("has-news")) cl.remove("has-news");
}

function openMenu() {
  if (!MENU.panel || _menuOpen) return;
  _menuOpen = true;
  _menuLastFocus = document.activeElement;
  clearTimeout(_menuHideT);
  MENU.scrim.hidden = false; MENU.panel.hidden = false;
  /* one frame with the panel in the DOM but not yet .is-open, so the browser
     has a start value to animate FROM */
  requestAnimationFrame(() => requestAnimationFrame(() => {
    MENU.scrim.classList.add("is-open");
    MENU.panel.classList.add("is-open");
  }));
  syncMenuButton();
  updateMenuStatus();
  /* focus the panel, not its first control: focusing a button would paint a
     focus ring on Close for someone who opened the panel with a mouse, and
     "the dialog is focused" is what a screen reader should announce anyway */
  setTimeout(() => { if (_menuOpen && MENU.panel) MENU.panel.focus({ preventScroll: true }); }, 120);
}

function closeMenu(returnFocus = true) {
  if (!MENU.panel || !_menuOpen) return;
  _menuOpen = false;
  MENU.scrim.classList.remove("is-open");
  MENU.panel.classList.remove("is-open");
  collapseMenuGroups();
  syncMenuButton();
  clearTimeout(_menuHideT);
  _menuHideT = setTimeout(() => {                 // after the slide-out
    if (_menuOpen) return;
    MENU.scrim.hidden = true; MENU.panel.hidden = true;
  }, 340);
  /* Focus goes back to the button, which is the control that owns this panel.
     Falling back to whatever was focused before would strand focus on <body>
     whenever the panel was opened from a keyboard shortcut or a script. */
  if (returnFocus) {
    const back = isShown(MENU.btn) ? MENU.btn : _menuLastFocus;
    if (back && back.focus) { try { back.focus(); } catch (e) {} }
  }
}
const toggleMenu = () => (_menuOpen ? closeMenu() : openMenu());

/* A credit change the shopper cannot see, because the panel is shut. */
function markMenuNews() {
  if (!MENU.btn || _menuOpen) return;
  MENU.btn.classList.remove("has-news"); void MENU.btn.offsetWidth;
  MENU.btn.classList.add("has-news");
}

/* Where the shopper is, in one line, refreshed every time the panel opens. */
const MENU_STEPS = ["Choose a reference", "Describe the charm", "Approve and order"];
function updateMenuStatus() {
  const txt = $("#menuStatusTxt"), fill = $("#menuStatusFill");
  if (!txt || !fill) return;
  const designing = state.designing || state.step > 1 || !!state.startMode;
  if (designing) {
    const n = Math.min(3, Math.max(1, state.step || 1));
    txt.innerHTML = `Step <b>${n}</b> of 3 &mdash; ${escapeHtml(MENU_STEPS[n - 1])}.`;
    fill.style.width = Math.round((n / 3) * 100) + "%";
  } else {
    txt.innerHTML = `<b>${CONFIG.guestFreeCredits} designs free.</b> No card, and no account needed to start.`;
    fill.style.width = "0%";
  }
}

/* ── Sub-menus ───────────────────────────────────────────────────────────── */
function setGroup(toggle, panel, open) {
  if (!toggle || !panel) return;
  toggle.setAttribute("aria-expanded", open ? "true" : "false");
  if (open) {
    panel.hidden = false;
    panel.classList.add("is-open");
    const h = panel.scrollHeight;
    panel.style.height = "0px";
    requestAnimationFrame(() => { panel.style.height = h + "px"; });
    /* once it has arrived, hand the height back to the content */
    setTimeout(() => { if (panel.classList.contains("is-open")) panel.style.height = "auto"; }, 400);
  } else {
    panel.style.height = panel.scrollHeight + "px";   // from auto to a number…
    requestAnimationFrame(() => {                     // …then to zero
      panel.classList.remove("is-open");
      panel.style.height = "0px";
    });
    setTimeout(() => { if (!panel.classList.contains("is-open")) panel.hidden = true; }, 400);
  }
}
const MENU_GROUPS = [["#menuStartToggle", "#menuStartPanel"], ["#menuHelpToggle", "#menuHelpPanel"]];
function collapseMenuGroups() {
  MENU_GROUPS.forEach(([t, p]) => setGroup($(t), $(p), false));
}
MENU_GROUPS.forEach(([t, p]) => {
  const toggle = $(t), panel = $(p);
  if (!toggle || !panel) return;
  toggle.addEventListener("click", () => {
    const open = toggle.getAttribute("aria-expanded") === "true";
    /* accordion: opening one closes the other, so the panel never scrolls */
    if (!open) MENU_GROUPS.forEach(([t2, p2]) => { if (t2 !== t) setGroup($(t2), $(p2), false); });
    setGroup(toggle, panel, !open);
  });
});

/* ── Wiring ──────────────────────────────────────────────────────────────── */
MENU.btn   && MENU.btn.addEventListener("click", toggleMenu);
MENU.close && MENU.close.addEventListener("click", () => closeMenu());
MENU.scrim && MENU.scrim.addEventListener("click", () => closeMenu());

document.addEventListener("keydown", (e) => {
  if (!_menuOpen) return;
  if (e.key === "Escape") { e.stopPropagation(); closeMenu(); return; }
  if (e.key !== "Tab") return;
  const items = menuFocusables();
  if (!items.length) return;
  const first = items[0], last = items[items.length - 1];
  if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
  else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
}, true);

/* Every row that leads somewhere else closes the panel first, so the surface
   it opens is never fighting the panel for the same corner of the screen. */
["#getMoreLink", "#pricingBtn", "#myDesignsBtn", "#creditPill", "#signInBtn"].forEach((sel) => {
  const el = $(sel);
  if (el) el.addEventListener("click", () => closeMenu(false));
});
/* ── START A DESIGN, FROM THE MENU ────────────────────────────────────────
   These three rows did nothing once a design was under way, and the third one
   did nothing ever.

   The old wiring was `if (!state.designing) beginDesign(); setStartMode(mode)`.
   beginDesign() is the ONLY thing in that line that navigates — so on a design
   already in progress the guard skipped it and setStartMode was left toggling
   `hidden` on #phaseSearch / #phaseUpload / #phaseDraw, every one of which
   lives inside #view-1. Sitting on step 2, the customer is shown #view-2, and
   the panel that just opened is behind it: the row is wired, fires, mutates
   the DOM, and looks completely dead. #menuStartDraw had no listener at all,
   so "Your Custom Design" was dead from either step.

   Choosing a source IS a return to step 1, whatever step you were on, so the
   navigation is unconditional here. The design itself is untouched — reference,
   versions, thread and renders all survive; maxStep is a max, so the stepper
   still offers the way back forward. */
function menuStartDesign(mode) {
  /* ── AND NOW THEY MEAN IT ─────────────────────────────────────────────
     These three rows used to navigate back to step 1 and bring the whole
     design with them: same session, same reference, same versions, same
     sketch. "Your Custom Design" therefore re-opened the sheet already on
     the sketchpad — the customer asked for a blank one and was handed the
     last one back.

     A source is not a step in a design, it is the START of one. So all
     three now leave the design entirely: the current one is saved with
     everything it is owed (including the brief typed but never sent),
     everything still generating is frozen onto it, and the studio is blank
     before the chosen source appears. See startNewProject. */
  startNewProject(mode);
}
$("#menuStartUpload") && $("#menuStartUpload").addEventListener("click", () => menuStartDesign("upload"));
$("#menuStartSearch") && $("#menuStartSearch").addEventListener("click", () => menuStartDesign("search"));
$("#menuStartDraw")   && $("#menuStartDraw").addEventListener("click",   () => menuStartDesign("draw"));
$("#menuFaq") && $("#menuFaq").addEventListener("click", () => {
  closeMenu(false);
  const faq = ROOT.querySelector(".faq-item");
  if (faq) setTimeout(() => faq.scrollIntoView({ behavior: "smooth", block: "center" }), 200);
});

/* The button's state follows surfaces it does not own — every modal and the
   designs drawer — so watch exactly those elements' class attributes rather
   than the whole subtree. Watching the subtree would include the button
   itself, and the button is what this observer writes to. */
if (window.MutationObserver) {
  const watcher = new MutationObserver(() => syncMenuButton());
  $$(".modal").concat($("#designsDrawer") ? [$("#designsDrawer")] : [])
    .forEach((el) => watcher.observe(el, { attributes: true, attributeFilter: ["class"] }));
}
syncMenuButton();
/* the Playwright menu suite drives these two directly, the same way the
   pricing suite reads window.__itemPrice */
window.__studioBuild   = "2026-08-18.09";
  window.__studioState   = state;
window.__renderCredits = renderCredits;
window.__measureBar    = measureBar;
window.__useGooglePopupFallback = useGooglePopupFallback;
window.__mountGoogleButton     = mountGoogleButton;
window.__detectInApp   = detectInApp;      // pure — the truth table is asserted directly
window.__inAppOpenUrl  = inAppOpenUrl;
window.__applyInAppUi  = applyInAppUi;
window.__claimHandoff  = claimHandoff;
window.__escapeUrl     = escapeUrl;
window.__readInAppForce = readInAppForce;
window.__openAuth      = openAuth;
window.__runRender     = runRender;
window.__openMarkup    = openMarkup;
window.__openCompose   = openCompose;
window.__markupState   = () => __mk;
window.__markupItems   = () => (__mk ? __mk.items : []);
window.__buildMarkupPayload = buildMarkupPayload;
window.__mkComposeToDataUrl = mkComposeToDataUrl;
window.__mkImportFiles = mkImportFiles;
window.__mkLibRefresh  = mkLibRefresh;
window.__mkLibraryItems = mkLibraryItems;
window.__mkChrome      = mkChrome;
window.__setStartMode  = setStartMode;
/* the project-lifecycle suite drives these directly: it starts a design,
   fires a shortcut mid-generation, and asserts on the two documents */
window.__startNewProject   = startNewProject;
window.__leaveProject      = leaveCurrentProject;
window.__resetProject      = resetProjectState;
window.__projectEpoch      = epochNow;
window.__flushSessionNow   = flushSessionNow;
window.__buildSessionDoc   = buildSessionDoc;
window.__outbox            = { read: outboxRead, put: outboxPut, drop: outboxDrop, drain: outboxDrain };
window.__loadSession   = loadSession;
window.__mkBucketFill  = mkBucketFill;
window.__mkPlaceAsset  = mkPlaceAsset;
window.__mkInsertLayers = mkInsertLayers;
window.__setReference  = setReference;
window.__mkCtxOpen     = mkCtxOpen;
window.__mkImportError = mkImportError;
window.__mkPack        = mkPack;
window.__mkTraceImage  = mkTraceImage;
window.__mkFillIntent  = mkFillIntent;
window.__mkMetalTally  = () => mkMetalTally(__mk ? __mk.items : []);
window.__mkMetalCheck  = mkMetalCheck;
window.__mkBucketFill  = window.__mkBucketFill || mkBucketFill;
window.__mkSetTool     = mkSetTool;
window.__mkDrawnBox    = mkDrawnBox;
window.__mkFillHit     = mkFillHit;
window.__mkZonesPayload = mkZonesPayload;
window.__mkBBox        = mkBBox;
window.__mkSnapTol     = mkSnapTol;
window.__mkApplyView   = () => mkApplyView();
window.__mkAlignApply  = mkAlignApply;
window.__mkAlignTargets = mkAlignTargets;
window.__mkCloseAllDrops = mkCloseAllDrops;
window.__mkRegionAt    = (x, y, o) => mkRegionAt(x, y, o);
window.__mkRegionSheet = (S) => mkRegionSheet(S);
window.__mkRegionToItem = mkRegionToItem;
window.__MK_FINE_S     = () => MK_FINE_S;
window.__mkWandAt      = mkWandAt;
window.__mkKillBackground = mkKillBackground;
window.__mkRestoreBackground = mkRestoreBackground;
window.__mkPictureFor  = mkPictureFor;
window.__mkPictureLocal = mkPictureLocal;
window.__resizeToMax   = resizeToMax;
window.__mkPaintExport = (ctx, w, h, items) => mkPaint(ctx, w, h, items, { export: true });
window.__traceVersion  = traceVersion;
window.__deleteDesign  = deleteDesign;
window.__deleteLibraryImage = deleteLibraryImage;
window.__mkSketchThumb = mkSketchThumb;
window.__renderDrawer  = renderDrawer;
window.__askConfirm    = askConfirm;
window.__mkTouch       = mkTouch;
window.__closeMarkup   = closeMarkup;
window.__mkSelIdx      = () => mkSelIdx();
window.__mkSelSet      = (l) => { mkSelSet(l); mkChrome(); mkPaintLayerSel(); };
window.__mkSelAdd      = (l) => { mkSelAdd(l); mkChrome(); mkPaintLayerSel(); };
window.__mkGroup       = () => mkGroupSelection();
window.__mkUngroup     = () => mkUngroupSelection();
window.__mkGroupsOf    = () => mkGroupList();
window.__mkSelBBox     = () => mkSelBBox();
window.__mkRenderLayers = mkRenderLayers;
window.__mkLayerAct    = mkLayerAct;
window.__mkArrange     = mkArrange;
window.__mkBlocks      = () => mkBlocks().map((b) => ({ g: b.g, idx: b.idx }));
window.__mkMultiSet    = (on) => { __mkMulti = !!on; };
window.__mkAssetKindReset = () => { __mkAssetKindDead = false; };
window.__renderStage   = renderStage;
/* One paste-in answer to "why is Google sign-in not working here": run
   window.__studioDiag() in the console and send the output. */
window.__studioDiag = async function () {
  const vis = (id) => { const el = $("#" + id) || document.getElementById(id); return !!(el && !el.hidden && el.getClientRects().length); };
  const out = {
    build: window.__studioBuild,
    origin: location.origin,
    googleClientId: D.googleClientId || "(blank)",
    renderedButtonEnabled: (D.googleRendered || "") === "1",
    ourGoogleButtonVisible: vis("googleBtn"),
    gsiIframeMounted: vis("googleMount"),
    firebase: { apiKey: !!FB_CFG.apiKey, authDomain: FB_CFG.authDomain, projectId: FB_CFG.projectId },
    fedcmAborts: "expected to be ABSENT in this build",
    /* the phone case: Google refuses OAuth inside an app's embedded browser,
       so this line is usually the whole answer on mobile */
    inAppBrowser: inAppInfo(),
    userAgent: navigator.userAgent,
  };
  try {
    await bootFirebase();
    const u = auth.currentUser;
    out.auth = { ready: true, uid: u && u.uid, anonymous: !!(u && u.isAnonymous), email: (u && u.email) || null };
  } catch (e) { out.auth = { ready: false, error: (e && e.message) || String(e) }; }
  out.popupProbe = "run window.__studioDiag.popup() to open the Google popup and report the raw result";
  console.table ? console.table(out) : console.log(out);
  return out;
};
window.__studioDiag.popup = async function () {
  try {
    await bootFirebase();
    const P = window.firebase.auth.GoogleAuthProvider;
    const prov = new P();
    const r = await (auth.currentUser && auth.currentUser.isAnonymous
      ? auth.currentUser.linkWithPopup(prov) : auth.signInWithPopup(prov));
    console.log("[diag] POPUP OK — signed in as:", r && r.user && r.user.email);
    return "OK: " + (r && r.user && r.user.email);
  } catch (e) {
    console.log("[diag] POPUP FAILED — code:", e && e.code, "message:", e && e.message);
    return "FAILED: " + (e && e.code) + " — " + (e && e.message);
  }
};

/* The other paste-in answer: "adding a picture from my computer does nothing."
   window.__importDiag() walks the whole path in order — build, sign-in,
   session, whether THIS deployment's britesAuth knows upload_asset — and
   prints the first thing that is wrong instead of leaving it a mystery. It
   sends a 1×1 pixel, so it is safe to run on a live account. */
window.__importDiag = async function () {
  const out = { build: window.__studioBuild, origin: location.origin };
  const step = async (name, fn) => {
    try { out[name] = await fn(); } catch (e) {
      out[name] = "FAILED: " + ((e && e.message) || String(e)) +
                  (e && e.status ? " (HTTP " + e.status + ")" : "");
    }
  };

  out.fileInput = (function () {
    const el = document.getElementById("mkFileInput");
    if (!el) return "MISSING — the section did not render the picker";
    const cs = getComputedStyle(el);
    return { present: true, display: cs.display, accept: el.accept,
             clickable: cs.display !== "none" };
  })();
  out.studioOpen = !!window.__mk;

  await step("auth", async () => {
    await bootFirebase();
    const u = auth.currentUser;
    if (!u) return "FAILED: nobody is signed in — not even anonymously";
    return { uid: u.uid, anonymous: !!u.isAnonymous, email: u.email || null };
  });

  await step("session", async () => {
    const s = state.session;
    return s && s.id ? { id: s.id, step: s.maxStep } : "none yet (import creates one)";
  });

  /* a 1×1 transparent PNG — the smallest honest test of the upload door */
  const PX = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJ" +
             "AAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=";
  const sid = (state.session && state.session.id) || "diag";

  await step("upload_asset", async () => {
    const r = await postAuthed("britesAuth", {
      kind: "upload_asset", sessionId: sid, filename: "diag.png", dataUrl: PX });
    if (r && r.ok && r.path) return { ok: true, path: r.path };
    return "NOT AVAILABLE: " + JSON.stringify(r) +
           "  → deploy britesAuth.js 1.8.0; until then import falls back to upload_ref";
  });

  if (typeof out.upload_asset === "string") {
    await step("upload_ref_fallback", async () => {
      const r = await postAuthed("britesAuth", {
        kind: "upload_ref", sessionId: sid, filename: "diag.png", dataUrl: PX });
      return r && r.ok && r.path
        ? { ok: true, path: r.path, note: "fallback works — import will succeed, " +
                                          "but each picture spends an upload slot" }
        : "FAILED: " + JSON.stringify(r);
    });
  }

  console.log("[import diag]", out);
  return out;
};

/* The studio bar is gone — the menu floats — so the chrome budget the sticky
   offsets derive from no longer carries its height. The variable stays so the
   budget arithmetic reads the same, and so re-introducing a bar would need no
   other change. */
function measureBar() {
  const bar = $("#studioBar");
  ROOT.style.setProperty("--bj-studiobar-h", bar ? bar.offsetHeight + "px" : "0px");
  /* the menu button centres itself in the designing-mode progress bar */
  const wrap = ROOT.querySelector(".stepper-wrap");
  if (wrap && wrap.offsetHeight) ROOT.style.setProperty("--bj-stepper-h", wrap.offsetHeight + "px");
}
window.addEventListener("resize", measureBar);
window.addEventListener("orientationchange", measureBar);
window.addEventListener("load", measureBar);
if (document.fonts && document.fonts.ready) document.fonts.ready.then(measureBar);
setTimeout(measureBar, 300);

/* ── first paint: everything that needs no network ─────────────────────── */
renderPricing();
renderCredits();
renderPictos();
renderHeroShow();
renderFaq();
syncBrief();
renderThread();
autoGrow();
updateOrder();
measureBar();
applyInAppUi();   /* before any work is started, not after it is stranded */

/* They tapped Sign in inside the app's browser, then Open in Chrome. The modal
   they were heading for needs NOTHING from the backend — it is static markup
   and a button — so it opens here, on the first frame, rather than at the end
   of the boot chain behind three script loads, a handoff claim and a config
   read. That chain was costing three to four seconds of a shopper staring at a
   page they did not ask to be on. One frame's delay so it animates in rather
   than being painted already open; the bonus-credit figure starts at the
   CONFIG default and is rewritten in place when the real one lands. */
const RESUME_AUTH = takeAuthIntent();
let _resumeKilled = false;
/* Stand down: this shopper turned out to have a real account, so the modal
   they were heading for is not for them. Written as a kill FLAG rather than a
   close call because the two are racing — on a warm cache the whole boot chain
   can finish before the first animation frame fires, and a close that lands
   before the open closes nothing. Whichever wins, the outcome is the same. */
function killResumeAuth() {
  _resumeKilled = true;
  const el = $("#authModal");
  if (el && el.classList.contains("is-open") && !$("#authEmail").value) {
    el.classList.remove("is-open");
  }
}
if (RESUME_AUTH) requestAnimationFrame(() => { if (!_resumeKilled) openAuth(RESUME_AUTH); });
attachZoomPan($("#stage"), ".result");
attachZoomPan($("#finalStage"), ".result");
attachZoomPan($("#doneThumb"));
attachZoomPan($("#refPreviewStage"));

/* ── then the live studio ──────────────────────────────────────────────── */
(async function boot() {
  try {
    await bootFirebase();

    /* A shopper we sent out of an app's embedded browser arrives here in a
       real one, carrying a one-time handoff code in the fragment. Claim it
       BEFORE anonymous auth: signing in anonymously first would hand them a
       brand-new empty uid, and the whole point of the code is that they keep
       the old one. */
    await claimHandoff();

    /* Anonymous auth FIRST. Every visitor gets a uid immediately anyway, and
       config/customStudio is readable by a signed-in visitor only — there is
       no reason for the studio's limits and prices to be world-readable when
       the studio always has a session by the time it needs them. */
    if (!auth.currentUser) {
      try { await auth.signInAnonymously(); }
      catch (e) { console.warn("[studio] anonymous auth:", e.message); }
    }

    await loadRemoteConfig();
    renderPricing();
    renderCredits();
    renderUploadAllowance();
    updateOrder();

    /* Opening the modal at first paint means opening it before we know who
       this is. Almost always they are a guest — that is what the marker means
       — but if the claim or a live session turns out to have handed us a real
       account, take it back down rather than asking a signed-in customer to
       sign in. Only if they have not already started using it. */
    if (RESUME_AUTH && !isGuestUser(auth.currentUser)) killResumeAuth();

    auth.onAuthStateChanged(async (u) => {
      if (!u) { try { await auth.signInAnonymously(); } catch (e) { console.warn("[studio] anonymous auth:", e.message); } return; }
      state.uid = u.uid;
      /* and again here, because a session can resolve late — after the modal
         has already animated in */
      if (RESUME_AUTH && !_resumeKilled && !isGuestUser(u)) killResumeAuth();
      if (!u.isAnonymous && u.email) {
        setAccountChip(u.displayName || u.email.split("@")[0], u.email,
          (u.providerData && u.providerData[0] && u.providerData[0].providerId) || "email");
      }
      watchWallet(u.uid);
      watchSessions(u.uid);
      refreshWallet();
      /* anything a previous visit could not write — a dropped connection, a
         tab closed mid-save — goes up now, before the customer touches
         anything. It is keyed by uid, so a shelf belonging to somebody else's
         session on a shared browser is left exactly where it is. */
      try { outboxDrain(); } catch (e) {}
      /* returning from the verification email lands on ?verify=1 */
      /* isGuestUser, not isAnonymous: a shopper who arrived through the in-app
         handoff is signed in with a CUSTOM token, so isAnonymous is false even
         though they have no account yet — asking the narrower question would
         skip the modal for exactly the people it is for. */
      if (/[?&]verify=1/.test(location.search) && isGuestUser(u)) openAuth("default");
    });
  } catch (e) {
    console.error("[studio] boot failed:", e);
    toast("The studio couldn't start — please refresh the page", "err");
  }
})();

})();

