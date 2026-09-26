/*  netlify/functions/_charmNestCustomRead.js
 *  Is this order line a custom order? (Paul, 25 Sep: a word in a listing title is not enough; many regular charm-only
 *  listings say "add on".) Claude reads everything the shop already holds about the line, never Etsy itself:
 *    - the line as the sorter read it (SKU, title, options, jewellery type, personalisation, buyer message, staff note,
 *      the Team's internal messages, the order's other lines), sent by the page;
 *    - the listing's stored description and tags (EtsyMail_Listings, the inbox's catalogue mirror);
 *    - the listing's first photo, from the stored image cache (cache only), fetched small from Etsy's image CDN
 *      (a picture download, not an Etsy API call);
 *    - the buyer's conversations with the shop, from the inbox's stored copy (EtsyMail_Threads).
 *  It answers per line: kind, a calibrated confidence, one line of what the shop must do, the earlier order a rework or
 *  add-on is for, and the evidence. Every answer is kept (Charm_Nest_CustomRead/{lineKey}.reads[hash]) so a line is read
 *  once, and again only when what it is read from changes. A person's own decision is kept beside it (decided, or
 *  decidedSandbox for the sandbox) and always wins.
 *  Cost: only lines with no design in a master file are sent, eight to a call, with the instructions cached. */
"use strict";
const { str, num } = require("./_charmNestAuth");

const MODEL = process.env.CHARM_NEST_CUSTOM_MODEL || "claude-sonnet-5";
const EFFORT = /^(low|medium|high)$/.test(process.env.CHARM_NEST_CUSTOM_EFFORT || "") ? process.env.CHARM_NEST_CUSTOM_EFFORT : "low";
const COLL = "Charm_Nest_CustomRead";
const PER_CALL = 8, MAX_LINES = 40;
const KINDS = ["custom", "rework", "addOnToOrder", "chainOnly", "other", "regular"];

const INSTRUCTIONS = `You sort order lines for a small jewellery shop on Etsy that laser-cuts charms in 14k gold filled, sterling silver and rose gold, and sells them as charm necklaces, charm-only pieces, huggie hoop charms, earrings and bracelets.

Every line you receive has a SKU with no design in the shop's master files (or no SKU at all). Most such lines are still REGULAR catalogue products whose SKU simply has not been indexed yet. Your job is to find the few that are not regular, and to say how sure you are.

Kinds:
- "regular": a normal catalogue listing (a named charm design, a charm necklace, a charm-only piece, huggies, earrings, a bracelet). Its listing photo shows the finished metal product. IMPORTANT: many regular listings are "Add On Charm" or "Add-on charm" listings (a single charm bought to add to a chain or to another piece). The words "add on" or "add-on" in a title NEVER make a line special by themselves.
- "custom": something made to the buyer's own design or request, not a catalogue design. Signs: the shop's SKU starts with CUSTOM, CSTM or CUST (the shop's own code for custom listings; very strong); the listing photo is a generic graphic rather than a product photo (often a blue background with words like "custom", "custom charm" or "custom order"), or it shows a customer's own drawing, photo, logo or a vector mock-up; the jewellery type is not specified and there are few or no options; the title or description says custom order / custom request / made to your design / private listing for a named person; the buyer or staff describe a design to be drawn.
- "rework": a repair, fix, replacement, resize, re-engraving or modification of something already made, often items the customer sends back. The text usually names the earlier order.
- "addOnToOrder": an extra bought AFTER an earlier order to be added to or shipped with it (an extra charm, letter or birthstone for a piece already ordered). It needs clear words tying it to an earlier order ("add this to my order 41…", "for the necklace I ordered last week"), not just "add on" in the listing title.
- "chainOnly": only a chain or extender is bought, with no charm to cut.
- "other": a non-product payment or service: deposit, balance, price difference, shipping upgrade, rush fee, gift box on its own.

How to judge:
- Weigh independent evidence. The SKU code, the listing photo, the listing description, the options, and the words of the buyer, staff and conversations are separate witnesses. A catalogue title plus a product photo of a finished charm means regular, whatever the title's filler words say.
- A personalised catalogue design (a name, initial or date engraved on a standard charm) is regular, not custom.
- "Jewellery: Type not specified" and "no options" point toward custom or special, but a regular listing can lack them too; they never decide alone.
- The listing photo is supporting evidence only. Product photos on white or neutral backgrounds are typical of regular listings. Never guess a kind from the photo alone.
- Conversations may be about other orders by the same buyer; use them only where they clearly concern this line.
- confidence is the probability that your kind is right, from 0 to 1. Give 0.9 or more only when two or more independent signs agree, or the shop's own SKU code says it. Give 0.5 to 0.7 when the evidence is thin or mixed.
- relatedOrder: the earlier order number (digits only) the line is for, when the text names one; otherwise null.
- summary: at most 18 plain words saying what the shop must make or do (for regular: which catalogue product it is).
- evidence: up to 4 short items, each naming its source (SKU, photo, title, description, options, buyer note, staff, Team, email).
Answer for every line, using its key exactly as given.`;

const SCHEMA = {
  type: "object", additionalProperties: false,
  properties: {
    lines: {
      type: "array",
      items: {
        type: "object", additionalProperties: false,
        properties: {
          key: { type: "string" },
          kind: { type: "string", enum: KINDS },
          confidence: { type: "number" },
          summary: { type: "string" },
          relatedOrder: { type: ["string", "null"] },
          evidence: { type: "array", items: { type: "string" } }
        },
        required: ["key", "kind", "confidence", "summary", "relatedOrder", "evidence"]
      }
    }
  },
  required: ["lines"]
};

const cleanKey = k => String(k || "").replace(/[^\w\-]/g, "").slice(0, 80);
const cleanHash = h => String(h || "").replace(/[^\w]/g, "").slice(0, 40);
const tsMs = v => (v && v.toMillis ? v.toMillis() : typeof v === "number" ? v : v && v._seconds ? v._seconds * 1000 : 0);

/** The line as the page sent it, bounded. */
function cleanLine(l) {
  const list = (a, n, m) => (Array.isArray(a) ? a : []).slice(0, n).map(x => str(x, m)).filter(Boolean);
  return {
    key: cleanKey(l.key), hash: cleanHash(l.hash), order: str(l.order, 20).replace(/\D/g, ""), listingId: str(l.listingId, 20).replace(/\D/g, ""),
    buyerUserId: str(l.buyerUserId, 20).replace(/\D/g, ""),
    sku: str(l.sku, 60), title: str(l.title, 240), quantity: num(l.quantity) || 1, jewellery: str(l.jewellery, 60), metal: str(l.metal, 40),
    options: (Array.isArray(l.options) ? l.options : []).slice(0, 12).map(o => ({ name: str(o && o.name, 60), value: str(o && o.value, 160) })).filter(o => o.name || o.value),
    personalization: list(l.personalization, 6, 400), buyerMessage: str(l.buyerMessage, 1200), staffNote: str(l.staffNote, 800),
    team: (Array.isArray(l.team) ? l.team : []).slice(-6).map(m => ({ who: str(m && m.who, 40), text: str(m && m.text, 400) })).filter(m => m.text),
    otherLines: (Array.isArray(l.otherLines) ? l.otherLines : []).slice(0, 6).map(o => ({ sku: str(o && o.sku, 60), title: str(o && o.title, 140), known: !!(o && o.known) })),
    hints: list(l.hints, 6, 120)
  };
}

/** What the shop already holds about the listing and the buyer: Firestore only, never Etsy. */
async function enrich(db, lines, fetch) {
  const ids = [...new Set(lines.map(l => l.listingId).filter(Boolean))].slice(0, MAX_LINES);
  const listings = {}, photos = {};
  if (ids.length) {
    const docs = await db.getAll(...ids.map(id => db.collection("EtsyMail_Listings").doc(id))).catch(() => []);
    for (const d of docs) if (d && d.exists) { const x = d.data() || {}; listings[d.id] = { description: str(x.description || x.descriptionShort, 700), tags: (Array.isArray(x.tags) ? x.tags : []).slice(0, 13).map(t => str(t, 40)), customizable: x.isCustomizable === true || x.is_customizable === true }; }
    // the stored listing photos only (cacheOnly: no Etsy call), fetched small from the image CDN
    let cached = {}; try { cached = await require("./_etsyImageCache").readMany(ids, { cacheOnly: true }); } catch (_) {}
    await Promise.all(ids.map(async id => {
      const first = cached[id] && cached[id].images && cached[id].images[0];
      let url = first && (first.url_170x135 || first.url_570xN || first.url);
      if (!url || !/^https:\/\/i\.etsystatic\.com\//.test(url)) return;
      url = url.replace(/il_(?:570xN|fullxfull|794xN|1140xN|1588xN)\./, "il_340x270.");
      try {
        const ctl = new AbortController(), t = setTimeout(() => ctl.abort(), 6000);
        const r = await fetch(url, { signal: ctl.signal }); clearTimeout(t);
        if (!r.ok) return;
        const type = (r.headers.get("content-type") || "").split(";")[0].trim();
        if (!/^image\/(jpeg|png|webp|gif)$/.test(type)) return;
        const buf = Buffer.from(await r.arrayBuffer()); if (buf.length > 400000) return;
        photos[id] = { type: "image", source: { type: "base64", media_type: type, data: buf.toString("base64") } };
      } catch (_) { /* no photo: the reading goes on without it */ }
    }));
  }
  // the buyer's conversations (the inbox's stored copy): threads of this order, then of the buyer, newest first
  const mail = {};
  const threads = db.collection("EtsyMail_Threads");
  await Promise.all([...new Set(lines.map(l => l.order + "|" + l.buyerUserId))].map(async pair => {
    const [order, buyer] = pair.split("|"); if (!order) return;
    try {
      const found = new Map();
      for (const d of (await threads.where("etsyOrderId", "==", order).limit(3).get()).docs) found.set(d.id, d.data());
      // the buyer: from those conversations, else from the inbox's stored receipt (never asked of Etsy)
      let who = buyer || [...found.values()].map(t => t.buyerUserId).find(Boolean) || "";
      if (!who) {
        const [r, c] = await db.getAll(db.collection("EtsyMail_Receipts").doc(order), db.collection("EtsyMail_OrderLinkBuyers").doc(order)).catch(() => []);
        const rd = r && r.exists ? r.data() : null, cd = c && c.exists ? c.data() : null;
        who = String((rd && (rd.buyer_user_id || rd.buyerUserId || (rd.raw && rd.raw.buyer_user_id))) || (cd && cd.buyerUserId) || "");
      }
      if (who) for (const d of (await threads.where("buyerUserId", "==", String(who)).limit(6).get()).docs) found.set(d.id, d.data());
      const at = t => Math.max(tsMs(t.lastInboundAt), tsMs(t.lastOutboundAt), tsMs(t.updatedAt));
      const pick = [...found].sort((a, b) => at(b[1]) - at(a[1])).slice(0, 3);
      const out = [];
      for (const [id, t] of pick) {
        const s = await threads.doc(id).collection("messages").orderBy("timestamp", "desc").limit(10).get();
        const msgs = s.docs.filter(d => !d.id.startsWith("optim_")).map(d => d.data() || {}).reverse()
          .map(m => `${m.direction === "inbound" ? "buyer" : "shop"}: ${str(String(m.text || "").replace(/\s+/g, " ").trim(), 360)}${Array.isArray(m.imageUrls) && m.imageUrls.length ? " [sent a photo]" : ""}`)
          .filter(x => !/: $/.test(x));
        if (msgs.length) out.push({ about: t.etsyOrderId ? `order ${t.etsyOrderId}` : "no order named", messages: msgs });
      }
      if (out.length) mail[order] = out;
    } catch (e) { console.warn("[customRead] mail", order, e.message); }
  }));
  return { listings, photos, mail };
}

/** The request content for one call: each line in words, its listing photo after it. */
function content(batch, ctx) {
  const out = [{ type: "text", text: `${batch.length} order line${batch.length === 1 ? "" : "s"} follow.` }];
  for (const l of batch) {
    const li = ctx.listings[l.listingId] || null, mail = ctx.mail[l.order] || [];
    const lines = [
      `── key ${l.key} · order ${l.order} ──`,
      `SKU: ${l.sku || "(none)"} (no design in the master files)`,
      `Title: ${l.title || "(none)"}`,
      `Jewellery type as read: ${l.jewellery || "Type not specified"} · metal: ${l.metal || "not read"} · quantity ${l.quantity}`,
      `Options: ${l.options.length ? l.options.map(o => `${o.name}: ${o.value}`).join(" | ") : "none"}`,
      `Personalisation: ${l.personalization.length ? JSON.stringify(l.personalization) : "none"}`,
      `Buyer message on the order: ${l.buyerMessage ? JSON.stringify(l.buyerMessage) : "none"}`,
      `Staff note: ${l.staffNote ? JSON.stringify(l.staffNote) : "none"}`,
      `Team (internal) messages: ${l.team.length ? l.team.map(m => `${m.who || "staff"}: ${JSON.stringify(m.text)}`).join(" / ") : "none"}`,
      `Other lines in the same order: ${l.otherLines.length ? l.otherLines.map(o => `${o.sku || "no SKU"} "${o.title}"${o.known ? " (catalogue design)" : ""}`).join("; ") : "none"}`,
      `Listing description: ${li && li.description ? JSON.stringify(li.description) : "not stored"}${li && li.tags.length ? ` · tags: ${li.tags.join(", ")}` : ""}${li && li.customizable ? " · listing allows personalisation" : ""}`,
      `Conversations with this buyer (stored copy): ${mail.length ? mail.map(t => `[${t.about}] ` + t.messages.join(" / ")).join(" ‖ ") : "none"}`,
      `The sorter's own rule hints: ${l.hints.length ? l.hints.join("; ") : "none"}`,
      `Listing photo: ${ctx.photos[l.listingId] ? "below" : "not stored"}`
    ];
    out.push({ type: "text", text: lines.join("\n") });
    if (ctx.photos[l.listingId]) out.push(ctx.photos[l.listingId]);
  }
  return out;
}

function normalize(x, want) {
  if (!x || !want.has(cleanKey(x.key))) return null;
  const kind = KINDS.includes(x.kind) ? x.kind : "regular";
  const rel = String(x.relatedOrder || "").replace(/\D/g, "");
  return {
    key: cleanKey(x.key), kind, confidence: Math.round(Math.max(0, Math.min(1, num(x.confidence))) * 100) / 100,
    summary: str(x.summary, 200), relatedOrder: rel.length >= 6 && rel.length <= 20 ? rel : null,
    evidence: (Array.isArray(x.evidence) ? x.evidence : []).slice(0, 4).map(e => str(e, 160)).filter(Boolean)
  };
}

/** Read up to 40 lines; every answer is kept, and the answers go back to the page. */
async function run(body, opts = {}) {
  if (!process.env.ANTHROPIC_API_KEY && !opts.anthropic) return { skipped: "ANTHROPIC_API_KEY is not set" };
  const admin = opts.admin || require("./firebaseAdmin"), db = opts.db || admin.firestore();
  const anthropic = opts.anthropic || require("./_etsyMailAnthropic"), fetch = opts.fetch || require("node-fetch");
  const lines = (Array.isArray(body.lines) ? body.lines : []).slice(0, MAX_LINES).map(cleanLine).filter(l => l.key && l.hash);
  if (!lines.length) return { error: "no lines" };
  const ctx = await enrich(db, lines, fetch);
  const reads = {}, usage = { input_tokens: 0, output_tokens: 0, cache_read_input_tokens: 0 }, problems = [];
  const batches = []; for (let i = 0; i < lines.length; i += PER_CALL) batches.push(lines.slice(i, i + PER_CALL));
  const one = async batch => {
    const want = new Set(batch.map(l => l.key));
    const res = await anthropic.callClaudeRaw({ model: MODEL, maxTokens: 12000, effort: EFFORT, system: [{ type: "text", text: INSTRUCTIONS, cache_control: { type: "ephemeral" } }], messages: [{ role: "user", content: content(batch, ctx) }], outputFormat: { type: "json_schema", schema: SCHEMA }, thinkingDisplay: "summarized" });
    for (const k of Object.keys(usage)) usage[k] += (res.usage && res.usage[k]) || 0;
    if (res.stop_reason === "refusal") throw new Error("model declined");
    const text = (res.content || []).filter(b => b.type === "text").map(b => b.text).join("");
    let parsed; try { parsed = JSON.parse(text); } catch (_) { throw new Error(res.stop_reason === "max_tokens" ? "answer cut off" : "unreadable answer"); }
    for (const x of parsed.lines || []) { const v = normalize(x, want); if (v) reads[v.key] = v; }
  };
  // two calls at a time: a 40-line job ends in about three calls' time
  for (let i = 0; i < batches.length; i += 2) {
    const got = await Promise.allSettled(batches.slice(i, i + 2).map(one));
    for (const g of got) if (g.status === "rejected") { problems.push(String(g.reason && g.reason.message || g.reason).slice(0, 200)); console.error("[customRead]", g.reason); }
  }
  // each answer is kept under the hash of what it was read from
  const FV = admin.firestore.FieldValue, at = Date.now();
  await Promise.all(lines.filter(l => reads[l.key]).map(l => {
    const v = Object.assign({}, reads[l.key], { hash: l.hash, model: MODEL, at });
    reads[l.key] = v;
    return db.collection(COLL).doc(l.key).set({ reads: { [l.hash]: v }, latest: l.hash, order: l.order, updatedAt: FV.serverTimestamp() }, { merge: true }).catch(e => console.warn("[customRead] keep", l.key, e.message));
  }));
  const missing = lines.filter(l => !reads[l.key]).map(l => l.key);
  if (!Object.keys(reads).length) return { skipped: `custom reading unavailable (${problems[0] || "no answer"})` };
  return { reads, missing, model: MODEL, usage, problems };
}

/** The kept answers for these lines (only those read from exactly what the page has now), and a person's decisions. */
async function lookup(db, items, sandbox) {
  const want = (Array.isArray(items) ? items : []).slice(0, 400).map(x => ({ key: cleanKey(x && x.key), hash: cleanHash(x && x.hash) })).filter(x => x.key);
  const reads = {}, decided = {}, field = sandbox ? "decidedSandbox" : "decided";
  for (let i = 0; i < want.length; i += 100) {
    const part = want.slice(i, i + 100), docs = await db.getAll(...part.map(x => db.collection(COLL).doc(x.key)));
    docs.forEach((d, j) => {
      if (!d.exists) return; const x = d.data() || {}, w = part[j];
      if (w.hash && x.reads && x.reads[w.hash]) reads[w.key] = x.reads[w.hash];
      if (x[field] && x[field].kind) decided[w.key] = x[field];
    });
  }
  return { reads, decided };
}
/** A person's decision on a line: custom (or another special kind) or regular; null takes it back. */
async function decide(db, FV, b, sandbox) {
  // one line (key) or the lines of one decision (keys: an Unknown SKU card can hold many orders)
  const keys = [...new Set((Array.isArray(b.keys) ? b.keys : [b.key]).map(cleanKey).filter(Boolean))];
  if (!keys.length) return { error: "which line?" };
  if (keys.length > 400) return { error: "too many lines in one decision" };
  const kind = b.kind == null ? null : KINDS.includes(b.kind) ? b.kind : null;
  if (b.kind != null && !kind) return { error: "bad kind" };
  const rec = kind ? { kind, by: str(b.by, 60) || "someone", at: Date.now() } : null;
  const batch = db.batch();
  for (const key of keys) batch.set(db.collection(COLL).doc(key), { [sandbox ? "decidedSandbox" : "decided"]: rec || FV.delete(), updatedAt: FV.serverTimestamp() }, { merge: true });
  await batch.commit();
  return { ok: true, decided: rec, keys };
}

module.exports = { run, lookup, decide, content, cleanLine, normalize, INSTRUCTIONS, SCHEMA, MODEL, KINDS, COLL, MAX_LINES };
