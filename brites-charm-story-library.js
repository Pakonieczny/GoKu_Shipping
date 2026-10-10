(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.BritesCharmStoryLibrary = factory();
}(typeof globalThis !== 'undefined' ? globalThis : this, function () {
  'use strict';
  // Validation only. Research content lives in Firestore; this public module
  // contains neither a catalogue of stories nor an offline storage fallback.
  var SOURCE_MS = 30 * 86400000, CURRENT_MS = 5 * 60000;
  var INSTITUTIONS = ['si.edu', 'metmuseum.org', 'vam.ac.uk', 'rmg.co.uk', 'nhm.ac.uk', 'kew.org', 'montereybayaquarium.org', 'allaboutbirds.org', 'audubon.org', 'amnh.org', 'historymuseum.ca', 'themorgan.org', 'gulbenkian.pt'];
  var FIELDS = ['schema','kind','provenance','productId','handle','productUrl','productTitle','productCheckedAt','checkedAt','libraryId','libraryVersion','motif','context','facts','interpretation','sources'];
  function object(v) { return !!v && typeof v === 'object' && !Array.isArray(v); }
  function fields(v, allowed) { return object(v) && Object.keys(v).every(function (name) { return allowed.includes(name); }); }
  function text(v, max) {
    if (typeof v !== 'string' || !v.trim() || v.length > max || /[<>\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/.test(v)) return null;
    var s = v.trim().replace(/\s+/g, ' ');
    if (/https?:\/\/|[\w.+-]+@[\w.-]+\.[a-z]{2,}|(?:\+?\d[\s().-]*){10,19}|\b(?:password|passcode|api[ _-]?key|access[ _-]?token|bearer|secret|credit card|gift note|design brief|engraving text)\b/i.test(s)) return null;
    if (/(?:system|developer|assistant|internal|hidden)\s+(?:prompt|message|instructions?)|\bignore\b.{0,30}\binstructions?\b|\b(?:assistant|concierge|model)\s+(?:must|should|shall)|(?:^|[.!?]\s*)(?:please\s+)?(?:click|select|add|remove|buy|checkout|navigate|open|execute|show)\b/i.test(s)) return null;
    if (/\b(?:cures?|heals?|treats?|guarantees?)\b|\b(?:will|can)\s+(?:protect you|bring (?:you )?luck|prevent illness)\b/i.test(s)) return null;
    return s;
  }
  function url(v, merchant) {
    try {
      if (typeof v !== 'string' || v.length > 1000) return null;
      var u = new URL(v), host = u.hostname.toLowerCase().replace(/^www\./, '');
      if (u.protocol !== 'https:' || u.username || u.password || u.port || u.search || u.hash || !host.includes('.') || /^(?:localhost|\d+(?:\.\d+){3})$/.test(host)) return null;
      if (merchant) return host === 'britesjewelry.com' && /^\/products\/[a-z0-9_-]{1,180}\/?$/.test(u.pathname) ? u.href.replace(/\/$/, '') : null;
      if (/\/(?:shop|cart|checkout|products?)(?:\/|$)/i.test(u.pathname)) return null;
      return /\.(?:gov|edu|gov\.uk|gc\.ca|ac\.uk)$/.test(host) || INSTITUTIONS.some(function (h) { return host === h || host.endsWith('.' + h); }) ? u.href : null;
    } catch (_) { return null; }
  }
  function fresh(at, now, ms) { return Number.isFinite(at) && at > 0 && at <= now + 60000 && now - at <= ms; }
  function motifInTitle(motif,title) { var normalize=function(value){return String(value).toLowerCase().replace(/[_-]+/g,' ').replace(/[^a-z0-9 ]/g,' ').replace(/\s+/g,' ').trim();};return (' '+normalize(title)+' ').includes(' '+normalize(motif)+' '); }
  function normalizeStory(value, now) {
    now = Number.isFinite(now) ? now : Date.now();
    if (!fields(value, FIELDS) || value.schema !== 1 || !/^gid:\/\/shopify\/Product\/[1-9]\d{0,19}$/.test(value.productId || '') || !/^[a-z0-9_-]{1,180}$/.test(value.handle || '')) return null;
    var researched = value.kind === 'researched-story' && value.provenance === 'agent_researched', published = value.kind === 'published-detail' && value.provenance === 'published_product';
    if (!researched && !published || !fresh(value.checkedAt, now, CURRENT_MS) || !fresh(value.productCheckedAt, now, CURRENT_MS)) return null;
    var productUrl = url(value.productUrl, true), title = text(value.productTitle, 300), context = text(value.context, 300), motif = value.motif === null ? null : text(value.motif, 100);
    if (!productUrl || new URL(productUrl).pathname !== '/products/' + value.handle || !title || !context) return null;
    if (researched && (!/^[a-z0-9][a-z0-9_-]{0,79}$/.test(value.libraryId || '') || !/^[a-f0-9]{64}$/.test(value.libraryVersion || '') || !motif || !motifInTitle(motif,title))) return null;
    if (published && (value.libraryId !== null || value.libraryVersion !== null || value.motif !== null || value.interpretation !== null)) return null;
    if (!Array.isArray(value.sources) || value.sources.length < 1 || value.sources.length > 4 || !Array.isArray(value.facts) || value.facts.length < 1 || value.facts.length > 3) return null;
    var sources = [], seen = new Set();
    for (var s of value.sources) {
      if (!fields(s, ['id','title','url','publisher','checkedAt','inspection']) || !/^[a-zA-Z0-9_-]{1,80}$/.test(s.id || '') || seen.has(s.id)) return null;
      var sourceUrl = url(s.url, published), sourceTitle = text(s.title, 300), publisher = text(s.publisher, 180);
      if (!sourceUrl || !sourceTitle || !publisher || !fresh(s.checkedAt, now, published ? CURRENT_MS : SOURCE_MS) || s.inspection !== (published ? 'published_check' : 'agent_inspected')) return null;
      if (published && sourceUrl !== productUrl) return null;
      sources.push({id:s.id,title:sourceTitle,url:sourceUrl,publisher:publisher,checkedAt:s.checkedAt,inspection:s.inspection}); seen.add(s.id);
    }
    var facts = [];
    for (var f of value.facts) {
      var factual = fields(f, ['text','sourceIds']) && text(f.text, 700), ids = f && f.sourceIds;
      if (!factual || !Array.isArray(ids) || !ids.length || ids.length > 4 || new Set(ids).size !== ids.length || ids.some(function (id) { return !seen.has(id); })) return null;
      facts.push({text:factual,sourceIds:ids.slice()});
    }
    // A merchant fallback cannot carry made-up source-derived symbolism.
    if (published && (facts.length !== 1 || sources.length !== 1 || facts[0].text !== 'The published listing names this piece “' + title + '”.')) return null;
    var interpretation = null;
    if (researched) {
      var i = value.interpretation, it = fields(i, ['text','context','optional']) && text(i.text, 700), ic = i && text(i.context, 300);
      if (!it || !ic || i.optional !== true) return null;
      interpretation = {text:it,context:ic,optional:true};
    }
    return {schema:1,kind:value.kind,provenance:value.provenance,productId:value.productId,handle:value.handle,productUrl:productUrl,productTitle:title,productCheckedAt:value.productCheckedAt,checkedAt:value.checkedAt,libraryId:value.libraryId,libraryVersion:value.libraryVersion,motif:motif,context:context,facts:facts,interpretation:interpretation,sources:sources};
  }
  function storyMatchesProduct(value, product, now) {
    var story = normalizeStory(value, now);
    return !!(story && object(product) && story.productId === product.id && story.handle === product.handle && story.productTitle === product.title && story.productUrl === url(product.url, true) && fresh(product.checkedAt, now === undefined ? Date.now() : now, CURRENT_MS) && !product.meaningHold && !product.recommendationHold && !product.cartHold);
  }
  function storyConnection(story, personal, options) {
    personal = object(personal) ? personal : {};
    var expanded = object(options) && options.expanded === true && story.kind === 'researched-story';
    var recipient = text(personal.recipient, 120) || '', occasion = text(personal.occasion, 120) || '', reason = text(personal.reason, 300) || '';
    var address = /^(?:me|myself|yourself|self|you)$/i.test(recipient) ? 'yourself' : recipient ? (/^(?:my|our|your)\s+/i.test(recipient) ? recipient.replace(/^(?:my|our)\s+/i,'your ') : 'your ' + recipient) : 'the person you have in mind';
    var gift = address + (occasion ? ' for ' + occasion : '');
    // Keep the first bite short. Full facts, attribution and the optional
    // interpretation remain available separately for an explicit detail request.
    var reply = expanded ? story.facts.map(function(fact){return fact.text;}).join(' ') : story.facts[0].text;
    if (story.kind === 'researched-story') reply += ' ' + story.context.replace(/[.!?]$/, '') + '.';
    if (expanded) reply += ' ' + story.interpretation.text + ' ' + story.interpretation.context.replace(/[.!?]$/, '') + '.';
    if (reason) reply += ' For ' + gift + ', you could let it stand for “' + reason + '”, if that feels right to you.';
    else if (story.kind === 'researched-story' && !expanded) reply += (recipient || occasion ? ' If it fits ' + gift + ', ' + story.interpretation.text.charAt(0).toLowerCase() + story.interpretation.text.slice(1) : ' ' + story.interpretation.text);
    else if (story.kind === 'published-detail') reply += ' You can give it your own meaning through a memory, hobby or occasion.';
    return {kind:story.kind,provenance:story.provenance,productId:story.productId,handle:story.handle,libraryId:story.libraryId,libraryVersion:story.libraryVersion,motif:story.motif,context:story.context,facts:story.facts,interpretation:story.interpretation,sources:story.sources,recipient:recipient,occasion:occasion,reason:reason,detail:expanded?'expanded':'brief',reply:reply,text:reply};
  }
  return {normalizeStory:normalizeStory,storyMatchesProduct:storyMatchesProduct,storyConnection:storyConnection,safeText:text,sourceUrl:url,SOURCE_MS:SOURCE_MS,CURRENT_MS:CURRENT_MS};
}));
