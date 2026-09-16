// Minimal DOM/browser shim for driving SKU_List_V1.html's inline script in Node.
// Only the surface that page actually touches is implemented; anything it uses
// that is missing here shows up immediately as a TypeError rather than silently
// passing, which is the point.
const fs = require('node:fs');
const path = require('node:path');

function parseSelector(sel) {
  const s = sel.trim();
  const out = { tag: null, id: null, classes: [], attrs: [] };
  const re = /(^[a-zA-Z][\w-]*)|#([\w-]+)|\.([\w-]+)|\[([\w-]+)(?:=(?:"([^"]*)"|'([^']*)'))?\]/g;
  let m, consumed = 0;
  while ((m = re.exec(s))) {
    consumed = re.lastIndex;
    if (m[1]) out.tag = m[1].toLowerCase();
    else if (m[2]) out.id = m[2];
    else if (m[3]) out.classes.push(m[3]);
    else if (m[4]) out.attrs.push([m[4], m[5] !== undefined ? m[5] : (m[6] !== undefined ? m[6] : null)]);
  }
  if (consumed !== s.length) throw new Error('Unsupported selector in harness: ' + sel);
  return out;
}

const camel = k => k.replace(/-(\w)/g, (_, c) => c.toUpperCase());

class ClassList {
  constructor(el) { this.el = el; }
  get _set() { return new Set(String(this.el.className || '').split(/\s+/).filter(Boolean)); }
  _write(set) { this.el.className = [...set].join(' '); }
  add(...c) { const s = this._set; c.forEach(x => s.add(x)); this._write(s); }
  remove(...c) { const s = this._set; c.forEach(x => s.delete(x)); this._write(s); }
  contains(c) { return this._set.has(c); }
  toggle(c, force) {
    const on = force === undefined ? !this.contains(c) : !!force;
    on ? this.add(c) : this.remove(c);
    return on;
  }
}

class Style {
  setProperty(k, v) { this['--prop:' + k] = v; }
  getPropertyValue(k) { return this['--prop:' + k] || ''; }
}

class TextNode {
  constructor(t) { this._text = String(t); this.parentNode = null; this.children = []; }
  get textContent() { return this._text; }
  set textContent(v) { this._text = String(v); }
  _descendants(out = []) { return out; }
  matches() { return false; }
}

class Element {
  constructor(tag, doc) {
    this.tagName = String(tag).toUpperCase();
    this.ownerDocument = doc;
    this.children = [];
    this.parentNode = null;
    this.className = '';
    this.id = '';
    this.style = new Style();
    this.dataset = {};
    this.classList = new ClassList(this);
    this.listeners = Object.create(null);
    this._text = '';
    this.checked = false;
    this.value = '';
    this.disabled = false;
    // Layout is opt-in: 0 means "not laid out yet", which is exactly what the
    // zoom code treats as "cannot clamp yet".
    this.clientWidth = 0;
    this.clientHeight = 0;
  }
  get textContent() {
    if (this.children.length) return this.children.map(c => c.textContent).join('');
    return this._text;
  }
  set textContent(v) { this.children.forEach(c => (c.parentNode = null)); this.children = []; this._text = String(v); }
  set innerHTML(v) {
    if (String(v) !== '') throw new Error('Harness only supports innerHTML = ""');
    this.children.forEach(c => (c.parentNode = null));
    this.children = [];
    this._text = '';
  }
  get innerHTML() { return this.children.length || this._text ? '<…>' : ''; }
  appendChild(child) {
    if (child && child.isFragment) {
      for (const c of [...child.children]) { c.parentNode = this; this.children.push(c); }
      child.children = [];
      return child;
    }
    child.parentNode = this;
    this.children.push(child);
    return child;
  }
  append(...nodes) {
    for (const n of nodes) {
      this.appendChild(typeof n === 'string' ? new TextNode(n) : n);
    }
  }
  removeChild(child) { this.children = this.children.filter(c => c !== child); child.parentNode = null; }
  remove() { if (this.parentNode) this.parentNode.removeChild(this); }
  matches(sel) {
    const p = parseSelector(sel);
    if (p.tag && this.tagName.toLowerCase() !== p.tag) return false;
    if (p.id && this.id !== p.id) return false;
    if (p.classes.some(c => !this.classList.contains(c))) return false;
    for (const [k, v] of p.attrs) {
      const own = k.startsWith('data-') ? this.dataset[camel(k.slice(5))] : this[k];
      if (own === undefined || own === null) return false;
      if (v !== null && String(own) !== v) return false;
    }
    return true;
  }
  closest(sel) {
    let n = this;
    while (n) { if (n.matches && n.matches(sel)) return n; n = n.parentNode; }
    return null;
  }
  _descendants(out = []) { for (const c of this.children) { out.push(c); c._descendants(out); } return out; }
  querySelectorAll(sel) { return this._descendants().filter(e => e.matches && e.matches(sel)); }
  querySelector(sel) { return this.querySelectorAll(sel)[0] || null; }
  addEventListener(type, fn) { (this.listeners[type] ||= []).push(fn); }
  removeEventListener(type, fn) { this.listeners[type] = (this.listeners[type] || []).filter(f => f !== fn); }
  async dispatch(type, ev = {}) {
    const fns = [...(this.listeners[type] || [])];
    for (const fn of fns) await fn.call(this, { type, preventDefault() {}, ...ev });
  }
  setAttribute(name, value) {
    if (name.startsWith('data-')) this.dataset[camel(name.slice(5))] = String(value);
    else this[name] = String(value);
  }
  getAttribute(name) {
    const v = name.startsWith('data-') ? this.dataset[camel(name.slice(5))] : this[name];
    return v === undefined ? null : String(v);
  }
  select() {}
  focus() { if (this.ownerDocument) this.ownerDocument.activeElement = this; }
  getBoundingClientRect() {
    const w = this.clientWidth || 100, h = this.clientHeight || 100;
    const left = this._left ?? 0, top = this._top ?? 0;
    return { left, top, width: w, height: h, right: left + w, bottom: this._bottom ?? (top + h) };
  }
}

function makeStorage() {
  const map = new Map();
  return {
    getItem: k => (map.has(String(k)) ? map.get(String(k)) : null),
    setItem: (k, v) => map.set(String(k), String(v)),
    removeItem: k => map.delete(String(k)),
    clear: () => map.clear(),
    _dump: () => Object.fromEntries(map),
  };
}

// SKU_SOURCE lets a bisect run the same suite against another copy of the page.
const SOURCE_PATH = process.env.SKU_SOURCE || path.join(__dirname, '..', '..', 'SKU_List_V1.html');
const SOURCE = fs.readFileSync(SOURCE_PATH, 'utf8');

const EXPORTS = [
  // OAuth / tokens
  'startOAuth', 'bootOAuth', 'takeVerifier', 'stashVerifier', 'apiFetch', 'refreshAccessToken',
  'setTokens', 'getAccessToken', 'getRefreshToken', 'tokenIsFresh', 'secondsUntilExpiry',
  // catalog / search / sync
  'Catalog', 'openCatalogStore', 'syncCatalog', 'searchCatalog', 'normalizeText', 'toRecord',
  'listingUpdatedAt', 'pickPrimaryImage', 'skusFromInventory', 'Budget', 'countCalls',
  'fetchListingPage', 'fetchSections', 'runSync', 'rebuildCatalog',
  // view
  'applyFilters', 'renderMore', 'renderChips', 'renderMeters', 'formatPrice', 'highlightInto',
  'loadSelectedSet', 'makeSkuFrom', 'openSkuEditor', 'generateAndSaveSku', 'saveSkuUpdates',
  'fetchInventoryDetail', 'cardFor', 'refreshQuota',
];

/**
 * Boot the page in a fresh sandbox.
 *   search    - query string on the landing URL
 *   origin    - window origin (defaults to the app's hard-bound origin)
 *   fetchImpl - (url, init) => Response-ish
 *   storage   - preseeded localStorage entries
 *   confirm   - what window.confirm returns (default true)
 */
function createApp(opts = {}) {
  const origin = opts.origin || 'https://sku.goldenspike.app';
  const navigations = [];
  const alerts = [];
  const confirms = [];
  const warnings = [];
  const errors = [];

  const location = {
    origin,
    pathname: opts.pathname || '/',
    search: opts.search || '',
    get href() { return origin + this.pathname + this.search; },
    set href(v) { navigations.push(String(v)); },
  };
  const history = {
    replaceState(_s, _t, url) {
      const u = String(url);
      const q = u.indexOf('?');
      location.search = q === -1 ? '' : u.slice(q);
      location.pathname = (q === -1 ? u : u.slice(0, q)).replace(/^https?:\/\/[^/]+/, '') || '/';
    },
  };

  const localStorage = makeStorage();
  const sessionStorage = makeStorage();
  for (const [k, v] of Object.entries(opts.storage || {})) localStorage.setItem(k, v);

  const doc = { byId: new Map(), listeners: Object.create(null) };
  const document = {
    activeElement: null,
    createElement(tag) { return new Element(tag, document); },
    createTextNode(t) { return new TextNode(t); },
    createDocumentFragment() { const f = new Element('#fragment', document); f.isFragment = true; return f; },
    getElementById(id) { return doc.byId.get(id) || null; },
    addEventListener(t, fn) { (doc.listeners[t] ||= []).push(fn); },
    async dispatch(type, ev = {}) {
      for (const fn of [...(doc.listeners[type] || [])]) await fn({ type, preventDefault() {}, ...ev });
    },
  };
  const mk = (tag, id, extra = {}) => {
    const el = new Element(tag, document);
    el.id = id;
    Object.assign(el, extra);
    doc.byId.set(id, el);
    return el;
  };
  const els = {
    connectEtsyBtn: mk('button', 'connectEtsyBtn'),
    syncBtn: mk('button', 'syncBtn', { _text: 'Sync' }),
    searchInput: mk('input', 'searchInput', { value: '' }),
    searchClear: mk('button', 'searchClear'),
    searchHint: mk('span', 'searchHint'),
    authStatus: mk('span', 'authStatus', { className: 'status' }),
    chipRow: mk('div', 'chipRow'),
    resultMeter: mk('span', 'resultMeter', { className: 'meter' }),
    catalogMeter: mk('span', 'catalogMeter', { className: 'meter' }),
    apiMeter: mk('span', 'apiMeter', { className: 'meter' }),
    syncProgress: mk('i', 'syncProgress'),
    listContainer: mk('main', 'listContainer', { className: 'grid' }),
    sentinel: mk('div', 'sentinel'),
    endcap: mk('div', 'endcap'),
  };
  document.body = mk('body', '__body', { offsetHeight: 2000 });

  const fetchCalls = [];
  const fetchImpl = opts.fetchImpl || (() => { throw new Error('unexpected fetch'); });
  const fetchStub = async (url, init = {}) => {
    fetchCalls.push({ url: String(url), init, method: (init.method || 'GET').toUpperCase() });
    return normalizeResponse(await fetchImpl(String(url), init));
  };

  const timers = new Set();
  const setTimeoutStub = (fn, ms) => { const t = setTimeout(fn, ms); timers.add(t); if (t.unref) t.unref(); return t; };
  const clearTimeoutStub = t => { clearTimeout(t); timers.delete(t); };

  const consoleStub = {
    log: () => {},
    warn: (...a) => warnings.push(a.map(String).join(' ')),
    error: (...a) => errors.push(a.map(String).join(' ')),
  };

  // Records the sentinel callback so a test can simulate scrolling to the end.
  let ioCallback = null;
  class IntersectionObserverStub {
    constructor(cb) { ioCallback = cb; }
    observe() {}
    disconnect() {}
  }

  // Records observed elements so a test can fire a re-measure.
  const resizeObserved = [];
  class ResizeObserverStub {
    constructor(cb) { this.cb = cb; }
    observe(el) { resizeObserved.push({ el, cb: this.cb }); }
    disconnect() {}
  }

  const window = {
    location,
    innerHeight: 900,
    scrollY: 0,
    scrollTo() {},
    addEventListener() {},
  };

  const body = SOURCE.match(/<script>([\s\S]*?)<\/script>/)[1];
  const factory = new Function(
    'window', 'document', 'location', 'history', 'localStorage', 'sessionStorage',
    'fetch', 'alert', 'confirm', 'console', 'setTimeout', 'clearTimeout', 'IntersectionObserver',
    'ResizeObserver',
    `${body}\nreturn { ${EXPORTS.join(', ')} };`
  );
  const api = factory(
    window, document, location, history, localStorage, sessionStorage,
    fetchStub,
    msg => alerts.push(String(msg)),
    msg => { confirms.push(String(msg)); return opts.confirm !== false; },
    consoleStub, setTimeoutStub, clearTimeoutStub, IntersectionObserverStub, ResizeObserverStub
  );

  return {
    api, els, document, window, location, history, localStorage, sessionStorage,
    navigations, alerts, confirms, warnings, errors, fetchCalls,
    status: () => els.authStatus.textContent,
    cards: () => els.listContainer.querySelectorAll('.card'),
    cardIds: () => els.listContainer.querySelectorAll('.card').map(c => Number(c.dataset.listingId)),
    titles: () => els.listContainer.querySelectorAll('.title').map(t => t.textContent),
    chips: () => els.chipRow.children.map(c => ({ label: c.textContent, on: c.classList.contains('on'), el: c })),
    notice: () => els.listContainer.querySelector('.notice')?.textContent || '',
    meters: () => ({
      result: els.resultMeter.textContent,
      catalog: els.catalogMeter.textContent,
      api: els.apiMeter.textContent,
    }),
    /** Simulate scrolling the sentinel into view. */
    async scrollToEnd(times = 1) {
      for (let i = 0; i < times; i++) {
        if (!ioCallback) throw new Error('no IntersectionObserver registered');
        await ioCallback([{ isIntersecting: true }]);
      }
    },
    async type(text) { els.searchInput.value = text; await els.searchInput.dispatch('input'); },
    /** Give a card's image box real geometry, as a browser would after layout. */
    layout(card, size = 300) {
      const box = card.querySelector('.thumb-wrap');
      const img = card.querySelector('.thumb');
      box.clientWidth = box.clientHeight = size;
      img.clientWidth = img.clientHeight = size;
      return { box, img };
    },
    /** Fire every registered ResizeObserver, as a relayout would. */
    async resize() { for (const { el, cb } of resizeObserved) await cb([{ target: el }]); },
    /** The transform currently applied to a card's image. */
    transform(card) { return card.querySelector('.thumb').style.transform || ''; },
    zoomState(card) {
      const img = card.querySelector('.thumb');
      return {
        scale: parseFloat(img.dataset.scale),
        x: parseFloat(img.dataset.offsetX),
        y: parseFloat(img.dataset.offsetY),
      };
    },
    /** Does this element have a listener of this type at all? */
    hasListener(el, type) { return (el.listeners[type] || []).length > 0; },
    async domReady() { for (const fn of doc.listeners.DOMContentLoaded || []) await fn(); },
    cleanup() { timers.forEach(t => clearTimeout(t)); timers.clear(); },
  };
}

function normalizeResponse(res) {
  if (res && typeof res.json === 'function' && typeof res.ok === 'boolean') return res;
  const status = (res && res.status) || 200;
  const bodyVal = res && 'body' in res ? res.body : res;
  const text = typeof bodyVal === 'string' ? bodyVal : JSON.stringify(bodyVal ?? {});
  return {
    ok: status >= 200 && status < 300,
    status,
    json: async () => JSON.parse(text),
    text: async () => text,
  };
}

module.exports = { createApp, Element, parseSelector };
