// Storine Console: a single-page app over the console's JSON API (see src/api.rs).
// No libraries and no build step: every element is built with h(), never from HTML strings.
//
// State lives in three places:
// - `app`: a small store for what the server tells us (session, overview, live samples).
//   Parts of the page subscribe to it and repaint when it changes.
// - `prefs`: what the user chose, kept in localStorage per server: each collection's query,
//   columns and analytics, query history, saved queries, pins, recent documents, theme.
// - `drafts`: unsaved document edits, kept in sessionStorage for this browser tab.
'use strict';

const KEY_STORE = 'storine-console-key';
const PREFS_STORE = 'storine-console-prefs';
const DRAFTS_STORE = 'storine-console-drafts';
const PAGE_SIZES = [25, 50, 100, 250];
const LIVE_MS = 5000;
const IDLE_POLL_MS = 30000;
const MAX_SAMPLES = 60;
const MAX_HISTORY = 15;
const MAX_RECENT = 12;
const MAX_SCOPES = 8;
const OPS = [
  ['eq', '='], ['ne', '≠'], ['lt', '<'], ['le', '≤'], ['gt', '>'], ['ge', '≥'],
  ['between', 'between'], ['in', 'is one of'], ['prefix', 'starts with'], ['contains', 'contains'],
  ['exists', 'exists'], ['missing', 'is missing'],
];
const AGGS = [
  ['count', 'count'], ['sum', 'sum'], ['avg', 'average'], ['min', 'min'], ['max', 'max'],
  ['count_distinct', 'count distinct'], ['approx_count_distinct', 'approx. distinct'],
];
const TAGS = { $int: 'int', $float: 'float', $decimal: 'decimal', $time: 'time', $bytes: 'bytes', $uuid: 'uuid', $id: 'id' };
const TABS = [['documents', 'Documents', 'doc'], ['analytics', 'Analytics', 'chart'], ['indexes', 'Indexes', 'bolt'], ['settings', 'Settings', 'tune']];
const IS_MAC = /Mac|iPhone|iPad/.test(navigator.platform || navigator.userAgent);
const MOD = IS_MAC ? '⌘' : 'Ctrl';

const ICONS = {
  db: 'M12 3C7.6 3 4 4.3 4 6v12c0 1.7 3.6 3 8 3s8-1.3 8-3V6c0-1.7-3.6-3-8-3zm0 2c3.9 0 6 1.1 6 1s-2.1 1-6 1-6-.9-6-1 2.1-1 6-1zm6 13c0 .1-2.1 1-6 1s-6-.9-6-1v-2.3c1.5.8 3.7 1.3 6 1.3s4.5-.5 6-1.3V18zm0-5c0 .1-2.1 1-6 1s-6-.9-6-1v-2.3c1.5.8 3.7 1.3 6 1.3s4.5-.5 6-1.3V13z',
  grid: 'M4 13h6V4H4v9zm0 7h6v-5H4v5zm10 0h6v-9h-6v9zm0-16v5h6V4h-6z',
  search: 'M15.5 14h-.79l-.28-.27A6.47 6.47 0 0 0 16 9.5 6.5 6.5 0 1 0 9.5 16c1.61 0 3.09-.59 4.23-1.57l.27.28v.79l5 4.99L20.49 19l-4.99-5zm-6 0C7.01 14 5 11.99 5 9.5S7.01 5 9.5 5 14 7.01 14 9.5 11.99 14 9.5 14z',
  moon: 'M12 3a9 9 0 1 0 9 9 7 7 0 0 1-9-9z',
  sun: 'M12 7a5 5 0 1 0 0 10 5 5 0 0 0 0-10zM2 13h2v-2H2v2zm18 0h2v-2h-2v2zM11 2v2h2V2h-2zm0 18v2h2v-2h-2zM5.99 4.58 4.58 5.99l1.41 1.41 1.41-1.41-1.41-1.41zm12.02 12.02-1.41 1.41 1.41 1.41 1.41-1.41-1.41-1.41zm1.41-10.61-1.41-1.41-1.41 1.41 1.41 1.41 1.41-1.41zM7.4 18.01 5.99 16.6l-1.41 1.41 1.41 1.41 1.41-1.41z',
  monitor: 'M20 3H4a2 2 0 0 0-2 2v10a2 2 0 0 0 2 2h6v2H8v2h8v-2h-2v-2h6a2 2 0 0 0 2-2V5a2 2 0 0 0-2-2zm0 12H4V5h16v10z',
  pin: 'M16 9V4h1V2H7v2h1v5l-2 2v2h5.2v7h1.6v-7H18v-2l-2-2z',
  plus: 'M19 13h-6v6h-2v-6H5v-2h6V5h2v6h6v2z',
  close: 'M19 6.41 17.59 5 12 10.59 6.41 5 5 6.41 10.59 12 5 17.59 6.41 19 12 13.41 17.59 19 19 17.59 13.41 12z',
  play: 'M8 5v14l11-7z',
  history: 'M13 3a9 9 0 0 0-9 9H1l3.89 3.89.07.14L9 12H6c0-3.87 3.13-7 7-7s7 3.13 7 7-3.13 7-7 7c-1.93 0-3.68-.79-4.94-2.06l-1.42 1.42A8.95 8.95 0 0 0 13 21a9 9 0 0 0 0-18zm-1 5v5l4.28 2.54.72-1.21-3.5-2.08V8H12z',
  star: 'M12 17.27 18.18 21l-1.64-7.03L22 9.24l-7.19-.61L12 2 9.19 8.63 2 9.24l5.46 4.73L5.82 21z',
  columns: 'M10 18h5V5h-5v13zm-6 0h5V5H4v13zM16 5v13h5V5h-5z',
  download: 'M19 9h-4V3H9v6H5l7 7 7-7zM5 18v2h14v-2H5z',
  copy: 'M16 1H4a2 2 0 0 0-2 2v14h2V3h12V1zm3 4H8a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h11a2 2 0 0 0 2-2V7a2 2 0 0 0-2-2zm0 16H8V7h11v14z',
  trash: 'M6 19a2 2 0 0 0 2 2h8a2 2 0 0 0 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z',
  left: 'M15.41 7.41 14 6l-6 6 6 6 1.41-1.41L10.83 12z',
  right: 'M10 6 8.59 7.41 13.17 12l-4.58 4.59L10 18l6-6z',
  up: 'M7.41 15.41 12 10.83l4.59 4.58L18 14l-6-6-6 6z',
  down: 'M7.41 8.59 12 13.17l4.59-4.58L18 10l-6 6-6-6 1.41-1.41z',
  refresh: 'M17.65 6.35A7.96 7.96 0 0 0 12 4a8 8 0 1 0 7.73 10h-2.08A6 6 0 1 1 12 6c1.66 0 3.14.69 4.22 1.78L13 11h7V4l-2.35 2.35z',
  pause: 'M6 19h4V5H6v14zm8-14v14h4V5h-4z',
  menu: 'M3 6h18v2H3zm0 5h18v2H3zm0 5h18v2H3z',
  doc: 'M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8l-6-6zm4 18H6V4h7v5h5v11z',
  bolt: 'M7 2v11h3v9l7-12h-4l4-8z',
  chart: 'M5 9.2h3V19H5zM10.6 5h2.8v14h-2.8zm5.6 8H19v6h-2.8z',
  table: 'M3 3h18v18H3V3zm2 2v4h14V5H5zm0 6v3h6v-3H5zm8 0v3h6v-3h-6zm-8 5v3h6v-3H5zm8 0v3h6v-3h-6z',
  tune: 'M3 17v2h6v-2H3zM3 5v2h10V5H3zm10 16v-2h8v-2h-8v-2h-2v6h2zM7 9v2H3v2h4v2h2V9H7zm14 4v-2H11v2h10zm-6-4h2V7h4V5h-4V3h-2v6z',
  keyboard: 'M20 5H4a2 2 0 0 0-2 2v10a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2V7a2 2 0 0 0-2-2zm-9 3h2v2h-2V8zm0 3h2v2h-2v-2zM8 8h2v2H8V8zm0 3h2v2H8v-2zm-1 2H5v-2h2v2zm0-3H5V8h2v2zm9 7H8v-2h8v2zm0-4h-2v-2h2v2zm0-3h-2V8h2v2zm3 3h-2v-2h2v2zm0-3h-2V8h2v2z',
  logout: 'M10.09 15.59 11.5 17l5-5-5-5-1.41 1.41L12.67 11H3v2h9.67l-2.58 2.59zM19 3H5a2 2 0 0 0-2 2v4h2V5h14v14H5v-4H3v4a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2V5a2 2 0 0 0-2-2z',
  tree: 'M22 11V3h-7v3H9V3H2v8h7V8h2v10h4v3h7v-8h-7v3h-2V8h2v3z',
  code: 'M9.4 16.6 4.8 12l4.6-4.6L8 6l-6 6 6 6 1.4-1.4zm5.2 0 4.6-4.6-4.6-4.6L16 6l6 6-6 6-1.4-1.4z',
  dup: 'M14 2H6a2 2 0 0 0-2 2v12h2V4h8V2zm3 4h-7a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h7a2 2 0 0 0 2-2V8a2 2 0 0 0-2-2zm0 14h-7V8h7v12z',
  filter: 'M10 18h4v-2h-4v2zM3 6v2h18V6H3zm3 7h12v-2H6v2z',
  density: 'M21 8H3V4h18v4zm0 2H3v4h18v-4zm0 6H3v4h18v-4z',
  key: 'M12.65 10A6 6 0 1 0 12.65 14H17v4h4v-4h2v-4H12.65zM7 14a2 2 0 1 1 0-4 2 2 0 0 1 0 4z',
  sort: 'M3 18h6v-2H3v2zM3 6v2h18V6H3zm0 7h12v-2H3v2z',
  enter: 'M19 7v4H5.83l3.58-3.59L8 6l-6 6 6 6 1.41-1.41L5.83 13H21V7h-2z',
  node: 'M20 13H4a1 1 0 0 0-1 1v6a1 1 0 0 0 1 1h16a1 1 0 0 0 1-1v-6a1 1 0 0 0-1-1zM7 19a2 2 0 1 1 0-4 2 2 0 0 1 0 4zM20 3H4a1 1 0 0 0-1 1v6a1 1 0 0 0 1 1h16a1 1 0 0 0 1-1V4a1 1 0 0 0-1-1zM7 9a2 2 0 1 1 0-4 2 2 0 0 1 0 4z',
  pulse: 'M3 13h4l3-8 4 14 3-6h4v-2h-5.2L14 15 10 1 6.2 11H3z',
};

// ---------------------------------------------------------------------------------------------
// Storage and state
// ---------------------------------------------------------------------------------------------

function storage(area) {
  return {
    get(k) { try { return area().getItem(k); } catch { return null; } },
    set(k, v) { try { area().setItem(k, v); } catch { /* storage unavailable or full */ } },
    del(k) { try { area().removeItem(k); } catch { /* storage unavailable */ } },
  };
}
const local = storage(() => localStorage);
const session = storage(() => sessionStorage);

function readJson(s, k) {
  try { const v = JSON.parse(s.get(k) || 'null'); return v && typeof v === 'object' ? v : null; } catch { return null; }
}

function debounced(fn, ms) {
  let t = null;
  const run = () => { t = null; fn(); };
  const call = () => { clearTimeout(t); t = setTimeout(run, ms); };
  call.flush = () => { if (t) { clearTimeout(t); run(); } };
  return call;
}

// A minimal store: set() merges, then every subscriber sees the new state.
function createStore(initial) {
  let state = initial;
  const subs = new Set();
  return {
    get: () => state,
    set(patch) {
      state = { ...state, ...(typeof patch === 'function' ? patch(state) : patch) };
      for (const f of [...subs]) f(state);
    },
    subscribe(f) { subs.add(f); return () => subs.delete(f); },
  };
}

const app = createStore({ key: null, session: null, overview: null, collections: [], samples: [], updatedAt: 0, latency: null, offline: false });

function stripCredentials(url) { return String(url || '').replace(/\/\/[^@/]*@/, '//'); }

function loadPrefs() {
  const p = readJson(local, PREFS_STORE) || {};
  const out = { theme: 'system', density: 'comfortable', navHidden: false, live: true, drawerMode: 'tree', servers: [], scopes: {}, ...p };
  // Settings of the first console version.
  if (!p.theme) {
    const t = local.get('storine-console-theme');
    if (t === 'light' || t === 'dark') out.theme = t;
  }
  if (!Array.isArray(out.servers)) out.servers = [];
  if (out.servers.length === 0 && local.get('storine-console-url')) out.servers = [stripCredentials(local.get('storine-console-url'))];
  if (!out.scopes || typeof out.scopes !== 'object') out.scopes = {};
  return out;
}

const prefs = loadPrefs();
const savePrefs = debounced(() => local.set(PREFS_STORE, JSON.stringify(prefs)), 200);
const drafts = readJson(session, DRAFTS_STORE) || {};
const saveDrafts = debounced(() => session.set(DRAFTS_STORE, JSON.stringify(drafts)), 300);
window.addEventListener('pagehide', () => { savePrefs.flush(); saveDrafts.flush(); });

// What this browser remembers about the connected server.
function scope() {
  const url = (app.get().session && app.get().session.url) || '';
  let s = prefs.scopes[url];
  if (!s) {
    s = prefs.scopes[url] = { pinned: [], recent: [], views: {}, history: {}, saved: {}, used: Date.now() };
    const names = Object.keys(prefs.scopes);
    if (names.length > MAX_SCOPES) {
      names.sort((a, b) => (prefs.scopes[a].used || 0) - (prefs.scopes[b].used || 0));
      for (const n of names.slice(0, names.length - MAX_SCOPES)) if (n !== url) delete prefs.scopes[n];
    }
    savePrefs();
  }
  for (const k of ['pinned', 'recent']) if (!Array.isArray(s[k])) s[k] = [];
  for (const k of ['views', 'history', 'saved']) if (!s[k] || typeof s[k] !== 'object') s[k] = {};
  return s;
}

// In-memory state of each collection page: results, paging cursors, selection.
const runtimes = new Map();
function runtimeOf(name) {
  if (!runtimes.has(name)) runtimes.set(name, { cursors: [null], page: 0, result: null, sorted: false, seq: 0, cursor: -1, selected: null, took: 0, agg: null });
  return runtimes.get(name);
}

// Things the current page registered, undone when it is replaced.
const page = { cleanup: [], docs: null };

// ---------------------------------------------------------------------------------------------
// DOM helpers
// ---------------------------------------------------------------------------------------------

function h(tag, props, ...children) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(props || {})) {
    if (v === null || v === undefined || v === false) continue;
    if (k === 'class') el.className = v;
    else if (k === 'text') el.textContent = v;
    else if (k.startsWith('on')) el.addEventListener(k.slice(2), v);
    else if (k === 'value') el.value = v;
    else if (k === 'checked') el.checked = !!v;
    // Through the CSSOM: the page's CSP blocks style attributes.
    else if (k === 'style') el.style.cssText = v;
    else if (v === true) el.setAttribute(k, '');
    else el.setAttribute(k, v);
  }
  for (const c of children.flat(Infinity)) {
    if (c === null || c === undefined || c === false) continue;
    el.append(c instanceof Node ? c : document.createTextNode(String(c)));
  }
  return el;
}

const SVG_NS = 'http://www.w3.org/2000/svg';
function svgEl(tag, attrs, ...children) {
  const el = document.createElementNS(SVG_NS, tag);
  for (const [k, v] of Object.entries(attrs || {})) if (v !== null && v !== undefined && v !== false) el.setAttribute(k, v);
  for (const c of children.flat()) if (c) el.append(c);
  return el;
}

function icon(name, size) {
  const n = size || 16;
  return svgEl('svg', { viewBox: '0 0 24 24', width: n, height: n, 'aria-hidden': 'true', class: 'ico' }, svgEl('path', { d: ICONS[name] || '', fill: 'currentColor' }));
}

function btn(label, o) {
  const opts = o || {};
  const cls = ['btn', opts.kind || '', opts.small ? 'small' : '', label ? '' : 'only-icon', opts.on ? 'on' : ''].filter(Boolean).join(' ');
  return h('button', {
    class: cls, type: 'button', title: opts.title || null, 'aria-label': !label && opts.title ? opts.title : null,
    disabled: opts.disabled, onclick: opts.onclick, 'aria-haspopup': opts.menu ? 'menu' : null, 'aria-expanded': opts.menu ? 'false' : null,
  },
  opts.icon ? icon(opts.icon, opts.small ? 14 : 16) : null,
  label ? h('span', {}, label) : null,
  opts.count ? h('span', { class: 'count-pill' }, String(opts.count)) : null,
  opts.kbd ? h('kbd', {}, opts.kbd) : null);
}

function iconBtn(name, title, onclick, extra) {
  return h('button', { class: `icon-btn${extra && extra.small ? ' small' : ''}`, type: 'button', title, 'aria-label': title, onclick, disabled: extra && extra.disabled }, icon(name, extra && extra.small ? 14 : 18));
}

function seg(options, value, onpick) {
  return h('div', { class: 'seg', role: 'group' }, options.map(([v, label, ic]) => h('button', {
    type: 'button', class: v === value ? 'on' : '', 'aria-pressed': v === value ? 'true' : 'false', onclick: () => onpick(v),
  }, ic ? icon(ic, 14) : null, label)));
}

const $ = (id) => document.getElementById(id);
// replaceChildren() with h()'s rules: arrays are flattened, null and false are skipped.
function fill(el, ...children) {
  el.replaceChildren(...children.flat(Infinity).filter((c) => c !== null && c !== undefined && c !== false));
  return el;
}
const fmt = new Intl.NumberFormat();
const fmt1 = new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 });
const num = (n) => (typeof n === 'number' ? fmt.format(n) : n === null || n === undefined ? '—' : String(n));
const enc = (s) => encodeURIComponent(s);
const plural = (n, one, many) => `${num(n)} ${n === 1 ? one : many || `${one}s`}`;

function bytes(n) {
  if (typeof n !== 'number') return '—';
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let i = 0;
  while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
  return `${i === 0 ? n : n.toFixed(n < 10 ? 2 : 1)} ${units[i]}`;
}

function rate(n) {
  if (typeof n !== 'number') return '—';
  if (n >= 1e6) return `${fmt1.format(n / 1e6)}M`;
  if (n >= 1e4) return `${fmt1.format(n / 1e3)}k`;
  return n >= 100 ? fmt.format(Math.round(n)) : fmt1.format(n);
}

function ago(t) {
  if (!t) return '';
  const s = Math.max(0, Math.round((Date.now() - t) / 1000));
  if (s < 5) return 'just now';
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ago`;
  return new Date(t).toLocaleDateString();
}

function agoEl(t, prefix) {
  return h('span', { 'data-ago': String(t || 0), 'data-prefix': prefix || '' }, t ? `${prefix || ''}${ago(t)}` : '');
}

function tickClocks() {
  for (const el of document.querySelectorAll('[data-ago]')) {
    const t = Number(el.getAttribute('data-ago'));
    if (t) el.textContent = `${el.getAttribute('data-prefix') || ''}${ago(t)}`;
  }
}

function typing(el) {
  return !!(el && el.closest && el.closest('input, textarea, select, [contenteditable="true"]'));
}

function download(name, text) {
  const url = URL.createObjectURL(new Blob([text], { type: 'application/json' }));
  const a = h('a', { href: url, download: name });
  document.body.append(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

async function copyText(text, what) {
  try { await navigator.clipboard.writeText(text); toast(`Copied ${what || ''}`.trim(), { kind: 'ok' }); } catch { toast('The browser did not allow copying', { kind: 'error' }); }
}

// ---------------------------------------------------------------------------------------------
// Toasts, popovers, dialogs
// ---------------------------------------------------------------------------------------------

function toast(message, o) {
  const opts = typeof o === 'string' ? { kind: o } : (o || {});
  const ms = opts.timeout || (opts.kind === 'error' ? 7000 : opts.action ? 7000 : 3500);
  const t = h('div', { class: `toast ${opts.kind || ''}`, role: opts.kind === 'error' ? 'alert' : 'status' }, h('span', { class: 'toast-msg' }, message));
  let timer = null;
  const close = () => {
    clearTimeout(timer);
    t.classList.add('leaving');
    setTimeout(() => t.remove(), 170);
  };
  if (opts.action) t.append(h('button', { class: 'toast-action', type: 'button', onclick: () => { close(); opts.action.run(); } }, opts.action.label));
  t.append(h('button', { class: 'toast-close', type: 'button', 'aria-label': 'Dismiss', onclick: close }, icon('close', 14)));
  const bar = h('span', { class: 'timer' });
  bar.style.animationDuration = `${ms}ms`;
  t.append(bar);
  $('toasts').append(t);
  timer = setTimeout(close, ms);
}

let openPop = null;

function closePopover() {
  if (!openPop) return;
  openPop.el.remove();
  openPop.anchor.setAttribute('aria-expanded', 'false');
  document.removeEventListener('pointerdown', popOutside, true);
  openPop = null;
}

function popOutside(e) {
  if (openPop && !openPop.el.contains(e.target) && !openPop.anchor.contains(e.target)) closePopover();
}

// Opens `build(close)` under `anchor`; a second click on the anchor closes it.
function popover(anchor, build, align) {
  if (openPop && openPop.anchor === anchor) { closePopover(); return; }
  closePopover();
  const el = h('div', { class: 'popover', role: 'menu' });
  el.append(...[build(closePopover)].flat(Infinity).filter(Boolean));
  document.body.append(el);
  const r = anchor.getBoundingClientRect();
  const w = el.offsetWidth;
  let left = align === 'end' ? r.right - w : r.left;
  left = Math.max(8, Math.min(left, innerWidth - w - 8));
  let top = r.bottom + 6;
  if (top + el.offsetHeight > innerHeight - 8) top = Math.max(8, r.top - el.offsetHeight - 6);
  el.style.left = `${left}px`;
  el.style.top = `${top}px`;
  anchor.setAttribute('aria-expanded', 'true');
  openPop = { el, anchor };
  setTimeout(() => document.addEventListener('pointerdown', popOutside, true));
  const first = el.querySelector('button, input');
  if (first) first.focus();
}

function menuItem(ic, label, opts) {
  const o = opts || {};
  return h('button', { class: 'menu-item', type: 'button', role: 'menuitem', onclick: o.onclick, title: o.title || null },
    ic ? icon(ic, 15) : null,
    h('span', { class: 'text' }, h('span', { class: 'main-line' }, label), o.sub ? h('span', { class: 'sub-line' }, o.sub) : null),
    o.trail || null);
}

function dialogOpen() { return $('palette').open || $('modal').open; }

// A modal dialog; resolves with the value of the button pressed, or null when dismissed.
// `content` may be a function given `finish`, for content that ends the dialog itself.
function modal(title, content, buttons) {
  const dlg = $('modal');
  return new Promise((resolve) => {
    let done = false;
    const finish = (v) => {
      if (done) return;
      done = true;
      dlg.close();
      resolve(v);
    };
    const body = typeof content === 'function' ? content(finish) : content;
    const foot = h('div', { class: 'modal-foot' }, buttons.map((b) => btn(b.label, { kind: b.kind, onclick: () => finish(typeof b.value === 'function' ? b.value() : b.value) })));
    dlg.replaceChildren(h('div', { class: 'modal-head' }, h('h2', {}, title)), h('div', { class: 'modal-body' }, body), foot);
    dlg.oncancel = (e) => { e.preventDefault(); finish(null); };
    dlg.onclick = (e) => { if (e.target === dlg) finish(null); };
    closePopover();
    dlg.showModal();
    const focus = dlg.querySelector('input') || foot.lastChild;
    if (focus) focus.focus();
  });
}

function confirmDialog(title, text, confirm, danger) {
  return modal(title, h('p', {}, text), [
    { label: 'Cancel', value: false },
    { label: confirm || 'Confirm', kind: danger ? 'danger' : 'primary', value: true },
  ]).then((v) => v === true);
}

function promptDialog(title, label, o) {
  const opts = o || {};
  let input;
  return modal(title, (finish) => {
    input = h('input', { type: 'text', value: opts.value || '', placeholder: opts.placeholder || '', spellcheck: 'false', autocomplete: 'off',
      onkeydown: (e) => { if (e.key === 'Enter') { e.preventDefault(); finish(input.value); } } });
    return [opts.text ? h('p', {}, opts.text) : null, h('label', { class: 'field' }, label, input)];
  }, [
    { label: 'Cancel', value: null },
    { label: opts.confirm || 'OK', kind: 'primary', value: () => input.value },
  ]).then((v) => (v === null || v === undefined ? null : String(v).trim() || null));
}

// ---------------------------------------------------------------------------------------------
// API
// ---------------------------------------------------------------------------------------------

class ApiError extends Error {
  constructor(status, message) { super(message); this.status = status; }
}

async function api(method, path, body, opts) {
  const headers = { 'X-Console-Key': app.get().key || '' };
  let payload;
  if (body !== undefined) {
    headers['Content-Type'] = 'application/json';
    payload = opts && opts.raw ? body : JSON.stringify(body);
  }
  let res;
  try {
    res = await fetch(`/api/${path}`, { method, headers, body: payload, cache: 'no-store', credentials: 'omit' });
  } catch {
    throw new ApiError(0, 'The console is not answering. Is storine-console still running?');
  }
  const text = await res.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch { data = null; }
  if (!res.ok) {
    const msg = (data && data.error) || text || res.statusText;
    if (res.status === 401) showLocked();
    throw new ApiError(res.status, msg);
  }
  return data;
}

// Loads the overview and records a sample of the counters for the live charts.
let polling = null;
function poll() {
  if (polling) return polling;
  const t0 = performance.now();
  polling = api('GET', 'overview').then((o) => {
    const hl = o.health || {};
    const docs = o.collections.reduce((a, x) => a + (typeof x.count === 'number' ? x.count : 0), 0);
    const sample = { t: Date.now(), reads: hl.total_reads, writes: hl.total_writes, dirty: hl.hot_dirty_bytes, docs };
    const samples = [...app.get().samples, sample].slice(-MAX_SAMPLES);
    app.set({ overview: o, collections: o.collections, samples, updatedAt: Date.now(), latency: performance.now() - t0, offline: false });
    return o;
  }, (e) => {
    if (e.status === 503 || e.status === 0) app.set({ offline: true });
    throw e;
  }).finally(() => { polling = null; });
  return polling;
}

// Per-second rates between consecutive samples of a counter.
function rates(samples, field) {
  const out = [];
  for (let i = 1; i < samples.length; i++) {
    const a = samples[i - 1];
    const b = samples[i];
    const dt = (b.t - a.t) / 1000;
    const d = (b[field] || 0) - (a[field] || 0);
    out.push(dt > 0 && d >= 0 ? d / dt : 0);
  }
  return out;
}

// ---------------------------------------------------------------------------------------------
// Values
// ---------------------------------------------------------------------------------------------

function tagOf(v) {
  if (v && typeof v === 'object' && !Array.isArray(v)) {
    const ks = Object.keys(v);
    if (ks.length === 1 && TAGS[ks[0]] && typeof v[ks[0]] === 'string') return ks[0];
  }
  return null;
}

function numberOf(v) {
  if (typeof v === 'number') return v;
  const t = tagOf(v);
  if (t === '$int' || t === '$decimal' || t === '$float') { const n = Number(v[t]); return Number.isFinite(n) ? n : null; }
  return null;
}

// A compact cell for a table.
function cell(v) {
  if (v === undefined) return h('span', { class: 'v-dim' }, '');
  if (v === null) return h('span', { class: 'v-null' }, 'null');
  if (typeof v === 'string') return h('span', { class: 'v-str', title: v.length > 60 ? v.slice(0, 2000) : null }, v);
  if (typeof v === 'number') return h('span', { class: 'v-num' }, String(v));
  if (typeof v === 'boolean') return h('span', { class: 'v-lit' }, String(v));
  if (Array.isArray(v)) return h('span', { class: 'v-dim' }, `[${plural(v.length, 'item')}]`);
  const tag = tagOf(v);
  if (tag === '$bytes') return h('span', { class: 'v-tag' }, `bytes · ${bytes(Math.floor(v.$bytes.replace(/=+$/, '').length * 3 / 4))}`);
  if (tag) return h('span', { class: tag === '$int' || tag === '$decimal' || tag === '$float' ? 'v-num' : 'v-tag', title: TAGS[tag] }, v[tag]);
  return h('span', { class: 'v-dim' }, `{${plural(Object.keys(v).length, 'field')}}`);
}

// A full value for the tree view.
function leaf(v) {
  if (typeof v === 'string') return h('span', { class: 'v-str' }, JSON.stringify(v));
  const tag = tagOf(v);
  if (tag) return h('span', {}, h('span', { class: 't-type' }, TAGS[tag]), h('span', { class: tag === '$bytes' ? 'v-tag' : numberOf(v) !== null ? 'v-num' : 'v-tag' }, tag === '$bytes' ? bytes(Math.floor(v.$bytes.replace(/=+$/, '').length * 3 / 4)) : v[tag]));
  if (Array.isArray(v) && v.length === 0) return h('span', { class: 'v-dim' }, '[]');
  if (v && typeof v === 'object' && Object.keys(v).length === 0) return h('span', { class: 'v-dim' }, '{}');
  return cell(v);
}

function tree(v, label, depth) {
  const d = depth || 0;
  const container = v && typeof v === 'object' && !tagOf(v) && (Array.isArray(v) ? v.length > 0 : Object.keys(v).length > 0);
  if (!container) return h('div', { class: 't-leaf' }, label !== undefined ? h('span', { class: 't-key' }, label) : null, leaf(v));
  const entries = Array.isArray(v) ? v.map((x, i) => [String(i), x]) : Object.entries(v);
  const kids = entries.map(([k, x]) => tree(x, k, d + 1));
  if (label === undefined) return h('div', { class: 'tree' }, kids);
  const summary = Array.isArray(v) ? `[${entries.length}]` : `{${entries.length}}`;
  return h('details', { open: d < 3 && entries.length <= 40 },
    h('summary', {}, h('span', { class: 't-key' }, label), h('span', { class: 't-sum' }, summary)),
    h('div', { class: 't-kids' }, kids));
}

// Pretty JSON text with syntax colours, built as text nodes.
function highlighted(text) {
  const pre = h('pre', { class: 'json' });
  const re = /("(?:\\.|[^"\\])*")(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/g;
  let last = 0;
  for (const m of text.matchAll(re)) {
    if (m.index > last) pre.append(text.slice(last, m.index));
    if (m[1]) {
      pre.append(h('span', { class: m[2] ? 'j-key' : 'j-str' }, m[1]));
      if (m[2]) pre.append(m[2]);
    } else {
      pre.append(h('span', { class: m[3] ? 'j-lit' : 'j-num' }, m[0]));
    }
    last = m.index + m[0].length;
  }
  pre.append(text.slice(last));
  return pre;
}

// What a filter value box means: JSON if it parses (42, true, "x", {"$decimal":"1.5"}), else text.
function literal(text) {
  const t = text.trim();
  if (t === '') return '';
  try { return JSON.parse(t); } catch { return text; }
}

function sparkline(values, level) {
  const w = 200;
  const ht = 34;
  const svg = svgEl('svg', { viewBox: `0 0 ${w} ${ht}`, width: '100%', height: ht, preserveAspectRatio: 'none', class: 'spark', 'aria-hidden': 'true' });
  if (values.length < 2) {
    svg.append(svgEl('line', { x1: 0, y1: ht - 1, x2: w, y2: ht - 1, class: 'spark-base' }));
    return svg;
  }
  const lo = level ? Math.min(...values) : 0;
  const span = Math.max(...values) - lo;
  const step = w / (values.length - 1);
  const y = (v) => (span > 0 ? ht - 2 - ((v - lo) / span) * (ht - 6) : level ? ht / 2 : ht - 2);
  const pts = values.map((v, i) => [i * step, y(v)]);
  const line = pts.map(([x, y], i) => `${i ? 'L' : 'M'}${x.toFixed(1)} ${y.toFixed(1)}`).join(' ');
  svg.append(
    svgEl('path', { d: `${line} L${w} ${ht} L${pts[0][0].toFixed(1)} ${ht} Z`, class: 'spark-area' }),
    svgEl('path', { d: line, class: 'spark-line' }));
  return svg;
}

function skeletonTable(rows, cols) {
  return h('div', { class: 'table-wrap' }, h('table', {},
    h('thead', {}, h('tr', {}, Array.from({ length: cols }, () => h('th', {}, h('span', { class: 'skel', style: 'width:60px' }))))),
    h('tbody', {}, Array.from({ length: rows }, (_, r) => h('tr', {}, Array.from({ length: cols }, (_, c) => h('td', {},
      h('span', { class: 'skel', style: `width:${40 + ((r * 7 + c * 13) % 50)}%` }))))))));
}

function emptyState(ic, title, text, action) {
  return h('div', { class: 'empty' }, h('div', { class: 'glyph' }, icon(ic, 22)), h('strong', {}, title), text ? h('span', {}, text) : null, action || null);
}

function errorPanel(e, retry) {
  return h('div', { class: 'card' }, emptyState('pulse', e.status === 503 ? 'The server is not reachable' : 'Something went wrong', null,
    h('div', {}, h('div', { class: 'error-text' }, e.message),
      retry ? btn('Try again', { icon: 'refresh', small: true, onclick: retry }) : null)));
}

// ---------------------------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------------------------

function safeDecode(s) { try { return decodeURIComponent(s); } catch { return s; } }

function route() {
  const raw = location.hash.replace(/^#/, '');
  const q = raw.indexOf('?');
  const path = q < 0 ? raw : raw.slice(0, q);
  const params = new URLSearchParams(q < 0 ? '' : raw.slice(q + 1));
  const parts = path.split('/').filter(Boolean).map(safeDecode);
  if (parts[0] === 'c' && parts[1]) {
    const tab = TABS.some(([t]) => t === parts[2]) ? parts[2] : 'documents';
    return { page: 'collection', name: parts[1], tab, doc: params.get('doc') };
  }
  return { page: 'overview' };
}

function collectionHash(name, tab, doc) {
  const t = tab && tab !== 'documents' ? `/${tab}` : '';
  return `#/c/${enc(name)}${t}${doc ? `?doc=${enc(doc)}` : ''}`;
}

function go(hash) {
  if (location.hash === hash) render();
  else location.hash = hash;
}

// Records the open document in the address without re-rendering the page.
function setDocInUrl(doc) {
  const r = route();
  if (r.page !== 'collection') return;
  history.replaceState(null, '', collectionHash(r.name, r.tab, doc));
}

// ---------------------------------------------------------------------------------------------
// Shell
// ---------------------------------------------------------------------------------------------

function applyTheme() {
  if (prefs.theme === 'light' || prefs.theme === 'dark') document.documentElement.setAttribute('data-theme', prefs.theme);
  else document.documentElement.removeAttribute('data-theme');
  const next = { system: 'light', light: 'dark', dark: 'system' }[prefs.theme] || 'light';
  const b = $('theme');
  b.replaceChildren(icon(prefs.theme === 'dark' ? 'moon' : prefs.theme === 'light' ? 'sun' : 'monitor', 18));
  b.title = `Theme: ${prefs.theme} (switch to ${next})`;
  b.setAttribute('aria-label', b.title);
}

function cycleTheme() {
  prefs.theme = { system: 'light', light: 'dark', dark: 'system' }[prefs.theme] || 'light';
  savePrefs();
  applyTheme();
}

function applyDensity() { document.body.classList.toggle('compact', prefs.density === 'compact'); }

function toggleDensity() {
  prefs.density = prefs.density === 'compact' ? 'comfortable' : 'compact';
  savePrefs();
  applyDensity();
}

const narrow = () => matchMedia('(max-width: 860px)').matches;

function setNav(open) {
  const layout = $('layout');
  if (narrow()) {
    layout.classList.toggle('nav-open', open);
    $('scrim').hidden = !open;
  } else {
    prefs.navHidden = !open;
    savePrefs();
    layout.classList.toggle('nav-hidden', !open);
  }
  $('menu').setAttribute('aria-expanded', String(open));
}

function navShown() {
  return narrow() ? $('layout').classList.contains('nav-open') : !prefs.navHidden;
}

function showLocked() {
  closeDrawer();
  for (const id of ['sidebar', 'conn', 'disconnect', 'menu', 'palette-open']) $(id).hidden = true;
  $('main').replaceChildren(h('div', { class: 'connect-wrap page' }, h('div', { class: 'card connect' },
    h('div', { class: 'logo' }, h('img', { src: '/icon.svg', alt: '', width: 26, height: 26 })),
    h('h1', {}, 'Open the console link'),
    h('p', { class: 'lead' }, 'This page needs the key the console printed when it started. Open the link from its output, which looks like:'),
    h('pre', { class: 'json' }, 'http://127.0.0.1:7880/#key=…'),
    h('p', { class: 'help' }, 'A new key is made each time storine-console starts, so links from earlier runs stop working.'))));
}

function updateShell() {
  const s = app.get();
  const connected = !!(s.session && s.session.connected);
  for (const id of ['sidebar', 'conn', 'disconnect', 'menu', 'palette-open']) $(id).hidden = !connected;
  if (!connected) return;
  $('conn-url').textContent = s.session.url;
  const c = s.overview && s.overview.cluster;
  const dot = $('conn-dot');
  const role = $('conn-role');
  if (s.offline) {
    dot.className = 'dot bad';
    role.textContent = 'unreachable';
    role.className = 'badge danger';
  } else if (c) {
    const standalone = c.members.length === 0 && c.node_id === 0;
    dot.className = `dot ${c.is_leader || c.leader_id || standalone ? 'ok' : 'warn'}`;
    role.textContent = standalone ? 'standalone' : c.is_leader ? `leader · node ${c.node_id}` : `follower · node ${c.node_id}`;
    role.className = `badge ${c.is_leader ? 'accent' : ''}`;
  } else {
    dot.className = 'dot';
    role.textContent = '';
  }
  $('conn-latency').textContent = s.latency !== null && !s.offline ? `${Math.round(s.latency)} ms` : '';
  renderNav();
}

let navSignature = '';
function renderNav(force) {
  const s = app.get();
  const r = route();
  const sc = scope();
  const q = $('nav-filter').value.trim().toLowerCase();
  const sig = JSON.stringify([s.collections.map((c) => [c.name, c.count]), sc.pinned, sc.recent.slice(0, 6), r.page, r.name, q]);
  if (sig === navSignature && !force) return;
  navSignature = sig;

  const names = new Set(s.collections.map((c) => c.name));
  const pinned = sc.pinned.filter((n) => names.has(n));
  const item = (c, pin) => {
    const on = sc.pinned.includes(c.name);
    return h('div', { class: 'nav-row' },
      h('a', { class: `nav-item${r.page === 'collection' && r.name === c.name ? ' active' : ''}`, href: collectionHash(c.name), title: c.name },
        icon(pin ? 'pin' : 'db', 15), h('span', { class: 'name' }, c.name), h('span', { class: 'count' }, c.count === null ? '' : num(c.count))),
      h('button', { class: `nav-pin${on ? ' on' : ''}`, type: 'button', title: on ? 'Unpin' : 'Pin to the top', 'aria-label': on ? `Unpin ${c.name}` : `Pin ${c.name}`,
        onclick: () => togglePin(c.name) }, icon('pin', 13)));
  };
  fill($('nav-top'),
    h('a', { class: `nav-item${r.page === 'overview' ? ' active' : ''}`, href: '#/' }, icon('grid', 15), h('span', { class: 'name' }, 'Overview')),
    pinned.length ? h('div', { class: 'nav-section' }, h('span', {}, 'Pinned')) : null,
    pinned.map((n) => item(s.collections.find((c) => c.name === n), true)));

  const list = $('nav-list');
  const items = s.collections.filter((c) => !q || c.name.toLowerCase().includes(q));
  if (s.collections.length === 0) list.replaceChildren(h('div', { class: 'nav-empty' }, 'No collections yet.'));
  else if (items.length === 0) list.replaceChildren(h('div', { class: 'nav-empty' }, 'No match.'));
  else list.replaceChildren(...items.map((c) => item(c, false)));

  const recent = sc.recent.filter((x) => names.has(x.c)).slice(0, 6);
  $('nav-recent').replaceChildren(...(recent.length ? [
    h('div', { class: 'nav-section' }, h('span', {}, 'Recent documents'),
      h('button', { class: 'icon-btn small', type: 'button', title: 'Clear recent documents', 'aria-label': 'Clear recent documents',
        onclick: () => { scope().recent = []; savePrefs(); renderNav(true); } }, icon('close', 13))),
    ...recent.map((x) => h('a', { class: 'nav-item', href: collectionHash(x.c, 'documents', x.key), title: `${x.key} in ${x.c}` },
      icon('doc', 15), h('span', { class: 'name mono' }, x.key), h('span', { class: 'sub' }, x.c))),
  ] : []));
}

function togglePin(name) {
  const sc = scope();
  const i = sc.pinned.indexOf(name);
  if (i >= 0) sc.pinned.splice(i, 1);
  else sc.pinned.push(name);
  savePrefs();
  renderNav(true);
  const pinBtn = document.querySelector('[data-role="pin-collection"]');
  if (pinBtn) {
    pinBtn.classList.toggle('on', i < 0);
    pinBtn.title = i < 0 ? 'Unpin' : 'Pin to the sidebar';
  }
}

function rememberRecent(name, key) {
  const sc = scope();
  sc.recent = [{ c: name, key, at: Date.now() }, ...sc.recent.filter((x) => !(x.c === name && x.key === key))].slice(0, MAX_RECENT);
  savePrefs();
  renderNav();
}

function render() {
  for (const f of page.cleanup.splice(0)) f();
  page.docs = null;
  closePopover();
  const s = app.get();
  if (!s.session) return;
  if (!s.session.connected) {
    closeDrawer();
    updateShell();
    renderConnect();
    return;
  }
  const r = route();
  renderNav();
  const main = $('main');
  const view = h('div', { class: 'page' });
  main.replaceChildren(view);
  if (r.page === 'collection') renderCollection(view, r);
  else renderOverview(view);
}

// ---------------------------------------------------------------------------------------------
// Connect
// ---------------------------------------------------------------------------------------------

function renderConnect() {
  const url = h('input', { type: 'text', value: prefs.servers[0] || 'storine://127.0.0.1:7878', autocomplete: 'off', spellcheck: 'false', 'aria-label': 'Server' });
  const token = h('input', { type: 'password', autocomplete: 'off', placeholder: 'Only if the server requires one', 'aria-label': 'Token' });
  const ca = h('input', { type: 'text', autocomplete: 'off', spellcheck: 'false', placeholder: '/etc/storine/ca.crt' });
  const sni = h('input', { type: 'text', autocomplete: 'off', spellcheck: 'false', placeholder: 'Defaults to the host connected to' });
  const err = h('div', { class: 'error-text', role: 'alert' });
  const submit = btn('Connect', { kind: 'primary' });
  const connect = async () => {
    err.textContent = '';
    submit.disabled = true;
    submit.lastChild.textContent = 'Connecting…';
    try {
      const sess = await api('POST', 'connect', { url: url.value, token: token.value, tls_ca: ca.value, tls_server_name: sni.value });
      const shown = stripCredentials(url.value.trim());
      prefs.servers = [shown, ...prefs.servers.filter((u) => u !== shown)].slice(0, 6);
      savePrefs();
      runtimes.clear();
      app.set({ session: sess, overview: null, collections: [], samples: [], offline: false });
      await poll().catch(() => {});
      go('#/');
    } catch (ex) {
      err.textContent = ex.message;
    } finally {
      submit.disabled = false;
      submit.lastChild.textContent = 'Connect';
    }
  };
  submit.addEventListener('click', connect);
  for (const i of [url, token, ca, sni]) i.addEventListener('keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); connect(); } });
  const recent = prefs.servers.length > 1 || (prefs.servers.length === 1 && prefs.servers[0] !== url.value)
    ? h('div', { class: 'recent-servers' }, prefs.servers.map((u) => h('button', { type: 'button', title: 'Use this server', onclick: () => { url.value = u; token.focus(); } }, u)))
    : null;
  $('main').replaceChildren(h('div', { class: 'connect-wrap page' }, h('div', { class: 'card connect' },
    h('div', { class: 'logo' }, h('img', { src: '/icon.svg', alt: '', width: 26, height: 26 })),
    h('h1', {}, 'Connect to Storine'),
    h('p', { class: 'lead' }, 'Browse documents, run queries and aggregates, manage indexes and watch the cluster.'),
    h('div', { class: 'form' },
      h('label', { class: 'field' }, 'Server', url, h('span', { class: 'hint' }, 'storine://host:7878, several hosts for a cluster, storines:// for TLS')),
      recent,
      h('label', { class: 'field' }, 'Token', token),
      h('details', { class: 'more' }, h('summary', {}, 'TLS'),
        h('div', {},
          h('label', { class: 'field' }, 'CA certificate file', ca, h('span', { class: 'hint' }, 'A path on the machine running the console. Giving one turns TLS on.')),
          h('label', { class: 'field' }, 'Server name', sni))),
      err,
      submit))));
  url.focus();
}

// ---------------------------------------------------------------------------------------------
// Overview
// ---------------------------------------------------------------------------------------------

function renderOverview(view) {
  const stats = h('div', { class: 'stats' }, Array.from({ length: 5 }, () => h('div', { class: 'card stat' }, h('span', { class: 'skel', style: 'width:50%' }), h('span', { class: 'skel big' }))));
  const collections = h('div', {}, skeletonTable(3, 3));
  const cluster = h('div', {});
  const clusterNote = h('span', { class: 'muted mono' });
  const health = h('dl', { class: 'kv' });
  const raw = h('div', {});
  const liveHost = h('div', { class: 'live' });
  const sub = h('div', { class: 'sub mono' });

  const drawLive = () => {
    liveHost.replaceChildren(
      agoEl(app.get().updatedAt, 'Updated '),
      seg([['on', 'Live', 'pulse'], ['off', 'Paused', 'pause']], prefs.live ? 'on' : 'off', (v) => {
        prefs.live = v === 'on';
        savePrefs();
        drawLive();
        if (prefs.live) poll().catch(() => {});
      }),
      iconBtn('refresh', 'Refresh now (r)', () => poll().catch((e) => toast(e.message, 'error'))));
  };
  drawLive();

  view.append(
    h('div', { class: 'page-head' }, h('div', {}, h('h1', {}, 'Overview'), sub), liveHost),
    stats,
    h('div', { class: 'grid-2' },
      h('div', { class: 'stack' },
        h('div', { class: 'card' }, h('div', { class: 'card-head' }, h('h2', {}, 'Collections'),
          btn('New collection', { icon: 'plus', small: true, onclick: newCollection })), collections),
        h('div', { class: 'card' }, h('div', { class: 'card-head' }, h('h2', {}, 'Cluster'), clusterNote), cluster)),
      h('div', { class: 'card' }, h('div', { class: 'card-head' }, h('h2', {}, 'Health of the connected node')),
        h('div', { class: 'card-body stack' }, health,
          h('details', { class: 'more' }, h('summary', {}, 'Full health report'), raw)))));

  let collSig = '';
  let clusterSig = '';
  const paint = () => {
    const s = app.get();
    const o = s.overview;
    if (!o) return;
    sub.textContent = o.url;
    const t = view.querySelector('[data-ago]');
    if (t) { t.setAttribute('data-ago', String(s.updatedAt)); tickClocks(); }
    const c = o.cluster;
    const standalone = c.members.length === 0 && c.node_id === 0;
    const docs = o.collections.reduce((a, x) => a + (typeof x.count === 'number' ? x.count : 0), 0);
    const hl = o.health || {};
    const arena = hl.arena || {};
    const reads = rates(s.samples, 'reads');
    const writes = rates(s.samples, 'writes');
    const last = (xs) => (xs.length ? xs[xs.length - 1] : null);
    const stat = (label, ic, value, unit, note, spark) => h('div', { class: 'card stat' },
      h('div', { class: 'label' }, icon(ic, 14), label),
      h('div', { class: 'value' }, value, unit ? h('small', {}, unit) : null),
      h('div', { class: 'foot' }, note || ''),
      spark || null);
    stats.replaceChildren(
      stat('Documents', 'doc', num(docs), null, plural(o.collections.length, 'collection'), sparkline(s.samples.map((x) => x.docs || 0), true)),
      stat('Reads', 'search', rate(last(reads)), '/s', `${num(hl.total_reads)} in total`, sparkline(reads)),
      stat('Writes', 'bolt', rate(last(writes)), '/s', `${num(hl.total_writes)} in total`, sparkline(writes)),
      stat('Unflushed', 'history', bytes(hl.hot_dirty_bytes), null, 'waiting for the flusher', sparkline(s.samples.map((x) => x.dirty || 0), true)),
      stat('Cluster', 'node', standalone ? 'Standalone' : c.is_leader ? 'Leader' : 'Follower', null,
        standalone ? `applied ${num(c.last_applied)}` : `node ${c.node_id} · term ${num(c.term)} · applied ${num(c.last_applied)}`));

    const cs = JSON.stringify([o.collections, scope().pinned]);
    if (cs !== collSig) {
      collSig = cs;
      const max = Math.max(1, ...o.collections.map((x) => (typeof x.count === 'number' ? x.count : 0)));
      collections.replaceChildren(o.collections.length === 0
        ? emptyState('db', 'No collections yet', 'A collection is created by its first document.', btn('New collection', { kind: 'primary', small: true, icon: 'plus', onclick: newCollection }))
        : h('div', { class: 'table-wrap' }, h('table', {},
          h('thead', {}, h('tr', {}, h('th', {}, 'Collection'), h('th', {}, 'Indexes'), h('th', { class: 'num' }, 'Documents'))),
          h('tbody', {}, o.collections.map((x) => h('tr', { class: 'clickable', tabindex: '0', onclick: () => go(collectionHash(x.name)),
            onkeydown: (e) => { if (e.key === 'Enter') go(collectionHash(x.name)); } },
          h('td', {}, h('span', { class: 'cell key' }, scope().pinned.includes(x.name) ? icon('pin', 13) : icon('db', 13), h('span', {}, x.name))),
          h('td', {}, x.indexes.length ? h('div', { class: 'chips' }, x.indexes.map((i) => h('span', { class: 'badge' }, i))) : h('span', { class: 'muted' }, '—')),
          h('td', { class: 'num' }, num(x.count),
            typeof x.count === 'number' ? h('div', { class: 'meter' }, h('span', { style: `width:${Math.max(2, (x.count / max) * 100).toFixed(1)}%` })) : null)))))));
    }

    const ks = JSON.stringify(c);
    if (ks !== clusterSig) {
      clusterSig = ks;
      clusterNote.textContent = c.leader_addr && !c.is_leader ? `leader ${c.leader_addr}` : '';
      cluster.replaceChildren(standalone
        ? emptyState('node', 'A standalone server', 'Start servers with --peers to form a replicated cluster.')
        : h('div', { class: 'nodes' }, c.members.map((m) => h('div', { class: `node${m.leader ? ' leader' : ''}` },
          h('div', { class: 'top' }, h('span', {}, `Node ${m.id}`), h('span', { class: `badge ${m.leader ? 'accent' : ''}` }, m.leader ? 'leader' : 'follower')),
          h('div', { class: 'addr' }, m.addr),
          m.id === c.node_id ? h('div', { class: 'you' }, 'connected to this one') : null))));
    }

    const rows = [
      ['Status', hl.status ? h('span', { class: `badge ${hl.status === 'ok' ? 'ok' : 'warn'}` }, hl.status) : '—'],
      ['Live records', num(hl.live_records)],
      ['Reads', num(hl.total_reads)],
      ['Writes', num(hl.total_writes)],
      ['Unflushed writes', bytes(hl.hot_dirty_bytes)],
      ['Arena segments', num(arena.segments)],
      ['Arena live / dead', arena.live !== undefined ? `${bytes(arena.live)} / ${bytes(arena.dead)}` : '—'],
      ['Compactions', num(arena.compactions)],
      ['Round trip', s.latency !== null ? `${Math.round(s.latency)} ms` : '—'],
    ];
    if (arena.last_error) rows.push(['Last arena error', h('span', { class: 'error-text' }, arena.last_error)]);
    health.replaceChildren(...rows.flatMap(([k, v]) => [h('dt', {}, k), h('dd', {}, v)]));
    raw.replaceChildren(highlighted(JSON.stringify(o.health, null, 2)));
  };
  page.cleanup.push(app.subscribe(paint));
  paint();
  poll().catch((e) => { if (!app.get().overview) view.replaceChildren(errorPanel(e, render)); else toast(e.message, 'error'); });
}

async function newCollection() {
  const name = await promptDialog('New collection', 'Name', { placeholder: 'e.g. orders', confirm: 'Create', text: 'A collection is created with its first document.' });
  if (!name) return;
  const s = app.get();
  if (!s.collections.some((c) => c.name === name)) app.set({ collections: [...s.collections, { name, count: 0, indexes: [] }] });
  location.hash = collectionHash(name);
  setTimeout(() => openNewDocument(name), 60);
}

// ---------------------------------------------------------------------------------------------
// Collection
// ---------------------------------------------------------------------------------------------

function defaultView() {
  return {
    conds: [], keyPrefix: '', sort: '', desc: false, limit: 50, columns: null,
    agg: { conds: [], groupBy: '', aggs: [{ fn: 'count', field: '', as: '' }], order: '', show: 'table' },
  };
}

// The saved query settings of a collection, repaired once per session if stored by an older page.
const checkedViews = new WeakSet();
function viewOf(name) {
  const sc = scope();
  let v = sc.views[name];
  if (!v || !checkedViews.has(v)) {
    const d = defaultView();
    v = { ...d, ...(v || {}) };
    v.agg = { ...d.agg, ...(v.agg || {}) };
    if (!Array.isArray(v.conds)) v.conds = [];
    if (!Array.isArray(v.agg.conds)) v.agg.conds = [];
    if (!Array.isArray(v.agg.aggs) || v.agg.aggs.length === 0) v.agg.aggs = d.agg.aggs;
    if (!PAGE_SIZES.includes(v.limit)) v.limit = 50;
    if (v.columns !== null && !Array.isArray(v.columns)) v.columns = null;
    sc.views[name] = v;
    checkedViews.add(v);
  }
  return v;
}

function renderCollection(view, r) {
  const { name, tab } = r;
  const known = app.get().collections.find((c) => c.name === name);
  const countEl = h('span', {}, known && typeof known.count === 'number' ? plural(known.count, 'document') : '');
  const chips = h('div', { class: 'chips' });
  const body = h('div', {});
  const pinned = scope().pinned.includes(name);
  view.append(
    h('div', { class: 'page-head' },
      h('div', {},
        h('div', { class: 'crumbs' }, h('a', { href: '#/' }, 'Overview'), icon('right', 14), h('span', {}, 'Collection')),
        h('div', { class: 'title-row' }, h('h1', { class: 'title-mono' }, name),
          h('button', { class: `icon-btn small nav-pin-inline${pinned ? ' on' : ''}`, type: 'button', 'data-role': 'pin-collection', title: pinned ? 'Unpin' : 'Pin to the sidebar',
            'aria-label': 'Pin or unpin this collection', onclick: () => togglePin(name) }, icon('pin', 14))),
        h('div', { class: 'sub' }, countEl, chips)),
      h('div', { class: 'actions' }, btn('New document', { kind: 'primary', icon: 'plus', kbd: 'N', onclick: () => openNewDocument(name) }))),
    h('div', { class: 'tabs', role: 'tablist' }, TABS.map(([id, label, ic]) => h('button', {
      class: `tab${tab === id ? ' active' : ''}`, type: 'button', role: 'tab', 'aria-selected': tab === id ? 'true' : 'false',
      onclick: () => go(collectionHash(name, id)),
    }, icon(ic, 15), label))),
    body);

  const info = api('GET', `collections/${enc(name)}`).then((i) => {
    countEl.textContent = plural(i.count, 'document');
    chips.replaceChildren(...i.indexes.map((x) => h('span', { class: 'badge', title: 'secondary index' }, icon('bolt', 11), x)),
      ...i.vector_indexes.map((v) => h('span', { class: 'badge accent', title: 'vector index' }, `${v.field} · ${v.dims}d`)));
    const s = app.get();
    if (s.collections.some((c) => c.name === name && c.count !== i.count)) app.set({ collections: s.collections.map((c) => (c.name === name ? { ...c, count: i.count } : c)) });
    return i;
  }, (e) => {
    if (e.status !== 404) toast(e.message, 'error');
    return null;
  });

  if (tab === 'analytics') renderAnalytics(name, body);
  else if (tab === 'indexes') {
    body.append(h('div', { class: 'card' }, skeletonTable(2, 4)));
    info.then((i) => { if (body.isConnected) renderIndexes(name, body, i); });
  } else if (tab === 'settings') info.then((i) => { if (body.isConnected) renderSettings(name, body, i); });
  else renderDocuments(name, body);
  if (r.doc) openDocument(name, r.doc);
}

// A list of conditions (all must hold) edited in place.
function conditionEditor(conds, onRun) {
  const wrap = h('div', { class: 'stack', style: 'gap:8px' });
  const draw = (focusLast) => {
    wrap.hidden = conds.length === 0;
    wrap.replaceChildren(...conds.map((c, i) => {
      const path = h('input', { type: 'text', value: c.path, placeholder: 'field, e.g. status or address.city', spellcheck: 'false', 'aria-label': 'Field', 'data-role': 'path',
        oninput: (e) => { c.path = e.target.value; savePrefs(); }, onkeydown: enter(onRun) });
      const op = h('select', { 'aria-label': 'Comparison', onchange: (e) => { c.op = e.target.value; savePrefs(); draw(); } },
        OPS.map(([v, l]) => h('option', { value: v, selected: c.op === v }, l)));
      const noValue = c.op === 'exists' || c.op === 'missing';
      const placeholder = c.op === 'between' ? 'low .. high' : c.op === 'in' ? 'a, b, c' : c.op === 'prefix' ? 'text' : 'value: 42, true, "text", {"$decimal":"9.99"}';
      const value = h('input', { type: 'text', value: c.value, placeholder, spellcheck: 'false', disabled: noValue, 'data-role': 'value', 'aria-label': 'Value',
        oninput: (e) => { c.value = e.target.value; savePrefs(); }, onkeydown: enter(onRun) });
      const del = iconBtn('close', 'Remove condition', () => { conds.splice(i, 1); savePrefs(); draw(); });
      return h('div', { class: 'cond' }, h('div', { class: 'joiner' }, i === 0 ? 'where' : 'and'), path, op, value, del);
    }));
    if (focusLast) {
      const inputs = wrap.querySelectorAll('[data-role="path"]');
      if (inputs.length) inputs[inputs.length - 1].focus();
    }
  };
  draw();
  return { el: wrap, redraw: draw, add: () => { conds.push({ path: '', op: 'eq', value: '' }); savePrefs(); draw(true); } };
}

function enter(fn) {
  return (e) => { if (e.key === 'Enter') { e.preventDefault(); fn(); } };
}

// The filter the conditions describe, or null.
function buildFilter(conds, keyPrefix) {
  const fs = [];
  for (const c of conds) {
    const path = c.path.trim();
    if (!path) continue;
    const f = { op: c.op, path };
    if (c.op === 'between') {
      const [lo, hi] = c.value.split('..');
      if (hi === undefined) throw new Error(`"between" on ${path} needs "low .. high"`);
      f.low = literal(lo);
      f.high = literal(hi);
    } else if (c.op === 'in') {
      f.values = c.value.split(',').map(literal).filter((v) => v !== '');
    } else if (c.op === 'prefix') {
      f.text = c.value;
    } else if (c.op !== 'exists' && c.op !== 'missing') {
      f.value = literal(c.value);
    }
    fs.push(f);
  }
  if (keyPrefix && keyPrefix.trim()) fs.push({ op: 'key_prefix', text: keyPrefix.trim() });
  if (fs.length === 0) return null;
  return fs.length === 1 ? fs[0] : { op: 'and', filters: fs };
}

// History and saved queries keep these fields of a view.
function snapshot(v) {
  return { conds: v.conds.filter((c) => c.path.trim()).map((c) => ({ path: c.path, op: c.op, value: c.value })), keyPrefix: v.keyPrefix, sort: v.sort, desc: v.desc };
}

function describe(q) {
  const label = Object.fromEntries(OPS);
  const parts = q.conds.map((c) => (c.op === 'exists' || c.op === 'missing' ? `${c.path} ${label[c.op]}` : `${c.path} ${label[c.op]} ${c.value}`));
  if (q.keyPrefix && q.keyPrefix.trim()) parts.push(`key starts with ${q.keyPrefix.trim()}`);
  const where = parts.length ? parts.join(' and ') : 'All documents';
  return q.sort && q.sort.trim() ? `${where} · by ${q.sort.trim()}${q.desc ? ' ↓' : ' ↑'}` : where;
}

function recordHistory(name, v) {
  const snap = snapshot(v);
  if (snap.conds.length === 0 && !snap.keyPrefix.trim() && !snap.sort.trim()) return;
  const sc = scope();
  const id = JSON.stringify(snap);
  const list = (sc.history[name] || []).filter((x) => JSON.stringify({ conds: x.conds, keyPrefix: x.keyPrefix, sort: x.sort, desc: x.desc }) !== id);
  sc.history[name] = [{ ...snap, at: Date.now() }, ...list].slice(0, MAX_HISTORY);
  savePrefs();
}

function applySnapshot(name, snap) {
  const v = viewOf(name);
  v.conds = snap.conds.map((c) => ({ ...c }));
  v.keyPrefix = snap.keyPrefix || '';
  v.sort = snap.sort || '';
  v.desc = !!snap.desc;
  savePrefs();
  const rt = runtimeOf(name);
  rt.cursors = [null];
  rt.page = 0;
  if (route().page === 'collection' && route().name === name && route().tab === 'documents') render();
  else go(collectionHash(name));
}

function renderDocuments(name, body) {
  const v = viewOf(name);
  const rt = runtimeOf(name);
  const results = h('div', { class: 'results' });
  const d = { name, v, rt, host: results };
  page.docs = d;
  const run = (record) => {
    rt.cursors = [null];
    rt.page = 0;
    rt.cursor = -1;
    runQuery(d, { record: record !== false });
  };
  d.run = run;
  const cond = conditionEditor(v.conds, run);
  d.addCondition = cond.add;
  d.focusFilter = () => {
    const first = cond.el.querySelector('[data-role="path"]');
    if (first) first.focus();
    else cond.add();
  };
  const keyPrefix = h('input', { type: 'text', value: v.keyPrefix, placeholder: 'Key starts with…', spellcheck: 'false', 'aria-label': 'Key prefix',
    oninput: (e) => { v.keyPrefix = e.target.value; savePrefs(); }, onkeydown: enter(run) });
  const sort = h('input', { type: 'text', value: v.sort, placeholder: 'Sort by field', spellcheck: 'false', 'aria-label': 'Sort by',
    oninput: (e) => { v.sort = e.target.value; savePrefs(); }, onkeydown: enter(run) });
  const dir = btn(v.desc ? 'Desc' : 'Asc', { small: true, kind: 'subtle', icon: v.desc ? 'down' : 'up', title: 'Sort direction' });
  dir.addEventListener('click', () => {
    v.desc = !v.desc;
    savePrefs();
    dir.replaceChildren(icon(v.desc ? 'down' : 'up', 14), h('span', {}, v.desc ? 'Desc' : 'Asc'));
    if (v.sort.trim()) run();
  });
  const sc = scope();
  const historyBtn = btn('', { small: true, kind: 'subtle', icon: 'history', title: 'Recent queries', menu: true });
  historyBtn.addEventListener('click', () => popover(historyBtn, (close) => {
    const list = sc.history[name] || [];
    return [h('div', { class: 'menu-label' }, 'Recent queries'),
      list.length === 0 ? h('div', { class: 'menu-empty' }, 'Queries you run appear here.') : list.map((q) => menuItem('history', describe(q), {
        trail: agoEl(q.at), onclick: () => { close(); applySnapshot(name, q); },
      })),
      list.length ? [h('div', { class: 'menu-sep' }), menuItem('close', 'Clear history', { onclick: () => { delete sc.history[name]; savePrefs(); close(); } })] : null];
  }, 'end'));
  const savedBtn = btn('', { small: true, kind: 'subtle', icon: 'star', title: 'Saved queries', menu: true });
  savedBtn.addEventListener('click', () => popover(savedBtn, (close) => {
    const list = sc.saved[name] || [];
    return [
      menuItem('plus', 'Save the current query…', { sub: describe(snapshot(v)), onclick: async () => {
        close();
        const label = await promptDialog('Save query', 'Name', { placeholder: 'e.g. Active in London', confirm: 'Save' });
        if (!label) return;
        sc.saved[name] = [{ name: label, ...snapshot(v), at: Date.now() }, ...(sc.saved[name] || []).filter((x) => x.name !== label)];
        savePrefs();
        toast(`Saved “${label}”`, { kind: 'ok' });
      } }),
      list.length ? h('div', { class: 'menu-sep' }) : null,
      list.length ? h('div', { class: 'menu-label' }, 'Saved') : null,
      list.map((q) => menuItem('star', q.name, {
        sub: describe(q),
        onclick: () => { close(); applySnapshot(name, q); },
        trail: h('span', { class: 'icon-btn small x', role: 'button', title: 'Delete', 'aria-label': `Delete ${q.name}`, onclick: (e) => {
          e.stopPropagation();
          sc.saved[name] = (sc.saved[name] || []).filter((x) => x !== q);
          savePrefs();
          close();
          toast(`Deleted “${q.name}”`);
        } }, icon('close', 12)),
      })),
    ];
  }, 'end'));
  const columnsBtn = btn('', { small: true, kind: 'subtle', icon: 'columns', title: 'Columns', menu: true });
  columnsBtn.addEventListener('click', () => popover(columnsBtn, () => {
    const docs = rt.result ? rt.result.docs : [];
    const auto = autoColumns(docs);
    const all = [...new Set([...(v.columns || []), ...auto, ...docs.flatMap((x) => Object.keys(x.doc))])];
    const drawList = () => [
      h('div', { class: 'menu-label' }, 'Columns'),
      h('label', { class: 'menu-item' }, h('input', { type: 'checkbox', checked: v.columns === null, onchange: () => { v.columns = v.columns === null ? auto.slice() : null; savePrefs(); drawResults(d); fill(pop, drawList()); } }),
        h('span', { class: 'text' }, h('span', { class: 'main-line' }, 'Automatic'), h('span', { class: 'sub-line' }, 'the most common fields on this page'))),
      h('div', { class: 'menu-sep' }),
      all.length === 0 ? h('div', { class: 'menu-empty' }, 'Run a query to see its fields.') : all.map((f) => h('label', { class: 'menu-item' },
        h('input', { type: 'checkbox', checked: (v.columns || auto).includes(f), onchange: (e) => {
          const cur = (v.columns || auto).slice();
          v.columns = e.target.checked ? all.filter((x) => cur.includes(x) || x === f) : cur.filter((x) => x !== f);
          savePrefs();
          drawResults(d);
          fill(pop, drawList());
        } }), h('span', { class: 'text mono' }, f))),
    ];
    const pop = h('div', {}, drawList());
    return pop;
  }, 'end'));
  const densityBtn = btn('', { small: true, kind: 'subtle', icon: 'density', title: 'Compact rows', on: prefs.density === 'compact' });
  densityBtn.addEventListener('click', () => { toggleDensity(); densityBtn.classList.toggle('on', prefs.density === 'compact'); });
  const exportBtn = btn('', { small: true, kind: 'subtle', icon: 'download', title: 'Download this page as JSON', onclick: () => {
    if (!rt.result) return;
    download(`${name}-page${rt.page + 1}.json`, JSON.stringify(rt.result.docs.map((x) => ({ key: x.key, version: x.version, doc: x.doc })), null, 2));
  } });

  body.append(h('div', { class: 'card' },
    h('div', { class: 'toolbar' },
      cond.el,
      h('div', { class: 'toolbar-row' },
        btn('Filter', { small: true, icon: 'filter', title: 'Add a condition (/)', onclick: () => cond.add() }),
        h('div', { class: 'with-icon' }, icon('key', 14), keyPrefix),
        h('div', { class: 'with-icon' }, icon('sort', 14), sort), dir,
        h('span', { class: 'spacer' }),
        historyBtn, savedBtn, columnsBtn, densityBtn, exportBtn,
        h('span', { class: 'divider' }),
        btn('Run', { small: true, kind: 'primary', icon: 'play', title: 'Run (Enter in any box)', onclick: () => run() }))),
    results));
  if (rt.result) drawResults(d);
  runQuery(d, { record: false });
}

async function runQuery(d, opts) {
  const { name, v, rt, host } = d;
  let filter;
  try {
    filter = buildFilter(v.conds, v.keyPrefix);
  } catch (e) {
    toast(e.message, 'error');
    return;
  }
  const sorted = v.sort.trim() !== '';
  const req = { filter, limit: v.limit };
  if (sorted) {
    req.sort = [`${v.desc ? '-' : ''}${v.sort.trim()}`];
    req.offset = rt.page * v.limit;
  } else {
    req.after = rt.cursors[rt.page];
  }
  if (!rt.result) host.replaceChildren(skeletonTable(8, 5));
  host.classList.add('busy');
  const seq = ++rt.seq;
  const t0 = performance.now();
  try {
    const r = await api('POST', `collections/${enc(name)}/query`, req);
    if (seq !== rt.seq) return;
    rt.result = r;
    rt.sorted = sorted;
    rt.took = performance.now() - t0;
    if (opts && opts.record) recordHistory(name, v);
    if (host.isConnected) drawResults(d);
  } catch (e) {
    if (seq !== rt.seq) return;
    host.replaceChildren(h('div', { class: 'error-box' }, e.message));
  } finally {
    if (seq === rt.seq) host.classList.remove('busy');
  }
}

function autoColumns(docs) {
  const seen = new Map();
  for (const d of docs) for (const k of Object.keys(d.doc)) seen.set(k, (seen.get(k) || 0) + 1);
  return [...seen.entries()].sort((a, b) => b[1] - a[1]).slice(0, 8).map(([k]) => k);
}

function draftKey(name, key) { return `${(app.get().session || {}).url}\u0000${name}\u0000${key}`; }

function drawResults(d) {
  const { name, v, rt, host } = d;
  const r = rt.result;
  if (!r) return;
  if (r.docs.length === 0) {
    const fresh = r.total === 0 && v.conds.every((c) => !c.path.trim()) && !v.keyPrefix.trim();
    host.replaceChildren(fresh
      ? emptyState('doc', 'This collection is empty', 'Add the first document to create it.', btn('New document', { kind: 'primary', small: true, icon: 'plus', onclick: () => openNewDocument(name) }))
      : emptyState('filter', 'No documents match', 'Change the conditions and run again.'));
    return;
  }
  const cols = v.columns || autoColumns(r.docs);
  const first = rt.page * v.limit + 1;
  const last = rt.page * v.limit + r.docs.length;
  const hasNext = rt.sorted ? last < r.total : !!r.next;
  const sortBy = (field) => {
    if (field === null) v.sort = '';
    else if (v.sort.trim() === field) v.desc = !v.desc;
    else { v.sort = field; v.desc = false; }
    rt.cursors = [null];
    rt.page = 0;
    rt.cursor = -1;
    savePrefs();
    recordHistory(name, v);
    render();
  };
  const arrow = (field) => (v.sort.trim() === field ? h('span', { class: 'arrow' }, v.desc ? '↓' : '↑') : null);
  const size = h('select', { 'aria-label': 'Page size', onchange: (e) => { v.limit = Number(e.target.value); savePrefs(); d.run(false); } },
    PAGE_SIZES.map((n) => h('option', { value: n, selected: v.limit === n }, `${n} per page`)));
  host.replaceChildren(
    h('div', { class: 'table-wrap' }, h('table', {},
      h('thead', {}, h('tr', {},
        h('th', { class: `sortable${v.sort.trim() ? '' : ' sorted'}`, title: 'Key order', onclick: () => sortBy(null) }, 'Key', v.sort.trim() ? null : h('span', { class: 'arrow' }, '↑')),
        cols.map((c) => h('th', { class: `sortable${v.sort.trim() === c ? ' sorted' : ''}`, title: `Sort by ${c}`, onclick: () => sortBy(c) }, c, arrow(c))),
        h('th', { class: 'num' }, 'Version'))),
      h('tbody', {}, r.docs.map((x, i) => h('tr', {
        class: `clickable${rt.selected === x.key ? ' selected' : ''}${rt.cursor === i ? ' cursor' : ''}`, tabindex: '0', 'data-key': x.key,
        onclick: () => { rt.cursor = i; openDocument(name, x.key); },
        onkeydown: enter(() => { rt.cursor = i; openDocument(name, x.key); }),
      },
      h('td', {}, h('span', { class: 'cell key', title: x.key }, drafts[draftKey(name, x.key)] ? h('span', { class: 'draft-dot', title: 'Unsaved edit' }) : null, h('span', {}, x.key))),
      cols.map((c) => h('td', {}, h('span', { class: 'cell' }, cell(x.doc[c])))),
      h('td', { class: 'num muted' }, String(x.version))))))),
    h('div', { class: 'results-foot' },
      h('span', {}, `${num(first)}–${num(last)} of ${num(r.total)}`, h('span', { class: 'muted' }, ` · ${Math.round(rt.took)} ms`)),
      h('div', { class: 'row' }, size,
        iconBtn('left', 'Previous page ([)', () => pageBy(d, -1), { disabled: rt.page === 0 }),
        h('span', { class: 'muted' }, `Page ${rt.page + 1}`),
        iconBtn('right', 'Next page (])', () => pageBy(d, 1), { disabled: !hasNext }))));
}

function pageBy(d, delta) {
  const { rt, v } = d;
  const r = rt.result;
  if (!r) return;
  if (delta < 0) {
    if (rt.page === 0) return;
    rt.page--;
  } else {
    const last = rt.page * v.limit + r.docs.length;
    if (rt.sorted ? last >= r.total : !r.next) return;
    if (!rt.sorted) rt.cursors[rt.page + 1] = r.next;
    rt.page++;
  }
  rt.cursor = -1;
  runQuery(d, {});
}

function moveCursor(delta) {
  const d = page.docs;
  if (!d || !d.rt.result || d.rt.result.docs.length === 0) return;
  const { rt } = d;
  const n = rt.result.docs.length;
  rt.cursor = Math.max(0, Math.min(n - 1, rt.cursor < 0 ? (delta > 0 ? 0 : n - 1) : rt.cursor + delta));
  const rows = d.host.querySelectorAll('tbody tr');
  rows.forEach((tr, i) => tr.classList.toggle('cursor', i === rt.cursor));
  if (rows[rt.cursor]) rows[rt.cursor].scrollIntoView({ block: 'nearest' });
  if (!$('drawer').hidden && drawer.name === d.name) openDocument(d.name, rt.result.docs[rt.cursor].key);
}

function refreshResults(name) {
  const d = page.docs;
  if (d && d.name === name) runQuery(d, {});
}

// ---------------------------------------------------------------------------------------------
// Document drawer
// ---------------------------------------------------------------------------------------------

const drawer = { name: null, key: null, seq: 0 };

function closeDrawer(keepUrl) {
  const d = $('drawer');
  if (d.hidden) return;
  d.hidden = true;
  d.replaceChildren();
  drawer.name = null;
  drawer.key = null;
  for (const rt of runtimes.values()) rt.selected = null;
  for (const tr of document.querySelectorAll('tr.selected')) tr.classList.remove('selected');
  if (keepUrl !== true) setDocInUrl(null);
}

function markSelected(key) {
  for (const tr of document.querySelectorAll('tbody tr[data-key]')) tr.classList.toggle('selected', tr.getAttribute('data-key') === key);
}

function drawerShell(name, title, version, extra) {
  const rt = runtimeOf(name);
  const docs = rt.result ? rt.result.docs : [];
  const i = docs.findIndex((x) => x.key === title);
  const neighbor = (delta) => {
    const n = docs[i + delta];
    if (!n) return;
    rt.cursor = i + delta;
    for (const [j, tr] of [...document.querySelectorAll('tbody tr[data-key]')].entries()) tr.classList.toggle('cursor', j === rt.cursor);
    openDocument(name, n.key);
  };
  return h('div', { class: 'drawer-head' },
    h('div', { class: 'title' }, h('div', { class: 'coll' }, name),
      h('div', { class: 'mono' }, h('span', { title }, title), extra || null)),
    version !== null ? h('span', { class: 'badge' }, `v${version}`) : null,
    i >= 0 ? iconBtn('up', 'Previous document (k)', () => neighbor(-1), { disabled: i === 0 }) : null,
    i >= 0 ? iconBtn('down', 'Next document (j)', () => neighbor(1), { disabled: i === docs.length - 1 }) : null,
    iconBtn('close', 'Close (Esc)', closeDrawer));
}

async function openDocument(name, key) {
  const rt = runtimeOf(name);
  rt.selected = key;
  markSelected(key);
  const d = $('drawer');
  const seq = ++drawer.seq;
  drawer.name = name;
  drawer.key = key;
  if (d.hidden) {
    d.hidden = false;
    d.replaceChildren(drawerShell(name, key, null), h('div', { class: 'drawer-body' }, h('span', { class: 'skel', style: 'width:40%' }), h('span', { class: 'skel', style: 'width:70%' }), h('span', { class: 'skel', style: 'width:55%' })));
  }
  setDocInUrl(key);
  let doc;
  try {
    doc = await api('GET', `collections/${enc(name)}/docs/${enc(key)}`);
  } catch (e) {
    if (seq !== drawer.seq) return;
    d.replaceChildren(drawerShell(name, key, null), h('div', { class: 'drawer-body' },
      e.status === 404 ? emptyState('doc', 'No such document', `There is no “${key}” in ${name}.`) : h('div', { class: 'error-box' }, e.message)));
    return;
  }
  if (seq !== drawer.seq) return;
  rememberRecent(name, key);
  drawDocument(name, doc);
}

function editorArea(text) {
  const ta = h('textarea', { class: 'editor', spellcheck: 'false', autocomplete: 'off', 'aria-label': 'Document JSON', value: text });
  const status = h('div', { class: 'help' });
  const check = () => {
    try { JSON.parse(ta.value); ta.classList.remove('invalid'); status.textContent = ''; status.className = 'help'; return true; } catch (e) {
      ta.classList.add('invalid'); status.textContent = e.message; status.className = 'error-text'; return false;
    }
  };
  ta.addEventListener('input', check);
  ta.addEventListener('keydown', (e) => {
    if (e.key === 'Tab' && !e.shiftKey) {
      e.preventDefault();
      ta.setRangeText('  ', ta.selectionStart, ta.selectionEnd, 'end');
    }
  });
  return { ta, status, check };
}

const TYPE_HELP = 'Plain JSON, plus typed values: {"$decimal": "9.99"}, {"$time": "2026-11-13T08:00:00Z"}, {"$bytes": "<base64>"}, {"$uuid": "…"}, {"$int": "<large integer>"}. 1 is an integer, 1.0 a float.';

function drawDocument(name, doc) {
  const d = $('drawer');
  const id = draftKey(name, doc.key);
  const draft = drafts[id];
  const ed = editorArea(draft ? draft.text : doc.text);
  const err = h('div', { class: 'error-text', role: 'alert' });
  const unsaved = h('span', { class: 'unsaved' }, h('span', { class: 'draft-dot' }), 'Unsaved');
  const note = h('div', {});
  const treeHost = h('div', {});
  const save = btn('Save', { kind: 'primary', kbd: `${MOD}↵` });
  let mode = prefs.drawerMode === 'json' ? 'json' : 'tree';

  const dirty = () => ed.ta.value !== doc.text;
  const syncDirty = () => {
    unsaved.hidden = !dirty();
    if (dirty()) drafts[id] = { text: ed.ta.value, version: doc.version, at: Date.now() };
    else delete drafts[id];
    saveDrafts();
    const dot = document.querySelector(`tbody tr[data-key="${CSS.escape(doc.key)}"] .draft-dot`);
    if (!dirty() && dot) dot.remove();
  };
  const drawTree = () => {
    let value;
    try { value = JSON.parse(ed.ta.value); } catch { treeHost.replaceChildren(h('div', { class: 'error-box' }, 'Fix the JSON to see the tree.')); return; }
    treeHost.replaceChildren(tree(value));
  };
  const setMode = (m) => {
    mode = m;
    prefs.drawerMode = m;
    savePrefs();
    modeSeg.replaceWith(modeSeg = seg([['tree', 'Tree', 'tree'], ['json', 'JSON', 'code']], mode, setMode));
    ed.ta.hidden = mode !== 'json';
    ed.status.hidden = mode !== 'json';
    help.hidden = mode !== 'json';
    treeHost.hidden = mode !== 'tree';
    if (mode === 'tree') drawTree();
    else ed.ta.focus();
  };
  let modeSeg = seg([['tree', 'Tree', 'tree'], ['json', 'JSON', 'code']], mode, setMode);
  const help = h('div', { class: 'help' }, TYPE_HELP);

  if (draft) {
    note.replaceChildren(h('div', { class: 'note' }, icon('history', 15),
      h('span', {}, `Your unsaved edit from ${ago(draft.at)} is restored${draft.version !== doc.version ? `; the document changed since (v${draft.version} → v${doc.version})` : ''}.`),
      h('span', { class: 'spacer' }),
      h('a', { href: '#', onclick: (e) => { e.preventDefault(); ed.ta.value = doc.text; ed.check(); syncDirty(); note.replaceChildren(); if (mode === 'tree') drawTree(); } }, 'Discard')));
  }

  const doSave = async (how) => {
    if (!ed.check()) { setMode('json'); return; }
    err.replaceChildren();
    save.disabled = true;
    try {
      const base = draft && draft.version !== doc.version && dirty() ? draft.version : doc.version;
      const q = how === 'put' ? 'mode=put' : `mode=replace&version=${base}`;
      const saved = await api('PUT', `collections/${enc(name)}/docs/${enc(doc.key)}?${q}`, ed.ta.value, { raw: true });
      delete drafts[id];
      saveDrafts();
      toast(`Saved ${doc.key} · v${saved.version}`, { kind: 'ok' });
      drawDocument(name, saved);
      refreshResults(name);
    } catch (e) {
      if (e.status === 409) {
        err.append('This document changed since you opened it. ',
          h('a', { href: '#', onclick: (ev) => { ev.preventDefault(); openDocument(name, doc.key); } }, 'Load the new version'), ' (your edit stays as a draft) or ',
          h('a', { href: '#', onclick: (ev) => { ev.preventDefault(); doSave('put'); } }, 'overwrite it'), '.');
      } else {
        err.textContent = e.message;
      }
    } finally {
      save.disabled = false;
    }
  };
  save.addEventListener('click', () => doSave('replace'));
  ed.ta.addEventListener('input', () => { syncDirty(); });
  ed.ta.addEventListener('keydown', (e) => { if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) { e.preventDefault(); doSave('replace'); } });
  d.onkeydown = (e) => { if (e.key === 'Enter' && (e.metaKey || e.ctrlKey) && !typing(e.target)) { e.preventDefault(); doSave('replace'); } };

  const del = btn('Delete', { kind: 'danger-ghost', icon: 'trash', onclick: async () => {
    if (!await confirmDialog(`Delete ${doc.key}?`, `It is removed from ${name}. You can undo for a few seconds.`, 'Delete', true)) return;
    try {
      await api('DELETE', `collections/${enc(name)}/docs/${enc(doc.key)}?version=${doc.version}`);
      delete drafts[id];
      saveDrafts();
      closeDrawer();
      refreshResults(name);
      toast(`Deleted ${doc.key}`, { action: { label: 'Undo', run: async () => {
        try {
          await api('PUT', `collections/${enc(name)}/docs/${enc(doc.key)}?mode=create`, doc.text, { raw: true });
          toast(`Restored ${doc.key}`, { kind: 'ok' });
          refreshResults(name);
        } catch (e) { toast(e.message, 'error'); }
      } } });
    } catch (e) {
      err.textContent = e.status === 409 ? 'This document changed since you opened it: reload it before deleting.' : e.message;
    }
  } });
  const dup = btn('', { icon: 'dup', title: 'Duplicate as a new document', onclick: () => openNewDocument(name, { key: `${doc.key}-copy`, text: ed.ta.value }) });
  const copy = btn('', { icon: 'copy', title: 'Copy the JSON', onclick: () => copyText(ed.ta.value, 'JSON') });

  d.replaceChildren(
    drawerShell(name, doc.key, doc.version, h('button', { class: 'icon-btn small', type: 'button', title: 'Copy the key', 'aria-label': 'Copy the key', onclick: () => copyText(doc.key, 'key') }, icon('copy', 13))),
    h('div', { class: 'drawer-bar' }, modeSeg, h('span', { class: 'spacer' }), unsaved),
    h('div', { class: 'drawer-body' }, note, treeHost, ed.ta, ed.status, help, err),
    h('div', { class: 'drawer-foot' }, del, dup, h('span', { class: 'spacer' }), copy, save));
  // The segmented control is replaced on each switch; keep the reference current.
  modeSeg = d.querySelector('.drawer-bar .seg');
  ed.check();
  unsaved.hidden = !dirty();
  setMode(mode);
}

function openNewDocument(name, o) {
  const opts = o || {};
  const d = $('drawer');
  d.hidden = false;
  drawer.name = name;
  drawer.key = null;
  ++drawer.seq;
  const key = h('input', { type: 'text', placeholder: 'e.g. user:1001', value: opts.key || '', spellcheck: 'false', autocomplete: 'off', 'aria-label': 'Key' });
  const ed = editorArea(opts.text || '{\n  \n}');
  const err = h('div', { class: 'error-text', role: 'alert' });
  const create = btn('Create', { kind: 'primary', kbd: `${MOD}↵` });
  const doCreate = async () => {
    if (!key.value.trim()) { err.textContent = 'Give the document a key.'; key.focus(); return; }
    if (!ed.check()) return;
    create.disabled = true;
    err.textContent = '';
    try {
      const saved = await api('PUT', `collections/${enc(name)}/docs/${enc(key.value.trim())}?mode=create`, ed.ta.value, { raw: true });
      toast(`Created ${saved.key}`, { kind: 'ok' });
      rememberRecent(name, saved.key);
      setDocInUrl(saved.key);
      drawer.key = saved.key;
      drawDocument(name, saved);
      refreshResults(name);
      poll().catch(() => {});
    } catch (e) {
      err.textContent = e.status === 409 ? `A document “${key.value.trim()}” already exists.` : e.message;
    } finally {
      create.disabled = false;
    }
  };
  create.addEventListener('click', doCreate);
  ed.ta.addEventListener('keydown', (e) => { if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) { e.preventDefault(); doCreate(); } });
  key.addEventListener('keydown', enter(() => ed.ta.focus()));
  d.onkeydown = null;
  d.replaceChildren(
    h('div', { class: 'drawer-head' }, h('div', { class: 'title' }, h('div', { class: 'coll' }, name), h('div', { class: 'mono' }, h('span', {}, opts.key ? 'Duplicate document' : 'New document'))),
      iconBtn('close', 'Close (Esc)', closeDrawer)),
    h('div', { class: 'drawer-body' }, h('label', { class: 'field' }, 'Key', key), h('label', { class: 'field' }, 'Fields', ed.ta), ed.status, h('div', { class: 'help' }, TYPE_HELP), err),
    h('div', { class: 'drawer-foot' }, h('span', { class: 'spacer' }), create));
  setDocInUrl(null);
  if (opts.key) { key.focus(); key.select(); } else { key.focus(); ed.ta.setSelectionRange(4, 4); }
}

// ---------------------------------------------------------------------------------------------
// Analytics
// ---------------------------------------------------------------------------------------------

function renderAnalytics(name, body) {
  const a = viewOf(name).agg;
  const rt = runtimeOf(name);
  const out = h('div', { class: 'results' });
  const run = async () => {
    let filter;
    try { filter = buildFilter(a.conds, ''); } catch (e) { toast(e.message, 'error'); return; }
    const groupBy = a.groupBy.split(',').map((s) => s.trim()).filter(Boolean);
    const req = { filter, group_by: groupBy, aggs: a.aggs.map((x) => ({ fn: x.fn, field: x.field.trim(), as: x.as.trim() })) };
    if (a.order.trim()) req.order_by = a.order.split(',').map((s) => s.trim()).filter(Boolean);
    if (!rt.agg) out.replaceChildren(skeletonTable(5, 3));
    out.classList.add('busy');
    const t0 = performance.now();
    try {
      rt.agg = await api('POST', `collections/${enc(name)}/aggregate`, req);
      rt.aggTook = performance.now() - t0;
      if (out.isConnected) drawAgg(a, rt, out);
    } catch (e) {
      out.replaceChildren(h('div', { class: 'error-box' }, e.message));
    } finally {
      out.classList.remove('busy');
    }
  };
  const cond = conditionEditor(a.conds, run);
  const aggList = h('div', { class: 'stack', style: 'gap:8px' });
  const drawAggs = () => {
    aggList.replaceChildren(...a.aggs.map((x, i) => h('div', { class: 'agg-row' },
      h('select', { 'aria-label': 'Aggregate', onchange: (e) => { x.fn = e.target.value; savePrefs(); } }, AGGS.map(([v, l]) => h('option', { value: v, selected: x.fn === v }, l))),
      h('input', { type: 'text', value: x.field, placeholder: x.fn === 'count' ? 'field (optional)' : 'field', spellcheck: 'false', 'aria-label': 'Field', oninput: (e) => { x.field = e.target.value; savePrefs(); }, onkeydown: enter(run) }),
      h('input', { type: 'text', value: x.as, placeholder: 'column name (optional)', spellcheck: 'false', 'aria-label': 'Column name', oninput: (e) => { x.as = e.target.value; savePrefs(); }, onkeydown: enter(run) }),
      iconBtn('close', 'Remove aggregate', () => { a.aggs.splice(i, 1); savePrefs(); drawAggs(); }, { disabled: a.aggs.length === 1 }))));
  };
  drawAggs();
  const groupBy = h('input', { type: 'text', value: a.groupBy, placeholder: 'fields, comma separated (none: one row)', spellcheck: 'false', class: 'grow', 'aria-label': 'Group by',
    oninput: (e) => { a.groupBy = e.target.value; savePrefs(); }, onkeydown: enter(run) });
  const order = h('input', { type: 'text', value: a.order, placeholder: 'Order by columns, -column for descending', spellcheck: 'false', 'aria-label': 'Order by', style: 'min-width:260px',
    oninput: (e) => { a.order = e.target.value; savePrefs(); }, onkeydown: enter(run) });
  const show = seg([['table', 'Table', 'table'], ['chart', 'Chart', 'chart']], a.show, (m) => {
    a.show = m;
    savePrefs();
    show.querySelectorAll('button').forEach((b, i) => { b.classList.toggle('on', (i === 0 ? 'table' : 'chart') === m); });
    if (rt.agg) drawAgg(a, rt, out);
  });
  body.append(h('div', { class: 'card' },
    h('div', { class: 'toolbar' },
      h('div', { class: 'builder-section' }, h('h3', {}, 'Where'), cond.el,
        h('div', {}, btn('Condition', { small: true, icon: 'filter', onclick: () => cond.add() }))),
      h('div', { class: 'builder-section' }, h('h3', {}, 'Group by'), h('div', { class: 'row' }, groupBy)),
      h('div', { class: 'builder-section' }, h('h3', {}, 'Compute'), aggList),
      h('div', { class: 'toolbar-row' },
        btn('Aggregate', { small: true, icon: 'plus', onclick: () => { a.aggs.push({ fn: 'sum', field: '', as: '' }); savePrefs(); drawAggs(); } }),
        order, h('span', { class: 'spacer' }), show, h('span', { class: 'divider' }),
        btn('Run', { small: true, kind: 'primary', icon: 'play', onclick: run }))),
    out));
  page.docs = null;
  page.runAnalytics = run;
  page.cleanup.push(() => { page.runAnalytics = null; });
  if (rt.agg) drawAgg(a, rt, out);
  else out.replaceChildren(emptyState('chart', 'Exact aggregates on the server', 'Sums and averages of integers and decimals are exact. Enable columnar storage for large collections.'));
}

function drawAgg(a, rt, out) {
  const r = rt.agg;
  const numericCol = r.columns.map((_, i) => r.rows.length > 0 && r.rows.every((row) => numberOf(row[i]) !== null || row[i] === null));
  const foot = h('div', { class: 'results-foot' },
    h('span', {}, `${plural(r.rows.length, 'row')} · ${plural(r.matched, 'document')} matched`, h('span', { class: 'muted' }, ` · ${Math.round(rt.aggTook || 0)} ms`)),
    h('span', {}, r.pruned_row_groups ? `${num(r.pruned_row_groups)} row groups skipped` : '', r.spilled_bytes ? ` · spilled ${bytes(r.spilled_bytes)}` : ''));
  if (r.rows.length === 0) { out.replaceChildren(emptyState('filter', 'No groups', 'No document matched.'), foot); return; }
  const valueCol = numericCol.lastIndexOf(true);
  if (a.show === 'chart' && valueCol >= 0) {
    const labels = r.columns.map((_, i) => i).filter((i) => !numericCol[i]);
    const rows = r.rows.slice(0, 100);
    const max = Math.max(...rows.map((row) => Math.abs(numberOf(row[valueCol]) || 0))) || 1;
    out.replaceChildren(h('div', { class: 'bars' },
      h('div', { class: 'bar-row muted' }, h('span', { class: 'label' }, labels.map((i) => r.columns[i]).join(' · ') || 'all'), h('span', {}), h('span', { class: 'val muted' }, r.columns[valueCol])),
      rows.map((row) => {
        const val = numberOf(row[valueCol]) || 0;
        const label = labels.length ? labels.map((i) => (row[i] === null ? 'null' : typeof row[i] === 'object' ? JSON.stringify(row[i]) : String(row[i]))).join(' · ') : 'all';
        return h('div', { class: 'bar-row' }, h('span', { class: 'label', title: label }, label),
          h('div', { class: 'bar-track' }, h('span', { class: 'bar-fill', style: `width:${Math.max(0.5, (Math.abs(val) / max) * 100).toFixed(2)}%` })),
          h('span', { class: 'val' }, num(Math.round(val * 100) / 100)));
      })), foot);
    return;
  }
  fill(out,
    a.show === 'chart' ? h('div', { class: 'note info', style: 'margin:12px 16px 0' }, 'A chart needs a numeric column.') : null,
    h('div', { class: 'table-wrap' }, h('table', {},
      h('thead', {}, h('tr', {}, r.columns.map((c, i) => h('th', { class: numericCol[i] ? 'num' : null }, c)))),
      h('tbody', {}, r.rows.map((row) => h('tr', {}, row.map((x, i) => h('td', { class: numericCol[i] ? 'num' : null }, cell(x)))))))),
    foot);
}

// ---------------------------------------------------------------------------------------------
// Indexes and settings
// ---------------------------------------------------------------------------------------------

function renderIndexes(name, body, info) {
  if (!info) { body.replaceChildren(h('div', { class: 'card' }, emptyState('bolt', 'No such collection yet', 'Create a document first.'))); return; }
  const field = h('input', { type: 'text', placeholder: 'field, e.g. email', spellcheck: 'false', 'aria-label': 'Field to index', class: 'grow' });
  const kind = h('select', { 'aria-label': 'Index kind' }, h('option', { value: 'secondary' }, 'Secondary index'), h('option', { value: 'vector' }, 'Vector index'));
  const dims = h('input', { type: 'number', min: 1, max: 4096, placeholder: 'dimensions', 'aria-label': 'Dimensions', style: 'width:130px', hidden: true });
  kind.addEventListener('change', () => { dims.hidden = kind.value !== 'vector'; });
  const add = btn('Create index', { kind: 'primary', small: true, icon: 'plus' });
  const create = async () => {
    if (!field.value.trim()) { field.focus(); return; }
    const req = { field: field.value.trim() };
    if (kind.value === 'vector') {
      if (!(Number(dims.value) > 0)) { dims.focus(); return; }
      req.dims = Number(dims.value);
    }
    add.disabled = true;
    add.lastChild.textContent = 'Building…';
    try {
      await api('POST', `collections/${enc(name)}/indexes`, req);
      toast(`Indexed ${req.field}`, { kind: 'ok' });
      render();
    } catch (e) {
      toast(e.message, 'error');
      add.disabled = false;
      add.lastChild.textContent = 'Create index';
    }
  };
  add.addEventListener('click', create);
  field.addEventListener('keydown', enter(create));
  const drop = async (f, vector) => {
    if (!await confirmDialog(`Drop the index on ${f}?`, `Queries on ${f} will scan the collection instead. Rebuilding it reads every document.`, 'Drop index', true)) return;
    try {
      await api('DELETE', `collections/${enc(name)}/indexes/${enc(f)}${vector ? '?vector=1' : ''}`);
      toast(`Dropped the index on ${f}`);
      render();
    } catch (e) { toast(e.message, 'error'); }
  };
  const rows = [
    ...info.indexes.map((f) => [f, 'Secondary', h('span', { class: 'badge ok' }, 'ready'), false]),
    ...info.building.map((f) => [f, 'Secondary', h('span', { class: 'badge warn' }, 'building'), false]),
    ...info.vector_indexes.map((x) => [x.field, `Vector · ${x.dims} dimensions`, h('span', { class: `badge ${x.ready ? 'ok' : 'warn'}` }, x.ready ? 'ready' : 'building'), true]),
  ];
  body.replaceChildren(h('div', { class: 'stack' },
    h('div', { class: 'card' }, h('div', { class: 'card-head' }, h('h2', {}, 'Indexes'), h('span', { class: 'muted' }, 'Up to 16 secondary and 4 vector indexes, each on one top-level field.')),
      rows.length === 0 ? emptyState('bolt', 'No indexes', 'Queries scan the collection. Index the fields you filter and sort on.') :
        h('div', { class: 'table-wrap' }, h('table', {},
          h('thead', {}, h('tr', {}, h('th', {}, 'Field'), h('th', {}, 'Kind'), h('th', {}, 'State'), h('th', {}, ''))),
          h('tbody', {}, rows.map(([f, k, s, vec]) => h('tr', {}, h('td', {}, h('span', { class: 'cell key' }, icon('bolt', 13), h('span', {}, f))), h('td', {}, k), h('td', {}, s),
            h('td', { class: 'num' }, btn('Drop', { kind: 'danger-ghost', small: true, onclick: () => drop(f, vec) })))))))),
    h('div', { class: 'card' }, h('div', { class: 'card-head' }, h('h2', {}, 'New index')),
      h('div', { class: 'card-body' }, h('div', { class: 'row' }, field, kind, dims, add),
        h('p', { class: 'help' }, 'Building reads every existing document, then the index stays current with each write.')))));
}

function renderSettings(name, body, info) {
  const confirmBox = h('input', { type: 'text', placeholder: name, spellcheck: 'false', autocomplete: 'off', 'aria-label': 'Type the collection name to confirm' });
  const dropBtn = btn('Drop collection', { kind: 'danger', icon: 'trash', disabled: true });
  confirmBox.addEventListener('input', () => { dropBtn.disabled = confirmBox.value !== name; });
  dropBtn.addEventListener('click', async () => {
    dropBtn.disabled = true;
    try {
      const r = await api('DELETE', `collections/${enc(name)}`);
      toast(`Dropped ${name}: ${plural(r.deleted, 'document')} deleted`);
      const sc = scope();
      delete sc.views[name];
      delete sc.history[name];
      sc.pinned = sc.pinned.filter((n) => n !== name);
      sc.recent = sc.recent.filter((x) => x.c !== name);
      savePrefs();
      runtimes.delete(name);
      await poll().catch(() => {});
      go('#/');
    } catch (e) {
      toast(e.message, 'error');
      dropBtn.disabled = false;
    }
  });
  const sc = scope();
  const kept = [(sc.history[name] || []).length && plural(sc.history[name].length, 'recent query', 'recent queries'), (sc.saved[name] || []).length && plural(sc.saved[name].length, 'saved query', 'saved queries')].filter(Boolean);
  body.replaceChildren(h('div', { class: 'stack' },
    h('div', { class: 'card' }, h('div', { class: 'card-head' }, h('h2', {}, 'Metadata')),
      h('div', { class: 'card-body' }, info ? highlighted(JSON.stringify(info.metadata, null, 2)) : h('span', { class: 'muted' }, 'None.'))),
    h('div', { class: 'card' }, h('div', { class: 'card-head' }, h('h2', {}, 'In this browser')),
      h('div', { class: 'card-body stack', style: 'gap:10px' },
        h('p', { class: 'help', style: 'margin:0' }, 'The query, columns, analytics and history of this collection are remembered in this browser only.',
          kept.length ? ` Kept: ${kept.join(', ')}.` : ''),
        h('div', {}, btn('Reset this collection’s view', { small: true, icon: 'refresh', onclick: async () => {
          if (!await confirmDialog('Reset the view?', 'Clears the conditions, sorting, columns, analytics and query history kept for this collection. Saved queries stay.', 'Reset')) return;
          delete sc.views[name];
          delete sc.history[name];
          runtimes.delete(name);
          savePrefs();
          toast('View reset');
        } })))),
    h('div', { class: 'card danger-zone' }, h('div', { class: 'card-head' }, h('h2', {}, 'Drop this collection')),
      h('div', { class: 'card-body stack', style: 'gap:10px' },
        h('p', { style: 'margin:0' }, 'Deletes every document and index in ', h('strong', { class: 'mono' }, name), '. This cannot be undone: back up first (storage-cli remote-backup).'),
        h('label', { class: 'field' }, 'Type the collection name to confirm', confirmBox),
        h('div', {}, dropBtn)))));
}

// ---------------------------------------------------------------------------------------------
// Command palette and shortcuts
// ---------------------------------------------------------------------------------------------

// How well `q` matches `text`: substrings rank first, then letters in order. 0 means no match.
function score(q, text) {
  if (!q) return 1;
  const t = text.toLowerCase();
  const i = t.indexOf(q);
  if (i >= 0) return 1000 - i * 2 - (t.length - q.length) * 0.1 + (i === 0 || /[\s:/._-]/.test(t[i - 1]) ? 50 : 0);
  let pos = 0;
  let run = 0;
  let s = 0;
  for (const ch of q) {
    const j = t.indexOf(ch, pos);
    if (j < 0) return 0;
    run = j === pos ? run + 1 : 0;
    s += 10 + run * 5;
    pos = j + 1;
  }
  return s;
}

function highlight(text, q) {
  if (!q) return text;
  const i = text.toLowerCase().indexOf(q);
  if (i < 0) return text;
  return [text.slice(0, i), h('mark', {}, text.slice(i, i + q.length)), text.slice(i + q.length)];
}

function paletteItems(raw) {
  const q = raw.trim().toLowerCase();
  const s = app.get();
  const sc = scope();
  const r = route();
  const items = [];
  const add = (group, ic, label, sub, run, extra) => items.push({ group, ic, label, sub, run, text: `${label} ${extra || ''}` });
  for (const x of sc.recent.slice(0, q ? MAX_RECENT : 5)) add('Recent documents', 'doc', x.key, x.c, () => go(collectionHash(x.c, 'documents', x.key)), x.c);
  const colls = [...s.collections].sort((a, b) => (sc.pinned.includes(b.name) ? 1 : 0) - (sc.pinned.includes(a.name) ? 1 : 0));
  for (const c of colls.slice(0, q ? 200 : 8)) {
    add('Collections', sc.pinned.includes(c.name) ? 'pin' : 'db', c.name, c.count === null ? '' : plural(c.count, 'document'), () => go(collectionHash(c.name)));
    if (q) for (const [t, label, ic] of TABS.slice(1)) add('Collections', ic, `${c.name} › ${label}`, '', () => go(collectionHash(c.name, t)));
  }
  for (const [cn, list] of Object.entries(sc.saved)) {
    if (!s.collections.some((c) => c.name === cn)) continue;
    for (const sq of list) add('Saved queries', 'star', sq.name, `${cn} · ${describe(sq)}`, () => applySnapshot(cn, sq), cn);
  }
  if (r.page === 'collection') add('Actions', 'plus', `New document in ${r.name}`, 'N', () => openNewDocument(r.name));
  add('Actions', 'grid', 'Go to the overview', 'G then O', () => go('#/'));
  add('Actions', 'plus', 'New collection', '', newCollection);
  add('Actions', prefs.theme === 'dark' ? 'moon' : prefs.theme === 'light' ? 'sun' : 'monitor', 'Switch theme', `now ${prefs.theme} · T`, cycleTheme, 'dark light');
  add('Actions', 'density', prefs.density === 'compact' ? 'Comfortable rows' : 'Compact rows', 'density', () => { toggleDensity(); if (page.docs) drawResults(page.docs); });
  add('Actions', prefs.live ? 'pause' : 'pulse', prefs.live ? 'Pause live updates' : 'Resume live updates', 'overview', () => { prefs.live = !prefs.live; savePrefs(); if (route().page === 'overview') render(); });
  add('Actions', 'menu', navShown() ? 'Hide the sidebar' : 'Show the sidebar', '', () => setNav(!navShown()));
  add('Actions', 'keyboard', 'Keyboard shortcuts', '?', showShortcuts);
  add('Actions', 'logout', 'Disconnect', s.session && s.session.url, disconnect);
  let out = q ? items.map((x) => ({ ...x, score: score(q, x.text) })).filter((x) => x.score > 0).sort((a, b) => b.score - a.score).slice(0, 40) : items;
  if (q && r.page === 'collection') {
    out = [...out, { group: 'Open', ic: 'enter', label: `Open “${raw.trim()}”`, sub: `document in ${r.name}`, run: () => openDocument(r.name, raw.trim()) }];
  }
  return out;
}

function openPalette() {
  if (!(app.get().session && app.get().session.connected)) return;
  const dlg = $('palette');
  closePopover();
  const input = h('input', { type: 'text', class: 'pal-input', placeholder: 'Jump to a collection, document or action…', spellcheck: 'false', autocomplete: 'off', 'aria-label': 'Search' });
  const list = h('div', { class: 'pal-list', role: 'listbox' });
  let items = [];
  let sel = 0;
  const pick = (i) => {
    const it = items[i];
    if (!it) return;
    dlg.close();
    it.run();
  };
  const draw = () => {
    const q = input.value.trim().toLowerCase();
    items = paletteItems(input.value);
    sel = Math.max(0, Math.min(sel, items.length - 1));
    if (items.length === 0) { list.replaceChildren(h('div', { class: 'menu-empty' }, 'Nothing matches.')); return; }
    const nodes = [];
    let group = null;
    items.forEach((it, i) => {
      if (!q && it.group !== group) { group = it.group; nodes.push(h('div', { class: 'pal-group' }, group)); }
      nodes.push(h('div', { class: `pal-item${i === sel ? ' sel' : ''}`, role: 'option', 'aria-selected': i === sel ? 'true' : 'false',
        onmousemove: () => { if (sel !== i) { sel = i; mark(); } }, onclick: () => pick(i) },
      h('span', { class: 'ico-wrap' }, icon(it.ic, 15)),
      h('span', { class: 'text' }, h('div', { class: 'main-line' }, highlight(it.label, q)), it.sub ? h('div', { class: 'sub-line' }, it.sub) : null),
      q ? h('span', { class: 'badge' }, it.group) : null,
      h('span', { class: 'enter' }, icon('enter', 14))));
    });
    list.replaceChildren(...nodes);
  };
  const mark = () => {
    list.querySelectorAll('.pal-item').forEach((el, i) => { el.classList.toggle('sel', i === sel); el.setAttribute('aria-selected', i === sel ? 'true' : 'false'); });
    const el = list.querySelectorAll('.pal-item')[sel];
    if (el) el.scrollIntoView({ block: 'nearest' });
  };
  input.addEventListener('input', () => { sel = 0; draw(); });
  input.addEventListener('keydown', (e) => {
    if (e.key === 'ArrowDown') { e.preventDefault(); sel = Math.min(items.length - 1, sel + 1); mark(); } else if (e.key === 'ArrowUp') { e.preventDefault(); sel = Math.max(0, sel - 1); mark(); } else if (e.key === 'Enter') { e.preventDefault(); pick(sel); }
  });
  dlg.replaceChildren(
    h('div', { class: 'pal-search' }, icon('search', 18), input, h('kbd', {}, 'esc')),
    list,
    h('div', { class: 'pal-foot' }, h('span', {}, h('kbd', {}, '↑'), h('kbd', {}, '↓'), 'move'), h('span', {}, h('kbd', {}, '↵'), 'open'), h('span', {}, h('kbd', {}, 'esc'), 'close')));
  dlg.oncancel = null;
  dlg.onclick = (e) => { if (e.target === dlg) dlg.close(); };
  draw();
  dlg.showModal();
  input.focus();
}

function showShortcuts() {
  const rows = [
    [[MOD, 'K'], 'Search and jump'],
    [['/'], 'Add or focus a filter'],
    [['J'], 'Next document'], [['K'], 'Previous document'], [['↵'], 'Open the selected document'],
    [['['], 'Previous page'], [[']'], 'Next page'],
    [['N'], 'New document'], [['R'], 'Run the query / refresh'],
    [['G', 'O'], 'Overview'], [['G', 'D'], 'Documents'], [['G', 'A'], 'Analytics'], [['G', 'I'], 'Indexes'],
    [['T'], 'Switch theme'], [['B'], 'Show or hide the sidebar'],
    [[MOD, '↵'], 'Save the open document'], [['Esc'], 'Close'],
  ];
  modal('Keyboard shortcuts', h('div', { class: 'shortcuts' }, rows.flatMap(([keys, what]) => [h('span', {}, what), h('span', { class: 'keys' }, keys.map((k) => h('kbd', {}, k)))])),
    [{ label: 'Close', kind: 'primary', value: true }]);
}

let pendingG = 0;
function onKey(e) {
  const mod = e.metaKey || e.ctrlKey;
  if (mod && e.key.toLowerCase() === 'k') { e.preventDefault(); if ($('palette').open) $('palette').close(); else openPalette(); return; }
  if (e.key === 'Escape') {
    if (openPop) { closePopover(); return; }
    if (!dialogOpen() && !$('drawer').hidden) { closeDrawer(); return; }
    if (!dialogOpen() && narrow() && navShown()) { setNav(false); return; }
    return;
  }
  if (mod || e.altKey || dialogOpen() || typing(e.target)) return;
  if (!(app.get().session && app.get().session.connected)) return;
  const r = route();
  if (pendingG && Date.now() - pendingG < 1200) {
    pendingG = 0;
    const tab = { d: 'documents', a: 'analytics', i: 'indexes', s: 'settings' }[e.key];
    if (e.key === 'o') go('#/');
    else if (tab && r.page === 'collection') go(collectionHash(r.name, tab));
    return;
  }
  const docsOpen = !$('drawer').hidden;
  switch (e.key) {
    case 'g': pendingG = Date.now(); break;
    case '?': showShortcuts(); break;
    case 't': cycleTheme(); break;
    case 'b': setNav(!navShown()); break;
    case '/':
      e.preventDefault();
      if (page.docs) page.docs.focusFilter();
      else $('nav-filter').focus();
      break;
    case 'n': if (r.page === 'collection') { e.preventDefault(); openNewDocument(r.name); } break;
    case 'j': moveCursor(1); break;
    case 'k': moveCursor(-1); break;
    case '[': if (page.docs) pageBy(page.docs, -1); break;
    case ']': if (page.docs) pageBy(page.docs, 1); break;
    case 'r':
      if (page.docs) page.docs.run(true);
      else if (page.runAnalytics) page.runAnalytics();
      else if (r.page === 'overview') poll().catch((err) => toast(err.message, 'error'));
      break;
    case 'Enter':
      if (e.target === document.body && page.docs && !docsOpen && page.docs.rt.cursor >= 0 && page.docs.rt.result) {
        openDocument(page.docs.name, page.docs.rt.result.docs[page.docs.rt.cursor].key);
      }
      break;
    default:
  }
}

// ---------------------------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------------------------

async function disconnect() {
  try { await api('POST', 'disconnect'); } catch (e) { toast(e.message, 'error'); return; }
  runtimes.clear();
  closeDrawer();
  app.set({ session: { connected: false }, overview: null, collections: [], samples: [], latency: null, offline: false });
  go('#/');
}

function startLiveUpdates() {
  let last = 0;
  setInterval(() => {
    const s = app.get();
    if (!(s.session && s.session.connected) || document.visibilityState !== 'visible') return;
    const onOverview = route().page === 'overview';
    const every = onOverview && prefs.live ? LIVE_MS : IDLE_POLL_MS;
    if (Date.now() - Math.max(last, s.updatedAt) < every - 250) return;
    last = Date.now();
    poll().catch(() => {});
  }, 1000);
}

async function start() {
  applyTheme();
  applyDensity();
  $('layout').classList.toggle('nav-hidden', !!prefs.navHidden);
  $('menu').append(icon('menu', 18));
  $('disconnect').append(icon('logout', 18));
  $('new-collection').append(icon('plus', 15));
  $('nav-search-icon').append(icon('search', 14));
  $('palette-open').append(icon('search', 15), h('span', { class: 'label' }, 'Jump to…'), h('kbd', {}, `${MOD} K`));

  const m = location.hash.match(/^#key=([A-Za-z0-9_-]+)/);
  if (m) {
    local.set(KEY_STORE, m[1]);
    history.replaceState(null, '', `${location.pathname}#/`);
  }
  app.set({ key: local.get(KEY_STORE) });

  $('theme').addEventListener('click', cycleTheme);
  $('disconnect').addEventListener('click', disconnect);
  $('new-collection').addEventListener('click', newCollection);
  $('palette-open').addEventListener('click', openPalette);
  $('shortcuts').addEventListener('click', showShortcuts);
  $('menu').addEventListener('click', () => setNav(!navShown()));
  $('scrim').addEventListener('click', () => setNav(false));
  $('sidebar').addEventListener('click', (e) => { if (e.target.closest('a') && narrow()) setNav(false); });
  $('nav-filter').addEventListener('input', () => renderNav());
  $('nav-filter').addEventListener('keydown', (e) => {
    if (e.key === 'Enter') { const a = $('nav-list').querySelector('a'); if (a) { a.click(); e.target.blur(); } }
  });
  document.addEventListener('keydown', onKey);
  window.addEventListener('hashchange', () => {
    const r = route();
    // Opening and closing documents only touches ?doc=; anything else is a new page.
    if (!(r.page === 'collection' && drawer.name === r.name && r.doc && r.doc === drawer.key)) closeDrawer(true);
    render();
  });
  matchMedia('(prefers-color-scheme: dark)').addEventListener('change', applyTheme);
  setInterval(tickClocks, 1000);
  app.subscribe(updateShell);

  if (!app.get().key) { showLocked(); return; }
  let sess;
  try {
    sess = await api('GET', 'session');
  } catch (e) {
    if (e.status !== 401) $('main').replaceChildren(errorPanel(e, () => location.reload()));
    return;
  }
  app.set({ session: sess });
  if (sess.connected) await poll().catch((e) => toast(e.message, 'error'));
  render();
  startLiveUpdates();
}

start();
