// Storine Console: a single-page app over the console's JSON API (see src/api.rs).
// No libraries; every element is built with h(), never from HTML strings.
'use strict';

const KEY_STORE = 'storine-console-key';
const THEME_STORE = 'storine-console-theme';
const PAGE_SIZES = [25, 50, 100, 250];
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

const store = {
  get(k) { try { return localStorage.getItem(k); } catch { return null; } },
  set(k, v) { try { localStorage.setItem(k, v); } catch { /* storage unavailable */ } },
  del(k) { try { localStorage.removeItem(k); } catch { /* storage unavailable */ } },
};

const state = {
  key: null,
  session: null,
  overview: null,
  collections: [],
  // Per collection: query and analytics settings, kept while browsing.
  views: new Map(),
  refreshTimer: null,
};

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

const $ = (id) => document.getElementById(id);
const fmt = new Intl.NumberFormat();
const num = (n) => (typeof n === 'number' ? fmt.format(n) : n === null || n === undefined ? '—' : String(n));

function bytes(n) {
  if (typeof n !== 'number') return '—';
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let i = 0;
  while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
  return `${i === 0 ? n : n.toFixed(n < 10 ? 2 : 1)} ${units[i]}`;
}

function toast(message, kind) {
  const t = h('div', { class: `toast ${kind || ''}`, role: kind === 'error' ? 'alert' : 'status' }, message);
  $('toasts').append(t);
  setTimeout(() => t.remove(), kind === 'error' ? 7000 : 3500);
}

function spinner(label) {
  return h('div', { class: 'loading' }, h('span', { class: 'spinner' }), label || 'Loading…');
}

function enc(s) { return encodeURIComponent(s); }

// ---------------------------------------------------------------------------------------------
// API
// ---------------------------------------------------------------------------------------------

class ApiError extends Error {
  constructor(status, message) { super(message); this.status = status; }
}

async function api(method, path, body, opts) {
  const headers = { 'X-Console-Key': state.key || '' };
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

// A compact cell for a table.
function cell(v) {
  if (v === undefined) return h('span', { class: 'v-dim' }, '');
  if (v === null) return h('span', { class: 'v-null' }, 'null');
  if (typeof v === 'string') return h('span', { class: 'v-str', title: v.length > 60 ? v.slice(0, 2000) : null }, v);
  if (typeof v === 'number') return h('span', { class: 'v-num' }, String(v));
  if (typeof v === 'boolean') return h('span', { class: 'v-lit' }, String(v));
  if (Array.isArray(v)) return h('span', { class: 'v-dim' }, `[${v.length} item${v.length === 1 ? '' : 's'}]`);
  const tag = tagOf(v);
  if (tag === '$bytes') return h('span', { class: 'v-tag' }, `bytes · ${bytes(Math.floor(v.$bytes.replace(/=+$/, '').length * 3 / 4))}`);
  if (tag) return h('span', { class: tag === '$int' || tag === '$decimal' || tag === '$float' ? 'v-num' : 'v-tag', title: TAGS[tag] }, v[tag]);
  const n = Object.keys(v).length;
  return h('span', { class: 'v-dim' }, `{${n} field${n === 1 ? '' : 's'}}`);
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

// ---------------------------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------------------------

function route() {
  const hash = location.hash.replace(/^#/, '');
  const parts = hash.split('/').filter(Boolean).map(decodeURIComponent);
  if (parts[0] === 'c' && parts[1]) return { page: 'collection', name: parts[1], tab: parts[2] || 'documents' };
  return { page: 'overview' };
}

function go(hash) {
  if (location.hash === hash) render();
  else location.hash = hash;
}

function collectionHash(name, tab) {
  return `#/c/${enc(name)}${tab && tab !== 'documents' ? `/${tab}` : ''}`;
}

// ---------------------------------------------------------------------------------------------
// Shell
// ---------------------------------------------------------------------------------------------

function applyTheme(t) {
  if (t) document.documentElement.setAttribute('data-theme', t);
  else document.documentElement.removeAttribute('data-theme');
}

function currentTheme() {
  const set = document.documentElement.getAttribute('data-theme');
  if (set) return set;
  return matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}

function showLocked() {
  closeDrawer();
  $('sidebar').hidden = true;
  $('conn').hidden = true;
  $('disconnect').hidden = true;
  $('menu').hidden = true;
  const main = $('main');
  main.replaceChildren(h('div', { class: 'connect-wrap' }, h('div', { class: 'card connect' },
    h('h1', {}, 'Open the console link'),
    h('p', { class: 'lead' }, 'This page needs the key the console printed when it started. Open the link from its output, which looks like:'),
    h('pre', { class: 'json' }, 'http://127.0.0.1:7880/#key=…'),
    h('p', { class: 'help' }, 'A new key is made each time storine-console starts, so links from earlier runs stop working.'),
  )));
}

function updateShell() {
  const s = state.session;
  const connected = !!(s && s.connected);
  $('sidebar').hidden = !connected;
  $('conn').hidden = !connected;
  $('disconnect').hidden = !connected;
  $('menu').hidden = !connected;
  if (connected) {
    $('conn-url').textContent = s.url;
    const c = state.overview && state.overview.cluster;
    const dot = $('conn-dot');
    const role = $('conn-role');
    if (c) {
      const standalone = c.members.length === 0 && c.node_id === 0;
      dot.className = `dot ${c.is_leader || c.leader_id ? 'ok' : 'warn'}`;
      role.textContent = standalone ? 'standalone' : c.is_leader ? `leader · node ${c.node_id}` : `follower · node ${c.node_id}`;
      role.className = `badge ${c.is_leader ? 'accent' : ''}`;
    } else {
      dot.className = 'dot';
      role.textContent = '';
    }
  }
  renderNav();
}

function renderNav() {
  const list = $('nav-list');
  const r = route();
  $('nav-overview').classList.toggle('active', r.page === 'overview');
  const q = $('nav-filter').value.trim().toLowerCase();
  const items = state.collections.filter((c) => !q || c.name.toLowerCase().includes(q));
  if (state.collections.length === 0) {
    list.replaceChildren(h('div', { class: 'nav-empty' }, 'No collections yet.'));
    return;
  }
  if (items.length === 0) {
    list.replaceChildren(h('div', { class: 'nav-empty' }, 'No match.'));
    return;
  }
  list.replaceChildren(...items.map((c) => h('a', {
    class: `nav-item${r.page === 'collection' && r.name === c.name ? ' active' : ''}`,
    href: collectionHash(c.name),
    title: c.name,
  },
  h('svg', { viewBox: '0 0 24 24', width: 15, height: 15, 'aria-hidden': 'true' }),
  h('span', { class: 'name' }, c.name),
  h('span', { class: 'count' }, c.count === null ? '' : num(c.count)))));
  for (const svg of list.querySelectorAll('svg')) {
    const p = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    p.setAttribute('fill', 'currentColor');
    p.setAttribute('d', 'M12 3C7.6 3 4 4.3 4 6v12c0 1.7 3.6 3 8 3s8-1.3 8-3V6c0-1.7-3.6-3-8-3zm0 2c3.9 0 6 1.1 6 1s-2.1 1-6 1-6-.9-6-1 2.1-1 6-1zm6 13c0 .1-2.1 1-6 1s-6-.9-6-1v-2.3c1.5.8 3.7 1.3 6 1.3s4.5-.5 6-1.3V18zm0-5c0 .1-2.1 1-6 1s-6-.9-6-1v-2.3c1.5.8 3.7 1.3 6 1.3s4.5-.5 6-1.3V13z');
    svg.append(p);
  }
}

async function loadOverview() {
  state.overview = await api('GET', 'overview');
  state.collections = state.overview.collections;
  updateShell();
  return state.overview;
}

async function render() {
  clearInterval(state.refreshTimer);
  if (!state.session) return;
  if (!state.session.connected) {
    renderConnect();
    updateShell();
    return;
  }
  const r = route();
  renderNav();
  if (r.page === 'collection') return renderCollection(r.name, r.tab);
  return renderOverview();
}

// ---------------------------------------------------------------------------------------------
// Connect
// ---------------------------------------------------------------------------------------------

function renderConnect() {
  closeDrawer();
  const url = h('input', { type: 'text', value: store.get('storine-console-url') || 'storine://127.0.0.1:7878', autocomplete: 'off', spellcheck: 'false', required: true });
  const token = h('input', { type: 'password', autocomplete: 'off', placeholder: 'Only if the server requires one' });
  const ca = h('input', { type: 'text', autocomplete: 'off', spellcheck: 'false', placeholder: '/etc/storine/ca.crt' });
  const sni = h('input', { type: 'text', autocomplete: 'off', spellcheck: 'false', placeholder: 'Defaults to the host connected to' });
  const err = h('div', { class: 'error-text', role: 'alert' });
  const submit = h('button', { class: 'btn', type: 'submit' }, 'Connect');
  const form = h('form', {
    onsubmit: async (e) => {
      e.preventDefault();
      err.textContent = '';
      submit.disabled = true;
      submit.textContent = 'Connecting…';
      try {
        state.session = await api('POST', 'connect', { url: url.value, token: token.value, tls_ca: ca.value, tls_server_name: sni.value });
        store.set('storine-console-url', url.value);
        await loadOverview();
        go('#/');
      } catch (ex) {
        err.textContent = ex.message;
      } finally {
        submit.disabled = false;
        submit.textContent = 'Connect';
      }
    },
  },
  h('label', { class: 'field' }, 'Server', url, h('span', { class: 'hint' }, 'storine://host:7878, several hosts for a cluster, storines:// for TLS')),
  h('label', { class: 'field' }, 'Token', token),
  h('details', { class: 'more' }, h('summary', {}, 'TLS'),
    h('div', {},
      h('label', { class: 'field' }, 'CA certificate file', ca, h('span', { class: 'hint' }, 'A path on the machine running the console. Giving one turns TLS on.')),
      h('label', { class: 'field' }, 'Server name', sni))),
  err,
  submit);
  $('main').replaceChildren(h('div', { class: 'connect-wrap' }, h('div', { class: 'card connect' },
    h('h1', {}, 'Connect to Storine'),
    h('p', { class: 'lead' }, 'Browse documents, run queries and aggregates, manage indexes and watch the cluster.'),
    form)));
  url.focus();
}

// ---------------------------------------------------------------------------------------------
// Overview
// ---------------------------------------------------------------------------------------------

function stat(label, value, note) {
  return h('div', { class: 'card stat' }, h('div', { class: 'label' }, label), h('div', { class: 'value' }, value), note ? h('div', { class: 'note' }, note) : null);
}

async function renderOverview() {
  const main = $('main');
  if (!state.overview) main.replaceChildren(spinner('Loading the overview…'));
  let o;
  try {
    o = await loadOverview();
  } catch (e) {
    main.replaceChildren(errorPanel(e, renderOverview));
    return;
  }
  if (route().page !== 'overview') return;
  const c = o.cluster;
  const standalone = c.members.length === 0 && c.node_id === 0;
  const docs = o.collections.reduce((a, x) => a + (typeof x.count === 'number' ? x.count : 0), 0);
  const hl = o.health || {};
  const arena = hl.arena || {};

  const members = standalone
    ? h('div', { class: 'empty' }, h('strong', {}, 'A standalone server'), 'Start servers with --peers to form a replicated cluster.')
    : h('div', { class: 'table-wrap' }, h('table', {},
      h('thead', {}, h('tr', {}, h('th', {}, 'Node'), h('th', {}, 'Address'), h('th', {}, 'Role'))),
      h('tbody', {}, c.members.map((m) => h('tr', {},
        h('td', { class: 'mono' }, String(m.id)),
        h('td', { class: 'mono' }, m.addr),
        h('td', {}, m.leader ? h('span', { class: 'badge accent' }, 'leader') : h('span', { class: 'badge' }, 'follower'),
          m.id === c.node_id ? h('span', { class: 'muted' }, '  (connected)') : null))))));

  const healthRows = [
    ['Status', hl.status ? h('span', { class: `badge ${hl.status === 'ok' ? 'ok' : 'warn'}` }, hl.status) : '—'],
    ['Live records', num(hl.live_records)],
    ['Reads', num(hl.total_reads)],
    ['Writes', num(hl.total_writes)],
    ['Unflushed writes', bytes(hl.hot_dirty_bytes)],
    ['Arena segments', num(arena.segments)],
    ['Arena live / dead', arena.live !== undefined ? `${bytes(arena.live)} / ${bytes(arena.dead)}` : '—'],
    ['Compactions', num(arena.compactions)],
  ];
  if (arena.last_error) healthRows.push(['Last arena error', h('span', { class: 'error-text' }, arena.last_error)]);
  const raw = h('details', { class: 'more' }, h('summary', {}, 'Full health report'), highlighted(JSON.stringify(o.health, null, 2)));

  const collections = o.collections.length === 0
    ? h('div', { class: 'empty' }, h('strong', {}, 'No collections yet'), 'A collection is created by its first document.',
      h('div', { style: 'margin-top:12px' }, h('button', { class: 'btn small', type: 'button', onclick: newCollection }, 'New collection')))
    : h('div', { class: 'table-wrap' }, h('table', {},
      h('thead', {}, h('tr', {}, h('th', {}, 'Collection'), h('th', { class: 'num' }, 'Documents'), h('th', {}, 'Indexes'))),
      h('tbody', {}, o.collections.map((x) => h('tr', { class: 'clickable', onclick: () => go(collectionHash(x.name)) },
        h('td', {}, h('span', { class: 'cell key' }, x.name)),
        h('td', { class: 'num' }, num(x.count)),
        h('td', {}, x.indexes.length ? h('div', { class: 'chips' }, x.indexes.map((i) => h('span', { class: 'badge' }, i))) : h('span', { class: 'muted' }, 'none')))))));

  main.replaceChildren(
    h('div', { class: 'page-head' },
      h('div', {}, h('h1', {}, 'Overview'), h('div', { class: 'sub mono' }, o.url)),
      h('button', { class: 'btn ghost small', type: 'button', onclick: renderOverview }, 'Refresh')),
    h('div', { class: 'stats' },
      stat('Collections', num(o.collections.length)),
      stat('Documents', num(docs)),
      stat('Role', standalone ? 'Standalone' : c.is_leader ? 'Leader' : 'Follower', standalone ? null : `node ${c.node_id}`),
      stat('Term', standalone ? '—' : num(c.term)),
      stat('Applied', num(c.last_applied), `commit ${num(c.commit_index)}`)),
    h('div', { class: 'grid-2' },
      h('div', { class: 'stack' },
        h('div', { class: 'card' }, h('div', { class: 'card-head' }, h('h2', {}, 'Collections')), collections),
        h('div', { class: 'card' }, h('div', { class: 'card-head' }, h('h2', {}, 'Cluster'),
          c.leader_addr && !c.is_leader ? h('span', { class: 'muted mono' }, `leader ${c.leader_addr}`) : null), members)),
      h('div', { class: 'card' }, h('div', { class: 'card-head' }, h('h2', {}, 'Health of the connected node')),
        h('div', { class: 'card-body stack' }, h('dl', { class: 'kv' }, healthRows.map(([k, v]) => [h('dt', {}, k), h('dd', {}, v)])), raw))));
  state.refreshTimer = setInterval(() => {
    if (document.visibilityState === 'visible' && route().page === 'overview' && $('drawer').hidden) renderOverviewQuiet();
  }, 10000);
}

async function renderOverviewQuiet() {
  try { await renderOverview(); } catch { /* shown on the next manual refresh */ }
}

function errorPanel(e, retry) {
  return h('div', { class: 'card' }, h('div', { class: 'empty' },
    h('strong', {}, e.status === 503 ? 'The server is not reachable' : 'Something went wrong'),
    h('div', { class: 'error-text' }, e.message),
    retry ? h('div', { style: 'margin-top:14px' }, h('button', { class: 'btn ghost small', type: 'button', onclick: retry }, 'Try again')) : null));
}

function newCollection() {
  const name = prompt('Name of the new collection. It is created with its first document.');
  if (!name || !name.trim()) return;
  const n = name.trim();
  if (!state.collections.some((c) => c.name === n)) state.collections.push({ name: n, count: 0, indexes: [] });
  location.hash = collectionHash(n);
  setTimeout(() => openNewDocument(n), 50);
}

// ---------------------------------------------------------------------------------------------
// Collection
// ---------------------------------------------------------------------------------------------

function viewOf(name) {
  if (!state.views.has(name)) {
    state.views.set(name, {
      conds: [], keyPrefix: '', sort: '', desc: false, limit: 50, cursors: [null], page: 0, result: null, selected: null,
      agg: { conds: [], groupBy: '', aggs: [{ fn: 'count', field: '', as: '' }], order: '', result: null },
    });
  }
  return state.views.get(name);
}

async function renderCollection(name, tab) {
  const main = $('main');
  const tabs = [['documents', 'Documents'], ['analytics', 'Analytics'], ['indexes', 'Indexes'], ['settings', 'Settings']];
  const known = state.collections.find((c) => c.name === name);
  const countEl = h('span', { class: 'sub' }, known && typeof known.count === 'number' ? `${num(known.count)} documents` : '');
  const chips = h('div', { class: 'chips' });
  const body = h('div', {});
  main.replaceChildren(
    h('div', { class: 'page-head' },
      h('div', {}, h('div', { class: 'crumbs' }, h('a', { href: '#/' }, 'Overview'), ' / collection'), h('h1', { class: 'title-mono' }, name), countEl),
      chips),
    h('div', { class: 'tabs', role: 'tablist' }, tabs.map(([id, label]) => h('button', {
      class: `tab${tab === id ? ' active' : ''}`, type: 'button', role: 'tab', 'aria-selected': tab === id ? 'true' : 'false',
      onclick: () => go(collectionHash(name, id)),
    }, label))),
    body);
  let info = null;
  try {
    info = await api('GET', `collections/${enc(name)}`);
    countEl.textContent = `${num(info.count)} document${info.count === 1 ? '' : 's'}`;
    chips.replaceChildren(...info.indexes.map((i) => h('span', { class: 'badge', title: 'secondary index' }, i)),
      ...info.vector_indexes.map((v) => h('span', { class: 'badge accent', title: 'vector index' }, `${v.field} · ${v.dims}d`)));
    const k = state.collections.find((c) => c.name === name);
    if (k) k.count = info.count;
    renderNav();
  } catch (e) {
    if (e.status !== 404) toast(e.message, 'error');
  }
  if (tab === 'analytics') return renderAnalytics(name, body);
  if (tab === 'indexes') return renderIndexes(name, body, info);
  if (tab === 'settings') return renderSettings(name, body, info);
  return renderDocuments(name, body);
}

// A list of conditions (all must hold) edited in place.
function conditionEditor(conds, onRun) {
  const wrap = h('div', { class: 'stack', style: 'gap:8px' });
  const draw = () => {
    wrap.replaceChildren(...conds.map((c, i) => {
      const path = h('input', { type: 'text', value: c.path, placeholder: 'field, e.g. status or address.city', spellcheck: 'false', 'aria-label': 'Field',
        oninput: (e) => { c.path = e.target.value; }, onkeydown: enter(onRun) });
      const op = h('select', { 'aria-label': 'Comparison', onchange: (e) => { c.op = e.target.value; draw(); } },
        OPS.map(([v, l]) => h('option', { value: v, selected: c.op === v }, l)));
      const noValue = c.op === 'exists' || c.op === 'missing';
      const placeholder = c.op === 'between' ? 'low .. high' : c.op === 'in' ? 'a, b, c' : c.op === 'prefix' ? 'text' : 'value: 42, true, "text", {"$decimal":"9.99"}';
      const value = h('input', { type: 'text', value: c.value, placeholder, spellcheck: 'false', disabled: noValue, 'data-role': 'value', 'aria-label': 'Value',
        oninput: (e) => { c.value = e.target.value; }, onkeydown: enter(onRun) });
      const del = h('button', { class: 'icon-btn', type: 'button', title: 'Remove condition', 'aria-label': 'Remove condition', onclick: () => { conds.splice(i, 1); draw(); } }, '×');
      return h('div', { class: 'cond' }, i > 0 ? h('div', { class: 'joiner' }, 'and') : null, path, op, value, del);
    }));
  };
  draw();
  return { el: wrap, redraw: draw };
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

function renderDocuments(name, body) {
  const v = viewOf(name);
  const resultsHost = h('div', {});
  const run = (reset) => {
    if (reset !== false) { v.cursors = [null]; v.page = 0; }
    runQuery(name, v, resultsHost);
  };
  const cond = conditionEditor(v.conds, run);
  const keyPrefix = h('input', { type: 'text', value: v.keyPrefix, placeholder: 'key starts with…', spellcheck: 'false', 'aria-label': 'Key prefix', style: 'width:180px',
    oninput: (e) => { v.keyPrefix = e.target.value; }, onkeydown: enter(run) });
  const sort = h('input', { type: 'text', value: v.sort, placeholder: 'sort by field (key order if empty)', spellcheck: 'false', 'aria-label': 'Sort by', style: 'width:220px',
    oninput: (e) => { v.sort = e.target.value; }, onkeydown: enter(run) });
  const dir = h('select', { 'aria-label': 'Sort direction', onchange: (e) => { v.desc = e.target.value === 'desc'; } },
    h('option', { value: 'asc', selected: !v.desc }, 'ascending'), h('option', { value: 'desc', selected: v.desc }, 'descending'));
  const size = h('select', { 'aria-label': 'Page size', onchange: (e) => { v.limit = Number(e.target.value); run(); } },
    PAGE_SIZES.map((n) => h('option', { value: n, selected: v.limit === n }, `${n} per page`)));
  body.replaceChildren(h('div', { class: 'card' },
    h('div', { class: 'builder' },
      cond.el,
      h('div', { class: 'builder-foot' },
        h('button', { class: 'btn ghost small', type: 'button', onclick: () => { v.conds.push({ path: '', op: 'eq', value: '' }); cond.redraw(); } }, '+ Condition'),
        keyPrefix, sort, dir, size,
        h('span', { class: 'spacer' }),
        h('button', { class: 'btn ghost small', type: 'button', onclick: () => openNewDocument(name) }, 'New document'),
        h('button', { class: 'btn small', type: 'button', onclick: () => run() }, 'Run'))),
    resultsHost));
  if (v.result) drawResults(name, v, resultsHost);
  run(false);
}

async function runQuery(name, v, host) {
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
    req.offset = v.page * v.limit;
  } else {
    req.after = v.cursors[v.page];
  }
  if (!v.result) host.replaceChildren(spinner('Running the query…'));
  try {
    v.result = await api('POST', `collections/${enc(name)}/query`, req);
    v.sorted = sorted;
    drawResults(name, v, host);
  } catch (e) {
    host.replaceChildren(h('div', { class: 'card-body' }, h('div', { class: 'error-text' }, e.message)));
  }
}

function columnsOf(docs) {
  const seen = new Map();
  for (const d of docs) for (const k of Object.keys(d.doc)) seen.set(k, (seen.get(k) || 0) + 1);
  return [...seen.entries()].sort((a, b) => b[1] - a[1]).slice(0, 8).map(([k]) => k);
}

function drawResults(name, v, host) {
  const r = v.result;
  if (!r) return;
  if (r.docs.length === 0) {
    host.replaceChildren(h('div', { class: 'empty' },
      h('strong', {}, r.total === 0 && v.conds.length === 0 && !v.keyPrefix ? 'This collection is empty' : 'No documents match'),
      r.total === 0 && v.conds.length === 0 && !v.keyPrefix ? 'Add the first document to create it.' : 'Change the conditions and run again.',
      h('div', { style: 'margin-top:12px' }, h('button', { class: 'btn small', type: 'button', onclick: () => openNewDocument(name) }, 'New document'))));
    return;
  }
  const cols = columnsOf(r.docs);
  const first = v.page * v.limit + 1;
  const last = v.page * v.limit + r.docs.length;
  const hasNext = v.sorted ? last < r.total : !!r.next;
  const prev = h('button', { class: 'btn ghost small', type: 'button', disabled: v.page === 0, onclick: () => { v.page--; runQuery(name, v, host); } }, '← Previous');
  const next = h('button', { class: 'btn ghost small', type: 'button', disabled: !hasNext, onclick: () => {
    if (!v.sorted) v.cursors[v.page + 1] = r.next;
    v.page++;
    runQuery(name, v, host);
  } }, 'Next →');
  host.replaceChildren(
    h('div', { class: 'table-wrap' }, h('table', {},
      h('thead', {}, h('tr', {}, h('th', {}, 'Key'), cols.map((c) => h('th', {}, c)), h('th', { class: 'num' }, 'Version'))),
      h('tbody', {}, r.docs.map((d) => h('tr', {
        class: `clickable${v.selected === d.key ? ' selected' : ''}`, tabindex: '0',
        onclick: () => openDocument(name, d.key),
        onkeydown: enter(() => openDocument(name, d.key)),
      },
      h('td', {}, h('span', { class: 'cell key', title: d.key }, d.key)),
      cols.map((c) => h('td', {}, h('span', { class: 'cell' }, cell(d.doc[c])))),
      h('td', { class: 'num muted' }, String(d.version))))))),
    h('div', { class: 'results-foot' },
      h('span', {}, `${num(first)}–${num(last)} of ${num(r.total)}`),
      h('div', { class: 'row' }, prev, next)));
}

// ---------------------------------------------------------------------------------------------
// Document drawer
// ---------------------------------------------------------------------------------------------

function closeDrawer() {
  const d = $('drawer');
  d.hidden = true;
  d.replaceChildren();
  for (const v of state.views.values()) v.selected = null;
  for (const tr of document.querySelectorAll('tr.selected')) tr.classList.remove('selected');
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

async function openDocument(name, key) {
  const v = viewOf(name);
  v.selected = key;
  for (const tr of document.querySelectorAll('tbody tr')) {
    tr.classList.toggle('selected', tr.querySelector('.cell.key') && tr.querySelector('.cell.key').textContent === key);
  }
  const d = $('drawer');
  d.hidden = false;
  d.replaceChildren(h('div', { class: 'drawer-body' }, spinner()));
  let doc;
  try {
    doc = await api('GET', `collections/${enc(name)}/docs/${enc(key)}`);
  } catch (e) {
    d.replaceChildren(drawerHead(name, key, null), h('div', { class: 'drawer-body' }, h('div', { class: 'error-text' }, e.message)));
    return;
  }
  drawDocument(name, doc);
}

function drawerHead(name, key, version) {
  return h('div', { class: 'drawer-head' },
    h('div', { class: 'title' }, h('span', { class: 'muted', style: 'font-size:12px' }, name), h('span', { class: 'mono', title: key }, key)),
    version !== null ? h('span', { class: 'badge' }, `v${version}`) : null,
    h('button', { class: 'icon-btn', type: 'button', title: 'Close (Esc)', 'aria-label': 'Close', onclick: closeDrawer }, '×'));
}

function drawDocument(name, doc) {
  const d = $('drawer');
  const ed = editorArea(doc.text);
  const err = h('div', { class: 'error-text', role: 'alert' });
  const save = h('button', { class: 'btn', type: 'button' }, 'Save');
  const doSave = async (mode) => {
    if (!ed.check()) return;
    err.replaceChildren();
    save.disabled = true;
    try {
      const q = mode === 'put' ? 'mode=put' : `mode=replace&version=${doc.version}`;
      const saved = await api('PUT', `collections/${enc(name)}/docs/${enc(doc.key)}?${q}`, ed.ta.value, { raw: true });
      toast(`Saved ${doc.key} (version ${saved.version})`);
      drawDocument(name, saved);
      refreshResults(name);
    } catch (e) {
      if (e.status === 409) {
        err.append('This document changed since you opened it. ',
          h('a', { href: '#', onclick: (ev) => { ev.preventDefault(); openDocument(name, doc.key); } }, 'Load the new version'), ' or ',
          h('a', { href: '#', onclick: (ev) => { ev.preventDefault(); doSave('put'); } }, 'overwrite it'), '.');
      } else {
        err.textContent = e.message;
      }
    } finally {
      save.disabled = false;
    }
  };
  save.addEventListener('click', () => doSave('replace'));
  ed.ta.addEventListener('keydown', (e) => { if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) { e.preventDefault(); doSave('replace'); } });
  const del = h('button', { class: 'btn danger ghost', type: 'button', onclick: async () => {
    if (!confirm(`Delete "${doc.key}" from ${name}? This cannot be undone.`)) return;
    try {
      await api('DELETE', `collections/${enc(name)}/docs/${enc(doc.key)}?version=${doc.version}`);
      toast(`Deleted ${doc.key}`);
      closeDrawer();
      refreshResults(name);
    } catch (e) {
      err.textContent = e.status === 409 ? 'This document changed since you opened it: reload it before deleting.' : e.message;
    }
  } }, 'Delete');
  const copy = h('button', { class: 'btn ghost', type: 'button', onclick: async () => {
    try { await navigator.clipboard.writeText(ed.ta.value); toast('Copied'); } catch { toast('The browser did not allow copying', 'error'); }
  } }, 'Copy');
  d.replaceChildren(
    drawerHead(name, doc.key, doc.version),
    h('div', { class: 'drawer-body' }, ed.ta, ed.status, h('div', { class: 'help' }, TYPE_HELP), err),
    h('div', { class: 'drawer-foot' }, del, h('span', { class: 'spacer' }), h('span', { class: 'help' }, '⌘/Ctrl + Enter'), copy, save));
  ed.ta.focus();
}

function openNewDocument(name) {
  const d = $('drawer');
  d.hidden = false;
  const key = h('input', { type: 'text', placeholder: 'e.g. user:1001', spellcheck: 'false', autocomplete: 'off', 'aria-label': 'Key' });
  const ed = editorArea('{\n  \n}');
  const err = h('div', { class: 'error-text', role: 'alert' });
  const create = h('button', { class: 'btn', type: 'button' }, 'Create');
  const doCreate = async () => {
    if (!key.value.trim()) { err.textContent = 'Give the document a key.'; key.focus(); return; }
    if (!ed.check()) return;
    create.disabled = true;
    err.textContent = '';
    try {
      const saved = await api('PUT', `collections/${enc(name)}/docs/${enc(key.value.trim())}?mode=create`, ed.ta.value, { raw: true });
      toast(`Created ${saved.key}`);
      if (!state.collections.some((c) => c.name === name)) state.collections.push({ name, count: 1, indexes: [] });
      drawDocument(name, saved);
      refreshResults(name);
      loadOverview().catch(() => {});
    } catch (e) {
      err.textContent = e.status === 409 ? `A document "${key.value.trim()}" already exists.` : e.message;
    } finally {
      create.disabled = false;
    }
  };
  create.addEventListener('click', doCreate);
  ed.ta.addEventListener('keydown', (e) => { if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) { e.preventDefault(); doCreate(); } });
  key.addEventListener('keydown', enter(() => ed.ta.focus()));
  d.replaceChildren(
    h('div', { class: 'drawer-head' }, h('div', { class: 'title' }, h('span', { class: 'muted', style: 'font-size:12px' }, name), h('span', { class: 'mono' }, 'New document')),
      h('button', { class: 'icon-btn', type: 'button', title: 'Close (Esc)', 'aria-label': 'Close', onclick: closeDrawer }, '×')),
    h('div', { class: 'drawer-body' }, h('label', { class: 'field' }, 'Key', key), h('label', { class: 'field' }, 'Fields', ed.ta), ed.status, h('div', { class: 'help' }, TYPE_HELP), err),
    h('div', { class: 'drawer-foot' }, h('span', { class: 'spacer' }), create));
  key.focus();
  ed.ta.setSelectionRange(4, 4);
}

function refreshResults(name) {
  const r = route();
  if (r.page === 'collection' && r.name === name && r.tab === 'documents') {
    const host = document.querySelector('.builder') && document.querySelector('.builder').nextElementSibling;
    if (host) runQuery(name, viewOf(name), host);
  }
}

// ---------------------------------------------------------------------------------------------
// Analytics
// ---------------------------------------------------------------------------------------------

function renderAnalytics(name, body) {
  const a = viewOf(name).agg;
  const out = h('div', {});
  const run = async () => {
    let filter;
    try { filter = buildFilter(a.conds, ''); } catch (e) { toast(e.message, 'error'); return; }
    const groupBy = a.groupBy.split(',').map((s) => s.trim()).filter(Boolean);
    const req = { filter, group_by: groupBy, aggs: a.aggs.map((x) => ({ fn: x.fn, field: x.field.trim(), as: x.as.trim() })) };
    if (a.order.trim()) req.order_by = a.order.split(',').map((s) => s.trim()).filter(Boolean);
    out.replaceChildren(spinner('Aggregating…'));
    try {
      a.result = await api('POST', `collections/${enc(name)}/aggregate`, req);
      drawAgg(a, out);
    } catch (e) {
      out.replaceChildren(h('div', { class: 'card-body' }, h('div', { class: 'error-text' }, e.message)));
    }
  };
  const cond = conditionEditor(a.conds, run);
  const aggList = h('div', { class: 'stack', style: 'gap:8px' });
  const drawAggs = () => {
    aggList.replaceChildren(...a.aggs.map((x, i) => h('div', { class: 'agg-row' },
      h('select', { 'aria-label': 'Aggregate', onchange: (e) => { x.fn = e.target.value; } }, AGGS.map(([v, l]) => h('option', { value: v, selected: x.fn === v }, l))),
      h('input', { type: 'text', value: x.field, placeholder: x.fn === 'count' ? 'field (optional)' : 'field', spellcheck: 'false', 'aria-label': 'Field', oninput: (e) => { x.field = e.target.value; }, onkeydown: enter(run) }),
      h('input', { type: 'text', value: x.as, placeholder: 'column name (optional)', spellcheck: 'false', 'aria-label': 'Column name', oninput: (e) => { x.as = e.target.value; }, onkeydown: enter(run) }),
      h('button', { class: 'icon-btn', type: 'button', title: 'Remove', 'aria-label': 'Remove aggregate', disabled: a.aggs.length === 1, onclick: () => { a.aggs.splice(i, 1); drawAggs(); } }, '×'))));
  };
  drawAggs();
  const groupBy = h('input', { type: 'text', value: a.groupBy, placeholder: 'group by fields, comma separated (none: one row)', spellcheck: 'false', class: 'grow', 'aria-label': 'Group by',
    oninput: (e) => { a.groupBy = e.target.value; }, onkeydown: enter(run) });
  const order = h('input', { type: 'text', value: a.order, placeholder: 'order by columns, -column for descending', spellcheck: 'false', class: 'grow', 'aria-label': 'Order by',
    oninput: (e) => { a.order = e.target.value; }, onkeydown: enter(run) });
  body.replaceChildren(h('div', { class: 'card' },
    h('div', { class: 'builder' },
      h('h3', {}, 'Where'), cond.el,
      h('div', {}, h('button', { class: 'btn ghost small', type: 'button', onclick: () => { a.conds.push({ path: '', op: 'eq', value: '' }); cond.redraw(); } }, '+ Condition')),
      h('h3', {}, 'Group by'), h('div', { class: 'row' }, groupBy),
      h('h3', {}, 'Compute'), aggList,
      h('div', { class: 'builder-foot' },
        h('button', { class: 'btn ghost small', type: 'button', onclick: () => { a.aggs.push({ fn: 'sum', field: '', as: '' }); drawAggs(); } }, '+ Aggregate'),
        order, h('span', { class: 'spacer' }), h('button', { class: 'btn small', type: 'button', onclick: run }, 'Run'))),
    out));
  if (a.result) drawAgg(a, out);
  else out.replaceChildren(h('div', { class: 'empty' }, h('strong', {}, 'Exact aggregates on the server'), 'Sums and averages of integers and decimals are exact. Enable columnar storage for large collections.'));
}

function drawAgg(a, out) {
  const r = a.result;
  const numericCol = r.columns.map((_, i) => r.rows.length > 0 && r.rows.every((row) => typeof row[i] === 'number' || (tagOf(row[i]) && ['$int', '$decimal', '$float'].includes(tagOf(row[i]))) || row[i] === null));
  out.replaceChildren(
    r.rows.length === 0 ? h('div', { class: 'empty' }, h('strong', {}, 'No groups'), 'No document matched.') :
      h('div', { class: 'table-wrap' }, h('table', {},
        h('thead', {}, h('tr', {}, r.columns.map((c, i) => h('th', { class: numericCol[i] ? 'num' : null }, c)))),
        h('tbody', {}, r.rows.map((row) => h('tr', {}, row.map((x, i) => h('td', { class: numericCol[i] ? 'num' : null }, cell(x)))))))),
    h('div', { class: 'results-foot' },
      h('span', {}, `${num(r.rows.length)} row${r.rows.length === 1 ? '' : 's'} · ${num(r.matched)} documents matched`),
      h('span', {}, r.pruned_row_groups ? `${num(r.pruned_row_groups)} row groups skipped` : '', r.spilled_bytes ? ` · spilled ${bytes(r.spilled_bytes)}` : '')));
}

// ---------------------------------------------------------------------------------------------
// Indexes and settings
// ---------------------------------------------------------------------------------------------

function renderIndexes(name, body, info) {
  if (!info) { body.replaceChildren(h('div', { class: 'card' }, h('div', { class: 'empty' }, h('strong', {}, 'No such collection yet'), 'Create a document first.'))); return; }
  const field = h('input', { type: 'text', placeholder: 'field, e.g. email', spellcheck: 'false', 'aria-label': 'Field to index', class: 'grow' });
  const kind = h('select', { 'aria-label': 'Index kind' }, h('option', { value: 'secondary' }, 'Secondary index'), h('option', { value: 'vector' }, 'Vector index'));
  const dims = h('input', { type: 'number', min: 1, max: 4096, placeholder: 'dimensions', 'aria-label': 'Dimensions', style: 'width:130px', hidden: true });
  kind.addEventListener('change', () => { dims.hidden = kind.value !== 'vector'; });
  const add = h('button', { class: 'btn small', type: 'button' }, 'Create index');
  add.addEventListener('click', async () => {
    if (!field.value.trim()) { field.focus(); return; }
    const req = { field: field.value.trim() };
    if (kind.value === 'vector') {
      if (!(Number(dims.value) > 0)) { dims.focus(); return; }
      req.dims = Number(dims.value);
    }
    add.disabled = true;
    add.textContent = 'Building…';
    try {
      const next = await api('POST', `collections/${enc(name)}/indexes`, req);
      toast(`Indexed ${req.field}`);
      renderCollection(name, 'indexes').then(() => {}, () => {});
      return next;
    } catch (e) {
      toast(e.message, 'error');
    } finally {
      add.disabled = false;
      add.textContent = 'Create index';
    }
  });
  const drop = async (f, vector) => {
    if (!confirm(`Drop the ${vector ? 'vector ' : ''}index on ${f}? Queries on it will scan instead.`)) return;
    try {
      await api('DELETE', `collections/${enc(name)}/indexes/${enc(f)}${vector ? '?vector=1' : ''}`);
      toast(`Dropped the index on ${f}`);
      renderCollection(name, 'indexes');
    } catch (e) { toast(e.message, 'error'); }
  };
  const rows = [
    ...info.indexes.map((f) => [f, 'secondary', h('span', { class: 'badge ok' }, 'ready'), false]),
    ...info.building.map((f) => [f, 'secondary', h('span', { class: 'badge warn' }, 'building'), false]),
    ...info.vector_indexes.map((x) => [x.field, `vector · ${x.dims} dimensions`, h('span', { class: `badge ${x.ready ? 'ok' : 'warn'}` }, x.ready ? 'ready' : 'building'), true]),
  ];
  body.replaceChildren(h('div', { class: 'stack' },
    h('div', { class: 'card' }, h('div', { class: 'card-head' }, h('h2', {}, 'Indexes'), h('span', { class: 'muted' }, 'Up to 16 secondary and 4 vector indexes, each on one top-level field.')),
      rows.length === 0 ? h('div', { class: 'empty' }, h('strong', {}, 'No indexes'), 'Queries scan the collection. Index the fields you filter and sort on.') :
        h('div', { class: 'table-wrap' }, h('table', {},
          h('thead', {}, h('tr', {}, h('th', {}, 'Field'), h('th', {}, 'Kind'), h('th', {}, 'State'), h('th', {}, ''))),
          h('tbody', {}, rows.map(([f, k, s, vec]) => h('tr', {}, h('td', { class: 'mono' }, f), h('td', {}, k), h('td', {}, s),
            h('td', { class: 'num' }, h('button', { class: 'btn danger ghost small', type: 'button', onclick: () => drop(f, vec) }, 'Drop')))))))),
    h('div', { class: 'card' }, h('div', { class: 'card-head' }, h('h2', {}, 'New index')),
      h('div', { class: 'card-body' }, h('div', { class: 'row' }, field, kind, dims, add),
        h('p', { class: 'help' }, 'Building reads every existing document, then the index stays current with each write.')))));
}

function renderSettings(name, body, info) {
  const confirmBox = h('input', { type: 'text', placeholder: name, spellcheck: 'false', autocomplete: 'off', 'aria-label': 'Type the collection name to confirm' });
  const dropBtn = h('button', { class: 'btn danger', type: 'button', disabled: true }, 'Drop collection');
  confirmBox.addEventListener('input', () => { dropBtn.disabled = confirmBox.value !== name; });
  dropBtn.addEventListener('click', async () => {
    dropBtn.disabled = true;
    try {
      const r = await api('DELETE', `collections/${enc(name)}`);
      toast(`Dropped ${name}: ${num(r.deleted)} documents deleted`);
      state.views.delete(name);
      await loadOverview();
      go('#/');
    } catch (e) {
      toast(e.message, 'error');
      dropBtn.disabled = false;
    }
  });
  body.replaceChildren(h('div', { class: 'stack' },
    h('div', { class: 'card' }, h('div', { class: 'card-head' }, h('h2', {}, 'Metadata')),
      h('div', { class: 'card-body' }, info ? highlighted(JSON.stringify(info.metadata, null, 2)) : h('span', { class: 'muted' }, 'None.'))),
    h('div', { class: 'card danger-zone' }, h('div', { class: 'card-head' }, h('h2', {}, 'Drop this collection')),
      h('div', { class: 'card-body stack', style: 'gap:10px' },
        h('p', { style: 'margin:0' }, 'Deletes every document and index in ', h('strong', { class: 'mono' }, name), '. This cannot be undone: back up first (storage-cli remote-backup).'),
        h('label', { class: 'field' }, 'Type the collection name to confirm', confirmBox),
        h('div', {}, dropBtn)))));
}

// ---------------------------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------------------------

async function start() {
  applyTheme(store.get(THEME_STORE));
  const m = location.hash.match(/^#key=([A-Za-z0-9_-]+)/);
  if (m) {
    store.set(KEY_STORE, m[1]);
    history.replaceState(null, '', `${location.pathname}#/`);
  }
  state.key = store.get(KEY_STORE);
  $('theme').addEventListener('click', () => {
    const next = currentTheme() === 'dark' ? 'light' : 'dark';
    applyTheme(next);
    store.set(THEME_STORE, next);
  });
  $('disconnect').addEventListener('click', async () => {
    try { state.session = await api('POST', 'disconnect'); } catch (e) { toast(e.message, 'error'); return; }
    state.overview = null;
    state.collections = [];
    state.views.clear();
    closeDrawer();
    go('#/');
  });
  $('new-collection').addEventListener('click', newCollection);
  const setNav = (open) => {
    document.querySelector('.layout').classList.toggle('nav-open', open);
    $('menu').setAttribute('aria-expanded', String(open));
  };
  $('menu').addEventListener('click', () => setNav(!document.querySelector('.layout').classList.contains('nav-open')));
  $('sidebar').addEventListener('click', (e) => { if (e.target.closest('a')) setNav(false); });
  $('nav-filter').addEventListener('input', renderNav);
  document.addEventListener('keydown', (e) => { if (e.key === 'Escape' && !$('drawer').hidden) closeDrawer(); });
  window.addEventListener('hashchange', () => { closeDrawer(); render(); });
  if (!state.key) { showLocked(); return; }
  try {
    state.session = await api('GET', 'session');
  } catch (e) {
    if (e.status !== 401) $('main').replaceChildren(errorPanel(e, () => location.reload()));
    return;
  }
  if (state.session.connected) {
    try { await loadOverview(); } catch (e) { toast(e.message, 'error'); }
  }
  updateShell();
  render();
}

start();
