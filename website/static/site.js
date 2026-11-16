// Storine website: theme, navigation, copy buttons, language tabs, the table of contents and
// search. No libraries; everything is served from this site.
(function () {
  "use strict";
  var root = document.documentElement;
  function store(k, v) { try { if (v === undefined) return localStorage.getItem(k); localStorage.setItem(k, v); } catch (e) { return null; } }

  // Theme
  function dark() {
    var t = root.getAttribute("data-theme");
    return t ? t === "dark" : window.matchMedia("(prefers-color-scheme: dark)").matches;
  }
  document.addEventListener("click", function (e) {
    var b = e.target.closest("[data-action]");
    if (!b) return;
    var a = b.getAttribute("data-action");
    if (a === "theme") {
      var next = dark() ? "light" : "dark";
      root.setAttribute("data-theme", next);
      store("storine-theme", next);
    } else if (a === "menu") {
      document.body.classList.toggle("nav-open");
    } else if (a === "search") {
      openSearch();
    } else if (a === "close-search") {
      closeSearch();
    }
  });

  // Keep the current page visible in the sidebar.
  var cur = document.querySelector('.sidebar a[aria-current="page"]');
  if (cur) cur.scrollIntoView({ block: "center" });

  // Copy buttons
  var COPY = '<svg viewBox="0 0 24 24" width="16" height="16" aria-hidden="true"><rect x="8" y="8" width="12" height="12" rx="2" fill="none" stroke="currentColor" stroke-width="2"/><path d="M16 8V6a2 2 0 0 0-2-2H6a2 2 0 0 0-2 2v8a2 2 0 0 0 2 2h2" fill="none" stroke="currentColor" stroke-width="2"/></svg>';
  var DONE = '<svg viewBox="0 0 24 24" width="16" height="16" aria-hidden="true"><path d="m5 12 5 5 9-10" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"/></svg>';
  function addCopy(box, getText) {
    var b = document.createElement("button");
    b.type = "button";
    b.className = "copy";
    b.setAttribute("aria-label", "Copy");
    b.innerHTML = COPY;
    b.addEventListener("click", function () {
      var text = getText();
      var ok = function () {
        b.innerHTML = DONE;
        b.classList.add("done");
        setTimeout(function () { b.innerHTML = COPY; b.classList.remove("done"); }, 1400);
      };
      if (navigator.clipboard) navigator.clipboard.writeText(text).then(ok, function () {});
    });
    box.appendChild(b);
  }
  document.querySelectorAll(".code").forEach(function (box) {
    addCopy(box, function () { return box.querySelector("code").innerText.replace(/\n$/, ""); });
  });
  document.querySelectorAll(".install").forEach(function (box) {
    addCopy(box, function () { return box.querySelector("code:not([hidden])").innerText; });
  });

  // Tabs: choosing a language switches every tab group with that language, and is remembered.
  function select(group, name) {
    var found = false;
    group.querySelectorAll("[role=tab]").forEach(function (t) { if (t.getAttribute("data-tab") === name) found = true; });
    if (!found) return;
    group.querySelectorAll("[role=tab]").forEach(function (t) { t.setAttribute("aria-selected", String(t.getAttribute("data-tab") === name)); });
    group.querySelectorAll("[role=tabpanel],[data-panel]").forEach(function (p) { p.hidden = (p.getAttribute("data-tab") || p.getAttribute("data-panel")) !== name; });
  }
  var groups = document.querySelectorAll(".tabs, .install-group");
  var saved = store("storine-tab");
  groups.forEach(function (g) {
    if (saved) select(g, saved);
    g.addEventListener("click", function (e) {
      var t = e.target.closest("[role=tab]");
      if (!t) return;
      var name = t.getAttribute("data-tab");
      var y = t.getBoundingClientRect().top;
      groups.forEach(function (o) { select(o, name); });
      window.scrollBy(0, t.getBoundingClientRect().top - y);
      store("storine-tab", name);
    });
  });
  if (!saved && /Win/.test(navigator.platform || "")) document.querySelectorAll(".install-group").forEach(function (g) { select(g, "Windows"); });

  // Table of contents: highlight the section being read.
  var tocLinks = Array.prototype.slice.call(document.querySelectorAll(".toc a"));
  if (tocLinks.length && "IntersectionObserver" in window) {
    var heads = tocLinks.map(function (a) { return document.getElementById(decodeURIComponent(a.hash.slice(1))); }).filter(Boolean);
    var visible = new Set();
    var io = new IntersectionObserver(function (entries) {
      entries.forEach(function (en) { if (en.isIntersecting) visible.add(en.target); else visible.delete(en.target); });
      var top = heads.filter(function (h) { return h.getBoundingClientRect().top < window.innerHeight * 0.35; }).pop() || heads[0];
      tocLinks.forEach(function (a) { a.classList.toggle("active", a.hash.slice(1) === top.id); });
    }, { rootMargin: "-60px 0px -60% 0px" });
    heads.forEach(function (h) { io.observe(h); });
  }

  // Search
  var box = document.querySelector(".search");
  var input = box && box.querySelector("input");
  var list = box && box.querySelector(".search-results");
  var empty = box && box.querySelector(".search-empty");
  var index = null, loading = null, results = [], sel = 0, lastFocus = null;
  function load() {
    if (index || loading) return loading;
    loading = fetch(box.getAttribute("data-index")).then(function (r) { return r.json(); }).then(function (d) {
      index = d.map(function (e) {
        return { e: e, t: (e.p + " " + e.h).toLowerCase(), x: e.x.toLowerCase() };
      });
      run();
    });
    return loading;
  }
  function openSearch() {
    if (!box) return;
    lastFocus = document.activeElement;
    box.hidden = false;
    document.body.style.overflow = "hidden";
    input.select();
    input.focus();
    load();
  }
  function closeSearch() {
    if (!box || box.hidden) return;
    box.hidden = true;
    document.body.style.overflow = "";
    if (lastFocus) lastFocus.focus();
  }
  function esc(s) { return s.replace(/[&<>"]/g, function (c) { return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]; }); }
  function mark(text, terms) {
    var out = esc(text);
    terms.forEach(function (t) {
      if (t.length < 2) return;
      out = out.replace(new RegExp("(" + t.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") + ")", "gi"), "<mark>$1</mark>");
    });
    return out;
  }
  function snippet(x, terms) {
    var low = x.toLowerCase(), at = -1;
    for (var i = 0; i < terms.length && at < 0; i++) at = low.indexOf(terms[i]);
    if (at < 0) return x.slice(0, 140);
    var s = Math.max(0, at - 50);
    return (s ? "… " : "") + x.slice(s, s + 160);
  }
  function run() {
    if (!index) return;
    var q = input.value.trim().toLowerCase();
    var terms = q.split(/\s+/).filter(Boolean);
    var long = terms.filter(function (t) { return t.length > 2; });
    if (long.length) terms = long;
    results = [];
    if (terms.length) {
      index.forEach(function (it) {
        var score = 0;
        for (var i = 0; i < terms.length; i++) {
          var t = terms[i];
          var inT = it.t.indexOf(t), inX = it.x.indexOf(t);
          if (inT < 0 && inX < 0) return;
          if (inT >= 0) score += 10 + (it.e.h.toLowerCase().indexOf(t) === 0 ? 6 : 0) + (new RegExp("\\b" + t.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") + "\\b").test(it.t) ? 4 : 0);
          if (inX >= 0) score += 2 + Math.min(4, it.x.split(t).length - 1);
        }
        if (it.t.indexOf(q) >= 0) score += 20;
        if (!it.e.h) score += 3;
        results.push({ s: score, it: it });
      });
      results.sort(function (a, b) { return b.s - a.s; });
      results = results.slice(0, 30);
    }
    sel = 0;
    var base = box.getAttribute("data-root");
    list.innerHTML = results.map(function (r, k) {
      var e = r.it.e;
      return '<li role="option" aria-selected="' + (k === 0) + '"><a href="' + base + e.u + '">' +
        '<span class="r-path">' + esc(e.s + " › " + e.p) + "</span>" +
        '<span class="r-title">' + mark(e.h || e.p, terms) + "</span>" +
        '<span class="r-text">' + mark(snippet(e.x, terms), terms) + "</span></a></li>";
    }).join("");
    empty.hidden = !(terms.length && !results.length);
  }
  function move(d) {
    var items = list.querySelectorAll("li");
    if (!items.length) return;
    items[sel].setAttribute("aria-selected", "false");
    sel = (sel + d + items.length) % items.length;
    items[sel].setAttribute("aria-selected", "true");
    items[sel].scrollIntoView({ block: "nearest" });
  }
  if (box) {
    input.addEventListener("input", run);
    input.addEventListener("keydown", function (e) {
      if (e.key === "ArrowDown") { e.preventDefault(); move(1); }
      else if (e.key === "ArrowUp") { e.preventDefault(); move(-1); }
      else if (e.key === "Enter") {
        var a = list.querySelectorAll("li a")[sel];
        if (a) { closeSearch(); location.href = a.href; }
      }
    });
    list.addEventListener("click", function () { closeSearch(); });
  }
  document.addEventListener("keydown", function (e) {
    var typing = /^(INPUT|TEXTAREA|SELECT)$/.test(document.activeElement.tagName) || document.activeElement.isContentEditable;
    if ((e.key === "k" && (e.metaKey || e.ctrlKey)) || (e.key === "/" && !typing)) {
      e.preventDefault();
      openSearch();
    } else if (e.key === "Escape") {
      closeSearch();
      document.body.classList.remove("nav-open");
    }
  });
})();
