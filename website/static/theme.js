// Applies the saved light or dark choice before the page paints.
try {
  var t = localStorage.getItem("storine-theme");
  if (t === "light" || t === "dark") document.documentElement.setAttribute("data-theme", t);
} catch (e) {}
