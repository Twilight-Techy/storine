#!/usr/bin/env python3
"""Builds the Storine website from Markdown, with nothing but the Python standard library.

    python3 website/build.py [--out DIR] [--base /storine/] [--check]

Pages come from two places: the guides in `website/content/` and documents that live in the
repository (the protocol, the changelog, each SDK's README, ...), listed in `website/nav.json`,
so the site never drifts from them. Links between Markdown files are rewritten to the pages
they became; links to other repository files go to GitHub. `--check` fails the build on a
broken internal link or anchor, a page missing from the navigation, or anything loaded from
another origin.
"""
import argparse
import html
import json
import os
import re
import shutil
import sys
import posixpath

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SITE = os.path.join(ROOT, "website")
REPO_URL = "https://github.com/Twilight-Techy/storine"
# The canonical address: share previews (Open Graph) need absolute URLs, and the GitHub Pages
# copy points search engines here.
SITE_URL = "https://storine.twilighttechy.dev/"
BLOB = REPO_URL + "/blob/main/"
TREE = REPO_URL + "/tree/main/"


def version():
    m = re.search(r'^version\s*=\s*"([^"]+)"', open(os.path.join(ROOT, "Cargo.toml")).read(), re.M)
    return m.group(1) if m else "0.0.0"


# ---------------------------------------------------------------------------------------------
# Markdown
# ---------------------------------------------------------------------------------------------

def slugify(text):
    """GitHub's heading anchors, so links written for GitHub keep working."""
    text = re.sub(r"<[^>]+>", "", text).strip().lower()
    text = re.sub(r"[^\w\- ]", "", text, flags=re.U)
    return text.replace(" ", "-")


INLINE_TAGS = r"(?:a|img|sub|sup|br|strong|em|b|i|kbd|code|span|small|del)"
PLACE = "\u0000{}\u0000"


class Inline:
    def __init__(self, link):
        self.link = link  # rewrites a URL; returns the new URL

    def render(self, text):
        keep = []

        def stash(s):
            keep.append(s)
            return PLACE.format(len(keep) - 1)

        # Code spans first: nothing inside them is markup.
        def code(m):
            inner = m.group(2).replace("\n", " ")
            return stash("<code>" + html.escape(inner.strip() if inner.strip() else inner) + "</code>")
        text = re.sub(r"(`+)(.+?)(?<!`)\1(?!`)", code, text, flags=re.S)
        # Inline HTML the documents use.
        def tag(m):
            t = m.group(0)
            t = re.sub(r'\b(href|src)="([^"]*)"', lambda a: f'{a.group(1)}="{html.escape(self.link(a.group(2)), quote=True)}"', t)
            return stash(t)
        text = re.sub(r"</?" + INLINE_TAGS + r"\b[^>]*>", tag, text)
        # Autolinks.
        text = re.sub(r"<(https?://[^>\s]+)>", lambda m: stash(f'<a href="{html.escape(m.group(1))}">{html.escape(m.group(1))}</a>'), text)
        # Images and links (the text may hold code spans, already stashed).
        def image(m):
            return stash(f'<img src="{html.escape(self.link(m.group(2)), quote=True)}" alt="{html.escape(m.group(1), quote=True)}" loading="lazy">')
        text = re.sub(r"!\[([^\]]*)\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)", image, text)

        def link(m):
            url = self.link(m.group(2))
            ext = ' rel="noopener"' if url.startswith("http") else ""
            return stash(f'<a href="{html.escape(url, quote=True)}"{ext}>') + m.group(1) + stash("</a>")
        text = re.sub(r"\[((?:[^\[\]]|\[[^\]]*\])+)\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)", link, text)
        text = html.escape(text, quote=False)
        text = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text, flags=re.S)
        text = re.sub(r"(?<![\w*])\*(?!\s)(.+?)(?<!\s)\*(?![\w*])", r"<em>\1</em>", text)
        text = re.sub(r"(?<![\w])_(?!\s)(.+?)(?<!\s)_(?![\w])", r"<em>\1</em>", text)
        text = text.replace("  \n", "<br>\n")
        while "\u0000" in text:
            text = re.sub("\u0000(\\d+)\u0000", lambda m: keep[int(m.group(1))], text)
        return text


def split_row(line):
    """Cells of a table row, splitting on pipes outside code spans and escapes."""
    line = line.strip()
    if line.startswith("|"):
        line = line[1:]
    if line.endswith("|") and not line.endswith("\\|"):
        line = line[:-1]
    cells, cur, tick = [], "", 0
    i = 0
    while i < len(line):
        c = line[i]
        if c == "\\" and i + 1 < len(line) and line[i + 1] == "|":
            cur += "|"
            i += 2
            continue
        if c == "`":
            j = i
            while j < len(line) and line[j] == "`":
                j += 1
            n = j - i
            tick = 0 if tick == n else (n if tick == 0 else tick)
            cur += line[i:j]
            i = j
            continue
        if c == "|" and not tick:
            cells.append(cur.strip())
            cur = ""
        else:
            cur += c
        i += 1
    cells.append(cur.strip())
    return cells


LIST_RE = re.compile(r"^( *)([-*+]|\d+[.)])( +|$)(.*)")
FENCE_RE = re.compile(r"^( *)(`{3,}|~{3,})\s*([\w+#-]*)\s*(?:tab=(.+?))?\s*$")
HTML_BLOCK_RE = re.compile(r"^ {0,3}<(/?)(p|div|table|details|summary|h[1-6]|img|picture|section|br|hr|ul|ol|pre|!--)\b", re.I)


class Markdown:
    def __init__(self, link, highlight):
        self.inline = Inline(link)
        self.highlight = highlight
        self.headings = []  # (level, id, text)
        self.ids = {}

    def heading_id(self, text):
        base = slugify(text) or "section"
        n = self.ids.get(base, 0)
        self.ids[base] = n + 1
        return base if n == 0 else f"{base}-{n}"

    def convert(self, text):
        lines = text.replace("\r\n", "\n").replace("\t", "    ").split("\n")
        return self.blocks(lines, top=True)

    def blocks(self, lines, top=False):
        out = []
        i = 0
        n = len(lines)
        while i < n:
            line = lines[i]
            if not line.strip():
                i += 1
                continue
            m = FENCE_RE.match(line)
            if m:
                tabs = []
                while True:
                    indent, fence, lang, tab = len(m.group(1)), m.group(2), m.group(3).lower(), m.group(4)
                    body = []
                    i += 1
                    while i < n and not re.match(r"^ *" + re.escape(fence[0]) + "{" + str(len(fence)) + r",}\s*$", lines[i]):
                        body.append(lines[i][indent:] if lines[i][:indent].strip() == "" else lines[i])
                        i += 1
                    i += 1
                    tabs.append((tab, lang, "\n".join(body)))
                    if not tab:
                        break
                    j = i
                    while j < n and not lines[j].strip():
                        j += 1
                    m = FENCE_RE.match(lines[j]) if j < n else None
                    if not m or not m.group(4):
                        break
                    i = j
                if len(tabs) == 1 and not tabs[0][0]:
                    out.append(self.code_block(tabs[0][2], tabs[0][1]))
                else:
                    out.append(self.tab_group(tabs))
                continue
            m = re.match(r"^(#{1,6})\s+(.*?)\s*#*\s*$", line)
            if m:
                level, raw = len(m.group(1)), m.group(2)
                hid = self.heading_id(raw)
                inner = self.inline.render(raw)
                if top:
                    self.headings.append((level, hid, re.sub(r"<[^>]+>", "", inner)))
                anchor = f'<a class="anchor" href="#{hid}" aria-label="Link to this section">#</a>' if level > 1 else ""
                out.append(f'<h{level} id="{hid}">{inner}{anchor}</h{level}>')
                i += 1
                continue
            if re.match(r"^ {0,3}([-*_])( *\1){2,} *$", line):
                out.append("<hr>")
                i += 1
                continue
            if HTML_BLOCK_RE.match(line):
                body = []
                while i < n and lines[i].strip():
                    body.append(lines[i])
                    i += 1
                block = "\n".join(body)
                block = re.sub(r'\b(href|src)="([^"]*)"', lambda a: f'{a.group(1)}="{html.escape(self.inline.link(a.group(2)), quote=True)}"', block)
                out.append(block)
                continue
            if line.lstrip().startswith(">"):
                body = []
                while i < n and lines[i].strip() and lines[i].lstrip().startswith(">"):
                    body.append(re.sub(r"^ *> ?", "", lines[i]))
                    i += 1
                kind = None
                if body and re.match(r"^\[!(NOTE|TIP|WARNING|IMPORTANT|CAUTION)\]\s*$", body[0].strip(), re.I):
                    kind = body[0].strip()[2:-1].lower()
                    body = body[1:]
                inner = self.blocks(body)
                if kind:
                    label = {"note": "Note", "tip": "Tip", "warning": "Warning", "important": "Important", "caution": "Caution"}[kind]
                    out.append(f'<div class="callout callout-{kind}"><p class="callout-title">{label}</p>{inner}</div>')
                else:
                    out.append(f"<blockquote>{inner}</blockquote>")
                continue
            if "|" in line and i + 1 < n and re.match(r"^\s*\|?\s*:?-{2,}:?\s*(\|\s*:?-{2,}:?\s*)*\|?\s*$", lines[i + 1]):
                head = split_row(line)
                aligns = []
                for c in split_row(lines[i + 1]):
                    c = c.strip()
                    aligns.append("right" if c.endswith(":") and not c.startswith(":") else "center" if c.startswith(":") and c.endswith(":") and len(c) > 2 and c != ":---:" or c == ":---:" else "")
                i += 2
                rows = []
                while i < n and lines[i].strip() and "|" in lines[i]:
                    rows.append(split_row(lines[i]))
                    i += 1
                out.append(self.table(head, aligns, rows))
                continue
            m = LIST_RE.match(line)
            if m and (m.group(4).strip() or m.group(3) == ""):
                i, block = self.list_block(lines, i)
                out.append(block)
                continue
            # Paragraph: until a blank line or the start of another block.
            body = [line.strip()]
            i += 1
            while i < n and lines[i].strip():
                nxt = lines[i]
                if FENCE_RE.match(nxt) or re.match(r"^#{1,6}\s", nxt) or nxt.lstrip().startswith(">") or HTML_BLOCK_RE.match(nxt):
                    break
                lm = LIST_RE.match(nxt)
                if lm and len(lm.group(1)) <= 3 and lm.group(4).strip() and (not lm.group(2)[0].isdigit() or lm.group(2)[:-1] == "1"):
                    break
                body.append(nxt.strip() if not nxt.endswith("  ") else nxt.strip() + "  ")
                i += 1
            out.append("<p>" + self.inline.render("\n".join(body)) + "</p>")
        return "\n".join(out)

    def list_block(self, lines, i):
        n = len(lines)
        first = LIST_RE.match(lines[i])
        base = len(first.group(1))
        ordered = first.group(2)[0].isdigit()
        start = int(first.group(2)[:-1]) if ordered else 1
        items = []
        loose = False
        while i < n:
            m = LIST_RE.match(lines[i])
            if not m or len(m.group(1)) != base or m.group(2)[0].isdigit() != ordered:
                break
            content_indent = len(m.group(1)) + len(m.group(2)) + max(1, len(m.group(3)))
            body = [m.group(4)]
            i += 1
            blank = False
            while i < n:
                ln = lines[i]
                if not ln.strip():
                    blank = True
                    body.append("")
                    i += 1
                    continue
                ind = len(ln) - len(ln.lstrip(" "))
                if ind >= content_indent or (ind > base and LIST_RE.match(ln)):
                    body.append(ln[min(ind, content_indent):] if ind >= content_indent else ln[base + 2:] if len(ln) > base + 2 else ln.lstrip())
                    if blank and ind >= content_indent and not LIST_RE.match(ln):
                        loose = True
                    blank = False
                    i += 1
                    continue
                if blank:
                    break
                lm = LIST_RE.match(ln)
                if lm:
                    break
                # Lazy continuation of the paragraph.
                body.append(ln.strip())
                i += 1
            while body and not body[-1].strip():
                body.pop()
            items.append(body)
            # A blank line between items makes the list loose.
            if i < n and i > 0 and not lines[i - 1].strip():
                m2 = LIST_RE.match(lines[i]) if i < n else None
                if m2 and len(m2.group(1)) == base:
                    loose = True
        parts = []
        for body in items:
            inner = self.blocks(body)
            if not loose and inner.startswith("<p>"):
                inner = re.sub(r"^<p>(.*?)</p>", r"\1", inner, count=1, flags=re.S)
            parts.append(f"<li>{inner}</li>")
        tag = "ol" if ordered else "ul"
        attr = f' start="{start}"' if ordered and start != 1 else ""
        return i, f"<{tag}{attr}>" + "".join(parts) + f"</{tag}>"

    def table(self, head, aligns, rows):
        def cell(tagname, c, k):
            a = aligns[k] if k < len(aligns) else ""
            style = f' class="t-{a}"' if a else ""
            return f"<{tagname}{style}>{self.inline.render(c)}</{tagname}>"
        h = "".join(cell("th", c, k) for k, c in enumerate(head))
        body = "".join("<tr>" + "".join(cell("td", c, k) for k, c in enumerate(r)) + "</tr>" for r in rows)
        empty_head = all(not c for c in head)
        thead = "" if empty_head else f"<thead><tr>{h}</tr></thead>"
        return f'<div class="table-wrap"><table>{thead}<tbody>{body}</tbody></table></div>'

    def tab_group(self, tabs):
        names = [t[0] for t in tabs]
        buttons = "".join(f'<button type="button" role="tab" aria-selected="{"true" if k == 0 else "false"}" data-tab="{html.escape(t)}">{html.escape(t)}</button>' for k, t in enumerate(names))
        panels = "".join(f'<div role="tabpanel" data-tab="{html.escape(t)}"{"" if k == 0 else " hidden"}>{self.code_block(code, lang)}</div>' for k, (t, lang, code) in enumerate(tabs))
        return f'<div class="tabs"><div class="tab-list" role="tablist">{buttons}</div>{panels}</div>'

    def code_block(self, code, lang):
        label = LANG_LABEL.get(lang, lang if lang not in ("", "text") else "")
        attr = f' data-lang="{html.escape(label)}"' if label else ""
        return f'<div class="code"{attr}><pre><code>{self.highlight(code, lang)}</code></pre></div>'


# ---------------------------------------------------------------------------------------------
# Syntax highlighting (at build time: the page runs no highlighter)
# ---------------------------------------------------------------------------------------------

LANG_LABEL = {
    "bash": "Shell", "sh": "Shell", "shell": "Shell", "console": "Shell", "powershell": "PowerShell", "ps1": "PowerShell",
    "python": "Python", "py": "Python", "rust": "Rust", "rs": "Rust", "go": "Go", "java": "Java",
    "csharp": "C#", "cs": "C#", "c#": "C#", "php": "PHP", "ruby": "Ruby", "rb": "Ruby", "ts": "TypeScript",
    "typescript": "TypeScript", "js": "JavaScript", "javascript": "JavaScript", "json": "JSON", "jsonl": "JSON lines",
    "sql": "SQL", "toml": "TOML", "xml": "XML", "yaml": "YAML", "yml": "YAML", "ini": "INI", "http": "HTTP",
    "text": "", "": "",
}

KEYWORDS = {
    "python": "and as assert async await break class continue def del elif else except False finally for from global if import in is lambda None nonlocal not or pass raise return True try while with yield",
    "rust": "as async await break const continue crate dyn else enum extern false fn for if impl in let loop match mod move mut pub ref return self Self static struct super trait true type unsafe use where while Some None Ok Err",
    "go": "break case chan const continue default defer else fallthrough for func go goto if import interface map package range return select struct switch type var nil true false",
    "java": "abstract boolean break byte case catch char class continue default do double else enum extends final finally float for if implements import instanceof int interface long new null package private protected public return short static super switch this throw throws true false try var void while",
    "csharp": "abstract as async await base bool break case catch class const continue decimal default do double else enum false finally float for foreach if in int interface is long namespace new null object out override private protected public readonly ref return static string struct switch this throw true try using var void while",
    "php": "array as break case class const continue declare default do echo else elseif extends false fn for foreach function if implements new null private protected public require require_once return static switch throw true try use while",
    "ruby": "begin break case class def do else elsif end ensure false for if in module next nil not or require return self then true unless until when while yield",
    "ts": "as async await break case catch class const continue default delete do else export extends false finally for from function if import in instanceof interface let new null of return switch this throw true try type typeof undefined var void while",
    "sql": "add all alter and as asc begin between by case commit create default delete desc distinct drop else end exists from group having if in index insert into is join key left like limit not null on or order primary references rollback select set table then transaction union unique update values when where with",
    "bash": "if then else elif fi for in do done while case esac function export set unset local return",
    "powershell": "if else elseif foreach function param return try catch finally throw",
}
ALIAS = {"py": "python", "rs": "rust", "cs": "csharp", "c#": "csharp", "rb": "ruby", "typescript": "ts", "js": "ts", "javascript": "ts",
         "sh": "bash", "shell": "bash", "console": "bash", "ps1": "powershell", "jsonl": "json", "toml": "ini", "yaml": "ini", "yml": "ini"}


def highlight(code, lang):
    lang = ALIAS.get(lang, lang)
    if lang in ("", "text", "xml", "http"):
        return html.escape(code)
    if lang == "json":
        pat = re.compile(r'(?P<key>"(?:\\.|[^"\\])*"(?=\s*:))|(?P<str>"(?:\\.|[^"\\])*")|(?P<num>-?\b\d+(?:\.\d+)?(?:[eE][+-]?\d+)?\b)|(?P<kw>\btrue\b|\bfalse\b|\bnull\b)')
    elif lang == "ini":
        pat = re.compile(r'(?P<com>#.*$)|(?P<key>^\s*[\w.-]+(?=\s*=))|(?P<str>"(?:\\.|[^"\\])*")|(?P<num>\b\d+\b)', re.M)
    else:
        kws = KEYWORDS.get(lang, "")
        if lang in ("python", "bash", "ruby", "powershell"):
            com = r"#.*$"
        elif lang == "sql":
            com = r"--.*$"
        elif lang == "php":
            com = r"//.*$|#(?!\[).*$|/\*[\s\S]*?\*/"
        else:
            com = r"//.*$|/\*[\s\S]*?\*/"
        strs = r'"""[\s\S]*?"""|\'\'\'[\s\S]*?\'\'\'|"(?:\\.|[^"\\\n])*"' if lang == "python" else r'"(?:\\.|[^"\\\n])*"'
        if lang in ("python", "ts", "php", "ruby", "bash", "sql", "powershell"):
            strs += r"|'(?:\\.|[^'\\\n])*'"
        if lang == "ts" or lang == "go":
            strs += r"|`[^`]*`"
        if lang == "rust":
            strs += r"|b?'(?:\\.|[^'\\\n])'"
        kw = r"\b(?:" + "|".join(re.escape(k) for k in kws.split()) + r")\b" if kws else r"(?!x)x"
        flags = re.M | (re.I if lang in ("sql", "powershell") else 0)
        extra = ""
        if lang == "bash":
            extra = r"|(?P<var>\$\{?[A-Za-z_][\w]*\}?)|(?P<flag>(?<=\s)--?[\w-]+)"
        elif lang == "powershell":
            extra = r"|(?P<var>\$[\w:]+)"
        elif lang == "php":
            extra = r"|(?P<var>\$\w+)"
        elif lang == "rust":
            extra = r"|(?P<macro>\b\w+!)"
        pat = re.compile(rf"(?P<com>{com})|(?P<str>{strs})|(?P<kw>{kw})|(?P<num>\b\d[\d_]*(?:\.\d+)?\b)|(?P<fn>\b[A-Za-z_]\w*(?=\())" + extra, flags)
    out, pos = [], 0
    for m in pat.finditer(code):
        out.append(html.escape(code[pos:m.start()]))
        kind = m.lastgroup
        if kind == "num" and lang in ("bash", "powershell"):
            out.append(html.escape(m.group(0)))
            pos = m.end()
            continue
        cls = {"com": "c", "str": "s", "kw": "k", "num": "n", "key": "p", "fn": "f", "var": "v", "flag": "v", "macro": "f"}[kind]
        out.append(f'<span class="tok-{cls}">{html.escape(m.group(0))}</span>')
        pos = m.end()
    out.append(html.escape(code[pos:]))
    return "".join(out)


# ---------------------------------------------------------------------------------------------
# Site
# ---------------------------------------------------------------------------------------------

def front_matter(text):
    meta = {}
    if text.startswith("---\n"):
        end = text.index("\n---\n", 4)
        for line in text[4:end].splitlines():
            if ":" in line:
                k, v = line.split(":", 1)
                meta[k.strip()] = v.strip()
        text = text[end + 5:]
    return meta, text


class Page:
    def __init__(self, src, url, title, section, nav_title=None, description="", repo_doc=False, drop_title=False):
        self.src = src  # repository-relative path of the Markdown
        self.url = url  # site path, e.g. "docs/clusters/"
        self.title = title
        self.nav_title = nav_title or title
        self.section = section
        self.description = description
        self.repo_doc = repo_doc
        self.drop_title = drop_title
        self.html = ""
        self.headings = []


def load_nav():
    nav = json.load(open(os.path.join(SITE, "nav.json")))
    pages = []
    for section in nav:
        for item in section["pages"]:
            src = item["src"]
            path = os.path.join(ROOT, src)
            text = open(path, encoding="utf-8").read()
            meta, _ = front_matter(text)
            title = item.get("title") or meta.get("title")
            if not title:
                m = re.search(r"^# (.+)$", text, re.M)
                title = m.group(1) if m else src
            pages.append(Page(src, item["url"], title, section["title"], item.get("nav") or meta.get("nav"),
                              item.get("description") or meta.get("description", ""), not src.startswith("website/"),
                              item.get("drop_title", False)))
    return nav, pages


class Site:
    def __init__(self, out, base):
        self.out = out
        self.base = base.rstrip("/") + "/"
        self.nav, self.pages = load_nav()
        self.by_src = {p.src: p for p in self.pages}
        self.errors = []
        self.links = []  # (page, url) to check
        self.version = version()

    # A link written in a Markdown file at `src`, as a URL relative to `page`.
    def rewrite(self, page, url):
        if not url or url.startswith(("#", "mailto:")) or re.match(r"^[a-z]+:", url):
            if url.startswith("#"):
                self.links.append((page, page.url + url))
            return url
        path, _, frag = url.partition("#")
        if path.startswith("/"):
            target = path.lstrip("/")
        else:
            target = posixpath.normpath(posixpath.join(posixpath.dirname(page.src), path))
        if target.startswith("../"):
            self.errors.append(f"{page.src}: link outside the repository: {url}")
            return url
        if target in self.by_src:
            dest = self.by_src[target].url + (("#" + frag) if frag else "")
            self.links.append((page, dest))
            return self.relative(page, dest)
        # Static files of the site (images) and images the documents show.
        if target.startswith("website/static/"):
            return self.relative(page, target[len("website/static/"):])
        if re.search(r"\.(png|svg|jpg|jpeg|gif|webp)$", target, re.I) and os.path.exists(os.path.join(ROOT, target)):
            rel = "assets/" + target.replace("/", "_")
            dst = os.path.join(self.out, rel)
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.copyfile(os.path.join(ROOT, target), dst)
            return self.relative(page, rel)
        full = os.path.join(ROOT, target)
        if not os.path.exists(full):
            self.errors.append(f"{page.src}: link to a missing file: {url}")
            return url
        return (TREE if os.path.isdir(full) else BLOB) + target + (("#" + frag) if frag else "")

    def relative(self, page, dest):
        """`dest` (a site path) relative to the directory of `page`'s URL."""
        path, _, frag = dest.partition("#")
        here = page.url if page.url.endswith("/") or page.url == "" else posixpath.dirname(page.url) + "/"
        rel = posixpath.relpath(path or ".", here or ".") if path else ""
        if path.endswith("/") and rel != "." and not rel.endswith("/"):
            rel += "/"
        if rel == ".":
            rel = "./"
        if path == "":
            rel = "./"
        return rel + (("#" + frag) if frag else "")

    def render_pages(self):
        for page in self.pages:
            if page.src.endswith(".html"):
                page.html, page.headings, page.ids = "", [], set()
                continue
            text = open(os.path.join(ROOT, page.src), encoding="utf-8").read()
            _, body = front_matter(text)
            body = body.replace("{{version}}", self.version)
            body = re.sub(r"\{\{include:([^}]+)\}\}", lambda m: self.include(page, m.group(1)), body)
            if page.repo_doc or page.drop_title:
                body = re.sub(r"\A\s*# .+\n", "", body, count=1)
            md = Markdown(lambda u, p=page: self.rewrite(p, u), highlight)
            page.html = md.convert(body)
            page.headings = md.headings
            page.ids = set(md.ids_seen()) if hasattr(md, "ids_seen") else set(re.findall(r'id="([^"]+)"', page.html))

    def include(self, page, path):
        full = os.path.join(ROOT, path)
        if not os.path.exists(full):
            self.errors.append(f"{page.src}: includes a missing file: {path}")
            return ""
        text = open(full, encoding="utf-8").read()
        text = re.sub(r"\A(//!.*\n)+\n?", "", text)
        return text.rstrip("\n")

    # -- templates --------------------------------------------------------------------------

    def asset(self, page, name):
        return self.relative(page, name)

    def head(self, page, title, description):
        desc = html.escape(description or "Storine: a fast document database you can embed, run as a server, or replicate across a cluster.", quote=True)
        return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(title)}</title>
<meta name="description" content="{desc}">
<meta name="color-scheme" content="light dark">
<meta http-equiv="Content-Security-Policy" content="default-src 'self'; img-src 'self' data: https://img.shields.io https://github.com; style-src 'self'; script-src 'self'; connect-src 'self'">
<link rel="icon" href="{self.asset(page, 'logo.svg')}" type="image/svg+xml">
<link rel="apple-touch-icon" href="{self.asset(page, 'icon-180.png')}">
{self.share_tags(page, title, desc)}
<link rel="stylesheet" href="{self.asset(page, 'site.css')}?v={self.version}">
<script src="{self.asset(page, 'theme.js')}?v={self.version}"></script>
<script src="{self.asset(page, 'site.js')}?v={self.version}" defer></script>
</head>"""

    def share_tags(self, page, title, desc):
        """Open Graph and Twitter card tags, read by WhatsApp, Slack, X, LinkedIn and others."""
        t = html.escape(title, quote=True)
        tags = ""
        if page.url != "404.html":
            url = SITE_URL + page.url
            tags += f'<link rel="canonical" href="{url}">\n<meta property="og:url" content="{url}">\n'
        return tags + f"""<meta property="og:type" content="website">
<meta property="og:site_name" content="Storine">
<meta property="og:title" content="{t}">
<meta property="og:description" content="{desc}">
<meta property="og:image" content="{SITE_URL}og.png">
<meta property="og:image:type" content="image/png">
<meta property="og:image:width" content="1200">
<meta property="og:image:height" content="630">
<meta property="og:image:alt" content="Storine: the document database that is fast, exact and small.">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{t}">
<meta name="twitter:description" content="{desc}">
<meta name="twitter:image" content="{SITE_URL}og.png">
<meta name="theme-color" content="#4f46e5">"""

    def header(self, page, active):
        home = self.relative(page, "")
        docs = self.relative(page, "docs/")
        links = [("Docs", docs, "docs"), ("Clients", self.relative(page, "docs/clients/"), "clients"),
                 ("Reference", self.relative(page, "docs/reference/configuration/"), "reference"),
                 ("Changelog", self.relative(page, "changelog/"), "changelog")]
        nav = "".join(f'<a href="{u}"{" aria-current=\"page\"" if k == active else ""}>{t}</a>' for t, u, k in links)
        return f"""<a class="skip" href="#main">Skip to content</a>
<header class="topbar">
  <div class="topbar-inner">
    <button class="icon-btn menu-btn" type="button" aria-label="Open the navigation" data-action="menu">{ICON_MENU}</button>
    <a class="brand" href="{home}"><img src="{self.asset(page, 'logo.svg')}" alt="" width="26" height="26"><span>Storine</span></a>
    <span class="version-pill" title="Latest release">v{self.version}</span>
    <nav class="topnav" aria-label="Main">{nav}</nav>
    <button class="search-btn" type="button" data-action="search" aria-label="Search the documentation">{ICON_SEARCH}<span>Search</span><kbd>/</kbd></button>
    <button class="icon-btn" type="button" data-action="theme" aria-label="Switch between light and dark">{ICON_SUN}{ICON_MOON}</button>
    <a class="icon-btn" href="{REPO_URL}" aria-label="Storine on GitHub" rel="noopener">{ICON_GITHUB}</a>
  </div>
</header>"""

    def search_dialog(self, page):
        return f"""<div class="search" hidden data-index="{self.asset(page, 'search.json')}?v={self.version}" data-root="{self.relative(page, '')}">
  <div class="search-backdrop" data-action="close-search"></div>
  <div class="search-panel" role="dialog" aria-modal="true" aria-label="Search">
    <div class="search-field">{ICON_SEARCH}<input type="search" placeholder="Search the documentation" aria-label="Search" autocomplete="off" spellcheck="false"><kbd>Esc</kbd></div>
    <ul class="search-results" role="listbox"></ul>
    <p class="search-empty" hidden>Nothing matches. Try fewer words.</p>
  </div>
</div>"""

    def footer(self, page):
        return f"""<footer class="footer">
  <div class="footer-inner">
    <div class="footer-brand"><img src="{self.asset(page, 'logo.svg')}" alt="" width="22" height="22"> Storine {self.version}</div>
    <nav aria-label="Footer">
      <a href="{self.relative(page, 'docs/installation/')}">Install</a>
      <a href="{self.relative(page, 'docs/')}">Documentation</a>
      <a href="{self.relative(page, 'docs/reference/protocol/')}">Wire protocol</a>
      <a href="{self.relative(page, 'docs/reference/benchmarks/')}">Benchmarks</a>
      <a href="{self.relative(page, 'changelog/')}">Changelog</a>
      <a href="{REPO_URL}" rel="noopener">GitHub</a>
    </nav>
    <p>MIT or Apache-2.0, at your option. This site loads nothing from other servers.</p>
  </div>
</footer>"""

    def sidebar(self, page):
        parts = []
        for section in self.nav:
            items = []
            for item in section["pages"]:
                p = next(x for x in self.pages if x.src == item["src"] and x.url == item["url"])
                if p.url in ("", "changelog/"):
                    continue
                cur = ' aria-current="page"' if p is page else ""
                items.append(f'<li><a href="{self.relative(page, p.url)}"{cur}>{html.escape(p.nav_title)}</a></li>')
            if items:
                parts.append(f'<div class="side-group"><p class="side-title">{html.escape(section["title"])}</p><ul>{"".join(items)}</ul></div>')
        return f'<aside class="sidebar" aria-label="Documentation"><nav>{"".join(parts)}</nav></aside><div class="sidebar-backdrop" data-action="menu"></div>'

    def toc(self, page):
        hs = [h for h in page.headings if h[0] in (2, 3)]
        if len(hs) < 2:
            return '<aside class="toc"></aside>'
        items = "".join(f'<li class="toc-l{lvl}"><a href="#{hid}">{html.escape(text)}</a></li>' for lvl, hid, text in hs)
        return f'<aside class="toc" aria-label="On this page"><p class="toc-title">On this page</p><ul>{items}</ul></aside>'

    def doc_page(self, page, i, docs):
        prev_p = docs[i - 1] if i > 0 else None
        next_p = docs[i + 1] if i + 1 < len(docs) else None
        pager = '<nav class="pager" aria-label="Previous and next">'
        if prev_p:
            pager += f'<a class="pager-prev" href="{self.relative(page, prev_p.url)}"><span>Previous</span>{html.escape(prev_p.nav_title)}</a>'
        else:
            pager += "<span></span>"
        if next_p:
            pager += f'<a class="pager-next" href="{self.relative(page, next_p.url)}"><span>Next</span>{html.escape(next_p.nav_title)}</a>'
        pager += "</nav>"
        source = f'<a class="edit" href="{BLOB}{page.src}" rel="noopener">{ICON_EDIT} Edit this page on GitHub</a>'
        lead = f'<p class="lead">{self.inline_plain(page, page.description)}</p>' if page.description else ""
        crumb = f'<p class="eyebrow">{html.escape(page.section)}</p>'
        active = "clients" if page.url.startswith("docs/clients") else "reference" if page.url.startswith("docs/reference") else "docs"
        body = f"""{self.head(page, f"{page.title} · Storine", page.description)}
<body class="doc">
{self.header(page, active)}
<div class="layout">
{self.sidebar(page)}
<main id="main" class="content">
<article class="prose">
{crumb}
<h1>{html.escape(page.title)}</h1>
{lead}
{page.html}
</article>
<div class="page-end">{source}</div>
{pager}
</main>
{self.toc(page)}
</div>
{self.search_dialog(page)}
{self.footer(page)}
</body>
</html>
"""
        return body

    def inline_plain(self, page, text):
        return Inline(lambda u: self.rewrite(page, u)).render(text)

    def changelog_page(self, page):
        body = f"""{self.head(page, "Changelog · Storine", "Every Storine release, what it added, changed and fixed, and how to upgrade.")}
<body class="doc">
{self.header(page, "changelog")}
<main id="main" class="content content-wide">
<article class="prose">
<p class="eyebrow">Releases</p>
<h1>Changelog</h1>
{page.html}
</article>
<div class="page-end"><a class="edit" href="{BLOB}{page.src}" rel="noopener">{ICON_EDIT} Edit this page on GitHub</a></div>
</main>
{self.search_dialog(page)}
{self.footer(page)}
</body>
</html>
"""
        return body

    def home_page(self, page):
        tpl = open(os.path.join(SITE, "home.html"), encoding="utf-8").read()
        tpl = tpl.replace("{{version}}", self.version)
        tpl = re.sub(r'\{\{link:([^}]*)\}\}', lambda m: self.link_to(page, m.group(1)), tpl)
        tpl = re.sub(r"\{\{code:(\w+)\}\}\n(.*?)\n\{\{/code\}\}", lambda m: highlight(m.group(2), m.group(1)), tpl, flags=re.S)
        return f"""{self.head(page, "Storine: a fast document database", "A fast document database you can embed, run as a server, or replicate across a cluster. Written in Rust, with no third-party dependencies.")}
<body class="home">
{self.header(page, "")}
<main id="main">
{tpl}
</main>
{self.search_dialog(page)}
{self.footer(page)}
</body>
</html>
"""

    def link_to(self, page, target):
        path, _, frag = target.partition("#")
        if path in self.by_src:
            dest = self.by_src[path].url + ("#" + frag if frag else "")
            self.links.append((page, dest))
            return self.relative(page, dest)
        if path.startswith("http"):
            return target
        return self.relative(page, target)

    def not_found(self):
        page = Page("website/404", "404.html", "Page not found", "")
        # 404.html is served from any depth, so it links from the site root.
        old = self.relative
        self.relative = lambda p, dest: self.base + dest if not dest.startswith("#") else dest
        try:
            body = f"""{self.head(page, "Page not found · Storine", "")}
<body class="home">
{self.header(page, "")}
<main id="main" class="notfound">
<p class="eyebrow">404</p>
<h1>This page does not exist</h1>
<p>It may have moved when the documentation was reorganised. Search for it, or start from the documentation.</p>
<p><a class="btn btn-primary" href="{self.base}docs/">Go to the documentation</a></p>
</main>
{self.search_dialog(page)}
{self.footer(page)}
</body>
</html>
"""
        finally:
            self.relative = old
        return body

    # -- search ---------------------------------------------------------------------------

    def search_index(self):
        entries = []
        for page in self.pages:
            if page.url == "":
                continue
            # Split the page at h2/h3 headings.
            chunks = re.split(r'(<h[23] id="[^"]+">.*?</h[23]>)', page.html)
            heading, hid = "", ""
            buf = chunks[0]
            def flush(h, i, b):
                text = re.sub(r"<[^>]+>", " ", b)
                text = html.unescape(re.sub(r"\s+", " ", text)).strip()
                if text or h:
                    entries.append({"p": page.title, "h": h, "u": page.url + (("#" + i) if i else ""), "s": page.section, "x": text[:1200]})
            for c in chunks[1:]:
                m = re.match(r'<h([23]) id="([^"]+)">(.*?)</h[23]>', c, re.S)
                if m:
                    flush(heading, hid, buf)
                    heading = html.unescape(re.sub(r"<[^>]+>", "", m.group(3))).rstrip("#").strip()
                    hid = m.group(2)
                    buf = ""
                else:
                    buf += c
            flush(heading, hid, buf)
        return entries

    # -- build ----------------------------------------------------------------------------

    def build(self):
        if os.path.exists(self.out):
            shutil.rmtree(self.out)
        os.makedirs(self.out)
        for name in os.listdir(os.path.join(SITE, "static")):
            shutil.copyfile(os.path.join(SITE, "static", name), os.path.join(self.out, name))
        shutil.copyfile(os.path.join(ROOT, "docs/images/logo.svg"), os.path.join(self.out, "logo.svg"))
        os.makedirs(os.path.join(self.out, "assets"))
        for name in os.listdir(os.path.join(ROOT, "docs/images")):
            shutil.copyfile(os.path.join(ROOT, "docs/images", name), os.path.join(self.out, "assets", "docs_images_" + name))
        self.render_pages()
        docs = [p for p in self.pages if p.url not in ("", "changelog/")]
        for page in self.pages:
            if page.url == "":
                out = self.home_page(page)
            elif page.url == "changelog/":
                out = self.changelog_page(page)
            else:
                out = self.doc_page(page, docs.index(page), docs)
            dst = os.path.join(self.out, page.url, "index.html") if page.url.endswith("/") or page.url == "" else os.path.join(self.out, page.url)
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            open(dst, "w", encoding="utf-8").write(out)
        open(os.path.join(self.out, "404.html"), "w", encoding="utf-8").write(self.not_found())
        json.dump(self.search_index(), open(os.path.join(self.out, "search.json"), "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))
        open(os.path.join(self.out, ".nojekyll"), "w").close()

    def check(self):
        errors = list(self.errors)
        ids = {p.url: set(re.findall(r'\bid="([^"]+)"', p.html)) for p in self.pages}
        urls = {p.url for p in self.pages}
        for page, dest in self.links:
            path, _, frag = dest.partition("#")
            if path not in urls:
                errors.append(f"{page.src}: link to a page that is not built: {dest}")
            elif frag and frag not in ids[path]:
                errors.append(f"{page.src}: link to a missing anchor: {dest}")
        content = os.path.join(SITE, "content")
        listed = {p.src for p in self.pages}
        for dirpath, _, files in os.walk(content):
            for f in files:
                rel = os.path.relpath(os.path.join(dirpath, f), ROOT).replace(os.sep, "/")
                if f.endswith(".md") and rel not in listed:
                    errors.append(f"{rel}: not in website/nav.json")
        # Nothing may load from another origin (links to other sites, and the canonical address, are fine).
        for dirpath, _, files in os.walk(self.out):
            for f in files:
                if f.endswith((".html", ".css", ".js")):
                    text = open(os.path.join(dirpath, f), encoding="utf-8").read()
                    for m in re.finditer(r'<(?:script|link(?![^>]*\brel="canonical")|iframe)\b[^>]*\b(?:src|href)="(https?:)?//', text):
                        errors.append(f"{f}: loads from another origin: {m.group(0)}")
                    for m in re.finditer(r"url\(\s*['\"]?https?://|@import\s+['\"]?https?://", text):
                        errors.append(f"{f}: loads from another origin: {m.group(0)}")
        return errors


ICON_MENU = '<svg viewBox="0 0 24 24" width="20" height="20" aria-hidden="true"><path d="M4 7h16M4 12h16M4 17h16" stroke="currentColor" stroke-width="2" stroke-linecap="round" fill="none"/></svg>'
ICON_SEARCH = '<svg viewBox="0 0 24 24" width="17" height="17" aria-hidden="true"><circle cx="11" cy="11" r="6.5" stroke="currentColor" stroke-width="2" fill="none"/><path d="m16 16 4.5 4.5" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>'
ICON_SUN = '<svg class="i-sun" viewBox="0 0 24 24" width="19" height="19" aria-hidden="true"><circle cx="12" cy="12" r="4" fill="none" stroke="currentColor" stroke-width="2"/><path d="M12 2.5v2M12 19.5v2M4.6 4.6l1.4 1.4M18 18l1.4 1.4M2.5 12h2M19.5 12h2M4.6 19.4 6 18M18 6l1.4-1.4" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>'
ICON_MOON = '<svg class="i-moon" viewBox="0 0 24 24" width="19" height="19" aria-hidden="true"><path d="M20 14.5A8 8 0 0 1 9.5 4a8 8 0 1 0 10.5 10.5Z" fill="none" stroke="currentColor" stroke-width="2" stroke-linejoin="round"/></svg>'
ICON_GITHUB = '<svg viewBox="0 0 24 24" width="20" height="20" aria-hidden="true"><path fill="currentColor" d="M12 2a10 10 0 0 0-3.16 19.49c.5.09.68-.22.68-.48v-1.7c-2.78.6-3.37-1.34-3.37-1.34-.45-1.16-1.11-1.47-1.11-1.47-.91-.62.07-.6.07-.6 1 .07 1.53 1.03 1.53 1.03.9 1.52 2.34 1.08 2.91.83.09-.65.35-1.09.63-1.34-2.22-.25-4.55-1.11-4.55-4.94 0-1.09.39-1.98 1.03-2.68-.1-.25-.45-1.27.1-2.64 0 0 .84-.27 2.75 1.02a9.5 9.5 0 0 1 5 0c1.91-1.29 2.75-1.02 2.75-1.02.55 1.37.2 2.39.1 2.64.64.7 1.03 1.59 1.03 2.68 0 3.84-2.34 4.68-4.57 4.93.36.31.68.92.68 1.85v2.74c0 .27.18.58.69.48A10 10 0 0 0 12 2Z"/></svg>'
ICON_EDIT = '<svg viewBox="0 0 24 24" width="15" height="15" aria-hidden="true"><path d="M4 20h4L19 9l-4-4L4 16v4Z" fill="none" stroke="currentColor" stroke-width="2" stroke-linejoin="round"/></svg>'


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--out", default=os.path.join(SITE, "_site"))
    ap.add_argument("--base", default="/storine/", help="the path the site is served under (for the 404 page)")
    ap.add_argument("--check", action="store_true", help="fail on broken links and anchors")
    args = ap.parse_args()
    site = Site(os.path.abspath(args.out), args.base)
    site.build()
    errors = site.check()
    n = len(site.pages)
    if errors:
        for e in errors:
            print("error:", e, file=sys.stderr)
        print(f"{len(errors)} problem(s) in {n} pages", file=sys.stderr)
        if args.check:
            sys.exit(1)
    else:
        print(f"built {n} pages into {os.path.relpath(site.out, os.getcwd())}")


if __name__ == "__main__":
    main()
