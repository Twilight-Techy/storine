//! A small backtracking regular-expression engine for `~`, `REGEXP` and `regexp_replace`
//! (POSIX-style subset: `. ^ $ * + ? {n,m} [...] (...) |`, `\d \w \s` and their negations,
//! lazy quantifiers). Matching is bounded by a step budget so hostile patterns cannot hang a
//! session.

use std::cell::Cell;

use crate::error::{SqlError, SqlResult};

#[derive(Debug, Clone)]
enum Node {
    Char(char),
    Any,
    Class { items: Vec<ClassItem>, negated: bool },
    Start,
    End,
    Group(Box<Node>),
    Alt(Vec<Node>),
    Concat(Vec<Node>),
    Repeat { node: Box<Node>, min: usize, max: usize, greedy: bool },
}

#[derive(Debug, Clone)]
enum ClassItem {
    Range(char, char),
    Digit,
    Word,
    Space,
}

#[derive(Debug, Clone)]
pub struct Regex {
    root: Node,
    ci: bool,
}

const STEP_LIMIT: usize = 2_000_000;

impl Regex {
    pub fn new(pattern: &str, case_insensitive: bool) -> SqlResult<Regex> {
        let chars: Vec<char> = pattern.chars().collect();
        let mut p = P { c: &chars, i: 0 };
        let root = p.alt()?;
        if p.i != chars.len() {
            return Err(bad(pattern));
        }
        Ok(Regex { root, ci: case_insensitive })
    }

    pub fn is_match(&self, s: &str) -> bool {
        let text: Vec<char> = s.chars().collect();
        (0..=text.len()).any(|start| self.match_at(&text, start).is_some())
    }

    /// Replaces the first (or every, `global`) match with `with` (literal text; `\N` not supported).
    pub fn replace(&self, s: &str, with: &str, global: bool) -> String {
        let text: Vec<char> = s.chars().collect();
        let mut out = String::new();
        let mut i = 0;
        let mut replaced = false;
        while i <= text.len() {
            if !(replaced && !global) {
                if let Some(end) = self.match_at(&text, i) {
                    out.push_str(with);
                    replaced = true;
                    if end > i {
                        i = end;
                        continue;
                    }
                }
            }
            if i < text.len() {
                out.push(text[i]);
            }
            i += 1;
        }
        out
    }

    fn match_at(&self, text: &[char], start: usize) -> Option<usize> {
        let steps = Cell::new(0usize);
        let mut result = None;
        let mut k = |end: usize| {
            result = Some(end);
            true
        };
        if m(&self.root, text, start, self.ci, &steps, &mut k) {
            result
        } else {
            None
        }
    }
}

fn bad(p: &str) -> SqlError {
    SqlError::new("2201B", format!("invalid regular expression: {p}"))
}

struct P<'a> {
    c: &'a [char],
    i: usize,
}

impl P<'_> {
    fn peek(&self) -> Option<char> {
        self.c.get(self.i).copied()
    }

    fn alt(&mut self) -> SqlResult<Node> {
        let mut options = vec![self.concat()?];
        while self.peek() == Some('|') {
            self.i += 1;
            options.push(self.concat()?);
        }
        Ok(if options.len() == 1 { options.pop().unwrap_or(Node::Concat(Vec::new())) } else { Node::Alt(options) })
    }

    fn concat(&mut self) -> SqlResult<Node> {
        let mut items = Vec::new();
        while let Some(c) = self.peek() {
            if c == '|' || c == ')' {
                break;
            }
            let atom = self.atom()?;
            items.push(self.quantified(atom)?);
        }
        Ok(Node::Concat(items))
    }

    fn quantified(&mut self, atom: Node) -> SqlResult<Node> {
        let (min, max) = match self.peek() {
            Some('*') => (0, usize::MAX),
            Some('+') => (1, usize::MAX),
            Some('?') => (0, 1),
            Some('{') => {
                let close = self.c[self.i..].iter().position(|c| *c == '}').map(|p| p + self.i);
                let Some(close) = close else { return Ok(atom) };
                let body: String = self.c[self.i + 1..close].iter().collect();
                let (lo, hi) = match body.split_once(',') {
                    Some((a, b)) => (a.trim().parse().map_err(|_| bad(&body))?, if b.trim().is_empty() { usize::MAX } else { b.trim().parse().map_err(|_| bad(&body))? }),
                    None => {
                        let n = body.trim().parse().map_err(|_| bad(&body))?;
                        (n, n)
                    }
                };
                self.i = close;
                (lo, hi)
            }
            _ => return Ok(atom),
        };
        self.i += 1;
        let greedy = if self.peek() == Some('?') {
            self.i += 1;
            false
        } else {
            true
        };
        Ok(Node::Repeat { node: Box::new(atom), min, max, greedy })
    }

    fn atom(&mut self) -> SqlResult<Node> {
        let c = self.peek().ok_or_else(|| bad("unexpected end"))?;
        self.i += 1;
        Ok(match c {
            '.' => Node::Any,
            '^' => Node::Start,
            '$' => Node::End,
            '(' => {
                if self.c.get(self.i) == Some(&'?') && self.c.get(self.i + 1) == Some(&':') {
                    self.i += 2;
                }
                let inner = self.alt()?;
                if self.peek() != Some(')') {
                    return Err(bad("missing )"));
                }
                self.i += 1;
                Node::Group(Box::new(inner))
            }
            '[' => self.class()?,
            '\\' => {
                let e = self.peek().ok_or_else(|| bad("trailing backslash"))?;
                self.i += 1;
                match e {
                    'd' => Node::Class { items: vec![ClassItem::Digit], negated: false },
                    'D' => Node::Class { items: vec![ClassItem::Digit], negated: true },
                    'w' => Node::Class { items: vec![ClassItem::Word], negated: false },
                    'W' => Node::Class { items: vec![ClassItem::Word], negated: true },
                    's' => Node::Class { items: vec![ClassItem::Space], negated: false },
                    'S' => Node::Class { items: vec![ClassItem::Space], negated: true },
                    'n' => Node::Char('\n'),
                    't' => Node::Char('\t'),
                    other => Node::Char(other),
                }
            }
            other => Node::Char(other),
        })
    }

    fn class(&mut self) -> SqlResult<Node> {
        let negated = if self.peek() == Some('^') {
            self.i += 1;
            true
        } else {
            false
        };
        let mut items = Vec::new();
        let mut first = true;
        loop {
            let c = self.peek().ok_or_else(|| bad("missing ]"))?;
            self.i += 1;
            if c == ']' && !first {
                break;
            }
            first = false;
            if c == '[' && self.peek() == Some(':') {
                let close = self.c[self.i..].iter().collect::<String>().find(":]").ok_or_else(|| bad("bad class"))?;
                let name: String = self.c[self.i + 1..self.i + close].iter().collect();
                self.i += close + 2;
                items.push(match name.as_str() {
                    "digit" => ClassItem::Digit,
                    "alnum" | "word" => ClassItem::Word,
                    "space" => ClassItem::Space,
                    "alpha" => ClassItem::Range('A', 'z'),
                    "upper" => ClassItem::Range('A', 'Z'),
                    "lower" => ClassItem::Range('a', 'z'),
                    _ => return Err(bad(&name)),
                });
                continue;
            }
            let lo = if c == '\\' {
                let e = self.peek().ok_or_else(|| bad("bad escape"))?;
                self.i += 1;
                match e {
                    'd' => {
                        items.push(ClassItem::Digit);
                        continue;
                    }
                    'w' => {
                        items.push(ClassItem::Word);
                        continue;
                    }
                    's' => {
                        items.push(ClassItem::Space);
                        continue;
                    }
                    other => other,
                }
            } else {
                c
            };
            if self.peek() == Some('-') && self.c.get(self.i + 1).is_some_and(|x| *x != ']') {
                let hi = self.c[self.i + 1];
                self.i += 2;
                items.push(ClassItem::Range(lo, hi));
            } else {
                items.push(ClassItem::Range(lo, lo));
            }
        }
        Ok(Node::Class { items, negated })
    }
}

fn eq_ci(a: char, b: char, ci: bool) -> bool {
    a == b || (ci && a.to_lowercase().eq(b.to_lowercase()))
}

fn class_has(items: &[ClassItem], c: char, ci: bool) -> bool {
    items.iter().any(|it| match it {
        ClassItem::Range(lo, hi) => {
            (*lo <= c && c <= *hi) || (ci && c.to_lowercase().chain(c.to_uppercase()).any(|x| *lo <= x && x <= *hi))
        }
        ClassItem::Digit => c.is_ascii_digit(),
        ClassItem::Word => c.is_alphanumeric() || c == '_',
        ClassItem::Space => c.is_whitespace(),
    })
}

/// Matches `node` at `pos`, calling `k` with every end position until it accepts. `steps`
/// bounds the total work of one match attempt.
fn m(node: &Node, t: &[char], pos: usize, ci: bool, steps: &Cell<usize>, k: &mut dyn FnMut(usize) -> bool) -> bool {
    steps.set(steps.get() + 1);
    if steps.get() > STEP_LIMIT {
        return false;
    }
    match node {
        Node::Char(c) => pos < t.len() && eq_ci(t[pos], *c, ci) && k(pos + 1),
        Node::Any => pos < t.len() && t[pos] != '\n' && k(pos + 1),
        Node::Class { items, negated } => pos < t.len() && class_has(items, t[pos], ci) != *negated && k(pos + 1),
        Node::Start => pos == 0 && k(pos),
        Node::End => pos == t.len() && k(pos),
        Node::Group(inner) => m(inner, t, pos, ci, steps, k),
        Node::Alt(options) => options.iter().any(|o| m(o, t, pos, ci, steps, k)),
        Node::Concat(items) => seq(items, t, pos, ci, steps, k),
        Node::Repeat { node, min, max, greedy } => rep(node, (*min, *max, *greedy), 0, t, pos, ci, steps, k),
    }
}

fn seq(items: &[Node], t: &[char], pos: usize, ci: bool, steps: &Cell<usize>, k: &mut dyn FnMut(usize) -> bool) -> bool {
    match items.split_first() {
        None => k(pos),
        Some((first, rest)) => m(first, t, pos, ci, steps, &mut |p| seq(rest, t, p, ci, steps, k)),
    }
}

#[allow(clippy::too_many_arguments)]
fn rep(
    node: &Node,
    (min, max, greedy): (usize, usize, bool),
    count: usize,
    t: &[char],
    pos: usize,
    ci: bool,
    steps: &Cell<usize>,
    k: &mut dyn FnMut(usize) -> bool,
) -> bool {
    steps.set(steps.get() + 1);
    if steps.get() > STEP_LIMIT {
        return false;
    }
    let spec = (min, max, greedy);
    if count < min {
        return m(node, t, pos, ci, steps, &mut |p| rep(node, spec, count + 1, t, p, ci, steps, k));
    }
    let more = |k: &mut dyn FnMut(usize) -> bool| {
        count < max && m(node, t, pos, ci, steps, &mut |p| p != pos && rep(node, spec, count + 1, t, p, ci, steps, k))
    };
    // Greedy tries one more repetition first; lazy tries stopping first.
    #[allow(clippy::if_same_then_else)]
    if greedy {
        more(k) || k(pos)
    } else {
        k(pos) || more(k)
    }
}

#[cfg(test)]
mod tests {
    use super::Regex;

    #[test]
    fn patterns() {
        let r = |p: &str, s: &str| Regex::new(p, false).unwrap().is_match(s);
        assert!(r("^(users)$", "users"));
        assert!(!r("^(users)$", "users2"));
        assert!(r("^a.*z$", "abcz"));
        assert!(r("\\d{3}-\\d{4}", "call 555-1234 now"));
        assert!(!r("^\\d+$", "12a"));
        assert!(r("^[a-c]+$", "abcabc"));
        assert!(r("^[^0-9]*$", "abc"));
        assert!(r("cat|dog", "hotdog"));
        assert!(r("^(ab)+$", "ababab"));
        assert!(Regex::new("^HELLO$", true).unwrap().is_match("hello"));
        assert_eq!(Regex::new("o", false).unwrap().replace("foo boo", "0", true), "f00 b00");
        assert_eq!(Regex::new("o+", false).unwrap().replace("foo boo", "0", false), "f0 boo");
        assert!(Regex::new("(", false).is_err());
        // Catastrophic patterns terminate.
        let evil = Regex::new("^(a+)+$", false).unwrap();
        let _ = evil.is_match(&("a".repeat(40) + "b"));
    }
}
