#!/usr/bin/env python3
"""Draws the figures of the example READMEs as SVG, from a data file per example.

    python3 tools/figures/figures.py examples/<ex>          rebuild one example's figures
    python3 tools/figures/figures.py                         rebuild every example that has docs/figures.json
    python3 tools/figures/figures.py --check [examples/..]   exit 1 when a committed SVG differs from its data,
                                                             is missing, or has no figure in the data

The data is examples/<ex>/docs/figures.json, {"figures": [..]}; each figure has a "type" and a "name" and is written
to examples/<ex>/docs/img/<name>.svg. The types and their fields are in `render` and the functions it calls. Python 3 standard
library only, and the output depends only on the data, so the check is exact.
"""
import json
import math
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

W = 880
RADIUS = 6
SANS = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif"
MONO = "ui-monospace, SFMono-Regular, Menlo, Consolas, 'Liberation Mono', monospace"

BG, BORDER, INK, MUTED, GRID, AXIS = "#ffffff", "#d0d7de", "#1f2328", "#59636e", "#eaeef2", "#8c959f"
SERIES = ["#3a6fd8", "#23917f", "#8659c9", "#6b7785", "#c2555e"]
KINDS = {  # fill, stroke, dashed
    "process": ("#eef3fb", "#9db4d9", False),
    "store": ("#e9f5f1", "#8fc2b1", False),
    "client": ("#ffffff", "#aab3bd", True),
    "external": ("#f6f6f4", "#b3b3ab", True),
}
GROUP_FILL, GROUP_STROKE = "#f7f8fa", "#d6dbe1"
TERM_BG, TERM_BAR, TERM_INK, TERM_CMD, TERM_DIM = "#22272e", "#2d333b", "#d7dce2", "#9cc2ff", "#8b949e"


def esc(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def f(v):
    """A coordinate, written the same on every run."""
    v = round(v, 1)
    return str(int(v)) if v == int(v) else str(v)


def width_of(s, size, mono=False):
    """An estimate of a text's width: no font metrics in the standard library."""
    if mono:
        return len(s) * size * 0.6
    w = 0.0
    for c in s:
        if c in "il.,:;'|!()[]":
            w += 0.3
        elif c in "mwMW@%":
            w += 0.85
        elif c.isupper() or c.isdigit():
            w += 0.62
        elif c == " ":
            w += 0.3
        else:
            w += 0.53
    return w * size


def text(x, y, s, size=13, fill=INK, weight=400, anchor="start", mono=False, extra=""):
    return (f'<text x="{f(x)}" y="{f(y)}" font-family="{esc(MONO if mono else SANS)}" font-size="{size}" '
            f'font-weight="{weight}" fill="{fill}" text-anchor="{anchor}"{extra}>{esc(s)}</text>')


def rect(x, y, w, h, fill, stroke, dashed=False, rx=RADIUS, extra=""):
    dash = ' stroke-dasharray="4 3"' if dashed else ""
    return (f'<rect x="{f(x)}" y="{f(y)}" width="{f(w)}" height="{f(h)}" rx="{rx}" fill="{fill}" '
            f'stroke="{stroke}"{dash}{extra}/>')


def document(w, h, title, body):
    return (f'<svg xmlns="http://www.w3.org/2000/svg" width="{f(w)}" height="{f(h)}" viewBox="0 0 {f(w)} {f(h)}" '
            f'role="img"><title>{esc(title)}</title>'
            + rect(0.5, 0.5, w - 1, h - 1, BG, BORDER) + "".join(body) + "</svg>\n")


def heading(fig, w):
    """The title and the one-line subtitle; returns the body and the y below them."""
    body, y = [], 20
    if fig.get("title"):
        y += 16
        body.append(text(24, y, fig["title"], 15, INK, 600))
    if fig.get("subtitle"):
        y += 20
        body.append(text(24, y, fig["subtitle"], 12.5, MUTED))
    return body, y


def number(v, sep=","):
    """A recorded number as the data writes it, with thousands grouped."""
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        raise ValueError(f"not a number: {v!r}")
    s = repr(v) if isinstance(v, float) else str(v)
    if "e" in s:
        raise ValueError(f"write {v!r} without an exponent")
    whole, _, frac = s.partition(".")
    neg = whole.startswith("-")
    whole = whole.lstrip("-")
    if len(whole) > 3 and sep:
        whole = re.sub(r"(?<=\d)(?=(\d{3})+$)", sep, whole)
    return ("-" if neg else "") + whole + ("." + frac if frac else "")


def nice_ticks(top, count=5):
    if top <= 0:
        return [0, 1]
    raw = top / count
    mag = 10 ** math.floor(math.log10(raw))
    step = next(m * mag for m in (1, 2, 2.5, 5, 10) if m * mag >= raw)
    n = math.ceil(top / step - 1e-9)
    return [round(i * step, 10) for i in range(n + 1)]


def tick_label(v):
    return number(int(v)) if v == int(v) else number(v)


def parse(raw, sep=","):
    """A datum as a float to draw and the text to print: a string keeps the recorded digits ("6.80", "10,002")."""
    if isinstance(raw, str):
        return float(raw.replace(",", "").replace(" ", "")), raw
    return float(raw), number(raw, sep)


def value(raw, unit, sep):
    """(low, high, label) of a datum: a number or a string of one, [low, high], {"v": .., "label": ..} or null."""
    label = None
    if isinstance(raw, dict):
        label, raw = raw.get("label"), raw["v"]
    if raw is None:
        return None, None, label or "not measured"
    u = f" {unit}" if unit else ""
    if isinstance(raw, list):
        (lo, lt), (hi, ht) = parse(raw[0], sep), parse(raw[1], sep)
        return lo, hi, label or f"{lt} to {ht}{u}"
    v, t = parse(raw, sep)
    return v, v, label or f"{t}{u}"


def legend(series, x, y):
    body = []
    for i, s in enumerate(series):
        c = s.get("color", SERIES[i % len(SERIES)])
        body.append(f'<rect x="{f(x)}" y="{f(y - 9)}" width="10" height="10" rx="2" fill="{c}"/>')
        body.append(text(x + 15, y, s["name"], 12, INK))
        x += 15 + width_of(s["name"], 12) + 20
    return body


# Bars: horizontal, one band per category, one bar per series in it.
def bars(fig):
    unit, sep = fig.get("unit", ""), fig.get("sep", ",")
    cats, series = fig["categories"], fig["series"]
    for s in series:
        if len(s["values"]) != len(cats):
            raise ValueError(f"{fig['name']}: series {s['name']} has {len(s['values'])} values for {len(cats)} categories")
    body, y = heading(fig, W)
    if len(series) > 1:
        y += 24
        body += legend(series, 24, y)
    y += 18
    label_w = min(300, max(width_of(c, 12.5) for c in cats) + 20)
    x0, x1 = 24 + label_w, W - 130
    data = [[value(v, unit, sep) for v in s["values"]] for s in series]
    top = max((hi for col in data for _, hi, _ in col if hi is not None), default=1)
    cap = fig.get("max")
    ticks = nice_ticks(min(top, cap) if cap else top)
    scale_max = ticks[-1]
    bar_h, bar_gap, band_pad = 16, 3, 12
    band_h = len(series) * bar_h + (len(series) - 1) * bar_gap + 2 * band_pad
    plot_top, plot_h = y, band_h * len(cats)
    sx = lambda v: x0 + (x1 - x0) * min(v, scale_max) / scale_max
    for t in ticks:
        body.append(f'<line x1="{f(sx(t))}" y1="{f(plot_top)}" x2="{f(sx(t))}" y2="{f(plot_top + plot_h)}" '
                    f'stroke="{GRID if t else AXIS}"/>')
        body.append(text(sx(t), plot_top + plot_h + 16, tick_label(t), 11, MUTED, anchor="middle"))
    axis = fig.get("axis", unit)
    if axis:
        body.append(text(x1 + width_of(tick_label(ticks[-1]), 11) / 2 + 5, plot_top + plot_h + 16, axis, 11, MUTED))
    for ci, cat in enumerate(cats):
        by = plot_top + ci * band_h
        if ci:
            body.append(f'<line x1="24" y1="{f(by)}" x2="{f(W - 24)}" y2="{f(by)}" stroke="{GRID}"/>')
        body.append(text(24, by + band_h / 2 + 4.5, cat, 12.5, INK))
        for si, s in enumerate(series):
            c = s.get("color", SERIES[si % len(SERIES)])
            lo, hi, label = data[si][ci]
            ty = by + band_pad + si * (bar_h + bar_gap)
            if lo is None:
                body.append(text(x0 + 6, ty + 12, label, 11.5, MUTED))
                continue
            body.append(f'<rect x="{f(x0)}" y="{f(ty)}" width="{f(max(sx(lo) - x0, 1.5))}" height="{bar_h}" '
                        f'rx="2" fill="{c}"/>')
            if hi > lo:
                body.append(f'<rect x="{f(sx(lo))}" y="{f(ty)}" width="{f(sx(hi) - sx(lo))}" height="{bar_h}" '
                            f'rx="2" fill="{c}" fill-opacity="0.4"/>')
            end = sx(hi)
            if hi > scale_max:  # cut: two slanted strokes across the bar's end
                for dx in (-14, -8):
                    body.append(f'<line x1="{f(end + dx)}" y1="{f(ty + bar_h + 2)}" x2="{f(end + dx + 5)}" '
                                f'y2="{f(ty - 2)}" stroke="{BG}" stroke-width="2.5"/>')
            body.append(text(end + 6, ty + 12, label, 11.5, INK))
    h = plot_top + plot_h + 30
    if fig.get("note"):
        h += 6
        body.append(text(24, h, fig["note"], 11.5, MUTED))
        h += 14
    return document(W, h, fig.get("title", fig["name"]), body)


# Line: series over a row of x labels (categories, in order).
def line(fig):
    unit, sep = fig.get("unit", ""), fig.get("sep", ",")
    xs, series = fig["x"], fig["series"]
    marks = fig.get("marks", [""] * len(xs))
    body, y = heading(fig, W)
    if len(series) > 1 or fig.get("legend"):
        y += 24
        body += legend(series, 24, y)
    y += 22
    plot_top, plot_h = y, fig.get("height", 220)
    x0, x1 = 70, W - 40
    for s in series:
        s["parsed"] = [None if v is None else parse(v, sep) for v in s["values"]]
    vals = [v[0] for s in series for v in s["parsed"] if v is not None]
    ticks = nice_ticks(max(vals))
    sy = lambda v: plot_top + plot_h - plot_h * v / ticks[-1]
    step = (x1 - x0) / max(len(xs) - 1, 1)
    sx = lambda i: x0 + i * step if len(xs) > 1 else (x0 + x1) / 2
    for t in ticks:
        body.append(f'<line x1="{x0}" y1="{f(sy(t))}" x2="{x1}" y2="{f(sy(t))}" stroke="{GRID if t else AXIS}"/>')
        body.append(text(x0 - 8, sy(t) + 4, tick_label(t), 11, MUTED, anchor="end"))
    if fig.get("axis", unit):
        body.append(text(x0 - 8, plot_top - 10, fig.get("axis", unit), 11, MUTED, anchor="end"))
    for i, (xl, mark) in enumerate(zip(xs, marks)):
        body.append(text(sx(i), plot_top + plot_h + 18, xl, 12, INK, anchor="middle"))
        if mark:
            body.append(text(sx(i), plot_top + plot_h + 34, mark, 11, MUTED, anchor="middle"))
    for si, s in enumerate(series):
        c = s.get("color", SERIES[si % len(SERIES)])
        pts = [(sx(i), sy(v[0]), v[1]) for i, v in enumerate(s["parsed"]) if v is not None]
        body.append(f'<polyline points="{" ".join(f(px) + "," + f(py) for px, py, _ in pts)}" fill="none" '
                    f'stroke="{c}" stroke-width="2" stroke-linejoin="round"/>')
        for i, (px, py, v) in enumerate(pts):
            body.append(f'<circle cx="{f(px)}" cy="{f(py)}" r="3.5" fill="{BG}" stroke="{c}" stroke-width="2"/>')
            if fig.get("point_labels", True):
                # The highest point at an x has its label above it, the others below.
                here = [o["parsed"][i][0] for o in series if i < len(o["parsed"]) and o["parsed"][i] is not None]
                dy = -9 if s["parsed"][i][0] >= max(here) else 17
                body.append(text(px, py + dy, v, 11, c, 600, anchor="middle"))
    h = plot_top + plot_h + (48 if any(marks) else 32)
    if fig.get("note"):
        body.append(text(24, h, fig["note"], 11.5, MUTED))
        h += 18
    return document(W, h, fig.get("title", fig["name"]), body)


# Topology: layers of groups of nodes, top to bottom, and arrows between nodes or groups.
def topology(fig):
    boxes, body = {}, []
    head, y = heading(fig, W)
    y += 16
    node_h_base, item_h, pad, label_h, gap_x, gap_y = 38, 16, 12, 24, 28, fig.get("gap", 64)
    groups_svg, nodes_svg = [], []
    for layer in fig["layers"]:
        placed, layer_h = [], 0
        for g in layer["groups"]:
            nodes = g.get("nodes", [])
            nw = max([width_of(n["label"], 13, False) + 26 for n in nodes]
                     + [width_of(n.get("sub", ""), 11.5) + 26 for n in nodes]
                     + [width_of(it, 11.5) + 36 for n in nodes for it in n.get("items", [])] + [88])
            nw = g.get("node_width", nw)
            nh = node_h_base + (14 if any(n.get("sub") for n in nodes) else 0) \
                + item_h * max([len(n.get("items", [])) for n in nodes] + [0]) + (6 if any(n.get("items") for n in nodes) else 0)
            gw = max(len(nodes) * nw + (len(nodes) - 1) * 12 + 2 * pad, width_of(g.get("label", ""), 12, False) + 2 * pad)
            gh = label_h + nh + pad if g.get("label") else nh + 2 * pad
            if not g.get("label") and not g.get("frame", False):
                gw, gh = len(nodes) * nw + (len(nodes) - 1) * 12, nh
            placed.append((g, nodes, nw, nh, gw, gh))
            layer_h = max(layer_h, gh)
        total = sum(p[4] for p in placed) + gap_x * (len(placed) - 1)
        if total > W - 48:
            raise ValueError(f"{fig['name']}: a layer is {total:.0f} px wide, more than {W - 48}")
        x = (W - total) / 2
        for g, nodes, nw, nh, gw, gh in placed:
            gx = g.get("x", x)
            gy = y + (layer_h - gh) / 2
            framed = bool(g.get("label")) or g.get("frame", False)
            if framed:
                groups_svg.append(rect(gx, gy, gw, gh, GROUP_FILL, GROUP_STROKE))
                if g.get("label"):
                    groups_svg.append(text(gx + pad, gy + 17, g["label"], 12, MUTED, 600))
            boxes[g["id"]] = (gx, gy, gw, gh)
            inner = (gw - (len(nodes) * nw + (len(nodes) - 1) * 12)) / 2
            ny = gy + (label_h if g.get("label") else pad if framed else 0)
            for i, n in enumerate(nodes):
                nx = gx + inner + i * (nw + 12)
                fill, stroke, dashed = KINDS[n.get("kind", "process")]
                nodes_svg.append(rect(nx, ny, nw, nh, fill, stroke, dashed))
                ty = ny + 23
                nodes_svg.append(text(nx + nw / 2, ty, n["label"], 13, INK, 600, anchor="middle"))
                if n.get("sub"):
                    ty += 15
                    nodes_svg.append(text(nx + nw / 2, ty, n["sub"], 11.5, MUTED, anchor="middle"))
                for it in n.get("items", []):
                    ty += item_h
                    nodes_svg.append(f'<rect x="{f(nx + 12)}" y="{f(ty - 7.5)}" width="6" height="6" rx="1" fill="{stroke}"/>')
                    nodes_svg.append(text(nx + 24, ty, it, 11.5, INK))
                if n["id"] in boxes:
                    raise ValueError(f"{fig['name']}: id {n['id']} twice")
                boxes[n["id"]] = (nx, ny, nw, nh)
            x = gx + gw + gap_x
        y += layer_h + gap_y
    edges_svg, labels_svg = [], []
    for e in fig.get("edges", []):
        a, b = boxes[e["from"]], boxes[e["to"]]
        ca = (a[0] + a[2] / 2 + e.get("from_dx", 0), a[1] + a[3] / 2)
        cb = (b[0] + b[2] / 2 + e.get("to_dx", 0), b[1] + b[3] / 2)
        bend = e.get("bend", 0)
        mx, my = (ca[0] + cb[0]) / 2, (ca[1] + cb[1]) / 2
        dx, dy = cb[0] - ca[0], cb[1] - ca[1]
        ln = math.hypot(dx, dy) or 1
        cx, cy = mx - dy / ln * bend, my + dx / ln * bend
        p = clip(a, ca, (cx, cy))
        q = clip(b, cb, (cx, cy))
        style = ' stroke-dasharray="5 4"' if e.get("dashed") else ""
        ends = ' marker-end="url(#arrow)"' + (' marker-start="url(#arrow)"' if e.get("both") else "")
        if bend:
            path = f"M{f(p[0])} {f(p[1])} Q{f(cx)} {f(cy)} {f(q[0])} {f(q[1])}"
            lx, ly = (p[0] + 2 * cx + q[0]) / 4, (p[1] + 2 * cy + q[1]) / 4
        else:
            path = f"M{f(p[0])} {f(p[1])} L{f(q[0])} {f(q[1])}"
            lx, ly = (p[0] + q[0]) / 2, (p[1] + q[1]) / 2
        edges_svg.append(f'<path d="{path}" fill="none" stroke="{MUTED}" stroke-width="1.3"{style}{ends}/>')
        if e.get("label"):
            rows = e["label"].split("\n")
            lw = max(width_of(r, 11.5) for r in rows) + 12
            lh = 15 * len(rows) + 4
            lx += e.get("label_dx", 0)
            ly += e.get("label_dy", 0)
            labels_svg.append(rect(lx - lw / 2, ly - lh / 2, lw, lh, BG, GRID, rx=4))
            for r, row in enumerate(rows):
                labels_svg.append(text(lx, ly - lh / 2 + 14 + 15 * r, row, 11.5, INK, anchor="middle"))
    h = y - gap_y + 24
    if fig.get("note"):
        labels_svg.append(text(24, h, fig["note"], 11.5, MUTED))
        h += 18
    defs = (f'<defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" '
            f'orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="{MUTED}"/></marker></defs>')
    return document(W, h, fig.get("title", fig["name"]), [defs] + head + groups_svg + edges_svg + nodes_svg + labels_svg)


def clip(box, c, toward):
    """Where the line from a box's centre c toward a point leaves the box, 3 px out."""
    x, y, w, h = box
    dx, dy = toward[0] - c[0], toward[1] - c[1]
    if dx == 0 and dy == 0:
        return c
    t = min(((w / 2 + 3) / abs(dx)) if dx else math.inf, ((h / 2 + 3) / abs(dy)) if dy else math.inf)
    return c[0] + dx * t, c[1] + dy * t


ANSI = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")


# Terminal: a recorded command and its output, as a window.
def terminal(fig, base):
    raw = (base / fig["file"]).read_text() if fig.get("file") else fig["text"]
    cols = fig.get("cols", 108)
    lines = []
    for ln in ANSI.sub("", raw).rstrip("\n").split("\n"):
        ln = ln.replace("\t", "    ").rstrip()
        kind = "cmd" if ln.startswith("$ ") else "dim" if ln.startswith("#") else "out"
        while len(ln) > cols:
            lines.append((kind, ln[:cols]))
            ln, kind = "  " + ln[cols:], "out" if kind == "cmd" else kind
        lines.append((kind, ln))
    limit = fig.get("max_lines", 40)
    if len(lines) > limit:
        cut = len(lines) - limit + 1
        lines = lines[:limit - 1] + [("dim", f"[{cut} more lines]")]
    size, lh, bar = 12.5, 18, 30
    h = bar + 16 + lh * len(lines) + 12
    body = [f'<rect x="0.5" y="0.5" width="{W - 1}" height="{f(h - 1)}" rx="{RADIUS}" fill="{TERM_BG}" stroke="{BORDER}"/>',
            f'<path d="M0.5 {bar} H{W - 0.5}" stroke="{TERM_BAR}"/>']
    for i in range(3):
        body.append(f'<circle cx="{18 + i * 16}" cy="{bar / 2}" r="5" fill="{TERM_BAR}" stroke="#444c56"/>')
    if fig.get("title"):
        body.append(text(W / 2, bar / 2 + 4.5, fig["title"], 12, TERM_DIM, anchor="middle"))
    colors = {"cmd": TERM_CMD, "out": TERM_INK, "dim": TERM_DIM}
    for i, (kind, ln) in enumerate(lines):
        body.append(text(18, bar + 16 + lh * i + 10, ln, size, colors[kind], 400, mono=True,
                         extra=' xml:space="preserve"'))
    return (f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{f(h)}" viewBox="0 0 {W} {f(h)}" role="img">'
            f'<title>{esc(fig.get("title", fig["name"]))}</title>' + "".join(body) + "</svg>\n")


def render(fig, base):
    kind = fig["type"]
    if kind == "bars":
        return bars(fig)
    if kind == "line":
        return line(fig)
    if kind == "topology":
        return topology(fig)
    if kind == "terminal":
        return terminal(fig, base)
    raise ValueError(f"{fig.get('name')}: unknown type {kind!r}")


def example_dirs(args):
    if args:
        return [Path(a).resolve() for a in args]
    return sorted(p.parent.parent for p in (ROOT / "examples").glob("*/docs/figures.json"))


def main(argv):
    check = "--check" in argv
    args = [a for a in argv if a != "--check"]
    bad = 0
    for ex in example_dirs(args):
        data = ex / "docs" / "figures.json"
        if not data.exists():
            print(f"{data.relative_to(ROOT)}: no such file", file=sys.stderr)
            bad += 1
            continue
        figs = json.loads(data.read_text())["figures"]
        img = ex / "docs" / "img"
        names = set()
        for fig in figs:
            out = img / f"{fig['name']}.svg"
            names.add(out.name)
            svg = render(fig, ex)
            rel = out.relative_to(ROOT)
            if check:
                if not out.exists():
                    print(f"{rel}: missing; run python3 tools/figures/figures.py {ex.relative_to(ROOT)}")
                    bad += 1
                elif out.read_text() != svg:
                    print(f"{rel}: differs from its data; run python3 tools/figures/figures.py {ex.relative_to(ROOT)}")
                    bad += 1
            else:
                img.mkdir(parents=True, exist_ok=True)
                if not out.exists() or out.read_text() != svg:
                    out.write_text(svg)
                    print(f"wrote {rel}")
        if check:
            for extra in sorted(img.glob("*.svg")):
                if extra.name not in names:
                    print(f"{extra.relative_to(ROOT)}: no figure in {data.relative_to(ROOT)} draws it")
                    bad += 1
    if check and not bad:
        print("figures: every SVG matches its data")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
