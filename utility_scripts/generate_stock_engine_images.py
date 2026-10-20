#!/usr/bin/env python3
"""Render the paired SEMOSS catalog artwork (Pillow + NumPy).

Run from any directory: python /path/to/Semoss/utility_scripts/generate_stock_engine_images.py
Optional: --output-root DIR --preview-dir DIR
The only preview produced is <output-root>/previews/stock-engines/preview.png
(or <preview-dir>/preview.png). Existing system-app and system-project preview
sheets are maintained separately; no close-ups or catalog screenshots are made.

The 240px PNGs are supersampled for crisp 48px avatars. Numeric prefixes retain
the original ordering because the server selects by sorted filename.
Preview files are kept outside the stock directories to avoid being selected.
"""

import argparse
import math
from pathlib import Path

import numpy as np
from PIL import Image, ImageDraw, ImageFilter

SIZE = 240
SCALE = 3
RES = SIZE * SCALE
ROOT = Path(__file__).resolve().parents[1]
Y, X = np.mgrid[0:RES, 0:RES] / SCALE

# Each pair shares geometry; light-mode pigments and dark-mode highlights are
# tuned separately instead of inverting colors. Third color is a quiet accent.
PALETTES = {
    "blue": ("#2460dc", "#36bad5", "#a5edec"),
    "violet": ("#7750d3", "#ac83ef", "#d1c3fc"),
    "sky": ("#087da6", "#35b9d2", "#a9e8ed"),
    "orchid": ("#9551c2", "#df85b1", "#f2c4d8"),
    "teal": ("#147f87", "#44b9ad", "#a2e4cd"),
    "indigo": ("#5559c9", "#8599ef", "#c3d5fc"),
    "forest": ("#187b78", "#4fb2a1", "#b0dfc9"),
    "sunset": ("#b95253", "#e89873", "#f6d6a2"),
    "dusk": ("#7650b0", "#bb8ed0", "#e9c2da"),
    "azure": ("#2776c1", "#55bfcb", "#b2e6e4"),
    "iris": ("#6956cf", "#48b3cf", "#c4c1f5"),
    "wave": ("#5262cb", "#43b6bf", "#b4dfdf"),
    "gem": ("#177d9a", "#42b8b5", "#b4eadd"),
    "ribbon": ("#7953be", "#d383ad", "#efc8dd"),
    "sandstone": ("#ae6251", "#db9b6d", "#f2d7a7"),
    "leaf": ("#26795f", "#76b584", "#cfebad"),
}


def rgb(color):
    if isinstance(color, str):
        return tuple(int(color[i : i + 2], 16) for i in (1, 3, 5))
    return tuple(color)


def mix(a, b, amount):
    return tuple(round(v * (1 - amount) + w * amount) for v, w in zip(rgb(a), rgb(b)))


def gradient(a, b, vertical=True):
    t = (Y if vertical else (X + Y) / 2) / SIZE
    data = np.asarray(rgb(a)) + t[..., None] * (np.asarray(rgb(b)) - np.asarray(rgb(a)))
    return Image.fromarray(np.uint8(np.clip(data, 0, 255)))


def bezier(start, *segments):
    """Sample a chain of cubic Beziers in the 240px design coordinate space."""
    points = [start]
    p0 = np.array(start, dtype=float)
    for c1, c2, end in segments:
        p1, p2, p3 = map(lambda p: np.array(p, dtype=float), (c1, c2, end))
        for t in np.linspace(0, 1, 65)[1:]:
            p = (
                (1 - t) ** 3 * p0
                + 3 * (1 - t) ** 2 * t * p1
                + 3 * (1 - t) * t * t * p2
                + t**3 * p3
            )
            points.append(tuple(p))
        p0 = p3
    return points


def ellipse_points(cx, cy, rx, ry, rotation=0, start=0, end=360):
    rotation = math.radians(rotation)
    points = []
    for angle in np.linspace(math.radians(start), math.radians(end), 241):
        x, y = rx * math.cos(angle), ry * math.sin(angle)
        points.append(
            (
                cx + x * math.cos(rotation) - y * math.sin(rotation),
                cy + x * math.sin(rotation) + y * math.cos(rotation),
            )
        )
    return points


class Artwork:
    def __init__(self, palette, theme):
        self.dark = theme == "dark"
        self.a, self.b, self.c = map(rgb, PALETTES[palette])
        self.ink = mix(self.a, "#17203b", 0.55)
        if self.dark:
            self.a = mix(self.a, self.c, 0.28)
            self.b = mix(self.b, self.c, 0.14)
            top, bottom = mix("#111a2b", self.a, 0.08), mix("#182338", self.a, 0.13)
        else:
            top, bottom = mix("#fcfcff", self.c, 0.16), mix("#f1f4f9", self.b, 0.12)
        self.image = gradient(top, bottom, False)
        # A broad, low-contrast pool of light helps the motif sit in the tile.
        halo = np.exp(-(((X - 107) / 105) ** 2 + ((Y - 91) / 110) ** 2) * 1.7)
        mask = Image.fromarray(np.uint8(halo * (24 if self.dark else 75)))
        self.image.paste(
            Image.new("RGB", (RES, RES), self.b if self.dark else "white"), (0, 0), mask
        )

    def mask(self, kind, geometry, width=1, radius=0):
        mask = Image.new("L", (RES, RES))
        draw = ImageDraw.Draw(mask)
        if kind in ("ellipse", "rect"):
            box = tuple(round(v * SCALE) for v in geometry)
            if kind == "ellipse":
                draw.ellipse(box, fill=255)
            else:
                draw.rounded_rectangle(box, radius=round(radius * SCALE), fill=255)
        else:
            pts = [(round(x * SCALE), round(y * SCALE)) for x, y in geometry]
            if kind == "polygon":
                draw.polygon(pts, fill=255)
            else:
                draw.line(
                    pts, fill=255, width=max(1, round(width * SCALE)), joint="curve"
                )
                r = width * SCALE / 2
                for x, y in (pts[0], pts[-1]):
                    draw.ellipse((x - r, y - r, x + r, y + r), fill=255)
        return mask

    def paint(
        self, kind, geometry, color, end=None, width=1, radius=0, shadow=0, opacity=255
    ):
        mask = self.mask(kind, geometry, width, radius)
        if shadow:
            offset = Image.new("L", (RES, RES))
            offset.paste(mask, (0, round(shadow * SCALE)))
            offset = offset.filter(ImageFilter.GaussianBlur(shadow * SCALE))
            offset = offset.point(lambda v: round(v * (0.34 if self.dark else 0.16)))
            self.image.paste(Image.new("RGB", (RES, RES), self.ink), (0, 0), offset)
        if opacity != 255:
            mask = mask.point(lambda v: round(v * opacity / 255))
        fill = gradient(color, end) if end else Image.new("RGB", (RES, RES), rgb(color))
        self.image.paste(fill, (0, 0), mask)

    def line(self, points, color, width=2, **kwargs):
        self.paint("line", points, color, width=width, **kwargs)

    def poly(self, points, color, **kwargs):
        self.paint("polygon", points, color, **kwargs)

    def dot(self, x, y, radius, color, **kwargs):
        self.paint(
            "ellipse", (x - radius, y - radius, x + radius, y + radius), color, **kwargs
        )

    def rect(self, box, color, radius=10, **kwargs):
        self.paint("rect", box, color, radius=radius, **kwargs)

    def arc(self, cx, cy, rx, ry, color, width=2, **kwargs):
        args = {
            key: kwargs.pop(key)
            for key in ("rotation", "start", "end")
            if key in kwargs
        }
        self.line(ellipse_points(cx, cy, rx, ry, **args), color, width, **kwargs)

    def echo(self, radius=82):
        self.arc(120, 120, radius, radius, self.b, 0.8, opacity=24 if self.dark else 30)

    def output(self):
        return self.image.resize((SIZE, SIZE), Image.Resampling.LANCZOS)


def processor(g):
    g.echo(88)
    diamond = [(120, 58), (184, 95), (120, 132), (56, 95)]
    for dy, color in [(45, g.a), (23, g.b), (0, g.c)]:
        top = [(x, y + dy) for x, y in diamond]
        g.poly(
            [
                (56, 95 + dy),
                (120, 132 + dy),
                (184, 95 + dy),
                (184, 108 + dy),
                (120, 145 + dy),
                (56, 108 + dy),
            ],
            mix(color, g.ink, 0.24),
            shadow=5,
        )
        g.poly(top, mix(color, "#ffffff", 0.16), end=color)
        g.line(top[:3], mix(color, "#ffffff", 0.50), 1.4)
    g.poly([(120, 76), (152, 95), (120, 114), (88, 95)], g.a, end=g.b)
    g.line([(90, 95), (120, 112), (150, 95)], mix(g.c, "#ffffff", 0.4), 1.5)


def modules(g):
    g.echo()
    g.rect((55, 53, 139, 137), mix(g.a, g.c, 0.56), radius=21, shadow=6)
    g.rect((91, 90, 184, 183), g.a, radius=23, end=mix(g.a, g.ink, 0.2), shadow=7)
    g.rect((107, 105, 166, 164), g.b, radius=15, end=g.a)
    g.rect((74, 72, 118, 116), g.c, radius=12)
    g.line([(104, 99), (153, 99)], mix(g.c, "#ffffff", 0.3), 1.6, opacity=150)


def channels(g):
    g.echo(87)
    for y, cy in [(71, 81), (169, 159)]:
        curve = bezier(
            (68, y),
            ((110, y), (110, cy), (120, 120)),
            ((130, 120), (133, 120), (166, 120)),
        )
        g.line(curve, g.b, 10, shadow=3)
        g.line(curve, g.c, 2, opacity=120)
    g.rect((44, 48, 91, 95), g.b, radius=14, end=g.a, shadow=5)
    g.rect((44, 145, 91, 192), g.a, radius=14, end=g.b, shadow=5)
    g.rect((144, 90, 204, 150), g.a, radius=18, end=mix(g.a, g.ink, 0.18), shadow=5)
    g.rect((158, 104, 190, 136), g.c, radius=9, end=g.b)


def constellation(g):
    nodes = [
        (120, 55, 14),
        (176, 88, 18),
        (176, 152, 12),
        (120, 185, 17),
        (64, 152, 14),
        (64, 88, 12),
    ]
    g.line([(x, y) for x, y, _ in nodes] + [(120, 55)], g.b, 5, opacity=135)
    for x, y, _ in nodes:
        g.line([(x, y), (120, 120)], g.b, 5, opacity=175)
    for i, (x, y, r) in enumerate(nodes):
        color = [g.a, g.b, g.a, g.b, g.a, g.b][i]
        g.dot(x, y, r, mix(color, g.c, 0.25), end=color, shadow=3)
    g.dot(120, 120, 26, g.c, end=g.b, shadow=5)
    g.dot(120, 120, 11, g.a)


def orbits(g):
    g.echo(89)
    electrons = []
    # Three identical ellipses share the tile center. Rotating each by 120
    # degrees also balances three equally sized electrons around the nucleus.
    for angle in (0, 120, 240):
        orbit = ellipse_points(120, 120, 78, 30, rotation=angle)
        g.line(orbit, g.b, 4.5, opacity=200)
        # One-eighth of the way around each path keeps the electrons on their
        # orbits and inside the motif's bounds instead of shifting its outline.
        electrons.append(orbit[len(orbit) // 8])
    g.dot(120, 120, 16, g.c, end=g.b)
    for x, y in electrons:
        g.dot(x, y, 9, g.a, end=g.b)


def pinwheel(g):
    for i in range(6):
        angle = i * math.pi / 3
        local = bezier(
            (13, -10), ((36, -38), (62, -43), (75, -23)), ((90, 1), (48, 13), (13, 10))
        )
        pts = [
            (
                120 + x * math.cos(angle) - y * math.sin(angle),
                120 + x * math.sin(angle) + y * math.cos(angle),
            )
            for x, y in local
        ]
        col = mix(g.a, g.b, i / 7)
        g.poly(pts, mix(col, g.c, 0.25), end=col, shadow=3)
    g.dot(120, 120, 20, g.c, end=g.b, shadow=3)


def landscape(g, style):
    if style == "mist":
        g.dot(154, 75, 24, g.c, opacity=210)
        ridges = [
            bezier(
                (-10, 142),
                ((37, 148), (64, 73), (118, 106)),
                ((171, 139), (184, 99), (250, 113)),
            ),
            bezier(
                (-10, 156),
                ((49, 111), (74, 139), (125, 159)),
                ((174, 178), (198, 122), (250, 145)),
            ),
            bezier(
                (-10, 194),
                ((59, 155), (94, 204), (157, 187)),
                ((201, 175), (227, 177), (250, 177)),
            ),
        ]
    elif style == "sharp":
        g.dot(152, 77, 31, g.c, end=g.b)
        ridges = [
            [(-5, 159), (48, 119), (76, 139), (135, 84), (207, 154), (245, 133)],
            [(-5, 177), (62, 138), (134, 179), (187, 130), (245, 169)],
            [(-5, 205), (46, 186), (106, 208), (175, 179), (245, 199)],
        ]
    else:
        g.dot(170, 73, 22, g.c, end=g.b)
        g.poly([(29, 147), (90, 80), (148, 147)], mix(g.b, g.c, 0.25), end=g.a)
        g.poly([(88, 147), (142, 107), (192, 147)], g.b, end=g.a)
        g.poly([(90, 80), (105, 147), (148, 147)], g.a, opacity=130)
        g.poly([(29, 154), (90, 204), (148, 154)], g.b, opacity=60)
        g.poly([(88, 154), (142, 184), (192, 154)], g.a, opacity=70)
        g.line([(29, 150), (208, 150)], g.c, 2, opacity=180)
        for y, x, w in [(166, 66, 102), (180, 88, 67), (194, 99, 42)]:
            g.line([(x, y), (x + w, y)], g.b, 1.5, opacity=80)
        return
    colors = [mix(g.b, g.c, 0.25), g.a, mix(g.a, g.ink, 0.38)]
    for ridge, col in zip(ridges, colors):
        g.poly(
            ridge + [(250, 250), (-10, 250)], col, end=mix(col, g.ink, 0.10), shadow=3
        )
        g.line(ridge, mix(col, g.c, 0.42), 1.3, opacity=160)


def radar(g):
    for radius, width, start, end, col in [
        (76, 13, 142, 412, g.a),
        (52, 12, 206, 480, g.b),
        (29, 10, 282, 548, g.c),
    ]:
        g.arc(120, 120, radius, radius, col, width, opacity=22)
        g.arc(120, 120, radius, radius, col, width, start=start, end=end, shadow=3)
    g.dot(120, 120, 9, g.b)


def linked(g):
    g.echo(88)
    g.arc(93, 101, 43, 53, g.a, 17, rotation=33, shadow=5)
    g.arc(147, 139, 43, 53, g.b, 17, rotation=33, shadow=5)
    # Redraw the foreground segment for a clear over-under link.
    g.arc(93, 101, 43, 53, g.a, 17, rotation=33, start=320, end=399)
    g.arc(93, 101, 43, 53, g.c, 2, rotation=33, start=165, end=258, opacity=145)
    g.arc(147, 139, 43, 53, g.c, 2, rotation=33, start=4, end=91, opacity=180)


def waveform(g):
    g.echo(89)
    for i, height in enumerate([30, 63, 98, 142, 106, 72, 38]):
        x = 51 + 23 * i
        col = mix(g.a, g.b, i / 6)
        g.rect(
            (x - 7, 120 - height / 2, x + 7, 120 + height / 2),
            mix(col, g.c, 0.18),
            radius=7,
            end=col,
            shadow=3,
        )


def faceted_gem(g):
    g.echo(87)
    outline = [(77, 58), (163, 58), (193, 103), (120, 189), (47, 103)]
    g.poly(outline, g.a, end=mix(g.a, g.ink, 0.18), shadow=6)
    g.poly([(77, 58), (99, 103), (47, 103)], g.b, end=g.a)
    g.poly([(77, 58), (163, 58), (141, 103), (99, 103)], g.c, end=g.b)
    g.poly([(163, 58), (193, 103), (141, 103)], g.b, end=g.a)
    g.poly([(99, 103), (141, 103), (120, 189)], g.b, end=g.a)
    g.poly([(141, 103), (193, 103), (120, 189)], mix(g.a, g.ink, 0.22))
    g.line([(48, 103), (192, 103)], g.c, 1.5, opacity=170)
    g.line([(77, 59), (163, 59)], mix(g.c, "#ffffff", 0.4), 1.5)


def woven_ribbons(g):
    g.echo(88)
    angle = math.pi / 4

    def band(points, color, **kwargs):
        rotated = [
            (
                120 + x * math.cos(angle) - y * math.sin(angle),
                120 + x * math.sin(angle) + y * math.cos(angle),
            )
            for x, y in points
        ]
        g.line(rotated, color, 23, **kwargs)

    for x in (-25, 25):
        band([(x, -66), (x, 66)], g.a, shadow=4)
    for y in (-25, 25):
        band([(-66, y), (66, y)], g.b, shadow=4)
    # Alternate which ribbon passes over each intersection.
    for x, y in [(-25, -25), (25, 25)]:
        band([(x, y - 20), (x, y + 20)], g.a)


def nested_arches(g):
    for radius, top, color in [(66, 49, g.a), (42, 80, g.b), (18, 111, g.c)]:
        left, right, shoulder = 120 - radius, 120 + radius, top + radius
        path = [(left, 181), (left, shoulder)]
        path += bezier(
            (left, shoulder),
            ((left, top + radius * 0.448), (120 - radius * 0.552, top), (120, top)),
            (
                (120 + radius * 0.552, top),
                (right, top + radius * 0.448),
                (right, shoulder),
            ),
        )[1:]
        path.append((right, 181))
        g.line(path, color, 17, shadow=4)


def leaf_sprout(g):
    g.echo(87)
    stem = bezier((120, 188), ((117, 150), (130, 112), (145, 62)))
    g.line(stem, g.a, 6)
    left = bezier(
        (122, 148), ((79, 153), (48, 119), (53, 80)), ((94, 78), (129, 108), (122, 148))
    )
    right = bezier(
        (124, 148),
        ((124, 110), (156, 89), (190, 97)),
        ((185, 133), (164, 154), (124, 148)),
    )
    top = bezier(
        (133, 104),
        ((109, 86), (117, 57), (148, 40)),
        ((163, 70), (158, 94), (133, 104)),
    )
    g.poly(left, g.b, end=g.a, shadow=4)
    g.poly(right, g.c, end=g.b, shadow=4)
    g.poly(top, g.c, end=g.b, shadow=3)
    g.line(bezier((64, 93), ((79, 114), (102, 135), (120, 146))), g.c, 1.8, opacity=140)
    g.line(
        bezier((175, 109), ((159, 123), (142, 137), (126, 146))), g.a, 1.8, opacity=100
    )


DESIGNS = [
    ("01_stacked_layers", "blue", processor, "Stacked layers"),
    ("02_overlapping_tiles", "violet", modules, "Overlapping tiles"),
    ("03_connected_nodes", "sky", channels, "Connected nodes"),
    ("04_hexagonal_network", "orchid", constellation, "Hexagonal network"),
    ("05_orbital_paths", "teal", orbits, "Orbital paths"),
    ("06_pinwheel", "indigo", pinwheel, "Pinwheel"),
    ("07_rolling_hills", "forest", lambda g: landscape(g, "mist"), "Rolling hills"),
    ("08_sunset_peaks", "sunset", lambda g: landscape(g, "sharp"), "Sunset peaks"),
    (
        "09_mountain_reflection",
        "dusk",
        lambda g: landscape(g, "reflection"),
        "Mountain reflection",
    ),
    ("10_concentric_arcs", "azure", radar, "Concentric arcs"),
    ("11_interlocking_rings", "iris", linked, "Interlocking rings"),
    ("12_waveform_bars", "wave", waveform, "Waveform bars"),
    ("13_faceted_gem", "gem", faceted_gem, "Faceted gem"),
    ("14_woven_ribbons", "ribbon", woven_ribbons, "Woven ribbons"),
    ("15_nested_arches", "sandstone", nested_arches, "Nested arches"),
    ("16_leaf_sprout", "leaf", leaf_sprout, "Leaf sprout"),
]


def make_preview(images, directory):
    directory.mkdir(parents=True, exist_ok=True)
    theme_height = 60 + math.ceil(len(DESIGNS) / 6) * 190
    sheet = Image.new("RGB", (1200, theme_height * 2), "#f7f8fc")
    draw = ImageDraw.Draw(sheet)
    for theme_index, theme in enumerate(("light", "dark")):
        base_y = theme_index * theme_height
        bg, fg, muted = (
            ("#f7f8fc", "#202a3b", "#697388")
            if theme == "light"
            else ("#101724", "#e9eef8", "#98a6bd")
        )
        draw.rectangle((0, base_y, 1200, base_y + theme_height), fill=bg)
        draw.text(
            (26, base_y + 18),
            f"SEMOSS / {theme.upper()}   /   Artwork + 48px catalog size",
            fill=fg,
            font_size=16,
        )
        for i, (name, _, _, label) in enumerate(DESIGNS):
            x, y = 26 + (i % 6) * 196, base_y + 54 + (i // 6) * 190
            im = images[(theme, name)]
            sheet.paste(im.resize((132, 132), Image.Resampling.LANCZOS), (x, y))
            sheet.paste(
                im.resize((48, 48), Image.Resampling.LANCZOS), (x + 140, y + 84)
            )
            draw.text((x, y + 144), f"{i+1:02d} / {label}", fill=muted, font_size=13)
    path = directory / "preview.png"
    sheet.save(path, optimize=True)
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, default=ROOT / "images")
    parser.add_argument(
        "--preview-dir",
        type=Path,
        help="Preview directory (default: <output-root>/previews/stock-engines)",
    )
    args = parser.parse_args()
    images, total = {}, 0
    for theme in ("light", "dark"):
        directory = args.output_root / f"stock-engines-{theme}"
        directory.mkdir(parents=True, exist_ok=True)
        for name, palette, render, _ in DESIGNS:
            artwork = Artwork(palette, theme)
            render(artwork)
            im = artwork.output()
            # RGB avoids palette banding on the soft backgrounds.
            path = directory / f"{name}.png"
            im.save(path, optimize=True)
            total += path.stat().st_size
            images[(theme, name)] = im
    print(f"Rendered {len(images)} PNGs, {SIZE}×{SIZE}, {total / 1024:.1f} KiB total")
    preview_dir = args.preview_dir or args.output_root / "previews" / "stock-engines"
    print(f"Preview: {make_preview(images, preview_dir)}")


if __name__ == "__main__":
    main()
