"""
iceberg-janitor — Scrolling Explainer (camera-pan approach)

Camera pans down through vertically-stacked sections.
Each section is a self-contained visual that fades in as the camera arrives.

Usage:
    cd /Users/jp/code/iceberg-janitor/video
    /tmp/manim3/bin/manim -qh janitor_scroll.py ScrollExplainer
"""

from manim import *

Text.set_default(font="Monaco")

# Dracula
BG      = "#282a36"
CYAN    = "#8be9fd"
GREEN   = "#50fa7b"
ORANGE  = "#ffb86c"
RED     = "#ff5555"
YELLOW  = "#f1fa8c"
PURPLE  = "#bd93f9"
PINK    = "#ff79c6"
COMMENT = "#6272a4"
FG      = "#f8f8f2"

SECTION = 10  # units between sections


class ScrollExplainer(MovingCameraScene):
    def construct(self):
        self.camera.background_color = BG
        cam = self.camera.frame

        # Helper: scroll camera to a y-position
        def scroll_to(y, duration=2):
            self.play(cam.animate.move_to([0, y, 0]), run_time=duration, rate_func=smooth)

        # Helper: bordered table
        def table(headers, rows, hdr_colors, y_offset, cell_w=2.7, cell_h=0.42):
            g = VGroup()
            for ri in range(-1, len(rows)):
                for ci in range(len(headers)):
                    x = -4 + ci * cell_w + cell_w / 2
                    y = y_offset - (ri + 1) * cell_h
                    cell = Rectangle(width=cell_w, height=cell_h, color=COMMENT,
                                     fill_opacity=0.1 if ri == -1 else 0.03, stroke_width=1)
                    cell.move_to([x, y, 0])
                    if ri == -1:
                        t = Text(headers[ci], font_size=10, color=hdr_colors[ci], weight=BOLD)
                    else:
                        c = GREEN if ci == 1 else (COMMENT if ci == 0 else FG)
                        t = Text(rows[ri][ci], font_size=9, color=c)
                    t.move_to(cell.get_center())
                    g.add(cell, t)
            return g

        # ═══════════════════════════════════════
        # Section 0: Title
        # ═══════════════════════════════════════
        y0 = 0
        title = Text("iceberg-janitor", font_size=64, color=FG, weight=BOLD)
        title.move_to([0, y0 + 0.5, 0])
        subtitle = Text("Catalog-less Iceberg Compaction in Go",
                        font_size=24, color=GREEN)
        subtitle.move_to([0, y0 - 0.3, 0])
        gh_url = Text("github.com/mystictraveler/iceberg-janitor",
                      font_size=16, color=CYAN)
        gh_url.move_to([0, y0 - 1, 0])
        self.play(Write(title), run_time=1)
        self.play(FadeIn(subtitle), FadeIn(gh_url))
        self.wait(3)

        # ═══════════════════════════════════════
        # Section 1: The Problem
        # ═══════════════════════════════════════
        y1 = -SECTION
        s1_title = Text("The Small-File Problem", font_size=40, color=FG)
        s1_title.move_to([0, y1 + 3, 0])

        s1_lines = VGroup(
            Text("Streaming writers → 60 commits/min", font_size=18, color=CYAN),
            Text("→ 300+ files in 5 minutes", font_size=18, color=RED),
            Text("→ query engines scan each → SLOW", font_size=18, color=ORANGE),
        )
        s1_lines.arrange(DOWN, buff=0.4)
        s1_lines.move_to([0, y1, 0])

        s1_files = VGroup()
        for r in range(4):
            for c in range(10):
                rect = Rectangle(width=0.38, height=0.28, color=CYAN,
                                 fill_opacity=0.5, stroke_width=1)
                rect.move_to([-2 + c * 0.45, y1 - 2 + r * 0.38, 0])
                s1_files.add(rect)

        self.add(s1_title, s1_lines, s1_files)
        scroll_to(y1, 2)
        self.wait(3)

        # ═══════════════════════════════════════
        # Section 2: Byte-Copy Stitch (metadata tree + anchor)
        # ═══════════════════════════════════════
        y2 = -2 * SECTION
        s2_title = Text("Phase 1: Byte-Copy Stitch", font_size=40, color=GREEN)
        s2_title.move_to([0, y2 + 3.5, 0])

        # Metadata tree: snapshot → manifest-list → manifests → data files
        meta_snap = RoundedRectangle(width=2, height=0.5, corner_radius=0.08,
                                      color=PURPLE, fill_opacity=0.3, stroke_width=2)
        meta_snap.move_to([-4, y2 + 2.5, 0])
        meta_snap_t = Text("snapshot", font_size=10, color=PURPLE)
        meta_snap_t.move_to(meta_snap.get_center())

        meta_ml = RoundedRectangle(width=2, height=0.5, corner_radius=0.08,
                                    color=PURPLE, fill_opacity=0.2, stroke_width=2)
        meta_ml.move_to([-4, y2 + 1.8, 0])
        meta_ml_t = Text("manifest-list", font_size=10, color=PURPLE)
        meta_ml_t.move_to(meta_ml.get_center())

        meta_arrow = Arrow(meta_snap.get_bottom(), meta_ml.get_top(),
                           color=COMMENT, buff=0.05, stroke_width=1.5, max_tip_length_to_length_ratio=0.2)

        # Small files branching from manifests
        file_colors = [CYAN, GREEN, ORANGE, PURPLE, PINK, RED]
        small_files = VGroup()
        small_labels = VGroup()
        for i in range(6):
            sf = RoundedRectangle(width=1.5, height=0.4, corner_radius=0.06,
                                  color=file_colors[i], fill_opacity=0.6, stroke_width=1.5)
            sf.move_to([-4 + (i % 3) * 1.8, y2 + 0.6 - (i // 3) * 0.6, 0])
            sfl = Text(f"file_{i+1}", font_size=8, color=COMMENT)
            sfl.next_to(sf, DOWN, buff=0.05)
            small_files.add(sf)
            small_labels.add(sfl)

        # Anchor file on the right
        anchor = RoundedRectangle(width=2.5, height=4.5, corner_radius=0.12,
                                   color=GREEN, fill_opacity=0.05, stroke_width=3)
        anchor.move_to([4, y2, 0])
        anchor_label = Text("anchor\n(output)", font_size=12, color=GREEN)
        anchor_label.move_to([4, y2 + 2.8, 0])

        # Gravity arrows: small files → anchor
        gravity_arrows = VGroup()
        for sf in small_files:
            arr = Arrow(sf.get_right(), [2.5, sf.get_center()[1], 0],
                        color=GREEN, buff=0.1, stroke_width=1,
                        max_tip_length_to_length_ratio=0.15)
            gravity_arrows.add(arr)

        # Row groups inside anchor
        rgs = VGroup()
        for i in range(6):
            rg = RoundedRectangle(width=2, height=0.5, corner_radius=0.06,
                                   color=file_colors[i], fill_opacity=0.6, stroke_width=1.5)
            rg.move_to([4, y2 + 1.8 - i * 0.65, 0])
            rg_t = Text(f"RG {i+1}", font_size=11, color=FG, weight=BOLD)
            rg_t.move_to(rg.get_center())
            rgs.add(rg, rg_t)

        s2_callout = Text("Zero decode · Zero CPU per row · Raw bytes only",
                          font_size=16, color=YELLOW, weight=BOLD)
        s2_callout.move_to([0, y2 - 3, 0])

        self.add(s2_title, meta_snap, meta_snap_t, meta_ml, meta_ml_t, meta_arrow,
                 small_files, small_labels, anchor, anchor_label, gravity_arrows,
                 rgs, s2_callout)
        scroll_to(y2, 2)
        self.wait(4)

        # ═══════════════════════════════════════
        # Section 3: Row Group Merge
        # ═══════════════════════════════════════
        y3 = -3 * SECTION
        s3_title = Text("Phase 2: Row Group Merge", font_size=40, color=ORANGE)
        s3_title.move_to([0, y3 + 3.5, 0])
        s3_trigger = Text("Triggers: RGs > 4 | sort order | deletes",
                          font_size=14, color=COMMENT)
        s3_trigger.move_to([0, y3 + 2.5, 0])

        s3_before = VGroup()
        for i in range(6):
            rg = Rectangle(width=2.5, height=0.4, color=CYAN, fill_opacity=0.4, stroke_width=1)
            rg.move_to([-3, y3 + 1.5 - i * 0.5, 0])
            s3_before.add(rg)

        s3_arrow = Text("decode → sort → encode ──▶", font_size=13, color=ORANGE)
        s3_arrow.move_to([0, y3, 0])

        s3_merged = RoundedRectangle(width=2.5, height=3, corner_radius=0.1,
                                      color=GREEN, fill_opacity=0.5, stroke_width=3)
        s3_merged.move_to([4, y3 + 0.5, 0])
        s3_merged_t = Text("1 merged RG\nfresh stats\nsorted", font_size=12, color=FG)
        s3_merged_t.move_to(s3_merged.get_center())

        s3_key = Text("Spark & Flink ALWAYS decode. Janitor only when needed.",
                      font_size=15, color=YELLOW, weight=BOLD)
        s3_key.move_to([0, y3 - 2.5, 0])

        self.add(s3_title, s3_trigger, s3_before, s3_arrow, s3_merged, s3_merged_t, s3_key)
        scroll_to(y3, 2)
        self.wait(4)

        # ═══════════════════════════════════════
        # Section 4: Delete Handling
        # ═══════════════════════════════════════
        y4 = -4 * SECTION
        s4_title = Text("Iceberg Delete Handling", font_size=40, color=RED)
        s4_title.move_to([0, y4 + 3.5, 0])

        rows_data = ["id=1 us ✓", "id=2 eu ✕", "id=3 us ✕",
                     "id=4 eu ✕", "id=5 us ✓", "id=6 eu ✕"]
        s4_rows = VGroup()
        for i, rd in enumerate(rows_data):
            c = GREEN if "✓" in rd else RED
            op = 0.6 if "✓" in rd else 0.15
            rr = RoundedRectangle(width=3.5, height=0.38, corner_radius=0.05,
                                   color=c, fill_opacity=op, stroke_width=1)
            rr.move_to([-2, y4 + 2 - i * 0.5, 0])
            rt = Text(rd, font_size=12, color=FG)
            rt.move_to(rr.get_center())
            s4_rows.add(rr, rt)

        s4_del = VGroup(
            Text("pos delete: row 2", font_size=12, color=RED),
            Text("eq delete: region='eu'", font_size=12, color=RED),
        )
        s4_del.arrange(DOWN, buff=0.2)
        s4_del.move_to([4, y4 + 1.5, 0])

        s4_result = Text("Output: 2 rows. I1: in=6 − del=4 == out=2 ✓",
                         font_size=14, color=YELLOW)
        s4_result.move_to([0, y4 - 1.5, 0])

        self.add(s4_title, s4_rows, s4_del, s4_result)
        scroll_to(y4, 2)
        self.wait(3)

        # ═══════════════════════════════════════
        # Section 5: Feature & Cost Comparison
        # ═══════════════════════════════════════
        y5 = -5 * SECTION
        s5_title = Text("Feature & Cost Comparison", font_size=40, color=FG)
        s5_title.move_to([0, y5 + 3.5, 0])

        feat_t = table(
            ["", "janitor", "Spark", "Flink"],
            [
                ["Compaction",  "byte-copy",    "decode/encode", "decode/encode"],
                ["CPU/row",     "zero",         "O(1)",          "O(1)"],
                ["Cold start",  "<200 ms",      "30-90 s",       "2-5 min"],
                ["Pre-commit",  "I1-I8",        "none",          "none"],
                ["Catalog",     "no",           "yes",           "yes"],
            ],
            [COMMENT, GREEN, YELLOW, PINK],
            y5 + 2,
        )
        cost_t = table(
            ["Scale", "janitor", "Spark", "Flink"],
            [
                ["1 TB",    "$95",    "$61",     "$248"],
                ["100 TB",  "$142",   "$608",    "$960"],
                ["1 PB",    "$557",   "$5,980",  "$2,700"],
                ["10K tbl", "$4,719", "$59,800", "$27,000"],
            ],
            [COMMENT, GREEN, YELLOW, PINK],
            y5 - 0.8,
            cell_h=0.48,
        )
        s5_winner = Text("At 1 PB: 10.7× cheaper than Spark, 4.8× cheaper than Flink",
                         font_size=15, color=GREEN, weight=BOLD)
        s5_winner.move_to([0, y5 - 3.5, 0])

        self.add(s5_title, feat_t, cost_t, s5_winner)
        scroll_to(y5, 2)
        self.wait(4)

        # ═══════════════════════════════════════
        # Section 6: Import from Hive
        # ═══════════════════════════════════════
        y6 = -6 * SECTION
        s6_title = Text("Import from Hive / Legacy Parquet", font_size=40, color=CYAN)
        s6_title.move_to([0, y6 + 3.5, 0])

        s6_steps = VGroup(
            Text("1. Scan *.parquet under table path", font_size=15, color=FG),
            Text("2. Read footer → infer schema", font_size=15, color=FG),
            Text("3. Detect Hive paths (col=value/)", font_size=15, color=FG),
            Text("4. CreateTable + AddFiles → Iceberg", font_size=15, color=FG),
            Text("5. Optional --compact → maintain", font_size=15, color=FG),
        )
        s6_steps.arrange(DOWN, buff=0.35, aligned_edge=LEFT)
        s6_steps.move_to([-1, y6, 0])

        s6_arrow = VGroup(
            Text("Hive / Legacy", font_size=14, color=ORANGE),
            Text("──────▶", font_size=14, color=GREEN),
            Text("Iceberg Table", font_size=14, color=GREEN),
        )
        s6_arrow.arrange(RIGHT, buff=0.2)
        s6_arrow.move_to([0, y6 - 2.5, 0])

        self.add(s6_title, s6_steps, s6_arrow)
        scroll_to(y6, 2)
        self.wait(3)

        # ═══════════════════════════════════════
        # Section 7: Takeaways
        # ═══════════════════════════════════════
        y7 = -7 * SECTION
        s7_title = Text("Takeaways", font_size=44, color=YELLOW, weight=BOLD)
        s7_title.move_to([0, y7 + 3.5, 0])

        takeaways = [
            ("192×", "file reduction on TPC-DS streaming bench"),
            ("42%", "faster Athena queries vs uncompacted"),
            ("$557/mo", "at 1 PB — 10.7× cheaper than Spark"),
            ("Zero", "CPU per row on the byte-copy stitch path"),
            ("I1-I8", "mandatory master check — no silent corruption"),
            ("0", "catalog services required"),
        ]
        s7_items = VGroup()
        for i, (num, desc) in enumerate(takeaways):
            n = Text(num, font_size=28, color=GREEN, weight=BOLD)
            d = Text(desc, font_size=14, color=FG)
            n.move_to([-4, y7 + 2 - i * 0.85, 0])
            d.move_to([0, y7 + 2 - i * 0.85, 0])
            s7_items.add(n, d)

        self.add(s7_title, s7_items)
        scroll_to(y7, 2)
        self.wait(4)

        # ═══════════════════════════════════════
        # Section 8: Closing
        # ═══════════════════════════════════════
        y8 = -8 * SECTION
        s8 = VGroup(
            Text("iceberg-janitor", font_size=56, color=FG, weight=BOLD),
            Text("github.com/mystictraveler/iceberg-janitor",
                 font_size=18, color=CYAN),
            Text("192× · 42% faster · $557/mo at 1 PB",
                 font_size=16, color=GREEN),
        )
        s8.arrange(DOWN, buff=0.4)
        s8.move_to([0, y8, 0])
        self.add(s8)
        scroll_to(y8, 2)
        self.wait(4)
