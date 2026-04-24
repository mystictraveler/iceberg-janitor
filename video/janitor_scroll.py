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

        # Helper: bordered table (center-justified)
        def table(headers, rows, hdr_colors, y_offset, cell_w=2.7, cell_h=0.42):
            g = VGroup()
            total_w = len(headers) * cell_w
            x_start = -total_w / 2
            for ri in range(-1, len(rows)):
                for ci in range(len(headers)):
                    x = x_start + ci * cell_w + cell_w / 2
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

        scroll_to(y1, 2)
        self.play(Write(s1_title), run_time=0.5)
        self.play(*[Write(l) for l in s1_lines], run_time=0.8)
        self.play(*[FadeIn(f, scale=0.5) for f in s1_files], run_time=0.6)
        self.wait(2)


        # ═══════════════════════════════════════
        # Section 2: Stitching Binpack + Row Group Merge (combined)
        # ═══════════════════════════════════════
        y2 = -2 * SECTION
        s2_title = Text("Stitching Binpack + Row Group Merge", font_size=36, color=GREEN)
        s2_title.move_to([0, y2 + 3.5, 0])

        p1_label = Text("Phase 1 — Byte-Copy Stitch", font_size=18, color=GREEN)
        p1_label.move_to([-3, y2 + 2.5, 0])

        # Source files on the left
        file_colors = [CYAN, GREEN, ORANGE, PURPLE, PINK, RED]
        source_files = VGroup()
        source_labels = VGroup()
        for i in range(6):
            sf = RoundedRectangle(width=1.6, height=0.45, corner_radius=0.06,
                                  color=file_colors[i], fill_opacity=0.6, stroke_width=2)
            sf.move_to([-4.5, y2 + 1.5 - i * 0.6, 0])
            sfl = Text(f"file_{i+1}", font_size=8, color=COMMENT)
            sfl.next_to(sf, LEFT, buff=0.1)
            source_files.add(sf)
            source_labels.add(sfl)

        # Anchor / output container on the right
        anchor = RoundedRectangle(width=2.5, height=4.2, corner_radius=0.12,
                                   color=GREEN, fill_opacity=0.05, stroke_width=3)
        anchor.move_to([3.5, y2, 0])
        anchor_label = Text("output.parquet", font_size=11, color=GREEN)
        anchor_label.move_to([3.5, y2 + 2.5, 0])

        scroll_to(y2, 2)
        self.play(Write(s2_title), run_time=0.5)
        self.play(Write(p1_label), run_time=0.3)
        self.play(
            *[FadeIn(sf, shift=RIGHT*0.2) for sf in source_files],
            *[Write(sl) for sl in source_labels],
            run_time=0.5,
        )
        self.play(FadeIn(anchor), Write(anchor_label), run_time=0.4)

        # Animate: each file's RG FLIES from source position to anchor slot
        landed_rgs = VGroup()
        for i in range(6):
            target_y = y2 + 1.5 - i * 0.6
            rg = RoundedRectangle(width=2, height=0.42, corner_radius=0.06,
                                   color=file_colors[i], fill_opacity=0.65, stroke_width=1.5)
            rg.move_to(source_files[i].get_center())  # start at source position
            rg_label = Text(f"RG {i+1}", font_size=10, color=FG, weight=BOLD)

            # Fly the chunk from source to anchor
            self.play(
                rg.animate.move_to([3.5, target_y, 0]),
                source_files[i].animate.set_fill(opacity=0.1).set_stroke(opacity=0.3),
                source_labels[i].animate.set_opacity(0.2),
                run_time=0.4,
                rate_func=smooth,
            )
            rg_label.move_to(rg.get_center())
            self.play(FadeIn(rg_label), run_time=0.1)
            landed_rgs.add(VGroup(rg, rg_label))

        s2_callout = Text("Zero decode · Zero CPU per row · Raw bytes only",
                          font_size=15, color=YELLOW, weight=BOLD)
        s2_callout.move_to([0, y2 - 2.8, 0])
        self.play(Write(s2_callout), run_time=0.5)
        self.wait(1.5)

        # Phase 2: RGs collapse into one merged block (same viewport)
        p2_label = Text("Phase 2 — Row Group Merge", font_size=18, color=ORANGE)
        p2_label.move_to([-3, y2 - 3.5, 0])
        trigger_text = Text("(if RGs > 4 or sort order or deletes)", font_size=12, color=COMMENT)
        trigger_text.next_to(p2_label, RIGHT, buff=0.3)
        self.play(Write(p2_label), Write(trigger_text), run_time=0.4)

        merged_rg = RoundedRectangle(width=2, height=3.5, corner_radius=0.1,
                                      color=GREEN, fill_opacity=0.6, stroke_width=3)
        merged_rg.move_to([3.5, y2, 0])
        merged_label = Text("1 merged RG\nfresh stats\nsorted", font_size=11, color=FG)
        merged_label.move_to(merged_rg.get_center())

        # All 6 RGs collapse into one
        self.play(
            *[rg.animate.move_to([3.5, y2, 0]).set_opacity(0) for rg in landed_rgs],
            FadeIn(merged_rg),
            run_time=0.8,
            rate_func=smooth,
        )
        self.play(Write(merged_label), run_time=0.3)

        s2_key = Text("Spark & Flink ALWAYS decode. Janitor only when needed.",
                      font_size=14, color=YELLOW, weight=BOLD)
        s2_key.move_to([0, y2 - 4.2, 0])
        self.play(Write(s2_key), run_time=0.5)
        self.wait(3)

        # ═══════════════════════════════════════
        # Section 3: Delete Handling
        # ═══════════════════════════════════════
        y3 = -3 * SECTION
        s4_title = Text("Iceberg Delete Handling", font_size=40, color=RED)
        s4_title.move_to([0, y3 + 3.5, 0])

        rows_data = ["id=1 us ✓", "id=2 eu ✕", "id=3 us ✕",
                     "id=4 eu ✕", "id=5 us ✓", "id=6 eu ✕"]
        s4_rows = VGroup()
        for i, rd in enumerate(rows_data):
            c = GREEN if "✓" in rd else RED
            op = 0.6 if "✓" in rd else 0.15
            rr = RoundedRectangle(width=3.5, height=0.38, corner_radius=0.05,
                                   color=c, fill_opacity=op, stroke_width=1)
            rr.move_to([-2, y3 + 2 - i * 0.5, 0])
            rt = Text(rd, font_size=12, color=FG)
            rt.move_to(rr.get_center())
            s4_rows.add(rr, rt)

        s4_del = VGroup(
            Text("pos delete: row 2", font_size=12, color=RED),
            Text("eq delete: region='eu'", font_size=12, color=RED),
        )
        s4_del.arrange(DOWN, buff=0.2)
        s4_del.move_to([4, y3 + 1.5, 0])

        s4_result = Text("Output: 2 rows. I1: in=6 − del=4 == out=2 ✓",
                         font_size=14, color=YELLOW)
        s4_result.move_to([0, y3 - 1.5, 0])

        scroll_to(y3, 2)
        self.play(Write(s4_title), run_time=0.5)
        for i in range(0, len(s4_rows), 2):
            self.play(FadeIn(s4_rows[i]), Write(s4_rows[i+1]), run_time=0.25)
        self.play(*[Write(d) for d in s4_del], run_time=0.5)
        self.play(Write(s4_result), run_time=0.5)
        self.wait(2)

        # ═══════════════════════════════════════
        # Section 4: Safety Gates
        # ═══════════════════════════════════════
        y4s = -4 * SECTION
        s5s_title = Text("Safety Gates", font_size=40, color=YELLOW, weight=BOLD)
        s5s_title.move_to([0, y4s + 3.5, 0])

        gates = [
            ("⏭", "Schema Guard",     "Mixed schemas → skip round",         CYAN),
            ("🚫", "V3 DV Refusal",    "Puffin deletes → refuse",            RED),
            ("✓", "Master Check",      "I1-I8 invariants before commit",     YELLOW),
            ("⚡", "11 Circuit Breakers","Loop, budget, ratio, ROI → pause",  PURPLE),
        ]
        gate_objs = VGroup()
        for i, (icon, name, desc, color) in enumerate(gates):
            bg = RoundedRectangle(width=9, height=0.7, corner_radius=0.1,
                                  color=color, fill_opacity=0.08, stroke_width=2)
            bg.move_to([0, y4s + 2 - i * 0.9, 0])
            ic = Text(icon, font_size=20)
            ic.move_to(bg.get_left() + RIGHT * 0.6)
            nm = Text(name, font_size=14, color=color, weight=BOLD)
            nm.move_to(bg.get_left() + RIGHT * 3)
            ds = Text(desc, font_size=12, color=FG)
            ds.move_to(bg.get_center() + RIGHT * 1.5)
            gate_objs.add(VGroup(bg, ic, nm, ds))

        s5s_principle = Text("Refuse loudly rather than corrupt silently",
                             font_size=17, color=YELLOW, weight=BOLD)
        s5s_principle.move_to([0, y4s - 2, 0])

        # CAS commit animation below the gates
        cas_title = Text("Safety (continued) — Atomic CAS Commit", font_size=18, color=PURPLE, weight=BOLD)
        cas_title.move_to([0, y4s - 3, 0])

        cas_steps = VGroup(
            RoundedRectangle(width=2, height=0.5, corner_radius=0.06,
                             color=CYAN, fill_opacity=0.2, stroke_width=1.5),
            RoundedRectangle(width=2, height=0.5, corner_radius=0.06,
                             color=GREEN, fill_opacity=0.2, stroke_width=1.5),
            RoundedRectangle(width=2, height=0.5, corner_radius=0.06,
                             color=PURPLE, fill_opacity=0.2, stroke_width=1.5),
        )
        cas_labels = VGroup(
            Text("1. LOAD", font_size=10, color=CYAN),
            Text("2. COMPACT", font_size=10, color=GREEN),
            Text("3. CAS PUT", font_size=10, color=PURPLE),
        )
        for i in range(3):
            cas_steps[i].move_to([-3 + i * 3, y4s - 3.8, 0])
            cas_labels[i].move_to(cas_steps[i].get_center())

        cas_arrows = VGroup()
        for i in range(2):
            a = Arrow(cas_steps[i].get_right(), cas_steps[i+1].get_left(),
                      color=COMMENT, buff=0.1, stroke_width=1.5, max_tip_length_to_length_ratio=0.2)
            cas_arrows.add(a)

        cas_retry = Text("412? → reload → re-plan → retry", font_size=12, color=ORANGE)
        cas_retry.move_to([0, y4s - 4.5, 0])
        cas_nolock = Text("No lock service. Just conditional PUT on S3.",
                          font_size=13, color=PURPLE)
        cas_nolock.move_to([0, y4s - 5, 0])

        scroll_to(y4s, 2)
        self.play(Write(s5s_title), run_time=0.5)
        for g in gate_objs:
            self.play(FadeIn(g, shift=RIGHT * 0.3), run_time=0.4)
        self.play(Write(s5s_principle), run_time=0.5)
        self.wait(2)

        # Scroll down to CAS section (Safety continued)
        scroll_to(y4s - 4, 1.5)
        self.play(Write(cas_title), run_time=0.4)
        for i in range(3):
            self.play(FadeIn(cas_steps[i]), Write(cas_labels[i]), run_time=0.3)
            if i < 2:
                self.play(GrowArrow(cas_arrows[i]), run_time=0.2)
        self.play(Write(cas_retry), run_time=0.4)
        self.play(Write(cas_nolock), run_time=0.4)
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

        scroll_to(y5, 2)
        self.play(Write(s5_title), run_time=0.5)
        self.play(FadeIn(feat_t), run_time=1.0)
        self.wait(2)
        self.play(FadeIn(cost_t), run_time=1.0)
        self.play(Write(s5_winner), run_time=0.5)
        self.wait(3)

        # ═══════════════════════════════════════
        # Section 5: Import from Hive
        # ═══════════════════════════════════════
        y6 = -6 * SECTION
        s6_title = Text("Import from Hive / Legacy Parquet", font_size=40, color=CYAN)
        s6_title.move_to([0, y6 + 3.5, 0])

        # Hive files on the left (orange = legacy)
        hive_files = VGroup()
        hive_paths = ["date=2026-04-01/", "date=2026-04-02/", "date=2026-04-03/"]
        for i, hp in enumerate(hive_paths):
            hf = RoundedRectangle(width=3, height=0.5, corner_radius=0.06,
                                   color=ORANGE, fill_opacity=0.5, stroke_width=2)
            hf.move_to([-4, y6 + 1.5 - i * 0.7, 0])
            hl = Text(f"{hp}*.parquet", font_size=10, color=FG)
            hl.move_to(hf.get_center())
            hive_files.add(VGroup(hf, hl))

        # Arrow
        import_arrow = Text("janitor-cli import ──▶", font_size=16, color=GREEN)
        import_arrow.move_to([0, y6, 0])

        # Iceberg table on the right (green = Iceberg)
        ice_box = RoundedRectangle(width=3, height=3, corner_radius=0.12,
                                    color=GREEN, fill_opacity=0.08, stroke_width=3)
        ice_box.move_to([4, y6, 0])
        ice_parts = VGroup(
            Text("metadata/", font_size=11, color=PURPLE),
            Text("  v1.metadata.json", font_size=10, color=COMMENT),
            Text("  manifest-list.avro", font_size=10, color=COMMENT),
            Text("data/", font_size=11, color=GREEN),
            Text("  (same parquet files)", font_size=10, color=COMMENT),
        )
        ice_parts.arrange(DOWN, buff=0.15, aligned_edge=LEFT)
        ice_parts.move_to(ice_box.get_center())

        s6_compact = Text("+ optional --compact → immediate optimization",
                          font_size=14, color=YELLOW)
        s6_compact.move_to([0, y6 - 2.5, 0])

        scroll_to(y6, 2)
        self.play(Write(s6_title), run_time=0.5)
        # Hive files appear
        for hf in hive_files:
            self.play(FadeIn(hf, shift=RIGHT * 0.2), run_time=0.3)
        self.wait(0.5)
        # Arrow
        self.play(Write(import_arrow), run_time=0.5)
        # Iceberg table builds
        self.play(Create(ice_box), run_time=0.4)
        for part in ice_parts:
            self.play(Write(part), run_time=0.2)
        # Hive files dim (they stay in place — data doesn't move)
        self.play(
            *[hf[0].animate.set_stroke(color=GREEN) for hf in hive_files],
            run_time=0.5,
        )
        self.play(Write(s6_compact), run_time=0.4)
        self.wait(2)

        # ═══════════════════════════════════════
        # Section 6: Takeaways
        # ═══════════════════════════════════════
        y7 = -7 * SECTION
        s7_title = Text("Takeaways", font_size=44, color=YELLOW, weight=BOLD)
        s7_title.move_to([0, y7 + 3.5, 0])

        takeaways = [
            ("192×", "fewer files after compaction"),
            ("42%", "faster query latency"),
            ("0", "rows decoded on the stitch fast path"),
            ("10.7×", "cheaper than Spark at 1 PB"),
            ("4.8×", "cheaper than Flink at 1 PB"),
            ("$557", "per month for 1,000 tables"),
            ("8", "mandatory invariants before every commit"),
            ("0", "catalog services required"),
        ]
        s7_items = VGroup()
        for i, (num, desc) in enumerate(takeaways):
            n = Text(num, font_size=28, color=GREEN, weight=BOLD)
            d = Text(desc, font_size=14, color=FG)
            n.move_to([-4, y7 + 2 - i * 0.85, 0])
            d.move_to([0, y7 + 2 - i * 0.85, 0])
            s7_items.add(n, d)

        scroll_to(y7, 2)
        self.play(Write(s7_title), run_time=0.5)
        for i in range(0, len(s7_items), 2):
            self.play(Write(s7_items[i]), Write(s7_items[i+1]), run_time=0.4)
        self.wait(3)

        # ═══════════════════════════════════════
        # Section 7: Closing
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
        scroll_to(y8, 2)
        self.play(FadeIn(s8, scale=0.9), run_time=1.0)
        self.wait(4)
