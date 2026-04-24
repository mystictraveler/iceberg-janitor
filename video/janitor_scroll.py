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
        # Section 1: The Problem — streamers pollute the table
        # ═══════════════════════════════════════
        y1 = -SECTION
        s1_title = Text("The Small-File Problem", font_size=40, color=FG)
        s1_title.move_to([0, y1 + 3.5, 0])

        # Streamers on the left
        streamers = VGroup()
        streamer_names = ["Writer A", "Writer B", "Writer C"]
        for i, name in enumerate(streamer_names):
            sb = RoundedRectangle(width=2, height=0.55, corner_radius=0.08,
                                   color=CYAN, fill_opacity=0.2, stroke_width=2)
            sb.move_to([-5.5, y1 + 2 - i * 0.8, 0])
            sl = Text(name, font_size=10, color=CYAN)
            sl.move_to(sb.get_center())
            streamers.add(VGroup(sb, sl))

        rate_label = Text("60 commits/min", font_size=12, color=COMMENT)
        rate_label.move_to([-5.5, y1 + 0, 0])

        # Iceberg table on the right — snapshot → manifest-list → manifests → files
        table_box = RoundedRectangle(width=5, height=4.5, corner_radius=0.12,
                                      color=PURPLE, fill_opacity=0.03, stroke_width=2)
        table_box.move_to([2.5, y1 + 0.5, 0])
        table_label = Text("Iceberg Table", font_size=14, color=PURPLE)
        table_label.move_to([2.5, y1 + 3, 0])

        snap_box = RoundedRectangle(width=1.8, height=0.4, corner_radius=0.06,
                                     color=PURPLE, fill_opacity=0.3, stroke_width=1.5)
        snap_box.move_to([2.5, y1 + 2.3, 0])
        snap_t = Text("snapshot", font_size=9, color=PURPLE)
        snap_t.move_to(snap_box.get_center())

        ml_box = RoundedRectangle(width=1.8, height=0.4, corner_radius=0.06,
                                   color=PURPLE, fill_opacity=0.2, stroke_width=1.5)
        ml_box.move_to([2.5, y1 + 1.6, 0])
        ml_t = Text("manifest-list", font_size=9, color=COMMENT)
        ml_t.move_to(ml_box.get_center())

        tree_arrow = Arrow(snap_box.get_bottom(), ml_box.get_top(),
                           color=COMMENT, buff=0.04, stroke_width=1, max_tip_length_to_length_ratio=0.2)

        scroll_to(y1, 2)
        self.play(Write(s1_title), run_time=0.5)
        self.play(*[FadeIn(s) for s in streamers], Write(rate_label), run_time=0.5)
        self.play(FadeIn(table_box), Write(table_label), run_time=0.3)
        self.play(FadeIn(snap_box), Write(snap_t), run_time=0.2)
        self.play(GrowArrow(tree_arrow), FadeIn(ml_box), Write(ml_t), run_time=0.3)

        # Animate: data flies from streamers → lands as micro-manifests + micro-files
        # Each commit produces one manifest + one small file
        manifests_area = VGroup()
        files_area = VGroup()

        for burst in range(3):  # 3 bursts of commits
            batch_size = [4, 3, 5][burst]  # bursty pattern
            for j in range(batch_size):
                i = sum([4, 3, 5][:burst]) + j
                col = i % 5
                row = i // 5

                # Small data file
                df = RoundedRectangle(width=0.7, height=0.3, corner_radius=0.03,
                                       color=CYAN, fill_opacity=0.5, stroke_width=1)
                df.move_to(streamers[burst % 3][0].get_center())  # start at streamer
                target_x = 0.8 + col * 0.85
                target_y = y1 + 0.5 - row * 0.45
                df_label = Text(f"f{i+1}", font_size=6, color=FG)

                # Micro-manifest (tiny orange dot)
                mm = Circle(radius=0.08, color=ORANGE, fill_opacity=0.7, stroke_width=0)
                mm.move_to(streamers[burst % 3][0].get_center())
                mm_target_x = 1.2 + (i % 4) * 0.4
                mm_target_y = y1 + 1.1

                # Fly both from streamer → table
                self.play(
                    df.animate.move_to([target_x, target_y, 0]),
                    mm.animate.move_to([mm_target_x, mm_target_y, 0]),
                    run_time=0.15,
                    rate_func=smooth,
                )
                df_label.move_to(df.get_center())
                self.add(df_label)
                files_area.add(VGroup(df, df_label))
                manifests_area.add(mm)

            # Brief pause between bursts (bursty pattern)
            self.wait(0.2)

        # Show the pollution callout
        manifest_count = Text(f"{len(manifests_area)} micro-manifests", font_size=13, color=ORANGE)
        manifest_count.move_to([2, y1 + 0.8, 0])
        file_count = Text(f"{len(files_area)} tiny files", font_size=13, color=RED)
        file_count.move_to([2.5, y1 - 1.2, 0])
        self.play(Write(manifest_count), Write(file_count), run_time=0.4)

        # Query engine struggling
        problem = Text("Query engine scans ALL of them → SLOW",
                       font_size=16, color=RED, weight=BOLD)
        problem.move_to([0, y1 - 2.5, 0])
        self.play(Write(problem), run_time=0.5)
        self.wait(3)


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
        # Section 5: Workload Auto-Classification
        # ═══════════════════════════════════════
        y_ac = -5 * SECTION
        ac_title = Text("Workload Auto-Classification", font_size=38, color=CYAN, weight=BOLD)
        ac_title.move_to([0, y_ac + 3.5, 0])
        ac_sub = Text("The server sweeps every table, reads its commit rate, and sorts into buckets",
                       font_size=14, color=COMMENT)
        ac_sub.move_to([0, y_ac + 2.7, 0])

        # 4 classification buckets on the right
        bucket_info = [
            ("streaming",     "> 6 commits/hr",  "maintain every 5 min",  GREEN),
            ("batch",         "1-6 commits/hr",  "maintain every 1 hr",   ORANGE),
            ("slow_changing", "< 1 commit/hr",   "maintain daily",        CYAN),
            ("dormant",       "no commits 7d",   "maintain weekly",       COMMENT),
        ]
        buckets = VGroup()
        bucket_rects = []
        for i, (cls, criteria, action, color) in enumerate(bucket_info):
            bkt = RoundedRectangle(width=3.5, height=0.8, corner_radius=0.1,
                                    color=color, fill_opacity=0.1, stroke_width=2)
            bkt.move_to([3.5, y_ac + 1.5 - i * 1.0, 0])
            cls_t = Text(cls, font_size=13, color=color, weight=BOLD)
            cls_t.move_to(bkt.get_left() + RIGHT * 0.9)
            crit_t = Text(criteria, font_size=9, color=FG)
            crit_t.move_to(bkt.get_center() + RIGHT * 0.3 + UP * 0.1)
            act_t = Text(action, font_size=9, color=YELLOW)
            act_t.move_to(bkt.get_center() + RIGHT * 0.3 + DOWN * 0.12)
            buckets.add(VGroup(bkt, cls_t, crit_t, act_t))
            bucket_rects.append(bkt)

        # Unsorted tables on the left (the "sweep")
        table_names = [
            ("orders",    GREEN),    # will be streaming
            ("dim_store", COMMENT),  # will be dormant
            ("events",    GREEN),    # streaming
            ("users",     ORANGE),   # batch
            ("inventory", CYAN),     # slow_changing
            ("logs",      GREEN),    # streaming
            ("products",  COMMENT),  # dormant
            ("clicks",    ORANGE),   # batch
        ]
        # Which bucket each table goes to (index into bucket_info)
        table_bucket = [0, 3, 0, 1, 2, 0, 3, 1]

        unsorted = VGroup()
        for i, (name, color) in enumerate(table_names):
            tb = RoundedRectangle(width=1.8, height=0.4, corner_radius=0.06,
                                   color=color, fill_opacity=0.4, stroke_width=1.5)
            tb.move_to([-4.5, y_ac + 2 - i * 0.55, 0])
            tl = Text(name, font_size=9, color=FG)
            tl.move_to(tb.get_center())
            unsorted.add(VGroup(tb, tl))

        sweep_label = Text("unsorted tables", font_size=12, color=COMMENT)
        sweep_label.move_to([-4.5, y_ac + 2.5, 0])

        scroll_to(y_ac, 2)
        self.play(Write(ac_title), run_time=0.5)
        self.play(Write(ac_sub), run_time=0.3)

        # Show buckets first
        self.play(*[FadeIn(b) for b in buckets], run_time=0.5)

        # Show unsorted tables
        self.play(Write(sweep_label), run_time=0.2)
        self.play(*[FadeIn(u, shift=RIGHT * 0.2) for u in unsorted], run_time=0.4)
        self.wait(0.5)

        # Animate: each table evaluates → flies into the right bucket
        classify_label = Text("classify →", font_size=14, color=YELLOW)
        classify_label.move_to([-1, y_ac + 1.5, 0])
        self.play(Write(classify_label), run_time=0.3)

        for i, (name, color) in enumerate(table_names):
            target_bucket = bucket_rects[table_bucket[i]]
            # Table flies from left into its bucket
            self.play(
                unsorted[i].animate.move_to(target_bucket.get_center()).scale(0.6).set_opacity(0.7),
                run_time=0.25,
                rate_func=smooth,
            )

        self.wait(0.5)
        ac_key = Text("No per-table config. No knobs. The server reads commit history and decides.",
                      font_size=14, color=CYAN, weight=BOLD)
        ac_key.move_to([0, y_ac - 2.5, 0])
        self.play(Write(ac_key), run_time=0.5)
        self.wait(3)

        # ═══════════════════════════════════════
        # Section 6: Feature & Cost Comparison
        # ═══════════════════════════════════════
        y5 = -6 * SECTION
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
        # Section 7: Manifest Rewrite
        # ═══════════════════════════════════════
        y_mr = -7 * SECTION
        mr_title = Text("Manifest Rewrite", font_size=40, color=CYAN)
        mr_title.move_to([0, y_mr + 3.5, 0])

        mr_before = Text("BEFORE — 12 micro-manifests", font_size=14, color=RED)
        mr_before.move_to([-3.5, y_mr + 2.2, 0])
        micro_m = VGroup()
        for i in range(12):
            mm = RoundedRectangle(width=0.7, height=0.35, corner_radius=0.04, color=ORANGE, fill_opacity=0.5, stroke_width=1)
            mm.move_to([-5 + (i%4)*0.85, y_mr + 1.5 - (i//4)*0.5, 0])
            micro_m.add(mm)
        mr_arrow = Text("rewrite ──────▶", font_size=16, color=GREEN)
        mr_arrow.move_to([0, y_mr + 0.8, 0])
        mr_after = Text("AFTER — 3 partition-organized", font_size=14, color=GREEN)
        mr_after.move_to([3.5, y_mr + 2.2, 0])
        consol = VGroup()
        for i, pn in enumerate(["partition A", "partition B", "partition C"]):
            cm = RoundedRectangle(width=2.5, height=0.55, corner_radius=0.08, color=GREEN, fill_opacity=0.5, stroke_width=2)
            cm.move_to([3.5, y_mr + 1.5 - i*0.7, 0])
            cm_t = Text(pn, font_size=10, color=FG)
            cm_t.move_to(cm.get_center())
            consol.add(VGroup(cm, cm_t))
        mr_key = Text("Compact walks 3 manifests, not 12", font_size=14, color=YELLOW, weight=BOLD)
        mr_key.move_to([0, y_mr - 1, 0])

        scroll_to(y_mr, 2)
        self.play(Write(mr_title), run_time=0.5)
        self.play(Write(mr_before), run_time=0.3)
        self.play(*[FadeIn(m, scale=0.8) for m in micro_m], run_time=0.6)
        self.play(Write(mr_arrow), run_time=0.4)
        self.play(Write(mr_after), run_time=0.3)
        self.play(*[FadeIn(c, shift=LEFT*0.3) for c in consol], *[m.animate.set_opacity(0.1) for m in micro_m], run_time=0.8)
        self.play(Write(mr_key), run_time=0.5)
        self.wait(3)

        # ═══════════════════════════════════════
        # Section 8: Snapshot Expiration
        # ═══════════════════════════════════════
        y_se = -8 * SECTION
        se_title = Text("Snapshot Expiration", font_size=40, color=ORANGE)
        se_title.move_to([0, y_se + 3.5, 0])
        se_policy = Text("keep_last=5  keep_within=24h", font_size=14, color=YELLOW)
        se_policy.move_to([0, y_se + 2.5, 0])

        snap_chain = VGroup()
        ages = ["7d", "6d", "5d", "3d", "2d", "1d", "4h", "now"]
        for i in range(8):
            expired = i < 5
            color = RED if expired else GREEN
            op = 0.25 if expired else 0.6
            s = RoundedRectangle(width=0.9, height=1.2, corner_radius=0.06, color=color, fill_opacity=op, stroke_width=2)
            s.move_to([-5 + i*1.4, y_se + 1, 0])
            at = Text(ages[i], font_size=10, color=color, weight=BOLD)
            at.move_to(s.get_center() + DOWN*0.15)
            it = Text(f"s{i+1}", font_size=8, color=FG)
            it.move_to(s.get_center() + UP*0.25)
            snap_chain.add(VGroup(s, at, it))

        ch_arrows = VGroup()
        for i in range(7):
            a = Arrow(snap_chain[i][0].get_right(), snap_chain[i+1][0].get_left(), color=COMMENT, buff=0.05, stroke_width=1, max_tip_length_to_length_ratio=0.2)
            ch_arrows.add(a)

        scroll_to(y_se, 2)
        self.play(Write(se_title), run_time=0.5)
        self.play(Write(se_policy), run_time=0.3)
        self.play(*[FadeIn(s) for s in snap_chain], run_time=0.6)
        self.play(*[GrowArrow(a) for a in ch_arrows], run_time=0.5)
        self.wait(1)
        for i in range(5):
            x = Text("✕", font_size=18, color=RED, weight=BOLD)
            x.move_to(snap_chain[i][0].get_center())
            self.play(snap_chain[i][0].animate.set_fill(opacity=0.05), FadeIn(x, scale=1.5), run_time=0.3)
        for i in range(5, 8):
            self.play(snap_chain[i][0].animate.set_stroke(color=GREEN, width=3), run_time=0.2)
        se_result = Text("Orphaned files now eligible for GC", font_size=14, color=YELLOW, weight=BOLD)
        se_result.move_to([0, y_se - 1.5, 0])
        self.play(Write(se_result), run_time=0.5)
        self.wait(3)

        # ═══════════════════════════════════════
        # Section 9: Import from Hive
        # ═══════════════════════════════════════
        y6 = -9 * SECTION
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
        y7 = -10 * SECTION
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
        y8 = -11 * SECTION
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
