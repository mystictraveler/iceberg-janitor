"""
iceberg-janitor — Algorithm + Value Proposition Video

8 scenes, ~5-6 min total:
  1. Value Proposition
  2. The Small-File Problem
  3. The Novel Idea: Stitching Binpack + Row Group Merge
  4. V2 Delete Handling
  5. Safety Gates
  6. Framework Comparison + Cost Analysis
  7. CAS Commit Protocol
  8. Import from Hive + Perf Results

Usage:
    source /tmp/manimenv/bin/activate
    manim -pql janitor_algorithm.py FullAlgorithm   # preview
    manim -pqh janitor_algorithm.py FullAlgorithm   # HD
    manim -pql janitor_algorithm.py ValueProp        # single scene
"""

from manim import *

# === Global font: terminal-style monospace (matches Claude Code look) ===
Text.set_default(font="Monaco")

# === Palette: Dracula (zsh terminal color scheme) ===
PARQUET_BLUE = "#8be9fd"   # cyan
PARQUET_LIGHT = "#6272a4"  # comment gray-blue
STITCH_GREEN = "#50fa7b"   # green
MERGE_ORANGE = "#ffb86c"   # orange
DELETE_RED = "#ff5555"      # red
MASTER_GOLD = "#f1fa8c"    # yellow
CAS_PURPLE = "#bd93f9"     # purple
BG_DARK = "#282a36"        # background
SCHEMA_TEAL = "#8be9fd"    # cyan
GRAY_TEXT = "#6272a4"       # comment
SPARK_YELLOW = "#f1fa8c"   # yellow
FLINK_PINK = "#ff79c6"     # pink


# ─────────────────────────────────────────────────────────
# Scene 1: Value Proposition
# ─────────────────────────────────────────────────────────
class ValueProp(Scene):
    def construct(self):
        self.camera.background_color = BG_DARK

        logo = Text("iceberg-janitor", font_size=52, color=WHITE, weight=BOLD)
        tagline = Text("Catalog-less Iceberg maintenance in Go", font_size=22, color=STITCH_GREEN)
        tagline.next_to(logo, DOWN, buff=0.3)
        self.play(Write(logo, run_time=0.35))
        self.play(FadeIn(tagline, shift=UP * 0.2))
        self.wait(1)
        self.play(FadeOut(logo), FadeOut(tagline))

        # 4 pillars
        pillars = [
            ("Zero Decode\nCompaction", "Byte-copy stitch:\nno Arrow, no Spark", STITCH_GREEN),
            ("No Catalog\nRequired", "Directory catalog on\nobject storage", PARQUET_BLUE),
            ("Mandatory\nMaster Check", "I1–I8 invariants\nbefore every commit", MASTER_GOLD),
            ("11 Circuit\nBreakers", "Auto-pause on\nrunaway maintenance", DELETE_RED),
        ]
        boxes = VGroup()
        for i, (head, desc, color) in enumerate(pillars):
            box = VGroup()
            bg = RoundedRectangle(width=2.8, height=2.5, corner_radius=0.15,
                                  color=color, fill_opacity=0.12, stroke_width=2)
            h = Text(head, font_size=16, color=color, weight=BOLD)
            h.move_to(bg.get_top() + DOWN * 0.5)
            d = Text(desc, font_size=12, color=WHITE)
            d.move_to(bg.get_center() + DOWN * 0.3)
            box.add(bg, h, d)
            boxes.add(box)
        boxes.arrange(RIGHT, buff=0.3)
        boxes.move_to(UP * 0.5)

        for box in boxes:
            self.play(FadeIn(box, shift=UP * 0.3), run_time=0.3)

        # Bench headline
        headline = Text("192× file reduction · 42% faster Athena · $89/mo at 1 PB",
                        font_size=18, color=MASTER_GOLD)
        headline.to_edge(DOWN, buff=0.8)
        self.play(Write(headline))
        self.wait(1.5)
        self.wait(5)


# ─────────────────────────────────────────────────────────
# Scene 2: The Small-File Problem
# ─────────────────────────────────────────────────────────
class TheProblem(Scene):
    def construct(self):
        self.camera.background_color = BG_DARK
        title = Text("The Small-File Problem", font_size=42, color=WHITE)
        title.to_edge(UP, buff=0.5)
        self.play(Write(title))
        self.wait(1)

        # Streaming source
        source = VGroup(
            RoundedRectangle(width=2.5, height=1.2, corner_radius=0.2,
                             color=PARQUET_BLUE, fill_opacity=0.3),
            Text("Streaming\nWriter", font_size=18, color=WHITE),
        )
        source.move_to(LEFT * 4.5 + UP * 0.5)
        self.play(FadeIn(source))

        arrow = Arrow(source.get_right(), LEFT * 1 + UP * 0.5, color=WHITE, buff=0.3)
        rate = Text("60 commits/min", font_size=14, color=GRAY_TEXT)
        rate.next_to(arrow, UP, buff=0.1)
        self.play(GrowArrow(arrow), Write(rate))

        # Files accumulating
        files = VGroup()
        for row in range(5):
            for col in range(8):
                r = Rectangle(width=0.4, height=0.3, color=PARQUET_BLUE,
                              fill_opacity=0.6, stroke_width=1)
                r.move_to(RIGHT * (-0.5 + col * 0.45) + UP * (1.5 - row * 0.4))
                files.add(r)
        for wave in range(0, len(files), 8):
            self.play(*[FadeIn(f, scale=0.5) for f in files[wave:wave+8]], run_time=0.4)

        count = Text("300+ files in 5 minutes", font_size=20, color=DELETE_RED)
        count.next_to(files, DOWN, buff=0.3)
        self.play(Write(count))

        # Query engine
        query = VGroup(
            RoundedRectangle(width=2, height=1, corner_radius=0.2,
                             color=MERGE_ORANGE, fill_opacity=0.3),
            Text("Athena /\nTrino", font_size=16, color=WHITE),
        )
        query.move_to(RIGHT * 5 + DOWN * 1)
        slow = Text("scans each file → SLOW", font_size=14, color=MERGE_ORANGE)
        slow.next_to(query, UP, buff=0.2)
        self.play(FadeIn(query), Write(slow))
        self.wait(5)


# ─────────────────────────────────────────────────────────
# Scene 3: The Novel Idea — Stitching Binpack + Row Group Merge
# ─────────────────────────────────────────────────────────
class NovelIdea(Scene):
    """The core innovation: two-phase compaction with chunk-flying animation."""
    def construct(self):
        self.camera.background_color = BG_DARK

        title = Text("The Novel Idea", font_size=44, color=STITCH_GREEN)
        title.to_edge(UP, buff=0.3)
        self.play(Write(title))
        self.wait(1)

        novel = Text("Stitching Binpack + Row Group Merge", font_size=30,
                      color=MASTER_GOLD, weight=BOLD)
        novel.next_to(title, DOWN, buff=0.15)
        self.play(Write(novel))
        self.wait(1)

        p1_label = Text("PHASE 1 — Byte-Copy Stitch", font_size=20, color=STITCH_GREEN)
        p1_label.move_to(UP * 1.3)
        self.play(Write(p1_label))
        self.wait(1)

        # ── Source files: 6 small parquet files on the left ──
        NUM_FILES = 6
        file_colors = [
            "#3498db", "#2ecc71", "#e67e22",
            "#9b59b6", "#1abc9c", "#e74c3c",
        ]
        source_files = VGroup()
        source_labels = VGroup()
        for i in range(NUM_FILES):
            # Each file: an outlined rectangle with a colored fill
            # representing its row group content
            box = RoundedRectangle(
                width=1.6, height=0.55, corner_radius=0.08,
                color=file_colors[i], fill_opacity=0.6, stroke_width=2,
            )
            box.move_to(LEFT * 4.5 + UP * (0.6 - i * 0.7))
            label = Text(f"file_{i+1}.parquet", font_size=10, color=GRAY_TEXT)
            label.next_to(box, LEFT, buff=0.15)
            source_files.add(box)
            source_labels.add(label)

        self.play(
            *[FadeIn(f, shift=RIGHT * 0.3) for f in source_files],
            *[Write(l) for l in source_labels],
            run_time=0.35,
        )
        self.wait(0.3)

        # ── Output file: empty container on the right ──
        output_box = RoundedRectangle(
            width=2.2, height=4.8, corner_radius=0.12,
            color=STITCH_GREEN, fill_opacity=0.05, stroke_width=3,
        )
        output_box.move_to(RIGHT * 3.5 + DOWN * 0.3)
        output_label = Text("stitched.parquet", font_size=14, color=STITCH_GREEN)
        output_label.next_to(output_box, UP, buff=0.15)
        self.play(Create(output_box), Write(output_label))

        # ── Animate each source file's row group flying into the output ──
        byte_copy_text = Text("byte-copy →", font_size=14, color=STITCH_GREEN)
        byte_copy_text.move_to(ORIGIN + UP * 1.3 + RIGHT * 0.3)

        landed_rgs = VGroup()  # track what's in the output

        for i in range(NUM_FILES):
            src = source_files[i]

            # The row group that will fly: a copy of the source rectangle
            # scaled to fit as a row group inside the output
            rg = RoundedRectangle(
                width=1.8, height=0.55, corner_radius=0.06,
                color=file_colors[i], fill_opacity=0.65, stroke_width=1.5,
            )
            rg_label = Text(f"RG {i+1}", font_size=12, color=WHITE, weight=BOLD)

            # Target position inside the output box (stacking top-down)
            target_y = output_box.get_top()[1] - 0.5 - i * 0.7
            target_pos = np.array([output_box.get_center()[0], target_y, 0])
            rg.move_to(src.get_center())
            rg_label.move_to(target_pos)

            if i == 0:
                self.play(Write(byte_copy_text), run_time=0.4)

            # Animate: chunk flies from source to output slot
            self.play(
                rg.animate.move_to(target_pos),
                src.animate.set_fill(opacity=0.1).set_stroke(opacity=0.3),
                source_labels[i].animate.set_opacity(0.3),
                run_time=0.3,
                rate_func=smooth,
            )
            self.play(Write(rg_label), run_time=0.3)
            landed_rgs.add(VGroup(rg, rg_label))

        # Label the result
        rg_count = Text(f"{NUM_FILES} row groups — 1 per source file", font_size=13, color=GRAY_TEXT)
        rg_count.next_to(output_box, DOWN, buff=0.15)
        zero_decode = Text("ZERO decode. ZERO CPU per row. Raw bytes only.",
                           font_size=14, color=STITCH_GREEN, weight=BOLD)
        zero_decode.next_to(rg_count, DOWN, buff=0.1)
        self.play(Write(rg_count), Write(zero_decode))
        self.wait(1.5)
        self.wait(1.5)

        # ── Phase 2: Row Group Merge ──
        # Clear source side + phase 1 labels
        self.play(
            FadeOut(byte_copy_text),
            FadeOut(p1_label),
            *[FadeOut(s) for s in source_files],
            *[FadeOut(l) for l in source_labels],
            FadeOut(rg_count), FadeOut(zero_decode),
            run_time=0.3,
        )

        p2_label = Text("PHASE 2 — Row Group Merge", font_size=20, color=MERGE_ORANGE)
        p2_label.move_to(UP * 1.3)
        trigger = Text("Triggers:  RGs > 4  |  sort order  |  V2 deletes",
                        font_size=13, color=GRAY_TEXT)
        trigger.next_to(p2_label, DOWN, buff=0.1)
        self.play(Write(p2_label), Write(trigger))
        self.wait(1)

        # Animate all row groups collapsing into one
        merged_rg = RoundedRectangle(
            width=1.8, height=3.5, corner_radius=0.1,
            color=STITCH_GREEN, fill_opacity=0.7, stroke_width=3,
        )
        merged_rg.move_to(output_box.get_center())
        merged_label = Text("1 merged\nrow group", font_size=16, color=WHITE, weight=BOLD)
        merged_label.move_to(merged_rg.get_center() + UP * 0.5)
        stats_label = Text("fresh column stats\n+ optional sort", font_size=12, color=MASTER_GOLD)
        stats_label.move_to(merged_rg.get_center() + DOWN * 0.5)

        # All 6 RGs shrink + merge into one
        self.play(
            *[rg.animate.move_to(output_box.get_center()).set_opacity(0) for rg in landed_rgs],
            FadeIn(merged_rg),
            run_time=0.3,
            rate_func=smooth,
        )
        self.play(Write(merged_label), Write(stats_label))

        # WHY callout
        why_box = RoundedRectangle(
            width=11, height=1.4, corner_radius=0.12,
            color=MASTER_GOLD, fill_opacity=0.08, stroke_width=2,
        )
        why_box.to_edge(DOWN, buff=0.15)
        why_lines = VGroup(
            Text("Spark & Flink decode every row → O(rows) CPU per compaction",
                 font_size=13, color=SPARK_YELLOW),
            Text("iceberg-janitor copies bytes → O(bytes) I/O only. Merge only when needed.",
                 font_size=13, color=STITCH_GREEN),
            Text("No other tool does this two-phase shape.", font_size=14,
                 color=MASTER_GOLD, weight=BOLD),
        )
        why_lines.arrange(DOWN, buff=0.08)
        why_lines.move_to(why_box.get_center())
        self.play(FadeIn(why_box))
        for line in why_lines:
            self.play(Write(line), run_time=0.3)
        self.wait(5)


# ─────────────────────────────────────────────────────────
# Scene 4: V2 Delete Handling
# ─────────────────────────────────────────────────────────
class V2DeleteHandling(Scene):
    """Iceberg Deletes — show the problem, then how the janitor handles it."""
    def construct(self):
        self.camera.background_color = BG_DARK
        title = Text("Iceberg Delete Handling", font_size=42, color=DELETE_RED)
        title.to_edge(UP, buff=0.4)
        self.play(Write(title))
        self.wait(1)

        # ── The Problem ──
        problem_label = Text("THE PROBLEM", font_size=18, color=DELETE_RED, weight=BOLD)
        problem_label.move_to(UP * 1.5)
        self.play(Write(problem_label))

        # Data file on the left
        data_box = RoundedRectangle(width=3.5, height=3, corner_radius=0.1,
                                     color=PARQUET_BLUE, fill_opacity=0.1, stroke_width=2)
        data_box.move_to(LEFT * 3.5 + DOWN * 0.5)
        data_label = Text("data file\n(6 rows)", font_size=13, color=PARQUET_BLUE)
        data_label.next_to(data_box, UP, buff=0.1)

        rows_data = [
            ("id=1", "us", True),  ("id=2", "eu", True),
            ("id=3", "us", True),  ("id=4", "eu", True),
            ("id=5", "us", True),  ("id=6", "eu", True),
        ]
        row_objs = VGroup()
        for i, (id_val, reg, _) in enumerate(rows_data):
            bg = RoundedRectangle(width=2.8, height=0.35, corner_radius=0.05,
                                  color=PARQUET_BLUE, fill_opacity=0.5, stroke_width=1)
            bg.move_to(data_box.get_top() + DOWN * (0.5 + i * 0.42))
            t = Text(f"  {id_val}   region={reg}  ", font_size=11, color=WHITE)
            t.move_to(bg.get_center())
            row_objs.add(VGroup(bg, t))

        self.play(Create(data_box), Write(data_label),
                  *[FadeIn(r) for r in row_objs], run_time=0.3)

        # Delete files on the right — the "problem" artifacts
        del_files = VGroup()

        pos_del_box = RoundedRectangle(width=3.5, height=1, corner_radius=0.1,
                                        color=DELETE_RED, fill_opacity=0.15, stroke_width=2)
        pos_del_box.move_to(RIGHT * 3 + UP * 0.5)
        pos_del_label = Text("position delete file", font_size=12, color=DELETE_RED, weight=BOLD)
        pos_del_label.move_to(pos_del_box.get_top() + DOWN * 0.25)
        pos_del_content = Text("delete row at position 2\n→ marks id=3 for removal",
                               font_size=10, color=WHITE)
        pos_del_content.move_to(pos_del_box.get_center() + DOWN * 0.15)
        del_files.add(VGroup(pos_del_box, pos_del_label, pos_del_content))

        eq_del_box = RoundedRectangle(width=3.5, height=1, corner_radius=0.1,
                                       color=DELETE_RED, fill_opacity=0.15, stroke_width=2)
        eq_del_box.move_to(RIGHT * 3 + DOWN * 0.8)
        eq_del_label = Text("equality delete file", font_size=12, color=DELETE_RED, weight=BOLD)
        eq_del_label.move_to(eq_del_box.get_top() + DOWN * 0.25)
        eq_del_content = Text('delete all rows where region="eu"\n→ marks id=2, 4, 6',
                              font_size=10, color=WHITE)
        eq_del_content.move_to(eq_del_box.get_center() + DOWN * 0.15)
        del_files.add(VGroup(eq_del_box, eq_del_label, eq_del_content))

        self.play(*[FadeIn(d) for d in del_files], run_time=0.3)
        self.wait(0.5)

        # Problem statement
        problem = Text("Without handling: compaction resurrects deleted rows!",
                       font_size=15, color=DELETE_RED)
        problem.move_to(DOWN * 2.5)
        self.play(Write(problem))
        self.wait(1)

        # ── The Solution ──
        self.play(
            FadeOut(problem_label), FadeOut(problem),
            run_time=0.35,
        )

        solution_label = Text("THE FIX — apply deletes during the merge pass",
                              font_size=18, color=STITCH_GREEN, weight=BOLD)
        solution_label.move_to(DOWN * 2.2)
        self.play(Write(solution_label))

        # Animate deletion: rows get masked
        deleted_indices = [1, 2, 3, 5]  # id=2(eu), id=3(pos), id=4(eu), id=6(eu)
        for idx in deleted_indices:
            x_mark = Text("✕", font_size=20, color=DELETE_RED, weight=BOLD)
            x_mark.move_to(row_objs[idx][0].get_left() + LEFT * 0.25)
            self.play(
                row_objs[idx][0].animate.set_fill(DELETE_RED, 0.2),
                row_objs[idx][1].animate.set_opacity(0.3),
                FadeIn(x_mark, scale=1.5),
                run_time=0.35,
            )

        self.wait(0.5)

        # Survivors highlighted
        surviving_indices = [0, 4]  # id=1, id=5
        for idx in surviving_indices:
            self.play(
                row_objs[idx][0].animate.set_fill(STITCH_GREEN, 0.6),
                run_time=0.3,
            )

        result = Text("Output: 2 rows survive. Master check: in=6 − deleted=4 == out=2 ✓",
                      font_size=15, color=MASTER_GOLD)
        result.move_to(DOWN * 3)
        self.play(Write(result))
        self.wait(1.5)
        self.wait(5)


# ─────────────────────────────────────────────────────────
# Scene 5: Safety Gates
# ─────────────────────────────────────────────────────────
class SafetyGates(Scene):
    def construct(self):
        self.camera.background_color = BG_DARK
        title = Text("Safety Gates", font_size=42, color=MASTER_GOLD)
        title.to_edge(UP, buff=0.5)
        self.play(Write(title))
        self.wait(1)

        gates = [
            ("⏭  Schema Guard", "Mixed schemas v1+v2 → skip round", SCHEMA_TEAL),
            ("🚫  V3 DV Refusal", "Puffin deletion vectors → refuse", DELETE_RED),
            ("✓  Master Check", "I1–I8: rows, schema, stats, bounds, refs", MASTER_GOLD),
            ("⚡ 11 Circuit Breakers", "Auto-pause: loops, budget, ratio, ROI", CAS_PURPLE),
        ]
        for i, (name, desc, color) in enumerate(gates):
            box = RoundedRectangle(width=10, height=1.1, corner_radius=0.15,
                                   color=color, fill_opacity=0.1, stroke_width=2)
            box.move_to(DOWN * (i * 1.3 - 1.2))
            n = Text(name, font_size=18, color=color, weight=BOLD)
            n.move_to(box.get_left() + RIGHT * 2.5)
            d = Text(desc, font_size=14, color=WHITE)
            d.move_to(box.get_center() + RIGHT * 1.5)
            self.play(FadeIn(VGroup(box, n, d)), run_time=0.3)

        principle = Text("Refuse loudly rather than corrupt silently",
                         font_size=20, color=MASTER_GOLD)
        principle.to_edge(DOWN, buff=0.3)
        self.play(Write(principle))
        self.wait(1.5)
        self.wait(5)


# ─────────────────────────────────────────────────────────
# Scene 6: Framework Comparison + Cost Analysis
# ─────────────────────────────────────────────────────────
class CostComparison(Scene):
    """Feature + cost comparison vs Spark and Flink."""
    def construct(self):
        self.camera.background_color = BG_DARK

        # ── Part 1: Feature comparison table ──
        title = Text("Feature Comparison", font_size=42, color=WHITE)
        title.to_edge(UP, buff=0.4)
        self.play(Write(title))
        self.wait(1)

        features = [
            ("Compaction method",   "byte-copy stitch",    "Arrow decode/encode", "Arrow decode/encode"),
            ("CPU per row",         "zero",                "O(1) decode+encode",  "O(1) decode+encode"),
            ("Cold start",          "<200 ms",             "30-90 s (JVM)",       "2-5 min (Flink)"),
            ("Pre-commit check",    "mandatory I1-I8",     "none",                "none"),
            ("Catalog required",    "no",                  "yes (Spark)",         "yes (Flink)"),
            ("Schema evolution",    "guard (skip round)",  "rewrite",             "rewrite"),
        ]

        # Column headers
        col_x = [-1.5, 1.5, 4.0]
        col_colors = [STITCH_GREEN, SPARK_YELLOW, FLINK_PINK]
        col_names = ["janitor", "Spark", "Flink"]
        for i, (name, color) in enumerate(zip(col_names, col_colors)):
            h = Text(name, font_size=15, color=color, weight=BOLD)
            h.move_to(RIGHT * col_x[i] + UP * 1.6)
            self.play(Write(h), run_time=0.3)

        # Feature rows
        for ri, (feat, j, s, f) in enumerate(features):
            y = 1.0 - ri * 0.5
            fl = Text(feat, font_size=12, color=GRAY_TEXT)
            fl.move_to(LEFT * 4.5 + UP * y)
            vals = [j, s, f]
            row = VGroup(fl)
            for ci, val in enumerate(vals):
                color = col_colors[ci] if ci == 0 else WHITE
                v = Text(val, font_size=12, color=color)
                v.move_to(RIGHT * col_x[ci] + UP * y)
                row.add(v)
            self.play(*[Write(r) for r in row], run_time=0.4)

        self.wait(5)
        self.clear()
        self.camera.background_color = BG_DARK

        # ── Part 2: Cost bars ──
        title2 = Text("Cost at Scale ($/mo)", font_size=42, color=WHITE)
        title2.to_edge(UP, buff=0.4)
        self.play(Write(title2))
        self.wait(1)

        headers2 = VGroup()
        for i, (name, color) in enumerate([
            ("janitor", STITCH_GREEN),
            ("Spark EMR", SPARK_YELLOW),
            ("Flink", FLINK_PINK),
        ]):
            h = Text(name, font_size=15, color=color, weight=BOLD)
            h.move_to(RIGHT * (i * 3.5 - 3.5) + UP * 1.8)
            headers2.add(h)
        self.play(*[Write(h) for h in headers2])

        scales = [
            ("1 TB", [95, 61, 248]),
            ("100 TB", [142, 608, 960]),
            ("1 PB", [557, 5980, 2700]),
        ]
        colors = [STITCH_GREEN, SPARK_YELLOW, FLINK_PINK]

        for si, (scale_label, costs) in enumerate(scales):
            y_pos = 0.8 - si * 1.6
            label = Text(scale_label, font_size=16, color=WHITE, weight=BOLD)
            label.move_to(LEFT * 6 + UP * y_pos)
            self.play(Write(label), run_time=0.2)

            max_cost = max(costs)
            for ci, cost in enumerate(costs):
                bar_width = (cost / max_cost) * 2.5
                bar = Rectangle(width=bar_width, height=0.35,
                                color=colors[ci], fill_opacity=0.7, stroke_width=1)
                bar.move_to(RIGHT * (ci * 3.5 - 3.5) + UP * y_pos)
                cost_text = Text(f"${cost}", font_size=12, color=WHITE)
                cost_text.next_to(bar, DOWN, buff=0.05)
                self.play(GrowFromEdge(bar, LEFT), Write(cost_text), run_time=0.35)

        winner = Text("At 1 PB: 10.7× cheaper than Spark, 4.8× cheaper than Flink",
                      font_size=17, color=STITCH_GREEN, weight=BOLD)
        winner.to_edge(DOWN, buff=0.5)
        self.play(Write(winner))
        self.wait(1.5)
        self.wait(5)


# ─────────────────────────────────────────────────────────
# Scene 7: CAS Commit Protocol
# ─────────────────────────────────────────────────────────
class WorkloadClassification(Scene):
    """Auto-classification of table workloads — zero operator tuning."""
    def construct(self):
        self.camera.background_color = BG_DARK
        title = Text("Auto-Classification", font_size=42, color=SCHEMA_TEAL)
        title.to_edge(UP, buff=0.4)
        self.play(Write(title))
        self.wait(1)

        sub = Text("Zero operator tuning — the server decides what to do",
                    font_size=18, color=GRAY_TEXT)
        sub.next_to(title, DOWN, buff=0.15)
        self.play(Write(sub))

        # Commit-rate signal
        signal_label = Text("Input: foreign-commit rate over 24h", font_size=15,
                            color=WHITE)
        signal_label.move_to(UP * 1.2)
        self.play(Write(signal_label))

        # 4 classes with visual indicators
        classes = [
            ("streaming", "> 6 commits/hr", "5 min",  "hot delta-stitch",  STITCH_GREEN,  "████████████"),
            ("batch",     "1-6 commits/hr", "1 hour", "cold per-partition", MERGE_ORANGE,  "██████"),
            ("slow_changing", "< 1/hr",     "daily",  "cold triggered",    PARQUET_BLUE,  "██"),
            ("dormant",   "no commits 7d",  "weekly", "skip unless stale", GRAY_TEXT,     "▪"),
        ]

        for i, (cls, rate, cadence, mode, color, bar) in enumerate(classes):
            y = 0.3 - i * 0.8

            # Class name
            cls_text = Text(cls, font_size=18, color=color, weight=BOLD)
            cls_text.move_to(LEFT * 5 + UP * y)

            # Commit rate bar (visual)
            bar_text = Text(bar, font_size=14, color=color)
            bar_text.move_to(LEFT * 2.5 + UP * y)

            # Rate description
            rate_text = Text(rate, font_size=12, color=WHITE)
            rate_text.move_to(LEFT * 0.5 + UP * y)

            # Cadence
            cad_text = Text(f"maintain every {cadence}", font_size=12, color=GRAY_TEXT)
            cad_text.move_to(RIGHT * 2 + UP * y)

            # Mode
            mode_text = Text(mode, font_size=12, color=color)
            mode_text.move_to(RIGHT * 5 + UP * y)

            self.play(
                Write(cls_text), Write(bar_text), Write(rate_text),
                Write(cad_text), Write(mode_text),
                run_time=0.3,
            )

        # Flow diagram
        flow = Text(
            "POST /maintain → classify → plan → dispatch (hot or cold) → compact → commit",
            font_size=14, color=MASTER_GOLD,
        )
        flow.move_to(DOWN * 2.5)
        self.play(Write(flow))

        key = Text("No per-table config. No knobs. The server reads the commit history and decides.",
                   font_size=15, color=SCHEMA_TEAL, weight=BOLD)
        key.to_edge(DOWN, buff=0.3)
        self.play(Write(key))
        self.wait(1.5)
        self.wait(5)


# ─────────────────────────────────────────────────────────
# Scene 8: Manifest Rewrite
# ─────────────────────────────────────────────────────────
class ManifestRewrite(Scene):
    """Consolidate per-commit micro-manifests into partition-organized layout."""
    def construct(self):
        self.camera.background_color = BG_DARK
        title = Text("Manifest Rewrite", font_size=44, color=PARQUET_BLUE)
        title.to_edge(UP, buff=0.4)
        self.play(Write(title))
        self.wait(1)

        sub = Text("Consolidate micro-manifests so the next compact walks fewer files",
                    font_size=18, color=GRAY_TEXT)
        sub.next_to(title, DOWN, buff=0.2)
        self.play(Write(sub))
        self.wait(1)

        # ── Before: many micro-manifests ──
        before_label = Text("BEFORE", font_size=20, color=DELETE_RED, weight=BOLD)
        before_label.move_to(LEFT * 3.5 + UP * 1.2)
        self.play(Write(before_label))

        manifest_list_box = RoundedRectangle(
            width=3, height=0.6, corner_radius=0.1,
            color=PARQUET_BLUE, fill_opacity=0.2, stroke_width=2,
        )
        manifest_list_box.move_to(LEFT * 3.5 + UP * 0.5)
        ml_label = Text("manifest-list.avro", font_size=12, color=PARQUET_BLUE)
        ml_label.move_to(manifest_list_box.get_center())
        self.play(Create(manifest_list_box), Write(ml_label))

        # Many small manifests below
        micro_manifests = VGroup()
        for i in range(12):
            row = i // 4
            col = i % 4
            mm = RoundedRectangle(
                width=0.6, height=0.4, corner_radius=0.05,
                color=MERGE_ORANGE, fill_opacity=0.5, stroke_width=1,
            )
            mm.move_to(LEFT * (4.5 - col * 0.75) + DOWN * (0.2 + row * 0.55))
            micro_manifests.add(mm)

        self.play(*[FadeIn(m, scale=0.8) for m in micro_manifests], run_time=0.3)

        mm_count = Text("12 micro-manifests\n(1 per streamer commit)", font_size=13, color=MERGE_ORANGE)
        mm_count.move_to(LEFT * 3.5 + DOWN * 2.2)
        self.play(Write(mm_count))
        self.wait(1.5)

        # Problem callout
        problem = Text("Compact walks ALL of these on every attempt → slow on hot tables",
                       font_size=15, color=DELETE_RED)
        problem.move_to(DOWN * 3)
        self.play(Write(problem))
        self.wait(1.5)

        # ── Arrow: rewrite ──
        rewrite_arrow = Text("rewrite  ─────▶", font_size=18, color=STITCH_GREEN)
        rewrite_arrow.move_to(ORIGIN + UP * 0.2)
        self.play(Write(rewrite_arrow))

        # ── After: few partition-organized manifests ──
        after_label = Text("AFTER", font_size=20, color=STITCH_GREEN, weight=BOLD)
        after_label.move_to(RIGHT * 3.5 + UP * 1.2)
        self.play(Write(after_label))

        consolidated = VGroup()
        partition_names = ["partition A", "partition B", "partition C"]
        for i, pname in enumerate(partition_names):
            cm = RoundedRectangle(
                width=2.5, height=0.6, corner_radius=0.08,
                color=STITCH_GREEN, fill_opacity=0.5, stroke_width=2,
            )
            cm.move_to(RIGHT * 3.5 + DOWN * (0 + i * 0.8))
            cm_label = Text(pname, font_size=13, color=WHITE)
            cm_label.move_to(cm.get_center())
            consolidated.add(VGroup(cm, cm_label))

        self.play(
            *[FadeIn(c, shift=LEFT * 0.3) for c in consolidated],
            *[m.animate.set_opacity(0.1) for m in micro_manifests],
            run_time=0.35,
        )

        after_count = Text("3 partition-organized manifests\ncompact walks 3, not 12", font_size=13, color=STITCH_GREEN)
        after_count.move_to(RIGHT * 3.5 + DOWN * 2.2)
        self.play(Write(after_count))
        self.wait(1)

        # Result
        self.play(FadeOut(problem))
        result = Text("Manifest rewrite runs BEFORE and AFTER compact in every maintain cycle",
                      font_size=16, color=MASTER_GOLD)
        result.move_to(DOWN * 3)
        self.play(Write(result))
        self.wait(1.5)
        self.wait(5)


# ─────────────────────────────────────────────────────────
# Scene 9: Snapshot Expiration
# ─────────────────────────────────────────────────────────
class SnapshotExpiry(Scene):
    """Expire old snapshots to free orphaned metadata."""
    def construct(self):
        self.camera.background_color = BG_DARK
        title = Text("Snapshot Expiration", font_size=44, color=MERGE_ORANGE)
        title.to_edge(UP, buff=0.4)
        self.play(Write(title))
        self.wait(1)

        sub = Text("Drop old snapshots so orphaned files can be garbage-collected",
                    font_size=18, color=GRAY_TEXT)
        sub.next_to(title, DOWN, buff=0.2)
        self.play(Write(sub))
        self.wait(1)

        # Snapshot chain
        chain_label = Text("Snapshot chain (main branch)", font_size=16, color=WHITE)
        chain_label.move_to(UP * 1)
        self.play(Write(chain_label))

        snapshots = VGroup()
        snap_labels = VGroup()
        for i in range(8):
            age = ["7d", "6d", "5d", "3d", "2d", "1d", "4h", "now"][i]
            color = DELETE_RED if i < 5 else STITCH_GREEN
            opacity = 0.3 if i < 5 else 0.7

            snap = RoundedRectangle(
                width=1.0, height=1.4, corner_radius=0.08,
                color=color, fill_opacity=opacity, stroke_width=2,
            )
            snap.move_to(LEFT * 5.5 + RIGHT * i * 1.5 + UP * 0.0)
            s_id = Text(f"snap {i+1}", font_size=11, color=WHITE)
            s_id.move_to(snap.get_center() + UP * 0.3)
            s_age = Text(age, font_size=13, color=color, weight=BOLD)
            s_age.move_to(snap.get_center() + DOWN * 0.2)
            snapshots.add(snap)
            snap_labels.add(VGroup(s_id, s_age))

        self.play(
            *[FadeIn(s) for s in snapshots],
            *[Write(l) for l in snap_labels],
            run_time=0.35,
        )

        # Arrows connecting them
        for i in range(7):
            arrow = Arrow(
                snapshots[i].get_right(), snapshots[i+1].get_left(),
                color=GRAY_TEXT, buff=0.08, stroke_width=1.5,
                max_tip_length_to_length_ratio=0.15,
            )
            self.play(GrowArrow(arrow), run_time=0.3)

        self.wait(1)

        # Retention policy
        policy = Text("Policy:  keep_last=5  AND  keep_within=24h", font_size=16, color=MASTER_GOLD)
        policy.move_to(DOWN * 1.5)
        self.play(Write(policy))
        self.wait(1.5)

        # Expire the old ones
        expire_label = Text("EXPIRE", font_size=22, color=DELETE_RED, weight=BOLD)
        expire_label.move_to(DOWN * 2.2)
        self.play(Write(expire_label))

        # Old snapshots fade out with X marks
        for i in range(5):
            x_mark = Text("✕", font_size=24, color=DELETE_RED, weight=BOLD)
            x_mark.move_to(snapshots[i].get_center())
            self.play(
                snapshots[i].animate.set_fill(opacity=0.05),
                snap_labels[i].animate.set_opacity(0.2),
                FadeIn(x_mark, scale=1.5),
                run_time=0.3,
            )

        self.wait(1)

        # Surviving snapshots highlighted
        for i in range(5, 8):
            self.play(
                snapshots[i].animate.set_stroke(color=STITCH_GREEN, width=3),
                run_time=0.3,
            )

        # Result
        result = VGroup(
            Text("Orphaned metadata + data files now eligible for GC", font_size=15, color=WHITE),
            Text("Master check verifies surviving chain is consistent before commit",
                 font_size=14, color=MASTER_GOLD),
        )
        result.arrange(DOWN, buff=0.15)
        result.move_to(DOWN * 3)
        self.play(Write(result[0]))
        self.wait(1)
        self.play(Write(result[1]))
        self.wait(1.5)
        self.wait(5)


class CASCommit(Scene):
    """Atomic CAS commit — step-by-step instead of simultaneous race."""
    def construct(self):
        self.camera.background_color = BG_DARK
        title = Text("How Commits Work (No Catalog Needed)", font_size=38, color=CAS_PURPLE)
        title.to_edge(UP, buff=0.4)
        self.play(Write(title))
        self.wait(1)

        # Timeline: 4 steps shown left-to-right
        steps = [
            ("1. LOAD", "Read current\nv3.metadata.json", PARQUET_BLUE),
            ("2. COMPACT", "Stitch files,\nmaster check", STITCH_GREEN),
            ("3. WRITE", "PUT v4.metadata.json\nIf-None-Match: *", CAS_PURPLE),
            ("4. RESULT", "", MASTER_GOLD),
        ]

        step_boxes = VGroup()
        for i, (name, desc, color) in enumerate(steps):
            box = RoundedRectangle(width=2.5, height=2, corner_radius=0.1,
                                    color=color, fill_opacity=0.1, stroke_width=2)
            box.move_to(LEFT * 4.5 + RIGHT * i * 3 + UP * 0.3)
            n = Text(name, font_size=14, color=color, weight=BOLD)
            n.move_to(box.get_top() + DOWN * 0.35)
            d = Text(desc, font_size=11, color=WHITE)
            d.move_to(box.get_center() + DOWN * 0.15)
            step_boxes.add(VGroup(box, n, d))

        # Show steps 1-3 sequentially
        for i in range(3):
            self.play(FadeIn(step_boxes[i], shift=RIGHT * 0.3), run_time=0.3)
            if i < 2:
                arrow = Arrow(
                    step_boxes[i][0].get_right(),
                    step_boxes[i+1][0].get_left(),
                    color=WHITE, buff=0.15, stroke_width=2,
                )
                self.play(GrowArrow(arrow), run_time=0.4)

        # Step 3 → two outcomes
        self.wait(0.5)

        # Success path
        success_box = step_boxes[3]
        success_box[2] = Text("v4 committed!\nSnapshot advanced.", font_size=11, color=STITCH_GREEN)
        success_box[2].move_to(success_box[0].get_center() + DOWN * 0.15)
        success_icon = Text("✓", font_size=36, color=STITCH_GREEN)
        success_icon.move_to(success_box[0].get_center() + UP * 0.3)

        succ_arrow = Arrow(step_boxes[2][0].get_right(), step_boxes[3][0].get_left(),
                           color=STITCH_GREEN, buff=0.15, stroke_width=2)
        succ_label = Text("S3 accepts\n(no v4 exists)", font_size=10, color=STITCH_GREEN)
        succ_label.next_to(succ_arrow, UP, buff=0.05)

        self.play(GrowArrow(succ_arrow), Write(succ_label))
        self.play(FadeIn(step_boxes[3][0]), Write(success_icon), Write(success_box[2]))
        self.wait(1)

        # Failure path (lower row)
        fail_label = Text("BUT — if a streamer committed v4 first:", font_size=16,
                          color=MERGE_ORANGE)
        fail_label.move_to(DOWN * 1.5)
        self.play(Write(fail_label))

        fail_steps = VGroup(
            Text("S3 returns 412\nPreconditionFailed", font_size=12, color=DELETE_RED),
            Text("→", font_size=20, color=WHITE),
            Text("Reload table\n(now at v4)", font_size=12, color=PARQUET_BLUE),
            Text("→", font_size=20, color=WHITE),
            Text("Re-plan,\nmaster check", font_size=12, color=MERGE_ORANGE),
            Text("→", font_size=20, color=WHITE),
            Text("Try v5\n✓", font_size=12, color=STITCH_GREEN),
        )
        fail_steps.arrange(RIGHT, buff=0.25)
        fail_steps.move_to(DOWN * 2.5)
        self.play(*[FadeIn(s) for s in fail_steps], run_time=0.4)

        key = Text("No coordinator. No lock service. Just conditional PUT on S3.",
                   font_size=15, color=CAS_PURPLE, weight=BOLD)
        key.to_edge(DOWN, buff=0.3)
        self.play(Write(key))
        self.wait(1.5)
        self.wait(5)


# ─────────────────────────────────────────────────────────
# Scene 8: Import from Hive + Performance Results
# ─────────────────────────────────────────────────────────
class ImportAndResults(Scene):
    def construct(self):
        self.camera.background_color = BG_DARK
        title = Text("Import & Results", font_size=42, color=SCHEMA_TEAL)
        title.to_edge(UP, buff=0.5)
        self.play(Write(title))
        self.wait(1)

        # Import section
        import_title = Text("janitor-cli import", font_size=22, color=SCHEMA_TEAL, weight=BOLD)
        import_title.move_to(LEFT * 3 + UP * 1.5)
        self.play(Write(import_title))

        steps = [
            "1. Scan *.parquet under table path",
            "2. Read first file's footer → schema",
            "3. Infer partition spec from Hive paths (col=value/)",
            "4. CreateTable via directory catalog",
            "5. AddFiles registers all parquet (reads footers)",
            "6. Optional --compact: maintain pipeline runs",
        ]
        step_group = VGroup()
        for i, s in enumerate(steps):
            t = Text(s, font_size=13, color=WHITE)
            t.move_to(LEFT * 2.5 + UP * (0.7 - i * 0.4))
            step_group.add(t)
            self.play(Write(t), run_time=0.4)

        # Migration arrow
        migration = VGroup(
            Text("Hive / Legacy Parquet", font_size=14, color=MERGE_ORANGE),
            Arrow(LEFT * 0.5, RIGHT * 0.5, color=STITCH_GREEN),
            Text("Iceberg Table", font_size=14, color=STITCH_GREEN),
        )
        migration.arrange(RIGHT, buff=0.2)
        migration.move_to(LEFT * 2.5 + DOWN * 2)
        self.play(FadeIn(migration))

        # Results section (right side)
        results_title = Text("Bench Results (Run 20)", font_size=22, color=MASTER_GOLD, weight=BOLD)
        results_title.move_to(RIGHT * 3 + UP * 1.5)
        self.play(Write(results_title))

        metrics = [
            ("File reduction", "192×", STITCH_GREEN),
            ("Athena speedup", "23–27%", MASTER_GOLD),
            ("CompactHot wall", "5m 36s", PARQUET_BLUE),
            ("vs Spark output", "equivalent", SPARK_YELLOW),
            ("vs Spark compute", "6.3× less", STITCH_GREEN),
            ("Master check", "zero failures", MASTER_GOLD),
        ]
        for i, (label, value, color) in enumerate(metrics):
            l = Text(label, font_size=13, color=GRAY_TEXT)
            l.move_to(RIGHT * 2 + UP * (0.7 - i * 0.45))
            v = Text(value, font_size=15, color=color, weight=BOLD)
            v.move_to(RIGHT * 4.5 + UP * (0.7 - i * 0.45))
            self.play(Write(l), Write(v), run_time=0.35)

        self.wait(5)


# ─────────────────────────────────────────────────────────
# Combined: Full Algorithm
# ─────────────────────────────────────────────────────────
class FullAlgorithm(Scene):
    def construct(self):
        self.camera.background_color = BG_DARK

        scenes = [
            ("1", "Value Proposition", ValueProp),
            ("2", "The Small-File Problem", TheProblem),
            ("3", "Stitching Binpack +\nRow Group Merge", NovelIdea),
            ("4", "Iceberg Delete Handling", V2DeleteHandling),
            ("5", "Safety Gates", SafetyGates),
            ("6", "Workload\nClassification", WorkloadClassification),
            ("7", "Feature & Cost\nComparison", CostComparison),
            ("8", "Manifest Rewrite", ManifestRewrite),
            ("9", "Snapshot Expiration", SnapshotExpiry),
            ("10", "CAS Commit Protocol", CASCommit),
            ("11", "Import & Results", ImportAndResults),
        ]

        # Title card
        main_title = Text("iceberg-janitor", font_size=56, color=WHITE, weight=BOLD)
        main_sub = Text("How catalog-less Iceberg compaction works",
                        font_size=24, color=GRAY_TEXT)
        main_sub.next_to(main_title, DOWN, buff=0.3)
        self.play(Write(main_title, run_time=0.35))
        self.play(Write(main_sub))
        self.wait(5)
        self.play(FadeOut(main_title), FadeOut(main_sub))

        for num, name, scene_cls in scenes:
            # Chapter card
            card = VGroup(
                Text(num, font_size=72, color=STITCH_GREEN, weight=BOLD),
                Text(name, font_size=26, color=WHITE),
            )
            card[1].next_to(card[0], DOWN, buff=0.3)
            self.play(FadeIn(card, scale=0.8))
            self.wait(1)
            self.play(FadeOut(card))

            # Run the scene
            s = scene_cls()
            s.camera = self.camera
            s.construct()
            self.clear()
            self.camera.background_color = BG_DARK

        # Closing
        close = VGroup(
            Text("iceberg-janitor", font_size=48, color=WHITE, weight=BOLD),
            Text("github.com/mystictraveler/iceberg-janitor",
                 font_size=20, color=PARQUET_BLUE),
            Text("192× file reduction · 42% faster queries · $89/mo at 1 PB",
                 font_size=16, color=STITCH_GREEN),
        )
        close.arrange(DOWN, buff=0.4)
        self.play(FadeIn(close, scale=0.9))
        self.wait(5)
        self.play(FadeOut(close))
