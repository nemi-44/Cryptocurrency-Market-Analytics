"""Generate the three required performance figures as portable PNG files."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Sequence


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _font(size: int):
    from PIL import ImageFont

    try:
        return ImageFont.truetype("DejaVuSans.ttf", size)
    except OSError:
        return ImageFont.load_default()


def _format_tick(value: float) -> str:
    if abs(value) >= 1000:
        return f"{value:,.0f}"
    if abs(value) >= 10:
        return f"{value:.0f}"
    return f"{value:.2f}".rstrip("0").rstrip(".")


def _draw_chart(
    *,
    x_values: list[float],
    y_values: list[float],
    x_label: str,
    y_label: str,
    title: str,
    output: Path,
) -> None:
    from PIL import Image, ImageDraw

    width, height = 1280, 720
    left, right, top, bottom = 130, 50, 90, 105
    plot_width = width - left - right
    plot_height = height - top - bottom
    image = Image.new("RGB", (width, height), "#f8fafc")
    draw = ImageDraw.Draw(image)
    title_font = _font(30)
    label_font = _font(20)
    tick_font = _font(17)

    x_min, x_max = min(x_values), max(x_values)
    y_min, y_max = min(0.0, min(y_values)), max(y_values)
    if x_min == x_max:
        x_max = x_min + 1
    if y_min == y_max:
        y_max = y_min + 1
    y_padding = (y_max - y_min) * 0.10
    y_max += y_padding

    def point(x_value: float, y_value: float) -> tuple[float, float]:
        x = left + ((x_value - x_min) / (x_max - x_min)) * plot_width
        y = top + plot_height - ((y_value - y_min) / (y_max - y_min)) * plot_height
        return x, y

    for index in range(6):
        fraction = index / 5
        y = top + plot_height - (fraction * plot_height)
        value = y_min + fraction * (y_max - y_min)
        draw.line((left, y, width - right, y), fill="#dbe4ee", width=1)
        label = _format_tick(value)
        box = draw.textbbox((0, 0), label, font=tick_font)
        draw.text((left - 15 - (box[2] - box[0]), y - 10), label, fill="#475569", font=tick_font)

    if len(x_values) <= 8:
        tick_x_values = x_values
    else:
        tick_indexes = {
            round(index * (len(x_values) - 1) / 7)
            for index in range(8)
        }
        tick_x_values = [x_values[index] for index in sorted(tick_indexes)]

    for x_value in tick_x_values:
        x, _ = point(x_value, y_min)
        draw.line((x, top, x, top + plot_height), fill="#edf2f7", width=1)
        label = _format_tick(x_value)
        box = draw.textbbox((0, 0), label, font=tick_font)
        draw.text((x - (box[2] - box[0]) / 2, top + plot_height + 12), label, fill="#475569", font=tick_font)

    draw.line((left, top, left, top + plot_height), fill="#334155", width=2)
    draw.line((left, top + plot_height, width - right, top + plot_height), fill="#334155", width=2)
    points = [point(x, y) for x, y in zip(x_values, y_values)]
    if len(points) > 1:
        draw.line(points, fill="#0369a1", width=5, joint="curve")
    for x, y in points:
        draw.ellipse((x - 7, y - 7, x + 7, y + 7), fill="#0ea5e9", outline="#075985", width=2)

    title_box = draw.textbbox((0, 0), title, font=title_font)
    draw.text(((width - (title_box[2] - title_box[0])) / 2, 28), title, fill="#0f172a", font=title_font)
    x_box = draw.textbbox((0, 0), x_label, font=label_font)
    draw.text(((width - (x_box[2] - x_box[0])) / 2, height - 47), x_label, fill="#334155", font=label_font)

    # Pillow text rotation keeps the y-axis label readable without extra dependencies.
    label_image = Image.new("RGBA", (plot_height, 40), (255, 255, 255, 0))
    label_draw = ImageDraw.Draw(label_image)
    label_draw.text((0, 5), y_label, fill="#334155", font=label_font)
    rotated = label_image.rotate(90, expand=True)
    image.paste(rotated, (20, top + (plot_height - rotated.height) // 2), rotated)

    output.parent.mkdir(parents=True, exist_ok=True)
    image.save(output, format="PNG", optimize=True)


def plot_metrics(batch_csv: Path, speed_csv: Path, output_dir: Path) -> list[Path]:
    batch = read_rows(batch_csv)
    speed = read_rows(speed_csv)
    figures = [
        (
            [float(row["worker_count"]) for row in batch],
            [float(row["speedup"]) for row in batch],
            "Worker count",
            "Speedup",
            "Local synthetic batch speedup vs worker count",
            "batch_speedup.png",
        ),
        (
            [float(row["target_ingestion_rate"]) for row in speed],
            [float(row["p95_processing_latency_ms"]) for row in speed],
            "Target ingestion rate (records/s)",
            "p95 processing latency (ms)",
            "Local controlled-load speed-layer latency",
            "latency_vs_ingestion_rate.png",
        ),
        (
            [float(row["target_ingestion_rate"]) for row in speed],
            [float(row["achieved_throughput"]) for row in speed],
            "Target ingestion rate (records/s)",
            "Achieved throughput (records/s)",
            "Local controlled-load speed-layer throughput",
            "throughput_vs_ingestion_rate.png",
        ),
    ]
    outputs: list[Path] = []
    for x_values, y_values, x_label, y_label, title, filename in figures:
        output = output_dir / filename
        _draw_chart(
            x_values=x_values,
            y_values=y_values,
            x_label=x_label,
            y_label=y_label,
            title=title,
            output=output,
        )
        outputs.append(output)
    return outputs


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Plot batch and speed benchmark CSV files.")
    parser.add_argument("--batch-csv", type=Path, default=Path("results/batch_benchmark.csv"))
    parser.add_argument("--speed-csv", type=Path, default=Path("results/speed_benchmark.csv"))
    parser.add_argument("--output-dir", type=Path, default=Path("results/figures"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    outputs = plot_metrics(args.batch_csv, args.speed_csv, args.output_dir)
    print("\n".join(str(path) for path in outputs))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
