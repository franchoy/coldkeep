#!/usr/bin/env python3

import argparse
import json
import math
import re
from collections import defaultdict
from pathlib import Path


FILE_RE = re.compile(r"^phase8-(?P<dataset>[a-z0-9_]+)-w(?P<workers>\d+)-r(?P<repeat>\d+)-(?P<size_mib>\d+)m\.json$")


def mean(values):
    return sum(values) / len(values) if values else math.nan


def pct_delta(current, baseline):
    if baseline == 0:
        return math.nan
    return ((current - baseline) / baseline) * 100.0


def parse_args():
    parser = argparse.ArgumentParser(description="Summarize Phase 8 block-size benchmark artifacts")
    parser.add_argument("--input-dir", default="tmp/bench_phase8", help="Directory containing per-run Phase 8 JSON outputs")
    parser.add_argument("--output", help="Optional path to write markdown summary")
    parser.add_argument("--json-output", help="Optional path to write machine-readable summary JSON")
    return parser.parse_args()


def load_runs(input_dir: Path):
    runs = []
    for path in sorted(input_dir.glob("phase8-*.json")):
        match = FILE_RE.match(path.name)
        if not match:
            continue
        try:
            payload = json.loads(path.read_text())
        except json.JSONDecodeError:
            continue
        if payload.get("status") != "ok":
            continue
        data = payload.get("data", {})
        rows = data.get("rows", [])
        runs.append(
            {
                "path": str(path),
                "dataset": match.group("dataset"),
                "workers": int(match.group("workers")),
                "repeat": int(match.group("repeat")),
                "size_mib": int(match.group("size_mib")),
                "generated_at_utc": data.get("generated_at_utc"),
                "rows": rows,
            }
        )
    return runs


def build_summary(runs):
    coverage = defaultdict(set)
    case_values = defaultdict(lambda: defaultdict(list))
    cases_by_group = defaultdict(set)

    for run in runs:
        group_key = (run["dataset"], run["workers"], run["size_mib"])
        coverage[group_key].add(run["repeat"])
        for row in run["rows"]:
            case_name = row["case"]
            cases_by_group[(run["dataset"], run["workers"])].add(case_name)
            metric_key = (run["dataset"], run["workers"], case_name)
            case_values[metric_key][run["size_mib"]].append(
                {
                    "duration_ms": row["duration_ms"],
                    "throughput_mbps": row["throughput_mbps"],
                }
            )

    summary = {
        "coverage": [],
        "groups": [],
    }

    for (dataset, workers, size_mib), repeats in sorted(coverage.items()):
        summary["coverage"].append(
            {
                "dataset": dataset,
                "workers": workers,
                "size_mib": size_mib,
                "completed_repeats": sorted(repeats),
                "completed_count": len(repeats),
            }
        )

    for dataset, workers in sorted(cases_by_group.keys()):
        group = {
            "dataset": dataset,
            "workers": workers,
            "cases": [],
        }
        for case_name in sorted(cases_by_group[(dataset, workers)]):
            metric_key = (dataset, workers, case_name)
            sizes = sorted(case_values[metric_key].keys())
            size_rows = []
            baseline_duration = None
            baseline_throughput = None
            for size_mib in sizes:
                duration_values = [item["duration_ms"] for item in case_values[metric_key][size_mib]]
                throughput_values = [item["throughput_mbps"] for item in case_values[metric_key][size_mib]]
                avg_duration = mean(duration_values)
                avg_throughput = mean(throughput_values)
                if size_mib == 1:
                    baseline_duration = avg_duration
                    baseline_throughput = avg_throughput
                size_rows.append(
                    {
                        "size_mib": size_mib,
                        "samples": len(duration_values),
                        "avg_duration_ms": avg_duration,
                        "avg_throughput_mbps": avg_throughput,
                    }
                )

            for row in size_rows:
                if baseline_duration is not None:
                    row["duration_delta_vs_1m_pct"] = pct_delta(row["avg_duration_ms"], baseline_duration)
                else:
                    row["duration_delta_vs_1m_pct"] = math.nan
                if baseline_throughput is not None:
                    row["throughput_delta_vs_1m_pct"] = pct_delta(row["avg_throughput_mbps"], baseline_throughput)
                else:
                    row["throughput_delta_vs_1m_pct"] = math.nan

            group["cases"].append(
                {
                    "case": case_name,
                    "sizes": size_rows,
                }
            )
        summary["groups"].append(group)

    return summary


def fmt_float(value, digits=1):
    if value is None or math.isnan(value):
        return "-"
    return f"{value:.{digits}f}"


def render_markdown(summary):
    lines = []
    lines.append("# Phase 8 Block Size Summary")
    lines.append("")
    lines.append("## Coverage")
    lines.append("")
    lines.append("| Dataset | Workers | Size (MiB) | Completed Repeats |")
    lines.append("| --- | ---: | ---: | --- |")
    for row in summary["coverage"]:
        repeats = ", ".join(str(item) for item in row["completed_repeats"]) or "-"
        lines.append(f"| {row['dataset']} | {row['workers']} | {row['size_mib']} | {repeats} |")

    focus_cases = [
        "store-large-file",
        "restore-large-file",
        "store-mixed-dataset",
        "snapshot-creation",
        "gc-after-churn",
        "stats-inspect",
        "store-many-small-files",
        "restore-many-files",
    ]

    for group in summary["groups"]:
        lines.append("")
        lines.append(f"## Dataset={group['dataset']} Workers={group['workers']}")
        lines.append("")
        for case in group["cases"]:
            if case["case"] not in focus_cases:
                continue
            lines.append(f"### {case['case']}")
            lines.append("")
            lines.append("| Size (MiB) | Samples | Avg Duration (ms) | Delta vs 1 MiB | Avg Throughput (MB/s) | Delta vs 1 MiB |")
            lines.append("| --- | ---: | ---: | ---: | ---: | ---: |")
            for size in case["sizes"]:
                lines.append(
                    "| {size_mib} | {samples} | {avg_duration} | {duration_delta} | {avg_throughput} | {throughput_delta} |".format(
                        size_mib=size["size_mib"],
                        samples=size["samples"],
                        avg_duration=fmt_float(size["avg_duration_ms"], 1),
                        duration_delta=fmt_float(size["duration_delta_vs_1m_pct"], 1) + "%",
                        avg_throughput=fmt_float(size["avg_throughput_mbps"], 3),
                        throughput_delta=fmt_float(size["throughput_delta_vs_1m_pct"], 1) + "%",
                    )
                )
            lines.append("")

    return "\n".join(lines) + "\n"


def main():
    args = parse_args()
    input_dir = Path(args.input_dir)
    runs = load_runs(input_dir)
    summary = build_summary(runs)
    markdown = render_markdown(summary)

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(markdown)
    if args.json_output:
        json_path = Path(args.json_output)
        json_path.parent.mkdir(parents=True, exist_ok=True)
        json_path.write_text(json.dumps(summary, indent=2, sort_keys=True))

    print(markdown, end="")


if __name__ == "__main__":
    main()