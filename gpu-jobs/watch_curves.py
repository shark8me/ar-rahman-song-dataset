#!/usr/bin/env python3
"""Live training-curve viewer for a job running on vidura.

Polls ~/exp/gliner-decide/results/exp1/metrics.jsonl on the GPU box via ssh,
renders an SVG chart to stdout-file, and prints the latest metric values.

Usage:
  python3 gpu-jobs/watch_curves.py [--once] [--interval 60] [--out curves.svg]

Without --once it loops: refresh the SVG every N seconds until Ctrl-C.
The SVG (dark theme, train+eval loss vs step) is written to --out (default
experiments/gliner-decide/results/exp1/curves.svg); open it in a browser or
refresh an already-open tab.
"""
import argparse, json, subprocess, time, math

HOST = "kk@192.168.2.64"
REMOTE = "~/exp/gliner-decide/results/exp1/metrics.jsonl"

SVG_HEAD = """<svg xmlns="http://www.w3.org/2000/svg" width="900" height="460"
 font-family="monospace"><rect width="900" height="460" fill="#11151c"/>
<text x="20" y="28" fill="#e6e6e6" font-size="18">exp1 LoRA training curves</text>
<text x="20" y="48" fill="#888" font-size="11">updated {ts}</text>"""


def fetch():
    try:
        r = subprocess.run(["ssh", "-o", "ConnectTimeout=8", HOST,
                            "cat " + REMOTE + " 2>/dev/null"],
                           capture_output=True, text=True, timeout=30)
        recs = []
        for line in r.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                recs.append(json.loads(line))
            except json.JSONDecodeError:
                pass  # partially written line
        return recs
    except Exception:
        return []


def render(recs, out):
    tr = [r for r in recs if r.get("kind") == "train" and "loss" in r]
    ev = [r for r in recs if r.get("kind") == "eval" and "eval_loss" in r]
    parts = [SVG_HEAD.format(ts=time.strftime("%H:%M:%S"))]
    series = [("train loss", tr, "loss", "#4fc3f7"), ("eval loss", ev, "eval_loss", "#ff8a65")]
    plotted = 0
    W, H, X0, Y0, YB = 840, 330, 40, 70, 400
    ymax, smax = 1.0, 1
    # joint scale (p99 cap keeps divergence visible); axes drawn once
    all_vals = [r[key] for _, pts, key, _ in series for r in pts]
    smax = max([r.get("step", i + 1) for _, pts, _, _ in series
                for i, r in enumerate(pts)] or [1])
    if all_vals:
        vcap = sorted(all_vals)[int(0.99 * (len(all_vals) - 1))] if len(all_vals) > 5 \
            else max(all_vals)
        ymax = max(vcap, 1e-6) * 1.1
        # y gridlines + tick labels
        nticks = 5
        for i in range(nticks + 1):
            v = ymax * i / nticks
            y = YB - (v / ymax) * (YB - Y0)
            parts.append(f'<line x1="{X0}" y1="{y:.1f}" x2="{X0+W}" y2="{y:.1f}" '
                         f'stroke="#232a36" stroke-width="1"/>')
            lab = f"{v:.1f}" if v >= 1 else f"{v:.3g}"
            parts.append(f'<text x="{X0-6}" y="{y+4:.1f}" fill="#888" font-size="11" '
                         f'text-anchor="end">{lab}</text>')
        # x ticks: steps
        xt = 5
        for i in range(xt + 1):
            s = smax * i / xt
            x = X0 + (s / smax) * W
            parts.append(f'<line x1="{x:.1f}" y1="{Y0}" x2="{x:.1f}" y2="{YB}" '
                         f'stroke="#1a2130" stroke-width="1"/>')
            parts.append(f'<text x="{x:.1f}" y="{YB+18}" fill="#888" font-size="11" '
                         f'text-anchor="middle">{s:.0f}</text>')
        # axis lines + axis titles
        parts.append(f'<line x1="{X0}" y1="{Y0}" x2="{X0}" y2="{YB}" stroke="#666" '
                     f'stroke-width="1.5"/>')
        parts.append(f'<line x1="{X0}" y1="{YB}" x2="{X0+W}" y2="{YB}" stroke="#666" '
                     f'stroke-width="1.5"/>')
        parts.append(f'<text x="14" y="{(Y0+YB)//2}" fill="#aaa" font-size="12" '
                     f'transform="rotate(-90 14 {(Y0+YB)//2})">loss</text>')
        parts.append(f'<text x="{X0+W//2}" y="{YB+38}" fill="#aaa" font-size="12" '
                     f'text-anchor="middle">step</text>')
    for label, pts, key, color in series:
        if not pts:
            continue
        vals = [r[key] for r in pts]
        poly = []
        for i, r in enumerate(pts):
            v = min(r[key], ymax)
            x = X0 + (r.get("step", i + 1) / smax) * W
            y = YB - (v / ymax) * (YB - Y0)
            poly.append(f"{x:.1f},{y:.1f}")
        parts.append(f'<text x="{X0}" y="{Y0-8}" fill="{color}" font-size="12">'
                     f'{label}: last={vals[-1]:.3f} min={min(vals):.3f} n={len(vals)}</text>')
        parts.append(f'<polyline fill="none" stroke="{color}" stroke-width="1.5" '
                     f'points="{" ".join(poly)}"/>')
        plotted += len(pts)
    if not plotted:
        parts.append('<text x="40" y="120" fill="#888" font-size="14">'
                     'waiting for metrics… (job may not have started)</text>')
    last = tr[-1] if tr else {}
    parts.append(f'<text x="40" y="440" fill="#aaa" font-size="12">'
                 f'step={last.get("step", "?")} epoch={last.get("epoch", "?")} '
                 f'lr={last.get("learning_rate", "?")} train_pts={len(tr)} eval_pts={len(ev)}</text>')
    parts.append("</svg>")
    with open(out, "w") as f:
        f.write("\n".join(parts))
    if tr:
        print(f"step={tr[-1].get('step')} epoch={tr[-1].get('epoch')} "
              f"train_loss={tr[-1]['loss']:.3f}", end=" ")
    if ev:
        print(f"eval_loss={ev[-1]['eval_loss']:.3f} (best={min(r['eval_loss'] for r in ev):.3f})", end="")
    print()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--interval", type=int, default=60)
    ap.add_argument("--out", default="experiments/gliner-decide/results/exp1/curves.svg")
    a = ap.parse_args()
    import os
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    while True:
        recs = fetch()
        render(recs, a.out)
        if a.once:
            break
        time.sleep(a.interval)


if __name__ == "__main__":
    import sys
    sys.exit(main())
