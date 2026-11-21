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
    for label, pts, key, color in series:
        if not pts:
            continue
        vals = [r[key] for r in pts]
        vmax = max(max(vals), 1e-6)
        W, H, X0, Y0, YB = 840, 330, 40, 70, 400
        # log-ish scale: cap display at p99 to keep divergence visible but scaled
        vcap = sorted(vals)[int(0.99 * (len(vals) - 1))] if len(vals) > 5 else vmax
        ymax = max(vcap, 1e-6) * 1.1
        smax = max(r.get("step", i + 1) for i, r in enumerate(pts))
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
