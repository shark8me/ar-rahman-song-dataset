#!/usr/bin/env python3
"""Render the run-1 (diverged) vs run-3 (converged) loss curves as a single SVG."""
import re, os

HERE = os.path.dirname(os.path.abspath(__file__))

def parse(name, total):
    txt = open(os.path.join(HERE, name)).read()
    d = {}
    for s, l in re.findall(r'(\d+)/%s \[[^\]]*?loss=([0-9.]+)' % total, txt):
        d[int(s)] = float(l)
    return sorted(d.items())

r1 = parse('results/finetune.log', 360)   # failed: task_lr 1e-3, saved final = diverged
r3 = parse('results/finetune3.log', 360)  # success: task_lr 5e-4, best ckpt

W, H = 960, 480
ML, MR, MT, MB = 64, 24, 36, 56
pw, ph = W - ML - MR, H - MT - MB
YMAX = 36.0

def path(series):
    pts = [(ML + s / 360 * pw, MT + (1 - min(l, YMAX) / YMAX) * ph) for s, l in series]
    return "M" + " L".join(f"{x:.1f},{y:.1f}" for x, y in pts)

def yticks():
    out = []
    for y in range(0, 36, 6):
        yy = MT + (1 - y / YMAX) * ph
        out.append(f'<line x1="{ML}" y1="{yy:.1f}" x2="{W-MR}" y2="{yy:.1f}" stroke="#2a2f3a" stroke-width="1"/>'
                   f'<text x="{ML-10}" y="{yy+4:.1f}" text-anchor="end" fill="#8b93a7" font-size="12">{y}</text>')
    return "\n".join(out)

def xticks():
    out = []
    for x in range(0, 361, 60):
        xx = ML + x / 360 * pw
        ep = x // 12
        out.append(f'<line x1="{xx:.1f}" y1="{MT}" x2="{xx:.1f}" y2="{MT+ph}" stroke="#20242e" stroke-width="1"/>'
                   f'<text x="{xx:.1f}" y="{MT+ph+18}" text-anchor="middle" fill="#8b93a7" font-size="12">{x} <tspan fill="#5b6472">(ep {ep})</tspan></text>')
    return "\n".join(out)

# annotations
def annot(x, y, label, tx, ty, color="#9aa3b5"):
    wpx = len(label) * 6.6 + 10
    return (f'<line x1="{x:.0f}" y1="{y:.0f}" x2="{tx:.0f}" y2="{ty:.0f}" stroke="{color}" stroke-width="1" stroke-dasharray="3,3"/>'
            f'<rect x="{tx:.0f}" y="{ty-12:.0f}" width="{wpx:.0f}" height="18" rx="4" fill="#14171d" opacity="0.88"/>'
            f'<text x="{tx+5:.0f}" y="{ty:.0f}" fill="{color}" font-size="12.5">{label}</text>')

s1 = dict(r1); s3 = dict(r3)
# smoothed overlays (window 9)
def smooth(series, w=9):
    ys = [l for _, l in series]
    out = []
    for i, (s, _) in enumerate(series):
        lo = max(0, i - w // 2); hi = min(len(ys), i + w // 2 + 1)
        out.append((s, sum(ys[lo:hi]) / (hi - lo)))
    return out

x150 = ML + 150/360*pw; y150 = MT + (1-0.0055/YMAX)*ph
x360 = ML + pw; y360r1 = MT + (1-30.97/YMAX)*ph

svg = f'''<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" font-family="-apple-system,Segoe UI,Helvetica,Arial,sans-serif">
<rect width="{W}" height="{H}" fill="#14171d"/>
<text x="{ML}" y="22" fill="#e8ebf2" font-size="16" font-weight="600">GLiNER2.5-Decide LoRA fine-tune — training loss per step (25 examples, batch 2)</text>
{yticks()}
{xticks()}
<line x1="{ML}" y1="{MT+ph}" x2="{W-MR}" y2="{MT+ph}" stroke="#5b6472" stroke-width="1"/>
<line x1="{ML}" y1="{MT}" x2="{ML}" y2="{MT+ph}" stroke="#5b6472" stroke-width="1"/>
<path d="{path(r3)}" fill="none" stroke="#4fc38a" stroke-width="1.8"/>
<path d="{path(r1)}" fill="none" stroke="#e06c6c" stroke-width="1.8"/>
<path d="{path(smooth(r1))}" fill="none" stroke="#e06c6c" stroke-width="3" opacity="0.35"/>
<path d="{path(smooth(r3))}" fill="none" stroke="#4fc38a" stroke-width="3" opacity="0.35"/>
{annot(x150, y150, "run 1 dips to ~0.005 (ep ~13): model fits the data", ML+70, MT+ph-56, "#e0a06c")}
{annot(x360-2, y360r1, "run 1 diverges to ~31", ML+pw-190, MT+44, "#e06c6c")}
<rect x="{ML+8}" y="{MT+8}" width="12" height="4" fill="#e06c6c" rx="1"/>
<text x="{ML+26}" y="{MT+14}" fill="#e06c6c" font-size="13">run 1 — FAILED (task_lr 1e-3, final checkpoint saved at ep 30)</text>
<rect x="{ML+8}" y="{MT+28}" width="12" height="4" fill="#4fc38a" rx="1"/>
<text x="{ML+26}" y="{MT+34}" fill="#4fc38a" font-size="13">run 3 — SUCCESS (task_lr 5e-4, best checkpoint @ ep 26, eval_loss 0.49)</text>
<text x="{ML}" y="{H-14}" fill="#5b6472" font-size="11.5">x: optimizer step (12 steps/epoch, 360 total) · y: classification loss · clipped at 36</text>
</svg>'''

out = os.path.join(HERE, 'diagrams')
os.makedirs(out, exist_ok=True)
open(os.path.join(out, 'training-curves.svg'), 'w').write(svg)
print("wrote", os.path.join(out, 'training-curves.svg'), len(svg), "bytes")
