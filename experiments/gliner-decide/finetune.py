#!/usr/bin/env python3
"""Task-7: LoRA fine-tune GLiNER2.5-Decide on the 31-question gold set.

Builds InputExample records with the same 8 classification heads used at
inference (query_schema label lists), splits 25 train / 6 test (seed 42),
trains a LoRA adapter on CPU, and saves it under results/lora/final plus a
split manifest.  The test split is then scored by predict.py --run finetuned.

Usage:
  finetune.py [--epochs 30] [--dry-run]      # dry-run: 1 epoch / 3 examples
"""
import argparse, json, os, random, sys, time

from gliner2 import GLiNER2
from gliner2.training.data import InputExample, Classification, TrainingDataset
from gliner2.training.trainer import TrainingConfig, GLiNER2Trainer

from query_schema import (SELECT_LABELS, FILTER_COLS, FILTER_OPS, AGG_LABELS,
                          GROUP_LABELS, JOIN_LABELS, ORDER_LABELS, LIMIT_LABELS)

HERE = os.path.dirname(os.path.abspath(__file__))

# head -> (labels, multi_label)
HEAD_SPECS = {
    "select_cols": (SELECT_LABELS, True),
    "filter_col":  (FILTER_COLS, False),
    "filter_op":   (FILTER_OPS, False),
    "agg":         (AGG_LABELS, False),
    "group_by":    (GROUP_LABELS, False),
    "join":        (JOIN_LABELS, True),
    "order":       (ORDER_LABELS, False),
    "limit":       (LIMIT_LABELS, False),
}


def gold_examples():
    out = []
    for line in open(os.path.join(HERE, 'gold.jsonl')):
        g = json.loads(line)
        cls = []
        for head, (labels, multi) in HEAD_SPECS.items():
            tv = g['heads'][head]
            if tv is None:
                tv = []
            if not isinstance(tv, list):
                tv = [tv]
            cls.append({"task": head, "labels": labels, "true_label": tv,
                        "multi_label": multi})
        out.append({"idx": g['idx'], "query": g['query'], "classifications": cls})
    return out


def instrument_head_losses(model):
    """Record per-head (per-classification-task) BCE contributions.

    GLiNER2 here is the span architecture: SpanExtractorModel.forward loops
    samples in order and calls _compute_sample_loss per sample; the wrapper
    stashes the batch + a call counter on the model, and the sample-loss
    wrapper re-evaluates the classifier per classification schema under
    torch.no_grad(), accumulating (loss_sum, label_count) per task name
    (recovered from schema_tokens_list) on model._head_loss_accum.  The
    metrics poller flushes per-head means to the JSONL so filter_col /
    filter_op loss is visible separately during training.
    """
    import torch
    import torch.nn.functional as F
    span = type(model)  # SpanExtractorModel
    orig_fwd = span.forward
    orig_loss = span._compute_sample_loss
    model._head_loss_accum = {}

    def fwd(self, batch, *a, **kw):
        self._instrument_batch = batch
        self._instrument_call = 0
        return orig_fwd(self, batch, *a, **kw)

    def sample_loss(self, *args, **kw):
        # signature-agnostic: gliner2 versions differ on extra kwargs
        # (e.g. span_scores_per_schema); the fields we need come through as
        # keywords from SpanExtractorModel.forward
        token_embeddings = kw.get("token_embeddings", args[0] if args else None)
        embs_per_schema = kw.get("embs_per_schema", args[1] if len(args) > 1 else None)
        task_types = kw.get("task_types", args[2] if len(args) > 2 else None)
        structure_labels = kw.get("structure_labels", args[3] if len(args) > 3 else None)
        device = kw.get("device", args[4] if len(args) > 4 else None)
        try:
            batch = self._instrument_batch
            si = self._instrument_call
            self._instrument_call = si + 1
            schemas = batch.schema_tokens_list[si]
            with torch.no_grad():
                for j, task_type in enumerate(task_types or []):
                    if task_type != "classifications" or j >= len(schemas):
                        continue
                    if not embs_per_schema[j]:
                        continue
                    schema_emb = torch.stack(embs_per_schema[j])
                    cls_embeds = schema_emb[1:]
                    logits = self.classifier(cls_embeds).squeeze(-1)
                    labels = torch.tensor(structure_labels[j], dtype=torch.float,
                                          device=device)
                    if labels.shape != logits.shape:
                        continue
                    loss = F.binary_cross_entropy_with_logits(
                        logits, labels, reduction="sum")
                    toks = schemas[j]
                    # same name recovery as gliner2's schema parsers: the task
                    # name sits at index 2 (optionally before " [DESCRIPTION] ")
                    name = str(toks[2]).split(" [DESCRIPTION] ")[0] if len(toks) > 2 \
                        else next((str(t) for t in toks if not t.startswith("[")),
                                  "unknown")
                    acc = self._head_loss_accum.setdefault(name, [0.0, 0])
                    acc[0] += float(loss.detach())
                    acc[1] += labels.numel()
        except Exception as e:
            print("head-loss instrumentation:", e, flush=True)
        return orig_loss(self, *args, **kw)

    span.forward = fwd
    span._compute_sample_loss = sample_loss


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--epochs", type=int, default=30)
    ap.add_argument("--batch-size", type=int, default=2)
    ap.add_argument("--grad-accum", type=int, default=1,
                    help="gradient accumulation steps (Pascal: 1-2 batch + accumulation)")
    ap.add_argument("--fp16", action="store_true",
                    help="force fp16 AMP (Pascal has no bf16)")
    ap.add_argument("--task-lr", type=float, default=1e-3)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--n-test", type=int, default=6)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--data", default=None,
                    help="scaled gold JSONL (default: gold.jsonl)")
    ap.add_argument("--split", default=None,
                    help="split JSON with train/test idx lists (e.g. exp1_split.json)")
    ap.add_argument("--out", default=None, help="adapter output dir override")
    ap.add_argument("--metrics-out", default=None,
                    help="JSONL path for live training curves (default: results/exp1/metrics.jsonl)")
    a = ap.parse_args()

    if a.data:
        raw = []
        for line in open(a.data):
            g = json.loads(line)
            cls = []
            for head, (labels, multi) in HEAD_SPECS.items():
                tv = g['heads'][head]
                if tv is None:
                    tv = []
                if not isinstance(tv, list):
                    tv = [tv]
                cls.append({"task": head, "labels": labels, "true_label": tv,
                            "multi_label": multi})
            raw.append({"idx": g['idx'], "query": g['query'], "classifications": cls})
    else:
        raw = gold_examples()

    by_idx = {r['idx']: r for r in raw}
    os.makedirs(os.path.join(HERE, "results"), exist_ok=True)
    if a.split:
        split = json.load(open(a.split))
    else:
        rng = random.Random(a.seed)
        order = list(range(len(raw)))
        rng.shuffle(order)
        test_idx = sorted(order[:a.n_test])
        train_idx = sorted(order[a.n_test:])
        split = {"train": [raw[i]['idx'] for i in train_idx],
                 "test": [raw[i]['idx'] for i in test_idx],
                 "seed": a.seed}
    with open(os.path.join(HERE, "results", "finetune_split.json"), "w") as f:
        json.dump(split, f, indent=1)
    print("split: train=%d test=%d" % (len(split['train']), len(split['test'])), flush=True)

    def to_example(r):
        return InputExample(text=r['query'], classifications=[
            Classification(task=c['task'], labels=c['labels'],
                           true_label=c['true_label'],
                           multi_label=c['multi_label']) for c in r['classifications']])

    examples = []
    for i in split['train']:
        r = by_idx.get(i)
        if r is None:
            print("WARN: split idx %r not in data, skipping" % (i,), flush=True)
            continue
        examples.append(to_example(r))
    # eval on the held-out test entries when present, else on train (old behavior)
    test_raw = [by_idx[i] for i in split.get('test', []) if by_idx.get(i)]
    eval_examples = [to_example(r) for r in test_raw] if len(test_raw) >= 2 else examples

    # validate data
    ds = TrainingDataset(examples)
    errs = ds.validate(raise_on_error=False)
    print("dataset validation:", errs)
    print(ds.stats())

    if a.dry_run:
        examples = examples[:3]
        a.epochs = 1
        out = os.path.join(HERE, "results", "lora_dry")
    elif a.out:
        out = a.out
    else:
        out = os.path.join(HERE, "results", "lora3")

    model = GLiNER2.from_pretrained("fastino/GLiNER2.5-Decide")
    instrument_head_losses(model)
    config = TrainingConfig(
        output_dir=out,
        experiment_name="arrqa-skeleton-lora",
        num_epochs=a.epochs,
        batch_size=a.batch_size,
        gradient_accumulation_steps=a.grad_accum,
        fp16=True if a.fp16 else None,
        bf16=False,
        use_lora=True,
        lora_r=16,
        lora_alpha=32.0,
        task_lr=a.task_lr,
        eval_strategy="epoch",
        save_best=True,
        metric_for_best="eval_loss",
        greater_is_better=False,
        save_adapter_only=True,
        save_total_limit=0,
        seed=a.seed,
    )
    trainer = GLiNER2Trainer(model=model, config=config)

    # Live training-curve poller: snapshot the trainer's in-memory metrics
    # history to a JSONL file every few seconds so it can be tailed over ssh.
    metrics_path = a.metrics_out or os.path.join(HERE, "results", "exp1", "metrics.jsonl")
    os.makedirs(os.path.dirname(metrics_path), exist_ok=True)
    import threading
    seen = {"n_train": 0, "n_eval": 0}
    stop = threading.Event()

    def poll_metrics():
        with open(metrics_path, "a") as f:
            while not stop.is_set():
                try:
                    th, eh = trainer.train_metrics_history, trainer.eval_metrics_history
                    new = ([("train", m) for m in th[seen["n_train"]:]] +
                           [("eval", m) for m in eh[seen["n_eval"]:]])
                    for kind, m in new:
                        rec = dict(m)
                        rec["kind"] = kind
                        rec["ts"] = time.time()
                        f.write(json.dumps(rec, default=str) + "\n")
                    # per-head loss means accumulated since the last flush
                    m = trainer.model
                    acc = getattr(m, "_head_loss_accum", None) or getattr(
                        getattr(m, "model", None), "_head_loss_accum", None)
                    if acc:
                        heads = {task: s / n for task, (s, n) in acc.items() if n}
                        acc.clear()
                        if heads:
                            f.write(json.dumps(
                                {"kind": "head_loss",
                                 "step": getattr(trainer, "global_step", None),
                                 "heads": heads, "ts": time.time()},
                                default=str) + "\n")
                    f.flush()
                    seen["n_train"] = len(th)
                    seen["n_eval"] = len(eh)
                except Exception as e:
                    print("metrics poller:", e, flush=True)
                stop.wait(5.0)

    poller = threading.Thread(target=poll_metrics, daemon=True)
    poller.start()
    summary = trainer.train(train_data=examples, eval_data=eval_examples)
    stop.set()
    poller.join(timeout=10)
    # final head-loss flush: the poller's 5s tick may never fire on short runs
    m = trainer.model
    acc = getattr(m, "_head_loss_accum", None) or getattr(
        getattr(m, "model", None), "_head_loss_accum", None)
    if acc:
        heads = {task: s / n for task, (s, n) in acc.items() if n}
        with open(metrics_path, "a") as f:
            f.write(json.dumps({"kind": "head_loss", "final": True,
                                "step": getattr(trainer, "global_step", None),
                                "heads": heads, "ts": time.time()},
                               default=str) + "\n")
    print("metrics:", metrics_path, flush=True)
    print("train summary:", json.dumps(summary, default=str)[:2000])

    final = os.path.join(out, "final")
    if not os.path.isdir(final):
        # eval_strategy=no: save the adapter explicitly
        trainer.model.save_pretrained(final)
    print("saved adapter:", final)


if __name__ == '__main__':
    sys.exit(main())
