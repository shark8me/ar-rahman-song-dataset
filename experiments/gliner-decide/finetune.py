#!/usr/bin/env python3
"""Task-7: LoRA fine-tune GLiNER2.5-Decide on the 31-question gold set.

Builds InputExample records with the same 8 classification heads used at
inference (query_schema label lists), splits 25 train / 6 test (seed 42),
trains a LoRA adapter on CPU, and saves it under results/lora/final plus a
split manifest.  The test split is then scored by predict.py --run finetuned.

Usage:
  finetune.py [--epochs 30] [--dry-run]      # dry-run: 1 epoch / 3 examples
"""
import argparse, json, os, random, sys

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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--epochs", type=int, default=30)
    ap.add_argument("--batch-size", type=int, default=2)
    ap.add_argument("--task-lr", type=float, default=1e-3)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--n-test", type=int, default=6)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    raw = gold_examples()
    rng = random.Random(a.seed)
    order = list(range(len(raw)))
    rng.shuffle(order)
    test_idx = sorted(order[:a.n_test])
    train_idx = sorted(order[a.n_test:])
    split = {"train": [raw[i]['idx'] for i in train_idx],
             "test": [raw[i]['idx'] for i in test_idx],
             "seed": a.seed}
    os.makedirs(os.path.join(HERE, "results"), exist_ok=True)
    with open(os.path.join(HERE, "results", "finetune_split.json"), "w") as f:
        json.dump(split, f, indent=1)
    print("split:", json.dumps(split))

    examples = []
    for i in train_idx:
        r = raw[i]
        cfs = []
        for c in r['classifications']:
            cfs.append({"task": c['task'], "labels": c['labels'],
                        "true_label": c['true_label'],
                        "multi_label": c['multi_label']})
        examples.append(InputExample(text=r['query'], classifications=[
            Classification(task=c['task'], labels=c['labels'],
                           true_label=c['true_label'],
                           multi_label=c['multi_label']) for c in cfs]))

    # validate data
    ds = TrainingDataset(examples)
    errs = ds.validate(raise_on_error=False)
    print("dataset validation:", errs)
    print(ds.stats())

    if a.dry_run:
        examples = examples[:3]
        a.epochs = 1
        out = os.path.join(HERE, "results", "lora_dry")
    else:
        out = os.path.join(HERE, "results", "lora3")

    model = GLiNER2.from_pretrained("fastino/GLiNER2.5-Decide")
    config = TrainingConfig(
        output_dir=out,
        experiment_name="arrqa-skeleton-lora",
        num_epochs=a.epochs,
        batch_size=a.batch_size,
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
    summary = trainer.train(train_data=examples, eval_data=examples)
    print("train summary:", json.dumps(summary, default=str)[:2000])

    final = os.path.join(out, "final")
    if not os.path.isdir(final):
        # eval_strategy=no: save the adapter explicitly
        trainer.model.save_pretrained(final)
    print("saved adapter:", final)


if __name__ == '__main__':
    sys.exit(main())
