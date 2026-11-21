# Running jobs remotely on vidura (kk@192.168.2.64)

End-to-end workflow for starting, running, monitoring and collecting jobs on the
network GPU box (GTX 1070 8GB, Pascal). The queue/retry system is already deployed —
see `gpu-jobs/README.md` for the full job-file format.

## 1. Sync code + data

```sh
rsync -av --delete --exclude results --exclude .venv \
    experiments/gliner-decide/ kk@192.168.2.64:exp/gliner-decide/
rsync -av arr.db data/ kk@192.168.2.64:exp/     # datasets are ~2 MB total
```

The repo is the source of truth; edit locally, rsync before each run.

## 2. Submit a job

```sh
ssh kk@192.168.2.64 ~/bin/jobsubmit exp1-lora --attempts 5 --timeout 14400 -- \
    "cd ~/exp/gliner-decide && ~/miniforge3/envs/ml/bin/python finetune.py \
     --data gold-multi-scaled.jsonl --epochs 8 --batch-size 1 --task-lr 5e-4 \
     > train.log 2>&1"
```

- Jobs are bash files queued FIFO in `~/jobs/jobs/`; one runs at a time (8 GB GPU — sequential by design).
- `--attempts N` = max retries; failed attempts wait out backoff (2m/5m/15m/30m) and requeue.
  Transient failures (OOM spikes, CUDA resets, killed SSH) self-heal; poison jobs land in
  `~/jobs/failed/` without blocking the queue.
- `--timeout SECS` kills hung jobs (recommended for training: give 2–4 h headroom).
- The interpreter is always `~/miniforge3/envs/ml/bin/python` (3.11, torch 2.6.0+cu124).
  Pascal rules: batch 1–2 + gradient accumulation, fp16 AMP (no bf16).

## 3. Monitor

```sh
ssh kk@192.168.2.64 ~/bin/jobstatus.sh      # one shot: counts, running job + elapsed + log tail
watch -c 'ssh kk@192.168.2.64 ~/bin/jobstatus.sh'    # live from this machine
```

Per-attempt logs: `~/jobs/logs/<name>.a<N>.log`. Runner trace: `~/jobs/runner.log`.
Interactive: `ssh kk@192.168.2.64 'tmux attach -t jobrunner'` (detach with Ctrl-b d).

## 4. Collect results

```sh
rsync -av kk@192.168.2.64:exp/gliner-decide/results/ experiments/gliner-decide/results/
```

## 5. Recovery

- Re-run a failed job: `ssh kk@192.168.2.64 'mv ~/jobs/failed/<name>.job ~/jobs/jobs/'`
  (raise `#MAX_ATTEMPTS:` in the job file if it needs more retries).
- Runner restart: `tmux kill-session -t jobrunner; tmux new -d -s jobrunner ~/bin/runner.sh`.

## Machine notes

- Driver 550.163 = CUDA ≤ 12.4 → cu124 wheels only; Pascal (sm_61) support ends at torch 2.7.
  Python: `~/miniforge3/envs/ml/bin/python`. System python 3.8 — ignore it.
- No sudo from the agent; reboot/driver work goes through the user.
