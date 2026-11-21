# vidura GPU job runner (kk@192.168.2.64)

Queue system for long/retry-prone training jobs. Deployed at `~/bin/` on vidura,
runner lives in tmux session `jobrunner`.

## Layout (~/jobs/)

- `jobs/`    pending queue (oldest first, FIFO)
- `holding/` failed jobs waiting out their backoff (2m, 5m, 15m, then 30m flat)
- `running/` the one job currently executing (sequential — one GPU job at a time)
- `done/`, `failed/`  finished jobs; `failed/<name>.error` has rc + last 30 log lines
- `logs/<name>.a<N>.log`  one log per attempt
- `runner.log`  runner's own trace

## Job file format

A `.job` file is a bash script with optional metadata comment headers:

```
#MAX_ATTEMPTS: 3        # default 3
#TIMEOUT_SECS: 7200     # 0 = no timeout (default)
echo "work here"
```

## Usage

```sh
# submit (from anywhere via ssh, or on vidura directly)
ssh kk@192.168.2.64 ~/bin/jobsubmit exp1-lora --attempts 5 --timeout 14400 -- \
    "cd ~/exp/gliner-decide && ~/venvs/gliner/bin/python finetune.py ... > train.log 2>&1"

# monitor
ssh kk@192.168.2.64 ~/bin/jobstatus.sh          # one-shot status
watch -c 'ssh kk@192.168.2.64 ~/bin/jobstatus.sh'   # live from local machine

# runner control
ssh kk@192.168.2.64 'tmux attach -t jobrunner'   # interactive (Ctrl-b d to detach)
ssh kk@192.168.2.64 'tmux kill-session -t jobrunner && tmux new -d -s jobrunner ~/bin/runner.sh'
```

Retry semantics: on failure the job waits out backoff and re-enters the queue at the END;
per-attempt logs are preserved, so each failure's output stays inspectable. After the last
attempt the job moves to `failed/` and the runner moves on — a poison job never blocks the queue.

## Re-submitting a failed job

`mv ~/jobs/failed/<name>.job ~/jobs/jobs/` — the runner resets attempt counts only after a
SUCCESS, so edit the `#MAX_ATTEMPTS:` header up if you want fresh retries.

## Source of truth

Scripts are versioned in this repo: `gpu-jobs/{runner,jobstatus,jobsubmit}.sh`.
Edit locally, then: `scp gpu-jobs/*.sh kk@192.168.2.64:~/bin/ && chmod +x ...`.

## Known state (2026-11-20)

- GPU driver mismatch (kernel 550.54.14 vs userspace 550.163.01) — `nvidia-smi` fails until
  a reboot; needs sudo on vidura.
- System python is 3.8.10; gliner2 needs a newer interpreter — use a venv from
  python3.10+ or miniconda (not yet installed at time of writing).
