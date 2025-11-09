#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
queuectl - CLI-based background job queue
Features:
- SQLite persistence
- Multiple workers with a supervisor process
- Exponential backoff retry: delay = base ** attempts (seconds)
- Dead Letter Queue (state='dead' after retry exhaustion)
- Config management
- Graceful shutdown (finish current job)
- No duplicate processing (transactional claim)
"""

import argparse
import datetime as dt
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
import uuid
from multiprocessing import Process, Event as MpEventFactory

# Proper type for annotations (Pylance-friendly)
try:
    from multiprocessing.synchronize import Event as MpEvent
except Exception:  # fallback for odd environments
    from typing import Any as MpEvent  # type: ignore

APP_DIR = os.environ.get("QUEUECTL_HOME", os.path.expanduser("~/.queuectl"))
DB_PATH = os.path.join(APP_DIR, "queue.db")
PIDFILE = os.path.join(APP_DIR, "supervisor.pid")

# ---------- helpers ----------

def now_utc():
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)

def iso(dtobj):
    return dtobj.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")

def ensure_app_dir():
    os.makedirs(APP_DIR, exist_ok=True)

def connect():
    ensure_app_dir()
    conn = sqlite3.connect(DB_PATH, isolation_level=None, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def init_db():
    conn = connect()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        command TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('pending','processing','completed','failed','dead')),
        attempts INTEGER NOT NULL DEFAULT 0,
        max_retries INTEGER NOT NULL DEFAULT 3,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        run_at TEXT NOT NULL,       -- when it becomes eligible to run (for backoff/scheduling)
        priority INTEGER NOT NULL DEFAULT 0,
        last_error TEXT,
        last_exit_code INTEGER,
        worker_id TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
    """)
    # defaults
    defaults = {
        "max_retries": "3",
        "backoff_base": "2",
        "poll_interval_sec": "1",
        "job_timeout_sec": "0"  # 0 = no timeout
    }
    for k, v in defaults.items():
        cur.execute("INSERT OR IGNORE INTO config(key,value) VALUES(?,?)", (k, v))
    conn.commit()
    conn.close()

def get_config(keys=None):
    conn = connect()
    cur = conn.cursor()
    if keys:
        qmarks = ",".join("?"*len(keys))
        rows = cur.execute(f"SELECT key,value FROM config WHERE key IN ({qmarks})", keys).fetchall()
    else:
        rows = cur.execute("SELECT key,value FROM config").fetchall()
    conn.close()
    return {k: v for k, v in rows}

def set_config(key, value):
    conn = connect()
    cur = conn.cursor()
    cur.execute("INSERT INTO config(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))
    conn.commit()
    conn.close()

def print_table(rows, headers):
    if not rows:
        print("(no rows)")
        return
    widths = [max(len(str(h)), *(len(str(r[i])) for r in rows)) for i, h in enumerate(headers)]
    fmt = "  ".join("{:" + str(w) + "}" for w in widths)
    print(fmt.format(*headers))
    print(fmt.format(*["-"*w for w in widths]))
    for r in rows:
        print(fmt.format(*[str(x) for x in r]))

# ---------- job operations ----------

def enqueue_job(job_json):
    init_db()
    # Accept either JSON string or path to JSON file
    if os.path.exists(job_json):
        # utf-8-sig handles files saved with BOM from PowerShell/VS Code on Windows
        with open(job_json, "r", encoding="utf-8-sig") as f:
            job = json.load(f)
    else:
        job = json.loads(job_json)

    # Fill defaults
    job_id = job.get("id") or str(uuid.uuid4())
    command = job["command"]
    state = job.get("state", "pending")
    attempts = int(job.get("attempts", 0))
    max_retries = int(job.get("max_retries", int(get_config(["max_retries"])["max_retries"])))
    created = job.get("created_at") or iso(now_utc())
    updated = job.get("updated_at") or created
    run_at = job.get("run_at") or created
    priority = int(job.get("priority", 0))

    conn = connect()
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO jobs(id,command,state,attempts,max_retries,created_at,updated_at,run_at,priority)
    VALUES(?,?,?,?,?,?,?,?,?)
    """, (job_id, command, state, attempts, max_retries, created, updated, run_at, priority))
    conn.commit()
    conn.close()
    print(f"Enqueued job {job_id}")

def list_jobs(state=None, limit=100):
    init_db()
    conn = connect()
    cur = conn.cursor()
    if state:
        rows = cur.execute("""
            SELECT id, state, attempts, max_retries, priority, run_at, command
            FROM jobs WHERE state=? ORDER BY run_at ASC, created_at ASC LIMIT ?
        """, (state, limit)).fetchall()
    else:
        rows = cur.execute("""
            SELECT id, state, attempts, max_retries, priority, run_at, command
            FROM jobs ORDER BY created_at DESC LIMIT ?
        """, (limit,)).fetchall()
    conn.close()
    print_table(rows, ["id", "state", "attempts", "max", "prio", "run_at", "command"])

def list_dlq(limit=100):
    list_jobs(state="dead", limit=limit)

def retry_dlq(job_id):
    init_db()
    conn = connect()
    cur = conn.cursor()
    row = cur.execute("SELECT id FROM jobs WHERE id=? AND state='dead'", (job_id,)).fetchone()
    if not row:
        print(f"Job {job_id} not found in DLQ.")
        conn.close()
        return
    now = iso(now_utc())
    cur.execute("""
        UPDATE jobs
        SET state='pending', attempts=0, run_at=?, updated_at=?, last_error=NULL, last_exit_code=NULL
        WHERE id=?
    """, (now, now, job_id))
    conn.commit()
    conn.close()
    print(f"Moved {job_id} from DLQ to pending.")

def is_pid_alive(pid: int) -> bool:
    """Cross-platform check if a PID is alive (Windows + POSIX)."""
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        OpenProcess = kernel32.OpenProcess
        OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        OpenProcess.restype = wintypes.HANDLE
        CloseHandle = kernel32.CloseHandle
        CloseHandle.argtypes = [wintypes.HANDLE]
        CloseHandle.restype = wintypes.BOOL
        h = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, int(pid))
        if h:
            CloseHandle(h)
            return True
        return False
    else:
        try:
            os.kill(int(pid), 0)
            return True
        except Exception:
            return False

def queue_status():
    init_db()
    conn = connect()
    cur = conn.cursor()
    states = ["pending","processing","completed","failed","dead"]
    counts = {s: cur.execute("SELECT COUNT(*) FROM jobs WHERE state=?", (s,)).fetchone()[0] for s in states}
    total = cur.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    conn.close()

    # workers: read supervisor pid file and probe children
    active_workers = 0
    supervisor_alive = False
    sup_pid = None
    if os.path.exists(PIDFILE):
        try:
            with open(PIDFILE, "r") as f:
                data = json.load(f)
            sup_pid = int(data["supervisor_pid"])
            worker_pids = data.get("worker_pids", [])
            if is_pid_alive(sup_pid):
                supervisor_alive = True
            for wp in worker_pids:
                if is_pid_alive(int(wp)):
                    active_workers += 1
        except Exception:
            pass

    print("Queue Status")
    print("------------")
    for s in states:
        print(f"{s:>10}: {counts[s]}")
    print(f"{'total':>10}: {total}")
    print("")
    print(f"Supervisor: {'running' if supervisor_alive else 'stopped'}", end="")
    if supervisor_alive:
        print(f" (pid {sup_pid})")
    else:
        print()
    print(f"Active workers: {active_workers}")

# ---------- worker logic ----------

def transactional_claim(conn, worker_id):
    """
    Atomically claim one eligible pending job (run_at <= now) by flipping it to 'processing'.
    Returns the claimed job row or None.
    """
    cur = conn.cursor()
    cur.execute("BEGIN IMMEDIATE")
    now = iso(now_utc())
    # find a pending job
    row = cur.execute("""
        SELECT id FROM jobs
        WHERE state='pending' AND run_at <= ?
        ORDER BY priority DESC, created_at ASC
        LIMIT 1
    """, (now,)).fetchone()
    if not row:
        conn.execute("COMMIT")
        return None
    job_id = row[0]
    # attempt to claim if still pending (avoids race)
    updated = cur.execute("""
        UPDATE jobs
        SET state='processing', worker_id=?, updated_at=?
        WHERE id=? AND state='pending'
    """, (worker_id, now, job_id))
    if updated.rowcount != 1:
        conn.execute("COMMIT")
        return None
    # fetch full job
    job = cur.execute("""
        SELECT id, command, attempts, max_retries, created_at, updated_at, priority
        FROM jobs WHERE id=?
    """, (job_id,)).fetchone()
    conn.execute("COMMIT")
    return {
        "id": job[0],
        "command": job[1],
        "attempts": int(job[2]),
        "max_retries": int(job[3]),
        "created_at": job[4],
        "updated_at": job[5],
        "priority": int(job[6]),
    }

def compute_backoff(attempts, base):
    # attempts is already incremented when computing next run slot
    return base ** attempts

def execute_job(job, cfg, shutdown_evt: MpEvent):
    """
    Run the command; return tuple (success(bool), exit_code(int), error_message(str or None))
    Respect optional job_timeout_sec config; allow graceful shutdown only after current job.
    """
    del shutdown_evt  # not used during single-command exec; kept to mirror signature
    cmd = job["command"]
    timeout = int(cfg.get("job_timeout_sec", "0"))
    try:
        if timeout and timeout > 0:
            result = subprocess.run(cmd, shell=True, capture_output=True, timeout=timeout)
        else:
            result = subprocess.run(cmd, shell=True, capture_output=True)
        ok = (result.returncode == 0)
        err = None if ok else (result.stderr.decode("utf-8", "ignore") or f"Non-zero exit ({result.returncode})")
        return ok, result.returncode, err
    except subprocess.TimeoutExpired:
        return False, -1, f"Timeout after {timeout}s"
    except FileNotFoundError:
        return False, 127, "Command not found"
    except Exception as e:
        return False, -1, f"Execution error: {e}"

def finish_job(conn, job_id, success, exit_code, err_text, attempts, max_retries, cfg):
    cur = conn.cursor()
    now = iso(now_utc())
    base = int(cfg.get("backoff_base", "2"))
    if success:
        cur.execute("""
            UPDATE jobs SET state='completed', updated_at=?, last_error=NULL, last_exit_code=?
            WHERE id=?
        """, (now, exit_code, job_id))
    else:
        attempts += 1
        if attempts > max_retries:
            cur.execute("""
                UPDATE jobs SET state='dead', attempts=?, updated_at=?, last_error=?, last_exit_code=?
                WHERE id=?
            """, (attempts, now, err_text, exit_code, job_id))
        else:
            delay = compute_backoff(attempts, base)
            run_at = iso(now_utc() + dt.timedelta(seconds=delay))
            cur.execute("""
                UPDATE jobs
                SET state='failed', attempts=?, updated_at=?, last_error=?, last_exit_code=?
                WHERE id=?
            """, (attempts, now, err_text, exit_code, job_id))
            # requeue to pending with future run_at
            cur.execute("""
                UPDATE jobs
                SET state='pending', run_at=?, updated_at=?
                WHERE id=?
            """, (run_at, now, job_id))
    conn.commit()

def worker_loop(worker_id, shutdown_evt: MpEvent):
    init_db()
    cfg = get_config()
    poll = float(cfg.get("poll_interval_sec", "1"))
    conn = connect()
    try:
        while not shutdown_evt.is_set():
            job = transactional_claim(conn, worker_id)
            if not job:
                # idle
                time.sleep(poll)
                continue
            # execute
            success, exit_code, err_text = execute_job(job, cfg, shutdown_evt)
            # finish
            finish_job(conn, job["id"], success, exit_code, err_text, job["attempts"], job["max_retries"], cfg)
    finally:
        conn.close()

# ---------- supervisor (start/stop) ----------

def write_pidfile(supervisor_pid, worker_pids):
    ensure_app_dir()
    with open(PIDFILE, "w") as f:
        json.dump({"supervisor_pid": supervisor_pid, "worker_pids": worker_pids}, f)

def read_pidfile():
    if not os.path.exists(PIDFILE):
        return None
    with open(PIDFILE, "r") as f:
        return json.load(f)

def remove_pidfile():
    try:
        os.remove(PIDFILE)
    except FileNotFoundError:
        pass

def supervisor_start(count):
    init_db()
    if os.path.exists(PIDFILE):
        try:
            data = read_pidfile()
            if data and is_pid_alive(int(data["supervisor_pid"])):
                print("Supervisor already running.")
                return
        except Exception:
            pass

    shutdown_evt = MpEventFactory()
    worker_procs = []

    def handle_sigterm(signum, frame):
        shutdown_evt.set()

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    # Spawn workers
    for i in range(count):
        wid = f"w-{os.getpid()}-{i}-{uuid.uuid4().hex[:6]}"
        p = Process(target=worker_loop, args=(wid, shutdown_evt), daemon=False)
        p.start()
        worker_procs.append(p)

    write_pidfile(os.getpid(), [p.pid for p in worker_procs])
    print(f"Supervisor started (pid {os.getpid()}) with {len(worker_procs)} worker(s). Press Ctrl+C to stop.")

    # Wait and regenerate pidfile if any worker exits (no auto-restart for simplicity)
    try:
        while not shutdown_evt.is_set():
            alive = [p for p in worker_procs if p.is_alive()]
            if len(alive) != len(worker_procs):
                worker_procs = alive
                write_pidfile(os.getpid(), [p.pid for p in worker_procs])
            time.sleep(1.0)
    finally:
        # Graceful stop: let workers finish current job: signal is already set
        for p in worker_procs:
            p.join(timeout=10)
        remove_pidfile()
        print("Supervisor stopped.")

def supervisor_stop():
    if not os.path.exists(PIDFILE):
        print("Supervisor not running.")
        return
    data = read_pidfile()
    if not data:
        print("No valid pidfile found.")
        remove_pidfile()
        return
    spid = int(data["supervisor_pid"])
    if is_pid_alive(spid):
        os.kill(spid, signal.SIGTERM)
        print(f"Sent SIGTERM to supervisor (pid {spid}).")
    else:
        print("Supervisor already stopped.")
        remove_pidfile()

# ---------- CLI ----------

def main():
    parser = argparse.ArgumentParser(prog="queuectl", description="CLI-based background job queue")
    sub = parser.add_subparsers(dest="cmd")

    # enqueue
    p_enq = sub.add_parser("enqueue", help="Enqueue a job (JSON string or path to JSON)")
    p_enq.add_argument("job_json", help="JSON string or path to JSON file")

    # worker start/stop
    p_worker = sub.add_parser("worker", help="Manage workers")
    subw = p_worker.add_subparsers(dest="wcmd")
    p_ws = subw.add_parser("start", help="Start workers (supervisor mode)")
    p_ws.add_argument("--count", type=int, default=1, help="Number of workers")
    subw.add_parser("stop", help="Stop workers (graceful)")

    # status
    sub.add_parser("status", help="Show queue and worker status")

    # list jobs
    p_list = sub.add_parser("list", help="List jobs")
    p_list.add_argument("--state", choices=["pending","processing","completed","failed","dead"], help="Filter by state")
    p_list.add_argument("--limit", type=int, default=100)

    # dlq
    p_dlq = sub.add_parser("dlq", help="Dead Letter Queue ops")
    sdlq = p_dlq.add_subparsers(dest="dcmd")
    sdlq.add_parser("list", help="List DLQ jobs")
    p_retry = sdlq.add_parser("retry", help="Retry DLQ job")
    p_retry.add_argument("job_id")

    # config
    p_cfg = sub.add_parser("config", help="Configuration")
    sc = p_cfg.add_subparsers(dest="ccmd")
    p_cfg_set = sc.add_parser("set", help="Set a config value")
    p_cfg_set.add_argument("key", choices=["max-retries","backoff-base","poll-interval-sec","job-timeout-sec"])
    p_cfg_set.add_argument("value")
    sc.add_parser("show", help="Show all config")

    args = parser.parse_args()

    if args.cmd == "enqueue":
        try:
            enqueue_job(args.job_json)
        except Exception as e:
            print(f"Failed to enqueue: {e}")
            sys.exit(1)

    elif args.cmd == "worker":
        if args.wcmd == "start":
            supervisor_start(args.count)
        elif args.wcmd == "stop":
            supervisor_stop()
        else:
            print("Specify 'start' or 'stop'")
            sys.exit(1)

    elif args.cmd == "status":
        queue_status()

    elif args.cmd == "list":
        list_jobs(state=args.state, limit=args.limit)

    elif args.cmd == "dlq":
        if args.dcmd == "list":
            list_dlq()
        elif args.dcmd == "retry":
            retry_dlq(args.job_id)
        else:
            print("Specify 'list' or 'retry <job_id>'")
            sys.exit(1)

    elif args.cmd == "config":
        if args.ccmd == "set":
            key_map = {
                "max-retries": "max_retries",
                "backoff-base": "backoff_base",
                "poll-interval-sec": "poll_interval_sec",
                "job-timeout-sec": "job_timeout_sec",
            }
            set_config(key_map[args.key], args.value)
            print(f"Set {args.key} = {args.value}")
        elif args.ccmd == "show":
            cfg = get_config()
            rows = [(k, v) for k, v in sorted(cfg.items())]
            print_table(rows, ["key","value"])
        else:
            print("Use: queuectl config show | queuectl config set <key> <value>")
            sys.exit(1)

    else:
        parser.print_help()

if __name__ == "__main__":
    main()
