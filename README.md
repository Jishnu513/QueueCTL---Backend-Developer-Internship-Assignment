
Demo Drive Link:https://drive.google.com/file/d/1ffIGkuu6gu5XtsNMSqcQhZgfIgKXxOu3/view?usp=drive_link

# queuectl

A minimal, production-grade **CLI background job queue** with workers, retries (exponential backoff), and a **Dead Letter Queue (DLQ)** — implemented in Python with SQLite persistence.

> **Why this design?**  
> - **SQLite + WAL** gives durable, crash-safe storage without extra services.  
> - **Transactional claim** prevents duplicate job execution across multiple workers.  
> - **Supervisor** manages N workers and supports graceful shutdown.

---

## ✨ Features

- Enqueue JSON jobs (persistent)
- Multiple workers in parallel (`worker start --count N`)
- Exponential backoff retries: `delay = base ** attempts`
- DLQ after retry exhaustion (`state='dead'`)
- Configurable: `max_retries`, `backoff_base`, `poll_interval_sec`, optional `job_timeout_sec`
- Graceful shutdown: finish current job on stop
- Clean CLI with help text
- Minimal tests / demo script

---

## 🧰 Tech Stack

- Python 3.9+
- Standard library only (no external deps)
- SQLite (file at `~/.queuectl/queue.db`)

---

## ⚙️ Setup

```bash
git clone <your-repo-url>
cd <your-repo>
chmod +x queuectl.py
# optional: set custom home
# export QUEUECTL_HOME=/path/to/state

