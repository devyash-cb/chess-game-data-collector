#!/usr/bin/env python3
"""Stage2 dataset analyser for GitHub Actions.

The Chess API may return between one and five legal candidate moves per FEN.
Processing is checkpointed in large batches and split across bounded jobs.
"""
from __future__ import annotations
import argparse, json, os, random, re, shutil, sqlite3, subprocess, sys, time, uuid, zipfile, threading
from datetime import datetime, timezone
import traceback
from pathlib import Path
from typing import Any

import requests
import websocket

ROOT = Path("stage2ds")
TEMP_STORAGE = Path("stage2dstempstorage")
TEMP_ZIP = Path("stage2dstempstorage.zip")
DB_PATH = ROOT / "progress.sqlite"
ERROR_DIR = ROOT / "errorlogs"
DRIVE_URL = os.getenv("DRIVE_FOLDER_URL", "https://drive.google.com/drive/folders/1FEext-LcjonVRFSHeav0Kxb2xOhS70n9")
API_WS = "wss://chess-api.com/v1"
API_DEPTH = 18
API_VARIANTS = 5
API_MAX_TIME = 100
REQUEST_GAP = 0.1
BATCH_MIN, BATCH_MAX = 900, 1100
PAUSE_MIN, PAUSE_MAX = 20, 40
CHECKPOINT_BATCHES = 5
MAX_FILES_PER_JOB = 10
EXPECTED_FILES = 125
EXPECTED_POSITIONS = 10000
SAFE_JOB_MINUTES = 350


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required GitHub secret/environment variable: {name}")
    return value


def db_open() -> sqlite3.Connection:
    ROOT.mkdir(exist_ok=True)
    con = sqlite3.connect(DB_PATH, timeout=60)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=FULL")
    con.executescript("""
    CREATE TABLE IF NOT EXISTS files(
      file_name TEXT PRIMARY KEY, dataset_id TEXT NOT NULL,
      total INTEGER NOT NULL, last_index INTEGER NOT NULL DEFAULT 0,
      status TEXT NOT NULL DEFAULT 'pending', updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS errors(
      dataset_id TEXT NOT NULL, file_name TEXT NOT NULL, position_index INTEGER NOT NULL,
      fen TEXT NOT NULL, error TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'failed',
      retry_reserved INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL,
      PRIMARY KEY(dataset_id, position_index)
    );
    CREATE TABLE IF NOT EXISTS recent(
      dataset_id TEXT NOT NULL, file_name TEXT NOT NULL, position_index INTEGER NOT NULL,
      fen TEXT NOT NULL, ca_json TEXT NOT NULL, updated_at TEXT NOT NULL,
      PRIMARY KEY(dataset_id, position_index)
    );
    CREATE TABLE IF NOT EXISTS runtime(key TEXT PRIMARY KEY, value TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS jobs(
      job_number INTEGER PRIMARY KEY, first_file TEXT NOT NULL, last_file TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending', runs INTEGER NOT NULL DEFAULT 0,
      updated_at TEXT NOT NULL
    );
    """)
    con.commit()
    return con


def runtime_set(con, **values):
    for k, v in values.items():
        con.execute("INSERT INTO runtime(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (k, str(v)))
    con.commit()


def runtime_get(con, key, default=""):
    row = con.execute("SELECT value FROM runtime WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def atomic_json(path: Path, obj: Any):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def error_path(file_name: str) -> Path:
    return ERROR_DIR / (Path(file_name).stem + ".txt")


def append_error(file_name: str, index: int, fen: str, message: str, tag="ERROR"):
    ERROR_DIR.mkdir(parents=True, exist_ok=True)
    p = error_path(file_name)
    if not p.exists():
        p.write_text("Fen\tindex\terror\tstatus\n", encoding="utf-8")
    clean = str(message).replace("\r", " ").replace("\n", " ").replace("\t", " ")
    prefix = f"{fen}\t{index}\t"
    if any(line.startswith(prefix) and line.endswith("\t" + tag) for line in p.read_text(encoding="utf-8").splitlines()[1:]):
        return
    with p.open("a", encoding="utf-8") as f:
        f.write(f"{fen}\t{index}\t{clean}\t{tag}\n")


def telegram_call(method: str, **data):
    token = require_env("TELEGRAM_BOT_TOKEN")
    url = f"https://api.telegram.org/bot{token}/{method}"
    response = requests.post(url, data=data, timeout=30)
    response.raise_for_status()
    payload = response.json()
    if not payload.get("ok"):
        raise RuntimeError(f"Telegram {method} failed: {payload}")
    return payload["result"]


def tg_message(text: str, strict=False):
    try:
        chat_id = require_env("TELEGRAM_CHAT_ID")
        telegram_call("sendMessage", chat_id=chat_id, text=text, disable_web_page_preview=True)
    except Exception as exc:
        print(f"Telegram message warning: {exc}")
        if strict: raise


def tg_document(path: Path, caption: str, strict=False):
    try:
        chat_id = require_env("TELEGRAM_CHAT_ID")
        token = require_env("TELEGRAM_BOT_TOKEN")
        with path.open("rb") as f:
            r = requests.post(f"https://api.telegram.org/bot{token}/sendDocument",
                              data={"chat_id": chat_id, "caption": caption[:900]},
                              files={"document": (path.name, f, "application/zip" if path.suffix.lower() == ".zip" else "text/plain")}, timeout=90)
        r.raise_for_status()
        result = r.json()
        if not result.get("ok"):
            raise RuntimeError(f"Telegram sendDocument failed: {result}")
    except Exception as exc:
        print(f"Telegram document warning: {exc}")
        if strict: raise


def status_text(con):
    current_file = runtime_get(con, "current_file", "None")
    current_index = runtime_get(con, "current_index", "-")
    state = runtime_get(con, "status", "idle")
    batch = runtime_get(con, "batch_length", "-")
    next_at = runtime_get(con, "next_batch_at", "")
    wait = "-"
    if next_at:
        try:
            wait = f"{max(0, int(float(next_at)-time.time()))} sec"
        except Exception:
            pass
    errs = con.execute("SELECT file_name,position_index,error FROM errors WHERE status IN ('failed','final_failed','retry_reserved') ORDER BY updated_at DESC LIMIT 3").fetchall()
    rec = con.execute("SELECT file_name,position_index,ca_json FROM recent ORDER BY updated_at DESC LIMIT 3").fetchall()
    progress_line = "File progress: -"
    if state == "downloading_drive":
        progress_line = f"Drive download: {runtime_get(con, 'download_completed', '0')}/{runtime_get(con, 'download_total', str(EXPECTED_FILES))} files complete; consecutive errors: {runtime_get(con, 'download_consecutive_errors', '0')}/3"
    frow = con.execute("SELECT dataset_id,total,last_index FROM files WHERE file_name=?", (current_file,)).fetchone()
    if frow:
        failed_n = con.execute("SELECT COUNT(*) n FROM errors WHERE dataset_id=? AND status IN ('failed','final_failed','retry_reserved')", (frow["dataset_id"],)).fetchone()["n"]
        done_n = int(frow["last_index"])
        progress_line = f"File progress: {done_n}/{frow['total']} checkpointed; {failed_n} failed; {max(0, frow['total']-done_n)} not yet checkpointed"
    lines = ["STAGE2DS STATUS", f"State: {state}", f"File: {current_file}", f"Position: {current_index}", progress_line, f"Batch length: {batch}", f"Next batch in: {wait}", "", "Last 3 errors:"]
    lines += [f"- {x['file_name']} #{x['position_index']}: {x['error'][:150]}" for x in errs] or ["- None"]
    lines += ["", "Last 3 analysed positions:"]
    for x in rec:
        try: moves = json.loads(x["ca_json"])
        except Exception: moves = []
        lines.append(f"- {x['file_name']} #{x['position_index']}: {', '.join(moves[:5])}")
    if not rec: lines.append("- None")
    return "\n".join(lines)


def send_error_log(con, all_logs=False):
    ERROR_DIR.mkdir(parents=True, exist_ok=True)
    logs = sorted(ERROR_DIR.glob("*.txt"), key=lambda p: p.stat().st_mtime, reverse=True)
    if all_logs:
        if not logs:
            empty = ERROR_DIR / "no-errors.txt"
            empty.write_text("No error logs have been created yet.\\n", encoding="utf-8")
            logs = [empty]
        archive = Path("/tmp/stage2ds-errorlogs.zip")
        archive.unlink(missing_ok=True)
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as z:
            for path in logs:
                z.write(path, path.name)
        tg_document(archive, f"Stage2DS error logs ({len(logs)} files)")
        return
    current = runtime_get(con, "current_file", "")
    preferred = error_path(current) if current and current not in ("-", "None") else None
    target = preferred if preferred and preferred.exists() else (logs[0] if logs else None)
    if target is None:
        target = ERROR_DIR / "no-errors.txt"
        target.write_text("No error log exists yet.\\n", encoding="utf-8")
    tg_document(target, f"Error log: {target.name} (may be incomplete/current)")


def progress_text(con):
    total_files = con.execute("SELECT COUNT(*) n FROM files").fetchone()["n"]
    completed_files = con.execute("SELECT COUNT(*) n FROM files WHERE status='completed'").fetchone()["n"]
    total_positions = con.execute("SELECT COALESCE(SUM(total),0) n FROM files").fetchone()["n"]
    checkpointed = con.execute("SELECT COALESCE(SUM(last_index),0) n FROM files").fetchone()["n"]
    failed = con.execute("SELECT COUNT(*) n FROM errors WHERE status IN ('failed','final_failed','retry_reserved')").fetchone()["n"]
    return (f"STAGE2DS PROGRESS\\nFiles: {completed_files}/{total_files} completed\\n"
            f"Positions checkpointed/attempted: {checkpointed:,}/{total_positions:,}\\n"
            f"Outstanding errors: {failed:,}")


def job_text(con):
    paths = sorted(ROOT.glob("*.json"), key=lambda p: p.name.lower())
    if not paths:
        return "No dataset files are available yet."
    rows = {r["file_name"]: r for r in con.execute("SELECT file_name,status FROM files").fetchall()}
    first = next((i for i, p in enumerate(paths) if p.name not in rows or rows[p.name]["status"] != "completed"), None)
    total_jobs = (len(paths) + MAX_FILES_PER_JOB - 1) // MAX_FILES_PER_JOB
    if first is None:
        return f"All {total_jobs} logical jobs have completed normal processing."
    number = first // MAX_FILES_PER_JOB + 1
    start = (number - 1) * MAX_FILES_PER_JOB
    group = paths[start:start + MAX_FILES_PER_JOB]
    record = con.execute("SELECT status,runs FROM jobs WHERE job_number=?", (number,)).fetchone()
    state = record["status"] if record else "pending"
    runs = record["runs"] if record else 0
    return (f"CURRENT LOGICAL JOB: {number}/{total_jobs}\\n"
            f"Files: {group[0].name} — {group[-1].name} ({len(group)} files)\\n"
            f"State: {state}\\nWorkflow runs used: {runs}\\n"
            "The next run resumes this same job until all its files are fully traversed.")


def poll_bot(con, force=False):
    if not os.getenv("TELEGRAM_BOT_TOKEN") or not os.getenv("TELEGRAM_CHAT_ID"):
        return
    last = float(runtime_get(con, "last_bot_poll", "0") or 0)
    if not force and time.time() - last < 5:
        return
    runtime_set(con, last_bot_poll=time.time())
    offset = int(runtime_get(con, "telegram_offset", "0") or 0)
    try:
        updates = telegram_call("getUpdates", offset=offset, timeout=0, allowed_updates='["message"]')
    except Exception as exc:
        print(f"Telegram polling warning: {exc}")
        return
    allowed = str(os.getenv("TELEGRAM_CHAT_ID", ""))
    for update in updates:
        runtime_set(con, telegram_offset=int(update["update_id"]) + 1)
        msg = update.get("message", {})
        chat = str(msg.get("chat", {}).get("id", ""))
        raw_text = (msg.get("text") or "").strip()
        command = raw_text.split()[0].split("@")[0].lower() if raw_text else ""
        if chat != allowed:
            continue
        try:
            if command in ("/ping", "/status"):
                tg_message(status_text(con))
            elif command == "/progress":
                tg_message(progress_text(con))
            elif command in ("/job", "/jobs"):
                tg_message(job_text(con))
            elif command == "/error":
                send_error_log(con, all_logs=False)
            elif command in ("/errors", "/errorlogs"):
                send_error_log(con, all_logs=True)
            elif command in ("/help", "/start"):
                tg_message(
                    "STAGE2DS COMMANDS\\n"
                    "/status or /ping — current processor status\\n"
                    "/progress — completed files and checkpointed positions\\n"
                    "/job — current logical job and its fixed file range\\n"
                    "/error — send current/latest TXT error log, including an incomplete one\\n"
                    "/errors — send a ZIP containing all TXT error logs"
                )
        except Exception as exc:
            print(f"Could not process Telegram command {command}: {exc}")
            tg_message(f"Command {command} failed: {str(exc)[:500]}")


class TelegramPoller:
    """Background Telegram command polling so /ping works during every active job."""
    def __init__(self):
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, name="stage2ds-telegram-poller", daemon=True)

    def start(self):
        if os.getenv("TELEGRAM_BOT_TOKEN") and os.getenv("TELEGRAM_CHAT_ID"):
            self.thread.start()
        return self

    def _run(self):
        con = None
        try:
            con = db_open()
            while not self.stop_event.is_set():
                try:
                    poll_bot(con, force=True)
                except Exception as exc:
                    print(f"Telegram background poll warning: {exc}")
                self.stop_event.wait(2)
        finally:
            if con:
                con.close()

    def stop(self):
        self.stop_event.set()
        if self.thread.is_alive():
            self.thread.join(timeout=5)


def parse_rank(data: dict) -> int | None:
    for key in ("multipv", "multiPv", "rank", "variant"):
        value = data.get(key)
        if isinstance(value, int) and 1 <= value <= 5:
            return value
    debug = str(data.get("debug", ""))
    match = re.search(r"\bmultipv\s+(\d+)\b", debug, re.I)
    if match:
        n = int(match.group(1))
        if 1 <= n <= 5: return n
    return None


def chess_api_http_probe(fen: str) -> dict:
    """Check whether the same FEN is accepted through the documented HTTPS API."""
    payload = {
        "fen": fen,
        "depth": API_DEPTH,
        "variants": API_VARIANTS,
        "maxThinkingTime": API_MAX_TIME,
        "taskId": "stage2ds-http-probe-" + uuid.uuid4().hex[:12],
    }
    response = requests.post(
        "https://chess-api.com/v1",
        json=payload,
        headers={"Accept": "application/json", "User-Agent": "Stage2DS/1.0"},
        timeout=(15, 60),
    )
    try:
        data = response.json()
    except Exception:
        raise RuntimeError(
            f"Chess API HTTPS probe returned non-JSON: "
            f"HTTP {response.status_code}; body={response.text[:2000]!r}"
        )
    if not response.ok:
        raise RuntimeError(
            f"Chess API HTTPS probe failed: HTTP {response.status_code}; "
            f"response={json.dumps(data, ensure_ascii=False)[:3000]}"
        )
    if not isinstance(data, dict):
        raise RuntimeError(
            f"Chess API HTTPS probe returned unexpected JSON type "
            f"{type(data).__name__}: {str(data)[:2000]}"
        )
    if data.get("type") == "error" or data.get("error"):
        raise RuntimeError(
            "Chess API rejected the sample FEN/request over HTTPS: "
            + json.dumps(data, ensure_ascii=False)[:3000]
        )
    if not data.get("move"):
        raise RuntimeError(
            "Chess API HTTPS probe returned HTTP success but no move field: "
            + json.dumps(data, ensure_ascii=False)[:3000]
        )
    return {"http_status": response.status_code, "request": payload, "response": data}


def parse_rank(data: dict) -> int | None:
    for key in ("multipv", "multiPv", "rank", "variant"):
        value = data.get(key)
        if isinstance(value, int) and 1 <= value <= 5:
            return value
    debug = str(data.get("debug", ""))
    match = re.search(r"\bmultipv\s+(\d+)\b", debug, re.I)
    if match:
        n = int(match.group(1))
        if 1 <= n <= 5:
            return n
    return None


class ChessSocket:
    def __init__(self):
        self.ws = None
        self.connect()

    def connect(self):
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass
        # Use the site's browser origin and a longer handshake timeout.
        self.ws = websocket.create_connection(
            API_WS,
            timeout=15,
            enable_multithread=True,
        )

    def analyse(self, fen: str) -> tuple[list[str], list[dict]]:
        if self.ws is None or not getattr(self.ws, "connected", False):
            self.connect()

        task_id = uuid.uuid4().hex[:12]
        payload = {
            "fen": fen,
            "depth": API_DEPTH,
            "variants": API_VARIANTS,
            "maxThinkingTime": API_MAX_TIME,
            "taskId": task_id,
        }
        try:
            self.ws.send(json.dumps(payload))
        except Exception as exc:
            raise RuntimeError(
                f"Chess API WebSocket send failed: {type(exc).__name__}: {exc}; "
                f"taskId={task_id}; request={json.dumps(payload, ensure_ascii=False)}"
            ) from exc

        found: dict[int, dict] = {}
        raw_messages = []
        # The API may replace our client taskId with a server-generated one.
        # Track the first response ID and use it to group this single in-flight request.
        response_task_id = None
        response_task_ids = set()
        best_seen = False
        last_message = time.monotonic()
        deadline = time.monotonic() + 90

        while time.monotonic() < deadline:
            self.ws.settimeout(0.5 if (best_seen or len(found) == 5) else 5)
            try:
                raw = self.ws.recv()
            except websocket.WebSocketTimeoutException:
                if (best_seen or len(found) == 5) and time.monotonic() - last_message >= 1.0:
                    break
                continue
            except Exception as exc:
                close_status = getattr(self.ws, "status", None)
                close_reason = getattr(self.ws, "close_reason", None)
                raise RuntimeError(
                    f"Chess API WebSocket receive failed: {type(exc).__name__}: {exc}; "
                    f"close_status={close_status!r}; close_reason={close_reason!r}; "
                    f"connected={getattr(self.ws, 'connected', None)!r}; "
                    f"requested_taskId={task_id}; response_taskIds={sorted(response_task_ids)}; "
                    f"request={json.dumps(payload, ensure_ascii=False)}; "
                    f"recent_raw_messages={json.dumps(raw_messages[-20:], ensure_ascii=False)[:6000]}"
                ) from exc

            try:
                message = json.loads(raw)
            except Exception:
                raw_messages.append({"unparsed_message": str(raw)[:2000]})
                continue

            batch = message if isinstance(message, list) else [message]
            for item in batch:
                if not isinstance(item, dict):
                    continue
                raw_messages.append(item)

                # Do not require the server to echo our client taskId. The HTTPS
                # endpoint has returned a different server-generated ID in testing.
                # Since this socket sends one position at a time, the first response
                # ID identifies the active stream; subsequent IDs are retained in
                # diagnostics but ignored if they belong to another stream.
                item_task_id = item.get("taskId")
                if item_task_id is not None:
                    item_task_id = str(item_task_id)
                    response_task_ids.add(item_task_id)
                    if response_task_id is None:
                        response_task_id = item_task_id
                    elif item_task_id != response_task_id:
                        continue

                if item.get("type") == "error" or item.get("error"):
                    raise RuntimeError(
                        "Chess API WebSocket returned an API error: "
                        + json.dumps({
                            "request": payload,
                            "requested_taskId": task_id,
                            "response_taskIds": sorted(response_task_ids),
                            "error_message": item,
                            "recent_raw_messages": raw_messages[-20:],
                        }, ensure_ascii=False)[:7000]
                    )
                rank = parse_rank(item)
                move = item.get("move")
                if item.get("type") == "bestmove":
                    best_seen = True
                    if rank is None:
                        rank = 1
                # Some API responses omit a MultiPV rank. Keep such candidate
                # moves by assigning the next unused rank in arrival order.
                if move and rank is None:
                    used_moves = {str(v.get("move")) for v in found.values() if v.get("move")}
                    if str(move) not in used_moves:
                        rank = next((n for n in range(1, 6) if n not in found), None)
                if rank and move:
                    old = found.get(rank)
                    old_depth = int(old.get("depth") or 0) if old else -1
                    new_depth = int(item.get("depth") or 0)
                    if old is None or new_depth >= old_depth or item.get("type") == "bestmove":
                        found[rank] = item
                last_message = time.monotonic()

            if len(found) == 5 and time.monotonic() - last_message > 0.5:
                break

        ordered = [found[i] for i in range(1, 6) if i in found]
        moves = []
        for item in ordered:
            move = item.get("move")
            if move:
                move = str(move)
                if move not in moves:
                    moves.append(move)
        # One to five distinct legal moves are all valid. A position with no
        # usable move is the only move-count failure.
        if not moves:
            recent = raw_messages[-20:]
            raise RuntimeError(
                "Chess API WebSocket completed without any usable legal candidate move. "
                f"requested_taskId={task_id}; response_taskIds={sorted(response_task_ids)}. "
                f"Recent raw messages: {json.dumps(recent, ensure_ascii=False)[:6000]}"
            )
        return moves[:5], raw_messages

    def close(self):
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass


def analyse_with_retries(sock: ChessSocket, fen: str, attempts: int = 3) -> tuple[list[str], list[dict]]:
    """Retry transient WebSocket failures while preserving the actual exception."""
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            return sock.analyse(fen)
        except Exception as exc:
            last_error = exc
            transient = (
                isinstance(exc, (websocket.WebSocketException, OSError, TimeoutError))
                or "WebSocket" in str(exc)
            )
            if not transient:
                raise
            print(f"[WEBSOCKET RETRY] Attempt {attempt}/{attempts} failed: {type(exc).__name__}: {exc}")
            if attempt == attempts:
                break
            time.sleep(5 * attempt)
            try:
                sock.connect()
            except Exception as reconnect_error:
                print(
                    f"[RECONNECT WARNING] {type(reconnect_error).__name__}: "
                    f"{reconnect_error}"
                )
    raise RuntimeError(
        f"Chess API analysis failed after {attempts} attempts. "
        f"Last exact error: {type(last_error).__name__}: {last_error}"
    ) from last_error


def report_fatal_error(command: str):
    """Send the exact fatal traceback to Telegram as a TXT attachment."""
    details = traceback.format_exc()
    ERROR_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    log_path = ERROR_DIR / f"fatal-{command}-{stamp}.txt"
    extra = ""
    probe_path = ROOT / "preflight_http_probe.json"
    if probe_path.exists():
        try:
            extra = "\n\nLast HTTPS probe:\n" + probe_path.read_text(encoding="utf-8")[:12000]
        except Exception:
            pass
    log_path.write_text(
        f"Stage2DS fatal error\nCommand: {command}\nUTC: {now()}\n\n"
        f"Exact traceback:\n{details}{extra}\n",
        encoding="utf-8",
    )
    last_line = details.strip().splitlines()[-1] if details.strip() else "Unknown error"
    tg_message(
        f"STAGE2DS FATAL ERROR\nPhase: {command}\n"
        f"Exact error: {last_line}\n"
        f"Full traceback and request diagnostics are attached as TXT."
    )
    tg_document(log_path, f"Exact Stage2DS error log: {command}")



def create_zip():
    ROOT.mkdir(exist_ok=True)
    # SQLite runs in WAL mode. Merge committed WAL pages before packaging the DB.
    if DB_PATH.exists():
        checkpoint_db = sqlite3.connect(DB_PATH, timeout=60)
        try:
            checkpoint_db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            checkpoint_db.close()
    zip_path = Path("stage2ds.zip")
    if zip_path.exists(): zip_path.unlink()
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as z:
        for p in ROOT.rglob("*"):
            if p.is_file() and not p.name.endswith((".tmp", "-wal", "-shm")):
                z.write(p, Path(ROOT.name) / p.relative_to(ROOT))
    return zip_path


def publish_snapshot():
    zip_path = create_zip()
    repo = require_env("GITHUB_REPOSITORY")
    token = require_env("GH_TOKEN")
    env = os.environ.copy(); env["GH_TOKEN"] = token
    subprocess.run(["gh", "release", "upload", "stage2ds-latest", str(zip_path), "--clobber", "--repo", repo], check=True, env=env)
    return zip_path


def valid_downloaded_json(path: Path) -> bool:
    """Only treat a complete, correctly-shaped 10,000-position JSON as cached."""
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        return (isinstance(obj, dict) and isinstance(obj.get("positions"), list)
                and len(obj["positions"]) == EXPECTED_POSITIONS
                and all(k in obj for k in ("rating_range", "dataset_id", "positions")))
    except Exception:
        return False


def download_drive_resumable(max_consecutive_errors: int = 3):
    """Download missing Drive files individually into persistent stage2dstempstorage/."""
    import gdown

    TEMP_STORAGE.mkdir(parents=True, exist_ok=True)
    con = db_open()
    poller = TelegramPoller().start()
    try:
        runtime_set(con, status="downloading_drive", current_file="Drive folder listing",
                    current_index="-", batch_length="-", next_batch_at="")
        tg_message("STAGE2DS DOWNLOAD STARTED. Existing complete files in stage2dstempstorage/ will be skipped.")
        print("[DOWNLOAD] Listing the public Drive folder...")
        entries = gdown.download_folder(url=DRIVE_URL, skip_download=True, quiet=True)
        entries = [x for x in entries if str(getattr(x, "path", "")).lower().endswith(".json")]
        if len(entries) != EXPECTED_FILES:
            raise RuntimeError(f"Google Drive listing returned {len(entries)} JSON files; expected {EXPECTED_FILES}.")
        names = [Path(str(x.path)).name for x in entries]
        if len(set(names)) != len(names):
            raise RuntimeError("Google Drive folder contains duplicate JSON filenames.")

        completed = sum(1 for name in names if valid_downloaded_json(TEMP_STORAGE / name))
        runtime_set(con, download_total=len(entries), download_completed=completed,
                    download_consecutive_errors=0)
        consecutive_errors = 0
        last_progress_update = 0.0

        for number, item in enumerate(entries, 1):
            name = Path(str(item.path)).name
            dest = TEMP_STORAGE / name
            if valid_downloaded_json(dest):
                print(f"[SKIP] {name} already downloaded and validated.")
                continue
            if dest.exists():
                dest.unlink()  # discard an invalid final file; any .part resume data is retained

            runtime_set(con, status="downloading_drive", current_file=name,
                        current_index=f"{number}/{len(entries)}")
            print(f"[DOWNLOAD {number}/{len(entries)}] {name}")
            try:
                def progress_callback(done, total):
                    nonlocal last_progress_update
                    now_mono = time.monotonic()
                    if now_mono - last_progress_update >= 5:
                        runtime_set(con, download_file_bytes=done,
                                    download_file_total=total if total is not None else "unknown")
                        last_progress_update = now_mono

                # resume=True reuses partial .part files and skips completed output files.
                gdown.download(id=item.id, output=str(dest), quiet=False, resume=True,
                               retries=0, progress=progress_callback)
                if not valid_downloaded_json(dest):
                    raise RuntimeError("Downloaded file failed JSON/10,000-position validation.")
                consecutive_errors = 0
                completed += 1
                runtime_set(con, download_completed=completed, download_consecutive_errors=0)
                print(f"[OK] {name} ({completed}/{len(entries)})")
            except Exception as exc:
                consecutive_errors += 1
                runtime_set(con, download_consecutive_errors=consecutive_errors)
                print(f"[DOWNLOAD ERROR {consecutive_errors}/{max_consecutive_errors}] {name}: {exc}")
                tg_message(f"Drive download failed ({consecutive_errors}/{max_consecutive_errors} consecutive errors): {name}\n{str(exc)[:700]}")
                if dest.exists() and not valid_downloaded_json(dest):
                    dest.unlink(missing_ok=True)
                if consecutive_errors >= max_consecutive_errors:
                    runtime_set(con, status="download_paused_after_errors", current_file=name)
                    raise RuntimeError(
                        f"Stopped after {max_consecutive_errors} consecutive Drive download errors. "
                        f"{completed}/{len(entries)} complete files are preserved in {TEMP_STORAGE}/. "
                        "Rerun the workflow with phase=start; completed files will be skipped."
                    ) from exc
    
        missing = [name for name in names if not valid_downloaded_json(TEMP_STORAGE / name)]
        if missing:
            raise RuntimeError(f"Download pass ended with {len(missing)} missing/incomplete files: {missing[:10]}")
        runtime_set(con, status="drive_download_complete", download_completed=len(entries),
                    download_consecutive_errors=0, current_file="-", current_index="-")
        tg_message(f"DRIVE DOWNLOAD COMPLETE: all {len(entries)} JSON files are saved in {TEMP_STORAGE}/. The folder is retained for future reruns.")
        print(f"[DOWNLOAD COMPLETE] {len(entries)} JSON files are ready.")
    finally:
        poller.stop()
        con.close()


def create_temp_storage_zip():
    TEMP_STORAGE.mkdir(parents=True, exist_ok=True)
    if TEMP_ZIP.exists():
        TEMP_ZIP.unlink()
    with zipfile.ZipFile(TEMP_ZIP, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=4) as archive:
        for path in TEMP_STORAGE.rglob("*"):
            if path.is_file() and not path.name.endswith((".tmp", "-wal", "-shm")):
                archive.write(path, Path(TEMP_STORAGE.name) / path.relative_to(TEMP_STORAGE))
    if not TEMP_ZIP.exists() or TEMP_ZIP.stat().st_size == 0:
        raise RuntimeError("Could not create persistent Drive download cache ZIP.")
    return TEMP_ZIP


def prepare_dataset_from_temp():
    files = sorted(TEMP_STORAGE.glob("*.json"), key=lambda p: p.name.lower())
    if len(files) != EXPECTED_FILES:
        raise RuntimeError(f"{TEMP_STORAGE}/ contains {len(files)} JSON files; expected {EXPECTED_FILES}.")
    bad = [p.name for p in files if not valid_downloaded_json(p)]
    if bad:
        raise RuntimeError(f"Cached download contains invalid/incomplete files: {bad[:10]}")
    if ROOT.exists():
        shutil.rmtree(ROOT)
    ROOT.mkdir(parents=True)
    for src in files:
        shutil.copy2(src, ROOT / src.name)
    ERROR_DIR.mkdir(parents=True, exist_ok=True)


def validate_dataset(con):
    json_files = sorted(ROOT.glob("*.json"), key=lambda p: p.name.lower())
    if len(json_files) != EXPECTED_FILES:
        raise RuntimeError(f"stage2ds contains {len(json_files)} JSON files; expected {EXPECTED_FILES}.")
    seen_ids = set()
    for path in json_files:
        obj = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(obj, dict) or not isinstance(obj.get("positions"), list):
            raise RuntimeError(f"Invalid root JSON structure: {path.name}")
        if not all(k in obj for k in ("rating_range", "dataset_id", "positions")):
            raise RuntimeError(f"Missing root fields in {path.name}")
        if len(obj["positions"]) != EXPECTED_POSITIONS:
            raise RuntimeError(f"{path.name}: expected 10,000 positions, found {len(obj['positions'])}.")
        dsid = str(obj.get("dataset_id", ""))
        if not dsid or dsid in seen_ids: raise RuntimeError(f"Missing/duplicate dataset_id in {path.name}")
        seen_ids.add(dsid)
        for i, pos in enumerate(obj["positions"], 1):
            if not isinstance(pos, dict) or not pos.get("fen") or "index" not in pos or "ca" not in pos:
                raise RuntimeError(f"Bad position structure: {path.name}, item {i}")
            if int(pos["index"]) != i or "s8" not in pos or "s2" not in pos:
                raise RuntimeError(f"Unexpected position index/fields: {path.name}, item {i}")
            if not isinstance(pos["ca"], list): raise RuntimeError(f"ca must be a list: {path.name} #{i}")
        con.execute("INSERT OR IGNORE INTO files(file_name,dataset_id,total,last_index,status,updated_at) VALUES(?,?,?,?,?,?)",
                    (path.name, dsid, len(obj["positions"]), 0, "pending", now()))
    con.commit()


def preflight():
    print("[1/7] Preparing stage2ds/ from persistent stage2dstempstorage/...")
    prepare_dataset_from_temp()
    con = db_open()
    poller = TelegramPoller().start()
    try:
        runtime_set(con, status="preflight_validation", current_file="-", current_index="-")
        print("[2/7] Validating all 125 JSON files and 1,250,000 positions...")
        validate_dataset(con)
        print("[3/7] Testing SQLite write/read...")
        con.execute("INSERT OR REPLACE INTO runtime(key,value) VALUES('preflight_test','ok')")
        con.commit()
        if con.execute("SELECT value FROM runtime WHERE key='preflight_test'").fetchone()[0] != "ok":
            raise RuntimeError("SQLite read/write test failed")
        con.execute("DELETE FROM runtime WHERE key='preflight_test'"); con.commit()
        print("[4/7] Checking Telegram credentials, polling mode and send permission...")
        require_env("TELEGRAM_CHAT_ID")
        webhook = telegram_call("getWebhookInfo")
        if webhook.get("url"):
            raise RuntimeError("Telegram bot has a webhook configured. Disable the webhook before using /ping polling.")
        tg_message("STAGE2DS PREFLIGHT: Telegram connection and message delivery are working.", strict=True)
        test_doc = Path("/tmp/stage2ds_telegram_test.txt")
        test_doc.write_text("Stage2DS Telegram document upload test passed.\n", encoding="utf-8")
        tg_document(test_doc, "Preflight document-upload test", strict=True)
        test_doc.unlink(missing_ok=True)
        first = json.loads(sorted(ROOT.glob("*.json"), key=lambda p: p.name.lower())[0].read_text(encoding="utf-8"))
        fen = first["positions"][0]["fen"]
        print("[5/8] Testing the sample FEN through the documented Chess API HTTPS endpoint...")
        http_result = chess_api_http_probe(fen)
        atomic_json(ROOT / "preflight_http_probe.json", {"fen": fen, **http_result})
        print(
            "[HTTPS OK] The Chess API accepted this exact FEN and returned move "
            + str(http_result["response"].get("move"))
        )
        print("[6/8] Testing Chess API WebSocket candidate moves (1 to 5 allowed) on the same sample FEN...")
        sock = ChessSocket()
        try:
            moves, raw = analyse_with_retries(sock, fen)
            atomic_json(ROOT / "preflight_api_sample.json", {"fen": fen, "moves": moves, "messages": raw})
        finally:
            sock.close()
        print("[7/8] Testing full-folder ZIP creation...")
        z = create_zip()
        if not z.exists() or z.stat().st_size == 0:
            raise RuntimeError("ZIP test failed")
        print("[8/8] Preflight passed. GitHub release write is tested by the following workflow upload step.")
        runtime_set(con, status="preflight_passed", current_file="-", current_index="-",
                    batch_length="-", next_batch_at="")
        tg_message("STAGE2DS PREFLIGHT PASSED. All 125 files validated; SQLite, Telegram, Chess API HTTPS and WebSocket candidate-move response, and ZIP creation passed. Processing will now start.")
    finally:
        poller.stop()
        con.close()


def restore_json_from_errors(con, obj):
    """Keep error state in SQLite; JSON ca is authoritative for successful results."""
    return obj


def load_file(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def save_file(path: Path, obj):
    atomic_json(path, obj)


def checkpoint(con, file_path=None, file_obj=None):
    if file_path and file_obj is not None: save_file(file_path, file_obj)
    con.commit()
    publish_snapshot()


def pause_with_poll(con, seconds: int):
    runtime_set(con, status="break", next_batch_at=time.time() + seconds)
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        time.sleep(min(2, max(0.1, end - time.monotonic())))
    runtime_set(con, status="processing", next_batch_at="")


def complete_file(con, path: Path, obj):
    dsid = str(obj["dataset_id"])
    row = con.execute("SELECT total,last_index FROM files WHERE dataset_id=?", (dsid,)).fetchone()
    total = len(obj["positions"])
    attempted = con.execute("SELECT COUNT(*) AS n FROM errors WHERE dataset_id=?", (dsid,)).fetchone()["n"]
    last_index = row["last_index"] if row else 0
    # A file is complete when every index was either successful (<= last_index) or recorded as failed.
    failed_indexes = {r[0] for r in con.execute("SELECT position_index FROM errors WHERE dataset_id=?", (dsid,)).fetchall()}
    if last_index >= total:
        con.execute("UPDATE files SET status='completed',updated_at=? WHERE dataset_id=?", (now(), dsid)); con.commit()
        log = error_path(path.name)
        if not log.exists(): log.write_text("No errors recorded.\n", encoding="utf-8")
        tg_message(f"FILE COMPLETED: {path.name}\nPositions: {total}\nFailed: {attempted}\nSuccessful: {total-attempted}")
        tg_document(log, f"Error log for {path.name}")
        return True
    return False


def process_job(max_minutes=SAFE_JOB_MINUTES, files_per_job=MAX_FILES_PER_JOB):
    con = db_open()
    poller = TelegramPoller().start()
    sock = ChessSocket()
    deadline = time.monotonic() + max_minutes * 60
    paths = sorted(ROOT.glob("*.json"), key=lambda p: p.name.lower())
    if not paths:
        raise RuntimeError("No Stage2DS JSON dataset files were found.")
    rows = {r["file_name"]: r for r in con.execute("SELECT file_name,status FROM files").fetchall()}
    first_incomplete = next((i for i, p in enumerate(paths)
                             if p.name not in rows or rows[p.name]["status"] != "completed"), None)
    if first_incomplete is None:
        sock.close()
        poller.stop()
        con.close()
        tg_message("All logical processing jobs are complete. Starting the final one-time retry phase.")
        dispatch("retry")
        return

    job_number = first_incomplete // files_per_job + 1
    group_start = (job_number - 1) * files_per_job
    job_paths = paths[group_start:group_start + files_per_job]
    first_name, last_name = job_paths[0].name, job_paths[-1].name
    con.execute(
        "INSERT INTO jobs(job_number,first_file,last_file,status,runs,updated_at) VALUES(?,?,?,'running',1,?) "
        "ON CONFLICT(job_number) DO UPDATE SET status='running',runs=jobs.runs+1,updated_at=excluded.updated_at",
        (job_number, first_name, last_name, now()),
    )
    con.commit()
    tg_message(
        f"LOGICAL JOB {job_number}/{(len(paths)+files_per_job-1)//files_per_job} STARTED.\\n"
        f"Fixed file range: {first_name} — {last_name} ({len(job_paths)} files).\\n"
        f"Run limit: {max_minutes} minutes. This run resumes only this job."
    )
    runtime_set(con, status="processing", current_job=job_number, job_started=now())
    batch_target = random.randint(BATCH_MIN, BATCH_MAX)
    batch_count = 0
    batches_since_checkpoint = 0
    stop = False
    files_completed_this_run = 0
    current_path = None
    current_obj = None
    last_attempted_idx = 0

    try:
        for path in job_paths:
            current_path = path
            obj = load_file(path)
            current_obj = obj
            dsid = str(obj["dataset_id"])
            total = len(obj["positions"])
            file_row = con.execute(
                "SELECT last_index,status FROM files WHERE dataset_id=?", (dsid,)
            ).fetchone()
            if file_row and file_row["status"] == "completed":
                continue
            last_index = int(file_row["last_index"] if file_row else 0)
            last_attempted_idx = last_index
            tg_message(f"JOB {job_number}: FILE RESUMED/STARTED: {path.name} from position {last_index+1}/{total}")
            con.execute(
                "UPDATE files SET status='processing',updated_at=? WHERE dataset_id=?",
                (now(), dsid),
            )
            con.commit()

            for pos in obj["positions"]:
                idx = int(pos["index"])
                if idx <= last_index:
                    continue
                # 350 minutes = 5 hours 50 minutes; 10 minutes remain for final upload/shutdown.
                if time.monotonic() >= deadline:
                    stop = True
                    break
                runtime_set(
                    con, current_file=path.name, current_index=idx,
                    status="processing", batch_length=batch_target,
                )
                try:
                    moves, _ = sock.analyse(pos["fen"])
                    # One, two, three, four or five distinct candidate moves are valid.
                    pos["ca"] = moves
                    con.execute(
                        "INSERT OR REPLACE INTO recent(dataset_id,file_name,position_index,fen,ca_json,updated_at) VALUES(?,?,?,?,?,?)",
                        (dsid, path.name, idx, pos["fen"], json.dumps(moves), now()),
                    )
                    con.execute(
                        "DELETE FROM recent WHERE rowid IN "
                        "(SELECT rowid FROM recent ORDER BY updated_at DESC LIMIT -1 OFFSET 3)"
                    )
                except Exception as exc:
                    msg = str(exc)[:1000]
                    append_error(path.name, idx, pos["fen"], msg)
                    con.execute(
                        "INSERT OR REPLACE INTO errors(dataset_id,file_name,position_index,fen,error,status,retry_reserved,updated_at) "
                        "VALUES(?,?,?,?,?,'failed',0,?)",
                        (dsid, path.name, idx, pos["fen"], msg, now()),
                    )
                    print(f"ERROR {path.name} #{idx}: {msg}")
                    sock.close()
                    time.sleep(5)

                last_attempted_idx = idx
                batch_count += 1
                time.sleep(REQUEST_GAP)
                if batch_count >= batch_target:
                    save_file(path, obj)
                    con.execute(
                        "UPDATE files SET last_index=?,updated_at=? WHERE dataset_id=?",
                        (idx, now(), dsid),
                    )
                    con.commit()
                    last_index = idx
                    batch_count = 0
                    batches_since_checkpoint += 1
                    if batches_since_checkpoint >= CHECKPOINT_BATCHES:
                        checkpoint(con)
                        batches_since_checkpoint = 0
                    pause = random.randint(PAUSE_MIN, PAUSE_MAX)
                    tg_message(
                        f"JOB {job_number} BATCH COMPLETED: {path.name} through position {idx}. "
                        f"Batch size: {batch_target}. Break: {pause} seconds."
                    )
                    pause_with_poll(con, pause)
                    batch_target = random.randint(BATCH_MIN, BATCH_MAX)

            if stop:
                break
            save_file(path, obj)
            con.execute(
                "UPDATE files SET last_index=?,status='completed',updated_at=? WHERE dataset_id=?",
                (total, now(), dsid),
            )
            con.commit()
            log = error_path(path.name)
            if not log.exists():
                log.write_text("No errors recorded.\\n", encoding="utf-8")
            failed = con.execute(
                "SELECT COUNT(*) n FROM errors WHERE dataset_id=? AND status IN ('failed','final_failed','retry_reserved')",
                (dsid,),
            ).fetchone()["n"]
            tg_message(
                f"FILE COMPLETED: {path.name}\\nPositions: {total}\\n"
                f"Normal-pass errors awaiting final retry: {failed}"
            )
            tg_document(log, f"Error log for {path.name}")
            batch_count = 0
            files_completed_this_run += 1
            checkpoint(con)

        if stop and current_path is not None and current_obj is not None and batch_count:
            save_file(current_path, current_obj)
            con.execute(
                "UPDATE files SET last_index=?,status='processing',updated_at=? WHERE dataset_id=?",
                (last_attempted_idx, now(), str(current_obj["dataset_id"])),
            )
            con.commit()

        group_complete = True
        for path in job_paths:
            row = con.execute("SELECT status FROM files WHERE file_name=?", (path.name,)).fetchone()
            if not row or row["status"] != "completed":
                group_complete = False
                break
        if group_complete:
            con.execute("UPDATE jobs SET status='completed',updated_at=? WHERE job_number=?", (now(), job_number))
            runtime_set(con, status="job_completed", current_index="-")
            tg_message(
                f"LOGICAL JOB {job_number} COMPLETED: all {len(job_paths)} assigned files are fully traversed. "
                "Recorded errors remain queued for the single final retry phase."
            )
        else:
            con.execute("UPDATE jobs SET status='paused',updated_at=? WHERE job_number=?", (now(), job_number))
            runtime_set(con, status="job_paused_checkpointed", current_index="-")
            tg_message(
                f"LOGICAL JOB {job_number} PAUSED AT THE {max_minutes}-MINUTE LIMIT. "
                "Checkpoint saved. The next workflow run resumes this same job."
            )
        con.commit()
        checkpoint(con)
    finally:
        sock.close()
        poller.stop()
        con.close()

    con2 = db_open()
    remaining = con2.execute("SELECT COUNT(*) n FROM files WHERE status!='completed'").fetchone()["n"]
    con2.close()
    next_phase = "continue" if remaining else "retry"
    tg_message(
        f"JOB RUN FINISHED. Logical job: {job_number}. Files completed this run: {files_completed_this_run}. "
        f"Files remaining overall: {remaining}. Next phase: {next_phase}."
    )
    dispatch(next_phase)


def retry_job(max_minutes=SAFE_JOB_MINUTES):
    con = db_open(); poller = TelegramPoller().start(); sock = ChessSocket()
    start = time.monotonic(); deadline = start + max_minutes * 60
    tg_message("FINAL RETRY JOB STARTED: each recorded failed position will receive at most one final retry.")
    runtime_set(con, status="final_retry", current_file="-", current_index="-")
    try:
        rows = con.execute("SELECT * FROM errors WHERE status='failed' AND retry_reserved=0 ORDER BY file_name,position_index").fetchall()
        retry_batch_target = random.randint(BATCH_MIN, BATCH_MAX)
        retry_batch_count = 0
        for row in rows:
            if time.monotonic() >= deadline - 900: break
            # Reserve the one permitted retry before sending the request.
            con.execute("UPDATE errors SET retry_reserved=1,status='retry_reserved',updated_at=? WHERE dataset_id=? AND position_index=?",
                        (now(), row["dataset_id"], row["position_index"]))
            con.commit()
            # Persist reservation so a resumed retry run will never blindly repeat it.
            publish_snapshot()
            path = ROOT / row["file_name"]
            obj = load_file(path)
            target = next(p for p in obj["positions"] if int(p["index"]) == int(row["position_index"]))
            runtime_set(con, current_file=row["file_name"], current_index=row["position_index"], status="final_retry", batch_length=retry_batch_target)
            try:
                # This is the single, separate final retry for this failed position.
                moves, _ = sock.analyse(row["fen"])
                target["ca"] = moves
                append_error(row["file_name"], row["position_index"], row["fen"], "Resolved by final retry", "RESOLVED")
                con.execute("UPDATE errors SET status='resolved',error=?,updated_at=? WHERE dataset_id=? AND position_index=?",
                            ("Resolved by final retry", now(), row["dataset_id"], row["position_index"]))
            except Exception as exc:
                msg = str(exc)[:1000]
                append_error(row["file_name"], row["position_index"], row["fen"], msg, "FINAL_RETRY_FAILED")
                con.execute("UPDATE errors SET status='final_failed',error=?,updated_at=? WHERE dataset_id=? AND position_index=?",
                            (msg, now(), row["dataset_id"], row["position_index"]))
                time.sleep(3)
            save_file(path, obj); con.commit(); publish_snapshot()
            time.sleep(REQUEST_GAP)
            retry_batch_count += 1
            if retry_batch_count >= retry_batch_target:
                retry_batch_count = 0
                retry_batch_target = random.randint(BATCH_MIN, BATCH_MAX)
                pause_with_poll(con, random.randint(PAUSE_MIN, PAUSE_MAX))
        # A retry_reserved entry means the runner stopped after reserving but before recording
        # a result. Do not retry it again; preserve an explicit uncertainty record in the TXT log.
        interrupted = con.execute("SELECT file_name,position_index,fen,dataset_id FROM errors WHERE status='retry_reserved'").fetchall()
        for item in interrupted:
            message = "Retry was reserved before an interruption; not repeated to preserve the one-time retry limit"
            append_error(item["file_name"], item["position_index"], item["fen"], message, "FINAL_RETRY_FAILED")
            con.execute("UPDATE errors SET status='final_failed',error=?,updated_at=? WHERE dataset_id=? AND position_index=?",
                        (message, now(), item["dataset_id"], item["position_index"]))
        con.commit()
        for path in sorted(ROOT.glob("*.json"), key=lambda p:p.name.lower()):
            dsid = str(load_file(path).get("dataset_id", ""))
            if con.execute("SELECT 1 FROM errors WHERE dataset_id=? AND status IN ('resolved','final_failed') LIMIT 1", (dsid,)).fetchone():
                log = error_path(path.name)
                if log.exists(): tg_document(log, f"Updated final error log for {path.name}")
    finally:
        sock.close(); poller.stop()
    con.commit()
    remaining = con.execute("SELECT COUNT(*) n FROM errors WHERE status='failed' AND retry_reserved=0").fetchone()["n"]
    con.close()
    publish_snapshot()
    if remaining:
        tg_message(f"Final retry is continuing in another run. Remaining retry candidates: {remaining}.")
        dispatch("retry")
    else:
        tg_message("STAGE2DS COMPLETE. All normal processing and the one-time final retry pass have finished. Download stage2ds.zip from the repository Releases page.")


def dispatch(phase: str):
    repo = require_env("GITHUB_REPOSITORY")
    token = require_env("GH_TOKEN")
    env = os.environ.copy(); env["GH_TOKEN"] = token
    try:
        subprocess.run(["gh", "workflow", "run", "stage2ds.yml", "--ref", os.getenv("GITHUB_REF_NAME", "main"), "-f", f"phase={phase}", "--repo", repo], check=True, env=env)
    except Exception as exc:
        try: tg_message(f"AUTOMATIC CONTINUATION FAILED: {exc}\nManually run workflow 'Stage2DS Processor' with phase={phase}.")
        except Exception: pass
        raise


def next_phase():
    con = db_open()
    remaining_files = con.execute("SELECT COUNT(*) n FROM files WHERE status!='completed'").fetchone()["n"]
    retry_left = con.execute("SELECT COUNT(*) n FROM errors WHERE status='failed' AND retry_reserved=0").fetchone()["n"]
    con.close()
    if remaining_files: print("continue")
    elif retry_left: print("retry")
    else: print("done")


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("download")
    sub.add_parser("temp-zip")
    sub.add_parser("preflight")
    p = sub.add_parser("process")
    p.add_argument("--max-minutes", type=int, default=SAFE_JOB_MINUTES)
    p.add_argument("--files-per-job", type=int, default=MAX_FILES_PER_JOB)
    r = sub.add_parser("retry"); r.add_argument("--max-minutes", type=int, default=SAFE_JOB_MINUTES)
    sub.add_parser("zip"); sub.add_parser("next-phase")
    args = parser.parse_args()
    try:
        if args.cmd == "download": download_drive_resumable(max_consecutive_errors=3)
        elif args.cmd == "temp-zip": print(create_temp_storage_zip())
        elif args.cmd == "preflight": preflight()
        elif args.cmd == "process": process_job(args.max_minutes, args.files_per_job)
        elif args.cmd == "retry": retry_job(args.max_minutes)
        elif args.cmd == "zip": print(create_zip())
        elif args.cmd == "next-phase": next_phase()
    except Exception:
        report_fatal_error(args.cmd)
        raise

if __name__ == "__main__":
    main()
