#!/usr/bin/env python3
"""
Steam Depot version backup tool using DepotDownloader and Borg.

Single-file, modular script. Necessary parameters are placed in constants
and can be overridden via CLI args or environment variables.

Flow (matches the provided specification):
1) Query manifest list for given `appid` and `depot_id` from an API endpoint.
2) Ensure working directory `/mnt/z/depots/{appid}_{depot_id}` exists; init borg repo if absent.
3) If repo exists, list archives. Ensure the dir is clean (only .borg). Extract latest archive.
4) Sort manifests by `seen_date`. For each manifest not yet archived:
   - Download with DepotDownloader for that manifest.
   - Create borg archive named by `manifest_id` with `--timestamp` from `seen_date`.
5) After processing, delete everything in the repo folder except `.borg`.

Example commands (shown for reference, handled by this script):
  borg init --encryption=none .borg
  DepotDownloader -username sliots -remember-password -all-platforms -dir ./ \
    -app 4075460 -depot 4075461 -manifest 652381323314602403
  borg create --stats -e .borg .borg::652381323314602403 ./ --timestamp 2025-11-03T06:16:03

Usage:
  python steam_borg_backup.py --appid 4075460 --depot 4075461 \
    [--work-dir /mnt/z/depots] [--api-url <URL>] [--username <name>] [--password <pass>] \
    [--downloader DepotDownloader] [--borg borg]

Notes:
- The manifest API must return a JSON object with a `data` list containing
  objects: {manifest_id, depot_id, seen_date, appid}.
- `seen_date` should be ISO8601, e.g. `2025-11-03T06:16:03.000Z`.
- DepotDownloader should be installed and accessible as a command (or provide full path).
- Borg should be installed and accessible as `borg` (or provide full path).
- The script uses only Python standard library.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import urllib.parse
import urllib.request
import ssl
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Dict, Tuple
import re


# =====================
# Configurable constants
# =====================

# Default working directory (WSL path as per requirement)
WORK_DIR = os.environ.get("WORK_DIR", "/mnt/z/depots")

# API endpoint to fetch manifests. Must accept appid and depot_id as query params.
# Example: http://localhost:3000/api/manifests?appid=4075460&depot_id=4075461
MANIFEST_API_URL = os.environ.get("MANIFEST_API_URL", "https://n8n-tcloud-gz-4c8g.sliots.com/webhook/steamdb_manifest")
API_TIMEOUT = int(os.environ.get("API_TIMEOUT", "15"))
VERIFY_SSL = os.environ.get("VERIFY_SSL", "1") in ("1", "true", "True")
USER_AGENT = os.environ.get("USER_AGENT", "steam-borg-backup/1.0")

# Executable names/paths
DEPOT_DOWNLOADER_CMD = os.environ.get("DEPOT_DOWNLOADER_CMD", "DepotDownloader")
BORG_CMD = os.environ.get("BORG_CMD", "borg")

# Steam credentials (prefer env vars for security)
STEAM_USERNAME = os.environ.get("STEAM_USERNAME", "sliots")
STEAM_PASSWORD = os.environ.get("STEAM_PASSWORD")  # optional; if None, rely on remember-password

# Extra flags for DepotDownloader
DEPOT_DOWNLOADER_EXTRA_ARGS = ["-remember-password", "-all-platforms"]

# Dry-run mode (skip executing external commands)
DRY_RUN = os.environ.get("DRY_RUN", "0") in ("1", "true", "True")


# =====================
# Data structures
# =====================

@dataclass
class ManifestItem:
    manifest_id: str
    depot_id: str
    seen_date: str
    seen_dt: datetime
    appid: str


# =====================
# Utilities
# =====================

def log(msg: str) -> None:
    print(f"[steam-borg] {msg}")


def run_cmd(args: List[str], cwd: Optional[Path] = None, capture_output: bool = False) -> subprocess.CompletedProcess:
    """Run a command with optional working dir and output capture. Honors DRY_RUN."""
    log(f"RUN: {' '.join(args)} (cwd={cwd})")
    if DRY_RUN:
        log("DRY-RUN: skipping execution")
        # Return a dummy CompletedProcess
        class _CP:
            def __init__(self):
                self.args = args
                self.returncode = 0
                self.stdout = ""
                self.stderr = ""
        return _CP()
    return subprocess.run(
        args,
        cwd=str(cwd) if cwd else None,
        check=True,
        text=True,
        capture_output=capture_output,
    )


def parse_iso_datetime(s: str) -> datetime:
    """Parse ISO8601 timestamps; handle trailing 'Z' and fractional seconds."""
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        # Fallback: strip fractional part and timezone
        if "." in s:
            s_no_frac = s.split(".")[0]
        else:
            s_no_frac = s
        if "+" in s_no_frac:
            s_no_tz = s_no_frac.split("+")[0]
        elif "-" in s_no_frac[19:]:  # handle tz like -03:00
            # naive approach: remove tz part after seconds
            s_no_tz = s_no_frac[:19]
        else:
            s_no_tz = s_no_frac
        dt = datetime.strptime(s_no_tz, "%Y-%m-%dT%H:%M:%S")
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def borg_timestamp(dt: datetime) -> str:
    """Format datetime for borg --timestamp (YYYY-MM-DDTHH:MM:SS, no timezone)."""
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def ensure_clean_repo_tree(repo_dir: Path) -> None:
    """Ensure repo dir contains only .borg (remove everything else)."""
    for p in repo_dir.iterdir():
        if p.name == ".borg":
            continue
        if p.is_dir():
            shutil.rmtree(p)
        else:
            p.unlink()


# =====================
# API client
# =====================

def build_manifest_url(base_url: str, appid: str, depot_id: str) -> str:
    parsed = urllib.parse.urlparse(base_url)
    # For http/https, attach query params; for other schemes (e.g. file://), leave as-is
    if parsed.scheme in ("http", "https"):
        q = urllib.parse.parse_qs(parsed.query)
        q["appid"] = [str(appid)]
        q["depot_id"] = [str(depot_id)]
        new_query = urllib.parse.urlencode({k: v[0] for k, v in q.items()})
        new_url = urllib.parse.urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                new_query,
                parsed.fragment,
            )
        )
        return new_url
    return base_url

def _read_json_from_url(url: str, timeout: int, verify_ssl: bool) -> Dict:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme in ("http", "https"):
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        ctx = ssl.create_default_context() if verify_ssl else ssl._create_unverified_context()
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            data = resp.read()
    elif parsed.scheme == "file":
        path = Path(urllib.request.url2pathname(parsed.path))
        with open(path, "rb") as f:
            data = f.read()
    else:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            data = resp.read()
    return json.loads(data)


def fetch_manifests(api_url: str, appid: str, depot_id: str, timeout: int = API_TIMEOUT, verify_ssl: bool = VERIFY_SSL) -> List[ManifestItem]:
    url = build_manifest_url(api_url, appid, depot_id)
    log(f"Fetching manifests: {url}")
    payload = _read_json_from_url(url, timeout=timeout, verify_ssl=verify_ssl)
    items = payload.get("data", [])
    manifests: List[ManifestItem] = []
    for it in items:
        m_id = str(it.get("manifest_id"))
        d_id = str(it.get("depot_id"))
        s_date = str(it.get("seen_date"))
        a_id = str(it.get("appid"))
        dt = parse_iso_datetime(s_date)
        manifests.append(ManifestItem(m_id, d_id, s_date, dt, a_id))
    manifests.sort(key=lambda x: x.seen_dt)
    log(f"Fetched {len(manifests)} manifests (sorted by seen_date ascending).")
    return manifests


# =====================
# Borg helpers
# =====================

def ensure_borg_repo(repo_dir: Path) -> None:
    borg_dir = repo_dir / ".borg"
    if not borg_dir.exists():
        log("Initializing borg repo (.borg)")
        run_cmd([BORG_CMD, "init", "--encryption=none", ".borg"], cwd=repo_dir)
    else:
        log("Borg repo exists (.borg)")


def list_borg_archives(repo_dir: Path) -> List[Dict]:
    """Return archives with name and time as datetime.
    Prefer JSON output; fallback to plain text parsing for older borg.
    """
    if DRY_RUN:
        log("DRY-RUN: list archives returns empty")
        return []

    # Try JSON first
    try:
        cp = run_cmd([BORG_CMD, "list", ".borg", "--json"], cwd=repo_dir, capture_output=True)
        payload = json.loads(cp.stdout)
        archives = payload.get("archives", [])
        norm_archives = []
        for a in archives:
            name = str(a.get("name"))
            time_str = str(a.get("time"))
            dt = parse_iso_datetime(time_str)
            norm_archives.append({"name": name, "time": dt})
        norm_archives.sort(key=lambda x: x["time"])  # ascending
        log(f"Found {len(norm_archives)} borg archives (JSON).")
        return norm_archives
    except Exception as e:
        log(f"borg list --json failed ({e}); trying plain output.")

    # Fallback: plain text
    try:
        cp2 = run_cmd([BORG_CMD, "list", ".borg"], cwd=repo_dir, capture_output=True)
        lines = cp2.stdout.splitlines()
        norm_archives = []
        # Example line:
        # 652381323314602403   Mon, 2025-11-03 14:16:03 [digest]
        pat = re.compile(r"^(?P<name>\S+)\s+(?P<date>\w{3},\s+\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})")
        for ln in lines:
            m = pat.match(ln.strip())
            if not m:
                # Skip unparsed lines silently
                continue
            name = m.group("name")
            date_str = m.group("date")
            try:
                dt = datetime.strptime(date_str, "%a, %Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            except Exception:
                dt = datetime.now(timezone.utc)
            norm_archives.append({"name": name, "time": dt})
        norm_archives.sort(key=lambda x: x["time"])  # ascending
        log(f"Found {len(norm_archives)} borg archives (plain parse).")
        return norm_archives
    except Exception as e:
        log(f"borg list plain failed ({e}); treating as no archives.")
        return []


def extract_borg_archive(repo_dir: Path, archive_name: str) -> None:
    log(f"Extracting latest archive: {archive_name}")
    run_cmd([BORG_CMD, "extract", f".borg::{archive_name}"], cwd=repo_dir)


def borg_create_archive(repo_dir: Path, manifest_id: str, timestamp: datetime) -> None:
    ts_str = borg_timestamp(timestamp)
    log(f"Creating borg archive {manifest_id} with timestamp {ts_str}")
    run_cmd([
        BORG_CMD,
        "create",
        "--stats",
        "-e", ".borg",
        f".borg::{manifest_id}",
        "./",
        "--timestamp", ts_str,
    ], cwd=repo_dir)


# =====================
# DepotDownloader
# =====================

def depot_download_manifest(
    repo_dir: Path,
    appid: str,
    depot_id: str,
    manifest_id: str,
    username: str,
    password: Optional[str],
    downloader_cmd: str,
) -> None:
    log(f"Downloading manifest {manifest_id} for app {appid}, depot {depot_id}")
    args = [
        downloader_cmd,
        "-username", username,
        "-dir", "./",
        "-app", str(appid),
        "-depot", str(depot_id),
        "-manifest", str(manifest_id),
    ] + DEPOT_DOWNLOADER_EXTRA_ARGS
    if password:
        args.extend(["-password", password])
    run_cmd(args, cwd=repo_dir)


# =====================
# Orchestration
# =====================

def ensure_repo_dir(work_dir: str, appid: str, depot_id: str) -> Path:
    root = Path(work_dir)
    root.mkdir(parents=True, exist_ok=True)
    repo_dir = root / f"{appid}_{depot_id}"
    repo_dir.mkdir(parents=True, exist_ok=True)
    return repo_dir


def orchestrate_backup(
    appid: str,
    depot_id: str,
    work_dir: str,
    api_url: str,
    downloader_cmd: str,
    borg_cmd: str,
    username: str,
    password: Optional[str],
    api_timeout: int = API_TIMEOUT,
    verify_ssl: bool = VERIFY_SSL,
) -> None:
    global BORG_CMD
    BORG_CMD = borg_cmd  # allow override via CLI

    repo_dir = ensure_repo_dir(work_dir, appid, depot_id)
    log(f"Working in {repo_dir}")

    # Ensure borg repo initialized
    ensure_borg_repo(repo_dir)

    # Fetch manifest list and sort
    manifests = fetch_manifests(api_url, appid, depot_id, timeout=api_timeout, verify_ssl=verify_ssl)
    manifest_by_id: Dict[str, ManifestItem] = {m.manifest_id: m for m in manifests}

    # List existing borg archives
    archives = list_borg_archives(repo_dir)
    existing_ids = {a["name"] for a in archives}
    latest_archive_name: Optional[str] = None
    if archives:
        latest_archive_name = archives[-1]["name"]
        log(f"Latest existing archive: {latest_archive_name}")

    # Ensure dir clean before extract or download
    ensure_clean_repo_tree(repo_dir)

    # Extract latest existing archive if present and we have newer manifests to process
    newer_to_process = [m for m in manifests if m.manifest_id not in existing_ids]
    if latest_archive_name and newer_to_process:
        extract_borg_archive(repo_dir, latest_archive_name)

    # Process manifests in ascending seen_date order, only those not yet archived
    for m in manifests:
        if m.manifest_id in existing_ids:
            log(f"Skip already archived manifest {m.manifest_id}")
            continue
        # Download this manifest
        depot_download_manifest(repo_dir, appid, depot_id, m.manifest_id, username, password, downloader_cmd)
        # Create borg archive with timestamp from seen_date
        borg_create_archive(repo_dir, m.manifest_id, m.seen_dt)
        existing_ids.add(m.manifest_id)

    # Final cleanup: leave only .borg
    ensure_clean_repo_tree(repo_dir)
    log("Completed. Cleaned directory, only .borg remains.")


# =====================
# CLI
# =====================

def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Steam depot backup via DepotDownloader + Borg")
    parser.add_argument("--appid", required=True, help="Steam App ID, e.g. 4075460")
    parser.add_argument("--depot", required=True, help="Steam Depot ID, e.g. 4075461")
    parser.add_argument("--work-dir", default=WORK_DIR, help="Working base directory (default: /mnt/z/depots)")
    parser.add_argument("--api-url", default=MANIFEST_API_URL, help="Manifest API base URL")
    parser.add_argument("--downloader", default=DEPOT_DOWNLOADER_CMD, help="DepotDownloader command or path")
    parser.add_argument("--borg", default=BORG_CMD, help="borg command or path")
    parser.add_argument("--username", default=STEAM_USERNAME, help="Steam username")
    parser.add_argument("--password", default=STEAM_PASSWORD, help="Steam password (optional)")
    parser.add_argument("--dry-run", action="store_true", help="Skip executing external commands for verification")
    parser.add_argument("--insecure", action="store_true", help="Skip SSL verification for API requests")
    parser.add_argument("--timeout", type=int, default=API_TIMEOUT, help="API request timeout in seconds")
    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    global DRY_RUN
    if getattr(args, "dry_run", False):
        DRY_RUN = True
    try:
        orchestrate_backup(
            appid=str(args.appid),
            depot_id=str(args.depot),
            work_dir=str(args.work_dir),
            api_url=str(args.api_url),
            downloader_cmd=str(args.downloader),
            borg_cmd=str(args.borg),
            username=str(args.username),
            password=str(args.password) if args.password else None,
            api_timeout=int(getattr(args, "timeout", API_TIMEOUT)),
            verify_ssl=not bool(getattr(args, "insecure", False)),
        )
        return 0
    except subprocess.CalledProcessError as e:
        log(f"Command failed: {e}\nstdout:\n{getattr(e, 'stdout', '')}\nstderr:\n{getattr(e, 'stderr', '')}")
        return e.returncode if hasattr(e, "returncode") else 1
    except Exception as e:
        log(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))