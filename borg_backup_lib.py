"""
Library module for Steam Depot backups using DepotDownloader and Borg.
Encapsulates the logic from steam_borg_backup.py for reuse.
"""

import json
import os
import re
import shutil
import ssl
import subprocess
import urllib.parse
import urllib.request
import urllib.error
import socket
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class ManifestItem:
    manifest_id: str
    depot_id: str
    seen_date: str
    seen_dt: datetime
    appid: str


@dataclass
class BackupConfig:
    work_dir: str = os.environ.get("WORK_DIR", "/mnt/z/depots")
    manifest_api_url: str = os.environ.get(
        "MANIFEST_API_URL",
        "https://n8n-tcloud-gz-4c8g.sliots.com/webhook/steamdb_manifest",
    )
    api_timeout: int = int(os.environ.get("API_TIMEOUT", "15"))
    verify_ssl: bool = os.environ.get("VERIFY_SSL", "1") in ("1", "true", "True")
    user_agent: str = os.environ.get("USER_AGENT", "steam-borg-backup/1.0")
    depot_downloader_cmd: str = os.environ.get("DEPOT_DOWNLOADER_CMD", "DepotDownloader")
    depot_downloader_extra_args: List[str] = None
    borg_cmd: str = os.environ.get("BORG_CMD", "borg")
    steam_username: str = os.environ.get("STEAM_USERNAME", "sliots")
    steam_password: Optional[str] = os.environ.get("STEAM_PASSWORD")
    dry_run: bool = os.environ.get("DRY_RUN", "0") in ("1", "true", "True")
    api_retries: int = int(os.environ.get("API_RETRIES", "3"))
    retry_backoff_sec: float = float(os.environ.get("RETRY_BACKOFF", "2.0"))

    def __post_init__(self):
        if self.depot_downloader_extra_args is None:
            self.depot_downloader_extra_args = ["-remember-password", "-all-platforms"]


def parse_iso_datetime(s: str) -> datetime:
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        if "." in s:
            s_no_frac = s.split(".")[0]
        else:
            s_no_frac = s
        if "+" in s_no_frac:
            s_no_tz = s_no_frac.split("+")[0]
        elif "-" in s_no_frac[19:]:
            s_no_tz = s_no_frac[:19]
        else:
            s_no_tz = s_no_frac
        dt = datetime.strptime(s_no_tz, "%Y-%m-%dT%H:%M:%S")
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def borg_timestamp(dt: datetime) -> str:
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


class SteamBorgBackup:
    def __init__(self, config: BackupConfig):
        self.cfg = config
        self._log_buffer: List[str] = []
        self._log_buffering: bool = False

    def log(self, msg: str) -> None:
        line = f"[steam-borg] {msg}"
        if self._log_buffering:
            self._log_buffer.append(line)
        else:
            print(line)

    def _start_buffering_logs(self) -> None:
        self._log_buffer = []
        self._log_buffering = True

    def _stop_buffering_logs(self) -> None:
        self._log_buffering = False

    def get_log_buffer(self) -> List[str]:
        return list(self._log_buffer)

    def clear_log_buffer(self) -> None:
        self._log_buffer.clear()

    def run_cmd(self, args: List[str], cwd: Optional[Path] = None, capture_output: bool = False):
        self.log(f"RUN: {' '.join(args)} (cwd={cwd})")
        if self.cfg.dry_run:
            self.log("DRY-RUN: skipping execution")
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

    def build_manifest_url(self, base_url: str, appid: str, depot_id: str) -> str:
        parsed = urllib.parse.urlparse(base_url)
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

    def _is_retryable_error(self, e: Exception) -> bool:
        # Treat common transient network/SSL errors as retryable
        if isinstance(e, (socket.timeout, TimeoutError, ssl.SSLError)):
            return True
        if isinstance(e, urllib.error.HTTPError):
            try:
                return int(e.code) >= 500
            except Exception:
                return False
        if isinstance(e, urllib.error.URLError):
            r = getattr(e, "reason", None)
            if isinstance(r, (socket.timeout, TimeoutError, ssl.SSLError)):
                return True
            if isinstance(r, str):
                msg = r.lower()
                if (
                    "timed out" in msg
                    or "timeout" in msg
                    or "unexpected eof" in msg
                    or "eof occurred" in msg
                    or "connection reset" in msg
                    or "remote host closed" in msg
                ):
                    return True
        msg = str(e).lower()
        return (
            "timed out" in msg
            or "timeout" in msg
            or "unexpected eof" in msg
            or "eof occurred" in msg
            or "connection reset" in msg
            or "remote host closed" in msg
        )

    def _read_json_from_url(self, url: str, timeout: int, verify_ssl: bool) -> Dict:
        parsed = urllib.parse.urlparse(url)
        attempt = 0
        while True:
            try:
                if parsed.scheme in ("http", "https"):
                    req = urllib.request.Request(url, headers={"User-Agent": self.cfg.user_agent})
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
            except Exception as e:
                if self._is_retryable_error(e) and attempt < int(getattr(self.cfg, "api_retries", 3)):
                    delay = float(getattr(self.cfg, "retry_backoff_sec", 2.0)) * (2 ** attempt)
                    self.log(f"接口请求异常，第 {attempt + 1} 次重试，等待 {delay:.1f}s: {e}")
                    time.sleep(delay)
                    attempt += 1
                    continue
                raise

    def fetch_manifests(self, appid: str, depot_id: str) -> List[ManifestItem]:
        url = self.build_manifest_url(self.cfg.manifest_api_url, appid, depot_id)
        self.log(f"Fetching manifests: {url}")
        payload = self._read_json_from_url(url, timeout=self.cfg.api_timeout, verify_ssl=self.cfg.verify_ssl)
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
        self.log(f"Fetched {len(manifests)} manifests (sorted by seen_date ascending).")
        return manifests

    def ensure_repo_dir(self, appid: str, depot_id: str) -> Path:
        root = Path(self.cfg.work_dir)
        root.mkdir(parents=True, exist_ok=True)
        repo_dir = root / f"{appid}_{depot_id}"
        repo_dir.mkdir(parents=True, exist_ok=True)
        return repo_dir

    def ensure_borg_repo(self, repo_dir: Path) -> None:
        borg_dir = repo_dir / ".borg"
        if not borg_dir.exists():
            self.log("Initializing borg repo (.borg)")
            self.run_cmd([self.cfg.borg_cmd, "init", "--encryption=none", ".borg"], cwd=repo_dir)
        else:
            self.log("Borg repo exists (.borg)")

    def list_borg_archives(self, repo_dir: Path) -> List[Dict]:
        if self.cfg.dry_run:
            self.log("DRY-RUN: list archives returns empty")
            return []
        try:
            cp = self.run_cmd([self.cfg.borg_cmd, "list", ".borg", "--json"], cwd=repo_dir, capture_output=True)
            payload = json.loads(cp.stdout)
            archives = payload.get("archives", [])
            norm_archives = []
            for a in archives:
                name = str(a.get("name"))
                time_str = str(a.get("time"))
                dt = parse_iso_datetime(time_str)
                norm_archives.append({"name": name, "time": dt})
            norm_archives.sort(key=lambda x: x["time"])  # ascending
            self.log(f"Found {len(norm_archives)} borg archives (JSON).")
            return norm_archives
        except Exception as e:
            self.log(f"borg list --json failed ({e}); trying plain output.")

        try:
            cp2 = self.run_cmd([self.cfg.borg_cmd, "list", ".borg"], cwd=repo_dir, capture_output=True)
            lines = cp2.stdout.splitlines()
            norm_archives = []
            pat = re.compile(r"^(?P<name>\S+)\s+(?P<date>\w{3},\s+\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})")
            for ln in lines:
                m = pat.match(ln.strip())
                if not m:
                    continue
                name = m.group("name")
                date_str = m.group("date")
                try:
                    dt = datetime.strptime(date_str, "%a, %Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                except Exception:
                    dt = datetime.now(timezone.utc)
                norm_archives.append({"name": name, "time": dt})
            norm_archives.sort(key=lambda x: x["time"])  # ascending
            self.log(f"Found {len(norm_archives)} borg archives (plain parse).")
            return norm_archives
        except Exception as e:
            self.log(f"borg list plain failed ({e}); treating as no archives.")
            return []

    def extract_borg_archive(self, repo_dir: Path, archive_name: str) -> None:
        self.log(f"Extracting latest archive: {archive_name}")
        self.run_cmd([self.cfg.borg_cmd, "extract", f".borg::{archive_name}"], cwd=repo_dir)

    def borg_create_archive(self, repo_dir: Path, manifest_id: str, timestamp: datetime) -> None:
        ts_str = borg_timestamp(timestamp)
        self.log(f"Creating borg archive {manifest_id} with timestamp {ts_str}")
        self.run_cmd([
            self.cfg.borg_cmd,
            "create",
            "--stats",
            "-e", ".borg",
            f".borg::{manifest_id}",
            "./",
            "--timestamp", ts_str,
        ], cwd=repo_dir)

    def depot_download_manifest(
        self,
        repo_dir: Path,
        appid: str,
        depot_id: str,
        manifest_id: str,
    ) -> None:
        self.log(f"Downloading manifest {manifest_id} for app {appid}, depot {depot_id}")
        args = [
            self.cfg.depot_downloader_cmd,
            "-username", self.cfg.steam_username,
            "-dir", "./",
            "-app", str(appid),
            "-depot", str(depot_id),
            "-manifest", str(manifest_id),
        ] + self.cfg.depot_downloader_extra_args
        if self.cfg.steam_password:
            args.extend(["-password", self.cfg.steam_password])
        self.run_cmd(args, cwd=repo_dir)

    def ensure_clean_repo_tree(self, repo_dir: Path) -> None:
        for p in repo_dir.iterdir():
            if p.name == ".borg":
                continue
            if p.is_dir():
                shutil.rmtree(p)
            else:
                p.unlink()

    def orchestrate_backup(self, appid: str, depot_id: str) -> bool:
        """Run backup pipeline. Returns True if any new archive was created, else False.

        All logs are buffered and should be printed by the caller conditionally.
        """
        self._start_buffering_logs()
        try:
            repo_dir = self.ensure_repo_dir(appid, depot_id)
            self.log(f"Working in {repo_dir}")
            self.ensure_borg_repo(repo_dir)
            manifests = self.fetch_manifests(appid, depot_id)
            manifest_by_id: Dict[str, ManifestItem] = {m.manifest_id: m for m in manifests}
            archives = self.list_borg_archives(repo_dir)
            existing_ids = {a["name"] for a in archives}
            latest_archive_name: Optional[str] = None
            if archives:
                latest_archive_name = archives[-1]["name"]
                self.log(f"Latest existing archive: {latest_archive_name}")
            # Ensure dir clean before extract or download
            self.ensure_clean_repo_tree(repo_dir)
            newer_to_process = [m for m in manifests if m.manifest_id not in existing_ids]
            if latest_archive_name and newer_to_process:
                self.extract_borg_archive(repo_dir, latest_archive_name)
            for m in manifests:
                if m.manifest_id in existing_ids:
                    self.log(f"Skip already archived manifest {m.manifest_id}")
                    continue
                self.depot_download_manifest(repo_dir, appid, depot_id, m.manifest_id)
                self.borg_create_archive(repo_dir, m.manifest_id, m.seen_dt)
                existing_ids.add(m.manifest_id)
            # Final cleanup
            self.ensure_clean_repo_tree(repo_dir)
            self.log("Completed. Cleaned directory, only .borg remains.")
            did_change = len(newer_to_process) > 0
            return did_change
        finally:
            # Stop buffering but keep buffer for the caller to decide to print or discard
            self._stop_buffering_logs()