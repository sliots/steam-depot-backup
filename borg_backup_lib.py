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
from dataclasses import dataclass, fields, MISSING
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any

try:
    import tomllib
except ImportError:
    try:
        import tomli as tomllib
    except ImportError:
        tomllib = None


@dataclass
class ManifestItem:
    manifest_id: str
    depot_id: str
    seen_date: str
    seen_dt: datetime
    appid: str


def _load_toml_file(path: str) -> Dict[str, Any]:
    if not tomllib:
        return {}
    try:
        with open(path, "rb") as f:
            return tomllib.load(f)
    except Exception:
        return {}


_ENV_CONVERTERS = {
    "api_timeout": int,
    "verify_ssl": lambda v: v.lower() in ("1", "true", "yes", "on"),
    "dry_run": lambda v: v.lower() in ("1", "true", "yes", "on"),
    "api_retries": int,
    "retry_backoff_sec": float,
    "loop_interval_sec": int,
}


@dataclass
class BackupConfig:
    work_dir: str = "/mnt/z/depots/data"
    manifest_api_url: str = ""
    list_api_url: str = ""
    api_timeout: int = 15
    verify_ssl: bool = True
    user_agent: str = "steam-borg-backup/1.0"
    depot_downloader_cmd: str = "DepotDownloader"
    depot_downloader_extra_args: Optional[List[str]] = None
    borg_cmd: str = "borg"
    steam_username: str = ""
    steam_password: Optional[str] = None
    dry_run: bool = False
    api_retries: int = 3
    retry_backoff_sec: float = 2.0
    loop_interval_sec: int = 0

    def __post_init__(self):
        if self.depot_downloader_extra_args is None:
            self.depot_downloader_extra_args = ["-remember-password", "-all-platforms"]

    @classmethod
    def from_strategies(cls, config_path: Optional[str] = None, **cli_args) -> "BackupConfig":
        """
        Load configuration with precedence:
        1. CLI Arguments (passed as kwargs, non-None)
        2. Environment Variables
        3. Config File (TOML)
        4. Defaults (class attributes)
        """
        # 1. Defaults (depot_downloader_extra_args uses None as its field default and
        # is populated by __post_init__, so including None here is intentional)
        config_data = {
            f.name: f.default
            for f in fields(cls)
            if f.default is not MISSING
        }
        
        # 2. Config File
        toml_data = {}
        target_path = config_path
        if not target_path:
            # Try default locations
            if os.path.exists("config.toml"):
                target_path = "config.toml"
            elif os.path.exists(os.path.join(os.path.dirname(__file__), "config.toml")):
                target_path = os.path.join(os.path.dirname(__file__), "config.toml")
        
        if target_path and os.path.exists(target_path):
            toml_data = _load_toml_file(target_path)
            # Filter unknown keys to avoid TypeError
            valid_keys = {f.name for f in fields(cls)}
            toml_data = {k: v for k, v in toml_data.items() if k in valid_keys}
            config_data.update(toml_data)

        # 3. Environment Variables
        env_map = {
            "WORK_DIR": "work_dir",
            "MANIFEST_API_URL": "manifest_api_url",
            "LIST_API_URL": "list_api_url",
            "API_TIMEOUT": "api_timeout",
            "VERIFY_SSL": "verify_ssl",
            "USER_AGENT": "user_agent",
            "DEPOT_DOWNLOADER_CMD": "depot_downloader_cmd",
            "BORG_CMD": "borg_cmd",
            "STEAM_USERNAME": "steam_username",
            "STEAM_PASSWORD": "steam_password",
            "DRY_RUN": "dry_run",
            "API_RETRIES": "api_retries",
            "RETRY_BACKOFF": "retry_backoff_sec",
            "LOOP_INTERVAL": "loop_interval_sec",
        }
        
        for env_key, field_name in env_map.items():
            val = os.environ.get(env_key)
            if val is not None:
                converter = _ENV_CONVERTERS.get(field_name)
                if converter is not None:
                    try:
                        config_data[field_name] = converter(val)
                    except (ValueError, TypeError):
                        pass
                else:
                    config_data[field_name] = val

        # 4. CLI Arguments (Overrides)
        for k, v in cli_args.items():
            if v is not None:
                config_data[k] = v

        return cls(**config_data)


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


def normalize_pairs(payload: Any) -> List[Dict[str, str]]:
    """Normalize API response into a list of {appid, depot_id} dicts."""
    if isinstance(payload, list):
        raw = payload
    elif isinstance(payload, dict):
        if "data" in payload and isinstance(payload["data"], list):
            raw = payload["data"]
        elif "pairs" in payload and isinstance(payload["pairs"], list):
            raw = payload["pairs"]
        else:
            raw = []
    else:
        raw = []

    out: List[Dict[str, str]] = []
    for it in raw:
        try:
            appid = str(it.get("appid") or it.get("app_id") or it.get("app"))
            depot_id = str(it.get("depot_id") or it.get("depot"))
        except Exception:
            continue
        if not appid or not depot_id or appid == "None" or depot_id == "None":
            continue
        out.append({"appid": appid, "depot_id": depot_id})
    return out


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
            print(line, flush=True)

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
        print(f"[steam-borg] RUN: {' '.join(args)} (cwd={cwd})", flush=True)
        if self.cfg.dry_run:
            self.log("DRY-RUN: skipping execution")
            return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")
        return subprocess.run(
            args,
            cwd=str(cwd) if cwd else None,
            check=True,
            text=True,
            capture_output=capture_output,
            stdin=subprocess.DEVNULL if capture_output else None,
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
                    path = Path(parsed.path)
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

    def fetch_pairs(self, url: str) -> List[Dict[str, str]]:
        self.log(f"Fetching backup pairs list: {url}")
        payload = self._read_json_from_url(url.strip(), timeout=self.cfg.api_timeout, verify_ssl=self.cfg.verify_ssl)
        pairs = normalize_pairs(payload)
        self.log(f"Fetched {len(pairs)} appid/depot_id pairs.")
        return pairs

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

    def _parse_borg_json(self, stdout: str) -> List[Dict]:
        payload = json.loads(stdout)
        return [
            {"name": str(a.get("name")), "time": parse_iso_datetime(str(a.get("time")))}
            for a in payload.get("archives", [])
        ]

    def _parse_borg_plain(self, stdout: str) -> List[Dict]:
        pat = re.compile(r"^(?P<name>\S+)\s+(?P<date>\w{3},\s+\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})")
        result = []
        for ln in stdout.splitlines():
            m = pat.match(ln.strip())
            if not m:
                continue
            try:
                dt = datetime.strptime(m.group("date"), "%a, %Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            except Exception:
                dt = datetime.now(timezone.utc)
            result.append({"name": m.group("name"), "time": dt})
        return result

    def list_borg_archives(self, repo_dir: Path) -> List[Dict]:
        if self.cfg.dry_run:
            self.log("DRY-RUN: list archives returns empty")
            return []
        parse_mode = "JSON"
        try:
            cp = self.run_cmd([self.cfg.borg_cmd, "list", ".borg", "--json"], cwd=repo_dir, capture_output=True)
            norm_archives = self._parse_borg_json(cp.stdout)
        except Exception as e:
            self.log(f"borg list --json failed ({e}); trying plain output.")
            parse_mode = "plain"
            try:
                cp2 = self.run_cmd([self.cfg.borg_cmd, "list", ".borg"], cwd=repo_dir, capture_output=True)
                norm_archives = self._parse_borg_plain(cp2.stdout)
            except Exception as e2:
                self.log(f"borg list plain failed ({e2}); treating as no archives.")
                return []
        norm_archives.sort(key=lambda x: x["time"])
        self.log(f"Found {len(norm_archives)} borg archives ({parse_mode}).")
        return norm_archives

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
        for p in list(repo_dir.iterdir()):
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