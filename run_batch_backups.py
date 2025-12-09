#!/usr/bin/env python3
"""
Batch runner: fetches a list of {appid, depot_id} pairs from an API
and runs Steam+Borg backups sequentially. Continues on errors.
"""

import argparse
import json
import ssl
import sys
import subprocess
import urllib.parse
import urllib.request
import urllib.error
import socket
import time
import os
import ctypes
from typing import List, Dict, Optional

from borg_backup_lib import BackupConfig, SteamBorgBackup

ANSI_RESET = "\x1b[0m"
ANSI_BOLD = "\x1b[1m"
ANSI_RED = "\x1b[31m"
ANSI_GREEN = "\x1b[32m"
ANSI_YELLOW = "\x1b[33m"
ANSI_BLUE = "\x1b[34m"
ANSI_MAGENTA = "\x1b[35m"
ANSI_CYAN = "\x1b[36m"
ANSI_GRAY = "\x1b[90m"

def _print_status(message: str, color: Optional[str] = None, finalize: bool = False) -> None:
    """在当前行打印状态，支持颜色与是否换行。"""
    try:
        colored = (color or "") + message + (ANSI_RESET if color else "")
        sys.stdout.write("\r\x1b[2K" + colored)
        if finalize:
            sys.stdout.write("\n")
        sys.stdout.flush()
    except Exception:
        # 控制台异常时忽略状态行
        pass

def _enable_ansi_colors() -> None:
    try:
        if os.name == "nt":
            kernel32 = ctypes.windll.kernel32
            for handle_id in (-11, -12):  # STD_OUTPUT_HANDLE, STD_ERROR_HANDLE
                handle = kernel32.GetStdHandle(handle_id)
                mode = ctypes.c_uint()
                if kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
                    kernel32.SetConsoleMode(handle, mode.value | 0x0004)  # ENABLE_VIRTUAL_TERMINAL_PROCESSING
    except Exception:
        pass

def _is_timeout_error(e: Exception) -> bool:
    # 识别常见的超时异常类型与文案
    if isinstance(e, (socket.timeout, TimeoutError)):
        return True
    if isinstance(e, urllib.error.URLError):
        r = getattr(e, "reason", None)
        if isinstance(r, (socket.timeout, TimeoutError)):
            return True
        if isinstance(r, str) and ("timed out" in r.lower() or "timeout" in r.lower()):
            return True
    msg = str(e).lower()
    return "timed out" in msg or "timeout" in msg


def _read_json_from_url(
    url: str,
    timeout: int,
    verify_ssl: bool,
    user_agent: str,
    retries: int = 0,
    backoff_sec: float = 2.0,
) -> Dict:
    parsed = urllib.parse.urlparse(url)
    attempt = 0
    while True:
        try:
            if parsed.scheme in ("http", "https"):
                req = urllib.request.Request(url, headers={"User-Agent": user_agent})
                ctx = ssl.create_default_context() if verify_ssl else ssl._create_unverified_context()
                with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
                    data = resp.read()
            elif parsed.scheme == "file":
                path = urllib.request.url2pathname(parsed.path)
                with open(path, "rb") as f:
                    data = f.read()
            else:
                with urllib.request.urlopen(url, timeout=timeout) as resp:
                    data = resp.read()
            return json.loads(data)
        except Exception as e:
            if _is_timeout_error(e) and attempt < retries:
                delay = backoff_sec * (2 ** attempt)
                print(ANSI_YELLOW + f"[steam-borg] 接口请求超时，第 {attempt + 1} 次重试，等待 {delay:.1f}s" + ANSI_RESET)
                time.sleep(delay)
                attempt += 1
                continue
            raise


def normalize_pairs(payload: Dict) -> List[Dict[str, str]]:
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
        if not appid or not depot_id:
            continue
        out.append({"appid": appid, "depot_id": depot_id})
    return out


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch Steam depot backups via DepotDownloader + Borg")
    parser.add_argument("--list-api", required=True, help="返回需要备份的 appid/depot_id 列表的接口 URL (http/https/file)")
    parser.add_argument("--work-dir", default=BackupConfig().work_dir, help="工作目录 (默认: 环境变量或 /mnt/z/depots)")
    parser.add_argument("--api-url", default=BackupConfig().manifest_api_url, help="manifest 列表接口基础 URL")
    parser.add_argument("--downloader", default=BackupConfig().depot_downloader_cmd, help="DepotDownloader 命令或路径")
    parser.add_argument("--borg", default=BackupConfig().borg_cmd, help="borg 命令或路径")
    parser.add_argument("--username", default=BackupConfig().steam_username, help="Steam 用户名")
    parser.add_argument("--password", default=BackupConfig().steam_password, help="Steam 密码 (可选)")
    parser.add_argument("--dry-run", action="store_true", help="跳过外部命令，仅验证流程")
    parser.add_argument("--insecure", action="store_true", help="拉取接口时跳过 SSL 验证")
    parser.add_argument("--timeout", type=int, default=BackupConfig().api_timeout, help="接口请求超时秒数")
    parser.add_argument("--api-retries", type=int, default=3, help="接口请求超时重试次数 (默认: 3)")
    parser.add_argument("--retry-backoff", type=float, default=2.0, help="重试退避基础间隔秒 (默认: 2.0)")
    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    _enable_ansi_colors()
    args = parse_args(argv)

    cfg = BackupConfig(
        work_dir=str(args.work_dir),
        manifest_api_url=str(args.api_url),
        api_timeout=int(getattr(args, "timeout", BackupConfig().api_timeout)),
        verify_ssl=not bool(getattr(args, "insecure", False)),
        depot_downloader_cmd=str(args.downloader),
        borg_cmd=str(args.borg),
        steam_username=str(args.username),
        steam_password=(str(args.password) if args.password else None),
        dry_run=bool(getattr(args, "dry_run", False)),
        api_retries=int(getattr(args, "api_retries", 3)),
        retry_backoff_sec=float(getattr(args, "retry_backoff", 2.0)),
    )

    runner = SteamBorgBackup(cfg)

    try:
        payload = _read_json_from_url(
            url=str(args.list_api),
            timeout=cfg.api_timeout,
            verify_ssl=cfg.verify_ssl,
            user_agent=cfg.user_agent,
            retries=int(getattr(args, "api_retries", 3)),
            backoff_sec=float(getattr(args, "retry_backoff", 2.0)),
        )
    except Exception as e:
        print(f"[steam-borg] 拉取列表接口失败: {e}")
        return 1

    pairs = normalize_pairs(payload)
    print(ANSI_CYAN + f"[steam-borg] 将顺序执行 {len(pairs)} 组备份任务" + ANSI_RESET)

    ok = 0
    fail = 0
    for idx, p in enumerate(pairs, 1):
        appid = p["appid"]
        depot_id = p["depot_id"]
        sep = "=" * 80
        head_lines = [
            "\n" + ANSI_GRAY + sep + ANSI_RESET,
            ANSI_BOLD + ANSI_CYAN + f"[steam-borg] >>> 开始备份 ({idx}/{len(pairs)}) appid={appid} depot_id={depot_id}" + ANSI_RESET,
            ANSI_GRAY + sep + ANSI_RESET,
        ]
        # 单行提示：正在处理
        _print_status(
            f"[steam-borg] 正在处理 ({idx}/{len(pairs)}) appid={appid} depot_id={depot_id}",
            color=ANSI_CYAN,
            finalize=False,
        )
        try:
            did_change = runner.orchestrate_backup(appid=appid, depot_id=depot_id)
            if did_change:
                # 单行提示：完成，并换行后输出详细日志
                _print_status(
                    f"[steam-borg] 完成 ({idx}/{len(pairs)}) appid={appid} depot_id={depot_id}",
                    color=ANSI_GREEN,
                    finalize=True,
                )
                for ln in head_lines:
                    print(ln)
                for ln in runner.get_log_buffer():
                    print(ln)
                print(ANSI_GREEN + f"[steam-borg] <<< 任务完成 appid={appid} depot_id={depot_id}" + ANSI_RESET)
                print(ANSI_GRAY + sep + ANSI_RESET)
            # 清理缓冲，无论是否有变更
            runner.clear_log_buffer()
            if not did_change:
                # 单行提示：跳过，无新备份（不换行，下一轮覆盖）
                _print_status(
                    f"[steam-borg] 跳过（无新备份） ({idx}/{len(pairs)}) appid={appid} depot_id={depot_id}",
                    color=ANSI_GRAY,
                    finalize=False,
                )
            ok += 1
        except subprocess.CalledProcessError as e:
            # 单行提示：失败，并换行后输出详细日志
            _print_status(
                f"[steam-borg] 失败 ({idx}/{len(pairs)}) appid={appid} depot_id={depot_id}",
                color=ANSI_RED,
                finalize=True,
            )
            for ln in head_lines:
                print(ln)
            for ln in runner.get_log_buffer():
                print(ln)
            runner.clear_log_buffer()
            print(
                ANSI_RED
                + f"[steam-borg] 任务失败 appid={appid} depot_id={depot_id}: {e}\nstdout:\n{getattr(e, 'stdout', '')}\nstderr:\n{getattr(e, 'stderr', '')}"
                + ANSI_RESET
            )
            print(ANSI_GRAY + sep + ANSI_RESET)
            fail += 1
            continue
        except Exception as e:
            # 单行提示：异常，并换行后输出详细日志
            _print_status(
                f"[steam-borg] 异常 ({idx}/{len(pairs)}) appid={appid} depot_id={depot_id}",
                color=ANSI_YELLOW,
                finalize=True,
            )
            for ln in head_lines:
                print(ln)
            for ln in runner.get_log_buffer():
                print(ln)
            runner.clear_log_buffer()
            print(ANSI_YELLOW + f"[steam-borg] 任务异常 appid={appid} depot_id={depot_id}: {e}" + ANSI_RESET)
            print(ANSI_GRAY + sep + ANSI_RESET)
            fail += 1
            continue

    # 清除可能残留的单行状态
    try:
        sys.stdout.write("\r\x1b[2K")
        sys.stdout.flush()
    except Exception:
        pass

    print(f"[steam-borg] 完成: 成功 {ANSI_GREEN}{ok}{ANSI_RESET}，失败 {ANSI_RED}{fail}{ANSI_RESET}")
    return 0 if fail == 0 else 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))