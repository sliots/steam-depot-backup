#!/usr/bin/env python3
"""
Batch runner: fetches a list of {appid, depot_id} pairs from an API
and runs Steam+Borg backups sequentially. Continues on errors.

Supports built-in loop mode via --interval / loop_interval_sec config.
"""

import argparse
import os
import signal
import subprocess
import sys
import threading
from typing import List, Optional

from borg_backup_lib import BackupConfig, SteamBorgBackup

# Allow borg to open repos that were relocated (e.g. work_dir path changed)
os.environ.setdefault("BORG_RELOCATED_REPO_ACCESS_POLICY", "allow")
# Fail immediately if a borg lock is held (instead of hanging indefinitely)
os.environ.setdefault("BORG_LOCK_WAIT", "0")

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
        pass


def _enable_ansi_colors() -> None:
    try:
        if os.name == "nt":
            import ctypes
            kernel32 = ctypes.windll.kernel32
            for handle_id in (-11, -12):  # STD_OUTPUT_HANDLE, STD_ERROR_HANDLE
                handle = kernel32.GetStdHandle(handle_id)
                mode = ctypes.c_uint()
                if kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
                    kernel32.SetConsoleMode(handle, mode.value | 0x0004)  # ENABLE_VIRTUAL_TERMINAL_PROCESSING
    except Exception:
        pass


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch Steam depot backups via DepotDownloader + Borg")
    parser.add_argument("--config", help="Path to TOML configuration file", default=None)
    parser.add_argument("--list-api", help="返回需要备份的 appid/depot_id 列表的接口 URL (http/https/file)")
    parser.add_argument("--work-dir", default=None, help="工作目录 (默认: 配置文件/环境变量 or /mnt/z/depots)")
    parser.add_argument("--api-url", default=None, help="manifest 列表接口基础 URL")
    parser.add_argument("--downloader", default=None, help="DepotDownloader 命令或路径")
    parser.add_argument("--borg", default=None, help="borg 命令或路径")
    parser.add_argument("--username", default=None, help="Steam 用户名")
    parser.add_argument("--password", default=None, help="Steam 密码 (可选)")
    parser.add_argument("--dry-run", action="store_true", help="跳过外部命令，仅验证流程")
    parser.add_argument("--insecure", action="store_true", help="拉取接口时跳过 SSL 验证")
    parser.add_argument("--timeout", type=int, default=None, help="接口请求超时秒数")
    parser.add_argument("--api-retries", type=int, default=None, help="接口请求超时重试次数 (默认: 3)")
    parser.add_argument("--retry-backoff", type=float, default=None, help="重试退避基础间隔秒 (默认: 2.0)")
    parser.add_argument(
        "--interval", type=int, default=None, metavar="SECONDS",
        help="轮询间隔秒数；0 = 单次运行（默认读 config loop_interval_sec，未设置则 0）"
    )
    return parser.parse_args(argv)


def _run_one_batch(runner: SteamBorgBackup, pairs: List[dict]) -> int:
    """执行一轮备份，返回失败任务数。"""
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
        _print_status(
            f"[steam-borg] 正在处理 ({idx}/{len(pairs)}) appid={appid} depot_id={depot_id}",
            color=ANSI_CYAN,
            finalize=True,
        )
        try:
            did_change = runner.orchestrate_backup(appid=appid, depot_id=depot_id)
            if did_change:
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
            runner.clear_log_buffer()
            if not did_change:
                _print_status(
                    f"[steam-borg] 跳过（无新备份） ({idx}/{len(pairs)}) appid={appid} depot_id={depot_id}",
                    color=ANSI_GRAY,
                    finalize=False,
                )
            ok += 1
        except subprocess.CalledProcessError as e:
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
        except Exception as e:
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

    # 清除可能残留的单行状态
    try:
        sys.stdout.write("\r\x1b[2K")
        sys.stdout.flush()
    except Exception:
        pass

    print(f"[steam-borg] 完成: 成功 {ANSI_GREEN}{ok}{ANSI_RESET}，失败 {ANSI_RED}{fail}{ANSI_RESET}")
    return fail


def main(argv: List[str]) -> int:
    _enable_ansi_colors()
    args = parse_args(argv)

    cli_overrides = {}
    if args.work_dir: cli_overrides["work_dir"] = args.work_dir
    if args.api_url: cli_overrides["manifest_api_url"] = args.api_url
    if args.downloader: cli_overrides["depot_downloader_cmd"] = args.downloader
    if args.borg: cli_overrides["borg_cmd"] = args.borg
    if args.username: cli_overrides["steam_username"] = args.username
    if args.password: cli_overrides["steam_password"] = args.password
    if args.timeout: cli_overrides["api_timeout"] = args.timeout
    if args.api_retries: cli_overrides["api_retries"] = args.api_retries
    if args.retry_backoff: cli_overrides["retry_backoff_sec"] = args.retry_backoff
    if args.list_api: cli_overrides["list_api_url"] = args.list_api
    if args.dry_run: cli_overrides["dry_run"] = True
    if args.insecure: cli_overrides["verify_ssl"] = False
    if args.interval is not None: cli_overrides["loop_interval_sec"] = args.interval

    cfg = BackupConfig.from_strategies(config_path=args.config, **cli_overrides)

    if not cfg.list_api_url:
        print(ANSI_RED + "[steam-borg] Error: Missing list API URL. Provide via --list-api or config file." + ANSI_RESET)
        return 1

    runner = SteamBorgBackup(cfg)
    interval = cfg.loop_interval_sec

    # Signal handling for graceful shutdown
    stop_event = threading.Event()

    def _handle_signal(sig, frame):
        print(ANSI_YELLOW + "\n[steam-borg] 收到信号，将在本轮完成后退出..." + ANSI_RESET)
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    round_num = 0
    while not stop_event.is_set():
        round_num += 1
        # Clear screen (same as `clear &&` in the original shell loop)
        print("\x1b[2J\x1b[H", end="", flush=True)
        if interval > 0:
            print(ANSI_CYAN + f"[steam-borg] === 第 {round_num} 轮开始 ===" + ANSI_RESET)

        try:
            pairs = runner.fetch_pairs(str(cfg.list_api_url))
        except Exception as e:
            print(ANSI_RED + f"[steam-borg] 拉取列表接口失败: {e}" + ANSI_RESET)
            if interval <= 0:
                return 1
            print(ANSI_YELLOW + f"[steam-borg] 等待 {interval}s 后重试... (Ctrl+C 退出)" + ANSI_RESET)
            stop_event.wait(timeout=interval)
            continue

        print(ANSI_CYAN + f"[steam-borg] 将顺序执行 {len(pairs)} 组备份任务" + ANSI_RESET)
        _run_one_batch(runner, pairs)

        if interval <= 0:
            break

        print(ANSI_GRAY + f"[steam-borg] 等待 {interval}s 后开始下一轮... (Ctrl+C 退出)" + ANSI_RESET)
        stop_event.wait(timeout=interval)

    if interval > 0:
        print(ANSI_CYAN + "[steam-borg] 已退出循环。" + ANSI_RESET)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
