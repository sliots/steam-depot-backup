#!/usr/bin/env python3
"""
Steam Depot version backup tool using DepotDownloader and Borg.
Wrapper around borg_backup_lib.
"""

import argparse
import sys
from typing import List
from borg_backup_lib import BackupConfig, SteamBorgBackup

def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Steam depot backup via DepotDownloader + Borg")
    parser.add_argument("--appid", required=True, help="Steam App ID, e.g. 4075460")
    parser.add_argument("--depot", required=True, help="Steam Depot ID, e.g. 4075461")
    parser.add_argument("--config", help="Path to TOML configuration file", default=None)
    parser.add_argument("--work-dir", default=None, help="Working base directory")
    parser.add_argument("--api-url", default=None, help="Manifest API base URL")
    parser.add_argument("--downloader", default=None, help="DepotDownloader command or path")
    parser.add_argument("--borg", default=None, help="borg command or path")
    parser.add_argument("--username", default=None, help="Steam username")
    parser.add_argument("--password", default=None, help="Steam password")
    parser.add_argument("--dry-run", action="store_true", help="Skip executing external commands")
    parser.add_argument("--insecure", action="store_true", help="Skip SSL verification")
    parser.add_argument("--timeout", type=int, default=None, help="API request timeout")
    parser.add_argument("--api-retries", type=int, default=None, help="接口请求超时重试次数")
    parser.add_argument("--retry-backoff", type=float, default=None, help="重试退避基础间隔秒")
    return parser.parse_args(argv)

def main(argv: List[str]) -> int:
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
    if args.dry_run: cli_overrides["dry_run"] = True
    if args.insecure: cli_overrides["verify_ssl"] = False

    cfg = BackupConfig.from_strategies(config_path=args.config, **cli_overrides)
    
    runner = SteamBorgBackup(cfg)
    
    try:
        did_change = runner.orchestrate_backup(str(args.appid), str(args.depot))
        
        # Output logs
        for line in runner.get_log_buffer():
            print(line)
            
        return 0
    except Exception as e:
        # Output logs collected so far
        for line in runner.get_log_buffer():
            print(line)
        print(f"[steam-borg] Error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
