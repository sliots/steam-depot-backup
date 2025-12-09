# SteamDepotBackup

> 注：这是一个 vibe coding 项目

## 项目概述

本项目提供了一套自动化的 Steam 应用历史版本（Depot）增量备份解决方案。通过结合 `DepotDownloader` 的下载能力与 `BorgBackup` 的去重存储特性，本工具能够高效地下载指定 AppID 和 DepotID 的历史 Manifest 版本，并将其存储在去重仓库中。

核心功能包括：

- **自动化清单获取**：从指定 API 接口获取应用的历史 Manifest 列表。
- **增量去重存储**：利用 BorgBackup 技术，仅存储版本间变更的数据块，极大节省存储空间。
- **智能流程编排**：自动处理仓库初始化、历史版本提取、差异下载及归档创建。
- **批量任务处理**：支持通过接口或文件列表批量执行多个应用的备份任务。

## 安装指南

### 1. 环境要求

运行本项目需要以下基础环境：

- **操作系统**：Windows (WSL2 推荐) / Linux / macOS
- **Python**：3.7 或更高版本
- **.NET Runtime**：用于运行 DepotDownloader

### 2. 依赖工具

请确保以下工具已安装并配置在系统环境变量中，或在运行时指定路径：

- **BorgBackup**：用于创建去重备份仓库。
  - [官方文档](https://borgbackup.readthedocs.io/)
- **DepotDownloader**：用于从 Steam 服务器下载指定版本的 Depot 文件。
  - [GitHub 仓库](https://github.com/SteamRE/DepotDownloader)

### 3. 项目部署

克隆代码仓库到本地目录：

```bash
git clone <repository_url>
cd steam_borg_backup
```

本项目主要依赖 Python 标准库，无需安装额外的 `pip` 包。

## 使用说明

本项目包含两个主要执行脚本，分别用于单任务备份和批量任务备份。

### 单任务备份

使用 `steam_borg_backup.py` 对指定的单个 AppID 和 DepotID 进行备份。

**基本命令格式：**

```bash
python steam_borg_backup.py --appid <APPID> --depot <DEPOT_ID> [参数]
```

**示例：**

```bash
python steam_borg_backup.py \
    --appid 4075460 \
    --depot 4075461 \
    --work-dir /mnt/z/depots \
    --username your_steam_user
```

### 批量任务备份

使用 `run_batch_backups.py` 从接口获取任务列表并顺序执行备份。

**基本命令格式：**

```bash
python run_batch_backups.py --list-api <LIST_API_URL> [参数]
```

**示例：**

```bash
python run_batch_backups.py \
    --list-api "http://localhost/api/backup_list" \
    --work-dir /mnt/z/depots \
    --username your_steam_user
```

## 配置说明

本项目支持通过命令行参数或环境变量进行配置。命令行参数优先级高于环境变量。

### 通用参数

| 参数名 | 环境变量 | 说明 | 默认值 |
| :--- | :--- | :--- | :--- |
| `--work-dir` | `WORK_DIR` | 备份数据存储的工作目录 | `/mnt/z/depots` |
| `--api-url` | `MANIFEST_API_URL` | Manifest 列表查询接口 URL | (内置默认地址) |
| `--downloader` | `DEPOT_DOWNLOADER_CMD` | DepotDownloader 可执行文件路径 | `DepotDownloader` |
| `--borg` | `BORG_CMD` | BorgBackup 可执行文件路径 | `borg` |
| `--username` | `STEAM_USERNAME` | Steam 账户用户名 | `sliots` |
| `--password` | `STEAM_PASSWORD` | Steam 账户密码 (可选) | 无 |
| `--timeout` | `API_TIMEOUT` | API 请求超时时间 (秒) | 15 |
| `--dry-run` | `DRY_RUN` | 仅输出流程日志，不执行实际命令 | False |
| `--insecure` | `VERIFY_SSL` | 跳过 SSL 证书验证 | False |

### 批量任务特有参数

以下参数仅适用于 `run_batch_backups.py`：

| 参数名 | 说明 | 默认值 |
| :--- | :--- | :--- |
| `--list-api` | **(必填)** 获取备份任务列表的 API 地址或文件路径 | 无 |
| `--api-retries` | 接口请求失败重试次数 | 3 |
| `--retry-backoff` | 重试等待的基础时间间隔 (秒) | 2.0 |

### 接口数据格式要求

**Manifest 列表接口 (`--api-url`) 返回格式：**

```json
{
  "data": [
    {
      "manifest_id": "652381323314602403",
      "depot_id": "4075461",
      "seen_date": "2025-11-03T06:16:03.000Z",
      "appid": "4075460"
    }
  ]
}
```

**批量任务列表接口 (`--list-api`) 返回格式：**

```json
{
  "data": [
    {
      "appid": "4075460",
      "depot_id": "4075461"
    }
  ]
}
```
