"""SQLite 连接与事务管理。

所有读写都通过 session() 进行：全局可重入锁 + 单连接事务。
发布 / 上传 / 查询任一操作要么整体完成、要么整体回滚，
保证"结构"与"数据"永远成对出现，交错调用不会混用。
"""
from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager

_lock = threading.RLock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_versions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    version_no  INTEGER NOT NULL UNIQUE,
    status      TEXT NOT NULL CHECK (status IN ('active', 'superseded')),
    note        TEXT NOT NULL DEFAULT '',
    created_at  TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS schema_columns (
    version_id  INTEGER NOT NULL REFERENCES schema_versions(id) ON DELETE CASCADE,
    column_id   TEXT NOT NULL,
    name        TEXT NOT NULL,
    type        TEXT NOT NULL,
    position    INTEGER NOT NULL,
    nullable    INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (version_id, column_id),
    UNIQUE (version_id, name)
);
CREATE TABLE IF NOT EXISTS batches (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id  INTEGER NOT NULL REFERENCES schema_versions(id),
    filename    TEXT NOT NULL DEFAULT '',
    row_count   INTEGER NOT NULL,
    priority    INTEGER NOT NULL DEFAULT 0,
    uploaded_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS observations (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id    INTEGER NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    station     TEXT NOT NULL,
    hour_utc    TEXT NOT NULL,
    values_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_obs_station_hour ON observations (station, hour_utc);
CREATE INDEX IF NOT EXISTS idx_obs_batch ON observations (batch_id);
CREATE TABLE IF NOT EXISTS merged_hours (
    station        TEXT NOT NULL,
    hour_utc       TEXT NOT NULL,
    status         TEXT NOT NULL CHECK (status IN ('ok', 'conflict')),
    values_json    TEXT NOT NULL,
    sources_json   TEXT NOT NULL,
    conflicts_json TEXT NOT NULL,
    computed_at    TEXT NOT NULL,
    PRIMARY KEY (station, hour_utc)
);
CREATE INDEX IF NOT EXISTS idx_merged_status ON merged_hours (status);
"""


def db_path() -> str:
    return os.environ.get("DB_PATH") or os.path.join(os.getcwd(), "data", "obs.db")


def connect() -> sqlite3.Connection:
    path = db_path()
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        conn.execute("PRAGMA journal_mode = WAL")
    except sqlite3.Error:
        pass
    return conn


def init_db() -> None:
    with _lock:
        with connect() as conn:
            conn.executescript(SCHEMA)
            _migrate(conn)
            _backfill_merged(conn)


def _migrate(conn) -> None:
    """旧库升级：为既有 batches 补上 priority 列（默认 0，与旧行为不冲突）。"""
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(batches)")}
    if "priority" not in cols:
        conn.execute(
            "ALTER TABLE batches ADD COLUMN priority INTEGER NOT NULL DEFAULT 0"
        )


def _backfill_merged(conn) -> None:
    """旧库升级：已有原始观测但还没有归并结果时补算一次。

    仍在 init_db 的同一个事务里完成；中途失败则整体回滚，不会只补一半。
    """
    from . import core  # 延迟导入，避免模块级循环依赖

    if conn.execute("SELECT COUNT(*) FROM merged_hours").fetchone()[0]:
        return
    if not conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]:
        return
    if core.get_active_version(conn) is None:
        return
    core.recompute_all(conn)


@contextmanager
def session():
    """全局锁 + 单连接事务：正常退出提交，异常回滚并关闭连接。"""
    with _lock:
        conn = connect()
        try:
            with conn:
                yield conn
        finally:
            conn.close()
