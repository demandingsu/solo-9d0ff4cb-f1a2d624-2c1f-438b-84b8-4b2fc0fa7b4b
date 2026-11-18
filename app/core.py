"""结构版本、批次上传、统一查询的核心逻辑。

设计要点：
- 每个逻辑列有稳定标识 column_id（station / hour_utc / c1 / c2 …）。
  改名、扩型都不改变 column_id，因此跨多版连续改名仍能按它找回旧值。
- 观测值按上传时的解析结果原样存入 values_json（{column_id: value}），
  之后任何版本操作都不改写它：旧批次保留原值与原结构；
  新增列在旧行自然为空；整数扩小数不回填、不改数。
- 发布与上传都先完整校验、再在单个事务里写入；失败整体回滚，
  现役版本与既有数据不受影响（失败仍按原版本供数）。
"""
from __future__ import annotations

import csv
import io
import json
import math
import re
from datetime import datetime, timezone

DATA_TYPES = ("integer", "float", "text")
SYSTEM_TYPES = ("station", "hour")
MAX_ERRORS = 20


class DomainError(Exception):
    """业务规则冲突（映射为 HTTP 400）。"""

    def __init__(self, message: str, errors: list[str] | None = None):
        super().__init__(message)
        self.message = message
        self.errors = errors or []


class NotFoundError(DomainError):
    """资源不存在（映射为 HTTP 404）。"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------- 读取

def _col_dict(r) -> dict:
    return {
        "column_id": r["column_id"],
        "name": r["name"],
        "type": r["type"],
        "position": r["position"],
        "nullable": bool(r["nullable"]),
    }


def get_active_version(conn) -> dict | None:
    r = conn.execute(
        "SELECT * FROM schema_versions WHERE status='active' "
        "ORDER BY version_no DESC LIMIT 1"
    ).fetchone()
    return dict(r) if r else None


def get_version(conn, version_no: int) -> dict | None:
    r = conn.execute(
        "SELECT * FROM schema_versions WHERE version_no=?", (version_no,)
    ).fetchone()
    return dict(r) if r else None


def version_columns(conn, version_id: int) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM schema_columns WHERE version_id=? ORDER BY position",
        (version_id,),
    ).fetchall()
    return [_col_dict(r) for r in rows]


def version_with_columns(conn, v: dict) -> dict:
    return {**v, "columns": version_columns(conn, v["id"])}


def list_versions(conn) -> list[dict]:
    rows = conn.execute("SELECT * FROM schema_versions ORDER BY version_no").fetchall()
    return [version_with_columns(conn, dict(r)) for r in rows]


def list_batches(conn) -> list[dict]:
    rows = conn.execute(
        """
        SELECT b.id, b.filename, b.row_count, b.uploaded_at, v.version_no
        FROM batches b JOIN schema_versions v ON v.id = b.version_id
        ORDER BY b.id
        """
    ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------- 发布

def _fresh_column_ids(conn, n: int) -> list[str]:
    maxn = 0
    for (cid,) in conn.execute("SELECT column_id FROM schema_columns"):
        if cid.startswith("c") and cid[1:].isdigit():
            maxn = max(maxn, int(cid[1:]))
    return [f"c{maxn + i + 1}" for i in range(n)]


def _check_name(raw, idx: int, errors: list[str]) -> str:
    name = raw.strip() if isinstance(raw, str) else ""
    if not name:
        errors.append(f"第 {idx} 列：列名不能为空")
    elif len(name) > 100:
        errors.append(f"第 {idx} 列：列名过长（超过 100 字符）")
    return name


def _check_dup_names(names: list[str], errors: list[str]) -> None:
    seen, dup = set(), set()
    for n in names:
        if n and n in seen:
            dup.add(n)
        seen.add(n)
    if dup:
        errors.append("列名重复：" + "、".join(sorted(dup)))


def _build_initial(conn, cols_spec) -> list[dict]:
    errors: list[str] = []
    items = []  # [role, name, type]
    n_station = n_hour = 0
    for i, c in enumerate(cols_spec, 1):
        if not isinstance(c, dict):
            errors.append(f"第 {i} 列：必须是对象")
            continue
        role = c.get("role") or "data"
        if role not in ("station", "hour", "data"):
            errors.append(f"第 {i} 列：role 只能是 station / hour / data")
            continue
        name = _check_name(c.get("name"), i, errors)
        if role == "station":
            n_station += 1
            items.append([role, name, "station"])
        elif role == "hour":
            n_hour += 1
            items.append([role, name, "hour"])
        else:
            ctype = c.get("type")
            if ctype not in DATA_TYPES:
                errors.append(
                    f"第 {i} 列（{name or '?'}）：数据列类型必须是 integer / float / text"
                )
                continue
            items.append([role, name, ctype])
    if n_station != 1:
        errors.append("初版结构必须且只能包含 1 个站号列（role=station）")
    if n_hour != 1:
        errors.append("初版结构必须且只能包含 1 个 UTC 小时列（role=hour）")
    _check_dup_names([n for _, n, _ in items], errors)
    if errors:
        raise DomainError("初版结构校验失败", errors)
    fresh = iter(_fresh_column_ids(conn, sum(1 for r, _, _ in items if r == "data")))
    out = []
    for pos, (role, name, ctype) in enumerate(items):
        cid = "station" if role == "station" else "hour_utc" if role == "hour" else next(fresh)
        out.append({"column_id": cid, "name": name, "type": ctype,
                    "position": pos, "nullable": True})
    return out


def _build_evolution(conn, old_cols, cols_spec) -> list[dict]:
    errors: list[str] = []
    old_by_id = {c["column_id"]: c for c in old_cols}
    used: dict[str, str] = {}
    entries = []  # {column_id|None, name, type}
    for i, c in enumerate(cols_spec, 1):
        if not isinstance(c, dict):
            errors.append(f"第 {i} 列：必须是对象")
            continue
        name = _check_name(c.get("name"), i, errors)
        src = c.get("source")
        if isinstance(src, (list, tuple, dict)):
            errors.append(
                f"第 {i} 列（{name or '?'}）：不支持两列合并（source 只能是单个来源列标识）"
            )
            continue
        if src is None:
            ctype = c.get("type")
            if ctype not in DATA_TYPES:
                errors.append(f"第 {i} 列（{name or '?'}）：新增列类型必须是 integer / float / text")
                continue
            if c.get("nullable") is False:
                errors.append(
                    f"第 {i} 列（{name or '?'}）：新增列必须可空（nullable=true），否则历史批次无法补值"
                )
                continue
            entries.append({"column_id": None, "name": name, "type": ctype})
            continue
        if src in used:
            errors.append(
                f"第 {i} 列（{name or '?'}）：来源列 {src} 已映射到「{used[src]}」，不允许一列拆成多列"
            )
            continue
        old = old_by_id.get(src)
        if old is None:
            errors.append(f"第 {i} 列（{name or '?'}）：来源列 {src} 在现役结构中不存在")
            continue
        used[src] = name
        new_type = old["type"]
        req = c.get("type")
        if req is not None and req != old["type"]:
            if old["type"] in SYSTEM_TYPES:
                errors.append(f"第 {i} 列（{name or '?'}）：站号 / 小时列的类型不可更改")
            elif old["type"] == "integer" and req == "float":
                new_type = "float"  # 唯一允许的扩型：整数 → 小数
            else:
                errors.append(
                    f"第 {i} 列（{name or '?'}）：只允许 整数→小数 扩型，不允许 {old['type']}→{req}"
                )
        entries.append({"column_id": src, "name": name, "type": new_type})
    missing = [c for c in old_cols if c["column_id"] not in used]
    if missing:
        errors.append(
            "不允许删除旧列，以下现役列缺少映射："
            + "、".join(f"{c['name']}({c['column_id']})" for c in missing)
        )
    _check_dup_names([e["name"] for e in entries], errors)
    if errors:
        raise DomainError("新版本结构校验失败", errors)
    fresh = iter(_fresh_column_ids(conn, sum(1 for e in entries if e["column_id"] is None)))
    out = []
    for pos, e in enumerate(entries):
        cid = e["column_id"] or next(fresh)
        out.append({"column_id": cid, "name": e["name"], "type": e["type"],
                    "position": pos, "nullable": True})
    return out


def publish_version(conn, spec) -> dict:
    """发布新版本。先完整校验，全部通过后才在事务内写入；失败回滚，现役版本不变。"""
    if not isinstance(spec, dict):
        raise DomainError("请求体必须是 JSON 对象")
    cols_spec = spec.get("columns")
    note = spec.get("note") or ""
    if not isinstance(note, str):
        raise DomainError("note 必须是字符串")
    if not isinstance(cols_spec, list) or not cols_spec:
        raise DomainError("columns 必须是非空数组")
    active = get_active_version(conn)
    if active is None:
        new_cols = _build_initial(conn, cols_spec)
    else:
        new_cols = _build_evolution(conn, version_columns(conn, active["id"]), cols_spec)
    maxno = conn.execute("SELECT COALESCE(MAX(version_no),0) FROM schema_versions").fetchone()[0]
    version_no = maxno + 1
    if active is not None:
        conn.execute("UPDATE schema_versions SET status='superseded' WHERE id=?", (active["id"],))
    cur = conn.execute(
        "INSERT INTO schema_versions (version_no, status, note, created_at) VALUES (?,?,?,?)",
        (version_no, "active", note.strip(), _now()),
    )
    vid = cur.lastrowid
    for c in new_cols:
        conn.execute(
            "INSERT INTO schema_columns (version_id, column_id, name, type, position, nullable)"
            " VALUES (?,?,?,?,?,?)",
            (vid, c["column_id"], c["name"], c["type"], c["position"], 1 if c["nullable"] else 0),
        )
    row = conn.execute("SELECT * FROM schema_versions WHERE id=?", (vid,)).fetchone()
    return version_with_columns(conn, dict(row))


# ---------------------------------------------------------------- 上传

_INT_RE = re.compile(r"^[+-]?\d+$")
_HOUR_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})[T ](\d{2})(?::(\d{2}))?(?::(\d{2}))?Z?$")


def normalize_hour(raw) -> str:
    """把常见 UTC 小时写法规范为 YYYY-MM-DDTHH:MM，便于按区间比较。"""
    s = (raw or "").strip()
    m = _HOUR_RE.match(s)
    if not m:
        raise ValueError(f"无法识别的小时格式 {s!r}（应为如 2026-11-18T03:00）")
    y, mo, d, h, mi, sec = m.groups()
    if sec and sec != "00":
        raise ValueError(f"小时须对齐到分钟，秒必须为 00：{s!r}")
    mi = mi or "00"
    try:
        datetime(int(y), int(mo), int(d), int(h), int(mi))
    except ValueError as e:
        raise ValueError(f"非法时间 {s!r}：{e}")
    return f"{y}-{mo}-{d}T{h}:{mi}"


def _parse_value(raw, ctype):
    """返回 (值, 错误)。空单元格 → (None, None)。数值按原始读数形态保存。"""
    s = (raw or "").strip()
    if s == "":
        return None, None
    if ctype == "integer":
        if not _INT_RE.match(s):
            return None, f"期望整数，得到 {s!r}"
        return int(s), None
    if ctype == "float":
        if _INT_RE.match(s):
            return int(s), None  # 保留原始读数形态（5 不写成 5.0）
        try:
            v = float(s)
        except ValueError:
            return None, f"期望小数，得到 {s!r}"
        if not math.isfinite(v):
            return None, f"期望有限小数，得到 {s!r}"
        return v, None
    return s, None  # text 原样保留


def upload_csv(conn, filename: str, text: str) -> dict:
    """按现役结构校验并入库。任何一行不合格则整批拒绝，不产生半成品批次。"""
    active = get_active_version(conn)
    if active is None:
        raise DomainError("尚未发布任何结构版本，请先发布初版结构")
    cols = version_columns(conn, active["id"])
    names = [c["name"] for c in cols]
    try:
        reader = csv.DictReader(io.StringIO(text))
        header = reader.fieldnames
    except csv.Error as e:
        raise DomainError(f"CSV 解析失败：{e}")
    if not header:
        raise DomainError("CSV 缺少表头")
    if len(set(header)) != len(header):
        raise DomainError("CSV 表头存在重复列名")
    missing = [n for n in names if n not in header]
    extra = [h for h in header if h not in names]
    if missing or extra:
        errs = []
        if missing:
            errs.append("缺少列：" + "、".join(missing))
        if extra:
            errs.append("多余列：" + "、".join(extra))
        errs.append(f"现役结构（v{active['version_no']}）要求表头：" + "、".join(names))
        raise DomainError("CSV 表头与现役结构不一致", errs)
    rows, errors = [], []
    for ln, raw in enumerate(reader, start=2):
        row_err: list[str] = []
        if None in raw:
            row_err.append(f"第 {ln} 行：列数多于表头")
        rec: dict = {}
        station = hour = None
        for c in cols:
            cell = raw.get(c["name"])
            if c["type"] == "station":
                s = (cell or "").strip()
                if not s:
                    row_err.append(f"第 {ln} 行：站号为空")
                elif len(s) > 100:
                    row_err.append(f"第 {ln} 行：站号过长（超过 100 字符）")
                else:
                    station = s
            elif c["type"] == "hour":
                try:
                    hour = normalize_hour(cell)
                except ValueError as e:
                    row_err.append(f"第 {ln} 行：{e}")
            else:
                v, err = _parse_value(cell, c["type"])
                if err:
                    row_err.append(f"第 {ln} 行 列「{c['name']}」：{err}")
                else:
                    rec[c["column_id"]] = v
        if row_err:
            errors.extend(row_err)
        else:
            rows.append((station, hour, json.dumps(rec, ensure_ascii=False)))
        if len(errors) >= MAX_ERRORS:
            errors.append("……错误过多，仅显示前部分")
            break
    if errors:
        raise DomainError(f"CSV 校验未通过（{len(errors)} 处问题），未写入任何数据", errors)
    if not rows:
        raise DomainError("CSV 没有数据行")
    cur = conn.execute(
        "INSERT INTO batches (version_id, filename, row_count, uploaded_at) VALUES (?,?,?,?)",
        (active["id"], filename, len(rows), _now()),
    )
    bid = cur.lastrowid
    conn.executemany(
        "INSERT INTO observations (batch_id, station, hour_utc, values_json) VALUES (?,?,?,?)",
        [(bid, s, h, j) for s, h, j in rows],
    )
    return {"batch_id": bid, "version_no": active["version_no"], "row_count": len(rows)}


# ---------------------------------------------------------------- 查询

def query_observations(conn, station=None, hour_from=None, hour_to=None,
                       version_no=None, limit=1000) -> dict:
    """按站号/时间查询。

    - 不指定 version_no：用现役版本的列名统一展示全部批次；
      改名列按 column_id 找回旧值，新增列在旧行显示空值，原值不做任何换算。
    - 指定 version_no：切到该历史版本视角，只看当时上传的批次，
      列名与读数都是当时的样子。
    """
    if version_no is not None:
        ver = get_version(conn, version_no)
        if ver is None:
            raise NotFoundError(f"版本 v{version_no} 不存在")
    else:
        ver = get_active_version(conn)
        if ver is None:
            raise NotFoundError("尚未发布任何结构版本")
    cols = version_columns(conn, ver["id"])
    where, params = [], []
    if version_no is not None:
        where.append("b.version_id = ?")
        params.append(ver["id"])
    if station:
        where.append("o.station = ?")
        params.append(station.strip())
    if hour_from:
        where.append("o.hour_utc >= ?")
        params.append(hour_from)
    if hour_to:
        where.append("o.hour_utc <= ?")
        params.append(hour_to)
    sql = (
        "SELECT o.station, o.hour_utc, o.values_json, o.batch_id, b.version_id "
        "FROM observations o JOIN batches b ON b.id = o.batch_id"
    )
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY o.station, o.hour_utc, o.batch_id, o.id LIMIT ?"
    params.append(limit + 1)
    fetched = conn.execute(sql, params).fetchall()
    truncated = len(fetched) > limit
    fetched = fetched[:limit]
    ver_map = {r["id"]: r["version_no"]
               for r in conn.execute("SELECT id, version_no FROM schema_versions")}
    rows = []
    for r in fetched:
        stored = json.loads(r["values_json"])
        values = {}
        for c in cols:
            if c["type"] == "station":
                values[c["name"]] = r["station"]
            elif c["type"] == "hour":
                values[c["name"]] = r["hour_utc"]
            else:
                values[c["name"]] = stored.get(c["column_id"])  # 旧批次没有该列 → 空值
        rows.append({
            "batch_id": r["batch_id"],
            "batch_version_no": ver_map.get(r["version_id"]),
            "values": values,
        })
    return {
        "version": version_with_columns(conn, ver),
        "row_count": len(rows),
        "truncated": truncated,
        "rows": rows,
    }
