#!/usr/bin/env python3
"""批次优先级归并自测：优先级、空值不遮盖、同级同值/异值、冲突、重算范围、
下载、原始观测回看，以及归并失败与交错并发下的事务一致性。

用法：python tests/merge_selftest.py
退出码 0 = 全部通过；1 = 有失败项。
"""
import json
import os
import sys
import tempfile
import threading

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

_fd, DB = tempfile.mkstemp(prefix="obs-merge-selftest-", suffix=".db")
os.close(_fd)
os.remove(DB)
os.environ["DB_PATH"] = DB

from fastapi.testclient import TestClient  # noqa: E402

from app import core  # noqa: E402
from app.db import session  # noqa: E402
from app.main import app  # noqa: E402

PASS, FAIL = [], []


def check(name, cond, extra=""):
    if cond:
        PASS.append(name)
        print(f"  ✓ {name}")
    else:
        FAIL.append(name)
        print(f"  ✗ {name}  {extra}")


def up(c, name, text, priority=None):
    data = {} if priority is None else {"priority": str(priority)}
    return c.post("/api/batches", files={"file": (name, text.encode("utf-8"), "text/csv")},
                  data=data)


def merged(c, **params):
    r = c.get("/api/merged", params=params)
    assert r.status_code == 200, r.text
    return {row["hour_utc"]: row for row in r.json()["rows"]}


def one(c, station, hour):
    return merged(c, station=station)[hour]


V1 = {"note": "初版", "columns": [
    {"role": "station", "name": "站号"},
    {"role": "hour", "name": "UTC小时"},
    {"name": "气温", "type": "integer"},
    {"name": "气压", "type": "integer"},
]}
HEADER = "站号,UTC小时,气温,气压\n"


def main():
    with TestClient(app) as c:
        print("== 1. 结构与批次 ==")
        r = c.post("/api/versions", json=V1)
        check("发布初版 v1", r.status_code == 201, r.text[:300])

        csv_a = HEADER + "S01,2026-11-18T00:00,5,1013\nS01,2026-11-18T01:00,6,1012\nS02,2026-11-18T00:00,3,1015\n"
        r = up(c, "a.csv", csv_a, priority=1)
        check("批次A priority=1", r.status_code == 201 and r.json()["priority"] == 1, r.text[:300])
        bid_a = r.json()["batch_id"]

        csv_b = HEADER + "S01,2026-11-18T00:00,7,\nS01,2026-11-18T01:00,,\n"
        r = up(c, "b.csv", csv_b, priority=5)
        check("批次B priority=5(含空值)", r.status_code == 201, r.text[:300])
        bid_b = r.json()["batch_id"]

        print("== 2. 高优先级覆盖 + 空值不遮盖 ==")
        row = one(c, "S01", "2026-11-18T00:00")
        check("高优先级非空值胜出(气温=7)", row["values"]["气温"] == 7, json.dumps(row, ensure_ascii=False))
        check("高优先级空值不遮盖低优先级气压(1013)", row["values"]["气压"] == 1013,
              json.dumps(row["values"], ensure_ascii=False))
        check("气温来源为批次B priority=5",
              row["sources"]["气温"]["priority"] == 5 and
              [b["batch_id"] for b in row["sources"]["气温"]["batches"]] == [bid_b],
              json.dumps(row["sources"]["气温"], ensure_ascii=False))
        check("气压来源为批次A priority=1",
              row["sources"]["气压"]["priority"] == 1 and
              [b["batch_id"] for b in row["sources"]["气压"]["batches"]] == [bid_a])
        check("该小时可交付", row["deliverable"] is True and row["status"] == "ok")
        row1 = one(c, "S01", "2026-11-18T01:00")
        check("全空高优先级批次不遮蔽两个低优先级读数",
              row1["values"]["气温"] == 6 and row1["values"]["气压"] == 1012,
              json.dumps(row1["values"], ensure_ascii=False))

        print("== 3. 同级同值只算一次 ==")
        csv_c = HEADER + "S01,2026-11-18T00:00,7,\n"
        r = up(c, "c.csv", csv_c, priority=5)
        bid_c = r.json()["batch_id"]
        row = one(c, "S01", "2026-11-18T00:00")
        check("同级同值不标冲突", row["deliverable"] is True and not row["conflicts"],
              json.dumps(row["conflicts"], ensure_ascii=False))
        check("同级同值只有一个读数(仍为7)", row["values"]["气温"] == 7)
        check("sources 记录两个来源批次(B,C)",
              sorted(b["batch_id"] for b in row["sources"]["气温"]["batches"]) ==
              sorted([bid_b, bid_c]), json.dumps(row["sources"]["气温"], ensure_ascii=False))
        check("每小时只有一行(无重复)", len(merged(c, station="S01")) == 2)

        print("== 4. 同级异值 → 冲突、该小时不可交付 ==")
        csv_d = HEADER + "S01,2026-11-18T00:00,8,\n"
        r = up(c, "d.csv", csv_d, priority=5)
        bid_d = r.json()["batch_id"]
        row = one(c, "S01", "2026-11-18T00:00")
        check("冲突小时标记冲突", row["status"] == "conflict" and row["deliverable"] is False,
              json.dumps(row, ensure_ascii=False))
        check("冲突列=气温", [x["column"] for x in row["conflicts"]] == ["气温"],
              json.dumps(row["conflicts"], ensure_ascii=False))
        check("冲突读数集合={7,8}", sorted(row["conflicts"][0]["values"]) == [7, 8],
              json.dumps(row["conflicts"][0], ensure_ascii=False))
        check("冲突候选列出来源批次",
              {x["batch_id"] for x in row["conflicts"][0]["candidates"]} ==
              {bid_b, bid_c, bid_d}, json.dumps(row["conflicts"][0], ensure_ascii=False))
        check("冲突原因含优先级和列名",
              "最高优先级 5" in row["conflicts"][0]["reason"] and
              "气温" in row["conflicts"][0]["reason"], row["conflicts"][0]["reason"])
        check("冲突小时值为空(不交付任一份)", row["values"]["气温"] is None)
        check("未冲突列仍取到有效值(气压=1013)", row["values"]["气压"] == 1013)

        r = c.get("/api/merged", params={"status": "conflict"})
        check("按 status=conflict 过滤", r.status_code == 200 and r.json()["row_count"] == 1,
              r.text[:300])
        r = c.get("/api/merged", params={"status": "bogus"})
        check("非法 status 返回 400", r.status_code == 400, r.status_code)

        print("== 5. 调整优先级后只重算受影响小时 ==")
        r = c.post(f"/api/batches/{bid_d}/priority", json={"priority": 6})
        check("批次D 提到 priority=6", r.status_code == 200 and r.json()["priority"] == 6,
              r.text[:300])
        check("重算小时数=1(只涉及 D 的 S01 00:00)", r.json()["recomputed_hours"] == 1,
              r.text[:300])
        row = one(c, "S01", "2026-11-18T00:00")
        check("冲突解除且取最高优先级读数(气温=8)", row["deliverable"] is True and
              row["values"]["气温"] == 8 and row["sources"]["气温"]["priority"] == 6,
              json.dumps(row, ensure_ascii=False))
        row_s02 = one(c, "S02", "2026-11-18T00:00")
        check("未受影响小时仍为原结果(气温=3)", row_s02["values"]["气温"] == 3)

        r = c.post(f"/api/batches/{bid_b}/priority", json={"priority": 10})
        check("批次B 提到 priority=10(再次超过D)", r.status_code == 200, r.text[:300])
        row = one(c, "S01", "2026-11-18T00:00")
        check("再调优先级后气温回到 7", row["values"]["气温"] == 7 and
              row["sources"]["气温"]["priority"] == 10, json.dumps(row["values"], ensure_ascii=False))
        check("B 的空值仍不遮盖 A 的气压", row["values"]["气压"] == 1013)

        r = c.post(f"/api/batches/{bid_b}/priority", json={"priority": 10})
        check("同优先级保存不重算", r.status_code == 200 and r.json()["recomputed_hours"] == 0,
              r.text[:300])
        r = c.post(f"/api/batches/{bid_b}/priority", json={"priority": "abc"})
        check("非法优先级返回 400", r.status_code == 400, r.text[:200])
        r = c.post("/api/batches/99999/priority", json={"priority": 1})
        check("不存在批次返回 404", r.status_code == 404, r.status_code)

        print("== 6. 原始观测仍可回看 ==")
        r = c.get("/api/observations", params={"station": "S01"})
        d = r.json()
        check("原始观测保留全部批次行(S01 共6行)",
              d["row_count"] == 6, f"row_count={d['row_count']}")
        check("原始观测含批次号与各自读数",
              {row["batch_id"] for row in d["rows"]} == {bid_a, bid_b, bid_c, bid_d})
        batches = c.get("/api/batches").json()["batches"]
        check("批次列表含优先级", all("priority" in b for b in batches))

        print("== 7. 下载 CSV ==")
        r = c.get("/api/merged.csv")
        check("CSV 下载 200/attachment", r.status_code == 200 and
              "attachment" in r.headers.get("content-disposition", ""), r.status_code)
        text = r.content.decode("utf-8-sig")
        lines = [ln for ln in text.splitlines() if ln]
        check("CSV 表头含来源批次列",
              "气温·来源批次" in lines[0] and "气压·来源优先级" in lines[0], lines[0])
        check("CSV 每小时一行(S01两小时+S02一小时=3)", len(lines) == 4, text[:400])
        check("CSV 数据行含来源批次号", f"#{bid_b}" in text, text[:400])

        print("== 8. 交错失败：归并写一半不能提交 ==")
        before_batches = len(c.get("/api/batches").json()["batches"])
        before = c.get("/api/merged", params={"station": "S02"}).json()["rows"]
        orig = core.merge_rows

        def boom(*a, **kw):
            raise RuntimeError("simulated merge failure")

        core.merge_rows = boom
        try:
            with TestClient(app, raise_server_exceptions=False) as c2:
                r = up(c2, "e.csv", HEADER + "S02,2026-11-18T05:00,9,1000\n", priority=99)
            check("归并异常时上传返回 500", r.status_code == 500, r.status_code)
        finally:
            core.merge_rows = orig
        after_batches = len(c.get("/api/batches").json()["batches"])
        check("失败上传不留下批次", before_batches == after_batches,
              f"{before_batches}→{after_batches}")
        after = c.get("/api/merged", params={"station": "S02"}).json()["rows"]
        check("失败上传不改变归并结果", before == after)
        r = c.get("/api/merged", params={"station": "S02", "hour_from": "2026-11-18T05:00"})
        check("失败批次的小时没有残留归并行", r.json()["row_count"] == 0, r.text[:300])

        print("== 9. 交错失败：换版重算失败整体回滚 ==")
        versions_before = c.get("/api/versions").json()["versions"]
        merged_before = c.get("/api/merged", params={"station": "S01"}).json()
        core.merge_rows = boom
        try:
            with TestClient(app, raise_server_exceptions=False) as c2:
                r = c2.post("/api/versions", json={"note": "坏换版", "columns": [
                    {"source": "station", "name": "站号"},
                    {"source": "hour_utc", "name": "UTC小时"},
                    {"source": "c1", "name": "气温_v2"},
                    {"source": "c2", "name": "气压"},
                ]})
            check("归并异常时换版返回 500", r.status_code == 500, r.status_code)
        finally:
            core.merge_rows = orig
        check("失败换版不留下新版本",
              len(c.get("/api/versions").json()["versions"]) == len(versions_before))
        cur = c.get("/api/versions/current").json()
        check("失败换版后现役版本不变", cur["version_no"] == versions_before[-1]["version_no"])
        check("失败换版后归并结果不变",
              c.get("/api/merged", params={"station": "S01"}).json() == merged_before)

        print("== 10. 交错失败：改优先级回滚 ==")
        before_row = one(c, "S01", "2026-11-18T00:00")
        core.merge_rows = boom
        try:
            with TestClient(app, raise_server_exceptions=False) as c2:
                r = c2.post(f"/api/batches/{bid_a}/priority", json={"priority": 50})
            check("归并异常时改优先级返回 500", r.status_code == 500, r.status_code)
        finally:
            core.merge_rows = orig
        batches = {b["id"]: b for b in c.get("/api/batches").json()["batches"]}
        check("失败调优先级不改变批次优先级", batches[bid_a]["priority"] == 1,
              batches[bid_a])
        after_row = one(c, "S01", "2026-11-18T00:00")
        check("失败调优先级不改变归并结果", before_row == after_row)

        print("== 11. 并发交错：结果来自同一已提交状态 ==")
        errors = []

        def uploader(i):
            try:
                r = up(c, f"t{i}.csv",
                       HEADER + f"S9,2026-11-18T{i:02d}:00,{i},1000\n",
                       priority=i % 3)
                if r.status_code != 201:
                    errors.append(("upload", r.status_code, r.text[:120]))
            except Exception as e:  # noqa: BLE001
                errors.append(("upload-exc", repr(e)))

        def reader():
            try:
                for _ in range(10):
                    r = c.get("/api/merged", params={"station": "S9"})
                    if r.status_code != 200:
                        errors.append(("read", r.status_code, r.text[:120]))
                        return
                    for row in r.json()["rows"]:
                        if row["status"] == "conflict" and not row["conflicts"]:
                            errors.append(("bad-conflict-row", row))
                        if row["status"] == "ok" and row["conflicts"]:
                            errors.append(("bad-ok-row", row))
            except Exception as e:  # noqa: BLE001
                errors.append(("read-exc", repr(e)))

        threads = [threading.Thread(target=uploader, args=(i,)) for i in range(8)]
        threads += [threading.Thread(target=reader) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        check("并发上传/查询无错误", not errors, repr(errors[:3]))
        stored = {(r["station"], r["hour_utc"]): r
                  for r in c.get("/api/merged", params={"station": "S9"}).json()["rows"]}
        check("并发后 S9 每小时一行(8行)", len(stored) == 8, len(stored))
        with session() as conn:
            core.recompute_all(conn)
        rebuilt = {(r["station"], r["hour_utc"]): r
                   for r in c.get("/api/merged", params={"station": "S9"}).json()["rows"]}
        check("增量重算结果 == 全量重算结果",
              {k: (v["status"], v["values"]) for k, v in stored.items()} ==
              {k: (v["status"], v["values"]) for k, v in rebuilt.items()},
              "增量与全量不一致")

        print("== 12. 空值不写掩盖：文本空串按空处理 ==")
        # 文本列场景由单元级纯函数校验
        res = core.merge_rows(
            [{"column_id": "c1", "name": "备注", "type": "text"}],
            [{"batch_id": 1, "priority": 1, "values": {"c1": "ok"}},
             {"batch_id": 2, "priority": 9, "values": {"c1": "   "}}])
        check("空白字符串不遮盖低优先级非空值", res["values"]["c1"] == "ok", res)
        res = core.merge_rows(
            [{"column_id": "c1", "name": "x", "type": "float"}],
            [{"batch_id": 1, "priority": 2, "values": {"c1": 5}},
             {"batch_id": 2, "priority": 2, "values": {"c1": 5.0}}])
        check("同级 5 与 5.0 视为同一读数", res["values"]["c1"] == 5 and not res["conflicts"], res)
        res = core.merge_rows(
            [{"column_id": "c1", "name": "x", "type": "integer"}],
            [{"batch_id": 1, "priority": 1, "values": {"c1": 0}}])
        check("0 是有效读数而非空值", res["values"]["c1"] == 0 and res["conflicts"] == {}, res)

        print("== 13. 换版后归并随新结构重算 ==")
        r = c.post("/api/versions", json={"note": "改名+加列", "columns": [
            {"source": "station", "name": "station_code"},
            {"source": "hour_utc", "name": "UTC小时"},
            {"source": "c1", "name": "温度"},
            {"source": "c2", "name": "气压", "type": "float"},
            {"source": None, "name": "湿度", "type": "integer", "nullable": True},
        ]})
        check("发布 v2", r.status_code == 201, r.text[:300])
        d = c.get("/api/merged", params={"station": "S01"}).json()
        check("归并展示结构立即切到 v2 列名",
              [x["name"] for x in d["columns"]] ==
              ["station_code", "UTC小时", "温度", "气压", "湿度"],
              json.dumps([x["name"] for x in d["columns"]], ensure_ascii=False))
        row = {r["hour_utc"]: r for r in d["rows"]}["2026-11-18T00:00"]
        check("改名后归并值按 column_id 找回(温度=7)",
              row["values"]["温度"] == 7 and row["sources"]["温度"]["priority"] == 10,
              json.dumps(row["values"], ensure_ascii=False))
        check("新增列在归并结果中为空且无来源", row["values"]["湿度"] is None and
              row["sources"]["湿度"] is None)
        check("扩型不改归并原值(气压=1013)", row["values"]["气压"] == 1013)

        print("== 14. 旧库升级：补 priority 列并回填归并 ==")
        legacy_db = os.path.join(tempfile.mkdtemp(prefix="obs-legacy-"), "old.db")
        import sqlite3
        lc = sqlite3.connect(legacy_db)
        lc.executescript("""
        CREATE TABLE schema_versions (id INTEGER PRIMARY KEY AUTOINCREMENT,
          version_no INTEGER NOT NULL UNIQUE,
          status TEXT NOT NULL CHECK (status IN ('active','superseded')),
          note TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL);
        CREATE TABLE schema_columns (version_id INTEGER NOT NULL, column_id TEXT NOT NULL,
          name TEXT NOT NULL, type TEXT NOT NULL, position INTEGER NOT NULL,
          nullable INTEGER NOT NULL DEFAULT 1, PRIMARY KEY (version_id, column_id),
          UNIQUE (version_id, name));
        CREATE TABLE batches (id INTEGER PRIMARY KEY AUTOINCREMENT,
          version_id INTEGER NOT NULL, filename TEXT NOT NULL DEFAULT '',
          row_count INTEGER NOT NULL, uploaded_at TEXT NOT NULL);
        CREATE TABLE observations (id INTEGER PRIMARY KEY AUTOINCREMENT,
          batch_id INTEGER NOT NULL, station TEXT NOT NULL, hour_utc TEXT NOT NULL,
          values_json TEXT NOT NULL);
        INSERT INTO schema_versions VALUES (1,1,'active','旧库','2026-01-01T00:00:00+00:00');
        INSERT INTO schema_columns VALUES
          (1,'station','站号','station',0,1),
          (1,'hour_utc','UTC小时','hour',1,1),
          (1,'c1','气温','integer',2,1);
        INSERT INTO batches VALUES (1,1,'old.csv',1,'2026-01-01T00:00:00+00:00');
        INSERT INTO observations VALUES (1,1,'S01','2026-11-18T00:00','{"c1": 9}');
        """)
        lc.commit()
        lc.close()
        os.environ["DB_PATH"] = legacy_db
        from app.db import init_db
        from app.main import app as legacy_app
        init_db()
        with TestClient(legacy_app) as lc2:
            r = lc2.get("/api/batches")
            check("旧库批次自动补 priority=0",
                  r.status_code == 200 and r.json()["batches"][0]["priority"] == 0,
                  r.text[:300])
            r = lc2.get("/api/merged", params={"station": "S01"})
            check("旧库启动时回填归并结果(1小时/气温=9)",
                  r.json()["row_count"] == 1 and
                  r.json()["rows"][0]["values"]["气温"] == 9, r.text[:300])
        os.environ["DB_PATH"] = DB

    print()
    print(f"通过 {len(PASS)} 项，失败 {len(FAIL)} 项")
    if FAIL:
        print("失败项：")
        for n in FAIL:
            print(" -", n)
        return 1
    return 0



if __name__ == "__main__":
    sys.exit(main())
