#!/usr/bin/env python3
"""端到端自测：连续改名、加列、扩型、历史视角、失败恢复。

用法：python tests/selftest.py
退出码 0 = 全部通过；1 = 有失败项。
"""
import json
import os
import sys
import tempfile

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

_fd, DB = tempfile.mkstemp(prefix="obs-selftest-", suffix=".db")
os.close(_fd)
os.remove(DB)  # 让应用自建全新库
os.environ["DB_PATH"] = DB

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402

PASS, FAIL = [], []


def check(name, cond, extra=""):
    if cond:
        PASS.append(name)
        print(f"  ✓ {name}")
    else:
        FAIL.append(name)
        print(f"  ✗ {name}  {extra}")


def up(c, name, text):
    return c.post("/api/batches", files={"file": (name, text.encode("utf-8"), "text/csv")})


def main():
    with TestClient(app) as c:
        print("== 1. 初始状态与初版发布 ==")
        r = c.get("/api/versions/current")
        check("初始无现役版本(404)", r.status_code == 404)

        r = c.post("/api/versions", json={"note": "初版", "columns": [
            {"role": "station", "name": "站号"},
            {"role": "hour", "name": "UTC小时"},
            {"name": "气温", "type": "integer"},
            {"name": "气压", "type": "integer"},
        ]})
        check("发布初版 v1", r.status_code == 201 and r.json()["version_no"] == 1, r.text[:300])
        cols1 = {x["name"]: x for x in r.json()["columns"]}
        temp_id, pres_id = cols1["气温"]["column_id"], cols1["气压"]["column_id"]

        print("== 2. 按现役结构上传批次A ==")
        csv_a = ("站号,UTC小时,气温,气压\n"
                 "S01,2026-11-18T00:00,5,1013\n"
                 "S01,2026-11-18T01:00,6,1012\n"
                 "S02,2026-11-18 00:00,3,1015\n")
        r = up(c, "a.csv", csv_a)
        check("批次A上传成功(3行)", r.status_code == 201 and r.json()["row_count"] == 3, r.text[:300])

        r = c.get("/api/observations")
        d = r.json()
        check("现役查询返回3行", r.status_code == 200 and d["row_count"] == 3, r.text[:300])
        check("小时格式已规范化(空格→T)",
              all(row["values"]["UTC小时"].startswith("2026-11-18T") for row in d["rows"]))

        print("== 3. v2：改名(气温→气温_C, 站号→station_code) + 扩型(气压→小数) ==")
        r = c.post("/api/versions", json={"note": "v2", "columns": [
            {"source": "station", "name": "station_code"},
            {"source": "hour_utc", "name": "UTC小时"},
            {"source": temp_id, "name": "气温_C"},
            {"source": pres_id, "name": "气压", "type": "float"},
        ]})
        check("发布 v2", r.status_code == 201 and r.json()["version_no"] == 2, r.text[:300])

        csv_b = "station_code,UTC小时,气温_C,气压\nS01,2026-11-18T02:00,4,1011.5\n"
        r = up(c, "b.csv", csv_b)
        check("批次B按v2上传成功", r.status_code == 201 and r.json()["version_no"] == 2, r.text[:300])

        print("== 4. v3：再改名(气温_C→温度) + 新增可空列(湿度) ==")
        r = c.post("/api/versions", json={"note": "v3", "columns": [
            {"source": "station", "name": "station_code"},
            {"source": "hour_utc", "name": "UTC小时"},
            {"source": temp_id, "name": "温度"},
            {"source": pres_id, "name": "气压"},
            {"source": None, "name": "湿度", "type": "integer", "nullable": True},
        ]})
        check("发布 v3", r.status_code == 201 and r.json()["version_no"] == 3, r.text[:300])
        hum_id = {x["name"]: x for x in r.json()["columns"]}["湿度"]["column_id"]

        csv_c = "station_code,UTC小时,温度,气压,湿度\nS01,2026-11-18T03:00,3,1010.8,87\n"
        r = up(c, "c.csv", csv_c)
        check("批次C按v3上传成功", r.status_code == 201, r.text[:300])

        print("== 5. 现役视角：跨批次统一列名展示 ==")
        r = c.get("/api/observations", params={"station": "S01"})
        d = r.json()
        check("现役视角列名为最新命名",
              [x["name"] for x in d["version"]["columns"]] ==
              ["station_code", "UTC小时", "温度", "气压", "湿度"], r.text[:300])
        rows = {row["values"]["UTC小时"]: row["values"] for row in d["rows"]}
        r0 = rows.get("2026-11-18T00:00", {})
        check("连续改名后旧值找回(v1气温→温度=5)", r0.get("温度") == 5,
              json.dumps(r0, ensure_ascii=False))
        check("扩型不改原值(气压=1013 仍为整数)",
              r0.get("气压") == 1013 and isinstance(r0.get("气压"), int), repr(r0.get("气压")))
        check("新增列在旧行显示空值(湿度=None)", r0.get("湿度") is None, repr(r0.get("湿度")))
        check("v2批次气压小数值=1011.5", rows.get("2026-11-18T02:00", {}).get("气压") == 1011.5)
        check("v3批次湿度=87", rows.get("2026-11-18T03:00", {}).get("湿度") == 87)

        print("== 6. 历史视角：当时的列名与原始读数 ==")
        r = c.get("/api/observations", params={"version_no": 1})
        d = r.json()
        check("历史视角仅见v1批次(3行)", d["row_count"] == 3, r.text[:300])
        check("历史视角用当时列名",
              [x["name"] for x in d["version"]["columns"]] == ["站号", "UTC小时", "气温", "气压"])
        vals = [row["values"] for row in d["rows"]
                if row["values"]["站号"] == "S01" and row["values"]["UTC小时"] == "2026-11-18T00:00"]
        check("历史视角原始读数(气温=5, 气压=1013)",
              bool(vals) and vals[0]["气温"] == 5 and vals[0]["气压"] == 1013)

        r = c.get("/api/observations",
                  params={"hour_from": "2026-11-18T01:00", "hour_to": "2026-11-18T02:00"})
        check("时间范围过滤(01:00~02:00 共2行)", r.json()["row_count"] == 2, r.text[:300])

        print("== 7. 拒绝非法发布，且现役版本与供数不变 ==")

        def expect_reject(name, payload, kw=""):
            r = c.post("/api/versions", json=payload)
            ok = r.status_code == 400 and (not kw or kw in r.text)
            check(name, ok, f"status={r.status_code} {r.text[:200]}")

        expect_reject("拒绝删除旧列", {"columns": [
            {"source": "station", "name": "station_code"},
            {"source": "hour_utc", "name": "UTC小时"},
            {"source": temp_id, "name": "温度"},
        ]}, "删除")
        expect_reject("拒绝两列合并", {"columns": [
            {"source": "station", "name": "station_code"},
            {"source": "hour_utc", "name": "UTC小时"},
            {"source": [temp_id, pres_id], "name": "温压合一"},
            {"source": hum_id, "name": "湿度"},
        ]}, "合并")
        expect_reject("拒绝一列拆多列", {"columns": [
            {"source": "station", "name": "station_code"},
            {"source": "hour_utc", "name": "UTC小时"},
            {"source": temp_id, "name": "温度"},
            {"source": temp_id, "name": "温度副本"},
            {"source": pres_id, "name": "气压"},
            {"source": hum_id, "name": "湿度"},
        ]}, "拆")
        expect_reject("拒绝缩型(小数→整数)", {"columns": [
            {"source": "station", "name": "station_code"},
            {"source": "hour_utc", "name": "UTC小时"},
            {"source": temp_id, "name": "温度"},
            {"source": pres_id, "name": "气压", "type": "integer"},
            {"source": hum_id, "name": "湿度"},
        ]}, "扩型")
        expect_reject("拒绝改站号列类型", {"columns": [
            {"source": "station", "name": "station_code", "type": "text"},
            {"source": "hour_utc", "name": "UTC小时"},
            {"source": temp_id, "name": "温度"},
            {"source": pres_id, "name": "气压"},
            {"source": hum_id, "name": "湿度"},
        ]}, "不可更改")
        expect_reject("拒绝新增非空列", {"columns": [
            {"source": "station", "name": "station_code"},
            {"source": "hour_utc", "name": "UTC小时"},
            {"source": temp_id, "name": "温度"},
            {"source": pres_id, "name": "气压"},
            {"source": hum_id, "name": "湿度"},
            {"source": None, "name": "风向", "type": "integer", "nullable": False},
        ]}, "可空")
        expect_reject("拒绝列名重复", {"columns": [
            {"source": "station", "name": "station_code"},
            {"source": "hour_utc", "name": "UTC小时"},
            {"source": temp_id, "name": "气压"},
            {"source": pres_id, "name": "气压"},
            {"source": hum_id, "name": "湿度"},
        ]}, "重复")

        r = c.get("/api/versions/current")
        check("连续失败发布后现役版本仍为 v3", r.json()["version_no"] == 3, r.text[:200])
        r = c.get("/api/observations")
        check("失败发布后供数不受影响(仍5行)", r.json()["row_count"] == 5, r.text[:200])

        print("== 8. 拒绝非法上传，且不产生半成品批次 ==")
        n_before = len(c.get("/api/batches").json()["batches"])
        r = up(c, "bad.csv", "station_code,UTC小时,温度,气压,湿度\nS01,2026-11-18T04:00,abc,1009.1,80\n")
        check("拒绝类型错误CSV(温度=abc)", r.status_code == 400 and "温度" in r.text, r.text[:200])
        r = up(c, "bad2.csv", "station_code,UTC小时,温度\nS01,2026-11-18T04:00,3\n")
        check("拒绝缺列CSV", r.status_code == 400 and "缺少列" in r.text, r.text[:200])
        r = up(c, "bad3.csv", "station_code,UTC小时,温度,气压,湿度\nS01,2026-11-18T04:00,3,1009.1,80\nEXTRA\n")
        check("拒绝列数不符的行", r.status_code == 400, r.text[:200])
        n_after = len(c.get("/api/batches").json()["batches"])
        check("失败上传不产生批次", n_before == n_after, f"{n_before}→{n_after}")
        r = c.get("/api/observations")
        check("失败上传后供数不受影响(仍5行)", r.json()["row_count"] == 5, r.text[:200])

        print("== 9. 交错操作冒烟：上传→失败发布→再上传→查询 ==")
        expect_reject("交错中仍拒绝删列", {"columns": [
            {"source": "station", "name": "station_code"},
            {"source": "hour_utc", "name": "UTC小时"},
        ]}, "删除")
        csv_d = "station_code,UTC小时,温度,气压,湿度\nS02,2026-11-18T03:00,1,1016.0,60\n"
        r = up(c, "d.csv", csv_d)
        check("交错后再上传成功", r.status_code == 201, r.text[:200])
        r = c.get("/api/observations", params={"station": "S02"})
        d = r.json()
        check("交错后数据一致(S02共2行, 结构为v3列名)",
              d["row_count"] == 2 and
              all(row["values"].get("温度") is not None for row in d["rows"]), r.text[:300])

        print("== 10. 其他边界 ==")
        r = c.get("/api/observations", params={"version_no": 99})
        check("查询不存在版本返回404", r.status_code == 404)
        r = c.get("/api/observations", params={"hour_from": "not-a-time"})
        check("非法时间参数返回400", r.status_code == 400)
        r = c.get("/")
        check("首页HTML可访问", r.status_code == 200 and "现役结构" in r.text)

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
