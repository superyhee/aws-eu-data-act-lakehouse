"""Data Act API 的授权与输入校验测试。

聚焦安全属性，而不是快乐路径：
  - 第三方不得删除整车数据，也不得给自己再授权（提权）
  - 授权过期后即使 DynamoDB TTL 尚未清理也必须拒绝
  - 导出/擦除的轮询端点必须校验归属，不能凭 ID 取回他人数据
  - 外部输入不得未经校验拼进 SQL
  - VIN 不得以明文进入日志

运行：python3 -m unittest discover -s tests -v
"""
import importlib
import json
import os
import sys
import time
import types
import unittest
from pathlib import Path

API_DIR = Path(__file__).resolve().parent.parent / "assets" / "lambda" / "data_api"


# --------------------------------------------------------------------------- #
# boto3 替身：在导入被测模块之前注入，避免真实建连
# --------------------------------------------------------------------------- #
class InvalidRequestException(Exception):
    """Athena 对格式合法但不存在的 QueryExecutionId 抛的就是这个。"""


class FakeAthena:
    def __init__(self):
        self.started = []
        self.executions = {}

    class exceptions:  # noqa: N801
        InvalidRequestException = InvalidRequestException

    def start_query_execution(self, **kwargs):
        self.started.append(kwargs)
        # Athena 的 QueryExecutionId 是 36 字符的 UUID，格式必须真实，
        # 否则会被 handler 的格式校验挡在归属校验之前
        n = len(self.started)
        qid = f"{n:08x}-1111-2222-3333-444455556666"
        self.executions[qid] = {
            "Status": {"State": "SUCCEEDED"},
            "WorkGroup": kwargs.get("WorkGroup"),
            "Query": kwargs["QueryString"],
            "Statistics": {"DataScannedInBytes": 1024},
        }
        return {"QueryExecutionId": qid}

    def get_query_execution(self, QueryExecutionId):  # noqa: N803
        if QueryExecutionId not in self.executions:
            raise InvalidRequestException(
                f"QueryExecution {QueryExecutionId} was not found")
        return {"QueryExecution": self.executions[QueryExecutionId]}

    def get_paginator(self, _name):
        outer = self

        class P:
            def paginate(self, QueryExecutionId):  # noqa: N803
                return [{
                    "ResultSet": {
                        "ResultSetMetadata": {"ColumnInfo": [{"Label": "vin"}]},
                        "Rows": [
                            {"Data": [{"VarCharValue": "vin"}]},
                            {"Data": [{"VarCharValue": "SAMPLEVEH02123456"}]},
                        ],
                    }
                }]

        return P()


class FakeS3:
    """S3 替身。

    object_count 可调，且 list_objects_v2 严格按每页 1000 个 key 切分——真实
    API 就是这个上限。只有这样，漏掉分页的实现才会在测试里暴露出来。
    """

    PAGE_SIZE = 1000

    def __init__(self):
        self.object_count = 1

    def _keys(self, prefix):
        return [f"{prefix}part-{i}.csv" for i in range(self.object_count)]

    def list_objects_v2(self, **kwargs):
        keys = self._keys(kwargs["Prefix"])[: self.PAGE_SIZE]
        return {"Contents": [{"Key": k, "Size": 42} for k in keys]}

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        outer = self

        class P:
            def paginate(self, Bucket, Prefix):  # noqa: N803
                keys = outer._keys(Prefix)
                for start in range(0, max(len(keys), 1), outer.PAGE_SIZE):
                    chunk = keys[start:start + outer.PAGE_SIZE]
                    yield {"Contents": [{"Key": k, "Size": 42} for k in chunk]}

        return P()

    def generate_presigned_url(self, *_args, **kwargs):
        return "https://example.invalid/" + kwargs["Params"]["Key"]


class ConcurrentRunsExceededException(Exception):
    pass


class EntityNotFoundException(Exception):
    pass


class FakeGlue:
    class exceptions:  # noqa: N801
        ConcurrentRunsExceededException = ConcurrentRunsExceededException
        EntityNotFoundException = EntityNotFoundException

    def __init__(self):
        self.runs = {}
        self.raise_concurrent = False

    def start_job_run(self, **kwargs):
        if self.raise_concurrent:
            raise ConcurrentRunsExceededException("limit")
        run_id = "jr_" + "a" * 16
        self.runs[run_id] = {
            "JobRunState": "RUNNING",
            "Arguments": kwargs["Arguments"],
            "StartedOn": None,
        }
        return {"JobRunId": run_id}

    def get_job_run(self, JobName, RunId):  # noqa: N803
        if RunId not in self.runs:
            raise EntityNotFoundException(RunId)
        return {"JobRun": self.runs[RunId]}


class FakeTable:
    def __init__(self):
        self.items = {}
        self.updates = []

    def get_item(self, Key):  # noqa: N803
        item = self.items.get((Key["user_id"], Key["vin"]))
        return {"Item": dict(item)} if item else {}

    def put_item(self, Item):  # noqa: N803
        self.items[(Item["user_id"], Item["vin"])] = Item

    def update_item(self, **kwargs):
        self.updates.append(kwargs)


FAKE_ATHENA = FakeAthena()
FAKE_S3 = FakeS3()
FAKE_GLUE = FakeGlue()
FAKE_TABLE = FakeTable()


def _install_fake_boto3():
    fake = types.ModuleType("boto3")

    def client(name, **_kwargs):
        return {"athena": FAKE_ATHENA, "s3": FAKE_S3, "glue": FAKE_GLUE}[name]

    def resource(name, **_kwargs):
        assert name == "dynamodb"
        return types.SimpleNamespace(Table=lambda _n: FAKE_TABLE)

    fake.client = client
    fake.resource = resource
    sys.modules["boto3"] = fake


os.environ.update({
    "AWS_REGION": "eu-central-1",
    "GLUE_DATABASE": "vehicle_iot",
    "TABLE_NAME": "vehicle_telemetry",
    "API_WORKGROUP": "data-access-api-dev",
    "ATHENA_OUTPUT": "s3://results/query-results/",
    "EXPORTS_BUCKET": "exports-bucket",
    "AUTH_TABLE": "VehicleAuthorization",
    "ERASURE_JOB_NAME": "iov-iceberg-maintenance-dev",
    "MAX_ROW_LIMIT": "10000",
})
_install_fake_boto3()
sys.path.insert(0, str(API_DIR))
api = importlib.import_module("index")

OWNER_VIN = "SAMPLEVEH02123456"
OTHER_VIN = "SAMPLEVEH02999999"


def event(method, resource, *, sub, path=None, body=None, qs=None):
    return {
        "httpMethod": method,
        "resource": resource,
        "pathParameters": path or {},
        "queryStringParameters": qs,
        "body": body,
        "requestContext": {"authorizer": {"claims": {"sub": sub}}},
    }


class AuthorizationTest(unittest.TestCase):
    def setUp(self):
        FAKE_TABLE.items.clear()
        FAKE_TABLE.updates.clear()
        FAKE_GLUE.runs.clear()
        FAKE_GLUE.raise_concurrent = False
        FAKE_ATHENA.started.clear()
        FAKE_ATHENA.executions.clear()
        # 车主记录
        FAKE_TABLE.put_item({"user_id": "owner-sub", "vin": OWNER_VIN, "role": "OWNER"})
        # 第三方记录（保险公司），仅授权两个信号
        FAKE_TABLE.put_item({
            "user_id": "insurer-1", "vin": OWNER_VIN, "role": "THIRD_PARTY",
            "allowed_signals": {"speed", "battery_soc"},
            "expires_at": int(time.time()) + 3600,
        })

    # ---------------------------------------------------------------- 越权删除
    def test_third_party_cannot_erase_vehicle_data(self):
        resp = api.handler(
            event("DELETE", "/vehicles/{vin}", sub="insurer-1", path={"vin": OWNER_VIN}), None)
        self.assertEqual(403, resp["statusCode"])
        self.assertIn("owner", resp["body"].lower())
        self.assertEqual({}, FAKE_GLUE.runs, "不得启动任何擦除作业")

    def test_owner_can_erase_vehicle_data(self):
        resp = api.handler(
            event("DELETE", "/vehicles/{vin}", sub="owner-sub", path={"vin": OWNER_VIN}), None)
        self.assertEqual(202, resp["statusCode"])
        self.assertEqual(1, len(FAKE_GLUE.runs))
        args = list(FAKE_GLUE.runs.values())[0]["Arguments"]
        self.assertEqual("erasure", args["--action"])
        self.assertEqual(OWNER_VIN, args["--vins"])

    # ------------------------------------------------------------------ 提权
    def test_third_party_cannot_grant_itself_more_scope(self):
        resp = api.handler(event(
            "POST", "/vehicles/{vin}/share", sub="insurer-1", path={"vin": OWNER_VIN},
            body='{"third_party_id":"insurer-1","allowed_signals":["vehicle_speed","motor_temp"]}',
        ), None)
        self.assertEqual(403, resp["statusCode"])
        # 授权范围不得被改写
        self.assertEqual({"speed", "battery_soc"},
                         FAKE_TABLE.items[("insurer-1", OWNER_VIN)]["allowed_signals"])

    def test_owner_share_marks_record_as_third_party(self):
        resp = api.handler(event(
            "POST", "/vehicles/{vin}/share", sub="owner-sub", path={"vin": OWNER_VIN},
            body='{"third_party_id":"garage-9","allowed_signals":["motor_temp"],"duration_days":30}',
        ), None)
        self.assertEqual(200, resp["statusCode"])
        created = FAKE_TABLE.items[("garage-9", OWNER_VIN)]
        self.assertEqual("THIRD_PARTY", created["role"],
                         "新建授权必须标记为第三方，否则它会获得车主级能力")

    # -------------------------------------------------------------- 授权边界
    def test_no_grant_is_denied(self):
        resp = api.handler(event(
            "GET", "/vehicles/{vin}/telemetry", sub="stranger", path={"vin": OWNER_VIN}), None)
        self.assertEqual(403, resp["statusCode"])

    def test_expired_grant_denied_even_before_ttl_cleanup(self):
        FAKE_TABLE.put_item({
            "user_id": "insurer-2", "vin": OWNER_VIN, "role": "THIRD_PARTY",
            "allowed_signals": {"speed"},
            "expires_at": int(time.time()) - 1,   # 已过期但 TTL 还没删
        })
        resp = api.handler(event(
            "GET", "/vehicles/{vin}/telemetry", sub="insurer-2", path={"vin": OWNER_VIN}), None)
        self.assertEqual(403, resp["statusCode"])
        self.assertIn("expired", resp["body"].lower())

    def test_third_party_without_signal_scope_is_denied_not_granted_everything(self):
        FAKE_TABLE.put_item({
            "user_id": "insurer-3", "vin": OWNER_VIN, "role": "THIRD_PARTY",
        })  # 没有 allowed_signals
        resp = api.handler(event(
            "GET", "/vehicles/{vin}/telemetry", sub="insurer-3", path={"vin": OWNER_VIN}), None)
        self.assertEqual(403, resp["statusCode"],
                         "缺少信号白名单必须拒绝，不能退化为放行全部信号")

    def test_third_party_query_is_restricted_to_allowed_signals(self):
        resp = api.handler(event(
            "GET", "/vehicles/{vin}/telemetry", sub="insurer-1", path={"vin": OWNER_VIN}), None)
        self.assertEqual(200, resp["statusCode"])
        sql = FAKE_ATHENA.started[0]["QueryString"]
        params = FAKE_ATHENA.started[0]["ExecutionParameters"]
        self.assertIn("signal_name IN (?, ?)", sql)
        self.assertEqual([OWNER_VIN, "battery_soc", "speed"], params)


class OwnershipOfAsyncJobsTest(unittest.TestCase):
    def setUp(self):
        FAKE_TABLE.items.clear()
        FAKE_GLUE.runs.clear()
        FAKE_ATHENA.started.clear()
        FAKE_ATHENA.executions.clear()
        FAKE_TABLE.put_item({"user_id": "owner-sub", "vin": OWNER_VIN, "role": "OWNER"})
        FAKE_TABLE.put_item({"user_id": "other-owner", "vin": OTHER_VIN, "role": "OWNER"})

    def test_cannot_fetch_another_owners_export_urls(self):
        started = api.handler(event(
            "POST", "/vehicles/{vin}/export", sub="owner-sub", path={"vin": OWNER_VIN},
            body='{"format":"CSV"}'), None)
        self.assertEqual(202, started["statusCode"])
        qid = list(FAKE_ATHENA.executions.keys())[0]

        # 另一个合法用户拿着 queryId 来取下载链接
        stolen = api.handler(event(
            "GET", "/exports/{queryId}", sub="other-owner", path={"queryId": qid}), None)
        self.assertEqual(403, stolen["statusCode"])
        self.assertNotIn("downloadUrls", stolen["body"])

        # 本人可以取回
        own = api.handler(event(
            "GET", "/exports/{queryId}", sub="owner-sub", path={"queryId": qid}), None)
        self.assertEqual(200, own["statusCode"])
        self.assertIn("downloadUrls", own["body"])

    def test_cannot_poll_another_owners_erasure_job(self):
        api.handler(event("DELETE", "/vehicles/{vin}", sub="owner-sub",
                          path={"vin": OWNER_VIN}), None)
        run_id = list(FAKE_GLUE.runs.keys())[0]

        stolen = api.handler(event("GET", "/erasures/{jobRunId}", sub="other-owner",
                                   path={"jobRunId": run_id}), None)
        self.assertEqual(403, stolen["statusCode"])

        own = api.handler(event("GET", "/erasures/{jobRunId}", sub="owner-sub",
                                path={"jobRunId": run_id}), None)
        self.assertEqual(200, own["statusCode"])

    def test_erasure_endpoint_does_not_expose_other_maintenance_jobs(self):
        FAKE_GLUE.runs["jr_" + "b" * 16] = {
            "JobRunState": "RUNNING",
            "Arguments": {"--action": "compact"},
            "StartedOn": None,
        }
        resp = api.handler(event("GET", "/erasures/{jobRunId}", sub="owner-sub",
                                 path={"jobRunId": "jr_" + "b" * 16}), None)
        self.assertEqual(403, resp["statusCode"])

    def test_concurrency_limit_surfaces_as_retryable_not_500(self):
        FAKE_GLUE.raise_concurrent = True
        resp = api.handler(event("DELETE", "/vehicles/{vin}", sub="owner-sub",
                                 path={"vin": OWNER_VIN}), None)
        self.assertEqual(429, resp["statusCode"],
                         "有法定时限的擦除请求不能以 500 静默失败")
        self.assertIn("retry", resp["body"].lower())


class ExportRetrievalTest(unittest.TestCase):
    """导出取回：必须完整，且"找不到"要和擦除端点一样报 404。"""

    def setUp(self):
        FAKE_TABLE.items.clear()
        FAKE_ATHENA.started.clear()
        FAKE_ATHENA.executions.clear()
        FAKE_S3.object_count = 1
        FAKE_TABLE.put_item({"user_id": "owner-sub", "vin": OWNER_VIN, "role": "OWNER"})

    def tearDown(self):
        FAKE_S3.object_count = 1

    def _start_export(self):
        api.handler(event("POST", "/vehicles/{vin}/export", sub="owner-sub",
                          path={"vin": OWNER_VIN}, body='{"format":"CSV"}'), None)
        return list(FAKE_ATHENA.executions.keys())[0]

    def test_all_export_files_are_returned_beyond_one_page(self):
        """UNLOAD 并行写多文件，超过 1000 个时必须续页。

        list_objects_v2 单次最多 1000 个 key 且不自动续页。少给下载链接却仍
        返回 SUCCEEDED，等于交付不完整的数据副本而调用方无法察觉——这对
        数据可携带权导出是正确性缺陷。
        """
        FAKE_S3.object_count = 2500
        qid = self._start_export()
        resp = api.handler(event("GET", "/exports/{queryId}", sub="owner-sub",
                                 path={"queryId": qid}), None)
        self.assertEqual(200, resp["statusCode"])
        body = json.loads(resp["body"])
        self.assertEqual(2500, body["fileCount"],
                         "导出链接被截断：list_objects_v2 未分页")
        self.assertEqual(2500, len(body["downloadUrls"]))
        self.assertEqual(2500, len(set(body["downloadUrls"])), "链接不得重复")

    def test_unknown_query_id_is_404_not_500(self):
        """格式合法但不存在的 ID：Athena 抛 InvalidRequestException。
        不捕获会变成 500，与 GET /erasures/{jobRunId} 的 404 语义不一致。"""
        resp = api.handler(event(
            "GET", "/exports/{queryId}", sub="owner-sub",
            path={"queryId": "deadbeef-1111-2222-3333-444455556666"}), None)
        self.assertEqual(404, resp["statusCode"])
        self.assertNotIn("downloadUrls", resp["body"])


class RequestBodyTest(unittest.TestCase):
    """畸形请求体属客户端错误，必须是 400。

    报成 500 会误导调用方来排查本服务，也会让真正的服务端故障淹没在告警噪声里。
    """

    ENDPOINTS = [
        ("/vehicles/{vin}/export", "POST"),
        ("/vehicles/{vin}/share", "POST"),
    ]
    BAD_BODIES = [
        ("非法 JSON", "{format:"),
        ("JSON 数组", "[]"),
        ("JSON 字符串", '"csv"'),
        ("JSON null", "null"),
        ("JSON 数字", "42"),
    ]

    def setUp(self):
        FAKE_TABLE.items.clear()
        FAKE_ATHENA.started.clear()
        FAKE_TABLE.put_item({"user_id": "owner-sub", "vin": OWNER_VIN, "role": "OWNER"})

    def test_malformed_body_is_client_error(self):
        for resource, method in self.ENDPOINTS:
            for label, body in self.BAD_BODIES:
                with self.subTest(endpoint=resource, body=label):
                    resp = api.handler(event(method, resource, sub="owner-sub",
                                             path={"vin": OWNER_VIN}, body=body), None)
                    self.assertEqual(400, resp["statusCode"])
                    self.assertNotIn("Internal server error", resp["body"])
        self.assertEqual([], FAKE_ATHENA.started, "畸形请求体不得触发任何查询")

    def test_missing_body_still_uses_defaults(self):
        """空 body 是合法的：导出应退回默认 CSV，不能被新校验挡掉。"""
        resp = api.handler(event("POST", "/vehicles/{vin}/export", sub="owner-sub",
                                 path={"vin": OWNER_VIN}, body=None), None)
        self.assertEqual(202, resp["statusCode"])
        self.assertEqual("CSV", json.loads(resp["body"])["format"])


class InputValidationTest(unittest.TestCase):
    def setUp(self):
        FAKE_TABLE.items.clear()
        FAKE_ATHENA.started.clear()
        FAKE_TABLE.put_item({"user_id": "owner-sub", "vin": OWNER_VIN, "role": "OWNER"})

    def test_rejects_sql_injection_in_vin(self):
        for bad in ["' OR '1'='1", "SAMPLEVEH0212345'; DROP TABLE x;--", "TOOSHORT",
                    "SAMPLEVEH021234IO"]:   # 含 ISO 3779 禁用字母 I/O
            resp = api.handler(event("GET", "/vehicles/{vin}/telemetry",
                                     sub="owner-sub", path={"vin": bad}), None)
            self.assertEqual(400, resp["statusCode"], f"应拒绝 VIN: {bad!r}")
        self.assertEqual([], FAKE_ATHENA.started, "非法输入不得触发任何查询")

    def test_rejects_malformed_dates_and_limits(self):
        base = dict(sub="owner-sub", path={"vin": OWNER_VIN})
        for qs in [{"start": "2026-01-01'; DROP"}, {"end": "not-a-date"}, {"limit": "abc"}]:
            resp = api.handler(
                event("GET", "/vehicles/{vin}/telemetry", qs=qs, **base), None)
            self.assertEqual(400, resp["statusCode"], f"应拒绝参数: {qs}")

    def test_limit_is_capped_to_max_row_limit(self):
        api.handler(event("GET", "/vehicles/{vin}/telemetry", sub="owner-sub",
                          path={"vin": OWNER_VIN}, qs={"limit": "999999"}), None)
        self.assertIn("LIMIT 10000", FAKE_ATHENA.started[0]["QueryString"])

    def test_vin_is_parameterized_not_interpolated(self):
        api.handler(event("GET", "/vehicles/{vin}/telemetry", sub="owner-sub",
                          path={"vin": OWNER_VIN}), None)
        sql = FAKE_ATHENA.started[0]["QueryString"]
        self.assertIn("vin = ?", sql)
        self.assertNotIn(OWNER_VIN, sql, "VIN 必须走参数化，不得出现在 SQL 文本中")
        self.assertEqual([OWNER_VIN], FAKE_ATHENA.started[0]["ExecutionParameters"])

    def test_export_uses_unload_with_parameterized_vin(self):
        api.handler(event("POST", "/vehicles/{vin}/export", sub="owner-sub",
                          path={"vin": OWNER_VIN}, body='{"format":"CSV"}'), None)
        sql = FAKE_ATHENA.started[0]["QueryString"]
        self.assertTrue(sql.startswith("UNLOAD"), "导出应使用 UNLOAD 而非 CTAS")
        self.assertNotIn("CREATE TABLE", sql, "不应创建需要事后清理的临时表")
        self.assertIn("vin = ?", sql)
        # Athena 的 UNLOAD 写出端只支持毫秒精度，timestamp(6) 必须转成文本
        # 才能导出（否则报 Incorrect timestamp precision）
        self.assertIn("CAST(event_time AS VARCHAR)", sql,
                      "导出必须把 timestamp(6) 转为文本，否则 Athena UNLOAD 会失败")

    def test_rejects_invalid_export_format(self):
        resp = api.handler(event("POST", "/vehicles/{vin}/export", sub="owner-sub",
                                 path={"vin": OWNER_VIN}, body='{"format":"EXE"}'), None)
        self.assertEqual(400, resp["statusCode"])

    def test_export_declares_compression_so_client_knows_what_it_gets(self):
        """UNLOAD 对文本格式默认 gzip；不声明的话调用方会拿到看似 .csv 的二进制。"""
        resp = api.handler(event("POST", "/vehicles/{vin}/export", sub="owner-sub",
                                 path={"vin": OWNER_VIN}, body='{"format":"CSV"}'), None)
        body = json.loads(resp["body"])
        self.assertEqual("gzip", body["compression"], "默认应声明为 gzip")
        self.assertIn("compression = 'gzip'", FAKE_ATHENA.started[0]["QueryString"])

    def test_export_honours_compression_none(self):
        resp = api.handler(event("POST", "/vehicles/{vin}/export", sub="owner-sub",
                                 path={"vin": OWNER_VIN},
                                 body='{"format":"CSV","compression":"none"}'), None)
        self.assertEqual("none", json.loads(resp["body"])["compression"])
        self.assertIn("compression = 'none'", FAKE_ATHENA.started[0]["QueryString"])

    def test_rejects_invalid_compression(self):
        resp = api.handler(event("POST", "/vehicles/{vin}/export", sub="owner-sub",
                                 path={"vin": OWNER_VIN},
                                 body='{"format":"CSV","compression":"bzip2"}'), None)
        self.assertEqual(400, resp["statusCode"])

    def test_parquet_export_reports_athena_default_gzip(self):
        """PARQUET 不传 compression 选项，由 Athena 用其默认值。

        该默认值是 gzip，不是 snappy（Athena UNLOAD 文档：
        "For ORC, the default is zlib, and for Parquet, the default is gzip"）。
        响应必须回报实际值——声明 snappy 而实际是 gzip，调用方会按错误方式解压。
        """
        resp = api.handler(event("POST", "/vehicles/{vin}/export", sub="owner-sub",
                                 path={"vin": OWNER_VIN}, body='{"format":"PARQUET"}'), None)
        self.assertEqual("gzip", json.loads(resp["body"])["compression"])
        self.assertNotIn("compression =", FAKE_ATHENA.started[0]["QueryString"])

    def test_all_export_formats_cast_timestamp_to_text(self):
        """Athena UNLOAD 对 PARQUET/JSON/TEXTFILE 三种格式都只支持毫秒精度，
        因此每种格式都必须把 timestamp(6) 转成文本，否则导出直接失败。"""
        for fmt in ("CSV", "JSON", "PARQUET"):
            FAKE_ATHENA.started.clear()
            api.handler(event("POST", "/vehicles/{vin}/export", sub="owner-sub",
                              path={"vin": OWNER_VIN}, body=f'{{"format":"{fmt}"}}'), None)
            self.assertIn("CAST(event_time AS VARCHAR)", FAKE_ATHENA.started[0]["QueryString"],
                          f"{fmt} 导出未转换时间戳，会触发 Incorrect timestamp precision")


class PiiRedactionTest(unittest.TestCase):
    def test_mask_vin_keeps_only_last_four(self):
        self.assertEqual("***3456", api.mask_vin(OWNER_VIN))

    def test_redact_removes_vin_from_sql_text(self):
        text = f"DELETE FROM t WHERE vin IN ('{OWNER_VIN}')"
        self.assertNotIn(OWNER_VIN, api.redact(text))
        self.assertIn("***3456", api.redact(text))


if __name__ == "__main__":
    unittest.main(verbosity=2)
