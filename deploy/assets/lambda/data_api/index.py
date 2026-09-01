"""EU Data Act / GDPR 数据访问 API。

端点
----
GET    /vehicles/{vin}/telemetry            按时间范围查询遥测数据
GET    /vehicles/{vin}/signals/{signal}     查询单个信号
POST   /vehicles/{vin}/export               异步导出全量数据（数据可携带权）
GET    /exports/{queryId}                   查询导出进度并取回下载链接
DELETE /vehicles/{vin}                      物理擦除该车全部数据（被遗忘权）
GET    /erasures/{jobRunId}                 查询擦除进度
POST   /vehicles/{vin}/share                向第三方授权（Data Act Art.5）

安全设计
--------
1) VIN 一律用 ISO 3779 正则严格校验后才允许进入 SQL；同时对查询条件使用
   Athena 参数化查询（ExecutionParameters）做第二层防护。
2) 导出使用 Athena UNLOAD 而非 CTAS：不创建临时表，因此没有表名拼接的注入面，
   也不留需要事后清理的残留表。
3) 删除走 Glue erasure 作业而非 Athena DELETE：Athena 恒为 merge-on-read，
   只写 delete file（逻辑删除）；Glue 侧表属性为 copy-on-write，DELETE 会
   立即重写数据文件并随后过期 snapshot，实现分钟级物理擦除。
"""
import datetime
import json
import logging
import os
import re
import time

import boto3

log = logging.getLogger()
log.setLevel(logging.INFO)

REGION = os.environ["AWS_REGION"]
DATABASE = os.environ["GLUE_DATABASE"]
TABLE = os.environ["TABLE_NAME"]
API_WORKGROUP = os.environ["API_WORKGROUP"]
ATHENA_OUTPUT = os.environ["ATHENA_OUTPUT"]
EXPORTS_BUCKET = os.environ["EXPORTS_BUCKET"]
AUTH_TABLE = os.environ["AUTH_TABLE"]
ERASURE_JOB = os.environ["ERASURE_JOB_NAME"]
MAX_ROW_LIMIT = int(os.environ.get("MAX_ROW_LIMIT", "10000"))

# 同步等待上限。API Gateway 硬超时 29s，留 4s 余量做结果拉取与序列化
SYNC_WAIT_SECONDS = 24
# pre-signed URL 有效期。用 Lambda 的临时凭证签名时，URL 在凭证过期后即失效，
# 因此不能声明 24 小时——那超出了 Lambda 执行角色会话的有效期。
# 6 小时在凭证生命周期内，且足够完成一次大文件下载；过期后重新调用
# GET /exports/{queryId} 即可换取新链接（导出产物本身保留 7 天）。
PRESIGN_TTL_SECONDS = 6 * 3600

VIN_RE = re.compile(r"^[A-HJ-NPR-Z0-9]{17}$")  # ISO 3779：排除 I / O / Q
SIGNAL_RE = re.compile(r"^[A-Za-z0-9_]{1,64}$")
THIRD_PARTY_RE = re.compile(r"^[A-Za-z0-9_\-]{1,64}$")
SELECT_COLUMNS = (
    "vin, event_time, signal_name, signal_value, latitude, longitude, speed, battery_soc"
)
# 导出用的列清单：event_time 转成文本。
#
# 实测原因（三种格式逐个验证过，不是只影响 CSV）：
#   Athena 的 UNLOAD 写出端固定按毫秒精度处理时间戳，而 Iceberg 列是
#   timestamp(6)（微秒）。PARQUET / JSON / TEXTFILE 三种格式【全部】报同一个错：
#     NOT_SUPPORTED: Incorrect timestamp precision for timestamp(6);
#     the configured precision is MILLISECONDS; column name: event_time
#
# 两种可行写法及取舍（都已实测成功）：
#   a) CAST(event_time AS VARCHAR)      -> 保留完整微秒，类型退化为字符串
#   b) CAST(event_time AS timestamp(3)) -> 保留时间戳类型，精度截断到毫秒
#
# 选 (a)：这是"数据可携带权"导出，完整性优先于类型便利性。静默丢掉微秒
# 属于数据失真，而文本时间戳任何消费方都能解析。
# 若下游明确需要 PARQUET 的原生时间戳类型，可改用 (b) 并在响应中声明精度损失。
EXPORT_COLUMNS = (
    "vin, CAST(event_time AS VARCHAR) AS event_time, signal_name, signal_value, "
    "latitude, longitude, speed, battery_soc"
)

# 授权记录角色。车主可读全部信号并执行删除/再授权；第三方受信号白名单限制。
ROLE_OWNER = "OWNER"
ROLE_THIRD_PARTY = "THIRD_PARTY"


def mask_vin(vin: str) -> str:
    """VIN 可关联到具体车主，属个人数据，不应以明文进入日志。"""
    return f"***{vin[-4:]}" if len(vin) >= 4 else "***"


def redact(text: str) -> str:
    """把文本中出现的完整 VIN 替换为脱敏形式，用于 SQL 日志。"""
    return re.sub(r"\b[A-HJ-NPR-Z0-9]{17}\b", lambda m: mask_vin(m.group(0)), text)

athena = boto3.client("athena", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
glue = boto3.client("glue", region_name=REGION)
ddb = boto3.resource("dynamodb", region_name=REGION).Table(AUTH_TABLE)


class ApiError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


# --------------------------------------------------------------------------- #
# 输入校验
# --------------------------------------------------------------------------- #
def require_vin(params: dict) -> str:
    vin = (params or {}).get("vin", "").strip().upper()
    if not VIN_RE.match(vin):
        raise ApiError(400, "Invalid VIN: expected 17 chars, ISO 3779 alphabet (no I/O/Q)")
    return vin


def parse_date(value: str, field: str, end_of_day: bool) -> str:
    """严格解析日期，返回可安全嵌入 SQL 的时间戳字面量。"""
    try:
        parsed = datetime.datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        raise ApiError(400, f"Invalid {field}: expected YYYY-MM-DD")
    suffix = "23:59:59.999999" if end_of_day else "00:00:00.000000"
    return f"{parsed.strftime('%Y-%m-%d')} {suffix}"


def parse_limit(value) -> int:
    try:
        limit = int(value)
    except (TypeError, ValueError):
        raise ApiError(400, "Invalid limit: expected integer")
    if limit < 1:
        raise ApiError(400, "Invalid limit: must be >= 1")
    return min(limit, MAX_ROW_LIMIT)


def parse_body(event: dict) -> dict:
    """解析请求体，把畸形输入归类为 400 而不是 500。

    直接 json.loads 有两个失败面，都会冒到兜底处理器变成 500：
      - 非法 JSON        -> JSONDecodeError
      - 合法但非对象的 JSON（数组 / 字符串 / null）-> 后续 .get 抛 AttributeError
    客户端错误报成服务端错误会误导调用方来排查本服务，也会污染 5xx 告警。
    """
    try:
        body = json.loads(event.get("body") or "{}")
    except ValueError:
        raise ApiError(400, "Invalid request body: expected JSON")
    if not isinstance(body, dict):
        raise ApiError(400, "Invalid request body: expected a JSON object")
    return body


# --------------------------------------------------------------------------- #
# 授权
# --------------------------------------------------------------------------- #
def authorize(event: dict, vin: str) -> dict:
    """校验调用方对该 VIN 的访问权，返回授权记录（含角色与信号范围限制）。"""
    claims = (
        event.get("requestContext", {}).get("authorizer", {}).get("claims")
        or {}
    )
    user_id = claims.get("sub")
    if not user_id:
        raise ApiError(401, "Missing authenticated identity")

    item = ddb.get_item(Key={"user_id": user_id, "vin": vin}).get("Item")
    if not item:
        # 不区分"无权限"与"不存在"，避免通过响应差异枚举 VIN
        raise ApiError(403, "Not authorized for this VIN")

    expires_at = item.get("expires_at")
    if expires_at and int(expires_at) <= int(time.time()):
        # DynamoDB TTL 删除有延迟（最长 48h），必须在读取时再判一次
        raise ApiError(403, "Authorization expired")

    item["_user_id"] = user_id
    # 缺省视为第三方：授权记录若没有显式标记 OWNER，一律按最小权限对待，
    # 避免历史数据或人工写入的记录意外获得车主级能力。
    item["_role"] = ROLE_OWNER if item.get("role") == ROLE_OWNER else ROLE_THIRD_PARTY
    return item


def require_owner(grant: dict, action: str) -> None:
    """限定只有车主本人可执行的操作。

    删除是不可恢复的，重新授权会改变数据共享范围——这两类操作若允许第三方
    执行，被授权的保险公司/维修商就能擦掉整车数据，或给自己扩大信号范围提权。
    """
    if grant["_role"] != ROLE_OWNER:
        raise ApiError(403, f"{action} requires vehicle owner authorization")


def signal_scope_clause(grant: dict) -> tuple[str, list[str]]:
    """把授权范围转成 SQL 条件。

    本表 schema 中信号是行（signal_name/signal_value）而非列，
    所以范围限制表现为对 signal_name 的行级过滤。
    """
    allowed = grant.get("allowed_signals")
    if not allowed:
        # 车主可看全部信号；第三方必须有显式的信号白名单，否则拒绝，
        # 不能因为字段缺失就退化为"放行全部"。
        if grant["_role"] == ROLE_OWNER:
            return "", []
        raise ApiError(403, "Third-party authorization must define allowed_signals")
    signals = sorted(str(s) for s in allowed)
    for s in signals:
        if not SIGNAL_RE.match(s):
            raise ApiError(500, f"Corrupt authorization record: invalid signal {s!r}")
    placeholders = ", ".join("?" for _ in signals)
    return f" AND signal_name IN ({placeholders})", signals


# --------------------------------------------------------------------------- #
# Athena
# --------------------------------------------------------------------------- #
def start_query(sql: str, parameters: list[str]) -> str:
    # VIN 与参数都要脱敏后才写日志：CloudWatch 日志保留 6 个月，
    # 明文 VIN 落进去等于把个人数据复制到了第二处存储。
    log.info(
        "Athena query: %s | params=%s",
        redact(" ".join(sql.split())),
        [redact(p) for p in parameters],
    )
    kwargs = {
        "QueryString": sql,
        "QueryExecutionContext": {"Database": DATABASE},
        "WorkGroup": API_WORKGROUP,
        "ResultConfiguration": {"OutputLocation": ATHENA_OUTPUT},
    }
    if parameters:
        kwargs["ExecutionParameters"] = parameters
    return athena.start_query_execution(**kwargs)["QueryExecutionId"]


def wait_for_query(query_id: str) -> dict:
    deadline = time.time() + SYNC_WAIT_SECONDS
    delay = 0.4
    while time.time() < deadline:
        execution = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
        state = execution["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return execution
        time.sleep(delay)
        delay = min(delay * 1.5, 2.0)
    raise ApiError(
        504,
        f"Query {query_id} still running after {SYNC_WAIT_SECONDS}s; "
        f"use POST /vehicles/{{vin}}/export for large result sets",
    )


def collect_rows(query_id: str) -> list[dict]:
    rows: list[dict] = []
    columns: list[str] = []
    paginator = athena.get_paginator("get_query_results")
    for page in paginator.paginate(QueryExecutionId=query_id):
        meta = page["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
        if not columns:
            columns = [c["Label"] for c in meta]
        for index, row in enumerate(page["ResultSet"]["Rows"]):
            # 首页第一行是表头
            if not rows and index == 0:
                continue
            values = [cell.get("VarCharValue") for cell in row["Data"]]
            rows.append(dict(zip(columns, values)))
    return rows


# --------------------------------------------------------------------------- #
# 端点实现
# --------------------------------------------------------------------------- #
def get_telemetry(event: dict, vin: str, grant: dict, signal: str | None = None) -> dict:
    qs = event.get("queryStringParameters") or {}
    start = parse_date(qs.get("start", "1970-01-01"), "start", end_of_day=False)
    end = parse_date(qs.get("end", datetime.date.today().isoformat()), "end", end_of_day=True)
    limit = parse_limit(qs.get("limit", 1000))

    scope_sql, scope_params = signal_scope_clause(grant)
    params = [vin]
    signal_sql = ""
    if signal:
        if not SIGNAL_RE.match(signal):
            raise ApiError(400, "Invalid signal name")
        signal_sql = " AND signal_name = ?"
        params.append(signal)
    params.extend(scope_params)

    # vin / signal 走参数化；时间与 limit 已严格校验后内插
    # （Athena 不支持在 LIMIT 与 TIMESTAMP 字面量位置使用 ? 占位符）
    sql = (
        f"SELECT {SELECT_COLUMNS} FROM {TABLE} "
        f"WHERE vin = ?{signal_sql}{scope_sql} "
        f"AND event_time BETWEEN TIMESTAMP '{start}' AND TIMESTAMP '{end}' "
        f"ORDER BY event_time DESC LIMIT {limit}"
    )

    query_id = start_query(sql, params)
    execution = wait_for_query(query_id)
    if execution["Status"]["State"] != "SUCCEEDED":
        raise ApiError(502, execution["Status"].get("StateChangeReason", "Query failed"))

    rows = collect_rows(query_id)
    stats = execution.get("Statistics", {})
    return {
        "vin": vin,
        "count": len(rows),
        "queryId": query_id,
        "bytesScanned": stats.get("DataScannedInBytes"),
        "data": rows,
    }


def start_export(event: dict, vin: str, grant: dict) -> dict:
    body = parse_body(event)
    fmt = str(body.get("format", "CSV")).upper()
    if fmt not in ("CSV", "JSON", "PARQUET"):
        raise ApiError(400, "Invalid format: expected CSV, JSON or PARQUET")

    # Athena 的 UNLOAD 对 TEXTFILE / JSON 默认启用 gzip。大批量导出压缩很有价值，
    # 但必须在响应里明确告知调用方——否则拿到的 .csv 链接实际是 gzip 数据，
    # 直接打开是乱码。允许显式设为 none 以换取"下载即可读"。
    compression = str(body.get("compression", "gzip")).lower()
    if compression not in ("gzip", "none"):
        raise ApiError(400, "Invalid compression: expected gzip or none")

    scope_sql, scope_params = signal_scope_clause(grant)
    stamp = int(time.time())
    destination = f"s3://{EXPORTS_BUCKET}/exports/{vin}/{stamp}/"

    # UNLOAD 不创建表，所以没有 CTAS 的表名拼接注入面，也不留残留表需要清理。
    unload_format = "TEXTFILE" if fmt == "CSV" else fmt
    with_opts = f"format = '{unload_format}'"
    if fmt == "CSV":
        with_opts += ", field_delimiter = ','"
    if fmt in ("CSV", "JSON"):
        with_opts += f", compression = '{compression}'"
    else:
        # PARQUET 走列式内置压缩，这里不传 compression 选项，由 Athena 用它的
        # Parquet 默认值。该默认值是 gzip 而非 snappy —— 见 Athena UNLOAD 文档
        # "For ORC, the default is zlib, and for Parquet, the default is gzip"。
        # 必须按实际值回报：声明 snappy 而实际是 gzip，会让调用方按错误的方式解压。
        compression = "gzip"

    sql = (
        f"UNLOAD (SELECT {EXPORT_COLUMNS} FROM {TABLE} "
        f"WHERE vin = ?{scope_sql} ORDER BY event_time) "
        f"TO '{destination}' WITH ({with_opts})"
    )
    query_id = start_query(sql, [vin] + scope_params)

    return {
        "status": "EXPORTING",
        "queryId": query_id,
        "format": fmt,
        "compression": compression,
        "poll": f"GET /exports/{query_id}",
    }


def get_export(event: dict) -> dict:
    query_id = (event.get("pathParameters") or {}).get("queryId", "")
    if not re.match(r"^[0-9a-fA-F\-]{36}$", query_id):
        raise ApiError(400, "Invalid queryId")

    # 格式合法但不存在的 ID，Athena 抛 InvalidRequestException。不捕获会冒到
    # 兜底处理器变成 500；这与 GET /erasures/{jobRunId} 的 404 语义不一致。
    try:
        execution = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
    except athena.exceptions.InvalidRequestException:
        raise ApiError(404, "Unknown export job")
    if execution.get("WorkGroup") != API_WORKGROUP:
        raise ApiError(403, "Unknown export job")

    # UNLOAD 的目标路径记录在语句里，直接从执行记录中取回
    match = re.search(r"TO\s+'(s3://[^']+)'", execution.get("Query", ""))
    if not match:
        raise ApiError(403, "Unknown export job")
    destination = match.group(1)

    # 归属校验：从导出路径里取出 VIN，再走一遍授权。
    # 少了这一步，任何已认证用户只要拿到 queryId 就能取回他人数据的下载链接。
    vin_match = re.search(r"/exports/([A-HJ-NPR-Z0-9]{17})/", destination)
    if not vin_match:
        raise ApiError(403, "Unknown export job")
    vin = vin_match.group(1)
    authorize(event, vin)

    state = execution["Status"]["State"]
    if state != "SUCCEEDED":
        return {
            "queryId": query_id,
            "status": state,
            "reason": execution["Status"].get("StateChangeReason"),
        }

    # 压缩方式同样从原始语句里还原，让调用方知道下载到的是什么。
    # 语句里没有 compression 选项时：PARQUET 走 Athena 默认的 gzip，其余为 none。
    compression = "gzip" if "PARQUET" in execution.get("Query", "") else "none"
    comp_match = re.search(r"compression\s*=\s*'([^']+)'", execution.get("Query", ""))
    if comp_match:
        compression = comp_match.group(1)

    prefix = destination.replace(f"s3://{EXPORTS_BUCKET}/", "")
    # 必须分页：list_objects_v2 单次最多返回 1000 个 key 且不自动续页。UNLOAD 是
    # 并行写多文件的，大车辆的全量导出很容易超过这个数。少给链接却仍返回
    # SUCCEEDED，等于给出不完整的数据副本而调用方无法察觉——对数据可携带权
    # （GDPR Art.20 / Data Act Art.4）来说这是正确性问题，不只是性能问题。
    paginator = s3.get_paginator("list_objects_v2")
    urls = [
        s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": EXPORTS_BUCKET, "Key": obj["Key"]},
            ExpiresIn=PRESIGN_TTL_SECONDS,
        )
        for page in paginator.paginate(Bucket=EXPORTS_BUCKET, Prefix=prefix)
        for obj in page.get("Contents", [])
        if obj["Size"] > 0
    ]
    return {
        "queryId": query_id,
        "status": "SUCCEEDED",
        "fileCount": len(urls),
        "compression": compression,
        "expiresInSeconds": PRESIGN_TTL_SECONDS,
        "downloadUrls": urls,
    }


def start_erasure(vin: str, grant: dict) -> dict:
    """启动物理擦除。

    走 Glue 作业而非 Athena DELETE：表属性为 copy-on-write，Spark 的 DELETE
    会立即重写数据文件（不产生 delete file），随后立刻过期 snapshot，
    从而消除 Time Travel 回溯路径 —— 这才是 GDPR 意义上的"擦除"。
    """
    try:
        run_id = glue.start_job_run(
            JobName=ERASURE_JOB,
            Arguments={"--action": "erasure", "--vins": vin},
        )["JobRunId"]
    except glue.exceptions.ConcurrentRunsExceededException:
        # 不能让擦除请求以 500 静默消失：这是有法定时限的权利请求，
        # 必须明确告知调用方稍后重试。
        log.warning("Erasure for %s rejected: concurrent run limit reached", mask_vin(vin))
        raise ApiError(
            429,
            "Maintenance job is at its concurrency limit; retry the erasure request shortly",
        )

    # 审计留痕：擦除请求本身也是必须可追溯的处理活动（GDPR Art.30）
    ddb.update_item(
        Key={"user_id": grant["_user_id"], "vin": vin},
        UpdateExpression="SET erasure_requested_at = :t, erasure_job_run_id = :r",
        ExpressionAttributeValues={":t": int(time.time()), ":r": run_id},
    )

    return {
        "status": "ERASURE_STARTED",
        "vin": vin,
        "jobRunId": run_id,
        "mode": "copy-on-write physical delete + immediate snapshot expiry",
        "poll": f"GET /erasures/{run_id}",
    }


def get_erasure(event: dict) -> dict:
    run_id = (event.get("pathParameters") or {}).get("jobRunId", "")
    if not re.match(r"^jr_[0-9a-f]{16,128}$", run_id):
        raise ApiError(400, "Invalid jobRunId")
    try:
        run = glue.get_job_run(JobName=ERASURE_JOB, RunId=run_id)["JobRun"]
    except glue.exceptions.EntityNotFoundException:
        raise ApiError(404, "Unknown erasure job")

    args = run.get("Arguments") or {}
    if args.get("--action") != "erasure":
        # 该端点只暴露擦除作业，不能被用来窥探其它维护作业的运行情况
        raise ApiError(403, "Unknown erasure job")

    # 归属校验：作业参数里带着目标 VIN，据此再走一遍授权
    vin = (args.get("--vins") or "").strip().upper()
    if not VIN_RE.match(vin):
        raise ApiError(403, "Unknown erasure job")
    authorize(event, vin)

    return {
        "jobRunId": run_id,
        "status": run["JobRunState"],
        "startedOn": run.get("StartedOn").isoformat() if run.get("StartedOn") else None,
        "error": run.get("ErrorMessage"),
    }


def create_share(event: dict, vin: str, grant: dict) -> dict:
    """把数据访问权授予第三方（保险公司 / 维修商）。"""
    body = parse_body(event)
    third_party = str(body.get("third_party_id", "")).strip()
    if not THIRD_PARTY_RE.match(third_party):
        raise ApiError(400, "Invalid third_party_id")

    signals = body.get("allowed_signals") or []
    if not isinstance(signals, list) or not signals:
        raise ApiError(400, "allowed_signals must be a non-empty list")
    for s in signals:
        if not SIGNAL_RE.match(str(s)):
            raise ApiError(400, f"Invalid signal name: {s!r}")

    try:
        days = int(body.get("duration_days", 90))
    except (TypeError, ValueError):
        raise ApiError(400, "Invalid duration_days")
    if not 1 <= days <= 3650:
        raise ApiError(400, "duration_days must be between 1 and 3650")

    now = int(time.time())
    expires_at = now + days * 86400
    ddb.put_item(
        Item={
            "user_id": third_party,
            "vin": vin,
            # 显式标记为第三方：该记录不得获得删除与再授权能力
            "role": ROLE_THIRD_PARTY,
            "granted_by": grant["_user_id"],
            "third_party_id": third_party,
            "allowed_signals": set(str(s) for s in signals),
            "granted_at": now,
            # DynamoDB TTL 自动清理过期授权；读取时仍会二次校验
            "expires_at": expires_at,
        }
    )
    return {
        "status": "GRANTED",
        "vin": vin,
        "third_party_id": third_party,
        "allowed_signals": sorted(str(s) for s in signals),
        "expires_at": expires_at,
    }


# --------------------------------------------------------------------------- #
# 路由
# --------------------------------------------------------------------------- #
def route(event: dict) -> dict:
    method = event.get("httpMethod", "")
    resource = event.get("resource", "")
    path_params = event.get("pathParameters") or {}

    if resource == "/exports/{queryId}" and method == "GET":
        return get_export(event)
    if resource == "/erasures/{jobRunId}" and method == "GET":
        return get_erasure(event)

    vin = require_vin(path_params)
    grant = authorize(event, vin)

    if resource == "/vehicles/{vin}/telemetry" and method == "GET":
        return get_telemetry(event, vin, grant)
    if resource == "/vehicles/{vin}/signals/{signalName}" and method == "GET":
        return get_telemetry(event, vin, grant, signal=path_params.get("signalName", ""))
    if resource == "/vehicles/{vin}/export" and method == "POST":
        return start_export(event, vin, grant)
    if resource == "/vehicles/{vin}/share" and method == "POST":
        # 只有车主能对外授权，否则被授权的第三方可以给自己扩大信号范围提权
        require_owner(grant, "Granting third-party access")
        return create_share(event, vin, grant)
    if resource == "/vehicles/{vin}" and method == "DELETE":
        # 删除不可恢复，只有车主本人能发起
        require_owner(grant, "Erasure")
        return start_erasure(vin, grant)

    raise ApiError(404, f"No route for {method} {resource}")


def _response(status: int, payload: dict) -> dict:
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Cache-Control": "no-store",
            "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
            "X-Content-Type-Options": "nosniff",
        },
        "body": json.dumps(payload, default=str),
    }


def handler(event, context):
    try:
        result = route(event)
        status = 202 if result.get("status") in ("EXPORTING", "ERASURE_STARTED") else 200
        return _response(status, result)
    except ApiError as exc:
        return _response(exc.status, {"error": exc.message})
    except Exception:  # noqa: BLE001
        # 不把内部异常细节回给调用方，仅在日志中保留完整堆栈
        log.exception("Unhandled error")
        return _response(500, {"error": "Internal server error"})
