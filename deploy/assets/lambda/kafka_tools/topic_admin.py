"""CloudFormation 自定义资源：在 MSK Serverless 上创建 / 更新 Kafka topic。

MSK Serverless 没有托管的 topic 管理 API（MSK 的 topic API 仅支持 Provisioned），
且 broker 配置不可修改，所以不能依赖 auto-create（自动创建只会给 1 个分区）。
这里用 Kafka AdminClient 显式建 topic，保证分区数确定。

支持的操作：
  Create/Update  -> 建 topic；已存在则按需增加分区数、更新保留时长
  Delete         -> 默认【不删除】topic（防止误删数据）
"""
import logging
import os
import time

from kafka.admin import ConfigResource, ConfigResourceType, KafkaAdminClient, NewPartitions, NewTopic
from kafka.errors import TopicAlreadyExistsError

from msk_auth import client_kwargs

log = logging.getLogger()
log.setLevel(logging.INFO)

BOOTSTRAP = os.environ["BOOTSTRAP_SERVERS"]
CONNECT_RETRIES = 10
CONNECT_BACKOFF_SECONDS = 15
# 总重试预算必须明显小于 Lambda 超时（10 分钟），否则失败会表现为
# "Task timed out after 600.00 seconds"，把真正的根因（认证配置错误、
# 网络不通等）完全掩盖，排查时只能去翻日志。
CONNECT_BUDGET_SECONDS = 420


def _admin() -> KafkaAdminClient:
    """MSK Serverless 刚创建时端点可能还未就绪，带退避重试。"""
    deadline = time.monotonic() + CONNECT_BUDGET_SECONDS
    last_error = None
    for attempt in range(1, CONNECT_RETRIES + 1):
        try:
            return KafkaAdminClient(**client_kwargs(BOOTSTRAP))
        except Exception as exc:  # noqa: BLE001 - 连接期任何异常都值得重试
            last_error = exc
            remaining = deadline - time.monotonic()
            log.warning(
                "AdminClient connect attempt %s/%s failed (%s): %s",
                attempt, CONNECT_RETRIES, type(exc).__name__, exc,
            )
            if remaining <= CONNECT_BACKOFF_SECONDS:
                break
            time.sleep(CONNECT_BACKOFF_SECONDS)
    raise RuntimeError(
        f"Cannot connect to MSK within {CONNECT_BUDGET_SECONDS}s. "
        f"Last error: {type(last_error).__name__}: {last_error}"
    )


def _ensure_topic(admin: KafkaAdminClient, name: str, partitions: int, retention_hours: int) -> str:
    retention_ms = str(int(retention_hours) * 3600 * 1000)
    # MSK Serverless 的副本因子由平台托管，必须传 -1 交由服务端决定
    topic = NewTopic(
        name=name,
        num_partitions=partitions,
        replication_factor=-1,
        topic_configs={"retention.ms": retention_ms},
    )
    try:
        admin.create_topics([topic])
        log.info("Created topic %s with %s partitions", name, partitions)
        return "created"
    except TopicAlreadyExistsError:
        log.info("Topic %s already exists, reconciling", name)

    existing = admin.describe_topics([name])[0]
    current_partitions = len(existing["partitions"])
    if partitions > current_partitions:
        # Kafka 只允许增加分区，不能减少
        admin.create_partitions({name: NewPartitions(total_count=partitions)})
        log.info("Increased partitions of %s: %s -> %s", name, current_partitions, partitions)
    elif partitions < current_partitions:
        log.warning(
            "Requested %s partitions but topic %s already has %s; Kafka cannot shrink partitions. Keeping %s.",
            partitions, name, current_partitions, current_partitions,
        )

    admin.alter_configs(
        [ConfigResource(ConfigResourceType.TOPIC, name, {"retention.ms": retention_ms})]
    )
    return "updated"


def handler(event, context):
    request_type = event["RequestType"]
    props = event["ResourceProperties"]
    topic_name = props["TopicName"]
    partitions = int(props["Partitions"])
    retention_hours = int(props["RetentionHours"])

    # 删除时【必须】原样回传已有的 PhysicalResourceId。
    # 若返回一个不同的 ID，Provider Framework 会直接报
    # "cannot change the physical resource ID during deletion"，
    # 该自定义资源随即 DELETE_FAILED 并卡住整个栈回滚。
    # 注意 CREATE 被取消的情况：此时物理 ID 是 CloudFormation 自动生成的，
    # 不是本函数返回过的值，所以不能凭本地规则重新拼。
    if request_type == "Delete":
        # 有意不删 topic：栈删除不应导致流量数据丢失。
        # 如需清理，请手动执行 kafka-topics.sh --delete。
        log.info("Delete requested for %s - intentionally skipped (data safety)", topic_name)
        return {
            "PhysicalResourceId": event["PhysicalResourceId"],
            "Data": {"Action": "skipped"},
        }

    physical_id = event.get("PhysicalResourceId") or f"msk-topic-{topic_name}"

    admin = _admin()
    try:
        action = _ensure_topic(admin, topic_name, partitions, retention_hours)
    finally:
        admin.close()

    return {
        "PhysicalResourceId": physical_id,
        "Data": {"TopicName": topic_name, "Partitions": str(partitions), "Action": action},
    }
