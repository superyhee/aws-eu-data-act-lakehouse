"""CloudFormation 自定义资源：启动 / 停止 Managed Flink 应用。

为什么不用 AwsCustomResource 直接调 API
--------------------------------------
StartApplication 与 StopApplication 都受应用当前状态约束：

  - Force stop 仅在 STARTING / UPDATING / STOPPING / AUTOSCALING / RUNNING 时合法。
    对 READY 状态强制停止会报 "You can't force stop an application in 'READY' status"，
    栈回滚时这个错误会让自定义资源 DELETE_FAILED，反过来卡住整个回滚。
  - 两个 API 都是异步的：返回成功只代表请求被接受。若不等待状态收敛，
    CloudFormation 可能在应用仍处于 STOPPING 时就去删除它。

靠猜错误码去 ignore 是不可靠的，因此这里改为读状态再决定动作，
并配合 Provider Framework 的 isComplete 等待状态收敛。

协议：on_event 发起动作，is_complete 轮询直到收敛。
"""
import logging
import os

import boto3

log = logging.getLogger()
log.setLevel(logging.INFO)

kda = boto3.client("kinesisanalyticsv2")

APP_NAME = os.environ["APPLICATION_NAME"]

# 可以（且需要）执行强制停止的状态
STOPPABLE = {"STARTING", "UPDATING", "RUNNING", "AUTOSCALING"}
# 已经停下来的状态
STOPPED = {"READY", "STOPPED"}
# 过渡状态，需要继续等待
TRANSIENT = {"STARTING", "STOPPING", "UPDATING", "AUTOSCALING", "DELETING", "ROLLING_BACK"}


def _status() -> str | None:
    """返回应用状态；应用不存在时返回 None。"""
    try:
        return kda.describe_application(ApplicationName=APP_NAME)["ApplicationDetail"][
            "ApplicationStatus"
        ]
    except kda.exceptions.ResourceNotFoundException:
        return None


def on_event(event, context):
    request_type = event["RequestType"]
    # 删除时必须原样回传已有的 PhysicalResourceId，否则 Provider Framework 报
    # "cannot change the physical resource ID during deletion"，
    # 自定义资源 DELETE_FAILED 会卡住整个栈回滚。
    # CREATE 被取消时物理 ID 由 CloudFormation 生成，不能按本地规则重拼。
    physical_id = event.get("PhysicalResourceId") or f"flink-control-{APP_NAME}"
    status = _status()
    log.info("RequestType=%s current status=%s", request_type, status)

    if request_type == "Delete":
        if status is None:
            log.info("Application already gone, nothing to stop")
        elif status in STOPPABLE:
            # Force=True 跳过快照，回滚/销毁场景不需要保留状态
            log.info("Force stopping application from status %s", status)
            kda.stop_application(ApplicationName=APP_NAME, Force=True)
        else:
            log.info("Status %s needs no stop action", status)
        return {"PhysicalResourceId": physical_id}

    if status is None:
        raise RuntimeError(f"Application {APP_NAME} not found; cannot start it")

    if status in STOPPED:
        # 首次创建时没有快照可恢复；更新时优先从最近快照恢复以保住状态
        restore_type = (
            "SKIP_RESTORE_FROM_SNAPSHOT"
            if request_type == "Create"
            else "RESTORE_FROM_LATEST_SNAPSHOT"
        )
        log.info("Starting application with restore type %s", restore_type)
        try:
            kda.start_application(
                ApplicationName=APP_NAME,
                RunConfiguration={
                    "ApplicationRestoreConfiguration": {"ApplicationRestoreType": restore_type},
                    "FlinkRunConfiguration": {"AllowNonRestoredState": True},
                },
            )
        except kda.exceptions.InvalidArgumentException:
            # 更新路径上可能确实还没有任何快照，退回到跳过恢复
            if restore_type == "SKIP_RESTORE_FROM_SNAPSHOT":
                raise
            log.warning("No snapshot available, retrying with SKIP_RESTORE_FROM_SNAPSHOT")
            kda.start_application(
                ApplicationName=APP_NAME,
                RunConfiguration={
                    "ApplicationRestoreConfiguration": {
                        "ApplicationRestoreType": "SKIP_RESTORE_FROM_SNAPSHOT"
                    },
                    "FlinkRunConfiguration": {"AllowNonRestoredState": True},
                },
            )
    else:
        log.info("Status %s already starting or running, no action needed", status)

    return {"PhysicalResourceId": physical_id}


def is_complete(event, context):
    status = _status()
    request_type = event["RequestType"]
    log.info("Polling RequestType=%s status=%s", request_type, status)

    if request_type == "Delete":
        # 必须等到应用真正停下来，否则 CloudFormation 会在 STOPPING 中途去删它
        done = status is None or status in STOPPED
        return {"IsComplete": done}

    if status == "RUNNING":
        return {"IsComplete": True, "Data": {"ApplicationStatus": status}}
    if status in TRANSIENT:
        return {"IsComplete": False}

    # READY 说明启动没生效，FORCE_STOPPING/失败等状态也应尽早暴露，
    # 而不是让 CloudFormation 一直等到超时。
    raise RuntimeError(
        f"Application {APP_NAME} settled in unexpected status {status!r} instead of RUNNING. "
        f"Check the application log group named in the stack output 'FlinkFlinkLogGroup*' "
        f"(it is auto-named and retained across rollbacks), or run: "
        f"aws kinesisanalyticsv2 describe-application --application-name {APP_NAME} "
        f"--query ApplicationDetail.CloudWatchLoggingOptionDescriptions"
    )
