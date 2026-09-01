"""CloudFormation 自定义资源：启动一次 Glue Job 并等待其完成。

用于在部署过程中同步完成 Iceberg 建表——Flink 应用必须在表存在之后才能启动。

采用 CDK Provider Framework 的异步协议：
  on_event    启动 job run，返回 JobRunId
  is_complete 轮询 job run 状态，直到 SUCCEEDED / FAILED
"""
import logging

import boto3

log = logging.getLogger()
log.setLevel(logging.INFO)

glue = boto3.client("glue")

TERMINAL_OK = {"SUCCEEDED"}
TERMINAL_FAILED = {"FAILED", "TIMEOUT", "STOPPED", "ERROR"}


def on_event(event, context):
    request_type = event["RequestType"]
    props = event["ResourceProperties"]
    job_name = props["JobName"]

    if request_type == "Delete":
        # 删除栈时不需要跑任何 job（表本身按 RemovalPolicy 处理）
        return {"PhysicalResourceId": event.get("PhysicalResourceId", f"glue-run-{job_name}")}

    arguments = props.get("Arguments") or {}
    # CloudFormation 会把所有属性值转成字符串，这里统一规整
    arguments = {str(k): str(v) for k, v in arguments.items()}

    log.info("Starting Glue job %s with arguments %s", job_name, arguments)
    resp = glue.start_job_run(JobName=job_name, Arguments=arguments)
    run_id = resp["JobRunId"]

    return {
        # 与 Delete 分支保持一致：已有物理 ID 时原样沿用，避免被判定为资源替换
        "PhysicalResourceId": event.get("PhysicalResourceId") or f"glue-run-{job_name}",
        "Data": {"JobRunId": run_id, "JobName": job_name},
    }


def is_complete(event, context):
    if event["RequestType"] == "Delete":
        return {"IsComplete": True}

    data = event.get("Data") or {}
    run_id = data.get("JobRunId")
    job_name = data.get("JobName") or event["ResourceProperties"]["JobName"]
    if not run_id:
        raise RuntimeError("Missing JobRunId from on_event response")

    state = glue.get_job_run(JobName=job_name, RunId=run_id)["JobRun"]
    status = state["JobRunState"]
    log.info("Glue job %s run %s state=%s", job_name, run_id, status)

    if status in TERMINAL_OK:
        return {"IsComplete": True, "Data": {"JobRunId": run_id, "State": status}}
    if status in TERMINAL_FAILED:
        # 把 Glue 的错误信息抛给 CloudFormation，避免只看到一句超时
        raise RuntimeError(
            f"Glue job {job_name} run {run_id} ended in {status}: "
            f"{state.get('ErrorMessage', 'no error message')}"
        )

    return {"IsComplete": False}
