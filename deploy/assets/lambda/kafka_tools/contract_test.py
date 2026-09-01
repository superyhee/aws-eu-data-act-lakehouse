"""MSK IAM 客户端配置的契约测试（需要 kafka-python 与 signer 已安装）。

这类缺陷无法靠语法检查发现，只能靠对真实依赖做类型/配置校验：
  - kafka-python 3.x 用 isinstance() 校验 sasl_oauth_token_provider 的基类，
    2.x 的鸭子类型写法会在连接时才报错
  - 未知配置项会抛 KafkaConfigurationError，而不是被忽略

在容器内运行：
  docker run --rm -v "$PWD/assets/lambda/kafka_tools":/w -w /w python:3.12-slim \
    bash -c 'pip install -q -r requirements.txt && AWS_REGION=eu-central-1 python contract_test.py'
"""
import os
import sys

os.environ.setdefault("AWS_REGION", "eu-central-1")

import msk_auth  # noqa: E402
from kafka import KafkaProducer  # noqa: E402
from kafka.admin import KafkaAdminClient  # noqa: E402
from kafka.net.sasl.oauth import AbstractTokenProvider, SaslMechanismOAuth  # noqa: E402

failures = []


def check(label: str, ok: bool, detail: str = "") -> None:
    print(f"{'PASS' if ok else 'FAIL'}  {label}{(' -> ' + detail) if detail else ''}")
    if not ok:
        failures.append(label)


kwargs = msk_auth.client_kwargs("b-1.example.com:9098")
provider = kwargs["sasl_oauth_token_provider"]

check("token provider 继承 AbstractTokenProvider", isinstance(provider, AbstractTokenProvider))
check("实现了 token()", callable(getattr(provider, "token", None)))
check("继承到 extensions()", provider.extensions() == {})

# 这正是真实部署中失败的那一步类型校验
try:
    SaslMechanismOAuth(**kwargs)
    check("SaslMechanismOAuth 构造成功", True)
except Exception as exc:  # noqa: BLE001
    check("SaslMechanismOAuth 构造成功", False, f"{type(exc).__name__}: {exc}")

# 未知配置项会让客户端在构造时直接抛错
for name, cls in (("KafkaAdminClient", KafkaAdminClient), ("KafkaProducer", KafkaProducer)):
    unknown = sorted(set(kwargs) - set(cls.DEFAULT_CONFIG))
    check(f"{name} 接受全部配置项", not unknown, f"未识别: {unknown}")

print()
if failures:
    print(f"{len(failures)} 项契约校验失败")
    sys.exit(1)
print("全部契约校验通过")
