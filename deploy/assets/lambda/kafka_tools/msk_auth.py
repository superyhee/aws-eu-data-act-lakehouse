"""MSK Serverless IAM 认证的 Kafka 客户端工具。

MSK Serverless 仅支持 IAM 认证（不支持 SASL/SCRAM 与 mTLS），
认证方式为 SASL_SSL + OAUTHBEARER，token 由 aws-msk-iam-sasl-signer 生成。
"""
import os
import time

from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

# kafka-python 3.x 会用 isinstance() 校验 token provider 的类型：
#   "sasl_oauth_token_provider must implement
#    kafka.net.sasl.oauth.AbstractTokenProvider"
# 也就是说 2.x 时代仅提供 token() 方法的鸭子类型写法已不再被接受，必须继承基类。
# 该导入路径与 kafka-python 3.x 绑定（requirements.txt 已固定版本）。
from kafka.net.sasl.oauth import AbstractTokenProvider

REGION = os.environ.get("AWS_REGION", "eu-central-1")

# token 到期前多久提前刷新
REFRESH_MARGIN_MS = 60_000


class MskTokenProvider(AbstractTokenProvider):
    """按需生成并缓存 MSK IAM 认证 token。

    基类文档明确要求实现方复用 token：AdminClient / Producer 在连接多个 broker
    时会反复调用 token()，每次都重新签名既慢也无必要。
    """

    def __init__(self, region: str = REGION):
        super().__init__()
        self._region = region
        self._token: str | None = None
        self._expiry_ms: int = 0

    def token(self) -> str:
        now_ms = int(time.time() * 1000)
        if self._token is None or now_ms >= self._expiry_ms - REFRESH_MARGIN_MS:
            self._token, self._expiry_ms = MSKAuthTokenProvider.generate_auth_token(self._region)
        return self._token


def client_kwargs(bootstrap_servers: str) -> dict:
    """两类客户端（AdminClient 与 KafkaProducer）都接受的配置。

    ⚠️ 只放两端都合法的键：kafka-python 会对未知配置项直接抛
    KafkaConfigurationError("Unrecognized configs")，而不是忽略。
    kafka-python 3.x 已移除 api_version_auto_timeout_ms，
    对应能力由 bootstrap_timeout_ms 承担。
    """
    return {
        "bootstrap_servers": bootstrap_servers.split(","),
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "OAUTHBEARER",
        "sasl_oauth_token_provider": MskTokenProvider(),
        "client_id": "iov-lakehouse-tools",
        # MSK Serverless 冷启动时元数据拉取可能偏慢
        "bootstrap_timeout_ms": 60_000,
        "request_timeout_ms": 60_000,
    }
