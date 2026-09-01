"""向 MSK 灌入模拟车联网遥测数据，用于端到端验证部署是否打通。

手动调用：
  aws lambda invoke --function-name <SampleProducerFunctionName> \
      --payload '{"vehicles":50,"messages":5000}' out.json

消息格式必须与 Flink 源表 DDL 一致（见 assets/flink/src/main/resources/sql/pipeline.sql）：
event_time 为 ISO-8601，因为源表配置了 json.timestamp-format.standard = ISO-8601。
"""
import datetime
import json
import logging
import os
import random
import string

from kafka import KafkaProducer

from msk_auth import client_kwargs

log = logging.getLogger()
log.setLevel(logging.INFO)

BOOTSTRAP = os.environ["BOOTSTRAP_SERVERS"]
TOPIC = os.environ["TOPIC_NAME"]
DEFAULT_VEHICLES = int(os.environ.get("SAMPLE_VEHICLES", "50"))
DEFAULT_MESSAGES = int(os.environ.get("SAMPLE_MESSAGES", "5000"))

VIN_ALPHABET = "ABCDEFGHJKLMNPRSTUVWXYZ0123456789"  # ISO 3779：排除 I/O/Q
SIGNALS = ("vehicle_speed", "battery_soc", "motor_temp", "odometer")


#: 合成 VIN 的固定前缀。刻意【不】使用任何真实厂商的 WMI（VIN 前三位是
#: 世界制造厂识别代号，由各国机构分配给具体车企）。借用真实 WMI 生成示例数据，
#: 意味着这些字符串带着某家车企的标识，且理论上可能与真实车辆碰撞——而 VIN
#: 本身就是本方案要保护的个人数据。前缀取 SAMPLEVEH0：长度 10，字符全部落在
#: ISO 3779 允许的字母表内（不含 I/O/Q），拼上 7 位数字正好 17 位，
#: 既能通过本方案的 VIN 格式校验，又一眼可辨为合成数据。
VIN_PREFIX = "SAMPLEVEH0"


def _make_vins(count: int) -> list[str]:
    rnd = random.Random(42)  # 固定种子，便于反复验证时命中同一批 VIN
    return [
        VIN_PREFIX + "".join(rnd.choice(string.digits) for _ in range(7))
        for _ in range(count)
    ]


def handler(event, context):
    vehicles = int(event.get("vehicles", DEFAULT_VEHICLES))
    total = int(event.get("messages", DEFAULT_MESSAGES))
    vins = _make_vins(vehicles)

    producer = KafkaProducer(
        **client_kwargs(BOOTSTRAP),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        linger_ms=200,
        acks="all",
    )

    now = datetime.datetime.now(datetime.timezone.utc)
    sent = 0
    try:
        for i in range(total):
            vin = vins[i % vehicles]
            ts = now - datetime.timedelta(milliseconds=(total - i) * 10)
            record = {
                "vin": vin,
                # 不带时区偏移的 ISO-8601，对应 Iceberg timestamp (without zone)
                "event_time": ts.strftime("%Y-%m-%dT%H:%M:%S.%f"),
                "signal_name": random.choice(SIGNALS),
                "signal_value": round(random.uniform(0, 240), 3),
                "latitude": round(random.uniform(47.3, 54.9), 6),
                "longitude": round(random.uniform(5.9, 15.0), 6),
                "speed": round(random.uniform(0, 180), 1),
                "battery_soc": round(random.uniform(5, 100), 1),
            }
            # 以 VIN 作为 partition key，保证同一辆车的消息有序
            producer.send(TOPIC, key=vin, value=record)
            sent += 1
        producer.flush(timeout=60)
    finally:
        producer.close(timeout=30)

    log.info("Produced %s messages for %s vehicles to %s", sent, vehicles, TOPIC)
    return {
        "topic": TOPIC,
        "vehicles": vehicles,
        "messagesSent": sent,
        "sampleVins": vins[:5],
    }
