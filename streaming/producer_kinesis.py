#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

import boto3


COUNTRY_REGIONS = {
    "BR": ["SUL", "SUDESTE", "CENTRO-OESTE", "NORDESTE", "NORTE"],
    "US": ["EAST", "WEST", "MIDWEST", "SOUTH"],
    "UK": ["LONDON", "NORTH", "MIDLANDS", "SCOTLAND"],
    "AR": ["BUENOS_AIRES", "CORDOBA", "SANTA_FE"],
}

CITIES = {
    "BR": ["Sao Paulo", "Rio de Janeiro", "Florianopolis", "Curitiba", "Recife", "Manaus"],
    "US": ["New York", "San Francisco", "Miami", "Chicago", "Seattle"],
    "UK": ["London", "Manchester", "Birmingham", "Edinburgh"],
    "AR": ["Buenos Aires", "Cordoba", "Rosario"],
}

PAYMENT_METHODS = ["CREDIT_CARD", "DEBIT_CARD", "PIX", "BOLETO", "APPLE_PAY", "GOOGLE_PAY"]
CHANNELS = ["ECOMMERCE", "POS", "MOBILE_APP", "WEB"]
CURRENCIES = {"BR": "BRL", "US": "USD", "UK": "GBP", "AR": "ARS"}


def parse_args():
    p = argparse.ArgumentParser(description="Kinesis Fraud Events Producer")
    # Mantido por compatibilidade, mas por padrão usa o profile default (sem --profile)
    p.add_argument("--profile", default=None, help="AWS CLI profile name (optional)")
    p.add_argument("--region", default="us-east-1", help="AWS region (default us-east-1)")
    p.add_argument("--stream-name", default="fraud-events-stream", help="Kinesis stream name")
    p.add_argument("--eps", type=int, default=5, help="Events per second (default 5)")
    # Mantido por compatibilidade; com a lógica abaixo os eventos SEMPRE caem no dia atual (UTC)
    p.add_argument("--backfill-days", type=int, default=60, help="(Ignored) kept for compatibility")
    p.add_argument("--sleep", type=float, default=1.0, help="Seconds to sleep between batches (default 1.0)")
    p.add_argument("--dry-run", action="store_true", help="Print events but do not send to Kinesis")
    return p.parse_args()


def build_boto3_session(profile, region: str) -> boto3.Session:
    if profile:
        return boto3.Session(profile_name=profile, region_name=region)
    return boto3.Session(region_name=region)


def event_datetime_today_utc() -> datetime:
    """
    Garante que os eventos sejam SEMPRE do dia de execução (UTC),
    para o Firehose particionar em year/month/day do dia atual.
    """
    now = datetime.now(timezone.utc)
    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)

    # jitter dentro do mesmo dia (0..86399s)
    dt = start_of_day + timedelta(seconds=random.randint(0, 86399))

    # evita "futuro" quando agora é perto da meia-noite
    if dt > now:
        dt = now
    return dt


def build_event() -> dict:
    dt = event_datetime_today_utc()  # sempre no dia atual (UTC)

    country = random.choice(list(COUNTRY_REGIONS.keys()))
    region = random.choice(COUNTRY_REGIONS[country])
    city = random.choice(CITIES[country])
    currency = CURRENCIES[country]

    amount = round(random.uniform(5, 5000), 2)
    risk_score = round(random.random(), 4)

    status = "DECLINED" if risk_score > 0.75 else "APPROVED"
    is_international = 1 if country != "BR" else 0

    return {
        "event_id": str(uuid.uuid4()),
        "transaction_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1, 50000)}",
        "merchant_id": f"m_{random.randint(1, 5000)}",
        "amount": amount,
        "currency": currency,
        "payment_method": random.choice(PAYMENT_METHODS),
        "channel": random.choice(CHANNELS),
        "country": country,
        "region": region,
        "city": city,
        "device_id": f"dev_{random.randint(1, 200000)}",
        "ip_address": f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}",
        "risk_score": risk_score,
        "is_international": is_international,
        "status": status,

        # ✅ campos exigidos pelo Firehose Dynamic Partitioning (JQ)
        "event_ts": dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "event_year": dt.strftime("%Y"),
        "event_month": dt.strftime("%m"),
        "event_day": dt.strftime("%d"),

        # opcional: formato humano
        "event_time": dt.strftime("%Y-%m-%d %H:%M:%S"),
    }


def main():
    args = parse_args()

    session = build_boto3_session(args.profile, args.region)
    kinesis = session.client("kinesis")

    print(
        "[producer] "
        f"profile={args.profile or 'default'} "
        f"region={args.region} "
        f"stream={args.stream_name} "
        f"eps={args.eps} "
        f"sleep={args.sleep} "
        f"mode=today_utc_only"
    )

    while True:
        records = []
        for _ in range(args.eps):
            evt = build_event()
            data = (json.dumps(evt, ensure_ascii=False) + "\n").encode("utf-8")
            records.append({"Data": data, "PartitionKey": evt["user_id"]})

        if args.dry_run:
            print(records[0]["Data"].decode("utf-8").strip())
            time.sleep(args.sleep)
            continue

        resp = kinesis.put_records(StreamName=args.stream_name, Records=records)
        failed = resp.get("FailedRecordCount", 0)

        # Se houver falhas, mostra um resumo para depurar
        if failed:
            # Mostra até 3 erros
            errors = []
            for r in resp.get("Records", [])[:]:
                if "ErrorCode" in r:
                    errors.append(f"{r.get('ErrorCode')}:{r.get('ErrorMessage')}")
                if len(errors) >= 3:
                    break
            print(f"Sent={len(records)} failed={failed} errors={errors}")
        else:
            print(f"Sent={len(records)} failed=0")

        time.sleep(args.sleep)


if __name__ == "__main__":
    main()