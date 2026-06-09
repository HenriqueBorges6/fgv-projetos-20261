"""
Valida o resultado do ETL incremental (Task 2, seção 5):

  1. Último run do Glue: SUCCEEDED.
  2. Objetos Parquet sob analytics/fact_orders/order_year=…/order_month=…/ (Hive-partitioned).
  3. etl_watermark avançou: last_processed_order_date == MAX(orders.orderDate) e status SUCCEEDED.
  4. fact_orders tem linhas e sales_amount == quantity_ordered * price_each.
  5. Existe partição para o ano dos pedidos mais recentes (coerência do delta).

Exit code 0 = aprovado, 1 = reprovado.

Uso:
  python validate_incremental_etl.py
"""
import io
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from db import load_config, connect_with_retry

BASE_DIR = Path(__file__).parent
load_dotenv(BASE_DIR / ".env")

import boto3

info_path = BASE_DIR / "pipeline_info.json"
if not info_path.exists():
    print("[ERRO] pipeline_info.json não encontrado. Execute 'terraform apply' primeiro.")
    sys.exit(1)

info = json.loads(info_path.read_text())
S3_BUCKET = info["s3_bucket_name"]
GLUE_JOB_NAME = info["glue_job_name"]
ANALYTICS_PREFIX = info.get("analytics_path", "analytics/")
FACT_PREFIX = f"{ANALYTICS_PREFIX}fact_orders/"

session = boto3.Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    aws_session_token=os.environ.get("AWS_SESSION_TOKEN"),
    region_name=os.environ.get("AWS_REGION", "us-east-1"),
)
glue = session.client("glue")
s3 = session.client("s3")

PASS = "[PASS]"
FAIL = "[FAIL]"
errors = []


def check(condition, msg):
    print(f"  {PASS if condition else FAIL} {msg}")
    if not condition:
        errors.append(msg)


def list_parquet_keys(prefix):
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
    return keys


def read_fact():
    import pyarrow as pa
    import pyarrow.parquet as pq

    keys = list_parquet_keys(FACT_PREFIX)
    if not keys:
        return None
    parts = []
    for key in keys:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        parts.append(pq.read_table(io.BytesIO(obj["Body"].read())))
    return pa.concat_tables(parts, promote_options="default").to_pandas()


# --- 1. Status do Glue ---
print("\n=== 1. Status do Glue Job ===")
runs = glue.get_job_runs(JobName=GLUE_JOB_NAME, MaxResults=1).get("JobRuns", [])
if not runs:
    check(False, f"Nenhum run encontrado para '{GLUE_JOB_NAME}'")
else:
    check(runs[0]["JobRunState"] == "SUCCEEDED", f"Último run: {runs[0]['JobRunState']}")

# --- 2. Partições Hive no S3 ---
print("\n=== 2. Partições de fact_orders no S3 ===")
fact_keys = list_parquet_keys(FACT_PREFIX)
check(len(fact_keys) > 0, f"{len(fact_keys)} arquivo(s) sob {FACT_PREFIX}")
has_year = any("order_year=" in k for k in fact_keys)
has_month = any("order_month=" in k for k in fact_keys)
check(has_year and has_month, "Layout Hive order_year=…/order_month=… presente")

# --- 3. Watermark avançou (via banco) ---
print("\n=== 3. Watermark ===")
max_order_date = None
conn = None
try:
    conn = connect_with_retry(load_config())
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(orderDate) FROM orders")
        max_order_date = cur.fetchone()[0]
        cur.execute(
            "SELECT last_processed_order_date, last_run_status "
            "FROM etl_watermark WHERE pipeline_name = %s",
            (info.get("pipeline_name", "classicmodels_sales"),),
        )
        row = cur.fetchone()
    if row is None:
        check(False, "Registro de watermark não encontrado")
    else:
        wm_date, wm_status = row
        check(wm_date is not None, f"last_processed_order_date não nulo (={wm_date})")
        check(wm_status == "SUCCEEDED", f"last_run_status = {wm_status}")
        check(str(wm_date) == str(max_order_date),
              f"watermark ({wm_date}) == MAX(orderDate) ({max_order_date}) — alcançou o delta")
finally:
    if conn:
        conn.close()

# --- 4 & 5. Conteúdo da fact_orders ---
print("\n=== 4 & 5. Conteúdo da fact_orders ===")
try:
    import pyarrow  # noqa — dispara ImportError cedo

    fact = read_fact()
    check(fact is not None and len(fact) > 0,
          f"fact_orders tem {0 if fact is None else len(fact)} linhas")

    if fact is not None and len(fact) > 0:
        required = {
            "order_id", "customer_id", "product_id",
            "order_date_key", "country_key",
            "quantity_ordered", "price_each", "sales_amount",
        }
        check(required.issubset(set(fact.columns)), "Colunas obrigatórias presentes")

        if required.issubset(set(fact.columns)):
            expected = (fact["quantity_ordered"] * fact["price_each"]).round(2)
            mismatches = (expected != fact["sales_amount"].round(2)).sum()
            check(mismatches == 0,
                  f"sales_amount == quantity_ordered * price_each ({mismatches} divergências)")

        # Coerência: partição do ano mais recente existe
        if max_order_date is not None:
            yr = max_order_date.year
            check(any(f"order_year={yr}" in k for k in fact_keys),
                  f"Existe partição order_year={yr} (ano do delta mais recente)")

except ImportError:
    print("  [WARN] pyarrow não instalado — pulando validação de conteúdo")
    print("         Execute: pip install pyarrow pandas")

# --- Resumo ---
print("\n" + "=" * 55)
if errors:
    print(f"REPROVADO — {len(errors)} verificação(ões) falharam:")
    for e in errors:
        print(f"  x {e}")
    sys.exit(1)
print("APROVADO — todas as verificações passaram.")
sys.exit(0)
