"""
Glue Job — ETL incremental do classicmodels para o star schema particionado.

Fluxo (Task 2):
  1. Lê etl_watermark (pymysql) para pipeline_name -> last_processed_order_date, last_run_status.
  2. Decide o modo:
       - status == 'NEVER_RUN' (ou data nula): FULL LOAD (sem filtro) — evita perder o histórico
         do A1 no primeiro run incremental.
       - caso contrário: INCREMENTAL — extrai apenas orders com orderDate > watermark.
  3. Extrai tabelas via Spark JDBC (dims sempre completas; orders/orderdetails filtradas no modo
     incremental).
  4. Constrói o star schema do A1 (dim_*, fact_orders) + colunas de partição order_year/order_month.
  5. Grava dims (overwrite) e fact_orders particionado em s3://.../analytics/.
       - fact full: grava todas as partições.
       - fact incremental: para cada (order_year, order_month) do delta, lê a partição existente,
         faz union + dropDuplicates((order_id, product_id)) preferindo o delta, e regrava só essas
         partições (partitionOverwriteMode=dynamic).
  6. Só em sucesso lógico: atualiza watermark (MAX(orderDate) processado, last_run_at, SUCCEEDED).
     Em falha: marca FAILED sem avançar a data e relança a exceção.
"""
import sys
from datetime import datetime

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import pymysql

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "RDS_ENDPOINT",
    "RDS_PORT",
    "RDS_USERNAME",
    "RDS_PASSWORD",
    "RDS_DB_NAME",
    "PIPELINE_NAME",
    "S3_ANALYTICS_PATH",
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Overwrite só das partições tocadas (não apaga as demais)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

ANALYTICS = args["S3_ANALYTICS_PATH"].rstrip("/") + "/"
FACT_PATH = f"{ANALYTICS}fact_orders/"
PIPELINE_NAME = args["PIPELINE_NAME"]

jdbc_url = (
    f"jdbc:mysql://{args['RDS_ENDPOINT']}:{args['RDS_PORT']}/{args['RDS_DB_NAME']}"
    "?useSSL=false&allowPublicKeyRetrieval=true"
)
jdbc_props = {
    "user": args["RDS_USERNAME"],
    "password": args["RDS_PASSWORD"],
    "driver": "com.mysql.cj.jdbc.Driver",
}


# --- Watermark via pymysql ---

def mysql_connect():
    return pymysql.connect(
        host=args["RDS_ENDPOINT"],
        port=int(args["RDS_PORT"]),
        user=args["RDS_USERNAME"],
        password=args["RDS_PASSWORD"],
        database=args["RDS_DB_NAME"],
        connect_timeout=10,
    )


def read_watermark():
    conn = mysql_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT last_processed_order_date, last_run_status "
                "FROM etl_watermark WHERE pipeline_name = %s",
                (PIPELINE_NAME,),
            )
            row = cur.fetchone()
        return row  # (date|None, status|None) ou None
    finally:
        conn.close()


def update_watermark(processed_date, status):
    """Atualiza o watermark. processed_date pode ser None (não avança a data)."""
    conn = mysql_connect()
    try:
        with conn.cursor() as cur:
            if processed_date is not None:
                cur.execute(
                    "UPDATE etl_watermark SET last_processed_order_date = %s, "
                    "last_run_at = %s, last_run_status = %s WHERE pipeline_name = %s",
                    (processed_date, datetime.utcnow(), status, PIPELINE_NAME),
                )
            else:
                cur.execute(
                    "UPDATE etl_watermark SET last_run_at = %s, last_run_status = %s "
                    "WHERE pipeline_name = %s",
                    (datetime.utcnow(), status, PIPELINE_NAME),
                )
        conn.commit()
    finally:
        conn.close()


# --- Extract ---

def read_table(name):
    print(f"[Extract] {name}")
    return spark.read.jdbc(url=jdbc_url, table=name, properties=jdbc_props)


def read_orders(watermark, full):
    if full:
        print("[Extract] orders (FULL)")
        return spark.read.jdbc(url=jdbc_url, table="orders", properties=jdbc_props)
    # INCREMENTAL: filtra no banco (pushdown via subquery)
    print(f"[Extract] orders (INCREMENTAL: orderDate > {watermark})")
    subq = f"(SELECT * FROM orders WHERE orderDate > '{watermark}') AS o"
    return spark.read.jdbc(url=jdbc_url, table=subq, properties=jdbc_props)


# --- Transform (reaproveita a lógica do A1) ---

def build_star(orders, orderdetails, customers, products, productlines, offices):
    dim_customers = customers.select(
        F.col("customerNumber").alias("customer_id"),
        F.col("customerName").alias("customer_name"),
        F.concat_ws(" ", F.col("contactLastName"), F.col("contactFirstName")).alias("contact_name"),
        F.col("city"),
        F.col("country"),
    )

    dim_products = products.join(productlines, "productLine", "left").select(
        F.col("productCode").alias("product_id"),
        F.col("productName").alias("product_name"),
        F.col("productLine").alias("product_line"),
        F.col("productVendor").alias("product_vendor"),
    )

    dim_dates = (
        orders.select(F.col("orderDate").alias("full_date")).distinct()
        .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("day", F.dayofmonth("full_date"))
        .select("date_key", "full_date", "year", "quarter", "month", "day")
    )

    country_territory = offices.select("country", "territory").distinct()
    dim_countries = (
        customers.select("country").distinct()
        .join(country_territory, "country", "left")
        .withColumn("country_key", F.row_number().over(Window.orderBy("country")))
        .select("country_key", "country", "territory")
    )

    orders_enriched = (
        orders
        .join(customers.select("customerNumber", "country"), "customerNumber")
        .join(dim_countries.select("country_key", "country"), "country")
        .join(dim_dates.select("date_key", F.col("full_date").alias("orderDate")), "orderDate")
    )

    fact_orders = orderdetails.join(orders_enriched, "orderNumber").select(
        F.col("orderNumber").cast("int").alias("order_id"),
        F.col("customerNumber").cast("int").alias("customer_id"),
        F.col("productCode").cast("string").alias("product_id"),
        F.col("date_key").cast("int").alias("order_date_key"),
        F.col("country_key").cast("int").alias("country_key"),
        F.col("quantityOrdered").cast("int").alias("quantity_ordered"),
        F.col("priceEach").cast("double").alias("price_each"),
        (F.col("quantityOrdered") * F.col("priceEach")).cast("double").alias("sales_amount"),
        # colunas de partição derivadas do orderDate
        F.year("orderDate").cast("int").alias("order_year"),
        F.month("orderDate").cast("int").alias("order_month"),
    )

    return {
        "dim_customers": dim_customers,
        "dim_products": dim_products,
        "dim_dates": dim_dates,
        "dim_countries": dim_countries,
        "fact_orders": fact_orders,
    }


# --- Load ---

def write_dim(df, name):
    path = f"{ANALYTICS}{name}/"
    print(f"[Load] dim {name} -> {path}")
    df.write.mode("overwrite").parquet(path)


def write_fact(fact, full):
    if full:
        print(f"[Load] fact_orders FULL -> {FACT_PATH}")
        (fact.write.mode("overwrite")
             .partitionBy("order_year", "order_month")
             .parquet(FACT_PATH))
        return

    # INCREMENTAL: merge nas partições afetadas
    affected = [(r["order_year"], r["order_month"])
                for r in fact.select("order_year", "order_month").distinct().collect()]
    if not affected:
        print("[Load] Delta vazio — nada a gravar em fact_orders.")
        return

    print(f"[Load] fact_orders INCREMENTAL — partições afetadas: {affected}")

    try:
        existing = spark.read.parquet(FACT_PATH)
    except Exception:
        existing = None  # primeira vez que a fato é escrita

    if existing is not None:
        cond = None
        for (y, m) in affected:
            c = (F.col("order_year") == y) & (F.col("order_month") == m)
            cond = c if cond is None else (cond | c)
        existing_affected = existing.where(cond).withColumn("_src", F.lit(0))
        delta = fact.withColumn("_src", F.lit(1))
        union = existing_affected.unionByName(delta)
        # mantém a linha do delta em caso de chave duplicada (order_id, product_id)
        w = Window.partitionBy("order_id", "product_id").orderBy(F.col("_src").desc())
        merged = (union.withColumn("_rn", F.row_number().over(w))
                       .where(F.col("_rn") == 1)
                       .drop("_src", "_rn"))
    else:
        merged = fact

    (merged.write.mode("overwrite")
           .partitionBy("order_year", "order_month")
           .parquet(FACT_PATH))


# --- Main ---

def main():
    wm = read_watermark()
    if wm is None:
        raise RuntimeError(
            f"Watermark '{PIPELINE_NAME}' não encontrado. Rode init_watermark.py (Task 1)."
        )
    watermark, status = wm[0], wm[1]
    full = (status == "NEVER_RUN") or (watermark is None)
    print(f"[Watermark] last_processed_order_date={watermark} status={status} -> "
          f"modo={'FULL' if full else 'INCREMENTAL'}")

    try:
        orders = read_orders(watermark, full)

        # Max(orderDate) do conjunto processado (None se delta vazio)
        max_row = orders.agg(F.max("orderDate").alias("m")).collect()[0]
        max_processed = max_row["m"]

        if not full and max_processed is None:
            print("[Incremental] Sem pedidos novos. Watermark inalterado.")
            update_watermark(None, "SUCCEEDED")
            job.commit()
            return

        # orderdetails: no incremental, só as linhas dos pedidos do delta
        orderdetails = read_table("orderdetails")
        if not full:
            orderdetails = orderdetails.join(
                orders.select("orderNumber").distinct(), "orderNumber"
            )

        # dims sempre completas (Opção A do brief)
        customers = read_table("customers")
        products = read_table("products")
        productlines = read_table("productlines")
        offices = read_table("offices")

        star = build_star(orders, orderdetails, customers, products, productlines, offices)

        # dims: overwrite completo
        write_dim(star["dim_customers"], "dim_customers")
        write_dim(star["dim_products"], "dim_products")
        write_dim(star["dim_dates"], "dim_dates")
        write_dim(star["dim_countries"], "dim_countries")

        # fact: full ou merge incremental
        write_fact(star["fact_orders"], full)

        # Sucesso: avança watermark (nunca retrocede)
        new_wm = max_processed
        if watermark is not None and max_processed is not None and max_processed < watermark:
            new_wm = watermark
        update_watermark(new_wm, "SUCCEEDED")
        print(f"[Watermark] atualizado -> {new_wm} (SUCCEEDED)")

        job.commit()
        print("[Done] ETL incremental concluído.")

    except Exception as exc:
        print(f"[ERRO] Falha no ETL: {exc}")
        try:
            update_watermark(None, "FAILED")
        except Exception as wm_exc:
            print(f"[ERRO] Falha ao marcar FAILED: {wm_exc}")
        raise


main()
