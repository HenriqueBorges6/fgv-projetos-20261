# --- Glue Data Catalog (consultável no Athena) ---

resource "aws_glue_catalog_database" "analytics" {
  name = var.glue_catalog_db
}

# Tabela externa fact_orders com particionamento por order_year/order_month.
# Usa PARTITION PROJECTION: o Athena enxerga as partições sem crawler nem MSCK REPAIR.
resource "aws_glue_catalog_table" "fact_orders" {
  name          = "fact_orders"
  database_name = aws_glue_catalog_database.analytics.name
  table_type    = "EXTERNAL_TABLE"

  # Colunas de partição (na ordem do caminho Hive)
  partition_keys {
    name = "order_year"
    type = "int"
  }
  partition_keys {
    name = "order_month"
    type = "int"
  }

  parameters = {
    classification        = "parquet"
    "parquet.compression" = "SNAPPY"
    EXTERNAL              = "TRUE"

    # Partition projection — partições visíveis ao Athena automaticamente
    "projection.enabled"            = "true"
    "projection.order_year.type"    = "integer"
    "projection.order_year.range"   = "2003,2027"
    "projection.order_month.type"   = "integer"
    "projection.order_month.range"  = "1,12"
    "storage.location.template"     = "s3://${aws_s3_bucket.data_lake.id}/analytics/fact_orders/order_year=$${order_year}/order_month=$${order_month}"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data_lake.id}/analytics/fact_orders/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    # Colunas de dados (NÃO incluir as de partição aqui). Tipos batem com o
    # cast explícito feito no glue_etl_incremental.py para evitar mismatch no Athena.
    columns {
      name = "order_id"
      type = "int"
    }
    columns {
      name = "customer_id"
      type = "int"
    }
    columns {
      name = "product_id"
      type = "string"
    }
    columns {
      name = "order_date_key"
      type = "int"
    }
    columns {
      name = "country_key"
      type = "int"
    }
    columns {
      name = "quantity_ordered"
      type = "int"
    }
    columns {
      name = "price_each"
      type = "double"
    }
    columns {
      name = "sales_amount"
      type = "double"
    }
  }
}
