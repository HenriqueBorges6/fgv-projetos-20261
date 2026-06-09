# --- S3 data lake ---

resource "aws_s3_bucket" "data_lake" {
  bucket        = var.s3_bucket_name
  force_destroy = true

  tags = { Name = var.s3_bucket_name }
}

# Upload do script do job Glue incremental.
# O caminho usa path.module (relativo ao repo), nunca um path local de um integrante (3.1.1).
resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/glue_etl_incremental.py"
  source = "${path.module}/../scripts/glue_etl_incremental.py"
  etag   = filemd5("${path.module}/../scripts/glue_etl_incremental.py")
}
