output "rds_endpoint" {
  value = aws_db_instance.classicmodels.address
}

output "rds_port" {
  value = aws_db_instance.classicmodels.port
}

output "s3_bucket_name" {
  value = aws_s3_bucket.data_lake.id
}

output "glue_job_name" {
  value = aws_glue_job.etl.name
}

output "glue_catalog_db" {
  value = aws_glue_catalog_database.analytics.name
}

output "eventbridge_rule" {
  value = aws_cloudwatch_event_rule.schedule.name
}

# Gera pipeline_info.json diretamente em scripts/ para os scripts Python lerem.
resource "local_file" "pipeline_info" {
  filename = "${path.module}/../scripts/pipeline_info.json"
  content = jsonencode({
    rds_endpoint    = aws_db_instance.classicmodels.address
    rds_port        = aws_db_instance.classicmodels.port
    rds_db_name     = "classicmodels"
    rds_admin_user  = var.rds_admin_user
    s3_bucket_name  = aws_s3_bucket.data_lake.id
    glue_job_name   = aws_glue_job.etl.name
    glue_catalog_db = aws_glue_catalog_database.analytics.name
    analytics_path  = "analytics/"
  })
}
