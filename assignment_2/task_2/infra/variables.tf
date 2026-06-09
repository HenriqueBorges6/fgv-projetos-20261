variable "aws_region" {
  default = "us-east-1"
}

variable "aws_access_key_id" {
  sensitive = true
}

variable "aws_secret_access_key" {
  sensitive = true
}

variable "aws_session_token" {
  sensitive = true
}

# --- RDS ---

variable "rds_instance_id" {
  description = "Identificador único da instância RDS"
  default     = "classicmodels-grupo3-a2t2"
}

variable "rds_admin_user" {
  default = "admin"
}

variable "rds_admin_password" {
  description = "Senha do usuário admin do RDS (mínimo 8 caracteres)"
  sensitive   = true
}

# --- S3 / Glue / Catálogo ---

variable "s3_bucket_name" {
  description = "Nome globalmente único para o bucket S3 do data lake"
  default     = "classicmodels-datalake-grupo3-a2t2"
}

variable "glue_catalog_db" {
  description = "Nome do database no Glue Data Catalog (para Athena)"
  default     = "classicmodels_analytics"
}

variable "glue_schedule_cron" {
  description = "Expressão cron do EventBridge (UTC) para disparar o job Glue"
  default     = "cron(0 12 ? * MON *)"
}

variable "pipeline_name" {
  description = "pipeline_name no etl_watermark"
  default     = "classicmodels_sales"
}

variable "manage_s3_endpoint" {
  description = "Criar o S3 VPC gateway endpoint. Deixe false se a VPC já tem um (ex.: A1)."
  type        = bool
  default     = true
}
