# --- IAM Role ---
# O AWS Academy não permite criar roles via API. Usa a LabRole pré-existente.
data "aws_iam_role" "lab_role" {
  name = "LabRole"
}

# --- Security Group do Glue ---

resource "aws_security_group" "glue" {
  name        = "glue-classicmodels-a2t2-sg"
  description = "Glue ETL job security group (A2 Task 2)"
  vpc_id      = data.aws_vpc.default.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "glue-classicmodels-a2t2-sg" }
}

# Regra self-referencing obrigatória pelo Glue para comunicação interna
resource "aws_security_group_rule" "glue_self" {
  type              = "ingress"
  from_port         = 0
  to_port           = 65535
  protocol          = "tcp"
  self              = true
  security_group_id = aws_security_group.glue.id
}

# --- Glue Connection para o RDS (extração via JDBC) ---

resource "aws_glue_connection" "rds" {
  name            = "classicmodels-rds-a2t2-connection"
  connection_type = "JDBC"

  connection_properties = {
    JDBC_CONNECTION_URL = "jdbc:mysql://${aws_db_instance.classicmodels.address}:${aws_db_instance.classicmodels.port}/classicmodels?useSSL=false&allowPublicKeyRetrieval=true"
    USERNAME            = var.rds_admin_user
    PASSWORD            = var.rds_admin_password
    JDBC_ENFORCE_SSL    = "false"
  }

  physical_connection_requirements {
    availability_zone      = data.aws_subnet.glue.availability_zone
    subnet_id              = data.aws_subnet.glue.id
    security_group_id_list = [aws_security_group.glue.id]
  }
}

# --- Glue Job incremental ---

resource "aws_glue_job" "etl" {
  name     = "classicmodels-incremental-etl-job"
  role_arn = data.aws_iam_role.lab_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.data_lake.id}/scripts/glue_etl_incremental.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"                          = "s3://${aws_s3_bucket.data_lake.id}/tmp/"
    # pymysql para ler/atualizar o etl_watermark (Spark JDBC não faz UPDATE de 1 linha)
    "--additional-python-modules" = "pymysql"
    "--RDS_ENDPOINT"              = aws_db_instance.classicmodels.address
    "--RDS_PORT"                  = tostring(aws_db_instance.classicmodels.port)
    "--RDS_USERNAME"             = var.rds_admin_user
    "--RDS_PASSWORD"             = var.rds_admin_password
    "--RDS_DB_NAME"              = "classicmodels"
    "--PIPELINE_NAME"           = var.pipeline_name
    "--S3_ANALYTICS_PATH"       = "s3://${aws_s3_bucket.data_lake.id}/analytics/"
  }

  connections = [aws_glue_connection.rds.name]

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 30

  tags = { Name = "classicmodels-incremental-etl-job" }
}
