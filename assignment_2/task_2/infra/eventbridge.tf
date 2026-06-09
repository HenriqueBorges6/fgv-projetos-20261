# --- Agendamento: EventBridge Rule -> Lambda -> Glue StartJobRun ---
#
# Regras clássicas do EventBridge não têm o Glue Job como target nativo, então a
# regra dispara uma Lambda mínima que chama glue.start_job_run (3.1.2).
# A Lambda usa a LabRole (Academy não permite criar roles), que já possui
# glue:StartJobRun. Documentado no README.

# Empacota o código da Lambda em zip
data "archive_file" "trigger_glue" {
  type        = "zip"
  source_file = "${path.module}/lambda/trigger_glue.py"
  output_path = "${path.module}/lambda/trigger_glue.zip"
}

resource "aws_lambda_function" "trigger_glue" {
  function_name    = "classicmodels-a2t2-trigger-glue"
  role             = data.aws_iam_role.lab_role.arn
  handler          = "trigger_glue.handler"
  runtime          = "python3.12"
  filename         = data.archive_file.trigger_glue.output_path
  source_code_hash = data.archive_file.trigger_glue.output_base64sha256
  timeout          = 30

  environment {
    variables = {
      GLUE_JOB_NAME = aws_glue_job.etl.name
    }
  }
}

# Regra cron (UTC). Default do brief: cron(0 12 ? * MON *) — semanal, segunda meio-dia.
resource "aws_cloudwatch_event_rule" "schedule" {
  name                = "classicmodels-a2t2-schedule"
  description         = "Dispara o ETL incremental do classicmodels"
  schedule_expression = var.glue_schedule_cron
}

resource "aws_cloudwatch_event_target" "glue_via_lambda" {
  rule      = aws_cloudwatch_event_rule.schedule.name
  target_id = "trigger-glue-lambda"
  arn       = aws_lambda_function.trigger_glue.arn
}

# Permite o EventBridge invocar a Lambda
resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.trigger_glue.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.schedule.arn
}
