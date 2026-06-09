"""
Lambda mínima disparada pelo EventBridge para iniciar o job Glue incremental.

Regras clássicas do EventBridge não invocam um Glue Job diretamente, então usamos
EventBridge Rule -> Lambda -> glue.start_job_run (a Lambda tem glue:StartJobRun via LabRole).
"""
import os
import boto3


def handler(event, context):
    glue = boto3.client("glue")
    job_name = os.environ["GLUE_JOB_NAME"]
    run_id = glue.start_job_run(JobName=job_name)["JobRunId"]
    print(f"[EventBridge] Started Glue job '{job_name}', JobRunId={run_id}")
    return {"JobName": job_name, "JobRunId": run_id}
