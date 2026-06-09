# Assignment 2 — Task 2: ETL incremental, partições e agendamento

Evolui o pipeline do Assignment 1 para processamento **incremental**: o job Glue lê o **watermark**
(`etl_watermark`, da Task 1), extrai via JDBC **apenas o delta** (`orderDate > watermark`), grava
`fact_orders` **particionado** por `order_year`/`order_month` no S3 e no Glue Catalog (consultável no
Athena) e atualiza o watermark só em sucesso. Execuções são **agendadas** via EventBridge → Lambda →
Glue, tudo em Terraform.

**Autocontido**: `assignment_2/task_2/infra/` provisiona o stack completo (RDS + S3 + Glue +
Catalog + EventBridge). Aplique **só a task_2** — ela é superset da task_1.

> **VPC limpa (importante).** A task_2 cria o próprio S3 VPC gateway endpoint na VPC default.
> Esse endpoint é um recurso **compartilhado a nível de VPC** — só pode existir um por serviço.
> Se houver outra stack viva na mesma VPC já com um endpoint S3 (ex.: o **A1** ou a **task_1**),
> o apply falha com `RouteAlreadyExists`. Para rodar a task_2 isolada, **destrua as outras stacks
> primeiro** (`terraform destroy` nas pastas delas) — o código permanece no git.
> Alternativa: setar `manage_s3_endpoint = false` no `terraform.tfvars` para reusar o endpoint já
> existente na VPC (nesse caso a task_2 deixa de ser totalmente independente).

## Estrutura

```
assignment_2/task_2/
├── infra/                       # Terraform — stack completo
│   ├── main.tf                  # provider, VPC, subnet glue, route tables, S3 VPC endpoint
│   ├── variables.tf  rds.tf  s3.tf  glue.tf  catalog.tf  eventbridge.tf  outputs.tf
│   ├── lambda/trigger_glue.py   # Lambda que faz glue.start_job_run (alvo do EventBridge)
│   ├── terraform.tfvars.example  .gitignore
├── scripts/
│   ├── db.py load_classicmodels.py init_watermark.py   # origem (reuso da Task 1)
│   ├── simulate_new_orders.py   # datas que AVANÇAM a cada run (ver "Notas")
│   ├── glue_etl_incremental.py  # job Glue: watermark, delta, merge particionado
│   ├── run_glue_job.py          # dispara o job e imprime o Job Run ID
│   ├── validate_incremental_etl.py # valida run, partições, watermark, sales_amount
│   ├── pipeline.py              # orquestra um ciclo (load→init→simulate→glue→validate)
│   ├── .env                     # credenciais AWS + senha RDS (NÃO commitado)
│   └── pipeline_info.json       # gerado pelo terraform apply (NÃO commitado)
├── evidence/                    # saídas das execuções (3.4.2 / 3.4.3)
├── .env.example
└── README.md
```

## Pré-requisitos

- Terraform + credenciais AWS (Learner Lab/STS).
- Python 3 com `pymysql`, `python-dotenv`, `boto3`, `pyarrow`, `pandas`.

## Passo a passo

### 1. Provisionar (Terraform)

```powershell
cd assignment_2\task_2\infra
copy terraform.tfvars.example terraform.tfvars   # edite com credenciais AWS + senha
terraform init
terraform apply                                   # cria stack e grava scripts/pipeline_info.json
```

### 2. Configurar credenciais dos scripts

```powershell
cd ..\scripts
copy ..\.env.example .env        # edite: credenciais AWS (boto3) + RDS_ADMIN_PASSWORD (= tfvars)
```

`.env`, `terraform.tfvars`, `pipeline_info.json` e o tfstate estão no `.gitignore`.

### 3. Primeiro ciclo (cold start → FULL load)

```powershell
python pipeline.py --count 5 --seed 42
```

Carrega o banco, inicializa o watermark (`NEVER_RUN`), simula 5 pedidos, roda o Glue (full load:
processa todo o histórico + delta) e valida. Ao final, watermark = `MAX(orderDate)` e status
`SUCCEEDED`. Salve a saída em `evidence/cycle1.txt`.

### 4. Segundo ciclo (INCREMENTAL — evidência principal)

```powershell
python pipeline.py --skip-load --skip-init --count 4 --seed 7
```

Simula mais pedidos (datas avançam além do watermark), roda o Glue em modo incremental (extrai só
`orderDate > watermark`) e valida. Salve em `evidence/cycle2.txt` — é a evidência de que **apenas o
delta** foi processado (3.4.2).

### 5. Consulta particionada no Athena

```sql
SELECT order_year, order_month, COUNT(*) linhas, SUM(sales_amount) receita
FROM classicmodels_analytics.fact_orders
WHERE order_year = 2026
GROUP BY order_year, order_month
ORDER BY order_month;
```

As partições aparecem sem crawler/MSCK (partition projection — ver abaixo).

### 6. Disparo agendado via EventBridge (3.4.3)

A regra `classicmodels-a2t2-schedule` dispara semanalmente (`cron(0 12 ? * MON *)`, UTC) a Lambda
`classicmodels-a2t2-trigger-glue`, que chama `glue:StartJobRun`. Para evidenciar sem esperar a
semana, invoque a Lambda manualmente uma vez e capture o Job Run ID:

```powershell
aws lambda invoke --function-name classicmodels-a2t2-trigger-glue out.json; type out.json
aws glue get-job-runs --job-name classicmodels-incremental-etl-job `
  --query "JobRuns[0].{Id:Id,Trigger:TriggeredBy,State:JobRunState}" --output table
```

Registre o `Id` em `evidence/eventbridge_run.txt`.

### 7. Destruir ao terminar

```powershell
cd ..\infra
terraform destroy
```

## Decisões de arquitetura

- **Cold start (full vs incremental).** Após a Task 1, `last_run_status='NEVER_RUN'` e watermark =
  2005-05-31. No 1º run o job faz **full load** (ignora o filtro) para não perder o histórico
  2003–2005; runs seguintes filtram `orderDate > watermark`.
- **Merge particionado da `fact_orders`.** Deltas de runs diferentes podem cair na mesma partição
  (`order_year=2026/order_month=06`). O job lê a partição afetada, faz `union` com o delta,
  `dropDuplicates((order_id, product_id))` preferindo o delta, e regrava só essas partições com
  `spark.sql.sources.partitionOverwriteMode=dynamic` — não apaga as demais.
- **Catálogo via partition projection.** A tabela `fact_orders` (Terraform, `catalog.tf`) declara
  `order_year`/`order_month` como partition keys com `projection.enabled=true`
  (`order_year` 2003–2027, `order_month` 1–12). O Athena enxerga as partições **na hora**, sem
  crawler nem `MSCK REPAIR`. As partições no S3 usam inteiro sem zero à esquerda
  (`order_month=6`), coerente com o tipo `int`.
- **EventBridge → Lambda → Glue.** Regras clássicas do EventBridge não têm Glue Job como target
  nativo, então `aws_cloudwatch_event_rule` + `aws_cloudwatch_event_target` apontam para uma Lambda
  mínima (`lambda/trigger_glue.py`) que chama `glue.start_job_run`.
- **IAM (3.1.2).** Todos os componentes (Glue Job e Lambda) usam a **`LabRole`** pré-existente do
  AWS Academy (o lab não permite criar roles). A `LabRole` já concede `glue:StartJobRun`. Fallback se
  o IAM bloquear o agendamento: disparar manualmente com `scripts/run_glue_job.py` (boto3).
- **Datas do `simulate`.** Aqui o `simulate_new_orders.py` gera datas **estritamente após o
  `MAX(orderDate)` atual** (diferente da Task 1, que usava "dias úteis até hoje"). Isso garante delta
  não-vazio a cada ciclo, mesmo rodando 2+ vezes seguidas — requisito da Task 2.
- **Segredos.** `terraform.tfvars`, `.env`, `pipeline_info.json`, tfstate e o `.zip` da Lambda
  ficam fora do git.

## Critérios da Task 2 (mapa)

| Critério | Onde |
|----------|------|
| Glue incremental via JDBC + filtro watermark | `scripts/glue_etl_incremental.py` |
| `fact_orders` particionado (S3 + catálogo) | `glue_etl_incremental.py` + `infra/catalog.tf` |
| Watermark atualizado só após sucesso | `glue_etl_incremental.py` (`update_watermark`) |
| EventBridge + target em Terraform | `infra/eventbridge.tf` |
| Schema do A1 + `sales_amount = qty * price` | `build_star()` (mesmas colunas/regra) |
| Sem extração via CSV local | extração 100% via Spark JDBC |
