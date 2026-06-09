# Evidências de execução (Task 2)

Coloque aqui as evidências exigidas pelo brief (3.4.2 / 3.4.3). Sugestão de conteúdo:

- `cycle1.txt` — saída de `pipeline.py` (1ª execução = full load).
- `cycle2.txt` — saída do 2º ciclo incremental (`pipeline.py --skip-load --skip-init`),
  mostrando que **apenas** pedidos com `orderDate > watermark anterior` foram extraídos e que
  o número de linhas novas em `fact_orders` é coerente com os pedidos simulados.
- `eventbridge_run.txt` — **Job Run ID** do disparo via EventBridge (3.4.3). Como obter:

  ```powershell
  # após habilitar/forçar a regra, liste os runs e identifique o disparado pela automação
  aws glue get-job-runs --job-name classicmodels-incremental-etl-job `
    --query "JobRuns[].{Id:Id,Trigger:TriggeredBy,State:JobRunState,Started:StartedOn}" --output table
  ```

- `athena_query.txt` — print/resultado de uma consulta particionada no Athena, ex.:

  ```sql
  SELECT order_year, order_month, COUNT(*) AS linhas, SUM(sales_amount) AS receita
  FROM classicmodels_analytics.fact_orders
  WHERE order_year = 2026
  GROUP BY order_year, order_month
  ORDER BY order_month;
  ```
