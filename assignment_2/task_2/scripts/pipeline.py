"""
Orquestra um ciclo do ETL incremental (Task 2), APÓS o `terraform apply`.

Ciclo completo (1ª vez):
  1. load_classicmodels        → carrega o dump no RDS
  2. init_watermark            → cria/inicializa etl_watermark (NEVER_RUN)
  3. simulate_new_orders       → insere o delta de pedidos
  4. run_glue_job              → roda o Glue (full no 1º run; incremental depois)
  5. validate_incremental_etl  → valida run, partições, watermark, sales_amount

Ciclos seguintes (incremental): use --skip-load --skip-init para repetir 3–5.

Uso:
  python pipeline.py --count 5 --seed 42                 # ciclo completo (cold start = full)
  python pipeline.py --skip-load --skip-init --count 4   # 2º ciclo (incremental)
"""
import argparse
import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).parent
PYTHON = sys.executable


def run(name, argv):
    print(f"\n{'='*60}\n>> {name}\n{'='*60}")
    result = subprocess.run([PYTHON, *argv], cwd=BASE_DIR)
    if result.returncode != 0:
        print(f"\n[PARADO] Etapa '{name}' falhou (exit {result.returncode}).")
        sys.exit(result.returncode)


def main():
    p = argparse.ArgumentParser(description="Orquestra um ciclo do ETL incremental (Task 2).")
    p.add_argument("--skip-load", action="store_true", help="Pula a carga do classicmodels.")
    p.add_argument("--skip-init", action="store_true", help="Pula a inicialização do watermark.")
    p.add_argument("--count", type=int, default=5, help="Pedidos a simular (default 5).")
    p.add_argument("--seed", type=int, default=None, help="Seed da simulação (opcional).")
    args = p.parse_args()

    if not args.skip_load:
        run("Carga do classicmodels", ["load_classicmodels.py"])
    if not args.skip_init:
        run("Inicialização do watermark", ["init_watermark.py"])

    sim = ["simulate_new_orders.py", "--count", str(args.count)]
    if args.seed is not None:
        sim += ["--seed", str(args.seed)]
    run("Simulação do delta", sim)

    run("Glue ETL incremental", ["run_glue_job.py"])
    run("Validação do ETL incremental", ["validate_incremental_etl.py"])

    print(f"\n{'='*60}\nCiclo do ETL incremental concluído com sucesso.\n{'='*60}")


if __name__ == "__main__":
    main()
