"""DAG проверки качества данных витрины labeled_transactions.

Запускается по расписанию, выполняет 5+ правил DQ и публикует HTML-отчёт
в MODELS_DIR/dq_reports/dq_report.html (рядом с моделями).
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task

from pipeline.dq.checks import run_dq_and_render


@dag(
    dag_id="data_quality_check",
    description="Проверки Data Quality витрины labeled_transactions с HTML-отчётом",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["dq", "monitoring"],
    doc_md="""
### DQ витрины labeled_transactions

Выполняет 5+ правил качества данных и сохраняет HTML-отчёт в
`MODELS_DIR/dq_reports/dq_report.html` — рядом с каталогом моделей.

Если хотя бы одно правило не пройдено, задача падает с `ValueError`,
но отчёт всё равно создаётся.
    """,
)
def data_quality_check():
    task(run_dq_and_render, task_id="run_dq_checks")()


data_quality_check()
