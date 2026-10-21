"""Data Quality проверки витрины labeled_transactions + HTML-отчёт."""

import logging
from pathlib import Path

import pandas as pd
from jinja2 import Template

from pipeline.db import get_conn
from pipeline.settings import MODELS_DIR

logger = logging.getLogger(__name__)


DQ_RULES = [
    (
        "merch не пустой",
        lambda df: df["merch"].notna().all(),
    ),
    (
        "cat_id не пустой",
        lambda df: df["cat_id"].notna().all(),
    ),
    (
        "amount в диапазоне [0, 1_000_000]",
        lambda df: df["amount"].between(0, 1_000_000).all(),
    ),
    (
        "target (fraud_flag) только 0 или 1",
        lambda df: df["target"].isin([0, 1]).all(),
    ),
    (
        "в витрине не менее 50 000 строк",
        lambda df: len(df) >= 50_000,
    ),
    (
        "нет пропусков в координатах клиента (lat, lon)",
        lambda df: df[["lat", "lon"]].notna().all().all(),
    ),
    (
        "transaction_time не в будущем",
        lambda df: (
            pd.to_datetime(df["transaction_time"]).max()
            <= pd.Timestamp.utcnow().tz_localize(None)
        ),
    ),
]


def _render_html(results: list[dict], rows: int) -> str:
    """Рендерит HTML-таблицу с результатами проверок."""
    html = Template("""
<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <title>DQ Report</title>
  <style>
    body { font-family: -apple-system, sans-serif; margin: 24px; }
    h1 { color: #222; }
    table { border-collapse: collapse; margin-top: 12px; }
    th, td { border: 1px solid #ccc; padding: 8px 12px; text-align: left; }
    th { background: #f3f3f3; }
    .pass { color: #0a7d28; font-weight: bold; }
    .fail { color: #b71c1c; font-weight: bold; }
    .summary { font-size: 1.1em; margin-top: 12px; }
  </style>
</head>
<body>
  <h1>Data Quality Report</h1>
  <p>Витрина: <code>labeled_transactions</code>. Всего строк: <b>{{ rows }}</b>.</p>
  <table>
    <tr><th>#</th><th>Правило</th><th>Статус</th></tr>
    {% for r in results %}
    <tr>
      <td>{{ loop.index }}</td>
      <td>{{ r.rule }}</td>
      <td class="{{ 'pass' if r.passed else 'fail' }}">
        {{ 'PASS' if r.passed else 'FAIL' }}
      </td>
    </tr>
    {% endfor %}
  </table>
  <p class="summary">
    Итог: {{ results | selectattr('passed') | list | length }} / {{ results | length }} правил пройдено.
  </p>
</body>
</html>
""")
    return html.render(results=results, rows=rows)


def run_dq_and_render() -> str:
    """Выполняет DQ-правила и сохраняет HTML-отчёт рядом с моделями.

    Читает: таблицу labeled_transactions из PostgreSQL.
    Пишет: MODELS_DIR/dq_reports/dq_report.html — перезаписывается при каждом
    запуске, поэтому повторный запуск не портит данные (только освежает отчёт).
    Возвращает путь к отчёту (для XCom).

    Если какое-то правило не пройдено, задача падает с ValueError:
    отчёт всё равно создаётся, чтобы можно было разобраться, что не так.
    """
    conn = get_conn()
    try:
        df = pd.read_sql("SELECT * FROM labeled_transactions", conn)
    finally:
        conn.close()

    results = []
    for name, check in DQ_RULES:
        try:
            passed = bool(check(df))
        except Exception:
            logger.exception("Ошибка в правиле: %s", name)
            passed = False
        results.append({"rule": name, "passed": passed})
        logger.info("DQ: %s — %s", name, "PASS" if passed else "FAIL")

    out_dir = Path(MODELS_DIR) / "dq_reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "dq_report.html"
    out_path.write_text(_render_html(results, rows=len(df)), encoding="utf-8")
    logger.info("DQ-отчёт сохранён: %s", out_path)

    failed = [r["rule"] for r in results if not r["passed"]]
    if failed:
        raise ValueError(f"DQ-провалы ({len(failed)}): {failed}")
    return str(out_path)
