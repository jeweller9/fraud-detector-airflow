"""Сравнение ROC-AUC новой модели с текущей prod-моделью перед публикацией."""

import json
import logging
from pathlib import Path

from pipeline.settings import MODELS_DIR

logger = logging.getLogger(__name__)


def compare_metrics(train_info: dict) -> str:
    """Решает, публиковать ли новую модель, сравнивая её ROC-AUC с prod.

    Читает:
      - train_info["roc_auc"] — метрика новой модели (XCom из fit_catboost);
      - MODELS_DIR / "latest.txt" — версия текущей prod-модели;
      - MODELS_DIR / f"my_catboost_{prev_version}.metrics.json" — метрики prod-модели.

    Возвращает task_id ветки:
      - "publish_model", если new_auc >= prev_auc или prod ещё нет;
      - "skip_publish", если новая модель хуже.

    Повторный запуск не меняет latest.txt и training_state: это делает только
    publish_model, который в ветке skip_publish не вызывается.
    """
    new_auc = train_info.get("roc_auc")
    if new_auc is None:
        logger.warning("ROC-AUC новой модели не определён — публикуем без сравнения.")
        return "publish_model"

    latest_file = Path(MODELS_DIR) / "latest.txt"
    if not latest_file.exists():
        logger.info("Предыдущей модели нет — публикуем первую версию.")
        return "publish_model"

    prev_version = latest_file.read_text(encoding="utf-8").strip()
    prev_metrics_path = Path(MODELS_DIR) / f"my_catboost_{prev_version}.metrics.json"
    if not prev_metrics_path.exists():
        logger.warning("Метрик prod-модели нет — публикуем новую.")
        return "publish_model"

    prev_metrics = json.loads(prev_metrics_path.read_text(encoding="utf-8"))
    prev_auc = prev_metrics.get("roc_auc")
    if prev_auc is None:
        logger.warning("У prod-модели нет ROC-AUC — публикуем новую.")
        return "publish_model"

    logger.info(
        "Gate по ROC-AUC: новая=%.4f, prod=%.4f (версия %s)",
        new_auc,
        prev_auc,
        prev_version,
    )
    if new_auc >= prev_auc:
        return "publish_model"
    return "skip_publish"
