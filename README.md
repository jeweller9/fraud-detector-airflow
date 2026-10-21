# MLOps Realtime Fraud Detector with Airflow

Сервис скоринга фродовых транзакций в реальном времени на Kafka + CatBoost,
с оркестрацией переобучения модели в Apache Airflow, проверкой улучшения
модели перед публикацией и Data Quality проверками.

## Что делает проект

- **`transaction_simulator`** — генерирует поток транзакций в Kafka (`transactions`).
- **`fraud_detector`** — читает `transactions`, считает скор и флаг фрода, пишет в `scoring`.
- **`scoring_writer`** — сохраняет результаты в PostgreSQL (`scores`).
- **`data_loader`** — заливает `train.csv` в витрину `labeled_transactions`.
- **`airflow`** — оркестрирует:
  - `retrain_catboost` — раз в 5 минут сдвигает окно, переобучает CatBoost и публикует модель (только если её ROC-AUC не хуже prod);
  - `supply_labeled_batch` — дописывает новый месяц в витрину;
  - `data_quality_check` — проверяет витрину по 7 DQ-правилам и публикует HTML-отчёт.

## Быстрый запуск

### 1. Подготовка

Склонируйте репозиторий и перейдите в его корень:

```bash
git clone https://github.com/jeweller9/fraud-detector-airflow.git
cd fraud-detector-airflow
```

Создайте `.env` из шаблона:

```bash
cp .env.example .env
```

В файле `.env` укажите **абсолютный путь** к CSV-файлу с транзакциями:

```
TRAIN_DATA_CSV=/absolute/path/to/train.csv
```

### 2. Запуск

```bash
docker compose up --build
```

Первый запуск занимает 5–10 минут (скачивание образов и сборка Airflow
с `catboost`). После старта `data_loader` загружает CSV в PostgreSQL и
создаёт стартовое окно обучения `2019-01 … 2019-03`.

### 3. Проверка, что данные загрузились

```bash
docker compose logs --tail=20 data_loader
```

Ожидаемые строки:

```
Loaded 786431 rows into labeled_transactions
Training window initialized: 2019-01, 2019-02, 2019-03
```

## Доступы

| Сервис       | URL                      | Логин / пароль    |
|--------------|--------------------------|-------------------|
| Airflow UI   | http://localhost:8081    | `admin` / `admin` |
| Kafka UI     | http://localhost:8080    | —                 |
| PostgreSQL   | localhost:5432           | `fraud_user` / `fraud_password` |

## Параметры

| Параметр                | Значение     | Где задано                                  |
|-------------------------|--------------|----------------------------------------------|
| `WINDOW_MONTHS`         | `3`          | `pipeline/retrain/slide_training_window.py`  |
| `HISTORY_DEPTH_YEARS`   | `3`          | `dags/supply_labeled_batch.py`               |
| `BATCH_SIZE`            | `100_000`    | `dags/supply_labeled_batch.py`               |
| Расписание переобучения | `*/5 * * * *` | `dags/retrain_catboost.py`                  |
| Расписание DQ           | `*/10 * * * *` | `dags/data_quality_check.py`               |

## Собственный препроцессинг

В `fraud_detector/src/preprocessing.py` (функция `add_features`) добавлены
**4 собственных признака**, которых нет в семинарском препроцессинге:

| Признак                  | Что это                     | Зачем                                            |
|--------------------------|-----------------------------|--------------------------------------------------|
| `log_amount`             | `log1p(amount)`             | Сглаживает выбросы сумм, стабильнее для деревьев |
| `is_night`               | `1`, если час в `[0, 6)`    | Ночные транзакции — частый паттерн фрода        |
| `minutes_since_midnight` | `hour * 60 + minute`        | Более гладкий временной признак, чем `hour`     |
| `amount_is_round`        | `1`, если `amount % 100 == 0` | «Круглые» суммы — маркер тестовых/фрод-операций |

Все признаки вычисляются **построчно** (без статистик по всей выборке),
поэтому одинаково работают и при обучении, и при онлайн-скоринге.

**Единая функция препроцессинга:** `run_preproc` импортируется и в
`pipeline/retrain/preprocess_dataset.py` (обучение), и в
`fraud_detector/app/app.py` (скоринг). Это гарантирует, что модель видит
одни и те же признаки в train и inference.

## Проверка улучшения модели (gate)

В DAG `retrain_catboost` между задачами `fit_catboost` и `publish_model`
добавлено ветвление `compare_metrics` (`BranchPythonOperator`):

- Новая модель обучается, её ROC-AUC на самом свежем месяце окна сравнивается
  с ROC-AUC текущей prod-модели.
- Метрики prod-модели хранятся в `/opt/airflow/models/my_catboost_<версия>.metrics.json`.
- Если `new_auc >= prev_auc` → выполняется `publish_model`:
  - переименовывает `.staging_<версия>.cbm` в `my_catboost_<версия>.cbm`,
  - обновляет `latest.txt` атомарно (через `os.replace`),
  - сдвигает `training_state` (указатель месяца окна).
- Если `new_auc < prev_auc` → выполняется `skip_publish` (пустой оператор):
  - `latest.txt` и `training_state` **не меняются**,
  - текущая prod-модель продолжает скорить,
  - окно **не сдвигается**, месяц не теряется.

Логика устойчива к повторным запускам: `training_state` обновляется
**только внутри** `publish_model`, поэтому при `skip` (или при падении на
предыдущих шагах) окно не уезжает.

## Data Quality

Отдельный DAG **`data_quality_check`** проверяет витрину
`labeled_transactions` по **7 правилам**:

1. `merch` не пустой
2. `cat_id` не пустой
3. `amount` в диапазоне `[0, 1_000_000]`
4. `target` (флаг фрода) принимает только значения `0` или `1`
5. в витрине не менее 50 000 строк
6. нет пропусков в координатах клиента (`lat`, `lon`)
7. `transaction_time` не в будущем

Расписание — `*/10 * * * *`, `catchup=False`.

HTML-отчёт публикуется рядом с каталогом моделей:

- путь в контейнере Airflow: `/opt/airflow/models/dq_reports/dq_report.html`
- скопировать на хост и открыть:
  ```bash
  docker compose cp airflow:/opt/airflow/models/dq_reports/dq_report.html ./dq_report.html
  open dq_report.html
  ```

Если хотя бы одно правило не пройдено, задача падает с `ValueError`,
но отчёт **всё равно создаётся** — в нём видно, какие именно правила не прошли.

## Как проверить работу

### Запуск DAG вручную

Airflow UI (`http://localhost:8081`) → DAG → кнопка **Trigger**.

### Проверить, что модель опубликована

```bash
# Файлы моделей
docker compose exec airflow ls -la /opt/airflow/models/

# Указатель текущей prod-модели
docker compose exec airflow cat /opt/airflow/models/latest.txt

# Метрики prod-модели
docker compose exec airflow cat /opt/airflow/models/my_catboost_<версия>.metrics.json
```

### Убедиться, что модель подменяется без перезапуска

```bash
# Дважды с паузой 30 секунд
docker compose exec postgres psql -U fraud_user -d fraud_db -c \
  "SELECT model_version, COUNT(*) FROM scores GROUP BY model_version ORDER BY model_version;"
```

Счётчик у **самой свежей версии** растёт, у остальных — застыл.
Это доказывает, что `fraud_detector` читает `latest.txt` перед каждой
транзакцией и подхватывает новую модель без перезапуска контейнера.

### Проверить gate (модель не публикуется, если хуже)

Логи `compare_metrics` в Airflow содержат строку:

```
Gate по ROC-AUC: новая=0.9785, prod=0.9798 (версия 20261021T091018)
```

При `skip_publish`:

- `latest.txt` указывает на прежнюю версию;
- `training_state` не сдвинулся;
- в `scores` растёт счётчик только у прежней модели.

### Проверить DQ

```bash
docker compose cp airflow:/opt/airflow/models/dq_reports/dq_report.html ./dq_report.html
open dq_report.html
```

В браузере — таблица с 7 правилами и итогом «7 / 7 правил пройдено».

## Структура проекта

```
dags/
├── retrain_catboost.py         # DAG переобучения (extract → slide → split → preprocess → fit → gate → publish)
├── supply_labeled_batch.py     # DAG пополнения витрины (новый месяц, 100k строк, глубина 3 года)
└── data_quality_check.py       # DAG DQ-проверок (7 правил + HTML-отчёт)

pipeline/
├── settings.py                 # пути, CAT_FEATURES, LABEL, константы
├── db.py                       # подключение к PostgreSQL, training_state
├── retrain/
│   ├── extract_month_batch.py
│   ├── slide_training_window.py    # скользящее окно WINDOW_MONTHS=3
│   ├── time_train_test_split.py    # разбиение по времени (не random!)
│   ├── preprocess_dataset.py       # вызывает run_preproc
│   ├── fit_catboost.py             # обучение + ROC-AUC / F1
│   ├── compare_metrics.py          # gate по ROC-AUC
│   └── publish_model.py            # публикация + сдвиг training_state
└── dq/
    └── checks.py                   # 7 DQ-правил + рендер HTML

fraud_detector/
├── app/app.py                      # Kafka consumer → run_preproc → make_pred
└── src/
    ├── preprocessing.py            # add_features + run_preproc (общий для обучения и скоринга)
    └── scorer.py                   # горячая загрузка latest.txt + predict_proba
```
