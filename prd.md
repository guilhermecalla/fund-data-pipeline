# PRD — Base Ops Runner UI

## 1. Overview

Add a **Streamlit web UI** to `base_ops` so that any pipeline command from `manage.py` can be triggered from a browser window with a command selector, dynamic date range input, and live log output — without touching code or the terminal directly.

---

## 2. Problem Statement

All batch commands in `manage.py` have **hardcoded date ranges** inside each module. To run a specific date range, a developer must:

1. Open the source file (e.g. `src/positions.py`)
2. Manually edit the `batch()` function dates
3. Run `python manage.py posicao_batch` from the terminal

This is error-prone, slow, and inaccessible to non-technical users.

---

## 3. Goals

- Provide a browser-based UI to run any `manage.py` command
- Allow dynamic date range selection for all batch commands
- Show real-time (or post-run) log output directly in the UI
- Preserve full backward compatibility with the CLI (`manage.py` still works as-is)

---

## 4. Current State — Codebase Audit

### 4.1 `manage.py` — CLI Entry Point

Click-based CLI with the following commands:

| Command | Module | Function | Type |
|---|---|---|---|
| `posicao` | `src/positions.py` | `run()` | Single run |
| `posicao_batch` | `src/positions.py` | `batch()` | Batch |
| `movimentacao` | `src/movimentos.py` | `run()` | Single run |
| `movimentacao_batch` | `src/movimentos.py` | `batch()` | Batch |
| `prices` | `src/precos.py` | `run()` | Single run |
| `prices_range` | `src/precos.py` | `batch()` | Batch |
| `pls` | `src/plfund.py` | `run()` | Single run |
| `pls_batch` | `src/plfund.py` | `batch()` | Batch |
| `operations` | `src/trades_tpe.py` | `run()` | Single run |
| `operations_batch` | `src/trades_tpe.py` | `batch()` | Batch |
| `carteiras` | `src/portfolio.py` | `run()` | Single run |
| `carteiras_batch` | `src/portfolio.py` | `batch()` | Batch |

### 4.2 Batch Function Patterns

There are **two distinct patterns** across all batch functions:

#### Pattern A — Month-End Dates (positions.py, portfolio.py)

These modules iterate over month boundaries, resolving each to the last trading day of that month via `tarpon_calendar.get_last_trading_day_of_month()`.

```python
# Current — hardcoded
def batch():
    datas = pd.date_range(datetime.date(2025, 10, 28), datetime.date(2025, 11, 28))
    df = pd.DataFrame({'date': datas})
    df['diff_month'] = df.date.dt.month - df.date.shift(-1).dt.month
    df = df[df.diff_month != 0].copy()
    for date in df.date.values:
        data = tarpon_calendar.get_last_trading_day_of_month(date)
        run(data)
```

| Module | Hardcoded start | Hardcoded end |
|---|---|---|
| `positions.py` | 2025-10-28 | 2025-11-28 |
| `portfolio.py` | 2025-10-31 | 2025-11-28 |

#### Pattern B — Business Days Range (movimentos.py, plfund.py, precos.py, trades_tpe.py)

These modules use `tarpon_calendar.get_business_days_in_range()` to iterate every business day.

```python
# Current — hardcoded
def batch():
    datas = tarpon_calendar.get_business_days_in_range(
        datetime.date(2025, 7, 31), datetime.date(2025, 9, 25)
    )
    for data in datas:
        run(data)
```

| Module | Hardcoded start | Hardcoded end |
|---|---|---|
| `movimentos.py` | 2025-07-31 | 2025-09-25 |
| `plfund.py` | 2025-07-25 | 2025-07-25 |
| `precos.py` | 2025-08-19 | 2025-08-19 |
| `trades_tpe.py` | 2020-01-01 | 2025-08-26 |

### 4.3 Key Infrastructure

- **TarponCalendar** (`src/calendar.py`): Brazilian trading calendar. Key methods:
  - `get_last_trading_day_of_month(date)` — used by Pattern A
  - `get_business_days_in_range(start, end)` — used by Pattern B
  - `get_previous_trading_day(date)` — used by `run()` when no date is passed

- **Logger** (`src/logger.py`): RotatingFileHandler + StreamHandler (console). All modules use named loggers (e.g. `"Posições"`, `"Carteiras"`).

- **Database** (`src/db.py`): PostgreSQL via SQLAlchemy. Batch inserts with deduplication on composite keys.

- **Environment variables**: Credentials loaded from `.env` (Maravi API + DB). The UI will inherit these automatically when run from the same directory.

---

## 5. Proposed Solution

### 5.1 Architecture

```
browser
  └── Streamlit app (app.py)
        ├── calls src/positions.batch(start_date, end_date)
        ├── calls src/movimentos.batch(start_date, end_date)
        ├── ... (all other modules)
        └── captures logging output → displays in UI

CLI (manage.py) — unchanged behavior, backward compatible
  └── python manage.py posicao_batch --start 2025-01-01 --end 2025-12-31
```

### 5.2 Changes Required

#### Change 1 — Refactor `batch()` signatures in all 6 modules

Add `start_date=None` and `end_date=None` parameters. When `None`, fall back to the current hardcoded defaults (CLI backward compatibility preserved).

**Pattern A change** (positions.py, portfolio.py):
```python
def batch(start_date=None, end_date=None):
    if start_date is None:
        start_date = datetime.date(2025, 10, 28)  # old default
    if end_date is None:
        end_date = datetime.date(2025, 11, 28)    # old default

    datas = pd.date_range(start_date, end_date)
    df = pd.DataFrame({'date': datas})
    df['diff_month'] = df.date.dt.month - df.date.shift(-1).dt.month
    df = df[df.diff_month != 0].copy()
    for date in df.date.values:
        data = tarpon_calendar.get_last_trading_day_of_month(date)
        run(data)
```

**Pattern B change** (movimentos.py, plfund.py, precos.py, trades_tpe.py):
```python
def batch(start_date=None, end_date=None):
    if start_date is None:
        start_date = datetime.date(2025, 7, 31)  # old default
    if end_date is None:
        end_date = datetime.date(2025, 9, 25)    # old default

    datas = tarpon_calendar.get_business_days_in_range(start_date, end_date)
    for data in datas:
        run(data)
```

#### Change 2 — Update `manage.py` batch commands

Add `--start` and `--end` Click options to all batch commands, forwarding them to the module `batch()` function.

```python
@cli.command()
@click.option('--start', default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="Start date YYYY-MM-DD")
@click.option('--end',   default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="End date YYYY-MM-DD")
def posicao_batch(start, end):
    start_date = start.date() if start else None
    end_date   = end.date()   if end   else None
    positions.batch(start_date=start_date, end_date=end_date)
```

#### Change 3 — Create `app.py` (Streamlit UI)

New file at project root. Features:
- Two tabs: **Batch Commands** and **Single Run**
- **Batch tab**: command dropdown + date range picker + Run button + log output area
- **Single tab**: command dropdown + Run button + log output area
- Log capture: temporarily attach a `logging.StreamHandler(StringIO)` to the root logger before calling the function, remove it after, then display captured output

**UI layout (Batch tab):**
```
Base Ops Runner
───────────────────────────────────────
[Batch Commands] [Single Run]

Command:  [posicao_batch ▼]

Date range:
  2025-01-01  →  2025-12-31

[▶ Run]

Output:
┌─────────────────────────────────────┐
│ 2025-01-16 - Posições - INFO - ...  │
│ 2025-01-16 - Posições - INFO - ...  │
└─────────────────────────────────────┘
```

**Command registry in app.py:**
```python
BATCH_COMMANDS = {
    "posicao_batch":       {"module": positions,   "label": "Posição (Batch)",         "pattern": "month_end"},
    "carteiras_batch":     {"module": portfolio,   "label": "Carteiras (Batch)",        "pattern": "month_end"},
    "movimentacao_batch":  {"module": movimentos,  "label": "Movimentação (Batch)",     "pattern": "business_days"},
    "prices_range":        {"module": precos,      "label": "Preços (Range)",           "pattern": "business_days"},
    "pls_batch":           {"module": plfund,      "label": "PL Fundos (Batch)",        "pattern": "business_days"},
    "operations_batch":    {"module": trades_tpe,  "label": "Operações TPE (Batch)",    "pattern": "business_days"},
}

SINGLE_COMMANDS = {
    "posicao":       {"module": positions,   "label": "Posição"},
    "movimentacao":  {"module": movimentos,  "label": "Movimentação"},
    "prices":        {"module": precos,      "label": "Preços"},
    "pls":           {"module": plfund,      "label": "PL Fundos"},
    "operations":    {"module": trades_tpe,  "label": "Operações TPE"},
    "carteiras":     {"module": portfolio,   "label": "Carteiras"},
}
```

---

## 6. Files Affected

| File | Change | Type |
|---|---|---|
| `src/positions.py` | `batch()` signature: add `start_date`, `end_date` | Modify |
| `src/portfolio.py` | `batch()` signature: add `start_date`, `end_date` | Modify |
| `src/movimentos.py` | `batch()` signature: add `start_date`, `end_date` | Modify |
| `src/plfund.py` | `batch()` signature: add `start_date`, `end_date` | Modify |
| `src/precos.py` | `batch()` signature: add `start_date`, `end_date` | Modify |
| `src/trades_tpe.py` | `batch()` signature: add `start_date`, `end_date` | Modify |
| `manage.py` | Add `--start`/`--end` options to all 6 batch commands | Modify |
| `app.py` | New Streamlit UI | Create |
| `requirements.txt` | Add `streamlit` | Modify |

---

## 7. Dependencies

One new dependency: `streamlit`.

```
pip install streamlit
streamlit run app.py
```

No other new dependencies. Streamlit is self-contained and does not conflict with existing packages.

---

## 8. How to Run

```bash
# Start the UI
streamlit run app.py

# CLI still works as before (backward compatible)
python manage.py posicao_batch

# CLI with new date options
python manage.py posicao_batch --start 2025-01-01 --end 2025-12-31
```

---

## 9. Out of Scope

- Authentication / access control for the UI (internal tool assumption)
- Real-time streaming log output (post-run display is sufficient)
- Scheduling / cron integration (separate concern)
- Modifying the API, DB schema, or calendar logic
