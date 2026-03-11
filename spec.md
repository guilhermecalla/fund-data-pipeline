# Implementation Spec — Base Ops Runner UI

> Derived from `prd.md`. This document tells the implementing agent exactly what to change, where, and how. Do not deviate from the file paths or patterns described here.

---

## Summary of Changes

| # | File | Action | Pattern |
|---|---|---|---|
| 1 | `src/positions.py` | Modify `batch()` signature | Pattern A |
| 2 | `src/portfolio.py` | Modify `batch()` signature | Pattern A |
| 3 | `src/movimentos.py` | Modify `batch()` signature | Pattern B |
| 4 | `src/plfund.py` | Modify `batch()` signature | Pattern B |
| 5 | `src/precos.py` | Modify `batch()` signature | Pattern B |
| 6 | `src/trades_tpe.py` | Modify `batch()` signature | Pattern B (has try/except + sleep) |
| 7 | `manage.py` | Add `--start`/`--end` Click options to 6 batch commands | — |
| 8 | `app.py` | Create new Streamlit UI | — |
| 9 | `requirements.txt` | Append `streamlit` | — |

---

## File 1 — `src/positions.py`

**Path:** `src/positions.py`
**Action:** Modify
**Target:** `batch()` function at line 146

**Current code (lines 146–156):**
```python
def batch():
    """Execução em lote para múltiplas datas"""
    datas = pd.date_range(datetime.date(2025, 10, 28), datetime.date(2025, 11, 28))

    df = pd.DataFrame({'date': datas})
    df['diff_month'] = df.date.dt.month - df.date.shift(-1).dt.month
    df = df[df.diff_month != 0].copy()

    for date in df.date.values:
        data = tarpon_calendar.get_last_trading_day_of_month(date)
        run(data)
```

**Replace with:**
```python
def batch(start_date=None, end_date=None):
    """Execução em lote para múltiplas datas"""
    if start_date is None:
        start_date = datetime.date(2025, 10, 28)
    if end_date is None:
        end_date = datetime.date(2025, 11, 28)

    datas = pd.date_range(start_date, end_date)

    df = pd.DataFrame({'date': datas})
    df['diff_month'] = df.date.dt.month - df.date.shift(-1).dt.month
    df = df[df.diff_month != 0].copy()

    for date in df.date.values:
        data = tarpon_calendar.get_last_trading_day_of_month(date)
        run(data)
```

**No other changes to this file.**

---

## File 2 — `src/portfolio.py`

**Path:** `src/portfolio.py`
**Action:** Modify
**Target:** `batch()` function at line 83

**Current code (lines 83–93):**
```python
def batch():
    """Execução em lote para múltiplas datas"""
    datas = pd.date_range(datetime.date(2025, 10, 31), datetime.date(2025,11, 28))

    df = pd.DataFrame({'date': datas})
    df['diff_month'] = df.date.dt.month - df.date.shift(-1).dt.month
    df = df[df.diff_month != 0].copy()

    for date in df.date.values:
        data = tarpon_calendar.get_last_trading_day_of_month(date)
        run(data)
```

**Replace with:**
```python
def batch(start_date=None, end_date=None):
    """Execução em lote para múltiplas datas"""
    if start_date is None:
        start_date = datetime.date(2025, 10, 31)
    if end_date is None:
        end_date = datetime.date(2025, 11, 28)

    datas = pd.date_range(start_date, end_date)

    df = pd.DataFrame({'date': datas})
    df['diff_month'] = df.date.dt.month - df.date.shift(-1).dt.month
    df = df[df.diff_month != 0].copy()

    for date in df.date.values:
        data = tarpon_calendar.get_last_trading_day_of_month(date)
        run(data)
```

**No other changes to this file.**

---

## File 3 — `src/movimentos.py`

**Path:** `src/movimentos.py`
**Action:** Modify
**Target:** `batch()` function at line 51

**Current code (lines 51–57):**
```python
def batch():
    #datas = tarpon_calendar.get_business_days_in_range(datetime.date(2006, 10, 1), datetime.date(2015, 12, 18)) #yyyy,mm,dd
    datas = tarpon_calendar.get_business_days_in_range(datetime.date(2025, 7, 31), datetime.date(2025, 9, 25)) #yyyy,mm,dd

    for data in datas:
        print(data.strftime("%Y-%m-%d"))
        run(data)
```

**Replace with:**
```python
def batch(start_date=None, end_date=None):
    if start_date is None:
        start_date = datetime.date(2025, 7, 31)
    if end_date is None:
        end_date = datetime.date(2025, 9, 25)

    datas = tarpon_calendar.get_business_days_in_range(start_date, end_date)

    for data in datas:
        print(data.strftime("%Y-%m-%d"))
        run(data)
```

**Note:** The commented-out old date line may be removed. Do not touch `run()` or any other function.

---

## File 4 — `src/plfund.py`

**Path:** `src/plfund.py`
**Action:** Modify
**Target:** `batch()` function at line 51

**Current code (lines 51–56):**
```python
def batch():
    datas = tarpon_calendar.get_business_days_in_range(datetime.date(2025, 7, 25), datetime.date(2025, 7, 25)) #yyyy,mm,dd

    for data in datas:
        #print(data)
        run(data)
```

**Replace with:**
```python
def batch(start_date=None, end_date=None):
    if start_date is None:
        start_date = datetime.date(2025, 7, 25)
    if end_date is None:
        end_date = datetime.date(2025, 7, 25)

    datas = tarpon_calendar.get_business_days_in_range(start_date, end_date)

    for data in datas:
        run(data)
```

**No other changes to this file.**

---

## File 5 — `src/precos.py`

**Path:** `src/precos.py`
**Action:** Modify
**Target:** `batch()` function at line 51

**Current code (lines 51–56):**
```python
def batch():
    datas = tarpon_calendar.get_business_days_in_range(datetime.date(2025, 8, 19), datetime.date(2025, 8, 19)) #yyyy,mm,dd

    for data in datas:
        #print(data)
        run(data)
```

**Replace with:**
```python
def batch(start_date=None, end_date=None):
    if start_date is None:
        start_date = datetime.date(2025, 8, 19)
    if end_date is None:
        end_date = datetime.date(2025, 8, 19)

    datas = tarpon_calendar.get_business_days_in_range(start_date, end_date)

    for data in datas:
        run(data)
```

**No other changes to this file.**

---

## File 6 — `src/trades_tpe.py`

**Path:** `src/trades_tpe.py`
**Action:** Modify
**Target:** `batch()` function at line 76

**Current code (lines 76–84):**
```python
def batch():
    datas = tarpon_calendar.get_business_days_in_range(datetime.date(2020, 1, 1), datetime.date(2025, 8, 26))
    for data in datas:
        try:
            run(data)
            time.sleep(1)  # Pausa entre requisições
        except Exception as e:
            logger.error(f"Erro para data {data}: {e}")
            continue
```

**Replace with:**
```python
def batch(start_date=None, end_date=None):
    if start_date is None:
        start_date = datetime.date(2020, 1, 1)
    if end_date is None:
        end_date = datetime.date(2025, 8, 26)

    datas = tarpon_calendar.get_business_days_in_range(start_date, end_date)
    for data in datas:
        try:
            run(data)
            time.sleep(1)  # Pausa entre requisições
        except Exception as e:
            logger.error(f"Erro para data {data}: {e}")
            continue
```

**No other changes to this file.**

---

## File 7 — `manage.py`

**Path:** `manage.py`
**Action:** Modify
**Target:** All 6 batch command functions

**Rule for all batch commands:** Add `@click.option('--start', ...)` and `@click.option('--end', ...)` decorators, update the function signature, and forward `start_date`/`end_date` to the module `batch()` call.

**Helper pattern to apply to every batch command:**
```python
@cli.command()
@click.option('--start', default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="Start date YYYY-MM-DD")
@click.option('--end',   default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="End date YYYY-MM-DD")
def <command_name>(start, end):
    start_date = start.date() if start else None
    end_date   = end.date()   if end   else None
    <module>.batch(start_date=start_date, end_date=end_date)
```

**Apply to these 6 commands (full replacement for each):**

### `posicao_batch` (currently line 51–52):
```python
@cli.command()
@click.option('--start', default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="Start date YYYY-MM-DD")
@click.option('--end',   default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="End date YYYY-MM-DD")
def posicao_batch(start, end):
    start_date = start.date() if start else None
    end_date   = end.date()   if end   else None
    positions.batch(start_date=start_date, end_date=end_date)
```

### `movimentacao_batch` (currently line 31–32):
```python
@cli.command()
@click.option('--start', default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="Start date YYYY-MM-DD")
@click.option('--end',   default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="End date YYYY-MM-DD")
def movimentacao_batch(start, end):
    start_date = start.date() if start else None
    end_date   = end.date()   if end   else None
    movimentos.batch(start_date=start_date, end_date=end_date)
```

### `prices_range` (currently line 26–27):
```python
@cli.command()
@click.option('--start', default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="Start date YYYY-MM-DD")
@click.option('--end',   default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="End date YYYY-MM-DD")
def prices_range(start, end):
    start_date = start.date() if start else None
    end_date   = end.date()   if end   else None
    precos.batch(start_date=start_date, end_date=end_date)
```

### `pls_batch` (currently line 41–42):
```python
@cli.command()
@click.option('--start', default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="Start date YYYY-MM-DD")
@click.option('--end',   default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="End date YYYY-MM-DD")
def pls_batch(start, end):
    start_date = start.date() if start else None
    end_date   = end.date()   if end   else None
    plfund.batch(start_date=start_date, end_date=end_date)
```

### `operations_batch` (currently line 60–61):
```python
@cli.command()
@click.option('--start', default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="Start date YYYY-MM-DD")
@click.option('--end',   default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="End date YYYY-MM-DD")
def operations_batch(start, end):
    start_date = start.date() if start else None
    end_date   = end.date()   if end   else None
    trades_tpe.batch(start_date=start_date, end_date=end_date)
```

### `carteiras_batch` (currently line 69–71):
```python
@cli.command()
@click.option('--start', default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="Start date YYYY-MM-DD")
@click.option('--end',   default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="End date YYYY-MM-DD")
def carteiras_batch(start, end):
    start_date = start.date() if start else None
    end_date   = end.date()   if end   else None
    portfolio.batch(start_date=start_date, end_date=end_date)
```

**Single-run commands (`movimentacao`, `prices`, `pls`, `posicao`, `operations`, `carteiras`) are NOT changed.**

---

## File 8 — `app.py`

**Path:** `app.py` (project root)
**Action:** Create (new file)

**Full file content:**

```python
import datetime
import logging
from io import StringIO

import streamlit as st

from src import positions, portfolio, movimentos, precos, plfund, trades_tpe

# ---------------------------------------------------------------------------
# Command registries
# ---------------------------------------------------------------------------

BATCH_COMMANDS = {
    "posicao_batch": {
        "module": positions,
        "label": "Posição (Batch)",
        "pattern": "month_end",
    },
    "carteiras_batch": {
        "module": portfolio,
        "label": "Carteiras (Batch)",
        "pattern": "month_end",
    },
    "movimentacao_batch": {
        "module": movimentos,
        "label": "Movimentação (Batch)",
        "pattern": "business_days",
    },
    "prices_range": {
        "module": precos,
        "label": "Preços (Range)",
        "pattern": "business_days",
    },
    "pls_batch": {
        "module": plfund,
        "label": "PL Fundos (Batch)",
        "pattern": "business_days",
    },
    "operations_batch": {
        "module": trades_tpe,
        "label": "Operações TPE (Batch)",
        "pattern": "business_days",
    },
}

SINGLE_COMMANDS = {
    "posicao": {"module": positions, "label": "Posição"},
    "movimentacao": {"module": movimentos, "label": "Movimentação"},
    "prices": {"module": precos, "label": "Preços"},
    "pls": {"module": plfund, "label": "PL Fundos"},
    "operations": {"module": trades_tpe, "label": "Operações TPE"},
    "carteiras": {"module": portfolio, "label": "Carteiras"},
}

# ---------------------------------------------------------------------------
# Log capture helper
# ---------------------------------------------------------------------------

def capture_logs(func, *args, **kwargs):
    """
    Runs `func(*args, **kwargs)` while capturing all log output.
    Returns (log_text: str, error: Exception | None).
    """
    log_stream = StringIO()
    handler = logging.StreamHandler(log_stream)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.addHandler(handler)

    error = None
    try:
        func(*args, **kwargs)
    except Exception as e:
        error = e
    finally:
        root_logger.removeHandler(handler)
        handler.close()

    return log_stream.getvalue(), error


# ---------------------------------------------------------------------------
# Streamlit UI
# ---------------------------------------------------------------------------

st.set_page_config(page_title="Base Ops Runner", layout="wide")
st.title("Base Ops Runner")

tab_batch, tab_single = st.tabs(["Batch Commands", "Single Run"])

# ------------------------------------------------------------------
# Tab 1 — Batch Commands
# ------------------------------------------------------------------
with tab_batch:
    st.subheader("Batch Commands")

    batch_labels = {k: v["label"] for k, v in BATCH_COMMANDS.items()}
    selected_batch_key = st.selectbox(
        "Command",
        options=list(batch_labels.keys()),
        format_func=lambda k: batch_labels[k],
        key="batch_cmd",
    )

    col1, col2 = st.columns(2)
    today = datetime.date.today()
    with col1:
        start_date = st.date_input("Start date", value=today.replace(day=1), key="batch_start")
    with col2:
        end_date = st.date_input("End date", value=today, key="batch_end")

    if st.button("Run", key="batch_run"):
        if start_date > end_date:
            st.error("Start date must be before or equal to end date.")
        else:
            cmd_info = BATCH_COMMANDS[selected_batch_key]
            module = cmd_info["module"]

            with st.spinner(f"Running {cmd_info['label']}..."):
                log_output, err = capture_logs(
                    module.batch,
                    start_date=start_date,
                    end_date=end_date,
                )

            if err:
                st.error(f"Command failed: {err}")

            if log_output:
                st.subheader("Output")
                st.code(log_output, language=None)
            elif not err:
                st.success("Done. No log output captured.")

# ------------------------------------------------------------------
# Tab 2 — Single Run
# ------------------------------------------------------------------
with tab_single:
    st.subheader("Single Run")

    single_labels = {k: v["label"] for k, v in SINGLE_COMMANDS.items()}
    selected_single_key = st.selectbox(
        "Command",
        options=list(single_labels.keys()),
        format_func=lambda k: single_labels[k],
        key="single_cmd",
    )

    if st.button("Run", key="single_run"):
        cmd_info = SINGLE_COMMANDS[selected_single_key]
        module = cmd_info["module"]

        with st.spinner(f"Running {cmd_info['label']}..."):
            log_output, err = capture_logs(module.run)

        if err:
            st.error(f"Command failed: {err}")

        if log_output:
            st.subheader("Output")
            st.code(log_output, language=None)
        elif not err:
            st.success("Done. No log output captured.")
```

---

## File 9 — `requirements.txt`

**Path:** `requirements.txt`
**Action:** Modify — append one line

Add `streamlit` at the end of the file. Do not change any existing lines.

```
streamlit
```

---

## Constraints & Rules for the Implementing Agent

1. **Backward compatibility is mandatory.** All `batch()` calls without arguments must behave identically to current behavior. The `None`-default pattern achieves this.

2. **Do not modify any `run()` function** in any module. Only `batch()` signatures change.

3. **Do not touch `src/calendar.py`, `src/db.py`, `src/logger.py`, `src/api.py`, `src/api2.py`, `src/api3.py`, `src/api4.py`.** These are infrastructure files with no required changes.

4. **`app.py` imports from `src.*`** — ensure it is run from the project root (`streamlit run app.py`) so the `src` package is on the Python path.

5. **Log capture strategy:** Attach a `StreamHandler(StringIO)` to the root logger before calling the function, remove it in a `finally` block. This captures all named loggers (`"Posições"`, `"Carteiras"`, etc.) because they propagate to root by default.

6. **No authentication or scheduling** is in scope for `app.py`.

---

## How to Run After Implementation

```bash
# Install new dependency
pip install streamlit

# Start UI
streamlit run app.py

# CLI still works unchanged
python manage.py posicao_batch

# CLI with new date options
python manage.py posicao_batch --start 2025-01-01 --end 2025-12-31
```
