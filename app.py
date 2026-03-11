import datetime
import logging
import threading
import time
from io import StringIO

from dotenv import load_dotenv
load_dotenv()

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
# Session state init
# ---------------------------------------------------------------------------

for _k, _v in [
    ("job_thread", None),
    ("job_error_holder", []),
    ("job_log_stream", None),
    ("job_stop_event", None),
    ("job_label", ""),
]:
    if _k not in st.session_state:
        st.session_state[_k] = _v

# Derive running state from thread liveness — never written from background thread
job_running = (
    st.session_state.job_thread is not None
    and st.session_state.job_thread.is_alive()
)

# ---------------------------------------------------------------------------
# Job launcher
# ---------------------------------------------------------------------------

def _launch(func, label, with_stop=False, **kwargs):
    stop_event = threading.Event() if with_stop else None
    log_stream = StringIO()
    error_holder = []

    handler = logging.StreamHandler(log_stream)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logging.getLogger().addHandler(handler)

    def _run():
        try:
            if with_stop:
                func(**kwargs, stop_event=stop_event)
            else:
                func(**kwargs)
        except Exception as e:
            error_holder.append(e)
        finally:
            logging.getLogger().removeHandler(handler)
            handler.close()

    thread = threading.Thread(target=_run, daemon=True)
    st.session_state.job_thread = thread
    st.session_state.job_error_holder = error_holder
    st.session_state.job_log_stream = log_stream
    st.session_state.job_stop_event = stop_event
    st.session_state.job_label = label
    thread.start()


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
        disabled=job_running,
    )

    col1, col2 = st.columns(2)
    today = datetime.date.today()
    with col1:
        start_date = st.date_input(
            "Start date", value=today.replace(day=1), key="batch_start",
            disabled=job_running,
        )
    with col2:
        end_date = st.date_input(
            "End date", value=today, key="batch_end",
            disabled=job_running,
        )

    btn_col1, btn_col2, _ = st.columns([1, 1, 8])
    with btn_col1:
        run_clicked = st.button("Run", key="batch_run", disabled=job_running)
    with btn_col2:
        stop_clicked = st.button(
            "Stop", key="batch_stop", type="primary",
            disabled=not (job_running and st.session_state.job_stop_event is not None),
        )

    if run_clicked:
        if start_date > end_date:
            st.error("Start date must be before or equal to end date.")
        else:
            cmd_info = BATCH_COMMANDS[selected_batch_key]
            _launch(
                cmd_info["module"].batch,
                label=cmd_info["label"],
                with_stop=True,
                start_date=start_date,
                end_date=end_date,
            )
            st.rerun()

    if stop_clicked and st.session_state.job_stop_event:
        st.session_state.job_stop_event.set()

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
        disabled=job_running,
    )

    if st.button("Run", key="single_run", disabled=job_running):
        cmd_info = SINGLE_COMMANDS[selected_single_key]
        _launch(cmd_info["module"].run, label=cmd_info["label"], with_stop=False)
        st.rerun()

# ------------------------------------------------------------------
# Shared status + output (below tabs)
# ------------------------------------------------------------------
st.divider()

if job_running:
    st.info(f"Running **{st.session_state.job_label}**... (Stop finishes after current date)")

err = (st.session_state.job_error_holder or [None])[0]
if err:
    st.error(f"Command failed: {err}")

log_stream = st.session_state.job_log_stream
if log_stream is not None:
    log_text = log_stream.getvalue()
    if log_text:
        st.subheader("Output")
        st.code(log_text, language=None)
    elif not job_running and not err:
        st.success("Done. No log output captured.")

# Polling loop — reruns every 0.5 s while the thread is alive
if job_running:
    time.sleep(0.5)
    st.rerun()
