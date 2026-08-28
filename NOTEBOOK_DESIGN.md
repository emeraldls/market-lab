# Market Lab Notebooks

This document defines the Market Lab Jupyter architecture. The research phase
described here is implemented; simulator-backed notebook backtesting remains a
later phase.

## Core decision

Market Lab does not execute `.ipynb` files through `mlab script` and does not
implement a custom Jupyter kernel.

`mlab notebook` starts a normal Jupyter session with the user's selected Python
environment. Market Lab injects one object named `mlab` into that Python kernel.
The object talks to the running `mlab notebook` process over a private local
socket.

There are two distinct notebook workflows:

1. **Research:** load complete historical datasets and analyse them interactively.
2. **Backtesting:** run a callback causally through the existing Rust simulator.

Research is the first release. Simulator-backed notebook backtesting is a later
phase.

## Starting a notebook

From a strategy directory:

```bash
mlab notebook
```

Market Lab resolves Python using the same order as Python Scripting V2:

1. `--python`
2. `./.venv/bin/python`
3. `python3` or `python`

An explicit environment looks like this:

```bash
mlab notebook --python .venv/bin/python
```

The selected environment must contain `ipykernel`. Market Lab first looks for
JupyterLab beside the selected interpreter, then on `PATH`. It does not
silently install or modify Python packages.

If `ipykernel` is missing, the command should stop with a direct error such as:

```text
Python notebook dependencies are missing: ipykernel
Install them with:
uv add ipykernel
```

Market Lab uses `uv add` when the project has `uv.lock`. Other projects receive
an installation command through the selected environment's `pip`.

## Research notebook, cell by cell

The user does not import Market Lab. `mlab` already exists when the Market Lab
kernel starts.

### Cell 1: import the user's analysis packages

```python
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
```

These packages come from the user's selected Python environment. Market Lab
does not bundle them.

### Cell 2: create a historical range

```python
history = mlab.history(
    from_="2026-01-01",
    to="2026-08-01",
)
```

This creates a `NotebookHistory` object. It records the UTC range but does not
fetch any market data yet.

### Cell 3: request a source

```python
CANDLES = "btc@candles@hyperliquidf@mmt:timeframe=3600"

candles = history.source(CANDLES)
```

The first call sends the selector and historical range to Rust. Market Lab:

1. parses and validates the selector;
2. fetches the historical records using the existing provider implementation;
3. normalizes the records using the existing scripting market-data types;
4. streams the records to Python in bounded chunks;
5. caches the completed dataset in the `NotebookHistory` object.

The returned records are ordered from oldest to newest. Unlike live Scripting
V2 history, notebook research history is not capped by the script lookback.

Calling the same source again uses the cache:

```python
same_candles = history.source(CANDLES)
```

The existing indexing convention remains available:

```python
latest = history.source(CANDLES, 0)
previous = history.source(CANDLES, 1)
```

Index `0` means the newest record in the loaded historical range.

### Cell 4: build a DataFrame

```python
df = pd.DataFrame(candles)
df["t"] = pd.to_datetime(df["t"], unit="ms", utc=True)
df = df.set_index("t")

df.head()
```

### Cell 5: analyse the data

```python
df["body"] = np.abs(df["o"] - df["c"])
df["sma_20"] = df["c"].rolling(20).mean()

df[["c", "sma_20"]].tail()
```

The user can use pandas, NumPy, statsmodels, scikit-learn, or any other package
installed in the selected environment.

Market Lab's stateless study functions are also available directly from
`mlab.study`:

```python
sma = mlab.study.sma(candles, {"field": "c", "window": 20})
sma["latest"]
```

### Cell 6: plot

```python
fig, ax = plt.subplots(figsize=(12, 5))
ax.plot(df.index, df["c"], label="BTC")
ax.plot(df.index, df["sma_20"], label="SMA 20")
ax.legend()
plt.show()
```

The notebook owns display and file output. Research mode does not need the
Market Lab artifacts directory.

### Cell 7: load another source when needed

```python
OI = "btc@oi@binancef@mmt:timeframe=3600"
oi = history.source(OI)
oi_df = pd.DataFrame(oi)
```

Notebook sources are lazy. There is no manifest, `script.sources`, TOML source
section, or static source discovery.

## Where `ctx` lives

The user should **not** create this in a research cell:

```python
ctx = mlab.context()
```

A free-standing `ctx` would have no current event, simulated clock, execution
state, positions, orders, or PnL. Methods such as `ctx.trade()`,
`ctx.positions()`, and `ctx.pnl()` would therefore have undefined meaning.

Research cells use:

```text
mlab                 Market Lab notebook session
mlab.study           stateless Market Lab studies
history              full-range historical data
history.source(...)  lazy source loading and cached access
```

`ctx` exists only while the Rust backtester is processing one historical event.
Market Lab creates it and passes it to the user's strategy callback:

```python
def strategy(ctx, history):
    ...
```

This is the same reason Scripting V2 receives `ctx` in `on_data`: execution
methods are meaningful only inside a specific event and simulator state.

## Future causal backtesting, cell by cell

Notebook backtesting is a separate phase after research history works.

### Cell 1: load research history

```python
CANDLES = "btc@candles@hyperliquidf@mmt:timeframe=3600"

history = mlab.history(
    from_="2026-01-01",
    to="2026-08-01",
)

candles = history.source(CANDLES)
```

### Cell 2: define the causal strategy

```python
def strategy(ctx, history):
    candles = history.source(CANDLES)
    current = history.source(CANDLES, 0)

    if current is None or len(candles) < 20:
        return

    sma = ctx.study.sma(candles, {"field": "c", "window": 20})["latest"]

    if current["c"] > sma and not ctx.positions().open:
        ctx.trade({
            "exchange": "hyperliquidf",
            "symbol": "BTC",
            "position": "open-long",
            "margin": 100,
            "leverage": 5,
        })
```

The callback receives a causal `history`. At each invocation it contains only
the records available at that event. It does not expose future records from the
research history object.

### Cell 3: run the Rust simulator

```python
result = mlab.backtest(
    strategy,
    history=history,
    params={"window": 20},
)
```

The function remains inside the running Python kernel. It is not serialized or
written to a temporary strategy file.

While the cell is running:

1. Python asks Rust to start a backtest with the sources already loaded by
   `history`.
2. Rust merges the records into the same deterministic event order used by
   `mlab script backtest`.
3. Rust sends the next event to the notebook bridge.
4. The bridge updates a separate causal history and invokes
   `strategy(ctx, history)` in the kernel.
5. Calls such as `ctx.trade()` are returned to Rust as execution commands.
6. Rust applies those commands to the existing simulator.
7. Processing continues until the range ends or the user interrupts the cell.

Python does not implement fills, fees, slippage, positions, order state, PnL,
or event ordering.

### Cell 4: inspect the result

```python
result.summary
result.performance
result.trades
result.positions
result.pnl
```

`result` is an immutable representation of the completed Rust simulation.
After the callback returns, users inspect `result`; they do not keep or reuse
the event-specific `ctx`.

### Cell 5: plot PnL

```python
pnl = pd.DataFrame(result.pnl)
pnl["t"] = pd.to_datetime(pnl["t"], unit="ms", utc=True)

plt.plot(pnl["t"], pnl["pnl"])
plt.title("Market Lab backtest PnL")
plt.show()
```

## How `mlab` is embedded into the kernel

The injected object is not built into Python, installed from PyPI, or placed
inside the user's notebook file.

When `mlab notebook` starts, the Rust process creates an ephemeral session:

```text
~/.market-lab/run/notebooks/<session-id>/
├── bridge.sock
├── bootstrap/
│   └── marketlab_notebook.py
├── ipython/
│   └── profile_default/
│       └── ipython_config.py
└── jupyter/
    └── kernels/
        └── python3/
            └── kernel.json
```

The temporary `kernel.json` starts the user's interpreter as a normal IPython
kernel:

```json
{
  "argv": [
    "/absolute/path/to/.venv/bin/python",
    "-m",
    "ipykernel_launcher",
    "-f",
    "{connection_file}"
  ],
  "display_name": "Market Lab (.venv)",
  "language": "python"
}
```

Market Lab supplies session-only environment variables to that kernel:

```text
PYTHONPATH=<session>/bootstrap
IPYTHONDIR=<session>/ipython
MLAB_NOTEBOOK_SOCKET=<session>/bridge.sock
MLAB_NOTEBOOK_TOKEN=<random-session-token>
```

The temporary IPython configuration loads `marketlab_notebook` as an extension.
Its startup function performs the equivalent of:

```python
def load_ipython_extension(ipython):
    bridge = MarketLabNotebookBridge.connect_from_environment()
    ipython.push({"mlab": bridge})
```

That final `push` is why the first notebook cell can use `mlab` without an
import.

This does not replace or intercept Jupyter's own protocol. Jupyter continues to
communicate normally with `ipykernel`. The separate Unix socket is used only for
Market Lab requests:

```text
Jupyter browser
      |
      | Jupyter protocol
      v
normal ipykernel in the user's .venv
      |
      | private Market Lab protocol
      v
bridge.sock
      |
      v
the `mlab notebook` Rust process
```

## Socket and session rules

- The socket and session directory are accessible only to the current Unix
  user.
- Every request carries the random session token.
- The bridge listens only on the Unix socket; it exposes no TCP port.
- Protocol messages have bounded sizes.
- Historical responses are streamed in bounded chunks.
- Interrupting a data fetch or backtest cancels the corresponding Rust task.
- Exiting `mlab notebook` stops its Jupyter child process and removes the
  socket, bootstrap module, IPython profile, and temporary kernelspec.
- The notebook bridge is local and independent from `mlabd`.
- Remote notebook sessions are outside the first release.

## Rust implementation boundary

Historical source parsing, validation, fetching, and normalization are shared
with the script backtest path:

```text
mlab script backtest
        |
        v
shared historical-data service
        ^
        |
mlab notebook
```

Notebook support must not duplicate provider requests or create notebook-only
market-data rules.

The future `mlab.backtest()` call must also reuse the existing script simulator.
There must be one implementation of event ordering, fills, fees, slippage,
orders, positions, and PnL.

## First release scope

Implement only:

```text
mlab notebook [--python <path>]
injected mlab object
mlab.history(from_, to)
history.source(selector)
history.source(selector, index)
mlab.study
lazy loading
session caching
chunked local IPC
clean startup, interruption, and shutdown
```

Do not include in the first release:

```text
.ipynb execution through mlab script
ctx = mlab.context()
live trading from research cells
mlab.backtest()
remote notebook sessions
a custom Jupyter kernel
a public Market Lab Python package
automatic Python package installation
```
