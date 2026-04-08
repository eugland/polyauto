# PythonProject

## Folder quick map

- `automata/`: betting engine modules (`main`, `btc_reach`, `eth_1h`, `view_bets`).
- `docs/`: static docs/demo files (`index.html`, `app.js`, `styles.css`, `running.md`).
- `.venv/`: local virtual environment.
- `.idea/`: IDE project settings.
- `.claude/`: local agent/tool metadata.

## Setup

```bash
pip install -r requirements.txt
```

## How to run (with examples)

- Main loop (dry run):

```bash
python -m automata.main
```

- Main loop (live orders):

```bash
python -m automata.main --bet
```

- BTC range analyzer:

```bash
python -m automata.btc_reach
```

- ETH 1H analyzer:

```bash
python -m automata.eth_1h
```

- Bets UI:

```bash
python -m automata.view_bets
```

Open: `http://localhost:5050`

## Parameters

- `--bet` (only for `automata.main`): enables real order placement.
- Other modules currently run without CLI flags.
