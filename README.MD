# PythonProject

## Folder quick map

- `automata/`: betting engine modules (`weather`, `eth`, `btc_reach`, `eth_1h`, `view_bets`).
- `docs/`: static docs/demo files (`index.html`, `app.js`, `styles.css`, `running.md`).
- `.venv/`: local virtual environment.
- `.idea/`: IDE project settings.
- `.claude/`: local agent/tool metadata.

## Setup

```bash
pip install -r requirements.txt
```

## How to run (with examples)

- Combined loop (dry run):

```bash
python -m automata
```

- Combined loop (live orders):

```bash
python -m automata --bet
```

- Combined loop with per-daemon spend caps:

```bash
python -m automata --bet --weather-max-balance 40 --eth-max-balance 20
```

- Weather daemon only:

```bash
python -m automata.weather --bet --once --max-balance 30
```

- ETH daemon only:

```bash
python -m automata.eth --bet --once --max-balance 30
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

- `automata`: `--bet`, `--weather-interval`, `--eth-interval`, `--weather-max-balance`, `--eth-max-balance`
- `automata.weather`: `--bet`, `--interval`, `--once`, `--max-balance`
- `automata.eth`: `--bet`, `--interval`, `--once`, `--max-balance`
