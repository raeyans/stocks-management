# ken-stocks

Collection of small Python scripts and utilities used to fetch, sync, and analyze Indonesian stock data.

## Overview

- Sync excel data and push to DB: `sync_from_excels.py`
- Data scrapers: `scrapping.py`
- Trading helpers and views: `trading_show.py`, `trading_record.py`
- Misc utilities: `dump_db.py`, `verify_db_excel.py`, `foreign_ratio.py`, `score_to_buy_v5.py`
- Data and outputs live in folders: `data/`, `DUMP/`, `output/`, `reports/`

## Requirements

- Python 3.8+
- Recommended: create a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt  # if you create one
```

## Usage

Run individual scripts directly. Example:

```bash
python3 sync_from_excels.py
python3 trading_show.py
```

Inspect each script for specific options or configuration (some scripts read local files such as `holiday.txt`, `issuer_code.txt`, and files under `balance/`).

## Contributing

Small personal project — contributions welcome. Open an issue or submit a pull request.

## License

This project is licensed under the MIT License — see `LICENSE` for details.
