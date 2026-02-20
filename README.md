# Simple pandas ETL

This is a minimal ETL example using pandas.

Files added:
- `etl.py` — ETL script (reads CSV, transforms, writes CSV)
- `data/input.csv` — sample input
- `requirements.txt` — dependencies

Run locally:

```bash
python -m pip install -r requirements.txt
python etl.py --input data/input.csv --output data/output.csv
```

Optional: set `--min-total` to filter rows by computed `total`.
# python
