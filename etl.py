import argparse
import pandas as pd
from pathlib import Path


def run_etl(input_path: Path, output_path: Path, min_total: float) -> int:
    df = pd.read_csv(input_path)

    # Extract: already read

    # Transform: drop duplicates
    df = df.drop_duplicates()

    # Fill numeric NaNs with column mean, non-numeric NaNs with 'unknown'
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    non_numeric_cols = df.select_dtypes(exclude="number").columns.tolist()

    for col in numeric_cols:
        df[col] = df[col].fillna(df[col].mean())

    for col in non_numeric_cols:
        df[col] = df[col].fillna("unknown")

    # Add a simple computed column 'total' summing numeric columns
    if numeric_cols:
        df["total"] = df[numeric_cols].sum(axis=1)
        df = df[df["total"] >= min_total]

    # Load: write to CSV
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    return len(df)


def main():
    parser = argparse.ArgumentParser(description="Simple pandas ETL: CSV -> transform -> CSV")
    parser.add_argument("--input", "-i", default="data/input.csv", help="Input CSV path")
    parser.add_argument("--output", "-o", default="data/output.csv", help="Output CSV path")
    parser.add_argument("--min-total", "-m", type=float, default=0.0, help="Minimum total to keep")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        raise SystemExit(1)

    written = run_etl(input_path, output_path, args.min_total)
    print(f"ETL complete — wrote {written} rows to {output_path}")


if __name__ == "__main__":
    main()
