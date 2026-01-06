import os
import io
import json
from pathlib import Path

import pandas as pd
import requests

API_URL = "https://api.bitcoinmagazinepro.com/metrics/realized-price"
# If needed:
# API_URL = "https://api.bitcoinmagazinepro.com/v1/metrics/realized-price"

def main():
    api_key = os.environ["BMP_API_KEY"]

    headers = {
        "Authorization": f"Bearer {api_key}",
    }

    resp = requests.get(API_URL, headers=headers, timeout=30)
    print("BMP API status code:", resp.status_code)
    resp.raise_for_status()

    raw_text = resp.text
    if not raw_text.strip():
        raise RuntimeError("Empty response from BMP API")

    # Response is often a quoted string with literal "\n"
    csv_quoted = raw_text.strip()
    if csv_quoted.startswith('"') and csv_quoted.endswith('"'):
        csv_quoted = csv_quoted[1:-1]
    csv_text = csv_quoted.replace("\\n", "\n")

    df = pd.read_csv(io.StringIO(csv_text))
    if df.empty:
        raise RuntimeError("Parsed empty DataFrame")

    print("Parsed columns:", list(df.columns))

    # Expect: Date, Price, realized_price (plus an unnamed first column)
    if "Date" not in df.columns:
        raise RuntimeError(f"Expected 'Date' column, got {list(df.columns)}")
    if "Price" not in df.columns:
        raise RuntimeError(f"Expected 'Price' column, got {list(df.columns)}")
    if "realized_price" not in df.columns:
        raise RuntimeError(f"Expected 'realized_price' column, got {list(df.columns)}")

    df = df[["Date", "Price", "realized_price"]].copy()
    df["Date"] = df["Date"].astype(str)
    df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
    df["realized_price"] = pd.to_numeric(df["realized_price"], errors="coerce")

    # Require both Price and realized_price for MVRV
    df = df.dropna(subset=["Date", "Price", "realized_price"])

    # Compute MVRV and round to 2 decimals
    df["mvrv"] = (df["Price"] / df["realized_price"]).round(2)

    data = []
    for _, row in df.iterrows():
        data.append({
            "date": row["Date"],
            "mvrv": float(row["mvrv"]),
            "price": float(row["Price"]),
            "realized_price": float(row["realized_price"]),
        })

    out_path = Path("data/mvrv.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(data, indent=2))

    print(f"Wrote {len(data)} points to {out_path}")

if __name__ == "__main__":
    main()
