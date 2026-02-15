import glob
import os
import pandas as pd

IN_GLOB = "data/processed/crime_*.parquet"
OUT_DIR = "data/agg"
os.makedirs(OUT_DIR, exist_ok=True)

USE_COLS = ["date", "primary_type", "latitude", "longitude"]

monthly_total_list = []
monthly_type_list = []
sample_list = []

SAMPLE_PER_YEAR = 30000

files = sorted(glob.glob(IN_GLOB))
if not files:
    raise FileNotFoundError(f"No files matched: {IN_GLOB}")

for f in files:
    year = os.path.basename(f).split("_")[1].split(".")[0]
    print(f"Reading {f} (year={year}) ...")

    df = pd.read_parquet(f, columns=USE_COLS)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "latitude", "longitude", "primary_type"])

    # --- monthly total ---
    df["month"] = df["date"].dt.to_period("M").dt.to_timestamp()
    mt = df.groupby("month").size().reset_index(name="count")
    monthly_total_list.append(mt)

    # --- monthly by type (top types later in dashboard) ---
    mtt = df.groupby(["month", "primary_type"]).size().reset_index(name="count")
    monthly_type_list.append(mtt)

    # --- sample points for map ---
    if len(df) > SAMPLE_PER_YEAR:
        samp = df.sample(SAMPLE_PER_YEAR, random_state=42)[["date","primary_type","latitude","longitude"]]
    else:
        samp = df[["date","primary_type","latitude","longitude"]]
    samp["year"] = int(year)
    sample_list.append(samp)

# Combine yearly aggregates
monthly_total = pd.concat(monthly_total_list, ignore_index=True).groupby("month", as_index=False)["count"].sum()
monthly_type = pd.concat(monthly_type_list, ignore_index=True).groupby(["month","primary_type"], as_index=False)["count"].sum()
sample_points = pd.concat(sample_list, ignore_index=True)

# Save
monthly_total.to_parquet(os.path.join(OUT_DIR, "monthly_total.parquet"), index=False)
monthly_type.to_parquet(os.path.join(OUT_DIR, "monthly_type.parquet"), index=False)
sample_points.to_parquet(os.path.join(OUT_DIR, "sample_points.parquet"), index=False)

print("Done.")
print("Saved:")
print(" - data/agg/monthly_total.parquet")
print(" - data/agg/monthly_type.parquet")
print(" - data/agg/sample_points.parquet")
