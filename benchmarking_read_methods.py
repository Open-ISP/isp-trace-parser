from time import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import polars as pl

start = datetime.strptime("2030-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
stop = datetime.strptime("2031-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")

print("Pandas v polars read:")

t0 = time()
for n in range(0, 189):
    df = pd.read_parquet(f"parquets/{n}.parquet")
ttp = time()-t0
print(f"Read with pandas: {ttp:.3f}s ")

t0 = time()
for n in range(0, 189):
    pl.read_parquet(f"parquets/{n}.parquet")
tt = time()-t0
print(f"Read with polars: {tt:.3f}s {ttp/tt:.1f}x")

print("\n")

print("Reading and filtering to one year with polars:")

t0 = time()
for n in range(0, 189):
    df = pl.read_parquet(f"parquets/{n}.parquet")
    df.filter(
        (pl.col("Datetime") > start) &
        (pl.col("Datetime") < stop)
    )
ttp = time()-t0
print(f"Read then filter with polars: {ttp:.1f}s")

t0 = time()
for n in range(0, 189):
    pl.scan_parquet(f"parquets/{n}.parquet").filter(
        (pl.col("Datetime") > start) &
        (pl.col("Datetime") < stop)
    ).collect()
tt = time()-t0
print(f"Scan and filter with polars: {tt:.3f}s {ttp/tt:.1f}x")

t0 = time()
# for n in range(0, 20):
pl.scan_parquet(f"parquets/*.parquet").filter(
    (pl.col("Datetime") > start) &
    (pl.col("Datetime") < stop)
).collect()
tt = time()-t0
print(f"Glob scan all at once and filter with polars: {tt:.3f}s {ttp/tt:.1f}x")

t0 = time()
for n in range(0, 189):
    pl.read_parquet(f"parquets_chunked/{n}.parquet")
tt = time()-t0
print(f"Read with polars filtering already done (as if partitioned on FY): {tt:.3f}s {ttp/tt:.1f}x")

t0 = time()
def read(file):
    df = pl.scan_parquet(file).filter(
        (pl.col("Datetime") > start) &
        (pl.col("Datetime") < stop)
    ).collect()
    return df
files = Path("parquets").iterdir()
with ThreadPoolExecutor() as executor:
    results = list(executor.map(read, files))
df_combined = pl.concat(results)
tt = time()-t0
print(f"Non polars parallelization: scan and filter with polars: {tt:.3f}s {ttp/tt:.1f}x")

t0 = time()
def read(file):
    df = pl.read_parquet(file)
    return df
files = Path("parquets_chunked").iterdir()
with ThreadPoolExecutor() as executor:
    results = list(executor.map(read, files))
df_combined = pl.concat(results)
tt = time()-t0
print(f"Non polars parallelization: read with polars filtering already done (as if partitioned on FY): {tt:.3f}s {ttp/tt:.1f}x")


