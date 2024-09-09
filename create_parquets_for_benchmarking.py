from pathlib import Path
from datetime import datetime

import pandas as pd

from isp_trace_parser import trace_formatter


start = datetime.strptime("2030-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
stop = datetime.strptime("2031-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")

solar_data_path = Path("D:/isp_2024_data/trace_data/solar_2011")

for n, filepath in enumerate(solar_data_path.iterdir()):
    # if n >= 20:
    #     break
    solar_data = pd.read_csv(filepath)
    solar_data = trace_formatter(solar_data)
    solar_data['name'] = n
    solar_data.to_parquet(Path(f"parquets/{n}.parquet"))

    solar_data = solar_data[(solar_data['Datetime'] > start) & (solar_data['Datetime'] < stop)]
    solar_data.to_parquet(Path(f"parquets_chunked/{n}.parquet"))










