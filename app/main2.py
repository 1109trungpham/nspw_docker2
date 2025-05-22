# Đẩy kết quả lên S3

import os
import tempfile
import pandas as pd
import dask.dataframe as dd
import xarray as xr
import time
from datetime import datetime
import json
import bz2
from fastapi import FastAPI, UploadFile, File, Form, Response
from pydantic import BaseModel
from typing import List, Union
from dask import delayed, compute
from functools import lru_cache
import boto3
from dotenv import load_dotenv
load_dotenv()

class WeatherResponse(BaseModel):
    header: List[str]
    value: List[List[Union[int, float]]]
    location: List[float]

class WeatherMultiResponse(BaseModel):
    duration: float
    data: List[WeatherResponse]

app = FastAPI()

@lru_cache(maxsize=1)
def get_dataset():
    filepath = 'https://nasa-power.s3.amazonaws.com/merra2/temporal/power_merra2_daily_temporal_lst.zarr'
    return xr.open_zarr(filepath, consolidated=True)

@delayed
def process_location(lon: float, lat: float, start_date: str, end_date: str, variables: List[str]) -> WeatherResponse:
    ds = get_dataset().copy(deep=True)

    subset = ds[variables].sel(
        lat=lat,
        lon=lon,
        method="nearest"
    ).sel(time=slice(start_date, end_date))

    df = subset.to_dataframe().reset_index()
    df = df.drop_duplicates(subset=['time'])

    df['day'] = df['time'].dt.day.astype(int)
    df['month'] = df['time'].dt.month.astype(int)
    df['year'] = df['time'].dt.year.astype(int)
    df['day_of_year'] = df['time'].dt.dayofyear.astype(int)

    df = df[['day', 'month', 'year', 'day_of_year', 'T2M_MAX', 'T2M_MIN', 'PRECTOTCORR']]

    values = [
        [int(row['day']), int(row['month']), int(row['year']), int(row['day_of_year']),
         float(f"{row['T2M_MAX']:.4f}"),
         float(f"{row['T2M_MIN']:.4f}"),
         float(f"{row['PRECTOTCORR']:.4f}")]
        for _, row in df.iterrows()
    ]

    return WeatherResponse(
        header=["day", "month", "year", "day_of_year", "t2m_max", "t2m_min", "precipitation"],
        value=values,
        location=[lon, lat]
    )


s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name="your-region",
)
BUCKET = "your-bucket-name"


@app.post("/weather")
async def get_weather_from_csv(
    file: UploadFile = File(...),
    start_year: int = Form(...),
    end_year: int = Form(...)
):
    start_time = time.time()

    # Ghi file upload ra file tạm
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        tmp.write(await file.read())
        tmp_path = tmp.name

    try:
        # Đọc csv bằng Dask
        ddf = dd.read_csv(tmp_path, assume_missing=True)
        df_coords = ddf.compute()

        if not {'lon', 'lat'}.issubset(df_coords.columns):
            return Response(content="CSV must contain 'lon' and 'lat' columns", status_code=400)

        start_date = f"{start_year}-01-01"
        end_date = f"{end_year}-12-31"
        variables = ['T2M_MAX', 'T2M_MIN', 'PRECTOTCORR']

        # Dask xử lý song song
        tasks = [
            process_location(float(row['lon']), float(row['lat']), start_date, end_date, variables)
            for _, row in df_coords.iterrows()
        ]

        results = compute(*tasks)

        result = WeatherMultiResponse(duration=round(time.time() - start_time, 2), data=list(results))
        

        s3_key = f"weather_results/{datetime.now().date()}_{datetime.now().time()}_weather.json.bz2"

        with tempfile.NamedTemporaryFile(delete=False, suffix=".json.bz2") as result_file:
            json_data = json.dumps(result.dict())
            compressed = bz2.compress(json_data.encode('utf-8'))
            result_file.write(compressed)
            result_path = result_file.name

        # Đẩy lên S3
        s3.upload_file(
            result_path, BUCKET, s3_key,
            ExtraArgs={"ContentType": "application/x-bzip2"}
        )

        # Tuỳ chọn: xoá file result
        os.remove(result_path)

        return {
            "message": "Success",
            "s3_key": s3_key,
            "duration": result.duration
        }

    finally:
        os.remove(tmp_path)

