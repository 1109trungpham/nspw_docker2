import os
import tempfile
import pandas as pd
import dask.dataframe as dd
import xarray as xr
import time
import json
import bz2
from fastapi import FastAPI, UploadFile, File, Form, Response
from pydantic import BaseModel
from typing import List, Union
from dask import delayed, compute
from functools import lru_cache

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

@app.post("/weather")
async def get_weather_from_csv(
    file: UploadFile = File(...),
    start_year: int = Form(...),
    end_year: int = Form(...),
    output_format: str = Form("json")
):
    start_time = time.time()

    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        tmp.write(await file.read())
        tmp_path = tmp.name

    try:
        ddf = dd.read_csv(tmp_path, assume_missing=True)
        df_coords = ddf.compute()

        if not {'lon', 'lat'}.issubset(df_coords.columns):
            return Response(content="CSV must contain 'lon' and 'lat' columns", status_code=400)

        start_date = f"{start_year}-01-01"
        end_date = f"{end_year}-12-31"
        variables = ['T2M_MAX', 'T2M_MIN', 'PRECTOTCORR']

        tasks = [
            process_location(float(row['lon']), float(row['lat']), start_date, end_date, variables)
            for _, row in df_coords.iterrows()
        ]

        results = compute(*tasks)

        result = WeatherMultiResponse(duration=round(time.time() - start_time, 2), data=list(results))

        if output_format == "bz2":
            json_data = json.dumps(result.dict())
            compressed = bz2.compress(json_data.encode('utf-8'))
            return Response(content=compressed, media_type="application/x-bzip2", headers={
                "Content-Disposition": "attachment; filename=weather_data.json.bz2"
            })
        else:
            return result
    finally:
        os.remove(tmp_path)
