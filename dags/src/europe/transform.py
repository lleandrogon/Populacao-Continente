import pandas as pd
import numpy as np
import re

def transform_europe(**kwargs):
    df = kwargs["ti"].xcom_pull(task_ids = "extract_europe")

    df = df.rename(columns = {
        "Location": "country",
        "UN estimate(2025)": "un_estimate",
        "%change": "porcent_change",
        "Officialfigure": "population",
        "Officialdate": "date"
    })

    df = df[df["country"] != "Europe"]

    df = df.drop(columns = [
        "un_estimate"
    ])

    df["porcent_change"] = df["porcent_change"] \
        .str.replace('%', '') \
        .str.replace('−', '-') \
        .str.strip() \
        .replace('', np.nan) \
        .replace('-', np.nan) \
        .astype(float) / 100
    
    df["population"] = df["population"].str.replace(',', '').astype(int)

    df["porcent_total"] = ((df["population"] / df["population"].sum()) * 100).round(4)

    df["date"] = df["date"].str.replace(r'\[.*\]', '', regex=True)

    df["date"] = df["date"].apply(
        lambda x: "1 " + x if pd.notna(x) and re.match(r"^[A-Za-z]{3} \d{4}$", x.strip()) else x
    )

    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    df["continent"] = "Europe"

    df = df[
        [
            "continent",
            "country",
            "porcent_total",
            "porcent_change",
            "population",
            "date"
        ]
    ]

    return df