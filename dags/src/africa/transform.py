import pandas as pd
import re

def transform_africa(**kwargs):
    df = kwargs["ti"].xcom_pull(task_ids = "extract_africa")

    df = df.rename(columns = {
        "Country": "country",
        "%Africa": "porcent_total",
        "Population[1]": "africa_population",
        "%growth": "porcent_change",
        "Officialfigure": "population",
        "Officialdate": "date"
    })

    df = df[df["country"] != "Total"]

    df["porcent_total"] = df["porcent_total"].str.replace('%', '').astype(float) / 100
    df["porcent_change"] = df["porcent_change"].str.replace('%', '').astype(float) / 100

    df = df.drop(columns = [
        "africa_population"
    ])

    df["population"] = df["population"].str.replace(',', '').astype(int)

    df["date"] = df["date"].str.replace(r'\[.*\]', '', regex=True)

    df["date"] = df["date"].apply(
        lambda x: "1 " + x if pd.notna(x) and re.match(r"^[A-Za-z]{3} \d{4}$", x.strip()) else x
    )

    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    df["continent"] = "Africa"

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