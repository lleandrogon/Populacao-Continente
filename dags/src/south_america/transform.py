import pandas as pd

def transform_south_america(**kwargs):
    df = kwargs["ti"].xcom_pull(task_ids = "extract_south_america")

    df = df.rename(columns = {
        "Country / dependency": "country",
        "% Total": "porcent_total",
        "South America population": "south_america_population",
        "% change": "porcent_change",
        "Official figure": "population",
        "Official date": "date"
    })

    df = df[df["country"] != "Total"]

    df["porcent_total"] = df["porcent_total"].str.replace('%', '').astype(float) / 100
    df["porcent_change"] = df["porcent_change"].str.replace('%', '').astype(float) / 100

    df = df.drop(columns = [
        "south_america_population"
    ])

    df["population"] = df["population"].str.replace(',', '').astype(int)

    df["date"] = df["date"].str.replace(r'\[.*\]', '', regex = True)
    df["date"] = pd.to_datetime(df["date"], format = "%d %b %Y")
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    df["continent"] = "South America"

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