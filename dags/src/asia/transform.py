import pandas as pd

def transform_asia(**kwargs):
    df = kwargs["ti"].xcom_pull(task_ids = "extract_asia")

    df = df.rename(columns = {
        "Rank": "rank",
        "Country / dependency": "country",
        "% Asia": "porcent_total",
        "Asian population": "asia_population",
        "Total population": "total_population",
        "% growth": "porcent_change",
        "Official figure": "population",
        "Official date": "date"
    })

    df = df.drop(columns = [
        "rank"
    ])

    df["porcent_total"] = df["porcent_total"].str.replace('%', '').astype(float) / 100
    df["porcent_change"] = df["porcent_change"].str.replace('%', '').astype(float) / 100

    df = df.drop(columns = [
        "asia_population",
        "total_population"
    ])

    df["population"] = df["population"].str.replace(',', '').astype(int)

    df["date"] = df["date"].replace("26 Sept 2024[39]", "26 sep 2024[39]")

    df["date"] = df["date"].str.replace(r'\[\d+\]', '', regex=True).str.strip()

    df["date_parsed"] = pd.to_datetime(
        df["date"],
        format = "%d %b %Y",
        errors = "coerce"
    )

    mask = df["date_parsed"].isna()

    df.loc[mask, "date_parsed"] = pd.to_datetime(
        df.loc[mask, "date"],
        format = "%Y",
        errors = "coerce"
    )

    df["date"] = df["date_parsed"].dt.strftime("%Y-%m-%d")

    df.drop(columns=["date_parsed"], inplace = True)

    df["continent"] = "Asia"

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