import pandas as pd

def transform_oceania(**kwargs):
    df = kwargs["ti"].xcom_pull(task_ids = "extract_oceania")

    df = df.rename(columns = {
        "Country / dependency": "country",
        "% Total": "porcent_total",
        "Oceania population": "oceania_population",
        "% growth": "porcent_change",
        "Official figure": "population",
        "Official date": "date"
    })

    df = df[df["country"] != "Total"]

    df["porcent_total"] = df["porcent_total"].str.replace('%', '').astype(float) / 100
    df["porcent_change"] = df["porcent_change"].str.replace('%', '').astype(float) / 100

    df = df.drop(columns = [
        "oceania_population"
    ])

    df["population"] = df["population"].str.replace(',', '').astype(int)

    df["date"] = df["date"].str.replace(r'\[.*\]', '', regex = True)

    df["date"] = pd.to_datetime(df["date"], format = "%d %b %Y")

    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    import pandas as pd

def transform_north_america(**kwargs):
    df = kwargs["ti"].xcom_pull(task_ids = "extract_north_america")

    df = df.rename(columns = {
        "Rank": "rank",
        "Country / dependency": "country",
        "% Total": "porcent_total",
        "North America population": "north_america_population",
        "% change": "porcent_change",
        "Official figure": "population",
        "Official date": "date"
    })

    df = df.drop(columns = [
        "rank"
    ])

    df = df[df["country"] != "total"]

    df["porcent_total"] = df["porcent_total"].str.replace('%', '').astype(float) / 100
    df["porcent_change"] = df["porcent_change"].str.replace('%', '').astype(float) / 100

    df = df.drop(columns = [
        "north_america_population"
    ])

    df["population"] = df["population"].str.replace(',', '').astype(int)
    
    df["date"] = df["date"].str.replace(r'\[.*\]', '', regex = True)
    df["date"] = pd.to_datetime(df["date"], format = "%d %b %Y")
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    df["continent"] = "Oceania"

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

    return df