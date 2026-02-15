import pandas as pd
import pprint

def extract_north_america(**kwargs):
    df = pd.read_csv(
        "/opt/airflow/volumes/North_American_countries_by_population.csv"
    )

    pprint.pprint(df)

    return df