import pandas as pd
import pprint

def extract_africa(**kwargs):
    df = pd.read_csv(
        "/opt/airflow/volumes/African_countries_by_population.csv"
    )

    pprint.pprint(df)

    return df