import pandas as pd
import pprint

def extract_caribbean(**kwargs):
    df = pd.read_csv(
        "/opt/airflow/volumes/Caribbean_countries_by_population.csv"
    )

    pprint.pprint(df)

    return df