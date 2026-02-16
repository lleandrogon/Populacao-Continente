import pandas as pd
import pprint

def extract_asia(**kwargs):
    df = pd.read_csv(
        "/opt/airflow/volumes/Asian_countries_by_population.csv"
    )

    pprint.pprint(df)

    return df