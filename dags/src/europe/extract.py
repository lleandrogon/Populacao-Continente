import pandas as pd
import pprint

def extract_europe(**kwargs):
    df = pd.read_csv(
        "/opt/airflow/volumes/Europe_countries_by_population.csv"
    )

    pprint.pprint(df)

    return df