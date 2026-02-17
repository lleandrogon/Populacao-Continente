import pandas as pd
import pprint

def extract_oceania(**kwargs):
    df = pd.read_csv(
        "/opt/airflow/volumes/Oceanian_countries_by_population.csv"
    )

    pprint.pprint

    return df