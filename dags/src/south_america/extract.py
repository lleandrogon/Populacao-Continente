import pandas as pd
import pprint

def extract_south_america(**kwargs):
    df = pd.read_csv(
        "/opt/airflow/volumes/South_America_countries_by_population.csv"
    )

    pprint.pprint(df)

    return df