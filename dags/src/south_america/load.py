from airflow.providers.postgres.hooks.postgres import PostgresHook

def load_south_america(**kwargs):
    df = kwargs["ti"].xcom_pull(task_ids = "transform_south_america")

    hook = PostgresHook(postgres_conn_id = "continentes")

    query = """--sql
        INSERT INTO population (
            continent,
            country,
            porcent_total,
            porcent_change,
            population,
            date
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (country, population, date) DO UPDATE SET
            continent = EXCLUDED.continent,
            porcent_total = EXCLUDED.porcent_total,
            porcent_change = EXCLUDED.porcent_change;
    """

    for row in df[
        [
            "continent",
            "country",
            "porcent_total",
            "porcent_change",
            "population",
            "date"
        ]
    ].itertuples(index = False):
        hook.run(query, parameters = row)