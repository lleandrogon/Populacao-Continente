CREATE TABLE IF NOT EXISTS population (
    id SERIAL PRIMARY KEY,
    continent VARCHAR(50) NOT NULL,
    country VARCHAR(255) NOT NULL,
    porcent_total DECIMAL(10,4),
    porcent_change DECIMAL(10,4),
    population BIGINT NOT NULL,
    date DATE NOT NULL,
    CONSTRAINT population_unique UNIQUE (country, population, date)
);