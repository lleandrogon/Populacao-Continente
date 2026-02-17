CREATE TABLE IF NOT EXISTS population (
    id SERIAL PRIMARY KEY,
    continent VARCHAR(50),
    country VARCHAR(255),
    porcent_total DECIMAL(10,4),
    porcent_change DECIMAL(10,4),
    population BIGINT,
    date DATE
);