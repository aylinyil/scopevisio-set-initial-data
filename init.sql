DROP TABLE IF EXISTS postcodes CASCADE;
DROP TABLE IF EXISTS regions CASCADE;
DROP TABLE IF EXISTS vehicles CASCADE;
DROP TABLE IF EXISTS yearly_mileage CASCADE;

CREATE TABLE regions (
    id SERIAL PRIMARY KEY,
    region VARCHAR(255) UNIQUE NOT NULL,
    region_factor REAL NOT NULL
);

CREATE TABLE postcodes (
    id SERIAL PRIMARY KEY,
    postcode VARCHAR(10) UNIQUE NOT NULL,
    region_id INTEGER REFERENCES regions(id)
);

CREATE TABLE vehicle (
    id SERIAL PRIMARY KEY,
    vehicle_type VARCHAR(255) UNIQUE NOT NULL,
    vehicle_factor REAL NOT NULL
);

CREATE TABLE yearly_mileage (
    id SERIAL PRIMARY KEY,
    yearly_milaege_from INTEGER NOT NULL,
    yearly_milaege_to NUMERIC,
    yearly_milaege_factor REAL NOT NULL
);
