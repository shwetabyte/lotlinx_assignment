-- NHTSA Database Initialization
-- Simple schema with just the 2 required tables

-- Table 1: Main processed NHTSA data
CREATE TABLE IF NOT EXISTS processed_nhtsa_data (
    sent_vin VARCHAR(20),
    manufacturer_name VARCHAR(100),
    make VARCHAR(50),
    model VARCHAR(100),
    model_year INTEGER,
    trim VARCHAR(100),
    vehicle_type_id INTEGER,
    body_class_id INTEGER,
    base_price FLOAT,
    ncsa_make VARCHAR(50),
    ncsa_model VARCHAR(100)
);

-- Table 2: NHTSA lookup table
CREATE TABLE IF NOT EXISTS nhtsa_lookup_table (
    vehicle_type_id INTEGER,
    vehicle_type VARCHAR(100),
    body_class_id INTEGER,
    body_class VARCHAR(100),
    lx_bodyclass_lvl1 VARCHAR(50),
    lx_bodyclass_lvl2 VARCHAR(50),
    incomplete_chassis BOOLEAN
);

-- Table 3: Gold layer - Top vehicle models (analytical results)
CREATE TABLE IF NOT EXISTS gold_top_vehicle_models (
    model_year INTEGER,
    make VARCHAR(50),
    model VARCHAR(100),
    vehicle_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table 4: Gold layer - Body class distribution (analytical results)
CREATE TABLE IF NOT EXISTS gold_body_class_distribution (
    lx_bodyclass_lvl1 VARCHAR(50),
    bodysegment VARCHAR(50),
    vehicle_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

