-- L_AIRPORT.csv

CREATE TABLE IF NOT EXISTS travel.airport (
    code varchar,
    description varchar
);

COPY travel.airport
FROM '/Users/ryan/Downloads/L_AIRPORT.csv'
WITH (FORMAT CSV, HEADER);

-- L_AIRPORT_SEQ_ID.csv
CREATE TABLE IF NOT EXISTS travel.airport_seq_id (
    code varchar,
    description varchar
);

COPY travel.airport_seq_id
FROM '/Users/ryan/Downloads/L_AIRPORT_SEQ_ID.csv'
WITH (FORMAT CSV, HEADER);

-- L_AIRLINE_ID.csv
CREATE TABLE IF NOT EXISTS travel.airline_id (
    code varchar,
    description varchar
);

COPY travel.airline_id
FROM '/Users/ryan/Downloads/L_AIRLINE_ID.csv'
WITH (FORMAT CSV, HEADER);


-- L_AIRPORT_ID.csv
CREATE TABLE IF NOT EXISTS travel.airport_id (
    code varchar,
    description varchar
);

COPY travel.airport_id
FROM '/Users/ryan/Downloads/L_AIRPORT_ID.csv'
WITH (FORMAT CSV, HEADER);

-- L_BRANDED_CODE_SHARE.csv
-- CREATE TABLE IF NOT EXISTS travel.branded_code_share (
--     code varchar,
--     description varchar
-- );
--
-- COPY travel.branded_code_share
-- FROM '/Users/ryan/Downloads/L_BRANDED_CODE_SHARE.csv'
-- WITH (FORMAT CSV, HEADER);


-- L_CANCELLATION.csv
CREATE TABLE IF NOT EXISTS travel.cancellation (
    code varchar,
    description varchar
);

COPY travel.cancellation
FROM '/Users/ryan/Downloads/L_CANCELLATION.csv'
WITH (FORMAT CSV, HEADER);

-- L_CARRIER_HISTORY.csv
CREATE TABLE IF NOT EXISTS travel.carrier_history (
    code varchar,
    description varchar
);

COPY travel.carrier_history
FROM '/Users/ryan/Downloads/L_CARRIER_HISTORY.csv'
WITH (FORMAT CSV, HEADER);

-- L_CITY_MARKET_ID.csv
CREATE TABLE IF NOT EXISTS travel.city_market_id (
    code varchar,
    description varchar
);

COPY travel.city_market_id
FROM '/Users/ryan/Downloads/L_CITY_MARKET_ID.csv'
WITH (FORMAT CSV, HEADER);

-- L_DEPARRBLK.csv
CREATE TABLE IF NOT EXISTS travel.deparrblk (
    code varchar,
    description varchar
);

COPY travel.deparrblk
FROM '/Users/ryan/Downloads/L_DEPARRBLK.csv'
WITH (FORMAT CSV, HEADER);

-- L_DISTANCE_GROUP_250.csv
CREATE TABLE IF NOT EXISTS travel.distance_group_250 (
    code varchar,
    description varchar
);

COPY travel.distance_group_250
FROM '/Users/ryan/Downloads/L_DISTANCE_GROUP_250.csv'
WITH (FORMAT CSV, HEADER);

-- L_DIVERSIONS.csv
CREATE TABLE IF NOT EXISTS travel.diversions (
    code varchar,
    description varchar
);

COPY travel.diversions
FROM '/Users/ryan/Downloads/L_DIVERSIONS.csv'
WITH (FORMAT CSV, HEADER);

-- L_MONTHS.csv
CREATE TABLE IF NOT EXISTS travel.months (
    code varchar,
    description varchar
);

COPY travel.months
FROM '/Users/ryan/Downloads/L_MONTHS.csv'
WITH (FORMAT CSV, HEADER);


-- L_ONTIME_DELAY_GROUPS.csv
CREATE TABLE IF NOT EXISTS travel.ontime_delay_groups (
    code varchar,
    description varchar
);

COPY travel.ontime_delay_groups
FROM '/Users/ryan/Downloads/L_ONTIME_DELAY_GROUPS.csv'
WITH (FORMAT CSV, HEADER);

-- L_QUARTERS.csv
CREATE TABLE IF NOT EXISTS travel.quarters (
    code varchar,
    description varchar
);

COPY travel.quarters
FROM '/Users/ryan/Downloads/L_QUARTERS.csv'
WITH (FORMAT CSV, HEADER);

-- L_STATE_ABR_AVIATION.csv
CREATE TABLE IF NOT EXISTS travel.state_abr_aviation (
    code varchar,
    description varchar
);

COPY travel.state_abr_aviation
FROM '/Users/ryan/Downloads/L_STATE_ABR_AVIATION.csv'
WITH (FORMAT CSV, HEADER);

-- L_STATE_FIPS.csv
CREATE TABLE IF NOT EXISTS travel.state_fips (
    code varchar,
    description varchar
);

COPY travel.state_fips
FROM '/Users/ryan/Downloads/L_STATE_FIPS.csv'
WITH (FORMAT CSV, HEADER);


-- L_UNIQUE_CARRIERS.csv
CREATE TABLE IF NOT EXISTS travel.unique_carriers (
    code varchar,
    description varchar
);

COPY travel.unique_carriers
FROM '/Users/ryan/Downloads/L_UNIQUE_CARRIERS.csv'
WITH (FORMAT CSV, HEADER);


-- L_WEEKDAYS.csv
CREATE TABLE IF NOT EXISTS travel.weekdays (
    code varchar,
    description varchar
);

COPY travel.weekdays
FROM '/Users/ryan/Downloads/L_WEEKDAYS.csv'
WITH (FORMAT CSV, HEADER);

-- L_AIRPORT.csv
CREATE TABLE IF NOT EXISTS travel.airport (
    code varchar,
    description varchar
);

COPY travel.airport
FROM '/Users/ryan/Downloads/L_AIRPORT.csv'
WITH (FORMAT CSV, HEADER);


-- L_WORLD_AREA_CODES.csv
CREATE TABLE IF NOT EXISTS travel.world_area_codes (
    code varchar,
    description varchar
);

COPY travel.world_area_codes
FROM '/Users/ryan/Downloads/L_WORLD_AREA_CODES.csv'
WITH (FORMAT CSV, HEADER);


-- L_YESNO_CHAR.csv
CREATE TABLE IF NOT EXISTS travel.yesno_char (
    code varchar,
    description varchar
);

COPY travel.yesno_char
FROM '/Users/ryan/Downloads/L_YESNO_CHAR.csv'
WITH (FORMAT CSV, HEADER);

-- L_YESNO_RESP.csv
CREATE TABLE IF NOT EXISTS travel.yesno_resp (
    code varchar,
    description varchar
);

COPY travel.yesno_resp
FROM '/Users/ryan/Downloads/L_YESNO_RESP.csv'
WITH (FORMAT CSV, HEADER);
