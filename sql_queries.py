# DROP TABLES
arrival_summary_table_drop = "DROP table if exists arrival_summary"
arrival_country_table_drop = "DROP table if exists arrival_country_code"
arrival_state_table_drop = "DROP table if exists arrival_state_code"
arrival_city_table_drop = "DROP table if exists arrival_city_code"
season_table_drop = "DROP table if exists season"
temperature_table_drop = "DROP table if exists temperature"
population_table_drop = "DROP table if exists population"
airport_table_drop = "DROP table if exists airport"

# CREATE TABLES
arrival_summary_table_create = ("""
    CREATE TABLE IF NOT EXISTS arrival_summary(
        arrival_id SERIAL primary key,
        arrival_month varchar(45) NOT NULL,
        arrival_state varchar(45) NULL,
        arrival_city varchar(45) NULL,
        traveller_citizenship int NULL,
        total_travellers int NULL,
        business_travellers int NULL,
        leisure_travellers int NULL,
        student_travellers int NULL,
        UNIQUE(arrival_id)
    )
""")

arrival_country_table_create = ("""
    CREATE TABLE IF NOT EXISTS arrival_country_code(
        country_id SERIAL primary key,
        country_code integer NOT NULL,
        country_name varchar(255) NULL,
        UNIQUE(country_id),
        UNIQUE(country_code)
    )
""")

arrival_state_table_create = ("""
    CREATE TABLE IF NOT EXISTS arrival_state_code(
        state_id SERIAL primary key,
        state_code varchar(45) NOT NULL,
        state_name varchar(255) NULL,
        UNIQUE(state_id),
        UNIQUE(state_code)
    )
""")

arrival_city_table_create = ("""
    CREATE TABLE IF NOT EXISTS arrival_city_code(
        city_id SERIAL primary key,
        city_code varchar(45) NOT NULL,
        city_name varchar(255) NULL,
        state_code varchar(45) NULL,
        UNIQUE(city_id),
        UNIQUE(city_code)
    )
""")

season_table_create = ("""
    CREATE TABLE IF NOT EXISTS season(
        season_id SERIAL primary key,
        season_month varchar(45) NOT NULL,
        season varchar(45) NOT NULL,
        UNIQUE(season_id),
        UNIQUE(season_month)
    )
""")

temperature_table_create = ("""
    CREATE TABLE IF NOT EXISTS temperature(
        temperature_id SERIAL primary key,
        city varchar(255) NOT NULL,
        measurement_month varchar(45) NOT NULL,
        total_measurements varchar(45) NULL,
        min_temperature decimal NULL,
        max_temperature decimal NULL,
        avg_temperature decimal NULL,
        UNIQUE(temperature_id),
        UNIQUE(city, measurement_month)
    )
""")

population_table_create = ("""
    CREATE TABLE IF NOT EXISTS population(
        population_id SERIAL primary key,
        state varchar(255) NOT NULL,
        city varchar(255) NOT NULL,
        median_age decimal NULL,
        male_population integer NULL,
        female_population integer NULL,
        total_population integer NULL,
        race_diversity integer NULL,
        UNIQUE(population_id),
        UNIQUE(state, city)
    )
""")

airport_table_create = ("""
    CREATE TABLE IF NOT EXISTS airport(
        airport_id varchar(45) primary key,
        gps_code varchar(45) NULL,
        iata_code varchar(45) NULL,
        local_code varchar(45) NULL,
        airport_name varchar(255) NULL,
        airport_description varchar(255) NULL,
        airport_state varchar(100) NULL,
        airport_city varchar(255) NULL,
        airport_coordinates_x decimal NULL,
        airport_coordinates_y decimal NULL,
        UNIQUE(airport_id)
    )
""")

# CREATE INDEXES

# INSERT RECORDS
arrival_summary_table_insert = ("""
INSERT INTO arrival_summary (arrival_month ,arrival_state ,arrival_city ,traveller_citizenship ,
    total_travellers ,business_travellers ,leisure_travellers ,student_travellers)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
""")

arrival_country_table_insert = ("""
INSERT INTO arrival_country_code (country_code, country_name)
                 VALUES (%s, %s)
ON CONFLICT(country_code) 
DO update set 
    country_name = EXCLUDED.country_name;
""")

arrival_state_table_insert = ("""
INSERT INTO arrival_state_code (state_code, state_name)
                 VALUES (%s, %s)
ON CONFLICT(state_code) 
DO update set 
    state_name = EXCLUDED.state_name;
""")

arrival_city_table_insert = ("""
INSERT INTO arrival_city_code (city_code ,city_name ,state_code)
                 VALUES (%s, %s, %s)
ON CONFLICT(city_code) 
DO update set 
    city_name = EXCLUDED.city_name,
    state_code = EXCLUDED.state_code;
""")

season_table_insert = ("""
INSERT INTO season (season_month, season)
                 VALUES (%s, %s)
ON CONFLICT(season_month) 
DO update set 
    season = EXCLUDED.season;
""")

temperature_table_insert = ("""
INSERT INTO temperature (city ,measurement_month ,total_measurements ,min_temperature ,max_temperature ,avg_temperature)
                 VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT(city, measurement_month) 
DO update set 
    total_measurements = EXCLUDED.total_measurements,
    min_temperature = EXCLUDED.min_temperature,
    max_temperature = EXCLUDED.max_temperature,
    avg_temperature = EXCLUDED.avg_temperature;
""")

population_table_insert = ("""
INSERT INTO population (state ,city ,median_age ,male_population ,female_population ,total_population ,race_diversity)
                 VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT(state, city) 
DO update set 
    median_age = EXCLUDED.median_age,
    male_population = EXCLUDED.male_population,
    female_population = EXCLUDED.female_population,
    total_population = EXCLUDED.total_population,
    race_diversity = EXCLUDED.race_diversity;
""")

airport_table_insert = ("""
INSERT INTO airport (airport_id ,gps_code ,iata_code ,local_code ,airport_name ,airport_description ,airport_state ,airport_city ,airport_coordinates_x ,airport_coordinates_y)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT(airport_id) 
DO update set 
    gps_code = EXCLUDED.gps_code,
    iata_code = EXCLUDED.iata_code,
    local_code = EXCLUDED.local_code,
    airport_name = EXCLUDED.airport_name,
    airport_description = EXCLUDED.airport_description,
    airport_state = EXCLUDED.airport_state,
    airport_city = EXCLUDED.airport_city,
    airport_coordinates_x = EXCLUDED.airport_coordinates_x,
    airport_coordinates_y = EXCLUDED.airport_coordinates_y;
""")

# SELECT USE CASE
use_case_1_description = """
USE CASE 1: Find the Top 5 Countries Who Most Frequented US in 2016
"""
use_case_1_select = ("""
select s.traveller_citizenship, c.country_name, sum(s.total_travellers) as yearly_travellers
from arrival_summary s
    left join arrival_country_code c on s.traveller_citizenship = c.country_code
group by s.traveller_citizenship, c.country_name
order by yearly_travellers desc
limit 5;
""")

use_case_2_description = """
USE CASE 2: Find the Top 5 Cities That Are Most Visited in 2016
"""
use_case_2_select = ("""
select s.arrival_city, cc.city_name, cc.state_code, 
    sc.state_name, sum(s.total_travellers) as yearly_travellers
from arrival_summary s
    left join arrival_city_code cc on s.arrival_city = cc.city_code
    left join arrival_state_code sc on trim(sc.state_code) = trim(cc.state_code)
group by  s.arrival_city, cc.city_name, cc.state_code, 
    sc.state_name
order by yearly_travellers desc
limit 5
"""
)

use_case_3_description = """
USE CASE 3: Study the relationship between season and the different types of travellers
"""
use_case_3_select = ("""
select a.arrival_month, s.season,
	sum(a.total_travellers) as total_travellers,
	sum(a.business_travellers) as business_travellers,
	sum(a.leisure_travellers) as leisure_travellers,
	sum(a.student_travellers) as student_travellers
from arrival_summary a
	left join season s on  a.arrival_month = s.season_month
group by a.arrival_month, s.season
order by EXTRACT(MONTH FROM to_date(a.arrival_month, 'Month'))
"""
)

# CHECK TABLES
arrival_summary_table_check = ("""
    select count(*) from arrival_summary
""")

arrival_country_table_check = ("""
    select count(*) from arrival_country_code
""")

arrival_state_table_check = ("""
    select count(*) from arrival_state_code
""")

arrival_city_table_check = ("""
    select count(*) from arrival_city_code
""")

season_table_check = ("""
    select count(*) from season
""")

temperature_table_check = ("""
    select count(*) from temperature
""")

population_table_check = ("""
    select count(*) from population
""")

airport_table_check = ("""
    select count(*) from airport
""")

# QUERY LISTS
drop_table_queries = [arrival_summary_table_drop, arrival_country_table_drop, 
    arrival_state_table_drop, arrival_city_table_drop,
    season_table_drop, temperature_table_drop,
    population_table_drop, airport_table_drop]
create_table_queries = [arrival_summary_table_create, arrival_country_table_create,
    arrival_state_table_create, arrival_city_table_create,
    season_table_create, temperature_table_create,
    population_table_create, airport_table_create]
check_table_queries = [arrival_summary_table_check, arrival_country_table_check, 
    arrival_state_table_check, arrival_city_table_check, 
    season_table_check, temperature_table_check, 
    population_table_check, airport_table_check]
select_table_queries = [use_case_1_select, use_case_2_select, use_case_3_select]
select_table_descriptions = [use_case_1_description, use_case_2_description, use_case_3_description]