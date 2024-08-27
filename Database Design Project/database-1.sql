/*PartC: database*/

PRAGMA foreign_keys = ON;

CREATE TABLE Locations (
    location TEXT,
    iso_code TEXT PRIMARY KEY,
    last_observation_date TEXT,
    source_name TEXT,
    source_website TEXT
);

CREATE TABLE vaccination_records (
    iso_code TEXT,
    date TEXT,
    total_vaccinations INTEGER,
    people_vaccinated INTEGER,
    people_fully_vaccinated INTEGER,
    total_boosters INTEGER,
    daily_vaccinations_raw INTEGER,
    daily_vaccinations INTEGER,
    population INTEGER,
    daily_people_vaccinated INTEGER,
    source_url TEXT,
    PRIMARY KEY (iso_code, date)
);

CREATE TABLE vaccine_by_country (
    vaccines VARCHAR(100),
    iso_code VARCHAR(100),
    PRIMARY KEY (vaccines, iso_code)
);



CREATE TABLE US_state_vaccination (
    date TEXT,
    iso_code TEXT,
    state_name TEXT,
    state_total_vaccinations INTEGER,
    state_total_distributed INTEGER,
    state_shared_doses_used INTEGER,
    state_people_vaccinated INTEGER,
    state_population INTEGER,
    state_fully_vaccinated INTEGER,
    state_daily_vaccinations_raw INTEGER,
    state_daily_vaccinations INTEGER,
    state_total_boosters INTEGER,
    PRIMARY KEY (iso_code, date, state_name),
    FOREIGN KEY (iso_code) REFERENCES locations (iso_code)
);


CREATE TABLE VaccinationByAgeGroup (
    iso_code TEXT,
    date TEXT,
    age_group TEXT,
    people_vaccinated_per_hundred REAL,
    people_fully_vaccinated_per_hundred REAL,
    people_with_booster_per_hundred REAL,
    PRIMARY KEY (iso_code, date, age_group),
    FOREIGN KEY (iso_code) REFERENCES locations (iso_code),
    FOREIGN KEY (date) REFERENCES vaccination_records (date)
);


CREATE TABLE Vaccination_by_manufacturer (
    iso_code TEXT,
    date TEXT,
    vaccine TEXT,
    total_vaccinations INTEGER,
    PRIMARY KEY (iso_code, date, vaccine),
    FOREIGN KEY (iso_code) REFERENCES locations (iso_code),
    FOREIGN KEY (date) REFERENCES vaccination_records (date)
);



/*Part D: Queries*/
/*Queries for D1*/
SELECT
    locations.location AS 'Country Name (CN)',
    (
        SELECT COUNT(DISTINCT date) - 1
        FROM vaccination_records vax
        WHERE vax.iso_code = vaccination_records.iso_code
        AND vax.date <= vaccination_records.date
    ) AS 'Administered Vaccine On Day Number (DN)',
    SUM(vaccination_records.daily_vaccinations) OVER (
        PARTITION BY vaccination_records.iso_code
        ORDER BY vaccination_records.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS 'Running total'
FROM
    vaccination_records
JOIN
    locations ON vaccination_records.iso_code = locations.iso_code
WHERE
    vaccination_records.iso_code NOT LIKE 'OWID_%'
    OR vaccination_records.iso_code IN ('OWID_CYN','OWID_ENG''OWID_KOS','OWID_WLS','OWID_NIR','OWID_SCT')
ORDER BY
    locations.location, vaccination_records.date;

/*Queries for D2*/
SELECT
    locations.location AS 'Country',
    MAX(vax2.running_total) AS 'Cumulative Doses'
FROM (
    SELECT
        vaccination_records.iso_code,
        SUM(vaccination_records.daily_vaccinations) AS running_total
    FROM
        vaccination_records
    GROUP BY
        vaccination_records.iso_code
) vax2
JOIN
    locations ON vax2.iso_code = locations.iso_code
WHERE
    vax2.iso_code NOT LIKE 'OWID_%'
    OR vax2.iso_code IN ('OWID_CYN','OWID_ENG''OWID_KOS','OWID_WLS','OWID_NIR','OWID_SCT')
GROUP BY
    locations.location
ORDER BY
    MAX(vax2.running_total) DESC;

/*Queries for D3*/
SELECT
    vaccine_by_country.vaccines AS 'Vaccine Type',
    locations.location AS 'Country'
FROM
    vaccine_by_country
JOIN
    locations ON vaccine_by_country.iso_code = locations.iso_code
ORDER BY
    vaccine_by_country.vaccines, locations.location;

/*Queries for D4*/
SELECT
    locations.source_website AS "Source Name (URL)",
    SUM(max_values.max_total_vaccinations) AS "Largest total Administered Vaccines"
FROM
    locations
JOIN
    (
        SELECT
            iso_code,
            MAX(total_vaccinations) AS max_total_vaccinations
        FROM
            vaccination_records
        GROUP BY
            iso_code
    ) max_values ON locations.iso_code = max_values.iso_code
WHERE
    locations.iso_code NOT IN (
        'OWID_AFR', 'OWID_ASI', 'OWID_EUR', 'OWID_EUN', 'OWID_SAM',
        'OWID_HIC', 'OWID_LIC', 'OWID_LMC', 'OWID_NAM', 'OWID_OCE',
        'OWID_UMC', 'OWID_WRL'
    )
GROUP BY
    locations.source_website
HAVING
    SUM(max_values.max_total_vaccinations) = (
        SELECT
            MAX(sum_total_vaccinations)
        FROM
            (
                SELECT
                    locations.source_website,
                    SUM(max_values.max_total_vaccinations) AS sum_total_vaccinations
                FROM
                    locations
                JOIN
                    (
                        SELECT
                            iso_code,
                            MAX(total_vaccinations) AS max_total_vaccinations
                        FROM
                            vaccination_records
                        GROUP BY
                            iso_code
                    ) max_values ON locations.iso_code = max_values.iso_code
                WHERE
                    locations.iso_code NOT IN (
                        'OWID_AFR', 'OWID_ASI', 'OWID_EUR', 'OWID_EUN', 'OWID_SAM',
                        'OWID_HIC', 'OWID_LIC', 'OWID_LMC', 'OWID_NAM', 'OWID_OCE',
                        'OWID_UMC', 'OWID_WRL'
                    )
                GROUP BY
                    locations.source_website
            ) max_sums
    )
ORDER BY
    "Largest total Administered Vaccines" DESC;


/*Queries for D5*/
SELECT
    strftime('%Y-%m', vaccination_records.date) AS Date,
    MAX(CASE WHEN locations.location = 'Australia' THEN vaccination_records.people_fully_vaccinated END) AS Australia,
    MAX(CASE WHEN locations.location = 'United States' THEN vaccination_records.people_fully_vaccinated END) AS "United States",
    MAX(CASE WHEN locations.location = 'England' THEN vaccination_records.people_fully_vaccinated END) AS England,
    MAX(CASE WHEN locations.location = 'New Zealand' THEN vaccination_records.people_fully_vaccinated END) AS "New Zealand"
FROM
    vaccination_records
JOIN
    locations ON vaccination_records.iso_code = locations.iso_code
WHERE
    vaccination_records.date >= '2022-01-01' AND vaccination_records.date < '2023-01-01'
    AND vaccination_records.iso_code IN ('AUS', 'USA', 'OWID_ENG', 'NZL')
GROUP BY
    strftime('%Y-%m', vaccination_records.date)
ORDER BY
    strftime('%Y-%m', vaccination_records.date);






























