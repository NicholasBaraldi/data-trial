CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.company_profiles_google_maps AS
SELECT
    name,
	phone,
    site,
    city,
    state,
    about::json
FROM
    public.company_profiles_google_maps
WHERE
    name IS NOT NULL
    AND city IS NOT NULL
    AND state IS NOT NULL
    AND verified IS true
    AND business_status LIKE 'OPERATIONAL'
    AND (site IS NOT NULL OR phone IS NOT NULL);