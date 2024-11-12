CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.customer_reviews_google AS
SELECT
    name,
    reviews,
    review_rating,
    review_text,
    review_datetime_utc::date
FROM
    public.customer_reviews_google
WHERE
    name IS NOT NULL
    AND reviews IS NOT NULL
    AND review_rating IS NOT NULL;
