-- Data validation for 'client' table


-- Truncate target table
DELETE
FROM client;


-- Create temporary table for data validation
CREATE TEMPORARY TABLE validated_data
AS
SELECT CUSTOMER_ID,
       REPORTING_DATE,
       AGE,
       EDUCATION,
       BUCKET,
       CASE
           WHEN CAST(CUSTOMER_ID AS INT) != CUSTOMER_ID OR CAST(CUSTOMER_ID AS INT) <= 0
               THEN FALSE -- customer_id is not positive
           WHEN REPORTING_DATE IS NULL OR DATE(strftime('%Y-%m-%d', substr(REPORTING_DATE, 7, 4) || '-' ||
                                                                    substr(REPORTING_DATE, 4, 2) || '-' ||
                                                                    substr(REPORTING_DATE, 1, 2))) IS NULL
               THEN FALSE -- reporting_date is null
           WHEN CAST(age AS INTEGER) != age OR CAST(age AS INTEGER) NOT BETWEEN 18 AND 100
               THEN FALSE -- age is not integer or not in range 18-100
           WHEN EDUCATION NOT IN ('Secondary', 'Elementary', 'Higher Education')
               THEN FALSE -- education is not in the list
           WHEN BUCKET <= 0 THEN FALSE -- bucket is not positive
           ELSE TRUE
           END           AS IS_VALID,
       (CASE
            WHEN CAST(CUSTOMER_ID AS INT) != CUSTOMER_ID OR CAST(CUSTOMER_ID AS INT) <= 0 THEN 'CUSTOMER_ID'
            ELSE '' END) || '~' ||
       (CASE
            WHEN REPORTING_DATE IS NULL OR DATE(strftime('%Y-%m-%d', substr(REPORTING_DATE, 7, 4) || '-' ||
                                                                     substr(REPORTING_DATE, 4, 2) || '-' ||
                                                                     substr(REPORTING_DATE, 1, 2))) IS NULL
                THEN 'REPORTING_DATE'
            ELSE '' END) || '~' ||
       (CASE
            WHEN CAST(age AS INTEGER) != age or CAST(age AS INTEGER) NOT BETWEEN 18 AND 100 THEN 'AGE'
            ELSE '' END) || '~' ||
       (CASE WHEN EDUCATION NOT IN ('Secondary', 'Elementary', 'Higher Education') THEN 'EDUCATION' ELSE '' END) ||
       '~' ||
       (CASE
            WHEN CAST(CUSTOMER_ID AS INT) != CUSTOMER_ID OR CAST(CUSTOMER_ID AS INT) <= 0 <= 0 THEN 'BUCKET'
            ELSE '' END) AS INVALID_COLUMNS
FROM client_stage;

-- Insert valid data into 'client' table
INSERT INTO client
    (CUSTOMER_ID, REPORTING_DATE, AGE, EDUCATION, BUCKET)
SELECT CUSTOMER_ID,

       -- Convert date format from DD.MM.YYYY to YYYY-MM-DD
       DATE(strftime('%Y-%m-%d', substr(REPORTING_DATE, 7, 4) || '-' ||
                                 substr(REPORTING_DATE, 4, 2) || '-' ||
                                 substr(REPORTING_DATE, 1, 2))) as REPORTING_DATE,
       AGE,
       EDUCATION,
       BUCKET
FROM validated_data
WHERE IS_VALID = TRUE;


-- Delete the validated data from the stage table
-- for later output of error_rows.csv file
INSERT INTO client_bad_data
(CUSTOMER_ID, REPORTING_DATE, AGE, EDUCATION, BUCKET, INVALID_COLUMNS, CREATION_DTM)
SELECT CUSTOMER_ID,

       -- Convert date format from DD.MM.YYYY to YYYY-MM-DD
       DATE(strftime('%Y-%m-%d', substr(REPORTING_DATE, 7, 4) || '-' ||
                                 substr(REPORTING_DATE, 4, 2) || '-' ||
                                 substr(REPORTING_DATE, 1, 2))) as REPORTING_DATE,
       AGE,
       EDUCATION,
       BUCKET,
       INVALID_COLUMNS,
       CURRENT_TIMESTAMP
FROM validated_data
WHERE IS_VALID = FALSE;


-- Drop temporary table
DROP TABLE validated_data;
