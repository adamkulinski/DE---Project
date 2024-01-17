-- Data validation for 'income' table


-- Truncate target table
DELETE
FROM income;


-- Create temporary table for data validation
CREATE TEMPORARY TABLE validated_data
AS
SELECT INCOME_ID,
       CUSTOMER_ID,
       REPORTING_DATE,
       FIRST_JOB,
       INCOME,
       BUCKET,
       CASE
           WHEN CAST(INCOME_ID AS INT) != INCOME_ID OR CAST(INCOME_ID AS INT) <= 0 THEN FALSE
           WHEN CAST(CUSTOMER_ID AS INT) != CUSTOMER_ID OR CAST(CUSTOMER_ID AS INT) <= 0 THEN FALSE
           WHEN REPORTING_DATE IS NULL OR DATE(strftime('%Y-%m-%d', substr(REPORTING_DATE, 7, 4) || '-' ||
                                                                     substr(REPORTING_DATE, 4, 2) || '-' ||
                                                                     substr(REPORTING_DATE, 1, 2))) IS NULL THEN FALSE
           WHEN FIRST_JOB NOT IN ('Y', 'N') THEN FALSE
           WHEN CAST(INCOME AS INT) != INCOME OR CAST(INCOME AS INT) NOT BETWEEN 1000 AND 30000 THEN FALSE
           WHEN CAST(BUCKET AS INT) != BUCKET OR CAST(BUCKET AS INT) <= 0 THEN FALSE
           ELSE TRUE
           END AS IS_VALID,
       (CASE WHEN CAST(INCOME_ID AS INT) != INCOME_ID OR CAST(INCOME_ID AS INT) <= 0 THEN 'INCOME_ID' ELSE '' END) || '~' ||
       (CASE WHEN CAST(CUSTOMER_ID AS INT) != CUSTOMER_ID OR CAST(CUSTOMER_ID AS INT) <= 0 THEN 'CUSTOMER_ID' ELSE '' END) || '~' ||
       (CASE WHEN REPORTING_DATE IS NULL OR DATE(strftime('%Y-%m-%d', substr(REPORTING_DATE, 7, 4) || '-' ||
                                                                     substr(REPORTING_DATE, 4, 2) || '-' ||
                                                                     substr(REPORTING_DATE, 1, 2))) IS NULL THEN 'REPORTING_DATE' ELSE '' END) || '~' ||
       (CASE WHEN FIRST_JOB NOT IN ('Y', 'N') THEN 'FIRST_JOB' ELSE '' END) || '~' ||
       (CASE WHEN CAST(INCOME AS INT) != INCOME OR CAST(INCOME AS INT) NOT BETWEEN 1000 AND 30000 THEN 'INCOME' ELSE '' END) ||
       '~' ||
       (CASE WHEN CAST(BUCKET AS INT) != BUCKET OR CAST(BUCKET AS INT) <= 0 THEN 'BUCKET' ELSE '' END)
            AS INVALID_COLUMNS

FROM income_stage;

-- Insert valid data into 'income' table
INSERT INTO income
(INCOME_ID, CUSTOMER_ID, REPORTING_DATE, FIRST_JOB, INCOME, BUCKET)
SELECT INCOME_ID,
       CUSTOMER_ID,

       -- Convert date format from DD.MM.YYYY to YYYY-MM-DD
       DATE(strftime('%Y-%m-%d', substr(REPORTING_DATE, 7, 4) || '-' ||
                                 substr(REPORTING_DATE, 4, 2) || '-' ||
                                 substr(REPORTING_DATE, 1, 2))) as REPORTING_DATE,
       FIRST_JOB,
       INCOME,
       BUCKET
FROM validated_data
WHERE IS_VALID = TRUE;


-- Delete the validated data from the stage table
-- for later output of error_rows.csv file
INSERT INTO income_bad_data
(INCOME_ID, CUSTOMER_ID, REPORTING_DATE, FIRST_JOB, INCOME, BUCKET, INVALID_COLUMNS, CREATION_DTM)
SELECT INCOME_ID,
       CUSTOMER_ID,

       -- Convert date format from DD.MM.YYYY to YYYY-MM-DD
       DATE(strftime('%Y-%m-%d', substr(REPORTING_DATE, 7, 4) || '-' ||
                                 substr(REPORTING_DATE, 4, 2) || '-' ||
                                 substr(REPORTING_DATE, 1, 2))) as REPORTING_DATE,
       FIRST_JOB,
       INCOME,
       BUCKET,
       INVALID_COLUMNS,
       CURRENT_TIMESTAMP
FROM validated_data
WHERE IS_VALID = FALSE;


-- Drop temporary table
DROP TABLE validated_data;
