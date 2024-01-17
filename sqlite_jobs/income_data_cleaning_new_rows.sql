-- Data validation for 'income' table

CREATE TEMP TABLE income_id_report_date
AS
SELECT INCOME_ID,
       MAX(REPORTING_DATE) as MAX_REPORTING_DATE
FROM income
GROUP BY INCOME_ID
;

-- Create temporary table for data validation
CREATE TEMPORARY TABLE validated_data
AS
SELECT i_s.INCOME_ID,
       i_s.CUSTOMER_ID,
       i_s.REPORTING_DATE,
       i_s.FIRST_JOB,
       i_s.INCOME,
       i_s.BUCKET,
       CASE
           WHEN CAST(i_s.INCOME_ID AS INT) != i_s.INCOME_ID OR CAST(i_s.INCOME_ID AS INT) <= 0 THEN FALSE
           WHEN CAST(i_s.CUSTOMER_ID AS INT) != i_s.CUSTOMER_ID OR CAST(i_s.CUSTOMER_ID AS INT) <= 0 THEN FALSE
           WHEN i_s.REPORTING_DATE IS NULL OR DATE(strftime('%Y-%m-%d', substr(i_s.REPORTING_DATE, 7, 4) || '-' ||
                                                                    substr(i_s.REPORTING_DATE, 4, 2) || '-' ||
                                                                    substr(i_s.REPORTING_DATE, 1, 2))) IS NULL THEN FALSE
           WHEN i_s.FIRST_JOB NOT IN ('Y', 'N') THEN FALSE
           WHEN CAST(i_s.INCOME AS INT) != i_s.INCOME OR CAST(i_s.INCOME AS INT) NOT BETWEEN 1000 AND 30000 THEN FALSE
           WHEN CAST(i_s.BUCKET AS INT) != i_s.BUCKET OR CAST(i_s.BUCKET AS INT) <= 0 THEN FALSE
           ELSE TRUE
           END AS IS_VALID,
       (CASE WHEN CAST(i_s.INCOME_ID AS INT) != i_s.INCOME_ID OR CAST(i_s.INCOME_ID AS INT) <= 0 THEN 'INCOME_ID' ELSE '' END) ||
       '~' ||
       (CASE
            WHEN CAST(i_s.CUSTOMER_ID AS INT) != i_s.CUSTOMER_ID OR CAST(i_s.CUSTOMER_ID AS INT) <= 0 THEN 'CUSTOMER_ID'
            ELSE '' END) || '~' ||
       (CASE
            WHEN i_s.REPORTING_DATE IS NULL OR DATE(strftime('%Y-%m-%d', substr(i_s.REPORTING_DATE, 7, 4) || '-' ||
                                                                     substr(i_s.REPORTING_DATE, 4, 2) || '-' ||
                                                                     substr(i_s.REPORTING_DATE, 1, 2))) IS NULL
                THEN 'REPORTING_DATE'
            ELSE '' END) || '~' ||
       (CASE WHEN i_s.FIRST_JOB NOT IN ('Y', 'N') THEN 'FIRST_JOB' ELSE '' END) || '~' ||
       (CASE
            WHEN CAST(i_s.INCOME AS INT) != i_s.INCOME OR CAST(i_s.INCOME AS INT) NOT BETWEEN 1000 AND 30000 THEN 'INCOME'
            ELSE '' END) ||
       '~' ||
       (CASE WHEN CAST(i_s.BUCKET AS INT) != i_s.BUCKET OR CAST(i_s.BUCKET AS INT) <= 0 THEN 'BUCKET' ELSE '' END)
               AS INVALID_COLUMNS

FROM income_stage as i_s

    -- Check if the data is already in the 'income' table
         LEFT JOIN income_id_report_date as i_id_d ON i_s.INCOME_ID = i_id_d.INCOME_ID
WHERE i_id_d.INCOME_ID IS NULL
   OR i_id_d.MAX_REPORTING_DATE < DATE(strftime('%Y-%m-%d', substr(i_s.REPORTING_DATE, 7, 4) || '-' ||
                                                                     substr(i_s.REPORTING_DATE, 4, 2) || '-' ||
                                                                     substr(i_s.REPORTING_DATE, 1, 2)))
;

DROP TABLE income_id_report_date;

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
