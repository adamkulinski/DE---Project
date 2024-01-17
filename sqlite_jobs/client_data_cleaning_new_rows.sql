-- Data validation for 'client' table

CREATE TEMP TABLE client_id_report_date
AS
SELECT CUSTOMER_ID,
       MAX(REPORTING_DATE) as MAX_REPORTING_DATE
FROM client
GROUP BY CUSTOMER_ID
;


-- Create temporary table for data validation
CREATE TEMPORARY TABLE validated_data
AS
SELECT cs.CUSTOMER_ID,
       cs.REPORTING_DATE,
       cs.AGE,
       cs.EDUCATION,
       cs.BUCKET,
       CASE
           WHEN CAST(cs.CUSTOMER_ID AS INT) != cs.CUSTOMER_ID OR CAST(cs.CUSTOMER_ID AS INT) <= 0
               THEN FALSE -- customer_id is not positive
           WHEN cs.REPORTING_DATE IS NULL OR DATE(strftime('%Y-%m-%d', substr(cs.REPORTING_DATE, 7, 4) || '-' ||
                                                                       substr(cs.REPORTING_DATE, 4, 2) || '-' ||
                                                                       substr(cs.REPORTING_DATE, 1, 2))) IS NULL
               THEN FALSE -- reporting_date is null
           WHEN CAST(cs.age AS INTEGER) != cs.age OR CAST(cs.age AS INTEGER) NOT BETWEEN 18 AND 100
               THEN FALSE -- age is not integer or not in range 18-100
           WHEN cs.EDUCATION NOT IN ('Secondary', 'Elementary', 'Higher Education')
               THEN FALSE -- education is not in the list
           WHEN cs.BUCKET <= 0 THEN FALSE -- bucket is not positive
           ELSE TRUE
           END           AS IS_VALID,
       (CASE
            WHEN CAST(cs.CUSTOMER_ID AS INT) != cs.CUSTOMER_ID OR CAST(cs.CUSTOMER_ID AS INT) <= 0 THEN 'CUSTOMER_ID'
            ELSE '' END) || '~' ||
       (CASE
            WHEN cs.REPORTING_DATE IS NULL OR DATE(strftime('%Y-%m-%d', substr(cs.REPORTING_DATE, 7, 4) || '-' ||
                                                                        substr(cs.REPORTING_DATE, 4, 2) || '-' ||
                                                                        substr(cs.REPORTING_DATE, 1, 2))) IS NULL
                THEN 'REPORTING_DATE'
            ELSE '' END) || '~' ||
       (CASE
            WHEN CAST(cs.age AS INTEGER) != cs.age or CAST(cs.age AS INTEGER) NOT BETWEEN 18 AND 100 THEN 'AGE'
            ELSE '' END) || '~' ||
       (CASE WHEN cs.EDUCATION NOT IN ('Secondary', 'Elementary', 'Higher Education') THEN 'EDUCATION' ELSE '' END) ||
       '~' ||
       (CASE
            WHEN CAST(cs.CUSTOMER_ID AS INT) != cs.CUSTOMER_ID OR CAST(cs.CUSTOMER_ID AS INT) <= 0 THEN 'BUCKET'
            ELSE '' END) AS INVALID_COLUMNS
FROM client_stage AS cs

-- Getting only new data
         LEFT JOIN client_id_report_date as c_id_d ON cs.CUSTOMER_ID = c_id_d.CUSTOMER_ID
WHERE c_id_d.CUSTOMER_ID IS NULL
   OR c_id_d.MAX_REPORTING_DATE < DATE(strftime('%Y-%m-%d', substr(cs.REPORTING_DATE, 7, 4) || '-' ||
                                                            substr(cs.REPORTING_DATE, 4, 2) || '-' ||
                                                            substr(cs.REPORTING_DATE, 1, 2)))
;

DROP TABLE client_id_report_date;

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
