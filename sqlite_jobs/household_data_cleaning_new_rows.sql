-- Data validation for 'household' table

CREATE TEMP TABLE household_id_report_date
AS
SELECT HOUSEHOLD_ID,
       MAX(REPORTING_DATE) as MAX_REPORTING_DATE
FROM household
GROUP BY HOUSEHOLD_ID
;

select * from household_id_report_date;

-- Create temporary table for data validation
CREATE TEMPORARY TABLE validated_data
AS
SELECT h_s.HOUSEHOLD_ID,
       h_s.INCOME_ID,
       h_s.REPORTING_DATE,
       h_s.MARRIED,
       h_s.HOUSE_OWNER,
       h_s.CHILD_NO,
       h_s.HH_MEMBERS,
       h_s.BUCKET,
       CASE
           WHEN CAST(h_s.HOUSEHOLD_ID AS INT) != h_s.HOUSEHOLD_ID OR CAST(h_s.HOUSEHOLD_ID AS INT) <= 0 THEN FALSE
           WHEN CAST(h_s.INCOME_ID AS INT) != h_s.INCOME_ID OR CAST(h_s.INCOME_ID AS INT) <= 0 THEN FALSE
           WHEN h_s.REPORTING_DATE IS NULL OR DATE(strftime('%Y-%m-%d', substr(h_s.REPORTING_DATE, 7, 4) || '-' ||
                                                                        substr(h_s.REPORTING_DATE, 4, 2) || '-' ||
                                                                        substr(h_s.REPORTING_DATE, 1, 2))) IS NULL
               THEN FALSE
           WHEN h_s.MARRIED NOT IN ('Y', 'N') THEN FALSE
           WHEN h_s.HOUSE_OWNER NOT IN ('Y', 'N') THEN FALSE
           WHEN CAST(h_s.CHILD_NO AS INT) != h_s.CHILD_NO OR CAST(h_s.CHILD_NO AS INT) NOT BETWEEN 0 AND 10 THEN FALSE
           WHEN CAST(h_s.HH_MEMBERS AS INT) != h_s.HH_MEMBERS OR CAST(h_s.HH_MEMBERS AS INT) NOT BETWEEN 1 AND 10
               THEN FALSE
           WHEN CAST(h_s.BUCKET AS INT) != h_s.BUCKET OR CAST(h_s.BUCKET AS INT) <= 0 THEN FALSE
           ELSE TRUE
           END AS IS_VALID,
       (CASE
            WHEN CAST(h_s.HOUSEHOLD_ID AS INT) != h_s.HOUSEHOLD_ID OR CAST(h_s.HOUSEHOLD_ID AS INT) <= 0
                THEN 'HOUSEHOLD_ID'
            ELSE '' END) || '~' ||
       (CASE
            WHEN CAST(h_s.INCOME_ID AS INT) != h_s.INCOME_ID OR CAST(h_s.INCOME_ID AS INT) <= 0 THEN 'INCOME_ID'
            ELSE '' END) || '~' ||
       (CASE
            WHEN h_s.REPORTING_DATE IS NULL OR DATE(strftime('%Y-%m-%d', substr(h_s.REPORTING_DATE, 7, 4) || '-' ||
                                                                         substr(h_s.REPORTING_DATE, 4, 2) || '-' ||
                                                                         substr(h_s.REPORTING_DATE, 1, 2))) IS NULL
                THEN 'REPORTING_DATE'
            ELSE '' END) || '~' ||
       (CASE WHEN h_s.MARRIED NOT IN ('Y', 'N') THEN 'MARRIED' ELSE '' END) || '~' ||
       (CASE WHEN h_s.HOUSE_OWNER NOT IN ('Y', 'N') THEN 'HOUSE_OWNER' ELSE '' END) || '~' ||
       (CASE
            WHEN CAST(h_s.CHILD_NO AS INT) != h_s.CHILD_NO OR CAST(h_s.CHILD_NO AS INT) NOT BETWEEN 0 AND 10
                THEN 'CHILD_NO'
            ELSE '' END) || '~' ||
       (CASE
            WHEN CAST(h_s.HH_MEMBERS AS INT) != h_s.HH_MEMBERS OR CAST(h_s.HH_MEMBERS AS INT) NOT BETWEEN 1 AND 10
                THEN 'HH_MEMBERS'
            ELSE '' END) || '~' ||
       (CASE WHEN CAST(h_s.BUCKET AS INT) != h_s.BUCKET OR CAST(h_s.BUCKET AS INT) <= 0 THEN 'BUCKET' ELSE '' END)
               AS INVALID_COLUMNS
FROM household_stage as h_s

-- Check if the data is already in the 'household' table
         LEFT JOIN household_id_report_date as h_id_d on h_s.HOUSEHOLD_ID = h_id_d.HOUSEHOLD_ID
WHERE h_id_d.HOUSEHOLD_ID IS NULL
   or h_id_d.MAX_REPORTING_DATE < DATE(strftime('%Y-%m-%d', substr(h_s.REPORTING_DATE, 7, 4) || '-' ||
                                                            substr(h_s.REPORTING_DATE, 4, 2) || '-' ||
                                                            substr(h_s.REPORTING_DATE, 1, 2)))
;


-- Drop temporary table
DROP TABLE household_id_report_date;

-- Insert valid data into 'household' table
INSERT INTO household
(HOUSEHOLD_ID, INCOME_ID, REPORTING_DATE, MARRIED, HOUSE_OWNER, CHILD_NO, HH_MEMBERS, BUCKET)
SELECT HOUSEHOLD_ID,
       INCOME_ID,

       -- Convert date format from DD.MM.YYYY to YYYY-MM-DD
       DATE(strftime('%Y-%m-%d', substr(REPORTING_DATE, 7, 4) || '-' ||
                                 substr(REPORTING_DATE, 4, 2) || '-' ||
                                 substr(REPORTING_DATE, 1, 2))) as REPORTING_DATE,
       MARRIED,
       HOUSE_OWNER,
       CHILD_NO,
       HH_MEMBERS,
       BUCKET
FROM validated_data
WHERE IS_VALID = TRUE;


-- Delete the validated data from the stage table
-- for later output of error_rows.csv file
INSERT INTO household_bad_data
(HOUSEHOLD_ID, INCOME_ID, REPORTING_DATE, MARRIED, HOUSE_OWNER, CHILD_NO, HH_MEMBERS, BUCKET, INVALID_COLUMNS,
 CREATION_DTM)
SELECT HOUSEHOLD_ID,
       INCOME_ID,

       -- Convert date format from DD.MM.YYYY to YYYY-MM-DD
       DATE(strftime('%Y-%m-%d', substr(REPORTING_DATE, 7, 4) || '-' ||
                                 substr(REPORTING_DATE, 4, 2) || '-' ||
                                 substr(REPORTING_DATE, 1, 2))) as REPORTING_DATE,
       MARRIED,
       HOUSE_OWNER,
       CHILD_NO,
       HH_MEMBERS,
       BUCKET,
       INVALID_COLUMNS,
       CURRENT_TIMESTAMP
FROM validated_data
WHERE IS_VALID = FALSE;


-- Drop temporary table
DROP TABLE validated_data;
