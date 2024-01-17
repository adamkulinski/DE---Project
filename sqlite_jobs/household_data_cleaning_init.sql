-- Data validation for 'household' table


-- Truncate target table
DELETE
FROM household;


-- Create temporary table for data validation
CREATE TEMPORARY TABLE validated_data
AS
SELECT HOUSEHOLD_ID,
       INCOME_ID,
       REPORTING_DATE,
       MARRIED,
       HOUSE_OWNER,
       CHILD_NO,
       HH_MEMBERS,
       BUCKET,
       CASE
           WHEN CAST(HOUSEHOLD_ID AS INT) != HOUSEHOLD_ID OR CAST(HOUSEHOLD_ID AS INT) <= 0 THEN FALSE
           WHEN CAST(INCOME_ID AS INT) != INCOME_ID OR CAST(INCOME_ID AS INT) <= 0 THEN FALSE
           WHEN REPORTING_DATE IS NULL OR DATE(strftime('%Y-%m-%d', substr(REPORTING_DATE, 7, 4) || '-' ||
                                                                     substr(REPORTING_DATE, 4, 2) || '-' ||
                                                                     substr(REPORTING_DATE, 1, 2))) IS NULL THEN FALSE
           WHEN MARRIED NOT IN ('Y', 'N') THEN FALSE
           WHEN HOUSE_OWNER NOT IN ('Y', 'N') THEN FALSE
           WHEN CAST(CHILD_NO AS INT) != CHILD_NO OR CAST(CHILD_NO AS INT) NOT BETWEEN 0 AND 10 THEN FALSE
           WHEN CAST(HH_MEMBERS AS INT) != HH_MEMBERS OR CAST(HH_MEMBERS AS INT) NOT BETWEEN 1 AND 10 THEN FALSE
           WHEN CAST(BUCKET AS INT) != BUCKET OR CAST(BUCKET AS INT) <= 0 THEN FALSE
           ELSE TRUE
           END AS IS_VALID,
       (CASE
            WHEN CAST(HOUSEHOLD_ID AS INT) != HOUSEHOLD_ID OR CAST(HOUSEHOLD_ID AS INT) <= 0 THEN 'HOUSEHOLD_ID'
            ELSE '' END) || '~' ||
       (CASE WHEN CAST(INCOME_ID AS INT) != INCOME_ID OR CAST(INCOME_ID AS INT) <= 0 THEN 'INCOME_ID' ELSE '' END) || '~' ||
       (CASE
            WHEN REPORTING_DATE IS NULL OR DATE(strftime('%Y-%m-%d', substr(REPORTING_DATE, 7, 4) || '-' ||
                                                                     substr(REPORTING_DATE, 4, 2) || '-' ||
                                                                     substr(REPORTING_DATE, 1, 2))) IS NULL
                THEN 'REPORTING_DATE'
            ELSE '' END) || '~' ||
       (CASE WHEN MARRIED NOT IN ('Y', 'N') THEN 'MARRIED' ELSE '' END) || '~' ||
       (CASE WHEN HOUSE_OWNER NOT IN ('Y', 'N') THEN 'HOUSE_OWNER' ELSE '' END) || '~' ||
       (CASE
            WHEN CAST(CHILD_NO AS INT) != CHILD_NO OR CAST(CHILD_NO AS INT) NOT BETWEEN 0 AND 10 THEN 'CHILD_NO'
            ELSE '' END) || '~' ||
       (CASE
            WHEN CAST(HH_MEMBERS AS INT) != HH_MEMBERS OR CAST(HH_MEMBERS AS INT) NOT BETWEEN 1 AND 10 THEN 'HH_MEMBERS'
            ELSE '' END) || '~' ||
       (CASE WHEN CAST(BUCKET AS INT) != BUCKET OR CAST(BUCKET AS INT) <= 0 THEN 'BUCKET' ELSE '' END)
               AS INVALID_COLUMNS
FROM household_stage;

SELECT * FROM validated_data;

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
