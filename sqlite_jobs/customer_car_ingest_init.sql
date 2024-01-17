-- This script is for initial data load into customer_car_target table
-- It is assumed that the customer_car_target table is empty
-- So we can use INSERT INTO instead of MERGE
DELETE
FROM customer_car;

INSERT INTO customer_car (CUSTOMER_ID,

    -- Attributes from Client table
                          CLIENT_REPORTING_DATE,
                          CLIENT_AGE,
                          CLIENT_EDUCATION,
                          CLIENT_BUCKET,

    -- Attributes from Household table
                          HOUSEHOLD_ID,
                          HOUSEHOLD_INCOME_ID,
                          HOUSEHOLD_REPORTING_DATE,
                          HOUSEHOLD_MARRIED,
                          HOUSEHOLD_HOUSE_OWNER,
                          HOUSEHOLD_CHILD_NO,
                          HOUSEHOLD_HH_MEMBERS,
                          HOUSEHOLD_BUCKET,

    -- Attributes from Loan table
                          LOAN_ID,
                          LOAN_REPORTING_DATE,
                          LOAN_INTODEFAULT,
                          LOAN_INSTALLMENT_NM,
                          LOAN_AMT,
                          LOAN_INSTALLMENT_AMT,
                          LOAN_PAST_DUE_AMT,
                          LOAN_BUCKET,
-- Attributes from Income table
                          INCOME_ID,
                          INCOME_REPORTING_DATE,
                          INCOME_FIRST_JOB,
                          INCOME_AMOUNT,
                          INCOME_BUCKET)
SELECT c.CUSTOMER_ID,
-- Client attributes
       c.REPORTING_DATE  AS CLIENT_REPORTING_DATE,
       c.AGE             AS CLIENT_AGE,
       c.EDUCATION       AS CLIENT_EDUCATION,
       c.BUCKET          AS CLIENT_BUCKET,

-- Household attributes
       h.HOUSEHOLD_ID    AS HOUSEHOLD_ID,
       h.INCOME_ID       AS HOUSEHOLD_INCOME_ID,
       h.REPORTING_DATE  AS HOUSEHOLD_REPORTING_DATE,
       h.MARRIED         AS HOUSEHOLD_MARRIED,
       h.HOUSE_OWNER     AS HOUSEHOLD_HOUSE_OWNER,
       h.CHILD_NO        AS HOUSEHOLD_CHILD_NO,
       h.HH_MEMBERS      AS HOUSEHOLD_HH_MEMBERS,
       h.BUCKET          AS HOUSEHOLD_BUCKET,

-- Loan attributes
       l.LOAN_ID         AS LOAN_ID,
       l.REPORTING_DATE  AS LOAN_REPORTING_DATE,
       l.INTODEFAULT     AS LOAN_INTODEFAULT,
       l.INSTALLMENT_NM  AS LOAN_INSTALLMENT_NM,
       l.LOAN_AMT        AS LOAN_AMT,
       l.INSTALLMENT_AMT AS LOAN_INSTALLMENT_AMT,
       l.PAST_DUE_AMT    AS LOAN_PAST_DUE_AMT,
       l.BUCKET          AS LOAN_BUCKET,

-- Income attributes
       i.INCOME_ID       AS INCOME_ID,
       i.REPORTING_DATE  AS INCOME_REPORTING_DATE,
       i.FIRST_JOB       AS INCOME_FIRST_JOB,
       i.INCOME          AS INCOME_AMOUNT,
       i.BUCKET          AS INCOME_BUCKET

-- Joining tables to get the latest record for each customer
-- The latest record is determined by the highest reporting date
FROM (SELECT CUSTOMER_ID,
             REPORTING_DATE,
             AGE,
             EDUCATION,
             BUCKET,
             ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY REPORTING_DATE DESC) as rn
      FROM client) AS c

         -- Joining income table to client table to get the latest record for each income
         LEFT JOIN (SELECT INCOME_ID,
                           CUSTOMER_ID,
                           REPORTING_DATE,
                           FIRST_JOB,
                           INCOME,
                           BUCKET,
                           ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY REPORTING_DATE DESC) as rn
                    FROM income) AS i ON c.CUSTOMER_ID = i.CUSTOMER_ID AND i.rn = 1

    -- Joining loan table to income table to get the latest record for each loan
         LEFT JOIN (SELECT LOAN_ID,
                           CUSTOMER_ID,
                           REPORTING_DATE,
                           INTODEFAULT,
                           INSTALLMENT_NM,
                           LOAN_AMT,
                           INSTALLMENT_AMT,
                           PAST_DUE_AMT,
                           BUCKET,
                           ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY REPORTING_DATE DESC) as rn
                    FROM loan) AS l ON c.CUSTOMER_ID = l.CUSTOMER_ID AND l.rn = 1

    -- Joining household table to income table to get the latest record for each income
         LEFT JOIN (SELECT HOUSEHOLD_ID,
                           INCOME_ID,
                           REPORTING_DATE,
                           MARRIED,
                           HOUSE_OWNER,
                           CHILD_NO,
                           HH_MEMBERS,
                           BUCKET,
                           ROW_NUMBER() OVER (PARTITION BY INCOME_ID ORDER BY REPORTING_DATE DESC) as rn
                    FROM household) AS h ON i.INCOME_ID = h.INCOME_ID AND h.rn = 1
WHERE c.rn = 1 -- Only get the latest record for each customer
;