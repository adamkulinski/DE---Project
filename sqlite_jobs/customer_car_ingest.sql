INSERT INTO customer_car_target (CUSTOMER_ID,

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
                                 INCOME_BUCKET,

-- Audit columns
                                 START_DATE,
                                 END_DATE,
                                 IS_DELETED)
;

SELECT c.CUSTOMER_ID,
-- Client attributes
       c.REPORTING_DATE,
       c.AGE,
       c.EDUCATION,
       c.BUCKET,

-- Household attributes
       h.HOUSEHOLD_ID,
       h.INCOME_ID,
       h.REPORTING_DATE,
       h.MARRIED,
       h.HOUSE_OWNER,
       h.CHILD_NO,
       h.HH_MEMBERS,
       h.BUCKET,

-- Loan attributes
       l.LOAN_ID,
       l.REPORTING_DATE,
       l.INTODEFAULT,
       l.INSTALLMENT_NM,
       l.LOAN_AMT,
       l.INSTALLMENT_AMT,
       l.PAST_DUE_AMT,
       l.BUCKET,

-- Income attributes
       i.INCOME_ID,
       i.REPORTING_DATE,
       i.FIRST_JOB,
       i.INCOME,
       i.BUCKET,

-- Audit columns
       CURRENT_DATE as START_DATE, -- Assuming data ingestion date as START_DATE
       '9999-01-01' as END_DATE,   -- END_DATE is NULL for newly ingested records
       FALSE        as IS_DELETED  -- IS_DELETED is FALSE for new records
FROM client as c
         LEFT JOIN household AS h ON c.CUSTOMER_ID = h.INCOME_ID -- Assuming linking via INCOME_ID
         LEFT JOIN income AS i ON c.CUSTOMER_ID = i.CUSTOMER_ID
         LEFT JOIN loan AS l ON c.CUSTOMER_ID = l.CUSTOMER_ID;

