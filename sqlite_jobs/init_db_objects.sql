DROP TABLE IF EXISTS client;

CREATE TABLE IF NOT EXISTS  client
(
    CUSTOMER_ID    integer not null,
    REPORTING_DATE date    not null,
    AGE            integer not null,
    EDUCATION      varchar not null,
    BUCKET         integer not null,

    PRIMARY KEY (CUSTOMER_ID, REPORTING_DATE)
);

-- indexes on frequently queried columns
CREATE INDEX  IF NOT EXISTS  idx_client_age ON Client (AGE);
CREATE INDEX  IF NOT EXISTS  idx_client_education ON Client (EDUCATION);
CREATE INDEX  IF NOT EXISTS  idx_client_customer_id ON Client (CUSTOMER_ID);

DROP TABLE IF EXISTS client_bad_data;

CREATE TABLE IF NOT EXISTS  client_bad_data (
    CUSTOMER_ID text,
    REPORTING_DATE text,
    AGE text,
    EDUCATION text,
    BUCKET text,
    INVALID_COLUMNS text,
    CREATION_DTM timestamp
);

CREATE INDEX  IF NOT EXISTS  client_bad_data_idx on client_bad_data (CREATION_DTM); -- for faster queries

DROP TABLE IF EXISTS client_stage;

CREATE TABLE IF NOT EXISTS  client_stage (
    CUSTOMER_ID text,
    REPORTING_DATE text,
    EDUCATION text,
    AGE text,
    BUCKET text
);


DROP TABLE IF EXISTS household;

CREATE TABLE IF NOT EXISTS  household
(
    HOUSEHOLD_ID   integer not null,
    INCOME_ID      integer not null,
    REPORTING_DATE date    not null,
    MARRIED        char    not null,
    HOUSE_OWNER    char    not null,
    CHILD_NO       integer not null,
    HH_MEMBERS     integer not null,
    BUCKET         integer not null,

    PRIMARY KEY (HOUSEHOLD_ID, REPORTING_DATE),
    FOREIGN KEY (INCOME_ID) REFERENCES income (INCOME_ID)
);

-- Index for.income_id
CREATE INDEX  IF NOT EXISTS  idx_household_customer ON Household (INCOME_ID);
CREATE INDEX  IF NOT EXISTS  idx_household_household_id ON Household (HOUSEHOLD_ID);


DROP TABLE IF EXISTS household_bad_data;

CREATE TABLE IF NOT EXISTS  household_bad_data (
    HOUSEHOLD_ID text,
    INCOME_ID text,
    REPORTING_DATE text,
    MARRIED text,
    HOUSE_OWNER text,
    CHILD_NO text,
    HH_MEMBERS text,
    BUCKET text,
    INVALID_COLUMNS text,
    CREATION_DTM timestamp
);

CREATE INDEX  IF NOT EXISTS  household_bad_data_idx on household_bad_data (CREATION_DTM); -- for faster queries


DROP TABLE IF EXISTS household_stage;

CREATE TABLE IF NOT EXISTS  household_stage (
    HOUSEHOLD_ID text,
    INCOME_ID text,
    REPORTING_DATE text,
    MARRIED text,
    HOUSE_OWNER text,
    CHILD_NO text,
    HH_MEMBERS text,
    BUCKET text
);

DROP TABLE IF EXISTS income;

CREATE TABLE IF NOT EXISTS  income
(
    INCOME_ID      integer not null,
    CUSTOMER_ID    integer not null,
    REPORTING_DATE date    not null,
    FIRST_JOB      char    not null,
    INCOME         integer not null,
    BUCKET         integer not null,

    PRIMARY KEY (INCOME_ID, REPORTING_DATE),
    FOREIGN KEY (CUSTOMER_ID) REFERENCES client (CUSTOMER_ID)
);

-- Index for customer ID
CREATE INDEX  IF NOT EXISTS  idx_income_customer_id ON income (CUSTOMER_ID);
CREATE INDEX  IF NOT EXISTS  idx_income_income_id ON income (INCOME_ID);

DROP TABLE IF EXISTS income_bad_data;

CREATE TABLE IF NOT EXISTS  income_bad_data (
    INCOME_ID text,
    CUSTOMER_ID text,
    REPORTING_DATE text,
    FIRST_JOB text,
    INCOME text,
    BUCKET text,
    INVALID_COLUMNS text,
    CREATION_DTM timestamp
);

CREATE INDEX  IF NOT EXISTS  income_bad_data_idx on income_bad_data (creation_dtm);

DROP TABLE IF EXISTS income_stage;

CREATE TABLE IF NOT EXISTS  income_stage (
    INCOME_ID text,
    CUSTOMER_ID text,
    REPORTING_DATE text,
    FIRST_JOB text,
    INCOME text,
    BUCKET text
);

DROP TABLE IF EXISTS loan;

CREATE TABLE IF NOT EXISTS  loan
(
    LOAN_ID         integer not null,
    CUSTOMER_ID     integer not null,
    REPORTING_DATE  date    not null,
    INTODEFAULT     char    not null,
    INSTALLMENT_NM  integer not null,
    LOAN_AMT        decimal not null,
    INSTALLMENT_AMT decimal not null,
    PAST_DUE_AMT    decimal not null,
    BUCKET          integer not null,

    PRIMARY KEY (LOAN_ID, REPORTING_DATE),
    FOREIGN KEY (CUSTOMER_ID) REFERENCES client (CUSTOMER_ID)
);

-- Index for customer_id
CREATE INDEX  IF NOT EXISTS  idx_loan_customer_id ON loan (CUSTOMER_ID);
CREATE INDEX  IF NOT EXISTS  idx_loan_loan_id ON loan (LOAN_ID);


DROP TABLE IF EXISTS loan_bad_data;

CREATE TABLE IF NOT EXISTS  loan_bad_data (
    LOAN_ID text,
    CUSTOMER_ID text,
    REPORTING_DATE text,
    INTODEFAULT text,
    INSTALLMENT_NM text,
    LOAN_AMT text,
    INSTALLMENT_AMT text,
    PAST_DUE_AMT text,
    BUCKET text,
    INVALID_COLUMNS text,
    CREATION_DTM timestamp
);

CREATE INDEX  IF NOT EXISTS  loan_bad_data_idx on loan_bad_data (CREATION_DTM);

DROP TABLE IF EXISTS loan_stage;

CREATE TABLE IF NOT EXISTS  loan_stage (
    LOAN_ID text,
    CUSTOMER_ID text,
    REPORTING_DATE text,
    INTODEFAULT text,
    INSTALLMENT_NM text,
    LOAN_AMT text,
    INSTALLMENT_AMT text,
    PAST_DUE_AMT text,
    BUCKET text
);


DROP TABLE IF EXISTS customer_car;

CREATE TABLE IF NOT EXISTS  customer_car
(
    CUSTOMER_ID              INTEGER NOT NULL,

    -- Attributes from Client table
    CLIENT_REPORTING_DATE    DATE,
    CLIENT_AGE               INTEGER,
    CLIENT_EDUCATION         VARCHAR,
    CLIENT_BUCKET            INTEGER,

    -- Attributes from Household table
    HOUSEHOLD_ID             INTEGER,
    HOUSEHOLD_INCOME_ID      INTEGER,
    HOUSEHOLD_REPORTING_DATE DATE,
    HOUSEHOLD_MARRIED        CHAR,
    HOUSEHOLD_HOUSE_OWNER    CHAR,
    HOUSEHOLD_CHILD_NO       INTEGER,
    HOUSEHOLD_HH_MEMBERS     INTEGER,
    HOUSEHOLD_BUCKET         INTEGER,

    -- Attributes from Loan table
    LOAN_ID                  INTEGER,
    LOAN_REPORTING_DATE      DATE,
    LOAN_INTODEFAULT         CHAR,
    LOAN_INSTALLMENT_NM      INTEGER,
    LOAN_AMT                 DECIMAL,
    LOAN_INSTALLMENT_AMT     DECIMAL,
    LOAN_PAST_DUE_AMT        DECIMAL,
    LOAN_BUCKET              INTEGER,

    -- Attributes from Income table
    INCOME_ID                INTEGER,
    INCOME_REPORTING_DATE    DATE,
    INCOME_FIRST_JOB         CHAR,
    INCOME_AMOUNT            INTEGER,
    INCOME_BUCKET            INTEGER,

    PRIMARY KEY (CUSTOMER_ID),
    FOREIGN KEY (CUSTOMER_ID) REFERENCES client (CUSTOMER_ID),
    FOREIGN KEY (HOUSEHOLD_ID) REFERENCES household (HOUSEHOLD_ID),
    FOREIGN KEY (LOAN_ID) REFERENCES loan (LOAN_ID),
    FOREIGN KEY (INCOME_ID) REFERENCES income (INCOME_ID)
);

-- Index for household, loan, income and customer IDs
CREATE INDEX  IF NOT EXISTS  idx_customer_car_household_id ON customer_car (HOUSEHOLD_ID);
CREATE INDEX  IF NOT EXISTS  idx_customer_car_loan_id ON customer_car (LOAN_ID);
CREATE INDEX  IF NOT EXISTS  idx_customer_car_income_id ON customer_car (INCOME_ID);

-- Index for reporting dates
CREATE INDEX  IF NOT EXISTS  idx_customer_car_customer_date ON customer_car (CLIENT_REPORTING_DATE);
CREATE INDEX  IF NOT EXISTS  idx_customer_car_household_date ON customer_car (HOUSEHOLD_REPORTING_DATE);
CREATE INDEX  IF NOT EXISTS  idx_customer_car_loan_date ON customer_car (LOAN_REPORTING_DATE);
CREATE INDEX  IF NOT EXISTS  idx_customer_car_income_date ON customer_car (INCOME_REPORTING_DATE);


DROP VIEW IF EXISTS common_invalid_columns;

CREATE VIEW IF NOT EXISTS  common_invalid_columns AS
SELECT
    'Income' AS TableName,
    INVALID_COLUMNS,
    COUNT(*) AS Frequency
FROM income_bad_data
GROUP BY INVALID_COLUMNS

UNION ALL

SELECT
    'Household',
    INVALID_COLUMNS,
    COUNT(*)
FROM household_bad_data
GROUP BY INVALID_COLUMNS

UNION ALL

SELECT
    'Client',
    INVALID_COLUMNS,
    COUNT(*)
FROM client_bad_data
GROUP BY INVALID_COLUMNS

UNION ALL

SELECT
    'Loan',
    INVALID_COLUMNS,
    COUNT(*)
FROM loan_bad_data
GROUP BY INVALID_COLUMNS;


DROP VIEW IF EXISTS data_quality_overview;

CREATE VIEW IF NOT EXISTS  data_quality_overview AS
SELECT
    'Income' AS TableName,
    COUNT(*) AS TotalInvalidRecords
FROM income_bad_data

UNION ALL

SELECT
    'Household',
    COUNT(*)
FROM household_bad_data

UNION ALL

SELECT
    'Client',
    COUNT(*)
FROM client_bad_data

UNION ALL

SELECT
    'Loan',
    COUNT(*)
FROM loan_bad_data;


DROP VIEW IF EXISTS data_quality_overview_today;

CREATE VIEW IF NOT EXISTS  data_quality_overview_today AS
SELECT
    'Income' AS TableName,
    COUNT(*) AS TotalInvalidRecords
FROM income_bad_data where DATE(CREATION_DTM) = current_date

UNION ALL

SELECT
    'Household',
    COUNT(*)
FROM household_bad_data where DATE(CREATION_DTM) = current_date

UNION ALL

SELECT
    'Client',
    COUNT(*)
FROM client_bad_data where DATE(CREATION_DTM) = current_date

UNION ALL

SELECT
    'Loan',
    COUNT(*)
FROM loan_bad_data where DATE(CREATION_DTM) = current_date;


DROP VIEW IF EXISTS DPD_bucket_report;

CREATE VIEW IF NOT EXISTS  DPD_bucket_report AS
with cte as (
    SELECT
    REPORTING_DATE,
    PAST_DUE_AMT,
    CASE
        WHEN PAST_DUE_AMT <= 100 THEN 0
        WHEN (julianday(MAX_REPORTING_DATE) - julianday(REPORTING_DATE)) < 30 THEN 0
        WHEN (julianday(MAX_REPORTING_DATE) - julianday(REPORTING_DATE)) >= 30 AND (julianday(MAX_REPORTING_DATE) - julianday(REPORTING_DATE)) < 60 THEN 1
        WHEN (julianday(MAX_REPORTING_DATE) - julianday(REPORTING_DATE)) >= 60 AND (julianday(MAX_REPORTING_DATE) - julianday(REPORTING_DATE)) < 90 THEN 2
        ELSE 3
    END AS DPD_BUCKET
FROM customer_car as c left join loan as l on c.LOAN_ID = l.LOAN_ID
JOIN (SELECT LOAN_ID, MAX(REPORTING_DATE) AS MAX_REPORTING_DATE FROM loan GROUP BY LOAN_ID) AS last_dates ON l.LOAN_ID = last_dates.LOAN_ID
WHERE PAST_DUE_AMT > 100
)
select REPORTING_DATE, DPD_BUCKET, CAST(SUM(PAST_DUE_AMT) as integer) AS SUMMED_PAST_DUE_AMT from cte group by REPORTING_DATE,DPD_BUCKET;
;


DROP VIEW IF EXISTS loan_dpd_view;

CREATE VIEW IF NOT EXISTS  loan_dpd_view AS
SELECT
    LOAN_ID,
    REPORTING_DATE,
    PAST_DUE_AMT,
    CASE
        WHEN PAST_DUE_AMT > 100 THEN julianday('now') - julianday(REPORTING_DATE)
        ELSE 0
    END AS Days_Past_Due,
    CASE
        WHEN PAST_DUE_AMT <= 100 THEN 0
        WHEN julianday('now') - julianday(REPORTING_DATE) < 30 THEN 0
        WHEN julianday('now') - julianday(REPORTING_DATE) >= 30 AND julianday('now') - julianday(REPORTING_DATE) < 60 THEN 1
        WHEN julianday('now') - julianday(REPORTING_DATE) >= 60 AND julianday('now') - julianday(REPORTING_DATE) < 90 THEN 2
        ELSE 3
    END AS DPD_Bucket
FROM loan;

DROP VIEW IF EXISTS recent_data_quality_issues;

CREATE VIEW IF NOT EXISTS  recent_data_quality_issues AS
SELECT
    'Household' AS TableName,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'HOUSEHOLD_ID') > 0 THEN 1 ELSE 0 END) AS Invalid_Household_ID,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'INCOME_ID') > 0 THEN 1 ELSE 0 END) AS Invalid_Income_ID,
    NULL as Invalid_Customer_ID,
    NULL as Invalid_Loan_ID,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'REPORTING_DATE') > 0 THEN 1 ELSE 0 END) AS Invalid_Reporting_Date,
    NULL as Invalid_Age,
    NULL as Invalid_Education,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'MARRIED') > 0 THEN 1 ELSE 0 END) AS Invalid_Married,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'HOUSE_OWNER') > 0 THEN 1 ELSE 0 END) AS Invalid_House_Owner,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'CHILD_NO') > 0 THEN 1 ELSE 0 END) AS Invalid_Childs_NO,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'HH_MEMBERS') > 0 THEN 1 ELSE 0 END) AS Invalid_HH_Members,
    NULL as Invalid_First_Job,
    NULL as Invalid_Income,
    NULL as Invalid_IntoDefault,
    NULL as Invalid_Installment_NM,
    NULL as Invalid_Loan_Amt,
    NULL as Invalid_Installment_Amt,
    NULL as Invalid_Past_Due_Amt,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'BUCKET') > 0 THEN 1 ELSE 0 END) AS Invalid_Bucket
FROM household_bad_data
WHERE CREATION_DTM >= date('now', '-1 days')

UNION ALL

SELECT
    'Client' AS TableName,
    NULL as Invalid_Household_ID,
    NULL as Invalid_Income_ID,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'CUSTOMER_ID') > 0 THEN 1 ELSE 0 END) AS Invalid_Customer_ID,
    NULL as Invalid_Loan_ID,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'REPORTING_DATE') > 0 THEN 1 ELSE 0 END) AS Invalid_Reporting_Date,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'AGE') > 0 THEN 1 ELSE 0 END) AS Invalid_Age,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'EDUCATION') > 0 THEN 1 ELSE 0 END) AS Invalid_Education,
    NULL as Invalid_Married,
    NULL as Invalid_House_Owner,
    NULL as Invalid_Childs_NO,
    NULL as Invalid_HH_Members,
    NULL as Invalid_First_Job,
    NULL as Invalid_Income,
    NULL as Invalid_IntoDefault,
    NULL as Invalid_Installment_NM,
    NULL as Invalid_Loan_Amt,
    NULL as Invalid_Installment_Amt,
    NULL as Invalid_Past_Due_Amt,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'BUCKET') > 0 THEN 1 ELSE 0 END) AS Invalid_Bucket
FROM client_bad_data
WHERE CREATION_DTM >= date('now', '-1 days')

UNION ALL

SELECT
    'Income' AS TableName,
    NULL as Invalid_Household_ID,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'INCOME_ID') > 0 THEN 1 ELSE 0 END) AS Invalid_Income_ID,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'CUSTOMER_ID') > 0 THEN 1 ELSE 0 END) AS Invalid_Customer_ID,
    NULL as Invalid_Loan_ID,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'REPORTING_DATE') > 0 THEN 1 ELSE 0 END) AS Invalid_Reporting_Date,
    NULL as Invalid_Age,
    NULL as Invalid_Education,
    NULL as Invalid_Married,
    NULL as Invalid_House_Owner,
    NULL as Invalid_Childs_NO,
    NULL as Invalid_HH_Members,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'FIRST_JOB') > 0 THEN 1 ELSE 0 END) AS Invalid_First_Job,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'INCOME') > 0 THEN 1 ELSE 0 END) AS Invalid_Income,
    NULL as Invalid_IntoDefault,
    NULL as Invalid_Installment_NM,
    NULL as Invalid_Loan_Amt,
    NULL as Invalid_Installment_Amt,
    NULL as Invalid_Past_Due_Amt,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'BUCKET') > 0 THEN 1 ELSE 0 END) AS Invalid_Bucket
FROM income_bad_data
WHERE CREATION_DTM >= date('now', '-1 days')

UNION ALL

SELECT
    'Loan' AS TableName,
    NULL as Invalid_Household_ID,
    NULL as Invalid_Income_ID,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'CUSTOMER_ID') > 0 THEN 1 ELSE 0 END) AS Invalid_Customer_ID,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'LOAN_ID') > 0 THEN 1 ELSE 0 END) AS Invalid_Loan_ID,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'REPORTING_DATE') > 0 THEN 1 ELSE 0 END) AS Invalid_Reporting_Date,
    NULL as Invalid_Age,
    NULL as Invalid_Education,
    NULL as Invalid_Married,
    NULL as Invalid_House_Owner,
    NULL as Invalid_Childs_NO,
    NULL as Invalid_HH_Members,
    NULL as Invalid_First_Job,
    NULL as Invalid_Income,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'INTODEFAULT') > 0 THEN 1 ELSE 0 END) AS Invalid_IntoDefault,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'INSTALLMENT_NM') > 0 THEN 1 ELSE 0 END) AS Invalid_Installment_NM,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'LOAN_AMT') > 0 THEN 1 ELSE 0 END) AS Invalid_Loan_Amt,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'INSTALLMENT_AMT') > 0 THEN 1 ELSE 0 END) AS Invalid_Installment_Amt,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'PAST_DUE_AMT') > 0 THEN 1 ELSE 0 END) AS Invalid_Past_Due_Amt,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'BUCKET') > 0 THEN 1 ELSE 0 END) AS Invalid_Bucket
FROM loan_bad_data
WHERE CREATION_DTM >= date('now', '-1 days')
;


DROP VIEW IF EXISTS Summary_Report;

CREATE VIEW IF NOT EXISTS  Summary_Report AS
SELECT
    l.REPORTING_DATE,
    SUM(l.LOAN_AMT) AS Total_Loan_Amount,
    SUM(l.PAST_DUE_AMT) AS Total_Past_Due_Amount,
    SUM(l.LOAN_AMT - l.PAST_DUE_AMT) AS Total_Paid_Loan_Amount
FROM
    customer_car as c LEFT JOIN LOAN as l ON c.LOAN_ID = l.LOAN_ID
WHERE
    l.REPORTING_DATE IN (
        SELECT DISTINCT REPORTING_DATE
        FROM loan
        ORDER BY loan.REPORTING_DATE DESC
        LIMIT 3
    )
GROUP BY
    l.REPORTING_DATE;

