drop table customer_car;

CREATE TABLE customer_car (
    CUSTOMER_ID integer not null,

    -- Attributes from Client table
    CLIENT_REPORTING_DATE date,
    CLIENT_AGE integer,
    CLIENT_EDUCATION varchar,
    CLIENT_BUCKET integer,

    -- Attributes from Household table
    HOUSEHOLD_ID integer,
    HOUSEHOLD_INCOME_ID integer,
    HOUSEHOLD_REPORTING_DATE date,
    HOUSEHOLD_MARRIED char,
    HOUSEHOLD_HOUSE_OWNER char,
    HOUSEHOLD_CHILD_NO integer,
    HOUSEHOLD_HH_MEMBERS integer,
    HOUSEHOLD_BUCKET integer,

    -- Attributes from Loan table
    LOAN_ID integer,
    LOAN_REPORTING_DATE date,
    LOAN_INTODEFAULT char,
    LOAN_INSTALLMENT_NM integer,
    LOAN_AMT decimal,
    LOAN_INSTALLMENT_AMT decimal,
    LOAN_PAST_DUE_AMT decimal,
    LOAN_BUCKET integer,

    -- Attributes from Income table
    INCOME_ID integer,
    INCOME_REPORTING_DATE date,
    INCOME_FIRST_JOB char,
    INCOME_AMOUNT integer,
    INCOME_BUCKET integer,

    PRIMARY KEY (CUSTOMER_ID)
);
