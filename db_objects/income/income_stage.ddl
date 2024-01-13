drop table income_stage;

CREATE TABLE income_stage (
    INCOME_ID INTEGER NOT NULL,
    CUSTOMER_ID INTEGER NOT NULL,
    REPORTING_DATE text,
    FIRST_JOB text,
    INCOME text,
    BUCKET text,
    PRIMARY KEY (INCOME_ID)
);
