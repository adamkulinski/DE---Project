drop table income;

CREATE TABLE income (
    INCOME_ID integer not null,
    CUSTOMER_ID integer not null,
    REPORTING_DATE date not null,
    FIRST_JOB char not null check (FIRST_JOB IN ('Y', 'N')),
    INCOME integer not null check (INCOME BETWEEN 1000 AND 30000),
    BUCKET integer not null,
    PRIMARY KEY (INCOME_ID)
);
