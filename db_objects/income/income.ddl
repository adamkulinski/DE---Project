drop table income;

CREATE TABLE income (
    INCOME_ID integer not null,
    CUSTOMER_ID integer not null,
    REPORTING_DATE date not null,
    FIRST_JOB char not null check (FIRST_JOB IN ('Y', 'N')),
    INCOME integer not null check (INCOME BETWEEN 1000 AND 30000),
    BUCKET integer not null,
    PRIMARY KEY (INCOME_ID),
    FOREIGN KEY (CUSTOMER_ID) REFERENCES client(CUSTOMER_ID)
);

-- Index for customer ID
CREATE INDEX idx_income_customer_id ON income (CUSTOMER_ID);