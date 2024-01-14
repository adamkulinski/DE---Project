drop table income;

CREATE TABLE income (
    INCOME_ID integer not null,
    CUSTOMER_ID integer not null,
    REPORTING_DATE date not null,
    FIRST_JOB char not null,
    INCOME integer not null,
    BUCKET integer not null,
    FOREIGN KEY (CUSTOMER_ID) REFERENCES client(CUSTOMER_ID)
);

-- Index for customer ID
CREATE INDEX idx_income_customer_id ON income (CUSTOMER_ID);
CREATE INDEX idx_income_income_id ON income (INCOME_ID);
