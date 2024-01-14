drop table client;

CREATE TABLE client (
    CUSTOMER_ID integer not null,
    REPORTING_DATE date not null,
    AGE integer not null check (AGE >= 18 AND AGE <= 100),
    EDUCATION varchar not null check (EDUCATION IN ('Secondary', 'Elementary', 'Higher Education')),
    BUCKET integer not null
);

-- indexes on frequently queried columns
CREATE INDEX idx_client_age ON Client (AGE);
CREATE INDEX idx_client_education ON Client (EDUCATION);
CREATE INDEX idx_client_customer_id ON Client (CUSTOMER_ID);