drop table client;

CREATE TABLE client (
    CUSTOMER_ID integer not null,
    REPORTING_DATE date not null,
    AGE integer not null,
    EDUCATION varchar not null,
    BUCKET integer not null,

    PRIMARY KEY (CUSTOMER_ID, REPORTING_DATE)
);

-- indexes on frequently queried columns
CREATE INDEX idx_client_age ON Client (AGE);
CREATE INDEX idx_client_education ON Client (EDUCATION);
CREATE INDEX idx_client_customer_id ON Client (CUSTOMER_ID);
