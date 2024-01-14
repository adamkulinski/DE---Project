drop table loan;

CREATE TABLE loan (
    LOAN_ID integer not null,
    CUSTOMER_ID integer not null,
    REPORTING_DATE date not null,
    INTODEFAULT char not null,
    INSTALLMENT_NM integer not null,
    LOAN_AMT decimal not null,
    INSTALLMENT_AMT decimal not null,
    PAST_DUE_AMT decimal not null,
    BUCKET integer not null,
    FOREIGN KEY (CUSTOMER_ID) REFERENCES client(CUSTOMER_ID)
);

-- Index for customer_id
CREATE INDEX idx_loan_customer_id ON loan (CUSTOMER_ID);
CREATE INDEX idx_loan_loan_id ON loan (LOAN_ID);
