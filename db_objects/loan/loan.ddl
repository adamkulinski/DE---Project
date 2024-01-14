drop table loan;

CREATE TABLE loan (
    LOAN_ID integer not null,
    CUSTOMER_ID integer not null,
    REPORTING_DATE date not null,
    INTODEFAULT char not null check (INTODEFAULT IN ('Y', 'N')),
    INSTALLMENT_NM integer not null check (INSTALLMENT_NM BETWEEN 12 AND 72),
    LOAN_AMT decimal not null check (LOAN_AMT BETWEEN 0.5 AND 100000),
    INSTALLMENT_AMT decimal not null check (INSTALLMENT_AMT BETWEEN 10 AND 100000),
    PAST_DUE_AMT decimal not null check (PAST_DUE_AMT >= 0),
    BUCKET integer not null,
    FOREIGN KEY (CUSTOMER_ID) REFERENCES client(CUSTOMER_ID)
);

-- Index for customer_id
CREATE INDEX idx_loan_customer_id ON loan (CUSTOMER_ID);
CREATE INDEX idx_loan_loan_id ON loan (LOAN_ID);
