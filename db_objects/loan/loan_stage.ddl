drop table loan_stage;

CREATE TABLE loan_stage (
    LOAN_ID integer not null,
    CUSTOMER_ID integer not null,
    REPORTING_DATE text,
    INTODEFAULT text,
    INSTALLMENT_NM text,
    LOAN_AMT text,
    INSTALLMENT_AMT text,
    PAST_DUE_AMT text,
    BUCKET text,
    PRIMARY KEY (LOAN_ID)
);
