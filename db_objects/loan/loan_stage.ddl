drop table loan_stage;

CREATE TABLE loan_stage (
    LOAN_ID text,
    CUSTOMER_ID text,
    REPORTING_DATE text,
    INTODEFAULT text,
    INSTALLMENT_NM text,
    LOAN_AMT text,
    INSTALLMENT_AMT text,
    PAST_DUE_AMT text,
    BUCKET text
);
