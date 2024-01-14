drop table loan_bad_data;

CREATE TABLE loan_bad_data (
    LOAN_ID text,
    CUSTOMER_ID text,
    REPORTING_DATE text,
    INTODEFAULT text,
    INSTALLMENT_NM text,
    LOAN_AMT text,
    INSTALLMENT_AMT text,
    PAST_DUE_AMT text,
    BUCKET text,
    INVALID_COLUMNS text,
    CREATION_DTM timestamp
);

create index loan_bad_data_idx on loan_bad_data (CREATION_DTM);