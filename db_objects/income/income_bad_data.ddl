drop table income_bad_data;

CREATE TABLE income_bad_data (
    INCOME_ID text,
    CUSTOMER_ID text,
    REPORTING_DATE text,
    FIRST_JOB text,
    INCOME text,
    BUCKET text,
    INVALID_COLUMNS text,
    CREATION_DTM timestamp
);

create index income_bad_data_idx on income_bad_data (creation_dtm);