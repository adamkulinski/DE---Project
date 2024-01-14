drop table client_bad_data;

CREATE TABLE client_bad_data (
    CUSTOMER_ID text,
    REPORTING_DATE text,
    AGE text,
    EDUCATION text,
    BUCKET text,
    INVALID_COLUMNS text,
    CREATION_DTM timestamp
);

create index client_bad_data_idx on client_bad_data (CREATION_DTM); -- for faster queries