drop table household_bad_data;

CREATE TABLE household_bad_data (
    HOUSEHOLD_ID text,
    INCOME_ID text,
    REPORTING_DATE text,
    MARRIED text,
    HOUSE_OWNER text,
    CHILD_NO text,
    HH_MEMBERS text,
    BUCKET text,
    INVALID_COLUMNS text,
    CREATION_DTM timestamp
);

create index household_bad_data_idx on household_bad_data (CREATION_DTM); -- for faster queries