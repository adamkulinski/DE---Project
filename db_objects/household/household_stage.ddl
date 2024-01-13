drop table household_stage;

CREATE TABLE household_stage (
    HOUSEHOLD_ID INTEGER NOT NULL,
    INCOME_ID INTEGER NOT NULL,
    REPORTING_DATE text,
    MARRIED text,
    HOUSE_OWNER text,
    CHILD_NO text,
    HH_MEMBERS text,
    BUCKET text,
    PRIMARY KEY (HOUSEHOLD_ID)
);
