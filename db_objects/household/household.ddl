drop table household;

CREATE TABLE household (
    household_id integer not null,
    INCOME_ID integer not null,
    REPORTING_DATE date not null,
    MARRIED char not null check (MARRIED IN ('Y', 'N')),
    HOUSE_OWNER char not null check (HOUSE_OWNER IN ('Y', 'N')),
    CHILD_NO integer not null check (CHILD_NO BETWEEN 0 AND 10),
    HH_MEMBERS integer not null check (HH_MEMBERS BETWEEN 1 AND 10),
    BUCKET integer not null,
    PRIMARY KEY (HOUSEHOLD_ID)
);
