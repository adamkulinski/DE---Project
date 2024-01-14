drop table household;

CREATE TABLE household (
    HOUSEHOLD_ID integer not null,
    INCOME_ID integer not null,
    REPORTING_DATE date not null,
    MARRIED char not null,
    HOUSE_OWNER char not null,
    CHILD_NO integer not null,
    HH_MEMBERS integer not null,
    BUCKET integer not null,
    FOREIGN KEY (INCOME_ID) REFERENCES income(INCOME_ID)
);

-- Index for.income_id
CREATE INDEX idx_household_customer ON Household (INCOME_ID);
CREATE INDEX idx_household_household_id ON Household (HOUSEHOLD_ID);
