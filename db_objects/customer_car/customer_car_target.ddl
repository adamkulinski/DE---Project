drop table customer_car_target;

CREATE TABLE customer_car_target (
    CUSTOMER_ID INTEGER NOT NULL,

    -- Attributes from Client table
    CLIENT_REPORTING_DATE DATE,
    CLIENT_AGE INTEGER,
    CLIENT_EDUCATION VARCHAR,
    CLIENT_BUCKET INTEGER,

    -- Attributes from Household table
    HOUSEHOLD_ID INTEGER,
    HOUSEHOLD_INCOME_ID INTEGER,
    HOUSEHOLD_REPORTING_DATE DATE,
    HOUSEHOLD_MARRIED CHAR,
    HOUSEHOLD_HOUSE_OWNER CHAR,
    HOUSEHOLD_CHILD_NO INTEGER,
    HOUSEHOLD_HH_MEMBERS INTEGER,
    HOUSEHOLD_BUCKET INTEGER,

    -- Attributes from Loan table
    LOAN_ID INTEGER,
    LOAN_REPORTING_DATE DATE,
    LOAN_INTODEFAULT CHAR,
    LOAN_INSTALLMENT_NM INTEGER,
    LOAN_AMT DECIMAL,
    LOAN_INSTALLMENT_AMT DECIMAL,
    LOAN_PAST_DUE_AMT DECIMAL,
    LOAN_BUCKET INTEGER,

    -- Attributes from Income table
    INCOME_ID INTEGER,
    INCOME_REPORTING_DATE DATE,
    INCOME_FIRST_JOB CHAR,
    INCOME_AMOUNT INTEGER,
    INCOME_BUCKET INTEGER,

    -- Audit columns
    START_DATE date,
    END_DATE date,
    IS_DELETED boolean,

    FOREIGN KEY (CUSTOMER_ID) REFERENCES client(CUSTOMER_ID),
    FOREIGN KEY (HOUSEHOLD_ID) REFERENCES household(HOUSEHOLD_ID),
    FOREIGN KEY (LOAN_ID) REFERENCES loan(LOAN_ID),
    FOREIGN KEY (INCOME_ID) REFERENCES income(INCOME_ID)
);

-- Index for household, loan, income and customer IDs
CREATE INDEX idx_customer_car_household_id ON customer_car_target (HOUSEHOLD_ID);
CREATE INDEX idx_customer_car_loan_id ON customer_car_target (LOAN_ID);
CREATE INDEX idx_customer_car_income_id ON customer_car_target (INCOME_ID);
CREATE INDEX idx_customer_car_customer_id ON customer_car_target (CUSTOMER_ID);
