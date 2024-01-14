DROP VIEW IF EXISTS loan_dpd_view;

CREATE VIEW loan_dpd_view AS
SELECT
    LOAN_ID,
    REPORTING_DATE,
    PAST_DUE_AMT,
    CASE
        WHEN PAST_DUE_AMT > 100 THEN julianday('now') - julianday(REPORTING_DATE)
        ELSE 0
    END AS Days_Past_Due,
    CASE
        WHEN PAST_DUE_AMT <= 100 THEN 0
        WHEN julianday('now') - julianday(REPORTING_DATE) < 30 THEN 0
        WHEN julianday('now') - julianday(REPORTING_DATE) >= 30 AND julianday('now') - julianday(REPORTING_DATE) < 60 THEN 1
        WHEN julianday('now') - julianday(REPORTING_DATE) >= 60 AND julianday('now') - julianday(REPORTING_DATE) < 90 THEN 2
        ELSE 3
    END AS DPD_Bucket
FROM loan;
