DROP VIEW IF EXISTS data_quality_overview_today;

CREATE VIEW data_quality_overview_today AS
SELECT
    'Income' AS TableName,
    COUNT(*) AS TotalInvalidRecords
FROM income_bad_data where DATE(CREATION_DTM) = current_date

UNION ALL

SELECT
    'Household',
    COUNT(*)
FROM household_bad_data where DATE(CREATION_DTM) = current_date

UNION ALL

SELECT
    'Client',
    COUNT(*)
FROM client_bad_data where DATE(CREATION_DTM) = current_date

UNION ALL

SELECT
    'Loan',
    COUNT(*)
FROM loan_bad_data where DATE(CREATION_DTM) = current_date;

