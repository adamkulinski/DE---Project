DROP VIEW IF EXISTS data_quality_overview;

CREATE VIEW data_quality_overview AS
SELECT
    'Income' AS TableName,
    COUNT(*) AS TotalInvalidRecords
FROM income_bad_data

UNION ALL

SELECT
    'Household',
    COUNT(*)
FROM household_bad_data

UNION ALL

SELECT
    'Client',
    COUNT(*)
FROM client_bad_data

UNION ALL

SELECT
    'Loan',
    COUNT(*)
FROM loan_bad_data;
