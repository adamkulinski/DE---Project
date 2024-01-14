DROP VIEW IF EXISTS common_invalid_columns;

CREATE VIEW common_invalid_columns AS
SELECT
    'Income' AS TableName,
    INVALID_COLUMNS,
    COUNT(*) AS Frequency
FROM income_bad_data
GROUP BY INVALID_COLUMNS

UNION ALL

SELECT
    'Household',
    INVALID_COLUMNS,
    COUNT(*)
FROM household_bad_data
GROUP BY INVALID_COLUMNS

UNION ALL

SELECT
    'Client',
    INVALID_COLUMNS,
    COUNT(*)
FROM client_bad_data
GROUP BY INVALID_COLUMNS

UNION ALL

SELECT
    'Loan',
    INVALID_COLUMNS,
    COUNT(*)
FROM loan_bad_data
GROUP BY INVALID_COLUMNS;
