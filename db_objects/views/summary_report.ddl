DROP VIEW IF EXISTS Summary_Report;

CREATE VIEW Summary_Report AS
SELECT
    l.REPORTING_DATE,
    SUM(l.LOAN_AMT) AS Total_Loan_Amount,
    SUM(l.PAST_DUE_AMT) AS Total_Past_Due_Amount,
    SUM(l.LOAN_AMT - l.PAST_DUE_AMT) AS Total_Paid_Loan_Amount
FROM
    customer_car as c LEFT JOIN LOAN as l ON c.LOAN_ID = l.LOAN_ID
WHERE
    l.REPORTING_DATE IN (
        SELECT DISTINCT REPORTING_DATE
        FROM loan
        ORDER BY loan.REPORTING_DATE DESC
        LIMIT 3
    )
GROUP BY
    l.REPORTING_DATE;

