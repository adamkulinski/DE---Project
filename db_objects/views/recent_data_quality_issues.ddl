DROP VIEW IF EXISTS recent_data_quality_issues;

CREATE VIEW recent_data_quality_issues AS
SELECT
    'Household' AS TableName,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'HOUSEHOLD_ID') > 0 THEN 1 ELSE 0 END) AS Invalid_Household_ID,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'INCOME_ID') > 0 THEN 1 ELSE 0 END) AS Invalid_Income_ID,
    NULL as Invalid_Customer_ID,
    NULL as Invalid_Loan_ID,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'REPORTING_DATE') > 0 THEN 1 ELSE 0 END) AS Invalid_Reporting_Date,
    NULL as Invalid_Age,
    NULL as Invalid_Education,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'MARRIED') > 0 THEN 1 ELSE 0 END) AS Invalid_Married,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'HOUSE_OWNER') > 0 THEN 1 ELSE 0 END) AS Invalid_House_Owner,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'CHILD_NO') > 0 THEN 1 ELSE 0 END) AS Invalid_Childs_NO,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'HH_MEMBERS') > 0 THEN 1 ELSE 0 END) AS Invalid_HH_Members,
    NULL as Invalid_First_Job,
    NULL as Invalid_Income,
    NULL as Invalid_IntoDefault,
    NULL as Invalid_Installment_NM,
    NULL as Invalid_Loan_Amt,
    NULL as Invalid_Installment_Amt,
    NULL as Invalid_Past_Due_Amt,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'BUCKET') > 0 THEN 1 ELSE 0 END) AS Invalid_Bucket
FROM household_bad_data
WHERE CREATION_DTM >= date('now', '-1 days')

UNION ALL

SELECT
    'Client' AS TableName,
    NULL as Invalid_Household_ID,
    NULL as Invalid_Income_ID,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'CUSTOMER_ID') > 0 THEN 1 ELSE 0 END) AS Invalid_Customer_ID,
    NULL as Invalid_Loan_ID,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'REPORTING_DATE') > 0 THEN 1 ELSE 0 END) AS Invalid_Reporting_Date,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'AGE') > 0 THEN 1 ELSE 0 END) AS Invalid_Age,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'EDUCATION') > 0 THEN 1 ELSE 0 END) AS Invalid_Education,
    NULL as Invalid_Married,
    NULL as Invalid_House_Owner,
    NULL as Invalid_Childs_NO,
    NULL as Invalid_HH_Members,
    NULL as Invalid_First_Job,
    NULL as Invalid_Income,
    NULL as Invalid_IntoDefault,
    NULL as Invalid_Installment_NM,
    NULL as Invalid_Loan_Amt,
    NULL as Invalid_Installment_Amt,
    NULL as Invalid_Past_Due_Amt,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'BUCKET') > 0 THEN 1 ELSE 0 END) AS Invalid_Bucket
FROM client_bad_data
WHERE CREATION_DTM >= date('now', '-1 days')

UNION ALL

SELECT
    'Income' AS TableName,
    NULL as Invalid_Household_ID,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'INCOME_ID') > 0 THEN 1 ELSE 0 END) AS Invalid_Income_ID,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'CUSTOMER_ID') > 0 THEN 1 ELSE 0 END) AS Invalid_Customer_ID,
    NULL as Invalid_Loan_ID,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'REPORTING_DATE') > 0 THEN 1 ELSE 0 END) AS Invalid_Reporting_Date,
    NULL as Invalid_Age,
    NULL as Invalid_Education,
    NULL as Invalid_Married,
    NULL as Invalid_House_Owner,
    NULL as Invalid_Childs_NO,
    NULL as Invalid_HH_Members,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'FIRST_JOB') > 0 THEN 1 ELSE 0 END) AS Invalid_First_Job,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'INCOME') > 0 THEN 1 ELSE 0 END) AS Invalid_Income,
    NULL as Invalid_IntoDefault,
    NULL as Invalid_Installment_NM,
    NULL as Invalid_Loan_Amt,
    NULL as Invalid_Installment_Amt,
    NULL as Invalid_Past_Due_Amt,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'BUCKET') > 0 THEN 1 ELSE 0 END) AS Invalid_Bucket
FROM income_bad_data
WHERE CREATION_DTM >= date('now', '-1 days')

UNION ALL

SELECT
    'Loan' AS TableName,
    NULL as Invalid_Household_ID,
    NULL as Invalid_Income_ID,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'CUSTOMER_ID') > 0 THEN 1 ELSE 0 END) AS Invalid_Customer_ID,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'LOAN_ID') > 0 THEN 1 ELSE 0 END) AS Invalid_Loan_ID,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'REPORTING_DATE') > 0 THEN 1 ELSE 0 END) AS Invalid_Reporting_Date,
    NULL as Invalid_Age,
    NULL as Invalid_Education,
    NULL as Invalid_Married,
    NULL as Invalid_House_Owner,
    NULL as Invalid_Childs_NO,
    NULL as Invalid_HH_Members,
    NULL as Invalid_First_Job,
    NULL as Invalid_Income,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'INTODEFAULT') > 0 THEN 1 ELSE 0 END) AS Invalid_IntoDefault,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'INSTALLMENT_NM') > 0 THEN 1 ELSE 0 END) AS Invalid_Installment_NM,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'LOAN_AMT') > 0 THEN 1 ELSE 0 END) AS Invalid_Loan_Amt,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'INSTALLMENT_AMT') > 0 THEN 1 ELSE 0 END) AS Invalid_Installment_Amt,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'PAST_DUE_AMT') > 0 THEN 1 ELSE 0 END) AS Invalid_Past_Due_Amt,
    SUM(CASE WHEN instr(INVALID_COLUMNS, 'BUCKET') > 0 THEN 1 ELSE 0 END) AS Invalid_Bucket
FROM loan_bad_data
WHERE CREATION_DTM >= date('now', '-1 days')
;
