DROP VIEW IF EXISTS DPD_bucket_report;

CREATE VIEW DPD_bucket_report AS
with cte as (
    SELECT
    REPORTING_DATE,
    PAST_DUE_AMT,
    CASE
        WHEN PAST_DUE_AMT <= 100 THEN 0
        WHEN (julianday(MAX_REPORTING_DATE) - julianday(REPORTING_DATE)) < 30 THEN 0
        WHEN (julianday(MAX_REPORTING_DATE) - julianday(REPORTING_DATE)) >= 30 AND (julianday(MAX_REPORTING_DATE) - julianday(REPORTING_DATE)) < 60 THEN 1
        WHEN (julianday(MAX_REPORTING_DATE) - julianday(REPORTING_DATE)) >= 60 AND (julianday(MAX_REPORTING_DATE) - julianday(REPORTING_DATE)) < 90 THEN 2
        ELSE 3
    END AS DPD_BUCKET
FROM customer_car as c left join loan as l on c.LOAN_ID = l.LOAN_ID
JOIN (SELECT LOAN_ID, MAX(REPORTING_DATE) AS MAX_REPORTING_DATE FROM loan GROUP BY LOAN_ID) AS last_dates ON l.LOAN_ID = last_dates.LOAN_ID
WHERE PAST_DUE_AMT > 100
)
select REPORTING_DATE, DPD_BUCKET, CAST(SUM(PAST_DUE_AMT) as integer) AS SUMMED_PAST_DUE_AMT from cte group by REPORTING_DATE,DPD_BUCKET;
;
