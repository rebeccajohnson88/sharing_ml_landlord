-- create table within schema
-- and distkey and sortkey
DROP TABLE IF EXISTS dssg_clean.housinglitig_tomerge;
CREATE TABLE dssg_clean.housinglitig_tomerge 
AS

SELECT 
            bin_clean
            ,week_start 
            ,month_start
            ,count(*) AS housinglitig_count
            ,sum(housinglitig_tenantaction) AS housinglitig_tenantaction_count
            ,sum(housinglitig_heatwater) AS housinglitig_heatwater_count
            ,CASE WHEN count(*) > 0 THEN 1 ELSE 0 END AS housinglitig_any
            ,CASE WHEN sum(housinglitig_tenantaction) > 0 THEN 1 ELSE 0 END AS housinglitig_tenantaction_any
            ,CASE WHEN sum(housinglitig_heatwater) > 0 THEN 1 ELSE 0 END AS housinglitig_heatwater_any
            ,SUM(COUNT(*)) OVER(PARTITION BY bin_clean ORDER BY bin_clean, week_start ASC ROWS UNBOUNDED PRECEDING) AS housinglitig_ever_count
FROM 
        (SELECT *,
                CASE WHEN casetype LIKE 'Tenant Action%' THEN 1 ELSE 0 END AS housinglitig_tenantaction,
                CASE WHEN casetype LIKE 'Heat%' THEN 1 ELSE 0 END AS housinglitig_heatwater,
                date_trunc('week', date_clean) as week_start,
                date_TRUNC('month', date_clean) AS month_start
         FROM  
                -- convert from varchar to date-time format for date-- use the date when DOB query is pushed to open data
                -- convert from integer to varchar for bin
                    (SELECT *,
                        to_date(caseopendate, 'MM--DD-YYYY') as date_clean,
                        bin::varchar(5204) AS bin_clean
                    FROM raw.housingcourt_litigation
                    )  as cleandate
         ) cleandate2

-- aggregate by bin and start of week
GROUP BY bin_clean, week_start, month_start
ORDER BY bin_clean, week_start ASC;
