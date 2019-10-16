set role nyc_peu_write;
CREATE TEMPORARY TABLE 
        --events_train_monthly_split_2015_06_01_2016_01_01
        events_train_monthly_split_{date_start_string}_{date_end_string}
        --DISTKEY(address_id)
        --SORTKEY(address_id, month_start)
        AS
        WITH
        months AS
            (
                -- create temp table months to call on later
                    WITH
                    digit AS
                    (
                     SELECT 0 AS D UNION ALL
                        SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL
                        SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL
                        SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
                    ),
                    seq AS (
                    SELECT a.d + (10 * b.d) + (100 * c.d) + (1000 * d.d) AS num
                    FROM digit a
                        CROSS JOIN
                        digit b
                        CROSS JOIN
                        digit c
                        CROSS JOIN
                        digit d
                    ORDER BY 1
                    )
                    SELECT
                             DATE_TRUNC('month', (now()::DATE - seq.num)::DATE)::DATE AS month_start
                            , DATE_TRUNC('month', (now()::DATE + interval '1 MONTH - 1 day'))::DATE AS month_end
                            , extract(month from (now()::DATE - seq.num)::DATE)::int AS "month"
                            , extract(year from (now()::DATE - seq.num)::DATE)::int AS "year"
                    FROM
                            seq
                    WHERE
                            (now()::DATE - seq.num)::DATE >= '{date_start}'  -- PEU initalized in approx June 2015 but we can go back further if we want
                    AND
                            (now()::DATE - seq.num)::DATE < '{date_end}'::DATE + interval '1 MONTH'  -- keep in an extra month so that labels are generated correctly
                    AND
                            extract(day from (now()::DATE - seq.num)::DATE) = 1 -- keep the first of each month
                    ORDER BY (now()::DATE - seq.num)::DATE desc
            )
        -- merge on bin_clean and month_start
        , hpd_violations AS
            (
            select
                        bin_clean
                        , DATE_TRUNC('month', month_start::DATE)::DATE AS month_start
                        , count(*) AS hpdviols_count
                        , SUM(COUNT(*)) OVER(PARTITION BY bin_clean ORDER BY bin_clean, month_start ASC ROWS UNBOUNDED PRECEDING) AS hpdviols_ever_count
                        , sum(hpdviols_count_classA) AS hpdviols_count_classA
                        , sum(hpdviols_count_classB) AS hpdviols_count_classB
                        , sum(hpdviols_count_classC) AS hpdviols_count_classC
                        , sum(hpdviols_count_classI) AS hpdviols_count_classI
                        , CASE WHEN count(*) > 0 THEN 1 ELSE 0 END AS hpdviols_any
                        , CASE WHEN sum(hpdviols_count_classA) > 0 THEN 1 ELSE 0 END AS hpdviols_any_classA
                        , CASE WHEN sum(hpdviols_count_classB) > 0 THEN 1 ELSE 0 END AS hpdviols_any_classB
                        , CASE WHEN sum(hpdviols_count_classC) > 0 THEN 1 ELSE 0 END AS hpdviols_any_classC
                        , CASE WHEN sum(hpdviols_count_classI) > 0 THEN 1 ELSE 0 END AS hpdviols_any_classI
            FROM        dssg_clean.hpdviolations_tomerge
            WHERE       month_start < '{date_end}'
            GROUP BY    bin_clean, month_start
            )
        -- merge on bin_clean and month_start
        , housing_litig AS
            (
            SELECT
                        bin_clean
                        ,  DATE_TRUNC('month', month_start::DATE)::DATE AS month_start
                        , count(*) AS housinglitig_count
                        , sum(housinglitig_tenantaction_count) AS housinglitig_tenantaction_count
                        , sum(housinglitig_heatwater_count) AS housinglitig_heatwater_count
                        , CASE WHEN count(*) > 0 THEN 1 ELSE 0 END AS housinglitig_any
                        , CASE WHEN sum(housinglitig_tenantaction_count) > 0 THEN 1 ELSE 0 END AS housinglitig_tenantaction_any
                        , CASE WHEN sum(housinglitig_heatwater_count) > 0 THEN 1 ELSE 0 END AS housinglitig_heatwater_any
                        , SUM(COUNT(*)) OVER(PARTITION BY bin_clean ORDER BY bin_clean, month_start ASC ROWS UNBOUNDED PRECEDING) AS housinglitig_ever_count
            FROM        dssg_clean.housinglitig_tomerge
            WHERE       month_start < '{date_end}'
            GROUP BY    bin_clean, month_start
            )
        -- merge on acs_tract_state_county_tract and year_tomerge_rounded
        , acs AS
            (SELECT
                    *
            FROM
                    dssg_clean.acs_long_tomerge
            )
        , canvass AS -- modified to subset to the training months in months_start arg
            (
                  SELECT
                  ccu.location_id
                  , ccu.month_start
                  , sum(ccu.knocks) AS internal_knocks_count
                  , CASE WHEN sum(ccu.knocks) > 0 THEN 1 ELSE 0 END AS internal_knocks_any
                  , sum(ccu.opens) AS internal_opens_count
                  , CASE WHEN sum(ccu.opens) > 0 THEN 1 ELSE 0 END AS internal_opens_any
                  , sum(ccu.cases) AS internal_cases_count
                  , CASE WHEN sum(ccu.cases) > 0 THEN 1 ELSE 0 END AS internal_cases_any
                  FROM
                  (SELECT
                          *
                          , DATE_TRUNC('month', datecanvassed)::DATE AS month_start
                          , CASE WHEN attempt__c = 'Yes' THEN 1 ELSE 0 END AS knocks
                          , CASE WHEN contact__c = 'Yes' THEN 1 ELSE 0 END AS opens
                          , CASE WHEN case__c = 'Yes' THEN 1 ELSE 0 END AS cases
                   FROM   dssg_clean.canvass_clean_unit
                   )ccu
                   WHERE       ccu.month_start < '{date_end}'::DATE + INTERVAL '1 MONTH'  
                   GROUP BY    ccu.location_id, ccu.month_start
            )
        , open_cases AS
            (
                 SELECT
                 c.month_start
                 ,c.address_id
                 ,COUNT(c.id) AS internal_cases_opened_thismonth_count
                 ,CASE WHEN count(c.id) > 0 THEN 1 ELSE 0 END AS internal_cases_opened_thismonth_any
                 ,SUM(c.legal) AS internalissue_cases_opened_count_legal
                 ,SUM(c.repair) AS internalissue_cases_opened_count_repair
                  ,SUM(c.service_access) AS internalissue_cases_opened_count_service_access
                  ,SUM(c.other) AS internalissue_cases_opened_count_other
                  -- create var for ever internal cases by summing all cases opened by each address id
                  ,SUM(COUNT(c.id)) OVER(PARTITION BY c.address_id ORDER BY c.address_id, c.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_cases_opened_ever_count
                  FROM
                  (SELECT
                      contacts.location_id__c AS address_id
                      , DATE_TRUNC('month', cases.date_of_intake__c)::DATE AS month_start
                      , cases.*
                      , CASE WHEN issues.legal_issues > 0 THEN 1 ELSE 0 END AS legal
                      , CASE WHEN issues.repair_issues > 0 THEN 1 ELSE 0 END AS repair
                      , CASE WHEN issues.service_access_issues > 0 THEN 1 ELSE 0 END AS service_access
                      , CASE WHEN issues.other_issues > 0 THEN 1 ELSE 0 END AS other
                  FROM
                      raw.case cases
                         -- join cases to contacts to get location_ids for each case
                  LEFT JOIN
                   (SELECT
                           id
                           ,location_id__c
                    FROM
                           raw.contact
                    ) contacts
                  ON cases.contactid = contacts.id
                  -- join issues to get case type
                  LEFT JOIN
                   (SELECT
                           case__c
                           ,COUNT(*) AS issues
                           ,SUM(CASE WHEN issue_type__c = 'Legal' THEN 1 ELSE 0 END) AS legal_issues
                           ,SUM(CASE WHEN issue_type__c = 'Repairs' THEN 1 ELSE 0 END) AS repair_issues
                           ,SUM(CASE WHEN issue_type__c = 'Service/Access' THEN 1 ELSE 0 END) AS service_access_issues
                           ,SUM(CASE WHEN issue_type__c = 'Other' THEN 1 ELSE 0 END) AS other_issues
                           ,SUM(CASE WHEN issue_outcome__c IN ('Resolved', 'Resolved by Legal', 'Enrolled') THEN 1 ELSE 0 END) AS resolved_issues
                           ,SUM(CASE WHEN issue_outcome__c NOT IN ('Resolved', 'Resolved by Legal', 'Enrolled') THEN 1 ELSE 0 END) AS unresolved_issues
                   FROM raw.issue
                        GROUP BY case__c) as issues
                        ON cases.id = issues.case__c
                  -- for labels, subset to only cases that originate from door knocks (6,002) as opposed to other pathways (referrals, community events, etc. = 2,000+)
                          WHERE       cases.origin = 'Door Knock'
                  ) as c
                 WHERE c.month_start < '{date_end}'::DATE + INTERVAL '1 MONTH' 
                 GROUP BY c.address_id, c.month_start
                 ORDER BY c.address_id, c.month_start ASC
            )
        , closed_cases AS
            (
                 SELECT
                 c.month_start
                 , c.address_id
                 , COUNT(c.id) AS internal_cases_closed_thismonth_count
                 , CASE WHEN count(c.id) > 0 THEN 1 ELSE 0 END AS internal_cases_closed_thismonth_any
                 , SUM(COUNT(c.id)) OVER(PARTITION BY c.address_id ORDER BY c.address_id, c.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_cases_closed_ever_count
                 , SUM(c.nya) AS internal_nya_cases_closed_thismonth_count
                 , SUM(SUM(c.nya)) OVER(PARTITION BY c.address_id ORDER BY c.address_id, c.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_nya_cases_closed_ever_count
                  FROM
                 (SELECT
                      contacts.location_id__c AS address_id
                      , DATE_TRUNC('month', case_closed_date__c)::DATE AS month_start
                      , cases.*
                 FROM
                      raw.case cases
                 -- join cases to contacts to get location_ids for each case
                 LEFT JOIN
                  (SELECT
                       id
                       ,location_id__c
                   FROM
                       raw.contact
                   ) as contacts
                 ON cases.contactid = contacts.id
                 WHERE cases.status = 'Closed'
                 ) as c
                 WHERE c.month_start < '{date_end}'
                 GROUP BY c.address_id, c.month_start
                 ORDER BY c.address_id, c.month_start ASC
            )
        , followups AS
            (
                   SELECT
                          followups.month_start
                          , followups.address_id
                          , COUNT(followups.case_id) AS internalfollow_all_followups_count
                          , CASE WHEN COUNT(followups.case_id) > 0 THEN 1 ELSE 0 END AS internalfollow_all_followups_any
                   FROM
                          (SELECT
                                      case_followup__c AS case_id
                                      , DATE_TRUNC('month', timestamp)::DATE AS month_start
                                      , location_id__c AS address_id
                           FROM
                                      raw.followups f
                           LEFT JOIN
                                      raw.CASE ca
                           ON
                                      ca.id = f.case_followup__c
                           LEFT JOIN
                                      raw.contact co
                           ON
                                      co.id = ca.contactid
                           ) followups
                   WHERE followups.month_start < '{date_end}'
                   GROUP BY followups.address_id, followups.month_start
             )
        , specialists AS
            (
                      SELECT
                            address_id
                            ,month_start
                            ,CASE WHEN SUM(sp_005t0000000ogcPAAQ) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcPAAQ_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogcIAAQ) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcIAAQ_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogcBAAQ) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcBAAQ_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogc7AAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogc7AAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogcDAAQ) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcDAAQ_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogcJAAQ) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcJAAQ_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogcHAAQ) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcHAAQ_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogcOAAQ) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcOAAQ_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogbYAAQ) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbYAAQ_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogbcAAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbcAAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogbaAAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbaAAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogbyAAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbyAAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogbnAAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbnAAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogc4AAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogc4AAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogcUAAQ) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcUAAQ_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogc3AAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogc3AAA_thismonth
                            ,CASE WHEN SUM(sp_005t00000014JjiAAE) > 0 THEN 1 ELSE 0 END AS sp_005t00000014JjiAAE_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogcEAAQ) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcEAAQ_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogc9AAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogc9AAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogcCAAQ) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcCAAQ_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogbZAAQ) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbZAAQ_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogbwAAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbwAAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogcSAAQ) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcSAAQ_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogbxAAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbxAAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogc0AAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogc0AAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogblAAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogblAAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogbfAAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbfAAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogc2AAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogc2AAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogbkAAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbkAAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogbuAAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbuAAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogbsAAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbsAAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogbpAAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbpAAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000001hsSUAAY) > 0 THEN 1 ELSE 0 END AS sp_005t0000001hsSUAAY_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogc6AAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogc6AAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogbdAAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbdAAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogcAAAQ) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcAAAQ_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogcKAAQ) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcKAAQ_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogc5AAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogc5AAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogboAAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogboAAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogbjAAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbjAAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogbzAAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbzAAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogbiAAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbiAAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000owxxAAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000owxxAAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogbXAAQ) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbXAAQ_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogbbAAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbbAAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogcGAAQ) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcGAAQ_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogcFAAQ) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcFAAQ_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogcQAAQ) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcQAAQ_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogbvAAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbvAAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogbmAAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbmAAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogbtAAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbtAAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogbhAAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbhAAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogbrAAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbrAAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogbqAAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbqAAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogc1AAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogc1AAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogbeAAA) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbeAAA_thismonth
                            ,CASE WHEN SUM(sp_005t0000000ogcLAAQ) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcLAAQ_thismonth
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogcPAAQ) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcPAAQ_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogcIAAQ) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcIAAQ_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogcBAAQ) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcBAAQ_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogc7AAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogc7AAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogcDAAQ) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcDAAQ_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogcJAAQ) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcJAAQ_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogcHAAQ) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcHAAQ_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogcOAAQ) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcOAAQ_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogbYAAQ) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbYAAQ_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogbcAAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbcAAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogbaAAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbaAAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogbyAAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbyAAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogbnAAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbnAAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogc4AAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogc4AAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogcUAAQ) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcUAAQ_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogc3AAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogc3AAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t00000014JjiAAE) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t00000014JjiAAE_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogcEAAQ) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcEAAQ_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogc9AAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogc9AAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogcCAAQ) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcCAAQ_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogbZAAQ) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbZAAQ_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogbwAAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbwAAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogcSAAQ) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcSAAQ_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogbxAAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbxAAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogc0AAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogc0AAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogblAAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogblAAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogbfAAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbfAAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogc2AAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogc2AAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogbkAAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbkAAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogbuAAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbuAAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogbsAAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbsAAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogbpAAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbpAAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000001hsSUAAY) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000001hsSUAAY_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogc6AAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogc6AAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogbdAAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbdAAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogcAAAQ) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcAAAQ_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogcKAAQ) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcKAAQ_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogc5AAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogc5AAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogboAAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogboAAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogbjAAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbjAAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogbzAAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbzAAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogbiAAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbiAAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000owxxAAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000owxxAAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogbXAAQ) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbXAAQ_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogbbAAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbbAAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogcGAAQ) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcGAAQ_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogcFAAQ) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcFAAQ_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogcQAAQ) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcQAAQ_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogbvAAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbvAAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogbmAAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbmAAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogbtAAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbtAAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogbhAAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbhAAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogbrAAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbrAAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogbqAAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbqAAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogc1AAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogc1AAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogbeAAA) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogbeAAA_ever
                            ,CASE WHEN SUM(CASE WHEN SUM(sp_005t0000000ogcLAAQ) > 0 THEN 1 ELSE 0 END) OVER(PARTITION BY address_id ORDER BY address_id, month_start ASC ROWS UNBOUNDED PRECEDING) > 0 THEN 1 ELSE 0 END AS sp_005t0000000ogcLAAQ_ever
                        FROM
                            (SELECT
                                    *
                                    , DATE_TRUNC('month', date_of_intake__c)::DATE AS month_start
                                    ,CASE WHEN user_id = '005t0000000ogcPAAQ' THEN 1 ELSE 0 END AS sp_005t0000000ogcPAAQ
                                    ,CASE WHEN user_id = '005t0000000ogcIAAQ' THEN 1 ELSE 0 END AS sp_005t0000000ogcIAAQ
                                    ,CASE WHEN user_id = '005t0000000ogcBAAQ' THEN 1 ELSE 0 END AS sp_005t0000000ogcBAAQ
                                    ,CASE WHEN user_id = '005t0000000ogc7AAA' THEN 1 ELSE 0 END AS sp_005t0000000ogc7AAA
                                    ,CASE WHEN user_id = '005t0000000ogcDAAQ' THEN 1 ELSE 0 END AS sp_005t0000000ogcDAAQ
                                    ,CASE WHEN user_id = '005t0000000ogcJAAQ' THEN 1 ELSE 0 END AS sp_005t0000000ogcJAAQ
                                    ,CASE WHEN user_id = '005t0000000ogcHAAQ' THEN 1 ELSE 0 END AS sp_005t0000000ogcHAAQ
                                    ,CASE WHEN user_id = '005t0000000ogcOAAQ' THEN 1 ELSE 0 END AS sp_005t0000000ogcOAAQ
                                    ,CASE WHEN user_id = '005t0000000ogbYAAQ' THEN 1 ELSE 0 END AS sp_005t0000000ogbYAAQ
                                    ,CASE WHEN user_id = '005t0000000ogbcAAA' THEN 1 ELSE 0 END AS sp_005t0000000ogbcAAA
                                    ,CASE WHEN user_id = '005t0000000ogbaAAA' THEN 1 ELSE 0 END AS sp_005t0000000ogbaAAA
                                    ,CASE WHEN user_id = '005t0000000ogbyAAA' THEN 1 ELSE 0 END AS sp_005t0000000ogbyAAA
                                    ,CASE WHEN user_id = '005t0000000ogbnAAA' THEN 1 ELSE 0 END AS sp_005t0000000ogbnAAA
                                    ,CASE WHEN user_id = '005t0000000ogc4AAA' THEN 1 ELSE 0 END AS sp_005t0000000ogc4AAA
                                    ,CASE WHEN user_id = '005t0000000ogcUAAQ' THEN 1 ELSE 0 END AS sp_005t0000000ogcUAAQ
                                    ,CASE WHEN user_id = '005t0000000ogc3AAA' THEN 1 ELSE 0 END AS sp_005t0000000ogc3AAA
                                    ,CASE WHEN user_id = '005t00000014JjiAAE' THEN 1 ELSE 0 END AS sp_005t00000014JjiAAE
                                    ,CASE WHEN user_id = '005t0000000ogcEAAQ' THEN 1 ELSE 0 END AS sp_005t0000000ogcEAAQ
                                    ,CASE WHEN user_id = '005t0000000ogc9AAA' THEN 1 ELSE 0 END AS sp_005t0000000ogc9AAA
                                    ,CASE WHEN user_id = '005t0000000ogcCAAQ' THEN 1 ELSE 0 END AS sp_005t0000000ogcCAAQ
                                    ,CASE WHEN user_id = '005t0000000ogbZAAQ' THEN 1 ELSE 0 END AS sp_005t0000000ogbZAAQ
                                    ,CASE WHEN user_id = '005t0000000ogbwAAA' THEN 1 ELSE 0 END AS sp_005t0000000ogbwAAA
                                    ,CASE WHEN user_id = '005t0000000ogcSAAQ' THEN 1 ELSE 0 END AS sp_005t0000000ogcSAAQ
                                    ,CASE WHEN user_id = '005t0000000ogbxAAA' THEN 1 ELSE 0 END AS sp_005t0000000ogbxAAA
                                    ,CASE WHEN user_id = '005t0000000ogc0AAA' THEN 1 ELSE 0 END AS sp_005t0000000ogc0AAA
                                    ,CASE WHEN user_id = '005t0000000ogblAAA' THEN 1 ELSE 0 END AS sp_005t0000000ogblAAA
                                    ,CASE WHEN user_id = '005t0000000ogbfAAA' THEN 1 ELSE 0 END AS sp_005t0000000ogbfAAA
                                    ,CASE WHEN user_id = '005t0000000ogc2AAA' THEN 1 ELSE 0 END AS sp_005t0000000ogc2AAA
                                    ,CASE WHEN user_id = '005t0000000ogbkAAA' THEN 1 ELSE 0 END AS sp_005t0000000ogbkAAA
                                    ,CASE WHEN user_id = '005t0000000ogbuAAA' THEN 1 ELSE 0 END AS sp_005t0000000ogbuAAA
                                    ,CASE WHEN user_id = '005t0000000ogbsAAA' THEN 1 ELSE 0 END AS sp_005t0000000ogbsAAA
                                    ,CASE WHEN user_id = '005t0000000ogbpAAA' THEN 1 ELSE 0 END AS sp_005t0000000ogbpAAA
                                    ,CASE WHEN user_id = '005t0000001hsSUAAY' THEN 1 ELSE 0 END AS sp_005t0000001hsSUAAY
                                    ,CASE WHEN user_id = '005t0000000ogc6AAA' THEN 1 ELSE 0 END AS sp_005t0000000ogc6AAA
                                    ,CASE WHEN user_id = '005t0000000ogbdAAA' THEN 1 ELSE 0 END AS sp_005t0000000ogbdAAA
                                    ,CASE WHEN user_id = '005t0000000ogcAAAQ' THEN 1 ELSE 0 END AS sp_005t0000000ogcAAAQ
                                    ,CASE WHEN user_id = '005t0000000ogcKAAQ' THEN 1 ELSE 0 END AS sp_005t0000000ogcKAAQ
                                    ,CASE WHEN user_id = '005t0000000ogc5AAA' THEN 1 ELSE 0 END AS sp_005t0000000ogc5AAA
                                    ,CASE WHEN user_id = '005t0000000ogboAAA' THEN 1 ELSE 0 END AS sp_005t0000000ogboAAA
                                    ,CASE WHEN user_id = '005t0000000ogbjAAA' THEN 1 ELSE 0 END AS sp_005t0000000ogbjAAA
                                    ,CASE WHEN user_id = '005t0000000ogbzAAA' THEN 1 ELSE 0 END AS sp_005t0000000ogbzAAA
                                    ,CASE WHEN user_id = '005t0000000ogbiAAA' THEN 1 ELSE 0 END AS sp_005t0000000ogbiAAA
                                    ,CASE WHEN user_id = '005t0000000owxxAAA' THEN 1 ELSE 0 END AS sp_005t0000000owxxAAA
                                    ,CASE WHEN user_id = '005t0000000ogbXAAQ' THEN 1 ELSE 0 END AS sp_005t0000000ogbXAAQ
                                    ,CASE WHEN user_id = '005t0000000ogbbAAA' THEN 1 ELSE 0 END AS sp_005t0000000ogbbAAA
                                    ,CASE WHEN user_id = '005t0000000ogcGAAQ' THEN 1 ELSE 0 END AS sp_005t0000000ogcGAAQ
                                    ,CASE WHEN user_id = '005t0000000ogcFAAQ' THEN 1 ELSE 0 END AS sp_005t0000000ogcFAAQ
                                    ,CASE WHEN user_id = '005t0000000ogcQAAQ' THEN 1 ELSE 0 END AS sp_005t0000000ogcQAAQ
                                    ,CASE WHEN user_id = '005t0000000ogbvAAA' THEN 1 ELSE 0 END AS sp_005t0000000ogbvAAA
                                    ,CASE WHEN user_id = '005t0000000ogbmAAA' THEN 1 ELSE 0 END AS sp_005t0000000ogbmAAA
                                    ,CASE WHEN user_id = '005t0000000ogbtAAA' THEN 1 ELSE 0 END AS sp_005t0000000ogbtAAA
                                    ,CASE WHEN user_id = '005t0000000ogbhAAA' THEN 1 ELSE 0 END AS sp_005t0000000ogbhAAA
                                    ,CASE WHEN user_id = '005t0000000ogbrAAA' THEN 1 ELSE 0 END AS sp_005t0000000ogbrAAA
                                    ,CASE WHEN user_id = '005t0000000ogbqAAA' THEN 1 ELSE 0 END AS sp_005t0000000ogbqAAA
                                    ,CASE WHEN user_id = '005t0000000ogc1AAA' THEN 1 ELSE 0 END AS sp_005t0000000ogc1AAA
                                    ,CASE WHEN user_id = '005t0000000ogbeAAA' THEN 1 ELSE 0 END AS sp_005t0000000ogbeAAA
                                    ,CASE WHEN user_id = '005t0000000ogcLAAQ' THEN 1 ELSE 0 END AS sp_005t0000000ogcLAAQ
                            FROM
                                    raw.CASE cases
                            LEFT JOIN
                                    (SELECT
                                            id
                                            ,location_id__c AS address_id
                                     FROM
                                            raw.contact
                                     ) contacts
                            ON cases.contactid = contacts.id
                            WHERE   cases.origin = 'Door Knock'
                            ) as specialist_first
            WHERE       specialist_first.month_start < '{date_end}'
            GROUP BY    specialist_first.address_id, specialist_first.month_start
            )
            
----------------------------------------------------------------------------------------------------------------------------------------
            
        SELECT
                      a.month_start
                    , a.month_end
                    , a.month
                    , a.year
                    , a.address_id
                    ---- nyc heating season flag (october 1 to may 31)
                    , CASE WHEN a.MONTH IN (10, 11, 12, 1, 2, 3, 4, 5) THEN 1 ELSE 0 END AS nyc_heating_season
                    ---- time varying tsu target zip flag
                    , CASE WHEN month_first_canvassed IS NULL THEN 0
                      WHEN month_first_canvassed IS NOT NULL AND a.month_start < month_first_canvassed THEN 0 ELSE 1
                      END AS internal_peu_target_zip
                ------------------------------------------------------------------- EXTERNAL FEATURES -------------------------------------------------------------------------------
                    ---- acs 2015 and 2016
                    , acs.*  --SHOULD comment back when we have acs data
                    ---- hpd violations
                    , CASE WHEN hpd_violations.hpdviols_count IS NULL THEN 0 ELSE hpd_violations.hpdviols_count END AS hpdviols_count_this_month
                    , CASE WHEN hpd_violations.hpdviols_count_classa IS NULL THEN 0 ELSE hpd_violations.hpdviols_count_classa END AS hpdviols_count_classa_this_month
                    , CASE WHEN hpd_violations.hpdviols_count_classb IS NULL THEN 0 ELSE hpd_violations.hpdviols_count_classb END AS hpdviols_count_classb_this_month
                    , CASE WHEN hpd_violations.hpdviols_count_classc IS NULL THEN 0 ELSE hpd_violations.hpdviols_count_classc END AS hpdviols_count_classc_this_month
                    , CASE WHEN hpd_violations.hpdviols_count_classi IS NULL THEN 0 ELSE hpd_violations.hpdviols_count_classi END AS hpdviols_count_classi_this_month
                    , CASE WHEN hpd_violations.hpdviols_any IS NULL THEN 0 ELSE hpd_violations.hpdviols_count END AS hpdviols_any_this_month
                    , CASE WHEN hpd_violations.hpdviols_any_classa IS NULL THEN 0 ELSE hpd_violations.hpdviols_any_classa END AS hpdviols_any_classa_this_month
                    , CASE WHEN hpd_violations.hpdviols_any_classb IS NULL THEN 0 ELSE hpd_violations.hpdviols_any_classb END AS hpdviols_any_classb_this_month
                    , CASE WHEN hpd_violations.hpdviols_any_classc IS NULL THEN 0 ELSE hpd_violations.hpdviols_any_classc END AS hpdviols_any_classc_this_month
                    , CASE WHEN hpd_violations.hpdviols_any_classi IS NULL THEN 0 ELSE hpd_violations.hpdviols_any_classi END AS hpdviols_any_classi_this_month
                    , CASE WHEN LAST_VALUE(hpd_violations.hpdviols_ever_count) 
                      OVER(PARTITION BY a.internal_bin ORDER BY case when hpd_violations.hpdviols_ever_count is not null then 1 else 0 end ASC,a.internal_bin, a.month_start ASC ROWS UNBOUNDED PRECEDING) IS NULL THEN 0 
                      ELSE LAST_VALUE(hpd_violations.hpdviols_ever_count) 
                      OVER(PARTITION BY a.internal_bin ORDER BY case when hpd_violations.hpdviols_ever_count is not null then 1 else 0 end ASC,a.internal_bin, a.month_start ASC ROWS UNBOUNDED PRECEDING) 
                      END AS hpdviols_count_ever
                    --, CASE WHEN LAST_VALUE(hpd_violations.hpdviols_ever_classa_count IGNORE NULLS) OVER(PARTITION BY a.internal_bin ORDER BY a.internal_bin, a.month_start ASC ROWS UNBOUNDED PRECEDING) IS NULL THEN 0 ELSE LAST_VALUE(hpd_violations.hpdviols_ever_classa_count IGNORE NULLS) OVER(PARTITION BY a.internal_bin ORDER BY a.internal_bin, a.month_start ASC ROWS UNBOUNDED PRECEDING) END AS hpdviols_ever_classa_count
                    --, CASE WHEN LAST_VALUE(hpd_violations.hpdviols_ever_classb_count IGNORE NULLS) OVER(PARTITION BY a.internal_bin ORDER BY a.internal_bin, a.month_start ASC ROWS UNBOUNDED PRECEDING) IS NULL THEN 0 ELSE LAST_VALUE(hpd_violations.hpdviols_ever_classb_count IGNORE NULLS) OVER(PARTITION BY a.internal_bin ORDER BY a.internal_bin, a.month_start ASC ROWS UNBOUNDED PRECEDING) END AS hpdviols_ever_classb_count
                    --, CASE WHEN LAST_VALUE(hpd_violations.hpdviols_ever_classc_count IGNORE NULLS) OVER(PARTITION BY a.internal_bin ORDER BY a.internal_bin, a.month_start ASC ROWS UNBOUNDED PRECEDING) IS NULL THEN 0 ELSE LAST_VALUE(hpd_violations.hpdviols_ever_classc_count IGNORE NULLS) OVER(PARTITION BY a.internal_bin ORDER BY a.internal_bin, a.month_start ASC ROWS UNBOUNDED PRECEDING) END AS hpdviols_ever_classc_count
                    ---- housing litigation
                    , CASE WHEN housing_litig.housinglitig_count IS NULL THEN 0 ELSE housing_litig.housinglitig_count END AS housinglitig_count_this_month
                    , CASE WHEN housing_litig.housinglitig_tenantaction_count IS NULL THEN 0 ELSE housing_litig.housinglitig_tenantaction_count END AS housinglitig_tenantaction_count_this_month
                    , CASE WHEN housing_litig.housinglitig_heatwater_count IS NULL THEN 0 ELSE housing_litig.housinglitig_heatwater_count END AS housinglitig_heatwater_count_this_month
                    , CASE WHEN housing_litig.housinglitig_any IS NULL THEN 0 ELSE housing_litig.housinglitig_any END AS housinglitig_any_this_month
                    , CASE WHEN housing_litig.housinglitig_tenantaction_any IS NULL THEN 0 ELSE housing_litig.housinglitig_tenantaction_any END AS housinglitig_tenantaction_any_this_month
                    , CASE WHEN housing_litig.housinglitig_heatwater_any IS NULL THEN 0 ELSE housing_litig.housinglitig_heatwater_any END AS housinglitig_heatwater_any_this_month
                    , CASE WHEN LAST_VALUE(housing_litig.housinglitig_ever_count) 
                      OVER(PARTITION BY a.internal_bin ORDER BY case when housing_litig.housinglitig_ever_count is not null then 1 else 0 end ASC,a.internal_bin, a.month_start ASC ROWS UNBOUNDED PRECEDING) IS NULL THEN 0 
                      ELSE LAST_VALUE(housing_litig.housinglitig_ever_count) 
                      OVER(PARTITION BY a.internal_bin ORDER BY  case when housing_litig.housinglitig_ever_count is not null then 1 else 0 end ASC,a.internal_bin, a.month_start ASC ROWS UNBOUNDED PRECEDING) 
                      END AS housinglitig_count_ever
                ------------------------------------------------------------------- LABELS  -------------------------------------------------------------------------------
                    -- knocks
                    , CASE WHEN LEAD(canvass.internal_knocks_count, 1) OVER(PARTITION BY a.address_id ORDER BY a.address_id, a.month_start ASC) IS NULL THEN 0 ELSE LEAD(canvass.internal_knocks_count, 1) OVER(PARTITION BY a.address_id ORDER BY a.address_id, a.month_start ASC) END AS internal_knocks_count_next_month
                    , CASE WHEN LEAD(canvass.internal_knocks_any, 1) OVER(PARTITION BY a.address_id ORDER BY a.address_id, a.month_start ASC) IS NULL THEN 0 ELSE LEAD(canvass.internal_knocks_any, 1) OVER(PARTITION BY a.address_id ORDER BY a.address_id, a.month_start ASC) END AS internal_knocks_any_next_month
                    -- opens
                    , LEAD(canvass.internal_opens_count, 1) OVER(PARTITION BY a.address_id ORDER BY a.address_id, a.month_start ASC) AS internal_opens_count_next_month
                    , LEAD(canvass.internal_opens_any, 1) OVER(PARTITION BY a.address_id ORDER BY a.address_id, a.month_start ASC) AS internal_opens_any_next_month
                    -- cases opened
                    , LEAD(CASE WHEN canvass.internal_opens_any IS NULL OR canvass.internal_opens_any = 0 THEN NULL
                                WHEN canvass.internal_knocks_any = 1 AND canvass.internal_opens_any = 1 AND open_cases.internal_cases_opened_thismonth_count IS NULL THEN 0
                                ELSE open_cases.internal_cases_opened_thismonth_count END, 1)
                      OVER(PARTITION BY a.address_id ORDER BY a.address_id, a.month_start ASC) AS internal_cases_opened_count_next_month
                    , LEAD(CASE WHEN canvass.internal_opens_any IS NULL OR canvass.internal_opens_any = 0 THEN NULL
                                WHEN canvass.internal_knocks_any = 1 AND canvass.internal_opens_any = 1 AND open_cases.internal_cases_opened_thismonth_any IS NULL THEN 0
                                ELSE open_cases.internal_cases_opened_thismonth_any END, 1)
                      OVER(PARTITION BY a.address_id ORDER BY a.address_id, a.month_start ASC) AS internal_cases_opened_any_next_month
                    -- cases opened ratios
                        -- cases opened per unit
                    , LEAD(CASE WHEN canvass.internal_opens_any IS NULL OR canvass.internal_opens_any = 0 THEN NULL
                                WHEN canvass.internal_knocks_any = 1 AND canvass.internal_opens_any = 1 AND open_cases.internal_cases_opened_thismonth_count IS NULL THEN 0
                                ELSE (open_cases.internal_cases_opened_thismonth_count*1.0000)/a.units::INT END, 1)
                      OVER(PARTITION BY a.address_id ORDER BY a.address_id, a.month_start ASC) AS internal_cases_opened_per_unit_next_month
                        -- cases opened per door opened
                    , LEAD(CASE WHEN canvass.internal_opens_any IS NULL OR canvass.internal_opens_any = 0 THEN NULL
                                WHEN canvass.internal_knocks_any = 1 AND canvass.internal_opens_any = 1 AND open_cases.internal_cases_opened_thismonth_count IS NULL THEN 0
                                WHEN canvass.internal_knocks_any = 1 AND canvass.internal_opens_any = 1 AND open_cases.internal_cases_opened_thismonth_count IS NOT NULL THEN (open_cases.internal_cases_opened_thismonth_count*1.0000)/canvass.internal_opens_count
                                ELSE NULL END, 1)
                      OVER(PARTITION BY a.address_id ORDER BY a.address_id, a.month_start ASC) AS internal_cases_opened_per_open_next_month
                        -- cases opened per knock
                    , LEAD(CASE WHEN canvass.internal_opens_any IS NULL OR canvass.internal_opens_any = 0 THEN NULL
                                WHEN canvass.internal_knocks_any = 1 AND canvass.internal_opens_any = 1 AND open_cases.internal_cases_opened_thismonth_count IS NULL THEN 0
                                ELSE (open_cases.internal_cases_opened_thismonth_count*1.0000)/canvass.internal_knocks_count END, 1)
                      OVER(PARTITION BY a.address_id ORDER BY a.address_id, a.month_start ASC) AS internal_cases_opened_per_knock_next_month
                    ------------------------------------------------------------------ INTERNAL FEATURES -------------------------------------------------------------------------------
                    ---- knocks and opens from canvass table
                    , CASE WHEN canvass.internal_knocks_count IS NULL THEN 0 ELSE canvass.internal_knocks_count END AS internal_knocks_count_this_month
                    , CASE WHEN canvass.internal_knocks_any IS NULL THEN 0 ELSE canvass.internal_knocks_any END AS internal_knocks_any_this_month
                    , canvass.internal_opens_count AS internal_opens_count_this_month
                    , canvass.internal_opens_any AS internal_opens_any_this_month
                    ---- open cases & issues from a combination of the contacts, cases, and issues tables
                        ---- NOTE: the definitions of 1s and 0s here are slightly inconsistent
                        ---- 1s are computed based on cases from any origin but 0s are only computed for addresses where there have been knocks in that month
                    , CASE WHEN canvass.internal_knocks_any = 1 AND canvass.internal_opens_any = 1 AND open_cases.internal_cases_opened_thismonth_count IS NULL THEN 0 ELSE open_cases.internal_cases_opened_thismonth_count END AS internal_cases_opened_count_this_month
                    , CASE WHEN canvass.internal_knocks_any = 1 AND canvass.internal_opens_any = 1 AND open_cases.internal_cases_opened_thismonth_any IS NULL THEN 0 ELSE open_cases.internal_cases_opened_thismonth_any END AS internal_cases_opened_any_this_month
                    , CASE WHEN canvass.internal_knocks_any = 1 AND canvass.internal_opens_any = 1 AND open_cases.internalissue_cases_opened_count_legal IS NULL THEN 0 ELSE open_cases.internalissue_cases_opened_count_legal END AS internal_issue_legal_cases_opened_count_this_month
                    , CASE WHEN canvass.internal_knocks_any = 1 AND canvass.internal_opens_any = 1 AND open_cases.internalissue_cases_opened_count_repair IS NULL THEN 0 ELSE open_cases.internalissue_cases_opened_count_repair END AS internal_issue_repair_cases_opened_count_this_month
                    , CASE WHEN canvass.internal_knocks_any = 1 AND canvass.internal_opens_any = 1 AND open_cases.internalissue_cases_opened_count_service_access IS NULL THEN 0 ELSE open_cases.internalissue_cases_opened_count_service_access END AS internal_issue_service_access_cases_opened_count_this_month
                    , CASE WHEN canvass.internal_knocks_any = 1 AND canvass.internal_opens_any = 1 AND open_cases.internalissue_cases_opened_count_other IS NULL THEN 0 ELSE open_cases.internalissue_cases_opened_count_other END AS internal_issue_other_cases_opened_count_this_month
                    ---- closed cases from the contacts & cases tables
                        ---- NOTE: these variables are null for all months/addresses where no case was closed
                        ---- in the future, might want to think about whether some of these should be zero instead
                        ---- i.e., for addresses where cases have been opened in the past, cases_closed could be 0 instead of null
                    , closed_cases.internal_cases_closed_thismonth_count AS internal_cases_closed_count_this_month
                    , closed_cases.internal_cases_closed_thismonth_any AS internal_cases_closed_any_this_month
                    , closed_cases.internal_nya_cases_closed_thismonth_count AS internal_nya_cases_closed_count_this_month
                    ---- running total of open and closed cases (as of a particular month, i.e. cumulative sum)
                    , LAST_VALUE(open_cases.internal_cases_opened_ever_count) 
                       OVER(PARTITION BY a.address_id ORDER BY case when open_cases.internal_cases_opened_ever_count is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) 
                       AS internal_cases_opened_count_ever
                    , LAST_VALUE(closed_cases.internal_cases_closed_ever_count) 
                       OVER(PARTITION BY a.address_id ORDER BY case when closed_cases.internal_cases_closed_ever_count is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) 
                       AS internal_cases_closed_count_ever
                    , LAST_VALUE(closed_cases.internal_nya_cases_closed_ever_count) 
                       OVER(PARTITION BY a.address_id ORDER BY case when closed_cases.internal_nya_cases_closed_ever_count is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) 
                       AS internal_nya_cases_closed_count_ever
                        --- add running total knocks??
                    ---- this month's follow ups
                    , followups.internalfollow_all_followups_count AS internal_follow_all_followups_count_this_month
                    , followups.internalfollow_all_followups_any AS internal_follow_all_followups_any_this_month
                    ---- specialists flags for this month (was this specialist responsible for any of the cases ever opened at a particular building)
                    , sp_005t0000000ogcPAAQ_thismonth AS internal_sp_005t0000000ogcPAAQ_any_this_month
                    , sp_005t0000000ogcIAAQ_thismonth AS internal_sp_005t0000000ogcIAAQ_any_this_month
                    , sp_005t0000000ogcBAAQ_thismonth AS internal_sp_005t0000000ogcBAAQ_any_this_month
                    , sp_005t0000000ogc7AAA_thismonth AS internal_sp_005t0000000ogc7AAA_any_this_month
                    , sp_005t0000000ogcDAAQ_thismonth AS internal_sp_005t0000000ogcDAAQ_any_this_month
                    , sp_005t0000000ogcJAAQ_thismonth AS internal_sp_005t0000000ogcJAAQ_any_this_month
                    , sp_005t0000000ogcHAAQ_thismonth AS internal_sp_005t0000000ogcHAAQ_any_this_month
                    , sp_005t0000000ogcOAAQ_thismonth AS internal_sp_005t0000000ogcOAAQ_any_this_month
                    , sp_005t0000000ogbYAAQ_thismonth AS internal_sp_005t0000000ogbYAAQ_any_this_month
                    , sp_005t0000000ogbcAAA_thismonth AS internal_sp_005t0000000ogbcAAA_any_this_month
                    , sp_005t0000000ogbaAAA_thismonth AS internal_sp_005t0000000ogbaAAA_any_this_month
                    , sp_005t0000000ogbyAAA_thismonth AS internal_sp_005t0000000ogbyAAA_any_this_month
                    , sp_005t0000000ogbnAAA_thismonth AS internal_sp_005t0000000ogbnAAA_any_this_month
                    , sp_005t0000000ogc4AAA_thismonth AS internal_sp_005t0000000ogc4AAA_any_this_month
                    , sp_005t0000000ogcUAAQ_thismonth AS internal_sp_005t0000000ogcUAAQ_any_this_month
                    , sp_005t0000000ogc3AAA_thismonth AS internal_sp_005t0000000ogc3AAA_any_this_month
                    , sp_005t00000014JjiAAE_thismonth AS internal_sp_005t00000014JjiAAE_any_this_month
                    , sp_005t0000000ogcEAAQ_thismonth AS internal_sp_005t0000000ogcEAAQ_any_this_month
                    , sp_005t0000000ogc9AAA_thismonth AS internal_sp_005t0000000ogc9AAA_any_this_month
                    , sp_005t0000000ogcCAAQ_thismonth AS internal_sp_005t0000000ogcCAAQ_any_this_month
                    , sp_005t0000000ogbZAAQ_thismonth AS internal_sp_005t0000000ogbZAAQ_any_this_month
                    , sp_005t0000000ogbwAAA_thismonth AS internal_sp_005t0000000ogbwAAA_any_this_month
                    , sp_005t0000000ogcSAAQ_thismonth AS internal_sp_005t0000000ogcSAAQ_any_this_month
                    , sp_005t0000000ogbxAAA_thismonth AS internal_sp_005t0000000ogbxAAA_any_this_month
                    , sp_005t0000000ogc0AAA_thismonth AS internal_sp_005t0000000ogc0AAA_any_this_month
                    , sp_005t0000000ogblAAA_thismonth AS internal_sp_005t0000000ogblAAA_any_this_month
                    , sp_005t0000000ogbfAAA_thismonth AS internal_sp_005t0000000ogbfAAA_any_this_month
                    , sp_005t0000000ogc2AAA_thismonth AS internal_sp_005t0000000ogc2AAA_any_this_month
                    , sp_005t0000000ogbkAAA_thismonth AS internal_sp_005t0000000ogbkAAA_any_this_month
                    , sp_005t0000000ogbuAAA_thismonth AS internal_sp_005t0000000ogbuAAA_any_this_month
                    , sp_005t0000000ogbsAAA_thismonth AS internal_sp_005t0000000ogbsAAA_any_this_month
                    , sp_005t0000000ogbpAAA_thismonth AS internal_sp_005t0000000ogbpAAA_any_this_month
                    , sp_005t0000001hsSUAAY_thismonth AS internal_sp_005t0000001hsSUAAY_any_this_month
                    , sp_005t0000000ogc6AAA_thismonth AS internal_sp_005t0000000ogc6AAA_any_this_month
                    , sp_005t0000000ogbdAAA_thismonth AS internal_sp_005t0000000ogbdAAA_any_this_month
                    , sp_005t0000000ogcAAAQ_thismonth AS internal_sp_005t0000000ogcAAAQ_any_this_month
                    , sp_005t0000000ogcKAAQ_thismonth AS internal_sp_005t0000000ogcKAAQ_any_this_month
                    , sp_005t0000000ogc5AAA_thismonth AS internal_sp_005t0000000ogc5AAA_any_this_month
                    , sp_005t0000000ogboAAA_thismonth AS internal_sp_005t0000000ogboAAA_any_this_month
                    , sp_005t0000000ogbjAAA_thismonth AS internal_sp_005t0000000ogbjAAA_any_this_month
                    , sp_005t0000000ogbzAAA_thismonth AS internal_sp_005t0000000ogbzAAA_any_this_month
                    , sp_005t0000000ogbiAAA_thismonth AS internal_sp_005t0000000ogbiAAA_any_this_month
                    , sp_005t0000000owxxAAA_thismonth AS internal_sp_005t0000000owxxAAA_any_this_month
                    , sp_005t0000000ogbXAAQ_thismonth AS internal_sp_005t0000000ogbXAAQ_any_this_month
                    , sp_005t0000000ogbbAAA_thismonth AS internal_sp_005t0000000ogbbAAA_any_this_month
                    , sp_005t0000000ogcGAAQ_thismonth AS internal_sp_005t0000000ogcGAAQ_any_this_month
                    , sp_005t0000000ogcFAAQ_thismonth AS internal_sp_005t0000000ogcFAAQ_any_this_month
                    , sp_005t0000000ogcQAAQ_thismonth AS internal_sp_005t0000000ogcQAAQ_any_this_month
                    , sp_005t0000000ogbvAAA_thismonth AS internal_sp_005t0000000ogbvAAA_any_this_month
                    , sp_005t0000000ogbmAAA_thismonth AS internal_sp_005t0000000ogbmAAA_any_this_month
                    , sp_005t0000000ogbtAAA_thismonth AS internal_sp_005t0000000ogbtAAA_any_this_month
                    , sp_005t0000000ogbhAAA_thismonth AS internal_sp_005t0000000ogbhAAA_any_this_month
                    , sp_005t0000000ogbrAAA_thismonth AS internal_sp_005t0000000ogbrAAA_any_this_month
                    , sp_005t0000000ogbqAAA_thismonth AS internal_sp_005t0000000ogbqAAA_any_this_month
                    , sp_005t0000000ogc1AAA_thismonth AS internal_sp_005t0000000ogc1AAA_any_this_month
                    , sp_005t0000000ogbeAAA_thismonth AS internal_sp_005t0000000ogbeAAA_any_this_month
                    , sp_005t0000000ogcLAAQ_thismonth AS internal_sp_005t0000000ogcLAAQ_any_this_month
                    ---- specialist flags (rolling): was this specialist responsible for any of the cases ever opened at this address in any preceding months
                    , LAST_VALUE(sp_005t0000000ogcPAAQ_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogcPAAQ_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogcPAAQ_any_ever
                    , LAST_VALUE(sp_005t0000000ogcIAAQ_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogcIAAQ_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogcIAAQ_any_ever
                    , LAST_VALUE(sp_005t0000000ogcBAAQ_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogcBAAQ_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogcBAAQ_any_ever
                    , LAST_VALUE(sp_005t0000000ogc7AAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogc7AAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogc7AAA_any_ever
                    , LAST_VALUE(sp_005t0000000ogcDAAQ_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogcDAAQ_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogcDAAQ_any_ever
                    , LAST_VALUE(sp_005t0000000ogcJAAQ_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogcJAAQ_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogcJAAQ_any_ever
                    , LAST_VALUE(sp_005t0000000ogcHAAQ_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogcHAAQ_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogcHAAQ_any_ever
                    , LAST_VALUE(sp_005t0000000ogcOAAQ_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogcOAAQ_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogcOAAQ_any_ever
                    , LAST_VALUE(sp_005t0000000ogbYAAQ_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogbYAAQ_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogbYAAQ_any_ever
                    , LAST_VALUE(sp_005t0000000ogbcAAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogbcAAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogbcAAA_any_ever
                    , LAST_VALUE(sp_005t0000000ogbaAAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogbaAAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogbaAAA_any_ever
                    , LAST_VALUE(sp_005t0000000ogbyAAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogbyAAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogbyAAA_any_ever
                    , LAST_VALUE(sp_005t0000000ogbnAAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogbnAAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogbnAAA_any_ever
                    , LAST_VALUE(sp_005t0000000ogc4AAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogc4AAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogc4AAA_any_ever
                    , LAST_VALUE(sp_005t0000000ogcUAAQ_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogcUAAQ_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogcUAAQ_any_ever
                    , LAST_VALUE(sp_005t0000000ogc3AAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogc3AAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogc3AAA_any_ever
                    , LAST_VALUE(sp_005t00000014JjiAAE_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t00000014JjiAAE_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t00000014JjiAAE_any_ever
                    , LAST_VALUE(sp_005t0000000ogcEAAQ_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogcEAAQ_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogcEAAQ_any_ever
                    , LAST_VALUE(sp_005t0000000ogc9AAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogc9AAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogc9AAA_any_ever
                    , LAST_VALUE(sp_005t0000000ogcCAAQ_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogcCAAQ_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogcCAAQ_any_ever
                    , LAST_VALUE(sp_005t0000000ogbZAAQ_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogbZAAQ_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogbZAAQ_any_ever
                    , LAST_VALUE(sp_005t0000000ogbwAAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogbwAAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogbwAAA_any_ever
                    , LAST_VALUE(sp_005t0000000ogcSAAQ_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogcSAAQ_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogcSAAQ_any_ever
                    , LAST_VALUE(sp_005t0000000ogbxAAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogbxAAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogbxAAA_any_ever
                    , LAST_VALUE(sp_005t0000000ogc0AAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogc0AAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogc0AAA_any_ever
                    , LAST_VALUE(sp_005t0000000ogblAAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogblAAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogblAAA_any_ever
                    , LAST_VALUE(sp_005t0000000ogbfAAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogbfAAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogbfAAA_any_ever
                    , LAST_VALUE(sp_005t0000000ogc2AAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogc2AAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogc2AAA_any_ever
                    , LAST_VALUE(sp_005t0000000ogbkAAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogbkAAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogbkAAA_any_ever
                    , LAST_VALUE(sp_005t0000000ogbuAAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogbuAAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogbuAAA_any_ever
                    , LAST_VALUE(sp_005t0000000ogbsAAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogbsAAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogbsAAA_any_ever
                    , LAST_VALUE(sp_005t0000000ogbpAAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogbpAAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogbpAAA_any_ever
                    , LAST_VALUE(sp_005t0000001hsSUAAY_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000001hsSUAAY_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000001hsSUAAY_any_ever
                    , LAST_VALUE(sp_005t0000000ogc6AAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogc6AAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogc6AAA_any_ever
                    , LAST_VALUE(sp_005t0000000ogbdAAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogbdAAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogbdAAA_any_ever
                    , LAST_VALUE(sp_005t0000000ogcAAAQ_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogcAAAQ_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogcAAAQ_any_ever
                    , LAST_VALUE(sp_005t0000000ogcKAAQ_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogcKAAQ_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogcKAAQ_any_ever
                    , LAST_VALUE(sp_005t0000000ogc5AAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogc5AAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogc5AAA_any_ever
                    , LAST_VALUE(sp_005t0000000ogboAAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogboAAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogboAAA_any_ever
                    , LAST_VALUE(sp_005t0000000ogbjAAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogbjAAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogbjAAA_any_ever
                    , LAST_VALUE(sp_005t0000000ogbzAAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogbzAAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogbzAAA_any_ever
                    , LAST_VALUE(sp_005t0000000ogbiAAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogbiAAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogbiAAA_any_ever
                    , LAST_VALUE(sp_005t0000000owxxAAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000owxxAAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000owxxAAA_any_ever
                    , LAST_VALUE(sp_005t0000000ogbXAAQ_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogbXAAQ_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogbXAAQ_any_ever
                    , LAST_VALUE(sp_005t0000000ogbbAAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogbbAAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogbbAAA_any_ever
                    , LAST_VALUE(sp_005t0000000ogcGAAQ_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogcGAAQ_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogcGAAQ_any_ever
                    , LAST_VALUE(sp_005t0000000ogcFAAQ_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogcFAAQ_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogcFAAQ_any_ever
                    , LAST_VALUE(sp_005t0000000ogcQAAQ_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogcQAAQ_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogcQAAQ_any_ever
                    , LAST_VALUE(sp_005t0000000ogbvAAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogbvAAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogbvAAA_any_ever
                    , LAST_VALUE(sp_005t0000000ogbmAAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogbmAAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogbmAAA_any_ever
                    , LAST_VALUE(sp_005t0000000ogbtAAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogbtAAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogbtAAA_any_ever
                    , LAST_VALUE(sp_005t0000000ogbhAAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogbhAAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogbhAAA_any_ever
                    , LAST_VALUE(sp_005t0000000ogbrAAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogbrAAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogbrAAA_any_ever
                    , LAST_VALUE(sp_005t0000000ogbqAAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogbqAAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogbqAAA_any_ever
                    , LAST_VALUE(sp_005t0000000ogc1AAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogc1AAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogc1AAA_any_ever
                    , LAST_VALUE(sp_005t0000000ogbeAAA_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogbeAAA_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogbeAAA_any_ever
                    , LAST_VALUE(sp_005t0000000ogcLAAQ_ever ) OVER(PARTITION BY a.address_id ORDER BY case when sp_005t0000000ogcLAAQ_ever is not null then 1 else 0 end ASC, a.address_id, a.month_start ASC ROWS UNBOUNDED PRECEDING) AS internal_sp_005t0000000ogcLAAQ_any_ever
        FROM
                (SELECT
                            month_start
                            , month_end
                            , MONTH
                            , YEAR
                            , a.address_id
                            , a.internal_bin_static as internal_bin
                            , a.internal_census_tract_static AS census_tract
                            , a.internal_zip_static AS zip
                            , a.internal_units_static AS units
                FROM
                            months, dssg_staging.entities_address_table_rs_clean as a
                WHERE       a.internal_units_static::int >= 6
                ) a
        ---- knocks & opens each month
        LEFT JOIN   canvass
        ON          a.address_id = canvass.location_id
        AND         a.month_start = canvass.month_start
        ---- cases opened each month
        LEFT JOIN   open_cases
        ON          a.address_id = open_cases.address_id
        AND         a.month_start = open_cases.month_start
        ---- cases closed each month
        LEFT JOIN   closed_cases
        ON          a.address_id = closed_cases.address_id
        AND         a.month_start = closed_cases.month_start
        ---- all follow ups this month
        LEFT JOIN   followups
        ON          a.address_id = followups.address_id
        AND         a.month_start = followups.month_start
        ---- specialist flags
        LEFT JOIN   specialists
        ON          a.address_id = specialists.address_id
        AND         a.month_start = specialists.month_start
        ---- external data merges
        LEFT JOIN   hpd_violations
        ON          a.internal_bin = hpd_violations.bin_clean
        AND         a.month_start = hpd_violations.month_start
        LEFT JOIN   housing_litig
        ON          a.internal_bin = housing_litig.bin_clean
        AND         a.month_start = housing_litig.month_start
        LEFT JOIN   acs   --should comment them back when we have acs
        ON          a.census_tract::BIGINT = acs.acs_tract_state_county_tract
        AND         a.year = acs.year_tomerge_rounded
        ---- time-varying tsu target zip flags
        LEFT JOIN   dssg_clean.date_first_canvassed dfc
        ON          a.zip = dfc.zip::int
        WHERE
                    -- drop dummy bins (no dummy bbls present)
                    a.internal_bin not like '%000000'
        ORDER BY    a.address_id, a.month_start ASC
        ;
        DROP TABLE IF exists dssg_staging.staging_monthly_split_{date_start_string}_{date_end_string}; --dssg_staging.staging_train_monthly_split_2015_06_01_2016_01_01; -- Need to change to                
        CREATE TABLE   --dssg_staging.staging_train_monthly_split_2015_06_01_2016_01_01
                       dssg_staging.staging_monthly_split_{date_start_string}_{date_end_string} 
        AS
        SELECT        *
        --FROM          events_train_monthly_split_2015_06_01_2016_01_01 t
        FROM          events_train_monthly_split_{date_start_string}_{date_end_string} t
        LEFT JOIN     dssg_staging.entities_address_table_rs_clean a
        USING         (address_id)
        WHERE         t.month_start < '{date_end}'
        ORDER BY      t.address_id, t.month_start ASC
        ;
