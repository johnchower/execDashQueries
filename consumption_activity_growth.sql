WITH 
platform_actions AS (
SELECT platform_action
FROM public.user_platform_action_facts
WHERE platform_action IN ('Answered Assessment Item' , 'Progressed Through Content' , 'Created Note in Content' , 'Added To-Do Item from Content' , 'Started Content')
),
champion_ids AS (
SELECT id AS champion_ids
FROM champion_dimensions
WHERE id IN (1,2,3,4,5,6,7,8,9)
),
interval_length AS (
SELECT 30 AS interval_length
),
date_ranges AS (
SELECT id
, case 
	when sql_date_stamp + (SELECT interval_length FROM interval_length) 
		>= current_date
		AND sql_date_stamp 
		< current_date
	THEN 1
	when sql_date_stamp + 2*(SELECT interval_length FROM interval_length) 
		>= current_date
		AND sql_date_stamp + (SELECT interval_length FROM interval_length) 
		< current_date
	THEN 0
	ELSE 2
end AS date_range
FROM date_dim
),
long_results AS (
SELECT uccb.champion_id AS champion_id
	, dr.date_range
	, count(DISTINCT upaf.id) AS value
FROM user_platform_action_facts upaf
left join date_ranges dr
ON upaf.date_id=dr.id
left join public.user_connected_to_champion_bridges uccb 
ON uccb.user_id=upaf.user_id
WHERE upaf.platform_action IN (SELECT platform_action FROM platform_actions)
AND dr.date_range!=2
AND uccb.sequence_number=1
GROUP BY uccb.champion_id, dr.date_range
),
wide_results_0 AS (
SELECT lr.champion_id
	, sum((lr.date_range=1)::INTEGER*lr.value) AS value_current
	, sum((lr.date_range=0)::INTEGER*lr.value) AS value_previous
FROM long_results lr
GROUP BY lr.champion_id
), 
wide_results AS (
SELECT wr.champion_id
	, cd.NAME AS champion_name
	, wr.value_previous
	, wr.value_current
	, case
	when wr.value_previous!=0
		THEN 1.0*(wr.value_current - wr.value_previous)/wr.value_previous 
	ELSE NULL 
	END AS pct_change
FROM wide_results_0 wr
left join champion_dimensions cd
ON cd.id=wr.champion_id
WHERE cd.id IN (SELECT champion_ids FROM champion_ids)
)
SELECT * FROM wide_results
;
