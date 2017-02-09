WITH 
platform_action_ids AS (
SELECT platform_action_id
FROM platform_action_fact
WHERE platform_action_id IN (41,42)
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
	, count(DISTINCT paf.id) AS value
FROM platform_action_fact paf
left join date_ranges dr
ON paf.date_id=dr.id
left join public.user_connected_to_champion_bridges uccb 
ON uccb.user_id=paf.user_id
WHERE paf.platform_action_id IN (SELECT platform_action_id FROM platform_action_ids)
AND dr.date_range!=2
AND uccb.sequence_number=1
GROUP BY uccb.champion_id, dr.date_range
),
wide_results AS (
SELECT lr.champion_id
	, sum(lr.date_range*lr.value) AS value_current
	, sum((1-lr.date_range)*lr.value) AS value_previous
FROM long_results lr
GROUP BY lr.champion_id
)
SELECT wr.champion_id
	, cd.NAME AS champion_name
	, wr.value_previous
	, wr.value_current
	, case
	when wr.value_previous!=0
		THEN 1.0*(wr.value_current - wr.value_previous)/wr.value_previous 
	ELSE NULL 
	END AS pct_change
FROM wide_results wr
left join champion_dimensions cd
ON cd.id=wr.champion_id
WHERE cd.id IN (SELECT champion_ids FROM champion_ids)
;
