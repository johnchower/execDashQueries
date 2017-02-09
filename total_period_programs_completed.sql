WITH 
champion_ids AS (
SELECT id AS champion_ids
FROM champion_dimensions
WHERE id IN (SELECT id FROM public.champion_dimensions)
),
interval_length AS (
SELECT 30 AS interval_length
),
date_ranges AS (
SELECT id
, CASE 
	WHEN sql_date_stamp + 
		(SELECT interval_length FROM interval_length) 
		>= current_date
		AND sql_date_stamp 
		< current_date
	THEN 1
	WHEN sql_date_stamp + 
		2*(SELECT interval_length FROM interval_length) 
		>= current_date
		AND sql_date_stamp + 
		(SELECT interval_length FROM interval_length) 
		< current_date
	THEN 0
	ELSE 2
END AS date_range
FROM date_dim
),
long_results_0 AS (
SELECT id
	, user_id
	, tree_id
	, date_id
FROM public.content_progress_facts
WHERE state='Complete'
), 
long_results AS (
SELECT tcb.champion_id AS champion_id
	, count(DISTINCT lr0.id) AS value
	, dr.date_range
FROM long_results_0 lr0
left join public.tree_to_champion_bridges tcb
ON tcb.tree_id=lr0.tree_id
left join date_ranges dr
ON dr.id=lr0.date_id
WHERE dr.date_range!=2
GROUP BY tcb.champion_id, dr.date_range
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
	, CASE
	WHEN wr.value_previous!=0
		THEN 1.0*(wr.value_current - wr.value_previous)/wr.value_previous 
	ELSE NULL 
	END AS pct_change
FROM wide_results wr
LEFT JOIN champion_dimensions cd
ON cd.id=wr.champion_id
WHERE cd.id IN (SELECT champion_ids FROM champion_ids)
;
