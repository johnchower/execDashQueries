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
	WHEN sql_date_stamp + (SELECT interval_length FROM interval_length) 
		>= current_date
		AND sql_date_stamp 
		< current_date
	THEN 1
	WHEN sql_date_stamp + 2*(SELECT interval_length FROM interval_length) 
		>= current_date
		AND sql_date_stamp + (SELECT interval_length FROM interval_length) 
		< current_date
	THEN 0
	ELSE 2
END AS date_range
FROM date_dim
),
week_numbers AS (
SELECT id
	, date_range
	, (
		( sum(1) 
		OVER (
		PARTITION BY date_range
		ORDER BY id DESC
		ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
		-1)/7
	) AS week_number
FROM date_ranges
WHERE date_range!=2
ORDER BY id DESC
),
long_results_0 AS (
SELECT uccb.champion_id AS champion_id
	, wn.date_range
	, wn.week_number AS calendar_week_start_date
	, count(DISTINCT sdf.user_id) AS count_active_users
FROM public.session_duration_fact sdf
LEFT JOIN week_numbers wn
ON wn.id=sdf.date_id
LEFT JOIN user_connected_to_champion_bridges uccb
ON uccb.user_id=sdf.user_id
LEFT JOIN date_dim dd
ON dd.id=sdf.date_id
WHERE wn.date_range!=2
AND uccb.sequence_number=1
AND wn.week_number <= (SELECT interval_length FROM interval_length)/7
GROUP BY uccb.champion_id, wn.date_range, wn.week_number
), 
long_results AS (
SELECT champion_id
	, date_range
	, avg(count_active_users) AS value
FROM long_results_0
GROUP BY champion_id, date_range
),
wide_results AS (
SELECT lr.champion_id
	, sum((lr.date_range=1)::INTEGER*lr.value) AS value_current
	, sum((lr.date_range=0)::INTEGER*lr.value) AS value_previous
FROM long_results lr
GROUP BY lr.champion_id
)
SELECT wr.champion_id
	, cd.NAME AS champion_name
	, wr.value_previous
	, wr.value_current
	, CASE
	WHEN wr.value_previous!=0
		THEN 100.0*(wr.value_current - wr.value_previous)/wr.value_previous 
	ELSE NULL 
	END AS pct_change
FROM wide_results wr
LEFT JOIN champion_dimensions cd
ON cd.id=wr.champion_id
WHERE cd.id IN (SELECT champion_ids FROM champion_ids)
;
