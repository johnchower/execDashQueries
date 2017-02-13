WITH 
champion_ids AS (
SELECT id AS champion_ids
FROM champions
WHERE id IN (SELECT id FROM champions)
),
interval_length AS (
SELECT 30 AS interval_length
),
champion_connections_0 AS (
SELECT connectable2_id AS champion_id
	, connectable1_id AS user_id
	, min(created_at::DATE) AS created_at 
FROM public.connections
WHERE connectable2_type='Champion'
GROUP BY connectable2_id, connectable1_id
),
champion_connections AS (
SELECT champion_id
	, user_id
	, case 
		when created_at + (SELECT interval_length FROM interval_length) 
			>= current_date
			AND created_at 
			< current_date
		THEN 1
		when created_at + 2*(SELECT interval_length FROM interval_length) 
			>= current_date
			AND created_at + (SELECT interval_length FROM interval_length) 
			< current_date
		THEN 0
		when created_at + 2*(SELECT interval_length FROM interval_length) 
			< current_date
		THEN -1
		ELSE 2
	end AS date_range
FROM champion_connections_0
),
long_results AS (
SELECT champion_id
	, date_range
	, count(DISTINCT user_id) AS value
FROM champion_connections
WHERE date_range >= 0
AND date_range <= 1
GROUP BY champion_id, date_range
),
wide_results AS (
SELECT lr.champion_id
	, sum(lr.date_range*lr.value) AS value_current
	, sum((1-lr.date_range)*lr.value) AS value_previous
FROM long_results lr
GROUP BY lr.champion_id
)
SELECT wr.champion_id
	, c.NAME AS champion_name
	, wr.value_previous
	, wr.value_current
	, case
	when wr.value_previous!=0
		THEN 100.0*(wr.value_current - wr.value_previous)/wr.value_previous 
	ELSE NULL 
	END AS pct_change
FROM wide_results wr
left join champions c
ON c.id=wr.champion_id
WHERE c.id IN (SELECT champion_ids FROM champion_ids)
ORDER BY pct_change DESC
;
