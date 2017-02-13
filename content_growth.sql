WITH 
champion_ids AS (
SELECT id AS champion_ids
FROM champion_dimensions
WHERE id IN (SELECT id FROM champion_dimensions)
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
  	when sql_date_stamp + 2*(SELECT interval_length FROM interval_length) 
		< current_date
	THEN -1
	ELSE 2
end AS date_range
FROM date_dim
),
long_results AS (
SELECT tcb.champion_id
	, dr.date_range
	, count(DISTINCT td.id) AS value
FROM tree_dimensions td
left join tree_to_champion_bridges tcb
ON tcb.tree_id=td.id
left join date_ranges dr
ON dr.id=td.created_date_id
WHERE dr.date_range!=2
GROUP BY tcb.champion_id, dr.date_range
),
wide_results AS (
SELECT lr.champion_id
	, sum(
		((lr.date_range IN (-1,0,1))::INTEGER)*lr.value
	) AS value_current
	, sum(
		((lr.date_range IN (-1,0))::INTEGER)*lr.value
	) AS value_previous
FROM long_results lr
GROUP BY lr.champion_id
),
final_results AS ( 
SELECT cd.id AS champion_id
	, cd.NAME AS champion_name
	, wr.value_previous
	, wr.value_current
	, case
	when wr.value_previous!=0
		THEN 1.0*(wr.value_current - wr.value_previous)/wr.value_previous 
	ELSE NULL 
	END AS pct_change
FROM champion_dimensions cd
left join wide_results wr
ON wr.champion_id=cd.id
WHERE cd.id IN (SELECT champion_ids FROM champion_ids)
)
SELECT *
FROM final_results
ORDER BY champion_name
;
