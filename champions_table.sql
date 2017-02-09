WITH 
consumption_actions AS (
SELECT platform_action
FROM public.user_platform_action_facts
WHERE platform_action IN ('Answered Assessment Item' , 'Progressed Through Content' , 'Created Note in Content' , 'Added To-Do Item from Content' , 'Started Content')
),
championing_actions AS (
SELECT platform_action
FROM public.user_platform_action_facts
WHERE platform_action IN ('Made Collection Private','Made Collection Public','Made Moment Private','Made Moment Public','Invited User To Private Space','Invited User To Shared Space','Invited User To Group Space','Posted to Feed','Commented on Feed Post','Shared Collection to Feed','Shared Form Response to Feed','Shared Media to Feed','Shared Moment to Feed','Shared Note to Feed','Shared Link to Feed','Shared Post to Feed','Shared Program to Feed','Shared Result to Feed','Shared LandingPage to Feed','Shared Collection to Group Space','Shared Collection to Shared Space','Shared Form Response to Group Space','Shared Form Response to Shared Space','Shared Link to Group Space','Shared Link to Shared Space','Shared Media to Group Space','Shared Media to Shared Space','Shared Moment to Group Space','Shared Moment to Shared Space','Shared Note to Group Space','Shared Note to Shared Space','Shared Post to Group Space','Shared Post to Shared Space','Shared Program to Group Space','Shared Program to Shared Space','Shared Result to Group Space','Shared Result to Shared Space','Space Membership Invitation Accepted','Commented on Shared Space','Commented on Group Space','Posted to Shared Space','Posted to Group Space','Shared LandingPage to Group Space','Assigned To-Do Item','Became Champion Member','Became Organization Member','Rated Champion','Rated Program','Clicked Button on Page','Clicked Social Icon on Page','Commented on Group Space Post','Commented on Private Space Post','Commented on Shared Space Post','Commented on Timeline Post')
),
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
	ELSE 2
end AS date_range
FROM date_dim
),
long_results_championing_actions AS (
SELECT uccb.champion_id AS champion_id
	, dr.date_range
	, count(DISTINCT upaf.id) AS value
FROM user_platform_action_facts upaf
left join date_ranges dr
ON upaf.date_id=dr.id
left join public.user_connected_to_champion_bridges uccb 
ON uccb.user_id=upaf.user_id
WHERE upaf.platform_action IN (SELECT platform_action FROM championing_actions)
AND dr.date_range!=2
AND uccb.sequence_number=1
GROUP BY uccb.champion_id, dr.date_range
),
wide_results_championing_actions_0 AS (
SELECT lr.champion_id
	, sum(lr.date_range*lr.value) AS value_current
	, sum((1-lr.date_range)*lr.value) AS value_previous
FROM long_results_championing_actions lr
GROUP BY lr.champion_id
),
wide_results_championing_actions AS (
SELECT wr.champion_id
	, cd.NAME AS champion_name
	, wr.value_previous 
	, wr.value_current
	, case
	when wr.value_previous!=0
		THEN 1.0*(wr.value_current - wr.value_previous)/wr.value_previous 
	ELSE NULL 
	END AS pct_change
FROM wide_results_championing_actions_0 wr
left join champion_dimensions cd
ON cd.id=wr.champion_id
),
long_results_content_growth AS (
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
wide_results_content_growth_0 AS (
SELECT lr.champion_id
	, sum(lr.date_range*lr.value) AS value_current
	, sum((1-lr.date_range)*lr.value) AS value_previous
FROM long_results_content_growth lr
GROUP BY lr.champion_id
),
wide_results_content_growth AS (
SELECT wr.champion_id
	, cd.NAME AS champion_name
	, wr.value_previous
	, wr.value_current
	, case
	when wr.value_previous!=0
		THEN 1.0*(wr.value_current - wr.value_previous)/wr.value_previous 
	ELSE NULL 
	END AS pct_change
FROM wide_results_content_growth_0 wr
left join champion_dimensions cd
ON cd.id=wr.champion_id
),
long_results_consumption_activity_growth AS (
SELECT uccb.champion_id AS champion_id
	, dr.date_range
	, count(DISTINCT upaf.id) AS value
FROM user_platform_action_facts upaf
left join date_ranges dr
ON upaf.date_id=dr.id
left join public.user_connected_to_champion_bridges uccb 
ON uccb.user_id=upaf.user_id
WHERE upaf.platform_action IN (SELECT platform_action FROM consumption_actions)
AND dr.date_range!=2
AND uccb.sequence_number=1
GROUP BY uccb.champion_id, dr.date_range
),
wide_results_consumption_activity_growth_0 AS (
SELECT lr.champion_id
	, sum(lr.date_range*lr.value) AS value_current
	, sum((1-lr.date_range)*lr.value) AS value_previous
FROM long_results_consumption_activity_growth lr
GROUP BY lr.champion_id
), 
wide_results_consumption_activity_growth AS (
SELECT wr.champion_id
	, cd.NAME AS champion_name
	, wr.value_previous
	, wr.value_current
	, case
	when wr.value_previous!=0
		THEN 1.0*(wr.value_current - wr.value_previous)/wr.value_previous 
	ELSE NULL 
	END AS pct_change
FROM wide_results_consumption_activity_growth_0 wr
left join champion_dimensions cd
ON cd.id=wr.champion_id
WHERE cd.id IN (SELECT champion_ids FROM champion_ids)
),
wide_results AS (
SELECT cd.id AS champion_id
	, cd.NAME AS champion_name
	, wrca.value_current AS championing_actions_current
	, wrca.value_previous AS championing_actions_previous
	, wrca.pct_change AS championing_actions_pct_change
	, wrcg.value_current AS content_growth_current
	, wrcg.value_previous AS content_growth_previous
	, wrcg.pct_change AS content_growth_pct_change
	, wrcag.value_current AS consumption_actions_current
	, wrcag.value_previous AS consumption_actions_previous
	, wrcag.pct_change AS consumption_actions_pct_change
FROM champion_dimensions cd
left join wide_results_championing_actions wrca
ON wrca.champion_id=cd.id
left join wide_results_content_growth wrcg
ON wrcg.champion_id=cd.id
left join wide_results_consumption_activity_growth wrcag
ON wrcag.champion_id=cd.id
WHERE cd.id IN (SELECT champion_ids FROM champion_ids)
)
SELECT * 
FROM wide_results
ORDER BY champion_id
;
