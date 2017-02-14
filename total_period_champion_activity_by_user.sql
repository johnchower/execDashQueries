WITH 
user_ids AS (
SELECT id AS user_ids
FROM public.users
WHERE id IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19)
),
interval_length AS (
SELECT 30 AS interval_length
),
long_results_invitations_sent_0 AS (
SELECT 	inviter_id
	, id
	, case 
		when created_at::DATE + (SELECT interval_length FROM interval_length) 
			>= current_date
			AND created_at::DATE 
			< current_date
		THEN 1
		when created_at::DATE + 2*(SELECT interval_length FROM interval_length) 
			>= current_date
			AND created_at::DATE + (SELECT interval_length FROM interval_length) 
			< current_date
		THEN 0
		when created_at::DATE + 2*(SELECT interval_length FROM interval_length) 
			< current_date
		THEN -1
		ELSE 2
	end AS date_range
FROM public.membership_invitations
),
long_results_invitations_sent AS (
SELECT inviter_id
	, date_range
	, count(DISTINCT id) AS value 
FROM long_results_invitations_sent_0
GROUP BY inviter_id, date_range
),
wide_results_invitations_sent AS (
SELECT lr.inviter_id AS user_id
	, sum((lr.date_range=1)::INTEGER*lr.value) AS value_current_invitations_sent
	, sum((lr.date_range=0)::INTEGER*lr.value) AS value_previous_invitations_sent
FROM long_results_invitations_sent lr
GROUP BY lr.inviter_id
),
user_connections AS (
SELECT connectable2_id
	, connectable1_id
	, min(created_at::DATE) AS created_at 
FROM public.connections
WHERE connectable2_type='User'
GROUP BY connectable2_id, connectable1_id
),
specific_user_connections_2 AS (
SELECT 	connectable2_id AS user_id_made_connection	
	, connectable1_id AS user_id_connected_to
	, created_at
FROM user_connections
WHERE connectable2_id IN (SELECT DISTINCT user_ids FROM user_ids)
),
specific_user_connections_1 AS (
SELECT 	connectable1_id AS user_id_made_connection	
	, connectable2_id AS user_id_connected_to
	, created_at
FROM user_connections
WHERE connectable1_id IN (SELECT DISTINCT user_ids FROM user_ids)
),
specific_user_connections AS (
SELECT DISTINCT * FROM specific_user_connections_1
UNION
SELECT DISTINCT * FROM specific_user_connections_2
),
long_results_connections_added_0 AS (
SELECT 	user_id_made_connection AS user_id
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
	, user_id_connected_to
FROM specific_user_connections
),
long_results_connections_added AS (
SELECT 	user_id
	, date_range
       	, count(DISTINCT user_id_connected_to) AS value	
FROM long_results_connections_added_0
GROUP BY user_id, date_range
),
wide_results_connections_added AS (
SELECT lr.user_id
	, sum((lr.date_range=1)::INTEGER*lr.value) AS value_current_connections_added
	, sum((lr.date_range=0)::INTEGER*lr.value) AS value_previous_connections_added
FROM long_results_connections_added lr
GROUP BY lr.user_id
),
results AS (
SELECT user_ids.user_ids AS user_id
	, wrca.value_current_connections_added
	, wrca.value_previous_connections_added
	, wris.value_current_invitations_sent
	, wris.value_previous_invitations_sent
FROM  user_ids
left join wide_results_connections_added wrca
ON wrca.user_id=user_ids.user_ids
left join wide_results_invitations_sent wris
ON wris.user_id=user_ids.user_ids
)
SELECT *
FROM results
ORDER BY user_id
;
