WITH
	interval_length AS (
		SELECT 30 AS interval_length),

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
		FROM date_dim),

	assignments_made AS (
          SELECT DISTINCT
            ud.id AS user_id,
	    dr.date_range,
            count(upaf.id) AS assignments_made
          FROM user_dimensions ud
            INNER JOIN user_platform_action_facts upaf ON ud.id = upaf.user_id
	    left join date_ranges dr ON	upaf.date_id=dr.id	
          WHERE upaf.platform_action = 'Assigned To-Do Item'
          GROUP BY ud.id, dr.date_range),

	wide_results_assignments_made AS (
    	  SELECT lr.user_id
    	  	  , sum(
    	  	  	  ((lr.date_range=1)::INTEGER)*lr.assignments_made
    	  	  ) AS value_current_assignments_made
    	  	  , sum(
    	  	  	  ((lr.date_range=0)::INTEGER)*lr.assignments_made
    	  	  ) AS value_previous_assignments_made
    	  FROM assignments_made lr
    	  GROUP BY lr.user_id
	),

        invitations_sent AS (
          SELECT DISTINCT
            ud.id AS user_id,
	    dr.date_range,
            count(upaf.id) AS invitations_sent
          FROM user_dimensions ud
            INNER JOIN user_platform_action_facts upaf ON ud.id = upaf.user_id
  	      left join date_ranges dr ON upaf.date_id=dr.id	
          WHERE upaf.platform_action IN (
            'Invited User To Group Space',
            'Invited User To Shared Space',
            'Invited User To Private Space',
            'Sent Tree Invitation',
            'Invited User To Unactivated Space'
          ) 
          GROUP BY ud.id, dr.date_range),

	wide_results_invitations_sent AS (
  	  SELECT lr.user_id
  	  	  , sum(
  	  	  	  ((lr.date_range=1)::INTEGER)*lr.invitations_sent
  	  	  ) AS value_current_invitations_sent
  	  	  , sum(
  	  	  	  ((lr.date_range=0)::INTEGER)*lr.invitations_sent
  	  	  ) AS value_previous_invitations_sent
  	  FROM invitations_sent lr
  	  GROUP BY lr.user_id
	),

        connections_added AS (
	  SELECT DISTINCT
	    ud.id AS user_id,
	    dr.date_range,
	    count(upaf.id) AS connections_added
	  FROM user_dimensions ud
	    INNER JOIN user_platform_action_facts upaf ON ud.id = upaf.user_id
	    left join date_ranges dr ON upaf.date_id=dr.id	
	  WHERE upaf.platform_action IN (
	    'Accepted User Connection',
	    'Requested User Connection'
	  ) 
	  GROUP BY ud.id, dr.date_range
	),

	wide_results_connections_added AS (
	  SELECT lr.user_id
	  	, sum(
	  		((lr.date_range=1)::INTEGER)*lr.connections_added
	  	) AS value_current_connections_added
	  	, sum(
	  		((lr.date_range=0)::INTEGER)*lr.connections_added
	  	) AS value_previous_connections_added
	  FROM connections_added lr
	  GROUP BY lr.user_id
	),

	wide_results_all AS (
		SELECT
		ud.id AS user_id,
		ud.first_name AS first_name,
		ud.last_name AS last_name,
		coalesce(inv.value_current_invitations_sent, 0) AS value_current_invitations_sent,
		coalesce(inv.value_previous_invitations_sent, 0) AS value_previous_invitations_sent,
		coalesce(am.value_current_assignments_made, 0) AS value_current_assignments_made,
		coalesce(am.value_previous_assignments_made, 0) AS value_previous_assignments_made,
		coalesce(ca.value_current_connections_added, 0) AS value_current_connections_added,
		coalesce(ca.value_previous_connections_added, 0) AS value_previous_connections_added
		FROM user_dimensions ud
		LEFT JOIN wide_results_assignments_made am ON ud.id = am.user_id
		LEFT JOIN wide_results_invitations_sent inv ON ud.id = inv.user_id
		LEFT JOIN wide_results_connections_added ca ON ud.id = ca.user_id
	)

SELECT
	user_id
	, first_name
	, last_name
	, value_current_invitations_sent
	, value_previous_invitations_sent
	, CASE
	WHEN value_previous_invitations_sent!=0
		THEN 100.0*(value_current_invitations_sent 
				- value_previous_invitations_sent)
				/value_previous_invitations_sent 
	ELSE NULL 
	END AS pct_change_invitations_sent
	, value_current_assignments_made
	, value_previous_assignments_made
	, CASE
	WHEN value_previous_assignments_made!=0
		THEN 100.0*(value_current_assignments_made 
				- value_previous_assignments_made)
				/value_previous_assignments_made 
	ELSE NULL 
	END AS pct_change_assignments_made
	, value_current_connections_added
	, value_previous_connections_added
	, CASE
	WHEN value_previous_connections_added!=0
		THEN 100.0*(value_current_connections_added 
				- value_previous_connections_added)
				/value_previous_connections_added 
	ELSE NULL 
	END AS pct_change_connections_added
FROM wide_results_all
WHERE user_id IN (1,2,3,4,5,6,7,8,9)--(#{champion_member_ids.join(',')})
ORDER BY pct_change_invitations_sent -- #{sort} #{order}
;
