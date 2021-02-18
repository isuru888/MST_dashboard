--select * from pulse.fn_get_ticket_details_json('[ieDigital,Buzz Bingo,Diaverum]'::text,null::date,null::date,'[Completed,Open]'::text)
-- DROP FUNCTION pulse.fn_get_ticket_details(text,date,date,text);

CREATE OR REPLACE FUNCTION pulse.fn_get_ticket_details_json(p_project_name text, p_start_date date, p_end_date date,p_status text) RETURNS table (ticket_details json) as $BODY$
DECLARE
v_project_name 	text[]:= string_to_array (replace(replace(p_project_name, '[', ''), ']', ''),',');
v_status 		text[]:= string_to_array (replace(replace(p_status, '[', ''), ']', ''),',');

BEGIN
	RAISE NOTICE '%', v_project_name;
RETURN QUERY
SELECT row_to_json(a) from (
	SELECT k.KEY ,
		   k.created_month_year ,
		   k.created_date ,
		   k.created_day ,
		   CASE
			   WHEN ( TRIM(k.created_day) NOT IN ('Saturday' ,
												  'Sunday')
					 AND k.created_time between '06:30:00'::time and '22:30:00'::time ) THEN 'ON_SHIFT'
			   ELSE 'ON_CALL'
		   END on_shift_or_call , --k.reated_hour,
	 K.created_time ,
	 k.creator_name ,
	 CASE
		 WHEN TRIM(k.creator_name) = 'PagerDuty' THEN 'Proactive'
		 ELSE 'Reactive'
	 END react_proact ,
	 k.current_assignee_name ,
	 k.current_status ,
	 k.issue_type ,
	 k.priority ,
	 k.priority2 ,
	 k.project_name ,
	 k.summary ,
	 k.updated,
	 K.planing_date
	FROM
		   ( SELECT KEY ,
					created_month_year ,
					created_date ,
					to_char(created_date, 'Day') AS "created_day" , --extract(hour from created_date ) AS created_hour,
	 created_date::time AS created_time ,
	 creator_name ,
	 creator_name2 ,
	 current_assignee_name ,
	 current_status ,
	 issue_type ,
	 priority ,
	 priority2 ,
	 project_name ,
	 summary ,
	 updated,
	 planing_date
			FROM PULSE.all_open_tickets
			UNION ALL SELECT KEY ,
							 created_month_year ,
							 created_date ,
							 to_char(created_date, 'Day') AS "created_day" , --extract(hour from created_date ) AS created_hour,
	 created_date::time AS created_time ,
	 creator_name ,
	 creator_name2 ,
	 current_assignee_name ,
	 current_status ,
	 issue_type ,
	 priority ,
	 priority2 ,
	 project_name ,
	 summary ,
	 updated,
	 planing_date
			FROM PULSE.all_closed_tickets ) k
	WHERE
	CASE WHEN array_length(v_project_name, 1) > 0  THEN
	k.project_name = ANY(v_project_name)
	ELSE TRUE END
    AND
	CASE WHEN ( p_start_date IS NOT NULL  AND p_end_date IS NOT NULL )  THEN
	K.created_date >= p_start_date::date and K.created_date::date <= p_end_date
	WHEN p_start_date IS NOT NULL  AND p_end_date IS NULL  THEN
	K.created_date >= p_start_date
	WHEN p_start_date IS  NULL  AND p_end_date IS NOT NULL  THEN
	K.created_date <= p_end_date
	ELSE TRUE
	END
	AND
	CASE WHEN array_length(v_status, 1) > 0  THEN
	K.current_status =ANY(v_status)
	ELSE TRUE
	END
	ORDER BY k.created_date desc ) a;



END $BODY$ LANGUAGE plpgsql;

;


ALTER FUNCTION pulse.fn_get_ticket_details_json(text,date,date,text) OWNER TO postgres;

--select * from pulse.fn_get_ticket_details(''::text,null::date,null::date,''::text)
 --select * from pulse.fn_get_ticket_details_json(array['ieDigital','Buzz Bingo','Diaverum']::text[],null::date,null::date,array['Completed', 'Open']::text[])
