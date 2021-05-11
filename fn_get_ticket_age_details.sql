--select * from  pulse.fn_get_ticket_age_details('[ieDigital,Buzz Bingo,Diaverum]'::text,null,null,'[Completed,Open]'::text,'[Incident,Problem]'::text,'[09-12WK,02WK,13-16WK,01WK]'::text)
 -- DROP FUNCTION pulse.fn_get_ticket_age_details(text, date, date, text,,text,text);

CREATE OR REPLACE FUNCTION pulse.fn_get_ticket_age_details( p_project_name text, p_start_date date, p_end_date date, p_status text,p_issue_type text,p_updated_age text) 
returns table ( 
	key character varying, 
	created_month_year character varying, 
	created_date timestamp without time zone,
	created_day text, 
	on_shift_or_call text, 
	created_time time without time zone,
	creator_name character varying,
	react_proact text, 
	current_assignee_name character varying, 
	current_status character varying, 
	issue_type character varying, 
	priority character varying, 
	priority2 character varying, 
	project_name character varying, 
	summary character varying, 
	updated timestamp without time zone,
	planing_date text,
	updated_age character varying,
	created_age character varying
) 
language plpgsql STABLE SECURITY DEFINER PARALLEL UNSAFE AS $BODY$
DECLARE
v_project_name 	text[]:= string_to_array (replace(replace(p_project_name, '[', ''), ']', ''),',');
v_status 		text[]:= string_to_array (replace(replace(p_status, '[', ''), ']', ''),',');
v_issue_type 	text[]:= string_to_array (replace(replace(p_issue_type, '[', ''), ']', ''),',');
v_updated_age   text[]:= string_to_array (replace(replace(p_updated_age, '[', ''), ']', ''),',');
BEGIN
return query
SELECT k.KEY,
		   k.created_month_year,
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
	 END react_proact,
	 k.current_assignee_name ,
	 k.current_status ,
	 k.issue_type ,
	 k.priority ,
	 k.priority2 ,
	 k.project_name ,
	 k.summary ,
	 k.updated,
	 K.planing_date::text,
	 K.updated_age,
	 K.created_age
	FROM
		   ( SELECT aot.KEY ,
					aot.created_month_year ,
					aot.created_date ,
					to_char(aot.created_date, 'Day') AS "created_day" , --extract(hour from created_date ) AS created_hour,
	 aot.created_date::time AS created_time ,
	 aot.creator_name ,
	 aot.creator_name2 ,
	 aot.current_assignee_name ,
	 aot.current_status ,
	 aot.issue_type ,
	 aot.priority ,
	 aot.priority2 ,
	 aot.project_name ,
	 aot.summary ,
	 aot.updated,
	 aot.planing_date,
	 aot.updated_age,
	 aot.created_age
			FROM PULSE.all_open_tickets aot
			-- UNION ALL SELECT act.KEY ,
							 -- act.created_month_year ,
							 -- act.created_date ,
							 -- to_char(act.created_date, 'Day') AS "created_day" , --extract(hour from created_date ) AS created_hour,
	 -- act.created_date::time AS created_time ,
	 -- act.creator_name ,
	 -- act.creator_name2 ,
	 -- act.current_assignee_name ,
	 -- act.current_status ,
	 -- act.issue_type ,
	 -- act.priority ,
	 -- act.priority2 ,
	 -- act.project_name ,
	 -- act.summary ,
	 -- act.updated,
	 -- act.planing_date,
	 -- act.updated_age,
	 -- act.created_age
			-- FROM PULSE.all_closed_tickets  act 
			) k

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
	AND
	CASE WHEN array_length(v_issue_type, 1) > 0  THEN
	K.issue_type =ANY(v_issue_type)
	ELSE TRUE
	END
	AND
	CASE WHEN array_length(v_updated_age, 1) > 0  THEN
	K.updated_age =ANY(v_updated_age)
	ELSE TRUE
	END
	ORDER BY k.created_date desc ;
END $BODY$;


ALTER FUNCTION pulse.fn_get_ticket_age_details(text, date, date, text,text,text) OWNER TO postgres;




