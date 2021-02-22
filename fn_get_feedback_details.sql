CREATE 
OR REPLACE FUNCTION pulse.fn_get_feedback_details() 
RETURNS 
table(
	id integer, 
	created_date date, 
	feedback varchar, 
	feedback_id varchar, 
	user_id integer, 
	rate double precision 
	) 
LANGUAGE 'sql' STABLE SECURITY DEFINER AS $ BODY $ 
SELECT
   id,
   created_date,
   feedback,
   feedback_id,
   user_id,
   rate 
FROM
   pulse.feedback_details fd 
WHERE
   fd.created_date 
   between (CURRENT_DATE - 60) and CURRENT_DATE $ BODY $ ;
   
ALTER FUNCTION pulse.fn_get_feedback_details() OWNER TO postgres;