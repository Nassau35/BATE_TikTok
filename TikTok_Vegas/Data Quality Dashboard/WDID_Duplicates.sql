SELECT
 unique_key
,employee_week_key
,employee_id
,effective_from
,effective_to
,telus_email
,COUNT(employee_id) OVER(


PARTITION BY unique_key


) AS RN
FROM `ti-is-datti-prodenv.GSS_TikTok.TikTok_Team_Members_Data_Tv1`
ORDER BY RN DESC