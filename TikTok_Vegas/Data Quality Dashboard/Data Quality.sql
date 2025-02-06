WITH DateParameter AS (
    SELECT DATE '2024-11-25' AS X
),
Recordsformapping AS (
    SELECT
        adh.Start_Date,
        CAST(adh.WDID AS STRING) AS ID_RawData,
        COALESCE(roster.employee_id, "-1") AS MappingResult,
        'Adherence' AS Metric_Identifier
    FROM
        `ti-is-datti-prodenv.GSS_TikTok.TikTok_WFM_Data_Adh_v1` AS adh
    CROSS JOIN
        DateParameter
    LEFT JOIN
        `ti-is-datti-prodenv.GSS_TikTok.TikTok_Team_Members_Data_Tv1` AS roster
    ON
        adh.WDID = CAST(roster.employee_id AS int64)
        AND adh.Start_Date BETWEEN roster.effective_from AND roster.effective_to
    
    WHERE
        adh.Start_Date >= DateParameter.X
        AND roster.employee_id IS NULL

    UNION ALL

    SELECT
        AHT1.Date,
        AHT1.Moderator,
        COALESCE(roster.telus_email, "-1") AS MappingResultAHT,
        'AHT' AS Metric_Identifier
    FROM
        `ti-is-datti-prodenv.GSS_TikTok.TikTok_Data_DataPower_Tv1` AS AHT1
    CROSS JOIN
    DateParameter
    LEFT JOIN
        `ti-is-datti-prodenv.GSS_TikTok.TikTok_Team_Members_Data_Tv1` AS roster
    ON
        AHT1.Moderator = roster.telus_email
        AND AHT1.Date BETWEEN roster.effective_from AND roster.effective_to
    WHERE
        AHT1.Date >= DateParameter.X
        AND roster.telus_email IS NULL

    UNION ALL

    SELECT
        abse.Date,
        CAST(abse.WDID AS STRING) AS ID_RawData,
        COALESCE(roster.employee_id, "-1") AS MappingResult,
        'Attendance' AS Metric_Identifier
    FROM
        `ti-is-datti-prodenv.GSS_TikTok.TikTok_WFM_Data_Abs_1` AS abse
    CROSS JOIN
    DateParameter
    LEFT JOIN
        `ti-is-datti-prodenv.GSS_TikTok.TikTok_Team_Members_Data_Tv1` AS roster
    ON
        CAST(abse.WDID AS int64) = CAST(roster.employee_id AS int64)
        AND abse.Date BETWEEN roster.effective_from AND roster.effective_to
    WHERE
        abse.Date >= DateParameter.X
        AND roster.employee_id IS NULL
)
SELECT DISTINCT * FROM Recordsformapping 
ORDER BY 1, 2, 3