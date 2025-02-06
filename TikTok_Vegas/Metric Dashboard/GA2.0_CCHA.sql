/* -------------------------------------------------------------------------
* This Query takes the moderations from the TikTok_CE_Client_Project_CCHA
* and TikTok_CE_Client_Project_Uno source data tables that contain one row
* per moderation, and generates a new table with one row per policy applied
* to each moderation as recorded in 3 separate columns, 
* (Policy_List_GT_Team_Full_Video, Policy_List_GT_Team_Keyframe,
* Mod_Policy_List) with the following possible cases:
* 
* - If no policies are applied in any of the 3 columns, only one line will
* exist in the output table (policy id 'none').
* - If the same n policies are applied in all 3 columns, n lines will
* exist in the output table.
* - If a different number of policies are applied in the columns, one line
* per each different policy will exist in the output table with no
* duplicate policies.
*
* The policy id must consist of an 8 digit hexadecimal value, except for
* the special value 'none'.
* ----------------------------------------------------------------------- */

WITH
/* -------------------------------------------------------------------------
* Define the full_video_policy_ccha CTE that contains the columns to be used
* by the model and transforms the data in the Policy_List_GT_Team_Full_Video
* column from a string like '["b58e12b5", "fc57b8ea"]' into an array of
* values [b58e12b5, fc57b8ea]. The values included in the array are 8 digit
* hexadecimal numbers, or the special value 'none'.
* ----------------------------------------------------------------------- */
full_video_policy_ccha AS (
  SELECT
    -- Keys to Dimension Tables
    Mod_Resolve_Time,
    Moderator,
    Mod_Queue_ID,
    -- Degenerate Dimensions
    Object_ID,
    Mod_Task_ID,
    Result_Type,
    BPO_Task_Link,
    -- Facts
    Is_Correct_All,

    REGEXP_EXTRACT_ALL(Mod_Policy_Title_List,r'[0-9a-f]{8}|none') AS arr_policy_id
  FROM `ti-is-datti-prodenv.GSS_TikTok.TikTok_CE_Client_Project_CCHA`
  WHERE
    REGEXP_CONTAINS(Mod_Policy_Title_List,r'[0-9a-f]{8}|none')
),

/* -------------------------------------------------------------------------
* Define the keyframe_policy_ccha CTE that contains the columns to be used
* by the model and transforms the data in the Policy_List_GT_Team_Keyframe
* column from a string like '["b58e12b5", "fc57b8ea"]' into an array of
* values [b58e12b5, fc57b8ea]. The values included in the array are 8 digit
* hexadecimal numbers, or the special value 'none'.
* ----------------------------------------------------------------------- */
keyframe_policy_ccha AS (
  SELECT
    -- Keys to Dimension Tables
    Mod_Resolve_Time,
    Moderator,
    Mod_Queue_ID,
    -- Degenerate Dimensions
    Object_ID,
    Mod_Task_ID,
    Result_Type,
    BPO_Task_Link,
    -- Facts
    Is_Correct_All,

    REGEXP_EXTRACT_ALL(Mod_Policy_Title_List,r'[0-9a-f]{8}|none') AS arr_policy_id
  FROM `ti-is-datti-prodenv.GSS_TikTok.TikTok_CE_Client_Project_CCHA`
  WHERE
    REGEXP_CONTAINS(Mod_Policy_Title_List,r'[0-9a-f]{8}|none')
),

/* -------------------------------------------------------------------------
* Define the mod_policy_ccha CTE that contains the columns to be used
* by the model and transforms the data in the Mod_Policy_List
* column from a string like '["b58e12b5", "fc57b8ea"]' into an array of
* values [b58e12b5, fc57b8ea]. The values included in the array are 8 digit
* hexadecimal numbers, or the special value 'none'.
* ----------------------------------------------------------------------- */
mod_policy_ccha AS (
  SELECT
    -- Keys to Dimension Tables
    Mod_Resolve_Time,
    Moderator,
    Mod_Queue_ID,
    -- Degenerate Dimensions
    Object_ID,
    Mod_Task_ID,
    Result_Type,
    BPO_Task_Link,
    -- Facts
    Is_Correct_All,

    REGEXP_EXTRACT_ALL(Mod_Policy_List,r'[0-9a-f]{8}|none') AS arr_policy_id
  FROM `ti-is-datti-prodenv.GSS_TikTok.TikTok_CE_Client_Project_CCHA`
  WHERE
    REGEXP_CONTAINS(Mod_Policy_List,r'[0-9a-f]{8}|none')
),

/* -------------------------------------------------------------------------
* Define the full_video_policy_uno CTE that contains the columns to be used
* by the model and transforms the data in the Policy_List_GT_Team_Full_Video
* column from a string like '["b58e12b5", "fc57b8ea"]' into an array of
* values [b58e12b5, fc57b8ea]. The values included in the array are 8 digit
* hexadecimal numbers, or the special value 'none'.
* ----------------------------------------------------------------------- */
full_video_policy_uno AS (
  SELECT
    -- Keys to Dimension Tables
    Mod_Resolve_Time,
    Moderator,
    Mod_Queue_ID,
    -- Degenerate Dimensions
    Object_ID,
    Mod_Task_ID,
    Result_Type,
    BPO_Task_Link,
    -- Facts
    Is_Correct_All,

    REGEXP_EXTRACT_ALL(Mod_Policy_Title_List,r'[0-9a-f]{8}|none') AS arr_policy_id
  FROM `ti-is-datti-prodenv.GSS_TikTok.TikTok_CE_Client_Project_Uno`
  WHERE
    REGEXP_CONTAINS(Mod_Policy_Title_List,r'[0-9a-f]{8}|none')
),

/* -------------------------------------------------------------------------
* Define the keyframe_policy_uno CTE that contains the columns to be used
* by the model and transforms the data in the Policy_List_GT_Team_Keyframe
* column from a string like '["b58e12b5", "fc57b8ea"]' into an array of
* values [b58e12b5, fc57b8ea]. The values included in the array are 8 digit
* hexadecimal numbers, or the special value 'none'.
* ----------------------------------------------------------------------- */
keyframe_policy_uno AS (
  SELECT
    -- Keys to Dimension Tables
    Mod_Resolve_Time,
    Moderator,
    Mod_Queue_ID,
    -- Degenerate Dimensions
    Object_ID,
    Mod_Task_ID,
    Result_Type,
    BPO_Task_Link,
    -- Facts
    Is_Correct_All,

    REGEXP_EXTRACT_ALL(Mod_Policy_Title_List,r'[0-9a-f]{8}|none') AS arr_policy_id
  FROM `ti-is-datti-prodenv.GSS_TikTok.TikTok_CE_Client_Project_Uno`
  WHERE
    REGEXP_CONTAINS(Mod_Policy_Title_List,r'[0-9a-f]{8}|none')
),

/* -------------------------------------------------------------------------
* Define the mod_policy_uno CTE that contains the columns to be used
* by the model and transforms the data in the Mod_Policy_List
* column from a string like '["b58e12b5", "fc57b8ea"]' into an array of
* values [b58e12b5, fc57b8ea]. The values included in the array are 8 digit
* hexadecimal numbers, or the special value 'none'.
* ----------------------------------------------------------------------- */
mod_policy_uno AS (
  SELECT
    -- Keys to Dimension Tables
    Mod_Resolve_Time,
    Moderator,
    Mod_Queue_ID,
    -- Degenerate Dimensions
    Object_ID,
    Mod_Task_ID,
    Result_Type,
    BPO_Task_Link,
    -- Facts
    Is_Correct_All,

    REGEXP_EXTRACT_ALL(Mod_Policy_List,r'[0-9a-f]{8}|none') AS arr_policy_id
  FROM `ti-is-datti-prodenv.GSS_TikTok.TikTok_CE_Client_Project_Uno`
  WHERE
    REGEXP_CONTAINS(Mod_Policy_List,r'[0-9a-f]{8}|none')
)

/* -------------------------------------------------------------------------
* Expand the full_video_policy_ccha CTE into a new table and create one row
* per item in the arr_policy_id array.
* ----------------------------------------------------------------------- */
SELECT
  -- Keys to Dimension Tables
  Mod_Resolve_Time,
  Moderator,
  Mod_Queue_ID,
  -- Degenerate Dimensions
  Object_ID,
  Mod_Task_ID,
  Result_Type,
  BPO_Task_Link,
  'Project CCHA' AS project,
  -- Facts
  Is_Correct_All,

  flattened_policies AS policy_id
FROM full_video_policy_ccha
CROSS JOIN UNNEST(full_video_policy_ccha.arr_policy_id) AS flattened_policies

-- Union the next table without repeating identical elements.
UNION DISTINCT

/* -------------------------------------------------------------------------
* Expand the keyframe_policy_ccha CTE into a new table and create one row
* per item in the arr_policy_id array.
* ----------------------------------------------------------------------- */
SELECT
  -- Keys to Dimension Tables
  Mod_Resolve_Time,
  Moderator,
  Mod_Queue_ID,
  -- Degenerate Dimensions
  Object_ID,
  Mod_Task_ID,
  Result_Type,
  BPO_Task_Link,
  'Project CCHA' AS project,
  -- Facts
  Is_Correct_All,

  flattened_policies AS policy_id
FROM keyframe_policy_ccha
CROSS JOIN UNNEST(keyframe_policy_ccha.arr_policy_id) AS flattened_policies

-- Union the next table without repeating identical elements.
UNION DISTINCT

/* -------------------------------------------------------------------------
* Expand the mod_policy_ccha CTE into a new table and create one row per
* item in the arr_policy_id array.
* ----------------------------------------------------------------------- */
SELECT
  -- Keys to Dimension Tables
  Mod_Resolve_Time,
  Moderator,
  Mod_Queue_ID,
  -- Degenerate Dimensions
  Object_ID,
  Mod_Task_ID,
  Result_Type,
  BPO_Task_Link,
  'Project CCHA' AS project,
  -- Facts
  Is_Correct_All,

  flattened_policies AS policy_id
FROM mod_policy_ccha
CROSS JOIN UNNEST(mod_policy_ccha.arr_policy_id) AS flattened_policies

-- Union the next table without repeating identical elements.
UNION DISTINCT

/* -------------------------------------------------------------------------
* Expand the full_video_policy_uno CTE into a new table and create one row per
* item in the arr_policy_id array.
* ----------------------------------------------------------------------- */
SELECT
  -- Keys to Dimension Tables
  Mod_Resolve_Time,
  Moderator,
  Mod_Queue_ID,
  -- Degenerate Dimensions
  Object_ID,
  Mod_Task_ID,
  Result_Type,
  BPO_Task_Link,
  'Project Uno' AS project,
  -- Facts
  Is_Correct_All,

  flattened_policies AS policy_id
FROM full_video_policy_uno
CROSS JOIN UNNEST(full_video_policy_uno.arr_policy_id) AS flattened_policies

-- Union the next table without repeating identical elements.
UNION DISTINCT

/* -------------------------------------------------------------------------
* Expand the keyframe_policy_uno CTE into a new table and create one row per
* item in the arr_policy_id array.
* ----------------------------------------------------------------------- */
SELECT
  -- Keys to Dimension Tables
  Mod_Resolve_Time,
  Moderator,
  Mod_Queue_ID,
  -- Degenerate Dimensions
  Object_ID,
  Mod_Task_ID,
  Result_Type,
  BPO_Task_Link,
  'Project Uno' AS project,
  -- Facts
  Is_Correct_All,

  flattened_policies AS policy_id
FROM keyframe_policy_uno
CROSS JOIN UNNEST(keyframe_policy_uno.arr_policy_id) AS flattened_policies

-- Union the next table without repeating identical elements.
UNION DISTINCT

/* -------------------------------------------------------------------------
* Expand the mod_policy_uno CTE into a new table and create one row per
* item in the arr_policy_id array.
* ----------------------------------------------------------------------- */
SELECT
  -- Keys to Dimension Tables
  Mod_Resolve_Time,
  Moderator,
  Mod_Queue_ID,
  -- Degenerate Dimensions
  Object_ID,
  Mod_Task_ID,
  Result_Type,
  BPO_Task_Link,
  'Project Uno' AS project,
  -- Facts
  Is_Correct_All,

  flattened_policies AS policy_id
FROM mod_policy_uno
CROSS JOIN UNNEST(mod_policy_uno.arr_policy_id) AS flattened_policies