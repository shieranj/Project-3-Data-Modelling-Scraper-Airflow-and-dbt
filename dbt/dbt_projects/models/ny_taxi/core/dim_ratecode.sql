{{ config(
    materialized='table'
) }}

WITH ratecodes AS (
    SELECT 1 AS ratecode_id, 'Standard rate' AS ratecode_type
    UNION ALL
    SELECT 2 AS ratecode_id, 'JFK' AS ratecode_type
    UNION ALL
    SELECT 3 AS ratecode_id, 'Newark' AS ratecode_type
    UNION ALL
    SELECT 4 AS ratecode_id, 'Nassau or Westchester' AS ratecode_type
    UNION ALL
    SELECT 5 AS ratecode_id, 'Negotiated fare' AS ratecode_type
    UNION ALL
    SELECT 6 AS ratecode_id, 'Group ride' AS ratecode_type
    UNION ALL
    SELECT 99 AS ratecode_id, 'Null/Unknown' AS ratecode_type
)
SELECT 
    ratecode_id,
    ratecode_type
FROM ratecodes
