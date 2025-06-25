{{ config(
    materialized='incremental',
    unique_key='time_id'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['_modified']) }} AS time_id,
    _modified AS timestamp,
    CAST(DATE(_modified) AS DATE) AS date,
    YEAR(_modified) AS year,
    MONTH(_modified) AS month,
    DAY(_modified) AS day,
    HOUR(_modified) AS hour,
    WEEKOFYEAR(_modified) AS week,
    DATE_FORMAT(_modified, 'EEEE') AS day_name,
    DATE_FORMAT(_modified, 'MMMM') AS month_name

FROM (
    SELECT DISTINCT _modified
    FROM {{ source('productivity', 'productivity') }}
