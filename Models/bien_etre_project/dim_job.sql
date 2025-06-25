{{ config(
    materialized='incremental',
    unique_key='job_id'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'job_type'
    ]) }} AS job_id,
    job_type
FROM (
    SELECT DISTINCT
        job_type
    FROM {{ source('productivity', 'productivity') }}
) AS distinct_values
