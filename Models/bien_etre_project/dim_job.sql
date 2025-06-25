{{ config(
    materialized='incremental'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'job_type'Add commentMore actions
    ]) }} AS job_id,
    job_type
FROM (
    SELECT DISTINCT
        job_type
    FROM {{ source('productivity', 'productivity') }}
) AS distinct_values
