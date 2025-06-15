SELECT
    {{ dbt_utils.generate_surrogate_key([
        'job_type',
    ]) }} AS job_id,
    job_type,
FROM (
    SELECT DISTINCT
        job_type,
    FROM {{ source('dbt_productivity', 'productivity') }}
) AS distinct_values
