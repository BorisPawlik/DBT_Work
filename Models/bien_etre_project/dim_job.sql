SELECT
    {{ dbt_utils.generate_surrogate_key([
        'job_type'
    ]) }} AS job_id,
    job_type
FROM (
    SELECT DISTINCT
        job_type
    FROM { source('pipeline', 'productivity2') }
) AS distinct_values
