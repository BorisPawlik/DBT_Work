SELECT
    {{ dbt_utils.generate_surrogate_key([
        'job_type',
        'work_hours_per_day',
        'breaks_during_work'
    ]) }} AS job_id,
    job_type,
    work_hours_per_day,
    breaks_during_work
FROM (
    SELECT DISTINCT
        job_type,
        work_hours_per_day,
        breaks_during_work
    FROM {{ source('dbt_productivity', 'productivity') }}
) AS distinct_values
