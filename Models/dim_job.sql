WITH source AS (
    SELECT DISTINCT
        job_type,
        work_hours_per_day,
        breaks_during_work
    FROM {{ source('dbt_demo', 'productivity') }}
),

with_ids AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['job_type', 'work_hours_per_day', 'breaks_during_work']) }} AS job_id,
        job_type,
        work_hours_per_day,
        breaks_during_work
    FROM source
)

SELECT * FROM with_ids
