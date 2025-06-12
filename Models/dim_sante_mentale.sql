WITH source AS (
    SELECT DISTINCT
        stress_level,
        days_feeling_burnout_per_month,
        weekly_offline_hours,
        job_satisfaction_score
    FROM {{ source('dbt_productivity', 'productivity') }}
),

with_ids AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'stress_level',
            'days_feeling_burnout_per_month',
            'weekly_offline_hours',
            'job_satisfaction_score'
        ]) }} AS sante_id,
        stress_level,
        days_feeling_burnout_per_month,
        weekly_offline_hours,
        job_satisfaction_score
    FROM source
)

SELECT * FROM with_ids;
