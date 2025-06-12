WITH base AS (
    SELECT
        src.*,
        {{ dbt_utils.generate_surrogate_key(['age', 'gender']) }} AS user_id,
        {{ dbt_utils.generate_surrogate_key(['job_type', 'work_hours_per_day', 'breaks_during_work']) }} AS job_id,
        {{ dbt_utils.generate_surrogate_key([
            'social_platform_preference',
            'daily_social_media_time',
            'number_of_notifications',
            'uses_focus_apps',
            'has_digital_wellbeing_enabled'
        ]) }} AS platform_id,
        {{ dbt_utils.generate_surrogate_key(['sleep_hours', 'screen_time_before_sleep', 'coffee_consumption_per_day']) }} AS sommeil_id,
        {{ dbt_utils.generate_surrogate_key(['perceived_productivity_score', 'actual_productivity_score']) }} AS productivite_id,
        {{ dbt_utils.generate_surrogate_key([
            'stress_level',
            'days_feeling_burnout_per_month',
            'weekly_offline_hours',
            'job_satisfaction_score'
        ]) }} AS sante_id
    FROM {{ source('dbt_demo', 'productivity') }} src
),

final AS (
    SELECT
        user_id,
        job_id,
        platform_id,
        sommeil_id,
        productivite_id,
        sante_id,
        perceived_productivity_score,
        actual_productivity_score,
        stress_level,
        job_satisfaction_score,
        days_feeling_burnout_per_month
    FROM base
)

SELECT * FROM final;
