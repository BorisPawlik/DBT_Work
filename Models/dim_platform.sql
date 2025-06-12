SELECT
    {{ dbt_utils.generate_surrogate_key([
        'social_platform_preference',
        'daily_social_media_time',
        'number_of_notifications',
        'uses_focus_apps',
        'has_digital_wellbeing_enabled'
    ]) }} AS platform_id,
    social_platform_preference,
    daily_social_media_time,
    number_of_notifications,
    uses_focus_apps,
    has_digital_wellbeing_enabled
FROM (
    SELECT DISTINCT
        social_platform_preference,
        daily_social_media_time,
        number_of_notifications,
        uses_focus_apps,
        has_digital_wellbeing_enabled
    FROM {{ source('dbt_productivity', 'productivity') }}
) AS distinct_values
