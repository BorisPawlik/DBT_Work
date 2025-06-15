SELECT
    {{ dbt_utils.generate_surrogate_key([
        'social_platform_preference'
    ]) }} AS platform_id,
    social_platform_preference
FROM (
    SELECT DISTINCT
        social_platform_preference
    FROM {{ source('dbt_productivity', 'productivity') }}
) AS distinct_values
