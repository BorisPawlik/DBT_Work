SELECT
    {{ dbt_utils.generate_surrogate_key([
        'sleep_hours',
        'screen_time_before_sleep',
        'coffee_consumption_per_day'
    ]) }} AS sommeil_id,
    sleep_hours,
    screen_time_before_sleep,
    coffee_consumption_per_day
FROM (
    SELECT DISTINCT
        sleep_hours,
        screen_time_before_sleep,
        coffee_consumption_per_day
    FROM {{ source('dbt_productivity', 'productivity') }}
) AS distinct_values
