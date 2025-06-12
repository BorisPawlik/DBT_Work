WITH source AS (
    SELECT DISTINCT
        sleep_hours,
        screen_time_before_sleep,
        coffee_consumption_per_day
    FROM {{ source('dbt_demo', 'productivity') }}
),

with_ids AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['sleep_hours', 'screen_time_before_sleep', 'coffee_consumption_per_day']) }} AS sommeil_id,
        sleep_hours,
        screen_time_before_sleep,
        coffee_consumption_per_day
    FROM source
)

SELECT * FROM with_ids;
