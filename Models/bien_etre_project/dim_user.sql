{{ config(
    materialized='table',
    unique_key='user_id'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['age', 'gender', 'uses_focus_apps', 'has_digital_wellbeing_enabled']) }} AS user_id,
    age,
    gender,
    uses_focus_apps,
    has_digital_wellbeing_enabled
FROM (
    SELECT DISTINCT
        age,
        gender,
        uses_focus_apps,
        has_digital_wellbeing_enabled
    FROM {{ source('pipeline', 'bien_etre_data') }}
) AS distinct_values
