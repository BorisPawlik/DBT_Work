{{ config(
    materialized='incremental',
    unique_key='user_id'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['age', 'gender', 'uses_focus_apps', 'has_digital_wellbeing_enabled']) }} AS user_id,
    age,
    gender,
    uses_focus_apps,
    has_digital_wellbeing_enabled
FROM {{ source('productivity', 'productivity') }}
{% if is_incremental() %}
WHERE _modified > (SELECT MAX(_modified) FROM {{ this }})
{% endif %}
