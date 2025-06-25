{{ config(
    materialized='incremental',
    unique_key='platform_id'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['social_platform_preference']) }} AS platform_id,
    social_platform_preference
FROM (
    SELECT DISTINCT
        social_platform_preference
    FROM {{ source('productivity', 'productivity') }}
    
    {% if is_incremental() %}
        WHERE _modified > (SELECT MAX(_modified) FROM {{ this }})
    {% endif %}
) AS distinct_values
