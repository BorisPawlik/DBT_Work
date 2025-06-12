WITH source AS (
    SELECT DISTINCT
        age,
        gender
    FROM {{ source('dbt_productivity', 'productivity') }}
),

with_ids AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['age', 'gender']) }} AS user_id,
        age,
        gender
    FROM source
)

SELECT * FROM with_ids;
